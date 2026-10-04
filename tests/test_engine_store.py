from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from catchuparr.engine import ArchiveStore


class ArchiveStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = ArchiveStore(self.root / "archive")
        self.source = self.root / "source.ts"
        self.source.write_bytes(b"synthetic transport stream")
        self.base = datetime(2026, 10, 25, 0, 30, tzinfo=timezone.utc)

    def tearDown(self):
        self.temp.cleanup()

    def add(self, offset, duration=6, **kwargs):
        return self.store.add_segment(
            "channel-1", self.source,
            self.base + timedelta(seconds=offset),
            self.base + timedelta(seconds=offset + duration), **kwargs,
        )

    def test_segment_is_copied_and_range_query_is_ordered(self):
        later = self.add(6)
        earlier = self.add(0)
        self.assertEqual([earlier.id, later.id], [s.id for s in self.store.segments("channel-1")])
        self.assertNotEqual(earlier.path, self.source)
        self.assertEqual(self.source.read_bytes(), earlier.path.read_bytes())
        self.assertEqual([later.id], [s.id for s in self.store.segments("channel-1", start_utc=self.base + timedelta(seconds=7))])

    def test_coverage_merges_adjacent_segments_and_reports_gaps(self):
        self.add(0)
        self.add(6)
        self.add(18)
        coverage = self.store.coverage("channel-1", self.base, self.base + timedelta(seconds=24))
        self.assertEqual(18.0, coverage.covered_seconds)
        self.assertFalse(coverage.complete)
        self.assertEqual([(self.base + timedelta(seconds=12), self.base + timedelta(seconds=18))], list(coverage.gaps))
        self.assertTrue(self.store.coverage("channel-1", self.base, self.base + timedelta(seconds=12)).complete)

    def test_requires_aware_utc_instants_and_valid_ranges(self):
        with self.assertRaises(ValueError):
            self.store.add_segment("channel-1", self.source, datetime(2026, 1, 1), self.base)
        with self.assertRaises(ValueError):
            self.store.add_segment("../escape", self.source, self.base, self.base + timedelta(seconds=1))
        with self.assertRaises(ValueError):
            self.store.add_segment("channel-1", self.source, self.base, self.base)

    def test_program_snapshots_keep_original_utc_schedule(self):
        # This instant is in the repeated local hour during the EU DST rollback.
        local = datetime(2026, 10, 25, 2, 30, tzinfo=timezone(timedelta(hours=2)))
        rowid = self.store.save_program_snapshot("channel-1", local, local + timedelta(minutes=30), "News", {"epg_id": "42"})
        rows = self.store.program_snapshots("channel-1", self.base, self.base + timedelta(hours=3))
        self.assertGreater(rowid, 0)
        self.assertEqual("News", rows[0]["title"])
        self.assertEqual(datetime(2026, 10, 25, 0, 30, tzinfo=timezone.utc), rows[0]["start_utc"])
        self.assertEqual({"epg_id": "42"}, rows[0]["payload"])

    def test_live_playback_lease_protects_segments_from_age_and_quota_cleanup(self):
        first = self.add(0)
        second = self.add(6)
        lease = self.store.begin_playback("channel-1", first.start_utc, first.end_utc, ttl_seconds=120)
        removed = self.store.cleanup(older_than_utc=self.base + timedelta(days=1), max_bytes=0)
        self.assertEqual([second.path], removed)
        self.assertTrue(first.path.exists())
        self.store.end_playback(lease.id)
        removed = self.store.cleanup(older_than_utc=self.base + timedelta(days=1))
        self.assertEqual([first.path], removed)
        self.assertEqual([], self.store.segments("channel-1"))

    def test_expired_lease_does_not_protect_retention(self):
        seg = self.add(0)
        lease = self.store.begin_playback("channel-1", seg.start_utc, seg.end_utc, ttl_seconds=1)
        # Force expiry through the public renewal API's persisted lease clock.
        import sqlite3
        db = sqlite3.connect(self.store.db_path)
        db.execute("UPDATE playback_leases SET expires_at=0 WHERE id=?", (lease.id,))
        db.commit()
        db.close()
        self.assertEqual([seg.path], self.store.cleanup(older_than_utc=self.base + timedelta(days=1)))

    def test_recorder_fencing_rejects_previous_owner(self):
        self.store.register_recorder_fence("channel-1", 12)
        accepted = self.add(0, fencing_token=12)
        self.assertTrue(accepted.path.exists())
        with self.assertRaises(RuntimeError):
            self.add(6, fencing_token=11)
        self.assertEqual(1, len(self.store.segments("channel-1")))


if __name__ == "__main__":
    unittest.main()
