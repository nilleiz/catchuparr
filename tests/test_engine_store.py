import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

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
        self.assertEqual(earlier, self.store.segment("channel-1", earlier.id))
        self.assertIsNone(self.store.segment("other-channel", earlier.id))

    def test_status_stats_include_progress_and_unselected_archive_usage(self):
        self.assertEqual(0, self.store.channel_stats("channel-1")["segments"])
        first = self.add(0, discontinuity=True)
        self.store.add_segment(
            "former-channel", self.source,
            self.base, self.base + timedelta(seconds=6),
        )
        stats = self.store.channel_stats("channel-1")
        self.assertEqual(1, stats["segments"])
        self.assertEqual(first.end_utc.isoformat(), stats["latest_end_utc"])
        self.assertEqual(1, stats["discontinuities"])
        self.assertEqual(first.path.stat().st_size * 2, self.store.indexed_size_bytes())

    def test_status_metrics_measure_whole_archive_and_usable_channel_history(self):
        first = self.add(0)
        self.add(6)
        self.add(18)
        former = self.store.add_segment(
            "former-channel", self.source,
            self.base, self.base + timedelta(seconds=6),
        )
        total_before_link = self.store.status_metrics(["channel-1"])["archive_storage_bytes"]
        outside = self.root / "outside.ts"
        outside.write_bytes(b"private synthetic outside file")
        (self.store.root / "outside-link").symlink_to(outside)
        metrics = self.store.status_metrics(
            ["channel-1"], now=self.base + timedelta(hours=5, minutes=7)
        )

        self.assertGreaterEqual(metrics["archive_storage_bytes"], first.path.stat().st_size * 4)
        self.assertEqual(total_before_link, metrics["archive_storage_bytes"])
        history = metrics["channels"]["channel-1"]
        self.assertEqual("5h7m", history["history"])
        self.assertEqual(first.start_utc.isoformat(), history["oldest_start_utc"])
        self.assertEqual((self.base + timedelta(seconds=24)).isoformat(), history["latest_end_utc"])
        self.assertEqual(1, len(history["gaps"]))
        self.assertEqual(3, history["segments"])
        self.assertEqual(former.path.stat().st_size, self.store.status_metrics(["former-channel"])["channels"]["former-channel"]["size_bytes"])

    def test_status_metrics_do_not_count_missing_or_external_symlink_segments(self):
        segment = self.add(0)
        before = self.store.status_metrics(["channel-1"])["archive_storage_bytes"]
        outside = self.root / "outside.ts"
        outside.write_bytes(b"synthetic external file")
        file_size = segment.path.stat().st_size
        segment.path.unlink()
        segment.path.symlink_to(outside)

        metrics = self.store.status_metrics(["channel-1"])

        self.assertEqual(0, metrics["channels"]["channel-1"]["segments"])
        self.assertIsNone(metrics["channels"]["channel-1"]["oldest_start_utc"])
        self.assertEqual(before - file_size, metrics["archive_storage_bytes"])

    def test_status_history_skips_empty_and_truncated_indexed_files(self):
        empty_source = self.root / "empty.ts"
        empty_source.write_bytes(b"")
        empty = self.store.add_segment(
            "channel-1", empty_source,
            self.base - timedelta(hours=3), self.base - timedelta(hours=2, minutes=59),
        )
        truncated = self.add(-7200)
        truncated.path.write_bytes(b"x")
        usable = self.store.add_segment(
            "channel-1", self.source,
            self.base - timedelta(hours=1), self.base,
        )

        metrics = self.store.status_metrics(["channel-1"], now=self.base)
        history = metrics["channels"]["channel-1"]

        self.assertTrue(empty.path.exists())
        self.assertEqual("1h0m", history["history"])
        self.assertEqual(usable.start_utc.isoformat(), history["oldest_start_utc"])
        self.assertEqual(usable.end_utc.isoformat(), history["latest_end_utc"])
        self.assertEqual(1, history["segments"])
        self.assertEqual(self.source.stat().st_size, history["size_bytes"])

    def test_coverage_merges_adjacent_segments_and_reports_gaps(self):
        self.add(0)
        self.add(6)
        self.add(18)
        coverage = self.store.coverage("channel-1", self.base, self.base + timedelta(seconds=24))
        self.assertEqual(18.0, coverage.covered_seconds)
        self.assertFalse(coverage.complete)
        self.assertEqual([(self.base + timedelta(seconds=12), self.base + timedelta(seconds=18))], list(coverage.gaps))
        self.assertTrue(self.store.coverage("channel-1", self.base, self.base + timedelta(seconds=12)).complete)

    def test_small_muxer_offsets_do_not_hide_an_archived_programme(self):
        self.add(0)
        self.add(6.14)
        near = self.store.coverage(
            "channel-1", self.base, self.base + timedelta(seconds=12.14)
        )
        self.assertTrue(near.complete)
        self.add(12.54)
        missing = self.store.coverage(
            "channel-1", self.base, self.base + timedelta(seconds=18.54)
        )
        self.assertFalse(missing.complete)
        self.assertEqual(1, len(missing.gaps))

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
        repeated = self.store.save_program_snapshot(
            "channel-1", local, local + timedelta(minutes=30), "News", {"epg_id": "43"}
        )
        rows = self.store.program_snapshots("channel-1", self.base, self.base + timedelta(hours=3))
        self.assertGreater(rowid, 0)
        self.assertEqual(rowid, repeated)
        self.assertEqual(1, len(rows))
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

    def test_event_lease_can_extend_and_protects_early_playlist_segments(self):
        first = self.add(0)
        second = self.add(6)
        lease = self.store.begin_playback("channel-1", first.start_utc, first.end_utc, ttl_seconds=120)
        self.assertTrue(self.store.extend_playback(lease.id, second.end_utc, ttl_seconds=120))
        removed = self.store.cleanup(older_than_utc=self.base + timedelta(days=1), max_bytes=0)
        self.assertEqual([], removed)
        self.assertTrue(first.path.exists())
        self.assertTrue(second.path.exists())

    def test_playback_lease_can_expand_backwards_for_ts_seek(self):
        earlier = self.add(0)
        later = self.add(12)
        lease = self.store.begin_playback(
            "channel-1", later.start_utc, later.end_utc, ttl_seconds=120
        )

        self.assertTrue(self.store.extend_playback(
            lease.id,
            later.end_utc,
            start_utc=earlier.start_utc,
            ttl_seconds=120,
        ))
        removed = self.store.cleanup(older_than_utc=self.base + timedelta(days=1), max_bytes=0)
        self.assertEqual([], removed)

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
        with self.assertRaisesRegex(RuntimeError, "required"):
            self.add(3)
        with self.assertRaises(RuntimeError):
            self.add(6, fencing_token=11)
        self.assertEqual(1, len(self.store.segments("channel-1")))

    def test_reconcile_removes_unindexed_or_missing_segment_files(self):
        indexed = self.add(0)
        indexed.path.unlink()
        orphan = self.store.root / "segments" / "channel-1" / "orphan.ts"
        orphan.write_bytes(b"orphan")
        partial = orphan.parent / ".segment-crash.partial"
        partial.write_bytes(b"partial")
        recovered = ArchiveStore(self.store.root, orphan_grace_seconds=0)
        self.assertEqual([], recovered.segments("channel-1"))
        self.assertFalse(orphan.exists())
        self.assertFalse(partial.exists())


if __name__ == "__main__":
    unittest.main()
