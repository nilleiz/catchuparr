import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catchuparr.engine.store import ArchiveStore
from catchuparr.security import TokenStore
from catchuparr.ts_http import ArchiveTSPlaybackService, StreamingTSHTTPResponse


class ArchiveTSPlaybackTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = ArchiveStore(self.root / "archive")
        self.start = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
        self.tokens = TokenStore(self.store.root)
        self.token = self.tokens.create("viewer")
        self.service = ArchiveTSPlaybackService(
            self.store,
            self.tokens,
            authorize_user_channel=lambda user, channel: user == "viewer" and channel == "news",
            catchup_enabled=lambda *_: True,
        )

    def tearDown(self):
        self.temp.cleanup()

    def add_segment(self, name, offset, body, *, discontinuity=False):
        source = self.root / name
        source.write_bytes(body)
        return self.store.add_segment(
            "news", source,
            self.start + timedelta(seconds=offset),
            self.start + timedelta(seconds=offset + 6),
            discontinuity=discontinuity,
        )

    def request(self, *, start=None, end=None, **kwargs):
        return self.service.stream(
            self.token,
            "news",
            start or self.start,
            end or self.start + timedelta(seconds=30),
            session_id=kwargs.pop("session_id", "living-room"),
            **kwargs,
        )

    def test_streams_contiguous_segments_in_bounded_chunks_and_releases_lease(self):
        self.add_segment("first.ts", 0, b"a" * 60_000)
        self.add_segment("second.ts", 6, b"b" * 60_000)
        response = self.request()

        self.assertIsInstance(response, StreamingTSHTTPResponse)
        self.assertEqual(response.status, 200)
        first_chunk = next(iter(response.body))
        self.assertLessEqual(len(first_chunk), 64 * 1024)
        rest = b"".join(response.body)
        self.assertEqual(len(first_chunk + rest), 120_000)
        response.close()
        paths = [segment.path for segment in self.store.segments("news")]
        removed = self.store.cleanup(
            older_than_utc=self.start + timedelta(days=1), max_bytes=0
        )
        self.assertCountEqual(removed, paths)

    def test_range_reads_join_adjacent_segment_bytes(self):
        self.add_segment("first.ts", 0, b"abcdef")
        self.add_segment("second.ts", 6, b"ghijkl")
        response = self.request(range_header="bytes=2-8")

        self.assertEqual(response.status, 206)
        self.assertEqual(response.headers["Content-Range"], "bytes 2-8/12")
        self.assertEqual(b"".join(response.body), b"cdefghi")
        response.close()

    def test_seek_reuses_and_expands_lease_backwards(self):
        early = self.add_segment("early.ts", 0, b"early!")
        late = self.add_segment("late.ts", 18, b"later!")
        first = self.request(
            start=late.start_utc,
            end=late.end_utc,
            live=True,
        )
        earlier = self.request(
            start=early.start_utc,
            end=early.end_utc,
            live=True,
        )

        self.assertEqual(first.status, 200)
        self.assertEqual(earlier.status, 200)
        self.assertEqual(first.lease_id, earlier.lease_id)
        self.assertEqual(self.store.cleanup(
            older_than_utc=self.start + timedelta(days=1), max_bytes=0
        ), [])
        first.close()
        earlier.close()
        self.assertTrue(self.service.end_session(self.token, "news", first.lease_id))
        removed = self.store.cleanup(older_than_utc=self.start + timedelta(days=1))
        self.assertCountEqual(removed, [early.path, late.path])

    def test_full_stream_rejects_discontinuity(self):
        self.add_segment("first.ts", 0, b"abcdef")
        self.add_segment("second.ts", 6, b"ghijkl", discontinuity=True)

        response = self.request()

        self.assertEqual(response.status, 409)
        paths = [segment.path for segment in self.store.segments("news")]
        removed = self.store.cleanup(
            older_than_utc=self.start + timedelta(days=1), max_bytes=0
        )
        self.assertCountEqual(removed, paths)

    def test_distinct_sessions_share_one_user_stream_limit(self):
        self.add_segment("first.ts", 0, b"abcdef")
        self.service.allow_new_session = lambda user, channel, count: count < 1

        first = self.request(session_id="device-a", live=True)
        blocked = self.request(session_id="device-b", live=True, method="HEAD")

        self.assertEqual(first.status, 200)
        self.assertEqual(blocked.status, 403)
        first.close()
        self.service.end_session(self.token, "news", first.lease_id)

    def test_explicit_device_key_callback_receives_only_hashed_identifier(self):
        self.add_segment("first.ts", 0, b"abcdef")
        seen = []
        self.service.allow_new_session = lambda user, channel, count, device: (
            seen.append((user, channel, count, device)) or True
        )

        response = self.service.stream_for_user(
            "viewer", "news", self.start, self.start + timedelta(seconds=6),
            session_id="xc-session", device_key="a" * 64, method="HEAD", live=True,
        )

        self.assertEqual(response.status, 200)
        self.assertEqual(seen, [("viewer", "news", 0, "a" * 64)])
        self.service.end_user_session("viewer", "news", response.lease_id)

    def test_live_head_and_error_responses_release_the_admission_slot(self):
        self.add_segment("first.ts", 0, b"abcdef")
        self.service.allow_new_session = lambda _user, _channel, count: count < 1

        missing_start = self.start + timedelta(days=1)
        missing = self.service.stream_for_user(
            "viewer", "news", missing_start, missing_start + timedelta(seconds=6),
            session_id="device-missing", device_key="b" * 64,
            live=True, live_follow_seconds=0,
        )
        self.assertEqual(missing.status, 404)
        with sqlite3.connect(self.store.db_path) as db:
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM ts_playback_sessions WHERE user_id='viewer' AND active=1"
            ).fetchone()[0], 0)

        head = self.service.stream_for_user(
            "viewer", "news", self.start, self.start + timedelta(seconds=6),
            session_id="device-head", device_key="c" * 64,
            method="HEAD", live=True,
        )
        self.assertEqual(head.status, 200)
        with sqlite3.connect(self.store.db_path) as db:
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM ts_playback_sessions WHERE user_id='viewer' AND active=1"
            ).fetchone()[0], 0)

        next_device = self.service.stream_for_user(
            "viewer", "news", self.start, self.start + timedelta(seconds=6),
            session_id="device-next", device_key="d" * 64,
            method="HEAD", live=True,
        )
        self.assertEqual(next_device.status, 200)

    def test_same_device_head_probe_preserves_an_open_live_stream_slot(self):
        self.add_segment("first.ts", 0, b"a" * 128_000)
        self.service.allow_new_session = lambda _user, _channel, count: count < 1
        first = self.service.stream_for_user(
            "viewer", "news", self.start, self.start + timedelta(seconds=6),
            session_id="living-room", device_key="f" * 64, live=True,
        )
        head = self.service.stream_for_user(
            "viewer", "news", self.start, self.start + timedelta(seconds=6),
            session_id="living-room", device_key="f" * 64, method="HEAD", live=True,
        )
        second_device = self.service.stream_for_user(
            "viewer", "news", self.start, self.start + timedelta(seconds=6),
            session_id="bedroom", device_key="g" * 64, method="HEAD", live=True,
        )

        self.assertEqual(first.status, 200)
        self.assertEqual(head.status, 200)
        self.assertEqual(second_device.status, 403)
        with sqlite3.connect(self.store.db_path) as db:
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM ts_playback_sessions WHERE user_id='viewer' AND active=1"
            ).fetchone()[0], 1)
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM ts_playback_streams WHERE lease_id=?",
                (first.lease_id,),
            ).fetchone()[0], 1)
        first.close()
        self.service.end_user_session("viewer", "news", first.lease_id)

    def test_end_session_for_another_user_keeps_stream_owner_rows(self):
        self.add_segment("first.ts", 0, b"a" * 128_000)
        response = self.service.stream_for_user(
            "viewer", "news", self.start, self.start + timedelta(seconds=6),
            session_id="owned", device_key="e" * 64,
            live=True,
        )
        self.assertIsInstance(response, StreamingTSHTTPResponse)

        self.assertFalse(self.service.end_user_session("other-user", "news", response.lease_id))
        with sqlite3.connect(self.store.db_path) as db:
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM ts_playback_streams WHERE lease_id=?",
                (response.lease_id,),
            ).fetchone()[0], 1)
        self.assertTrue(next(iter(response.body)))
        response.close()


if __name__ == "__main__":
    unittest.main()
