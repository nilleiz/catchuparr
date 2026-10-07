import sqlite3
import tempfile
import threading
import time
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr.engine.playlist import build_hls_playlist
from catchuparr.engine.store import ArchiveStore
from catchuparr.http import ArchiveHTTPService
from catchuparr.security import TokenStore
from catchuparr.ts_http import ArchiveTSPlaybackService
from catchuparr.xc_runtime import (
    _active_hls_session_count,
    _make_callbacks,
    _xc_session_keys,
    active_ts_session_count,
)


class _CoverageStore:
    def __init__(self, edge):
        self.edge = edge
        self.covered_end = None

    def channel_stats(self, channel_uuid):
        return {"latest_end_utc": self.edge.isoformat()}

    def coverage(self, channel_uuid, start, end):
        self.covered_end = end
        return SimpleNamespace(complete=True)


class XCRuntimeTests(unittest.TestCase):
    def test_combined_limit_helpers_count_live_hls_and_ts_sessions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ArchiveStore(root)
            ArchiveTSPlaybackService(
                store,
                authorize_user_channel=lambda *_: True,
                catchup_enabled=lambda *_: True,
            )
            now = time.time()
            with sqlite3.connect(store.db_path) as db:
                db.execute(
                    """CREATE TABLE http_playback_sessions (
                        lease_id TEXT PRIMARY KEY,user_id TEXT,expires_at REAL,grace_until REAL
                    )"""
                )
                db.executemany(
                    "INSERT INTO http_playback_sessions VALUES(?,?,?,?)",
                    [
                        ("hls-active", "viewer", now + 60, None),
                        ("hls-grace", "viewer", now + 30, now + 30),
                        ("hls-expired", "viewer", now - 1, None),
                    ],
                )
                db.execute(
                    """INSERT INTO ts_playback_sessions
                        (lease_id,user_id,channel_id,device_key,request_key,start_utc,end_utc,expires_at,active)
                        VALUES('ts-active','viewer','news','device','request',0,10,?,1)""",
                    (now + 60,),
                )
                db.execute(
                    """INSERT INTO ts_playback_admissions
                        (request_key,user_id,device_key,expires_at)
                        VALUES('pending','viewer','other',?)""",
                    (now + 60,),
                )

            self.assertEqual(active_ts_session_count(root, "viewer"), 2)
            self.assertEqual(_active_hls_session_count(root, "viewer"), 1)

    def test_current_epg_coverage_ends_at_latest_committed_segment(self):
        now = datetime.now(timezone.utc)
        edge = now - timedelta(seconds=8)
        store = _CoverageStore(edge)
        channel_uuid = "channel-9"
        callbacks = _make_callbacks(SimpleNamespace(), SimpleNamespace())
        config = SimpleNamespace(channel_uuids=(channel_uuid,), archive_root=Path("/tmp/archive"))
        start = (now - timedelta(minutes=20)).strftime("%Y-%m-%d %H:%M:%S")
        current_end = (now + timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")

        with (
            patch("catchuparr.xc_runtime._load_config", return_value=config),
            patch("catchuparr.xc_runtime._catchup_enabled", return_value=True),
            patch("catchuparr.xc_runtime._archive_store", return_value=store),
        ):
            self.assertTrue(callbacks.program_available(
                channel_uuid, start, current_end, SimpleNamespace()
            ))

        self.assertEqual(store.covered_end, edge)

    def test_current_xc_playback_uses_core_duration_and_committed_edge(self):
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=20)
        edge = now - timedelta(seconds=5)
        store = _CoverageStore(edge)
        channel_uuid = "channel-9"
        channel = SimpleNamespace(uuid=channel_uuid)
        timeshift_views = SimpleNamespace(
            parse_catchup_timestamp=lambda value: start.replace(tzinfo=None),
            resolve_catchup_duration=lambda channel, value, client_hint=None: 65,
        )
        callbacks = _make_callbacks(SimpleNamespace(), timeshift_views)
        config = SimpleNamespace(channel_uuids=(channel_uuid,), archive_root=Path("/tmp/archive"))

        with (
            patch("catchuparr.xc_runtime._load_config", return_value=config),
            patch("catchuparr.xc_runtime._catchup_enabled", return_value=True),
            patch("catchuparr.xc_runtime._channel_by_uuid", return_value=channel),
            patch("catchuparr.xc_runtime._channel_policy_allows", return_value=True),
            patch("catchuparr.xc_runtime._archive_store", return_value=store),
        ):
            self.assertTrue(callbacks.playback_available(
                channel_uuid, "core-format", "60", SimpleNamespace()
            ))

        self.assertEqual(store.covered_end, edge)

    def test_xc_session_keys_are_stable_and_do_not_contain_credentials(self):
        request = SimpleNamespace(
            GET={"username": "viewer", "password": "sensitive-password"},
            META={"HTTP_USER_AGENT": "XC Player"},
        )
        user = SimpleNamespace(id=9)
        first = _xc_session_keys(request, user)
        second = _xc_session_keys(request, user)

        self.assertEqual(first, second)
        self.assertNotIn("sensitive-password", "".join(first))
        self.assertNotIn("viewer", "".join(first))

    def test_native_xc_timestamp_seek_reuses_the_limited_device_session(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ArchiveStore(root)
            start = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
            first_path = root / "first.ts"
            later_path = root / "later.ts"
            first_path.write_bytes(b"a" * 16)
            later_path.write_bytes(b"b" * 16)
            store.add_segment("channel-9", first_path, start, start + timedelta(seconds=6))
            store.add_segment(
                "channel-9", later_path,
                start + timedelta(minutes=20), start + timedelta(minutes=20, seconds=6),
            )
            user = SimpleNamespace(id=9)
            request = SimpleNamespace(
                GET={"username": "viewer", "password": "sensitive-password"},
                META={"HTTP_USER_AGENT": "XC Player"},
            )
            session_id, device_key = _xc_session_keys(request, user)
            service = ArchiveTSPlaybackService(
                store,
                authorize_user_channel=lambda *_: True,
                catchup_enabled=lambda *_: True,
                allow_new_session=lambda _user, _channel, count: count < 1,
            )

            first = service.stream_for_user(
                "9", "channel-9", start, start + timedelta(seconds=6),
                session_id=session_id, device_key=device_key, live=True,
            )
            later_start = start + timedelta(minutes=20)
            later = service.stream_for_user(
                "9", "channel-9", later_start, later_start + timedelta(seconds=6),
                session_id=session_id, device_key=device_key, live=True,
            )

            self.assertEqual(first.status, 200)
            self.assertEqual(later.status, 200)
            self.assertEqual(first.lease_id, later.lease_id)
            first.close()
            later.close()
            service.end_user_session("9", "channel-9", first.lease_id)

    def test_session_count_helpers_fail_closed_on_unexpected_schema_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with sqlite3.connect(root / "archive.sqlite3") as db:
                db.execute("CREATE TABLE ts_playback_sessions (wrong_column TEXT)")
            with self.assertRaises(sqlite3.OperationalError):
                active_ts_session_count(root, "viewer")

    def test_empty_selected_channels_are_not_advertised_as_local_archive(self):
        class ChannelQuery:
            def filter(self, **_kwargs):
                return self

            def values_list(self, *_fields):
                return [(8, "archived"), (9, "empty")]

        channels = types.ModuleType("apps.channels.models")
        channels.Channel = SimpleNamespace(objects=ChannelQuery())
        channel_utils = types.ModuleType("apps.channels.utils")
        channel_utils.is_catchup_enabled = lambda **_kwargs: True
        apps = types.ModuleType("apps")
        apps.__path__ = []
        apps_channels = types.ModuleType("apps.channels")
        apps_channels.__path__ = []
        store = SimpleNamespace(
            segments=lambda channel_uuid, *_range: [object()] if channel_uuid == "archived" else []
        )
        config = SimpleNamespace(
            channel_uuids=("archived", "empty"), archive_root=Path("/tmp/archive"),
            retention_hours=72,
        )
        callbacks = _make_callbacks(SimpleNamespace(), SimpleNamespace())

        with (
            patch.dict("sys.modules", {
                "apps": apps,
                "apps.channels": apps_channels,
                "apps.channels.models": channels,
                "apps.channels.utils": channel_utils,
            }),
            patch("catchuparr.xc_runtime._load_config", return_value=config),
            patch("catchuparr.xc_runtime._archive_store", return_value=store),
        ):
            self.assertEqual(callbacks.channel_archive_days(SimpleNamespace(uuid="empty")), 0)
            self.assertEqual(callbacks.channel_archive_days(SimpleNamespace(uuid="archived")), 3)
            self.assertEqual(callbacks.m3u_channel_archive_days(SimpleNamespace(id=1)), {"8": 3})

    def test_hls_and_ts_admission_share_one_per_user_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ArchiveStore(root)
            tokens = TokenStore(root)
            token = tokens.create("viewer")
            start = datetime(2026, 1, 1, tzinfo=timezone.utc)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"archive segment")
            store.add_segment("news", segment_path, start, start + timedelta(seconds=6))
            ts_service = ArchiveTSPlaybackService(
                store,
                authorize_user_channel=lambda *_: True,
                catchup_enabled=lambda *_: True,
                allow_new_session=lambda user, _channel, count, _device: (
                    count + _active_hls_session_count(root, user) < 1
                ),
            )
            builder_entered = threading.Event()
            release_builder = threading.Event()
            ts_done = threading.Event()
            hls_result = []
            ts_result = []

            def blocked_builder(segments, *, live, uri_for):
                builder_entered.set()
                if not release_builder.wait(5):
                    raise TimeoutError("test did not release HLS renderer")
                return build_hls_playlist(segments, live=live, uri_for=uri_for)

            hls_service = ArchiveHTTPService(
                store,
                tokens,
                authorize_user_channel=lambda *_: True,
                catchup_enabled=lambda *_: True,
                allow_new_session=lambda user, _channel, count: (
                    count + active_ts_session_count(root, user) < 1
                ),
                playlist_builder=blocked_builder,
            )
            hls_thread = threading.Thread(target=lambda: hls_result.append(
                hls_service.playlist(token, "news", start, start + timedelta(seconds=6))
            ))
            def run_ts():
                ts_result.append(ts_service.stream_for_user(
                    "viewer", "news", start, start + timedelta(seconds=6),
                    session_id="xc-device", device_key="a" * 64,
                    method="HEAD", live=True,
                ))
                ts_done.set()

            ts_thread = threading.Thread(target=run_ts)

            hls_thread.start()
            self.assertTrue(builder_entered.wait(5))
            ts_thread.start()
            try:
                self.assertFalse(ts_done.wait(0.1))
            finally:
                release_builder.set()
            hls_thread.join(5)
            ts_thread.join(5)

            self.assertFalse(hls_thread.is_alive())
            self.assertFalse(ts_thread.is_alive())
            self.assertEqual(hls_result[0].status, 200)
            self.assertEqual(ts_result[0].status, 403)


if __name__ == "__main__":
    unittest.main()
