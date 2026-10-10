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
from catchuparr.ts_http import (
    ArchiveTSPlaybackService,
    StreamingTSHTTPResponse,
    TSHTTPResponse,
)
from catchuparr.xc_runtime import (
    _active_hls_session_count,
    _local_playback_window,
    _make_callbacks,
    _PlaybackHeartbeatIterator,
    _to_django_response,
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


class _FakeResponse(dict):
    def __init__(self, content=b"", status=200):
        super().__init__()
        self.content = content
        self.status_code = status


class XCRuntimeTests(unittest.TestCase):
    def test_playback_heartbeat_marks_initial_lease_then_subsequent_heartbeat(self):
        calls = []
        stream = _PlaybackHeartbeatIterator(
            iter((b"first", b"next")), calls.append, interval=0
        )
        self.assertEqual(b"first", next(stream))
        self.assertEqual(b"next", next(stream))
        self.assertEqual([True, False], calls)

    def test_stats_heartbeat_failure_does_not_interrupt_archive_bytes(self):
        def failed_heartbeat(_first_chunk):
            raise OSError("local projection unavailable")

        stream = _PlaybackHeartbeatIterator(iter((b"first", b"next")), failed_heartbeat, 0)
        with patch("catchuparr.xc_runtime.logger.warning") as warning:
            self.assertEqual(b"first", next(stream))
            self.assertEqual(b"next", next(stream))
        self.assertEqual(2, warning.call_count)

    def test_streaming_local_response_runs_core_db_connection_finalizer(self):
        class FakeStreamingHttpResponse(dict):
            def __init__(self, streaming_content, *, status):
                super().__init__()
                self.streaming_content = streaming_content
                self.status_code = status

        finalized = []
        django = types.ModuleType("django")
        django.__path__ = []
        django_http = types.ModuleType("django.http")
        django_http.StreamingHttpResponse = FakeStreamingHttpResponse
        core = SimpleNamespace(
            HttpResponse=lambda content, status: SimpleNamespace(content=content, status_code=status),
            _finalize_timeshift_response=lambda response: finalized.append(response) or response,
        )
        value = StreamingTSHTTPResponse(
            200, {"Content-Length": "1"}, (chunk for chunk in (b"x",)), "lease-id"
        )

        with patch.dict("sys.modules", {"django": django, "django.http": django_http}):
            response = _to_django_response(value, core)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Length"], "1")
        self.assertEqual(finalized, [response])

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

    def test_xc_first_admission_upgrades_legacy_http_sessions_and_preserves_grace(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ArchiveStore(root)
            start = datetime.now(timezone.utc) - timedelta(days=1)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"legacy lease segment")
            store.add_segment("news", segment_path, start, start + timedelta(seconds=6))
            legacy_lease = store.begin_playback(
                "news", start, start + timedelta(seconds=6), ttl_seconds=4 * 60 * 60
            )
            now = time.time()
            previous_expiry = now + 4 * 60 * 60
            with sqlite3.connect(store.db_path) as db:
                db.execute(
                    "CREATE TABLE http_playback_sessions ("
                    "lease_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,"
                    "channel_id TEXT NOT NULL,request_key TEXT,start_utc REAL NOT NULL,"
                    "end_utc REAL NOT NULL,expires_at REAL NOT NULL)"
                )
                db.execute(
                    "INSERT INTO http_playback_sessions VALUES(?,?,?,?,?,?,?)",
                    (
                        legacy_lease.id, "viewer", "news", "old-request",
                        start.timestamp(), (start + timedelta(seconds=6)).timestamp(),
                        previous_expiry,
                    ),
                )

            admission_hls_counts = []

            def allow_new_session(user_id, _channel_id, plugin_sessions):
                hls_sessions = _active_hls_session_count(root, user_id)
                admission_hls_counts.append(hls_sessions)
                return plugin_sessions + hls_sessions < 1

            service = ArchiveTSPlaybackService(
                store,
                authorize_user_channel=lambda *_: True,
                catchup_enabled=lambda *_: True,
                allow_new_session=allow_new_session,
            )
            response = service.stream_for_user(
                "viewer", "news", start, start + timedelta(seconds=6),
                session_id="xc-request", device_key="b" * 64, method="HEAD",
            )

            with sqlite3.connect(store.db_path) as db:
                columns = {
                    row[1] for row in db.execute(
                        "PRAGMA table_info(http_playback_sessions)"
                    )
                }
                session_expiry, grace_until = db.execute(
                    "SELECT expires_at,grace_until FROM http_playback_sessions "
                    "WHERE lease_id=?", (legacy_lease.id,),
                ).fetchone()
                lease_expiry = db.execute(
                    "SELECT expires_at FROM playback_leases WHERE id=?",
                    (legacy_lease.id,),
                ).fetchone()[0]

        self.assertEqual(response.status, 200)
        self.assertEqual(admission_hls_counts, [0])
        self.assertTrue({"request_key", "device_key", "grace_until"}.issubset(columns))
        self.assertGreater(grace_until, now)
        self.assertLess(grace_until, previous_expiry)
        self.assertEqual(session_expiry, grace_until)
        self.assertGreater(lease_expiry, now)
        self.assertLess(lease_expiry, previous_expiry)

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

    def test_current_xc_playback_clips_exact_window_to_committed_edge(self):
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=20)
        edge = now - timedelta(seconds=5)
        store = _CoverageStore(edge)
        channel_uuid = "channel-9"
        channel = SimpleNamespace(uuid=channel_uuid)
        timeshift_views = SimpleNamespace(
            parse_catchup_timestamp=lambda value: start.replace(tzinfo=None),
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

    def test_xc_one_minute_local_window_does_not_include_provider_padding(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ArchiveStore(root)
            start = datetime.now(timezone.utc) - timedelta(days=1)
            segment_path = root / "one-minute.ts"
            segment_path.write_bytes(b"one minute of archived transport stream")
            store.add_segment(
                "channel-9", segment_path, start, start + timedelta(minutes=1)
            )
            channel = SimpleNamespace(uuid="channel-9")
            timeshift_views = SimpleNamespace(
                parse_catchup_timestamp=lambda _value: start.replace(tzinfo=None),
                resolve_catchup_duration=lambda *_args, **_kwargs: 6,
                HttpResponse=lambda content, status=200: _FakeResponse(content, status),
            )
            callbacks = _make_callbacks(SimpleNamespace(), timeshift_views)
            config = SimpleNamespace(
                channel_uuids=("channel-9",), archive_root=root
            )
            user = SimpleNamespace(id=9)
            request = SimpleNamespace(GET={}, META={}, method="GET")

            class RecordingService:
                def __init__(self):
                    self.window = None

                def stream_for_user(self, _user_id, _channel_uuid, window_start, window_end, **_kwargs):
                    self.window = (window_start, window_end)
                    return TSHTTPResponse(200, {"Content-Length": "1"}, b"x")

            service = RecordingService()
            with (
                patch("catchuparr.xc_runtime._load_config", return_value=config),
                patch("catchuparr.xc_runtime._catchup_enabled", return_value=True),
                patch("catchuparr.xc_runtime._channel_by_uuid", return_value=channel),
                patch("catchuparr.xc_runtime._channel_policy_allows", return_value=True),
                patch("catchuparr.xc_runtime._archive_store", return_value=store),
                patch("catchuparr.xc_runtime._ts_service", return_value=service),
            ):
                self.assertTrue(callbacks.playback_available(
                    "channel-9", "timestamp", "1", user
                ))
                response = callbacks.serve_local_playback(
                    request, user, channel, "timestamp", "1"
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(service.window[0], start)
        self.assertEqual(service.window[1], start + timedelta(seconds=60))

    def test_xc_missing_duration_uses_exact_epg_end_without_provider_padding(self):
        programme_start = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
        start = programme_start + timedelta(minutes=15)
        expected_end = programme_start + timedelta(minutes=60)
        timeshift_views = SimpleNamespace(
            parse_catchup_timestamp=lambda _value: start.replace(tzinfo=None),
        )
        helpers = types.ModuleType("apps.timeshift.helpers")
        helpers.MAX_DURATION_MINUTES = 480
        helpers.get_programme_info = lambda *_args: {
                "start_time": programme_start.isoformat(),
                "end_time": expected_end.isoformat(),
                "duration_secs": 3600,
            }
        apps = types.ModuleType("apps")
        apps.__path__ = []
        apps_timeshift = types.ModuleType("apps.timeshift")
        apps_timeshift.__path__ = []
        with patch.dict("sys.modules", {
            "apps": apps,
            "apps.timeshift": apps_timeshift,
            "apps.timeshift.helpers": helpers,
        }):
            self.assertEqual(
                _local_playback_window(timeshift_views, object(), "timestamp", None),
                (start, expected_end),
            )

    def test_xc_missing_duration_without_trustworthy_epg_end_is_not_local(self):
        start = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
        timeshift_views = SimpleNamespace(
            parse_catchup_timestamp=lambda _value: start.replace(tzinfo=None),
        )
        helpers = types.ModuleType("apps.timeshift.helpers")
        helpers.MAX_DURATION_MINUTES = 480
        helpers.get_programme_info = lambda *_args: None
        apps = types.ModuleType("apps")
        apps.__path__ = []
        apps_timeshift = types.ModuleType("apps.timeshift")
        apps_timeshift.__path__ = []
        with patch.dict("sys.modules", {
            "apps": apps,
            "apps.timeshift": apps_timeshift,
            "apps.timeshift.helpers": helpers,
        }):
            self.assertIsNone(
                _local_playback_window(timeshift_views, object(), "timestamp", None)
            )

    def test_xc_gap_inside_requested_minute_keeps_provider_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ArchiveStore(root)
            start = datetime.now(timezone.utc) - timedelta(days=1)
            segment_path = root / "half-minute.ts"
            segment_path.write_bytes(b"half a minute")
            store.add_segment(
                "channel-9", segment_path, start, start + timedelta(seconds=30)
            )
            channel = SimpleNamespace(uuid="channel-9")
            timeshift_views = SimpleNamespace(
                parse_catchup_timestamp=lambda _value: start.replace(tzinfo=None),
            )
            callbacks = _make_callbacks(SimpleNamespace(), timeshift_views)
            config = SimpleNamespace(
                channel_uuids=("channel-9",), archive_root=root
            )

            with (
                patch("catchuparr.xc_runtime._load_config", return_value=config),
                patch("catchuparr.xc_runtime._catchup_enabled", return_value=True),
                patch("catchuparr.xc_runtime._channel_by_uuid", return_value=channel),
                patch("catchuparr.xc_runtime._channel_policy_allows", return_value=True),
                patch("catchuparr.xc_runtime._archive_store", return_value=store),
            ):
                self.assertFalse(callbacks.playback_available(
                    "channel-9", "timestamp", "1", SimpleNamespace()
                ))

    def test_xc_session_keys_are_stable_and_do_not_contain_credentials(self):
        request = SimpleNamespace(
            GET={"username": "viewer", "password": "sensitive-password"},
            META={"HTTP_USER_AGENT": "XC Player"},
        )
        user = SimpleNamespace(id=9)
        channel = SimpleNamespace(uuid="channel-9")
        start = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
        first = _xc_session_keys(request, user, channel, start)
        second = _xc_session_keys(request, user, channel, start)
        later_programme = _xc_session_keys(
            request, user, channel, start + timedelta(hours=25)
        )

        self.assertEqual(first, second)
        self.assertNotEqual(first[0], later_programme[0])
        self.assertEqual(first[1], later_programme[1])
        self.assertNotIn("sensitive-password", "".join(first))
        self.assertNotIn("viewer", "".join(first))

    def test_native_xc_programme_switch_reuses_device_but_replaces_bounded_session(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ArchiveStore(root)
            start = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
            first_path = root / "first.ts"
            later_path = root / "later.ts"
            first_path.write_bytes(b"a" * 16)
            later_path.write_bytes(b"b" * 16)
            store.add_segment("channel-9", first_path, start, start + timedelta(seconds=6))
            later_start = start + timedelta(hours=25)
            store.add_segment(
                "channel-9", later_path,
                later_start, later_start + timedelta(seconds=6),
            )
            user = SimpleNamespace(id=9)
            request = SimpleNamespace(
                GET={"username": "viewer", "password": "sensitive-password"},
                META={"HTTP_USER_AGENT": "XC Player"},
            )
            channel = SimpleNamespace(uuid="channel-9")
            session_id, device_key = _xc_session_keys(request, user, channel, start)
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
            same_programme = service.stream_for_user(
                "9", "channel-9", start, start + timedelta(seconds=6),
                session_id=session_id, device_key=device_key, live=True,
            )
            later_session_id, later_device_key = _xc_session_keys(
                request, user, channel, later_start
            )
            later = service.stream_for_user(
                "9", "channel-9", later_start, later_start + timedelta(seconds=6),
                session_id=later_session_id, device_key=later_device_key, live=True,
            )

            self.assertEqual(first.status, 200)
            self.assertEqual(same_programme.status, 200)
            self.assertEqual(later.status, 200)
            self.assertEqual(first.lease_id, same_programme.lease_id)
            self.assertNotEqual(first.lease_id, later.lease_id)
            self.assertEqual(device_key, later_device_key)
            self.assertEqual(active_ts_session_count(root, "9"), 2)
            first.close()
            self.assertEqual(active_ts_session_count(root, "9"), 2)
            same_programme.close()
            self.assertEqual(active_ts_session_count(root, "9"), 1)
            later.close()
            service.end_user_session("9", "channel-9", later.lease_id)

    def test_session_count_helpers_fail_closed_on_unexpected_schema_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with sqlite3.connect(root / "archive.sqlite3") as db:
                db.execute("CREATE TABLE ts_playback_sessions (wrong_column TEXT)")
                db.execute(
                    "CREATE TABLE ts_playback_streams "
                    "(id TEXT,lease_id TEXT,expires_at REAL)"
                )
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

            def blocked_builder(segments, *, live, uri_for, start_offset=None):
                builder_entered.set()
                if not release_builder.wait(5):
                    raise TimeoutError("test did not release HLS renderer")
                return build_hls_playlist(
                    segments, live=live, uri_for=uri_for,
                    start_offset=start_offset,
                )

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
