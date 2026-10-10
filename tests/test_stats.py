"""Synthetic contracts for the native Stats projection."""

from __future__ import annotations

import hashlib
import inspect
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from catchuparr import stats


class StatsProjectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "archive.sqlite3"
        self.options = mock.patch.object(
            stats, "_options",
            return_value={
                "show_archive_playback_in_stats": True,
                "hide_recorders_in_stats": True,
            },
        )
        self.options.start()
        self.addCleanup(self.options.stop)
        self.addCleanup(self.temp.cleanup)

    def _connect(self):
        return sqlite3.connect(self.database)

    def test_successful_viewer_uses_stable_opaque_id_and_expires_after_180s(self):
        with mock.patch.object(stats, "_db_path", return_value=self.database):
            first = stats.successful_playback(
                7, "synthetic-channel-uuid", "hashed-device-key", now=1000
            )
            second = stats.successful_playback(
                7, "synthetic-channel-uuid", "hashed-device-key", now=1100
            )
            self.assertEqual(first, second)
            self.assertTrue(first.startswith(stats.DISPLAY_ID_PREFIX))
            self.assertNotIn("hashed-device-key", first)
            with self._connect() as db:
                db.execute(
                    "CREATE TABLE ts_playback_sessions (lease_id TEXT,user_id TEXT,"
                    "channel_id TEXT,device_key TEXT,active INTEGER,expires_at REAL)"
                )
                db.execute(
                    "INSERT INTO ts_playback_sessions VALUES(?,?,?,?,?,?)",
                    (
                        "lease-1", "7", "synthetic-channel-uuid",
                        "hashed-device-key", 1, 2000,
                    ),
                )
            stats.successful_playback(
                7,
                "synthetic-channel-uuid",
                "hashed-device-key",
                playback_lease_id="lease-1",
                programme_start_epoch=1_760_000_000,
                client_ip="192.0.2.20",
                now=1200,
            )
            with self._connect() as db:
                rows = db.execute(
                    "SELECT display_id,logical_started_at,last_success_at,observed_position,"
                    "programme_start_epoch,client_ip "
                    "FROM catchuparr_stats_viewers"
                ).fetchall()
            self.assertEqual(
                [(first, 1000.0, 1200.0, None, 1_760_000_000.0, "192.0.2.20")],
                rows,
            )

    def test_stats_heartbeat_is_display_only_and_expires_at_180_seconds(self):
        with mock.patch.object(stats, "_db_path", return_value=self.database):
            stats.successful_playback(7, "synthetic-channel-uuid", "hashed-device-key", now=1000)
            with mock.patch.object(stats.time, "time", return_value=1179), mock.patch.object(
                stats, "_viewer_row", side_effect=lambda row: {"display_id": row["display_id"]}
            ):
                self.assertEqual(1, len(stats._active_viewers()))
            with mock.patch.object(stats.time, "time", return_value=1181), mock.patch.object(
                stats, "_viewer_row", side_effect=lambda row: {"display_id": row["display_id"]}
            ):
                self.assertEqual([], stats._active_viewers())

    def test_hls_lease_validation_accepts_only_current_unexpired_device_lease(self):
        with self._connect() as db:
            db.execute(
                "CREATE TABLE http_playback_sessions (lease_id TEXT,user_id TEXT,"
                "channel_id TEXT,device_key TEXT,grace_until REAL,expires_at REAL)"
            )
            db.executemany(
                "INSERT INTO http_playback_sessions VALUES(?,?,?,?,?,?)",
                [
                    ("current", "7", "channel-uuid", "device-key", None, 500),
                    ("grace", "7", "channel-uuid", "device-key", 300, 300),
                    ("other-device", "7", "channel-uuid", "other-device", None, 500),
                    ("expired", "7", "channel-uuid", "device-key", None, 99),
                ],
            )
            self.assertTrue(
                stats._valid_current_playback_lease(
                    db, "current", "7", "channel-uuid", "device-key", 100
                )
            )
            for lease_id in ("grace", "other-device", "expired"):
                with self.subTest(lease_id=lease_id):
                    self.assertFalse(
                        stats._valid_current_playback_lease(
                            db, lease_id, "7", "channel-uuid", "device-key", 100
                        )
                    )

    def test_native_timeshift_websocket_is_asked_to_refresh(self):
        calls = []
        redis = object()
        apps = types.ModuleType("apps")
        apps.__path__ = []
        timeshift = types.ModuleType("apps.timeshift")
        timeshift.__path__ = []
        views = types.ModuleType("apps.timeshift.views")
        views._trigger_timeshift_stats_update = lambda value: calls.append(value)
        core = types.ModuleType("core")
        core.__path__ = []
        utils = types.ModuleType("core.utils")
        utils.RedisClient = types.SimpleNamespace(get_client=lambda: redis)
        with mock.patch.dict(sys.modules, {
            "apps": apps,
            "apps.timeshift": timeshift,
            "apps.timeshift.views": views,
            "core": core,
            "core.utils": utils,
        }):
            stats._emit_timeshift_stats_update()
        self.assertEqual([redis], calls)

    def test_worker_installs_live_pre_cap_display_filter_without_web_identity_hooks(self):
        class ChannelStatus:
            @staticmethod
            def get_basic_channel_info(channel_id):
                return {"channel_id": channel_id, "clients": [], "client_count": 0}

            @staticmethod
            def get_detailed_channel_info(channel_id):
                return {"channel_id": channel_id, "clients": []}

        def native_builder(redis_client):
            return {"channels": [], "count": 0}

        modules = {
            "version": types.ModuleType("version"),
            "apps": types.ModuleType("apps"),
            "apps.proxy": types.ModuleType("apps.proxy"),
            "apps.proxy.live_proxy": types.ModuleType("apps.proxy.live_proxy"),
            "apps.proxy.live_proxy.channel_status": types.ModuleType(
                "apps.proxy.live_proxy.channel_status"
            ),
            "apps.proxy.live_proxy.views": types.ModuleType(
                "apps.proxy.live_proxy.views"
            ),
            "apps.proxy.stats_views": types.ModuleType("apps.proxy.stats_views"),
            "apps.proxy.tasks": types.ModuleType("apps.proxy.tasks"),
            "apps.timeshift": types.ModuleType("apps.timeshift"),
            "apps.timeshift.stats": types.ModuleType("apps.timeshift.stats"),
            "apps.timeshift.stats_views": types.ModuleType("apps.timeshift.stats_views"),
        }
        modules["version"].__version__ = "0.31.0"
        modules["apps"].__path__ = []
        modules["apps.proxy"].__path__ = []
        modules["apps.proxy.live_proxy"].__path__ = []
        modules["apps.timeshift"].__path__ = []
        modules["apps.proxy.live_proxy.channel_status"].ChannelStatus = ChannelStatus
        for name in (
            "apps.proxy.live_proxy.channel_status",
            "apps.proxy.live_proxy.views",
            "apps.proxy.stats_views",
            "apps.proxy.tasks",
            "apps.timeshift.stats",
            "apps.timeshift.stats_views",
        ):
            modules[name].build_live_channel_stats_data = native_builder
            modules[name].build_timeshift_stats_data = native_builder
        original_count = len(stats._ORIGINALS)
        with mock.patch.dict(sys.modules, modules):
            self.assertTrue(stats.install_stats_hooks(route_hooks=False))
            self.assertTrue(
                getattr(ChannelStatus.get_basic_channel_info, stats.HOOK_MARKER)
            )
            self.assertTrue(
                getattr(ChannelStatus.get_detailed_channel_info, stats.HOOK_MARKER)
            )
            self.assertFalse(hasattr(modules["apps.proxy.live_proxy.views"], "stream_ts"))
        while len(stats._ORIGINALS) > original_count:
            owner, name, original = stats._ORIGINALS.pop()
            setattr(owner, name, original)

    def test_native_stream_drf_callback_closure_is_accepted(self):
        def stream_ts(request, channel_id, user=None, force_output_format=None):
            return request, channel_id, user, force_output_format

        def get(self, *args, **kwargs):
            return stream_ts(*args, **kwargs)

        view_class = type(
            "stream_ts",
            (),
            {
                "__module__": stream_ts.__module__,
                "http_method_names": ["get", "options"],
                "get": get,
            },
        )

        def callback(request, *args, **kwargs):
            return stream_ts(request, *args, **kwargs)

        callback.__name__ = "view"
        callback.__module__ = stream_ts.__module__
        callback.cls = view_class
        route = types.SimpleNamespace(
            name="stream",
            callback=callback,
            pattern=types.SimpleNamespace(converters={"channel_id": object()}),
        )
        from catchuparr.adapters.recorder_proxy import _stream_route_channel_id_issue

        signature = tuple(inspect.signature(callback).parameters)
        self.assertTrue(stats._supported_stream_view_signature(callback, signature))
        self.assertIsNone(_stream_route_channel_id_issue(route, 0, callback))

    def test_stats_toggles_are_independent_and_read_applied_settings(self):
        from catchuparr import runtime

        self.options.stop()
        try:
            for show_archive in (False, True):
                for hide_recorders in (False, True):
                    with self.subTest(show_archive=show_archive, hide_recorders=hide_recorders):
                        with mock.patch.object(
                            runtime, "load_runtime_settings",
                            return_value={
                                "show_archive_playback_in_stats": show_archive,
                                "hide_recorders_in_stats": hide_recorders,
                            },
                            create=True,
                        ):
                            self.assertEqual(
                                {
                                    "show_archive_playback_in_stats": show_archive,
                                    "hide_recorders_in_stats": hide_recorders,
                                },
                                stats._options(),
                            )
        finally:
            self.options.start()

    def test_timeshift_projection_copies_native_payload_and_appends_metadata_row(self):
        native = {
            "timeshift_sessions": [{"session_id": "native-id", "connections": []}],
            "total_connections": 1,
            "timestamp": 12.0,
        }
        archive_row = {
            "session_id": "ca_display_only",
            "stats_channel_id": "ca_display_only",
            "channel_id": 42,
            "channel_uuid": "synthetic-channel-uuid",
            "channel_name": "Synthetic Channel A",
            "programme_start": None,
            "position_anchor_at": None,
            "playback_base_secs": None,
            "paused": False,
            "connection_count": 1,
            "connections": [{"user_id": "7", "username": "Synthetic Viewer"}],
        }
        with mock.patch.object(stats, "_active_viewers", return_value=[archive_row]):
            result = stats.project_timeshift_stats(native)
        self.assertEqual(1, len(native["timeshift_sessions"]))
        self.assertEqual(2, result["total_connections"])
        self.assertEqual("native-id", result["timeshift_sessions"][0]["session_id"])
        self.assertEqual("ca_display_only", result["timeshift_sessions"][1]["session_id"])
        self.assertIsNone(result["timeshift_sessions"][1]["playback_base_secs"])

    def test_rest_combined_and_websocket_builder_aliases_share_projection(self):
        native = {
            "timeshift_sessions": [],
            "total_connections": 0,
            "timestamp": 12.0,
        }
        archive_row = {
            "session_id": "ca_socket_viewer",
            "stats_channel_id": "ca_socket_viewer",
            "channel_id": 42,
            "channel_uuid": "synthetic-channel-uuid",
            "channel_name": "Synthetic Channel A",
            "connections": [{"user_id": "7", "username": "Synthetic Viewer"}],
        }
        modules = [types.SimpleNamespace(), types.SimpleNamespace(), types.SimpleNamespace()]

        def native_builder(redis_client):
            return native

        for module in modules:
            module.build_timeshift_stats_data = native_builder
        original_count = len(stats._ORIGINALS)
        with mock.patch.object(stats, "_active_viewers", return_value=[archive_row]):
            for module in modules:
                self.assertTrue(stats._install_builder(
                    module, "build_timeshift_stats_data", stats.project_timeshift_stats
                ))
            projected = [module.build_timeshift_stats_data(object()) for module in modules]
        self.assertEqual([1, 1, 1], [item["total_connections"] for item in projected])
        self.assertEqual(
            ["ca_socket_viewer"] * 3,
            [item["timeshift_sessions"][0]["session_id"] for item in projected],
        )
        self.assertEqual([], native["timeshift_sessions"])
        while len(stats._ORIGINALS) > original_count:
            module, name, original = stats._ORIGINALS.pop()
            setattr(module, name, original)

    def test_recorder_filter_requires_exact_server_marked_client_and_preserves_source(self):
        payload = {
            "channels": [
                {
                    "channel_id": "channel-1",
                    "client_count": 2,
                    "clients": [
                        {"client_id": "verified-recorder"},
                        {"client_id": "ordinary-viewer", "user_agent": "FFmpeg"},
                    ],
                }
            ],
            "total_connections": 2,
        }
        with mock.patch.object(
            stats, "_is_recorder_client",
            side_effect=lambda channel, client: (channel, client) == (
                "channel-1", "verified-recorder"
            ),
        ):
            result = stats.project_live_stats(payload)
        self.assertEqual(2, len(payload["channels"][0]["clients"]))
        self.assertEqual(
            [{"client_id": "ordinary-viewer", "user_agent": "FFmpeg"}],
            result["channels"][0]["clients"],
        )
        self.assertEqual(1, result["channels"][0]["client_count"])
        self.assertEqual(1, result["total_connections"])

    def test_unknown_recorder_like_client_is_not_hidden(self):
        payload = {
            "channels": [{
                "channel_id": "channel-2",
                "clients": [{"client_id": "normal", "user_agent": "Catchuparr recorder"}],
            }],
            "total_connections": 1,
        }
        result = stats.project_live_stats(payload)
        self.assertEqual(payload, result)

    def test_verified_recorder_is_removed_before_native_top_ten(self):
        class FakeRedis:
            def smembers(self, key):
                return {b"recorder"} | {f"viewer-{index}".encode() for index in range(11)}

            def hmget(self, key, *fields):
                client_id = key.rsplit(":", 1)[-1]
                values = {
                    "user_agent": f"Synthetic {client_id}".encode(),
                    "ip_address": None,
                    "connected_at": b"1.0",
                    "user_id": b"7",
                    "output_format": b"mpegts",
                    "output_profile_id": None,
                }
                return [values[field] for field in fields]

        class FakeKeys:
            @staticmethod
            def clients(channel_id):
                return f"live:channel:{channel_id}:clients"

            @staticmethod
            def client_metadata(channel_id, client_id):
                return f"live:channel:{channel_id}:clients:{client_id}"

        redis = FakeRedis()
        channel_status = types.SimpleNamespace(
            ProxyServer=types.SimpleNamespace(
                get_instance=lambda: types.SimpleNamespace(redis_client=redis)
            ),
            RedisKeys=FakeKeys,
        )
        native = {
            "client_count": 12,
            "clients": [{"client_id": "recorder"}]
            + [{"client_id": f"viewer-{index}"} for index in range(9)],
        }
        with mock.patch.object(
            stats,
            "_is_recorder_client",
            side_effect=lambda channel, client: client == "recorder",
        ):
            result = stats._visible_basic_channel_info(channel_status, "42", native)
        self.assertEqual(12, native["client_count"])
        self.assertEqual(11, result["client_count"])
        self.assertEqual(10, len(result["clients"]))
        self.assertNotIn("recorder", {client["client_id"] for client in result["clients"]})
        self.assertIn("viewer-10", {client["client_id"] for client in result["clients"]})

    def test_recorder_capability_must_match_current_fenced_lease_and_channel(self):
        token = "synthetic-short-lived-capability"
        digest = hashlib.sha256(token.encode("ascii")).hexdigest()
        lease_value = "4:synthetic-owner"
        records = {
            f"catchuparr:recorder:stats-cap:{digest}": f"channel-uuid|{lease_value}",
            "catchuparr:recorder:channel-uuid": lease_value,
        }

        class FakeRedis:
            def get(self, key):
                return records.get(key)

        core_utils = types.ModuleType("core.utils")
        core_utils.RedisClient = types.SimpleNamespace(get_client=lambda: FakeRedis())
        core = types.ModuleType("core")
        core.utils = core_utils

        class Request:
            headers = {"X-Catchuparr-Recorder": token}

        with mock.patch.dict(sys.modules, {"core": core, "core.utils": core_utils}):
            self.assertEqual(digest, stats._verified_recorder_request(Request(), "channel-uuid"))
            self.assertEqual(
                digest,
                stats._verified_recorder_request(
                    types.SimpleNamespace(
                        META={"HTTP_X_CATCHUPARR_STATS_RECORDER": token}
                    ),
                    "channel-uuid",
                    meta_header="HTTP_X_CATCHUPARR_STATS_RECORDER",
                    request_header="X-Catchuparr-Stats-Recorder",
                ),
            )
            self.assertIsNone(stats._verified_recorder_request(Request(), "other-channel"))
            records["catchuparr:recorder:channel-uuid"] = "5:replacement-owner"
            self.assertIsNone(stats._verified_recorder_request(Request(), "channel-uuid"))

    def test_valid_capability_tags_only_actual_native_client_registration(self):
        token = "synthetic-recorder-capability"
        digest = hashlib.sha256(token.encode("ascii")).hexdigest()
        lease_value = "9:synthetic-owner"

        class FakeRedis:
            def __init__(self):
                self.values = {
                    f"catchuparr:recorder:stats-cap:{digest}": f"channel-uuid|{lease_value}",
                    "catchuparr:recorder:channel-uuid": lease_value,
                }
                self.sets = {}

            def get(self, key):
                return self.values.get(key)

            def set(self, key, value, **_kwargs):
                self.values[key] = value

            def sadd(self, key, value):
                self.sets.setdefault(key, set()).add(value)

            def srem(self, key, value):
                self.sets.get(key, set()).discard(value)

            def expire(self, *_args):
                return True

            def delete(self, key):
                self.values.pop(key, None)

        redis = FakeRedis()
        class ChannelStatus:
            @staticmethod
            def get_basic_channel_info(channel_id):
                return {"channel_id": channel_id, "clients": [], "client_count": 0}

            @staticmethod
            def get_detailed_channel_info(channel_id):
                return {"channel_id": channel_id, "clients": []}

        class ClientManager:
            channel_id = "worker-7"

            def add_client(
                self, client_id, client_ip, user_agent=None, user=None,
                output_format="mpegts", output_profile_id=None,
            ):
                self.registered = client_id
                return True

            def remove_client(self, client_id):
                self.removed = client_id

        registrations = []

        def native_stream(request, channel_id, user=None, force_output_format=None):
            manager = ClientManager()
            manager.add_client("client_1", "synthetic-ip", "synthetic-agent", None)
            registrations.append(manager)
            return "native-response"

        class Route:
            name = "stream"

            def __init__(self, callback):
                self.callback = callback
                self.pattern = types.SimpleNamespace(
                    converters={"channel_id": object()}
                )

        guard_calls = []

        def stream_guard(request, channel_id, *args, **kwargs):
            guard_calls.append(channel_id)
            return native_stream(request, channel_id, *args, **kwargs)

        stream_guard._catchuparr_managed_id_guard = True
        stream_guard._catchuparr_original = native_stream
        route = Route(stream_guard)
        channel_status = types.ModuleType("apps.proxy.live_proxy.channel_status")
        channel_status.ChannelStatus = ChannelStatus
        channel_status.ProxyServer = types.SimpleNamespace(
            get_instance=lambda: types.SimpleNamespace(redis_client=redis)
        )
        channel_status.RedisKeys = types.SimpleNamespace()
        live_views = types.ModuleType("apps.proxy.live_proxy.views")
        live_views.stream_ts = native_stream
        live_urls = types.ModuleType("apps.proxy.live_proxy.urls")
        live_urls.urlpatterns = [route]
        client_manager_module = types.ModuleType("apps.proxy.live_proxy.client_manager")
        client_manager_module.ClientManager = ClientManager
        proxy_live = types.ModuleType("apps.proxy.live_proxy")
        proxy_live.__path__ = []
        proxy = types.ModuleType("apps.proxy")
        proxy.__path__ = []
        apps = types.ModuleType("apps")
        apps.__path__ = []
        core_utils = types.ModuleType("core.utils")
        core_utils.RedisClient = types.SimpleNamespace(get_client=lambda: redis)
        core = types.ModuleType("core")
        core.__path__ = []

        class Request:
            def __init__(self, cap):
                self.headers = {"X-Catchuparr-Recorder": cap}
                self.META = {"HTTP_X_CATCHUPARR_RECORDER": cap}

        modules = {
            "apps": apps,
            "apps.proxy": proxy,
            "apps.proxy.live_proxy": proxy_live,
            "apps.proxy.live_proxy.channel_status": channel_status,
            "apps.proxy.live_proxy.views": live_views,
            "apps.proxy.live_proxy.urls": live_urls,
            "apps.proxy.live_proxy.client_manager": client_manager_module,
            "core": core,
            "core.utils": core_utils,
        }
        original_count = len(stats._ORIGINALS)
        original_route_count = len(stats._IDENTITY_ROUTES)
        try:
            with mock.patch.dict(sys.modules, modules):
                self.assertTrue(stats._install_identity_hooks(channel_status, live_views))
                installed_callback = route.callback
                self.assertTrue(stats._install_identity_hooks(channel_status, live_views))
                self.assertIs(installed_callback, route.callback)
                self.assertEqual("native-response", route.callback(Request(token), "channel-uuid"))
                self.assertEqual(["channel-uuid"], guard_calls)
                marker = "catchuparr:stats:recorder-client:worker-7:client_1"
                self.assertEqual(f"{digest}|channel-uuid", redis.values[marker])
                registrations[0].remove_client("client_1")
                self.assertNotIn(marker, redis.values)
                self.assertEqual(
                    "native-response", route.callback(Request("invalid"), "channel-uuid")
                )
                self.assertEqual(["channel-uuid", "channel-uuid"], guard_calls)
                self.assertEqual(2, len(registrations))
                self.assertFalse(any(
                    key.startswith("catchuparr:stats:recorder-client:worker-7:")
                    for key in redis.values
                ))
        finally:
            while len(stats._ORIGINALS) > original_count:
                owner, name, original = stats._ORIGINALS.pop()
                setattr(owner, name, original)
            while len(stats._IDENTITY_ROUTES) > original_route_count:
                route_entry, original = stats._IDENTITY_ROUTES.pop()
                route_entry.callback = original

    def test_stop_revokes_only_mapped_plugin_device_sessions(self):
        with mock.patch.object(stats, "_db_path", return_value=self.database):
            with self._connect() as db:
                stats._ensure_schema(db)
                db.executescript(
                    """
                    CREATE TABLE ts_playback_sessions (
                        lease_id TEXT PRIMARY KEY,user_id TEXT,channel_id TEXT,
                        device_key TEXT,active INTEGER,expires_at REAL
                    );
                    CREATE TABLE ts_playback_streams(id TEXT,lease_id TEXT);
                    CREATE TABLE http_playback_sessions (
                        lease_id TEXT PRIMARY KEY,user_id TEXT,channel_id TEXT,
                        device_key TEXT,grace_until REAL,expires_at REAL
                    );
                    """
                )
                db.execute(
                    "INSERT INTO catchuparr_stats_viewers "
                    "(viewer_key,display_id,user_id,channel_uuid,playback_device_key,"
                    "logical_started_at,last_success_at,playback_lease_id) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (
                        hashlib.sha256(
                            f"catchuparr-viewer\0{7}\0channel-uuid\0device-key".encode()
                        ).hexdigest(),
                        "ca_stop_me", "7", "channel-uuid", "device-key", 10, 20, "lease-a",
                    ),
                )
                db.execute(
                    "INSERT INTO ts_playback_sessions "
                    "VALUES('lease-a','7','channel-uuid','device-key',1,1000)"
                )
                db.execute("INSERT INTO ts_playback_streams VALUES('stream-a','lease-a')")
                db.execute(
                    "INSERT INTO ts_playback_sessions "
                    "VALUES('lease-b','8','channel-uuid','other-device',1,1000)"
                )
                db.commit()
            with mock.patch("catchuparr.runtime.load_config", return_value=None):
                self.assertTrue(stats.revoke_display_session("ca_stop_me"))
                stats.successful_playback(
                    7, "channel-uuid", "device-key",
                    playback_lease_id="lease-a", now=30,
                )
                with self._connect() as db:
                    self.assertEqual(
                        1,
                        db.execute(
                            "SELECT revoked FROM catchuparr_stats_viewers "
                            "WHERE display_id='ca_stop_me'"
                        ).fetchone()[0],
                    )
                    db.execute(
                        "INSERT INTO ts_playback_sessions "
                        "VALUES('replacement-lease','7','channel-uuid','device-key',1,1000)"
                    )
                stats.successful_playback(
                    7, "channel-uuid", "device-key",
                    playback_lease_id="replacement-lease", now=31,
                )
                stats.successful_playback(
                    7, "channel-uuid", "device-key",
                    playback_lease_id="lease-a", now=32,
                )
            with self._connect() as db:
                self.assertEqual(
                    0,
                    db.execute(
                        "SELECT revoked FROM catchuparr_stats_viewers "
                        "WHERE display_id='ca_stop_me'"
                    ).fetchone()[0],
                )
                self.assertEqual(
                    "replacement-lease",
                    db.execute(
                        "SELECT playback_lease_id FROM catchuparr_stats_viewers "
                        "WHERE display_id='ca_stop_me'"
                    ).fetchone()[0],
                )
                self.assertEqual(
                    0,
                    db.execute(
                        "SELECT COUNT(*) FROM ts_playback_sessions WHERE lease_id='lease-a'"
                    ).fetchone()[0],
                )
                self.assertEqual(
                    1,
                    db.execute(
                        "SELECT COUNT(*) FROM ts_playback_sessions WHERE lease_id='lease-b'"
                    ).fetchone()[0],
                )
                self.assertEqual(
                    0, db.execute("SELECT COUNT(*) FROM ts_playback_streams").fetchone()[0]
                )
            self.assertFalse(stats.revoke_display_session("native-session-id"))

    def test_stop_hook_keeps_native_admin_response_and_delegates_unknown_ids(self):
        class FakeResponse:
            def __init__(self, status_code):
                self.status_code = status_code

        class Route:
            name = "catchup_stop_client"

            def __init__(self, callback):
                self.callback = callback

        calls = []

        def stop_timeshift_session(request):
            calls.append(request.data["session_id"])
            return FakeResponse(403 if not request.is_admin else 404)

        class FakeIsAdmin:
            pass

        def make_drf_callback(endpoint):
            def post(self, *args, **kwargs):
                return endpoint(*args, **kwargs)

            view_class = type(
                "stop_timeshift_session",
                (),
                {
                    "__module__": endpoint.__module__,
                    "http_method_names": ["post", "options"],
                    "permission_classes": [FakeIsAdmin],
                    "post": post,
                },
            )

            def callback(request, *args, **kwargs):
                return endpoint(request, *args, **kwargs)

            callback.__name__ = "view"
            callback.__module__ = endpoint.__module__
            callback.cls = view_class
            return callback

        drf_stop = make_drf_callback(stop_timeshift_session)

        stats_views = types.ModuleType("apps.timeshift.stats_views")
        stats_views.stop_timeshift_session = drf_stop
        routes = types.ModuleType("apps.timeshift.urls")
        route = Route(drf_stop)
        routes.urlpatterns = [route]
        fake_apps = types.ModuleType("apps")
        fake_accounts = types.ModuleType("apps.accounts")
        fake_permissions = types.ModuleType("apps.accounts.permissions")
        fake_permissions.IsAdmin = FakeIsAdmin
        fake_accounts.permissions = fake_permissions
        fake_timeshift = types.ModuleType("apps.timeshift")
        fake_apps.timeshift = fake_timeshift
        fake_timeshift.stats_views = stats_views
        fake_timeshift.urls = routes
        fake_django = types.ModuleType("django")
        fake_django_http = types.ModuleType("django.http")

        class JsonResponse:
            def __init__(self, data):
                self.data = data
                self.status_code = 200

        fake_django_http.JsonResponse = JsonResponse
        fake_django.http = fake_django_http

        class Request:
            def __init__(self, session_id, is_admin=True):
                self.data = {"session_id": session_id}
                self.is_admin = is_admin

        old_hook = stats._STOP_HOOK
        try:
            with mock.patch.dict(
                sys.modules,
                {
                    "apps": fake_apps,
                    "apps.accounts": fake_accounts,
                    "apps.accounts.permissions": fake_permissions,
                    "apps.timeshift": fake_timeshift,
                    "apps.timeshift.stats_views": stats_views,
                    "apps.timeshift.urls": routes,
                    "django": fake_django,
                    "django.http": fake_django_http,
                },
            ), mock.patch.object(stats, "revoke_display_session", return_value=True) as revoke:
                stats._STOP_HOOK = None
                self.assertTrue(stats._install_stop_hook())
                installed_callback = route.callback
                self.assertTrue(stats._install_stop_hook())
                self.assertIs(installed_callback, route.callback)
                result = route.callback(Request("ca_display_id", is_admin=True))
                self.assertEqual(200, result.status_code)
                revoke.assert_called_once_with("ca_display_id")

                revoke.reset_mock()
                denied = route.callback(Request("ca_other_id", is_admin=False))
                self.assertEqual(403, denied.status_code)
                revoke.assert_not_called()

                native = route.callback(Request("native-session-id", is_admin=True))
                self.assertEqual(404, native.status_code)
                self.assertEqual("native-session-id", calls[-1])
                revoke.assert_not_called()
        finally:
            stats._STOP_HOOK = old_hook


if __name__ == "__main__":
    unittest.main()
