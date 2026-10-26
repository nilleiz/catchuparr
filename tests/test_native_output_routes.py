import sys
import types
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

from catchuparr import views
from catchuparr.engine import store as archive_store_module


class NativeArchiveServiceTests(unittest.TestCase):
    def test_service_uses_dispatcharr_core_redis_client_export(self):
        apps = types.ModuleType("apps")
        channels = types.ModuleType("apps.channels")
        channel_utils = types.ModuleType("apps.channels.utils")
        channel_utils.is_catchup_enabled = Mock(return_value=True)
        proxy = types.ModuleType("apps.proxy")
        proxy_utils = types.ModuleType("apps.proxy.utils")
        proxy_utils.get_user_active_connections = Mock(return_value=[])
        dispatcharr = types.ModuleType("dispatcharr")
        dispatcharr_utils = types.ModuleType("dispatcharr.utils")
        # Exact supported Dispatcharr releases export this helper from
        # core.utils, not dispatcharr.utils.
        dispatcharr_utils.get_client_ip = Mock(return_value="192.0.2.9")
        core = types.ModuleType("core")
        core_utils = types.ModuleType("core.utils")
        redis_client = Mock()
        core_utils.RedisClient = SimpleNamespace(get_client=Mock(return_value=redis_client))
        channels.utils = channel_utils
        proxy.utils = proxy_utils
        apps.channels = channels
        apps.proxy = proxy
        dispatcharr.utils = dispatcharr_utils
        core.utils = core_utils

        service_result = object()
        archive_store_type = Mock(return_value=object())
        service_type = Mock(return_value=service_result)
        native_identity = Mock(return_value="native:synthetic")
        active_ts_count = Mock(return_value=0)
        modules = {
            "apps": apps,
            "apps.channels": channels,
            "apps.channels.utils": channel_utils,
            "apps.proxy": proxy,
            "apps.proxy.utils": proxy_utils,
            "dispatcharr": dispatcharr,
            "dispatcharr.utils": dispatcharr_utils,
            "core": core,
            "core.utils": core_utils,
        }
        with (
            patch.dict(sys.modules, modules),
            patch.object(archive_store_module, "ArchiveStore", archive_store_type),
            patch("catchuparr.http.ArchiveHTTPService", service_type),
            patch("catchuparr.native_output_security.native_device_identity", native_identity),
            patch("catchuparr.xc_runtime.active_ts_session_count", active_ts_count),
        ):
            request = SimpleNamespace(
                META={"HTTP_USER_AGENT": "Synthetic Client/1", "REMOTE_ADDR": "192.0.2.9"}
            )
            user = SimpleNamespace(id=7, stream_limit=0)
            claims = {"channel": "channel-id", "profile": None, "nonce": "synthetic"}
            result = views._native_archive_service(
                request,
                {},
                SimpleNamespace(archive_root="/tmp/synthetic-native-archive"),
                user,
                claims,
                b"k" * 32,
                "synthetic-capability",
            )

        self.assertIs(result, service_result)
        archive_store_type.assert_called_once_with("/tmp/synthetic-native-archive")
        service_type.assert_called_once()
        kwargs = service_type.call_args.kwargs
        self.assertEqual(kwargs["base_path"], "/catchuparr/native")
        self.assertEqual(
            kwargs["device_identity"]("synthetic-capability", "7", "channel-id"),
            "native:synthetic",
        )
        self.assertIs(core_utils.RedisClient.get_client.return_value, redis_client)


class NativeArchiveParityTests(unittest.TestCase):
    class Response:
        def __init__(self, content=b"", status=200, content_type=None):
            self.content = content
            self.status_code = status
            self.headers = {}
            if content_type:
                self.headers["Content-Type"] = content_type

        def __setitem__(self, key, value):
            self.headers[key] = value

    def setUp(self):
        self.now = datetime.now(timezone.utc).timestamp()
        self.start = self.now - 120
        self.duration = 60
        self.requested_end = self.start + self.duration
        self.user = SimpleNamespace(id=7)
        self.claims = {"channel": "channel-synthetic"}
        self.candidates = [object()]
        self.service = SimpleNamespace(
            store=SimpleNamespace(segments=Mock(return_value=self.candidates)),
            _manifest_segments=Mock(return_value=[object()]),
            playlist=Mock(return_value=SimpleNamespace(status=200, headers={}, body=b"#EXTM3U")),
        )
        self.context = ({}, SimpleNamespace(retention_hours=48), self.user, self.claims, b"k" * 32)
        self.request = SimpleNamespace(
            method="GET",
            GET={
                "token": "synthetic",
                "channel_id": "channel-synthetic",
                "utc": str(self.start),
                "duration": str(self.duration),
            },
            headers={},
            META={},
        )
        django_http = types.ModuleType("django.http")
        django_http.HttpResponse = self.Response
        self.django_modules = {"django": types.ModuleType("django"), "django.http": django_http}

    def call_archive(self, method="GET", live=False):
        self.request.method = method
        with (
            patch.dict(sys.modules, self.django_modules),
            patch.object(views, "_native_playback_context", return_value=self.context),
            patch.object(views, "_native_archive_service", return_value=self.service),
            patch.object(
                views,
                "_archive_epg_bounds",
                return_value=(self.requested_end + 60, None),
            ),
            patch.object(
                views,
                "_archive_playback_window",
                return_value=(self.requested_end, None),
            ),
            patch.object(views, "_archive_window_live", return_value=live),
            patch.object(views, "_record_native_playback"),
            patch.object(views, "_native_client_ip", return_value="192.0.2.10"),
        ):
            return views._native_archive_response(self.request, None, None)

    def test_get_delegates_sparse_window_to_shared_playlist_service(self):
        response = self.call_archive()

        self.assertEqual(response.status_code, 200)
        self.service.playlist.assert_called_once()
        self.service.store.segments.assert_not_called()
        args = self.service.playlist.call_args
        self.assertEqual(self.requested_end, args.args[3])
        self.assertFalse(args.kwargs["live"])
        self.assertFalse(args.kwargs["continuous"])

    def test_unexpanded_duration_uses_server_epg_and_never_requires_client_replacement(self):
        self.request.GET["duration"] = "{duration}"
        self.request.GET["lutc"] = str(int(self.now))
        response = self.call_archive()
        self.assertEqual(200, response.status_code)
        self.assertEqual(self.requested_end + 60, self.service.playlist.call_args.args[3])

    def test_missing_duration_and_malformed_duration_are_distinguished(self):
        self.request.GET.pop("duration")
        self.assertEqual(200, self.call_archive().status_code)
        self.service.playlist.reset_mock()
        self.request.GET["duration"] = "bad"
        self.assertEqual(400, self.call_archive().status_code)
        self.service.playlist.assert_not_called()

    def test_continuous_playback_requires_explicit_opt_in(self):
        self.request.GET["playback"] = "continuous"
        self.assertEqual(200, self.call_archive().status_code)
        self.assertTrue(self.service.playlist.call_args.kwargs["continuous"])
        self.assertEqual(self.start + 86400, self.service.playlist.call_args.args[3])

    def test_actual_manifest_contains_requested_archive_not_later_live_segments(self):
        import tempfile
        from datetime import timedelta
        from pathlib import Path

        from catchuparr.engine.store import ArchiveStore
        from catchuparr.http import ArchiveHTTPService

        with tempfile.TemporaryDirectory() as temp:
            store = ArchiveStore(Path(temp))
            old_ids, live_ids = [], []
            for offset in (0, 6, 12, 102, 108):
                stamp = datetime.fromtimestamp(self.start + offset, timezone.utc)
                path = Path(temp) / f"synthetic-{offset}.ts"
                path.write_bytes(b"\x47" + bytes([offset]) * 187)
                segment = store.add_segment("channel-synthetic", path, stamp,
                                            stamp + timedelta(seconds=6))
                (old_ids if offset < 60 else live_ids).append(str(segment.id))
            self.service = ArchiveHTTPService(
                store, SimpleNamespace(lookup=lambda token: "7" if token == "synthetic" else None),
                authorize_user_channel=lambda *_: True, catchup_enabled=lambda *_: True,
            )
            response = self.call_archive()
            self.assertEqual(200, response.status_code)
            text = response.content.decode()
            self.assertIn("#EXT-X-ENDLIST", text)
            self.assertIn("#EXT-X-START:TIME-OFFSET=0.000", text)
            for segment_id in old_ids:
                self.assertIn(f"/{segment_id}?", text)
            for segment_id in live_ids:
                self.assertNotIn(f"/{segment_id}?", text)

    def test_running_programme_appends_without_exposing_the_next_programme(self):
        import tempfile
        from datetime import timedelta
        from pathlib import Path

        from catchuparr.engine.store import ArchiveStore
        from catchuparr.http import ArchiveHTTPService

        with tempfile.TemporaryDirectory() as directory:
            store = ArchiveStore(Path(directory))
            self.duration = 3600
            self.requested_end = self.start + self.duration
            self.request.GET["duration"] = str(self.duration)
            self.service = ArchiveHTTPService(
                store, SimpleNamespace(lookup=lambda token: "7"),
                authorize_user_channel=lambda *_: True, catchup_enabled=lambda *_: True,
            )
            def add(offset):
                stamp = datetime.fromtimestamp(self.start + offset, timezone.utc)
                path = Path(directory) / f"synthetic-{offset}.ts"
                path.write_bytes(b"\x47" + bytes(187))
                return store.add_segment("channel-synthetic", path, stamp,
                                         stamp + timedelta(seconds=6)).id
            first = add(0)
            initial = self.call_archive(live=True).content.decode()
            self.assertNotIn("#EXT-X-ENDLIST", initial)
            second = add(6)
            later_programme = add(3606)
            updated = self.call_archive(live=True).content.decode()
            for identity in (first, second):
                self.assertIn(f"/{identity}?", updated)
            self.assertNotIn(f"/{later_programme}?", updated)
            self.assertNotIn("#EXT-X-ENDLIST", updated)
            closed = self.call_archive(live=False).content.decode()
            self.assertIn("#EXT-X-ENDLIST", closed)

    def test_head_checks_shared_manifest_without_creating_a_playlist(self):
        response = self.call_archive("HEAD")

        self.assertEqual(response.status_code, 200)
        self.service.store.segments.assert_called_once_with(
            "channel-synthetic", self.start, self.requested_end
        )
        self.service._manifest_segments.assert_called_once_with(
            "channel-synthetic",
            "[]",
            self.candidates,
            playback_end=self.requested_end,
        )
        self.service.playlist.assert_not_called()

    def test_head_preserves_empty_manifest_not_found(self):
        self.service._manifest_segments.return_value = []

        response = self.call_archive("HEAD")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.content, b"")
        self.service.playlist.assert_not_called()


if __name__ == "__main__":
    unittest.main()
