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

    def call_archive(self, method="GET"):
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
            patch.object(views, "_archive_window_live", return_value=False),
            patch.object(views, "_record_native_playback"),
            patch.object(views, "_native_client_ip", return_value="192.0.2.10"),
        ):
            return views._native_archive_response(self.request, None, None)

    def test_get_delegates_sparse_window_to_shared_playlist_service(self):
        response = self.call_archive()

        self.assertEqual(response.status_code, 200)
        self.service.playlist.assert_called_once()
        self.service.store.segments.assert_not_called()

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
            programme_end=self.requested_end + 60,
        )
        self.service.playlist.assert_not_called()

    def test_head_preserves_empty_manifest_not_found(self):
        self.service._manifest_segments.return_value = []

        response = self.call_archive("HEAD")

        self.assertEqual(response.status_code, 404)
        self.service.playlist.assert_not_called()


if __name__ == "__main__":
    unittest.main()
