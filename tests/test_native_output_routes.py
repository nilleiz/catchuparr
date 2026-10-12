import sys
import types
import unittest
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


if __name__ == "__main__":
    unittest.main()
