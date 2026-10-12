import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
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


class NativeArchiveCoverageTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        self.store = archive_store_module.ArchiveStore(self.root)
        self.now = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc).timestamp()
        self.start = self.now - 20
        self.end = self.now + 10
        self.programme_end = self.now + 60

    def add_segment(self, start, end):
        path = self.root / f"segment-{len(list(self.root.glob('*.ts')))}.ts"
        path.write_bytes(b"synthetic transport stream")
        self.store.add_segment(
            "channel-synthetic",
            path,
            datetime.fromtimestamp(start, timezone.utc),
            datetime.fromtimestamp(end, timezone.utc),
        )

    def available(self, end=None, programme_end=None):
        return views._native_archive_coverage_complete(
            self.store,
            "channel-synthetic",
            self.start,
            self.end if end is None else end,
            self.now,
            self.programme_end if programme_end is None else programme_end,
        )

    def test_current_window_allows_only_the_unfinalized_trailing_tail(self):
        self.add_segment(self.start, self.now - 5)
        self.assertTrue(self.available())

        self.add_segment(self.now - 5, self.now)
        self.assertTrue(self.available())

    def test_current_window_rejects_leading_and_internal_gaps(self):
        self.add_segment(self.start + 2, self.now)
        self.assertFalse(self.available())

        self.store = archive_store_module.ArchiveStore(self.root / "internal")
        self.add_segment(self.start, self.start + 5)
        self.add_segment(self.start + 6, self.now)
        self.assertFalse(self.available())

    def test_current_window_without_segments_is_unavailable(self):
        self.assertFalse(self.available())

    def test_completed_windows_require_full_coverage(self):
        completed_end = self.now - 2
        self.add_segment(self.start, completed_end)
        self.assertTrue(self.available(end=completed_end))

        self.store = archive_store_module.ArchiveStore(self.root / "partial")
        self.add_segment(self.start, completed_end - 5)
        self.assertFalse(self.available(end=completed_end))

    def test_manifest_programme_boundary_and_requested_window_bound_segments(self):
        bounded_end = self.now - 2
        # A segment crossing the programme boundary is excluded by the shared
        # HLS manifest builder even when it overlaps the requested window.
        self.add_segment(self.start, bounded_end + 1)
        self.assertFalse(self.available(end=bounded_end, programme_end=bounded_end))

        self.store = archive_store_module.ArchiveStore(self.root / "bounded")
        self.add_segment(self.start, bounded_end)
        self.assertTrue(self.available(end=bounded_end, programme_end=bounded_end))


if __name__ == "__main__":
    unittest.main()
