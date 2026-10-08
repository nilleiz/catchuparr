import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr.engine.store import ArchiveStore
from catchuparr.runtime import require_supported_version, status


class RuntimeStatusTests(unittest.TestCase):
    def test_status_reports_last_segment_and_storage_usage(self):
        channel = "33ef4df1-b5b2-4e0c-a1c4-97f6ae6dbb47"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.ts"
            source.write_bytes(b"transport stream")
            store = ArchiveStore(root / "archive")
            start = datetime(2026, 10, 5, tzinfo=timezone.utc)
            segment = store.add_segment(channel, source, start, start + timedelta(seconds=6))
            result = status({
                "channel_uuids": channel,
                "archive_root": str(store.root),
                "retention_hours": 2,
                "max_storage_gib": 5,
            })
        self.assertEqual(segment.end_utc.isoformat(), result["channels"][0]["latest_end_utc"])
        self.assertEqual(1, result["channels"][0]["segments"])
        self.assertEqual(len(b"transport stream"), result["indexed_storage_bytes"])
        self.assertIn("recorder_running", result["channels"][0])

    def test_runtime_version_matrix(self):
        for version in ("0.31.0", "0.32.0"):
            with self.subTest(version=version), patch.dict(
                sys.modules, {"version": types.SimpleNamespace(__version__=version)}
            ):
                require_supported_version()
        with patch.dict(
            sys.modules, {"version": types.SimpleNamespace(__version__="0.33.0")}
        ):
            with self.assertRaisesRegex(RuntimeError, "Unsupported Dispatcharr version: 0.33.0"):
                require_supported_version()

    def test_no_active_snapshot_never_falls_back_to_legacy_draft_channels(self):
        apps = types.ModuleType("apps")
        plugins = types.ModuleType("apps.plugins")
        models = types.ModuleType("apps.plugins.models")

        class Manager:
            def filter(self, **kwargs):
                return self

            def first(self):
                return SimpleNamespace(
                    enabled=True,
                    settings={"channel_uuids": "00000000-0000-0000-0000-000000000001"},
                )

        models.PluginConfig = type("PluginConfig", (), {"objects": Manager()})
        plugins.models = models
        apps.plugins = plugins
        with patch.dict(sys.modules, {
            "apps": apps,
            "apps.plugins": plugins,
            "apps.plugins.models": models,
        }), patch("catchuparr.configuration.reset_legacy_configuration", return_value=True), patch(
            "catchuparr.configuration.load_active_configuration", return_value=None
        ):
            from catchuparr.runtime import load_config

            self.assertIsNone(load_config())

    def test_status_explains_that_apply_is_required_after_legacy_reset(self):
        with tempfile.TemporaryDirectory() as directory, patch(
            "catchuparr.configuration.configuration_reset_required", return_value=True
        ):
            result = status({
                "channel_uuids": "",
                "archive_root": directory,
                "retention_hours": 2,
                "max_storage_gib": 5,
            })
        self.assertEqual([], result["channels"])
        self.assertIn("cleared", result["configuration_status"])
        self.assertIn("apply filter_config", result["configuration_status"])


if __name__ == "__main__":
    unittest.main()
