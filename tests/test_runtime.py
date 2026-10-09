import importlib
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr.engine.store import ArchiveStore
from catchuparr.recorder_control import RecorderControlState
from catchuparr.runtime import require_supported_version, status


class RuntimeStatusTests(unittest.TestCase):
    def test_public_base_url_normalization_rejects_untrusted_url_parts(self):
        from catchuparr.runtime import normalize_public_base_url

        self.assertEqual(
            "https://media.example.test/dispatcharr",
            normalize_public_base_url("https://media.example.test/dispatcharr/"),
        )
        for value in (
            "",
            "ftp://media.example.test",
            "https://user:pass@media.example.test",
            "https://media.example.test/?query=1",
            "https://media.example.test/#fragment",
            "https://media.example.test/%2e%2e/elsewhere",
        ):
            if value == "":
                continue
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "public_base_url"):
                normalize_public_base_url(value)

    def test_create_access_token_returns_two_labeled_copyable_authorized_urls(self):
        from catchuparr import runtime

        apps = types.ModuleType("apps")
        accounts = types.ModuleType("apps.accounts")
        models = types.ModuleType("apps.accounts.models")

        class Query:
            def filter(self, **kwargs):
                self.kwargs = kwargs
                return self

            def exists(self):
                return self.kwargs == {"id": 9}

        models.User = type("User", (), {"objects": Query()})
        accounts.models = models
        apps.accounts = accounts

        issued = []

        class Store:
            def __init__(self, root):
                self.root = Path(root)

            def issue(self, user_id):
                issued.append((self.root, user_id))
                return "synthetic/token+value"

        settings = {
            "archive_root": "/tmp/synthetic-catchuparr",
            "playback_user_id": 9,
            "public_base_url": "https://media.example.test/dispatcharr/",
        }
        with patch.dict(
            sys.modules,
            {
                "apps": apps,
                "apps.accounts": accounts,
                "apps.accounts.models": models,
            },
        ), patch.object(runtime, "require_supported_version"), patch(
            "catchuparr.security.AccessTokenStore", Store
        ):
            result = runtime.create_access_token(settings)

        self.assertEqual(
            "https://media.example.test/dispatcharr/catchuparr/m3u"
            "?access_token=synthetic%2Ftoken%2Bvalue",
            result["playlist_url"],
        )
        self.assertEqual(
            "https://media.example.test/dispatcharr/catchuparr/xmltv"
            "?access_token=synthetic%2Ftoken%2Bvalue",
            result["xmltv_url"],
        )
        self.assertEqual(
            "M3U playlist URL: "
            + result["playlist_url"]
            + "\nXMLTV EPG URL: "
            + result["xmltv_url"],
            result["message"],
        )
        self.assertEqual([(Path("/tmp/synthetic-catchuparr"), 9)], issued)

        settings_without_base = dict(settings)
        settings_without_base.pop("public_base_url")
        with patch.dict(
            sys.modules,
            {
                "apps": apps,
                "apps.accounts": accounts,
                "apps.accounts.models": models,
            },
        ), patch.object(runtime, "require_supported_version"), patch(
            "catchuparr.security.AccessTokenStore", Store
        ), self.assertRaisesRegex(ValueError, "public_base_url"):
            runtime.create_access_token(settings_without_base)
        self.assertEqual([(Path("/tmp/synthetic-catchuparr"), 9)], issued)

    def test_status_reports_last_segment_and_storage_usage(self):
        channel = "33ef4df1-b5b2-4e0c-a1c4-97f6ae6dbb47"
        with tempfile.TemporaryDirectory() as directory, patch(
            "catchuparr.recorder_control.load_recorder_control",
            return_value=RecorderControlState(False, 0),
        ):
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
            }, control_state=RecorderControlState(False, 0))
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
            "catchuparr.configuration.load_applied_state",
            return_value=(None, RecorderControlState(False, 0)),
        ):
            from catchuparr.runtime import load_config

            self.assertIsNone(load_config())

    def test_load_config_derives_runtime_settings_from_supplied_snapshot(self):
        apps = types.ModuleType("apps")
        plugins = types.ModuleType("apps.plugins")
        models = types.ModuleType("apps.plugins.models")

        class Manager:
            def filter(self, **kwargs):
                self.kwargs = kwargs
                return self

            def first(self):
                return SimpleNamespace(enabled=True)

        models.PluginConfig = type("PluginConfig", (), {"objects": Manager()})
        plugins.models = models
        apps.plugins = plugins
        channel = "33ef4df1-b5b2-4e0c-a1c4-97f6ae6dbb47"
        active = {
            "channel_uuids": channel,
            "archive_root": "/tmp/synthetic-active-root",
            "retention_hours": 72,
            "max_storage_gib": 8,
        }
        with patch.dict(sys.modules, {
            "apps": apps,
            "apps.plugins": plugins,
            "apps.plugins.models": models,
        }), patch(
            "catchuparr.configuration.load_active_configuration",
            side_effect=AssertionError("active snapshot was reread"),
        ):
            from catchuparr.runtime import load_config

            config = load_config(active_snapshot=active)
        self.assertEqual((channel,), config.channel_uuids)
        self.assertEqual(Path("/tmp/synthetic-active-root"), config.archive_root)
        self.assertEqual(72, config.retention_hours)

    def test_status_explains_that_apply_is_required_after_legacy_reset(self):
        configuration = importlib.import_module("catchuparr.configuration")
        with tempfile.TemporaryDirectory() as directory, patch.object(
            configuration, "configuration_reset_required", return_value=True
        ):
            result = status({
                "channel_uuids": "",
                "archive_root": directory,
                "retention_hours": 2,
                "max_storage_gib": 5,
            }, control_state=RecorderControlState(False, 0))
        self.assertEqual([], result["channels"])
        self.assertIn("cleared", result["configuration_status"])
        self.assertIn("apply filter_config", result["configuration_status"])

    def test_status_exposes_recording_control_without_affecting_archive_stats(self):
        channel = "33ef4df1-b5b2-4e0c-a1c4-97f6ae6dbb47"
        with tempfile.TemporaryDirectory() as directory:
            result = status({
                "channel_uuids": channel,
                "archive_root": directory,
                "retention_hours": 2,
                "max_storage_gib": 5,
                "recording_schedule": {
                    "timezone": "UTC",
                    "channels": {channel: {"mode": "continuous"}},
                },
            }, control_state=RecorderControlState(True, 8))
        self.assertFalse(result["recording_enabled"])
        self.assertEqual(8, result["control_generation"])
        self.assertTrue(result["recording_control_available"])
        self.assertTrue(result["channels"][0]["recording_scheduled"])
        self.assertIn("segments", result["channels"][0])


if __name__ == "__main__":
    unittest.main()
