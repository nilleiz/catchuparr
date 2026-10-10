import importlib
import logging
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
    def _status_with_celery_probe(self, active, *, paused=False, schedule=None, probe_error=None):
        channel = "33ef4df1-b5b2-4e0c-a1c4-97f6ae6dbb47"
        celery = types.ModuleType("celery")

        class Inspector:
            def active(self):
                if probe_error is not None:
                    raise probe_error
                return active

        celery.current_app = SimpleNamespace(
            control=SimpleNamespace(inspect=lambda **_kwargs: Inspector())
        )
        core = types.ModuleType("core")
        core_utils = types.ModuleType("core.utils")

        class Redis:
            def ping(self):
                return True

        core_utils.RedisClient = SimpleNamespace(get_client=lambda: Redis())
        core.utils = core_utils
        schedule = schedule or {"timezone": "UTC", "channels": {channel: {"mode": "continuous"}}}
        with tempfile.TemporaryDirectory() as directory, patch.dict(sys.modules, {
            "celery": celery,
            "core": core,
            "core.utils": core_utils,
        }), patch(
            "catchuparr.configuration.configuration_reset_required", return_value=False
        ):
            return status({
                "version": 1,
                "channel_uuids": channel,
                "archive_root": directory,
                "retention_hours": 24,
                "max_storage_gib": 5,
                "recording_schedule": schedule,
            }, control_state=RecorderControlState(paused, 1))

    def test_public_base_url_normalization_rejects_untrusted_url_parts(self):
        from catchuparr.runtime import normalize_public_base_url

        self.assertEqual(
            "https://media.example.test/dispatcharr",
            normalize_public_base_url("https://media.example.test/dispatcharr/"),
        )
        for value in (
            "https://media.example.test",
            "https://media.example.test/",
        ):
            with self.subTest(value=value):
                normalized = normalize_public_base_url(value)
                self.assertEqual("https://media.example.test", normalized)
                self.assertEqual(normalized, normalize_public_base_url(normalized))
        for value in (
            "",
            "ftp://media.example.test",
            "https://user:pass@media.example.test",
            "https://media.example.test/?query=1",
            "https://media.example.test/#fragment",
            "https://media.example.test/%2e%2e/elsewhere",
            "https://media.example.test/%3Cscript%3E",
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
        self.assertEqual(1, result["selected_channel_count"])
        self.assertIsNone(result["running_recorder_count"])
        self.assertGreaterEqual(result["archive_storage_bytes"], len(b"transport stream"))
        self.assertIn("in an unknown state", result["message"])

    def test_status_uses_active_celery_recording_tasks_not_recorder_leases(self):
        channel = "33ef4df1-b5b2-4e0c-a1c4-97f6ae6dbb47"
        running = self._status_with_celery_probe({"worker-a": [{
            "name": "catchuparr.record_channel", "args": [channel, "synthetic-config", 1],
        }]})
        stopped = self._status_with_celery_probe({"worker-a": []})

        self.assertTrue(running["channels"][0]["recorder_running"])
        self.assertEqual(1, running["running_recorder_count"])
        self.assertEqual("running", running["channels"][0]["recorder_reason"])
        self.assertIn("Catchuparr is running", running["message"])
        self.assertFalse(stopped["channels"][0]["recorder_running"])
        self.assertEqual(0, stopped["running_recorder_count"])
        self.assertEqual("eligible", stopped["channels"][0]["recorder_reason"])
        self.assertIn("Catchuparr is stopped", stopped["message"])

    def test_status_reports_unknown_and_error_when_celery_state_is_unavailable(self):
        unknown = self._status_with_celery_probe(None)
        errored = self._status_with_celery_probe({}, probe_error=RuntimeError("synthetic inspector failure"))

        for result in (unknown, errored):
            with self.subTest(result=result):
                self.assertIsNone(result["channels"][0]["recorder_running"])
                self.assertIsNone(result["running_recorder_count"])
                self.assertEqual("error_recorder_state_unavailable", result["channels"][0]["recorder_reason"])
                self.assertIn("in an unknown state", result["message"])

    def test_status_reports_schedule_and_pause_eligibility_reasons(self):
        channel = "33ef4df1-b5b2-4e0c-a1c4-97f6ae6dbb47"
        outside_schedule = self._status_with_celery_probe(
            {"worker-a": []},
            schedule={"timezone": "UTC", "channels": {channel: {
                "mode": "weekly", "intervals": [],
            }}},
        )
        paused = self._status_with_celery_probe({"worker-a": []}, paused=True)

        self.assertFalse(outside_schedule["channels"][0]["recording_scheduled"])
        self.assertEqual("outside_schedule", outside_schedule["channels"][0]["recorder_reason"])
        self.assertEqual("paused", paused["channels"][0]["recorder_reason"])

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
        self.assertEqual("paused", result["channels"][0]["recorder_reason"])
        self.assertIn("segments", result["channels"][0])

    def test_runtime_state_denies_recording_when_applied_control_is_unavailable(self):
        from catchuparr import runtime
        from catchuparr.recorder_control import RecorderControlError

        active = {
            "archive_root": "/tmp/synthetic-active-root",
            "retention_hours": 48,
            "max_storage_gib": 12,
            "recording_enabled": True,
            "log_level": "INFO",
            "channel_uuids": "",
            "channel_profile_ids": [],
            "source_policies": {},
        }
        with patch("catchuparr.configuration.reset_legacy_configuration"), patch(
            "catchuparr.configuration.load_applied_state",
            side_effect=RecorderControlError("synthetic pending deny marker"),
        ), patch("catchuparr.configuration.load_active_configuration", return_value=active):
            settings, control = runtime.load_runtime_state()

        self.assertIsNone(control)
        self.assertFalse(settings["recording_enabled"])
        self.assertEqual(48, settings["retention_hours"])

    def test_runtime_apply_returns_explicit_recovery_required_result(self):
        from catchuparr import runtime
        configuration = importlib.import_module("catchuparr.configuration")
        logging_utils = importlib.import_module("catchuparr.logging_utils")
        from catchuparr.recorder_control import RecorderControlError

        result = {
            "applied": False,
            "activation_pending": True,
            "recovery_required": True,
            "selected_channel_count": 2,
            "source_policy_count": 1,
            "recording_paused": True,
            "warning": "Apply did not complete; the previous settings remain active and recorder admission is denied until activation recovery succeeds.",
            "message": "Apply did not complete; the previous settings remain active and recorder admission is denied until activation recovery succeeds.",
        }
        with patch.object(configuration, "apply_configuration", return_value=result) as apply, patch.object(
            configuration,
            "load_applied_state",
            side_effect=RecorderControlError("synthetic pending deny marker"),
        ), patch.object(
            configuration,
            "load_active_configuration",
            return_value={"log_level": "INFO"},
        ), patch.object(logging_utils, "apply_log_level"), patch.object(
            logging_utils, "event"
        ) as event:
            returned = runtime.apply_configuration()

        apply.assert_called_once_with(None)
        self.assertFalse(returned["applied"])
        self.assertTrue(returned["activation_pending"])
        self.assertTrue(returned["recovery_required"])
        self.assertTrue(returned["recording_paused"])
        self.assertIn("previous settings remain active", returned["warning"])
        event.assert_called_once_with(
            "configuration_activation_pending",
            logging.WARNING,
            activation_pending=True,
            recovery_required=True,
            recording_paused=True,
        )

    def test_runtime_apply_emits_success_only_for_confirmed_apply(self):
        from catchuparr import runtime
        configuration = importlib.import_module("catchuparr.configuration")
        logging_utils = importlib.import_module("catchuparr.logging_utils")
        result = {
            "applied": True,
            "selected_channel_count": 2,
            "source_policy_count": 1,
            "recording_paused": False,
        }
        with patch.object(configuration, "apply_configuration", return_value=result), patch.object(
            configuration,
            "load_applied_state",
            return_value=({"log_level": "INFO"}, object()),
        ), patch.object(logging_utils, "apply_log_level"), patch.object(
            logging_utils, "event"
        ) as event:
            returned = runtime.apply_configuration()

        self.assertTrue(returned["applied"])
        event.assert_called_once_with(
            "configuration_applied",
            channel_count=2,
            source_policy_count=1,
            recording_paused=False,
        )

    def test_runtime_apply_logs_unknown_and_recovery_required_without_success_event(self):
        from catchuparr import runtime
        configuration = importlib.import_module("catchuparr.configuration")
        logging_utils = importlib.import_module("catchuparr.logging_utils")
        from catchuparr.recorder_control import RecorderControlError

        cases = (
            (
                {
                    "applied": False,
                    "outcome_unknown": True,
                    "activation_pending": True,
                    "recovery_required": True,
                    "recording_paused": True,
                },
                "configuration_outcome_unknown",
                {"outcome_unknown": True, "recovery_required": True},
            ),
            (
                {"applied": False, "recovery_required": True, "recording_paused": True},
                "configuration_recovery_required",
                {"recovery_required": True},
            ),
        )
        for result, expected_event, event_flags in cases:
            with self.subTest(expected_event=expected_event):
                with patch.object(
                    configuration, "apply_configuration", return_value=result
                ), patch.object(
                    configuration,
                    "load_applied_state",
                    side_effect=RecorderControlError("synthetic denied state"),
                ), patch.object(
                    configuration,
                    "load_active_configuration",
                    return_value={"log_level": "INFO"},
                ), patch.object(logging_utils, "apply_log_level"), patch.object(
                    logging_utils, "event"
                ) as event:
                    returned = runtime.apply_configuration()

                self.assertFalse(returned["applied"])
                event.assert_called_once_with(
                    expected_event,
                    logging.WARNING,
                    recording_paused=True,
                    **event_flags,
                )
                self.assertNotEqual("configuration_applied", event.call_args.args[0])

    def test_bootstrap_logs_allowlisted_recovery_failure(self):
        from catchuparr import runtime
        configuration = importlib.import_module("catchuparr.configuration")
        celery = types.ModuleType("celery")

        def fake_shared_task(**_kwargs):
            def decorate(function):
                return function

            return decorate

        celery.shared_task = fake_shared_task

        with patch.dict(sys.modules, {"celery": celery}), patch.object(
            runtime, "apply_committed_log_level"
        ), patch.object(
            runtime, "require_supported_version"
        ), patch.object(
            configuration,
            "recover_interrupted_activation",
            side_effect=RuntimeError("synthetic recovery failure"),
        ), patch.object(
            configuration, "reset_legacy_configuration"
        ) as reset, patch.object(runtime, "_ensure_schedule"), patch.object(
            sys, "argv", ["celery"]
        ), self.assertLogs("catchuparr", level="ERROR") as captured:
            runtime.bootstrap()

        reset.assert_not_called()
        self.assertIn(
            "[Catchuparr] configuration_recovery_failed",
            [record.getMessage() for record in captured.records],
        )


if __name__ == "__main__":
    unittest.main()
