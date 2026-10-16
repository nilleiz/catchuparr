import unittest
from copy import deepcopy

from scripts.aio_051_configuration_probe import (
    should_run_probe,
    validate_active_v6_snapshot,
)


class ActiveConfigurationProbeSchemaTests(unittest.TestCase):
    def test_probe_runs_as_script_or_django_shell_stdin_but_not_import(self):
        self.assertTrue(should_run_probe("__main__"))
        self.assertTrue(should_run_probe("django.core.management.commands.shell"))
        self.assertFalse(should_run_probe("scripts.aio_051_configuration_probe"))

    def setUp(self):
        self.channels = ("synthetic-channel-a", "synthetic-channel-b")
        self.defaults = {
            "enabled": False,
            "live_stability_seconds": 5,
            "last_viewer_return_seconds": 30,
            "min_switch_interval_seconds": 60,
        }
        sharing = {channel: dict(self.defaults) for channel in self.channels}
        self.active = {"version": 6, "connection_sharing": deepcopy(sharing)}
        self.persisted = {"version": 6, "connection_sharing": deepcopy(sharing)}

    def test_accepts_v6_snapshot_with_disabled_sharing_defaults(self):
        validate_active_v6_snapshot(self.active, self.persisted, self.channels)

    def test_rejects_stale_v5_schema(self):
        self.active["version"] = 5
        with self.assertRaisesRegex(RuntimeError, "version 6"):
            validate_active_v6_snapshot(self.active, self.persisted, self.channels)

    def test_rejects_float_schema_version(self):
        self.active["version"] = 6.0
        with self.assertRaisesRegex(RuntimeError, "version 6"):
            validate_active_v6_snapshot(self.active, self.persisted, self.channels)

    def test_rejects_missing_channel_and_enabled_default(self):
        self.active["connection_sharing"].pop(self.channels[1])
        with self.assertRaisesRegex(RuntimeError, "every selected channel"):
            validate_active_v6_snapshot(self.active, self.persisted, self.channels)
        self.active["connection_sharing"] = {
            channel: dict(self.defaults) for channel in self.channels
        }
        self.active["connection_sharing"][self.channels[0]]["enabled"] = True
        with self.assertRaisesRegex(RuntimeError, "strict disabled sharing defaults"):
            validate_active_v6_snapshot(self.active, self.persisted, self.channels)

    def test_rejects_integer_boolean_and_float_cooldown_coercions(self):
        malformed_values = (("enabled", 0), ("live_stability_seconds", 5.0))
        for field, malformed in malformed_values:
            with self.subTest(field=field):
                self.active["connection_sharing"] = {
                    channel: dict(self.defaults) for channel in self.channels
                }
                self.persisted["connection_sharing"] = {
                    channel: dict(self.defaults) for channel in self.channels
                }
                self.active["connection_sharing"][self.channels[0]][field] = malformed
                with self.assertRaisesRegex(RuntimeError, "strict disabled sharing defaults"):
                    validate_active_v6_snapshot(self.active, self.persisted, self.channels)

    def test_rejects_malformed_raw_cooldown_even_when_active_view_is_valid(self):
        self.persisted["connection_sharing"][self.channels[0]]["min_switch_interval_seconds"] = 60.0
        with self.assertRaisesRegex(RuntimeError, "raw v6 snapshot"):
            validate_active_v6_snapshot(self.active, self.persisted, self.channels)

    def test_rejects_raw_snapshot_mismatch(self):
        self.persisted["connection_sharing"] = {
            channel: dict(self.defaults) for channel in self.channels
        }
        self.persisted["connection_sharing"][self.channels[0]]["enabled"] = True
        with self.assertRaisesRegex(RuntimeError, "raw v6 snapshot"):
            validate_active_v6_snapshot(self.active, self.persisted, self.channels)


if __name__ == "__main__":
    unittest.main()
