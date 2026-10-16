import unittest
from copy import deepcopy

from catchuparr.configuration import _active_configuration_view
from catchuparr.sharing_planner import (
    BorrowTarget,
    SharingAction,
    SharingObservation,
    SharingSettings,
    observe,
    resolve_transition,
)
from scripts.aio_051_configuration_probe import (
    should_run_probe,
    validate_active_v7_snapshot,
)
from scripts.aio_recorder_media import (
    assert_applied_global_sharing,
    global_sharing_draft,
)


class ActiveConfigurationProbeSchemaTests(unittest.TestCase):
    def test_probe_runs_as_script_or_django_shell_stdin_but_not_import(self):
        self.assertTrue(should_run_probe("__main__"))
        self.assertTrue(should_run_probe("django.core.management.commands.shell"))
        self.assertFalse(should_run_probe("scripts.aio_051_configuration_probe"))

    def setUp(self):
        self.channels = ("synthetic-channel-a", "synthetic-channel-b")
        self.defaults = {
            "connection_sharing_enabled": False,
            "live_stability_seconds": 5,
            "last_viewer_return_seconds": 30,
            "min_switch_interval_seconds": 60,
        }
        self.runtime_defaults = {
            "enabled": False,
            "live_stability_seconds": 5,
            "last_viewer_return_seconds": 30,
            "min_switch_interval_seconds": 60,
        }
        self.persisted = {
            "version": 7,
            "settings": deepcopy(self.defaults),
            "channel_uuids": list(self.channels),
            "channel_profile_ids": [],
            "source_policies": {},
            "recording_schedule": {
                "timezone": "UTC",
                "channels": {channel: {"mode": "continuous"} for channel in self.channels},
            },
            "configuration_origins": {},
        }
        # Match load_active_configuration's real flattened view shape.
        self.active = _active_configuration_view(self.persisted)

    def test_accepts_v7_global_snapshot_and_runtime_expansion(self):
        validate_active_v7_snapshot(self.active, self.persisted, self.channels)

    def test_rejects_stale_v6_schema(self):
        self.active["version"] = 6
        with self.assertRaisesRegex(RuntimeError, "version 7"):
            validate_active_v7_snapshot(self.active, self.persisted, self.channels)

    def test_rejects_float_schema_version(self):
        self.active["version"] = 7.0
        with self.assertRaisesRegex(RuntimeError, "version 7"):
            validate_active_v7_snapshot(self.active, self.persisted, self.channels)

    def test_rejects_missing_channel_or_runtime_mismatch(self):
        self.active["connection_sharing"].pop(self.channels[1])
        with self.assertRaisesRegex(RuntimeError, "every selected channel"):
            validate_active_v7_snapshot(self.active, self.persisted, self.channels)
        self.active["connection_sharing"] = {
            channel: deepcopy(self.runtime_defaults) for channel in self.channels
        }
        self.active["connection_sharing"][self.channels[0]]["enabled"] = True
        with self.assertRaisesRegex(RuntimeError, "runtime expansion"):
            validate_active_v7_snapshot(self.active, self.persisted, self.channels)

    def test_rejects_malformed_global_values(self):
        malformed_values = (
            ("connection_sharing_enabled", 0),
            ("live_stability_seconds", 5.0),
        )
        for field, malformed in malformed_values:
            with self.subTest(field=field):
                for key, value in self.defaults.items():
                    self.active[key] = value
                self.persisted["settings"] = deepcopy(self.defaults)
                self.active[field] = malformed
                with self.assertRaisesRegex(RuntimeError, "global sharing defaults"):
                    validate_active_v7_snapshot(self.active, self.persisted, self.channels)

    def test_rejects_raw_per_channel_map_and_raw_setting_mismatch(self):
        self.persisted["connection_sharing"] = {
            channel: deepcopy(self.runtime_defaults) for channel in self.channels
        }
        with self.assertRaisesRegex(RuntimeError, "must not persist per-channel"):
            validate_active_v7_snapshot(self.active, self.persisted, self.channels)
        self.persisted.pop("connection_sharing")
        self.persisted["settings"]["connection_sharing_enabled"] = True
        with self.assertRaisesRegex(RuntimeError, "raw v7 snapshot"):
            validate_active_v7_snapshot(self.active, self.persisted, self.channels)

    def test_aio_global_gui_draft_stays_enabled_and_initial_attach_is_immediate(self):
        draft = global_sharing_draft(
            {"filter_config": "version: 1\n"}, enabled=True,
            live_stability_seconds=1, last_viewer_return_seconds=2,
            min_switch_interval_seconds=60,
        )
        self.assertEqual({
            "connection_sharing_enabled": True,
            "live_stability_seconds": 1,
            "last_viewer_return_seconds": 2,
            "min_switch_interval_seconds": 60,
        }, {key: draft[key] for key in (
            "connection_sharing_enabled", "live_stability_seconds",
            "last_viewer_return_seconds", "min_switch_interval_seconds",
        )})
        self.assertNotIn("connection_sharing_config", draft)

        channel = "synthetic-channel-a"
        runtime = {
            "enabled": True,
            "live_stability_seconds": draft["live_stability_seconds"],
            "last_viewer_return_seconds": draft["last_viewer_return_seconds"],
            "min_switch_interval_seconds": draft["min_switch_interval_seconds"],
        }
        active = {"version": 7, **draft, "connection_sharing": {channel: runtime}}
        assert_applied_global_sharing(
            active, channel, enabled=True, live_stability_seconds=1,
            last_viewer_return_seconds=2, min_switch_interval_seconds=60,
        )

        target = BorrowTarget(
            channel_uuid=channel, worker_uuid="worker-synthetic",
            owner_lock_value="owner-synthetic", metadata_owner="owner-synthetic",
            stream_id="stream-synthetic", account_id="account-synthetic",
            m3u_profile_id="profile-synthetic", input_profile_id="input-synthetic",
            ordinary_client_ids=("viewer-synthetic",), owner_heartbeat_healthy=True,
            assigned_stream_matches=True, account_active=True, profile_active=True,
            media_compatible=True, useful_buffer_available=True, native_state_healthy=True,
        )
        initial_observation = SharingObservation(
            channel_uuid=channel, now=100, configuration_generation="config-synthetic",
            control_generation=1, recorder_fence=1, lease_is_current=True,
            settings=SharingSettings(True, 1, 2, 60), target=target,
        )
        immediate = observe(None, initial_observation)
        self.assertEqual(SharingAction.ATTACH_BORROW, immediate.action)
        self.assertIsNone(immediate.deadline)

        borrowed = resolve_transition(
            immediate.state, immediate.action, succeeded=True, now=100,
            current_state=immediate.state, current_observation=initial_observation,
        )
        later = observe(borrowed, SharingObservation(
            channel_uuid=channel, now=101, configuration_generation="config-synthetic",
            control_generation=1, recorder_fence=1, lease_is_current=True,
            settings=SharingSettings(True, 1, 2, 60),
            target=BorrowTarget(
                channel_uuid=channel, worker_uuid="worker-synthetic",
                owner_lock_value="owner-synthetic", metadata_owner="owner-synthetic",
                stream_id="stream-synthetic", account_id="account-synthetic",
                m3u_profile_id="profile-synthetic", input_profile_id="input-synthetic",
                ordinary_client_ids=(), owner_heartbeat_healthy=True,
                assigned_stream_matches=True, account_active=True, profile_active=True,
                media_compatible=True, useful_buffer_available=True, native_state_healthy=True,
            ),
        ))
        self.assertEqual(SharingAction.WAIT, later.action)
        self.assertEqual(160, later.deadline)


if __name__ == "__main__":
    unittest.main()
