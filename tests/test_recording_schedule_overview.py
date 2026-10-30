import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr.action_messages import ROW_SEPARATOR, join_rows
from catchuparr.configuration import SourceCatalog, apply_configuration, load_applied_state_readonly
from catchuparr.plugin import Plugin
from catchuparr.recording_schedule_overview import today_schedule
from catchuparr.schedule import normalize_schedule


def settings(schedule, timezone_name="Europe/Berlin"):
    return {
        "channel_uuids": "\n".join(schedule),
        "recording_schedule": {
            "timezone": timezone_name,
            "channels": {
                key: normalize_schedule(value).to_snapshot() for key, value in schedule.items()
            },
        },
    }


class RecordingScheduleOverviewTests(unittest.TestCase):
    def test_continuous_and_multiple_windows_sorted_with_channel_metadata(self):
        result = today_schedule(settings({"a": "continuous", "b": {"daily": [
            {"start": "09:00", "end": "10:00"}, {"start": "13:00", "end": "14:00"}]}}),
            {"a": {"number": 2, "name": "Beta"}, "b": {"number": 1, "name": "Alpha"}},
            now=datetime(2026, 10, 19, 8, tzinfo=timezone.utc))
        rows = result["channels"]
        self.assertEqual([row["name"] for row in rows], ["Alpha", "Beta"])
        self.assertEqual([(window["start"], window["end"]) for window in rows[0]["windows"]],
                         [("09:00", "10:00"), ("13:00", "14:00")])
        self.assertEqual(rows[1]["windows"][0]["end"], "24:00")
        self.assertEqual(result["message"], join_rows((
            "All day › Beta", "09:00–10:00, 13:00–14:00 › Alpha",
        )))

    def test_empty_day_and_paused_state_visible(self):
        result = today_schedule(settings({"a": {"sunday": []}, "b": "continuous"}),
                                {"a": {"number": 10, "name": "Empty"},
                                 "b": {"number": 2, "name": "Continuous"}},
                                now=datetime(2026, 10, 18, 12, tzinfo=timezone.utc))
        self.assertEqual([row["number"] for row in result["channels"]], [2, 10])
        empty = result["channels"][1]
        self.assertEqual(empty["windows"], [])
        self.assertEqual(result["message"], "All day › Continuous")
        self.assertIsNone(result["recording_paused"])
        self.assertNotIn("Pause state unknown", result["message"])
        result = today_schedule(settings({"a": {"daily": [{"start": "09:00", "end": "10:00"}]}}),
                                {}, control_state=type("Control", (), {"paused": True})(),
                                now=datetime(2026, 10, 19, 7, tzinfo=timezone.utc))
        self.assertTrue(result["recording_paused"])
        self.assertEqual(result["message"], join_rows((
            "Recording paused", "09:00–10:00 › Unknown channel",
        )))
        active = today_schedule(settings({"a": "continuous"}), {},
                                 control_state=type("Control", (), {"paused": False})(),
                                 now=datetime(2026, 10, 19, 8, tzinfo=timezone.utc))
        self.assertIs(active["recording_paused"], False)
        self.assertEqual(active["summary"]["pause_state"], "active")
        paused_empty = today_schedule(settings({"a": {"sunday": []}}), {},
                                      control_state=type("Control", (), {"paused": True})(),
                                      now=datetime(2026, 10, 18, 12, tzinfo=timezone.utc))
        self.assertTrue(paused_empty["recording_paused"])
        self.assertEqual(paused_empty["message"], join_rows((
            "Recording paused", "No recordings scheduled today.",
        )))

    def test_past_current_and_upcoming_statuses(self):
        result = today_schedule(
            settings({"a": {"monday": [
                {"start": "09:00", "end": "10:00"},
                {"start": "11:00", "end": "13:00"},
                {"start": "14:00", "end": "15:00"},
            ]}}), {}, now=datetime(2026, 10, 19, 10, tzinfo=timezone.utc))
        self.assertEqual(
            [window["status"] for window in result["channels"][0]["windows"]],
            ["past", "current", "upcoming"],
        )
        self.assertEqual(result["message"], "09:00–10:00, 11:00–13:00, 14:00–15:00 › Unknown channel")

    def test_message_omits_free_channels_and_has_no_heading(self):
        result = today_schedule(
            settings({
                "active": {"daily": [
                    {"start": "09:00", "end": "10:00"},
                    {"start": "13:00", "end": "14:00"},
                ]},
                "free": {"sunday": []},
            }),
            {"active": {"name": "Synthetic Channel A"}, "free": {"name": "Free Channel"}},
            now=datetime(2026, 10, 18, 8, tzinfo=timezone.utc),
        )
        self.assertEqual(result["message"], "09:00–10:00, 13:00–14:00 › Synthetic Channel A")
        self.assertEqual(len(result["channels"]), 2)
        self.assertNotIn("Free Channel", result["message"])

    def test_empty_message_and_schedule_unavailable_are_explicit(self):
        empty = today_schedule(settings({"free": {"sunday": []}}), {},
                               now=datetime(2026, 10, 18, 8, tzinfo=timezone.utc))
        self.assertEqual(empty["message"], "No recordings scheduled today.")
        invalid = today_schedule({"channel_uuids": "a", "recording_schedule": {}}, {})
        self.assertEqual(invalid["status"], "schedule_unavailable")
        self.assertIn("unavailable", invalid["message"].lower())
        malformed = today_schedule({"channel_uuids": "a", "recording_schedule": "invalid"}, {})
        self.assertEqual(malformed["status"], "schedule_unavailable")

    def test_invalid_timezone_rejects_continuous_schedule(self):
        invalid = today_schedule(settings({"a": "continuous"}, "Invalid/Zone"), {})
        self.assertEqual(invalid["status"], "schedule_unavailable")
        self.assertIn("unavailable", invalid["message"].lower())

    def test_only_applied_selected_channels_are_listed(self):
        active = settings({"selected": "continuous"})
        active["recording_schedule"]["channels"]["unselected"] = normalize_schedule(
            "continuous"
        ).to_snapshot()
        result = today_schedule(active, {}, now=datetime(2026, 10, 19, 8, tzinfo=timezone.utc))
        self.assertEqual([row["channel_uuid"] for row in result["channels"]], ["selected"])

    def test_channel_display_rejects_urls_credentials_tokens_and_injected_numbers(self):
        secret_name = "https://user:secret@example.test/channel?access_token=private"
        secret_number = "10\nAuthorization: Bearer private"
        result = today_schedule(
            settings({"a": "continuous"}),
            {"a": {"number": secret_number, "name": secret_name}},
            now=datetime(2026, 10, 19, 8, tzinfo=timezone.utc),
        )
        output = repr(result)
        self.assertEqual(result["channels"][0]["name"], "Unknown channel")
        self.assertIsNone(result["channels"][0]["number"])
        for secret in ("user:secret", "access_token", "private", "Authorization", "Bearer", "example.test"):
            self.assertNotIn(secret, output)

    def test_nonfinite_channel_numbers_are_omitted(self):
        for number in (float("inf"), float("nan")):
            with self.subTest(number=number):
                result = today_schedule(
                    settings({"a": "continuous"}),
                    {"a": {"number": number, "name": "Synthetic Channel"}},
                    now=datetime(2026, 10, 19, 8, tzinfo=timezone.utc),
                )
                self.assertIsNone(result["channels"][0]["number"])

    def test_unsafe_channel_metadata_is_removed_from_rows_and_message(self):
        result = today_schedule(
            settings({"selected": "continuous"}),
            {"selected": {"number": 1, "name": "https://example.invalid/watch?token=synthetic"}},
            now=datetime(2026, 10, 19, 8, tzinfo=timezone.utc),
        )
        self.assertEqual(result["channels"][0]["name"], "Unknown channel")
        self.assertNotIn("example.invalid", result["message"])
        self.assertNotIn("synthetic", result["message"])

    def test_statuses_overnight_and_week_wrap_are_clipped_to_today(self):
        result = today_schedule(settings({"a": {"sunday": [{"start": "23:00", "end": "01:00"}],
                                              "monday": [{"start": "08:00", "end": "09:00"}]}}), {},
                                now=datetime(2026, 10, 18, 20, tzinfo=timezone.utc))
        rows = result["channels"]
        self.assertEqual([(r["start"], r["end"], r["status"]) for r in rows[0]["windows"]],
                         [("23:00", "24:00", "upcoming")])

    def test_timezone_dst_wall_clock(self):
        result = today_schedule(settings({"a": {"daily": [{"start": "02:00", "end": "03:00"}]}}), {},
                                now=datetime(2026, 10, 25, 0, 30, tzinfo=timezone.utc))
        self.assertEqual(result["channels"][0]["windows"][0]["start"], "02:00")
        spring = today_schedule(
            settings({"a": {"daily": [{"start": "02:00", "end": "03:00"}]}}), {},
            now=datetime(2026, 3, 29, 12, tzinfo=timezone.utc),
        )
        self.assertEqual(spring["channels"][0]["windows"], [])

    def test_plugin_route_uses_readonly_applied_loader(self):
        active = settings({"uuid": {"daily": [{"start": "09:00", "end": "10:00"}]}})
        draft = settings({"uuid": {"daily": [{"start": "15:00", "end": "16:00"}]}})
        with patch("catchuparr.configuration.load_applied_state_readonly", return_value=(active, None)), \
             patch("catchuparr.recording_schedule_overview.channel_metadata", return_value={}):
            result = Plugin.__new__(Plugin).run("today_schedule", {}, {"settings": draft})
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["channels"][0]["name"], "Unknown channel")
        self.assertEqual(result["channels"][0]["windows"][0]["start"], "09:00")

    def test_today_schedule_action_pads_visible_rows_and_keeps_full_structured_rows(self):
        active = settings({
            "first": "continuous",
            "second": "continuous",
            "free": {"sunday": []},
        })
        channels = {
            "first": {"number": 37, "name": "Synthetic Channel A"},
            "second": {"number": 42, "name": "Synthetic Channel B"},
            "free": {"number": 99, "name": "Free Synthetic Channel"},
        }
        control = SimpleNamespace(paused=True)
        with patch("catchuparr.configuration.load_applied_state_readonly",
                   return_value=(active, control)), \
             patch("catchuparr.recording_schedule_overview.channel_metadata",
                   return_value=channels):
            result = Plugin.__new__(Plugin).run("today_schedule", {}, {})

        self.assertEqual(
            result["message"].split(ROW_SEPARATOR),
            ["Recording paused", "All day › Synthetic Channel A, Synthetic Channel B"],
        )
        self.assertNotIn("\u00a0", result["message"])
        self.assertNotIn("\n", result["message"])
        self.assertNotIn("37", result["message"])
        self.assertNotIn("42", result["message"])
        self.assertNotIn("99", result["message"])
        self.assertNotIn("Free Synthetic Channel", result["message"])
        self.assertEqual(
            [(row["channel_uuid"], row["number"], row["name"],
              [(window["start"], window["end"]) for window in row["windows"]])
             for row in result["channels"]],
            [
                ("first", 37, "Synthetic Channel A", [("00:00", "24:00")]),
                ("second", 42, "Synthetic Channel B", [("00:00", "24:00")]),
                ("free", 99, "Free Synthetic Channel", []),
            ],
        )

    def test_plugin_unavailable_route_keeps_shared_channel_list_shape(self):
        with patch("catchuparr.configuration.load_applied_state_readonly", return_value=(None, None)):
            result = Plugin.__new__(Plugin).run("today_schedule", {}, {})
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["channels"], [])
        self.assertIsNone(result["recording_paused"])

    def test_readonly_loader_does_not_create_missing_lock_or_sidecars(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            active_path = root / "active.json"
            self.assertEqual(load_applied_state_readonly(active_path), (None, None))
            self.assertEqual(list(root.iterdir()), [])

    def test_readonly_loader_returns_persisted_v7_applied_view_without_rewriting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            active_path = root / "active.json"
            lock_path = active_path.with_suffix(".json.lock")
            channel = "00000000-0000-0000-0000-000000000001"
            apply_configuration(
                {
                    "filter_config": "version: 1\nprofile: Synthetic Profile\nrules:\n  - channels: {profile: all}\n",
                    "archive_root": str(root / "archive"),
                    "retention_hours": 24,
                    "max_storage_gib": 20,
                },
                SourceCatalog(
                    channels=({"uuid": channel, "number": "1", "name": "Synthetic Channel A"},),
                    accounts=(), streams_by_channel={channel: ()},
                    profiles=({"id": "1", "name": "Synthetic Profile", "channel_uuids": (channel,)},),
                ),
                active_path,
            )
            contents = active_path.read_bytes()
            active_path.write_bytes(contents)
            # Apply creates the real v7 snapshot and control sidecar; remove
            # the latter to exercise the valid-active/unknown-control contract.
            (root / ".catchuparr-recorder-control.json").unlink(missing_ok=True)
            (root / ".catchuparr-recorder-control.json.lock").unlink(missing_ok=True)
            lock_path.touch()
            entries_before = {path.name for path in root.iterdir()}

            active, control = load_applied_state_readonly(active_path)

            self.assertEqual(active["version"], 7)
            self.assertEqual(active["connection_sharing"][channel]["enabled"], False)
            self.assertEqual(active["channel_uuids"], channel)
            self.assertIsNone(control)
            self.assertEqual(active_path.read_bytes(), contents)
            self.assertEqual({path.name for path in root.iterdir()}, entries_before)


if __name__ == "__main__":
    unittest.main()
