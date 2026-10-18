import unittest
import tempfile
from unittest.mock import patch
from datetime import datetime, timezone
from pathlib import Path

from catchuparr.recording_schedule_overview import today_schedule
from catchuparr.schedule import normalize_schedule
from catchuparr.plugin import Plugin
from catchuparr.configuration import load_applied_state_readonly


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
        self.assertIn("2026-10-19", result["message"])

    def test_empty_day_and_paused_state_visible(self):
        result = today_schedule(settings({"a": {"sunday": []}, "b": "continuous"}),
                                {"a": {"number": 10, "name": "Empty"},
                                 "b": {"number": 2, "name": "Continuous"}},
                                now=datetime(2026, 10, 18, 12, tzinfo=timezone.utc))
        self.assertEqual([row["number"] for row in result["channels"]], [2, 10])
        empty = result["channels"][1]
        self.assertEqual(empty["windows"], [])
        self.assertIn("No recording windows planned", result["message"])
        self.assertIn("2026-10-18", result["message"])
        self.assertIsNone(result["recording_paused"])
        self.assertIn("Pause state unknown", result["message"])
        result = today_schedule(settings({"a": {"daily": [{"start": "09:00", "end": "10:00"}]}}),
                                {}, control_state=type("Control", (), {"paused": True})(),
                                now=datetime(2026, 10, 19, 8, tzinfo=timezone.utc))
        self.assertTrue(result["recording_paused"])
        self.assertIn("Recording paused", result["message"])
        active = today_schedule(settings({"a": "continuous"}), {},
                                 control_state=type("Control", (), {"paused": False})(),
                                 now=datetime(2026, 10, 19, 8, tzinfo=timezone.utc))
        self.assertIs(active["recording_paused"], False)
        self.assertEqual(active["summary"]["pause_state"], "active")
        paused_empty = today_schedule(settings({"a": {"sunday": []}}), {},
                                      control_state=type("Control", (), {"paused": True})(),
                                      now=datetime(2026, 10, 18, 12, tzinfo=timezone.utc))
        self.assertTrue(paused_empty["recording_paused"])
        self.assertIn("Recording paused", paused_empty["message"])

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


if __name__ == "__main__":
    unittest.main()
