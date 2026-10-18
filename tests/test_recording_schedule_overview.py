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
    return {"recording_schedule": {"timezone": timezone_name, "channels": {
        key: normalize_schedule(value).to_snapshot() for key, value in schedule.items()
    }}}


class RecordingScheduleOverviewTests(unittest.TestCase):
    def test_continuous_and_multiple_windows_sorted_with_channel_metadata(self):
        result = today_schedule(settings({"a": "continuous", "b": {"daily": [
            {"start": "09:00", "end": "10:00"}, {"start": "13:00", "end": "14:00"}]}}),
            {"a": {"number": 2, "name": "Beta"}, "b": {"number": 1, "name": "Alpha"}},
            now=datetime(2026, 10, 19, 8, tzinfo=timezone.utc))
        self.assertEqual([(r["start"], r["channel"]) for r in result["channels"]["today_schedule"]],
                         [("00:00", "2 · Beta"), ("09:00", "1 · Alpha"), ("13:00", "1 · Alpha")])

    def test_empty_day_and_paused_state_visible(self):
        result = today_schedule(settings({"a": {"monday": []}}), {},
                                now=datetime(2026, 10, 18, 12, tzinfo=timezone.utc))
        self.assertEqual(result["channels"]["today_schedule"], [])
        self.assertIn("No recording windows", result["message"])
        result = today_schedule(settings({"a": {"daily": [{"start": "09:00", "end": "10:00"}]}}),
                                {}, control_state=type("Control", (), {"paused": True})(),
                                now=datetime(2026, 10, 19, 8, tzinfo=timezone.utc))
        self.assertTrue(result["channels"]["today_schedule"][0]["paused"])

    def test_statuses_overnight_and_week_wrap_are_clipped_to_today(self):
        result = today_schedule(settings({"a": {"sunday": [{"start": "23:00", "end": "01:00"}],
                                              "monday": [{"start": "08:00", "end": "09:00"}]}}), {},
                                now=datetime(2026, 10, 18, 20, tzinfo=timezone.utc))
        rows = result["channels"]["today_schedule"]
        self.assertEqual([(r["start"], r["end"], r["status"]) for r in rows], [("23:00", "24:00", "upcoming")])

    def test_timezone_dst_wall_clock(self):
        result = today_schedule(settings({"a": {"daily": [{"start": "02:00", "end": "03:00"}]}}), {},
                                now=datetime(2026, 10, 25, 0, 30, tzinfo=timezone.utc))
        self.assertEqual(result["channels"]["today_schedule"][0]["start"], "02:00")

    def test_plugin_route_uses_readonly_applied_loader(self):
        active = settings({"uuid": {"daily": [{"start": "09:00", "end": "10:00"}]}})
        with patch("catchuparr.configuration.load_applied_state_readonly", return_value=(active, None)), \
             patch("catchuparr.recording_schedule_overview.channel_metadata", return_value={}):
            result = Plugin.__new__(Plugin).run("today_schedule", {}, {})
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["channels"]["today_schedule"][0]["channel"], "Unknown channel")

    def test_readonly_loader_does_not_create_missing_lock_or_sidecars(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            active_path = root / "active.json"
            self.assertEqual(load_applied_state_readonly(active_path), (None, None))
            self.assertEqual(list(root.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
