import unittest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from catchuparr.schedule import (
    CONTINUOUS_SCHEDULE,
    MINUTES_PER_DAY,
    MINUTES_PER_WEEK,
    RecordingSchedule,
    ScheduleError,
    normalize_schedule,
    schedule_from_snapshot,
    schedule_is_active,
    validate_timezone,
)


class ScheduleTests(unittest.TestCase):
    def test_continuous_and_empty_weekly_schedule_are_distinct(self):
        self.assertEqual(CONTINUOUS_SCHEDULE, normalize_schedule("continuous"))
        self.assertEqual(RecordingSchedule("weekly", ()), normalize_schedule({}))
        self.assertTrue(
            schedule_is_active(CONTINUOUS_SCHEDULE, "Europe/Berlin", datetime.now(timezone.utc))
        )
        self.assertFalse(
            schedule_is_active(
                RecordingSchedule("weekly", ()), "Europe/Berlin", datetime.now(timezone.utc)
            )
        )

    def test_full_day_and_adjacent_windows_normalize_to_minute_intervals(self):
        direct_full_day = normalize_schedule({
            "monday": [{"start": "00:00", "end": "24:00"}],
        })
        adjacent_halves = normalize_schedule({
            "monday": [
                {"start": "00:00", "end": "12:00"},
                {"start": "12:00", "end": "24:00"},
            ],
        })
        self.assertEqual(((0, MINUTES_PER_DAY),), direct_full_day.intervals)
        self.assertEqual(direct_full_day, adjacent_halves)

        full_week = normalize_schedule({
            day: [{"start": "00:00", "end": "24:00"}]
            for day in (
                "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"
            )
        })
        self.assertEqual(((0, MINUTES_PER_WEEK),), full_week.intervals)
        self.assertEqual("weekly", full_week.mode)

    def test_overlapping_and_cross_midnight_windows_merge(self):
        schedule = normalize_schedule({
            "monday": [
                {"start": "22:00", "end": "02:00"},
                {"start": "23:00", "end": "24:00"},
            ],
            "tuesday": [{"start": "00:00", "end": "03:00"}],
        })
        self.assertEqual(((22 * 60, 27 * 60),), schedule.intervals)

    def test_overnight_window_belongs_to_start_day(self):
        schedule = normalize_schedule({
            "monday": [{"start": "22:00", "end": "02:00"}],
        })
        berlin = ZoneInfo("Europe/Berlin")
        self.assertEqual(((22 * 60, 26 * 60),), schedule.intervals)
        self.assertTrue(schedule_is_active(
            schedule, "Europe/Berlin", datetime(2026, 1, 5, 23, 59, tzinfo=berlin)
        ))
        self.assertTrue(schedule_is_active(
            schedule, "Europe/Berlin", datetime(2026, 1, 6, 1, 59, tzinfo=berlin)
        ))
        self.assertFalse(schedule_is_active(
            schedule, "Europe/Berlin", datetime(2026, 1, 6, 2, 0, tzinfo=berlin)
        ))

    def test_sunday_overnight_wraps_at_week_boundary(self):
        schedule = normalize_schedule({
            "sunday": [{"start": "22:00", "end": "02:00"}],
        })
        self.assertEqual(((0, 120), (9960, MINUTES_PER_WEEK)), schedule.intervals)
        berlin = ZoneInfo("Europe/Berlin")
        self.assertTrue(schedule_is_active(
            schedule, "Europe/Berlin", datetime(2026, 1, 4, 23, 0, tzinfo=berlin)
        ))
        self.assertTrue(schedule_is_active(
            schedule, "Europe/Berlin", datetime(2026, 1, 5, 1, 59, tzinfo=berlin)
        ))
        self.assertFalse(schedule_is_active(
            schedule, "Europe/Berlin", datetime(2026, 1, 5, 2, 0, tzinfo=berlin)
        ))

    def test_dst_fold_is_active_on_both_repeated_wall_clock_occurrences(self):
        schedule = normalize_schedule({
            "sunday": [{"start": "02:00", "end": "03:00"}],
        })
        first_0230 = datetime(2026, 10, 25, 0, 30, tzinfo=timezone.utc)
        second_0230 = datetime(2026, 10, 25, 1, 30, tzinfo=timezone.utc)
        self.assertTrue(schedule_is_active(schedule, "Europe/Berlin", first_0230))
        self.assertTrue(schedule_is_active(schedule, "Europe/Berlin", second_0230))

    def test_nonexistent_spring_forward_minutes_do_not_occur(self):
        schedule = normalize_schedule({
            "sunday": [{"start": "02:30", "end": "04:00"}],
        })
        before_jump = datetime(2026, 3, 29, 0, 59, tzinfo=timezone.utc)
        after_jump = datetime(2026, 3, 29, 1, 0, tzinfo=timezone.utc)
        self.assertFalse(schedule_is_active(schedule, "Europe/Berlin", before_jump))
        self.assertTrue(schedule_is_active(schedule, "Europe/Berlin", after_jump))

    def test_rejects_equal_endpoints_and_invalid_clock_boundaries(self):
        invalid = (
            {"monday": [{"start": "01:00", "end": "01:00"}]},
            {"monday": [{"start": "24:00", "end": "24:00"}]},
            {"monday": [{"start": "00:00", "end": "24:01"}]},
            {"monday": [{"start": "1:00", "end": "02:00"}]},
        )
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ScheduleError):
                normalize_schedule(value)

    def test_rejects_unknown_weekdays_and_window_fields(self):
        invalid = (
            {"mon": [{"start": "01:00", "end": "02:00"}]},
            {"monday": [{"start": "01:00", "end": "02:00", "extra": "value"}]},
            {"monday": "01:00-02:00"},
            "weekly",
        )
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ScheduleError):
                normalize_schedule(value)

    def test_validates_iana_timezone_names(self):
        self.assertEqual("Europe/Berlin", validate_timezone("Europe/Berlin"))
        with self.assertRaisesRegex(ScheduleError, "unknown IANA timezone"):
            validate_timezone("Synthetic/Unknown")
        with self.assertRaises(ScheduleError):
            validate_timezone(42)

    def test_active_snapshot_round_trip_and_strict_normalization(self):
        schedule = normalize_schedule({
            "monday": [
                {"start": "00:00", "end": "12:00"},
                {"start": "12:00", "end": "24:00"},
            ],
            "sunday": [{"start": "22:00", "end": "02:00"}],
        })
        self.assertEqual(schedule, schedule_from_snapshot(schedule.to_snapshot()))
        for invalid in (
            {"mode": "weekly", "intervals": [[0, 720], [720, 1440]]},
            {"mode": "weekly", "intervals": [[0, True]]},
            {"mode": "continuous", "intervals": []},
            {"mode": "weekly", "intervals": [[-1, 10]]},
        ):
            with self.subTest(value=invalid), self.assertRaises(ScheduleError):
                schedule_from_snapshot(invalid)

    def test_evaluation_requires_an_aware_instant_for_weekly_schedule(self):
        schedule = normalize_schedule({"monday": [{"start": "01:00", "end": "02:00"}]})
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            schedule_is_active(schedule, "Europe/Berlin", datetime(2026, 1, 5, 1))


if __name__ == "__main__":
    unittest.main()
