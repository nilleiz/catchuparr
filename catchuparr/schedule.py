"""Pure weekly recorder schedule parsing and evaluation helpers."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DEFAULT_TIMEZONE = "Europe/Berlin"
MINUTES_PER_DAY = 24 * 60
MINUTES_PER_WEEK = 7 * MINUTES_PER_DAY

_DAYS = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}
_CLOCK = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
_END_CLOCK = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$|^24:00$")


class ScheduleError(ValueError):
    """A schedule or timezone value is invalid."""


@dataclass(frozen=True)
class RecordingSchedule:
    """A continuous schedule or normalized half-open week-minute intervals."""

    mode: Literal["continuous", "weekly"]
    intervals: tuple[tuple[int, int], ...] = ()

    def to_snapshot(self) -> dict[str, Any]:
        if self.mode == "continuous":
            return {"mode": "continuous"}
        return {
            "mode": "weekly",
            "intervals": [[start, end] for start, end in self.intervals],
        }


CONTINUOUS_SCHEDULE = RecordingSchedule("continuous")


def validate_timezone(
    value: Any,
    *,
    line: int = 1,
    field: str = "timezone",
) -> str:
    """Return an installed IANA timezone name or raise a safe field error."""
    if not isinstance(value, str) or not value:
        raise ScheduleError(f"line {line}, field {field}: expected an IANA timezone")
    try:
        ZoneInfo(value)
    except (ValueError, ZoneInfoNotFoundError):
        raise ScheduleError(f"line {line}, field {field}: unknown IANA timezone") from None
    return value


def _parse_clock(
    value: Any,
    *,
    allow_day_end: bool,
    line: int,
    field: str,
) -> int:
    pattern = _END_CLOCK if allow_day_end else _CLOCK
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ScheduleError(f"line {line}, field {field}: expected a clock time in HH:MM form")
    if value == "24:00":
        return MINUTES_PER_DAY
    hour, minute = value.split(":", 1)
    return int(hour) * 60 + int(minute)


def _merge_intervals(
    intervals: list[tuple[int, int]],
) -> tuple[tuple[int, int], ...]:
    merged: list[list[int]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return tuple((start, end) for start, end in merged)


def normalize_schedule(
    value: Any,
    *,
    line: int = 1,
    field: str = "schedule",
) -> RecordingSchedule:
    """Compile `continuous` or a weekday map into canonical week-minute ranges."""
    if value == "continuous":
        return CONTINUOUS_SCHEDULE
    if not isinstance(value, Mapping):
        raise ScheduleError(f"line {line}, field {field}: expected continuous or a weekday mapping")

    intervals: list[tuple[int, int]] = []
    for day, windows in value.items():
        if not isinstance(day, str) or day not in _DAYS:
            raise ScheduleError(f"line {line}, field {field}: unknown weekday")
        day_field = f"{field}.{day}"
        if not isinstance(windows, list):
            raise ScheduleError(f"line {line}, field {day_field}: expected a list of windows")
        day_offset = _DAYS[day] * MINUTES_PER_DAY
        for index, window in enumerate(windows):
            window_field = f"{day_field}[{index}]"
            if not isinstance(window, Mapping) or set(window) != {"start", "end"}:
                raise ScheduleError(
                    f"line {line}, field {window_field}: expected only start and end"
                )
            start = _parse_clock(
                window["start"],
                allow_day_end=False,
                line=line,
                field=f"{window_field}.start",
            )
            end = _parse_clock(
                window["end"],
                allow_day_end=True,
                line=line,
                field=f"{window_field}.end",
            )
            if start == end:
                raise ScheduleError(
                    f"line {line}, field {window_field}: start and end must differ"
                )

            start_abs = day_offset + start
            end_abs = day_offset + end
            if end < start:
                end_abs += MINUTES_PER_DAY
            if end_abs <= MINUTES_PER_WEEK:
                intervals.append((start_abs, end_abs))
            else:
                intervals.append((start_abs, MINUTES_PER_WEEK))
                intervals.append((0, end_abs - MINUTES_PER_WEEK))

    return RecordingSchedule("weekly", _merge_intervals(intervals))


def schedule_from_snapshot(value: Any) -> RecordingSchedule:
    """Load and strictly validate one canonical active-snapshot schedule."""
    if not isinstance(value, Mapping):
        raise ScheduleError("active recording schedule must be a mapping")
    mode = value.get("mode")
    if mode == "continuous":
        if set(value) != {"mode"}:
            raise ScheduleError("continuous active schedule has unexpected fields")
        return CONTINUOUS_SCHEDULE
    if mode != "weekly" or set(value) != {"mode", "intervals"}:
        raise ScheduleError("active recording schedule has an unknown mode or fields")
    raw_intervals = value.get("intervals")
    if not isinstance(raw_intervals, list):
        raise ScheduleError("active weekly schedule intervals must be a list")

    intervals: list[tuple[int, int]] = []
    previous_end = -1
    for index, pair in enumerate(raw_intervals):
        if not isinstance(pair, list) or len(pair) != 2:
            raise ScheduleError(f"active weekly schedule interval {index} must be a pair")
        start, end = pair
        if (
            type(start) is not int
            or type(end) is not int
            or start < 0
            or end > MINUTES_PER_WEEK
            or start >= end
            or (intervals and start <= previous_end)
        ):
            raise ScheduleError(f"active weekly schedule interval {index} is invalid or unnormalized")
        intervals.append((start, end))
        previous_end = end
    return RecordingSchedule("weekly", tuple(intervals))


def schedule_is_active(
    schedule: RecordingSchedule,
    timezone_name: str,
    instant: datetime,
) -> bool:
    """Check an aware instant against local wall-clock schedule intervals."""
    if schedule.mode == "continuous":
        return True
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("schedule evaluation requires a timezone-aware instant")
    try:
        local = instant.astimezone(ZoneInfo(timezone_name))
    except (ValueError, ZoneInfoNotFoundError):
        raise ValueError("schedule evaluation requires a valid IANA timezone") from None
    minute = local.weekday() * MINUTES_PER_DAY + local.hour * 60 + local.minute
    return any(start <= minute < end for start, end in schedule.intervals)
