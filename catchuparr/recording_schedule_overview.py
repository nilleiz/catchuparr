"""Read-only overviews of today's applied channel schedule and planned shows."""

from __future__ import annotations

import math
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

from .action_messages import deliver_action_message, split_group_row
from .schedule import schedule_from_snapshot


def channel_metadata(channel_ids):
    """Read selected channel numbers and names from Dispatcharr's catalog."""
    try:
        from apps.channels.models import Channel

        fields = {field.name for field in Channel._meta.get_fields()}
        number_field = next((name for name in ("channel_number", "number") if name in fields), None)
        columns = ["uuid", "name"] + ([number_field] if number_field else [])
        rows = Channel.objects.filter(uuid__in=channel_ids).values(*columns)
        from .recording_overview import _safe_display

        return {
            str(row["uuid"]): {
                "name": _safe_display(row.get("name")) or "Unknown channel",
                "number": _safe_number(row.get(number_field)) if number_field else None,
            }
            for row in rows
        }
    except Exception:
        return {}


def _day_context(settings, now):
    """Validated inputs both overviews share, or an error result."""
    unavailable = _error("schedule_unavailable", "Applied recording schedule is unavailable.")
    schedule_config = settings.get("recording_schedule")
    if not isinstance(schedule_config, dict):
        return None, unavailable
    timezone_name = schedule_config.get("timezone")
    if not isinstance(timezone_name, str):
        return None, unavailable
    try:
        zone = ZoneInfo(timezone_name)
        instant = now or datetime.now(timezone.utc)
        local_now = instant.astimezone(zone)
    except (ValueError, TypeError, KeyError):
        return None, unavailable
    schedules = schedule_config.get("channels")
    if not isinstance(schedules, dict):
        return None, unavailable
    channel_ids = settings.get("channel_uuids", "")
    if isinstance(channel_ids, str):
        selected_ids = list(dict.fromkeys(item for item in channel_ids.splitlines() if item))
    elif isinstance(channel_ids, (tuple, list)):
        selected_ids = list(dict.fromkeys(item for item in channel_ids if isinstance(item, str) and item))
    else:
        return None, unavailable
    return {
        "zone": zone,
        "timezone": timezone_name,
        "now": local_now,
        "schedules": schedules,
        "selected_ids": selected_ids,
        "day_start": datetime.combine(local_now.date(), time.min, tzinfo=zone),
        "day_end": datetime.combine(local_now.date() + timedelta(days=1), time.min, tzinfo=zone),
    }, None


def _pause_state(control_state):
    paused = getattr(control_state, "paused", None)
    return paused if type(paused) is bool else None


def _channel_row(channel_id, channels, **fields):
    from .recording_overview import _safe_display

    metadata = channels.get(channel_id, {})
    if not isinstance(metadata, dict):
        metadata = {}
    return {
        "channel_uuid": channel_id,
        "number": _safe_number(metadata.get("number")),
        "name": _safe_display(metadata.get("name")) or "Unknown channel",
        **fields,
    }


def _status(start, end, now):
    if end <= now:
        return "past"
    if start <= now < end:
        return "current"
    return "upcoming"


def _result(rows, lines, *, title, summary, run_key, paused, day, extra):
    pause_label = "paused" if paused is True else "active" if paused is False else "unknown"
    return {
        "status": "ok",
        "message": deliver_action_message(
            lines, title=title, summary=summary, run_key=f"{run_key}-{uuid4().hex[:12]}",
        ),
        "channels": rows,
        "summary": {
            "channel_count": len(rows),
            "recording_paused": paused,
            "pause_state": pause_label,
            **extra,
        },
        "schedule_date": day["now"].date().isoformat(),
        "timezone": day["timezone"],
        "recording_paused": paused,
    }


def today_schedule(settings, channels, *, control_state=None, now=None):
    """Render today's weekly channel schedule. ``channels`` maps UUIDs to metadata.

    Metadata values may contain ``number`` and ``name``. The caller supplies
    only the validated applied snapshot, never editable draft settings. Show
    rules have their own overview, :func:`today_shows`.
    """
    day, error = _day_context(settings, now)
    if error:
        return error
    paused = _pause_state(control_state)

    from .recording_overview import _channel_sort_key, _safe_display
    from .schedule import schedule_is_active

    zone, local_now = day["zone"], day["now"]
    day_start, day_end = day["day_start"], day["day_end"]
    rows = []
    for channel_id in day["selected_ids"]:
        row = _channel_row(channel_id, channels, windows=[])
        try:
            schedule = schedule_from_snapshot(day["schedules"].get(channel_id))
        except (ValueError, TypeError):
            return _error("schedule_unavailable", "Applied recording schedule is unavailable.")
        windows = []
        if schedule.mode == "continuous":
            windows = [(day_start, day_end)]
        else:
            # Sample real minutes and use the existing evaluator, preserving its
            # wall-clock/DST matching without inventing nonexistent spring times.
            cursor = day_start.astimezone(timezone.utc)
            utc_day_end = day_end.astimezone(timezone.utc)
            opened = None
            while cursor < utc_day_end:
                active = schedule_is_active(schedule, day["timezone"], cursor)
                if active and opened is None:
                    opened = cursor.astimezone(zone)
                elif not active and opened is not None:
                    windows.append((opened, cursor.astimezone(zone)))
                    opened = None
                cursor += timedelta(minutes=1)
            if opened is not None:
                windows.append((opened, day_end))
        for start, end in windows:
            row["windows"].append({
                "start": start.strftime("%H:%M"),
                "end": "24:00" if end.date() > local_now.date() else end.strftime("%H:%M"),
                "status": _status(start, end, local_now),
            })
        rows.append(row)
    rows.sort(key=_channel_sort_key)
    # Channels sharing identical windows form one "period › channels" row.
    groups = {}
    for row in rows:
        if row["windows"]:
            key = tuple((window["start"], window["end"]) for window in row["windows"])
            groups.setdefault(key, []).append(_safe_display(row.get("name")) or "Unknown channel")
    lines = []
    for key in sorted(groups, key=lambda windows: (windows != (("00:00", "24:00"),), windows)):
        period = "All day" if key == (("00:00", "24:00"),) else ", ".join(
            f"{start}–{end}" for start, end in key
        )
        lines.extend(split_group_row(period, groups[key]))
    channel_total = sum(len(names) for names in groups.values())
    summary = ("Recording paused. " if paused is True else "") + (
        f"Today's channel schedule: {channel_total} {'channel' if channel_total == 1 else 'channels'} "
        f"in {len(groups)} {'period' if len(groups) == 1 else 'periods'}."
        if lines else "No channel recordings scheduled today."
    )
    return _result(
        rows, lines, title="Channel schedule", summary=summary, run_key="schedule",
        paused=paused, day=day,
        extra={"window_count": sum(len(row["windows"]) for row in rows)},
    )


def today_shows(settings, channels, *, control_state=None, now=None):
    """Render today's planned shows of the applied show rules."""
    day, error = _day_context(settings, now)
    if error:
        return error
    paused = _pause_state(control_state)

    from .recording_overview import _channel_sort_key

    rows = []
    for channel_id in day["selected_ids"]:
        shows = _shows_of_day(settings, channel_id, day["day_start"], day["day_end"])
        if not shows:
            continue
        for show in shows:
            show["status"] = _status(show["start_ts"], show["end_ts"], day["now"].timestamp())
        rows.append(_channel_row(channel_id, channels, shows=shows))
    rows.sort(key=_channel_sort_key)
    entries = [
        (row["name"], show["start_ts"], show["end_ts"], show["title"])
        for row in rows for show in row["shows"]
    ]
    lines = []
    if entries:
        from .show_windows import format_show_rows

        lines = format_show_rows(entries, day["timezone"], day["now"].timestamp())
    show_count = len(entries)
    if not settings.get("show_rules"):
        text = "No show rules are applied."
    elif not entries:
        text = "No shows planned today."
    else:
        text = (f"Today's shows: {show_count} {'show' if show_count == 1 else 'shows'} "
                f"on {len(rows)} {'channel' if len(rows) == 1 else 'channels'}.")
    summary = ("Recording paused. " if paused is True else "") + text
    return _result(
        rows, lines, title="Shows schedule", summary=summary, run_key="shows",
        paused=paused, day=day, extra={"show_count": show_count},
    )


def _shows_of_day(settings, channel_id, day_start, day_end):
    """Planned programmes of the day with their local times, for the overview rows."""
    if not settings.get("show_rules"):
        return []
    from .show_windows import day_shows, rule_names

    names = rule_names(settings)
    try:
        shows = day_shows(settings, channel_id, day_start.timestamp(), day_end.timestamp())
    except Exception:
        return []
    return [{
        "title": show.title, "rule": names.get(show.rule, f"rule {show.rule}"),
        "start": datetime.fromtimestamp(show.start, day_start.tzinfo).strftime("%H:%M"),
        "end": datetime.fromtimestamp(show.end, day_start.tzinfo).strftime("%H:%M"),
        "start_ts": show.start, "end_ts": show.end,
    } for show in shows]


def _error(status, message):
    return {
        "status": status,
        "message": message,
        "channels": [],
        "summary": {"channel_count": 0, "window_count": 0, "recording_paused": None,
                    "pause_state": "unknown"},
        "recording_paused": None,
    }


def _safe_number(value):
    """Return finite numeric channel numbering without accepting display text."""
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    try:
        numeric = float(value)
    except (OverflowError, TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return value if isinstance(value, int) else numeric
