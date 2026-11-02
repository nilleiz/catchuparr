"""Read-only rendering of the applied recording schedule for today."""

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


def today_schedule(settings, channels, *, control_state=None, now=None):
    """Render today's applied schedule. ``channels`` maps UUIDs to metadata.

    Metadata values may contain ``number`` and ``name``. The caller supplies
    only the validated applied snapshot, never editable draft settings.
    """
    schedule_config = settings.get("recording_schedule")
    if not isinstance(schedule_config, dict):
        return _error("schedule_unavailable", "Applied recording schedule is unavailable.")
    timezone_name = schedule_config.get("timezone")
    if not isinstance(timezone_name, str):
        return _error("schedule_unavailable", "Applied recording schedule is unavailable.")
    try:
        zone = ZoneInfo(timezone_name)
        instant = now or datetime.now(timezone.utc)
        local_now = instant.astimezone(zone)
    except (ValueError, TypeError, KeyError):
        return _error("schedule_unavailable", "Applied recording schedule is unavailable.")
    paused = getattr(control_state, "paused", None)
    if type(paused) is not bool:
        paused = None
    schedules = schedule_config.get("channels")
    if not isinstance(schedules, dict):
        return _error("schedule_unavailable", "Applied recording schedule is unavailable.")

    from .recording_overview import _channel_sort_key, _safe_display

    channel_ids = settings.get("channel_uuids", "")
    if isinstance(channel_ids, str):
        selected_ids = list(dict.fromkeys(item for item in channel_ids.splitlines() if item))
    elif isinstance(channel_ids, (tuple, list)):
        selected_ids = list(dict.fromkeys(item for item in channel_ids if isinstance(item, str) and item))
    else:
        return _error("schedule_unavailable", "Applied recording schedule is unavailable.")

    rows = []
    day_start = datetime.combine(local_now.date(), time.min, tzinfo=zone)
    day_end = datetime.combine(local_now.date() + timedelta(days=1), time.min, tzinfo=zone)
    for channel_id in selected_ids:
        metadata = channels.get(channel_id, {})
        if not isinstance(metadata, dict):
            metadata = {}
        row = {
            "channel_uuid": channel_id,
            "number": _safe_number(metadata.get("number")),
            "name": _safe_display(metadata.get("name")) or "Unknown channel",
            "windows": [],
            "shows": [],
        }
        snapshot = schedules.get(channel_id)
        try:
            schedule = schedule_from_snapshot(snapshot)
        except (ValueError, TypeError):
            return _error("schedule_unavailable", "Applied recording schedule is unavailable.")
        windows = []
        if schedule.mode == "continuous":
            windows = [(day_start, day_end)]
        else:
            # Sample real minutes and use the existing evaluator, preserving its
            # wall-clock/DST matching without inventing nonexistent spring times.
            from .schedule import schedule_is_active
            cursor = day_start.astimezone(timezone.utc)
            utc_day_end = day_end.astimezone(timezone.utc)
            opened = None
            while cursor < utc_day_end:
                active = schedule_is_active(schedule, timezone_name, cursor)
                if active and opened is None:
                    opened = cursor.astimezone(zone)
                elif not active and opened is not None:
                    windows.append((opened, cursor.astimezone(zone)))
                    opened = None
                cursor += timedelta(minutes=1)
            if opened is not None:
                windows.append((opened, day_end))
        windows = _with_show_windows(settings, channel_id, windows, day_start, day_end, zone)
        row["shows"] = _shows_of_day(settings, channel_id, day_start, day_end)
        for start, end in windows:
            if end.astimezone(timezone.utc) <= local_now.astimezone(timezone.utc):
                status = "past"
            elif start.astimezone(timezone.utc) <= local_now.astimezone(timezone.utc) < end.astimezone(timezone.utc):
                status = "current"
            else:
                status = "upcoming"
            row["windows"].append({
                "start": start.strftime("%H:%M"),
                "end": "24:00" if end.date() > local_now.date() else end.strftime("%H:%M"),
                "status": status,
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
    show_entries = [
        (row["name"], show["start_ts"], show["end_ts"], show["title"])
        for row in rows for show in row["shows"]
    ]
    if show_entries:
        from .show_windows import format_show_rows

        lines.extend(f"Show · {text}" for text in format_show_rows(
            show_entries, timezone_name, local_now.timestamp(),
        ))
    channel_total = sum(len(names) for names in groups.values())
    summary = ("Recording paused. " if paused is True else "") + (
        f"Today's schedule: {channel_total} {'channel' if channel_total == 1 else 'channels'} "
        f"in {len(groups)} {'period' if len(groups) == 1 else 'periods'}"
        + (f", {len(show_entries)} planned show{'' if len(show_entries) == 1 else 's'}." if show_entries else ".")
        if lines else "No recordings scheduled today."
    )
    pause_label = "paused" if paused is True else "active" if paused is False else "unknown"
    window_count = sum(len(row["windows"]) for row in rows)
    return {
        "status": "ok",
        "message": deliver_action_message(
            lines, title="Schedule", summary=summary,
            run_key=f"schedule-{uuid4().hex[:12]}",
        ),
        "channels": rows,
        "summary": {
            "channel_count": len(rows),
            "window_count": window_count,
            "recording_paused": paused,
            "pause_state": pause_label,
        },
        "schedule_date": local_now.date().isoformat(),
        "timezone": timezone_name,
        "recording_paused": paused,
    }


def _with_show_windows(settings, channel_id, windows, day_start, day_end, zone):
    """Add today's planned shows windows to the weekly ones; both admit the recorder."""
    if not settings.get("show_rules"):
        return windows
    try:
        from .show_windows import covered_channels, load_windows, windows_path

        if channel_id not in covered_channels(settings):
            return windows
        stored = load_windows(windows_path(settings)).get(channel_id, ())
    except Exception:
        return windows
    spans = [(start, end) for start, end in windows]
    for window in stored:
        start = max(datetime.fromtimestamp(window.start, timezone.utc).astimezone(zone), day_start)
        end = min(datetime.fromtimestamp(window.end, timezone.utc).astimezone(zone), day_end)
        if start < end:
            spans.append((start, end))
    merged = []
    for start, end in sorted(spans, key=lambda span: (span[0], span[1])):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


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
