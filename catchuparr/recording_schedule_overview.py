"""Read-only rendering of the applied recording schedule for today."""

from __future__ import annotations

import math
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

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
    schedule_config = settings.get("recording_schedule") or {}
    timezone_name = schedule_config.get("timezone")
    if not isinstance(timezone_name, str):
        return _error("schedule_unavailable", "Applied recording schedule is unavailable.")
    try:
        zone = ZoneInfo(timezone_name)
        instant = now or datetime.now(timezone.utc)
        local_now = instant.astimezone(zone)
    except (ValueError, TypeError):
        return _error("schedule_unavailable", "Applied recording schedule is unavailable.")
    paused = getattr(control_state, "paused", None)
    if type(paused) is not bool:
        paused = None
    schedules = schedule_config.get("channels")
    if not isinstance(schedules, dict):
        return _error("schedule_unavailable", "Applied recording schedule is unavailable.")

    from .recording_overview import _channel_sort_key, _display_name, _safe_display

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
    pause_label = "paused" if paused is True else "active" if paused is False else "unknown"
    header = f"Today's recording schedule · {local_now.date().isoformat()} · {timezone_name}"
    if paused is True:
        header += " · Recording paused"
    elif paused is None:
        header += " · Pause state unknown"
    lines = []
    for row in rows:
        label = _display_name(row)
        if not row["windows"]:
            lines.append(f"{label} — No recording windows planned.")
            continue
        lines.extend(
            f"{label} — {window['start']}–{window['end']} ({window['status']})"
            for window in row["windows"]
        )
    if not lines:
        lines.append("No channels are selected.")
    window_count = sum(len(row["windows"]) for row in rows)
    return {
        "status": "ok",
        "message": header + "\n" + "\n".join(lines),
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
