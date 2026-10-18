"""Read-only rendering of the applied recording schedule for today."""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
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
        return {
            str(row["uuid"]): {
                "name": row.get("name"),
                "number": row.get(number_field) if number_field else None,
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
    paused = bool(control_state and control_state.paused)
    schedules = schedule_config.get("channels")
    if not isinstance(schedules, dict):
        return _error("schedule_unavailable", "Applied recording schedule is unavailable.")

    rows = []
    day_start = datetime.combine(local_now.date(), time.min, tzinfo=zone)
    day_end = datetime.combine(local_now.date() + timedelta(days=1), time.min, tzinfo=zone)
    for channel_id, snapshot in schedules.items():
        try:
            schedule = schedule_from_snapshot(snapshot)
        except (ValueError, TypeError):
            return _error("schedule_unavailable", "Applied recording schedule is unavailable.")
        metadata = channels.get(channel_id, {})
        number = metadata.get("number")
        name = metadata.get("name")
        label = " · ".join(str(x) for x in (number, name) if x not in (None, "")) or "Unknown channel"
        windows = []
        if schedule.mode == "continuous":
            windows = [(day_start, day_end)]
        else:
            # Evaluate each local minute through the existing schedule evaluator;
            # this preserves its wall-clock/DST matching semantics.
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
            rows.append({"channel": label, "channel_uuid": channel_id,
                         "start": start.strftime("%H:%M"),
                         "end": "24:00" if end.date() > local_now.date() else end.strftime("%H:%M"),
                         "status": status, "paused": paused})
    rows.sort(key=lambda row: (row["start"], row["channel"].casefold()))
    if not rows:
        message = f"No recording windows scheduled for {local_now.strftime('%A, %Y-%m-%d')}."
    else:
        lines = [
            f"{row['start']}–{row['end']}  {row['channel']}  "
            f"({row['status']}{', paused' if row['paused'] else ''})"
            for row in rows
        ]
        message = f"Today's recording schedule · {timezone_name}\n" + "\n".join(lines)
    return {"status": "ok", "message": message,
            "channels": {"today_schedule": rows},
            "schedule_date": local_now.date().isoformat(), "timezone": timezone_name}


def _error(status, message):
    return {"status": status, "message": message, "channels": {"today_schedule": []}}
