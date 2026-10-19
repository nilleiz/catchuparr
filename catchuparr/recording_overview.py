"""Read-only current recorder overview for applied Catchuparr channels."""

from __future__ import annotations

import re
import sqlite3
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from .engine.recorder import (
    ARCHIVE_INDEX_FINALIZATION_GRACE_SECONDS,
    DEFAULT_MEDIA_IDLE_TIMEOUT_SECONDS,
    DEFAULT_RECORDER_STARTUP_TIMEOUT_SECONDS,
)
from .recorder_diagnostics import (
    _selected_channel_ids,
    last_error_key,
    live_key,
    read_recorder_diagnostics,
)

_SAFE_DISPLAY = re.compile(r"^[^\x00-\x1f\x7f]{1,120}$")
_SENSITIVE_DISPLAY = re.compile(
    r"(?i)(://|\b[a-z][a-z0-9+.-]*:|\b(?:bearer|authorization|password|api[_ -]?key|access[_ -]?token)\b|[?&](?:token|key|auth)=|\b[a-z0-9.-]+\.(?:com|net|org|io|tv)(?:/|$))"
)


def current_recordings(settings, control_state=None, *, redis_client=None, now=None):
    """Return state for every selected applied channel without changing state.

    The action does not construct ArchiveStore or recover configuration. It
    observes lease-bound diagnostics and opens the existing archive index with
    SQLite read-only/query-only flags.
    """
    try:
        channel_ids = _selected_channel_ids(settings)
        instant = now or datetime.now(timezone.utc)
        timestamp = instant.timestamp() if isinstance(instant, datetime) else float(instant)
        if not isinstance(instant, datetime):
            instant = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    except Exception:
        return _unavailable("applied_configuration_unavailable")

    try:
        from .configuration import active_configuration_generation
        generation = active_configuration_generation(settings)
    except Exception:
        generation = None
    if (not channel_ids or control_state is None or generation is None
            or control_state.configuration_generation != generation):
        return _unknown_all(settings, channel_ids, control_state, "applied_state_unavailable")

    if redis_client is None:
        try:
            from core.utils import RedisClient

            redis_client = RedisClient.get_client()
        except Exception:
            redis_client = None
    redis_ready = False
    try:
        if redis_client is not None:
            redis_client.ping()
            redis_ready = True
    except Exception:
        redis_ready = False

    diagnostics = read_recorder_diagnostics(
        redis_client if redis_ready else None, settings, control_state, now=timestamp,
    )
    metadata = _channel_metadata(channel_ids)
    rows = []
    schedule = settings.get("recording_schedule")
    schedule_channels = schedule.get("channels") if isinstance(schedule, dict) else None
    timezone_name = schedule.get("timezone") if isinstance(schedule, dict) else None
    for channel_uuid in channel_ids:
        channel = metadata.get(channel_uuid, {})
        diagnostic = diagnostics.get(channel_uuid)
        evidence = None
        if diagnostic and diagnostic.get("phase") != "error":
            evidence = _indexed_media_since(
                settings, channel_uuid, diagnostic["started_at"], timestamp,
            )
        if not redis_ready:
            state, mode, reason = "unknown", "unconfirmed", "redis_unavailable"
        elif diagnostic and diagnostic.get("phase") == "error":
            state, mode, reason = "error", _mode(diagnostic), "reported_recorder_error"
        elif diagnostic and diagnostic.get("phase") == "cleanup_pending":
            state, mode, reason = "cleanup_pending", _mode(diagnostic), "recorder_cleanup_pending"
        elif control_state.paused:
            state, mode, reason = "paused", _mode(diagnostic), "recording_paused"
        else:
            scheduled, schedule_error = _scheduled_now(
                schedule_channels, timezone_name, channel_uuid, instant,
            )
            if schedule_error:
                state, mode, reason = "unknown", _mode(diagnostic), "schedule_unavailable"
            elif not scheduled:
                state, mode, reason = "outside_schedule", _mode(diagnostic), "outside_applied_schedule"
            else:
                state, mode, reason = _state_for_channel(
                    settings, channel_uuid, diagnostic, redis_client, timestamp,
                    evidence=evidence,
                )
        source = _source_for_diagnostic(channel_uuid, diagnostic) if diagnostic else None
        if source is not None:
            binding = source_metadata(
                channel_uuid,
                source.get("stream_id"),
                source.get("account_id"),
                source.get("m3u_profile_id"),
                source.get("input_profile_id"),
            )
            if binding is None:
                source = None
                if state in {"recording", "starting", "no_media_progress"}:
                    state, mode, reason = "unknown", "unconfirmed", "source_binding_unavailable"
            else:
                source.update(binding)
        channel_row = {
            "channel_uuid": channel_uuid,
            "number": channel.get("number"),
            "name": _safe_display(channel.get("name")) or "Unknown channel",
            "state": state,
            "mode": mode,
            "reason": reason,
            "source": source,
        }
        if diagnostic and diagnostic.get("phase") == "error":
            channel_row["error_code"] = diagnostic.get("error_code")
        if diagnostic and diagnostic.get("phase") == "recording":
            channel_row["indexed_segments_since_attempt"] = (
                evidence["count"] if evidence and evidence["available"] else None
            )
            channel_row["recent_media_progress"] = (
                evidence["recent"] if evidence and evidence["available"] else None
            )
        progress_at = evidence.get("last_progress_at") if evidence and evidence.get("available") else None
        channel_row["last_confirmed_progress_at"] = (
            datetime.fromtimestamp(progress_at, tz=timezone.utc).isoformat()
            if progress_at is not None else None
        )
        channel_row["last_confirmed_progress_none"] = progress_at is None
        rows.append(channel_row)

    rows.sort(key=_channel_sort_key)
    counts = {name: sum(row["state"] == name for row in rows) for name in _STATES}
    problem_count = sum(
        row["state"] in {"no_media_progress", "cleanup_pending", "error", "unknown"}
        for row in rows
    )
    counts["problems"] = problem_count
    lines = [f"Problems: {problem_count}"]
    lines.extend(_message_row(row) for row in rows)
    if not rows:
        lines.append("No channels are selected.")
    message = "Current recordings\n" + "\n".join(lines)
    return {"status": "ok", "message": message, "channels": rows, "summary": counts}


_STATES = (
    "recording", "starting", "no_media_progress", "cleanup_pending", "error",
    "paused", "outside_schedule", "unknown",
)


def source_metadata(channel_uuid, stream_id, account_id, m3u_profile_id=None, input_profile_id=None):
    """Read display-only metadata for an exact currently assigned source."""
    if not all(isinstance(value, str) and value for value in (channel_uuid, stream_id, account_id)):
        return None
    try:
        from apps.channels.models import ChannelStream

        assignment = (
            ChannelStream.objects.filter(channel__uuid=channel_uuid, stream_id=stream_id)
            .select_related("stream", "stream__m3u_account")
            .first()
        )
        if assignment is None:
            return None
        stream = assignment.stream
        account = getattr(stream, "m3u_account", None)
        actual_account_id = str(getattr(stream, "m3u_account_id", "") or "")
        if account is None or actual_account_id != str(account_id):
            return None
        result = {
            "stream_name": _safe_display(getattr(stream, "name", None)),
            "stream_id": str(getattr(stream, "id", "") or "") or None,
            "provider_id": actual_account_id,
            "provider_name": _safe_display(getattr(account, "name", None)),
        }
        if not _safe_identifier(result["stream_id"]) or not _safe_identifier(actual_account_id):
            return None
        if m3u_profile_id is not None:
            if not isinstance(m3u_profile_id, str) or not m3u_profile_id:
                return None
            m3u_profile = account.profiles.filter(id=m3u_profile_id).only("id", "name").first()
            if m3u_profile is None:
                return None
            result["m3u_profile_id"] = str(m3u_profile.id)
            result["m3u_profile_name"] = _safe_display(getattr(m3u_profile, "name", None))
        if input_profile_id is not None:
            if not isinstance(input_profile_id, str) or not input_profile_id:
                return None
            from core.models import StreamProfile

            input_profile = StreamProfile.objects.filter(id=input_profile_id).only("id", "name").first()
            if input_profile is None:
                return None
            result["input_profile_id"] = str(input_profile.id)
            result["input_profile_name"] = _safe_display(getattr(input_profile, "name", None))
            # Keep the generic field tied to Dispatcharr's actual input profile.
            result["profile_id"] = str(input_profile.id)
            result["profile_name"] = result["input_profile_name"]
        return result
    except Exception:
        return None


def _source_for_diagnostic(channel_uuid, diagnostic):
    if not diagnostic:
        return None
    return {
        "mode": diagnostic.get("source", "unconfirmed"),
        "stream_id": diagnostic.get("stream_id"),
        "account_id": diagnostic.get("account_id"),
        "m3u_profile_id": diagnostic.get("m3u_profile_id"),
        "input_profile_id": diagnostic.get("input_profile_id") or None,
    }


def _mode(diagnostic):
    source = diagnostic.get("source") if diagnostic else None
    return source if source in {"dedicated", "borrowed"} else "unconfirmed"


def _state_for_channel(settings, channel_uuid, diagnostic, redis_client, now, *, evidence=None):
    if diagnostic is None:
        try:
            if int(redis_client.ttl(live_key(channel_uuid))) > 0:
                return "unknown", "unconfirmed", "attempt_binding_unconfirmed"
            error = redis_client.hgetall(last_error_key(channel_uuid))
            if error:
                # read_recorder_diagnostics normally validates and returns this;
                # never parse an unvalidated error hash here.
                return "unknown", "unconfirmed", "diagnostic_binding_unavailable"
        except Exception:
            return "unknown", "unconfirmed", "redis_observation_failed"
        return "unknown", "unconfirmed", "no_current_attempt_observed"
    phase = diagnostic.get("phase")
    mode = diagnostic.get("source", "unconfirmed")
    if phase == "error":
        return "error", mode, "reported_recorder_error"
    if phase == "cleanup_pending":
        return "cleanup_pending", mode, "recorder_cleanup_pending"
    if phase == "no_media_progress":
        return "no_media_progress", mode, "media_progress_not_confirmed"
    if phase == "starting":
        if evidence is not None and not evidence["available"]:
            return "unknown", mode, "archive_index_unavailable"
        if now - diagnostic["started_at"] <= DEFAULT_RECORDER_STARTUP_TIMEOUT_SECONDS:
            return "starting", mode, "recorder_starting"
        return "no_media_progress", mode, "startup_media_not_confirmed"
    if phase == "recording":
        evidence = evidence or _indexed_media_since(
            settings, channel_uuid, diagnostic["started_at"], now,
        )
        if not evidence["available"]:
            return "unknown", mode, "archive_index_unavailable"
        if evidence["count"] < 1:
            if now - diagnostic["started_at"] <= DEFAULT_RECORDER_STARTUP_TIMEOUT_SECONDS:
                return "starting", mode, "waiting_for_first_indexed_segment"
            return "no_media_progress", mode, "no_indexed_segment_since_attempt"
        if evidence["recent"]:
            return "recording", mode, "indexed_media_progress"
        return "no_media_progress", mode, "indexed_media_stalled"
    return "unknown", "unconfirmed", "diagnostic_phase_unrecognized"


def _indexed_media_since(settings, channel_uuid, started_at, now):
    try:
        archive_root = Path(settings["archive_root"]).expanduser()
        database = archive_root / "archive.sqlite3"
        if not database.is_file():
            return {"available": False, "count": 0, "recent": False}
        uri = "file:" + quote(str(database.resolve()), safe="/") + "?mode=ro"
        db = sqlite3.connect(uri, uri=True, timeout=0.25)
        try:
            db.execute("PRAGMA query_only=ON")
            row = db.execute(
                "SELECT COUNT(*), MAX(created_at) FROM segments "
                "WHERE channel_id=? AND created_at>=? AND created_at<=?",
                (channel_uuid, float(started_at),
                 float(now) + ARCHIVE_INDEX_FINALIZATION_GRACE_SECONDS),
            ).fetchone()
        finally:
            db.close()
        count, latest = int(row[0] or 0), row[1]
        fresh = (
            latest is not None
            and -ARCHIVE_INDEX_FINALIZATION_GRACE_SECONDS <= float(now) - float(latest)
            <= DEFAULT_MEDIA_IDLE_TIMEOUT_SECONDS + ARCHIVE_INDEX_FINALIZATION_GRACE_SECONDS
        )
        return {
            "available": True, "count": count, "recent": bool(count and fresh),
            "last_progress_at": float(latest) if latest is not None else None,
        }
    except (OSError, sqlite3.Error, TypeError, ValueError, KeyError):
        return {"available": False, "count": 0, "recent": False, "last_progress_at": None}


def _safe_display(value):
    if value is None:
        return None
    text = unicodedata.normalize("NFKC", str(value)).strip()
    if not _SAFE_DISPLAY.fullmatch(text) or _SENSITIVE_DISPLAY.search(text):
        return None
    return text or None


def _safe_identifier(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value) is not None


def _message_row(row):
    state = row["state"].replace("_", " ")
    parts = [f"{_display_name(row)} — {state}"]
    source = row.get("source")
    if source:
        details = [source.get("mode")]
        stream_name = source.get("stream_name")
        stream_id = source.get("stream_id")
        if stream_name:
            details.append(f"{stream_name} (id {stream_id})" if stream_id else stream_name)
        elif stream_id:
            details.append(f"stream id {stream_id}")
        provider_name, provider_id = source.get("provider_name"), source.get("provider_id")
        if provider_name:
            details.append(f"provider {provider_name} (id {provider_id})" if provider_id else f"provider {provider_name}")
        profile_name, profile_id = source.get("input_profile_name"), source.get("input_profile_id")
        if profile_name:
            details.append(f"Stream profile {profile_name} (id {profile_id})" if profile_id
                           else f"Stream profile {profile_name}")
        elif profile_id:
            details.append(f"Stream profile id {profile_id}")
        m3u_name, m3u_id = source.get("m3u_profile_name"), source.get("m3u_profile_id")
        if m3u_name:
            details.append(f"provider profile {m3u_name} (id {m3u_id})" if m3u_id
                           else f"provider profile {m3u_name}")
        parts.append("  Source: " + "; ".join(value for value in details if value))
    if row.get("last_confirmed_progress_none"):
        parts.append("  Last confirmed progress: none")
    elif row.get("last_confirmed_progress_at"):
        parts.append(f"  Last confirmed progress: {row['last_confirmed_progress_at']}")
    return "\n".join(parts)


def _scheduled_now(schedule_channels, timezone_name, channel_uuid, now):
    if not isinstance(schedule_channels, dict) or not isinstance(timezone_name, str):
        return False, True
    try:
        from .schedule import schedule_from_snapshot, schedule_is_active

        snapshot = schedule_channels[channel_uuid]
        parsed = schedule_from_snapshot(snapshot)
        return schedule_is_active(parsed, timezone_name, now), False
    except Exception:
        return False, True


def _channel_metadata(channel_ids):
    try:
        from .recording_schedule_overview import channel_metadata

        return channel_metadata(channel_ids)
    except Exception:
        return {}


def _unknown_all(settings, channel_ids, control_state, reason):
    metadata = _channel_metadata(channel_ids)
    rows = [
        {"channel_uuid": channel_id, "number": metadata.get(channel_id, {}).get("number"),
         "name": _safe_display(metadata.get(channel_id, {}).get("name")) or "Unknown channel",
         "state": "unknown", "mode": "unconfirmed", "reason": reason, "source": None,
         "last_confirmed_progress_at": None, "last_confirmed_progress_none": True}
        for channel_id in channel_ids
    ]
    rows.sort(key=_channel_sort_key)
    summary = {name: sum(row["state"] == name for row in rows) for name in _STATES}
    summary["problems"] = len(rows)
    return {
        "status": "ok",
        "message": "Current recordings\nProblems: " + str(len(rows)) + "\n" + "\n".join(
            f"{_message_row(row)}\n  Reason: {reason.replace('_', ' ')}" for row in rows
        ),
        "channels": rows,
        "summary": summary,
    }


def _unavailable(reason):
    return {"status": "unavailable", "message": "Current recordings are unavailable.",
            "channels": [], "summary": {**{name: 0 for name in _STATES}, "problems": 0},
            "reason": reason}


def _channel_sort_key(row):
    number = row.get("number")
    try:
        number_key = (0, float(number)) if number is not None else (1, 0.0)
    except (TypeError, ValueError):
        number_key = (1, 0.0)
    return number_key, str(row.get("name") or "").casefold(), row["channel_uuid"]


def _display_name(row):
    parts = [str(value) for value in (row.get("number"), row.get("name")) if value not in (None, "")]
    return " · ".join(parts) or "Unknown channel"
