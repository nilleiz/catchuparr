"""Small Dispatcharr-specific lifecycle and configuration bridge."""

from __future__ import annotations

import ipaddress
import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from .compatibility import (
    SUPPORTED_DISPATCHARR_VERSION as SUPPORTED_DISPATCHARR_VERSION,
)
from .compatibility import (
    is_supported_dispatcharr_version,
)

PLUGIN_KEY = "catchuparr"
RECONCILE_TASK = "catchuparr.reconcile"
RECONCILE_SCHEDULE = "catchuparr-recorder-reconcile"
SNAPSHOT_TASK = "catchuparr.snapshot_epg"
SNAPSHOT_SCHEDULE = "catchuparr-epg-snapshot"


@dataclass(frozen=True)
class Config:
    channel_uuids: tuple[str, ...]
    archive_root: Path
    retention_hours: int
    max_storage_bytes: int


def parse_settings(settings: dict) -> Config:
    import uuid

    raw_channels = str(settings.get("channel_uuids") or "")
    values: list[str] = []
    for raw in raw_channels.replace(",", "\n").splitlines():
        candidate = raw.strip()
        if not candidate:
            continue
        values.append(str(uuid.UUID(candidate)))
    retention_hours = int(settings.get("retention_hours", 24))
    max_gib = int(settings.get("max_storage_gib", 20))
    if retention_hours < 1 or retention_hours > 720:
        raise ValueError("retention_hours must be between 1 and 720")
    if max_gib < 1 or max_gib > 10240:
        raise ValueError("max_storage_gib must be between 1 and 10240")
    root = Path(settings.get("archive_root") or "/data/catchuparr").expanduser()
    if not root.is_absolute():
        raise ValueError("archive_root must be an absolute path")
    return Config(tuple(dict.fromkeys(values)), root, retention_hours, max_gib * 1024**3)


def normalize_public_base_url(value) -> str:
    """Validate a configured external base URL used for one-time token links."""
    if not isinstance(value, str):
        raise ValueError("public_base_url must be text")
    value = value.strip()
    if not value:
        return ""
    if any(character.isspace() or ord(character) < 0x20 for character in value):
        raise ValueError("public_base_url must be an absolute http(s) base URL")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ValueError("public_base_url must be an absolute http(s) base URL") from None
    if (
        parsed.scheme.lower() not in {"http", "https"}
        or not parsed.netloc
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or "?" in value
        or "#" in value
        or "\\" in parsed.path
    ):
        raise ValueError("public_base_url must be an absolute http(s) base URL")
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("public_base_url port is invalid")
    hostname = parsed.hostname
    if ":" in hostname:
        try:
            ipaddress.IPv6Address(hostname)
        except ipaddress.AddressValueError:
            raise ValueError("public_base_url host is invalid") from None
    else:
        try:
            ascii_hostname = hostname.encode("idna").decode("ascii")
        except UnicodeError:
            raise ValueError("public_base_url host is invalid") from None
        labels = ascii_hostname.rstrip(".").split(".")
        if not labels or any(
            not label
            or len(label) > 63
            or not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?", label)
            for label in labels
        ):
            raise ValueError("public_base_url host is invalid")
    decoded_path = unquote(parsed.path)
    if (
        decoded_path.startswith("//")
        or "\\" in decoded_path
        or (
            decoded_path
            and re.fullmatch(r"/[A-Za-z0-9._~!$&'()*+,;=:@%/-]*", decoded_path) is None
        )
    ):
        raise ValueError("public_base_url path is invalid")
    path_parts = decoded_path.split("/")
    if any(part in {".", ".."} for part in path_parts):
        raise ValueError("public_base_url path is invalid")
    return urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc,
            parsed.path.rstrip("/"),
            "",
            "",
        )
    )


def load_config(active_snapshot: dict | None = None) -> Config | None:
    from apps.plugins.models import PluginConfig

    from .configuration import load_active_configuration

    plugin = PluginConfig.objects.filter(key=PLUGIN_KEY, enabled=True).first()
    if plugin is None:
        return None
    active = active_snapshot if active_snapshot is not None else load_active_configuration()
    from .logging_utils import apply_log_level

    apply_log_level(active.get("log_level", "INFO") if active else "INFO")
    if active is None:
        return None
    return parse_settings(active)


def load_plugin_settings(fallback: dict | None = None) -> dict:
    """Read the editable draft through Dispatcharr's existing PluginConfig row."""
    from .configuration import load_draft_settings

    return load_draft_settings(fallback)


def load_runtime_settings() -> dict:
    """Return only committed settings; an unapplied draft never reaches runtime."""
    settings, _control = load_runtime_state()
    return settings


def load_runtime_state() -> tuple[dict, object]:
    """Read applied settings while keeping archive access independent of recorder state."""
    from .configuration import (
        load_active_configuration,
        load_applied_state,
        reset_legacy_configuration,
    )
    from .recorder_control import RecorderControlError

    reset_legacy_configuration()
    try:
        active, control = load_applied_state()
    except RecorderControlError:
        # A broken recorder sidecar denies new recording but must not disable
        # archive playback, status, or token management.
        active = load_active_configuration()
        control = None
    if active is not None:
        runtime_settings = dict(active)
        runtime_settings["recording_enabled"] = (
            control.recording_enabled
            if control is not None
            else False
        )
        return runtime_settings, control
    return {
        "archive_root": "/data/catchuparr",
        "retention_hours": 24,
        "max_storage_gib": 20,
        "playback_user_id": 0,
        "public_base_url": "",
        "recording_enabled": False,
        "log_level": "INFO",
        "channel_uuids": "",
        "channel_profile_ids": [],
        "source_policies": {},
    }, control


def apply_committed_log_level(settings: dict | None = None) -> str:
    """Reload the logger level only from the active, committed snapshot."""
    from .configuration import load_active_configuration, load_applied_state
    from .logging_utils import apply_log_level

    if settings is not None:
        active = settings
    else:
        try:
            active, _control = load_applied_state()
        except Exception:
            active = load_active_configuration()
    return apply_log_level(active.get("log_level", "INFO") if active else "INFO")


def validate_configuration(settings: dict) -> dict:
    from .configuration import validate_configuration as validate

    return validate(settings)


def apply_configuration(settings: dict | None = None) -> dict:
    from .configuration import apply_configuration as apply
    from .configuration import load_active_configuration, load_applied_state
    from .logging_utils import apply_log_level, event
    from .recorder_control import RecorderControlError

    result = apply(settings)
    try:
        active, _control = load_applied_state()
    except RecorderControlError:
        if not result.get("recording_paused"):
            raise
        active = load_active_configuration()
    apply_log_level(active.get("log_level", "INFO") if active else "INFO")
    event_fields = {
        "recording_paused": bool(result.get("recording_paused", False)),
    }
    if result.get("applied") is True:
        event(
            "configuration_applied",
            channel_count=result["selected_channel_count"],
            source_policy_count=result["source_policy_count"],
            **event_fields,
        )
    elif result.get("outcome_unknown") is True:
        event(
            "configuration_outcome_unknown",
            logging.WARNING,
            outcome_unknown=True,
            recovery_required=True,
            **event_fields,
        )
    elif result.get("activation_pending") is True:
        event(
            "configuration_activation_pending",
            logging.WARNING,
            activation_pending=True,
            recovery_required=bool(result.get("recovery_required", True)),
            **event_fields,
        )
    else:
        event(
            "configuration_recovery_required",
            logging.WARNING,
            recovery_required=True,
            **event_fields,
        )
    return result


def pause_recorders() -> dict:
    from .logging_utils import event
    from .recorder_control import pause_recorders as pause

    state = pause()
    event("control_paused", paused=True, control_generation=state.generation)
    _enqueue_recorder_reconcile()
    return {"paused": state.paused, "generation": state.generation}


def resume_recorders() -> dict:
    from .logging_utils import event
    from .recorder_control import resume_recorders as resume

    state = resume()
    event("control_resumed", paused=False, control_generation=state.generation)
    _enqueue_recorder_reconcile()
    return {"paused": state.paused, "generation": state.generation}


def _enqueue_recorder_reconcile() -> None:
    from .logging_utils import error

    try:
        require_supported_version()
        from .tasks import reconcile_recorders

        reconcile_recorders.apply_async(queue="dvr")
    except Exception:
        # The durable control or configuration change is already authoritative;
        # the regular periodic task can retry this best-effort wake-up.
        error("schedule_install_failed")


def active_configuration(settings: dict) -> dict:
    """Return applied filters; absent state has no selected channels or overrides."""
    from .configuration import load_active_configuration

    return load_active_configuration() or {
        "channel_uuids": "",
        "channel_profile_ids": [],
        "source_policies": {},
    }


def require_supported_version() -> None:
    try:
        from version import __version__ as dispatcharr_version
    except ImportError as exc:
        raise RuntimeError("Catchuparr requires Dispatcharr") from exc
    if not is_supported_dispatcharr_version(dispatcharr_version):
        raise RuntimeError(f"Unsupported Dispatcharr version: {dispatcharr_version}")


def bootstrap() -> None:
    """Register tasks/routes only for the inspected Dispatcharr version."""
    from .logging_utils import error, event

    try:
        apply_committed_log_level()
    except Exception:
        from .logging_utils import apply_log_level

        apply_log_level("INFO")
    try:
        require_supported_version()
    except RuntimeError:
        event("runtime_disabled", logging.ERROR, reason="version")
        return
    from .configuration import recover_interrupted_activation, reset_legacy_configuration

    try:
        recover_interrupted_activation()
        reset_legacy_configuration()
    except Exception:
        # An unresolved activation keeps recorder admission denied and the
        # last validated bundle in use. Bootstrap can still expose playback
        # and status paths against that bundle.
        error("configuration_recovery_failed")
    # Import the tasks in every worker so Celery sees plugin task names.
    from . import tasks  # noqa: F401

    celery_process = "celery" in " ".join(sys.argv).lower()
    if not celery_process:
        try:
            from .views import install_routes

            install_routes()
        except Exception:
            error("route_install_failed")
        try:
            from .xc_runtime import install_xc_integration

            result = install_xc_integration()
            if not result.installed:
                event("runtime_disabled", logging.WARNING, reason="xc_hooks")
        except Exception:
            error("xc_install_failed")
    try:
        from .stats import install_stats_hooks

        if not install_stats_hooks(route_hooks=not celery_process):
            event("runtime_disabled", logging.WARNING, reason="stats_hooks")
    except Exception:
        error("stats_hook_install_failed")
    try:
        _ensure_schedule()
    except Exception:
        # Migrations may not have completed during first discovery. An admin
        # reconcile action can retry; never start uncoordinated recorders.
        error("schedule_install_failed")


def _ensure_schedule() -> None:
    require_supported_version()
    from django_celery_beat.models import IntervalSchedule, PeriodicTask

    interval, _ = IntervalSchedule.objects.get_or_create(every=30, period=IntervalSchedule.SECONDS)
    PeriodicTask.objects.update_or_create(
        name=RECONCILE_SCHEDULE,
        defaults={"task": RECONCILE_TASK, "interval": interval, "queue": "dvr", "enabled": True},
    )
    snapshot_interval, _ = IntervalSchedule.objects.get_or_create(
        every=300, period=IntervalSchedule.SECONDS
    )
    PeriodicTask.objects.update_or_create(
        name=SNAPSHOT_SCHEDULE,
        defaults={
            "task": SNAPSHOT_TASK, "interval": snapshot_interval,
            "queue": "dvr", "enabled": True,
        },
    )


def shutdown() -> None:
    from .logging_utils import error, event

    try:
        from .stats import uninstall_stats_hooks

        if not uninstall_stats_hooks():
            event("runtime_disabled", logging.WARNING, reason="stats_uninstall")
    except Exception:
        error("stats_hook_uninstall_failed")
    try:
        from .xc_runtime import uninstall_xc_integration

        result = uninstall_xc_integration()
        if not result.installed:
            event("runtime_disabled", logging.WARNING, reason="xc_uninstall")
    except Exception:
        error("xc_uninstall_failed")
    try:
        from django_celery_beat.models import PeriodicTask

        PeriodicTask.objects.filter(
            name__in=(RECONCILE_SCHEDULE, SNAPSHOT_SCHEDULE)
        ).update(enabled=False)
    except Exception:
        error("scheduler_disable_failed")
    try:
        from .views import uninstall_routes

        uninstall_routes()
    except Exception:
        error("route_uninstall_failed")


def reconcile() -> None:
    require_supported_version()
    _ensure_schedule()
    from .tasks import reconcile_recorders, snapshot_epg

    reconcile_recorders.apply_async(queue="dvr")
    snapshot_epg.apply_async(queue="dvr")


def status(settings: dict, *, control_state=None) -> dict:
    from .engine.store import ArchiveStore
    from .recorder_control import RecorderControlError
    from .schedule import ScheduleError, schedule_from_snapshot, schedule_is_active

    config = parse_settings(settings)
    store = ArchiveStore(config.archive_root)
    try:
        from apps.channels.models import Channel

        names = {
            str(uuid): name
            for uuid, name in Channel.objects.filter(uuid__in=config.channel_uuids).values_list(
                "uuid", "name"
            )
        }
    except Exception:
        names = {}
    try:
        from core.utils import RedisClient

        redis = RedisClient.get_client()
        redis.ping()
    except Exception:
        redis = None
    if control_state is None:
        try:
            from .configuration import load_applied_state

            active, control_state = load_applied_state()
            if active is not None:
                settings = active
                config = parse_settings(settings)
                store = ArchiveStore(config.archive_root)
        except RecorderControlError:
            from .configuration import load_active_configuration

            settings = load_active_configuration() or settings
            config = parse_settings(settings)
            store = ArchiveStore(config.archive_root)
            control_state = None
    schedule_config = settings.get("recording_schedule", {})
    timezone_name = schedule_config.get("timezone") if isinstance(schedule_config, dict) else None
    schedules = schedule_config.get("channels") if isinstance(schedule_config, dict) else None
    channels = []
    for channel in config.channel_uuids:
        stats = store.channel_stats(channel)
        if redis is None:
            recorder_running = None
        else:
            try:
                recorder_running = bool(redis.exists(f"catchuparr:recorder:{channel}"))
            except Exception:
                recorder_running = None
        channel_status = {
            "uuid": channel,
            "name": names.get(channel),
            **stats,
            "recorder_running": recorder_running,
        }
        if isinstance(schedules, dict) and isinstance(timezone_name, str):
            try:
                schedule = schedule_from_snapshot(schedules[channel])
                channel_status["recording_scheduled"] = schedule_is_active(
                    schedule, timezone_name, datetime.now(timezone.utc)
                )
            except (KeyError, ScheduleError, ValueError):
                channel_status["recording_scheduled"] = None
        channels.append(channel_status)
    from .configuration import configuration_reset_required

    result = {
        "channels": channels,
        "archive_root": str(config.archive_root),
        "retention_hours": config.retention_hours,
        "max_storage_bytes": config.max_storage_bytes,
        "indexed_storage_bytes": store.indexed_size_bytes(),
        "recording_control_available": control_state is not None,
    }
    if control_state is not None:
        result["recording_enabled"] = (
            control_state.recording_enabled and type(settings.get("version")) is int
        )
        result["control_generation"] = control_state.generation
    if configuration_reset_required():
        result["configuration_status"] = (
            "Previous channel selection was cleared. Validate and apply filter_config to resume recording."
        )
    elif not config.channel_uuids:
        result["configuration_status"] = "No channels are selected. Validate and apply filter_config."
    return result


def create_access_token(settings: dict) -> dict:
    require_supported_version()
    from apps.accounts.models import User

    from .security import AccessTokenStore

    config = parse_settings(settings)
    base_url = normalize_public_base_url(settings.get("public_base_url", ""))
    if not base_url:
        raise ValueError("Set public_base_url before creating an access token")
    user_id = int(settings.get("playback_user_id") or 0)
    if user_id <= 0 or not User.objects.filter(id=user_id).exists():
        raise ValueError("Set playback_user_id to an existing Dispatcharr user ID")
    token = AccessTokenStore(config.archive_root).issue(user_id)
    encoded_token = quote(token, safe="")
    playlist_url = f"{base_url}/catchuparr/m3u?access_token={encoded_token}"
    xmltv_url = f"{base_url}/catchuparr/xmltv?access_token={encoded_token}"
    # The admin action returns the one-time token and both authorized links;
    # only its digest is stored. Plain text stays selectable in the action toast.
    message = f"M3U playlist URL: {playlist_url}\nXMLTV EPG URL: {xmltv_url}"
    return {
        "user_id": user_id,
        "access_token": token,
        "playlist_url": playlist_url,
        "xmltv_url": xmltv_url,
        "message": message,
        "warning": "Copy once; authenticated links are sensitive",
    }
