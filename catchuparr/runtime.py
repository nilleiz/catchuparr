"""Small Dispatcharr-specific lifecycle and configuration bridge."""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

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


def load_config(active_snapshot: dict | None = None) -> Config | None:
    from apps.plugins.models import PluginConfig

    from .configuration import load_active_configuration

    plugin = PluginConfig.objects.filter(key=PLUGIN_KEY, enabled=True).first()
    if plugin is None:
        return None
    active = active_snapshot if active_snapshot is not None else load_active_configuration()
    if active is None:
        return None
    return parse_settings(active)


def load_plugin_settings(fallback: dict | None = None) -> dict:
    """Read the editable draft through Dispatcharr's existing PluginConfig row."""
    from .configuration import load_draft_settings

    return load_draft_settings(fallback)


def load_runtime_settings(fallback: dict | None = None) -> dict:
    """Use applied IDs, or draft base settings with no selected channels."""
    from .configuration import load_active_configuration, reset_legacy_configuration

    reset_legacy_configuration()
    active = load_active_configuration()
    if active is not None:
        return active
    draft = load_plugin_settings(fallback)
    draft.pop("channel_uuids", None)
    draft.pop("source_rules", None)
    draft["channel_uuids"] = ""
    return draft


def validate_configuration(settings: dict) -> dict:
    from .configuration import validate_configuration as validate

    return validate(settings)


def apply_configuration(settings: dict | None = None) -> dict:
    from .configuration import apply_configuration as apply
    from .logging_utils import event

    result = apply(settings)
    event(
        "configuration_applied",
        channel_count=result["selected_channel_count"],
        source_policy_count=result["source_policy_count"],
    )
    return result


def apply_recorder_control() -> dict:
    from .logging_utils import event
    from .recorder_control import apply_recorder_control as apply

    state = apply()
    event("control_applied", paused=state.paused, control_generation=state.generation)
    _enqueue_recorder_reconcile()
    return {"paused": state.paused, "generation": state.generation}


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
    from .logging_utils import apply_log_level, error, event

    try:
        from .configuration import load_draft_settings

        apply_log_level(load_draft_settings().get("log_level", "INFO"))
    except Exception:
        apply_log_level("INFO")
    try:
        require_supported_version()
    except RuntimeError:
        event("runtime_disabled", logging.ERROR, reason="version")
        return
    from .configuration import reset_legacy_configuration

    reset_legacy_configuration()
    # Import the tasks in every worker so Celery sees plugin task names.
    from . import tasks  # noqa: F401

    if "celery" not in " ".join(sys.argv).lower():
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


def status(settings: dict) -> dict:
    from .engine.store import ArchiveStore
    from .recorder_control import RecorderControlError, load_recorder_control
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
    control_state = None
    try:
        control_state = load_recorder_control()
    except RecorderControlError:
        pass
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
        result["recording_enabled"] = control_state.recording_enabled
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
    user_id = int(settings.get("playback_user_id") or 0)
    if user_id <= 0 or not User.objects.filter(id=user_id).exists():
        raise ValueError("Set playback_user_id to an existing Dispatcharr user ID")
    token = AccessTokenStore(config.archive_root).issue(user_id)
    # The admin action returns this one-time value; only its digest is stored.
    return {"user_id": user_id, "access_token": token, "warning": "Copy once; URL is sensitive"}
