"""Small Dispatcharr-specific lifecycle and configuration bridge."""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from pathlib import Path

from .compatibility import (
    SUPPORTED_DISPATCHARR_VERSION,
    is_supported_dispatcharr_version,
)

logger = logging.getLogger(__name__)
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


def load_config() -> Config | None:
    from apps.plugins.models import PluginConfig

    plugin = PluginConfig.objects.filter(key=PLUGIN_KEY, enabled=True).first()
    if plugin is None:
        return None
    return parse_settings(plugin.settings or {})


def require_supported_version() -> None:
    try:
        from version import __version__ as dispatcharr_version
    except ImportError as exc:
        raise RuntimeError("Catchuparr requires Dispatcharr") from exc
    if not is_supported_dispatcharr_version(dispatcharr_version):
        raise RuntimeError(f"Unsupported Dispatcharr version: {dispatcharr_version}")


def bootstrap() -> None:
    """Register tasks/routes only for the inspected Dispatcharr version."""
    try:
        require_supported_version()
    except RuntimeError as exc:
        logger.error("Catchuparr runtime disabled: %s", exc)
        return
    # Import the tasks in every worker so Celery sees plugin task names.
    from . import tasks  # noqa: F401

    if "celery" not in " ".join(sys.argv).lower():
        try:
            from .views import install_routes

            install_routes()
        except Exception:
            logger.exception("Catchuparr route installation failed")
    try:
        _ensure_schedule()
    except Exception:
        # Migrations may not have completed during first discovery. An admin
        # reconcile action can retry; never start uncoordinated recorders.
        logger.exception("Catchuparr periodic reconciliation was not installed")


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
    try:
        from django_celery_beat.models import PeriodicTask

        PeriodicTask.objects.filter(
            name__in=(RECONCILE_SCHEDULE, SNAPSHOT_SCHEDULE)
        ).update(enabled=False)
    except Exception:
        logger.exception("Failed to disable Catchuparr scheduler")
    try:
        from .views import uninstall_routes

        uninstall_routes()
    except Exception:
        logger.exception("Failed to remove Catchuparr routes")


def reconcile() -> None:
    require_supported_version()
    _ensure_schedule()
    from .tasks import reconcile_recorders, snapshot_epg

    reconcile_recorders.apply_async(queue="dvr")
    snapshot_epg.apply_async(queue="dvr")


def status(settings: dict) -> dict:
    from .engine.store import ArchiveStore

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
        channels.append({
            "uuid": channel,
            "name": names.get(channel),
            **stats,
            "recorder_running": recorder_running,
        })
    return {
        "channels": channels,
        "archive_root": str(config.archive_root),
        "retention_hours": config.retention_hours,
        "max_storage_bytes": config.max_storage_bytes,
        "indexed_storage_bytes": store.indexed_size_bytes(),
    }


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
