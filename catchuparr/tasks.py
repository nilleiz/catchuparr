"""Celery recorder and cleanup tasks loaded by the Dispatcharr plugin manager."""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone

from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task(name="catchuparr.reconcile")
def reconcile_recorders():
    from apps.channels.models import Channel
    from core.utils import RedisClient

    from .runtime import load_config, require_supported_version

    require_supported_version()
    config = load_config()
    if config is None:
        return {"queued": 0}
    redis = RedisClient.get_client()
    desired = set(config.channel_uuids)
    valid = set(str(value) for value in Channel.objects.filter(uuid__in=desired).values_list("uuid", flat=True))
    queued = 0
    for channel in sorted(valid):
        lease_key = f"catchuparr:recorder:{channel}"
        dispatch_key = f"catchuparr:dispatch:{channel}"
        if redis.exists(lease_key):
            continue
        if redis.set(dispatch_key, "1", nx=True, ex=60):
            record_channel.apply_async(args=[channel], queue="dvr")
            queued += 1
    from .engine.store import ArchiveStore

    store = ArchiveStore(config.archive_root)
    store.cleanup(
        older_than_utc=datetime.now(timezone.utc) - timedelta(hours=config.retention_hours),
        max_bytes=config.max_storage_bytes,
    )
    if redis.set("catchuparr:orphan-reconcile", "1", nx=True, ex=600):
        store.reconcile_orphans(grace_seconds=3600)
    return {"queued": queued}


@shared_task(name="catchuparr.snapshot_epg")
def snapshot_epg():
    """Keep the schedule visible when Dispatcharr later replaces EPG rows."""
    from apps.channels.managers import with_effective_values
    from apps.channels.models import Channel

    from .engine.store import ArchiveStore
    from .runtime import load_config, require_supported_version

    require_supported_version()
    config = load_config()
    if config is None:
        return {"saved": 0}
    store = ArchiveStore(config.archive_root)
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=config.retention_hours)
    window_end = now + timedelta(days=2)
    channels = with_effective_values(
        Channel.objects.filter(uuid__in=config.channel_uuids),
        select_related_fks=True,
    ).select_related("epg_data__epg_source", "override__epg_data__epg_source")
    saved = 0
    for channel in channels:
        epg_data = channel.effective_epg_data_obj
        if epg_data is None:
            continue
        programs = epg_data.programs.filter(
            end_time__gt=window_start,
            start_time__lt=window_end,
        ).order_by("start_time")
        for program in programs.iterator(chunk_size=500):
            store.save_program_snapshot(
                str(channel.uuid), program.start_time, program.end_time,
                program.title or "",
                {
                    "description": program.description or "",
                    "epg_id": str(epg_data.id),
                    "source_program_id": str(program.id),
                },
            )
            saved += 1
    return {"saved": saved}


@shared_task(name="catchuparr.record_channel")
def record_channel(channel_uuid: str):
    from apps.channels.tasks import get_dvr_stream_base_url
    from core.utils import RedisClient

    from .engine.leases import RedisRecorderLease
    from .engine.recorder import FFmpegCopyRecorder
    from .engine.store import ArchiveStore
    from .runtime import load_config, require_supported_version

    require_supported_version()
    redis = RedisClient.get_client()
    redis.delete(f"catchuparr:dispatch:{channel_uuid}")
    config = load_config()
    if config is None or channel_uuid not in config.channel_uuids:
        return {"status": "disabled"}
    lease = RedisRecorderLease(redis, channel_uuid, ttl_seconds=30)
    fence = lease.acquire()
    if fence is None:
        return {"status": "already_running"}
    store = ArchiveStore(config.archive_root)
    store.register_recorder_fence(channel_uuid, fence)
    stop_event = threading.Event()
    # Dispatcharr's DVR helper accounts for modular and AIO deployments.
    proxy_url = f"{get_dvr_stream_base_url().rstrip('/')}/proxy/ts/stream/{channel_uuid}"
    recorder = FFmpegCopyRecorder(
        store, channel_uuid, proxy_url, config.archive_root / "work",
        fencing_token=fence,
        on_error=lambda message: logger.warning("%s: %s", channel_uuid, message),
    )

    def supervise():
        while not stop_event.wait(10):
            try:
                current = load_config()
                if current is None or channel_uuid not in current.channel_uuids or not lease.renew():
                    stop_event.set()
                    return
            except Exception:
                logger.exception("Recorder supervision failed for %s", channel_uuid)
                stop_event.set()
                return

    monitor = threading.Thread(target=supervise, name=f"catchuparr-{channel_uuid}", daemon=True)
    monitor.start()
    try:
        recorder.run_forever(stop_event)
    finally:
        stop_event.set()
        monitor.join(timeout=12)
        lease.release()
    return {"status": "stopped"}
