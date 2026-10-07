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
    from .configuration import load_active_configuration
    from .recorder_proxy import (
        candidate_is_current,
        configuration_generation,
        issue_recorder_attempt,
        ranked_source_candidates,
        stop_recorder_attempt,
    )
    from .adapters.recorder_proxy import core_api_supported, install_proxyserver_cleanup_hook

    require_supported_version()
    redis = RedisClient.get_client()
    redis.delete(f"catchuparr:dispatch:{channel_uuid}")
    config = load_config()
    if config is None or channel_uuid not in config.channel_uuids:
        return {"status": "disabled"}
    store = ArchiveStore(config.archive_root)
    lease = RedisRecorderLease(
        redis, channel_uuid, ttl_seconds=30, archive_store=store
    )
    fence = lease.acquire()
    if fence is None:
        return {"status": "already_running"}
    stop_event = None
    monitor = None
    attempt_state_lock = threading.Lock()
    attempt_state = {"attempt": None, "candidate": None}
    try:
        stop_event = threading.Event()
        active = load_active_configuration()
        generation = configuration_generation(active) if active is not None else ""
        candidates = ranked_source_candidates(channel_uuid, active)
        # A missing policy or mode=unchanged deliberately keeps Dispatcharr's
        # channel URL and its shared live worker.
        if candidates is not None and not candidates:
            return {"status": "no_permitted_sources"}
        if candidates is not None and not core_api_supported():
            logger.error("Recorder source overrides disabled for channel %s: unverified core API", channel_uuid)
            return {"status": "proxy_integration_unsupported"}
        if candidates is not None and not install_proxyserver_cleanup_hook():
            logger.error("Recorder source overrides disabled for channel %s: cleanup guard unavailable", channel_uuid)
            return {"status": "proxy_integration_unsupported"}

        # Dispatcharr's DVR helper accounts for modular and AIO deployments.
        proxy_base_url = get_dvr_stream_base_url().rstrip("/")
        proxy_url = f"{proxy_base_url}/proxy/ts/stream/{channel_uuid}"

        def supervise():
            while not stop_event.wait(10):
                try:
                    current = load_config()
                    current_active = load_active_configuration()
                    current_generation = (
                        configuration_generation(current_active)
                        if current_active is not None else ""
                    )
                    if (
                        current is None
                        or channel_uuid not in current.channel_uuids
                        or current_generation != generation
                        or not lease.renew()
                    ):
                        stop_event.set()
                        return
                    with attempt_state_lock:
                        current_attempt = attempt_state["attempt"]
                        current_candidate = attempt_state["candidate"]
                    if current_attempt is not None and not current_attempt.renew(redis):
                        stop_event.set()
                        return
                    if current_candidate is not None and not candidate_is_current(
                        channel_uuid,
                        str(current_candidate.get("id") or current_candidate.get("stream_id") or ""),
                        str(current_candidate.get("account_id") or ""),
                        current_active,
                    ):
                        stop_event.set()
                        return
                except Exception:
                    logger.exception("Recorder supervision failed for %s", channel_uuid)
                    stop_event.set()
                    return

        monitor = threading.Thread(
            target=supervise, name=f"catchuparr-{channel_uuid}", daemon=True
        )
        monitor.start()
        if candidates is None:
            recorder = FFmpegCopyRecorder(
                store, channel_uuid, proxy_url, config.archive_root / "work",
                fencing_token=fence,
                on_error=lambda message: logger.warning("%s: %s", channel_uuid, message),
            )
            recorder.run_forever(stop_event)
        else:
            for index, candidate in enumerate(candidates):
                if stop_event.is_set():
                    break
                attempt = issue_recorder_attempt(
                    redis,
                    lease,
                    channel_uuid=channel_uuid,
                    candidate=candidate,
                    config_generation=generation,
                    internal_base_url=proxy_base_url,
                )
                with attempt_state_lock:
                    attempt_state["attempt"] = attempt
                    attempt_state["candidate"] = candidate
                recorder = FFmpegCopyRecorder(
                    store,
                    channel_uuid,
                    attempt.input_url,
                    config.archive_root / "work",
                    fencing_token=fence,
                    input_headers=attempt.input_headers,
                    require_media_progress=True,
                )
                if index:
                    recorder._mark_next_discontinuity = True
                try:
                    result = recorder.run_candidate(
                        stop_event,
                        startup_timeout=60,
                        media_idle_timeout=120,
                    )
                except Exception:
                    # Exception text from HTTP/FFmpeg libraries can contain
                    # request details, so log only the stable source ID.
                    logger.warning(
                        "Recorder candidate %s failed for channel %s",
                        attempt.stream_id,
                        channel_uuid,
                    )
                    result = None
                finally:
                    attempt.revoke(redis)
                    cleanup_ok = False
                    try:
                        cleanup_ok = stop_recorder_attempt(redis, attempt, lease)
                    except Exception:
                        logger.exception(
                            "Could not stop recorder worker for channel %s source %s",
                            channel_uuid,
                            attempt.stream_id,
                        )
                    if not cleanup_ok:
                        stop_event.set()
                    with attempt_state_lock:
                        if attempt_state["attempt"] is attempt:
                            attempt_state["attempt"] = None
                            attempt_state["candidate"] = None
                if result is not None and result.status == "stopped":
                    break
    finally:
        try:
            if stop_event is not None:
                stop_event.set()
            if monitor is not None and monitor.ident is not None:
                monitor.join(timeout=12)
        finally:
            lease.release()
    return {"status": "stopped"}
