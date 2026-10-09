"""Celery recorder and cleanup tasks loaded by the Dispatcharr plugin manager."""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

from celery import shared_task


@shared_task(name="catchuparr.reconcile")
def reconcile_recorders():
    from apps.channels.models import Channel
    from core.utils import RedisClient

    from .configuration import load_active_configuration, reset_legacy_configuration
    from .engine.store import ArchiveStore
    from .recorder_control import RecorderControlError, load_recorder_control
    from .recorder_proxy import configuration_generation
    from .runtime import load_config, require_supported_version
    from .schedule import ScheduleError, schedule_from_snapshot, schedule_is_active

    require_supported_version()
    reset_legacy_configuration()
    config = load_config()
    queued = 0
    redis = RedisClient.get_client()
    active = load_active_configuration() if config is not None else None
    if config is not None and active is not None:
        generation = configuration_generation(active)
        desired = set(config.channel_uuids)
        valid = set(
            str(value)
            for value in Channel.objects.filter(uuid__in=desired).values_list("uuid", flat=True)
        )
        try:
            control = load_recorder_control()
        except RecorderControlError:
            control = None
        if control is not None and not control.paused:
            schedules = active.get("recording_schedule", {})
            timezone_name = schedules.get("timezone") if isinstance(schedules, dict) else None
            channel_schedules = schedules.get("channels") if isinstance(schedules, dict) else None
            if isinstance(timezone_name, str) and isinstance(channel_schedules, dict):
                now = datetime.now(timezone.utc)
                for channel in sorted(valid):
                    try:
                        schedule = schedule_from_snapshot(channel_schedules[channel])
                        if not schedule_is_active(schedule, timezone_name, now):
                            continue
                    except (KeyError, ScheduleError, ValueError):
                        continue
                    lease_key = f"catchuparr:recorder:{channel}"
                    dispatch_key = f"catchuparr:dispatch:{channel}"
                    if redis.exists(lease_key):
                        continue
                    if redis.set(dispatch_key, "1", nx=True, ex=60):
                        record_channel.apply_async(
                            args=[channel, generation, control.generation], queue="dvr"
                        )
                        queued += 1
    if config is not None:
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

    from .configuration import reset_legacy_configuration
    from .engine.store import ArchiveStore
    from .runtime import load_config, require_supported_version

    require_supported_version()
    reset_legacy_configuration()
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
def record_channel(
    channel_uuid: str,
    expected_generation: str | None = None,
    expected_control_generation: int | None = None,
):
    from apps.channels.tasks import get_dvr_stream_base_url
    from core.utils import RedisClient

    from .adapters.recorder_proxy import core_api_supported, install_proxyserver_cleanup_hook
    from .configuration import reset_legacy_configuration
    from .engine.leases import RedisRecorderLease
    from .engine.recorder import FFmpegCopyRecorder
    from .engine.store import ArchiveStore
    from .logging_utils import error, event
    from .recorder_proxy import (
        candidate_is_current,
        issue_recorder_attempt,
        ranked_source_candidates,
        stop_recorder_attempt,
    )
    from .runtime import require_supported_version

    require_supported_version()
    reset_legacy_configuration()
    redis = RedisClient.get_client()
    redis.delete(f"catchuparr:dispatch:{channel_uuid}")
    if not isinstance(expected_generation, str) or not expected_generation:
        return {"status": "legacy_job"}
    if type(expected_control_generation) is not int or expected_control_generation < 0:
        return {"status": "legacy_job"}
    state = _recorder_admission_state(
        channel_uuid, expected_generation, expected_control_generation
    )
    if state["status"] != "ready":
        return {"status": state["status"]}
    config = state["config"]
    active = state["active"]
    generation = expected_generation
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
        candidates = ranked_source_candidates(channel_uuid, active)
        # A missing filter override keeps Dispatcharr's default live route and
        # its shared worker.
        if candidates is not None and not candidates:
            return {"status": "no_permitted_sources"}
        if candidates is not None and not core_api_supported():
            event("runtime_disabled", level=40, reason="proxy_api")
            return {"status": "proxy_integration_unsupported"}
        if candidates is not None and not install_proxyserver_cleanup_hook():
            event("runtime_disabled", level=40, reason="cleanup_guard")
            return {"status": "proxy_integration_unsupported"}

        # Dispatcharr's DVR helper accounts for modular and AIO deployments.
        proxy_base_url = get_dvr_stream_base_url().rstrip("/")
        proxy_url = f"{proxy_base_url}/proxy/ts/stream/{channel_uuid}"

        def supervise():
            while not stop_event.wait(10):
                try:
                    current_admission = _recorder_admission_state(
                        channel_uuid, expected_generation, expected_control_generation
                    )
                    if (
                        current_admission["status"] != "ready" or not lease.renew()
                    ):
                        stop_event.set()
                        return
                    current_active = current_admission["active"]
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
                    error("supervision_failed")
                    stop_event.set()
                    return

        monitor = threading.Thread(
            target=supervise, name=f"catchuparr-{channel_uuid}", daemon=True
        )
        monitor.start()
        if candidates is None:
            current_state = _recorder_admission_state(
                channel_uuid, expected_generation, expected_control_generation
            )
            if current_state["status"] != "ready":
                return {"status": current_state["status"]}
            recorder = FFmpegCopyRecorder(
                store, channel_uuid, proxy_url, config.archive_root / "work",
                fencing_token=fence,
                on_error=lambda _message: error("recorder_worker_stopped"),
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
                current_state = _recorder_admission_state(
                    channel_uuid, expected_generation, expected_control_generation
                )
                if current_state["status"] != "ready":
                    attempt.revoke(redis)
                    stop_recorder_attempt(redis, attempt, lease)
                    with attempt_state_lock:
                        attempt_state["attempt"] = None
                        attempt_state["candidate"] = None
                    return {"status": current_state["status"]}
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
                    error("recorder_worker_stopped")
                    result = None
                finally:
                    attempt.revoke(redis)
                    cleanup_ok = False
                    try:
                        cleanup_ok = stop_recorder_attempt(redis, attempt, lease)
                    except Exception:
                        error("supervision_failed")
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


def _active_schedule(active, channel_uuid):
    from .schedule import ScheduleError, schedule_from_snapshot, validate_timezone

    schedule_config = active.get("recording_schedule") if isinstance(active, dict) else None
    if not isinstance(schedule_config, dict):
        raise ScheduleError("active schedule is missing")
    timezone_name = validate_timezone(schedule_config.get("timezone"))
    schedules = schedule_config.get("channels")
    if not isinstance(schedules, dict) or channel_uuid not in schedules:
        raise ScheduleError("channel schedule is missing")
    return schedule_from_snapshot(schedules[channel_uuid]), timezone_name


def _recorder_admission_state(channel_uuid, expected_generation, expected_control_generation):
    from .configuration import load_active_configuration
    from .logging_utils import error, event
    from .recorder_control import RecorderControlError, load_recorder_control
    from .recorder_proxy import configuration_generation
    from .runtime import load_config
    from .schedule import ScheduleError, schedule_is_active

    try:
        control = load_recorder_control()
    except RecorderControlError:
        error("control_state_invalid")
        return {"status": "control_unavailable"}
    if control.paused:
        return {"status": "recording_paused"}
    if control.generation != expected_control_generation:
        event("recorder_stale_job")
        return {"status": "stale_control"}
    active = load_active_configuration()
    if active is None:
        event("recorder_disabled")
        return {"status": "disabled"}
    config = load_config(active_snapshot=active)
    if config is None or channel_uuid not in config.channel_uuids:
        event("recorder_disabled")
        return {"status": "disabled"}
    if configuration_generation(active) != expected_generation:
        event("recorder_stale_job")
        return {"status": "stale_configuration"}
    active_channels = set(str(active.get("channel_uuids") or "").splitlines())
    if channel_uuid not in active_channels or set(config.channel_uuids) != active_channels:
        event("recorder_stale_job")
        return {"status": "stale_configuration"}
    try:
        schedule, timezone_name = _active_schedule(active, channel_uuid)
    except (ScheduleError, ValueError):
        error("schedule_install_failed")
        return {"status": "invalid_schedule"}
    if not schedule_is_active(schedule, timezone_name, datetime.now(timezone.utc)):
        event("recorder_schedule_closed")
        return {"status": "outside_schedule"}
    return {"status": "ready", "config": config, "active": active, "control": control}
