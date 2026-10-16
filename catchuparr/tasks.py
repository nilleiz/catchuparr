"""Celery recorder and cleanup tasks loaded by the Dispatcharr plugin manager."""

from __future__ import annotations

import hashlib
import logging
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone

from celery import shared_task

logger = logging.getLogger(__name__)
RECORDER_CAPABILITY_HEADER = "X-Catchuparr-Recorder"
RECORDER_CAPABILITY_TTL_SECONDS = 30


def _issue_recorder_capability(redis, channel_uuid: str, fence: int, owner: str):
    token = secrets.token_urlsafe(32)
    digest = hashlib.sha256(token.encode("ascii")).hexdigest()
    key = f"catchuparr:recorder:stats-cap:{digest}"
    value = f"{channel_uuid}|{int(fence)}:{owner}"
    if not redis.set(key, value, ex=RECORDER_CAPABILITY_TTL_SECONDS, nx=True):
        raise RuntimeError("Unable to register recorder identity capability")
    return key, token, digest


@shared_task(name="catchuparr.reconcile")
def reconcile_recorders():
    from apps.channels.models import Channel
    from core.utils import RedisClient

    from .configuration import load_applied_state, reset_legacy_configuration
    from .engine.store import ArchiveStore
    from .logging_utils import apply_log_level
    from .recorder_control import RecorderControlError
    from .recorder_proxy import configuration_generation
    from .runtime import load_config, require_supported_version
    from .schedule import ScheduleError, schedule_from_snapshot, schedule_is_active

    require_supported_version()
    reset_legacy_configuration()
    queued = 0
    redis = RedisClient.get_client()
    try:
        active, control = load_applied_state()
    except RecorderControlError:
        from .configuration import load_active_configuration

        active, control = load_active_configuration(), None
    apply_log_level(active.get("log_level", "INFO") if active else "INFO")
    config = load_config(active_snapshot=active) if active is not None else None
    if config is not None and active is not None:
        generation = configuration_generation(active)
        desired = set(config.channel_uuids)
        valid = set(
            str(value)
            for value in Channel.objects.filter(uuid__in=desired).values_list("uuid", flat=True)
        )
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

    from .configuration import load_active_configuration, reset_legacy_configuration
    from .engine.store import ArchiveStore
    from .logging_utils import apply_log_level
    from .runtime import load_config, require_supported_version

    require_supported_version()
    reset_legacy_configuration()
    active = load_active_configuration()
    apply_log_level(active.get("log_level", "INFO") if active else "INFO")
    config = load_config(active_snapshot=active) if active is not None else None
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
        RecorderAttemptAdmissionUncertain,
        candidate_is_current,
        current_sharing_authority_context,
        issue_recorder_attempt,
        ranked_source_candidates,
        reconcile_unlaunched_sharing_operation,
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
    stats_capability_key = None
    stats_capability_digest = None
    stop_event = None
    monitor = None
    attempt_state_lock = threading.Lock()
    attempt_state = {
        "attempt": None, "candidate": None, "switch": None, "borrow": None,
        "return_prepared": None,
    }
    try:
        stop_event = threading.Event()
        sharing = active.get("connection_sharing")
        sharing_setting = sharing.get(channel_uuid) if isinstance(sharing, dict) else None
        sharing_enabled = isinstance(sharing_setting, dict) and sharing_setting.get("enabled") is True
        candidates = ranked_source_candidates(
            channel_uuid, active, all_sources=sharing_enabled,
        )
        # Sharing-enabled channels use the all-assigned-source ordering even
        # when the user did not add a source filter. The legacy channel route
        # remains unchanged while sharing is disabled.
        if candidates is not None and not candidates and not sharing_enabled:
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
        stats_capability_key, stats_capability, stats_capability_digest = (
            _issue_recorder_capability(redis, channel_uuid, fence, lease.owner)
        )
        shared_input_headers = (
            {RECORDER_CAPABILITY_HEADER: stats_capability}
            if candidates is None else None
        )
        sharing_coordinator = None
        if sharing_enabled:
            from .sharing_runtime import RecorderBorrowCoordinator

            sharing_coordinator = RecorderBorrowCoordinator(
                redis,
                config.archive_root,
                str(channel_uuid),
                active=active,
                control_generation=expected_control_generation,
                recorder_fence=fence,
                lease_value=str(lease._value),
                dedicated_source_available=bool(candidates),
            )
            pending_return_reader = getattr(sharing_coordinator, "pending_return_intent", None)
            pending_return = pending_return_reader() if callable(pending_return_reader) else None
            if pending_return is not None:
                return_receipt = sharing_coordinator.recovered_return_receipt()
                if return_receipt is None:
                    error("sharing_return_recovery_pending")
                    return {"status": "sharing_return_recovery_pending"}
                attempt_state["return_prepared"] = return_receipt
            # A channel without an allowed dedicated source still needs a live
            # supervisor so an existing viewer connection can be borrowed.
            if candidates is None:
                candidates = []

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
                    if stats_capability_key is not None:
                        if not redis.expire(
                            stats_capability_key, RECORDER_CAPABILITY_TTL_SECONDS
                        ):
                            stop_event.set()
                            return
                        from .stats import refresh_recorder_markers

                        refresh_recorder_markers(redis, stats_capability_digest)
                    current_active = current_admission["active"]
                    if sharing_coordinator is not None:
                        prepared = sharing_coordinator.poll(
                            active=current_active,
                            control_generation=expected_control_generation,
                        )
                        if prepared is not None:
                            with attempt_state_lock:
                                if attempt_state["borrow"] is None:
                                    attempt_state["borrow"] = prepared
                                else:
                                    prepared = None
                            if prepared is not None:
                                with attempt_state_lock:
                                    current_switch = attempt_state["switch"]
                                if current_switch is not None:
                                    current_switch.request()
                    with attempt_state_lock:
                        current_attempt = attempt_state["attempt"]
                        current_candidate = attempt_state["candidate"]
                        current_switch = attempt_state["switch"]
                    if current_attempt is not None and current_switch is not None:
                        if current_attempt.operation_needs_switch(redis):
                            current_switch.request()
                        elif not current_attempt.renew(redis):
                            current_switch.request()
                    elif current_attempt is not None and not current_attempt.renew(redis):
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

        # Observe native viewers synchronously before the first dedicated
        # provider attempt. The periodic supervisor starts only after this
        # pass, so a viewer on an excluded source is not missed while the
        # task opens or fails its first allowed source.
        if sharing_coordinator is not None:
            initial_admission = _recorder_admission_state(
                channel_uuid, expected_generation, expected_control_generation
            )
            if initial_admission["status"] != "ready":
                return {"status": initial_admission["status"]}
            try:
                initial_borrow = sharing_coordinator.poll(
                    active=initial_admission["active"],
                    control_generation=expected_control_generation,
                )
            except Exception:
                error("sharing_observation_failed")
                return {"status": "sharing_unavailable"}
            if initial_borrow is not None:
                attempt_state["borrow"] = initial_borrow

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
                input_headers=shared_input_headers,
                on_error=lambda _message: error("recorder_worker_stopped"),
            )
            recorder.run_forever(stop_event)
        else:
            def cleanup_prepared_borrow(prepared) -> bool:
                try:
                    from .sharing_episode import BorrowEpisodeLedger
                    from .sharing_runtime import (
                        borrow_cleanup_acknowledged,
                        retry_durable_open_cleanups,
                        retry_pending_open_cleanups,
                    )

                    episode_ledger = BorrowEpisodeLedger(
                        config.archive_root / ".catchuparr-sharing.sqlite3",
                    )
                    # Restrict reconciliation to this episode. The SQLite
                    # file is shared by channels, and an unrelated opening
                    # may still have a live signed capability.
                    retry_durable_open_cleanups(
                        episode_ledger, redis, now=time.time(),
                        stale_after=0, episode_id=prepared.episode_id,
                    )
                    retry_pending_open_cleanups()
                    return borrow_cleanup_acknowledged(redis, episode_ledger, prepared)
                except Exception:
                    return False

            def run_prepared_borrow() -> bool:
                with attempt_state_lock:
                    prepared = attempt_state["borrow"]
                    attempt_state["borrow"] = None
                if prepared is None:
                    return False
                current_state = _recorder_admission_state(
                    channel_uuid, expected_generation, expected_control_generation
                )
                if stop_event.is_set() or current_state["status"] != "ready":
                    # The opening claim has been consumed. Do not fall through
                    # to an allowlisted recorder while its native registration
                    # may still be present.
                    if not cleanup_prepared_borrow(prepared):
                        error("borrow_cleanup_pending")
                    stop_event.set()
                    return True
                from .adapters.sharing_borrow_route import encode_borrow_capability

                recorder = FFmpegCopyRecorder(
                    store,
                    channel_uuid,
                    f"{proxy_base_url}/catchuparr/sharing/{channel_uuid}",
                    config.archive_root / "work",
                    fencing_token=fence,
                    input_headers={
                        "X-Catchuparr-Borrow": encode_borrow_capability(prepared.capability),
                        "X-Catchuparr-Stats-Recorder": stats_capability,
                    },
                    require_media_progress=True,
                )
                recorder._mark_next_discontinuity = True
                current_state = _recorder_admission_state(
                    channel_uuid, expected_generation, expected_control_generation
                )
                if stop_event.is_set() or current_state["status"] != "ready":
                    if not cleanup_prepared_borrow(prepared):
                        error("borrow_cleanup_pending")
                    stop_event.set()
                    return True
                borrow_outcome = "failure"
                try:
                    borrow_result = recorder.run_candidate(
                        stop_event, startup_timeout=60, media_idle_timeout=120,
                    )
                except Exception:
                    error("borrowed_recorder_worker_stopped")
                else:
                    if stop_event.is_set():
                        borrow_outcome = "stop"
                    elif (sharing_coordinator is not None
                            and sharing_coordinator.viewer_return_deadline_reached(now=time.time())):
                        borrow_outcome = "return"
                    elif borrow_result.status == "stopped":
                        borrow_outcome = "stop"
                # run_candidate has reaped FFmpeg, so exact reader cleanup may
                # now be retried. Never switch inputs while its native client
                # or episode projection can still be live.
                cleanup_acknowledged = cleanup_prepared_borrow(prepared)
                if cleanup_acknowledged and sharing_coordinator is not None:
                    cleanup_acknowledged = sharing_coordinator.record_return_after_cleanup(
                        prepared, now=time.time(), outcome=borrow_outcome,
                    )
                    if cleanup_acknowledged:
                        return_token = sharing_coordinator.pending_return_intent()
                        if (return_token is not None
                                and return_token.intent_epoch > prepared.capability.intent_epoch):
                            with attempt_state_lock:
                                attempt_state["return_prepared"] = prepared
                if not cleanup_acknowledged:
                    error("borrow_cleanup_pending")
                    stop_event.set()
                return True

            candidate_index = 0
            while True:
                while candidate_index < len(candidates):
                    if run_prepared_borrow():
                        continue
                    return_prepared = None
                    return_token = None
                    with attempt_state_lock:
                        return_prepared = attempt_state["return_prepared"]
                    if return_prepared is not None and sharing_coordinator is not None:
                        return_token = sharing_coordinator.pending_return_intent()
                        return_origin_epoch = (
                            return_prepared.capability.intent_epoch
                            if hasattr(return_prepared, "capability")
                            else return_prepared.intent_epoch
                        )
                        if (return_token is None
                                or return_token.intent_epoch <= return_origin_epoch):
                            # The dedicated source is not authorized until the
                            # exact post-borrow RETURN intent exists.
                            if stop_event.wait(0.5):
                                break
                            continue
                        remaining = return_token.not_before - time.time()
                        if remaining > 0:
                            if stop_event.wait(min(remaining, 0.5)):
                                break
                            continue
                    index = candidate_index
                    candidate = candidates[index]
                    if stop_event.is_set():
                        break
                    sharing_authority = None
                    attempt_switch = None
                    if sharing_enabled:
                        sharing_authority = current_sharing_authority_context(
                            redis, channel_uuid, config_generation=generation,
                            control_generation=expected_control_generation,
                        )
                        if sharing_authority is None:
                            return {"status": "sharing_authority_unavailable"}
                        from .engine.recorder import RecorderAttemptSwitch
    
                        attempt_switch = RecorderAttemptSwitch()
                    try:
                        attempt = issue_recorder_attempt(
                            redis,
                            lease,
                            channel_uuid=channel_uuid,
                            candidate=candidate,
                            config_generation=generation,
                            control_generation=expected_control_generation,
                            internal_base_url=proxy_base_url,
                            sharing_authority=sharing_authority,
                        )
                    except RecorderAttemptAdmissionUncertain as exc:
                        uncertain = exc.attempt
                        digest = uncertain.capability_key.removeprefix(
                            "catchuparr:recorder-proxy:capability:"
                        )
                        if not reconcile_unlaunched_sharing_operation(
                            redis, channel_uuid=str(channel_uuid),
                            operation_id=str(uncertain.sharing_operation_id),
                            operation_revision=int(uncertain.sharing_operation_revision),
                            worker_id=uncertain.worker_id,
                            capability_digest=digest,
                            current_lease_value=str(lease._value),
                            current_lease_fence=int(lease.fence),
                            unreturned_capability=True,
                        ):
                            error("sharing_attempt_cleanup_pending")
                            stop_event.set()
                            return {"status": "sharing_cleanup_pending"}
                        error("sharing_attempt_reconciled")
                        return {"status": "sharing_admission_uncertain"}
                    with attempt_state_lock:
                        attempt_state["attempt"] = attempt
                        attempt_state["candidate"] = candidate
                        attempt_state["switch"] = attempt_switch
                        attempt_state["return_token"] = return_token
                    admission_status = None
                    result = None
                    try:
                        current_state = _recorder_admission_state(
                            channel_uuid, expected_generation, expected_control_generation
                        )
                        if current_state["status"] != "ready":
                            admission_status = current_state["status"]
                        else:
                            recorder = FFmpegCopyRecorder(
                                store,
                                channel_uuid,
                                attempt.input_url,
                                config.archive_root / "work",
                                fencing_token=fence,
                                input_headers={
                                    **attempt.input_headers,
                                    "X-Catchuparr-Stats-Recorder": stats_capability,
                                },
                                require_media_progress=True,
                            )
                            if index:
                                recorder._mark_next_discontinuity = True
                            first_media_callback = None
                            if (return_token is not None and return_prepared is not None
                                    and sharing_coordinator is not None):
                                def acknowledge_return_media(media_at):
                                    acknowledged = sharing_coordinator.acknowledge_return_media(
                                        return_prepared, return_token, attempt, candidate,
                                        media_at=media_at,
                                    )
                                    if acknowledged:
                                        with attempt_state_lock:
                                            if attempt_state.get("return_prepared") is return_prepared:
                                                attempt_state["return_prepared"] = None
                                    elif not sharing_coordinator.return_ack_retryable(
                                        return_prepared, return_token, attempt, candidate,
                                    ):
                                        # A failed fresh completion proof means the
                                        # old attempt is no longer authorized. Stop
                                        # instead of keeping media flowing under an
                                        # uncommitted RETURN intent. A transient SQL
                                        # commit failure is retried on later media.
                                        stop_event.set()
                                    return acknowledged
    
                                first_media_callback = acknowledge_return_media
                            try:
                                result = recorder.run_candidate(
                                    stop_event,
                                    startup_timeout=60,
                                    media_idle_timeout=120,
                                    **({"attempt_switch": attempt_switch}
                                       if attempt_switch is not None else {}),
                                    **({"on_first_media": first_media_callback}
                                       if first_media_callback is not None else {}),
                                )
                            except Exception:
                                error("recorder_worker_stopped")
                    finally:
                        revoked = attempt.revoke(redis)
                        cleanup_ok = False
                        try:
                            cleanup_ok = stop_recorder_attempt(redis, attempt, lease)
                            if cleanup_ok and sharing_authority is not None:
                                cleanup_ok = attempt.acknowledge_reaped(redis)
                        except Exception:
                            error("supervision_failed")
                        if not revoked or not cleanup_ok:
                            stop_event.set()
                        with attempt_state_lock:
                            if attempt_state["attempt"] is attempt:
                                attempt_state["attempt"] = None
                                attempt_state["candidate"] = None
                                attempt_state["switch"] = None
                                attempt_state["return_token"] = None
                    if admission_status is not None:
                        return {"status": admission_status}
                    if result is not None and result.status == "stopped":
                        break
                    if run_prepared_borrow():
                        # The prior attempt was reaped and its sharing operation
                        # acknowledged before the borrowed reader starts. Return to
                        # an allowlisted source after that reader closes at grace.
                        candidate_index = 0
                        continue
                    candidate_index += 1
                if sharing_coordinator is None or stop_event.is_set():
                    break
                if run_prepared_borrow():
                    candidate_index = 0
                    continue
                if stop_event.wait(0.5):
                    break
    finally:
        try:
            if stop_event is not None:
                stop_event.set()
            if monitor is not None and monitor.ident is not None:
                monitor.join(timeout=12)
        finally:
            if stats_capability_key is not None:
                try:
                    redis.delete(stats_capability_key)
                except Exception:
                    logger.exception("Unable to remove recorder identity capability")
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
    from .configuration import load_applied_state
    from .logging_utils import apply_log_level, error, event
    from .recorder_control import RecorderControlError
    from .recorder_proxy import configuration_generation
    from .runtime import load_config
    from .schedule import ScheduleError, schedule_is_active

    try:
        active, control = load_applied_state()
    except RecorderControlError:
        error("control_state_invalid")
        return {"status": "control_unavailable"}
    if control.paused:
        return {"status": "recording_paused"}
    if control.generation != expected_control_generation:
        event("recorder_stale_job")
        return {"status": "stale_control"}
    if active is None:
        event("recorder_disabled")
        return {"status": "disabled"}
    apply_log_level(active.get("log_level", "INFO"))
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
