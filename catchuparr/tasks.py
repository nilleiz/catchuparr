"""Celery recorder and cleanup tasks loaded by the Dispatcharr plugin manager."""

from __future__ import annotations

import hashlib
import logging
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
from threading import Event as _WakeEvent

from celery import shared_task

logger = logging.getLogger(__name__)
RECORDER_CAPABILITY_HEADER = "X-Catchuparr-Recorder"
RECORDER_CAPABILITY_TTL_SECONDS = 30
RECORDER_CANDIDATE_RETRY_INITIAL_SECONDS = 2.0
RECORDER_CANDIDATE_RETRY_MAX_SECONDS = 30.0
RECORDER_CLEANUP_RETRY_SECONDS = 0.2
SHARING_WATCH_INTERVAL_SECONDS = 1.0
RECORDER_SUPERVISION_INTERVAL_SECONDS = 10.0


def _sharing_watch_signature(redis, channel_uuid):
    """Read native viewer/source changes without refreshing or mutating them."""
    prefix = f"live:channel:{channel_uuid}"
    try:
        first = redis.pipeline(transaction=False)
        first.get(f"{prefix}:owner")
        first.hmget(f"{prefix}:metadata", "state", "owner", "stream_id", "m3u_profile", "stream_profile")
        first.smembers(f"{prefix}:clients")
        first.exists(f"{prefix}:input:buffer:index")
        first.exists(f"{prefix}:input:buffer:chunk_timestamps")
        owner, metadata, clients, index_exists, timestamps_exist = first.execute()
        clients = sorted(clients, key=lambda value: str(value))
        second = redis.pipeline(transaction=False)
        for client in clients:
            if isinstance(client, bytes):
                client = client.decode("utf-8", errors="strict")
            second.hmget(f"{prefix}:clients:{client}", "connected_at", "worker_id")
            second.exists(f"{prefix}:client:{client}:stop")
        details = second.execute() if clients else []
        return (owner, tuple(metadata), tuple(clients), bool(index_exists),
                bool(timestamps_exist), tuple(tuple(x) if isinstance(x, list) else x for x in details))
    except Exception:
        return None


def _sharing_deadline_due(coordinator, now):
    """Wake at an admitted attach deadline without changing its configured delay."""
    try:
        from .sharing_planner import SharingAction, SharingPhase

        record = coordinator.state_record
        token = record.intent
        if token is not None:
            return token.action is SharingAction.ATTACH_BORROW and now >= token.not_before
        state = record.state
        if state.phase is not SharingPhase.WAIT_LIVE or state.candidate_since is None:
            return False
        setting = coordinator.active["connection_sharing"][coordinator.channel_uuid]
        stability = setting["live_stability_seconds"]
        minimum = setting["min_switch_interval_seconds"]
        if type(stability) is not int or type(minimum) is not int:
            return False
        deadline = state.candidate_since + (stability if state.last_switch_at is not None else 0)
        if state.last_switch_at is not None:
            deadline = max(deadline, state.last_switch_at + minimum)
        return now >= deadline
    except Exception:
        return False


def _finish_borrow_cleanup(cleanup, lease, stop_event, *, still_current, on_pending):
    """Hold this recorder lease until the exact borrowed HTTP cleanup finishes."""
    while True:
        if cleanup():
            return True
        try:
            on_pending()
        except Exception:
            pass  # Diagnostics cannot authorize or abort cleanup.
        if stop_event.is_set():
            return False
        try:
            current = still_current() and lease.renew()
        except Exception:
            current = False
        if not current:
            stop_event.set()
            return False
        if stop_event.wait(RECORDER_CLEANUP_RETRY_SECONDS):
            return False


def _wait_for_borrow_wakeup(stop_event, wakeup, timeout, *, clock=time.monotonic):
    """Wake an exhausted-source recorder on ATTACH without waiting its backoff."""
    deadline = clock() + timeout
    while not stop_event.is_set():
        remaining = deadline - clock()
        if remaining <= 0:
            return "timeout"
        if wakeup.wait(min(remaining, 0.2)):
            return "borrow"
    return "stop"


def _retry_failure_attach(coordinator, active, control_generation, state_lock, attempt_state):
    """Try one fresh, fenced attach immediately after exact failure cleanup."""
    with state_lock:
        if attempt_state.get("borrow") is not None:
            return False
        prepared = coordinator.poll(active=active, control_generation=control_generation)
        if prepared is None:
            return False
        attempt_state["borrow"] = prepared
        # The fresh ATTACH superseded the old RETURN token and receipt.
        attempt_state["return_prepared"] = None
        return True


def _request_viewer_switch(switch, active, prepared, make_recorder, attempt_state):
    """Offer one staged reader only for the enabled, current attempt."""
    if switch is None:
        return False
    offered = False
    if (active.get("synchronize_before_switch") is True
            and not attempt_state.get("return_syncing", False)):
        try:
            target = make_recorder(prepared)
            attempt_state["syncing"] = True
            offered = switch.offer_synchronized_input(target)
        except Exception:
            offered = False
        if not offered:
            attempt_state["syncing"] = False
    if not offered:
        switch.request()
    return offered


def _run_claimed_borrow_with_cleanup(prepared, staged_input, run, cleanup, on_pending):
    """Retain the exact cleanup obligation when a claimed handover raises."""
    try:
        return run(prepared, staged_input)
    except BaseException:
        try:
            if staged_input is not None:
                staged_input.close()
        finally:
            if not cleanup(prepared):
                on_pending()
        raise


def _finish_recorder_attempt(
    redis, attempt, lease, stop_event, *, sharing, still_current, on_pending,
):
    """Retain this source's position until its exact teardown is acknowledged.

    A native stop request and its HTTP close callback can finish separately.
    Ending the task on the first missing ACK loses the failed source position,
    so the next reconciliation can retry the same broken source forever.
    Keep admission revoked and retry only this attempt while ownership holds.
    """
    from .logging_utils import error
    from .recorder_proxy import stop_recorder_attempt

    revoked = False
    while True:
        try:
            if not revoked:
                revoked = attempt.revoke(redis)
            if revoked and stop_recorder_attempt(redis, attempt, lease):
                if not sharing or attempt.acknowledge_reaped(redis):
                    return True
        except Exception as exc:
            error("supervision_failed", exc, callsite="dedicated_worker")
        try:
            on_pending()
        except Exception:
            # Diagnostic publication cannot decide native resource ownership.
            pass
        if stop_event.is_set():
            return False
        try:
            current = still_current() and lease.renew()
        except Exception as exc:
            error("supervision_failed", exc, callsite="dedicated_worker")
            current = False
        if not current:
            stop_event.set()
            return False
        if stop_event.wait(RECORDER_CLEANUP_RETRY_SECONDS):
            return False


def _refresh_current_recorder_diagnostic(
    state_lock, attempt_state, publish_dedicated, publish_borrow, *, now=None,
):
    """Renew the current phase under its transition lock, never a stale snapshot."""
    current_time = time.monotonic() if now is None else float(now)
    with state_lock:
        diagnostic = attempt_state.get("diagnostic")
        if not isinstance(diagnostic, dict) or current_time - diagnostic.get("last_published", 0) < 10:
            return
        if diagnostic.get("kind") == "dedicated":
            publish_dedicated(
                diagnostic.get("attempt"), diagnostic.get("phase", "starting"),
                started_at=diagnostic.get("started_at"),
            )
        elif diagnostic.get("kind") == "borrowed":
            publish_borrow(
                diagnostic.get("prepared"), diagnostic.get("phase", "starting"),
                started_at=diagnostic.get("started_at"),
            )


def _remember_recorder_diagnostic(attempt_state, kind, binding, phase, started_at, *,
                                  published, now=None):
    """Remember confirmed media phase even when its advisory Redis write fails."""
    previous = attempt_state.get("diagnostic") or {}
    changed_phase = previous.get("phase") != phase
    attempt_state["diagnostic"] = {
        "kind": kind, "attempt" if kind == "dedicated" else "prepared": binding,
        "started_at": started_at,
        "phase": phase,
        "last_published": (
            time.monotonic() if now is None else float(now)
        ) if published else (0.0 if changed_phase else previous.get("last_published", 0.0)),
    }


def _complete_return_media(
    attempt_state_lock, attempt_state, sharing_coordinator, stop_event,
    attempt, prepared, token, candidate, media_at,
):
    """Complete a media-confirmed RETURN without accepting a stale attempt."""
    if (sharing_coordinator is None or prepared is None or token is None
            or attempt is None):
        return True
    with attempt_state_lock:
        if attempt_state.get("attempt") is not attempt:
            return False
        completed = attempt_state.get("completed_return_media")
        if (completed is not None and completed[0] is attempt
                and completed[1] is prepared and completed[2] is token
                and completed[3] is candidate):
            return True
        if (attempt_state.get("return_prepared") is not prepared
                or attempt_state.get("return_token") != token):
            return False
        first_media_at = attempt_state.get("return_media_at")
        if first_media_at is None:
            first_media_at = float(media_at)
            attempt_state["return_media_at"] = first_media_at
        # Keep local completion serialized across the SQL acknowledgement. A
        # callback that passed the pending-state check must not race a second
        # ACK after the supervisor commits and records its completion receipt.
        acknowledged = sharing_coordinator.acknowledge_return_media(
            prepared, token, attempt, candidate, media_at=first_media_at,
        )
        if acknowledged:
            if (attempt_state.get("attempt") is attempt
                    and attempt_state.get("return_prepared") is prepared
                    and attempt_state.get("return_token") == token):
                attempt_state["completed_return_media"] = (
                    attempt, prepared, token, candidate, first_media_at,
                )
                attempt_state["return_prepared"] = None
                attempt_state["return_token"] = None
                attempt_state["return_media_at"] = None
            return True
        if not sharing_coordinator.return_ack_retryable(
            prepared, token, attempt, candidate, media_at=first_media_at,
        ):
            stop_event.set()
        return False


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

    from .configuration import (
        load_applied_state,
        rehydrate_current_sharing_authority,
        reset_legacy_configuration,
    )
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
    if config is not None and active is not None and control is not None:
        if not rehydrate_current_sharing_authority(active, control):
            from .logging_utils import error

            error("sharing_recovery_failed", callsite="startup_recovery")
            return {"queued": 0, "cleanup_pending": 0}
    cleanup_pending = set()
    if config is not None and active is not None:
        generation = configuration_generation(active)
        desired = set(config.channel_uuids)
        valid = set(
            str(value)
            for value in Channel.objects.filter(uuid__in=desired).values_list("uuid", flat=True)
        )
        sharing = active.get("connection_sharing")
        if isinstance(sharing, dict):
            from .sharing_episode import BorrowEpisodeLedger
            from .sharing_runtime import retry_durable_open_cleanups

            episode_ledger = BorrowEpisodeLedger(
                config.archive_root / ".catchuparr-sharing.sqlite3",
            )
            for channel in sorted(valid):
                setting = sharing.get(channel)
                if not isinstance(setting, dict) or setting.get("enabled") is not True:
                    continue
                lease_key = f"catchuparr:recorder:{channel}"
                if redis.exists(lease_key):
                    continue
                try:
                    _, pending = retry_durable_open_cleanups(
                        episode_ledger, redis, now=time.time(), channel_uuid=channel,
                    )
                except Exception as exc:
                    pending = 1
                    from .logging_utils import error

                    error("sharing_recovery_failed", exc, callsite="reconcile_cleanup")
                if pending:
                    cleanup_pending.add(channel)
                    from .logging_utils import error

                    error("sharing_cleanup_pending", callsite="reconcile_cleanup")
        if control is not None and not control.paused:
            schedules = active.get("recording_schedule", {})
            timezone_name = schedules.get("timezone") if isinstance(schedules, dict) else None
            channel_schedules = schedules.get("channels") if isinstance(schedules, dict) else None
            if isinstance(timezone_name, str) and isinstance(channel_schedules, dict):
                now = datetime.now(timezone.utc)
                for channel in sorted(valid):
                    if channel in cleanup_pending:
                        continue
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
    return {"queued": queued, "cleanup_pending": len(cleanup_pending)}


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
    from .engine.recorder import (
        DEFAULT_MEDIA_IDLE_TIMEOUT_SECONDS,
        DEFAULT_RECORDER_STARTUP_TIMEOUT_SECONDS,
        FFmpegCopyRecorder,
        RecorderAttemptResult,
    )
    from .engine.store import ArchiveStore
    from .logging_utils import error, event
    from .recorder_proxy import (
        RecorderAttemptAdmissionUncertain,
        candidate_is_current,
        current_sharing_authority_context,
        issue_recorder_attempt,
        ranked_source_candidates,
        reconcile_unlaunched_sharing_operation,
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
    cleanup_prepared_borrow = None
    # The diagnostic callback and supervisor renewal share this lock across the
    # Redis CAS so an older phase cannot overwrite a newer first-media phase.
    attempt_state_lock = threading.RLock()
    borrow_wakeup = _WakeEvent()
    attempt_state = {
        "attempt": None, "candidate": None, "switch": None, "borrow": None,
        "active_borrow": None, "return_prepared": None, "return_token": None,
        "return_media_at": None, "completed_return_media": None,
        "diagnostic": None,
        "cleaning_up": False,
        "syncing": False, "prepared_input": None,
        "return_syncing": False, "return_sync_marker": None,
    }

    def publish_dedicated_diagnostic(
        attempt, phase: str, *, error_code: str | None = None,
        started_at: float | None = None,
    ) -> None:
        if attempt is None:
            return
        with attempt_state_lock:
            try:
                from .recorder_diagnostics import publish_recorder_diagnostic

                effective_started_at = started_at or time.time()
                published = publish_recorder_diagnostic(
                    redis, str(channel_uuid), str(lease._value),
                    worker_id=attempt.worker_id, phase=phase, source="dedicated",
                    stream_id=attempt.stream_id, account_id=attempt.account_id,
                    started_at=effective_started_at,
                    config_generation=attempt.config_generation,
                    control_generation=attempt.control_generation,
                    fence=int(lease.fence), error_code=error_code,
                )
                _remember_recorder_diagnostic(
                    attempt_state, "dedicated", attempt, phase, effective_started_at,
                    published=published,
                )
            except Exception:
                # Diagnostics are advisory and must never alter admission or cleanup.
                return

    def publish_borrow_diagnostic(
        prepared, phase: str, *, error_code: str | None = None,
        started_at: float | None = None,
    ) -> None:
        with attempt_state_lock:
            try:
                from .recorder_diagnostics import publish_borrowed_recorder_diagnostic

                capability = prepared.capability
                identity = capability.connection_identity
                effective_started_at = started_at or time.time()
                published = publish_borrowed_recorder_diagnostic(
                    redis, str(channel_uuid), str(lease._value),
                    episode_id=prepared.episode_id, client_id=prepared.client_id,
                    worker_id=identity[1],
                    owner_lock_value=identity[2], metadata_owner=identity[3],
                    stream_id=identity[4], account_id=identity[5],
                    m3u_profile_id=identity[6], input_profile_id=identity[7],
                    catalog_generation=capability.catalog_generation,
                    phase=phase, started_at=effective_started_at,
                    config_generation=capability.configuration_generation,
                    control_generation=capability.control_generation,
                    fence=capability.recorder_fence, error_code=error_code,
                )
                _remember_recorder_diagnostic(
                    attempt_state, "borrowed", prepared, phase, effective_started_at,
                    published=published,
                )
            except Exception:
                # Diagnostics are advisory and must never alter admission or cleanup.
                return

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

            # Reconcile exact private resources before observing viewers or
            # consuming another ATTACH claim. Current live episodes are never
            # discarded, and unresolved cleanup blocks provider fallback too.
            try:
                from .sharing_episode import BorrowEpisodeLedger
                from .sharing_runtime import retry_durable_open_cleanups

                episode_ledger = BorrowEpisodeLedger(
                    config.archive_root / ".catchuparr-sharing.sqlite3",
                )
                _, cleanup_pending = retry_durable_open_cleanups(
                    episode_ledger, redis, now=time.time(), channel_uuid=str(channel_uuid),
                )
            except Exception as exc:
                cleanup_pending = 1
                error("sharing_recovery_failed", exc, callsite="startup_recovery")
            if cleanup_pending:
                error("sharing_cleanup_pending", callsite="startup_recovery")
                return {"status": "sharing_cleanup_pending"}

            from .sharing_recovery import recover_superseded_borrow

            def recovery_authority():
                admission = _recorder_admission_state(
                    channel_uuid, expected_generation, expected_control_generation,
                )
                if admission["status"] != "ready":
                    return None
                return current_sharing_authority_context(
                    redis, str(channel_uuid), config_generation=expected_generation,
                    control_generation=expected_control_generation,
                )

            if not recover_superseded_borrow(
                episode_ledger, redis, str(channel_uuid),
                configuration_generation=expected_generation,
                control_generation=expected_control_generation,
                recorder_fence=fence, lease_value=str(lease._value),
                current_authority=recovery_authority,
            ):
                error("sharing_cleanup_pending", callsite="startup_recovery")
                return {"status": "sharing_cleanup_pending"}

            # Recovery may have crossed a lease or configuration boundary.
            # Keep the existing consumed planner claim, but admit fresh work
            # only after rechecking the exact current task fence.
            recovered_admission = _recorder_admission_state(
                channel_uuid, expected_generation, expected_control_generation
            )
            if (recovered_admission["status"] != "ready" or not lease.renew()):
                error("sharing_recovery_stale", callsite="startup_admission")
                return {"status": "sharing_recovery_stale"}
            active = recovered_admission["active"]

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
                    error("sharing_return_recovery_pending", callsite="return_recovery")
                    return {"status": "sharing_return_recovery_pending"}
                attempt_state["return_prepared"] = return_receipt
            # A channel without an allowed dedicated source still needs a live
            # supervisor so an existing viewer connection can be borrowed.
            if candidates is None:
                candidates = []

        def complete_return_media(attempt, prepared, token, candidate, media_at):
            return _complete_return_media(
                attempt_state_lock, attempt_state, sharing_coordinator, stop_event,
                attempt, prepared, token, candidate, media_at,
            )

        def make_borrow_recorder(prepared):
            from .adapters.sharing_borrow_route import encode_borrow_capability

            return FFmpegCopyRecorder(
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

        def supervise():
            signature = None
            next_full_check = time.monotonic() + RECORDER_SUPERVISION_INTERVAL_SECONDS
            interval = (SHARING_WATCH_INTERVAL_SECONDS if sharing_coordinator is not None
                        else RECORDER_SUPERVISION_INTERVAL_SECONDS)
            while not stop_event.wait(interval):
                if sharing_coordinator is not None:
                    current_signature = _sharing_watch_signature(redis, channel_uuid)
                    changed = current_signature is not None and current_signature != signature
                    signature = current_signature
                    if (not changed and time.monotonic() < next_full_check
                            and not _sharing_deadline_due(sharing_coordinator, time.time())):
                        continue
                next_full_check = time.monotonic() + RECORDER_SUPERVISION_INTERVAL_SECONDS
                try:
                    current_admission = _supervisor_recorder_admission_state(
                        channel_uuid, expected_generation, expected_control_generation
                    )
                    if (
                        current_admission["status"] != "ready" or not lease.renew()
                    ):
                        stop_event.set()
                        return
                    # Finish a media-confirmed RETURN before observing new
                    # viewers. Otherwise poll() sees the pending RETURN and
                    # suppresses the ATTACH that this ACK has just enabled.
                    with attempt_state_lock:
                        pending_return = (
                            attempt_state.get("attempt"),
                            attempt_state.get("return_prepared"),
                            attempt_state.get("return_token"),
                            attempt_state.get("candidate"),
                            attempt_state.get("return_media_at"),
                        )
                    if pending_return[4] is not None:
                        complete_return_media(*pending_return[:4], pending_return[4])
                    _refresh_current_recorder_diagnostic(
                        attempt_state_lock, attempt_state,
                        publish_dedicated_diagnostic, publish_borrow_diagnostic,
                    )
                    if stats_capability_key is not None:
                        if not redis.expire(
                            stats_capability_key, RECORDER_CAPABILITY_TTL_SECONDS
                        ):
                            stop_event.set()
                            return
                        from .stats import refresh_recorder_markers

                        refresh_recorder_markers(redis, stats_capability_digest)
                    current_active = current_admission["active"]
                    with attempt_state_lock:
                        cleaning_up = attempt_state["cleaning_up"]
                    # Revocation is intentional during teardown. Keep the
                    # lease/control supervision alive without renewing the
                    # revoked capability or preparing another native input.
                    if cleaning_up:
                        continue
                    with attempt_state_lock:
                        syncing = attempt_state["syncing"]
                    if sharing_coordinator is not None and not syncing:
                        prepared = sharing_coordinator.poll(
                            active=current_active,
                            control_generation=expected_control_generation,
                        )
                        if prepared is not None:
                            with attempt_state_lock:
                                if attempt_state["borrow"] is None:
                                    attempt_state["borrow"] = prepared
                                    borrow_wakeup.set()
                                else:
                                    prepared = None
                            if prepared is not None:
                                with attempt_state_lock:
                                    current_switch = attempt_state["switch"]
                                    _request_viewer_switch(
                                        current_switch, current_active, prepared,
                                        make_borrow_recorder, attempt_state,
                                    )
                    with attempt_state_lock:
                        if attempt_state["cleaning_up"]:
                            continue
                        current_attempt = attempt_state["attempt"]
                        current_candidate = attempt_state["candidate"]
                        current_switch = attempt_state["switch"]
                        # Serialize this renewal with the cleanup flag. A
                        # supervisor that took an earlier snapshot must not
                        # stop the task because teardown revoked its bearer.
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
                except Exception as exc:
                    error("supervision_failed", exc, callsite="recorder_supervisor")
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
            except Exception as exc:
                error("sharing_observation_failed", exc, callsite="native_observation")
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
                        channel_uuid=str(channel_uuid),
                    )
                    return borrow_cleanup_acknowledged(redis, episode_ledger, prepared)
                except Exception:
                    return False

            def run_prepared_borrow() -> bool:
                with attempt_state_lock:
                    prepared = attempt_state["borrow"]
                    attempt_state["borrow"] = None
                    staged_input = attempt_state["prepared_input"]
                    attempt_state["prepared_input"] = None
                    attempt_state["syncing"] = False
                    borrow_wakeup.clear()
                if prepared is None:
                    if staged_input is not None:
                        staged_input.close()
                    return False
                with attempt_state_lock:
                    attempt_state["return_sync_marker"] = None
                return _run_claimed_borrow_with_cleanup(
                    prepared, staged_input, run_claimed_borrow, cleanup_prepared_borrow,
                    lambda: error("borrow_cleanup_pending", callsite="borrowed_cleanup"),
                )

            def run_claimed_borrow(prepared, staged_input) -> bool:
                current_state = _recorder_admission_state(
                    channel_uuid, expected_generation, expected_control_generation
                )
                if stop_event.is_set() or current_state["status"] != "ready":
                    # The opening claim has been consumed. Do not fall through
                    # to an allowlisted recorder while its native registration
                    # may still be present.
                    if staged_input is not None:
                        staged_input.close()
                    if not cleanup_prepared_borrow(prepared):
                        error("borrow_cleanup_pending", callsite="borrowed_cleanup")
                    stop_event.set()
                    return True
                recorder = staged_input.recorder if staged_input is not None else make_borrow_recorder(prepared)
                recorder._mark_next_discontinuity = True
                current_state = _recorder_admission_state(
                    channel_uuid, expected_generation, expected_control_generation
                )
                if stop_event.is_set() or current_state["status"] != "ready":
                    if staged_input is not None:
                        staged_input.close()
                    if not cleanup_prepared_borrow(prepared):
                        error("borrow_cleanup_pending", callsite="borrowed_cleanup")
                    stop_event.set()
                    return True
                borrow_outcome = "failure"
                borrow_started_at = time.time()
                publish_borrow_diagnostic(
                    prepared, "starting", started_at=borrow_started_at,
                )

                def record_borrow_media(_media_at):
                    publish_borrow_diagnostic(
                        prepared, "recording", started_at=borrow_started_at,
                    )
                    return True

                try:
                    borrow_result = recorder.run_candidate(
                        stop_event, startup_timeout=DEFAULT_RECORDER_STARTUP_TIMEOUT_SECONDS,
                        media_idle_timeout=DEFAULT_MEDIA_IDLE_TIMEOUT_SECONDS,
                        on_first_media=record_borrow_media,
                        **({"staged_input": staged_input} if staged_input is not None else {}),
                    )
                except Exception as exc:
                    if staged_input is not None:
                        staged_input.close()
                    error("borrowed_recorder_worker_stopped", exc, callsite="borrowed_worker")
                    publish_borrow_diagnostic(
                        prepared, "error", error_code="borrowed_recorder_worker_stopped",
                        started_at=borrow_started_at,
                    )
                else:
                    if borrow_result.useful_segments == 0 and not stop_event.is_set():
                        publish_borrow_diagnostic(
                            prepared, "no_media_progress", started_at=borrow_started_at,
                        )
                    if stop_event.is_set():
                        borrow_outcome = "stop"
                    elif (sharing_coordinator is not None
                            and sharing_coordinator.viewer_return_deadline_reached(now=time.time())):
                        borrow_outcome = "return"
                    elif borrow_result.status == "stopped":
                        borrow_outcome = "stop"
                return_sync_marker = (
                    getattr(recorder, "_current_attempt_segment_id", None)
                    if (borrow_outcome == "return"
                        and active.get("synchronize_before_switch") is True)
                    else None
                )
                # run_candidate has reaped FFmpeg, so exact reader cleanup may
                # now be retried. Never switch inputs while its native client
                # or episode projection can still be live.
                def borrow_cleanup_current():
                    return _recorder_admission_state(
                        channel_uuid, expected_generation, expected_control_generation,
                    )["status"] == "ready"

                def borrow_cleanup_pending():
                    publish_borrow_diagnostic(
                        prepared, "cleanup_pending", started_at=borrow_started_at,
                    )

                with attempt_state_lock:
                    attempt_state["cleaning_up"] = True
                try:
                    cleanup_acknowledged = _finish_borrow_cleanup(
                        lambda: cleanup_prepared_borrow(prepared), lease, stop_event,
                        still_current=borrow_cleanup_current, on_pending=borrow_cleanup_pending,
                    )
                    if cleanup_acknowledged and sharing_coordinator is not None:
                        cleanup_acknowledged = sharing_coordinator.record_return_after_cleanup(
                            prepared, now=time.time(), outcome=borrow_outcome,
                        )
                        if not cleanup_acknowledged:
                            error("sharing_return_recovery_pending", callsite="return_completion")
                        if cleanup_acknowledged:
                            return_token = sharing_coordinator.pending_return_intent()
                            if (return_token is not None
                                    and return_token.intent_epoch > prepared.capability.intent_epoch):
                                with attempt_state_lock:
                                    attempt_state["return_prepared"] = prepared
                                    attempt_state["return_sync_marker"] = return_sync_marker
                            if borrow_outcome == "failure" and not stop_event.is_set():
                                # Re-observe immediately after the exact cleanup;
                                # normal viewer departures keep their settings.
                                try:
                                    admission = _recorder_admission_state(
                                        channel_uuid, expected_generation, expected_control_generation,
                                    )
                                    if admission["status"] == "ready":
                                        _retry_failure_attach(
                                            sharing_coordinator, admission["active"],
                                            expected_control_generation, attempt_state_lock, attempt_state,
                                        )
                                except Exception as exc:
                                    error("sharing_observation_failed", exc, callsite="native_observation")
                    if not cleanup_acknowledged:
                        error("borrow_cleanup_pending", callsite="borrowed_cleanup")
                        publish_borrow_diagnostic(
                            prepared, "cleanup_pending", started_at=borrow_started_at,
                        )
                        stop_event.set()
                    else:
                        with attempt_state_lock:
                            current_diagnostic = attempt_state.get("diagnostic")
                            if (isinstance(current_diagnostic, dict)
                                    and current_diagnostic.get("kind") == "borrowed"
                                    and current_diagnostic.get("prepared") is prepared):
                                attempt_state["diagnostic"] = None
                finally:
                    with attempt_state_lock:
                        attempt_state["cleaning_up"] = False
                return True

            candidate_index = 0
            candidate_retry_seconds = RECORDER_CANDIDATE_RETRY_INITIAL_SECONDS
            while True:
                while candidate_index < len(candidates):
                    if run_prepared_borrow():
                        candidate_retry_seconds = RECORDER_CANDIDATE_RETRY_INITIAL_SECONDS
                        continue
                    return_prepared = None
                    return_token = None
                    with attempt_state_lock:
                        return_prepared = attempt_state["return_prepared"]
                        return_sync_marker = attempt_state["return_sync_marker"]
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
                    current_state = _recorder_admission_state(
                        channel_uuid, expected_generation, expected_control_generation,
                    )
                    if current_state["status"] != "ready":
                        return {"status": current_state["status"]}
                    if not lease.renew():
                        return {"status": "lease_lost"}
                    sharing_authority = None
                    attempt_switch = None
                    if sharing_enabled:
                        sharing_authority = current_sharing_authority_context(
                            redis, channel_uuid, config_generation=generation,
                            control_generation=expected_control_generation,
                        )
                        if sharing_authority is None:
                            error("sharing_binding_lost")
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
                            error("sharing_attempt_cleanup_pending", callsite="attempt_reconciliation")
                            stop_event.set()
                            return {"status": "sharing_cleanup_pending"}
                        error("sharing_attempt_reconciled", callsite="attempt_reconciliation")
                        return {"status": "sharing_admission_uncertain"}
                    with attempt_state_lock:
                        attempt_state["attempt"] = attempt
                        attempt_state["candidate"] = candidate
                        attempt_state["switch"] = attempt_switch
                        attempt_state["completed_return_media"] = None
                        attempt_state["return_prepared"] = return_prepared
                        attempt_state["return_token"] = return_token
                        attempt_state["return_media_at"] = None
                    dedicated_started_at = time.time()
                    publish_dedicated_diagnostic(
                        attempt, "starting", started_at=dedicated_started_at,
                    )
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
                            def acknowledge_return_media(media_at):
                                publish_dedicated_diagnostic(
                                    attempt, "recording", started_at=dedicated_started_at,
                                )
                                if (return_token is None or return_prepared is None
                                        or sharing_coordinator is None):
                                    return True
                                return complete_return_media(
                                    attempt, return_prepared, return_token, candidate, media_at,
                                )

                            first_media_callback = acknowledge_return_media
                            try:
                                staged_return = None
                                tried_return_sync = False
                                if (return_token is not None and return_sync_marker is not None
                                        and attempt_switch is not None
                                        and current_state["active"].get("synchronize_before_switch") is True):
                                    tried_return_sync = True
                                    with attempt_state_lock:
                                        attempt_state["return_syncing"] = True
                                    try:
                                        staged_return = recorder.prepare_synchronized_return(
                                            return_sync_marker, stop_event, attempt_switch,
                                        )
                                    finally:
                                        with attempt_state_lock:
                                            attempt_state["return_syncing"] = False
                                if (tried_return_sync and staged_return is None
                                        and (stop_event.is_set() or attempt_switch.is_set())):
                                    status = "stopped" if stop_event.is_set() else "switch_requested"
                                    result = RecorderAttemptResult(
                                        status, 0, None,
                                        attempt_switch_id=(
                                            attempt_switch.attempt_id if status == "switch_requested" else None
                                        ),
                                    )
                                else:
                                    result = recorder.run_candidate(
                                        stop_event,
                                        startup_timeout=DEFAULT_RECORDER_STARTUP_TIMEOUT_SECONDS,
                                        media_idle_timeout=DEFAULT_MEDIA_IDLE_TIMEOUT_SECONDS,
                                        **({"attempt_switch": attempt_switch}
                                           if attempt_switch is not None else {}),
                                        **({"on_first_media": first_media_callback}
                                           if first_media_callback is not None else {}),
                                        **({"staged_input": staged_return}
                                           if staged_return is not None else {}),
                                    )
                                with attempt_state_lock:
                                    attempt_state["prepared_input"] = getattr(result, "staged_input", None)
                                    if result.useful_segments:
                                        attempt_state["return_sync_marker"] = None
                            except Exception as exc:
                                error("recorder_worker_stopped", exc, callsite="dedicated_worker")
                                publish_dedicated_diagnostic(
                                    attempt, "error", error_code="recorder_worker_stopped",
                                    started_at=dedicated_started_at,
                                )
                            else:
                                if result.status in {"no_media", "media_stalled", "exited"}:
                                    error("recorder_worker_stopped", callsite="dedicated_worker")
                                    publish_dedicated_diagnostic(
                                        attempt, "error", error_code="recorder_worker_stopped",
                                        started_at=dedicated_started_at,
                                    )
                                elif result.useful_segments == 0 and result.status != "stopped":
                                    publish_dedicated_diagnostic(
                                        attempt, "no_media_progress",
                                        started_at=dedicated_started_at,
                                    )
                            finally:
                                if staged_return is not None:
                                    try:
                                        staged_return.close()
                                    except Exception as exc:
                                        error("recorder_cleanup_pending", exc, callsite="dedicated_worker")
                    finally:
                        with attempt_state_lock:
                            attempt_state["cleaning_up"] = True

                        def cleanup_still_current():
                            return _recorder_admission_state(
                                channel_uuid, expected_generation, expected_control_generation,
                            )["status"] == "ready"

                        def cleanup_pending():
                            error("recorder_cleanup_pending", callsite="attempt_reconciliation")
                            with attempt_state_lock:
                                staged = attempt_state["prepared_input"]
                            if staged is not None:
                                staged.within_limits()
                            publish_dedicated_diagnostic(
                                attempt, "cleanup_pending", started_at=dedicated_started_at,
                            )

                        try:
                            cleanup_ok = _finish_recorder_attempt(
                                redis, attempt, lease, stop_event,
                                sharing=sharing_authority is not None,
                                still_current=cleanup_still_current, on_pending=cleanup_pending,
                            )
                            if not cleanup_ok:
                                stop_event.set()
                        finally:
                            with attempt_state_lock:
                                if attempt_state["attempt"] is attempt:
                                    attempt_state["attempt"] = None
                                    attempt_state["candidate"] = None
                                    attempt_state["switch"] = None
                                    attempt_state["completed_return_media"] = None
                                    attempt_state["return_token"] = None
                                    attempt_state["return_media_at"] = None
                                    attempt_state["cleaning_up"] = False
                                current_diagnostic = attempt_state.get("diagnostic")
                                if (isinstance(current_diagnostic, dict)
                                        and current_diagnostic.get("kind") == "dedicated"
                                        and current_diagnostic.get("attempt") is attempt):
                                    attempt_state["diagnostic"] = None
                    if admission_status is not None:
                        return {"status": admission_status}
                    if result is not None and result.useful_segments > 0:
                        candidate_retry_seconds = RECORDER_CANDIDATE_RETRY_INITIAL_SECONDS
                    if result is not None and result.status == "stopped":
                        run_prepared_borrow()
                        break
                    if run_prepared_borrow():
                        # The prior attempt was reaped and its sharing operation
                        # acknowledged before the borrowed reader starts. Return to
                        # an allowlisted source after that reader closes at grace.
                        candidate_index = 0
                        candidate_retry_seconds = RECORDER_CANDIDATE_RETRY_INITIAL_SECONDS
                        continue
                    candidate_index += 1
                if sharing_coordinator is None or stop_event.is_set():
                    break
                if run_prepared_borrow():
                    candidate_index = 0
                    candidate_retry_seconds = RECORDER_CANDIDATE_RETRY_INITIAL_SECONDS
                    continue
                # A sharing task deliberately keeps its lease while it waits
                # for viewers. Do not leave an exhausted dedicated candidate
                # list at len(candidates) forever: a failed RETURN then blocks
                # new ATTACH decisions indefinitely. Retry the ranked sources
                # as a fresh cycle with bounded backoff, while rechecking task
                # authority and refreshing the source list before any new work.
                wake = _wait_for_borrow_wakeup(stop_event, borrow_wakeup, candidate_retry_seconds)
                if wake == "stop":
                    break
                if wake == "borrow":
                    continue
                retry_admission = _recorder_admission_state(
                    channel_uuid, expected_generation, expected_control_generation
                )
                if retry_admission["status"] != "ready":
                    return {"status": retry_admission["status"]}
                if not lease.renew():
                    error("recorder_retry_lease_lost", callsite="candidate_retry")
                    return {"status": "lease_lost"}
                try:
                    refreshed_candidates = ranked_source_candidates(
                        channel_uuid, retry_admission["active"], all_sources=sharing_enabled,
                    )
                except Exception as exc:
                    error("recorder_retry_candidates_failed", exc, callsite="candidate_retry")
                    return {"status": "candidate_refresh_failed"}
                if refreshed_candidates is None:
                    error("recorder_retry_candidates_unavailable", callsite="candidate_retry")
                    return {"status": "candidate_refresh_unavailable"}
                candidates = refreshed_candidates
                sharing_coordinator.dedicated_source_available = bool(candidates)
                candidate_index = 0
                candidate_retry_seconds = min(
                    RECORDER_CANDIDATE_RETRY_MAX_SECONDS,
                    max(RECORDER_CANDIDATE_RETRY_INITIAL_SECONDS,
                        candidate_retry_seconds * 2),
                )
    finally:
        try:
            if stop_event is not None:
                stop_event.set()
            if monitor is not None and monitor.ident is not None:
                monitor.join(timeout=12)
        finally:
            with attempt_state_lock:
                pending_prepared = attempt_state["borrow"]
                pending_input = attempt_state["prepared_input"]
                attempt_state["borrow"] = None
                attempt_state["prepared_input"] = None
            if pending_input is not None:
                try:
                    pending_input.close()
                except Exception as exc:
                    error("borrow_cleanup_pending", exc, callsite="borrowed_cleanup")
            if pending_prepared is not None and cleanup_prepared_borrow is not None:
                if not cleanup_prepared_borrow(pending_prepared):
                    error("borrow_cleanup_pending", callsite="borrowed_cleanup")
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


def _supervisor_recorder_admission_state(
    channel_uuid, expected_generation, expected_control_generation,
):
    """Run periodic ORM-backed admission away from Dispatcharr's gevent loop."""
    try:
        from gevent import get_hub
    except ImportError:
        # Unit-only environments without Dispatcharr's gevent runtime retain
        # the normal synchronous behavior.
        return _recorder_admission_state(
            channel_uuid, expected_generation, expected_control_generation,
        )

    def check_admission():
        try:
            from django.db import close_old_connections
        except ImportError:
            close_old_connections = None
        if close_old_connections is not None:
            close_old_connections()
        try:
            return _recorder_admission_state(
                channel_uuid, expected_generation, expected_control_generation,
            )
        finally:
            if close_old_connections is not None:
                close_old_connections()

    return get_hub().threadpool.apply(check_admission)


def _recorder_admission_state(channel_uuid, expected_generation, expected_control_generation):
    from .configuration import load_applied_state
    from .logging_utils import apply_log_level, error, event
    from .recorder_control import RecorderControlError
    from .recorder_proxy import configuration_generation
    from .runtime import load_config
    from .schedule import ScheduleError, schedule_is_active

    try:
        active, control = load_applied_state()
    except RecorderControlError as exc:
        error("control_state_invalid", exc, callsite="control_state_read")
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
    except (ScheduleError, ValueError) as exc:
        error("schedule_install_failed", exc, callsite="schedule_validation")
        return {"status": "invalid_schedule"}
    if not schedule_is_active(schedule, timezone_name, datetime.now(timezone.utc)):
        event("recorder_schedule_closed")
        return {"status": "outside_schedule"}
    return {"status": "ready", "config": config, "active": active, "control": control}
