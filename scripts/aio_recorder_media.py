"""Synthetic native-route media acceptance probe for disposable AIO containers."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

CRASH_CHILD_RESULT_PREFIX = "CATCHUPARR_SHARING_CRASH_CHILD_RESULT "


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _failed_opening_identity(channel_uuid, native_owner, stream, metadata):
    """Build the exact channel identity used by the recovery fixture."""
    return (
        str(channel_uuid), str(native_owner), str(native_owner),
        str(native_owner), str(stream.id), str(stream.m3u_account_id),
        metadata["m3u_profile"], metadata["stream_profile"],
    )


def _ack_failed_opening_cleanup(ledger, channel_uuid, episode_id, result, *, acknowledged):
    """Validate only the first ACK for the exact seeded failed opening."""
    if acknowledged:
        return True
    episode = ledger.read(str(channel_uuid))
    _require(
        result in {(0, 0), (1, 0)}
        and episode.episode_id == episode_id
        and episode.phase == "closed",
        "Task did not ACK cleanup for the exact failed channel opening",
    )
    return True


def _run_current_recordings_readonly(action, archive_db):
    """Run the action while an SQLite authorizer watches its archive reads."""
    import sqlite3
    from urllib.parse import unquote, urlsplit

    from catchuparr import recording_overview

    archive_path = Path(archive_db).resolve()
    original_connect = recording_overview.sqlite3.connect
    action_thread_id = threading.get_ident()
    inspected = []
    attempted_writes = []
    write_actions = {
        getattr(sqlite3, name) for name in (
            "SQLITE_INSERT", "SQLITE_UPDATE", "SQLITE_DELETE",
            "SQLITE_CREATE_INDEX", "SQLITE_CREATE_TABLE", "SQLITE_CREATE_TEMP_INDEX",
            "SQLITE_CREATE_TEMP_TABLE", "SQLITE_CREATE_TEMP_TRIGGER",
            "SQLITE_CREATE_TEMP_VIEW", "SQLITE_CREATE_TRIGGER", "SQLITE_CREATE_VIEW",
            "SQLITE_DROP_INDEX", "SQLITE_DROP_TABLE", "SQLITE_DROP_TEMP_INDEX",
            "SQLITE_DROP_TEMP_TABLE", "SQLITE_DROP_TEMP_TRIGGER", "SQLITE_DROP_TEMP_VIEW",
            "SQLITE_DROP_TRIGGER", "SQLITE_DROP_VIEW", "SQLITE_ALTER_TABLE",
            "SQLITE_REINDEX", "SQLITE_ANALYZE", "SQLITE_CREATE_VTABLE",
            "SQLITE_DROP_VTABLE", "SQLITE_ATTACH", "SQLITE_DETACH",
        ) if hasattr(sqlite3, name)
    }

    class GuardedConnection:
        def __init__(self, connection, observation):
            self._connection = connection
            self._observation = observation

        def execute(self, statement, *args, **kwargs):
            result = self._connection.execute(statement, *args, **kwargs)
            if statement.strip().upper() == "PRAGMA QUERY_ONLY=ON":
                self._observation["query_only"] = bool(
                    self._connection.execute("PRAGMA query_only").fetchone()[0]
                )
            return result

        def __enter__(self):
            self._connection.__enter__()
            return self

        def __exit__(self, *args):
            return self._connection.__exit__(*args)

        def __getattr__(self, name):
            return getattr(self._connection, name)

    def guarded_connect(database, *args, **kwargs):
        if threading.get_ident() != action_thread_id:
            return original_connect(database, *args, **kwargs)
        value = str(database)
        parsed_path = unquote(urlsplit(value).path) if value.startswith("file:") else value
        try:
            is_archive = Path(parsed_path).resolve() == archive_path
        except (OSError, ValueError):
            is_archive = False
        connection = original_connect(database, *args, **kwargs)
        if not is_archive:
            return connection
        uri_readonly = value.startswith("file:") and "mode=ro" in value.split("?", 1)[-1].split("&")
        observation = {"uri_readonly": uri_readonly, "query_only": False}

        def authorize(action_code, arg1, arg2, _database, _trigger):
            if action_code in write_actions:
                attempted_writes.append(str(arg1 or "unknown"))
                return sqlite3.SQLITE_DENY
            if action_code == getattr(sqlite3, "SQLITE_PRAGMA", None) and arg2 is not None:
                allowed_query_only = (
                    str(arg1 or "").lower() == "query_only"
                    and str(arg2).upper() == "ON"
                )
                if not allowed_query_only:
                    attempted_writes.append("pragma")
                    return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        connection.set_authorizer(authorize)
        inspected.append(observation)
        return GuardedConnection(connection, observation)

    recording_overview.sqlite3.connect = guarded_connect
    try:
        result = action()
    finally:
        recording_overview.sqlite3.connect = original_connect
    _require(bool(inspected), "Current recordings did not inspect the archive index")
    _require(all(item["uri_readonly"] and item["query_only"] for item in inspected),
             "Current recordings archive connection was not read-only/query-only")
    _require(not attempted_writes,
             "Current recordings attempted to change the archive index")
    return result


def _overview_source_metadata_matches(source, expected_mode):
    if not isinstance(source, dict) or source.get("mode") != expected_mode:
        return False
    if not all(source.get(field) for field in (
        "stream_name", "provider_name", "stream_id", "provider_id",
        "m3u_profile_id", "m3u_profile_name",
    )):
        return False
    profile_fields = ("input_profile_id", "input_profile_name")
    if expected_mode == "borrowed":
        return all(source.get(field) for field in profile_fields)
    if expected_mode == "dedicated":
        # The diagnostic skeleton has an input ID placeholder; source_metadata
        # omits both profile bindings when Dispatcharr initialized with None.
        return source.get("input_profile_id") is None and "input_profile_name" not in source
    return False


def _assert_live_overview(archive_root, active_path, control_path, channel_uuid, expected_mode):
    """Validate read-only current-recording output while a real attempt is live."""
    import hashlib
    from datetime import datetime, timezone

    from catchuparr.plugin import Plugin

    archive_root = Path(archive_root)
    archive_db = archive_root / "archive.sqlite3"
    outbox_db = active_path.with_name(".catchuparr-sharing-authority.sqlite3")

    def file_digest(path):
        try:
            return hashlib.sha256(path.read_bytes()).digest()
        except FileNotFoundError:
            return None

    def state_snapshot():
        return (
            active_path.read_bytes(), control_path.read_bytes(),
            file_digest(outbox_db),
        )

    before = state_snapshot()
    result = _run_current_recordings_readonly(
        lambda: Plugin.__new__(Plugin).run("current_recordings", {}, {}),
        archive_db,
    )
    after = state_snapshot()
    _require(before == after,
             "Current recordings overview changed applied configuration or outbox state")
    _require(result.get("status") == "ok" and isinstance(result.get("channels"), list),
             "Current recordings overview did not return applied channel rows")
    rows = {row.get("channel_uuid"): row for row in result["channels"]}
    row = rows.get(str(channel_uuid))
    _require(row is not None and row.get("state") == "recording"
             and row.get("mode") == expected_mode,
             f"Live {expected_mode} overview did not confirm indexed recorder progress")
    progress = row.get("last_confirmed_progress_at")
    try:
        parsed = datetime.fromisoformat(progress.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        parsed = None
    _require(isinstance(parsed, datetime) and parsed.tzinfo is not None
             and parsed.utcoffset() == timezone.utc.utcoffset(parsed),
             "Current recordings overview omitted UTC media progress time")
    source = row.get("source")
    _require(
        _overview_source_metadata_matches(source, expected_mode),
        "Current recordings overview omitted exact source/provider/profile metadata",
    )
    _require("://" not in str(source) and "token" not in str(source).lower(),
             "Current recordings overview leaked a URL or credential-like value")
    _require(type(row.get("indexed_segments_since_attempt")) is int
             and row["indexed_segments_since_attempt"] > 0
             and row.get("recent_media_progress") is True,
             "Current recordings overview did not confirm a fresh indexed segment")
    expected_problems = sum(
        item.get("state") in {"unknown", "error", "cleanup_pending", "no_media_progress"}
        for item in result["channels"]
    )
    _require(result.get("summary", {}).get("problems") == expected_problems,
             "Current recordings summary problem count disagrees with channel states")


def global_sharing_draft(
    settings: dict, *, enabled: bool, live_stability_seconds: int,
    last_viewer_return_seconds: int, min_switch_interval_seconds: int,
) -> dict:
    """Build settings through the current global Dispatcharr GUI fields."""
    values = {
        "connection_sharing_enabled": enabled,
        "live_stability_seconds": live_stability_seconds,
        "last_viewer_return_seconds": last_viewer_return_seconds,
        "min_switch_interval_seconds": min_switch_interval_seconds,
    }
    if type(enabled) is not bool or any(
        type(values[name]) is not int or not 0 <= values[name] <= 3600
        for name in (
            "live_stability_seconds", "last_viewer_return_seconds",
            "min_switch_interval_seconds",
        )
    ):
        raise ValueError("global sharing settings are outside the plugin GUI schema")
    result = dict(settings)
    result.update(values)
    result.pop("connection_sharing_config", None)
    return result


def assert_applied_global_sharing(
    active: dict, channel_uuid: str, *, enabled: bool,
    live_stability_seconds: int, last_viewer_return_seconds: int,
    min_switch_interval_seconds: int,
) -> None:
    """Check both persisted GUI values and the runtime-expanded channel view."""
    _require(isinstance(active, dict), "Applied active configuration is unavailable")
    expected = {
        "connection_sharing_enabled": enabled,
        "live_stability_seconds": live_stability_seconds,
        "last_viewer_return_seconds": last_viewer_return_seconds,
        "min_switch_interval_seconds": min_switch_interval_seconds,
    }
    _require(active.get("connection_sharing_enabled") is enabled,
             "Applied global sharing enable setting differs from the requested GUI value")
    _require(
        all(
            type(active.get(key)) is type(value)
            and active.get(key) == value
            for key, value in expected.items()
        ),
        "Applied global sharing delays differ from the requested GUI values",
    )
    runtime = active.get("connection_sharing", {}).get(channel_uuid)
    _require(isinstance(runtime, dict), "Applied sharing runtime view omitted the channel")
    expected_runtime = {
        "enabled": enabled,
        "live_stability_seconds": live_stability_seconds,
        "last_viewer_return_seconds": last_viewer_return_seconds,
        "min_switch_interval_seconds": min_switch_interval_seconds,
    }
    _require(
        set(runtime) == set(expected_runtime)
        and all(type(runtime.get(key)) is type(value) and runtime.get(key) == value
                for key, value in expected_runtime.items()),
             "Applied channel runtime sharing values differ from the GUI settings")


def _crash_child_failure_diagnostic(result, expected_code, parsed_value, stdout_shape):
    """Describe a child failure without exposing captured stdout or stderr."""
    if not isinstance(parsed_value, dict):
        shape = stdout_shape
        keys = []
        exception_class = "unavailable"
    else:
        shape = "object"
        keys = sorted(
            key for key in parsed_value
            if key in {
                "native_registered", "completed", "pending", "denied",
                "closed", "opened", "read_ts", "error", "exception_class",
            }
        )
        raw_exception_class = parsed_value.get("exception_class")
        exception_class = (
            raw_exception_class
            if raw_exception_class in {
                "AssertionError", "AttributeError", "BorrowBindingLost",
                "ConnectionError", "ImportError", "JSONDecodeError", "KeyError", "OSError",
                "PermissionError", "RedisError", "RuntimeError", "TimeoutError",
                "TypeError", "ValueError",
            }
            else "other" if parsed_value.get("error") is True else "unavailable"
        )
    exit_code = result.returncode if type(result.returncode) is int else None
    return {
        "exit_code": exit_code,
        "expected_exit_code": expected_code,
        "stdout_json_shape": shape,
        "known_result_keys": keys,
        "child_exception_class": exception_class,
    }


def _parse_crash_child_result(stdout):
    """Parse exactly one framed child receipt while ignoring unrelated startup output."""
    if not isinstance(stdout, str):
        return None, "invalid_output"
    frames = [
        line[len(CRASH_CHILD_RESULT_PREFIX):]
        for line in stdout.splitlines()
        if line.startswith(CRASH_CHILD_RESULT_PREFIX)
    ]
    if not frames:
        return None, "no_receipt"
    if len(frames) != 1:
        return None, "multiple_receipts"
    try:
        value = json.loads(frames[0])
    except (json.JSONDecodeError, TypeError):
        return None, "invalid_receipt_json"
    if isinstance(value, dict):
        return value, "object"
    if isinstance(value, list):
        return None, "array"
    return None, "scalar"


def _require_with_failure_diagnostic(
    condition: bool,
    message: str,
    diagnostic_factory,
    *,
    emit=print,
    diagnostic_label="CATCHUPARR_NO_SOURCE_PAUSE_DIAGNOSTIC",
) -> None:
    """Emit best-effort diagnostics without replacing the original gate error."""
    if condition:
        return
    try:
        diagnostic = diagnostic_factory()
        emit(diagnostic_label + " " + json.dumps(
            diagnostic, sort_keys=True,
        ))
    except Exception as exc:
        try:
            emit(diagnostic_label + " " + json.dumps({
                "diagnostic_error": type(exc).__name__,
            }, sort_keys=True))
        except Exception:
            pass
    raise RuntimeError(message)


def _native_teardown_readiness_timeout(
    shutdown_delay: float,
    cleanup_interval: float,
    *,
    stopping_marker_ttl: float = 30.0,
    monitor_allowance: float = 5.0,
) -> float:
    """Budget sequential native disconnect, cleanup, and stop-marker phases."""
    return max(
        stopping_marker_ttl + monitor_allowance,
        float(shutdown_delay) + float(cleanup_interval)
        + stopping_marker_ttl + monitor_allowance,
    )


def _native_teardown_ready_conditions(
    *,
    route_available: bool,
    stopping_marker_absent: bool,
    metadata_not_stopping: bool,
    owner_absent: bool,
    clients_zero: bool,
    managers_absent: bool,
    source_inactive: bool,
    profile_at_baseline: bool,
    previous_borrow_absent: bool,
) -> dict[str, bool]:
    """Define readiness from exact A teardown facts, independent of B activity."""
    return {
        "native_route_admits_new_clients": route_available,
        "redis_stopping_marker_absent": stopping_marker_absent,
        "metadata_not_stopping": metadata_not_stopping,
        "native_owner_absent": owner_absent,
        "native_clients_zero": clients_zero,
        "native_managers_absent": managers_absent,
        "source_a_inactive": source_inactive,
        "profile_a_at_baseline": profile_at_baseline,
        "previous_borrow_registration_absent": previous_borrow_absent,
    }


def _wait_for_native_teardown_after_pause(
    pause_conditions: dict[str, bool],
    wait_for_native_teardown,
    read_final_conditions,
):
    """Wait only after CLOSED/PAUSED proof, then read final gate conditions."""
    required = {
        "episode_closed", "borrow_hash_absent", "borrow_membership_absent",
        "borrow_marker_absent", "ledger_paused", "grace_deadline_elapsed",
        "task_completed_cleanly",
    }
    _require(
        set(pause_conditions) == required and all(pause_conditions.values()),
        "Borrow cleanup did not settle the no-source task in PAUSED after native grace",
    )
    wait_for_native_teardown()
    return read_final_conditions()


def _source_removal_transition_conditions(
    *,
    a_owner_preserved: bool,
    a_active: bool,
    a_client_preserved: bool,
    a_provider_preserved: bool,
    a_profile_preserved: bool,
    a_assignments_preserved: bool,
    b_worker_released: bool,
    b_native_resources_absent: bool,
    b_binding_preserved: bool,
    b_reservation_released: bool,
    b_private_markers_absent: bool,
    b_provider_inactive: bool,
    b_request_count_preserved: bool,
    b_profile_released: bool,
) -> dict[str, bool]:
    """Keep the ordinary A viewer while the removed dedicated B worker drains."""
    return {
        "ordinary_a_owner_preserved": a_owner_preserved,
        "ordinary_a_active": a_active,
        "ordinary_a_client_preserved": a_client_preserved,
        "ordinary_a_provider_preserved": a_provider_preserved,
        "ordinary_a_profile_preserved": a_profile_preserved,
        "ordinary_a_assignments_preserved": a_assignments_preserved,
        "dedicated_b_worker_released": b_worker_released,
        "dedicated_b_native_resources_absent": b_native_resources_absent,
        "dedicated_b_binding_preserved": b_binding_preserved,
        "dedicated_b_reservation_released": b_reservation_released,
        "dedicated_b_private_markers_absent": b_private_markers_absent,
        "dedicated_b_provider_inactive": b_provider_inactive,
        "dedicated_b_request_count_preserved": b_request_count_preserved,
        "dedicated_b_profile_released": b_profile_released,
    }


def _stop_join_and_freeze_attempt_bindings(
    task,
    read_records,
    *,
    baseline_worker_ids: set[str],
    stream_id: str,
    cooperative_sleep=time.sleep,
):
    """Stop a recorder task, then freeze every exact attempt it created.

    Attempts observed before stop are retained as evidence, but the final set
    is read only after join so a retry created during shutdown cannot escape
    the cleanup oracle.
    """
    def matching(records):
        return [
            record for record in records
            if record.get("stream_id") == stream_id
            and record.get("worker_id")
            and str(record.get("worker_id")) not in baseline_worker_ids
        ]

    before = matching(read_records())
    task.stop()
    task.join(timeout=25, cooperative_sleep=cooperative_sleep)
    after = matching(read_records())
    by_worker = {}
    for record in (*before, *after):
        worker_id = str(record["worker_id"])
        binding = (
            record.get("capability_digest"),
            record.get("reservation_id"),
        )
        if (not all(isinstance(value, str) and value for value in binding)
                or (worker_id in by_worker and by_worker[worker_id] != binding)):
            return (), {}, False
        by_worker[worker_id] = binding
    return tuple(sorted(by_worker)), {
        worker: {
            "capability_digest": by_worker[worker][0],
            "reservation_id": by_worker[worker][1],
        }
        for worker in sorted(by_worker)
    }, bool(by_worker)


def _attempt_records_match_frozen_bindings(
    records,
    *,
    baseline_worker_ids: set[str],
    stream_id: str,
    attempt_ids: tuple[str, ...],
    bindings: dict,
):
    """Require an unchanged frozen attempt set and all exact credentials."""
    matching = [
        record for record in records
        if record.get("stream_id") == stream_id
        and record.get("worker_id")
        and str(record.get("worker_id")) not in baseline_worker_ids
    ]
    current_ids = {str(record["worker_id"]) for record in matching}
    if current_ids != set(attempt_ids):
        return False, {}
    by_worker = {str(record["worker_id"]): record for record in matching}
    exact = all(
        by_worker[worker].get("capability_digest")
        == bindings[worker]["capability_digest"]
        and by_worker[worker].get("reservation_id")
        == bindings[worker]["reservation_id"]
        for worker in attempt_ids
    )
    return exact, by_worker


def _profile_count_matches_original_baseline(current_count: int, original_baseline: int) -> bool:
    """Do not let an intermediate async reservation become the expected value."""
    return current_count == original_baseline


def _run_after_required_gate(gate, action):
    """Run a side effect only after the required gate returns successfully."""
    gate()
    return action()


def _run_after_readiness_ack(
    snapshot_reader,
    action,
    timeout_seconds: float,
    *,
    message: str,
    diagnostic_label: str,
    monotonic=time.monotonic,
    sleep=time.sleep,
    poll_interval: float = 0.1,
):
    """Do not begin a replacement attempt before its release snapshot is ACKed."""
    ready, snapshot = _wait_for_readiness_snapshot(
        snapshot_reader, timeout_seconds,
        monotonic=monotonic, sleep=sleep, poll_interval=poll_interval,
    )
    _require_with_failure_diagnostic(
        ready, message, lambda: snapshot, diagnostic_label=diagnostic_label,
    )
    return action()


def _wait_for_source_removal_transition(
    snapshot_reader,
    timeout_seconds: float,
    *,
    monotonic=time.monotonic,
    sleep=time.sleep,
    poll_interval: float = 0.1,
):
    """Fail fast on any A change; wait only for the independent B cleanup ACK."""
    deadline = monotonic() + max(0.0, float(timeout_seconds))
    while True:
        snapshot = snapshot_reader()
        conditions = snapshot.get("conditions", {})
        a_conditions = {
            key: value for key, value in conditions.items()
            if key.startswith("ordinary_a_")
        }
        b_conditions = {
            key: value for key, value in conditions.items()
            if key.startswith("dedicated_b_")
        }
        if not a_conditions or not b_conditions or not all(a_conditions.values()):
            return False, snapshot, "ordinary_a_changed"
        if all(b_conditions.values()):
            return True, snapshot, None
        remaining = deadline - monotonic()
        if remaining <= 0:
            return False, snapshot, "dedicated_b_release_timeout"
        sleep(min(poll_interval, remaining))


def _read_after_current_buffer_head(session, buffer, *, minimum_bytes: int, timeout: float):
    """Read only chunks published after the newly admitted buffer's current head."""
    floor = _current_buffer_head(buffer)
    return session.reader.read_after(floor, minimum_bytes=minimum_bytes, timeout=timeout)


def _current_buffer_head(buffer) -> int:
    return int(buffer.redis_client.get(buffer.buffer_index_key) or 0)


def _allowlisted_state(value, allowed: frozenset[str]) -> str:
    """Keep free-form ledger contents out of probe diagnostics."""
    return value if isinstance(value, str) and value in allowed else "unknown"


def _native_client_identity_matches(
    client_ids,
    *,
    expected_client_id: str,
    connected_at,
    expected_connected_at: str,
    worker_id,
    expected_worker_id: str,
) -> bool:
    return (
        set(client_ids) == {expected_client_id}
        and connected_at == expected_connected_at
        and worker_id == expected_worker_id
    )


def _wait_for_readiness_snapshot(
    snapshot_reader,
    timeout_seconds: float,
    *,
    monotonic=time.monotonic,
    sleep=time.sleep,
    poll_interval: float = 0.1,
):
    """Poll a read-only boolean snapshot until ready or its explicit bound."""
    deadline = monotonic() + max(0.0, float(timeout_seconds))
    while True:
        snapshot = snapshot_reader()
        conditions = snapshot.get("conditions", {})
        if conditions and all(conditions.values()):
            return True, snapshot
        remaining = deadline - monotonic()
        if remaining <= 0:
            return False, snapshot
        sleep(min(poll_interval, remaining))


def _open_after_readiness(wait_for_ready, open_request):
    """Issue exactly one request, only after the read-only readiness gate."""
    wait_for_ready()
    return open_request()


def _recorder_task_early_exit_diagnostic(run) -> dict:
    """Return only safe terminal task/attempt summaries, never inputs or secrets."""
    lock = getattr(run, "_lock", None)
    if lock is None:
        failures = list(getattr(run, "failures", ()))
        results = list(getattr(run, "results", ()))
        inputs = list(getattr(run, "input_headers", ()))
        processes = list(getattr(run, "processes", ()))
    else:
        with lock:
            failures = list(getattr(run, "failures", ()))
            results = list(getattr(run, "results", ()))
            inputs = list(getattr(run, "input_headers", ()))
            processes = list(getattr(run, "processes", ()))

    allowed_statuses = {
        "stopped", "switch_requested", "media_stalled", "exited", "no_media",
        "already_running", "no_permitted_sources", "proxy_integration_unsupported",
        "sharing_return_recovery_pending", "sharing_authority_unavailable",
        "sharing_cleanup_pending", "sharing_admission_uncertain", "recording_paused",
        "sharing_unavailable", "control_unavailable", "legacy_job", "stale_control",
        "disabled", "stale_configuration", "invalid_schedule", "outside_schedule",
    }
    allowed_exception_classes = {
        "AssertionError", "AttributeError", "BorrowBindingLost", "CalledProcessError",
        "ConnectionError", "FileExistsError", "ImportError", "KeyError", "OSError",
        "PermissionError", "RecorderAttemptAdmissionUncertain", "RecorderControlError",
        "RecorderLeaseError", "RedisError", "RuntimeError", "ScheduleError",
        "TimeoutError", "TimeoutExpired", "TypeError", "ValueError",
    }

    def safe_status(value):
        return value if isinstance(value, str) and value in allowed_statuses else "other"

    attempt_summaries = []
    for result in results:
        useful = getattr(result, "useful_segments", None)
        return_code = getattr(result, "return_code", None)
        attempt_summaries.append({
            "status": safe_status(getattr(result, "status", None)),
            "useful_segments": useful if type(useful) is int and useful >= 0 else None,
            "return_code": return_code if type(return_code) is int else None,
            "switch_id_present": isinstance(
                getattr(result, "attempt_switch_id", None), str,
            ),
        })

    task_result = getattr(run, "result", None)
    task_status = (
        safe_status(task_result.get("status"))
        if isinstance(task_result, dict) else "unavailable"
    )
    failure_classes = []
    for failure in failures:
        if isinstance(failure, str):
            class_name = (
                failure[len("task:"):] if failure.startswith("task:") else failure
            )
        else:
            class_name = ""
        failure_classes.append(
            class_name if class_name in allowed_exception_classes else "other"
        )

    process_exit_codes = []
    for process in processes:
        code = process.poll()
        process_exit_codes.append(code if type(code) is int else None)

    thread = getattr(run, "thread", None)
    stop_event = getattr(run, "stop_event", None)
    return {
        "thread_alive": bool(thread is not None and thread.is_alive()),
        "stop_requested": bool(stop_event is not None and stop_event.is_set()),
        "stop_source": (
            getattr(stop_event, "first_set_source", "unavailable")
            if stop_event is not None else "unavailable"
        ),
        "supervisor_admission_status": (
            getattr(stop_event, "first_supervisor_admission_status", "unavailable")
            if stop_event is not None else "unavailable"
        ),
        "supervisor_stop_reason": (
            getattr(stop_event, "first_supervisor_stop_reason", "unavailable")
            if stop_event is not None else "unavailable"
        ),
        "task_status": task_status,
        "failure_classes": failure_classes,
        "exception_call_sites": [
            [
                {
                    "file": (
                        frame.get("file")[:80]
                        if isinstance(frame.get("file"), str) else None
                    ),
                    "function": (
                        frame.get("function")[:80]
                        if isinstance(frame.get("function"), str) else None
                    ),
                    "line": (
                        frame.get("line")
                        if type(frame.get("line")) is int else None
                    ),
                }
                for frame in call_sites[-8:]
                if isinstance(frame, dict)
            ]
            for call_sites in getattr(run, "exception_call_sites", ())[-4:]
            if isinstance(call_sites, list)
        ],
        "attempts": attempt_summaries,
        "attempt_count": len(inputs),
        "process_exit_codes": process_exit_codes,
    }


def _wait_for_allowed_source_request(
    run,
    request_times_reader,
    request_baseline: int,
    *,
    timeout: float,
    sleep=time.sleep,
    monotonic=time.monotonic,
) -> float:
    """Wait for the existing allowed-source request gate and report task state on failure."""
    message = "Allowed B did not start after the observed native return deadline"
    deadline = monotonic() + timeout
    request_times = request_times_reader()
    while len(request_times) <= request_baseline and monotonic() < deadline:
        sleep(0.05)
        request_times = request_times_reader()
    _require_with_failure_diagnostic(
        len(request_times) > request_baseline,
        message,
        lambda: _recorder_task_early_exit_diagnostic(run),
        diagnostic_label="CATCHUPARR_RECORDER_EARLY_EXIT_DIAGNOSTIC",
    )
    return request_times[request_baseline]


def _assert_private_worker_stop_request_count(before: dict, after: dict) -> None:
    """A cooperative stop must not open another provider request."""
    _require(
        after.get("source-b.ts") == before.get("source-b.ts"),
        "Stopping a private recorder unexpectedly reopened provider B",
    )


def _run_registration_crash_recovery_probe(
    *, active_path: Path, settings: dict, archive_root: Path, channel_uuid: str,
    redis_client, native_server, redis_keys, metadata_key: str,
    native_owner: str, active_state: str, source_server, profile_a,
    profile_connections_key: str, profile_a_baseline: int,
) -> None:
    """Crash one owned request after real REGISTER, then recover in a new process."""
    from apps.proxy.live_proxy.constants import ChannelMetadataField

    from catchuparr.adapters.sharing_native import native_registration_is_absent
    from catchuparr.configuration import (
        apply_configuration,
        load_active_configuration,
        load_applied_state,
    )
    from catchuparr.engine.leases import RedisRecorderLease
    from catchuparr.engine.store import ArchiveStore
    from catchuparr.sharing_episode import BorrowEpisodeLedger
    from catchuparr.sharing_runtime import RecorderBorrowCoordinator

    shared_settings = global_sharing_draft(
        settings, enabled=True, live_stability_seconds=0,
        last_viewer_return_seconds=0, min_switch_interval_seconds=0,
    )
    lease = None
    restore_settings = True
    helper = Path(__file__).resolve().with_name("aio_sharing_registration_crash.py")
    import catchuparr

    plugin_root = Path(catchuparr.__file__).resolve().parent.parent

    def run_child(mode: str, payload: dict, expected_code: int) -> dict:
        try:
            result = subprocess.run(
                [sys.executable, str(helper), mode],
                cwd="/app",
                env={
                    **os.environ,
                    "CATCHUPARR_APP_ROOT": "/app",
                    "CATCHUPARR_PLUGIN_ROOT": str(plugin_root),
                },
                input=json.dumps(payload, separators=(",", ":")),
                capture_output=True,
                text=True,
                timeout=35,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError(f"Owned crash-probe child failed: {type(exc).__name__}") from None
        value, stdout_shape = _parse_crash_child_result(result.stdout)
        _require_with_failure_diagnostic(
            result.returncode == expected_code and isinstance(value, dict),
            f"Owned crash-probe child returned an unexpected result in {mode}",
            lambda: _crash_child_failure_diagnostic(
                result, expected_code, value, stdout_shape,
            ),
            diagnostic_label="CATCHUPARR_SHARING_CRASH_CHILD_FAILURE",
        )
        return value

    def prepare_fresh():
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            prepared = coordinator.poll(active=active, control_generation=control.generation)
            if prepared is not None:
                return prepared
            time.sleep(0.1)
        diagnostic = {}
        try:
            from catchuparr.adapters.sharing_native import (
                _private_client,
                _text,
                observe_native_connection,
                read_native_episode_grant,
            )
            from catchuparr.configuration import _sharing_catalog_generation
            from catchuparr.recorder_proxy import configuration_generation
            from catchuparr.sharing_planner import SharingObservation, SharingSettings, observe
            from catchuparr.sharing_runtime import _native_source_catalog

            current_sources, current_catalog = _native_source_catalog(channel_uuid)
            native = observe_native_connection(
                redis_client, channel_uuid,
                catalog_generation=current_catalog,
                assigned_sources=current_sources,
                observed_at=time.time(),
            )
            target = native.target if native is not None else None
            latest = coordinator.intent_ledger.read(channel_uuid)
            state = latest.state if latest is not None else None
            setting = active.get("connection_sharing", {}).get(channel_uuid, {})
            current_generation = configuration_generation(active)
            live_episode = read_native_episode_grant(redis_client, channel_uuid)
            native_observer = {}
            owner_key = f"live:channel:{channel_uuid}:owner"
            metadata_key = f"live:channel:{channel_uuid}:metadata"
            owner_raw = redis_client.get(owner_key)
            metadata_raw = redis_client.hgetall(metadata_key) or {}
            metadata = {_text(key): _text(value) for key, value in metadata_raw.items()}
            owner = _text(owner_raw) if owner_raw else ""
            heartbeat_key = f"live:worker:{owner}:heartbeat"
            source_matches = [
                source for source in current_sources
                if source.channel_uuid == channel_uuid
                and source.stream_id == metadata.get("stream_id")
                and source.m3u_profile_id == metadata.get("m3u_profile")
                and source.input_profile_id == metadata.get("stream_profile")
                and source.assigned
            ]
            buffer_index_key = f"live:channel:{channel_uuid}:input:buffer:index"
            head_raw = redis_client.get(buffer_index_key)
            buffer_head = int(_text(head_raw)) if head_raw is not None else 0
            newest_chunks = redis_client.zrevrange(
                f"live:channel:{channel_uuid}:input:buffer:chunk_timestamps", 0, 0,
            )
            newest_chunk = int(_text(newest_chunks[0])) if newest_chunks else 0
            clients_key = f"live:channel:{channel_uuid}:clients"
            native_client_ids = [_text(item) for item in redis_client.smembers(clients_key)]
            ordinary_client_ids = []
            unknown_client_classifications = 0
            for client_id in native_client_ids:
                classification = _private_client(redis_client, channel_uuid, client_id)
                if classification is None:
                    unknown_client_classifications += 1
                    continue
                if classification:
                    continue
                record = redis_client.hgetall(
                    f"live:channel:{channel_uuid}:clients:{client_id}",
                ) or {}
                fields = {_text(key): _text(value) for key, value in record.items()}
                if (
                    fields.get("worker_id")
                    and fields.get("connected_at")
                    and not redis_client.exists(
                        f"live:channel:{channel_uuid}:client:{client_id}:stop",
                    )
                ):
                    ordinary_client_ids.append(client_id)
            source = source_matches[0] if len(source_matches) == 1 else None
            native_observer = {
                "owner_present": bool(owner),
                "metadata_present": bool(metadata),
                "metadata_owner_matches_lock": metadata.get("owner") == owner and bool(owner),
                "metadata_state_allowed": metadata.get("state", "").lower()
                in {"active", "waiting_for_clients"},
                "metadata_channel_matches": metadata.get("channel_uuid") == channel_uuid,
                "owner_ttl_positive": int(redis_client.ttl(owner_key)) > 0,
                "channel_not_stopping": not bool(
                    redis_client.exists(f"live:channel:{channel_uuid}:stopping"),
                ),
                "worker_heartbeat_positive": bool(
                    redis_client.exists(heartbeat_key)
                    and int(redis_client.ttl(heartbeat_key)) > 0
                ),
                "metadata_stream_present": bool(metadata.get("stream_id")),
                "metadata_m3u_profile_present": bool(metadata.get("m3u_profile")),
                "metadata_input_profile_present": bool(metadata.get("stream_profile")),
                "assigned_metadata_match_count": len(source_matches),
                "matched_source_eligible": bool(
                    source
                    and source.account_active
                    and source.profile_active
                    and source.media_compatible
                    and source.supported_worker_kind
                    and not source.redirect_channel
                    and not source.vod_or_preview
                ),
                "buffer_head_positive": buffer_head > 0,
                "buffer_timestamp_present": bool(newest_chunks),
                "buffer_chunk_in_range": 0 < newest_chunk <= buffer_head,
                "buffer_chunk_present": bool(
                    newest_chunk > 0
                    and redis_client.exists(
                        f"live:channel:{channel_uuid}:input:buffer:chunk:{newest_chunk}",
                    )
                ),
                "native_client_count": len(native_client_ids),
                "unknown_private_marker_classifications": unknown_client_classifications,
                "ordinary_clients_with_valid_records": len(ordinary_client_ids),
            }
            diagnostic = {
                "active_sharing_enabled": setting.get("enabled") is True,
                "control_paused": bool(control.paused),
                "config_generation_matches": current_generation == coordinator.configuration_generation,
                "catalog_generation_matches": (
                    current_catalog == coordinator.catalog_generation
                    and _sharing_catalog_generation(channel_uuid) == current_catalog
                ),
                "recorder_lease_current": coordinator._recovery_is_current(
                    channel_uuid,
                    latest.revision if latest is not None else None,
                    coordinator.configuration_generation,
                    control.generation,
                    coordinator.recorder_fence,
                ),
                "native_episode_phase": live_episode.phase if live_episode else "absent",
                "native_target_present": target is not None,
                "native_target_healthy": bool(
                    target and target.healthy_existing_connection(channel_uuid)
                ),
                "native_target_eligible": bool(
                    target and target.eligible_for_new_borrow(channel_uuid)
                ),
                "ordinary_viewer_count": len(target.ordinary_client_ids) if target else 0,
                "native_observer_guards": native_observer,
                "source_catalog_row_count": len(current_sources),
                "intent_action": latest.intent.action.value if latest and latest.intent else None,
                "sharing_phase": state.phase.value if state else None,
                "last_switch_clock_present": bool(state and state.last_switch_at is not None),
            }
            if state is not None and setting.get("enabled") is True:
                planned = observe(
                    state,
                    SharingObservation(
                        channel_uuid=channel_uuid,
                        now=time.time(),
                        configuration_generation=coordinator.configuration_generation,
                        control_generation=control.generation,
                        recorder_fence=coordinator.recorder_fence,
                        lease_is_current=diagnostic["recorder_lease_current"],
                        settings=SharingSettings(
                            enabled=True,
                            live_stability_seconds=setting["live_stability_seconds"],
                            last_viewer_return_seconds=setting["last_viewer_return_seconds"],
                            min_switch_interval_seconds=setting["min_switch_interval_seconds"],
                        ),
                        target=target,
                        dedicated_source_available=True,
                    ),
                )
                diagnostic["planner_action"] = planned.action.value
                diagnostic["planner_reason"] = planned.reason
        except Exception as exc:
            diagnostic["diagnostic_error_type"] = type(exc).__name__
        raise RuntimeError(
            "Coordinator did not issue the synthetic live-viewer borrow: "
            + json.dumps(diagnostic, sort_keys=True)
        )

    try:
        applied = apply_configuration(shared_settings, active_path=active_path)
        _require(applied.get("applied") is True, "Crash-recovery sharing Apply failed")
        active = load_active_configuration(active_path)
        _require(active is not None, "Crash-recovery sharing snapshot is unavailable")
        assert_applied_global_sharing(
            active, channel_uuid, enabled=True, live_stability_seconds=0,
            last_viewer_return_seconds=0, min_switch_interval_seconds=0,
        )
        _, control = load_applied_state(active_path)
        provider_before = source_server.snapshot()
        profile_before = _profile_count(redis_client, profile_a.id, profile_connections_key)
        _require(
            profile_before == profile_a_baseline + 1
            and provider_before[1].get("source-a.ts") == 1
            and provider_before[1].get("source-b.ts") == 0,
            "Crash probe did not start with the original live-A provider binding",
        )
        store = ArchiveStore(archive_root)
        lease = RedisRecorderLease(
            redis_client, channel_uuid, ttl_seconds=90, archive_store=store,
        )
        fence = lease.acquire()
        _require(fence is not None, "Crash-recovery recorder lease was unavailable")
        database_path = archive_root / ".catchuparr-sharing.sqlite3"
        coordinator = RecorderBorrowCoordinator(
            redis_client, archive_root, channel_uuid, active=active,
            control_generation=control.generation, recorder_fence=fence,
            lease_value=lease._value, dedicated_source_available=True,
        )

        first = prepare_fresh()
        capability = __import__(
            "catchuparr.adapters.sharing_borrow_route", fromlist=["encode_borrow_capability"],
        ).encode_borrow_capability(first.capability)
        crashed = run_child(
            "crash-after-register", {"capability": capability}, expected_code=73,
        )
        _require(crashed.get("native_registered") is True,
                 "Child exited before confirming the real native registration ACK")
        opening = BorrowEpisodeLedger(database_path).read(channel_uuid)
        _require(
            opening.episode_id == first.episode_id
            and opening.phase == "opening"
            and opening.client_id == first.client_id,
            "Crash did not leave the exact durable registration attempt pending",
        )
        registration = BorrowEpisodeLedger(database_path).registration_attempt(first.episode_id)
        _require(
            registration.status == "prepared"
            and registration.client_id == first.client_id
            and registration.connected_at is None
            and registration.projection_version is None,
            "Crash child persisted a registration ACK before exiting",
        )
        from catchuparr.adapters.sharing_borrow_route import _recover_registration_ack

        native_ack = _recover_registration_ack(
            redis_client, opening, client_id=registration.client_id,
            local_worker_id=registration.local_worker_id,
            user_agent=registration.user_agent,
            client_ip=registration.client_ip,
            backend_signature=registration.backend_signature,
        )
        _require(
            native_ack is not None
            and native_ack.episode_id == first.episode_id
            and native_ack.client_id == first.client_id
            and native_ack.projection_version == first.episode_version + 1
            and native_ack.connected_at,
            "Native client hash, marker, set, and episode projection do not match the prepared ACK",
        )

        recovered = run_child(
            "reconcile",
            {"database_path": str(database_path), "episode_id": first.episode_id},
            expected_code=0,
        )
        _require(recovered == {"completed": 1, "pending": 0},
                 "Fresh process did not reconcile exactly the crashed registration")
        closed = BorrowEpisodeLedger(database_path).read(channel_uuid)
        _require(
            closed.episode_id == first.episode_id and closed.phase == "closed"
            and native_registration_is_absent(
                redis_client, channel_uuid=channel_uuid, client_id=first.client_id,
            ),
            "Restart recovery did not close and remove the exact borrowed client",
        )
        _wait_for_native_client_count(
            redis_client, native_server, redis_keys, channel_uuid, 1,
        )
        _require(
            _redis_text(redis_client.get(redis_keys.channel_owner(channel_uuid))) == native_owner
            and _redis_text(redis_client.hget(metadata_key, ChannelMetadataField.STATE)) == active_state
            and source_server.snapshot() == provider_before
            and _profile_count(redis_client, profile_a.id, profile_connections_key) == profile_before,
            "Crash recovery changed the original viewer owner or native worker",
        )
        replay = run_child("replay", {"capability": capability}, expected_code=0)
        _require(replay == {"denied": True}, "Consumed crash capability was replayable")

        # Recovery of the closed ATTACH claim and a new coordinator poll must
        # issue a distinct ticket before a fresh reader is allowed to attach.
        second = prepare_fresh()
        _require(second.episode_id != first.episode_id and second.client_id != first.client_id,
                 "Fresh legitimate borrow reused the crashed episode identity")
        second_token = __import__(
            "catchuparr.adapters.sharing_borrow_route", fromlist=["encode_borrow_capability"],
        ).encode_borrow_capability(second.capability)
        read = run_child(
            "open-and-read", {"capability": second_token}, expected_code=0,
        )
        _require(read == {"closed": True, "opened": True, "read_ts": True},
                 "Fresh legitimate borrow did not read aligned native TS packets")
        fresh_episode = BorrowEpisodeLedger(database_path).read(channel_uuid)
        _require(
            fresh_episode.episode_id == second.episode_id
            and fresh_episode.phase == "closed"
            and native_registration_is_absent(
                redis_client, channel_uuid=channel_uuid, client_id=second.client_id,
            ),
            "Fresh legitimate reader did not acknowledge exact client and episode cleanup",
        )
        _wait_for_native_client_count(
            redis_client, native_server, redis_keys, channel_uuid, 1,
        )
        _require(
            _redis_text(redis_client.get(redis_keys.channel_owner(channel_uuid))) == native_owner
            and source_server.snapshot() == provider_before
            and _profile_count(redis_client, profile_a.id, profile_connections_key) == profile_before,
            "Fresh reader cleanup disturbed the original viewer",
        )
    finally:
        try:
            if lease is not None and getattr(lease, "_value", None) is not None:
                lease.release()
        finally:
            if restore_settings:
                restored = apply_configuration(settings, active_path=active_path)
                _require(restored.get("applied") is True,
                         "Crash-recovery sharing configuration could not be restored")


def _current_orm_context_snapshot() -> dict[str, object]:
    """Return only connection counts and transaction booleans for this context."""
    try:
        from django.db import connections

        initialized = connections.all(initialized_only=True)
        default = next(
            (wrapper for wrapper in initialized if wrapper.alias == "default"), None,
        )
        default_initialized = bool(default is not None and default.connection is not None)
        default_autocommit = None
        if default_initialized:
            try:
                default_autocommit = bool(default.get_autocommit())
            except Exception:
                default_autocommit = None
        return {
            "wrapper_count": len(initialized),
            "initialized_count": len(initialized),
            "default_initialized": default_initialized,
            "default_in_atomic_block": bool(default and default.in_atomic_block),
            "default_autocommit": default_autocommit,
        }
    except Exception:
        return {
            "wrapper_count": 0,
            "initialized_count": 0,
            "default_initialized": False,
            "default_in_atomic_block": False,
            "default_autocommit": None,
        }


def _issue_media_probe_attempt(
    redis_client,
    lease,
    *,
    channel_uuid: str,
    candidate: dict,
    active_path: Path,
    internal_base_url: str,
):
    """Issue using the current validated snapshot/control pair, never cached state."""
    from catchuparr.configuration import load_applied_state
    from catchuparr.recorder_proxy import (
        configuration_generation,
        issue_recorder_attempt,
    )

    active, control = load_applied_state(active_path)
    _require(active is not None, "Applied recorder configuration is unavailable")
    _require(not control.paused, "Recorder control is paused during capability issue")
    config_generation = configuration_generation(active)
    _require(
        control.configuration_generation == config_generation,
        "Applied recorder control is not bound to the current configuration",
    )
    return issue_recorder_attempt(
        redis_client,
        lease,
        channel_uuid=channel_uuid,
        candidate=candidate,
        config_generation=config_generation,
        control_generation=control.generation,
        internal_base_url=internal_base_url,
    )


_NATIVE_LIVE_A_INVARIANT_NAMES = (
    "owner_read_ok",
    "owner_present",
    "owner_matches",
    "stream_manager_lookup_ok",
    "stream_manager_present",
    "stream_manager_identity",
    "stream_manager_running",
    "client_manager_lookup_ok",
    "client_manager_present",
    "client_manager_identity",
    "local_client_count_read_ok",
    "local_client_count_one",
    "global_client_count_read_ok",
    "global_client_count_one",
    "metadata_state_read_ok",
    "metadata_active",
)


def _native_live_a_invariant_flags(**observations) -> dict[str, bool]:
    """Return only named boolean checks for the synthetic live-A worker."""
    return {
        name: observations.get(name) is True
        for name in _NATIVE_LIVE_A_INVARIANT_NAMES
    }


def _native_live_a_invariant_message(stage: str, flags: dict[str, bool]) -> str:
    """Format sanitized per-conjunct state without emitting Redis/native values."""
    return (
        f"Native live A invariant failed at {stage}; "
        f"flags={json.dumps(flags, sort_keys=True)}"
    )


class _SyntheticSourceServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False
    allow_reuse_address = True

    def __init__(self, payloads: dict[str, bytes]):
        self.payloads = payloads
        self._counts_lock = threading.Lock()
        self.request_counts = {name: 0 for name in payloads}
        self.active_counts = {name: 0 for name in payloads}
        self.request_started_at = {name: [] for name in payloads}
        self.request_start_callback = None
        self.request_start_callback_errors = []
        self.request_callback_finished = threading.Event()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def _serve(self, *, body: bool) -> None:
                name = self.path.split("?", 1)[0].lstrip("/")
                payload = owner.payloads.get(name)
                if payload is None:
                    self.send_error(404)
                    return
                with owner._counts_lock:
                    owner.request_counts[name] += 1
                    owner.active_counts[name] += 1
                    started_at = time.time()
                    owner.request_started_at[name].append(started_at)
                    request_number = owner.request_counts[name]
                    callback = owner.request_start_callback
                if callback is not None:
                    if not owner._run_request_start_callback(
                        callback, name, request_number, started_at,
                    ):
                        with owner._counts_lock:
                            owner.active_counts[name] -= 1
                        self.send_error(500)
                        return
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "video/mp2t")
                    self.send_header("Cache-Control", "no-store")
                    self.end_headers()
                    if not body:
                        return
                    while True:
                        for offset in range(0, len(payload), 188 * 32):
                            self.wfile.write(payload[offset:offset + 188 * 32])
                            self.wfile.flush()
                            time.sleep(0.02)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                finally:
                    with owner._counts_lock:
                        owner.active_counts[name] -= 1

            def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                self._serve(body=True)

            def do_HEAD(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                self._serve(body=False)

            def log_message(self, _format: str, *_args) -> None:
                return

        super().__init__(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(
            target=self.serve_forever,
            name="catchuparr-synthetic-source",
            daemon=True,
        )

    def _run_request_start_callback(self, callback, name, request_number, started_at):
        """Run and record a boundary check before the upstream response starts."""
        try:
            completed = callback(name, request_number, started_at) is True
        except Exception as exc:
            with self._counts_lock:
                self.request_start_callback_errors.append(exc)
            self.request_callback_finished.set()
            return False
        if completed:
            self.request_callback_finished.set()
        return True

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.server_port}"

    def snapshot(self) -> tuple[dict[str, int], dict[str, int]]:
        with self._counts_lock:
            return dict(self.request_counts), dict(self.active_counts)

    def request_times(self, source_name: str) -> list[float]:
        with self._counts_lock:
            return list(self.request_started_at.get(source_name, ()))

    def close(self) -> None:
        if self.thread.is_alive():
            self.shutdown()
        self.server_close()
        self.thread.join(timeout=5)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and any(self.snapshot()[1].values()):
            time.sleep(0.05)


def _make_transport_stream(
    ffmpeg: str,
    ffprobe: str,
    output: Path,
    *,
    color: str,
    frequency: int,
    service_name: str,
) -> bytes:
    command = [
        ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i", f"color=c={color}:s=320x240:r=25:d=8",
        "-f", "lavfi", "-i", f"sine=frequency={frequency}:sample_rate=48000:duration=8",
        "-shortest", "-c:v", "mpeg2video", "-pix_fmt", "yuv420p", "-g", "12",
        "-b:v", "250k", "-c:a", "mp2", "-b:a", "96k",
        "-metadata", f"service_name={service_name}",
        "-metadata", "service_provider=Synthetic Catchuparr CI",
        "-f", "mpegts", str(output),
    ]
    try:
        subprocess.run(
            command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=30,
        )
        payload = output.read_bytes()
        _require(len(payload) >= 188 * 100, "Synthetic TS fixture is too small")
        _require(payload[0] == 0x47, "Synthetic TS fixture is not packet aligned")
        probe = subprocess.run(
            [ffprobe, "-v", "error", "-count_frames", "-show_programs", "-show_streams", "-of", "json", str(output)],
            check=True, capture_output=True, timeout=15,
        )
        details = json.loads(probe.stdout.decode("utf-8"))
        programs = details.get("programs", [])
        _require(
            any(program.get("tags", {}).get("service_name") == service_name for program in programs),
            "Synthetic TS fixture lost its source identifier",
        )
        frame_counts = {
            item.get("codec_type"): int(item.get("nb_read_frames", "0") or 0)
            for item in details.get("streams", [])
        }
        _require(
            frame_counts.get("video", 0) > 0 and frame_counts.get("audio", 0) > 0,
            "Synthetic TS fixture must decode useful audio and video frames",
        )
        return payload
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Synthetic TS fixture generation exceeded its timeout") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError("Synthetic TS fixture generation failed") from exc


def _stream_default_profile_id(CoreSettings):
    for name in ("get_default_stream_profile_id", "get_default_stream_profile"):
        getter = getattr(CoreSettings, name, None)
        if callable(getter):
            value = getter()
            return getattr(value, "id", value)
    getter = getattr(CoreSettings, "_get_group", None)
    if callable(getter):
        try:
            group = getter("stream_settings")
        except TypeError:
            group = getter("stream_settings", {})
        if isinstance(group, dict):
            return group.get("default_stream_profile")
    raise RuntimeError("Native default StreamProfile setting cannot be inspected")


def _set_stream_default_profile(CoreSettings, profile_id) -> None:
    updater = getattr(CoreSettings, "_update_group", None)
    if not callable(updater):
        raise RuntimeError("Native StreamProfile settings API is unavailable")
    updater("stream_settings", "Stream Settings", {"default_stream_profile": profile_id})


def _key_dump(redis_client, key):
    try:
        return redis_client.dump(key)
    except AttributeError:
        kind = redis_client.type(key)
        if isinstance(kind, bytes):
            kind = kind.decode("ascii")
        if kind in (None, "none"):
            return None
        if kind == "string":
            return (kind, redis_client.get(key))
        if kind == "hash":
            return (kind, tuple(sorted(redis_client.hgetall(key).items())))
        raise RuntimeError("Unexpected Redis type on synthetic native key")


def _profile_count(redis_client, profile_id, key_builder) -> int:
    value = redis_client.get(key_builder(profile_id))
    return int(value or 0)


def _redis_text(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _constant_text(value):
    return _redis_text(getattr(value, "value", value))


class _NativeBufferYieldTracker:
    """Associate parent yielded bytes with the native Redis buffer indexes."""

    def __init__(self, buffer):
        self.buffer = buffer
        self._read_native = buffer.get_optimized_client_data
        self._pending = deque()
        self._lock = threading.Lock()
        self.last_yielded_index = None
        self.unmapped_batches = 0
        self._had_instance_method = "get_optimized_client_data" in vars(buffer)
        self._previous_instance_method = vars(buffer).get("get_optimized_client_data")
        buffer.get_optimized_client_data = self._read

    def _read(self, client_index):
        chunks, next_index = self._read_native(client_index)
        client_index = int(client_index)
        next_index = int(next_index)
        with self._lock:
            if len(chunks) != max(0, next_index - client_index):
                self.unmapped_batches += 1
                self._pending.extend((chunk, None) for chunk in chunks)
                return chunks, next_index
            self._pending.extend(
                (chunk, client_index + offset)
                for offset, chunk in enumerate(chunks, start=1)
            )
        return chunks, next_index

    def observe(self, iterator):
        for chunk in iterator:
            with self._lock:
                self.last_yielded_index = None
                matched_index = None
                for pending_index, (pending_chunk, native_index) in enumerate(self._pending):
                    if pending_chunk is chunk:
                        matched_index = pending_index
                        self.last_yielded_index = native_index
                        break
                if matched_index is not None:
                    for _ in range(matched_index + 1):
                        self._pending.popleft()
            yield chunk

    def close(self):
        if self._had_instance_method:
            self.buffer.get_optimized_client_data = self._previous_instance_method
        else:
            del self.buffer.get_optimized_client_data
        with self._lock:
            self._pending.clear()


class _NativeLiveMediaReader:
    """Continuously read one live client and retain indexed chunks for checks."""

    def __init__(self, iterator, tracker: _NativeBufferYieldTracker):
        self._iterator = iterator
        self._tracker = tracker
        self._condition = threading.Condition()
        self._chunks = deque()
        self._size = 0
        self._finished = False
        self._failure = None
        self._stop_requested = threading.Event()
        self.thread = threading.Thread(
            target=self._run,
            name="catchuparr-live-media-reader",
            daemon=True,
        )
        self.thread.start()

    def _run(self) -> None:
        try:
            while not self._stop_requested.is_set():
                try:
                    raw_chunk = next(self._iterator)
                except StopIteration:
                    break
                index = self._tracker.last_yielded_index
                if index is None:
                    continue
                chunk = bytes(raw_chunk)
                with self._condition:
                    self._chunks.append((index, chunk))
                    self._size += len(chunk)
                    while self._size > 2 * 1024 * 1024 and self._chunks:
                        _old_index, old_chunk = self._chunks.popleft()
                        self._size -= len(old_chunk)
                    self._condition.notify_all()
        except Exception as exc:  # Avoid leaking HTTP or URL details to CI output.
            with self._condition:
                self._failure = type(exc).__name__
                self._condition.notify_all()
        finally:
            with self._condition:
                self._finished = True
                self._condition.notify_all()

    def read_after(self, floor: int, *, minimum_bytes: int, timeout: float) -> tuple[bytes, int]:
        deadline = time.monotonic() + timeout
        with self._condition:
            while True:
                selected = [
                    (index, chunk) for index, chunk in self._chunks if index > floor
                ]
                total = sum(len(chunk) for _index, chunk in selected)
                if total >= minimum_bytes:
                    return b"".join(chunk for _index, chunk in selected), selected[-1][0]
                if self._failure is not None:
                    raise RuntimeError("Native live reader failed while archive recording ran")
                if self._finished:
                    raise RuntimeError("Native live reader ended before fresh media arrived")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError(
                        "Native live route did not publish fresh media before timeout"
                    )
                self._condition.wait(min(remaining, 0.25))

    def close(self, response, *, force_release=None, join_timeout: float = 5) -> None:
        self._stop_requested.set()
        self.thread.join(timeout=join_timeout)
        force_release_error = None
        if self.thread.is_alive() and force_release is not None:
            try:
                force_release()
            except Exception as exc:
                force_release_error = type(exc).__name__
            self.thread.join(timeout=join_timeout)
        _require(not self.thread.is_alive(), "Native live reader did not stop at a chunk boundary")
        response.close()
        if force_release_error is not None:
            raise RuntimeError("Native live reader fallback stop failed")


class _NativeLiveReaderSession:
    """Own exactly one indexed reader from response startup through cleanup."""

    def __init__(self, response, buffer):
        self.response = response
        self.tracker = _NativeBufferYieldTracker(buffer)
        iterator = iter(self.tracker.observe(iter(response.streaming_content)))
        try:
            self.reader = _NativeLiveMediaReader(iterator, self.tracker)
        except Exception:
            self.tracker.close()
            raise
        self._closed = False

    def close(self, *, force_release=None) -> None:
        if self._closed:
            return
        self.reader.close(self.response, force_release=force_release)
        self.tracker.close()
        self._closed = True


def _create_native_live_reader_session(response, native_server, worker_id):
    """Bind the reader to the buffer installed by successful native startup."""
    buffer = native_server.get_buffer(worker_id, profile=None)
    _require(buffer is not None, "Native live route did not install its stream buffer")
    return _NativeLiveReaderSession(response, buffer), buffer


def _assert_tone_matches(actual: int | None, expected: int, label: str) -> None:
    _require(actual is not None, f"{label} media has no decodable synthetic audio tone")
    _require(
        abs(actual - expected) <= 20,
        f"{label} media decoded to an unexpected synthetic source tone",
    )


def _verify_audio_video_tone(
    ffmpeg: str, ffprobe: str, media: bytes, expected: int, label: str,
) -> int:
    _require(
        len(media) >= 188 * 100
        and all(media[offset] == 0x47 for offset in range(0, len(media) - 187, 188)),
        f"{label} route returned no useful packet-aligned transport stream",
    )
    try:
        probe = subprocess.run(
            [
                ffprobe, "-v", "error", "-count_frames", "-f", "mpegts",
                "-show_streams", "-of", "json", "pipe:0",
            ],
            input=media,
            check=True,
            capture_output=True,
            timeout=15,
        )
        decode = subprocess.run(
            [
                ffmpeg, "-hide_banner", "-loglevel", "error", "-f", "mpegts",
                "-i", "pipe:0", "-map", "0:a:0", "-vn", "-ac", "1", "-ar", "48000",
                "-t", "1.5", "-f", "s16le", "pipe:1",
            ],
            input=media,
            check=True,
            capture_output=True,
            timeout=15,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"{label} media decoding exceeded its bounded timeout") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"{label} media was not decodable audio/video") from exc
    details = json.loads(probe.stdout.decode("utf-8"))
    frame_counts = {
        item.get("codec_type"): int(item.get("nb_read_frames", "0") or 0)
        for item in details.get("streams", [])
    }
    _require(
        frame_counts.get("video", 0) > 0 and frame_counts.get("audio", 0) > 0,
        f"{label} route returned no decoded audio/video frames",
    )
    from aio_recorder_failover import _estimate_tone_frequency

    frequency = _estimate_tone_frequency(decode.stdout)
    _assert_tone_matches(frequency, expected, label)
    return frequency


def _verified_task_borrow_client_ids(
    task,
    *,
    worker_id: str,
    channel_uuid: str,
    decode_capability,
    is_server_verified,
):
    """Return only this task's signed borrow IDs that are still server verified."""
    with task._lock:
        headers = [dict(item) for item in task.input_headers]
    verified = set()
    for item in headers:
        token = item.get("X-Catchuparr-Borrow")
        if not isinstance(token, str) or not token:
            continue
        try:
            capability = decode_capability(token)
        except (TypeError, ValueError):
            continue
        if capability.channel_uuid != channel_uuid:
            continue
        client_id = f"catchuparr-client-{capability.nonce}"
        if is_server_verified(worker_id, client_id):
            verified.add(client_id)
    return frozenset(verified)


def _native_ordinary_client_observation(
    redis_client,
    clients_key: str,
    client_ids: set[str],
    verified_borrow_ids: set[str],
    *,
    local_clients: int,
    expected_owner: str,
    expected_identity=None,
):
    """Validate one exact ordinary client plus at most one current task borrow."""
    current_borrow_ids = client_ids & verified_borrow_ids
    ordinary_ids = client_ids - current_borrow_ids
    missing_verified_ids = verified_borrow_ids - client_ids
    _require(
        local_clients == 1
        and len(client_ids) in (1, 2)
        and len(ordinary_ids) == 1
        and len(current_borrow_ids) <= 1
        and len(verified_borrow_ids) <= 1
        and (
            not missing_verified_ids
            or (
                len(client_ids) == 1
                and len(missing_verified_ids) == 1
                and len(verified_borrow_ids) == 1
            )
        ),
        "Native recorder parent client detached or an unverified client joined",
    )
    ordinary_client_id = next(iter(ordinary_ids))
    if expected_identity is not None:
        _require(
            ordinary_client_id == expected_identity["client_id"],
            "Native recorder ordinary client registration changed before ACTIVE",
        )
    ordinary_key = f"{clients_key}:{ordinary_client_id}"
    identity = {
        "client_id": ordinary_client_id,
        "connected_at": _redis_text(redis_client.hget(ordinary_key, "connected_at")),
        "worker_id": _redis_text(redis_client.hget(ordinary_key, "worker_id")),
    }
    _require(
        identity["connected_at"]
        and identity["worker_id"] == expected_owner
        and (expected_identity is None or identity == expected_identity),
        "Native recorder ordinary client registration changed before ACTIVE",
    )
    return identity, bool(missing_verified_ids)


def _wait_for_native_active(
    redis_client,
    native_server,
    worker_id,
    *,
    redis_keys,
    metadata_field,
    channel_state,
    timeout: float = 45,
    verified_borrow_client_ids=None,
    expected_ordinary_identity=None,
):
    """Verify native readiness while preserving the exact ordinary client.

    The default contract remains one global/local ordinary client. Later sharing
    probe phases may pass the current task's server-verified borrow IDs; then one
    exact borrow can join the ordinary client while ACTIVE readiness is pending.
    """
    from time import monotonic

    owner_key = redis_keys.channel_owner(worker_id)
    metadata_key = redis_keys.channel_metadata(worker_id)
    clients_key = redis_keys.clients(worker_id)
    expected_owner = _redis_text(native_server.worker_id)
    active_state = _constant_text(channel_state.ACTIVE)
    deadline = monotonic() + timeout
    ordinary_identity = None

    while True:
        owner = _redis_text(redis_client.get(owner_key))
        manager = native_server.stream_managers.get(worker_id)
        client_manager = native_server.client_managers.get(worker_id)
        _require(owner == expected_owner, "Native recorder parent lost its Redis owner")
        _require(manager is not None, "Native recorder parent stream manager is absent")
        _require(client_manager is not None, "Native recorder parent client manager is absent")
        local_clients = int(client_manager.get_client_count())
        if verified_borrow_client_ids is None:
            global_clients = int(redis_client.scard(clients_key) or 0)
            _require(
                local_clients == 1 and global_clients == 1,
                "Native recorder parent client detached before ACTIVE readiness",
            )
        else:
            # SMEMBERS is the authoritative complete membership observation.
            # Do not compare it with a separately sampled SCARD: an atomic
            # borrow registration can occur between those two Redis reads.
            client_ids = {
                _redis_text(value) for value in redis_client.smembers(clients_key)
            }
            verified_borrow_ids = {
                _redis_text(value) for value in verified_borrow_client_ids()
            }
            expected_identity = expected_ordinary_identity or ordinary_identity
            observed_identity, missing_verified_ids = _native_ordinary_client_observation(
                redis_client,
                clients_key,
                client_ids,
                verified_borrow_ids,
                local_clients=local_clients,
                expected_owner=expected_owner,
                expected_identity=expected_identity,
            )
            ordinary_identity = observed_identity
            if missing_verified_ids:
                # The server verifier saw this task's atomic registration after
                # SMEMBERS. Validate the local ordinary client and reject every
                # unrelated shape before taking a fresh membership snapshot.
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise RuntimeError(
                        "Native recorder did not reach ACTIVE before its bounded timeout"
                    )
                time.sleep(min(0.01, remaining))
                continue
            current_borrow_ids = client_ids & verified_borrow_ids
            _require(
                len(current_borrow_ids) == len(verified_borrow_ids),
                "Native recorder borrow membership changed during readiness",
            )
        state = _redis_text(redis_client.hget(metadata_key, metadata_field.STATE))
        manager_running = bool(getattr(manager, "running", False))
        if state == active_state and manager_running:
            return expected_owner, manager, client_manager

        remaining = deadline - monotonic()
        if remaining <= 0:
            raise RuntimeError("Native recorder did not reach ACTIVE before its bounded timeout")
        # The public route's actual response iterator is already being consumed;
        # keep polling only the native readiness fields while it waits for data.
        time.sleep(min(0.1, remaining))


class _MediaReadDiagnosticError(RuntimeError):
    """A sanitized media-reader failure that can cross the follower process boundary."""

    def __init__(self, diagnostic: dict) -> None:
        self.diagnostic = diagnostic
        super().__init__(json.dumps(diagnostic, sort_keys=True))


def _native_media_reader_diagnostic_context(
    redis_client, proxy_server, worker_id, expected_owner, *, metadata_key,
    redis_keys, metadata_field, channel_state, binding, capability_binding_current_fn,
    load_applied_state_fn, configuration_generation_fn, source_server=None,
):
    """Return only safe native state flags and synthetic source counts."""
    owner = None
    state = None
    try:
        owner = _redis_text(redis_client.get(redis_keys.channel_owner(worker_id)))
        state = _redis_text(redis_client.hget(metadata_key, metadata_field.STATE))
    except Exception:
        pass
    manager = getattr(proxy_server, "stream_managers", {}).get(worker_id)
    client_manager = getattr(proxy_server, "client_managers", {}).get(worker_id)
    try:
        capability_current = bool(
            binding is not None and capability_binding_current_fn(redis_client, binding)
        )
    except Exception:
        capability_current = False
    control_current = False
    try:
        active, control = load_applied_state_fn()
        control_current = bool(
            binding is not None
            and active is not None
            and not control.paused
            and int(control.generation) == int(binding.get("control_generation", -1))
            and str(configuration_generation_fn(active)) == str(binding.get("config_generation"))
        )
    except Exception:
        pass
    result = {
        "owner_present": owner is not None,
        "owner_matches": owner == expected_owner,
        "manager_present": manager is not None,
        "manager_running": bool(getattr(manager, "running", False)),
        "client_manager_present": client_manager is not None,
        "metadata_active": state == _constant_text(channel_state.ACTIVE),
        "metadata_stopping": state == _constant_text(getattr(channel_state, "STOPPING", "__missing__")),
        "metadata_error": state == _constant_text(getattr(channel_state, "ERROR", "__missing__")),
        "capability_binding_current": capability_current,
        "control_binding_current": control_current,
    }
    if source_server is not None:
        try:
            counts, active = source_server.snapshot()
            result["synthetic_source_requests"] = {
                name: int(counts.get(name, 0))
                for name in ("source-a.ts", "source-b.ts", "source-c.ts")
            }
            result["synthetic_source_connections"] = {
                name: int(active.get(name, 0))
                for name in ("source-a.ts", "source-b.ts", "source-c.ts")
            }
        except Exception:
            result["synthetic_source_status_available"] = False
    return result


def _read_stream_iterator(
    iterator, *, minimum_bytes: int, timeout: float, close=None, accept_chunk=None,
    diagnostic_label: str = "media_reader",
    diagnostic_context=None,
    diagnostic_fields=None,
) -> bytes:
    chunks = []
    total = 0
    accepted_chunks = 0
    discarded_bytes = 0
    discarded_chunks = 0
    termination = None

    def consume() -> None:
        nonlocal total, accepted_chunks, discarded_bytes, discarded_chunks, termination
        try:
            while total < minimum_bytes:
                chunk = next(iterator)
                if accept_chunk is not None and not accept_chunk():
                    discarded_chunks += 1
                    discarded_bytes += len(chunk)
                    continue
                media_chunk = bytes(chunk)
                chunks.append(media_chunk)
                accepted_chunks += 1
                total += len(media_chunk)
        except StopIteration:
            termination = "stop_iteration"
        except Exception as exc:  # Keep exception/URL data out of CI logs.
            termination = type(exc).__name__

    reader = threading.Thread(target=consume, name="catchuparr-media-reader", daemon=True)
    reader.start()
    reader.join(timeout)

    def failure(kind: str) -> _MediaReadDiagnosticError:
        diagnostic = {
            "callsite": diagnostic_label,
            "accepted_bytes": total,
            "accepted_chunks": accepted_chunks,
            "discarded_bytes": discarded_bytes,
            "discarded_chunks": discarded_chunks,
            "minimum_bytes": int(minimum_bytes),
            "termination": kind,
        }
        for callback, key in ((diagnostic_context, "native"), (diagnostic_fields, None)):
            if callback is None:
                continue
            try:
                fields = callback()
                if isinstance(fields, dict):
                    if key is None:
                        diagnostic.update(fields)
                    else:
                        diagnostic[key] = fields
            except Exception:
                if key is not None:
                    diagnostic[key] = {"available": False}
        return _MediaReadDiagnosticError(diagnostic)

    if reader.is_alive():
        error = failure("timeout")
        if close is not None:
            try:
                close()
            except Exception:
                pass
        reader.join(3)
        raise error
    if termination is not None:
        raise failure(termination)
    media = b"".join(chunks)
    if len(media) < minimum_bytes:
        raise failure("stop_iteration")
    return media


def _read_stream_response(
    response, *, minimum_bytes: int, timeout: float, diagnostic_label="media_reader",
    diagnostic_context=None,
) -> bytes:
    return _read_stream_iterator(
        iter(response.streaming_content), minimum_bytes=minimum_bytes, timeout=timeout,
        close=response.close, diagnostic_label=diagnostic_label,
        diagnostic_context=diagnostic_context,
    )


def _verify_media_identity(ffprobe: str, media: bytes) -> None:
    _require(media[:1] == b"\x47", "Recorder response is not MPEG-TS")
    _require(
        len(media) >= 188 * 100
        and all(media[offset] == 0x47 for offset in range(0, len(media) - 187, 188)),
        "Recorder response contains no useful packet-aligned TS media",
    )
    try:
        result = subprocess.run(
            [ffprobe, "-v", "error", "-count_frames", "-f", "mpegts", "-show_programs", "-show_streams", "-of", "json", "pipe:0"],
            input=media, check=True, capture_output=True, timeout=15,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Synthetic recorder media inspection exceeded its timeout") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError("Synthetic recorder response was not decodable media") from exc
    details = json.loads(result.stdout.decode("utf-8"))
    programs = details.get("programs", [])
    _require(
        any(program.get("tags", {}).get("service_name") == "Synthetic Source B" for program in programs),
        "Recorder route returned media from a source other than the included B source",
    )
    stream_types = {item.get("codec_type") for item in details.get("streams", [])}
    _require(stream_types >= {"video", "audio"}, "Recorder route did not expose audio and video streams")
    frame_counts = {
        item.get("codec_type"): int(item.get("nb_read_frames", "0") or 0)
        for item in details.get("streams", [])
    }
    _require(
        frame_counts.get("video", 0) > 0 and frame_counts.get("audio", 0) > 0,
        "Recorder route returned no decoded audio/video frames",
    )

def _run_fresh_process_follower(
    attempt,
    *,
    channel_uuid: str,
    stream_id: str,
    account_id: str,
    profile_id: int,
    expected_profile_count: int,
    ffprobe: str,
    native_owner: str,
    parent_client_count: int,
    diagnostic_context=None,
) -> None:
    """Attach from a fresh Django process using the same private capability."""
    bootstrap = (
        "import os,runpy,sys; from pathlib import Path; "
        "sys.path.insert(0, '/data/plugins'); "
        "os.environ['DJANGO_SECRET_KEY']=Path('/data/jwt').read_text().strip(); "
        "sys.argv=['manage.py','shell']; "
        "runpy.run_path('/app/manage.py',run_name='__main__')"
    )
    route_path = f"/catchuparr/recorder/{channel_uuid}"
    child_script = f"""
import os
import json
import sys
sys.path.insert(0, "/data/plugins")
sys.path.insert(0, "/tmp")
from django.test import Client
from apps.m3u.connection_pool import profile_connections_key
from apps.proxy.live_proxy.constants import ChannelMetadataField, ChannelState
from apps.proxy.live_proxy.redis_keys import RedisKeys
from apps.proxy.live_proxy.server import ProxyServer
from core.utils import RedisClient
from catchuparr import runtime
from catchuparr.adapters.recorder_proxy import read_worker_record
from catchuparr.configuration import load_active_configuration, load_applied_state
from catchuparr.recorder_proxy import (
    capability_binding_current,
    configuration_generation,
    verify_recorder_capability,
)
from aio_recorder_media import (
    _MediaReadDiagnosticError, _constant_text, _native_media_reader_diagnostic_context,
    _read_stream_response, _redis_text, _verify_media_identity,
)

def follower_require(condition):
    if not condition:
        raise RuntimeError("synthetic follower assertion failed")

runtime.bootstrap()
token = {attempt.capability!r}
worker_id = {str(attempt.worker_id)!r}
channel_uuid = {channel_uuid!r}
stream_id = {stream_id!r}
account_id = {account_id!r}
profile_id = {int(profile_id)!r}
parent_pid = {os.getpid()!r}
follower_require(os.getpid() != parent_pid)
redis_client = RedisClient.get_client()
proxy_server = ProxyServer.get_instance()
expected_owner = {native_owner!r}
follower_require(_redis_text(proxy_server.worker_id) != expected_owner)
follower_require(_redis_text(redis_client.get(RedisKeys.channel_owner(worker_id))) == expected_owner)
follower_require(worker_id not in proxy_server.stream_managers)
metadata_key = RedisKeys.channel_metadata(worker_id)
clients_key = RedisKeys.clients(worker_id)
active_state = _constant_text(ChannelState.ACTIVE)
follower_require(
    _redis_text(redis_client.hget(metadata_key, ChannelMetadataField.STATE)) == active_state
)
follower_require(int(redis_client.scard(clients_key) or 0) == {parent_client_count!r})
binding = verify_recorder_capability(redis_client, token)
follower_require(binding is not None and capability_binding_current(redis_client, binding))
follower_require(binding.get("worker_id") == worker_id)
follower_require(binding.get("channel_uuid") == channel_uuid)
follower_require(binding.get("stream_id") == stream_id)
follower_require(binding.get("account_id") == account_id)
follower_require(binding.get("config_generation") == {str(attempt.config_generation)!r})
active = load_active_configuration()
follower_require(active is not None)
follower_require(configuration_generation(active) == binding.get("config_generation"))
record_before = read_worker_record(redis_client, worker_id)
follower_require(record_before is not None)
follower_require(record_before.get("worker_id") == worker_id)
follower_require(record_before.get("state") == "active")
follower_require(record_before.get("reservation_state") == "reserved")
follower_require(record_before.get("stream_id") == stream_id)
follower_require(record_before.get("account_id") == account_id)
follower_require(record_before.get("profile_id") == str(profile_id))
follower_require(record_before.get("config_generation") == binding.get("config_generation"))
profile_key = profile_connections_key(profile_id)
follower_require(int(redis_client.get(profile_key) or 0) == {expected_profile_count!r})

response = None
try:
    client = Client(raise_request_exception=False)
    response = client.get(
        {route_path!r},
        HTTP_HOST="localhost",
        HTTP_USER_AGENT="Synthetic recorder follower",
        HTTP_X_CATCHUPARR_RECORDER=token,
    )
    follower_require(response.status_code == 200)
    follower_require(response.get("Content-Type", "").split(";", 1)[0] == "video/mp2t")
    follower_require(not response.get("Location"))
    follower_require(getattr(response, "streaming", False))
    follower_require(worker_id not in proxy_server.stream_managers)
    try:
        media = _read_stream_response(
            response, minimum_bytes=188 * 512, timeout=20,
            diagnostic_label="follower_initial",
            diagnostic_context=lambda: _native_media_reader_diagnostic_context(
                redis_client, proxy_server, worker_id, expected_owner,
                redis_keys=RedisKeys, metadata_key=metadata_key,
                metadata_field=ChannelMetadataField, channel_state=ChannelState,
                binding=binding,
                capability_binding_current_fn=capability_binding_current,
                load_applied_state_fn=load_applied_state,
                configuration_generation_fn=configuration_generation,
            ),
        )
    except _MediaReadDiagnosticError as exc:
        print("CATCHUPARR_RECORDER_FOLLOWER_MEDIA_DIAGNOSTIC " + json.dumps(
            exc.diagnostic, sort_keys=True,
        ))
        raise
    _verify_media_identity({ffprobe!r}, media)
    client_manager = proxy_server.client_managers.get(worker_id)
    follower_require(client_manager is not None and client_manager.get_client_count() == 1)
    follower_require(_redis_text(redis_client.get(RedisKeys.channel_owner(worker_id))) == expected_owner)
    follower_require(int(redis_client.scard(clients_key) or 0) == {parent_client_count + 1!r})
finally:
    if response is not None:
        response.close()

follower_require(worker_id not in proxy_server.stream_managers)
follower_require(client_manager is not None and client_manager.get_client_count() == 0)
follower_require(_redis_text(redis_client.get(RedisKeys.channel_owner(worker_id))) == expected_owner)
follower_require(
    _redis_text(redis_client.hget(metadata_key, ChannelMetadataField.STATE)) == active_state
)
follower_require(int(redis_client.scard(clients_key) or 0) == {parent_client_count!r})
follower_require(capability_binding_current(redis_client, binding))
record_after = read_worker_record(redis_client, worker_id)
follower_require(record_after is not None)
follower_require(record_after.get("worker_id") == worker_id)
follower_require(record_after.get("state") == "active")
follower_require(record_after.get("reservation_state") == "reserved")
follower_require(record_after.get("reservation_id") == record_before.get("reservation_id"))
follower_require(record_after.get("lease_value") == binding.get("lease_value"))
follower_require(record_after.get("lease_fence") == binding.get("lease_fence"))
follower_require(record_after.get("config_generation") == binding.get("config_generation"))
follower_require(int(redis_client.get(profile_key) or 0) == {expected_profile_count!r})
print("CATCHUPARR_RECORDER_FOLLOWER_OK")
"""
    try:
        result = subprocess.run(
            [sys.executable, "-c", bootstrap],
            input=child_script.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            cwd="/app",
            timeout=45,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("Fresh-process recorder follower exceeded its bounded timeout") from None
    diagnostic_line = next(
        (line for line in result.stdout.splitlines()
         if line.startswith(b"CATCHUPARR_RECORDER_FOLLOWER_MEDIA_DIAGNOSTIC ")),
        None,
    )
    if diagnostic_line is not None:
        try:
            child_diagnostic = json.loads(
                diagnostic_line.split(b" ", 1)[1].decode("utf-8")
            )
        except (UnicodeDecodeError, json.JSONDecodeError):
            child_diagnostic = {
                "callsite": "follower_initial",
                "termination": "diagnostic_unavailable",
            }
        if diagnostic_context is not None:
            try:
                fields = diagnostic_context()
                if isinstance(fields, dict):
                    child_diagnostic["parent_synthetic_source"] = fields
            except Exception:
                child_diagnostic["parent_synthetic_source"] = {"available": False}
        raise _MediaReadDiagnosticError(child_diagnostic)
    _require(
        result.returncode == 0
        and b"CATCHUPARR_RECORDER_FOLLOWER_OK" in result.stdout.splitlines(),
        "Fresh-process recorder follower did not attach and release cleanly",
    )

def probe_actual_recorder_media(root: Path) -> None:
    """Exercise the guarded private route against real native Dispatcharr APIs."""
    from apps.channels.models import (
        Channel,
        ChannelProfile,
        ChannelProfileMembership,
        ChannelStream,
        Stream,
    )
    from apps.m3u.connection_pool import (
        profile_connections_key,
        profile_credential_release_key,
    )
    from apps.m3u.models import M3UAccount
    from core.models import CoreSettings, StreamProfile
    from core.utils import RedisClient
    from django.test import Client
    from django.urls import resolve

    from catchuparr.adapters.recorder_proxy import (
        _release_worker_reservation,
        read_worker_record,
        worker_id_key,
    )
    from catchuparr.configuration import (
        active_settings_path,
        apply_configuration,
        load_active_configuration,
        load_applied_state,
    )
    from catchuparr.engine.leases import RedisRecorderLease
    from catchuparr.engine.store import ArchiveStore
    from catchuparr.recorder_control import control_deny_path, control_state_path
    from catchuparr.recorder_proxy import (
        capability_binding_current,
        configuration_generation,
        ranked_source_candidates,
        stop_recorder_attempt,
        verify_recorder_capability,
    )

    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")
    _require(ffmpeg is not None and ffprobe is not None, "AIO media probe requires ffmpeg and ffprobe")
    root = Path(root)
    fixture_root = root / "synthetic-recorder-sources"
    fixture_root.mkdir(parents=True, exist_ok=True)
    source_server = None
    active_path = active_settings_path()
    active_existed = active_path.exists()
    active_bytes = active_path.read_bytes() if active_existed else None
    active_lock_path = active_path.with_suffix(active_path.suffix + ".lock")
    active_lock_existed = active_lock_path.exists()
    active_lock_bytes = active_lock_path.read_bytes() if active_lock_existed else None
    reset_marker_path = active_path.with_name(".catchuparr-configuration-reset-required")
    reset_marker_existed = reset_marker_path.exists()
    reset_marker_bytes = reset_marker_path.read_bytes() if reset_marker_existed else None
    control_path = control_state_path(active_path)
    control_existed = control_path.exists()
    control_bytes = control_path.read_bytes() if control_existed else None
    control_deny = control_deny_path(active_path)
    control_deny_existed = control_deny.exists()
    control_deny_bytes = control_deny.read_bytes() if control_deny_existed else None
    saved_default_profile = None
    default_profile_saved = False
    redis_client = None
    lease = None
    response = None
    managed_attempt = None
    attempts = []
    created_accounts = []
    created_streams = []
    created_profiles = []
    created_channel_profiles = []
    channel = None
    original_assignments = None
    profile_baselines = {}
    media_baselines = {}
    cleanup_errors = []
    native_yield_tracker = None

    try:
        payload_a = _make_transport_stream(
            ffmpeg, ffprobe, fixture_root / "source-a.ts",
            color="red", frequency=440, service_name="Synthetic Source A",
        )
        payload_b = _make_transport_stream(
            ffmpeg, ffprobe, fixture_root / "source-b.ts",
            color="blue", frequency=880, service_name="Synthetic Source B",
        )
        payload_c = _make_transport_stream(
            ffmpeg, ffprobe, fixture_root / "source-c.ts",
            color="green", frequency=660, service_name="Synthetic Source C",
        )
        source_server = _SyntheticSourceServer({
            "source-a.ts": payload_a,
            "source-b.ts": payload_b,
            "source-c.ts": payload_c,
        })
        source_server.thread.start()

        account_a = M3UAccount.objects.create(
            name="Synthetic media source A", is_active=True, max_streams=1,
        )
        account_b = M3UAccount.objects.create(
            name="Synthetic media source B", is_active=True, max_streams=1,
        )
        account_c = M3UAccount.objects.create(
            name="Synthetic media source C", is_active=True, max_streams=1,
        )
        created_accounts.extend((account_a, account_b, account_c))
        source_a = Stream.objects.create(
            name="Synthetic media A", url=f"{source_server.base_url}/source-a.ts", m3u_account=account_a,
        )
        source_b = Stream.objects.create(
            name="Synthetic media B", url=f"{source_server.base_url}/source-b.ts", m3u_account=account_b,
        )
        source_c = Stream.objects.create(
            name="Unassigned synthetic media C", url=f"{source_server.base_url}/source-c.ts",
            m3u_account=account_c,
        )
        created_streams.extend((source_a, source_b, source_c))
        channel = Channel.objects.create(
            name="Synthetic recorder media channel", channel_number=99, user_level=0,
        )
        profile_name = "Synthetic recorder media profile"
        channel_profile = ChannelProfile.objects.create(name=profile_name)
        created_channel_profiles.append(channel_profile)
        ChannelProfileMembership.objects.filter(channel_profile=channel_profile).update(
            enabled=False
        )
        ChannelProfileMembership.objects.update_or_create(
            channel_profile=channel_profile,
            channel=channel,
            defaults={"enabled": True},
        )
        ChannelStream.objects.create(channel=channel, stream=source_a, order=0)
        ChannelStream.objects.create(channel=channel, stream=source_b, order=1)
        original_assignments = list(
            ChannelStream.objects.filter(channel=channel)
            .order_by("order", "id").values_list("stream_id", "order")
        )

        profiles = []
        for account in created_accounts:
            profile = account.profiles.filter(is_default=True).first()
            _require(profile is not None, "Native M3U account default profile was not created")
            profile.max_streams = 1
            profile.is_active = True
            profile.save(update_fields=("max_streams", "is_active"))
            profiles.append(profile)
        profile_a, profile_b, profile_c = profiles
        created_profiles.extend(profiles)

        redirect_profile = StreamProfile.objects.filter(name__iexact="Redirect").first()
        _require(redirect_profile is not None, "Native Redirect StreamProfile is unavailable")
        saved_default_profile = _stream_default_profile_id(CoreSettings)
        default_profile_saved = True
        _set_stream_default_profile(CoreSettings, redirect_profile.id)
        _require(
            str(_stream_default_profile_id(CoreSettings)) == str(redirect_profile.id),
            "Native default StreamProfile was not set to Redirect",
        )

        settings = {
            "filter_config": (
                "version: 1\n"
                f"profile: {profile_name}\n"
                "rules:\n"
                "  - channels: {profile: all}\n"
                "    include: [Synthetic media source B]\n"
            ),
            "archive_root": str(root),
            "retention_hours": 1,
            "max_storage_gib": 1,
        }
        apply_configuration(settings, active_path=active_path)
        active = load_active_configuration(active_path)
        _require(active is not None, "Applied recorder media configuration was not readable")
        candidates = ranked_source_candidates(str(channel.uuid), active)
        _require(
            candidates is not None and [str(item.get("id")) for item in candidates] == [str(source_b.id)],
            "Applied include-only rule did not leave only source B",
        )
        redis_client = RedisClient.get_client()
        redis_client.ping()
        store = ArchiveStore(root)
        lease = RedisRecorderLease(
            redis_client, str(channel.uuid), ttl_seconds=120, archive_store=store,
        )
        _require(lease.acquire() is not None, "Synthetic recorder lease could not be acquired")
        profile_baselines = {
            int(profile.id): _profile_count(redis_client, profile.id, profile_connections_key)
            for profile in created_profiles
        }
        _require(
            all(value == 0 for key, value in profile_baselines.items() if isinstance(key, int)),
            "Synthetic profile counters were not empty",
        )
        for profile in created_profiles:
            marker_key = profile_credential_release_key(profile.id)
            profile_baselines[f"marker:{profile.id}"] = _key_dump(redis_client, marker_key)

        from apps.proxy.live_proxy.redis_keys import RedisKeys

        native_keys = {
            f"channel_stream:{source_a.id}",
            f"channel_stream:{source_b.id}",
            f"channel_stream:{source_c.id}",
            f"channel_stream:{channel.uuid}",
            f"channel_stream:{channel.id}",
            f"stream_profile:{source_a.id}",
            f"stream_profile:{source_b.id}",
            f"stream_profile:{channel.id}",
            RedisKeys.channel_metadata(str(channel.uuid)),
            RedisKeys.channel_owner(str(channel.uuid)),
        }
        native_snapshot = {key: _key_dump(redis_client, key) for key in native_keys}
        media_baselines = source_server.snapshot()[0]
        route_path = f"/catchuparr/recorder/{channel.uuid}"
        client = Client(raise_request_exception=False)

        def issue(candidate):
            attempt = _issue_media_probe_attempt(
                redis_client, lease,
                channel_uuid=str(channel.uuid),
                candidate=candidate,
                active_path=active_path,
                internal_base_url="http://127.0.0.1",
            )
            attempts.append(attempt)
            return attempt

        attempt_b = issue({"id": str(source_b.id), "account_id": str(account_b.id)})
        no_header = client.get(route_path, HTTP_HOST="localhost")
        _require(no_header.status_code == 401, "Private recorder route did not reject a missing capability")
        no_header.close()
        forged = attempt_b.capability[:-1] + ("A" if attempt_b.capability[-1] != "A" else "B")
        forged_response = client.get(
            route_path, HTTP_HOST="localhost", HTTP_X_CATCHUPARR_RECORDER=forged,
        )
        _require(forged_response.status_code == 403, "Private recorder route did not reject a forged capability")
        forged_response.close()

        public_path = f"/proxy/ts/stream/{attempt_b.worker_id}"
        match = resolve(public_path)
        _require(match.url_name == "stream", "Native public stream route could not be resolved")
        _require(
            bool(getattr(match.func, "_catchuparr_managed_id_guard", False)),
            "Native public stream route lacks the managed-worker rejection guard",
        )
        public_response = client.get(public_path, HTTP_HOST="localhost")
        _require(public_response.status_code == 404, "Native public route exposed a managed recorder ID")
        public_response.close()

        attempt_a = issue({"id": str(source_a.id), "account_id": str(account_a.id)})
        forbidden_response = client.get(
            route_path, HTTP_HOST="localhost",
            HTTP_X_CATCHUPARR_RECORDER=attempt_a.capability,
        )
        _require(forbidden_response.status_code == 403, "Include-only route accepted forbidden source A")
        forbidden_response.close()
        attempt_c = issue({"id": str(source_c.id), "account_id": str(account_c.id)})
        unassigned_response = client.get(
            route_path, HTTP_HOST="localhost",
            HTTP_X_CATCHUPARR_RECORDER=attempt_c.capability,
        )
        _require(unassigned_response.status_code == 403, "Recorder route accepted an unassigned source")
        unassigned_response.close()

        _require(
            {
                int(profile.id): _profile_count(redis_client, profile.id, profile_connections_key)
                for profile in created_profiles
            } == {key: value for key, value in profile_baselines.items() if isinstance(key, int)},
            "Denied recorder requests changed a provider profile counter",
        )
        _require(
            {profile.id: _key_dump(redis_client, profile_credential_release_key(profile.id))
             for profile in created_profiles}
            == {profile.id: profile_baselines[f"marker:{profile.id}"] for profile in created_profiles},
            "Denied recorder requests changed a provider credential marker",
        )
        _require(source_server.snapshot()[0] == media_baselines, "A denied source unexpectedly opened")

        response = client.get(
            route_path, HTTP_HOST="localhost", HTTP_USER_AGENT="Synthetic recorder integration",
            HTTP_X_CATCHUPARR_RECORDER=attempt_b.capability,
        )
        managed_attempt = attempt_b
        _require(response.status_code == 200, "Authorized private recorder route did not return HTTP 200")
        _require(response.get("Content-Type", "").split(";", 1)[0] == "video/mp2t",
                 "Authorized recorder response is not MPEG-TS")
        _require(not response.get("Location"), "Redirect profile leaked a provider redirect")
        _require(getattr(response, "streaming", False), "Recorder response is not a live stream")
        from apps.proxy.live_proxy.constants import ChannelMetadataField, ChannelState
        from apps.proxy.live_proxy.redis_keys import RedisKeys
        from apps.proxy.live_proxy.server import ProxyServer

        native_server = ProxyServer.get_instance()
        expected_native_owner = _redis_text(native_server.worker_id)
        native_buffer = native_server.get_buffer(attempt_b.worker_id, profile=None)
        native_yield_tracker = _NativeBufferYieldTracker(native_buffer)
        parent_iterator = iter(
            native_yield_tracker.observe(iter(response.streaming_content))
        )
        media = _read_stream_iterator(
            parent_iterator, minimum_bytes=188 * 512, timeout=20, close=response.close,
            diagnostic_label="parent_initial",
            diagnostic_context=lambda: _native_media_reader_diagnostic_context(
                redis_client, native_server, attempt_b.worker_id, expected_native_owner,
                redis_keys=RedisKeys,
                metadata_key=RedisKeys.channel_metadata(attempt_b.worker_id),
                metadata_field=ChannelMetadataField, channel_state=ChannelState,
                binding=verify_recorder_capability(redis_client, attempt_b.capability),
                capability_binding_current_fn=capability_binding_current,
                load_applied_state_fn=load_applied_state, source_server=source_server,
                configuration_generation_fn=configuration_generation,
            ),
        )
        _verify_media_identity(ffprobe, media)
        counts, _ = source_server.snapshot()
        _require(counts["source-a.ts"] == 0, "Forbidden source A was opened by the recorder")
        _require(counts["source-b.ts"] >= 1, "Included source B was not opened")
        _require(counts["source-c.ts"] == 0, "Unassigned source C was opened by the recorder")
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key) == profile_baselines[profile_b.id] + 1,
            "Authorized recorder did not reserve exactly one provider profile slot",
        )
        active_record = read_worker_record(redis_client, attempt_b.worker_id)
        _require(
            active_record is not None
            and active_record.get("profile_id") == str(profile_b.id)
            and active_record.get("stream_id") == str(source_b.id),
            "Native recorder did not reserve the expected source B profile",
        )
        _require(
            {key: _key_dump(redis_client, key) for key in native_keys} == native_snapshot,
            "Recorder source changed an unscoped native live Redis key",
        )
        _require(
            list(ChannelStream.objects.filter(channel=channel).order_by("order", "id")
                 .values_list("stream_id", "order")) == original_assignments,
            "Recorder source changed native ChannelStream assignments",
        )

        native_owner, native_manager, native_client_manager = _wait_for_native_active(
            redis_client,
            native_server,
            attempt_b.worker_id,
            redis_keys=RedisKeys,
            metadata_field=ChannelMetadataField,
            channel_state=ChannelState,
            timeout=45,
        )
        active_state = _constant_text(ChannelState.ACTIVE)
        parent_client_count = int(redis_client.scard(RedisKeys.clients(attempt_b.worker_id)) or 0)
        _require(parent_client_count == 1, "Native Redis client count is not one for the parent")
        _require(
            native_client_manager.get_client_count() == 1,
            "Native local client count is not one for the parent",
        )
        _require(native_manager.running, "Native parent stream manager is not running")
        _require(
            _redis_text(
                redis_client.hget(
                    RedisKeys.channel_metadata(attempt_b.worker_id),
                    ChannelMetadataField.STATE,
                )
            ) == active_state,
            "Native parent metadata is not ACTIVE",
        )
        counts_before_follower, active_before_follower = source_server.snapshot()
        record_before_follower = read_worker_record(redis_client, attempt_b.worker_id)
        _require(record_before_follower is not None, "Recorder worker disappeared before follower attach")
        reservation_count_before_follower = _profile_count(
            redis_client, profile_b.id, profile_connections_key,
        )
        _require(
            reservation_count_before_follower == profile_baselines[profile_b.id] + 1,
            "Recorder provider slot was not reserved before follower attach",
        )
        _require(
            active_before_follower["source-b.ts"] >= 1,
            "Synthetic B source was not active before follower attach",
        )
        _run_fresh_process_follower(
            attempt_b,
            channel_uuid=str(channel.uuid),
            stream_id=str(source_b.id),
            account_id=str(account_b.id),
            profile_id=int(profile_b.id),
            expected_profile_count=reservation_count_before_follower,
            ffprobe=ffprobe,
            native_owner=native_owner,
            parent_client_count=parent_client_count,
            diagnostic_context=lambda: _native_media_reader_diagnostic_context(
                redis_client, native_server, attempt_b.worker_id, native_owner,
                redis_keys=RedisKeys,
                metadata_key=RedisKeys.channel_metadata(attempt_b.worker_id),
                metadata_field=ChannelMetadataField, channel_state=ChannelState,
                binding=verify_recorder_capability(redis_client, attempt_b.capability),
                capability_binding_current_fn=capability_binding_current,
                load_applied_state_fn=load_applied_state, source_server=source_server,
                configuration_generation_fn=configuration_generation,
            ),
        )
        counts_after_follower, active_after_follower = source_server.snapshot()
        _require(
            counts_after_follower == counts_before_follower,
            "Fresh-process follower opened another synthetic provider connection",
        )
        _require(
            active_after_follower["source-b.ts"] >= 1,
            "Closing the follower stopped the parent synthetic B source",
        )
        record_after_follower = read_worker_record(redis_client, attempt_b.worker_id)
        _require(
            record_after_follower is not None
            and record_after_follower.get("worker_id") == attempt_b.worker_id
            and record_after_follower.get("state") == "active"
            and record_after_follower.get("reservation_state") == "reserved"
            and record_after_follower.get("reservation_id")
            == record_before_follower.get("reservation_id")
            and record_after_follower.get("lease_value")
            == record_before_follower.get("lease_value")
            and record_after_follower.get("lease_fence")
            == record_before_follower.get("lease_fence")
            and record_after_follower.get("config_generation")
            == record_before_follower.get("config_generation")
            and record_after_follower.get("stream_id") == str(source_b.id)
            and record_after_follower.get("profile_id") == str(profile_b.id),
            "Fresh-process follower changed the parent recorder worker or reservation",
        )
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key)
            == reservation_count_before_follower,
            "Fresh-process follower changed provider profile capacity",
        )
        _require(
            _redis_text(redis_client.get(RedisKeys.channel_owner(attempt_b.worker_id)))
            == native_owner,
            "Follower disconnect changed the native owner",
        )
        _require(
            _redis_text(
                redis_client.hget(
                    RedisKeys.channel_metadata(attempt_b.worker_id),
                    ChannelMetadataField.STATE,
                )
            ) == active_state,
            "Follower disconnect changed native ACTIVE metadata",
        )
        _require(
            native_server.stream_managers.get(attempt_b.worker_id) is native_manager,
            "Follower disconnect replaced the native parent stream manager",
        )
        _require(native_manager.running, "Follower disconnect stopped the native parent manager")
        _require(
            int(redis_client.scard(RedisKeys.clients(attempt_b.worker_id)) or 0)
            == parent_client_count,
            "Follower disconnect did not return native global clients to the parent baseline",
        )
        _require(
            native_client_manager.get_client_count() == parent_client_count,
            "Follower disconnect changed the native parent local client count",
        )

        publication_floor = int(
            native_buffer.redis_client.get(native_buffer.buffer_index_key) or 0
        )
        _require(publication_floor > 0, "Native recorder parent did not publish a buffer chunk")
        continued_media = _read_stream_iterator(
            parent_iterator,
            minimum_bytes=188 * 512,
            timeout=20,
            close=response.close,
            diagnostic_label="parent_after_follower",
            accept_chunk=lambda: (
                native_yield_tracker.last_yielded_index is not None
                and native_yield_tracker.last_yielded_index > publication_floor
            ),
            diagnostic_context=lambda: _native_media_reader_diagnostic_context(
                redis_client, native_server, attempt_b.worker_id, native_owner,
                redis_keys=RedisKeys,
                metadata_key=RedisKeys.channel_metadata(attempt_b.worker_id),
                metadata_field=ChannelMetadataField, channel_state=ChannelState,
                binding=verify_recorder_capability(redis_client, attempt_b.capability),
                capability_binding_current_fn=capability_binding_current,
                load_applied_state_fn=load_applied_state, source_server=source_server,
                configuration_generation_fn=configuration_generation,
            ),
            diagnostic_fields=lambda: {
                "publication_floor": int(publication_floor),
                "current_head": int(native_buffer.redis_client.get(native_buffer.buffer_index_key) or 0),
                "last_yield_index": int(native_yield_tracker.last_yielded_index or 0),
            },
        )
        published_head = int(
            native_buffer.redis_client.get(native_buffer.buffer_index_key) or 0
        )
        _require(
            published_head > publication_floor
            and native_yield_tracker.last_yielded_index is not None
            and native_yield_tracker.last_yielded_index > publication_floor,
            "Parent did not publish and consume a native buffer chunk newer than follower close",
        )
        _verify_media_identity(ffprobe, continued_media)
        _require(
            continued_media != media,
            "Parent recorder response did not advance to different useful B media after follower close",
        )
        _require(
            source_server.snapshot()[0] == counts_before_follower,
            "Parent recorder continuation opened another synthetic provider connection",
        )
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key)
            == reservation_count_before_follower,
            "Parent recorder continuation changed provider profile capacity",
        )
        _require(
            source_server.snapshot()[1]["source-b.ts"] >= 1,
            "Parent recorder no longer owns an active synthetic B source after follower close",
        )

        response.close()
        native_yield_tracker.close()
        native_yield_tracker = None
        response = None
        _require(stop_recorder_attempt(redis_client, attempt_b, lease), "Managed recorder worker did not stop cleanly")
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key) == profile_baselines[profile_b.id],
            "Recorder teardown did not restore provider profile capacity",
        )
        _require(
            not _release_worker_reservation(redis_client, attempt_b.worker_id),
            "Duplicate cleanup claimed a second recorder reservation",
        )
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key) == profile_baselines[profile_b.id],
            "Duplicate cleanup decremented provider profile capacity twice",
        )
        released_record = read_worker_record(redis_client, attempt_b.worker_id)
        _require(released_record is not None, "Recorder worker ledger disappeared before cleanup verification")
        from catchuparr.adapters.recorder_proxy import reservation_credential_marker_key

        _require(
            not redis_client.exists(
                reservation_credential_marker_key(released_record.get("reservation_id", ""))
            ),
            "Recorder cleanup left its private credential reservation marker",
        )
        cleanup_deadline = time.monotonic() + 5
        while time.monotonic() < cleanup_deadline:
            _counts, active_counts = source_server.snapshot()
            if not any(active_counts.values()):
                break
            time.sleep(0.05)
        _require(not any(source_server.snapshot()[1].values()), "Synthetic provider connection remained open after stop")
        _require(
            {key: _key_dump(redis_client, key) for key in native_keys} == native_snapshot,
            "Recorder teardown changed an unscoped native live Redis key",
        )
        managed_attempt = None

        # Keep a fresh, otherwise valid capability so this 403 tests only the
        # applied configuration generation check.
        generation_attempt = issue({"id": str(source_b.id), "account_id": str(account_b.id)})
        generation_binding = verify_recorder_capability(redis_client, generation_attempt.capability)
        _require(
            generation_binding is not None
            and capability_binding_current(redis_client, generation_binding),
            "Generation-check capability was not valid before changing configuration",
        )
        counts_before_generation_change = source_server.snapshot()[0]
        counters_before_generation_change = {
            profile.id: _profile_count(redis_client, profile.id, profile_connections_key)
            for profile in created_profiles
        }
        changed_settings = dict(settings, retention_hours=2)
        apply_configuration(changed_settings, active_path=active_path)
        stale_generation = client.get(
            route_path, HTTP_HOST="localhost",
            HTTP_X_CATCHUPARR_RECORDER=generation_attempt.capability,
        )
        _require(stale_generation.status_code == 403, "Recorder capability survived an applied config change")
        stale_generation.close()
        _require(
            source_server.snapshot()[0] == counts_before_generation_change,
            "Stale generation capability opened a synthetic provider source",
        )
        _require(
            {
                profile.id: _profile_count(redis_client, profile.id, profile_connections_key)
                for profile in created_profiles
            } == counters_before_generation_change,
            "Stale generation capability changed provider profile capacity",
        )
        apply_configuration(settings, active_path=active_path)
        restored_active, restored_control = load_applied_state(active_path)
        _require(
            restored_active is not None
            and not restored_control.paused
            and restored_control.configuration_generation
            == configuration_generation(restored_active),
            "Restored recorder configuration/control pair is not current",
        )
        _require(
            not capability_binding_current(redis_client, generation_binding),
            "Old capability became valid after a configuration restore advanced control generation",
        )
        stale_restored = client.get(
            route_path, HTTP_HOST="localhost",
            HTTP_X_CATCHUPARR_RECORDER=generation_attempt.capability,
        )
        _require(
            stale_restored.status_code == 403,
            "Old capability was accepted after restoring the prior configuration",
        )
        stale_restored.close()
        restored_attempt = issue({"id": str(source_b.id), "account_id": str(account_b.id)})
        restored_binding = verify_recorder_capability(
            redis_client, restored_attempt.capability,
        )
        _require(
            restored_attempt.control_generation == restored_control.generation
            and restored_binding is not None
            and capability_binding_current(redis_client, restored_binding),
            "Fresh restored-state capability did not bind to the current control generation",
        )
        _require(
            source_server.snapshot()[0] == counts_before_generation_change
            and {
                profile.id: _profile_count(redis_client, profile.id, profile_connections_key)
                for profile in created_profiles
            } == counters_before_generation_change,
            "Capability generation checks changed provider source or capacity state",
        )

        # Keep another otherwise-valid capability, then replace only its owner lease.
        owner_attempt = issue({"id": str(source_b.id), "account_id": str(account_b.id)})
        owner_binding = verify_recorder_capability(redis_client, owner_attempt.capability)
        _require(
            owner_binding is not None and capability_binding_current(redis_client, owner_binding),
            "Lease-check capability was not valid before replacing its owner",
        )
        counts_before_lease_change = source_server.snapshot()[0]
        counters_before_lease_change = {
            profile.id: _profile_count(redis_client, profile.id, profile_connections_key)
            for profile in created_profiles
        }
        _require(lease.release(), "Synthetic recorder lease did not release")
        lease = None
        replacement = RedisRecorderLease(redis_client, str(channel.uuid), ttl_seconds=120, archive_store=store)
        lease = replacement
        _require(replacement.acquire() is not None, "Replacement recorder lease could not be acquired")
        replayed = client.get(
            route_path, HTTP_HOST="localhost",
            HTTP_X_CATCHUPARR_RECORDER=owner_attempt.capability,
        )
        _require(replayed.status_code == 403, "Old owner capability was replayable after lease replacement")
        replayed.close()
        _require(
            source_server.snapshot()[0] == counts_before_lease_change,
            "Old owner capability opened a synthetic provider source",
        )
        _require(
            {
                profile.id: _profile_count(redis_client, profile.id, profile_connections_key)
                for profile in created_profiles
            } == counters_before_lease_change,
            "Old owner capability changed provider profile capacity",
        )
        _require(replacement.release(), "Replacement recorder lease did not release")
        lease = None

        _require(
            list(ChannelStream.objects.filter(channel=channel).order_by("order", "id")
                 .values_list("stream_id", "order")) == original_assignments,
            "Native source assignments changed during recorder cleanup",
        )
        _require(
            all(
                _profile_count(redis_client, profile.id, profile_connections_key) == profile_baselines[profile.id]
                for profile in created_profiles
            ),
            "Recorder teardown did not restore all synthetic profile counters",
        )
        _require(
            all(
                _key_dump(redis_client, profile_credential_release_key(profile.id))
                == profile_baselines[f"marker:{profile.id}"]
                for profile in created_profiles
            ),
            "Recorder teardown changed a shared profile credential marker",
        )
        print("AIO private recorder route returned identified B audio/video TS with cross-process reuse and fenced cleanup")
    finally:
        if response is not None:
            try:
                response.close()
            except Exception:
                cleanup_errors.append("response")
        if native_yield_tracker is not None:
            try:
                native_yield_tracker.close()
            except Exception:
                cleanup_errors.append("native-yield-tracker")
        if redis_client is not None:
            if managed_attempt is not None and lease is not None:
                try:
                    if not stop_recorder_attempt(redis_client, managed_attempt, lease):
                        cleanup_errors.append("managed-worker")
                except Exception:
                    cleanup_errors.append("managed-worker")
            try:
                from catchuparr.adapters.recorder_proxy import stop_managed_workers

                if not stop_managed_workers(timeout_seconds=8):
                    cleanup_errors.append("remaining-workers")
            except Exception:
                cleanup_errors.append("remaining-workers")
            for attempt in attempts:
                try:
                    attempt.revoke(redis_client)
                    record = read_worker_record(redis_client, attempt.worker_id)
                    if record and record.get("reservation_state") == "reserved":
                        _release_worker_reservation(redis_client, attempt.worker_id)
                    redis_client.delete(worker_id_key(attempt.worker_id))
                except Exception:
                    cleanup_errors.append("attempt-ledger")
            if lease is not None:
                try:
                    lease.release()
                except Exception:
                    cleanup_errors.append("lease")
            try:
                from catchuparr.adapters.recorder_proxy import managed_worker_ids

                workers_active = bool(managed_worker_ids(redis_client, active_only=True))
            except Exception:
                workers_active = True
            if workers_active:
                cleanup_errors.append("active-managed-workers")
            capacity_restored = all(
                _profile_count(redis_client, profile.id, profile_connections_key)
                == profile_baselines.get(profile.id, 0)
                for profile in created_profiles
            )
            if not capacity_restored:
                cleanup_errors.append("profile-capacity")
        if source_server is not None:
            try:
                source_server.close()
                if any(source_server.snapshot()[1].values()):
                    cleanup_errors.append("source-connections")
            except Exception:
                cleanup_errors.append("source-server")
        if default_profile_saved:
            try:
                _set_stream_default_profile(CoreSettings, saved_default_profile)
                _require(
                    str(_stream_default_profile_id(CoreSettings)) == str(saved_default_profile),
                    "Native default StreamProfile was not restored",
                )
            except Exception:
                cleanup_errors.append("default-stream-profile")
        try:
            if active_existed:
                active_path.write_bytes(active_bytes)
            else:
                active_path.unlink(missing_ok=True)
            if active_lock_existed:
                active_lock_path.write_bytes(active_lock_bytes)
            else:
                active_lock_path.unlink(missing_ok=True)
            if reset_marker_existed:
                reset_marker_path.write_bytes(reset_marker_bytes)
            else:
                reset_marker_path.unlink(missing_ok=True)
            if control_existed:
                control_path.write_bytes(control_bytes)
            else:
                control_path.unlink(missing_ok=True)
            if control_deny_existed:
                control_deny.write_bytes(control_deny_bytes)
            else:
                control_deny.unlink(missing_ok=True)
        except Exception:
            cleanup_errors.append("active-config")
        if not cleanup_errors:
            try:
                for profile in created_profiles:
                    if redis_client is not None:
                        redis_client.delete(
                            profile_connections_key(profile.id),
                            profile_credential_release_key(profile.id),
                        )
                    profile.delete()
                for stream in created_streams:
                    stream.delete()
                for account in created_accounts:
                    account.delete()
                if channel is not None:
                    channel.delete()
                for channel_profile in created_channel_profiles:
                    channel_profile.delete()
            except Exception:
                cleanup_errors.append("synthetic-database-rows")
        try:
            shutil.rmtree(fixture_root, ignore_errors=True)
        except Exception:
            cleanup_errors.append("fixture-files")
        if cleanup_errors:
            raise RuntimeError("Synthetic recorder media cleanup was incomplete")

    probe_actual_live_archive_isolation(root)


def _wait_for_indexed_tone(
    run,
    store,
    channel_uuid: str,
    ffmpeg: str,
    ffprobe: str,
    *,
    expected_hz: int,
    existing_ids: set[str],
    minimum: int,
    timeout: float,
    cooperative_sleep=None,
) -> list:
    from aio_recorder_failover import _segment_has_useful_av, _segment_tone_frequency

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        task_alive = run.thread is not None and run.thread.is_alive()
        _require_with_failure_diagnostic(
            task_alive,
            "Synthetic recorder task ended before expected media was indexed",
            lambda: _recorder_task_early_exit_diagnostic(run),
            diagnostic_label="CATCHUPARR_RECORDER_EARLY_EXIT_DIAGNOSTIC",
        )
        matched = []
        for segment in store.segments(channel_uuid):
            if segment.id in existing_ids:
                continue
            _require(
                _segment_has_useful_av(ffprobe, segment.path),
                "Indexed synthetic archive segment lacks decoded audio/video",
            )
            frequency = _segment_tone_frequency(ffmpeg, segment.path)
            _assert_tone_matches(frequency, expected_hz, "Indexed synthetic archive")
            matched.append(segment)
        if len(matched) >= minimum:
            return matched
        (cooperative_sleep or time.sleep)(0.2)
    raise RuntimeError("Synthetic recorder did not index the expected decoded source in time")


def _native_source_metadata(redis_client, metadata_key) -> dict[str, str]:
    raw = redis_client.hgetall(metadata_key)
    relevant = {}
    for key, value in raw.items():
        name = _redis_text(key).lower()
        if any(token in name for token in ("url", "source", "stream", "profile")):
            relevant[name] = _redis_text(value)
    return relevant


def _wait_for_native_client_count(
    redis_client,
    native_server,
    redis_keys,
    worker_id: str,
    expected: int,
    *,
    expected_local: int | None = None,
    timeout: float = 15,
    reject_excess: bool = False,
    return_identity: bool = False,
    verified_borrow_client_ids=None,
):
    # The Redis set is shared across workers, while ClientManager counts are
    # process-local. Borrowed readers deliberately live in a task-owned manager
    # in the HTTP worker, so the probe must be able to assert those counts
    # independently instead of pretending they describe one manager.
    if expected_local is None:
        expected_local = expected
    if (
        isinstance(expected, bool) or not isinstance(expected, int) or expected < 0
        or isinstance(expected_local, bool) or not isinstance(expected_local, int)
        or expected_local < 0
    ):
        raise ValueError("native client counts must be non-negative integers")
    deadline = time.monotonic() + timeout
    clients_key = redis_keys.clients(worker_id)
    expected_owner = _redis_text(native_server.worker_id) if return_identity else None
    ordinary_identity = None
    while time.monotonic() < deadline:
        manager = native_server.client_managers.get(worker_id)
        if return_identity:
            client_ids = {_redis_text(item) for item in redis_client.smembers(clients_key)}
            global_count = len(client_ids)
        else:
            client_ids = None
            global_count = int(redis_client.scard(clients_key) or 0)
        local_count = int(manager.get_client_count()) if manager is not None else -1
        if return_identity and verified_borrow_client_ids is not None:
            if expected != 1 or expected_local != 1:
                raise ValueError("verified borrow identity capture requires one ordinary client")
            verified_ids = {
                _redis_text(value) for value in verified_borrow_client_ids()
            }
            if local_count > 1 or len(verified_ids) > 1:
                raise RuntimeError("Native live route registered an unverified client")
            if ordinary_identity is not None and local_count != 1:
                raise RuntimeError(
                    "Native recorder ordinary client detached during identity capture"
                )
            if local_count == 1:
                ordinary_identity, missing_verified_ids = _native_ordinary_client_observation(
                    redis_client,
                    clients_key,
                    client_ids,
                    verified_ids,
                    local_clients=local_count,
                    expected_owner=expected_owner,
                    expected_identity=ordinary_identity,
                )
                if not missing_verified_ids:
                    return ordinary_identity
                # A verified borrow can register between SMEMBERS and the
                # task-local capability scan. Reconcile only after validating
                # and latching the exact ordinary client above.
                time.sleep(min(0.01, max(0, deadline - time.monotonic())))
                continue
            # The native ordinary client may be between Redis and local
            # ClientManager registration. Do not accept arbitrary memberships
            # while waiting for that one expected local client.
            ordinary_ids = client_ids - verified_ids
            _require(
                local_count == 0
                and len(client_ids) <= 2
                and len(ordinary_ids) <= 1
                and (not ordinary_ids or len(verified_ids) <= 1),
                "Native live route registered an unverified client",
            )
            time.sleep(min(0.1, max(0, deadline - time.monotonic())))
            continue
        if global_count == expected and local_count == expected_local:
            if not return_identity:
                return None
            if expected != 1:
                raise ValueError("identity capture requires exactly one expected client")
            client_id = next(iter(client_ids))
            client_key = f"{clients_key}:{client_id}"
            connected_at = _redis_text(redis_client.hget(client_key, "connected_at"))
            client_worker = _redis_text(redis_client.hget(client_key, "worker_id"))
            if connected_at and client_worker == expected_owner:
                return {
                    "client_id": client_id,
                    "connected_at": connected_at,
                    "worker_id": client_worker,
                }
        if reject_excess:
            _require(
                global_count <= expected and local_count <= expected_local,
                "Native live route registered more clients than the fixture expected",
            )
        time.sleep(0.1)
    raise RuntimeError("Native live client count did not reach the expected bounded state")


def _probe_running_recorder_controls(
    *,
    channel_uuid: str,
    settings: dict,
    active_path: Path,
    archive_store,
    source_server,
    redis_client,
    native_server,
    native_buffer,
    native_owner: str,
    native_manager,
    native_client_manager,
    metadata_key: str,
    active_state: str,
    assignment_snapshot: dict,
    source_metadata: dict,
    live_reader,
    profile_a,
    profile_b,
    profile_baselines: dict,
    native_a_marker,
    initial_b_request_baseline: int,
    ffmpeg: str,
    ffprobe: str,
    harnesses: list,
    gevent_sleep,
    initial_run,
) -> None:
    """Exercise pause, resume and schedule fences against live native media."""
    import logging
    from datetime import datetime, timedelta, timezone
    from unittest.mock import patch

    from aio_recorder_failover import (
        _assert_worker_cleanup,
        _RecorderTaskRun,
        _worker_records,
    )
    from apps.m3u.connection_pool import (
        profile_connections_key,
        profile_credential_release_key,
    )
    from apps.plugins.models import PluginConfig
    from apps.proxy.live_proxy.config_helper import ConfigHelper
    from apps.proxy.live_proxy.constants import ChannelMetadataField
    from apps.proxy.live_proxy.redis_keys import RedisKeys

    from catchuparr import runtime, tasks
    from catchuparr.configuration import load_active_configuration
    from catchuparr.recorder_control import load_recorder_control
    from catchuparr.recorder_proxy import (
        CAPABILITY_PREFIX,
        configuration_generation,
    )
    from catchuparr.schedule import schedule_from_snapshot, schedule_is_active

    def require_live_a(
        stage: str,
        expected_b_active: int,
        *,
        read_fresh_media: bool,
    ) -> None:
        observations = {}
        try:
            owner = redis_client.get(RedisKeys.channel_owner(channel_uuid))
            observations["owner_read_ok"] = True
            observations["owner_present"] = owner is not None
            observations["owner_matches"] = _redis_text(owner) == native_owner
        except Exception:
            observations.update({
                "owner_read_ok": False,
                "owner_present": False,
                "owner_matches": False,
            })

        try:
            manager = native_server.stream_managers.get(channel_uuid)
            observations["stream_manager_lookup_ok"] = True
            observations["stream_manager_present"] = manager is not None
            observations["stream_manager_identity"] = manager is native_manager
            observations["stream_manager_running"] = bool(
                getattr(manager, "running", False)
            )
        except Exception:
            observations.update({
                "stream_manager_lookup_ok": False,
                "stream_manager_present": False,
                "stream_manager_identity": False,
                "stream_manager_running": False,
            })

        try:
            client_manager = native_server.client_managers.get(channel_uuid)
            observations["client_manager_lookup_ok"] = True
            observations["client_manager_present"] = client_manager is not None
            observations["client_manager_identity"] = (
                client_manager is native_client_manager
            )
        except Exception:
            client_manager = None
            observations.update({
                "client_manager_lookup_ok": False,
                "client_manager_present": False,
                "client_manager_identity": False,
            })

        try:
            observations["local_client_count_read_ok"] = client_manager is not None
            observations["local_client_count_one"] = (
                client_manager is not None
                and int(client_manager.get_client_count()) == 1
            )
        except Exception:
            observations.update({
                "local_client_count_read_ok": False,
                "local_client_count_one": False,
            })

        try:
            global_client_count = int(
                redis_client.scard(RedisKeys.clients(channel_uuid)) or 0
            )
            observations["global_client_count_read_ok"] = True
            observations["global_client_count_one"] = global_client_count == 1
        except Exception:
            observations.update({
                "global_client_count_read_ok": False,
                "global_client_count_one": False,
            })

        try:
            state = _redis_text(
                redis_client.hget(metadata_key, ChannelMetadataField.STATE)
            )
            observations["metadata_state_read_ok"] = True
            observations["metadata_active"] = state == active_state
        except Exception:
            observations.update({
                "metadata_state_read_ok": False,
                "metadata_active": False,
            })

        flags = _native_live_a_invariant_flags(**observations)
        _require(
            all(flags.values()),
            _native_live_a_invariant_message(stage, flags),
        )
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1
            and _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id] + expected_b_active
            and bool(redis_client.exists(f"catchuparr:recorder:{channel_uuid}"))
            == bool(expected_b_active),
            "Recorder control changed native provider capacity unexpectedly",
        )
        _require(
            _key_dump(redis_client, profile_credential_release_key(profile_a.id))
            == native_a_marker,
            "Recorder control changed the native live A credential release marker",
        )
        _require(
            {key: _key_dump(redis_client, key) for key in assignment_snapshot}
            == assignment_snapshot
            and _native_source_metadata(redis_client, metadata_key) == source_metadata,
            "Recorder control changed native live A assignments or source metadata",
        )
        counts, active_counts = source_server.snapshot()
        _require(
            counts["source-a.ts"] > 0
            and active_counts["source-a.ts"] == 1
            and active_counts["source-b.ts"] == expected_b_active,
            "Recorder control interrupted live A or left the archive B source active",
        )
        if read_fresh_media:
            floor = int(native_buffer.redis_client.get(native_buffer.buffer_index_key) or 0)
            _require(floor > 0, "Native live A has no published buffer head")
            media, index = live_reader.read_after(
                floor, minimum_bytes=188 * 100, timeout=20,
            )
            _require(index > floor, "Native live A did not publish fresh media after recorder control")
            _verify_audio_video_tone(ffmpeg, ffprobe, media, 440, "Native live A during recorder control")

    def current_private_worker() -> str:
        records = [
            record for record in _worker_records(redis_client, channel_uuid)
            if record.get("state") == "active"
            and record.get("reservation_state") == "reserved"
        ]
        _require(len(records) == 1, "Expected one active private archive worker")
        worker_id = records[0].get("worker_id")
        _require(
            bool(worker_id) and worker_id != channel_uuid,
            "Archive recording did not use its dedicated worker identity",
        )
        return str(worker_id)

    def persisted_recording_enabled() -> object:
        return dict(PluginConfig.objects.get(key="catchuparr").settings or {}).get(
            "recording_enabled"
        )

    def persist_draft(draft_settings: dict) -> None:
        from django.db import transaction

        with transaction.atomic():
            row = PluginConfig.objects.select_for_update().get(key="catchuparr")
            current_settings = dict(row.settings or {})
            current_settings.update(draft_settings)
            row.settings = current_settings
            row.save(update_fields=("settings",))

    def wait_for_private_cleanup(worker_id: str, expected_b_requests: int) -> None:
        deadline = time.monotonic() + _native_teardown_readiness_timeout(
            float(ConfigHelper.channel_shutdown_delay()),
            float(ConfigHelper.cleanup_check_interval()),
            stopping_marker_ttl=90,
        )
        while time.monotonic() < deadline:
            manager = native_server.stream_managers.get(worker_id)
            client_manager = native_server.client_managers.get(worker_id)
            owner = redis_client.get(RedisKeys.channel_owner(worker_id))
            clients = int(redis_client.scard(RedisKeys.clients(worker_id)) or 0)
            active_counts = source_server.snapshot()[1]
            profile_count = _profile_count(
                redis_client, profile_b.id, profile_connections_key,
            )
            if (
                manager is None
                and worker_id not in native_server._live_stream_managers
                and client_manager is None
                and owner is None and clients == 0
                and not redis_client.exists(RedisKeys.channel_stopping(worker_id))
                and active_counts["source-b.ts"] == 0
                and source_server.snapshot()[0]["source-b.ts"] == expected_b_requests
                and profile_count == profile_baselines[profile_b.id]
            ):
                break
            gevent_sleep(0.1)
        _require(
            native_server.stream_managers.get(worker_id) is None
            and worker_id not in native_server._live_stream_managers
            and native_server.client_managers.get(worker_id) is None
            and redis_client.get(RedisKeys.channel_owner(worker_id)) is None
            and int(redis_client.scard(RedisKeys.clients(worker_id)) or 0) == 0
            and not redis_client.exists(RedisKeys.channel_stopping(worker_id))
            and source_server.snapshot()[1]["source-b.ts"] == 0
            and source_server.snapshot()[0]["source-b.ts"] == expected_b_requests
            and _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id],
            "Recorder supervisor did not release the private worker and provider slot",
        )
        _assert_worker_cleanup(redis_client, channel_uuid)
        _require(
            all(
                not redis_client.exists(CAPABILITY_PREFIX + str(record.get("capability_digest")))
                for record in _worker_records(redis_client, channel_uuid)
                if record.get("capability_digest")
            ),
            "Recorder supervisor left a private capability active",
        )

    def reconcile_arguments() -> tuple:
        captured = []

        def capture(*, args=None, queue=None, **_kwargs):
            captured.append((tuple(args or ()), queue))
            return None

        with patch.object(tasks.record_channel, "apply_async", side_effect=capture):
            result = tasks.reconcile_recorders.run()
        _require(
            isinstance(result, dict)
            and result.get("queued") == 1
            and len(captured) == 1,
                 "Real recorder reconciliation did not queue exactly the selected channel")
        args, queue = captured[0]
        _require(queue == "dvr" and len(args) == 3 and str(args[0]) == channel_uuid,
                 "Recorder reconciliation produced unexpected task arguments")
        current_active = load_active_configuration(active_path)
        current_control = load_recorder_control(active_path)
        _require(
            current_active is not None
            and args[1] == configuration_generation(current_active)
            and args[2] == current_control.generation,
            "Recorder reconciliation queued stale applied generations",
        )
        return args

    def invoke_control_action(action, name: str) -> dict:
        dispatched = []

        def capture(*args, **kwargs):
            dispatched.append((args, kwargs))
            return None

        with patch.object(tasks.reconcile_recorders, "apply_async", side_effect=capture):
            result = action()
        _require(
            len(dispatched) == 1 and dispatched[0][1].get("queue") == "dvr",
            f"Real {name} action did not enqueue one DVR reconciliation",
        )
        return result

    def start_reconciled_run(label: str) -> _RecorderTaskRun:
        args = reconcile_arguments()
        run = _RecorderTaskRun(
            channel_uuid, startup_timeout=45, media_idle_timeout=25,
        )
        harnesses.append(run)
        run.start(task_args=args)
        _require(
            run.expected_generation == args[1]
            and run.expected_control_generation == args[2],
            f"{label} recorder did not use the generations selected by reconciliation",
        )
        return run

    persist_draft(settings)
    previous_control_generation = load_recorder_control(active_path).generation
    active = load_active_configuration(active_path)
    _require(active is not None, "Running-control probe requires an applied configuration")
    _require(
        initial_run.thread is not None and initial_run.thread.is_alive()
        and configuration_generation(active) == initial_run.expected_generation,
        "Initial B recorder is not actively using the applied configuration",
    )
    _require(
        settings.get("filter_config")
        and schedule_is_active(
            schedule_from_snapshot(active["recording_schedule"]["channels"][channel_uuid]),
            active["recording_schedule"]["timezone"],
            datetime.now(timezone.utc),
        ),
        "Running-control probe requires an active applied schedule",
    )
    initial_worker_id = current_private_worker()
    require_live_a("initial_b_active", 1, read_fresh_media=True)
    initial_counts, initial_active_counts = source_server.snapshot()
    _require(
        initial_counts["source-b.ts"] == initial_b_request_baseline + 1
        and initial_active_counts["source-b.ts"] == 1,
        "Initial running-control B recorder did not hold one provider connection",
    )
    initial_control = load_recorder_control(active_path)
    _require(
        initial_control.generation == initial_run.expected_control_generation
        and not initial_control.paused,
        "Running archive worker did not start with the current control generation",
    )

    paused_result = invoke_control_action(runtime.pause_recorders, "Pause")
    paused = load_recorder_control(active_path)
    _require(
        paused.paused and paused.generation > initial_control.generation
        and paused_result == {"paused": True, "generation": paused.generation}
        and persisted_recording_enabled() is False,
        "Real Pause action did not persist and advance recorder control",
    )
    initial_run.join_after_supervisor(timeout=25, cooperative_sleep=gevent_sleep)
    _require(
        initial_run.result == {"status": "stopped"},
        "Pause did not stop the running recorder through its supervisor",
    )
    wait_for_private_cleanup(initial_worker_id, initial_counts["source-b.ts"])
    require_live_a("after_pause_private_cleanup", 0, read_fresh_media=True)
    pause_reconcile_dispatches = []

    def capture_pause_reconcile(*, args=None, queue=None, **_kwargs):
        pause_reconcile_dispatches.append((tuple(args or ()), queue))
        return None

    with patch.object(tasks.record_channel, "apply_async", side_effect=capture_pause_reconcile):
        pause_reconcile_result = tasks.reconcile_recorders.run()
    _require(
        isinstance(pause_reconcile_result, dict)
        and pause_reconcile_result.get("queued") == 0
        and not pause_reconcile_dispatches,
        "Paused real reconciliation queued another recorder task",
    )
    _require(
        source_server.snapshot()[0]["source-b.ts"] == initial_counts["source-b.ts"],
        "Paused recorder reopened archive B after its worker stopped",
    )

    resumed_result = invoke_control_action(runtime.resume_recorders, "Resume")
    resumed = load_recorder_control(active_path)
    _require(
        not resumed.paused and resumed.generation > paused.generation
        and resumed_result == {"paused": False, "generation": resumed.generation}
        and persisted_recording_enabled() is True,
        "Real Resume action did not publish a newer enabled control generation",
    )
    _require(
        resumed.generation > previous_control_generation,
        "Pause and Resume did not fence the pre-pause queued job generation",
    )

    resumed_baseline_ids = {
        segment.id for segment in archive_store.segments(channel_uuid)
    }
    resumed_request_baseline = source_server.snapshot()[0]["source-b.ts"]
    resumed_run = start_reconciled_run("Resumed")
    resumed_segments = _wait_for_indexed_tone(
        resumed_run, archive_store, channel_uuid, ffmpeg, ffprobe,
        expected_hz=880, existing_ids=resumed_baseline_ids, minimum=1, timeout=90,
        cooperative_sleep=gevent_sleep,
    )
    _require(
        all(segment.id not in resumed_baseline_ids for segment in resumed_segments),
        "Resume did not index fresh source B archive segments",
    )
    resumed_counts, resumed_active_counts = source_server.snapshot()
    _require(
        resumed_counts["source-b.ts"] == resumed_request_baseline + 1
        and resumed_active_counts["source-b.ts"] == 1,
        "Resume did not open exactly one new B provider connection",
    )
    resumed_worker_id = current_private_worker()
    require_live_a("after_resume_new_private_worker", 1, read_fresh_media=True)
    resumed_generation = resumed_run.expected_generation
    before_closed_apply = source_server.snapshot()[0]

    closed_settings = dict(
        settings,
        filter_config=settings["filter_config"].replace(
            "rules:\n", "schedule: {}\nrules:\n", 1,
        ),
    )
    persist_draft(closed_settings)
    closed_apply_result = runtime.apply_configuration()
    _require(
        closed_apply_result.get("applied") is True,
        "Current-row recorder Apply did not accept the closed schedule",
    )
    closed_active = load_active_configuration(active_path)
    _require(closed_active is not None, "Applied closed schedule was not readable")
    closed_generation = configuration_generation(closed_active)
    _require(
        closed_generation != resumed_generation
        and closed_active["recording_schedule"]["channels"][channel_uuid]
        == {"mode": "weekly", "intervals": []},
        "Applying a closed schedule did not publish an immutable empty channel schedule",
    )
    resumed_run.join_after_supervisor(timeout=25, cooperative_sleep=gevent_sleep)
    _require(
        resumed_run.result == {"status": "stopped"},
        "Applying a closed schedule did not stop the running recorder supervisor",
    )
    wait_for_private_cleanup(resumed_worker_id, resumed_counts["source-b.ts"])
    require_live_a("after_closed_schedule_private_cleanup", 0, read_fresh_media=True)
    _require(
        source_server.snapshot()[0]["source-b.ts"] == before_closed_apply["source-b.ts"],
        "Closed-schedule Apply reopened archive B after the running worker stopped",
    )

    from catchuparr.tasks import record_channel

    counts_before_stale = source_server.snapshot()
    profile_counts_before_stale = {
        int(profile.id): _profile_count(
            redis_client, profile.id, profile_connections_key,
        )
        for profile in (profile_a, profile_b)
    }
    current_control = load_recorder_control(active_path)
    stale_configuration = record_channel.run(
        channel_uuid, resumed_generation, current_control.generation,
    )
    stale_control = record_channel.run(
        channel_uuid, initial_run.expected_generation, previous_control_generation,
    )
    outside_schedule = record_channel.run(
        channel_uuid, closed_generation, current_control.generation,
    )
    _require(
        stale_configuration == {"status": "stale_configuration"}
        and stale_control == {"status": "stale_control"}
        and outside_schedule == {"status": "outside_schedule"},
        "Actual recorder task did not reject stale generations and a closed schedule",
    )
    _require(
        source_server.snapshot() == counts_before_stale
        and profile_counts_before_stale == {
            int(profile.id): _profile_count(
                redis_client, profile.id, profile_connections_key,
            )
            for profile in (profile_a, profile_b)
        }
        and not redis_client.exists(f"catchuparr:recorder:{channel_uuid}")
        and not any(record.get("state") == "active" for record in _worker_records(
            redis_client, channel_uuid,
        )),
        "Rejected stale or closed-schedule task changed a provider or worker resource",
    )
    require_live_a("after_stale_task_rejections", 0, read_fresh_media=True)

    window_start = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    window_end = window_start + timedelta(minutes=2 if datetime.now(timezone.utc).second < 30 else 3)
    weekday = window_start.strftime("%A").lower()
    start_clock = window_start.strftime("%H:%M")
    end_clock = window_end.strftime("%H:%M")

    timed_settings = dict(
        settings,
        filter_config=(
            settings["filter_config"].split("rules:", 1)[0]
            + "timezone: UTC\n"
            + "schedule:\n"
            + f"  {weekday}:\n"
            + f"    - {{start: \"{start_clock}\", end: \"{end_clock}\"}}\n"
            + "rules:\n"
            + "  - channels: {profile: all}\n"
            + "    include: [Synthetic archive isolation B]\n"
        ),
    )
    persist_draft(timed_settings)
    timed_apply_result = runtime.apply_configuration()
    _require(
        timed_apply_result.get("applied") is True,
        "Current-row recorder Apply did not accept the active timed schedule",
    )
    timed_active = load_active_configuration(active_path)
    _require(timed_active is not None, "Timed schedule snapshot was not readable")
    timed_generation = configuration_generation(timed_active)
    timed_schedule = schedule_from_snapshot(
        timed_active["recording_schedule"]["channels"][channel_uuid]
    )
    _require(
        schedule_is_active(timed_schedule, "UTC", datetime.now(timezone.utc)),
        "Synthetic timed schedule was not active at recorder startup",
    )
    timed_baseline_ids = {
        segment.id for segment in archive_store.segments(channel_uuid)
    }
    timed_request_baseline = source_server.snapshot()[0]["source-b.ts"]
    schedule_events = []

    class ScheduleEventCapture(logging.Handler):
        def emit(self, record):
            if (
                record.name == "catchuparr"
                and record.getMessage() == "[Catchuparr] recorder_schedule_closed"
            ):
                schedule_events.append(record.getMessage())

    logger = logging.getLogger("catchuparr")
    previous_log_level = logger.level
    schedule_event_capture = ScheduleEventCapture(logging.INFO)
    logger.setLevel(logging.INFO)
    logger.addHandler(schedule_event_capture)
    try:
        timed_run = start_reconciled_run("Timed schedule")
        timed_segments = _wait_for_indexed_tone(
            timed_run, archive_store, channel_uuid, ffmpeg, ffprobe,
            expected_hz=880, existing_ids=timed_baseline_ids, minimum=2, timeout=90,
            cooperative_sleep=gevent_sleep,
        )
        _require(bool(timed_segments), "Active timed schedule did not index useful source B media")
        timed_counts, timed_active_counts = source_server.snapshot()
        _require(
            timed_counts["source-b.ts"] == timed_request_baseline + 1
            and timed_active_counts["source-b.ts"] == 1,
            "Timed recorder did not open exactly one B provider connection",
        )
        timed_worker_id = current_private_worker()
        require_live_a("during_timed_schedule", 1, read_fresh_media=True)
        _require(
            timed_run.expected_generation == timed_generation
            and _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id] + 1
            and redis_client.exists(f"catchuparr:recorder:{channel_uuid}"),
            "Timed recorder did not hold its applied generation, lease, and provider slot",
        )
        close_deadline = time.monotonic() + 150
        while schedule_is_active(timed_schedule, "UTC", datetime.now(timezone.utc)):
            _require(
                time.monotonic() < close_deadline,
                "Synthetic weekly schedule did not reach its real-time close boundary",
            )
            _require(
                timed_run.thread is not None and timed_run.thread.is_alive()
                and source_server.snapshot()[1]["source-b.ts"] == 1
                and _profile_count(redis_client, profile_b.id, profile_connections_key)
                == profile_baselines[profile_b.id] + 1
                and redis_client.exists(f"catchuparr:recorder:{channel_uuid}"),
                "Recorder stopped before the applied weekly schedule reached its end",
            )
            gevent_sleep(0.25)
        timed_run.join_after_supervisor(timeout=20, cooperative_sleep=gevent_sleep)
        _require(
            timed_run.result == {"status": "stopped"},
            "Recorder supervisor did not stop when the active schedule window closed",
        )
        _require(
            bool(schedule_events),
            "Recorder supervisor did not report the closed schedule as its stop reason",
        )
        wait_for_private_cleanup(timed_worker_id, timed_counts["source-b.ts"])
        require_live_a("after_timed_schedule_close", 0, read_fresh_media=True)
        latest_timed_active = load_active_configuration(active_path)
        _require(
            latest_timed_active is not None
            and configuration_generation(latest_timed_active) == timed_generation
            and load_recorder_control(active_path).generation
            == timed_run.expected_control_generation,
            "Schedule-boundary stop changed configuration or control generation",
        )
        after_close_counts = source_server.snapshot()
        after_close_profiles = {
            int(profile.id): _profile_count(
                redis_client, profile.id, profile_connections_key,
            )
            for profile in (profile_a, profile_b)
        }
    finally:
        logger.removeHandler(schedule_event_capture)
        logger.setLevel(previous_log_level)

    closed_by_clock = record_channel.run(
        channel_uuid, timed_generation, timed_run.expected_control_generation,
    )
    _require(
        closed_by_clock == {"status": "outside_schedule"}
        and source_server.snapshot() == after_close_counts
        and {
            int(profile.id): _profile_count(
                redis_client, profile.id, profile_connections_key,
            )
            for profile in (profile_a, profile_b)
        } == after_close_profiles
        and not redis_client.exists(f"catchuparr:recorder:{channel_uuid}")
        and after_close_counts[0]["source-b.ts"] == initial_b_request_baseline + 3,
        "Actual closed schedule admitted a task or changed recorder resources",
    )
    print("AIO running pause, resume, schedule-close, stale-job, and live-viewer gates passed")


def probe_actual_live_archive_isolation(root: Path) -> None:
    """Keep native live A active while real recorder tasks first share A, then isolate B."""
    from unittest.mock import patch

    from aio_recorder_failover import (
        _assert_worker_cleanup,
        _DjangoHTTPBridge,
        _make_paced_transport_stream,
        _RecorderTaskRun,
        _segment_has_useful_av,
        _segment_tone_frequency,
        _SyntheticFailoverSourceServer,
        _worker_records,
    )
    from apps.channels import tasks as channel_tasks
    from apps.channels.models import (
        Channel,
        ChannelProfile,
        ChannelProfileMembership,
        ChannelStream,
        Stream,
    )
    from apps.m3u.connection_pool import (
        profile_connections_key,
        profile_credential_release_key,
    )
    from apps.m3u.models import M3UAccount
    from apps.plugins.models import PluginConfig
    from apps.proxy.live_proxy.config_helper import ConfigHelper
    from apps.proxy.live_proxy.constants import ChannelMetadataField, ChannelState
    from apps.proxy.live_proxy.redis_keys import RedisKeys
    from apps.proxy.live_proxy.server import ProxyServer
    from core.models import CoreSettings, StreamProfile
    from core.utils import RedisClient
    from django.test import Client
    from gevent import sleep as gevent_sleep

    from catchuparr import runtime as catchuparr_runtime
    from catchuparr import tasks as catchuparr_tasks
    from catchuparr.adapters.recorder_proxy import (
        _release_worker_reservation,
        reservation_credential_marker_key,
    )
    from catchuparr.configuration import (
        active_settings_path,
        apply_configuration,
        load_active_configuration,
        load_applied_state,
    )
    from catchuparr.engine.store import ArchiveStore
    from catchuparr.recorder_control import control_deny_path, control_state_path
    from catchuparr.recorder_proxy import (
        CAPABILITY_PREFIX,
        configuration_generation,
        ranked_source_candidates,
    )

    _require(
        os.environ.get("CATCHUPARR_INTEGRATION_TEST") == "1",
        "Live/archive isolation probe requires a disposable integration container",
    )
    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")
    _require(
        ffmpeg is not None and ffprobe is not None,
        "AIO live/archive probe requires ffmpeg and ffprobe",
    )

    root = Path(root)
    fixture_root = root / "synthetic-live-archive-fixtures"
    archive_root = root / "synthetic-live-archive"
    fixture_root.mkdir(parents=True, exist_ok=True)
    archive_root.mkdir(parents=True, exist_ok=True)

    active_path = active_settings_path()
    active_existed = active_path.exists()
    active_bytes = active_path.read_bytes() if active_existed else None
    active_lock_path = active_path.with_suffix(active_path.suffix + ".lock")
    active_lock_existed = active_lock_path.exists()
    active_lock_bytes = active_lock_path.read_bytes() if active_lock_existed else None
    reset_marker_path = active_path.with_name(".catchuparr-configuration-reset-required")
    reset_marker_existed = reset_marker_path.exists()
    reset_marker_bytes = reset_marker_path.read_bytes() if reset_marker_existed else None
    control_path = control_state_path(active_path)
    control_existed = control_path.exists()
    control_bytes = control_path.read_bytes() if control_existed else None
    control_deny = control_deny_path(active_path)
    control_deny_existed = control_deny.exists()
    control_deny_bytes = control_deny.read_bytes() if control_deny_existed else None
    plugin_config = PluginConfig.objects.get(key="catchuparr")
    original_plugin_settings = dict(plugin_config.settings or {})

    def apply_stats_recorder_option(hide_recorders: bool) -> None:
        from aio_stats_probe import stats_options_draft

        current = PluginConfig.objects.get(key="catchuparr")
        full_settings = dict(current.settings or {})
        full_settings.update(settings)
        draft = stats_options_draft(full_settings, hide_recorders=hide_recorders)
        current.settings = draft
        current.save(update_fields=("settings",))
        applied = catchuparr_runtime.apply_configuration(draft)
        _require(applied.get("applied") is True,
                 "Unified Apply rejected recorder Stats visibility option")
        settings["hide_recorders_in_stats"] = hide_recorders

    original_base_url = channel_tasks.get_dvr_stream_base_url
    saved_default_profile = None
    default_profile_saved = False
    source_server = None
    bridge = None
    redis_client = None
    live_response = None
    live_reader = None
    live_session = None
    churn_session = None
    native_server = None
    native_diagnostic = None
    native_diagnostic_report = None
    channel = None
    created_accounts = []
    created_streams = []
    created_profiles = []
    created_channel_profiles = []
    harnesses = []
    profile_baselines = {}
    marker_baselines = {}
    cleanup_errors = []
    cleanup_exception_types = []
    primary_exception = None

    try:
        payload_a = _make_paced_transport_stream(
            ffmpeg,
            ffprobe,
            fixture_root / "source-a.ts",
            service_name="Synthetic Live Source A",
            frequency=440,
        )
        payload_b = _make_paced_transport_stream(
            ffmpeg,
            ffprobe,
            fixture_root / "source-b.ts",
            service_name="Synthetic Archive Source B",
            frequency=880,
        )
        source_server = _SyntheticFailoverSourceServer(
            {"source-a.ts": payload_a, "source-b.ts": payload_b},
            {"source-a.ts": 8.0, "source-b.ts": 8.0},
        )
        source_server.thread.start()

        account_a = M3UAccount.objects.create(
            name="Synthetic live isolation A", is_active=True, max_streams=1,
        )
        account_b = M3UAccount.objects.create(
            name="Synthetic archive isolation B", is_active=True, max_streams=1,
        )
        created_accounts.extend((account_a, account_b))
        stream_a = Stream.objects.create(
            name="Synthetic isolation source A",
            url=f"{source_server.base_url}/source-a.ts",
            m3u_account=account_a,
        )
        stream_b = Stream.objects.create(
            name="Synthetic isolation source B",
            url=f"{source_server.base_url}/source-b.ts",
            m3u_account=account_b,
        )
        created_streams.extend((stream_a, stream_b))
        channel = Channel.objects.create(
            name="Synthetic live archive isolation",
            channel_number=97,
            user_level=0,
        )
        profile_name = "Synthetic live archive profile"
        channel_profile = ChannelProfile.objects.create(name=profile_name)
        created_channel_profiles.append(channel_profile)
        ChannelProfileMembership.objects.filter(channel_profile=channel_profile).update(
            enabled=False
        )
        ChannelProfileMembership.objects.update_or_create(
            channel_profile=channel_profile,
            channel=channel,
            defaults={"enabled": True},
        )
        ChannelStream.objects.create(channel=channel, stream=stream_a, order=0)
        ChannelStream.objects.create(channel=channel, stream=stream_b, order=1)
        assignment_rows = list(
            ChannelStream.objects.filter(channel=channel)
            .order_by("order", "id")
            .values_list("stream_id", "order")
        )
        _require(
            assignment_rows == [(stream_a.id, 0), (stream_b.id, 1)],
            "Synthetic live/archive fixture did not preserve A then B assignment order",
        )

        profiles = []
        for account in created_accounts:
            profile = account.profiles.filter(is_default=True).first()
            _require(profile is not None, "Synthetic live/archive M3U profile was not created")
            profile.max_streams = 1
            profile.is_active = True
            profile.save(update_fields=("max_streams", "is_active"))
            profiles.append(profile)
        profile_a, profile_b = profiles
        created_profiles.extend(profiles)

        proxy_profile = StreamProfile.objects.filter(
            name__iexact="Proxy", locked=True, is_active=True,
        ).first()
        _require(proxy_profile is not None and proxy_profile.is_proxy(),
                 "Native non-redirect Proxy StreamProfile is unavailable")
        saved_default_profile = _stream_default_profile_id(CoreSettings)
        default_profile_saved = True
        _set_stream_default_profile(CoreSettings, proxy_profile.id)
        _require(
            str(_stream_default_profile_id(CoreSettings)) == str(proxy_profile.id),
            "Native default StreamProfile was not set to Proxy",
        )

        settings = {
            "filter_config": (
                "version: 1\n"
                f"profile: {profile_name}\n"
                "rules:\n"
                "  - channels: {profile: all}\n"
                "    exclude: []\n"
            ),
            "archive_root": str(archive_root),
            "retention_hours": 1,
            "max_storage_gib": 1,
            "show_archive_playback_in_stats": bool(
                original_plugin_settings.get("show_archive_playback_in_stats", True)
            ),
            "hide_recorders_in_stats": True,
        }
        apply_configuration(settings, active_path=active_path)
        active = load_active_configuration(active_path)
        _require(active is not None, "Applied no-rules live/archive configuration was not readable")
        _require(
            ranked_source_candidates(str(channel.uuid), active) is None,
            "No-rules configuration did not preserve native channel route selection",
        )
        from catchuparr.runtime import load_config

        runtime_config = load_config()
        _require(
            runtime_config is not None and str(channel.uuid) in runtime_config.channel_uuids,
            "Synthetic live/archive channel is absent from active recorder configuration",
        )

        redis_client = RedisClient.get_client()
        redis_client.ping()
        profile_baselines = {
            int(profile.id): _profile_count(redis_client, profile.id, profile_connections_key)
            for profile in created_profiles
        }
        marker_baselines = {
            int(profile.id): _key_dump(
                redis_client, profile_credential_release_key(profile.id),
            )
            for profile in created_profiles
        }
        _require(
            all(value == 0 for value in profile_baselines.values()),
            "Synthetic live/archive provider counters were not empty before use",
        )
        _require(
            all(value is None for value in marker_baselines.values()),
            "Synthetic live/archive credential markers were not empty before use",
        )
        store = ArchiveStore(archive_root)
        bridge = _DjangoHTTPBridge()
        bridge.start()
        channel_tasks.get_dvr_stream_base_url = lambda: bridge.base_url

        client = Client(raise_request_exception=False)
        native_server = ProxyServer.get_instance()
        worker_id = str(channel.uuid)
        from aio_native_redis_diagnostic import NativeRedisInitDiagnostic

        native_diagnostic = NativeRedisInitDiagnostic(
            worker_id=worker_id,
            redis_keys=RedisKeys,
            native_server=native_server,
        )
        native_diagnostic.start(
            redis_client=redis_client,
        )
        live_response = client.get(
            f"/proxy/ts/stream/{channel.uuid}",
            HTTP_HOST="localhost",
            HTTP_USER_AGENT="Synthetic live/archive integration",
        )
        _require(live_response.status_code == 200, "Native live A route did not return HTTP 200")
        _require(
            live_response.get("Content-Type", "").split(";", 1)[0] == "video/mp2t",
            "Native live A route did not return MPEG-TS",
        )
        _require(
            not live_response.get("Location"),
            "Native Proxy profile redirected the live A route",
        )
        _require(getattr(live_response, "streaming", False), "Native live A route was not a stream")

        # Keep one reader on the real public response through native startup.
        # On failure, outer cleanup stops the channel before joining the reader.
        live_session, native_buffer = _create_native_live_reader_session(
            live_response, native_server, worker_id,
        )
        live_reader = live_session.reader
        _wait_for_native_client_count(
            redis_client,
            native_server,
            RedisKeys,
            worker_id,
            1,
            timeout=45,
            reject_excess=True,
        )
        native_owner, native_manager, native_client_manager = _wait_for_native_active(
            redis_client,
            native_server,
            worker_id,
            redis_keys=RedisKeys,
            metadata_field=ChannelMetadataField,
            channel_state=ChannelState,
            timeout=45,
        )
        # Capture only the native startup interval. Once the real route client
        # is registered and the first ownership check has completed, restore
        # every diagnostic hook before the longer archive/control phases.
        native_diagnostic_report = native_diagnostic.close()
        diagnostic_observed = native_diagnostic_report["observed"]
        _require(
            all(
                diagnostic_observed[name]
                for name in (
                    "channel_initialize",
                    "metadata_write",
                    "native_manager_thread_start",
                    "ownership_check",
                    "ownership_allowed",
                    "client_add",
                )
            )
            and native_diagnostic_report["redis_targets_match"],
            "Native Redis startup diagnostic did not observe the complete synthetic init path",
        )
        active_state = _constant_text(ChannelState.ACTIVE)
        first_publication_floor = int(
            native_buffer.redis_client.get(native_buffer.buffer_index_key) or 0
        )
        _require(
            first_publication_floor > 0,
            "Native live A did not publish an initial buffer chunk",
        )
        first_live_media, first_live_index = live_reader.read_after(
            first_publication_floor, minimum_bytes=188 * 512, timeout=20,
        )
        _require(
            first_live_index > first_publication_floor,
            "Native live A did not deliver a newly published buffer chunk",
        )
        _verify_audio_video_tone(ffmpeg, ffprobe, first_live_media, 440, "Native live A")
        _require(native_manager.running, "Native live A stream manager is not running")
        _require(
            native_client_manager is native_server.client_managers.get(worker_id),
            "Native live A client manager changed during activation",
        )
        metadata_key = RedisKeys.channel_metadata(worker_id)
        _require(
            _redis_text(redis_client.hget(metadata_key, ChannelMetadataField.STREAM_ID))
            == str(stream_a.id),
            "Native live A metadata does not identify assigned source A",
        )
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1,
            "Native live A did not reserve exactly one A profile slot",
        )
        native_a_marker = _key_dump(
            redis_client, profile_credential_release_key(profile_a.id),
        )
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id],
            "Native live A unexpectedly reserved B profile capacity",
        )
        counts, active_counts = source_server.snapshot()
        _require(counts["source-a.ts"] >= 1 and active_counts["source-a.ts"] == 1,
                 "Native live A did not open its synthetic A source")
        _require(counts["source-b.ts"] == 0, "Native live A opened archive source B")
        _run_registration_crash_recovery_probe(
            active_path=active_path,
            settings=settings,
            archive_root=archive_root,
            channel_uuid=worker_id,
            redis_client=redis_client,
            native_server=native_server,
            redis_keys=RedisKeys,
            metadata_key=metadata_key,
            native_owner=native_owner,
            active_state=active_state,
            source_server=source_server,
            profile_a=profile_a,
            profile_connections_key=profile_connections_key,
            profile_a_baseline=profile_baselines[profile_a.id],
        )
        _wait_for_native_client_count(redis_client, native_server, RedisKeys, worker_id, 1)

        assignment_keys = {
            f"channel_stream:{stream_a.id}",
            f"channel_stream:{stream_b.id}",
            f"channel_stream:{worker_id}",
            f"channel_stream:{channel.id}",
            f"stream_profile:{stream_a.id}",
            f"stream_profile:{stream_b.id}",
            f"stream_profile:{channel.id}",
            RedisKeys.channel_owner(worker_id),
        }
        assignment_snapshot = {
            key: _key_dump(redis_client, key) for key in assignment_keys
        }
        assigned_stream_id = _redis_text(
            redis_client.get(f"channel_stream:{channel.id}")
        )
        _require(
            assigned_stream_id == str(stream_a.id)
            and assignment_snapshot[f"stream_profile:{stream_a.id}"] is not None,
            "Native live A did not publish its channel-keyed stream and profile keys",
        )
        source_metadata = _native_source_metadata(redis_client, metadata_key)
        _require(
            source_metadata.get("stream_id") == str(stream_a.id)
            and source_metadata.get("stream_name") == stream_a.name
            and source_metadata.get("url") == stream_a.url
            and any(
                "profile" in key and value
                for key, value in source_metadata.items()
            ),
            "Native live A source metadata is incomplete",
        )
        apply_stats_recorder_option(True)
        # With no policy, the real recorder task must attach to this same
        # native A worker and index decoded A rather than opening a second
        # provider connection.
        run_a = _RecorderTaskRun(worker_id, startup_timeout=45, media_idle_timeout=25)
        run_a.start()
        harnesses.append(run_a)
        baseline_ids = {segment.id for segment in store.segments(worker_id)}
        segments_a = _wait_for_indexed_tone(
            run_a, store, worker_id, ffmpeg, ffprobe,
            expected_hz=440, existing_ids=baseline_ids, minimum=1, timeout=90,
            cooperative_sleep=gevent_sleep,
        )
        _require(bool(segments_a), "No-rules recorder did not archive synthetic live source A")
        _wait_for_native_client_count(redis_client, native_server, RedisKeys, worker_id, 2)
        _require(
            native_server.stream_managers.get(worker_id) is native_manager
            and native_manager.running
            and _redis_text(redis_client.get(RedisKeys.channel_owner(worker_id))) == native_owner
            and _redis_text(redis_client.hget(metadata_key, ChannelMetadataField.STATE))
            == active_state,
            "No-rules archive did not share the existing native live A worker",
        )
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1,
            "No-rules archive acquired an extra A profile reservation",
        )
        counts_after_shared, active_after_shared = source_server.snapshot()
        _require(
            counts_after_shared["source-a.ts"] == counts["source-a.ts"]
            and active_after_shared["source-a.ts"] == 1
            and counts_after_shared["source-b.ts"] == 0,
            "No-rules archive opened a second provider connection or source B",
        )
        _require(
            {key: _key_dump(redis_client, key) for key in assignment_keys}
            == assignment_snapshot,
            "No-rules archive changed native live A assignment keys",
        )
        _require(
            _native_source_metadata(redis_client, metadata_key) == source_metadata,
            "No-rules archive changed native live A source metadata",
        )
        from aio_stats_probe import probe_actual_recorder_stats

        probe_actual_recorder_stats(
            channel_id=worker_id,
            redis_client=redis_client,
            native_server=native_server,
            profile_a=profile_a,
            profile_b=profile_b,
            profile_connections_key=profile_connections_key,
            assignment_snapshot=assignment_snapshot,
            metadata_key=metadata_key,
        )
        run_a.stop()
        run_a.join(cooperative_sleep=gevent_sleep)
        _wait_for_native_client_count(redis_client, native_server, RedisKeys, worker_id, 1)
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1,
            "Stopping the shared no-rules archive released live A capacity",
        )
        _require(
            _key_dump(redis_client, profile_credential_release_key(profile_a.id))
            == native_a_marker,
            "Stopping the shared no-rules archive changed the live A release marker",
        )
        # Applying B while A is live must create a dedicated recorder worker.
        from aio_stats_probe import stats_options_draft

        settings_b = stats_options_draft(
            dict(
                settings,
                filter_config=(
                    "version: 1\n"
                    f"profile: {profile_name}\n"
                    "rules:\n"
                    "  - channels: {profile: all}\n"
                    "    include: [Synthetic archive isolation B]\n"
                ),
            ),
            hide_recorders=True,
        )
        current_plugin_config = PluginConfig.objects.get(key="catchuparr")
        full_settings_b = dict(current_plugin_config.settings or {})
        full_settings_b.update(settings_b)
        settings_b = full_settings_b
        current_plugin_config.settings = dict(settings_b)
        current_plugin_config.save(update_fields=("settings",))
        applied_b = catchuparr_runtime.apply_configuration(settings_b)
        _require(applied_b.get("applied") is True,
                 "Unified Apply rejected B recorder fixture with Stats flags")
        active_b = load_active_configuration(active_path)
        _require(active_b is not None, "Applied B-only recorder configuration was not readable")
        candidates_b = ranked_source_candidates(worker_id, active_b)
        _require(
            candidates_b is not None
            and [str(candidate.get("id")) for candidate in candidates_b] == [str(stream_b.id)],
            "Applied include-only policy did not select only assigned source B",
        )
        baseline_ids_b = {segment.id for segment in store.segments(worker_id)}
        publication_floor_before_b = int(
            native_buffer.redis_client.get(native_buffer.buffer_index_key) or 0
        )
        _require(publication_floor_before_b > 0,
                 "Native live A did not publish chunks before the B archive started")
        run_b = _RecorderTaskRun(worker_id, startup_timeout=45, media_idle_timeout=25)
        run_b.start()
        harnesses.append(run_b)
        segments_b = _wait_for_indexed_tone(
            run_b, store, worker_id, ffmpeg, ffprobe,
            expected_hz=880, existing_ids=baseline_ids_b, minimum=1, timeout=90,
            cooperative_sleep=gevent_sleep,
        )
        _require(bool(segments_b), "B-only recorder did not index synthetic archive source B")
        _require(
            all(segment.id not in baseline_ids_b for segment in segments_b),
            "B-only recorder did not publish new archive segment IDs",
        )
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1
            and _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id] + 1,
            "Live A and archive B did not hold exactly one separate profile slot each",
        )
        counts_during_b, active_during_b = source_server.snapshot()
        _require(
            counts_during_b["source-a.ts"] == counts["source-a.ts"]
            and active_during_b["source-a.ts"] == 1
            and counts_during_b["source-b.ts"] == counts_after_shared["source-b.ts"] + 1
            and active_during_b["source-b.ts"] == 1,
            "Archive B disturbed live A or failed to open its own source",
        )
        _require(
            native_server.stream_managers.get(worker_id) is native_manager
            and native_manager.running
            and _redis_text(redis_client.get(RedisKeys.channel_owner(worker_id))) == native_owner,
            "Archive B replaced the existing native live A owner or manager",
        )
        _require(
            _redis_text(redis_client.hget(metadata_key, ChannelMetadataField.STATE))
            == active_state,
            "Native live A was not ACTIVE while archive B ran",
        )
        _wait_for_native_client_count(redis_client, native_server, RedisKeys, worker_id, 1)
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1,
            "Dedicated archive B added a second live A profile reservation",
        )
        _require(
            _key_dump(redis_client, profile_credential_release_key(profile_a.id))
            == native_a_marker,
            "Dedicated archive B changed the live A release marker",
        )
        _require(
            {key: _key_dump(redis_client, key) for key in assignment_keys}
            == assignment_snapshot,
            "Archive B changed native live A assignment keys",
        )
        _require(
            _native_source_metadata(redis_client, metadata_key) == source_metadata,
            "Archive B changed native live A source metadata",
        )
        try:
            current_assignment_rows = list(
                ChannelStream.objects.filter(channel=channel).order_by("order", "id")
                .values_list("stream_id", "order")
            )
        except BaseException as error:
            print(
                "CATCHUPARR_DB_CONTEXT_DIAGNOSTIC "
                + json.dumps(
                    {
                        "operation": "native_assignment_read",
                        "exception_type": type(error).__name__,
                        "db_context": _current_orm_context_snapshot(),
                    },
                    sort_keys=True,
                )
            )
            raise
        _require(
            current_assignment_rows == assignment_rows,
            "Archive B reordered native ChannelStream rows",
        )
        active_records = [
            record for record in _worker_records(redis_client, worker_id)
            if record.get("state") == "active"
            and record.get("reservation_state") == "reserved"
        ]
        _require(
            len(active_records) == 1,
            "B archive did not have exactly one active managed worker",
        )
        active_record = active_records[0]
        b_worker_id = active_record.get("worker_id")
        _require(
            b_worker_id and b_worker_id != worker_id
            and active_record.get("stream_id") == str(stream_b.id)
            and active_record.get("profile_id") == str(profile_b.id),
            "Archive B did not use a separate managed worker and B profile",
        )
        from aio_stats_probe import probe_actual_private_recorder_stats

        probe_actual_private_recorder_stats(
            channel_id=b_worker_id,
            ordinary_channel_id=worker_id,
            native_server=native_server,
            redis_client=redis_client,
            expected_hidden=True,
        )
        _require(
            source_server.snapshot() == (counts_during_b, active_during_b),
            "Hide=true private Stats probe changed provider source activity",
        )
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1
            and _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id] + 1,
            "Native Stats probes changed active live A or private B provider slots",
        )
        _require(
            _key_dump(redis_client, profile_credential_release_key(profile_a.id))
            == native_a_marker
            and {key: _key_dump(redis_client, key) for key in assignment_keys}
            == assignment_snapshot
            and _native_source_metadata(redis_client, metadata_key) == source_metadata,
            "Native Stats probes changed provider assignments or live A source metadata",
        )

        # A hide=false result must come from a fresh committed generation. Stop
        # B and prove every private resource is released before applying it.
        first_b_worker_id = b_worker_id
        run_b.stop()
        run_b.join(cooperative_sleep=gevent_sleep)
        deadline = time.monotonic() + _native_teardown_readiness_timeout(
            float(ConfigHelper.channel_shutdown_delay()),
            float(ConfigHelper.cleanup_check_interval()),
            stopping_marker_ttl=90,
        )
        while time.monotonic() < deadline:
            b_manager = native_server.stream_managers.get(first_b_worker_id)
            b_client_manager = native_server.client_managers.get(first_b_worker_id)
            b_clients = int(redis_client.scard(RedisKeys.clients(first_b_worker_id)) or 0)
            _counts, b_active = source_server.snapshot()
            if (
                b_manager is None
                and first_b_worker_id not in native_server._live_stream_managers
                and b_client_manager is None
                and redis_client.get(RedisKeys.channel_owner(first_b_worker_id)) is None
                and b_clients == 0
                and not redis_client.exists(RedisKeys.channel_stopping(first_b_worker_id))
                and b_active["source-b.ts"] == 0
            ):
                break
            gevent_sleep(0.1)
        first_b_stopped_counts, first_b_stopped_active = source_server.snapshot()
        _require(
            native_server.stream_managers.get(first_b_worker_id) is None
            and first_b_worker_id not in native_server._live_stream_managers
            and native_server.client_managers.get(first_b_worker_id) is None
            and redis_client.get(RedisKeys.channel_owner(first_b_worker_id)) is None
            and int(redis_client.scard(RedisKeys.clients(first_b_worker_id)) or 0) == 0
            and not redis_client.exists(RedisKeys.channel_stopping(first_b_worker_id))
            and first_b_stopped_active["source-b.ts"] == 0
            and first_b_stopped_counts["source-b.ts"]
            == counts_during_b["source-b.ts"],
            "Stopping the hide=true B worker did not release its native source",
        )
        _assert_private_worker_stop_request_count(counts_during_b, first_b_stopped_counts)
        _assert_worker_cleanup(redis_client, worker_id)
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id]
            and _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1,
            "Stopping hide=true B did not release its profile slot exactly once",
        )
        _require(
            native_server.stream_managers.get(worker_id) is native_manager
            and native_manager.running
            and _redis_text(redis_client.get(RedisKeys.channel_owner(worker_id)))
            == native_owner
            and _native_source_metadata(redis_client, metadata_key) == source_metadata
            and {key: _key_dump(redis_client, key) for key in assignment_keys}
            == assignment_snapshot,
            "Stopping hide=true B changed live A ownership or assignments",
        )

        settings_b = stats_options_draft(settings_b, hide_recorders=False)
        current_plugin_config = PluginConfig.objects.get(key="catchuparr")
        current_plugin_config.settings = dict(settings_b)
        current_plugin_config.save(update_fields=("settings",))
        applied_b_visible = catchuparr_runtime.apply_configuration(settings_b)
        _require(applied_b_visible.get("applied") is True,
                 "Unified Apply rejected B recorder fixture with Hide=false")
        active_b_visible = load_active_configuration(active_path)
        _require(active_b_visible is not None,
                 "Applied Hide=false B configuration was not readable")
        _require(
            ranked_source_candidates(worker_id, active_b_visible) is not None
            and [str(candidate.get("id")) for candidate in
                 ranked_source_candidates(worker_id, active_b_visible)] == [str(stream_b.id)],
            "Hide=false Apply changed the selected B source or was not committed",
        )
        visible_generation = configuration_generation(active_b_visible)
        _require(
            visible_generation != configuration_generation(active_b),
            "Hide=false Apply did not publish its applied recorder generation",
        )
        baseline_ids_b_visible = {segment.id for segment in store.segments(worker_id)}
        run_b = _RecorderTaskRun(worker_id, startup_timeout=45, media_idle_timeout=25)
        run_b.start()
        harnesses.append(run_b)
        segments_b_visible = _wait_for_indexed_tone(
            run_b, store, worker_id, ffmpeg, ffprobe,
            expected_hz=880, existing_ids=baseline_ids_b_visible, minimum=1,
            timeout=90, cooperative_sleep=gevent_sleep,
        )
        _require(bool(segments_b_visible),
                 "Hide=false B recorder did not index synthetic source media")
        visible_counts, visible_active = source_server.snapshot()
        _require(
            visible_counts["source-b.ts"] == first_b_stopped_counts["source-b.ts"] + 1
            and visible_active["source-b.ts"] == 1
            and visible_active["source-a.ts"] == 1,
            "Hide=false B recorder did not open exactly one B source beside live A",
        )
        active_visible_records = [
            record for record in _worker_records(redis_client, worker_id)
            if record.get("state") == "active"
            and record.get("reservation_state") == "reserved"
        ]
        _require(len(active_visible_records) == 1,
                 "Hide=false B recorder did not own exactly one active worker")
        visible_worker_id = active_visible_records[0].get("worker_id")
        _require(
            visible_worker_id and visible_worker_id != first_b_worker_id
            and active_visible_records[0].get("config_generation") == visible_generation,
            "Hide=false B recorder reused a stale worker or recorder generation",
        )
        b_worker_id = visible_worker_id
        probe_actual_private_recorder_stats(
            channel_id=b_worker_id,
            ordinary_channel_id=worker_id,
            native_server=native_server,
            redis_client=redis_client,
            expected_hidden=False,
        )
        _require(
            source_server.snapshot() == (visible_counts, visible_active)
            and _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1
            and _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id] + 1
            and {key: _key_dump(redis_client, key) for key in assignment_keys}
            == assignment_snapshot
            and _native_source_metadata(redis_client, metadata_key) == source_metadata,
            "Hide=false private Stats probe changed sources, slots, or live A metadata",
        )
        publication_floor_during_b = int(
            native_buffer.redis_client.get(native_buffer.buffer_index_key) or 0
        )
        _require(
            publication_floor_during_b > publication_floor_before_b,
            "Native live A did not publish a newer chunk after archive B became active",
        )
        live_media_during_b, live_index_during_b = live_reader.read_after(
            publication_floor_during_b, minimum_bytes=188 * 100, timeout=20,
        )
        _require(live_index_during_b > publication_floor_during_b,
                 "Native live A did not yield a new buffer chunk while B recorded")
        _verify_audio_video_tone(
            ffmpeg, ffprobe, live_media_during_b, 440, "Native live A during archive B",
        )

        _probe_running_recorder_controls(
            channel_uuid=worker_id,
            settings=settings_b,
            active_path=active_path,
            archive_store=store,
            source_server=source_server,
            redis_client=redis_client,
            native_server=native_server,
            native_buffer=native_buffer,
            native_owner=native_owner,
            native_manager=native_manager,
            native_client_manager=native_client_manager,
            metadata_key=metadata_key,
            active_state=active_state,
            assignment_snapshot=assignment_snapshot,
            source_metadata=source_metadata,
            live_reader=live_reader,
            profile_a=profile_a,
            profile_b=profile_b,
            profile_baselines=profile_baselines,
            native_a_marker=native_a_marker,
            initial_b_request_baseline=first_b_stopped_counts["source-b.ts"],
            ffmpeg=ffmpeg,
            ffprobe=ffprobe,
            harnesses=harnesses,
            gevent_sleep=gevent_sleep,
            initial_run=run_b,
        )
        counts_after_running_controls, _ = source_server.snapshot()

        run_b.stop()
        run_b.join(cooperative_sleep=gevent_sleep)
        all_segments_after_b_stop = store.segments(worker_id)
        segment_by_id = {segment.id: segment for segment in all_segments_after_b_stop}
        _require(
            baseline_ids_b <= set(segment_by_id),
            "B archive cleanup removed previously indexed A media",
        )
        _require(
            {segment.id for segment in segments_b} <= set(segment_by_id),
            "B archive cleanup removed an indexed B segment",
        )
        for segment in all_segments_after_b_stop:
            if segment.id in baseline_ids_b:
                _require(
                    _segment_has_useful_av(ffprobe, segment.path),
                    "Previously indexed A archive media became unreadable",
                )
                _assert_tone_matches(
                    _segment_tone_frequency(ffmpeg, segment.path),
                    440,
                    "Previously indexed live A archive",
                )
            else:
                _require(
                    _segment_has_useful_av(ffprobe, segment.path),
                    "Final indexed B archive segment lacks decoded audio/video",
                )
                _assert_tone_matches(
                    _segment_tone_frequency(ffmpeg, segment.path),
                    880,
                    "Final indexed archive B",
                )
        deadline = time.monotonic() + _native_teardown_readiness_timeout(
            float(ConfigHelper.channel_shutdown_delay()),
            float(ConfigHelper.cleanup_check_interval()),
            stopping_marker_ttl=90,
        )
        while time.monotonic() < deadline:
            b_manager = native_server.stream_managers.get(b_worker_id)
            b_client_manager = native_server.client_managers.get(b_worker_id)
            b_clients = int(redis_client.scard(RedisKeys.clients(b_worker_id)) or 0)
            _counts, b_active = source_server.snapshot()
            if (
                b_manager is None
                and b_worker_id not in native_server._live_stream_managers
                and b_client_manager is None
                and redis_client.get(RedisKeys.channel_owner(b_worker_id)) is None
                and b_clients == 0
                and not redis_client.exists(RedisKeys.channel_stopping(b_worker_id))
                and b_active["source-b.ts"] == 0
            ):
                break
            gevent_sleep(0.1)
        _require(
            native_server.stream_managers.get(b_worker_id) is None
            and b_worker_id not in native_server._live_stream_managers
            and native_server.client_managers.get(b_worker_id) is None
            and redis_client.get(RedisKeys.channel_owner(b_worker_id)) is None
            and int(redis_client.scard(RedisKeys.clients(b_worker_id)) or 0) == 0
            and not redis_client.exists(RedisKeys.channel_stopping(b_worker_id))
            and source_server.snapshot()[0]["source-b.ts"]
            == counts_after_running_controls["source-b.ts"]
            and source_server.snapshot()[1]["source-b.ts"] == 0,
            "Stopping archive B left its private native worker or provider source active",
        )
        _assert_worker_cleanup(redis_client, worker_id)
        _require(
            not _release_worker_reservation(redis_client, b_worker_id),
            "Duplicate archive B cleanup claimed a second profile reservation",
        )
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id]
            and _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1,
            "Stopping archive B did not release B once while preserving live A capacity",
        )
        _require(
            _key_dump(redis_client, profile_credential_release_key(profile_a.id))
            == native_a_marker,
            "Stopping archive B changed the live A release marker",
        )
        _require(
            native_server.stream_managers.get(worker_id) is native_manager
            and native_manager.running
            and _redis_text(redis_client.get(RedisKeys.channel_owner(worker_id))) == native_owner,
            "Stopping archive B stopped the native live A manager or owner",
        )
        _require(
            _redis_text(redis_client.hget(metadata_key, ChannelMetadataField.STATE))
            == active_state,
            "Native live A stopped being ACTIVE after archive B cleanup",
        )
        _wait_for_native_client_count(redis_client, native_server, RedisKeys, worker_id, 1)
        _require(
            {key: _key_dump(redis_client, key) for key in assignment_keys}
            == assignment_snapshot,
            "Stopping archive B changed native live A assignment keys",
        )
        _require(
            _native_source_metadata(redis_client, metadata_key) == source_metadata,
            "Stopping archive B changed native live A source metadata",
        )
        counts_after_b_stop, active_after_b_stop = source_server.snapshot()
        _require(
            counts_after_b_stop["source-a.ts"] == counts["source-a.ts"]
            and active_after_b_stop["source-a.ts"] >= 1,
            "Stopping archive B interrupted the existing live A source",
        )
        stop_floor = int(native_buffer.redis_client.get(native_buffer.buffer_index_key) or 0)
        _require(stop_floor > 0, "Native live A buffer head disappeared after B cleanup")
        live_media_after_b_stop, live_index_after_b_stop = live_reader.read_after(
            stop_floor, minimum_bytes=188 * 100, timeout=20,
        )
        _require(live_index_after_b_stop > stop_floor,
                 "Native live A did not yield fresh media after archive B cleanup")
        _verify_audio_video_tone(
            ffmpeg, ffprobe, live_media_after_b_stop, 440,
            "Native live A after archive B cleanup",
        )

        # Exercise the real opt-in connection-sharing path with the only
        # allowed archive source set to B while the existing native viewer is
        # on A. The first recorder input must borrow A's published TS buffer;
        # after the final ordinary viewer leaves, it must return to B only
        # after the applied switch cooldown.
        from aio_stats_probe import stats_options_draft

        sharing_settings = global_sharing_draft(
            stats_options_draft(settings_b, hide_recorders=True),
            enabled=True, live_stability_seconds=1,
            last_viewer_return_seconds=2, min_switch_interval_seconds=60,
        )
        current_plugin_config = PluginConfig.objects.get(key="catchuparr")
        current_plugin_config.settings = dict(sharing_settings)
        current_plugin_config.save(update_fields=("settings",))
        applied_sharing = catchuparr_runtime.apply_configuration(sharing_settings)
        _require(
            applied_sharing.get("applied") is True,
            "Unified Apply rejected the synthetic sharing-enabled B policy",
        )
        active_sharing = load_active_configuration(active_path)
        _require(active_sharing is not None, "Applied sharing configuration was not readable")
        assert_applied_global_sharing(
            active_sharing, worker_id, enabled=True, live_stability_seconds=1,
            last_viewer_return_seconds=2, min_switch_interval_seconds=60,
        )
        _require(
            active_sharing["connection_sharing"][worker_id]["enabled"] is True
            and [str(item.get("id")) for item in ranked_source_candidates(
                worker_id, active_sharing,
            )] == [str(stream_b.id)],
            "Sharing did not retain the B-only fallback policy",
        )
        share_source_baseline, share_active_baseline = source_server.snapshot()
        share_b_request_baseline = len(source_server.request_times("source-b.ts"))
        _require(
            share_active_baseline["source-a.ts"] == 1
            and share_active_baseline["source-b.ts"] == 0,
            "Sharing fixture did not start with only the existing live A connection",
        )
        # Leave one exact registered opening durably awaiting cleanup. The
        # next recorder task must close this channel-local residue before its
        # first native observation, then attach to this same ordinary viewer.
        from hashlib import sha256

        from catchuparr.adapters.sharing_native import NativeClientRegistrationAck
        from catchuparr.adapters.sharing_reader import redis_backend_signature
        from catchuparr.recorder_proxy import configuration_generation
        from catchuparr.sharing_episode import BorrowEpisodeLedger
        from catchuparr.sharing_persistence import SharingIntentToken
        from catchuparr.sharing_planner import SharingAction

        sharing_active, sharing_control = load_applied_state(active_path)
        _require(sharing_active is not None, "Sharing recovery snapshot is unavailable")
        sharing_episode_ledger = BorrowEpisodeLedger(
            archive_root / ".catchuparr-sharing.sqlite3",
        )
        prior_episode = sharing_episode_ledger.read(str(channel.uuid))
        _require(
            prior_episode.phase == "closed",
            "Failed-opening fixture requires the prior channel episode cleanup ACK",
        )
        sharing_identity = _failed_opening_identity(
            channel.uuid, native_owner, stream_a, source_metadata,
        )
        failed_token = SharingIntentToken(
            channel_uuid=str(channel.uuid), revision=1, intent_epoch=9000,
            action=SharingAction.ATTACH_BORROW,
            configuration_generation=configuration_generation(sharing_active),
            control_generation=sharing_control.generation,
            recorder_fence=0, target_identity=sharing_identity,
            target_admitted=True, not_before=time.time(),
        )
        failed_opening = sharing_episode_ledger.begin_open(
            failed_token,
            lease_digest=sha256(b"synthetic expired opening lease").hexdigest(),
            identity=sharing_identity,
            client_id="catchuparr-client-synthetic-failed-opening",
            expected_version=prior_episode.version,
        )
        failed_registration = sharing_episode_ledger.prepare_registration(
            failed_opening, local_worker_id=str(worker_id),
            backend_signature=redis_backend_signature(redis_client),
            user_agent="Synthetic recorder cleanup test", client_ip="127.0.0.1",
        )
        failed_ack = NativeClientRegistrationAck(
            channel_uuid=str(channel.uuid), episode_id=failed_opening.episode_id,
            client_id=failed_registration.client_id,
            local_worker_id=failed_registration.local_worker_id,
            redis_backend_signature=failed_registration.backend_signature,
            connected_at=str(time.time()),
            projection_version=failed_opening.version + 1,
            user_agent=failed_registration.user_agent,
            client_ip=failed_registration.client_ip,
        )
        sharing_episode_ledger.record_registration_ack(
            failed_opening.episode_id, failed_ack,
        )
        sharing_episode_ledger.revoke(
            failed_opening.episode_id,
            expected_version=failed_opening.version,
            reason="cleanup_pending", cleanup_acknowledged=False,
        )
        recovery_order = []
        failed_opening_acknowledged = False
        from catchuparr import sharing_runtime as sharing_runtime_module

        original_retry_cleanup = sharing_runtime_module.retry_durable_open_cleanups
        original_native_observation = sharing_runtime_module.observe_native_connection

        def observe_after_channel_cleanup(*args, **kwargs):
            observed_channel = args[1] if len(args) > 1 else kwargs.get("channel_uuid")
            if str(observed_channel) == str(channel.uuid):
                _require(
                    failed_opening_acknowledged,
                    "Native viewer observation ran before failed-opening cleanup ACK",
                )
                if "native_observation" not in recovery_order:
                    recovery_order.append("native_observation")
            return original_native_observation(*args, **kwargs)

        def retry_channel_cleanup(*args, **kwargs):
            nonlocal failed_opening_acknowledged
            result = original_retry_cleanup(*args, **kwargs)
            if kwargs.get("channel_uuid") == str(channel.uuid):
                _ack_failed_opening_cleanup(
                    sharing_episode_ledger, channel.uuid,
                    failed_opening.episode_id, result,
                    acknowledged=failed_opening_acknowledged,
                )
                if not failed_opening_acknowledged:
                    failed_opening_acknowledged = True
                    recovery_order.append("cleanup_acknowledged")
            return result

        run_share = _RecorderTaskRun(
            worker_id, startup_timeout=45, media_idle_timeout=25,
        )
        with patch.object(
            sharing_runtime_module, "retry_durable_open_cleanups", retry_channel_cleanup,
        ), patch.object(
            sharing_runtime_module, "observe_native_connection", observe_after_channel_cleanup,
        ):
            run_share.start()
            harnesses.append(run_share)

            borrow_headers = None
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                with run_share._lock:
                    borrow_headers = next(
                        (dict(headers) for headers in run_share.input_headers
                         if "X-Catchuparr-Borrow" in headers),
                        None,
                    )
                if borrow_headers is not None:
                    break
                if run_share.failures:
                    raise RuntimeError("Sharing recorder failed before opening a native borrow")
                time.sleep(0.1)
        _require(borrow_headers is not None,
                 "Sharing recorder did not request the signed native-buffer route")
        _require(
            recovery_order[:2] == ["cleanup_acknowledged", "native_observation"],
            "New recorder did not recover the channel opening before observing its viewer",
        )
        _wait_for_native_client_count(
            redis_client, native_server, RedisKeys, worker_id, 2,
            expected_local=1, timeout=30,
        )
        from catchuparr import stats as catchuparr_stats
        from catchuparr.adapters.sharing_borrow_route import decode_borrow_capability

        capability_token = borrow_headers["X-Catchuparr-Borrow"]
        borrow_capability = decode_borrow_capability(capability_token)
        borrow_client_id = f"catchuparr-client-{borrow_capability.nonce}"
        _require(
            catchuparr_stats._is_recorder_client(worker_id, borrow_client_id),
            "The borrowed native reader was not tagged as a server-verified recorder",
        )
        _require(
            native_server.stream_managers.get(worker_id) is native_manager
            and native_manager.running
            and _redis_text(redis_client.get(RedisKeys.channel_owner(worker_id))) == native_owner
            and _native_source_metadata(redis_client, metadata_key) == source_metadata
            and {key: _key_dump(redis_client, key) for key in assignment_keys}
            == assignment_snapshot,
            "Borrowing changed native A ownership, source metadata, or assignments",
        )
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1
            and _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id],
            "Borrowing reserved a duplicate provider profile slot",
        )
        counts_borrowing, active_borrowing = source_server.snapshot()
        _require(
            counts_borrowing["source-a.ts"] == share_source_baseline["source-a.ts"]
            and counts_borrowing["source-b.ts"] == share_source_baseline["source-b.ts"]
            and active_borrowing["source-a.ts"] == 1
            and active_borrowing["source-b.ts"] == 0,
            "Borrowing opened another provider source instead of reusing native A",
        )
        borrowed_ids = {segment.id for segment in store.segments(worker_id)}
        borrowed_segments = _wait_for_indexed_tone(
            run_share, store, worker_id, ffmpeg, ffprobe,
            expected_hz=440, existing_ids=borrowed_ids, minimum=1, timeout=45,
            cooperative_sleep=gevent_sleep,
        )
        _require(bool(borrowed_segments), "Borrowed live A did not reach the archive")
        from aio_recorder_diagnostics_probe import probe_live_borrowed_diagnostic

        borrowed_diagnostic_cleanup = probe_live_borrowed_diagnostic(redis_client, worker_id)
        try:
            _assert_live_overview(
                archive_root, active_path, control_path, worker_id, "borrowed",
            )
        finally:
            borrowed_diagnostic_cleanup()

        # A consumed opening capability cannot be replayed into a second
        # reader, even while its first reader remains active.
        replay = client.get(
            f"/catchuparr/sharing/{channel.uuid}",
            HTTP_HOST="localhost",
            HTTP_X_CATCHUPARR_BORROW=capability_token,
        )
        _require(replay.status_code == 404,
                 "A consumed native-borrow capability opened a second reader")
        _wait_for_native_client_count(
            redis_client, native_server, RedisKeys, worker_id, 2,
            expected_local=1, timeout=10,
        )

        # Hide=true must hide only the signed borrow client from native Stats.
        from aio_stats_probe import probe_actual_recorder_stats

        probe_actual_recorder_stats(
            channel_id=worker_id,
            redis_client=redis_client,
            native_server=native_server,
            profile_a=profile_a,
            profile_b=profile_b,
            profile_connections_key=profile_connections_key,
            assignment_snapshot=assignment_snapshot,
            metadata_key=metadata_key,
        )

        # Add a second ordinary viewer, then remove the original. The borrow
        # must continue while that second viewer remains, so return is based on
        # actual ordinary membership rather than viewer count snapshots.
        churn_response = client.get(
            f"/proxy/ts/stream/{channel.uuid}",
            HTTP_HOST="localhost",
            HTTP_USER_AGENT="Synthetic sharing churn viewer",
        )
        _require(churn_response.status_code == 200,
                 "A second synthetic native viewer could not join the borrowed source")
        churn_session, _churn_buffer = _create_native_live_reader_session(
            churn_response, native_server, worker_id,
        )
        _wait_for_native_client_count(
            redis_client, native_server, RedisKeys, worker_id, 3,
            expected_local=2, timeout=20,
        )
        live_session.close()
        live_session = None
        live_reader = None
        live_response = None
        _wait_for_native_client_count(
            redis_client, native_server, RedisKeys, worker_id, 2,
            expected_local=1, timeout=15,
        )
        gevent_sleep(3)
        counts_with_churn, active_with_churn = source_server.snapshot()
        _require(
            counts_with_churn["source-b.ts"] == share_source_baseline["source-b.ts"]
            and active_with_churn["source-a.ts"] == 1,
            "The borrow returned while another ordinary native viewer remained",
        )
        churn_session.close()
        churn_session = None
        _wait_for_native_client_count(
            redis_client, native_server, RedisKeys, worker_id, 1,
            expected_local=0, timeout=15,
        )
        sharing_episode_key = f"catchuparr:sharing:episode:{worker_id}"
        wait_return_until = time.monotonic() + 10
        while _redis_text(redis_client.hget(sharing_episode_key, "phase")) != "wait_return":
            _require(time.monotonic() < wait_return_until,
                     "Native borrow did not enter viewer-return grace")
            gevent_sleep(0.05)
        attached_clock = float(redis_client.hget(sharing_episode_key, "last_switch_at"))
        return_deadline = float(redis_client.hget(sharing_episode_key, "return_deadline"))
        _require(
            return_deadline >= attached_clock + 59.9,
            "Native viewer grace did not preserve the full switch interval from ATTACH",
        )
        _require(time.time() < return_deadline,
                 "The synthetic viewer left after the tested switch deadline")
        first_b_boundary = {}

        def verify_first_b_effect(source_name, request_number, started_at):
            if (source_name != "source-b.ts"
                    or request_number != share_b_request_baseline + 1):
                return False
            first_b_boundary["started_at"] = started_at
            first_b_boundary["return_deadline"] = return_deadline
            _require(
                started_at >= return_deadline,
                "The allowed B provider request preceded the actual native return deadline",
            )
            # This callback runs at the provider request boundary, before the
            # synthetic source sends its first byte. A later polling snapshot
            # cannot stand in for the state at the first fallback effect.
            with run_share._lock:
                processes_at_boundary = list(run_share.processes)
                results_at_boundary = list(run_share.results)
            _require(
                bool(processes_at_boundary)
                and processes_at_boundary[0].poll() is not None,
                "Borrowed FFmpeg was still alive when the first B request started",
            )
            _require(
                bool(results_at_boundary)
                and results_at_boundary[0].useful_segments > 0,
                "Borrowed input had not completed with useful media before the first B request",
            )
            borrow_client_key = f"live:channel:{worker_id}:clients:{borrow_client_id}"
            borrow_clients_key = RedisKeys.clients(worker_id)
            borrow_private_key = f"catchuparr:sharing:private:{worker_id}"
            _require(
                not redis_client.exists(borrow_client_key)
                and not redis_client.sismember(borrow_clients_key, borrow_client_id)
                and redis_client.hget(borrow_private_key, borrow_client_id) is None,
                "Borrowed native client hash/set/marker remained at the first B provider request",
            )
            _require(
                _redis_text(redis_client.hget(sharing_episode_key, "phase")) == "closed",
                "Borrowed episode cleanup was not acknowledged before the first B provider request",
            )
            return True

        source_server.install_request_start_callback(verify_first_b_effect)
        first_return_request_at = _wait_for_allowed_source_request(
            run_share,
            lambda: source_server.request_times("source-b.ts"),
            share_b_request_baseline,
            timeout=150,
            sleep=gevent_sleep,
        )
        callback_deadline = time.monotonic() + 5
        while not source_server.request_callback_finished.is_set():
            _require(time.monotonic() < callback_deadline,
                     "Provider request boundary check did not complete")
            gevent_sleep(0.01)
        source_server.clear_request_start_callback(verify_first_b_effect)
        _require(
            first_return_request_at >= return_deadline,
            "The allowed B provider request preceded the actual native return deadline",
        )
        _require(not source_server.request_start_callback_errors,
                 "The first B provider effect failed its borrowed-cleanup boundary checks")
        _require(first_b_boundary.get("started_at") == first_return_request_at,
                 "The first B request was not checked synchronously at its provider boundary")

        b_before_share = {segment.id for segment in store.segments(worker_id)}
        returned_segments = _wait_for_indexed_tone(
            run_share, store, worker_id, ffmpeg, ffprobe,
            expected_hz=880, existing_ids=b_before_share, minimum=1, timeout=150,
            cooperative_sleep=gevent_sleep,
        )
        _require(bool(returned_segments), "Recorder did not return to allowed source B")
        from aio_recorder_diagnostics_probe import probe_live_dedicated_diagnostic

        dedicated_diagnostic_cleanup = probe_live_dedicated_diagnostic(redis_client, worker_id)
        try:
            _assert_live_overview(
                archive_root, active_path, control_path, worker_id, "dedicated",
            )
        finally:
            dedicated_diagnostic_cleanup()
        with run_share._lock:
            share_results = list(run_share.results)
        _require(
            bool(share_results)
            and share_results[0].useful_segments > 0
            and share_results[0].status in {"switch_requested", "media_stalled", "exited"},
            "The borrowed recorder did not finish its useful A input before returning",
        )

        from apps.proxy.live_proxy.services.channel_service import ChannelService

        def wait_for_native_a_teardown(previous_borrow_client_id: str) -> None:
            """Wait for the exact borrowed A owner and its reservation to release."""
            shutdown_bound = _native_teardown_readiness_timeout(
                float(ConfigHelper.channel_shutdown_delay()),
                float(ConfigHelper.cleanup_check_interval()),
                # Conservatively allow both source-defined stop-marker phases:
                # 60 seconds from StreamManager failure and 30 seconds from
                # normal owner release, plus the configured disconnect/cleanup
                # phases below. Do not infer completion from provider count.
                stopping_marker_ttl=90,
            )

            def snapshot():
                state = _redis_text(redis_client.hget(
                    RedisKeys.channel_metadata(worker_id), ChannelMetadataField.STATE,
                ))
                profile_a_count = _profile_count(
                    redis_client, profile_a.id, profile_connections_key,
                )
                provider_counts, provider_active = source_server.snapshot()
                local_manager = native_server.client_managers.get(worker_id)
                borrowed_hash = (
                    f"live:channel:{worker_id}:clients:{previous_borrow_client_id}"
                )
                borrowed_absent = (
                    not redis_client.exists(borrowed_hash)
                    and not redis_client.sismember(
                        RedisKeys.clients(worker_id), previous_borrow_client_id,
                    )
                    and redis_client.hget(
                        f"catchuparr:sharing:private:{worker_id}",
                        previous_borrow_client_id,
                    ) is None
                )
                conditions = _native_teardown_ready_conditions(
                    route_available=(
                        not ChannelService.is_channel_unavailable_for_new_clients(worker_id)
                    ),
                    stopping_marker_absent=not redis_client.exists(
                        RedisKeys.channel_stopping(worker_id),
                    ),
                    metadata_not_stopping=(
                        state != _constant_text(ChannelState.STOPPING)
                    ),
                    owner_absent=(
                        redis_client.get(RedisKeys.channel_owner(worker_id)) is None
                    ),
                    clients_zero=(
                        int(redis_client.scard(RedisKeys.clients(worker_id)) or 0) == 0
                    ),
                    managers_absent=(
                        worker_id not in native_server.stream_managers
                        and worker_id not in native_server._live_stream_managers
                        and local_manager is None
                    ),
                    source_inactive=provider_active["source-a.ts"] == 0,
                    profile_at_baseline=(
                        profile_a_count == profile_baselines[profile_a.id]
                    ),
                    previous_borrow_absent=borrowed_absent,
                )
                return {
                    "conditions": conditions,
                    "metadata_state_stopping": (
                        state == _constant_text(ChannelState.STOPPING)
                    ),
                    "metadata_state_error": (
                        state == _constant_text(ChannelState.ERROR)
                    ),
                    "provider_a": {
                        "active": provider_active["source-a.ts"],
                        "requests": provider_counts["source-a.ts"],
                    },
                    "native": {
                        "global_clients": int(
                            redis_client.scard(RedisKeys.clients(worker_id)) or 0
                        ),
                        "owner_present": redis_client.get(
                            RedisKeys.channel_owner(worker_id),
                        ) is not None,
                        "stream_manager_present": (
                            worker_id in native_server.stream_managers
                        ),
                        "live_stream_manager_present": (
                            worker_id in native_server._live_stream_managers
                        ),
                    },
                    "profile_a": {
                        "current": profile_a_count,
                        "baseline": profile_baselines[profile_a.id],
                    },
                    "previous_borrow_registration_absent": borrowed_absent,
                }

            ready, last_snapshot = _wait_for_readiness_snapshot(
                snapshot,
                shutdown_bound,
                monotonic=time.monotonic,
                sleep=gevent_sleep,
                poll_interval=0.1,
            )
            if ready:
                return

            def readiness_failure_diagnostic():
                diagnostic = dict(last_snapshot)
                manager = native_server.client_managers.get(worker_id)
                diagnostic["native_local_clients"] = (
                    int(manager.get_client_count()) if manager is not None else 0
                )
                diagnostic["native"]["stopping_marker_ttl_seconds"] = int(
                    redis_client.ttl(RedisKeys.channel_stopping(worker_id))
                )
                diagnostic["native"]["last_disconnect_ttl_seconds"] = int(
                    redis_client.ttl(RedisKeys.last_client_disconnect(worker_id))
                )
                return diagnostic

            _require_with_failure_diagnostic(
                False,
                "Native A teardown did not finish after its last borrowed reader",
                readiness_failure_diagnostic,
                diagnostic_label="CATCHUPARR_NATIVE_REOPEN_DIAGNOSTIC",
            )

        # The native last-client disconnect releases profile capacity after
        # its configured shutdown and cleanup phases. Wait for that exact
        # teardown before asserting the provider/profile handoff to B. This
        # readiness gate does not require the independent B source to stop.
        wait_for_native_a_teardown(borrow_client_id)
        counts_after_share_return, active_after_share_return = source_server.snapshot()
        _require(
            counts_after_share_return["source-b.ts"]
            == share_source_baseline["source-b.ts"] + 1
            and active_after_share_return["source-b.ts"] == 1
            and counts_after_share_return["source-a.ts"]
            == share_source_baseline["source-a.ts"]
            and active_after_share_return["source-a.ts"] == 0,
            "Return did not use one allowed B connection after releasing borrowed A",
        )

        profile_a_return_count = _profile_count(
            redis_client, profile_a.id, profile_connections_key,
        )
        profile_b_return_count = _profile_count(
            redis_client, profile_b.id, profile_connections_key,
        )
        profile_return_conditions = {
            "profile_a_at_baseline": (
                profile_a_return_count == profile_baselines[profile_a.id]
            ),
            "profile_b_has_one_allowed_reservation": (
                profile_b_return_count == profile_baselines[profile_b.id] + 1
            ),
        }

        def borrow_return_diagnostic():
            manager = native_server.client_managers.get(worker_id)
            local_client_count = (
                int(manager.get_client_count()) if manager is not None else 0
            )
            return {
                "conditions": profile_return_conditions,
                "profile_reservations": {
                    "a": {
                        "current": profile_a_return_count,
                        "baseline": profile_baselines[profile_a.id],
                    },
                    "b": {
                        "current": profile_b_return_count,
                        "expected": profile_baselines[profile_b.id] + 1,
                    },
                },
                "provider": {
                    "a_active": active_after_share_return["source-a.ts"],
                    "a_requests": counts_after_share_return["source-a.ts"],
                    "b_active": active_after_share_return["source-b.ts"],
                    "b_requests": counts_after_share_return["source-b.ts"],
                    "b_expected_requests": share_source_baseline["source-b.ts"] + 1,
                },
                "native": {
                    "global_clients": int(
                        redis_client.scard(RedisKeys.clients(worker_id)) or 0
                    ),
                    "local_clients": local_client_count,
                    "owner_present": redis_client.get(
                        RedisKeys.channel_owner(worker_id),
                    ) is not None,
                    "disconnect_present": bool(redis_client.exists(
                        RedisKeys.last_client_disconnect(worker_id),
                    )),
                },
                "recorder": {
                    "useful_segments": int(share_results[0].useful_segments),
                    "status": str(share_results[0].status),
                },
            }

        def require_borrow_return_gate():
            _require_with_failure_diagnostic(
                all(profile_return_conditions.values()),
                "Borrow return did not release A and reserve only the allowed B profile",
                borrow_return_diagnostic,
                diagnostic_label="CATCHUPARR_BORROW_RETURN_DIAGNOSTIC",
            )

        def prepare_dedicated_b():
            # Establish the dedicated B fixture under sharing-OFF before opening
            # the next ordinary A viewer. With sharing left enabled, live_stability
            # is zero and the long A startup/validation path can legitimately let
            # the supervisor migrate B onto A before the source-removal assertion.
            # This Apply boundary keeps the later B-active prerequisite stable
            # without changing its gate or extending any recorder deadline.
            old_dedicated_b_records = [
                record for record in _worker_records(redis_client, worker_id)
                if record.get("state") == "active"
                and record.get("reservation_state") == "reserved"
                and record.get("stream_id") == str(stream_b.id)
                and record.get("profile_id") == str(profile_b.id)
            ]
            _require(
                len(old_dedicated_b_records) == 1
                and old_dedicated_b_records[0].get("worker_id")
                and old_dedicated_b_records[0].get("capability_digest")
                and old_dedicated_b_records[0].get("reservation_id"),
                "Sharing-enabled B phase lacked one exact active worker before reconfiguration",
            )
            old_dedicated_b = old_dedicated_b_records[0]
            old_dedicated_b_worker = str(old_dedicated_b["worker_id"])
            old_dedicated_b_digest = str(old_dedicated_b["capability_digest"])
            old_dedicated_b_reservation = str(old_dedicated_b["reservation_id"])
            dedicated_settings = global_sharing_draft(
                sharing_settings, enabled=False, live_stability_seconds=1,
                last_viewer_return_seconds=2, min_switch_interval_seconds=60,
            )
            dedicated_plugin = PluginConfig.objects.get(key="catchuparr")
            dedicated_plugin.settings = dict(dedicated_settings)
            dedicated_plugin.save(update_fields=("settings",))
            applied_dedicated = catchuparr_runtime.apply_configuration(dedicated_settings)
            _require(
                applied_dedicated.get("applied") is True,
                "Unified Apply rejected the sharing-off B preparation phase",
            )
            active_dedicated = load_active_configuration(active_path)
            assert_applied_global_sharing(
                active_dedicated, worker_id, enabled=False, live_stability_seconds=1,
                last_viewer_return_seconds=2, min_switch_interval_seconds=60,
            )
            _require(
                active_dedicated is not None
                and active_dedicated["connection_sharing"][worker_id]["enabled"] is False
                and [str(item.get("id")) for item in ranked_source_candidates(
                    worker_id, active_dedicated,
                )] == [str(stream_b.id)],
                "Sharing-off preparation did not preserve the assigned B fallback",
            )
            run_share.stop()
            run_share.join(timeout=30, cooperative_sleep=gevent_sleep)
            old_dedicated_b_request_count = len(
                source_server.request_times("source-b.ts"),
            )

            def old_dedicated_b_cleanup_snapshot():
                matching = [
                    record for record in _worker_records(redis_client, worker_id)
                    if record.get("worker_id") == old_dedicated_b_worker
                ]
                record = matching[0] if len(matching) == 1 else None
                counts, active_counts = source_server.snapshot()
                return {
                    "conditions": {
                        "attempt_binding_preserved": bool(
                            record
                            and record.get("capability_digest") == old_dedicated_b_digest
                            and record.get("reservation_id") == old_dedicated_b_reservation
                        ),
                        "attempt_released": bool(
                            record
                            and record.get("state") in {"failed", "released"}
                            and record.get("reservation_state") in {"none", "released"}
                        ),
                        "attempt_credentials_absent": (
                            not redis_client.exists(CAPABILITY_PREFIX + old_dedicated_b_digest)
                            and not redis_client.exists(
                                reservation_credential_marker_key(
                                    old_dedicated_b_reservation,
                                ),
                            )
                        ),
                        "native_manager_absent": (
                            old_dedicated_b_worker not in native_server.stream_managers
                            and old_dedicated_b_worker not in native_server._live_stream_managers
                            and old_dedicated_b_worker not in native_server.client_managers
                        ),
                        "native_owner_absent": (
                            redis_client.get(
                                RedisKeys.channel_owner(old_dedicated_b_worker),
                            ) is None
                        ),
                        "native_clients_absent": (
                            int(redis_client.scard(
                                RedisKeys.clients(old_dedicated_b_worker),
                            ) or 0) == 0
                        ),
                        "native_stopping_marker_absent": not redis_client.exists(
                            RedisKeys.channel_stopping(old_dedicated_b_worker),
                        ),
                        "provider_b_inactive": active_counts["source-b.ts"] == 0,
                        "provider_b_requests_stable": (
                            counts["source-b.ts"] == old_dedicated_b_request_count
                            and len(source_server.request_times("source-b.ts"))
                            == old_dedicated_b_request_count
                        ),
                        "profile_b_at_original_baseline": (
                            _profile_count(
                                redis_client, profile_b.id, profile_connections_key,
                            ) == profile_baselines[profile_b.id]
                        ),
                    },
                    "worker_state": _allowlisted_state(
                        record.get("state") if record else None,
                        frozenset({
                            "issued", "reserving", "reserved", "starting", "active",
                            "releasing", "released", "release_failed", "failed",
                        }),
                    ),
                    "reservation_state": _allowlisted_state(
                        record.get("reservation_state") if record else None,
                        frozenset({
                            "none", "reserving", "reserved", "releasing",
                            "released", "release_failed",
                        }),
                    ),
                }

            old_dedicated_b_timeout = _native_teardown_readiness_timeout(
                float(ConfigHelper.channel_shutdown_delay()),
                float(ConfigHelper.cleanup_check_interval()),
                stopping_marker_ttl=90,
            )
            def start_dedicated_b_after_ack():
                request_baseline = len(
                    source_server.request_times("source-b.ts"),
                )
                segment_baseline = {
                    segment.id for segment in store.segments(worker_id)
                }
                run = _RecorderTaskRun(
                    worker_id, startup_timeout=45, media_idle_timeout=25,
                )
                run.start()
                harnesses.append(run)
                return run, request_baseline, segment_baseline

            (
                run_dedicated_b,
                dedicated_b_request_baseline,
                dedicated_b_segment_baseline,
            ) = _run_after_readiness_ack(
                old_dedicated_b_cleanup_snapshot,
                start_dedicated_b_after_ack,
                old_dedicated_b_timeout,
                message=(
                    "Prior B attempt had not received exact native/profile release acknowledgment"
                ),
                diagnostic_label="CATCHUPARR_DEDICATED_B_PREPARATION_DIAGNOSTIC",
                monotonic=time.monotonic,
                sleep=gevent_sleep,
                poll_interval=0.1,
            )
            dedicated_b_segments = _wait_for_indexed_tone(
                run_dedicated_b, store, worker_id, ffmpeg, ffprobe,
                expected_hz=880, existing_ids=dedicated_b_segment_baseline,
                minimum=1, timeout=90, cooperative_sleep=gevent_sleep,
            )
            _require(bool(dedicated_b_segments),
                     "Sharing-off B preparation did not index fresh B media")
            dedicated_b_counts, dedicated_b_active = source_server.snapshot()
            dedicated_b_records = [
                record for record in _worker_records(redis_client, worker_id)
                if record.get("state") == "active"
                and record.get("reservation_state") == "reserved"
                and record.get("stream_id") == str(stream_b.id)
                and record.get("profile_id") == str(profile_b.id)
            ]
            _require(
                dedicated_b_counts["source-b.ts"] == dedicated_b_request_baseline + 1
                and dedicated_b_active["source-b.ts"] == 1
                and dedicated_b_active["source-a.ts"] == 0
                and _profile_count(
                    redis_client, profile_b.id, profile_connections_key,
                ) == profile_baselines[profile_b.id] + 1
                and len(dedicated_b_records) == 1
                and dedicated_b_records[0].get("capability_digest")
                and dedicated_b_records[0].get("reservation_id"),
                "Sharing-off preparation did not establish one exact active B worker",
            )
            # The source-removal phase below joins this task after its enabled
            # no-source Apply, while the exact active B bindings are captured.
            return run_dedicated_b

        run_dedicated_b = _run_after_required_gate(
            require_borrow_return_gate, prepare_dedicated_b,
        )
        run_share = run_dedicated_b

        # Remove all dedicated sources while an ordinary A viewer is present.
        # The new task must still borrow the existing A reader, close it at the
        # native grace deadline, persist PAUSED, and resume on B after Apply
        # restores an allowed source.
        no_source_response = client.get(
            f"/proxy/ts/stream/{channel.uuid}",
            HTTP_HOST="localhost",
            HTTP_USER_AGENT="Synthetic no-source ordinary viewer",
        )
        _require(no_source_response.status_code == 200,
                 "No-source ordinary A viewer could not open")
        live_session, no_source_native_buffer = _create_native_live_reader_session(
            no_source_response, native_server, worker_id,
        )
        live_reader = live_session.reader
        _wait_for_native_client_count(
            redis_client, native_server, RedisKeys, worker_id, 1, timeout=20,
        )
        no_source_a_client_ids = {
            _redis_text(client_id)
            for client_id in redis_client.smembers(RedisKeys.clients(worker_id))
        }
        _require(
            len(no_source_a_client_ids) == 1,
            "Ordinary A viewer did not have one exact native registration",
        )
        no_source_a_client_id = next(iter(no_source_a_client_ids))
        no_source_a_client_key = (
            f"live:channel:{worker_id}:clients:{no_source_a_client_id}"
        )
        no_source_a_connected_at = _redis_text(
            redis_client.hget(no_source_a_client_key, "connected_at"),
        )
        no_source_a_client_worker = _redis_text(
            redis_client.hget(no_source_a_client_key, "worker_id"),
        )
        _require(
            no_source_a_connected_at and no_source_a_client_worker,
            "Ordinary A client registration did not expose its stable identity fields",
        )
        no_source_native_owner, no_source_native_manager, no_source_client_manager = (
            _wait_for_native_active(
                redis_client, native_server, worker_id, redis_keys=RedisKeys,
                metadata_field=ChannelMetadataField,
                channel_state=ChannelState, timeout=45,
            )
        )
        _require(
            _native_client_identity_matches(
                {
                    _redis_text(client_id)
                    for client_id in redis_client.smembers(RedisKeys.clients(worker_id))
                },
                expected_client_id=no_source_a_client_id,
                connected_at=_redis_text(redis_client.hget(
                    no_source_a_client_key, "connected_at",
                )),
                expected_connected_at=no_source_a_connected_at,
                worker_id=_redis_text(redis_client.hget(
                    no_source_a_client_key, "worker_id",
                )),
                expected_worker_id=no_source_a_client_worker,
            ),
            "Ordinary A registration changed before reaching ACTIVE",
        )
        no_source_first_media, _no_source_first_index = _read_after_current_buffer_head(
            live_session, no_source_native_buffer,
            minimum_bytes=188 * 100, timeout=20,
        )
        _verify_audio_video_tone(
            ffmpeg, ffprobe, no_source_first_media, 440,
            "Ordinary live A before dedicated-source removal",
        )
        no_source_a_counts, no_source_a_active = source_server.snapshot()
        no_source_a_profile_count = _profile_count(
            redis_client, profile_a.id, profile_connections_key,
        )
        no_source_a_assignments = {
            key: _key_dump(redis_client, key) for key in assignment_keys
        }
        no_source_a_metadata = _native_source_metadata(redis_client, metadata_key)
        _require(
            no_source_a_active["source-a.ts"] == 1
            and no_source_a_profile_count == profile_baselines[profile_a.id] + 1
            and no_source_native_manager.running
            and no_source_client_manager.get_client_count() == 1
            and no_source_a_connected_at
            and no_source_a_client_worker,
            "Ordinary A viewer did not reach ACTIVE with its exact provider reservation",
        )
        live_dedicated_b_records = [
            record for record in _worker_records(redis_client, worker_id)
            if record.get("state") == "active"
            and record.get("reservation_state") == "reserved"
            and record.get("stream_id") == str(stream_b.id)
            and record.get("profile_id") == str(profile_b.id)
        ]
        _require(
            len(live_dedicated_b_records) == 1
            and no_source_a_active["source-b.ts"] == 1
            and _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id] + 1,
            "Dedicated B worker was not active before source-removal Apply",
        )
        no_source_b_record = live_dedicated_b_records[0]
        no_source_b_worker_id = no_source_b_record.get("worker_id")
        _require(
            no_source_b_worker_id and no_source_b_worker_id != worker_id,
            "Dedicated B worker identity was not available for the removal check",
        )
        no_source_b_requests = len(source_server.request_times("source-b.ts"))
        no_source_b_reservation_id = no_source_b_record.get("reservation_id")
        no_source_b_capability_digest = no_source_b_record.get("capability_digest")
        _require(
            isinstance(no_source_b_reservation_id, str) and no_source_b_reservation_id
            and isinstance(no_source_b_capability_digest, str)
            and no_source_b_capability_digest
            and redis_client.exists(CAPABILITY_PREFIX + no_source_b_capability_digest),
            "Dedicated B worker did not expose its active reservation and capability bindings",
        )
        no_source_settings = dict(sharing_settings)
        no_source_settings["filter_config"] = (
            "version: 1\n"
            f"profile: {profile_name}\n"
            "rules:\n"
            "  - channels: {profile: all}\n"
            f'    exclude: ["{account_a.name}", "{account_b.name}"]\n'
        )
        no_source_plugin = PluginConfig.objects.get(key="catchuparr")
        no_source_plugin.settings = dict(no_source_settings)
        no_source_plugin.save(update_fields=("settings",))
        applied_no_source = catchuparr_runtime.apply_configuration(no_source_settings)
        _require(applied_no_source.get("applied") is True,
                 "Unified Apply rejected the no-allowed-source policy")
        active_no_source = load_active_configuration(active_path)
        assert_applied_global_sharing(
            active_no_source, worker_id, enabled=True, live_stability_seconds=1,
            last_viewer_return_seconds=2, min_switch_interval_seconds=60,
        )
        _require(
            active_no_source is not None
            and active_no_source["connection_sharing"][worker_id]["enabled"] is True
            and ranked_source_candidates(worker_id, active_no_source) == [],
            "No-source policy did not leave the channel eligible with zero dedicated candidates",
        )
        run_share.join_after_supervisor(timeout=30, cooperative_sleep=gevent_sleep)
        no_source_release_timeout = _native_teardown_readiness_timeout(
            float(ConfigHelper.channel_shutdown_delay()),
            float(ConfigHelper.cleanup_check_interval()),
            stopping_marker_ttl=90,
        )
        def no_source_transition_snapshot():
            current_counts, current_active = source_server.snapshot()
            current_a_profile = _profile_count(
                redis_client, profile_a.id, profile_connections_key,
            )
            current_b_profile = _profile_count(
                redis_client, profile_b.id, profile_connections_key,
            )
            matching_b_records = [
                record for record in _worker_records(redis_client, worker_id)
                if record.get("worker_id") == no_source_b_worker_id
            ]
            b_record = matching_b_records[0] if len(matching_b_records) == 1 else None
            b_worker_released = bool(
                b_record
                and b_record.get("state") in {"released", "failed"}
                and b_record.get("reservation_state") in {"released", "none"}
            )
            b_reservation_released = (
                b_record is not None
                and (no_source_b_reservation_id is None
                     or b_record.get("reservation_id") == no_source_b_reservation_id)
            )
            b_binding_preserved = bool(
                b_record
                and b_record.get("reservation_id") == no_source_b_reservation_id
                and b_record.get("capability_digest") == no_source_b_capability_digest
            )
            markers_absent = (
                not redis_client.exists(CAPABILITY_PREFIX + no_source_b_capability_digest)
                and not redis_client.exists(
                    reservation_credential_marker_key(no_source_b_reservation_id),
                )
            )
            state = _redis_text(redis_client.hget(
                metadata_key, ChannelMetadataField.STATE,
            ))
            manager = native_server.stream_managers.get(worker_id)
            client_manager = native_server.client_managers.get(worker_id)
            owner = _redis_text(redis_client.get(RedisKeys.channel_owner(worker_id)))
            conditions = _source_removal_transition_conditions(
                a_owner_preserved=(
                    owner == no_source_native_owner
                    and _redis_text(native_server.worker_id) == no_source_native_owner
                ),
                a_active=(
                    state == _constant_text(ChannelState.ACTIVE)
                    and manager is no_source_native_manager
                    and bool(getattr(manager, "running", False))
                ),
                a_client_preserved=(
                    int(redis_client.scard(RedisKeys.clients(worker_id)) or 0) == 1
                    and client_manager is no_source_client_manager
                    and client_manager is not None
                    and int(client_manager.get_client_count()) == 1
                    and bool(redis_client.exists(no_source_a_client_key))
                    and _native_client_identity_matches(
                        {
                            _redis_text(client_id)
                            for client_id in redis_client.smembers(RedisKeys.clients(worker_id))
                        },
                        expected_client_id=no_source_a_client_id,
                        connected_at=_redis_text(redis_client.hget(
                            no_source_a_client_key, "connected_at",
                        )),
                        expected_connected_at=no_source_a_connected_at,
                        worker_id=_redis_text(redis_client.hget(
                            no_source_a_client_key, "worker_id",
                        )),
                        expected_worker_id=no_source_a_client_worker,
                    )
                ),
                a_provider_preserved=(
                    current_active["source-a.ts"] == 1
                    and current_counts["source-a.ts"] == no_source_a_counts["source-a.ts"]
                ),
                a_profile_preserved=(
                    current_a_profile == no_source_a_profile_count
                ),
                a_assignments_preserved=(
                    {key: _key_dump(redis_client, key) for key in assignment_keys}
                    == no_source_a_assignments
                    and _native_source_metadata(redis_client, metadata_key)
                    == no_source_a_metadata
                ),
                b_worker_released=b_worker_released,
                b_native_resources_absent=(
                    no_source_b_worker_id not in native_server.stream_managers
                    and no_source_b_worker_id not in native_server._live_stream_managers
                    and no_source_b_worker_id not in native_server.client_managers
                    and redis_client.get(
                        RedisKeys.channel_owner(no_source_b_worker_id),
                    ) is None
                    and int(redis_client.scard(
                        RedisKeys.clients(no_source_b_worker_id),
                    ) or 0) == 0
                    and not redis_client.exists(
                        RedisKeys.channel_stopping(no_source_b_worker_id),
                    )
                    and _redis_text(redis_client.hget(
                        RedisKeys.channel_metadata(no_source_b_worker_id),
                        ChannelMetadataField.STATE,
                    )) != _constant_text(ChannelState.STOPPING)
                ),
                b_binding_preserved=b_binding_preserved,
                b_reservation_released=b_reservation_released,
                b_private_markers_absent=markers_absent,
                b_provider_inactive=current_active["source-b.ts"] == 0,
                b_request_count_preserved=(
                    len(source_server.request_times("source-b.ts")) == no_source_b_requests
                    and current_counts["source-b.ts"] == no_source_a_counts["source-b.ts"]
                ),
                b_profile_released=(
                    current_b_profile == profile_baselines[profile_b.id]
                ),
            )
            return {
                "conditions": conditions,
                "a_provider": {
                    "active": current_active["source-a.ts"],
                    "requests": current_counts["source-a.ts"],
                },
                "b_provider": {
                    "active": current_active["source-b.ts"],
                    "requests": current_counts["source-b.ts"],
                },
                "a_profile": current_a_profile,
                "b_profile": current_b_profile,
                "b_worker_state": _allowlisted_state(
                    b_record.get("state") if b_record else None,
                    frozenset({
                        "issued", "reserving", "reserved", "starting", "active",
                        "releasing", "released", "release_failed", "failed",
                    }),
                ),
                "b_reservation_state": (
                    _allowlisted_state(
                        b_record.get("reservation_state") if b_record else None,
                        frozenset({
                            "none", "reserving", "reserved", "releasing",
                            "released", "release_failed",
                        }),
                    )
                ),
                "native_a_active": conditions["ordinary_a_active"],
            }

        no_source_ready, no_source_last_snapshot, no_source_wait_reason = (
            _wait_for_source_removal_transition(
            no_source_transition_snapshot, no_source_release_timeout,
            monotonic=time.monotonic, sleep=gevent_sleep, poll_interval=0.1,
            )
        )
        _require_with_failure_diagnostic(
            no_source_ready,
            "Removing dedicated sources disturbed live A or retained B's reservation",
            lambda: dict(no_source_last_snapshot, wait_reason=no_source_wait_reason),
            diagnostic_label="CATCHUPARR_NO_SOURCE_TRANSITION_DIAGNOSTIC",
        )
        no_source_counts, no_source_active = source_server.snapshot()
        run_no_source = _RecorderTaskRun(
            worker_id, startup_timeout=45, media_idle_timeout=25,
        )
        run_no_source.start()
        harnesses.append(run_no_source)
        no_source_borrow_headers = None
        no_source_borrow_deadline = time.monotonic() + 90
        while time.monotonic() < no_source_borrow_deadline:
            with run_no_source._lock:
                no_source_borrow_headers = next(
                    (dict(headers) for headers in run_no_source.input_headers
                     if "X-Catchuparr-Borrow" in headers),
                    None,
                )
            if no_source_borrow_headers is not None:
                break
            if run_no_source.failures:
                raise RuntimeError("No-source task failed before borrowing live A")
            gevent_sleep(0.1)
        _require(no_source_borrow_headers is not None,
                 "Recorder did not borrow the existing A viewer with no dedicated sources")
        _wait_for_native_client_count(
            redis_client, native_server, RedisKeys, worker_id, 2,
            expected_local=1, timeout=30,
        )
        no_source_capability = decode_borrow_capability(
            no_source_borrow_headers["X-Catchuparr-Borrow"],
        )
        no_source_client_id = f"catchuparr-client-{no_source_capability.nonce}"
        no_source_old_segments = {segment.id for segment in store.segments(worker_id)}
        no_source_media = _wait_for_indexed_tone(
            run_no_source, store, worker_id, ffmpeg, ffprobe,
            expected_hz=440, existing_ids=no_source_old_segments,
            minimum=1, timeout=45, cooperative_sleep=gevent_sleep,
        )
        _require(bool(no_source_media), "No-source borrow did not archive live A media")
        _require(
            len(source_server.request_times("source-b.ts")) == no_source_b_requests
            and source_server.snapshot()[1]["source-a.ts"] == 1,
            "No-source borrowing opened another provider connection or duplicated A",
        )
        live_session.close()
        live_session = None
        live_reader = None
        no_source_episode_key = f"catchuparr:sharing:episode:{worker_id}"
        no_source_grace_deadline = time.monotonic() + 10
        while _redis_text(redis_client.hget(no_source_episode_key, "phase")) != "wait_return":
            _require(time.monotonic() < no_source_grace_deadline,
                     "No-source borrow did not enter native viewer grace")
            gevent_sleep(0.05)
        no_source_return_deadline = float(
            redis_client.hget(no_source_episode_key, "return_deadline")
        )
        from catchuparr.sharing_persistence import SharingIntentLedger

        no_source_ledger = SharingIntentLedger(
            archive_root / ".catchuparr-sharing.sqlite3",
        )
        no_source_cleanup_deadline = time.monotonic() + 150
        no_source_record = None
        while time.monotonic() < no_source_cleanup_deadline:
            no_source_projection_phase = _redis_text(
                redis_client.hget(no_source_episode_key, "phase")
            )
            no_source_client_key = (
                f"live:channel:{worker_id}:clients:{no_source_client_id}"
            )
            no_source_private_key = f"catchuparr:sharing:private:{worker_id}"
            no_source_record = no_source_ledger.read(worker_id)
            if (
                no_source_projection_phase == "closed"
                and not redis_client.exists(no_source_client_key)
                and not redis_client.sismember(
                    RedisKeys.clients(worker_id), no_source_client_id,
                )
                and redis_client.hget(no_source_private_key, no_source_client_id) is None
                and no_source_record is not None
                and no_source_record.state.phase.value == "paused"
            ):
                break
            gevent_sleep(0.1)
        pause_preconditions = {
            "episode_closed": no_source_projection_phase == "closed",
            "borrow_hash_absent": not redis_client.exists(no_source_client_key),
            "borrow_membership_absent": not redis_client.sismember(
                RedisKeys.clients(worker_id), no_source_client_id,
            ),
            "borrow_marker_absent": redis_client.hget(
                f"catchuparr:sharing:private:{worker_id}", no_source_client_id,
            ) is None,
            "ledger_paused": (
                no_source_record is not None
                and no_source_record.state.phase.value == "paused"
            ),
            "grace_deadline_elapsed": time.time() >= no_source_return_deadline,
            "task_completed_cleanly": not run_no_source.failures,
        }
        # PAUSED is acknowledged before Dispatcharr finishes its asynchronous
        # last-owner/profile teardown. Use the same A-scoped readiness contract
        # as the return and reopen boundaries; B remains independently checked
        # by the exact zero-request/profile assertions below.
        def read_final_no_source_conditions():
            final_counts, final_active = source_server.snapshot()
            pause_conditions = {
                "source_b_request_count_unchanged": (
                    len(source_server.request_times("source-b.ts")) == no_source_b_requests
                ),
                "source_a_active_zero": final_active["source-a.ts"] == 0,
                "source_a_request_count_unchanged": (
                    final_counts["source-a.ts"] == no_source_counts["source-a.ts"]
                ),
                "native_global_clients_zero": (
                    int(redis_client.scard(RedisKeys.clients(worker_id)) or 0) == 0
                ),
                "native_owner_absent": redis_client.get(
                    RedisKeys.channel_owner(worker_id),
                ) is None,
                "profile_a_at_baseline": (
                    _profile_count(redis_client, profile_a.id, profile_connections_key)
                    == profile_baselines[profile_a.id]
                ),
                "profile_b_at_baseline": (
                    _profile_count(redis_client, profile_b.id, profile_connections_key)
                    == profile_baselines[profile_b.id]
                ),
            }
            return final_counts, final_active, pause_conditions

        (
            final_no_source_counts,
            final_no_source_active,
            no_source_pause_conditions,
        ) = _wait_for_native_teardown_after_pause(
            pause_preconditions,
            lambda: wait_for_native_a_teardown(no_source_client_id),
            read_final_no_source_conditions,
        )
        disconnect_key = RedisKeys.last_client_disconnect(worker_id)

        def no_source_pause_diagnostic():
            from apps.proxy.live_proxy.config_helper import ConfigHelper

            diagnostic_manager = native_server.client_managers.get(worker_id)
            diagnostic_local_count = (
                int(diagnostic_manager.get_client_count())
                if diagnostic_manager is not None else 0
            )
            return {
                "conditions": no_source_pause_conditions,
                "native_local_clients_zero": diagnostic_local_count == 0,
                "provider_request_delta": {
                    "source_a": (
                        final_no_source_counts["source-a.ts"]
                        - no_source_counts["source-a.ts"]
                    ),
                    "source_b": (
                        len(source_server.request_times("source-b.ts"))
                        - no_source_b_requests
                    ),
                },
                "provider_active_counts": {
                    "source_a": final_no_source_active["source-a.ts"],
                    "source_b": final_no_source_active["source-b.ts"],
                },
                "native_client_count": {
                    "global": int(redis_client.scard(RedisKeys.clients(worker_id)) or 0),
                    "local": diagnostic_local_count,
                },
                "native_keys": {
                    "owner_present": redis_client.get(
                        RedisKeys.channel_owner(worker_id),
                    ) is not None,
                    "stop_present": bool(redis_client.exists(
                        RedisKeys.channel_stopping(worker_id),
                    )),
                    "disconnect_present": bool(redis_client.exists(disconnect_key)),
                    "disconnect_ttl_seconds": int(redis_client.ttl(disconnect_key)),
                },
                "reservations": {
                    "profile_a": int(_profile_count(
                        redis_client, profile_a.id, profile_connections_key,
                    )),
                    "profile_b": int(_profile_count(
                        redis_client, profile_b.id, profile_connections_key,
                    )),
                },
                "native_cleanup_interval_seconds": int(
                    ConfigHelper.cleanup_check_interval()
                ),
                "native_channel_shutdown_delay_seconds": int(
                    ConfigHelper.channel_shutdown_delay()
                ),
            }
        _require_with_failure_diagnostic(
            all(no_source_pause_conditions.values()),
            "Paused no-source recording opened a provider stream or kept a reservation",
            no_source_pause_diagnostic,
        )
        resumed_plugin = PluginConfig.objects.get(key="catchuparr")
        resumed_plugin.settings = dict(sharing_settings)
        resumed_plugin.save(update_fields=("settings",))
        applied_resume = catchuparr_runtime.apply_configuration(sharing_settings)
        _require(applied_resume.get("applied") is True,
                 "Unified Apply rejected restoring the B-only source policy")
        active_resume = load_active_configuration(active_path)
        assert_applied_global_sharing(
            active_resume, worker_id, enabled=True, live_stability_seconds=1,
            last_viewer_return_seconds=2, min_switch_interval_seconds=60,
        )
        run_no_source.join_after_supervisor(timeout=30, cooperative_sleep=gevent_sleep)
        share_b_request_baseline = len(source_server.request_times("source-b.ts"))
        run_share = _RecorderTaskRun(
            worker_id, startup_timeout=45, media_idle_timeout=25,
        )
        run_share.start()
        harnesses.append(run_share)
        resume_deadline = time.monotonic() + 150
        while time.monotonic() < resume_deadline:
            resumed_counts, resumed_active = source_server.snapshot()
            if (len(source_server.request_times("source-b.ts")) > share_b_request_baseline
                    and resumed_active["source-b.ts"] == 1):
                break
            if run_share.failures:
                raise RuntimeError("Recorder failed while resuming allowed source B")
            gevent_sleep(0.1)
        _require(
            len(source_server.request_times("source-b.ts")) > share_b_request_baseline
            and resumed_active["source-a.ts"] == 0
            and resumed_active["source-b.ts"] == 1,
            "Recorder did not resume the allowed B source after the paused episode",
        )
        # Re-open A after the same native teardown readiness gate used
        # immediately after the first return.
        second_cycle_a_baseline = source_server.snapshot()[0]["source-a.ts"]
        live_response = _open_after_readiness(
            lambda: wait_for_native_a_teardown(no_source_client_id),
            lambda: client.get(
                f"/proxy/ts/stream/{channel.uuid}",
                HTTP_HOST="localhost",
                HTTP_USER_AGENT="Synthetic second-cycle ordinary viewer",
            ),
        )
        _require(live_response.status_code == 200,
                 "Second-cycle ordinary A viewer could not open")
        live_session, second_cycle_native_buffer = _create_native_live_reader_session(
            live_response, native_server, worker_id,
        )
        live_reader = live_session.reader
        second_cycle_ordinary_identity = _wait_for_native_client_count(
            redis_client, native_server, RedisKeys, worker_id, 1, timeout=20,
            return_identity=True,
            verified_borrow_client_ids=lambda: _verified_task_borrow_client_ids(
                run_share,
                worker_id=worker_id,
                channel_uuid=str(channel.uuid),
                decode_capability=decode_borrow_capability,
                is_server_verified=catchuparr_stats._is_recorder_client,
            ),
        )
        _wait_for_native_active(
            redis_client, native_server, worker_id, redis_keys=RedisKeys,
            metadata_field=ChannelMetadataField,
            channel_state=ChannelState, timeout=45,
            expected_ordinary_identity=second_cycle_ordinary_identity,
            verified_borrow_client_ids=lambda: _verified_task_borrow_client_ids(
                run_share,
                worker_id=worker_id,
                channel_uuid=str(channel.uuid),
                decode_capability=decode_borrow_capability,
                is_server_verified=catchuparr_stats._is_recorder_client,
            ),
        )
        second_cycle_first_media, _second_cycle_media_index = _read_after_current_buffer_head(
            live_session, second_cycle_native_buffer,
            minimum_bytes=188 * 100, timeout=20,
        )
        _verify_audio_video_tone(
            ffmpeg, ffprobe, second_cycle_first_media, 440,
            "Second-cycle ordinary live A before borrow",
        )
        _require(
            source_server.snapshot()[0]["source-a.ts"] == second_cycle_a_baseline + 1,
            "Second-cycle viewer did not open exactly one native A connection",
        )
        second_borrow_headers = None
        second_cycle_deadline = time.monotonic() + 90
        while time.monotonic() < second_cycle_deadline:
            with run_share._lock:
                borrow_headers_seen = [
                    dict(headers) for headers in run_share.input_headers
                    if "X-Catchuparr-Borrow" in headers
                ]
            if len(borrow_headers_seen) >= 1:
                second_borrow_headers = borrow_headers_seen[0]
                break
            if run_share.failures:
                raise RuntimeError("Second sharing cycle failed before native borrow")
            gevent_sleep(0.1)
        _require(second_borrow_headers is not None,
                 "Resumed recorder did not borrow the compatible A viewer")
        _wait_for_native_client_count(
            redis_client, native_server, RedisKeys, worker_id, 2,
            expected_local=1, timeout=20,
        )
        from catchuparr.adapters.sharing_borrow_route import decode_borrow_capability

        second_capability = decode_borrow_capability(
            second_borrow_headers["X-Catchuparr-Borrow"],
        )
        second_borrow_client_id = f"catchuparr-client-{second_capability.nonce}"
        _require(
            second_borrow_client_id != borrow_client_id
            and catchuparr_stats._is_recorder_client(worker_id, second_borrow_client_id),
            "Second borrow did not create a distinct verified recorder reader",
        )
        second_native_manager = native_server.stream_managers.get(worker_id)
        second_native_owner = redis_client.get(RedisKeys.channel_owner(worker_id))
        second_source_metadata = _native_source_metadata(redis_client, metadata_key)
        second_source_counts, second_active_counts = source_server.snapshot()
        _require(
            second_native_manager is not None and second_native_manager.running
            and second_native_owner is not None
            and second_source_counts["source-a.ts"] == second_cycle_a_baseline + 1
            and second_active_counts["source-a.ts"] == 1
            and second_active_counts["source-b.ts"] == 0,
            "Second borrow opened an extra provider source instead of reading A",
        )
        second_borrowed_ids = {
            segment.id for segment in store.segments(worker_id)
        }
        second_borrowed_segments = _wait_for_indexed_tone(
            run_share, store, worker_id, ffmpeg, ffprobe,
            expected_hz=440, existing_ids=second_borrowed_ids,
            minimum=1, timeout=45, cooperative_sleep=gevent_sleep,
        )
        _require(bool(second_borrowed_segments),
                 "Second native borrow did not archive fresh A media")

        # Exercise a real recorder-task restart with a durable RETURN pending.
        # Leave the native reader at its deadline, let the task create the
        # RETURN intent, then deny B before useful media so a new task must
        # recover that exact intent and acknowledge it only after fresh media.
        return_ledger = SharingIntentLedger(
            archive_root / ".catchuparr-sharing.sqlite3",
        )
        restart_b_worker_ids_before = {
            str(record.get("worker_id"))
            for record in _worker_records(redis_client, worker_id)
            if record.get("stream_id") == str(stream_b.id)
            and record.get("worker_id")
        }
        # Compare against the original pre-probe baseline. Sampling during
        # asynchronous cleanup could capture a still-live B reservation as
        # the expected value and hide its later leak.
        restart_b_profile_baseline = profile_baselines[profile_b.id]
        source_server.set_mode("source-b.ts", "unavailable")
        restart_b_request_baseline = len(source_server.request_times("source-b.ts"))
        live_session.close()
        live_session = None
        live_reader = None
        restart_episode_key = f"catchuparr:sharing:episode:{worker_id}"
        restart_grace_deadline = time.monotonic() + 10
        while _redis_text(redis_client.hget(restart_episode_key, "phase")) != "wait_return":
            _require(time.monotonic() < restart_grace_deadline,
                     "Restart probe did not enter native viewer-return grace")
            gevent_sleep(0.05)
        restart_native_deadline = float(
            redis_client.hget(restart_episode_key, "return_deadline")
        )
        pending_return = None
        restart_return_deadline = time.monotonic() + 150
        while time.monotonic() < restart_return_deadline:
            current_return = return_ledger.read(worker_id)
            if (current_return is not None and current_return.intent is not None
                    and current_return.intent.action.value == "return_to_allowed"):
                pending_return = current_return.intent
                break
            if run_share.failures:
                raise RuntimeError("Task failed before persisting its RETURN intent")
            gevent_sleep(0.1)
        _require(
            pending_return is not None
            and time.time() >= restart_native_deadline
            and pending_return.not_before >= restart_native_deadline,
            "Native return did not produce a durable RETURN intent at its deadline",
        )
        b_unavailable_deadline = time.monotonic() + 15
        while (len(source_server.request_times("source-b.ts"))
               <= restart_b_request_baseline):
            _require(time.monotonic() < b_unavailable_deadline,
                     "Task did not attempt B while the RETURN intent was pending")
            gevent_sleep(0.05)
        (
            restart_b_attempt_ids,
            restart_b_attempt_bindings,
            restart_b_attempts_bound,
        ) = _stop_join_and_freeze_attempt_bindings(
            run_share,
            lambda: _worker_records(redis_client, worker_id),
            baseline_worker_ids=restart_b_worker_ids_before,
            stream_id=str(stream_b.id),
            cooperative_sleep=gevent_sleep,
        )
        _require(
            restart_b_attempts_bound,
            "Pending RETURN did not expose stable exact B attempt bindings after task join",
        )
        restart_b_request_count_after_stop = len(
            source_server.request_times("source-b.ts"),
        )

        def restart_b_cleanup_snapshot():
            attempt_set_unchanged, current_records = (
                _attempt_records_match_frozen_bindings(
                    _worker_records(redis_client, worker_id),
                    baseline_worker_ids=restart_b_worker_ids_before,
                    stream_id=str(stream_b.id),
                    attempt_ids=restart_b_attempt_ids,
                    bindings=restart_b_attempt_bindings,
                )
            )
            records = [current_records.get(worker) for worker in restart_b_attempt_ids]
            exact_bindings = attempt_set_unchanged
            ledger_released = bool(records) and all(
                record is not None
                and record.get("state") in {"failed", "released"}
                and record.get("reservation_state") in {"none", "released"}
                for record in records
            )
            private_markers_absent = all(
                record is not None
                and not redis_client.exists(
                    CAPABILITY_PREFIX
                    + restart_b_attempt_bindings[worker]["capability_digest"],
                )
                and not redis_client.exists(
                    reservation_credential_marker_key(
                        restart_b_attempt_bindings[worker]["reservation_id"],
                    ),
                )
                for worker, record in zip(restart_b_attempt_ids, records)
            )
            native_resources_absent = all(
                worker not in native_server.stream_managers
                and worker not in native_server._live_stream_managers
                and worker not in native_server.client_managers
                and redis_client.get(RedisKeys.channel_owner(worker)) is None
                and int(redis_client.scard(RedisKeys.clients(worker)) or 0) == 0
                and not redis_client.exists(RedisKeys.channel_stopping(worker))
                for worker in restart_b_attempt_ids
            )
            provider_counts, provider_active = source_server.snapshot()
            return {
                "conditions": {
                    "attempt_ledger_released": ledger_released,
                    "attempt_binding_preserved": exact_bindings,
                    "attempt_set_unchanged_after_join": attempt_set_unchanged,
                    "attempt_markers_absent": private_markers_absent,
                    "native_resources_absent": native_resources_absent,
                    "provider_b_inactive": provider_active["source-b.ts"] == 0,
                    "provider_b_requests_unchanged": (
                        len(source_server.request_times("source-b.ts"))
                        == restart_b_request_count_after_stop
                        and provider_counts["source-b.ts"]
                        == restart_b_request_count_after_stop
                    ),
                    "profile_b_at_baseline": (
                        _profile_count_matches_original_baseline(
                            _profile_count(
                                redis_client, profile_b.id,
                                profile_connections_key,
                            ),
                            restart_b_profile_baseline,
                        )
                    ),
                },
                "worker_states": [
                    _allowlisted_state(
                        record.get("state") if record else None,
                        frozenset({
                            "issued", "reserving", "reserved", "starting", "active",
                            "releasing", "released", "release_failed", "failed",
                        }),
                    )
                    for record in records
                ],
                "reservation_states": [
                    _allowlisted_state(
                        record.get("reservation_state") if record else None,
                        frozenset({
                            "none", "reserving", "reserved", "releasing",
                            "released", "release_failed",
                        }),
                    )
                    for record in records
                ],
                "provider_b_active": provider_active["source-b.ts"],
                "provider_b_requests": provider_counts["source-b.ts"],
            }

        restart_b_release_timeout = _native_teardown_readiness_timeout(
            float(ConfigHelper.channel_shutdown_delay()),
            float(ConfigHelper.cleanup_check_interval()),
            stopping_marker_ttl=90,
        )
        restart_b_resources_released, restart_b_release_snapshot = (
            _wait_for_readiness_snapshot(
                restart_b_cleanup_snapshot, restart_b_release_timeout,
                monotonic=time.monotonic, sleep=gevent_sleep, poll_interval=0.1,
            )
        )
        _require_with_failure_diagnostic(
            restart_b_resources_released,
            "Stopping the pending RETURN task did not release its exact B attempt",
            lambda: restart_b_release_snapshot,
            diagnostic_label="CATCHUPARR_PENDING_RETURN_B_CLEANUP_DIAGNOSTIC",
        )
        interrupted_return = return_ledger.read(worker_id)
        _require(
            interrupted_return is not None
            and interrupted_return.intent is not None
            and interrupted_return.intent.action.value == "return_to_allowed"
            and interrupted_return.intent.intent_epoch == pending_return.intent_epoch
            and not redis_client.exists(
                f"live:channel:{worker_id}:clients:{second_borrow_client_id}"
            )
            and _redis_text(redis_client.hget(restart_episode_key, "phase")) == "closed",
            "Stopping before B media lost the durable RETURN or retained its borrow client",
        )
        source_server.set_mode("source-b.ts", "continuous")
        restart_b_media_baseline = {segment.id for segment in store.segments(worker_id)}
        restart_request_baseline = len(source_server.request_times("source-b.ts"))
        run_restart = _RecorderTaskRun(
            worker_id, startup_timeout=45, media_idle_timeout=25,
        )
        run_restart.start()
        harnesses.append(run_restart)
        restarted_return_request_deadline = time.monotonic() + 150
        while time.monotonic() < restarted_return_request_deadline:
            if len(source_server.request_times("source-b.ts")) > restart_request_baseline:
                break
            if run_restart.failures:
                raise RuntimeError("Replacement task failed before retrying the pending RETURN")
            gevent_sleep(0.1)
        _require(
            len(source_server.request_times("source-b.ts")) > restart_request_baseline,
            "Replacement task did not resume the pending allowed B source",
        )
        restart_returned_segments = _wait_for_indexed_tone(
            run_restart, store, worker_id, ffmpeg, ffprobe,
            expected_hz=880, existing_ids=restart_b_media_baseline,
            minimum=1, timeout=45, cooperative_sleep=gevent_sleep,
        )
        _require(bool(restart_returned_segments),
                 "Restarted RETURN did not archive useful B media")
        restart_committed = return_ledger.read(worker_id)
        _require(
            restart_committed is not None
            and restart_committed.intent is None
            and restart_committed.state.phase.value == "dedicated"
            and restart_committed.state.last_switch_at is not None
            and restart_committed.state.last_switch_at >= pending_return.not_before,
            "The restarted task did not commit the RETURN on its first useful B media",
        )
        # The replacement task now borrows a fresh ordinary A viewer. Keep it
        # alive through Pause to prove cleanup leaves the native viewer intact.
        second_cycle_a_baseline = source_server.snapshot()[0]["source-a.ts"]
        live_response = _open_after_readiness(
            lambda: wait_for_native_a_teardown(second_borrow_client_id),
            lambda: client.get(
                f"/proxy/ts/stream/{channel.uuid}",
                HTTP_HOST="localhost",
                HTTP_USER_AGENT="Synthetic post-restart ordinary viewer",
            ),
        )
        _require(live_response.status_code == 200,
                 "Post-restart ordinary A viewer could not open")
        live_session, post_restart_native_buffer = _create_native_live_reader_session(
            live_response, native_server, worker_id,
        )
        live_reader = live_session.reader
        post_restart_ordinary_identity = _wait_for_native_client_count(
            redis_client, native_server, RedisKeys, worker_id, 1, timeout=20,
            return_identity=True,
            verified_borrow_client_ids=lambda: _verified_task_borrow_client_ids(
                run_restart,
                worker_id=worker_id,
                channel_uuid=str(channel.uuid),
                decode_capability=decode_borrow_capability,
                is_server_verified=catchuparr_stats._is_recorder_client,
            ),
        )
        _wait_for_native_active(
            redis_client, native_server, worker_id, redis_keys=RedisKeys,
            metadata_field=ChannelMetadataField,
            channel_state=ChannelState, timeout=45,
            expected_ordinary_identity=post_restart_ordinary_identity,
            verified_borrow_client_ids=lambda: _verified_task_borrow_client_ids(
                run_restart,
                worker_id=worker_id,
                channel_uuid=str(channel.uuid),
                decode_capability=decode_borrow_capability,
                is_server_verified=catchuparr_stats._is_recorder_client,
            ),
        )
        post_restart_first_media, _post_restart_media_index = _read_after_current_buffer_head(
            live_session, post_restart_native_buffer,
            minimum_bytes=188 * 100, timeout=20,
        )
        _verify_audio_video_tone(
            ffmpeg, ffprobe, post_restart_first_media, 440,
            "Post-restart ordinary live A before borrow",
        )
        post_restart_borrow_headers = None
        post_restart_borrow_deadline = time.monotonic() + 90
        while time.monotonic() < post_restart_borrow_deadline:
            with run_restart._lock:
                post_restart_borrow_headers = next(
                    (dict(headers) for headers in run_restart.input_headers
                     if "X-Catchuparr-Borrow" in headers),
                    None,
                )
            if post_restart_borrow_headers is not None:
                break
            if run_restart.failures:
                raise RuntimeError("Restarted task failed before borrowing live A")
            gevent_sleep(0.1)
        _require(post_restart_borrow_headers is not None,
                 "Restarted task did not borrow the post-restart A viewer")
        _wait_for_native_client_count(
            redis_client, native_server, RedisKeys, worker_id, 2,
            expected_local=1, timeout=20,
        )
        second_capability = decode_borrow_capability(
            post_restart_borrow_headers["X-Catchuparr-Borrow"],
        )
        second_borrow_client_id = f"catchuparr-client-{second_capability.nonce}"
        second_native_manager = native_server.stream_managers.get(worker_id)
        second_native_owner = redis_client.get(RedisKeys.channel_owner(worker_id))
        second_source_metadata = _native_source_metadata(redis_client, metadata_key)
        second_source_counts, second_active_counts = source_server.snapshot()
        _require(
            second_borrow_client_id != borrow_client_id
            and catchuparr_stats._is_recorder_client(worker_id, second_borrow_client_id)
            and second_native_manager is not None and second_native_manager.running
            and second_native_owner is not None
            and second_source_counts["source-a.ts"] == second_cycle_a_baseline + 1
            and second_active_counts["source-a.ts"] == 1
            and second_active_counts["source-b.ts"] == 0,
            "Post-restart borrow opened another provider source instead of reading A",
        )
        second_borrowed_ids = {segment.id for segment in store.segments(worker_id)}
        second_borrowed_segments = _wait_for_indexed_tone(
            run_restart, store, worker_id, ffmpeg, ffprobe,
            expected_hz=440, existing_ids=second_borrowed_ids,
            minimum=1, timeout=45, cooperative_sleep=gevent_sleep,
        )
        _require(bool(second_borrowed_segments),
                 "Post-restart borrow did not archive fresh A media")
        final_a_request_total = second_cycle_a_baseline + 1
        run_share = run_restart
        share_b_request_baseline = restart_request_baseline
        from catchuparr import sharing_runtime as sharing_runtime_module
        from catchuparr.adapters import sharing_borrow_route

        real_borrow_cleanup = sharing_borrow_route.cleanup_borrowed_client
        cleanup_attempts = []
        cleanup_ack_released = {"value": False}

        def fail_first_borrow_cleanup(*args, **kwargs):
            result = real_borrow_cleanup(*args, **kwargs)
            # Simulate a lost reply only after Redis has removed the exact
            # registration. Keep suppressing successful replies until the
            # task's normal close/retry paths have run; their call count is an
            # implementation detail. A later retry must receive the
            # idempotent native absence ACK.
            if result and not cleanup_ack_released["value"]:
                cleanup_attempts.append(False)
                return False
            cleanup_attempts.append(result)
            return result

        # Force one exact native cleanup ACK loss on the Stop path. The
        # durable session retry must remove only the recorder reader and leave
        # the ordinary viewer connected.
        with patch.object(
            sharing_borrow_route,
            "cleanup_borrowed_client",
            side_effect=fail_first_borrow_cleanup,
        ), patch.object(catchuparr_tasks.reconcile_recorders, "apply_async"):
            second_pause_result = catchuparr_runtime.pause_recorders()
            _require(
                second_pause_result.get("paused") is True,
                "Pause did not persist during the second borrowed cycle",
            )
            run_share.join_after_supervisor(timeout=25, cooperative_sleep=gevent_sleep)
            _require(
                run_share.result == {"status": "stopped"},
                "Pause did not stop the second borrowed recorder attempt",
            )
            _require(
                cleanup_attempts and all(result is False for result in cleanup_attempts),
                "The synthetic borrowed cleanup did not suppress post-effect ACKs",
            )
            borrow_client_key = (
                f"live:channel:{worker_id}:clients:{second_borrow_client_id}"
            )
            _require(
                not redis_client.exists(borrow_client_key)
                and not redis_client.sismember(
                    RedisKeys.clients(worker_id), second_borrow_client_id,
                ),
                "Lost cleanup ACK did not follow an actual native registration removal",
            )
            cleanup_ack_released["value"] = True
            completed_retry, pending_retry = (
                sharing_runtime_module.retry_pending_open_cleanups()
            )
            _require(
                completed_retry >= 1
                and pending_retry == 0
                and cleanup_attempts[-1] is True,
                "A retained borrowed cleanup did not succeed on its explicit retry ACK",
            )
        _require(
            run_share.result == {"status": "stopped"},
            "Borrowing recorder restarted after Stop before viewer cleanup",
        )
        _wait_for_native_client_count(
            redis_client, native_server, RedisKeys, worker_id, 1, timeout=15,
        )
        second_borrow_client_key = (
            f"live:channel:{worker_id}:clients:{second_borrow_client_id}"
        )
        _require(
            not redis_client.exists(second_borrow_client_key)
            and not redis_client.sismember(RedisKeys.clients(worker_id), second_borrow_client_id)
            and redis_client.hget(
                f"catchuparr:sharing:private:{worker_id}", second_borrow_client_id,
            ) is None,
            "Pause or the lost-ACK retry left the second borrowed reader registered",
        )
        paused_source_counts, paused_active_counts = source_server.snapshot()
        _require(
            native_server.stream_managers.get(worker_id) is second_native_manager
            and second_native_manager.running
            and redis_client.get(RedisKeys.channel_owner(worker_id)) == second_native_owner
            and _native_source_metadata(redis_client, metadata_key) == second_source_metadata
            and paused_source_counts["source-a.ts"] == second_source_counts["source-a.ts"]
            and paused_active_counts["source-a.ts"] == 1
            and paused_active_counts["source-b.ts"] == 0
            and _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1
            and _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id],
            "Pause changed the ordinary A viewer, its native owner, or provider reservations",
        )

        if live_session is not None:
            live_session.close()
            live_response = None
            live_reader = None
            live_session = None
        # Reuse the full native A release acknowledgment instead of shorter
        # owner-only waits: Dispatcharr can retain the stopping marker after
        # the last response closes while its native cleanup monitor drains.
        wait_for_native_a_teardown(second_borrow_client_id)
        counts_final, active_final = source_server.snapshot()
        _require(
            redis_client.get(RedisKeys.channel_owner(worker_id)) is None
            and native_server.stream_managers.get(worker_id) is None
            and int(redis_client.scard(RedisKeys.clients(worker_id)) or 0) == 0
            and native_server.client_managers.get(worker_id) is None
            and _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id]
            and counts_final["source-a.ts"] == final_a_request_total
            and active_final["source-a.ts"] == 0
            and _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id],
            "Stopping the final native live A client did not release its owner and provider slot",
        )
        _require(
            {
                int(profile.id): _key_dump(
                    redis_client, profile_credential_release_key(profile.id),
                )
                for profile in created_profiles
            } == marker_baselines,
            "Live/archive cleanup did not restore synthetic credential markers",
        )
        _require(
            list(ChannelStream.objects.filter(channel=channel).order_by("order", "id")
                 .values_list("stream_id", "order")) == assignment_rows,
            "Live/archive probe changed native ChannelStream rows",
        )
        print("AIO native live A remained isolated while the real recorder indexed B")
    except BaseException as error:
        primary_exception = error
        raise
    finally:
        # On a startup failure, freeze the observed state before cleanup
        # changes Redis keys or native worker state. The successful path has
        # already captured and restored the hooks above.
        if native_diagnostic is not None and native_diagnostic_report is None:
            try:
                native_diagnostic_report = native_diagnostic.close()
            except Exception:
                # Diagnostics must not replace the probe's original result.
                pass
        for run in reversed(harnesses):
            if run.thread is not None:
                try:
                    run.stop()
                    run.join(20, cooperative_sleep=gevent_sleep)
                except Exception:
                    cleanup_errors.append("recorder-task")
        if live_session is not None:
            try:
                force_release = None
                if native_server is not None and channel is not None:
                    worker_to_release = str(channel.uuid)

                    def force_release_native() -> None:
                        from apps.proxy.live_proxy.services.channel_service import ChannelService

                        ChannelService.stop_channel(worker_to_release)

                    force_release = force_release_native
                live_session.close(force_release=force_release)
                live_reader = None
                live_session = None
            except Exception:
                cleanup_errors.append("native-live-response")
        elif live_response is not None:
            try:
                live_response.close()
            except Exception:
                cleanup_errors.append("native-live-response")
        if churn_session is not None:
            try:
                churn_session.close()
                churn_session = None
            except Exception:
                cleanup_errors.append("native-churn-response")
        try:
            from catchuparr.adapters.recorder_proxy import stop_managed_workers

            if not stop_managed_workers(timeout_seconds=8):
                cleanup_errors.append("managed-workers")
        except Exception:
            cleanup_errors.append("managed-workers")
        channel_tasks.get_dvr_stream_base_url = original_base_url
        if bridge is not None:
            try:
                bridge.close()
                if bridge.server.active_requests():
                    cleanup_errors.append("http-bridge")
            except Exception:
                cleanup_errors.append("http-bridge")
        if native_server is not None and channel is not None and redis_client is not None:
            try:
                worker_id = str(channel.uuid)
                if native_server.stream_managers.get(worker_id) is not None:
                    from apps.proxy.live_proxy.services.channel_service import ChannelService

                    # This is failure-path cleanup only. The successful path
                    # above still proves that closing the real response stops
                    # its own native client and leaves the worker intact until
                    # that final client disconnects.
                    ChannelService.stop_channel(worker_id)
                    deadline = time.monotonic() + 10
                    while (
                        time.monotonic() < deadline
                        and native_server.stream_managers.get(worker_id) is not None
                    ):
                        time.sleep(0.1)
                if redis_client.get(RedisKeys.channel_owner(worker_id)) is not None:
                    cleanup_errors.append("native-live-owner")
                if native_server.stream_managers.get(worker_id) is not None:
                    cleanup_errors.append("native-live-manager")
            except Exception:
                cleanup_errors.append("native-live-cleanup-check")
        if native_diagnostic is not None:
            try:
                if native_diagnostic_report is not None:
                    print(
                        "CATCHUPARR_NATIVE_REDIS_INIT_DIAGNOSTIC "
                        + json.dumps(native_diagnostic_report, sort_keys=True)
                    )
            except Exception:
                # Diagnostics must not replace the probe's original result.
                pass
        if source_server is not None:
            try:
                source_server.close()
                if any(source_server.snapshot()[1].values()):
                    cleanup_errors.append("synthetic-source-connections")
            except Exception:
                cleanup_errors.append("synthetic-source-server")
        if default_profile_saved:
            try:
                _set_stream_default_profile(CoreSettings, saved_default_profile)
                _require(
                    str(_stream_default_profile_id(CoreSettings)) == str(saved_default_profile),
                    "Native default StreamProfile was not restored",
                )
            except Exception:
                cleanup_errors.append("default-stream-profile")
        try:
            if active_existed:
                active_path.write_bytes(active_bytes)
            else:
                active_path.unlink(missing_ok=True)
            if active_lock_existed:
                active_lock_path.write_bytes(active_lock_bytes)
            else:
                active_lock_path.unlink(missing_ok=True)
            if reset_marker_existed:
                reset_marker_path.write_bytes(reset_marker_bytes)
            else:
                reset_marker_path.unlink(missing_ok=True)
            if control_existed:
                control_path.write_bytes(control_bytes)
            else:
                control_path.unlink(missing_ok=True)
            if control_deny_existed:
                control_deny.write_bytes(control_deny_bytes)
            else:
                control_deny.unlink(missing_ok=True)
        except Exception:
            cleanup_errors.append("active-config")
        try:
            latest_plugin_config = PluginConfig.objects.get(key="catchuparr")
            latest_plugin_config.settings = original_plugin_settings
            latest_plugin_config.save(update_fields=("settings",))
        except Exception:
            cleanup_errors.append("plugin-settings")
        if redis_client is not None and not cleanup_errors:
            try:
                _require(
                    all(
                        _profile_count(redis_client, profile.id, profile_connections_key)
                        == profile_baselines.get(profile.id, 0)
                        for profile in created_profiles
                    ),
                    "Live/archive cleanup did not restore provider profile capacity",
                )
                _require(
                    {
                        int(profile.id): _key_dump(
                            redis_client, profile_credential_release_key(profile.id),
                        )
                        for profile in created_profiles
                    } == marker_baselines,
                    "Live/archive cleanup did not restore credential markers",
                )
            except Exception:
                cleanup_errors.append("profile-capacity")
        if not cleanup_errors:
            try:
                for profile in created_profiles:
                    if redis_client is not None:
                        redis_client.delete(
                            profile_connections_key(profile.id),
                            profile_credential_release_key(profile.id),
                        )
                    profile.delete()
                for stream in created_streams:
                    stream.delete()
                for account in created_accounts:
                    account.delete()
                if channel is not None:
                    channel.delete()
                for channel_profile in created_channel_profiles:
                    channel_profile.delete()
            except Exception as error:
                cleanup_errors.append("synthetic-database-rows")
                cleanup_exception_types.append(type(error).__name__)
        if not cleanup_errors:
            shutil.rmtree(archive_root, ignore_errors=True)
        shutil.rmtree(fixture_root, ignore_errors=True)
        if cleanup_errors:
            details = ", ".join(sorted(set(cleanup_errors)))
            cleanup_diagnostic = {
                "cleanup_stages": sorted(set(cleanup_errors)),
                "cleanup_exception_types": sorted(set(cleanup_exception_types)),
                "db_context": _current_orm_context_snapshot(),
            }
            if primary_exception is not None:
                cleanup_diagnostic["primary_exception_type"] = type(primary_exception).__name__
                print(
                    "CATCHUPARR_CLEANUP_AFTER_PRIMARY_FAILURE "
                    + json.dumps(cleanup_diagnostic, sort_keys=True)
                )
            else:
                raise RuntimeError(
                    "Synthetic live/archive isolation cleanup was incomplete "
                    f"({details}); "
                    + json.dumps(cleanup_diagnostic, sort_keys=True)
                )
