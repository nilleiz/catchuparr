"""Exercise recorder diagnostic Lua CAS against live native AIO state."""

from __future__ import annotations

import json
import time


def probe_live_dedicated_diagnostic(redis_client, channel_uuid: str, *, now=None):
    """Probe the dedicated writer using the task's actual active worker binding."""
    from catchuparr.recorder_diagnostics import (
        last_error_key,
        live_key,
        publish_recorder_diagnostic,
    )

    return _probe_dedicated_binding(redis_client, channel_uuid, now=now,
                                    last_error_key=last_error_key, live_key=live_key,
                                    publish=publish_recorder_diagnostic)


def _probe_dedicated_binding(redis_client, channel_uuid, *, now, last_error_key, live_key,
                             publish):
    instant = float(time.time() if now is None else now)
    lease_key = f"catchuparr:recorder:{channel_uuid}"
    lease_value = _text(redis_client.get(lease_key))
    live_diagnostic_key = live_key(channel_uuid)
    error_diagnostic_key = last_error_key(channel_uuid)
    if not lease_value or int(redis_client.ttl(lease_key)) <= 0:
        raise AssertionError("dedicated recorder lease is not live")
    try:
        diagnostic = _wait_for_recording(redis_client, live_diagnostic_key, "dedicated")
    except AssertionError:
        guards = _dedicated_writer_guard_checks(
            redis_client, channel_uuid, lease_value, live_diagnostic_key,
        )
        raise AssertionError(
            "dedicated recording diagnostic was not confirmed in time; "
            f"guard_checks={json.dumps(guards, sort_keys=True, separators=(',', ':'))}"
        ) from None
    saved = {
        live_diagnostic_key: _capture_hash(redis_client, live_diagnostic_key),
        error_diagnostic_key: _capture_hash(redis_client, error_diagnostic_key),
    }

    def cleanup():
        for key, record in saved.items():
            _restore_hash(redis_client, key, record)

    common = {
        "worker_id": diagnostic["worker_id"],
        "phase": "recording",
        "source": "dedicated",
        "stream_id": diagnostic["stream_id"],
        "account_id": diagnostic["account_id"],
        "started_at": float(diagnostic["started_at"]),
        "config_generation": diagnostic["config_generation"],
        "control_generation": int(diagnostic["control_generation"]),
        "fence": int(diagnostic["fence"]),
    }
    try:
        if not publish(redis_client, channel_uuid, lease_value, **common, now=instant):
            raise AssertionError("dedicated diagnostic Lua rejected its live task binding")
        if publish(redis_client, channel_uuid, "stale-lease", **common, now=instant):
            raise AssertionError("dedicated diagnostic Lua accepted a stale lease")
        if publish(redis_client, channel_uuid, lease_value,
                   **{**common, "config_generation": "0" * 64}, now=instant):
            raise AssertionError("dedicated diagnostic Lua accepted stale configuration")
        if publish(redis_client, channel_uuid, lease_value,
                   **{**common, "fence": common["fence"] + 1}, now=instant):
            raise AssertionError("dedicated diagnostic Lua accepted a stale fence")
        if not publish(redis_client, channel_uuid, lease_value,
                       **{**common, "phase": "error", "error_code": "recorder_worker_stopped"},
                       now=instant):
            raise AssertionError("dedicated last-error Lua write failed")
        ttl = int(redis_client.ttl(error_diagnostic_key))
        if not 0 < ttl <= 24 * 60 * 60:
            raise AssertionError("dedicated last-error TTL is outside 24 hours")
        if not publish(redis_client, channel_uuid, lease_value, **common, now=instant):
            raise AssertionError("dedicated recording diagnostic could not be restored")
    except Exception:
        cleanup()
        raise
    return cleanup


def _dedicated_writer_guard_checks(redis_client, channel_uuid, lease_value, live_key):
    """Return bounded booleans for the current attempt, not a stale live row."""
    from catchuparr.adapters.recorder_proxy import (
        _ACTIVE_WORKER_STATES,
        PLUGIN_REDIS_PREFIX,
    )
    from catchuparr.recorder_diagnostics import _HEX64, _SAFE_ID, _SAFE_NATIVE_OWNER

    checks = {
        "live_record_present": False,
        "live_record_is_dedicated": False,
        "live_record_is_recording": False,
        "live_record_matches_current_attempt": False,
        "current_attempt_record_found": False,
        "current_attempt_worker_shapes_valid": False,
        "current_attempt_lease_matches": False,
        "current_attempt_native_owner_lock_live": False,
        "current_attempt_native_owner_heartbeat_live": False,
        "current_attempt_metadata_matches": False,
        "current_attempt_profile_matches": False,
        "current_attempt_state_supported": False,
        "current_attempt_stop_marker_clear": False,
        "redis_reads_available": False,
    }
    try:
        live_record = _decode_hash(redis_client.hgetall(live_key))
        checks["redis_reads_available"] = True
        checks["live_record_present"] = bool(live_record)
        checks["live_record_is_dedicated"] = live_record.get("source") == "dedicated"
        checks["live_record_is_recording"] = live_record.get("phase") == "recording"
        lease_key = f"catchuparr:recorder:{channel_uuid}"
        pattern = f"{PLUGIN_REDIS_PREFIX}worker:catchuparr-r*"
        expected_worker_prefix = f"{PLUGIN_REDIS_PREFIX}worker:"
        current_attempts = []
        for worker_key in redis_client.scan_iter(match=pattern, count=100):
            worker_key_text = _text(worker_key)
            if not isinstance(worker_key_text, str):
                continue
            worker = _decode_hash(redis_client.hgetall(worker_key))
            if (worker.get("channel_uuid") == channel_uuid
                    and worker.get("lease_key") == lease_key
                    and worker.get("lease_value") == lease_value
                    and worker.get("state") in _ACTIVE_WORKER_STATES):
                worker_id = worker.get("worker_id")
                expected_worker_key = expected_worker_prefix + worker_id if worker_id else ""
                if worker_key_text == expected_worker_key:
                    current_attempts.append((worker_id, worker))
        if len(current_attempts) != 1:
            return checks
        worker_id, worker = current_attempts[0]
        checks["current_attempt_record_found"] = True
        checks["live_record_matches_current_attempt"] = bool(
            live_record.get("worker_id") == worker_id
            and live_record.get("source") == "dedicated"
        )
        checks["current_attempt_worker_shapes_valid"] = bool(
            worker.get("channel_uuid") == channel_uuid
            and worker.get("worker_id") == worker_id
            and _SAFE_ID.fullmatch(worker.get("stream_id", ""))
            and _SAFE_ID.fullmatch(worker.get("account_id", ""))
            and _HEX64.fullmatch(worker.get("config_generation", ""))
            and worker.get("control_generation", "").isdecimal()
            and worker.get("lease_fence", "").isdecimal()
        )
        checks["current_attempt_lease_matches"] = bool(
            worker.get("lease_key") == lease_key
            and worker.get("lease_value") == lease_value
            and _text(redis_client.get(lease_key)) == lease_value
            and int(redis_client.ttl(lease_key)) > 0
        )
        owner_key = f"live:channel:{worker_id}:owner"
        metadata_key = f"live:channel:{worker_id}:metadata"
        owner = _text(redis_client.get(owner_key))
        heartbeat_key = f"live:worker:{owner}:heartbeat" if owner else ""
        metadata = _decode_hash(redis_client.hgetall(metadata_key))
        checks["current_attempt_native_owner_lock_live"] = bool(
            owner and _SAFE_NATIVE_OWNER.fullmatch(owner)
            and int(redis_client.ttl(owner_key)) > 0
        )
        checks["current_attempt_native_owner_heartbeat_live"] = bool(
            owner and int(redis_client.ttl(heartbeat_key)) > 0
        )
        checks["current_attempt_metadata_matches"] = bool(
            metadata.get("owner") == owner
            and metadata.get("stream_id") == worker.get("stream_id")
        )
        checks["current_attempt_profile_matches"] = bool(
            worker.get("profile_id")
            and metadata.get("m3u_profile") == worker.get("profile_id")
            and metadata.get("stream_profile")
        )
        checks["current_attempt_state_supported"] = metadata.get("state", "").lower() in {
            "active", "waiting_for_clients",
        }
        checks["current_attempt_stop_marker_clear"] = not bool(
            redis_client.exists(f"live:channel:{worker_id}:stopping")
        )
    except Exception:
        pass
    return checks


def probe_live_borrowed_diagnostic(redis_client, channel_uuid: str, *, now=None):
    """Probe the borrowed writer using the currently registered native grant."""
    from catchuparr.adapters.sharing_native import read_native_episode_grant
    from catchuparr.recorder_diagnostics import (
        last_error_key,
        live_key,
        publish_borrowed_recorder_diagnostic,
    )

    instant = float(time.time() if now is None else now)
    grant = read_native_episode_grant(redis_client, channel_uuid)
    if grant is None:
        raise AssertionError("borrowed diagnostic requires a current native grant")
    lease_key = f"catchuparr:recorder:{channel_uuid}"
    lease_value = _text(redis_client.get(lease_key))
    if not lease_value or int(redis_client.ttl(lease_key)) <= 0:
        raise AssertionError("borrowed recorder lease is not live")
    live_diagnostic_key = live_key(channel_uuid)
    error_diagnostic_key = last_error_key(channel_uuid)
    try:
        diagnostic = _wait_for_recording(redis_client, live_diagnostic_key, "borrowed")
    except AssertionError:
        guards = _borrowed_writer_guard_checks(redis_client, grant, lease_value)
        raise AssertionError(
            "borrowed recording diagnostic was not confirmed in time; "
            f"guard_checks={json.dumps(guards, sort_keys=True, separators=(',', ':'))}"
        ) from None
    saved = {
        live_diagnostic_key: _capture_hash(redis_client, live_diagnostic_key),
        error_diagnostic_key: _capture_hash(redis_client, error_diagnostic_key),
    }

    def cleanup():
        for key, record in saved.items():
            _restore_hash(redis_client, key, record)

    common = {
        "episode_id": grant.episode_id,
        "client_id": grant.client_id,
        "worker_id": grant.worker_id,
        "owner_lock_value": grant.owner_lock_value,
        "metadata_owner": grant.metadata_owner,
        "stream_id": grant.stream_id,
        "account_id": grant.account_id,
        "m3u_profile_id": grant.m3u_profile_id,
        "input_profile_id": grant.input_profile_id,
        "catalog_generation": grant.catalog_generation,
        "phase": "recording",
        "started_at": float(diagnostic["started_at"]),
        "config_generation": grant.configuration_generation,
        "control_generation": grant.control_generation,
        "fence": grant.recorder_fence,
    }
    try:
        if not publish_borrowed_recorder_diagnostic(
            redis_client, channel_uuid, lease_value, **common, now=instant,
        ):
            raise AssertionError("borrowed diagnostic Lua rejected the live grant/client")
        denials = (
            ("lease", {"lease_value": "stale-lease"}),
            ("configuration", {"config_generation": "0" * 64}),
            ("fence", {"fence": grant.recorder_fence + 1}),
            ("client", {"client_id": "catchuparr-client-not-a-member"}),
        )
        for _, override in denials:
            rejected_lease = override.get("lease_value", lease_value)
            call_args = {key: value for key, value in override.items() if key != "lease_value"}
            if publish_borrowed_recorder_diagnostic(
                redis_client, channel_uuid, rejected_lease,
                **{**common, **call_args}, now=instant,
            ):
                raise AssertionError("borrowed diagnostic Lua accepted a stale binding")
        if not publish_borrowed_recorder_diagnostic(
            redis_client, channel_uuid, lease_value,
            **{**common, "phase": "error", "error_code": "borrowed_recorder_worker_stopped"},
            now=instant,
        ):
            raise AssertionError("borrowed last-error Lua write failed")
        ttl = int(redis_client.ttl(error_diagnostic_key))
        if not 0 < ttl <= 24 * 60 * 60:
            raise AssertionError("borrowed last-error TTL is outside 24 hours")
        if not publish_borrowed_recorder_diagnostic(
            redis_client, channel_uuid, lease_value, **common, now=instant,
        ):
            raise AssertionError("borrowed recording diagnostic could not be restored")
    except Exception:
        cleanup()
        raise
    return cleanup


def _borrowed_writer_guard_checks(redis_client, grant, lease_value):
    """Return bounded booleans for the writer's input and Redis CAS guards."""
    from catchuparr.recorder_diagnostics import _HEX64, _SAFE_ID, _SAFE_NATIVE_OWNER

    channel = grant.channel_uuid
    lease_key = f"catchuparr:recorder:{channel}"
    current_key = f"catchuparr:sharing:current:{channel}"
    episode_key = f"catchuparr:sharing:episode:{channel}"
    live_diagnostic_key = f"catchuparr:recorder:diagnostic:{channel}"
    error_diagnostic_key = f"catchuparr:recorder:last-error:{channel}"
    private_key = f"catchuparr:sharing:private:{channel}"
    client_set_key = f"live:channel:{channel}:clients"
    client_key = f"live:channel:{channel}:clients:{grant.client_id}"
    owner_key = f"live:channel:{channel}:owner"
    heartbeat_key = f"live:worker:{grant.metadata_owner}:heartbeat"
    metadata_key = f"live:channel:{channel}:metadata"
    client_stop_key = f"live:channel:{channel}:client:{grant.client_id}:stop"
    stopping_key = f"live:channel:{channel}:stopping"
    checks = {
        "identifier_shapes_valid": False,
        "native_owner_shapes_valid": False,
        "generation_shapes_valid": False,
        "control_fence_valid": False,
        "lease_value_present": False,
        "redis_reads_available": False,
        "key_types_match": False,
        "lease_and_episode_live": False,
        "current_authority_matches": False,
        "episode_grant_matches": False,
        "private_client_registered": False,
        "client_record_live": False,
        "owner_lock_and_heartbeat_live": False,
        "metadata_matches": False,
        "stop_markers_clear": False,
    }
    try:
        checks["identifier_shapes_valid"] = all(
            isinstance(value, str) and _SAFE_ID.fullmatch(value)
            for value in (
                grant.episode_id, grant.client_id, grant.stream_id, grant.account_id,
                grant.m3u_profile_id, grant.input_profile_id,
            )
        )
        checks["native_owner_shapes_valid"] = all(
            isinstance(value, str) and _SAFE_NATIVE_OWNER.fullmatch(value)
            for value in (grant.worker_id, grant.owner_lock_value, grant.metadata_owner)
        )
        checks["generation_shapes_valid"] = bool(
            isinstance(grant.configuration_generation, str)
            and _HEX64.fullmatch(grant.configuration_generation)
            and isinstance(grant.catalog_generation, str)
            and _HEX64.fullmatch(grant.catalog_generation)
        )
        checks["control_fence_valid"] = bool(
            type(grant.control_generation) is int and grant.control_generation >= 0
            and type(grant.recorder_fence) is int and grant.recorder_fence >= 1
            and grant.phase in {"active", "wait_return"}
        )
        checks["lease_value_present"] = isinstance(lease_value, str) and bool(lease_value)
        current = _decode_hash(redis_client.hgetall(current_key))
        episode = _decode_hash(redis_client.hgetall(episode_key))
        private = _decode_hash(redis_client.hgetall(private_key))
        client = _decode_hash(redis_client.hgetall(client_key))
        metadata = _decode_hash(redis_client.hgetall(metadata_key))
        owner = _text(redis_client.get(owner_key))
        live_types = tuple(_text(redis_client.type(key)) for key in (
            lease_key, current_key, episode_key, live_diagnostic_key,
            error_diagnostic_key, private_key, client_set_key, client_key,
            client_stop_key, owner_key, heartbeat_key, metadata_key, stopping_key,
        ))
        checks["redis_reads_available"] = True
        checks["key_types_match"] = bool(
            live_types[0:3] == ("string", "hash", "hash")
            and live_types[3] in {"none", "hash"}
            and live_types[4] in {"none", "hash"}
            and live_types[5:8] == ("hash", "set", "hash")
            and live_types[8] == "none"
            and live_types[9:12] == ("string", "string", "hash")
            and live_types[12] in {"none", "string"}
        )
        checks["lease_and_episode_live"] = bool(
            _text(redis_client.get(lease_key)) == lease_value
            and int(redis_client.ttl(lease_key)) > 0
            and int(redis_client.ttl(episode_key)) > 0
        )
        checks["current_authority_matches"] = bool(
            current.get("configuration_generation") == grant.configuration_generation
            and current.get("control_generation") == str(grant.control_generation)
            and current.get("catalog_generation") == grant.catalog_generation
            and int(current.get("authority_revision", "0")) >= 1
        )
        episode_fields = {
            "channel_uuid": channel, "episode_id": grant.episode_id,
            "worker_id": grant.worker_id, "stream_id": grant.stream_id,
            "account_id": grant.account_id, "m3u_profile_id": grant.m3u_profile_id,
            "input_profile_id": grant.input_profile_id,
            "catalog_generation": grant.catalog_generation,
            "configuration_generation": grant.configuration_generation,
            "control_generation": str(grant.control_generation),
            "recorder_fence": str(grant.recorder_fence),
            "lease_value": lease_value, "client_id": grant.client_id,
        }
        checks["episode_grant_matches"] = bool(
            all(episode.get(key) == value for key, value in episode_fields.items())
            and episode.get("phase") in {"active", "wait_return"}
        )
        checks["private_client_registered"] = bool(
            private.get(grant.client_id) == grant.episode_id
            and redis_client.sismember(client_set_key, grant.client_id)
        )
        checks["client_record_live"] = bool(
            client.get("worker_id") == grant.client_worker_id
            and client.get("connected_at") == grant.connected_at
            and int(redis_client.ttl(client_key)) > 0
        )
        checks["owner_lock_and_heartbeat_live"] = bool(
            owner == grant.owner_lock_value
            and int(redis_client.ttl(owner_key)) > 0
            and int(redis_client.ttl(heartbeat_key)) > 0
        )
        checks["metadata_matches"] = bool(
            metadata.get("owner") == grant.metadata_owner
            and metadata.get("stream_id") == grant.stream_id
            and metadata.get("m3u_profile") == grant.m3u_profile_id
            and metadata.get("stream_profile") == grant.input_profile_id
            and metadata.get("state") in {"active", "waiting_for_clients"}
            and metadata.get("channel_uuid", channel) == channel
        )
        checks["stop_markers_clear"] = not bool(
            redis_client.exists(client_stop_key) or redis_client.exists(stopping_key)
        )
    except Exception:
        pass
    return checks


def _capture_hash(redis_client, key):
    return {"values": redis_client.hgetall(key), "ttl": int(redis_client.ttl(key))}


def _wait_for_recording(redis_client, key, source, *, timeout=10):
    deadline = time.monotonic() + timeout
    while True:
        diagnostic = _decode_hash(redis_client.hgetall(key))
        if diagnostic.get("phase") == "recording" and diagnostic.get("source") == source:
            return diagnostic
        if time.monotonic() >= deadline:
            raise AssertionError(f"{source} recording diagnostic was not confirmed in time")
        time.sleep(0.05)


def _restore_hash(redis_client, key, record):
    redis_client.delete(key)
    if record["values"]:
        redis_client.hset(key, mapping=record["values"])
        if record["ttl"] > 0:
            redis_client.expire(key, record["ttl"])


def _decode_hash(values):
    return {_text(key): _text(value) for key, value in (values or {}).items()}


def _text(value):
    return value.decode("utf-8", errors="strict") if isinstance(value, bytes) else value
