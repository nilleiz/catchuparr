"""Exercise recorder diagnostic Lua CAS against live native AIO state."""

from __future__ import annotations

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
    diagnostic = _wait_for_recording(redis_client, live_diagnostic_key, "dedicated")
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
    diagnostic = _wait_for_recording(redis_client, live_diagnostic_key, "borrowed")
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
