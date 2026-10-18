"""Disposable-AIO acceptance checks for the recorder diagnostic Redis Lua CAS."""

from __future__ import annotations

import time
from types import SimpleNamespace


def probe_actual_recorder_diagnostics(
    redis_client,
    *,
    dedicated_channel_uuid: str,
    active_snapshot: dict,
    control_state,
    dedicated_candidate: dict,
    borrowed_channel_uuid: str,
    now: float | None = None,
):
    """Exercise both writer Lua scripts against live disposable AIO state.

    The dedicated channel must have a live recorder lease and assigned source;
    the borrowed channel must have an active, registered native borrow. The
    returned cleanup callback restores prior diagnostic hashes and removes the
    synthetic dedicated worker row after the caller has checked Plugin.run.
    """
    if dedicated_channel_uuid == borrowed_channel_uuid:
        raise AssertionError("diagnostic acceptance requires separate dedicated and borrowed channels")
    instant = float(time.time() if now is None else now)

    from catchuparr.adapters.recorder_proxy import worker_id_key
    from catchuparr.adapters.sharing_native import read_native_episode_grant
    from catchuparr.configuration import active_configuration_generation
    from catchuparr.recorder_diagnostics import (
        last_error_key,
        live_key,
        publish_borrowed_recorder_diagnostic,
        publish_recorder_diagnostic,
    )
    from catchuparr.recorder_proxy import issue_recorder_attempt

    diagnostic_keys = {
        live_key(dedicated_channel_uuid), last_error_key(dedicated_channel_uuid),
        live_key(borrowed_channel_uuid), last_error_key(borrowed_channel_uuid),
    }
    saved = {key: _capture_hash(redis_client, key) for key in diagnostic_keys}
    attempt = None
    dedicated_capability_key = None

    def cleanup():
        if attempt is not None:
            attempt.revoke(redis_client)
            redis_client.delete(worker_id_key(attempt.worker_id))
        if dedicated_capability_key:
            redis_client.delete(dedicated_capability_key)
        for key, record in saved.items():
            _restore_hash(redis_client, key, record)

    try:
        generation = active_configuration_generation(active_snapshot)
        control_generation = getattr(control_state, "generation", None)
        dedicated_lease_key = f"catchuparr:recorder:{dedicated_channel_uuid}"
        lease_value = _text(redis_client.get(dedicated_lease_key))
        if not lease_value or int(redis_client.ttl(dedicated_lease_key)) <= 0:
            raise AssertionError("dedicated recorder lease is not live")
        fence = int(lease_value.split(":", 1)[0])
        lease = SimpleNamespace(
            key=dedicated_lease_key, _value=lease_value, fence=fence,
        )
        attempt = issue_recorder_attempt(
            redis_client, lease, channel_uuid=dedicated_channel_uuid,
            candidate=dedicated_candidate, config_generation=generation,
            control_generation=control_generation,
            internal_base_url="http://dispatcharr",
        )
        dedicated_capability_key = attempt.capability_key
        common = {
            "worker_id": attempt.worker_id, "phase": "recording", "source": "dedicated",
            "stream_id": attempt.stream_id, "account_id": attempt.account_id,
            "started_at": instant, "config_generation": generation,
            "control_generation": control_generation, "fence": fence,
        }
        if not publish_recorder_diagnostic(
            redis_client, dedicated_channel_uuid, lease_value, **common, now=instant,
        ):
            raise AssertionError("dedicated diagnostic Lua rejected the exact live attempt")
        if publish_recorder_diagnostic(
            redis_client, dedicated_channel_uuid, "stale-lease", **common, now=instant,
        ):
            raise AssertionError("dedicated diagnostic Lua accepted a stale lease")
        if publish_recorder_diagnostic(
            redis_client, dedicated_channel_uuid, lease_value,
            **{**common, "config_generation": "0" * 64}, now=instant,
        ):
            raise AssertionError("dedicated diagnostic Lua accepted a stale configuration")
        if publish_recorder_diagnostic(
            redis_client, dedicated_channel_uuid, lease_value,
            **{**common, "fence": fence + 1}, now=instant,
        ):
            raise AssertionError("dedicated diagnostic Lua accepted a stale fence")
        if not publish_recorder_diagnostic(
            redis_client, dedicated_channel_uuid, lease_value,
            **{**common, "phase": "error", "error_code": "recorder_worker_stopped"},
            now=instant,
        ):
            raise AssertionError("dedicated error diagnostic Lua write failed")
        ttl = int(redis_client.ttl(last_error_key(dedicated_channel_uuid)))
        if not 0 < ttl <= 24 * 60 * 60:
            raise AssertionError("dedicated last-error diagnostic TTL is outside 24 hours")

        grant = read_native_episode_grant(redis_client, borrowed_channel_uuid)
        if grant is None:
            raise AssertionError("borrowed diagnostic acceptance requires a current native grant")
        borrowed_lease_key = f"catchuparr:recorder:{borrowed_channel_uuid}"
        borrowed_lease_value = _text(redis_client.get(borrowed_lease_key))
        if not borrowed_lease_value or int(redis_client.ttl(borrowed_lease_key)) <= 0:
            raise AssertionError("borrowed recorder lease is not live")
        borrowed_common = {
            "episode_id": grant.episode_id, "client_id": grant.client_id,
            "worker_id": grant.worker_id, "stream_id": grant.stream_id,
            "account_id": grant.account_id, "m3u_profile_id": grant.m3u_profile_id,
            "input_profile_id": grant.input_profile_id,
            "catalog_generation": grant.catalog_generation,
            "phase": "recording", "started_at": instant,
            "config_generation": grant.configuration_generation,
            "control_generation": grant.control_generation,
            "fence": grant.recorder_fence,
        }
        if not publish_borrowed_recorder_diagnostic(
            redis_client, borrowed_channel_uuid, borrowed_lease_value,
            **borrowed_common, now=instant,
        ):
            raise AssertionError("borrowed diagnostic Lua rejected the exact live grant/client")
        denials = (
            {"lease_value": "stale-lease"},
            {"config_generation": "0" * 64},
            {"fence": grant.recorder_fence + 1},
            {"client_id": "catchuparr-client-not-a-member"},
        )
        for override in denials:
            lease_override = override.pop("lease_value", borrowed_lease_value)
            if publish_borrowed_recorder_diagnostic(
                redis_client, borrowed_channel_uuid, lease_override,
                **{**borrowed_common, **override}, now=instant,
            ):
                raise AssertionError("borrowed diagnostic Lua accepted a stale binding")
        if not publish_borrowed_recorder_diagnostic(
            redis_client, borrowed_channel_uuid, borrowed_lease_value,
            **{**borrowed_common, "phase": "error",
               "error_code": "borrowed_recorder_worker_stopped"}, now=instant,
        ):
            raise AssertionError("borrowed error diagnostic Lua write failed")
        ttl = int(redis_client.ttl(last_error_key(borrowed_channel_uuid)))
        if not 0 < ttl <= 24 * 60 * 60:
            raise AssertionError("borrowed last-error diagnostic TTL is outside 24 hours")
    except Exception:
        cleanup()
        raise
    return {
        "channels": [
            {"channel_uuid": dedicated_channel_uuid, "mode": "dedicated"},
            {"channel_uuid": borrowed_channel_uuid, "mode": "borrowed"},
        ],
        "cleanup": cleanup,
    }


def _capture_hash(redis_client, key):
    raw = redis_client.hgetall(key)
    ttl = int(redis_client.ttl(key))
    return {"values": raw, "ttl": ttl}


def _restore_hash(redis_client, key, record):
    redis_client.delete(key)
    if record["values"]:
        redis_client.hset(key, mapping=record["values"])
        if record["ttl"] > 0:
            redis_client.expire(key, record["ttl"])


def _text(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="strict")
    return value
