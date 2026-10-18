"""Lease-bound, privacy-safe recorder heartbeat diagnostics."""

from __future__ import annotations

import hashlib
import math
import re
import time
from typing import Any

LIVE_PREFIX = "catchuparr:recorder:diagnostic:"
ERROR_PREFIX = "catchuparr:recorder:last-error:"
LIVE_TTL_CAP_SECONDS = 120
LAST_ERROR_TTL_SECONDS = 24 * 60 * 60

PHASES = frozenset({"starting", "recording", "no_media_progress", "cleanup_pending", "error"})
SOURCES = frozenset({"dedicated", "borrowed", "unconfirmed"})
ERROR_CODES = frozenset({
    "recorder_worker_stopped", "borrowed_recorder_worker_stopped",
    "borrow_cleanup_pending", "sharing_cleanup_pending",
    "sharing_attempt_cleanup_pending", "sharing_attempt_reconciled",
    "sharing_observation_failed", "sharing_projection_failed",
    "sharing_registration_failed", "sharing_binding_lost",
    "sharing_recovery_pending", "sharing_recovery_failed",
    "sharing_recovery_stale", "supervision_failed",
    "control_state_invalid", "schedule_install_failed", "runtime_disabled",
})
_SAFE_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")


_PUBLISH_LUA = r"""
local function kind(key) return redis.call('TYPE', key).ok end
local function finite_number(value)
  local number = tonumber(value)
  if not number or number ~= number or number == math.huge or number == -math.huge then return nil end
  return number
end
if kind(KEYS[1]) ~= 'string' or kind(KEYS[2]) ~= 'hash'
   or (kind(KEYS[3]) ~= 'none' and kind(KEYS[3]) ~= 'hash')
   or (kind(KEYS[4]) ~= 'none' and kind(KEYS[4]) ~= 'hash') then return 0 end
local ttl = redis.call('TTL', KEYS[1])
local started = finite_number(ARGV[8])
local now = finite_number(ARGV[9])
local fence = finite_number(ARGV[7])
local control = finite_number(ARGV[6])
if ttl <= 0 or ttl > 86400 or not started or started <= 0 or not now
   or not fence or fence <= 0 or not control or control < 0 then return 0 end
if redis.call('GET', KEYS[1]) ~= ARGV[1]
   or redis.call('HGET', KEYS[2], 'worker_id') ~= ARGV[2]
   or redis.call('HGET', KEYS[2], 'channel_uuid') ~= ARGV[3]
   or redis.call('HGET', KEYS[2], 'stream_id') ~= ARGV[4]
   or redis.call('HGET', KEYS[2], 'account_id') ~= ARGV[5]
   or redis.call('HGET', KEYS[2], 'config_generation') ~= ARGV[10]
   or redis.call('HGET', KEYS[2], 'control_generation') ~= ARGV[6]
   or redis.call('HGET', KEYS[2], 'lease_fence') ~= ARGV[7]
   or redis.call('HGET', KEYS[2], 'lease_key') ~= KEYS[1]
   or redis.call('HGET', KEYS[2], 'lease_value') ~= ARGV[1] then return 0 end
local diagnostic_ttl = math.min(ttl, tonumber(ARGV[13]))
if not diagnostic_ttl or diagnostic_ttl < 1 then return 0 end
redis.call('HSET', KEYS[3],
  'channel_uuid', ARGV[3], 'worker_id', ARGV[2],
  'attempt_digest', ARGV[11], 'stream_id', ARGV[4], 'account_id', ARGV[5],
  'config_generation', ARGV[10], 'control_generation', ARGV[6],
  'fence', ARGV[7], 'phase', ARGV[14], 'source', ARGV[15],
  'started_at', ARGV[8], 'updated_at', ARGV[9], 'error_code', ARGV[16])
redis.call('EXPIRE', KEYS[3], diagnostic_ttl)
if ARGV[14] == 'error' and ARGV[16] ~= '' then
  redis.call('HSET', KEYS[4],
    'channel_uuid', ARGV[3], 'worker_id', ARGV[2],
    'attempt_digest', ARGV[11], 'stream_id', ARGV[4], 'account_id', ARGV[5],
    'config_generation', ARGV[10], 'control_generation', ARGV[6],
    'fence', ARGV[7], 'phase', 'error', 'source', ARGV[15],
    'started_at', ARGV[8], 'updated_at', ARGV[9], 'error_code', ARGV[16])
  redis.call('EXPIRE', KEYS[4], tonumber(ARGV[12]))
end
return 1
"""

_PUBLISH_BORROWED_LUA = r"""
local function kind(key) return redis.call('TYPE', key).ok end
local function finite_number(value)
  local number = tonumber(value)
  if not number or number ~= number or number == math.huge or number == -math.huge then return nil end
  return number
end
if kind(KEYS[1]) ~= 'string' or kind(KEYS[2]) ~= 'hash' or kind(KEYS[3]) ~= 'hash'
   or (kind(KEYS[4]) ~= 'none' and kind(KEYS[4]) ~= 'hash')
   or (kind(KEYS[5]) ~= 'none' and kind(KEYS[5]) ~= 'hash')
   or kind(KEYS[6]) ~= 'hash' or kind(KEYS[7]) ~= 'set'
   or kind(KEYS[8]) ~= 'hash' or kind(KEYS[9]) ~= 'none' then return 0 end
local lease_ttl = redis.call('TTL', KEYS[1])
local episode_ttl = redis.call('TTL', KEYS[3])
local started = finite_number(ARGV[13])
local now = finite_number(ARGV[14])
local fence = finite_number(ARGV[12])
local control = finite_number(ARGV[11])
local authority_revision = finite_number(redis.call('HGET', KEYS[2], 'authority_revision') or '')
if lease_ttl <= 0 or episode_ttl <= 0 or not started or started <= 0 or not now
   or not fence or fence <= 0 or not control or control < 0
   or not authority_revision or authority_revision < 1 then return 0 end
local grant_phase = redis.call('HGET', KEYS[3], 'phase')
local client_worker_id = redis.call('HGET', KEYS[3], 'client_worker_id')
local connected_at = redis.call('HGET', KEYS[3], 'connected_at')
if redis.call('GET', KEYS[1]) ~= ARGV[1]
   or redis.call('HGET', KEYS[2], 'configuration_generation') ~= ARGV[10]
   or redis.call('HGET', KEYS[2], 'control_generation') ~= ARGV[11]
   or redis.call('HGET', KEYS[2], 'catalog_generation') ~= ARGV[9]
   or redis.call('HGET', KEYS[3], 'channel_uuid') ~= ARGV[2]
   or redis.call('HGET', KEYS[3], 'episode_id') ~= ARGV[3]
   or redis.call('HGET', KEYS[3], 'worker_id') ~= ARGV[4]
   or redis.call('HGET', KEYS[3], 'stream_id') ~= ARGV[5]
   or redis.call('HGET', KEYS[3], 'account_id') ~= ARGV[6]
   or redis.call('HGET', KEYS[3], 'm3u_profile_id') ~= ARGV[7]
   or redis.call('HGET', KEYS[3], 'input_profile_id') ~= ARGV[8]
   or redis.call('HGET', KEYS[3], 'catalog_generation') ~= ARGV[9]
   or redis.call('HGET', KEYS[3], 'configuration_generation') ~= ARGV[10]
   or redis.call('HGET', KEYS[3], 'control_generation') ~= ARGV[11]
   or redis.call('HGET', KEYS[3], 'recorder_fence') ~= ARGV[12]
   or redis.call('HGET', KEYS[3], 'lease_value') ~= ARGV[1]
   or redis.call('HGET', KEYS[3], 'client_id') ~= ARGV[21]
   or redis.call('HGET', KEYS[6], ARGV[21]) ~= ARGV[3]
   or redis.call('SISMEMBER', KEYS[7], ARGV[21]) ~= 1
   or redis.call('HGET', KEYS[8], 'worker_id') ~= client_worker_id
   or redis.call('HGET', KEYS[8], 'connected_at') ~= connected_at
   or redis.call('TTL', KEYS[8]) <= 0
   or not client_worker_id or not connected_at
   or (grant_phase ~= 'active' and grant_phase ~= 'wait_return') then return 0 end
local diagnostic_ttl = math.min(lease_ttl, episode_ttl, tonumber(ARGV[20]))
if not diagnostic_ttl or diagnostic_ttl < 1 then return 0 end
redis.call('HSET', KEYS[4],
  'channel_uuid', ARGV[2], 'worker_digest', ARGV[15], 'episode_digest', ARGV[16],
  'attempt_digest', ARGV[15], 'stream_id', ARGV[5], 'account_id', ARGV[6],
  'm3u_profile_id', ARGV[7], 'input_profile_id', ARGV[8],
  'catalog_generation', ARGV[9], 'config_generation', ARGV[10],
  'control_generation', ARGV[11], 'fence', ARGV[12],
  'phase', ARGV[17], 'source', 'borrowed', 'started_at', ARGV[13],
  'updated_at', ARGV[14], 'error_code', ARGV[18])
redis.call('EXPIRE', KEYS[4], diagnostic_ttl)
if ARGV[17] == 'error' and ARGV[18] ~= '' then
  redis.call('HSET', KEYS[5],
    'channel_uuid', ARGV[2], 'worker_digest', ARGV[15], 'episode_digest', ARGV[16],
    'attempt_digest', ARGV[15], 'stream_id', ARGV[5], 'account_id', ARGV[6],
    'm3u_profile_id', ARGV[7], 'input_profile_id', ARGV[8],
    'catalog_generation', ARGV[9], 'config_generation', ARGV[10],
    'control_generation', ARGV[11], 'fence', ARGV[12],
    'phase', 'error', 'source', 'borrowed', 'started_at', ARGV[13],
    'updated_at', ARGV[14], 'error_code', ARGV[18])
  redis.call('EXPIRE', KEYS[5], tonumber(ARGV[19]))
end
return 1
"""


def live_key(channel_uuid: str) -> str:
    return LIVE_PREFIX + _channel_id(channel_uuid)


def last_error_key(channel_uuid: str) -> str:
    return ERROR_PREFIX + _channel_id(channel_uuid)


def publish_recorder_diagnostic(
    redis,
    channel_uuid: str,
    lease_value: str,
    *,
    worker_id: str,
    phase: str,
    source: str,
    stream_id: str,
    account_id: str,
    started_at: float,
    config_generation: str,
    control_generation: int,
    fence: int,
    error_code: str | None = None,
    now: float | None = None,
) -> bool:
    """Atomically publish only while the exact leased attempt still owns channel.

    The lease value and worker identity are used for comparisons/digests but
    are never stored in diagnostic hashes. False means the attempt no longer
    proves current ownership or the backend rejected the safe write.
    """
    try:
        channel_uuid = _channel_id(channel_uuid)
        if not isinstance(lease_value, str) or not lease_value:
            return False
        from .adapters.recorder_proxy import is_managed_worker_id, worker_id_key

        if not is_managed_worker_id(worker_id):
            return False
        if phase not in PHASES or source not in SOURCES:
            return False
        if error_code is None:
            error_code = ""
        if error_code and error_code not in ERROR_CODES:
            return False
        if phase == "error" and not error_code:
            return False
        if phase != "error" and error_code:
            return False
        if not _HEX64.fullmatch(str(config_generation)):
            return False
        if type(control_generation) is not int or control_generation < 0:
            return False
        if type(fence) is not int or fence < 1:
            return False
        if not _SAFE_ID.fullmatch(str(stream_id)) or not _SAFE_ID.fullmatch(str(account_id)):
            return False
        started_at = float(started_at)
        current_time = float(time.time() if now is None else now)
        if not math.isfinite(started_at) or started_at <= 0 or not math.isfinite(current_time):
            return False
        worker_key = worker_id_key(worker_id)
        result = redis.eval(
            _PUBLISH_LUA,
            4,
            f"catchuparr:recorder:{channel_uuid}",
            worker_key,
            live_key(channel_uuid),
            last_error_key(channel_uuid),
            lease_value,
            worker_id,
            channel_uuid,
            str(stream_id),
            str(account_id),
            str(control_generation),
            str(fence),
            str(started_at),
            str(current_time),
            str(config_generation),
            hashlib.sha256(worker_id.encode("ascii")).hexdigest(),
            str(LAST_ERROR_TTL_SECONDS),
            str(LIVE_TTL_CAP_SECONDS),
            phase,
            source,
            error_code,
        )
        return type(result) is int and result == 1
    except Exception:
        return False


def publish_borrowed_recorder_diagnostic(
    redis,
    channel_uuid: str,
    lease_value: str,
    *,
    episode_id: str,
    client_id: str,
    worker_id: str,
    stream_id: str,
    account_id: str,
    m3u_profile_id: str,
    input_profile_id: str,
    catalog_generation: str,
    phase: str,
    started_at: float,
    config_generation: str,
    control_generation: int,
    fence: int,
    error_code: str | None = None,
    now: float | None = None,
) -> bool:
    """Publish a borrowed reader heartbeat under exact live grant/lease CAS."""
    try:
        channel_uuid = _channel_id(channel_uuid)
        if not isinstance(lease_value, str) or not lease_value:
            return False
        if any(not _SAFE_ID.fullmatch(str(value)) for value in (
            episode_id, client_id, worker_id, stream_id, account_id, m3u_profile_id, input_profile_id,
        )):
            return False
        if not _HEX64.fullmatch(str(config_generation)) or not _HEX64.fullmatch(str(catalog_generation)):
            return False
        if type(control_generation) is not int or control_generation < 0 or type(fence) is not int or fence < 1:
            return False
        if phase not in PHASES:
            return False
        error_code = "" if error_code is None else error_code
        if error_code and error_code not in ERROR_CODES:
            return False
        if (phase == "error") != bool(error_code):
            return False
        started_at = float(started_at)
        current_time = float(time.time() if now is None else now)
        if not math.isfinite(started_at) or started_at <= 0 or not math.isfinite(current_time):
            return False
        result = redis.eval(
            _PUBLISH_BORROWED_LUA, 9,
            f"catchuparr:recorder:{channel_uuid}",
            f"catchuparr:sharing:current:{channel_uuid}",
            f"catchuparr:sharing:episode:{channel_uuid}",
            live_key(channel_uuid), last_error_key(channel_uuid),
            f"catchuparr:sharing:private:{channel_uuid}",
            f"live:channel:{channel_uuid}:clients",
            f"live:channel:{channel_uuid}:clients:{client_id}",
            f"live:channel:{channel_uuid}:client:{client_id}:stop",
            lease_value, channel_uuid, episode_id, worker_id, stream_id, account_id,
            m3u_profile_id, input_profile_id, catalog_generation,
            str(config_generation), str(control_generation), str(fence),
            str(started_at), str(current_time),
            hashlib.sha256(worker_id.encode("utf-8")).hexdigest(),
            hashlib.sha256(episode_id.encode("utf-8")).hexdigest(),
            phase, error_code, str(LAST_ERROR_TTL_SECONDS), str(LIVE_TTL_CAP_SECONDS),
            client_id,
        )
        return type(result) is int and result == 1
    except Exception:
        return False


def read_recorder_diagnostics(redis, settings: dict, control_state, *, now=None) -> dict[str, dict[str, Any]]:
    """Read sanitized diagnostics for selected channels, validating owner state."""
    from .adapters.recorder_proxy import read_worker_record
    from .configuration import active_configuration_generation

    try:
        config_generation = active_configuration_generation(settings)
        channel_ids = _selected_channel_ids(settings)
    except Exception:
        return {}
    if control_state is None or redis is None:
        return {}
    result: dict[str, dict[str, Any]] = {}
    for channel_uuid in channel_ids:
        try:
            raw = _hash(redis.hgetall(live_key(channel_uuid)))
            ttl = int(redis.ttl(live_key(channel_uuid)))
            if raw and 0 < ttl <= LIVE_TTL_CAP_SECONDS:
                record = _validated_record(raw, channel_uuid, config_generation, control_state)
                if record is not None:
                    valid_owner = (
                        _borrowed_grant_match(redis, record, channel_uuid)
                        if record.get("source") == "borrowed"
                        else _worker_and_lease_match(
                            redis, read_worker_record(redis, record.get("worker_id", "")),
                            record, channel_uuid,
                        )
                    )
                    if valid_owner:
                        result[channel_uuid] = record
                        continue
            last = _hash(redis.hgetall(last_error_key(channel_uuid)))
            error_ttl = int(redis.ttl(last_error_key(channel_uuid)))
            if last and 0 < error_ttl <= LAST_ERROR_TTL_SECONDS:
                record = _validated_record(last, channel_uuid, config_generation, control_state)
                if record is not None and record.get("phase") == "error":
                    result[channel_uuid] = record
        except Exception:
            continue
    return result


def _worker_and_lease_match(redis, worker, record, channel_uuid):
    if not worker:
        return False
    expected = {
        "worker_id": record["worker_id"], "channel_uuid": channel_uuid,
        "stream_id": record["stream_id"], "account_id": record["account_id"],
        "config_generation": record["config_generation"],
        "control_generation": str(record["control_generation"]),
        "lease_fence": str(record["fence"]),
        "lease_key": f"catchuparr:recorder:{channel_uuid}",
    }
    if any(worker.get(key) != value for key, value in expected.items()):
        return False
    lease_value = worker.get("lease_value")
    if not lease_value:
        return False
    try:
        current = redis.get(expected["lease_key"])
        if isinstance(current, bytes):
            current = current.decode("utf-8", errors="strict")
        return current == lease_value and int(redis.ttl(expected["lease_key"])) > 0
    except Exception:
        return False


def _borrowed_grant_match(redis, record, channel_uuid):
    try:
        from .adapters.sharing_native import read_native_episode_grant

        grant = read_native_episode_grant(redis, channel_uuid)
        if grant is None:
            return False
        expected = (
            record.get("episode_digest") == hashlib.sha256(grant.episode_id.encode()).hexdigest()
            and record.get("worker_digest") == hashlib.sha256(grant.worker_id.encode()).hexdigest()
            and record.get("stream_id") == grant.stream_id
            and record.get("account_id") == grant.account_id
            and record.get("m3u_profile_id") == grant.m3u_profile_id
            and record.get("input_profile_id") == grant.input_profile_id
            and record.get("catalog_generation") == grant.catalog_generation
            and record.get("config_generation") == grant.configuration_generation
            and record.get("control_generation") == grant.control_generation
            and record.get("fence") == grant.recorder_fence
        )
        if not expected:
            return False
        current = _hash(redis.hgetall(f"catchuparr:sharing:current:{channel_uuid}"))
        if (current.get("configuration_generation") != grant.configuration_generation
                or current.get("control_generation") != str(grant.control_generation)
                or current.get("catalog_generation") != grant.catalog_generation):
            return False
        if int(current.get("authority_revision", "0")) < 1:
            return False
        private_map = _hash(redis.hgetall(f"catchuparr:sharing:private:{channel_uuid}"))
        if private_map.get(grant.client_id) != grant.episode_id:
            return False
        if not redis.sismember(f"live:channel:{channel_uuid}:clients", grant.client_id):
            return False
        client = _hash(redis.hgetall(
            f"live:channel:{channel_uuid}:clients:{grant.client_id}"
        ))
        if (client.get("worker_id") != grant.client_worker_id
                or client.get("connected_at") != grant.connected_at
                or int(redis.ttl(f"live:channel:{channel_uuid}:clients:{grant.client_id}")) <= 0
                or redis.exists(f"live:channel:{channel_uuid}:client:{grant.client_id}:stop")):
            return False
        lease = redis.get(f"catchuparr:recorder:{channel_uuid}")
        if isinstance(lease, bytes):
            lease = lease.decode("utf-8", errors="strict")
        return lease == grant.lease_value and int(redis.ttl(f"catchuparr:recorder:{channel_uuid}")) > 0
    except Exception:
        return False


def _validated_record(raw, channel_uuid, config_generation, control_state):
    try:
        if (raw.get("channel_uuid") != channel_uuid
                or raw.get("config_generation") != config_generation
                or int(raw.get("control_generation", "-1")) != control_state.generation
                or raw.get("phase") not in PHASES
                or raw.get("source") not in SOURCES
                or raw.get("error_code", "") not in ERROR_CODES | {""}
                or not _HEX64.fullmatch(raw.get("attempt_digest", ""))):
            return None
        record = dict(raw)
        record["fence"] = int(record["fence"])
        record["control_generation"] = int(record["control_generation"])
        record["started_at"] = float(record["started_at"])
        record["updated_at"] = float(record["updated_at"])
        if record["fence"] < 1 or not math.isfinite(record["started_at"]) or not math.isfinite(record["updated_at"]):
            return None
        if not _SAFE_ID.fullmatch(record.get("stream_id", "")) or not _SAFE_ID.fullmatch(record.get("account_id", "")):
            return None
        if record.get("source") == "borrowed":
            if (not _HEX64.fullmatch(record.get("episode_digest", ""))
                    or not _HEX64.fullmatch(record.get("worker_digest", ""))
                    or not _SAFE_ID.fullmatch(record.get("m3u_profile_id", ""))
                    or not _SAFE_ID.fullmatch(record.get("input_profile_id", ""))
                    or not _HEX64.fullmatch(record.get("catalog_generation", ""))):
                return None
        elif not record.get("worker_id"):
            return None
        return record
    except (TypeError, ValueError, KeyError):
        return None


def _hash(raw):
    if not raw:
        return {}
    try:
        result = {}
        for key, value in raw.items():
            if isinstance(key, bytes):
                key = key.decode("utf-8", errors="strict")
            if isinstance(value, bytes):
                value = value.decode("utf-8", errors="strict")
            if not isinstance(key, str) or not isinstance(value, str):
                return {}
            result[key] = value
        return result
    except Exception:
        return {}


def _selected_channel_ids(settings):
    import uuid

    raw = settings.get("channel_uuids", "")
    values = raw if isinstance(raw, (list, tuple)) else str(raw or "").replace(",", "\n").splitlines()
    result = []
    for value in values:
        try:
            channel = str(uuid.UUID(str(value).strip()))
        except (ValueError, AttributeError, TypeError):
            continue
        if channel not in result:
            result.append(channel)
    return result


def _channel_id(value):
    import uuid

    return str(uuid.UUID(str(value)))
