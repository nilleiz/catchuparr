"""Applied source policy routing and signed internal recorder capabilities."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import secrets
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Mapping

from .source_rules import SourcePolicy, rank_candidates

logger = logging.getLogger(__name__)

CAPABILITY_PREFIX = "catchuparr:recorder-proxy:capability:"
CAPABILITY_TTL_SECONDS = 45
CAPABILITY_HEADER = "X-Catchuparr-Recorder"
SHARING_OPERATION_PREFIX = "catchuparr:sharing:attempt:"

_ISSUE_SHARING_ATTEMPT_LUA = """
local function kind(key) return redis.call('TYPE', key).ok end
if kind(KEYS[1]) ~= 'hash' or kind(KEYS[2]) ~= 'string'
   or kind(KEYS[3]) ~= 'none' or (kind(KEYS[4]) ~= 'none' and kind(KEYS[4]) ~= 'string')
   or kind(KEYS[5]) ~= 'none' or kind(KEYS[6]) ~= 'none'
   or kind(KEYS[7]) ~= 'none' then return 0 end
local revision = tonumber(ARGV[1])
local fence = tonumber(ARGV[5])
local control = tonumber(ARGV[4])
local authority_revision = tonumber(ARGV[6])
local sequence = tonumber(redis.call('GET', KEYS[4]) or '0')
if not revision or revision < 1 or revision > 9007199254740990 or revision % 1 ~= 0
   or not fence or fence < 1 or fence % 1 ~= 0
   or not control or control < 0 or control % 1 ~= 0
   or not authority_revision or authority_revision < 1 or authority_revision % 1 ~= 0
   or not sequence or sequence < 0 or sequence >= 9007199254740990 or sequence % 1 ~= 0
   or revision ~= sequence + 1 then return 0 end
if redis.call('HGET', KEYS[1], 'configuration_generation') ~= ARGV[2]
   or redis.call('HGET', KEYS[1], 'control_generation') ~= ARGV[4]
   or redis.call('HGET', KEYS[1], 'catalog_generation') ~= ARGV[3]
   or redis.call('HGET', KEYS[1], 'authority_revision') ~= ARGV[6]
   or redis.call('GET', KEYS[2]) ~= ARGV[7]
   or redis.call('TTL', KEYS[2]) <= 0 then return 0 end
local current_value = ARGV[8] .. ':' .. ARGV[1]
local operation_ttl = tonumber(ARGV[17])
local capability_ttl = tonumber(ARGV[16])
if not operation_ttl or operation_ttl < 60 or operation_ttl > 2592000
   or not capability_ttl or capability_ttl < 1 or capability_ttl > 120
   or ARGV[8] == '' or ARGV[9] == '' or ARGV[10] == '' or ARGV[11] == ''
   or ARGV[12] == '' or ARGV[13] == '' or ARGV[14] == '' or ARGV[15] == '' then return 0 end
redis.call('INCR', KEYS[4])
redis.call('SET', KEYS[3], current_value)
redis.call('HSET', KEYS[5],
  'operation_id', ARGV[8], 'operation_revision', ARGV[1], 'status', 'active',
  'channel_uuid', ARGV[18], 'configuration_generation', ARGV[2],
  'control_generation', ARGV[4], 'catalog_generation', ARGV[3],
  'authority_revision', ARGV[6], 'recorder_fence', ARGV[5],
  'worker_id', ARGV[9], 'capability_digest', ARGV[10],
  'stream_id', ARGV[12], 'account_id', ARGV[13],
  'lease_key', KEYS[2], 'lease_value', ARGV[7], 'open_count', '0')
redis.call('EXPIRE', KEYS[5], operation_ttl)
redis.call('SET', KEYS[6], ARGV[11], 'EX', capability_ttl)
redis.call('HSET', KEYS[7], 'worker_id', ARGV[9], 'channel_uuid', ARGV[18],
  'stream_id', ARGV[12], 'account_id', ARGV[13], 'config_generation', ARGV[2],
  'control_generation', ARGV[4], 'lease_fence', ARGV[5], 'lease_key', KEYS[2],
  'lease_value', ARGV[7], 'capability_digest', ARGV[10],
  'reservation_id', ARGV[14], 'reservation_state', 'none',
  'state', 'issued', 'created_at', ARGV[15],
  'sharing_operation_id', ARGV[8], 'sharing_operation_revision', ARGV[1],
  'sharing_authority_revision', ARGV[6], 'sharing_catalog_generation', ARGV[3])
redis.call('EXPIRE', KEYS[7], operation_ttl)
return revision
"""

_BEGIN_SHARING_OPERATION_OPEN_LUA = """
local function kind(key) return redis.call('TYPE', key).ok end
if kind(KEYS[1]) ~= 'hash' or kind(KEYS[2]) ~= 'string'
   or kind(KEYS[3]) ~= 'string' or kind(KEYS[4]) ~= 'hash'
   or kind(KEYS[5]) ~= 'string' or kind(KEYS[6]) ~= 'hash'
   or kind(KEYS[7]) ~= 'none' then return 0 end
local count = tonumber(redis.call('HGET', KEYS[4], 'open_count') or '')
if redis.call('HGET', KEYS[4], 'status') ~= 'active'
   or redis.call('GET', KEYS[3]) ~= ARGV[1] .. ':' .. ARGV[2]
   or redis.call('HGET', KEYS[4], 'operation_id') ~= ARGV[1]
   or redis.call('HGET', KEYS[4], 'operation_revision') ~= ARGV[2]
   or redis.call('HGET', KEYS[4], 'worker_id') ~= ARGV[8]
   or redis.call('HGET', KEYS[4], 'capability_digest') ~= ARGV[9]
   or redis.call('HGET', KEYS[4], 'authority_revision') ~= ARGV[3]
   or redis.call('HGET', KEYS[4], 'configuration_generation') ~= ARGV[4]
   or redis.call('HGET', KEYS[4], 'control_generation') ~= ARGV[5]
   or redis.call('HGET', KEYS[4], 'catalog_generation') ~= ARGV[6]
   or redis.call('HGET', KEYS[4], 'recorder_fence') ~= ARGV[7]
   or redis.call('HGET', KEYS[1], 'authority_revision') ~= ARGV[3]
   or redis.call('HGET', KEYS[1], 'configuration_generation') ~= ARGV[4]
   or redis.call('HGET', KEYS[1], 'control_generation') ~= ARGV[5]
   or redis.call('HGET', KEYS[1], 'catalog_generation') ~= ARGV[6]
   or redis.call('GET', KEYS[2]) ~= ARGV[10]
   or redis.call('TTL', KEYS[2]) <= 0
   or not count or count < 0 or count >= 1 or count % 1 ~= 0
   or redis.call('EXISTS', KEYS[5]) ~= 1
   or redis.call('HGET', KEYS[6], 'sharing_operation_id') ~= ARGV[1]
   or redis.call('HGET', KEYS[6], 'sharing_operation_revision') ~= ARGV[2]
   or redis.call('HGET', KEYS[6], 'state') == 'released'
   or redis.call('HGET', KEYS[6], 'state') == 'failed'
   or redis.call('GET', KEYS[5]) ~= ARGV[12]
   or ARGV[11] == '' or ARGV[12] == '' then return 0 end
redis.call('SET', KEYS[7], 'pending:' .. ARGV[1] .. ':' .. ARGV[2] .. ':' .. ARGV[11])
redis.call('HINCRBY', KEYS[4], 'open_count', 1)
return 1
"""

_ACTIVATE_SHARING_OPERATION_OPEN_LUA = """
local function kind(key) return redis.call('TYPE', key).ok end
if kind(KEYS[1]) ~= 'hash' or kind(KEYS[2]) ~= 'string'
   or kind(KEYS[3]) ~= 'string' or kind(KEYS[4]) ~= 'hash'
   or kind(KEYS[5]) ~= 'string' or kind(KEYS[6]) ~= 'hash'
   or kind(KEYS[7]) ~= 'string' then return 0 end
local pending = 'pending:' .. ARGV[1] .. ':' .. ARGV[2] .. ':' .. ARGV[11]
local active = 'active:' .. ARGV[1] .. ':' .. ARGV[2] .. ':' .. ARGV[11]
local state = redis.call('GET', KEYS[7])
if state ~= pending and state ~= active
   or redis.call('HGET', KEYS[4], 'status') ~= 'active'
   or redis.call('GET', KEYS[3]) ~= ARGV[1] .. ':' .. ARGV[2]
   or redis.call('HGET', KEYS[4], 'operation_id') ~= ARGV[1]
   or redis.call('HGET', KEYS[4], 'operation_revision') ~= ARGV[2]
   or redis.call('HGET', KEYS[4], 'worker_id') ~= ARGV[8]
   or redis.call('HGET', KEYS[4], 'capability_digest') ~= ARGV[9]
   or redis.call('HGET', KEYS[1], 'authority_revision') ~= ARGV[3]
   or redis.call('HGET', KEYS[1], 'configuration_generation') ~= ARGV[4]
   or redis.call('HGET', KEYS[1], 'control_generation') ~= ARGV[5]
   or redis.call('HGET', KEYS[1], 'catalog_generation') ~= ARGV[6]
   or redis.call('GET', KEYS[2]) ~= ARGV[10]
   or redis.call('TTL', KEYS[2]) <= 0
   or redis.call('EXISTS', KEYS[5]) ~= 1
   or redis.call('GET', KEYS[5]) ~= ARGV[12]
   or redis.call('HGET', KEYS[6], 'state') == 'released'
   or redis.call('HGET', KEYS[6], 'state') == 'failed' then return 0 end
if state == active then return 1 end
redis.call('SET', KEYS[7], active)
return 1
"""

_END_SHARING_OPERATION_OPEN_LUA = """
local function kind(key) return redis.call('TYPE', key).ok end
if kind(KEYS[1]) ~= 'hash' or (kind(KEYS[2]) ~= 'string' and kind(KEYS[2]) ~= 'none') then return 0 end
local open_value = ARGV[1] .. ':' .. ARGV[2] .. ':' .. ARGV[3]
local pending_value = 'pending:' .. open_value
local active_value = 'active:' .. open_value
local ended_value = 'ended:' .. open_value
if redis.call('HGET', KEYS[1], 'operation_id') ~= ARGV[1]
   or redis.call('HGET', KEYS[1], 'operation_revision') ~= ARGV[2] then return 0 end
if kind(KEYS[2]) == 'string' and redis.call('GET', KEYS[2]) == ended_value then return 1 end
if kind(KEYS[2]) ~= 'string' then return 0 end
local existing = redis.call('GET', KEYS[2])
if existing ~= pending_value and existing ~= active_value then return 0 end
local count = tonumber(redis.call('HGET', KEYS[1], 'open_count') or '')
if not count or count < 1 or count % 1 ~= 0 then return 0 end
redis.call('SET', KEYS[2], ended_value, 'EX', ARGV[4])
redis.call('HINCRBY', KEYS[1], 'open_count', -1)
return 1
"""

_BEGIN_SHARING_OPERATION_CLOSE_LUA = """
local function kind(key) return redis.call('TYPE', key).ok end
if (kind(KEYS[1]) ~= 'string' and kind(KEYS[1]) ~= 'none') or kind(KEYS[2]) ~= 'hash'
   or (kind(KEYS[3]) ~= 'string' and kind(KEYS[3]) ~= 'none') then return 0 end
if redis.call('HGET', KEYS[2], 'operation_id') ~= ARGV[1]
   or redis.call('HGET', KEYS[2], 'operation_revision') ~= ARGV[2]
   or redis.call('HGET', KEYS[2], 'worker_id') ~= ARGV[3] then return 0 end
local state = redis.call('HGET', KEYS[2], 'status')
if state == 'closed' then
  return kind(KEYS[1]) == 'none' and kind(KEYS[3]) == 'none'
     and redis.call('HGET', KEYS[2], 'capability_digest') == ARGV[4] and 1 or 0
end
if state ~= 'active' and state ~= 'closing'
   or kind(KEYS[1]) ~= 'string'
   or redis.call('GET', KEYS[1]) ~= ARGV[1] .. ':' .. ARGV[2]
   or redis.call('HGET', KEYS[2], 'capability_digest') ~= ARGV[4] then return 0 end
redis.call('HSET', KEYS[2], 'status', 'closing')
redis.call('DEL', KEYS[3])
return 1
"""

_CLOSE_SHARING_OPERATION_LUA = """
local function kind(key) return redis.call('TYPE', key).ok end
if (kind(KEYS[1]) ~= 'string' and kind(KEYS[1]) ~= 'none') or kind(KEYS[2]) ~= 'hash'
   or kind(KEYS[3]) ~= 'hash' or (kind(KEYS[4]) ~= 'string' and kind(KEYS[4]) ~= 'none') then return 0 end
if redis.call('HGET', KEYS[2], 'operation_id') ~= ARGV[1]
   or redis.call('HGET', KEYS[2], 'operation_revision') ~= ARGV[2]
   or redis.call('HGET', KEYS[2], 'worker_id') ~= ARGV[3]
   or redis.call('HGET', KEYS[3], 'sharing_operation_id') ~= ARGV[1]
   or redis.call('HGET', KEYS[3], 'sharing_operation_revision') ~= ARGV[2]
   or redis.call('HGET', KEYS[3], 'worker_id') ~= ARGV[3] then return 0 end
local count = tonumber(redis.call('HGET', KEYS[2], 'open_count') or '')
local state = redis.call('HGET', KEYS[2], 'status')
local worker_state = redis.call('HGET', KEYS[3], 'state')
if not count or count ~= 0 or (worker_state ~= 'released' and worker_state ~= 'failed')
   or kind(KEYS[4]) ~= 'none' then return 0 end
if state == 'closed' then
  if kind(KEYS[1]) ~= 'none'
     or redis.call('HGET', KEYS[2], 'capability_digest') ~= ARGV[4] then return 0 end
  return 1
end
if state ~= 'closing' or kind(KEYS[1]) ~= 'string'
   or redis.call('GET', KEYS[1]) ~= ARGV[1] .. ':' .. ARGV[2]
   or redis.call('HGET', KEYS[2], 'capability_digest') ~= ARGV[4] then return 0 end
redis.call('HSET', KEYS[2], 'status', 'closed', 'closed_at', ARGV[5])
redis.call('EXPIRE', KEYS[2], ARGV[6])
redis.call('DEL', KEYS[1])
return 1
"""

_RECONCILE_UNLAUNCHED_SHARING_OPERATION_LUA = """
local function kind(key) return redis.call('TYPE', key).ok end
if (kind(KEYS[1]) ~= 'string' and kind(KEYS[1]) ~= 'none')
   or kind(KEYS[2]) ~= 'hash'
   or (kind(KEYS[3]) ~= 'string' and kind(KEYS[3]) ~= 'none')
   or kind(KEYS[4]) ~= 'hash' or kind(KEYS[5]) ~= 'string' then return 0 end
local old_fence = tonumber(redis.call('HGET', KEYS[2], 'recorder_fence') or '')
local current_fence = tonumber(ARGV[6])
local count = tonumber(redis.call('HGET', KEYS[2], 'open_count') or '')
if redis.call('GET', KEYS[5]) ~= ARGV[5]
   or redis.call('HGET', KEYS[2], 'operation_id') ~= ARGV[1]
   or redis.call('HGET', KEYS[2], 'operation_revision') ~= ARGV[2]
   or redis.call('HGET', KEYS[2], 'worker_id') ~= ARGV[3]
   or redis.call('HGET', KEYS[2], 'capability_digest') ~= ARGV[4]
   or redis.call('HGET', KEYS[4], 'sharing_operation_id') ~= ARGV[1]
   or redis.call('HGET', KEYS[4], 'sharing_operation_revision') ~= ARGV[2]
   or redis.call('HGET', KEYS[4], 'worker_id') ~= ARGV[3]
   or redis.call('HGET', KEYS[4], 'capability_digest') ~= ARGV[4]
   or redis.call('HGET', KEYS[4], 'state') ~= 'issued'
   or not old_fence or not current_fence or current_fence < old_fence
   or (current_fence == old_fence and ARGV[7] ~= 'unreturned')
   or not count or count ~= 0 then return 0 end
local state = redis.call('HGET', KEYS[2], 'status')
if state ~= 'active' and state ~= 'closing' then
  if state == 'closed' and kind(KEYS[1]) == 'none' and kind(KEYS[3]) == 'none' then return 1 end
  return 0
end
if redis.call('GET', KEYS[1]) ~= ARGV[1] .. ':' .. ARGV[2]
   or redis.call('EXISTS', KEYS[3]) ~= 1 then return 0 end
redis.call('HSET', KEYS[2], 'status', 'closed', 'closed_at', ARGV[8])
redis.call('EXPIRE', KEYS[2], ARGV[9])
redis.call('HSET', KEYS[4], 'state', 'failed')
redis.call('DEL', KEYS[1], KEYS[3])
return 1
"""


class RecorderAttemptAdmissionUncertain(RuntimeError):
    """An issue write may have committed; exact attempt identity is retained."""

    def __init__(self, attempt: "RecorderProxyAttempt"):
        super().__init__("sharing recorder admission outcome is uncertain")
        self.attempt = attempt


@dataclass(frozen=True)
class RecorderProxyAttempt:
    """One ranked source attempt; bearer material is excluded from repr."""

    worker_id: str
    stream_id: str
    account_id: str
    config_generation: str
    control_generation: int
    capability_key: str
    input_url: str
    capability: str = field(repr=False)
    channel_uuid: str | None = None
    sharing_operation_id: str | None = None
    sharing_operation_revision: int | None = None
    sharing_authority_revision: int | None = None
    sharing_catalog_generation: str | None = None

    @property
    def input_headers(self) -> dict[str, str]:
        return {CAPABILITY_HEADER: self.capability}

    def renew(self, redis_client, ttl_seconds: int = CAPABILITY_TTL_SECONDS) -> bool:
        return bool(redis_client.expire(self.capability_key, ttl_seconds))

    def revoke(self, redis_client) -> bool:
        if self.sharing_operation_id is not None:
            if not close_sharing_operation_admission(redis_client, self):
                return False
        else:
            redis_client.delete(self.capability_key)
        return True

    def operation_needs_switch(self, redis_client) -> bool:
        if self.sharing_operation_id is None:
            return False
        return not sharing_operation_is_current(redis_client, self)

    def acknowledge_reaped(self, redis_client) -> bool:
        if self.sharing_operation_id is None:
            return True
        return close_sharing_operation_after_reap(redis_client, self)


def _sharing_operation_key(channel_uuid: str, operation_id: str) -> str:
    return f"{SHARING_OPERATION_PREFIX}{channel_uuid}:operation:{operation_id}"


def _sharing_operation_current_key(channel_uuid: str) -> str:
    return f"{SHARING_OPERATION_PREFIX}{channel_uuid}:current"


def _sharing_operation_sequence_key(channel_uuid: str) -> str:
    return f"{SHARING_OPERATION_PREFIX}{channel_uuid}:sequence"


def _sharing_operation_open_key(channel_uuid: str, operation_id: str, nonce: str) -> str:
    return f"{SHARING_OPERATION_PREFIX}{channel_uuid}:open:{operation_id}:{nonce}"


def _sharing_operation_open_pattern(channel_uuid: str, operation_id: str) -> str:
    return f"{SHARING_OPERATION_PREFIX}{channel_uuid}:open:{operation_id}:*"


def close_sharing_operation_admission(redis_client, attempt: RecorderProxyAttempt) -> bool:
    """Atomically tombstone one attempt before its native process is stopped."""
    if (not isinstance(attempt, RecorderProxyAttempt)
            or not isinstance(attempt.sharing_operation_id, str)
            or type(attempt.sharing_operation_revision) is not int
            or attempt.capability_key != CAPABILITY_PREFIX + hashlib.sha256(
                attempt.capability.encode("ascii")
            ).hexdigest()):
        return False
    try:
        result = redis_client.eval(
            _BEGIN_SHARING_OPERATION_CLOSE_LUA, 3,
            _sharing_operation_current_key(_channel_from_attempt(attempt)),
            _sharing_operation_key(_channel_from_attempt(attempt), attempt.sharing_operation_id),
            attempt.capability_key,
            attempt.sharing_operation_id, str(attempt.sharing_operation_revision), attempt.worker_id,
            hashlib.sha256(attempt.capability.encode("ascii")).hexdigest(),
        )
    except Exception:
        return False
    return type(result) is int and result == 1


def _channel_from_attempt(attempt: RecorderProxyAttempt) -> str:
    # The channel is part of the private worker binding, but keep it explicitly
    # on the attempt instead of parsing the opaque worker ID.
    value = getattr(attempt, "channel_uuid", None)
    if not isinstance(value, str) or not value:
        raise ValueError("recorder attempt has no channel binding")
    return value


def close_sharing_operation_after_reap(redis_client, attempt: RecorderProxyAttempt) -> bool:
    """Release the channel admission fence only after process and route ACKs."""
    try:
        channel_uuid = _channel_from_attempt(attempt)
        token_digest = hashlib.sha256(attempt.capability.encode("ascii")).hexdigest()
    except (AttributeError, UnicodeError, ValueError):
        return False
    if (
        not isinstance(attempt.sharing_operation_id, str)
        or type(attempt.sharing_operation_revision) is not int
        or attempt.capability_key != CAPABILITY_PREFIX + token_digest
    ):
        return False
    if not reconcile_operation_open_slots_after_reap(redis_client, attempt):
        return False
    return close_sharing_operation_after_reap_exact(
        redis_client, channel_uuid=channel_uuid,
        operation_id=attempt.sharing_operation_id,
        operation_revision=attempt.sharing_operation_revision,
        worker_id=attempt.worker_id, capability_digest=token_digest,
    )


def reconcile_operation_open_slots_after_reap(
    redis_client, attempt: RecorderProxyAttempt,
) -> bool:
    """Close persisted HTTP reservations after the exact recorder was reaped."""
    try:
        channel_uuid = _channel_from_attempt(attempt)
        operation_id = attempt.sharing_operation_id
        revision = attempt.sharing_operation_revision
    except Exception:
        return False
    return _reconcile_operation_open_slots(
        redis_client, channel_uuid, operation_id, revision,
    )


def _reconcile_operation_open_slots(
    redis_client, channel_uuid: str, operation_id: str, revision: int,
) -> bool:
    try:
        operation = redis_client.hgetall(
            _sharing_operation_key(channel_uuid, operation_id),
        ) or {}
        operation = {_decode(key): _decode(value) for key, value in operation.items()}
        if operation.get("open_count") == "0":
            return True
        scan_iter = getattr(redis_client, "scan_iter", None)
        if not callable(scan_iter):
            return False
        keys = scan_iter(
            match=_sharing_operation_open_pattern(channel_uuid, operation_id),
        )
    except Exception:
        return False
    binding = {
        "channel_uuid": channel_uuid,
        "sharing_operation_id": operation_id,
        "sharing_operation_revision": revision,
    }
    try:
        for raw_key in keys:
            key = _decode(raw_key)
            value = _decode(redis_client.get(key))
            if value is None:
                continue
            prefix = f"{operation_id}:{revision}:"
            if value.startswith("ended:" + prefix):
                continue
            if not value.startswith(prefix):
                return False
            nonce = value[len(prefix):]
            if not nonce or not end_sharing_operation_open(
                redis_client, binding, key, nonce,
            ):
                return False
    except Exception:
        return False
    return True


def reconcile_prior_sharing_operation(
    redis_client, *, channel_uuid: str, current_lease_value: str,
    current_lease_fence: int,
) -> bool:
    """Startup recovery for exact prior attempts after a newer lease is held."""
    if (not isinstance(channel_uuid, str) or not channel_uuid
            or not isinstance(current_lease_value, str) or not current_lease_value
            or type(current_lease_fence) is not int or current_lease_fence < 1):
        return False
    try:
        pointer = _decode(redis_client.get(_sharing_operation_current_key(channel_uuid)))
        if pointer is None:
            return True
        operation_id, separator, revision_text = str(pointer).rpartition(":")
        if not separator or not operation_id or not revision_text.isdecimal():
            return False
        revision = int(revision_text)
        operation = redis_client.hgetall(
            _sharing_operation_key(channel_uuid, operation_id),
        ) or {}
        operation = {_decode(key): _decode(value) for key, value in operation.items()}
        if (operation.get("operation_id") != operation_id
                or operation.get("operation_revision") != str(revision)):
            return False
        old_fence = int(operation.get("recorder_fence", "0"))
        if old_fence < 1 or old_fence >= current_lease_fence:
            return False
        worker_id = operation.get("worker_id")
        digest = operation.get("capability_digest")
        worker = redis_client.hgetall(_worker_record_key(str(worker_id))) or {}
        worker = {_decode(key): _decode(value) for key, value in worker.items()}
        if (worker.get("sharing_operation_id") != operation_id
                or worker.get("sharing_operation_revision") != str(revision)
                or worker.get("worker_id") != worker_id
                or worker.get("capability_digest") != digest):
            return False
        if worker.get("state") == "issued" and operation.get("open_count") == "0":
            return reconcile_unlaunched_sharing_operation(
                redis_client, channel_uuid=channel_uuid, operation_id=operation_id,
                operation_revision=revision, worker_id=str(worker_id),
                capability_digest=str(digest), current_lease_value=current_lease_value,
                current_lease_fence=current_lease_fence,
            )
        if (worker.get("state") not in {"released", "failed"}
                or operation.get("status") not in {"active", "closing"}):
            return False
        if not _reconcile_operation_open_slots(
            redis_client, channel_uuid, operation_id, revision,
        ):
            return False
        return close_sharing_operation_after_reap_exact(
            redis_client, channel_uuid=channel_uuid, operation_id=operation_id,
            operation_revision=revision, worker_id=str(worker_id),
            capability_digest=str(digest),
        )
    except Exception:
        return False


def close_sharing_operation_after_reap_exact(
    redis_client, *, channel_uuid: str, operation_id: str,
    operation_revision: int, worker_id: str, capability_digest: str,
) -> bool:
    """Idempotently release exact operation admission after native reap ACK."""
    try:
        result = redis_client.eval(
            _CLOSE_SHARING_OPERATION_LUA, 4,
            _sharing_operation_current_key(channel_uuid),
            _sharing_operation_key(channel_uuid, operation_id),
            _worker_record_key(worker_id),
            CAPABILITY_PREFIX + capability_digest,
            operation_id, str(operation_revision),
            worker_id, capability_digest,
            str(time.time()), str(24 * 60 * 60),
        )
    except Exception:
        return False
    return type(result) is int and result == 1


def reconcile_unlaunched_sharing_operation(
    redis_client, *, channel_uuid: str, operation_id: str,
    operation_revision: int, worker_id: str, capability_digest: str,
    current_lease_value: str, current_lease_fence: int,
    unreturned_capability: bool = False,
) -> bool:
    """Close only an exact issued attempt that has no open HTTP reservation.

    A same-fence reconciliation is allowed only in the issue call that never
    returned the bearer capability to its caller. Restart recovery requires a
    strictly newer lease fence, proving the old task cannot open the route.
    """
    if (
        not isinstance(channel_uuid, str) or not channel_uuid
        or not isinstance(operation_id, str) or not operation_id
        or type(operation_revision) is not int or operation_revision < 1
        or not isinstance(worker_id, str) or not worker_id
        or not isinstance(capability_digest, str) or len(capability_digest) != 64
        or any(c not in "0123456789abcdef" for c in capability_digest)
        or not isinstance(current_lease_value, str) or not current_lease_value
        or type(current_lease_fence) is not int or current_lease_fence < 1
        or type(unreturned_capability) is not bool
    ):
        return False
    try:
        result = redis_client.eval(
            _RECONCILE_UNLAUNCHED_SHARING_OPERATION_LUA, 5,
            _sharing_operation_current_key(channel_uuid),
            _sharing_operation_key(channel_uuid, operation_id),
            CAPABILITY_PREFIX + capability_digest,
            _worker_record_key(worker_id),
            f"catchuparr:recorder:{channel_uuid}",
            operation_id, str(operation_revision), worker_id, capability_digest,
            current_lease_value, str(current_lease_fence),
            "unreturned" if unreturned_capability else "reaped",
            str(time.time()), str(24 * 60 * 60),
        )
    except Exception:
        return False
    return type(result) is int and result == 1


def _worker_record_key(worker_id: str) -> str:
    from .adapters.recorder_proxy import worker_id_key

    return worker_id_key(worker_id)


def sharing_operation_is_current(redis_client, attempt: RecorderProxyAttempt) -> bool:
    """Conservatively detect revocation so the task can stop this FFmpeg attempt."""
    try:
        channel_uuid = _channel_from_attempt(attempt)
        operation = redis_client.hgetall(
            _sharing_operation_key(channel_uuid, attempt.sharing_operation_id)
        ) or {}
        operation = {_decode(key): _decode(value) for key, value in operation.items()}
        authority = redis_client.hgetall(f"catchuparr:sharing:current:{channel_uuid}") or {}
        authority = {_decode(key): _decode(value) for key, value in authority.items()}
        current = _decode(redis_client.get(_sharing_operation_current_key(channel_uuid)))
        lease = _decode(redis_client.get(f"catchuparr:recorder:{channel_uuid}"))
        return (
            operation.get("status") == "active"
            and operation.get("operation_id") == attempt.sharing_operation_id
            and operation.get("operation_revision") == str(attempt.sharing_operation_revision)
            and operation.get("worker_id") == attempt.worker_id
            and current == f"{attempt.sharing_operation_id}:{attempt.sharing_operation_revision}"
            and operation.get("lease_value") == lease
            and operation.get("authority_revision") == str(attempt.sharing_authority_revision)
            and operation.get("catalog_generation") == attempt.sharing_catalog_generation
            and authority.get("authority_revision") == str(attempt.sharing_authority_revision)
            and authority.get("configuration_generation") == attempt.config_generation
            and authority.get("control_generation") == str(attempt.control_generation)
            and authority.get("catalog_generation") == attempt.sharing_catalog_generation
            and redis_client.exists(attempt.capability_key)
        )
    except Exception:
        return False


def _normalized_sharing_operation_binding(binding: Mapping[str, Any]) -> dict[str, Any] | None:
    # Recorder capabilities use the namespaced sharing_* fields in the signed
    # payload. Normalize those exact fields for the Lua argument interface,
    # while preserving the original map when comparing the stored capability.
    aliases = {
        "operation_id": "sharing_operation_id",
        "operation_revision": "sharing_operation_revision",
        "authority_revision": "sharing_authority_revision",
        "catalog_generation": "sharing_catalog_generation",
    }
    normalized = dict(binding) if isinstance(binding, Mapping) else {}
    for name, signed_name in aliases.items():
        signed_value = normalized.get(signed_name)
        direct_value = normalized.get(name)
        if signed_value is not None and direct_value is not None and str(signed_value) != str(direct_value):
            return None
        value = direct_value if direct_value is not None else signed_value
        if value is not None:
            normalized[name] = str(value)
    required = (
        "channel_uuid", "worker_id", "operation_id", "operation_revision",
        "authority_revision", "config_generation", "control_generation",
        "catalog_generation", "lease_fence", "lease_key", "lease_value",
        "capability_digest",
    )
    if any(not isinstance(normalized.get(name), str) or not normalized[name] for name in required):
        return None
    return normalized


def _sharing_operation_open_keys(normalized: Mapping[str, Any], open_key: str) -> tuple[str, ...]:
    channel_uuid = normalized["channel_uuid"]
    operation_id = normalized["operation_id"]
    return (
        f"catchuparr:sharing:current:{channel_uuid}",
        normalized["lease_key"],
        _sharing_operation_current_key(channel_uuid),
        _sharing_operation_key(channel_uuid, operation_id),
        CAPABILITY_PREFIX + normalized["capability_digest"],
        _worker_record_key(normalized["worker_id"]),
        open_key,
    )


def _sharing_operation_open_args(normalized: Mapping[str, Any], binding: Mapping[str, Any],
                                 nonce: str) -> tuple[str, ...]:
    return (
        normalized["operation_id"], normalized["operation_revision"],
        normalized["authority_revision"], normalized["config_generation"],
        normalized["control_generation"], normalized["catalog_generation"],
        normalized["lease_fence"], normalized["worker_id"],
        normalized["capability_digest"], normalized["lease_value"], nonce,
        json.dumps({key: str(value) for key, value in binding.items()},
                   sort_keys=True, separators=(",", ":")),
    )


def begin_sharing_operation_open(redis_client, binding: Mapping[str, Any]) -> tuple[str, str] | None:
    """Reserve one response slot before native worker initialization/reservation."""
    normalized = _normalized_sharing_operation_binding(binding)
    if normalized is None:
        return None
    channel_uuid = normalized["channel_uuid"]
    operation_id = normalized["operation_id"]
    nonce = uuid.uuid4().hex
    open_key = _sharing_operation_open_key(channel_uuid, operation_id, nonce)
    keys = _sharing_operation_open_keys(normalized, open_key)
    args = _sharing_operation_open_args(normalized, binding, nonce)
    try:
        result = redis_client.eval(_BEGIN_SHARING_OPERATION_OPEN_LUA, len(keys), *keys, *args)
    except Exception:
        # The nonce and exact Redis key are known before EVAL. If Redis applied
        # BEGIN but its reply was lost, release that exact slot now. END is
        # idempotent, so a lost cleanup reply remains safely retryable.
        end_sharing_operation_open(redis_client, binding, open_key, nonce)
        return None
    if type(result) is not int or result != 1:
        # A malformed/lost response must not strand a slot created by a
        # successful script. A denied script has no matching key, so this is
        # harmless in that case.
        end_sharing_operation_open(redis_client, binding, open_key, nonce)
        return None
    return open_key, nonce


def activate_sharing_operation_open(redis_client, binding: Mapping[str, Any],
                                    open_key: str, nonce: str) -> bool:
    """Mark a reserved request active only after source response setup succeeds."""
    normalized = _normalized_sharing_operation_binding(binding)
    if (normalized is None or not isinstance(open_key, str) or not isinstance(nonce, str)
            or not nonce or open_key != _sharing_operation_open_key(
                normalized["channel_uuid"], normalized["operation_id"], nonce,
            )):
        return False
    try:
        result = redis_client.eval(
            _ACTIVATE_SHARING_OPERATION_OPEN_LUA, 7,
            *_sharing_operation_open_keys(normalized, open_key),
            *_sharing_operation_open_args(normalized, binding, nonce),
        )
    except Exception:
        return False
    return type(result) is int and result == 1


def end_sharing_operation_open(redis_client, attempt: RecorderProxyAttempt,
                               open_key: str, nonce: str) -> bool:
    if (not isinstance(open_key, str) or not isinstance(nonce, str)):
        return False
    if isinstance(attempt, RecorderProxyAttempt):
        channel_uuid = _channel_from_attempt(attempt)
        operation_id = attempt.sharing_operation_id
        operation_revision = attempt.sharing_operation_revision
    elif isinstance(attempt, Mapping):
        channel_uuid = attempt.get("channel_uuid")
        operation_id = attempt.get("sharing_operation_id")
        raw_revision = attempt.get("sharing_operation_revision")
        try:
            operation_revision = int(raw_revision)
        except (TypeError, ValueError):
            return False
    else:
        return False
    if (not isinstance(channel_uuid, str) or not channel_uuid
            or not isinstance(operation_id, str) or not operation_id
            or type(operation_revision) is not int):
        return False
    try:
        result = redis_client.eval(
            _END_SHARING_OPERATION_OPEN_LUA, 2,
            _sharing_operation_key(channel_uuid, operation_id),
            open_key, operation_id, str(operation_revision), nonce, str(30 * 24 * 60 * 60),
        )
    except Exception:
        return False
    return type(result) is int and result == 1


def _attach_response_closer(response: Any, closer) -> bool:
    closers = getattr(response, "_resource_closers", None)
    if not isinstance(closers, list):
        return False
    closers.append(closer)
    return True


def _operation_slot_closer(redis_client, binding: Mapping[str, Any], open_key: str, nonce: str):
    """Return a retryable exact Redis slot closer for a streaming response."""
    lock = threading.Lock()
    acknowledged = False

    def close() -> bool:
        nonlocal acknowledged
        with lock:
            if acknowledged:
                return True
            acknowledged = end_sharing_operation_open(
                redis_client, binding, open_key, nonce,
            )
            return acknowledged

    return close


def configuration_generation(active_configuration: Mapping[str, Any]) -> str:
    from .configuration import active_configuration_generation

    return active_configuration_generation(dict(active_configuration))


def current_sharing_authority_context(redis_client, channel_uuid: str, *,
                                      config_generation: str,
                                      control_generation: int) -> dict[str, Any] | None:
    """Materialize current DB catalog and Redis revision before a candidate attempt."""
    try:
        from .configuration import _sharing_catalog_generation

        catalog_generation = _sharing_catalog_generation(channel_uuid)
        raw = redis_client.hgetall(f"catchuparr:sharing:current:{channel_uuid}") or {}
        current = {_decode(key): _decode(value) for key, value in raw.items()}
        revision = current.get("authority_revision")
        if (current.get("configuration_generation") != config_generation
                or current.get("control_generation") != str(control_generation)
                or current.get("catalog_generation") != catalog_generation
                or not isinstance(revision, str) or not revision.isdecimal()
                or int(revision) < 1):
            return None
        return {
            "authority_revision": int(revision),
            "catalog_generation": catalog_generation,
        }
    except Exception:
        return None


def _decode(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


def _active_configuration():
    from .configuration import load_applied_state

    return load_applied_state()[0]


def _close_database_connections() -> None:
    """Release a safe, existing default ORM connection before a long stream."""
    try:
        from django.db import connections
    except ImportError:
        return
    try:
        initialized = connections.all(initialized_only=True)
    except Exception:
        return
    default = next(
        (connection for connection in initialized if connection.alias == "default"),
        None,
    )
    if (
        default is None
        or default.connection is None
        or default.in_atomic_block
    ):
        return
    try:
        autocommit = default.get_autocommit()
    except Exception:
        return
    if autocommit:
        default.close()


def _policy_for_channel(active: Mapping[str, Any], channel_uuid: str) -> SourcePolicy | None:
    encoded = active.get("source_policies", {}).get(str(channel_uuid))
    if encoded is None:
        return None
    if not isinstance(encoded, dict):
        raise ValueError("applied source policy is invalid")
    include_ids = encoded.get("include_account_ids")
    if include_ids is not None and (
        not isinstance(include_ids, list)
        or any(not isinstance(value, (str, int)) or not str(value) for value in include_ids)
    ):
        raise ValueError("applied source include IDs are invalid")
    exclude_ids = encoded.get("exclude_account_ids")
    if not isinstance(exclude_ids, list) or any(
        not isinstance(value, (str, int)) or not str(value) for value in exclude_ids
    ):
        raise ValueError("applied source exclude IDs are invalid")
    if include_ids is not None and exclude_ids:
        raise ValueError("applied source policy has both include and exclude")
    priorities = encoded.get("priorities", [])
    if not isinstance(priorities, list):
        raise ValueError("applied source priorities are invalid")
    decoded_priorities = []
    for pair in priorities:
        if (
            not isinstance(pair, (tuple, list))
            or len(pair) != 2
            or not isinstance(pair[0], (str, int))
            or not str(pair[0])
            or isinstance(pair[1], bool)
            or not isinstance(pair[1], int)
        ):
            raise ValueError("applied source priority value is invalid")
        decoded_priorities.append((str(pair[0]), pair[1]))
    if len({account_id for account_id, _ in decoded_priorities}) != len(decoded_priorities):
        raise ValueError("applied source priorities contain duplicates")
    known = encoded.get("known_account_ids")
    if isinstance(known, list) and all(
        isinstance(value, (str, int)) and str(value) for value in known
    ):
        known_ids = frozenset(str(value) for value in known)
        if len(known_ids) != len(known):
            raise ValueError("applied known account IDs contain duplicates")
    else:
        raise ValueError("applied known account IDs are invalid")
    include_ids_set = frozenset(str(value) for value in (include_ids or ()))
    exclude_ids_set = frozenset(str(value) for value in exclude_ids)
    if (
        not include_ids_set <= known_ids
        or not exclude_ids_set <= known_ids
        or not {account_id for account_id, _ in decoded_priorities} <= known_ids
    ):
        raise ValueError("applied policy references an unknown account")
    return SourcePolicy(
        include_account_ids=include_ids_set if include_ids is not None else None,
        exclude_account_ids=exclude_ids_set,
        priorities=tuple(decoded_priorities),
        known_account_ids=known_ids,
    )


def ranked_source_candidates(
    channel_uuid: str,
    active_configuration: Mapping[str, Any] | None = None,
    *,
    all_sources: bool = False,
) -> list[dict[str, Any]] | None:
    """Return ranked assigned sources, or None when the default live route applies."""
    active = active_configuration if active_configuration is not None else _active_configuration()
    if active is None:
        return None
    policy = _policy_for_channel(active, str(channel_uuid))
    if policy is None and not all_sources:
        return None
    from .configuration import source_catalog
    from .source_rules import SourcePolicy

    try:
        catalog = source_catalog()
    finally:
        # The returned catalog is fully materialized. Do not retain a pooled
        # Django connection in a task supervisor or a request thread that may
        # now spend minutes consuming a transport stream.
        _close_database_connections()
    current_accounts = {
        str(account["id"]) for account in catalog.accounts if account.get("id") is not None
    }
    if policy is None:
        policy = SourcePolicy(
            include_account_ids=None,
            exclude_account_ids=frozenset(),
            priorities=(),
            known_account_ids=frozenset(current_accounts),
        )
    if not policy.known_account_ids <= current_accounts:
        current_ids = policy.known_account_ids & current_accounts
        policy = SourcePolicy(
            include_account_ids=(
                policy.include_account_ids & current_ids
                if policy.include_account_ids is not None else None
            ),
            exclude_account_ids=policy.exclude_account_ids & current_ids,
            priorities=tuple(
                (account_id, score)
                for account_id, score in policy.priorities
                if account_id in current_ids
            ),
            known_account_ids=current_ids,
        )
    streams = catalog.streams_by_channel.get(str(channel_uuid), ())
    return [dict(candidate) for candidate in rank_candidates(policy, streams)]


def candidate_is_current(
    channel_uuid: str,
    stream_id: str,
    account_id: str,
    active_configuration: Mapping[str, Any] | None = None,
) -> bool:
    """Recheck assignment membership and the applied policy at each HTTP open."""
    try:
        active = active_configuration if active_configuration is not None else _active_configuration()
        if active is None:
            return False
        from .runtime import parse_settings

        if str(channel_uuid) not in set(parse_settings(dict(active)).channel_uuids):
            return False
        sharing = active.get("connection_sharing")
        sharing_setting = sharing.get(str(channel_uuid)) if isinstance(sharing, dict) else None
        candidates = ranked_source_candidates(
            str(channel_uuid), active,
            all_sources=isinstance(sharing_setting, dict) and sharing_setting.get("enabled") is True,
        )
        if candidates is None:
            return False
        return any(
            str(candidate.get("id")) == str(stream_id)
            and str(candidate.get("account_id")) == str(account_id)
            for candidate in candidates
        )
    except Exception:
        logger.exception("Could not validate applied recorder source for channel %s", channel_uuid)
        return False


def _secret_key() -> bytes:
    from django.conf import settings

    value = getattr(settings, "SECRET_KEY", "")
    if not isinstance(value, str) or not value:
        raise RuntimeError("Dispatcharr signing key is unavailable")
    return value.encode("utf-8")


def _canonical_binding(binding: Mapping[str, Any], nonce: str) -> bytes:
    value = {key: binding[key] for key in sorted(binding)}
    value["nonce"] = nonce
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _signature(binding: Mapping[str, Any], nonce: str) -> str:
    signature = hmac.new(_secret_key(), _canonical_binding(binding, nonce), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")


def issue_recorder_attempt(
    redis_client,
    lease,
    *,
    channel_uuid: str,
    candidate: Mapping[str, Any],
    config_generation: str,
    control_generation: int,
    internal_base_url: str,
    sharing_authority: Mapping[str, Any] | None = None,
) -> RecorderProxyAttempt:
    """Issue an HMAC-signed one-recorder capability and store only its digest."""
    from .adapters.recorder_proxy import make_worker_id, write_worker_record

    fence = getattr(lease, "fence", None)
    lease_value = getattr(lease, "_value", None)
    lease_key = getattr(lease, "key", None)
    if not fence or not lease_value or not lease_key:
        raise ValueError("recorder lease is not active")
    if str(lease_key) != f"catchuparr:recorder:{channel_uuid}":
        raise ValueError("recorder lease does not match the requested channel")
    stream_id = str(candidate.get("id") or candidate.get("stream_id") or "")
    account_id = str(candidate.get("account_id") or "")
    if not stream_id or not account_id:
        raise ValueError("assigned recorder source is invalid")
    if type(control_generation) is not int or control_generation < 0:
        raise ValueError("recorder control generation is invalid")
    attempt_id = uuid.uuid4().hex
    worker_id = make_worker_id(
        str(channel_uuid), stream_id, config_generation, int(fence), attempt_id
    )
    binding = {
        "account_id": account_id,
        "channel_uuid": str(channel_uuid),
        "config_generation": str(config_generation),
        "control_generation": str(control_generation),
        "lease_fence": str(int(fence)),
        "lease_key": str(lease_key),
        "lease_value": str(lease_value),
        "stream_id": stream_id,
        "worker_id": worker_id,
    }
    operation_revision = None
    authority_revision = None
    catalog_generation = None
    operation_current_key = None
    operation_sequence_key = None
    operation_key = None
    if sharing_authority is not None:
        if not isinstance(sharing_authority, Mapping):
            raise ValueError("sharing authority must be a mapping")
        catalog_generation = sharing_authority.get("catalog_generation")
        authority_revision = sharing_authority.get("authority_revision")
        if (not isinstance(catalog_generation, str) or not catalog_generation
                or type(authority_revision) is not int or authority_revision < 1):
            raise ValueError("sharing authority revision or catalog binding is invalid")
        operation_current_key = _sharing_operation_current_key(str(channel_uuid))
        operation_sequence_key = _sharing_operation_sequence_key(str(channel_uuid))
        operation_key = _sharing_operation_key(str(channel_uuid), attempt_id)
        if not reconcile_prior_sharing_operation(
            redis_client, channel_uuid=str(channel_uuid),
            current_lease_value=str(lease_value), current_lease_fence=int(fence),
        ):
            raise RuntimeError("previous sharing operation still requires exact cleanup")
        raw_sequence = redis_client.get(operation_sequence_key)
        if raw_sequence is None:
            sequence = 0
        else:
            try:
                sequence = int(_decode(raw_sequence))
            except (TypeError, ValueError, UnicodeError):
                raise RuntimeError("sharing operation sequence is malformed") from None
        if sequence < 0 or sequence >= 9007199254740990:
            raise RuntimeError("sharing operation sequence is outside its supported range")
        operation_revision = sequence + 1
        binding.update({
            "sharing_operation_id": attempt_id,
            "sharing_operation_revision": str(operation_revision),
            "sharing_authority_revision": str(authority_revision),
            "sharing_catalog_generation": catalog_generation,
        })
    nonce = secrets.token_urlsafe(32)
    token = f"{nonce}.{_signature(binding, nonce)}"
    digest = hashlib.sha256(token.encode("ascii")).hexdigest()
    key = CAPABILITY_PREFIX + digest
    record = {**binding, "capability_digest": digest}
    reservation_id = uuid.uuid4().hex
    created_at = str(time.time())
    attempt = RecorderProxyAttempt(
        worker_id=worker_id,
        stream_id=stream_id,
        account_id=account_id,
        config_generation=str(config_generation),
        control_generation=control_generation,
        capability_key=key,
        input_url=f"{internal_base_url.rstrip('/')}/catchuparr/recorder/{channel_uuid}",
        capability=token,
        channel_uuid=str(channel_uuid),
        sharing_operation_id=attempt_id if sharing_authority is not None else None,
        sharing_operation_revision=operation_revision,
        sharing_authority_revision=authority_revision,
        sharing_catalog_generation=catalog_generation,
    )
    if sharing_authority is None:
        if not redis_client.set(
            key,
            json.dumps(record, sort_keys=True, separators=(",", ":")),
            nx=True,
            ex=CAPABILITY_TTL_SECONDS,
        ):
            raise RuntimeError("could not issue recorder capability")
        try:
            write_worker_record(
                redis_client,
                worker_id,
                {
                    **binding,
                    "worker_id": worker_id,
                    "capability_digest": digest,
                    "reservation_id": reservation_id,
                    "reservation_state": "none",
                    "state": "issued",
                    "created_at": created_at,
                },
            )
        except Exception:
            redis_client.delete(key)
            raise
    else:
        keys = (
            f"catchuparr:sharing:current:{channel_uuid}",
            str(lease_key), operation_current_key, operation_sequence_key,
            operation_key, key, _worker_record_key(worker_id),
        )
        args = (
            str(operation_revision), str(config_generation), catalog_generation,
            str(control_generation), str(int(fence)), str(authority_revision),
            str(lease_value), attempt_id, worker_id, digest,
            json.dumps(record, sort_keys=True, separators=(",", ":")),
            stream_id, account_id, reservation_id, created_at,
            str(CAPABILITY_TTL_SECONDS), str(30 * 24 * 60 * 60), str(channel_uuid),
        )
        try:
            result = redis_client.eval(
                _ISSUE_SHARING_ATTEMPT_LUA, len(keys), *keys, *args,
            )
        except Exception as exc:
            reconciled = reconcile_unlaunched_sharing_operation(
                redis_client, channel_uuid=str(channel_uuid), operation_id=attempt_id,
                operation_revision=operation_revision, worker_id=worker_id,
                capability_digest=digest, current_lease_value=str(lease_value),
                current_lease_fence=int(fence), unreturned_capability=True,
            )
            if not reconciled:
                raise RecorderAttemptAdmissionUncertain(attempt) from exc
            raise RuntimeError("sharing recorder admission failed and was reconciled") from exc
        if type(result) is not int or result != operation_revision:
            if type(result) is not int:
                if not reconcile_unlaunched_sharing_operation(
                    redis_client, channel_uuid=str(channel_uuid), operation_id=attempt_id,
                    operation_revision=operation_revision, worker_id=worker_id,
                    capability_digest=digest, current_lease_value=str(lease_value),
                    current_lease_fence=int(fence), unreturned_capability=True,
                ):
                    raise RecorderAttemptAdmissionUncertain(attempt)
            raise RuntimeError("sharing recorder operation admission was denied")
    return attempt


def verify_recorder_capability(redis_client, token: str) -> dict[str, str] | None:
    if not isinstance(token, str) or not 48 <= len(token) <= 256:
        return None
    try:
        nonce, supplied_signature = token.split(".", 1)
        digest = hashlib.sha256(token.encode("ascii")).hexdigest()
        raw = redis_client.get(CAPABILITY_PREFIX + digest)
        if not raw:
            return None
        record = json.loads(_decode(raw))
        if not isinstance(record, dict):
            return None
        binding = {
            key: str(value)
            for key, value in record.items()
            if key != "capability_digest"
        }
        expected = _signature(binding, nonce)
        if not hmac.compare_digest(expected, supplied_signature):
            return None
        if record.get("capability_digest") != digest:
            return None
        return {key: str(value) for key, value in binding.items()} | {
            "capability_digest": digest
        }
    except Exception:
        return None


def capability_binding_current(redis_client, binding: Mapping[str, Any]) -> bool:
    """Validate current lease ownership, generation, channel, membership, and policy."""
    try:
        digest = str(binding["capability_digest"])
        raw_capability = redis_client.get(CAPABILITY_PREFIX + digest)
        if not raw_capability:
            return False
        stored = json.loads(_decode(raw_capability))
        if (
            not isinstance(stored, dict)
            or stored.get("capability_digest") != digest
        ):
            return False
        stored_binding = {
            key: str(value)
            for key, value in stored.items()
            if key != "capability_digest"
        }
        supplied_binding = {
            str(key): str(value)
            for key, value in binding.items()
            if key != "capability_digest"
        }
        if stored_binding != supplied_binding:
            return False
        if str(binding["lease_key"]) != f"catchuparr:recorder:{binding['channel_uuid']}":
            return False
        current_lease = _decode(redis_client.get(str(binding["lease_key"])))
        if current_lease != str(binding["lease_value"]):
            return False
        fence, _owner = current_lease.split(":", 1)
        if int(fence) != int(binding["lease_fence"]):
            return False
        from .configuration import load_applied_state

        active, control = load_applied_state()
        if control.paused or control.generation != int(binding["control_generation"]):
            return False
        if active is None or configuration_generation(active) != str(binding["config_generation"]):
            return False
        return candidate_is_current(
            str(binding["channel_uuid"]),
            str(binding["stream_id"]),
            str(binding["account_id"]),
            active,
        )
    except Exception:
        return False


def stream_recorder_view(request, channel_uuid: str):
    """Private TS route opened only by a currently fenced recorder process."""
    from core.utils import RedisClient
    from django.http import HttpResponse

    from .adapters.recorder_proxy import (
        core_api_supported,
        install_proxyserver_cleanup_hook,
        open_managed_source,
    )
    from .configuration import load_active_configuration
    from .runtime import require_supported_version

    if request.method != "GET":
        return HttpResponse(status=405)
    try:
        require_supported_version()
    except RuntimeError:
        return HttpResponse("Recorder source proxy is unavailable", status=503)
    if not install_proxyserver_cleanup_hook() or not core_api_supported():
        return HttpResponse("Recorder source proxy is unavailable", status=503)
    token = getattr(request, "headers", {}).get(CAPABILITY_HEADER, "")
    if not token:
        return HttpResponse("Recorder capability required", status=401)
    try:
        redis_client = RedisClient.get_client()
    except Exception:
        return HttpResponse("Recorder source proxy is unavailable", status=503)
    binding = verify_recorder_capability(redis_client, token)
    if binding is None or str(binding.get("channel_uuid")) != str(channel_uuid):
        return HttpResponse("Recorder capability is invalid", status=403)
    active = load_active_configuration()
    if active is None or not capability_binding_current(redis_client, binding):
        return HttpResponse("Recorder capability is stale", status=403)

    operation_open = None
    if binding.get("sharing_operation_id"):
        operation_open = begin_sharing_operation_open(redis_client, binding)
        if operation_open is None:
            return HttpResponse("Recorder attempt is no longer admitted", status=403)

    try:
        from apps.channels.models import StreamProfile
        from core.models import PROXY_PROFILE_NAME

        if not StreamProfile.objects.filter(
            name=PROXY_PROFILE_NAME, locked=True, is_active=True
        ).exists():
            if operation_open is not None:
                end_sharing_operation_open(redis_client, binding, *operation_open)
            return HttpResponse("Dispatcharr Proxy profile is unavailable", status=503)
    except Exception:
        logger.exception("Could not verify Dispatcharr Proxy profile")
        if operation_open is not None:
            end_sharing_operation_open(redis_client, binding, *operation_open)
        return HttpResponse("Recorder source proxy is unavailable", status=503)

    try:
        response = open_managed_source(request, str(binding["worker_id"]), binding)
        if 300 <= int(getattr(response, "status_code", 200)) < 400 or "Location" in response:
            if operation_open is not None:
                end_sharing_operation_open(redis_client, binding, *operation_open)
            return HttpResponse("Recorder source proxy cannot redirect", status=502)
        if operation_open is not None:
            open_key, nonce = operation_open
            if not activate_sharing_operation_open(
                redis_client, binding, open_key, nonce,
            ):
                close_response = getattr(response, "close", None)
                if callable(close_response):
                    try:
                        close_response()
                    except Exception:
                        logger.exception("Could not close a denied recorder response")
                end_sharing_operation_open(redis_client, binding, open_key, nonce)
                return HttpResponse("Recorder attempt is no longer admitted", status=502)
            close_operation_slot = _operation_slot_closer(
                redis_client, binding, open_key, nonce,
            )
            if not _attach_response_closer(response, close_operation_slot):
                close_operation_slot()
                return HttpResponse("Recorder stream lifecycle is unavailable", status=502)
        return response
    except Exception:
        if operation_open is not None:
            end_sharing_operation_open(redis_client, binding, *operation_open)
        raise
    finally:
        # The internal response is a long-lived stream. Release database
        # connections after all setup ORM reads and before the TS body starts.
        _close_database_connections()


def stop_recorder_attempt(redis_client, attempt: RecorderProxyAttempt, lease) -> bool:
    """Stop only the namespaced worker recorded for this recorder lease."""
    from .adapters.recorder_proxy import (
        _finalize_if_native_worker_gone,
        read_worker_record,
    )

    record = read_worker_record(redis_client, attempt.worker_id)
    if record is None:
        return attempt.sharing_operation_id is None
    if (
        record.get("lease_value") != getattr(lease, "_value", None)
        or record.get("lease_fence") != str(getattr(lease, "fence", ""))
    ):
        return False
    if attempt.sharing_operation_id is not None and (
        record.get("sharing_operation_id") != attempt.sharing_operation_id
        or record.get("sharing_operation_revision") != str(attempt.sharing_operation_revision)
    ):
        return False
    if record.get("state") in {"released", "failed"}:
        return True
    try:
        from apps.proxy.live_proxy.services.channel_service import ChannelService

        ChannelService.stop_channel(attempt.worker_id)
    except Exception:
        logger.exception("Could not stop recorder proxy worker %s", attempt.worker_id)
        return False
    deadline = time.monotonic() + 6.0
    while time.monotonic() < deadline:
        current = read_worker_record(redis_client, attempt.worker_id) or {}
        if current.get("state") in {"released", "failed"}:
            return True
        if _finalize_if_native_worker_gone(redis_client, attempt.worker_id):
            return True
        time.sleep(0.1)
    current = read_worker_record(redis_client, attempt.worker_id) or {}
    return current.get("state") in {"released", "failed"}
