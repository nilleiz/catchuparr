"""Read-only Dispatcharr native observations and Redis episode CAS primitives.

This module is an unwired adapter foundation. It never resolves URLs, reserves
provider slots, initializes/stops native channels, or touches shared native
manager maps. ORM/catalog construction belongs to coordinator code outside the
streaming hot path; the Redis methods here perform bounded native observations
and exact episode/client compare-and-set operations.
"""

from __future__ import annotations

import hashlib
import math
import time
from dataclasses import dataclass, field
from typing import Any, Iterable

from ..sharing_episode import BorrowEpisodeSnapshot
from ..sharing_planner import BorrowTarget
from .sharing_reader import BorrowReaderUnavailable, redis_backend_signature


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="strict")
    if isinstance(value, str):
        return value
    raise ValueError("Redis returned an unsupported value")


@dataclass(frozen=True)
class NativeAssignedSource:
    """Sanitized model/catalog facts loaded by coordinator code, never per chunk."""

    channel_uuid: str
    stream_id: str
    account_id: str
    m3u_profile_id: str
    input_profile_id: str
    assigned: bool
    account_active: bool
    profile_active: bool
    media_compatible: bool
    supported_worker_kind: bool = True
    redirect_channel: bool = False
    vod_or_preview: bool = False

    def __post_init__(self) -> None:
        for name in (
            "channel_uuid", "stream_id", "account_id", "m3u_profile_id", "input_profile_id",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be a non-empty string")
        for name in (
            "assigned", "account_active", "profile_active", "media_compatible",
            "supported_worker_kind", "redirect_channel", "vod_or_preview",
        ):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be boolean")


@dataclass(frozen=True)
class NativeSharingObservation:
    target: BorrowTarget
    observed_at: float
    state: str
    buffer_head: int
    ordinary_viewer_ids: tuple[str, ...]
    catalog_generation: str


@dataclass(frozen=True)
class NativeClientRegistrationAck:
    """Exact Lua acknowledgment required before local manager bookkeeping."""

    channel_uuid: str
    episode_id: str
    client_id: str
    local_worker_id: str
    redis_backend_signature: str
    connected_at: str
    projection_version: int

    def __post_init__(self) -> None:
        for name in (
            "channel_uuid", "episode_id", "client_id", "local_worker_id",
            "redis_backend_signature", "connected_at",
        ):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"{name} must be a non-empty string")
        if type(self.projection_version) is not int or self.projection_version < 2:
            raise ValueError("projection_version must be an acknowledged active version")


@dataclass(frozen=True)
class NativeEpisodeGrant:
    """Strict current Redis projection; unlike SQLite this is hot-path authority."""

    channel_uuid: str
    episode_id: str
    phase: str
    configuration_generation: str
    control_generation: int
    catalog_generation: str
    recorder_fence: int
    intent_epoch: int
    version: int
    lease_value: str = field(repr=False)
    worker_id: str
    owner_lock_value: str
    metadata_owner: str
    stream_id: str
    account_id: str
    m3u_profile_id: str
    input_profile_id: str
    client_id: str
    client_worker_id: str
    connected_at: str
    return_seconds: int
    minimum_switch_seconds: int
    last_switch_at: float
    last_viewer_left_at: float | None
    return_deadline: float | None

    def __post_init__(self) -> None:
        for name in (
            "channel_uuid", "episode_id", "configuration_generation", "catalog_generation",
            "lease_value", "worker_id", "owner_lock_value", "metadata_owner", "stream_id",
            "account_id", "m3u_profile_id", "input_profile_id", "client_id", "client_worker_id",
            "connected_at",
        ):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"{name} must be a non-empty string")
        if self.phase not in {"active", "wait_return"}:
            raise ValueError("only active episode projections grant reads")
        if not self.client_id.startswith("catchuparr-client-"):
            raise ValueError("client_id is not a private recorder client")
        for name in ("control_generation", "recorder_fence", "intent_epoch"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if type(self.version) is not int or self.version < 2:
            raise ValueError("version is not an active projection version")
        for name in ("return_seconds", "minimum_switch_seconds"):
            value = getattr(self, name)
            if type(value) is not int or not 0 <= value <= 3600:
                raise ValueError(f"{name} is outside its supported range")
        if isinstance(self.last_switch_at, bool) or not isinstance(self.last_switch_at, (float, int)) or not math.isfinite(self.last_switch_at):
            raise ValueError("last_switch_at must be finite")
        for name in ("last_viewer_left_at", "return_deadline"):
            value = getattr(self, name)
            if value is not None and (isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value)):
                raise ValueError(f"{name} must be finite or null")
        if self.phase == "active" and (self.last_viewer_left_at is not None or self.return_deadline is not None):
            raise ValueError("active grants cannot retain viewer grace")
        if self.phase == "wait_return" and (self.last_viewer_left_at is None or self.return_deadline is None):
            raise ValueError("waiting grants require finite viewer grace")
        try:
            if not math.isfinite(float(self.connected_at)):
                raise ValueError("connected_at must be finite")
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("connected_at must be finite") from exc
        if (self.phase == "wait_return" and self.return_deadline is not None
                and self.last_viewer_left_at is not None
                and self.return_deadline < self.last_viewer_left_at):
            raise ValueError("return deadline cannot precede viewer departure")


def _private_client(redis: Any, channel_uuid: str, client_id: str) -> bool | None:
    """Return private/ordinary/unknown using only server-ledger evidence."""
    private_key = f"catchuparr:sharing:private:{channel_uuid}"
    private_type = _text(redis.type(private_key))
    if private_type not in {"none", "hash"}:
        return None
    episode_id = redis.hget(private_key, client_id) if private_type == "hash" else None
    if episode_id is not None:
        try:
            episode = redis.hgetall(f"catchuparr:sharing:episode:{channel_uuid}") or {}
            values = {_text(key): _text(value) for key, value in episode.items()}
            if _text(episode_id) == values.get("episode_id"):
                return True
            return None
        except (UnicodeError, ValueError):
            return None

    # Dedicated recorder clients have an existing exact capability marker.
    marker_key = f"catchuparr:stats:recorder-client:{channel_uuid}:{client_id}"
    marker_type = _text(redis.type(marker_key))
    if marker_type == "none":
        return False
    if marker_type != "string":
        return None
    marker = redis.get(marker_key)
    if marker is None:
        return False
    try:
        digest, separator, marked_channel = _text(marker).partition("|")
        if not separator or not digest or marked_channel != channel_uuid:
            return None
        cap_key = f"catchuparr:recorder:stats-cap:{digest}"
        cap_type = _text(redis.type(cap_key))
        lease_key = f"catchuparr:recorder:{channel_uuid}"
        lease_type = _text(redis.type(lease_key))
        if cap_type != "string" or lease_type != "string":
            return None
        cap = redis.get(cap_key)
        lease = redis.get(lease_key)
        if cap is None or lease is None:
            return None
        cap_channel, cap_separator, cap_lease = _text(cap).partition("|")
        if cap_separator and cap_channel == channel_uuid and cap_lease == _text(lease):
            return True
        return None
    except (UnicodeError, ValueError):
        return None


def observe_native_connection(
    redis: Any,
    channel_uuid: str,
    *,
    catalog_generation: str,
    assigned_sources: Iterable[NativeAssignedSource],
    observed_at: float,
    recording_allowed_account_ids: frozenset[str] | None = None,
) -> NativeSharingObservation | None:
    """Observe an existing native connection, independent of source filters.

    The caller supplies a full, preloaded assigned-source catalog. This lets an
    excluded account's already-running connection be observed without making it
    eligible for a new dedicated launch. ORM work stays outside this adapter.
    """
    if not isinstance(channel_uuid, str) or not channel_uuid:
        raise ValueError("channel_uuid is required")
    if not isinstance(catalog_generation, str) or not catalog_generation:
        raise ValueError("catalog_generation is required")
    if isinstance(observed_at, bool) or not isinstance(observed_at, (int, float)) or not math.isfinite(float(observed_at)):
        raise ValueError("observed_at must be a finite wall-clock timestamp")
    try:
        owner_value = redis.get(f"live:channel:{channel_uuid}:owner")
        metadata_raw = redis.hgetall(f"live:channel:{channel_uuid}:metadata") or {}
        if not owner_value or not metadata_raw:
            return None
        owner_value = _text(owner_value)
        metadata = {_text(key): _text(value) for key, value in metadata_raw.items()}
        owner = metadata.get("owner", "")
        state = metadata.get("state", "").lower()
        if (
            owner != owner_value
            or state not in {"active", "waiting_for_clients"}
            or metadata.get("channel_uuid") != channel_uuid
            or int(redis.ttl(f"live:channel:{channel_uuid}:owner")) <= 0
            or redis.exists(f"live:channel:{channel_uuid}:stopping")
        ):
            return None
        heartbeat_key = f"live:worker:{owner}:heartbeat"
        if not redis.exists(heartbeat_key) or int(redis.ttl(heartbeat_key)) <= 0:
            return None
        stream_id = metadata.get("stream_id", "")
        m3u_profile_id = metadata.get("m3u_profile", "")
        input_profile_id = metadata.get("stream_profile", "")
        if not stream_id or not m3u_profile_id or not input_profile_id:
            return None
        matches = [
            source for source in assigned_sources
            if source.channel_uuid == channel_uuid
            and source.stream_id == stream_id
            and source.m3u_profile_id == m3u_profile_id
            and source.input_profile_id == input_profile_id
            and source.assigned
        ]
        if len(matches) != 1:
            return None
        source = matches[0]
        if (
            not source.account_active or not source.profile_active
            or not source.media_compatible or not source.supported_worker_kind
            or source.redirect_channel or source.vod_or_preview
        ):
            return None

        head_raw = redis.get(f"live:channel:{channel_uuid}:input:buffer:index")
        head = int(_text(head_raw)) if head_raw is not None else 0
        if head < 1:
            return None
        newest = redis.zrevrange(
            f"live:channel:{channel_uuid}:input:buffer:chunk_timestamps", 0, 0,
        )
        if not newest:
            return None
        chunk_id = int(_text(newest[0]))
        if chunk_id < 1 or chunk_id > head or not redis.exists(
            f"live:channel:{channel_uuid}:input:buffer:chunk:{chunk_id}"
        ):
            return None

        clients_key = f"live:channel:{channel_uuid}:clients"
        ordinary: list[str] = []
        for raw_client_id in redis.smembers(clients_key):
            client_id = _text(raw_client_id)
            private = _private_client(redis, channel_uuid, client_id)
            if private is None:
                return None
            if private:
                continue
            record = redis.hgetall(f"live:channel:{channel_uuid}:clients:{client_id}") or {}
            fields = {_text(key): _text(value) for key, value in record.items()}
            if (
                fields.get("worker_id")
                and fields.get("connected_at")
                and not redis.exists(f"live:channel:{channel_uuid}:client:{client_id}:stop")
            ):
                ordinary.append(client_id)
        ordinary_ids = tuple(sorted(set(ordinary)))
        target = BorrowTarget(
            channel_uuid=channel_uuid,
            worker_uuid=owner,
            owner_lock_value=owner_value,
            metadata_owner=owner,
            stream_id=source.stream_id,
            account_id=source.account_id,
            m3u_profile_id=source.m3u_profile_id,
            input_profile_id=source.input_profile_id,
            ordinary_client_ids=ordinary_ids,
            owner_heartbeat_healthy=True,
            assigned_stream_matches=True,
            account_active=source.account_active,
            profile_active=source.profile_active,
            media_compatible=source.media_compatible,
            useful_buffer_available=True,
            native_state_healthy=True,
            account_allowed_by_recording_policy=(
                recording_allowed_account_ids is None
                or source.account_id in recording_allowed_account_ids
            ),
            redirect_worker=source.redirect_channel,
            vod_or_preview_worker=source.vod_or_preview,
        )
        return NativeSharingObservation(
            target=target,
            observed_at=float(observed_at),
            state=state,
            buffer_head=head,
            ordinary_viewer_ids=ordinary_ids,
            catalog_generation=catalog_generation,
        )
    except (TypeError, ValueError, UnicodeError, OverflowError):
        return None


_PROJECT_EPISODE_LUA = """
local current_type = redis.call('TYPE', KEYS[1]).ok
local lease_type = redis.call('TYPE', KEYS[2]).ok
local projection_type = redis.call('TYPE', KEYS[3]).ok
if current_type ~= 'hash' or lease_type ~= 'string'
   or (projection_type ~= 'none' and projection_type ~= 'hash') then return 0 end
local ttl = tonumber(ARGV[18])
local return_seconds = tonumber(ARGV[14])
local minimum_switch = tonumber(ARGV[15])
local switch_at = tonumber(ARGV[16])
local version = tonumber(ARGV[17])
if not ttl or ttl < 1 or ttl > 720 or ttl % 1 ~= 0
   or not return_seconds or return_seconds < 0 or return_seconds > 3600 or return_seconds % 1 ~= 0
   or not minimum_switch or minimum_switch < 0 or minimum_switch > 3600 or minimum_switch % 1 ~= 0
   or not switch_at or switch_at < 0 or switch_at ~= switch_at or switch_at == math.huge
   or not version or version < 1 or version % 1 ~= 0 then return 0 end
if redis.call('HGET', KEYS[1], 'configuration_generation') ~= ARGV[1]
   or redis.call('HGET', KEYS[1], 'control_generation') ~= ARGV[2]
   or redis.call('HGET', KEYS[1], 'catalog_generation') ~= ARGV[3]
   or redis.call('GET', KEYS[2]) ~= ARGV[5]
   or redis.call('TTL', KEYS[2]) <= 0 then return 0 end
local phase = redis.call('HGET', KEYS[3], 'phase')
if phase and phase ~= 'closed' then return 0 end
redis.call('DEL', KEYS[3])
redis.call('HSET', KEYS[3],
  'channel_uuid', ARGV[19], 'episode_id', ARGV[6], 'phase', 'opening',
  'configuration_generation', ARGV[1], 'control_generation', ARGV[2],
  'catalog_generation', ARGV[3], 'recorder_fence', ARGV[4], 'intent_epoch', ARGV[20],
  'lease_value', ARGV[5], 'lease_digest', ARGV[21],
  'worker_id', ARGV[7], 'owner_lock_value', ARGV[8], 'metadata_owner', ARGV[9],
  'stream_id', ARGV[10], 'account_id', ARGV[11],
  'm3u_profile_id', ARGV[12], 'input_profile_id', ARGV[13],
  'return_seconds', ARGV[14], 'minimum_switch_seconds', ARGV[15],
  'last_switch_at', ARGV[16], 'version', ARGV[17])
redis.call('EXPIRE', KEYS[3], ttl)
return 1
"""

_PUBLISH_CURRENT_AUTHORITY_LUA = """
local kind = redis.call('TYPE', KEYS[1]).ok
if kind ~= 'none' and kind ~= 'hash' then return 0 end
if ARGV[3] == '' or ARGV[4] == '' or ARGV[5] == '' then return 0 end
local expected = tonumber(ARGV[1])
local next_revision = tonumber(ARGV[2])
local control = tonumber(ARGV[4])
local current = tonumber(redis.call('HGET', KEYS[1], 'authority_revision') or '0')
if not expected or expected < 0 or expected % 1 ~= 0
   or not next_revision or next_revision <= expected or next_revision % 1 ~= 0
   or not control or control < 0 or control % 1 ~= 0
   or not current or current % 1 ~= 0 or current ~= expected then return 0 end
redis.call('HSET', KEYS[1], 'configuration_generation', ARGV[3],
  'control_generation', ARGV[4], 'catalog_generation', ARGV[5],
  'authority_revision', tostring(next_revision))
return 1
"""

_REGISTER_BORROWED_CLIENT_LUA = """
local function private_client(id, channel)
  local episode_id = redis.call('HGET', KEYS[11], id)
  if episode_id then
    if episode_id == redis.call('HGET', KEYS[1], 'episode_id') then return true end
    return nil
  end
  local marker_key = 'catchuparr:stats:recorder-client:' .. channel .. ':' .. id
  local marker_type = redis.call('TYPE', marker_key).ok
  if marker_type ~= 'none' and marker_type ~= 'string' then return nil end
  local marker = redis.call('GET', marker_key)
  if not marker then return false end
  local digest, marked_channel = string.match(marker, '^([^|]+)|([^|]+)$')
  if not digest or marked_channel ~= channel then return nil end
  local cap_key = 'catchuparr:recorder:stats-cap:' .. digest
  local cap_type = redis.call('TYPE', cap_key).ok
  if cap_type ~= 'none' and cap_type ~= 'string' then return nil end
  local cap = redis.call('GET', cap_key)
  local lease = redis.call('GET', KEYS[3])
  if not cap or not lease then return nil end
  local cap_channel, cap_lease = string.match(cap, '^([^|]+)|(.+)$')
  if cap_channel == channel and cap_lease == lease then return true end
  return nil
end
local expected = {'hash', 'hash', 'string', 'string', 'string', 'hash', 'none', 'none', 'none', 'none', 'none', 'string', 'none'}
local allowed = {{'hash'}, {'hash'}, {'string'}, {'string'}, {'string'}, {'hash'},
  {'none', 'string'}, {'none', 'string'}, {'none', 'hash'}, {'none', 'set'},
  {'none', 'hash'}, {'string'}, {'none', 'zset'}}
for i = 1, #expected do
  local actual = redis.call('TYPE', KEYS[i]).ok
  local ok = false
  for _, value in ipairs(allowed[i]) do if actual == value then ok = true end end
  if not ok then return 0 end
end
local ttl = tonumber(ARGV[20])
if not ttl or ttl < 1 or ttl > 720 or ttl % 1 ~= 0 then return 0 end
if redis.call('HGET', KEYS[2], 'configuration_generation') ~= ARGV[1]
   or redis.call('HGET', KEYS[2], 'control_generation') ~= ARGV[2]
   or redis.call('HGET', KEYS[2], 'catalog_generation') ~= ARGV[3]
   or redis.call('HGET', KEYS[1], 'configuration_generation') ~= ARGV[1]
   or redis.call('HGET', KEYS[1], 'control_generation') ~= ARGV[2]
   or redis.call('HGET', KEYS[1], 'catalog_generation') ~= ARGV[3]
   or redis.call('HGET', KEYS[1], 'recorder_fence') ~= ARGV[4]
   or redis.call('HGET', KEYS[1], 'version') ~= ARGV[5]
   or redis.call('HGET', KEYS[1], 'intent_epoch') ~= ARGV[21]
   or redis.call('HGET', KEYS[1], 'lease_value') ~= ARGV[6]
   or redis.call('GET', KEYS[3]) ~= ARGV[6]
   or redis.call('TTL', KEYS[3]) <= 0
   or redis.call('TTL', KEYS[1]) <= 0
   or redis.call('HGET', KEYS[1], 'phase') ~= 'opening'
   or redis.call('HGET', KEYS[1], 'episode_id') ~= ARGV[7]
   or redis.call('HGET', KEYS[1], 'worker_id') ~= ARGV[9]
   or redis.call('HGET', KEYS[1], 'owner_lock_value') ~= ARGV[8]
   or redis.call('HGET', KEYS[1], 'metadata_owner') ~= ARGV[10]
   or redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[11]
   or redis.call('HGET', KEYS[1], 'account_id') ~= ARGV[12]
   or redis.call('HGET', KEYS[1], 'm3u_profile_id') ~= ARGV[13]
   or redis.call('HGET', KEYS[1], 'input_profile_id') ~= ARGV[14]
   or redis.call('HGET', KEYS[1], 'channel_uuid') ~= ARGV[15]
   or redis.call('GET', KEYS[4]) ~= ARGV[8]
   or redis.call('HGET', KEYS[6], 'owner') ~= ARGV[10]
   or redis.call('HGET', KEYS[6], 'channel_uuid') ~= ARGV[15]
   or redis.call('HGET', KEYS[6], 'stream_id') ~= ARGV[11]
   or redis.call('HGET', KEYS[6], 'm3u_profile') ~= ARGV[13]
   or redis.call('HGET', KEYS[6], 'stream_profile') ~= ARGV[14]
   or (redis.call('HGET', KEYS[6], 'state') ~= 'active'
       and redis.call('HGET', KEYS[6], 'state') ~= 'waiting_for_clients')
   or redis.call('EXISTS', KEYS[7]) ~= 0 or redis.call('EXISTS', KEYS[8]) ~= 0
   or redis.call('EXISTS', KEYS[9]) ~= 0
   or redis.call('SISMEMBER', KEYS[10], ARGV[16]) ~= 0
   or redis.call('HEXISTS', KEYS[11], ARGV[16]) ~= 0 then return 0 end
if redis.call('TTL', KEYS[4]) <= 0 or redis.call('TTL', KEYS[5]) <= 0 then return 0 end
local head = tonumber(redis.call('GET', KEYS[12]) or '')
local newest = redis.call('ZREVRANGE', KEYS[13], 0, 0)
if not head or head < 1 or head % 1 ~= 0 or #newest == 0 then return 0 end
local chunk_key = 'live:channel:' .. ARGV[15] .. ':input:buffer:chunk:' .. newest[1]
if redis.call('TYPE', chunk_key).ok ~= 'string' then return 0 end
local ordinary = 0
for _, id in ipairs(redis.call('SMEMBERS', KEYS[10])) do
  local client_key = 'live:channel:' .. ARGV[15] .. ':clients:' .. id
  local stop_key = 'live:channel:' .. ARGV[15] .. ':client:' .. id .. ':stop'
  local client_type = redis.call('TYPE', client_key).ok
  local stop_type = redis.call('TYPE', stop_key).ok
  if (client_type ~= 'none' and client_type ~= 'hash')
     or (stop_type ~= 'none' and stop_type ~= 'string') then return 0 end
  local connected_at = client_type == 'hash' and tonumber(redis.call('HGET', client_key, 'connected_at') or '') or nil
  local worker_id = client_type == 'hash' and redis.call('HGET', client_key, 'worker_id') or nil
  local private = private_client(id, ARGV[15])
  if private == nil then return 0 end
  if not private and client_type == 'hash'
     and connected_at and connected_at > 0 and connected_at == connected_at
     and connected_at < math.huge and worker_id and worker_id ~= '' and stop_type == 'none' then
    ordinary = ordinary + 1
  end
end
if ordinary < 1 then return 0 end
local nowparts = redis.call('TIME')
local now = tonumber(nowparts[1]) + tonumber(nowparts[2]) / 1000000
local nowstr = string.format('%.6f', now)
local version = tonumber(ARGV[5]) + 1
redis.call('HSET', KEYS[9], 'user_agent', ARGV[17], 'ip_address', ARGV[18],
  'connected_at', nowstr, 'last_active', nowstr, 'worker_id', ARGV[19],
  'user_id', '0', 'output_format', 'mpegts', 'output_profile_id', '')
redis.call('EXPIRE', KEYS[9], ttl)
redis.call('SADD', KEYS[10], ARGV[16])
redis.call('EXPIRE', KEYS[10], ttl)
redis.call('HSET', KEYS[11], ARGV[16], ARGV[7])
redis.call('EXPIRE', KEYS[11], ttl)
redis.call('HSET', KEYS[1], 'phase', 'active', 'client_id', ARGV[16],
  'connected_at', nowstr, 'viewer_left_at', '', 'return_deadline', '',
  'version', tostring(version), 'client_worker_id', ARGV[19])
redis.call('EXPIRE', KEYS[1], ttl)
return {1, nowstr, tostring(version)}
"""

_CHECK_AND_TOUCH_EPISODE_LUA = """
local function private_client(id, channel)
  local own_episode = redis.call('HGET', KEYS[10], id)
  if own_episode then
    if own_episode == ARGV[7] then return true end
    return nil
  end
  local marker_key = 'catchuparr:stats:recorder-client:' .. channel .. ':' .. id
  local marker_type = redis.call('TYPE', marker_key).ok
  if marker_type ~= 'none' and marker_type ~= 'string' then return nil end
  local marker = redis.call('GET', marker_key)
  if not marker then return false end
  local digest, marked_channel = string.match(marker, '^([^|]+)|([^|]+)$')
  if not digest or marked_channel ~= channel then return nil end
  local cap_key = 'catchuparr:recorder:stats-cap:' .. digest
  local cap_type = redis.call('TYPE', cap_key).ok
  if cap_type ~= 'none' and cap_type ~= 'string' then return nil end
  local cap = redis.call('GET', cap_key)
  local lease = redis.call('GET', KEYS[3])
  if not cap or not lease then return nil end
  local cap_channel, cap_lease = string.match(cap, '^([^|]+)|(.+)$')
  if cap_channel == channel and cap_lease == lease then return true end
  return nil
end
local allowed = {{'hash'}, {'hash'}, {'string'}, {'string'}, {'string'}, {'hash'},
  {'none', 'string'}, {'hash'}, {'none', 'set'}, {'none', 'hash'},
  {'none', 'string'}, {'string'}, {'none', 'zset'}}
for i = 1, #allowed do
  local kind = redis.call('TYPE', KEYS[i]).ok
  local valid = false
  for _, expected in ipairs(allowed[i]) do if kind == expected then valid = true end end
  if not valid then return 0 end
end
local ttl = tonumber(ARGV[20])
if not ttl or ttl < 1 or ttl > 720 or ttl % 1 ~= 0 then return 0 end
local phase = redis.call('HGET', KEYS[1], 'phase')
if (phase ~= 'active' and phase ~= 'wait_return')
   or redis.call('HGET', KEYS[1], 'episode_id') ~= ARGV[7]
   or redis.call('HGET', KEYS[1], 'version') ~= ARGV[8]
   or redis.call('HGET', KEYS[1], 'intent_epoch') ~= ARGV[9]
   or redis.call('HGET', KEYS[1], 'channel_uuid') ~= ARGV[10]
   or redis.call('HGET', KEYS[1], 'configuration_generation') ~= ARGV[1]
   or redis.call('HGET', KEYS[1], 'control_generation') ~= ARGV[2]
   or redis.call('HGET', KEYS[1], 'catalog_generation') ~= ARGV[3]
   or redis.call('HGET', KEYS[1], 'recorder_fence') ~= ARGV[4]
   or redis.call('HGET', KEYS[1], 'lease_value') ~= ARGV[5]
   or redis.call('GET', KEYS[3]) ~= ARGV[5]
   or redis.call('GET', KEYS[4]) ~= ARGV[6]
   or redis.call('HGET', KEYS[1], 'owner_lock_value') ~= ARGV[6]
   or redis.call('HGET', KEYS[1], 'worker_id') ~= ARGV[12]
   or redis.call('HGET', KEYS[1], 'metadata_owner') ~= ARGV[11]
   or redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[13]
   or redis.call('HGET', KEYS[1], 'account_id') ~= ARGV[14]
   or redis.call('HGET', KEYS[1], 'm3u_profile_id') ~= ARGV[15]
   or redis.call('HGET', KEYS[1], 'input_profile_id') ~= ARGV[16]
   or redis.call('HGET', KEYS[1], 'client_id') ~= ARGV[17]
   or redis.call('HGET', KEYS[1], 'connected_at') ~= ARGV[18]
   or redis.call('HGET', KEYS[1], 'client_worker_id') ~= ARGV[19]
   or redis.call('HGET', KEYS[2], 'configuration_generation') ~= ARGV[1]
   or redis.call('HGET', KEYS[2], 'control_generation') ~= ARGV[2]
   or redis.call('HGET', KEYS[2], 'catalog_generation') ~= ARGV[3]
   or redis.call('HGET', KEYS[6], 'owner') ~= ARGV[11]
   or redis.call('HGET', KEYS[6], 'channel_uuid') ~= ARGV[10]
   or redis.call('HGET', KEYS[6], 'stream_id') ~= ARGV[13]
   or redis.call('HGET', KEYS[6], 'm3u_profile') ~= ARGV[15]
   or redis.call('HGET', KEYS[6], 'stream_profile') ~= ARGV[16]
   or (redis.call('HGET', KEYS[6], 'state') ~= 'active'
       and redis.call('HGET', KEYS[6], 'state') ~= 'waiting_for_clients')
   or redis.call('EXISTS', KEYS[7]) ~= 0
   or redis.call('EXISTS', KEYS[11]) ~= 0
   or redis.call('HGET', KEYS[8], 'connected_at') ~= ARGV[18]
   or redis.call('HGET', KEYS[8], 'worker_id') ~= ARGV[19]
   or redis.call('HGET', KEYS[8], 'user_id') ~= '0'
   or redis.call('HGET', KEYS[8], 'output_format') ~= 'mpegts'
   or redis.call('HGET', KEYS[8], 'output_profile_id') ~= ''
   or redis.call('SISMEMBER', KEYS[9], ARGV[17]) ~= 1
   or redis.call('HGET', KEYS[10], ARGV[17]) ~= ARGV[7] then return 0 end
if redis.call('TTL', KEYS[1]) <= 0 or redis.call('TTL', KEYS[3]) <= 0
   or redis.call('TTL', KEYS[4]) <= 0 or redis.call('TTL', KEYS[5]) <= 0
   or redis.call('TTL', KEYS[8]) <= 0 then return 0 end
local head = tonumber(redis.call('GET', KEYS[12]) or '')
local newest = redis.call('ZREVRANGE', KEYS[13], 0, 0)
if not head or head < 1 or head % 1 ~= 0 or #newest == 0 then return 0 end
local chunk_key = 'live:channel:' .. ARGV[10] .. ':input:buffer:chunk:' .. newest[1]
if redis.call('TYPE', chunk_key).ok ~= 'string' then return 0 end
local ordinary = 0
for _, id in ipairs(redis.call('SMEMBERS', KEYS[9])) do
  local client_key = 'live:channel:' .. ARGV[10] .. ':clients:' .. id
  local stop_key = 'live:channel:' .. ARGV[10] .. ':client:' .. id .. ':stop'
  local client_type = redis.call('TYPE', client_key).ok
  local stop_type = redis.call('TYPE', stop_key).ok
  if (client_type ~= 'none' and client_type ~= 'hash')
     or (stop_type ~= 'none' and stop_type ~= 'string') then return 0 end
  local connected_at = client_type == 'hash' and tonumber(redis.call('HGET', client_key, 'connected_at') or '') or nil
  local worker_id = client_type == 'hash' and redis.call('HGET', client_key, 'worker_id') or nil
  local private = private_client(id, ARGV[10])
  if private == nil then return 0 end
  if not private and client_type == 'hash'
     and connected_at and connected_at > 0 and connected_at == connected_at
     and connected_at < math.huge and worker_id and worker_id ~= '' and stop_type == 'none' then
    ordinary = ordinary + 1
  end
end
local nowparts = redis.call('TIME')
local now = tonumber(nowparts[1]) + tonumber(nowparts[2]) / 1000000
local return_seconds = tonumber(redis.call('HGET', KEYS[1], 'return_seconds') or '')
local minimum_switch = tonumber(redis.call('HGET', KEYS[1], 'minimum_switch_seconds') or '')
local last_switch = tonumber(redis.call('HGET', KEYS[1], 'last_switch_at') or '')
if not return_seconds or return_seconds < 0 or return_seconds > 3600 or return_seconds % 1 ~= 0
   or not minimum_switch or minimum_switch < 0 or minimum_switch > 3600 or minimum_switch % 1 ~= 0
   or not last_switch or last_switch < 0 or last_switch ~= last_switch or last_switch == math.huge then return 0 end
local deadline_raw = redis.call('HGET', KEYS[1], 'return_deadline') or ''
local deadline = nil
if deadline_raw ~= '' then
  deadline = tonumber(deadline_raw)
  if not deadline or deadline < 0 or deadline ~= deadline or deadline == math.huge then return 0 end
end
local next_phase = phase
local changed = false
if phase == 'active' and ordinary == 0 then
  if last_switch > now then last_switch = now end
  deadline = math.max(now + return_seconds, last_switch + minimum_switch)
  next_phase = 'wait_return'
  changed = true
elseif phase == 'wait_return' then
  if deadline == nil or now >= deadline then return 0 end
  if ordinary > 0 then
    next_phase = 'active'
    deadline = nil
    changed = true
  end
end
if ordinary == 0 and deadline ~= nil and now >= deadline then return 0 end
local nowstr = string.format('%.6f', now)
redis.call('HSET', KEYS[8], 'last_active', nowstr)
redis.call('EXPIRE', KEYS[8], ttl)
redis.call('EXPIRE', KEYS[9], ttl)
redis.call('EXPIRE', KEYS[10], ttl)
local version = tonumber(ARGV[8])
if changed then
  version = version + 1
  redis.call('HSET', KEYS[1], 'phase', next_phase,
    'viewer_left_at', next_phase == 'wait_return' and nowstr or '',
    'return_deadline', deadline and string.format('%.6f', deadline) or '',
    'version', tostring(version))
end
redis.call('EXPIRE', KEYS[1], ttl)
return {1, deadline and string.format('%.6f', deadline) or '', tostring(version)}
"""


def episode_projection_key(channel_uuid: str) -> str:
    return f"catchuparr:sharing:episode:{channel_uuid}"


def read_native_episode_grant(redis: Any, channel_uuid: str) -> NativeEpisodeGrant | None:
    """Read and strictly decode the current Redis projection, never SQLite."""
    if not isinstance(channel_uuid, str) or not channel_uuid:
        raise ValueError("channel_uuid is required")
    try:
        raw = redis.hgetall(episode_projection_key(channel_uuid)) or {}
        values = {_text(key): _text(value) for key, value in raw.items()}
        if values.get("channel_uuid") != channel_uuid:
            return None
        left = values.get("viewer_left_at", "")
        deadline = values.get("return_deadline", "")
        grant = NativeEpisodeGrant(
            channel_uuid=channel_uuid, episode_id=values["episode_id"], phase=values["phase"],
            configuration_generation=values["configuration_generation"],
            control_generation=int(values["control_generation"]),
            catalog_generation=values["catalog_generation"],
            recorder_fence=int(values["recorder_fence"]), intent_epoch=int(values["intent_epoch"]),
            version=int(values["version"]), lease_value=values["lease_value"],
            worker_id=values["worker_id"], owner_lock_value=values["owner_lock_value"],
            metadata_owner=values["metadata_owner"], stream_id=values["stream_id"],
            account_id=values["account_id"], m3u_profile_id=values["m3u_profile_id"],
            input_profile_id=values["input_profile_id"], client_id=values["client_id"],
            client_worker_id=values["client_worker_id"],
            connected_at=values["connected_at"], return_seconds=int(values["return_seconds"]),
            minimum_switch_seconds=int(values["minimum_switch_seconds"]),
            last_switch_at=float(values["last_switch_at"]),
            last_viewer_left_at=None if left == "" else float(left),
            return_deadline=None if deadline == "" else float(deadline),
        )
        if hashlib.sha256(grant.lease_value.encode()).hexdigest() != values.get("lease_digest"):
            return None
        return grant
    except (KeyError, TypeError, ValueError, UnicodeError, OverflowError):
        return None


def publish_current_authority(redis: Any, channel_uuid: str, *, configuration_generation: str,
                              control_generation: int, catalog_generation: str,
                              expected_revision: int, authority_revision: int) -> bool:
    """CAS DB-issued monotonic authority revisions after Apply/Stop/catalog commit."""
    if (not channel_uuid or not configuration_generation or not catalog_generation
            or type(control_generation) is not int or control_generation < 0
            or type(expected_revision) is not int or expected_revision < 0
            or type(authority_revision) is not int or authority_revision <= expected_revision):
        raise ValueError("current authority identity is incomplete")
    key = f"catchuparr:sharing:current:{channel_uuid}"
    status = redis.eval(
        _PUBLISH_CURRENT_AUTHORITY_LUA, 1, key,
        str(expected_revision), str(authority_revision), configuration_generation,
        str(control_generation), catalog_generation,
    )
    return type(status) is int and status == 1


def project_opening_episode(redis: Any, episode: BorrowEpisodeSnapshot, *, lease_value: str,
                            catalog_generation: str, return_seconds: int,
                            minimum_switch_seconds: int, last_switch_at: float | None,
                            ttl_seconds: int = 120) -> bool:
    """Project an acknowledged coordinator opening for native Lua admission."""
    if (episode.phase != "opening" or not isinstance(lease_value, str) or not lease_value
            or hashlib.sha256(lease_value.encode()).hexdigest() != episode.lease_digest
            or not isinstance(catalog_generation, str) or not catalog_generation):
        return False
    for name, value in (("return_seconds", return_seconds), ("minimum_switch_seconds", minimum_switch_seconds)):
        if type(value) is not int or not 0 <= value <= 3600:
            raise ValueError(f"{name} must be an integer from 0 to 3600")
    if (last_switch_at is not None and
            (isinstance(last_switch_at, bool) or not isinstance(last_switch_at, (int, float)))):
        raise ValueError("last_switch_at must be finite and non-negative")
    switch_at = 0.0 if last_switch_at is None else float(last_switch_at)
    if not math.isfinite(switch_at) or switch_at < 0:
        raise ValueError("last_switch_at must be finite and non-negative")
    if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 720:
        raise ValueError("ttl_seconds must be an integer from 1 to 720")
    current = f"catchuparr:sharing:current:{episode.channel_uuid}"
    status = redis.eval(
        _PROJECT_EPISODE_LUA, 3,
        current, f"catchuparr:recorder:{episode.channel_uuid}",
        episode_projection_key(episode.channel_uuid),
        episode.configuration_generation, str(episode.control_generation), catalog_generation,
        str(episode.recorder_fence), lease_value, episode.episode_id,
        episode.identity[1], episode.identity[2], episode.identity[3],
        episode.identity[4], episode.identity[5], episode.identity[6], episode.identity[7],
        str(return_seconds), str(minimum_switch_seconds), str(switch_at),
        str(episode.version), str(ttl_seconds), episode.channel_uuid,
        str(episode.intent_epoch), episode.lease_digest,
    )
    return type(status) is int and status == 1


def register_borrowed_client(redis: Any, episode: BorrowEpisodeSnapshot, *,
                             lease_value: str, catalog_generation: str, client_id: str,
                             local_worker_id: str, user_agent: str, client_ip: str,
                             client_record_ttl: int = 60) -> NativeClientRegistrationAck | None:
    """Atomically admit and ledger a private client while ordinary viewers exist."""
    identity = episode.identity
    if (episode.phase != "opening" or not isinstance(client_id, str)
            or not client_id.startswith("catchuparr-client-")
            or identity[1] != identity[2] or identity[1] != identity[3]
            or not all(isinstance(x, str) and x for x in (local_worker_id, user_agent, client_ip))
            or not isinstance(lease_value, str) or not lease_value
            or hashlib.sha256(lease_value.encode()).hexdigest() != episode.lease_digest
            or not isinstance(catalog_generation, str) or not catalog_generation
            or type(client_record_ttl) is not int or not 1 <= client_record_ttl <= 720):
        return None
    try:
        backend_signature = redis_backend_signature(redis)
    except BorrowReaderUnavailable:
        return None
    channel = episode.channel_uuid
    keys = [
        episode_projection_key(channel), f"catchuparr:sharing:current:{channel}",
        f"catchuparr:recorder:{channel}", f"live:channel:{channel}:owner",
        f"live:worker:{identity[1]}:heartbeat", f"live:channel:{channel}:metadata",
        f"live:channel:{channel}:stopping", f"live:channel:{channel}:client:{client_id}:stop",
        f"live:channel:{channel}:clients:{client_id}", f"live:channel:{channel}:clients",
        f"catchuparr:sharing:private:{channel}", f"live:channel:{channel}:input:buffer:index",
        f"live:channel:{channel}:input:buffer:chunk_timestamps",
    ]
    result = redis.eval(
        _REGISTER_BORROWED_CLIENT_LUA, len(keys), *keys,
        episode.configuration_generation, str(episode.control_generation), catalog_generation,
        str(episode.recorder_fence), str(episode.version), lease_value, episode.episode_id,
        identity[2], identity[1], identity[3], identity[4], identity[5], identity[6],
        identity[7], identity[0], client_id, user_agent, client_ip, local_worker_id,
        client_record_ttl, str(episode.intent_epoch),
    )
    if not isinstance(result, (list, tuple)) or len(result) != 3 or result[0] != 1:
        return None
    try:
        connected_at = _text(result[1])
        version = int(_text(result[2]))
        if not math.isfinite(float(connected_at)) or version != episode.version + 1:
            return None
    except (TypeError, ValueError, UnicodeError, OverflowError):
        return None
    return NativeClientRegistrationAck(
        channel_uuid=channel, episode_id=episode.episode_id, client_id=client_id,
        local_worker_id=local_worker_id, redis_backend_signature=backend_signature,
        connected_at=connected_at, projection_version=version,
    )


def adopt_registered_client_in_manager(manager: Any, ack: NativeClientRegistrationAck) -> bool:
    """Install only local bookkeeping after Lua atomically registered the client.

    This deliberately avoids ``ClientManager.add_client``: that method performs
    unrelated Redis writes, shutdown cancellation, initialization-key deletion,
    and event publication after the atomic registration.
    """
    if (not isinstance(ack, NativeClientRegistrationAck)
            or getattr(manager, "channel_id", None) != ack.channel_uuid
            or getattr(manager, "worker_id", None) != ack.local_worker_id):
        return False
    try:
        manager_backend = redis_backend_signature(getattr(manager, "redis_client", None))
    except BorrowReaderUnavailable:
        return False
    if manager_backend != ack.redis_backend_signature:
        return False
    lock = getattr(manager, "lock", None)
    clients = getattr(manager, "clients", None)
    registered = getattr(manager, "_registered_clients", None)
    heartbeat_times = getattr(manager, "last_heartbeat_time", None)
    if (lock is None or not hasattr(lock, "__enter__") or not hasattr(lock, "__exit__")
            or not isinstance(clients, set) or not isinstance(registered, set)
            or not isinstance(heartbeat_times, dict)):
        return False
    with lock:
        if ack.client_id in clients or ack.client_id in registered:
            return False
        clients.add(ack.client_id)
        registered.add(ack.client_id)
        heartbeat_times[ack.client_id] = time.time()
        if hasattr(manager, "last_active_time"):
            manager.last_active_time = time.time()
    return True


def check_and_touch_borrowed_client(redis: Any, grant: NativeEpisodeGrant, *,
                                    client_record_ttl: int = 60) -> tuple[bool, float | None, int | None]:
    """Revalidate current Redis grant and heartbeat the exact client before publication."""
    if not isinstance(grant, NativeEpisodeGrant):
        return False, None, None
    if (grant.worker_id != grant.owner_lock_value
            or grant.worker_id != grant.metadata_owner):
        return False, None, None
    if type(client_record_ttl) is not int or not 1 <= client_record_ttl <= 720:
        return False, None, None
    channel = grant.channel_uuid
    client_id = grant.client_id
    keys = [
        episode_projection_key(channel), f"catchuparr:sharing:current:{channel}",
        f"catchuparr:recorder:{channel}", f"live:channel:{channel}:owner",
        f"live:worker:{grant.worker_id}:heartbeat", f"live:channel:{channel}:metadata",
        f"live:channel:{channel}:stopping", f"live:channel:{channel}:clients:{client_id}",
        f"live:channel:{channel}:clients", f"catchuparr:sharing:private:{channel}",
        f"live:channel:{channel}:client:{client_id}:stop",
        f"live:channel:{channel}:input:buffer:index",
        f"live:channel:{channel}:input:buffer:chunk_timestamps",
    ]
    result = redis.eval(
        _CHECK_AND_TOUCH_EPISODE_LUA, len(keys), *keys,
        grant.configuration_generation, str(grant.control_generation), grant.catalog_generation,
        str(grant.recorder_fence), grant.lease_value, grant.owner_lock_value,
        grant.episode_id, str(grant.version), str(grant.intent_epoch), channel,
        grant.metadata_owner, grant.worker_id, grant.stream_id, grant.account_id,
        grant.m3u_profile_id, grant.input_profile_id, client_id, grant.connected_at,
        grant.client_worker_id, client_record_ttl,
    )
    if not isinstance(result, (list, tuple)) or len(result) != 3 or result[0] != 1:
        return False, None, None
    try:
        raw_deadline = _text(result[1])
        deadline = None if raw_deadline == "" else float(raw_deadline)
        version = int(_text(result[2]))
        if deadline is not None and not math.isfinite(deadline):
            return False, None, None
        if version not in {grant.version, grant.version + 1}:
            return False, None, None
    except (TypeError, ValueError, UnicodeError, OverflowError):
        return False, None, None
    return True, deadline, version
