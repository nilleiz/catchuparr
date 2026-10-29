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
import re
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
    user_agent: str = field(repr=False)
    client_ip: str = field(repr=False)

    def __post_init__(self) -> None:
        for name in (
            "channel_uuid", "episode_id", "client_id", "local_worker_id",
            "redis_backend_signature", "connected_at", "user_agent", "client_ip",
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


@dataclass(frozen=True)
class NativeEpisodeInvalidation:
    """Persistable exact revocation identity without the raw recorder lease."""

    channel_uuid: str
    episode_id: str
    phase: str
    configuration_generation: str
    control_generation: int
    catalog_generation: str
    recorder_fence: int
    intent_epoch: int
    version: int
    lease_digest: str
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

    def __post_init__(self) -> None:
        for name in (
            "channel_uuid", "episode_id", "configuration_generation", "catalog_generation",
            "worker_id", "owner_lock_value", "metadata_owner", "stream_id", "account_id",
            "m3u_profile_id", "input_profile_id", "client_id", "client_worker_id", "connected_at",
        ):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"{name} must be a non-empty string")
        if self.phase not in {"active", "wait_return"}:
            raise ValueError("only an active prior episode may be invalidated")
        for name in ("control_generation", "recorder_fence", "intent_epoch"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if type(self.version) is not int or self.version < 2:
            raise ValueError("version must be an active projection version")
        if not re.fullmatch(r"[0-9a-f]{64}", self.lease_digest):
            raise ValueError("lease_digest must be a SHA-256 hex digest")

    @classmethod
    def from_grant(cls, grant: NativeEpisodeGrant) -> "NativeEpisodeInvalidation":
        if not isinstance(grant, NativeEpisodeGrant):
            raise ValueError("native episode grant is required")
        return cls(
            channel_uuid=grant.channel_uuid, episode_id=grant.episode_id, phase=grant.phase,
            configuration_generation=grant.configuration_generation,
            control_generation=grant.control_generation, catalog_generation=grant.catalog_generation,
            recorder_fence=grant.recorder_fence, intent_epoch=grant.intent_epoch,
            version=grant.version,
            lease_digest=hashlib.sha256(grant.lease_value.encode("utf-8")).hexdigest(),
            worker_id=grant.worker_id, owner_lock_value=grant.owner_lock_value,
            metadata_owner=grant.metadata_owner, stream_id=grant.stream_id,
            account_id=grant.account_id, m3u_profile_id=grant.m3u_profile_id,
            input_profile_id=grant.input_profile_id, client_id=grant.client_id,
            client_worker_id=grant.client_worker_id, connected_at=grant.connected_at,
        )


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
        # The marker vanished between TYPE and GET (for example, an expiring
        # recorder marker). Do not reinterpret that ambiguous client as a
        # positive ordinary-viewer signal.
        return None
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
    rejection_reasons: list[str] | None = None,
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
    def rejected(reason: str):
        if rejection_reasons is not None:
            rejection_reasons.append(reason)
        return None

    try:
        owner_value = redis.get(f"live:channel:{channel_uuid}:owner")
        metadata_raw = redis.hgetall(f"live:channel:{channel_uuid}:metadata") or {}
        if not owner_value or not metadata_raw:
            return rejected("native-metadata-unavailable")
        owner_value = _text(owner_value)
        metadata = {_text(key): _text(value) for key, value in metadata_raw.items()}
        owner = metadata.get("owner", "")
        state = metadata.get("state", "").lower()
        if (
            owner != owner_value
            # Native metadata is already scoped by the channel-specific Redis
            # key. Current Dispatcharr writers do not store channel_uuid in
            # this hash; reject it only when a writer supplies a contradiction.
            or (
                "channel_uuid" in metadata
                and metadata["channel_uuid"] != channel_uuid
            )
            or int(redis.ttl(f"live:channel:{channel_uuid}:owner")) <= 0
            or redis.exists(f"live:channel:{channel_uuid}:stopping")
        ):
            return rejected("native-owner-or-state-stale")
        if state not in {"active", "waiting_for_clients"}:
            return rejected("native-buffering" if state == "buffering" else "native-owner-or-state-stale")
        heartbeat_key = f"live:worker:{owner}:heartbeat"
        if not redis.exists(heartbeat_key) or int(redis.ttl(heartbeat_key)) <= 0:
            return rejected("native-owner-unhealthy")
        stream_id = metadata.get("stream_id", "")
        m3u_profile_id = metadata.get("m3u_profile", "")
        input_profile_id = metadata.get("stream_profile", "")
        if not stream_id or not m3u_profile_id or not input_profile_id:
            return rejected("native-source-incomplete")
        matches = [
            source for source in assigned_sources
            if source.channel_uuid == channel_uuid
            and source.stream_id == stream_id
            and source.m3u_profile_id == m3u_profile_id
            and source.input_profile_id == input_profile_id
            and source.assigned
        ]
        if len(matches) != 1:
            return rejected("native-assignment-mismatch")
        source = matches[0]
        if (
            not source.account_active or not source.profile_active
            or not source.media_compatible or not source.supported_worker_kind
            or source.redirect_channel or source.vod_or_preview
        ):
            return rejected("native-source-unavailable")

        # Both tested native writers publish the index before the timestamp set.
        # Read the set first, then its upper bound. Reading index first can
        # compare an old head with a newly published chunk and falsely tear
        # down a healthy borrow on ordinary forward buffer progress.
        newest = redis.zrevrange(
            f"live:channel:{channel_uuid}:input:buffer:chunk_timestamps", 0, 0,
        )
        if not newest:
            return rejected("native-buffer-empty")
        head_raw = redis.get(f"live:channel:{channel_uuid}:input:buffer:index")
        head = int(_text(head_raw)) if head_raw is not None else 0
        if head < 1:
            return rejected("native-buffer-empty")
        chunk_id = int(_text(newest[0]))
        if chunk_id < 1 or chunk_id > head:
            return rejected("native-buffer-index-inconsistent")
        if not redis.exists(f"live:channel:{channel_uuid}:input:buffer:chunk:{chunk_id}"):
            return rejected("native-buffer-chunk-unavailable")

        clients_key = f"live:channel:{channel_uuid}:clients"
        ordinary: list[str] = []
        for raw_client_id in redis.smembers(clients_key):
            client_id = _text(raw_client_id)
            private = _private_client(redis, channel_uuid, client_id)
            if private is None:
                return rejected("native-client-classification-unknown")
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
        return rejected("native-data-invalid")


_PROJECT_EPISODE_LUA = """
local current_type = redis.call('TYPE', KEYS[1]).ok
local lease_type = redis.call('TYPE', KEYS[2]).ok
local projection_type = redis.call('TYPE', KEYS[3]).ok
local tombstone_type = redis.call('TYPE', KEYS[4]).ok
if current_type ~= 'hash' or lease_type ~= 'string' or tombstone_type ~= 'none'
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
  'client_id', ARGV[22],
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
  if marker_type == 'none' then return false end
  local marker = redis.call('GET', marker_key)
  if not marker then return nil end
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
local expected = {'hash', 'hash', 'string', 'string', 'string', 'hash', 'none', 'none', 'none', 'none', 'none', 'string', 'none', 'none'}
local allowed = {{'hash'}, {'hash'}, {'string'}, {'string'}, {'string'}, {'hash'},
  {'none', 'string'}, {'none', 'string'}, {'none', 'hash'}, {'none', 'set'},
  {'none', 'hash'}, {'string'}, {'none', 'zset'}, {'none', 'string'}}
for i = 1, #expected do
  local actual = redis.call('TYPE', KEYS[i]).ok
  local ok = false
  for _, value in ipairs(allowed[i]) do if actual == value then ok = true end end
  if not ok then return 0 end
end
local ttl = tonumber(ARGV[20])
if not ttl or ttl < 1 or ttl > 720 or ttl % 1 ~= 0 then return 0 end
local metadata_channel = redis.call('HGET', KEYS[6], 'channel_uuid')
if metadata_channel and metadata_channel ~= ARGV[15] then return 0 end
if redis.call('EXISTS', KEYS[14]) ~= 0 then return 0 end
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
   or redis.call('HGET', KEYS[1], 'client_id') ~= ARGV[16]
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
  'last_switch_at', nowstr,
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
  if marker_type == 'none' then return false end
  local marker = redis.call('GET', marker_key)
  if not marker then return nil end
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
  if not valid then return {0, "key-type"} end
end
local ttl = tonumber(ARGV[20])
if not ttl or ttl < 1 or ttl > 720 or ttl % 1 ~= 0 then return {0, "invalid-ttl"} end
local metadata_channel = redis.call('HGET', KEYS[6], 'channel_uuid')
if metadata_channel and metadata_channel ~= ARGV[10] then return {0, "metadata-channel"} end
local phase = redis.call('HGET', KEYS[1], 'phase')
if (phase ~= 'active' and phase ~= 'wait_return') then return {0, "episode-phase"} end
if redis.call('HGET', KEYS[1], 'episode_id') ~= ARGV[7] then return {0, "episode-id"} end
if redis.call('HGET', KEYS[1], 'version') ~= ARGV[8] then return {0, "episode-version"} end
if redis.call('HGET', KEYS[1], 'intent_epoch') ~= ARGV[9] then return {0, "intent-epoch"} end
if redis.call('HGET', KEYS[1], 'channel_uuid') ~= ARGV[10] then return {0, "channel-binding"} end
if redis.call('HGET', KEYS[1], 'configuration_generation') ~= ARGV[1] then return {0, "episode-config"} end
if redis.call('HGET', KEYS[1], 'control_generation') ~= ARGV[2] then return {0, "episode-control"} end
if redis.call('HGET', KEYS[1], 'catalog_generation') ~= ARGV[3] then return {0, "episode-catalog"} end
if redis.call('HGET', KEYS[1], 'recorder_fence') ~= ARGV[4] then return {0, "episode-fence"} end
if redis.call('HGET', KEYS[1], 'lease_value') ~= ARGV[5] then return {0, "episode-lease"} end
if redis.call('GET', KEYS[3]) ~= ARGV[5] then return {0, "live-lease"} end
if redis.call('GET', KEYS[4]) ~= ARGV[6] then return {0, "native-owner"} end
if redis.call('HGET', KEYS[1], 'owner_lock_value') ~= ARGV[6] then return {0, "episode-owner"} end
if redis.call('HGET', KEYS[1], 'worker_id') ~= ARGV[12] then return {0, "episode-worker"} end
if redis.call('HGET', KEYS[1], 'metadata_owner') ~= ARGV[11] then return {0, "episode-metadata-owner"} end
if redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[13] then return {0, "episode-stream"} end
if redis.call('HGET', KEYS[1], 'account_id') ~= ARGV[14] then return {0, "episode-account"} end
if redis.call('HGET', KEYS[1], 'm3u_profile_id') ~= ARGV[15] then return {0, "episode-m3u-profile"} end
if redis.call('HGET', KEYS[1], 'input_profile_id') ~= ARGV[16] then return {0, "episode-input-profile"} end
if redis.call('HGET', KEYS[1], 'client_id') ~= ARGV[17] then return {0, "episode-client"} end
if redis.call('HGET', KEYS[1], 'connected_at') ~= ARGV[18] then return {0, "episode-connected-at"} end
if redis.call('HGET', KEYS[1], 'client_worker_id') ~= ARGV[19] then return {0, "episode-client-worker"} end
if redis.call('HGET', KEYS[2], 'configuration_generation') ~= ARGV[1] then return {0, "authority-config"} end
if redis.call('HGET', KEYS[2], 'control_generation') ~= ARGV[2] then return {0, "authority-control"} end
if redis.call('HGET', KEYS[2], 'catalog_generation') ~= ARGV[3] then return {0, "authority-catalog"} end
if redis.call('HGET', KEYS[6], 'owner') ~= ARGV[11] then return {0, "native-metadata-owner"} end
if redis.call('HGET', KEYS[6], 'stream_id') ~= ARGV[13] then return {0, "native-metadata-stream"} end
if redis.call('HGET', KEYS[6], 'm3u_profile') ~= ARGV[15] then return {0, "native-metadata-m3u-profile"} end
if redis.call('HGET', KEYS[6], 'stream_profile') ~= ARGV[16] then return {0, "native-metadata-input-profile"} end
if (redis.call('HGET', KEYS[6], 'state') ~= 'active'
       and redis.call('HGET', KEYS[6], 'state') ~= 'waiting_for_clients') then return {0, "native-state"} end
if redis.call('EXISTS', KEYS[7]) ~= 0 then return {0, "native-stopping"} end
if redis.call('EXISTS', KEYS[11]) ~= 0 then return {0, "client-stop"} end
if redis.call('HGET', KEYS[8], 'connected_at') ~= ARGV[18] then return {0, "client-connected-at"} end
if redis.call('HGET', KEYS[8], 'worker_id') ~= ARGV[19] then return {0, "client-worker"} end
if redis.call('HGET', KEYS[8], 'user_id') ~= '0' then return {0, "client-user"} end
if redis.call('HGET', KEYS[8], 'output_format') ~= 'mpegts' then return {0, "client-format"} end
if redis.call('HGET', KEYS[8], 'output_profile_id') ~= '' then return {0, "client-output-profile"} end
if redis.call('SISMEMBER', KEYS[9], ARGV[17]) ~= 1 then return {0, "client-membership"} end
if redis.call('HGET', KEYS[10], ARGV[17]) ~= ARGV[7] then return {0, "client-private-marker"} end
if redis.call('TTL', KEYS[1]) <= 0 then return {0, "episode-expired"} end
if redis.call('TTL', KEYS[3]) <= 0 then return {0, "recorder-lease-expired"} end
if redis.call('TTL', KEYS[4]) <= 0 then return {0, "owner-expired"} end
if redis.call('TTL', KEYS[5]) <= 0 then return {0, "worker-heartbeat-expired"} end
if redis.call('TTL', KEYS[8]) <= 0 then return {0, "client-expired"} end
local head = tonumber(redis.call('GET', KEYS[12]) or '')
local newest = redis.call('ZREVRANGE', KEYS[13], 0, 0)
if not head or head < 1 or head % 1 ~= 0 or #newest == 0 then return {0, "buffer-empty"} end
local chunk_key = 'live:channel:' .. ARGV[10] .. ':input:buffer:chunk:' .. newest[1]
if redis.call('TYPE', chunk_key).ok ~= 'string' then return {0, "buffer-chunk-missing"} end
local ordinary = 0
for _, id in ipairs(redis.call('SMEMBERS', KEYS[9])) do
  local client_key = 'live:channel:' .. ARGV[10] .. ':clients:' .. id
  local stop_key = 'live:channel:' .. ARGV[10] .. ':client:' .. id .. ':stop'
  local client_type = redis.call('TYPE', client_key).ok
  local stop_type = redis.call('TYPE', stop_key).ok
  if (client_type ~= 'none' and client_type ~= 'hash')
     or (stop_type ~= 'none' and stop_type ~= 'string') then return {0, "viewer-key-type"} end
  local connected_at = client_type == 'hash' and tonumber(redis.call('HGET', client_key, 'connected_at') or '') or nil
  local worker_id = client_type == 'hash' and redis.call('HGET', client_key, 'worker_id') or nil
  local private = private_client(id, ARGV[10])
  if private == nil then return {0, "viewer-classification"} end
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
   or not last_switch or last_switch < 0 or last_switch ~= last_switch or last_switch == math.huge then return {0, "invalid-timing"} end
local deadline_raw = redis.call('HGET', KEYS[1], 'return_deadline') or ''
local deadline = nil
if deadline_raw ~= '' then
  deadline = tonumber(deadline_raw)
  if not deadline or deadline < 0 or deadline ~= deadline or deadline == math.huge then return {0, "invalid-deadline"} end
end
local next_phase = phase
local changed = false
if phase == 'active' and ordinary == 0 then
  if last_switch > now then last_switch = now end
  deadline = math.max(now + return_seconds, last_switch + minimum_switch)
  next_phase = 'wait_return'
  changed = true
elseif phase == 'wait_return' then
  if deadline == nil or now >= deadline then return {0, "return-deadline-reached"} end
  if ordinary > 0 then
    next_phase = 'active'
    deadline = nil
    changed = true
  end
end
if ordinary == 0 and deadline ~= nil and now >= deadline then return {0, "return-deadline-reached"} end
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


def opening_tombstone_key(channel_uuid: str, episode_id: str) -> str:
    """Permanent, non-authorizing fence for one expired opening attempt."""
    if not isinstance(channel_uuid, str) or not channel_uuid or not isinstance(episode_id, str) or not episode_id:
        raise ValueError("channel_uuid and episode_id are required")
    return f"catchuparr:sharing:opening-tombstone:{channel_uuid}:{episode_id}"


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


def read_native_episode_cleanup_binding(
    redis: Any, episode: BorrowEpisodeSnapshot,
) -> str | None:
    """Read only the exact projection facts needed for nonauthorizing cleanup.

    Opening/closing/closed projections intentionally do not decode as live
    grants. This reader cannot authorize a stream; it only returns the catalog
    fence after matching the durable episode's immutable identity fields.
    """
    if not isinstance(episode, BorrowEpisodeSnapshot):
        return None
    try:
        key = episode_projection_key(episode.channel_uuid)
        if _text(redis.type(key)) != "hash" or int(redis.ttl(key)) <= 0:
            return None
        raw = redis.hgetall(key) or {}
        values = {_text(name): _text(value) for name, value in raw.items()}
        expected = {
            "channel_uuid": episode.channel_uuid,
            "episode_id": episode.episode_id,
            "configuration_generation": episode.configuration_generation,
            "control_generation": str(episode.control_generation),
            "recorder_fence": str(episode.recorder_fence),
            "intent_epoch": str(episode.intent_epoch),
            "lease_digest": episode.lease_digest,
            "worker_id": episode.identity[1],
            "owner_lock_value": episode.identity[2],
            "metadata_owner": episode.identity[3],
            "stream_id": episode.identity[4],
            "account_id": episode.identity[5],
            "m3u_profile_id": episode.identity[6],
            "input_profile_id": episode.identity[7],
        }
        if any(values.get(name) != value for name, value in expected.items()):
            return None
        if values.get("phase") not in {"opening", "closing", "closed"}:
            return None
        catalog = values.get("catalog_generation")
        return catalog if isinstance(catalog, str) and catalog else None
    except Exception:
        return None


def observe_native_episode_for_authority_drain(redis: Any, channel_uuid: str):
    """Distinguish truly absent, exact live, already revoked, and malformed state."""
    from ..sharing_authority_outbox import SharingAuthorityPriorObservation

    if not isinstance(channel_uuid, str) or not channel_uuid:
        return SharingAuthorityPriorObservation("unknown")
    key = episode_projection_key(channel_uuid)
    try:
        key_type = _text(redis.type(key))
        if key_type == "none":
            return SharingAuthorityPriorObservation("absent")
        if key_type != "hash":
            return SharingAuthorityPriorObservation("unknown")
        raw = redis.hgetall(key) or {}
        values = {_text(name): _text(value) for name, value in raw.items()}
        if not values or values.get("channel_uuid") != channel_uuid:
            return SharingAuthorityPriorObservation("unknown")
        phase = values.get("phase")
        if phase in {"active", "wait_return"}:
            grant = read_native_episode_grant(redis, channel_uuid)
            if (grant is None or grant.phase != phase
                    or grant.episode_id != values.get("episode_id")
                    or grant.lease_value != values.get("lease_value")):
                return SharingAuthorityPriorObservation("unknown")
            return SharingAuthorityPriorObservation("exact", grant)
        if phase in {"closing", "closed"}:
            required = (
                "channel_uuid", "episode_id", "configuration_generation", "catalog_generation",
                "worker_id", "owner_lock_value", "metadata_owner", "stream_id",
                "account_id", "m3u_profile_id", "input_profile_id", "client_id",
                "lease_value", "lease_digest",
            )
            if any(not values.get(name) for name in required):
                return SharingAuthorityPriorObservation("unknown")
            if (not re.fullmatch(r"[0-9a-f]{64}", values["lease_digest"])
                    or hashlib.sha256(values["lease_value"].encode("utf-8")).hexdigest()
                    != values["lease_digest"]):
                return SharingAuthorityPriorObservation("unknown")
            for name in ("control_generation", "recorder_fence", "intent_epoch", "version"):
                value = values.get(name, "")
                if not value.isdecimal():
                    return SharingAuthorityPriorObservation("unknown")
            if (int(values["control_generation"]) < 0
                    or int(values["recorder_fence"]) < 1
                    or int(values["intent_epoch"]) < 0
                    or int(values["version"]) < 2):
                return SharingAuthorityPriorObservation("unknown")
            connected_at = values.get("connected_at", "")
            client_worker_id = values.get("client_worker_id", "")
            if phase == "closing" and (not connected_at or not client_worker_id):
                return SharingAuthorityPriorObservation("unknown")
            if bool(connected_at) != bool(client_worker_id):
                return SharingAuthorityPriorObservation("unknown")
            if connected_at:
                try:
                    if not math.isfinite(float(connected_at)):
                        return SharingAuthorityPriorObservation("unknown")
                except (TypeError, ValueError, OverflowError):
                    return SharingAuthorityPriorObservation("unknown")
            return SharingAuthorityPriorObservation("known_revoked")
    except Exception:
        return SharingAuthorityPriorObservation("unknown")
    return SharingAuthorityPriorObservation("unknown")


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


_INVALIDATE_NATIVE_EPISODE_LUA = """
local lease_type = redis.call('TYPE', KEYS[3]).ok
if redis.call('TYPE', KEYS[1]).ok ~= 'hash'
   or redis.call('TYPE', KEYS[2]).ok ~= 'hash'
   or (lease_type ~= 'string' and lease_type ~= 'none') then return 0 end
local version = tonumber(ARGV[9])
if not version or version < 2 or version > 9007199254740990
   or version % 1 ~= 0 or redis.call('TTL', KEYS[2]) <= 0 then return 0 end
if redis.call('HGET', KEYS[1], 'configuration_generation') ~= ARGV[1]
   or redis.call('HGET', KEYS[1], 'control_generation') ~= ARGV[2]
   or redis.call('HGET', KEYS[1], 'catalog_generation') ~= ARGV[3]
   or redis.call('HGET', KEYS[1], 'authority_revision') ~= ARGV[4]
   or redis.call('HGET', KEYS[2], 'episode_id') ~= ARGV[5]
   or redis.call('HGET', KEYS[2], 'configuration_generation') ~= ARGV[11]
   or redis.call('HGET', KEYS[2], 'control_generation') ~= ARGV[12]
   or redis.call('HGET', KEYS[2], 'catalog_generation') ~= ARGV[13]
   or redis.call('HGET', KEYS[2], 'recorder_fence') ~= ARGV[14]
   or redis.call('HGET', KEYS[2], 'intent_epoch') ~= ARGV[15]
   or redis.call('HGET', KEYS[2], 'client_id') ~= ARGV[6]
   or redis.call('HGET', KEYS[2], 'connected_at') ~= ARGV[7]
   or redis.call('HGET', KEYS[2], 'worker_id') ~= ARGV[8]
   or redis.call('HGET', KEYS[2], 'stream_id') ~= ARGV[16]
   or redis.call('HGET', KEYS[2], 'account_id') ~= ARGV[17]
   or redis.call('HGET', KEYS[2], 'm3u_profile_id') ~= ARGV[18]
   or redis.call('HGET', KEYS[2], 'input_profile_id') ~= ARGV[19]
   or redis.call('HGET', KEYS[2], 'lease_digest') ~= ARGV[20]
   or redis.call('HGET', KEYS[2], 'owner_lock_value') ~= ARGV[21]
   or redis.call('HGET', KEYS[2], 'metadata_owner') ~= ARGV[22]
   or redis.call('HGET', KEYS[2], 'client_worker_id') ~= ARGV[23]
   or (ARGV[10] ~= 'active' and ARGV[10] ~= 'wait_return') then return 0 end
local current_version = tonumber(redis.call('HGET', KEYS[2], 'version') or '')
local current_phase = redis.call('HGET', KEYS[2], 'phase')
if not current_version or current_version < version or current_version >= 9007199254740990
   or current_version % 1 ~= 0 then return 0 end
if current_phase ~= 'active' and current_phase ~= 'wait_return' and current_phase ~= 'closing' then
  return 0
end
local target_revision = ARGV[4]
local target_configuration = ARGV[1]
local target_control = ARGV[2]
local target_catalog = ARGV[3]
-- A previous invalidation may have committed while its reply was lost. The
-- durable target stamp makes only this exact committed authority retryable.
if current_phase == 'closing'
   and redis.call('HGET', KEYS[2], 'revocation_authority_revision') == target_revision
   and redis.call('HGET', KEYS[2], 'revocation_configuration_generation') == target_configuration
   and redis.call('HGET', KEYS[2], 'revocation_control_generation') == target_control
   and redis.call('HGET', KEYS[2], 'revocation_catalog_generation') == target_catalog then
  return 1
end
if current_phase ~= 'active' and current_phase ~= 'wait_return'
   or lease_type ~= 'string'
   or redis.call('HGET', KEYS[2], 'lease_value') ~= redis.call('GET', KEYS[3])
   or redis.call('TTL', KEYS[3]) <= 0
   or redis.call('TTL', KEYS[2]) <= 0 then return 0 end
redis.call('HSET', KEYS[2], 'phase', 'closing', 'viewer_left_at', '',
  'return_deadline', '', 'version', tostring(current_version + 1),
  'revocation_authority_revision', target_revision,
  'revocation_configuration_generation', target_configuration,
  'revocation_control_generation', target_control,
  'revocation_catalog_generation', target_catalog)
return 1
"""


def invalidate_native_episode(redis: Any, grant: NativeEpisodeGrant | NativeEpisodeInvalidation, *,
                              configuration_generation: str, control_generation: int,
                              catalog_generation: str, authority_revision: int) -> bool:
    """Fence one exact old borrowed episode after a durable authority change."""
    if isinstance(grant, NativeEpisodeGrant):
        grant = NativeEpisodeInvalidation.from_grant(grant)
    if (not isinstance(grant, NativeEpisodeInvalidation)
            or not isinstance(configuration_generation, str) or not configuration_generation
            or type(control_generation) is not int or control_generation < 0
            or not isinstance(catalog_generation, str) or not catalog_generation
            or type(authority_revision) is not int or authority_revision < 1):
        return False
    channel = grant.channel_uuid
    keys = (
        f"catchuparr:sharing:current:{channel}", episode_projection_key(channel),
        f"catchuparr:recorder:{channel}",
    )
    result = redis.eval(
        _INVALIDATE_NATIVE_EPISODE_LUA, len(keys), *keys,
        configuration_generation, str(control_generation), catalog_generation,
        str(authority_revision), grant.episode_id, grant.client_id, grant.connected_at,
        grant.worker_id, str(grant.version), grant.phase,
        grant.configuration_generation, str(grant.control_generation),
        grant.catalog_generation, str(grant.recorder_fence), str(grant.intent_epoch),
        grant.stream_id, grant.account_id, grant.m3u_profile_id, grant.input_profile_id,
        grant.lease_digest,
        grant.owner_lock_value, grant.metadata_owner, grant.client_worker_id,
    )
    return type(result) is int and result == 1


def project_opening_episode(redis: Any, episode: BorrowEpisodeSnapshot, *, lease_value: str,
                            catalog_generation: str, return_seconds: int,
                            minimum_switch_seconds: int, last_switch_at: float | None,
                            ttl_seconds: int = 120) -> bool:
    """Project an acknowledged coordinator opening for native Lua admission."""
    if (episode.phase != "opening" or not isinstance(lease_value, str) or not lease_value
            or hashlib.sha256(lease_value.encode()).hexdigest() != episode.lease_digest
            or not isinstance(catalog_generation, str) or not catalog_generation
            or not isinstance(episode.client_id, str)
            or not episode.client_id.startswith("catchuparr-client-")):
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
        _PROJECT_EPISODE_LUA, 4,
        current, f"catchuparr:recorder:{episode.channel_uuid}",
        episode_projection_key(episode.channel_uuid),
        opening_tombstone_key(episode.channel_uuid, episode.episode_id),
        episode.configuration_generation, str(episode.control_generation), catalog_generation,
        str(episode.recorder_fence), lease_value, episode.episode_id,
        episode.identity[1], episode.identity[2], episode.identity[3],
        episode.identity[4], episode.identity[5], episode.identity[6], episode.identity[7],
        str(return_seconds), str(minimum_switch_seconds), str(switch_at),
        str(episode.version), str(ttl_seconds), episode.channel_uuid,
        str(episode.intent_epoch), episode.lease_digest, episode.client_id,
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
            or episode.client_id != client_id
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
        opening_tombstone_key(channel, episode.episode_id),
    ]
    try:
        result = redis.eval(
            _REGISTER_BORROWED_CLIENT_LUA, len(keys), *keys,
            episode.configuration_generation, str(episode.control_generation), catalog_generation,
            str(episode.recorder_fence), str(episode.version), lease_value, episode.episode_id,
            identity[2], identity[1], identity[3], identity[4], identity[5], identity[6],
            identity[7], identity[0], client_id, user_agent, client_ip, local_worker_id,
            client_record_ttl, str(episode.intent_epoch),
        )
    except Exception:
        # Redis may have committed the Lua transaction while its reply was
        # lost. Recover only from the full exact projection/hash/set/marker
        # tuple; never retry registration or infer authority from a client ID.
        return _recover_registration_ack(
            redis, episode, client_id=client_id, local_worker_id=local_worker_id,
            user_agent=user_agent, client_ip=client_ip,
            backend_signature=backend_signature,
        )
    if not isinstance(result, (list, tuple)) or len(result) != 3 or result[0] != 1:
        return None
    try:
        connected_at = _text(result[1])
        version = int(_text(result[2]))
        if not math.isfinite(float(connected_at)) or version != episode.version + 1:
            return _recover_registration_ack(
                redis, episode, client_id=client_id, local_worker_id=local_worker_id,
                user_agent=user_agent, client_ip=client_ip,
                backend_signature=backend_signature,
            )
    except (TypeError, ValueError, UnicodeError, OverflowError):
        return _recover_registration_ack(
            redis, episode, client_id=client_id, local_worker_id=local_worker_id,
            user_agent=user_agent, client_ip=client_ip,
            backend_signature=backend_signature,
        )
    return NativeClientRegistrationAck(
        channel_uuid=channel, episode_id=episode.episode_id, client_id=client_id,
        local_worker_id=local_worker_id, redis_backend_signature=backend_signature,
        connected_at=connected_at, projection_version=version,
        user_agent=user_agent, client_ip=client_ip,
    )


def _recover_registration_ack(redis: Any, episode: BorrowEpisodeSnapshot, *,
                              client_id: str, local_worker_id: str, user_agent: str,
                              client_ip: str, backend_signature: str) -> NativeClientRegistrationAck | None:
    """Reconcile a lost Lua reply from exact committed native records."""
    try:
        grant = read_native_episode_grant(redis, episode.channel_uuid)
        if grant is None or not (
            grant.episode_id == episode.episode_id and grant.phase == "active"
            and grant.configuration_generation == episode.configuration_generation
            and grant.control_generation == episode.control_generation
            and grant.recorder_fence == episode.recorder_fence
            and grant.intent_epoch == episode.intent_epoch
            and grant.client_id == client_id and grant.client_worker_id == local_worker_id
            and grant.version == episode.version + 1
        ):
            return None
        client_key = f"live:channel:{episode.channel_uuid}:clients:{client_id}"
        client_raw = redis.hgetall(client_key) or {}
        client = {_text(key): _text(value) for key, value in client_raw.items()}
        private_key = f"catchuparr:sharing:private:{episode.channel_uuid}"
        if (
            client.get("connected_at") != grant.connected_at
            or client.get("worker_id") != local_worker_id
            or client.get("user_agent") != user_agent
            or client.get("ip_address") != client_ip
            or client.get("user_id") != "0"
            or client.get("output_format") != "mpegts"
            or client.get("output_profile_id") != ""
            or _text(redis.hget(private_key, client_id)) != episode.episode_id
            or client_id not in {_text(value) for value in redis.smembers(
                f"live:channel:{episode.channel_uuid}:clients"
            )}
            or int(redis.ttl(client_key)) <= 0
        ):
            return None
        return NativeClientRegistrationAck(
            channel_uuid=episode.channel_uuid, episode_id=episode.episode_id,
            client_id=client_id, local_worker_id=local_worker_id,
            redis_backend_signature=backend_signature, connected_at=grant.connected_at,
            projection_version=grant.version, user_agent=user_agent, client_ip=client_ip,
        )
    except (AttributeError, TypeError, ValueError, UnicodeError, OverflowError):
        return None


_FENCE_UNREGISTERED_OPENING_LUA = """
if redis.call('TYPE', KEYS[1]).ok ~= 'hash' then return 0 end
if redis.call('TTL', KEYS[1]) <= 0 then return 0 end
local version = tonumber(ARGV[2])
if not version or version < 1 or version > 9007199254740990
   or version % 1 ~= 0 then return 0 end
if redis.call('HGET', KEYS[1], 'episode_id') ~= ARGV[1]
   or redis.call('HGET', KEYS[1], 'client_id') ~= ARGV[17]
   or redis.call('HGET', KEYS[1], 'channel_uuid') ~= ARGV[3]
   or redis.call('HGET', KEYS[1], 'configuration_generation') ~= ARGV[4]
   or redis.call('HGET', KEYS[1], 'control_generation') ~= ARGV[5]
   or redis.call('HGET', KEYS[1], 'catalog_generation') ~= ARGV[6]
   or redis.call('HGET', KEYS[1], 'recorder_fence') ~= ARGV[7]
   or redis.call('HGET', KEYS[1], 'intent_epoch') ~= ARGV[8]
   or redis.call('HGET', KEYS[1], 'worker_id') ~= ARGV[9]
   or redis.call('HGET', KEYS[1], 'owner_lock_value') ~= ARGV[10]
   or redis.call('HGET', KEYS[1], 'metadata_owner') ~= ARGV[11]
   or redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[12]
   or redis.call('HGET', KEYS[1], 'account_id') ~= ARGV[13]
   or redis.call('HGET', KEYS[1], 'm3u_profile_id') ~= ARGV[14]
   or redis.call('HGET', KEYS[1], 'input_profile_id') ~= ARGV[15]
   or redis.call('HGET', KEYS[1], 'lease_digest') ~= ARGV[16] then return 0 end
local phase = redis.call('HGET', KEYS[1], 'phase')
if phase == 'closing' then
  local closed_version = tonumber(redis.call('HGET', KEYS[1], 'version') or '')
  if closed_version == version + 1 then return 2 end
  return 0
end
if phase == 'closed' then
  local closed_version = tonumber(redis.call('HGET', KEYS[1], 'version') or '')
  if closed_version == version + 2 then return 2 end
  return 0
end
if phase ~= 'opening' then return 0 end
if redis.call('HGET', KEYS[1], 'version') ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[1], 'phase', 'closing', 'viewer_left_at', '',
  'return_deadline', '', 'version', tostring(version + 1))
return 1
"""


def fence_unregistered_opening(redis: Any, episode: BorrowEpisodeSnapshot, *,
                               catalog_generation: str) -> bool:
    """Fence an exact opening before declaring an unknown client absent.

    Registration Lua compares this projection version/phase atomically. After
    this CAS succeeds, a delayed registration command cannot create a client.
    """
    if (not isinstance(episode, BorrowEpisodeSnapshot) or episode.phase != "opening"
            or not isinstance(episode.client_id, str)
            or not episode.client_id.startswith("catchuparr-client-")
            or not isinstance(catalog_generation, str) or not catalog_generation):
        return False
    try:
        result = redis.eval(
            _FENCE_UNREGISTERED_OPENING_LUA, 1,
            episode_projection_key(episode.channel_uuid), episode.episode_id,
            str(episode.version), episode.channel_uuid, episode.configuration_generation,
            str(episode.control_generation), catalog_generation,
            str(episode.recorder_fence), str(episode.intent_epoch), episode.identity[1],
            episode.identity[2], episode.identity[3], episode.identity[4], episode.identity[5],
            episode.identity[6], episode.identity[7], episode.lease_digest, episode.client_id,
        )
    except Exception:
        return False
    return type(result) is int and result in {1, 2}


def native_registration_is_absent(redis: Any, *, channel_uuid: str, client_id: str) -> bool:
    """Fail-closed exact hash/marker/membership check after fencing an opening."""
    if not isinstance(channel_uuid, str) or not channel_uuid or not isinstance(client_id, str) or not client_id:
        return False
    client_key = f"live:channel:{channel_uuid}:clients:{client_id}"
    clients_key = f"live:channel:{channel_uuid}:clients"
    marker_key = f"catchuparr:sharing:private:{channel_uuid}"
    try:
        def kind(key: str) -> str:
            result = redis.type(key)
            if isinstance(result, bytes):
                result = result.decode("ascii", errors="strict")
            return result

        client_kind, clients_kind, marker_kind = map(kind, (client_key, clients_key, marker_key))
        if client_kind != "none" or clients_kind not in {"none", "set"}:
            return False
        if marker_kind == "hash":
            if redis.hget(marker_key, client_id) is not None:
                return False
        elif marker_kind != "none":
            return False
        return clients_kind == "none" or not redis.sismember(clients_key, client_id)
    except Exception:
        return False


_TOMBSTONE_EXPIRED_OPENING_LUA = """
local allowed = {
  {'hash'}, {'none'}, {'none', 'string'}, {'none', 'hash'},
  {'none', 'set'}, {'none', 'hash'}, {'none', 'string'}
}
for i = 1, #allowed do
  local actual = redis.call('TYPE', KEYS[i]).ok
  local ok = false
  for _, value in ipairs(allowed[i]) do if actual == value then ok = true end end
  if not ok then return 0 end
end
if redis.call('HGET', KEYS[1], 'configuration_generation') ~= ARGV[1]
   or redis.call('HGET', KEYS[1], 'control_generation') ~= ARGV[2]
   or redis.call('HGET', KEYS[1], 'catalog_generation') ~= ARGV[3]
   or redis.call('HGET', KEYS[1], 'authority_revision') ~= ARGV[4]
   or redis.call('EXISTS', KEYS[4]) ~= 0
   or (redis.call('TYPE', KEYS[5]).ok == 'set'
       and redis.call('SISMEMBER', KEYS[5], ARGV[7]) ~= 0)
   or (redis.call('TYPE', KEYS[6]).ok == 'hash'
       and redis.call('HGET', KEYS[6], ARGV[7]) ~= false) then return 0 end
local value = ARGV[8]
local existing = redis.call('GET', KEYS[7])
if existing then
  if existing == value then return 2 end
  return 0
end
-- A permanent exact tombstone is safe to acknowledge after the old lease has
-- been replaced: it cannot reopen authority, and the current authority CAS
-- plus exact resource-absence checks above still apply. A first tombstone,
-- however, must be fenced by the original lease observation.
if redis.call('TYPE', KEYS[3]).ok ~= ARGV[5]
   or (ARGV[5] == 'string' and redis.call('GET', KEYS[3]) ~= ARGV[6]) then
  return 0
end
redis.call('SET', KEYS[7], value)
return 1
"""


def tombstone_expired_unprojected_opening(
    redis: Any, episode: BorrowEpisodeSnapshot, *, client_id: str,
    expected_lease: str | None = None,
    expected_authority: tuple[str, int, str, int] | None = None,
) -> bool:
    """Atomically prove absence and permanently fence a delayed expired opening.

    The tombstone contains only opaque identifiers and the lease digest. It is
    intentionally permanent: delayed project/register calls must remain
    fenced after worker restarts and after the short projection TTL expires.
    """
    if (not isinstance(episode, BorrowEpisodeSnapshot)
            or episode.phase not in {"opening", "cleanup_pending"}
            or not isinstance(client_id, str)
            or not client_id.startswith("catchuparr-client-")
            or episode.client_id != client_id):
        return False
    channel = episode.channel_uuid
    current_key = f"catchuparr:sharing:current:{channel}"
    try:
        current = redis.hgetall(current_key) or {}
        normalized = {_text(key): _text(value) for key, value in current.items()}
        config = normalized["configuration_generation"]
        control = normalized["control_generation"]
        catalog = normalized["catalog_generation"]
        revision = normalized["authority_revision"]
        if (not config or not control.isdigit() or not catalog
                or not revision.isdigit()):
            return False
        lease_key = f"catchuparr:recorder:{channel}"
        lease_kind = redis.type(lease_key)
        if isinstance(lease_kind, bytes):
            lease_kind = lease_kind.decode("ascii", errors="strict")
        if lease_kind not in {"none", "string"}:
            return False
        current_lease = redis.get(lease_key) if lease_kind == "string" else None
        if isinstance(current_lease, bytes):
            current_lease = current_lease.decode("utf-8", errors="strict")
        if current_lease is not None and not isinstance(current_lease, str):
            return False
        if expected_lease is not None and current_lease != expected_lease:
            return False
        if expected_authority is not None and (
            config, control, catalog, revision
        ) != tuple(str(value) for value in expected_authority):
            return False
        keys = (
            current_key, episode_projection_key(channel), lease_key,
            f"live:channel:{channel}:clients:{client_id}",
            f"live:channel:{channel}:clients", f"catchuparr:sharing:private:{channel}",
            opening_tombstone_key(channel, episode.episode_id),
        )
        value = "|".join((channel, episode.episode_id, client_id, episode.lease_digest))
        result = redis.eval(
            _TOMBSTONE_EXPIRED_OPENING_LUA, len(keys), *keys,
            config, control, catalog, revision, lease_kind, current_lease or "",
            client_id, value,
        )
        return type(result) is int and result in {1, 2}
    except Exception:
        return False


_CLOSE_FENCED_OPENING_LUA = """
local episode_type = redis.call('TYPE', KEYS[1]).ok
local client_type = redis.call('TYPE', KEYS[2]).ok
local clients_type = redis.call('TYPE', KEYS[3]).ok
local private_type = redis.call('TYPE', KEYS[4]).ok
if episode_type ~= 'hash' or (client_type ~= 'none' and client_type ~= 'hash')
   or (clients_type ~= 'none' and clients_type ~= 'set')
   or (private_type ~= 'none' and private_type ~= 'hash') then return 0 end
if redis.call('TTL', KEYS[1]) <= 0 then return 0 end
local version = tonumber(ARGV[2])
if not version or version < 1 or version > 9007199254740989
   or version % 1 ~= 0 then return 0 end
if redis.call('HGET', KEYS[1], 'episode_id') ~= ARGV[1]
   or redis.call('HGET', KEYS[1], 'client_id') ~= ARGV[18]
   or redis.call('HGET', KEYS[1], 'channel_uuid') ~= ARGV[3]
   or redis.call('HGET', KEYS[1], 'configuration_generation') ~= ARGV[4]
   or redis.call('HGET', KEYS[1], 'control_generation') ~= ARGV[5]
   or redis.call('HGET', KEYS[1], 'catalog_generation') ~= ARGV[6]
   or redis.call('HGET', KEYS[1], 'recorder_fence') ~= ARGV[7]
   or redis.call('HGET', KEYS[1], 'intent_epoch') ~= ARGV[8]
   or redis.call('HGET', KEYS[1], 'worker_id') ~= ARGV[9]
   or redis.call('HGET', KEYS[1], 'owner_lock_value') ~= ARGV[10]
   or redis.call('HGET', KEYS[1], 'metadata_owner') ~= ARGV[11]
   or redis.call('HGET', KEYS[1], 'stream_id') ~= ARGV[12]
   or redis.call('HGET', KEYS[1], 'account_id') ~= ARGV[13]
   or redis.call('HGET', KEYS[1], 'm3u_profile_id') ~= ARGV[14]
   or redis.call('HGET', KEYS[1], 'input_profile_id') ~= ARGV[15]
   or redis.call('HGET', KEYS[1], 'lease_digest') ~= ARGV[16] then return 0 end
local phase = redis.call('HGET', KEYS[1], 'phase')
local stored_version = tonumber(redis.call('HGET', KEYS[1], 'version') or '')
if client_type ~= 'none' or (clients_type == 'set' and redis.call('SISMEMBER', KEYS[3], ARGV[17]) ~= 0)
   or (private_type == 'hash' and redis.call('HGET', KEYS[4], ARGV[17]) ~= false) then return 0 end
if phase == 'closed' and stored_version == version + 2 then return 2 end
if phase ~= 'closing' or stored_version ~= version + 1 then return 0 end
redis.call('HSET', KEYS[1], 'phase', 'closed', 'version', tostring(version + 2))
return 1
"""


def close_fenced_unregistered_opening(redis: Any, episode: BorrowEpisodeSnapshot, *,
                                      catalog_generation: str, client_id: str) -> bool:
    """Atomically close a fenced opening only when all exact client resources are absent."""
    if (not isinstance(episode, BorrowEpisodeSnapshot) or episode.phase != "opening"
            or not isinstance(catalog_generation, str) or not catalog_generation
            or not isinstance(client_id, str) or not client_id.startswith("catchuparr-client-")
            or episode.client_id != client_id):
        return False
    channel = episode.channel_uuid
    keys = (
        episode_projection_key(channel),
        f"live:channel:{channel}:clients:{client_id}",
        f"live:channel:{channel}:clients",
        f"catchuparr:sharing:private:{channel}",
    )
    try:
        result = redis.eval(
            _CLOSE_FENCED_OPENING_LUA, len(keys), *keys,
            episode.episode_id, str(episode.version), channel,
            episode.configuration_generation, str(episode.control_generation),
            catalog_generation, str(episode.recorder_fence), str(episode.intent_epoch),
            episode.identity[1], episode.identity[2], episode.identity[3],
            episode.identity[4], episode.identity[5], episode.identity[6],
            episode.identity[7], episode.lease_digest, client_id, episode.client_id,
        )
    except Exception:
        return False
    return type(result) is int and result in {1, 2}


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


_CLEANUP_BORROWED_CLIENT_LUA = """
local function kind(key)
  return redis.call('TYPE', key).ok
end
local client_type = kind(KEYS[1])
local clients_type = kind(KEYS[2])
local private_type = kind(KEYS[3])
local disconnect_type = kind(KEYS[4])
local episode_type = kind(KEYS[5])
if (client_type ~= 'none' and client_type ~= 'hash')
   or (clients_type ~= 'none' and clients_type ~= 'set')
   or (private_type ~= 'none' and private_type ~= 'hash')
   or (disconnect_type ~= 'none' and disconnect_type ~= 'string')
   or (episode_type ~= 'none' and episode_type ~= 'hash') then return 0 end
local marked_episode = redis.call('HGET', KEYS[3], ARGV[1])
if marked_episode and marked_episode ~= ARGV[2] then return 0 end
if client_type == 'hash' then
  if redis.call('HGET', KEYS[1], 'connected_at') ~= ARGV[3]
     or redis.call('HGET', KEYS[1], 'worker_id') ~= ARGV[4]
     or redis.call('HGET', KEYS[1], 'user_agent') ~= ARGV[5]
     or redis.call('HGET', KEYS[1], 'ip_address') ~= ARGV[6]
     or redis.call('HGET', KEYS[1], 'user_id') ~= '0'
     or redis.call('HGET', KEYS[1], 'output_format') ~= 'mpegts'
     or redis.call('HGET', KEYS[1], 'output_profile_id') ~= '' then return 0 end
end
local close_episode = episode_type == 'hash'
   and redis.call('HGET', KEYS[5], 'episode_id') == ARGV[2]
   and redis.call('HGET', KEYS[5], 'client_id') == ARGV[1]
   and redis.call('HGET', KEYS[5], 'connected_at') == ARGV[3]
   and redis.call('HGET', KEYS[5], 'client_worker_id') == ARGV[4]
local episode_version = close_episode and tonumber(redis.call('HGET', KEYS[5], 'version') or '') or nil
if close_episode and (not episode_version or episode_version < 2
   or episode_version % 1 ~= 0) then return 0 end
-- All key types and exact registration fields were checked before mutation.
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('DEL', KEYS[1])
if marked_episode == ARGV[2] then redis.call('HDEL', KEYS[3], ARGV[1]) end
if kind(KEYS[3]) == 'hash' and redis.call('HLEN', KEYS[3]) == 0 then redis.call('DEL', KEYS[3]) end
if close_episode then
  redis.call('HSET', KEYS[5], 'phase', 'closed', 'viewer_left_at', '',
    'return_deadline', '', 'version', tostring(episode_version + 1))
end
if redis.call('SCARD', KEYS[2]) == 0 then
  redis.call('SET', KEYS[4], ARGV[7], 'EX', ARGV[8])
end
return 1
"""


def cleanup_borrowed_client(redis: Any, ack: NativeClientRegistrationAck, *,
                            episode_id: str, manager: Any, timestamp: float | None = None,
                            ttl_seconds: int = 60) -> bool:
    """Remove only the exact acknowledged client under its native heartbeat lock.

    The private marker remains until the exact hash/set registration disappears
    in the same Lua operation. No native owner, source, manager map, or unrelated
    client can be stopped or removed by this cleanup.
    """
    if (not isinstance(ack, NativeClientRegistrationAck)
            or not isinstance(episode_id, str) or episode_id != ack.episode_id
            or getattr(manager, "channel_id", None) != ack.channel_uuid
            or getattr(manager, "worker_id", None) != ack.local_worker_id
            or type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 720):
        return False
    now = time.time() if timestamp is None else timestamp
    if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(float(now)):
        return False
    try:
        if redis_backend_signature(redis) != ack.redis_backend_signature:
            return False
        if redis_backend_signature(getattr(manager, "redis_client", None)) != ack.redis_backend_signature:
            return False
    except BorrowReaderUnavailable:
        return False
    lock = getattr(manager, "lock", None)
    clients = getattr(manager, "clients", None)
    registered = getattr(manager, "_registered_clients", None)
    heartbeat_times = getattr(manager, "last_heartbeat_time", None)
    if (lock is None or not hasattr(lock, "__enter__") or not hasattr(lock, "__exit__")
            or not isinstance(clients, set) or not isinstance(registered, set)
            or not isinstance(heartbeat_times, dict)):
        return False
    channel = ack.channel_uuid
    keys = (
        f"live:channel:{channel}:clients:{ack.client_id}",
        f"live:channel:{channel}:clients",
        f"catchuparr:sharing:private:{channel}",
        f"live:channel:{channel}:last_client_disconnect_time",
        episode_projection_key(channel),
    )
    with lock:
        try:
            result = redis.eval(
                _CLEANUP_BORROWED_CLIENT_LUA, len(keys), *keys,
                ack.client_id, episode_id, ack.connected_at, ack.local_worker_id,
                ack.user_agent, ack.client_ip, str(float(now)), ttl_seconds,
            )
        except Exception:
            return False
        if type(result) is not int or result != 1:
            return False
        clients.discard(ack.client_id)
        registered.discard(ack.client_id)
        heartbeat_times.pop(ack.client_id, None)
        if hasattr(manager, "last_active_time"):
            manager.last_active_time = float(now)
    return True


_TOUCH_REJECTION_REASONS = frozenset({
    "episode-phase",
    "episode-id",
    "episode-version",
    "intent-epoch",
    "channel-binding",
    "episode-config",
    "episode-control",
    "episode-catalog",
    "episode-fence",
    "episode-lease",
    "live-lease",
    "native-owner",
    "episode-owner",
    "episode-worker",
    "episode-metadata-owner",
    "episode-stream",
    "episode-account",
    "episode-m3u-profile",
    "episode-input-profile",
    "episode-client",
    "episode-connected-at",
    "episode-client-worker",
    "authority-config",
    "authority-control",
    "authority-catalog",
    "native-metadata-owner",
    "native-metadata-stream",
    "native-metadata-m3u-profile",
    "native-metadata-input-profile",
    "native-state",
    "native-stopping",
    "client-stop",
    "client-connected-at",
    "client-worker",
    "client-user",
    "client-format",
    "client-output-profile",
    "client-membership",
    "client-private-marker",
    "key-type",
    "invalid-ttl",
    "metadata-channel",
    "episode-expired", "recorder-lease-expired", "owner-expired",
    "worker-heartbeat-expired", "client-expired",
    "buffer-empty",
    "buffer-chunk-missing",
    "viewer-key-type",
    "viewer-classification",
    "invalid-timing",
    "invalid-deadline",
    "return-deadline-reached",
})


def check_and_touch_borrowed_client(redis: Any, grant: NativeEpisodeGrant, *,
                                    client_record_ttl: int = 60,
                                    rejection_reasons: list[str] | None = None) -> tuple[bool, float | None, int | None]:
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
        if rejection_reasons is not None:
            try:
                reason = _text(result[1]) if isinstance(result, (list, tuple)) and len(result) == 2 else "unknown"
            except (TypeError, ValueError, UnicodeError):
                reason = "unknown"
            rejection_reasons.append(reason if reason in _TOUCH_REJECTION_REASONS else "unknown")
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
