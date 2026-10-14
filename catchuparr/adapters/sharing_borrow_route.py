"""Unwired signed open and read-only borrowed stream orchestration.

This module is deliberately not registered in ``views.install_routes``. It
coordinates already-reviewed authority primitives, but performs no source
resolution, native channel initialization, provider reservation, or shared
manager-map mutation.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import math
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator

from ..sharing_episode import BorrowEpisodeLedger, BorrowEpisodeRejected, BorrowEpisodeSnapshot
from .sharing_native import (
    NativeAssignedSource,
    NativeClientRegistrationAck,
    _recover_registration_ack,
    adopt_registered_client_in_manager,
    check_and_touch_borrowed_client,
    cleanup_borrowed_client,
    close_fenced_unregistered_opening,
    fence_unregistered_opening,
    observe_native_connection,
    read_native_episode_grant,
    register_borrowed_client,
)
from .sharing_reader import (
    BorrowBindingLost,
    BorrowReadBinding,
    BorrowReaderUnavailable,
    BorrowReadProof,
    ReadOnlyTsBufferReader,
    make_task_local_input_buffer,
    redis_backend_signature,
)


@dataclass(frozen=True)
class SignedBorrowCapability:
    """Short-lived route grant; the signature is never included in repr."""

    channel_uuid: str
    episode_id: str
    user_id: str
    configuration_generation: str
    catalog_generation: str
    control_generation: int
    recorder_fence: int
    intent_epoch: int
    connection_identity: tuple[str, ...]
    episode_version: int
    expires_at: float
    nonce: str
    signature: str = field(repr=False)

    def __post_init__(self) -> None:
        for name in (
            "channel_uuid", "episode_id", "user_id", "configuration_generation",
            "catalog_generation", "nonce",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be a non-empty string")
        for name in ("control_generation", "recorder_fence", "intent_epoch"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if type(self.episode_version) is not int or self.episode_version < 1:
            raise ValueError("episode_version must be positive")
        if (
            not isinstance(self.connection_identity, tuple)
            or len(self.connection_identity) != 8
            or any(not isinstance(item, str) or not item for item in self.connection_identity)
            or self.connection_identity[0] != self.channel_uuid
        ):
            raise ValueError("connection_identity must be the exact native identity")
        if (
            isinstance(self.expires_at, bool)
            or not isinstance(self.expires_at, (float, int))
            or not math.isfinite(float(self.expires_at))
        ):
            raise ValueError("expires_at must be finite")
        if not isinstance(self.signature, str) or len(self.signature) != 64:
            raise ValueError("signature must be a SHA-256 HMAC")

    def payload(self) -> bytes:
        fields = {
            "channel_uuid": self.channel_uuid,
            "episode_id": self.episode_id,
            "user_id": self.user_id,
            "configuration_generation": self.configuration_generation,
            "catalog_generation": self.catalog_generation,
            "control_generation": self.control_generation,
            "recorder_fence": self.recorder_fence,
            "intent_epoch": self.intent_epoch,
            "connection_identity": self.connection_identity,
            "episode_version": self.episode_version,
            "expires_at": float(self.expires_at),
            "nonce": self.nonce,
        }
        return json.dumps(fields, sort_keys=True, separators=(",", ":")).encode("utf-8")

    def verify(self, secret: bytes, *, user_id: str, now: float) -> bool:
        if not isinstance(secret, bytes) or len(secret) < 32:
            return False
        if not isinstance(user_id, str) or not hmac.compare_digest(self.user_id, user_id):
            return False
        if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(float(now)):
            return False
        if float(now) >= float(self.expires_at):
            return False
        expected = hmac.new(secret, self.payload(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, self.signature)


def _mark_episode_cleanup_pending(
    ledger: BorrowEpisodeLedger | None, episode: BorrowEpisodeSnapshot,
) -> bool:
    """Persist the exact episode as non-authorizing before a failed open escapes."""
    if ledger is None:
        return False
    try:
        current = ledger.read(episode.channel_uuid)
        if current.episode_id != episode.episode_id:
            return False
        if current.phase == "cleanup_pending":
            return True
        if current.phase == "closed":
            return True
        ledger.revoke(
            current.episode_id, expected_version=current.version,
            reason="cleanup_pending", cleanup_acknowledged=False,
        )
        return True
    except Exception:
        return False


def _mark_current_episode_cleanup_pending(
    ledger: BorrowEpisodeLedger | None, episode_id: str, channel_uuid: str,
) -> bool:
    if ledger is None:
        return False
    try:
        current = ledger.read(channel_uuid)
    except Exception:
        return False
    if current.episode_id != episode_id:
        return False
    return _mark_episode_cleanup_pending(ledger, current)


class BorrowOpenCleanupPending(BorrowEpisodeRejected):
    """An exact registration could not be confirmed absent during failed open.

    The manager and acknowledgment remain reachable so a caller can retry the
    exact Redis cleanup. An uncertain registration with no acknowledgment is
    deliberately not cleaned by client ID alone and never stops its manager.
    """

    def __init__(self, message: str, *, ack: NativeClientRegistrationAck | None,
                 manager: Any, owns_manager: bool, cleanup: Callable[..., bool],
                 ledger: BorrowEpisodeLedger, episode: BorrowEpisodeSnapshot,
                 reconcile: Callable[[], NativeClientRegistrationAck | None] | None = None,
                 confirm_absent: Callable[[], bool] | None = None,
                 native_removed: bool = False, manager_stopped: bool = False) -> None:
        super().__init__(message)
        self.ack = ack
        self.manager = manager
        self.owns_manager = owns_manager
        self._cleanup = cleanup
        self._ledger = ledger
        self._episode = episode
        self._reconcile = reconcile
        self._confirm_absent = confirm_absent
        self._native_removed = native_removed
        self._manager_stopped = manager_stopped
        self._ledger_closed = False
        self._completed = False
        self.cleanup_pending_persisted = _mark_episode_cleanup_pending(ledger, episode)

    def retry_cleanup(self) -> bool:
        """Retry only a known exact registration; retain ownership on failure."""
        if self._completed:
            return True
        if self.ack is None and self._reconcile is not None:
            try:
                self.ack = self._reconcile()
            except Exception:
                return False
        if self.ack is None:
            if self._confirm_absent is None:
                return False
            try:
                self._native_removed = self._confirm_absent()
            except Exception:
                return False
            if not self._native_removed:
                return False
        if not self._native_removed:
            try:
                removed = self._cleanup(self.ack, self.manager, self.owns_manager)
            except Exception:
                return False
            if not removed:
                return False
            self._native_removed = True
        if self.owns_manager and not self._manager_stopped:
            stop = getattr(self.manager, "stop", None)
            if not callable(stop):
                return False
            try:
                stop()
            except Exception:
                return False
            self._manager_stopped = True
        if not self._ledger_closed:
            if not BorrowedReaderSession._close_episode_ledger(
                self._ledger, self._episode.episode_id, self._episode.channel_uuid,
            ):
                self.cleanup_pending_persisted = _mark_episode_cleanup_pending(
                    self._ledger, self._episode,
                )
                return False
            self._ledger_closed = True
        self.ack = None
        self._completed = True
        return True


def sign_borrow_capability(
    *, secret: bytes, channel_uuid: str, episode_id: str, user_id: str,
    configuration_generation: str, catalog_generation: str, control_generation: int,
    recorder_fence: int, intent_epoch: int, connection_identity: tuple[str, ...],
    episode_version: int, expires_at: float,
    nonce: str,
) -> SignedBorrowCapability:
    """Create a capability after the coordinator has admitted the route user."""
    unsigned = SignedBorrowCapability(
        channel_uuid, episode_id, user_id, configuration_generation, catalog_generation,
        control_generation, recorder_fence, intent_epoch, connection_identity,
        episode_version, expires_at, nonce, "0" * 64,
    )
    if not isinstance(secret, bytes) or len(secret) < 32:
        raise ValueError("capability secret must contain at least 32 bytes")
    signature = hmac.new(secret, unsigned.payload(), hashlib.sha256).hexdigest()
    return SignedBorrowCapability(
        unsigned.channel_uuid, unsigned.episode_id, unsigned.user_id,
        unsigned.configuration_generation, unsigned.catalog_generation, unsigned.control_generation,
        unsigned.recorder_fence, unsigned.intent_epoch, unsigned.connection_identity,
        unsigned.episode_version, unsigned.expires_at, unsigned.nonce, signature,
    )


_CAPABILITY_FIELDS = frozenset({
    "channel_uuid", "episode_id", "user_id", "configuration_generation",
    "catalog_generation", "control_generation", "recorder_fence", "intent_epoch",
    "connection_identity", "episode_version", "expires_at", "nonce",
})


def encode_borrow_capability(capability: SignedBorrowCapability) -> str:
    """Encode a short-lived capability for the private recorder URL."""
    if not isinstance(capability, SignedBorrowCapability):
        raise ValueError("signed borrow capability is required")
    envelope = json.loads(capability.payload().decode("utf-8"))
    envelope["signature"] = capability.signature
    payload = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_borrow_capability(token: str) -> SignedBorrowCapability:
    """Decode a bounded capability without accepting duplicate or extra fields."""
    if not isinstance(token, str) or not 1 <= len(token) <= 4096:
        raise ValueError("borrow capability token is invalid")
    if re.fullmatch(r"[A-Za-z0-9_-]+", token) is None:
        raise ValueError("borrow capability token is invalid")
    try:
        padded = token + "=" * (-len(token) % 4)
        raw = base64.b64decode(padded, altchars=b"-_", validate=True)

        def reject_duplicates(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("duplicate capability field")
                result[key] = value
            return result

        envelope = json.loads(raw.decode("utf-8"), object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError, binascii.Error, ValueError):
        raise ValueError("borrow capability token is invalid") from None
    if not isinstance(envelope, dict) or set(envelope) != _CAPABILITY_FIELDS | {"signature"}:
        raise ValueError("borrow capability fields are invalid")
    identity = envelope.get("connection_identity")
    if not isinstance(identity, list):
        raise ValueError("borrow capability identity is invalid")
    try:
        capability = SignedBorrowCapability(
            channel_uuid=envelope["channel_uuid"],
            episode_id=envelope["episode_id"],
            user_id=envelope["user_id"],
            configuration_generation=envelope["configuration_generation"],
            catalog_generation=envelope["catalog_generation"],
            control_generation=envelope["control_generation"],
            recorder_fence=envelope["recorder_fence"],
            intent_epoch=envelope["intent_epoch"],
            connection_identity=tuple(identity),
            episode_version=envelope["episode_version"],
            expires_at=envelope["expires_at"],
            nonce=envelope["nonce"],
            signature=envelope["signature"],
        )
    except (TypeError, ValueError, KeyError):
        raise ValueError("borrow capability fields are invalid") from None
    return capability


_CONSUME_BORROW_CAPABILITY_LUA = """
local function kind(key)
  return redis.call('TYPE', key).ok
end
if kind(KEYS[1]) ~= 'hash' or kind(KEYS[2]) ~= 'hash'
   or kind(KEYS[3]) ~= 'string' or kind(KEYS[4]) ~= 'none'
   or (kind(KEYS[5]) ~= 'none' and kind(KEYS[5]) ~= 'string')
   or (kind(KEYS[6]) ~= 'none' and kind(KEYS[6]) ~= 'hash')
   or (kind(KEYS[7]) ~= 'none' and kind(KEYS[7]) ~= 'string') then return 0 end
local ttl = tonumber(ARGV[1])
local expires = tonumber(ARGV[2])
local version = tonumber(ARGV[3])
if not ttl or ttl < 1 or ttl > 120 or ttl % 1 ~= 0
   or not expires or expires ~= expires or expires == math.huge
   or not version or version < 1 or version % 1 ~= 0 then return 0 end
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) + tonumber(now_parts[2]) / 1000000
if expires <= now or expires > now + 120 then return 0 end
if redis.call('HGET', KEYS[1], 'configuration_generation') ~= ARGV[4]
   or redis.call('HGET', KEYS[1], 'control_generation') ~= ARGV[5]
   or redis.call('HGET', KEYS[1], 'catalog_generation') ~= ARGV[6]
   or redis.call('HGET', KEYS[2], 'phase') ~= 'opening'
   or redis.call('HGET', KEYS[2], 'channel_uuid') ~= ARGV[7]
   or redis.call('HGET', KEYS[2], 'episode_id') ~= ARGV[8]
   or redis.call('HGET', KEYS[2], 'configuration_generation') ~= ARGV[4]
   or redis.call('HGET', KEYS[2], 'control_generation') ~= ARGV[5]
   or redis.call('HGET', KEYS[2], 'catalog_generation') ~= ARGV[6]
   or redis.call('HGET', KEYS[2], 'recorder_fence') ~= ARGV[9]
   or redis.call('HGET', KEYS[2], 'intent_epoch') ~= ARGV[10]
   or redis.call('HGET', KEYS[2], 'version') ~= ARGV[3]
   or redis.call('HGET', KEYS[2], 'worker_id') ~= ARGV[11]
   or redis.call('HGET', KEYS[2], 'owner_lock_value') ~= ARGV[12]
   or redis.call('HGET', KEYS[2], 'metadata_owner') ~= ARGV[13]
   or redis.call('HGET', KEYS[2], 'stream_id') ~= ARGV[14]
   or redis.call('HGET', KEYS[2], 'account_id') ~= ARGV[15]
   or redis.call('HGET', KEYS[2], 'm3u_profile_id') ~= ARGV[16]
   or redis.call('HGET', KEYS[2], 'input_profile_id') ~= ARGV[17]
   or redis.call('HGET', KEYS[2], 'lease_value') ~= redis.call('GET', KEYS[3])
   or redis.call('TTL', KEYS[2]) <= 0 or redis.call('TTL', KEYS[3]) <= 0
   or redis.call('EXISTS', KEYS[7]) ~= 0 then return 0 end
if redis.call('GET', KEYS[5]) ~= ARGV[12]
   or redis.call('HGET', KEYS[6], 'owner') ~= ARGV[13]
   or redis.call('HGET', KEYS[6], 'stream_id') ~= ARGV[14]
   or redis.call('HGET', KEYS[6], 'm3u_profile') ~= ARGV[16]
   or redis.call('HGET', KEYS[6], 'stream_profile') ~= ARGV[17]
   or redis.call('TTL', KEYS[5]) <= 0 then return 0 end
local marker = ARGV[8] .. '|' .. ARGV[18]
if redis.call('SET', KEYS[4], marker, 'NX', 'EX', ttl) then return 1 end
return 0
"""


def consume_opening_capability(redis: Any, capability: SignedBorrowCapability,
                               episode: BorrowEpisodeSnapshot, *, catalog_generation: str,
                               now: float, secret: bytes, user_id: str) -> bool:
    """Atomically consume a signed nonce against the current opening projection."""
    if (
        not isinstance(capability, SignedBorrowCapability)
        or not isinstance(episode, BorrowEpisodeSnapshot)
        or episode.phase != "opening"
        or capability.channel_uuid != episode.channel_uuid
        or capability.episode_id != episode.episode_id
        or capability.configuration_generation != episode.configuration_generation
        or capability.catalog_generation != catalog_generation
        or capability.control_generation != episode.control_generation
        or capability.recorder_fence != episode.recorder_fence
        or capability.intent_epoch != episode.intent_epoch
        or capability.connection_identity != episode.identity
        or capability.episode_version != episode.version
        or not capability.verify(secret, user_id=user_id, now=now)
        or isinstance(now, bool) or not isinstance(now, (int, float))
        or not math.isfinite(float(now)) or float(now) >= float(capability.expires_at)
    ):
        return False
    ttl = min(120, max(1, int(math.ceil(float(capability.expires_at) - float(now)))))
    channel = episode.channel_uuid
    nonce_key = hashlib.sha256(capability.nonce.encode("utf-8")).hexdigest()
    marker_digest = hashlib.sha256(capability.signature.encode("ascii")).hexdigest()
    keys = (
        f"catchuparr:sharing:current:{channel}",
        f"catchuparr:sharing:episode:{channel}",
        f"catchuparr:recorder:{channel}",
        f"catchuparr:sharing:consumed:{channel}:{nonce_key}",
        f"live:channel:{channel}:owner",
        f"live:channel:{channel}:metadata",
        f"live:channel:{channel}:stopping",
    )
    identity = episode.identity
    try:
        result = redis.eval(
            _CONSUME_BORROW_CAPABILITY_LUA, len(keys), *keys,
            str(ttl), str(float(capability.expires_at)), str(episode.version),
            episode.configuration_generation, str(episode.control_generation),
            catalog_generation, channel, episode.episode_id,
            str(episode.recorder_fence), str(episode.intent_epoch), identity[1],
            identity[2], identity[3], identity[4], identity[5], identity[6], identity[7],
            marker_digest,
        )
    except Exception:
        return False
    return type(result) is int and result == 1


def native_borrow_proof_reader(
    *,
    redis: Any,
    episode: BorrowEpisodeSnapshot,
    ack: NativeClientRegistrationAck,
    assigned_sources: tuple[NativeAssignedSource, ...],
    catalog_generation: str,
    clock: Callable[[], float] = time.time,
    client_record_ttl: int = 60,
) -> Callable[[], BorrowReadProof]:
    """Build a no-ORM per-read proof from current Redis grant and native CAS."""
    if (
        not isinstance(episode, BorrowEpisodeSnapshot)
        or not isinstance(ack, NativeClientRegistrationAck)
        or not isinstance(assigned_sources, tuple)
        or not assigned_sources
        or any(not isinstance(source, NativeAssignedSource) for source in assigned_sources)
        or not isinstance(catalog_generation, str) or not catalog_generation
        or not callable(clock)
    ):
        raise BorrowReaderUnavailable("native borrowed proof inputs are incomplete")

    def proof() -> BorrowReadProof:
        checked_at = clock()
        if (
            isinstance(checked_at, bool)
            or not isinstance(checked_at, (float, int))
            or not math.isfinite(float(checked_at))
        ):
            raise BorrowBindingLost("native proof clock is invalid")
        grant = read_native_episode_grant(redis, episode.channel_uuid)
        if grant is None or not (
            grant.episode_id == episode.episode_id
            and grant.phase in {"active", "wait_return"}
            and grant.catalog_generation == catalog_generation
            and grant.configuration_generation == episode.configuration_generation
            and grant.control_generation == episode.control_generation
            and grant.recorder_fence == episode.recorder_fence
            and grant.intent_epoch == episode.intent_epoch
            and hashlib.sha256(grant.lease_value.encode()).hexdigest() == episode.lease_digest
            and grant.client_id == ack.client_id
            and grant.client_worker_id == ack.local_worker_id
            and grant.connected_at == ack.connected_at
            and grant.channel_uuid == ack.channel_uuid
        ):
            raise BorrowBindingLost("active Redis episode no longer matches this reader")

        current, deadline, version = check_and_touch_borrowed_client(
            redis, grant, client_record_ttl=client_record_ttl,
        )
        if not current or version is None:
            raise BorrowBindingLost("native client grant was revoked")
        current_grant = read_native_episode_grant(redis, episode.channel_uuid)
        if current_grant is None or current_grant.version != version or not (
            current_grant.episode_id == episode.episode_id
            and current_grant.configuration_generation == episode.configuration_generation
            and current_grant.control_generation == episode.control_generation
            and current_grant.catalog_generation == catalog_generation
            and current_grant.recorder_fence == episode.recorder_fence
            and current_grant.intent_epoch == episode.intent_epoch
            and hashlib.sha256(current_grant.lease_value.encode()).hexdigest() == episode.lease_digest
            and current_grant.client_id == ack.client_id
            and current_grant.client_worker_id == ack.local_worker_id
            and current_grant.connected_at == ack.connected_at
        ):
            raise BorrowBindingLost("native episode changed during read authorization")
        observed = observe_native_connection(
            redis, episode.channel_uuid,
            catalog_generation=current_grant.catalog_generation,
            assigned_sources=assigned_sources, observed_at=float(checked_at),
        )
        if observed is None or observed.target.connection_identity != episode.identity:
            raise BorrowBindingLost("native owner, source, or stream profile changed")
        target = observed.target
        return BorrowReadProof(
            connection_identity=target.connection_identity,
            configuration_generation=current_grant.configuration_generation,
            control_generation=current_grant.control_generation,
            recorder_fence=current_grant.recorder_fence,
            ordinary_viewer_ids=target.ordinary_client_ids,
            checked_at=float(checked_at),
            owner_matches=(target.owner_lock_value == current_grant.owner_lock_value),
            owner_heartbeat_healthy=target.owner_heartbeat_healthy,
            native_active=target.native_state_healthy,
            assigned_source_matches=target.assigned_stream_matches,
            account_active=target.account_active,
            profile_active=target.profile_active,
            media_compatible=target.media_compatible,
            useful_buffer_available=target.useful_buffer_available,
            supported_worker_kind=not target.redirect_worker and not target.vod_or_preview_worker,
            borrowed_client_current=True,
            pending_return_deadline=deadline,
        )

    return proof


class BorrowedReaderSession:
    """One exact borrowed client and its private, read-only TS cursor."""

    def __init__(
        self,
        *,
        reader: ReadOnlyTsBufferReader,
        ack: NativeClientRegistrationAck,
        client_manager: Any,
        owns_client_manager: bool,
        cleanup: Callable[[NativeClientRegistrationAck, Any, bool], bool],
        episode_ledger: BorrowEpisodeLedger | None = None,
        cleanup_clock: Callable[[], float] = time.time,
    ) -> None:
        if not isinstance(ack, NativeClientRegistrationAck):
            raise ValueError("exact native registration acknowledgment is required")
        if type(owns_client_manager) is not bool or not callable(cleanup):
            raise ValueError("owned manager state and exact cleanup are required")
        self.reader = reader
        self.ack = ack
        self.client_manager = client_manager
        self.owns_client_manager = owns_client_manager
        self.cleanup_callback = cleanup
        self.episode_ledger = episode_ledger
        self.cleanup_clock = cleanup_clock
        self._closed = False
        self._native_removed = False
        self._manager_stopped = False
        self._close_lock = threading.Lock()

    @classmethod
    def open(
        cls,
        *,
        capability: SignedBorrowCapability,
        capability_secret: bytes,
        authenticated_user_id: str,
        now: float,
        episode: BorrowEpisodeSnapshot,
        lease_value: str,
        catalog_generation: str,
        ordinary_viewer_ids: tuple[str, ...],
        redis_text: Any,
        redis_binary: Any,
        existing_manager: Any,
        manager_type: Any,
        worker_id: str,
        buffer_type: Any,
        assigned_sources: tuple[NativeAssignedSource, ...],
        client_id: str,
        client_ip: str,
        user_agent: str,
        ledger: BorrowEpisodeLedger,
        clock: Callable[[], float] = time.time,
        client_record_ttl: int = 60,
    ) -> "BorrowedReaderSession":
        """Consume a signed opening and install a local read-only client.

        ``ledger.begin_open`` and capability issuance occur before this call.
        The Redis nonce consume is atomic against the opening projection and
        current authority, so route replay is rejected before constructing a
        manager or registering a native client.
        """
        if not capability.verify(
            capability_secret, user_id=authenticated_user_id, now=now,
        ):
            raise BorrowEpisodeRejected("signed route capability is invalid or expired")
        if (
            episode.phase != "opening"
            or capability.channel_uuid != episode.channel_uuid
            or capability.episode_id != episode.episode_id
            or capability.configuration_generation != episode.configuration_generation
            or capability.catalog_generation != catalog_generation
            or capability.control_generation != episode.control_generation
            or capability.recorder_fence != episode.recorder_fence
            or capability.intent_epoch != episode.intent_epoch
            or capability.episode_version != episode.version
            or capability.connection_identity != episode.identity
            or episode.client_id != client_id
            or not ordinary_viewer_ids
        ):
            raise BorrowEpisodeRejected("opening snapshot has no matching live viewer authority")
        try:
            text_backend = redis_backend_signature(redis_text)
            binary_backend = redis_backend_signature(redis_binary)
        except BorrowReaderUnavailable:
            raise
        if text_backend != binary_backend:
            raise BorrowReaderUnavailable("native metadata and TS buffer use different Redis backends")
        if (not isinstance(assigned_sources, tuple) or not assigned_sources
                or any(not isinstance(source, NativeAssignedSource) for source in assigned_sources)):
            raise BorrowReaderUnavailable("preloaded assigned-source catalog is required")

        # Resolve and validate the task-local manager before consuming the
        # capability. Its backend must share the exact authority/reader backend
        # including TLS and authentication configuration.
        from ..sharing_episode import select_task_local_client_manager

        manager, owns_manager = select_task_local_client_manager(
            existing_manager, manager_type, channel_uuid=episode.channel_uuid,
            redis_client=redis_text, worker_id=worker_id,
        )
        try:
            manager_backend = redis_backend_signature(manager.redis_client)
        except (AttributeError, BorrowReaderUnavailable):
            if owns_manager:
                manager.stop()
            raise BorrowReaderUnavailable("task-local manager Redis backend is unavailable")
        if manager_backend != text_backend or getattr(manager, "worker_id", None) != worker_id:
            if owns_manager:
                manager.stop()
            raise BorrowReaderUnavailable("task-local manager backend or worker does not match")
        if not consume_opening_capability(
            redis_text, capability, episode, catalog_generation=catalog_generation, now=now,
            secret=capability_secret, user_id=authenticated_user_id,
        ):
            if owns_manager:
                manager.stop()
            raise BorrowEpisodeRejected("signed route capability was already consumed")

        # The viewer proof and source catalog are supplied from a fresh
        # coordinator snapshot before any native registration side effect.
        ack = None
        registration_attempted = False
        try:
            buffer = make_task_local_input_buffer(buffer_type, episode.channel_uuid, redis_binary)
            ledger.prepare_registration(
                episode, local_worker_id=worker_id,
                backend_signature=text_backend, user_agent=user_agent,
                client_ip=client_ip,
            )
            registration_attempted = True
            ack = register_borrowed_client(
                redis_text, episode, lease_value=lease_value,
                catalog_generation=catalog_generation, client_id=client_id,
                local_worker_id=worker_id, user_agent=user_agent, client_ip=client_ip,
                client_record_ttl=client_record_ttl,
            )
            if ack is None:
                raise BorrowEpisodeRejected("native client registration was not acknowledged locally")
            ledger.record_registration_ack(episode.episode_id, ack)
            if ack.redis_backend_signature != text_backend:
                raise BorrowReaderUnavailable("registration ACK belongs to another Redis backend")
            if not adopt_registered_client_in_manager(manager, ack):
                raise BorrowEpisodeRejected("native client registration was not acknowledged locally")
            active = ledger.activate(
                episode.episode_id, expected_version=episode.version,
                client_id=ack.client_id, connected_at=ack.connected_at,
                ordinary_viewer_ids=ordinary_viewer_ids,
            )
            binding = BorrowReadBinding(
                episode.identity, active.configuration_generation,
                active.control_generation, active.recorder_fence,
                pending_return_deadline=None, dynamic_grace=True,
            )
            proof_reader = native_borrow_proof_reader(
                redis=redis_text, episode=active, ack=ack,
                assigned_sources=assigned_sources, catalog_generation=catalog_generation,
                clock=clock,
                client_record_ttl=client_record_ttl,
            )
            reader = ReadOnlyTsBufferReader(buffer, binding, proof_reader)
            return cls(
                reader=reader, ack=ack, client_manager=manager,
                owns_client_manager=owns_manager,
                episode_ledger=ledger,
                cleanup=lambda exact_ack, exact_manager, _owned: cleanup_borrowed_client(
                    redis_text, exact_ack, episode_id=exact_ack.episode_id,
                    manager=exact_manager,
                ),
            )
        except Exception as exc:
            def cleanup(exact_ack: NativeClientRegistrationAck, exact_manager: Any,
                        _owned: bool) -> bool:
                return cleanup_borrowed_client(
                    redis_text, exact_ack, episode_id=exact_ack.episode_id,
                    manager=exact_manager,
                )
            if ack is not None:
                try:
                    removed = cleanup(ack, manager, owns_manager)
                except Exception:
                    removed = False
                if removed:
                    manager_stopped = False
                    if owns_manager:
                        stop = getattr(manager, "stop", None)
                        if not callable(stop):
                            raise BorrowOpenCleanupPending(
                                "native client is absent but owned manager shutdown is pending",
                                ack=ack, manager=manager, owns_manager=owns_manager,
                                cleanup=cleanup, ledger=ledger, episode=episode,
                                native_removed=True,
                            ) from exc
                        try:
                            stop()
                            manager_stopped = True
                        except Exception as stop_error:
                            raise BorrowOpenCleanupPending(
                                "native client is absent but owned manager shutdown is pending",
                                ack=ack, manager=manager, owns_manager=owns_manager,
                                cleanup=cleanup, ledger=ledger, episode=episode,
                                native_removed=True,
                            ) from stop_error
                    if not cls._close_episode_ledger(
                        ledger, episode.episode_id, episode.channel_uuid,
                    ):
                        raise BorrowOpenCleanupPending(
                            "native client and manager are closed but episode recovery state is pending",
                            ack=ack, manager=manager, owns_manager=owns_manager,
                            cleanup=cleanup, ledger=ledger, episode=episode,
                            native_removed=True, manager_stopped=manager_stopped,
                        ) from exc
                else:
                    raise BorrowOpenCleanupPending(
                        "borrowed registration cleanup is pending; retain its exact handle",
                        ack=ack, manager=manager, owns_manager=owns_manager,
                        cleanup=cleanup, ledger=ledger, episode=episode,
                    ) from exc
            elif registration_attempted:
                # A transport failure may have happened after Redis committed
                # registration. Without an exact ACK, keep the manager and
                # require exact ACK recovery or a fenced opening plus verified
                # absence before cleanup.
                raise BorrowOpenCleanupPending(
                    "registration outcome is uncertain; manager retained for reconciliation",
                    ack=None, manager=manager, owns_manager=owns_manager,
                    cleanup=cleanup, ledger=ledger, episode=episode,
                    reconcile=lambda: _recover_registration_ack(
                        redis_text, episode, client_id=client_id,
                        local_worker_id=worker_id, user_agent=user_agent, client_ip=client_ip,
                        backend_signature=text_backend,
                    ),
                    confirm_absent=lambda: (
                        fence_unregistered_opening(
                            redis_text, episode, catalog_generation=catalog_generation,
                        )
                        and close_fenced_unregistered_opening(
                            redis_text, episode, catalog_generation=catalog_generation,
                            client_id=client_id,
                        )
                    ),
                ) from exc
            elif owns_manager:
                stop = getattr(manager, "stop", None)
                if callable(stop):
                    try:
                        stop()
                    except Exception as stop_error:
                        raise BorrowOpenCleanupPending(
                            "owned manager shutdown is pending after an empty open",
                            ack=None, manager=manager, owns_manager=True,
                            cleanup=cleanup, ledger=ledger, episode=episode,
                            confirm_absent=lambda: True,
                        ) from stop_error
            raise

    def iter_chunks(self, stop: threading.Event | None = None) -> Iterator[bytes]:
        try:
            yield from self.reader.iter_chunks(stop)
        finally:
            self.close()

    @staticmethod
    def _close_episode_ledger(
        ledger: BorrowEpisodeLedger | None, episode_id: str, channel_uuid: str,
    ) -> bool:
        if ledger is None:
            return True
        try:
            current = ledger.read(channel_uuid)
            if current.episode_id != episode_id:
                return True
            if current.phase != "closed":
                ledger.revoke(
                    episode_id, expected_version=current.version, reason="closed",
                    cleanup_acknowledged=True,
                )
            return True
        except Exception:
            return False

    def close(self) -> bool:
        """Clean only the exact registration; retain owned manager on no ACK."""
        with self._close_lock:
            if self._closed:
                return True
            if not self._native_removed:
                try:
                    self._native_removed = self.cleanup_callback(
                        self.ack, self.client_manager, self.owns_client_manager,
                    ) is True
                except Exception:
                    self._native_removed = False
                if not self._native_removed:
                    _mark_current_episode_cleanup_pending(
                        self.episode_ledger, self.ack.episode_id, self.ack.channel_uuid,
                    )
                    return False
            if self.owns_client_manager and not self._manager_stopped:
                stop = getattr(self.client_manager, "stop", None)
                if not callable(stop):
                    _mark_current_episode_cleanup_pending(
                        self.episode_ledger, self.ack.episode_id, self.ack.channel_uuid,
                    )
                    return False
                try:
                    stop()
                except Exception:
                    _mark_current_episode_cleanup_pending(
                        self.episode_ledger, self.ack.episode_id, self.ack.channel_uuid,
                    )
                    return False
                self._manager_stopped = True
            if not self._close_episode_ledger(
                self.episode_ledger, self.ack.episode_id, self.ack.channel_uuid,
            ):
                _mark_current_episode_cleanup_pending(
                    self.episode_ledger, self.ack.episode_id, self.ack.channel_uuid,
                )
                return False
            self._closed = True
            return True

    def retry_cleanup(self) -> bool:
        """Retry an exact close retained after a streaming response ended."""
        return self.close()
