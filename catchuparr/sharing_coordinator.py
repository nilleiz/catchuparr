"""Unwired coordinator boundaries for authority changes and source switches.

These helpers do not install task or HTTP hooks. The recorder owner calls them
only after applied state changes are durable and only with native/catalog
observations loaded outside the TS body hot path.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from .adapters.sharing_native import (
    NativeEpisodeGrant,
    invalidate_native_episode,
    observe_native_connection,
    publish_current_authority,
)
from .engine.recorder import RecorderAttemptResult, RecorderAttemptSwitch
from .sharing_persistence import (
    SharingEffectDispatch,
    SharingEffectReceipt,
    SharingIntentLedger,
    SharingIntentToken,
)
from .sharing_planner import SharingAction


@dataclass(frozen=True)
class AuthorityChange:
    """DB-issued revision to publish after Apply, Stop, or catalog commit."""

    channel_uuid: str
    expected_revision: int
    authority_revision: int
    configuration_generation: str
    control_generation: int
    catalog_generation: str

    def __post_init__(self) -> None:
        if not isinstance(self.channel_uuid, str) or not self.channel_uuid:
            raise ValueError("channel_uuid is required")
        if (type(self.expected_revision) is not int or self.expected_revision < 0
                or type(self.authority_revision) is not int
                or self.authority_revision <= self.expected_revision):
            raise ValueError("authority revisions must increase monotonically")
        if (not isinstance(self.configuration_generation, str)
                or not self.configuration_generation
                or not isinstance(self.catalog_generation, str)
                or not self.catalog_generation):
            raise ValueError("configuration and catalog generations are required")
        if type(self.control_generation) is not int or self.control_generation < 0:
            raise ValueError("control_generation is invalid")


@dataclass(frozen=True)
class AuthorityChangeOutcome:
    current_authority_confirmed: bool
    prior_episode_invalidated: bool


def _matches_published_authority(redis: Any, change: AuthorityChange) -> bool:
    """Recognize only this exact committed revision after a lost Lua reply."""
    try:
        raw = redis.hgetall(f"catchuparr:sharing:current:{change.channel_uuid}") or {}
        values = {
            (key.decode("utf-8") if isinstance(key, bytes) else str(key)):
            (value.decode("utf-8") if isinstance(value, bytes) else str(value))
            for key, value in raw.items()
        }
        return (
            values.get("configuration_generation") == change.configuration_generation
            and values.get("control_generation") == str(change.control_generation)
            and values.get("catalog_generation") == change.catalog_generation
            and values.get("authority_revision") == str(change.authority_revision)
        )
    except Exception:
        return False


def publish_authority_change(
    redis: Any,
    change: AuthorityChange,
    *,
    prior_episode: NativeEpisodeGrant | None,
) -> AuthorityChangeOutcome:
    """Publish a monotonic revocation, then fence the exact prior episode.

    The caller must invoke this only after the database/config/catalog commit.
    A stale publisher cannot replace a newer revision. If the publish reply is
    lost, the current hash is accepted only when all target generations and the
    exact new revision already match. Failed episode fencing is returned as a
    cleanup/retry condition; the new current generation already denies reads.
    """
    if prior_episode is not None and prior_episode.channel_uuid != change.channel_uuid:
        return AuthorityChangeOutcome(False, False)
    try:
        published = publish_current_authority(
            redis, change.channel_uuid,
            configuration_generation=change.configuration_generation,
            control_generation=change.control_generation,
            catalog_generation=change.catalog_generation,
            expected_revision=change.expected_revision,
            authority_revision=change.authority_revision,
        )
    except Exception:
        published = False
    if not published and not _matches_published_authority(redis, change):
        return AuthorityChangeOutcome(False, False)
    if prior_episode is None:
        return AuthorityChangeOutcome(True, True)
    try:
        invalidated = invalidate_native_episode(
            redis, prior_episode,
            configuration_generation=change.configuration_generation,
            control_generation=change.control_generation,
            catalog_generation=change.catalog_generation,
            authority_revision=change.authority_revision,
        )
    except Exception:
        invalidated = False
    return AuthorityChangeOutcome(True, invalidated)


@dataclass(frozen=True)
class SourceSnapshot:
    existing_connection: Any
    allowed_candidates: Any


@dataclass(frozen=True)
class CleanupReconciliation:
    closed_episode_ids: tuple[str, ...]
    pending_episode_ids: tuple[str, ...]


def reconcile_pending_borrow_cleanup(
    ledger: Any,
    *,
    exact_resources_absent: Callable[[Any], bool],
) -> CleanupReconciliation:
    """Close persisted episodes only after a fresh exact native absence ACK.

    Startup must never restore a persisted borrow as read authority. The
    callback must use current Redis/native evidence to remove or confirm the
    exact client, opening nonce, and episode resources. A timeout, malformed
    row, changed version, or missing ACK leaves the episode pending.
    """
    if not callable(getattr(ledger, "pending_cleanup", None)) or not callable(exact_resources_absent):
        raise ValueError("persistent ledger and exact native cleanup proof are required")
    closed: list[str] = []
    pending: list[str] = []
    try:
        episodes = ledger.pending_cleanup()
    except Exception:
        return CleanupReconciliation((), ("ledger-unreadable",))
    for episode in episodes:
        try:
            if exact_resources_absent(episode) is not True:
                pending.append(episode.episode_id)
                continue
            ledger.revoke(
                episode.episode_id,
                expected_version=episode.version,
                reason="closed",
                cleanup_acknowledged=True,
            )
        except Exception:
            pending.append(episode.episode_id)
        else:
            closed.append(episode.episode_id)
    return CleanupReconciliation(tuple(closed), tuple(pending))


def observe_before_candidate_filter(
    redis: Any,
    channel_uuid: str,
    *,
    catalog_generation: str,
    assigned_sources: tuple,
    observed_at: float,
    rank_allowed_candidates: Callable[[], Any],
    recording_allowed_account_ids: frozenset[str] | None = None,
) -> SourceSnapshot:
    """Observe a reusable live source before applying the new-source filter."""
    existing = observe_native_connection(
        redis, channel_uuid,
        catalog_generation=catalog_generation,
        assigned_sources=assigned_sources,
        observed_at=observed_at,
        recording_allowed_account_ids=recording_allowed_account_ids,
    )
    # Deliberately run the filter second. An existing excluded account can be
    # borrowed only as its current connection; the candidate list still limits
    # every newly launched dedicated source.
    allowed = rank_allowed_candidates()
    return SourceSnapshot(existing, allowed)


def request_source_switch(
    signal: RecorderAttemptSwitch,
    token: SharingIntentToken,
    *,
    ledger: SharingIntentLedger,
    fresh_validate: Callable[[SharingIntentToken], bool],
) -> bool:
    """Claim a current return/pause intent before stopping one input attempt.

    The signal is scoped to the FFmpeg attempt. STOP remains a separate
    recorder-owner action and cannot be translated into this switch.
    """
    if (not isinstance(signal, RecorderAttemptSwitch)
            or not isinstance(token, SharingIntentToken)
            or token.action not in {
                SharingAction.RETURN_TO_ALLOWED,
                SharingAction.PAUSE_NO_ALLOWED_SOURCE,
            }
            or not callable(fresh_validate)):
        return False
    try:
        claimed, _ = ledger.run_if_current(
            token,
            fresh_validate=fresh_validate,
            side_effect=lambda _exact_token: signal.request(),
        )
    except Exception:
        return False
    return claimed


def return_to_allowed_after_reap(
    result: RecorderAttemptResult,
    token: SharingIntentToken,
    *,
    switch_intent: SharingIntentToken,
    attempt_switch: RecorderAttemptSwitch,
    ledger: SharingIntentLedger,
    fresh_validate: Callable[[SharingIntentToken], bool],
    exact_cleanup_acknowledged: Callable[[], bool],
    source_identity: tuple[str, ...],
    source_client_id: str,
    candidate: Mapping[str, Any] | None,
    candidate_is_currently_allowed: Callable[[Mapping[str, Any]], bool],
    launch_allowed_candidate: Callable[[SharingIntentToken, Mapping[str, Any], str], SharingEffectReceipt],
) -> tuple[str, SharingEffectDispatch | None]:
    """Launch fallback only after FFmpeg returned and old input cleanup ACKs.

    ``run_candidate`` returns its result after waiting for the FFmpeg process
    to exit. The cleanup callback must confirm the exact previous attempt and
    reservation are gone. Launch uses a single logical intent claim and a
    second fresh validation; no source URL is resolved here. The launcher must
    use the supplied durable operation id as its idempotency and reconciliation
    key, including when its response is lost.
    """
    if (not isinstance(result, RecorderAttemptResult)
            or result.status != "switch_requested"
            or not isinstance(attempt_switch, RecorderAttemptSwitch)
            or result.attempt_switch_id != attempt_switch.attempt_id
            or not isinstance(token, SharingIntentToken)
            or token.action != SharingAction.RETURN_TO_ALLOWED
            or not isinstance(switch_intent, SharingIntentToken)
            or switch_intent.action not in {
                SharingAction.RETURN_TO_ALLOWED,
                SharingAction.PAUSE_NO_ALLOWED_SOURCE,
            }
            or token.intent_epoch <= switch_intent.intent_epoch
            or token.channel_uuid != switch_intent.channel_uuid
            or token.configuration_generation != switch_intent.configuration_generation
            or token.control_generation != switch_intent.control_generation
            or token.recorder_fence != switch_intent.recorder_fence):
        return "not_a_return_switch", None
    try:
        if exact_cleanup_acknowledged() is not True:
            return "cleanup_pending", None
    except Exception:
        return "cleanup_pending", None
    if candidate is None:
        return "no_allowed_candidate", None
    if (not isinstance(source_identity, tuple) or len(source_identity) != 8
            or any(not isinstance(part, str) or not part for part in source_identity)
            or source_identity[0] != token.channel_uuid):
        return "stale", None
    if (not isinstance(source_client_id, str) or not source_client_id
            or len(source_client_id) > 256):
        return "stale", None
    if not isinstance(candidate, Mapping) or not callable(candidate_is_currently_allowed):
        return "stale", None
    target_identity = candidate.get("identity")
    if (not isinstance(target_identity, tuple) or len(target_identity) != 8
            or any(not isinstance(part, str) or not part for part in target_identity)
            or target_identity[0] != token.channel_uuid):
        return "stale", None

    def validate_current(exact_token: SharingIntentToken) -> bool:
        try:
            return (
                fresh_validate(exact_token) is True
                and candidate_is_currently_allowed(candidate) is True
            )
        except Exception:
            return False

    operation_id = uuid.uuid4().hex

    def dispatch(exact_token: SharingIntentToken) -> SharingEffectReceipt:
        ledger.begin_effect_attempt(
            exact_token, operation_id=operation_id, target_identity=target_identity,
            source_attempt_id=attempt_switch.attempt_id,
            source_identity=source_identity,
            source_client_id=source_client_id,
        )
        try:
            receipt = launch_allowed_candidate(exact_token, candidate, operation_id)
            if (not isinstance(receipt, SharingEffectReceipt)
                    or receipt.operation_id != operation_id):
                raise ValueError("candidate launcher did not acknowledge this exact operation")
            ledger.acknowledge_effect_attempt(operation_id, resource_id=receipt.resource_id)
            return receipt
        except Exception:
            try:
                ledger.mark_effect_cleanup_pending(operation_id)
            except Exception:
                # The pre-dispatch row remains dispatching and still blocks
                # another effect until exact startup reconciliation.
                pass
            raise

    try:
        committed, receipt = ledger.run_if_current(
            token,
            fresh_validate=validate_current,
            side_effect=dispatch,
        )
    except Exception:
        try:
            pending = ledger.effect_attempt(operation_id)
            if pending is not None:
                if pending.status != "cleanup_pending":
                    pending = ledger.mark_effect_cleanup_pending(operation_id)
                return "cleanup_pending", SharingEffectDispatch("cleanup_pending", pending)
        except Exception:
            # Dispatch may have been persisted even though its reply was
            # lost. Report unresolved ownership; startup reconciliation scans
            # the durable per-channel operation row.
            return "cleanup_pending", SharingEffectDispatch("cleanup_pending", None)
        return "stale", None
    if not committed:
        return "stale", None
    try:
        operation = ledger.effect_attempt(operation_id)
    except Exception:
        return "cleanup_pending", SharingEffectDispatch("cleanup_pending", None)
    if (not isinstance(receipt, SharingEffectReceipt) or operation is None
            or operation.status != "active" or operation.resource_id != receipt.resource_id):
        if operation is not None:
            try:
                operation = ledger.mark_effect_cleanup_pending(operation_id)
            except Exception:
                pass
            return "cleanup_pending", SharingEffectDispatch("cleanup_pending", operation)
        return "cleanup_pending", None
    return "launched", SharingEffectDispatch("active", operation, receipt)
