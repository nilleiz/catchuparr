"""Recover superseded borrow state before admitting another recorder opening."""

from __future__ import annotations

import hashlib
from typing import Callable

from .adapters.sharing_native import (
    episode_projection_key,
    native_registration_is_absent,
    tombstone_expired_unprojected_opening,
)
from .sharing_episode import BorrowEpisodeLedger


def recover_superseded_borrow(
    ledger: BorrowEpisodeLedger, redis, channel_uuid: str, *,
    configuration_generation: str, control_generation: int, recorder_fence: int,
    lease_value: str, current_authority: Callable[[], dict | None],
) -> bool:
    """Close only an older, unprojected episode with no private registration.

    The caller owns the current recorder lease. Authority is revalidated before
    each durable transition. An atomic native tombstone prevents a delayed old
    HTTP registration from returning after the absence proof. No viewer, owner
    or source is stopped, and intent claims are neither deleted nor reused.
    Any missing proof leaves new openings blocked for a later startup retry.
    """
    def authority():
        context = current_authority()
        if not context:
            return None
        value = redis.get(f"catchuparr:recorder:{channel_uuid}")
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="strict")
        if (value != lease_value
                or int(redis.ttl(f"catchuparr:recorder:{channel_uuid}")) <= 0):
            return None
        return (
            configuration_generation, control_generation,
            context["catalog_generation"], context["authority_revision"],
        )

    try:
        if (not isinstance(lease_value, str) or not lease_value
                or type(recorder_fence) is not int or recorder_fence < 1
                or not lease_value.startswith(f"{recorder_fence}:")):
            return False
        old = ledger.read_optional(channel_uuid)
        if old is None or old.phase == "closed":
            return True
        if (old.phase not in {"active", "wait_return", "closing", "cleanup_pending"}
                or old.recorder_fence >= recorder_fence
                or old.control_generation > control_generation
                or old.configuration_generation != configuration_generation
                or old.lease_digest == hashlib.sha256(lease_value.encode()).hexdigest()
                or not old.client_id or not old.client_id.startswith("catchuparr-client-")):
            return False
        expected = authority()
        if (expected is None or redis.exists(episode_projection_key(channel_uuid))
                or not native_registration_is_absent(
                    redis, channel_uuid=channel_uuid, client_id=old.client_id,
                )):
            return False
        pending = old if old.phase == "cleanup_pending" else ledger.revoke(
            old.episode_id, expected_version=old.version, reason="cleanup_pending",
        )
        if (authority() != expected or not tombstone_expired_unprojected_opening(
                redis, pending, client_id=old.client_id,
                expected_lease=lease_value, expected_authority=expected,
        ) or authority() != expected):
            return False
        closed = ledger.revoke(
            pending.episode_id, expected_version=pending.version,
            reason="closed", cleanup_acknowledged=True,
        )
        return closed.episode_id == old.episode_id and closed.phase == "closed"
    except Exception:
        # Corrupt durable bindings, SQL contention, and unavailable native
        # proofs are unresolved cleanup, never proof that an episode is absent.
        return False
