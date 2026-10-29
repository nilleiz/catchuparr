"""Server-side admission for the internal read-only native sharing route.

The capability is a one-use opening claim. Every request rebuilds the small
ORM catalog once, checks the current applied/control generation and recorder
lease, then hands media reads to the native Redis-only reader session. No ORM
or SQLite operation runs in the chunk iterator.
"""

from __future__ import annotations

import hashlib
import json
import math
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .adapters.sharing_borrow_route import (
    BorrowedReaderSession,
    SignedBorrowCapability,
    sign_borrow_capability,
)
from .adapters.sharing_native import (
    NativeAssignedSource,
    close_fenced_unregistered_opening,
    fence_unregistered_opening,
    native_registration_is_absent,
    observe_native_connection,
    project_opening_episode,
)
from .sharing_episode import BorrowEpisodeLedger, BorrowEpisodeRejected
from .sharing_persistence import SharingIntentLedger, SharingIntentToken
from .sharing_planner import (
    BorrowTarget,
    SharingAction,
    SharingObservation,
    SharingPhase,
    SharingSettings,
    observe,
)


@dataclass(frozen=True)
class PreparedBorrowOpening:
    """A durably claimed and Redis-projected one-use reader opening."""

    episode_id: str
    episode_version: int
    client_id: str
    capability: SignedBorrowCapability


@dataclass(frozen=True)
class PreparedBorrowReturn:
    """Non-authorizing durable receipt for a closed borrow awaiting return media."""

    channel_uuid: str
    episode_id: str
    client_id: str
    intent_epoch: int
    configuration_generation: str
    control_generation: int
    recorder_fence: int
    connection_identity: tuple[str, ...]
    lease_digest: str


class RecorderBorrowCoordinator:
    """Poll the real native viewer state and durably issue one borrow opening.

    The recorder task calls ``poll`` from its supervisor thread. The full
    assigned-source/profile catalog is loaded once when the coordinator is
    constructed; source policy is used only to decide whether a dedicated
    fallback exists. An excluded source can therefore be observed for an
    existing viewer connection, but is never made a new recorder candidate.
    """

    def __init__(
        self, redis: Any, archive_root: Path, channel_uuid: str, *,
        active: dict[str, Any], control_generation: int, recorder_fence: int,
        lease_value: str, dedicated_source_available: bool,
    ) -> None:
        from .configuration import _sharing_catalog_generation
        from .recorder_proxy import configuration_generation

        if type(dedicated_source_available) is not bool:
            raise ValueError("dedicated source availability must be boolean")
        self.redis = redis
        self.channel_uuid = channel_uuid
        self.active = active
        self.control_generation = control_generation
        self.recorder_fence = recorder_fence
        self.lease_value = lease_value
        self.dedicated_source_available = dedicated_source_available
        self.configuration_generation = configuration_generation(active)
        self.intent_ledger = SharingIntentLedger(
            Path(archive_root) / ".catchuparr-sharing.sqlite3",
        )
        self.episode_ledger = BorrowEpisodeLedger(
            Path(archive_root) / ".catchuparr-sharing.sqlite3",
        )
        self.assigned_sources, self.catalog_generation = _native_source_catalog(channel_uuid)
        if _sharing_catalog_generation(channel_uuid) != self.catalog_generation:
            raise BorrowEpisodeRejected("source catalog changed during preload")
        from django.db import close_old_connections

        close_old_connections()
        self.state_record = self.intent_ledger.read(channel_uuid)
        if (
            self.state_record is None
            or self.state_record.state.configuration_generation != self.configuration_generation
            or self.state_record.state.control_generation != control_generation
            or self.state_record.state.recorder_fence != recorder_fence
        ):
            previous = self.state_record
            self.state_record = self.intent_ledger.recover(
                channel_uuid,
                configuration_generation=self.configuration_generation,
                control_generation=control_generation,
                recorder_fence=recorder_fence,
                now=time.time(),
                expected_revision=previous.revision if previous is not None else None,
                recovery_is_current=self._recovery_is_current,
            )
        self._recovered_return_receipt = self._read_return_receipt()

    def _read_return_receipt(self) -> PreparedBorrowReturn | None:
        """Rebuild cleanup evidence only; this never restores a borrowed grant."""
        try:
            current = self.intent_ledger.read(self.channel_uuid)
            token = current.intent if current is not None else None
            if (current is None
                    or current.state.configuration_generation != self.configuration_generation
                    or current.state.control_generation != self.control_generation
                    or current.state.recorder_fence != self.recorder_fence):
                return None
            if token is None:
                if (current.state.phase is not SharingPhase.FAILED
                        or current.state.pending_action is not None):
                    return None
                epoch_limit = current.state.intent_epoch + 1
            elif (token.action is not SharingAction.RETURN_TO_ALLOWED
                  or current.state.phase not in {SharingPhase.WAIT_RETURN, SharingPhase.FAILED}
                  or current.state.pending_action is not SharingAction.RETURN_TO_ALLOWED
                  or current.state.intent_epoch != token.intent_epoch
                  or token.configuration_generation != self.configuration_generation
                  or token.control_generation != self.control_generation
                  or token.recorder_fence != self.recorder_fence):
                return None
            else:
                epoch_limit = token.intent_epoch
            episode = self.episode_ledger.read(self.channel_uuid)
            if (episode.phase != "closed" or not episode.client_id
                    or episode.configuration_generation != self.configuration_generation
                    or episode.control_generation != self.control_generation
                    or episode.recorder_fence > self.recorder_fence
                    or episode.intent_epoch >= epoch_limit
                    or episode.identity is None):
                return None
            # Equal-fence recovery is allowed only when the closed episode was
            # opened under this exact live lease. Older fences remain historical
            # cleanup evidence; a future fence is always stale.
            if (episode.recorder_fence == self.recorder_fence
                    and episode.lease_digest != hashlib.sha256(
                        str(self.lease_value).encode("utf-8"),
                    ).hexdigest()):
                return None
            return PreparedBorrowReturn(
                channel_uuid=self.channel_uuid,
                episode_id=episode.episode_id,
                client_id=episode.client_id,
                intent_epoch=episode.intent_epoch,
                configuration_generation=episode.configuration_generation,
                control_generation=episode.control_generation,
                recorder_fence=episode.recorder_fence,
                connection_identity=episode.identity,
                lease_digest=episode.lease_digest,
            )
        except Exception as exc:
            from .logging_utils import error

            error("sharing_return_recovery_pending", exc, callsite="return_recovery")
            return None

    def recovered_return_receipt(self) -> PreparedBorrowReturn | None:
        return self._recovered_return_receipt

    def _recovery_is_current(
        self, channel_uuid: str, _revision: int | None,
        config_generation: str, control_generation: int, recorder_fence: int,
    ) -> bool:
        if channel_uuid != self.channel_uuid or config_generation != self.configuration_generation:
            return False
        if control_generation != self.control_generation or recorder_fence != self.recorder_fence:
            return False
        try:
            return (
                self.redis.get(f"catchuparr:recorder:{channel_uuid}") == self.lease_value
                and int(self.redis.ttl(f"catchuparr:recorder:{channel_uuid}")) > 0
            )
        except Exception:
            return False

    def _recover_closed_attach(self) -> bool:
        """Release a consumed ATTACH only after its exact opening is closed."""
        token = self.state_record.intent
        if token is None or token.action is not SharingAction.ATTACH_BORROW:
            return False
        try:
            episode = self.episode_ledger.read(self.channel_uuid)
            if (
                episode.phase != "closed"
                or episode.intent_epoch != token.intent_epoch
                or episode.configuration_generation != token.configuration_generation
                or episode.control_generation != token.control_generation
                or episode.recorder_fence != token.recorder_fence
                or episode.identity != token.target_identity
                or not episode.client_id
            ):
                return False
            from .adapters.sharing_native import (
                native_registration_is_absent,
                observe_native_episode_for_authority_drain,
            )

            projection = observe_native_episode_for_authority_drain(
                self.redis, self.channel_uuid,
            )
            if getattr(projection, "state", None) not in {"absent", "known_revoked"}:
                return False
            if not native_registration_is_absent(
                self.redis, channel_uuid=self.channel_uuid,
                client_id=episode.client_id,
            ):
                return False
            recovered = self.intent_ledger.recover(
                self.channel_uuid,
                configuration_generation=self.configuration_generation,
                control_generation=self.control_generation,
                recorder_fence=self.recorder_fence,
                now=time.time(),
                expected_revision=self.state_record.revision,
                recovery_is_current=self._recovery_is_current,
            )
            self.state_record = recovered
            return True
        except Exception:
            return False

    def _attach_claim_was_consumed(self, token: SharingIntentToken) -> bool:
        """Check both durable claim ledgers before retrying an ATTACH."""
        return (
            self.intent_ledger.claim_was_consumed(token)
            or self.episode_ledger.opening_claim_was_consumed(token)
        )

    def _retry_unclaimed_attach(
        self, *, active: dict[str, Any], control_generation: int,
    ) -> PreparedBorrowOpening | None:
        """Resume only a current ATTACH intent whose durable claim is unused."""
        token = self.state_record.intent
        if token is None or token.action is not SharingAction.ATTACH_BORROW:
            return None
        try:
            current_record = self.intent_ledger.read(self.channel_uuid)
            if (current_record is None or current_record.intent != token
                    or current_record.revision != self.state_record.revision
                    or self._attach_claim_was_consumed(token)):
                return None
            self.state_record = current_record
            from .configuration import _sharing_catalog_generation
            from .recorder_proxy import configuration_generation

            if (
                control_generation != self.control_generation
                or active.get("connection_sharing", {}).get(self.channel_uuid, {}).get("enabled") is not True
                or configuration_generation(active) != token.configuration_generation
                or token.configuration_generation != self.configuration_generation
                or token.control_generation != self.control_generation
                or token.recorder_fence != self.recorder_fence
                or token.target_identity is None
                or _sharing_catalog_generation(self.channel_uuid) != self.catalog_generation
                or not self._recovery_is_current(
                    self.channel_uuid, current_record.revision,
                    self.configuration_generation, self.control_generation,
                    self.recorder_fence,
                )
            ):
                return None
            current_sources, current_catalog = _native_source_catalog(self.channel_uuid)
            if current_catalog != self.catalog_generation:
                return None
            now = time.time()
            native = observe_native_connection(
                self.redis, self.channel_uuid,
                catalog_generation=current_catalog,
                assigned_sources=current_sources,
                observed_at=now,
            )
            target = native.target if native is not None else None
            if (target is None or target.connection_identity != token.target_identity
                    or not target.eligible_for_new_borrow(self.channel_uuid)):
                return None
            setting = active["connection_sharing"][self.channel_uuid]
            return_seconds = setting["last_viewer_return_seconds"]
            minimum_switch = setting["min_switch_interval_seconds"]
            if (type(return_seconds) is not int or not 0 <= return_seconds <= 3600
                    or type(minimum_switch) is not int
                    or not 0 <= minimum_switch <= 3600):
                return None

            def fresh_validate(exact_token: SharingIntentToken) -> bool:
                if exact_token != token:
                    return False
                try:
                    latest = self.intent_ledger.read(self.channel_uuid)
                    if (latest is None or latest.intent != token
                            or latest.revision != current_record.revision
                            or token.configuration_generation != self.configuration_generation
                            or token.control_generation != self.control_generation
                            or token.recorder_fence != self.recorder_fence
                            or not self._recovery_is_current(
                                self.channel_uuid, latest.revision,
                                self.configuration_generation, self.control_generation,
                                self.recorder_fence,
                            )
                            or _sharing_catalog_generation(self.channel_uuid) != self.catalog_generation):
                        return False
                    sources, catalog = _native_source_catalog(self.channel_uuid)
                    if catalog != self.catalog_generation:
                        return False
                    observed = observe_native_connection(
                        self.redis, self.channel_uuid,
                        catalog_generation=catalog,
                        assigned_sources=sources,
                        observed_at=time.time(),
                    )
                    current_target = observed.target if observed is not None else None
                    return bool(
                        current_target is not None
                        and current_target.connection_identity == token.target_identity
                        and current_target.eligible_for_new_borrow(self.channel_uuid)
                    )
                except Exception:
                    return False
                finally:
                    try:
                        from django.db import close_old_connections

                        close_old_connections()
                    except Exception:
                        pass

            claimed, prepared = prepare_borrow_opening(
                self.redis, self.intent_ledger, self.episode_ledger, token, target,
                lease_value=self.lease_value,
                catalog_generation=self.catalog_generation,
                return_seconds=return_seconds,
                minimum_switch_seconds=minimum_switch,
                last_switch_at=current_record.state.last_switch_at,
                capability_secret=_django_secret(),
                fresh_validate=fresh_validate,
            )
            self.state_record = self.intent_ledger.read(self.channel_uuid)
            return prepared if claimed else None
        except Exception:
            return None
        finally:
            try:
                from django.db import close_old_connections

                close_old_connections()
            except Exception:
                pass

    def record_return_after_cleanup(
        self, prepared: PreparedBorrowOpening, *, now: float,
        outcome: str = "return",
    ) -> bool:
        """Create a fenced return intent after the exact borrowed reader is gone.

        Cleanup is not a successful source switch. The return cooldown is
        committed later, after the allowlisted recorder publishes useful media.
        """
        if (outcome not in {"return", "failure", "stop"}
                or not isinstance(prepared, PreparedBorrowOpening)
                or isinstance(now, bool) or not isinstance(now, (int, float))
                or not math.isfinite(float(now))
                or not borrow_cleanup_acknowledged(self.redis, self.episode_ledger, prepared)):
            return False
        try:
            current = self.intent_ledger.read(self.channel_uuid)
            if current is None:
                return False
            self.state_record = current
            token = current.intent
            if token is not None:
                # A route that failed before activation consumes ATTACH, but it
                # is not a successful source switch and earns no cooldown.
                return bool(
                    token.action is SharingAction.ATTACH_BORROW
                    and token.intent_epoch == prepared.capability.intent_epoch
                    and self._recover_closed_attach()
                )
            state = current.state
            if outcome == "stop":
                # Exact cleanup was checked above. A control/Stop path must
                # not require the revoked lease or create a fallback intent.
                return True
            if (
                state.phase.value not in {"borrowed", "wait_return", "failed"}
                or state.intent_epoch != prepared.capability.intent_epoch
                or state.configuration_generation != prepared.capability.configuration_generation
                or state.control_generation != prepared.capability.control_generation
                or state.recorder_fence != prepared.capability.recorder_fence
                or not self._recovery_is_current(
                    self.channel_uuid, current.revision,
                    self.configuration_generation, self.control_generation,
                    self.recorder_fence,
                )
            ):
                return False
            if not self.dedicated_source_available:
                from dataclasses import replace

                paused = replace(
                    state, phase=(type(state.phase).FAILED if outcome == "failure"
                                  else type(state.phase).PAUSED),
                    candidate_identity=None, candidate_connection_identity=None,
                    candidate_since=None, borrowed_identity=None,
                    pending_action=None,
                )
                self.state_record = self.intent_ledger.save(
                    paused, action=None, expected_revision=current.revision,
                    updated_at=float(now),
                )
                return True
            if (token is not None
                    and token.action is SharingAction.RETURN_TO_ALLOWED
                    and token.intent_epoch > prepared.capability.intent_epoch):
                return True
            if token is not None:
                return False
            from dataclasses import replace

            from .configuration import _sharing_catalog_generation

            if _sharing_catalog_generation(self.channel_uuid) != self.catalog_generation:
                return False
            settings = self.active.get("connection_sharing", {}).get(self.channel_uuid, {})
            return_seconds = settings.get("last_viewer_return_seconds")
            minimum_switch = settings.get("min_switch_interval_seconds")
            if (type(return_seconds) is not int or not 0 <= return_seconds <= 3600
                    or type(minimum_switch) is not int or not 0 <= minimum_switch <= 3600):
                return False
            left_at = state.last_viewer_left_at
            if left_at is None:
                left_at = float(now)
            if outcome == "failure":
                # Recorder failure bypasses ordinary viewer-return and switch
                # cooldowns after the borrowed process has been reaped.
                deadline = float(now)
            else:
                deadline = max(
                    float(left_at) + return_seconds,
                    (float(state.last_switch_at) + minimum_switch)
                    if state.last_switch_at is not None else float(left_at),
                )
            next_state = replace(
                state, phase=(type(state.phase).FAILED if outcome == "failure"
                              else type(state.phase).WAIT_RETURN),
                candidate_identity=None, candidate_connection_identity=None,
                candidate_since=None, borrowed_identity=None,
                last_viewer_left_at=float(left_at),
                pending_action=SharingAction.RETURN_TO_ALLOWED,
                intent_epoch=state.intent_epoch + 1,
            )
            self.state_record = self.intent_ledger.save(
                next_state, action=SharingAction.RETURN_TO_ALLOWED,
                expected_revision=current.revision, not_before=deadline,
                updated_at=float(now),
            )
            return True
        except Exception:
            return False

    def pending_return_intent(self) -> SharingIntentToken | None:
        """Read the exact return token the recorder may attempt after its deadline."""
        try:
            latest = self.intent_ledger.read(self.channel_uuid)
            if latest is not None:
                self.state_record = latest
            token = latest.intent if latest is not None else None
            if (token is not None and token.action is SharingAction.RETURN_TO_ALLOWED
                    and token.intent_epoch > 0):
                return token
        except Exception:
            return None
        return None

    def viewer_return_deadline_reached(self, *, now: float) -> bool:
        """Use the first native zero-viewer observation, not reader teardown time."""
        if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(float(now)):
            return False
        try:
            current = self.intent_ledger.read(self.channel_uuid)
            if current is None:
                return False
            state = current.state
            left_at = state.last_viewer_left_at
            if state.phase.value != "wait_return" or left_at is None:
                return False
            settings = self.active.get("connection_sharing", {}).get(self.channel_uuid, {})
            return_seconds = settings.get("last_viewer_return_seconds")
            minimum_switch = settings.get("min_switch_interval_seconds")
            if (type(return_seconds) is not int or not 0 <= return_seconds <= 3600
                    or type(minimum_switch) is not int or not 0 <= minimum_switch <= 3600):
                return False
            deadline = max(
                float(left_at) + return_seconds,
                (float(state.last_switch_at) + minimum_switch)
                if state.last_switch_at is not None else float(left_at),
            )
            self.state_record = current
            return float(now) >= deadline
        except Exception:
            return False

    @staticmethod
    def _return_transition_is_committed(current, token, media_at: float) -> bool:
        """Recognize only the single CAS result for this exact first-media ACK."""
        if current is None or current.intent is not None:
            return False
        state = current.state
        return bool(
            current.revision == token.revision + 1
            and state.channel_uuid == token.channel_uuid
            and state.configuration_generation == token.configuration_generation
            and state.control_generation == token.control_generation
            and state.recorder_fence == token.recorder_fence
            and state.intent_epoch == token.intent_epoch
            and state.phase.value == "dedicated"
            and state.pending_action is None
            and state.last_switch_at == float(media_at)
            and state.last_viewer_left_at is None
        )

    def _return_media_binding_failure(
        self, prepared, token, attempt, candidate, media_at: float,
    ) -> str | None:
        """Return a fixed diagnostic category when current proof is absent."""
        from .configuration import _sharing_catalog_generation
        from .recorder_proxy import (
            candidate_is_current,
            current_sharing_authority_context,
            sharing_operation_is_current,
        )

        cleanup_ok = (
            borrow_cleanup_acknowledged(self.redis, self.episode_ledger, prepared)
            if isinstance(prepared, PreparedBorrowOpening)
            else borrow_return_cleanup_acknowledged(
                self.redis, self.episode_ledger, prepared,
            )
        )
        if not cleanup_ok:
            return "sharing_return_cleanup_pending"
        if (token.channel_uuid != self.channel_uuid
                or token.configuration_generation != self.configuration_generation
                or token.control_generation != self.control_generation
                or token.recorder_fence != self.recorder_fence
                or token.action is not SharingAction.RETURN_TO_ALLOWED
                or float(media_at) < token.not_before
                or (isinstance(prepared, PreparedBorrowOpening)
                    and (token.intent_epoch <= prepared.capability.intent_epoch
                         or token.recorder_fence != prepared.capability.recorder_fence))
                or (isinstance(prepared, PreparedBorrowReturn)
                    and (token.intent_epoch <= prepared.intent_epoch
                         or prepared.configuration_generation != self.configuration_generation
                         or prepared.control_generation != self.control_generation
                         or prepared.recorder_fence > self.recorder_fence))):
            return "sharing_return_binding_stale"
        if (getattr(attempt, "channel_uuid", None) != self.channel_uuid
                or getattr(attempt, "config_generation", None) != self.configuration_generation
                or getattr(attempt, "control_generation", None) != self.control_generation
                or getattr(attempt, "sharing_catalog_generation", None) != self.catalog_generation
                or str(candidate.get("id") or candidate.get("stream_id") or "")
                != getattr(attempt, "stream_id", None)
                or str(candidate.get("account_id") or "") != getattr(attempt, "account_id", None)):
            return "sharing_return_binding_stale"
        try:
            if (not self._recovery_is_current(
                    self.channel_uuid, None, self.configuration_generation,
                    self.control_generation, self.recorder_fence,
                )
                    or _sharing_catalog_generation(self.channel_uuid) != self.catalog_generation
                    or not candidate_is_current(
                        self.channel_uuid, attempt.stream_id, attempt.account_id, self.active,
                    )
                    or not sharing_operation_is_current(self.redis, attempt)):
                return "sharing_return_binding_stale"
            authority = current_sharing_authority_context(
                self.redis, self.channel_uuid,
                config_generation=self.configuration_generation,
                control_generation=self.control_generation,
            )
            if (authority is None
                    or authority.get("authority_revision")
                    != getattr(attempt, "sharing_authority_revision", None)
                    or authority.get("catalog_generation") != self.catalog_generation):
                return "sharing_return_binding_stale"
        except Exception:
            return "sharing_return_binding_stale"
        return None

    def acknowledge_return_media(
        self, prepared: PreparedBorrowOpening | PreparedBorrowReturn, token: SharingIntentToken,
        attempt: Any, candidate: dict[str, Any], *, media_at: float,
    ) -> bool:
        """Commit the return clock only after a current allowed source writes media."""
        if (not isinstance(prepared, (PreparedBorrowOpening, PreparedBorrowReturn))
                or not isinstance(token, SharingIntentToken)
                or token.action is not SharingAction.RETURN_TO_ALLOWED
                or not isinstance(candidate, dict)
                or isinstance(media_at, bool) or not isinstance(media_at, (int, float))
                or not math.isfinite(float(media_at))):
            return False
        from .sharing_planner import resolve_transition

        try:
            current = self.intent_ledger.read(self.channel_uuid)
            if current is None:
                from .logging_utils import error

                error("sharing_return_binding_stale", callsite="return_completion")
                return False
            failure = self._return_media_binding_failure(
                prepared, token, attempt, candidate, float(media_at),
            )
            if failure is not None:
                from .logging_utils import error

                error(failure, callsite="return_completion")
                return False
            if self._return_transition_is_committed(current, token, float(media_at)):
                self.state_record = current
                return True
            if (current.intent != token
                    or current.state.phase not in {SharingPhase.WAIT_RETURN, SharingPhase.FAILED}
                    or current.state.pending_action is not SharingAction.RETURN_TO_ALLOWED
                    or current.state.intent_epoch != token.intent_epoch
                    or current.state.configuration_generation != self.configuration_generation
                    or current.state.control_generation != self.control_generation
                    or current.state.recorder_fence != self.recorder_fence):
                from .logging_utils import error

                error("sharing_return_binding_stale", callsite="return_completion")
                return False
            settings_value = self.active.get("connection_sharing", {}).get(self.channel_uuid, {})
            settings = SharingSettings(
                enabled=True,
                live_stability_seconds=settings_value["live_stability_seconds"],
                last_viewer_return_seconds=settings_value["last_viewer_return_seconds"],
                min_switch_interval_seconds=settings_value["min_switch_interval_seconds"],
            )
            observation = SharingObservation(
                channel_uuid=self.channel_uuid, now=float(media_at),
                configuration_generation=self.configuration_generation,
                control_generation=self.control_generation,
                recorder_fence=self.recorder_fence, lease_is_current=True,
                settings=settings, target=None, dedicated_source_available=True,
            )
            resolved = resolve_transition(
                current.state, SharingAction.RETURN_TO_ALLOWED, succeeded=True,
                now=float(media_at), current_state=current.state,
                current_observation=observation,
                admitted_return_deadline=token.not_before,
            )
            if (resolved.pending_action is not None
                    or resolved.last_switch_at != float(media_at)):
                from .logging_utils import error

                error("sharing_return_media_unconfirmed", callsite="return_completion")
                return False
            # Fresh ownership is checked immediately before the SQL revision CAS.
            if not self._recovery_is_current(
                self.channel_uuid, current.revision, self.configuration_generation,
                self.control_generation, self.recorder_fence,
            ):
                from .logging_utils import error

                error("sharing_return_binding_stale", callsite="return_completion")
                return False
            try:
                saved = self.intent_ledger.save(
                    resolved, action=None, expected_revision=token.revision,
                    updated_at=time.time(),
                )
            except Exception as exc:
                # A caller may lose the result after SQLite committed. Only the
                # exact one-step state transition is an idempotent success.
                latest = self.intent_ledger.read(self.channel_uuid)
                if (latest is not None
                        and self._return_transition_is_committed(latest, token, float(media_at))
                        and self._return_media_binding_failure(
                            prepared, token, attempt, candidate, float(media_at),
                        ) is None):
                    self.state_record = latest
                    return True
                from .logging_utils import error

                error("sharing_return_media_unconfirmed", exc, callsite="return_completion")
                return False
            self.state_record = saved
            return True
        except Exception as exc:
            from .logging_utils import error

            error("sharing_return_media_unconfirmed", exc, callsite="return_completion")
            return False

    def return_ack_retryable(
        self, prepared: PreparedBorrowOpening | PreparedBorrowReturn, token: SharingIntentToken,
        attempt: Any, candidate: dict[str, Any], *, media_at: float,
    ) -> bool:
        """Retry a transient commit failure only while the same source is current."""
        try:
            current = self.intent_ledger.read(self.channel_uuid)
            if current is None:
                return False
            if self._return_media_binding_failure(
                prepared, token, attempt, candidate, float(media_at),
            ) is not None:
                return False
            if self._return_transition_is_committed(current, token, float(media_at)):
                self.state_record = current
                return True
            if (current.intent != token
                    or current.state.phase.value != "wait_return"
                    or current.state.pending_action is not SharingAction.RETURN_TO_ALLOWED
                    or current.state.intent_epoch != token.intent_epoch):
                return False
            self.state_record = current
            return True
        except Exception:
            return False

    def _retry_closed_return_for_new_viewer(
        self, *, active: dict[str, Any], control_generation: int,
    ) -> PreparedBorrowOpening | None:
        """Supersede an expired RETURN only for a different, fully eligible viewer.

        The prior native borrower must already be durably closed and absent.
        A fresh ATTACH token is then written by SQLite revision CAS; the old
        RETURN token is never reused. Same-identity viewers still wait for the
        ordinary dedicated-media return acknowledgement.
        """
        token = self.state_record.intent
        if (token is None or token.action is not SharingAction.RETURN_TO_ALLOWED
                or control_generation != self.control_generation
                or active.get("connection_sharing", {}).get(self.channel_uuid, {}).get("enabled") is not True
                or active.get("recording_enabled") is not True):
            return None
        try:
            from dataclasses import replace

            from .configuration import _sharing_catalog_generation
            from .recorder_proxy import configuration_generation
            from .schedule import schedule_from_snapshot, schedule_is_active

            current = self.intent_ledger.read(self.channel_uuid)
            if (current is None or current.intent != token
                    or current.revision != self.state_record.revision
                    or current.state.phase not in {
                        SharingPhase.WAIT_RETURN, SharingPhase.FAILED, SharingPhase.PAUSED,
                    }
                    or current.state.pending_action is not SharingAction.RETURN_TO_ALLOWED
                    or current.state.intent_epoch != token.intent_epoch
                    or token.configuration_generation != self.configuration_generation
                    or token.control_generation != self.control_generation
                    or token.recorder_fence != self.recorder_fence
                    or configuration_generation(active) != self.configuration_generation
                    or self.intent_ledger.claim_was_consumed(token)):
                return None
            self.state_record = current
            now = time.time()
            if now < token.not_before:
                return None
            schedules = active.get("recording_schedule")
            if not isinstance(schedules, dict):
                return None
            schedule_config = schedules.get("channels")
            timezone_name = schedules.get("timezone")
            if (not isinstance(schedule_config, dict)
                    or self.channel_uuid not in schedule_config
                    or not isinstance(timezone_name, str)):
                return None
            schedule = schedule_from_snapshot(schedule_config[self.channel_uuid])
            if not schedule_is_active(
                    schedule, timezone_name, datetime.fromtimestamp(now, timezone.utc)):
                return None
            if (not self._recovery_is_current(
                    self.channel_uuid, current.revision, self.configuration_generation,
                    self.control_generation, self.recorder_fence,
                )
                    or _sharing_catalog_generation(self.channel_uuid) != self.catalog_generation):
                return None
            current_sources, current_catalog = _native_source_catalog(self.channel_uuid)
            if current_catalog != self.catalog_generation:
                return None
            native = observe_native_connection(
                self.redis, self.channel_uuid, catalog_generation=current_catalog,
                assigned_sources=current_sources, observed_at=now,
            )
            target = native.target if native is not None else None
            if target is None or not target.eligible_for_new_borrow(self.channel_uuid):
                return None

            receipt = self._read_return_receipt()
            if (receipt is None or receipt.intent_epoch >= token.intent_epoch
                    or receipt.recorder_fence > self.recorder_fence
                    or (target.connection_identity == receipt.connection_identity
                        and current.state.phase is not SharingPhase.FAILED)
                    or not borrow_return_cleanup_acknowledged(
                        self.redis, self.episode_ledger, receipt,
                    )):
                return None
            latest = self.intent_ledger.read(self.channel_uuid)
            if (latest is None or latest.intent != token or latest.revision != current.revision
                    or not self._recovery_is_current(
                        self.channel_uuid, latest.revision, self.configuration_generation,
                        self.control_generation, self.recorder_fence,
                    )
                    or _sharing_catalog_generation(self.channel_uuid) != self.catalog_generation):
                return None

            setting = active["connection_sharing"][self.channel_uuid]
            settings = SharingSettings(
                enabled=True,
                live_stability_seconds=setting["live_stability_seconds"],
                last_viewer_return_seconds=setting["last_viewer_return_seconds"],
                min_switch_interval_seconds=setting["min_switch_interval_seconds"],
            )
            # Clear only the completed prior route intent. Preserve the last
            # successful switch timestamp so the normal stability/cooldown
            # calculation still applies to this new viewer.
            base = replace(
                latest.state,
                phase=SharingPhase.DEDICATED,
                candidate_identity=None,
                candidate_connection_identity=None,
                candidate_since=None,
                borrowed_identity=None,
                last_viewer_left_at=None,
                pending_action=None,
            )
            observation = SharingObservation(
                channel_uuid=self.channel_uuid, now=now,
                configuration_generation=self.configuration_generation,
                control_generation=self.control_generation,
                recorder_fence=self.recorder_fence, lease_is_current=True,
                settings=settings, target=target,
                recording_enabled=True, schedule_open=True,
                dedicated_source_available=self.dedicated_source_available,
            )
            failure_recovery = latest.state.phase is SharingPhase.FAILED
            planning = replace(base, last_switch_at=None) if failure_recovery else base
            decision = observe(planning, observation)
            if failure_recovery:
                # Preserve the true switch time for subsequent normal moves;
                # this one exact, freshly admitted failure recovery is exempt.
                decision = replace(decision, state=replace(
                    decision.state, last_switch_at=base.last_switch_at,
                ))
            action = decision.action if decision.action not in {
                SharingAction.WAIT, SharingAction.NOOP,
            } else None
            target_identity = (
                decision.state.candidate_connection_identity
                if action is SharingAction.ATTACH_BORROW else None
            )
            if not self._recovery_is_current(
                    self.channel_uuid, latest.revision, self.configuration_generation,
                    self.control_generation, self.recorder_fence,
                ):
                return None
            self.state_record = self.intent_ledger.save(
                decision.state, action=action,
                expected_revision=latest.revision,
                target_identity=target_identity,
                target_admitted=action is SharingAction.ATTACH_BORROW,
                not_before=(
                    (decision.deadline if decision.deadline is not None else now)
                    if action is not None else None
                ),
                updated_at=now,
            )
            if (self.state_record.intent is None
                    or self.state_record.intent.action is not SharingAction.ATTACH_BORROW):
                return None
            return self._retry_unclaimed_attach(
                active=active, control_generation=control_generation,
            )
        except Exception:
            return None
        finally:
            try:
                from django.db import close_old_connections

                close_old_connections()
            except Exception:
                pass

    def poll(self, *, active: dict[str, Any], control_generation: int) -> PreparedBorrowOpening | None:
        """Persist cooldown observations and claim an ATTACH only at its deadline."""
        latest = self.intent_ledger.read(self.channel_uuid)
        if latest is not None and latest.revision > self.state_record.revision:
            self.state_record = latest
        try:
            from .adapters.sharing_native import read_native_episode_grant

            live_episode = read_native_episode_grant(self.redis, self.channel_uuid)
            if live_episode is not None and live_episode.phase in {"opening", "active", "wait_return", "closing"}:
                if live_episode.phase in {"active", "wait_return"}:
                    self._sync_viewer_grace(live_episode)
                return None
        except Exception:
            return None
        if (self.state_record.intent is None
                and self.state_record.state.phase in {SharingPhase.BORROWED, SharingPhase.WAIT_RETURN}):
            # Native cleanup can remove its projection before the recorder has
            # reaped the reader and recorded its fenced return. The persisted
            # state has no borrowed identity, so ordinary observation here
            # would otherwise erase the return phase and its grace clocks.
            return None
        if (
            self.state_record.intent is not None
            or control_generation != self.control_generation
            or active.get("connection_sharing", {}).get(self.channel_uuid, {}).get("enabled") is not True
        ):
            if (
                self.state_record.intent is not None
                and control_generation == self.control_generation
                and active.get("connection_sharing", {}).get(self.channel_uuid, {}).get("enabled") is True
            ):
                token = self.state_record.intent
                if token.action is SharingAction.ATTACH_BORROW:
                    try:
                        unclaimed = not self._attach_claim_was_consumed(token)
                    except Exception:
                        return None
                    if unclaimed:
                        return self._retry_unclaimed_attach(
                            active=active, control_generation=control_generation,
                        )
                elif token.action is SharingAction.RETURN_TO_ALLOWED:
                    return self._retry_closed_return_for_new_viewer(
                        active=active, control_generation=control_generation,
                    )
                self._recover_closed_attach()
            return None
        from .configuration import _sharing_catalog_generation
        from .recorder_proxy import configuration_generation

        config_generation = configuration_generation(active)
        if config_generation != self.configuration_generation:
            return None
        now = time.time()
        if not self._recovery_is_current(
            self.channel_uuid, self.state_record.revision, config_generation,
            control_generation, self.recorder_fence,
        ):
            return None
        try:
            if _sharing_catalog_generation(self.channel_uuid) != self.catalog_generation:
                return None
            native = observe_native_connection(
                self.redis, self.channel_uuid,
                catalog_generation=self.catalog_generation,
                assigned_sources=self.assigned_sources,
                observed_at=now,
            )
        except Exception as exc:
            from .logging_utils import error

            error("sharing_observation_failed", exc, callsite="native_observation")
            return None
        finally:
            try:
                from django.db import close_old_connections

                close_old_connections()
            except Exception:
                pass
        setting = active["connection_sharing"][self.channel_uuid]
        settings = SharingSettings(
            enabled=True,
            live_stability_seconds=setting["live_stability_seconds"],
            last_viewer_return_seconds=setting["last_viewer_return_seconds"],
            min_switch_interval_seconds=setting["min_switch_interval_seconds"],
        )
        observation = SharingObservation(
            channel_uuid=self.channel_uuid,
            now=now,
            configuration_generation=self.configuration_generation,
            control_generation=self.control_generation,
            recorder_fence=self.recorder_fence,
            lease_is_current=True,
            settings=settings,
            target=native.target if native is not None else None,
            dedicated_source_available=self.dedicated_source_available,
        )
        state = self.state_record.state
        if (state.phase is SharingPhase.FAILED and observation.target is None
                and not self.dedicated_source_available):
            # Keep the failure marker until a viewer appears; no private
            # source can resume recording while the allowed list is empty.
            return None
        failure_recovery = False
        if state.phase is SharingPhase.FAILED and observation.target is not None:
            # Only the exact, durably closed failure episode can authorize
            # bypassing normal cooldowns. No viewer means normal fallback.
            receipt = self._read_return_receipt()
            failure_recovery = bool(
                receipt is not None
                and receipt.intent_epoch == state.intent_epoch
                and receipt.recorder_fence == self.recorder_fence
                and borrow_return_cleanup_acknowledged(self.redis, self.episode_ledger, receipt)
            )
        if failure_recovery:
            from dataclasses import replace

            planning_state = replace(state, last_switch_at=None)
            decision = observe(planning_state, observation)
            decision = replace(decision, state=replace(
                decision.state, last_switch_at=state.last_switch_at,
            ))
        else:
            decision = observe(state, observation)
        if decision.action is SharingAction.NOOP and decision.state == state:
            return None
        token_action = decision.action if decision.action not in {SharingAction.WAIT, SharingAction.NOOP} else None
        target_identity = (
            decision.state.candidate_connection_identity
            if token_action is SharingAction.ATTACH_BORROW else None
        )
        self.state_record = self.intent_ledger.save(
            decision.state,
            action=token_action,
            expected_revision=self.state_record.revision,
            target_identity=target_identity,
            target_admitted=token_action is SharingAction.ATTACH_BORROW,
            not_before=(decision.deadline if decision.deadline is not None else now)
            if token_action is not None else None,
            updated_at=now,
        )
        token = self.state_record.intent
        if token is None or token.action is not SharingAction.ATTACH_BORROW:
            return None
        target = native.target if native is not None else None
        if target is None:
            return None

        def fresh_validate(exact_token: SharingIntentToken) -> bool:
            if exact_token != token or not self._recovery_is_current(
                self.channel_uuid, self.state_record.revision,
                self.configuration_generation, self.control_generation,
                self.recorder_fence,
            ):
                return False
            try:
                catalog = _sharing_catalog_generation(self.channel_uuid)
                current_sources, current_catalog = _native_source_catalog(self.channel_uuid)
                if catalog != self.catalog_generation or current_catalog != self.catalog_generation:
                    return False
                current = observe_native_connection(
                    self.redis, self.channel_uuid,
                    catalog_generation=current_catalog,
                    assigned_sources=current_sources,
                    observed_at=time.time(),
                )
                return bool(
                    current is not None
                    and current.target.connection_identity == exact_token.target_identity
                    and current.target.eligible_for_new_borrow(self.channel_uuid)
                )
            except Exception:
                return False
            finally:
                try:
                    from django.db import close_old_connections

                    close_old_connections()
                except Exception:
                    pass

        claimed, prepared = prepare_borrow_opening(
            self.redis, self.intent_ledger, self.episode_ledger, token, target,
            lease_value=self.lease_value,
            catalog_generation=self.catalog_generation,
            return_seconds=settings.last_viewer_return_seconds,
            minimum_switch_seconds=settings.min_switch_interval_seconds,
            last_switch_at=self.state_record.state.last_switch_at,
            capability_secret=_django_secret(),
            fresh_validate=fresh_validate,
        )
        self.state_record = self.intent_ledger.read(self.channel_uuid)
        return prepared if claimed else None

    def _sync_viewer_grace(self, grant: Any) -> None:
        """Persist the native episode's first zero-viewer time without SQL on reads."""
        from dataclasses import replace

        try:
            current = self.intent_ledger.read(self.channel_uuid)
            if current is None or current.intent is not None:
                return
            state = current.state
            if (grant.configuration_generation != self.configuration_generation
                    or grant.control_generation != self.control_generation
                    or grant.recorder_fence != self.recorder_fence
                    or grant.intent_epoch != state.intent_epoch):
                return
            if grant.phase == "active":
                phase = type(state.phase).BORROWED
                left_at = None
            else:
                phase = type(state.phase).WAIT_RETURN
                left_at = float(grant.last_viewer_left_at)
            if state.phase == phase and state.last_viewer_left_at == left_at:
                self.state_record = current
                return
            updated = replace(
                state, phase=phase, last_viewer_left_at=left_at,
                candidate_identity=None, candidate_connection_identity=None,
                candidate_since=None, borrowed_identity=None,
                pending_action=None,
            )
            self.state_record = self.intent_ledger.save(
                updated, action=None, expected_revision=current.revision,
                updated_at=time.time(),
            )
        except Exception:
            # The native grant remains the authoritative reader gate. A failed
            # advisory persistence update cannot broaden its deadline.
            return


def borrow_cleanup_acknowledged(
    redis: Any, ledger: BorrowEpisodeLedger, prepared: PreparedBorrowOpening,
) -> bool:
    """Require exact durable and native absence before another source can start."""
    if (not isinstance(ledger, BorrowEpisodeLedger)
            or not isinstance(prepared, PreparedBorrowOpening)):
        return False
    try:
        episode = ledger.read(prepared.capability.channel_uuid)
        if (
            episode.phase != "closed"
            or episode.episode_id != prepared.episode_id
            or episode.version < prepared.episode_version
            or episode.client_id != prepared.client_id
            or episode.identity != prepared.capability.connection_identity
            or episode.configuration_generation != prepared.capability.configuration_generation
            or episode.control_generation != prepared.capability.control_generation
            or episode.recorder_fence != prepared.capability.recorder_fence
            or episode.intent_epoch != prepared.capability.intent_epoch
        ):
            return False
        from .adapters.sharing_native import (
            native_registration_is_absent,
            observe_native_episode_for_authority_drain,
        )

        projection = observe_native_episode_for_authority_drain(
            redis, prepared.capability.channel_uuid,
        )
        if getattr(projection, "state", None) not in {"absent", "known_revoked"}:
            return False
        return native_registration_is_absent(
            redis,
            channel_uuid=prepared.capability.channel_uuid,
            client_id=prepared.client_id,
        )
    except Exception:
        return False


def borrow_return_cleanup_acknowledged(
    redis: Any, ledger: BorrowEpisodeLedger, receipt: PreparedBorrowReturn,
) -> bool:
    """Validate persisted close evidence without reauthorizing the old borrow."""
    if not isinstance(ledger, BorrowEpisodeLedger) or not isinstance(receipt, PreparedBorrowReturn):
        return False
    try:
        episode = ledger.read(receipt.channel_uuid)
        if (episode.phase != "closed"
                or episode.episode_id != receipt.episode_id
                or episode.client_id != receipt.client_id
                or episode.identity != receipt.connection_identity
                or episode.intent_epoch != receipt.intent_epoch
                or episode.configuration_generation != receipt.configuration_generation
                or episode.control_generation != receipt.control_generation
                or episode.recorder_fence != receipt.recorder_fence
                or episode.lease_digest != receipt.lease_digest):
            return False
        from .adapters.sharing_native import (
            native_registration_is_absent,
            observe_native_episode_for_authority_drain,
        )

        projection = observe_native_episode_for_authority_drain(redis, receipt.channel_uuid)
        if getattr(projection, "state", None) not in {"absent", "known_revoked"}:
            return False
        return native_registration_is_absent(
            redis, channel_uuid=receipt.channel_uuid, client_id=receipt.client_id,
        )
    except Exception:
        return False


def acknowledge_opened_borrow(
    capability: SignedBorrowCapability,
    intent_ledger: SharingIntentLedger,
    episode_ledger: BorrowEpisodeLedger,
    *,
    target: BorrowTarget,
    settings: SharingSettings,
    now: float,
) -> bool:
    """Commit the ATTACH cooldown only after exact native registration ACK."""

    from .sharing_planner import resolve_transition

    if (
        not isinstance(capability, SignedBorrowCapability)
        or not isinstance(intent_ledger, SharingIntentLedger)
        or not isinstance(episode_ledger, BorrowEpisodeLedger)
        or not isinstance(target, BorrowTarget)
        or target.connection_identity != capability.connection_identity
        or not isinstance(settings, SharingSettings)
        or isinstance(now, bool) or not isinstance(now, (int, float))
        or not math.isfinite(float(now))
    ):
        return False
    current = intent_ledger.read(capability.channel_uuid)
    if current is None:
        return False
    token = current.intent
    if (
        token is None
        or token.action is not SharingAction.ATTACH_BORROW
        or token.intent_epoch != capability.intent_epoch
        or token.configuration_generation != capability.configuration_generation
        or token.control_generation != capability.control_generation
        or token.recorder_fence != capability.recorder_fence
        or token.target_identity != capability.connection_identity
    ):
        return False
    try:
        episode = episode_ledger.read(capability.channel_uuid)
    except Exception:
        return False
    if (
        episode.phase != "active"
        or episode.episode_id != capability.episode_id
        or episode.version != capability.episode_version + 1
        or episode.identity != capability.connection_identity
        or episode.intent_epoch != capability.intent_epoch
        or episode.recorder_fence != capability.recorder_fence
    ):
        return False
    observation = SharingObservation(
        channel_uuid=capability.channel_uuid,
        now=float(now),
        configuration_generation=capability.configuration_generation,
        control_generation=capability.control_generation,
        recorder_fence=capability.recorder_fence,
        lease_is_current=True,
        settings=settings,
        target=target,
        dedicated_source_available=True,
    )
    resolved = resolve_transition(
        current.state, SharingAction.ATTACH_BORROW, succeeded=True,
        now=float(now), current_state=current.state,
        current_observation=observation,
        admitted_attach_deadline=token.not_before,
    )
    if resolved.pending_action is not None or resolved.last_switch_at != float(now):
        return False
    try:
        intent_ledger.save(
            resolved, action=None, expected_revision=current.revision,
            updated_at=float(now),
        )
    except Exception:
        return False
    return True


_pending_open_cleanup_lock = threading.Lock()
_pending_open_cleanups: dict[str, Any] = {}
_pending_open_cleanup_channels: dict[str, str] = {}
_pending_open_cleanup_locks: dict[str, threading.Lock] = {}


class _RetainedCleanup:
    """Keep a task-local manager and exact retry callback alive across requests."""

    def __init__(self, retry: Callable[[], bool], *, channel_uuid: str) -> None:
        self._retry = retry
        self.channel_uuid = str(channel_uuid)

    def retry_cleanup(self) -> bool:
        return self._retry() is True


def _retain_durable_cleanup(
    episode_id: str, operation: Any, *, channel_uuid: str,
) -> None:
    _store_retained_cleanup(episode_id, operation, channel_uuid=channel_uuid)


def _store_retained_cleanup(episode_id: str, operation: Any, *, channel_uuid: str) -> None:
    with _pending_open_cleanup_lock:
        operation_lock = _pending_open_cleanup_locks.setdefault(episode_id, threading.Lock())
    with operation_lock:
        with _pending_open_cleanup_lock:
            _pending_open_cleanup_locks[episode_id] = operation_lock
            _pending_open_cleanups[episode_id] = operation
            _pending_open_cleanup_channels[episode_id] = str(channel_uuid)


def retain_open_cleanup_pending(error: BaseException) -> None:
    """Keep the exact open-failure cleanup owner reachable in this web process."""
    from .adapters.sharing_borrow_route import BorrowOpenCleanupPending

    if not isinstance(error, BorrowOpenCleanupPending):
        return
    episode = getattr(error, "_episode", None)
    episode_id = getattr(episode, "episode_id", None)
    if not isinstance(episode_id, str) or not episode_id:
        return
    channel_uuid = getattr(episode, "channel_uuid", None)
    _store_retained_cleanup(
        episode_id, error,
        channel_uuid=(channel_uuid if isinstance(channel_uuid, str) and channel_uuid else ""),
    )


def retain_session_cleanup_pending(session: BorrowedReaderSession) -> None:
    """Retain an exact session whose HTTP close has not been acknowledged."""
    ack = getattr(session, "ack", None)
    episode_id = getattr(ack, "episode_id", None)
    if not isinstance(episode_id, str) or not episode_id:
        return
    channel_uuid = getattr(ack, "channel_uuid", None)
    _store_retained_cleanup(
        episode_id, session,
        channel_uuid=(channel_uuid if isinstance(channel_uuid, str) and channel_uuid else ""),
    )


def retry_pending_open_cleanups(
    *, channel_uuid: str | None = None, episode_id: str | None = None,
) -> tuple[int, int]:
    """Retry retained exact cleanup handles; never infer closure from a timeout."""
    completed = 0
    pending = 0
    with _pending_open_cleanup_lock:
        items = tuple(
            (key, value, _pending_open_cleanup_locks.setdefault(key, threading.Lock()))
            for key, value in _pending_open_cleanups.items()
            if (episode_id is None or key == episode_id)
            and (channel_uuid is None
                 or _pending_open_cleanup_channels.get(key) == str(channel_uuid))
        )
    for episode_id, operation, operation_lock in items:
        with operation_lock:
            with _pending_open_cleanup_lock:
                if _pending_open_cleanups.get(episode_id) is not operation:
                    continue
            try:
                done = operation.retry_cleanup() is True
            except Exception:
                done = False
            with _pending_open_cleanup_lock:
                if _pending_open_cleanups.get(episode_id) is not operation:
                    continue
                if done:
                    del _pending_open_cleanups[episode_id]
                    _pending_open_cleanup_channels.pop(episode_id, None)
                    _pending_open_cleanup_locks.pop(episode_id, None)
                    completed += 1
                else:
                    pending += 1
    return completed, pending


def retry_durable_open_cleanups(ledger: BorrowEpisodeLedger, redis_text: Any, *,
                                now: float | None = None, stale_after: float = 120.0,
                                episode_id: str | None = None,
                                channel_uuid: str | None = None) -> tuple[int, int]:
    """Rebuild exact cleanup ownership from SQLite after a web worker restart.

    This runs only at route setup, never in the TS byte loop. Stale opening
    tickets have expired before they are fenced; active episodes are not
    selected unless a prior failure durably marked cleanup_pending.
    """
    from dataclasses import replace

    from .adapters.sharing_borrow_route import _recover_registration_ack
    from .adapters.sharing_native import (
        NativeClientRegistrationAck,
        cleanup_borrowed_client,
        close_fenced_unregistered_opening,
        fence_unregistered_opening,
        native_registration_is_absent,
        read_native_episode_cleanup_binding,
        read_native_episode_grant,
        tombstone_expired_unprojected_opening,
    )
    from .adapters.sharing_reader import redis_backend_signature

    observed_at = time.time() if now is None else now
    if (isinstance(observed_at, bool) or not isinstance(observed_at, (int, float))
            or not math.isfinite(float(observed_at))
            or isinstance(stale_after, bool) or not isinstance(stale_after, (int, float))
            or not math.isfinite(float(stale_after)) or stale_after < 0):
        return 0, 0
    retained_completed, retained_pending = retry_pending_open_cleanups(
        channel_uuid=channel_uuid, episode_id=episode_id,
    )
    try:
        candidates = ledger.registration_recovery_candidates(
            stale_opening_before=float(observed_at) - float(stale_after),
            episode_id=episode_id, channel_uuid=channel_uuid,
        )
        text_backend = redis_backend_signature(redis_text)
    except Exception:
        return 0, 1
    with _pending_open_cleanup_lock:
        retained_ids = set(_pending_open_cleanups)
        retained_for_scope = {
            key for key in retained_ids
            if (episode_id is None or key == episode_id)
            and (channel_uuid is None
                 or _pending_open_cleanup_channels.get(key) == str(channel_uuid))
        }
    completed = retained_completed
    pending = retained_pending
    for snapshot, registration in candidates:
        if snapshot.episode_id in retained_ids:
            if snapshot.episode_id not in retained_for_scope:
                pending += 1
                retained_for_scope.add(snapshot.episode_id)
            continue
        manager = None
        owns_manager = False
        cleanup_progress = {"native_done": False, "manager_stopped": False}
        try:
            grant = read_native_episode_grant(redis_text, snapshot.channel_uuid)
            if grant is not None and grant.episode_id == snapshot.episode_id:
                catalog_generation = grant.catalog_generation
            else:
                # Opening/closing/closed projections are deliberately not
                # decodable as live authority. Cleanup may use only a strict
                # immutable identity match and never treats this as a grant.
                catalog_generation = read_native_episode_cleanup_binding(
                    redis_text, snapshot,
                )
                if catalog_generation is None:
                    # A durable ACK is enough to remove that exact native
                    # registration even after the short-lived episode hash
                    # expires. This does not recover or grant read authority.
                    # Unknown/prepared registrations still require a live
                    # projection so a possibly delayed registration can fence.
                    if registration is None or registration.status != "registered":
                        # Once the exact projection TTL has elapsed, the
                        # registration Lua can no longer authorize a delayed
                        # HTTP request. Require the full opening lifetime,
                        # an actually absent projection key, and absence of
                        # every exact private-client resource before closing
                        # an unknown/prepared attempt.
                        client_id = registration.client_id if registration is not None else snapshot.client_id
                        cutoff = float(observed_at) - max(120.0, float(stale_after))
                        if (
                            isinstance(client_id, str)
                            and client_id.startswith("catchuparr-client-")
                            and ledger.registration_attempt_is_stale(snapshot, before=cutoff)
                        ):
                            current = ledger.read(snapshot.channel_uuid)
                            if (current.episode_id == snapshot.episode_id
                                    and current.version == snapshot.version
                                    and current.phase in {"opening", "cleanup_pending"}):
                                if tombstone_expired_unprojected_opening(
                                    redis_text, current, client_id=client_id,
                                ):
                                    ledger.revoke(
                                        current.episode_id, expected_version=current.version,
                                        reason="closed", cleanup_acknowledged=True,
                                    )
                                    completed += 1
                                    continue
                        pending += 1
                        continue
                    catalog_generation = "expired-registration-projection"
            opening_version = (
                registration.opening_version if registration is not None
                else snapshot.version - (1 if snapshot.phase == "cleanup_pending" else 0)
            )
            opening = replace(
                snapshot, phase="opening", version=opening_version,
                connected_at=None, last_viewer_left_at=None, return_deadline=None,
                client_id=(registration.client_id if registration is not None else snapshot.client_id),
            )

            def close_sqlite_after_ack(exact_snapshot=snapshot) -> bool:
                try:
                    current = ledger.read(exact_snapshot.channel_uuid)
                    if current.episode_id != exact_snapshot.episode_id:
                        return False
                    if current.phase != "closed":
                        ledger.revoke(
                            current.episode_id, expected_version=current.version,
                            reason="closed", cleanup_acknowledged=True,
                        )
                    return True
                except Exception:
                    return False

            def retain_retry(
                retry_native: Callable[[], bool], *, exact_manager=None,
                exact_snapshot=snapshot,
            ) -> None:
                nonlocal manager
                if exact_manager is None:
                    exact_manager = manager
                if exact_manager is None:
                    return
                progress = {"native_done": False, "manager_stopped": False}

                def retry_owned_cleanup(
                    exact_retry=retry_native,
                    exact_manager=exact_manager,
                    exact_close=close_sqlite_after_ack,
                ) -> bool:
                    if not progress["native_done"]:
                        if exact_retry() is not True:
                            return False
                        progress["native_done"] = True
                    if not progress["manager_stopped"]:
                        stop = getattr(exact_manager, "stop", None)
                        if not callable(stop):
                            return False
                        try:
                            stop()
                        except Exception:
                            return False
                        progress["manager_stopped"] = True
                    return exact_close()

                _retain_durable_cleanup(
                    exact_snapshot.episode_id,
                    _RetainedCleanup(retry_owned_cleanup, channel_uuid=exact_snapshot.channel_uuid),
                    channel_uuid=exact_snapshot.channel_uuid,
                )
                manager = None

            if registration is None:
                if not fence_unregistered_opening(
                    redis_text, opening, catalog_generation=catalog_generation,
                ) or not native_registration_is_absent(
                    redis_text, channel_uuid=snapshot.channel_uuid,
                    client_id=str(opening.client_id),
                ) or not close_fenced_unregistered_opening(
                    redis_text, opening, catalog_generation=catalog_generation,
                    client_id=str(opening.client_id),
                ):
                    pending += 1
                    continue
                cleanup_progress["native_done"] = True
            else:
                if registration.backend_signature != text_backend:
                    pending += 1
                    continue
                from apps.proxy.live_proxy.client_manager import ClientManager

                manager = ClientManager(
                    channel_id=snapshot.channel_uuid, redis_client=redis_text,
                    worker_id=registration.local_worker_id,
                )
                owns_manager = True
                if (getattr(manager, "channel_id", None) != snapshot.channel_uuid
                        or getattr(manager, "worker_id", None) != registration.local_worker_id
                        or redis_backend_signature(manager.redis_client) != registration.backend_signature):
                    # This manager was constructed by this scanner but does not
                    # match the persisted worker/backend binding. Never use it
                    # to clean native state; retain only its own stop operation.
                    rejected_manager = manager
                    def stop_rejected_manager(exact_manager=rejected_manager) -> bool:
                        stop = getattr(exact_manager, "stop", None)
                        if not callable(stop):
                            return False
                        try:
                            stop()
                            return True
                        except Exception:
                            return False
                    if not stop_rejected_manager():
                        _retain_durable_cleanup(
                            snapshot.episode_id,
                            _RetainedCleanup(
                                stop_rejected_manager, channel_uuid=snapshot.channel_uuid,
                            ),
                            channel_uuid=snapshot.channel_uuid,
                        )
                    manager = None
                    pending += 1
                    continue
                if registration.status == "registered":
                    ack = NativeClientRegistrationAck(
                        channel_uuid=registration.channel_uuid,
                        episode_id=registration.episode_id,
                        client_id=registration.client_id,
                        local_worker_id=registration.local_worker_id,
                        redis_backend_signature=registration.backend_signature,
                        connected_at=registration.connected_at,
                        projection_version=registration.projection_version,
                        user_agent=registration.user_agent,
                        client_ip=registration.client_ip,
                    )
                else:
                    ack = _recover_registration_ack(
                        redis_text, opening, client_id=registration.client_id,
                        local_worker_id=registration.local_worker_id,
                        user_agent=registration.user_agent, client_ip=registration.client_ip,
                        backend_signature=registration.backend_signature,
                    )
                if ack is None:
                    if registration.status == "registered":
                        from .logging_utils import error

                        error(
                            "sharing_registration_failed",
                            callsite="attempt_reconciliation",
                        )
                    def clean_unregistered(
                        exact_opening=opening,
                        exact_catalog=catalog_generation,
                        exact_channel=snapshot.channel_uuid,
                        exact_client=registration.client_id,
                    ) -> bool:
                        fenced = fence_unregistered_opening(
                            redis_text, exact_opening,
                            catalog_generation=exact_catalog,
                        )
                        absent = fenced and native_registration_is_absent(
                            redis_text, channel_uuid=exact_channel,
                            client_id=exact_client,
                        )
                        return bool(absent and close_fenced_unregistered_opening(
                            redis_text, exact_opening,
                            catalog_generation=exact_catalog,
                            client_id=exact_client,
                        ))

                    closed = clean_unregistered()
                    if not closed:
                        retain_retry(clean_unregistered)
                        pending += 1
                        continue
                    cleanup_progress["native_done"] = True
                else:
                    def clean_registered(
                        exact_ack=ack, exact_episode=snapshot.episode_id,
                        exact_manager=manager,
                    ) -> bool:
                        return cleanup_borrowed_client(
                            redis_text, exact_ack, episode_id=exact_episode,
                            manager=exact_manager,
                        )

                    if not clean_registered():
                        # Preserve the exact local manager and ACK until Redis
                        # confirms removal; constructing another manager on
                        # every request would leave heartbeat workers behind.
                        retained_ack = ack
                        retained_manager = manager
                        exact_snapshot = snapshot

                        def retry_registered(
                            exact_ack=retained_ack,
                            exact_manager=retained_manager,
                            exact_episode=exact_snapshot.episode_id,
                        ) -> bool:
                            return cleanup_borrowed_client(
                                redis_text, exact_ack,
                                episode_id=exact_episode,
                                manager=exact_manager,
                            )

                        retain_retry(retry_registered, exact_manager=retained_manager)
                        pending += 1
                        continue
                    cleanup_progress["native_done"] = True
            if manager is not None:
                manager.stop()
                cleanup_progress["manager_stopped"] = True
            current = ledger.read(snapshot.channel_uuid)
            if current.episode_id != snapshot.episode_id:
                pending += 1
                continue
            if current.phase != "closed":
                ledger.revoke(
                    current.episode_id, expected_version=current.version,
                    reason="closed", cleanup_acknowledged=True,
                )
            completed += 1
            manager = None
        except Exception:
            pending += 1
            if manager is not None and owns_manager:
                # Preserve an exact manager across transient scanner failures.
                # A later scan can recover registration from the durable row;
                # this process-local owner must not be silently leaked meanwhile.
                exact_manager = manager
                exact_snapshot = snapshot
                exact_registration = registration

                def retry_after_exception(
                    exact_manager=exact_manager,
                    exact_snapshot=exact_snapshot,
                    exact_registration=exact_registration,
                    exact_opening=opening,
                    exact_catalog=catalog_generation,
                    exact_close=close_sqlite_after_ack,
                    exact_progress=cleanup_progress,
                ) -> bool:
                    try:
                        if not exact_progress["native_done"]:
                            recovered_ack = _recover_registration_ack(
                                redis_text,
                                replace(
                                    exact_snapshot, phase="opening",
                                    version=(exact_registration.opening_version
                                             if exact_registration is not None
                                             else exact_snapshot.version),
                                    connected_at=None, last_viewer_left_at=None,
                                    return_deadline=None,
                                    client_id=(exact_registration.client_id
                                               if exact_registration is not None
                                               else exact_snapshot.client_id),
                                ),
                                client_id=(exact_registration.client_id
                                           if exact_registration is not None
                                           else str(exact_snapshot.client_id)),
                                local_worker_id=(exact_registration.local_worker_id
                                                 if exact_registration is not None else ""),
                                user_agent=(exact_registration.user_agent
                                            if exact_registration is not None else None),
                                client_ip=(exact_registration.client_ip
                                           if exact_registration is not None else None),
                                backend_signature=(exact_registration.backend_signature
                                                   if exact_registration is not None else ""),
                            ) if exact_registration is not None else None
                            if recovered_ack is not None:
                                native_done = cleanup_borrowed_client(
                                    redis_text, recovered_ack,
                                    episode_id=exact_snapshot.episode_id,
                                    manager=exact_manager,
                                )
                            else:
                                native_done = (
                                    fence_unregistered_opening(
                                        redis_text, exact_opening,
                                        catalog_generation=exact_catalog,
                                    )
                                    and native_registration_is_absent(
                                        redis_text,
                                        channel_uuid=exact_snapshot.channel_uuid,
                                        client_id=str(exact_snapshot.client_id),
                                    )
                                    and close_fenced_unregistered_opening(
                                        redis_text, exact_opening,
                                        catalog_generation=exact_catalog,
                                        client_id=str(exact_snapshot.client_id),
                                    )
                                )
                            if not native_done:
                                return False
                            exact_progress["native_done"] = True
                        if not exact_progress["manager_stopped"]:
                            exact_manager.stop()
                            exact_progress["manager_stopped"] = True
                        return exact_close()
                    except Exception:
                        return False

                _retain_durable_cleanup(
                    exact_snapshot.episode_id,
                    _RetainedCleanup(
                        retry_after_exception, channel_uuid=exact_snapshot.channel_uuid,
                    ),
                    channel_uuid=exact_snapshot.channel_uuid,
                )
                manager = None
    return completed, pending


class BorrowedSessionStream:
    """Ensure a response closed before its first iteration still releases ownership."""

    def __init__(self, session: BorrowedReaderSession):
        self.session = session
        self._iterator = None
        self._closed = False

    def __iter__(self):
        if self._closed:
            return iter(())
        if self._iterator is None:
            self._iterator = iter(self.session.iter_chunks())
        return self._iterator

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        iterator = self._iterator
        if iterator is not None:
            close = getattr(iterator, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
        try:
            cleaned = self.session.close() is True
        except Exception:
            cleaned = False
        if not cleaned:
            retain_session_cleanup_pending(self.session)


def prepare_borrow_opening(
    redis: Any,
    intent_ledger: SharingIntentLedger,
    episode_ledger: BorrowEpisodeLedger,
    token: SharingIntentToken,
    target: BorrowTarget,
    *,
    lease_value: str,
    catalog_generation: str,
    return_seconds: int,
    minimum_switch_seconds: int,
    last_switch_at: float | None,
    capability_secret: bytes,
    capability_ttl_seconds: int = 90,
    fresh_validate: Callable[[SharingIntentToken], bool],
    clock: Callable[[], float] = time.time,
) -> tuple[bool, PreparedBorrowOpening | None]:
    """Claim an exact ATTACH intent, project its opening, and sign one route ticket.

    The caller still must consume the ticket through the recorder's private HTTP
    route and wait for the native registration/episode activation ACK before it
    treats the borrow as active. No URL resolution or manager-map mutation is
    performed here.
    """
    if (
        not isinstance(intent_ledger, SharingIntentLedger)
        or not isinstance(episode_ledger, BorrowEpisodeLedger)
        or not isinstance(token, SharingIntentToken)
        or token.action is not SharingAction.ATTACH_BORROW
        or not isinstance(target, BorrowTarget)
        or target.connection_identity != token.target_identity
        or not target.eligible_for_new_borrow(token.channel_uuid)
        or not isinstance(lease_value, str) or not lease_value
        or not isinstance(catalog_generation, str) or not catalog_generation
        or type(return_seconds) is not int or not 0 <= return_seconds <= 3600
        or type(minimum_switch_seconds) is not int
        or not 0 <= minimum_switch_seconds <= 3600
        or type(capability_ttl_seconds) is not int
        or not 1 <= capability_ttl_seconds <= 120
        or not isinstance(capability_secret, bytes) or len(capability_secret) < 32
        or not callable(fresh_validate) or not callable(clock)
    ):
        return False, None

    def dispatch(exact_token: SharingIntentToken) -> PreparedBorrowOpening:
        now = clock()
        if (
            isinstance(now, bool)
            or not isinstance(now, (int, float))
            or not math.isfinite(float(now))
        ):
            raise BorrowEpisodeRejected("sharing clock is invalid")
        if exact_token.target_identity != target.connection_identity:
            raise BorrowEpisodeRejected("borrow target changed before opening")
        previous = None
        try:
            previous = episode_ledger.read(exact_token.channel_uuid)
        except BorrowEpisodeRejected:
            pass
        episode = episode_ledger.begin_open(
            exact_token,
            lease_digest=hashlib.sha256(lease_value.encode("utf-8")).hexdigest(),
            identity=target.connection_identity,
            client_id=f"catchuparr-client-{uuid.uuid4().hex}",
            expected_version=previous.version if previous is not None else None,
        )
        projected = False
        try:
            projected = project_opening_episode(
                redis,
                episode,
                lease_value=lease_value,
                catalog_generation=catalog_generation,
                return_seconds=return_seconds,
                minimum_switch_seconds=minimum_switch_seconds,
                last_switch_at=last_switch_at,
            )
            if not projected:
                from .logging_utils import error

                error("sharing_projection_failed", callsite="opening_projection")
                raise BorrowEpisodeRejected("native opening projection was not acknowledged")
            capability = sign_borrow_capability(
                secret=capability_secret,
                channel_uuid=episode.channel_uuid,
                episode_id=episode.episode_id,
                user_id="0",
                configuration_generation=episode.configuration_generation,
                catalog_generation=catalog_generation,
                control_generation=episode.control_generation,
                recorder_fence=episode.recorder_fence,
                intent_epoch=episode.intent_epoch,
                connection_identity=episode.identity,
                episode_version=episode.version,
                expires_at=float(now) + capability_ttl_seconds,
                nonce=episode.client_id.removeprefix("catchuparr-client-"),
            )
            # A final coordinator proof closes the gap between token claim and
            # ticket return. The route performs its own stricter Redis CAS.
            if fresh_validate(exact_token) is not True:
                raise BorrowEpisodeRejected("borrow intent became stale during opening")
            return PreparedBorrowOpening(
                episode.episode_id, episode.version, episode.client_id, capability,
            )
        except Exception as exc:
            # The opening is not retried. Close only after exact native evidence
            # says the projection and client resources are absent.
            closed = False
            if projected or episode.phase == "opening":
                try:
                    fenced = fence_unregistered_opening(
                        redis, episode, catalog_generation=catalog_generation,
                    )
                    absent = fenced and native_registration_is_absent(
                        redis,
                        channel_uuid=episode.channel_uuid,
                        client_id=episode.client_id or "",
                    )
                    closed = bool(
                        absent and close_fenced_unregistered_opening(
                            redis, episode,
                            catalog_generation=catalog_generation,
                            client_id=episode.client_id or "",
                        )
                    )
                except Exception:
                    closed = False
            if not closed:
                from .logging_utils import error

                error("sharing_cleanup_pending", exc, callsite="opening_cleanup")
            try:
                episode_ledger.revoke(
                    episode.episode_id,
                    expected_version=episode.version,
                    reason="closed" if closed else "cleanup_pending",
                    cleanup_acknowledged=closed,
                )
            except Exception:
                pass
            raise

    claimed, prepared = intent_ledger.run_if_current(
        token,
        fresh_validate=fresh_validate,
        side_effect=dispatch,
    )
    if not claimed or not isinstance(prepared, PreparedBorrowOpening):
        return False, None
    return True, prepared


def _native_source_catalog(channel_uuid: str) -> tuple[tuple[NativeAssignedSource, ...], str]:
    """Materialize exact assignment/profile facts outside the media iterator."""
    from apps.channels.models import Channel, ChannelStream

    channel = Channel.objects.filter(uuid=channel_uuid).first()
    if channel is None:
        raise BorrowEpisodeRejected("sharing channel is no longer assigned")
    input_profile = channel.get_stream_profile()
    input_profile_id = str(getattr(input_profile, "id", "") or "")
    is_redirect = getattr(input_profile, "is_redirect", None)
    if not input_profile_id or not callable(is_redirect):
        raise BorrowEpisodeRejected("input profile compatibility is unavailable")
    try:
        redirect_input = is_redirect() is True
    except Exception:
        raise BorrowEpisodeRejected("input profile compatibility is unavailable") from None

    rows: list[NativeAssignedSource] = []
    assignments = ChannelStream.objects.filter(channel=channel).select_related(
        "stream", "stream__m3u_account",
    ).order_by("order", "id")
    for assignment in assignments:
        stream = assignment.stream
        account = getattr(stream, "m3u_account", None)
        account_id = str(getattr(stream, "m3u_account_id", "") or "")
        stream_id = str(getattr(stream, "id", "") or "")
        if account is None or not account_id or not stream_id:
            continue
        account_active = getattr(account, "is_active", None)
        if type(account_active) is not bool:
            raise BorrowEpisodeRejected("M3U account status is unavailable")
        profiles_manager = getattr(account, "profiles", None)
        if profiles_manager is None or not callable(getattr(profiles_manager, "all", None)):
            raise BorrowEpisodeRejected("M3U profile assignments are unavailable")
        try:
            profiles = list(profiles_manager.all().order_by("id"))
        except Exception:
            raise BorrowEpisodeRejected("M3U profile assignments are unavailable") from None
        for profile in profiles:
            profile_id = str(getattr(profile, "id", "") or "")
            active = getattr(profile, "is_active", None)
            if not profile_id or type(active) is not bool:
                raise BorrowEpisodeRejected("M3U profile status is unavailable")
            rows.append(NativeAssignedSource(
                channel_uuid=channel_uuid,
                stream_id=stream_id,
                account_id=account_id,
                m3u_profile_id=profile_id,
                input_profile_id=input_profile_id,
                assigned=True,
                account_active=account_active,
                profile_active=active,
                media_compatible=not redirect_input,
                supported_worker_kind=True,
                redirect_channel=redirect_input,
                vod_or_preview=False,
            ))
    if not rows:
        raise BorrowEpisodeRejected("no fully described M3U source assignments remain")
    canonical = [
        [row.channel_uuid, row.stream_id, row.account_id, row.m3u_profile_id,
         row.input_profile_id, row.assigned, row.account_active, row.profile_active,
         row.media_compatible, row.supported_worker_kind, row.redirect_channel,
         row.vod_or_preview]
        for row in rows
    ]
    digest = hashlib.sha256(
        json.dumps(canonical, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    ).hexdigest()
    return tuple(rows), digest


def open_borrow_session(capability: SignedBorrowCapability) -> BorrowedReaderSession:
    """Open a signed route only while its exact current native grant is valid."""
    if not isinstance(capability, SignedBorrowCapability):
        raise BorrowEpisodeRejected("signed sharing capability is invalid")
    if not capability.verify(
        _django_secret(), user_id=capability.user_id, now=time.time(),
    ):
        raise BorrowEpisodeRejected("signed sharing capability is invalid or expired")
    retry_pending_open_cleanups(channel_uuid=capability.channel_uuid)
    from core.utils import RedisClient

    from .configuration import load_applied_state
    from .recorder_proxy import configuration_generation
    from .runtime import load_config

    redis_text = RedisClient.get_client()
    redis_binary = RedisClient.get_buffer()
    if redis_text is None or redis_binary is None:
        raise BorrowEpisodeRejected("sharing Redis backend is unavailable")
    active, control = load_applied_state()
    if active is None or control.paused:
        raise BorrowEpisodeRejected("sharing recorder admission is closed")
    if configuration_generation(active) != capability.configuration_generation:
        raise BorrowEpisodeRejected("sharing configuration changed")
    if control.generation != capability.control_generation:
        raise BorrowEpisodeRejected("sharing control generation changed")
    sharing = active.get("connection_sharing")
    setting = sharing.get(capability.channel_uuid) if isinstance(sharing, dict) else None
    if not isinstance(setting, dict) or setting.get("enabled") is not True:
        raise BorrowEpisodeRejected("connection sharing is disabled")

    config = load_config(active_snapshot=active)
    if config is None:
        raise BorrowEpisodeRejected("sharing archive configuration is unavailable")
    ledger = BorrowEpisodeLedger(Path(config.archive_root) / ".catchuparr-sharing.sqlite3")
    retry_durable_open_cleanups(ledger, redis_text, now=time.time())
    episode = ledger.read(capability.channel_uuid)
    if (
        episode.phase != "opening"
        or episode.episode_id != capability.episode_id
        or episode.version != capability.episode_version
        or episode.identity != capability.connection_identity
        or episode.configuration_generation != capability.configuration_generation
        or episode.control_generation != capability.control_generation
        or episode.recorder_fence != capability.recorder_fence
        or episode.intent_epoch != capability.intent_epoch
    ):
        raise BorrowEpisodeRejected("sharing opening no longer matches its durable episode")
    nonce = capability.nonce
    if len(nonce) != 32 or any(char not in "0123456789abcdef" for char in nonce):
        raise BorrowEpisodeRejected("sharing opening nonce is invalid")
    client_id = f"catchuparr-client-{nonce}"
    if episode.client_id != client_id:
        raise BorrowEpisodeRejected("sharing client differs from the persisted opening")

    lease_value = redis_text.get(f"catchuparr:recorder:{capability.channel_uuid}")
    if not isinstance(lease_value, str) or not lease_value:
        raise BorrowEpisodeRejected("recorder lease is unavailable")
    lease_digest = hashlib.sha256(lease_value.encode("utf-8")).hexdigest()
    if lease_digest != episode.lease_digest:
        raise BorrowEpisodeRejected("recorder lease changed")

    try:
        assigned_sources, current_catalog_generation = _native_source_catalog(
            capability.channel_uuid
        )
        if current_catalog_generation != capability.catalog_generation:
            raise BorrowEpisodeRejected("source catalog changed")
        observation = observe_native_connection(
            redis_text, capability.channel_uuid,
            catalog_generation=current_catalog_generation,
            assigned_sources=assigned_sources,
            observed_at=time.time(),
        )
    finally:
        try:
            from django.db import close_old_connections

            close_old_connections()
        except Exception:
            pass
    if (
        observation is None
        or observation.target.connection_identity != capability.connection_identity
        or not observation.ordinary_viewer_ids
    ):
        raise BorrowEpisodeRejected("native source has no current ordinary viewer authority")

    from apps.proxy.live_proxy.client_manager import ClientManager
    from apps.proxy.live_proxy.input.buffer import StreamBuffer

    # A private manager is created for this route request and is never inserted
    # into ProxyServer's channel maps. Its worker ID is an opaque local reader
    # identity, not the native source owner's ID.
    worker_id = f"catchuparr-reader-{uuid.uuid4().hex}"
    secret = _django_secret()
    session = BorrowedReaderSession.open(
        capability=capability,
        capability_secret=secret,
        authenticated_user_id=capability.user_id,
        now=time.time(),
        episode=episode,
        lease_value=lease_value,
        catalog_generation=current_catalog_generation,
        ordinary_viewer_ids=observation.ordinary_viewer_ids,
        redis_text=redis_text,
        redis_binary=redis_binary,
        existing_manager=None,
        manager_type=ClientManager,
        worker_id=worker_id,
        buffer_type=StreamBuffer,
        assigned_sources=assigned_sources,
        client_id=client_id,
        client_ip="127.0.0.1",
        user_agent="Catchuparr read-only sharing reader",
        ledger=ledger,
    )
    intent_ledger = SharingIntentLedger(Path(config.archive_root) / ".catchuparr-sharing.sqlite3")
    if not acknowledge_opened_borrow(
        capability, intent_ledger, ledger, target=observation.target,
        settings=SharingSettings(
            enabled=True,
            live_stability_seconds=setting["live_stability_seconds"],
            last_viewer_return_seconds=setting["last_viewer_return_seconds"],
            min_switch_interval_seconds=setting["min_switch_interval_seconds"],
        ),
        # The native REGISTER Lua ACK is the committed ATTACH point. Use its
        # server timestamp for the planner clock so native grace and durable
        # cooldown state share the same reference after restart.
        now=float(session.ack.connected_at),
    ):
        try:
            cleaned = session.close() is True
        except Exception:
            cleaned = False
        if not cleaned:
            retain_session_cleanup_pending(session)
        raise BorrowEpisodeRejected("borrow activation could not be committed to the recorder planner")
    return session


def _django_secret() -> bytes:
    from django.conf import settings

    secret = getattr(settings, "SECRET_KEY", "")
    if not isinstance(secret, str) or not secret:
        raise BorrowEpisodeRejected("sharing signing key is unavailable")
    return secret.encode("utf-8")
