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

    def poll(self, *, active: dict[str, Any], control_generation: int) -> PreparedBorrowOpening | None:
        """Persist cooldown observations and claim an ATTACH only at its deadline."""
        latest = self.intent_ledger.read(self.channel_uuid)
        if latest is not None and latest.revision > self.state_record.revision:
            self.state_record = latest
        try:
            from .adapters.sharing_native import read_native_episode_grant

            live_episode = read_native_episode_grant(self.redis, self.channel_uuid)
            if live_episode is not None and live_episode.phase in {"opening", "active", "wait_return", "closing"}:
                return None
        except Exception:
            return None
        if (
            self.state_record.intent is not None
            or control_generation != self.control_generation
            or active.get("connection_sharing", {}).get(self.channel_uuid, {}).get("enabled") is not True
        ):
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
        decision = observe(self.state_record.state, observation)
        if decision.action is SharingAction.NOOP and decision.state == self.state_record.state:
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


class _RetainedCleanup:
    """Keep a task-local manager and exact retry callback alive across requests."""

    def __init__(self, retry: Callable[[], bool]) -> None:
        self._retry = retry

    def retry_cleanup(self) -> bool:
        return self._retry() is True


def _retain_durable_cleanup(episode_id: str, operation: Any) -> None:
    with _pending_open_cleanup_lock:
        _pending_open_cleanups[episode_id] = operation


def retain_open_cleanup_pending(error: BaseException) -> None:
    """Keep the exact open-failure cleanup owner reachable in this web process."""
    from .adapters.sharing_borrow_route import BorrowOpenCleanupPending

    if not isinstance(error, BorrowOpenCleanupPending):
        return
    episode = getattr(error, "_episode", None)
    episode_id = getattr(episode, "episode_id", None)
    if not isinstance(episode_id, str) or not episode_id:
        return
    with _pending_open_cleanup_lock:
        _pending_open_cleanups[episode_id] = error


def retain_session_cleanup_pending(session: BorrowedReaderSession) -> None:
    """Retain an exact session whose HTTP close has not been acknowledged."""
    ack = getattr(session, "ack", None)
    episode_id = getattr(ack, "episode_id", None)
    if not isinstance(episode_id, str) or not episode_id:
        return
    with _pending_open_cleanup_lock:
        _pending_open_cleanups[episode_id] = session


def retry_pending_open_cleanups() -> tuple[int, int]:
    """Retry retained exact cleanup handles; never infer closure from a timeout."""
    completed = 0
    pending = 0
    with _pending_open_cleanup_lock:
        items = tuple(_pending_open_cleanups.items())
    for episode_id, operation in items:
        try:
            done = operation.retry_cleanup() is True
        except Exception:
            done = False
        with _pending_open_cleanup_lock:
            if _pending_open_cleanups.get(episode_id) is not operation:
                continue
            if done:
                del _pending_open_cleanups[episode_id]
                completed += 1
            else:
                pending += 1
    return completed, pending


def retry_durable_open_cleanups(ledger: BorrowEpisodeLedger, redis_text: Any, *,
                                now: float | None = None, stale_after: float = 120.0) -> tuple[int, int]:
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
    )
    from .adapters.sharing_reader import redis_backend_signature

    observed_at = time.time() if now is None else now
    if (isinstance(observed_at, bool) or not isinstance(observed_at, (int, float))
            or not math.isfinite(float(observed_at))
            or isinstance(stale_after, bool) or not isinstance(stale_after, (int, float))
            or not math.isfinite(float(stale_after)) or stale_after < 0):
        return 0, 0
    try:
        candidates = ledger.registration_recovery_candidates(
            stale_opening_before=float(observed_at) - float(stale_after),
        )
        text_backend = redis_backend_signature(redis_text)
    except Exception:
        return 0, 1
    with _pending_open_cleanup_lock:
        retained_ids = set(_pending_open_cleanups)
    completed = pending = 0
    for snapshot, registration in candidates:
        if snapshot.episode_id in retained_ids:
            continue
        manager = None
        owns_manager = False
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

                def retry_owned_cleanup(
                    exact_retry=retry_native,
                    exact_manager=exact_manager,
                    exact_close=close_sqlite_after_ack,
                ) -> bool:
                    if exact_retry() is not True:
                        return False
                    stop = getattr(exact_manager, "stop", None)
                    if not callable(stop):
                        return False
                    try:
                        stop()
                    except Exception:
                        return False
                    return exact_close()

                _retain_durable_cleanup(
                    exact_snapshot.episode_id, _RetainedCleanup(retry_owned_cleanup),
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
                            _RetainedCleanup(stop_rejected_manager),
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
            if manager is not None:
                manager.stop()
                manager = None
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
                ) -> bool:
                    try:
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
                        exact_manager.stop()
                        return exact_close()
                    except Exception:
                        return False

                _retain_durable_cleanup(
                    exact_snapshot.episode_id,
                    _RetainedCleanup(retry_after_exception),
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
        except Exception:
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
    retry_pending_open_cleanups()
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
        now=time.time(),
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
