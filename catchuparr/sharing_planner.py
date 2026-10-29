"""Pure planner for fenced, coalesced recorder connection-sharing transitions.

This module only compares observations and timestamps. It does not inspect or
mutate Dispatcharr, Redis, native clients, buffers, or provider connections.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from enum import Enum
from typing import Any, Mapping


class SharingPhase(str, Enum):
    DEDICATED = "dedicated"
    WAIT_LIVE = "wait_live"
    BORROWED = "borrowed"
    WAIT_RETURN = "wait_return"
    PAUSED = "paused"
    STOPPED = "stopped"
    FAILED = "failed"


class SharingAction(str, Enum):
    NOOP = "noop"
    WAIT = "wait"
    ATTACH_BORROW = "attach_borrow"
    RETURN_TO_ALLOWED = "return_to_allowed"
    PAUSE_NO_ALLOWED_SOURCE = "pause_no_allowed_source"
    STOP = "stop"


@dataclass(frozen=True)
class SharingSettings:
    enabled: bool = False
    live_stability_seconds: int = 5
    last_viewer_return_seconds: int = 30
    min_switch_interval_seconds: int = 60

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool:
            raise ValueError("enabled must be boolean")
        for name in (
            "live_stability_seconds",
            "last_viewer_return_seconds",
            "min_switch_interval_seconds",
        ):
            value = getattr(self, name)
            if type(value) is not int or not 0 <= value <= 3600:
                raise ValueError(f"{name} must be an integer from 0 to 3600")


@dataclass(frozen=True)
class BorrowTarget:
    """Sanitized native identity evidence; deliberately contains no URL/token."""

    channel_uuid: str
    worker_uuid: str
    owner_lock_value: str
    metadata_owner: str
    stream_id: str
    account_id: str
    m3u_profile_id: str
    input_profile_id: str
    ordinary_client_ids: tuple[str, ...]
    owner_heartbeat_healthy: bool
    assigned_stream_matches: bool
    account_active: bool
    profile_active: bool
    media_compatible: bool
    useful_buffer_available: bool
    native_state_healthy: bool
    account_allowed_by_recording_policy: bool = True
    redirect_worker: bool = False
    vod_or_preview_worker: bool = False

    def __post_init__(self) -> None:
        for name in (
            "channel_uuid", "worker_uuid", "owner_lock_value", "metadata_owner",
            "stream_id", "account_id", "m3u_profile_id", "input_profile_id",
        ):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"{name} must be a non-empty string")
        if (
            not isinstance(self.ordinary_client_ids, tuple)
            or any(not isinstance(value, str) or not value for value in self.ordinary_client_ids)
            or len(set(self.ordinary_client_ids)) != len(self.ordinary_client_ids)
        ):
            raise ValueError("ordinary_client_ids must be unique non-empty strings")
        for name in (
            "owner_heartbeat_healthy", "assigned_stream_matches", "account_active",
            "profile_active", "media_compatible", "useful_buffer_available",
            "native_state_healthy", "account_allowed_by_recording_policy",
            "redirect_worker", "vod_or_preview_worker",
        ):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be boolean")

    @property
    def connection_identity(self) -> tuple[str, ...]:
        """Identity used to prove that an existing buffer has not changed."""
        return (
            self.channel_uuid,
            self.worker_uuid,
            self.owner_lock_value,
            self.metadata_owner,
            self.stream_id,
            self.account_id,
            self.m3u_profile_id,
            self.input_profile_id,
        )

    @property
    def stability_identity(self) -> tuple[str, ...]:
        """Keep stability across viewer-count changes on the same connection."""
        return self.connection_identity

    def healthy_existing_connection(self, channel_uuid: str) -> bool:
        """Checks reusable native identity without applying source-policy filters."""
        return (
            self.channel_uuid == channel_uuid
            and self.owner_lock_value == self.metadata_owner
            and self.owner_heartbeat_healthy
            and self.assigned_stream_matches
            and self.account_active
            and self.profile_active
            and self.media_compatible
            and self.useful_buffer_available
            and self.native_state_healthy
            and not self.redirect_worker
            and not self.vod_or_preview_worker
        )

    def eligible_for_new_borrow(self, channel_uuid: str) -> bool:
        """A new borrow additionally requires a real, ordinary live viewer."""
        return bool(self.ordinary_client_ids) and self.healthy_existing_connection(channel_uuid)


@dataclass(frozen=True)
class SharingObservation:
    channel_uuid: str
    now: float
    configuration_generation: str
    control_generation: int
    recorder_fence: int
    lease_is_current: bool
    settings: SharingSettings
    target: BorrowTarget | None = None
    recording_enabled: bool = True
    schedule_open: bool = True
    explicit_stop: bool = False
    recorder_failure: bool = False
    dedicated_source_available: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.channel_uuid, str) or not self.channel_uuid:
            raise ValueError("channel_uuid must be a non-empty string")
        if isinstance(self.now, bool) or not isinstance(self.now, (int, float)):
            raise ValueError("now must be a finite timestamp")
        if not math.isfinite(float(self.now)):
            raise ValueError("now must be a finite timestamp")
        if not isinstance(self.configuration_generation, str) or not self.configuration_generation:
            raise ValueError("configuration_generation must be a non-empty string")
        for name in ("control_generation", "recorder_fence"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        for name in (
            "lease_is_current", "recording_enabled", "schedule_open",
            "explicit_stop", "recorder_failure", "dedicated_source_available",
        ):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be boolean")
        if self.target is not None and not isinstance(self.target, BorrowTarget):
            raise ValueError("target must be a BorrowTarget")
        if not isinstance(self.settings, SharingSettings):
            raise ValueError("settings must be SharingSettings")


@dataclass(frozen=True)
class SharingState:
    channel_uuid: str
    configuration_generation: str
    control_generation: int
    recorder_fence: int
    phase: SharingPhase = SharingPhase.DEDICATED
    candidate_identity: tuple[str, ...] | None = None
    candidate_connection_identity: tuple[str, ...] | None = None
    candidate_since: float | None = None
    borrowed_identity: tuple[str, ...] | None = None
    last_viewer_left_at: float | None = None
    last_switch_at: float | None = None
    pending_action: SharingAction | None = None
    intent_epoch: int = 0

    def to_snapshot(self) -> dict[str, Any]:
        """Persist only cooldown evidence; never persist a borrowed binding."""
        return {
            "version": 2,
            "channel_uuid": self.channel_uuid,
            "configuration_generation": self.configuration_generation,
            "control_generation": self.control_generation,
            "phase": self.phase.value,
            "candidate_identity": list(self.candidate_identity) if self.candidate_identity else None,
            "candidate_connection_identity": (
                list(self.candidate_connection_identity)
                if self.candidate_connection_identity else None
            ),
            "candidate_since": self.candidate_since,
            "last_viewer_left_at": self.last_viewer_left_at,
            "last_switch_at": self.last_switch_at,
            "intent_epoch": self.intent_epoch,
        }


@dataclass(frozen=True)
class SharingDecision:
    action: SharingAction
    state: SharingState
    deadline: float | None = None
    reason: str = ""


def initial_state(observation: SharingObservation) -> SharingState:
    return SharingState(
        channel_uuid=observation.channel_uuid,
        configuration_generation=observation.configuration_generation,
        control_generation=observation.control_generation,
        recorder_fence=observation.recorder_fence,
    )


def restore_state(
    snapshot: Mapping[str, Any],
    *,
    channel_uuid: str,
    configuration_generation: str,
    control_generation: int,
    recorder_fence: int,
    now: float,
) -> SharingState:
    """Restore valid cooldown clocks, but always re-observe any borrowed route."""
    if (
        not isinstance(channel_uuid, str)
        or not channel_uuid
        or not isinstance(configuration_generation, str)
        or not configuration_generation
        or type(control_generation) is not int
        or control_generation < 0
        or type(recorder_fence) is not int
        or recorder_fence < 0
        or isinstance(now, bool)
        or not isinstance(now, (int, float))
        or not math.isfinite(float(now))
    ):
        raise ValueError("current sharing fence and timestamp must be valid")
    now = float(now)
    blank = SharingState(channel_uuid, configuration_generation, control_generation, recorder_fence)
    if not isinstance(snapshot, Mapping):
        return blank
    expected_v1 = {
        "version", "channel_uuid", "configuration_generation", "control_generation",
        "phase", "candidate_identity", "candidate_connection_identity", "candidate_since",
        "last_viewer_left_at", "last_switch_at",
    }
    expected_v2 = expected_v1 | {"intent_epoch"}
    snapshot_version = snapshot.get("version")
    if (
        (snapshot_version == 1 and set(snapshot) != expected_v1)
        or (snapshot_version == 2 and set(snapshot) != expected_v2)
        or snapshot_version not in {1, 2}
        or type(snapshot.get("version")) is not int
        or snapshot.get("channel_uuid") != channel_uuid
        or snapshot.get("configuration_generation") != configuration_generation
        or type(snapshot.get("control_generation")) is not int
        or snapshot.get("control_generation") != control_generation
        or not isinstance(snapshot.get("phase"), str)
        or snapshot.get("phase") not in {phase.value for phase in SharingPhase}
    ):
        return blank

    def valid_time(value: Any, *, allow_future: bool = False) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        numeric = float(value)
        if not math.isfinite(numeric) or now - numeric > 3600:
            return None
        if numeric > now:
            if not allow_future:
                return None
            # Clamp all future skew, including unreasonable values, to now so
            # the configured minimum interval still starts at this observation.
            return now
        return numeric

    identity = snapshot.get("candidate_identity")
    connection_identity = snapshot.get("candidate_connection_identity")
    if identity is not None and (
        not isinstance(identity, list)
        or not identity
        or any(not isinstance(value, str) or not value for value in identity)
    ):
        identity = None
    if connection_identity is not None and (
        not isinstance(connection_identity, list)
        or not connection_identity
        or any(not isinstance(value, str) or not value for value in connection_identity)
    ):
        connection_identity = None
    candidate_since = valid_time(snapshot.get("candidate_since"))
    last_viewer_left = valid_time(snapshot.get("last_viewer_left_at"))
    last_switch = valid_time(snapshot.get("last_switch_at"), allow_future=True)
    intent_epoch = snapshot.get("intent_epoch", 0)
    if type(intent_epoch) is not int or intent_epoch < 0:
        intent_epoch = 0
    if identity is None or connection_identity is None or candidate_since is None:
        identity = None
        connection_identity = None
        candidate_since = None
    # The old fence is intentionally not restored as authority. The current
    # caller supplies a fresh lease fence, and no borrowed identity survives.
    return SharingState(
        channel_uuid=channel_uuid,
        configuration_generation=configuration_generation,
        control_generation=control_generation,
        recorder_fence=recorder_fence,
        phase=SharingPhase.DEDICATED,
        candidate_identity=tuple(identity) if identity else None,
        candidate_connection_identity=(
            tuple(connection_identity) if connection_identity else None
        ),
        candidate_since=candidate_since,
        borrowed_identity=None,
        last_viewer_left_at=last_viewer_left,
        last_switch_at=last_switch,
        intent_epoch=intent_epoch,
    )


def observe(state: SharingState | None, observation: SharingObservation) -> SharingDecision:
    """Coalesce one observation into one deadline or one fenced transition intent."""
    current = state or initial_state(observation)
    now = float(observation.now)
    if not observation.lease_is_current:
        return SharingDecision(SharingAction.NOOP, current, reason="stale recorder lease")

    invalidated_borrow = False
    if (
        current.channel_uuid != observation.channel_uuid
        or current.configuration_generation != observation.configuration_generation
        or current.control_generation != observation.control_generation
    ):
        switched_at = current.last_switch_at
        invalidated_borrow = current.borrowed_identity is not None
        current = replace(initial_state(observation), last_switch_at=switched_at)
    elif current.recorder_fence != observation.recorder_fence:
        # A new lease cannot inherit a borrowed route or pending intent. Preserve
        # only a still-relevant successful-switch clock to avoid cooldown resets.
        invalidated_borrow = current.borrowed_identity is not None
        current = replace(
            current,
            recorder_fence=observation.recorder_fence,
            phase=SharingPhase.DEDICATED,
            candidate_identity=None,
            candidate_connection_identity=None,
            candidate_since=None,
            borrowed_identity=None,
            pending_action=None,
        )

    if observation.explicit_stop:
        stopped = replace(current, phase=SharingPhase.STOPPED, pending_action=None)
        return SharingDecision(SharingAction.STOP, stopped, reason="explicit stop bypasses cooldowns")
    if current.phase == SharingPhase.STOPPED:
        return SharingDecision(SharingAction.NOOP, current, reason="channel remains stopped")
    if not observation.recording_enabled or not observation.schedule_open:
        stopped = replace(current, phase=SharingPhase.STOPPED, pending_action=None)
        return SharingDecision(SharingAction.STOP, stopped, reason="recording admission is closed")
    if observation.recorder_failure:
        failed = replace(
            current,
            phase=SharingPhase.FAILED,
            candidate_identity=None,
            candidate_connection_identity=None,
            candidate_since=None,
        )
        action = (
            SharingAction.RETURN_TO_ALLOWED
            if observation.dedicated_source_available
            else SharingAction.PAUSE_NO_ALLOWED_SOURCE
        )
        return _intent(
            failed,
            action,
            SharingPhase.FAILED,
            "failure bypasses cooldowns",
        )

    if invalidated_borrow:
        action = (
            SharingAction.RETURN_TO_ALLOWED
            if observation.dedicated_source_available
            else SharingAction.PAUSE_NO_ALLOWED_SOURCE
        )
        return _intent(
            current,
            action,
            SharingPhase.FAILED,
            "configuration, control, or lease fence invalidated the previous route",
        )

    if not observation.settings.enabled:
        if current.borrowed_identity is not None:
            action = (
                SharingAction.RETURN_TO_ALLOWED
                if observation.dedicated_source_available
                else SharingAction.PAUSE_NO_ALLOWED_SOURCE
            )
            return _intent(current, action, SharingPhase.FAILED, "sharing disabled")
        if current.phase == SharingPhase.PAUSED:
            if observation.dedicated_source_available:
                return _intent(
                    current,
                    SharingAction.RETURN_TO_ALLOWED,
                    SharingPhase.PAUSED,
                    "sharing disabled; an allowed source can resume recording",
                )
            return SharingDecision(
                SharingAction.NOOP,
                current,
                reason="sharing disabled; recorder remains paused without an allowed source",
            )
        if not observation.dedicated_source_available:
            return SharingDecision(
                SharingAction.NOOP,
                replace(
                    current,
                    phase=SharingPhase.PAUSED,
                    candidate_identity=None,
                    candidate_connection_identity=None,
                    candidate_since=None,
                    pending_action=None,
                ),
                reason="sharing disabled and no allowed source is available",
            )
        return SharingDecision(
            SharingAction.NOOP,
            replace(current, phase=SharingPhase.DEDICATED, pending_action=None),
            reason="sharing disabled",
        )

    target = observation.target
    healthy_target = bool(
        target is not None and target.healthy_existing_connection(observation.channel_uuid)
    )
    new_target = bool(
        healthy_target
        and target is not None
        and target.eligible_for_new_borrow(observation.channel_uuid)
    )

    if (
        current.phase == SharingPhase.FAILED
        and current.pending_action in {
            SharingAction.RETURN_TO_ALLOWED,
            SharingAction.PAUSE_NO_ALLOWED_SOURCE,
        }
    ):
        return SharingDecision(
            SharingAction.NOOP,
            current,
            reason="fail-closed return transition is already coalesced",
        )

    if current.phase == SharingPhase.FAILED and current.borrowed_identity is not None:
        action = (
            SharingAction.RETURN_TO_ALLOWED
            if observation.dedicated_source_available
            else SharingAction.PAUSE_NO_ALLOWED_SOURCE
        )
        return _intent(
            current,
            action,
            SharingPhase.FAILED,
            "failed return must complete before selecting another route",
        )

    if current.phase in {SharingPhase.BORROWED, SharingPhase.WAIT_RETURN} and current.borrowed_identity:
        if not healthy_target or target is None or target.connection_identity != current.borrowed_identity:
            action = (
                SharingAction.RETURN_TO_ALLOWED
                if observation.dedicated_source_available
                else SharingAction.PAUSE_NO_ALLOWED_SOURCE
            )
            return _intent(
                current,
                action,
                SharingPhase.FAILED,
                "borrowed identity is no longer valid",
            )
        if target.ordinary_client_ids:
            return SharingDecision(
                SharingAction.NOOP,
                replace(current, phase=SharingPhase.BORROWED, last_viewer_left_at=None, pending_action=None),
                reason="borrow remains attached to the same healthy native identity",
            )
        left_at = current.last_viewer_left_at
        if left_at is None:
            left_at = now
        deadline = _later_deadline(
            left_at,
            observation.settings.last_viewer_return_seconds,
            current.last_switch_at,
            observation.settings.min_switch_interval_seconds,
        )
        if now < deadline:
            waiting = replace(
                current,
                phase=SharingPhase.WAIT_RETURN,
                last_viewer_left_at=left_at,
                pending_action=None,
            )
            return SharingDecision(SharingAction.WAIT, waiting, deadline, "waiting for last viewer/minimum-switch cooldown")
        action = (
            SharingAction.RETURN_TO_ALLOWED
            if observation.dedicated_source_available
            else SharingAction.PAUSE_NO_ALLOWED_SOURCE
        )
        return _intent(current, action, SharingPhase.WAIT_RETURN, "last-viewer return deadline elapsed", left_at=left_at)

    if not new_target or target is None:
        if current.phase == SharingPhase.PAUSED:
            if observation.dedicated_source_available:
                return _intent(
                    current,
                    SharingAction.RETURN_TO_ALLOWED,
                    SharingPhase.PAUSED,
                    "allowed source is available; resume dedicated recording",
                )
            return SharingDecision(
                SharingAction.NOOP,
                replace(
                    current,
                    candidate_identity=None,
                    candidate_connection_identity=None,
                    candidate_since=None,
                    pending_action=None,
                ),
                reason="recorder remains paused until a viewer or allowed source appears",
            )
        if not observation.dedicated_source_available:
            return SharingDecision(
                SharingAction.NOOP,
                replace(
                    current,
                    phase=SharingPhase.PAUSED,
                    candidate_identity=None,
                    candidate_connection_identity=None,
                    candidate_since=None,
                    last_viewer_left_at=None,
                    pending_action=None,
                ),
                reason="no eligible live viewer or allowed source is available",
            )
        cleared = replace(
            current,
            phase=SharingPhase.DEDICATED,
            candidate_identity=None,
            candidate_connection_identity=None,
            candidate_since=None,
            last_viewer_left_at=None,
            pending_action=None,
        )
        return SharingDecision(SharingAction.NOOP, cleared, reason="no eligible existing live viewer connection")

    identity = target.stability_identity
    same_candidate = current.candidate_identity == identity
    since = current.candidate_since if same_candidate else None
    if since is None:
        since = now
    candidate_state = replace(
        current,
        candidate_identity=identity,
        candidate_connection_identity=target.connection_identity,
        candidate_since=since,
        last_viewer_left_at=None,
        pending_action=(current.pending_action if same_candidate else None),
    )
    deadline = _later_deadline(
        since,
        (0 if current.last_switch_at is None else observation.settings.live_stability_seconds),
        current.last_switch_at,
        observation.settings.min_switch_interval_seconds,
    )
    if now < deadline:
        waiting = replace(
            candidate_state,
            phase=SharingPhase.WAIT_LIVE,
        )
        return SharingDecision(SharingAction.WAIT, waiting, deadline, "waiting for stable live target/minimum-switch cooldown")
    return _intent(
        candidate_state,
        SharingAction.ATTACH_BORROW,
        SharingPhase.WAIT_LIVE,
        ("initial eligible viewer connection attaches immediately"
         if current.last_switch_at is None
         else "eligible existing live connection is stable"),
    )


def _later_deadline(
    base_at: float,
    base_delay: int,
    last_switch_at: float | None,
    minimum_interval: int,
) -> float:
    deadlines = []
    if base_delay:
        deadlines.append(base_at + base_delay)
    if minimum_interval and last_switch_at is not None:
        deadlines.append(last_switch_at + minimum_interval)
    return max(deadlines, default=base_at)


def _intent(
    state: SharingState,
    action: SharingAction,
    phase: SharingPhase,
    reason: str,
    *,
    left_at: float | None = None,
) -> SharingDecision:
    if state.pending_action == action:
        return SharingDecision(SharingAction.NOOP, state, reason="transition is already coalesced")
    pending = replace(
        state,
        phase=phase,
        pending_action=action,
        intent_epoch=state.intent_epoch + 1,
        last_viewer_left_at=(left_at if left_at is not None else state.last_viewer_left_at),
    )
    return SharingDecision(action, pending, reason=reason)


def resolve_transition(
    state: SharingState,
    action: SharingAction,
    *,
    succeeded: bool,
    now: float,
    current_state: SharingState,
    current_observation: SharingObservation,
    admitted_return_deadline: float | None = None,
    admitted_attach_deadline: float | None = None,
) -> SharingState:
    """Commit only successful route changes; failures never advance switch time."""
    if not isinstance(state, SharingState) or not isinstance(current_state, SharingState):
        raise ValueError("state must be SharingState")
    if not isinstance(action, SharingAction):
        raise ValueError("action must be SharingAction")
    if type(succeeded) is not bool:
        raise ValueError("succeeded must be boolean")
    if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(float(now)):
        raise ValueError("now must be a finite timestamp")
    if not isinstance(current_observation, SharingObservation):
        raise ValueError("current_observation must be a fresh SharingObservation")
    if admitted_return_deadline is not None and (
        action is not SharingAction.RETURN_TO_ALLOWED
        or isinstance(admitted_return_deadline, bool)
        or not isinstance(admitted_return_deadline, (int, float))
        or not math.isfinite(float(admitted_return_deadline))
    ):
        raise ValueError("admitted return deadline must be finite and bound to RETURN_TO_ALLOWED")
    if admitted_attach_deadline is not None and (
        action is not SharingAction.ATTACH_BORROW
        or isinstance(admitted_attach_deadline, bool)
        or not isinstance(admitted_attach_deadline, (int, float))
        or not math.isfinite(float(admitted_attach_deadline))
    ):
        raise ValueError("admitted attach deadline must be finite and bound to ATTACH_BORROW")
    if (
        not current_observation.lease_is_current
        or current_observation.channel_uuid != state.channel_uuid
        or current_observation.recorder_fence != state.recorder_fence
        or current_observation.configuration_generation != state.configuration_generation
        or current_observation.control_generation != state.control_generation
        or current_state.recorder_fence != state.recorder_fence
        or current_state.channel_uuid != state.channel_uuid
        or current_state.configuration_generation != state.configuration_generation
        or current_state.control_generation != state.control_generation
        or current_state.intent_epoch != state.intent_epoch
        or current_state.pending_action != action
        or state.pending_action != action
    ):
        return current_state
    if action == SharingAction.ATTACH_BORROW:
        target = current_observation.target
        if (
            not current_observation.settings.enabled
            or not current_observation.recording_enabled
            or not current_observation.schedule_open
            or current_observation.explicit_stop
            or target is None
            or not target.eligible_for_new_borrow(current_observation.channel_uuid)
            or target.connection_identity != current_state.candidate_connection_identity
            or target.stability_identity != current_state.candidate_identity
        ):
            return current_state
    if action in {
        SharingAction.RETURN_TO_ALLOWED,
        SharingAction.PAUSE_NO_ALLOWED_SOURCE,
    }:
        if (
            action == SharingAction.RETURN_TO_ALLOWED
            and not current_observation.dedicated_source_available
        ) or (
            action == SharingAction.PAUSE_NO_ALLOWED_SOURCE
            and current_observation.dedicated_source_available
        ):
            return current_state
        if (
            not current_observation.recording_enabled
            or not current_observation.schedule_open
            or current_observation.explicit_stop
        ):
            return current_state
        borrowed_identity = current_state.borrowed_identity
        current_target = current_observation.target
        ordinary_return_has_healthy_viewers = bool(
            current_state.phase == SharingPhase.WAIT_RETURN
            and borrowed_identity is not None
            and current_target is not None
            and current_target.connection_identity == borrowed_identity
            and current_target.healthy_existing_connection(current_observation.channel_uuid)
            and current_target.ordinary_client_ids
        )
        if ordinary_return_has_healthy_viewers:
            return current_state

    if succeeded and action in {
        SharingAction.ATTACH_BORROW,
        SharingAction.RETURN_TO_ALLOWED,
        SharingAction.PAUSE_NO_ALLOWED_SOURCE,
    }:
        deadline = _completion_deadline(
            current_state, current_observation, action,
            admitted_return_deadline=admitted_return_deadline,
            admitted_attach_deadline=admitted_attach_deadline,
        )
        if deadline is not None and (
            float(now) < deadline or float(current_observation.now) < deadline
        ):
            return current_state
    state = current_state
    if not succeeded:
        return replace(
            state,
            phase=(
                SharingPhase.STOPPED
                if action == SharingAction.STOP
                else (
                    SharingPhase.PAUSED
                    if action in {
                        SharingAction.RETURN_TO_ALLOWED,
                        SharingAction.PAUSE_NO_ALLOWED_SOURCE,
                    } and state.borrowed_identity is None
                    else SharingPhase.FAILED
                )
            ),
            candidate_identity=None,
            candidate_connection_identity=None,
            candidate_since=None,
            borrowed_identity=(
                state.borrowed_identity
                if action in {
                    SharingAction.RETURN_TO_ALLOWED,
                    SharingAction.PAUSE_NO_ALLOWED_SOURCE,
                }
                else None
            ),
            pending_action=None,
        )
    if action == SharingAction.ATTACH_BORROW:
        return replace(
            state,
            phase=SharingPhase.BORROWED,
            borrowed_identity=state.candidate_connection_identity,
            candidate_identity=None,
            candidate_connection_identity=None,
            candidate_since=None,
            last_viewer_left_at=None,
            last_switch_at=float(now),
            pending_action=None,
        )
    if action in {SharingAction.RETURN_TO_ALLOWED, SharingAction.PAUSE_NO_ALLOWED_SOURCE}:
        return replace(
            state,
            phase=(SharingPhase.DEDICATED if action == SharingAction.RETURN_TO_ALLOWED else SharingPhase.PAUSED),
            borrowed_identity=None,
            candidate_identity=None,
            candidate_connection_identity=None,
            candidate_since=None,
            last_viewer_left_at=None,
            last_switch_at=float(now),
            pending_action=None,
        )
    if action == SharingAction.STOP:
        return replace(state, phase=SharingPhase.STOPPED, pending_action=None)
    return replace(state, pending_action=None)


def _completion_deadline(
    state: SharingState,
    observation: SharingObservation,
    action: SharingAction,
    *,
    admitted_return_deadline: float | None = None,
    admitted_attach_deadline: float | None = None,
) -> float | None:
    """Recheck normal switching deadlines at transition completion.

    Forced cleanup and paused recovery carry FAILED/PAUSED phases and therefore
    bypass cooldowns. Only ordinary viewer-driven transitions are delayed.
    """
    if action == SharingAction.ATTACH_BORROW:
        if admitted_attach_deadline is not None:
            return float(admitted_attach_deadline)
        if state.phase != SharingPhase.WAIT_LIVE or state.candidate_since is None:
            return None
        return _later_deadline(
            state.candidate_since,
            (0 if state.last_switch_at is None
             else observation.settings.live_stability_seconds),
            state.last_switch_at,
            observation.settings.min_switch_interval_seconds,
        )
    if action in {
        SharingAction.RETURN_TO_ALLOWED,
        SharingAction.PAUSE_NO_ALLOWED_SOURCE,
    }:
        if admitted_return_deadline is not None:
            if action is not SharingAction.RETURN_TO_ALLOWED:
                raise ValueError("admitted return deadline is only valid for RETURN_TO_ALLOWED")
            return float(admitted_return_deadline)
        if state.phase != SharingPhase.WAIT_RETURN or state.last_viewer_left_at is None:
            return None
        return _later_deadline(
            state.last_viewer_left_at,
            observation.settings.last_viewer_return_seconds,
            state.last_switch_at,
            observation.settings.min_switch_interval_seconds,
        )
    return None
