"""Pure planner for the private tail reader of a return to a faster source.

When the borrowed viewer stream is behind the dedicated source, a private tail
reader keeps recording the delayed stream after the borrowed reader's cleanup
until the audio comparison finds the seam to the dedicated source. This module
only compares events and timestamps. It does not inspect or mutate Dispatcharr,
Redis, native clients, processes or the archive. See ``docs/return-overlap.md``.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, replace
from enum import Enum
from typing import Any, Mapping

TAIL_LEAD_SECONDS = 20.0
MIN_LEAD_SECONDS = 8.0
JOIN_TIMEOUT_SECONDS = 12.0
TAIL_ALLOWANCE_SECONDS = 60.0
SEAM_MARGIN_SECONDS = 20.0
TAIL_CLEANUP_SECONDS = 30.0
MIN_OVERLAP_SECONDS = 10
MAX_OVERLAP_SECONDS = 120
PRIVATE_CLIENT_PREFIX = "catchuparr-client-"


class TailPhase(str, Enum):
    IDLE = "idle"
    OPENING = "opening"
    JOINING = "joining"
    TAILING = "tailing"
    SEEKING = "seeking"
    CLOSING = "closing"
    CLEANUP_PENDING = "cleanup_pending"
    CLOSED = "closed"


ACTIVE_PHASES = frozenset({TailPhase.OPENING, TailPhase.JOINING, TailPhase.TAILING, TailPhase.SEEKING})


class TailAction(str, Enum):
    PLAIN_RETURN = "plain_return"
    OPEN_TAIL = "open_tail"
    CLEANUP_BORROW = "cleanup_borrow"
    STAGE_DEDICATED = "stage_dedicated"
    SWITCH_TO_DEDICATED = "switch_to_dedicated"
    FALLBACK_TO_DEDICATED = "fallback_to_dedicated"
    CLOSE_TAIL = "close_tail"
    CLOSE_DEDICATED_STAGED = "close_dedicated_staged"
    STOP_RECORDER = "stop_recorder"


class TailEventKind(str, Enum):
    TAIL_REGISTERED = "tail_registered"
    VIEWER_RETURNED = "viewer_returned"
    RETURN_DEADLINE = "return_deadline"
    TAIL_JOINED = "tail_joined"
    TAIL_JOIN_FAILED = "tail_join_failed"
    BORROW_CLEANED = "borrow_cleaned"
    RETURN_TOKEN = "return_token"
    DEDICATED_STAGED = "dedicated_staged"
    DEDICATED_FAILED = "dedicated_failed"
    SEAM_FOUND = "seam_found"
    TAIL_FAILED = "tail_failed"
    STOP = "stop"
    TICK = "tick"
    TAIL_CLOSED = "tail_closed"
    TAIL_CLOSE_FAILED = "tail_close_failed"


class Publisher(str, Enum):
    BORROW = "borrow"
    TAIL = "tail"
    DEDICATED = "dedicated"
    NONE = "none"


@dataclass(frozen=True)
class ReturnRequest:
    """Facts while the borrowed reader waits for its return (``wait_return``)."""

    channel_uuid: str
    tail_id: str
    recorder_fence: int
    now: float
    return_deadline: float
    borrowed_account_id: str
    dedicated_account_id: str
    delayed_account_ids: frozenset[str]
    max_overlap_seconds: Any
    synchronize_before_switch: bool


@dataclass(frozen=True)
class TailEvent:
    kind: TailEventKind
    tail_id: str
    at: float
    client_id: str | None = None


@dataclass(frozen=True)
class TailState:
    channel_uuid: str
    tail_id: str
    recorder_fence: int
    phase: TailPhase
    publisher: Publisher
    max_overlap_seconds: int = 0
    client_id: str | None = None
    opened_at: float | None = None
    return_deadline: float | None = None
    join_deadline: float | None = None
    hard_deadline: float | None = None
    seam_deadline: float | None = None
    closing_at: float | None = None
    cleanup_deadline: float | None = None
    return_due: bool = False
    borrow_cleaned: bool = False
    dedicated_requested: bool = False
    dedicated_staged: bool = False
    outcome: str | None = None

    def to_snapshot(self) -> dict[str, Any]:
        """Persist only identity, phase and deadlines; never a URL or token."""
        snapshot = {"version": 1, **asdict(self)}
        snapshot["phase"] = self.phase.value
        snapshot["publisher"] = self.publisher.value
        return snapshot


@dataclass(frozen=True)
class TailDecision:
    state: TailState
    actions: tuple[TailAction, ...] = ()
    reason: str = ""


_TIME_FIELDS = (
    "opened_at", "return_deadline", "join_deadline", "hard_deadline",
    "seam_deadline", "closing_at", "cleanup_deadline",
)
_FLAG_FIELDS = ("return_due", "borrow_cleaned", "dedicated_requested", "dedicated_staged")


def _finite(value: Any) -> bool:
    return (not isinstance(value, bool) and isinstance(value, (int, float))
            and math.isfinite(float(value)))


def _private_client_id(value: Any) -> bool:
    return (isinstance(value, str) and value.startswith(PRIVATE_CLIENT_PREFIX)
            and len(value) > len(PRIVATE_CLIENT_PREFIX))


def _close(state: TailState, at: float, outcome: str, publisher: Publisher,
           *actions: TailAction) -> TailDecision:
    closing = replace(
        state, phase=TailPhase.CLOSING, publisher=publisher, outcome=outcome,
        closing_at=at, cleanup_deadline=at + TAIL_CLEANUP_SECONDS,
    )
    return TailDecision(closing, actions, outcome)


def plan_return(request: ReturnRequest) -> TailDecision:
    """Decide whether a pending return opens a tail reader or runs as today."""
    idle = TailState(request.channel_uuid, request.tail_id, request.recorder_fence,
                     TailPhase.IDLE, Publisher.BORROW)

    def ineligible(reason: str) -> TailDecision:
        return TailDecision(replace(idle, outcome="ineligible"), (TailAction.PLAIN_RETURN,), reason)

    seconds = request.max_overlap_seconds
    delayed = request.delayed_account_ids
    if request.synchronize_before_switch is not True:
        return ineligible("synchronized switching is disabled")
    if type(seconds) is not int or not MIN_OVERLAP_SECONDS <= seconds <= MAX_OVERLAP_SECONDS:
        return ineligible("no valid handover overlap is configured")
    if (not isinstance(delayed, frozenset) or request.borrowed_account_id not in delayed
            or not isinstance(request.dedicated_account_id, str) or not request.dedicated_account_id
            or request.dedicated_account_id in delayed):
        return ineligible("the return does not leave a delayed source for a faster one")
    if (not isinstance(request.tail_id, str) or not request.tail_id
            or not isinstance(request.channel_uuid, str) or not request.channel_uuid
            or not _finite(request.now) or not _finite(request.return_deadline)):
        return ineligible("tail identity or timing is invalid")
    now, deadline = float(request.now), float(request.return_deadline)
    lead = deadline - now
    if lead > TAIL_LEAD_SECONDS:
        return TailDecision(replace(idle, outcome="not_yet"), (), "return deadline is not near yet")
    if lead < MIN_LEAD_SECONDS:
        return ineligible("too little time remains before the return deadline")
    opening = replace(
        idle, phase=TailPhase.OPENING, max_overlap_seconds=seconds, opened_at=now,
        return_deadline=deadline, join_deadline=deadline + JOIN_TIMEOUT_SECONDS,
        hard_deadline=deadline + seconds + TAIL_ALLOWANCE_SECONDS,
    )
    return TailDecision(opening, (TailAction.OPEN_TAIL,), "open the tail reader")


def _expired(state: TailState, at: float) -> TailDecision | None:
    """Apply every deadline that has passed at ``at`` before the event itself."""
    if state.phase in {TailPhase.OPENING, TailPhase.JOINING} and at >= state.join_deadline:
        return _close(state, at, "join_failed", Publisher.BORROW,
                      TailAction.CLOSE_TAIL, TailAction.PLAIN_RETURN)
    if state.phase is TailPhase.TAILING and at >= state.hard_deadline:
        return _close(state, at, "deadline", Publisher.NONE, TailAction.CLOSE_TAIL)
    if state.phase is TailPhase.SEEKING and at >= state.seam_deadline:
        return _close(state, at, "deadline", Publisher.DEDICATED,
                      TailAction.FALLBACK_TO_DEDICATED, TailAction.CLOSE_TAIL)
    return None


def advance(state: TailState, event: TailEvent) -> TailDecision:
    """Apply one event; stale, duplicate and late events change nothing."""
    unchanged = TailDecision(state)
    if (not isinstance(event, TailEvent) or event.tail_id != state.tail_id
            or state.phase in {TailPhase.IDLE, TailPhase.CLOSED} or not _finite(event.at)):
        return unchanged
    at, kind, phase = float(event.at), event.kind, state.phase
    expired = _expired(state, at)
    if expired is not None:
        return expired
    if phase in ACTIVE_PHASES and not state.return_due and (
            kind is TailEventKind.RETURN_DEADLINE or at >= state.return_deadline):
        state = replace(state, return_due=True)
    due = TailDecision(state)

    if kind is TailEventKind.STOP and phase in ACTIVE_PHASES:
        actions = (TailAction.CLOSE_TAIL,)
        if phase is TailPhase.SEEKING:
            actions += (TailAction.CLOSE_DEDICATED_STAGED,)
        return _close(state, at, "stopped", Publisher.NONE, *actions)

    if phase in {TailPhase.OPENING, TailPhase.JOINING}:
        if kind is TailEventKind.TAIL_REGISTERED and phase is TailPhase.OPENING:
            if not _private_client_id(event.client_id):
                return _close(state, at, "invalid_client", Publisher.BORROW,
                              TailAction.CLOSE_TAIL, TailAction.PLAIN_RETURN)
            return TailDecision(replace(state, phase=TailPhase.JOINING, client_id=event.client_id))
        if kind is TailEventKind.VIEWER_RETURNED and not state.return_due:
            return _close(state, at, "viewer_returned", Publisher.BORROW, TailAction.CLOSE_TAIL)
        if kind in {TailEventKind.TAIL_JOIN_FAILED, TailEventKind.TAIL_FAILED}:
            return _close(state, at, "join_failed", Publisher.BORROW,
                          TailAction.CLOSE_TAIL, TailAction.PLAIN_RETURN)
        if kind is TailEventKind.TAIL_JOINED and phase is TailPhase.JOINING and state.return_due:
            joined = replace(state, phase=TailPhase.TAILING, publisher=Publisher.TAIL)
            return TailDecision(joined, (TailAction.CLEANUP_BORROW,), "tail took over publication")
        return due

    if phase is TailPhase.TAILING:
        if kind is TailEventKind.BORROW_CLEANED:
            return TailDecision(replace(state, borrow_cleaned=True))
        if kind is TailEventKind.RETURN_TOKEN and state.borrow_cleaned and not state.dedicated_requested:
            return TailDecision(replace(state, dedicated_requested=True), (TailAction.STAGE_DEDICATED,))
        if kind is TailEventKind.DEDICATED_STAGED and state.dedicated_requested:
            seam = min(state.hard_deadline, at + state.max_overlap_seconds + SEAM_MARGIN_SECONDS)
            return TailDecision(replace(
                state, phase=TailPhase.SEEKING, dedicated_staged=True, seam_deadline=seam,
            ))
        if kind is TailEventKind.DEDICATED_FAILED and state.dedicated_requested:
            return _close(state, at, "dedicated_failed", Publisher.NONE, TailAction.CLOSE_TAIL)
        if kind is TailEventKind.TAIL_FAILED:
            return _close(state, at, "tail_failed", Publisher.NONE, TailAction.CLOSE_TAIL)
        return due

    if phase is TailPhase.SEEKING:
        if kind is TailEventKind.SEAM_FOUND:
            return _close(state, at, "seam", Publisher.DEDICATED,
                          TailAction.SWITCH_TO_DEDICATED, TailAction.CLOSE_TAIL)
        if kind is TailEventKind.TAIL_FAILED:
            return _close(state, at, "tail_failed", Publisher.DEDICATED,
                          TailAction.FALLBACK_TO_DEDICATED, TailAction.CLOSE_TAIL)
        if kind is TailEventKind.DEDICATED_FAILED:
            return _close(state, at, "dedicated_failed", Publisher.NONE, TailAction.CLOSE_TAIL)
        return due

    # Closing: only the cleanup acknowledgement and its deadline matter.
    if kind is TailEventKind.TAIL_CLOSED:
        return TailDecision(replace(state, phase=TailPhase.CLOSED), (), "tail closed")
    if kind is TailEventKind.TAIL_CLOSE_FAILED and phase is TailPhase.CLOSING:
        return TailDecision(replace(state, phase=TailPhase.CLEANUP_PENDING))
    if kind is TailEventKind.TICK:
        if at >= state.cleanup_deadline:
            return TailDecision(replace(state, phase=TailPhase.CLEANUP_PENDING),
                                (TailAction.CLOSE_TAIL, TailAction.STOP_RECORDER),
                                "tail cleanup is not acknowledged")
        if phase is TailPhase.CLEANUP_PENDING:
            return TailDecision(state, (TailAction.CLOSE_TAIL,), "retry tail cleanup")
    return unchanged


def _restored(snapshot: Mapping[str, Any], channel_uuid: str) -> TailState | None:
    expected = {"version", *TailState.__dataclass_fields__}
    if not isinstance(snapshot, Mapping) or set(snapshot) != expected or snapshot["version"] != 1:
        return None
    values = {name: snapshot[name] for name in TailState.__dataclass_fields__}
    try:
        values["phase"] = TailPhase(values["phase"])
        values["publisher"] = Publisher(values["publisher"])
    except (TypeError, ValueError):
        return None
    if (values["channel_uuid"] != channel_uuid
            or not isinstance(values["tail_id"], str) or not values["tail_id"]
            or type(values["recorder_fence"]) is not int or values["recorder_fence"] < 0
            or type(values["max_overlap_seconds"]) is not int
            or (values["client_id"] is not None and not _private_client_id(values["client_id"]))
            or (values["outcome"] is not None and not isinstance(values["outcome"], str))
            or any(values[name] is not None and not _finite(values[name]) for name in _TIME_FIELDS)
            or any(type(values[name]) is not bool for name in _FLAG_FIELDS)):
        return None
    return TailState(**values)


def restore(snapshot: Mapping[str, Any], *, channel_uuid: str, now: float) -> TailDecision:
    """Never resume a tail after a restart; always close what may be live."""
    now = float(now) if _finite(now) else 0.0
    state = _restored(snapshot, channel_uuid)
    if state is None:
        tail_id = snapshot.get("tail_id") if isinstance(snapshot, Mapping) else None
        unknown = TailState(
            channel_uuid, tail_id if isinstance(tail_id, str) else "", 0,
            TailPhase.CLEANUP_PENDING, Publisher.NONE, outcome="recovered_invalid",
            closing_at=now, cleanup_deadline=now + TAIL_CLEANUP_SECONDS,
        )
        return TailDecision(unknown, (TailAction.CLOSE_TAIL,), "close every tail client of the channel")
    if state.phase in {TailPhase.IDLE, TailPhase.CLOSED}:
        return TailDecision(state)
    return _close(state, now, "recovered", Publisher.NONE, TailAction.CLOSE_TAIL)
