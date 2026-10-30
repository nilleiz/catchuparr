"""Pure planner for the private tail reader of a return to a faster source.

When the borrowed viewer stream is behind the dedicated source, a private tail
reader keeps recording the delayed stream after the borrowed reader's cleanup
until the audio comparison finds the seam to the dedicated source. This module
only compares events and timestamps. It does not inspect or mutate Dispatcharr,
Redis, native clients, processes or the archive. See ``docs/return-overlap.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
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
        raise NotImplementedError


@dataclass(frozen=True)
class TailDecision:
    state: TailState
    actions: tuple[TailAction, ...] = ()
    reason: str = ""


def plan_return(request: ReturnRequest) -> TailDecision:
    """Decide whether a pending return opens a tail reader or runs as today."""
    raise NotImplementedError


def advance(state: TailState, event: TailEvent) -> TailDecision:
    """Apply one event; stale, duplicate and late events change nothing."""
    raise NotImplementedError


def restore(snapshot: Mapping[str, Any], *, channel_uuid: str, now: float) -> TailDecision:
    """Never resume a tail after a restart; always close what may be live."""
    raise NotImplementedError
