"""Runtime of the private tail reader for a return to a faster source.

Decisions and deadlines come from :mod:`sharing_return_tail`. This module owns
the native client, the piped FFmpeg input and the thread that keeps recording
the delayed viewer stream until the dedicated source joins it. See
``docs/return-overlap.md``.
"""

from __future__ import annotations

import math
import threading
import time
import uuid
from typing import Any, Callable

from .sharing_return_tail import (
    Publisher,
    ReturnRequest,
    TailAction,
    TailEvent,
    TailEventKind,
    TailPhase,
    TailState,
    advance,
    plan_return,
)

TAIL_USER_AGENT = "Catchuparr return tail reader"
TAIL_CLIENT_IP = "127.0.0.1"
TAIL_CLIENT_TTL_SECONDS = 60
TAIL_RECORD_PREFIX = "catchuparr:sharing:tail:"
GENERATION_CHECK_SECONDS = 1.0
CLEANUP_RETRY_SECONDS = 1.0

# Read-only proof that the borrowed native connection is unchanged. The exact
# client itself is checked and touched by NativeClientActivityGuard.
_TAIL_NATIVE_CHECK_LUA = """
if redis.call('EXISTS', KEYS[5]) ~= 0 then return 0 end
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[2], 'owner') ~= ARGV[2] then return 0 end
if redis.call('HGET', KEYS[2], 'stream_id') ~= ARGV[3] then return 0 end
if redis.call('HGET', KEYS[2], 'm3u_profile') ~= ARGV[4] then return 0 end
if redis.call('HGET', KEYS[2], 'stream_profile') ~= ARGV[5] then return 0 end
local state = redis.call('HGET', KEYS[2], 'state')
if state ~= 'active' and state ~= 'waiting_for_clients' then return 0 end
if redis.call('EXISTS', KEYS[3]) == 0 then return 0 end
if redis.call('GET', KEYS[4]) ~= ARGV[6] then return 0 end
return 1
"""

_RECORD_FIELDS = (
    "tail_id", "client_id", "connected_at", "worker_id", "user_agent",
    "ip_address", "user_id", "output_format", "output_profile_id",
)


def tail_record_key(channel_uuid: str) -> str:
    return f"{TAIL_RECORD_PREFIX}{channel_uuid}"


def _text(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _mark_private(channel_uuid: str, client_id: str, digest: str) -> bool:
    from .stats import recorder_client_registered

    return recorder_client_registered(channel_uuid, client_id, digest, channel_uuid)


def _unmark_private(channel_uuid: str, client_id: str) -> bool:
    from .stats import recorder_client_removed

    recorder_client_removed(channel_uuid, client_id)
    return True


def recover_stale_tail(redis_text: Any, channel_uuid: str, *, clock: Callable[[], float] = time.time) -> bool:
    """Remove a tail client a previous recorder process left behind.

    A tail is never resumed. Only the exact persisted client generation is
    removed; a replacement with the same ID stays untouched.
    """
    from .adapters.sharing_reader import _REMOVE_EXACT_NATIVE_CLIENT_LUA

    key = tail_record_key(channel_uuid)
    try:
        if not redis_text.exists(key):
            return True
        raw = redis_text.hgetall(key) or {}
        record = {_text(name): _text(value) for name, value in raw.items()}
        if not record:
            return True
        client_id = record.get("client_id", "")
        if all(name in record for name in _RECORD_FIELDS) and client_id.startswith("catchuparr-client-"):
            result = redis_text.eval(
                _REMOVE_EXACT_NATIVE_CLIENT_LUA, 3,
                f"live:channel:{channel_uuid}:clients",
                f"live:channel:{channel_uuid}:clients:{client_id}",
                f"live:channel:{channel_uuid}:last_client_disconnect_time",
                client_id, record["connected_at"], record["worker_id"], record["user_agent"],
                record["ip_address"], record["user_id"], record["output_format"],
                record["output_profile_id"], str(float(clock())), TAIL_CLIENT_TTL_SECONDS,
            )
            if type(result) is not int or result not in (0, 1):
                return False
            _unmark_private(channel_uuid, client_id)
        redis_text.delete(key)
        return True
    except Exception:
        return False


class NativeTailReader:
    """One private client on the borrowed native connection, read in-process."""

    def __init__(
        self, *, redis_text: Any, redis_binary: Any, channel_uuid: str,
        identity: tuple[str, ...], configuration_generation: str, control_generation: int,
        recorder_fence: int, lease_value: str, stats_digest: str, hard_deadline: float,
        generation_is_current: Callable[[], bool], tail_id: str,
        clock: Callable[[], float] = time.time,
    ) -> None:
        from .adapters.sharing_reader import BorrowReadBinding

        if (not isinstance(identity, tuple) or len(identity) != 8 or identity[0] != channel_uuid
                or not isinstance(lease_value, str) or not lease_value
                or not isinstance(stats_digest, str) or not stats_digest
                or not math.isfinite(float(hard_deadline)) or not callable(generation_is_current)):
            raise ValueError("tail reader identity is incomplete")
        self.redis_text = redis_text
        self.redis_binary = redis_binary
        self.channel_uuid = channel_uuid
        self.identity = identity
        self.lease_value = lease_value
        self.stats_digest = stats_digest
        self.hard_deadline = float(hard_deadline)
        self.generation_is_current = generation_is_current
        self.tail_id = tail_id
        self.clock = clock
        self.client_id = f"catchuparr-client-{uuid.uuid4().hex}"
        self.binding = BorrowReadBinding(
            connection_identity=identity,
            configuration_generation=configuration_generation,
            control_generation=control_generation,
            recorder_fence=recorder_fence,
            pending_return_deadline=self.hard_deadline,
        )
        self.registration = None
        self.reader = None
        self._marked = False
        self._generation_checked_at: float | None = None
        self._closed = False

    def _proof(self):
        from .adapters.sharing_reader import BorrowBindingLost, BorrowReadProof

        now = self.clock()
        if not math.isfinite(float(now)) or now >= self.hard_deadline:
            raise BorrowBindingLost("tail deadline reached")
        channel, _worker, owner_value, metadata_owner, stream_id, _account, m3u, stream_profile = self.identity
        result = self.redis_text.eval(
            _TAIL_NATIVE_CHECK_LUA, 5,
            f"live:channel:{channel}:owner",
            f"live:channel:{channel}:metadata",
            f"live:worker:{metadata_owner}:heartbeat",
            f"catchuparr:recorder:{channel}",
            f"live:channel:{channel}:stopping",
            owner_value, metadata_owner, stream_id, m3u, stream_profile, self.lease_value,
        )
        if result != 1:
            raise BorrowBindingLost("borrowed native connection changed for the tail")
        if (self._generation_checked_at is None
                or now - self._generation_checked_at >= GENERATION_CHECK_SECONDS):
            if not self.generation_is_current():
                raise BorrowBindingLost("recorder generation changed for the tail")
            self._generation_checked_at = now
        return BorrowReadProof(
            connection_identity=self.identity,
            configuration_generation=self.binding.configuration_generation,
            control_generation=self.binding.control_generation,
            recorder_fence=self.binding.recorder_fence,
            ordinary_viewer_ids=(), checked_at=float(now),
            owner_matches=True, owner_heartbeat_healthy=True, native_active=True,
            assigned_source_matches=True, account_active=True, profile_active=True,
            media_compatible=True, useful_buffer_available=True, supported_worker_kind=True,
            borrowed_client_current=True, pending_return_deadline=self.hard_deadline,
        )

    def open(self, manager_type: Any, buffer_type: Any) -> None:
        from .adapters.sharing_reader import (
            BorrowedClientRegistration,
            BorrowReaderUnavailable,
            NativeClientActivityGuard,
            make_task_local_input_buffer,
        )
        from .sharing_episode import select_task_local_client_manager

        # Mark first: an unmarked client would count as a viewer and end the
        # borrow's return grace.
        if not _mark_private(self.channel_uuid, self.client_id, self.stats_digest):
            raise BorrowReaderUnavailable("tail private marker could not be written")
        self._marked = True
        try:
            manager, owned = select_task_local_client_manager(
                None, manager_type, channel_uuid=self.channel_uuid,
                redis_client=self.redis_text, worker_id=f"catchuparr-reader-{uuid.uuid4().hex}",
            )
            guard = NativeClientActivityGuard(
                self.redis_text, self.binding, recorder_lease_value=self.lease_value,
                client_record_ttl=TAIL_CLIENT_TTL_SECONDS,
                generation_is_current=lambda _binding: self.generation_is_current(),
            )
            self.registration = BorrowedClientRegistration(
                manager, self.binding, self._proof,
                client_id=self.client_id, client_ip=TAIL_CLIENT_IP, user_agent=TAIL_USER_AGENT,
                owns_client_manager=owned,
                register_private_marker=lambda client_id, _connected_at: _mark_private(
                    self.channel_uuid, client_id, self.stats_digest,
                ),
                remove_private_marker=lambda client_id, _connected_at: _unmark_private(
                    self.channel_uuid, client_id,
                ),
                activity_writer=guard,
            )
            buffer = make_task_local_input_buffer(buffer_type, self.channel_uuid, self.redis_binary)
            self.reader = self.registration.open(buffer)
            self._write_record()
        except Exception:
            self.close()
            raise

    def _write_record(self) -> None:
        fields = dict(self.registration._client_fields or {})
        record = {"tail_id": self.tail_id, "client_id": self.client_id, **fields}
        key = tail_record_key(self.channel_uuid)
        self.redis_text.hset(key, mapping=record)
        ttl = max(1, int(math.ceil(self.hard_deadline - self.clock())) + 2 * TAIL_CLIENT_TTL_SECONDS)
        self.redis_text.expire(key, ttl)

    def iter_chunks(self, stop):
        if self.reader is None:
            return
        yield from self.reader.iter_chunks(stop)

    def record_activity(self) -> None:
        self.registration.record_published_activity()

    def close(self) -> bool:
        """Remove the exact client, its marker and record; True once nothing remains."""
        if self._closed:
            return True
        try:
            if self.registration is not None:
                self.registration.close()
            elif self._marked:
                _unmark_private(self.channel_uuid, self.client_id)
            self.redis_text.delete(tail_record_key(self.channel_uuid))
        except Exception:
            return False
        self._closed = True
        return True


class ReturnTail:
    """Drive one tail: open during return grace, join, record, hand over, close."""

    def __init__(
        self, *, request_factory: Callable[[float], ReturnRequest],
        make_reader: Callable[[TailState], Any],
        make_recorder: Callable[[Any], Any],
        stop_event: threading.Event, clock: Callable[[], float] = time.time,
        on_error: Callable[[str], None] | None = None,
    ) -> None:
        self.request_factory = request_factory
        self.make_reader = make_reader
        self.make_recorder = make_recorder
        self.stop_event = stop_event
        self.clock = clock
        self.on_error = on_error or (lambda _category: None)
        self.state: TailState | None = None
        self.reader = None
        self.recorder = None
        self.staged = None
        self.switch = None
        self.thread: threading.Thread | None = None
        self.result = None
        self._lock = threading.RLock()

    # -- planner glue -----------------------------------------------------

    def _apply(self, kind: TailEventKind, *, client_id: str | None = None) -> tuple[TailAction, ...]:
        if self.state is None:
            return ()
        decision = advance(self.state, TailEvent(kind, self.state.tail_id, self.clock(), client_id))
        self.state = decision.state
        return decision.actions

    @property
    def active(self) -> bool:
        return self.state is not None and self.state.phase in {
            TailPhase.OPENING, TailPhase.JOINING, TailPhase.TAILING, TailPhase.SEEKING,
        }

    @property
    def staged_ready(self) -> bool:
        return self.staged is not None and self.state is not None and self.state.phase is TailPhase.JOINING

    # -- during return grace ---------------------------------------------

    def poll(self, borrow_recorder: Any, *, episode_phase: str | None) -> None:
        """Open the tail in the lead window; close it when the viewer returns."""
        from .logging_utils import event

        with self._lock:
            if self.state is None:
                if episode_phase != "wait_return" or self.stop_event.is_set():
                    return
                decision = plan_return(self.request_factory(self.clock()))
                if TailAction.OPEN_TAIL not in decision.actions:
                    if decision.state.outcome != "not_yet":
                        self.state = decision.state
                    return
                self.state = decision.state
                try:
                    self.reader = self.make_reader(self.state)
                    self.recorder = self.make_recorder(self.reader)
                    self.staged = self.recorder._start_staged_input()
                    self.staged.max_seconds = (
                        self.state.join_deadline - self.clock() + self.staged.max_seconds
                    )
                except Exception:
                    self._abandon(TailEventKind.TAIL_JOIN_FAILED)
                    return
                self._apply(TailEventKind.TAIL_REGISTERED, client_id=self.reader.client_id)
                if self.state.phase is not TailPhase.JOINING:
                    self._abandon(TailEventKind.TAIL_JOIN_FAILED)
                    return
                borrow_recorder.final_segment_min_seconds = math.inf
                event("return_tail_opened")
                return
            if not self.active or self.state.phase is not TailPhase.JOINING:
                return
            kind = (TailEventKind.VIEWER_RETURNED if episode_phase == "active"
                    else TailEventKind.STOP if self.stop_event.is_set() else TailEventKind.TICK)
            actions = self._apply(kind)
            if TailAction.CLOSE_TAIL in actions:
                from .engine.recorder import FINAL_SEGMENT_MIN_SECONDS

                borrow_recorder.final_segment_min_seconds = FINAL_SEGMENT_MIN_SECONDS
                self._close_now()

    def _abandon(self, kind: TailEventKind) -> None:
        self._apply(kind)
        self._close_now()

    def _close_now(self) -> bool:
        from .logging_utils import event

        if self.staged is not None:
            try:
                self.staged.close()
            except Exception:
                pass
            self.staged = None
        closed = True
        if self.reader is not None:
            deadline = self.clock() + (
                self.state.cleanup_deadline - self.clock()
                if self.state is not None and self.state.cleanup_deadline is not None else 30.0
            )
            while True:
                closed = self.reader.close()
                if closed or self.clock() >= deadline:
                    break
                time.sleep(CLEANUP_RETRY_SECONDS)
        if closed:
            self._apply(TailEventKind.TAIL_CLOSED)
            event("return_tail_closed")
        else:
            self._apply(TailEventKind.TAIL_CLOSE_FAILED)
            self.on_error("sharing_tail_cleanup_pending")
            self.stop_event.set()
        return closed

    # -- after the borrow ended ------------------------------------------

    def join(self, borrow_outcome: str, previous_segment_id: str | None) -> bool:
        """Join the frozen archive tail; on any failure return as before."""
        from .logging_utils import event

        with self._lock:
            if not self.staged_ready:
                return False
            if borrow_outcome != "return" or not isinstance(previous_segment_id, str):
                self._abandon(TailEventKind.TAIL_JOIN_FAILED if borrow_outcome == "return"
                              else TailEventKind.STOP)
                return False
            self._apply(TailEventKind.RETURN_DEADLINE)
            timeout = max(0.0, self.state.join_deadline - self.clock())
            joined = self.recorder.join_staged_after_archive_tail(
                self.staged, previous_segment_id, self.stop_event, timeout=timeout,
            )
            if not joined:
                self._abandon(TailEventKind.TAIL_JOIN_FAILED)
                return False
            actions = self._apply(TailEventKind.TAIL_JOINED)
            if TailAction.CLEANUP_BORROW not in actions:
                self._abandon(TailEventKind.TAIL_JOIN_FAILED)
                return False
            event("return_tail_joined")
            return True

    def start(self, *, startup_timeout: float, media_idle_timeout: float) -> None:
        from .engine.recorder import RecorderAttemptSwitch

        self.switch = RecorderAttemptSwitch()
        staged, self.staged = self.staged, None
        recorder = self.recorder

        def run() -> None:
            try:
                self.result = recorder.run_candidate(
                    self.stop_event, startup_timeout=startup_timeout,
                    media_idle_timeout=media_idle_timeout, attempt_switch=self.switch,
                    staged_input=staged,
                )
            except Exception:
                self.result = None
            finally:
                with self._lock:
                    if self.state is not None and self.state.phase in {
                            TailPhase.TAILING, TailPhase.SEEKING}:
                        staged_next = getattr(self.result, "staged_input", None)
                        seam = staged_next is not None and (staged_next.matched or staged_next.frozen)
                        self._apply(TailEventKind.SEAM_FOUND if seam else TailEventKind.TAIL_FAILED)
                    self._close_now()

        self.thread = threading.Thread(target=run, name="catchuparr-return-tail", daemon=True)
        self.thread.start()

    def borrow_cleaned(self) -> None:
        with self._lock:
            self._apply(TailEventKind.BORROW_CLEANED)

    # -- hand over to the dedicated source --------------------------------

    def hand_over(self, dedicated: Any, dedicated_switch: Any) -> Any:
        """Offer the dedicated input to the running tail; return its staged input.

        Returns ``None`` when the tail already ended or the hand-over was
        interrupted; the dedicated input then starts as without a tail.
        """
        if self.thread is None:
            return None
        with self._lock:
            self._apply(TailEventKind.RETURN_TOKEN)
            alive = self.thread.is_alive() and self.state is not None and self.state.dedicated_requested
            if alive:
                self._apply(TailEventKind.DEDICATED_STAGED)
                seam_deadline = self.state.seam_deadline or self.clock()
                dedicated.handover_overlap_seconds = max(1.0, seam_deadline - self.clock())
        offered = alive and self.switch.offer_synchronized_input(dedicated)
        if not offered:
            self.switch.request()
        while self.thread.is_alive():
            if self.stop_event.is_set() or (dedicated_switch is not None and dedicated_switch.is_set()):
                self.switch.request()
            self.thread.join(0.2)
        result, self.result = self.result, None
        staged = getattr(result, "staged_input", None) if offered else None
        if staged is not None and (self.stop_event.is_set()
                                   or (dedicated_switch is not None and dedicated_switch.is_set())):
            staged.close()
            return None
        return staged

    def close(self) -> None:
        """Final cleanup on any recorder exit path."""
        if self.thread is not None:
            self.switch.request()
            self.thread.join(30)
            if self.thread.is_alive():
                self.on_error("sharing_tail_cleanup_pending")
            return
        with self._lock:
            if self.state is not None and self.state.phase not in {TailPhase.IDLE, TailPhase.CLOSED}:
                if self.active:
                    self._apply(TailEventKind.STOP)
                self._close_now()

    @property
    def publisher(self) -> Publisher | None:
        return self.state.publisher if self.state is not None else None
