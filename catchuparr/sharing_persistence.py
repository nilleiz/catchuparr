"""Persistent, fenced sharing intents with short SQLite transactions.

This module stores planner cooldown state and the exact pending transition token.
It never restores a borrowed route as authority and contains no native/client/
FFmpeg operations. Callers must provide a fresh policy/lease/target proof before
an external side effect; a separate native CAS is still required at that boundary.
"""

from __future__ import annotations

import contextlib
import json
import math
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Iterator

from .sharing_planner import (
    SharingAction,
    SharingPhase,
    SharingState,
    restore_state,
)

try:
    from gevent.lock import RLock as _CooperativeRLock
except ImportError:
    # Unit tests can run without the host application's gevent dependency.
    _CooperativeRLock = threading.RLock


_INTENT_ACTIONS = {
    SharingAction.ATTACH_BORROW,
    SharingAction.RETURN_TO_ALLOWED,
    SharingAction.PAUSE_NO_ALLOWED_SOURCE,
    SharingAction.STOP,
}


class StaleSharingIntent(RuntimeError):
    """The persisted transition no longer matches the caller's exact token."""


@dataclass(frozen=True)
class SharingIntentToken:
    channel_uuid: str
    revision: int
    intent_epoch: int
    action: SharingAction
    configuration_generation: str
    control_generation: int
    recorder_fence: int
    target_identity: tuple[str, ...] | None
    target_admitted: bool
    not_before: float

    def __post_init__(self) -> None:
        if not isinstance(self.channel_uuid, str) or not self.channel_uuid:
            raise ValueError("channel_uuid must be non-empty")
        if type(self.revision) is not int or self.revision < 1:
            raise ValueError("revision must be positive")
        if type(self.intent_epoch) is not int or self.intent_epoch < 0:
            raise ValueError("intent_epoch must be non-negative")
        if not isinstance(self.action, SharingAction) or self.action not in _INTENT_ACTIONS:
            raise ValueError("action is not a side-effect transition")
        if not isinstance(self.configuration_generation, str) or not self.configuration_generation:
            raise ValueError("configuration_generation must be non-empty")
        if type(self.control_generation) is not int or self.control_generation < 0:
            raise ValueError("control_generation must be non-negative")
        if type(self.recorder_fence) is not int or self.recorder_fence < 0:
            raise ValueError("recorder_fence must be non-negative")
        if type(self.target_admitted) is not bool:
            raise ValueError("target_admitted must be boolean")
        if self.target_identity is not None and (
            not isinstance(self.target_identity, tuple)
            or len(self.target_identity) != 8
            or any(not isinstance(value, str) or not value for value in self.target_identity)
            or self.target_identity[0] != self.channel_uuid
        ):
            raise ValueError("target_identity must be the exact sanitized channel identity")
        if self.action == SharingAction.ATTACH_BORROW:
            if self.target_identity is None or not self.target_admitted:
                raise ValueError("borrow intent requires an admitted exact target")
        elif self.target_admitted:
            raise ValueError("only a new borrow intent may carry target admission")
        if (
            isinstance(self.not_before, bool)
            or not isinstance(self.not_before, (int, float))
            or not math.isfinite(float(self.not_before))
        ):
            raise ValueError("not_before must be a finite timestamp")


@dataclass(frozen=True)
class PersistedSharingState:
    revision: int
    state: SharingState
    intent: SharingIntentToken | None


class SharingIntentLedger:
    """SQLite ledger with local cooperative locks and durable cross-process CAS."""

    _lock_map_guard = _CooperativeRLock()
    _thread_locks: dict[tuple[str, str], object] = {}

    def __init__(self, database_path: Path, *, clock: Callable[[], float] = time.time):
        self.database_path = Path(database_path).expanduser().resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        if not callable(clock):
            raise ValueError("clock must be callable")
        self.clock = clock
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.database_path, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=5000")
        return db

    @contextmanager
    def _database(self) -> Iterator[sqlite3.Connection]:
        db = self._connect()
        try:
            yield db
        finally:
            db.close()

    def _initialize(self) -> None:
        with self._database() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.execute(
                """CREATE TABLE IF NOT EXISTS sharing_runtime (
                    channel_uuid TEXT PRIMARY KEY,
                    revision INTEGER NOT NULL,
                    state_json TEXT NOT NULL,
                    action TEXT,
                    intent_epoch INTEGER,
                    configuration_generation TEXT NOT NULL,
                    control_generation INTEGER NOT NULL,
                    recorder_fence INTEGER NOT NULL,
                    target_identity_json TEXT,
                    target_admitted INTEGER NOT NULL,
                    not_before REAL,
                    updated_at REAL NOT NULL
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS sharing_intent_claims (
                    channel_uuid TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    intent_epoch INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    claim_nonce TEXT NOT NULL,
                    claimed_at REAL NOT NULL,
                    PRIMARY KEY (channel_uuid, revision)
                )"""
            )

    @staticmethod
    def _channel(channel_uuid: str) -> str:
        if (
            not isinstance(channel_uuid, str)
            or not channel_uuid
            or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for char in channel_uuid)
        ):
            raise ValueError("channel_uuid contains unsupported characters")
        return channel_uuid

    @staticmethod
    def _finite_time(value: float, name: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise ValueError(f"{name} must be a finite timestamp")
        return float(value)

    @contextlib.contextmanager
    def action_lock(self, channel_uuid: str) -> Iterator[None]:
        channel = self._channel(channel_uuid)
        map_key = (str(self.database_path), channel)
        with self._lock_map_guard:
            thread_lock = self._thread_locks.setdefault(map_key, _CooperativeRLock())
        # Never use blocking flock here: this gate may be called from a gevent
        # worker. SQLite CAS/revisions serialize durable writes across processes;
        # recorder fencing and native CAS protect external state boundaries.
        with thread_lock:
            yield

    @staticmethod
    def _state_json(state: SharingState) -> str:
        return json.dumps(state.to_snapshot(), sort_keys=True, separators=(",", ":"))

    def save(
        self,
        state: SharingState,
        *,
        action: SharingAction | None,
        expected_revision: int | None,
        target_identity: tuple[str, ...] | None = None,
        target_admitted: bool = False,
        not_before: float | None = None,
        updated_at: float | None = None,
    ) -> PersistedSharingState:
        if not isinstance(state, SharingState):
            raise ValueError("state must be SharingState")
        channel = self._channel(state.channel_uuid)
        if action is None:
            if target_identity is not None or target_admitted or not_before is not None:
                raise ValueError("state-only writes cannot contain pending intent fields")
        else:
            pending_matches = (
                state.phase == SharingPhase.STOPPED and state.pending_action is None
                if action == SharingAction.STOP
                else state.pending_action == action
            )
            if not pending_matches or action not in _INTENT_ACTIONS:
                raise ValueError("persisted intent must match the planner's pending action")
            if not_before is None:
                raise ValueError("pending intent requires its exact not-before deadline")
            not_before = self._finite_time(not_before, "not_before")
        if type(target_admitted) is not bool:
            raise ValueError("target_admitted must be boolean")
        if target_identity is not None:
            if (
                not isinstance(target_identity, tuple)
                or len(target_identity) != 8
                or any(not isinstance(value, str) or not value for value in target_identity)
                or target_identity[0] != channel
            ):
                raise ValueError("target_identity must be an exact sanitized channel identity")
        if action == SharingAction.ATTACH_BORROW:
            if target_identity is None or not target_admitted:
                raise ValueError("borrow intent requires exact admitted target identity")
            if state.candidate_connection_identity != target_identity:
                raise ValueError("borrow target does not match the planner candidate")
        elif target_admitted:
            raise ValueError("target_admitted applies only to attach-borrow intents")
        if expected_revision is not None and (type(expected_revision) is not int or expected_revision < 1):
            raise ValueError("expected_revision must be positive or None for first insert")
        updated = self._finite_time(self.clock() if updated_at is None else updated_at, "updated_at")
        state_json = self._state_json(state)
        target_json = (
            json.dumps(list(target_identity), separators=(",", ":"))
            if target_identity is not None else None
        )
        with self.action_lock(channel), self._database() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT revision FROM sharing_runtime WHERE channel_uuid=?", (channel,)
            ).fetchone()
            if (row is None) != (expected_revision is None):
                db.rollback()
                raise StaleSharingIntent("sharing state revision changed")
            revision = 1 if row is None else int(row["revision"]) + 1
            if row is not None and int(row["revision"]) != expected_revision:
                db.rollback()
                raise StaleSharingIntent("sharing state revision changed")
            values = (
                state_json,
                action.value if action is not None else None,
                state.intent_epoch if action is not None else None,
                state.configuration_generation,
                state.control_generation,
                state.recorder_fence,
                target_json,
                int(target_admitted),
                not_before,
                updated,
                channel,
            )
            db.execute(
                """INSERT INTO sharing_runtime (
                    state_json, action, intent_epoch, configuration_generation,
                    control_generation, recorder_fence, target_identity_json,
                    target_admitted, not_before, updated_at, channel_uuid, revision
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(channel_uuid) DO UPDATE SET
                    state_json=excluded.state_json,
                    action=excluded.action,
                    intent_epoch=excluded.intent_epoch,
                    configuration_generation=excluded.configuration_generation,
                    control_generation=excluded.control_generation,
                    recorder_fence=excluded.recorder_fence,
                    target_identity_json=excluded.target_identity_json,
                    target_admitted=excluded.target_admitted,
                    not_before=excluded.not_before,
                    updated_at=excluded.updated_at,
                    revision=excluded.revision""",
                (*values, revision),
            )
            db.commit()
        return self.read(channel)

    def read(self, channel_uuid: str) -> PersistedSharingState | None:
        channel = self._channel(channel_uuid)
        with self._database() as db:
            row = db.execute(
                "SELECT * FROM sharing_runtime WHERE channel_uuid=?", (channel,)
            ).fetchone()
        return self._decode(row) if row is not None else None

    @staticmethod
    def _strict_json_loads(value: str):
        def reject_constant(_constant: str):
            raise ValueError("non-finite persisted JSON number")

        return json.loads(value, parse_constant=reject_constant)

    @staticmethod
    def _decode(row: sqlite3.Row) -> PersistedSharingState:
        snapshot = SharingIntentLedger._strict_json_loads(row["state_json"])
        expected_snapshot_fields = {
            "version", "channel_uuid", "configuration_generation", "control_generation",
            "phase", "candidate_identity", "candidate_connection_identity", "candidate_since",
            "last_viewer_left_at", "last_switch_at", "intent_epoch",
        }
        if (
            not isinstance(snapshot, dict)
            or set(snapshot) != expected_snapshot_fields
            or type(snapshot.get("version")) is not int
            or snapshot["version"] != 2
            or snapshot.get("channel_uuid") != row["channel_uuid"]
            or snapshot.get("configuration_generation") != row["configuration_generation"]
            or type(snapshot.get("control_generation")) is not int
            or snapshot["control_generation"] != int(row["control_generation"])
            or type(snapshot.get("intent_epoch")) is not int
            or snapshot["intent_epoch"] < 0
            or type(row["revision"]) is not int
            or row["revision"] < 1
            or type(row["recorder_fence"]) is not int
            or row["recorder_fence"] < 0
            or not isinstance(snapshot.get("configuration_generation"), str)
            or not snapshot["configuration_generation"]
            or not isinstance(row["configuration_generation"], str)
            or not row["configuration_generation"]
            or type(row["control_generation"]) is not int
            or row["control_generation"] < 0
            or type(row["target_admitted"]) is not int
            or row["target_admitted"] not in (0, 1)
            or not isinstance(row["updated_at"], (int, float))
            or not math.isfinite(float(row["updated_at"]))
        ):
            raise ValueError("persisted sharing state snapshot is inconsistent")
        try:
            phase = SharingPhase(snapshot["phase"])
        except (TypeError, ValueError) as exc:
            raise ValueError("persisted sharing phase is invalid") from exc
        action_value = row["action"]
        if action_value is not None and not isinstance(action_value, str):
            raise ValueError("persisted sharing action is invalid")
        candidate = snapshot.get("candidate_identity")
        candidate_connection = snapshot.get("candidate_connection_identity")
        if candidate is not None and (
            not isinstance(candidate, list)
            or len(candidate) != 8
            or any(not isinstance(part, str) or not part for part in candidate)
            or candidate[0] != row["channel_uuid"]
        ):
            raise ValueError("persisted candidate identity is invalid")
        if candidate_connection is not None and (
            not isinstance(candidate_connection, list)
            or len(candidate_connection) != 8
            or any(not isinstance(part, str) or not part for part in candidate_connection)
            or candidate_connection[0] != row["channel_uuid"]
        ):
            raise ValueError("persisted candidate connection identity is invalid")
        if (candidate is None) != (candidate_connection is None):
            raise ValueError("persisted candidate identities are inconsistent")
        for name in ("candidate_since", "last_viewer_left_at", "last_switch_at"):
            value = snapshot.get(name)
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
            ):
                raise ValueError(f"persisted {name} must be finite or null")
        if (candidate is None) != (snapshot.get("candidate_since") is None):
            raise ValueError("persisted candidate clock is inconsistent")
        if action_value is None:
            if (
                row["intent_epoch"] is not None
                or row["target_identity_json"] is not None
                or int(row["target_admitted"]) != 0
                or row["not_before"] is not None
            ):
                raise ValueError("state-only sharing row contains stale intent fields")
        elif (
            action_value not in {action.value for action in _INTENT_ACTIONS}
            or type(row["intent_epoch"]) is not int
            or int(row["intent_epoch"]) != snapshot["intent_epoch"]
            or (phase == SharingPhase.STOPPED and action_value != SharingAction.STOP.value)
            or (action_value == SharingAction.STOP.value and phase != SharingPhase.STOPPED)
            or row["not_before"] is None
            or isinstance(row["not_before"], bool)
            or not isinstance(row["not_before"], (int, float))
            or not math.isfinite(float(row["not_before"]))
        ):
            raise ValueError("persisted sharing intent is inconsistent")
        # The snapshot never includes borrowed_identity; loading cannot restore it.
        state = SharingState(
            channel_uuid=snapshot["channel_uuid"],
            configuration_generation=snapshot["configuration_generation"],
            control_generation=snapshot["control_generation"],
            recorder_fence=int(row["recorder_fence"]),
            phase=phase,
            candidate_identity=tuple(candidate) if candidate is not None else None,
            candidate_connection_identity=(
                tuple(candidate_connection) if candidate_connection is not None else None
            ),
            candidate_since=snapshot.get("candidate_since"),
            borrowed_identity=None,
            last_viewer_left_at=snapshot.get("last_viewer_left_at"),
            last_switch_at=snapshot.get("last_switch_at"),
            pending_action=SharingAction(action_value) if action_value else None,
            intent_epoch=int(snapshot.get("intent_epoch", 0)),
        )
        intent = None
        if action_value is not None:
            target = SharingIntentLedger._strict_json_loads(row["target_identity_json"]) if row["target_identity_json"] else None
            if target is not None and (
                not isinstance(target, list)
                or len(target) != 8
                or any(not isinstance(part, str) or not part for part in target)
                or target[0] != row["channel_uuid"]
            ):
                raise ValueError("persisted target identity is invalid")
            if action_value == SharingAction.ATTACH_BORROW.value:
                if (
                    target is None
                    or row["target_admitted"] != 1
                    or tuple(target) != state.candidate_connection_identity
                ):
                    raise ValueError("persisted borrow intent target is inconsistent")
            elif target is not None or row["target_admitted"] != 0:
                raise ValueError("non-borrow intent contains target admission")
            intent = SharingIntentToken(
                channel_uuid=row["channel_uuid"],
                revision=int(row["revision"]),
                intent_epoch=int(row["intent_epoch"]),
                action=SharingAction(action_value),
                configuration_generation=row["configuration_generation"],
                control_generation=int(row["control_generation"]),
                recorder_fence=int(row["recorder_fence"]),
                target_identity=tuple(target) if target is not None else None,
                target_admitted=bool(row["target_admitted"]),
                not_before=float(row["not_before"]),
            )
        return PersistedSharingState(int(row["revision"]), state, intent)

    def recover(
        self,
        channel_uuid: str,
        *,
        configuration_generation: str,
        control_generation: int,
        recorder_fence: int,
        now: float,
        expected_revision: int | None,
        recovery_is_current: Callable[[str, int | None, str, int, int], bool],
    ) -> PersistedSharingState:
        """Restore clocks only after exact revision and current-owner validation.

        The callback must verify current recorder ownership/admission and the
        requested config/control/fence tuple. SQLite then CASes the revision
        and rejects fence/control rollback before replacing the row.
        """
        channel = self._channel(channel_uuid)
        now = self._finite_time(now, "now")
        if (
            not isinstance(configuration_generation, str)
            or not configuration_generation
            or type(control_generation) is not int
            or control_generation < 0
            or type(recorder_fence) is not int
            or recorder_fence < 0
            or (expected_revision is not None and (type(expected_revision) is not int or expected_revision < 1))
            or not callable(recovery_is_current)
        ):
            raise ValueError("recovery identity and revision must be valid")
        if recovery_is_current(
            channel, expected_revision, configuration_generation, control_generation, recorder_fence
        ) is not True:
            raise StaleSharingIntent("current recorder ownership did not authorize recovery")
        with self.action_lock(channel), self._database() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM sharing_runtime WHERE channel_uuid=?", (channel,)
            ).fetchone()
            if (row is None) != (expected_revision is None) or (
                row is not None and int(row["revision"]) != expected_revision
            ):
                db.rollback()
                raise StaleSharingIntent("sharing state revision changed during recovery")
            if row is None:
                from .sharing_planner import SharingObservation, SharingSettings, initial_state

                state = initial_state(SharingObservation(
                    channel_uuid=channel,
                    now=now,
                    configuration_generation=configuration_generation,
                    control_generation=control_generation,
                    recorder_fence=recorder_fence,
                    lease_is_current=True,
                    settings=SharingSettings(),
                ))
                revision = 1
            else:
                if recorder_fence < int(row["recorder_fence"]):
                    db.rollback()
                    raise StaleSharingIntent("recovery recorder fence cannot move backwards")
                if control_generation < int(row["control_generation"]):
                    db.rollback()
                    raise StaleSharingIntent("recovery control generation cannot move backwards")
                restored = restore_state(
                    self._strict_json_loads(row["state_json"]),
                    channel_uuid=channel,
                    configuration_generation=configuration_generation,
                    control_generation=control_generation,
                    recorder_fence=recorder_fence,
                    now=now,
                )
                old_epoch = int(row["intent_epoch"] or 0)
                state = replace(
                    restored,
                    phase=SharingPhase.DEDICATED,
                    borrowed_identity=None,
                    pending_action=None,
                    intent_epoch=max(restored.intent_epoch, old_epoch) + 1,
                )
                revision = int(row["revision"]) + 1
            db.execute(
                """INSERT INTO sharing_runtime (
                    channel_uuid, revision, state_json, action, intent_epoch,
                    configuration_generation, control_generation, recorder_fence,
                    target_identity_json, target_admitted, not_before, updated_at
                ) VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, NULL, 0, NULL, ?)
                ON CONFLICT(channel_uuid) DO UPDATE SET
                    revision=excluded.revision,
                    state_json=excluded.state_json,
                    action=NULL,
                    intent_epoch=NULL,
                    configuration_generation=excluded.configuration_generation,
                    control_generation=excluded.control_generation,
                    recorder_fence=excluded.recorder_fence,
                    target_identity_json=NULL,
                    target_admitted=0,
                    not_before=NULL,
                    updated_at=excluded.updated_at""",
                (
                    channel,
                    revision,
                    self._state_json(state),
                    configuration_generation,
                    control_generation,
                    recorder_fence,
                    now,
                ),
            )
            db.commit()
        result = self.read(channel)
        assert result is not None
        return result

    def run_if_current(
        self,
        token: SharingIntentToken,
        *,
        fresh_validate: Callable[[SharingIntentToken], bool],
        side_effect: Callable[[SharingIntentToken], object],
    ) -> tuple[bool, object | None]:
        """Claim one intent durably before its one permitted side-effect call.

        A short BEGIN IMMEDIATE transaction consumes this exact revision once
        across processes. A crash after the claim fails closed: the same token
        is never automatically retried. The effect callback must still perform
        its native/current lease/config/control/target CAS at the mutation
        boundary; no SQLite transaction or Python lock spans the callback.
        """
        if not isinstance(token, SharingIntentToken):
            raise ValueError("token must be SharingIntentToken")
        if not callable(fresh_validate) or not callable(side_effect):
            raise ValueError("fresh_validate and side_effect must be callable")
        with self.action_lock(token.channel_uuid):
            current = self.read(token.channel_uuid)
            if current is None or current.intent != token:
                return False, None
            if self._finite_time(self.clock(), "clock") < float(token.not_before):
                return False, None
            if fresh_validate(token) is not True:
                return False, None
            # Claim and recheck the deadline after proof, under SQLite's
            # cross-process write lock. This transaction contains no callback.
            now = self._finite_time(self.clock(), "clock")
            if now < float(token.not_before):
                return False, None
            nonce = uuid.uuid4().hex
            with self._database() as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute(
                    "SELECT * FROM sharing_runtime WHERE channel_uuid=?", (token.channel_uuid,)
                ).fetchone()
                if row is None or self._decode(row).intent != token:
                    db.rollback()
                    return False, None
                dispatch_time = self._finite_time(self.clock(), "clock")
                if dispatch_time < float(token.not_before):
                    db.rollback()
                    return False, None
                inserted = db.execute(
                    """INSERT OR IGNORE INTO sharing_intent_claims
                    (channel_uuid, revision, intent_epoch, action, claim_nonce, claimed_at)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (token.channel_uuid, token.revision, token.intent_epoch,
                     token.action.value, nonce, dispatch_time),
                ).rowcount
                if inserted != 1:
                    db.rollback()
                    return False, None
                db.commit()
            # Refresh the native/current-lease proof once more after the
            # durable claim. Its effect boundary must still use a native CAS.
            if fresh_validate(token) is not True:
                return False, None
            if self._finite_time(self.clock(), "clock") < float(token.not_before):
                return False, None
            current = self.read(token.channel_uuid)
            if current is None or current.intent != token:
                return False, None
            if self._finite_time(self.clock(), "clock") < float(token.not_before):
                return False, None
            return True, side_effect(token)
