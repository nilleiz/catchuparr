"""Durable authority for one successfully attached borrowed-reader episode.

This is an unwired coordinator foundation. It does not inspect native state or
perform side effects. The caller may activate an episode only after consuming
the exact ATTACH_BORROW claim and receiving acknowledgment for its own native
client registration. SQLite is used for transitions/recovery only, never for
the media byte loop.
"""

from __future__ import annotations

import json
import math
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .adapters.sharing_reader import BorrowReaderUnavailable, redis_backend_signature
from .sharing_persistence import SharingIntentToken
from .sharing_planner import SharingAction


class BorrowEpisodeRejected(RuntimeError):
    """The active borrowed-reader authority is stale or inconsistent."""


@dataclass(frozen=True)
class BorrowEpisodeSnapshot:
    episode_id: str
    channel_uuid: str
    phase: str
    configuration_generation: str
    control_generation: int
    recorder_fence: int
    intent_epoch: int
    identity: tuple[str, ...]
    lease_digest: str
    client_id: str | None
    connected_at: str | None
    version: int
    last_viewer_left_at: float | None
    return_deadline: float | None

    def __post_init__(self) -> None:
        if (not isinstance(self.episode_id, str) or not self.episode_id
                or not isinstance(self.channel_uuid, str) or not self.channel_uuid):
            raise ValueError("episode and channel identity must be non-empty strings")
        if self.phase not in {"opening", "active", "wait_return", "closing", "cleanup_pending", "closed"}:
            raise ValueError("episode phase is invalid")
        if not isinstance(self.configuration_generation, str) or not self.configuration_generation:
            raise ValueError("configuration_generation must be non-empty")
        for name in ("control_generation", "recorder_fence", "intent_epoch", "version"):
            value = getattr(self, name)
            minimum = 1 if name == "version" else 0
            if type(value) is not int or value < minimum:
                raise ValueError(f"{name} is invalid")
        if (not isinstance(self.identity, tuple) or len(self.identity) != 8
                or any(not isinstance(part, str) or not part for part in self.identity)
                or self.identity[0] != self.channel_uuid):
            raise ValueError("identity must be the exact eight-field channel binding")
        if (not isinstance(self.lease_digest, str) or len(self.lease_digest) != 64
                or any(char not in "0123456789abcdef" for char in self.lease_digest)):
            raise ValueError("lease_digest is invalid")
        if self.client_id is not None and (not isinstance(self.client_id, str) or not self.client_id):
            raise ValueError("client_id must be non-empty or null")
        if self.connected_at is not None and (not isinstance(self.connected_at, str) or not self.connected_at):
            raise ValueError("connected_at must be non-empty or null")
        for name in ("last_viewer_left_at", "return_deadline"):
            value = getattr(self, name)
            if value is not None:
                _finite_time(value, name)
        if self.phase == "opening" and self.connected_at is not None:
            raise ValueError("opening episode cannot already have a connected client")
        if self.phase in {"active", "wait_return"} and (self.client_id is None or self.connected_at is None):
            raise ValueError("active episode must bind its exact native client")
        if self.phase == "active" and (self.last_viewer_left_at is not None or self.return_deadline is not None):
            raise ValueError("active episode cannot retain last-viewer grace")
        if self.phase == "wait_return":
            if self.last_viewer_left_at is None or self.return_deadline is None:
                raise ValueError("wait_return episode requires finite grace timestamps")
            if self.return_deadline < self.last_viewer_left_at:
                raise ValueError("return deadline cannot precede last-viewer time")
        if self.phase in {"closing", "cleanup_pending", "closed"} and self.return_deadline is not None:
            raise ValueError("revoked episode cannot retain return grace")


def _finite_time(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(float(value)):
        raise ValueError(f"{name} must be a finite wall-clock timestamp")
    return float(value)


class BorrowEpisodeLedger:
    """Short SQLite transitions for opening/active/grace/revocation state."""

    def __init__(self, database_path: Path, *, clock: Callable[[], float] = time.time):
        self.database_path = Path(database_path).expanduser().resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        if not callable(clock):
            raise ValueError("clock must be callable")
        self.clock = clock
        with self._db() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.execute(
                """CREATE TABLE IF NOT EXISTS sharing_borrow_episodes (
                    channel_uuid TEXT PRIMARY KEY,
                    episode_id TEXT NOT NULL UNIQUE,
                    phase TEXT NOT NULL,
                    configuration_generation TEXT NOT NULL,
                    control_generation INTEGER NOT NULL,
                    recorder_fence INTEGER NOT NULL,
                    intent_epoch INTEGER NOT NULL,
                    identity_json TEXT NOT NULL,
                    lease_digest TEXT NOT NULL,
                    client_id TEXT,
                    connected_at TEXT,
                    version INTEGER NOT NULL,
                    last_viewer_left_at REAL,
                    return_deadline REAL,
                    updated_at REAL NOT NULL
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS sharing_borrow_episode_claims (
                    channel_uuid TEXT NOT NULL,
                    configuration_generation TEXT NOT NULL,
                    control_generation INTEGER NOT NULL,
                    recorder_fence INTEGER NOT NULL,
                    intent_epoch INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    episode_id TEXT NOT NULL UNIQUE,
                    claimed_at REAL NOT NULL,
                    PRIMARY KEY (channel_uuid, configuration_generation,
                                 control_generation, recorder_fence, intent_epoch, action)
                )"""
            )

    def _db(self):
        db = sqlite3.connect(self.database_path, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=5000")
        return db

    @staticmethod
    def _valid_identity(identity: tuple[str, ...], channel: str) -> None:
        if (not isinstance(identity, tuple) or len(identity) != 8
                or any(not isinstance(part, str) or not part for part in identity)
                or identity[0] != channel):
            raise ValueError("identity must be the exact eight-field native channel identity")

    def begin_open(self, token: SharingIntentToken, *, lease_digest: str,
                   identity: tuple[str, ...], client_id: str | None = None,
                   expected_version: int | None = None) -> BorrowEpisodeSnapshot:
        """Persist the exact private client attempt before any Redis side effect."""
        if not isinstance(token, SharingIntentToken) or token.action is not SharingAction.ATTACH_BORROW:
            raise ValueError("opening requires a consumed ATTACH_BORROW token")
        self._valid_identity(identity, token.channel_uuid)
        if identity != token.target_identity or not token.target_admitted:
            raise ValueError("opening identity differs from admitted intent target")
        if client_id is not None and (
            not isinstance(client_id, str) or not client_id.startswith("catchuparr-client-")
        ):
            raise ValueError("opening client_id must be a private recorder client id")
        if not isinstance(lease_digest, str) or len(lease_digest) != 64 or any(c not in "0123456789abcdef" for c in lease_digest):
            raise ValueError("lease_digest must be a lowercase SHA-256 digest")
        now = _finite_time(self.clock(), "clock")
        episode_id = uuid.uuid4().hex
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM sharing_borrow_episodes WHERE channel_uuid=?", (token.channel_uuid,)).fetchone()
            if row is not None:
                # A transition may not repair or overwrite a corrupt authority
                # row. Validate the complete row before trusting its phase or
                # version, while the write transaction is still held.
                self._snapshot_from_row(row)
            if (row is None) != (expected_version is None) or (row is not None and row["version"] != expected_version):
                db.rollback()
                raise BorrowEpisodeRejected("episode changed before opening")
            if row is not None and row["phase"] != "closed":
                db.rollback()
                raise BorrowEpisodeRejected("previous episode cleanup is not acknowledged")
            version = 1 if row is None else expected_version + 1
            claim = db.execute(
                """INSERT OR IGNORE INTO sharing_borrow_episode_claims
                (channel_uuid,configuration_generation,control_generation,recorder_fence,
                 intent_epoch,action,episode_id,claimed_at) VALUES (?,?,?,?,?,?,?,?)""",
                (token.channel_uuid, token.configuration_generation, token.control_generation,
                 token.recorder_fence, token.intent_epoch, token.action.value, episode_id, now),
            )
            if claim.rowcount != 1:
                db.rollback()
                raise BorrowEpisodeRejected("logical opening claim was already consumed")
            db.execute(
                """INSERT INTO sharing_borrow_episodes VALUES (?, ?, 'opening', ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, NULL, ?)
                ON CONFLICT(channel_uuid) DO UPDATE SET episode_id=excluded.episode_id,phase='opening',
                configuration_generation=excluded.configuration_generation,control_generation=excluded.control_generation,
                recorder_fence=excluded.recorder_fence,intent_epoch=excluded.intent_epoch,identity_json=excluded.identity_json,
                lease_digest=excluded.lease_digest,client_id=excluded.client_id,connected_at=NULL,version=excluded.version,
                last_viewer_left_at=NULL,return_deadline=NULL,updated_at=excluded.updated_at""",
                (token.channel_uuid, episode_id, token.configuration_generation, token.control_generation,
                 token.recorder_fence, token.intent_epoch, json.dumps(identity, separators=(",", ":")),
                 lease_digest, client_id, version, now),
            )
            db.commit()
        return self.read(token.channel_uuid)

    def activate(self, episode_id: str, *, expected_version: int, client_id: str,
                 connected_at: str, ordinary_viewer_ids: tuple[str, ...]) -> BorrowEpisodeSnapshot:
        """Acknowledge exact native registration; opening authority becomes an episode."""
        if not isinstance(client_id, str) or not client_id or not isinstance(connected_at, str) or not connected_at:
            raise ValueError("exact registered client identity is required")
        if not isinstance(ordinary_viewer_ids, tuple) or not ordinary_viewer_ids:
            raise BorrowEpisodeRejected("a new borrow requires an ordinary viewer at registration")
        if any(not isinstance(value, str) or not value for value in ordinary_viewer_ids):
            raise ValueError("ordinary viewer ids must be non-empty strings")
        now = _finite_time(self.clock(), "clock")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM sharing_borrow_episodes WHERE episode_id=?", (episode_id,)).fetchone()
            if row is not None:
                self._snapshot_from_row(row)
            if row is None or row["phase"] != "opening" or row["version"] != expected_version:
                db.rollback()
                raise BorrowEpisodeRejected("opening episode is stale")
            version = expected_version + 1
            cur = db.execute("UPDATE sharing_borrow_episodes SET phase='active',client_id=?,connected_at=?,version=?,updated_at=? WHERE episode_id=? AND version=?",
                             (client_id, connected_at, version, now, episode_id, expected_version))
            if cur.rowcount != 1:
                db.rollback()
                raise BorrowEpisodeRejected("opening episode changed during activation")
            db.commit()
        return self.read(row["channel_uuid"])

    def update_viewers(self, episode_id: str, *, expected_version: int,
                       ordinary_viewer_ids: tuple[str, ...], observed_at: float,
                       return_seconds: int, minimum_switch_seconds: int,
                       last_switch_at: float | None = None) -> BorrowEpisodeSnapshot:
        """Update grace from planner-owned wall clocks; caller must provide fresh native proof."""
        now = _finite_time(observed_at, "observed_at")
        if not isinstance(ordinary_viewer_ids, tuple) or any(not isinstance(x, str) or not x for x in ordinary_viewer_ids):
            raise ValueError("ordinary_viewer_ids must be a tuple of non-empty strings")
        for name, value in (("return_seconds", return_seconds), ("minimum_switch_seconds", minimum_switch_seconds)):
            if type(value) is not int or not 0 <= value <= 3600:
                raise ValueError(f"{name} must be an integer from 0 to 3600")
        switch_at = None if last_switch_at is None else _finite_time(last_switch_at, "last_switch_at")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM sharing_borrow_episodes WHERE episode_id=?", (episode_id,)).fetchone()
            if row is not None:
                self._snapshot_from_row(row)
            if row is None or row["phase"] not in {"active", "wait_return"} or row["version"] != expected_version:
                db.rollback()
                raise BorrowEpisodeRejected("active episode is stale or revoked")
            if ordinary_viewer_ids:
                if row["phase"] == "wait_return" and now >= float(row["return_deadline"]):
                    phase, left, deadline = "closing", row["last_viewer_left_at"], None
                else:
                    phase, left, deadline = "active", None, None
            elif row["phase"] == "wait_return":
                phase, left, deadline = row["phase"], row["last_viewer_left_at"], row["return_deadline"]
            else:
                phase, left = "wait_return", now
                # Recovery may observe a backwards wall-clock jump. Do not let
                # any future last-switch timestamp extend cooldown/grace.
                normalized_switch = min(switch_at, now) if switch_at is not None else now
                deadline = max(now + return_seconds, normalized_switch + minimum_switch_seconds)
            if not ordinary_viewer_ids and (deadline is None or now >= float(deadline)):
                phase, deadline = "closing", None
            version = expected_version + 1
            cur = db.execute("UPDATE sharing_borrow_episodes SET phase=?,last_viewer_left_at=?,return_deadline=?,version=?,updated_at=? WHERE episode_id=? AND version=?",
                             (phase, left, deadline, version, now, episode_id, expected_version))
            if cur.rowcount != 1:
                db.rollback()
                raise BorrowEpisodeRejected("episode changed during viewer update")
            db.commit()
        return self.read(row["channel_uuid"])

    def revoke(self, episode_id: str, *, expected_version: int, reason: str,
               cleanup_acknowledged: bool = False) -> BorrowEpisodeSnapshot:
        if reason not in {"stop", "mandatory_failure", "identity_changed", "disabled", "cleanup_pending", "closed"}:
            raise ValueError("unsupported revocation reason")
        if type(cleanup_acknowledged) is not bool:
            raise ValueError("cleanup_acknowledged must be boolean")
        now = _finite_time(self.clock(), "clock")
        phase = "closed" if cleanup_acknowledged else "cleanup_pending"
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM sharing_borrow_episodes WHERE episode_id=?", (episode_id,)).fetchone()
            if row is not None:
                self._snapshot_from_row(row)
            if row is None or row["version"] != expected_version:
                db.rollback()
                raise BorrowEpisodeRejected("episode changed before revocation")
            cur = db.execute("UPDATE sharing_borrow_episodes SET phase=?,version=version+1,return_deadline=NULL,updated_at=? WHERE episode_id=? AND version=?",
                             (phase, now, episode_id, expected_version))
            if cur.rowcount != 1:
                db.rollback()
                raise BorrowEpisodeRejected("episode changed during revocation")
            db.commit()
        return self.read(row["channel_uuid"])

    def read(self, channel_uuid: str) -> BorrowEpisodeSnapshot:
        if not isinstance(channel_uuid, str) or not channel_uuid:
            raise ValueError("channel_uuid must be non-empty")
        with self._db() as db:
            row = db.execute("SELECT * FROM sharing_borrow_episodes WHERE channel_uuid=?", (channel_uuid,)).fetchone()
        if row is None:
            raise BorrowEpisodeRejected("no active borrow episode")
        return self._snapshot_from_row(row)

    def pending_cleanup(self) -> tuple[BorrowEpisodeSnapshot, ...]:
        """List durable non-closed episodes for exact startup reconciliation."""
        with self._db() as db:
            rows = db.execute(
                "SELECT * FROM sharing_borrow_episodes WHERE phase != 'closed' ORDER BY updated_at"
            ).fetchall()
        # A malformed row aborts the snapshot; callers must fail closed instead
        # of skipping an operation whose native resources might still exist.
        return tuple(self._snapshot_from_row(row) for row in rows)

    @staticmethod
    def _snapshot_from_row(row: sqlite3.Row) -> BorrowEpisodeSnapshot:
        """Decode and validate a persisted row before any transition trusts it."""
        try:
            identity = json.loads(row["identity_json"], parse_constant=lambda _: (_ for _ in ()).throw(ValueError("invalid JSON constant")))
            if not isinstance(identity, list):
                raise ValueError("persisted identity must be a list")
            updated_at = row["updated_at"]
            _finite_time(updated_at, "updated_at")
            return BorrowEpisodeSnapshot(
                episode_id=row["episode_id"], channel_uuid=row["channel_uuid"], phase=row["phase"],
                configuration_generation=row["configuration_generation"],
                control_generation=row["control_generation"], recorder_fence=row["recorder_fence"],
                intent_epoch=row["intent_epoch"], identity=tuple(identity), lease_digest=row["lease_digest"],
                client_id=row["client_id"], connected_at=row["connected_at"], version=row["version"],
                last_viewer_left_at=row["last_viewer_left_at"], return_deadline=row["return_deadline"],
            )
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise BorrowEpisodeRejected("persisted borrow episode is malformed") from exc

    @staticmethod
    def authorize_snapshot(snapshot: BorrowEpisodeSnapshot, *, episode_id: str,
                           configuration_generation: str, control_generation: int,
                           recorder_fence: int, identity: tuple[str, ...], lease_digest: str,
                           ordinary_viewer_ids: tuple[str, ...], checked_at: float) -> float | None:
        """Return only the authenticated episode grace deadline; never opens without viewers."""
        if not isinstance(snapshot, BorrowEpisodeSnapshot):
            raise BorrowEpisodeRejected("borrow episode snapshot is unavailable")
        try:
            snapshot.__post_init__()
            _finite_time(checked_at, "checked_at")
        except (TypeError, ValueError) as exc:
            raise BorrowEpisodeRejected("borrow episode snapshot is malformed") from exc
        if (not isinstance(episode_id, str) or not isinstance(configuration_generation, str)
                or type(control_generation) is not int or control_generation < 0
                or type(recorder_fence) is not int or recorder_fence < 0
                or not isinstance(identity, tuple) or len(identity) != 8
                or not isinstance(lease_digest, str) or len(lease_digest) != 64):
            raise BorrowEpisodeRejected("borrow proof identity is malformed")
        now = _finite_time(checked_at, "checked_at")
        if (snapshot.episode_id != episode_id or snapshot.phase not in {"active", "wait_return"}
                or snapshot.configuration_generation != configuration_generation
                or snapshot.control_generation != control_generation or snapshot.recorder_fence != recorder_fence
                or snapshot.identity != identity or snapshot.lease_digest != lease_digest
                or snapshot.client_id is None or snapshot.connected_at is None):
            raise BorrowEpisodeRejected("borrow episode binding is stale")
        if not isinstance(ordinary_viewer_ids, tuple) or any(
            not isinstance(value, str) or not value for value in ordinary_viewer_ids
        ):
            raise BorrowEpisodeRejected("ordinary viewer evidence is malformed")
        if snapshot.phase == "active" and ordinary_viewer_ids:
            return None
        if snapshot.phase == "active":
            raise BorrowEpisodeRejected("a new zero-viewer borrow is not admitted")
        if ordinary_viewer_ids:
            raise BorrowEpisodeRejected("viewer return must clear the persisted grace state first")
        if snapshot.return_deadline is None or now >= snapshot.return_deadline:
            raise BorrowEpisodeRejected("borrow episode return grace expired")
        return snapshot.return_deadline


def select_task_local_client_manager(existing_manager, manager_type, *, channel_uuid: str,
                                     redis_client, worker_id: str):
    """Reuse an exact local manager or construct an owned one without native init.

    This function never writes the native ProxyServer manager map. The caller
    must pass only an already-local lookup result and must stop a constructed
    manager only after its exact client cleanup succeeds.
    """
    if not isinstance(channel_uuid, str) or not channel_uuid or not isinstance(worker_id, str) or not worker_id:
        raise BorrowReaderUnavailable("task-local manager identity is incomplete")
    target_signature = redis_backend_signature(redis_client)

    def matching(manager) -> bool:
        return (
            getattr(manager, "channel_id", None) == channel_uuid
            and getattr(manager, "redis_client", None) is not None
            and redis_backend_signature(manager.redis_client) == target_signature
            and callable(getattr(manager, "add_client", None))
            and callable(getattr(manager, "remove_client", None))
            and callable(getattr(manager, "stop", None))
        )

    if existing_manager is not None and matching(existing_manager):
        return existing_manager, False
    if not callable(manager_type):
        raise BorrowReaderUnavailable("native task-local manager constructor is unavailable")
    try:
        manager = manager_type(channel_id=channel_uuid, redis_client=redis_client, worker_id=worker_id)
    except Exception as exc:
        raise BorrowReaderUnavailable("native task-local manager could not be constructed") from exc
    if not matching(manager):
        stop = getattr(manager, "stop", None)
        if callable(stop):
            stop()
        raise BorrowReaderUnavailable("constructed native manager has an unsupported namespace")
    return manager, True
