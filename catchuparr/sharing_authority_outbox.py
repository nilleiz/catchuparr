"""Durable, monotonic publication of sharing authority changes.

Apply and recorder-control transitions stage authority targets before they
release their local deny marker. A target is acknowledged only after Redis
CAS publication and exact prior-episode invalidation are confirmed. This
module performs no ORM work and is never used by the media chunk iterator.
"""

from __future__ import annotations

import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable


@dataclass(frozen=True)
class SharingAuthorityTarget:
    channel_uuid: str
    configuration_generation: str
    control_generation: int
    catalog_generation: str

    def __post_init__(self) -> None:
        for name in ("channel_uuid", "configuration_generation", "catalog_generation"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be non-empty")
        if type(self.control_generation) is not int or self.control_generation < 0:
            raise ValueError("control_generation must be non-negative")


@dataclass(frozen=True)
class SharingAuthorityOutboxEvent:
    event_id: str
    activation_id: str
    channel_uuid: str
    expected_revision: int
    authority_revision: int
    configuration_generation: str
    control_generation: int
    catalog_generation: str


@dataclass(frozen=True)
class SharingAuthorityEventRecord:
    event: SharingAuthorityOutboxEvent
    status: str


@dataclass(frozen=True)
class SharingAuthorityDrain:
    completed_event_ids: tuple[str, ...]
    pending_event_ids: tuple[str, ...]


class SharingAuthorityOutbox:
    """SQLite source of revisions and pending Redis CAS publications."""

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
                """CREATE TABLE IF NOT EXISTS sharing_authority_revisions (
                    channel_uuid TEXT PRIMARY KEY,
                    acknowledged_revision INTEGER NOT NULL CHECK(acknowledged_revision >= 0)
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS sharing_authority_outbox (
                    event_id TEXT PRIMARY KEY,
                    activation_id TEXT NOT NULL,
                    channel_uuid TEXT NOT NULL,
                    expected_revision INTEGER NOT NULL,
                    authority_revision INTEGER NOT NULL,
                    configuration_generation TEXT NOT NULL,
                    control_generation INTEGER NOT NULL,
                    catalog_generation TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('pending','complete','cancelled')),
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(activation_id, channel_uuid)
                )"""
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS sharing_authority_pending "
                "ON sharing_authority_outbox(status, activation_id, channel_uuid)"
            )

    def _db(self):
        db = sqlite3.connect(self.database_path, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=5000")
        return db

    def stage(self, activation_id: str, targets: Iterable[SharingAuthorityTarget]) -> tuple[SharingAuthorityOutboxEvent, ...]:
        if not isinstance(activation_id, str) or not activation_id:
            raise ValueError("activation_id is required")
        normalized = tuple(targets)
        if any(not isinstance(target, SharingAuthorityTarget) for target in normalized):
            raise ValueError("authority targets are invalid")
        channels = [target.channel_uuid for target in normalized]
        if len(channels) != len(set(channels)):
            raise ValueError("authority targets contain duplicate channels")
        now = self._time(self.clock())
        staged: list[SharingAuthorityOutboxEvent] = []
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                for target in normalized:
                    pending = db.execute(
                        "SELECT 1 FROM sharing_authority_outbox "
                        "WHERE channel_uuid=? AND status='pending' LIMIT 1",
                        (target.channel_uuid,),
                    ).fetchone()
                    if pending is not None:
                        raise RuntimeError("a prior sharing authority publication remains pending")
                    state = db.execute(
                        "SELECT acknowledged_revision FROM sharing_authority_revisions WHERE channel_uuid=?",
                        (target.channel_uuid,),
                    ).fetchone()
                    expected = int(state["acknowledged_revision"]) if state else 0
                    event = SharingAuthorityOutboxEvent(
                        uuid.uuid4().hex, activation_id, target.channel_uuid, expected,
                        expected + 1, target.configuration_generation,
                        target.control_generation, target.catalog_generation,
                    )
                    db.execute(
                        """INSERT INTO sharing_authority_outbox VALUES
                        (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)""",
                        (event.event_id, event.activation_id, event.channel_uuid,
                         event.expected_revision, event.authority_revision,
                         event.configuration_generation, event.control_generation,
                         event.catalog_generation, now, now),
                    )
                    staged.append(event)
                db.commit()
            except Exception:
                db.rollback()
                raise
        return tuple(staged)

    def cancel_activation(self, activation_id: str) -> bool:
        if not isinstance(activation_id, str) or not activation_id:
            return False
        now = self._time(self.clock())
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute(
                "UPDATE sharing_authority_outbox SET status='cancelled',updated_at=? "
                "WHERE activation_id=? AND status='pending'",
                (now, activation_id),
            ).rowcount
            db.commit()
        return changed >= 0

    def cancel_event(self, event_id: str) -> bool:
        if not isinstance(event_id, str) or not event_id:
            return False
        now = self._time(self.clock())
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute(
                "UPDATE sharing_authority_outbox SET status='cancelled',updated_at=? "
                "WHERE event_id=? AND status='pending'",
                (now, event_id),
            ).rowcount
            db.commit()
        return changed == 1

    def pending(self, activation_id: str | None = None) -> tuple[SharingAuthorityOutboxEvent, ...]:
        with self._db() as db:
            if activation_id is None:
                rows = db.execute(
                    "SELECT * FROM sharing_authority_outbox WHERE status='pending' "
                    "ORDER BY created_at,event_id"
                ).fetchall()
            else:
                rows = db.execute(
                    "SELECT * FROM sharing_authority_outbox WHERE status='pending' "
                    "AND activation_id=? ORDER BY channel_uuid",
                    (activation_id,),
                ).fetchall()
        return tuple(self._event(row) for row in rows)

    def latest_for_channel(self, channel_uuid: str) -> SharingAuthorityEventRecord | None:
        if not isinstance(channel_uuid, str) or not channel_uuid:
            raise ValueError("channel_uuid is required")
        with self._db() as db:
            row = db.execute(
                "SELECT * FROM sharing_authority_outbox WHERE channel_uuid=? "
                "ORDER BY created_at DESC,event_id DESC LIMIT 1",
                (channel_uuid,),
            ).fetchone()
        if row is None:
            return None
        return SharingAuthorityEventRecord(self._event(row), row["status"])

    def drain(
        self,
        redis: Any,
        *,
        activation_id: str | None = None,
        read_prior: Callable[[Any, str], Any],
        publish: Callable[[Any, SharingAuthorityOutboxEvent], bool],
        invalidate: Callable[[Any, Any, SharingAuthorityOutboxEvent], bool],
    ) -> SharingAuthorityDrain:
        if not all(callable(item) for item in (read_prior, publish, invalidate)):
            raise ValueError("authority drain callbacks are required")
        try:
            events = self.pending(activation_id)
        except Exception:
            return SharingAuthorityDrain((), ("outbox-unreadable",))
        completed: list[str] = []
        pending: list[str] = []
        for event in events:
            try:
                prior = read_prior(redis, event.channel_uuid)
                published = publish(redis, event)
                if published is not True:
                    # A lost Lua reply is accepted only when the exact target
                    # hash is already visible at the event's revision.
                    if not self._target_is_current(redis, event):
                        pending.append(event.event_id)
                        continue
                if prior is not None and invalidate(redis, prior, event) is not True:
                    pending.append(event.event_id)
                    continue
                self._acknowledge(event)
            except Exception:
                pending.append(event.event_id)
            else:
                completed.append(event.event_id)
        return SharingAuthorityDrain(tuple(completed), tuple(pending))

    def _target_is_current(self, redis: Any, event: SharingAuthorityOutboxEvent) -> bool:
        try:
            raw = redis.hgetall(f"catchuparr:sharing:current:{event.channel_uuid}") or {}
            values = {
                self._text(key): self._text(value) for key, value in raw.items()
            }
            return (
                values.get("authority_revision") == str(event.authority_revision)
                and values.get("configuration_generation") == event.configuration_generation
                and values.get("control_generation") == str(event.control_generation)
                and values.get("catalog_generation") == event.catalog_generation
            )
        except Exception:
            return False

    def _acknowledge(self, event: SharingAuthorityOutboxEvent) -> None:
        now = self._time(self.clock())
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM sharing_authority_outbox WHERE event_id=?", (event.event_id,)
            ).fetchone()
            if row is None:
                db.rollback()
                raise RuntimeError("authority event disappeared")
            current = self._event(row)
            if current != event:
                db.rollback()
                raise RuntimeError("authority event changed before acknowledgment")
            revision = db.execute(
                "SELECT acknowledged_revision FROM sharing_authority_revisions WHERE channel_uuid=?",
                (event.channel_uuid,),
            ).fetchone()
            acknowledged = int(revision["acknowledged_revision"]) if revision else 0
            if acknowledged != event.expected_revision:
                db.rollback()
                raise RuntimeError("authority revision changed before acknowledgment")
            db.execute(
                "INSERT INTO sharing_authority_revisions VALUES (?, ?) "
                "ON CONFLICT(channel_uuid) DO UPDATE SET acknowledged_revision=excluded.acknowledged_revision",
                (event.channel_uuid, event.authority_revision),
            )
            db.execute(
                "UPDATE sharing_authority_outbox SET status='complete',updated_at=? "
                "WHERE event_id=? AND status='pending'",
                (now, event.event_id),
            )
            db.commit()

    @staticmethod
    def _event(row: sqlite3.Row) -> SharingAuthorityOutboxEvent:
        return SharingAuthorityOutboxEvent(
            row["event_id"], row["activation_id"], row["channel_uuid"],
            row["expected_revision"], row["authority_revision"],
            row["configuration_generation"], row["control_generation"],
            row["catalog_generation"],
        )

    @staticmethod
    def _text(value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="strict")
        if isinstance(value, str):
            return value
        raise ValueError("Redis returned an unsupported value")

    @staticmethod
    def _time(value: Any) -> float:
        import math

        if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(float(value)):
            raise ValueError("outbox clock must be finite")
        return float(value)
