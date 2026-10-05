"""Durable local index and segment storage for Catchuparr."""

from __future__ import annotations

import contextlib
import os
import shutil
import sqlite3
import tempfile
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

TIMELINE_GAP_TOLERANCE_SECONDS = 0.25


def _utc_epoch(value: datetime | str | int | float) -> float:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must be timezone-aware")
        return value.astimezone(timezone.utc).timestamp()
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return _utc_epoch(parsed)
    return float(value)


def _datetime(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def _channel_key(channel_id: str) -> str:
    value = str(channel_id).strip()
    if not value or value in {".", ".."} or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in value):
        raise ValueError("channel_id must contain only letters, digits, '_' or '-'")
    return value


@dataclass(frozen=True)
class Segment:
    id: str
    channel_id: str
    path: Path
    start_utc: datetime
    end_utc: datetime
    discontinuity: bool = False

    @property
    def duration(self) -> float:
        return (self.end_utc - self.start_utc).total_seconds()


@dataclass(frozen=True)
class Coverage:
    start_utc: datetime
    end_utc: datetime
    covered_seconds: float
    spans: tuple[tuple[datetime, datetime], ...]
    gaps: tuple[tuple[datetime, datetime], ...]

    @property
    def complete(self) -> bool:
        return not self.gaps


@dataclass(frozen=True)
class PlaybackLease:
    id: str
    channel_id: str
    start_utc: datetime
    end_utc: datetime
    expires_at: float


class ArchiveStore:
    """SQLite WAL index backed by immutable, atomically published files.

    Segment source files are copied into this store and are not consumed. All
    indexed times are UTC instants; naive datetimes are rejected deliberately.
    """

    def __init__(self, root: Path, *, orphan_grace_seconds: float | None = None):
        if orphan_grace_seconds is not None and orphan_grace_seconds < 0:
            raise ValueError("orphan_grace_seconds cannot be negative")
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / "archive.sqlite3"
        self._initialize()
        if orphan_grace_seconds is not None:
            self.reconcile_orphans(grace_seconds=orphan_grace_seconds)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=30000")
        return db

    @contextmanager
    def _database(self):
        db = self._connect()
        try:
            yield db
        finally:
            db.close()

    def _initialize(self) -> None:
        with self._database() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS segments (
                    id TEXT PRIMARY KEY,
                    channel_id TEXT NOT NULL,
                    relpath TEXT NOT NULL UNIQUE,
                    start_utc REAL NOT NULL,
                    end_utc REAL NOT NULL,
                    discontinuity INTEGER NOT NULL DEFAULT 0,
                    size_bytes INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    CHECK(end_utc > start_utc)
                );
                CREATE INDEX IF NOT EXISTS segments_time
                    ON segments(channel_id, start_utc, end_utc);
                CREATE TABLE IF NOT EXISTS programs (
                    id INTEGER PRIMARY KEY,
                    channel_id TEXT NOT NULL,
                    start_utc REAL NOT NULL,
                    end_utc REAL NOT NULL,
                    title TEXT NOT NULL,
                    payload TEXT NOT NULL DEFAULT '{}',
                    captured_at REAL NOT NULL,
                    CHECK(end_utc > start_utc)
                );
                CREATE INDEX IF NOT EXISTS programs_time
                    ON programs(channel_id, start_utc, end_utc);
                CREATE INDEX IF NOT EXISTS programs_identity
                    ON programs(channel_id, start_utc, end_utc, title);
                CREATE TABLE IF NOT EXISTS playback_leases (
                    id TEXT PRIMARY KEY,
                    channel_id TEXT NOT NULL,
                    start_utc REAL NOT NULL,
                    end_utc REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    created_at REAL NOT NULL,
                    CHECK(end_utc > start_utc)
                );
                CREATE INDEX IF NOT EXISTS leases_range
                    ON playback_leases(channel_id, start_utc, end_utc, expires_at);
                CREATE TABLE IF NOT EXISTS recorder_fences (
                    channel_id TEXT PRIMARY KEY,
                    token INTEGER NOT NULL
                );
                """
            )

    def add_segment(
        self,
        channel_id: str,
        path: Path,
        start_utc: datetime | str | int | float,
        end_utc: datetime | str | int | float,
        *,
        discontinuity: bool = False,
        segment_id: str | None = None,
        fencing_token: int | None = None,
    ) -> Segment:
        channel = _channel_key(channel_id)
        start, end = _utc_epoch(start_utc), _utc_epoch(end_utc)
        if end <= start:
            raise ValueError("end_utc must be after start_utc")
        source = Path(path).expanduser().resolve(strict=True)
        if not source.is_file():
            raise ValueError("segment source must be a regular file")
        sid = segment_id or uuid.uuid4().hex
        if not sid or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in sid):
            raise ValueError("segment_id contains unsupported characters")
        final_dir = self.root / "segments" / channel
        final_dir.mkdir(parents=True, exist_ok=True)
        final_path = final_dir / f"{sid}{source.suffix or '.ts'}"
        relpath = final_path.relative_to(self.root).as_posix()
        temp_path: Path | None = None
        published = False
        try:
            # Stage on the same filesystem so os.replace publishes atomically.
            fd, tmp_name = tempfile.mkstemp(prefix=".segment-", suffix=".partial", dir=final_dir)
            temp_path = Path(tmp_name)
            with os.fdopen(fd, "wb") as target, source.open("rb") as origin:
                shutil.copyfileobj(origin, target, 1024 * 1024)
                target.flush()
                os.fsync(target.fileno())
            size = temp_path.stat().st_size
            db = self._connect()
            try:
                db.execute("BEGIN IMMEDIATE")
                current = db.execute("SELECT token FROM recorder_fences WHERE channel_id=?", (channel,)).fetchone()
                if current is not None and fencing_token is None:
                    raise RuntimeError("fencing token required for this channel")
                if fencing_token is not None:
                    if current is not None and fencing_token < int(current[0]):
                        raise RuntimeError("stale recorder fencing token")
                    if current is None or fencing_token > int(current[0]):
                        db.execute(
                            "INSERT INTO recorder_fences(channel_id,token) VALUES(?,?) ON CONFLICT(channel_id) DO UPDATE SET token=excluded.token",
                            (channel, fencing_token),
                        )
                if final_path.exists():
                    raise FileExistsError(f"segment destination already exists: {final_path}")
                os.replace(temp_path, final_path)
                published = True
                dir_fd = os.open(final_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
                db.execute(
                    "INSERT INTO segments(id,channel_id,relpath,start_utc,end_utc,discontinuity,size_bytes,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (sid, channel, relpath, start, end, int(discontinuity), size, time.time()),
                )
                db.commit()
            except BaseException:
                db.rollback()
                raise
            finally:
                db.close()
        except BaseException:
            if temp_path is not None:
                with contextlib.suppress(FileNotFoundError):
                    temp_path.unlink()
            if published:
                with contextlib.suppress(FileNotFoundError):
                    final_path.unlink()
            raise
        return Segment(sid, channel, final_path, _datetime(start), _datetime(end), discontinuity)

    def recorder_fence(self, channel_id: str) -> int:
        """Return the last durable recorder fence, or zero before first use."""
        with self._database() as db:
            row = db.execute("SELECT token FROM recorder_fences WHERE channel_id=?", (_channel_key(channel_id),)).fetchone()
        return int(row[0]) if row is not None else 0

    def register_recorder_fence(self, channel_id: str, fencing_token: int) -> None:
        """Record a Redis fencing token before a recorder begins publishing.

        Any later ``add_segment`` call carrying a lower token is rejected.
        Equal tokens remain valid for the current recorder owner.
        """
        channel = _channel_key(channel_id)
        if fencing_token <= 0:
            raise ValueError("fencing_token must be positive")
        with self._database() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT token FROM recorder_fences WHERE channel_id=?", (channel,)).fetchone()
            if row is not None and fencing_token < int(row[0]):
                db.rollback()
                raise RuntimeError("recorder fencing token must increase")
            db.execute(
                "INSERT INTO recorder_fences(channel_id,token) VALUES(?,?) ON CONFLICT(channel_id) DO UPDATE SET token=excluded.token",
                (channel, fencing_token),
            )
            db.commit()

    def segments(
        self,
        channel_id: str,
        start_utc: datetime | str | int | float | None = None,
        end_utc: datetime | str | int | float | None = None,
    ) -> list[Segment]:
        channel = _channel_key(channel_id)
        clauses, params = ["channel_id=?"], [channel]
        if start_utc is not None:
            clauses.append("end_utc>? ")
            params.append(_utc_epoch(start_utc))
        if end_utc is not None:
            clauses.append("start_utc<?")
            params.append(_utc_epoch(end_utc))
        with self._database() as db:
            rows = db.execute(
                "SELECT * FROM segments WHERE " + " AND ".join(clauses) + " ORDER BY start_utc,end_utc,id", params
            ).fetchall()
        return [self._row_segment(row) for row in rows if (self.root / row["relpath"]).is_file()]

    def channel_stats(self, channel_id: str) -> dict:
        """Return indexed recorder progress without loading every segment."""
        channel = _channel_key(channel_id)
        with self._database() as db:
            row = db.execute(
                """SELECT COUNT(*) AS segments, COALESCE(SUM(size_bytes), 0) AS size_bytes,
                          MAX(end_utc) AS latest_end_utc,
                          COALESCE(SUM(discontinuity), 0) AS discontinuities
                   FROM segments WHERE channel_id=?""",
                (channel,),
            ).fetchone()
        return {
            "segments": int(row["segments"]),
            "size_bytes": int(row["size_bytes"]),
            "latest_end_utc": _datetime(row["latest_end_utc"]).isoformat() if row["latest_end_utc"] is not None else None,
            "discontinuities": int(row["discontinuities"]),
        }

    def indexed_size_bytes(self) -> int:
        """Count all archived channels, including ones no longer selected."""
        with self._database() as db:
            return int(db.execute("SELECT COALESCE(SUM(size_bytes), 0) FROM segments").fetchone()[0])

    def segment(self, channel_id: str, segment_id: str) -> Segment | None:
        """Look up one immutable segment by the indexed ID and channel."""
        channel = _channel_key(channel_id)
        if not segment_id or len(segment_id) > 128:
            return None
        with self._database() as db:
            row = db.execute(
                "SELECT * FROM segments WHERE id=? AND channel_id=?",
                (segment_id, channel),
            ).fetchone()
        if row is None or not (self.root / row["relpath"]).is_file():
            return None
        return self._row_segment(row)

    def _row_segment(self, row: sqlite3.Row) -> Segment:
        return Segment(
            row["id"], row["channel_id"], self.root / row["relpath"],
            _datetime(row["start_utc"]), _datetime(row["end_utc"]), bool(row["discontinuity"]),
        )

    def coverage(
        self,
        channel_id: str,
        start_utc: datetime | str | int | float,
        end_utc: datetime | str | int | float,
    ) -> Coverage:
        start, end = _utc_epoch(start_utc), _utc_epoch(end_utc)
        if end <= start:
            raise ValueError("end_utc must be after start_utc")
        rows = self.segments(channel_id, start, end)
        merged: list[list[float]] = []
        for item in rows:
            left, right = max(start, item.start_utc.timestamp()), min(end, item.end_utc.timestamp())
            if right <= left:
                continue
            # FFmpeg's segment CSV can leave a repeatable sub-frame offset
            # between otherwise continuous TS files (0.14 s on the Dev Vu+
            # stream). Treat only larger holes as unavailable archive time.
            if merged and left <= merged[-1][1] + TIMELINE_GAP_TOLERANCE_SECONDS:
                merged[-1][1] = max(merged[-1][1], right)
            else:
                merged.append([left, right])
        gaps: list[tuple[datetime, datetime]] = []
        cursor = start
        for left, right in merged:
            if left > cursor + TIMELINE_GAP_TOLERANCE_SECONDS:
                gaps.append((_datetime(cursor), _datetime(left)))
            cursor = max(cursor, right)
        if cursor < end - TIMELINE_GAP_TOLERANCE_SECONDS:
            gaps.append((_datetime(cursor), _datetime(end)))
        spans = tuple((_datetime(left), _datetime(right)) for left, right in merged)
        return Coverage(
            _datetime(start), _datetime(end),
            sum(right - left for left, right in merged), spans, tuple(gaps),
        )

    def save_program_snapshot(
        self,
        channel_id: str,
        start_utc: datetime | str | int | float,
        end_utc: datetime | str | int | float,
        title: str,
        payload: str | dict | None = None,
        *,
        captured_at: datetime | str | int | float | None = None,
    ) -> int:
        import json

        channel, start, end = _channel_key(channel_id), _utc_epoch(start_utc), _utc_epoch(end_utc)
        if end <= start:
            raise ValueError("end_utc must be after start_utc")
        body = payload if isinstance(payload, str) else json.dumps(payload or {}, sort_keys=True, separators=(",", ":"))
        with self._database() as db:
            existing = db.execute(
                "SELECT id FROM programs WHERE channel_id=? AND start_utc=? AND end_utc=? AND title=? LIMIT 1",
                (channel, start, end, str(title)),
            ).fetchone()
            if existing is not None:
                return int(existing[0])
            cur = db.execute(
                "INSERT INTO programs(channel_id,start_utc,end_utc,title,payload,captured_at) VALUES(?,?,?,?,?,?)",
                (channel, start, end, str(title), body, _utc_epoch(captured_at) if captured_at is not None else time.time()),
            )
            return int(cur.lastrowid)

    def program_snapshots(
        self, channel_id: str, start_utc: datetime | str | int | float, end_utc: datetime | str | int | float
    ) -> list[dict]:
        import json

        with self._database() as db:
            rows = db.execute(
                "SELECT * FROM programs WHERE channel_id=? AND end_utc>? AND start_utc<? ORDER BY start_utc,captured_at",
                (_channel_key(channel_id), _utc_epoch(start_utc), _utc_epoch(end_utc)),
            ).fetchall()
        return [
            {"channel_id": r["channel_id"], "start_utc": _datetime(r["start_utc"]), "end_utc": _datetime(r["end_utc"]),
             "title": r["title"], "payload": json.loads(r["payload"]), "captured_at": _datetime(r["captured_at"])}
            for r in rows
        ]

    def begin_playback(
        self, channel_id: str, start_utc: datetime | str | int | float, end_utc: datetime | str | int | float,
        *, ttl_seconds: float = 120,
    ) -> PlaybackLease:
        channel, start, end = _channel_key(channel_id), _utc_epoch(start_utc), _utc_epoch(end_utc)
        if end <= start or ttl_seconds <= 0:
            raise ValueError("playback range and ttl_seconds must be positive")
        lease = PlaybackLease(uuid.uuid4().hex, channel, _datetime(start), _datetime(end), time.time() + ttl_seconds)
        with self._database() as db:
            db.execute(
                "INSERT INTO playback_leases(id,channel_id,start_utc,end_utc,expires_at,created_at) VALUES(?,?,?,?,?,?)",
                (lease.id, channel, start, end, lease.expires_at, time.time()),
            )
        return lease

    def renew_playback(self, lease_id: str, *, ttl_seconds: float = 120) -> bool:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        with self._database() as db:
            cur = db.execute("UPDATE playback_leases SET expires_at=? WHERE id=? AND expires_at>?", (time.time() + ttl_seconds, lease_id, time.time()))
            return cur.rowcount == 1

    def extend_playback(
        self, lease_id: str, end_utc: datetime | str | int | float, *, ttl_seconds: float = 120
    ) -> bool:
        """Renew an EVENT playback lease and extend its protected window end.

        EVENT playlists append segments and cannot discard early entries, so a
        session should keep this lease from the event's first segment and
        extend it on every playlist reload.
        """
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        with self._database() as db:
            cur = db.execute(
                "UPDATE playback_leases SET end_utc=MAX(end_utc,?),expires_at=? WHERE id=? AND expires_at>?",
                (_utc_epoch(end_utc), time.time() + ttl_seconds, lease_id, time.time()),
            )
            return cur.rowcount == 1

    def end_playback(self, lease_id: str) -> None:
        with self._database() as db:
            db.execute("DELETE FROM playback_leases WHERE id=?", (lease_id,))

    def reconcile_orphans(self, *, grace_seconds: float = 3600) -> tuple[int, int]:
        """Remove stale unindexed segment files and index rows with missing files.

        Recent unindexed files are left alone because another worker may be in
        the rename-to-database-commit window. SQLite WAL/SHM files are managed
        by SQLite itself and are intentionally not touched.
        """
        if grace_seconds < 0:
            raise ValueError("grace_seconds cannot be negative")
        cutoff = time.time() - grace_seconds
        removed_files = removed_rows = 0
        with self._database() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute("SELECT id,relpath FROM segments").fetchall()
            for row in rows:
                if not (self.root / row["relpath"]).is_file():
                    db.execute("DELETE FROM segments WHERE id=?", (row["id"],))
                    removed_rows += 1
            indexed = {row[0] for row in db.execute("SELECT relpath FROM segments")}
            db.commit()
        segment_root = self.root / "segments"
        if not segment_root.exists():
            return removed_files, removed_rows
        for path in segment_root.rglob("*"):
            if not path.is_file():
                continue
            relpath = path.relative_to(self.root).as_posix()
            try:
                stale = path.stat().st_mtime <= cutoff
            except FileNotFoundError:
                continue
            if relpath not in indexed and stale:
                with contextlib.suppress(FileNotFoundError):
                    path.unlink()
                    removed_files += 1
        for directory in sorted((p for p in segment_root.rglob("*") if p.is_dir()), reverse=True):
            with contextlib.suppress(OSError):
                directory.rmdir()
        return removed_files, removed_rows

    def cleanup(self, *, older_than_utc: datetime | str | int | float, max_bytes: int | None = None) -> list[Path]:
        """Delete expired content by age and, if requested, oldest-first by quota.

        Any segment overlapping a live playback lease is protected. Files are
        removed only after the row is deleted under an immediate transaction.
        """
        cutoff = _utc_epoch(older_than_utc)
        if max_bytes is not None and max_bytes < 0:
            raise ValueError("max_bytes cannot be negative")
        removed: list[Path] = []
        now = time.time()
        with self._database() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("DELETE FROM playback_leases WHERE expires_at<=?", (now,))
            rows = db.execute(
                "SELECT s.* FROM segments s WHERE s.end_utc<=? AND NOT EXISTS (SELECT 1 FROM playback_leases l WHERE l.channel_id=s.channel_id AND l.start_utc<s.end_utc AND l.end_utc>s.start_utc AND l.expires_at>?) ORDER BY s.end_utc,s.start_utc",
                (cutoff, now),
            ).fetchall()
            for row in rows:
                db.execute("DELETE FROM segments WHERE id=?", (row["id"],))
                removed.append(self.root / row["relpath"])
            if max_bytes is not None:
                total = int(db.execute("SELECT COALESCE(SUM(size_bytes),0) FROM segments").fetchone()[0])
                candidates = db.execute(
                    "SELECT s.* FROM segments s WHERE NOT EXISTS (SELECT 1 FROM playback_leases l WHERE l.channel_id=s.channel_id AND l.start_utc<s.end_utc AND l.end_utc>s.start_utc AND l.expires_at>?) ORDER BY s.end_utc,s.start_utc",
                    (now,),
                ).fetchall()
                for row in candidates:
                    if total <= max_bytes:
                        break
                    db.execute("DELETE FROM segments WHERE id=?", (row["id"],))
                    removed.append(self.root / row["relpath"])
                    total -= int(row["size_bytes"])
            db.commit()
        for path in removed:
            with contextlib.suppress(FileNotFoundError):
                path.unlink()
        return removed
