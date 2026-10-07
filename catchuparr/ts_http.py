"""Bounded HTTP service for XC-compatible MPEG-TS archive playback.

The XC adapter authenticates and authorizes requests before calling this
service. Its user-facing entry point accepts a Dispatcharr user ID and a
stable, already-hashed device key; the optional token entry point is useful
for standalone callers. Session rows and archive leases share the ArchiveStore
database, while TS sessions remain separate from HLS admission state.
"""

from __future__ import annotations

import hashlib
import inspect
import re
import sqlite3
import time
import uuid
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

from .engine.admission import playback_admission_lock
from .http import RangeNotSatisfiable, parse_byte_range

_TS_STREAM_CHUNK_BYTES = 64 * 1024
_SESSION_ID_RE = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")


@dataclass(frozen=True)
class TSHTTPResponse:
    status: int
    headers: dict[str, str]
    body: bytes = b""
    lease_id: str | None = None


class _CleanupIterator:
    """Forward a byte iterator and release its lease exactly once."""

    def __init__(self, iterator: Iterator[bytes], on_close: Callable[[], None]):
        self._iterator = iterator
        self._on_close = on_close
        self._closed = False

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        if self._closed:
            raise StopIteration
        try:
            return next(self._iterator)
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        close = getattr(self._iterator, "close", None)
        try:
            if close is not None:
                close()
        finally:
            self._on_close()


@dataclass
class StreamingTSHTTPResponse:
    status: int
    headers: dict[str, str]
    body: _CleanupIterator
    lease_id: str
    _closed: bool = False

    def __iter__(self) -> Iterator[bytes]:
        return self.body

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.body.close()


class _PermissionDenied(Exception):
    pass


class _PlaybackWindowTooWide(ValueError):
    pass


def _error(status: int, message: str = "", *, headers: dict[str, str] | None = None) -> TSHTTPResponse:
    body = message.encode("utf-8") if message else b""
    result_headers = {"Content-Length": str(len(body)), "Cache-Control": "no-store"}
    if body:
        result_headers["Content-Type"] = "text/plain; charset=utf-8"
    if headers:
        result_headers.update(headers)
    return TSHTTPResponse(status, result_headers, body)


def _epoch(value: datetime | str | int | float) -> float:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must be timezone-aware")
        return value.astimezone(timezone.utc).timestamp()
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return _epoch(parsed)
    return float(value)


class ArchiveTSPlaybackService:
    """Serve a bounded time window as one XC-compatible TS byte resource.

    A request with a stable ``session_id`` reuses one archive lease across
    seeks. The lease protects the union of requested times and is capped at
    ``max_lease_span_seconds``. Responses read immutable segment files in
    bounded chunks. Full responses and byte ranges crossing a gap or explicit
    discontinuity are rejected because raw TS concatenation is not safe there.

    ``allow_new_session`` receives the number of other local sessions for the
    user and, when supported, the hashed device key. Runtime callers should
    combine TS and HLS session counts with Dispatcharr's active connections.
    """

    max_lease_span_seconds = 24 * 60 * 60

    def __init__(
        self,
        store,
        token_store=None,
        *,
        authorize_user_channel: Callable[[str, str], bool],
        catchup_enabled: Callable[[str, str], bool],
        allow_new_session: Callable[..., bool] | None = None,
        lease_ttl_seconds: float = 4 * 60 * 60,
        clock: Callable[[], float] = time.time,
    ):
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be positive")
        self.store = store
        self.tokens = token_store
        self.authorize_user_channel = authorize_user_channel
        self.catchup_enabled = catchup_enabled
        self.allow_new_session = allow_new_session
        self.lease_ttl_seconds = lease_ttl_seconds
        self.clock = clock
        self._init_sessions()

    @staticmethod
    def _callback_accepts_device(callback: Callable[..., Any] | None) -> bool:
        if callback is None:
            return False
        try:
            inspect.signature(callback).bind("user", "channel", 0, "device")
        except (TypeError, ValueError):
            return False
        return True

    def _connect(self) -> sqlite3.Connection:
        database = Path(self.store.root) / "archive.sqlite3"
        db = sqlite3.connect(database, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")
        return db

    def _init_sessions(self) -> None:
        with closing(self._connect()) as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS ts_playback_sessions (
                    lease_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    device_key TEXT NOT NULL,
                    request_key TEXT NOT NULL,
                    start_utc REAL NOT NULL,
                    end_utc REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1
                )"""
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS ts_sessions_user "
                "ON ts_playback_sessions(user_id, active, expires_at)"
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS ts_sessions_request "
                "ON ts_playback_sessions(request_key, active, expires_at)"
            )
            db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ts_sessions_active_device "
                "ON ts_playback_sessions(device_key) WHERE active=1"
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS ts_playback_streams (
                    id TEXT PRIMARY KEY,
                    lease_id TEXT NOT NULL,
                    expires_at REAL NOT NULL
                )"""
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS ts_streams_lease "
                "ON ts_playback_streams(lease_id, expires_at)"
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS ts_playback_admissions (
                    request_key TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    device_key TEXT NOT NULL,
                    expires_at REAL NOT NULL
                )"""
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS ts_admissions_user "
                "ON ts_playback_admissions(user_id, expires_at)"
            )

    def _authorize(self, user_id: str, channel_id: str) -> bool:
        try:
            return bool(
                user_id
                and self.authorize_user_channel(user_id, channel_id)
                and self.catchup_enabled(user_id, channel_id)
            )
        except Exception:
            return False

    def stream(
        self,
        token: str | None,
        channel_id: str,
        start_utc: datetime | str | int | float,
        end_utc: datetime | str | int | float,
        *,
        session_id: str,
        method: str = "GET",
        range_header: str | None = None,
        live: bool = False,
        live_follow_seconds: float = 1.0,
    ) -> TSHTTPResponse | StreamingTSHTTPResponse:
        """Token-authenticated standalone entry point."""
        if self.tokens is None or not token:
            return _error(401, "unauthorized")
        try:
            user_id = self.tokens.lookup(token)
        except Exception:
            return _error(401, "unauthorized")
        if user_id is None:
            return _error(401, "unauthorized")
        return self.stream_for_user(
            user_id, channel_id, start_utc, end_utc,
            session_id=session_id,
            device_key=hashlib.sha256(
                f"ts-device\0{user_id}\0{channel_id}\0{session_id}".encode("utf-8")
            ).hexdigest(),
            method=method,
            range_header=range_header,
            live=live,
            live_follow_seconds=live_follow_seconds,
        )

    def stream_for_user(
        self,
        user_id: str | int,
        channel_id: str,
        start_utc: datetime | str | int | float,
        end_utc: datetime | str | int | float,
        *,
        session_id: str,
        device_key: str,
        method: str = "GET",
        range_header: str | None = None,
        live: bool = False,
        live_follow_seconds: float = 1.0,
    ) -> TSHTTPResponse | StreamingTSHTTPResponse:
        """Serve local TS for a user already authenticated by Dispatcharr."""
        method = method.upper()
        if method not in {"GET", "HEAD"}:
            return _error(405, "method not allowed")
        if not isinstance(session_id, str) or not _SESSION_ID_RE.fullmatch(session_id):
            return _error(400, "invalid playback session")
        if not isinstance(device_key, str) or not _SESSION_ID_RE.fullmatch(device_key):
            return _error(400, "invalid playback device")
        if not isinstance(live_follow_seconds, (int, float)) or not 0 <= live_follow_seconds <= 10:
            return _error(400, "invalid live follow timeout")
        user = str(user_id).strip()
        channel = str(channel_id).strip()
        if not self._authorize(user, channel):
            return _error(403, "forbidden")
        try:
            start, end = _epoch(start_utc), _epoch(end_utc)
            if end <= start:
                return _error(400, "invalid playback range")
        except (TypeError, ValueError, OverflowError):
            return _error(400, "invalid playback range")

        request_key = hashlib.sha256(
            f"ts\0{user}\0{channel}\0{session_id}".encode("utf-8")
        ).hexdigest()
        try:
            acquired = self._acquire_lease(
                user, channel, start, end, request_key, device_key,
                response_stream=method == "GET",
            )
            if method == "GET":
                lease_id, response_stream_id = acquired
            else:
                lease_id, response_stream_id = acquired, None
        except _PermissionDenied:
            return _error(403, "stream limit exceeded")
        except _PlaybackWindowTooWide:
            return _error(400, "playback window exceeds configured limit")
        except (OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
            return _error(503, "archive temporarily unavailable")

        def finish_response(*, retain_live_session: bool = False) -> None:
            self._finish_response(
                lease_id, response_stream_id, retain_live_session=retain_live_session
            )

        try:
            segments = self.store.segments(channel, start, end)
        except (TypeError, ValueError):
            finish_response()
            return _error(400, "invalid channel")
        except (OSError, RuntimeError, sqlite3.Error):
            finish_response()
            return _error(503, "archive temporarily unavailable")

        deadline = time.monotonic() + live_follow_seconds if live and live_follow_seconds else None
        while not segments and deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.1, remaining))
            segments = self.store.segments(channel, start, end)
        if not segments:
            finish_response()
            return _error(404, "no archived segments in requested range")

        def files_for(candidate_segments):
            unsafe_boundaries: list[int] = []
            byte_offset = 0
            selected_files: list[tuple[Path, int]] = []
            try:
                root = Path(self.store.root).resolve(strict=True)
                for segment in candidate_segments:
                    if not self._renew_lease(user, channel, lease_id, device_key, segment):
                        return None, None, _error(403, "invalid or expired playback lease")
                    path = Path(segment.path).resolve(strict=True)
                    path.relative_to(root)
                    if not path.is_file():
                        return None, None, _error(404, "segment not found")
                    length = path.stat().st_size
                    selected_files.append((path, length))
            except (OSError, ValueError):
                return None, None, _error(404, "segment not found")
            previous_end = None
            for segment, (_, length) in zip(candidate_segments, selected_files):
                current_start = segment.start_utc.timestamp()
                if previous_end is not None and (
                    segment.discontinuity or current_start > previous_end + 0.25
                ):
                    unsafe_boundaries.append(byte_offset)
                previous_end = max(
                    previous_end or segment.end_utc.timestamp(), segment.end_utc.timestamp()
                )
                byte_offset += length
            return selected_files, unsafe_boundaries, None

        files, unsafe_boundaries, file_error = files_for(segments)
        if file_error is not None:
            finish_response()
            return file_error

        size = sum(length for _, length in files)
        try:
            selected = parse_byte_range(range_header, size)
        except RangeNotSatisfiable:
            selected = None
            if deadline is not None and range_header is not None:
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    time.sleep(min(0.1, remaining))
                    refreshed = self.store.segments(channel, start, end)
                    if len(refreshed) <= len(segments):
                        continue
                    refreshed_files, refreshed_boundaries, file_error = files_for(refreshed)
                    if file_error is not None:
                        finish_response()
                        return file_error
                    refreshed_size = sum(length for _, length in refreshed_files)
                    if refreshed_size <= size:
                        continue
                    segments, files, size = refreshed, refreshed_files, refreshed_size
                    unsafe_boundaries = refreshed_boundaries
                    try:
                        selected = parse_byte_range(range_header, size)
                    except RangeNotSatisfiable:
                        continue
                    break

        if unsafe_boundaries and (
            (selected is None and range_header is None)
            or (
                selected is not None
                and any(selected.start < boundary <= selected.end for boundary in unsafe_boundaries)
            )
        ):
            finish_response()
            return _error(409, "TS playback cannot cross an archive gap or discontinuity")
        if selected is None and range_header is not None:
            finish_response()
            return _error(
                416,
                headers={
                    "Content-Range": f"bytes */{size}",
                    "Content-Length": "0",
                    "Cache-Control": "private, no-store",
                    **({"Retry-After": "1"} if live else {}),
                },
            )

        etag_source = "\0".join(
            f"{segment.id}:{length}" for segment, (_, length) in zip(segments, files)
        )
        etag = hashlib.sha256(etag_source.encode("utf-8")).hexdigest()
        headers = {
            "Accept-Ranges": "bytes",
            "Cache-Control": "private, no-store",
            "Content-Type": "video/mp2t",
            "ETag": f'"{etag}"',
            "X-Content-Type-Options": "nosniff",
        }
        if selected is None:
            status, start_offset, end_offset = 200, 0, size - 1
        else:
            status, start_offset, end_offset = 206, selected.start, selected.end
            headers["Content-Range"] = f"bytes {selected.start}-{selected.end}/{size}"
        length = max(0, end_offset - start_offset + 1)
        headers["Content-Length"] = str(length)
        if method == "HEAD" or length == 0:
            finish_response()
            return TSHTTPResponse(status, headers)

        def body_chunks() -> Iterator[bytes]:
            virtual_offset = 0
            renewal_interval = min(60.0, self.lease_ttl_seconds / 3)
            renew_at = time.monotonic() + renewal_interval
            try:
                for segment, (path, file_size) in zip(segments, files):
                    file_end = virtual_offset + file_size - 1
                    overlap_start = max(start_offset, virtual_offset)
                    overlap_end = min(end_offset, file_end)
                    if overlap_start <= overlap_end:
                        if not self._renew_lease(user, channel, lease_id, device_key, segment):
                            raise OSError("playback lease expired during TS stream")
                        remaining = overlap_end - overlap_start + 1
                        with path.open("rb") as source:
                            source.seek(overlap_start - virtual_offset)
                            while remaining:
                                if time.monotonic() >= renew_at:
                                    if (
                                        not self._renew_lease(user, channel, lease_id, device_key, segment)
                                        or not self._renew_response_stream(response_stream_id, lease_id)
                                    ):
                                        raise OSError("playback lease expired during TS stream")
                                    renew_at = time.monotonic() + renewal_interval
                                chunk = source.read(min(_TS_STREAM_CHUNK_BYTES, remaining))
                                if not chunk:
                                    raise OSError("archive segment changed during TS stream")
                                remaining -= len(chunk)
                                yield chunk
                    virtual_offset += file_size
            finally:
                finish_response(retain_live_session=live)

        body = _CleanupIterator(body_chunks(), lambda: finish_response(retain_live_session=live))
        return StreamingTSHTTPResponse(status, headers, body, lease_id)

    def _acquire_lease(
        self,
        user_id: str,
        channel_id: str,
        start: float,
        end: float,
        request_key: str,
        device_key: str,
        *,
        response_stream: bool,
    ):
        # Match HLS admission's per-user cross-process lock. Keep it through
        # the callback and committed session row so neither protocol can pass
        # a positive stream limit using a stale count from the other.
        with playback_admission_lock(self.store.root, user_id):
            return self._acquire_lease_locked(
                user_id, channel_id, start, end, request_key, device_key,
                response_stream=response_stream,
            )

    def _acquire_lease_locked(
        self,
        user_id: str,
        channel_id: str,
        start: float,
        end: float,
        request_key: str,
        device_key: str,
        *,
        response_stream: bool,
    ):
        wait_deadline = time.monotonic() + 10
        while True:
            expired: list[str] = []
            with closing(self._connect()) as db:
                db.execute("BEGIN IMMEDIATE")
                now = self.clock()
                expired.extend(
                    str(row[0]) for row in db.execute(
                        "SELECT lease_id FROM ts_playback_sessions WHERE expires_at<=?", (now,)
                    )
                )
                db.execute("DELETE FROM ts_playback_streams WHERE expires_at<=?", (now,))
                db.execute("DELETE FROM ts_playback_sessions WHERE expires_at<=?", (now,))
                db.execute("DELETE FROM ts_playback_admissions WHERE expires_at<=?", (now,))
                db.execute(
                    "DELETE FROM ts_playback_streams WHERE NOT EXISTS "
                    "(SELECT 1 FROM ts_playback_sessions s WHERE s.lease_id=ts_playback_streams.lease_id)"
                )
                existing = db.execute(
                    "SELECT lease_id,start_utc,end_utc FROM ts_playback_sessions "
                    "WHERE request_key=? AND active=1 AND expires_at>? ORDER BY expires_at DESC LIMIT 1",
                    (request_key, now),
                ).fetchone()
                if existing is not None:
                    lease_id = str(existing["lease_id"])
                    expanded_start = min(float(existing["start_utc"]), start)
                    expanded_end = max(float(existing["end_utc"]), end)
                    if expanded_end - expanded_start > self.max_lease_span_seconds:
                        db.commit()
                        for expired_id in expired:
                            self.store.end_playback(expired_id)
                        raise _PlaybackWindowTooWide
                    db.commit()
                    for expired_id in expired:
                        self.store.end_playback(expired_id)
                    if not self.store.extend_playback(
                        lease_id,
                        expanded_end,
                        start_utc=expanded_start,
                        ttl_seconds=self.lease_ttl_seconds,
                    ):
                        self._drop_stale_session(lease_id, request_key)
                        continue
                    stream_id = uuid.uuid4().hex if response_stream else None
                    with closing(self._connect()) as session_db:
                        session_db.execute("BEGIN IMMEDIATE")
                        active = session_db.execute(
                            "SELECT 1 FROM ts_playback_sessions WHERE lease_id=? "
                            "AND request_key=? AND active=1 AND expires_at>?",
                            (lease_id, request_key, self.clock()),
                        ).fetchone()
                        if active is None:
                            session_db.commit()
                            continue
                        session_db.execute(
                            "UPDATE ts_playback_sessions SET start_utc=MIN(start_utc,?),"
                            "end_utc=MAX(end_utc,?),expires_at=? WHERE lease_id=?",
                            (expanded_start, expanded_end, self.clock() + self.lease_ttl_seconds, lease_id),
                        )
                        if stream_id is not None:
                            session_db.execute(
                                "INSERT INTO ts_playback_streams(id,lease_id,expires_at) VALUES(?,?,?)",
                                (stream_id, lease_id, self.clock() + self.lease_ttl_seconds),
                            )
                        session_db.commit()
                    return (lease_id, stream_id) if response_stream else lease_id

                pending = db.execute(
                    "SELECT 1 FROM ts_playback_admissions WHERE request_key=?", (request_key,)
                ).fetchone()
                if pending is not None:
                    db.commit()
                    for expired_id in expired:
                        self.store.end_playback(expired_id)
                    if time.monotonic() >= wait_deadline:
                        raise _PermissionDenied
                    time.sleep(0.01)
                    continue

                active_sessions = int(db.execute(
                    "SELECT COUNT(*) FROM ts_playback_sessions WHERE user_id=? AND active=1 "
                    "AND device_key<>? AND expires_at>?",
                    (user_id, device_key, now),
                ).fetchone()[0])
                pending_sessions = int(db.execute(
                    "SELECT COUNT(*) FROM ts_playback_admissions WHERE user_id=? "
                    "AND device_key<>? AND expires_at>?",
                    (user_id, device_key, now),
                ).fetchone()[0])
                db.execute(
                    "INSERT INTO ts_playback_admissions(request_key,user_id,device_key,expires_at) "
                    "VALUES(?,?,?,?)",
                    (request_key, user_id, device_key, now + 60),
                )
                db.commit()
            for expired_id in expired:
                self.store.end_playback(expired_id)

            if self.allow_new_session is not None:
                try:
                    if self._callback_accepts_device(self.allow_new_session):
                        allowed = self.allow_new_session(
                            user_id, channel_id, active_sessions + pending_sessions, device_key
                        )
                    else:
                        allowed = self.allow_new_session(
                            user_id, channel_id, active_sessions + pending_sessions
                        )
                except Exception:
                    allowed = False
                if not allowed:
                    self._delete_admission(request_key)
                    raise _PermissionDenied

            lease = None
            try:
                lease = self.store.begin_playback(
                    channel_id, start, end, ttl_seconds=self.lease_ttl_seconds
                )
                stream_id = uuid.uuid4().hex if response_stream else None
                retired: list[str] = []
                with closing(self._connect()) as session_db:
                    session_db.execute("BEGIN IMMEDIATE")
                    session_db.execute(
                        "DELETE FROM ts_playback_admissions WHERE request_key=?", (request_key,)
                    )
                    old_sessions = session_db.execute(
                        "SELECT lease_id FROM ts_playback_sessions WHERE device_key=? "
                        "AND active=1 AND expires_at>?",
                        (device_key, self.clock()),
                    ).fetchall()
                    for old in old_sessions:
                        old_id = str(old["lease_id"])
                        session_db.execute(
                            "UPDATE ts_playback_sessions SET active=0 WHERE lease_id=?", (old_id,)
                        )
                        stream_count = int(session_db.execute(
                            "SELECT COUNT(*) FROM ts_playback_streams WHERE lease_id=? AND expires_at>?",
                            (old_id, self.clock()),
                        ).fetchone()[0])
                        if stream_count == 0:
                            session_db.execute(
                                "DELETE FROM ts_playback_sessions WHERE lease_id=?", (old_id,)
                            )
                            retired.append(old_id)
                    session_db.execute(
                        "INSERT INTO ts_playback_sessions "
                        "(lease_id,user_id,channel_id,device_key,request_key,start_utc,end_utc,expires_at,active) "
                        "VALUES(?,?,?,?,?,?,?,?,1)",
                        (
                            lease.id, user_id, channel_id, device_key, request_key,
                            start, end, lease.expires_at,
                        ),
                    )
                    if stream_id is not None:
                        session_db.execute(
                            "INSERT INTO ts_playback_streams(id,lease_id,expires_at) VALUES(?,?,?)",
                            (stream_id, lease.id, lease.expires_at),
                        )
                    session_db.commit()
                for old_id in retired:
                    self.store.end_playback(old_id)
            except BaseException:
                if lease is not None:
                    self.store.end_playback(lease.id)
                self._delete_admission(request_key)
                raise
            return (lease.id, stream_id) if response_stream else lease.id

    def _drop_stale_session(self, lease_id: str, request_key: str) -> None:
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "DELETE FROM ts_playback_sessions WHERE lease_id=? AND request_key=?",
                (lease_id, request_key),
            )
            db.execute("DELETE FROM ts_playback_streams WHERE lease_id=?", (lease_id,))
            db.commit()
        self.store.end_playback(lease_id)

    def _delete_admission(self, request_key: str) -> None:
        with closing(self._connect()) as db:
            db.execute("DELETE FROM ts_playback_admissions WHERE request_key=?", (request_key,))

    def _renew_lease(
        self, user_id: str, channel_id: str, lease_id: str, device_key: str, segment
    ) -> bool:
        now = self.clock()
        with closing(self._connect()) as db:
            row = db.execute(
                "SELECT start_utc,end_utc FROM ts_playback_sessions WHERE lease_id=? "
                "AND user_id=? AND channel_id=? AND device_key=? AND expires_at>?",
                (lease_id, user_id, channel_id, device_key, now),
            ).fetchone()
        if row is None:
            return False
        if (
            segment.end_utc.timestamp() <= float(row["start_utc"])
            or segment.start_utc.timestamp() >= float(row["end_utc"])
        ):
            return False
        if not self.store.renew_playback(lease_id, ttl_seconds=self.lease_ttl_seconds):
            return False
        with closing(self._connect()) as db:
            db.execute(
                "UPDATE ts_playback_sessions SET expires_at=? WHERE lease_id=?",
                (now + self.lease_ttl_seconds, lease_id),
            )
        return True

    def _renew_response_stream(self, stream_id: str | None, lease_id: str) -> bool:
        if not stream_id:
            return False
        now = self.clock()
        expires_at = now + self.lease_ttl_seconds
        with closing(self._connect()) as db:
            active = db.execute(
                "SELECT 1 FROM ts_playback_sessions WHERE lease_id=? AND expires_at>?",
                (lease_id, now),
            ).fetchone()
            if active is None:
                return False
            cursor = db.execute(
                "UPDATE ts_playback_streams SET expires_at=? WHERE id=? AND lease_id=?",
                (expires_at, stream_id, lease_id),
            )
            return cursor.rowcount == 1

    def _finish_response(
        self,
        lease_id: str,
        stream_id: str | None,
        *,
        retain_live_session: bool,
    ) -> None:
        retire = False
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            if stream_id is not None:
                db.execute(
                    "DELETE FROM ts_playback_streams WHERE id=? AND lease_id=?",
                    (stream_id, lease_id),
                )
            if not retain_live_session:
                db.execute(
                    "UPDATE ts_playback_sessions SET active=0 WHERE lease_id=?", (lease_id,)
                )
            active_streams = int(db.execute(
                "SELECT COUNT(*) FROM ts_playback_streams WHERE lease_id=? AND expires_at>?",
                (lease_id, self.clock()),
            ).fetchone()[0])
            active_session = db.execute(
                "SELECT active FROM ts_playback_sessions WHERE lease_id=?", (lease_id,)
            ).fetchone()
            if active_session is not None and not int(active_session["active"]) and active_streams == 0:
                db.execute("DELETE FROM ts_playback_sessions WHERE lease_id=?", (lease_id,))
                retire = True
            db.commit()
        if retire:
            self.store.end_playback(lease_id)

    def end_session(self, token: str | None, channel_id: str, lease_id: str) -> bool:
        """End a token-owned session early."""
        if self.tokens is None or not token:
            return False
        user_id = self.tokens.lookup(token)
        if user_id is None:
            return False
        return self.end_user_session(user_id, channel_id, lease_id)

    def end_user_session(self, user_id: str | int, channel_id: str, lease_id: str) -> bool:
        user, channel = str(user_id), str(channel_id)
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            deleted = db.execute(
                "DELETE FROM ts_playback_sessions WHERE lease_id=? AND user_id=? AND channel_id=?",
                (lease_id, user, channel),
            ).rowcount
            if deleted:
                db.execute("DELETE FROM ts_playback_streams WHERE lease_id=?", (lease_id,))
            db.commit()
        if deleted:
            self.store.end_playback(lease_id)
            return True
        return False


def response_has_streaming_body(value: Any) -> bool:
    """Return whether a service response owns a streaming body iterator."""
    return isinstance(value, StreamingTSHTTPResponse)


__all__ = [
    "ArchiveTSPlaybackService",
    "StreamingTSHTTPResponse",
    "TSHTTPResponse",
    "response_has_streaming_body",
]
