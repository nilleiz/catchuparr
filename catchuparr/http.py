"""Small HTTP-independent handlers for authenticated archive playback.

The Dispatcharr integration can translate :class:`HTTPResponse` instances to
framework responses. Tokens are bearer credentials and therefore appear in
playlist segment URLs; the integration must redact the ``token`` query
parameter from access logs.
"""

from __future__ import annotations

import fcntl
import hashlib
import mimetypes
import re
import sqlite3
import time
from contextlib import closing, contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import quote

from .engine.store import TIMELINE_GAP_TOLERANCE_SECONDS


@dataclass(frozen=True)
class HTTPResponse:
    status: int
    headers: dict[str, str]
    body: bytes = b""


@dataclass(frozen=True)
class ByteRange:
    start: int
    end: int  # inclusive

    @property
    def length(self) -> int:
        return self.end - self.start + 1


class RangeNotSatisfiable(ValueError):
    pass


class _PermissionDenied(Exception):
    pass


@dataclass(frozen=True)
class _LeaseAcquisition:
    lease_id: str
    state: str
    previous_lease_id: str | None = None
    previous_expires_at: float | None = None
    provisional: bool = False


SESSION_REPLACEMENT_GRACE_SECONDS = 30.0


def parse_byte_range(value: str | None, size: int) -> ByteRange | None:
    """Parse one RFC 9110 byte range; unsupported multi-ranges return 416."""
    if size < 0:
        raise ValueError("size cannot be negative")
    if value is None:
        return None
    match = re.fullmatch(r"\s*bytes=(\d*)-(\d*)\s*", value, flags=re.IGNORECASE)
    if not match or (not match.group(1) and not match.group(2)):
        raise RangeNotSatisfiable("invalid or unsupported byte range")
    if size == 0:
        raise RangeNotSatisfiable("empty resource has no byte ranges")
    left, right = match.groups()
    if not left:
        suffix = int(right)
        if suffix <= 0:
            raise RangeNotSatisfiable("suffix range must be positive")
        return ByteRange(max(0, size - suffix), size - 1)
    start = int(left)
    if start >= size:
        raise RangeNotSatisfiable("range starts beyond the resource")
    end = min(int(right), size - 1) if right else size - 1
    if end < start:
        raise RangeNotSatisfiable("range end precedes its start")
    return ByteRange(start, end)


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


def _error(status: int, message: str = "") -> HTTPResponse:
    body = message.encode("utf-8") if message else b""
    headers = {"Content-Length": str(len(body)), "Cache-Control": "no-store"}
    if body:
        headers["Content-Type"] = "text/plain; charset=utf-8"
    return HTTPResponse(status, headers, body)


class ArchiveHTTPService:
    """Serve HLS playlists and immutable archive segments.

    ``authorize_user_channel(user_id, channel_id)`` and
    ``catchup_enabled(user_id, channel_id)`` must enforce Dispatcharr's user
    permissions and the channel's local-catch-up switch respectively. The
    service never relies on channel identifiers supplied by a token alone.
    """

    def __init__(
        self,
        store,
        token_store,
        *,
        authorize_user_channel: Callable[[str, str], bool],
        catchup_enabled: Callable[[str, str], bool],
        allow_new_session: Callable[[str, str, int], bool] | None = None,
        base_path: str = "/catchuparr",
        lease_ttl_seconds: float = 4 * 60 * 60,
        replacement_grace_seconds: float = SESSION_REPLACEMENT_GRACE_SECONDS,
        clock: Callable[[], float] = time.time,
        playlist_builder: Callable[..., str] | None = None,
    ):
        if not base_path.startswith("/") or "\r" in base_path or "\n" in base_path:
            raise ValueError("base_path must be an absolute URL path")
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be positive")
        if replacement_grace_seconds <= 0:
            raise ValueError("replacement_grace_seconds must be positive")
        self.store = store
        self.tokens = token_store
        self.authorize_user_channel = authorize_user_channel
        self.catchup_enabled = catchup_enabled
        self.allow_new_session = allow_new_session
        self.base_path = base_path.rstrip("/")
        self.lease_ttl_seconds = lease_ttl_seconds
        self.replacement_grace_seconds = replacement_grace_seconds
        self.clock = clock
        self.playlist_builder = playlist_builder
        self._init_sessions()

    def _connect(self) -> sqlite3.Connection:
        db_path = Path(self.store.root) / "archive.sqlite3"
        db = sqlite3.connect(db_path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")
        return db

    def _init_sessions(self) -> None:
        with self._admission_lock():
            now = self.clock()
            with closing(self._connect()) as db:
                db.execute(
                    """CREATE TABLE IF NOT EXISTS http_playback_sessions (
                        lease_id TEXT PRIMARY KEY,
                        user_id TEXT NOT NULL,
                        channel_id TEXT NOT NULL,
                        request_key TEXT,
                        device_key TEXT,
                        start_utc REAL NOT NULL,
                        end_utc REAL NOT NULL,
                        expires_at REAL NOT NULL,
                        grace_until REAL
                    )"""
                )
                columns = {row[1] for row in db.execute("PRAGMA table_info(http_playback_sessions)")}
                for name, definition in (
                    ("request_key", "TEXT"),
                    ("device_key", "TEXT"),
                    ("grace_until", "REAL"),
                    ("committed", "INTEGER NOT NULL DEFAULT 1"),
                    ("pending_admissions", "INTEGER NOT NULL DEFAULT 0"),
                    ("replacement_pending", "INTEGER NOT NULL DEFAULT 0"),
                ):
                    if name not in columns:
                        db.execute(f"ALTER TABLE http_playback_sessions ADD COLUMN {name} {definition}")
                db.execute("CREATE INDEX IF NOT EXISTS http_sessions_user ON http_playback_sessions(user_id, channel_id)")
                db.execute("CREATE INDEX IF NOT EXISTS http_sessions_request ON http_playback_sessions(request_key, expires_at)")
                db.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS http_sessions_device_active "
                    "ON http_playback_sessions(device_key) "
                    "WHERE device_key IS NOT NULL AND grace_until IS NULL"
                )
                legacy = [
                    (str(row["lease_id"]), min(float(row["expires_at"]), now + self.replacement_grace_seconds))
                    for row in db.execute(
                        "SELECT lease_id,expires_at FROM http_playback_sessions "
                        "WHERE device_key IS NULL AND grace_until IS NULL AND expires_at>?",
                        (now,),
                    )
                ]
                db.executemany(
                    "UPDATE http_playback_sessions SET grace_until=?,expires_at=? WHERE lease_id=?",
                    [(expiry, expiry, lease_id) for lease_id, expiry in legacy],
                )
            for lease_id, expiry in legacy:
                ttl = max(0.001, expiry - now)
                if not self.store.renew_playback(lease_id, ttl_seconds=ttl):
                    self.store.end_playback(lease_id)

    @contextmanager
    def _admission_lock(self):
        """Serialize playback admission across web workers and processes."""
        lock_path = Path(self.store.root) / ".http-playback-admission.lock"
        with lock_path.open("a") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _authorize(self, token: str | None, channel_id: str) -> str | None:
        if not token:
            return None
        user_id = self.tokens.lookup(token)
        if user_id is None:
            return None
        if not self.authorize_user_channel(user_id, channel_id):
            raise _PermissionDenied
        if not self.catchup_enabled(user_id, channel_id):
            raise _PermissionDenied
        return user_id

    def _make_playlist(self, segments: Iterable, *, live: bool, uri_for: Callable) -> str:
        builder = self.playlist_builder
        if builder is None:
            # Lazy import keeps the HTTP/service module usable in lightweight
            # tools and during plugin discovery without importing engine code.
            from .engine.playlist import build_hls_playlist

            builder = build_hls_playlist
        return builder(segments, live=live, uri_for=uri_for)

    def _new_lease(
        self,
        user_id: str,
        channel_id: str,
        start: float,
        end: float,
        request_key: str,
        device_key: str,
    ) -> _LeaseAcquisition:
        with self._admission_lock():
            now = self.clock()
            with closing(self._connect()) as db:
                expired = [
                    str(row["lease_id"])
                    for row in db.execute(
                        "SELECT lease_id FROM http_playback_sessions WHERE expires_at<=?", (now,)
                    )
                ]
                db.execute("DELETE FROM http_playback_sessions WHERE expires_at<=?", (now,))
                existing = db.execute(
                    "SELECT lease_id,request_key,expires_at,committed FROM http_playback_sessions "
                    "WHERE device_key=? AND grace_until IS NULL AND expires_at>?",
                    (device_key, now),
                ).fetchone()
            for old_id in expired:
                self.store.end_playback(old_id)

            old_id = str(existing["lease_id"]) if existing is not None else None
            if existing is not None and existing["request_key"] == request_key:
                if self.store.extend_playback(
                    old_id, end, ttl_seconds=self.lease_ttl_seconds
                ):
                    with closing(self._connect()) as db:
                        db.execute(
                            "UPDATE http_playback_sessions "
                            "SET end_utc=MAX(end_utc,?),expires_at=? WHERE lease_id=?",
                            (end, now + self.lease_ttl_seconds, old_id),
                        )
                        provisional = not bool(existing["committed"])
                        if provisional:
                            db.execute(
                                "UPDATE http_playback_sessions "
                                "SET pending_admissions=pending_admissions+1 WHERE lease_id=?",
                                (old_id,),
                            )
                    return _LeaseAcquisition(old_id, "reused", provisional=provisional)
                with closing(self._connect()) as db:
                    db.execute("DELETE FROM http_playback_sessions WHERE lease_id=?", (old_id,))
                self.store.end_playback(old_id)
                old_id = None

            if self.allow_new_session is not None:
                with closing(self._connect()) as db:
                    active_count = int(db.execute(
                        "SELECT COUNT(*) FROM http_playback_sessions "
                        "WHERE user_id=? AND expires_at>? AND grace_until IS NULL "
                        "AND (device_key IS NULL OR device_key<>?)",
                        (user_id, now, device_key),
                    ).fetchone()[0])
                if not self.allow_new_session(user_id, channel_id, active_count):
                    raise _PermissionDenied

            lease = self.store.begin_playback(
                channel_id, start, end, ttl_seconds=self.lease_ttl_seconds
            )
            grace_until = now + self.replacement_grace_seconds
            try:
                with closing(self._connect()) as db:
                    db.execute("BEGIN IMMEDIATE")
                    if old_id is not None:
                        db.execute(
                            "UPDATE http_playback_sessions "
                            "SET grace_until=?,expires_at=?,replacement_pending=1 "
                            "WHERE lease_id=? AND device_key=? AND grace_until IS NULL",
                            (grace_until, grace_until, old_id, device_key),
                        )
                    db.execute(
                        "INSERT INTO http_playback_sessions "
                        "(lease_id,user_id,channel_id,request_key,device_key,start_utc,end_utc,expires_at,grace_until,committed,pending_admissions) "
                        "VALUES(?,?,?,?,?,?,?,?,NULL,0,1)",
                        (
                            lease.id, user_id, channel_id, request_key, device_key,
                            start, end, lease.expires_at,
                        ),
                    )
                    db.execute("COMMIT")
            except BaseException:
                self.store.end_playback(lease.id)
                raise
            return _LeaseAcquisition(
                lease.id,
                "replaced" if old_id is not None else "created",
                previous_lease_id=old_id,
                previous_expires_at=float(existing["expires_at"]) if old_id is not None else None,
                provisional=True,
            )

    def _commit_acquisition(self, acquisition: _LeaseAcquisition) -> None:
        """Make a rendered playlist's lease durable against other failed renders."""
        if not acquisition.provisional:
            return
        previous_grace_until = None
        previous_missing = False
        with self._admission_lock():
            with closing(self._connect()) as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    "UPDATE http_playback_sessions SET committed=1,pending_admissions=0 "
                    "WHERE lease_id=?",
                    (acquisition.lease_id,),
                )
                if acquisition.previous_lease_id is not None:
                    previous = db.execute(
                        "SELECT grace_until FROM http_playback_sessions WHERE lease_id=?",
                        (acquisition.previous_lease_id,),
                    ).fetchone()
                    if previous is None:
                        previous_missing = True
                    elif previous["grace_until"] is not None:
                        previous_grace_until = previous["grace_until"]
                        db.execute(
                            "UPDATE http_playback_sessions SET replacement_pending=0 "
                            "WHERE lease_id=?",
                            (acquisition.previous_lease_id,),
                        )
                db.execute("COMMIT")
            if (
                acquisition.previous_lease_id is not None
                and not previous_missing
                and previous_grace_until is not None
            ):
                remaining = (previous_grace_until or self.clock()) - self.clock()
                if remaining <= 0 or not self.store.renew_playback(
                    acquisition.previous_lease_id, ttl_seconds=remaining
                ):
                    with closing(self._connect()) as db:
                        db.execute(
                            "DELETE FROM http_playback_sessions WHERE lease_id=? "
                            "AND grace_until IS NOT NULL",
                            (acquisition.previous_lease_id,),
                        )
                    self.store.end_playback(acquisition.previous_lease_id)

    def _rollback_acquisition(
        self, acquisition: _LeaseAcquisition, user_id: str, device_key: str
    ) -> None:
        """Release a failed provisional lease without destroying a reused session."""
        if not acquisition.provisional:
            return
        now = self.clock()
        restore_previous = False
        with self._admission_lock():
            with closing(self._connect()) as db:
                db.execute("BEGIN IMMEDIATE")
                created = db.execute(
                    "SELECT grace_until,committed,pending_admissions FROM http_playback_sessions "
                    "WHERE lease_id=? AND user_id=? AND device_key=?",
                    (acquisition.lease_id, user_id, device_key),
                ).fetchone()
                if created is None:
                    db.execute("COMMIT")
                    return
                if created["committed"]:
                    db.execute("COMMIT")
                    return
                if created["pending_admissions"] > 1:
                    db.execute(
                        "UPDATE http_playback_sessions SET pending_admissions=pending_admissions-1 "
                        "WHERE lease_id=?",
                        (acquisition.lease_id,),
                    )
                    db.execute("COMMIT")
                    return
                active = db.execute(
                    "SELECT lease_id FROM http_playback_sessions "
                    "WHERE device_key=? AND grace_until IS NULL AND expires_at>?",
                    (device_key, now),
                ).fetchone()
                can_restore = (
                    acquisition.state == "replaced"
                    and acquisition.previous_lease_id is not None
                    and acquisition.previous_expires_at is not None
                    and acquisition.previous_expires_at > now
                    and created["grace_until"] is None
                    and (active is None or active["lease_id"] == acquisition.lease_id)
                )
                db.execute(
                    "DELETE FROM http_playback_sessions WHERE lease_id=? AND user_id=? AND device_key=?",
                    (acquisition.lease_id, user_id, device_key),
                )
                if can_restore:
                    cursor = db.execute(
                        "UPDATE http_playback_sessions "
                        "SET grace_until=NULL,expires_at=?,replacement_pending=0 "
                        "WHERE lease_id=? AND user_id=? AND device_key=? AND grace_until IS NOT NULL",
                        (
                            acquisition.previous_expires_at,
                            acquisition.previous_lease_id,
                            user_id,
                            device_key,
                        ),
                    )
                    restore_previous = cursor.rowcount == 1
                db.execute("COMMIT")
            self.store.end_playback(acquisition.lease_id)
            if acquisition.previous_lease_id is not None and restore_previous:
                ttl = max(0.001, acquisition.previous_expires_at - now)
                if not self.store.renew_playback(acquisition.previous_lease_id, ttl_seconds=ttl):
                    self.store.end_playback(acquisition.previous_lease_id)
            elif acquisition.previous_lease_id is not None and not can_restore:
                # The prior session has either expired or a newer programme is
                # already active. Leave newer playback intact and let the old
                # grace lease expire on its own deadline.
                pass

    def _renew_lease(
        self, user_id: str, channel_id: str, lease_id: str, device_key: str, segment
    ) -> bool:
        now = self.clock()
        with self._admission_lock():
            with closing(self._connect()) as db:
                row = db.execute(
                    "SELECT start_utc,end_utc,grace_until,replacement_pending "
                    "FROM http_playback_sessions "
                    "WHERE lease_id=? AND user_id=? AND channel_id=? "
                    "AND (device_key=? OR (device_key IS NULL AND grace_until IS NOT NULL)) "
                    "AND expires_at>?",
                    (lease_id, user_id, channel_id, device_key, now),
                ).fetchone()
            if row is None:
                return False
            if (
                segment.end_utc.timestamp() <= float(row["start_utc"])
                or segment.start_utc.timestamp() >= float(row["end_utc"])
            ):
                return False
            grace_until = row["grace_until"]
            ttl = self.lease_ttl_seconds
            if grace_until is not None:
                ttl = min(ttl, float(grace_until) - now)
                if ttl <= 0:
                    return False
            if not row["replacement_pending"] and not self.store.renew_playback(
                lease_id, ttl_seconds=ttl
            ):
                return False
            expires_at = now + ttl
            with closing(self._connect()) as db:
                db.execute(
                    "UPDATE http_playback_sessions SET expires_at=? "
                    "WHERE lease_id=? AND user_id=? AND device_key=?",
                    (expires_at, lease_id, user_id, device_key),
                )
            return True

    def playlist(
        self,
        token: str | None,
        channel_id: str,
        start_utc: datetime | str | int | float,
        end_utc: datetime | str | int | float,
        *,
        live: bool = False,
    ) -> HTTPResponse:
        """Render an authenticated playlist for indexed segments in a range.

        For a growing recording, call this route again periodically. Reloads
        reuse a stable cleanup lease and segment URLs; only committed segments
        are included. Partial coverage is reflected by omitted segments while
        HLS program date-time tags retain their absolute times.
        """
        try:
            start, end = _epoch(start_utc), _epoch(end_utc)
            if end <= start:
                return _error(400, "invalid playback range")
        except (TypeError, ValueError, OverflowError):
            return _error(400, "invalid playback range")
        try:
            user_id = self._authorize(token, channel_id)
        except _PermissionDenied:
            return _error(403, "forbidden")
        if user_id is None:
            return _error(401, "unauthorized")
        device_key = hashlib.sha256(token.encode("utf-8")).hexdigest()
        request_key = hashlib.sha256(
            f"{device_key}\0{channel_id}\0{start:.6f}\0{end:.6f}".encode()
        ).hexdigest()
        try:
            # Protect the entire requested window before reading the index. A
            # concurrent cleanup cannot remove segments selected below.
            acquisition = self._new_lease(
                user_id, channel_id, start, end, request_key, device_key
            )
            lease_id = acquisition.lease_id
            segments = self.store.segments(channel_id, start, end)
        except _PermissionDenied:
            return _error(403, "stream limit exceeded")
        except (TypeError, ValueError):
            return _error(400, "invalid channel")
        if not segments:
            self._rollback_acquisition(acquisition, user_id, device_key)
            return _error(404, "no archived segments in requested range")

        # Expose discontinuities for both explicit transport discontinuities
        # and holes between indexed segments. PDT tags preserve wall-clock time.
        ordered = sorted(segments, key=lambda item: (item.start_utc, item.id))
        marked = []
        previous_end = None
        for segment in ordered:
            has_gap = (
                previous_end is not None
                and segment.start_utc.timestamp() > previous_end + TIMELINE_GAP_TOLERANCE_SECONDS
            )
            marked.append(replace(segment, discontinuity=segment.discontinuity or has_gap))
            previous_end = max(previous_end or segment.end_utc.timestamp(), segment.end_utc.timestamp())
        try:
            root = self.base_path
            channel_component = quote(str(channel_id), safe="")

            def uri_for(segment) -> str:
                segment_component = quote(str(segment.id), safe="")
                return (
                    f"{root}/segment/{channel_component}/{segment_component}"
                    f"?token={quote(token, safe='')}&lease={quote(lease_id, safe='')}"
                )

            body = self._make_playlist(marked, live=bool(live), uri_for=uri_for).encode("utf-8")
        except (OSError, RuntimeError, ValueError):
            self._rollback_acquisition(acquisition, user_id, device_key)
            return _error(503, "archive temporarily unavailable")
        self._commit_acquisition(acquisition)
        return HTTPResponse(
            200,
            {
                "Content-Type": "application/vnd.apple.mpegurl; charset=utf-8",
                "Content-Length": str(len(body)),
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
            },
            body,
        )

    def segment(
        self,
        token: str | None,
        channel_id: str,
        segment_id: str,
        lease_id: str | None,
        *,
        method: str = "GET",
        range_header: str | None = None,
    ) -> HTTPResponse:
        """Serve one immutable segment with optional single byte range."""
        method = method.upper()
        if method not in {"GET", "HEAD"}:
            return _error(405, "method not allowed")
        try:
            user_id = self._authorize(token, channel_id)
        except _PermissionDenied:
            return _error(403, "forbidden")
        if user_id is None:
            return _error(401, "unauthorized")
        if not lease_id:
            return _error(403, "playback lease required")
        try:
            segment = self.store.segment(channel_id, segment_id)
        except (TypeError, ValueError):
            return _error(400, "invalid channel")
        if segment is None:
            return _error(404, "segment not found")
        device_key = hashlib.sha256(token.encode("utf-8")).hexdigest()
        if not self._renew_lease(user_id, channel_id, lease_id, device_key, segment):
            return _error(403, "invalid or expired playback lease")
        try:
            root = Path(self.store.root).resolve(strict=True)
            path = Path(segment.path).resolve(strict=True)
            path.relative_to(root)
            if not path.is_file():
                return _error(404, "segment not found")
            size = path.stat().st_size
            selected = parse_byte_range(range_header, size)
        except RangeNotSatisfiable:
            return HTTPResponse(
                416,
                {"Content-Range": f"bytes */{size}", "Content-Length": "0", "Cache-Control": "private, no-store"},
            )
        except (OSError, ValueError):
            return _error(404, "segment not found")

        content_type = (
            "video/mp2t" if path.suffix.lower() == ".ts"
            else mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        )
        headers = {
            "Accept-Ranges": "bytes",
            "Cache-Control": "private, max-age=31536000, immutable",
            "Content-Type": content_type,
            "ETag": f'"{segment.id}"',
            "X-Content-Type-Options": "nosniff",
        }
        if selected is None:
            status, offset, length = 200, 0, size
        else:
            status, offset, length = 206, selected.start, selected.length
            headers["Content-Range"] = f"bytes {selected.start}-{selected.end}/{size}"
        headers["Content-Length"] = str(length)
        if method == "HEAD":
            return HTTPResponse(status, headers)
        try:
            with path.open("rb") as source:
                source.seek(offset)
                body = source.read(length)
        except OSError:
            return _error(404, "segment not found")
        if len(body) != length:
            # Files are immutable after publication; a short read indicates a
            # concurrent storage fault and should not be presented as complete.
            return _error(503, "segment changed during read")
        return HTTPResponse(status, headers, body)

    def end_session(self, token: str | None, channel_id: str, lease_id: str) -> bool:
        """End a caller-owned playback session early; expired leases self-clean."""
        try:
            user_id = self._authorize(token, channel_id)
        except _PermissionDenied:
            return False
        if user_id is None:
            return False
        device_key = hashlib.sha256(token.encode("utf-8")).hexdigest()
        return self._delete_session(lease_id, user_id, device_key)

    def _delete_session(self, lease_id: str, user_id: str, device_key: str) -> bool:
        with self._admission_lock():
            with closing(self._connect()) as db:
                cursor = db.execute(
                    "DELETE FROM http_playback_sessions "
                    "WHERE lease_id=? AND user_id=? AND device_key=?",
                    (lease_id, user_id, device_key),
                )
            if cursor.rowcount:
                self.store.end_playback(lease_id)
                return True
            return False
