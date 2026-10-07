"""Small HTTP-independent handlers for authenticated archive playback.

The Dispatcharr integration can translate :class:`HTTPResponse` instances to
framework responses. Tokens are bearer credentials and therefore appear in
playlist segment URLs; the integration must redact the ``token`` query
parameter from access logs.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import mimetypes
import re
import sqlite3
import time
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import quote

from .engine.admission import playback_admission_lock
from .engine.store import TIMELINE_GAP_TOLERANCE_SECONDS

logger = logging.getLogger(__name__)


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
    reused: bool
    previous_lease_id: str | None = None


@dataclass(frozen=True)
class _PlaylistSegment:
    id: str
    channel_id: str
    path: Path
    start_utc: datetime
    end_utc: datetime
    discontinuity: bool

    @property
    def duration(self) -> float:
        return (self.end_utc - self.start_utc).total_seconds()


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


@contextmanager
def _archive_file_lock(root: Path | str, name: str):
    lock_path = Path(root) / f".http-playback-{name}.lock"
    with lock_path.open("a") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def ensure_http_playback_sessions(
    store,
    *,
    replacement_grace_seconds: float = SESSION_REPLACEMENT_GRACE_SECONDS,
    clock: Callable[[], float] = time.time,
) -> None:
    """Create or upgrade the HLS lease table and preserve legacy lease grace."""
    if replacement_grace_seconds <= 0:
        raise ValueError("replacement_grace_seconds must be positive")
    legacy = []
    with _archive_file_lock(store.root, "schema"):
        now = clock()
        db_path = Path(store.root) / "archive.sqlite3"
        with closing(sqlite3.connect(db_path, timeout=30, isolation_level=None)) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA busy_timeout=30000")
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
                    grace_until REAL,
                    programme_end_utc REAL,
                    continuation_end_utc REAL,
                    continuation_reached INTEGER NOT NULL DEFAULT 0,
                    boundary_reached INTEGER NOT NULL DEFAULT 0,
                    terminal INTEGER NOT NULL DEFAULT 0,
                    manifest_json TEXT NOT NULL DEFAULT '[]'
                )"""
            )
            columns = {
                row[1] for row in db.execute(
                    "PRAGMA table_info(http_playback_sessions)"
                )
            }
            for name, definition in (
                ("request_key", "TEXT"),
                ("device_key", "TEXT"),
                ("grace_until", "REAL"),
                ("programme_end_utc", "REAL"),
                ("continuation_end_utc", "REAL"),
                ("continuation_reached", "INTEGER NOT NULL DEFAULT 0"),
                ("boundary_reached", "INTEGER NOT NULL DEFAULT 0"),
                ("terminal", "INTEGER NOT NULL DEFAULT 0"),
                ("manifest_json", "TEXT NOT NULL DEFAULT '[]'"),
            ):
                if name not in columns:
                    db.execute(
                        f"ALTER TABLE http_playback_sessions ADD COLUMN {name} {definition}"
                    )
            db.execute(
                "CREATE INDEX IF NOT EXISTS http_sessions_user "
                "ON http_playback_sessions(user_id, channel_id)"
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS http_sessions_request "
                "ON http_playback_sessions(request_key, expires_at)"
            )
            db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS http_sessions_device_active "
                "ON http_playback_sessions(device_key) "
                "WHERE device_key IS NOT NULL AND grace_until IS NULL"
            )
            legacy = [
                (
                    str(row["lease_id"]),
                    min(float(row["expires_at"]), now + replacement_grace_seconds),
                )
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
        if not store.renew_playback(lease_id, ttl_seconds=ttl):
            store.end_playback(lease_id)


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
        ensure_http_playback_sessions(
            self.store,
            replacement_grace_seconds=self.replacement_grace_seconds,
            clock=self.clock,
        )

    @contextmanager
    def _file_lock(self, name: str):
        """Serialize a short named operation across web workers and processes."""
        with _archive_file_lock(self.store.root, name):
            yield

    def _admission_lock(self, user_id: str):
        # Admission limits are per user, so independent accounts can render in
        # parallel. Segment renewal uses a separate, short-lived lock.
        return playback_admission_lock(self.store.root, user_id)

    def _session_lock(self, user_id: str):
        key = hashlib.sha256(user_id.encode("utf-8")).hexdigest()[:24]
        return self._file_lock(f"session-{key}")

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
        now = self.clock()
        with self._session_lock(user_id):
            with closing(self._connect()) as db:
                expired = [
                    str(row["lease_id"])
                    for row in db.execute(
                        "SELECT lease_id FROM http_playback_sessions "
                        "WHERE user_id=? AND expires_at<=?", (user_id, now)
                    )
                ]
                db.execute(
                    "DELETE FROM http_playback_sessions WHERE user_id=? AND expires_at<=?",
                    (user_id, now),
                )
                existing = db.execute(
                    "SELECT lease_id,request_key FROM http_playback_sessions "
                    "WHERE device_key=? AND grace_until IS NULL AND expires_at>?",
                    (device_key, now),
                ).fetchone()
            for expired_id in expired:
                self.store.end_playback(expired_id)

            old_id = str(existing["lease_id"]) if existing is not None else None
            if old_id is not None and not self.store.renew_playback(
                old_id, ttl_seconds=self.lease_ttl_seconds
            ):
                with closing(self._connect()) as db:
                    db.execute(
                        "DELETE FROM http_playback_sessions WHERE lease_id=? AND user_id=?",
                        (old_id, user_id),
                    )
                old_id = None
            if old_id is not None and existing["request_key"] == request_key:
                return _LeaseAcquisition(old_id, reused=True)

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
        return _LeaseAcquisition(
            lease.id,
            reused=False,
            previous_lease_id=old_id,
        )

    def _commit_acquisition(
        self,
        acquisition: _LeaseAcquisition,
        user_id: str,
        channel_id: str,
        request_key: str,
        device_key: str,
        start: float,
        end: float,
        manifest_json: str,
        programme_end: float | None,
        continuation_end: float | None,
        terminal: bool,
    ) -> bool:
        """Commit a rendered playlist without blocking segment reads during render."""
        with self._session_lock(user_id):
            return self._commit_acquisition_locked(
                acquisition, user_id, channel_id, request_key, device_key, start, end,
                manifest_json, programme_end, continuation_end, terminal,
            )

    def _commit_acquisition_locked(
        self,
        acquisition: _LeaseAcquisition,
        user_id: str,
        channel_id: str,
        request_key: str,
        device_key: str,
        start: float,
        end: float,
        manifest_json: str,
        programme_end: float | None,
        continuation_end: float | None,
        terminal: bool,
    ) -> bool:
        now = self.clock()
        if acquisition.reused:
            if not self.store.extend_playback(
                acquisition.lease_id,
                end,
                ttl_seconds=self.lease_ttl_seconds,
            ):
                return False
            with closing(self._connect()) as db:
                db.execute(
                    "UPDATE http_playback_sessions SET end_utc=MAX(end_utc,?),expires_at=?,"
                    "manifest_json=CASE WHEN terminal=1 THEN manifest_json ELSE ? END,"
                    "terminal=MAX(terminal,?),"
                    "programme_end_utc=COALESCE(programme_end_utc,?),"
                    "continuation_end_utc=COALESCE(continuation_end_utc,?) "
                    "WHERE lease_id=?",
                    (
                        end, now + self.lease_ttl_seconds, manifest_json, int(terminal),
                        programme_end, continuation_end, acquisition.lease_id,
                    ),
                )
            return True

        if not self.store.renew_playback(
            acquisition.lease_id, ttl_seconds=self.lease_ttl_seconds
        ):
            return False
        grace_until = now + self.replacement_grace_seconds
        try:
            with closing(self._connect()) as db:
                db.execute("BEGIN IMMEDIATE")
                if acquisition.previous_lease_id is not None:
                    db.execute(
                        "UPDATE http_playback_sessions SET grace_until=?,expires_at=? "
                        "WHERE lease_id=? AND grace_until IS NULL",
                        (grace_until, grace_until, acquisition.previous_lease_id),
                    )
                db.execute(
                    "INSERT INTO http_playback_sessions "
                    "(lease_id,user_id,channel_id,request_key,device_key,start_utc,end_utc,"
                    "expires_at,grace_until,programme_end_utc,continuation_end_utc,"
                    "continuation_reached,manifest_json,terminal) "
                    "VALUES(?,?,?,?,?,?,?,?,NULL,?,?,0,?,?)",
                    (
                        acquisition.lease_id, user_id, channel_id,
                        request_key, device_key, start, end, now + self.lease_ttl_seconds,
                        programme_end, continuation_end, manifest_json, int(terminal),
                    ),
                )
                db.execute("COMMIT")
        except BaseException:
            self.store.end_playback(acquisition.lease_id)
            raise
        if acquisition.previous_lease_id is not None:
            try:
                previous_renewed = self.store.renew_playback(
                    acquisition.previous_lease_id,
                    ttl_seconds=self.replacement_grace_seconds,
                )
            except Exception:
                logger.exception("Could not shorten previous archive lease after session commit")
                previous_renewed = False
            if not previous_renewed:
                # The new session is already committed. A predecessor cleanup
                # failure must never invalidate its segment URLs.
                with closing(self._connect()) as db:
                    try:
                        db.execute(
                            "DELETE FROM http_playback_sessions "
                            "WHERE lease_id=? AND grace_until IS NOT NULL",
                            (acquisition.previous_lease_id,),
                        )
                    except Exception:
                        logger.exception("Could not remove previous HTTP session")
                try:
                    self.store.end_playback(acquisition.previous_lease_id)
                except Exception:
                    logger.exception("Could not release previous archive lease")
        return True

    def _rollback_acquisition(
        self, acquisition: _LeaseAcquisition
    ) -> None:
        """Discard only the new store lease; the previous session is untouched."""
        if not acquisition.reused:
            self.store.end_playback(acquisition.lease_id)

    def _renew_lease(
        self, user_id: str, channel_id: str, lease_id: str, device_key: str, segment
    ) -> bool:
        now = self.clock()
        with self._session_lock(user_id):
            with closing(self._connect()) as db:
                row = db.execute(
                    "SELECT start_utc,end_utc,grace_until "
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
            if not self.store.renew_playback(lease_id, ttl_seconds=ttl):
                return False
            expires_at = now + ttl
            with closing(self._connect()) as db:
                db.execute(
                    "UPDATE http_playback_sessions SET expires_at=? "
                    "WHERE lease_id=? AND user_id=? AND device_key=?",
                    (expires_at, lease_id, user_id, device_key),
                )
            return True

    def _mark_continuation_boundary(
        self, user_id: str, channel_id: str, lease_id: str, device_key: str, segment
    ) -> bool:
        """Persist playback at the EPG edge, even before the next segment exists."""
        segment_start = segment.start_utc.timestamp()
        segment_end = segment.end_utc.timestamp()
        with self._session_lock(user_id):
            candidate = self._continuation_boundary_row(
                user_id, channel_id, lease_id, device_key
            )
        if not self._is_continuation_boundary_candidate(
            candidate, segment_start, segment_end, segment.id
        ):
            return False
        # Match the same lock order used by playlist reloads.
        with self._admission_lock(user_id):
            with self._session_lock(user_id):
                row = self._continuation_boundary_row(
                    user_id, channel_id, lease_id, device_key
                )
                if not self._is_continuation_boundary_candidate(
                    row, segment_start, segment_end, segment.id
                ):
                    return False
                if not row["boundary_reached"]:
                    with closing(self._connect()) as db:
                        db.execute(
                            "UPDATE http_playback_sessions SET boundary_reached=1 "
                            "WHERE lease_id=? AND user_id=? AND channel_id=? "
                            "AND device_key=? AND expires_at>?",
                            (lease_id, user_id, channel_id, device_key, self.clock()),
                        )
            # If continuation media is already indexed, unlock it now. Otherwise
            # the persisted boundary flag lets a later playlist reload unlock it.
            return self._unlock_continuation_locked(
                user_id, channel_id, lease_id, device_key
            )

    def _continuation_boundary_row(self, user_id, channel_id, lease_id, device_key):
        with closing(self._connect()) as db:
            return db.execute(
                "SELECT programme_end_utc,continuation_end_utc,boundary_reached,"
                "continuation_reached,terminal,manifest_json FROM http_playback_sessions "
                "WHERE lease_id=? AND user_id=? AND channel_id=? "
                "AND device_key=? AND expires_at>?",
                (lease_id, user_id, channel_id, device_key, self.clock()),
            ).fetchone()

    @staticmethod
    def _is_continuation_boundary_candidate(row, segment_start, segment_end, segment_id):
        if row is None or row["terminal"]:
            return False
        boundary = row["programme_end_utc"]
        continuation_end = row["continuation_end_utc"]
        return bool(
            boundary is not None
            and continuation_end is not None
            and float(continuation_end) > float(boundary)
            and segment_start < float(boundary)
            and segment_end >= float(boundary) - TIMELINE_GAP_TOLERANCE_SECONDS
            and ArchiveHTTPService._is_manifest_tail(row["manifest_json"], segment_id)
        )

    def _unlock_continuation_locked(
        self, user_id: str, channel_id: str, lease_id: str, device_key: str
    ) -> bool:
        """Unlock indexed continuation after the caller has observed the boundary.

        The caller holds the per-user admission lock. The boundary flag is
        deliberately persisted separately so a reload can retry after an
        archive segment arrives later.
        """
        with self._session_lock(user_id):
            with closing(self._connect()) as db:
                row = db.execute(
                    "SELECT start_utc,end_utc,programme_end_utc,continuation_end_utc,"
                    "boundary_reached,continuation_reached,terminal,manifest_json "
                    "FROM http_playback_sessions WHERE lease_id=? AND user_id=? "
                    "AND channel_id=? AND device_key=? AND expires_at>?",
                    (lease_id, user_id, channel_id, device_key, self.clock()),
                ).fetchone()
            if (
                row is None
                or row["terminal"]
                or not row["boundary_reached"]
                or row["continuation_reached"]
                or row["programme_end_utc"] is None
                or row["continuation_end_utc"] is None
                or float(row["continuation_end_utc"]) <= float(row["programme_end_utc"])
            ):
                return False
            manifest = self._manifest_segments(channel_id, row["manifest_json"], [])
            if not manifest:
                return False
            tail = manifest[-1]
            boundary = float(row["programme_end_utc"])
            continuation_end = min(
                float(row["continuation_end_utc"]),
                float(row["start_utc"]) + 24 * 60 * 60,
                boundary + 24 * 60 * 60,
            )
            if (
                tail.start_utc.timestamp() >= boundary
                or tail.end_utc.timestamp() < boundary - TIMELINE_GAP_TOLERANCE_SECONDS
                or continuation_end <= boundary
                or not self._has_contiguous_continuation(
                    channel_id, tail, continuation_end
                )
                or not self.store.extend_playback(
                    lease_id,
                    continuation_end,
                    ttl_seconds=self.lease_ttl_seconds,
                )
            ):
                return False
            now = self.clock()
            with closing(self._connect()) as db:
                cursor = db.execute(
                    "UPDATE http_playback_sessions SET "
                    "end_utc=MAX(end_utc,?),continuation_reached=1,expires_at=? "
                    "WHERE lease_id=? AND user_id=? AND channel_id=? AND device_key=? "
                    "AND boundary_reached=1 AND continuation_reached=0 AND terminal=0",
                    (
                        continuation_end, now + self.lease_ttl_seconds,
                        lease_id, user_id, channel_id, device_key,
                    ),
                )
            return cursor.rowcount == 1

    @staticmethod
    def _is_manifest_tail(manifest_json: str, segment_id: str) -> bool:
        try:
            manifest = json.loads(manifest_json or "[]")
        except (TypeError, ValueError):
            return False
        return bool(
            isinstance(manifest, list)
            and manifest
            and isinstance(manifest[-1], list)
            and manifest[-1]
            and manifest[-1][0] == segment_id
        )

    def _has_contiguous_continuation(self, channel_id: str, segment, end: float) -> bool:
        """Require indexed media immediately after the published tail before extending."""
        try:
            tail_end = segment.end_utc.timestamp()
            following = self.store.segments(channel_id, tail_end, end)
        except (OSError, RuntimeError, sqlite3.Error):
            logger.exception("Could not verify archived media after programme boundary")
            return False
        for item in following:
            item_start = item.start_utc.timestamp()
            if item_start < tail_end:
                continue
            return item_start <= tail_end + TIMELINE_GAP_TOLERANCE_SECONDS
        return False

    def playlist(
        self,
        token: str | None,
        channel_id: str,
        start_utc: datetime | str | int | float,
        end_utc: datetime | str | int | float,
        *,
        live: bool = False,
        request_identity_end: datetime | str | int | float | None = None,
        programme_end_utc: datetime | str | int | float | None = None,
        continuation_end_utc: datetime | str | int | float | None = None,
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
            identity_end = (
                end if request_identity_end is None else _epoch(request_identity_end)
            )
            programme_end = (
                None if programme_end_utc is None else _epoch(programme_end_utc)
            )
            continuation_end = (
                None if continuation_end_utc is None else _epoch(continuation_end_utc)
            )
            if not start < identity_end <= start + 24 * 60 * 60:
                return _error(400, "invalid playback identity range")
            if programme_end is not None and programme_end <= start:
                programme_end = None
            if (
                continuation_end is not None
                and (
                    programme_end is None
                    or continuation_end <= programme_end
                    or continuation_end > start + 24 * 60 * 60
                )
            ):
                continuation_end = None
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
            f"{device_key}\0{channel_id}\0{start:.6f}\0{identity_end:.6f}".encode()
        ).hexdigest()
        with self._admission_lock(user_id):
            return self._playlist_locked(
                token, user_id, channel_id, start, end, request_key, device_key, live,
                programme_end, continuation_end,
            )

    def _playlist_locked(
        self,
        token: str,
        user_id: str,
        channel_id: str,
        start: float,
        end: float,
        request_key: str,
        device_key: str,
        live: bool,
        requested_programme_end: float | None,
        requested_continuation_end: float | None,
    ) -> HTTPResponse:
        acquisition = None
        try:
            # The lock protects the archive window and serializes admission until
            # the rendered playlist and its session row are committed.
            acquisition = self._new_lease(
                user_id, channel_id, start, end, request_key, device_key
            )
            lease_id = acquisition.lease_id
            state = self._session_state(lease_id)
            if (
                state is not None
                and state["boundary_reached"]
                and not state["continuation_reached"]
                and not state["terminal"]
            ):
                self._unlock_continuation_locked(
                    user_id, channel_id, lease_id, device_key
                )
                state = self._session_state(lease_id)
            programme_end = (
                state["programme_end_utc"]
                if state is not None and state["programme_end_utc"] is not None
                else requested_programme_end
            )
            continuation_end = (
                state["continuation_end_utc"]
                if state is not None and state["continuation_end_utc"] is not None
                else requested_continuation_end
            )
            continuation_reached = bool(
                state is not None and state["continuation_reached"]
            )
            effective_end = end
            if continuation_reached and state is not None:
                effective_end = max(effective_end, float(state["end_utc"]))
            pending_continuation = bool(
                programme_end is not None
                and continuation_end is not None
                and continuation_end > programme_end
                and not continuation_reached
            )
            terminal = bool(state is not None and state["terminal"])
            effective_live = bool(live or pending_continuation)
            if terminal:
                effective_live = False
            segments = self.store.segments(channel_id, start, effective_end)
            manifest = self._manifest_segments(
                channel_id,
                state["manifest_json"] if state is not None else "[]",
                segments,
                allow_append=not terminal,
            )
        except _PermissionDenied:
            return _error(403, "stream limit exceeded")
        except (TypeError, ValueError):
            if acquisition is not None:
                self._rollback_acquisition(acquisition)
            return _error(400, "invalid channel")
        except (OSError, RuntimeError, sqlite3.Error):
            if acquisition is not None:
                self._rollback_acquisition(acquisition)
            return _error(503, "archive temporarily unavailable")
        if not manifest:
            self._rollback_acquisition(acquisition)
            return _error(404, "no archived segments in requested range")
        try:
            root = self.base_path
            channel_component = quote(str(channel_id), safe="")

            def uri_for(segment) -> str:
                segment_component = quote(str(segment.id), safe="")
                return (
                    f"{root}/segment/{channel_component}/{segment_component}"
                    f"?token={quote(token, safe='')}&lease={quote(lease_id, safe='')}"
                )

            body = self._make_playlist(
                manifest, live=effective_live, uri_for=uri_for
            ).encode("utf-8")
            manifest_json = self._serialize_manifest(manifest)
        except (OSError, RuntimeError, ValueError):
            self._rollback_acquisition(acquisition)
            return _error(503, "archive temporarily unavailable")
        try:
            committed = self._commit_acquisition(
                acquisition, user_id, channel_id, request_key, device_key, start,
                effective_end, manifest_json, programme_end, continuation_end,
                terminal=not effective_live,
            )
        except (OSError, RuntimeError, sqlite3.Error):
            self._rollback_acquisition(acquisition)
            return _error(503, "archive temporarily unavailable")
        if not committed:
            self._rollback_acquisition(acquisition)
            return _error(503, "archive temporarily unavailable")
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

    def _session_state(self, lease_id: str):
        with closing(self._connect()) as db:
            return db.execute(
                "SELECT end_utc,programme_end_utc,continuation_end_utc,"
                "continuation_reached,boundary_reached,terminal,manifest_json "
                "FROM http_playback_sessions "
                "WHERE lease_id=?",
                (lease_id,),
            ).fetchone()

    def _manifest_segments(
        self, channel_id: str, manifest_json: str, candidates, *, allow_append: bool = True
    ):
        try:
            stored = json.loads(manifest_json or "[]")
        except (TypeError, ValueError) as exc:
            raise sqlite3.DatabaseError("invalid persisted HLS manifest") from exc
        if not isinstance(stored, list):
            raise sqlite3.DatabaseError("invalid persisted HLS manifest")
        manifest = []
        known_ids = set()
        for row in stored:
            if not isinstance(row, list) or len(row) != 5:
                raise sqlite3.DatabaseError("invalid persisted HLS manifest row")
            segment_id, start, end, discontinuity, relative_path = row
            try:
                start = datetime.fromtimestamp(float(start), tz=timezone.utc)
                end = datetime.fromtimestamp(float(end), tz=timezone.utc)
            except (OverflowError, TypeError, ValueError) as exc:
                raise sqlite3.DatabaseError("invalid persisted HLS manifest range") from exc
            if end <= start or not isinstance(segment_id, str):
                raise sqlite3.DatabaseError("invalid persisted HLS manifest range")
            relative_path = Path(str(relative_path))
            if relative_path.is_absolute() or ".." in relative_path.parts:
                raise sqlite3.DatabaseError("invalid persisted HLS manifest path")
            manifest.append(_PlaylistSegment(
                segment_id, channel_id, Path(self.store.root) / relative_path,
                start, end, bool(discontinuity),
            ))
            known_ids.add(segment_id)

        if not allow_append:
            return manifest

        high_water = max(
            (segment.end_utc.timestamp() for segment in manifest), default=None
        )
        for candidate in sorted(candidates, key=lambda item: (item.start_utc, item.id)):
            if candidate.id in known_ids:
                continue
            candidate_start = candidate.start_utc.timestamp()
            candidate_end = candidate.end_utc.timestamp()
            if high_water is not None and candidate_start < high_water:
                # EVENT playlists are append-only; a late segment may not be
                # inserted before the already published high-water mark.
                continue
            has_gap = (
                high_water is not None
                and candidate_start > high_water + TIMELINE_GAP_TOLERANCE_SECONDS
            )
            manifest.append(_PlaylistSegment(
                candidate.id,
                channel_id,
                candidate.path,
                candidate.start_utc,
                candidate.end_utc,
                bool(candidate.discontinuity or has_gap),
            ))
            known_ids.add(candidate.id)
            high_water = max(high_water or candidate_end, candidate_end)
        return manifest

    def _serialize_manifest(self, segments) -> str:
        records = []
        root = Path(self.store.root).resolve()
        for segment in segments:
            relative_path = Path(segment.path).resolve(strict=False).relative_to(root)
            records.append([
                segment.id,
                segment.start_utc.timestamp(),
                segment.end_utc.timestamp(),
                bool(segment.discontinuity),
                relative_path.as_posix(),
            ])
        return json.dumps(records, separators=(",", ":"))

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
        full_get = method == "GET" and (
            selected is None or (selected.start == 0 and selected.end == size - 1)
        )
        if full_get:
            try:
                self._mark_continuation_boundary(
                    user_id, channel_id, lease_id, device_key, segment
                )
            except Exception:
                # The segment was already read successfully. A boundary
                # bookkeeping failure must not turn that media response into
                # an error; it simply leaves the EVENT range closed.
                logger.exception("Could not advance archive programme boundary")
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
        with self._session_lock(user_id):
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
