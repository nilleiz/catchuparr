"""Opaque, revocable per-user credentials for archive playback.

Only SHA-256 digests are persisted. Callers must not log the token returned by
``create`` or the token supplied to ``lookup``/``revoke``.
"""

from __future__ import annotations

import hashlib
import secrets
import sqlite3
import time
from contextlib import closing
from pathlib import Path


class TokenStore:
    """Store per-user bearer tokens in the archive's SQLite database."""

    def __init__(self, archive_root: Path, *, database_path: Path | None = None):
        self.root = Path(archive_root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.database_path = Path(database_path) if database_path is not None else self.root / "archive.sqlite3"
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS http_tokens (
                    token_hash TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    revoked_at REAL
                )"""
            )
            db.execute("CREATE INDEX IF NOT EXISTS http_tokens_user ON http_tokens(user_id, revoked_at)")

    @staticmethod
    def _user_key(user_id: str) -> str:
        value = str(user_id).strip()
        if not value or len(value) > 255 or "\x00" in value:
            raise ValueError("user_id must be a non-empty identifier of at most 255 characters")
        return value

    @staticmethod
    def _digest(token: str) -> str:
        if not isinstance(token, str) or not token:
            return ""
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def create(self, user_id: str) -> str:
        """Create and return a 256-bit opaque token; plaintext is never stored."""
        user = self._user_key(user_id)
        while True:
            token = secrets.token_urlsafe(32)
            digest = self._digest(token)
            try:
                with closing(self._connect()) as db:
                    db.execute(
                        "INSERT INTO http_tokens(token_hash,user_id,created_at) VALUES(?,?,?)",
                        (digest, user, time.time()),
                    )
                return token
            except sqlite3.IntegrityError:
                # A digest collision is practically impossible, but retrying is
                # safer than returning a credential that belongs to another user.
                continue

    def lookup(self, token: str) -> str | None:
        """Resolve an active token to its user identifier."""
        digest = self._digest(token)
        if not digest:
            return None
        with closing(self._connect()) as db:
            row = db.execute(
                "SELECT user_id FROM http_tokens WHERE token_hash=? AND revoked_at IS NULL", (digest,)
            ).fetchone()
        return str(row["user_id"]) if row else None

    def revoke(self, token: str) -> bool:
        """Revoke one token. The operation is idempotent for known tokens."""
        digest = self._digest(token)
        if not digest:
            return False
        with closing(self._connect()) as db:
            cursor = db.execute(
                "UPDATE http_tokens SET revoked_at=? WHERE token_hash=? AND revoked_at IS NULL",
                (time.time(), digest),
            )
        return cursor.rowcount == 1

    def revoke_user(self, user_id: str) -> int:
        """Revoke every active token belonging to one user."""
        user = self._user_key(user_id)
        with closing(self._connect()) as db:
            cursor = db.execute(
                "UPDATE http_tokens SET revoked_at=? WHERE user_id=? AND revoked_at IS NULL",
                (time.time(), user),
            )
        return int(cursor.rowcount)


class AccessTokenStore(TokenStore):
    """Runtime-facing name and ``issue`` verb used by the plugin adapter."""

    def issue(self, user_id: str) -> str:
        return self.create(user_id)
