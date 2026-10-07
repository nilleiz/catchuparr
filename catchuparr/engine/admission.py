"""Cross-protocol playback admission locks shared by HLS and TS."""

from __future__ import annotations

import fcntl
import hashlib
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def playback_admission_lock(archive_root: Path | str, user_id: str):
    """Serialize one user's HLS and TS session admission across workers."""
    key = hashlib.sha256(str(user_id).encode("utf-8")).hexdigest()[:24]
    lock_path = Path(archive_root) / f".http-playback-admission-{key}.lock"
    with lock_path.open("a") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
