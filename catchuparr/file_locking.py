"""Cooperative advisory file locks.

Dispatcharr runs uWSGI with gevent. A blocking ``fcntl.flock`` call stops the
whole worker process, including the greenlet that already holds the lock
through another file descriptor, so the worker deadlocks. Polling with a
non-blocking request and ``time.sleep`` yields to other greenlets (gevent
patches ``time.sleep``) and behaves like a blocking lock elsewhere.
"""

from __future__ import annotations

import fcntl
import time

_POLL_INITIAL_SECONDS = 0.005
_POLL_MAX_SECONDS = 0.1


def cooperative_flock(fd: int, operation: int) -> None:
    """Acquire ``operation`` (LOCK_SH or LOCK_EX) on ``fd``, waiting cooperatively."""
    delay = _POLL_INITIAL_SECONDS
    while True:
        try:
            fcntl.flock(fd, operation | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            time.sleep(delay)
            delay = min(delay * 2, _POLL_MAX_SECONDS)
