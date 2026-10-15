"""Owned subprocess modes for the live shared-reader REGISTER crash probe.

This helper is invoked only by the disposable AIO media integration probe. It
accepts a short-lived synthetic capability on stdin and emits fixed boolean /
count observations only. The crash mode exits itself immediately after the
native REGISTER Lua ACK and before the route can persist its SQLite ACK.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path


def _emit(value: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(value, sort_keys=True) + "\n")
    sys.stdout.flush()


def _payload() -> dict[str, object]:
    raw = sys.stdin.read(8193)
    if not raw or len(raw) > 8192:
        raise ValueError("bounded probe input is required")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("probe input must be an object")
    return value


def _setup_django() -> None:
    configured_root = os.environ.get("CATCHUPARR_PLUGIN_ROOT")
    if isinstance(configured_root, str) and configured_root:
        plugin_root = str(Path(configured_root).resolve())
    else:
        candidate = Path("/data/plugins")
        plugin_root = str(candidate.resolve()) if candidate.is_dir() else ""
    if not plugin_root:
        raise RuntimeError("Catchuparr plugin import root is unavailable")
    if plugin_root not in sys.path:
        sys.path.insert(0, plugin_root)
    import django

    django.setup()


def _open(capability_token: str):
    from catchuparr.adapters.sharing_borrow_route import decode_borrow_capability
    from catchuparr.sharing_runtime import open_borrow_session

    return open_borrow_session(decode_borrow_capability(capability_token))


def _crash_after_register(capability_token: str) -> int:
    import catchuparr.adapters.sharing_borrow_route as route

    original = route.register_borrowed_client

    def register_then_exit(*args, **kwargs):
        ack = original(*args, **kwargs)
        if ack is None:
            _emit({"native_registered": False})
            raise SystemExit(71)
        # This marker proves the real native Lua transaction returned an exact
        # ACK. os._exit deliberately skips SQLite activation/ACK and all Python
        # finally blocks, like a crash of this one owned request process.
        _emit({"native_registered": True})
        os._exit(73)

    route.register_borrowed_client = register_then_exit
    session = _open(capability_token)
    session.close()
    _emit({"native_registered": False})
    return 72


def _reconcile(database_path: str, episode_id: str) -> int:
    from core.utils import RedisClient

    from catchuparr.sharing_episode import BorrowEpisodeLedger
    from catchuparr.sharing_runtime import retry_durable_open_cleanups

    ledger = BorrowEpisodeLedger(Path(database_path))
    redis = RedisClient.get_client()
    if redis is None:
        _emit({"completed": 0, "pending": 1})
        return 74
    completed, pending = retry_durable_open_cleanups(
        ledger, redis, now=time.time(), stale_after=0, episode_id=episode_id,
    )
    _emit({"completed": int(completed), "pending": int(pending)})
    return 0 if completed == 1 and pending == 0 else 75


def _replay(capability_token: str) -> int:
    from catchuparr.sharing_episode import BorrowEpisodeRejected

    try:
        session = _open(capability_token)
    except BorrowEpisodeRejected:
        _emit({"denied": True})
        return 0
    session.close()
    _emit({"denied": False})
    return 76


def _successful_open_and_read(capability_token: str) -> int:
    session = _open(capability_token)
    iterator = session.iter_chunks()
    try:
        chunk = next(iterator)
        read_useful_ts = isinstance(chunk, bytes) and bool(chunk) and len(chunk) % 188 == 0
    finally:
        iterator.close()
        cleanup_acknowledged = session.close() is True
    _emit({"closed": cleanup_acknowledged, "opened": True, "read_ts": read_useful_ts})
    return 0 if read_useful_ts and cleanup_acknowledged else 77


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 1 or args[0] not in {
        "crash-after-register", "reconcile", "replay", "open-and-read",
    }:
        return 64
    try:
        payload = _payload()
        _setup_django()
        mode = args[0]
        if mode == "crash-after-register":
            token = payload.get("capability")
            if not isinstance(token, str) or not token:
                return 64
            return _crash_after_register(token)
        if mode == "reconcile":
            database_path = payload.get("database_path")
            episode_id = payload.get("episode_id")
            if not isinstance(database_path, str) or not isinstance(episode_id, str):
                return 64
            return _reconcile(database_path, episode_id)
        token = payload.get("capability")
        if not isinstance(token, str) or not token:
            return 64
        if mode == "replay":
            return _replay(token)
        return _successful_open_and_read(token)
    except Exception:
        # Never echo exceptions: Redis and Django errors can contain private
        # connection details. The caller receives only this fixed result code.
        _emit({"error": True})
        return 70


if __name__ == "__main__":
    raise SystemExit(main())
