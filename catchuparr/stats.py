"""Version-checked projections for native Dispatcharr Stats.

The native builders remain the source of truth for provider and user state.
Catchuparr adds only display rows for its own playback sessions and filters a
recorder client only when a server-side capability marked that exact client.
"""

from __future__ import annotations

import contextvars
import copy
import functools
import hashlib
import inspect
import json
import logging
import secrets
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .compatibility import is_supported_dispatcharr_version

logger = logging.getLogger(__name__)

HOOK_MARKER = "__catchuparr_stats_hook_v1__"
DISPLAY_TIMEOUT_SECONDS = 180
HEARTBEAT_WRITE_INTERVAL_SECONDS = 30
DISPLAY_ID_PREFIX = "ca_"
_ORIGINALS: list[tuple[Any, str, Any]] = []
RECORDER_MARKER_TTL_SECONDS = 120
_STOP_HOOK: tuple[Any, Any, list[tuple[Any, Any]]] | None = None
_RECORDER_CONTEXT: contextvars.ContextVar[tuple[str, str, str] | None] = contextvars.ContextVar(
    "catchuparr_recorder_stats_context", default=None
)
_IDENTITY_ROUTES: list[tuple[Any, Any]] = []


def _db_path() -> Path:
    from .runtime import load_config

    config = load_config()
    if config is None:
        raise RuntimeError("Catchuparr is not configured")
    return Path(config.archive_root) / "archive.sqlite3"


def _ensure_schema(db: sqlite3.Connection) -> None:
    db.execute(
        """CREATE TABLE IF NOT EXISTS catchuparr_stats_viewers (
            viewer_key TEXT PRIMARY KEY,
            display_id TEXT NOT NULL UNIQUE,
            user_id TEXT NOT NULL,
            channel_uuid TEXT NOT NULL,
            playback_device_key TEXT NOT NULL DEFAULT '',
            logical_started_at REAL NOT NULL,
            last_success_at REAL NOT NULL,
            observed_position REAL,
            playback_lease_id TEXT,
            programme_start_epoch REAL,
            client_ip TEXT,
            revoked INTEGER NOT NULL DEFAULT 0
        )"""
    )
    columns = {str(row[1]) for row in db.execute("PRAGMA table_info(catchuparr_stats_viewers)")}
    if "playback_device_key" not in columns:
        db.execute(
            "ALTER TABLE catchuparr_stats_viewers "
            "ADD COLUMN playback_device_key TEXT NOT NULL DEFAULT ''"
        )
    for column, declaration in (
        ("programme_start_epoch", "REAL"),
        ("client_ip", "TEXT"),
    ):
        if column not in columns:
            db.execute(
                f"ALTER TABLE catchuparr_stats_viewers ADD COLUMN {column} {declaration}"
            )
    if "revoked" not in columns:
        db.execute(
            "ALTER TABLE catchuparr_stats_viewers "
            "ADD COLUMN revoked INTEGER NOT NULL DEFAULT 0"
        )
    if "playback_lease_id" not in columns:
        db.execute(
            "ALTER TABLE catchuparr_stats_viewers ADD COLUMN playback_lease_id TEXT"
        )


def _options() -> dict[str, bool]:
    """Read only the applied configuration when that API is available."""
    try:
        from .runtime import load_runtime_settings

        settings = load_runtime_settings()
    except Exception:
        # Compatibility for installations predating the unified Apply API.
        # Both options keep their documented defaults until that API exists.
        settings = {}
    return {
        "show_archive_playback_in_stats": _as_bool(
            settings.get("show_archive_playback_in_stats", True), True
        ),
        "hide_recorders_in_stats": _as_bool(
            settings.get("hide_recorders_in_stats", True), True
        ),
    }


def _valid_current_playback_lease(
    db: sqlite3.Connection,
    lease_id: str,
    user_id: str | int,
    channel_uuid: str,
    device_key: str,
    now: float,
) -> bool:
    """Verify the current TS/HLS lease from the same database transaction."""
    tables = {
        str(row[0])
        for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('ts_playback_sessions','http_playback_sessions')"
        )
    }
    if "ts_playback_sessions" in tables:
        current_ts = db.execute(
            "SELECT 1 FROM ts_playback_sessions WHERE lease_id=? AND user_id=? "
            "AND channel_id=? AND device_key=? AND active=1 AND expires_at>? LIMIT 1",
            (str(lease_id), str(user_id), str(channel_uuid), str(device_key), now),
        ).fetchone()
        if current_ts is not None:
            return True
    if "http_playback_sessions" in tables:
        current_http = db.execute(
            "SELECT 1 FROM http_playback_sessions WHERE lease_id=? AND user_id=? "
            "AND channel_id=? AND device_key=? AND grace_until IS NULL "
            "AND expires_at>? LIMIT 1",
            (str(lease_id), str(user_id), str(channel_uuid), str(device_key), now),
        ).fetchone()
        if current_http is not None:
            return True
    return False


def _as_bool(value: Any, default: bool) -> bool:
    if type(value) is bool:
        return value
    return default


def successful_playback(
    user_id: str | int,
    channel_uuid: str,
    logical_viewer_key: str,
    *,
    observed_position: float | None = None,
    heartbeat: bool = False,
    playback_lease_id: str | None = None,
    programme_start_epoch: float | None = None,
    client_ip: str | None = None,
    now: float | None = None,
) -> str:
    """Record a successful archive response and return its display-only ID.

    ``logical_viewer_key`` is a plugin-generated stable device identity, never
    a native timeshift session ID or playback credential. Position is left
    unknown unless a future playback implementation supplies an observation.
    """
    if not channel_uuid or not logical_viewer_key:
        raise ValueError("viewer and channel identity are required")
    if observed_position is not None:
        raise ValueError("archive playback position is not currently observed")
    now = time.time() if now is None else float(now)
    viewer_key = hashlib.sha256(
        f"catchuparr-viewer\0{user_id}\0{channel_uuid}\0{logical_viewer_key}".encode()
    ).hexdigest()
    database = _db_path()
    database.parent.mkdir(parents=True, exist_ok=True)
    changed = False
    with closing(sqlite3.connect(database, timeout=5)) as db:
        db.execute("PRAGMA busy_timeout=5000")
        _ensure_schema(db)
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            "SELECT display_id, logical_started_at, last_success_at, revoked, playback_lease_id, "
            "programme_start_epoch, client_ip "
            "FROM catchuparr_stats_viewers WHERE viewer_key=?",
            (viewer_key,),
        ).fetchone()
        if playback_lease_id is not None and not _valid_current_playback_lease(
            db,
            playback_lease_id,
            user_id,
            channel_uuid,
            logical_viewer_key,
            now,
        ):
            db.commit()
            return str(row[0]) if row is not None else ""
        if row is None:
            changed = True
            display_id = DISPLAY_ID_PREFIX + secrets.token_urlsafe(18)
            db.execute(
                "INSERT INTO catchuparr_stats_viewers "
                "(viewer_key,display_id,user_id,channel_uuid,playback_device_key,"
                "logical_started_at,last_success_at,observed_position,playback_lease_id,"
                "programme_start_epoch,client_ip) "
                "VALUES(?,?,?,?,?,?,?,NULL,?,?,?)",
                (
                    viewer_key, display_id, str(user_id), str(channel_uuid),
                    logical_viewer_key, now, now, playback_lease_id,
                    programme_start_epoch, client_ip,
                ),
            )
        else:
            display_id = str(row[0])
            lease_changed = (
                playback_lease_id is not None
                and row[4] is not None
                and str(row[4]) != str(playback_lease_id)
            )
            if heartbeat and (bool(row[3]) or lease_changed):
                db.commit()
                return display_id
            if bool(row[3]):
                # A delayed first body chunk from the stopped lease is still
                # that lease. Only a distinct, validated current TS lease may
                # start the logical viewer again.
                if not lease_changed:
                    db.commit()
                    return display_id
                changed = True
                db.execute(
                    "UPDATE catchuparr_stats_viewers SET playback_device_key=?,"
                    "logical_started_at=?,last_success_at=?,observed_position=NULL,"
                    "playback_lease_id=?,programme_start_epoch=?,client_ip=?,revoked=0 "
                    "WHERE viewer_key=?",
                    (
                        logical_viewer_key, now, now, playback_lease_id,
                        programme_start_epoch, client_ip, viewer_key,
                    ),
                )
            started_at = now if now - float(row[2]) > DISPLAY_TIMEOUT_SECONDS else float(row[1])
            if not bool(row[3]) and now - float(row[2]) >= HEARTBEAT_WRITE_INTERVAL_SECONDS:
                changed = True
                db.execute(
                    "UPDATE catchuparr_stats_viewers SET playback_device_key=?,"
                    "logical_started_at=?,last_success_at=?,observed_position=NULL,"
                    "playback_lease_id=?,programme_start_epoch=COALESCE(?,programme_start_epoch),"
                    "client_ip=COALESCE(?,client_ip) "
                    "WHERE viewer_key=?",
                    (
                        logical_viewer_key, started_at, now, playback_lease_id,
                        programme_start_epoch, client_ip, viewer_key,
                    ),
                )
            elif playback_lease_id is not None and not heartbeat:
                changed = True
                db.execute(
                    "UPDATE catchuparr_stats_viewers SET playback_lease_id=?,"
                    "programme_start_epoch=COALESCE(?,programme_start_epoch),"
                    "client_ip=COALESCE(?,client_ip) "
                    "WHERE viewer_key=?",
                    (playback_lease_id, programme_start_epoch, client_ip, viewer_key),
                )
            elif not heartbeat and (
                programme_start_epoch is not None or client_ip is not None
            ):
                changed = True
                db.execute(
                    "UPDATE catchuparr_stats_viewers SET "
                    "programme_start_epoch=COALESCE(?,programme_start_epoch),"
                    "client_ip=COALESCE(?,client_ip) WHERE viewer_key=?",
                    (programme_start_epoch, client_ip, viewer_key),
                )
        db.commit()
    if changed:
        _emit_timeshift_stats_update()
    return display_id


def recorder_client_registered(
    channel_id: str, client_id: str, capability_digest: str, channel_uuid: str
) -> bool:
    """Mark one native client after its capability was verified server-side."""
    if not channel_id or not client_id or not capability_digest or not channel_uuid:
        return False
    channel_key, client_key = str(channel_id), str(client_id)
    try:
        from core.utils import RedisClient

        redis = RedisClient.get_client()
        redis.set(
            _recorder_marker_key(channel_key, client_key),
            f"{capability_digest}|{channel_uuid}",
            ex=RECORDER_MARKER_TTL_SECONDS,
        )
        redis.sadd(_recorder_marker_index_key(capability_digest), _recorder_marker_key(channel_key, client_key))
        redis.expire(_recorder_marker_index_key(capability_digest), RECORDER_MARKER_TTL_SECONDS * 2)
    except Exception:
        logger.exception("Unable to persist verified recorder Stats identity")
        return False
    return True


def recorder_client_removed(channel_id: str, client_id: str) -> None:
    channel_key, client_key = str(channel_id), str(client_id)
    try:
        from core.utils import RedisClient

        redis = RedisClient.get_client()
        marker_key = _recorder_marker_key(channel_key, client_key)
        marker = redis.get(marker_key)
        if marker is not None:
            digest = _decode_redis_value(marker).partition("|")[0]
            redis.srem(_recorder_marker_index_key(digest), marker_key)
        redis.delete(marker_key)
    except Exception:
        logger.exception("Unable to remove verified recorder Stats identity")


def _recorder_marker_key(channel_id: str, client_id: str) -> str:
    return f"catchuparr:stats:recorder-client:{channel_id}:{client_id}"


def _recorder_marker_index_key(capability_digest: str) -> str:
    return f"catchuparr:stats:recorder-markers:{capability_digest}"


def refresh_recorder_markers(redis: Any, capability_digest: str) -> None:
    """Keep active native registrations hidden while their recorder lease renews."""
    index_key = _recorder_marker_index_key(capability_digest)
    prefix = "catchuparr:stats:recorder-client:"
    try:
        for raw_key in redis.smembers(index_key):
            key = _decode_redis_value(raw_key)
            if not key.startswith(prefix):
                redis.srem(index_key, raw_key)
                continue
            identity = key[len(prefix):]
            channel_id, separator, client_id = identity.partition(":")
            if not separator or not client_id:
                redis.srem(index_key, raw_key)
                continue
            live_clients = f"live:channel:{channel_id}:clients"
            if redis.sismember(live_clients, client_id):
                redis.expire(key, RECORDER_MARKER_TTL_SECONDS)
            else:
                redis.delete(key)
                redis.srem(index_key, raw_key)
        redis.expire(index_key, RECORDER_MARKER_TTL_SECONDS * 2)
    except Exception:
        logger.debug("Unable to refresh verified recorder Stats markers", exc_info=True)


def _is_recorder_client(channel_id: str, client_id: str) -> bool:
    try:
        from core.utils import RedisClient

        redis = RedisClient.get_client()
        marker = redis.get(_recorder_marker_key(str(channel_id), str(client_id)))
        return bool(marker and _capability_is_current(redis, marker))
    except Exception:
        return False


def _decode_redis_value(value: Any) -> str:
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)


def _verified_recorder_ids(redis: Any, channel_id: str, client_ids: list[str]) -> set[str]:
    if not client_ids:
        return set()
    try:
        keys = [_recorder_marker_key(channel_id, client_id) for client_id in client_ids]
        values = redis.mget(keys)
        verified = set()
        for client_id, value in zip(client_ids, values, strict=False):
            if value is None or not _capability_is_current(redis, value):
                continue
            redis.expire(_recorder_marker_key(channel_id, client_id), RECORDER_MARKER_TTL_SECONDS)
            verified.add(client_id)
        return verified
    except Exception:
        return {
            client_id for client_id in client_ids
            if _is_recorder_client(channel_id, client_id)
        }


def _capability_is_current(redis: Any, marker: Any) -> bool:
    digest, separator, channel_uuid = _decode_redis_value(marker).partition("|")
    if not separator or not digest or not channel_uuid:
        return False
    try:
        capability = redis.get(f"catchuparr:recorder:stats-cap:{digest}")
        lease = redis.get(f"catchuparr:recorder:{channel_uuid}")
    except Exception:
        return False
    if capability is None or lease is None:
        return False
    expected = _decode_redis_value(capability)
    lease_value = _decode_redis_value(lease)
    expected_channel, separator, owner = expected.partition("|")
    return bool(separator and expected_channel == channel_uuid and owner and owner == lease_value)


def _emit_timeshift_stats_update() -> None:
    """Ask native Stats websocket clients to refresh their shared projection."""
    try:
        from apps.timeshift.views import _trigger_timeshift_stats_update
        from core.utils import RedisClient

        _trigger_timeshift_stats_update(RedisClient.get_client())
    except Exception:
        logger.debug("Unable to emit native Timeshift Stats update", exc_info=True)


def _verified_recorder_request(
    request: Any,
    channel_uuid: str,
    *,
    meta_header: str = "HTTP_X_CATCHUPARR_RECORDER",
    request_header: str = "X-Catchuparr-Recorder",
):
    capability = ""
    try:
        capability = str(request.META.get(meta_header) or "")
    except Exception:
        pass
    if not capability:
        try:
            capability = str(request.headers.get(request_header) or "")
        except Exception:
            pass
    if not capability or len(capability) > 128:
        return None
    digest = hashlib.sha256(capability.encode("ascii", errors="ignore")).hexdigest()
    capability_key = f"catchuparr:recorder:stats-cap:{digest}"
    try:
        from core.utils import RedisClient

        redis = RedisClient.get_client()
        record = redis.get(capability_key)
        if record is None:
            return None
        record = _decode_redis_value(record)
        expected_channel, separator, lease_owner = record.partition("|")
        if not separator or expected_channel != str(channel_uuid) or not lease_owner:
            return None
        current_lease = redis.get(f"catchuparr:recorder:{expected_channel}")
        if current_lease is None or _decode_redis_value(current_lease) != lease_owner:
            return None
    except Exception:
        return None
    return digest


def revoke_display_session(display_id: str) -> bool:
    """Revoke the plugin playback leases represented by one display ID."""
    if not isinstance(display_id, str) or not display_id.startswith(DISPLAY_ID_PREFIX):
        return False
    database = _db_path()
    with closing(sqlite3.connect(database, timeout=5)) as db:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=5000")
        _ensure_schema(db)
        row = db.execute(
            "SELECT user_id,channel_uuid,playback_device_key,revoked "
            "FROM catchuparr_stats_viewers "
            "WHERE display_id=?",
            (display_id,),
        ).fetchone()
        if row is None or bool(row["revoked"]):
            return False
        user_id = str(row["user_id"])
        channel_uuid = str(row["channel_uuid"])
        device_key = str(row["playback_device_key"])
        db.execute("BEGIN IMMEDIATE")
        ts_leases = [
            str(item[0]) for item in db.execute(
                "SELECT lease_id FROM ts_playback_sessions "
                "WHERE user_id=? AND channel_id=? AND device_key=?",
                (user_id, channel_uuid, device_key),
            ).fetchall()
        ] if _table_exists(db, "ts_playback_sessions") else []
        http_leases = [
            str(item[0]) for item in db.execute(
                "SELECT lease_id FROM http_playback_sessions "
                "WHERE user_id=? AND channel_id=? AND device_key=?",
                (user_id, channel_uuid, device_key),
            ).fetchall()
        ] if _table_exists(db, "http_playback_sessions") else []
        for lease_id in ts_leases:
            if _table_exists(db, "ts_playback_streams"):
                db.execute("DELETE FROM ts_playback_streams WHERE lease_id=?", (lease_id,))
            db.execute("DELETE FROM ts_playback_sessions WHERE lease_id=?", (lease_id,))
        for lease_id in http_leases:
            if _table_exists(db, "http_playback_sessions"):
                db.execute("DELETE FROM http_playback_sessions WHERE lease_id=?", (lease_id,))
        db.execute(
            "UPDATE catchuparr_stats_viewers SET revoked=1,last_success_at=0 "
            "WHERE display_id=?",
            (display_id,),
        )
        db.commit()
    try:
        from .engine.store import ArchiveStore
        from .runtime import load_config

        config = load_config()
        if config is not None:
            store = ArchiveStore(config.archive_root)
            for lease_id in ts_leases + http_leases:
                store.end_playback(lease_id)
    except Exception:
        logger.exception("Unable to release Catchuparr archive leases after Stop")
    _emit_timeshift_stats_update()
    return True


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _request_session_id(request: Any) -> str:
    data = getattr(request, "data", None)
    if isinstance(data, dict):
        return str(data.get("session_id") or "")
    post = getattr(request, "POST", None)
    if post is not None:
        value = post.get("session_id")
        if value:
            return str(value)
    try:
        data = json.loads(request.body or b"{}")
    except (AttributeError, TypeError, ValueError, json.JSONDecodeError):
        return ""
    return str(data.get("session_id") or "") if isinstance(data, dict) else ""


def _registered_drf_endpoint(
    route: Any,
    expected_view: Any,
    method: str,
    *,
    endpoint_name: str,
    permission_class: type,
) -> bool:
    """Check that a URL still registers the exact native endpoint we wrap."""
    callback = getattr(route, "callback", None)
    if callback is None or callback is not expected_view:
        return False
    try:
        parameters = inspect.signature(callback).parameters
    except (TypeError, ValueError):
        return False
    if tuple(parameters) != ("request", "args", "kwargs"):
        return False
    parameter_values = tuple(parameters.values())
    if (
        parameter_values[0].kind is not inspect.Parameter.POSITIONAL_OR_KEYWORD
        or parameter_values[1].kind is not inspect.Parameter.VAR_POSITIONAL
        or parameter_values[2].kind is not inspect.Parameter.VAR_KEYWORD
    ):
        return False
    # DRF @api_view callbacks are variadic. Validate their generated APIView
    # class and the wrapped native function before accepting that signature.
    try:
        unwrapped = inspect.unwrap(callback)
    except Exception:
        unwrapped = callback
    view_class = getattr(callback, "cls", None) or getattr(unwrapped, "cls", None)
    if (
        view_class is None
        or getattr(view_class, "__name__", None) != endpoint_name
        or getattr(view_class, "__module__", None) != getattr(expected_view, "__module__", None)
    ):
        return False
    allowed_methods = set(getattr(view_class, "http_method_names", ()))
    if method not in allowed_methods:
        return False
    if permission_class not in tuple(getattr(view_class, "permission_classes", ())):
        return False
    handler = getattr(view_class, method, None)
    if handler is None:
        return False
    candidates = [handler]
    for cell in getattr(handler, "__closure__", ()) or ():
        try:
            enclosed = cell.cell_contents
        except ValueError:
            continue
        if callable(enclosed):
            candidates.append(enclosed)
    for candidate in candidates:
        if (
            getattr(candidate, "__name__", None) != endpoint_name
            or getattr(candidate, "__module__", None) != getattr(expected_view, "__module__", None)
        ):
            continue
        try:
            if tuple(inspect.signature(candidate).parameters) == ("request",):
                return True
        except (TypeError, ValueError):
            continue
    return False


def _supported_stream_view_signature(stream_view: Any, signature: tuple[str, ...]) -> bool:
    if signature == ("request", "channel_id", "user", "force_output_format"):
        return True
    if signature != ("request", "args", "kwargs"):
        return False
    try:
        parameters = tuple(inspect.signature(stream_view).parameters.values())
    except (TypeError, ValueError):
        return False
    return (
        parameters[0].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        and parameters[1].kind is inspect.Parameter.VAR_POSITIONAL
        and parameters[2].kind is inspect.Parameter.VAR_KEYWORD
    )


def _install_live_display_hooks(channel_status: Any) -> bool:
    """Install display-only live builders in web and worker processes."""
    basic = getattr(channel_status.ChannelStatus, "get_basic_channel_info", None)
    detail = getattr(channel_status.ChannelStatus, "get_detailed_channel_info", None)
    if (
        basic is None
        or tuple(inspect.signature(basic).parameters) != ("channel_id",)
        or detail is None
        or tuple(inspect.signature(detail).parameters) != ("channel_id",)
    ):
        return False
    if not getattr(basic, HOOK_MARKER, False):
        @functools.wraps(basic)
        def basic_wrapper(channel_id):
            return _visible_basic_channel_info(
                channel_status, channel_id, basic(channel_id)
            )

        setattr(basic_wrapper, HOOK_MARKER, True)
        setattr(basic_wrapper, "__catchuparr_original__", basic)
        channel_status.ChannelStatus.get_basic_channel_info = basic_wrapper
        _ORIGINALS.append((channel_status.ChannelStatus, "get_basic_channel_info", basic))

    if not getattr(detail, HOOK_MARKER, False):
        @functools.wraps(detail)
        def detail_wrapper(channel_id):
            return _visible_detail_channel_info(channel_id, detail(channel_id))

        setattr(detail_wrapper, HOOK_MARKER, True)
        setattr(detail_wrapper, "__catchuparr_original__", detail)
        channel_status.ChannelStatus.get_detailed_channel_info = detail_wrapper
        _ORIGINALS.append((channel_status.ChannelStatus, "get_detailed_channel_info", detail))
    return True


def _install_stop_hook() -> bool:
    global _STOP_HOOK
    import apps.timeshift.stats_views as stats_views
    import apps.timeshift.urls as timeshift_urls

    original = getattr(stats_views, "stop_timeshift_session", None)
    if original is None or getattr(original, HOOK_MARKER, False):
        return original is not None
    from apps.accounts.permissions import IsAdmin
    changed_routes: list[tuple[Any, Any]] = []
    already_installed = False
    for route in getattr(timeshift_urls, "urlpatterns", ()):
        if getattr(route, "name", None) != "catchup_stop_client":
            continue
        callback = getattr(route, "callback", None)
        if (
            getattr(callback, HOOK_MARKER, False)
            and getattr(callback, "__catchuparr_original__", None) is original
        ):
            already_installed = True
            continue
        if not _registered_drf_endpoint(
            route,
            original,
            "post",
            endpoint_name="stop_timeshift_session",
            permission_class=IsAdmin,
        ):
            continue

        @functools.wraps(callback)
        def wrapped(*args, __original=callback, **kwargs):
            # Wrap the registered DRF callback so its native permission classes
            # and response finalization run before plugin IDs are considered.
            request = args[0] if args else kwargs.get("request")
            response = __original(*args, **kwargs)
            if request is None:
                return response
            display_id = _request_session_id(request)
            if not display_id.startswith(DISPLAY_ID_PREFIX):
                return response
            if getattr(response, "status_code", 200) != 404:
                return response
            if not revoke_display_session(display_id):
                return response
            from django.http import JsonResponse

            return JsonResponse({"success": True})

        setattr(wrapped, HOOK_MARKER, True)
        setattr(wrapped, "__catchuparr_original__", callback)
        route.callback = wrapped
        changed_routes.append((route, callback))

    if not changed_routes:
        return already_installed
    _STOP_HOOK = (stats_views, original, changed_routes)
    return True


def _install_identity_hooks(channel_status: Any, live_views: Any) -> bool:
    global _IDENTITY_ROUTES
    import apps.proxy.live_proxy.urls as live_urls
    from apps.proxy.live_proxy.client_manager import ClientManager

    basic = getattr(channel_status.ChannelStatus, "get_basic_channel_info", None)
    detail = getattr(channel_status.ChannelStatus, "get_detailed_channel_info", None)
    original_add = getattr(ClientManager, "add_client", None)
    original_remove = getattr(ClientManager, "remove_client", None)
    stream_ts = getattr(live_views, "stream_ts", None)
    expected_add = (
        "self", "client_id", "client_ip", "user_agent", "user",
        "output_format", "output_profile_id",
    )
    try:
        stream_signature = tuple(inspect.signature(stream_ts).parameters)
    except (TypeError, ValueError):
        stream_signature = ()
    if (
        basic is None or tuple(inspect.signature(basic).parameters) != ("channel_id",)
        or detail is None or tuple(inspect.signature(detail).parameters) != ("channel_id",)
        or original_add is None or tuple(inspect.signature(original_add).parameters) != expected_add
        or original_remove is None
        or tuple(inspect.signature(original_remove).parameters) != ("self", "client_id")
        or stream_ts is None
        or not _supported_stream_view_signature(stream_ts, stream_signature)
    ):
        return False
    from .adapters.recorder_proxy import _stream_route_channel_id_issue

    route_matches = []
    for index, route in enumerate(getattr(live_urls, "urlpatterns", ())):
        if getattr(route, "name", None) != "stream":
            continue
        callback = getattr(route, "callback", None)
        guarded_original = getattr(callback, "_catchuparr_original", None)
        stats_original = getattr(callback, "__catchuparr_original__", None)
        if callback is not stream_ts and not (
            getattr(callback, "_catchuparr_managed_id_guard", False)
            and guarded_original is stream_ts
        ) and not (
            getattr(callback, HOOK_MARKER, False)
            and (
                stats_original is stream_ts
                or (
                    getattr(stats_original, "_catchuparr_managed_id_guard", False)
                    and getattr(stats_original, "_catchuparr_original", None) is stream_ts
                )
            )
        ):
            continue
        validated_route = route
        if callback is not stream_ts and not getattr(callback, "_catchuparr_managed_id_guard", False):
            # Validate the original registered endpoint under our own wrapper.
            validated_route = copy.copy(route)
            validated_route.callback = stats_original
        if _stream_route_channel_id_issue(validated_route, index, stream_ts) is None:
            route_matches.append((route, callback))
    if not route_matches:
        return False

    if not getattr(original_add, HOOK_MARKER, False):
        @functools.wraps(original_add)
        def add_client_wrapper(
            self, client_id, client_ip, user_agent=None, user=None,
            output_format="mpegts", output_profile_id=None,
        ):
            result = original_add(
                self, client_id, client_ip, user_agent, user,
                output_format, output_profile_id,
            )
            context = _RECORDER_CONTEXT.get()
            if context is not None and result:
                recorder_client_registered(
                    str(self.channel_id), str(client_id), context[1], context[2]
                )
            return result

        setattr(add_client_wrapper, HOOK_MARKER, True)
        setattr(add_client_wrapper, "__catchuparr_original__", original_add)
        ClientManager.add_client = add_client_wrapper
        _ORIGINALS.append((ClientManager, "add_client", original_add))

    if not getattr(original_remove, HOOK_MARKER, False):
        @functools.wraps(original_remove)
        def remove_client_wrapper(self, client_id):
            result = original_remove(self, client_id)
            recorder_client_removed(str(self.channel_id), str(client_id))
            return result

        setattr(remove_client_wrapper, HOOK_MARKER, True)
        setattr(remove_client_wrapper, "__catchuparr_original__", original_remove)
        ClientManager.remove_client = remove_client_wrapper
        _ORIGINALS.append((ClientManager, "remove_client", original_remove))

    for route, route_original in route_matches:
        if getattr(route_original, HOOK_MARKER, False):
            continue

        @functools.wraps(route_original)
        def stream_wrapper(request, channel_id, *args, __original=route_original, **kwargs):
            capability_digest = _verified_recorder_request(request, str(channel_id))
            if capability_digest is None:
                return __original(request, channel_id, *args, **kwargs)
            meta = getattr(request, "META", None)
            header_name = "HTTP_X_CATCHUPARR_RECORDER"
            if isinstance(meta, dict):
                meta.pop(header_name, None)
            context_token = _RECORDER_CONTEXT.set(
                (str(channel_id), capability_digest, str(channel_id))
            )
            try:
                return __original(request, channel_id, *args, **kwargs)
            finally:
                _RECORDER_CONTEXT.reset(context_token)

        setattr(stream_wrapper, HOOK_MARKER, True)
        setattr(stream_wrapper, "__catchuparr_original__", route_original)
        route.callback = stream_wrapper
        _IDENTITY_ROUTES.append((route, route_original))
    return True


def _active_viewers() -> list[dict[str, Any]]:
    options = _options()
    if not options["show_archive_playback_in_stats"]:
        return []
    try:
        database = _db_path()
        with closing(sqlite3.connect(database, timeout=2)) as db:
            db.row_factory = sqlite3.Row
            _ensure_schema(db)
            now = time.time()
            rows = db.execute(
                "SELECT * FROM catchuparr_stats_viewers "
                "WHERE revoked=0 AND last_success_at>?",
                (now - DISPLAY_TIMEOUT_SECONDS,),
            ).fetchall()
        return [_viewer_row(row) for row in rows]
    except (OSError, sqlite3.Error, RuntimeError):
        logger.exception("Unable to read Catchuparr Stats viewer projection")
        return []
    finally:
        # This projection can run in native websocket/background greenlets,
        # where request-finished connection cleanup does not run. Release only
        # the initialized default connection in this context and only when it
        # is safe to close (outside atomic blocks with autocommit enabled).
        try:
            from .recorder_proxy import _close_database_connections

            _close_database_connections()
        except Exception:
            logger.error("Unable to release Stats metadata database connection")


def _viewer_row(row: sqlite3.Row) -> dict[str, Any] | None:
    try:
        from apps.accounts.models import User
        from apps.channels.models import Channel

        channel = Channel.objects.filter(uuid=row["channel_uuid"]).first()
        user = User.objects.filter(id=row["user_id"]).first()
    except Exception:
        logger.exception("Unable to resolve Catchuparr Stats metadata")
        return None
    if channel is None or user is None:
        return None
    display_id = str(row["display_id"])
    started_at = float(row["logical_started_at"])
    last_seen = float(row["last_success_at"])
    connection = {
        "client_id": display_id,
        "session_id": display_id,
        "ip_address": row["client_ip"],
        "user_agent": "Catchuparr archive playback",
        "user_id": str(user.id),
        "username": str(getattr(user, "username", "")),
        "connected_at": started_at,
        "duration": max(0, int(last_seen - started_at)),
        "bytes_streamed": 0,
        "avg_bitrate_kbps": None,
        "m3u_profile": {},
        "m3u_profile_id": None,
    }
    programme_epoch = row["programme_start_epoch"]
    programme_start = (
        datetime.fromtimestamp(float(programme_epoch), timezone.utc).strftime("%Y-%m-%d:%H-%M")
        if programme_epoch is not None else None
    )
    return {
        "session_id": display_id,
        "stats_channel_id": display_id,
        "channel_id": int(channel.id),
        "channel_uuid": str(channel.uuid),
        "channel_name": str(channel.name),
        "logo_id": getattr(channel, "logo_id", None),
        "programme_start": programme_start,
        "position_anchor_at": None,
        "playback_base_secs": row["observed_position"],
        "paused": None,
        "resolution": None,
        "source_fps": None,
        "video_codec": None,
        "audio_codec": None,
        "audio_channels": None,
        "stream_type": "catchuparr_archive",
        "connection_count": 1,
        "connections": [connection],
    }


def project_timeshift_stats(native_result: Any) -> Any:
    """Copy and extend the native builder result without touching its source."""
    if not isinstance(native_result, dict):
        return native_result
    result = copy.deepcopy(native_result)
    sessions = result.get("timeshift_sessions")
    if not isinstance(sessions, list):
        return result
    additional = [item for item in _active_viewers() if item is not None]
    if additional:
        sessions.extend(additional)
        result["total_connections"] = int(result.get("total_connections") or 0) + len(additional)
    return result


def project_live_stats(native_result: Any) -> Any:
    """Hide only exact verified recorder clients from a copied live payload."""
    if not _options()["hide_recorders_in_stats"] or not isinstance(native_result, dict):
        return native_result
    result = copy.deepcopy(native_result)
    channels = result.get("channels")
    if not isinstance(channels, list):
        return result
    hidden_total = 0
    for channel in channels:
        if not isinstance(channel, dict):
            continue
        channel_key = str(channel.get("channel_id", ""))
        clients = channel.get("clients")
        if not isinstance(clients, list):
            continue
        retained = []
        hidden = 0
        for client in clients:
            client_id = str(client.get("client_id", "")) if isinstance(client, dict) else ""
            if _is_recorder_client(channel_key, client_id):
                hidden += 1
            else:
                retained.append(client)
        if hidden:
            channel["clients"] = retained
            if "client_count" in channel:
                channel["client_count"] = max(0, int(channel["client_count"]) - hidden)
            if "connection_count" in channel:
                channel["connection_count"] = max(0, int(channel["connection_count"]) - hidden)
            hidden_total += hidden
    if hidden_total and "total_connections" in result:
        result["total_connections"] = max(0, int(result["total_connections"]) - hidden_total)
    return result


def _basic_client_from_metadata(client_id: str, values: list[Any]) -> dict[str, Any]:
    user_agent, ip_address, connected_at, user_id, output_format, raw_profile_id = (
        _decode_redis_value(value) if value is not None else None for value in values
    )
    profile_id = (
        int(raw_profile_id)
        if raw_profile_id and raw_profile_id not in ("None", "0", "")
        else None
    )
    client = {
        "client_id": client_id,
        "user_agent": user_agent,
        "output_format": output_format or "mpegts",
        "output_profile_id": profile_id,
    }
    if ip_address:
        client["ip_address"] = ip_address
    if connected_at:
        client["connected_at"] = float(connected_at)
    if user_id:
        client["user_id"] = user_id
    return client


def _visible_basic_channel_info(
    channel_status_module: Any, channel_id: Any, native_info: Any
) -> Any:
    """Rebuild the visible top-ten list from real native client IDs first."""
    if not _options()["hide_recorders_in_stats"] or not isinstance(native_info, dict):
        return native_info
    proxy_server = getattr(channel_status_module, "proxy_server", None)
    if proxy_server is None:
        proxy_server_class = getattr(channel_status_module, "ProxyServer", None)
        if proxy_server_class is not None:
            try:
                proxy_server = proxy_server_class.get_instance()
            except Exception:
                proxy_server = None
    redis = getattr(proxy_server, "redis_client", None)
    redis_keys = getattr(channel_status_module, "RedisKeys", None)
    if redis is None or redis_keys is None:
        return native_info
    try:
        client_set_key = redis_keys.clients(channel_id)
        raw_ids = redis.smembers(client_set_key)
        client_ids = [_decode_redis_value(value) for value in raw_ids]
        hidden_ids = _verified_recorder_ids(redis, str(channel_id), client_ids)
        visible_ids = [client_id for client_id in client_ids if client_id not in set(hidden_ids)]
        visible_by_id = {
            str(item.get("client_id")): copy.deepcopy(item)
            for item in native_info.get("clients", [])
            if isinstance(item, dict) and item.get("client_id") is not None
        }
        ordered_visible_ids = [
            client_id for client_id in visible_by_id if client_id in set(visible_ids)
        ]
        ordered_visible_ids.extend(sorted(
            client_id for client_id in visible_ids if client_id not in visible_by_id
        ))
        clients = []
        for client_id in ordered_visible_ids:
            if client_id in visible_by_id:
                clients.append(visible_by_id[client_id])
                continue
            metadata_key = redis_keys.client_metadata(channel_id, client_id)
            metadata = redis.hmget(
                metadata_key,
                "user_agent", "ip_address", "connected_at", "user_id",
                "output_format", "output_profile_id",
            )
            clients.append(_basic_client_from_metadata(client_id, metadata))
        result = copy.deepcopy(native_info)
        result["clients"] = clients[:10]
        if "client_count" in result:
            result["client_count"] = max(
                0, int(result.get("client_count") or 0) - len(hidden_ids)
            )
        return result
    except Exception:
        logger.exception("Unable to filter verified recorder identities in live Stats")
        return native_info


def _visible_detail_channel_info(channel_id: Any, native_info: Any) -> Any:
    if not _options()["hide_recorders_in_stats"] or not isinstance(native_info, dict):
        return native_info
    result = copy.deepcopy(native_info)
    clients = result.get("clients")
    if not isinstance(clients, list):
        return result
    channel_key = str(channel_id)
    client_ids = [
        str(item.get("client_id", ""))
        for item in clients if isinstance(item, dict)
    ]
    try:
        from core.utils import RedisClient

        hidden_ids = _verified_recorder_ids(
            RedisClient.get_client(), channel_key, client_ids
        )
    except Exception:
        hidden_ids = set()
    retained = [
        item for item in clients
        if not isinstance(item, dict)
        or str(item.get("client_id", "")) not in hidden_ids
    ]
    hidden = len(clients) - len(retained)
    result["clients"] = retained
    if hidden and "client_count" in result:
        result["client_count"] = max(0, int(result["client_count"]) - hidden)
    return result


def _checked_wrapper(function: Any, transform):
    if getattr(function, HOOK_MARKER, False):
        return function
    try:
        signature = inspect.signature(function)
    except (TypeError, ValueError):
        return None
    if tuple(signature.parameters) != ("redis_client",):
        return None

    @functools.wraps(function)
    def wrapped(redis_client):
        return transform(function(redis_client))

    setattr(wrapped, HOOK_MARKER, True)
    setattr(wrapped, "__catchuparr_original__", function)
    return wrapped


def _install_builder(module: Any, name: str, transform) -> bool:
    function = getattr(module, name, None)
    if function is None:
        return False
    wrapped = _checked_wrapper(function, transform)
    if wrapped is None:
        return False
    if wrapped is function:
        return True
    setattr(module, name, wrapped)
    _ORIGINALS.append((module, name, function))
    return True


def install_stats_hooks(*, route_hooks: bool = True) -> bool:
    """Install hooks only for inspected Dispatcharr 0.31/0.32 signatures."""
    from version import __version__ as dispatcharr_version

    if not is_supported_dispatcharr_version(dispatcharr_version):
        return False
    import apps.proxy.live_proxy.channel_status as channel_status
    import apps.proxy.live_proxy.views as live_views
    import apps.proxy.stats_views as proxy_stats_views
    import apps.proxy.tasks as proxy_tasks
    import apps.timeshift.stats as timeshift_stats
    import apps.timeshift.stats_views as timeshift_stats_views

    targets = (
        (timeshift_stats, "build_timeshift_stats_data", project_timeshift_stats),
        (timeshift_stats_views, "build_timeshift_stats_data", project_timeshift_stats),
        (proxy_stats_views, "build_timeshift_stats_data", project_timeshift_stats),
        (channel_status, "build_live_channel_stats_data", project_live_stats),
        (live_views, "build_live_channel_stats_data", project_live_stats),
        (proxy_stats_views, "build_live_channel_stats_data", project_live_stats),
        (proxy_tasks, "build_live_channel_stats_data", project_live_stats),
    )
    installed = _install_live_display_hooks(channel_status)
    for module, name, transform in targets:
        # Some verified imports only expose one of the two builders.
        if getattr(module, name, None) is not None:
            installed = _install_builder(module, name, transform) and installed
    if not route_hooks:
        return installed
    installed = _install_identity_hooks(channel_status, live_views) and installed
    return _install_stop_hook() and installed


def uninstall_stats_hooks() -> bool:
    global _STOP_HOOK
    restored = True
    if _STOP_HOOK is not None:
        module, original, routes = _STOP_HOOK
        for route, route_original in routes:
            callback = getattr(route, "callback", None)
            if getattr(callback, "__catchuparr_original__", None) is route_original:
                route.callback = route_original
            else:
                restored = False
        _STOP_HOOK = None
    while _IDENTITY_ROUTES:
        route, original = _IDENTITY_ROUTES.pop()
        callback = getattr(route, "callback", None)
        if getattr(callback, "__catchuparr_original__", None) is original:
            route.callback = original
        elif callback is not original:
            restored = False
    while _ORIGINALS:
        module, name, function = _ORIGINALS.pop()
        current = getattr(module, name, None)
        if getattr(current, "__catchuparr_original__", None) is not function:
            restored = False
            continue
        setattr(module, name, function)
    return restored
