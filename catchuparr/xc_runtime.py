"""Runtime bindings for Dispatcharr XC hooks and local TS playback."""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import math
import sqlite3
import threading
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote

from .adapters.xc import (
    XCCallbacks,
    finalize_dispatcharr_response,
    install_xc_hooks,
    uninstall_xc_hooks,
)
from .compatibility import is_supported_dispatcharr_version

logger = logging.getLogger(__name__)
_TS_SERVICES = {}
_TS_SERVICES_LOCK = threading.Lock()
_CURRENT_EDGE_MAX_AGE_SECONDS = 90


class _PlaybackHeartbeatIterator:
    """Refresh only the display heartbeat while successful TS bytes arrive."""

    def __init__(self, iterator, heartbeat, interval: float = 30.0):
        self.iterator = iterator
        self.heartbeat = heartbeat
        self.interval = interval
        self.last_heartbeat = time.monotonic()
        self.started = False

    def __iter__(self):
        return self

    def __next__(self):
        chunk = next(self.iterator)
        now = time.monotonic()
        if not self.started or now - self.last_heartbeat >= self.interval:
            self.heartbeat(not self.started)
            self.last_heartbeat = now
            self.started = True
        return chunk

    def close(self):
        close = getattr(self.iterator, "close", None)
        if close is not None:
            close()


def install_xc_integration():
    """Install the inspected XC hooks in a Dispatcharr web process."""
    from version import __version__ as dispatcharr_version

    if not is_supported_dispatcharr_version(dispatcharr_version):
        return install_xc_hooks(
            None, None, dispatcharr_version=dispatcharr_version, callbacks=XCCallbacks()
        )
    import apps.output.views as output_views
    import apps.timeshift.views as timeshift_views

    return install_xc_hooks(
        output_views,
        timeshift_views,
        dispatcharr_version=dispatcharr_version,
        callbacks=_make_callbacks(output_views, timeshift_views),
    )


def uninstall_xc_integration():
    """Restore Dispatcharr's original functions during plugin shutdown."""
    import apps.output.views as output_views
    import apps.timeshift.views as timeshift_views

    return uninstall_xc_hooks(output_views, timeshift_views)


def active_ts_session_count(archive_root: Path | str, user_id: str | int) -> int:
    """Count active TS sessions and in-flight admissions for HLS limits."""
    return _active_session_count(archive_root, user_id, table_prefix="ts")


def _active_hls_session_count(archive_root: Path | str, user_id: str | int) -> int:
    _ensure_http_session_schema_for_count(archive_root)
    return _active_session_count(archive_root, user_id, table_prefix="http")


def _ensure_http_session_schema_for_count(archive_root: Path | str) -> None:
    database = Path(archive_root) / "archive.sqlite3"
    with closing(sqlite3.connect(database, timeout=5)) as db:
        row = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            ("http_playback_sessions",),
        ).fetchone()
        if row is None:
            return
        columns = {
            column[1]
            for column in db.execute("PRAGMA table_info(http_playback_sessions)")
        }
    if "grace_until" in columns:
        return
    from .http import ensure_http_playback_sessions

    ensure_http_playback_sessions(_archive_store(Path(archive_root)))


def _active_session_count(
    archive_root: Path | str,
    user_id: str | int,
    *,
    table_prefix: str,
    exclude_device_key: str | None = None,
) -> int:
    database = Path(archive_root) / "archive.sqlite3"
    now = time.time()
    total = 0
    with closing(sqlite3.connect(database, timeout=5)) as db:
        db.execute("PRAGMA busy_timeout=5000")
        tables = {
            row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if table_prefix == "http":
            if "http_playback_sessions" not in tables:
                return 0
            total += int(db.execute(
                "SELECT COUNT(*) FROM http_playback_sessions WHERE user_id=? "
                "AND expires_at>? AND grace_until IS NULL",
                (str(user_id), now),
            ).fetchone()[0])
        else:
            ts_tables = {
                "ts_playback_sessions", "ts_playback_streams", "ts_playback_admissions"
            }
            present = tables.intersection(ts_tables)
            if not present:
                return 0
            if present != ts_tables:
                raise sqlite3.OperationalError("incomplete TS playback session schema")
            sql = (
                "SELECT COUNT(*) FROM ts_playback_sessions s WHERE s.user_id=? "
                "AND s.expires_at>? AND (s.active=1 OR EXISTS ("
                "SELECT 1 FROM ts_playback_streams st WHERE st.lease_id=s.lease_id "
                "AND st.expires_at>?))"
            )
            params: tuple = (str(user_id), now, now)
            if exclude_device_key is not None:
                sql += " AND s.device_key<>?"
                params += (exclude_device_key,)
            total += int(db.execute(sql, params).fetchone()[0])
        if table_prefix == "ts":
            sql = "SELECT COUNT(*) FROM ts_playback_admissions WHERE user_id=? AND expires_at>?"
            params = (str(user_id), now)
            if exclude_device_key is not None:
                sql += " AND device_key<>?"
                params += (exclude_device_key,)
            total += int(db.execute(sql, params).fetchone()[0])
    return total


def _make_callbacks(output_views, timeshift_views) -> XCCallbacks:
    def channel_archive_days(channel):
        config = _load_config()
        channel_uuid = str(getattr(channel, "uuid", ""))
        if (
            config is None
            or channel_uuid not in config.channel_uuids
            or not _has_recent_archive(config, channel_uuid)
        ):
            return 0
        return _retention_days(config)

    def channel_uuid_for_epg_id(channel_id, user):
        channel = _authorized_channel(timeshift_views, channel_id, user)
        if channel is None:
            return None
        config = _load_config()
        channel_uuid = str(channel.uuid)
        return channel_uuid if config and channel_uuid in config.channel_uuids else None

    def epg_archive_days(channel_uuid, user):
        config = _load_config()
        if config is None or str(channel_uuid) not in config.channel_uuids:
            return 0
        if not _catchup_enabled(user):
            return 0
        return _retention_days(config)

    def epg_snapshots(channel_uuid, user, days):
        config = _load_config()
        if config is None or str(channel_uuid) not in config.channel_uuids:
            return []
        store = _archive_store(config.archive_root)
        now = datetime.now(timezone.utc)
        snapshots = store.program_snapshots(
            str(channel_uuid), now - timedelta(days=max(0, int(days))), now
        )
        try:
            from apps.channels.models import Channel

            channel_id = str(Channel.objects.filter(uuid=channel_uuid).values_list("id", flat=True).first() or "")
        except Exception:
            channel_id = ""
        result = []
        for item in snapshots:
            start = item["start_utc"].astimezone(timezone.utc)
            end = item["end_utc"].astimezone(timezone.utc)
            payload = item.get("payload") or {}
            title = item.get("title") or ""
            description = payload.get("description") or ""
            identity = f"{channel_uuid}\0{start.timestamp()}\0{end.timestamp()}\0{title}"
            result.append({
                "id": hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16],
                "epg_id": str(payload.get("epg_id") or "0"),
                "title": base64.b64encode(title.encode("utf-8")).decode("ascii"),
                "description": base64.b64encode(description.encode("utf-8")).decode("ascii"),
                "start": start.strftime("%Y-%m-%d %H:%M:%S"),
                "end": end.strftime("%Y-%m-%d %H:%M:%S"),
                "start_timestamp": str(int(start.timestamp())),
                "stop_timestamp": str(int(end.timestamp())),
                "channel_id": channel_id,
                "stream_id": channel_id,
                "has_archive": 0,
                "now_playing": int(start <= now <= end),
            })
        return result

    def program_available(channel_uuid, start_text, end_text, user):
        config = _load_config()
        if config is None or str(channel_uuid) not in config.channel_uuids or not _catchup_enabled(user):
            return False
        start = _parse_epg_utc(start_text)
        end = _parse_epg_utc(end_text)
        if start is None or end is None or end <= start:
            return False
        store = _archive_store(config.archive_root)
        now = datetime.now(timezone.utc)
        if start <= now < end:
            edge = _latest_edge(store, str(channel_uuid))
            if edge is None or (now - edge).total_seconds() > _CURRENT_EDGE_MAX_AGE_SECONDS:
                return False
            end = min(end, edge)
        if end <= start:
            return False
        return store.coverage(str(channel_uuid), start, end).complete

    def playback_available(channel_uuid, timestamp, duration_hint, user):
        config = _load_config()
        channel_uuid = str(channel_uuid)
        if config is None or channel_uuid not in config.channel_uuids or not _catchup_enabled(user):
            return False
        channel = _channel_by_uuid(channel_uuid)
        if channel is None or not _channel_policy_allows(timeshift_views, user, channel):
            return False
        window = _local_playback_window(
            timeshift_views, channel, timestamp, duration_hint
        )
        if window is None:
            return False
        start, end = window
        now = datetime.now(timezone.utc)
        if start > now:
            return False
        store = _archive_store(config.archive_root)
        if start <= now < end:
            edge = _latest_edge(store, channel_uuid)
            if edge is None or edge <= start or (now - edge).total_seconds() > _CURRENT_EDGE_MAX_AGE_SECONDS:
                return False
            end = min(end, edge)
        return end > start and store.coverage(channel_uuid, start, end).complete

    def local_playback_supported(request, user, channel):
        return _local_xc_request_supported(request, user, channel, timeshift_views)

    def authorize_local_playback(request, user, channel, timestamp, duration_hint):
        if not _local_xc_request_supported(request, user, channel, timeshift_views):
            return False
        if not _catchup_enabled(user):
            return False
        config = _load_config()
        channel_uuid = str(getattr(channel, "uuid", ""))
        if config is None or channel_uuid not in config.channel_uuids:
            return False
        return _channel_policy_allows(timeshift_views, user, channel)

    def serve_local_playback(request, user, channel, timestamp, duration_hint):
        config = _load_config()
        if config is None:
            return _service_unavailable(timeshift_views)
        window = _local_playback_window(
            timeshift_views, channel, timestamp, duration_hint
        )
        if window is None:
            return _service_unavailable(timeshift_views)
        start, end = window

        service = _ts_service(config.archive_root, timeshift_views)
        session_id, device_key = _xc_session_keys(request, user, channel, start)
        value = service.stream_for_user(
            user.id,
            str(channel.uuid),
            start,
            end,
            session_id=session_id,
            device_key=device_key,
            method=str(getattr(request, "method", "GET")),
            range_header=getattr(request, "META", {}).get("HTTP_RANGE"),
            live=start <= datetime.now(timezone.utc) < end,
        )
        if (
            getattr(value, "status", None) in (200, 206)
            and str(getattr(request, "method", "GET")).upper() == "GET"
        ):
            try:
                from .stats import successful_playback

                def heartbeat(first_chunk: bool):
                    return successful_playback(
                        user.id,
                        str(channel.uuid),
                        device_key,
                        heartbeat=not first_chunk,
                        playback_lease_id=getattr(value, "lease_id", None),
                    )
                from .ts_http import StreamingTSHTTPResponse

                if isinstance(value, StreamingTSHTTPResponse):
                    value.body = _PlaybackHeartbeatIterator(value.body, heartbeat)
                elif getattr(value, "body", None):
                    heartbeat(True)
            except Exception:
                pass
        return _to_django_response(value, timeshift_views)

    def authorize_xc_m3u(request, user):
        return _authorized_xc_request(request, user)

    def m3u_channel_archive_days(user):
        config = _load_config()
        if config is None or not _catchup_enabled(user):
            return {}
        try:
            from apps.channels.models import Channel

            store = _archive_store(config.archive_root)
            lookback = datetime.now(timezone.utc) - timedelta(hours=config.retention_hours)
            cutoff = datetime.now(timezone.utc)
            days = _retention_days(config)
            return {
                str(channel_id): days
                for channel_id, channel_uuid in Channel.objects.filter(
                    uuid__in=config.channel_uuids
                ).values_list("id", "uuid")
                if store.segments(str(channel_uuid), lookback, cutoff)
            }
        except Exception:
            logger.exception("Could not resolve selected channels for XC playlist")
            return {}

    return XCCallbacks(
        channel_archive_days=channel_archive_days,
        channel_uuid_for_epg_id=channel_uuid_for_epg_id,
        epg_archive_days=epg_archive_days,
        epg_snapshots=epg_snapshots,
        program_available=program_available,
        playback_available=playback_available,
        local_playback_supported=local_playback_supported,
        authorize_local_playback=authorize_local_playback,
        serve_local_playback=serve_local_playback,
        authorize_xc_m3u=authorize_xc_m3u,
        m3u_channel_archive_days=m3u_channel_archive_days,
    )


def _local_playback_window(timeshift_views, channel, timestamp, duration_hint):
    """Resolve the exact requested local window without provider lag padding."""
    parser = getattr(timeshift_views, "parse_catchup_timestamp", None)
    if parser is None:
        return None
    try:
        start = parser(timestamp)
    except Exception:
        return None
    if start is None:
        return None
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    else:
        start = start.astimezone(timezone.utc)

    maximum_value = getattr(timeshift_views, "MAX_DURATION_MINUTES", None)
    if maximum_value is None:
        maximum_value = _timeshift_helper("MAX_DURATION_MINUTES", 480)
    maximum = _positive_duration_constant(maximum_value, 480)
    minutes = _client_duration_minutes(duration_hint, maximum)
    if minutes is not None:
        return start, start + timedelta(minutes=minutes)

    # For URL shapes without a usable duration, use the guide's actual end.
    # Provider playback keeps using Dispatcharr's padded resolver unchanged.
    programme_info = getattr(timeshift_views, "get_programme_info", None)
    if programme_info is None:
        programme_info = _timeshift_helper("get_programme_info")
    if programme_info is not None:
        try:
            info = programme_info(channel, timestamp)
        except Exception:
            info = None
        end = _programme_end_utc(info)
        if end is not None and end > start:
            return start, min(end, start + timedelta(minutes=maximum))
    return None


def _timeshift_helper(name, default=None):
    try:
        from importlib import import_module

        helpers = import_module("apps.timeshift.helpers")
    except Exception:
        return default
    return getattr(helpers, name, default)


def _client_duration_minutes(value, maximum):
    if value is None:
        return None
    try:
        minutes = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    if minutes <= 0 or minutes > maximum:
        return None
    return minutes


def _positive_duration_constant(value, fallback):
    try:
        minutes = int(value)
    except (TypeError, ValueError):
        return fallback
    return minutes if minutes > 0 else fallback


def _programme_end_utc(info):
    if not isinstance(info, dict):
        return None
    end = _epg_datetime_utc(info.get("end_time"))
    if end is not None:
        return end
    programme_start = _epg_datetime_utc(info.get("start_time"))
    try:
        duration_seconds = float(info.get("duration_secs"))
    except (TypeError, ValueError):
        return None
    if programme_start is None or duration_seconds <= 0:
        return None
    return programme_start + timedelta(seconds=duration_seconds)


def _epg_datetime_utc(value):
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _load_config():
    from .runtime import load_config

    return load_config()


def _retention_days(config) -> int:
    return max(1, min(365, math.ceil(config.retention_hours / 24)))


def _has_recent_archive(config, channel_uuid: str) -> bool:
    try:
        store = _archive_store(config.archive_root)
        cutoff = datetime.now(timezone.utc)
        lookback = cutoff - timedelta(hours=config.retention_hours)
        return bool(store.segments(str(channel_uuid), lookback, cutoff))
    except Exception:
        logger.exception("Could not verify local archive availability for XC channel")
        return False


def _archive_store(archive_root: Path):
    from .engine.store import ArchiveStore

    return ArchiveStore(archive_root)


def _channel_by_uuid(channel_uuid):
    try:
        from apps.channels.models import Channel

        return Channel.objects.filter(uuid=channel_uuid).first()
    except Exception:
        return None


def _authorized_channel(timeshift_views, channel_id, user):
    try:
        from apps.channels.models import Channel

        channel = Channel.objects.filter(id=int(channel_id)).first()
    except (TypeError, ValueError):
        return None
    except Exception:
        return None
    return channel if channel and _channel_policy_allows(timeshift_views, user, channel) else None


def _channel_policy_allows(timeshift_views, user, channel) -> bool:
    function = getattr(timeshift_views, "_user_can_access_channel", None)
    if function is None:
        return False
    try:
        return bool(function(user, channel))
    except Exception:
        return False


def _catchup_enabled(user) -> bool:
    try:
        from apps.channels.utils import is_catchup_enabled

        return bool(is_catchup_enabled(user=user))
    except Exception:
        return False


def _authorized_xc_request(request, user) -> bool:
    if user is None or not getattr(user, "is_active", True):
        return False
    query = getattr(request, "GET", {})
    username = str(query.get("username") or "")
    password = str(query.get("password") or "")
    properties = getattr(user, "custom_properties", None) or {}
    expected = properties.get("xc_password")
    if not username or not password or username != str(getattr(user, "username", "")) or not expected:
        return False
    if not hmac.compare_digest(str(expected), password):
        return False
    return _xc_network_allowed(request, user)


def _xc_network_allowed(request, user) -> bool:
    try:
        from dispatcharr.utils import network_access_allowed

        return bool(network_access_allowed(request, "XC_API", user))
    except Exception:
        return False


def _local_xc_request_supported(request, user, channel, timeshift_views) -> bool:
    if _authorized_xc_request(request, user):
        return _request_matches_channel(request, channel)
    if user is None or not getattr(user, "is_active", True):
        return False
    if not _catchup_enabled(user) or not _xc_network_allowed(request, user):
        return False
    if not _channel_policy_allows(timeshift_views, user, channel):
        return False
    path = str(getattr(request, "path_info", None) or getattr(request, "path", ""))
    try:
        path_parts = [part for part in path.split("/") if part]
        timeshift_index = path_parts.index("timeshift")
        path_channel = path_parts[-1]
    except (ValueError, IndexError):
        return False
    if len(path_parts) != timeshift_index + 6:
        return False
    username = unquote(path_parts[timeshift_index + 1])
    password = unquote(path_parts[timeshift_index + 2])
    expected = (getattr(user, "custom_properties", None) or {}).get("xc_password")
    if (
        username != str(getattr(user, "username", ""))
        or not expected
        or not hmac.compare_digest(str(expected), password)
    ):
        return False
    if path_channel.endswith(".ts"):
        path_channel = path_channel[:-3]
    try:
        return int(path_channel) == int(channel.id)
    except (TypeError, ValueError, AttributeError):
        return False


def _request_matches_channel(request, channel) -> bool:
    value = str(getattr(request, "GET", {}).get("stream") or "")
    if value.endswith(".ts"):
        value = value[:-3]
    try:
        return int(value) == int(channel.id)
    except (TypeError, ValueError, AttributeError):
        return False


def _parse_epg_utc(value):
    try:
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _latest_edge(store, channel_uuid):
    try:
        value = store.channel_stats(channel_uuid).get("latest_end_utc")
        return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None
    except Exception:
        return None


def _xc_session_keys(request, user, channel, start):
    query = getattr(request, "GET", {})
    provided = str(query.get("session_id") or "")
    user_agent = str(getattr(request, "META", {}).get("HTTP_USER_AGENT", ""))
    username = str(query.get("username") or "")
    password = str(query.get("password") or "")
    fingerprint = hashlib.sha256(
        f"{user.id}\0{username}\0{password}\0{user_agent}".encode("utf-8")
    ).hexdigest()
    start_epoch = int(start.timestamp())
    channel_identity = str(getattr(channel, "uuid", getattr(channel, "id", "")))
    session_material = f"xc-session\0{user.id}\0{fingerprint}\0{provided}\0{channel_identity}\0{start_epoch}"
    request_identity = hashlib.sha256(session_material.encode("utf-8")).hexdigest()
    session_id = "xc-" + request_identity
    device_key = hashlib.sha256(f"xc-device\0{user.id}\0{fingerprint}".encode("ascii")).hexdigest()
    return session_id, device_key


def _ts_service(archive_root: Path, timeshift_views):
    root = str(Path(archive_root).resolve())
    with _TS_SERVICES_LOCK:
        service = _TS_SERVICES.get(root)
        if service is not None:
            return service

        from .ts_http import ArchiveTSPlaybackService

        store = _archive_store(Path(root))

        def authorize(subject, channel_uuid):
            config = _load_config()
            if config is None or str(channel_uuid) not in config.channel_uuids:
                return False
            try:
                from apps.accounts.models import User

                user = User.objects.filter(id=int(subject), is_active=True).first()
            except Exception:
                return False
            channel = _channel_by_uuid(str(channel_uuid))
            return bool(user and channel and _channel_policy_allows(timeshift_views, user, channel))

        def catchup(subject, channel_uuid):
            if not _catchup_enabled_for_user_id(subject):
                return False
            config = _load_config()
            return bool(config and str(channel_uuid) in config.channel_uuids)

        def allow_new_session(subject, channel_uuid, plugin_sessions, device_key):
            config = _load_config()
            if config is None or str(channel_uuid) not in config.channel_uuids:
                return False
            try:
                from apps.accounts.models import User

                user = User.objects.filter(id=int(subject), is_active=True).first()
            except Exception:
                return False
            if user is None or not _catchup_enabled(user):
                return False
            hls_sessions = _active_hls_session_count(config.archive_root, subject)
            return _dispatcharr_limit_allows(user, plugin_sessions + hls_sessions)

        service = ArchiveTSPlaybackService(
            store,
            authorize_user_channel=authorize,
            catchup_enabled=catchup,
            allow_new_session=allow_new_session,
        )
        _TS_SERVICES[root] = service
        return service


def _catchup_enabled_for_user_id(user_id) -> bool:
    try:
        from apps.accounts.models import User

        user = User.objects.filter(id=int(user_id), is_active=True).first()
        return bool(user and _catchup_enabled(user))
    except Exception:
        return False


def _dispatcharr_limit_allows(user, plugin_sessions: int) -> bool:
    limit = int(getattr(user, "stream_limit", 0) or 0)
    if limit <= 0:
        return True
    try:
        from apps.proxy.utils import get_user_active_connections
        from core.utils import RedisClient

        from .views import _session_limit_allows

        redis = RedisClient.get_client()
        return _session_limit_allows(
            limit,
            plugin_sessions,
            redis,
            lambda: get_user_active_connections(user.id),
        )
    except Exception:
        logger.exception("Unable to verify combined Dispatcharr playback limits")
        return False


def _service_unavailable(timeshift_views):
    response_type = getattr(timeshift_views, "HttpResponse", None)
    if response_type is not None:
        return finalize_dispatcharr_response(
            timeshift_views,
            response_type("Local archive temporarily unavailable", status=503),
        )
    return None


def _to_django_response(value, timeshift_views):
    if value is None:
        return _service_unavailable(timeshift_views)
    if isinstance(value, (bytes, bytearray)):
        response = timeshift_views.HttpResponse(bytes(value), status=200)
        return finalize_dispatcharr_response(timeshift_views, response)
    from .ts_http import StreamingTSHTTPResponse

    if isinstance(value, StreamingTSHTTPResponse):
        from django.http import StreamingHttpResponse

        response = StreamingHttpResponse(value, status=value.status)
    else:
        response = timeshift_views.HttpResponse(value.body, status=value.status)
    for name, header in value.headers.items():
        response[name] = header
    return finalize_dispatcharr_response(timeshift_views, response)
