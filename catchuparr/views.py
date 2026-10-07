"""In-process, token-authenticated M3U/XMLTV and archive routes."""

from __future__ import annotations

import copy
import logging
import math
import os
import re
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
ARCHIVE_FINALIZATION_GRACE_SECONDS = 120
_ROUTE_NAMES = frozenset(
    {"catchuparr-m3u", "catchuparr-xmltv", "catchuparr-archive", "catchuparr-segment"}
)


def _session_limit_allows(stream_limit, plugin_sessions, redis_client, active_connections):
    """Fail closed if Dispatcharr's active connection count cannot be verified."""
    if stream_limit <= 0:
        return True
    try:
        redis_client.ping()
        dispatcharr_sessions = len(active_connections())
        redis_client.ping()
    except Exception:
        logger.exception("Unable to verify active connections; denying archive playback")
        return False
    return dispatcharr_sessions + plugin_sessions < stream_limit


def install_routes() -> None:
    """Install routes before Dispatcharr's broad XC and React catch-all paths."""
    import dispatcharr.urls as root_urls
    from django.urls import clear_url_caches, path

    if any(getattr(route, "name", None) == "catchuparr-m3u" for route in root_urls.urlpatterns):
        return
    new_routes = [
        path("catchuparr/m3u", m3u_view, name="catchuparr-m3u"),
        path("catchuparr/xmltv", xmltv_view, name="catchuparr-xmltv"),
        path("catchuparr/archive", archive_view, name="catchuparr-archive"),
        path(
            "catchuparr/segment/<str:channel_id>/<str:segment_id>",
            segment_view,
            name="catchuparr-segment",
        ),
    ]
    root_urls.urlpatterns[0:0] = new_routes
    clear_url_caches()


def uninstall_routes() -> None:
    import dispatcharr.urls as root_urls
    from django.urls import clear_url_caches

    root_urls.urlpatterns[:] = [
        route for route in root_urls.urlpatterns if getattr(route, "name", None) not in _ROUTE_NAMES
    ]
    clear_url_caches()


def _authenticate(request, *, playback: bool = False):
    """Return (user, config, token) or (None, None, None), without logging tokens."""
    from apps.accounts.models import User
    from dispatcharr.utils import network_access_allowed

    from .runtime import load_config, require_supported_version
    from .security import AccessTokenStore

    try:
        require_supported_version()
        config = load_config()
        if config is None:
            return None, None, None
        token = _access_token(request)
        if not token:
            return None, None, None
        user_id = AccessTokenStore(config.archive_root).lookup(token)
        if user_id is None:
            return None, None, None
        user = User.objects.filter(id=user_id, is_active=True).first()
        if user is None or not _network_allowed(
            request, user, network_access_allowed, playback=playback
        ):
            return None, None, None
        return user, config, token
    except Exception:
        logger.exception("Catchuparr authentication failed")
        return None, None, None


def _access_token(request) -> str:
    """Allow private HTTP clients to keep bearer values out of request URLs."""
    header = getattr(request, "headers", {}).get("X-Catchuparr-Token", "")
    return header or request.GET.get("access_token") or request.GET.get("token", "")


def _network_allowed(request, user, checker, *, playback: bool) -> bool:
    """Archive playback must pass both playlist and stream network policies."""
    return bool(
        checker(request, "M3U_EPG", user)
        and (not playback or checker(request, "STREAMS", user))
    )


def _denied():
    from django.http import HttpResponse

    return HttpResponse("Unauthorized", status=401)


def _no_cache(response):
    response["Cache-Control"] = "private, no-store"
    response["Referrer-Policy"] = "no-referrer"
    return response


def _trace_component(value: str) -> str:
    return value if re.fullmatch(r"[0-9a-fA-F-]{1,64}", value) else "invalid"


def _trace_range(value: str | None) -> str:
    if value is None:
        return "none"
    if len(value) <= 128 and re.fullmatch(
        r"bytes=(?:\d{0,20}-\d{0,20})(?:,\d{0,20}-\d{0,20}){0,4}", value
    ):
        return value
    return "other"


def _trace_archive_request(request, channel: str, start: float, duration: int, status: int):
    if os.environ.get("CATCHUPARR_TRACE_REQUESTS") == "1":
        logger.info(
            "Catchuparr request route=archive method=%s channel=%s utc=%.3f duration=%d status=%d",
            request.method, _trace_component(channel), start, duration, status,
        )


def _trace_segment_request(request, channel: str, segment: str, status: int):
    if os.environ.get("CATCHUPARR_TRACE_REQUESTS") == "1":
        logger.info(
            "Catchuparr request route=segment method=%s channel=%s segment=%s range=%s status=%d",
            request.method, _trace_component(channel), _trace_component(segment),
            _trace_range(request.headers.get("Range")), status,
        )


def m3u_view(request):
    from apps.channels.utils import is_catchup_enabled
    from apps.output.views import generate_m3u
    from core.utils import build_absolute_uri_with_port
    from django.http import HttpResponse

    from .adapters.m3u import annotate_m3u
    from .engine.store import ArchiveStore

    if request.method not in {"GET", "HEAD"}:
        return HttpResponse(status=405)
    user, config, token = _authenticate(request)
    if user is None:
        return _denied()
    if request.method == "HEAD":
        return _no_cache(HttpResponse(content_type="audio/x-mpegurl"))

    response = generate_m3u(_core_request(request), user=user)
    if response.status_code != 200:
        return response
    playlist = response.content.decode("utf-8")
    xmltv_url = build_absolute_uri_with_port(request, "/catchuparr/xmltv")
    xmltv_url += "?access_token=" + _url_token(token)
    lines = playlist.splitlines(keepends=True)
    if lines and lines[0].startswith("#EXTM3U"):
        lines[0] = f'#EXTM3U x-tvg-url="{xmltv_url}" url-tvg="{xmltv_url}"\n'
    playlist = "".join(lines)

    if is_catchup_enabled(user=user):
        store = ArchiveStore(config.archive_root)
        channel_map = {
            channel: channel
            for channel in config.channel_uuids
            if store.segments(channel)
        }
        endpoint = build_absolute_uri_with_port(request, "/catchuparr/archive")
        playlist = annotate_m3u(
            playlist, channel_map, endpoint, token,
            catchup_days=max(1, math.ceil(config.retention_hours / 24)),
        )
    result = HttpResponse(playlist, content_type="audio/x-mpegurl")
    result["Content-Disposition"] = 'attachment; filename="catchuparr.m3u"'
    return _no_cache(result)


def xmltv_view(request):
    from apps.channels.utils import is_catchup_enabled
    from apps.output.epg import generate_epg
    from django.http import HttpResponse

    from .adapters.m3u import MAX_XMLTV_BYTES, filter_xmltv, merge_xmltv_snapshots
    from .engine.store import ArchiveStore

    if request.method not in {"GET", "HEAD"}:
        return HttpResponse(status=405)
    user, config, _token = _authenticate(request)
    if user is None:
        return _denied()
    if request.method == "HEAD":
        return _no_cache(HttpResponse(content_type="application/xml"))

    request_copy = _epg_request(request, config.retention_hours)
    response = generate_epg(request_copy, user=user)
    if response.status_code != 200:
        return response
    content = bytearray()
    for part in response.streaming_content:
        content.extend(part.encode("utf-8") if isinstance(part, str) else part)
        if len(content) > MAX_XMLTV_BYTES:
            return _no_cache(HttpResponse("Guide too large", status=413))

    store = ArchiveStore(config.archive_root)
    channel_map = (
        {
            epg_id: channel
            for epg_id, channel in _xmltv_channel_map(request, user, config).items()
            if store.segments(channel)
        }
        if is_catchup_enabled(user=user) else {}
    )
    now = datetime.now(timezone.utc)

    def is_covered(epg_channel_id, start, end):
        archive_channel = channel_map.get(epg_channel_id)
        return bool(archive_channel and store.coverage(archive_channel, start, end).complete)

    filtered = filter_xmltv(
        bytes(content), is_covered, now=now, local_channel_ids=channel_map.keys()
    )
    snapshots = {
        epg_channel: store.program_snapshots(
            archive_channel,
            now.timestamp() - config.retention_hours * 3600,
            now,
        )
        for epg_channel, archive_channel in channel_map.items()
    }
    try:
        merged = merge_xmltv_snapshots(filtered, snapshots, is_covered, now=now)
    except ValueError:
        return _no_cache(HttpResponse("Guide too large", status=413))
    return _no_cache(HttpResponse(merged, content_type="application/xml"))


def _xmltv_channel_map(request, user, config):
    from apps.output.views import generate_m3u

    response = generate_m3u(_core_request(request), user=user)
    if response.status_code != 200:
        return {}
    playlist = response.content.decode("utf-8")
    mapping = {}
    ambiguous = set()
    pending_id = None
    for line in playlist.splitlines():
        if line.startswith("#EXTINF:"):
            found = re.search(r'\btvg-id="([^"]+)"', line)
            pending_id = found.group(1) if found else None
        elif pending_id and "/proxy/ts/stream/" in line:
            channel = line.split("/proxy/ts/stream/", 1)[1].split("?", 1)[0].strip("/")
            if channel in config.channel_uuids and pending_id not in ambiguous:
                if pending_id in mapping and mapping[pending_id] != channel:
                    mapping.pop(pending_id)
                    ambiguous.add(pending_id)
                else:
                    mapping[pending_id] = channel
            pending_id = None
    return mapping


def _epg_request(request, retention_hours: int):
    """Add local history defaults without reducing requested provider history."""
    copied = _core_request(request)
    # Dispatcharr treats days=0 as unbounded future EPG. Keep a default import
    # practical while preserving an explicitly requested provider lookback.
    if "days" not in copied.GET:
        copied.GET["days"] = "2"
    if "prev_days" not in copied.GET:
        copied.GET["prev_days"] = str(min(30, math.ceil(retention_hours / 24)))
    return copied


def _selected_proxy_channels(playlist: str, channel_uuids) -> set[str]:
    """Find authorized archive UUIDs independently of optional/shared EPG IDs."""
    selected = set(channel_uuids)
    return {
        match.group(1)
        for line in playlist.splitlines()
        if not line.startswith("#")
        and (match := re.search(r"/proxy/ts/stream/([^/?#\s]+)", line))
        and match.group(1) in selected
    }


def _url_token(token: str) -> str:
    from urllib.parse import quote

    return quote(token, safe="")


def _core_request(request):
    """Keep bearer credentials out of Dispatcharr's M3U/EPG cache keys."""
    clean = copy.copy(request)
    clean.GET = request.GET.copy()
    clean.GET.pop("access_token", None)
    clean.GET.pop("token", None)
    clean.GET.pop("lease", None)
    return clean


def archive_view(request):
    """Serve a growing HLS playlist for a covered catch-up time window."""
    from django.http import HttpResponse

    if request.method not in {"GET", "HEAD"}:
        return HttpResponse(status=405)
    user, config, token = _authenticate(request, playback=True)
    if user is None:
        return _denied()
    channel = request.GET.get("channel_id", "")
    start = request.GET.get("utc", "")
    duration = request.GET.get("duration", "")
    try:
        start_epoch = _catchup_epoch(start)
        duration_seconds = int(duration)
    except (TypeError, ValueError, OverflowError):
        return _no_cache(HttpResponse("Invalid catch-up time", status=400))
    if not 0 < duration_seconds <= 24 * 60 * 60:
        return _no_cache(HttpResponse("Invalid catch-up duration", status=400))
    now_epoch = datetime.now(timezone.utc).timestamp()
    if start_epoch > now_epoch or start_epoch + duration_seconds < now_epoch - config.retention_hours * 3600:
        return _no_cache(HttpResponse("No archived programme", status=404))
    service = _archive_service(request, user, config)
    end_epoch = start_epoch + duration_seconds
    response = service.playlist(
        token, channel, start_epoch, end_epoch,
        live=_archive_window_live(service, str(user.id), channel, end_epoch, now_epoch),
    )
    _trace_archive_request(request, channel, start_epoch, duration_seconds, response.status)
    return _to_django_response(response, request.method)


def _archive_window_live(service, user_id, channel_id, end_epoch, now_epoch):
    """Wait briefly for a final indexed segment after the EPG boundary."""
    from .engine.store import TIMELINE_GAP_TOLERANCE_SECONDS

    if end_epoch > now_epoch:
        return True
    if now_epoch >= end_epoch + ARCHIVE_FINALIZATION_GRACE_SECONDS:
        return False
    if not (
        service.authorize_user_channel(user_id, channel_id)
        and service.catchup_enabled(user_id, channel_id)
    ):
        return False
    tail = service.store.segments(
        channel_id, end_epoch - ARCHIVE_FINALIZATION_GRACE_SECONDS, end_epoch
    )
    return not tail or max(segment.end_utc.timestamp() for segment in tail) < (
        end_epoch - TIMELINE_GAP_TOLERANCE_SECONDS
    )


def segment_view(request, channel_id: str, segment_id: str):
    """Serve one immutable transport-stream segment, including byte ranges."""
    from django.http import HttpResponse

    if request.method not in {"GET", "HEAD"}:
        return HttpResponse(status=405)
    user, config, token = _authenticate(request, playback=True)
    if user is None:
        return _denied()
    service = _archive_service(request, user, config)
    response = service.segment(
        token, channel_id, segment_id, request.GET.get("lease"),
        method=request.method, range_header=request.headers.get("Range"),
    )
    _trace_segment_request(request, channel_id, segment_id, response.status)
    return _to_django_response(response, request.method)


def _catchup_epoch(value: str) -> float:
    """Accept TiviMate's UTC epoch placeholder and unambiguous ISO timestamps."""
    try:
        epoch = float(value)
    except ValueError:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("catch-up timestamps must carry a timezone")
        epoch = parsed.timestamp()
    if not math.isfinite(epoch):
        raise ValueError("catch-up timestamp must be finite")
    return epoch


def _archive_service(request, user, config):
    from apps.channels.utils import is_catchup_enabled
    from apps.output.views import generate_m3u
    from apps.proxy.utils import get_user_active_connections
    from core.utils import RedisClient

    from .engine.store import ArchiveStore
    from .http import ArchiveHTTPService
    from .security import AccessTokenStore

    response = generate_m3u(_core_request(request), user=user)
    allowed = (
        _selected_proxy_channels(response.content.decode("utf-8"), config.channel_uuids)
        if response.status_code == 200 else set()
    )
    user_id = str(user.id)
    catchup_allowed = bool(is_catchup_enabled(user=user))

    def allow_new_session(subject, channel, plugin_sessions):
        if subject != user_id or channel not in allowed:
            return False
        limit = int(getattr(user, "stream_limit", 0) or 0)
        if limit <= 0:
            return True
        try:
            redis = RedisClient.get_client()
        except Exception:
            logger.exception("Unable to verify Redis availability; denying archive playback")
            return False
        return _session_limit_allows(
            limit, plugin_sessions, redis,
            lambda: get_user_active_connections(user.id),
        )

    return ArchiveHTTPService(
        ArchiveStore(config.archive_root),
        AccessTokenStore(config.archive_root),
        authorize_user_channel=lambda subject, channel: (
            subject == user_id and channel in allowed
        ),
        catchup_enabled=lambda subject, channel: (
            catchup_allowed and subject == user_id and channel in config.channel_uuids
        ),
        allow_new_session=allow_new_session,
    )


def _to_django_response(value, method: str):
    from django.http import HttpResponse

    result = HttpResponse(
        value.body if method != "HEAD" else b"",
        status=value.status,
    )
    for key, header_value in value.headers.items():
        result[key] = header_value
    return _no_cache(result)
