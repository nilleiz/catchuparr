"""In-process, token-authenticated M3U/XMLTV and archive routes."""

from __future__ import annotations

import copy
import logging
import math
import re
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
_ROUTE_NAMES = frozenset(
    {"catchuparr-m3u", "catchuparr-xmltv", "catchuparr-archive", "catchuparr-segment"}
)


def install_routes() -> None:
    """Install routes before Dispatcharr's broad XC and React catch-all paths."""
    from django.urls import clear_url_caches, path

    import dispatcharr.urls as root_urls

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
    from django.urls import clear_url_caches

    import dispatcharr.urls as root_urls

    root_urls.urlpatterns[:] = [
        route for route in root_urls.urlpatterns if getattr(route, "name", None) not in _ROUTE_NAMES
    ]
    clear_url_caches()


def _authenticate(request):
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
        token = request.GET.get("access_token") or request.GET.get("token", "")
        if not token:
            return None, None, None
        user_id = AccessTokenStore(config.archive_root).lookup(token)
        if user_id is None:
            return None, None, None
        user = User.objects.filter(id=user_id, is_active=True).first()
        if user is None or not network_access_allowed(request, "M3U_EPG", user):
            return None, None, None
        return user, config, token
    except Exception:
        logger.exception("Catchuparr authentication failed")
        return None, None, None


def _denied():
    from django.http import HttpResponse

    return HttpResponse("Unauthorized", status=401)


def _no_cache(response):
    response["Cache-Control"] = "private, no-store"
    response["Referrer-Policy"] = "no-referrer"
    return response


def m3u_view(request):
    from django.http import HttpResponse

    from apps.channels.utils import is_catchup_enabled
    from apps.output.views import generate_m3u
    from core.utils import build_absolute_uri_with_port

    from .adapters.m3u import annotate_m3u
    from .engine.store import ArchiveStore

    if request.method not in {"GET", "HEAD"}:
        return HttpResponse(status=405)
    user, config, token = _authenticate(request)
    if user is None:
        return _denied()
    if request.method == "HEAD":
        return _no_cache(HttpResponse(content_type="audio/x-mpegurl"))

    response = generate_m3u(request, user=user)
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
    from django.http import HttpResponse

    from apps.output.epg import generate_epg

    from .adapters.m3u import MAX_XMLTV_BYTES, filter_xmltv
    from .engine.store import ArchiveStore

    if request.method not in {"GET", "HEAD"}:
        return HttpResponse(status=405)
    user, config, _token = _authenticate(request)
    if user is None:
        return _denied()
    if request.method == "HEAD":
        return _no_cache(HttpResponse(content_type="application/xml"))

    request_copy = copy.copy(request)
    request_copy.GET = request.GET.copy()
    request_copy.GET["prev_days"] = str(min(30, math.ceil(config.retention_hours / 24)))
    response = generate_epg(request_copy, user=user)
    if response.status_code != 200:
        return response
    content = bytearray()
    for part in response.streaming_content:
        content.extend(part.encode("utf-8") if isinstance(part, str) else part)
        if len(content) > MAX_XMLTV_BYTES:
            return _no_cache(HttpResponse("Guide too large", status=413))

    store = ArchiveStore(config.archive_root)
    channel_map = _xmltv_channel_map(request, user, config)
    now = datetime.now(timezone.utc)

    def is_covered(epg_channel_id, start, end):
        archive_channel = channel_map.get(epg_channel_id)
        return bool(archive_channel and store.coverage(archive_channel, start, end).complete)

    filtered = filter_xmltv(bytes(content), is_covered, now=now)
    return _no_cache(HttpResponse(filtered, content_type="application/xml"))


def _xmltv_channel_map(request, user, config):
    from apps.output.views import generate_m3u

    playlist = generate_m3u(request, user=user).content.decode("utf-8")
    mapping = {}
    pending_id = None
    for line in playlist.splitlines():
        if line.startswith("#EXTINF:"):
            found = re.search(r'\btvg-id="([^"]+)"', line)
            pending_id = found.group(1) if found else None
        elif pending_id and "/proxy/ts/stream/" in line:
            channel = line.split("/proxy/ts/stream/", 1)[1].split("?", 1)[0].strip("/")
            if channel in config.channel_uuids:
                mapping[pending_id] = channel
            pending_id = None
    return mapping


def _url_token(token: str) -> str:
    from urllib.parse import quote

    return quote(token, safe="")


def archive_view(request):
    """Serve a growing HLS playlist for a covered catch-up time window."""
    from django.http import HttpResponse

    if request.method not in {"GET", "HEAD"}:
        return HttpResponse(status=405)
    user, config, token = _authenticate(request)
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
    response = service.playlist(
        token, channel, start_epoch, start_epoch + duration_seconds,
        live=start_epoch + duration_seconds > now_epoch,
    )
    return _to_django_response(response, request.method)


def segment_view(request, channel_id: str, segment_id: str):
    """Serve one immutable transport-stream segment, including byte ranges."""
    from django.http import HttpResponse

    if request.method not in {"GET", "HEAD"}:
        return HttpResponse(status=405)
    user, config, token = _authenticate(request)
    if user is None:
        return _denied()
    service = _archive_service(request, user, config)
    response = service.segment(
        token, channel_id, segment_id, request.GET.get("lease"),
        method=request.method, range_header=request.headers.get("Range"),
    )
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

    from .engine.store import ArchiveStore
    from .http import ArchiveHTTPService
    from .security import AccessTokenStore

    allowed = set(_xmltv_channel_map(request, user, config).values())
    user_id = str(user.id)
    catchup_allowed = bool(is_catchup_enabled(user=user))
    return ArchiveHTTPService(
        ArchiveStore(config.archive_root),
        AccessTokenStore(config.archive_root),
        authorize_user_channel=lambda subject, channel: (
            subject == user_id and channel in allowed
        ),
        catchup_enabled=lambda subject, channel: (
            catchup_allowed and subject == user_id and channel in config.channel_uuids
        ),
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
