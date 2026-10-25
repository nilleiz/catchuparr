"""Optional archive augmentation and capability routes for native output."""

from __future__ import annotations

import inspect
import logging
import math
import secrets
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)
_HOOK_MARKER = "__catchuparr_native_output_hook_v1__"
_OUTPUT_HOOKS: list[tuple[Any, Any]] = []


def _is_direct_native_m3u(request) -> bool:
    query = getattr(request, "GET", {})
    return str(query.get("direct", "false")).lower() == "true"


@dataclass(frozen=True)
class _NativeContext:
    request: Any
    active: dict[str, Any]
    config: Any
    user: Any
    mode: str
    profile_id: str | None
    renderer_user: Any


def install_output_hooks() -> bool:
    """Wrap only the pinned native output endpoints after their own guards."""
    try:
        import apps.output.urls as output_urls
    except ModuleNotFoundError:
        # Unit tests and unsupported Dispatcharr layouts can install the
        # standalone routes without exposing native output augmentation.
        return False

    routes = {
        getattr(route, "name", None): route
        for route in getattr(output_urls, "urlpatterns", ())
    }
    expected = {"m3u_endpoint", "epg_endpoint"}
    if not expected.issubset(routes):
        return False
    for name in expected:
        callback = getattr(routes[name], "callback", None)
        if callback is None:
            return False
        if getattr(callback, _HOOK_MARKER, False):
            callback = getattr(callback, "__catchuparr_original__", None)
        try:
            parameters = tuple(inspect.signature(callback).parameters)
        except (TypeError, ValueError):
            return False
        if parameters != ("request", "profile_name", "user"):
            return False
    installed = []
    for name in sorted(expected):
        route = routes[name]
        original = getattr(route, "callback", None)
        if original is None:
            continue
        if getattr(original, _HOOK_MARKER, False):
            continue

        def wrapped(request, *args, __name=name, __original=original, **kwargs):
            response = __original(request, *args, **kwargs)
            if str(getattr(request, "method", "GET")).upper() != "GET":
                return response
            profile_name = kwargs.get("profile_name")
            if profile_name is None and args:
                profile_name = args[0]
            try:
                renderer_user = kwargs.get("user")
                if renderer_user is None and len(args) > 1:
                    renderer_user = args[1]
                if __name == "m3u_endpoint" and _is_direct_native_m3u(request):
                    return response
                context = _native_export_context(request, profile_name, renderer_user)
                if context is None or getattr(response, "status_code", 500) != 200:
                    return response
                if __name == "m3u_endpoint":
                    return _augment_native_m3u(response, context, profile_name)
                return _augment_native_epg(response, context, profile_name)
            except Exception:
                logger.exception("Native output archive augmentation failed closed")
                return response

        setattr(wrapped, _HOOK_MARKER, True)
        setattr(wrapped, "__catchuparr_original__", original)
        route.callback = wrapped
        installed.append((route, original))
    _OUTPUT_HOOKS.extend(installed)
    return True


def uninstall_output_hooks() -> None:
    for route, original in reversed(_OUTPUT_HOOKS):
        callback = getattr(route, "callback", None)
        if getattr(callback, "__catchuparr_original__", None) is original:
            route.callback = original
    _OUTPUT_HOOKS.clear()


def _native_export_context(
    request, profile_name: str | None, renderer_user=None
) -> _NativeContext | None:
    from apps.accounts.models import User
    from apps.channels.models import ChannelProfile
    from apps.channels.utils import is_catchup_enabled
    from dispatcharr.utils import network_access_allowed

    from .configuration import load_active_configuration
    from .runtime import load_config, require_supported_version

    require_supported_version()
    active = load_active_configuration()
    if not active or not active.get("native_output_catchup"):
        return None
    config = load_config(active_snapshot=active)
    if config is None:
        return None
    if renderer_user is not None:
        if not (
            getattr(renderer_user, "is_authenticated", False)
            and getattr(renderer_user, "is_active", True)
        ):
            return None
        user = renderer_user
        mode = "trusted_native_user"
    else:
        user_id = active.get("playback_user_id")
        if not user_id:
            return None
        user = User.objects.filter(id=user_id, is_active=True).first()
        mode = "anonymous_admission"
    if user is None or not is_catchup_enabled(user=user):
        return None
    if not (
        network_access_allowed(request, "M3U_EPG", user)
        and network_access_allowed(request, "STREAMS", user)
    ):
        return None
    profile_id = None
    if profile_name is not None:
        profile = ChannelProfile.objects.filter(name=str(profile_name)).first()
        if profile is None:
            return None
        profile_id = str(profile.id)
    return _NativeContext(
        request, active, config, user, mode, profile_id, renderer_user
    )


def _native_channel_allowed(context: _NativeContext, channel_id: str, profile_id: str | None) -> bool:
    from apps.channels.managers import with_effective_values
    from apps.channels.models import Channel

    try:
        import uuid

        if str(uuid.UUID(channel_id)) != channel_id:
            return False
    except (ValueError, TypeError, AttributeError):
        return False
    if channel_id not in context.config.channel_uuids:
        return False
    user = context.user
    query = Channel.objects.filter(
        uuid=channel_id,
        user_level__lte=int(getattr(user, "user_level", 0)),
    )
    if profile_id is not None:
        query = query.filter(
            channelprofilemembership__enabled=True,
            channelprofilemembership__channel_profile_id=profile_id,
        )
    level = int(getattr(user, "user_level", 0))
    if level < 10:
        memberships = getattr(user, "channel_profiles", None)
        if memberships is not None and memberships.count() > 0:
            query = query.filter(
                channelprofilemembership__enabled=True,
                channelprofilemembership__channel_profile__in=memberships.all(),
            )
    custom = getattr(user, "custom_properties", None) or {}
    if custom.get("hide_adult_content", False):
        query = query.filter(is_adult=False)
    return (
        with_effective_values(query.distinct(), select_related_fks=True)
        .exclude(hidden_from_output=True)
        .exists()
    )


def _augment_native_m3u(response, context: _NativeContext, profile_name: str | None):
    # Dispatcharr's direct=true playlist can contain both provider URLs and
    # proxy fallbacks. Preserve the complete native direct playlist unchanged;
    # annotating only its proxy rows would still change the user's selected
    # output mode and would mix archive playback metadata into that export.
    if _is_direct_native_m3u(context.request):
        return response

    from apps.output.views import generate_m3u
    from core.utils import build_absolute_uri_with_port

    from .adapters.m3u import annotate_native_m3u
    from .engine.store import ArchiveStore
    from .native_output_security import create_native_capability
    from .views import _core_request, _selected_proxy_channels

    playlist = response.content.decode("utf-8")
    authorized_response = generate_m3u(
        _core_request(context.request),
        profile_name,
        user=context.user,
    )
    if getattr(authorized_response, "status_code", 500) != 200:
        return response
    authorized = _selected_proxy_channels(
        authorized_response.content.decode("utf-8"), context.config.channel_uuids
    )
    authorized = {
        channel for channel in authorized
        if _native_channel_allowed(context, channel, context.profile_id)
    }
    store = ArchiveStore(context.config.archive_root)
    eligible = {channel for channel in authorized if store.segments(channel)}
    if not eligible:
        return response
    key_id = context.active.get("native_output_key_id")
    if not key_id:
        return response
    nonce = secrets.token_hex(24)
    capabilities = {
        channel: create_native_capability(
            _active_settings_path(),
            key_id=key_id,
            mode=context.mode,
            subject=str(context.user.id),
            channel=channel,
            profile=context.profile_id,
            nonce=nonce,
        )
        for channel in eligible
    }
    endpoint = build_absolute_uri_with_port(
        context.request, "/catchuparr/native/archive.m3u8"
    )
    response.content = annotate_native_m3u(
        playlist,
        capabilities,
        endpoint,
        max(1, math.ceil(context.config.retention_hours / 24)),
    )
    response["Cache-Control"] = "private, no-store"
    response["Referrer-Policy"] = "no-referrer"
    return response


def _augment_native_epg(response, context: _NativeContext, profile_name: str | None):
    from apps.output.views import generate_m3u

    from .adapters.m3u import MAX_XMLTV_BYTES, merge_xmltv_snapshots, xmltv_channel_map_from_m3u
    from .engine.store import ArchiveStore
    from .views import _core_request

    m3u_response = generate_m3u(
        _core_request(context.request), profile_name, user=context.renderer_user
    )
    if getattr(m3u_response, "status_code", 500) != 200:
        return response
    playlist = m3u_response.content.decode("utf-8")
    selected = set(context.config.channel_uuids)
    channel_map = xmltv_channel_map_from_m3u(playlist, selected)
    channel_map = {
        xmltv_id: channel
        for xmltv_id, channel in channel_map.items()
        if _native_channel_allowed(context, channel, context.profile_id)
    }
    store = ArchiveStore(context.config.archive_root)
    channel_map = {
        xmltv_id: channel
        for xmltv_id, channel in channel_map.items()
        if store.segments(channel)
    }
    if not channel_map:
        return response
    now = datetime.now(timezone.utc)
    snapshots = {
        xmltv_id: store.program_snapshots(
            channel, now.timestamp() - context.config.retention_hours * 3600, now
        )
        for xmltv_id, channel in channel_map.items()
    }

    def covered(xmltv_id, start, end):
        channel = channel_map.get(xmltv_id)
        return bool(channel and store.coverage(channel, start, end).complete)

    original = iter(response.streaming_content)

    def stream():
        pieces = []
        size = 0
        try:
            for part in original:
                value = part.encode("utf-8") if isinstance(part, str) else bytes(part)
                size += len(value)
                if size > MAX_XMLTV_BYTES:
                    yield from pieces
                    yield value
                    yield from original
                    return
                pieces.append(value)
            raw = b"".join(pieces)
            try:
                merged = merge_xmltv_snapshots(
                    raw.decode("utf-8"), snapshots, covered, now=now
                )
            except (UnicodeError, ValueError, ET.ParseError):
                yield raw
            else:
                yield merged.encode("utf-8")
        finally:
            close = getattr(original, "close", None)
            if close is not None:
                close()

    response.streaming_content = stream()
    response["Cache-Control"] = "private, no-store"
    response["Referrer-Policy"] = "no-referrer"
    return response


def native_archive_view(request):
    return _native_playback_view(request, segment_id=None)


def native_segment_view(request, channel_id: str, segment_id: str):
    return _native_playback_view(request, channel_id=channel_id, segment_id=segment_id)


def _native_playback_view(request, channel_id: str | None = None, segment_id: str | None = None):
    from django.http import HttpResponse

    from .views import _native_archive_response

    try:
        response = _native_archive_response(request, channel_id, segment_id)
    except Exception:
        logger.exception("Native archive playback was denied")
        response = HttpResponse(status=503)
    response["Cache-Control"] = "private, no-store"
    response["Referrer-Policy"] = "no-referrer"
    return response


def _active_settings_path():
    from .configuration import active_settings_path

    return active_settings_path()
