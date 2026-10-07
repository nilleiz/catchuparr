"""Version-guarded XC compatibility hooks for inspected Dispatcharr releases.

Dispatcharr does not expose public hooks for its XC serializers or catch-up
handler. This module wraps only the three inspected functions and refuses to
patch a changed version/signature. The local playback callback is deliberately
separate from Dispatcharr's provider path: it must enforce Dispatcharr-equivalent
stream limits and create/release an archive playback lease.
"""

from __future__ import annotations

import copy
import inspect
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from ..compatibility import (
    SUPPORTED_DISPATCHARR_VERSION as SUPPORTED_DISPATCHARR_VERSION,
)
from ..compatibility import (
    is_supported_dispatcharr_version,
)

_HOOK_MARKER = "__catchuparr_xc_hook__"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class XCCallbacks:
    """Callbacks supplied by the archive engine.

    ``channel_archive_days`` returns the number of locally retained days for a
    channel, or zero. ``program_available`` must return true only when the
    interval passed to it is backed by usable local segments.
    ``channel_uuid_for_epg_id`` resolves the XC integer stream ID after the
    original ACL-gated EPG call. ``epg_archive_days`` returns the history depth
    needed to preserve the channel's provider archive window and include its
    local archive; the adapter also preserves larger URL and user lookback
    settings.
    ``epg_snapshots`` returns missing XC-shaped historical listings from the
    plugin's EPG snapshots.
    ``playback_available`` resolves the request timestamp and client duration
    against the archive index and must return true only for a covered interval.

    ``authorize_local_playback`` must enforce user catch-up permission, channel
    access, Dispatcharr connection limits and per-playback session/lease rules.
    Return ``True`` to allow, ``False`` to deny, or an HTTP response to return
    directly. ``serve_local_playback`` then returns the archive HTTP response.
    Both callbacks are required before local playback is enabled.
    """

    channel_archive_days: Callable[[Any], int] | None = None
    channel_uuid_for_epg_id: Callable[[str, Any], str | None] | None = None
    epg_archive_days: Callable[[str, Any], int] | None = None
    epg_snapshots: Callable[[str, Any, int], list[dict[str, Any]]] | None = None
    program_available: Callable[[Any, str, str, Any], bool] | None = None
    playback_available: Callable[[Any, str, Any, Any], bool] | None = None
    authorize_local_playback: Callable[[Any, Any, Any, str, Any], Any] | None = None
    serve_local_playback: Callable[[Any, Any, Any, str, Any], Any] | None = None


@dataclass(frozen=True)
class HookInstallResult:
    installed: bool
    reason: str
    hooks: tuple[str, ...] = ()


def _signature_matches(function: Callable[..., Any], expected: tuple[str, ...]) -> bool:
    try:
        signature = inspect.signature(function)
    except (TypeError, ValueError):
        return False
    parameters = tuple(signature.parameters.values())
    if tuple(parameter.name for parameter in parameters) != expected:
        return False
    if any(parameter.kind not in (inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                  inspect.Parameter.KEYWORD_ONLY)
           for parameter in parameters):
        return False
    return True


def _is_response(value: Any) -> bool:
    return hasattr(value, "status_code") and hasattr(value, "__getitem__")


def install_xc_hooks(
    output_views: Any,
    timeshift_views: Any,
    *,
    dispatcharr_version: str,
    callbacks: XCCallbacks,
) -> HookInstallResult:
    """Install safe, idempotent hooks on the inspected Dispatcharr modules.

    No mutation occurs unless the exact supported version and every target
    signature match. An already-installed hook is reported as success.
    """
    if not is_supported_dispatcharr_version(dispatcharr_version):
        return HookInstallResult(False, f"unsupported Dispatcharr version: {dispatcharr_version}")

    targets = (
        (output_views, "_xc_channel_entry", ("channel", "channel_num_map", "_get_default_group_id",
         "_logo_url_prefix", "_logo_url_suffix", "catchup_allowed")),
        (output_views, "xc_get_epg", ("request", "user", "short")),
        (timeshift_views, "_serve_catchup", ("request", "user", "channel", "timestamp",
         "client_duration_hint")),
    )
    originals: dict[str, Callable[..., Any]] = {}
    installed_wrappers: list[Callable[..., Any]] = []
    for module, name, expected in targets:
        function = getattr(module, name, None)
        if function is None:
            return HookInstallResult(False, f"missing Dispatcharr function: {name}")
        if getattr(function, _HOOK_MARKER, False):
            installed_wrappers.append(function)
            continue
        if not _signature_matches(function, expected):
            return HookInstallResult(False, f"unexpected Dispatcharr signature: {name}")
        originals[name] = function

    # Repeated startup refreshes callback bindings rather than leaving wrappers
    # attached to stale plugin configuration.
    if not originals:
        states = [getattr(function, "__catchuparr_state__", None)
                  for function in installed_wrappers]
        if len(states) != len(targets) or any(state is None for state in states):
            return HookInstallResult(False, "incomplete XC hook state")
        if any(state is not states[0] for state in states[1:]):
            return HookInstallResult(False, "inconsistent XC hook state")
        states[0]["callbacks"] = callbacks
        return HookInstallResult(True, "already installed", tuple(name for _, name, _ in targets))

    # Validate the complete installation before patching any function. In the
    # unlikely event a partial prior install exists, do not layer wrappers.
    if len(originals) != len(targets):
        return HookInstallResult(False, "partial XC hook installation detected")

    state: dict[str, XCCallbacks] = {"callbacks": callbacks}

    def channel_entry_wrapper(channel, channel_num_map, _get_default_group_id,
                              _logo_url_prefix, _logo_url_suffix, *,
                              catchup_allowed=True):
        callbacks = state["callbacks"]
        result = originals["_xc_channel_entry"](
            channel, channel_num_map, _get_default_group_id, _logo_url_prefix,
            _logo_url_suffix, catchup_allowed=catchup_allowed,
        )
        if not catchup_allowed or callbacks.channel_archive_days is None:
            return result
        try:
            local_days = max(0, int(callbacks.channel_archive_days(channel)))
        except Exception:
            logger.exception("Catchuparr could not read local archive retention")
            return result
        if local_days:
            result = dict(result)
            result["tv_archive"] = 1
            result["tv_archive_duration"] = max(int(result.get("tv_archive_duration", 0) or 0), local_days)
        return result

    def epg_wrapper(request, user, short=False):
        callbacks = state["callbacks"]
        channel_id = request.GET.get("stream_id")
        # Run Dispatcharr's channel/profile filtering first. Only then resolve
        # the XC database ID into the UUID used by the archive engine.
        result = originals["xc_get_epg"](request, user, short=short)
        channel_uuid = None
        if (channel_id and callbacks.channel_uuid_for_epg_id is not None
                and _catchup_enabled(output_views, user)):
            try:
                channel_uuid = callbacks.channel_uuid_for_epg_id(channel_id, user)
                if channel_uuid is not None:
                    channel_uuid = str(channel_uuid)
            except Exception:
                logger.exception("Catchuparr could not resolve authorized XC channel UUID")
        local_days = 0
        if channel_uuid and callbacks.epg_archive_days is not None and not short:
            try:
                local_days = max(0, min(365, int(callbacks.epg_archive_days(channel_uuid, user))))
            except Exception:
                logger.exception("Catchuparr could not read local EPG lookback")
            if local_days:
                epg_request = _request_with_local_lookback(request, user, local_days)
                if epg_request is not request:
                    result = originals["xc_get_epg"](epg_request, user, short=short)
        if (not isinstance(result, dict) or callbacks.program_available is None
                or not channel_uuid or not _catchup_enabled(output_views, user)):
            return result
        listings = result.get("epg_listings")
        if not isinstance(listings, list):
            return result
        # Dispatcharr may cache and reuse this response. Keep annotations and
        # locally restored rows on a shallow copy of both the response and
        # each mutable listing dictionary.
        result = dict(result)
        listings = [dict(item) if isinstance(item, dict) else item for item in listings]
        result["epg_listings"] = listings
        if not short and local_days and callbacks.epg_snapshots is not None:
            try:
                snapshots = callbacks.epg_snapshots(channel_uuid, user, local_days)
            except Exception:
                logger.exception("Catchuparr could not load historical EPG snapshots")
                snapshots = []
            existing = {(item.get("start"), item.get("end")) for item in listings
                        if isinstance(item, dict)}
            for item in snapshots:
                if not isinstance(item, dict):
                    continue
                key = (item.get("start"), item.get("end"))
                if key[0] and key[1] and key not in existing:
                    listings.append(dict(item))
                    existing.add(key)
        # XC timestamps are emitted in UTC as YYYY-MM-DD HH:MM:SS. Keep the
        # serializer's EPG and availability decisions separate from the source
        # provider's channel-level archive flag. For the live programme, only
        # require coverage from its start through now; its future end is not
        # expected to be archived yet.
        now = datetime.now(timezone.utc)
        for listing in listings:
            try:
                start = datetime.strptime(listing["start"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                end = datetime.strptime(listing["end"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                if start > now:
                    continue
                coverage_end = min(end, now)
                available = callbacks.program_available(
                    channel_uuid, listing["start"],
                    coverage_end.strftime("%Y-%m-%d %H:%M:%S"), user,
                )
            except Exception:
                logger.exception("Catchuparr could not check XC programme coverage")
                available = False
            if available:
                listing["has_archive"] = 1
        return result

    def serve_wrapper(request, user, channel, timestamp, client_duration_hint=None):
        callbacks = state["callbacks"]
        # Mirror the existing handler's catch-up toggle before considering local
        # storage. The caller has already performed authentication, network
        # restrictions and channel ACL checks in v0.31.0's route handlers.
        if not _catchup_enabled(timeshift_views, user):
            return originals["_serve_catchup"](
                request, user, channel, timestamp,
                client_duration_hint=client_duration_hint,
            )
        timestamp_parser = getattr(timeshift_views, "parse_catchup_timestamp", None)
        if timestamp_parser is None:
            return originals["_serve_catchup"](
                request, user, channel, timestamp,
                client_duration_hint=client_duration_hint,
            )
        try:
            if timestamp_parser(timestamp) is None:
                return originals["_serve_catchup"](
                    request, user, channel, timestamp,
                    client_duration_hint=client_duration_hint,
                )
        except Exception:
            logger.exception("Dispatcharr catch-up timestamp validation failed")
            return _service_unavailable(timeshift_views, "Catch-up timestamp validation failed")
        if callbacks.playback_available is None:
            return originals["_serve_catchup"](
                request, user, channel, timestamp,
                client_duration_hint=client_duration_hint,
            )
        try:
            channel_uuid = getattr(channel, "uuid", None)
            if not channel_uuid:
                return originals["_serve_catchup"](
                    request, user, channel, timestamp,
                    client_duration_hint=client_duration_hint,
                )
            channel_uuid = str(channel_uuid)
            covered = callbacks.playback_available(
                channel_uuid, timestamp, client_duration_hint, user,
            )
        except Exception:
            logger.exception("Catchuparr local coverage lookup failed; using provider path")
            covered = False
        if not covered:
            return originals["_serve_catchup"](
                request, user, channel, timestamp,
                client_duration_hint=client_duration_hint,
            )

        # Dispatcharr's current stream-limit/session implementation is embedded
        # in _serve_catchup. Do not skip it silently: local playback is enabled
        # only when the archive engine supplies an explicit policy callback.
        if callbacks.authorize_local_playback is None or callbacks.serve_local_playback is None:
            return _service_unavailable(timeshift_views,
                                        "Local catch-up policy/session integration is not configured")
        try:
            authorization = callbacks.authorize_local_playback(
                request, user, channel, timestamp, client_duration_hint,
            )
        except Exception:
            logger.exception("Catchuparr local playback authorization failed")
            return _service_unavailable(timeshift_views, "Local catch-up authorization failed")
        if _is_response(authorization):
            return authorization
        if authorization is not True:
            return _forbidden(timeshift_views, "Local catch-up access denied")
        try:
            return callbacks.serve_local_playback(
                request, user, channel, timestamp, client_duration_hint,
            )
        except Exception:
            logger.exception("Catchuparr local playback failed")
            return _service_unavailable(timeshift_views, "Local catch-up playback failed")

    wrappers = {
        "_xc_channel_entry": channel_entry_wrapper,
        "xc_get_epg": epg_wrapper,
        "_serve_catchup": serve_wrapper,
    }
    for module, name, _expected in targets:
        wrapped = wrappers[name]
        setattr(wrapped, _HOOK_MARKER, True)
        setattr(wrapped, "__catchuparr_original__", originals[name])
        setattr(wrapped, "__catchuparr_state__", state)
        setattr(module, name, wrapped)
    return HookInstallResult(True, "installed", tuple(name for _, name, _ in targets))


def uninstall_xc_hooks(output_views: Any, timeshift_views: Any) -> HookInstallResult:
    """Restore original Dispatcharr functions when the plugin is disabled."""
    targets = (
        (output_views, "_xc_channel_entry"),
        (output_views, "xc_get_epg"),
        (timeshift_views, "_serve_catchup"),
    )
    restored = []
    for module, name in targets:
        function = getattr(module, name, None)
        if not getattr(function, _HOOK_MARKER, False):
            continue
        original = getattr(function, "__catchuparr_original__", None)
        if original is None:
            return HookInstallResult(False, f"missing original Dispatcharr function: {name}")
        restored.append((module, name, original))
    for module, name, original in restored:
        setattr(module, name, original)
    return HookInstallResult(True, "uninstalled", tuple(name for _, name, _ in restored))


def _request_with_local_lookback(request: Any, user: Any, local_days: int) -> Any:
    """Copy an XC request and widen only its EPG lookback query parameter."""
    try:
        query = request.GET.copy() if hasattr(request.GET, "copy") else dict(request.GET)
        configured_days = 0
        for value in (
            query.get("prev_days"),
            (getattr(user, "custom_properties", None) or {}).get("epg_prev_days"),
        ):
            try:
                configured_days = max(configured_days, int(value or 0))
            except (TypeError, ValueError):
                continue
        requested_days = min(365, max(local_days, configured_days, 0))
        query["prev_days"] = str(requested_days)
        copied_request = copy.copy(request)
        copied_request.GET = query
        return copied_request
    except Exception:
        # If a request implementation cannot be safely copied, leave it intact;
        # local playback metadata may still apply to whatever EPG it returns.
        logger.exception("Could not create isolated XC request with local EPG lookback")
        return request


def _catchup_enabled(module: Any, user: Any) -> bool:
    function = getattr(module, "is_catchup_enabled", None)
    if function is None:
        # Absence of the Dispatcharr policy helper is not permission to enable
        # an alternate playback path.
        return False
    try:
        return bool(function(user=user))
    except Exception:
        logger.exception("Dispatcharr catch-up policy check failed")
        return False


def _service_unavailable(module: Any, message: str) -> Any:
    response_type = getattr(module, "HttpResponse", None)
    if response_type is not None:
        return response_type(message, status=503)
    return _FallbackResponse(message, 503)


def _forbidden(module: Any, message: str) -> Any:
    response_type = getattr(module, "HttpResponseForbidden", None)
    if response_type is not None:
        return response_type(message)
    return _FallbackResponse(message, 403)


class _FallbackResponse:
    """Minimal response shape for import-time mocks and defensive failures."""

    def __init__(self, content: str, status: int):
        self.content = content
        self.status_code = status
        self.headers: dict[str, str] = {}

    def __getitem__(self, key: str) -> str:
        return self.headers[key]

    def __setitem__(self, key: str, value: str) -> None:
        self.headers[key] = value
