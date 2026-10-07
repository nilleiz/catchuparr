"""Version-guarded XC compatibility hooks for inspected Dispatcharr releases.

Dispatcharr does not expose public hooks for its XC serializers or catch-up
handler. This module wraps only inspected functions and refuses to patch a
changed version/signature. The local playback callback is deliberately
separate from Dispatcharr's provider path: it must enforce Dispatcharr-equivalent
stream limits and create/release an archive playback lease.
"""

from __future__ import annotations

import copy
import inspect
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping
from urllib.parse import urlencode, urlsplit

from ..compatibility import (
    SUPPORTED_DISPATCHARR_VERSION as SUPPORTED_DISPATCHARR_VERSION,
)
from ..compatibility import (
    is_supported_dispatcharr_version,
)

_HOOK_MARKER = "__catchuparr_xc_hook__"
_MAX_M3U_BYTES = 16 * 1024 * 1024
_MAX_M3U_LINES = 100_000
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

    ``authorize_xc_m3u`` confirms the request uses valid XC credentials and
    passes Dispatcharr's XC network policy. ``m3u_channel_archive_days`` maps
    selected local Dispatcharr channel IDs to their retention in whole days.
    The M3U wrapper only applies that map to entries already emitted by the
    core's authorized channel query.
    """

    channel_archive_days: Callable[[Any], int] | None = None
    channel_uuid_for_epg_id: Callable[[str, Any], str | None] | None = None
    epg_archive_days: Callable[[str, Any], int] | None = None
    epg_snapshots: Callable[[str, Any, int], list[dict[str, Any]]] | None = None
    program_available: Callable[[Any, str, str, Any], bool] | None = None
    playback_available: Callable[[Any, str, Any, Any], bool] | None = None
    local_playback_supported: Callable[[Any, Any, Any], bool] | None = None
    authorize_local_playback: Callable[[Any, Any, Any, str, Any], Any] | None = None
    serve_local_playback: Callable[[Any, Any, Any, str, Any], Any] | None = None
    authorize_xc_m3u: Callable[[Any, Any], bool] | None = None
    m3u_channel_archive_days: Callable[[Any], Mapping[str, int]] | None = None


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

    targets = [
        (output_views, "_xc_channel_entry", ("channel", "channel_num_map", "_get_default_group_id",
         "_logo_url_prefix", "_logo_url_suffix", "catchup_allowed")),
        (output_views, "xc_get_epg", ("request", "user", "short")),
        (timeshift_views, "_serve_catchup", ("request", "user", "channel", "timestamp",
         "client_duration_hint")),
    ]
    targets.append((output_views, "generate_m3u", ("request", "profile_name", "user")))
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
        # provider's channel-level archive flag. The runtime clips a current
        # programme to the latest committed segment edge.
        now = datetime.now(timezone.utc)
        for listing in listings:
            try:
                start = datetime.strptime(listing["start"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                datetime.strptime(listing["end"], "%Y-%m-%d %H:%M:%S")
                if start > now:
                    continue
                available = callbacks.program_available(
                    channel_uuid, listing["start"], listing["end"], user,
                )
            except Exception:
                logger.exception("Catchuparr could not check XC programme coverage")
                available = False
            if available:
                listing["has_archive"] = 1
        return result

    def serve_wrapper(request, user, channel, timestamp, client_duration_hint=None):
        callbacks = state["callbacks"]
        if callbacks.local_playback_supported is not None:
            try:
                if not callbacks.local_playback_supported(request, user, channel):
                    return originals["_serve_catchup"](
                        request, user, channel, timestamp,
                        client_duration_hint=client_duration_hint,
                    )
            except Exception:
                logger.exception("Catchuparr could not verify the XC playback route")
                return _service_unavailable(timeshift_views, "XC playback route validation failed")
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
            return finalize_dispatcharr_response(timeshift_views, authorization)
        if authorization is not True:
            return _forbidden(timeshift_views, "Local catch-up access denied")
        try:
            return callbacks.serve_local_playback(
                request, user, channel, timestamp, client_duration_hint
            )
        except Exception:
            logger.exception("Catchuparr local playback failed")
            return _service_unavailable(timeshift_views, "Local catch-up playback failed")

    def m3u_wrapper(request, profile_name=None, user=None):
        response = originals["generate_m3u"](request, profile_name=profile_name, user=user)
        callbacks = state["callbacks"]
        if not _native_xc_m3u_allowed(output_views, request, user, callbacks):
            return response
        if callbacks.m3u_channel_archive_days is None:
            return response
        try:
            channel_days = callbacks.m3u_channel_archive_days(user)
        except Exception:
            logger.exception("Catchuparr could not read selected XC archive channels")
            return response
        if not channel_days:
            return response
        try:
            base_builder = getattr(output_views, "build_absolute_uri_with_port")
            base_url = str(base_builder(request, "")).rstrip("/")
            username = str(request.GET.get("username") or "")
            password = str(request.GET.get("password") or "")
            credentials = urlencode({"username": username, "password": password})
            timestamp_parameter = "utc" if dispatcharr_version == "0.32.0" else "start"
            source_prefix = (
                f"{base_url}/streaming/timeshift.php?{credentials}&stream="
            )
            return _annotate_native_xc_m3u(
                response, channel_days, source_prefix, timestamp_parameter
            )
        except Exception:
            # Core output remains usable if its response cannot be safely edited.
            logger.exception("Catchuparr could not annotate the XC playlist")
            return response

    wrappers = {
        "_xc_channel_entry": channel_entry_wrapper,
        "xc_get_epg": epg_wrapper,
        "_serve_catchup": serve_wrapper,
    }
    if "generate_m3u" in originals:
        wrappers["generate_m3u"] = m3u_wrapper
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
        (output_views, "generate_m3u"),
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


def _native_xc_m3u_allowed(module: Any, request: Any, user: Any, callbacks: XCCallbacks) -> bool:
    """Limit M3U changes to authenticated, non-direct XC requests."""
    if user is None or callbacks.authorize_xc_m3u is None:
        return False
    if str(getattr(request, "method", "GET")).upper() != "GET":
        return False
    query = getattr(request, "GET", {})
    username = str(query.get("username") or "")
    password = str(query.get("password") or "")
    if not username or not password or str(query.get("direct", "false")).lower() == "true":
        return False
    if not _catchup_enabled(module, user):
        return False
    try:
        return bool(callbacks.authorize_xc_m3u(request, user))
    except Exception:
        logger.exception("Catchuparr could not verify XC playlist authorization")
        return False


def _annotate_native_xc_m3u(
    response: Any,
    channel_days: Mapping[str, int],
    source_prefix: str,
    timestamp_parameter: str,
) -> Any:
    """Add local catch-up templates to selected core-emitted XC entries.

    The response size and line count are bounded. Existing catch-up sources
    are preserved. Numeric provider lookback values increase only when local
    retention is longer, and repeated calls are idempotent.
    """
    if getattr(response, "status_code", None) != 200 or not hasattr(response, "content"):
        return response
    raw = response.content
    if isinstance(raw, str):
        text = raw
        raw_size = len(raw.encode("utf-8"))
    elif isinstance(raw, bytes):
        raw_size = len(raw)
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            return response
    else:
        return response
    if raw_size > _MAX_M3U_BYTES or not text.lstrip("\ufeff").startswith("#EXTM3U"):
        return response
    lines = text.splitlines(keepends=True)
    if len(lines) > _MAX_M3U_LINES:
        return response
    builder = getattr(response, "_charset", None) or "utf-8"
    changed = False
    index = 0
    while index < len(lines):
        line = lines[index]
        entry = line.rstrip("\r\n")
        if entry.startswith("#EXTINF:") and index + 1 < len(lines):
            channel_id = _xc_channel_id_from_live_url(lines[index + 1].strip())
            if channel_id is not None:
                try:
                    local_days = max(0, min(365, int(channel_days.get(channel_id, 0) or 0)))
                except (TypeError, ValueError):
                    local_days = 0
                if local_days:
                    annotated = _annotate_xc_extinf(
                        entry, local_days, source_prefix + channel_id, timestamp_parameter
                    )
                    if annotated != entry:
                        ending = line[len(entry):]
                        lines[index] = annotated + ending
                        changed = True
        index += 1
    if changed:
        result = "".join(lines)
        response.content = result.encode(builder) if isinstance(raw, bytes) else result
    return response


def _xc_channel_id_from_live_url(value: str) -> str | None:
    try:
        path_parts = [part for part in urlsplit(value).path.split("/") if part]
        live_index = path_parts.index("live")
        channel_id = path_parts[live_index + 3]
    except (ValueError, IndexError):
        return None
    return channel_id if re.fullmatch(r"\d{1,12}", channel_id) else None


def _annotate_xc_extinf(
    line: str, local_days: int, source: str, timestamp_parameter: str
) -> str:
    separator = _m3u_title_comma(line)
    if separator is None:
        return line
    head, tail = line[:separator], line[separator:]
    attributes = _m3u_attributes(head)
    if "catchup" not in attributes:
        head = _set_m3u_attribute(head, "catchup", "default")
    if "catchup-source" not in attributes:
        head = _set_m3u_attribute(
            head, "catchup-source",
            source + f"&{timestamp_parameter}={{utc}}&duration={{duration:60}}",
        )
    existing_days = attributes.get("catchup-days")
    if existing_days is None:
        head = _set_m3u_attribute(head, "catchup-days", str(local_days))
    else:
        try:
            provider_days = int(existing_days)
        except (TypeError, ValueError):
            provider_days = local_days
        if provider_days < local_days:
            head = _set_m3u_attribute(head, "catchup-days", str(local_days))
    return head + tail


def _m3u_title_comma(line: str) -> int | None:
    in_quote = False
    escaped = False
    for index, character in enumerate(line):
        if escaped:
            escaped = False
            continue
        if character == "\\" and in_quote:
            escaped = True
            continue
        if character == '"':
            in_quote = not in_quote
        elif character == "," and not in_quote:
            return index
    return None


def _m3u_attributes(line: str) -> dict[str, str]:
    separator = _m3u_title_comma(line)
    header = line if separator is None else line[:separator]
    return {
        match.group(1): match.group(2)
        for match in re.finditer(r'(?<!\S)([A-Za-z0-9_-]+)="([^"]*)"', header)
    }


def _set_m3u_attribute(line: str, name: str, value: str) -> str:
    separator = _m3u_title_comma(line)
    if separator is None:
        head, tail = line, ""
    else:
        head, tail = line[:separator], line[separator:]
    pattern = re.compile(rf'(?<!\S){re.escape(name)}="[^"]*"')
    replacement = f'{name}="{value}"'
    if pattern.search(head):
        head = pattern.sub(replacement, head, count=1)
    else:
        head = head.rstrip() + " " + replacement
    return head + tail


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
        return finalize_dispatcharr_response(module, response_type(message, status=503))
    return _FallbackResponse(message, 503)


def _forbidden(module: Any, message: str) -> Any:
    response_type = getattr(module, "HttpResponseForbidden", None)
    if response_type is not None:
        return finalize_dispatcharr_response(module, response_type(message))
    return _FallbackResponse(message, 403)


def finalize_dispatcharr_response(module: Any, response: Any) -> Any:
    """Close request-scoped Django DB connections before returning playback."""
    if response is None:
        return None
    finalizer = getattr(module, "_finalize_timeshift_response", None)
    if finalizer is None:
        return response
    try:
        finalized = finalizer(response)
        return finalized if finalized is not None else response
    except Exception:
        logger.exception("Dispatcharr could not finalize the local XC response")
        try:
            from django.db import close_old_connections

            close_old_connections()
        except Exception:
            logger.exception("Could not close Django connections after XC playback")
        return response


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
