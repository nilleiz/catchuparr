"""Pure M3U/XMLTV helpers for exposing locally archived channel programmes.

These functions emit TiviMate's ``{utc}`` and ``{duration}`` placeholders in
seconds.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Callable, Collection, Iterable, Mapping
from urllib.parse import quote, urlsplit

MAX_XMLTV_BYTES = 64 * 1024 * 1024


def set_xmltv_playlist_urls(playlist: str, url: str) -> str:
    """Set XMLTV header URLs while preserving the native M3U header metadata."""
    if not isinstance(url, str) or not url:
        raise ValueError("XMLTV URL is required")
    lines = playlist.splitlines(keepends=True)
    if not lines:
        return playlist
    header = lines[0]
    ending = header[len(header.rstrip("\r\n")) :]
    content = header[: len(header) - len(ending)] if ending else header
    bom = "\ufeff" if content.startswith("\ufeff") else ""
    directive = content[len(bom) :]
    if not directive.startswith("#EXTM3U"):
        return playlist

    directive = _set_header_attribute(directive, "x-tvg-url", url)
    directive = _set_header_attribute(directive, "url-tvg", url)
    lines[0] = bom + directive + ending
    return "".join(lines)


def build_catchup_source(
    endpoint: str,
    channel_id: str,
    access_token: str,
) -> str:
    """Build a URL template carrying a revocable opaque access token.

    The returned playlist URL is a bearer credential and must be treated as
    sensitive: do not log it or include it in diagnostics. Use a token scoped
    to one user, and allow it to be revoked by the archive endpoint. TiviMate
    is not assumed to send separate authorization headers.
    """
    parts = urlsplit(endpoint)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise ValueError("endpoint must be an absolute HTTP(S) URL")
    if parts.username is not None or parts.password is not None:
        raise ValueError("endpoint must not contain embedded credentials")
    if parts.fragment:
        raise ValueError("endpoint must not contain a fragment")
    if not channel_id or not access_token:
        raise ValueError("channel_id and access_token are required")

    separator = "&" if parts.query else "?"
    # Keep the two player placeholders literal for the playlist consumer.
    return (
        f"{endpoint}{separator}channel_id={quote(str(channel_id), safe='')}"
        f"&access_token={quote(str(access_token), safe='')}"
        f"&utc={{utc}}&duration={{duration}}"
    )


def annotate_m3u(
    playlist: str,
    channel_map: Mapping[str, str],
    endpoint: str,
    access_token: str,
    catchup_days: int = 1,
    *,
    preserve_existing_catchup: bool = False,
) -> str:
    """Add local catch-up attributes to selected Dispatcharr M3U entries.

    ``channel_map`` maps the effective ``tvg-id`` to the plugin's stable
    archive channel ID. As fallbacks it may map the live URL verbatim or the
    final path component of that URL (useful for Dispatcharr proxy UUIDs).
    The generated XMLTV must use matching effective ``tvg-id`` values.
    Original channel IDs, metadata and live URLs are kept. Non-selected entries
    and comments are returned unchanged.
    """
    if catchup_days < 0:
        raise ValueError("catchup_days must be non-negative")
    lines = playlist.splitlines(keepends=True)
    output: list[str] = []
    for line_index, line in enumerate(lines):
        stripped = line.rstrip("\r\n")
        ending = line[len(stripped) :]
        if not stripped.startswith("#EXTINF:"):
            output.append(line)
            continue

        attrs = _parse_extinf_attributes(stripped)
        if preserve_existing_catchup and any(
            name in attrs for name in ("catchup", "catchup-source", "catchup-days")
        ):
            output.append(line)
            continue
        archive_id = None
        has_native_proxy = False
        if line_index + 1 < len(lines):
            live_url = lines[line_index + 1].strip()
            native_stream_id = _proxy_stream_id(live_url)
            has_native_proxy = native_stream_id is not None
            if native_stream_id is not None:
                # A recognized Dispatcharr proxy ID is authoritative. If it is
                # not selected, a conflicting tvg-id must not grant archive access.
                archive_id = channel_map.get(native_stream_id)
            else:
                archive_id = channel_map.get(live_url)
                if archive_id is None:
                    path_key = urlsplit(live_url).path.rstrip("/").rsplit("/", 1)[-1]
                    archive_id = channel_map.get(path_key)
        if archive_id is None and not has_native_proxy:
            archive_id = channel_map.get(attrs.get("tvg-id", ""))
        if archive_id is None:
            output.append(line)
            continue

        source = build_catchup_source(endpoint, archive_id, access_token)
        stripped = _set_extinf_attribute(stripped, "catchup", "default")
        stripped = _set_extinf_attribute(stripped, "catchup-source", source)
        stripped = _set_extinf_attribute(stripped, "catchup-days", str(catchup_days))
        stripped = _set_extinf_attribute(stripped, "catchup-timezone", "UTC")
        output.append(stripped + ending)
    return "".join(output)


def annotate_native_m3u(
    playlist: str,
    capabilities: Mapping[str, str],
    endpoint: str,
    catchup_days: int = 1,
) -> str:
    """Add signed native archive templates only to emitted proxy channels."""
    if catchup_days < 0:
        raise ValueError("catchup_days must be non-negative")
    parts = urlsplit(endpoint)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise ValueError("endpoint must be an absolute HTTP(S) URL")
    if parts.username is not None or parts.password is not None or parts.fragment:
        raise ValueError("endpoint must not contain credentials or a fragment")
    lines = playlist.splitlines(keepends=True)
    output: list[str] = []
    for index, line in enumerate(lines):
        stripped = line.rstrip("\r\n")
        ending = line[len(stripped):]
        if not stripped.startswith("#EXTINF:"):
            output.append(line)
            continue
        attrs = _parse_extinf_attributes(stripped)
        if any(name in attrs for name in ("catchup", "catchup-source", "catchup-days")):
            output.append(line)
            continue
        stream_id = _proxy_stream_id(lines[index + 1]) if index + 1 < len(lines) else None
        token = capabilities.get(stream_id) if stream_id is not None else None
        if token is None:
            output.append(line)
            continue
        query = "&" if parts.query else "?"
        source = (
            f"{endpoint}{query}channel_id={quote(stream_id, safe='')}"
            f"&token={quote(token, safe='')}&utc={{utc}}&duration={{duration}}"
        )
        stripped = _set_extinf_attribute(stripped, "catchup", "default")
        stripped = _set_extinf_attribute(stripped, "catchup-source", source)
        stripped = _set_extinf_attribute(stripped, "catchup-days", str(catchup_days))
        stripped = _set_extinf_attribute(stripped, "catchup-timezone", "UTC")
        output.append(stripped + ending)
    return "".join(output)


def xmltv_channel_map_from_m3u(
    playlist: str,
    eligible_channel_ids: Collection[str],
) -> dict[str, str]:
    """Map emitted output-profile tvg IDs to eligible native stream IDs."""
    eligible = {str(channel_id) for channel_id in eligible_channel_ids}
    emitted: dict[str, set[str | None]] = {}
    pending_tvg_id = None
    for line in playlist.splitlines():
        if line.startswith("#EXTINF:"):
            pending_tvg_id = _parse_extinf_attributes(line).get("tvg-id")
            continue
        if line.startswith("#") or pending_tvg_id is None:
            continue
        stream_id = _proxy_stream_id(line)
        tvg_id = pending_tvg_id
        pending_tvg_id = None
        if not tvg_id:
            continue
        emitted.setdefault(tvg_id, set()).add(stream_id)
    return {
        tvg_id: next(iter(stream_ids))
        for tvg_id, stream_ids in emitted.items()
        if len(stream_ids) == 1
        and next(iter(stream_ids)) is not None
        and next(iter(stream_ids)) in eligible
    }


def _proxy_stream_id(line: str) -> str | None:
    path_parts = [part for part in urlsplit(line.strip()).path.split("/") if part]
    for index in range(len(path_parts) - 3):
        if path_parts[index : index + 3] == ["proxy", "ts", "stream"]:
            return path_parts[index + 3]
    return None


def filter_xmltv(
    xmltv: str | bytes,
    is_covered: Callable[[str, datetime, datetime], bool],
    now: datetime | None = None,
    *,
    local_channel_ids: Collection[str] | None = None,
) -> str:
    """Drop selected local history without archive coverage.

    With ``local_channel_ids`` set, all other channels retain their provider
    history. Current/future entries, entries without parseable boundaries and
    non-programme XMLTV elements are retained. ``is_covered`` receives the
    XMLTV channel ID and UTC-aware start/stop datetimes. The in-memory tree is
    limited to ``MAX_XMLTV_BYTES``.
    """
    size = len(xmltv.encode("utf-8")) if isinstance(xmltv, str) else len(xmltv)
    if size > MAX_XMLTV_BYTES:
        raise ValueError(f"XMLTV input exceeds {MAX_XMLTV_BYTES} byte limit")
    root = ET.fromstring(xmltv)
    current = _as_utc(now or datetime.now(timezone.utc))
    for programme in list(root.findall("programme")):
        stop_text = programme.get("stop")
        start_text = programme.get("start")
        channel = programme.get("channel")
        if not (stop_text and start_text and channel):
            continue
        if local_channel_ids is not None and channel not in local_channel_ids:
            continue
        try:
            stop = _parse_xmltv_time(stop_text)
            start = _parse_xmltv_time(start_text)
        except (ValueError, TypeError):
            continue
        if stop >= current:
            continue
        if not is_covered(channel, start, stop):
            root.remove(programme)
    return ET.tostring(root, encoding="unicode", xml_declaration=False)


def merge_xmltv_snapshots(
    xmltv: str,
    snapshots: Mapping[str, Iterable[dict]],
    is_covered: Callable[[str, datetime, datetime], bool],
    *,
    now: datetime | None = None,
) -> str:
    """Restore archived past EPG entries removed by a provider refresh.

    Keys in ``snapshots`` are the effective XMLTV channel IDs emitted in the
    M3U. Stored schedule times are absolute instants, so DST transitions do
    not need local-time reconstruction.
    """
    size = len(xmltv.encode("utf-8"))
    if size > MAX_XMLTV_BYTES:
        raise ValueError(f"XMLTV input exceeds {MAX_XMLTV_BYTES} byte limit")
    root = ET.fromstring(xmltv)
    present = {_xmltv_programme_key(item) for item in root.findall("programme")}
    current = _as_utc(now or datetime.now(timezone.utc))
    for channel_id, programs in snapshots.items():
        for program in programs:
            start = _as_utc(program["start_utc"])
            end = _as_utc(program["end_utc"])
            if end >= current or not is_covered(channel_id, start, end):
                continue
            start_text = start.strftime("%Y%m%d%H%M%S +0000")
            end_text = end.strftime("%Y%m%d%H%M%S +0000")
            key = (channel_id, start, end)
            if key in present:
                continue
            entry = ET.SubElement(root, "programme", start=start_text, stop=end_text, channel=channel_id)
            ET.SubElement(entry, "title").text = str(program["title"])
            description = program.get("payload", {}).get("description")
            if description:
                ET.SubElement(entry, "desc").text = str(description)
            present.add(key)
    result = ET.tostring(root, encoding="unicode", xml_declaration=False)
    if len(result.encode("utf-8")) > MAX_XMLTV_BYTES:
        raise ValueError("XMLTV output exceeds size limit")
    return result


def _parse_extinf_attributes(line: str) -> dict[str, str]:
    """Parse quoted EXTINF attributes without disturbing the original line."""
    return {
        key: line[value_start:value_end]
        for key, value_start, value_end in _extinf_attribute_spans(line)
    }


def _extinf_attribute_spans(line: str) -> list[tuple[str, int, int]]:
    spans = []
    limit = _unquoted_comma(line)
    if limit < 0:
        limit = len(line)
    index = line.find(":") + 1
    while index < limit:
        while index < limit and line[index].isspace():
            index += 1
        start = index
        while index < limit and (line[index].isalnum() or line[index] in "_-:"):
            index += 1
        if index == start:
            index += 1
            continue
        key = line[start:index]
        while index < limit and line[index].isspace():
            index += 1
        if index >= limit or line[index] != "=":
            continue
        index += 1
        while index < limit and line[index].isspace():
            index += 1
        if index >= limit:
            break
        quote_char = line[index] if line[index] in "\"'" else None
        if quote_char:
            index += 1
            value_start = index
            while index < limit:
                if line[index] == "\\" and index + 1 < limit:
                    index += 2
                    continue
                if line[index] == quote_char:
                    break
                index += 1
            value_end = index
            if index < limit:
                index += 1
        else:
            value_start = index
            while index < limit and not line[index].isspace():
                index += 1
            value_end = index
        spans.append((key, value_start, value_end))
    return spans


def _set_extinf_attribute(line: str, name: str, value: str) -> str:
    # M3U is not XML; preserve query separators as literal ampersands.
    safe_value = value.replace('"', "%22")
    spans = [
        (value_start, value_end)
        for key, value_start, value_end in _extinf_attribute_spans(line)
        if key == name
    ]
    comma = _unquoted_comma(line)
    if spans:
        for start, stop in reversed(spans):
            line = line[:start] + safe_value + line[stop:]
        return line
    if comma < 0:
        return line + f' {name}="{safe_value}"'
    return line[:comma] + f' {name}="{safe_value}"' + line[comma:]


def _set_header_attribute(line: str, name: str, value: str) -> str:
    safe_value = value.replace('"', "%22").replace("'", "%27")
    spans = _header_attribute_spans(line, name)
    if not spans:
        return line + f' {name}="{safe_value}"'
    for start, end in reversed(spans):
        line = line[:start] + safe_value + line[end:]
    return line


def _header_attribute_spans(line: str, wanted: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    index = len("#EXTM3U")
    while index < len(line):
        while index < len(line) and line[index].isspace():
            index += 1
        start = index
        while index < len(line) and (line[index].isalnum() or line[index] in "_-:"):
            index += 1
        if index == start:
            index += 1
            continue
        key = line[start:index]
        if index >= len(line) or line[index] != "=":
            continue
        index += 1
        if index >= len(line):
            break
        quote_char = line[index] if line[index] in "\"'" else None
        if quote_char:
            index += 1
            value_start = index
            while index < len(line):
                if line[index] == "\\" and index + 1 < len(line):
                    index += 2
                    continue
                if line[index] == quote_char:
                    break
                index += 1
            value_end = index
            if index < len(line):
                index += 1
        else:
            value_start = index
            while index < len(line) and not line[index].isspace():
                index += 1
            value_end = index
        if key == wanted:
            spans.append((value_start, value_end))
    return spans


def _unquoted_comma(line: str) -> int:
    quote_char = None
    escaped = False
    for index, char in enumerate(line):
        if escaped:
            escaped = False
            continue
        if char == "\\" and quote_char:
            escaped = True
        elif char in "\"'":
            if quote_char == char:
                quote_char = None
            elif quote_char is None:
                quote_char = char
        elif char == "," and quote_char is None:
            return index
    return -1


def _xmltv_programme_key(programme: ET.Element):
    channel = programme.get("channel")
    start_text = programme.get("start")
    stop_text = programme.get("stop")
    try:
        return (channel, _parse_xmltv_time(start_text), _parse_xmltv_time(stop_text))
    except (AttributeError, TypeError, ValueError):
        return (channel, "raw", start_text, stop_text)


def _parse_xmltv_time(value: str) -> datetime:
    # XMLTV dates conventionally use YYYYMMDDhhmmss with optional offset.
    raw, _, zone = value.strip().partition(" ")
    parsed = datetime.strptime(raw, "%Y%m%d%H%M%S")
    if zone:
        if len(zone) == 5 and zone[0] in "+-":
            parsed = parsed.replace(tzinfo=timezone.utc if zone == "+0000" else None)
            if parsed.tzinfo is None:
                from datetime import timedelta

                sign = 1 if zone[0] == "+" else -1
                delta = timedelta(hours=int(zone[1:3]), minutes=int(zone[3:5]))
                parsed = parsed.replace(tzinfo=timezone(sign * delta))
        else:
            parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
