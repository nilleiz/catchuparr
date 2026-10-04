"""Pure M3U/XMLTV helpers for exposing locally archived channel programmes.

These functions intentionally do not assume a particular TiviMate template
implementation. ``{utc}`` and ``{duration}`` are emitted as configured protocol
placeholders and must be verified against the target client.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Mapping
from urllib.parse import quote, urlsplit
import re
import xml.etree.ElementTree as ET

MAX_XMLTV_BYTES = 64 * 1024 * 1024


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
        archive_id = channel_map.get(attrs.get("tvg-id", ""))
        if archive_id is None and line_index + 1 < len(lines):
            live_url = lines[line_index + 1].strip()
            archive_id = channel_map.get(live_url)
            if archive_id is None:
                path_key = urlsplit(live_url).path.rstrip("/").rsplit("/", 1)[-1]
                archive_id = channel_map.get(path_key)
        if archive_id is None:
            output.append(line)
            continue

        source = build_catchup_source(endpoint, archive_id, access_token)
        stripped = _set_extinf_attribute(stripped, "catchup", "default")
        stripped = _set_extinf_attribute(stripped, "catchup-source", source)
        stripped = _set_extinf_attribute(stripped, "catchup-days", str(catchup_days))
        output.append(stripped + ending)
    return "".join(output)


def filter_xmltv(
    xmltv: str | bytes,
    is_covered: Callable[[str, datetime, datetime], bool],
    now: datetime | None = None,
) -> str:
    """Drop historical XMLTV programmes without archive coverage.

    Current/future entries, entries without parseable boundaries, channels, and
    all non-programme XMLTV elements are retained. ``is_covered`` receives the
    XMLTV channel ID and UTC-aware start/stop datetimes. The in-memory tree is
    limited to ``MAX_XMLTV_BYTES``; larger guides must be filtered upstream or
    handled by a streaming adapter.
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


def _parse_extinf_attributes(line: str) -> dict[str, str]:
    """Parse quoted EXTINF attributes without disturbing the original line."""
    attrs: dict[str, str] = {}
    index = line.find(":") + 1
    while index < len(line):
        if line[index].isspace() or line[index] == ",":
            if line[index] == ",":
                break
            index += 1
            continue
        start = index
        while index < len(line) and (line[index].isalnum() or line[index] in "_-:"):
            index += 1
        key = line[start:index]
        if not key or index >= len(line) or line[index] != "=":
            index += 1
            continue
        index += 1
        if index >= len(line) or line[index] != '"':
            while index < len(line) and not line[index].isspace() and line[index] != ",":
                index += 1
            continue
        index += 1
        value_start = index
        while index < len(line) and line[index] != '"':
            index += 1
        attrs[key] = line[value_start:index]
        index += 1
    return attrs


def _set_extinf_attribute(line: str, name: str, value: str) -> str:
    # M3U is not XML; preserve query separators as literal ampersands.
    safe_value = value.replace('"', "%22")
    pattern = re.compile(rf'(?<![\w-]){re.escape(name)}="[^"]*"')
    replacement = f'{name}="{safe_value}"'
    if pattern.search(line):
        return pattern.sub(replacement, line, count=1)
    comma = line.find(",")
    if comma < 0:
        return line + " " + replacement
    return line[:comma] + " " + replacement + line[comma:]


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
