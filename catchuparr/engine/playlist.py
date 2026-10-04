"""HLS playlist rendering for the immutable archive segments."""

from __future__ import annotations

import math
from datetime import timezone
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import quote

from .store import Segment


def build_hls_playlist(
    segments: Iterable[Segment],
    *,
    live: bool,
    uri_for: Callable[[Segment], str] | None = None,
    media_sequence: int = 0,
) -> str:
    """Build an EVENT playlist for a growing capture or VOD for a closed one.

    For live playback call again periodically so newly committed segments are
    appended. URIs should normally be authenticated plugin endpoint URLs.
    """
    items = sorted(segments, key=lambda item: (item.start_utc, item.id))
    if media_sequence < 0:
        raise ValueError("media_sequence cannot be negative")
    durations = [segment.duration for segment in items]
    if any(duration <= 0 for duration in durations):
        raise ValueError("HLS segments must have positive duration")
    target = max(1, math.ceil(max(durations, default=1)))
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{target}",
        f"#EXT-X-MEDIA-SEQUENCE:{media_sequence}",
    ]
    if live:
        lines.append("#EXT-X-PLAYLIST-TYPE:EVENT")
    else:
        lines.append("#EXT-X-PLAYLIST-TYPE:VOD")
    for item in items:
        if item.discontinuity:
            lines.append("#EXT-X-DISCONTINUITY")
        stamp = item.start_utc.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        lines.append(f"#EXT-X-PROGRAM-DATE-TIME:{stamp}")
        lines.append(f"#EXTINF:{item.duration:.3f},")
        if uri_for is not None:
            uri = uri_for(item)
        else:
            uri = quote(Path(item.path).name)
        if "\r" in uri or "\n" in uri:
            raise ValueError("segment URI cannot contain newlines")
        lines.append(uri)
    if not live:
        lines.append("#EXT-X-ENDLIST")
    return "\n".join(lines) + "\n"
