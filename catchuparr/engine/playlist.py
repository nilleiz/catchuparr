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
    target_duration: int = 60,
    start_offset: float | None = None,
) -> str:
    """Build an EVENT playlist that can close after the capture ends.

    For live playback call again periodically so newly committed segments are
    appended. URIs should normally be authenticated plugin endpoint URLs.
    """
    items = sorted(segments, key=lambda item: (item.start_utc, item.id))
    if media_sequence < 0:
        raise ValueError("media_sequence cannot be negative")
    durations = [segment.duration for segment in items]
    if any(duration <= 0 for duration in durations):
        raise ValueError("HLS segments must have positive duration")
    if target_duration < 1:
        raise ValueError("target_duration must be positive")
    if start_offset is not None and (not math.isfinite(start_offset) or start_offset < 0):
        raise ValueError("start_offset must be a finite non-negative number")
    # EXT-X-TARGETDURATION is fixed for a media playlist across reloads. Keep
    # it independent of the current EVENT contents; reject an unexpectedly
    # long GOP rather than emit a playlist with a changing/invalid target.
    if any(math.floor(duration + 0.5) > target_duration for duration in durations):
        raise ValueError("segment duration exceeds the fixed HLS target duration; configure a larger stable value")
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{target_duration}",
        f"#EXT-X-MEDIA-SEQUENCE:{media_sequence}",
    ]
    # A start-over URL is reloaded as one media playlist while the programme
    # crosses its end time. Changing its type from EVENT to VOD on that reload
    # would violate HLS playlist mutability rules; EVENT may add ENDLIST.
    lines.append("#EXT-X-PLAYLIST-TYPE:EVENT")
    if start_offset is not None:
        lines.append(f"#EXT-X-START:TIME-OFFSET={start_offset:.3f}")
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
