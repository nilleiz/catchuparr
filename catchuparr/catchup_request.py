"""Bounded catch-up ranges for clients with different placeholder support."""

from __future__ import annotations

import math
import re

MAX_ARCHIVE_SECONDS = 24 * 60 * 60


def archive_request_end(query, start: float, now: float, *, programme_end=None) -> float:
    """Resolve seconds, explicit end, guide end, or a bounded current snapshot.

    ``lutc`` denotes the client clock, never the programme end. Only known
    unexpanded placeholders are treated as absent; arbitrary malformed values
    remain errors. A numeric duration is in seconds, unlike XC path minutes.
    """
    for name in ("utc", "duration", "utcend", "lutc", "playback"):
        if hasattr(query, "getlist") and len(query.getlist(name)) > 1:
            raise ValueError("duplicate catch-up parameter")
    if not math.isfinite(start) or not math.isfinite(now):
        raise ValueError("invalid catch-up clock")

    def number(name, placeholders):
        value = query.get(name, "")
        if value in ("", *placeholders):
            return None
        if not isinstance(value, str) or re.fullmatch(r"[0-9]{1,12}", value) is None:
            raise ValueError("invalid catch-up parameter")
        return int(value)

    duration = number("duration", ("{duration}", "${duration}"))
    explicit_end = number("utcend", ("{utcend}", "${end}"))
    client_now = number("lutc", ("{lutc}", "${now}"))
    if duration is not None and not 0 < duration <= MAX_ARCHIVE_SECONDS:
        raise ValueError("invalid catch-up duration")
    if client_now is not None and client_now <= 0:
        raise ValueError("invalid client clock")
    if explicit_end is not None and duration is not None and explicit_end != start + duration:
        raise ValueError("conflicting catch-up bounds")
    if duration is not None or explicit_end is not None:
        end = explicit_end if explicit_end is not None else start + duration
    elif programme_end is not None and math.isfinite(programme_end) and programme_end > start:
        end = min(programme_end, start + MAX_ARCHIVE_SECONDS)
    else:
        end = min(now, client_now if client_now is not None else now,
                  start + MAX_ARCHIVE_SECONDS)
    if not start < end <= start + MAX_ARCHIVE_SECONDS:
        raise ValueError("invalid catch-up range")
    return float(end)
