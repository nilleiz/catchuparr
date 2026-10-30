"""Format action rows for Dispatcharr's plain-text notification toasts.

Dispatcharr renders an action's ``message`` as a single text node with normal
white-space handling, inside a 350px Mantine notification capped at 200px
height. Newlines collapse and overflow is clipped, so rows are separated by a
run of ideographic spaces: the run is wider than the toast and hangs at the end
of a line, which starts every row on its own line at any toast width. Rows that
would not fit one toast are delivered as numbered follow-up toasts.
"""

from __future__ import annotations

import logging
import threading
import unicodedata
from collections.abc import Callable, Iterable, Sequence
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

ROW_SEPARATOR = "\u3000" * 40
# Conservative estimates for the 14px description text of a 350px toast.
TEXT_WIDTH_PX = 230.0
MAX_LINES_PER_TOAST = 7
FOLLOW_UP_DELAY_SECONDS = 0.8
_FONT_SCALE = 14.0 / 17.0
_PAGE_PREFIX_ALLOWANCE = "00/00 · "
FALLBACK_LATIN_WIDTH_PX = 8.5
EAST_ASIAN_WIDTH_PX = 17.0
# Canvas measurements for 17px Lato, scaled to the toast font at lookup.
_CALIBRATED_WIDTHS = {
    ' ': 4.35205,
    '!': 4.57306,
    '"': 6.29857,
    '#': 9.86012,
    '$': 9.86012,
    '%': 13.62566,
    '&': 12.10414,
    "'": 3.46805,
    '(': 4.53905,
    ')': 4.53905,
    '*': 7.22508,
    '+': 9.86012,
    ',': 3.85904,
    '-': 6.31557,
    '.': 4.01205,
    '/': 7.68410,
    '0': 9.86012,
    '1': 9.86012,
    '2': 9.86012,
    '3': 9.86012,
    '4': 9.86012,
    '5': 9.86012,
    '6': 9.86012,
    '7': 9.86012,
    '8': 9.86012,
    '9': 9.86012,
    ':': 4.25005,
    ';': 4.44556,
    '<': 9.86012,
    '=': 9.86012,
    '>': 9.86012,
    '?': 7.60759,
    '@': 14.22067,
    'A': 11.50914,
    'B': 10.99063,
    'C': 11.35614,
    'D': 12.92865,
    'E': 9.81761,
    'F': 9.61362,
    'G': 12.41864,
    'H': 12.97964,
    'I': 4.76006,
    'J': 7.18259,
    'K': 11.26263,
    'L': 8.72960,
    'M': 15.78468,
    'N': 12.97964,
    'O': 13.60866,
    'P': 10.20862,
    'Q': 13.60866,
    'R': 10.65062,
    'S': 9.22261,
    'T': 10.03862,
    'U': 12.50365,
    'V': 11.50914,
    'W': 17.60370,
    'X': 11.03313,
    'Y': 10.60812,
    'Z': 10.23412,
    '[': 5.20206,
    '\\': 7.68410,
    ']': 5.20206,
    '^': 9.86012,
    '_': 7.80309,
    '`': 6.80008,
    'a': 8.44910,
    'b': 9.52011,
    'c': 8.11760,
    'd': 9.52011,
    'e': 8.97610,
    'f': 5.95857,
    'g': 8.84010,
    'h': 9.48611,
    'i': 4.08005,
    'j': 4.08005,
    'k': 8.63611,
    'l': 4.01205,
    'm': 13.98267,
    'n': 9.48611,
    'o': 9.63911,
    'p': 9.52861,
    'q': 9.52011,
    'r': 6.18808,
    's': 7.36108,
    't': 6.09457,
    'u': 9.47762,
    'v': 8.77211,
    'w': 13.35365,
    'x': 8.46609,
    'y': 8.76360,
    'z': 7.68410,
    '{': 5.11707,
    '|': 4.25854,
    '}': 5.11707,
    '~': 9.86012,
    'À': 11.50914,
    'Á': 11.50914,
    'Ä': 11.50914,
    'Å': 11.50914,
    'Æ': 15.81868,
    'Ç': 11.35614,
    'È': 9.81761,
    'É': 9.81761,
    'Ñ': 12.97964,
    'Ö': 13.60866,
    'Ü': 12.50365,
    'ß': 9.86012,
    'à': 8.44910,
    'á': 8.44910,
    'ä': 8.44910,
    'å': 8.44910,
    'æ': 13.68517,
    'ç': 8.12610,
    'è': 8.97610,
    'é': 8.97610,
    'ñ': 9.48611,
    'ö': 9.63911,
    'ø': 9.63062,
    'ü': 9.47762,
    '–': 9.86012,
    '—': 13.48965,
    '…': 12.75015,
    '€': 9.86012,
    '’': 3.62955,
    '\xa0': 4.35205,
}


def _character_width(character: str) -> float:
    width = _CALIBRATED_WIDTHS.get(character)
    if width is None:
        if unicodedata.combining(character):
            return 0.0
        if unicodedata.east_asian_width(character) in {"W", "F"}:
            width = EAST_ASIAN_WIDTH_PX
        else:
            width = FALLBACK_LATIN_WIDTH_PX
    return width * _FONT_SCALE


def wrapped_line_count(text: str, width: float = TEXT_WIDTH_PX) -> int:
    """Estimate visual lines, including CJK break opportunities."""
    lines = 1
    line_width = 0.0
    space = _character_width(" ")
    for word in text.split():
        units = []
        for character in word:
            if unicodedata.east_asian_width(character) in {"W", "F"}:
                units.append((character, _character_width(character)))
            elif units and units[-1][0] == "\0":
                units[-1] = ("\0", units[-1][1] + _character_width(character))
            else:
                units.append(("\0", _character_width(character)))
        for index, (kind, unit_width) in enumerate(units):
            if index == 0 and line_width:
                line_width += space
                if line_width + unit_width > width:
                    lines += 1
                    line_width = 0.0
            break_before = kind != "\0" or (index > 0 and units[index - 1][0] != "\0")
            if break_before and line_width and line_width + unit_width > width:
                lines += 1
                line_width = unit_width
            else:
                line_width += unit_width
            while line_width > width:
                # Assume an unbreakable run longer than a line wraps anywhere.
                lines += 1
                line_width -= width
    return lines


def join_rows(rows: Iterable[str]) -> str:
    """Join rows so each starts on its own visual line in the toast."""
    return ROW_SEPARATOR.join(str(row) for row in rows)


def split_group_row(prefix: str, names: Sequence[str],
                    max_lines: int = MAX_LINES_PER_TOAST) -> list[str]:
    """Render ``prefix › a, b, …`` as rows that each fit one toast."""
    rows: list[str] = []
    current: list[str] = []
    for name in names:
        candidate = current + [name]
        if current and wrapped_line_count(f"{prefix} › {', '.join(candidate)}") > max_lines:
            rows.append(f"{prefix} › {', '.join(current)}")
            candidate = [name]
        current = candidate
    if current or not rows:
        rows.append(f"{prefix} › {', '.join(current)}")
    return rows


def paginate_rows(rows: Iterable[str], max_lines: int = MAX_LINES_PER_TOAST) -> list[list[str]]:
    """Split rows into toast pages without splitting a row.

    The first row of the first page is measured with room for its ``n/N``
    prefix. A row taller than a page gets a page of its own.
    """
    pages: list[list[str]] = []
    page: list[str] = []
    used = 0
    for row in (str(row) for row in rows):
        measured = _PAGE_PREFIX_ALLOWANCE + row if not pages and not page else row
        lines = wrapped_line_count(measured)
        if page and used + lines > max_lines:
            pages.append(page)
            page, used = [], 0
        page.append(row)
        used += lines
    if page:
        pages.append(page)
    return pages


def _send_follow_ups(notifications: list[dict], sender: Callable[[dict], None]) -> None:
    for notification in notifications:
        sender(notification)


def _send_follow_ups_later(notifications: list[dict], sender: Callable[[dict], None]) -> None:
    try:
        _send_follow_ups(notifications, sender)
    except Exception:
        logger.warning("Unable to push follow-up Catchuparr action toasts", exc_info=True)


def _default_sender() -> Callable[[dict], None]:
    from core.utils import send_websocket_notification

    return send_websocket_notification


def deliver_action_message(
    rows: Iterable[str],
    *,
    title: str,
    summary: str,
    run_key: str,
    sender: Callable[[dict], None] | None = None,
    delay_seconds: float | None = None,
) -> str:
    """Return a short summary for the action toast and push the rows.

    Every page of rows is pushed as an admin-only Dispatcharr system
    notification, marked dismissed so it shows only as a toast and never as an
    unread entry. If the push is unavailable, the summary toast carries as many
    rows as fit and names how many are missing.
    """
    pages = paginate_rows(rows)
    if not pages:
        return summary
    total = len(pages)
    created_at = datetime.now(timezone.utc).isoformat()
    notifications = [
        {
            "notification_key": f"catchuparr-{run_key}-{number}",
            "notification_type": "info",
            "priority": "high",
            "admin_only": True,
            "is_active": True,
            "is_dismissed": True,
            "action_data": {},
            "created_at": created_at,
            "title": f"Catchuparr – {title}" + (f" {number}/{total}" if total > 1 else ""),
            "message": join_rows(page),
        }
        for number, page in enumerate(pages, start=1)
    ]
    if delay_seconds is None:
        delay_seconds = FOLLOW_UP_DELAY_SECONDS
    try:
        send = sender or _default_sender()
        if delay_seconds > 0:
            # Let the summary toast appear before the detail toasts.
            timer = threading.Timer(delay_seconds, _send_follow_ups_later, (notifications, send))
            timer.daemon = True
            timer.start()
        else:
            _send_follow_ups(notifications, send)
    except Exception:
        logger.debug("Unable to push detail action toasts", exc_info=True)
        shown = [summary] + [row for page in pages for row in page]
        missing = 0
        while sum(map(wrapped_line_count, shown)) > MAX_LINES_PER_TOAST and len(shown) > 2:
            shown.pop()
            missing += 1
        if missing:
            while (sum(map(wrapped_line_count, shown + [f"+{missing} more not shown"]))
                   > MAX_LINES_PER_TOAST and len(shown) > 2):
                shown.pop()
                missing += 1
            shown.append(f"+{missing} more not shown")
        return join_rows(shown)
    return f"{summary} Details in {total} {'toast' if total == 1 else 'toasts'}."
