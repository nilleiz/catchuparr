"""Format compact action rows with static, calibrated visual width estimates."""

from __future__ import annotations

import math
import unicodedata
from collections.abc import Iterable

# Canvas measurements for 17px Lato at the 350px action-message width.
# Estimates are static and do not inspect the runtime font or viewport.
TARGET_WIDTH_PX = 350.0
NBSP_WIDTH_PX = 4.35205
FALLBACK_LATIN_WIDTH_PX = 8.5
EAST_ASIAN_WIDTH_PX = 17.0
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
    if width is not None:
        return width
    if unicodedata.combining(character):
        return 0.0
    if unicodedata.east_asian_width(character) in {"W", "F"}:
        return EAST_ASIAN_WIDTH_PX
    return FALLBACK_LATIN_WIDTH_PX


def _text_width(text: str) -> float:
    return sum(_character_width(character) for character in text)


def _last_visual_line_width(text: str) -> float:
    """Simulate browser whitespace wrapping at the calibrated width."""
    line_width = 0.0
    for word in text.split():
        word_width = _text_width(word)
        if line_width and line_width + _CALIBRATED_WIDTHS[" "] + word_width > TARGET_WIDTH_PX:
            line_width = word_width
        else:
            line_width += (_CALIBRATED_WIDTHS[" "] if line_width else 0.0) + word_width
    return line_width


def format_action_message(rows: Iterable[str]) -> str:
    """Join visible rows with width-bounded NBSP padding between them."""
    lines = [str(row) for row in rows]
    if not lines:
        return ""
    parts = [lines[0]]
    for previous, current in zip(lines, lines[1:]):
        remaining_width = TARGET_WIDTH_PX - _last_visual_line_width(previous)
        padding_count = max(0, math.floor(remaining_width / NBSP_WIDTH_PX) - 1)
        parts.extend((" " + "\u00a0" * padding_count + " ", current))
    return "".join(parts)
