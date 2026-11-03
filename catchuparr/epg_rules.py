"""Pure show matching and recording-window planning for YAML ``shows`` rules."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass

MAX_REGEX_LENGTH = 200
# Bounds the work of one regex search; EPG descriptions rarely exceed this.
MAX_TEXT_LENGTH = 4096
MAX_PADDING_MINUTES = 240
MAX_NEIGHBOUR_MINUTES = 1440
MAX_ALTERNATIVES = 64
MAX_DEPTH = 6
# EPG providers round programme bounds; a shorter gap is treated as touching so
# consecutive matches are recorded without an interruption.
MERGE_GAP_SECONDS = 60.0
START_LOOKAHEAD_SECONDS = 35.0

TEXT_FIELDS = ("title", "sub_title", "description")
_OPERATORS = ("equals", "starts_with", "ends_with", "contains", "includes", "wildcard", "regex")
_COMBINATORS = ("any", "all", "not")
_NESTED_QUANTIFIER = re.compile(r"\([^()]*[+*][^()]*\)\s*(?:[+*]|\{\d*,?\d*\})")


class ShowRuleError(ValueError):
    """A show match specification is invalid."""


@dataclass(frozen=True)
class Programme:
    """One EPG entry; times are UTC epoch seconds."""

    start: float
    end: float
    title: str = ""
    sub_title: str = ""
    description: str = ""


@dataclass(frozen=True)
class Text:
    """A field normalized once: ``plain`` keeps case, ``folded`` is casefolded."""

    plain: str
    folded: str


def prepare_text(value: str) -> Text:
    plain = " ".join(unicodedata.normalize("NFKC", value or "")[:MAX_TEXT_LENGTH].split())
    return Text(plain, plain.casefold())


def programme_texts(programme: Programme) -> dict[str, Text]:
    return {name: prepare_text(getattr(programme, name)) for name in TEXT_FIELDS}


@dataclass(frozen=True)
class Neighbour:
    """Include the adjacent programme before or after a match.

    A neighbour shorter than ``min_seconds`` brings exactly one more programme;
    ``max_seconds`` caps the whole extra time before the start or after the end.
    """

    min_seconds: float | None = None
    max_seconds: float | None = None


@dataclass(frozen=True)
class ShowRule:
    """A compiled show matcher with its padding and optional start restriction."""

    index: int
    matches: Callable[..., bool]
    before_seconds: float = 0.0
    after_seconds: float = 0.0
    start_allowed: Callable[[float], bool] | None = None
    previous: Neighbour | None = None
    next: Neighbour | None = None


MAX_TITLE_LENGTH = 80


@dataclass(frozen=True)
class Show:
    """One matched programme without padding; ``rule`` is the rule that matched it."""

    start: float
    end: float
    title: str
    rule: int


@dataclass(frozen=True)
class Window:
    """A contiguous recording interval; ``rules`` holds the rules that opened it.

    ``shows`` lists the matched programmes inside it for the overviews.
    """

    start: float
    end: float
    rules: tuple[int, ...] = ()
    shows: tuple[Show, ...] = ()


def _merged_shows(*groups: Iterable[Show]) -> tuple[Show, ...]:
    return tuple(sorted({show for group in groups for show in group},
                        key=lambda show: (show.start, show.end, show.rule, show.title)))


def normalize_text(value: str, *, case_sensitive: bool = False) -> str:
    text = prepare_text(value)
    return text.plain if case_sensitive else text.folded


def _values(value, *, field: str) -> list[str]:
    items = value if isinstance(value, list) else [value]
    if not items or len(items) > MAX_ALTERNATIVES or any(not isinstance(i, str) or not i for i in items):
        raise ShowRuleError(f"field {field}: expected a text or a non-empty list of texts")
    return items


def _wildcard(pattern: str, flags: int) -> re.Pattern:
    body = "".join(".*" if c == "*" else "." if c == "?" else re.escape(c) for c in pattern)
    return re.compile(body, flags | re.DOTALL)


def _operator(name: str, value, *, field: str, case_sensitive: bool) -> Callable[[Text], bool]:
    values = _values(value, field=field)
    flags = 0 if case_sensitive else re.IGNORECASE
    if name == "regex":
        patterns = []
        for text in values:
            if len(text) > MAX_REGEX_LENGTH:
                raise ShowRuleError(f"field {field}: regex is longer than {MAX_REGEX_LENGTH} characters")
            if _NESTED_QUANTIFIER.search(text):
                raise ShowRuleError(f"field {field}: regex with nested repetition is not allowed")
            try:
                patterns.append(re.compile(text, flags))
            except re.error:
                raise ShowRuleError(f"field {field}: invalid regular expression") from None
        # Casefolding can change letters (for example a sharp s), so a regex
        # runs on the case-preserving form and ignores case through its flag.
        return _first_true([pattern.search for pattern in patterns], lambda text: text.plain)
    if name == "wildcard":
        patterns = [_wildcard(normalize_text(t, case_sensitive=case_sensitive), flags) for t in values]
        return _first_true([pattern.fullmatch for pattern in patterns], _side_of(case_sensitive))
    needles = [normalize_text(t, case_sensitive=case_sensitive) for t in values]
    side = _side_of(case_sensitive)
    if name == "equals":
        return lambda text, side=side: side(text) in needles
    if name == "contains" or name == "includes":
        if len(needles) == 1:
            needle = needles[0]
            return lambda text, side=side: needle in side(text)
        return lambda text, side=side: _any_in(side(text), needles)
    method = "startswith" if name == "starts_with" else "endswith"
    if len(needles) == 1:
        needle = needles[0]
        return lambda text, side=side: getattr(side(text), method)(needle)
    # str.startswith and str.endswith accept a tuple of alternatives.
    alternatives = tuple(needles)
    return lambda text, side=side: getattr(side(text), method)(alternatives)


def _side(text: Text, case_sensitive: bool) -> str:
    return text.plain if case_sensitive else text.folded


def _side_of(case_sensitive: bool) -> Callable[[Text], str]:
    return (lambda text: text.plain) if case_sensitive else (lambda text: text.folded)


def _any_in(haystack: str, needles: Sequence[str]) -> bool:
    for needle in needles:
        if needle in haystack:
            return True
    return False


def _first_true(tests: Sequence[Callable[[str], object]], side: Callable[[Text], str]) -> Callable[[Text], bool]:
    """True when any of ``tests`` accepts the text; loops instead of a generator for speed."""
    if len(tests) == 1:
        test = tests[0]
        return lambda text: bool(test(side(text)))

    def check(text):
        value = side(text)
        for test in tests:
            if test(value):
                return True
        return False
    return check


def _any_of(parts: Sequence[Callable[[Text], bool]]) -> Callable[[Text], bool]:
    if len(parts) == 1:
        return parts[0]

    def check(text):
        for part in parts:
            if part(text):
                return True
        return False
    return check


def _all_of(parts: Sequence[Callable[[Text], bool]]) -> Callable[[Text], bool]:
    if len(parts) == 1:
        return parts[0]

    def check(text):
        for part in parts:
            if not part(text):
                return False
        return True
    return check


def compile_text_condition(spec, *, field: str, case_sensitive: bool = False,
                           _depth: int = 0) -> Callable[[Text], bool]:
    """Compile a condition mapping; several keys in one mapping must all hold."""
    if _depth > MAX_DEPTH:
        raise ShowRuleError(f"field {field}: conditions are nested too deeply")
    if not isinstance(spec, Mapping) or not spec:
        raise ShowRuleError(f"field {field}: expected a condition mapping")
    unknown = set(spec) - set(_OPERATORS) - set(_COMBINATORS)
    if unknown:
        raise ShowRuleError(f"field {field}: unknown condition key")
    parts: list[Callable[[Text], bool]] = []
    for key, value in spec.items():
        path = f"{field}.{key}"
        if key in _OPERATORS:
            parts.append(_operator(key, value, field=path, case_sensitive=case_sensitive))
        elif key == "not":
            inner = compile_text_condition(value, field=path, case_sensitive=case_sensitive, _depth=_depth + 1)
            parts.append(lambda text, inner=inner: not inner(text))
        else:
            if not isinstance(value, list) or not value or len(value) > MAX_ALTERNATIVES:
                raise ShowRuleError(f"field {path}: expected a non-empty list of conditions")
            inner = [
                compile_text_condition(item, field=f"{path}[{n}]", case_sensitive=case_sensitive, _depth=_depth + 1)
                for n, item in enumerate(value)
            ]
            parts.append(_any_of(inner) if key == "any" else _all_of(inner))
    return _all_of(parts)


def compile_match(spec, *, field: str = "match") -> Callable[..., bool]:
    """Compile a ``match`` mapping: fields combine with AND."""
    if not isinstance(spec, Mapping):
        raise ShowRuleError(f"field {field}: expected a mapping")
    allowed = set(TEXT_FIELDS) | {"min_minutes", "max_minutes", "case_sensitive"}
    if set(spec) - allowed:
        raise ShowRuleError(f"field {field}: unknown key")
    case_sensitive = spec.get("case_sensitive", False)
    if not isinstance(case_sensitive, bool):
        raise ShowRuleError(f"field {field}.case_sensitive: expected true or false")
    if not any(name in spec for name in TEXT_FIELDS):
        raise ShowRuleError(f"field {field}: specify at least one of title, sub_title or description")
    limits = {}
    for key in ("min_minutes", "max_minutes"):
        if key in spec:
            value = spec[key]
            if type(value) is not int or not 0 < value <= 24 * 60:
                raise ShowRuleError(f"field {field}.{key}: expected minutes between 1 and 1440")
            limits[key] = value * 60.0
    if len(limits) == 2 and limits["min_minutes"] > limits["max_minutes"]:
        raise ShowRuleError(f"field {field}: min_minutes exceeds max_minutes")
    conditions = [
        (name, compile_text_condition(spec[name], field=f"{field}.{name}", case_sensitive=case_sensitive))
        for name in TEXT_FIELDS if name in spec
    ]

    def matches(programme: Programme, texts: Mapping[str, Text] | None = None) -> bool:
        length = programme.end - programme.start
        if length < limits.get("min_minutes", 0.0) or length > limits.get("max_minutes", float("inf")):
            return False
        if texts is None:
            texts = programme_texts(programme)
        for name, condition in conditions:
            if not condition(texts[name]):
                return False
        return True

    return matches


def compile_padding(spec, *, field: str = "padding") -> tuple[float, float]:
    """Return (before, after) in seconds from a ``{before, after}`` minutes mapping."""
    if spec is None:
        return 0.0, 0.0
    if not isinstance(spec, Mapping) or set(spec) - {"before", "after"}:
        raise ShowRuleError(f"field {field}: expected only before and after")
    result = []
    for key in ("before", "after"):
        value = spec.get(key, 0)
        if type(value) is not int or not 0 <= value <= MAX_PADDING_MINUTES:
            raise ShowRuleError(f"field {field}.{key}: expected minutes between 0 and {MAX_PADDING_MINUTES}")
        result.append(value * 60.0)
    return result[0], result[1]


def compile_neighbour(spec, *, field: str) -> Neighbour | None:
    """``true``/``false`` or a mapping with optional ``min`` and ``max`` minutes."""
    if spec is None or spec is False:
        return None
    if spec is True:
        return Neighbour()
    if not isinstance(spec, Mapping) or set(spec) - {"min", "max"}:
        raise ShowRuleError(f"field {field}: expected true or a mapping with min and max")
    limits = {}
    for key in ("min", "max"):
        if key in spec:
            value = spec[key]
            if type(value) is not int or not 1 <= value <= MAX_NEIGHBOUR_MINUTES:
                raise ShowRuleError(
                    f"field {field}.{key}: expected minutes between 1 and {MAX_NEIGHBOUR_MINUTES}"
                )
            limits[key] = value * 60.0
    if len(limits) == 2 and limits["min"] > limits["max"]:
        raise ShowRuleError(f"field {field}: min exceeds max")
    return Neighbour(limits.get("min"), limits.get("max"))


def neighbour_snapshot(neighbour: Neighbour | None) -> dict[str, int] | None:
    """The minutes mapping stored in the applied snapshot; ``{}`` means the whole programme."""
    if neighbour is None:
        return None
    return {
        key: int(value // 60) for key, value in (("min", neighbour.min_seconds), ("max", neighbour.max_seconds))
        if value is not None
    }


def _usable(programme: Programme) -> bool:
    return programme.end > programme.start


def _adjacent_before(ordered: Sequence[Programme], index: int, boundary: float) -> int | None:
    """The programme with the latest start before ``boundary`` that ends at it."""
    for position in range(index - 1, -1, -1):
        candidate = ordered[position]
        if candidate.start >= boundary or not _usable(candidate):
            continue
        return position if abs(candidate.end - boundary) <= MERGE_GAP_SECONDS else None
    return None


def _adjacent_after(ordered: Sequence[Programme], index: int, boundary: float) -> int | None:
    """The first programme that starts at ``boundary``."""
    for position in range(index + 1, len(ordered)):
        candidate = ordered[position]
        if candidate.start < boundary - MERGE_GAP_SECONDS or not _usable(candidate):
            continue
        return position if candidate.start <= boundary + MERGE_GAP_SECONDS else None
    return None


def _lead_seconds(ordered: Sequence[Programme], index: int, neighbour: Neighbour) -> float:
    show = ordered[index]
    first = _adjacent_before(ordered, index, show.start)
    if first is None:
        return 0.0
    chain_start = ordered[first].start
    if neighbour.min_seconds is not None and ordered[first].end - ordered[first].start < neighbour.min_seconds:
        second = _adjacent_before(ordered, first, ordered[first].start)
        if second is not None:
            chain_start = ordered[second].start
    lead = max(0.0, show.start - chain_start)
    return lead if neighbour.max_seconds is None else min(lead, neighbour.max_seconds)


def _tail_seconds(ordered: Sequence[Programme], index: int, neighbour: Neighbour) -> float:
    show = ordered[index]
    first = _adjacent_after(ordered, index, show.end)
    if first is None:
        return 0.0
    chain_end = ordered[first].end
    if neighbour.min_seconds is not None and ordered[first].end - ordered[first].start < neighbour.min_seconds:
        second = _adjacent_after(ordered, first, ordered[first].end)
        if second is not None:
            chain_end = ordered[second].end
    tail = max(0.0, chain_end - show.end)
    return tail if neighbour.max_seconds is None else min(tail, neighbour.max_seconds)


def recording_span(ordered: Sequence[Programme], index: int, rule: ShowRule) -> tuple[float, float]:
    """Start and end one rule records for ``ordered[index]``, neighbours and padding included."""
    programme = ordered[index]
    lead = _lead_seconds(ordered, index, rule.previous) if rule.previous is not None else 0.0
    tail = _tail_seconds(ordered, index, rule.next) if rule.next is not None else 0.0
    return programme.start - lead - rule.before_seconds, programme.end + tail + rule.after_seconds


def _ordered(programmes: Iterable[Programme]) -> list[Programme]:
    return sorted(programmes, key=lambda p: (p.start, p.end))


def merge_windows(windows: Iterable[Window], *, gap: float = MERGE_GAP_SECONDS) -> list[Window]:
    """Union overlapping or touching windows into contiguous ones."""
    merged: list[Window] = []
    for window in sorted(windows, key=lambda w: (w.start, w.end)):
        if merged and window.start <= merged[-1].end + gap:
            last = merged[-1]
            rules = last.rules if window.start > last.start else tuple(sorted({*last.rules, *window.rules}))
            merged[-1] = Window(
                last.start, max(last.end, window.end), rules, _merged_shows(last.shows, window.shows),
            )
        else:
            merged.append(window)
    return merged


def plan_windows(programmes: Iterable[Programme], rules: Sequence[ShowRule]) -> list[Window]:
    """Recording windows of one channel for every programme any rule matches."""
    windows = []
    ordered = _ordered(programmes)
    for index, programme in enumerate(ordered):
        if not programme.end > programme.start:
            continue
        texts = programme_texts(programme)
        for rule in rules:
            if rule.start_allowed is not None and not rule.start_allowed(programme.start):
                continue
            if rule.matches(programme, texts):
                start, end = recording_span(ordered, index, rule)
                windows.append(Window(
                    start,
                    end,
                    (rule.index,),
                    (Show(programme.start, programme.end, programme.title[:MAX_TITLE_LENGTH], rule.index),),
                ))
    return merge_windows(windows)


def preview_matches(programmes: Iterable[Programme], rules: Sequence[ShowRule], *, now: float,
                    limit: int = 20) -> list[dict]:
    """Upcoming or running programmes any rule matches, soonest first, for the preview."""
    found = []
    ordered = _ordered(programmes)
    for index, programme in enumerate(ordered):
        if programme.end <= now or not programme.end > programme.start:
            continue
        texts = programme_texts(programme)
        hit_rules = [
            rule for rule in rules
            if (rule.start_allowed is None or rule.start_allowed(programme.start))
            and rule.matches(programme, texts)
        ]
        if hit_rules:
            spans = [recording_span(ordered, index, rule) for rule in hit_rules]
            found.append({
                "title": programme.title, "sub_title": programme.sub_title,
                "start": programme.start, "end": programme.end,
                "rules": [rule.index for rule in hit_rules],
                "record_start": min(span[0] for span in spans),
                "record_end": max(span[1] for span in spans),
                "neighbours": any(rule.previous is not None or rule.next is not None for rule in hit_rules),
            })
            if len(found) >= limit:
                break
    return found


def freeze_running(previous: Iterable[Window], planned: Iterable[Window], now: float) -> list[Window]:
    """Keep a running window's start and never shorten its end.

    A refresh of the EPG may replace or drop the programme that opened a window
    already recording; the recording must not be cut off by that.
    """
    # A window counts as running from the moment admission opens it.
    running = [w for w in previous if w.start - START_LOOKAHEAD_SECONDS <= now < w.end]
    return [w for w in merge_windows([*planned, *running]) if w.end > now]


def window_active(windows: Iterable[Window], now: float, *, lookahead: float = START_LOOKAHEAD_SECONDS) -> bool:
    return any(w.start - lookahead <= now < w.end for w in windows)


def next_window_start(windows: Iterable[Window], now: float) -> float | None:
    starts = [w.start for w in windows if w.start > now]
    return min(starts) if starts else None
