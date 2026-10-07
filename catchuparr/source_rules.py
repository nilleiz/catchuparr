"""Pure parser and candidate ranker for per-channel source rules."""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping


class SourceRuleError(ValueError):
    """Raised when source rules or the supplied channel/account data are invalid."""


@dataclass(frozen=True)
class SourcePolicy:
    """Resolved immutable source policy for one channel."""

    mode: str
    account_ids: frozenset[str]
    priorities: tuple[tuple[str, int], ...]
    known_account_ids: frozenset[str]

    def priority_for(self, account_id: str) -> int:
        return dict(self.priorities).get(account_id, 0)


_NUMBER = re.compile(r"(?:\d+(?:\.\d*)?|\.\d+)")
_RANGE = re.compile(rf"^({_NUMBER.pattern})\s*-\s*({_NUMBER.pattern})$")
_PRIORITY = re.compile(r"^\s*(\"(?:[^\"\\]|\\.)*\")\s*:\s*(-?\d+)")
_MODES = {"include-only", "exclude-only", "priority", "unchanged"}


@dataclass(frozen=True)
class _Rule:
    selector_kind: str
    selector_values: tuple[str, ...]
    mode: str
    account_ids: frozenset[str]
    priorities: tuple[tuple[str, int], ...]
    is_global: bool = False


def _split_fields(line: str) -> list[str]:
    fields: list[str] = []
    start = 0
    quoted = False
    escaped = False
    for index, char in enumerate(line):
        if escaped:
            escaped = False
            continue
        if quoted and char == "\\":
            escaped = True
        elif char == '"':
            quoted = not quoted
        elif char == "|" and not quoted:
            fields.append(line[start:index].strip())
            start = index + 1
    if quoted or escaped:
        raise SourceRuleError("unterminated quoted value")
    fields.append(line[start:].strip())
    if any(not field for field in fields):
        raise SourceRuleError("rule fields cannot be empty")
    return fields


def _quoted_value(value: str, field_name: str) -> str:
    values = _quoted_list(value, field_name)
    if len(values) != 1:
        raise SourceRuleError(f"{field_name} selector must contain exactly one quoted value")
    return values[0]


def _unescape(value: str, field_name: str) -> str:
    result: list[str] = []
    escaped = False
    for char in value:
        if escaped:
            if char not in {'"', "\\"}:
                raise SourceRuleError(f"unsupported escape in {field_name}")
            result.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        else:
            result.append(char)
    if escaped:
        raise SourceRuleError(f"unterminated escape in {field_name}")
    text = "".join(result)
    if not text:
        raise SourceRuleError(f"{field_name} values cannot be empty")
    return text


def _quoted_list(value: str, field_name: str) -> tuple[str, ...]:
    remaining = value.strip()
    result: list[str] = []
    while remaining:
        if not remaining.startswith('"'):
            raise SourceRuleError(f'{field_name} values must be quoted, for example {field_name}="Name"')
        escaped = False
        end = None
        for index in range(1, len(remaining)):
            char = remaining[index]
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                end = index
                break
        if end is None:
            raise SourceRuleError(f"unterminated quoted value in {field_name}")
        result.append(_unescape(remaining[1:end], field_name))
        remaining = remaining[end + 1 :].strip()
        if not remaining:
            break
        if not remaining.startswith(","):
            raise SourceRuleError(f"expected a comma between {field_name} values")
        remaining = remaining[1:].strip()
        if not remaining:
            raise SourceRuleError(f"{field_name} list cannot end with a comma")
    if not result:
        raise SourceRuleError(f"{field_name} list cannot be empty")
    if len(set(result)) != len(result):
        raise SourceRuleError(f"{field_name} list contains duplicate names")
    return tuple(result)


def _priority_list(value: str) -> tuple[tuple[str, int], ...]:
    remaining = value.strip()
    pairs: list[tuple[str, int]] = []
    while remaining:
        match = _PRIORITY.match(remaining)
        if match is None:
            raise SourceRuleError('priority values must look like priority="Name":100')
        name = _unescape(match.group(1)[1:-1], "priority")
        pairs.append((name, int(match.group(2))))
        remaining = remaining[match.end() :].strip()
        if not remaining:
            break
        if not remaining.startswith(","):
            raise SourceRuleError("expected a comma between priority values")
        remaining = remaining[1:].strip()
        if not remaining:
            raise SourceRuleError("priority list cannot end with a comma")
    if not pairs:
        raise SourceRuleError("priority list cannot be empty")
    if len({name for name, _ in pairs}) != len(pairs):
        raise SourceRuleError("priority list contains duplicate account names")
    return tuple(pairs)


def _parse_selector(selector: str) -> tuple[str, tuple[str, ...], bool]:
    selector = selector.strip()
    if selector == "*":
        return "all", (), True
    if selector.startswith("number:"):
        parts = selector[len("number:") :].split(",")
        if not parts or any(not part.strip() for part in parts):
            raise SourceRuleError("number selector cannot be empty")
        normalized: list[str] = []
        for part in parts:
            token = part.strip()
            range_match = _RANGE.fullmatch(token)
            if range_match:
                low, high = Decimal(range_match.group(1)), Decimal(range_match.group(2))
                if low > high:
                    raise SourceRuleError(f"number range {token!r} is reversed")
                normalized.append(f"{low}..{high}")
            elif _NUMBER.fullmatch(token):
                normalized.append(str(Decimal(token)))
            else:
                raise SourceRuleError(f"invalid channel number selector {token!r}")
        return "number", tuple(normalized), False
    for kind in ("name", "group"):
        prefix = f"{kind}:"
        if selector.startswith(prefix):
            return kind, (_quoted_value(selector[len(prefix) :], kind),), False
    raise SourceRuleError(f"invalid selector {selector!r}")


def _parse_rule(
    line: str,
    accounts_by_name: Mapping[str, str],
    ambiguous_account_names: frozenset[str],
) -> _Rule:
    fields = _split_fields(line)
    kind, values, is_global = _parse_selector(fields[0])
    options: dict[str, str] = {}
    for field in fields[1:]:
        if "=" not in field:
            raise SourceRuleError(f"invalid rule option {field!r}")
        key, value = (piece.strip() for piece in field.split("=", 1))
        if key not in {"mode", "m3u", "priority"}:
            raise SourceRuleError(f"unknown rule option {key!r}")
        if key in options:
            raise SourceRuleError(f"duplicate rule option {key!r}")
        options[key] = value
    mode = options.get("mode")
    if mode not in _MODES:
        raise SourceRuleError(f"mode must be one of {', '.join(sorted(_MODES))}")
    if mode in {"include-only", "exclude-only"} and "m3u" not in options:
        raise SourceRuleError(f"mode={mode} requires an m3u account list")
    if mode == "unchanged" and ("m3u" in options or "priority" in options):
        raise SourceRuleError("mode=unchanged cannot include m3u or priority values")
    if "priority" in options and mode != "priority":
        raise SourceRuleError("priority values require mode=priority")

    names = _quoted_list(options["m3u"], "m3u") if "m3u" in options else ()
    priority_names = _priority_list(options["priority"]) if "priority" in options else ()
    referenced_names = set(names) | {name for name, _ in priority_names}
    ambiguous = sorted(referenced_names & ambiguous_account_names)
    if ambiguous:
        raise SourceRuleError(f"ambiguous M3U account name(s): {', '.join(ambiguous)}")
    unknown = sorted(referenced_names - accounts_by_name.keys())
    if unknown:
        raise SourceRuleError(f"unknown M3U account name(s): {', '.join(unknown)}")
    account_ids = frozenset(accounts_by_name[name] for name in names)
    priorities = tuple((accounts_by_name[name], score) for name, score in priority_names)
    return _Rule(kind, values, mode, account_ids, priorities, is_global)


def _channel_number(value: Any, channel_uuid: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise SourceRuleError(f"channel {channel_uuid!r} has an invalid number") from None
    if not number.is_finite() or number < 0:
        raise SourceRuleError(f"channel {channel_uuid!r} has an invalid number")
    return number


def _resolve_channels(rule: _Rule, channels: list[dict[str, Any]]) -> set[str]:
    if rule.is_global:
        return {str(channel["uuid"]) for channel in channels}
    if rule.selector_kind == "number":
        numbered_channels = []
        for channel in channels:
            raw_number = channel.get("number")
            if raw_number is None or (isinstance(raw_number, str) and not raw_number.strip()):
                continue
            number = _channel_number(raw_number, str(channel["uuid"]))
            numbered_channels.append((channel, number))
        result: set[str] = set()
        for token in rule.selector_values:
            if ".." in token:
                low_text, high_text = token.split("..", 1)
                low, high = Decimal(low_text), Decimal(high_text)
                matches = [
                    (channel, number)
                    for channel, number in numbered_channels
                    if low <= number <= high
                ]
                if not matches:
                    raise SourceRuleError(f"number range {low}..{high} matches no channels")
                by_number: dict[Decimal, list[str]] = {}
                for channel, number in matches:
                    by_number.setdefault(number, []).append(str(channel["uuid"]))
                if any(len(uuids) > 1 for uuids in by_number.values()):
                    raise SourceRuleError(f"number range {low}..{high} is ambiguous")
                result.update(str(channel["uuid"]) for channel, _number in matches)
            else:
                wanted = Decimal(token)
                matches = [
                    channel
                    for channel, number in numbered_channels
                    if number == wanted
                ]
                if not matches:
                    raise SourceRuleError(f"channel number {wanted} matches no channels")
                if len(matches) > 1:
                    raise SourceRuleError(f"channel number {wanted} is ambiguous")
                result.add(str(matches[0]["uuid"]))
        return result

    field = "name" if rule.selector_kind == "name" else "group"
    wanted = rule.selector_values[0]
    matches = [channel for channel in channels if str(channel.get(field, "")) == wanted]
    if not matches:
        raise SourceRuleError(f"channel {rule.selector_kind} {wanted!r} matches no channels")
    if rule.selector_kind == "name" and len(matches) > 1:
        raise SourceRuleError(f"channel name {wanted!r} is ambiguous")
    return {str(channel["uuid"]) for channel in matches}


def _channel_rows(channels: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for channel in channels:
        if "uuid" not in channel or not str(channel["uuid"]):
            raise SourceRuleError("each channel must have a non-empty uuid")
        channel_uuid = str(channel["uuid"])
        if channel_uuid in seen:
            raise SourceRuleError(f"duplicate channel uuid {channel_uuid!r}")
        seen.add(channel_uuid)
        rows.append(dict(channel))
    return rows


def _account_index(
    accounts: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, str], frozenset[str], frozenset[str]]:
    ids: set[str] = set()
    names: dict[str, str] = {}
    ambiguous_names: set[str] = set()
    for account in accounts:
        if "id" not in account or not str(account["id"]):
            raise SourceRuleError("each account must have a non-empty id")
        account_id = str(account["id"])
        if account_id in ids:
            raise SourceRuleError(f"duplicate account id {account_id!r}")
        ids.add(account_id)
        name = account.get("name")
        if not isinstance(name, str) or not name:
            raise SourceRuleError(f"account {account_id!r} must have a non-empty exact name")
        if name in names:
            ambiguous_names.add(name)
        else:
            names[name] = account_id
    names = {name: account_id for name, account_id in names.items() if name not in ambiguous_names}
    return names, frozenset(ids), frozenset(ambiguous_names)


def compile_source_rules(
    text: str,
    channels: Iterable[Mapping[str, Any]],
    accounts: Iterable[Mapping[str, Any]],
) -> dict[str, SourcePolicy]:
    """Parse rules and resolve them to channel UUIDs and source account IDs.

    Rules are newline separated. A global ``*`` rule supplies a fallback to
    every channel; a specific rule replaces that policy for its matched UUIDs.
    Specific selectors may not overlap one another.
    """
    channel_rows = _channel_rows(channels)
    account_names, known_ids, ambiguous_names = _account_index(accounts)
    parsed: list[_Rule] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            parsed.append(_parse_rule(stripped, account_names, ambiguous_names))
        except SourceRuleError as exc:
            raise SourceRuleError(f"line {line_number}: {exc}") from exc
    if not parsed:
        raise SourceRuleError("at least one source rule is required")

    global_rules = [rule for rule in parsed if rule.is_global]
    if len(global_rules) > 1:
        raise SourceRuleError("only one global * rule is allowed")
    global_rule = global_rules[0] if global_rules else None
    resolved: list[tuple[_Rule, set[str]]] = []
    occupied: set[str] = set()
    for rule in parsed:
        if rule.is_global:
            continue
        channel_uuids = _resolve_channels(rule, channel_rows)
        overlap = occupied & channel_uuids
        if overlap:
            first = sorted(overlap)[0]
            raise SourceRuleError(f"specific source rules overlap at channel {first!r}")
        occupied.update(channel_uuids)
        resolved.append((rule, channel_uuids))

    policies: dict[str, SourcePolicy] = {}
    if global_rule is not None:
        fallback = SourcePolicy(global_rule.mode, global_rule.account_ids, global_rule.priorities, known_ids)
        policies.update((str(channel["uuid"]), fallback) for channel in channel_rows)
    for rule, channel_uuids in resolved:
        policy = SourcePolicy(rule.mode, rule.account_ids, rule.priorities, known_ids)
        for channel_uuid in channel_uuids:
            policies[channel_uuid] = policy
    return policies


def rank_candidates(
    policy: SourcePolicy,
    streams: Iterable[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    """Filter and stably rank assigned stream DTOs for a resolved policy."""
    ordered_rows = list(enumerate(streams))
    try:
        ordered = sorted(
            ordered_rows,
            key=lambda item: (Decimal(str(item[1].get("order", item[0]))), item[0]),
        )
    except (InvalidOperation, ValueError):
        raise SourceRuleError("each stream must have a numeric original order") from None
    candidates: list[tuple[int, Mapping[str, Any]]] = []
    for _, stream in ordered:
        account_id_value = stream.get("account_id")
        if account_id_value is None:
            continue
        account_id = str(account_id_value)
        if account_id not in policy.known_account_ids:
            continue
        if policy.mode == "include-only" and account_id not in policy.account_ids:
            continue
        if policy.mode == "exclude-only" and account_id in policy.account_ids:
            continue
        candidates.append((policy.priority_for(account_id), stream))
    if policy.mode == "priority":
        candidates.sort(key=lambda item: -item[0])
    return [stream for _, stream in candidates]

