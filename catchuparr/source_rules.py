"""Strict YAML source filters resolved against the current Dispatcharr catalog."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

import yaml
from yaml.constructor import ConstructorError
from yaml.events import AliasEvent
from yaml.nodes import MappingNode

from .schedule import (
    CONTINUOUS_SCHEDULE,
    RecordingSchedule,
    ScheduleError,
    expand_schedule_days,
    normalize_expanded_schedule_days,
    resolve_timezone,
)

MAX_FILTER_CONFIG_BYTES = 64 * 1024
MAX_RULES = 256


class SourceRuleError(ValueError):
    """Raised when the YAML filter document or its resolved catalog is invalid."""


@dataclass(frozen=True)
class SourcePolicy:
    """Resolved filter for one channel; IDs, not editable names, drive runtime."""

    include_account_ids: frozenset[str] | None
    exclude_account_ids: frozenset[str]
    priorities: tuple[tuple[str, int], ...]
    known_account_ids: frozenset[str]

    def priority_for(self, account_id: str) -> int:
        return dict(self.priorities).get(account_id, 0)


@dataclass(frozen=True)
class _FilterPatch:
    filter_kind: str | None
    filter_ids: frozenset[str]
    priorities: tuple[tuple[str, int], ...] | None


@dataclass(frozen=True)
class FilterCompilation:
    channel_uuids: tuple[str, ...]
    profile_ids: tuple[str, ...]
    source_policies: dict[str, SourcePolicy]
    channels: tuple[dict[str, Any], ...]
    timezone: str
    channel_schedules: dict[str, RecordingSchedule] = field(default_factory=dict)
    channel_origins: dict[str, dict[str, Any]] = field(default_factory=dict)


class _RestrictedSafeLoader(yaml.SafeLoader):
    """SafeLoader variant that rejects aliases, anchors, merges and duplicate keys."""

    def compose_node(self, parent, index):
        if self.check_event(AliasEvent):
            raise ConstructorError(None, None, "YAML aliases are not allowed", self.peek_event().start_mark)
        event = self.peek_event()
        if getattr(event, "anchor", None) is not None:
            raise ConstructorError(None, None, "YAML anchors are not allowed", event.start_mark)
        return super().compose_node(parent, index)

    def construct_mapping(self, node, deep=False):
        if not isinstance(node, MappingNode):
            raise ConstructorError(None, None, "expected a mapping", node.start_mark)
        mapping = {}
        for key_node, value_node in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                raise ConstructorError(None, None, "YAML merge keys are not allowed", key_node.start_mark)
            key = self.construct_object(key_node, deep=deep)
            try:
                duplicate = key in mapping
            except TypeError:
                raise ConstructorError(
                    None, None, "mapping keys must be scalar values", key_node.start_mark
                ) from None
            if duplicate:
                raise ConstructorError(None, None, "duplicate YAML key", key_node.start_mark)
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


def _yaml_document(text: str) -> tuple[dict[str, Any] | None, tuple[int, ...]]:
    if not isinstance(text, str):
        raise SourceRuleError("field filter_config: expected text")
    try:
        encoded_size = len(text.encode("utf-8"))
    except UnicodeEncodeError:
        raise SourceRuleError("line 1, field filter_config: invalid text encoding") from None
    if encoded_size > MAX_FILTER_CONFIG_BYTES:
        raise SourceRuleError("field filter_config: YAML exceeds the 64 KiB limit")
    if not text.strip():
        return None, ()
    try:
        value = yaml.load(text, Loader=_RestrictedSafeLoader)
    except (yaml.YAMLError, ValueError, RecursionError, OverflowError) as exc:
        mark = getattr(exc, "problem_mark", None)
        line = mark.line + 1 if mark is not None else 1
        column = mark.column + 1 if mark is not None else 1
        problem = getattr(exc, "problem", None) or "invalid YAML"
        if problem not in {
            "YAML aliases are not allowed",
            "YAML anchors are not allowed",
            "YAML merge keys are not allowed",
            "duplicate YAML key",
            "mapping keys must be scalar values",
            "expected a mapping",
        }:
            problem = "invalid or unsupported YAML syntax"
        raise SourceRuleError(f"line {line}, column {column}: {problem}") from None
    if value is None:
        return None, ()
    if not isinstance(value, dict):
        raise SourceRuleError("line 1, field filter_config: expected a mapping")
    try:
        root = yaml.compose(text, Loader=_RestrictedSafeLoader)
    except (yaml.YAMLError, ValueError, RecursionError, OverflowError) as exc:
        mark = getattr(exc, "problem_mark", None)
        line = mark.line + 1 if mark is not None else 1
        raise SourceRuleError(f"line {line}, field filter_config: invalid YAML") from None
    rule_lines: tuple[int, ...] = ()
    if isinstance(root, MappingNode):
        for key_node, value_node in root.value:
            if getattr(key_node, "value", None) == "rules" and isinstance(value_node, yaml.SequenceNode):
                rule_lines = tuple(item.start_mark.line + 1 for item in value_node.value)
                break
    return value, rule_lines


def _mapping(value: Any, *, line: int, field: str, allowed: set[str], required: set[str] = frozenset()):
    if not isinstance(value, dict):
        raise SourceRuleError(f"line {line}, field {field}: expected a mapping")
    unknown = set(value) - allowed
    if unknown:
        name = sorted(str(key) for key in unknown)[0]
        raise SourceRuleError(f"line {line}, field {field}.{name}: unknown field")
    missing = required - set(value)
    if missing:
        name = sorted(missing)[0]
        raise SourceRuleError(f"line {line}, field {field}.{name}: required field is missing")
    if any(not isinstance(key, str) for key in value):
        raise SourceRuleError(f"line {line}, field {field}: keys must be strings")
    return value


def _profile_catalog(
    profiles: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], set[str]]:
    by_name: dict[str, dict[str, Any]] = {}
    enabled_union: set[str] = set()
    seen_ids: set[str] = set()
    for profile in profiles:
        profile_id = str(profile.get("id") or "")
        name = profile.get("name")
        members = profile.get("channel_uuids", ())
        if not profile_id.isdigit() or not isinstance(name, str) or not name:
            raise SourceRuleError("channel profile catalog contains an invalid entry")
        if not isinstance(members, (list, tuple, set, frozenset)):
            raise SourceRuleError("channel profile membership catalog is invalid")
        if profile_id in seen_ids:
            raise SourceRuleError("channel profile catalog contains a duplicate ID")
        if name in by_name:
            raise SourceRuleError("channel profile catalog contains an ambiguous name")
        normalized_members = {str(value) for value in members}
        seen_ids.add(profile_id)
        by_name[name] = {"id": profile_id, "name": name, "members": normalized_members}
        enabled_union.update(normalized_members)
    return by_name, enabled_union


def _rule_call(index: int, function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except SourceRuleError as exc:
        raise SourceRuleError(f"rule {index}: {exc}") from None


def _account_index(accounts: Iterable[Mapping[str, Any]]) -> tuple[dict[str, str], set[str], set[str]]:
    ids: set[str] = set()
    names: dict[str, str] = {}
    ambiguous: set[str] = set()
    for account in accounts:
        account_id = str(account.get("id") or "")
        name = account.get("name")
        if not account_id or not isinstance(name, str) or not name:
            raise SourceRuleError("M3U account catalog contains an invalid entry")
        if account_id in ids:
            raise SourceRuleError("M3U account catalog contains a duplicate ID")
        ids.add(account_id)
        if name in names:
            ambiguous.add(name)
        else:
            names[name] = account_id
    for name in ambiguous:
        names.pop(name, None)
    return names, ids, ambiguous


def _channel_rows(channels: Iterable[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    by_uuid: dict[str, dict[str, Any]] = {}
    for raw in channels:
        row = dict(raw)
        channel_uuid = str(row.get("uuid") or "")
        if not channel_uuid or channel_uuid in by_uuid:
            raise SourceRuleError("channel catalog contains an invalid or duplicate UUID")
        row["uuid"] = channel_uuid
        rows.append(row)
        by_uuid[channel_uuid] = row
    return rows, by_uuid


def _channel_number(value: Any, *, line: int, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise SourceRuleError(f"line {line}, field {field}: expected a channel number")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise SourceRuleError(f"line {line}, field {field}: invalid channel number") from None
    if not number.is_finite() or number < 0:
        raise SourceRuleError(f"line {line}, field {field}: invalid channel number")
    return number


_RANGE = re.compile(r"^\s*(\d+(?:\.\d*)?|\.\d+)\s*-\s*(\d+(?:\.\d*)?|\.\d+)\s*$")


def _number_selector(value: Any, channel_rows: list[dict[str, Any]], *, line: int) -> set[str]:
    if not isinstance(value, list) or not value:
        raise SourceRuleError(f"line {line}, field channels.numbers: expected a non-empty list")
    matches: set[str] = set()
    seen_tokens: set[str] = set()
    indexed: list[tuple[dict[str, Any], Decimal]] = []
    for channel in channel_rows:
        raw_number = channel.get("number")
        if raw_number is None or (isinstance(raw_number, str) and not raw_number.strip()):
            continue
        indexed.append((channel, _channel_number(raw_number, line=line, field="catalog.number")))
    for index, raw in enumerate(value):
        field = f"channels.numbers[{index}]"
        if isinstance(raw, str):
            range_match = _RANGE.fullmatch(raw)
        else:
            range_match = None
        if range_match:
            low, high = Decimal(range_match.group(1)), Decimal(range_match.group(2))
            if low > high:
                raise SourceRuleError(f"line {line}, field {field}: reversed range")
            token = f"{low}..{high}"
            if token in seen_tokens:
                raise SourceRuleError(f"line {line}, field {field}: duplicate selector")
            seen_tokens.add(token)
            found = [(channel, number) for channel, number in indexed if low <= number <= high]
            if not found:
                raise SourceRuleError(f"line {line}, field {field}: range matches no channels")
            counts: dict[Decimal, int] = {}
            for _, number in found:
                counts[number] = counts.get(number, 0) + 1
            if any(count > 1 for count in counts.values()):
                raise SourceRuleError(f"line {line}, field {field}: range is ambiguous")
            if matches & {channel["uuid"] for channel, _ in found}:
                raise SourceRuleError(f"line {line}, field {field}: selectors overlap")
            matches.update(channel["uuid"] for channel, _ in found)
            continue
        number = _channel_number(raw, line=line, field=field)
        token = str(number)
        if token in seen_tokens:
            raise SourceRuleError(f"line {line}, field {field}: duplicate selector")
        seen_tokens.add(token)
        found = [channel for channel, candidate in indexed if candidate == number]
        if not found:
            raise SourceRuleError(f"line {line}, field {field}: channel number matches no channels")
        if len(found) > 1:
            raise SourceRuleError(f"line {line}, field {field}: channel number is ambiguous")
        if found[0]["uuid"] in matches:
            raise SourceRuleError(f"line {line}, field {field}: selectors overlap")
        matches.add(found[0]["uuid"])
    return matches


def _string_list(value: Any, *, line: int, field: str, allow_empty: bool = False) -> list[str]:
    if not isinstance(value, list) or (not value and not allow_empty):
        requirement = "a list" if allow_empty else "a non-empty list"
        raise SourceRuleError(f"line {line}, field {field}: expected {requirement}")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise SourceRuleError(f"line {line}, field {field}: values must be non-empty strings")
    if len(set(value)) != len(value):
        raise SourceRuleError(f"line {line}, field {field}: duplicate value")
    return value


def _resolve_channel_selector(
    selector: dict[str, Any],
    *,
    line: int,
    channel_rows: list[dict[str, Any]],
    eligible_channel_ids: set[str],
    profiles_by_name: dict[str, dict[str, Any]],
) -> tuple[set[str], set[str], bool]:
    if len(selector) != 1:
        raise SourceRuleError(
            f"line {line}, field channels: specify exactly one of numbers, names, groups, or profile"
        )
    kind, value = next(iter(selector.items()))
    profile_ids: set[str] = set()
    if kind == "profile":
        if not isinstance(value, str) or not value:
            raise SourceRuleError(f"line {line}, field channels.profile: expected a profile name or all")
        if value == "all":
            if not eligible_channel_ids:
                raise SourceRuleError(
                    f"line {line}, field channels.profile: selector matches no eligible channels"
                )
            return set(eligible_channel_ids), set(), True
        profile = profiles_by_name.get(value)
        if profile is None:
            raise SourceRuleError(f"line {line}, field channels.profile: unknown channel profile")
        profile_ids.add(profile["id"])
        members = profile["members"] & eligible_channel_ids
        if not members:
            raise SourceRuleError(
                f"line {line}, field channels.profile: profile has no eligible channels"
            )
        return members, profile_ids, False
    if kind == "numbers":
        selected = _number_selector(value, channel_rows, line=line)
    elif kind in {"names", "groups"}:
        values = _string_list(value, line=line, field=f"channels.{kind}")
        field = "name" if kind == "names" else "group"
        selected = set()
        for name in values:
            found = [
                row for row in channel_rows
                if str(row.get(field) or "") == name and row["uuid"] in eligible_channel_ids
            ]
            if not found:
                raise SourceRuleError(f"line {line}, field channels.{kind}: selector matches no eligible channels")
            if kind == "names" and len(found) > 1:
                raise SourceRuleError(f"line {line}, field channels.names: channel name is ambiguous")
            selected.update(row["uuid"] for row in found)
    else:
        raise SourceRuleError(f"line {line}, field channels.{kind}: unknown selector")
    selected &= eligible_channel_ids
    if not selected:
        raise SourceRuleError(f"line {line}, field channels.{kind}: selector matches no eligible channels")
    return selected, profile_ids, False


def _resolve_filter(
    rule: dict[str, Any],
    *,
    line: int,
    account_names: dict[str, str],
    account_ids: set[str],
    ambiguous_account_names: set[str],
) -> _FilterPatch:
    include_present = "include" in rule
    exclude_present = "exclude" in rule
    if include_present and exclude_present:
        raise SourceRuleError(f"line {line}, fields include/exclude: choose only one")
    filter_kind = "include" if include_present else "exclude" if exclude_present else None
    filter_ids: frozenset[str] = frozenset()
    if include_present:
        names = _string_list(rule["include"], line=line, field="include")
        filter_ids = frozenset(
            _account_id(name, "include", line, account_names, account_ids, ambiguous_account_names)
            for name in names
        )
    elif exclude_present:
        names = _string_list(rule["exclude"], line=line, field="exclude", allow_empty=True)
        filter_ids = frozenset(
            _account_id(name, "exclude", line, account_names, account_ids, ambiguous_account_names)
            for name in names
        )

    priorities: tuple[tuple[str, int], ...] | None = None
    if "priority" in rule:
        raw_priorities = rule["priority"]
        if not isinstance(raw_priorities, dict):
            raise SourceRuleError(f"line {line}, field priority: expected a mapping")
        pairs = []
        for name, value in raw_priorities.items():
            if not isinstance(name, str) or not name:
                raise SourceRuleError(f"line {line}, field priority: names must be strings")
            if isinstance(value, bool) or not isinstance(value, int):
                raise SourceRuleError(f"line {line}, field priority.{name}: expected an integer")
            account_id = _account_id(
                name, f"priority.{name}", line, account_names, account_ids, ambiguous_account_names
            )
            pairs.append((account_id, value))
        priorities = tuple(pairs)
    return _FilterPatch(filter_kind, filter_ids, priorities)


def _effective_filter(
    patch: _FilterPatch | None,
    inherited: SourcePolicy | None,
    *,
    line: int,
    known_account_ids: set[str],
) -> SourcePolicy | None:
    if patch is None:
        return inherited
    if patch.filter_kind == "include":
        include_ids = patch.filter_ids
        exclude_ids = frozenset()
    elif patch.filter_kind == "exclude":
        include_ids = None
        exclude_ids = patch.filter_ids
    elif inherited is not None:
        include_ids = inherited.include_account_ids
        exclude_ids = inherited.exclude_account_ids
    else:
        include_ids = None
        exclude_ids = frozenset()
    priorities = patch.priorities
    if priorities is None:
        priorities = inherited.priorities if inherited is not None else ()
    if patch.filter_kind is None and patch.priorities is None:
        return inherited
    known_ids = frozenset(known_account_ids)
    return SourcePolicy(include_ids, exclude_ids, priorities, known_ids)


def _account_id(
    name: str,
    field: str,
    line: int,
    account_names: dict[str, str],
    account_ids: set[str],
    ambiguous_account_names: set[str],
) -> str:
    if name in ambiguous_account_names:
        raise SourceRuleError(f"line {line}, field {field}: M3U account name is ambiguous")
    account_id = account_names.get(name)
    if account_id is None or account_id not in account_ids:
        raise SourceRuleError(f"line {line}, field {field}: unknown M3U account name")
    return account_id


def _ordered_assigned(streams: Iterable[Mapping[str, Any]], known_ids: set[str]) -> list[Mapping[str, Any]]:
    rows = list(enumerate(streams))
    try:
        order_values = [
            Decimal(str(row.get("order", index))) for index, row in rows
        ]
        if any(not order.is_finite() for order in order_values):
            raise ValueError
        ordered = sorted(
            zip(rows, order_values),
            key=lambda item: (item[1], item[0][0]),
        )
    except (InvalidOperation, ValueError):
        raise SourceRuleError("each assigned stream must have a numeric order") from None
    return [
        row for ((_, row), _) in ordered
        if row.get("account_id") is not None and str(row.get("account_id")) in known_ids
    ]


def rank_candidates(policy: SourcePolicy, streams: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Filter assigned source rows, then stably rank by descending priority."""
    ordered = _ordered_assigned(streams, set(policy.known_account_ids))
    if policy.include_account_ids is not None:
        ordered = [row for row in ordered if str(row.get("account_id")) in policy.include_account_ids]
    elif policy.exclude_account_ids:
        ordered = [row for row in ordered if str(row.get("account_id")) not in policy.exclude_account_ids]
    priorities = dict(policy.priorities)
    if priorities:
        indexed = list(enumerate(ordered))
        indexed.sort(
            key=lambda item: (-priorities.get(str(item[1].get("account_id")), 0), item[0])
        )
        ordered = [row for _, row in indexed]
    return ordered


def _load_document(text: str) -> tuple[dict[str, Any] | None, tuple[int, ...]]:
    document, rule_lines = _yaml_document(text)
    if document is None:
        return None, ()
    document = _mapping(
        document,
        line=1,
        field="filter_config",
        allowed={"version", "profile", "rules", "timezone", "schedule"},
        required={"version", "rules"},
    )
    return document, rule_lines


def compile_filter_config(
    text: str,
    channels: Iterable[Mapping[str, Any]],
    accounts: Iterable[Mapping[str, Any]],
    profiles: Iterable[Mapping[str, Any]],
    streams_by_channel: Mapping[str, Iterable[Mapping[str, Any]]],
) -> FilterCompilation:
    """Validate YAML and resolve eligible channels/policies to stable catalog IDs."""
    document, rule_lines = _load_document(text)
    if document is None:
        return FilterCompilation((), (), {}, (), timezone=resolve_timezone())
    if type(document.get("version")) is not int or document["version"] != 1:
        raise SourceRuleError("line 1, field version: expected integer version 1")
    try:
        timezone_name = (
            resolve_timezone(document["timezone"])
            if "timezone" in document else resolve_timezone()
        )
        global_schedule_days = expand_schedule_days(
            document.get("schedule", "continuous"), field="schedule"
        )
    except ScheduleError as exc:
        raise SourceRuleError(str(exc)) from None
    scope_name = document.get("profile", "all")
    if not isinstance(scope_name, str) or not scope_name:
        raise SourceRuleError("line 1, field profile: expected all or a channel profile name")

    channel_rows, channels_by_uuid = _channel_rows(channels)
    profiles_by_name, all_members = _profile_catalog(profiles)
    if scope_name == "all":
        eligible_ids = all_members & set(channels_by_uuid)
        referenced_profile_ids = {
            item["id"] for item in profiles_by_name.values() if item["members"]
        }
    else:
        scoped_profile = profiles_by_name.get(scope_name)
        if scoped_profile is None:
            raise SourceRuleError("line 1, field profile: unknown channel profile")
        eligible_ids = scoped_profile["members"] & set(channels_by_uuid)
        referenced_profile_ids = {scoped_profile["id"]}

    raw_rules = document.get("rules")
    if not isinstance(raw_rules, list):
        raise SourceRuleError("line 1, field rules: expected a list")
    if len(raw_rules) > MAX_RULES:
        raise SourceRuleError("line 1, field rules: too many rules")
    if not raw_rules:
        return FilterCompilation(
            (), tuple(sorted(referenced_profile_ids, key=int)), {}, (), timezone_name, {}
        )

    account_names, account_ids, ambiguous_account_names = _account_index(accounts)
    all_rule = None
    profile_rules: list[dict[str, Any]] = []
    direct_rules: list[dict[str, Any]] = []
    occupied_direct: set[str] = set()
    occupied_profiles: set[str] = set()
    for index, raw_rule in enumerate(raw_rules, start=1):
        line = rule_lines[index - 1] if index <= len(rule_lines) else index + 2
        rule = _rule_call(index, _mapping,
            raw_rule,
            line=line,
            field=f"rules[{index - 1}]",
            allowed={"channels", "include", "exclude", "priority", "schedule", "schedule_mode"},
            required={"channels"},
        )
        schedule_mode = rule.get("schedule_mode", "replace")
        if not isinstance(schedule_mode, str) or schedule_mode not in {"replace", "override_days"}:
            raise SourceRuleError(
                f"line {line}, field schedule_mode: expected replace or override_days"
            )
        if "schedule_mode" in rule and "schedule" not in rule:
            raise SourceRuleError(f"line {line}, field schedule_mode: requires schedule")
        try:
            rule_schedule_days = (
                expand_schedule_days(
                    rule["schedule"],
                    line=line,
                    field=f"rules[{index - 1}].schedule",
                )
                if "schedule" in rule else None
            )
        except ScheduleError as exc:
            raise SourceRuleError(f"rule {index}: {exc}") from None
        selector = _rule_call(index, _mapping,
            rule["channels"],
            line=line,
            field=f"rules[{index - 1}].channels",
            allowed={"numbers", "names", "groups", "profile"},
        )
        selected, selector_profile_ids, is_all = _rule_call(index, _resolve_channel_selector,
            selector,
            line=line,
            channel_rows=[row for row in channel_rows if row["uuid"] in eligible_ids],
            eligible_channel_ids=eligible_ids,
            profiles_by_name=profiles_by_name,
        )
        referenced_profile_ids.update(selector_profile_ids)
        selector_kind = next(iter(selector))
        patch = _rule_call(index, _resolve_filter,
            rule,
            line=line,
            account_names=account_names,
            account_ids=account_ids,
            ambiguous_account_names=ambiguous_account_names,
        )
        entry = {
            "index": index,
            "line": line,
            "selected": selected,
            "selector_kind": selector_kind,
            "patch": patch,
            "schedule_days": rule_schedule_days,
            "schedule_mode": schedule_mode,
        }
        if is_all:
            if all_rule is not None:
                raise SourceRuleError(f"line {line}, rule {index}, field channels.profile: duplicate all rule")
            all_rule = entry
            continue
        if selector_kind == "profile":
            if occupied_profiles & selected:
                raise SourceRuleError(
                    f"line {line}, rule {index}, field channels.profile: competing named profile rules"
                )
            occupied_profiles.update(selected)
            profile_rules.append(entry)
        else:
            if occupied_direct & selected:
                raise SourceRuleError(
                    f"line {line}, rule {index}, field channels: direct channel rules overlap"
                )
            occupied_direct.update(selected)
            direct_rules.append(entry)

    profile_by_channel: dict[str, dict[str, Any]] = {}
    direct_by_channel: dict[str, dict[str, Any]] = {}
    for entry in profile_rules:
        for channel_uuid in entry["selected"]:
            profile_by_channel[channel_uuid] = entry
    for entry in direct_rules:
        if entry["selector_kind"] not in {"names", "numbers"} and (
            entry["selected"] & profile_by_channel.keys()
        ):
            raise SourceRuleError(
                f"line {entry['line']}, field channels: named-profile exceptions require names or numbers"
            )
        for channel_uuid in entry["selected"]:
            direct_by_channel[channel_uuid] = entry
    all_selected = all_rule["selected"] if all_rule is not None else set()
    selected_ids = set(all_selected) | set(profile_by_channel) | set(direct_by_channel)
    effective: dict[str, tuple[SourcePolicy | None, RecordingSchedule, dict[str, Any]]] = {}
    days = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
    global_schedule_origin = "document schedule" if "schedule" in document else "default continuous"
    for channel_uuid in sorted(selected_ids):
        inherited_policy = None
        origin = {"source_filter": None, "priority": None}
        inherited_days = dict(global_schedule_days)
        schedule_origins = {day: global_schedule_origin for day in days}
        base_entries = []
        # Resolve the all rule before a named profile, regardless of YAML order.
        if channel_uuid in all_selected:
            base_entries.append(all_rule)
        if channel_uuid in profile_by_channel:
            base_entries.append(profile_by_channel[channel_uuid])
        for base_entry in base_entries:
            inherited_policy = _effective_filter(
                base_entry["patch"], inherited_policy, line=base_entry["line"],
                known_account_ids=account_ids,
            )
            if base_entry["patch"].filter_kind is not None:
                origin["source_filter"] = f"rule {base_entry['index']}"
            if base_entry["patch"].priorities is not None:
                origin["priority"] = f"rule {base_entry['index']}"
            if base_entry["schedule_days"] is not None:
                if base_entry["schedule_mode"] == "override_days":
                    for day, windows in base_entry["schedule_days"].items():
                        inherited_days[day] = windows
                        schedule_origins[day] = f"rule {base_entry['index']}"
                else:
                    inherited_days = {
                        day: base_entry["schedule_days"].get(day, ()) for day in days
                    }
                    schedule_origins = {day: f"rule {base_entry['index']}" for day in days}
        direct_entry = direct_by_channel.get(channel_uuid)
        if direct_entry is not None:
            direct_patch = direct_entry["patch"]
            effective_policy = _effective_filter(
                direct_patch, inherited_policy, line=direct_entry["line"],
                known_account_ids=account_ids,
            )
            if direct_patch.filter_kind is not None:
                origin["source_filter"] = f"rule {direct_entry['index']}"
            if direct_patch.priorities is not None:
                origin["priority"] = f"rule {direct_entry['index']}"
            if direct_entry["schedule_days"] is not None:
                if direct_entry["schedule_mode"] == "override_days":
                    for day, windows in direct_entry["schedule_days"].items():
                        inherited_days[day] = windows
                        schedule_origins[day] = f"rule {direct_entry['index']}"
                else:
                    inherited_days = {
                        day: direct_entry["schedule_days"].get(day, ()) for day in days
                    }
                    schedule_origins = {day: f"rule {direct_entry['index']}" for day in days}
        else:
            effective_policy = inherited_policy
        if effective_policy is not None:
            if (
                effective_policy.priorities
                and effective_policy.include_account_ids is None
                and not effective_policy.exclude_account_ids
                and origin["source_filter"] is None
            ):
                raise SourceRuleError("field priority: requires include or exclude")
            for account_id, _score in effective_policy.priorities:
                if (
                    effective_policy.include_account_ids is not None
                    and account_id not in effective_policy.include_account_ids
                ):
                    raise SourceRuleError(
                        "field priority: account is excluded by include filter"
                    )
                if account_id in effective_policy.exclude_account_ids:
                    raise SourceRuleError(
                        "field priority: account is excluded by source filter"
                    )
        channel_schedule = (
            CONTINUOUS_SCHEDULE
            if len(inherited_days) == 7 and all(
                inherited_days.get(day) == ((0, 1440),) for day in days
            )
            else normalize_expanded_schedule_days(inherited_days)
        )
        effective[channel_uuid] = (
            effective_policy, channel_schedule,
            {"source_filter": origin["source_filter"] or "inherited default",
             "priority": origin["priority"] or "inherited default",
             "schedule_days": schedule_origins},
        )

    source_policies: dict[str, SourcePolicy] = {}
    channel_schedules: dict[str, RecordingSchedule] = {}
    preview_by_uuid: dict[str, dict[str, Any]] = {}
    channel_origins: dict[str, dict[str, Any]] = {}
    account_name_by_id = {
        str(item.get("id")): str(item.get("name") or "") for item in accounts
    }
    for channel_uuid in sorted(selected_ids):
        policy, channel_schedule, origins = effective[channel_uuid]
        channel_origins[channel_uuid] = origins
        channel_schedules[channel_uuid] = channel_schedule
        streams = streams_by_channel.get(channel_uuid, ())
        baseline = _ordered_assigned(streams, account_ids)
        ranked = rank_candidates(policy, streams) if policy is not None else baseline
        if policy is not None and _requires_override(policy):
            source_policies[channel_uuid] = policy
        candidate_views = []
        for row in ranked:
            account_id = str(row.get("account_id") or "")
            account = next((item for item in accounts if str(item.get("id")) == account_id), {})
            candidate_views.append({
                "stream_id": str(row.get("id") or ""),
                "stream_name": str(row.get("name") or ""),
                "account_id": account_id,
                "account_name": str(account.get("name") or ""),
                "order": int(row.get("order", 0) or 0),
            })
        channel = channels_by_uuid[channel_uuid]
        preview_by_uuid[channel_uuid] = {
            "channel_uuid": channel_uuid,
            "channel_name": str(channel.get("name") or ""),
            "channel_number": str(channel.get("number") or ""),
            "channel_group": str(channel.get("group") or ""),
            "candidates": candidate_views,
            "source_override": channel_uuid in source_policies,
            "effective_source_policy": {
                "include": (
                    sorted(account_name_by_id[account_id]
                           for account_id in policy.include_account_ids)
                    if policy is not None and policy.include_account_ids is not None else None
                ),
                "exclude": sorted(
                    account_name_by_id[account_id]
                    for account_id in (policy.exclude_account_ids if policy is not None else ())
                ),
                "priority": (
                    {account_name_by_id[account_id]: score
                     for account_id, score in policy.priorities}
                    if policy is not None else {}
                ),
            },
            "recording_schedule": channel_schedule.to_snapshot(),
            "configuration_origin": origins,
            "warning": (
                "This override uses a dedicated worker and may need another provider or tuner slot."
                if channel_uuid in source_policies else None
            ),
        }
    return FilterCompilation(
        channel_uuids=tuple(sorted(selected_ids)),
        profile_ids=tuple(sorted(referenced_profile_ids, key=int)),
        source_policies=source_policies,
        channels=tuple(preview_by_uuid[channel] for channel in sorted(preview_by_uuid)),
        timezone=timezone_name,
        channel_schedules=channel_schedules,
        channel_origins=channel_origins,
    )


def _requires_override(policy: SourcePolicy) -> bool:
    """Keep every effective filter/ranking private even if today's order matches."""
    return (
        policy.include_account_ids is not None
        or bool(policy.exclude_account_ids)
        or bool(policy.priorities)
    )


CONNECTION_SHARING_DEFAULTS = {
    "enabled": False,
    "live_stability_seconds": 5,
    "last_viewer_return_seconds": 30,
    "min_switch_interval_seconds": 60,
}
_CONNECTION_SHARING_COOLDOWNS = (
    "live_stability_seconds",
    "last_viewer_return_seconds",
    "min_switch_interval_seconds",
)


def compile_connection_sharing_config(
    text: str,
    channels: Iterable[Mapping[str, Any]],
    profiles: Iterable[Mapping[str, Any]],
    selected_channel_uuids: Iterable[str],
) -> dict[str, dict[str, Any]]:
    """Resolve strict per-channel sharing defaults and overrides to stable UUIDs."""
    from copy import deepcopy

    selected_ids = {str(value) for value in selected_channel_uuids}
    selected_rows, selected_by_uuid = _channel_rows(
        row for row in channels if str(row.get("uuid") or "") in selected_ids
    )
    if set(selected_by_uuid) != selected_ids:
        raise SourceRuleError("connection_sharing_config references an unknown selected channel")
    result = {channel: deepcopy(CONNECTION_SHARING_DEFAULTS) for channel in sorted(selected_ids)}
    if not text.strip():
        return result
    document, rule_lines = _yaml_document(text)
    if document is None:
        return result
    document = _mapping(
        document,
        line=1,
        field="connection_sharing_config",
        allowed={"version", "default", "rules"},
        required={"version"},
    )
    if type(document.get("version")) is not int or document["version"] != 1:
        raise SourceRuleError(
            "line 1, field connection_sharing_config.version: expected integer version 1"
        )

    def values(raw: Any, *, line: int, field: str, base: dict[str, Any]) -> dict[str, Any]:
        patch = _mapping(
            raw, line=line, field=field,
            allowed={"enabled", *_CONNECTION_SHARING_COOLDOWNS},
        )
        merged = dict(base)
        if "enabled" in patch:
            if type(patch["enabled"]) is not bool:
                raise SourceRuleError(f"line {line}, field {field}.enabled: expected a boolean")
            merged["enabled"] = patch["enabled"]
        for key in _CONNECTION_SHARING_COOLDOWNS:
            if key not in patch:
                continue
            value = patch[key]
            if type(value) is not int or not 0 <= value <= 3600:
                raise SourceRuleError(
                    f"line {line}, field {field}.{key}: expected an integer from 0 to 3600"
                )
            merged[key] = value
        return merged

    defaults = values(
        document.get("default", {}), line=1,
        field="connection_sharing_config.default",
        base=CONNECTION_SHARING_DEFAULTS,
    )
    result = {channel: dict(defaults) for channel in sorted(selected_ids)}
    raw_rules = document.get("rules", [])
    if not isinstance(raw_rules, list):
        raise SourceRuleError("line 1, field connection_sharing_config.rules: expected a list")
    if len(raw_rules) > MAX_RULES:
        raise SourceRuleError("line 1, field connection_sharing_config.rules: too many rules")

    profiles_by_name, _ = _profile_catalog(profiles)
    occupied: set[str] = set()
    for index, raw_rule in enumerate(raw_rules, start=1):
        line = rule_lines[index - 1] if index <= len(rule_lines) else index + 2
        rule = _mapping(
            raw_rule, line=line, field=f"connection_sharing_config.rules[{index - 1}]",
            allowed={"channels", "enabled", *_CONNECTION_SHARING_COOLDOWNS},
            required={"channels"},
        )
        selector = _mapping(
            rule["channels"], line=line,
            field=f"connection_sharing_config.rules[{index - 1}].channels",
            allowed={"numbers", "names", "groups", "profile"},
        )
        selected, _, _ = _resolve_channel_selector(
            selector,
            line=line,
            channel_rows=selected_rows,
            eligible_channel_ids=selected_ids,
            profiles_by_name=profiles_by_name,
        )
        if occupied & selected:
            raise SourceRuleError(
                f"line {line}, field connection_sharing_config.rules: channel selectors overlap"
            )
        occupied.update(selected)
        override = values(
            {key: value for key, value in rule.items() if key != "channels"},
            line=line,
            field=f"connection_sharing_config.rules[{index - 1}]",
            base=defaults,
        )
        for channel_uuid in selected:
            result[channel_uuid] = override
    return result
