"""Draft validation and active source policy persistence.

Dispatcharr keeps editable settings in PluginConfig. Source policies are compiled
against the current channel assignments and stored separately so a draft never
changes recorder behavior until Apply succeeds.
"""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .runtime import PLUGIN_KEY, parse_settings

ACTIVE_CONFIG_NAME = ".catchuparr-active-settings.json"
ACTIVE_CONFIG_VERSION = 1


@dataclass(frozen=True)
class SourceCatalog:
    channels: tuple[dict[str, Any], ...]
    accounts: tuple[dict[str, Any], ...]
    streams_by_channel: dict[str, tuple[dict[str, Any], ...]]


def load_draft_settings(fallback: dict | None = None) -> dict:
    """Read settings from Dispatcharr's PluginConfig row, with action context fallback."""
    try:
        from apps.plugins.models import PluginConfig

        row = PluginConfig.objects.filter(key=PLUGIN_KEY).first()
        if row is not None:
            return dict(row.settings or {})
    except (ImportError, ModuleNotFoundError):
        pass
    return dict(fallback or {})


def source_catalog() -> SourceCatalog:
    """Build safe DTOs from channels, their assigned streams, and M3U accounts."""
    from apps.channels.models import Channel, ChannelStream

    channel_by_uuid = {
        str(channel.uuid): {
            "uuid": str(channel.uuid),
            "number": str(getattr(channel, "channel_number", "") or ""),
            "name": str(getattr(channel, "name", "") or ""),
            "group": _group_name(channel),
        }
        for channel in Channel.objects.select_related("channel_group").all()
    }
    assigned = list(
        ChannelStream.objects.select_related(
            "channel", "stream", "stream__m3u_account"
        ).order_by("channel__uuid", "order", "id")
    )
    account_by_id: dict[str, dict[str, Any]] = {}
    streams_by_channel: dict[str, list[dict[str, Any]]] = {}
    for assignment in assigned:
        channel = assignment.channel
        channel_uuid = str(channel.uuid)
        stream = assignment.stream
        account = getattr(stream, "m3u_account", None)
        account_id = str(getattr(stream, "m3u_account_id", "") or "")
        if account is not None and account_id:
            account_by_id[account_id] = {
                "id": account_id,
                "name": str(getattr(account, "name", "") or ""),
            }
        streams_by_channel.setdefault(channel_uuid, []).append({
            "id": str(stream.id),
            "name": str(getattr(stream, "name", "") or ""),
            "account_id": account_id,
            "order": int(getattr(assignment, "order", 0) or 0),
        })

    # Include extant M3U accounts with no current assignment in validation
    # diagnostics, while a compiled policy can only select assigned candidates.
    try:
        from apps.m3u.models import M3UAccount

        for account in M3UAccount.objects.all().only("id", "name"):
            account_by_id.setdefault(str(account.id), {
                "id": str(account.id), "name": str(account.name or "")
            })
    except (ImportError, ModuleNotFoundError):
        pass

    return SourceCatalog(
        channels=tuple(channel_by_uuid.values()),
        accounts=tuple(account_by_id.values()),
        streams_by_channel={key: tuple(value) for key, value in streams_by_channel.items()},
    )


def _group_name(channel) -> str:
    value = getattr(channel, "channel_group", None)
    return str(getattr(value, "name", "") or "")


def compile_draft(
    settings: dict, catalog: SourceCatalog | None = None
) -> tuple[dict[str, Any], SourceCatalog, list[dict[str, Any]]]:
    """Validate base fields and compile editable source rules into stable IDs."""
    parse_settings(settings)
    catalog = catalog or source_catalog()
    from .source_rules import compile_source_rules, rank_candidates

    raw_rules = settings.get("source_rules")
    if raw_rules is not None and not isinstance(raw_rules, str):
        raise ValueError("source_rules must be text")
    rules_text = (raw_rules or "").strip()
    policies = (
        compile_source_rules(rules_text, catalog.channels, catalog.accounts)
        if rules_text else {}
    )
    encoded: dict[str, dict[str, Any]] = {}
    previews: list[dict[str, Any]] = []
    for channel_uuid, policy in sorted(policies.items()):
        # Save only IDs and rule order. Names are refreshed from the current
        # Dispatcharr catalog on the next Apply and never control runtime access.
        account_ids = sorted(str(value) for value in policy.account_ids)
        priorities = [
            [str(account_id), int(priority)] for account_id, priority in policy.priorities
        ]
        encoded[str(channel_uuid)] = {
            "mode": str(policy.mode),
            "account_ids": account_ids,
            "priorities": priorities,
            "known_account_ids": sorted(str(value) for value in policy.known_account_ids),
        }
        stream_dtos = catalog.streams_by_channel.get(str(channel_uuid), ())
        ranked = rank_candidates(policy, stream_dtos)
        candidate_views = []
        for candidate in ranked:
            account_id = str(candidate.get("account_id") or "")
            account = next(
                (item for item in catalog.accounts if str(item["id"]) == account_id), {}
            )
            candidate_views.append({
                "stream_id": str(candidate.get("id") or ""),
                "stream_name": str(candidate.get("name") or ""),
                "account_id": account_id,
                "account_name": str(account.get("name") or ""),
                "order": int(candidate.get("order", 0) or 0),
            })
        previews.append({
            "channel_uuid": str(channel_uuid),
            "candidates": candidate_views,
            "warning": (
                "Selected sources may open a separate provider connection."
                if len({item["account_id"] for item in candidate_views if item["account_id"]}) > 1
                else None
            ),
        })
    return {"source_policies": encoded}, catalog, previews


def validate_configuration(settings: dict, catalog: SourceCatalog | None = None) -> dict[str, Any]:
    active, _, previews = compile_draft(settings, catalog)
    return {
        "valid": True,
        "channels": previews,
        "source_policy_count": len(active["source_policies"]),
    }


def apply_configuration(
    settings: dict,
    catalog: SourceCatalog | None = None,
    active_path: Path | None = None,
) -> dict[str, Any]:
    """Compile fully before atomically replacing the active source config."""
    parse_settings(settings)
    active, _, previews = compile_draft(settings, catalog)
    path = Path(active_path) if active_path is not None else active_settings_path()
    document = {
        "version": ACTIVE_CONFIG_VERSION,
        "settings": _applicable_settings(settings),
        "source_policies": active["source_policies"],
    }
    _atomic_json_replace(path, document)
    return {
        "applied": True,
        "source_policy_count": len(active["source_policies"]),
        "channels": previews,
    }


def active_settings_path() -> Path:
    """Locate private active state beside the replaceable plugin directory."""
    return Path(__file__).resolve().parent.parent / ACTIVE_CONFIG_NAME


def _applicable_settings(settings: dict) -> dict[str, Any]:
    """Persist only known non-secret runtime and plugin action settings."""
    parsed = parse_settings(settings)
    applicable: dict[str, Any] = {
        "channel_uuids": "\n".join(parsed.channel_uuids),
        "archive_root": str(parsed.archive_root),
        "retention_hours": parsed.retention_hours,
        "max_storage_gib": parsed.max_storage_bytes // 1024**3,
    }
    if "source_rules" in settings:
        raw_rules = settings["source_rules"]
        if raw_rules is not None and not isinstance(raw_rules, str):
            raise ValueError("source_rules must be text")
        applicable["source_rules"] = raw_rules or ""
    raw_user_id = settings.get("playback_user_id")
    if isinstance(raw_user_id, bool):
        raise ValueError("playback_user_id must be a positive user ID")
    if raw_user_id not in (None, "", 0, "0"):
        if not isinstance(raw_user_id, (int, str)) or not str(raw_user_id).isdigit():
            raise ValueError("playback_user_id must be a positive user ID")
        user_id = int(raw_user_id)
        if user_id <= 0:
            raise ValueError("playback_user_id must be a positive user ID")
        applicable["playback_user_id"] = user_id
    return applicable


def load_active_configuration(active_path: Path | None = None) -> dict[str, Any] | None:
    """Load the applied settings snapshot, or None before the first Apply."""
    path = Path(active_path) if active_path is not None else active_settings_path()
    try:
        with _config_lock(path, exclusive=False):
            data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or set(data) != {"version", "settings", "source_policies"}:
            raise ValueError("Active Catchuparr configuration is invalid")
        if (
            type(data.get("version")) is not int
            or data.get("version") != ACTIVE_CONFIG_VERSION
            or not isinstance(data.get("source_policies"), dict)
            or not isinstance(data.get("settings"), dict)
        ):
            raise ValueError("Active Catchuparr configuration is invalid")
        _validate_active_document(data)
        return {**data["settings"], "source_policies": data["source_policies"]}
    except FileNotFoundError:
        return None


def _validate_active_document(data: dict[str, Any]) -> None:
    """Reject malformed persisted policy data before any recorder can use it."""
    from .runtime import parse_settings

    settings = data["settings"]
    allowed_settings = {
        "channel_uuids", "archive_root", "retention_hours", "max_storage_gib",
        "source_rules", "playback_user_id",
    }
    if set(settings) - allowed_settings:
        raise ValueError("Active Catchuparr settings contain unknown fields")
    if "channel_uuids" in settings and not isinstance(settings["channel_uuids"], str):
        raise ValueError("Active Catchuparr channel IDs are invalid")
    if "source_rules" in settings and not isinstance(settings["source_rules"], str):
        raise ValueError("Active Catchuparr source rules are invalid")
    for field_name, minimum, maximum in (
        ("retention_hours", 1, 720), ("max_storage_gib", 1, 10240)
    ):
        if field_name in settings:
            value = settings[field_name]
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, str))
                or not str(value).isdigit()
                or not minimum <= int(value) <= maximum
            ):
                raise ValueError(f"Active Catchuparr {field_name} is invalid")
    if "playback_user_id" in settings:
        user_id = settings["playback_user_id"]
        if isinstance(user_id, bool):
            raise ValueError("Active Catchuparr playback user ID is invalid")
        if user_id not in (None, "", 0, "0") and (
            not isinstance(user_id, (int, str))
            or not str(user_id).isdigit()
            or int(user_id) <= 0
        ):
            raise ValueError("Active Catchuparr playback user ID is invalid")
    parsed = parse_settings(settings)
    channels = set(parsed.channel_uuids)
    policies = data["source_policies"]
    allowed_modes = {"include-only", "exclude-only", "priority", "unchanged"}
    for raw_channel, policy in policies.items():
        try:
            channel_uuid = str(uuid.UUID(str(raw_channel)))
        except (ValueError, TypeError, AttributeError):
            raise ValueError("Active Catchuparr source policy has an invalid channel ID") from None
        if channel_uuid != str(raw_channel) or channel_uuid not in channels:
            raise ValueError("Active Catchuparr source policy channel is not configured")
        if not isinstance(policy, dict):
            raise ValueError("Active Catchuparr source policy is invalid")
        expected_keys = {"mode", "account_ids", "priorities"}
        if "known_account_ids" in policy:
            expected_keys.add("known_account_ids")
        if set(policy) != expected_keys:
            raise ValueError("Active Catchuparr source policy fields are invalid")
        mode = policy.get("mode")
        if mode not in allowed_modes:
            raise ValueError("Active Catchuparr source policy mode is invalid")
        account_ids = _validate_account_id_list(policy.get("account_ids"), "account IDs")
        if mode in {"include-only", "exclude-only"} and not account_ids:
            raise ValueError("Active source selection policy has no accounts")
        known_ids = None
        if "known_account_ids" in policy:
            known_ids = _validate_account_id_list(
                policy["known_account_ids"], "known account IDs"
            )
            if not set(account_ids) <= set(known_ids):
                raise ValueError("Active Catchuparr source policy references an unknown account")
        priorities = policy.get("priorities")
        if not isinstance(priorities, list):
            raise ValueError("Active Catchuparr source priorities are invalid")
        priority_ids: set[str] = set()
        for pair in priorities:
            if (
                not isinstance(pair, list)
                or len(pair) != 2
                or isinstance(pair[1], bool)
                or not isinstance(pair[1], int)
            ):
                raise ValueError("Active Catchuparr source priority is invalid")
            account_id = _validate_account_id(pair[0], "priority account ID")
            if account_id in priority_ids or (
                known_ids is not None and account_id not in set(known_ids)
            ):
                raise ValueError("Active Catchuparr source priority account is invalid")
            priority_ids.add(account_id)
        if mode != "priority" and priorities:
            raise ValueError("Active Catchuparr priorities require priority mode")
        if mode == "unchanged" and (account_ids or priorities):
            raise ValueError("Active unchanged source policy cannot select accounts")


def _validate_account_id_list(values, field_name: str) -> list[str]:
    if not isinstance(values, list):
        raise ValueError(f"Active Catchuparr {field_name} are invalid")
    result = [_validate_account_id(value, field_name) for value in values]
    if len(set(result)) != len(result):
        raise ValueError(f"Active Catchuparr {field_name} contain duplicates")
    return result


def _validate_account_id(value, field_name: str) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"Active Catchuparr {field_name} are invalid")
    text = str(value)
    if not text.isdigit() or int(text) <= 0:
        raise ValueError(f"Active Catchuparr {field_name} are invalid")
    return text


def _atomic_json_replace(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _config_lock(path, exclusive=True):
        descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as output:
                json.dump(value, output, sort_keys=True, separators=(",", ":"))
                output.write("\n")
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, path)
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass


class _config_lock:
    def __init__(self, path: Path, *, exclusive: bool):
        self.path = path
        self.exclusive = exclusive
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.with_suffix(self.path.suffix + ".lock").open("a+")
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX if self.exclusive else fcntl.LOCK_SH)
        return self

    def __exit__(self, exc_type, exc, traceback):
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        self.handle.close()

