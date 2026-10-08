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
ACTIVE_CONFIG_VERSION = 2
RESET_REQUIRED_NAME = ".catchuparr-configuration-reset-required"


@dataclass(frozen=True)
class SourceCatalog:
    channels: tuple[dict[str, Any], ...]
    accounts: tuple[dict[str, Any], ...]
    streams_by_channel: dict[str, tuple[dict[str, Any], ...]]
    profiles: tuple[dict[str, Any], ...] = ()


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
    """Build safe DTOs from channels, enabled profile memberships and sources."""
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

    profile_by_id: dict[str, dict[str, Any]] = {}
    try:
        from apps.channels.models import ChannelProfile, ChannelProfileMembership

        profile_by_id = {
            str(profile.id): {
                "id": str(profile.id),
                "name": str(profile.name or ""),
                "channel_uuids": set(),
            }
            for profile in ChannelProfile.objects.all().only("id", "name")
        }
        memberships = ChannelProfileMembership.objects.filter(enabled=True).select_related(
            "channel_profile", "channel"
        )
        for membership in memberships:
            profile_id = str(
                getattr(membership, "channel_profile_id", None)
                or getattr(getattr(membership, "channel_profile", None), "id", "")
            )
            channel = getattr(membership, "channel", None)
            channel_uuid = str(getattr(channel, "uuid", "") or "")
            if profile_id in profile_by_id and channel_uuid in channel_by_uuid:
                profile_by_id[profile_id]["channel_uuids"].add(channel_uuid)
    except (ImportError, ModuleNotFoundError):
        pass

    profiles = tuple(
        {
            **profile,
            "channel_uuids": tuple(sorted(profile["channel_uuids"])),
        }
        for profile in sorted(profile_by_id.values(), key=lambda item: int(item["id"]))
    )

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
        profiles=profiles,
    )


def _group_name(channel) -> str:
    value = getattr(channel, "channel_group", None)
    return str(getattr(value, "name", "") or "")


def compile_draft(
    settings: dict, catalog: SourceCatalog | None = None
) -> tuple[dict[str, Any], SourceCatalog, list[dict[str, Any]]]:
    """Validate base fields and compile YAML filters into stable catalog IDs."""
    if any(key in settings for key in ("channel_uuids", "source_rules")):
        raise ValueError("legacy channel selection was removed; validate and apply filter_config")
    parsed_settings = parse_settings(settings)
    catalog = catalog or source_catalog()
    from .source_rules import compile_filter_config

    raw_filter_config = settings.get("filter_config", "")
    if raw_filter_config is None:
        raw_filter_config = ""
    if not isinstance(raw_filter_config, str):
        raise ValueError("filter_config must be text")
    compiled = compile_filter_config(
        raw_filter_config,
        catalog.channels,
        catalog.accounts,
        catalog.profiles,
        catalog.streams_by_channel,
    )
    encoded = {
        channel_uuid: {
            "include_account_ids": (
                sorted(policy.include_account_ids)
                if policy.include_account_ids is not None else None
            ),
            "exclude_account_ids": sorted(policy.exclude_account_ids),
            "priorities": [[account_id, score] for account_id, score in policy.priorities],
            "known_account_ids": sorted(policy.known_account_ids),
        }
        for channel_uuid, policy in compiled.source_policies.items()
    }
    previews = [
        {
            **preview,
            "warning": (
                "This filter opens a dedicated provider connection and may use "
                "additional provider or tuner capacity."
                if preview["source_override"] and preview["candidates"] else None
            ),
        }
        for preview in compiled.channels
    ]
    parsed = {
        "archive_root": str(parsed_settings.archive_root),
        "retention_hours": parsed_settings.retention_hours,
        "max_storage_gib": parsed_settings.max_storage_bytes // 1024**3,
    }
    if "playback_user_id" in settings:
        parsed["playback_user_id"] = settings["playback_user_id"]
    return {
        "settings": parsed,
        "channel_uuids": list(compiled.channel_uuids),
        "channel_profile_ids": list(compiled.profile_ids),
        "source_policies": encoded,
    }, catalog, previews


def validate_configuration(settings: dict, catalog: SourceCatalog | None = None) -> dict[str, Any]:
    active, _, previews = compile_draft(settings, catalog)
    return {
        "valid": True,
        "channels": previews,
        "selected_channel_count": len(active["channel_uuids"]),
        "source_policy_count": len(active["source_policies"]),
        "message": (
            "No channels are selected." if not active["channel_uuids"]
            else f"{len(active['channel_uuids'])} channel(s) selected."
        ),
    }


def apply_configuration(
    settings: dict,
    catalog: SourceCatalog | None = None,
    active_path: Path | None = None,
) -> dict[str, Any]:
    """Compile fully before atomically replacing the active source config."""
    active, _, previews = compile_draft(settings, catalog)
    path = Path(active_path) if active_path is not None else active_settings_path()
    document = {
        "version": ACTIVE_CONFIG_VERSION,
        "settings": active["settings"],
        "channel_uuids": active["channel_uuids"],
        "channel_profile_ids": active["channel_profile_ids"],
        "source_policies": active["source_policies"],
    }
    _validate_active_document(document)
    _atomic_json_replace(path, document)
    _remove_reset_required_marker(path)
    return {
        "applied": True,
        "selected_channel_count": len(active["channel_uuids"]),
        "source_policy_count": len(active["source_policies"]),
        "message": (
            "No channels are selected." if not active["channel_uuids"]
            else f"{len(active['channel_uuids'])} channel(s) selected."
        ),
        "channels": previews,
    }


def active_settings_path() -> Path:
    """Locate private active state beside the replaceable plugin directory."""
    return Path(__file__).resolve().parent.parent / ACTIVE_CONFIG_NAME


def _applicable_settings(settings: dict) -> dict[str, Any]:
    """Persist only known non-secret runtime and plugin action settings."""
    if any(key in settings for key in ("channel_uuids", "source_rules")):
        raise ValueError("legacy channel selection was removed; use filter_config")
    parsed = parse_settings(settings)
    applicable: dict[str, Any] = {
        "archive_root": str(parsed.archive_root),
        "retention_hours": parsed.retention_hours,
        "max_storage_gib": parsed.max_storage_bytes // 1024**3,
    }
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
    """Load a v2 applied snapshot, or None before the first valid Apply."""
    path = Path(active_path) if active_path is not None else active_settings_path()
    try:
        with _config_lock(path, exclusive=False):
            data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or set(data) != {
            "version", "settings", "channel_uuids", "channel_profile_ids", "source_policies"
        }:
            raise ValueError("Active Catchuparr configuration is invalid")
        if (
            type(data.get("version")) is not int
            or data.get("version") != ACTIVE_CONFIG_VERSION
            or not isinstance(data.get("source_policies"), dict)
            or not isinstance(data.get("settings"), dict)
        ):
            raise ValueError("Active Catchuparr configuration is invalid")
        _validate_active_document(data)
        return {
            **data["settings"],
            "channel_uuids": "\n".join(data["channel_uuids"]),
            "channel_profile_ids": list(data["channel_profile_ids"]),
            "source_policies": data["source_policies"],
        }
    except FileNotFoundError:
        return None


def configuration_reset_required(active_path: Path | None = None) -> bool:
    path = Path(active_path) if active_path is not None else active_settings_path()
    return path.with_name(RESET_REQUIRED_NAME).is_file()


def reset_legacy_configuration(active_path: Path | None = None) -> bool:
    """Clear only known pre-0.2.1 source selection before runtime startup."""
    path = Path(active_path) if active_path is not None else active_settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _config_lock(path, exclusive=True):
        try:
            current = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            current = None
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            # Corrupt state is not classified as a known legacy snapshot.
            current = None
        current_version = current.get("version") if isinstance(current, dict) else None
        applied_v2 = type(current_version) is int and current_version == ACTIVE_CONFIG_VERSION
        legacy_snapshot = type(current_version) is int and current_version == 1

        try:
            from apps.plugins.models import PluginConfig
            from django.db import transaction
        except (ImportError, ModuleNotFoundError):
            if legacy_snapshot:
                _delete_legacy_snapshot_locked(path)
                _write_reset_required_marker(path)
                return True
            return False

        with transaction.atomic():
            row = (
                PluginConfig.objects.select_for_update()
                .filter(key=PLUGIN_KEY)
                .first()
            )
            settings = dict(row.settings or {}) if row is not None else None
            old_fields = {"channel_uuids", "source_rules"}
            legacy_draft = settings is not None and bool(old_fields.intersection(settings))
            if applied_v2:
                if legacy_draft:
                    for key in old_fields:
                        settings.pop(key, None)
                    row.settings = settings
                    row.save(update_fields=["settings"])
                # A v2 snapshot is proof that a fresh Apply succeeded. Clear
                # an apply-required marker left by a process crash between the
                # atomic snapshot replace and marker removal.
                _remove_reset_required_marker(path)
                return legacy_draft
            if not legacy_snapshot and not legacy_draft:
                return False
            if settings is not None:
                for key in old_fields:
                    settings.pop(key, None)
                settings["filter_config"] = ""
                row.settings = settings
                row.save(update_fields=["settings"])
            if legacy_snapshot:
                _delete_legacy_snapshot_locked(path)
            _write_reset_required_marker(path)
            return True


def _delete_legacy_snapshot_locked(path: Path) -> None:
    try:
        latest = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return
    except (json.JSONDecodeError, UnicodeDecodeError, OSError):
        return
    if isinstance(latest, dict) and type(latest.get("version")) is int and latest["version"] == 1:
        path.unlink(missing_ok=True)


def _write_reset_required_marker(path: Path) -> None:
    marker = path.with_name(RESET_REQUIRED_NAME)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{marker.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write("apply-required\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, marker)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _remove_reset_required_marker(path: Path) -> None:
    path.with_name(RESET_REQUIRED_NAME).unlink(missing_ok=True)


def _validate_active_document(data: dict[str, Any]) -> None:
    """Reject malformed persisted policy data before any recorder can use it."""
    from .runtime import parse_settings

    settings = data["settings"]
    allowed_settings = {"archive_root", "retention_hours", "max_storage_gib", "playback_user_id"}
    if set(settings) - allowed_settings:
        raise ValueError("Active Catchuparr settings contain unknown fields")
    channel_uuids = data.get("channel_uuids")
    if not isinstance(channel_uuids, list):
        raise ValueError("Active Catchuparr channel IDs are invalid")
    normalized_channels = []
    for raw_channel in channel_uuids:
        try:
            channel_uuid = str(uuid.UUID(str(raw_channel)))
        except (ValueError, TypeError, AttributeError):
            raise ValueError("Active Catchuparr channel ID is invalid") from None
        if channel_uuid != raw_channel:
            raise ValueError("Active Catchuparr channel ID is not canonical")
        normalized_channels.append(channel_uuid)
    if len(normalized_channels) != len(set(normalized_channels)):
        raise ValueError("Active Catchuparr channel IDs contain duplicates")
    profile_ids = data.get("channel_profile_ids")
    if (
        not isinstance(profile_ids, list)
        or any(
            isinstance(value, bool)
            or not isinstance(value, str)
            or not value.isdigit()
            or int(value) <= 0
            or str(int(value)) != value
            for value in profile_ids
        )
        or len(profile_ids) != len(set(profile_ids))
    ):
        raise ValueError("Active Catchuparr channel profile IDs are invalid")
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
    parse_settings(settings)
    channels = set(normalized_channels)
    policies = data["source_policies"]
    for raw_channel, policy in policies.items():
        try:
            channel_uuid = str(uuid.UUID(str(raw_channel)))
        except (ValueError, TypeError, AttributeError):
            raise ValueError("Active Catchuparr source policy has an invalid channel ID") from None
        if channel_uuid != str(raw_channel) or channel_uuid not in channels:
            raise ValueError("Active Catchuparr source policy channel is not configured")
        if not isinstance(policy, dict) or set(policy) != {
            "include_account_ids", "exclude_account_ids", "priorities", "known_account_ids"
        }:
            raise ValueError("Active Catchuparr source policy is invalid")
        include_ids = policy.get("include_account_ids")
        if include_ids is not None:
            include_ids = _validate_account_id_list(include_ids, "included account IDs")
            if not include_ids:
                raise ValueError("Active Catchuparr source include filter is empty")
        exclude_ids = _validate_account_id_list(
            policy.get("exclude_account_ids"), "excluded account IDs"
        )
        if include_ids is not None and exclude_ids:
            raise ValueError("Active Catchuparr source policy has both include and exclude")
        known_ids = _validate_account_id_list(policy.get("known_account_ids"), "known account IDs")
        filter_ids = set(include_ids or ()) | set(exclude_ids)
        if not filter_ids <= set(known_ids):
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
            if account_id in priority_ids or account_id not in set(known_ids):
                raise ValueError("Active Catchuparr source priority account is invalid")
            priority_ids.add(account_id)
        if include_ids is not None and not priority_ids <= set(include_ids):
            raise ValueError("Active Catchuparr source priority violates the include filter")
        if priority_ids & set(exclude_ids):
            raise ValueError("Active Catchuparr source priority violates the exclude filter")
        if include_ids is None and not exclude_ids and not priorities:
            raise ValueError("Active Catchuparr source policy has no filter or ranking")


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
    if str(int(text)) != text:
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
