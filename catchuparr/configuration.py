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

    policies = compile_source_rules(
        str(settings.get("source_rules") or ""), catalog.channels, catalog.accounts
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
    keys = (
        "channel_uuids", "archive_root", "retention_hours",
        "max_storage_gib", "source_rules", "playback_user_id",
    )
    return {key: settings[key] for key in keys if key in settings}


def load_active_configuration(active_path: Path | None = None) -> dict[str, Any] | None:
    """Load the applied settings snapshot, or None before the first Apply."""
    path = Path(active_path) if active_path is not None else active_settings_path()
    try:
        with _config_lock(path, exclusive=False):
            data = json.loads(path.read_text(encoding="utf-8"))
        if (
            data.get("version") != ACTIVE_CONFIG_VERSION
            or not isinstance(data.get("source_policies"), dict)
            or not isinstance(data.get("settings"), dict)
        ):
            raise ValueError("Active Catchuparr configuration is invalid")
        return {**data["settings"], "source_policies": data["source_policies"]}
    except FileNotFoundError:
        return None


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

