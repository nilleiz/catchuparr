"""Draft validation and active source policy persistence.

Dispatcharr keeps editable settings in PluginConfig. Source policies are compiled
against the current channel assignments and stored separately so a draft never
changes recorder behavior until Apply succeeds.
"""

from __future__ import annotations

import base64
import binascii
import fcntl
import hashlib
import json
import os
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .runtime import PLUGIN_KEY, parse_settings

ACTIVE_CONFIG_NAME = ".catchuparr-active-settings.json"
ACTIVE_CONFIG_VERSION = 4
SUPPORTED_ACTIVE_CONFIG_VERSIONS = frozenset({2, 3, ACTIVE_CONFIG_VERSION})
RESET_REQUIRED_NAME = ".catchuparr-configuration-reset-required"
ACTIVATION_STATE_NAME = ".catchuparr-configuration-activation.json"
ACTIVATION_BACKUP_NAME = ".catchuparr-configuration-activation-previous.json"
ACTIVATION_COMMIT_NAME = ".catchuparr-configuration-activation-commit.json"
ACTIVATION_STATE_VERSION = 1


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
    parsed_settings = _applicable_settings(settings)
    recording_enabled = settings.get("recording_enabled", True)
    if type(recording_enabled) is not bool:
        raise ValueError("recording_enabled must be boolean")
    from .logging_utils import normalize_log_level

    log_level = normalize_log_level(settings.get("log_level", "INFO"))
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
    parsed_settings["recording_enabled"] = recording_enabled
    parsed_settings["log_level"] = log_level
    return {
        "settings": parsed_settings,
        "channel_uuids": list(compiled.channel_uuids),
        "channel_profile_ids": list(compiled.profile_ids),
        "source_policies": encoded,
        "recording_schedule": {
            "timezone": compiled.timezone,
            "channels": {
                channel_uuid: compiled.channel_schedules[channel_uuid].to_snapshot()
                for channel_uuid in compiled.channel_uuids
            },
        },
        "recording_enabled": recording_enabled,
        "log_level": log_level,
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
    settings: dict | None = None,
    catalog: SourceCatalog | None = None,
    active_path: Path | None = None,
) -> dict[str, Any]:
    """Compile under the shared activation lock before replacing applied state."""
    path = Path(active_path) if active_path is not None else active_settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _config_lock(path, exclusive=True):
        if settings is None:
            prepared = None
            try:
                with _current_settings_transaction() as current_settings:
                    result, prepared = _apply_configuration_locked(
                        current_settings, catalog, path
                    )
            except Exception:
                if prepared is not None:
                    prepared.rollback()
                raise
        else:
            result, prepared = _apply_configuration_locked(settings, catalog, path)
        prepared.commit()
        return result


def _apply_configuration_locked(
    settings: dict,
    catalog: SourceCatalog | None,
    path: Path,
) -> tuple[dict[str, Any], "_PreparedActivation"]:
    active, _, previews = compile_draft(settings, catalog)
    document = {
        "version": ACTIVE_CONFIG_VERSION,
        "settings": active["settings"],
        "channel_uuids": active["channel_uuids"],
        "channel_profile_ids": active["channel_profile_ids"],
        "source_policies": active["source_policies"],
        "recording_schedule": active["recording_schedule"],
    }
    _validate_active_document(document)
    _prevalidate_archive_root(document["settings"]["archive_root"])
    try:
        previous_active = _load_active_configuration_locked(path)
    except ValueError:
        # A validated Apply is also the recovery path for a malformed active
        # snapshot. The old archive location is unknown in that case.
        previous_active = None
    archive_root_changed = bool(
        previous_active is not None
        and Path(previous_active["archive_root"]).expanduser().resolve()
        != Path(document["settings"]["archive_root"]).expanduser().resolve()
    )
    prepared = _prepare_document_with_control_locked(path, document)
    return {
        "applied": True,
        "selected_channel_count": len(active["channel_uuids"]),
        "source_policy_count": len(active["source_policies"]),
        "message": (
            "No channels are selected." if not active["channel_uuids"]
            else f"{len(active['channel_uuids'])} channel(s) selected."
        ),
        "archive_root_changed": archive_root_changed,
        "archive_notice": (
            "Existing archive data and access tokens remain at the previous archive path; "
            "they were not moved or deleted."
            if archive_root_changed else None
        ),
        "channels": previews,
    }, prepared


def _prevalidate_archive_root(raw_path: str) -> None:
    """Confirm the applied archive location can be written without touching old data."""
    path = Path(raw_path).expanduser()
    try:
        path.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=".catchuparr-apply-check.", dir=path)
        try:
            with os.fdopen(descriptor, "wb") as output:
                output.write(b"ok\n")
                output.flush()
                os.fsync(output.fileno())
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
        directory = os.open(path, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError:
        raise ValueError("archive_root is not accessible for recording and playback") from None


class _PreparedActivation:
    """Keep admission denied until storage and the DB transaction both finish."""

    def __init__(
        self,
        active_path: Path,
        sidecar: Path,
        previous_active: bytes | None,
        previous_control: bytes | None,
        marker_was_present: bool,
        activation_id: str,
    ):
        self.active_path = active_path
        self.sidecar = sidecar
        self.previous_active = previous_active
        self.previous_control = previous_control
        self.marker_was_present = marker_was_present
        self.activation_id = activation_id
        self.finished = False

    def commit(self) -> None:
        if self.finished:
            return
        from .recorder_control import RecorderControlError

        try:
            _write_activation_commit_locked(self.active_path, self.activation_id)
        except OSError:
            # The pending journal remains authoritative if the commit record
            # could not be confirmed durable. Do not infer durability from a
            # file that may merely be visible in the current process.
            raise RecorderControlError(
                "configuration commit durability could not be confirmed; previous settings remain active"
            ) from None
        try:
            _write_activation_state_locked(
                self.active_path, "committed", self.activation_id
            )
        except OSError:
            state = _read_activation_state_locked(self.active_path)
            if state != ("committed", self.activation_id):
                raise RecorderControlError(
                    "configuration commit could not be completed; previous settings remain active"
                ) from None
        self.finished = True
        try:
            from .recorder_control import _clear_deny_marker

            _clear_deny_marker(self.sidecar)
        except Exception:
            # The configuration is durably committed; a leftover deny marker
            # only pauses admission and will be handled on the next recovery.
            pass
        try:
            _remove_reset_required_marker(self.active_path)
        except OSError:
            pass
        _cleanup_committed_activation_locked(self.active_path)

    def rollback(self) -> bool:
        if self.finished:
            return True
        active_restored = _restore_file_bytes_locked(self.active_path, self.previous_active)
        control_restored = _restore_file_bytes_locked(self.sidecar, self.previous_control)
        restored = active_restored and control_restored
        if restored:
            try:
                _restore_control_deny_marker(self.sidecar, self.marker_was_present)
            except Exception:
                restored = False
        if restored:
            try:
                _remove_activation_state_locked(self.active_path)
                _remove_activation_backup_locked(self.active_path)
                _remove_activation_commit_locked(self.active_path)
            except Exception:
                restored = False
        self.finished = restored
        return restored


def _prepare_document_with_control_locked(
    path: Path, document: dict[str, Any]
) -> _PreparedActivation:
    """Coordinate active and control files behind one fail-closed marker."""
    from .recorder_control import (
        RecorderControlError,
        _configuration_control_state_locked,
        _deny_marker_exists,
        _write_deny_marker,
        control_deny_path,
        control_state_path,
    )

    sidecar = control_state_path(path)
    _recover_interrupted_activation_locked(path)
    previous_active = _read_file_bytes(path)
    previous_control = _read_file_bytes(sidecar)
    marker_was_present = _deny_marker_exists(control_deny_path(path))
    activation_id = str(uuid.uuid4())
    backup = {
        "version": ACTIVATION_STATE_VERSION,
        "activation_id": activation_id,
        "active": _encode_optional_bytes(previous_active),
        "control": _encode_optional_bytes(previous_control),
        "control_deny_present": marker_was_present,
    }
    _write_activation_backup_locked(path, backup)
    _write_activation_state_locked(path, "pending", activation_id)
    try:
        _write_deny_marker(sidecar)
    except OSError:
        _remove_activation_state_locked(path)
        _remove_activation_backup_locked(path)
        raise RecorderControlError("configuration activation could not deny recorder admission") from None

    active = _active_configuration_view(document)
    generation = active_configuration_generation(active)
    prepared = _PreparedActivation(
        path, sidecar, previous_active, previous_control, marker_was_present,
        activation_id,
    )
    try:
        _atomic_json_replace_locked(path, document)
        _configuration_control_state_locked(
            sidecar,
            recording_enabled=document["settings"]["recording_enabled"],
            configuration_generation=generation,
            recovering_pending=marker_was_present,
        )
    except Exception:
        restored = prepared.rollback()
        if restored:
            raise RecorderControlError(
                "configuration activation failed; the previous configuration was restored"
            ) from None
        raise RecorderControlError(
            "configuration activation failed; recorder admission remains denied"
        ) from None
    return prepared


def _read_file_bytes(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except FileNotFoundError:
        return None


def _activation_state_path(path: Path) -> Path:
    return path.with_name(ACTIVATION_STATE_NAME)


def _activation_backup_path(path: Path) -> Path:
    return path.with_name(ACTIVATION_BACKUP_NAME)


def _activation_commit_path(path: Path) -> Path:
    return path.with_name(ACTIVATION_COMMIT_NAME)


def _encode_optional_bytes(value: bytes | None) -> str | None:
    return base64.b64encode(value).decode("ascii") if value is not None else None


def _decode_optional_bytes(value: Any) -> bytes | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("Configuration activation backup is invalid")
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        raise ValueError("Configuration activation backup is invalid") from None


def _write_activation_backup_locked(path: Path, backup: dict[str, Any]) -> None:
    _atomic_json_replace_locked(_activation_backup_path(path), backup)


def _read_activation_backup_locked(path: Path) -> dict[str, Any]:
    backup_path = _activation_backup_path(path)
    try:
        raw = json.loads(backup_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("Configuration activation backup is unavailable") from None
    if (
        not isinstance(raw, dict)
        or set(raw) != {
            "version", "activation_id", "active", "control", "control_deny_present"
        }
        or type(raw.get("version")) is not int
        or raw["version"] != ACTIVATION_STATE_VERSION
        or not _valid_activation_id(raw.get("activation_id"))
        or type(raw.get("control_deny_present")) is not bool
    ):
        raise ValueError("Configuration activation backup is invalid")
    return {
        "activation_id": raw["activation_id"],
        "active": _decode_optional_bytes(raw["active"]),
        "control": _decode_optional_bytes(raw["control"]),
        "control_deny_present": raw["control_deny_present"],
    }


def _valid_activation_id(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return str(uuid.UUID(value)) == value
    except ValueError:
        return False


def _read_activation_state_locked(path: Path) -> tuple[str, str] | None:
    state_path = _activation_state_path(path)
    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("Configuration activation state is unreadable") from None
    if (
        not isinstance(raw, dict)
        or set(raw) != {"version", "state", "activation_id"}
        or type(raw.get("version")) is not int
        or raw["version"] != ACTIVATION_STATE_VERSION
        or raw.get("state") not in {"pending", "committed"}
        or not _valid_activation_id(raw.get("activation_id"))
    ):
        raise ValueError("Configuration activation state is invalid")
    return raw["state"], raw["activation_id"]


def _write_activation_state_locked(path: Path, state: str, activation_id: str) -> None:
    if state not in {"pending", "committed"} or not _valid_activation_id(activation_id):
        raise ValueError("Configuration activation state is invalid")
    _atomic_json_replace_locked(
        _activation_state_path(path),
        {
            "version": ACTIVATION_STATE_VERSION,
            "state": state,
            "activation_id": activation_id,
        },
    )


def _write_activation_commit_locked(path: Path, activation_id: str) -> None:
    _atomic_json_replace_locked(
        _activation_commit_path(path),
        {"version": ACTIVATION_STATE_VERSION, "activation_id": activation_id},
    )


def _read_activation_commit_locked(path: Path) -> str | None:
    try:
        raw = json.loads(_activation_commit_path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if (
        not isinstance(raw, dict)
        or set(raw) != {"version", "activation_id"}
        or type(raw.get("version")) is not int
        or raw["version"] != ACTIVATION_STATE_VERSION
        or not _valid_activation_id(raw.get("activation_id"))
    ):
        return None
    return raw["activation_id"]


def _remove_durable_file(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _remove_activation_state_locked(path: Path) -> None:
    _remove_durable_file(_activation_state_path(path))


def _remove_activation_backup_locked(path: Path) -> None:
    _remove_durable_file(_activation_backup_path(path))


def _remove_activation_commit_locked(path: Path) -> None:
    _remove_durable_file(_activation_commit_path(path))


def _restore_control_deny_marker(sidecar: Path, present: bool) -> None:
    from .recorder_control import _clear_deny_marker, _write_deny_marker

    if present:
        _write_deny_marker(sidecar)
    else:
        _clear_deny_marker(sidecar)


def _restore_activation_backup_locked(path: Path, backup: dict[str, Any]) -> bool:
    from .recorder_control import control_state_path

    sidecar = control_state_path(path)
    active_restored = _restore_file_bytes_locked(path, backup["active"])
    control_restored = _restore_file_bytes_locked(sidecar, backup["control"])
    if not active_restored or not control_restored:
        return False
    try:
        _restore_control_deny_marker(sidecar, backup["control_deny_present"])
    except Exception:
        return False
    return True


def _recover_interrupted_activation_locked(path: Path) -> None:
    state = _read_activation_state_locked(path)
    if state is None:
        _remove_activation_backup_locked(path)
        _remove_activation_commit_locked(path)
        return
    state_name, activation_id = state
    backup = _read_activation_backup_locked(path)
    if backup["activation_id"] != activation_id:
        raise ValueError("Configuration activation backup does not match its journal")
    if state_name == "pending":
        if not _restore_activation_backup_locked(path, backup):
            raise ValueError("Interrupted configuration activation could not be restored")
    elif _read_activation_commit_locked(path) != activation_id:
        if not _restore_activation_backup_locked(path, backup):
            raise ValueError("Unconfirmed configuration activation could not be restored")
    else:
        try:
            from .recorder_control import _clear_deny_marker, control_state_path

            _clear_deny_marker(control_state_path(path))
        except Exception:
            # A leftover deny marker only blocks recording. The committed
            # snapshot remains authoritative and cleanup can be retried later.
            pass
    _remove_activation_state_locked(path)
    _remove_activation_backup_locked(path)
    _remove_activation_commit_locked(path)


def _cleanup_committed_activation_locked(path: Path) -> None:
    try:
        _remove_activation_state_locked(path)
    except OSError:
        return
    try:
        _remove_activation_backup_locked(path)
    except OSError:
        pass
    try:
        _remove_activation_commit_locked(path)
    except OSError:
        pass


def _active_configuration_from_bytes(contents: bytes | None) -> dict[str, Any] | None:
    if contents is None:
        return None
    try:
        data = json.loads(contents.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("Active Catchuparr configuration is invalid") from None
    if not isinstance(data, dict):
        raise ValueError("Active Catchuparr configuration is invalid")
    _validate_active_document(data)
    return _active_configuration_view(data)


def _restore_file_bytes_locked(path: Path, contents: bytes | None) -> bool:
    if contents is None:
        try:
            path.unlink(missing_ok=True)
            descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            return True
        except OSError:
            return False
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.restore.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(contents)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return True
    except OSError:
        return False
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


@contextmanager
def _current_settings_transaction():
    """Yield the current persisted draft while its row remains locked."""
    try:
        from apps.plugins.models import PluginConfig
        from django.db import transaction
    except (ImportError, ModuleNotFoundError):
        raise ValueError("Dispatcharr settings are unavailable for Apply") from None
    with transaction.atomic():
        row = (
            PluginConfig.objects.select_for_update()
            .filter(key=PLUGIN_KEY)
            .first()
        )
        if row is None or not isinstance(row.settings, dict):
            raise ValueError("Catchuparr settings are unavailable for Apply")
        yield dict(row.settings)


def active_settings_path() -> Path:
    """Locate private active state beside the replaceable plugin directory."""
    return Path(__file__).resolve().parent.parent / ACTIVE_CONFIG_NAME


def _applicable_settings(settings: dict) -> dict[str, Any]:
    """Validate and persist all non-secret settings activated by Apply."""
    if any(key in settings for key in ("channel_uuids", "source_rules")):
        raise ValueError("legacy channel selection was removed; use filter_config")
    parsed = parse_settings(settings)
    from .logging_utils import normalize_log_level
    from .runtime import normalize_public_base_url

    recording_enabled = settings.get("recording_enabled", True)
    if type(recording_enabled) is not bool:
        raise ValueError("recording_enabled must be boolean")
    applicable: dict[str, Any] = {
        "archive_root": str(parsed.archive_root),
        "retention_hours": parsed.retention_hours,
        "max_storage_gib": parsed.max_storage_bytes // 1024**3,
        "recording_enabled": recording_enabled,
        "log_level": normalize_log_level(settings.get("log_level", "INFO")),
        "public_base_url": normalize_public_base_url(settings.get("public_base_url", "")),
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
    """Load a validated applied snapshot, or None before the first valid Apply."""
    path = Path(active_path) if active_path is not None else active_settings_path()
    try:
        with _config_lock(path, exclusive=False):
            return _load_active_configuration_locked(path)
    except FileNotFoundError:
        return None


def load_applied_state(
    active_path: Path | None = None,
) -> tuple[dict[str, Any] | None, Any]:
    """Read one validated snapshot/control pair under their shared lock."""
    from .recorder_control import (
        RecorderControlError,
        _deny_marker_exists,
        _load_or_initialize_locked,
        control_deny_path,
        control_state_path,
    )

    path = Path(active_path) if active_path is not None else active_settings_path()
    with _config_lock(path, exclusive=True):
        try:
            activation_state = _read_activation_state_locked(path)
        except ValueError:
            raise RecorderControlError("configuration activation state is invalid") from None
        if activation_state is not None and (
            activation_state[0] == "pending"
            or _read_activation_commit_locked(path) != activation_state[1]
        ):
            raise RecorderControlError("configuration activation is incomplete")
        active = _load_active_configuration_locked(path)
        sidecar = control_state_path(path)
        if active is not None and active.get("version", 0) >= 4:
            # A v4 Apply always commits its bound control sidecar under the same
            # lock. Missing state means a partial activation and is not repaired
            # by assuming recording is enabled.
            _deny_marker_exists(control_deny_path(path))
            if not sidecar.exists():
                raise RecorderControlError("applied recorder control state is missing")
        control = _load_or_initialize_locked(sidecar)
        if active is not None and active.get("version", 0) >= 4:
            active_generation = active_configuration_generation(active)
            if control.configuration_generation != active_generation:
                raise RecorderControlError("applied configuration and control state do not match")
        return active, control


def _load_active_configuration_locked(path: Path) -> dict[str, Any] | None:
    activation_state = _read_activation_state_locked(path)
    if activation_state is not None and (
        activation_state[0] == "pending"
        or _read_activation_commit_locked(path) != activation_state[1]
    ):
        backup = _read_activation_backup_locked(path)
        if backup["activation_id"] != activation_state[1]:
            raise ValueError("Configuration activation backup does not match its journal")
        return _active_configuration_from_bytes(backup["active"])
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    if not isinstance(data, dict):
        raise ValueError("Active Catchuparr configuration is invalid")
    _validate_active_document(data)
    return _active_configuration_view(data)


def _active_configuration_view(data: dict[str, Any]) -> dict[str, Any]:
    channel_ids = data["channel_uuids"]
    if data["version"] == 2:
        from .schedule import CONTINUOUS_SCHEDULE, DEFAULT_TIMEZONE

        recording_schedule = {
            "timezone": DEFAULT_TIMEZONE,
            "channels": {
                channel: CONTINUOUS_SCHEDULE.to_snapshot() for channel in channel_ids
            },
        }
    else:
        recording_schedule = data["recording_schedule"]
    return {
        **data["settings"],
        "channel_uuids": "\n".join(channel_ids),
        "channel_profile_ids": list(data["channel_profile_ids"]),
        "source_policies": data["source_policies"],
        "recording_schedule": recording_schedule,
        "version": data["version"],
    }


def active_configuration_generation(active: dict[str, Any]) -> str:
    """Return the stable generation used to bind control and queued tasks."""
    serialized = json.dumps(active, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


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
        applied_snapshot = (
            type(current_version) is int
            and current_version in SUPPORTED_ACTIVE_CONFIG_VERSIONS
        )
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
            if applied_snapshot:
                if legacy_draft:
                    for key in old_fields:
                        settings.pop(key, None)
                    row.settings = settings
                    row.save(update_fields=["settings"])
                # A supported applied snapshot is proof that a fresh Apply succeeded. Clear
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

    version = data.get("version")
    if type(version) is not int or version not in SUPPORTED_ACTIVE_CONFIG_VERSIONS:
        raise ValueError("Active Catchuparr configuration version is invalid")
    expected_fields = {
        "version", "settings", "channel_uuids", "channel_profile_ids", "source_policies"
    }
    if version >= 3:
        expected_fields.add("recording_schedule")
    if set(data) != expected_fields:
        raise ValueError("Active Catchuparr configuration fields are invalid")
    settings = data["settings"]
    if not isinstance(settings, dict) or not isinstance(data.get("source_policies"), dict):
        raise ValueError("Active Catchuparr configuration is invalid")
    allowed_settings = {
        "archive_root",
        "retention_hours",
        "max_storage_gib",
        "playback_user_id",
    }
    if version >= 4:
        allowed_settings.update(
            {"recording_enabled", "log_level", "public_base_url"}
        )
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
    if version >= 4:
        if type(settings.get("recording_enabled")) is not bool:
            raise ValueError("Active Catchuparr recording_enabled is invalid")
        from .logging_utils import normalize_log_level

        try:
            normalized_level = normalize_log_level(settings.get("log_level"))
        except ValueError:
            raise ValueError("Active Catchuparr log_level is invalid") from None
        if normalized_level != settings.get("log_level"):
            raise ValueError("Active Catchuparr log_level is not normalized")
        from .runtime import normalize_public_base_url

        try:
            normalized_url = normalize_public_base_url(settings.get("public_base_url"))
        except ValueError:
            raise ValueError("Active Catchuparr public_base_url is invalid") from None
        if normalized_url != settings.get("public_base_url"):
            raise ValueError("Active Catchuparr public_base_url is not normalized")
    parse_settings(settings)
    channels = set(normalized_channels)
    if version >= 3:
        _validate_recording_schedule(data.get("recording_schedule"), normalized_channels)
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


def _validate_recording_schedule(value: Any, channel_uuids: list[str]) -> None:
    from .schedule import ScheduleError, schedule_from_snapshot, validate_timezone

    if not isinstance(value, dict) or set(value) != {"timezone", "channels"}:
        raise ValueError("Active Catchuparr recording schedule is invalid")
    try:
        validate_timezone(value["timezone"])
    except ScheduleError:
        raise ValueError("Active Catchuparr recording timezone is invalid") from None
    schedules = value["channels"]
    if not isinstance(schedules, dict) or set(schedules) != set(channel_uuids):
        raise ValueError("Active Catchuparr recording schedule channels are invalid")
    for snapshot in schedules.values():
        try:
            schedule_from_snapshot(snapshot)
        except ScheduleError:
            raise ValueError("Active Catchuparr channel schedule is invalid") from None


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
        _atomic_json_replace_locked(path, value)


def _atomic_json_replace_locked(path: Path, value: dict[str, Any]) -> None:
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
