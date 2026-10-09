"""Durable recorder-only pause state, independent of archive playback state."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CONTROL_STATE_NAME = ".catchuparr-recorder-control.json"
CONTROL_DENY_NAME = ".catchuparr-recorder-control-deny"
CONTROL_STATE_VERSION = 2


class RecorderControlError(ValueError):
    """Recorder control is unavailable or its persisted state is invalid."""


@dataclass(frozen=True)
class RecorderControlState:
    paused: bool
    generation: int
    configuration_generation: str | None = None

    @property
    def recording_enabled(self) -> bool:
        return not self.paused

    def to_json(self) -> dict[str, Any]:
        if self.configuration_generation is not None:
            return {
                "version": CONTROL_STATE_VERSION,
                "paused": self.paused,
                "generation": self.generation,
                "configuration_generation": self.configuration_generation,
            }
        return {
            "version": 1,
            "paused": self.paused,
            "generation": self.generation,
        }


def control_state_path(active_path: Path | None = None) -> Path:
    """Return the control sidecar path adjacent to the active config snapshot."""
    path = Path(active_path) if active_path is not None else _default_active_path()
    return path.with_name(CONTROL_STATE_NAME)


def control_deny_path(active_path: Path | None = None) -> Path:
    """Return the persistent admission-deny marker beside the control state."""
    path = Path(active_path) if active_path is not None else _default_active_path()
    return path.with_name(CONTROL_DENY_NAME)


def load_recorder_control(active_path: Path | None = None) -> RecorderControlState:
    """Read control state, initializing when absent and denying pending changes."""
    from .configuration import _config_lock

    active = Path(active_path) if active_path is not None else _default_active_path()
    sidecar = control_state_path(active)
    with _config_lock(active, exclusive=True):
        return _load_or_initialize_locked(sidecar)


def apply_recorder_control(active_path: Path | None = None) -> RecorderControlState:
    """Apply the persisted recording_enabled setting without parsing filter YAML."""
    from .configuration import _config_lock

    active = Path(active_path) if active_path is not None else _default_active_path()
    sidecar = control_state_path(active)
    with _config_lock(active, exclusive=True):
        current = _load_or_initialize_locked(sidecar, allow_pending=True)
        try:
            enabled = _read_recording_enabled_setting()
        except RecorderControlError:
            _force_paused_locked(sidecar, current)
            raise
        return _transition_locked(sidecar, current, paused=not enabled)


def pause_recorders(active_path: Path | None = None) -> RecorderControlState:
    """Pause all recording and mirror the resulting state to PluginConfig."""
    return _set_recording_enabled(False, active_path)


def resume_recorders(active_path: Path | None = None) -> RecorderControlState:
    """Resume recording and mirror the resulting state to PluginConfig."""
    return _set_recording_enabled(True, active_path)


def _set_recording_enabled(enabled: bool, active_path: Path | None) -> RecorderControlState:
    if type(enabled) is not bool:
        raise RecorderControlError("recording_enabled must be boolean")
    from .configuration import _config_lock

    active = Path(active_path) if active_path is not None else _default_active_path()
    sidecar = control_state_path(active)
    with _config_lock(active, exclusive=True):
        current = _load_or_initialize_locked(sidecar, allow_pending=True)
        target_paused = not enabled
        if target_paused:
            current = _transition_locked(sidecar, current, paused=True)
            try:
                _write_recording_enabled_setting(enabled)
            except RecorderControlError:
                # The filesystem state is already paused, so a database failure
                # cannot accidentally reopen recording.
                raise
            return current

        try:
            _write_recording_enabled_setting(enabled)
        except RecorderControlError:
            _force_paused_locked(sidecar, current)
            raise
        return _transition_locked(sidecar, current, paused=False)


def _default_active_path() -> Path:
    from .configuration import active_settings_path

    return active_settings_path()


def _load_or_initialize_locked(
    sidecar: Path, *, allow_pending: bool = False
) -> RecorderControlState:
    marker = control_deny_path(sidecar)
    pending = _deny_marker_exists(marker)
    if pending and not allow_pending:
        raise RecorderControlError("recorder admission is denied by a pending control transition")
    try:
        raw = json.loads(sidecar.read_text(encoding="utf-8"))
    except FileNotFoundError:
        initial = RecorderControlState(paused=False, generation=0)
        try:
            if not pending:
                _write_deny_marker(sidecar)
            _atomic_replace_sidecar(sidecar, initial)
            if not allow_pending:
                _clear_deny_marker(sidecar)
        except OSError:
            raise RecorderControlError("recorder control state could not be initialized") from None
        except RecorderControlError:
            raise RecorderControlError("recorder control state could not be initialized") from None
        return initial
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise RecorderControlError("recorder control state is unreadable") from None
    return _parse_state(raw)


def _configuration_control_state_locked(
    sidecar: Path,
    *,
    recording_enabled: bool,
    configuration_generation: str,
) -> RecorderControlState:
    """Write the control half of a unified Apply while its deny marker is held."""
    if type(recording_enabled) is not bool:
        raise RecorderControlError("recording_enabled must be boolean")
    if (
        not isinstance(configuration_generation, str)
        or len(configuration_generation) != 64
        or any(character not in "0123456789abcdef" for character in configuration_generation)
    ):
        raise RecorderControlError("applied configuration generation is invalid")
    current = _load_or_initialize_locked(sidecar, allow_pending=True)
    paused = not recording_enabled
    changed = (
        current.paused != paused
        or current.configuration_generation != configuration_generation
    )
    updated = RecorderControlState(
        paused=paused,
        generation=current.generation + (1 if changed else 0),
        configuration_generation=configuration_generation,
    )
    try:
        _atomic_replace_sidecar(sidecar, updated)
    except OSError:
        raise RecorderControlError("configuration control state could not be saved") from None
    return updated


def _parse_state(raw: Any) -> RecorderControlState:
    if not isinstance(raw, dict):
        raise RecorderControlError("recorder control state is invalid")
    version = raw.get("version")
    expected = {"version", "paused", "generation"}
    if version == 2:
        expected.add("configuration_generation")
    if (
        type(version) is not int
        or version not in {1, CONTROL_STATE_VERSION}
        or set(raw) != expected
        or type(raw.get("paused")) is not bool
        or type(raw.get("generation")) is not int
        or raw["generation"] < 0
    ):
        raise RecorderControlError("recorder control state is invalid")
    config_generation = raw.get("configuration_generation")
    if version == 2 and (
        not isinstance(config_generation, str)
        or len(config_generation) != 64
        or any(character not in "0123456789abcdef" for character in config_generation)
    ):
        raise RecorderControlError("recorder control state is invalid")
    return RecorderControlState(raw["paused"], raw["generation"], config_generation)


def _transition_locked(
    sidecar: Path,
    current: RecorderControlState,
    *,
    paused: bool,
) -> RecorderControlState:
    marker_pending = _deny_marker_exists(control_deny_path(sidecar))
    if current.paused == paused and not marker_pending:
        return current
    try:
        # The marker is durably in place before any sidecar state that could
        # admit recording. Readers deny admission while it exists, including
        # after a replace that reports a later fsync error.
        _write_deny_marker(sidecar)
    except OSError:
        raise RecorderControlError("recorder control admission deny could not be persisted") from None
    # A pending deny marker may be the only evidence that a prior Pause
    # failed before publishing its paused sidecar. Re-enabling in that state
    # must fence jobs queued under the old generation even if the sidecar
    # already reads unpaused.
    advance_generation = current.paused != paused or (marker_pending and not paused)
    updated = RecorderControlState(
        paused=paused,
        generation=current.generation + (1 if advance_generation else 0),
        configuration_generation=current.configuration_generation,
    )
    try:
        _atomic_replace_sidecar(sidecar, updated)
    except OSError:
        # Keep the deny marker on every state-write failure. A published
        # unpaused sidecar is still non-authoritative while that marker exists.
        if paused:
            try:
                _write_paused_in_place(sidecar, updated)
            except OSError:
                pass
        raise RecorderControlError("recorder control transition failed; admission remains denied") from None
    try:
        _clear_deny_marker(sidecar)
    except RecorderControlError:
        raise RecorderControlError("recorder control transition remains denied") from None
    return updated


def _force_paused_locked(
    sidecar: Path,
    current: RecorderControlState,
) -> RecorderControlState:
    marker_pending = _deny_marker_exists(control_deny_path(sidecar))
    if current.paused and not marker_pending:
        return current
    generation = current.generation + (0 if current.paused else 1)
    paused = RecorderControlState(
        paused=True,
        generation=generation,
        configuration_generation=current.configuration_generation,
    )
    try:
        _write_deny_marker(sidecar)
        _atomic_replace_sidecar(sidecar, paused)
    except OSError:
        try:
            _write_paused_in_place(sidecar, paused)
        except OSError:
            raise RecorderControlError(
                "recorder control could not confirm a paused state; admission remains denied"
            ) from None
    _clear_deny_marker(sidecar)
    return paused


def _deny_marker_exists(marker: Path) -> bool:
    try:
        marker.lstat()
    except FileNotFoundError:
        return False
    except OSError:
        # An unreadable marker path is not evidence that recording is safe.
        raise RecorderControlError("recorder admission deny state is unreadable") from None
    return True


def _write_deny_marker(sidecar: Path) -> None:
    marker = control_deny_path(sidecar)
    marker.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{marker.name}.", dir=marker.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write("deny\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, marker)
        _fsync_directory(marker.parent)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _clear_deny_marker(sidecar: Path) -> None:
    marker = control_deny_path(sidecar)
    try:
        marker.unlink()
    except FileNotFoundError:
        return
    except OSError:
        raise RecorderControlError("recorder admission deny state could not be cleared") from None

    # The control sidecar is fsynced before this unlink. If syncing the parent
    # fails, a crash may restore the marker, which only denies admission; the
    # current process may safely treat the durable sidecar as authoritative.
    try:
        _fsync_directory(marker.parent)
    except OSError:
        pass


def _fsync_directory(path: Path) -> None:
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _write_paused_in_place(path: Path, state: RecorderControlState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as output:
        json.dump(state.to_json(), output, sort_keys=True, separators=(",", ":"))
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _atomic_replace_sidecar(path: Path, state: RecorderControlState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(state.to_json(), output, sort_keys=True, separators=(",", ":"))
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


def _read_recording_enabled_setting() -> bool:
    try:
        PluginConfig, transaction = _plugin_config_api()
        with transaction.atomic():
            row = _locked_plugin_config(PluginConfig)
            enabled = _settings_from_row(row).get("recording_enabled", True)
    except RecorderControlError:
        raise
    except Exception:
        raise RecorderControlError("recording control setting could not be read") from None
    if type(enabled) is not bool:
        raise RecorderControlError("recording_enabled setting must be boolean")
    return enabled


def _write_recording_enabled_setting(enabled: bool) -> None:
    try:
        PluginConfig, transaction = _plugin_config_api()
        with transaction.atomic():
            row = _locked_plugin_config(PluginConfig)
            settings = _settings_from_row(row)
            if settings.get("recording_enabled", True) is not enabled:
                settings["recording_enabled"] = enabled
                row.settings = settings
                row.save(update_fields=["settings"])
    except RecorderControlError:
        raise
    except Exception:
        raise RecorderControlError("recording control setting could not be saved") from None


def _plugin_config_api():
    try:
        from apps.plugins.models import PluginConfig
        from django.db import transaction
    except (ImportError, ModuleNotFoundError):
        raise RecorderControlError("Dispatcharr recorder control storage is unavailable") from None
    return PluginConfig, transaction


def _locked_plugin_config(plugin_config_model):
    from .runtime import PLUGIN_KEY

    row = (
        plugin_config_model.objects.select_for_update()
        .filter(key=PLUGIN_KEY)
        .first()
    )
    if row is None:
        raise RecorderControlError("Catchuparr settings are unavailable")
    return row


def _settings_from_row(row) -> dict[str, Any]:
    settings = row.settings or {}
    if not isinstance(settings, dict):
        raise RecorderControlError("Catchuparr settings are invalid")
    return dict(settings)
