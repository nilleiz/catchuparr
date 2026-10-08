"""Durable recorder-only pause state, independent of archive playback state."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CONTROL_STATE_NAME = ".catchuparr-recorder-control.json"
CONTROL_STATE_VERSION = 1


class RecorderControlError(ValueError):
    """Recorder control is unavailable or its persisted state is invalid."""


@dataclass(frozen=True)
class RecorderControlState:
    paused: bool
    generation: int

    @property
    def recording_enabled(self) -> bool:
        return not self.paused

    def to_json(self) -> dict[str, Any]:
        return {
            "version": CONTROL_STATE_VERSION,
            "paused": self.paused,
            "generation": self.generation,
        }


def control_state_path(active_path: Path | None = None) -> Path:
    """Return the control sidecar path adjacent to the active config snapshot."""
    path = Path(active_path) if active_path is not None else _default_active_path()
    return path.with_name(CONTROL_STATE_NAME)


def load_recorder_control(active_path: Path | None = None) -> RecorderControlState:
    """Read control state, initializing the enabled default only when absent."""
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
        current = _load_or_initialize_locked(sidecar)
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
        current = _load_or_initialize_locked(sidecar)
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


def _load_or_initialize_locked(sidecar: Path) -> RecorderControlState:
    try:
        raw = json.loads(sidecar.read_text(encoding="utf-8"))
    except FileNotFoundError:
        initial = RecorderControlState(paused=False, generation=0)
        try:
            _atomic_replace_sidecar(sidecar, initial)
        except OSError:
            raise RecorderControlError("recorder control state could not be initialized") from None
        return initial
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise RecorderControlError("recorder control state is unreadable") from None
    return _parse_state(raw)


def _parse_state(raw: Any) -> RecorderControlState:
    if (
        not isinstance(raw, dict)
        or set(raw) != {"version", "paused", "generation"}
        or type(raw.get("version")) is not int
        or raw.get("version") != CONTROL_STATE_VERSION
        or type(raw.get("paused")) is not bool
        or type(raw.get("generation")) is not int
        or raw.get("generation") < 0
    ):
        raise RecorderControlError("recorder control state is invalid")
    return RecorderControlState(raw["paused"], raw["generation"])


def _transition_locked(
    sidecar: Path,
    current: RecorderControlState,
    *,
    paused: bool,
) -> RecorderControlState:
    if current.paused == paused:
        return current
    updated = RecorderControlState(paused=paused, generation=current.generation + 1)
    try:
        _atomic_replace_sidecar(sidecar, updated)
    except OSError:
        # A failed directory fsync can be reported after replace(2) has
        # already published the requested state. If that state was resume,
        # publish a newer paused generation even when `current` was paused.
        safe_generation = updated.generation + (1 if not paused else 0)
        _force_paused_locked(
            sidecar, current, minimum_generation=safe_generation
        )
        raise RecorderControlError("recorder control transition failed; recording remains paused") from None
    return updated


def _force_paused_locked(
    sidecar: Path,
    current: RecorderControlState,
    *,
    minimum_generation: int | None = None,
) -> RecorderControlState:
    generation = current.generation + (0 if current.paused else 1)
    if minimum_generation is not None:
        generation = max(generation, minimum_generation)
    paused = RecorderControlState(paused=True, generation=generation)
    try:
        # Rewrite even an already-paused state. An earlier replace may have
        # succeeded before its directory fsync reported failure.
        _atomic_replace_sidecar(sidecar, paused)
    except OSError:
        # If a rename fails, prefer a directly written paused document over
        # preserving a potentially published resume state. Truncation during
        # this fallback also fails closed because malformed JSON is denied.
        try:
            _write_paused_in_place(sidecar, paused)
        except OSError:
            raise RecorderControlError(
                "recorder control could not confirm a paused state"
            ) from None
    return paused


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
