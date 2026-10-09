"""Scoped, sanitized logging controls for Catchuparr events."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass

LOG_LEVELS = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING, "ERROR": logging.ERROR}
MAX_ERROR_CATEGORIES = 64
ERROR_WINDOW_SECONDS = 300.0

_EVENTS = frozenset({
    "configuration_applied",
    "control_applied",
    "control_paused",
    "control_resumed",
    "recorder_disabled",
    "recorder_schedule_closed",
    "recorder_stale_job",
    "recorder_worker_stopped",
    "supervision_failed",
    "runtime_disabled",
    "route_install_failed",
    "schedule_install_failed",
    "xc_install_failed",
    "xc_uninstall_failed",
    "scheduler_disable_failed",
    "route_uninstall_failed",
    "control_state_invalid",
    "setting_invalid",
    "error_suppressed",
})
_STATE_KEYS = frozenset({
    "generation", "config_generation", "control_generation", "channel_count",
    "source_policy_count", "queued", "paused", "enabled", "count", "level",
    "reason",
})
_SAFE_REASONS = frozenset({
    "version", "proxy_api", "cleanup_guard", "xc_hooks", "xc_uninstall", "log_level",
})


@dataclass
class _ErrorWindow:
    started_at: float
    count: int = 0


_error_lock = threading.Lock()
_error_windows: dict[str, _ErrorWindow] = {}


def normalize_log_level(value: object) -> str:
    """Validate a setting without accepting arbitrary logger levels."""
    if not isinstance(value, str):
        raise ValueError("log_level must be DEBUG, INFO, WARNING, or ERROR")
    normalized = value.upper()
    if normalized not in LOG_LEVELS:
        raise ValueError("log_level must be DEBUG, INFO, WARNING, or ERROR")
    return normalized


def apply_log_level(value: object) -> str:
    """Set only the Catchuparr logger hierarchy; preserve Dispatcharr handlers."""
    try:
        level = normalize_log_level(value)
    except ValueError:
        level = "INFO"
        logging.getLogger("catchuparr").setLevel(LOG_LEVELS[level])
        event("setting_invalid", logging.WARNING, reason="log_level")
    logging.getLogger("catchuparr").setLevel(LOG_LEVELS[level])
    return level


def event(name: str, level: int = logging.INFO, **state: object) -> None:
    """Write a prefixed event using only fixed names and non-sensitive state."""
    if name not in _EVENTS:
        name = "runtime_disabled"
    safe_fields: list[str] = []
    for key, value in sorted(state.items()):
        if key not in _STATE_KEYS:
            continue
        if type(value) is bool:
            rendered = "true" if value else "false"
        elif type(value) is int and value >= 0:
            rendered = str(value)
        elif key == "level" and isinstance(value, str) and value in LOG_LEVELS:
            rendered = value
        elif key == "reason" and isinstance(value, str) and value in _SAFE_REASONS:
            rendered = value
        else:
            continue
        safe_fields.append(f"{key}={rendered}")
    suffix = " " + " ".join(safe_fields) if safe_fields else ""
    logging.getLogger("catchuparr").log(level, "[Catchuparr] %s%s", name, suffix)


def error(category: str) -> None:
    """Rate-limit a fixed error category without retaining arbitrary messages."""
    if category not in _EVENTS:
        category = "runtime_disabled"
    now = time.monotonic()
    suppressed = 0
    log_initial = False
    with _error_lock:
        window = _error_windows.get(category)
        if window is None:
            if len(_error_windows) >= MAX_ERROR_CATEGORIES:
                oldest = min(_error_windows, key=lambda item: _error_windows[item].started_at)
                del _error_windows[oldest]
            _error_windows[category] = _ErrorWindow(now)
            log_initial = True
        elif now - window.started_at < ERROR_WINDOW_SECONDS:
            window.count += 1
        else:
            suppressed = window.count
            _error_windows[category] = _ErrorWindow(now)
            log_initial = True
    target = logging.getLogger("catchuparr")
    if suppressed:
        target.error(
            "[Catchuparr] error_suppressed category=%s count=%d", category, suppressed
        )
    if log_initial:
        target.error("[Catchuparr] %s", category)
