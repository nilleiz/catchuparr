"""Dispatcharr plugin entry point.

Dispatcharr imports this module only after an administrator enables the plugin.
Imports that need Django, Celery, or Dispatcharr are deliberately deferred.
"""

from __future__ import annotations


class Plugin:
    name = "Catchuparr"
    version = "0.7.4-rc.1"
    description = "Local rolling catch-up archive, start-over and optional live connection sharing"
    author = "nilleiz"
    help_url = "https://github.com/nilleiz/catchuparr"

    def __init__(self):
        from .runtime import bootstrap

        bootstrap()

    def run(self, action: str, params: dict, context: dict):
        if action in {"current_recordings", "today_schedule"}:
            from .configuration import load_applied_state_readonly

            settings, control = load_applied_state_readonly()
            if settings is None:
                return {
                    "status": "unavailable",
                    "message": "Applied Catchuparr configuration is unavailable.",
                    "channels": [],
                    "summary": {
                        "channel_count": 0,
                        "window_count": 0,
                        "recording_paused": None,
                        "pause_state": "unknown",
                    },
                    "recording_paused": None,
                }
            if action == "current_recordings":
                from .recording_overview import current_recordings

                return current_recordings(settings, control_state=control)
            from .recording_schedule_overview import channel_metadata, today_schedule

            ids = settings.get("channel_uuids", "").splitlines()
            return today_schedule(settings, channel_metadata(ids), control_state=control)
        from .logging_utils import apply_log_level
        from .runtime import (
            apply_committed_log_level,
            apply_configuration,
            create_access_token,
            load_runtime_state,
            pause_recorders,
            reconcile,
            resume_recorders,
            status,
            validate_configuration,
        )

        fallback = context.get("settings") or {}
        if action in {"validate_configuration", "apply_configuration"}:
            from .runtime import load_plugin_settings

            draft_settings = load_plugin_settings(fallback)
            settings = draft_settings
            control = None
            apply_committed_log_level()
        else:
            settings, control = load_runtime_state()
            apply_log_level(settings.get("log_level", "INFO"))
        if action == "status":
            return status(settings, control_state=control)
        if action == "reconcile":
            reconcile()
            return {"status": "queued"}
        if action == "create_access_token":
            return create_access_token(settings)
        if action == "validate_configuration":
            return validate_configuration(draft_settings)
        if action == "apply_configuration":
            return apply_configuration()
        if action == "pause_recorders":
            return pause_recorders()
        if action == "resume_recorders":
            return resume_recorders()
        raise ValueError(f"Unknown Catchuparr action: {action}")

    def stop(self, context: dict):
        from .runtime import shutdown

        shutdown()
