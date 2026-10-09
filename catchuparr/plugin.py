"""Dispatcharr plugin entry point.

Dispatcharr imports this module only after an administrator enables the plugin.
Imports that need Django, Celery, or Dispatcharr are deliberately deferred.
"""

from __future__ import annotations


class Plugin:
    name = "Catchuparr"
    version = "0.3.0"
    description = "Local rolling catch-up archive and start-over"
    author = "nilleiz"
    help_url = "https://github.com/nilleiz/catchuparr"

    def __init__(self):
        from .runtime import bootstrap

        bootstrap()

    def run(self, action: str, params: dict, context: dict):
        from .logging_utils import apply_log_level
        from .runtime import (
            apply_configuration,
            apply_recorder_control,
            create_access_token,
            load_plugin_settings,
            load_runtime_settings,
            pause_recorders,
            reconcile,
            resume_recorders,
            status,
            validate_configuration,
        )

        fallback = context.get("settings") or {}
        draft_settings = load_plugin_settings(fallback)
        apply_log_level(draft_settings.get("log_level", "INFO"))
        if action in {"validate_configuration", "apply_configuration"}:
            settings = draft_settings
        else:
            settings = load_runtime_settings(fallback)
        if action == "status":
            return status(settings)
        if action == "reconcile":
            reconcile()
            return {"status": "queued"}
        if action == "create_access_token":
            return create_access_token(settings)
        if action == "validate_configuration":
            return validate_configuration(draft_settings)
        if action == "apply_configuration":
            return apply_configuration()
        if action == "apply_recorder_control":
            return apply_recorder_control()
        if action == "pause_recorders":
            return pause_recorders()
        if action == "resume_recorders":
            return resume_recorders()
        raise ValueError(f"Unknown Catchuparr action: {action}")

    def stop(self, context: dict):
        from .runtime import shutdown

        shutdown()
