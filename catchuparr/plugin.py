"""Dispatcharr plugin entry point.

Dispatcharr imports this module only after an administrator enables the plugin.
Imports that need Django, Celery, or Dispatcharr are deliberately deferred.
"""

from __future__ import annotations


class Plugin:
    name = "Catchuparr"
    version = "0.4.0"
    description = "Local rolling catch-up archive and start-over"
    author = "nilleiz"
    help_url = "https://github.com/nilleiz/catchuparr"

    def __init__(self):
        from .runtime import bootstrap

        bootstrap()

    def run(self, action: str, params: dict, context: dict):
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
