"""Dispatcharr plugin entry point.

Dispatcharr imports this module only after an administrator enables the plugin.
Imports that need Django, Celery, or Dispatcharr are deliberately deferred.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class Plugin:
    name = "Catchuparr"
    version = "0.2.0"
    description = "Local rolling catch-up archive and start-over"
    author = "nilleiz"
    help_url = "https://github.com/nilleiz/catchuparr"

    def __init__(self):
        from .runtime import bootstrap

        bootstrap()

    def run(self, action: str, params: dict, context: dict):
        from .runtime import (
            apply_configuration,
            create_access_token,
            load_plugin_settings,
            load_runtime_settings,
            reconcile,
            status,
            validate_configuration,
        )

        fallback = context.get("settings") or {}
        draft_settings = load_plugin_settings(fallback)
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
            return apply_configuration(draft_settings)
        raise ValueError(f"Unknown Catchuparr action: {action}")

    def stop(self, context: dict):
        from .runtime import shutdown

        shutdown()
