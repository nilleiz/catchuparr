"""Dispatcharr plugin entry point.

Dispatcharr imports this module only after an administrator enables the plugin.
Imports that need Django, Celery, or Dispatcharr are deliberately deferred.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class Plugin:
    name = "Catchuparr"
    version = "0.1.1"
    description = "Local rolling catch-up archive and start-over"
    author = "nilleiz"
    help_url = "https://github.com/nilleiz/catchuparr"

    def __init__(self):
        from .runtime import bootstrap

        bootstrap()

    def run(self, action: str, params: dict, context: dict):
        from .runtime import create_access_token, reconcile, status

        settings = context.get("settings") or {}
        if action == "status":
            return status(settings)
        if action == "reconcile":
            reconcile()
            return {"status": "queued"}
        if action == "create_access_token":
            return create_access_token(settings)
        raise ValueError(f"Unknown Catchuparr action: {action}")

    def stop(self, context: dict):
        from .runtime import shutdown

        shutdown()
