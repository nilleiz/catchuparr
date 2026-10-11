"""Bounded AIO gate for Stats reads during an Apply-like ORM yield."""
from __future__ import annotations

import contextlib
import os
import subprocess
import sys
import tempfile
import types
from pathlib import Path


def _child() -> None:
    import django
    django.setup()

    import gevent
    from django.db import transaction
    from gevent.event import Event

    from catchuparr import configuration, stats
    from catchuparr.configuration import SourceCatalog, apply_configuration

    entered = Event()
    release = Event()
    holder_calls = []

    class Query:
        def select_for_update(self):
            return self

        def filter(self, **kwargs):
            return self

        def first(self):
            holder_calls.append(True)
            if len(holder_calls) == 1:
                entered.set()
                release.wait()
            return types.SimpleNamespace(settings={})

    class Objects:
        def __init__(self):
            self.query = Query()

        def select_for_update(self):
            return self.query

    from apps.plugins.models import PluginConfig

    original_objects = PluginConfig.objects
    temp_dir = tempfile.TemporaryDirectory(prefix="catchuparr-stats-gevent-")
    root = Path(temp_dir.name)
    active_path = root / ".catchuparr-active-settings.json"
    settings = {
        "filter_config": "",
        "archive_root": str(root / "archive"),
        "retention_hours": 24,
        "max_storage_gib": 20,
        "show_archive_playback_in_stats": False,
        "hide_recorders_in_stats": False,
    }
    apply_configuration(
        settings,
        SourceCatalog(channels=(), accounts=(), streams_by_channel={}),
        active_path,
    )
    PluginConfig.objects = Objects()

    @contextlib.contextmanager
    def no_database_atomic():
        yield

    original_atomic = transaction.atomic
    transaction.atomic = no_database_atomic
    try:
        with _patch_active_path(configuration, active_path):
            holder = gevent.spawn(configuration.reset_legacy_configuration, active_path)
            if not entered.wait(timeout=1):
                raise RuntimeError("Synthetic reset did not reach its ORM yield")

            contender = gevent.spawn(stats._options)
            contender.join(timeout=0.5)
            if not contender.ready():
                raise RuntimeError("Stats options blocked behind the Apply lock")
            if contender.value != {
                "show_archive_playback_in_stats": True,
                "hide_recorders_in_stats": True,
            }:
                raise RuntimeError("Contended Stats read did not use display defaults")
            if holder.ready():
                raise RuntimeError("Synthetic reset released before the lock contention check")

            gevent.spawn_later(0.05, release.set)
            holder.join(timeout=1)
            if not holder.ready() or holder.exception is not None:
                raise RuntimeError("Synthetic reset did not finish after lock release")
            if holder.value is not False:
                raise RuntimeError("Synthetic reset unexpectedly changed committed state")
            if stats._options() != {
                "show_archive_playback_in_stats": False,
                "hide_recorders_in_stats": False,
            }:
                raise RuntimeError("Stats did not read the committed settings after release")
            if len(holder_calls) != 1:
                raise RuntimeError("Stats contention path accessed PluginConfig ORM")
    finally:
        release.set()
        transaction.atomic = original_atomic
        PluginConfig.objects = original_objects
        temp_dir.cleanup()


def _patch_active_path(configuration, active_path):
    from unittest.mock import patch

    return patch.object(configuration, "active_settings_path", return_value=active_path)


def run_actual_gevent_probe() -> None:
    """Run the blocking-sensitive test in a bounded isolated Python process."""
    from django.conf import settings

    child_env = os.environ.copy()
    child_env.setdefault("DJANGO_SETTINGS_MODULE", settings.SETTINGS_MODULE)
    child_env["PYTHONPATH"] = os.pathsep.join(sys.path)
    child = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "--child"],
        capture_output=True,
        text=True,
        timeout=8,
        check=False,
        env=child_env,
    )
    if child.returncode != 0:
        detail = "gevent child failed"
        if child.stderr:
            detail += ": " + child.stderr[-1000:]
        raise RuntimeError(detail)


if __name__ == "__main__":
    if sys.argv[1:] != ["--child"]:
        raise SystemExit("This script is invoked by the AIO integration probe")
    _child()
