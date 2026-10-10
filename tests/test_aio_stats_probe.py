"""Focused assertions for native Stats integration probe helpers."""

import ast
import sqlite3
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from catchuparr import runtime, stats
from scripts.aio_stats_probe import (
    _assert_archive_projection,
    _assert_native_route_match,
    _playback_auth_diagnostics,
    stats_options_draft,
)


class AIOStatsProbeTests(unittest.TestCase):
    def test_stats_options_releases_runtime_settings_orm_connection(self):
        class FakeConnection:
            def __init__(self, *, opened=True, in_atomic_block=False, autocommit=True):
                self.alias = "default"
                self.connection = object() if opened else None
                self.in_atomic_block = in_atomic_block
                self.autocommit = autocommit
                self.close_calls = 0

            def get_autocommit(self):
                return self.autocommit

            def close(self):
                self.close_calls += 1
                self.connection = None

        class FakeConnections:
            def __init__(self, connection):
                self.connection = connection
                self.all_calls = []

            def all(self, *, initialized_only=False):
                self.all_calls.append(initialized_only)
                if initialized_only:
                    return [self.connection] if self.connection.connection is not None else []
                return [self.connection]

        cases = (
            ("success", FakeConnection(), False, 1),
            ("runtime_error", FakeConnection(), True, 1),
            ("atomic", FakeConnection(in_atomic_block=True), False, 0),
            ("manual", FakeConnection(autocommit=False), False, 0),
            ("unopened", FakeConnection(opened=False), False, 0),
        )
        for name, default, runtime_error, expected_closes in cases:
            with self.subTest(name=name):
                connections = FakeConnections(default)
                django_module = types.ModuleType("django")
                django_module.__path__ = []
                django_db_module = types.ModuleType("django.db")
                django_db_module.connections = connections
                django_module.db = django_db_module
                settings_loader = Mock(
                    side_effect=RuntimeError("synthetic settings query failure")
                    if runtime_error else None,
                    return_value={
                        "show_archive_playback_in_stats": False,
                        "hide_recorders_in_stats": False,
                    },
                )
                with (
                    patch.dict(sys.modules, {
                        "django": django_module,
                        "django.db": django_db_module,
                    }),
                    patch.object(runtime, "load_runtime_settings", settings_loader),
                ):
                    options = stats._options()

                self.assertEqual(default.close_calls, expected_closes)
                self.assertEqual(connections.all_calls, [True])
                self.assertEqual(
                    options,
                    {
                        "show_archive_playback_in_stats": runtime_error,
                        "hide_recorders_in_stats": runtime_error,
                    },
                )

    def test_stats_projection_uses_real_safe_database_release_guards(self):
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "archive.sqlite3"
            with sqlite3.connect(database) as db:
                stats._ensure_schema(db)
                db.execute(
                    "INSERT INTO catchuparr_stats_viewers "
                    "(viewer_key,display_id,user_id,channel_uuid,logical_started_at,last_success_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (
                        "viewer", "ca_synthetic", "7", "synthetic-channel",
                        time.time(), time.time(),
                    ),
                )

            class FakeConnection:
                def __init__(
                    self,
                    alias="default",
                    *,
                    opened=True,
                    in_atomic_block=False,
                    autocommit=True,
                    close_error=False,
                ):
                    self.alias = alias
                    self.connection = object() if opened else None
                    self.in_atomic_block = in_atomic_block
                    self.autocommit = autocommit
                    self.close_error = close_error
                    self.close_calls = 0

                def get_autocommit(self):
                    return self.autocommit

                def close(self):
                    self.close_calls += 1
                    if self.close_error:
                        raise RuntimeError("synthetic close failure")
                    self.connection = None

            class FakeConnections:
                def __init__(self, values):
                    self.values = values
                    self.all_calls = []

                def all(self, *, initialized_only=False):
                    self.all_calls.append(initialized_only)
                    if initialized_only:
                        return [value for value in self.values if value.connection is not None]
                    return self.values

            options = {
                "show_archive_playback_in_stats": True,
                "hide_recorders_in_stats": True,
            }
            cases = (
                ("success", FakeConnection(), False, [{"session_id": "ca_synthetic"}], 1),
                ("query_error", FakeConnection(), True, [], 1),
                ("atomic", FakeConnection(in_atomic_block=True), False,
                 [{"session_id": "ca_synthetic"}], 0),
                ("manual", FakeConnection(autocommit=False), False,
                 [{"session_id": "ca_synthetic"}], 0),
                ("other_alias", FakeConnection(alias="analytics"), False,
                 [{"session_id": "ca_synthetic"}], 0),
                ("unopened", FakeConnection(opened=False), False,
                 [{"session_id": "ca_synthetic"}], 0),
                ("close_error", FakeConnection(close_error=True), False,
                 [{"session_id": "ca_synthetic"}], 1),
            )

            for name, default, query_error, expected_rows, expected_closes in cases:
                with self.subTest(name=name):
                    connections = FakeConnections([default])
                    django_module = types.ModuleType("django")
                    django_module.__path__ = []
                    django_db_module = types.ModuleType("django.db")
                    django_db_module.connections = connections
                    django_module.db = django_db_module

                    def viewer_row(_row):
                        if query_error:
                            raise sqlite3.OperationalError("synthetic metadata query failure")
                        return {"session_id": "ca_synthetic"}

                    with (
                        patch.dict(sys.modules, {
                            "django": django_module,
                            "django.db": django_db_module,
                        }),
                        patch.object(stats, "_options", return_value=options),
                        patch.object(stats, "_db_path", return_value=database),
                        patch.object(stats, "_viewer_row", side_effect=viewer_row),
                    ):
                        rows = stats._active_viewers()

                    self.assertEqual(rows, expected_rows)
                    self.assertEqual(default.close_calls, expected_closes)
                    self.assertEqual(connections.all_calls, [True])

    def test_auth_diagnostic_is_boolean_only_and_never_contains_token(self):
        token = "synthetic-secret-token"

        class FakeUser:
            id = 7
            objects = SimpleNamespace(
                filter=Mock(return_value=SimpleNamespace(exists=Mock(return_value=True))),
            )

        user = FakeUser()
        runtime = SimpleNamespace(
            require_supported_version=Mock(return_value=None),
            load_config=Mock(return_value=object()),
        )
        token_store = SimpleNamespace(lookup=Mock(return_value="7"))

        flags = _playback_auth_diagnostics(
            request=object(),
            user=user,
            token=token,
            root=Path("/synthetic/archive"),
            network_checker=Mock(return_value=True),
            runtime=runtime,
            access_token_store=Mock(return_value=token_store),
        )

        self.assertEqual(set(flags), {
            "supportedVersion", "activeConfig", "tokenPresent", "tokenLookupFound",
            "userActive", "playlistNetworkAllowed", "streamsNetworkAllowed",
        })
        self.assertTrue(all(type(value) is bool for value in flags.values()))
        self.assertNotIn(token, repr(flags))

    def test_hls_admission_probe_precedes_one_slot_live_xc_fixtures(self):
        source = Path(__file__).resolve().parents[1] / "scripts/aio_integration_probe.py"
        tree = ast.parse(source.read_text(encoding="utf-8"))
        probe = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "probe"
        )
        stats_call = next(
            node.lineno for node in probe.body
            if isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
            and node.value.func.id == "probe_actual_stats"
        )
        xc_calls = [
            node.lineno for node in ast.walk(probe)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "xc_playback"
        ]

        self.assertTrue(xc_calls)
        self.assertLess(stats_call, min(xc_calls))

    def test_native_route_identity_rejects_frontend_fallback(self):
        native = SimpleNamespace(namespace="proxy:catchup", url_name="catchup_stats")
        frontend = SimpleNamespace(namespace="", url_name="index")

        _assert_native_route_match(native, "proxy:catchup", "catchup_stats")
        with self.assertRaisesRegex(RuntimeError, "outside native namespace"):
            _assert_native_route_match(frontend, "proxy:catchup", "catchup_stats")

    def test_archive_projection_adds_one_opaque_session_and_one_connection(self):
        native = {
            "timeshift_sessions": [{"session_id": "native-session"}],
            "total_connections": 4,
        }
        projected = {
            "timeshift_sessions": [
                {"session_id": "native-session"},
                {"session_id": "ca_synthetic-display"},
            ],
            "total_connections": 5,
        }

        _assert_archive_projection(
            native,
            projected,
            ["ca_synthetic-display"],
            "ca_synthetic-display",
        )

    def test_archive_projection_rejects_duplicate_or_inflated_totals(self):
        native = {"timeshift_sessions": [], "total_connections": 0}
        duplicated = {
            "timeshift_sessions": [
                {"session_id": "ca_synthetic-display"},
                {"session_id": "ca_synthetic-display"},
            ],
            "total_connections": 2,
        }

        with self.assertRaisesRegex(RuntimeError, "missing or duplicated"):
            _assert_archive_projection(
                native, duplicated, ["ca_synthetic-display"], "ca_synthetic-display",
            )

        inflated = {
            "timeshift_sessions": [{"session_id": "ca_synthetic-display"}],
            "total_connections": 2,
        }
        with self.assertRaisesRegex(RuntimeError, "not truthful"):
            _assert_archive_projection(
                native, inflated, ["ca_synthetic-display"], "ca_synthetic-display",
            )

    def test_stats_option_draft_preserves_full_recorder_fixture(self):
        fixture = {
            "filter_config": "version: 1\nprofile: Synthetic\nrules: []\n",
            "archive_root": "/synthetic/archive",
            "retention_hours": 1,
            "max_storage_gib": 1,
            "show_archive_playback_in_stats": True,
            "hide_recorders_in_stats": True,
        }

        draft = stats_options_draft(
            fixture, show_archive=False, hide_recorders=False,
        )

        self.assertEqual(draft["filter_config"], fixture["filter_config"])
        self.assertEqual(draft["archive_root"], fixture["archive_root"])
        self.assertEqual(draft["retention_hours"], fixture["retention_hours"])
        self.assertEqual(draft["max_storage_gib"], fixture["max_storage_gib"])
        self.assertFalse(draft["show_archive_playback_in_stats"])
        self.assertFalse(draft["hide_recorders_in_stats"])
        self.assertTrue(fixture["show_archive_playback_in_stats"])
        self.assertTrue(fixture["hide_recorders_in_stats"])


if __name__ == "__main__":
    unittest.main()
