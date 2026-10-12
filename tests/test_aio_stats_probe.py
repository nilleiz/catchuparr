"""Focused assertions for native Stats integration probe helpers."""

import ast
import json
import sqlite3
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from catchuparr import configuration, stats
from scripts.aio_stats_probe import (
    _assert_archive_projection,
    _assert_native_route_match,
    _NativeBasicProjectionCapture,
    _playback_auth_diagnostics,
    _private_worker_capability_binding,
    stats_options_draft,
)


class AIOStatsProbeTests(unittest.TestCase):
    def test_private_worker_binding_ignores_ledger_only_fields(self):
        from catchuparr import recorder_proxy

        channel_id = "00000000-0000-0000-0000-000000000041"
        lease_key = f"catchuparr:recorder:{channel_id}"
        active = {
            "channel_uuids": channel_id,
            "source_policies": {},
            "hide_recorders_in_stats": True,
        }
        digest = "synthetic-capability-digest"
        binding = {
            "account_id": "12",
            "channel_uuid": channel_id,
            "config_generation": recorder_proxy.configuration_generation(active),
            "control_generation": "8",
            "lease_fence": "7",
            "lease_key": lease_key,
            "lease_value": "7:synthetic-owner",
            "stream_id": "44",
            "worker_id": "managed-synthetic-worker",
            "capability_digest": digest,
        }
        capability = json.dumps(binding)

        class CapabilityRedis:
            values = {
                recorder_proxy.CAPABILITY_PREFIX + digest: capability,
                lease_key: "7:synthetic-owner",
            }

            def get(self, key):
                return self.values.get(key)

        worker_record = {
            **binding,
            "state": "active",
            "reservation_state": "reserved",
            "reservation_id": "synthetic-reservation",
            "profile_id": "13",
            "created_at": "123.0",
        }
        redis = CapabilityRedis()
        with patch(
            "catchuparr.configuration.load_applied_state",
            return_value=(active, SimpleNamespace(paused=False, generation=8)),
        ), patch.object(recorder_proxy, "candidate_is_current", return_value=True):
            extracted = _private_worker_capability_binding(redis, worker_record)
            self.assertTrue(recorder_proxy.capability_binding_current(redis, extracted))

        self.assertEqual(extracted, binding)
        self.assertIsNone(
            _private_worker_capability_binding(
                redis, {**worker_record, "stream_id": "45"},
            )
        )

    def test_native_projection_capture_ignores_background_same_channel_calls(self):
        delegated = []

        def real_projection(module, channel_id, native_info):
            delegated.append((module, channel_id, native_info))
            return {
                **native_info,
                "clients": list(native_info.get("clients", [])),
            }

        capture = _NativeBasicProjectionCapture(real_projection, "channel-a")
        request_native = {"clients": [{"client_id": "request-client"}]}
        background_native = {"clients": [{"client_id": "background-client"}]}

        request_result = capture("native", "channel-a", request_native)
        background = threading.Thread(
            target=capture,
            args=("native", "channel-a", background_native),
        )
        background.start()
        background.join(timeout=5)

        self.assertFalse(background.is_alive())
        self.assertEqual(len(delegated), 2)
        self.assertEqual(delegated[0], ("native", "channel-a", request_native))
        self.assertEqual(delegated[1], ("native", "channel-a", background_native))
        self.assertEqual(
            capture.samples,
            [({"request-client"}, {"request-client"})],
        )
        self.assertEqual(request_result["clients"], request_native["clients"])

    def test_stats_options_reads_committed_config_without_database_cleanup(self):
        with patch.object(
            configuration,
            "load_stats_configuration",
            return_value={
                "show_archive_playback_in_stats": False,
                "hide_recorders_in_stats": False,
            },
        ), patch.object(stats, "_release_orm_connection") as release_connection:
            options = stats._options()

        self.assertEqual(
            options,
            {
                "show_archive_playback_in_stats": False,
                "hide_recorders_in_stats": False,
            },
        )
        release_connection.assert_not_called()

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

    def test_stats_probe_releases_replacement_ts_lease_before_native_admission(self):
        source = Path(__file__).resolve().parents[1] / "scripts/aio_stats_probe.py"
        tree = ast.parse(source.read_text(encoding="utf-8"))
        probe = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "probe_actual_stats"
        )
        replacement_line = next(
            node.lineno for node in ast.walk(probe)
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "replacement_body"
                    for target in node.targets)
        )
        stop_line = next(
            node.lineno for node in ast.walk(probe)
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "replacement_stop"
                    for target in node.targets)
        )
        admission_count_lines = [
            node.lineno for node in ast.walk(probe)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "active_ts_session_count"
        ]
        stop_call = next(
            node for node in ast.walk(probe)
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "replacement_stop"
                    for target in node.targets)
        ).value

        self.assertLess(replacement_line, stop_line)
        self.assertTrue(any(line > stop_line for line in admission_count_lines))
        self.assertIsInstance(stop_call, ast.Call)
        self.assertIsInstance(stop_call.func, ast.Attribute)
        self.assertEqual(stop_call.func.attr, "post")
        self.assertEqual(ast.unparse(stop_call.args[0]), "native_paths['stop']")
        probe_source = ast.get_source_segment(source.read_text(encoding="utf-8"), probe)
        self.assertIn("SELECT active FROM ts_playback_sessions WHERE lease_id=?", probe_source)
        self.assertIn("Native Stop did not revoke the replacement Stats row", probe_source)

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
