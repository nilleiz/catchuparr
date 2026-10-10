"""Focused assertions for native Stats integration probe helpers."""

import ast
import unittest
from pathlib import Path
from types import SimpleNamespace

from scripts.aio_stats_probe import (
    _assert_archive_projection,
    _assert_native_route_match,
    stats_options_draft,
)


class AIOStatsProbeTests(unittest.TestCase):
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
