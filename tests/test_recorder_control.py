import copy
import json
import subprocess
import sys
import tempfile
import types
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from catchuparr import recorder_control
from catchuparr.recorder_control import (
    RecorderControlError,
    RecorderControlState,
    apply_recorder_control,
    control_deny_path,
    control_state_path,
    load_recorder_control,
    pause_recorders,
    resume_recorders,
)


class _FakeDatabase:
    def __init__(self, settings):
        self.events = []
        self.row = types.SimpleNamespace(settings=copy.deepcopy(settings))
        self.fail_save = False

        database = self

        class Query:
            def select_for_update(self):
                database.events.append("select_for_update")
                return self

            def filter(self, *, key):
                database.events.append(("filter", key))
                return self

            def first(self):
                database.events.append("first")
                return database.row

        class Manager:
            def filter(self, **kwargs):
                query = Query()
                return query.filter(**kwargs)

            def select_for_update(self):
                return Query().select_for_update()

        class PluginConfig:
            objects = Manager()

        database.row.save = database.save
        self.plugin_config = PluginConfig

        @contextmanager
        def atomic():
            database.events.append("transaction_begin")
            previous = copy.deepcopy(database.row.settings)
            try:
                yield
            except Exception:
                database.row.settings = previous
                database.events.append("transaction_rollback")
                raise
            else:
                database.events.append("transaction_commit")

        self.transaction = types.SimpleNamespace(atomic=atomic)

    def save(self, update_fields):
        self.events.append(("save", tuple(update_fields)))
        if self.fail_save:
            raise RuntimeError("synthetic database failure")

    def patch(self):
        return patch(
            "catchuparr.recorder_control._plugin_config_api",
            return_value=(self.plugin_config, self.transaction),
        )


class RecorderControlTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.active_path = self.root / ".catchuparr-active-settings.json"
        self.sidecar = control_state_path(self.active_path)

    def tearDown(self):
        self.directory.cleanup()

    def _write_state(self, state):
        self.sidecar.write_text(json.dumps(state.to_json()), encoding="utf-8")

    def _read_json_state(self):
        return json.loads(self.sidecar.read_text(encoding="utf-8"))

    def _assert_cross_process_denied(self, active_path):
        script = (
            "import sys; from pathlib import Path; "
            "from catchuparr.recorder_control import RecorderControlError, load_recorder_control; "
            "\ntry: load_recorder_control(Path(sys.argv[1]))\n"
            "except RecorderControlError: print('denied')\n"
            "else: print('admitted'); sys.exit(1)"
        )
        result = subprocess.run(
            [sys.executable, "-c", script, str(active_path)],
            cwd=Path(__file__).resolve().parents[1],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("denied", result.stdout.strip())

    def _instrument_lock(self, events):
        @contextmanager
        def lock(path, *, exclusive):
            events.append(("lock_enter", Path(path), exclusive))
            try:
                yield
            finally:
                events.append(("lock_exit", Path(path), exclusive))

        return lock

    def test_missing_sidecar_initializes_enabled_default_without_reading_draft(self):
        with patch(
            "catchuparr.recorder_control._plugin_config_api",
            side_effect=AssertionError("draft settings must not be read during initialization"),
        ):
            state = load_recorder_control(self.active_path)

        self.assertEqual(RecorderControlState(paused=False, generation=0), state)
        self.assertEqual(
            {"version": 1, "paused": False, "generation": 0}, self._read_json_state()
        )

    def test_existing_sidecar_is_authoritative_over_newer_draft(self):
        self._write_state(RecorderControlState(paused=False, generation=8))
        database = _FakeDatabase({"recording_enabled": False})

        with database.patch():
            loaded = load_recorder_control(self.active_path)

        self.assertEqual(RecorderControlState(paused=False, generation=8), loaded)
        self.assertEqual([], database.events)
        self.assertEqual(8, self._read_json_state()["generation"])

    def test_corrupt_or_unknown_sidecar_denies_control_without_replacement(self):
        invalid_values = (
            "{broken",
            '{"version":1,"paused":false,"generation":0,"extra":true}',
            '{"version":1,"paused":0,"generation":0}',
            '{"version":1,"paused":false,"generation":-1}',
            '{"version":2,"paused":false,"generation":0}',
        )
        for value in invalid_values:
            with self.subTest(value=value):
                self.sidecar.write_text(value, encoding="utf-8")
                with self.assertRaises(RecorderControlError):
                    load_recorder_control(self.active_path)
                self.assertEqual(value, self.sidecar.read_text(encoding="utf-8"))

    def test_apply_consumes_only_persisted_toggle_even_if_filter_yaml_is_invalid(self):
        database = _FakeDatabase({
            "recording_enabled": False,
            "filter_config": "not: [valid",
            "archive_root": "/synthetic/archive",
        })
        events = database.events
        lock = self._instrument_lock(events)

        with database.patch(), patch("catchuparr.configuration._config_lock", lock):
            state = apply_recorder_control(self.active_path)

        self.assertEqual(RecorderControlState(paused=True, generation=1), state)
        self.assertEqual(False, database.row.settings["recording_enabled"])
        self.assertEqual("not: [valid", database.row.settings["filter_config"])
        self.assertEqual(("lock_enter", self.active_path, True), events[0])
        self.assertLess(events.index("transaction_begin"), events.index("select_for_update"))
        self.assertEqual(("lock_exit", self.active_path, True), events[-1])

    def test_pause_resume_are_idempotent_and_only_transitions_advance_generation(self):
        database = _FakeDatabase({"recording_enabled": True})
        with database.patch():
            first_pause = pause_recorders(self.active_path)
            repeated_pause = pause_recorders(self.active_path)
            first_resume = resume_recorders(self.active_path)
            repeated_resume = resume_recorders(self.active_path)

        self.assertEqual(RecorderControlState(True, 1), first_pause)
        self.assertEqual(first_pause, repeated_pause)
        self.assertEqual(RecorderControlState(False, 2), first_resume)
        self.assertEqual(first_resume, repeated_resume)
        self.assertTrue(database.row.settings["recording_enabled"])

    def test_database_failure_after_pause_keeps_durable_pause_and_rolls_back_setting(self):
        database = _FakeDatabase({"recording_enabled": True})
        database.fail_save = True

        with database.patch(), self.assertRaises(RecorderControlError):
            pause_recorders(self.active_path)

        self.assertTrue(self._read_json_state()["paused"])
        self.assertTrue(database.row.settings["recording_enabled"])

    def test_database_failure_during_resume_keeps_existing_pause(self):
        database = _FakeDatabase({"recording_enabled": False})
        database.fail_save = True
        self._write_state(RecorderControlState(paused=True, generation=4))

        with database.patch(), self.assertRaises(RecorderControlError):
            resume_recorders(self.active_path)

        self.assertEqual(
            {"version": 1, "paused": True, "generation": 4}, self._read_json_state()
        )
        self.assertFalse(database.row.settings["recording_enabled"])

    def test_failed_resume_replace_stays_denied_when_rollback_writes_fail(self):
        database = _FakeDatabase({"recording_enabled": False})
        self._write_state(RecorderControlState(paused=True, generation=4))
        atomic_replace = recorder_control._atomic_replace_sidecar

        def fail_publishing_resume_and_rollback(path, state):
            if state.paused:
                raise OSError("synthetic persistent rollback failure")
            atomic_replace(path, state)
            raise OSError("synthetic directory fsync failure")

        with (
            database.patch(),
            patch.object(
                recorder_control, "_atomic_replace_sidecar", fail_publishing_resume_and_rollback
            ),
            patch.object(
                recorder_control,
                "_write_paused_in_place",
                side_effect=OSError("synthetic persistent in-place failure"),
            ) as fallback_write,
            self.assertRaises(RecorderControlError),
        ):
            resume_recorders(self.active_path)
        fallback_write.assert_not_called()

        self.assertEqual(
            {"version": 1, "paused": False, "generation": 5}, self._read_json_state()
        )
        self.assertTrue(control_deny_path(self.active_path).exists())
        with self.assertRaises(RecorderControlError):
            load_recorder_control(self.active_path)
        self._assert_cross_process_denied(self.active_path)
        self.assertTrue(database.row.settings["recording_enabled"])

    def test_failed_resume_replace_stays_denied_when_atomic_replace_fails(self):
        database = _FakeDatabase({"recording_enabled": False})
        self._write_state(RecorderControlState(paused=True, generation=4))

        with (
            database.patch(),
            patch.object(
                recorder_control,
                "_atomic_replace_sidecar",
                side_effect=OSError("synthetic rename failure"),
            ),
            self.assertRaises(RecorderControlError),
        ):
            resume_recorders(self.active_path)

        self.assertEqual(
            {"version": 1, "paused": True, "generation": 4}, self._read_json_state()
        )
        self.assertTrue(control_deny_path(self.active_path).exists())
        with self.assertRaises(RecorderControlError):
            load_recorder_control(self.active_path)

    def test_initialization_publish_failure_during_pause_or_apply_keeps_deny(self):
        for action_name in ("pause", "apply"):
            with self.subTest(action=action_name), tempfile.TemporaryDirectory() as directory:
                active_path = Path(directory) / ".catchuparr-active-settings.json"
                sidecar = control_state_path(active_path)
                marker = control_deny_path(active_path)
                real_atomic_replace = recorder_control._atomic_replace_sidecar

                def fail_after_publishing_initial_state(path, state):
                    real_atomic_replace(path, state)
                    if state.generation == 0:
                        raise OSError("synthetic directory fsync failure")

                database = _FakeDatabase({"recording_enabled": False})
                action = pause_recorders if action_name == "pause" else apply_recorder_control
                with (
                    database.patch(),
                    patch.object(
                        recorder_control,
                        "_atomic_replace_sidecar",
                        fail_after_publishing_initial_state,
                    ),
                    self.assertRaises(RecorderControlError),
                ):
                    action(active_path)

                self.assertTrue(sidecar.exists())
                self.assertTrue(marker.exists())
                with self.assertRaises(RecorderControlError):
                    load_recorder_control(active_path)
                self._assert_cross_process_denied(active_path)

    def test_pause_action_recovers_a_pending_failed_resume(self):
        self._write_state(RecorderControlState(paused=True, generation=4))
        database = _FakeDatabase({"recording_enabled": False})
        real_atomic_replace = recorder_control._atomic_replace_sidecar

        def fail_after_publishing_resume(path, state):
            real_atomic_replace(path, state)
            if not state.paused:
                raise OSError("synthetic directory fsync failure")

        with (
            database.patch(),
            patch.object(recorder_control, "_atomic_replace_sidecar", fail_after_publishing_resume),
            self.assertRaises(RecorderControlError),
        ):
            resume_recorders(self.active_path)

        with database.patch():
            paused = pause_recorders(self.active_path)

        self.assertEqual(RecorderControlState(paused=True, generation=6), paused)
        self.assertFalse(control_deny_path(self.active_path).exists())
        self.assertEqual(paused, load_recorder_control(self.active_path))

    def test_marker_is_durable_before_resume_sidecar_publication(self):
        database = _FakeDatabase({"recording_enabled": False})
        self._write_state(RecorderControlState(paused=True, generation=4))
        events = database.events
        write_marker = recorder_control._write_deny_marker
        replace_state = recorder_control._atomic_replace_sidecar
        clear_marker = recorder_control._clear_deny_marker

        def record_marker(sidecar):
            result = write_marker(sidecar)
            events.append("deny_marker_durable")
            return result

        def record_state(sidecar, state):
            result = replace_state(sidecar, state)
            if not state.paused:
                events.append("unpaused_state_durable")
            return result

        def record_clear(sidecar):
            events.append("clear_deny_marker")
            return clear_marker(sidecar)

        with (
            database.patch(),
            patch.object(recorder_control, "_write_deny_marker", record_marker),
            patch.object(recorder_control, "_atomic_replace_sidecar", record_state),
            patch.object(recorder_control, "_clear_deny_marker", record_clear),
        ):
            resume_recorders(self.active_path)

        self.assertLess(events.index("transaction_commit"), events.index("deny_marker_durable"))
        self.assertLess(events.index("deny_marker_durable"), events.index("unpaused_state_durable"))
        self.assertLess(events.index("unpaused_state_durable"), events.index("clear_deny_marker"))

    def test_deny_marker_directory_fsync_failure_prevents_state_publication(self):
        database = _FakeDatabase({"recording_enabled": False})
        self._write_state(RecorderControlState(paused=True, generation=2))
        atomic_replace = recorder_control._atomic_replace_sidecar

        with (
            database.patch(),
            patch.object(
                recorder_control,
                "_fsync_directory",
                side_effect=OSError("synthetic deny directory fsync failure"),
            ),
            patch.object(recorder_control, "_atomic_replace_sidecar", wraps=atomic_replace) as replace,
            self.assertRaises(RecorderControlError),
        ):
            resume_recorders(self.active_path)

        replace.assert_not_called()
        self.assertEqual(
            {"version": 1, "paused": True, "generation": 2}, self._read_json_state()
        )
        self.assertTrue(control_deny_path(self.active_path).exists())
        with self.assertRaises(RecorderControlError):
            load_recorder_control(self.active_path)
        self._assert_cross_process_denied(self.active_path)

    def test_marker_clear_failure_after_resume_keeps_all_readers_denied(self):
        database = _FakeDatabase({"recording_enabled": False})
        self._write_state(RecorderControlState(paused=True, generation=2))

        with (
            database.patch(),
            patch.object(
                recorder_control,
                "_clear_deny_marker",
                side_effect=RecorderControlError("synthetic marker unlink failure"),
            ),
            self.assertRaises(RecorderControlError),
        ):
            resume_recorders(self.active_path)

        self.assertTrue(control_deny_path(self.active_path).exists())
        with self.assertRaises(RecorderControlError):
            load_recorder_control(self.active_path)
        self._assert_cross_process_denied(self.active_path)

    def test_setting_read_failure_best_effort_pauses_existing_enabled_state(self):
        self._write_state(RecorderControlState(paused=False, generation=3))
        with (
            patch(
                "catchuparr.recorder_control._plugin_config_api",
                side_effect=RecorderControlError("synthetic unavailable database"),
            ),
            self.assertRaises(RecorderControlError),
        ):
            apply_recorder_control(self.active_path)

        self.assertEqual(
            {"version": 1, "paused": True, "generation": 4}, self._read_json_state()
        )

    def test_sidecar_replace_fsyncs_content_before_replace_and_directory_afterward(self):
        calls = []
        real_fsync = recorder_control.os.fsync
        real_replace = recorder_control.os.replace

        def record_fsync(descriptor):
            calls.append("fsync")
            return real_fsync(descriptor)

        def record_replace(source, destination):
            calls.append("replace")
            return real_replace(source, destination)

        with (
            patch.object(recorder_control.os, "fsync", record_fsync),
            patch.object(recorder_control.os, "replace", record_replace),
        ):
            recorder_control._atomic_replace_sidecar(
                self.sidecar, RecorderControlState(paused=True, generation=1)
            )

        self.assertEqual(["fsync", "replace", "fsync"], calls)
        self.assertTrue(self.sidecar.exists())


if __name__ == "__main__":
    unittest.main()
