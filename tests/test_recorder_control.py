import copy
import json
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

    def test_failed_resume_replace_republishes_a_newer_paused_generation(self):
        database = _FakeDatabase({"recording_enabled": False})
        self._write_state(RecorderControlState(paused=True, generation=4))
        atomic_replace = recorder_control._atomic_replace_sidecar

        def fail_after_publishing_resume(path, state):
            atomic_replace(path, state)
            if not state.paused:
                raise OSError("synthetic directory fsync failure")

        with (
            database.patch(),
            patch.object(recorder_control, "_atomic_replace_sidecar", fail_after_publishing_resume),
            self.assertRaises(RecorderControlError),
        ):
            resume_recorders(self.active_path)

        self.assertEqual(
            {"version": 1, "paused": True, "generation": 6}, self._read_json_state()
        )
        self.assertTrue(database.row.settings["recording_enabled"])

    def test_failed_resume_replace_uses_fail_closed_in_place_fallback(self):
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
            {"version": 1, "paused": True, "generation": 6}, self._read_json_state()
        )

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
