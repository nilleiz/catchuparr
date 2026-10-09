import json
import sys
import tempfile
import threading
import types
import unittest
from contextlib import contextmanager, nullcontext
from pathlib import Path
from unittest.mock import patch

from catchuparr import configuration
from catchuparr.configuration import (
    ACTIVE_CONFIG_VERSION,
    RESET_REQUIRED_NAME,
    SourceCatalog,
    apply_configuration,
    configuration_reset_required,
    load_active_configuration,
    load_applied_state,
    load_draft_settings,
    reset_legacy_configuration,
    source_catalog,
    validate_configuration,
)
from catchuparr.runtime import load_runtime_settings

CHANNEL_A = "00000000-0000-0000-0000-000000000001"
CHANNEL_B = "00000000-0000-0000-0000-000000000002"
CHANNEL_C = "00000000-0000-0000-0000-000000000003"


class ConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.active_path = self.root / "outside-plugin" / ".catchuparr-active-settings.json"
        self.settings = {
            "filter_config": (
                "version: 1\n"
                "profile: Synthetic Profile All\n"
                "rules:\n"
                "  - channels: {profile: all}\n"
            ),
            "archive_root": str(self.root / "archive"),
            "retention_hours": 24,
            "max_storage_gib": 20,
        }
        self.catalog = SourceCatalog(
            channels=(
                {"uuid": CHANNEL_A, "number": "7", "name": "Synthetic Channel A", "group": "Synthetic Group"},
                {"uuid": CHANNEL_B, "number": "8", "name": "Synthetic Channel B", "group": "Synthetic Group"},
            ),
            accounts=(
                {"id": "12", "name": "Synthetic Provider"},
                {"id": "13", "name": "Synthetic Provider B"},
            ),
            streams_by_channel={
                CHANNEL_A: (
                    {"id": "44", "name": "Synthetic Stream A", "account_id": "12", "order": 0},
                    {"id": "45", "name": "Synthetic Stream B", "account_id": "13", "order": 1},
                ),
                CHANNEL_B: (
                    {"id": "46", "name": "Synthetic Stream C", "account_id": "12", "order": 0},
                ),
            },
            profiles=(
                {"id": "7", "name": "Synthetic Profile All", "channel_uuids": (CHANNEL_A, CHANNEL_B)},
                {"id": "8", "name": "Synthetic Profile B", "channel_uuids": (CHANNEL_B,)},
            ),
        )

    def tearDown(self):
        self.directory.cleanup()

    def test_real_compiler_persists_only_stable_ids_and_safe_preview(self):
        result = validate_configuration(self.settings, self.catalog)
        applied = apply_configuration(self.settings, self.catalog, self.active_path)
        active = load_active_configuration(self.active_path)
        saved = json.loads(self.active_path.read_text(encoding="utf-8"))

        self.assertEqual(2, result["selected_channel_count"])
        self.assertEqual("2 channel(s) selected.", result["message"])
        self.assertTrue(applied["applied"])
        self.assertEqual(ACTIVE_CONFIG_VERSION, saved["version"])
        self.assertEqual([CHANNEL_A, CHANNEL_B], saved["channel_uuids"])
        self.assertEqual(["7"], saved["channel_profile_ids"])
        self.assertEqual([CHANNEL_A, CHANNEL_B], active["channel_uuids"].splitlines())
        serialized = self.active_path.read_text(encoding="utf-8")
        self.assertNotIn("Synthetic Provider", serialized)
        self.assertNotIn("Synthetic Channel", serialized)
        self.assertNotIn("filter_config", serialized)
        self.assertNotIn("https://provider.invalid", serialized)

    def test_v4_snapshot_contains_settings_and_stable_per_channel_schedule(self):
        settings = dict(
            self.settings,
            filter_config=(
                "version: 1\ntimezone: UTC\nschedule: {}\n"
                "rules:\n  - channels: {profile: all}\n"
                "    schedule: continuous\n"
            ),
        )
        apply_configuration(settings, self.catalog, self.active_path)
        snapshot = json.loads(self.active_path.read_text(encoding="utf-8"))
        active = load_active_configuration(self.active_path)
        self.assertEqual(4, snapshot["version"])
        self.assertEqual(True, snapshot["settings"]["recording_enabled"])
        self.assertEqual("INFO", snapshot["settings"]["log_level"])
        self.assertEqual(
            {
                "timezone": "UTC",
                "channels": {
                    CHANNEL_A: {"mode": "continuous"},
                    CHANNEL_B: {"mode": "continuous"},
                },
            },
            snapshot["recording_schedule"],
        )
        self.assertEqual(snapshot["recording_schedule"], active["recording_schedule"])
        sidecar = json.loads(
            (self.active_path.parent / ".catchuparr-recorder-control.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(2, sidecar["version"])
        self.assertFalse(sidecar["paused"])
        applied, control = load_applied_state(self.active_path)
        self.assertEqual(active["recording_enabled"], applied["recording_enabled"])
        self.assertEqual(snapshot["settings"]["recording_enabled"], applied["recording_enabled"])
        self.assertEqual(
            configuration.active_configuration_generation(applied),
            control.configuration_generation,
        )

    def test_v4_control_snapshot_mismatch_fails_closed(self):
        apply_configuration(self.settings, self.catalog, self.active_path)
        sidecar_path = self.active_path.parent / ".catchuparr-recorder-control.json"
        sidecar_path.write_text(
            json.dumps({"version": 1, "paused": False, "generation": 0}),
            encoding="utf-8",
        )

        from catchuparr.recorder_control import RecorderControlError

        with self.assertRaisesRegex(RecorderControlError, "do not match"):
            load_applied_state(self.active_path)

    def test_apply_activates_recording_and_log_settings_together(self):
        settings = dict(self.settings, recording_enabled=False, log_level="WARNING")
        apply_configuration(settings, self.catalog, self.active_path)

        snapshot = json.loads(self.active_path.read_text(encoding="utf-8"))
        active, control = load_applied_state(self.active_path)
        self.assertFalse(snapshot["settings"]["recording_enabled"])
        self.assertEqual("WARNING", snapshot["settings"]["log_level"])
        self.assertTrue(control.paused)
        self.assertFalse(active["recording_enabled"])

    def test_public_base_url_is_normalized_and_applied_with_other_settings(self):
        settings = dict(
            self.settings,
            public_base_url="https://media.example.test/dispatcharr/",
        )
        apply_configuration(settings, self.catalog, self.active_path)

        active = load_active_configuration(self.active_path)
        self.assertEqual(
            "https://media.example.test/dispatcharr",
            active["public_base_url"],
        )

    def test_root_public_base_url_is_valid_and_normalized_idempotently(self):
        for base_url in ("https://media.example.test", "https://media.example.test/"):
            with self.subTest(base_url=base_url):
                settings = dict(self.settings, public_base_url=base_url)
                validate_configuration(settings, self.catalog)
                apply_configuration(settings, self.catalog, self.active_path)
                active = load_active_configuration(self.active_path)
                self.assertEqual("https://media.example.test", active["public_base_url"])

    def test_invalid_public_base_urls_fail_validation_without_activation(self):
        for base_url in (
            "ftp://media.example.test",
            "https://user:pass@media.example.test",
            "https://media.example.test/?token=x",
            "https://media.example.test/#fragment",
            "https://media.example.test:bad",
            "https://media.example.test/a/../b",
            "https://media.example.test/%2e%2e/b",
            "https://media.example.test/%3Cscript%3E",
        ):
            with self.subTest(base_url=base_url):
                with self.assertRaisesRegex(ValueError, "public_base_url"):
                    validate_configuration(
                        dict(self.settings, public_base_url=base_url),
                        self.catalog,
                    )
                self.assertFalse(self.active_path.exists())

    def test_v4_required_settings_are_strict(self):
        apply_configuration(self.settings, self.catalog, self.active_path)
        valid_document = json.loads(self.active_path.read_text(encoding="utf-8"))
        for key, value in (
            ("recording_enabled", None),
            ("recording_enabled", "false"),
            ("log_level", "info"),
            ("log_level", "TRACE"),
            ("public_base_url", None),
            ("public_base_url", "https://user:pass@media.example.test"),
        ):
            with self.subTest(key=key, value=value):
                document = json.loads(json.dumps(valid_document))
                if value is None:
                    document["settings"].pop(key)
                else:
                    document["settings"][key] = value
                self.active_path.write_text(json.dumps(document), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, key):
                    load_active_configuration(self.active_path)

    def test_archive_root_apply_preserves_old_archive_without_copying_or_deleting(self):
        old_root = Path(self.settings["archive_root"])
        first = apply_configuration(self.settings, self.catalog, self.active_path)
        sentinel = old_root / "archive.sqlite3"
        sentinel.write_bytes(b"synthetic archive and token database")
        next_root = self.root / "next-archive"
        result = apply_configuration(
            dict(self.settings, archive_root=str(next_root)),
            self.catalog,
            self.active_path,
        )

        self.assertFalse(first["archive_root_changed"])
        self.assertTrue(result["archive_root_changed"])
        self.assertIn("not moved or deleted", result["archive_notice"])
        self.assertEqual(b"synthetic archive and token database", sentinel.read_bytes())
        self.assertTrue(next_root.is_dir())
        self.assertEqual([], list(next_root.iterdir()))

    def test_control_write_failure_restores_active_and_control_pair(self):
        apply_configuration(self.settings, self.catalog, self.active_path)
        previous_active = self.active_path.read_bytes()
        sidecar_path = self.active_path.parent / ".catchuparr-recorder-control.json"
        previous_control = sidecar_path.read_bytes()
        updated = dict(self.settings, retention_hours=48)

        from catchuparr import recorder_control
        from catchuparr.recorder_control import RecorderControlError

        with patch.object(
            recorder_control,
            "_atomic_replace_sidecar",
            side_effect=OSError("synthetic sidecar write failure"),
        ), self.assertRaisesRegex(RecorderControlError, "previous configuration was restored"):
            apply_configuration(updated, self.catalog, self.active_path)

        self.assertEqual(previous_active, self.active_path.read_bytes())
        self.assertEqual(previous_control, sidecar_path.read_bytes())
        self.assertFalse(
            (self.active_path.parent / ".catchuparr-recorder-control-deny").exists()
        )
        active, control = load_applied_state(self.active_path)
        self.assertEqual(24, active["retention_hours"])
        self.assertEqual(
            configuration.active_configuration_generation(active),
            control.configuration_generation,
        )

    def test_database_commit_failure_restores_both_applied_files(self):
        apply_configuration(self.settings, self.catalog, self.active_path)
        previous_active = self.active_path.read_bytes()
        sidecar_path = self.active_path.parent / ".catchuparr-recorder-control.json"
        previous_control = sidecar_path.read_bytes()
        row = types.SimpleNamespace(settings=dict(self.settings, retention_hours=48))
        modules = self._django_config_modules(row)

        @contextmanager
        def fail_commit():
            yield
            raise RuntimeError("synthetic DB commit failure")

        modules["django.db"].transaction = types.SimpleNamespace(atomic=fail_commit)
        with patch.dict(sys.modules, modules), self.assertRaisesRegex(
            RuntimeError, "DB commit failure"
        ):
            apply_configuration(None, self.catalog, self.active_path)

        self.assertEqual(previous_active, self.active_path.read_bytes())
        self.assertEqual(previous_control, sidecar_path.read_bytes())
        self.assertFalse(
            (self.active_path.parent / ".catchuparr-recorder-control-deny").exists()
        )
        active, control = load_applied_state(self.active_path)
        self.assertEqual(24, active["retention_hours"])
        self.assertEqual(
            configuration.active_configuration_generation(active),
            control.configuration_generation,
        )

    def test_incomplete_apply_keeps_previous_non_recorder_settings_authoritative(self):
        apply_configuration(self.settings, self.catalog, self.active_path)
        updated = dict(self.settings, retention_hours=2)

        with patch.object(configuration._PreparedActivation, "commit"):
            apply_configuration(updated, self.catalog, self.active_path)

        # The active file already contains the candidate, but the durable
        # pending marker makes the previous snapshot authoritative until Apply
        # reaches its commit point.
        self.assertEqual(2, json.loads(self.active_path.read_text())["settings"]["retention_hours"])
        self.assertEqual(24, load_active_configuration(self.active_path)["retention_hours"])
        self.assertEqual(24, load_runtime_settings()["retention_hours"])
        from catchuparr.recorder_control import RecorderControlError

        with self.assertRaisesRegex(RecorderControlError, "incomplete"):
            load_applied_state(self.active_path)

        # A later Apply first restores the interrupted snapshot, then commits
        # the new candidate under a fresh activation marker.
        apply_configuration(self.settings, self.catalog, self.active_path)
        self.assertEqual(24, load_active_configuration(self.active_path)["retention_hours"])
        self.assertFalse((self.active_path.parent / configuration.ACTIVATION_STATE_NAME).exists())

    def test_apply_recovery_from_pending_control_deny_advances_generation(self):
        apply_configuration(self.settings, self.catalog, self.active_path)
        active, control = load_applied_state(self.active_path)
        from catchuparr import recorder_control

        pending = recorder_control.RecorderControlState(
            paused=False,
            generation=control.generation + 4,
            configuration_generation=configuration.active_configuration_generation(active),
        )
        recorder_control.control_state_path(self.active_path).write_text(
            json.dumps(pending.to_json()), encoding="utf-8"
        )
        recorder_control._write_deny_marker(recorder_control.control_state_path(self.active_path))

        apply_configuration(self.settings, self.catalog, self.active_path)

        _active, recovered = load_applied_state(self.active_path)
        self.assertFalse(recovered.paused)
        self.assertEqual(pending.generation + 1, recovered.generation)

    def test_applied_state_reader_waits_for_both_files_during_apply(self):
        apply_configuration(self.settings, self.catalog, self.active_path)
        updated = dict(self.settings, retention_hours=48)
        from catchuparr import recorder_control

        entered_control_write = threading.Event()
        allow_control_write = threading.Event()
        reader_done = threading.Event()
        observed = []
        failures = []
        real_control_write = recorder_control._configuration_control_state_locked

        def pause_between_file_writes(*args, **kwargs):
            entered_control_write.set()
            if not allow_control_write.wait(2):
                raise RuntimeError("synthetic Apply synchronization timed out")
            return real_control_write(*args, **kwargs)

        def writer():
            try:
                apply_configuration(updated, self.catalog, self.active_path)
            except Exception as exc:
                failures.append(exc)

        def reader():
            try:
                observed.append(load_applied_state(self.active_path))
            except Exception as exc:
                failures.append(exc)
            finally:
                reader_done.set()

        with patch.object(
            recorder_control,
            "_configuration_control_state_locked",
            side_effect=pause_between_file_writes,
        ):
            writer_thread = threading.Thread(target=writer)
            writer_thread.start()
            self.assertTrue(entered_control_write.wait(2))
            reader_thread = threading.Thread(target=reader)
            reader_thread.start()
            self.assertFalse(reader_done.wait(0.05))
            allow_control_write.set()
            writer_thread.join(2)
            reader_thread.join(2)

        self.assertFalse(writer_thread.is_alive())
        self.assertFalse(reader_thread.is_alive())
        self.assertEqual([], failures)
        self.assertEqual(1, len(observed))
        active, control = observed[0]
        self.assertEqual(48, active["retention_hours"])
        self.assertEqual(
            configuration.active_configuration_generation(active),
            control.configuration_generation,
        )

    def test_v3_snapshot_remains_readable_without_v4_settings(self):
        apply_configuration(self.settings, self.catalog, self.active_path)
        snapshot = json.loads(self.active_path.read_text(encoding="utf-8"))
        snapshot["version"] = 3
        snapshot["settings"].pop("recording_enabled")
        snapshot["settings"].pop("log_level")
        snapshot["settings"].pop("public_base_url")
        self.active_path.write_text(json.dumps(snapshot), encoding="utf-8")

        active = load_active_configuration(self.active_path)

        self.assertEqual(3, active["version"])
        self.assertNotIn("recording_enabled", active)
        self.assertNotIn("log_level", active)

    def test_v2_snapshot_loads_as_continuous_without_rewriting_or_resetting(self):
        apply_configuration(self.settings, self.catalog, self.active_path)
        snapshot = json.loads(self.active_path.read_text(encoding="utf-8"))
        snapshot["version"] = 2
        snapshot.pop("recording_schedule")
        snapshot["settings"].pop("recording_enabled")
        snapshot["settings"].pop("log_level")
        snapshot["settings"].pop("public_base_url")
        self.active_path.write_text(json.dumps(snapshot), encoding="utf-8")
        original = self.active_path.read_bytes()
        row = types.SimpleNamespace(settings={"filter_config": self.settings["filter_config"]})
        row.save = lambda **_kwargs: None

        with patch.dict(sys.modules, self._django_config_modules(row)):
            active = load_active_configuration(self.active_path)
            self.assertFalse(reset_legacy_configuration(self.active_path))

        self.assertEqual(original, self.active_path.read_bytes())
        self.assertEqual(2, active["version"])
        self.assertEqual(
            {
                "timezone": "Europe/Berlin",
                "channels": {
                    CHANNEL_A: {"mode": "continuous"},
                    CHANNEL_B: {"mode": "continuous"},
                },
            },
            active["recording_schedule"],
        )

    def test_apply_reads_current_saved_settings_under_the_file_lock(self):
        persisted = dict(
            self.settings,
            filter_config=(
                "version: 1\nrules:\n"
                "  - channels: {names: [Synthetic Channel B]}\n"
                "    schedule: {}\n"
            ),
        )
        row = types.SimpleNamespace(settings=persisted)
        row.save = lambda **_kwargs: None
        with patch.dict(sys.modules, self._django_config_modules(row)):
            result = apply_configuration(None, self.catalog, self.active_path)
        active = load_active_configuration(self.active_path)
        self.assertEqual(1, result["selected_channel_count"])
        self.assertEqual(CHANNEL_B, active["channel_uuids"])
        self.assertEqual(
            {"mode": "weekly", "intervals": []},
            active["recording_schedule"]["channels"][CHANNEL_B],
        )

    def test_invalid_recorder_control_and_log_level_settings_are_rejected(self):
        for key, value in (("recording_enabled", "false"), ("log_level", "TRACE")):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, key):
                validate_configuration(dict(self.settings, **{key: value}), self.catalog)

    def test_malformed_v4_schedule_fails_closed(self):
        apply_configuration(self.settings, self.catalog, self.active_path)
        document = json.loads(self.active_path.read_text(encoding="utf-8"))
        document["recording_schedule"]["channels"].pop(CHANNEL_A)
        self.active_path.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "schedule channels"):
            load_active_configuration(self.active_path)

    def test_blank_yaml_validates_zero_and_applies_no_recording_channels(self):
        settings = dict(self.settings, filter_config="\n  \n")
        result = validate_configuration(settings, self.catalog)
        apply_configuration(settings, self.catalog, self.active_path)
        active = load_active_configuration(self.active_path)

        self.assertEqual(0, result["selected_channel_count"])
        self.assertEqual("No channels are selected.", result["message"])
        self.assertEqual("", active["channel_uuids"])
        self.assertEqual([], active["channel_profile_ids"])

    def test_complete_specific_override_filters_sources_and_priority(self):
        settings = dict(
            self.settings,
            filter_config=(
                "version: 1\n"
                "profile: Synthetic Profile All\n"
                "rules:\n"
                "  - channels: {profile: all}\n"
                "    exclude: [Synthetic Provider B]\n"
                "  - channels: {names: [Synthetic Channel A]}\n"
                "    include: [Synthetic Provider B]\n"
                "    priority: {Synthetic Provider B: 100}\n"
            ),
        )
        result = validate_configuration(settings, self.catalog)
        preview = {row["channel_uuid"]: row for row in result["channels"]}

        self.assertEqual(["45"], [row["stream_id"] for row in preview[CHANNEL_A]["candidates"]])
        self.assertEqual(["46"], [row["stream_id"] for row in preview[CHANNEL_B]["candidates"]])
        self.assertTrue(preview[CHANNEL_A]["source_override"])
        self.assertTrue(preview[CHANNEL_B]["source_override"])

    def test_empty_exclude_filter_without_priority_preserves_shared_route(self):
        settings = dict(
            self.settings,
            filter_config=(
                "version: 1\nprofile: Synthetic Profile All\nrules:\n"
                "  - channels: {profile: all}\n    exclude: []\n"
            ),
        )
        apply_configuration(settings, self.catalog, self.active_path)
        active = load_active_configuration(self.active_path)
        self.assertEqual({}, active["source_policies"])

    def test_invalid_yaml_or_filter_preserves_previous_active(self):
        apply_configuration(self.settings, self.catalog, self.active_path)
        previous = self.active_path.read_bytes()
        for invalid in (
            dict(self.settings, filter_config="version: 1\nprofile: all\nrules: []\nextra: true\n"),
            dict(
                self.settings,
                filter_config=(
                    "version: 1\nprofile: all\nrules:\n"
                    "  - channels: {profile: all}\n    include: [Missing]\n"
                ),
            ),
        ):
            with self.subTest(filter_config=invalid["filter_config"]), self.assertRaises(ValueError):
                apply_configuration(invalid, self.catalog, self.active_path)
            self.assertEqual(previous, self.active_path.read_bytes())

    def test_legacy_filter_fields_cannot_be_applied_or_migrated(self):
        legacy = {**self.settings, "channel_uuids": CHANNEL_A, "source_rules": "old"}
        with self.assertRaisesRegex(ValueError, "legacy channel selection was removed"):
            validate_configuration(legacy, self.catalog)
        self.active_path.parent.mkdir(parents=True, exist_ok=True)
        old_snapshot = {
            "version": 1,
            "settings": {"archive_root": str(self.root / "archive"), "channel_uuids": CHANNEL_A},
            "source_policies": {},
        }
        self.active_path.write_text(json.dumps(old_snapshot), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "invalid"):
            load_active_configuration(self.active_path)

    def test_active_snapshot_corruption_fails_closed(self):
        self.active_path.parent.mkdir(parents=True, exist_ok=True)
        self.active_path.write_text("{partial", encoding="utf-8")
        with self.assertRaises(json.JSONDecodeError):
            load_active_configuration(self.active_path)

    def test_active_policy_rejects_priority_forbidden_by_include_filter(self):
        settings = dict(
            self.settings,
            filter_config=(
                "version: 1\nprofile: all\nrules:\n"
                "  - channels: {names: [Synthetic Channel A]}\n"
                "    include: [Synthetic Provider]\n"
            ),
        )
        apply_configuration(settings, self.catalog, self.active_path)
        document = json.loads(self.active_path.read_text(encoding="utf-8"))
        document["source_policies"][CHANNEL_A]["priorities"] = [["13", 10]]
        self.active_path.write_text(json.dumps(document), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "violates the include filter"):
            load_active_configuration(self.active_path)

    def test_apply_readers_observe_only_complete_v2_snapshots(self):
        apply_configuration(self.settings, self.catalog, self.active_path)
        start = threading.Event()
        failures = []

        def writer():
            start.wait()
            try:
                for _ in range(20):
                    apply_configuration(self.settings, self.catalog, self.active_path)
            except Exception as exc:  # surfaced from the worker thread
                failures.append(exc)

        worker = threading.Thread(target=writer)
        worker.start()
        start.set()
        for _ in range(80):
            try:
                current = load_active_configuration(self.active_path)
                self.assertEqual([CHANNEL_A, CHANNEL_B], current["channel_uuids"].splitlines())
            except Exception as exc:
                failures.append(exc)
                break
        worker.join()
        self.assertEqual([], failures)

    def _django_config_modules(self, row):
        class Manager:
            def select_for_update(self):
                return self

            def filter(self, **kwargs):
                self.kwargs = kwargs
                return self

            def first(self):
                return row

        plugins = types.ModuleType("apps.plugins")
        models = types.ModuleType("apps.plugins.models")
        models.PluginConfig = type("PluginConfig", (), {"objects": Manager()})
        plugins.models = models
        apps = types.ModuleType("apps")
        apps.plugins = plugins
        django = types.ModuleType("django")
        db = types.ModuleType("django.db")
        db.transaction = types.SimpleNamespace(atomic=nullcontext)
        django.db = db
        return {
            "apps": apps,
            "apps.plugins": plugins,
            "apps.plugins.models": models,
            "django": django,
            "django.db": db,
        }

    def test_draft_only_legacy_reset_preserves_general_settings_and_is_idempotent(self):
        row = types.SimpleNamespace(
            settings={
                "channel_uuids": CHANNEL_A,
                "source_rules": "legacy source text",
                "archive_root": str(self.root / "archive"),
                "retention_hours": 36,
                "max_storage_gib": 12,
                "playback_user_id": 9,
            },
            save=lambda **kwargs: None,
        )
        row.saved = []
        row.save = lambda **kwargs: row.saved.append(kwargs)
        with patch.dict(sys.modules, self._django_config_modules(row)):
            self.assertTrue(reset_legacy_configuration(self.active_path))
            self.assertFalse(reset_legacy_configuration(self.active_path))
        self.assertNotIn("channel_uuids", row.settings)
        self.assertNotIn("source_rules", row.settings)
        self.assertEqual("", row.settings["filter_config"])
        self.assertEqual(str(self.root / "archive"), row.settings["archive_root"])
        self.assertEqual(36, row.settings["retention_hours"])
        self.assertEqual(12, row.settings["max_storage_gib"])
        self.assertEqual(9, row.settings["playback_user_id"])
        self.assertTrue(configuration_reset_required(self.active_path))
        apply_configuration(self.settings, self.catalog, self.active_path)
        self.assertFalse(configuration_reset_required(self.active_path))

    def test_active_only_and_combined_legacy_reset_removes_only_v1_snapshot(self):
        recording = self.root / "archive" / "segments" / "synthetic.ts"
        recording.parent.mkdir(parents=True)
        recording.write_bytes(b"synthetic recording")
        token_store = self.root / "archive" / "access-tokens.json"
        token_store.write_bytes(b"synthetic token digest")
        self.active_path.parent.mkdir(parents=True, exist_ok=True)
        row = types.SimpleNamespace(
            settings={"archive_root": str(self.root / "archive"), "retention_hours": 24},
            save=lambda **kwargs: None,
        )
        legacy = {
            "version": 1,
            "settings": {"archive_root": str(self.root / "archive"), "channel_uuids": CHANNEL_A},
            "source_policies": {},
        }
        self.active_path.write_text(json.dumps(legacy), encoding="utf-8")
        with patch.dict(sys.modules, self._django_config_modules(row)):
            self.assertTrue(reset_legacy_configuration(self.active_path))
        self.assertFalse(self.active_path.exists())
        self.assertEqual(str(self.root / "archive"), row.settings["archive_root"])
        self.assertEqual("", row.settings["filter_config"])
        self.assertTrue(recording.is_file())
        self.assertEqual(b"synthetic token digest", token_store.read_bytes())

    def test_combined_legacy_reset_clears_filter_keys_and_preserves_other_settings(self):
        self.active_path.parent.mkdir(parents=True, exist_ok=True)
        self.active_path.write_text(
            json.dumps({
                "version": 1,
                "settings": {"channel_uuids": CHANNEL_A},
                "source_policies": {},
            }),
            encoding="utf-8",
        )
        row = types.SimpleNamespace(
            settings={
                "channel_uuids": CHANNEL_A,
                "source_rules": "legacy source rules",
                "filter_config": "draft YAML must be reset",
                "archive_root": str(self.root / "archive"),
                "playback_user_id": 23,
            },
        )
        row.save = lambda **kwargs: None
        with patch.dict(sys.modules, self._django_config_modules(row)):
            self.assertTrue(reset_legacy_configuration(self.active_path))
        self.assertFalse(self.active_path.exists())
        self.assertEqual(
            {
                "filter_config": "",
                "archive_root": str(self.root / "archive"),
                "playback_user_id": 23,
            },
            row.settings,
        )

    def test_reset_rereads_v2_after_waiting_for_apply_lock(self):
        row = types.SimpleNamespace(
            settings={
                "channel_uuids": CHANNEL_A,
                "source_rules": "old",
                "filter_config": "version: 1\nprofile: Synthetic Profile All\nrules: []\n",
            },
            save=lambda **kwargs: None,
        )
        legacy = {
            "version": 1,
            "settings": {"archive_root": str(self.root / "archive"), "channel_uuids": CHANNEL_A},
            "source_policies": {},
        }
        self.active_path.parent.mkdir(parents=True, exist_ok=True)
        self.active_path.write_text(json.dumps(legacy), encoding="utf-8")
        apply_configuration(self.settings, self.catalog, self.root / "pending-v2.json")
        new_snapshot = (self.root / "pending-v2.json").read_bytes()
        original = new_snapshot
        started = threading.Event()
        result = []

        def reset_worker():
            started.set()
            result.append(reset_legacy_configuration(self.active_path))

        with patch.dict(sys.modules, self._django_config_modules(row)):
            with configuration._config_lock(self.active_path, exclusive=True):
                worker = threading.Thread(target=reset_worker)
                worker.start()
                self.assertTrue(started.wait(timeout=1))
                # Simulate Apply finishing its atomic replace before the waiting
                # bootstrap can re-read and classify the snapshot.
                self.active_path.write_bytes(new_snapshot)
            worker.join(timeout=2)
        self.assertFalse(worker.is_alive())
        self.assertEqual([True], result)
        self.assertEqual(original, self.active_path.read_bytes())
        self.assertNotIn("channel_uuids", row.settings)
        self.assertNotIn("source_rules", row.settings)
        self.assertTrue(row.settings["filter_config"].startswith("version: 1"))

    def test_existing_v2_apply_clears_stale_reset_marker(self):
        row = types.SimpleNamespace(
            settings={"filter_config": self.settings["filter_config"]},
            save=lambda **kwargs: None,
        )
        apply_configuration(self.settings, self.catalog, self.active_path)
        marker = self.active_path.with_name(RESET_REQUIRED_NAME)
        marker.write_text("apply-required\n", encoding="utf-8")

        with patch.dict(sys.modules, self._django_config_modules(row)):
            self.assertFalse(reset_legacy_configuration(self.active_path))

        self.assertFalse(marker.exists())
        active = load_active_configuration(self.active_path)
        self.assertEqual([CHANNEL_A, CHANNEL_B], active["channel_uuids"].splitlines())

    def test_source_catalog_uses_enabled_native_channel_profile_memberships(self):
        channel = types.SimpleNamespace(
            uuid=CHANNEL_A,
            channel_number=7,
            name="Synthetic Channel A",
            channel_group=types.SimpleNamespace(name="Synthetic Group"),
        )
        profile = types.SimpleNamespace(id=7, name="Synthetic Native Profile")
        membership = types.SimpleNamespace(
            channel_profile_id=7, channel=channel, enabled=True,
        )
        disabled_channel = types.SimpleNamespace(uuid=CHANNEL_B)
        disabled_membership = types.SimpleNamespace(
            channel_profile_id=7, channel=disabled_channel, enabled=False,
        )
        account = types.SimpleNamespace(id=12, name="Synthetic Provider")
        stream = types.SimpleNamespace(
            id=44, name="Synthetic Stream A", m3u_account_id=12, m3u_account=account,
            url="https://provider.invalid/synthetic",
        )
        assignment = types.SimpleNamespace(channel=channel, stream=stream, order=3)

        class Query:
            def __init__(self, values):
                self.values = values

            def select_related(self, *args):
                return self

            def order_by(self, *args):
                return self.values

            def all(self):
                return self

            def only(self, *args):
                return self.values

            def filter(self, **kwargs):
                self.filter_kwargs = kwargs
                if kwargs.get("enabled") is True:
                    self.values = [value for value in self.values if value.enabled]
                return self

            def __iter__(self):
                return iter(self.values)

        channels = types.ModuleType("apps.channels")
        channel_models = types.ModuleType("apps.channels.models")
        channel_models.Channel = type("Channel", (), {"objects": Query([channel])})
        channel_models.ChannelStream = type("ChannelStream", (), {"objects": Query([assignment])})
        channel_models.ChannelProfile = type("ChannelProfile", (), {"objects": Query([profile])})
        channel_models.ChannelProfileMembership = type(
            "ChannelProfileMembership", (), {"objects": Query([membership, disabled_membership])}
        )
        channels.models = channel_models
        m3u = types.ModuleType("apps.m3u")
        m3u_models = types.ModuleType("apps.m3u.models")
        m3u_models.M3UAccount = type("M3UAccount", (), {"objects": Query([account])})
        m3u.models = m3u_models
        apps = types.ModuleType("apps")
        apps.channels = channels
        apps.m3u = m3u
        modules = {
            "apps": apps,
            "apps.channels": channels,
            "apps.channels.models": channel_models,
            "apps.m3u": m3u,
            "apps.m3u.models": m3u_models,
        }
        with patch.dict(sys.modules, modules):
            catalog = source_catalog()
        self.assertEqual("7", catalog.profiles[0]["id"])
        self.assertEqual((CHANNEL_A,), catalog.profiles[0]["channel_uuids"])
        self.assertNotIn("url", repr(catalog))

    def test_plugin_config_row_is_preferred_to_context_fallback(self):
        plugins = types.ModuleType("apps.plugins")
        models = types.ModuleType("apps.plugins.models")

        class Manager:
            def filter(self, **kwargs):
                self.kwargs = kwargs
                return self

            def first(self):
                return types.SimpleNamespace(settings={"filter_config": "saved YAML"})

        models.PluginConfig = type("PluginConfig", (), {"objects": Manager()})
        plugins.models = models
        apps = types.ModuleType("apps")
        apps.plugins = plugins
        with patch.dict(sys.modules, {
            "apps": apps,
            "apps.plugins": plugins,
            "apps.plugins.models": models,
        }):
            settings = load_draft_settings({"filter_config": "unsaved"})
        self.assertEqual("saved YAML", settings["filter_config"])

    def test_runtime_without_apply_uses_safe_defaults_not_draft_settings(self):
        with patch("catchuparr.configuration.load_applied_state", return_value=(None, types.SimpleNamespace(paused=False, generation=0))), patch(
            "catchuparr.configuration.reset_legacy_configuration", return_value=False
        ), patch(
            "catchuparr.configuration.load_draft_settings",
            side_effect=AssertionError("runtime must not read the unsaved draft"),
        ):
            settings = load_runtime_settings()
        self.assertEqual("", settings["channel_uuids"])
        self.assertNotIn("source_rules", settings)
        self.assertFalse(settings["recording_enabled"])


if __name__ == "__main__":
    unittest.main()
