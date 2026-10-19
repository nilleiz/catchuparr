import copy
import sqlite3
import sys
import tempfile
import time
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from catchuparr.configuration import active_configuration_generation
from catchuparr.plugin import Plugin
from catchuparr.recording_overview import (
    ACTION_MESSAGE_SEPARATOR,
    _message_row,
    _safe_display,
    _source_for_diagnostic,
    current_recordings,
)
from catchuparr.schedule import normalize_schedule


class OverviewRedis:
    def ping(self):
        return True

    def ttl(self, _key):
        return -2

    def hgetall(self, _key):
        return {}


class RecordingOverviewTests(unittest.TestCase):
    def setUp(self):
        self.channel = str(uuid.uuid4())
        self.now = datetime(2026, 10, 19, 12, tzinfo=timezone.utc)
        self.root = Path(tempfile.mkdtemp())
        self.database = self.root / "archive.sqlite3"
        with sqlite3.connect(self.database) as db:
            db.execute(
                "CREATE TABLE segments (channel_id TEXT, start_utc REAL, end_utc REAL, created_at REAL)"
            )
        self.settings = {
            "version": 7,
            "channel_uuids": self.channel,
            "archive_root": str(self.root),
            "recording_schedule": {
                "timezone": "UTC",
                "channels": {self.channel: normalize_schedule("continuous").to_snapshot()},
            },
        }
        generation = active_configuration_generation(self.settings)
        self.control = SimpleNamespace(paused=False, generation=4, configuration_generation=generation)
        self.redis = OverviewRedis()

    def _diagnostic(self, *, started, phase="recording"):
        return {self.channel: {
            "phase": phase, "source": "dedicated", "stream_id": "17", "account_id": "4",
            "m3u_profile_id": "9", "input_profile_id": "4",
            "started_at": started, "error_code": "",
        }}

    def _call(self, diagnostics, control=None):
        with patch("catchuparr.recording_overview._channel_metadata", return_value={
            self.channel: {"number": 12, "name": "Synthetic Channel A"},
        }), patch("catchuparr.recording_overview.read_recorder_diagnostics", return_value=diagnostics), \
             patch("catchuparr.recording_overview.source_metadata", return_value={
                 "stream_name": "Synthetic Stream A", "provider_id": "4", "provider_name": "Synthetic Provider",
             }):
            return current_recordings(
                self.settings, control or self.control, redis_client=self.redis, now=self.now,
            )

    def test_dedicated_diagnostic_without_native_input_profile_has_no_profile_lookup(self):
        source = _source_for_diagnostic(self.channel, {
            "source": "dedicated", "stream_id": "17", "account_id": "4",
            "m3u_profile_id": "9", "input_profile_id": "",
        })
        self.assertIsNone(source["input_profile_id"])

    def test_recording_requires_fresh_indexed_media_after_attempt_start(self):
        start = self.now.timestamp() - 100
        with sqlite3.connect(self.database) as db:
            db.execute("INSERT INTO segments VALUES (?, ?, ?, ?)", (
                self.channel, start - 60, start - 20, start - 20,
            ))
            db.execute("INSERT INTO segments VALUES (?, ?, ?, ?)", (
                self.channel, self.now.timestamp() - 30, self.now.timestamp() - 10,
                self.now.timestamp() - 10,
            ))
        result = self._call(self._diagnostic(started=start))
        row = result["channels"][0]
        self.assertEqual(row["state"], "recording")
        self.assertEqual(row["indexed_segments_since_attempt"], 1)
        self.assertTrue(row["recent_media_progress"])
        self.assertEqual(
            datetime.fromtimestamp(self.now.timestamp() - 10, tz=timezone.utc).isoformat(),
            row["last_confirmed_progress_at"],
        )
        self.assertFalse(row["last_confirmed_progress_none"])
        self.assertIn("Synthetic Channel A", result["message"])

    def test_old_segments_do_not_count_and_startup_becomes_no_progress(self):
        started = self.now.timestamp() - 120
        with sqlite3.connect(self.database) as db:
            db.execute("INSERT INTO segments VALUES (?, ?, ?, ?)", (
                self.channel, started - 30, started - 5, started - 5,
            ))
        result = self._call(self._diagnostic(started=started))
        self.assertEqual(result["channels"][0]["state"], "no_media_progress")
        self.assertEqual(result["channels"][0]["indexed_segments_since_attempt"], 0)

    def test_stale_indexed_media_reports_no_progress(self):
        started = self.now.timestamp() - 500
        with sqlite3.connect(self.database) as db:
            db.execute("INSERT INTO segments VALUES (?, ?, ?, ?)", (
                self.channel, started + 20, self.now.timestamp() - 130,
                self.now.timestamp() - 130,
            ))
        result = self._call(self._diagnostic(started=started))
        self.assertEqual(result["channels"][0]["state"], "no_media_progress")
        self.assertFalse(result["channels"][0]["recent_media_progress"])

    def test_preexisting_future_segment_is_not_attempt_progress(self):
        started = self.now.timestamp() - 100
        with sqlite3.connect(self.database) as db:
            db.execute("INSERT INTO segments VALUES (?, ?, ?, ?)", (
                self.channel, started - 5, self.now.timestamp() + 3600,
                started - 1,
            ))
        result = self._call(self._diagnostic(started=started))
        row = result["channels"][0]
        self.assertEqual(0, row["indexed_segments_since_attempt"])
        self.assertIsNone(row["last_confirmed_progress_at"])
        self.assertTrue(row["last_confirmed_progress_none"])

    def test_missing_source_assignment_fails_closed(self):
        with patch("catchuparr.recording_overview._channel_metadata", return_value={}), \
             patch("catchuparr.recording_overview.read_recorder_diagnostics",
                   return_value=self._diagnostic(started=self.now.timestamp() - 10)), \
             patch("catchuparr.recording_overview.source_metadata", return_value=None):
            result = current_recordings(self.settings, self.control, redis_client=self.redis, now=self.now)
        self.assertEqual(result["channels"][0]["state"], "unknown")
        self.assertEqual(result["channels"][0]["mode"], "unconfirmed")
        self.assertEqual(result["channels"][0]["reason"], "source_binding_unavailable")

    def test_pause_is_visible_and_keeps_selected_channel(self):
        paused = SimpleNamespace(paused=True, generation=5, configuration_generation=self.control.configuration_generation)
        result = self._call({}, control=paused)
        self.assertEqual(len(result["channels"]), 1)
        self.assertEqual(result["channels"][0]["state"], "paused")

    def test_cleanup_and_error_states_outside_active_schedule_are_hidden_from_message_only(self):
        self.settings["recording_schedule"]["channels"][self.channel] = normalize_schedule({
            "monday": [],
        }).to_snapshot()
        self.control.configuration_generation = active_configuration_generation(self.settings)
        cleanup = self._call(self._diagnostic(started=self.now.timestamp(), phase="cleanup_pending"))
        self.assertEqual(cleanup["channels"][0]["state"], "cleanup_pending")
        self.assertEqual(cleanup["message"], "No recordings scheduled now.")
        error = self._diagnostic(started=self.now.timestamp(), phase="error")
        error[self.channel]["error_code"] = "recorder_worker_stopped"
        failed = self._call(error)
        self.assertEqual(failed["channels"][0]["state"], "error")
        self.assertNotIn("failed:", failed["message"])

    def test_borrowed_source_displays_only_current_grant_metadata(self):
        diagnostic = self._diagnostic(started=self.now.timestamp(), phase="starting")
        diagnostic[self.channel].update({
            "source": "borrowed", "m3u_profile_id": "9",
        })
        with patch("catchuparr.recording_overview._channel_metadata", return_value={
            self.channel: {"number": 12, "name": "Synthetic Channel A"},
        }), patch("catchuparr.recording_overview.read_recorder_diagnostics", return_value=diagnostic), \
             patch("catchuparr.recording_overview.source_metadata", return_value={
                 "stream_name": "Synthetic Stream A", "provider_id": "4",
                 "provider_name": "Synthetic Provider", "m3u_profile_id": "9",
                 "m3u_profile_name": "Synthetic Provider Profile", "input_profile_id": "4",
                 "input_profile_name": "Synthetic Stream Profile", "profile_id": "4",
                 "profile_name": "Synthetic Stream Profile",
             }):
            result = current_recordings(
                self.settings, self.control, redis_client=self.redis, now=self.now,
            )
        row = result["channels"][0]
        self.assertEqual(row["mode"], "borrowed")
        self.assertEqual(row["source"]["input_profile_name"], "Synthetic Stream Profile")
        self.assertEqual(row["source"]["m3u_profile_name"], "Synthetic Provider Profile")

    def test_outside_schedule_is_explicit(self):
        self.settings["recording_schedule"]["channels"][self.channel] = normalize_schedule({
            "monday": [],
        }).to_snapshot()
        self.control.configuration_generation = active_configuration_generation(self.settings)
        result = self._call({})
        self.assertEqual(result["channels"][0]["state"], "outside_schedule")

    def test_redis_failure_is_explicit_unknown(self):
        class BrokenRedis:
            def ping(self):
                raise OSError("unavailable")

        result = current_recordings(
            self.settings, self.control, redis_client=BrokenRedis(), now=self.now,
        )
        self.assertEqual(result["channels"][0]["state"], "unknown")
        self.assertEqual(result["channels"][0]["reason"], "redis_unavailable")

    def test_missing_archive_database_is_unknown_without_creation(self):
        self.database.unlink()
        result = self._call(self._diagnostic(started=self.now.timestamp() - 200))
        self.assertEqual(result["channels"][0]["state"], "unknown")
        self.assertEqual(result["channels"][0]["reason"], "archive_index_unavailable")
        self.assertFalse(self.database.exists())

    def test_starting_with_unavailable_archive_is_unknown(self):
        self.database.unlink()
        result = self._call(self._diagnostic(
            started=self.now.timestamp() - 5, phase="starting",
        ))
        self.assertEqual("unknown", result["channels"][0]["state"])
        self.assertEqual("archive_index_unavailable", result["channels"][0]["reason"])
        self.assertTrue(result["channels"][0]["last_confirmed_progress_none"])

    def test_message_is_compact_and_keeps_structured_source_progress(self):
        diagnostic = self._diagnostic(started=self.now.timestamp() - 5, phase="starting")
        with patch("catchuparr.recording_overview._channel_metadata", return_value={
            self.channel: {"number": 12, "name": "Synthetic Channel A"},
        }), patch("catchuparr.recording_overview.read_recorder_diagnostics", return_value=diagnostic), \
             patch("catchuparr.recording_overview.source_metadata", return_value={
                 "stream_name": "Synthetic Stream A", "stream_id": "17",
                 "provider_id": "4", "provider_name": "Synthetic Provider",
                 "m3u_profile_id": "9", "m3u_profile_name": "Synthetic Provider Profile",
                 "input_profile_id": "4", "input_profile_name": "Synthetic Stream Profile",
                 "profile_id": "4", "profile_name": "Synthetic Stream Profile",
             }):
            result = current_recordings(
                self.settings, self.control, redis_client=self.redis, now=self.now,
            )
        self.assertEqual(result["message"], "Synthetic Channel A - Synthetic [starting]")
        self.assertEqual(result["channels"][0]["source"]["provider_name"], "Synthetic Provider")
        self.assertEqual(result["channels"][0]["source"]["stream_name"], "Synthetic Stream A")
        self.assertEqual(result["channels"][0]["last_confirmed_progress_at"], None)
        self.assertNotIn("provider Synthetic Provider", result["message"])
        self.assertNotIn("id 17", result["message"])

    def test_current_recordings_action_uses_short_m3u_label_and_padding_only_in_message(self):
        second = str(uuid.uuid4())
        active = copy.deepcopy(self.settings)
        active["channel_uuids"] = f"{self.channel}\n{second}"
        active["recording_schedule"]["channels"][second] = normalize_schedule(
            "continuous",
        ).to_snapshot()
        control = SimpleNamespace(
            paused=False,
            configuration_generation=active_configuration_generation(active),
        )
        channels = {
            self.channel: {"number": 731, "name": "Synthetic Channel A"},
            second: {"number": 842, "name": "Synthetic Channel B"},
        }
        diagnostics = {
            channel_id: {
                "phase": "starting", "source": "dedicated", "stream_id": stream_id,
                "account_id": "account", "m3u_profile_id": profile_id,
                "input_profile_id": "input", "started_at": time.time() - 5,
                "error_code": "",
            }
            for channel_id, stream_id, profile_id in (
                (self.channel, "stream-a", "profile-a"),
                (second, "stream-b", "profile-b"),
            )
        }
        sources = {
            self.channel: {
                "stream_name": "Distinct Stream Full Name",
                "m3u_profile_name": "M3UActual Full Profile",
                "provider_name": "Synthetic Provider Full Name",
            },
            second: {
                "stream_name": "Different Stream Full Name",
                "m3u_profile_name": "Alternate Actual Profile",
                "provider_name": "Other Synthetic Provider",
            },
        }
        core_module = ModuleType("core")
        core_module.__path__ = []
        utils_module = ModuleType("core.utils")

        class RedisClientFactory:
            @staticmethod
            def get_client():
                return self.redis

        utils_module.RedisClient = RedisClientFactory
        with patch("catchuparr.configuration.load_applied_state_readonly",
                   return_value=(active, control)), \
             patch.dict(sys.modules, {"core": core_module, "core.utils": utils_module}), \
             patch("catchuparr.recording_overview._selected_channel_ids",
                   return_value=[self.channel, second]), \
             patch("catchuparr.recording_overview._channel_metadata", return_value=channels), \
             patch("catchuparr.recording_overview.read_recorder_diagnostics",
                   return_value=diagnostics), \
             patch("catchuparr.recording_overview.source_metadata",
                   side_effect=lambda channel_id, *_args: sources[channel_id]):
            result = Plugin.__new__(Plugin).run("current_recordings", {}, {})

        expected = ACTION_MESSAGE_SEPARATOR.join((
            "Synthetic Channel A - M3UActual [starting]",
            "Synthetic Channel B - Alternate [starting]",
        ))
        self.assertEqual([row["state"] for row in result["channels"]], ["starting", "starting"])
        self.assertEqual(result["message"], expected)
        self.assertNotIn("\n", result["message"])
        self.assertEqual(result["message"].count("\u00a0"), 200)
        self.assertNotIn("731", result["message"])
        self.assertNotIn("842", result["message"])
        self.assertNotIn("Distinct Stream Full Name", result["message"])
        self.assertNotIn("M3UActual Full Profile", result["message"])
        self.assertNotIn("Synthetic Provider Full Name", result["message"])
        rows = {row["channel_uuid"]: row for row in result["channels"]}
        self.assertEqual(rows[self.channel]["number"], 731)
        self.assertEqual(rows[self.channel]["source"]["stream_name"], "Distinct Stream Full Name")
        self.assertEqual(rows[self.channel]["source"]["m3u_profile_name"], "M3UActual Full Profile")
        self.assertEqual(result["summary"]["starting"], 2)

    def test_active_confirmed_recording_and_borrowed_attachment_compact_status(self):
        started = self.now.timestamp() - 20
        with sqlite3.connect(self.database) as db:
            db.execute("INSERT INTO segments VALUES (?, ?, ?, ?)", (
                self.channel, started, self.now.timestamp(), self.now.timestamp(),
            ))
        diagnostic = self._diagnostic(started=started)
        diagnostic[self.channel]["source"] = "borrowed"
        with patch("catchuparr.recording_overview._channel_metadata", return_value={
            self.channel: {"number": 12, "name": "Synthetic Channel A"},
        }), patch("catchuparr.recording_overview.read_recorder_diagnostics", return_value=diagnostic), \
             patch("catchuparr.recording_overview.source_metadata", return_value={
                 "stream_name": "Synthetic Stream Name", "m3u_profile_name": "Synthetic M3U Profile",
                 "provider_name": "private provider",
                 "provider_id": "42", "stream_id": "17",
             }):
            result = current_recordings(
                self.settings, self.control, redis_client=self.redis, now=self.now,
            )
        self.assertEqual(result["message"],
                         "Synthetic Channel A - Synthetic [ok, attached]")
        self.assertEqual(result["channels"][0]["state"], "recording")

    def test_active_error_is_safe_and_unknown_schedule_is_explicit(self):
        error = self._diagnostic(started=self.now.timestamp(), phase="error")
        error[self.channel]["error_code"] = "recorder_worker_stopped"
        result = self._call(error)
        self.assertEqual(result["message"],
                         "Synthetic Channel A - — [failed: recorder-stopped]")
        self.assertNotIn("recorder_worker_stopped", result["message"])
        self.settings["recording_schedule"] = {}
        unknown = self._call(error)
        self.assertEqual(unknown["status"], "unavailable")
        self.assertEqual(unknown["message"], "Recording schedule unavailable.")

    def test_redis_failure_outside_window_is_not_rendered_as_current_recording(self):
        self.settings["recording_schedule"]["channels"][self.channel] = normalize_schedule(
            {"monday": []},
        ).to_snapshot()
        self.control.configuration_generation = active_configuration_generation(self.settings)

        class BrokenRedis:
            def ping(self):
                raise OSError("unavailable")

        result = current_recordings(self.settings, self.control, redis_client=BrokenRedis(), now=self.now)
        self.assertEqual(result["message"], "No recordings scheduled now.")
        self.assertEqual(result["channels"][0]["state"], "unknown")
        self.assertEqual(result["channels"][0]["reason"], "redis_unavailable")

    def test_unknown_applied_state_filters_by_current_schedule_and_reports_unknown_schedule(self):
        with patch("catchuparr.recording_overview._channel_metadata", return_value={
            self.channel: {"number": 12, "name": "Synthetic Channel A"},
        }):
            result = current_recordings(
                self.settings, None, redis_client=self.redis, now=self.now,
            )
        self.assertEqual(result["message"], "Synthetic Channel A - — [failed: unknown]")
        self.assertEqual(result["channels"][0]["state"], "unknown")
        self.settings["recording_schedule"] = {}
        unavailable = current_recordings(
            self.settings, None, redis_client=self.redis, now=self.now,
        )
        self.assertEqual(unavailable["status"], "unavailable")
        self.assertEqual(unavailable["message"], "Recording schedule unavailable.")

    def test_bad_timezone_is_unavailable_for_continuous_schedule_in_normal_and_fallback_paths(self):
        self.settings["recording_schedule"]["timezone"] = "Invalid/Zone"
        self.control.configuration_generation = active_configuration_generation(self.settings)
        diagnostic = self._diagnostic(started=self.now.timestamp(), phase="error")
        diagnostic[self.channel]["error_code"] = "recorder_worker_stopped"
        normal = self._call(diagnostic)
        self.assertEqual(normal["status"], "unavailable")
        self.assertEqual(normal["message"], "Recording schedule unavailable.")
        self.assertEqual(normal["channels"][0]["state"], "error")
        self.assertEqual(normal["summary"]["error"], 1)
        fallback = current_recordings(
            self.settings, None, redis_client=self.redis, now=self.now,
        )
        self.assertEqual(fallback["status"], "unavailable")
        self.assertEqual(fallback["message"], "Recording schedule unavailable.")

    def test_active_and_off_window_error_rows_keep_structured_states_and_summary(self):
        off_window = str(uuid.uuid4())
        self.settings["channel_uuids"] += "\n" + off_window
        self.settings["recording_schedule"]["channels"][off_window] = normalize_schedule(
            {"monday": []},
        ).to_snapshot()
        self.control.configuration_generation = active_configuration_generation(self.settings)
        diagnostics = self._diagnostic(started=self.now.timestamp(), phase="error")
        diagnostics[self.channel]["error_code"] = "recorder_worker_stopped"
        diagnostics[off_window] = dict(diagnostics[self.channel])
        with patch("catchuparr.recording_overview._selected_channel_ids",
                   return_value=[self.channel, off_window]), \
             patch("catchuparr.recording_overview._channel_metadata", return_value={
                 self.channel: {"number": 12, "name": "Synthetic Channel A"},
                 off_window: {"number": 13, "name": "Synthetic Channel B"},
             }), patch("catchuparr.recording_overview.read_recorder_diagnostics", return_value=diagnostics), \
             patch("catchuparr.recording_overview.source_metadata", return_value=None):
            result = current_recordings(
                self.settings, self.control, redis_client=self.redis, now=self.now,
            )
        rows = {row["channel_uuid"]: row for row in result["channels"]}
        self.assertEqual(rows[self.channel]["state"], "error")
        self.assertEqual(rows[off_window]["state"], "error")
        self.assertTrue(rows[self.channel]["scheduled_now"])
        self.assertFalse(rows[off_window]["scheduled_now"])
        self.assertEqual(result["summary"]["error"], 2)
        self.assertEqual(result["message"],
                         "Synthetic Channel A - — [failed: recorder-stopped]")

    def test_compact_failure_labels_are_fixed_and_state_specific(self):
        row = {"number": 1, "name": "Synthetic Channel", "source": None,
               "state": "no_media_progress"}
        self.assertEqual(_message_row(row), "Synthetic Channel - — [failed: no-media]")
        row["state"] = "cleanup_pending"
        self.assertEqual(_message_row(row), "Synthetic Channel - — [failed: cleanup-pending]")
        row["state"] = "unknown"
        self.assertEqual(_message_row(row), "Synthetic Channel - — [failed: unknown]")
        row.update(state="error", error_code="unrecognized_private_error")
        self.assertEqual(_message_row(row), "Synthetic Channel - — [failed: unknown]")
        self.assertNotIn("no-tuners", _message_row(row))

    def test_source_display_suppresses_urls_and_credentials(self):
        self.assertEqual("Good provider", _safe_display("Good provider"))
        self.assertIsNone(_safe_display("https://user:secret@example.invalid/feed"))
        self.assertIsNone(_safe_display("Bearer private-token"))


if __name__ == "__main__":
    unittest.main()
