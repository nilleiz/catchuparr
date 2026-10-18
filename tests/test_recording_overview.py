import sqlite3
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr.configuration import active_configuration_generation
from catchuparr.recording_overview import _safe_display, current_recordings
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

    def test_cleanup_and_error_states_are_not_hidden_by_schedule(self):
        self.settings["recording_schedule"]["channels"][self.channel] = normalize_schedule({
            "monday": [],
        }).to_snapshot()
        self.control.configuration_generation = active_configuration_generation(self.settings)
        cleanup = self._call(self._diagnostic(started=self.now.timestamp(), phase="cleanup_pending"))
        self.assertEqual(cleanup["channels"][0]["state"], "cleanup_pending")
        error = self._diagnostic(started=self.now.timestamp(), phase="error")
        error[self.channel]["error_code"] = "recorder_worker_stopped"
        failed = self._call(error)
        self.assertEqual(failed["channels"][0]["state"], "error")

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

    def test_message_includes_safe_source_progress_and_problem_count(self):
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
        self.assertIn("Problems: 0", result["message"])
        self.assertIn("Synthetic Stream A (id 17)", result["message"])
        self.assertIn("provider Synthetic Provider (id 4)", result["message"])
        self.assertIn("Stream profile Synthetic Stream Profile (id 4)", result["message"])
        self.assertIn("provider profile Synthetic Provider Profile (id 9)", result["message"])
        self.assertIn("Last confirmed progress: none", result["message"])

    def test_source_display_suppresses_urls_and_credentials(self):
        self.assertEqual("Good provider", _safe_display("Good provider"))
        self.assertIsNone(_safe_display("https://user:secret@example.invalid/feed"))
        self.assertIsNone(_safe_display("Bearer private-token"))


if __name__ == "__main__":
    unittest.main()
