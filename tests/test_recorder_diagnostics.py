import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr.configuration import active_configuration_generation
from catchuparr.recorder_diagnostics import (
    _PUBLISH_BORROWED_LUA,
    ERROR_CODES,
    LAST_ERROR_TTL_SECONDS,
    LIVE_TTL_CAP_SECONDS,
    publish_borrowed_recorder_diagnostic,
    publish_recorder_diagnostic,
    read_recorder_diagnostics,
)


class RecordingRedis:
    def __init__(self):
        self.calls = []

    def eval(self, script, key_count, *args):
        self.calls.append((script, key_count, args))
        return 1


class RecorderDiagnosticWriterTests(unittest.TestCase):
    def setUp(self):
        self.redis = RecordingRedis()
        self.channel = str(uuid.uuid4())
        self.generation = "a" * 64
        self.catalog = "b" * 64
        self.worker_id = "catchuparr-r" + "c" * 40

    def test_dedicated_write_uses_atomic_worker_and_lease_cas(self):
        with patch("catchuparr.adapters.recorder_proxy.is_managed_worker_id", return_value=True):
            result = publish_recorder_diagnostic(
                self.redis, self.channel, "8:synthetic-owner", worker_id=self.worker_id,
                phase="recording", source="dedicated", stream_id="31", account_id="7",
                started_at=1000, config_generation=self.generation,
                control_generation=3, fence=8, now=1010,
            )
        self.assertTrue(result)
        script, key_count, args = self.redis.calls[0]
        self.assertIn("HGET", script)
        self.assertIn("TTL", script)
        self.assertLess(script.index("TYPE"), script.index("HSET"))
        self.assertEqual(key_count, 4)
        self.assertTrue(args[1].endswith(self.worker_id))
        self.assertEqual(args[-3:], ("recording", "dedicated", ""))

    def test_borrowed_write_checks_current_authority_and_exact_grant(self):
        result = publish_borrowed_recorder_diagnostic(
            self.redis, self.channel, "8:synthetic-owner", episode_id="episode_9",
            client_id="catchuparr-client-9",
            worker_id="nativeworker", stream_id="31", account_id="7",
            m3u_profile_id="5", input_profile_id="4", catalog_generation=self.catalog,
            phase="starting", started_at=1000, config_generation=self.generation,
            control_generation=3, fence=8, now=1010,
        )
        self.assertTrue(result)
        script, key_count, args = self.redis.calls[0]
        self.assertIs(script, _PUBLISH_BORROWED_LUA)
        self.assertEqual(key_count, 9)
        self.assertIn("authority_revision", script)
        self.assertIn("m3u_profile_id", script)
        self.assertEqual(args[-5:], (
            "starting", "", str(LAST_ERROR_TTL_SECONDS), str(LIVE_TTL_CAP_SECONDS),
            "catchuparr-client-9",
        ))

    def test_invalid_inputs_never_call_redis(self):
        with patch("catchuparr.adapters.recorder_proxy.is_managed_worker_id", return_value=True):
            self.assertFalse(publish_recorder_diagnostic(
                self.redis, self.channel, "lease", worker_id=self.worker_id,
                phase="active", source="dedicated", stream_id="31", account_id="7",
                started_at=1000, config_generation=self.generation,
                control_generation=3, fence=8,
            ))
            self.assertFalse(publish_recorder_diagnostic(
                self.redis, self.channel, "lease", worker_id=self.worker_id,
                phase="error", source="dedicated", stream_id="31", account_id="7",
                started_at=1000, config_generation=self.generation,
                control_generation=3, fence=8, error_code="raw exception text",
            ))
        self.assertFalse(publish_borrowed_recorder_diagnostic(
            self.redis, self.channel, "lease", episode_id="episode",
            client_id="catchuparr-client-9", worker_id="nativeworker",
            stream_id="31", account_id="7", m3u_profile_id="5", input_profile_id="4",
            catalog_generation=self.catalog, phase="error", started_at=1000,
            config_generation=self.generation, control_generation=3, fence=8,
            error_code="raw exception text",
        ))
        self.assertEqual(self.redis.calls, [])

    def test_error_codes_are_a_fixed_allowlist(self):
        self.assertIn("sharing_recovery_failed", ERROR_CODES)
        self.assertIn("sharing_recovery_stale", ERROR_CODES)
        self.assertNotIn("unknown", ERROR_CODES)


class DiagnosticReaderRedis:
    def __init__(self, live=None, error=None, lease="5:owner", live_ttl=20, error_ttl=3600):
        self.live = live or {}
        self.error = error or {}
        self.lease = lease
        self.live_ttl = live_ttl
        self.error_ttl = error_ttl

    def hgetall(self, key):
        return self.error if ":last-error:" in key else self.live

    def ttl(self, key):
        return self.error_ttl if ":last-error:" in key else self.live_ttl

    def get(self, _key):
        return self.lease


class RecorderDiagnosticReaderTests(unittest.TestCase):
    def setUp(self):
        self.channel = str(uuid.uuid4())
        self.worker_id = "catchuparr-r" + "d" * 40
        self.settings = {
            "version": 7, "channel_uuids": self.channel,
            "archive_root": "/tmp/synthetic-archive",
            "recording_schedule": {"timezone": "UTC", "channels": {}},
        }
        generation = active_configuration_generation(self.settings)
        self.control = SimpleNamespace(
            paused=False, generation=6, configuration_generation=generation,
        )
        self.worker = {
            "worker_id": self.worker_id, "channel_uuid": self.channel,
            "stream_id": "31", "account_id": "7", "config_generation": generation,
            "control_generation": "6", "lease_fence": "5",
            "lease_key": f"catchuparr:recorder:{self.channel}", "lease_value": "5:owner",
        }
        self.live = {
            "channel_uuid": self.channel, "worker_id": self.worker_id,
            "attempt_digest": "e" * 64, "stream_id": "31", "account_id": "7",
            "config_generation": generation, "control_generation": "6", "fence": "5",
            "phase": "recording", "source": "dedicated", "started_at": "1000",
            "updated_at": "1010", "error_code": "",
        }

    def _read(self, redis):
        with patch("catchuparr.adapters.recorder_proxy.read_worker_record", return_value=self.worker):
            return read_recorder_diagnostics(redis, self.settings, self.control, now=1010)

    def test_exact_worker_and_live_lease_are_required(self):
        redis = DiagnosticReaderRedis(live=self.live)
        self.assertIn(self.channel, self._read(redis))
        redis.lease = "6:replacement-owner"
        self.assertNotIn(self.channel, self._read(redis))

    def test_generation_and_worker_source_mismatch_fail_closed(self):
        redis = DiagnosticReaderRedis(live=self.live)
        self.worker["stream_id"] = "32"
        self.assertNotIn(self.channel, self._read(redis))
        self.worker["stream_id"] = "31"
        self.live["config_generation"] = "f" * 64
        self.assertNotIn(self.channel, self._read(redis))

    def test_last_error_is_bounded_to_one_day_and_current_generations(self):
        last = dict(self.live, phase="error", error_code="recorder_worker_stopped")
        redis = DiagnosticReaderRedis(live_ttl=-2, error=last, error_ttl=LAST_ERROR_TTL_SECONDS)
        self.assertEqual(self._read(redis)[self.channel]["phase"], "error")
        redis.error_ttl = LAST_ERROR_TTL_SECONDS + 1
        self.assertNotIn(self.channel, self._read(redis))
        redis.error_ttl = 100
        redis.error["control_generation"] = "5"
        self.assertNotIn(self.channel, self._read(redis))


if __name__ == "__main__":
    unittest.main()
