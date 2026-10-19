import hashlib
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr.configuration import active_configuration_generation
from catchuparr.recorder_diagnostics import (
    _PUBLISH_BORROWED_LUA,
    _SAFE_ID,
    _SAFE_NATIVE_OWNER,
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
        self.worker_id = None
        self.native_owner = "native.owner.synthetic:123"

    def hgetall(self, key):
        if self.worker_id and key.endswith(self.worker_id):
            return {b"profile_id": b"5", b"account_id": b"7"}
        if ":metadata" in key:
            return {
                b"owner": self.native_owner.encode(), b"stream_id": b"31",
                b"m3u_profile": b"5", b"stream_profile": b"4", b"state": b"active",
            }
        return {}

    def get(self, key):
        return self.native_owner if key.endswith(":owner") else None

    def ttl(self, _key):
        return 30

    def exists(self, _key):
        return 0

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
        self.redis.worker_id = self.worker_id

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
        self.assertEqual(key_count, 8)
        self.assertTrue(args[1].endswith(self.worker_id))
        self.assertEqual(args[-6:], (
            "recording", "dedicated", "", "5", "4", self.redis.native_owner,
        ))
        self.assertEqual(args[5], f"live:worker:{self.redis.native_owner}:heartbeat")

    def test_borrowed_write_checks_current_authority_and_exact_grant(self):
        self.assertIsNone(_SAFE_ID.fullmatch("native:worker.123"))
        self.assertIsNotNone(_SAFE_NATIVE_OWNER.fullmatch("native:worker.123"))
        result = publish_borrowed_recorder_diagnostic(
            self.redis, self.channel, "8:synthetic-owner", episode_id="episode_9",
            client_id="catchuparr-client-9",
            worker_id="native:worker.123", owner_lock_value="native:lock.123",
            metadata_owner="native:owner.123", stream_id="31", account_id="7",
            m3u_profile_id="5", input_profile_id="4", catalog_generation=self.catalog,
            phase="starting", started_at=1000, config_generation=self.generation,
            control_generation=3, fence=8, now=1010,
        )
        self.assertTrue(result)
        script, key_count, args = self.redis.calls[0]
        self.assertIs(script, _PUBLISH_BORROWED_LUA)
        self.assertEqual(key_count, 13)
        self.assertIn("authority_revision", script)
        self.assertIn("m3u_profile_id", script)
        self.assertEqual(args[-7:], (
            "starting", "", str(LAST_ERROR_TTL_SECONDS), str(LIVE_TTL_CAP_SECONDS),
            "catchuparr-client-9", "native:lock.123", "native:owner.123",
        ))
        self.assertTrue(args[10].endswith(":native:owner.123:heartbeat"))
        self.assertEqual(args[16], "native:worker.123")

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
            owner_lock_value="native-owner", metadata_owner="native-owner",
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
        self.alive = True
        self.native_metadata = {
            "owner": self.live.get("worker_id"),
            "stream_id": self.live.get("stream_id"),
            "m3u_profile": self.live.get("m3u_profile_id"),
            "stream_profile": self.live.get("input_profile_id"),
            "state": "active",
        }

    def hgetall(self, key):
        if ":last-error:" in key:
            return self.error
        if ":metadata" in key:
            return self.native_metadata
        return self.live

    def ttl(self, key):
        if ":last-error:" in key:
            return self.error_ttl
        if key.endswith(":owner") or key.endswith(":heartbeat"):
            return 20 if self.alive else -2
        return self.live_ttl

    def get(self, key):
        if key.endswith(":owner"):
            return self.live.get("worker_id")
        return self.lease

    def exists(self, key):
        return int(key.endswith(":heartbeat") and self.alive)


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
            "profile_id": "9",
        }
        self.live = {
            "channel_uuid": self.channel, "worker_id": self.worker_id,
            "attempt_digest": "e" * 64, "stream_id": "31", "account_id": "7",
            "config_generation": generation, "control_generation": "6", "fence": "5",
            "phase": "recording", "source": "dedicated", "started_at": "1000",
            "updated_at": "1010", "error_code": "",
            "m3u_profile_id": "9", "input_profile_id": "4",
        }
        self.redis_native_metadata = {
            "owner": self.worker_id, "stream_id": "31", "m3u_profile": "9",
            "stream_profile": "4", "state": "active",
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

    def test_dedicated_binding_requires_native_owner_profile_and_heartbeat(self):
        redis = DiagnosticReaderRedis(live=self.live)
        redis.native_metadata = {
            "owner": self.worker_id, "stream_id": "31", "m3u_profile": "9",
            "stream_profile": "4", "state": "active",
        }
        redis.native_alive = True
        redis.hgetall = lambda key: (
            redis.error if ":last-error:" in key else
            redis.native_metadata if ":metadata" in key else redis.live
        )
        redis.get = lambda key: self.worker_id if key.endswith(":owner") else redis.lease
        redis.exists = lambda key: int(key.endswith(":heartbeat") and redis.native_alive)
        redis.ttl = lambda key: (
            redis.error_ttl if ":last-error:" in key else
            20 if key.endswith(":owner") or key.endswith(":heartbeat") else redis.live_ttl
        )
        self.assertIn(self.channel, self._read(redis))
        redis.native_alive = False
        self.assertNotIn(self.channel, self._read(redis))
        redis.native_alive = True
        redis.native_metadata["stream_profile"] = "stale"
        self.assertNotIn(self.channel, self._read(redis))

    def test_borrowed_binding_requires_live_native_owner_and_metadata(self):
        episode_id = "episode_9"
        native_id = self.channel
        native_owner = "native-owner"
        client_id = "catchuparr-client-9"
        grant = SimpleNamespace(
            episode_id=episode_id, worker_id=native_id, stream_id="31", account_id="7",
            m3u_profile_id="9", input_profile_id="4", catalog_generation="b" * 64,
            configuration_generation=self.control.configuration_generation,
            control_generation=6, recorder_fence=5, client_id=client_id,
            client_worker_id=native_id, connected_at="1000", lease_value="5:owner",
            owner_lock_value=native_owner, metadata_owner=native_owner,
        )
        diagnostic = dict(
            self.live, source="borrowed", m3u_profile_id="9", input_profile_id="4",
            episode_digest=hashlib.sha256(episode_id.encode()).hexdigest(),
            worker_digest=hashlib.sha256(native_id.encode()).hexdigest(),
            catalog_generation="b" * 64,
        )
        redis = DiagnosticReaderRedis(live=diagnostic)
        redis.alive = True
        redis.hgetall = lambda key: (
            diagnostic if ":diagnostic:" in key else
            {"configuration_generation": self.control.configuration_generation,
             "control_generation": "6", "catalog_generation": "b" * 64,
             "authority_revision": "3"} if ":sharing:current:" in key else
            {client_id: episode_id} if ":sharing:private:" in key else
            {"worker_id": native_id, "connected_at": "1000"} if f":clients:{client_id}" in key else
            {"owner": native_owner, "stream_id": "31", "m3u_profile": "9",
             "stream_profile": "4", "state": "active"} if ":metadata" in key else {}
        )
        redis.get = lambda key: native_owner if key.endswith(":owner") else redis.lease
        redis.sismember = lambda _key, member: member == client_id
        owner_heartbeat = f"live:worker:{native_owner}:heartbeat"
        channel_heartbeat = f"live:worker:{native_id}:heartbeat"
        redis.owner_heartbeat_exists = True
        redis.channel_heartbeat_exists = True
        redis.owner_heartbeat_ttl = 20
        redis.exists = lambda key: int(
            (key == owner_heartbeat and redis.owner_heartbeat_exists)
            or (key == channel_heartbeat and redis.channel_heartbeat_exists)
        )
        redis.ttl = lambda key: (
            redis.owner_heartbeat_ttl if key == owner_heartbeat else
            20 if key == channel_heartbeat else
            20 if not key.endswith(":last-error:") else 3600
        )
        with patch("catchuparr.adapters.sharing_native.read_native_episode_grant", return_value=grant):
            self.assertIn(self.channel, read_recorder_diagnostics(
                redis, self.settings, self.control, now=1010,
            ))
            redis.owner_heartbeat_exists = False
            # A live channel UUID heartbeat cannot substitute for the owner process.
            self.assertNotIn(self.channel, read_recorder_diagnostics(
                redis, self.settings, self.control, now=1010,
            ))
            redis.owner_heartbeat_exists = True
            redis.owner_heartbeat_ttl = -2
            self.assertNotIn(self.channel, read_recorder_diagnostics(
                redis, self.settings, self.control, now=1010,
            ))


if __name__ == "__main__":
    unittest.main()
