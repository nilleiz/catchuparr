import hashlib
import unittest
from types import SimpleNamespace

from catchuparr.adapters.sharing_native import (
    _CHECK_AND_TOUCH_EPISODE_LUA,
    _PROJECT_EPISODE_LUA,
    _PUBLISH_CURRENT_AUTHORITY_LUA,
    _REGISTER_BORROWED_CLIENT_LUA,
    NativeAssignedSource,
    NativeClientRegistrationAck,
    NativeEpisodeGrant,
    adopt_registered_client_in_manager,
    check_and_touch_borrowed_client,
    observe_native_connection,
    project_opening_episode,
    publish_current_authority,
    read_native_episode_grant,
    register_borrowed_client,
)
from catchuparr.adapters.sharing_reader import redis_backend_signature
from catchuparr.sharing_episode import BorrowEpisodeSnapshot

CHANNEL = "channel-synthetic-7"
IDENTITY = (
    CHANNEL, "worker-synthetic-7", "worker-synthetic-7", "worker-synthetic-7",
    "stream-synthetic-7", "account-synthetic-7", "m3u-profile-synthetic-7",
    "input-profile-synthetic-7",
)
LEASE = "lease-value-synthetic"


class Connection:
    __module__ = "redis.connection"


def episode():
    return BorrowEpisodeSnapshot(
        episode_id="episode-synthetic-7", channel_uuid=CHANNEL, phase="opening",
        configuration_generation="config-synthetic-3", control_generation=7,
        recorder_fence=11, intent_epoch=9, identity=IDENTITY,
        lease_digest=hashlib.sha256(LEASE.encode()).hexdigest(), client_id=None,
        connected_at=None, version=1, last_viewer_left_at=None, return_deadline=None,
    )


class CaptureRedis:
    def __init__(self, result=1):
        self.calls = []
        self.result = result
        self.connection_pool = SimpleNamespace(
            connection_class=Connection,
            connection_kwargs={
                "host": "redis.synthetic.invalid", "port": 6379, "db": 4,
                "decode_responses": False,
            },
        )

    def eval(self, script, count, *args):
        self.calls.append((script, count, args))
        return self.result


class NativeObservationRedis:
    def __init__(self):
        self.strings = {
            f"live:channel:{CHANNEL}:owner": IDENTITY[2],
            f"live:worker:{IDENTITY[1]}:heartbeat": "alive",
            f"live:channel:{CHANNEL}:input:buffer:index": "8",
            f"live:channel:{CHANNEL}:input:buffer:chunk:8": "ts-data",
        }
        self.hashes = {
            f"live:channel:{CHANNEL}:metadata": {
                "owner": IDENTITY[3], "channel_uuid": CHANNEL, "state": "active",
                "stream_id": IDENTITY[4], "m3u_profile": IDENTITY[6],
                "stream_profile": IDENTITY[7],
            },
            f"live:channel:{CHANNEL}:clients:viewer-synthetic-1": {
                "connected_at": "100.0", "worker_id": "viewer-worker-synthetic-1",
            },
        }
        self.sets = {f"live:channel:{CHANNEL}:clients": {"viewer-synthetic-1"}}
        self.zsets = {f"live:channel:{CHANNEL}:input:buffer:chunk_timestamps": [(8, 100.0)]}

    def get(self, key):
        return self.strings.get(key)

    def type(self, key):
        if key in self.strings:
            return "string"
        if key in self.hashes:
            return "hash"
        if key in self.sets:
            return "set"
        if key in self.zsets:
            return "zset"
        return "none"

    def hgetall(self, key):
        return self.hashes.get(key, {})

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def exists(self, key):
        return key in self.strings or key in self.hashes or key in self.sets

    def ttl(self, key):
        return 30 if key in self.strings else -2

    def zrevrange(self, key, start, end):
        items = sorted(self.zsets.get(key, []), key=lambda row: row[1], reverse=True)
        return [str(row[0]) for row in items[start:end + 1]]

    def smembers(self, key):
        return self.sets.get(key, set())


class Manager:
    def __init__(self, redis_client=None, worker_id="task-worker-synthetic"):
        self.channel_id = CHANNEL
        self.worker_id = worker_id
        self.redis_client = redis_client or CaptureRedis()
        self.lock = __import__("threading").RLock()
        self.clients = set()
        self._registered_clients = set()
        self.last_heartbeat_time = {}
        self.last_active_time = 0
        self.add_calls = 0

    def add_client(self, *args, **kwargs):
        self.add_calls += 1
        raise AssertionError("generic add_client must never be used")


class NativeSharingTests(unittest.TestCase):
    def test_observes_existing_connection_even_when_account_is_excluded_for_new_sources(self):
        redis = NativeObservationRedis()
        source = NativeAssignedSource(
            channel_uuid=CHANNEL, stream_id=IDENTITY[4], account_id=IDENTITY[5],
            m3u_profile_id=IDENTITY[6], input_profile_id=IDENTITY[7], assigned=True,
            account_active=True, profile_active=True, media_compatible=True,
        )
        result = observe_native_connection(
            redis, CHANNEL, catalog_generation="catalog-synthetic-4",
            assigned_sources=(source,), observed_at=100.5,
            recording_allowed_account_ids=frozenset(),
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.target.ordinary_client_ids, ("viewer-synthetic-1",))
        self.assertFalse(result.target.account_allowed_by_recording_policy)
        self.assertTrue(result.target.useful_buffer_available)

    def test_exact_recorder_capability_marker_is_excluded_without_user_agent_guessing(self):
        redis = NativeObservationRedis()
        redis.strings.update({
            f"catchuparr:stats:recorder-client:{CHANNEL}:viewer-synthetic-1": f"digest-synthetic|{CHANNEL}",
            "catchuparr:recorder:stats-cap:digest-synthetic": f"{CHANNEL}|lease-synthetic",
            f"catchuparr:recorder:{CHANNEL}": "lease-synthetic",
        })
        source = NativeAssignedSource(
            channel_uuid=CHANNEL, stream_id=IDENTITY[4], account_id=IDENTITY[5],
            m3u_profile_id=IDENTITY[6], input_profile_id=IDENTITY[7], assigned=True,
            account_active=True, profile_active=True, media_compatible=True,
        )
        result = observe_native_connection(
            redis, CHANNEL, catalog_generation="catalog-synthetic-4",
            assigned_sources=(source,), observed_at=101,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.ordinary_viewer_ids, ())

    def test_stale_recorder_capability_marker_fails_closed(self):
        redis = NativeObservationRedis()
        redis.strings[
            f"catchuparr:stats:recorder-client:{CHANNEL}:viewer-synthetic-1"
        ] = f"missing-capability-synthetic|{CHANNEL}"
        source = NativeAssignedSource(
            channel_uuid=CHANNEL, stream_id=IDENTITY[4], account_id=IDENTITY[5],
            m3u_profile_id=IDENTITY[6], input_profile_id=IDENTITY[7], assigned=True,
            account_active=True, profile_active=True, media_compatible=True,
        )
        result = observe_native_connection(
            redis, CHANNEL, catalog_generation="catalog-synthetic-4",
            assigned_sources=(source,), observed_at=101,
        )
        self.assertIsNone(result)

    def test_wrong_type_recorder_capability_markers_fail_closed(self):
        source = NativeAssignedSource(
            channel_uuid=CHANNEL, stream_id=IDENTITY[4], account_id=IDENTITY[5],
            m3u_profile_id=IDENTITY[6], input_profile_id=IDENTITY[7], assigned=True,
            account_active=True, profile_active=True, media_compatible=True,
        )
        marker = f"catchuparr:stats:recorder-client:{CHANNEL}:viewer-synthetic-1"
        cap = "catchuparr:recorder:stats-cap:digest-wrong-type-synthetic"
        lease_key = f"catchuparr:recorder:{CHANNEL}"
        for wrong_key in (marker, cap):
            with self.subTest(wrong_key=wrong_key):
                redis = NativeObservationRedis()
                redis.strings[marker] = f"digest-wrong-type-synthetic|{CHANNEL}"
                redis.strings[lease_key] = "lease-wrong-type-synthetic"
                if wrong_key == marker:
                    redis.strings.pop(marker)
                    redis.hashes[marker] = {"marker": "wrong-type"}
                else:
                    redis.hashes[cap] = {"cap": "wrong-type"}
                result = observe_native_connection(
                    redis, CHANNEL, catalog_generation="catalog-synthetic-4",
                    assigned_sources=(source,), observed_at=101,
                )
                self.assertIsNone(result)

    def test_native_observation_fails_closed_on_profile_or_owner_mismatch(self):
        source = NativeAssignedSource(
            channel_uuid=CHANNEL, stream_id=IDENTITY[4], account_id=IDENTITY[5],
            m3u_profile_id=IDENTITY[6], input_profile_id="wrong-input-profile",
            assigned=True, account_active=True, profile_active=True, media_compatible=True,
        )
        self.assertIsNone(observe_native_connection(
            NativeObservationRedis(), CHANNEL, catalog_generation="catalog-synthetic-4",
            assigned_sources=(source,), observed_at=101,
        ))

    def test_project_uses_exact_native_key_contract_and_rejects_wrong_lease_digest(self):
        redis = CaptureRedis()
        snap = episode()
        self.assertTrue(project_opening_episode(
            redis, snap, lease_value=LEASE, catalog_generation="catalog-synthetic-4",
            return_seconds=30, minimum_switch_seconds=60, last_switch_at=95,
        ))
        script, numkeys, args = redis.calls[0]
        self.assertIs(script, _PROJECT_EPISODE_LUA)
        self.assertEqual(numkeys, 3)
        self.assertEqual(args[:numkeys], (
            f"catchuparr:sharing:current:{CHANNEL}",
            f"catchuparr:recorder:{CHANNEL}",
            f"catchuparr:sharing:episode:{CHANNEL}",
        ))
        values = args[numkeys:]
        self.assertEqual(values[:3], (
            "config-synthetic-3", "7", "catalog-synthetic-4",
        ))
        self.assertEqual(values[3:6], ("11", LEASE, "episode-synthetic-7"))
        self.assertEqual(values[6:13], IDENTITY[1:])
        self.assertEqual(values[-2:], (str(snap.intent_epoch), snap.lease_digest))
        self.assertEqual(values[-3], CHANNEL)
        before = len(redis.calls)
        self.assertFalse(project_opening_episode(
            redis, snap, lease_value="wrong-lease", catalog_generation="catalog-synthetic-4",
            return_seconds=30, minimum_switch_seconds=60, last_switch_at=95,
        ))
        self.assertEqual(len(redis.calls), before)

    def test_current_authority_uses_durable_monotonic_revision_and_exact_key(self):
        redis = CaptureRedis()
        self.assertTrue(publish_current_authority(
            redis, CHANNEL, configuration_generation="config-next",
            control_generation=8, catalog_generation="catalog-next",
            expected_revision=13, authority_revision=14,
        ))
        script, count, args = redis.calls[0]
        self.assertIs(script, _PUBLISH_CURRENT_AUTHORITY_LUA)
        self.assertEqual(count, 1)
        self.assertEqual(args, (
            f"catchuparr:sharing:current:{CHANNEL}", "13", "14",
            "config-next", "8", "catalog-next",
        ))
        with self.assertRaises(ValueError):
            publish_current_authority(
                redis, CHANNEL, configuration_generation="stale-config",
                control_generation=7, catalog_generation="stale-catalog",
                expected_revision=14, authority_revision=14,
            )
        self.assertIn("current ~= expected", script)
        self.assertIn("next_revision <= expected", script)

    def test_register_uses_thirteen_native_keys_and_exact_generation_identity(self):
        redis = CaptureRedis([1, "100.250000", "2"])
        snap = episode()
        ack = register_borrowed_client(
            redis, snap, lease_value=LEASE, catalog_generation="catalog-synthetic-4",
            client_id="catchuparr-client-synthetic-7", local_worker_id="task-worker-synthetic",
            user_agent="Synthetic Player", client_ip="192.0.2.7", client_record_ttl=60,
        )
        self.assertIsInstance(ack, NativeClientRegistrationAck)
        self.assertEqual((ack.connected_at, ack.projection_version), ("100.250000", 2))
        script, numkeys, args = redis.calls[0]
        self.assertIs(script, _REGISTER_BORROWED_CLIENT_LUA)
        keys, values = args[:numkeys], args[numkeys:]
        self.assertEqual(numkeys, 13)
        self.assertEqual(keys, (
            f"catchuparr:sharing:episode:{CHANNEL}",
            f"catchuparr:sharing:current:{CHANNEL}",
            f"catchuparr:recorder:{CHANNEL}",
            f"live:channel:{CHANNEL}:owner",
            f"live:worker:{IDENTITY[1]}:heartbeat",
            f"live:channel:{CHANNEL}:metadata",
            f"live:channel:{CHANNEL}:stopping",
            f"live:channel:{CHANNEL}:client:catchuparr-client-synthetic-7:stop",
            f"live:channel:{CHANNEL}:clients:catchuparr-client-synthetic-7",
            f"live:channel:{CHANNEL}:clients",
            f"catchuparr:sharing:private:{CHANNEL}",
            f"live:channel:{CHANNEL}:input:buffer:index",
            f"live:channel:{CHANNEL}:input:buffer:chunk_timestamps",
        ))
        self.assertEqual(values, (
            "config-synthetic-3", "7", "catalog-synthetic-4", "11", "1", LEASE,
            "episode-synthetic-7", IDENTITY[2], IDENTITY[1], IDENTITY[3], IDENTITY[4],
            IDENTITY[5], IDENTITY[6], IDENTITY[7], CHANNEL,
            "catchuparr-client-synthetic-7", "Synthetic Player", "192.0.2.7",
            "task-worker-synthetic", 60, "9",
        ))

    def test_local_bookkeeping_requires_ack_and_never_calls_generic_add(self):
        redis = CaptureRedis()
        manager = Manager(redis)
        ack = NativeClientRegistrationAck(
            channel_uuid=CHANNEL, episode_id="episode-synthetic-7",
            client_id="catchuparr-client-synthetic-7", local_worker_id="task-worker-synthetic",
            redis_backend_signature=redis_backend_signature(redis), connected_at="100.0",
            projection_version=2,
        )
        self.assertTrue(adopt_registered_client_in_manager(manager, ack))
        self.assertIn(ack.client_id, manager.clients)
        self.assertIn(ack.client_id, manager._registered_clients)
        self.assertGreater(manager.last_heartbeat_time[ack.client_id], 0)
        self.assertEqual(manager.add_calls, 0)
        wrong_worker = Manager(redis, worker_id="other-worker-synthetic")
        self.assertFalse(adopt_registered_client_in_manager(wrong_worker, ack))
        other_backend = CaptureRedis()
        other_backend.connection_pool.connection_kwargs["db"] = 5
        wrong_backend = Manager(other_backend)
        self.assertFalse(adopt_registered_client_in_manager(wrong_backend, ack))

    def test_redis_grant_decoder_is_strict_and_check_uses_native_key_order(self):
        values = {
            "channel_uuid": CHANNEL, "episode_id": "episode-synthetic-7", "phase": "active",
            "configuration_generation": "config-synthetic-3", "control_generation": "7",
            "catalog_generation": "catalog-synthetic-4", "recorder_fence": "11",
            "intent_epoch": "9", "version": "2", "lease_value": LEASE,
            "lease_digest": hashlib.sha256(LEASE.encode()).hexdigest(),
            "worker_id": IDENTITY[1], "owner_lock_value": IDENTITY[2],
            "metadata_owner": IDENTITY[3], "stream_id": IDENTITY[4],
            "account_id": IDENTITY[5], "m3u_profile_id": IDENTITY[6],
            "input_profile_id": IDENTITY[7], "client_id": "catchuparr-client-synthetic-7",
            "client_worker_id": "task-worker-synthetic",
            "connected_at": "100.250000", "return_seconds": "30",
            "minimum_switch_seconds": "60", "last_switch_at": "95",
            "viewer_left_at": "", "return_deadline": "",
        }

        class GrantRedis(CaptureRedis):
            def hgetall(self, key):
                return values

        redis = GrantRedis([1, "", "2"])
        grant = read_native_episode_grant(redis, CHANNEL)
        self.assertIsInstance(grant, NativeEpisodeGrant)
        self.assertEqual(grant.version, 2)
        ok, deadline, version = check_and_touch_borrowed_client(redis, grant)
        self.assertTrue(ok)
        self.assertIsNone(deadline)
        self.assertEqual(version, 2)
        _, count, args = redis.calls[0]
        self.assertEqual(count, 13)
        self.assertEqual(args[:count], (
            f"catchuparr:sharing:episode:{CHANNEL}",
            f"catchuparr:sharing:current:{CHANNEL}",
            f"catchuparr:recorder:{CHANNEL}",
            f"live:channel:{CHANNEL}:owner",
            f"live:worker:{IDENTITY[1]}:heartbeat",
            f"live:channel:{CHANNEL}:metadata",
            f"live:channel:{CHANNEL}:stopping",
            f"live:channel:{CHANNEL}:clients:catchuparr-client-synthetic-7",
            f"live:channel:{CHANNEL}:clients",
            f"catchuparr:sharing:private:{CHANNEL}",
            f"live:channel:{CHANNEL}:client:catchuparr-client-synthetic-7:stop",
            f"live:channel:{CHANNEL}:input:buffer:index",
            f"live:channel:{CHANNEL}:input:buffer:chunk_timestamps",
        ))
        self.assertEqual(args[count:], (
            "config-synthetic-3", "7", "catalog-synthetic-4", "11", LEASE,
            IDENTITY[2], "episode-synthetic-7", "2", "9", CHANNEL,
            IDENTITY[3], IDENTITY[1], IDENTITY[4], IDENTITY[5], IDENTITY[6],
            IDENTITY[7], "catchuparr-client-synthetic-7", "100.250000",
            "task-worker-synthetic", 60,
        ))
        values["return_deadline"] = "nan"
        self.assertIsNone(read_native_episode_grant(redis, CHANNEL))

    def test_lua_preflights_types_and_viewer_evidence_before_any_write(self):
        for script in (_PROJECT_EPISODE_LUA, _REGISTER_BORROWED_CLIENT_LUA, _CHECK_AND_TOUCH_EPISODE_LUA):
            self.assertIn("redis.call('TYPE'", script)
            self.assertLess(script.index("redis.call('TYPE'"), script.index("redis.call('HSET'"))
        registration = _REGISTER_BORROWED_CLIENT_LUA
        self.assertIn("redis.call('SISMEMBER', KEYS[10]", registration)
        self.assertIn("connected_at", registration)
        self.assertIn("worker_id", registration)
        self.assertIn("client:", registration)
        self.assertIn("client_id", registration)
        self.assertIn("TTL', KEYS[5]", registration)
        self.assertIn("redis.call('EXISTS', KEYS[9]) ~= 0", registration)
        self.assertNotIn("user_agent", registration.split("local function private_client", 1)[1].split("local expected", 1)[0])
        activity = _CHECK_AND_TOUCH_EPISODE_LUA
        self.assertIn("redis.call('HGET', KEYS[2], 'catalog_generation')", activity)
        self.assertIn("redis.call('GET', KEYS[3]) ~= ARGV[5]", activity)
        self.assertIn("redis.call('EXISTS', KEYS[11]) ~= 0", activity)
        self.assertLess(activity.index("now >= deadline"), activity.index("if ordinary > 0 then"))
        self.assertLess(activity.index("redis.call('SMEMBERS', KEYS[9])"), activity.index("redis.call('HSET', KEYS[8]"))


if __name__ == "__main__":
    unittest.main()
