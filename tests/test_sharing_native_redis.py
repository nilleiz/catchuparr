"""Faithful Redis/Lua tests for the unwired sharing native adapter.

Set REDIS_CLI_DOCKER_CONTAINER to the name of the task-owned, isolated Redis
container. Commands run via ``sudo -n docker exec``; no host Redis port or
credentials are used. Tests use synthetic-only keys and flush logical DB 0.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr import recorder_proxy
from catchuparr.adapters.recorder_proxy import worker_id_key
from catchuparr.adapters.sharing_borrow_route import (
    consume_opening_capability,
    sign_borrow_capability,
)
from catchuparr.adapters.sharing_native import (
    check_and_touch_borrowed_client,
    cleanup_borrowed_client,
    close_fenced_unregistered_opening,
    fence_unregistered_opening,
    invalidate_native_episode,
    native_registration_is_absent,
    project_opening_episode,
    publish_current_authority,
    read_native_episode_grant,
    register_borrowed_client,
)
from catchuparr.sharing_episode import BorrowEpisodeSnapshot

CHANNEL = "channel-synthetic-lua-060"
WORKER = "worker-synthetic-lua-060"
STREAM = "stream-synthetic-lua-060"
ACCOUNT = "account-synthetic-lua-060"
M3U_PROFILE = "m3u-profile-synthetic-lua-060"
INPUT_PROFILE = "input-profile-synthetic-lua-060"
LEASE = "synthetic-only-lease-lua-060"
VIEWER = "viewer-synthetic-lua-060"
CLIENT = "catchuparr-client-synthetic-lua-060"
TASK_WORKER = "task-worker-synthetic-lua-060"
CATALOG = "catalog-synthetic-lua-060"
CONFIG = "config-synthetic-lua-060"
EPISODE_ID = "episode-synthetic-lua-060"
CAPABILITY_SECRET = b"synthetic-capability-secret-060-at-least-32-bytes"


class Connection:
    __module__ = "redis.connection"


class RedisCli:
    """Small redis-py-shaped facade backed by redis-cli in the test container."""

    def __init__(self, container: str):
        self.container = container
        self.connection_pool = SimpleNamespace(
            connection_class=Connection,
            connection_kwargs={
                "host": "redis-isolated.synthetic.invalid", "port": 6379,
                "db": 0, "decode_responses": False,
            },
        )

    def command(self, *args: object) -> list[str]:
        result = subprocess.run(
            ["sudo", "-n", "docker", "exec", self.container,
             "redis-cli", "--raw", *(str(arg) for arg in args)],
            check=False, capture_output=True, text=True, timeout=10,
        )
        if result.returncode:
            raise RuntimeError("isolated redis-cli command failed")
        return result.stdout.rstrip("\n").splitlines() if result.stdout else []

    def eval(self, script: str, numkeys: int, *args: object):
        self.last_invocation = (numkeys, len(args), script[-100:])
        result = subprocess.run(
            ["sudo", "-n", "docker", "exec", self.container,
             "redis-cli", "--json", "EVAL", script, str(numkeys),
             *(str(arg) for arg in args)],
            check=False, capture_output=True, text=True, timeout=10,
        )
        if result.returncode:
            raise RuntimeError("isolated redis-cli EVAL failed")
        try:
            output = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise AssertionError("Redis returned a malformed JSON Lua result") from exc
        self.last_eval = output
        if isinstance(output, str) and output.startswith("ERR "):
            raise AssertionError(f"Redis rejected Lua script: {output}")
        return output

    def get(self, key: str):
        values = self.command("GET", key)
        return values[0] if values else None

    def hget(self, key: str, field: str):
        values = self.command("HGET", key, field)
        return values[0] if values else None

    def hgetall(self, key: str):
        # JSON preserves empty hash values. `redis-cli --raw` prints an empty
        # final field as a blank line, which loses output_profile_id=''
        # during lost-registration-ACK reconciliation.
        result = subprocess.run(
            ["sudo", "-n", "docker", "exec", self.container,
             "redis-cli", "--json", "HGETALL", key],
            check=False, capture_output=True, text=True, timeout=10,
        )
        if result.returncode:
            raise RuntimeError("isolated redis-cli command failed")
        return _decode_hgetall_json(result.stdout)

    def type(self, key: str):
        """Match redis-py's TYPE method used by exact absence reconciliation."""
        values = self.command("TYPE", key)
        if not values:
            raise AssertionError("Redis TYPE returned no value")
        return values[0]

    def sismember(self, key: str, member: str):
        values = self.command("SISMEMBER", key, member)
        if values not in (["0"], ["1"]):
            raise AssertionError("Redis SISMEMBER returned an invalid value")
        return values == ["1"]

    def smembers(self, key: str):
        return set(self.command("SMEMBERS", key))

    def exists(self, key: str):
        return bool(int(self.command("EXISTS", key)[0]))

    def ttl(self, key: str):
        return int(self.command("TTL", key)[0])

    def scan_iter(self, *, match: str):
        cursor = "0"
        while True:
            values = self.command("SCAN", cursor, "MATCH", match, "COUNT", "100")
            if not values:
                raise AssertionError("Redis SCAN returned no cursor")
            cursor = values[0]
            yield from values[1:]
            if cursor == "0":
                return


def _decode_hgetall_json(payload: str) -> dict[str, str]:
    """Decode Redis 7 JSON objects without losing empty hash values."""
    values = json.loads(payload)
    if isinstance(values, dict):
        if any(not isinstance(key, str) or not isinstance(value, str)
               for key, value in values.items()):
            raise AssertionError("Redis returned malformed HGETALL object")
        return values
    # Keep compatibility with redis-cli builds that represent RESP arrays as
    # flat JSON arrays. Unlike --raw, both forms preserve empty strings.
    if (isinstance(values, list) and len(values) % 2 == 0
            and all(isinstance(value, str) for value in values)):
        return dict(zip(values[::2], values[1::2]))
    raise AssertionError("Redis returned malformed HGETALL JSON")


class RedisCliJsonTests(unittest.TestCase):
    def test_prior_observation_distinguishes_absent_exact_revoked_and_unknown(self):
        from dataclasses import asdict

        from catchuparr.adapters.sharing_native import (
            NativeEpisodeGrant,
            observe_native_episode_for_authority_drain,
        )

        lease_value = "synthetic-outbox-observation-lease"
        grant = NativeEpisodeGrant(
            channel_uuid=CHANNEL, episode_id="episode-observation-synthetic",
            phase="active", configuration_generation=CONFIG,
            control_generation=7, catalog_generation=CATALOG,
            recorder_fence=11, intent_epoch=4, version=2,
            lease_value=lease_value, worker_id=WORKER,
            owner_lock_value="owner-observation-synthetic",
            metadata_owner="owner-observation-synthetic", stream_id=STREAM,
            account_id=ACCOUNT, m3u_profile_id=M3U_PROFILE,
            input_profile_id=INPUT_PROFILE, client_id=CLIENT,
            client_worker_id=TASK_WORKER, connected_at="100.5",
            return_seconds=30, minimum_switch_seconds=60, last_switch_at=95.0,
            last_viewer_left_at=None, return_deadline=None,
        )
        class Redis:
            kind = "none"
            values = {}

            def type(self, _key):
                return self.kind

            def hgetall(self, _key):
                return dict(self.values)

        redis = Redis()
        self.assertEqual(
            "absent", observe_native_episode_for_authority_drain(redis, CHANNEL).state,
        )
        redis.kind = "string"
        self.assertEqual(
            "unknown", observe_native_episode_for_authority_drain(redis, CHANNEL).state,
        )
        redis.kind = "hash"
        redis.values = {"channel_uuid": CHANNEL}
        self.assertEqual(
            "unknown", observe_native_episode_for_authority_drain(redis, CHANNEL).state,
        )
        raw_grant = asdict(grant)
        raw_grant["last_viewer_left_at"] = ""
        raw_grant["return_deadline"] = ""
        values = {name: str(value) for name, value in raw_grant.items()}
        values["lease_digest"] = hashlib.sha256(lease_value.encode()).hexdigest()
        redis.values = values
        exact = observe_native_episode_for_authority_drain(redis, CHANNEL)
        self.assertEqual("exact", exact.state)
        self.assertEqual(grant.episode_id, exact.value.episode_id)
        redis.values = {**values, "phase": "closing"}
        self.assertEqual(
            "known_revoked", observe_native_episode_for_authority_drain(redis, CHANNEL).state,
        )
        redis.values = {**values, "phase": "active", "lease_digest": "0" * 64}
        self.assertEqual(
            "unknown", observe_native_episode_for_authority_drain(redis, CHANNEL).state,
        )

    def test_type_facade_matches_redis_py_value_shape(self):
        cli = RedisCli("synthetic-test-container")
        with patch(
            "tests.test_sharing_native_redis.subprocess.run",
            return_value=SimpleNamespace(returncode=0, stdout="hash\n"),
        ) as run:
            self.assertEqual(cli.type("key-synthetic"), "hash")
        self.assertEqual(run.call_args.args[0][-2:], ["TYPE", "key-synthetic"])

    def test_sismember_facade_matches_redis_py_boolean_shape(self):
        cli = RedisCli("synthetic-test-container")
        with patch(
            "tests.test_sharing_native_redis.subprocess.run",
            return_value=SimpleNamespace(returncode=0, stdout="1\n"),
        ) as run:
            self.assertTrue(cli.sismember("set-synthetic", "client-synthetic"))
        self.assertEqual(
            run.call_args.args[0][-3:],
            ["SISMEMBER", "set-synthetic", "client-synthetic"],
        )

    def test_hgetall_json_object_preserves_empty_native_profile_field(self):
        self.assertEqual(
            _decode_hgetall_json('{"output_profile_id":"","worker_id":"worker-synthetic"}'),
            {"output_profile_id": "", "worker_id": "worker-synthetic"},
        )

    def test_hgetall_json_flat_array_preserves_empty_native_profile_field(self):
        self.assertEqual(
            _decode_hgetall_json('["output_profile_id","","worker_id","worker-synthetic"]'),
            {"output_profile_id": "", "worker_id": "worker-synthetic"},
        )

    def test_hgetall_json_rejects_null_or_wrong_shape(self):
        for payload in ('{"output_profile_id":null}', '["field"]', '1'):
            with self.subTest(payload=payload), self.assertRaises(AssertionError):
                _decode_hgetall_json(payload)


def _episode(*, epoch: int = 9, client_id: str = CLIENT) -> BorrowEpisodeSnapshot:
    identity = (
        CHANNEL, WORKER, WORKER, WORKER, STREAM, ACCOUNT, M3U_PROFILE,
        INPUT_PROFILE,
    )
    return BorrowEpisodeSnapshot(
        episode_id=EPISODE_ID, channel_uuid=CHANNEL, phase="opening",
        configuration_generation=CONFIG, control_generation=7,
        recorder_fence=11, intent_epoch=epoch, identity=identity,
        lease_digest=hashlib.sha256(LEASE.encode()).hexdigest(), client_id=client_id,
        connected_at=None, version=1, last_viewer_left_at=None,
        return_deadline=None,
    )


def _keys():
    return {
        "current": f"catchuparr:sharing:current:{CHANNEL}",
        "episode": f"catchuparr:sharing:episode:{CHANNEL}",
        "lease": f"catchuparr:recorder:{CHANNEL}",
        "owner": f"live:channel:{CHANNEL}:owner",
        "heartbeat": f"live:worker:{WORKER}:heartbeat",
        "metadata": f"live:channel:{CHANNEL}:metadata",
        "clients": f"live:channel:{CHANNEL}:clients",
        "viewer": f"live:channel:{CHANNEL}:clients:{VIEWER}",
        "buffer_index": f"live:channel:{CHANNEL}:input:buffer:index",
        "timestamps": f"live:channel:{CHANNEL}:input:buffer:chunk_timestamps",
        "chunk": f"live:channel:{CHANNEL}:input:buffer:chunk:1",
        "private": f"catchuparr:sharing:private:{CHANNEL}",
        "client": f"live:channel:{CHANNEL}:clients:{CLIENT}",
    }


@unittest.skipUnless(os.environ.get("REDIS_CLI_DOCKER_CONTAINER"),
                     "requires the isolated task-owned Redis test container")
class NativeLuaRedisTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.redis = RedisCli(os.environ["REDIS_CLI_DOCKER_CONTAINER"])

    def setUp(self):
        self.redis.command("FLUSHDB")

    def _base_authority(self, *, revision: int = 1):
        keys = _keys()
        self.redis.command(
            "HSET", keys["current"], "configuration_generation", CONFIG,
            "control_generation", "7", "catalog_generation", CATALOG,
            "authority_revision", revision,
        )
        self.redis.command("SET", keys["lease"], LEASE, "EX", 600)

    def test_recorder_operation_issue_open_revoke_and_reap_are_one_fenced_flow(self):
        channel = "channel-synthetic-operation-060"
        lease_key = f"catchuparr:recorder:{channel}"
        current_key = f"catchuparr:sharing:current:{channel}"
        config_generation = "config-operation-synthetic-060"
        catalog_generation = "catalog-operation-synthetic-060"
        self.redis.command(
            "HSET", current_key,
            "configuration_generation", config_generation,
            "control_generation", "7",
            "catalog_generation", catalog_generation,
            "authority_revision", "3",
        )
        lease_value = "11:lease-owner-synthetic-060"
        self.redis.command("SET", lease_key, lease_value, "EX", 600)
        lease = SimpleNamespace(fence=11, _value=lease_value, key=lease_key)
        django = type(sys)("django")
        django.__path__ = []
        django_conf = type(sys)("django.conf")
        django_conf.settings = SimpleNamespace(SECRET_KEY="synthetic-operation-signing-key")
        with patch.dict(sys.modules, {"django": django, "django.conf": django_conf}), patch(
            "catchuparr.configuration._sharing_catalog_generation",
            return_value=catalog_generation,
        ):
            authority = recorder_proxy.current_sharing_authority_context(
                self.redis, channel, config_generation=config_generation,
                control_generation=7,
            )
            self.assertEqual(3, authority["authority_revision"])
            attempt = recorder_proxy.issue_recorder_attempt(
                self.redis, lease, channel_uuid=channel,
                candidate={"id": "stream-operation-synthetic", "account_id": "account-operation-synthetic"},
                config_generation=config_generation, control_generation=7,
                internal_base_url="http://dispatcharr.invalid", sharing_authority=authority,
            )
            binding = recorder_proxy.verify_recorder_capability(self.redis, attempt.capability)
            self.assertIsNotNone(binding)
            opened = recorder_proxy.begin_sharing_operation_open(self.redis, binding)
            self.assertIsNotNone(opened)
            self.assertIsNone(recorder_proxy.begin_sharing_operation_open(self.redis, binding))
            self.assertTrue(recorder_proxy.activate_sharing_operation_open(
                self.redis, binding, *opened,
            ))
            self.assertTrue(recorder_proxy.end_sharing_operation_open(
                self.redis, binding, *opened,
            ))
            # Leave a second exact slot active to exercise restart/reaper
            # reconciliation after its HTTP close acknowledgment is lost.
            second_open = recorder_proxy.begin_sharing_operation_open(self.redis, binding)
            self.assertIsNotNone(second_open)
            self.assertTrue(recorder_proxy.activate_sharing_operation_open(
                self.redis, binding, *second_open,
            ))
            self.assertTrue(attempt.revoke(self.redis))
            self.redis.command(
                "HSET", worker_id_key(attempt.worker_id), "state", "released",
            )
            self.assertTrue(attempt.acknowledge_reaped(self.redis))
            self.assertEqual("0", self.redis.hget(
                recorder_proxy._sharing_operation_key(channel, attempt.sharing_operation_id),
                "open_count",
            ))
            self.assertIsNone(self.redis.get(
                recorder_proxy._sharing_operation_current_key(channel)
            ))
            self.assertEqual("closed", self.redis.hget(
                recorder_proxy._sharing_operation_key(channel, attempt.sharing_operation_id),
                "status",
            ))

            # A control change after the authority snapshot cannot issue the next
            # source attempt with the stale generation.
            self.redis.command("HSET", current_key, "control_generation", "8")
            with self.assertRaisesRegex(RuntimeError, "admission was denied"):
                recorder_proxy.issue_recorder_attempt(
                    self.redis, lease, channel_uuid=channel,
                    candidate={"id": "stream-operation-synthetic", "account_id": "account-operation-synthetic"},
                    config_generation=config_generation, control_generation=7,
                    internal_base_url="http://dispatcharr.invalid", sharing_authority=authority,
                )

    def _native_source_and_viewer(self):
        keys = _keys()
        self.redis.command("SET", keys["owner"], WORKER, "EX", 600)
        self.redis.command("SET", keys["heartbeat"], "alive", "EX", 600)
        self.redis.command(
            "HSET", keys["metadata"], "owner", WORKER,
            "channel_uuid", CHANNEL, "state", "active", "stream_id", STREAM,
            "m3u_profile", M3U_PROFILE, "stream_profile", INPUT_PROFILE,
        )
        self.redis.command("SET", keys["buffer_index"], "1")
        self.redis.command("ZADD", keys["timestamps"], "100", "1")
        self.redis.command("SET", keys["chunk"], "synthetic-ts")
        self.redis.command("SADD", keys["clients"], VIEWER)
        self.redis.command(
            "HSET", keys["viewer"], "connected_at", "100",
            "worker_id", "viewer-worker-synthetic-lua-060", "user_id", "7",
            "output_format", "mpegts", "output_profile_id", "",
        )

    def _opening_projection(self, *, episode=None, client_id=CLIENT, return_seconds=30,
                            minimum_switch_seconds=60, last_switch_at=95):
        episode = episode or _episode(client_id=client_id)
        self._base_authority()
        self.assertTrue(project_opening_episode(
            self.redis, episode, lease_value=LEASE, catalog_generation=CATALOG,
            return_seconds=return_seconds,
            minimum_switch_seconds=minimum_switch_seconds,
            last_switch_at=last_switch_at,
        ))
        return episode

    def _register(self, *, client_id: str = CLIENT, return_seconds=30,
                  minimum_switch_seconds=60, last_switch_at=95):
        episode = self._opening_projection(
            client_id=client_id,
            return_seconds=return_seconds,
            minimum_switch_seconds=minimum_switch_seconds,
            last_switch_at=last_switch_at,
        )
        self._native_source_and_viewer()
        ack = register_borrowed_client(
            self.redis, episode, lease_value=LEASE, catalog_generation=CATALOG,
            client_id=client_id, local_worker_id=TASK_WORKER,
            user_agent="Synthetic Player", client_ip="192.0.2.60",
            client_record_ttl=60,
        )
        return episode, ack

    def _capability(self, episode, *, expires_at=None):
        identity = (
            CHANNEL, WORKER, WORKER, WORKER, STREAM, ACCOUNT,
            M3U_PROFILE, INPUT_PROFILE,
        )
        return sign_borrow_capability(
            secret=CAPABILITY_SECRET, channel_uuid=CHANNEL, episode_id=episode.episode_id,
            user_id="user-synthetic-lua-060",
            configuration_generation=episode.configuration_generation,
            catalog_generation=CATALOG, control_generation=episode.control_generation,
            recorder_fence=episode.recorder_fence, intent_epoch=episode.intent_epoch,
            connection_identity=identity, episode_version=episode.version,
            expires_at=time.time() + 60 if expires_at is None else expires_at,
            nonce="nonce-synthetic-lua-060",
        )

    def test_signed_opening_capability_is_current_and_single_use(self):
        episode = self._opening_projection()
        self._native_source_and_viewer()
        capability = self._capability(episode)
        self.assertTrue(consume_opening_capability(
            self.redis, capability, episode, catalog_generation=CATALOG, now=time.time(),
            secret=CAPABILITY_SECRET, user_id="user-synthetic-lua-060",
        ))
        self.assertFalse(consume_opening_capability(
            self.redis, capability, episode, catalog_generation=CATALOG, now=time.time(),
            secret=CAPABILITY_SECRET, user_id="user-synthetic-lua-060",
        ))

    def test_fenced_opening_rejects_delayed_registration_before_absence_cleanup(self):
        episode = self._opening_projection()
        self.assertTrue(fence_unregistered_opening(
            self.redis, episode, catalog_generation=CATALOG,
        ))
        self._native_source_and_viewer()
        self.assertIsNone(register_borrowed_client(
            self.redis, episode, lease_value=LEASE, catalog_generation=CATALOG,
            client_id=CLIENT, local_worker_id=TASK_WORKER,
            user_agent="Synthetic Player", client_ip="192.0.2.60",
        ))
        self.assertTrue(native_registration_is_absent(
            self.redis, channel_uuid=CHANNEL, client_id=CLIENT,
        ))
        self.assertTrue(close_fenced_unregistered_opening(
            self.redis, episode, catalog_generation=CATALOG, client_id=CLIENT,
        ))
        self.assertTrue(fence_unregistered_opening(
            self.redis, episode, catalog_generation=CATALOG,
        ))
        self.assertTrue(close_fenced_unregistered_opening(
            self.redis, episode, catalog_generation=CATALOG, client_id=CLIENT,
        ))
        self.assertEqual(self.redis.hget(_keys()["episode"], "phase"), "closed")
        next_episode = _episode(epoch=episode.intent_epoch + 1)
        self.assertTrue(self._opening_projection(episode=next_episode))

    def test_fenced_opening_close_rejects_hash_membership_and_marker_resources(self):
        for resource in ("hash", "membership", "marker"):
            with self.subTest(resource=resource):
                self.redis.command("FLUSHDB")
                episode = self._opening_projection()
                self.assertTrue(fence_unregistered_opening(
                    self.redis, episode, catalog_generation=CATALOG,
                ))
                if resource == "hash":
                    self.redis.command(
                        "HSET", _keys()["client"], "connected_at", "replacement-synthetic",
                    )
                elif resource == "membership":
                    self.redis.command("SADD", _keys()["clients"], CLIENT)
                else:
                    self.redis.command("HSET", _keys()["private"], CLIENT, "other-episode-synthetic")
                self.assertFalse(close_fenced_unregistered_opening(
                    self.redis, episode, catalog_generation=CATALOG, client_id=CLIENT,
                ))
                self.assertEqual(self.redis.hget(_keys()["episode"], "phase"), "closing")

    def test_closed_opening_retry_requires_resources_still_absent(self):
        episode = self._opening_projection()
        self.assertTrue(fence_unregistered_opening(
            self.redis, episode, catalog_generation=CATALOG,
        ))
        self.assertTrue(close_fenced_unregistered_opening(
            self.redis, episode, catalog_generation=CATALOG, client_id=CLIENT,
        ))
        self.assertTrue(fence_unregistered_opening(
            self.redis, episode, catalog_generation=CATALOG,
        ))
        self.assertTrue(close_fenced_unregistered_opening(
            self.redis, episode, catalog_generation=CATALOG, client_id=CLIENT,
        ))
        for resource in ("hash", "membership", "marker"):
            with self.subTest(resource=resource):
                self.redis.command("FLUSHDB")
                episode = self._opening_projection()
                self.assertTrue(fence_unregistered_opening(
                    self.redis, episode, catalog_generation=CATALOG,
                ))
                self.assertTrue(close_fenced_unregistered_opening(
                    self.redis, episode, catalog_generation=CATALOG, client_id=CLIENT,
                ))
                if resource == "hash":
                    self.redis.command(
                        "HSET", _keys()["client"], "connected_at", "replacement-synthetic",
                    )
                elif resource == "membership":
                    self.redis.command("SADD", _keys()["clients"], CLIENT)
                else:
                    self.redis.command(
                        "HSET", _keys()["private"], CLIENT, "replacement-episode-synthetic",
                    )
                self.assertFalse(close_fenced_unregistered_opening(
                    self.redis, episode, catalog_generation=CATALOG, client_id=CLIENT,
                ))
                self.assertEqual(self.redis.hget(_keys()["episode"], "phase"), "closed")

    def test_signed_opening_capability_rejects_stale_authority_without_nonce_write(self):
        episode = self._opening_projection()
        capability = self._capability(episode)
        self.redis.command("HSET", _keys()["current"], "catalog_generation", "catalog-new-synthetic")
        self.assertFalse(consume_opening_capability(
            self.redis, capability, episode, catalog_generation=CATALOG, now=time.time(),
            secret=CAPABILITY_SECRET, user_id="user-synthetic-lua-060",
        ))
        nonce_key = "catchuparr:sharing:consumed:" + CHANNEL + ":" + hashlib.sha256(
            capability.nonce.encode()
        ).hexdigest()
        self.assertFalse(self.redis.exists(nonce_key))

    def test_exact_cleanup_keeps_marker_until_registration_and_handles_replacement(self):
        episode, ack = self._register()
        self.assertIsNotNone(ack)
        keys = _keys()
        manager = SimpleNamespace(
            channel_id=CHANNEL, worker_id=TASK_WORKER, redis_client=self.redis,
            lock=__import__("threading").RLock(), clients={CLIENT},
            _registered_clients={CLIENT}, last_heartbeat_time={CLIENT: 100.0},
        )
        self.redis.command("HSET", keys["client"], "connected_at", "replacement-time-synthetic")
        self.assertFalse(cleanup_borrowed_client(
            self.redis, ack, episode_id=episode.episode_id, manager=manager,
        ))
        self.assertEqual(self.redis.hget(keys["private"], CLIENT), EPISODE_ID)
        self.assertIn(CLIENT, self.redis.smembers(keys["clients"]))
        self.redis.command("HSET", keys["client"], "connected_at", ack.connected_at)
        self.assertTrue(cleanup_borrowed_client(
            self.redis, ack, episode_id=episode.episode_id, manager=manager,
        ))
        self.assertFalse(self.redis.exists(keys["client"]))
        self.assertIsNone(self.redis.hget(keys["private"], CLIENT))
        self.assertNotIn(CLIENT, self.redis.smembers(keys["clients"]))
        self.assertNotIn(CLIENT, manager.clients)

    def test_current_authority_revision_cas_prevents_stale_overwrite(self):
        self._base_authority(revision=4)
        self.assertTrue(publish_current_authority(
            self.redis, CHANNEL, configuration_generation="config-next",
            control_generation=8, catalog_generation="catalog-next",
            expected_revision=4, authority_revision=5,
        ))
        self.assertFalse(publish_current_authority(
            self.redis, CHANNEL, configuration_generation=CONFIG,
            control_generation=7, catalog_generation=CATALOG,
            expected_revision=4, authority_revision=6,
        ))
        self.assertEqual(self.redis.hget(_keys()["current"], "authority_revision"), "5")
        self.assertEqual(self.redis.hget(_keys()["current"], "configuration_generation"), "config-next")

    def test_committed_authority_change_invalidates_only_exact_prior_episode(self):
        _episode, ack = self._register()
        self.assertIsNotNone(ack)
        grant = read_native_episode_grant(self.redis, CHANNEL)
        self.assertIsNotNone(grant)
        self.assertTrue(publish_current_authority(
            self.redis, CHANNEL, configuration_generation="config-next",
            control_generation=8, catalog_generation="catalog-next",
            expected_revision=1, authority_revision=2,
        ))
        self.assertTrue(invalidate_native_episode(
            self.redis, grant, configuration_generation="config-next",
            control_generation=8, catalog_generation="catalog-next",
            authority_revision=2,
        ))
        self.assertEqual(self.redis.hget(_keys()["episode"], "phase"), "closing")
        self.assertEqual(self.redis.hget(_keys()["episode"], "version"), str(grant.version + 1))
        # A lost response after the exact closing CAS is safely idempotent.
        self.assertTrue(invalidate_native_episode(
            self.redis, grant, configuration_generation="config-next",
            control_generation=8, catalog_generation="catalog-next",
            authority_revision=2,
        ))

    def test_invalidation_survives_viewer_leave_and_return_after_prior_snapshot(self):
        _episode, ack = self._register()
        self.assertIsNotNone(ack)
        prior = read_native_episode_grant(self.redis, CHANNEL)
        self.assertIsNotNone(prior)
        keys = _keys()

        # Interleave valid native lease/grace transitions between the durable
        # prior snapshot and the authority publication/invalidation callback.
        self.redis.command("SREM", keys["clients"], VIEWER)
        self.redis.command("DEL", keys["viewer"])
        current = read_native_episode_grant(self.redis, CHANNEL)
        ok, deadline, departed_version = check_and_touch_borrowed_client(self.redis, current)
        self.assertTrue(ok)
        self.assertIsNotNone(deadline)
        self.assertEqual("wait_return", self.redis.hget(keys["episode"], "phase"))

        self.redis.command("SADD", keys["clients"], VIEWER)
        self.redis.command(
            "HSET", keys["viewer"], "connected_at", "200",
            "worker_id", "viewer-worker-synthetic-lua-060", "user_id", "7",
            "output_format", "mpegts", "output_profile_id", "",
        )
        current = read_native_episode_grant(self.redis, CHANNEL)
        ok, deadline, returned_version = check_and_touch_borrowed_client(self.redis, current)
        self.assertTrue(ok)
        self.assertIsNone(deadline)
        self.assertEqual(departed_version + 1, returned_version)
        self.assertEqual("active", self.redis.hget(keys["episode"], "phase"))
        self.assertGreater(returned_version, prior.version)

        self.assertTrue(publish_current_authority(
            self.redis, CHANNEL, configuration_generation="config-next",
            control_generation=8, catalog_generation="catalog-next",
            expected_revision=1, authority_revision=2,
        ))
        self.assertTrue(invalidate_native_episode(
            self.redis, prior, configuration_generation="config-next",
            control_generation=8, catalog_generation="catalog-next",
            authority_revision=2,
        ))
        self.assertEqual("closing", self.redis.hget(keys["episode"], "phase"))
        self.assertEqual(str(returned_version + 1), self.redis.hget(keys["episode"], "version"))
        self.assertEqual("2", self.redis.hget(
            keys["episode"], "revocation_authority_revision",
        ))
        # The exact committed operation stamp, not the stale prior version, is
        # the retry acknowledgement after a lost Lua response.
        self.assertTrue(invalidate_native_episode(
            self.redis, prior, configuration_generation="config-next",
            control_generation=8, catalog_generation="catalog-next",
            authority_revision=2,
        ))

    def test_current_authority_invalidation_rejects_stale_revision_and_projection(self):
        changed_fields = (
            ("client_id", "replacement-client-synthetic"),
            ("owner_lock_value", "replacement-owner-lock-synthetic"),
            ("metadata_owner", "replacement-metadata-owner-synthetic"),
            ("client_worker_id", "replacement-client-worker-synthetic"),
            ("lease_value", "replacement-lease-synthetic"),
        )
        for field, replacement in changed_fields:
            with self.subTest(field=field):
                self.redis.command("FLUSHDB")
                _episode, ack = self._register()
                self.assertIsNotNone(ack)
                grant = read_native_episode_grant(self.redis, CHANNEL)
                self.assertIsNotNone(grant)
                self.assertTrue(publish_current_authority(
                    self.redis, CHANNEL, configuration_generation="config-next",
                    control_generation=8, catalog_generation="catalog-next",
                    expected_revision=1, authority_revision=2,
                ))
                self.assertFalse(invalidate_native_episode(
                    self.redis, grant, configuration_generation="config-next",
                    control_generation=8, catalog_generation="catalog-next",
                    authority_revision=3,
                ))
                self.redis.command("HSET", _keys()["episode"], field, replacement)
                self.assertFalse(invalidate_native_episode(
                    self.redis, grant, configuration_generation="config-next",
                    control_generation=8, catalog_generation="catalog-next",
                    authority_revision=2,
                ))
                self.assertEqual(self.redis.hget(_keys()["episode"], "phase"), "active")

    def test_project_rejects_wrong_projection_type_without_replacing_key(self):
        self._base_authority()
        keys = _keys()
        self.redis.command("SET", keys["episode"], "original-string-synthetic")
        self.assertFalse(project_opening_episode(
            self.redis, _episode(), lease_value=LEASE, catalog_generation=CATALOG,
            return_seconds=30, minimum_switch_seconds=60, last_switch_at=95,
        ))
        self.assertEqual(self.redis.get(keys["episode"]), "original-string-synthetic")

    def test_projection_and_registration_execute_lua_against_real_key_types(self):
        episode, ack = self._register()
        self.assertIsNotNone(
            ack,
            f"Lua registration returned {self.redis.last_eval!r}; phase="
            f"{self.redis.hget(_keys()['episode'], 'phase')!r}; call={self.redis.last_invocation!r}",
        )
        keys = _keys()
        self.assertEqual(self.redis.hget(keys["episode"], "phase"), "active")
        self.assertEqual(self.redis.smembers(keys["clients"]), {VIEWER, CLIENT})
        self.assertEqual(self.redis.hget(keys["private"], CLIENT), EPISODE_ID)
        self.assertEqual(self.redis.hget(keys["client"], "worker_id"), TASK_WORKER)
        self.assertGreater(self.redis.ttl(keys["client"]), 0)
        grant = read_native_episode_grant(self.redis, CHANNEL)
        self.assertIsNotNone(grant)
        self.assertEqual(grant.version, episode.version + 1)

    def test_registration_rejects_client_id_different_from_persisted_attempt(self):
        episode = self._opening_projection()
        self._native_source_and_viewer()
        self.assertIsNone(register_borrowed_client(
            self.redis, episode, lease_value=LEASE, catalog_generation=CATALOG,
            client_id="catchuparr-client-replacement-synthetic",
            local_worker_id=TASK_WORKER, user_agent="Synthetic Player",
            client_ip="192.0.2.60", client_record_ttl=60,
        ))
        self.assertEqual(self.redis.hget(_keys()["episode"], "phase"), "opening")
        self.assertEqual(self.redis.hget(_keys()["episode"], "client_id"), CLIENT)
        self.assertFalse(self.redis.exists(_keys()["client"]))

    def test_cleanup_fences_only_the_persisted_client_attempt(self):
        episode = self._opening_projection()
        wrong_client = _episode(client_id="catchuparr-client-wrong-synthetic")
        self.assertFalse(fence_unregistered_opening(
            self.redis, wrong_client, catalog_generation=CATALOG,
        ))
        self.assertFalse(close_fenced_unregistered_opening(
            self.redis, episode, catalog_generation=CATALOG,
            client_id="catchuparr-client-wrong-synthetic",
        ))
        self.assertEqual(self.redis.hget(_keys()["episode"], "phase"), "opening")
        self.assertEqual(self.redis.hget(_keys()["episode"], "client_id"), CLIENT)

    def test_registration_lost_reply_recovers_exact_committed_ack(self):
        episode = self._opening_projection()
        self._native_source_and_viewer()

        class LostReplyRedis:
            def __init__(self, delegate):
                self.delegate = delegate
                self.connection_pool = delegate.connection_pool
                self.lost = False

            def __getattr__(self, name):
                return getattr(self.delegate, name)

            def eval(self, *args):
                result = self.delegate.eval(*args)
                if not self.lost and result and result[0] == 1:
                    self.lost = True
                    raise RuntimeError("synthetic reply lost after Lua commit")
                return result

        wrapped = LostReplyRedis(self.redis)
        ack = register_borrowed_client(
            wrapped, episode, lease_value=LEASE, catalog_generation=CATALOG,
            client_id=CLIENT, local_worker_id=TASK_WORKER,
            user_agent="Synthetic Player", client_ip="192.0.2.60",
            client_record_ttl=60,
        )
        self.assertIsNotNone(ack)
        self.assertEqual(ack.episode_id, episode.episode_id)
        self.assertEqual(ack.client_id, CLIENT)
        self.assertEqual(self.redis.hget(_keys()["private"], CLIENT), EPISODE_ID)

    def test_registration_rejects_stale_authority_and_has_no_partial_writes(self):
        episode = self._opening_projection()
        self._native_source_and_viewer()
        self.redis.command("HSET", _keys()["current"], "control_generation", "8")
        result = register_borrowed_client(
            self.redis, episode, lease_value=LEASE, catalog_generation=CATALOG,
            client_id=CLIENT, local_worker_id=TASK_WORKER,
            user_agent="Synthetic Player", client_ip="192.0.2.60",
        )
        self.assertIsNone(result)
        keys = _keys()
        self.assertEqual(self.redis.hget(keys["episode"], "phase"), "opening")
        self.assertFalse(self.redis.exists(keys["client"]))
        self.assertFalse(self.redis.smembers(keys["clients"]) - {VIEWER})
        self.assertIsNone(self.redis.hget(keys["private"], CLIENT))

    def test_registration_rejects_hash_only_client_collision_without_replacing_it(self):
        episode = self._opening_projection()
        self._native_source_and_viewer()
        keys = _keys()
        self.redis.command("HSET", keys["client"], "connected_at", "original", "user_id", "42")
        result = register_borrowed_client(
            self.redis, episode, lease_value=LEASE, catalog_generation=CATALOG,
            client_id=CLIENT, local_worker_id=TASK_WORKER,
            user_agent="Synthetic Player", client_ip="192.0.2.60",
        )
        self.assertIsNone(result)
        self.assertEqual(self.redis.hget(keys["client"], "connected_at"), "original")
        self.assertNotIn(CLIENT, self.redis.smembers(keys["clients"]))
        self.assertIsNone(self.redis.hget(keys["private"], CLIENT))

    def test_registration_type_preflight_prevents_partial_mutation(self):
        for wrong_key in ("client", "clients", "private"):
            with self.subTest(wrong_key=wrong_key):
                self.redis.command("FLUSHDB")
                episode = self._opening_projection()
                self._native_source_and_viewer()
                keys = _keys()
                if wrong_key == "clients":
                    self.redis.command("DEL", keys["clients"])
                    self.redis.command("HSET", keys["clients"], "wrong-type", "sentinel")
                else:
                    self.redis.command("SET", keys[wrong_key], "wrong-type-sentinel")
                before = self.redis.hgetall(keys["episode"])
                result = register_borrowed_client(
                    self.redis, episode, lease_value=LEASE, catalog_generation=CATALOG,
                    client_id=CLIENT, local_worker_id=TASK_WORKER,
                    user_agent="Synthetic Player", client_ip="192.0.2.60",
                )
                self.assertIsNone(result)
                self.assertEqual(self.redis.hgetall(keys["episode"]), before)
                self.assertEqual(self.redis.command("TYPE", keys[wrong_key]),
                                 ["string" if wrong_key != "clients" else "hash"])
                self.assertEqual(self.redis.get(keys["client"]),
                                 "wrong-type-sentinel" if wrong_key == "client" else None)
                self.assertEqual(self.redis.command("TYPE", keys["private"]),
                                 ["string"] if wrong_key == "private" else ["none"])

    def test_registration_rejection_matrix_preserves_opening_authority(self):
        cases = (
            "channel_stop", "client_stop", "owner_changed", "profile_changed",
            "heartbeat_expired", "lease_expired", "current_generation_changed",
            "buffer_chunk_missing", "invalid_ttl",
        )
        for case in cases:
            with self.subTest(case=case):
                self.redis.command("FLUSHDB")
                episode = self._opening_projection()
                self._native_source_and_viewer()
                keys = _keys()
                if case == "channel_stop":
                    self.redis.command("SET", f"live:channel:{CHANNEL}:stopping", "1")
                elif case == "client_stop":
                    self.redis.command(
                        "SET", f"live:channel:{CHANNEL}:client:{CLIENT}:stop", "1"
                    )
                elif case == "owner_changed":
                    self.redis.command("SET", keys["owner"], "replacement-owner-synthetic", "EX", 600)
                elif case == "profile_changed":
                    self.redis.command("HSET", keys["metadata"], "stream_profile", "replacement-profile-synthetic")
                elif case == "heartbeat_expired":
                    self.redis.command("DEL", keys["heartbeat"])
                elif case == "lease_expired":
                    self.redis.command("DEL", keys["lease"])
                elif case == "current_generation_changed":
                    self.redis.command("HSET", keys["current"], "catalog_generation", "catalog-new-synthetic")
                elif case == "buffer_chunk_missing":
                    self.redis.command("DEL", keys["chunk"])
                elif case != "invalid_ttl":
                    self.fail(f"unknown registration case {case}")
                before_projection = self.redis.hgetall(keys["episode"])
                before_clients = self.redis.smembers(keys["clients"])
                result = register_borrowed_client(
                    self.redis, episode, lease_value=LEASE, catalog_generation=CATALOG,
                    client_id=CLIENT, local_worker_id=TASK_WORKER,
                    user_agent="Synthetic Player", client_ip="192.0.2.60",
                    client_record_ttl=0 if case == "invalid_ttl" else 60,
                )
                self.assertIsNone(result)
                self.assertEqual(self.redis.hgetall(keys["episode"]), before_projection)
                self.assertEqual(self.redis.smembers(keys["clients"]), before_clients)
                self.assertFalse(self.redis.exists(keys["client"]))
                self.assertIsNone(self.redis.hget(keys["private"], CLIENT))

    def test_check_type_preflight_prevents_partial_heartbeat_or_projection_writes(self):
        self._register()
        keys = _keys()
        grant = read_native_episode_grant(self.redis, CHANNEL)
        self.redis.command("DEL", keys["client"])
        self.redis.command("SET", keys["client"], "original-string-synthetic")
        before_projection = self.redis.hgetall(keys["episode"])
        before_members = self.redis.smembers(keys["clients"])
        ok, _, _ = check_and_touch_borrowed_client(self.redis, grant)
        self.assertFalse(ok)
        self.assertEqual(self.redis.get(keys["client"]), "original-string-synthetic")
        self.assertEqual(self.redis.hgetall(keys["episode"]), before_projection)
        self.assertEqual(self.redis.smembers(keys["clients"]), before_members)

    def test_check_and_touch_executes_active_and_viewer_return_grace_transitions(self):
        self._register()
        keys = _keys()
        grant = read_native_episode_grant(self.redis, CHANNEL)
        ok, deadline, version = check_and_touch_borrowed_client(self.redis, grant)
        self.assertTrue(ok)
        self.assertIsNone(deadline)
        self.assertEqual(version, grant.version)

        self.redis.command("SREM", keys["clients"], VIEWER)
        self.redis.command("DEL", keys["viewer"])
        grant = read_native_episode_grant(self.redis, CHANNEL)
        ok, deadline, version = check_and_touch_borrowed_client(self.redis, grant)
        self.assertTrue(ok)
        self.assertIsNotNone(deadline)
        self.assertEqual(version, grant.version + 1)
        self.assertEqual(self.redis.hget(keys["episode"], "phase"), "wait_return")

        self.redis.command("SADD", keys["clients"], VIEWER)
        self.redis.command(
            "HSET", keys["viewer"], "connected_at", "200",
            "worker_id", "viewer-worker-synthetic-lua-060", "user_id", "7",
            "output_format", "mpegts", "output_profile_id", "",
        )
        grant = read_native_episode_grant(self.redis, CHANNEL)
        ok, deadline, version = check_and_touch_borrowed_client(self.redis, grant)
        self.assertTrue(ok)
        self.assertIsNone(deadline)
        self.assertEqual(version, grant.version + 1)
        self.assertEqual(self.redis.hget(keys["episode"], "phase"), "active")

    def test_check_rejects_revoked_flags_expired_grace_and_stale_projection(self):
        self._register()
        keys = _keys()
        grant = read_native_episode_grant(self.redis, CHANNEL)
        self.redis.command("SET", f"live:channel:{CHANNEL}:stopping", "1")
        before = self.redis.hget(keys["client"], "last_active")
        ok, _, _ = check_and_touch_borrowed_client(self.redis, grant)
        self.assertFalse(ok)
        self.assertEqual(self.redis.hget(keys["client"], "last_active"), before)

    def test_active_check_rejection_matrix_preserves_client_and_projection(self):
        cases = (
            "channel_stop", "client_stop", "owner_changed", "profile_changed",
            "heartbeat_expired", "lease_expired", "current_generation_changed",
            "connected_at_replaced", "stale_grant_version", "invalid_ttl",
        )
        for case in cases:
            with self.subTest(case=case):
                self.redis.command("FLUSHDB")
                self._register()
                keys = _keys()
                grant = read_native_episode_grant(self.redis, CHANNEL)
                if case == "channel_stop":
                    self.redis.command("SET", f"live:channel:{CHANNEL}:stopping", "1")
                elif case == "client_stop":
                    self.redis.command(
                        "SET", f"live:channel:{CHANNEL}:client:{CLIENT}:stop", "1"
                    )
                elif case == "owner_changed":
                    self.redis.command("SET", keys["owner"], "replacement-owner-synthetic", "EX", 600)
                elif case == "profile_changed":
                    self.redis.command("HSET", keys["metadata"], "m3u_profile", "replacement-m3u-synthetic")
                elif case == "heartbeat_expired":
                    self.redis.command("DEL", keys["heartbeat"])
                elif case == "lease_expired":
                    self.redis.command("DEL", keys["lease"])
                elif case == "current_generation_changed":
                    self.redis.command("HSET", keys["current"], "control_generation", "8")
                elif case == "connected_at_replaced":
                    self.redis.command("HSET", keys["client"], "connected_at", "replacement-time-synthetic")
                elif case == "stale_grant_version":
                    self.redis.command("HINCRBY", keys["episode"], "version", 1)
                elif case != "invalid_ttl":
                    self.fail(f"unknown active check case {case}")
                before_projection = self.redis.hgetall(keys["episode"])
                before_client = self.redis.hgetall(keys["client"])
                before_members = self.redis.smembers(keys["clients"])
                ok, _, _ = check_and_touch_borrowed_client(
                    self.redis, grant, client_record_ttl=0 if case == "invalid_ttl" else 60
                )
                self.assertFalse(ok)
                self.assertEqual(self.redis.hgetall(keys["episode"]), before_projection)
                self.assertEqual(self.redis.hgetall(keys["client"]), before_client)
                self.assertEqual(self.redis.smembers(keys["clients"]), before_members)

    def test_dynamic_grace_deadline_retention_and_cooldown_boundaries(self):
        # A very old/future clock record is clamped conservatively at the Redis
        # observation, then retained exactly while the same absence continues.
        self._register(return_seconds=0, minimum_switch_seconds=3600,
                       last_switch_at=10**12)
        keys = _keys()
        self.redis.command("SREM", keys["clients"], VIEWER)
        self.redis.command("DEL", keys["viewer"])
        grant = read_native_episode_grant(self.redis, CHANNEL)
        observed_before = time.time()
        ok, deadline, version = check_and_touch_borrowed_client(self.redis, grant)
        observed_after = time.time()
        self.assertTrue(ok)
        self.assertIsNotNone(deadline)
        self.assertGreaterEqual(deadline, observed_before + 3590)
        self.assertLessEqual(deadline, observed_after + 3601)
        self.assertEqual(version, grant.version + 1)
        stored_deadline = self.redis.hget(keys["episode"], "return_deadline")
        self.assertEqual(float(stored_deadline), deadline)
        grant = read_native_episode_grant(self.redis, CHANNEL)
        ok, retained_deadline, retained_version = check_and_touch_borrowed_client(self.redis, grant)
        self.assertTrue(ok)
        self.assertEqual(retained_deadline, deadline)
        self.assertEqual(retained_version, grant.version)

        # The maximum supported viewer-return grace also remains within the
        # documented bound when no switch cooldown is requested.
        self.redis.command("FLUSHDB")
        self._register(return_seconds=3600, minimum_switch_seconds=0,
                       last_switch_at=time.time())
        keys = _keys()
        self.redis.command("SREM", keys["clients"], VIEWER)
        self.redis.command("DEL", keys["viewer"])
        grant = read_native_episode_grant(self.redis, CHANNEL)
        observed_before = time.time()
        ok, max_deadline, _ = check_and_touch_borrowed_client(self.redis, grant)
        observed_after = time.time()
        self.assertTrue(ok)
        self.assertIsNotNone(max_deadline)
        self.assertGreaterEqual(max_deadline, observed_before + 3590)
        self.assertLessEqual(max_deadline, observed_after + 3601)

        self.redis.command("DEL", f"live:channel:{CHANNEL}:stopping")
        # Reinitialize the first episode for the expired-deadline rejection.
        self.redis.command("FLUSHDB")
        self._register()
        keys = _keys()
        self.redis.command("DEL", f"live:channel:{CHANNEL}:stopping")
        self.redis.command("HSET", keys["episode"], "phase", "wait_return",
                           "viewer_left_at", "1", "return_deadline", "1")
        expired = read_native_episode_grant(self.redis, CHANNEL)
        self.assertIsNotNone(expired)
        before = self.redis.hget(keys["client"], "last_active")
        ok, _, _ = check_and_touch_borrowed_client(self.redis, expired)
        self.assertFalse(ok)
        self.assertEqual(self.redis.hget(keys["client"], "last_active"), before)

    def test_stale_recorder_marker_is_not_promoted_to_ordinary_viewer(self):
        # A present server marker with missing/expired/malformed capability is
        # unknown, never positive evidence of an ordinary viewer.
        for condition in ("missing_cap", "expired_cap", "wrong_lease", "wrong_marker_type"):
            with self.subTest(condition=condition):
                self.redis.command("FLUSHDB")
                episode = self._opening_projection()
                self._native_source_and_viewer()
                keys = _keys()
                self.redis.command("DEL", keys["clients"])
                self.redis.command("SADD", keys["clients"], VIEWER)
                marker = f"catchuparr:stats:recorder-client:{CHANNEL}:{VIEWER}"
                cap = "catchuparr:recorder:stats-cap:digest-stale-synthetic"
                self.redis.command("SET", marker, f"digest-stale-synthetic|{CHANNEL}")
                if condition == "expired_cap":
                    self.redis.command("SET", cap, f"{CHANNEL}|{LEASE}", "EX", 1)
                    self.redis.command("EXPIRE", cap, 0)
                elif condition == "wrong_lease":
                    self.redis.command("SET", cap, f"{CHANNEL}|wrong-lease-synthetic")
                elif condition == "wrong_marker_type":
                    self.redis.command("DEL", marker)
                    self.redis.command("HSET", marker, "marker", "wrong-type")
                elif condition != "missing_cap":
                    self.fail(f"unknown marker condition {condition}")
                result = register_borrowed_client(
                    self.redis, episode, lease_value=LEASE, catalog_generation=CATALOG,
                    client_id=CLIENT, local_worker_id=TASK_WORKER,
                    user_agent="Synthetic Player", client_ip="192.0.2.60",
                )
                self.assertIsNone(result)
                self.assertEqual(self.redis.hget(keys["episode"], "phase"), "opening")
                self.assertFalse(self.redis.exists(keys["client"]))

    def test_check_and_touch_rejects_unknown_stale_marker(self):
        self._register()
        keys = _keys()
        self.redis.command("SREM", keys["clients"], VIEWER)
        self.redis.command("DEL", keys["viewer"])
        # Retain an ordinary-looking native hash but attach an expired server
        # capability marker; Lua must not count it as a viewer or publish.
        self.redis.command("SADD", keys["clients"], VIEWER)
        self.redis.command(
            "HSET", keys["viewer"], "connected_at", "100",
            "worker_id", "viewer-worker-synthetic-lua-060",
        )
        self.redis.command(
            "SET", f"catchuparr:stats:recorder-client:{CHANNEL}:{VIEWER}",
            f"digest-stale-synthetic|{CHANNEL}",
        )
        grant = read_native_episode_grant(self.redis, CHANNEL)
        before = self.redis.hget(keys["client"], "last_active")
        ok, _, _ = check_and_touch_borrowed_client(self.redis, grant)
        self.assertFalse(ok)
        self.assertEqual(self.redis.hget(keys["client"], "last_active"), before)

    def test_grant_repr_does_not_expose_lease_bearer(self):
        self._register()
        grant = read_native_episode_grant(self.redis, CHANNEL)
        self.assertIsNotNone(grant)
        self.assertNotIn(LEASE, repr(grant))


if __name__ == "__main__":
    unittest.main()
