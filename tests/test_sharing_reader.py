import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr.adapters.sharing_reader import (
    _REMOVE_EXACT_NATIVE_CLIENT_LUA,
    _TOUCH_NATIVE_CLIENT_LUA,
    BorrowBindingLost,
    BorrowedClientRegistration,
    BorrowNativeBuffering,
    BorrowReadBinding,
    BorrowReaderUnavailable,
    BorrowReadProof,
    NativeClientActivityGuard,
    ReadOnlyTsBufferReader,
    make_task_local_input_buffer,
    native_read_api_supported,
    redis_backend_signature,
    validate_borrow_read_proof,
)

IDENTITY = (
    "channel-7",
    "viewer-worker-7",
    "owner-lock-7",
    "owner-lock-7",
    "stream-7",
    "account-7",
    "m3u-profile-7",
    "input-profile-7",
)
PACKET = b"\x47" + b"\x00" * 187
CLIENT_ID = "catchuparr-client-synthetic-7"


def binding(*, deadline=None, fence=11, generation="config-a", control=3, dynamic_grace=False):
    return BorrowReadBinding(
        IDENTITY,
        generation,
        control,
        fence,
        pending_return_deadline=deadline,
        dynamic_grace=dynamic_grace,
    )


def proof(
    *,
    identity=IDENTITY,
    deadline=None,
    viewers=("viewer-1",),
    now=100,
    fence=11,
    generation="config-a",
    control=3,
    **overrides,
):
    values = {
        "connection_identity": identity,
        "configuration_generation": generation,
        "control_generation": control,
        "recorder_fence": fence,
        "ordinary_viewer_ids": tuple(viewers),
        "checked_at": now,
        "owner_matches": True,
        "owner_heartbeat_healthy": True,
        "native_active": True,
        "assigned_source_matches": True,
        "account_active": True,
        "profile_active": True,
        "media_compatible": True,
        "useful_buffer_available": True,
        "supported_worker_kind": True,
        "borrowed_client_current": False,
        "pending_return_deadline": deadline,
    }
    values.update(overrides)
    return BorrowReadProof(**values)


class Connection:
    __module__ = "redis.connection"


class SSLConnection:
    __module__ = "redis.connection"


class FakeRedisState:
    def __init__(self, head=5):
        self.head = head
        self.values = {}
        self.sets = {}
        self.hashes = {}


class FakeRedis:
    def __init__(self, head=5, *, state=None, decode_responses=False, backend=None):
        self.state = state or FakeRedisState(head)
        self.values = self.state.values
        self.sets = self.state.sets
        self.hashes = self.state.hashes
        self.reads = []
        self.expirations = {}
        connection_kwargs = {
            "host": "redis.synthetic.invalid",
            "port": 6379,
            "db": 4,
            "username": "synthetic-user",
            "password": "synthetic-secret",
            "decode_responses": decode_responses,
        }
        if backend is not None:
            connection_kwargs.update(backend)
        self.connection_pool = SimpleNamespace(
            connection_class=Connection,
            connection_kwargs=connection_kwargs,
        )
        self.fail_remove = False

    def get(self, key):
        self.reads.append(key)
        value = self.values.get(key, str(self.state.head))
        return value if self.connection_pool.connection_kwargs["decode_responses"] else str(value).encode()

    def exists(self, key):
        return key in self.hashes or key in self.values

    def sismember(self, key, member):
        return member in self.sets.get(key, set())

    def sadd(self, key, member):
        self.sets.setdefault(key, set()).add(member)

    def srem(self, key, member):
        self.sets.setdefault(key, set()).discard(member)

    def hset(self, key, mapping=None, **kwargs):
        self.hashes.setdefault(key, {}).update(mapping or kwargs)

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def delete(self, key):
        self.hashes.pop(key, None)
        self.values.pop(key, None)
        self.sets.pop(key, None)
        self.expirations.pop(key, None)

    def expire(self, key, seconds):
        if key not in self.hashes and key not in self.sets and key not in self.values:
            return False
        self.expirations[key] = seconds
        return True

    @staticmethod
    def _string(value):
        return value.decode() if isinstance(value, bytes) else str(value)

    def eval(self, script, numkeys, *args):
        self.eval_call = (script, numkeys, args)
        if self.fail_remove and script == _REMOVE_EXACT_NATIVE_CLIENT_LUA:
            return 0
        if script == _TOUCH_NATIVE_CLIENT_LUA:
            keys, argv = args[:numkeys], args[numkeys:]
            client_set, client_key, owner_key, metadata_key, heartbeat_key, lease_key, stop_key, stopping_key = keys
            self.eval_call = (script, numkeys, args)
            client_id = self._string(argv[0])
            if client_id not in self.sets.get(client_set, set()) or client_key not in self.hashes:
                return 0
            if stop_key in self.values or stopping_key in self.values:
                return 0
            if self._string(self.values.get(owner_key, "")) != self._string(argv[1]):
                return 0
            metadata = self.hashes.get(metadata_key, {})
            if any(self._string(metadata.get(field, "")) != self._string(argv[index])
                   for field, index in (("owner", 2), ("stream_id", 3), ("m3u_profile", 12), ("stream_profile", 13))):
                return 0
            if self._string(metadata.get("state", "")) not in {"active", "waiting_for_clients"}:
                return 0
            if heartbeat_key not in self.values or self._string(self.values.get(lease_key, "")) != self._string(argv[4]):
                return 0
            client = self.hashes[client_key]
            fields = ("connected_at", "worker_id", "user_agent", "ip_address", "user_id", "output_format", "output_profile_id")
            if any(self._string(client.get(field, "")) != self._string(argv[index])
                   for field, index in zip(fields, (5, 6, 7, 8, 9, 10, 11), strict=True)):
                return 0
            client["last_active"] = self._string(argv[14])
            return 1
        if script == _REMOVE_EXACT_NATIVE_CLIENT_LUA:
            keys, argv = args[:numkeys], args[numkeys:]
            client_set, client_key, disconnect_key = keys
            client_id = self._string(argv[0])
            client = self.hashes.get(client_key)
            is_member = client_id in self.sets.get(client_set, set())
            if not client:
                if not is_member:
                    return 0
                self.sets[client_set].discard(client_id)
                if not self.sets[client_set]:
                    self.values[disconnect_key] = self._string(argv[8])
                return 1
            fields = ("connected_at", "worker_id", "user_agent", "ip_address", "user_id", "output_format", "output_profile_id")
            if not client or any(self._string(client.get(field, "")) != self._string(argv[index])
                                 for field, index in zip(fields, (1, 2, 3, 4, 5, 6, 7), strict=True)):
                return 0
            self.hashes.pop(client_key, None)
            self.sets.setdefault(client_set, set()).discard(client_id)
            if not self.sets.get(client_set):
                self.values[disconnect_key] = self._string(argv[8])
            return 1
        raise AssertionError("unexpected Redis script")


class FakeBuffer:
    def __init__(self, index=5, batches=(), redis_client=None):
        self.index = index
        self.channel_id = "channel-7"
        self.redis_client = redis_client or FakeRedis(index, decode_responses=False)
        self.buffer_index_key = f"live:channel:{self.channel_id}:input:buffer:index"
        self.buffer_prefix = f"live:channel:{self.channel_id}:input:buffer:chunk:"
        self.chunk_timestamps_key = f"live:channel:{self.channel_id}:input:buffer:chunk_timestamps"
        self.target_chunk_size = 188
        self.batches = list(batches)
        self.read_calls = []
        self.oldest_calls = []

    def get_optimized_client_data(self, client_index):
        self.read_calls.append(client_index)
        if self.batches:
            return self.batches.pop(0)
        return [], client_index

    def find_oldest_available_chunk(self, client_index):
        self.oldest_calls.append(client_index)
        return None


def task_local_buffer(index=5, batches=(), redis_client=None):
    if redis_client is None:
        redis_client = FakeRedis(index, decode_responses=False)
    elif redis_client.connection_pool.connection_kwargs["decode_responses"]:
        redis_client = FakeRedis(
            index,
            state=redis_client.state,
            decode_responses=False,
            backend={key: value for key, value in redis_client.connection_pool.connection_kwargs.items()
                     if key != "decode_responses"},
        )
    class Buffer(FakeBuffer):
        def __init__(self, channel_id=None, redis_client=None):
            super().__init__(index=index, batches=batches, redis_client=redis_client)
            self.channel_id = channel_id
            self.buffer_index_key = f"live:channel:{channel_id}:input:buffer:index"

    return make_task_local_input_buffer(Buffer, "channel-7", redis_client)


class FakeClientManager:
    def __init__(self, *, add_result=None, redis=None, channel_id="channel-7"):
        self.clients = set()
        self.add_result = add_result
        self.add_calls = []
        self.remove_calls = []
        self.stop_calls = 0
        self.channel_id = channel_id
        self.client_set_key = f"live:channel:{channel_id}:clients"
        if redis is None:
            self.redis_client = FakeRedis(decode_responses=True)
        elif redis.connection_pool.connection_kwargs["decode_responses"]:
            self.redis_client = redis
        else:
            self.redis_client = FakeRedis(
                state=redis.state,
                decode_responses=True,
                backend={key: value for key, value in redis.connection_pool.connection_kwargs.items()
                         if key != "decode_responses"},
            )
        self.buffer_redis = FakeRedis(
            state=self.redis_client.state,
            decode_responses=False,
            backend={key: value for key, value in self.redis_client.connection_pool.connection_kwargs.items()
                     if key != "decode_responses"},
        )
        self.worker_id = "viewer-worker-7"
        self.fail_remove = False
        self.add_failure_stage = None
        self.add_failure_snapshot = None
        self.lock = threading.RLock()
        self._registered_clients = set()
        self.last_heartbeat_time = {}
        self.last_active_time = 100.0
        self.client_ttl = 60

    def _write_registration(self, client_id, client_ip, user_agent, output_format, output_profile_id):
        client_key = f"live:channel:{self.channel_id}:clients:{client_id}"
        if self.add_failure_stage == "hset":
            raise RuntimeError("synthetic HSET failure")
        self.redis_client.hset(client_key, mapping={
                "user_agent": user_agent or "unknown",
                "ip_address": client_ip,
                "connected_at": "100.0",
                "last_active": "100.0",
                "worker_id": self.worker_id,
                "user_id": "0",
                "output_format": output_format,
                "output_profile_id": output_profile_id or "",
            })
        if self.add_failure_stage == "expire_hash":
            raise RuntimeError("synthetic hash EXPIRE failure")
        self.redis_client.expire(client_key, self.client_ttl)
        if self.add_failure_stage == "sadd":
            raise RuntimeError("synthetic SADD failure")
        self.redis_client.sadd(self.client_set_key, client_id)
        if self.add_failure_stage == "expire_set":
            raise RuntimeError("synthetic set EXPIRE failure")
        self.redis_client.expire(self.client_set_key, self.client_ttl)

    def add_client(
        self, client_id, client_ip, user_agent=None, user=None,
        output_format="mpegts", output_profile_id=None,
    ):
        self.add_calls.append((client_id, client_ip, user_agent, user, output_format, output_profile_id))
        self.clients.add(client_id)
        self._registered_clients.add(client_id)
        try:
            self._write_registration(client_id, client_ip, user_agent, output_format, output_profile_id)
        except Exception:
            client_key = f"live:channel:{self.channel_id}:clients:{client_id}"
            self.add_failure_snapshot = {
                "hash_exists": self.redis_client.exists(client_key),
                "member_exists": self.redis_client.sismember(self.client_set_key, client_id),
                "hash_ttl": self.redis_client.expirations.get(client_key),
                "set_ttl": self.redis_client.expirations.get(self.client_set_key),
            }
            self.clients.discard(client_id)
            self._registered_clients.discard(client_id)
            return False
        if self.add_result is False:
            return False
        if self.add_result == "raise":
            self.clients.discard(client_id)
            self._registered_clients.discard(client_id)
            raise RuntimeError("synthetic post-write add failure")
        return self.add_result if self.add_result is not None else len(self.clients)

    def remove_client(self, client_id):
        self.remove_calls.append(client_id)
        if self.fail_remove:
            raise RuntimeError("synthetic native removal failure")
        self.clients.discard(client_id)
        self.redis_client.srem(self.client_set_key, client_id)
        self.redis_client.delete(f"live:channel:{self.channel_id}:clients:{client_id}")
        return len(self.clients)

    def stop(self):
        self.stop_calls += 1


class SharingReaderTests(unittest.TestCase):
    @staticmethod
    def registration_proof(manager, **kwargs):
        client_key = f"live:channel:{manager.channel_id}:clients:{CLIENT_ID}"
        registered = (
            CLIENT_ID in manager.clients
            and manager.redis_client.sismember(manager.client_set_key, CLIENT_ID)
            and manager.redis_client.exists(client_key)
        )
        return proof(borrowed_client_current=registered, **kwargs)

    @staticmethod
    def activity_writer(_binding, _client_id, native_proof, _client_fields):
        return native_proof.borrowed_client_current

    def test_api_gate_accepts_pinned_buffer_and_client_manager_signatures(self):
        class PinnedBuffer:
            def __init__(self, channel_id=None, redis_client=None):
                self.index = 0

            def get_optimized_client_data(self, client_index):
                return [], client_index

            def find_oldest_available_chunk(self, client_index):
                return None

        class PinnedManager(FakeClientManager):
            def __init__(self, channel_id=None, redis_client=None, heartbeat_interval=1, worker_id=None):
                super().__init__()

        self.assertTrue(native_read_api_supported(PinnedBuffer, PinnedManager))

        class MissingOldest(PinnedBuffer):
            find_oldest_available_chunk = None

        self.assertFalse(native_read_api_supported(MissingOldest, PinnedManager))

    def test_task_local_buffer_uses_only_channel_and_redis_constructor(self):
        calls = []

        class TaskBuffer(FakeBuffer):
            def __init__(self, channel_id=None, redis_client=None):
                calls.append((channel_id, redis_client))
                super().__init__(index=8, redis_client=redis_client)

        redis = FakeRedis(8)
        result = make_task_local_input_buffer(TaskBuffer, "channel-7", redis)
        self.assertEqual([("channel-7", redis)], calls)
        self.assertEqual(8, result.index)

    def test_reader_rejects_shared_buffer_without_private_marker(self):
        with self.assertRaises(BorrowReaderUnavailable):
            ReadOnlyTsBufferReader(FakeBuffer(), binding(), lambda: proof())

    def test_task_local_buffer_rejects_unaligned_native_chunk_configuration(self):
        class UnalignedBuffer(FakeBuffer):
            def __init__(self, channel_id=None, redis_client=None):
                super().__init__()
                self.target_chunk_size = 1024

        with self.assertRaises(BorrowReaderUnavailable):
            make_task_local_input_buffer(UnalignedBuffer, "channel-7", object())

    def test_task_local_buffer_rejects_native_channel_namespace_mismatch(self):
        class WrongChannelBuffer(FakeBuffer):
            def __init__(self, channel_id=None, redis_client=None):
                super().__init__(index=5, redis_client=redis_client)
                self.channel_id = "channel-99"
                self.buffer_index_key = "live:channel:channel-99:input:buffer:index"

        with self.assertRaises(BorrowReaderUnavailable):
            make_task_local_input_buffer(WrongChannelBuffer, "channel-7", FakeRedis())

    def test_direct_reader_rejects_wrong_native_chunk_namespaces(self):
        for field, wrong in (
            ("buffer_prefix", "live:channel:channel-99:input:buffer:chunk:"),
            ("chunk_timestamps_key", "live:channel:channel-99:input:buffer:chunk_timestamps"),
        ):
            buffer = task_local_buffer(batches=[([PACKET], 6)])
            setattr(buffer, field, wrong)
            with self.subTest(field=field), self.assertRaises(BorrowReaderUnavailable):
                ReadOnlyTsBufferReader(buffer, binding(), lambda: proof())

    def test_redis_pool_backend_signatures_hide_credentials_and_reject_unknown_transport(self):
        text = FakeRedis(decode_responses=True)
        binary = FakeRedis(state=text.state, decode_responses=False,
                           backend={k: v for k, v in text.connection_pool.connection_kwargs.items()
                                    if k != "decode_responses"})
        self.assertEqual(redis_backend_signature(text), redis_backend_signature(binary))
        self.assertNotIn("synthetic-secret", redis_backend_signature(text))
        for delta in ({"db": 8}, {"password": "other-secret"}):
            changed = FakeRedis(decode_responses=True, backend=delta)
            self.assertNotEqual(redis_backend_signature(text), redis_backend_signature(changed))
        binary.connection_pool.connection_class = type("Unknown", (), {"__module__": "unknown.redis"})
        with self.assertRaises(BorrowReaderUnavailable):
            redis_backend_signature(binary)

    def test_proof_requires_exact_identity_fences_and_media_eligibility(self):
        validate_borrow_read_proof(binding(), proof())
        for changed in (
            proof(identity=IDENTITY[:-1] + ("other-profile",)),
            proof(fence=12),
            proof(generation="config-b"),
            proof(control=4),
            proof(media_compatible=False),
            proof(owner_matches=False),
            proof(native_active=False),
        ):
            with self.subTest(changed=changed):
                with self.assertRaises(BorrowBindingLost):
                    validate_borrow_read_proof(binding(), changed)

    def test_zero_viewer_read_requires_same_live_pending_return_deadline(self):
        expected = binding(deadline=130)
        validate_borrow_read_proof(
            expected,
            proof(viewers=(), deadline=130, now=129),
        )
        for invalid in (
            proof(viewers=(), deadline=130, now=130),
            proof(viewers=(), deadline=131, now=129),
            proof(viewers=(), deadline=None, now=129),
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaises(BorrowBindingLost):
                    validate_borrow_read_proof(expected, invalid)
        with self.assertRaises(BorrowBindingLost):
            validate_borrow_read_proof(binding(), proof(viewers=()))

    def test_authenticated_dynamic_grace_accepts_current_deadline_after_viewer_departure(self):
        dynamic = binding(dynamic_grace=True)
        validate_borrow_read_proof(
            dynamic,
            proof(viewers=(), now=101, deadline=130, borrowed_client_current=True),
        )
        with self.assertRaises(BorrowBindingLost):
            validate_borrow_read_proof(
                dynamic,
                proof(viewers=(), now=101, deadline=130, borrowed_client_current=False),
            )
        with self.assertRaises(BorrowBindingLost):
            validate_borrow_read_proof(
                dynamic,
                proof(viewers=(), now=130, deadline=130, borrowed_client_current=True),
            )

    def test_reader_yields_only_aligned_chunks_after_repeated_identity_checks(self):
        buffer = task_local_buffer(index=5, batches=[([PACKET, PACKET], 7)])
        reads = []

        def current_proof():
            reads.append(True)
            return proof()

        reader = ReadOnlyTsBufferReader(buffer, binding(), current_proof)
        iterator = reader.iter_chunks()
        self.assertEqual(PACKET, next(iterator))
        self.assertEqual(PACKET, next(iterator))
        self.assertEqual(3, len(reads))  # before batch, then before each chunk
        self.assertEqual([5], buffer.read_calls)
        self.assertEqual(7, reader.local_index)
        iterator.close()

    def test_reader_discards_batch_when_identity_changes_before_publication(self):
        buffer = task_local_buffer(index=5, batches=[([PACKET], 6)])
        evidence = [proof(), proof(identity=IDENTITY[:-1] + ("new-input",))]
        reader = ReadOnlyTsBufferReader(
            buffer, binding(), lambda: evidence.pop(0)
        )
        iterator = reader.iter_chunks()
        with self.assertRaises(BorrowBindingLost):
            next(iterator)

    def test_reader_waits_for_fresh_proof_before_publishing_a_buffered_batch(self):
        buffer = task_local_buffer(index=5, batches=[([PACKET], 6)])
        waiting = threading.Event()
        recovered = threading.Event()
        calls = []

        def current_proof():
            calls.append(True)
            if len(calls) > 1 and not recovered.is_set():
                waiting.set()
                raise BorrowNativeBuffering("temporary buffering")
            return proof()

        reader = ReadOnlyTsBufferReader(buffer, binding(), current_proof, poll_interval=0.01)
        received = []
        thread = threading.Thread(target=lambda: received.append(next(reader.iter_chunks())))
        thread.start()
        self.assertTrue(waiting.wait(1))
        self.assertEqual(received, [])
        self.assertEqual(buffer.read_calls, [5])
        recovered.set()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(received, [PACKET])
        self.assertGreaterEqual(len(calls), 3)

    def test_reader_never_publishes_if_binding_changes_during_buffering(self):
        buffer = task_local_buffer(index=5, batches=[([PACKET], 6)])
        calls = []

        def current_proof():
            calls.append(True)
            if len(calls) == 1:
                return proof()
            if len(calls) == 2:
                raise BorrowNativeBuffering("temporary buffering")
            return proof(identity=IDENTITY[:-1] + ("replaced-profile",))

        reader = ReadOnlyTsBufferReader(buffer, binding(), current_proof, poll_interval=0.001)
        with self.assertRaises(BorrowBindingLost):
            next(reader.iter_chunks())
        self.assertEqual(len(calls), 3)

    def test_buffering_grace_is_bounded_and_does_not_reuse_a_stale_proof(self):
        buffer = task_local_buffer(index=5, batches=[([PACKET], 6)])
        now = [0.0]

        class FakeStop:
            def wait(self, delay):
                now[0] += delay
                return False

        reader = ReadOnlyTsBufferReader(
            buffer, binding(), lambda: (_ for _ in ()).throw(BorrowNativeBuffering("buffering")),
            poll_interval=0.5,
        )
        with patch("catchuparr.adapters.sharing_reader.time.monotonic", side_effect=lambda: now[0]):
            with self.assertRaisesRegex(BorrowBindingLost, "did not recover"):
                reader._verify(FakeStop())
        self.assertEqual(now[0], 3.0)
        self.assertEqual(buffer.read_calls, [])

    def test_reader_stop_interrupts_buffering_wait_without_publishing(self):
        buffer = task_local_buffer(index=5, batches=[([PACKET], 6)])
        waiting = threading.Event()
        stop = threading.Event()
        outcomes = []

        def current_proof():
            waiting.set()
            raise BorrowNativeBuffering("temporary buffering")

        reader = ReadOnlyTsBufferReader(buffer, binding(), current_proof, poll_interval=0.1)

        def consume():
            try:
                outcomes.append(next(reader.iter_chunks(stop)))
            except StopIteration:
                outcomes.append("stopped")

        thread = threading.Thread(target=consume)
        thread.start()
        self.assertTrue(waiting.wait(1))
        stop.set()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(outcomes, ["stopped"])
        self.assertEqual(buffer.read_calls, [])

    def test_reader_skips_expired_chunks_using_only_read_lookup(self):
        buffer = task_local_buffer(index=9, batches=[([], 5), ([PACKET], 10)])
        buffer.redis_client.head = 9
        buffer.find_oldest_available_chunk = lambda cursor: 9
        reader = ReadOnlyTsBufferReader(buffer, binding(), lambda: proof())
        reader.local_index = 5
        self.assertEqual(PACKET, next(reader.iter_chunks()))
        self.assertEqual([5, 9], buffer.read_calls)
        self.assertEqual(10, reader.local_index)

    def test_reader_refreshes_only_its_private_buffer_head(self):
        redis = FakeRedis(12)
        buffer = make_task_local_input_buffer(
            type("TaskBuffer", (FakeBuffer,), {
                "__init__": lambda self, channel_id=None, redis_client=None: FakeBuffer.__init__(
                    self, index=5, batches=[([PACKET], 13)], redis_client=redis_client
                )
            }),
            "channel-7",
            redis,
        )
        shared_buffer = FakeBuffer(index=5)
        reader = ReadOnlyTsBufferReader(buffer, binding(), lambda: proof())
        self.assertEqual(PACKET, next(reader.iter_chunks()))
        self.assertEqual(["live:channel:channel-7:input:buffer:index"], redis.reads)
        self.assertEqual(12, buffer.index)
        self.assertEqual(5, shared_buffer.index)

    def test_reader_rejects_non_transport_stream_chunks(self):
        buffer = task_local_buffer(index=5, batches=[([b"not-ts"], 6)])
        reader = ReadOnlyTsBufferReader(buffer, binding(), lambda: proof())
        with self.assertRaises(BorrowReaderUnavailable):
            next(reader.iter_chunks())

    def test_redis_tls_security_options_are_bound_but_tuning_may_differ(self):
        def ssl_pool(**overrides):
            result = FakeRedis(decode_responses=True, backend={
                "ssl": True, "ssl_cert_reqs": "required", "ssl_validate_ocsp": False,
                "ssl_validate_ocsp_stapled": False, "ssl_ciphers": "DEFAULT",
                "ssl_ca_path": "/synthetic/ca",
            })
            result.connection_pool.connection_class = SSLConnection
            result.connection_pool.connection_kwargs.update(overrides)
            return result

        baseline = ssl_pool()
        for option, value in (("ssl_validate_ocsp", True), ("ssl_validate_ocsp_stapled", True),
                              ("ssl_ciphers", "HIGH"), ("ssl_ca_path", "/synthetic/other-ca")):
            with self.subTest(option=option):
                self.assertNotEqual(redis_backend_signature(baseline),
                                    redis_backend_signature(ssl_pool(**{option: value})))
        tuned = ssl_pool(socket_timeout=0.25, health_check_interval=10,
                         socket_keepalive=True, decode_responses=False)
        self.assertEqual(redis_backend_signature(baseline), redis_backend_signature(tuned))
        with self.assertRaises(BorrowReaderUnavailable) as captured:
            redis_backend_signature(ssl_pool(credential_provider=object()))
        self.assertNotIn("synthetic-secret", str(captured.exception))
        with self.assertRaises(BorrowReaderUnavailable):
            redis_backend_signature(ssl_pool(ssl_unknown_security_option=True))

    def test_client_registration_cleans_only_its_exact_client_and_private_marker(self):
        manager = FakeClientManager()
        manager.clients.add("ordinary-viewer")
        events = []
        registration = BorrowedClientRegistration(
            manager,
            binding(),
            lambda: self.registration_proof(manager),
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
            register_private_marker=lambda client_id, epoch: events.append(("mark", client_id, epoch)) or True,
            remove_private_marker=lambda client_id, epoch: events.append(("unmark", client_id, epoch)) or True,
            activity_writer=self.activity_writer,
        )
        reader = registration.open(task_local_buffer(redis_client=manager.redis_client))
        self.assertIsInstance(reader, ReadOnlyTsBufferReader)
        self.assertEqual({"ordinary-viewer", CLIENT_ID}, manager.clients)
        self.assertEqual(
            (CLIENT_ID, "192.0.2.10", "Synthetic recorder", None, "mpegts", None),
            manager.add_calls[0],
        )
        registration.close()
        registration.close()
        self.assertEqual(["ordinary-viewer"], sorted(manager.clients))
        self.assertEqual([], manager.remove_calls)
        self.assertEqual([("mark", CLIENT_ID, "100.0"), ("unmark", CLIENT_ID, "100.0")], events)
        self.assertEqual(0, manager.stop_calls)

    def test_client_manager_must_match_the_bound_native_channel_namespace(self):
        manager = FakeClientManager(channel_id="channel-99")
        with self.assertRaises(BorrowReaderUnavailable):
            BorrowedClientRegistration(
                manager,
                binding(),
                lambda: self.registration_proof(manager),
                client_id=CLIENT_ID,
                client_ip="192.0.2.10",
                user_agent="Synthetic recorder",
                activity_writer=self.activity_writer,
            )

    def test_stale_proof_prevents_client_registration(self):
        manager = FakeClientManager()
        registration = BorrowedClientRegistration(
            manager,
            binding(),
            lambda: proof(fence=12),
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
            activity_writer=self.activity_writer,
        )
        with self.assertRaises(BorrowBindingLost):
            registration.open(task_local_buffer(redis_client=manager.redis_client))
        self.assertEqual([], manager.add_calls)
        self.assertEqual([], manager.remove_calls)

    def test_recorder_client_must_be_excluded_from_ordinary_viewer_identity(self):
        manager = FakeClientManager()
        calls = []

        def current_proof():
            calls.append(True)
            if len(calls) < 2:
                return proof()
            return self.registration_proof(manager, viewers=("viewer-1", CLIENT_ID))

        registration = BorrowedClientRegistration(
            manager,
            binding(),
            current_proof,
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
            activity_writer=self.activity_writer,
        )
        with self.assertRaises(BorrowBindingLost):
            registration.open(task_local_buffer(redis_client=manager.redis_client))
        self.assertEqual(set(), manager.clients)
        self.assertEqual([], manager.remove_calls)

    def test_partial_registration_failure_rolls_back_its_client_only(self):
        class PartialManager(FakeClientManager):
            def add_client(self, client_id, client_ip, user_agent=None, user=None, output_format="mpegts", output_profile_id=None):
                self._write_registration(client_id, client_ip, user_agent, output_format, output_profile_id)
                raise RuntimeError("synthetic add failure")

        manager = PartialManager()
        manager.clients.add("ordinary-viewer")
        registration = BorrowedClientRegistration(
            manager,
            binding(),
            lambda: proof(),
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
            activity_writer=self.activity_writer,
        )
        with self.assertRaises(BorrowReaderUnavailable):
            registration.open(task_local_buffer(redis_client=manager.redis_client))
        self.assertEqual({"ordinary-viewer"}, manager.clients)
        self.assertEqual([], manager.remove_calls)

    def test_owned_task_local_manager_stops_after_exact_client_cleanup(self):
        manager = FakeClientManager()
        registration = BorrowedClientRegistration(
            manager,
            binding(),
            lambda: proof(),
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
            owns_client_manager=True,
            activity_writer=self.activity_writer,
        )
        # Proof becomes current after the native client is inserted.
        registration.proof_reader = lambda: self.registration_proof(manager)
        registration.open(task_local_buffer(redis_client=manager.redis_client))
        registration.close()
        self.assertEqual([], manager.remove_calls)
        self.assertEqual(1, manager.stop_calls)

    def test_native_last_active_is_refreshed_from_real_publication_beyond_ghost_timeout(self):
        manager = FakeClientManager()
        now = [100.0]
        writes = []

        def proof_reader():
            return self.registration_proof(manager, now=now[0])

        def write_activity(bound, client_id, native_proof, fields):
            writes.append((bound, client_id, native_proof.checked_at, dict(fields)))
            return native_proof.borrowed_client_current

        registration = BorrowedClientRegistration(
            manager,
            binding(),
            proof_reader,
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
            activity_writer=write_activity,
        )
        registration.open(task_local_buffer(redis_client=manager.redis_client))
        for timestamp in (100.0, 125.0, 151.0):
            now[0] = timestamp
            registration.record_published_activity()
        self.assertEqual([100.0, 125.0, 151.0], [row[2] for row in writes])
        self.assertEqual("viewer-worker-7", writes[0][3]["worker_id"])
        self.assertEqual("", writes[0][3]["output_profile_id"])
        registration.close()

    def test_missing_native_registration_or_changed_owner_prevents_publication(self):
        manager = FakeClientManager()
        writes = []
        registration = BorrowedClientRegistration(
            manager,
            binding(),
            lambda: self.registration_proof(manager),
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
            activity_writer=lambda *args: writes.append(args) or True,
        )
        registration.open(task_local_buffer(redis_client=manager.redis_client))
        manager.redis_client.srem(manager.client_set_key, CLIENT_ID)
        with self.assertRaises(BorrowBindingLost):
            registration.record_published_activity()
        self.assertEqual([], writes)
        registration.close()

    def test_reused_client_id_with_changed_registration_record_is_rejected(self):
        manager = FakeClientManager()
        registration = BorrowedClientRegistration(
            manager,
            binding(),
            lambda: self.registration_proof(manager),
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
            activity_writer=self.activity_writer,
        )
        registration.open(task_local_buffer(redis_client=manager.redis_client))
        manager.redis_client.hset(
            f"live:channel:{manager.channel_id}:clients:{CLIENT_ID}",
            mapping={"connected_at": "101.0"},
        )
        with self.assertRaises(BorrowBindingLost):
            registration.record_published_activity()
        registration.close()

    def test_add_client_false_after_redis_write_is_cleaned_without_owned_manager_stop(self):
        manager = FakeClientManager(add_result=False)
        registration = BorrowedClientRegistration(
            manager,
            binding(),
            lambda: self.registration_proof(manager),
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
            owns_client_manager=True,
            activity_writer=self.activity_writer,
        )
        with self.assertRaises(BorrowReaderUnavailable):
            registration.open(task_local_buffer(redis_client=manager.redis_client))
        self.assertFalse(manager.redis_client.exists(f"live:channel:{manager.channel_id}:clients:{CLIENT_ID}"))
        self.assertFalse(manager.redis_client.sismember(manager.client_set_key, CLIENT_ID))
        self.assertEqual(1, manager.stop_calls)

    def test_partial_add_cleanup_failure_remains_open_for_exact_retry(self):
        manager = FakeClientManager(add_result="raise")
        manager.redis_client.fail_remove = True
        registration = BorrowedClientRegistration(
            manager,
            binding(),
            lambda: proof(),
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
            owns_client_manager=True,
            activity_writer=self.activity_writer,
        )
        with self.assertRaises(BorrowReaderUnavailable):
            registration.open(task_local_buffer(redis_client=manager.redis_client))
        self.assertTrue(manager.redis_client.exists(f"live:channel:{manager.channel_id}:clients:{CLIENT_ID}"))
        self.assertEqual(0, manager.stop_calls)
        manager.redis_client.fail_remove = False
        registration.close()
        self.assertFalse(manager.redis_client.exists(f"live:channel:{manager.channel_id}:clients:{CLIENT_ID}"))
        self.assertFalse(manager.redis_client.sismember(manager.client_set_key, CLIENT_ID))
        self.assertEqual(1, manager.stop_calls)

    def test_each_native_registration_write_failure_is_exactly_cleaned(self):
        for stage in ("hset", "expire_hash", "sadd", "expire_set"):
            with self.subTest(stage=stage):
                manager = FakeClientManager()
                manager.add_failure_stage = stage
                registration = BorrowedClientRegistration(
                    manager, binding(), lambda: proof(), client_id=CLIENT_ID,
                    client_ip="192.0.2.10", user_agent="Synthetic recorder",
                    owns_client_manager=True, activity_writer=self.activity_writer,
                )
                with self.assertRaises(BorrowReaderUnavailable):
                    registration.open(task_local_buffer(redis_client=manager.redis_client))
                snapshot = manager.add_failure_snapshot
                self.assertIsNotNone(snapshot)
                if stage == "expire_hash":
                    self.assertTrue(snapshot["hash_exists"])
                    self.assertFalse(snapshot["member_exists"])
                    self.assertIsNone(snapshot["hash_ttl"])
                elif stage == "sadd":
                    self.assertTrue(snapshot["hash_exists"])
                    self.assertFalse(snapshot["member_exists"])
                    self.assertEqual(60, snapshot["hash_ttl"])
                elif stage == "expire_set":
                    self.assertTrue(snapshot["hash_exists"])
                    self.assertTrue(snapshot["member_exists"])
                    self.assertEqual(60, snapshot["hash_ttl"])
                    self.assertIsNone(snapshot["set_ttl"])
                key = f"live:channel:{manager.channel_id}:clients:{CLIENT_ID}"
                self.assertFalse(manager.redis_client.exists(key))
                self.assertFalse(manager.redis_client.sismember(manager.client_set_key, CLIENT_ID))
                self.assertNotIn(CLIENT_ID, manager.clients)
                self.assertEqual(1, manager.stop_calls)

    def test_membership_only_expired_metadata_ghost_is_removed_safely(self):
        manager = FakeClientManager()
        registration = BorrowedClientRegistration(
            manager, binding(), lambda: self.registration_proof(manager),
            client_id=CLIENT_ID, client_ip="192.0.2.10", user_agent="Synthetic recorder",
            owns_client_manager=True, activity_writer=self.activity_writer,
        )
        registration.proof_reader = lambda: self.registration_proof(manager)
        registration.open(task_local_buffer(redis_client=manager.redis_client))
        client_key = f"live:channel:{manager.channel_id}:clients:{CLIENT_ID}"
        manager.redis_client.hashes.pop(client_key)
        self.assertTrue(manager.redis_client.sismember(manager.client_set_key, CLIENT_ID))
        registration.close()
        self.assertFalse(manager.redis_client.sismember(manager.client_set_key, CLIENT_ID))
        self.assertFalse(manager.redis_client.exists(client_key))
        self.assertEqual(1, manager.stop_calls)

    def test_stop_event_between_batch_yields_suppresses_remaining_chunks(self):
        manager = FakeClientManager()
        stop = threading.Event()
        buffer = task_local_buffer(index=5, batches=[([PACKET, PACKET], 7)], redis_client=manager.redis_client)
        reader = ReadOnlyTsBufferReader(buffer, binding(), lambda: proof())
        iterator = reader.iter_chunks(stop)
        self.assertEqual(PACKET, next(iterator))
        stop.set()
        with self.assertRaises(StopIteration):
            next(iterator)

    def test_activity_guard_uses_atomic_exact_client_owner_and_lease_checks(self):
        redis = FakeRedis(decode_responses=True)
        redis.sets["live:channel:channel-7:clients"] = {CLIENT_ID}
        redis.hashes[f"live:channel:channel-7:clients:{CLIENT_ID}"] = {
            "connected_at": "100.0", "worker_id": "viewer-worker-7", "user_agent": "Synthetic recorder",
            "ip_address": "192.0.2.10", "user_id": "0", "output_format": "mpegts", "output_profile_id": "",
        }
        redis.values.update({
            "live:channel:channel-7:owner": "owner-lock-7",
            "live:worker:owner-lock-7:heartbeat": "alive",
            "catchuparr:recorder:channel-7": "11:recorder-owner",
        })
        redis.hashes["live:channel:channel-7:metadata"] = {
            "owner": "owner-lock-7", "stream_id": "stream-7", "m3u_profile": "m3u-profile-7",
            "stream_profile": "input-profile-7", "state": "active",
        }
        generations = []
        guard = NativeClientActivityGuard(
            redis,
            binding(),
            recorder_lease_value="11:recorder-owner",
            client_record_ttl=60,
            generation_is_current=lambda current: generations.append(current) or True,
        )
        client_fields = {
            "connected_at": "100.0",
            "worker_id": "viewer-worker-7",
            "user_agent": "Synthetic recorder",
            "ip_address": "192.0.2.10",
            "user_id": "0",
            "output_format": "mpegts",
            "output_profile_id": "",
        }
        self.assertTrue(
            guard(
                binding(),
                CLIENT_ID,
                proof(borrowed_client_current=True),
                client_fields,
            )
        )
        script, numkeys, args = redis.eval_call
        self.assertEqual(8, numkeys)
        self.assertEqual(
            (
                "live:channel:channel-7:clients",
                "live:channel:channel-7:clients:catchuparr-client-synthetic-7",
                "live:channel:channel-7:owner",
                "live:channel:channel-7:metadata",
                "live:worker:owner-lock-7:heartbeat",
                "catchuparr:recorder:channel-7",
                "live:channel:channel-7:client:catchuparr-client-synthetic-7:stop",
                "live:channel:channel-7:stopping",
            ),
            args[:8],
        )
        self.assertIn("SISMEMBER", script)
        self.assertIn("EXISTS', KEYS[2]", script)
        self.assertIn("HSET', KEYS[2], 'last_active'", script)
        self.assertNotIn("'url'", script.lower())
        self.assertNotIn("'token'", script.lower())
        self.assertEqual([binding()], generations)

    def test_activity_guard_rejects_stale_generation_without_touching_redis(self):
        redis = FakeRedis()
        guard = NativeClientActivityGuard(
            redis,
            binding(),
            recorder_lease_value="11:recorder-owner",
            client_record_ttl=60,
            generation_is_current=lambda _current: False,
        )
        fields = {
            "connected_at": "100.0",
            "worker_id": "viewer-worker-7",
            "user_agent": "Synthetic recorder",
            "ip_address": "192.0.2.10",
            "user_id": "0",
            "output_format": "mpegts",
            "output_profile_id": "",
        }
        self.assertFalse(
            guard(binding(), CLIENT_ID, proof(borrowed_client_current=True), fields)
        )
        self.assertFalse(hasattr(redis, "eval_call"))

    def test_activity_atomic_guard_rejects_profile_changes_and_stop_markers(self):
        client_fields = {
            "connected_at": "100.0", "worker_id": "viewer-worker-7", "user_agent": "Synthetic recorder",
            "ip_address": "192.0.2.10", "user_id": "0", "output_format": "mpegts", "output_profile_id": "",
        }
        variants = (
            ("metadata", "m3u_profile", "other-m3u"),
            ("metadata", "stream_profile", "other-input"),
            ("value", "live:channel:channel-7:client:catchuparr-client-synthetic-7:stop", "stop"),
            ("value", "live:channel:channel-7:stopping", "stop"),
        )
        for kind, key, value in variants:
            redis = FakeRedis(decode_responses=True)
            redis.sets["live:channel:channel-7:clients"] = {CLIENT_ID}
            redis.hashes[f"live:channel:channel-7:clients:{CLIENT_ID}"] = dict(client_fields)
            redis.hashes["live:channel:channel-7:metadata"] = {
                "owner": "owner-lock-7", "stream_id": "stream-7", "m3u_profile": "m3u-profile-7",
                "stream_profile": "input-profile-7", "state": "active",
            }
            redis.values.update({"live:channel:channel-7:owner": "owner-lock-7",
                                 "live:worker:owner-lock-7:heartbeat": "alive",
                                 "catchuparr:recorder:channel-7": "11:recorder-owner"})
            destination = redis.hashes["live:channel:channel-7:metadata"] if kind == "metadata" else redis.values
            destination[key] = value
            guard = NativeClientActivityGuard(
                redis, binding(), recorder_lease_value="11:recorder-owner", client_record_ttl=60,
                generation_is_current=lambda _current: True,
            )
            self.assertFalse(guard(binding(), CLIENT_ID, proof(borrowed_client_current=True), client_fields))
            self.assertNotIn("last_active", redis.hashes[f"live:channel:channel-7:clients:{CLIENT_ID}"])

    def test_failed_cleanup_keeps_private_marker_and_owned_manager_for_retry(self):
        class FailingOnceManager(FakeClientManager):
            fail_remove = True


        manager = FailingOnceManager()
        manager.redis_client.fail_remove = True
        events = []
        registration = BorrowedClientRegistration(
            manager,
            binding(),
            lambda: proof(),
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
            owns_client_manager=True,
            register_private_marker=lambda client_id, epoch: events.append(("mark", client_id, epoch)) or True,
            remove_private_marker=lambda client_id, epoch: events.append(("unmark", client_id, epoch)) or True,
            activity_writer=self.activity_writer,
        )
        registration.proof_reader = lambda: self.registration_proof(manager)
        registration.open(task_local_buffer(redis_client=manager.redis_client))
        with self.assertRaises(BorrowReaderUnavailable):
            registration.close()
        self.assertIn(CLIENT_ID, manager.clients)
        self.assertEqual([("mark", CLIENT_ID, "100.0")], events)
        self.assertEqual(0, manager.stop_calls)
        manager.redis_client.fail_remove = False
        registration.close()
        self.assertNotIn(CLIENT_ID, manager.clients)
        self.assertEqual([("mark", CLIENT_ID, "100.0"), ("unmark", CLIENT_ID, "100.0")], events)
        self.assertEqual(1, manager.stop_calls)

    def test_cleanup_after_read_proof_is_revoked_uses_exact_registration_cas(self):
        manager = FakeClientManager()
        events = []
        registration = BorrowedClientRegistration(
            manager, binding(), lambda: self.registration_proof(manager),
            client_id=CLIENT_ID, client_ip="192.0.2.10", user_agent="Synthetic recorder",
            owns_client_manager=True,
            register_private_marker=lambda client_id, epoch: events.append(("mark", client_id, epoch)) or True,
            remove_private_marker=lambda client_id, epoch: events.append(("unmark", client_id, epoch)) or True,
            activity_writer=self.activity_writer, cleanup_clock=lambda: 123.0,
        )
        registration.proof_reader = lambda: self.registration_proof(manager)
        registration.open(task_local_buffer(redis_client=manager.redis_client))
        # The read lease is revoked. Cleanup remains authorized only by this
        # exact native registration epoch and the private marker epoch.
        registration.proof_reader = lambda: (_ for _ in ()).throw(BorrowBindingLost("revoked"))
        registration.close()
        self.assertFalse(manager.redis_client.exists(f"live:channel:{manager.channel_id}:clients:{CLIENT_ID}"))
        self.assertFalse(manager.redis_client.sismember(manager.client_set_key, CLIENT_ID))
        self.assertEqual([("mark", CLIENT_ID, "100.0"), ("unmark", CLIENT_ID, "100.0")], events)
        self.assertEqual(1, manager.stop_calls)

    def test_invalid_local_cleanup_clock_keeps_registration_for_safe_retry(self):
        manager = FakeClientManager()
        now = [float("nan")]
        registration = BorrowedClientRegistration(
            manager, binding(), lambda: self.registration_proof(manager),
            client_id=CLIENT_ID, client_ip="192.0.2.10", user_agent="Synthetic recorder",
            owns_client_manager=True, activity_writer=self.activity_writer,
            cleanup_clock=lambda: now[0],
        )
        registration.proof_reader = lambda: self.registration_proof(manager)
        registration.open(task_local_buffer(redis_client=manager.redis_client))
        with self.assertRaises(BorrowReaderUnavailable):
            registration.close()
        client_key = f"live:channel:{manager.channel_id}:clients:{CLIENT_ID}"
        self.assertTrue(manager.redis_client.exists(client_key))
        self.assertIn(CLIENT_ID, manager.clients)
        self.assertEqual(0, manager.stop_calls)
        now[0] = 124.0
        registration.proof_reader = lambda: (_ for _ in ()).throw(BorrowBindingLost("revoked"))
        registration.close()
        self.assertFalse(manager.redis_client.exists(client_key))
        self.assertEqual(1, manager.stop_calls)

    def test_owned_manager_stays_open_until_private_marker_absence_is_confirmed(self):
        manager = FakeClientManager()
        marker_results = iter((False, True))
        registration = BorrowedClientRegistration(
            manager,
            binding(),
            lambda: self.registration_proof(manager),
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
            owns_client_manager=True,
            register_private_marker=lambda _client_id, _epoch: True,
            remove_private_marker=lambda _client_id, _epoch: next(marker_results),
            activity_writer=self.activity_writer,
        )
        registration.open(task_local_buffer(redis_client=manager.redis_client))
        with self.assertRaises(BorrowReaderUnavailable):
            registration.close()
        self.assertEqual(0, manager.stop_calls)
        registration.close()
        self.assertEqual(1, manager.stop_calls)


if __name__ == "__main__":
    unittest.main()
