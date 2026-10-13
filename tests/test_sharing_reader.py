import unittest

from catchuparr.adapters.sharing_reader import (
    BorrowBindingLost,
    BorrowedClientRegistration,
    BorrowReadBinding,
    BorrowReaderUnavailable,
    BorrowReadProof,
    ReadOnlyTsBufferReader,
    make_task_local_input_buffer,
    native_read_api_supported,
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


def binding(*, deadline=None, fence=11, generation="config-a", control=3):
    return BorrowReadBinding(
        IDENTITY,
        generation,
        control,
        fence,
        pending_return_deadline=deadline,
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
        "pending_return_deadline": deadline,
    }
    values.update(overrides)
    return BorrowReadProof(**values)


class FakeRedis:
    def __init__(self, head=5):
        self.head = head
        self.reads = []

    def get(self, key):
        self.reads.append(key)
        return str(self.head).encode()


class FakeBuffer:
    def __init__(self, index=5, batches=(), redis_client=None):
        self.index = index
        self.redis_client = redis_client
        self.buffer_index_key = "synthetic:buffer:index"
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


def task_local_buffer(index=5, batches=()):
    class Buffer(FakeBuffer):
        def __init__(self, channel_id=None, redis_client=None):
            super().__init__(index=index, batches=batches, redis_client=redis_client)

    return make_task_local_input_buffer(Buffer, "channel-7", FakeRedis(index))


class FakeClientManager:
    def __init__(self, *, add_result=None):
        self.clients = set()
        self.add_result = add_result
        self.add_calls = []
        self.remove_calls = []
        self.stop_calls = 0

    def add_client(
        self, client_id, client_ip, user_agent=None, user=None,
        output_format="mpegts", output_profile_id=None,
    ):
        self.add_calls.append((client_id, client_ip, user_agent, user, output_format, output_profile_id))
        if self.add_result is False:
            return False
        self.clients.add(client_id)
        return self.add_result if self.add_result is not None else len(self.clients)

    def remove_client(self, client_id):
        self.remove_calls.append(client_id)
        self.clients.discard(client_id)
        return len(self.clients)

    def stop(self):
        self.stop_calls += 1


class SharingReaderTests(unittest.TestCase):
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
        self.assertEqual(["synthetic:buffer:index"], redis.reads)
        self.assertEqual(12, buffer.index)
        self.assertEqual(5, shared_buffer.index)

    def test_reader_rejects_non_transport_stream_chunks(self):
        buffer = task_local_buffer(index=5, batches=[([b"not-ts"], 6)])
        reader = ReadOnlyTsBufferReader(buffer, binding(), lambda: proof())
        with self.assertRaises(BorrowReaderUnavailable):
            next(reader.iter_chunks())

    def test_client_registration_cleans_only_its_exact_client_and_private_marker(self):
        manager = FakeClientManager()
        manager.clients.add("ordinary-viewer")
        events = []
        registration = BorrowedClientRegistration(
            manager,
            binding(),
            lambda: proof(),
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
            register_private_marker=lambda client_id: events.append(("mark", client_id)),
            remove_private_marker=lambda client_id: events.append(("unmark", client_id)),
        )
        reader = registration.open(task_local_buffer())
        self.assertIsInstance(reader, ReadOnlyTsBufferReader)
        self.assertEqual({"ordinary-viewer", CLIENT_ID}, manager.clients)
        self.assertEqual(
            (CLIENT_ID, "192.0.2.10", "Synthetic recorder", None, "mpegts", None),
            manager.add_calls[0],
        )
        registration.close()
        registration.close()
        self.assertEqual(["ordinary-viewer"], sorted(manager.clients))
        self.assertEqual([CLIENT_ID], manager.remove_calls)
        self.assertEqual([("mark", CLIENT_ID), ("unmark", CLIENT_ID)], events)
        self.assertEqual(0, manager.stop_calls)

    def test_stale_proof_prevents_client_registration(self):
        manager = FakeClientManager()
        registration = BorrowedClientRegistration(
            manager,
            binding(),
            lambda: proof(fence=12),
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
        )
        with self.assertRaises(BorrowBindingLost):
            registration.open(task_local_buffer())
        self.assertEqual([], manager.add_calls)
        self.assertEqual([], manager.remove_calls)

    def test_recorder_client_must_be_excluded_from_ordinary_viewer_identity(self):
        manager = FakeClientManager()
        calls = []

        def current_proof():
            calls.append(True)
            if len(calls) < 2:
                return proof()
            return proof(viewers=("viewer-1", CLIENT_ID))

        registration = BorrowedClientRegistration(
            manager,
            binding(),
            current_proof,
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
        )
        with self.assertRaises(BorrowBindingLost):
            registration.open(task_local_buffer())
        self.assertEqual(set(), manager.clients)
        self.assertEqual([CLIENT_ID], manager.remove_calls)

    def test_partial_registration_failure_rolls_back_its_client_only(self):
        class PartialManager(FakeClientManager):
            def add_client(self, client_id, client_ip, user_agent=None, user=None, output_format="mpegts", output_profile_id=None):
                self.clients.add(client_id)
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
        )
        with self.assertRaises(BorrowReaderUnavailable):
            registration.open(task_local_buffer())
        self.assertEqual({"ordinary-viewer"}, manager.clients)
        self.assertEqual([CLIENT_ID], manager.remove_calls)

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
        )
        registration.open(task_local_buffer())
        registration.close()
        self.assertEqual([CLIENT_ID], manager.remove_calls)
        self.assertEqual(1, manager.stop_calls)

    def test_failed_cleanup_keeps_private_marker_and_owned_manager_for_retry(self):
        class FailingOnceManager(FakeClientManager):
            fail_remove = True

            def remove_client(self, client_id):
                self.remove_calls.append(client_id)
                if self.fail_remove:
                    self.fail_remove = False
                    raise RuntimeError("synthetic remove failure")
                return super().remove_client(client_id)

        manager = FailingOnceManager()
        events = []
        registration = BorrowedClientRegistration(
            manager,
            binding(),
            lambda: proof(),
            client_id=CLIENT_ID,
            client_ip="192.0.2.10",
            user_agent="Synthetic recorder",
            owns_client_manager=True,
            register_private_marker=lambda client_id: events.append(("mark", client_id)),
            remove_private_marker=lambda client_id: events.append(("unmark", client_id)),
        )
        registration.open(task_local_buffer())
        with self.assertRaises(BorrowReaderUnavailable):
            registration.close()
        self.assertIn(CLIENT_ID, manager.clients)
        self.assertEqual([("mark", CLIENT_ID)], events)
        self.assertEqual(0, manager.stop_calls)
        registration.close()
        self.assertNotIn(CLIENT_ID, manager.clients)
        self.assertEqual([("mark", CLIENT_ID), ("unmark", CLIENT_ID)], events)
        self.assertEqual(1, manager.stop_calls)


if __name__ == "__main__":
    unittest.main()
