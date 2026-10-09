import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts import aio_recorder_media as probe


class _FakeBuffer:
    def __init__(self, chunks, next_index):
        self.chunks = chunks
        self.next_index = next_index

    def get_optimized_client_data(self, _client_index):
        return self.chunks, self.next_index


class _FakeRedis:
    def __init__(self, state_after_reads):
        self.state_after_reads = state_after_reads
        self.reads = 0

    def get(self, _key):
        return b"native-owner"

    def hget(self, _key, _field):
        self.reads += 1
        if self.reads >= self.state_after_reads:
            return b"ACTIVE"
        return b"CONNECTING"

    def scard(self, _key):
        return 1


class RecorderMediaProbeTests(unittest.TestCase):
    def test_live_a_invariant_diagnostic_reports_each_boolean_without_values(self):
        all_true = dict.fromkeys(probe._NATIVE_LIVE_A_INVARIANT_NAMES, True)
        self.assertTrue(all(probe._native_live_a_invariant_flags(**all_true).values()))

        for failed_name in probe._NATIVE_LIVE_A_INVARIANT_NAMES:
            observations = dict(all_true)
            observations[failed_name] = False
            flags = probe._native_live_a_invariant_flags(**observations)
            self.assertEqual(
                [name for name, passed in flags.items() if not passed],
                [failed_name],
            )
            message = probe._native_live_a_invariant_message(
                "after_pause_private_cleanup", flags,
            )
            self.assertIn("after_pause_private_cleanup", message)
            self.assertIn(f'"{failed_name}": false', message)
            self.assertNotIn("synthetic-owner-value", message)

    def test_live_route_tone_assertion_requires_expected_source(self):
        probe._assert_tone_matches(441, 440, "Synthetic live A")
        with self.assertRaisesRegex(RuntimeError, "unexpected synthetic source tone"):
            probe._assert_tone_matches(880, 440, "Synthetic live A")
        with self.assertRaisesRegex(RuntimeError, "no decodable synthetic audio tone"):
            probe._assert_tone_matches(None, 440, "Synthetic live A")

    def test_native_source_metadata_snapshot_ignores_health_fields(self):
        class MetadataRedis:
            @staticmethod
            def hgetall(_key):
                return {
                    b"stream_id": b"23",
                    b"stream_name": b"Synthetic isolation source A",
                    b"url": b"http://127.0.0.1:12345/source-a.ts",
                    b"state": b"ACTIVE",
                    b"client_count": b"1",
                }

        metadata = probe._native_source_metadata(MetadataRedis(), "metadata")
        self.assertEqual(
            metadata,
            {
                "stream_id": "23",
                "stream_name": "Synthetic isolation source A",
                "url": "http://127.0.0.1:12345/source-a.ts",
            },
        )

    def test_native_live_reader_returns_only_chunks_after_publication_floor(self):
        class Tracker:
            last_yielded_index = None

        tracker = Tracker()

        def chunks():
            for index in range(1, 5):
                tracker.last_yielded_index = index
                yield bytes((index,)) * 188

        reader = probe._NativeLiveMediaReader(iter(chunks()), tracker)
        try:
            media, last_index = reader.read_after(2, minimum_bytes=376, timeout=1)
        finally:
            reader.thread.join(timeout=1)
        self.assertEqual(last_index, 4)
        self.assertEqual(media, bytes((3,)) * 188 + bytes((4,)) * 188)

    def test_live_reader_stops_at_iterator_boundary_before_response_close(self):
        class Tracker:
            last_yielded_index = 0

        tracker = Tracker()

        class SlowIterator:
            def __init__(self):
                self.lock = threading.Lock()
                self.active = False

            def __iter__(self):
                return self

            def __next__(self):
                with self.lock:
                    self.active = True
                time.sleep(0.02)
                with self.lock:
                    self.active = False
                tracker.last_yielded_index += 1
                return b"x" * 188

        iterator = SlowIterator()
        reader = probe._NativeLiveMediaReader(iterator, tracker)

        class Response:
            closed = False

            def close(self):
                with iterator.lock:
                    self.assert_idle = not iterator.active
                self.assert_not_reading = not reader.thread.is_alive()
                self.closed = True

        response = Response()
        force_calls = []
        reader.close(response, force_release=lambda: force_calls.append(True), join_timeout=1)
        self.assertTrue(response.closed)
        self.assertTrue(response.assert_idle)
        self.assertTrue(response.assert_not_reading)
        self.assertEqual(force_calls, [])

    def test_live_reader_session_owns_one_reader_through_idempotent_close(self):
        buffer = _FakeBuffer([b"x" * 188], next_index=1)

        class Response:
            def __init__(self):
                self.close_count = 0

            def stream(self):
                chunks, _next_index = buffer.get_optimized_client_data(0)
                yield from chunks

            @property
            def streaming_content(self):
                return self.stream()

            def close(self):
                self.close_count += 1

        response = Response()
        reader_type = probe._NativeLiveMediaReader
        constructed = []

        def construct_reader(iterator, tracker):
            reader = reader_type(iterator, tracker)
            constructed.append(reader)
            return reader

        with patch.object(
            probe, "_NativeLiveMediaReader", side_effect=construct_reader,
        ) as reader_factory:
            session = probe._NativeLiveReaderSession(response, buffer)
            self.assertEqual(reader_factory.call_count, 1)
            self.assertIs(session.reader, constructed[0])
            session.close()
            session.close()

        self.assertFalse(session.reader.thread.is_alive())
        self.assertEqual(response.close_count, 1)
        self.assertEqual(reader_factory.call_count, 1)

    def test_live_reader_session_reacquires_buffer_after_native_initialization(self):
        preinit_buffer = _FakeBuffer([], next_index=0)
        initialized_buffer = _FakeBuffer([], next_index=0)

        class NativeServer:
            def __init__(self):
                self.current_buffer = preinit_buffer

            def get_buffer(self, worker_id, *, profile):
                self.lookup = (worker_id, profile)
                return self.current_buffer

        class Response:
            streaming_content = iter(())

            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        native_server = NativeServer()
        preinit_lookup = native_server.get_buffer("synthetic-worker", profile=None)
        native_server.current_buffer = initialized_buffer
        response = Response()

        session, bound_buffer = probe._create_native_live_reader_session(
            response, native_server, "synthetic-worker",
        )
        self.assertIsNot(preinit_lookup, initialized_buffer)
        self.assertIs(bound_buffer, initialized_buffer)
        self.assertIs(session.tracker.buffer, initialized_buffer)
        self.assertEqual(native_server.lookup, ("synthetic-worker", None))
        session.close()
        self.assertTrue(response.closed)

    def test_blocked_live_reader_stops_native_channel_before_response_close(self):
        class Tracker:
            last_yielded_index = None

        class BlockingIterator:
            def __init__(self):
                self.started = threading.Event()
                self.unblock = threading.Event()

            def __iter__(self):
                return self

            def __next__(self):
                self.started.set()
                if not self.unblock.wait(1):
                    raise RuntimeError("test iterator was not released")
                raise StopIteration

        iterator = BlockingIterator()
        reader = probe._NativeLiveMediaReader(iterator, Tracker())
        self.assertTrue(iterator.started.wait(1))
        events = []

        class Response:
            def close(self):
                self.closed_after_reader_stopped = not reader.thread.is_alive()
                events.append("response-close")

        response = Response()

        def stop_native_channel():
            events.append("native-stop")
            iterator.unblock.set()

        reader.close(response, force_release=stop_native_channel, join_timeout=0.01)
        self.assertEqual(events, ["native-stop", "response-close"])
        self.assertTrue(response.closed_after_reader_stopped)
        self.assertFalse(reader.thread.is_alive())

    def test_redis_metadata_normalizes_bytes_strings_and_enum_values(self):
        enum_value = SimpleNamespace(value="ACTIVE")
        self.assertEqual(probe._redis_text(b"owner"), "owner")
        self.assertEqual(probe._redis_text("owner"), "owner")
        self.assertEqual(probe._constant_text(enum_value), "ACTIVE")

    def test_native_active_wait_polls_until_state_is_really_active(self):
        redis = _FakeRedis(state_after_reads=2)
        manager = SimpleNamespace(running=True)
        client_manager = SimpleNamespace(get_client_count=lambda: 1)
        server = SimpleNamespace(
            worker_id="native-owner",
            stream_managers={"worker": manager},
            client_managers={"worker": client_manager},
        )

        class Keys:
            @staticmethod
            def channel_owner(_worker_id):
                return "owner"

            @staticmethod
            def channel_metadata(_worker_id):
                return "metadata"

            @staticmethod
            def clients(_worker_id):
                return "clients"

        class Fields:
            STATE = "state"

        class States:
            ACTIVE = "ACTIVE"

        owner, found_manager, found_client_manager = probe._wait_for_native_active(
            redis,
            server,
            "worker",
            redis_keys=Keys,
            metadata_field=Fields,
            channel_state=States,
            timeout=1,
        )

        self.assertEqual(owner, "native-owner")
        self.assertIs(found_manager, manager)
        self.assertIs(found_client_manager, client_manager)
        self.assertEqual(redis.reads, 2)

    def test_native_client_wait_waits_for_one_registered_local_and_redis_client(self):
        local_counts = iter((0, 1))
        redis_counts = iter((0, 1))
        manager = SimpleNamespace(get_client_count=lambda: next(local_counts))
        server = SimpleNamespace(client_managers={"worker": manager})

        class ClientRedis:
            @staticmethod
            def scard(_key):
                return next(redis_counts)

        class Keys:
            @staticmethod
            def clients(_worker_id):
                return "clients"

        probe._wait_for_native_client_count(
            ClientRedis(), server, Keys, "worker", 1, timeout=1,
            reject_excess=True,
        )

    def test_native_client_wait_rejects_extra_clients_during_registration(self):
        manager = SimpleNamespace(get_client_count=lambda: 2)
        server = SimpleNamespace(client_managers={"worker": manager})

        class ClientRedis:
            @staticmethod
            def scard(_key):
                return 2

        class Keys:
            @staticmethod
            def clients(_worker_id):
                return "clients"

        with self.assertRaisesRegex(RuntimeError, "more clients"):
            probe._wait_for_native_client_count(
                ClientRedis(), server, Keys, "worker", 1, timeout=1,
                reject_excess=True,
            )

    def test_native_client_wait_allows_bounded_two_to_one_disconnect(self):
        local_counts = iter((2, 1))
        redis_counts = iter((2, 1))
        manager = SimpleNamespace(get_client_count=lambda: next(local_counts))
        server = SimpleNamespace(client_managers={"worker": manager})

        class ClientRedis:
            @staticmethod
            def scard(_key):
                return next(redis_counts)

        class Keys:
            @staticmethod
            def clients(_worker_id):
                return "clients"

        probe._wait_for_native_client_count(
            ClientRedis(), server, Keys, "worker", 1, timeout=1,
        )

    def test_native_active_wait_does_not_accept_connecting(self):
        redis = _FakeRedis(state_after_reads=10**9)
        manager = SimpleNamespace(running=True)
        client_manager = SimpleNamespace(get_client_count=lambda: 1)
        server = SimpleNamespace(
            worker_id="native-owner",
            stream_managers={"worker": manager},
            client_managers={"worker": client_manager},
        )

        class Keys:
            channel_owner = staticmethod(lambda _worker_id: "owner")
            channel_metadata = staticmethod(lambda _worker_id: "metadata")
            clients = staticmethod(lambda _worker_id: "clients")

        class Fields:
            STATE = "state"

        class States:
            ACTIVE = "ACTIVE"

        def media_chunks():
            while True:
                redis.reads += 1
                yield b"x" * 188

        with self.assertRaisesRegex(RuntimeError, "did not reach ACTIVE"):
            probe._wait_for_native_active(
                redis,
                server,
                "worker",
                redis_keys=Keys,
                metadata_field=Fields,
                channel_state=States,
                timeout=0.03,
            )

    def test_fresh_buffer_chunk_proof_skips_prefetched_equal_bytes(self):
        old_chunk = bytes(bytearray(b"same-payload"))
        new_chunk = bytes(bytearray(b"same-payload"))
        self.assertEqual(old_chunk, new_chunk)
        self.assertIsNot(old_chunk, new_chunk)
        buffer = _FakeBuffer([old_chunk, new_chunk], next_index=12)
        tracker = probe._NativeBufferYieldTracker(buffer)
        try:
            chunks, next_index = buffer.get_optimized_client_data(10)
            self.assertEqual(next_index, 12)
            iterator = tracker.observe(iter(chunks))
            publication_floor = 11
            fresh = probe._read_stream_iterator(
                iterator,
                minimum_bytes=len(new_chunk),
                timeout=1,
                accept_chunk=lambda: (
                    tracker.last_yielded_index is not None
                    and tracker.last_yielded_index > publication_floor
                ),
            )
            self.assertEqual(fresh, new_chunk)
            self.assertEqual(tracker.last_yielded_index, 12)
        finally:
            tracker.close()

    def test_unmapped_native_batch_cannot_claim_a_fresh_index(self):
        buffer = _FakeBuffer([b"one", b"two"], next_index=1)
        tracker = probe._NativeBufferYieldTracker(buffer)
        try:
            chunks, _next_index = buffer.get_optimized_client_data(0)
            iterator = tracker.observe(iter(chunks))
            self.assertEqual(next(iterator), b"one")
            self.assertIsNone(tracker.last_yielded_index)
            self.assertEqual(next(iterator), b"two")
            self.assertIsNone(tracker.last_yielded_index)
            self.assertEqual(tracker.unmapped_batches, 1)
        finally:
            tracker.close()

    def test_child_probe_is_compiled_and_capability_stays_off_command_line(self):
        secret_capability = "synthetic-private-capability"
        attempt = SimpleNamespace(
            capability=secret_capability,
            worker_id="synthetic-worker",
            config_generation="generation-1",
        )
        captured = {}

        def fake_run(args, **kwargs):
            captured["args"] = args
            captured.update(kwargs)
            return SimpleNamespace(
                returncode=0,
                stdout=b"CATCHUPARR_RECORDER_FOLLOWER_OK\n",
            )

        with patch.object(probe.subprocess, "run", side_effect=fake_run):
            probe._run_fresh_process_follower(
                attempt,
                channel_uuid="synthetic-channel",
                stream_id="synthetic-stream",
                account_id="synthetic-account",
                profile_id=7,
                expected_profile_count=3,
                ffprobe="ffprobe",
                native_owner="native-owner",
                parent_client_count=1,
            )

        child_script = captured["input"].decode("utf-8")
        compile(child_script, "<recorder-follower-probe>", "exec")
        self.assertIn("import os", child_script)
        self.assertIn("worker_id not in proxy_server.stream_managers", child_script)
        self.assertIn("ChannelState.ACTIVE", child_script)
        self.assertIn("int(redis_client.scard(clients_key) or 0) == 2", child_script)
        self.assertIn(secret_capability, child_script)
        self.assertNotIn(secret_capability, repr(captured["args"]))
        self.assertNotIn("env", captured)


if __name__ == "__main__":
    unittest.main()
