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
        if self.reads >= self.state_after_reads:
            return b"ACTIVE"
        return b"CONNECTING"

    def scard(self, _key):
        return 1


class RecorderMediaProbeTests(unittest.TestCase):
    def test_redis_metadata_normalizes_bytes_strings_and_enum_values(self):
        enum_value = SimpleNamespace(value="ACTIVE")
        self.assertEqual(probe._redis_text(b"owner"), "owner")
        self.assertEqual(probe._redis_text("owner"), "owner")
        self.assertEqual(probe._constant_text(enum_value), "ACTIVE")

    def test_native_active_wait_consumes_until_state_is_really_active(self):
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

        def media_chunks():
            while True:
                redis.reads += 1
                yield b"x" * 188

        owner, found_manager, found_client_manager = probe._wait_for_native_active(
            iter(media_chunks()),
            SimpleNamespace(close=lambda: None),
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
                iter(media_chunks()),
                SimpleNamespace(close=lambda: None),
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
