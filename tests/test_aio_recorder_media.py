import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts.aio_recorder_media import (
    _NativeReadObserver,
    _read_stream_iterator,
    _run_fresh_process_follower,
)


class NativeReadObserverTests(unittest.TestCase):
    def test_prefetched_and_equal_media_cannot_prove_new_parent_consumption(self):
        old = bytes(bytearray(b"repeated TS payload"))
        new = bytes(bytearray(old))
        self.assertIsNot(old, new)
        batches = iter([([old, old], 10), ([new], 14)])

        class Buffer:
            def get_optimized_client_data(self, _cursor):
                return next(batches)

        buffer = Buffer()
        observer = _NativeReadObserver(buffer)
        try:
            def native_iterator():
                chunks, _ = buffer.get_optimized_client_data(8)
                yield from chunks
                chunks, _ = buffer.get_optimized_client_data(13)
                yield from chunks

            parent = observer.consume(native_iterator())
            # The first read leaves an old chunk prefetched inside the native
            # iterator. Even identical newly published bytes must be consumed
            # from a batch requested past the post-disconnect head.
            self.assertEqual(next(parent), old)
            self.assertFalse(observer.consumed_after(12))
            media = _read_stream_iterator(
                parent, minimum_bytes=len(new), timeout=1,
                accept_chunk=lambda: observer.consumed_after(12),
            )
            self.assertEqual(media, new)
            self.assertEqual(observer.last_consumed_range, (13, 14))
        finally:
            observer.close()
        self.assertNotIn("get_optimized_client_data", vars(buffer))

    def test_fetch_without_actual_native_yield_does_not_count(self):
        stored = bytes(bytearray(b"same"))
        unrelated = bytes(bytearray(stored))

        class Buffer:
            def get_optimized_client_data(self, _cursor):
                return [stored], 23

        observer = _NativeReadObserver(Buffer())
        try:
            observer.read(22)
            self.assertIs(next(observer.consume(iter([unrelated]))), unrelated)
            self.assertFalse(observer.consumed_after(20))
            self.assertIs(next(observer.consume(iter([stored]))), stored)
            self.assertTrue(observer.consumed_after(20))
            self.assertFalse(observer.consumed_after(22))
        finally:
            observer.close()

    def test_no_fresh_media_fails_even_with_enough_prefetched_bytes(self):
        class Buffer:
            def get_optimized_client_data(self, _cursor):
                return [b"old media"], 8

        observer = _NativeReadObserver(Buffer())
        try:
            old, _ = observer.read(7)
            with self.assertRaisesRegex(RuntimeError, "too little media"):
                _read_stream_iterator(
                    observer.consume(iter(old)), minimum_bytes=1, timeout=1,
                    accept_chunk=lambda: observer.consumed_after(8),
                )
        finally:
            observer.close()


class FreshProcessFollowerTests(unittest.TestCase):
    def test_capability_only_travels_in_stdin_with_sanitized_failure(self):
        token = "synthetic-private-capability"
        attempt = SimpleNamespace(capability=token, worker_id="synthetic-worker", config_generation="1")
        with patch("scripts.aio_recorder_media.subprocess.run") as run:
            run.return_value = SimpleNamespace(returncode=1, stdout=token.encode())
            with self.assertRaisesRegex(RuntimeError, "did not attach and release cleanly") as failure:
                _run_fresh_process_follower(
                    attempt, channel_uuid="synthetic-channel", stream_id="2", account_id="3",
                    profile_id=4, expected_profile_count=1, ffprobe="ffprobe",
                    native_owner=b"synthetic-owner", parent_client_count=1,
                )
        arguments, options = run.call_args
        self.assertNotIn(token, str(arguments))
        self.assertNotIn("env", options)
        self.assertEqual(options["timeout"], 45)
        child = options["input"].decode()
        self.assertIn(token, child)
        self.assertIn("import os\n", child)
        self.assertIn("redis_client.scard(RedisKeys.clients(worker_id)) == 2", child)
        self.assertIn("redis_client.scard(RedisKeys.clients(worker_id)) == 1", child)
        self.assertIn("worker_id not in proxy_server.stream_managers", child)
        compile(child, "<synthetic-follower>", "exec")
        self.assertNotIn(token, str(failure.exception))
