import unittest
from pathlib import Path
from types import SimpleNamespace

from scripts import aio_return_tail_probe as probe

CHANNEL = "00000000-0000-0000-0000-000000000011"
ROOT = Path(__file__).resolve().parents[1]


class _FakeRedis:
    def __init__(self, values=None, hashes=None, sets=None):
        self.values = dict(values or {})
        self.hashes = dict(hashes or {})
        self.sets = {key: set(members) for key, members in (sets or {}).items()}

    def get(self, key):
        return self.values.get(key)

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def scan_iter(self, match, count):
        prefix = match.rstrip("*")
        return [key.encode() for key in self.values if key.startswith(prefix)]

    def sismember(self, key, member):
        return member in self.sets.get(key, set())

    def exists(self, key):
        return int(key in self.values or key in self.hashes)


class StatsDigestTests(unittest.TestCase):
    def lease_redis(self, **caps):
        values = {f"catchuparr:recorder:{CHANNEL}": "lease-1"}
        values.update({f"catchuparr:recorder:stats-cap:{digest}": value for digest, value in caps.items()})
        return _FakeRedis(values)

    def test_digest_bound_to_the_current_lease_is_found(self):
        redis = self.lease_redis(aaa=f"{CHANNEL}|lease-1", bbb=f"{CHANNEL}|lease-0",
                                 ccc="other-channel|lease-1")
        self.assertEqual(probe.recorder_stats_digest(redis, CHANNEL), "aaa")

    def test_missing_or_ambiguous_digest_fails_closed(self):
        for caps in ({}, {"aaa": f"{CHANNEL}|lease-1", "bbb": f"{CHANNEL}|lease-1"}):
            with self.subTest(caps=sorted(caps)):
                with self.assertRaises(RuntimeError):
                    probe.recorder_stats_digest(self.lease_redis(**caps), CHANNEL)

    def test_missing_lease_fails_closed(self):
        with self.assertRaises(RuntimeError):
            probe.recorder_stats_digest(_FakeRedis(), CHANNEL)


class AuthoritySnapshotTests(unittest.TestCase):
    def test_snapshot_is_order_independent_and_includes_the_operation(self):
        key = f"catchuparr:sharing:current:{CHANNEL}"
        operation = f"catchuparr:sharing:operation:current:{CHANNEL}"
        first = _FakeRedis({operation: b"op-1"}, {key: {b"revision": b"4", b"catalog": b"c"}})
        second = _FakeRedis({operation: "op-1"}, {key: {"catalog": "c", "revision": "4"}})
        self.assertEqual(probe.authority_snapshot(first, CHANNEL), probe.authority_snapshot(second, CHANNEL))
        changed = _FakeRedis({operation: "op-2"}, {key: {"catalog": "c", "revision": "4"}})
        self.assertNotEqual(probe.authority_snapshot(first, CHANNEL), probe.authority_snapshot(changed, CHANNEL))


class ReadingTests(unittest.TestCase):
    def test_new_chunks_after_the_return_pass(self):
        tail = probe.ReturnTailProbe(None, None, CHANNEL, None, clock=iter(range(1000)).__next__)
        tail.bytes_read = 100

        def arrive(_seconds):
            tail.bytes_read += 188

        tail.require_reading(timeout=50, sleep=arrive)

    def test_no_new_chunk_fails(self):
        tail = probe.ReturnTailProbe(None, None, CHANNEL, None, clock=iter(range(1000)).__next__)
        with self.assertRaises(RuntimeError):
            tail.require_reading(timeout=5, sleep=lambda _seconds: None)

    def test_consumer_error_fails_immediately(self):
        tail = probe.ReturnTailProbe(None, None, CHANNEL, None, clock=iter(range(1000)).__next__)
        tail.consumer_error = "BorrowBindingLost"
        with self.assertRaises(RuntimeError):
            tail.require_reading(timeout=50, sleep=lambda _seconds: None)

    def test_consumer_reports_activity_per_chunk(self):
        reader = SimpleNamespace(activity=0)
        reader.iter_chunks = lambda stop: iter([b"a" * 188, b"b" * 376])

        def record():
            reader.activity += 1

        reader.record_activity = record
        tail = probe.ReturnTailProbe(None, None, CHANNEL, None)
        tail.reader = reader
        tail._consume()
        self.assertEqual((tail.bytes_read, reader.activity, tail.consumer_error), (564, 2, None))


class CloseTests(unittest.TestCase):
    def test_close_stops_the_consumer_and_removes_the_exact_client(self):
        redis = _FakeRedis()
        reader = SimpleNamespace(client_id="catchuparr-client-1", closes=0)

        def close():
            reader.closes += 1
            return True

        reader.close = close
        tail = probe.ReturnTailProbe(redis, None, CHANNEL, None)
        tail.reader = reader
        self.assertTrue(tail.close())
        self.assertTrue(tail.stop.is_set())
        self.assertEqual(reader.closes, 1)
        self.assertTrue(tail.close())
        self.assertEqual(reader.closes, 1)

    def test_close_reports_a_client_that_remains(self):
        client = "catchuparr-client-1"
        redis = _FakeRedis(values={f"live:channel:{CHANNEL}:clients:{client}": "x"},
                           sets={f"live:channel:{CHANNEL}:clients": {client}})
        tail = probe.ReturnTailProbe(redis, None, CHANNEL, None)
        tail.reader = SimpleNamespace(client_id=client, close=lambda: True)
        self.assertFalse(tail.close())

    def test_never_started_probe_closes_cleanly(self):
        self.assertTrue(probe.ReturnTailProbe(_FakeRedis(), None, CHANNEL, None).close())


class WiringTests(unittest.TestCase):
    def test_integration_copies_the_probe_and_the_media_probe_uses_it(self):
        runner = (ROOT / "scripts/run_aio_integration.py").read_text()
        media = (ROOT / "scripts/aio_recorder_media.py").read_text()
        self.assertIn("scripts/aio_return_tail_probe.py", runner)
        for call in ("tail_probe.start()", 'tail_probe.require_alive("at the first B provider request")',
                     "tail_probe.require_reading(", "tail_probe.close()"):
            self.assertIn(call, media)
        # The tail client must be gone before the native A teardown wait.
        self.assertLess(media.index("tail_probe = None\n        print("),
                        media.index("def wait_for_native_a_teardown"))


if __name__ == "__main__":
    unittest.main()
