import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

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
    def make(self, heads, chunks=(b"\x47" * 188,)):
        heads = iter(heads)
        buffer = SimpleNamespace(
            buffer_index_key="index",
            get_optimized_client_data=lambda index: (list(chunks), index + 1),
        )
        redis_binary = SimpleNamespace(get=lambda _key: str(next(heads)).encode())
        clock = iter(range(0, 1000))
        tail = probe.ReturnTailProbe(None, redis_binary, CHANNEL, clock=lambda: next(clock))
        tail.buffer, tail.start_head = buffer, 10
        return tail

    def test_advancing_buffer_with_a_readable_chunk_passes(self):
        self.make([10, 11, 12]).require_reading(timeout=50, sleep=lambda _seconds: None)

    def test_stalled_buffer_fails(self):
        with self.assertRaises(RuntimeError):
            self.make([10] * 100).require_reading(timeout=5, sleep=lambda _seconds: None)

    def test_unreadable_chunk_fails(self):
        with self.assertRaises(RuntimeError):
            self.make([20], chunks=()).require_reading(timeout=5, sleep=lambda _seconds: None)


class ActivityTests(unittest.TestCase):
    def test_touch_refreshes_only_an_existing_client_hash(self):
        calls = []

        class Redis:
            def __init__(self, exists):
                self.exists = exists

            def eval(self, script, numkeys, key, value):
                calls.append((numkeys, key, value))
                return 1 if self.exists else 0

        tail = probe.ReturnTailProbe(Redis(True), None, CHANNEL, clock=lambda: 42.0)
        self.assertTrue(tail.touch())
        self.assertEqual(calls[-1], (1, f"live:channel:{CHANNEL}:clients:{tail.client_id}", "42.0"))
        self.assertIn("EXISTS", probe._TOUCH_EXISTING_CLIENT_LUA)
        self.assertFalse(probe.ReturnTailProbe(Redis(False), None, CHANNEL).touch())

    def test_activity_reporter_stops_when_the_client_is_gone(self):
        tail = probe.ReturnTailProbe(None, None, CHANNEL)
        with patch.object(probe, "ACTIVITY_INTERVAL_SECONDS", 0.01), \
                patch.object(tail, "touch", side_effect=[True, True, False]) as touch:
            tail._report_activity()
        self.assertEqual(touch.call_count, 3)


class CloseTests(unittest.TestCase):
    def test_close_removes_client_stops_owned_manager_and_marker_once(self):
        calls = []
        redis = _FakeRedis()
        tail = probe.ReturnTailProbe(redis, None, CHANNEL)
        tail.manager = SimpleNamespace(remove_client=lambda client: calls.append(("remove", client)),
                                       stop=lambda: calls.append(("stop",)))
        tail.owned_manager = tail.registered = tail.marked = True
        with patch("catchuparr.stats.recorder_client_removed",
                   side_effect=lambda channel, client: calls.append(("marker", client))):
            self.assertTrue(tail.close())
            self.assertTrue(tail.close())
        self.assertEqual(calls, [("remove", tail.client_id), ("stop",), ("marker", tail.client_id)])

    def test_never_started_probe_closes_cleanly(self):
        self.assertTrue(probe.ReturnTailProbe(_FakeRedis(), None, CHANNEL).close())


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
