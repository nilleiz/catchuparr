import tempfile
import unittest
from pathlib import Path

from catchuparr.engine import ArchiveStore
from catchuparr.engine.leases import RedisRecorderLease


class FakeRedis:
    def __init__(self):
        self.values = {}

    def eval(self, script, numkeys, *args):
        keys = args[:numkeys]
        argv = args[numkeys:]
        if "local floor" in script:
            lock_key, counter_key = keys
            owner, floor, _ttl = argv
            current = int(self.values.get(counter_key, 0))
            if current < int(floor):
                current = int(floor)
                self.values[counter_key] = current
            current += 1
            self.values[counter_key] = current
            if lock_key in self.values:
                return 0
            self.values[lock_key] = f"{current}:{owner}"
            return current
        if "EXPIRE" in script:
            key = keys[0]
            expected = argv[0]
            return int(self.values.get(key) == expected)
        if "DEL" in script:
            key = keys[0]
            expected = argv[0]
            if self.values.get(key) == expected:
                del self.values[key]
                return 1
            return 0
        raise AssertionError("unexpected Redis Lua script")


class RecorderLeaseTests(unittest.TestCase):
    def test_fence_recovers_above_durable_store_after_redis_state_loss(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ArchiveStore(Path(tmp) / "archive")
            store.register_recorder_fence("ch", 25)
            redis = FakeRedis()
            first = RedisRecorderLease(redis, "ch", archive_store=store)
            self.assertEqual(26, first.acquire())
            self.assertEqual(26, store.recorder_fence("ch"))

            # Model a Redis restart that lost both the lease and its counter.
            redis.values.clear()
            second = RedisRecorderLease(redis, "ch", archive_store=store)
            self.assertEqual(27, second.acquire())
            self.assertFalse(first.renew())
            self.assertEqual(27, store.recorder_fence("ch"))


if __name__ == "__main__":
    unittest.main()
