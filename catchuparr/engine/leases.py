"""Optional Redis-backed recorder ownership lease (no redis dependency)."""

from __future__ import annotations

import uuid


class RedisRecorderLease:
    """A Redis lease with a monotonically increasing fencing token.

    Pass a redis-py compatible client. Callers should include ``fence`` in
    recorder ownership checks before publishing results, so an expired former
    leader cannot continue writing after another worker acquires the lease.
    """

    _ACQUIRE = """
    local current = tonumber(redis.call('GET', KEYS[2]) or '0')
    local floor = tonumber(ARGV[2])
    if current < floor then
        redis.call('SET', KEYS[2], floor)
    end
    local fence = redis.call('INCR', KEYS[2])
    local value = tostring(fence) .. ':' .. ARGV[1]
    if redis.call('SET', KEYS[1], value, 'NX', 'EX', ARGV[3]) then return fence end
    return 0
    """
    _RENEW = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('EXPIRE', KEYS[1], ARGV[2]) else return 0 end"
    _RELEASE = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"

    def __init__(
        self, redis_client, channel_id: str, *, ttl_seconds: int = 30,
        prefix: str = "catchuparr:recorder", archive_store=None,
    ):
        if ttl_seconds < 2:
            raise ValueError("ttl_seconds must be at least 2")
        self.redis, self.ttl = redis_client, ttl_seconds
        self.archive_store, self.channel_id = archive_store, str(channel_id)
        self.key = f"{prefix}:{channel_id}"
        self.counter_key = f"{self.key}:fence"
        self.owner = uuid.uuid4().hex
        self.fence: int | None = None
        self._value: str | None = None

    def acquire(self, *, minimum_fence: int | None = None) -> int | None:
        if minimum_fence is None:
            minimum_fence = self.archive_store.recorder_fence(self.channel_id) if self.archive_store is not None else 0
        if minimum_fence < 0:
            raise ValueError("minimum_fence cannot be negative")
        result = int(self.redis.eval(
            self._ACQUIRE, 2, self.key, self.counter_key, self.owner, minimum_fence, self.ttl
        ))
        if result <= 0:
            return None
        self.fence = result
        self._value = f"{result}:{self.owner}"
        if self.archive_store is not None:
            try:
                self.archive_store.register_recorder_fence(self.channel_id, result)
            except Exception:
                self.release()
                raise
        return result

    def renew(self) -> bool:
        if self._value is None:
            return False
        return bool(self.redis.eval(self._RENEW, 1, self.key, self._value, self.ttl))

    def release(self) -> bool:
        if self._value is None:
            return False
        released = bool(self.redis.eval(self._RELEASE, 1, self.key, self._value))
        self._value = None
        return released
