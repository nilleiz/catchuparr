import tempfile
import unittest
from pathlib import Path

from catchuparr.sharing_authority_outbox import (
    SharingAuthorityOutbox,
    SharingAuthorityTarget,
)


class FakeRedis:
    def __init__(self):
        self.hashes = {}

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))


class SharingAuthorityOutboxTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "authority.sqlite"
        self.now = [100.0]
        self.outbox = SharingAuthorityOutbox(
            self.database, clock=lambda: self.now[0],
        )
        self.redis = FakeRedis()
        self.target = SharingAuthorityTarget(
            "channel-outbox-synthetic", "config-outbox-synthetic", 4,
            "catalog-outbox-synthetic",
        )

    def tearDown(self):
        self.temp.cleanup()

    def stage(self, activation="activation-outbox-synthetic"):
        return self.outbox.stage(activation, (self.target,))[0]

    @staticmethod
    def publish(redis, event):
        key = f"catchuparr:sharing:current:{event.channel_uuid}"
        current = redis.hashes.get(key, {})
        revision = int(current.get("authority_revision", "0"))
        if revision != event.expected_revision:
            return False
        redis.hashes[key] = {
            "authority_revision": str(event.authority_revision),
            "configuration_generation": event.configuration_generation,
            "control_generation": str(event.control_generation),
            "catalog_generation": event.catalog_generation,
        }
        return True

    def drain(self, **kwargs):
        return self.outbox.drain(
            self.redis,
            read_prior=kwargs.get("read_prior", lambda _redis, _channel: None),
            publish=kwargs.get("publish", self.publish),
            invalidate=kwargs.get("invalidate", lambda _redis, _prior, _event: True),
        )

    def test_revision_advances_only_after_redis_and_episode_invalidation_ack(self):
        event = self.stage()
        result = self.drain(
            read_prior=lambda *_args: "exact-old-episode",
            invalidate=lambda *_args: False,
        )
        self.assertEqual((event.event_id,), result.pending_event_ids)
        self.assertEqual((), result.completed_event_ids)
        self.assertEqual(event, self.outbox.pending()[0])

        result = self.drain()
        self.assertEqual((event.event_id,), result.completed_event_ids)
        self.assertEqual((), result.pending_event_ids)
        second = self.outbox.stage("next-activation", (self.target,))[0]
        self.assertEqual(1, second.expected_revision)
        self.assertEqual(2, second.authority_revision)

    def test_lost_publish_reply_is_reconciled_only_from_exact_current_hash(self):
        event = self.stage()

        def publish_then_lose_reply(redis, current):
            self.publish(redis, current)
            return False

        result = self.drain(publish=publish_then_lose_reply)
        self.assertEqual((event.event_id,), result.completed_event_ids)
        self.assertEqual((), self.outbox.pending())

    def test_stale_redis_revision_does_not_acknowledge_or_overwrite(self):
        event = self.stage()
        key = f"catchuparr:sharing:current:{event.channel_uuid}"
        self.redis.hashes[key] = {
            "authority_revision": "8",
            "configuration_generation": "newer-config-synthetic",
            "control_generation": "9",
            "catalog_generation": "newer-catalog-synthetic",
        }
        result = self.drain()
        self.assertEqual((), result.completed_event_ids)
        self.assertEqual((event.event_id,), result.pending_event_ids)
        self.assertEqual("newer-config-synthetic", self.redis.hashes[key]["configuration_generation"])

    def test_cancelled_precommit_event_does_not_consume_revision(self):
        event = self.stage()
        self.assertTrue(self.outbox.cancel_activation(event.activation_id))
        self.assertEqual((), self.outbox.pending())
        next_event = self.outbox.stage("replacement-activation", (self.target,))[0]
        self.assertEqual(0, next_event.expected_revision)
        self.assertEqual(1, next_event.authority_revision)

    def test_reopened_outbox_retries_the_same_durable_event(self):
        event = self.stage()
        reopened = SharingAuthorityOutbox(self.database, clock=lambda: self.now[0])
        self.outbox = reopened
        result = self.drain()
        self.assertEqual((event.event_id,), result.completed_event_ids)

    def test_exact_prior_episode_invalidation_runs_after_publication(self):
        event = self.stage()
        seen = []

        def invalidate(redis, prior, current):
            seen.append((prior, redis.hgetall(
                f"catchuparr:sharing:current:{current.channel_uuid}"
            )["authority_revision"]))
            return True

        result = self.drain(
            read_prior=lambda _redis, _channel: "exact-old-episode",
            invalidate=invalidate,
        )
        self.assertEqual((event.event_id,), result.completed_event_ids)
        self.assertEqual([("exact-old-episode", "1")], seen)


if __name__ == "__main__":
    unittest.main()
