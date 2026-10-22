import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from catchuparr.adapters.sharing_native import (
    NativeEpisodeGrant,
    NativeEpisodeInvalidation,
)
from catchuparr.sharing_authority_outbox import (
    SharingAuthorityOutbox,
    SharingAuthorityPriorObservation,
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
        self.outbox.commit_activation(kwargs.get("activation_id", "activation-outbox-synthetic"))
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

        retries = []
        result = self.drain(
            read_prior=lambda *_args: self.fail("prior episode must come from durable snapshot"),
            invalidate=lambda _redis, prior, _event: retries.append(prior) or True,
        )
        self.assertEqual((event.event_id,), result.completed_event_ids)
        self.assertEqual((), result.pending_event_ids)
        self.assertEqual(["exact-old-episode"], retries)
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

    def test_claimed_event_cannot_be_cancelled_during_publication(self):
        event = self.stage()
        cancel_results = []

        def publish_during_claim(redis, current):
            cancel_results.append(self.outbox.cancel_event(current.event_id))
            return self.publish(redis, current)

        result = self.drain(publish=publish_during_claim)
        self.assertEqual([False], cancel_results)
        self.assertEqual((event.event_id,), result.completed_event_ids)
        self.assertEqual("complete", self.outbox.latest_for_channel(self.target.channel_uuid).status)
        self.assertFalse(self.outbox.cancel_activation(event.activation_id))

    def test_latest_channel_event_uses_revision_when_wall_clock_moves_back(self):
        first = self.stage("first-clock-event")
        self.assertEqual(
            (first.event_id,),
            self.drain(activation_id="first-clock-event").completed_event_ids,
        )
        self.now[0] = 90.0
        second = self.outbox.stage("second-clock-event", (self.target,))[0]
        latest = self.outbox.latest_for_channel(self.target.channel_uuid)
        self.assertEqual(second, latest.event)
        self.assertEqual(2, latest.event.authority_revision)

    def test_uncommitted_event_cannot_publish_and_can_be_cancelled(self):
        event = self.stage()
        result = self.outbox.drain(
            self.redis, read_prior=lambda *_args: None,
            publish=self.publish, invalidate=lambda *_args: True,
        )
        self.assertEqual((), result.completed_event_ids)
        self.assertEqual((event.event_id,), result.pending_event_ids)
        self.assertEqual({}, self.redis.hashes)
        self.assertTrue(self.outbox.cancel_activation(event.activation_id))
        self.assertEqual((), self.outbox.pending())

    def test_native_prior_snapshot_is_durable_without_raw_lease_and_retryable(self):
        event = self.stage()
        grant = NativeEpisodeGrant(
            channel_uuid=self.target.channel_uuid, episode_id="episode-outbox-synthetic",
            phase="active", configuration_generation="old-config-synthetic",
            control_generation=3, catalog_generation="old-catalog-synthetic",
            recorder_fence=12, intent_epoch=8, version=2,
            lease_value="synthetic-secret-lease-value", worker_id="worker-outbox-synthetic",
            owner_lock_value="owner-outbox-synthetic", metadata_owner="owner-outbox-synthetic",
            stream_id="stream-outbox-synthetic", account_id="account-outbox-synthetic",
            m3u_profile_id="m3u-outbox-synthetic", input_profile_id="input-outbox-synthetic",
            client_id="catchuparr-client-outbox-synthetic", client_worker_id="worker-outbox-synthetic",
            connected_at="12.5", return_seconds=30, minimum_switch_seconds=60,
            last_switch_at=10.0, last_viewer_left_at=None, return_deadline=None,
        )
        self.outbox.commit_activation(event.activation_id)
        calls = []
        first = self.outbox.drain(
            self.redis, read_prior=lambda *_args: SharingAuthorityPriorObservation("exact", grant),
            publish=self.publish,
            invalidate=lambda _redis, prior, _event: calls.append(prior) or False,
            encode_prior=NativeEpisodeInvalidation.from_grant,
            decode_prior=lambda value: NativeEpisodeInvalidation(**value),
        )
        self.assertEqual((event.event_id,), first.pending_event_ids)
        with self.outbox._db() as db:
            stored = db.execute(
                "SELECT prior_json,prior_observed FROM sharing_authority_outbox WHERE event_id=?",
                (event.event_id,),
            ).fetchone()
        self.assertEqual(1, stored["prior_observed"])
        self.assertNotIn("synthetic-secret-lease-value", stored["prior_json"])
        self.assertNotIn("lease_value", stored["prior_json"])
        retry = self.outbox.drain(
            self.redis,
            read_prior=lambda *_args: self.fail("stored prior episode must be reused"),
            publish=self.publish,
            invalidate=lambda _redis, prior, _event: calls.append(prior) or True,
            encode_prior=NativeEpisodeInvalidation.from_grant,
            decode_prior=lambda value: NativeEpisodeInvalidation(**value),
        )
        self.assertEqual((event.event_id,), retry.completed_event_ids)
        self.assertEqual(2, len(calls))
        self.assertTrue(all(isinstance(call, NativeEpisodeInvalidation) for call in calls))

    def test_no_prior_episode_skips_native_codec_and_completes(self):
        event = self.stage()
        self.assertTrue(self.outbox.commit_activation(event.activation_id))
        encoded = []
        result = self.outbox.drain(
            self.redis,
            read_prior=lambda *_args: None,
            publish=self.publish,
            invalidate=lambda *_args: self.fail("there is no prior episode to invalidate"),
            encode_prior=lambda prior: (
                encoded.append(prior) or NativeEpisodeInvalidation.from_grant(prior)
            ),
            decode_prior=lambda prior: NativeEpisodeInvalidation(**prior),
        )
        self.assertEqual((event.event_id,), result.completed_event_ids)
        self.assertEqual([], encoded)
        with self.outbox._db() as db:
            row = db.execute(
                "SELECT prior_observed,prior_json FROM sharing_authority_outbox WHERE event_id=?",
                (event.event_id,),
            ).fetchone()
        self.assertEqual(1, row["prior_observed"])
        self.assertIsNone(row["prior_json"])

    def test_unknown_existing_native_projection_blocks_publication_and_retry_claim(self):
        event = self.stage()
        publish = Mock(side_effect=self.publish)
        result = self.drain(
            read_prior=lambda *_args: SharingAuthorityPriorObservation("unknown"),
            publish=publish,
        )
        self.assertEqual((event.event_id,), result.pending_event_ids)
        publish.assert_not_called()
        with self.outbox._db() as db:
            row = db.execute(
                "SELECT prior_observed,prior_json FROM sharing_authority_outbox WHERE event_id=?",
                (event.event_id,),
            ).fetchone()
        self.assertEqual(0, row["prior_observed"])
        self.assertIsNone(row["prior_json"])

    def test_known_revoked_projection_needs_no_second_invalidation(self):
        event = self.stage()
        invalidation = Mock()
        result = self.drain(
            read_prior=lambda *_args: SharingAuthorityPriorObservation("known_revoked"),
            invalidate=invalidation,
        )
        self.assertEqual((event.event_id,), result.completed_event_ids)
        invalidation.assert_not_called()
        with self.outbox._db() as db:
            row = db.execute(
                "SELECT prior_observed,prior_json FROM sharing_authority_outbox WHERE event_id=?",
                (event.event_id,),
            ).fetchone()
        self.assertEqual(1, row["prior_observed"])
        self.assertIn("known_revoked", row["prior_json"])

    def test_configuration_drain_uses_native_codec_for_a_real_empty_prior_target(self):
        from catchuparr.configuration import (
            SHARING_AUTHORITY_OUTBOX_NAME,
            _drain_sharing_authority_outbox,
        )

        active_path = self.database.with_name("active-synthetic.json")
        outbox = SharingAuthorityOutbox(
            active_path.with_name(SHARING_AUTHORITY_OUTBOX_NAME),
            clock=lambda: self.now[0],
        )
        activation = "activation-config-empty-prior-synthetic"
        event = outbox.stage(activation, (self.target,))[0]
        self.assertTrue(outbox.commit_activation(activation))
        published = []
        core = types.ModuleType("core")
        core.__path__ = []
        core_utils = types.ModuleType("core.utils")
        core_utils.RedisClient = types.SimpleNamespace(get_client=lambda: self.redis)
        with (
            patch.dict(sys.modules, {"core": core, "core.utils": core_utils}),
            patch(
                "catchuparr.adapters.sharing_native.observe_native_episode_for_authority_drain",
                return_value=SharingAuthorityPriorObservation("absent"),
            ),
            patch(
                "catchuparr.adapters.sharing_native.publish_current_authority",
                side_effect=lambda _redis, channel, **kwargs: (
                    published.append((channel, kwargs)) or True
                ),
            ),
        ):
            self.assertTrue(_drain_sharing_authority_outbox(active_path, activation))
        self.assertEqual(1, len(published))
        self.assertEqual(self.target.channel_uuid, published[0][0])
        self.assertEqual(event.authority_revision, published[0][1]["authority_revision"])
        self.assertEqual("complete", outbox.latest_for_channel(self.target.channel_uuid).status)

    def test_configuration_drain_keeps_malformed_projection_pending(self):
        from catchuparr.configuration import (
            SHARING_AUTHORITY_OUTBOX_NAME,
            _drain_sharing_authority_outbox,
        )

        active_path = self.database.with_name("active-synthetic-malformed-prior.json")
        outbox = SharingAuthorityOutbox(
            active_path.with_name(SHARING_AUTHORITY_OUTBOX_NAME),
            clock=lambda: self.now[0],
        )
        activation = "activation-config-malformed-prior-synthetic"
        event = outbox.stage(activation, (self.target,))[0]
        self.assertTrue(outbox.commit_activation(activation))
        published = Mock(return_value=True)
        core = types.ModuleType("core")
        core.__path__ = []
        core_utils = types.ModuleType("core.utils")
        core_utils.RedisClient = types.SimpleNamespace(get_client=lambda: self.redis)
        with (
            patch.dict(sys.modules, {"core": core, "core.utils": core_utils}),
            patch(
                "catchuparr.adapters.sharing_native.observe_native_episode_for_authority_drain",
                return_value=SharingAuthorityPriorObservation("unknown"),
            ),
            patch(
                "catchuparr.adapters.sharing_native.publish_current_authority",
                side_effect=published,
            ),
        ):
            self.assertFalse(_drain_sharing_authority_outbox(active_path, activation))
        published.assert_not_called()
        self.assertEqual(
            (event.event_id,), tuple(item.event_id for item in outbox.pending(activation)),
        )

    def test_configuration_drain_rehydrates_latest_committed_head_before_pending_event(self):
        from catchuparr.configuration import (
            SHARING_AUTHORITY_OUTBOX_NAME,
            _drain_sharing_authority_outbox,
        )

        active_path = self.database.with_name("active-synthetic-recovery.json")
        outbox_path = active_path.with_name(SHARING_AUTHORITY_OUTBOX_NAME)
        outbox = SharingAuthorityOutbox(outbox_path, clock=lambda: self.now[0])
        # Build committed durable history through revision 3.
        for revision in range(1, 4):
            activation = f"activation-recovery-{revision}"
            event = outbox.stage(activation, (self.target,))[0]
            outbox.commit_activation(activation)
            result = outbox.drain(
                self.redis,
                read_prior=lambda *_args: SharingAuthorityPriorObservation("absent"),
                publish=self.publish,
                invalidate=lambda *_args: self.fail("an absent episode needs no invalidation"),
            )
            self.assertEqual((event.event_id,), result.completed_event_ids)

        # Stage and durably commit revision 4, then simulate loss of the
        # current-authority hash while unrelated current lease state remains.
        activation = "activation-recovery-pending"
        pending = outbox.stage(activation, (self.target,))[0]
        outbox.commit_activation(activation)
        lease_key = "catchuparr:sharing:episode:lease-preservation-synthetic"
        self.redis.hashes[lease_key] = {"phase": "active", "lease_value": "keep"}
        self.redis.hashes.pop(f"catchuparr:sharing:current:{self.target.channel_uuid}")

        calls = []

        def strict_publish(redis, channel, **kwargs):
            calls.append((kwargs["expected_revision"], kwargs["authority_revision"]))
            event = type("PublishEvent", (), {
                "channel_uuid": channel,
                "expected_revision": kwargs["expected_revision"],
                "authority_revision": kwargs["authority_revision"],
                "configuration_generation": kwargs["configuration_generation"],
                "control_generation": kwargs["control_generation"],
                "catalog_generation": kwargs["catalog_generation"],
            })()
            return self.publish(redis, event)

        core = types.ModuleType("core")
        core.__path__ = []
        core_utils = types.ModuleType("core.utils")
        core_utils.RedisClient = types.SimpleNamespace(get_client=lambda: self.redis)
        with (
            patch.dict(sys.modules, {"core": core, "core.utils": core_utils}),
            patch(
                "catchuparr.adapters.sharing_native.observe_native_episode_for_authority_drain",
                return_value=SharingAuthorityPriorObservation("absent"),
            ),
            patch(
                "catchuparr.adapters.sharing_native.publish_current_authority",
                side_effect=strict_publish,
            ),
        ):
            self.assertTrue(_drain_sharing_authority_outbox(active_path, activation))

        self.assertEqual([(0, 3), (3, 4)], calls)
        self.assertEqual("4", self.redis.hgetall(
            f"catchuparr:sharing:current:{self.target.channel_uuid}"
        )["authority_revision"])
        self.assertEqual({"phase": "active", "lease_value": "keep"}, self.redis.hgetall(lease_key))
        self.assertEqual("complete", outbox.latest_for_channel(self.target.channel_uuid).status)
        self.assertEqual(pending.authority_revision, 4)

    def test_rehydration_preserves_a_newer_redis_authority(self):
        from catchuparr.configuration import _rehydrate_sharing_authority_head

        event = self.stage()
        newer_key = f"catchuparr:sharing:current:{event.channel_uuid}"
        self.redis.hashes[newer_key] = {
            "authority_revision": "9",
            "configuration_generation": "newer-config-synthetic",
            "control_generation": "9",
            "catalog_generation": "newer-catalog-synthetic",
        }
        publish = Mock()
        self.assertTrue(_rehydrate_sharing_authority_head(self.redis, event, publish))
        publish.assert_not_called()
        self.assertEqual("newer-config-synthetic", self.redis.hashes[newer_key]["configuration_generation"])

    def test_startup_rehydrates_missing_projection_from_exact_committed_head(self):
        from catchuparr.configuration import (
            SHARING_AUTHORITY_OUTBOX_NAME,
            rehydrate_current_sharing_authority,
        )

        active_path = Path(self.temp.name) / "active-startup-synthetic.json"
        outbox = SharingAuthorityOutbox(
            active_path.with_name(SHARING_AUTHORITY_OUTBOX_NAME),
            clock=lambda: self.now[0],
        )
        event = outbox.stage("activation-startup-synthetic", (self.target,))[0]
        self.assertTrue(outbox.commit_activation(event.activation_id))
        outbox.drain(
            self.redis,
            read_prior=lambda *_args: SharingAuthorityPriorObservation("absent"),
            publish=self.publish,
            invalidate=lambda *_args: True,
        )
        cancelled = outbox.stage("activation-startup-cancelled-synthetic", (self.target,))[0]
        self.assertTrue(outbox.cancel_activation(cancelled.activation_id))
        key = f"catchuparr:sharing:current:{self.target.channel_uuid}"
        self.assertEqual("1", self.redis.hgetall(key)["authority_revision"])
        self.redis.hashes.pop(key)
        episode_key = f"catchuparr:sharing:episode:{self.target.channel_uuid}"
        self.redis.hashes[episode_key] = {"phase": "active", "lease": "preserve"}

        published = []

        def publish_current(redis, channel_uuid, **kwargs):
            record = types.SimpleNamespace(channel_uuid=channel_uuid, **kwargs)
            published.append((channel_uuid, kwargs["expected_revision"], kwargs["authority_revision"]))
            return self.publish(redis, record)

        core = types.ModuleType("core")
        core.__path__ = []
        core_utils = types.ModuleType("core.utils")
        core_utils.RedisClient = types.SimpleNamespace(get_client=lambda: self.redis)
        active = {"connection_sharing": {self.target.channel_uuid: {"enabled": True}}}
        control = types.SimpleNamespace(paused=False, generation=self.target.control_generation)
        with (
            patch.dict(sys.modules, {"core": core, "core.utils": core_utils}),
            patch("catchuparr.configuration.active_configuration_generation", return_value=self.target.configuration_generation),
            patch("catchuparr.configuration._sharing_catalog_generation", return_value=self.target.catalog_generation),
            patch("catchuparr.adapters.sharing_native.publish_current_authority", side_effect=publish_current),
        ):
            self.assertTrue(rehydrate_current_sharing_authority(active, control, active_path))

        self.assertEqual([(self.target.channel_uuid, 0, event.authority_revision)], published)
        self.assertEqual(str(event.authority_revision), self.redis.hgetall(key)["authority_revision"])
        self.assertEqual({"phase": "active", "lease": "preserve"}, self.redis.hgetall(episode_key))
        self.assertEqual(event, outbox.committed_authority_head(self.target.channel_uuid))
        self.assertEqual("cancelled", outbox.latest_for_channel(self.target.channel_uuid).status)

    def test_startup_rehydration_fails_closed_when_committed_head_is_stale(self):
        from catchuparr.configuration import (
            SHARING_AUTHORITY_OUTBOX_NAME,
            rehydrate_current_sharing_authority,
        )

        active_path = Path(self.temp.name) / "active-startup-stale-synthetic.json"
        outbox = SharingAuthorityOutbox(
            active_path.with_name(SHARING_AUTHORITY_OUTBOX_NAME),
            clock=lambda: self.now[0],
        )
        event = outbox.stage("activation-startup-stale-synthetic", (self.target,))[0]
        self.assertTrue(outbox.commit_activation(event.activation_id))
        outbox.drain(
            self.redis,
            read_prior=lambda *_args: SharingAuthorityPriorObservation("absent"),
            publish=self.publish,
            invalidate=lambda *_args: True,
        )
        key = f"catchuparr:sharing:current:{self.target.channel_uuid}"
        self.redis.hashes.pop(key)
        published = Mock(return_value=True)

        core = types.ModuleType("core")
        core.__path__ = []
        core_utils = types.ModuleType("core.utils")
        core_utils.RedisClient = types.SimpleNamespace(get_client=lambda: self.redis)
        active = {"connection_sharing": {self.target.channel_uuid: {"enabled": True}}}
        control = types.SimpleNamespace(paused=False, generation=self.target.control_generation)
        with (
            patch.dict(sys.modules, {"core": core, "core.utils": core_utils}),
            patch("catchuparr.configuration.active_configuration_generation", return_value="new-config-synthetic"),
            patch("catchuparr.configuration._sharing_catalog_generation", return_value=self.target.catalog_generation),
            patch("catchuparr.adapters.sharing_native.publish_current_authority", side_effect=published),
        ):
            self.assertFalse(rehydrate_current_sharing_authority(active, control, active_path))

        published.assert_not_called()
        self.assertFalse(self.redis.hgetall(key))

    def test_startup_rehydration_does_not_run_while_outbox_transition_is_pending(self):
        from catchuparr.configuration import (
            SHARING_AUTHORITY_OUTBOX_NAME,
            rehydrate_current_sharing_authority,
        )

        active_path = Path(self.temp.name) / "active-startup-pending-synthetic.json"
        outbox = SharingAuthorityOutbox(
            active_path.with_name(SHARING_AUTHORITY_OUTBOX_NAME),
            clock=lambda: self.now[0],
        )
        event = outbox.stage("activation-startup-pending-synthetic", (self.target,))[0]
        published = Mock(return_value=True)

        core = types.ModuleType("core")
        core.__path__ = []
        core_utils = types.ModuleType("core.utils")
        core_utils.RedisClient = types.SimpleNamespace(get_client=lambda: self.redis)
        active = {"connection_sharing": {self.target.channel_uuid: {"enabled": True}}}
        control = types.SimpleNamespace(paused=False, generation=self.target.control_generation)
        with (
            patch.dict(sys.modules, {"core": core, "core.utils": core_utils}),
            patch("catchuparr.configuration.active_configuration_generation", return_value=self.target.configuration_generation),
            patch("catchuparr.configuration._sharing_catalog_generation", return_value=self.target.catalog_generation),
            patch("catchuparr.adapters.sharing_native.publish_current_authority", side_effect=published),
        ):
            self.assertFalse(rehydrate_current_sharing_authority(active, control, active_path))

        self.assertEqual((event,), outbox.pending())
        published.assert_not_called()
        self.assertFalse(self.redis.hgetall(f"catchuparr:sharing:current:{event.channel_uuid}"))

    def test_startup_rehydration_never_overwrites_a_newer_redis_projection(self):
        from catchuparr.configuration import (
            SHARING_AUTHORITY_OUTBOX_NAME,
            rehydrate_current_sharing_authority,
        )

        active_path = Path(self.temp.name) / "active-startup-newer-synthetic.json"
        outbox = SharingAuthorityOutbox(
            active_path.with_name(SHARING_AUTHORITY_OUTBOX_NAME),
            clock=lambda: self.now[0],
        )
        event = outbox.stage("activation-startup-newer-synthetic", (self.target,))[0]
        self.assertTrue(outbox.commit_activation(event.activation_id))
        outbox.drain(
            self.redis,
            read_prior=lambda *_args: SharingAuthorityPriorObservation("absent"),
            publish=self.publish,
            invalidate=lambda *_args: True,
        )
        key = f"catchuparr:sharing:current:{self.target.channel_uuid}"
        self.redis.hashes[key] = {
            "authority_revision": "2",
            "configuration_generation": "newer-config-synthetic",
            "control_generation": str(self.target.control_generation),
            "catalog_generation": "newer-catalog-synthetic",
        }
        published = Mock(return_value=True)

        core = types.ModuleType("core")
        core.__path__ = []
        core_utils = types.ModuleType("core.utils")
        core_utils.RedisClient = types.SimpleNamespace(get_client=lambda: self.redis)
        active = {"connection_sharing": {self.target.channel_uuid: {"enabled": True}}}
        control = types.SimpleNamespace(paused=False, generation=self.target.control_generation)
        with (
            patch.dict(sys.modules, {"core": core, "core.utils": core_utils}),
            patch("catchuparr.configuration.active_configuration_generation", return_value=self.target.configuration_generation),
            patch("catchuparr.configuration._sharing_catalog_generation", return_value=self.target.catalog_generation),
            patch("catchuparr.adapters.sharing_native.publish_current_authority", side_effect=published),
        ):
            self.assertFalse(rehydrate_current_sharing_authority(active, control, active_path))

        published.assert_not_called()
        self.assertEqual("newer-config-synthetic", self.redis.hgetall(key)["configuration_generation"])

    def test_rehydration_rejects_incomplete_hashes_even_when_their_revision_is_newer(self):
        from catchuparr.configuration import _rehydrate_sharing_authority_head

        event = self.stage()
        key = f"catchuparr:sharing:current:{event.channel_uuid}"
        for fields in (
            {"authority_revision": "1"},
            {"authority_revision": "9", "configuration_generation": "newer"},
        ):
            with self.subTest(fields=fields):
                self.redis.hashes[key] = dict(fields)
                publish = Mock()
                self.assertFalse(_rehydrate_sharing_authority_head(self.redis, event, publish))
                publish.assert_not_called()
                self.assertEqual(fields, self.redis.hashes[key])

    def test_invalid_completed_durable_history_blocks_rehydration(self):
        event = self.stage()
        self.assertEqual((event.event_id,), self.drain().completed_event_ids)
        with self.outbox._db() as db:
            db.execute(
                "UPDATE sharing_authority_outbox SET expected_revision=4 WHERE event_id=?",
                (event.event_id,),
            )
            db.commit()
        with self.assertRaisesRegex(ValueError, "not contiguous"):
            self.outbox.committed_authority_heads()


if __name__ == "__main__":
    unittest.main()
