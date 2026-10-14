import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from catchuparr.sharing_episode import (
    BorrowEpisodeLedger,
    BorrowEpisodeRejected,
    select_task_local_client_manager,
)
from catchuparr.sharing_persistence import SharingIntentToken
from catchuparr.sharing_planner import SharingAction

CHANNEL = "channel-7"
IDENTITY = (
    CHANNEL, "worker-7", "owner-7", "owner-7", "stream-7", "account-7",
    "m3u-profile-7", "input-profile-7",
)
LEASE_DIGEST = "a" * 64


def attach_token():
    return SharingIntentToken(
        channel_uuid=CHANNEL,
        revision=4,
        intent_epoch=9,
        action=SharingAction.ATTACH_BORROW,
        configuration_generation="config-3",
        control_generation=7,
        recorder_fence=11,
        target_identity=IDENTITY,
        target_admitted=True,
        not_before=100,
    )


class Clock:
    now = 100.0

    def __call__(self):
        return self.now


class Connection:
    __module__ = "redis.connection"


class FakeRedis:
    def __init__(self, decode=False):
        self.connection_pool = SimpleNamespace(
            connection_class=Connection,
            connection_kwargs={
                "host": "redis.synthetic.invalid", "port": 6379, "db": 4,
                "decode_responses": decode,
            },
        )


class Manager:
    def __init__(self, *, channel_id, redis_client, worker_id):
        self.channel_id = channel_id
        self.redis_client = redis_client
        self.worker_id = worker_id
        self.stopped = 0

    def add_client(self, *args, **kwargs):
        pass

    def remove_client(self, *args, **kwargs):
        pass

    def stop(self):
        self.stopped += 1


class BorrowEpisodeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.clock = Clock()
        self.ledger = BorrowEpisodeLedger(Path(self.temp.name) / "episodes.sqlite", clock=self.clock)

    def tearDown(self):
        self.temp.cleanup()

    def test_claimed_attach_requires_exact_target_then_registration_ack(self):
        token = attach_token()
        opening = self.ledger.begin_open(token, lease_digest=LEASE_DIGEST, identity=IDENTITY)
        self.assertEqual(opening.phase, "opening")
        self.assertIsNone(opening.client_id)
        with self.assertRaises(BorrowEpisodeRejected):
            self.ledger.authorize_snapshot(
                opening, episode_id=opening.episode_id, configuration_generation="config-3",
                control_generation=7, recorder_fence=11, identity=IDENTITY,
                lease_digest=LEASE_DIGEST, ordinary_viewer_ids=("viewer-1",), checked_at=100,
            )
        active = self.ledger.activate(
            opening.episode_id, expected_version=opening.version, client_id="client-1",
            connected_at="100.5", ordinary_viewer_ids=("viewer-1",),
        )
        self.assertEqual(active.phase, "active")
        self.assertEqual(active.intent_epoch, token.intent_epoch)
        self.assertIsNone(self.ledger.authorize_snapshot(
            active, episode_id=active.episode_id, configuration_generation="config-3",
            control_generation=7, recorder_fence=11, identity=IDENTITY,
            lease_digest=LEASE_DIGEST, ordinary_viewer_ids=("viewer-1",), checked_at=101,
        ))

    def test_opening_persists_exact_client_attempt_for_restart_cleanup_reconciliation(self):
        opening = self.ledger.begin_open(
            attach_token(), lease_digest=LEASE_DIGEST, identity=IDENTITY,
            client_id="catchuparr-client-episode-recovery-synthetic",
        )
        self.assertEqual(opening.client_id, "catchuparr-client-episode-recovery-synthetic")
        self.assertEqual(self.ledger.pending_cleanup(), (opening,))
        pending = self.ledger.revoke(
            opening.episode_id, expected_version=opening.version,
            reason="cleanup_pending", cleanup_acknowledged=False,
        )
        self.assertEqual(pending.phase, "cleanup_pending")
        self.assertEqual(self.ledger.pending_cleanup(), (pending,))

    def test_registration_ack_cannot_replace_persisted_client_attempt(self):
        opening = self.ledger.begin_open(
            attach_token(), lease_digest=LEASE_DIGEST, identity=IDENTITY,
            client_id="catchuparr-client-planned-synthetic",
        )
        with self.assertRaises(BorrowEpisodeRejected):
            self.ledger.activate(
                opening.episode_id, expected_version=opening.version,
                client_id="catchuparr-client-replacement-synthetic",
                connected_at="100.5", ordinary_viewer_ids=("viewer-synthetic",),
            )
        self.assertEqual(self.ledger.read(CHANNEL), opening)
        self.assertEqual(self.ledger.pending_cleanup(), (opening,))

    def test_grace_is_episode_bound_dynamic_and_canceled_on_viewer_return(self):
        opening = self.ledger.begin_open(attach_token(), lease_digest=LEASE_DIGEST, identity=IDENTITY)
        active = self.ledger.activate(
            opening.episode_id, expected_version=opening.version, client_id="client-1",
            connected_at="100.5", ordinary_viewer_ids=("viewer-1",),
        )
        waiting = self.ledger.update_viewers(
            active.episode_id, expected_version=active.version, ordinary_viewer_ids=(),
            observed_at=101, return_seconds=30, minimum_switch_seconds=60, last_switch_at=80,
        )
        self.assertEqual(waiting.phase, "wait_return")
        self.assertEqual(waiting.return_deadline, 140)
        self.assertEqual(self.ledger.authorize_snapshot(
            waiting, episode_id=waiting.episode_id, configuration_generation="config-3",
            control_generation=7, recorder_fence=11, identity=IDENTITY,
            lease_digest=LEASE_DIGEST, ordinary_viewer_ids=(), checked_at=102,
        ), 140)
        with self.assertRaises(BorrowEpisodeRejected):
            self.ledger.authorize_snapshot(
                waiting, episode_id="stale-episode", configuration_generation="config-3",
                control_generation=7, recorder_fence=11, identity=IDENTITY,
                lease_digest=LEASE_DIGEST, ordinary_viewer_ids=(), checked_at=102,
            )
        for changes in (
            {"configuration_generation": "config-new"},
            {"control_generation": 8},
            {"recorder_fence": 12},
            {"lease_digest": "b" * 64},
        ):
            binding = {
                "episode_id": waiting.episode_id,
                "configuration_generation": "config-3",
                "control_generation": 7,
                "recorder_fence": 11,
                "identity": IDENTITY,
                "lease_digest": LEASE_DIGEST,
                "ordinary_viewer_ids": (),
                "checked_at": 102,
            }
            binding.update(changes)
            with self.assertRaises(BorrowEpisodeRejected):
                self.ledger.authorize_snapshot(waiting, **binding)
        resumed = self.ledger.update_viewers(
            waiting.episode_id, expected_version=waiting.version, ordinary_viewer_ids=("viewer-2",),
            observed_at=110, return_seconds=30, minimum_switch_seconds=60,
        )
        self.assertEqual(resumed.phase, "active")
        self.assertIsNone(resumed.return_deadline)

    def test_expired_grace_and_revoked_episode_fail_closed(self):
        opening = self.ledger.begin_open(attach_token(), lease_digest=LEASE_DIGEST, identity=IDENTITY)
        active = self.ledger.activate(
            opening.episode_id, expected_version=opening.version, client_id="client-1",
            connected_at="100.5", ordinary_viewer_ids=("viewer-1",),
        )
        waiting = self.ledger.update_viewers(
            active.episode_id, expected_version=active.version, ordinary_viewer_ids=(),
            observed_at=101, return_seconds=30, minimum_switch_seconds=0,
        )
        with self.assertRaises(BorrowEpisodeRejected):
            self.ledger.authorize_snapshot(
                waiting, episode_id=waiting.episode_id, configuration_generation="config-3",
                control_generation=7, recorder_fence=11, identity=IDENTITY,
                lease_digest=LEASE_DIGEST, ordinary_viewer_ids=(), checked_at=140,
            )
        closing = self.ledger.update_viewers(
            waiting.episode_id, expected_version=waiting.version,
            ordinary_viewer_ids=("late-viewer",), observed_at=140,
            return_seconds=30, minimum_switch_seconds=0,
        )
        self.assertEqual(closing.phase, "closing")
        self.assertIsNone(closing.return_deadline)
        revoked = self.ledger.revoke(
            closing.episode_id, expected_version=closing.version, reason="stop",
            cleanup_acknowledged=False,
        )
        self.assertEqual(revoked.phase, "cleanup_pending")
        with self.assertRaises(BorrowEpisodeRejected):
            self.ledger.begin_open(attach_token(), lease_digest=LEASE_DIGEST,
                                   identity=IDENTITY, expected_version=revoked.version)
        closed = self.ledger.revoke(
            revoked.episode_id, expected_version=revoked.version, reason="closed",
            cleanup_acknowledged=True,
        )
        with self.assertRaises(BorrowEpisodeRejected):
            self.ledger.begin_open(
                attach_token(), lease_digest=LEASE_DIGEST, identity=IDENTITY,
                expected_version=closed.version,
            )
        fresh = attach_token()
        fresh = SharingIntentToken(
            **{**fresh.__dict__, "revision": 5, "intent_epoch": 10}
        )
        next_opening = self.ledger.begin_open(
            fresh, lease_digest=LEASE_DIGEST, identity=IDENTITY,
            expected_version=closed.version,
        )
        self.assertNotEqual(next_opening.episode_id, closed.episode_id)

    def test_future_last_switch_is_clamped_before_return_deadline(self):
        opening = self.ledger.begin_open(attach_token(), lease_digest=LEASE_DIGEST, identity=IDENTITY)
        active = self.ledger.activate(
            opening.episode_id, expected_version=opening.version, client_id="client-1",
            connected_at="100.5", ordinary_viewer_ids=("viewer-1",),
        )
        for future_switch in (3600, 3601, 7200):
            with self.subTest(future_switch=future_switch):
                # Each transition is independently exercised on a fresh DB row.
                if future_switch != 3600:
                    active = self.ledger.update_viewers(
                        active.episode_id, expected_version=active.version,
                        ordinary_viewer_ids=("viewer-1",), observed_at=101,
                        return_seconds=30, minimum_switch_seconds=60,
                    )
                waiting = self.ledger.update_viewers(
                    active.episode_id, expected_version=active.version,
                    ordinary_viewer_ids=(), observed_at=101,
                    return_seconds=30, minimum_switch_seconds=60,
                    last_switch_at=future_switch,
                )
                self.assertEqual(waiting.return_deadline, 161)
                active = self.ledger.update_viewers(
                    waiting.episode_id, expected_version=waiting.version,
                    ordinary_viewer_ids=("viewer-1",), observed_at=102,
                    return_seconds=30, minimum_switch_seconds=60,
                )

    def test_backwards_clock_future_switch_does_not_extend_grace(self):
        opening = self.ledger.begin_open(attach_token(), lease_digest=LEASE_DIGEST, identity=IDENTITY)
        active = self.ledger.activate(
            opening.episode_id, expected_version=opening.version, client_id="client-1",
            connected_at="100.5", ordinary_viewer_ids=("viewer-1",),
        )
        waiting = self.ledger.update_viewers(
            active.episode_id, expected_version=active.version, ordinary_viewer_ids=(),
            observed_at=99, return_seconds=30, minimum_switch_seconds=60,
            last_switch_at=100,
        )
        self.assertEqual(waiting.return_deadline, 159)

    def test_corrupt_persisted_grace_and_malformed_direct_snapshot_fail_closed(self):
        opening = self.ledger.begin_open(attach_token(), lease_digest=LEASE_DIGEST, identity=IDENTITY)
        active = self.ledger.activate(
            opening.episode_id, expected_version=opening.version, client_id="client-1",
            connected_at="100.5", ordinary_viewer_ids=("viewer-1",),
        )
        waiting = self.ledger.update_viewers(
            active.episode_id, expected_version=active.version, ordinary_viewer_ids=(),
            observed_at=101, return_seconds=30, minimum_switch_seconds=0,
        )
        with self.ledger._db() as db:
            db.execute(
                "UPDATE sharing_borrow_episodes SET return_deadline=? WHERE channel_uuid=?",
                (float("inf"), CHANNEL),
            )
            before = db.execute(
                "SELECT phase,version,return_deadline FROM sharing_borrow_episodes WHERE channel_uuid=?",
                (CHANNEL,),
            ).fetchone()
        with self.assertRaises(BorrowEpisodeRejected):
            self.ledger.read(CHANNEL)
        with self.assertRaises(BorrowEpisodeRejected):
            self.ledger.update_viewers(
                waiting.episode_id, expected_version=waiting.version,
                ordinary_viewer_ids=("viewer-returned",), observed_at=110,
                return_seconds=30, minimum_switch_seconds=60,
            )
        with self.ledger._db() as db:
            after = db.execute(
                "SELECT phase,version,return_deadline FROM sharing_borrow_episodes WHERE channel_uuid=?",
                (CHANNEL,),
            ).fetchone()
        self.assertEqual(tuple(after), tuple(before))
        with self.assertRaises(ValueError):
            replace(waiting, return_deadline=float("nan"))

    def test_persisted_phase_client_mismatch_and_malformed_proof_are_rejected(self):
        opening = self.ledger.begin_open(attach_token(), lease_digest=LEASE_DIGEST, identity=IDENTITY)
        with self.ledger._db() as db:
            db.execute(
                "UPDATE sharing_borrow_episodes SET phase='active' WHERE channel_uuid=?",
                (CHANNEL,),
            )
        with self.assertRaises(BorrowEpisodeRejected):
            self.ledger.read(CHANNEL)

        valid = BorrowEpisodeLedger(Path(self.temp.name) / "valid.sqlite", clock=self.clock)
        opening = valid.begin_open(attach_token(), lease_digest=LEASE_DIGEST, identity=IDENTITY)
        active = valid.activate(
            opening.episode_id, expected_version=opening.version, client_id="client-1",
            connected_at="100.5", ordinary_viewer_ids=("viewer-1",),
        )
        with self.assertRaises(BorrowEpisodeRejected):
            valid.authorize_snapshot(
                active, episode_id=active.episode_id,
                configuration_generation=active.configuration_generation,
                control_generation=True, recorder_fence=active.recorder_fence,
                identity=active.identity, lease_digest=active.lease_digest,
                ordinary_viewer_ids=("viewer-1",), checked_at=100,
            )

    def test_manager_is_task_local_and_never_mutates_native_shared_mapping(self):
        redis = FakeRedis(decode=True)
        created = []

        def factory(**kwargs):
            manager = Manager(**kwargs)
            created.append(manager)
            return manager

        shared_map = {CHANNEL: None}
        manager, owned = select_task_local_client_manager(
            None, factory, channel_uuid=CHANNEL, redis_client=redis, worker_id="web-worker-2",
        )
        self.assertTrue(owned)
        self.assertIs(manager, created[0])
        self.assertEqual(manager.worker_id, "web-worker-2")
        self.assertIsNone(shared_map[CHANNEL])
        local, owned_local = select_task_local_client_manager(
            manager, factory, channel_uuid=CHANNEL, redis_client=redis, worker_id="web-worker-2",
        )
        self.assertIs(local, manager)
        self.assertFalse(owned_local)
        self.assertEqual(len(created), 1)

    def test_manager_namespace_or_redis_backend_mismatch_is_not_reused(self):
        good = Manager(channel_id=CHANNEL, redis_client=FakeRedis(decode=True), worker_id="w")
        other_backend = FakeRedis(decode=True)
        other_backend.connection_pool.connection_kwargs["db"] = 5
        replacement, owned = select_task_local_client_manager(
            good, Manager, channel_uuid=CHANNEL, redis_client=other_backend, worker_id="w2",
        )
        self.assertTrue(owned)
        self.assertIsNot(replacement, good)
        self.assertEqual(good.stopped, 0)


if __name__ == "__main__":
    unittest.main()
