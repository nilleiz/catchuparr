import tempfile
import unittest
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

from catchuparr.sharing_episode import BorrowEpisodeLedger, BorrowEpisodeRejected
from catchuparr.sharing_persistence import SharingIntentToken
from catchuparr.sharing_planner import SharingAction
from catchuparr.sharing_recovery import recover_superseded_borrow

CHANNEL = "synthetic-recovery-channel"
CLIENT = "catchuparr-client-synthetic-old"
LEASE = "12:synthetic-current-owner"
IDENTITY = (CHANNEL, "worker", "owner", "owner", "stream", "account", "m3u", "input")


class NativeState:
    def __init__(self):
        self.lease = LEASE
        self.projection = False
        self.client = False
        self.ttl_value = 30
        self.eval_calls = []
        self.eval_result = 1

    def get(self, key):
        return self.lease

    def ttl(self, key):
        return self.ttl_value

    def exists(self, key):
        return self.projection

    def type(self, key):
        if key == f"catchuparr:recorder:{CHANNEL}":
            return "string"
        if key == f"live:channel:{CHANNEL}:clients:{CLIENT}":
            return "hash" if self.client else "none"
        return "none"

    def hgetall(self, key):
        return dict(configuration_generation="synthetic-config", control_generation="8",
                    catalog_generation="synthetic-catalog", authority_revision="3")

    def eval(self, *args):
        self.eval_calls.append(args)
        return self.eval_result


class SupersededBorrowRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.ledger = BorrowEpisodeLedger(Path(self.temp.name) / "sharing.sqlite3")
        self.token = SharingIntentToken(
            channel_uuid=CHANNEL, revision=1, intent_epoch=1,
            action=SharingAction.ATTACH_BORROW,
            configuration_generation="synthetic-config", control_generation=7,
            recorder_fence=11, target_identity=IDENTITY, target_admitted=True, not_before=100,
        )
        opening = self.ledger.begin_open(
            self.token, lease_digest=sha256(b"11:old-owner").hexdigest(),
            identity=IDENTITY, client_id=CLIENT, expected_version=None,
        )
        self.old = self.ledger.activate(
            opening.episode_id, expected_version=opening.version, client_id=CLIENT,
            connected_at="100", ordinary_viewer_ids=("synthetic-viewer",),
        )
        self.redis = NativeState()
        self.context = {"catalog_generation": "synthetic-catalog", "authority_revision": 3}

    def recover(self, **kwargs):
        args = dict(configuration_generation="synthetic-config", control_generation=8,
                    recorder_fence=12, lease_value=LEASE,
                    current_authority=lambda: self.context)
        args.update(kwargs)
        return recover_superseded_borrow(self.ledger, self.redis, CHANNEL, **args)

    def test_old_active_blocks_open_until_exact_tombstone_then_new_claim_opens_once(self):
        fresh = replace(self.token, control_generation=8, recorder_fence=12, intent_epoch=2)
        with self.assertRaises(BorrowEpisodeRejected):
            self.ledger.begin_open(fresh, lease_digest="b" * 64, identity=IDENTITY,
                                   client_id="catchuparr-client-new", expected_version=self.old.version)
        self.assertTrue(self.recover())
        closed = self.ledger.read(CHANNEL)
        self.assertEqual((self.old.episode_id, "closed"), (closed.episode_id, closed.phase))
        self.assertTrue(self.recover())
        self.assertEqual(1, len(self.redis.eval_calls))
        opened = self.ledger.begin_open(
            fresh, lease_digest=sha256(LEASE.encode()).hexdigest(), identity=IDENTITY,
            client_id="catchuparr-client-new", expected_version=closed.version,
        )
        self.assertNotEqual(closed.episode_id, opened.episode_id)
        with self.assertRaises(BorrowEpisodeRejected):
            self.ledger.begin_open(fresh, lease_digest="b" * 64, identity=IDENTITY,
                                   expected_version=opened.version)

    def test_live_projection_or_private_registration_is_never_revoked(self):
        for field in ("projection", "client"):
            with self.subTest(field=field):
                setattr(self.redis, field, True)
                self.assertFalse(self.recover())
                self.assertEqual(self.old, self.ledger.read(CHANNEL))
                setattr(self.redis, field, False)
        self.assertEqual([], self.redis.eval_calls)

    def test_same_or_newer_fence_configuration_and_authority_changes_deny(self):
        for args in ({"recorder_fence": 11, "lease_value": "11:other-owner"},
                     {"recorder_fence": 10, "lease_value": "10:other-owner"},
                     {"configuration_generation": "changed-config"},
                     {"control_generation": 6}, {"current_authority": lambda: None}):
            with self.subTest(args=args):
                self.assertFalse(self.recover(**args))
                self.assertEqual(self.old, self.ledger.read(CHANNEL))

    def test_lost_lease_and_expired_lease_deny_before_sql_transition(self):
        self.redis.lease = "13:other-owner"
        self.assertFalse(self.recover())
        self.redis.lease = LEASE
        self.redis.ttl_value = 0
        self.assertFalse(self.recover())
        self.assertEqual(self.old, self.ledger.read(CHANNEL))

    def test_failed_native_proof_remains_pending_and_retries_idempotently(self):
        self.redis.eval_result = 0
        self.assertFalse(self.recover())
        pending = self.ledger.read(CHANNEL)
        self.assertEqual("cleanup_pending", pending.phase)
        self.redis.eval_result = 1
        self.assertTrue(self.recover())
        self.assertEqual(pending.version + 1, self.ledger.read(CHANNEL).version)

    def test_authority_race_or_sql_cas_failure_never_acknowledges_cleanup(self):
        contexts = iter((self.context, None))
        self.assertFalse(self.recover(current_authority=lambda: next(contexts)))
        self.assertEqual("cleanup_pending", self.ledger.read(CHANNEL).phase)
        with patch.object(self.ledger, "revoke", side_effect=BorrowEpisodeRejected("CAS")):
            self.assertFalse(self.recover())
        self.assertEqual("cleanup_pending", self.ledger.read(CHANNEL).phase)

    def test_absent_is_allowed_but_malformed_sql_row_is_not_absence(self):
        other = "synthetic-absent-channel"
        self.assertIsNone(self.ledger.read_optional(other))
        with self.ledger._db() as db:
            db.execute("UPDATE sharing_borrow_episodes SET identity_json='{}'")
        self.assertFalse(self.recover())

    def test_expected_authority_and_lease_are_bound_before_native_eval(self):
        self.context = {"catalog_generation": "changed-catalog", "authority_revision": 3}
        self.assertFalse(self.recover())
        self.assertEqual([], self.redis.eval_calls)
        self.assertEqual("cleanup_pending", self.ledger.read(CHANNEL).phase)


if __name__ == "__main__":
    unittest.main()
