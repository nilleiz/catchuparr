import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from catchuparr.sharing_persistence import (
    SharingIntentLedger,
    StaleSharingIntent,
)
from catchuparr.sharing_planner import (
    BorrowTarget,
    SharingAction,
    SharingObservation,
    SharingPhase,
    SharingSettings,
    observe,
)

CHANNEL = "00000000-0000-0000-0000-000000000007"
IDENTITY = (
    CHANNEL,
    "native-worker-7",
    "owner-lock-7",
    "owner-lock-7",
    "stream-7",
    "account-7",
    "m3u-profile-7",
    "input-profile-7",
)


def target():
    return BorrowTarget(
        channel_uuid=IDENTITY[0],
        worker_uuid=IDENTITY[1],
        owner_lock_value=IDENTITY[2],
        metadata_owner=IDENTITY[3],
        stream_id=IDENTITY[4],
        account_id=IDENTITY[5],
        m3u_profile_id=IDENTITY[6],
        input_profile_id=IDENTITY[7],
        ordinary_client_ids=("viewer-synthetic-1",),
        owner_heartbeat_healthy=True,
        assigned_stream_matches=True,
        account_active=True,
        profile_active=True,
        media_compatible=True,
        useful_buffer_available=True,
        native_state_healthy=True,
    )


def pending_attach():
    start = observe(
        None,
        SharingObservation(
            channel_uuid=CHANNEL,
            now=100,
            configuration_generation="config-a",
            control_generation=3,
            recorder_fence=11,
            lease_is_current=True,
            settings=SharingSettings(True, 5, 30, 60),
            target=target(),
        ),
    )
    ready = observe(
        start.state,
        SharingObservation(
            channel_uuid=CHANNEL,
            now=105,
            configuration_generation="config-a",
            control_generation=3,
            recorder_fence=11,
            lease_is_current=True,
            settings=SharingSettings(True, 5, 30, 60),
            target=target(),
        ),
    )
    assert ready.action is SharingAction.ATTACH_BORROW
    return ready.state


class SharingPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.now = [105.0]
        self.ledger = SharingIntentLedger(
            Path(self.directory.name) / "archive.sqlite3",
            clock=lambda: self.now[0],
        )

    def tearDown(self):
        self.directory.cleanup()

    def save_attach(self):
        state = pending_attach()
        return self.ledger.save(
            state,
            action=SharingAction.ATTACH_BORROW,
            expected_revision=None,
            target_identity=IDENTITY,
            target_admitted=True,
            not_before=105.0,
        )

    def test_persists_exact_admitted_intent_with_monotonic_revision(self):
        saved = self.save_attach()
        self.assertEqual(1, saved.revision)
        self.assertEqual(1, saved.intent.revision)
        self.assertEqual(state_epoch := saved.state.intent_epoch, saved.intent.intent_epoch)
        self.assertEqual(1, state_epoch)
        self.assertEqual(SharingAction.ATTACH_BORROW, saved.intent.action)
        self.assertEqual(IDENTITY, saved.intent.target_identity)
        self.assertTrue(saved.intent.target_admitted)
        self.assertEqual(105.0, saved.intent.not_before)
        self.assertEqual(saved, self.ledger.read(CHANNEL))

    def test_stale_revision_cannot_overwrite_newer_intent(self):
        original = self.save_attach()
        newer_state = replace(original.state, intent_epoch=original.state.intent_epoch + 1)
        newer = self.ledger.save(
            newer_state,
            action=SharingAction.ATTACH_BORROW,
            expected_revision=original.revision,
            target_identity=IDENTITY,
            target_admitted=True,
            not_before=106.0,
        )
        self.assertEqual(2, newer.revision)
        with self.assertRaises(StaleSharingIntent):
            self.ledger.save(
                original.state,
                action=SharingAction.ATTACH_BORROW,
                expected_revision=original.revision,
                target_identity=IDENTITY,
                target_admitted=True,
                not_before=105.0,
            )

    def test_same_action_with_new_epoch_fences_old_completion(self):
        first = self.save_attach()
        second_state = replace(first.state, intent_epoch=first.state.intent_epoch + 1)
        second = self.ledger.save(
            second_state,
            action=SharingAction.ATTACH_BORROW,
            expected_revision=first.revision,
            target_identity=IDENTITY,
            target_admitted=True,
            not_before=105.0,
        )
        effects = []
        ran, result = self.ledger.run_if_current(
            first.intent,
            fresh_validate=lambda _token: True,
            side_effect=lambda token: effects.append(token),
        )
        self.assertFalse(ran)
        self.assertIsNone(result)
        self.assertEqual([], effects)
        ran, _ = self.ledger.run_if_current(
            second.intent,
            fresh_validate=lambda token: token == second.intent,
            side_effect=lambda token: effects.append(token.action.value),
        )
        self.assertTrue(ran)
        self.assertEqual(["attach_borrow"], effects)

    def test_deadline_and_fresh_external_validation_gate_side_effect(self):
        saved = self.save_attach()
        effects = []
        self.now[0] = 104.99
        ran, _ = self.ledger.run_if_current(
            saved.intent,
            fresh_validate=lambda _token: True,
            side_effect=lambda _token: effects.append("early"),
        )
        self.assertFalse(ran)
        self.now[0] = 105.0
        ran, _ = self.ledger.run_if_current(
            saved.intent,
            fresh_validate=lambda token: token.recorder_fence == 11,
            side_effect=lambda _token: effects.append("current"),
        )
        self.assertTrue(ran)
        ran, _ = self.ledger.run_if_current(
            saved.intent,
            fresh_validate=lambda _token: False,
            side_effect=lambda _token: effects.append("stale external state"),
        )
        self.assertFalse(ran)
        self.assertEqual(["current"], effects)

    def test_recovery_keeps_cooldown_clocks_but_drops_borrow_and_pending_authority(self):
        saved = self.save_attach()
        persisted = dict(self.ledger.read(CHANNEL).state.to_snapshot())
        # Simulate persisted borrowed/planned route metadata; recovery may retain
        # only planner clocks and must issue a new epoch before later actions.
        borrowed_state = replace(
            saved.state,
            phase=SharingPhase.BORROWED,
            borrowed_identity=IDENTITY,
            pending_action=SharingAction.ATTACH_BORROW,
        )
        self.ledger.save(
            borrowed_state,
            action=SharingAction.ATTACH_BORROW,
            expected_revision=saved.revision,
            target_identity=IDENTITY,
            target_admitted=True,
            not_before=105.0,
        )
        recovered = self.ledger.recover(
            CHANNEL,
            configuration_generation="config-a",
            control_generation=3,
            recorder_fence=12,
            now=106,
        )
        self.assertIsNone(recovered.intent)
        self.assertEqual(SharingPhase.DEDICATED, recovered.state.phase)
        self.assertIsNone(recovered.state.borrowed_identity)
        self.assertIsNone(recovered.state.pending_action)
        self.assertGreater(recovered.state.intent_epoch, borrowed_state.intent_epoch)
        self.assertEqual(borrowed_state.last_switch_at, recovered.state.last_switch_at)
        self.assertNotEqual(IDENTITY, persisted.get("borrowed_identity"))
        ran, _ = self.ledger.run_if_current(
            saved.intent,
            fresh_validate=lambda _token: True,
            side_effect=lambda _token: self.fail("pre-restart intent ran"),
        )
        self.assertFalse(ran)

    def test_corrupt_persisted_action_fails_closed(self):
        saved = self.save_attach()
        with self.ledger._connect() as db:
            db.execute(
                "UPDATE sharing_runtime SET action=? WHERE channel_uuid=?",
                ("unknown-action", CHANNEL),
            )
        with self.assertRaises(ValueError):
            self.ledger.read(CHANNEL)
        self.assertEqual(1, saved.revision)

    def test_state_only_write_clears_pending_intent(self):
        saved = self.save_attach()
        cleared_state = replace(saved.state, pending_action=None)
        cleared = self.ledger.save(
            cleared_state,
            action=None,
            expected_revision=saved.revision,
        )
        self.assertIsNone(cleared.intent)
        ran, _ = self.ledger.run_if_current(
            saved.intent,
            fresh_validate=lambda _token: True,
            side_effect=lambda _token: self.fail("cleared intent ran"),
        )
        self.assertFalse(ran)

    def test_explicit_stop_is_persisted_even_without_planner_pending_action(self):
        stopped = replace(
            pending_attach(),
            phase=SharingPhase.STOPPED,
            pending_action=None,
        )
        saved = self.ledger.save(
            stopped,
            action=SharingAction.STOP,
            expected_revision=None,
            not_before=105.0,
        )
        self.assertEqual(SharingAction.STOP, saved.intent.action)
        ran, result = self.ledger.run_if_current(
            saved.intent,
            fresh_validate=lambda token: token.action is SharingAction.STOP,
            side_effect=lambda token: token.action.value,
        )
        self.assertTrue(ran)
        self.assertEqual("stop", result)


if __name__ == "__main__":
    unittest.main()
