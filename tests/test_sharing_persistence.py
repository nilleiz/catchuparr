import json
import multiprocessing
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


def _claim_from_process(database_path, token, barrier, results):
    ledger = SharingIntentLedger(Path(database_path), clock=lambda: 105.0)

    calls = [0]
    def validate(_token):
        calls[0] += 1
        if calls[0] == 1 and barrier is not None:
            barrier.wait(timeout=10)
        return True

    ran, _ = ledger.run_if_current(
        token, fresh_validate=validate,
        side_effect=lambda _token: results.put("effect"),
    )
    results.put("ran" if ran else "denied")


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

    def test_duplicate_exact_token_is_durably_consumed_after_first_dispatch(self):
        saved = self.save_attach()
        effects = []
        for index in range(2):
            ran, _ = self.ledger.run_if_current(
                saved.intent, fresh_validate=lambda _token: True,
                side_effect=lambda _token: effects.append("effect"),
            )
            self.assertEqual(index == 0, ran)
        self.assertEqual(["effect"], effects)

    def test_same_logical_intent_resaved_at_new_revision_cannot_dispatch_twice(self):
        first = self.save_attach()
        effects = []
        ran, _ = self.ledger.run_if_current(
            first.intent, fresh_validate=lambda _token: True,
            side_effect=lambda _token: effects.append("first"),
        )
        self.assertTrue(ran)
        second = self.ledger.save(
            first.state, action=SharingAction.ATTACH_BORROW,
            expected_revision=first.revision, target_identity=IDENTITY,
            target_admitted=True, not_before=105.0,
        )
        self.assertEqual(first.intent.intent_epoch, second.intent.intent_epoch)
        self.assertNotEqual(first.revision, second.revision)
        ran, _ = self.ledger.run_if_current(
            second.intent, fresh_validate=lambda _token: True,
            side_effect=lambda _token: effects.append("duplicate"),
        )
        self.assertFalse(ran)
        self.assertEqual(["first"], effects)

    def test_coalesced_revision_update_during_callback_cannot_redrive_intent(self):
        first = self.save_attach()
        effects = []
        def callback(_token):
            effects.append("effect")
            self.ledger.save(
                first.state, action=SharingAction.ATTACH_BORROW,
                expected_revision=first.revision, target_identity=IDENTITY,
                target_admitted=True, not_before=105.0,
            )
        ran, _ = self.ledger.run_if_current(
            first.intent, fresh_validate=lambda _token: True, side_effect=callback,
        )
        self.assertTrue(ran)
        current = self.ledger.read(CHANNEL)
        self.assertNotEqual(first.revision, current.revision)
        ran, _ = self.ledger.run_if_current(
            current.intent, fresh_validate=lambda _token: True,
            side_effect=lambda _token: effects.append("duplicate"),
        )
        self.assertFalse(ran)
        self.assertEqual(["effect"], effects)

    def test_explicit_new_intent_epoch_can_dispatch_with_fresh_proof(self):
        first = self.save_attach()
        effects = []
        ran, _ = self.ledger.run_if_current(
            first.intent, fresh_validate=lambda _token: True,
            side_effect=lambda _token: effects.append(1),
        )
        self.assertTrue(ran)
        retry_state = replace(first.state, intent_epoch=first.state.intent_epoch + 1)
        retry = self.ledger.save(
            retry_state, action=SharingAction.ATTACH_BORROW,
            expected_revision=first.revision, target_identity=IDENTITY,
            target_admitted=True, not_before=105.0,
        )
        ran, _ = self.ledger.run_if_current(
            retry.intent, fresh_validate=lambda token: token.intent_epoch == 2,
            side_effect=lambda _token: effects.append(2),
        )
        self.assertTrue(ran)
        self.assertEqual([1, 2], effects)

    def test_cross_process_revision_update_cannot_redrive_claimed_logical_intent(self):
        first = self.save_attach()
        context = multiprocessing.get_context("fork")
        results = context.Queue()
        first_process = context.Process(
            target=_claim_from_process,
            args=(str(self.ledger.database_path), first.intent, None, results),
        )
        first_process.start()
        first_process.join(15)
        self.assertEqual(0, first_process.exitcode)
        self.assertEqual(["effect", "ran"], [results.get(timeout=2), results.get(timeout=2)])
        second = self.ledger.save(
            first.state, action=SharingAction.ATTACH_BORROW,
            expected_revision=first.revision, target_identity=IDENTITY,
            target_admitted=True, not_before=105.0,
        )
        second_process = context.Process(
            target=_claim_from_process,
            args=(str(self.ledger.database_path), second.intent, None, results),
        )
        second_process.start()
        second_process.join(15)
        self.assertEqual(0, second_process.exitcode)
        self.assertEqual(["denied"], [results.get(timeout=2)])

    def test_old_revision_claims_migrate_to_conservative_epoch_tombstones(self):
        saved = self.save_attach()
        with self.ledger._connect() as db:
            db.execute("""INSERT INTO sharing_intent_claims
                (channel_uuid, revision, intent_epoch, action, claim_nonce, claimed_at)
                VALUES (?, ?, ?, ?, ?, ?)""",
                (CHANNEL, saved.revision, saved.intent.intent_epoch, saved.intent.action.value,
                 "synthetic-old-claim", 105.0))
            db.execute("DROP TABLE sharing_logical_intent_claims")
            db.execute("DROP TABLE sharing_legacy_intent_claims")
            db.execute("DROP TABLE sharing_schema_meta")
        migrated = SharingIntentLedger(self.ledger.database_path, clock=lambda: 105.0)
        with migrated._connect() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM sharing_legacy_intent_claims").fetchone()[0])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM sharing_intent_claims").fetchone()[0])
        ran, _ = migrated.run_if_current(
            saved.intent, fresh_validate=lambda _token: True,
            side_effect=lambda _token: self.fail("migrated consumed claim ran again"),
        )
        self.assertFalse(ran)
        retry = replace(saved.state, intent_epoch=saved.state.intent_epoch + 1)
        current = migrated.save(
            retry, action=SharingAction.ATTACH_BORROW, expected_revision=saved.revision,
            target_identity=IDENTITY, target_admitted=True, not_before=105.0,
        )
        ran, _ = migrated.run_if_current(
            current.intent, fresh_validate=lambda _token: True,
            side_effect=lambda _token: "fresh epoch",
        )
        self.assertTrue(ran)

    def test_two_os_processes_cannot_claim_the_same_intent(self):
        saved = self.save_attach()
        context = multiprocessing.get_context("fork")
        barrier, results = context.Barrier(2), context.Queue()
        processes = [context.Process(target=_claim_from_process,
                                     args=(str(self.ledger.database_path), saved.intent, barrier, results))
                     for _ in range(2)]
        for process in processes:
            process.start()
        for process in processes:
            process.join(15)
        self.assertEqual([0, 0], [process.exitcode for process in processes])
        outcomes = [results.get(timeout=2) for _ in range(3)]
        self.assertEqual(1, outcomes.count("effect"))
        self.assertEqual(1, outcomes.count("ran"))
        self.assertEqual(1, outcomes.count("denied"))

    def test_crashed_durable_claim_fails_closed_without_retry(self):
        saved = self.save_attach()
        with self.ledger._connect() as db:
            db.execute("""INSERT INTO sharing_intent_claims
                (channel_uuid, revision, intent_epoch, action, claim_nonce, claimed_at)
                VALUES (?, ?, ?, ?, ?, ?)""",
                (CHANNEL, saved.revision, saved.intent.intent_epoch, saved.intent.action.value,
                 "synthetic-crashed-claim", 105.0))
        effects = []
        ran, _ = self.ledger.run_if_current(
            saved.intent, fresh_validate=lambda _token: True,
            side_effect=lambda _token: effects.append("effect"),
        )
        self.assertFalse(ran)
        self.assertEqual([], effects)

    def test_clock_rollback_during_fresh_validation_blocks_dispatch(self):
        saved = self.save_attach()
        effects = []
        def rollback(_token):
            self.now[0] = 104.0
            return True
        ran, _ = self.ledger.run_if_current(
            saved.intent, fresh_validate=rollback,
            side_effect=lambda _token: effects.append("effect"),
        )
        self.assertFalse(ran)
        self.assertEqual([], effects)
        self.now[0] = 105.0
        ran, _ = self.ledger.run_if_current(
            saved.intent, fresh_validate=lambda _token: True,
            side_effect=lambda _token: effects.append("effect"),
        )
        self.assertTrue(ran)
        self.assertEqual(["effect"], effects)

    def test_clock_rollback_after_durable_claim_blocks_final_dispatch(self):
        saved = self.save_attach()
        values = iter((105.0, 105.0, 105.0, 105.0, 104.0))
        self.ledger.clock = lambda: next(values)
        effects = []
        ran, _ = self.ledger.run_if_current(
            saved.intent,
            fresh_validate=lambda _token: True,
            side_effect=lambda _token: effects.append("effect"),
        )
        self.assertFalse(ran)
        self.assertEqual([], effects)

    def test_superseding_stop_after_claim_prevents_attach_dispatch(self):
        saved = self.save_attach()
        stopped = replace(saved.state, phase=SharingPhase.STOPPED, pending_action=None,
                          intent_epoch=saved.state.intent_epoch + 1)
        effects = []
        validations = [0]
        def supersede(_token):
            validations[0] += 1
            if validations[0] == 2:
                self.ledger.save(stopped, action=SharingAction.STOP,
                                 expected_revision=saved.revision, not_before=105.0)
            return True
        ran, _ = self.ledger.run_if_current(
            saved.intent, fresh_validate=supersede,
            side_effect=lambda _token: effects.append("stale attach"),
        )
        self.assertFalse(ran)
        self.assertEqual([], effects)
        self.assertEqual(SharingAction.STOP, self.ledger.read(CHANNEL).intent.action)

    def test_corrupt_nested_snapshot_fields_fail_closed(self):
        mutators = (
            lambda snapshot: snapshot.update(candidate_identity="abcdefgh"),
            lambda snapshot: snapshot.update(candidate_since=float("nan")),
            lambda snapshot: snapshot.update(candidate_connection_identity=[CHANNEL, "short"]),
            lambda snapshot: snapshot.update(last_switch_at=True),
            lambda snapshot: snapshot.update(control_generation=True),
            lambda snapshot: snapshot.update(candidate_connection_identity=["other-channel", *IDENTITY[1:]]),
            lambda snapshot: snapshot.update(candidate_identity=[*IDENTITY[:4], "other-stream", *IDENTITY[5:]]),
        )
        for index, mutate in enumerate(mutators):
            with self.subTest(index=index):
                path = Path(self.directory.name) / f"corrupt-{index}.sqlite3"
                ledger = SharingIntentLedger(path, clock=lambda: 105.0)
                saved = ledger.save(pending_attach(), action=SharingAction.ATTACH_BORROW,
                                   expected_revision=None, target_identity=IDENTITY,
                                   target_admitted=True, not_before=105.0)
                snapshot = saved.state.to_snapshot()
                mutate(snapshot)
                raw = json.dumps(snapshot, allow_nan=True, separators=(",", ":"))
                with ledger._connect() as db:
                    db.execute("UPDATE sharing_runtime SET state_json=? WHERE channel_uuid=?", (raw, CHANNEL))
                with self.assertRaises(ValueError):
                    ledger.read(CHANNEL)

    def test_corrupt_target_disagreement_fails_closed(self):
        saved = self.save_attach()
        wrong = list(IDENTITY)
        wrong[-1] = "different-input-profile"
        with self.ledger._connect() as db:
            db.execute("UPDATE sharing_runtime SET target_identity_json=? WHERE channel_uuid=?",
                       (json.dumps(wrong), CHANNEL))
        with self.assertRaises(ValueError):
            self.ledger.read(CHANNEL)
        self.assertEqual(1, saved.revision)

    def test_attach_intent_requires_wait_live_candidate_phase(self):
        saved = self.save_attach()
        snapshot = saved.state.to_snapshot()
        snapshot["phase"] = SharingPhase.DEDICATED.value
        with self.ledger._connect() as db:
            db.execute("UPDATE sharing_runtime SET state_json=? WHERE channel_uuid=?",
                       (json.dumps(snapshot), CHANNEL))
        with self.assertRaises(ValueError):
            self.ledger.read(CHANNEL)

    def test_recovery_discards_mismatched_advisory_candidate_pair(self):
        saved = self.save_attach()
        snapshot = saved.state.to_snapshot()
        wrong = list(IDENTITY)
        wrong[4] = "other-stream"
        snapshot["candidate_identity"] = wrong
        snapshot["last_switch_at"] = 101.0
        with self.ledger._connect() as db:
            db.execute("UPDATE sharing_runtime SET state_json=? WHERE channel_uuid=?",
                       (json.dumps(snapshot), CHANNEL))
        recovered = self.ledger.recover(
            CHANNEL, configuration_generation="config-a", control_generation=3,
            recorder_fence=12, now=106, expected_revision=saved.revision,
            recovery_is_current=lambda *_args: True,
        )
        self.assertIsNone(recovered.state.candidate_identity)
        self.assertIsNone(recovered.state.candidate_connection_identity)
        self.assertIsNone(recovered.state.candidate_since)
        self.assertEqual(101.0, recovered.state.last_switch_at)

    def test_recovery_rejects_stale_revision_fence_and_control(self):
        saved = self.save_attach()
        for revision, control, fence in ((saved.revision + 1, 3, 12),
                                         (saved.revision, 2, 12),
                                         (saved.revision, 3, 10)):
            with self.subTest(revision=revision, control=control, fence=fence):
                with self.assertRaises(StaleSharingIntent):
                    self.ledger.recover(
                        CHANNEL, configuration_generation="config-a", control_generation=control,
                        recorder_fence=fence, now=106, expected_revision=revision,
                        recovery_is_current=lambda *_args: True,
                    )
        self.assertEqual(saved, self.ledger.read(CHANNEL))

    def test_recovery_requires_fresh_owner_admission(self):
        saved = self.save_attach()
        with self.assertRaises(StaleSharingIntent):
            self.ledger.recover(
                CHANNEL, configuration_generation="config-a", control_generation=3,
                recorder_fence=12, now=106, expected_revision=saved.revision,
                recovery_is_current=lambda *_args: False,
            )
        self.assertEqual(saved, self.ledger.read(CHANNEL))

    def test_recovery_keeps_cooldown_clocks_but_drops_borrow_and_pending_authority(self):
        saved = self.save_attach()
        persisted = dict(self.ledger.read(CHANNEL).state.to_snapshot())
        # The persisted candidate is still only a planner suggestion; recovery
        # retains its cooldown clock but never restores borrowed authority.
        borrowed_state = replace(
            saved.state,
            last_switch_at=101.0,
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
            expected_revision=2,
            recovery_is_current=lambda *_args: True,
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

    def test_nonborrow_target_is_rejected_before_persisting_any_intent(self):
        for action, phase, pending in (
            (SharingAction.RETURN_TO_ALLOWED, SharingPhase.FAILED, SharingAction.RETURN_TO_ALLOWED),
            (SharingAction.PAUSE_NO_ALLOWED_SOURCE, SharingPhase.FAILED,
             SharingAction.PAUSE_NO_ALLOWED_SOURCE),
            (SharingAction.STOP, SharingPhase.STOPPED, None),
        ):
            with self.subTest(action=action):
                ledger = SharingIntentLedger(
                    Path(self.directory.name) / f"invalid-{action.value}.sqlite3",
                    clock=lambda: self.now[0],
                )
                current = ledger.save(
                    pending_attach(), action=SharingAction.ATTACH_BORROW,
                    expected_revision=None, target_identity=IDENTITY,
                    target_admitted=True, not_before=105.0,
                )
                before = ledger.read(CHANNEL)
                invalid_state = replace(current.state, phase=phase, pending_action=pending)
                with self.assertRaises(ValueError):
                    ledger.save(
                        invalid_state, action=action, expected_revision=current.revision,
                        target_identity=IDENTITY, target_admitted=False, not_before=105.0,
                    )
                self.assertEqual(before, ledger.read(CHANNEL))


if __name__ == "__main__":
    unittest.main()
