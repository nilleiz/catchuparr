from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from catchuparr import sharing_coordinator as coordinator
from catchuparr.adapters.sharing_native import NativeEpisodeGrant
from catchuparr.engine.recorder import RecorderAttemptResult, RecorderAttemptSwitch
from catchuparr.sharing_episode import BorrowEpisodeLedger
from catchuparr.sharing_persistence import (
    SharingEffectReceipt,
    SharingIntentLedger,
    SharingIntentToken,
    StaleSharingIntent,
)
from catchuparr.sharing_planner import (
    SharingAction,
    SharingObservation,
    SharingPhase,
    SharingSettings,
    SharingState,
    observe,
    resolve_transition,
)

CHANNEL = "channel-synthetic-coordinator-060"
IDENTITY = (
    CHANNEL, "worker-synthetic", "owner-synthetic", "owner-synthetic",
    "stream-synthetic", "account-synthetic", "m3u-synthetic", "input-synthetic",
)


def _grant() -> NativeEpisodeGrant:
    return NativeEpisodeGrant(
        channel_uuid=CHANNEL, episode_id="episode-synthetic", phase="active",
        configuration_generation="config-old", control_generation=1,
        catalog_generation="catalog-old", recorder_fence=4, intent_epoch=7,
        version=3, lease_value="synthetic-lease", worker_id=IDENTITY[1],
        owner_lock_value=IDENTITY[2], metadata_owner=IDENTITY[3],
        stream_id=IDENTITY[4], account_id=IDENTITY[5],
        m3u_profile_id=IDENTITY[6], input_profile_id=IDENTITY[7],
        client_id="catchuparr-client-synthetic", client_worker_id="task-worker-synthetic",
        connected_at="100.000000", return_seconds=30, minimum_switch_seconds=60,
        last_switch_at=0.0, last_viewer_left_at=None, return_deadline=None,
    )


def _return_token(ledger: SharingIntentLedger):
    state = SharingState(
        channel_uuid=CHANNEL, configuration_generation="config-current",
        control_generation=2, recorder_fence=4, phase=SharingPhase.BORROWED,
        borrowed_identity=IDENTITY, last_switch_at=0.0,
    )
    decision = observe(
        state,
        SharingObservation(
            channel_uuid=CHANNEL, now=100, configuration_generation="config-current",
            control_generation=2, recorder_fence=4, lease_is_current=True,
            settings=SharingSettings(True, 5, 30, 60), target=None,
            dedicated_source_available=True,
        ),
    )
    assert decision.action is SharingAction.RETURN_TO_ALLOWED
    saved = ledger.save(
        decision.state, action=decision.action, expected_revision=None,
        not_before=100.0,
    )
    return saved.intent


def _switch_and_fallback_tokens(ledger: SharingIntentLedger):
    switch_intent = _return_token(ledger)
    attempt_switch = RecorderAttemptSwitch()
    claimed = coordinator.request_source_switch(
        attempt_switch, switch_intent, ledger=ledger,
        fresh_validate=lambda _token: True,
    )
    assert claimed
    persisted = ledger.read(CHANNEL)
    observation = SharingObservation(
        channel_uuid=CHANNEL, now=100, configuration_generation="config-current",
        control_generation=2, recorder_fence=4, lease_is_current=True,
        settings=SharingSettings(True, 5, 30, 60), target=None,
        dedicated_source_available=True,
    )
    failed = resolve_transition(
        persisted.state, SharingAction.RETURN_TO_ALLOWED, succeeded=False,
        now=100, current_state=persisted.state, current_observation=observation,
    )
    saved_failure = ledger.save(
        failed, action=None, expected_revision=persisted.revision,
    )
    next_decision = observe(saved_failure.state, observation)
    assert next_decision.action is SharingAction.RETURN_TO_ALLOWED
    saved_fallback = ledger.save(
        next_decision.state, action=next_decision.action,
        expected_revision=saved_failure.revision, not_before=100.0,
    )
    return switch_intent, saved_fallback.intent, attempt_switch


class SharingCoordinatorTests(unittest.TestCase):
    def test_authority_publish_precedes_exact_prior_episode_invalidation(self):
        events = []
        change = coordinator.AuthorityChange(
            CHANNEL, 10, 11, "config-new", 2, "catalog-new",
        )
        with (
            patch.object(coordinator, "publish_current_authority", side_effect=lambda *_a, **_k: events.append("publish") or True),
            patch.object(coordinator, "invalidate_native_episode", side_effect=lambda *_a, **_k: events.append("invalidate") or True),
        ):
            outcome = coordinator.publish_authority_change(
                object(), change, prior_episode=_grant(),
            )
        self.assertEqual(events, ["publish", "invalidate"])
        self.assertEqual(outcome, coordinator.AuthorityChangeOutcome(True, True))

    def test_stale_authority_publish_never_invalidates_episode(self):
        change = coordinator.AuthorityChange(
            CHANNEL, 10, 11, "config-new", 2, "catalog-new",
        )
        with (
            patch.object(coordinator, "publish_current_authority", return_value=False),
            patch.object(coordinator, "_matches_published_authority", return_value=False),
            patch.object(coordinator, "invalidate_native_episode") as invalidate,
        ):
            outcome = coordinator.publish_authority_change(
                object(), change, prior_episode=_grant(),
            )
        self.assertFalse(outcome.current_authority_confirmed)
        invalidate.assert_not_called()

    def test_observes_live_native_source_before_filtering_candidates(self):
        events = []
        observation = object()
        with patch.object(coordinator, "observe_native_connection", side_effect=lambda *_a, **_k: events.append("observe") or observation):
            snapshot = coordinator.observe_before_candidate_filter(
                object(), CHANNEL, catalog_generation="catalog-synthetic",
                assigned_sources=("preloaded-source",), observed_at=10.0,
                rank_allowed_candidates=lambda: events.append("filter") or (),
            )
        self.assertEqual(events, ["observe", "filter"])
        self.assertIs(snapshot.existing_connection, observation)
        self.assertEqual(snapshot.allowed_candidates, ())

    def test_per_attempt_switch_does_not_change_global_stop_event(self):
        import threading

        with tempfile.TemporaryDirectory() as directory:
            ledger = SharingIntentLedger(Path(directory) / "state.sqlite", clock=lambda: 100.0)
            switch_intent = _return_token(ledger)
            switch = RecorderAttemptSwitch()
            global_stop = threading.Event()
            self.assertTrue(coordinator.request_source_switch(
                switch, switch_intent, ledger=ledger, fresh_validate=lambda _token: True,
            ))
            self.assertTrue(switch.is_set())
            self.assertFalse(global_stop.is_set())
            self.assertFalse(coordinator.request_source_switch(
                switch, switch_intent, ledger=ledger, fresh_validate=lambda _token: True,
            ))

    def test_explicit_stop_intent_cannot_become_attempt_switch(self):
        switch = RecorderAttemptSwitch()
        self.assertFalse(coordinator.request_source_switch(
            switch, object(), ledger=object(), fresh_validate=lambda _token: True,
        ))
        self.assertFalse(switch.is_set())

    def test_fallback_requires_reaped_attempt_and_exact_cleanup_ack(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = SharingIntentLedger(Path(directory) / "state.sqlite", clock=lambda: 100.0)
            switch_intent, token, attempt_switch = _switch_and_fallback_tokens(ledger)
            launched = []
            result = coordinator.return_to_allowed_after_reap(
                RecorderAttemptResult(
                    "switch_requested", 2, 0,
                    attempt_switch_id=attempt_switch.attempt_id,
                ), token,
                switch_intent=switch_intent,
                attempt_switch=attempt_switch,
                ledger=ledger, fresh_validate=lambda _token: True,
                exact_cleanup_acknowledged=lambda: False,
                source_identity=IDENTITY,
                source_client_id="catchuparr-client-source-synthetic",
                candidate={"identity": IDENTITY},
                candidate_is_currently_allowed=lambda _candidate: True,
                launch_allowed_candidate=lambda *_args: self.fail("launch before cleanup ACK"),
            )
            self.assertEqual(result, ("cleanup_pending", None))
            self.assertEqual(launched, [])

    def test_fallback_consumes_current_intent_once_after_cleanup_and_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = SharingIntentLedger(Path(directory) / "state.sqlite", clock=lambda: 100.0)
            switch_intent, token, attempt_switch = _switch_and_fallback_tokens(ledger)
            events = []
            result = coordinator.return_to_allowed_after_reap(
                RecorderAttemptResult(
                    "switch_requested", 2, 0,
                    attempt_switch_id=attempt_switch.attempt_id,
                ), token,
                switch_intent=switch_intent,
                attempt_switch=attempt_switch,
                ledger=ledger,
                fresh_validate=lambda _token: events.append("validate") or True,
                exact_cleanup_acknowledged=lambda: events.append("cleanup") or True,
                source_identity=IDENTITY,
                source_client_id="catchuparr-client-source-synthetic",
                candidate={"identity": IDENTITY},
                candidate_is_currently_allowed=lambda _candidate: True,
                launch_allowed_candidate=lambda _token, _candidate, op_id: (
                    events.append("launch")
                    or SharingEffectReceipt(op_id, "resource-synthetic-1")
                ),
            )
            self.assertEqual(result[0], "launched")
            self.assertEqual(result[1].status, "active")
            self.assertEqual(result[1].receipt.resource_id, "resource-synthetic-1")
            # The ledger validates once before and once inside the cross-process
            # claim boundary, then performs exactly one fallback side effect.
            self.assertEqual(events, ["cleanup", "validate", "validate", "launch"])
            again = coordinator.return_to_allowed_after_reap(
                RecorderAttemptResult(
                    "switch_requested", 2, 0,
                    attempt_switch_id=attempt_switch.attempt_id,
                ), token,
                switch_intent=switch_intent,
                attempt_switch=attempt_switch,
                ledger=ledger, fresh_validate=lambda _token: True,
                exact_cleanup_acknowledged=lambda: True,
                source_identity=IDENTITY,
                source_client_id="catchuparr-client-source-synthetic",
                candidate={"identity": IDENTITY},
                candidate_is_currently_allowed=lambda _candidate: True,
                launch_allowed_candidate=lambda *_args: self.fail("duplicate fallback launch"),
            )
            self.assertEqual(again, ("stale", None))

    def test_fallback_rejects_unallowed_candidate_without_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = SharingIntentLedger(Path(directory) / "state.sqlite", clock=lambda: 100.0)
            switch_intent, token, attempt_switch = _switch_and_fallback_tokens(ledger)
            outcome = coordinator.return_to_allowed_after_reap(
                RecorderAttemptResult(
                    "switch_requested", 0, 0,
                    attempt_switch_id=attempt_switch.attempt_id,
                ), token,
                switch_intent=switch_intent,
                attempt_switch=attempt_switch,
                ledger=ledger, fresh_validate=lambda _token: True,
                exact_cleanup_acknowledged=lambda: True,
                source_identity=IDENTITY,
                source_client_id="catchuparr-client-source-synthetic",
                candidate={"identity": IDENTITY},
                candidate_is_currently_allowed=lambda _candidate: False,
                launch_allowed_candidate=lambda *_args: self.fail("unallowed fallback launch"),
            )
            self.assertEqual(outcome, ("stale", None))

    def test_fallback_lost_ack_keeps_durable_cleanup_ownership(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "state.sqlite"
            ledger = SharingIntentLedger(database_path, clock=lambda: 100.0)
            switch_intent, token, attempt_switch = _switch_and_fallback_tokens(ledger)
            created_resources = []
            calls = []

            def launch(_token, _candidate, operation_id):
                calls.append(operation_id)
                created_resources.append((operation_id, "resource-synthetic-lost-ack"))
                raise RuntimeError("synthetic lost launch ACK")

            outcome = coordinator.return_to_allowed_after_reap(
                RecorderAttemptResult(
                    "switch_requested", 2, 0,
                    attempt_switch_id=attempt_switch.attempt_id,
                ), token,
                switch_intent=switch_intent,
                attempt_switch=attempt_switch,
                ledger=ledger,
                fresh_validate=lambda _token: True,
                exact_cleanup_acknowledged=lambda: True,
                source_identity=IDENTITY,
                source_client_id="catchuparr-client-source-synthetic",
                candidate={"identity": IDENTITY},
                candidate_is_currently_allowed=lambda _candidate: True,
                launch_allowed_candidate=launch,
            )
            self.assertEqual(outcome[0], "cleanup_pending")
            dispatch = outcome[1]
            self.assertEqual(dispatch.status, "cleanup_pending")
            operation = dispatch.operation
            self.assertEqual(operation.status, "cleanup_pending")
            self.assertEqual(operation.target_identity, IDENTITY)
            self.assertEqual(operation.source_identity, IDENTITY)
            self.assertEqual(operation.source_attempt_id, attempt_switch.attempt_id)
            self.assertEqual(operation.source_client_id, "catchuparr-client-source-synthetic")
            self.assertEqual(created_resources, [(operation.operation_id, "resource-synthetic-lost-ack")])

            # The consumed operation is never replayed, and a later epoch
            # cannot start until the exact unknown resource has been removed.
            repeated = coordinator.return_to_allowed_after_reap(
                RecorderAttemptResult(
                    "switch_requested", 2, 0,
                    attempt_switch_id=attempt_switch.attempt_id,
                ), token,
                switch_intent=switch_intent,
                attempt_switch=attempt_switch,
                ledger=ledger,
                fresh_validate=lambda _token: True,
                exact_cleanup_acknowledged=lambda: True,
                source_identity=IDENTITY,
                source_client_id="catchuparr-client-source-synthetic",
                candidate={"identity": IDENTITY},
                candidate_is_currently_allowed=lambda _candidate: True,
                launch_allowed_candidate=launch,
            )
            self.assertEqual(repeated, ("stale", None))
            self.assertEqual(calls, [operation.operation_id])

            # A fresh process-facing ledger sees the unresolved operation and
            # still cannot admit the next epoch until its exact absence ACK.
            ledger = SharingIntentLedger(database_path, clock=lambda: 100.0)
            current = ledger.read(CHANNEL)
            self.assertEqual(
                tuple(item.operation_id for item in ledger.pending_effect_attempts(CHANNEL)),
                (operation.operation_id,),
            )
            retry_state = replace(current.state, intent_epoch=current.state.intent_epoch + 1)
            with self.assertRaisesRegex(StaleSharingIntent, "exact cleanup ACK"):
                ledger.save(
                    retry_state, action=SharingAction.RETURN_TO_ALLOWED,
                    expected_revision=current.revision, not_before=100.0,
                )

            def remove_exact_resource(saved):
                self.assertEqual(saved.operation_id, operation.operation_id)
                self.assertIn((saved.operation_id, "resource-synthetic-lost-ack"), created_resources)
                created_resources.remove((saved.operation_id, "resource-synthetic-lost-ack"))
                return True

            self.assertTrue(ledger.reconcile_effect_attempt(
                operation.operation_id, exact_resources_absent=remove_exact_resource,
            ))
            self.assertEqual(created_resources, [])
            retry = ledger.save(
                retry_state, action=SharingAction.RETURN_TO_ALLOWED,
                expected_revision=current.revision, not_before=100.0,
            )
            self.assertEqual(retry.intent.intent_epoch, token.intent_epoch + 1)

    def test_old_attempt_switch_result_cannot_launch_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = SharingIntentLedger(Path(directory) / "state.sqlite", clock=lambda: 100.0)
            switch_intent, token, attempt_switch = _switch_and_fallback_tokens(ledger)
            unrelated_attempt = RecorderAttemptSwitch()
            outcome = coordinator.return_to_allowed_after_reap(
                RecorderAttemptResult(
                    "switch_requested", 0, 0,
                    attempt_switch_id=unrelated_attempt.attempt_id,
                ), token,
                switch_intent=switch_intent,
                attempt_switch=attempt_switch,
                ledger=ledger, fresh_validate=lambda _token: True,
                exact_cleanup_acknowledged=lambda: self.fail("stale cleanup check"),
                source_identity=IDENTITY,
                source_client_id="catchuparr-client-source-synthetic",
                candidate={"identity": IDENTITY},
                candidate_is_currently_allowed=lambda _candidate: True,
                launch_allowed_candidate=lambda *_args: self.fail("stale fallback launch"),
            )
            self.assertEqual(outcome, ("not_a_return_switch", None))

    def test_old_attempt_completion_does_not_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = SharingIntentLedger(Path(directory) / "state.sqlite", clock=lambda: 100.0)
            switch_intent, token, attempt_switch = _switch_and_fallback_tokens(ledger)
            outcome = coordinator.return_to_allowed_after_reap(
                RecorderAttemptResult("global_stop", 0, 0), token,
                switch_intent=switch_intent,
                attempt_switch=attempt_switch,
                ledger=ledger, fresh_validate=lambda _token: True,
                exact_cleanup_acknowledged=lambda: True,
                source_identity=IDENTITY,
                source_client_id="catchuparr-client-source-synthetic",
                candidate={"identity": IDENTITY},
                candidate_is_currently_allowed=lambda _candidate: True,
                launch_allowed_candidate=lambda *_args: self.fail("fallback after global stop"),
            )
            self.assertEqual(outcome, ("not_a_return_switch", None))

    def test_restart_cleanup_needs_exact_absence_ack_and_does_not_replay_open(self):
        with tempfile.TemporaryDirectory() as directory:
            episodes = BorrowEpisodeLedger(Path(directory) / "episodes.sqlite", clock=lambda: 100.0)
            token = SharingIntentToken(
                channel_uuid=CHANNEL, revision=1, intent_epoch=1,
                action=SharingAction.ATTACH_BORROW,
                configuration_generation="config-synthetic", control_generation=1,
                recorder_fence=2, target_identity=IDENTITY, target_admitted=True,
                not_before=100.0,
            )
            episode = episodes.begin_open(
                token, lease_digest="a" * 64, identity=IDENTITY,
                client_id="catchuparr-client-restart-synthetic",
            )
            unresolved = coordinator.reconcile_pending_borrow_cleanup(
                episodes, exact_resources_absent=lambda _episode: False,
            )
            self.assertEqual(unresolved.closed_episode_ids, ())
            self.assertEqual(unresolved.pending_episode_ids, (episode.episode_id,))
            self.assertEqual(episodes.read(CHANNEL), episode)

            resolved = coordinator.reconcile_pending_borrow_cleanup(
                episodes,
                exact_resources_absent=lambda saved: (
                    saved.episode_id == episode.episode_id
                    and saved.client_id == "catchuparr-client-restart-synthetic"
                ),
            )
            self.assertEqual(resolved.closed_episode_ids, (episode.episode_id,))
            self.assertEqual(resolved.pending_episode_ids, ())
            self.assertEqual(episodes.read(CHANNEL).phase, "closed")


if __name__ == "__main__":
    unittest.main()
