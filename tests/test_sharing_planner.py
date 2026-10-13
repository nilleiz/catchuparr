import unittest

from catchuparr.sharing_planner import (
    BorrowTarget,
    SharingAction,
    SharingObservation,
    SharingPhase,
    SharingSettings,
    observe,
    restore_state,
)
from catchuparr.sharing_planner import (
    resolve_transition as _resolve_transition,
)

CHANNEL = "00000000-0000-0000-0000-000000000007"


def target(
    *,
    worker="native-worker-1",
    owner="native-owner-1",
    clients=("viewer-1",),
    allowed=True,
    **overrides,
):
    values = {
        "channel_uuid": CHANNEL,
        "worker_uuid": worker,
        "owner_lock_value": owner,
        "metadata_owner": owner,
        "stream_id": "stream-7",
        "account_id": "account-7",
        "m3u_profile_id": "profile-7",
        "input_profile_id": "input-7",
        "ordinary_client_ids": tuple(clients),
        "owner_heartbeat_healthy": True,
        "assigned_stream_matches": True,
        "account_active": True,
        "profile_active": True,
        "media_compatible": True,
        "useful_buffer_available": True,
        "native_state_healthy": True,
        "account_allowed_by_recording_policy": allowed,
    }
    values.update(overrides)
    return BorrowTarget(**values)


def observation(
    now,
    *,
    candidate=None,
    settings=None,
    fence=11,
    lease=True,
    generation="config-a",
    control=3,
    **overrides,
):
    values = {
        "channel_uuid": CHANNEL,
        "now": now,
        "configuration_generation": generation,
        "control_generation": control,
        "recorder_fence": fence,
        "lease_is_current": lease,
        "settings": settings or SharingSettings(enabled=True),
        "target": candidate,
    }
    values.update(overrides)
    return SharingObservation(**values)


def target_from_identity(identity, *, clients=("viewer-1",)):
    if identity is None:
        return None
    (
        channel_uuid,
        worker_uuid,
        owner_lock_value,
        metadata_owner,
        stream_id,
        account_id,
        m3u_profile_id,
        input_profile_id,
    ) = identity
    return target(
        clients=clients,
        channel_uuid=channel_uuid,
        worker=worker_uuid,
        owner=owner_lock_value,
        metadata_owner=metadata_owner,
        stream_id=stream_id,
        account_id=account_id,
        m3u_profile_id=m3u_profile_id,
        input_profile_id=input_profile_id,
    )


def resolve_transition(state, action, *, succeeded, now):
    identity = (
        state.candidate_connection_identity
        if action == SharingAction.ATTACH_BORROW
        else state.borrowed_identity
    )
    current_observation = observation(
        now,
        candidate=target_from_identity(
            identity,
            clients=() if action != SharingAction.ATTACH_BORROW else ("viewer-1",),
        ),
        fence=state.recorder_fence,
        generation=state.configuration_generation,
        control=state.control_generation,
        dedicated_source_available=action != SharingAction.PAUSE_NO_ALLOWED_SOURCE,
    )
    return _resolve_transition(
        state,
        action,
        succeeded=succeeded,
        now=now,
        current_state=state,
        current_observation=current_observation,
    )


class SharingPlannerTests(unittest.TestCase):
    def test_live_target_stability_and_event_burst_coalesce_one_intent(self):
        initial = observation(100, candidate=target(allowed=False))
        decision = observe(None, initial)
        self.assertEqual(SharingAction.WAIT, decision.action)
        self.assertEqual(105, decision.deadline)

        state = decision.state
        for now in (101, 102, 103, 104):
            burst = observe(state, observation(now, candidate=target(allowed=False)))
            self.assertEqual(SharingAction.WAIT, burst.action)
            self.assertEqual(105, burst.deadline)
            state = burst.state

        ready = observe(state, observation(105, candidate=target(allowed=False)))
        self.assertEqual(SharingAction.ATTACH_BORROW, ready.action)
        duplicate = observe(ready.state, observation(105, candidate=target(allowed=False)))
        self.assertEqual(SharingAction.NOOP, duplicate.action)
        self.assertEqual(ready.state.pending_action, duplicate.state.pending_action)

        borrowed = resolve_transition(
            ready.state, SharingAction.ATTACH_BORROW, succeeded=True, now=105
        )
        self.assertEqual(SharingPhase.BORROWED, borrowed.phase)
        self.assertEqual(target(allowed=False).connection_identity, borrowed.borrowed_identity)
        self.assertEqual(105, borrowed.last_switch_at)

    def test_target_identity_change_restarts_live_stability(self):
        first = observe(None, observation(100, candidate=target()))
        self.assertEqual(105, first.deadline)
        changed = observe(
            first.state,
            observation(104, candidate=target(worker="native-worker-2", owner="native-owner-2")),
        )
        self.assertEqual(SharingAction.WAIT, changed.action)
        self.assertEqual(109, changed.deadline)

    def test_viewer_churn_keeps_live_stability_for_the_same_native_connection(self):
        first = observe(None, observation(100, candidate=target(clients=("viewer-1",))))
        more_viewers = observe(
            first.state,
            observation(102, candidate=target(clients=("viewer-1", "viewer-2"))),
        )
        fewer_viewers = observe(
            more_viewers.state,
            observation(104, candidate=target(clients=("viewer-2",))),
        )
        self.assertEqual(SharingAction.WAIT, more_viewers.action)
        self.assertEqual(105, more_viewers.deadline)
        self.assertEqual(SharingAction.WAIT, fewer_viewers.action)
        self.assertEqual(105, fewer_viewers.deadline)
        stable = observe(
            fewer_viewers.state,
            observation(105, candidate=target(clients=("viewer-2",))),
        )
        self.assertEqual(SharingAction.ATTACH_BORROW, stable.action)

    def test_last_viewer_return_uses_later_deadline_and_new_viewer_cancels_it(self):
        fast = SharingSettings(True, live_stability_seconds=0)
        attach = observe(None, observation(100, candidate=target(), settings=fast))
        self.assertEqual(SharingAction.ATTACH_BORROW, attach.action)
        borrowed = resolve_transition(
            attach.state, attach.action, succeeded=True, now=100
        )

        left = observe(borrowed, observation(101, candidate=target(clients=())))
        self.assertEqual(SharingAction.WAIT, left.action)
        self.assertEqual(160, left.deadline)  # max(101 + 30, 100 + 60)
        still_waiting = observe(left.state, observation(150, candidate=target(clients=())))
        self.assertEqual(160, still_waiting.deadline)

        joined = observe(
            still_waiting.state,
            observation(155, candidate=target(clients=("viewer-2",))),
        )
        self.assertEqual(SharingAction.NOOP, joined.action)
        self.assertEqual(SharingPhase.BORROWED, joined.state.phase)
        self.assertIsNone(joined.state.last_viewer_left_at)

        left_again = observe(joined.state, observation(156, candidate=target(clients=())))
        self.assertEqual(186, left_again.deadline)
        due = observe(left_again.state, observation(186, candidate=target(clients=())))
        self.assertEqual(SharingAction.RETURN_TO_ALLOWED, due.action)
        returned = resolve_transition(due.state, due.action, succeeded=True, now=186)
        self.assertEqual(SharingPhase.DEDICATED, returned.phase)
        self.assertEqual(186, returned.last_switch_at)

    def test_return_without_allowed_source_pauses_after_deadline(self):
        fast = SharingSettings(True, live_stability_seconds=0)
        attach = observe(None, observation(10, candidate=target(), settings=fast))
        borrowed = resolve_transition(attach.state, attach.action, succeeded=True, now=10)
        waiting = observe(borrowed, observation(11, candidate=target(clients=()), settings=fast))
        self.assertEqual(SharingAction.WAIT, waiting.action)
        self.assertEqual(70, waiting.deadline)
        due = observe(
            waiting.state,
            observation(
                70,
                candidate=target(clients=()),
                settings=fast,
                dedicated_source_available=False,
            ),
        )
        self.assertEqual(SharingAction.PAUSE_NO_ALLOWED_SOURCE, due.action)
        paused = resolve_transition(due.state, due.action, succeeded=True, now=70)
        self.assertEqual(SharingPhase.PAUSED, paused.phase)
        still_paused = observe(
            paused,
            observation(
                71,
                candidate=target(clients=()),
                settings=fast,
                dedicated_source_available=False,
            ),
        )
        self.assertEqual(SharingAction.NOOP, still_paused.action)
        self.assertEqual(SharingPhase.PAUSED, still_paused.state.phase)
        recovery = observe(
            still_paused.state,
            observation(
                72,
                candidate=target(clients=()),
                settings=fast,
                dedicated_source_available=True,
            ),
        )
        self.assertEqual(SharingAction.RETURN_TO_ALLOWED, recovery.action)
        failed_recovery = resolve_transition(
            recovery.state, recovery.action, succeeded=False, now=72
        )
        self.assertEqual(SharingPhase.PAUSED, failed_recovery.phase)
        retry_recovery = observe(
            failed_recovery,
            observation(
                73,
                candidate=target(clients=()),
                settings=fast,
                dedicated_source_available=True,
            ),
        )
        self.assertEqual(SharingAction.RETURN_TO_ALLOWED, retry_recovery.action)
        resumed = resolve_transition(
            retry_recovery.state, retry_recovery.action, succeeded=True, now=73
        )
        self.assertEqual(SharingPhase.DEDICATED, resumed.phase)

    def test_disabled_sharing_does_not_clear_paused_state_without_allowed_source(self):
        fast = SharingSettings(
            True,
            live_stability_seconds=0,
            last_viewer_return_seconds=0,
            min_switch_interval_seconds=0,
        )
        attach = observe(None, observation(10, candidate=target(), settings=fast))
        borrowed = resolve_transition(attach.state, attach.action, succeeded=True, now=10)
        pause = observe(
            borrowed,
            observation(
                11,
                candidate=target(clients=()),
                settings=fast,
                dedicated_source_available=False,
            ),
        )
        paused = resolve_transition(pause.state, pause.action, succeeded=True, now=11)
        disabled = observe(
            paused,
            observation(
                12,
                candidate=target(clients=()),
                settings=SharingSettings(),
                dedicated_source_available=False,
            ),
        )
        self.assertEqual(SharingAction.NOOP, disabled.action)
        self.assertEqual(SharingPhase.PAUSED, disabled.state.phase)
        source_returns = observe(
            disabled.state,
            observation(
                13,
                candidate=target(clients=()),
                settings=SharingSettings(),
                dedicated_source_available=True,
            ),
        )
        self.assertEqual(SharingAction.RETURN_TO_ALLOWED, source_returns.action)

    def test_disabled_sharing_with_no_allowed_source_enters_paused_phase(self):
        decision = observe(
            None,
            observation(
                10,
                candidate=target(clients=()),
                settings=SharingSettings(),
                dedicated_source_available=False,
            ),
        )
        self.assertEqual(SharingAction.NOOP, decision.action)
        self.assertEqual(SharingPhase.PAUSED, decision.state.phase)

    def test_paused_recorder_can_borrow_when_an_ordinary_viewer_returns(self):
        no_wait = SharingSettings(
            True,
            live_stability_seconds=0,
            last_viewer_return_seconds=0,
            min_switch_interval_seconds=0,
        )
        attach = observe(
            None,
            observation(10, candidate=target(), settings=no_wait),
        )
        borrowed = resolve_transition(attach.state, attach.action, succeeded=True, now=10)
        last_viewer = observe(
            borrowed,
            observation(
                11,
                candidate=target(clients=()),
                settings=no_wait,
                dedicated_source_available=False,
            ),
        )
        paused = resolve_transition(
            last_viewer.state, last_viewer.action, succeeded=True, now=11
        )
        self.assertEqual(SharingPhase.PAUSED, paused.phase)
        viewer_returns = observe(
            paused,
            observation(
                12,
                candidate=target(clients=("viewer-2",), allowed=False),
                settings=no_wait,
                dedicated_source_available=False,
            ),
        )
        self.assertEqual(SharingAction.ATTACH_BORROW, viewer_returns.action)

    def test_unhealthy_or_missing_owner_proof_never_starts_a_borrow(self):
        unhealthy = target(owner="lock-owner", metadata_owner="other-owner")
        denied = observe(None, observation(50, candidate=unhealthy))
        self.assertEqual(SharingAction.NOOP, denied.action)
        no_viewers = observe(None, observation(50, candidate=target(clients=())))
        self.assertEqual(SharingAction.NOOP, no_viewers.action)
        self.assertEqual(SharingPhase.DEDICATED, no_viewers.state.phase)

    def test_stale_lease_cannot_advance_or_emit_pending_transition(self):
        pending = observe(None, observation(100, candidate=target()))
        stale = observe(
            pending.state,
            observation(105, candidate=target(), lease=False),
        )
        self.assertEqual(SharingAction.NOOP, stale.action)
        self.assertEqual(pending.state, stale.state)
        current = observe(stale.state, observation(105, candidate=target()))
        self.assertEqual(SharingAction.ATTACH_BORROW, current.action)

    def test_configuration_or_control_generation_change_restarts_live_stability(self):
        pending = observe(None, observation(100, candidate=target()))
        applied = observe(
            pending.state,
            observation(104, candidate=target(), generation="config-b"),
        )
        self.assertEqual(SharingAction.WAIT, applied.action)
        self.assertEqual(109, applied.deadline)
        controlled = observe(
            applied.state,
            observation(106, candidate=target(), generation="config-b", control=4),
        )
        self.assertEqual(SharingAction.WAIT, controlled.action)
        self.assertEqual(111, controlled.deadline)

    def test_disabled_default_keeps_the_existing_dedicated_route(self):
        decision = observe(
            None,
            observation(100, candidate=target(), settings=SharingSettings()),
        )
        self.assertEqual(SharingAction.NOOP, decision.action)
        self.assertEqual(SharingPhase.DEDICATED, decision.state.phase)

    def test_restart_preserves_valid_cooldowns_but_never_a_borrowed_binding(self):
        pending = observe(None, observation(100, candidate=target()))
        restored = restore_state(
            pending.state.to_snapshot(),
            channel_uuid=CHANNEL,
            configuration_generation="config-a",
            control_generation=3,
            recorder_fence=12,
            now=104,
        )
        self.assertEqual(SharingPhase.DEDICATED, restored.phase)
        self.assertIsNone(restored.borrowed_identity)
        resumed = observe(restored, observation(104, candidate=target(), fence=12))
        self.assertEqual(SharingAction.WAIT, resumed.action)
        self.assertEqual(105, resumed.deadline)

        fast = SharingSettings(True, live_stability_seconds=0)
        attach = observe(None, observation(200, candidate=target(), settings=fast))
        borrowed = resolve_transition(attach.state, attach.action, succeeded=True, now=200)
        snapshot = borrowed.to_snapshot()
        self.assertNotIn("borrowed_identity", snapshot)
        restarted = restore_state(
            snapshot,
            channel_uuid=CHANNEL,
            configuration_generation="config-a",
            control_generation=3,
            recorder_fence=13,
            now=205,
        )
        self.assertIsNone(restarted.borrowed_identity)
        self.assertEqual(200, restarted.last_switch_at)

    def test_restart_preserves_switch_cooldown_across_small_backward_clock_jump(self):
        fast = SharingSettings(
            True,
            live_stability_seconds=0,
            last_viewer_return_seconds=0,
            min_switch_interval_seconds=60,
        )
        attach = observe(None, observation(100, candidate=target(), settings=fast))
        borrowed = resolve_transition(attach.state, attach.action, succeeded=True, now=100)
        restarted = restore_state(
            borrowed.to_snapshot(),
            channel_uuid=CHANNEL,
            configuration_generation="config-a",
            control_generation=3,
            recorder_fence=12,
            now=99,
        )
        self.assertEqual(99, restarted.last_switch_at)
        retried = observe(
            restarted,
            observation(99, candidate=target(worker="native-worker-2"), fence=12, settings=fast),
        )
        self.assertEqual(SharingAction.WAIT, retried.action)
        self.assertEqual(159, retried.deadline)

    def test_future_switch_clock_is_clamped_to_now_for_all_skew_sizes(self):
        fast = SharingSettings(
            True,
            live_stability_seconds=0,
            last_viewer_return_seconds=0,
            min_switch_interval_seconds=60,
        )
        for skew in (3600, 3601, 7200):
            with self.subTest(skew=skew):
                attach = observe(None, observation(100, candidate=target(), settings=fast))
                borrowed = resolve_transition(
                    attach.state, attach.action, succeeded=True, now=100
                )
                snapshot = borrowed.to_snapshot()
                snapshot["last_switch_at"] = 100 + skew
                restored = restore_state(
                    snapshot,
                    channel_uuid=CHANNEL,
                    configuration_generation="config-a",
                    control_generation=3,
                    recorder_fence=12,
                    now=100,
                )
                self.assertEqual(100, restored.last_switch_at)
                waiting = observe(
                    restored,
                    observation(100, candidate=target(), fence=12, settings=fast),
                )
                self.assertEqual(SharingAction.WAIT, waiting.action)
                self.assertEqual(160, waiting.deadline)

    def test_stale_completion_cannot_resolve_newer_same_action_intent(self):
        pending = observe(None, observation(100, candidate=target()))
        newer = pending.state.__class__(
            **{
                **pending.state.__dict__,
                "intent_epoch": pending.state.intent_epoch + 1,
            }
        )
        fresh = observation(105, candidate=target())
        result = _resolve_transition(
            pending.state,
            pending.action,
            succeeded=True,
            now=105,
            current_state=newer,
            current_observation=fresh,
        )
        self.assertIs(newer, result)
        self.assertEqual(SharingPhase.WAIT_LIVE, result.phase)
        self.assertIsNone(result.borrowed_identity)

    def test_same_action_completion_rejects_a_changed_native_target(self):
        pending = observe(None, observation(100, candidate=target()))
        changed_target = target(worker="native-worker-2", owner="native-owner-2")
        result = _resolve_transition(
            pending.state,
            pending.action,
            succeeded=True,
            now=105,
            current_state=pending.state,
            current_observation=observation(105, candidate=changed_target),
        )
        self.assertIs(pending.state, result)
        self.assertIsNone(result.borrowed_identity)

    def test_return_and_pause_completion_recheck_viewers_on_same_borrow(self):
        no_wait = SharingSettings(
            True,
            live_stability_seconds=0,
            last_viewer_return_seconds=0,
            min_switch_interval_seconds=0,
        )
        attach = observe(None, observation(10, candidate=target(), settings=no_wait))
        borrowed = resolve_transition(attach.state, attach.action, succeeded=True, now=10)
        returning = observe(
            borrowed,
            observation(11, candidate=target(clients=()), settings=no_wait),
        )
        self.assertEqual(SharingAction.RETURN_TO_ALLOWED, returning.action)
        viewer_rejoined = observation(
            12,
            candidate=target(clients=("viewer-2",)),
            settings=no_wait,
        )
        stale_return = _resolve_transition(
            returning.state,
            returning.action,
            succeeded=True,
            now=12,
            current_state=returning.state,
            current_observation=viewer_rejoined,
        )
        self.assertIs(returning.state, stale_return)

        pause = observe(
            borrowed,
            observation(
                11,
                candidate=target(clients=()),
                settings=no_wait,
                dedicated_source_available=False,
            ),
        )
        self.assertEqual(SharingAction.PAUSE_NO_ALLOWED_SOURCE, pause.action)
        viewer_rejoined_no_source = observation(
            12,
            candidate=target(clients=("viewer-2",)),
            settings=no_wait,
            dedicated_source_available=False,
        )
        stale_pause = _resolve_transition(
            pause.state,
            pause.action,
            succeeded=True,
            now=12,
            current_state=pause.state,
            current_observation=viewer_rejoined_no_source,
        )
        self.assertIs(pause.state, stale_pause)

    def test_stale_lease_or_generation_cannot_complete_pending_intent(self):
        pending = observe(None, observation(100, candidate=target()))
        for current in (
            observation(105, candidate=target(), lease=False),
            observation(105, candidate=target(), fence=12),
            observation(105, candidate=target(), generation="config-b"),
            observation(105, candidate=target(), control=4),
        ):
            with self.subTest(current=current):
                result = _resolve_transition(
                    pending.state,
                    pending.action,
                    succeeded=True,
                    now=105,
                    current_state=pending.state,
                    current_observation=current,
                )
                self.assertIs(pending.state, result)
                self.assertIsNone(result.borrowed_identity)

    def test_new_fence_preserves_successful_switch_cooldown_but_drops_target(self):
        fast = SharingSettings(
            True,
            live_stability_seconds=0,
            last_viewer_return_seconds=0,
            min_switch_interval_seconds=0,
        )
        attach = observe(None, observation(100, candidate=target(), settings=fast))
        borrowed = resolve_transition(attach.state, attach.action, succeeded=True, now=100)
        returning = observe(borrowed, observation(160, candidate=target(clients=()), settings=fast))
        dedicated = resolve_transition(
            returning.state, returning.action, succeeded=True, now=160
        )
        minimum = SharingSettings(
            True,
            live_stability_seconds=0,
            last_viewer_return_seconds=0,
            min_switch_interval_seconds=60,
        )
        on_new_lease = observe(
            dedicated,
            observation(170, candidate=target(worker="native-worker-2"), fence=12, settings=minimum),
        )
        self.assertEqual(SharingAction.WAIT, on_new_lease.action)
        self.assertEqual(220, on_new_lease.deadline)
        self.assertIsNone(on_new_lease.state.borrowed_identity)

    def test_explicit_stop_and_failure_bypass_all_cooldowns(self):
        waiting = observe(None, observation(100, candidate=target()))
        stopped = observe(
            waiting.state,
            observation(101, candidate=target(), explicit_stop=True),
        )
        self.assertEqual(SharingAction.STOP, stopped.action)
        self.assertEqual(SharingPhase.STOPPED, stopped.state.phase)

        fast = SharingSettings(True, live_stability_seconds=0)
        attach = observe(None, observation(200, candidate=target(), settings=fast))
        borrowed = resolve_transition(attach.state, attach.action, succeeded=True, now=200)
        failed = observe(
            borrowed,
            observation(201, candidate=target(clients=()), recorder_failure=True),
        )
        self.assertEqual(SharingAction.RETURN_TO_ALLOWED, failed.action)
        self.assertIn("bypass", failed.reason)

    def test_failed_transition_does_not_advance_switch_time_and_zero_disables_wait(self):
        no_wait = SharingSettings(
            True,
            live_stability_seconds=0,
            last_viewer_return_seconds=0,
            min_switch_interval_seconds=0,
        )
        attach = observe(None, observation(20, candidate=target(), settings=no_wait))
        self.assertEqual(SharingAction.ATTACH_BORROW, attach.action)
        failed_state = resolve_transition(
            attach.state, attach.action, succeeded=False, now=20
        )
        self.assertIsNone(failed_state.last_switch_at)
        retry = observe(failed_state, observation(21, candidate=target(), settings=no_wait))
        self.assertEqual(SharingAction.ATTACH_BORROW, retry.action)

    def test_failed_return_keeps_borrow_identity_and_retries_without_cooldown(self):
        fast = SharingSettings(
            True,
            live_stability_seconds=0,
            last_viewer_return_seconds=0,
            min_switch_interval_seconds=0,
        )
        attach = observe(None, observation(10, candidate=target(), settings=fast))
        borrowed = resolve_transition(attach.state, attach.action, succeeded=True, now=10)
        returning = observe(borrowed, observation(11, candidate=target(clients=()), settings=fast))
        self.assertEqual(SharingAction.RETURN_TO_ALLOWED, returning.action)
        failed = resolve_transition(returning.state, returning.action, succeeded=False, now=11)
        self.assertEqual(borrowed.borrowed_identity, failed.borrowed_identity)
        self.assertEqual(10, failed.last_switch_at)
        retried = observe(failed, observation(12, candidate=target(clients=()), settings=fast))
        self.assertEqual(SharingAction.RETURN_TO_ALLOWED, retried.action)

    def test_apply_generation_change_does_not_reset_successful_switch_interval(self):
        fast = SharingSettings(
            True,
            live_stability_seconds=0,
            last_viewer_return_seconds=0,
            min_switch_interval_seconds=0,
        )
        attach = observe(None, observation(100, candidate=target(), settings=fast))
        borrowed = resolve_transition(attach.state, attach.action, succeeded=True, now=100)
        returning = observe(borrowed, observation(101, candidate=target(clients=()), settings=fast))
        dedicated = resolve_transition(
            returning.state, returning.action, succeeded=True, now=101
        )
        settings = SharingSettings(
            True,
            live_stability_seconds=0,
            last_viewer_return_seconds=0,
            min_switch_interval_seconds=60,
        )
        reapplied = observe(
            dedicated,
            observation(110, candidate=target(), settings=settings, generation="config-b"),
        )
        self.assertEqual(SharingAction.WAIT, reapplied.action)
        self.assertEqual(161, reapplied.deadline)

    def test_apply_generation_invalidates_borrow_before_any_new_target(self):
        fast = SharingSettings(
            True,
            live_stability_seconds=0,
            last_viewer_return_seconds=0,
            min_switch_interval_seconds=60,
        )
        attach = observe(None, observation(100, candidate=target(), settings=fast))
        borrowed = resolve_transition(attach.state, attach.action, succeeded=True, now=100)
        replaced = observe(
            borrowed,
            observation(101, candidate=target(), settings=fast, generation="config-b"),
        )
        self.assertEqual(SharingAction.RETURN_TO_ALLOWED, replaced.action)
        dedicated = resolve_transition(
            replaced.state, replaced.action, succeeded=True, now=101
        )
        next_target = observe(
            dedicated,
            observation(110, candidate=target(), settings=fast, generation="config-b"),
        )
        self.assertEqual(SharingAction.WAIT, next_target.action)
        self.assertEqual(161, next_target.deadline)

    def test_snapshot_with_stale_generation_or_future_time_is_discarded(self):
        pending = observe(None, observation(100, candidate=target()))
        snapshot = pending.state.to_snapshot()
        wrong_generation = restore_state(
            snapshot,
            channel_uuid=CHANNEL,
            configuration_generation="config-b",
            control_generation=3,
            recorder_fence=12,
            now=104,
        )
        self.assertIsNone(wrong_generation.candidate_since)
        snapshot["candidate_since"] = 105
        future = restore_state(
            snapshot,
            channel_uuid=CHANNEL,
            configuration_generation="config-a",
            control_generation=3,
            recorder_fence=12,
            now=104,
        )
        self.assertIsNone(future.candidate_since)

    def test_restore_accepts_previous_v1_snapshot_without_pending_intent_epoch(self):
        pending = observe(None, observation(100, candidate=target()))
        snapshot = pending.state.to_snapshot()
        snapshot.pop("intent_epoch")
        snapshot["version"] = 1
        restored = restore_state(
            snapshot,
            channel_uuid=CHANNEL,
            configuration_generation="config-a",
            control_generation=3,
            recorder_fence=12,
            now=104,
        )
        self.assertEqual(0, restored.intent_epoch)
        self.assertEqual(100, restored.candidate_since)

    def test_restore_and_transition_reject_invalid_fences_and_timestamps(self):
        snapshot = observe(None, observation(100, candidate=target())).state.to_snapshot()
        with self.assertRaises(ValueError):
            restore_state(
                snapshot,
                channel_uuid=CHANNEL,
                configuration_generation="config-a",
                control_generation=True,
                recorder_fence=12,
                now=104,
            )
        with self.assertRaises(ValueError):
            restore_state(
                snapshot,
                channel_uuid=CHANNEL,
                configuration_generation="config-a",
                control_generation=3,
                recorder_fence=12,
                now=float("nan"),
            )
        pending = observe(None, observation(100, candidate=target()))
        with self.assertRaises(ValueError):
            resolve_transition(
                pending.state, pending.action, succeeded=1, now=105
            )
        with self.assertRaises(ValueError):
            resolve_transition(
                pending.state, pending.action, succeeded=True, now=float("inf")
            )


if __name__ == "__main__":
    unittest.main()
