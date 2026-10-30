import random
import unittest
from dataclasses import replace

from catchuparr.sharing_return_tail import (
    ACTIVE_PHASES,
    JOIN_TIMEOUT_SECONDS,
    MIN_LEAD_SECONDS,
    SEAM_MARGIN_SECONDS,
    TAIL_ALLOWANCE_SECONDS,
    TAIL_CLEANUP_SECONDS,
    TAIL_LEAD_SECONDS,
    Publisher,
    ReturnRequest,
    TailAction,
    TailEvent,
    TailEventKind,
    TailPhase,
    advance,
    plan_return,
    restore,
)

CHANNEL = "00000000-0000-0000-0000-000000000009"
TAIL = "tail-1"
CLIENT = "catchuparr-client-0123456789abcdef"
T0 = 1_000_000.0
RD = T0 + 15  # native return deadline, inside the lead window
HARD = RD + 90 + TAIL_ALLOWANCE_SECONDS
K = TailEventKind
A = TailAction


def request(**overrides):
    values = {
        "channel_uuid": CHANNEL,
        "tail_id": TAIL,
        "recorder_fence": 11,
        "now": T0,
        "return_deadline": RD,
        "borrowed_account_id": "account-delayed",
        "dedicated_account_id": "account-fast",
        "delayed_account_ids": frozenset({"account-delayed"}),
        "max_overlap_seconds": 90,
        "synchronize_before_switch": True,
    }
    values.update(overrides)
    return ReturnRequest(**values)


def event(kind, at, *, tail_id=TAIL, client_id=None):
    return TailEvent(kind, tail_id, at, client_id)


def drive(*steps, **overrides):
    """Plan an eligible return and apply ``(kind, at[, client_id])`` steps."""
    decision = plan_return(request(**overrides))
    for step in steps:
        kind, at = step[0], step[1]
        client = step[2] if len(step) > 2 else (CLIENT if kind is K.TAIL_REGISTERED else None)
        decision = advance(decision.state, event(kind, at, client_id=client))
    return decision


REGISTERED = ((K.TAIL_REGISTERED, T0 + 1),)
DUE = REGISTERED + ((K.RETURN_DEADLINE, RD),)
JOINED = DUE + ((K.TAIL_JOINED, RD + 2),)
CLEANED = JOINED + ((K.BORROW_CLEANED, RD + 4), (K.RETURN_TOKEN, RD + 5))
STAGED = CLEANED + ((K.DEDICATED_STAGED, RD + 6),)


class EligibilityTests(unittest.TestCase):
    def test_eligible_return_opens_a_tail_and_keeps_the_borrow_publishing(self):
        decision = plan_return(request())
        self.assertEqual(decision.state.phase, TailPhase.OPENING)
        self.assertEqual(decision.actions, (A.OPEN_TAIL,))
        self.assertEqual(decision.state.publisher, Publisher.BORROW)
        self.assertEqual(decision.state.opened_at, T0)
        self.assertEqual(decision.state.return_deadline, RD)
        self.assertEqual(decision.state.join_deadline, RD + JOIN_TIMEOUT_SECONDS)
        self.assertEqual(decision.state.hard_deadline, HARD)
        self.assertNotIn(A.CLEANUP_BORROW, decision.actions)

    def test_ineligible_returns_run_exactly_as_today(self):
        cases = {
            "sync disabled": {"synchronize_before_switch": False},
            "overlap unset": {"max_overlap_seconds": None},
            "overlap below range": {"max_overlap_seconds": 9},
            "overlap above range": {"max_overlap_seconds": 121},
            "overlap not int": {"max_overlap_seconds": 30.0},
            "overlap bool": {"max_overlap_seconds": True},
            "borrow not delayed": {"borrowed_account_id": "account-other"},
            "dedicated delayed": {"dedicated_account_id": "account-delayed"},
            "dedicated unknown": {"dedicated_account_id": ""},
            "no delayed sources": {"delayed_account_ids": frozenset()},
            "no tail id": {"tail_id": ""},
            "lead too short": {"return_deadline": T0 + MIN_LEAD_SECONDS - 0.1},
            "deadline passed": {"return_deadline": T0},
            "deadline not finite": {"return_deadline": float("inf")},
        }
        for name, overrides in cases.items():
            with self.subTest(name):
                decision = plan_return(request(**overrides))
                self.assertEqual(decision.state.phase, TailPhase.IDLE)
                self.assertEqual(decision.actions, (A.PLAIN_RETURN,))
                self.assertEqual(decision.state.publisher, Publisher.BORROW)
                self.assertEqual(decision.state.outcome, "ineligible")

    def test_request_before_the_lead_window_waits(self):
        decision = plan_return(request(return_deadline=T0 + TAIL_LEAD_SECONDS + 0.1))
        self.assertEqual(decision.state.phase, TailPhase.IDLE)
        self.assertEqual(decision.actions, ())
        self.assertEqual(decision.state.outcome, "not_yet")

    def test_lead_window_bounds_are_inclusive(self):
        for lead in (MIN_LEAD_SECONDS, TAIL_LEAD_SECONDS):
            with self.subTest(lead=lead):
                self.assertEqual(plan_return(request(return_deadline=T0 + lead)).state.phase,
                                 TailPhase.OPENING)

    def test_overlap_bounds_are_inclusive(self):
        for seconds in (10, 120):
            with self.subTest(seconds=seconds):
                self.assertEqual(plan_return(request(max_overlap_seconds=seconds)).state.phase,
                                 TailPhase.OPENING)


class JoinTests(unittest.TestCase):
    def test_registration_with_private_client_id_starts_the_join(self):
        decision = drive((K.TAIL_REGISTERED, T0 + 1))
        self.assertEqual(decision.state.phase, TailPhase.JOINING)
        self.assertEqual(decision.state.client_id, CLIENT)
        self.assertEqual(decision.actions, ())
        self.assertEqual(decision.state.publisher, Publisher.BORROW)

    def test_registration_with_unclassifiable_client_falls_back(self):
        decision = drive((K.TAIL_REGISTERED, T0 + 1, "ordinary-client"))
        self.assertEqual(decision.state.phase, TailPhase.CLOSING)
        self.assertEqual(set(decision.actions), {A.CLOSE_TAIL, A.PLAIN_RETURN})
        self.assertEqual(decision.state.outcome, "invalid_client")

    def test_join_before_the_return_deadline_is_ignored(self):
        decision = drive(*REGISTERED, (K.TAIL_JOINED, T0 + 4))
        self.assertEqual(decision.state.phase, TailPhase.JOINING)
        self.assertEqual(decision.actions, ())
        self.assertEqual(decision.state.publisher, Publisher.BORROW)

    def test_return_deadline_marks_the_return_due(self):
        for steps in (((K.RETURN_DEADLINE, RD),), DUE):
            with self.subTest(steps=len(steps)):
                decision = drive(*steps)
                self.assertTrue(decision.state.return_due)
                self.assertEqual(decision.actions, ())
                self.assertIn(decision.state.phase, {TailPhase.OPENING, TailPhase.JOINING})

    def test_viewer_return_before_the_deadline_keeps_the_borrow(self):
        for steps in ((), REGISTERED):
            with self.subTest(steps=len(steps)):
                decision = drive(*steps, (K.VIEWER_RETURNED, T0 + 5))
                self.assertEqual(decision.state.phase, TailPhase.CLOSING)
                self.assertEqual(decision.actions, (A.CLOSE_TAIL,))
                self.assertEqual(decision.state.publisher, Publisher.BORROW)
                self.assertEqual(decision.state.outcome, "viewer_returned")

    def test_viewer_after_the_deadline_waits_for_the_tail(self):
        for steps in (DUE, JOINED, STAGED):
            with self.subTest(steps=len(steps)):
                state = drive(*steps).state
                decision = advance(state, event(K.VIEWER_RETURNED, RD + 7))
                self.assertEqual(decision.state, state)
                self.assertEqual(decision.actions, ())

    def test_join_hands_publication_to_the_tail_before_borrow_cleanup(self):
        decision = drive(*JOINED)
        self.assertEqual(decision.state.phase, TailPhase.TAILING)
        self.assertEqual(decision.actions, (A.CLEANUP_BORROW,))
        self.assertEqual(decision.state.publisher, Publisher.TAIL)

    def test_join_failure_closes_the_tail_and_returns_as_today(self):
        for steps in (((K.TAIL_JOIN_FAILED, T0 + 2),),
                      REGISTERED + ((K.TAIL_JOIN_FAILED, T0 + 5),),
                      DUE + ((K.TAIL_JOIN_FAILED, RD + 3),)):
            with self.subTest(steps=steps):
                decision = drive(*steps)
                self.assertEqual(decision.state.phase, TailPhase.CLOSING)
                self.assertEqual(set(decision.actions), {A.CLOSE_TAIL, A.PLAIN_RETURN})
                self.assertEqual(decision.state.publisher, Publisher.BORROW)
                self.assertEqual(decision.state.outcome, "join_failed")

    def test_join_deadline_follows_the_return_deadline(self):
        before = drive(*DUE, (K.TICK, RD + JOIN_TIMEOUT_SECONDS - 0.1))
        self.assertEqual(before.state.phase, TailPhase.JOINING)
        self.assertEqual(before.actions, ())
        after = advance(before.state, event(K.TICK, RD + JOIN_TIMEOUT_SECONDS))
        self.assertEqual(after.state.phase, TailPhase.CLOSING)
        self.assertEqual(set(after.actions), {A.CLOSE_TAIL, A.PLAIN_RETURN})
        self.assertEqual(after.state.outcome, "join_failed")


class TailingTests(unittest.TestCase):
    def test_return_token_before_borrow_cleanup_is_ignored(self):
        decision = drive(*JOINED, (K.RETURN_TOKEN, RD + 3))
        self.assertEqual(decision.actions, ())
        self.assertFalse(decision.state.dedicated_requested)

    def test_dedicated_is_staged_only_under_the_token_after_cleanup(self):
        cleaned = drive(*JOINED, (K.BORROW_CLEANED, RD + 4))
        self.assertEqual(cleaned.state.phase, TailPhase.TAILING)
        self.assertEqual(cleaned.actions, ())
        self.assertTrue(cleaned.state.borrow_cleaned)
        token = advance(cleaned.state, event(K.RETURN_TOKEN, RD + 5))
        self.assertEqual(token.actions, (A.STAGE_DEDICATED,))
        self.assertEqual(token.state.phase, TailPhase.TAILING)
        self.assertEqual(token.state.publisher, Publisher.TAIL)
        repeated = advance(token.state, event(K.RETURN_TOKEN, RD + 5.5))
        self.assertEqual(repeated.actions, ())

    def test_staged_dedicated_starts_the_seam_search_with_bounded_deadline(self):
        decision = drive(*STAGED)
        self.assertEqual(decision.state.phase, TailPhase.SEEKING)
        self.assertEqual(decision.state.publisher, Publisher.TAIL)
        self.assertEqual(decision.state.seam_deadline,
                         min(HARD, RD + 6 + 90 + SEAM_MARGIN_SECONDS))

    def test_seam_deadline_never_exceeds_the_hard_deadline(self):
        late = CLEANED + ((K.DEDICATED_STAGED, RD + 55),)
        decision = drive(*late)
        self.assertEqual(decision.state.seam_deadline, decision.state.hard_deadline)

    def test_tail_failure_before_dedicated_closes_the_tail(self):
        decision = drive(*JOINED, (K.TAIL_FAILED, RD + 9))
        self.assertEqual(decision.state.phase, TailPhase.CLOSING)
        self.assertEqual(decision.actions, (A.CLOSE_TAIL,))
        self.assertEqual(decision.state.publisher, Publisher.NONE)
        self.assertEqual(decision.state.outcome, "tail_failed")

    def test_hard_deadline_without_token_closes_the_tail(self):
        decision = drive(*JOINED, (K.TICK, HARD))
        self.assertEqual(decision.state.phase, TailPhase.CLOSING)
        self.assertEqual(decision.actions, (A.CLOSE_TAIL,))
        self.assertEqual(decision.state.publisher, Publisher.NONE)
        self.assertEqual(decision.state.outcome, "deadline")

    def test_dedicated_failure_closes_the_tail(self):
        for steps in (CLEANED + ((K.DEDICATED_FAILED, RD + 6),),
                      STAGED + ((K.DEDICATED_FAILED, RD + 20),)):
            with self.subTest(steps=steps[-1]):
                decision = drive(*steps)
                self.assertEqual(decision.state.phase, TailPhase.CLOSING)
                self.assertEqual(decision.actions, (A.CLOSE_TAIL,))
                self.assertEqual(decision.state.publisher, Publisher.NONE)
                self.assertEqual(decision.state.outcome, "dedicated_failed")


class SeamTests(unittest.TestCase):
    def test_seam_switches_to_dedicated_and_closes_the_tail(self):
        decision = drive(*STAGED, (K.SEAM_FOUND, RD + 80))
        self.assertEqual(decision.state.phase, TailPhase.CLOSING)
        self.assertEqual(decision.actions, (A.SWITCH_TO_DEDICATED, A.CLOSE_TAIL))
        self.assertEqual(decision.state.publisher, Publisher.DEDICATED)
        self.assertEqual(decision.state.outcome, "seam")
        self.assertEqual(decision.state.closing_at, RD + 80)
        self.assertEqual(decision.state.cleanup_deadline, RD + 80 + TAIL_CLEANUP_SECONDS)

    def test_seam_outside_the_search_is_ignored(self):
        decision = drive(*CLEANED, (K.SEAM_FOUND, RD + 9))
        self.assertEqual(decision.state.phase, TailPhase.TAILING)
        self.assertEqual(decision.actions, ())

    def test_seam_deadline_falls_back_to_the_unsynchronized_return(self):
        state = drive(*STAGED).state
        decision = advance(state, event(K.TICK, state.seam_deadline))
        self.assertEqual(decision.actions, (A.FALLBACK_TO_DEDICATED, A.CLOSE_TAIL))
        self.assertEqual(decision.state.publisher, Publisher.DEDICATED)
        self.assertEqual(decision.state.outcome, "deadline")

    def test_tail_failure_during_search_falls_back(self):
        decision = drive(*STAGED, (K.TAIL_FAILED, RD + 30))
        self.assertEqual(decision.actions, (A.FALLBACK_TO_DEDICATED, A.CLOSE_TAIL))
        self.assertEqual(decision.state.publisher, Publisher.DEDICATED)
        self.assertEqual(decision.state.outcome, "tail_failed")


class StopAndCleanupTests(unittest.TestCase):
    def test_stop_in_every_active_phase_closes_without_switching(self):
        prefixes = {
            TailPhase.OPENING: (),
            TailPhase.JOINING: REGISTERED,
            TailPhase.TAILING: CLEANED,
            TailPhase.SEEKING: STAGED,
        }
        for phase, steps in prefixes.items():
            with self.subTest(phase=phase):
                state = drive(*steps).state
                self.assertEqual(state.phase, phase)
                decision = advance(state, event(K.STOP, RD + 10))
                self.assertEqual(decision.state.phase, TailPhase.CLOSING)
                self.assertEqual(decision.state.publisher, Publisher.NONE)
                self.assertEqual(decision.state.outcome, "stopped")
                expected = {A.CLOSE_TAIL}
                if phase is TailPhase.SEEKING:
                    expected.add(A.CLOSE_DEDICATED_STAGED)
                self.assertEqual(set(decision.actions), expected)
                self.assertFalse({A.SWITCH_TO_DEDICATED, A.FALLBACK_TO_DEDICATED,
                                  A.PLAIN_RETURN, A.STAGE_DEDICATED} & set(decision.actions))

    def test_stop_while_closing_adds_nothing(self):
        state = drive(*STAGED, (K.SEAM_FOUND, RD + 80)).state
        decision = advance(state, event(K.STOP, RD + 81))
        self.assertEqual(decision.actions, ())
        self.assertEqual(decision.state.publisher, Publisher.DEDICATED)

    def test_acknowledged_close_is_terminal(self):
        closing = drive(*STAGED, (K.SEAM_FOUND, RD + 80)).state
        closed = advance(closing, event(K.TAIL_CLOSED, RD + 81))
        self.assertEqual(closed.state.phase, TailPhase.CLOSED)
        self.assertEqual(closed.actions, ())
        self.assertEqual(closed.state.publisher, Publisher.DEDICATED)
        for kind in K:
            with self.subTest(kind=kind):
                after = advance(closed.state, event(kind, T0 + 500))
                self.assertEqual(after.state, closed.state)
                self.assertEqual(after.actions, ())

    def test_failed_close_is_retried_then_stops_the_recorder(self):
        closing = drive(*STAGED, (K.SEAM_FOUND, RD + 80)).state
        pending = advance(closing, event(K.TAIL_CLOSE_FAILED, RD + 81))
        self.assertEqual(pending.state.phase, TailPhase.CLEANUP_PENDING)
        retry = advance(pending.state, event(K.TICK, RD + 85))
        self.assertEqual(retry.actions, (A.CLOSE_TAIL,))
        self.assertEqual(retry.state.cleanup_deadline, RD + 80 + TAIL_CLEANUP_SECONDS)
        expired = advance(retry.state, event(K.TICK, RD + 80 + TAIL_CLEANUP_SECONDS))
        self.assertEqual(expired.state.phase, TailPhase.CLEANUP_PENDING)
        self.assertIn(A.STOP_RECORDER, expired.actions)
        late = advance(expired.state, event(K.TAIL_CLOSED, RD + 200))
        self.assertEqual(late.state.phase, TailPhase.CLOSED)

    def test_unacknowledged_closing_also_stops_after_the_cleanup_deadline(self):
        closing = drive(*JOINED, (K.TAIL_FAILED, RD + 9)).state
        decision = advance(closing, event(K.TICK, RD + 9 + TAIL_CLEANUP_SECONDS))
        self.assertIn(A.STOP_RECORDER, decision.actions)


class EventHygieneTests(unittest.TestCase):
    def test_events_for_another_tail_change_nothing(self):
        state = drive(*STAGED).state
        for kind in K:
            with self.subTest(kind=kind):
                decision = advance(state, event(kind, RD + 30, tail_id="tail-2", client_id=CLIENT))
                self.assertEqual(decision.state, state)
                self.assertEqual(decision.actions, ())

    def test_duplicate_events_are_idempotent(self):
        joined = drive(*JOINED)
        again = advance(joined.state, event(K.TAIL_JOINED, RD + 3))
        self.assertEqual(again.state, joined.state)
        self.assertEqual(again.actions, ())
        registered = advance(joined.state, event(K.TAIL_REGISTERED, RD + 3, client_id="catchuparr-client-other"))
        self.assertEqual(registered.state.client_id, CLIENT)
        self.assertEqual(registered.actions, ())

    def test_no_event_extends_a_deadline(self):
        state = drive(*STAGED).state
        hard, seam = state.hard_deadline, state.seam_deadline
        for kind in (K.TAIL_REGISTERED, K.RETURN_DEADLINE, K.TAIL_JOINED, K.BORROW_CLEANED,
                     K.RETURN_TOKEN, K.DEDICATED_STAGED, K.VIEWER_RETURNED, K.TICK):
            state = advance(state, event(kind, RD + 40, client_id=CLIENT)).state
        self.assertEqual((state.hard_deadline, state.seam_deadline), (hard, seam))


class InvariantTests(unittest.TestCase):
    def test_random_sequences_keep_the_ordering_invariants(self):
        rng = random.Random(7)
        kinds = list(K)
        for _ in range(400):
            decision = plan_return(request())
            now = T0
            seen_closing = False
            cleaned = staged = False
            for _ in range(30):
                now += rng.choice((0.0, 0.5, 3.0, 20.0))
                kind = rng.choice(kinds)
                client = CLIENT if rng.random() < 0.9 else "ordinary-client"
                previous = decision.state
                decision = advance(previous, event(kind, now, client_id=client))
                state, actions = decision.state, set(decision.actions)
                cleaned = cleaned or state.borrow_cleaned
                staged = staged or state.dedicated_staged
                if A.CLEANUP_BORROW in actions:
                    self.assertEqual(previous.phase, TailPhase.JOINING)
                    self.assertTrue(state.return_due)
                    self.assertGreaterEqual(now, RD)
                    self.assertEqual(state.publisher, Publisher.TAIL)
                if A.STAGE_DEDICATED in actions:
                    self.assertTrue(previous.borrow_cleaned)
                if A.SWITCH_TO_DEDICATED in actions or A.FALLBACK_TO_DEDICATED in actions:
                    self.assertTrue(cleaned and staged)
                    self.assertEqual(state.publisher, Publisher.DEDICATED)
                if state.publisher is Publisher.DEDICATED:
                    self.assertTrue(cleaned and staged)
                if seen_closing:
                    self.assertNotIn(state.phase, ACTIVE_PHASES)
                seen_closing = seen_closing or state.phase not in ACTIVE_PHASES
                if state.phase in ACTIVE_PHASES:
                    self.assertLess(now, state.hard_deadline)
                if (kind is K.TICK and state.phase in {TailPhase.CLOSING, TailPhase.CLEANUP_PENDING}
                        and now >= state.cleanup_deadline):
                    self.assertIn(A.STOP_RECORDER, actions)


class PersistenceTests(unittest.TestCase):
    def test_snapshot_holds_identity_and_deadlines_only(self):
        state = drive(*STAGED).state
        snapshot = state.to_snapshot()
        self.assertEqual(snapshot["version"], 1)
        self.assertEqual(snapshot["tail_id"], TAIL)
        self.assertEqual(snapshot["channel_uuid"], CHANNEL)
        self.assertEqual(snapshot["phase"], "seeking")
        self.assertEqual(snapshot["client_id"], CLIENT)
        self.assertEqual(snapshot["hard_deadline"], state.hard_deadline)
        text = repr(snapshot).lower()
        for forbidden in ("url", "token", "capability", "http"):
            self.assertNotIn(forbidden, text)

    def test_restore_never_resumes_a_live_tail(self):
        for steps in ((), REGISTERED, DUE, JOINED, STAGED):
            with self.subTest(steps=len(steps)):
                snapshot = drive(*steps).state.to_snapshot()
                decision = restore(snapshot, channel_uuid=CHANNEL, now=RD + 300)
                self.assertEqual(decision.state.phase, TailPhase.CLOSING)
                self.assertEqual(decision.actions, (A.CLOSE_TAIL,))
                self.assertEqual(decision.state.publisher, Publisher.NONE)
                self.assertEqual(decision.state.outcome, "recovered")
                self.assertEqual(decision.state.client_id, snapshot["client_id"])

    def test_restore_of_a_closed_tail_is_a_no_op(self):
        closing = drive(*STAGED, (K.SEAM_FOUND, RD + 80)).state
        closed = advance(closing, event(K.TAIL_CLOSED, RD + 81)).state
        decision = restore(closed.to_snapshot(), channel_uuid=CHANNEL, now=RD + 300)
        self.assertEqual(decision.state.phase, TailPhase.CLOSED)
        self.assertEqual(decision.actions, ())

    def test_unvalidated_snapshot_closes_every_tail_client_of_the_channel(self):
        valid = drive(*STAGED).state.to_snapshot()
        broken = [
            {},
            {**valid, "version": 2},
            {**valid, "phase": "unknown"},
            {**valid, "client_id": "ordinary-client"},
            {**valid, "hard_deadline": "soon"},
            {**valid, "extra": 1},
        ]
        for snapshot in broken:
            with self.subTest(snapshot=sorted(snapshot)):
                decision = restore(snapshot, channel_uuid=CHANNEL, now=RD + 300)
                self.assertEqual(decision.state.phase, TailPhase.CLEANUP_PENDING)
                self.assertEqual(decision.actions, (A.CLOSE_TAIL,))
                self.assertIsNone(decision.state.client_id)
                self.assertEqual(decision.state.publisher, Publisher.NONE)

    def test_snapshot_of_another_channel_is_not_trusted(self):
        snapshot = drive(*STAGED).state.to_snapshot()
        decision = restore(snapshot, channel_uuid="00000000-0000-0000-0000-000000000010", now=RD + 300)
        self.assertEqual(decision.state.phase, TailPhase.CLEANUP_PENDING)
        self.assertIsNone(decision.state.client_id)

    def test_round_trip_of_replaced_fields_keeps_equality(self):
        state = replace(drive(*JOINED).state, outcome=None)
        self.assertEqual(state.to_snapshot(), replace(state).to_snapshot())


if __name__ == "__main__":
    unittest.main()
