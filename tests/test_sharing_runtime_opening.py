import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from catchuparr.sharing_episode import BorrowEpisodeLedger
from catchuparr.sharing_persistence import SharingIntentLedger
from catchuparr.sharing_planner import (
    BorrowTarget,
    SharingAction,
    SharingObservation,
    SharingSettings,
    observe,
)
from catchuparr.sharing_runtime import prepare_borrow_opening

CHANNEL = "channel-runtime-opening-synthetic"
IDENTITY = (
    CHANNEL,
    "worker-runtime-opening-synthetic",
    "owner-runtime-opening-synthetic",
    "owner-runtime-opening-synthetic",
    "stream-runtime-opening-synthetic",
    "account-runtime-opening-synthetic",
    "m3u-runtime-opening-synthetic",
    "input-runtime-opening-synthetic",
)
LEASE = "lease-runtime-opening-synthetic"


def target():
    return BorrowTarget(
        channel_uuid=IDENTITY[0], worker_uuid=IDENTITY[1],
        owner_lock_value=IDENTITY[2], metadata_owner=IDENTITY[3],
        stream_id=IDENTITY[4], account_id=IDENTITY[5],
        m3u_profile_id=IDENTITY[6], input_profile_id=IDENTITY[7],
        ordinary_client_ids=("viewer-runtime-opening-synthetic",),
        owner_heartbeat_healthy=True, assigned_stream_matches=True,
        account_active=True, profile_active=True, media_compatible=True,
        useful_buffer_available=True, native_state_healthy=True,
    )


def attach_state():
    first = observe(None, SharingObservation(
        channel_uuid=CHANNEL, now=100, configuration_generation="config-synthetic",
        control_generation=2, recorder_fence=7, lease_is_current=True,
        settings=SharingSettings(True, 5, 30, 60), target=target(),
    ))
    ready = observe(first.state, SharingObservation(
        channel_uuid=CHANNEL, now=105, configuration_generation="config-synthetic",
        control_generation=2, recorder_fence=7, lease_is_current=True,
        settings=SharingSettings(True, 5, 30, 60), target=target(),
    ))
    if ready.action is not SharingAction.ATTACH_BORROW:
        raise AssertionError("synthetic target should be ready to attach")
    return ready.state


class SharingRuntimeOpeningTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.intent_ledger = SharingIntentLedger(root / "intent.sqlite", clock=lambda: 105.0)
        self.episode_ledger = BorrowEpisodeLedger(root / "episode.sqlite", clock=lambda: 105.0)
        self.saved = self.intent_ledger.save(
            attach_state(), action=SharingAction.ATTACH_BORROW,
            expected_revision=None, target_identity=IDENTITY,
            target_admitted=True, not_before=105.0,
        )

    def tearDown(self):
        self.temp.cleanup()

    def prepare(self, *, project=True, close=True, validate=lambda _token: True):
        with patch(
            "catchuparr.sharing_runtime.project_opening_episode", return_value=project,
        ) as project_opening, patch(
            "catchuparr.sharing_runtime.fence_unregistered_opening",
            return_value=True,
        ) as fence_opening, patch(
            "catchuparr.sharing_runtime.native_registration_is_absent",
            return_value=True,
        ) as check_absent, patch(
            "catchuparr.sharing_runtime.close_fenced_unregistered_opening",
            return_value=close,
        ) as close_opening:
            result = prepare_borrow_opening(
                object(), self.intent_ledger, self.episode_ledger, self.saved.intent,
                target(), lease_value=LEASE, catalog_generation="catalog-synthetic",
                return_seconds=30, minimum_switch_seconds=60, last_switch_at=None,
                capability_secret=b"runtime-opening-synthetic-secret-000000",
                fresh_validate=validate, clock=lambda: 105.0,
            )
        return result, project_opening, (fence_opening, check_absent, close_opening)

    def test_opening_claim_precedes_projection_and_signs_exact_persisted_episode(self):
        (claimed, prepared), projector, cleanup = self.prepare()
        self.assertTrue(claimed)
        self.assertIsNotNone(prepared)
        episode = self.episode_ledger.read(CHANNEL)
        self.assertEqual("opening", episode.phase)
        self.assertEqual(prepared.episode_id, episode.episode_id)
        self.assertEqual(prepared.episode_version, episode.version)
        self.assertEqual(prepared.client_id, episode.client_id)
        self.assertEqual(IDENTITY, prepared.capability.connection_identity)
        self.assertEqual(episode.episode_id, prepared.capability.episode_id)
        self.assertEqual(episode.intent_epoch, prepared.capability.intent_epoch)
        self.assertTrue(prepared.capability.verify(
            b"runtime-opening-synthetic-secret-000000", user_id="0", now=105.0,
        ))
        projector.assert_called_once()
        for step in cleanup:
            step.assert_not_called()

    def test_consumed_opening_token_cannot_project_or_sign_twice(self):
        (claimed, prepared), projector, _cleanup = self.prepare()
        self.assertTrue(claimed)
        self.assertIsNotNone(prepared)
        second, _projector, _cleanup = self.prepare()
        self.assertEqual((False, None), second)
        projector.assert_called_once()

    def test_stale_claim_never_creates_episode_or_projects(self):
        result, projector, cleanup = self.prepare(validate=lambda _token: False)
        self.assertEqual((False, None), result)
        self.assertIsNone(self.episode_ledger.read(CHANNEL) if self._episode_exists() else None)
        projector.assert_not_called()
        for step in cleanup:
            step.assert_not_called()

    def test_projection_failure_retains_cleanup_pending_without_absence_ack(self):
        with self.assertRaisesRegex(RuntimeError, "projection"):
            self.prepare(project=False, close=False)
        episode = self.episode_ledger.read(CHANNEL)
        self.assertEqual("cleanup_pending", episode.phase)

    def _episode_exists(self):
        try:
            self.episode_ledger.read(CHANNEL)
        except Exception:
            return False
        return True


if __name__ == "__main__":
    unittest.main()
