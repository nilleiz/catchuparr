import sys
import tempfile
import types
import unittest
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
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

    def test_durable_cleanup_scanner_rebuilds_ack_and_manager_after_restart(self):
        from catchuparr.sharing_runtime import retry_durable_open_cleanups

        opening = self.episode_ledger.begin_open(
            self.saved.intent,
            lease_digest=sha256(LEASE.encode()).hexdigest(),
            identity=IDENTITY,
            client_id="catchuparr-client-runtime-recovery-synthetic",
        )
        attempt = self.episode_ledger.prepare_registration(
            opening, local_worker_id="catchuparr-reader-runtime-recovery-synthetic",
            backend_signature="d" * 64, user_agent="Catchuparr reader",
            client_ip="127.0.0.1",
        )
        ack = SimpleNamespace(
            episode_id=opening.episode_id, channel_uuid=opening.channel_uuid,
            client_id=attempt.client_id, local_worker_id=attempt.local_worker_id,
            redis_backend_signature=attempt.backend_signature,
            connected_at="105.5", projection_version=opening.version + 1,
            user_agent=attempt.user_agent, client_ip=attempt.client_ip,
        )
        self.episode_ledger.record_registration_ack(opening.episode_id, ack)
        self.episode_ledger.revoke(
            opening.episode_id, expected_version=opening.version,
            reason="cleanup_pending", cleanup_acknowledged=False,
        )
        managers = []

        class Manager:
            def __init__(self, *, channel_id, redis_client, worker_id):
                self.channel_id = channel_id
                self.redis_client = redis_client
                self.worker_id = worker_id
                self.stopped = 0
                managers.append(self)

            def stop(self):
                self.stopped += 1

        live_proxy = types.ModuleType("apps.proxy.live_proxy")
        live_proxy.__path__ = []
        client_manager = types.ModuleType("apps.proxy.live_proxy.client_manager")
        client_manager.ClientManager = Manager
        redis_text = object()
        redis_keys = {
            "apps": types.ModuleType("apps"),
            "apps.proxy": types.ModuleType("apps.proxy"),
            "apps.proxy.live_proxy": live_proxy,
            "apps.proxy.live_proxy.client_manager": client_manager,
        }
        with patch.dict(sys.modules, redis_keys), patch(
            "catchuparr.adapters.sharing_reader.redis_backend_signature",
            return_value=attempt.backend_signature,
        ), patch(
            "catchuparr.adapters.sharing_native.read_native_episode_grant",
            return_value=SimpleNamespace(
                episode_id=opening.episode_id, catalog_generation="catalog-synthetic",
            ),
        ), patch(
            "catchuparr.adapters.sharing_native.cleanup_borrowed_client",
            return_value=True,
        ) as cleanup:
            self.assertEqual((1, 0), retry_durable_open_cleanups(
                self.episode_ledger, redis_text, now=300.0,
            ))
        self.assertEqual(1, managers[0].stopped)
        self.assertEqual("closed", self.episode_ledger.read(CHANNEL).phase)
        cleanup.assert_called_once()
        rebuilt_ack = cleanup.call_args.args[1]
        self.assertEqual(ack.client_id, rebuilt_ack.client_id)
        self.assertEqual(ack.connected_at, rebuilt_ack.connected_at)

    def test_durable_cleanup_scanner_fences_uncertain_registration_before_stop(self):
        from catchuparr.sharing_runtime import retry_durable_open_cleanups

        opening = self.episode_ledger.begin_open(
            self.saved.intent,
            lease_digest=sha256(LEASE.encode()).hexdigest(),
            identity=IDENTITY,
            client_id="catchuparr-client-unknown-recovery-synthetic",
        )
        self.episode_ledger.prepare_registration(
            opening, local_worker_id="catchuparr-reader-unknown-recovery-synthetic",
            backend_signature="e" * 64, user_agent="Catchuparr reader",
            client_ip="127.0.0.1",
        )
        self.episode_ledger.revoke(
            opening.episode_id, expected_version=opening.version,
            reason="cleanup_pending", cleanup_acknowledged=False,
        )
        managers = []

        class Manager:
            def __init__(self, *, channel_id, redis_client, worker_id):
                self.channel_id = channel_id
                self.redis_client = redis_client
                self.worker_id = worker_id
                self.stopped = 0
                managers.append(self)

            def stop(self):
                self.stopped += 1

        live_proxy = types.ModuleType("apps.proxy.live_proxy")
        live_proxy.__path__ = []
        client_manager = types.ModuleType("apps.proxy.live_proxy.client_manager")
        client_manager.ClientManager = Manager
        modules = {
            "apps": types.ModuleType("apps"),
            "apps.proxy": types.ModuleType("apps.proxy"),
            "apps.proxy.live_proxy": live_proxy,
            "apps.proxy.live_proxy.client_manager": client_manager,
        }
        redis_text = object()
        with patch.dict(sys.modules, modules), patch(
            "catchuparr.adapters.sharing_reader.redis_backend_signature",
            return_value="e" * 64,
        ), patch(
            "catchuparr.adapters.sharing_native.read_native_episode_grant",
            return_value=SimpleNamespace(
                episode_id=opening.episode_id, catalog_generation="catalog-synthetic",
            ),
        ), patch(
            "catchuparr.adapters.sharing_borrow_route._recover_registration_ack",
            return_value=None,
        ), patch(
            "catchuparr.adapters.sharing_native.fence_unregistered_opening",
            return_value=True,
        ) as fence, patch(
            "catchuparr.adapters.sharing_native.native_registration_is_absent",
            return_value=True,
        ) as absent, patch(
            "catchuparr.adapters.sharing_native.close_fenced_unregistered_opening",
            return_value=True,
        ) as close:
            self.assertEqual((1, 0), retry_durable_open_cleanups(
                self.episode_ledger, redis_text, now=300.0,
            ))
        fence.assert_called_once()
        absent.assert_called_once_with(
            redis_text, channel_uuid=CHANNEL,
            client_id="catchuparr-client-unknown-recovery-synthetic",
        )
        close.assert_called_once()
        self.assertEqual(1, managers[0].stopped)
        self.assertEqual("closed", self.episode_ledger.read(CHANNEL).phase)

    def _episode_exists(self):
        try:
            self.episode_ledger.read(CHANNEL)
        except Exception:
            return False
        return True


if __name__ == "__main__":
    unittest.main()
