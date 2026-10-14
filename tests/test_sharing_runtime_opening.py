import sys
import tempfile
import types
import unittest
from dataclasses import replace
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
from catchuparr.sharing_runtime import (
    PreparedBorrowOpening,
    RecorderBorrowCoordinator,
    acknowledge_opened_borrow,
    borrow_cleanup_acknowledged,
    prepare_borrow_opening,
)

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

    def test_consumed_attach_recovers_only_after_exact_episode_and_native_absence(self):
        opening = self.episode_ledger.begin_open(
            self.saved.intent,
            lease_digest=sha256(LEASE.encode()).hexdigest(),
            identity=IDENTITY,
            client_id="catchuparr-client-runtime-recovery-synthetic",
        )
        self.episode_ledger.revoke(
            opening.episode_id,
            expected_version=opening.version,
            reason="closed",
            cleanup_acknowledged=True,
        )
        coordinator = object.__new__(RecorderBorrowCoordinator)
        coordinator.channel_uuid = CHANNEL
        coordinator.redis = object()
        coordinator.intent_ledger = self.intent_ledger
        coordinator.episode_ledger = self.episode_ledger
        coordinator.state_record = self.saved
        coordinator.configuration_generation = "config-synthetic"
        coordinator.control_generation = 2
        coordinator.recorder_fence = 7
        coordinator.lease_value = LEASE
        coordinator._recovery_is_current = lambda *_args: True

        with patch(
            "catchuparr.adapters.sharing_native.observe_native_episode_for_authority_drain",
            return_value=SimpleNamespace(state="unknown"),
        ), patch(
            "catchuparr.adapters.sharing_native.native_registration_is_absent",
            return_value=True,
        ):
            self.assertFalse(RecorderBorrowCoordinator._recover_closed_attach(coordinator))
        self.assertIsNotNone(coordinator.state_record.intent)

        with patch(
            "catchuparr.adapters.sharing_native.observe_native_episode_for_authority_drain",
            return_value=SimpleNamespace(state="known_revoked"),
        ), patch(
            "catchuparr.adapters.sharing_native.native_registration_is_absent",
            return_value=True,
        ):
            self.assertTrue(RecorderBorrowCoordinator._recover_closed_attach(coordinator))
        self.assertIsNone(coordinator.state_record.intent)
        self.assertGreater(coordinator.state_record.state.intent_epoch, self.saved.state.intent_epoch)

    def test_borrow_cleanup_gate_requires_closed_episode_and_absent_native_client(self):
        from catchuparr.adapters.sharing_borrow_route import sign_borrow_capability

        opening = self.episode_ledger.begin_open(
            self.saved.intent,
            lease_digest=sha256(LEASE.encode()).hexdigest(),
            identity=IDENTITY,
            client_id="catchuparr-client-runtime-cleanup-synthetic",
        )
        capability = sign_borrow_capability(
            secret=b"runtime-cleanup-synthetic-secret-00000",
            channel_uuid=CHANNEL,
            episode_id=opening.episode_id,
            user_id="0",
            configuration_generation=opening.configuration_generation,
            catalog_generation="catalog-synthetic",
            control_generation=opening.control_generation,
            recorder_fence=opening.recorder_fence,
            intent_epoch=opening.intent_epoch,
            connection_identity=IDENTITY,
            episode_version=opening.version,
            expires_at=200.0,
            nonce="a" * 32,
        )
        prepared = PreparedBorrowOpening(
            opening.episode_id, opening.version, opening.client_id, capability,
        )
        redis = object()
        with patch(
            "catchuparr.adapters.sharing_native.observe_native_episode_for_authority_drain",
            return_value=SimpleNamespace(state="known_revoked"),
        ), patch(
            "catchuparr.adapters.sharing_native.native_registration_is_absent",
            return_value=True,
        ):
            self.assertFalse(borrow_cleanup_acknowledged(redis, self.episode_ledger, prepared))
        self.episode_ledger.revoke(
            opening.episode_id, expected_version=opening.version,
            reason="closed", cleanup_acknowledged=True,
        )
        with patch(
            "catchuparr.adapters.sharing_native.observe_native_episode_for_authority_drain",
            return_value=SimpleNamespace(state="known_revoked"),
        ), patch(
            "catchuparr.adapters.sharing_native.native_registration_is_absent",
            return_value=True,
        ) as absent:
            self.assertTrue(borrow_cleanup_acknowledged(redis, self.episode_ledger, prepared))
        absent.assert_called_once_with(
            redis, channel_uuid=CHANNEL, client_id=opening.client_id,
        )

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
            return_value=None,
        ), patch(
            "catchuparr.adapters.sharing_native.read_native_episode_cleanup_binding",
            return_value=None,
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

    def test_retained_cleanup_callbacks_keep_each_channel_ack_and_manager(self):
        from catchuparr.sharing_runtime import (
            retry_durable_open_cleanups,
            retry_pending_open_cleanups,
        )

        channels = (CHANNEL, "channel-runtime-opening-second-synthetic")
        registrations = []
        for index, channel in enumerate(channels):
            identity = (channel, *IDENTITY[1:])
            state = replace(
                attach_state(), channel_uuid=channel,
                candidate_identity=identity,
                candidate_connection_identity=identity,
            )
            saved = (
                self.saved if channel == CHANNEL else self.intent_ledger.save(
                    state, action=SharingAction.ATTACH_BORROW,
                    expected_revision=None, target_identity=identity,
                    target_admitted=True, not_before=105.0,
                )
            )
            opening = self.episode_ledger.begin_open(
                saved.intent, lease_digest=sha256(LEASE.encode()).hexdigest(),
                identity=identity,
                client_id=f"catchuparr-client-retry-{index}-synthetic",
            )
            registration = self.episode_ledger.prepare_registration(
                opening, local_worker_id=f"catchuparr-worker-retry-{index}-synthetic",
                backend_signature="f" * 64, user_agent="Catchuparr reader",
                client_ip="127.0.0.1",
            )
            ack = SimpleNamespace(
                episode_id=opening.episode_id, channel_uuid=channel,
                client_id=registration.client_id,
                local_worker_id=registration.local_worker_id,
                redis_backend_signature=registration.backend_signature,
                connected_at=f"{105 + index}.5",
                projection_version=opening.version + 1,
                user_agent=registration.user_agent,
                client_ip=registration.client_ip,
            )
            self.episode_ledger.record_registration_ack(opening.episode_id, ack)
            self.episode_ledger.revoke(
                opening.episode_id, expected_version=opening.version,
                reason="cleanup_pending", cleanup_acknowledged=False,
            )
            registrations.append((opening, registration, ack))

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

        client_manager = types.ModuleType("apps.proxy.live_proxy.client_manager")
        client_manager.ClientManager = Manager
        modules = {
            "apps": types.ModuleType("apps"),
            "apps.proxy": types.ModuleType("apps.proxy"),
            "apps.proxy.live_proxy": types.ModuleType("apps.proxy.live_proxy"),
            "apps.proxy.live_proxy.client_manager": client_manager,
        }
        redis_text = object()
        cleanup_calls = []

        def cleanup(_redis, ack, *, episode_id, manager):
            cleanup_calls.append((ack.client_id, episode_id, manager.worker_id))
            # Fail the scanner pass, then succeed when retained owners retry.
            return len(cleanup_calls) > len(registrations)

        def native_grant(_redis, channel):
            opening, _registration, _ack = next(
                item for item in registrations if item[0].channel_uuid == channel
            )
            return SimpleNamespace(
                episode_id=opening.episode_id, catalog_generation="catalog-synthetic",
            )

        with patch.dict(sys.modules, modules), patch(
            "catchuparr.adapters.sharing_reader.redis_backend_signature",
            return_value="f" * 64,
        ), patch(
            "catchuparr.adapters.sharing_native.read_native_episode_grant",
            side_effect=native_grant,
        ), patch(
            "catchuparr.adapters.sharing_native.cleanup_borrowed_client",
            side_effect=cleanup,
        ):
            self.assertEqual((0, 2), retry_durable_open_cleanups(
                self.episode_ledger, redis_text, now=300.0,
            ))
            self.assertEqual((2, 0), retry_pending_open_cleanups())

        expected = [
            (registration.client_id, opening.episode_id, registration.local_worker_id)
            for opening, registration, _ack in registrations
        ]
        self.assertEqual(expected, cleanup_calls[2:])
        self.assertEqual(2, len(managers))
        self.assertTrue(all(manager.stopped == 1 for manager in managers))
        self.assertTrue(all(
            self.episode_ledger.read(channel).phase == "closed"
            for channel in channels
        ))

    def _episode_exists(self):
        try:
            self.episode_ledger.read(CHANNEL)
        except Exception:
            return False
        return True


    def test_native_registration_ack_resolves_attach_and_persists_switch_clock(self):
        result, _project, _cleanup = self.prepare()
        claimed, prepared = result
        self.assertTrue(claimed)
        self.assertIsNotNone(prepared)
        opening = self.episode_ledger.read(CHANNEL)
        self.assertEqual("opening", opening.phase)
        active = self.episode_ledger.activate(
            opening.episode_id, expected_version=opening.version,
            client_id=opening.client_id, connected_at="105.000000",
            ordinary_viewer_ids=("viewer-runtime-opening-synthetic",),
        )
        self.assertEqual("active", active.phase)
        self.assertTrue(acknowledge_opened_borrow(
            prepared.capability, self.intent_ledger, self.episode_ledger, target=target(),
            settings=SharingSettings(True, 5, 30, 60), now=105.0,
        ))
        persisted = self.intent_ledger.read(CHANNEL)
        self.assertIsNone(persisted.intent)
        self.assertEqual("borrowed", persisted.state.phase.value)
        self.assertEqual(105.0, persisted.state.last_switch_at)


if __name__ == "__main__":
    unittest.main()
