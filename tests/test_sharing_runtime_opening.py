import importlib
import sys
import tempfile
import threading
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
    resolve_transition,
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
        settings=SharingSettings(True, 5, 30, 60), target=None,
    ))
    ready = observe(first.state, SharingObservation(
        channel_uuid=CHANNEL, now=100, configuration_generation="config-synthetic",
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

    def test_return_clock_starts_only_after_current_allowed_source_media_ack(self):
        waiting = attach_state()
        opening_observation = SharingObservation(
            channel_uuid=CHANNEL, now=105.0,
            configuration_generation="config-synthetic", control_generation=2,
            recorder_fence=7, lease_is_current=True,
            settings=SharingSettings(True, 5, 30, 60), target=target(),
        )
        borrowed = resolve_transition(
            waiting, SharingAction.ATTACH_BORROW, succeeded=True, now=105.0,
            current_state=waiting, current_observation=opening_observation,
        )
        # The supervisor saw the first zero-viewer time before reader teardown.
        borrowed = replace(
            borrowed, phase=type(borrowed.phase).WAIT_RETURN,
            last_viewer_left_at=100.0,
        )
        saved_borrowed = self.intent_ledger.save(
            borrowed, action=None, expected_revision=self.saved.revision,
            updated_at=105.0,
        )
        opening = self.episode_ledger.begin_open(
            self.saved.intent,
            lease_digest=sha256(LEASE.encode()).hexdigest(),
            identity=IDENTITY,
            client_id="catchuparr-client-runtime-return-synthetic",
        )
        self.episode_ledger.revoke(
            opening.episode_id, expected_version=opening.version,
            reason="closed", cleanup_acknowledged=True,
        )
        from catchuparr.adapters.sharing_borrow_route import sign_borrow_capability

        capability = sign_borrow_capability(
            secret=b"runtime-return-synthetic-secret-000000",
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
            expires_at=300.0,
            nonce="b" * 32,
        )
        prepared = PreparedBorrowOpening(
            opening.episode_id, opening.version, opening.client_id, capability,
        )
        coordinator = object.__new__(RecorderBorrowCoordinator)
        coordinator.channel_uuid = CHANNEL
        coordinator.redis = object()
        coordinator.intent_ledger = self.intent_ledger
        coordinator.episode_ledger = self.episode_ledger
        coordinator.state_record = saved_borrowed
        coordinator.dedicated_source_available = True
        coordinator.configuration_generation = "config-synthetic"
        coordinator.control_generation = 2
        coordinator.recorder_fence = 7
        coordinator.lease_value = LEASE
        coordinator.catalog_generation = "catalog-synthetic"
        coordinator.active = {"connection_sharing": {CHANNEL: {
            "enabled": True, "live_stability_seconds": 5,
            "last_viewer_return_seconds": 30, "min_switch_interval_seconds": 60,
        }}}
        with patch("catchuparr.sharing_runtime.borrow_cleanup_acknowledged", return_value=True), patch(
            "catchuparr.configuration._sharing_catalog_generation",
            return_value="catalog-synthetic",
        ), patch.object(coordinator, "_recovery_is_current", return_value=True):
            self.assertTrue(RecorderBorrowCoordinator.record_return_after_cleanup(
                coordinator, prepared, now=200.0,
            ))
        pending = self.intent_ledger.read(CHANNEL)
        self.assertEqual(105.0, pending.state.last_switch_at)
        self.assertEqual("wait_return", pending.state.phase.value)
        self.assertEqual(SharingAction.RETURN_TO_ALLOWED, pending.intent.action)
        self.assertEqual(165.0, pending.intent.not_before)

        # Simulate worker restart after native cleanup but before useful
        # dedicated media: recovery rebinds the return intent, never the borrow.
        recovered = self.intent_ledger.recover(
            CHANNEL, configuration_generation="config-synthetic", control_generation=2,
            recorder_fence=8, now=201.0, expected_revision=pending.revision,
            recovery_is_current=lambda *_args: True,
        )
        pending = recovered
        coordinator.recorder_fence = 8
        coordinator.state_record = recovered
        return_receipt = coordinator._read_return_receipt()
        self.assertIsNotNone(return_receipt)
        self.assertEqual(opening.episode_id, return_receipt.episode_id)
        self.assertEqual(7, return_receipt.recorder_fence)

        attempt = SimpleNamespace(
            channel_uuid=CHANNEL, config_generation="config-synthetic",
            control_generation=2, sharing_catalog_generation="catalog-synthetic",
            stream_id="stream-synthetic", account_id="account-synthetic",
            sharing_authority_revision=7,
        )
        allowed_candidate = {"id": "stream-synthetic", "account_id": "account-synthetic"}
        with (
            patch("catchuparr.sharing_runtime.borrow_return_cleanup_acknowledged", return_value=True),
            patch("catchuparr.configuration._sharing_catalog_generation", return_value="catalog-synthetic"),
            patch("catchuparr.recorder_proxy.candidate_is_current", return_value=True),
            patch("catchuparr.recorder_proxy.sharing_operation_is_current", return_value=False),
            patch("catchuparr.recorder_proxy.current_sharing_authority_context", return_value={
                "authority_revision": 7, "catalog_generation": "catalog-synthetic",
            }),
            patch.object(coordinator, "_recovery_is_current", return_value=True),
        ):
            self.assertFalse(coordinator.acknowledge_return_media(
                return_receipt, pending.intent, attempt, allowed_candidate, media_at=270.0,
            ))
            self.assertEqual(pending.intent, self.intent_ledger.read(CHANNEL).intent)
        with (
            patch("catchuparr.sharing_runtime.borrow_return_cleanup_acknowledged", return_value=True),
            patch("catchuparr.configuration._sharing_catalog_generation", return_value="catalog-synthetic"),
            patch("catchuparr.recorder_proxy.candidate_is_current", return_value=True),
            patch("catchuparr.recorder_proxy.sharing_operation_is_current", return_value=True),
            patch("catchuparr.recorder_proxy.current_sharing_authority_context", return_value={
                "authority_revision": 7, "catalog_generation": "catalog-synthetic",
            }),
            patch.object(coordinator, "_recovery_is_current", return_value=True),
        ):
            self.assertFalse(coordinator.acknowledge_return_media(
                return_receipt, pending.intent, attempt, allowed_candidate, media_at=164.0,
            ))
            self.assertTrue(coordinator.acknowledge_return_media(
                return_receipt, pending.intent, attempt, allowed_candidate, media_at=270.0,
            ))
        reloaded = self.intent_ledger.read(CHANNEL)
        self.assertEqual(270.0, reloaded.state.last_switch_at)
        self.assertEqual("dedicated", reloaded.state.phase.value)
        # A viewer can return while the allowed-source operation is active.
        # The pending RETURN cannot be replaced by ATTACH before useful media
        # proves the return, but after that exact ACK the same viewer is
        # observed again and admitted only after the normal switch interval.
        class FakeRedis:
            def get(self, key):
                return LEASE if key == f"catchuparr:recorder:{CHANNEL}" else None

            def ttl(self, key):
                return 60 if key == f"catchuparr:recorder:{CHANNEL}" else -2

        coordinator.redis = FakeRedis()
        coordinator.lease_value = LEASE
        coordinator.state_record = pending
        coordinator.assigned_sources = ()
        coordinator.catalog_generation = "catalog-synthetic"
        native_observation = SimpleNamespace(target=target())
        with (
            patch("catchuparr.adapters.sharing_native.read_native_episode_grant", return_value=None),
            patch("catchuparr.configuration._sharing_catalog_generation", return_value="catalog-synthetic"),
            patch("catchuparr.recorder_proxy.configuration_generation", return_value="config-synthetic"),
            patch("catchuparr.sharing_runtime.observe_native_connection", return_value=native_observation),
            patch.object(coordinator, "_recovery_is_current", return_value=True),
            patch("catchuparr.sharing_runtime.time.time", return_value=275.0),
        ):
            self.assertIsNone(coordinator.poll(active=coordinator.active, control_generation=2))
        self.assertIsNone(self.intent_ledger.read(CHANNEL).intent)

        # The attach token remains fenced until the original switch cooldown
        # expires.
        with (
            patch("catchuparr.adapters.sharing_native.read_native_episode_grant", return_value=None),
            patch("catchuparr.configuration._sharing_catalog_generation", return_value="catalog-synthetic"),
            patch("catchuparr.recorder_proxy.configuration_generation", return_value="config-synthetic"),
            patch("catchuparr.sharing_runtime.observe_native_connection", return_value=native_observation),
            patch.object(coordinator, "_recovery_is_current", return_value=True),
            patch("catchuparr.sharing_runtime.time.time", return_value=329.0),
        ):
            self.assertIsNone(coordinator.poll(active=coordinator.active, control_generation=2))
        self.assertIsNone(self.intent_ledger.read(CHANNEL).intent)

        # After the cooldown, poll prepares the exact opening for the same
        # ordinary viewer without creating a second upstream attempt.
        with (
            patch("catchuparr.adapters.sharing_native.read_native_episode_grant", return_value=None),
            patch("catchuparr.configuration._sharing_catalog_generation", return_value="catalog-synthetic"),
            patch("catchuparr.recorder_proxy.configuration_generation", return_value="config-synthetic"),
            patch("catchuparr.sharing_runtime.observe_native_connection", return_value=native_observation),
            patch("catchuparr.sharing_runtime.prepare_borrow_opening", return_value=(True, "synthetic-opening")),
            patch("catchuparr.sharing_runtime._django_secret", return_value=b"synthetic-secret"),
            patch.object(coordinator, "_recovery_is_current", return_value=True),
            patch("catchuparr.sharing_runtime.time.time", return_value=331.0),
        ):
            self.assertEqual("synthetic-opening", coordinator.poll(
                active=coordinator.active, control_generation=2,
            ))
        admitted = self.intent_ledger.read(CHANNEL).intent
        self.assertEqual(SharingAction.ATTACH_BORROW, admitted.action)
        self.assertEqual(331.0, admitted.not_before)


    def test_failure_cleanup_bypasses_return_and_switch_cooldowns(self):
        channel = CHANNEL + "-failure"
        borrowed = replace(
            attach_state(), channel_uuid=channel,
            phase=type(attach_state().phase).BORROWED,
            candidate_identity=None, candidate_connection_identity=None,
            candidate_since=None, borrowed_identity=None,
            last_viewer_left_at=None, last_switch_at=105.0,
            pending_action=None, intent_epoch=4,
        )
        saved = self.intent_ledger.save(
            borrowed, action=None, expected_revision=None, updated_at=105.0,
        )
        from catchuparr.adapters.sharing_borrow_route import sign_borrow_capability

        capability = sign_borrow_capability(
            secret=b"runtime-failure-return-synthetic-secret",
            channel_uuid=channel, episode_id="episode-failure-return-synthetic",
            user_id="0", configuration_generation="config-synthetic",
            catalog_generation="catalog-synthetic", control_generation=2,
            recorder_fence=7, intent_epoch=4,
            connection_identity=(channel, *IDENTITY[1:]),
            episode_version=2, expires_at=500.0, nonce="d" * 32,
        )
        prepared = PreparedBorrowOpening(
            "episode-failure-return-synthetic", 2,
            "catchuparr-client-failure-return-synthetic", capability,
        )
        coordinator = object.__new__(RecorderBorrowCoordinator)
        coordinator.channel_uuid = channel
        coordinator.redis = object()
        coordinator.intent_ledger = self.intent_ledger
        coordinator.episode_ledger = self.episode_ledger
        coordinator.state_record = saved
        coordinator.dedicated_source_available = True
        coordinator.configuration_generation = "config-synthetic"
        coordinator.control_generation = 2
        coordinator.recorder_fence = 7
        coordinator.lease_value = LEASE
        coordinator.catalog_generation = "catalog-synthetic"
        coordinator.active = {"connection_sharing": {channel: {
            "enabled": True, "live_stability_seconds": 5,
            "last_viewer_return_seconds": 30, "min_switch_interval_seconds": 60,
        }}}
        runtime_module = sys.modules[RecorderBorrowCoordinator.__module__]
        configuration_module = importlib.import_module("catchuparr.configuration")
        self.assertIs(sys.modules["catchuparr"].sharing_runtime, runtime_module)
        self.assertIs(sys.modules["catchuparr"].configuration, configuration_module)
        with (
            patch.object(runtime_module, "borrow_cleanup_acknowledged", return_value=True),
            patch.object(configuration_module, "_sharing_catalog_generation", return_value="catalog-synthetic"),
            patch.object(coordinator, "_recovery_is_current", return_value=True),
        ):
            self.assertTrue(RecorderBorrowCoordinator.record_return_after_cleanup(
                coordinator, prepared, now=400.0, outcome="failure",
            ))
        self.assertEqual(400.0, self.intent_ledger.read(channel).intent.not_before)

    def test_native_viewer_grace_first_zero_time_is_persisted(self):
        current = self.intent_ledger.read(CHANNEL)
        borrowed = resolve_transition(
            current.state, SharingAction.ATTACH_BORROW, succeeded=True, now=105.0,
            current_state=current.state,
            current_observation=SharingObservation(
                channel_uuid=CHANNEL, now=105.0,
                configuration_generation="config-synthetic", control_generation=2,
                recorder_fence=7, lease_is_current=True,
                settings=SharingSettings(True, 5, 30, 60), target=target(),
            ),
        )
        self.intent_ledger.save(
            borrowed, action=None, expected_revision=current.revision, updated_at=105.0,
        )
        coordinator = object.__new__(RecorderBorrowCoordinator)
        coordinator.channel_uuid = CHANNEL
        coordinator.intent_ledger = self.intent_ledger
        coordinator.state_record = current
        coordinator.configuration_generation = "config-synthetic"
        coordinator.control_generation = 2
        coordinator.recorder_fence = 7
        grant = SimpleNamespace(
            phase="wait_return", configuration_generation="config-synthetic",
            control_generation=2, recorder_fence=7,
            intent_epoch=borrowed.intent_epoch, last_viewer_left_at=110.0,
        )
        coordinator._sync_viewer_grace(grant)
        saved = self.intent_ledger.read(CHANNEL)
        self.assertEqual("wait_return", saved.state.phase.value)
        self.assertEqual(110.0, saved.state.last_viewer_left_at)

    def test_cleanup_without_allowed_source_keeps_state_paused_without_return_clock(self):
        waiting = attach_state()
        observation = SharingObservation(
            channel_uuid=CHANNEL, now=105.0,
            configuration_generation="config-synthetic", control_generation=2,
            recorder_fence=7, lease_is_current=True,
            settings=SharingSettings(True, 5, 30, 60), target=target(),
        )
        borrowed = resolve_transition(
            waiting, SharingAction.ATTACH_BORROW, succeeded=True, now=105.0,
            current_state=waiting, current_observation=observation,
        )
        saved = self.intent_ledger.save(
            borrowed, action=None, expected_revision=self.saved.revision, updated_at=105.0,
        )
        opening = self.episode_ledger.begin_open(
            self.saved.intent, lease_digest=sha256(LEASE.encode()).hexdigest(),
            identity=IDENTITY, client_id="catchuparr-client-paused-return-synthetic",
        )
        self.episode_ledger.revoke(
            opening.episode_id, expected_version=opening.version,
            reason="closed", cleanup_acknowledged=True,
        )
        from catchuparr.adapters.sharing_borrow_route import sign_borrow_capability

        capability = sign_borrow_capability(
            secret=b"runtime-paused-return-synthetic-secret",
            channel_uuid=CHANNEL, episode_id=opening.episode_id,
            user_id="0", configuration_generation=opening.configuration_generation,
            catalog_generation="catalog-synthetic", control_generation=2,
            recorder_fence=7, intent_epoch=opening.intent_epoch,
            connection_identity=IDENTITY, episode_version=opening.version,
            expires_at=300.0, nonce="c" * 32,
        )
        prepared = PreparedBorrowOpening(
            opening.episode_id, opening.version, opening.client_id, capability,
        )
        coordinator = object.__new__(RecorderBorrowCoordinator)
        coordinator.channel_uuid = CHANNEL
        coordinator.redis = object()
        coordinator.intent_ledger = self.intent_ledger
        coordinator.episode_ledger = self.episode_ledger
        coordinator.state_record = saved
        coordinator.dedicated_source_available = False
        coordinator.configuration_generation = "config-synthetic"
        coordinator.control_generation = 2
        coordinator.recorder_fence = 7
        coordinator.lease_value = LEASE
        with patch("catchuparr.sharing_runtime.borrow_cleanup_acknowledged", return_value=True), patch.object(
            coordinator, "_recovery_is_current", return_value=True,
        ):
            self.assertTrue(coordinator.record_return_after_cleanup(prepared, now=200.0))
        reloaded = self.intent_ledger.read(CHANNEL)
        self.assertEqual("paused", reloaded.state.phase.value)
        self.assertEqual(105.0, reloaded.state.last_switch_at)
        self.assertIsNone(reloaded.intent)

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
        from catchuparr.sharing_runtime import (
            retry_durable_open_cleanups,
            retry_pending_open_cleanups,
        )

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
                self.fail_stop_once = True
                managers.append(self)

            def stop(self):
                self.stopped += 1
                if self.fail_stop_once:
                    self.fail_stop_once = False
                    raise RuntimeError("synthetic manager stop interruption")

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
            self.assertEqual((0, 1), retry_durable_open_cleanups(
                self.episode_ledger, redis_text, now=300.0,
            ))
            self.assertEqual((1, 0), retry_pending_open_cleanups())
        self.assertEqual(2, managers[0].stopped)
        self.assertEqual("closed", self.episode_ledger.read(CHANNEL).phase)
        cleanup.assert_called_once()
        rebuilt_ack = cleanup.call_args.args[1]
        self.assertEqual(ack.client_id, rebuilt_ack.client_id)
        self.assertEqual(ack.connected_at, rebuilt_ack.connected_at)

    def test_channel_scoped_cleanup_leaves_other_and_live_opening_untouched(self):
        from catchuparr.sharing_runtime import retry_durable_open_cleanups

        target_opening = self.episode_ledger.begin_open(
            self.saved.intent, lease_digest=sha256(LEASE.encode()).hexdigest(),
            identity=IDENTITY, client_id="catchuparr-client-target-cleanup-synthetic",
        )
        target_registration = self.episode_ledger.prepare_registration(
            target_opening, local_worker_id="catchuparr-reader-target-cleanup-synthetic",
            backend_signature="a" * 64, user_agent="Catchuparr reader",
            client_ip="127.0.0.1",
        )
        target_ack = SimpleNamespace(
            episode_id=target_opening.episode_id, channel_uuid=CHANNEL,
            client_id=target_registration.client_id,
            local_worker_id=target_registration.local_worker_id,
            redis_backend_signature=target_registration.backend_signature,
            connected_at="105.5", projection_version=target_opening.version + 1,
            user_agent=target_registration.user_agent,
            client_ip=target_registration.client_ip,
        )
        self.episode_ledger.record_registration_ack(target_opening.episode_id, target_ack)
        self.episode_ledger.revoke(
            target_opening.episode_id, expected_version=target_opening.version,
            reason="cleanup_pending", cleanup_acknowledged=False,
        )

        other_channel = "channel-runtime-cleanup-other-synthetic"
        other_identity = (other_channel, *IDENTITY[1:])
        other_state = replace(
            attach_state(), channel_uuid=other_channel,
            candidate_identity=other_identity,
            candidate_connection_identity=other_identity,
        )
        other_saved = self.intent_ledger.save(
            other_state, action=SharingAction.ATTACH_BORROW,
            expected_revision=None, target_identity=other_identity,
            target_admitted=True, not_before=105.0,
        )
        other_opening = self.episode_ledger.begin_open(
            other_saved.intent, lease_digest=sha256(LEASE.encode()).hexdigest(),
            identity=other_identity, client_id="catchuparr-client-other-cleanup-synthetic",
        )
        self.episode_ledger.revoke(
            other_opening.episode_id, expected_version=other_opening.version,
            reason="cleanup_pending", cleanup_acknowledged=False,
        )

        live_channel = "channel-runtime-live-opening-synthetic"
        live_identity = (live_channel, *IDENTITY[1:])
        live_state = replace(
            attach_state(), channel_uuid=live_channel, intent_epoch=7,
            candidate_identity=live_identity,
            candidate_connection_identity=live_identity,
        )
        live_saved = self.intent_ledger.save(
            live_state, action=SharingAction.ATTACH_BORROW,
            expected_revision=None, target_identity=live_identity,
            target_admitted=True, not_before=105.0,
        )
        live_opening = self.episode_ledger.begin_open(
            live_saved.intent, lease_digest=sha256(LEASE.encode()).hexdigest(),
            identity=live_identity,
            client_id="catchuparr-client-live-opening-synthetic",
        )

        class Manager:
            def __init__(self, *, channel_id, redis_client, worker_id):
                self.channel_id = channel_id
                self.redis_client = redis_client
                self.worker_id = worker_id

            def stop(self):
                return None

        client_manager = types.ModuleType("apps.proxy.live_proxy.client_manager")
        client_manager.ClientManager = Manager
        modules = {
            "apps": types.ModuleType("apps"),
            "apps.proxy": types.ModuleType("apps.proxy"),
            "apps.proxy.live_proxy": types.ModuleType("apps.proxy.live_proxy"),
            "apps.proxy.live_proxy.client_manager": client_manager,
        }
        with patch.dict(sys.modules, modules), patch(
            "catchuparr.adapters.sharing_reader.redis_backend_signature",
            return_value=target_registration.backend_signature,
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
                self.episode_ledger, object(), now=300.0,
                stale_after=1000.0, channel_uuid=CHANNEL,
            ))
        cleanup.assert_called_once()
        self.assertEqual(target_registration.client_id, cleanup.call_args.args[1].client_id)
        self.assertEqual("closed", self.episode_ledger.read(CHANNEL).phase)
        self.assertEqual("cleanup_pending", self.episode_ledger.read(other_channel).phase)
        live_after = self.episode_ledger.read(live_channel)
        self.assertEqual("opening", live_after.phase)
        self.assertEqual(live_opening.episode_id, live_after.episode_id)

        # The consumed ATTACH remains consumed through cleanup. The new task
        # first recovers that exact closed opening, then observes the same
        # still-present viewer and may create a fresh claim on a later poll.
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
        coordinator.catalog_generation = "catalog-synthetic"
        coordinator.assigned_sources = ()
        coordinator.dedicated_source_available = False
        coordinator._recovery_is_current = lambda *_args: True
        active = {"connection_sharing": {CHANNEL: {
            "enabled": True, "live_stability_seconds": 0,
            "last_viewer_return_seconds": 0, "min_switch_interval_seconds": 0,
        }}}
        with patch(
            "catchuparr.adapters.sharing_native.read_native_episode_grant",
            return_value=None,
        ), patch(
            "catchuparr.adapters.sharing_native.observe_native_episode_for_authority_drain",
            return_value=SimpleNamespace(state="known_revoked"),
        ), patch(
            "catchuparr.adapters.sharing_native.native_registration_is_absent",
            return_value=True,
        ):
            self.assertIsNone(coordinator.poll(active=active, control_generation=2))
        self.assertIsNone(coordinator.state_record.intent)
        prepared_sentinel = object()
        with patch(
            "catchuparr.adapters.sharing_native.read_native_episode_grant",
            return_value=None,
        ), patch(
            "catchuparr.configuration._sharing_catalog_generation",
            return_value="catalog-synthetic",
        ), patch(
            "catchuparr.recorder_proxy.configuration_generation",
            return_value="config-synthetic",
        ), patch(
            "catchuparr.sharing_runtime.observe_native_connection",
            return_value=SimpleNamespace(target=target()),
        ), patch(
            "catchuparr.sharing_runtime.prepare_borrow_opening",
            return_value=(True, prepared_sentinel),
        ) as prepare_open, patch(
            "catchuparr.sharing_runtime._django_secret",
            return_value="synthetic-secret",
        ):
            self.assertIs(
                prepared_sentinel,
                coordinator.poll(active=active, control_generation=2),
            )
        recovered_token = prepare_open.call_args.args[3]
        self.assertEqual(SharingAction.ATTACH_BORROW, recovered_token.action)
        self.assertGreater(recovered_token.intent_epoch, self.saved.state.intent_epoch)

    def test_retained_cleanup_is_retried_for_its_channel_and_blocks_until_ack(self):
        from catchuparr.sharing_runtime import (
            _pending_open_cleanup_channels,
            _pending_open_cleanup_lock,
            _pending_open_cleanups,
            _retain_durable_cleanup,
            retry_durable_open_cleanups,
        )

        opening = self.episode_ledger.begin_open(
            self.saved.intent, lease_digest=sha256(LEASE.encode()).hexdigest(),
            identity=IDENTITY, client_id="catchuparr-client-retained-channel-synthetic",
        )
        registration = self.episode_ledger.prepare_registration(
            opening, local_worker_id="catchuparr-worker-retained-channel-synthetic",
            backend_signature="b" * 64, user_agent="Synthetic reader", client_ip="127.0.0.1",
        )
        ack = SimpleNamespace(
            episode_id=opening.episode_id, channel_uuid=CHANNEL,
            client_id=registration.client_id,
            local_worker_id=registration.local_worker_id,
            redis_backend_signature=registration.backend_signature,
            connected_at="105.5", projection_version=opening.version + 1,
            user_agent=registration.user_agent, client_ip=registration.client_ip,
        )
        self.episode_ledger.record_registration_ack(opening.episode_id, ack)
        self.episode_ledger.revoke(
            opening.episode_id, expected_version=opening.version,
            reason="cleanup_pending", cleanup_acknowledged=False,
        )

        other_channel = "channel-runtime-retained-other-synthetic"
        other_calls = []

        class Retry:
            def __init__(self):
                self.calls = 0
                self.succeed = False

            def retry_cleanup(self):
                self.calls += 1
                if not self.succeed:
                    return False
                current = self_episode_ledger.read(CHANNEL)
                self_episode_ledger.revoke(
                    current.episode_id, expected_version=current.version,
                    reason="closed", cleanup_acknowledged=True,
                )
                return True

        class OtherRetry:
            def retry_cleanup(self):
                other_calls.append("retried")
                return False

        retained = Retry()
        self_episode_ledger = self.episode_ledger
        _retain_durable_cleanup(opening.episode_id, retained, channel_uuid=CHANNEL)
        other_id = "episode-runtime-retained-other-synthetic"
        _retain_durable_cleanup(other_id, OtherRetry(), channel_uuid=other_channel)
        try:
            with patch(
                "catchuparr.adapters.sharing_reader.redis_backend_signature",
                return_value="b" * 64,
            ):
                self.assertEqual((0, 1), retry_durable_open_cleanups(
                    self.episode_ledger, object(), now=300.0, channel_uuid=CHANNEL,
                ))
                self.assertEqual(1, retained.calls)
                self.assertEqual([], other_calls)
                self.assertEqual("cleanup_pending", self.episode_ledger.read(CHANNEL).phase)

                # Replace with the exact durable operation's successful ACK;
                # the next startup recovery must retry it before observation.
                retained.succeed = True
                self.assertEqual((1, 0), retry_durable_open_cleanups(
                    self.episode_ledger, object(), now=301.0, channel_uuid=CHANNEL,
                ))
                self.assertEqual(2, retained.calls)
                self.assertEqual([], other_calls)
        finally:
            with _pending_open_cleanup_lock:
                _pending_open_cleanups.pop(opening.episode_id, None)
                _pending_open_cleanup_channels.pop(opening.episode_id, None)
                _pending_open_cleanups.pop(other_id, None)
                _pending_open_cleanup_channels.pop(other_id, None)

    def test_retained_cleanup_retry_is_serialized_per_episode(self):
        from catchuparr.sharing_runtime import (
            _pending_open_cleanup_channels,
            _pending_open_cleanup_lock,
            _pending_open_cleanup_locks,
            _pending_open_cleanups,
            _retain_durable_cleanup,
            retry_pending_open_cleanups,
        )

        episode_id = "episode-runtime-retained-concurrency-synthetic"
        entered = threading.Event()
        release = threading.Event()

        class Retry:
            def __init__(self):
                self.calls = 0

            def retry_cleanup(self):
                self.calls += 1
                entered.set()
                if not release.wait(2):
                    return False
                return True

        operation = Retry()
        _retain_durable_cleanup(episode_id, operation, channel_uuid=CHANNEL)
        results = []
        first = threading.Thread(
            target=lambda: results.append(retry_pending_open_cleanups(channel_uuid=CHANNEL)),
        )
        second = threading.Thread(
            target=lambda: results.append(retry_pending_open_cleanups(channel_uuid=CHANNEL)),
        )
        try:
            first.start()
            self.assertTrue(entered.wait(2))
            second.start()
            release.set()
            first.join(timeout=2)
            second.join(timeout=2)
            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())
            self.assertEqual(1, operation.calls)
            self.assertEqual([(0, 0), (1, 0)], sorted(results))
        finally:
            release.set()
            first.join(timeout=2)
            second.join(timeout=2)
            with _pending_open_cleanup_lock:
                _pending_open_cleanups.pop(episode_id, None)
                _pending_open_cleanup_channels.pop(episode_id, None)
                _pending_open_cleanup_locks.pop(episode_id, None)

    def test_durable_cleanup_scanner_fences_uncertain_registration_before_stop(self):
        from catchuparr.sharing_runtime import (
            retry_durable_open_cleanups,
            retry_pending_open_cleanups,
        )

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
                self.fail_first_stop = True
                managers.append(self)

            def stop(self):
                self.stopped += 1
                if self.fail_first_stop:
                    self.fail_first_stop = False
                    raise RuntimeError("synthetic manager stop failure")

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
            self.assertEqual((0, 1), retry_durable_open_cleanups(
                self.episode_ledger, redis_text, now=300.0,
            ))
            # The exact operation retains the already acknowledged native
            # cleanup; retrying must not re-fence or rebuild the registration.
            self.assertEqual((1, 0), retry_pending_open_cleanups())
        fence.assert_called_once()
        absent.assert_called_once_with(
            redis_text, channel_uuid=CHANNEL,
            client_id="catchuparr-client-unknown-recovery-synthetic",
        )
        close.assert_called_once()
        self.assertEqual(2, managers[0].stopped)
        self.assertEqual("closed", self.episode_ledger.read(CHANNEL).phase)

    def test_expired_unregistered_opening_closes_only_after_atomic_tombstone(self):
        from catchuparr.sharing_runtime import retry_durable_open_cleanups

        opening = self.episode_ledger.begin_open(
            self.saved.intent,
            lease_digest=sha256(LEASE.encode()).hexdigest(),
            identity=IDENTITY,
            client_id="catchuparr-client-expired-recovery-synthetic",
        )
        self.episode_ledger.revoke(
            opening.episode_id, expected_version=opening.version,
            reason="cleanup_pending", cleanup_acknowledged=False,
        )
        expired_snapshot = self.episode_ledger.read(CHANNEL)
        redis_text = object()
        with patch(
            "catchuparr.adapters.sharing_reader.redis_backend_signature",
            return_value="f" * 64,
        ), patch(
            "catchuparr.adapters.sharing_native.read_native_episode_grant",
            return_value=None,
        ), patch(
            "catchuparr.adapters.sharing_native.read_native_episode_cleanup_binding",
            return_value=None,
        ), patch(
            "catchuparr.adapters.sharing_native.tombstone_expired_unprojected_opening",
            return_value=True,
        ) as tombstone:
            self.assertEqual((1, 0), retry_durable_open_cleanups(
                self.episode_ledger, redis_text, now=300.0,
            ))
        tombstone.assert_called_once_with(
            redis_text, expired_snapshot,
            client_id=opening.client_id,
        )
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
