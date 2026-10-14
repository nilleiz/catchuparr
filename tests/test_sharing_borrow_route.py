import unittest
from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr.adapters.sharing_borrow_route import (
    BorrowedReaderSession,
    BorrowOpenCleanupPending,
    decode_borrow_capability,
    encode_borrow_capability,
    native_borrow_proof_reader,
    sign_borrow_capability,
)
from catchuparr.adapters.sharing_native import (
    NativeAssignedSource,
    NativeClientRegistrationAck,
    NativeEpisodeGrant,
)
from catchuparr.adapters.sharing_reader import (
    BorrowBindingLost,
    BorrowReadBinding,
    BorrowReaderUnavailable,
    validate_borrow_read_proof,
)
from catchuparr.sharing_episode import BorrowEpisodeRejected, BorrowEpisodeSnapshot

SECRET = b"synthetic-route-secret-060-with-at-least-32-bytes"
IDENTITY = (
    "channel-route-synthetic", "worker-route-synthetic", "owner-route-synthetic",
    "owner-route-synthetic", "stream-route-synthetic", "account-route-synthetic",
    "m3u-route-synthetic", "input-route-synthetic",
)


class FakeManager:
    def __init__(self):
        self.stopped = 0

    def stop(self):
        self.stopped += 1


class FakeReader:
    def iter_chunks(self, stop=None):
        yield b"synthetic-ts-chunk"
        raise RuntimeError("synthetic stream failure")


def capability():
    return sign_borrow_capability(
        secret=SECRET, channel_uuid=IDENTITY[0], episode_id="episode-route-synthetic",
        user_id="user-route-synthetic", configuration_generation="config-route-synthetic",
        catalog_generation="catalog-route-synthetic",
        control_generation=3, recorder_fence=9, intent_epoch=7,
        connection_identity=IDENTITY, episode_version=1,
        expires_at=200, nonce="nonce-route-synthetic",
    )


class SharingBorrowRouteTests(unittest.TestCase):
    def test_capability_url_codec_roundtrips_and_rejects_modified_or_ambiguous_fields(self):
        original = capability()
        encoded = encode_borrow_capability(original)
        decoded = decode_borrow_capability(encoded)
        self.assertEqual(original, decoded)
        self.assertTrue(decoded.verify(SECRET, user_id="user-route-synthetic", now=199))
        tampered = encoded[:-1] + ("A" if encoded[-1] != "A" else "B")
        with self.assertRaises(ValueError):
            decode_borrow_capability(tampered)
        with self.assertRaises(ValueError):
            decode_borrow_capability("e30")
        with self.assertRaises(ValueError):
            decode_borrow_capability("a" * 4097)

    def test_signed_capability_binds_user_expiry_and_exact_episode(self):
        grant = capability()
        self.assertTrue(grant.verify(SECRET, user_id="user-route-synthetic", now=199))
        self.assertFalse(grant.verify(SECRET, user_id="other-user-synthetic", now=199))
        self.assertFalse(grant.verify(SECRET, user_id="user-route-synthetic", now=200))
        self.assertFalse(grant.verify(b"wrong-secret-synthetic-060-123456", user_id="user-route-synthetic", now=199))
        self.assertNotIn(grant.signature, repr(grant))
        changed = replace(grant, episode_id="replacement-episode-synthetic")
        self.assertFalse(changed.verify(SECRET, user_id="user-route-synthetic", now=199))

    def _current_proof_inputs(self):
        lease = "synthetic-route-lease"
        episode = BorrowEpisodeSnapshot(
            episode_id="episode-route-synthetic", channel_uuid=IDENTITY[0],
            phase="active", configuration_generation="config-route-synthetic",
            control_generation=3, recorder_fence=9, intent_epoch=7,
            identity=IDENTITY, lease_digest=sha256(lease.encode()).hexdigest(),
            client_id="catchuparr-client-route-synthetic", connected_at="123.5",
            version=2, last_viewer_left_at=None, return_deadline=None,
        )
        ack = NativeClientRegistrationAck(
            channel_uuid=IDENTITY[0], episode_id=episode.episode_id,
            client_id=episode.client_id, local_worker_id=IDENTITY[1],
            redis_backend_signature="backend-signature-synthetic", connected_at="123.5",
            projection_version=2, user_agent="Synthetic Player", client_ip="192.0.2.60",
        )
        grant = NativeEpisodeGrant(
            channel_uuid=IDENTITY[0], episode_id=episode.episode_id, phase="wait_return",
            configuration_generation=episode.configuration_generation,
            control_generation=episode.control_generation,
            catalog_generation="catalog-route-synthetic", recorder_fence=episode.recorder_fence,
            intent_epoch=episode.intent_epoch, version=5, lease_value=lease,
            worker_id=IDENTITY[1], owner_lock_value=IDENTITY[2],
            metadata_owner=IDENTITY[3], stream_id=IDENTITY[4], account_id=IDENTITY[5],
            m3u_profile_id=IDENTITY[6], input_profile_id=IDENTITY[7],
            client_id=ack.client_id, client_worker_id=ack.local_worker_id,
            connected_at=ack.connected_at, return_seconds=30, minimum_switch_seconds=60,
            last_switch_at=90.0, last_viewer_left_at=100.0, return_deadline=130.0,
        )
        target = SimpleNamespace(
            connection_identity=IDENTITY, owner_lock_value=IDENTITY[2],
            ordinary_client_ids=(), owner_heartbeat_healthy=True,
            native_state_healthy=True, assigned_stream_matches=True,
            account_active=True, profile_active=True, media_compatible=True,
            useful_buffer_available=True, redirect_worker=False, vod_or_preview_worker=False,
        )
        return episode, ack, grant, target

    def test_fresh_native_cas_proof_authenticates_dynamic_grace(self):
        episode, ack, grant, target = self._current_proof_inputs()
        with (
            patch("catchuparr.adapters.sharing_borrow_route.read_native_episode_grant",
                  side_effect=(grant, grant)),
            patch("catchuparr.adapters.sharing_borrow_route.check_and_touch_borrowed_client",
                  return_value=(True, 130.0, grant.version)),
            patch("catchuparr.adapters.sharing_borrow_route.observe_native_connection",
                  return_value=SimpleNamespace(target=target)),
        ):
            reader = native_borrow_proof_reader(
                redis=object(), episode=episode, ack=ack,
                assigned_sources=(self._assigned_source(),),
                catalog_generation="catalog-route-synthetic", clock=lambda: 120.0,
            )
            proof = reader()
        binding = BorrowReadBinding(
            IDENTITY, episode.configuration_generation, episode.control_generation,
            episode.recorder_fence, dynamic_grace=True,
        )
        validate_borrow_read_proof(binding, proof)
        self.assertEqual(proof.pending_return_deadline, 130.0)
        self.assertTrue(proof.borrowed_client_current)

    def test_dynamic_grace_rejects_replaced_owner_profile_lease_and_grant_version(self):
        episode, ack, grant, target = self._current_proof_inputs()
        cases = (
            ((replace(grant, lease_value="replacement-lease-synthetic"), grant), target, 5),
            ((grant, grant), SimpleNamespace(
                **{**vars(target), "connection_identity": IDENTITY[:2] + ("replacement-owner",) + IDENTITY[3:]}
            ), 5),
            ((grant, grant), SimpleNamespace(
                **{**vars(target), "connection_identity": IDENTITY[:-1] + ("replacement-profile",)}
            ), 5),
            ((grant, replace(grant, version=6)), target, 5),
        )
        for grants, observed_target, cas_version in cases:
            with self.subTest(grants=grants, target=observed_target, cas_version=cas_version):
                with (
                    patch("catchuparr.adapters.sharing_borrow_route.read_native_episode_grant",
                          side_effect=grants),
                    patch("catchuparr.adapters.sharing_borrow_route.check_and_touch_borrowed_client",
                          return_value=(True, 130.0, cas_version)),
                    patch("catchuparr.adapters.sharing_borrow_route.observe_native_connection",
                          return_value=SimpleNamespace(target=observed_target)),
                ):
                    reader = native_borrow_proof_reader(
                        redis=object(), episode=episode, ack=ack,
                        assigned_sources=(self._assigned_source(),),
                        catalog_generation="catalog-route-synthetic", clock=lambda: 120.0,
                    )
                    with self.assertRaises(BorrowBindingLost):
                        reader()

    def test_dynamic_grace_rejects_catalog_epoch_change_before_native_cas(self):
        episode, ack, grant, _target = self._current_proof_inputs()
        with patch("catchuparr.adapters.sharing_borrow_route.read_native_episode_grant",
                   return_value=replace(grant, catalog_generation="catalog-replaced-synthetic")), \
                patch("catchuparr.adapters.sharing_borrow_route.check_and_touch_borrowed_client") as touch:
            reader = native_borrow_proof_reader(
                redis=object(), episode=episode, ack=ack,
                assigned_sources=(self._assigned_source(),),
                catalog_generation="catalog-route-synthetic", clock=lambda: 120.0,
            )
            with self.assertRaises(BorrowBindingLost):
                reader()
        touch.assert_not_called()

    @staticmethod
    def _assigned_source():
        return NativeAssignedSource(
            channel_uuid=IDENTITY[0], stream_id=IDENTITY[4], account_id=IDENTITY[5],
            m3u_profile_id=IDENTITY[6], input_profile_id=IDENTITY[7],
            assigned=True, account_active=True, profile_active=True, media_compatible=True,
        )

    @staticmethod
    def _opening_episode():
        return BorrowEpisodeSnapshot(
            episode_id="episode-route-synthetic", channel_uuid=IDENTITY[0],
            phase="opening", configuration_generation="config-route-synthetic",
            control_generation=3, recorder_fence=9, intent_epoch=7,
            identity=IDENTITY, lease_digest=sha256(b"synthetic-route-lease").hexdigest(),
            client_id="catchuparr-client-route-synthetic", connected_at=None, version=1,
            last_viewer_left_at=None, return_deadline=None,
        )

    def _open(self, *, cleanup_reply=True, registration_reply=True, manager_worker=None,
              owns_manager=True):
        opening = self._opening_episode()
        active = replace(
            opening, phase="active", client_id="catchuparr-client-route-synthetic",
            connected_at="123.5", version=2,
        )
        manager = FakeManager()
        manager.worker_id = manager_worker or IDENTITY[1]
        manager.redis_client = object()
        ack = NativeClientRegistrationAck(
            channel_uuid=IDENTITY[0], episode_id=opening.episode_id,
            client_id="catchuparr-client-route-synthetic", local_worker_id=IDENTITY[1],
            redis_backend_signature="backend-route-synthetic", connected_at="123.5",
            projection_version=2, user_agent="Synthetic Player", client_ip="192.0.2.60",
        )
        ledger = SimpleNamespace(activate=lambda *_args, **_kwargs: active)
        calls = []
        mocks = (
            patch("catchuparr.adapters.sharing_borrow_route.redis_backend_signature",
                  return_value="backend-route-synthetic"),
            patch("catchuparr.sharing_episode.select_task_local_client_manager",
                  return_value=(manager, owns_manager)),
            patch("catchuparr.adapters.sharing_borrow_route.consume_opening_capability",
                  side_effect=lambda *_args, **_kwargs: calls.append("consume") or True),
            patch("catchuparr.adapters.sharing_borrow_route.make_task_local_input_buffer",
                  side_effect=lambda *_args, **_kwargs: calls.append("buffer") or object()),
            patch("catchuparr.adapters.sharing_borrow_route.register_borrowed_client",
                  side_effect=lambda *_args, **_kwargs: calls.append("register") or
                  (ack if registration_reply else None)),
            patch("catchuparr.adapters.sharing_borrow_route.adopt_registered_client_in_manager",
                  side_effect=lambda *_args: calls.append("adopt") or True),
            patch("catchuparr.adapters.sharing_borrow_route.cleanup_borrowed_client",
                  side_effect=lambda *_args, **_kwargs: calls.append("cleanup") or cleanup_reply),
            patch("catchuparr.adapters.sharing_borrow_route.ReadOnlyTsBufferReader",
                  side_effect=lambda *_args: calls.append("reader") or FakeReader()),
        )
        return opening, active, manager, ack, ledger, calls, mocks

    def test_open_orders_capability_registration_activation_and_reader(self):
        opening, _active, manager, ack, ledger, calls, mocks = self._open()
        with mocks[0], mocks[1], mocks[2], mocks[3], mocks[4], mocks[5], mocks[6], mocks[7], \
                patch("catchuparr.adapters.sharing_borrow_route.native_borrow_proof_reader",
                      return_value=lambda: None):
            session = BorrowedReaderSession.open(
                capability=capability(), capability_secret=SECRET,
                authenticated_user_id="user-route-synthetic", now=199,
                episode=opening, lease_value="synthetic-route-lease",
                catalog_generation="catalog-route-synthetic",
                ordinary_viewer_ids=("viewer-route-synthetic",),
                redis_text=object(), redis_binary=object(), existing_manager=None,
                manager_type=object, worker_id=IDENTITY[1], buffer_type=object,
                assigned_sources=(self._assigned_source(),), client_id=ack.client_id,
                client_ip="192.0.2.60", user_agent="Synthetic Player", ledger=ledger,
            )
        self.assertEqual(calls, ["consume", "buffer", "register", "adopt", "reader"])
        self.assertIs(session.client_manager, manager)
        self.assertTrue(session.owns_client_manager)

    def test_open_replay_does_not_construct_or_register(self):
        opening, _active, _manager, _ack, _ledger, calls, mocks = self._open()
        with mocks[0], mocks[1], patch(
            "catchuparr.adapters.sharing_borrow_route.consume_opening_capability", return_value=False,
        ), mocks[3], mocks[4]:
            with self.assertRaises(BorrowEpisodeRejected):
                BorrowedReaderSession.open(
                    capability=capability(), capability_secret=SECRET,
                    authenticated_user_id="user-route-synthetic", now=199,
                    episode=opening, lease_value="synthetic-route-lease",
                    catalog_generation="catalog-route-synthetic",
                    ordinary_viewer_ids=("viewer-route-synthetic",),
                    redis_text=object(), redis_binary=object(), existing_manager=None,
                    manager_type=object, worker_id=IDENTITY[1], buffer_type=object,
                    assigned_sources=(self._assigned_source(),),
                    client_id="catchuparr-client-route-synthetic", client_ip="192.0.2.60",
                    user_agent="Synthetic Player", ledger=_ledger,
                )
        self.assertEqual(calls, [])

    def test_open_backend_mismatch_precedes_nonce_and_registration(self):
        opening, _active, _manager, _ack, ledger, calls, mocks = self._open()
        with patch("catchuparr.adapters.sharing_borrow_route.redis_backend_signature",
                   side_effect=("text-backend", "binary-backend")), mocks[2], mocks[4]:
            with self.assertRaises(BorrowReaderUnavailable):
                BorrowedReaderSession.open(
                    capability=capability(), capability_secret=SECRET,
                    authenticated_user_id="user-route-synthetic", now=199,
                    episode=opening, lease_value="synthetic-route-lease",
                    catalog_generation="catalog-route-synthetic",
                    ordinary_viewer_ids=("viewer-route-synthetic",),
                    redis_text=object(), redis_binary=object(), existing_manager=None,
                    manager_type=object, worker_id=IDENTITY[1], buffer_type=object,
                    assigned_sources=(self._assigned_source(),),
                    client_id="catchuparr-client-route-synthetic", client_ip="192.0.2.60",
                    user_agent="Synthetic Player", ledger=ledger,
                )
        self.assertEqual(calls, [])

    def test_open_manager_security_backend_mismatch_precedes_nonce(self):
        opening, _active, manager, _ack, ledger, calls, mocks = self._open()
        with patch("catchuparr.adapters.sharing_borrow_route.redis_backend_signature",
                   side_effect=("secure-backend", "secure-backend", "different-security-backend")), \
                mocks[1], mocks[2]:
            with self.assertRaises(BorrowReaderUnavailable):
                BorrowedReaderSession.open(
                    capability=capability(), capability_secret=SECRET,
                    authenticated_user_id="user-route-synthetic", now=199,
                    episode=opening, lease_value="synthetic-route-lease",
                    catalog_generation="catalog-route-synthetic",
                    ordinary_viewer_ids=("viewer-route-synthetic",),
                    redis_text=object(), redis_binary=object(), existing_manager=None,
                    manager_type=object, worker_id=IDENTITY[1], buffer_type=object,
                    assigned_sources=(self._assigned_source(),),
                    client_id="catchuparr-client-route-synthetic", client_ip="192.0.2.60",
                    user_agent="Synthetic Player", ledger=ledger,
                )
        self.assertEqual(calls, [])
        self.assertEqual(manager.stopped, 1)

    def test_open_failed_cleanup_exposes_retry_handle_and_keeps_manager(self):
        opening, _active, manager, _ack, ledger, _calls, mocks = self._open(cleanup_reply=False)
        cleanup_replies = iter((False, True))
        with mocks[0], mocks[1], mocks[2], mocks[3], mocks[4], mocks[5], patch(
            "catchuparr.adapters.sharing_borrow_route.cleanup_borrowed_client",
            side_effect=lambda *_args, **_kwargs: next(cleanup_replies),
        ), \
                patch("catchuparr.adapters.sharing_borrow_route.ReadOnlyTsBufferReader",
                      side_effect=RuntimeError("synthetic reader construction failure")), \
                patch("catchuparr.adapters.sharing_borrow_route.native_borrow_proof_reader",
                      return_value=lambda: None):
            with self.assertRaises(BorrowOpenCleanupPending) as raised:
                BorrowedReaderSession.open(
                    capability=capability(), capability_secret=SECRET,
                    authenticated_user_id="user-route-synthetic", now=199,
                    episode=opening, lease_value="synthetic-route-lease",
                    catalog_generation="catalog-route-synthetic",
                    ordinary_viewer_ids=("viewer-route-synthetic",),
                    redis_text=object(), redis_binary=object(), existing_manager=None,
                    manager_type=object, worker_id=IDENTITY[1], buffer_type=object,
                    assigned_sources=(self._assigned_source(),),
                    client_id="catchuparr-client-route-synthetic", client_ip="192.0.2.60",
                    user_agent="Synthetic Player", ledger=ledger,
                )
            self.assertIsNotNone(raised.exception.ack)
            self.assertEqual(manager.stopped, 0)
            self.assertTrue(raised.exception.retry_cleanup())
            self.assertEqual(manager.stopped, 1)
            self.assertTrue(raised.exception.retry_cleanup())
            self.assertEqual(manager.stopped, 1)

    def test_open_uncertain_registration_without_ack_retains_owned_manager(self):
        opening, _active, manager, _ack, ledger, _calls, mocks = self._open()
        with mocks[0], mocks[1], mocks[2], mocks[3], patch(
            "catchuparr.adapters.sharing_borrow_route.register_borrowed_client",
            side_effect=RuntimeError("synthetic Redis reply unavailable"),
        ), mocks[5], mocks[6]:
            with self.assertRaises(BorrowOpenCleanupPending) as raised:
                BorrowedReaderSession.open(
                    capability=capability(), capability_secret=SECRET,
                    authenticated_user_id="user-route-synthetic", now=199,
                    episode=opening, lease_value="synthetic-route-lease",
                    catalog_generation="catalog-route-synthetic",
                    ordinary_viewer_ids=("viewer-route-synthetic",),
                    redis_text=object(), redis_binary=object(), existing_manager=None,
                    manager_type=object, worker_id=IDENTITY[1], buffer_type=object,
                    assigned_sources=(self._assigned_source(),),
                    client_id="catchuparr-client-route-synthetic", client_ip="192.0.2.60",
                    user_agent="Synthetic Player", ledger=ledger,
                )
        self.assertIsNone(raised.exception.ack)
        self.assertFalse(raised.exception.retry_cleanup())
        self.assertEqual(manager.stopped, 0)

    def test_open_unknown_registration_shared_manager_uses_fenced_absence_retry(self):
        opening, _active, manager, _ack, ledger, _calls, mocks = self._open(owns_manager=False)
        with mocks[0], mocks[1], mocks[2], mocks[3], patch(
            "catchuparr.adapters.sharing_borrow_route.register_borrowed_client",
            side_effect=RuntimeError("synthetic Redis reply unavailable"),
        ), mocks[5], mocks[6], patch(
            "catchuparr.adapters.sharing_borrow_route._recover_registration_ack",
            return_value=None,
        ), patch(
            "catchuparr.adapters.sharing_borrow_route.fence_unregistered_opening",
            return_value=True,
        ), patch(
            "catchuparr.adapters.sharing_borrow_route.close_fenced_unregistered_opening",
            return_value=True,
        ):
            with self.assertRaises(BorrowOpenCleanupPending) as raised:
                BorrowedReaderSession.open(
                    capability=capability(), capability_secret=SECRET,
                    authenticated_user_id="user-route-synthetic", now=199,
                    episode=opening, lease_value="synthetic-route-lease",
                    catalog_generation="catalog-route-synthetic",
                    ordinary_viewer_ids=("viewer-route-synthetic",),
                    redis_text=object(), redis_binary=object(), existing_manager=None,
                    manager_type=object, worker_id=IDENTITY[1], buffer_type=object,
                    assigned_sources=(self._assigned_source(),),
                    client_id="catchuparr-client-route-synthetic", client_ip="192.0.2.60",
                    user_agent="Synthetic Player", ledger=ledger,
                )
            self.assertTrue(raised.exception.retry_cleanup())
            self.assertTrue(raised.exception.retry_cleanup())
        self.assertEqual(manager.stopped, 0)

    def test_stream_failure_runs_exact_cleanup_and_stops_only_owned_manager(self):
        manager = FakeManager()
        calls = []
        ack = NativeClientRegistrationAck(
            channel_uuid=IDENTITY[0], episode_id="episode-route-synthetic",
            client_id="catchuparr-client-route-synthetic", local_worker_id=IDENTITY[1],
            redis_backend_signature="backend-signature-synthetic", connected_at="123.5",
            projection_version=2, user_agent="Synthetic Player", client_ip="192.0.2.60",
        )
        session = BorrowedReaderSession(
            reader=FakeReader(), ack=ack, client_manager=manager,
            owns_client_manager=True,
            cleanup=lambda received_ack, received_manager, owned: calls.append(
                (received_ack, received_manager, owned)
            ) or True,
        )
        iterator = session.iter_chunks()
        self.assertEqual(next(iterator), b"synthetic-ts-chunk")
        with self.assertRaisesRegex(RuntimeError, "synthetic stream failure"):
            next(iterator)
        self.assertEqual(calls, [(ack, manager, True)])
        self.assertEqual(manager.stopped, 1)
        self.assertTrue(session.close())
        self.assertEqual(len(calls), 1)

    def test_failed_exact_cleanup_keeps_owned_manager_alive_for_retry(self):
        manager = FakeManager()
        replies = iter((False, True))
        ack = NativeClientRegistrationAck(
            channel_uuid=IDENTITY[0], episode_id="episode-route-synthetic",
            client_id="catchuparr-client-route-synthetic", local_worker_id=IDENTITY[1],
            redis_backend_signature="backend-signature-synthetic", connected_at="123.5",
            projection_version=2, user_agent="Synthetic Player", client_ip="192.0.2.60",
        )
        session = BorrowedReaderSession(
            reader=FakeReader(), ack=ack, client_manager=manager,
            owns_client_manager=True,
            cleanup=lambda *_args: next(replies),
        )
        self.assertFalse(session.close())
        self.assertEqual(manager.stopped, 0)
        self.assertTrue(session.close())
        self.assertEqual(manager.stopped, 1)


if __name__ == "__main__":
    unittest.main()
