import io
import json
import sys
import threading
import time
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import patch

from scripts import aio_recorder_failover
from scripts import aio_recorder_media as probe


class _FakeBuffer:
    def __init__(self, chunks, next_index):
        self.chunks = chunks
        self.next_index = next_index

    def get_optimized_client_data(self, _client_index):
        return self.chunks, self.next_index


class _FakeRedis:
    def __init__(self, state_after_reads):
        self.state_after_reads = state_after_reads
        self.reads = 0

    def get(self, _key):
        return b"native-owner"

    def hget(self, _key, _field):
        self.reads += 1
        if self.reads >= self.state_after_reads:
            return b"ACTIVE"
        return b"CONNECTING"

    def scard(self, _key):
        return 1


class RecorderMediaProbeTests(unittest.TestCase):
    def test_dead_recorder_failure_reports_safe_terminal_task_diagnostics(self):
        thread = threading.Thread(target=lambda: None)
        thread.start()
        thread.join()
        run = SimpleNamespace(
            _lock=threading.Lock(),
            thread=thread,
            stop_event=threading.Event(),
            failures=[
                "task:ValueError", "SyntheticSecretTokenABC123",
                "arbitrary:SyntheticSecretTokenABC123",
            ],
            result={"status": "sharing_cleanup_pending"},
            results=[SimpleNamespace(
                status="media_stalled", useful_segments=0, return_code=17,
                attempt_switch_id=None,
            )],
            processes=[SimpleNamespace(poll=lambda: 17)],
            input_headers=[{"X-Catchuparr-Borrow": "synthetic-bearer-secret"}],
        )
        output = io.StringIO()
        with redirect_stdout(output), self.assertRaisesRegex(
            RuntimeError, "Synthetic recorder task ended before expected media was indexed",
        ):
            with patch.dict(
                sys.modules,
                {"aio_recorder_failover": SimpleNamespace(
                    _segment_has_useful_av=lambda *_: True,
                    _segment_tone_frequency=lambda *_: 880,
                )},
            ):
                probe._wait_for_indexed_tone(
                    run, object(), "synthetic-channel", "ffmpeg", "ffprobe",
                    expected_hz=880, existing_ids=set(), minimum=1, timeout=1,
                )
        line = next(
            line for line in output.getvalue().splitlines()
            if line.startswith("CATCHUPARR_RECORDER_EARLY_EXIT_DIAGNOSTIC ")
        )
        diagnostic = json.loads(line.split(" ", 1)[1])
        self.assertEqual("sharing_cleanup_pending", diagnostic["task_status"])
        self.assertEqual(
            ["ValueError", "other", "other"], diagnostic["failure_classes"],
        )
        self.assertEqual(
            [{
                "status": "media_stalled", "useful_segments": 0,
                "return_code": 17, "switch_id_present": False,
            }],
            diagnostic["attempts"],
        )
        self.assertEqual([17], diagnostic["process_exit_codes"])
        self.assertNotIn("synthetic-bearer-secret", line)
        self.assertNotIn("SyntheticSecretTokenABC123", line)

    def test_recorder_task_diagnostic_preserves_finite_task_statuses_only(self):
        statuses = {
            "control_unavailable", "legacy_job", "invalid_schedule",
            "sharing_unavailable", "sharing_return_recovery_pending",
            "sharing_authority_unavailable", "sharing_cleanup_pending",
            "sharing_admission_uncertain", "recording_paused", "stale_control",
            "disabled", "stale_configuration", "outside_schedule",
            "no_permitted_sources", "proxy_integration_unsupported",
            "already_running", "stopped", "no_media",
        }
        for status in statuses:
            with self.subTest(status=status):
                diagnostic = probe._recorder_task_early_exit_diagnostic(
                    SimpleNamespace(result={"status": status})
                )
                self.assertEqual(status, diagnostic["task_status"])

        diagnostic = probe._recorder_task_early_exit_diagnostic(
            SimpleNamespace(result={"status": "SyntheticSecretTokenABC123"})
        )
        self.assertEqual("other", diagnostic["task_status"])

    def test_teardown_budget_accounts_for_sequential_shutdown_and_marker_ttl(self):
        self.assertEqual(
            156.0,
            probe._native_teardown_readiness_timeout(120, 1),
        )

    def test_borrow_teardown_budget_covers_native_error_marker_ttl(self):
        self.assertEqual(
            186.0,
            probe._native_teardown_readiness_timeout(
                120, 1, stopping_marker_ttl=60,
            ),
        )

    def test_readiness_poll_observes_shutdown_then_marker_before_open(self):
        class FakeClock:
            now = 0.0
            phases = []

            def monotonic(self):
                return self.now

            def sleep(self, duration):
                self.now += duration

        clock = FakeClock()
        opens = []

        def read_native_state():
            if clock.now < 120:
                phase = "disconnect_delay"
            elif clock.now < 150:
                phase = "stopping_marker_ttl"
            else:
                phase = "ready"
            clock.phases.append(phase)
            return {
                "conditions": {"ready": phase == "ready"},
                "phase": phase,
                "independent_source_b_active": 1,
            }

        ready, final = probe._wait_for_readiness_snapshot(
            read_native_state,
            probe._native_teardown_readiness_timeout(120, 1),
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            poll_interval=1,
        )
        self.assertTrue(ready)
        self.assertEqual("ready", final["phase"])
        self.assertEqual(1, final["independent_source_b_active"])
        self.assertIn("disconnect_delay", clock.phases)
        self.assertIn("stopping_marker_ttl", clock.phases)
        self.assertEqual(150, clock.now)
        result = probe._open_after_readiness(
            lambda: self.assertTrue(ready),
            lambda: opens.append("one ordinary GET") or "response",
        )
        self.assertEqual("response", result)
        self.assertEqual(["one ordinary GET"], opens)

    def test_unready_native_snapshot_stays_bounded_and_never_opens_viewer(self):
        class FakeClock:
            now = 0.0

            def monotonic(self):
                return self.now

            def sleep(self, duration):
                self.now += duration

        clock = FakeClock()
        reads = []
        opens = []

        def wait_for_ready():
            ready, last = probe._wait_for_readiness_snapshot(
                lambda: reads.append(clock.now) or {"conditions": {"ready": False}},
                3,
                monotonic=clock.monotonic,
                sleep=clock.sleep,
                poll_interval=1,
            )
            self.assertFalse(ready)
            self.assertEqual({"ready": False}, last["conditions"])
            raise RuntimeError("bounded readiness timeout")

        with self.assertRaisesRegex(RuntimeError, "bounded readiness timeout"):
            probe._open_after_readiness(
                wait_for_ready,
                lambda: opens.append("must not run"),
            )
        self.assertEqual(3, clock.now)
        self.assertEqual(4, len(reads))
        self.assertEqual([], opens)

    def test_readiness_failure_diagnostic_getter_error_is_guarded(self):
        manager = SimpleNamespace(
            get_client_count=lambda: (_ for _ in ()).throw(ValueError("bad count")),
        )
        emitted = []
        with self.assertRaisesRegex(RuntimeError, "native readiness timeout"):
            probe._require_with_failure_diagnostic(
                False,
                "native readiness timeout",
                lambda: {"local": int(manager.get_client_count())},
                emit=emitted.append,
                diagnostic_label="CATCHUPARR_NATIVE_REOPEN_DIAGNOSTIC",
            )
        self.assertIn('"diagnostic_error": "ValueError"', emitted[0])

    def test_failure_diagnostic_read_error_preserves_original_gate_failure(self):
        def broken_diagnostic():
            raise ValueError("synthetic secret diagnostic data")

        emitted = []
        with self.assertRaisesRegex(RuntimeError, "original gate failure"):
            probe._require_with_failure_diagnostic(
                False,
                "original gate failure",
                broken_diagnostic,
                emit=emitted.append,
            )
        self.assertEqual(len(emitted), 1)
        self.assertIn('"diagnostic_error": "ValueError"', emitted[0])
        self.assertNotIn("synthetic secret", emitted[0])

    def test_local_manager_count_error_preserves_original_gate_failure(self):
        manager = SimpleNamespace(
            get_client_count=lambda: (_ for _ in ()).throw(ValueError("bad count")),
        )
        emitted = []

        def diagnostic_boundary():
            return {"local": int(manager.get_client_count())}

        with self.assertRaisesRegex(RuntimeError, "original gate failure"):
            probe._require_with_failure_diagnostic(
                False,
                "original gate failure",
                diagnostic_boundary,
                emit=emitted.append,
            )
        self.assertEqual(len(emitted), 1)
        self.assertIn('"diagnostic_error": "ValueError"', emitted[0])

    def test_successful_gate_does_not_read_local_manager_count(self):
        manager = SimpleNamespace(
            get_client_count=lambda: self.fail("diagnostic-only count was read"),
        )
        probe._require_with_failure_diagnostic(
            True,
            "unused",
            lambda: {"local": manager.get_client_count()},
        )

    def test_failure_diagnostic_output_error_preserves_original_gate_failure(self):
        def fail_output(_message):
            raise OSError("synthetic output detail")

        with self.assertRaisesRegex(RuntimeError, "original gate failure"):
            probe._require_with_failure_diagnostic(
                False,
                "original gate failure",
                lambda: {"safe": True},
                emit=fail_output,
            )

    def test_failure_diagnostic_supports_separate_borrow_return_label(self):
        emitted = []
        with self.assertRaisesRegex(RuntimeError, "borrow return failed"):
            probe._require_with_failure_diagnostic(
                False,
                "borrow return failed",
                lambda: {"profile_a_at_baseline": False},
                emit=emitted.append,
                diagnostic_label="CATCHUPARR_BORROW_RETURN_DIAGNOSTIC",
            )
        self.assertEqual(1, len(emitted))
        self.assertTrue(emitted[0].startswith(
            "CATCHUPARR_BORROW_RETURN_DIAGNOSTIC ",
        ))
        self.assertIn('"profile_a_at_baseline": false', emitted[0])

    def test_successful_gate_does_not_run_diagnostic(self):
        def unexpected_diagnostic():
            self.fail("successful gate must not read diagnostic state")

        probe._require_with_failure_diagnostic(
            True, "unused", unexpected_diagnostic,
        )

    def test_no_source_acceptance_keeps_seven_original_conditions(self):
        conditions = {
            "source_b_request_count_unchanged": True,
            "source_a_active_zero": True,
            "source_a_request_count_unchanged": True,
            "native_global_clients_zero": True,
            "native_owner_absent": True,
            "profile_a_at_baseline": True,
            "profile_b_at_baseline": True,
        }
        self.assertEqual(7, len(conditions))
        self.assertTrue(all(conditions.values()))
        # The local worker count is emitted only as diagnostic context.
        diagnostic = {"conditions": conditions, "native_local_clients_zero": False}
        self.assertTrue(all(diagnostic["conditions"].values()))

    def test_live_failover_provider_exposes_pre_response_request_boundary(self):
        source = object.__new__(aio_recorder_failover._SyntheticFailoverSourceServer)
        source._lock = threading.Lock()
        source.request_started_at = {"source-b.ts": [123.5]}
        source.request_start_callback = None
        source.request_start_callback_errors = []
        source.request_callback_finished = threading.Event()
        observed = []

        def callback(name, number, started):
            observed.append((name, number, started))
            return True

        self.assertEqual([123.5], source.request_times("source-b.ts"))
        source.install_request_start_callback(callback)
        self.assertIs(source.request_start_callback, callback)
        self.assertTrue(source._run_request_start_callback(
            source.request_start_callback, "source-b.ts", 1, 123.5,
        ))
        self.assertTrue(source.request_callback_finished.is_set())
        self.assertEqual([("source-b.ts", 1, 123.5)], observed)
        self.assertTrue(source.clear_request_start_callback(callback))
        self.assertIsNone(source.request_start_callback)

    def test_synthetic_provider_waits_for_boundary_callback_and_rejects_failed_boundary(self):
        source = object.__new__(probe._SyntheticSourceServer)
        source._counts_lock = threading.Lock()
        source.request_start_callback_errors = []
        source.request_callback_finished = threading.Event()
        callback_entered = threading.Event()
        release_callback = threading.Event()

        def blocked(*_args):
            callback_entered.set()
            self.assertTrue(release_callback.wait(2))
            return True

        worker = threading.Thread(
            target=source._run_request_start_callback,
            args=(blocked, "source-b.ts", 1, 1.0),
        )
        worker.start()
        self.assertTrue(callback_entered.wait(2))
        self.assertFalse(source.request_callback_finished.is_set())
        release_callback.set()
        worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertTrue(source.request_callback_finished.is_set())

        source.request_callback_finished.clear()
        def fail(*_args):
            raise RuntimeError("synthetic boundary denial")
        self.assertFalse(source._run_request_start_callback(fail, "source-b.ts", 1, 1.0))
        self.assertTrue(source.request_callback_finished.is_set())
        self.assertEqual(1, len(source.request_start_callback_errors))

        source.request_callback_finished.clear()
        self.assertTrue(
            source._run_request_start_callback(
                lambda *_args: False, "unrelated", 2, 2.0,
            ),
        )
        self.assertFalse(source.request_callback_finished.is_set())
        self.assertTrue(
            source._run_request_start_callback(
                lambda *_args: True, "source-b.ts", 3, 3.0,
            ),
        )
        self.assertTrue(source.request_callback_finished.is_set())

    def test_private_worker_stop_does_not_count_a_provider_reconnect(self):
        before = {"source-b.ts": 4}
        probe._assert_private_worker_stop_request_count(
            before, {"source-b.ts": 4},
        )
        with self.assertRaises(RuntimeError):
            probe._assert_private_worker_stop_request_count(
                before, {"source-b.ts": 5},
            )

    def test_media_probe_attempt_uses_current_applied_control_generation(self):
        from catchuparr import recorder_proxy

        active = {"version": 4, "channel_uuids": "synthetic-channel"}
        generation = recorder_proxy.configuration_generation(active)
        controls = (
            SimpleNamespace(
                paused=False, generation=12, configuration_generation=generation,
            ),
            SimpleNamespace(
                paused=False, generation=14, configuration_generation=generation,
            ),
        )
        redis_client = object()
        lease = object()
        active_path = probe.Path("/synthetic/active.json")
        candidate = {"id": "23", "account_id": "7"}

        with patch(
            "catchuparr.configuration.load_applied_state",
            side_effect=[(active, controls[0]), (active, controls[1])],
        ) as load_applied_state, patch(
            "catchuparr.recorder_proxy.issue_recorder_attempt",
            autospec=True,
            side_effect=lambda *_args, **_kwargs: "attempt",
        ) as issue:
            for control in controls:
                self.assertEqual(
                    "attempt",
                    probe._issue_media_probe_attempt(
                        redis_client,
                        lease,
                        channel_uuid="synthetic-channel",
                        candidate=candidate,
                        active_path=active_path,
                        internal_base_url="http://127.0.0.1",
                    ),
                )

        self.assertEqual(load_applied_state.call_count, 2)
        self.assertEqual(
            [call.kwargs["control_generation"] for call in issue.call_args_list],
            [12, 14],
        )
        self.assertTrue(all(call.kwargs["config_generation"] == generation for call in issue.call_args_list))

    def test_media_probe_attempt_rejects_paused_or_mismatched_control_state(self):
        from catchuparr import recorder_proxy

        active = {"version": 4, "channel_uuids": "synthetic-channel"}
        generation = recorder_proxy.configuration_generation(active)
        invalid_controls = (
            SimpleNamespace(
                paused=True, generation=12, configuration_generation=generation,
            ),
            SimpleNamespace(
                paused=False, generation=12, configuration_generation="stale-generation",
            ),
        )
        with patch(
            "catchuparr.configuration.load_applied_state",
            side_effect=[(active, control) for control in invalid_controls],
        ), patch(
            "catchuparr.recorder_proxy.issue_recorder_attempt",
            autospec=True,
        ) as issue:
            for _ in invalid_controls:
                with self.assertRaises(RuntimeError):
                    probe._issue_media_probe_attempt(
                        object(),
                        object(),
                        channel_uuid="synthetic-channel",
                        candidate={"id": "23", "account_id": "7"},
                        active_path=probe.Path("/synthetic/active.json"),
                        internal_base_url="http://127.0.0.1",
                    )
        issue.assert_not_called()

    def test_live_a_invariant_diagnostic_reports_each_boolean_without_values(self):
        all_true = dict.fromkeys(probe._NATIVE_LIVE_A_INVARIANT_NAMES, True)
        self.assertTrue(all(probe._native_live_a_invariant_flags(**all_true).values()))

        for failed_name in probe._NATIVE_LIVE_A_INVARIANT_NAMES:
            observations = dict(all_true)
            observations[failed_name] = False
            flags = probe._native_live_a_invariant_flags(**observations)
            self.assertEqual(
                [name for name, passed in flags.items() if not passed],
                [failed_name],
            )
            message = probe._native_live_a_invariant_message(
                "after_pause_private_cleanup", flags,
            )
            self.assertIn("after_pause_private_cleanup", message)
            self.assertIn(f'"{failed_name}": false', message)
            self.assertNotIn("synthetic-owner-value", message)

    def test_early_exhaustion_diagnostic_counts_accepted_and_filtered_chunks(self):
        decisions = iter((False, True, True))
        iterator = iter((b"filtered", b"accepted", b"tail"))

        with self.assertRaises(probe._MediaReadDiagnosticError) as raised:
            probe._read_stream_iterator(
                iterator,
                minimum_bytes=20,
                timeout=1,
                diagnostic_label="parent_after_follower",
                accept_chunk=lambda: next(decisions),
                diagnostic_fields=lambda: {
                    "publication_floor": 7,
                    "current_head": 9,
                    "last_yield_index": 8,
                },
            )

        self.assertEqual(
            raised.exception.diagnostic,
            {
                "callsite": "parent_after_follower",
                "accepted_bytes": len(b"accepted") + len(b"tail"),
                "accepted_chunks": 2,
                "discarded_bytes": len(b"filtered"),
                "discarded_chunks": 1,
                "minimum_bytes": 20,
                "termination": "stop_iteration",
                "publication_floor": 7,
                "current_head": 9,
                "last_yield_index": 8,
            },
        )

    def test_filtered_stream_exhaustion_reports_static_callsite_and_no_payload(self):
        with self.assertRaises(probe._MediaReadDiagnosticError) as raised:
            probe._read_stream_iterator(
                iter((b"synthetic-payload",)),
                minimum_bytes=100,
                timeout=1,
                diagnostic_label="follower_initial",
                accept_chunk=lambda: False,
            )

        diagnostic = raised.exception.diagnostic
        self.assertEqual(diagnostic["callsite"], "follower_initial")
        self.assertEqual(diagnostic["accepted_bytes"], 0)
        self.assertEqual(diagnostic["accepted_chunks"], 0)
        self.assertEqual(diagnostic["discarded_bytes"], len(b"synthetic-payload"))
        self.assertEqual(diagnostic["discarded_chunks"], 1)
        self.assertEqual(diagnostic["termination"], "stop_iteration")
        self.assertNotIn("synthetic-payload", str(diagnostic))

    def test_initial_media_exhaustion_keeps_native_diagnostic_before_readiness_wait(self):
        class Redis:
            @staticmethod
            def get(_key):
                return b"synthetic-owner"

            @staticmethod
            def hget(_key, _field):
                return b"ACTIVE"

        class Keys:
            channel_owner = staticmethod(lambda _worker_id: "owner")

        class Fields:
            STATE = "state"

        class States:
            ACTIVE = "ACTIVE"

        manager = SimpleNamespace(running=True)
        native_server = SimpleNamespace(
            worker_id="synthetic-owner",
            stream_managers={"worker": manager},
            client_managers={"worker": object()},
        )
        expected_native_owner = probe._redis_text(native_server.worker_id)

        with self.assertRaises(probe._MediaReadDiagnosticError) as raised:
            probe._read_stream_iterator(
                iter(()),
                minimum_bytes=96256,
                timeout=1,
                diagnostic_label="parent_initial",
                diagnostic_context=lambda: probe._native_media_reader_diagnostic_context(
                    Redis(),
                    native_server,
                    "worker",
                    expected_native_owner,
                    metadata_key="metadata",
                    redis_keys=Keys,
                    metadata_field=Fields,
                    channel_state=States,
                    binding=None,
                    capability_binding_current_fn=lambda *_args: False,
                    load_applied_state_fn=lambda: (None, None),
                    configuration_generation_fn=lambda _active: "generation",
                ),
            )

        diagnostic = raised.exception.diagnostic
        self.assertEqual(diagnostic["callsite"], "parent_initial")
        self.assertEqual(diagnostic["termination"], "stop_iteration")
        self.assertEqual(
            diagnostic["native"],
            {
                "owner_present": True,
                "owner_matches": True,
                "manager_present": True,
                "manager_running": True,
                "client_manager_present": True,
                "metadata_active": True,
                "metadata_stopping": False,
                "metadata_error": False,
                "capability_binding_current": False,
                "control_binding_current": False,
            },
        )

    def test_follower_child_media_diagnostic_is_preserved_by_parent(self):
        child_diagnostic = {
            "callsite": "follower_initial",
            "accepted_bytes": 17,
            "accepted_chunks": 1,
            "discarded_bytes": 0,
            "discarded_chunks": 0,
            "minimum_bytes": 96256,
            "termination": "stop_iteration",
            "native": {"owner_matches": True},
        }
        output = (
            b"CATCHUPARR_RECORDER_FOLLOWER_MEDIA_DIAGNOSTIC "
            + json.dumps(child_diagnostic).encode("utf-8")
            + b"\n"
        )
        attempt = SimpleNamespace(
            capability="synthetic-capability",
            worker_id="synthetic-worker",
            config_generation="synthetic-generation",
        )

        with patch.object(
            probe.subprocess,
            "run",
            return_value=SimpleNamespace(returncode=1, stdout=output),
        ):
            with self.assertRaises(probe._MediaReadDiagnosticError) as raised:
                probe._run_fresh_process_follower(
                    attempt,
                    channel_uuid="synthetic-channel",
                    stream_id="synthetic-stream",
                    account_id="synthetic-account",
                    profile_id=7,
                    expected_profile_count=3,
                    ffprobe="ffprobe",
                    native_owner="synthetic-owner",
                    parent_client_count=1,
                    diagnostic_context=lambda: {"synthetic_source_requests": {"source-b.ts": 1}},
                )

        self.assertEqual(raised.exception.diagnostic["callsite"], "follower_initial")
        self.assertEqual(raised.exception.diagnostic["termination"], "stop_iteration")
        self.assertEqual(raised.exception.diagnostic["native"], {"owner_matches": True})
        self.assertEqual(
            raised.exception.diagnostic["parent_synthetic_source"],
            {"synthetic_source_requests": {"source-b.ts": 1}},
        )

    def test_live_route_tone_assertion_requires_expected_source(self):
        probe._assert_tone_matches(441, 440, "Synthetic live A")
        with self.assertRaisesRegex(RuntimeError, "unexpected synthetic source tone"):
            probe._assert_tone_matches(880, 440, "Synthetic live A")
        with self.assertRaisesRegex(RuntimeError, "no decodable synthetic audio tone"):
            probe._assert_tone_matches(None, 440, "Synthetic live A")

    def test_native_source_metadata_snapshot_ignores_health_fields(self):
        class MetadataRedis:
            @staticmethod
            def hgetall(_key):
                return {
                    b"stream_id": b"23",
                    b"stream_name": b"Synthetic isolation source A",
                    b"url": b"http://127.0.0.1:12345/source-a.ts",
                    b"state": b"ACTIVE",
                    b"client_count": b"1",
                }

        metadata = probe._native_source_metadata(MetadataRedis(), "metadata")
        self.assertEqual(
            metadata,
            {
                "stream_id": "23",
                "stream_name": "Synthetic isolation source A",
                "url": "http://127.0.0.1:12345/source-a.ts",
            },
        )

    def test_native_live_reader_returns_only_chunks_after_publication_floor(self):
        class Tracker:
            last_yielded_index = None

        tracker = Tracker()

        def chunks():
            for index in range(1, 5):
                tracker.last_yielded_index = index
                yield bytes((index,)) * 188

        reader = probe._NativeLiveMediaReader(iter(chunks()), tracker)
        try:
            media, last_index = reader.read_after(2, minimum_bytes=376, timeout=1)
        finally:
            reader.thread.join(timeout=1)
        self.assertEqual(last_index, 4)
        self.assertEqual(media, bytes((3,)) * 188 + bytes((4,)) * 188)

    def test_live_reader_stops_at_iterator_boundary_before_response_close(self):
        class Tracker:
            last_yielded_index = 0

        tracker = Tracker()

        class SlowIterator:
            def __init__(self):
                self.lock = threading.Lock()
                self.active = False

            def __iter__(self):
                return self

            def __next__(self):
                with self.lock:
                    self.active = True
                time.sleep(0.02)
                with self.lock:
                    self.active = False
                tracker.last_yielded_index += 1
                return b"x" * 188

        iterator = SlowIterator()
        reader = probe._NativeLiveMediaReader(iterator, tracker)

        class Response:
            closed = False

            def close(self):
                with iterator.lock:
                    self.assert_idle = not iterator.active
                self.assert_not_reading = not reader.thread.is_alive()
                self.closed = True

        response = Response()
        force_calls = []
        reader.close(response, force_release=lambda: force_calls.append(True), join_timeout=1)
        self.assertTrue(response.closed)
        self.assertTrue(response.assert_idle)
        self.assertTrue(response.assert_not_reading)
        self.assertEqual(force_calls, [])

    def test_live_reader_session_owns_one_reader_through_idempotent_close(self):
        buffer = _FakeBuffer([b"x" * 188], next_index=1)

        class Response:
            def __init__(self):
                self.close_count = 0

            def stream(self):
                chunks, _next_index = buffer.get_optimized_client_data(0)
                yield from chunks

            @property
            def streaming_content(self):
                return self.stream()

            def close(self):
                self.close_count += 1

        response = Response()
        reader_type = probe._NativeLiveMediaReader
        constructed = []

        def construct_reader(iterator, tracker):
            reader = reader_type(iterator, tracker)
            constructed.append(reader)
            return reader

        with patch.object(
            probe, "_NativeLiveMediaReader", side_effect=construct_reader,
        ) as reader_factory:
            session = probe._NativeLiveReaderSession(response, buffer)
            self.assertEqual(reader_factory.call_count, 1)
            self.assertIs(session.reader, constructed[0])
            session.close()
            session.close()

        self.assertFalse(session.reader.thread.is_alive())
        self.assertEqual(response.close_count, 1)
        self.assertEqual(reader_factory.call_count, 1)

    def test_live_reader_session_reacquires_buffer_after_native_initialization(self):
        preinit_buffer = _FakeBuffer([], next_index=0)
        initialized_buffer = _FakeBuffer([], next_index=0)

        class NativeServer:
            def __init__(self):
                self.current_buffer = preinit_buffer

            def get_buffer(self, worker_id, *, profile):
                self.lookup = (worker_id, profile)
                return self.current_buffer

        class Response:
            streaming_content = iter(())

            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        native_server = NativeServer()
        preinit_lookup = native_server.get_buffer("synthetic-worker", profile=None)
        native_server.current_buffer = initialized_buffer
        response = Response()

        session, bound_buffer = probe._create_native_live_reader_session(
            response, native_server, "synthetic-worker",
        )
        self.assertIsNot(preinit_lookup, initialized_buffer)
        self.assertIs(bound_buffer, initialized_buffer)
        self.assertIs(session.tracker.buffer, initialized_buffer)
        self.assertEqual(native_server.lookup, ("synthetic-worker", None))
        session.close()
        self.assertTrue(response.closed)

    def test_blocked_live_reader_stops_native_channel_before_response_close(self):
        class Tracker:
            last_yielded_index = None

        class BlockingIterator:
            def __init__(self):
                self.started = threading.Event()
                self.unblock = threading.Event()

            def __iter__(self):
                return self

            def __next__(self):
                self.started.set()
                if not self.unblock.wait(1):
                    raise RuntimeError("test iterator was not released")
                raise StopIteration

        iterator = BlockingIterator()
        reader = probe._NativeLiveMediaReader(iterator, Tracker())
        self.assertTrue(iterator.started.wait(1))
        events = []

        class Response:
            def close(self):
                self.closed_after_reader_stopped = not reader.thread.is_alive()
                events.append("response-close")

        response = Response()

        def stop_native_channel():
            events.append("native-stop")
            iterator.unblock.set()

        reader.close(response, force_release=stop_native_channel, join_timeout=0.01)
        self.assertEqual(events, ["native-stop", "response-close"])
        self.assertTrue(response.closed_after_reader_stopped)
        self.assertFalse(reader.thread.is_alive())

    def test_redis_metadata_normalizes_bytes_strings_and_enum_values(self):
        enum_value = SimpleNamespace(value="ACTIVE")
        self.assertEqual(probe._redis_text(b"owner"), "owner")
        self.assertEqual(probe._redis_text("owner"), "owner")
        self.assertEqual(probe._constant_text(enum_value), "ACTIVE")

    def test_native_active_wait_polls_until_state_is_really_active(self):
        redis = _FakeRedis(state_after_reads=2)
        manager = SimpleNamespace(running=True)
        client_manager = SimpleNamespace(get_client_count=lambda: 1)
        server = SimpleNamespace(
            worker_id="native-owner",
            stream_managers={"worker": manager},
            client_managers={"worker": client_manager},
        )

        class Keys:
            @staticmethod
            def channel_owner(_worker_id):
                return "owner"

            @staticmethod
            def channel_metadata(_worker_id):
                return "metadata"

            @staticmethod
            def clients(_worker_id):
                return "clients"

        class Fields:
            STATE = "state"

        class States:
            ACTIVE = "ACTIVE"

        owner, found_manager, found_client_manager = probe._wait_for_native_active(
            redis,
            server,
            "worker",
            redis_keys=Keys,
            metadata_field=Fields,
            channel_state=States,
            timeout=1,
        )

        self.assertEqual(owner, "native-owner")
        self.assertIs(found_manager, manager)
        self.assertIs(found_client_manager, client_manager)
        self.assertEqual(redis.reads, 2)

    def test_native_client_wait_waits_for_one_registered_local_and_redis_client(self):
        local_counts = iter((0, 1))
        redis_counts = iter((0, 1))
        manager = SimpleNamespace(get_client_count=lambda: next(local_counts))
        server = SimpleNamespace(client_managers={"worker": manager})

        class ClientRedis:
            @staticmethod
            def scard(_key):
                return next(redis_counts)

        class Keys:
            @staticmethod
            def clients(_worker_id):
                return "clients"

        probe._wait_for_native_client_count(
            ClientRedis(), server, Keys, "worker", 1, timeout=1,
            reject_excess=True,
        )

    def test_native_client_wait_rejects_extra_clients_during_registration(self):
        manager = SimpleNamespace(get_client_count=lambda: 2)
        server = SimpleNamespace(client_managers={"worker": manager})

        class ClientRedis:
            @staticmethod
            def scard(_key):
                return 2

        class Keys:
            @staticmethod
            def clients(_worker_id):
                return "clients"

        with self.assertRaisesRegex(RuntimeError, "more clients"):
            probe._wait_for_native_client_count(
                ClientRedis(), server, Keys, "worker", 1, timeout=1,
                reject_excess=True,
            )

    def test_native_client_wait_allows_bounded_two_to_one_disconnect(self):
        local_counts = iter((2, 1))
        redis_counts = iter((2, 1))
        manager = SimpleNamespace(get_client_count=lambda: next(local_counts))
        server = SimpleNamespace(client_managers={"worker": manager})

        class ClientRedis:
            @staticmethod
            def scard(_key):
                return next(redis_counts)

        class Keys:
            @staticmethod
            def clients(_worker_id):
                return "clients"

        probe._wait_for_native_client_count(
            ClientRedis(), server, Keys, "worker", 1, timeout=1,
        )

    def test_native_client_wait_separates_global_borrow_from_parent_local_count(self):
        manager = SimpleNamespace(get_client_count=lambda: 1)
        server = SimpleNamespace(client_managers={"worker": manager})

        class ClientRedis:
            @staticmethod
            def scard(_key):
                return 2

        class Keys:
            @staticmethod
            def clients(_worker_id):
                return "clients"

        probe._wait_for_native_client_count(
            ClientRedis(), server, Keys, "worker", 2,
            expected_local=1, timeout=1,
        )

    def test_native_client_wait_rejects_global_borrow_missing_from_redis(self):
        manager = SimpleNamespace(get_client_count=lambda: 1)
        server = SimpleNamespace(client_managers={"worker": manager})

        class ClientRedis:
            @staticmethod
            def scard(_key):
                return 1

        class Keys:
            @staticmethod
            def clients(_worker_id):
                return "clients"

        with self.assertRaisesRegex(RuntimeError, "bounded state"):
            probe._wait_for_native_client_count(
                ClientRedis(), server, Keys, "worker", 2,
                expected_local=1, timeout=0.01,
            )

    def test_native_active_wait_does_not_accept_connecting(self):
        redis = _FakeRedis(state_after_reads=10**9)
        manager = SimpleNamespace(running=True)
        client_manager = SimpleNamespace(get_client_count=lambda: 1)
        server = SimpleNamespace(
            worker_id="native-owner",
            stream_managers={"worker": manager},
            client_managers={"worker": client_manager},
        )

        class Keys:
            channel_owner = staticmethod(lambda _worker_id: "owner")
            channel_metadata = staticmethod(lambda _worker_id: "metadata")
            clients = staticmethod(lambda _worker_id: "clients")

        class Fields:
            STATE = "state"

        class States:
            ACTIVE = "ACTIVE"

        def media_chunks():
            while True:
                redis.reads += 1
                yield b"x" * 188

        with self.assertRaisesRegex(RuntimeError, "did not reach ACTIVE"):
            probe._wait_for_native_active(
                redis,
                server,
                "worker",
                redis_keys=Keys,
                metadata_field=Fields,
                channel_state=States,
                timeout=0.03,
            )

    def test_fresh_buffer_chunk_proof_skips_prefetched_equal_bytes(self):
        old_chunk = bytes(bytearray(b"same-payload"))
        new_chunk = bytes(bytearray(b"same-payload"))
        self.assertEqual(old_chunk, new_chunk)
        self.assertIsNot(old_chunk, new_chunk)
        buffer = _FakeBuffer([old_chunk, new_chunk], next_index=12)
        tracker = probe._NativeBufferYieldTracker(buffer)
        try:
            chunks, next_index = buffer.get_optimized_client_data(10)
            self.assertEqual(next_index, 12)
            iterator = tracker.observe(iter(chunks))
            publication_floor = 11
            fresh = probe._read_stream_iterator(
                iterator,
                minimum_bytes=len(new_chunk),
                timeout=1,
                accept_chunk=lambda: (
                    tracker.last_yielded_index is not None
                    and tracker.last_yielded_index > publication_floor
                ),
            )
            self.assertEqual(fresh, new_chunk)
            self.assertEqual(tracker.last_yielded_index, 12)
        finally:
            tracker.close()

    def test_unmapped_native_batch_cannot_claim_a_fresh_index(self):
        buffer = _FakeBuffer([b"one", b"two"], next_index=1)
        tracker = probe._NativeBufferYieldTracker(buffer)
        try:
            chunks, _next_index = buffer.get_optimized_client_data(0)
            iterator = tracker.observe(iter(chunks))
            self.assertEqual(next(iterator), b"one")
            self.assertIsNone(tracker.last_yielded_index)
            self.assertEqual(next(iterator), b"two")
            self.assertIsNone(tracker.last_yielded_index)
            self.assertEqual(tracker.unmapped_batches, 1)
        finally:
            tracker.close()

    def test_child_probe_is_compiled_and_capability_stays_off_command_line(self):
        secret_capability = "synthetic-private-capability"
        attempt = SimpleNamespace(
            capability=secret_capability,
            worker_id="synthetic-worker",
            config_generation="generation-1",
        )
        captured = {}

        def fake_run(args, **kwargs):
            captured["args"] = args
            captured.update(kwargs)
            return SimpleNamespace(
                returncode=0,
                stdout=b"CATCHUPARR_RECORDER_FOLLOWER_OK\n",
            )

        with patch.object(probe.subprocess, "run", side_effect=fake_run):
            probe._run_fresh_process_follower(
                attempt,
                channel_uuid="synthetic-channel",
                stream_id="synthetic-stream",
                account_id="synthetic-account",
                profile_id=7,
                expected_profile_count=3,
                ffprobe="ffprobe",
                native_owner="native-owner",
                parent_client_count=1,
            )

        child_script = captured["input"].decode("utf-8")
        compile(child_script, "<recorder-follower-probe>", "exec")
        self.assertIn("import os", child_script)
        self.assertIn("worker_id not in proxy_server.stream_managers", child_script)
        self.assertIn("ChannelState.ACTIVE", child_script)
        self.assertIn("int(redis_client.scard(clients_key) or 0) == 2", child_script)
        self.assertIn(secret_capability, child_script)
        self.assertNotIn(secret_capability, repr(captured["args"]))
        self.assertNotIn("env", captured)


if __name__ == "__main__":
    unittest.main()
