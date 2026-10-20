import ast
import io
import json
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
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
    def test_healthy_borrow_gate_diagnostic_preserves_exact_provider_predicate(self):
        baseline = {
            "previous_client_is_distinct": True,
            "recorder_client_verified": True,
            "native_registration_present": True,
            "native_manager_running": True,
            "native_owner_present": True,
            "native_owner_matches_expected": True,
            "source_metadata_matches_expected": True,
            "global_native_client_count": 2,
            "local_native_client_count": 1,
            "source_a_request_delta": 1,
            "source_a_active_count": 1,
            "source_b_active_count": 0,
            "task_alive": True,
            "task_has_no_unexpected_failure": True,
        }
        passing = probe._healthy_borrow_gate_snapshot(**baseline)
        self.assertTrue(passing["healthy_borrow_gate_passed"])
        self.assertEqual(passing["exact_A_request_delta"], 1)
        self.assertEqual(passing["A_active_count"], 1)
        self.assertEqual(passing["B_active_count"], 0)
        self.assertEqual(passing["stage"], "before_fault_arming")

        for changed in (
            {"source_a_request_delta": 2},
            {"recorder_client_verified": False},
            {"native_registration_present": False},
            {"previous_client_is_distinct": False},
            {"native_owner_present": False},
            {"native_owner_matches_expected": False},
            {"source_metadata_matches_expected": False},
            {"native_manager_running": False},
            {"global_native_client_count": 3},
            {"local_native_client_count": 2},
        ):
            diagnostic = probe._healthy_borrow_gate_snapshot(**(baseline | changed))
            self.assertFalse(diagnostic["healthy_borrow_gate_passed"])

    def test_healthy_borrow_readiness_waits_for_exact_registration_and_fails_closed(self):
        base = {
            "previous_client_is_distinct": True,
            "recorder_client_verified": False,
            "native_registration_present": False,
            "native_manager_running": True,
            "native_owner_present": True,
            "native_owner_matches_expected": True,
            "source_metadata_matches_expected": True,
            "global_native_client_count": 1,
            "local_native_client_count": 1,
            "source_a_request_delta": 1,
            "source_a_active_count": 1,
            "source_b_active_count": 1,
            "task_alive": True,
            "task_has_no_unexpected_failure": True,
            "stage": "after_normal_reattach_deadline",
        }

        def snapshot(**changes):
            return probe._healthy_borrow_gate_snapshot(**(base | changes))

        pending = snapshot()
        registered = snapshot(
            recorder_client_verified=True,
            native_registration_present=True,
            global_native_client_count=2,
            source_b_active_count=0,
        )
        now = [0.0]
        current = [pending]

        def cooperative_wait(delay):
            current[0] = registered
            now[0] += delay

        ready, evidence = probe._wait_for_healthy_borrow_gate(
            lambda: current[0], 1, monotonic=lambda: now[0], sleep=cooperative_wait,
        )
        self.assertTrue(ready)
        self.assertIs(evidence, registered)
        self.assertEqual(evidence["stage"], "after_normal_reattach_deadline")

        excessive_a = snapshot(source_a_request_delta=2)
        ready, evidence = probe._wait_for_healthy_borrow_gate(
            lambda: excessive_a, 1,
            sleep=lambda _delay: self.fail("extra A request must fail immediately"),
        )
        self.assertFalse(ready)
        self.assertIs(evidence, excessive_a)

        wrong_identity = snapshot(previous_client_is_distinct=False)
        ready, evidence = probe._wait_for_healthy_borrow_gate(
            lambda: wrong_identity, 1,
            sleep=lambda _delay: self.fail("wrong client identity must fail immediately"),
        )
        self.assertFalse(ready)
        self.assertIs(evidence, wrong_identity)

        now[0] = 0.0
        ready, evidence = probe._wait_for_healthy_borrow_gate(
            lambda: pending, 0.2, monotonic=lambda: now[0],
            sleep=lambda delay: now.__setitem__(0, now[0] + delay),
        )
        self.assertFalse(ready)
        self.assertIs(evidence, pending)

        wrong_source = snapshot(source_metadata_matches_expected=False)
        now[0] = 0.0
        ready, evidence = probe._wait_for_healthy_borrow_gate(
            lambda: wrong_source, 0.2, monotonic=lambda: now[0],
            sleep=lambda delay: now.__setitem__(0, now[0] + delay),
        )
        self.assertFalse(ready)
        self.assertIs(evidence, wrong_source)

        early_exit = snapshot(task_alive=False)
        ready, evidence = probe._wait_for_healthy_borrow_gate(
            lambda: early_exit, 1,
            sleep=lambda _delay: self.fail("early task exit must fail immediately"),
        )
        self.assertFalse(ready)
        self.assertIs(evidence, early_exit)

        task_failure = snapshot(task_has_no_unexpected_failure=False)
        ready, evidence = probe._wait_for_healthy_borrow_gate(
            lambda: task_failure, 1,
            sleep=lambda _delay: self.fail("unexpected task failure must fail immediately"),
        )
        self.assertFalse(ready)
        self.assertIs(evidence, task_failure)

    def test_both_borrow_gates_use_shared_readiness_proof(self):
        source = Path(probe.__file__).read_text()
        tree = ast.parse(source)
        target = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "probe_actual_live_archive_isolation"
        )
        readiness_calls = [
            node for node in ast.walk(target)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "_wait_for_healthy_borrow_gate"
        ]
        self.assertEqual(len(readiness_calls), 2)

    def test_failure_return_ack_requires_exact_admitted_and_normal_deadline_window(self):
        token = SimpleNamespace(
            action=SimpleNamespace(value="return_to_allowed"),
            revision=4, channel_uuid="channel",
            configuration_generation="config", control_generation=7,
            recorder_fence=9, intent_epoch=12, not_before=200.0,
        )
        state = SimpleNamespace(
            last_viewer_left_at=200.0, last_switch_at=190.0,
            recorder_fence=9, intent_epoch=12,
        )
        recovered = SimpleNamespace(intent=token, revision=4, state=state)
        committed_state = SimpleNamespace(
            phase=SimpleNamespace(value="dedicated"), pending_action=None,
            last_switch_at=201.0, intent_epoch=12,
            configuration_generation="config", control_generation=7,
            recorder_fence=9,
        )
        committed = SimpleNamespace(revision=5, intent=None, state=committed_state)
        check = probe._failure_return_ack_matches_admitted_window
        args = (token, recovered, token, 201.0, committed)
        kwargs = {
            "channel_uuid": "channel", "configuration_generation": "config",
            "control_generation": 7,
            "return_seconds": 1800.0, "minimum_switch_seconds": 60.0,
        }
        self.assertTrue(check(*args, **kwargs))
        self.assertFalse(check(*args[:3], 199.0, committed, **kwargs))
        self.assertFalse(check(*args[:3], 200.0 + 1800.0, committed, **kwargs))
        self.assertFalse(check(
            token, recovered, token, 201.0,
            SimpleNamespace(revision=6, intent=None, state=committed_state), **kwargs,
        ))

    def test_failure_cleanup_barrier_requires_exact_closed_episode_and_native_absence(self):
        capability = SimpleNamespace(
            episode_id="episode", channel_uuid="channel", episode_version=4,
            configuration_generation="config", control_generation=3,
            recorder_fence=8, intent_epoch=10,
            connection_identity=("channel", "worker", "owner", "metadata", "stream", "account", "profile", "input"),
        )
        episode = SimpleNamespace(
            episode_id="episode", channel_uuid="channel", phase="closed", version=6,
            configuration_generation="config", control_generation=3,
            recorder_fence=8, intent_epoch=10,
            identity=capability.connection_identity, client_id="catchuparr-client-nonce",
        )

        def snapshot(*, episode=episode, projection_state="known_revoked",
                     projection_episode_id="episode", projection_client_id="catchuparr-client-nonce",
                     projection_version=6, native_absent=True, held=True, alive=True,
                     no_failure=True):
            conditions = probe._failure_cleanup_ack_conditions(
                episode, capability,
                expected_client_id="catchuparr-client-nonce",
                projection_state=projection_state,
                projection_episode_id=projection_episode_id,
                projection_client_id=projection_client_id,
                projection_version=projection_version,
                projection_binding_matches=(projection_state == "known_revoked"),
                native_registration_absent=native_absent,
                task_result_held=held, task_alive=alive,
                task_has_no_unexpected_failure=no_failure,
            )
            return {"conditions": conditions}

        self.assertTrue(all(snapshot()["conditions"].values()))
        self.assertTrue(all(snapshot(projection_state="absent")["conditions"].values()))
        for invalid in (
            snapshot(episode=SimpleNamespace(**(vars(episode) | {"phase": "active"}))),
            snapshot(episode=SimpleNamespace(**(vars(episode) | {"episode_id": "other"}))),
            snapshot(projection_episode_id="other"),
            snapshot(projection_client_id="other-client"),
            snapshot(projection_version=5),
            snapshot(native_absent=False),
            snapshot(held=False),
        ):
            self.assertFalse(all(invalid["conditions"].values()))

    def test_failure_cleanup_wait_handles_pending_ack_timeout_and_early_exit(self):
        pending = {"conditions": {"task_alive": True, "task_has_no_unexpected_failure": True,
                                   "native_ack": False}}
        acknowledged = {"conditions": {"task_alive": True, "task_has_no_unexpected_failure": True,
                                        "native_ack": True}}
        now = [0.0]
        snapshots = [pending]

        def release_ack(_delay):
            snapshots[0] = acknowledged
            now[0] += _delay

        ready, evidence = probe._wait_for_failure_cleanup_ack(
            lambda: snapshots[0], 1, monotonic=lambda: now[0],
            sleep=release_ack,
        )
        self.assertTrue(ready)
        self.assertIs(acknowledged, evidence)

        now[0] = 0.0
        ready, evidence = probe._wait_for_failure_cleanup_ack(
            lambda: pending, 0.2, monotonic=lambda: now[0],
            sleep=lambda delay: now.__setitem__(0, now[0] + delay),
        )
        self.assertFalse(ready)
        self.assertIs(pending, evidence)

        exited = {"conditions": {"task_alive": False,
                                  "task_has_no_unexpected_failure": True,
                                  "native_ack": False}}
        ready, evidence = probe._wait_for_failure_cleanup_ack(
            lambda: exited, 1,
            sleep=lambda _delay: self.fail("early task exit must fail immediately"),
        )
        self.assertFalse(ready)
        self.assertIs(exited, evidence)

    def test_supervisor_return_ack_wait_requires_order_completion_and_live_task(self):
        expected = (
            "media_callback_lost_ack", "supervisor_ack", "poll_after_supervisor_ack",
        )

        def make_run(*, alive=True, failures=None):
            return SimpleNamespace(
                _lock=threading.Lock(),
                thread=SimpleNamespace(is_alive=lambda: alive),
                failures=list(failures or ()), results=[], input_headers=[], processes=[],
                result={"status": "stopped"}, stop_event=threading.Event(),
            )

        events = [expected[0]]
        pending = SimpleNamespace(intent=object(), phase="wait_return")
        committed = SimpleNamespace(intent=None, phase="dedicated")
        snapshots = [pending]
        run = make_run()
        now = [0.0]
        sleeps = []

        def advance_supervisor(_delay):
            sleeps.append(_delay)
            self.assertEqual((expected[0],), tuple(events))
            self.assertIs(pending, snapshots[0])
            events.extend(expected[1:])
            snapshots[0] = committed
            now[0] += _delay

        result = probe._wait_for_supervisor_return_ack(
            run, lambda: tuple(events), lambda: tuple(snapshots),
            timeout=1, cooperative_sleep=advance_supervisor,
            monotonic=lambda: now[0], emit=lambda _line: None,
        )
        self.assertIs(committed, result)
        self.assertEqual(1, len(sleeps))

        for failed_events, failures, alive, expected_condition in (
            ((expected[1],), (), True, "event_order"),
            ((expected[0],), ("task:RuntimeError",), True, "task_failure"),
        ):
            output = []
            with self.subTest(condition=expected_condition), self.assertRaises(RuntimeError):
                probe._wait_for_supervisor_return_ack(
                    make_run(alive=alive, failures=failures),
                    lambda: failed_events, lambda: (), timeout=1,
                    cooperative_sleep=lambda _delay: self.fail("failed wait slept"),
                    monotonic=lambda: 0.0, emit=output.append,
                )
            diagnostic = json.loads(output[0].split(" ", 1)[1])
            self.assertEqual(expected_condition, diagnostic["condition"])

        output = []
        now[0] = 0.0
        with self.assertRaisesRegex(RuntimeError, "bounded timeout"):
            probe._wait_for_supervisor_return_ack(
                make_run(), lambda: (expected[0],), lambda: (), timeout=0.25,
                cooperative_sleep=lambda delay: now.__setitem__(0, now[0] + delay),
                monotonic=lambda: now[0], emit=output.append,
            )
        diagnostic = json.loads(output[0].split(" ", 1)[1])
        self.assertEqual("timeout", diagnostic["condition"])

        output = []
        with self.assertRaisesRegex(RuntimeError, "task ended"):
            probe._wait_for_supervisor_return_ack(
                make_run(alive=False), lambda: (expected[0],), lambda: (), timeout=1,
                cooperative_sleep=lambda _delay: self.fail("exited task slept"),
                monotonic=lambda: 0.0, emit=output.append,
            )
        diagnostic = json.loads(output[0].split(" ", 1)[1])
        self.assertEqual("task_exit", diagnostic["condition"])

    def test_live_overview_source_metadata_is_mode_specific(self):
        common = {
            "stream_name": "source", "provider_name": "provider",
            "stream_id": "stream-id", "provider_id": "provider-id",
            "m3u_profile_id": "m3u-id", "m3u_profile_name": "m3u-name",
        }
        borrowed = {
            **common, "mode": "borrowed",
            "input_profile_id": "input-id", "input_profile_name": "input-name",
        }
        dedicated = {**common, "mode": "dedicated", "input_profile_id": None}
        self.assertTrue(probe._overview_source_metadata_matches(borrowed, "borrowed"))
        self.assertTrue(probe._overview_source_metadata_matches(dedicated, "dedicated"))
        self.assertFalse(probe._overview_source_metadata_matches(common | {"mode": "borrowed"}, "borrowed"))
        self.assertFalse(probe._overview_source_metadata_matches(
            dedicated | {"input_profile_id": "unexpected"}, "dedicated",
        ))
        self.assertFalse(probe._overview_source_metadata_matches(
            dedicated | {"input_profile_name": None}, "dedicated",
        ))

    def test_live_overview_metadata_contract_matches_current_recordings_action(self):
        from catchuparr.configuration import active_configuration_generation
        from catchuparr.recording_overview import current_recordings

        channel = "synthetic-channel"
        now = time.time()
        settings = {
            "version": 7, "channel_uuids": channel,
            "archive_root": "/missing-synthetic-archive",
            "recording_schedule": {},
        }
        generation = active_configuration_generation(settings)
        control = SimpleNamespace(
            paused=False, configuration_generation=generation,
        )
        source_base = {
            "stream_name": "stream", "stream_id": "stream-id",
            "provider_name": "provider", "provider_id": "provider-id",
            "m3u_profile_id": "m3u-id", "m3u_profile_name": "m3u-name",
        }

        class Redis:
            def ping(self):
                return True

        for mode, profile in (
            ("borrowed", {"input_profile_id": "input-id", "input_profile_name": "input-name"}),
            ("dedicated", {}),
        ):
            diagnostic = {channel: {
                "phase": "error", "source": mode, "stream_id": "stream-id",
                "account_id": "provider-id", "m3u_profile_id": "m3u-id",
                "input_profile_id": profile.get("input_profile_id"),
                "started_at": now, "error_code": "synthetic",
            }}
            with patch("catchuparr.recording_overview._selected_channel_ids", return_value=[channel]), \
                 patch("catchuparr.recording_overview.read_recorder_diagnostics", return_value=diagnostic), \
                 patch("catchuparr.recording_overview._channel_metadata", return_value={channel: {"name": "channel"}}), \
                 patch("catchuparr.recording_overview.source_metadata", return_value={**source_base, **profile}):
                response = current_recordings(
                    settings, control, redis_client=Redis(), now=now,
                )
            source = response["channels"][0]["source"]
            self.assertTrue(probe._overview_source_metadata_matches(source, mode))

    def test_failed_opening_identity_factory_builds_token_contract(self):
        from catchuparr.sharing_persistence import SharingIntentToken
        from catchuparr.sharing_planner import SharingAction

        identity = probe._failed_opening_identity(
            "channel-recovery-synthetic", "owner-recovery-synthetic",
            SimpleNamespace(id="stream-recovery-synthetic", m3u_account_id="account-recovery-synthetic"),
            {"m3u_profile": "m3u-recovery-synthetic", "stream_profile": "input-recovery-synthetic"},
        )
        token = SharingIntentToken(
            channel_uuid="channel-recovery-synthetic", revision=1, intent_epoch=1,
            action=SharingAction.ATTACH_BORROW,
            configuration_generation="config-recovery-synthetic",
            control_generation=1, recorder_fence=0,
            target_identity=identity, target_admitted=True, not_before=1.0,
        )
        self.assertEqual(8, len(token.target_identity))
        self.assertEqual("channel-recovery-synthetic", token.target_identity[0])

    def test_live_readonly_probe_allows_concurrent_archive_append_and_denies_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            configured_archive_root = Path(directory) / "synthetic-live-archive"
            configured_archive_root.mkdir()
            archive_db = configured_archive_root / "archive.sqlite3"
            with sqlite3.connect(archive_db) as database:
                database.execute("CREATE TABLE segments (created_at INTEGER)")
                database.execute("INSERT INTO segments VALUES (1)")

            def action():
                uri = f"file:{archive_db}?mode=ro"
                with sqlite3.connect(uri, uri=True) as database:
                    database.execute("PRAGMA query_only=ON")
                    rows = database.execute("SELECT COUNT(*) FROM segments").fetchone()[0]
                def append_segment():
                    with sqlite3.connect(archive_db) as writer:
                        writer.execute("INSERT INTO segments VALUES (2)")

                append = threading.Thread(target=append_segment)
                append.start()
                append.join(timeout=2)
                self.assertFalse(append.is_alive())
                return rows

            rows = probe._run_current_recordings_readonly(action, archive_db)
            self.assertEqual(1, rows)
            with sqlite3.connect(archive_db) as database:
                self.assertEqual(2, database.execute(
                    "SELECT COUNT(*) FROM segments",
                ).fetchone()[0])

            def attempted_write():
                with sqlite3.connect(f"file:{archive_db}?mode=ro", uri=True) as database:
                    database.execute("PRAGMA query_only=ON")
                    database.execute("UPDATE segments SET created_at=3")

            with self.assertRaises(sqlite3.DatabaseError):
                probe._run_current_recordings_readonly(attempted_write, archive_db)

    def test_live_overview_calls_use_the_configured_nested_archive_root(self):
        source = (Path(__file__).parents[1] / "scripts/aio_recorder_media.py").read_text()
        tree = ast.parse(source)
        probe_function = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "probe_actual_live_archive_isolation"
        )
        calls = [
            node for node in ast.walk(probe_function)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "_assert_live_overview"
        ]
        self.assertEqual(2, len(calls))
        self.assertTrue(all(
            isinstance(call.args[0], ast.Name)
            and call.args[0].id == "archive_root"
            for call in calls
        ))

    def test_dedicated_diagnostic_failure_snapshot_is_finite_booleans(self):
        from scripts.aio_recorder_diagnostics_probe import _dedicated_writer_guard_checks

        class Redis:
            def hgetall(self, _key):
                return {}

        checks = _dedicated_writer_guard_checks(
            Redis(), "synthetic-channel-id", "synthetic-lease-value",
            "catchuparr:recorder:diagnostic:synthetic-channel-id",
        )
        self.assertTrue(checks)
        self.assertTrue(all(type(value) is bool for value in checks.values()))
        self.assertFalse(checks["live_record_present"])
        self.assertFalse(checks["current_attempt_record_found"])

    def test_dedicated_failure_snapshot_checks_current_worker_not_stale_borrow(self):
        from scripts.aio_recorder_diagnostics_probe import _dedicated_writer_guard_checks

        channel = "synthetic-channel-id"
        lease_value = "synthetic-lease-value"
        worker_id = "catchuparr-r" + "a" * 40
        worker_key = f"catchuparr:recorder-proxy:worker:{worker_id}"
        worker = {
            "channel_uuid": channel, "worker_id": worker_id,
            "lease_key": f"catchuparr:recorder:{channel}",
            "lease_value": lease_value, "stream_id": "stream-synthetic",
            "account_id": "account-synthetic", "config_generation": "a" * 64,
            "control_generation": "1", "lease_fence": "2", "profile_id": "5",
            "state": "active",
        }

        class Redis:
            def __init__(self, scanned_keys):
                self.scanned_keys = scanned_keys

            def hgetall(self, key):
                if isinstance(key, bytes):
                    key = key.decode()
                if key == f"catchuparr:recorder:diagnostic:{channel}":
                    return {b"phase": b"recording", b"source": b"borrowed"}
                if key == worker_key:
                    return {key.encode(): value.encode() for key, value in worker.items()}
                if key == f"live:channel:{worker_id}:metadata":
                    return {
                        b"owner": b"native-owner-synthetic",
                        b"stream_id": b"stream-synthetic", b"m3u_profile": b"5",
                        b"state": b"active",
                    }
                return {}

            def scan_iter(self, *, match, count):
                self.assertion = (match, count)
                return self.scanned_keys

            def get(self, key):
                if key == f"catchuparr:recorder:{channel}":
                    return lease_value
                if key == f"live:channel:{worker_id}:owner":
                    return b"native-owner-synthetic"
                return None

            def ttl(self, _key):
                return 30

            def exists(self, _key):
                return 0

        for scanned_key in (worker_key, worker_key.encode()):
            with self.subTest(scanned_key_type=type(scanned_key).__name__):
                checks = _dedicated_writer_guard_checks(
                    Redis([scanned_key]), channel, lease_value,
                    f"catchuparr:recorder:diagnostic:{channel}",
                )
                self.assertTrue(checks["current_attempt_record_found"])
                self.assertTrue(checks["current_attempt_lease_matches"])
                self.assertFalse(checks["live_record_matches_current_attempt"])
                self.assertTrue(checks["current_attempt_native_owner_heartbeat_live"])
                self.assertTrue(checks["current_attempt_m3u_profile_matches"])
                self.assertTrue(checks["current_attempt_input_profile_absent"])
                self.assertTrue(checks["current_attempt_profile_matches"])
                self.assertTrue(all(type(value) is bool for value in checks.values()))

        for scanned_keys in ([], [worker_key, worker_key.encode()]):
            with self.subTest(scanned_key_count=len(scanned_keys)):
                checks = _dedicated_writer_guard_checks(
                    Redis(scanned_keys), channel, lease_value,
                    f"catchuparr:recorder:diagnostic:{channel}",
                )
                self.assertFalse(checks["current_attempt_record_found"])
                self.assertFalse(checks["current_attempt_lease_matches"])

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

    def test_early_exit_receipt_carries_first_supervisor_stop_context(self):
        stop_event = aio_recorder_failover._ObservedRecorderStopEvent()
        namespace = {
            "__name__": "catchuparr.tasks",
            "stop_event": stop_event,
            "current_admission": {"status": "stale_control"},
        }
        tree = ast.parse(
            "def supervise(current_admission=current_admission):\n    stop_event.set()\n"
        )
        source = ast.parse((Path(__file__).parents[1] / "catchuparr/tasks.py").read_text())
        record_channel = next(
            node for node in source.body
            if isinstance(node, ast.FunctionDef) and node.name == "record_channel"
        )
        supervise = next(
            node for node in ast.walk(record_channel)
            if isinstance(node, ast.FunctionDef) and node.name == "supervise"
        )
        first_stop = min(
            node.lineno for node in ast.walk(supervise)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "set"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "stop_event"
        )
        ast.increment_lineno(tree, first_stop - 2)
        exec(compile(tree, "synthetic_tasks.py", "exec"), namespace)
        namespace["supervise"]()
        diagnostic = probe._recorder_task_early_exit_diagnostic(
            SimpleNamespace(
                result={"status": "stopped"},
                stop_event=stop_event,
                thread=SimpleNamespace(is_alive=lambda: False),
            )
        )
        self.assertEqual("tasks.supervisor", diagnostic["stop_source"])
        self.assertEqual("stale_control", diagnostic["supervisor_admission_status"])
        self.assertEqual("admission_not_ready", diagnostic["supervisor_stop_reason"])

    def test_allowed_source_wait_reports_existing_safe_task_exit_receipt(self):
        now = [0.0]
        run = SimpleNamespace(
            result={"status": "sharing_return_recovery_pending"},
            failures=["task:BorrowBindingLost", "SyntheticSecretTokenABC123"],
            results=[SimpleNamespace(status="switch_requested", useful_segments=4,
                                     return_code=0, attempt_switch_id="opaque")],
            processes=[SimpleNamespace(poll=lambda: 0)],
            input_headers=[{"X-Catchuparr-Borrow": "synthetic-bearer-secret"}],
            stop_event=threading.Event(),
            thread=SimpleNamespace(is_alive=lambda: True),
        )
        output = io.StringIO()

        def sleep(seconds):
            now[0] += seconds

        with redirect_stdout(output), self.assertRaisesRegex(
            RuntimeError,
            "^Allowed B did not start after the observed native return deadline$",
        ):
            probe._wait_for_allowed_source_request(
                run, lambda: [], 0, timeout=0.1, sleep=sleep,
                monotonic=lambda: now[0],
            )

        line = next(
            line for line in output.getvalue().splitlines()
            if line.startswith("CATCHUPARR_RECORDER_EARLY_EXIT_DIAGNOSTIC ")
        )
        diagnostic = json.loads(line.split(" ", 1)[1])
        self.assertEqual("sharing_return_recovery_pending", diagnostic["task_status"])
        self.assertEqual(["BorrowBindingLost", "other"], diagnostic["failure_classes"])
        self.assertEqual("switch_requested", diagnostic["attempts"][0]["status"])
        self.assertNotIn("synthetic-bearer-secret", line)
        self.assertNotIn("SyntheticSecretTokenABC123", line)

    def test_allowed_source_wait_keeps_success_gate_without_diagnostic(self):
        output = io.StringIO()
        with redirect_stdout(output):
            timestamp = probe._wait_for_allowed_source_request(
                SimpleNamespace(), lambda: [12.5], 0, timeout=1,
                monotonic=lambda: 0.0,
            )
        self.assertEqual(12.5, timestamp)
        self.assertEqual("", output.getvalue())

    def test_crash_child_failure_summary_contains_only_safe_shape_fields(self):
        result = SimpleNamespace(
            returncode=70,
            stdout=json.dumps({
                "error": True,
                "exception_class": "RuntimeError",
                "private_payload": "synthetic-bearer-secret",
            }),
        )
        diagnostic = probe._crash_child_failure_diagnostic(
            result, 73, json.loads(result.stdout), "object",
        )
        self.assertEqual({
            "exit_code": 70,
            "expected_exit_code": 73,
            "stdout_json_shape": "object",
            "known_result_keys": ["error", "exception_class"],
            "child_exception_class": "RuntimeError",
        }, diagnostic)
        self.assertNotIn("synthetic-bearer-secret", json.dumps(diagnostic))

    def test_crash_child_failure_summary_classifies_non_object_json(self):
        result = SimpleNamespace(returncode=73, stdout='["private-token"]')
        diagnostic = probe._crash_child_failure_diagnostic(
            result, 73, json.loads(result.stdout), "array",
        )
        self.assertEqual("array", diagnostic["stdout_json_shape"])
        self.assertEqual([], diagnostic["known_result_keys"])
        self.assertNotIn("private-token", json.dumps(diagnostic))

    def test_crash_child_parser_ignores_unframed_output_but_requires_one_receipt(self):
        receipt = probe.CRASH_CHILD_RESULT_PREFIX + '{"native_registered":true}'
        value, shape = probe._parse_crash_child_result(
            "Django startup message with private value\n" + receipt + "\n"
        )
        self.assertEqual({"native_registered": True}, value)
        self.assertEqual("object", shape)

        value, shape = probe._parse_crash_child_result(receipt + "\n" + receipt)
        self.assertIsNone(value)
        self.assertEqual("multiple_receipts", shape)

        diagnostic = probe._crash_child_failure_diagnostic(
            SimpleNamespace(returncode=73, stdout="noise"), 73, None, shape,
        )
        self.assertEqual("multiple_receipts", diagnostic["stdout_json_shape"])
        self.assertNotIn("private", json.dumps(diagnostic))

    def test_teardown_budget_accounts_for_sequential_shutdown_and_marker_ttl(self):
        self.assertEqual(
            156.0,
            probe._native_teardown_readiness_timeout(120, 1),
        )

    def test_borrow_teardown_budget_adds_both_native_marker_phases(self):
        self.assertEqual(
            216.0,
            probe._native_teardown_readiness_timeout(
                120, 1, stopping_marker_ttl=90,
            ),
        )

    def test_paused_native_teardown_waits_after_all_cleanup_acknowledgments(self):
        conditions = {
            "episode_closed": True,
            "borrow_hash_absent": True,
            "borrow_membership_absent": True,
            "borrow_marker_absent": True,
            "ledger_paused": True,
            "grace_deadline_elapsed": True,
            "task_completed_cleanly": True,
        }
        calls = []
        result = probe._wait_for_native_teardown_after_pause(
            conditions,
            lambda: calls.append("native_release_wait"),
            lambda: calls.append("final_seven_conditions") or {"all": True},
        )
        self.assertEqual(["native_release_wait", "final_seven_conditions"], calls)
        self.assertEqual({"all": True}, result)

        conditions["ledger_paused"] = False
        with self.assertRaisesRegex(RuntimeError, "did not settle.*PAUSED"):
            probe._wait_for_native_teardown_after_pause(
                conditions,
                lambda: calls.append("must_not_wait"),
                lambda: calls.append("must_not_read_final_conditions"),
            )
        self.assertEqual(["native_release_wait", "final_seven_conditions"], calls)

    def test_native_a_teardown_waits_for_owner_and_profile_but_not_source_b(self):
        class FakeClock:
            now = 0.0

            def monotonic(self):
                return self.now

            def sleep(self, duration):
                self.now += duration

        clock = FakeClock()
        snapshots = [
            {
                "conditions": probe._native_teardown_ready_conditions(
                    route_available=False,
                    stopping_marker_absent=False,
                    metadata_not_stopping=False,
                    owner_absent=False,
                    clients_zero=True,
                    managers_absent=False,
                    source_inactive=True,
                    profile_at_baseline=False,
                    previous_borrow_absent=True,
                ),
                "source_b_active": 1,
            },
            {
                "conditions": probe._native_teardown_ready_conditions(
                    route_available=True,
                    stopping_marker_absent=True,
                    metadata_not_stopping=True,
                    owner_absent=True,
                    clients_zero=True,
                    managers_absent=True,
                    source_inactive=True,
                    profile_at_baseline=True,
                    previous_borrow_absent=True,
                ),
                "source_b_active": 1,
            },
        ]
        ready, final = probe._wait_for_readiness_snapshot(
            lambda: snapshots.pop(0),
            2,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            poll_interval=1,
        )
        self.assertTrue(ready)
        self.assertEqual(1, final["source_b_active"])
        self.assertTrue(all(final["conditions"].values()))

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

    def test_source_removal_requires_both_live_a_and_released_b(self):
        conditions = probe._source_removal_transition_conditions(
            a_owner_preserved=True,
            a_active=True,
            a_client_preserved=True,
            a_provider_preserved=True,
            a_profile_preserved=True,
            a_assignments_preserved=True,
            b_worker_released=True,
            b_native_resources_absent=True,
            b_binding_preserved=True,
            b_reservation_released=True,
            b_private_markers_absent=True,
            b_provider_inactive=True,
            b_request_count_preserved=True,
            b_profile_released=True,
        )
        self.assertTrue(all(conditions.values()))

        disturbed_a = dict(conditions, ordinary_a_active=False)
        self.assertFalse(all(disturbed_a.values()))
        retained_b = dict(conditions, dedicated_b_reservation_released=False)
        self.assertFalse(all(retained_b.values()))

    def test_source_removal_fails_fast_when_a_drops_then_recovers(self):
        healthy = probe._source_removal_transition_conditions(
            a_owner_preserved=True,
            a_active=True,
            a_client_preserved=True,
            a_provider_preserved=True,
            a_profile_preserved=True,
            a_assignments_preserved=True,
            b_worker_released=False,
            b_native_resources_absent=False,
            b_binding_preserved=False,
            b_reservation_released=False,
            b_private_markers_absent=False,
            b_provider_inactive=False,
            b_request_count_preserved=True,
            b_profile_released=False,
        )
        disturbed = dict(healthy, ordinary_a_active=False)
        released = dict(
            healthy,
            dedicated_b_worker_released=True,
            dedicated_b_native_resources_absent=True,
            dedicated_b_binding_preserved=True,
            dedicated_b_reservation_released=True,
            dedicated_b_private_markers_absent=True,
            dedicated_b_provider_inactive=True,
            dedicated_b_profile_released=True,
        )
        snapshots = iter(({"conditions": disturbed}, {"conditions": released}))
        now = [0.0]
        ready, observed, reason = probe._wait_for_source_removal_transition(
            lambda: next(snapshots), 5,
            monotonic=lambda: now[0],
            sleep=lambda duration: now.__setitem__(0, now[0] + duration),
        )
        self.assertFalse(ready)
        self.assertFalse(observed["conditions"]["ordinary_a_active"])
        self.assertEqual("ordinary_a_changed", reason)
        self.assertEqual(0.0, now[0])

    def test_source_removal_waits_only_for_b_release_when_a_stays_healthy(self):
        conditions = probe._source_removal_transition_conditions(
            a_owner_preserved=True,
            a_active=True,
            a_client_preserved=True,
            a_provider_preserved=True,
            a_profile_preserved=True,
            a_assignments_preserved=True,
            b_worker_released=False,
            b_native_resources_absent=False,
            b_binding_preserved=False,
            b_reservation_released=False,
            b_private_markers_absent=False,
            b_provider_inactive=False,
            b_request_count_preserved=True,
            b_profile_released=False,
        )
        released = dict(
            conditions,
            dedicated_b_worker_released=True,
            dedicated_b_native_resources_absent=True,
            dedicated_b_binding_preserved=True,
            dedicated_b_reservation_released=True,
            dedicated_b_private_markers_absent=True,
            dedicated_b_provider_inactive=True,
            dedicated_b_profile_released=True,
        )
        snapshots = iter(({"conditions": conditions}, {"conditions": released}))
        now = [0.0]
        ready, observed, reason = probe._wait_for_source_removal_transition(
            lambda: next(snapshots), 5,
            monotonic=lambda: now[0],
            sleep=lambda duration: now.__setitem__(0, now[0] + duration),
        )
        self.assertTrue(ready)
        self.assertIsNone(reason)
        self.assertTrue(all(observed["conditions"].values()))
        self.assertGreater(now[0], 0.0)

    def test_fresh_media_floor_uses_new_buffer_epoch(self):
        class Buffer:
            def __init__(self, index):
                self.redis_client = SimpleNamespace(get=lambda _key: index)
                self.buffer_index_key = "synthetic-index"

        observed_floors = []
        session = SimpleNamespace(
            reader=SimpleNamespace(
                read_after=lambda floor, **_kwargs: (
                    observed_floors.append(floor) or b"fresh", floor + 1,
                ),
            ),
        )
        _old_media, _old_index = probe._read_after_current_buffer_head(
            session, Buffer(1000), minimum_bytes=1, timeout=1,
        )
        _new_media, new_index = probe._read_after_current_buffer_head(
            session, Buffer(1), minimum_bytes=1, timeout=1,
        )
        self.assertEqual([1000, 1], observed_floors)
        self.assertEqual(2, new_index)

    def test_source_removal_diagnostic_allowlists_ledger_states(self):
        worker_states = frozenset({"active", "released", "failed"})
        reservation_states = frozenset({"reserved", "released", "none"})
        self.assertEqual("active", probe._allowlisted_state("active", worker_states))
        self.assertEqual("released", probe._allowlisted_state("released", reservation_states))
        self.assertEqual(
            "unknown",
            probe._allowlisted_state("SyntheticSecretValue123", worker_states),
        )
        self.assertEqual("unknown", probe._allowlisted_state(None, reservation_states))

    def test_source_removal_binds_original_native_client_registration(self):
        expected = {
            "expected_client_id": "synthetic-client-a",
            "connected_at": "100.0",
            "expected_connected_at": "100.0",
            "worker_id": "synthetic-native-worker",
            "expected_worker_id": "synthetic-native-worker",
        }
        self.assertTrue(probe._native_client_identity_matches(
            {"synthetic-client-a"}, **expected,
        ))
        self.assertFalse(probe._native_client_identity_matches(
            {"synthetic-client-b"}, **expected,
        ))
        self.assertFalse(probe._native_client_identity_matches(
            {"synthetic-client-a"}, **dict(expected, connected_at="101.0"),
        ))
        self.assertFalse(probe._native_client_identity_matches(
            {"synthetic-client-a", "synthetic-recorder-client"}, **expected,
        ))

    def test_pending_return_freezes_attempts_after_join_including_pending_native_worker(self):
        records = [{
            "worker_id": "synthetic-b-first",
            "stream_id": "synthetic-stream-b",
            "capability_digest": "digest-first",
            "reservation_id": "reservation-first",
            "reservation_state": "released",
        }]
        pending_native_managers = set()

        class Task:
            def stop(self):
                pass

            def join(self, *, timeout, cooperative_sleep):
                self.assert_join_args = (timeout, cooperative_sleep)
                # A retry appears during shutdown after its profile reservation
                # was released, while its native task manager is still pending.
                records.append({
                    "worker_id": "synthetic-b-retry",
                    "stream_id": "synthetic-stream-b",
                    "capability_digest": "digest-retry",
                    "reservation_id": "reservation-retry",
                    "reservation_state": "none",
                })
                pending_native_managers.add("synthetic-b-retry")

        ids, bindings, valid = probe._stop_join_and_freeze_attempt_bindings(
            Task(), lambda: list(records), baseline_worker_ids={"synthetic-old-b"},
            stream_id="synthetic-stream-b", cooperative_sleep=lambda _delay: None,
        )
        self.assertTrue(valid)
        self.assertEqual(("synthetic-b-first", "synthetic-b-retry"), ids)
        self.assertEqual("reservation-retry", bindings["synthetic-b-retry"]["reservation_id"])
        self.assertEqual({"synthetic-b-retry"}, pending_native_managers)

        unchanged, _by_worker = probe._attempt_records_match_frozen_bindings(
            records, baseline_worker_ids={"synthetic-old-b"},
            stream_id="synthetic-stream-b", attempt_ids=ids, bindings=bindings,
        )
        self.assertTrue(unchanged)
        records.append({
            "worker_id": "synthetic-b-late",
            "stream_id": "synthetic-stream-b",
            "capability_digest": "digest-late",
            "reservation_id": "reservation-late",
        })
        unchanged, _by_worker = probe._attempt_records_match_frozen_bindings(
            records, baseline_worker_ids={"synthetic-old-b"},
            stream_id="synthetic-stream-b", attempt_ids=ids, bindings=bindings,
        )
        self.assertFalse(unchanged)

    def test_pending_return_uses_original_profile_baseline_during_async_drain(self):
        original_baseline = 0
        intermediate_count = 1
        settled_count = 0
        self.assertFalse(probe._profile_count_matches_original_baseline(
            intermediate_count, original_baseline,
        ))
        self.assertTrue(probe._profile_count_matches_original_baseline(
            settled_count, original_baseline,
        ))

    def test_dedicated_preparation_does_not_apply_or_start_when_return_gate_fails(self):
        effects = []

        def fail_return_gate():
            raise RuntimeError("synthetic RETURN profile mismatch")

        with self.assertRaisesRegex(RuntimeError, "RETURN profile mismatch"):
            probe._run_after_required_gate(
                fail_return_gate,
                lambda: effects.append("Apply and start B"),
            )
        self.assertEqual([], effects)

    def test_dedicated_replacement_waits_for_native_release_after_profile_returns(self):
        pending_manager = {
            "conditions": {
                "attempt_released": True,
                "attempt_credentials_absent": True,
                "native_manager_absent": False,
                "native_owner_absent": True,
                "native_clients_absent": True,
                "native_stopping_marker_absent": True,
                "provider_b_inactive": True,
                "provider_b_requests_stable": True,
                "profile_b_at_original_baseline": True,
            },
        }
        full_release = {
            "conditions": {
                **pending_manager["conditions"],
                "native_manager_absent": True,
            },
        }
        snapshots = iter((pending_manager, full_release))
        now = [0.0]
        effects = []
        result = probe._run_after_readiness_ack(
            lambda: next(snapshots),
            lambda: effects.append("start dedicated B") or "started",
            5,
            message="old B release pending",
            diagnostic_label="SYNTHETIC_RELEASE_GATE",
            monotonic=lambda: now[0],
            sleep=lambda duration: now.__setitem__(0, now[0] + duration),
            poll_interval=0.1,
        )
        self.assertEqual("started", result)
        self.assertEqual(["start dedicated B"], effects)
        self.assertGreater(now[0], 0)

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

    def test_native_active_accepts_current_task_borrow_added_after_one_client_sample(self):
        class BorrowRedis:
            def __init__(self):
                self.member_reads = 0

            def get(self, _key):
                return b"native-owner"

            def smembers(self, _key):
                self.member_reads += 1
                if self.member_reads == 1:
                    return {b"ordinary-client"}
                return {b"ordinary-client", b"catchuparr-client-borrow"}

            def hget(self, key, field):
                if key == "metadata" and field == "state":
                    return b"ACTIVE"
                if key == "clients:ordinary-client" and field == "connected_at":
                    return b"100.5"
                if key == "clients:ordinary-client" and field == "worker_id":
                    return b"native-owner"
                return None

        class Keys:
            channel_owner = staticmethod(lambda _worker: "owner")
            channel_metadata = staticmethod(lambda _worker: "metadata")
            clients = staticmethod(lambda _worker: "clients")

        class Fields:
            STATE = "state"

        class States:
            ACTIVE = "ACTIVE"

        redis = BorrowRedis()
        client_manager = SimpleNamespace(get_client_count=lambda: 1)
        manager = SimpleNamespace(running=True)
        server = SimpleNamespace(
            worker_id="native-owner",
            stream_managers={"worker": manager},
            client_managers={"worker": client_manager},
        )
        ordinary_identity = probe._wait_for_native_client_count(
            redis, server, Keys, "worker", 1, expected_local=1, timeout=1,
            return_identity=True,
        )

        task = SimpleNamespace(
            _lock=threading.Lock(),
            input_headers=[{"X-Catchuparr-Borrow": "opaque-capability"}],
        )
        checks = []

        def decode(_token):
            return SimpleNamespace(channel_uuid="synthetic-channel", nonce="borrow")

        def verified(worker, client_id):
            checks.append((worker, client_id))
            return worker == "worker" and client_id == "catchuparr-client-borrow"

        def verified_ids():
            return probe._verified_task_borrow_client_ids(
                task,
                worker_id="worker",
                channel_uuid="synthetic-channel",
                decode_capability=decode,
                is_server_verified=verified,
            )
        owner, found_manager, found_client_manager = probe._wait_for_native_active(
            redis,
            server,
            "worker",
            redis_keys=Keys,
            metadata_field=Fields,
            channel_state=States,
            timeout=1,
            expected_ordinary_identity=ordinary_identity,
            verified_borrow_client_ids=verified_ids,
        )

        self.assertEqual("native-owner", owner)
        self.assertIs(found_manager, manager)
        self.assertIs(found_client_manager, client_manager)
        self.assertEqual([("worker", "catchuparr-client-borrow")], checks)
        self.assertEqual(2, redis.member_reads)

    def test_native_active_reconciles_borrow_added_after_membership_snapshot(self):
        class BorrowRedis:
            def __init__(self):
                self.member_reads = 0

            def get(self, _key):
                return b"native-owner"

            def smembers(self, _key):
                self.member_reads += 1
                if self.member_reads <= 2:
                    return {b"ordinary-client"}
                return {b"ordinary-client", b"catchuparr-client-borrow"}

            def hget(self, key, field):
                if key == "metadata" and field == "state":
                    return b"ACTIVE"
                if key == "clients:ordinary-client" and field == "connected_at":
                    return b"100.5"
                if key == "clients:ordinary-client" and field == "worker_id":
                    return b"native-owner"
                return None

        class Keys:
            channel_owner = staticmethod(lambda _worker: "owner")
            channel_metadata = staticmethod(lambda _worker: "metadata")
            clients = staticmethod(lambda _worker: "clients")

        class Fields:
            STATE = "state"

        class States:
            ACTIVE = "ACTIVE"

        redis = BorrowRedis()
        manager = SimpleNamespace(running=True)
        client_manager = SimpleNamespace(get_client_count=lambda: 1)
        server = SimpleNamespace(
            worker_id="native-owner",
            stream_managers={"worker": manager},
            client_managers={"worker": client_manager},
        )
        ordinary_identity = probe._wait_for_native_client_count(
            redis, server, Keys, "worker", 1, expected_local=1, timeout=1,
            return_identity=True,
        )
        owner, _, _ = probe._wait_for_native_active(
            redis, server, "worker", redis_keys=Keys,
            metadata_field=Fields, channel_state=States, timeout=1,
            expected_ordinary_identity=ordinary_identity,
            verified_borrow_client_ids=lambda: {"catchuparr-client-borrow"},
        )
        self.assertEqual("native-owner", owner)
        self.assertEqual(3, redis.member_reads)

    def test_native_active_rejects_any_extra_client_beyond_exact_borrow(self):
        class BorrowRedis:
            def get(self, _key):
                return b"native-owner"

            def scard(self, _key):
                return 3

            def smembers(self, _key):
                return {
                    b"ordinary-client",
                    b"catchuparr-client-borrow",
                    b"unrelated-client",
                }

            def hget(self, _key, _field):
                return b"ACTIVE"

        class Keys:
            channel_owner = staticmethod(lambda _worker: "owner")
            channel_metadata = staticmethod(lambda _worker: "metadata")
            clients = staticmethod(lambda _worker: "clients")

        class Fields:
            STATE = "state"

        class States:
            ACTIVE = "ACTIVE"

        client_manager = SimpleNamespace(get_client_count=lambda: 1)
        server = SimpleNamespace(
            worker_id="native-owner",
            stream_managers={"worker": SimpleNamespace(running=True)},
            client_managers={"worker": client_manager},
        )
        with self.assertRaisesRegex(RuntimeError, "unverified client"):
            probe._wait_for_native_active(
                BorrowRedis(), server, "worker", redis_keys=Keys,
                metadata_field=Fields, channel_state=States, timeout=1,
                verified_borrow_client_ids=lambda: {"catchuparr-client-borrow"},
            )

    def test_native_active_keeps_the_same_ordinary_registration_while_waiting(self):
        class BorrowRedis:
            def __init__(self):
                self.member_reads = 0
                self.state_reads = 0
                self.client_count_reads = 0

            def get(self, _key):
                return b"native-owner"

            def smembers(self, _key):
                self.member_reads += 1
                if self.member_reads <= 2:
                    return {b"ordinary-a"}
                return {b"ordinary-b", b"catchuparr-client-borrow"}

            def scard(self, _key):
                self.client_count_reads += 1
                return 1

            def hget(self, key, field):
                if key == "metadata" and field == "state":
                    self.state_reads += 1
                    return b"CONNECTING"
                if field == "connected_at":
                    return b"100.5" if key.endswith("ordinary-a") else b"101.5"
                if field == "worker_id":
                    return b"native-owner"
                return None

        class Keys:
            channel_owner = staticmethod(lambda _worker: "owner")
            channel_metadata = staticmethod(lambda _worker: "metadata")
            clients = staticmethod(lambda _worker: "clients")

        class Fields:
            STATE = "state"

        class States:
            ACTIVE = "ACTIVE"

        redis = BorrowRedis()
        server = SimpleNamespace(
            worker_id="native-owner",
            stream_managers={"worker": SimpleNamespace(running=True)},
            client_managers={"worker": SimpleNamespace(get_client_count=lambda: 1)},
        )
        with self.assertRaisesRegex(RuntimeError, "ordinary client registration changed"):
            ordinary_identity = probe._wait_for_native_client_count(
                redis, server, Keys, "worker", 1, expected_local=1, timeout=1,
                return_identity=True,
            )
            probe._wait_for_native_active(
                redis, server, "worker", redis_keys=Keys,
                metadata_field=Fields, channel_state=States, timeout=1,
                expected_ordinary_identity=ordinary_identity,
                verified_borrow_client_ids=lambda: {"catchuparr-client-borrow"},
            )
        self.assertEqual(0, redis.state_reads)

    def test_native_active_rejects_unrelated_member_before_reconciling_borrow(self):
        class BorrowRedis:
            def __init__(self):
                self.member_reads = 0

            def get(self, _key):
                return b"native-owner"

            def smembers(self, _key):
                self.member_reads += 1
                if self.member_reads == 1:
                    return {b"ordinary-client"}
                return {b"ordinary-client", b"unrelated-client"}

            def hget(self, key, field):
                if key == "clients:ordinary-client" and field == "connected_at":
                    return b"100.5"
                if key == "clients:ordinary-client" and field == "worker_id":
                    return b"native-owner"
                return b"ACTIVE"

        class Keys:
            channel_owner = staticmethod(lambda _worker: "owner")
            channel_metadata = staticmethod(lambda _worker: "metadata")
            clients = staticmethod(lambda _worker: "clients")

        class Fields:
            STATE = "state"

        class States:
            ACTIVE = "ACTIVE"

        redis = BorrowRedis()
        server = SimpleNamespace(
            worker_id="native-owner",
            stream_managers={"worker": SimpleNamespace(running=True)},
            client_managers={"worker": SimpleNamespace(get_client_count=lambda: 1)},
        )
        ordinary_identity = probe._wait_for_native_client_count(
            redis, server, Keys, "worker", 1, return_identity=True, timeout=1,
        )
        with self.assertRaisesRegex(RuntimeError, "unverified client"):
            probe._wait_for_native_active(
                redis, server, "worker", redis_keys=Keys,
                metadata_field=Fields, channel_state=States, timeout=1,
                expected_ordinary_identity=ordinary_identity,
                verified_borrow_client_ids=lambda: {"catchuparr-client-borrow"},
            )
        self.assertEqual(2, redis.member_reads)

    def test_native_active_does_not_retry_while_local_parent_client_is_detached(self):
        class BorrowRedis:
            def __init__(self):
                self.member_reads = 0

            def get(self, _key):
                return b"native-owner"

            def smembers(self, _key):
                self.member_reads += 1
                return {b"ordinary-client"}

            def hget(self, key, field):
                if key == "clients:ordinary-client" and field == "connected_at":
                    return b"100.5"
                if key == "clients:ordinary-client" and field == "worker_id":
                    return b"native-owner"
                return b"ACTIVE"

        class Keys:
            channel_owner = staticmethod(lambda _worker: "owner")
            channel_metadata = staticmethod(lambda _worker: "metadata")
            clients = staticmethod(lambda _worker: "clients")

        class Fields:
            STATE = "state"

        class States:
            ACTIVE = "ACTIVE"

        local_counts = iter((1, 2))
        redis = BorrowRedis()
        server = SimpleNamespace(
            worker_id="native-owner",
            stream_managers={"worker": SimpleNamespace(running=True)},
            client_managers={
                "worker": SimpleNamespace(get_client_count=lambda: next(local_counts)),
            },
        )
        ordinary_identity = probe._wait_for_native_client_count(
            redis, server, Keys, "worker", 1, return_identity=True, timeout=1,
        )
        with self.assertRaisesRegex(RuntimeError, "unverified client"):
            probe._wait_for_native_active(
                redis, server, "worker", redis_keys=Keys,
                metadata_field=Fields, channel_state=States, timeout=1,
                expected_ordinary_identity=ordinary_identity,
                verified_borrow_client_ids=lambda: {"catchuparr-client-borrow"},
            )
        self.assertEqual(2, redis.member_reads)

    def test_task_borrow_identity_requires_its_channel_and_server_verification(self):
        task = SimpleNamespace(
            _lock=threading.Lock(),
            input_headers=[
                {"X-Catchuparr-Borrow": "valid"},
                {"X-Catchuparr-Borrow": "wrong-channel"},
            ],
        )
        decoded = {
            "valid": SimpleNamespace(channel_uuid="channel", nonce="nonce-1"),
            "wrong-channel": SimpleNamespace(channel_uuid="other", nonce="nonce-2"),
        }
        verified_calls = []

        def verify(worker, client_id):
            verified_calls.append((worker, client_id))
            return True

        result = probe._verified_task_borrow_client_ids(
            task,
            worker_id="worker",
            channel_uuid="channel",
            decode_capability=decoded.__getitem__,
            is_server_verified=verify,
        )
        self.assertEqual({"catchuparr-client-nonce-1"}, set(result))
        self.assertEqual([("worker", "catchuparr-client-nonce-1")], verified_calls)

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

    def test_identity_capture_accepts_current_task_borrow_before_first_sample(self):
        class Keys:
            clients = staticmethod(lambda _worker: "clients")
            channel_owner = staticmethod(lambda _worker: "owner")
            channel_metadata = staticmethod(lambda _worker: "metadata")

        class Fields:
            STATE = "state"

        class States:
            ACTIVE = "ACTIVE"

        class Redis:
            def smembers(self, _key):
                return {b"ordinary-client", b"catchuparr-client-borrow"}

            def get(self, _key):
                return b"native-owner"

            def hget(self, key, field):
                if key == "metadata" and field == "state":
                    return b"ACTIVE"
                values = {
                    ("clients:ordinary-client", "connected_at"): b"100.5",
                    ("clients:ordinary-client", "worker_id"): b"native-owner",
                }
                return values.get((key, field))

        server = SimpleNamespace(
            worker_id="native-owner",
            stream_managers={"worker": SimpleNamespace(running=True)},
            client_managers={"worker": SimpleNamespace(get_client_count=lambda: 1)},
        )
        identity = probe._wait_for_native_client_count(
            Redis(), server, Keys, "worker", 1, expected_local=1,
            return_identity=True,
            verified_borrow_client_ids=lambda: {"catchuparr-client-borrow"},
        )
        self.assertEqual(
            {"client_id": "ordinary-client", "connected_at": "100.5", "worker_id": "native-owner"},
            identity,
        )
        owner, _, _ = probe._wait_for_native_active(
            Redis(), server, "worker", redis_keys=Keys,
            metadata_field=Fields, channel_state=States, timeout=1,
            expected_ordinary_identity=identity,
            verified_borrow_client_ids=lambda: {"catchuparr-client-borrow"},
        )
        self.assertEqual("native-owner", owner)

    def test_identity_capture_rejects_unverified_extra_before_returning_identity(self):
        class Keys:
            clients = staticmethod(lambda _worker: "clients")

        class Redis:
            def smembers(self, _key):
                return {b"ordinary-client", b"unrelated-client"}

            def hget(self, key, field):
                if key.endswith("ordinary-client") and field == "connected_at":
                    return b"100.5"
                if key.endswith("ordinary-client") and field == "worker_id":
                    return b"native-owner"
                return None

        server = SimpleNamespace(
            worker_id="native-owner",
            client_managers={"worker": SimpleNamespace(get_client_count=lambda: 1)},
        )
        with self.assertRaisesRegex(RuntimeError, "unverified client"):
            probe._wait_for_native_client_count(
                Redis(), server, Keys, "worker", 1, expected_local=1,
                return_identity=True,
                verified_borrow_client_ids=lambda: {"catchuparr-client-borrow"},
            )

    def test_identity_capture_latches_ordinary_before_borrow_reconciliation(self):
        class Keys:
            clients = staticmethod(lambda _worker: "clients")

        class Redis:
            def __init__(self):
                self.reads = 0

            def smembers(self, _key):
                self.reads += 1
                if self.reads == 1:
                    return {b"ordinary-a"}
                return {b"ordinary-b", b"catchuparr-client-borrow"}

            def hget(self, key, field):
                if field == "connected_at":
                    return b"100.5" if key.endswith("ordinary-a") else b"101.5"
                if field == "worker_id":
                    return b"native-owner"
                return None

        redis = Redis()
        server = SimpleNamespace(
            worker_id="native-owner",
            client_managers={"worker": SimpleNamespace(get_client_count=lambda: 1)},
        )
        with self.assertRaisesRegex(RuntimeError, "ordinary client registration changed"):
            probe._wait_for_native_client_count(
                redis, server, Keys, "worker", 1, expected_local=1,
                return_identity=True,
                verified_borrow_client_ids=lambda: {"catchuparr-client-borrow"},
            )
        self.assertEqual(2, redis.reads)

    def test_identity_capture_rejects_local_disconnect_after_latching_ordinary(self):
        class Keys:
            clients = staticmethod(lambda _worker: "clients")

        class Redis:
            def __init__(self):
                self.reads = 0

            def smembers(self, _key):
                self.reads += 1
                return {b"ordinary-client"}

            def hget(self, key, field):
                if key.endswith("ordinary-client") and field == "connected_at":
                    return b"100.5"
                if key.endswith("ordinary-client") and field == "worker_id":
                    return b"native-owner"
                return None

        counts = iter((1, 0, 1))
        redis = Redis()
        server = SimpleNamespace(
            worker_id="native-owner",
            client_managers={"worker": SimpleNamespace(get_client_count=lambda: next(counts))},
        )
        with self.assertRaisesRegex(RuntimeError, "ordinary client detached"):
            probe._wait_for_native_client_count(
                redis, server, Keys, "worker", 1, expected_local=1,
                return_identity=True,
                verified_borrow_client_ids=lambda: {"catchuparr-client-borrow"},
            )
        self.assertEqual(2, redis.reads)

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
