import array
import ast
import contextlib
import http.client
import io
import json
import math
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import aio_recorder_failover as probe


def _sine_pcm(frequency: int, sample_rate: int = 48_000, duration: float = 1.5) -> bytes:
    samples = array.array(
        "h",
        (
            round(12_000 * math.sin(2 * math.pi * frequency * index / sample_rate))
            for index in range(round(sample_rate * duration))
        ),
    )
    if sys.byteorder != "little":
        samples.byteswap()
    return samples.tobytes()


class _SyntheticArchiveStore:
    def __init__(self, paths):
        self._segments = [SimpleNamespace(path=path) for path in paths]

    def segments(self, _channel_uuid):
        return list(self._segments)


class RecorderFailoverMediaTests(unittest.TestCase):
    def test_recorder_task_threading_preserves_observed_event_and_reentrant_lock(self):
        stop_event = threading.Event()
        task_threading = probe._recorder_task_threading(stop_event)

        self.assertIs(task_threading.Event(), stop_event)
        lock = task_threading.RLock()
        with lock:
            with lock:
                self.assertTrue(lock.acquire(blocking=False))
                lock.release()

    def test_thread_database_cleanup_runs_on_normal_and_exceptional_exit(self):
        cleanup_threads = []

        def cleanup():
            cleanup_threads.append(threading.get_ident())

        def run_case(callback):
            owner_thread = []
            failures = []

            def invoke():
                owner_thread.append(threading.get_ident())
                try:
                    probe._run_with_thread_database_cleanup(callback)
                except Exception as exc:
                    failures.append(exc)

            with patch.object(probe, "_close_current_thread_database_connections", cleanup):
                thread = threading.Thread(target=invoke)
                thread.start()
                thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(cleanup_threads[-1], owner_thread[0])
            return failures

        with self.subTest("normal exit"):
            result = []
            run_case(lambda: result.append("complete"))
            self.assertEqual(result, ["complete"])

        with self.subTest("exception exit"):
            def fail():
                raise RuntimeError("synthetic task failure")

            failures = run_case(fail)
            self.assertEqual(len(failures), 1)
            self.assertIsInstance(failures[0], RuntimeError)
            self.assertEqual(str(failures[0]), "synthetic task failure")
        self.assertEqual(len(cleanup_threads), 2)

    def test_failover_configuration_forces_global_sharing_off(self):
        original = {
            "filter_config": "version: 1\nrules: []\n",
            "connection_sharing_enabled": True,
            "live_stability_seconds": 60,
        }

        settings = probe._failover_configuration_settings(original)

        self.assertFalse(settings["connection_sharing_enabled"])
        self.assertEqual(settings["live_stability_seconds"], 0)
        self.assertEqual(settings["last_viewer_return_seconds"], 0)
        self.assertEqual(settings["min_switch_interval_seconds"], 0)
        self.assertTrue(settings["connection_sharing_migration_confirmed"])
        self.assertTrue(original["connection_sharing_enabled"])
        self.assertEqual(original["live_stability_seconds"], 60)

    def test_supervisor_stop_reason_map_covers_actual_static_stop_sites(self):
        tree = ast.parse((Path(__file__).parents[1] / "catchuparr/tasks.py").read_text())
        record_channel = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "record_channel"
        )
        supervise = next(
            node for node in ast.walk(record_channel)
            if isinstance(node, ast.FunctionDef) and node.name == "supervise"
        )
        stop_lines = {
            node.lineno for node in ast.walk(supervise)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "set"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "stop_event"
        }
        self.assertEqual(stop_lines, set(probe._SUPERVISOR_STOP_REASONS_BY_LINE))
        self.assertEqual(
            probe._safe_supervisor_stop_reason(
                "catchuparr.tasks", "supervise", 501,
                {"current_admission": {"status": "ready"}},
            ),
            "lease_renewal",
        )
        self.assertEqual(
            probe._safe_supervisor_stop_reason(
                "catchuparr.tasks", "supervise", 501,
                {"current_admission": {"status": "stale_control"}},
            ),
            "admission_not_ready",
        )
        self.assertEqual(
            probe._safe_supervisor_stop_reason(
                "catchuparr.tasks", "supervise", 524, {},
            ),
            "stats_capability_refresh",
        )
        self.assertEqual(
            probe._safe_supervisor_stop_reason(
                "catchuparr.tasks", "supervise", 556, {},
            ),
            "attempt_renewal",
        )
        self.assertEqual(
            probe._safe_supervisor_stop_reason(
                "catchuparr.tasks", "supervise", 564, {},
            ),
            "candidate_currentness",
        )
        self.assertEqual(
            probe._safe_supervisor_stop_reason(
                "catchuparr.tasks", "supervise", 568, {},
            ),
            "exception",
        )
        self.assertEqual(
            probe._safe_supervisor_stop_reason("untrusted", "supervise", 501, {}),
            "unavailable",
        )

    def test_stall_source_keeps_one_http_response_open_with_null_tail(self):
        payload = b"b" * (188 * 64)
        server = probe._SyntheticFailoverSourceServer(
            {"source-b.ts": payload}, {"source-b.ts": 0.05},
        )
        server.set_mode("source-b.ts", "stall", repeats=1)
        server.thread.start()
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_port, timeout=3,
        )
        response = None
        try:
            connection.request("GET", "/source-b.ts")
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(response.read(len(payload)), payload)
            null_chunk_size = 188 * probe.SOURCE_CHUNK_PACKETS
            self.assertEqual(
                response.read(null_chunk_size),
                probe._null_transport_stream()[:null_chunk_size],
            )

            request_counts, active_counts = server.snapshot()
            deadline = time.monotonic() + 1
            stall = server.stall_snapshot()["source-b.ts"]
            while stall["null_bytes"] == 0 and time.monotonic() < deadline:
                time.sleep(0.01)
                stall = server.stall_snapshot()["source-b.ts"]
            self.assertEqual(request_counts["source-b.ts"], 1)
            self.assertEqual(active_counts["source-b.ts"], 1)
            self.assertEqual(stall["started"], 1)
            self.assertGreater(stall["null_bytes"], 0)
        finally:
            if response is not None:
                response.close()
            connection.close()
            server.close()

        self.assertEqual(server.snapshot()[1]["source-b.ts"], 0)

    def test_recorder_attempt_summary_exposes_only_known_status_and_count(self):
        summary = probe._recorder_attempt_summary([
            SimpleNamespace(
                status="media_stalled", useful_segments=2,
                input_url="http://127.0.0.1/private-token",
            ),
            SimpleNamespace(status="private-value", useful_segments="bad"),
        ])

        self.assertEqual(
            summary,
            [
                {"status": "media_stalled", "useful_segments": 2},
                {"status": "other", "useful_segments": None},
            ],
        )
        self.assertNotIn("private-token", json.dumps(summary))

    def test_recorder_attempt_summary_omits_unknown_status_and_raising_properties(self):
        class SensitiveAttempt:
            input_url = "http://user:password@synthetic.invalid/private-token"

            @property
            def status(self):
                raise RuntimeError("Bearer secret-token")

            @property
            def useful_segments(self):
                raise RuntimeError("http://user:password@synthetic.invalid/private-token")

        summary = probe._recorder_attempt_summary([
            SimpleNamespace(
                status="https://user:password@synthetic.invalid/private-token",
                useful_segments=1,
            ),
            SensitiveAttempt(),
        ])
        serialized = json.dumps(summary)

        self.assertEqual(
            summary,
            [
                {"status": "other", "useful_segments": 1},
                {"status": "other", "useful_segments": None},
            ],
        )
        for private_value in ("synthetic.invalid", "password", "private-token", "secret-token"):
            self.assertNotIn(private_value, serialized)

    def test_recorder_task_exit_diagnostic_is_allowlisted(self):
        class FakeProcess:
            @staticmethod
            def poll():
                return 7

        class FakeThread:
            @staticmethod
            def is_alive():
                return False

        run = SimpleNamespace(
            results=[SimpleNamespace(
                status="no_media", useful_segments=0,
                input_url="http://user:password@synthetic.invalid/private-token",
            )],
            failures=["task:BorrowBindingLost", "Bearer private-token"],
            exception_call_sites=[[
                {"file": "tasks.py", "function": "record_channel", "line": 375,
                 "locals": "private-token"},
            ]],
            processes=[FakeProcess()],
            result={"status": "sharing_cleanup_pending", "token": "private-token"},
            thread=FakeThread(),
            stop_event=threading.Event(),
            expected_generation="synthetic-generation",
            expected_control_generation=3,
        )

        diagnostic = probe._recorder_task_exit_diagnostic(run)
        serialized = json.dumps(diagnostic)
        self.assertEqual(diagnostic["task_status"], "sharing_cleanup_pending")
        self.assertEqual(diagnostic["failure_classes"], ["BorrowBindingLost", "other"])
        self.assertEqual(diagnostic["exception_call_sites"], [[
            {"file": "tasks.py", "function": "record_channel", "line": 375},
        ]])
        self.assertEqual(diagnostic["attempts"], [
            {"status": "no_media", "useful_segments": 0},
        ])
        self.assertEqual(diagnostic["process_exit_codes"], [7])
        self.assertEqual(diagnostic["stop_set_source"], "unavailable")
        self.assertEqual(diagnostic["first_supervisor_admission_status"], "unavailable")
        self.assertEqual(diagnostic["first_supervisor_stop_reason"], "unavailable")
        for private_value in ("synthetic.invalid", "password", "private-token", "synthetic-generation"):
            self.assertNotIn(private_value, serialized)

    def test_stop_set_source_is_a_finite_callsite_category(self):
        self.assertEqual(
            "tasks.supervisor",
            probe._classify_stop_set_source("catchuparr.tasks", "supervise"),
        )
        self.assertEqual(
            "tasks.other",
            probe._classify_stop_set_source("catchuparr.tasks", "unknown_callable"),
        )
        self.assertEqual(
            "probe.harness_cleanup",
            probe._classify_stop_set_source("scripts.aio_recorder_failover", "stop"),
        )
        self.assertEqual(
            "other",
            probe._classify_stop_set_source("SyntheticSecretTokenABC123", "private"),
        )
        event = probe._ObservedRecorderStopEvent()
        namespace = {
            "__name__": "catchuparr.tasks",
            "event": event,
            "current_admission": {"status": "stale_configuration"},
        }
        tree = ast.parse("def supervise(current_admission=current_admission):\n    event.set()\n")
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
        self.assertEqual("tasks.supervisor", event.first_set_source)
        self.assertEqual(
            "stale_configuration", event.first_supervisor_admission_status,
        )
        self.assertEqual("admission_not_ready", event.first_supervisor_stop_reason)

    def test_concurrent_stop_setters_preserve_first_caller_atomically(self):
        event = probe._ObservedRecorderStopEvent()
        classifier_entered = threading.Event()
        release_classifier = threading.Event()
        classified = []

        def classify(_module, function):
            classified.append(function)
            if function == "first_setter":
                classifier_entered.set()
                self.assertTrue(release_classifier.wait(2))
                return "tasks.supervisor"
            return "probe.harness_cleanup"

        def first_setter():
            event.set()

        def second_setter():
            event.set()

        with patch.object(probe, "_classify_stop_set_source", side_effect=classify):
            first = threading.Thread(target=first_setter, name="first_setter")
            second = threading.Thread(target=second_setter, name="second_setter")
            first.start()
            self.assertTrue(classifier_entered.wait(1))
            second.start()
            release_classifier.set()
            first.join(2)
            second.join(2)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertTrue(event.is_set())
        self.assertEqual("tasks.supervisor", event.first_set_source)
        self.assertEqual(["first_setter"], classified)

    def test_early_task_exit_emits_diagnostic_and_keeps_original_error(self):
        run = SimpleNamespace(
            thread=SimpleNamespace(is_alive=lambda: False),
            result={"status": "stale_configuration"},
            failures=["task:RecorderControlError"],
            results=[],
            processes=[],
            stop_event=threading.Event(),
            expected_generation="synthetic-generation",
            expected_control_generation=2,
        )
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            with self.assertRaisesRegex(RuntimeError, "^Recorder task ended before media was indexed$"):
                probe._wait_for_source_segments(
                    run, object(), "synthetic-channel", "ffmpeg", "ffprobe",
                    "Synthetic Source B", minimum=1, timeout=1,
                )
        line = stderr.getvalue().strip()
        self.assertTrue(line.startswith(probe.RECORDER_EXIT_DIAGNOSTIC_PREFIX))
        diagnostic = json.loads(line[len(probe.RECORDER_EXIT_DIAGNOSTIC_PREFIX):])
        self.assertEqual(diagnostic["task_status"], "stale_configuration")
        self.assertEqual(diagnostic["failure_classes"], ["RecorderControlError"])
        self.assertNotIn("synthetic-generation", line)

    def test_native_failure_observations_are_categories_without_native_values(self):
        worker_a = "catchuparr-r" + "a" * 40

        class FakeRedis:
            def __init__(self):
                self.values = {
                    f"metadata:{worker_a}": True,
                    f"stopping:{worker_a}": False,
                    f"owner:{worker_a}": b"native-worker-secret",
                    f"state:{worker_a}": b"https://user:password@synthetic.invalid/token",
                    f"clients:{worker_a}": 7,
                }

            def exists(self, key):
                return self.values.get(key, False)

            def get(self, key):
                return self.values.get(key)

            def hget(self, key, _field):
                return self.values.get(f"state:{key.removeprefix('metadata:')}")

            def scard(self, key):
                return self.values.get(key, 0)

        class FakeKeys:
            channel_metadata = staticmethod(lambda worker_id: f"metadata:{worker_id}")
            channel_owner = staticmethod(lambda worker_id: f"owner:{worker_id}")
            channel_stopping = staticmethod(lambda worker_id: f"stopping:{worker_id}")
            clients = staticmethod(lambda worker_id: f"clients:{worker_id}")

        class FakeThread:
            @staticmethod
            def is_alive():
                return True

        class FakeNativeServer:
            worker_id = "different-native-worker"

            @staticmethod
            def _get_stream_thread(worker_id):
                return FakeThread() if worker_id.endswith("a" * 40) else None

        observations = probe._native_source_role_observations(
            FakeRedis(),
            [{
                "channel_uuid": "synthetic-channel",
                "stream_id": "synthetic-stream-a",
                "worker_id": worker_a,
                "created_at": "1",
            }],
            {"a": "synthetic-stream-a", "b": "synthetic-stream-b"},
            redis_keys=FakeKeys,
            metadata_state_field="state",
            native_server=FakeNativeServer(),
        )
        serialized = json.dumps(observations)

        self.assertEqual(
            observations["a"],
            {
                "worker_records": 1,
                "owner_present": True,
                "owner_matches_local": False,
                "metadata_exists": True,
                "stopping": False,
                "client_count": "many",
                "state": "other",
                "manager_running": True,
            },
        )
        self.assertEqual(observations["b"]["worker_records"], 0)
        for private_value in (
            worker_a,
            "native-worker-secret",
            "synthetic-stream-a",
            "synthetic.invalid",
            "password",
            "token",
        ):
            self.assertNotIn(private_value, serialized)

    def test_decoded_tones_identify_b_c_and_d_separately(self):
        expected = {
            "Synthetic Source B": 880,
            "Synthetic Source C": 660,
            "Synthetic Source D": 440,
        }
        for source, frequency in expected.items():
            with self.subTest(source=source):
                detected = probe._estimate_tone_frequency(_sine_pcm(frequency))
                self.assertIsNotNone(detected)
                self.assertLessEqual(abs(detected - frequency), 2)
                self.assertEqual(probe._synthetic_source_for_frequency(detected), source)
                short_detected = probe._estimate_tone_frequency(
                    _sine_pcm(frequency, duration=0.1)
                )
                self.assertIsNotNone(short_detected)
                self.assertLessEqual(abs(short_detected - frequency), 5)
                self.assertEqual(probe._synthetic_source_for_frequency(short_detected), source)

        self.assertIsNone(probe._synthetic_source_for_frequency(750))
        self.assertIsNone(
            probe._synthetic_source_for_frequency(
                probe._estimate_tone_frequency(_sine_pcm(750, duration=0.1))
            )
        )
        self.assertIsNone(probe._estimate_tone_frequency(b"\0" * 96_000))

    def test_indexed_segments_are_classified_by_decoded_audio_not_service_tag(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            frequencies = {
                "segment-b.ts": 880,
                "segment-c.ts": 660,
                "segment-d.ts": 440,
            }
            paths = []
            for name in frequencies:
                path = root / name
                path.write_bytes(b"synthetic segment fixture")
                paths.append(path)

            useful_streams = [
                {"codec_type": "video", "nb_read_frames": "12"},
                {"codec_type": "audio", "nb_read_frames": "30"},
            ]

            def fake_run(command, **_kwargs):
                if command[0] == "ffprobe":
                    # Remuxed segments intentionally have no source service tag.
                    payload = json.dumps({"programs": [], "streams": useful_streams}).encode()
                    return SimpleNamespace(returncode=0, stdout=payload)
                path = Path(command[5])
                return SimpleNamespace(
                    returncode=0,
                    stdout=_sine_pcm(frequencies[path.name]),
                )

            probe._cached_segment_identity.cache_clear()
            try:
                with patch.object(probe.subprocess, "run", side_effect=fake_run):
                    sources = probe._verified_segments(
                        _SyntheticArchiveStore(paths),
                        "synthetic-channel",
                        "ffmpeg",
                        "ffprobe",
                    )
            finally:
                probe._cached_segment_identity.cache_clear()

        self.assertEqual(
            set(sources),
            {"Synthetic Source B", "Synthetic Source C", "Synthetic Source D"},
        )
        self.assertEqual(len(sources["Synthetic Source B"]), 1)
        self.assertEqual(len(sources["Synthetic Source C"]), 1)
        self.assertEqual(len(sources["Synthetic Source D"]), 1)
        self.assertEqual(sources["Synthetic Source B"][0].path.name, "segment-b.ts")
        self.assertEqual(sources["Synthetic Source C"][0].path.name, "segment-c.ts")
        self.assertEqual(sources["Synthetic Source D"][0].path.name, "segment-d.ts")

    def test_a_decoded_tone_without_both_audio_and_video_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "audio-only.ts"
            path.write_bytes(b"synthetic segment fixture")

            def fake_run(command, **_kwargs):
                if command[0] != "ffprobe":
                    raise AssertionError("audio should not be decoded for a non-useful segment")
                payload = json.dumps(
                    {"programs": [], "streams": [{"codec_type": "audio", "nb_read_frames": "20"}]}
                ).encode()
                return SimpleNamespace(returncode=0, stdout=payload)

            probe._cached_segment_identity.cache_clear()
            try:
                with patch.object(probe.subprocess, "run", side_effect=fake_run):
                    with self.assertRaisesRegex(RuntimeError, "lacks decoded audio/video"):
                        probe._verified_segments(
                            _SyntheticArchiveStore([path]),
                            "synthetic-channel",
                            "ffmpeg",
                            "ffprobe",
                        )
            finally:
                probe._cached_segment_identity.cache_clear()

    def test_useful_av_with_an_unrecognized_tone_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "unknown-tone.ts"
            path.write_bytes(b"synthetic segment fixture")
            useful_streams = [
                {"codec_type": "video", "nb_read_frames": "12"},
                {"codec_type": "audio", "nb_read_frames": "30"},
            ]

            def fake_run(command, **_kwargs):
                if command[0] == "ffprobe":
                    payload = json.dumps({"programs": [], "streams": useful_streams}).encode()
                    return SimpleNamespace(returncode=0, stdout=payload)
                return SimpleNamespace(returncode=0, stdout=_sine_pcm(750))

            probe._cached_segment_identity.cache_clear()
            try:
                with patch.object(probe.subprocess, "run", side_effect=fake_run):
                    with self.assertRaisesRegex(RuntimeError, "no recognized synthetic source"):
                        probe._verified_segments(
                            _SyntheticArchiveStore([path]),
                            "synthetic-channel",
                            "ffmpeg",
                            "ffprobe",
                        )
            finally:
                probe._cached_segment_identity.cache_clear()


class RecorderFailoverBridgeTests(unittest.TestCase):
    def test_d_media_timeout_emits_bounded_existing_observations_and_keeps_error(self):
        verified = {
            "Synthetic Source B": [object(), object()],
            "Synthetic Source D": [],
        }
        baseline = (
            {
                "source-a.ts": 1,
                "source-b.ts": 2,
                "source-c.ts": 7,
                "source-d.ts": 8,
            },
            {
                "source-a.ts": 0,
                "source-b.ts": 0,
                "source-c.ts": 0,
                "source-d.ts": 0,
            },
        )
        current = (
            {
                "source-a.ts": 3,
                "source-b.ts": 5,
                "source-c.ts": 7,
                "source-d.ts": 9,
            },
            {
                "source-a.ts": 0,
                "source-b.ts": 1,
                "source-c.ts": 0,
                "source-d.ts": 0,
            },
        )

        class SourceServer:
            def __init__(self):
                self._snapshots = iter((baseline, current))

            def snapshot(self):
                return next(self._snapshots)

            @staticmethod
            def stall_snapshot():
                return {
                    "source-b.ts": {
                        "started": 4,
                        "duration_seconds": 3.25,
                        "null_bytes": 4096,
                    },
                }

        run = SimpleNamespace(
            thread=SimpleNamespace(is_alive=lambda: True),
            results=[SimpleNamespace(status="media_stalled", useful_segments=2)],
        )
        source_server = SourceServer()
        request_baseline, active_baseline = source_server.snapshot()
        stall_baseline = {
            "started": 3,
            "duration_seconds": 1.0,
            "null_bytes": 1024,
        }
        diagnostic = {
            "attempts": [{"status": "media_stalled", "useful_segments": 2}],
            "source_deltas": {
                role: {"request_delta": 1, "active_delta": 0}
                for role in ("a", "b", "c", "d")
            },
            "b_stall": {
                "started_delta": 1,
                "duration_ms": 2250,
                "null_bytes_delta": 3072,
            },
            "indexed_segments": {"b": 2, "c": 0, "d": 0},
            "native": {"a": {"state": "active"}},
        }
        callback_verified = []
        diagnostic_context = patch.object(
            probe, "_native_source_observations", return_value=diagnostic["native"],
        )

        def build_diagnostic(indexed):
            callback_verified.append(indexed)
            with diagnostic_context:
                return timeout_callback(indexed)

        timeout_callback = probe._make_runtime_timeout_diagnostic_callback(
            run,
            source_server,
            request_baseline,
            active_baseline,
            stall_baseline,
            object(),
            "synthetic-channel",
            {},
        )

        stderr = io.StringIO()
        with (
            patch.object(probe, "_verified_segments", return_value=verified) as verify,
            patch.object(probe.time, "monotonic", side_effect=(0.0, 0.0, 2.0)),
            patch.object(probe.time, "sleep"),
            contextlib.redirect_stderr(stderr),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "^Synthetic recorder did not index expected source media in time$",
            ):
                probe._wait_for_source_segments(
                    run,
                    object(),
                    "synthetic-channel",
                    "ffmpeg",
                    "ffprobe",
                    "Synthetic Source D",
                    minimum=1,
                    timeout=1,
                    timeout_diagnostic=build_diagnostic,
                )

        self.assertEqual(verify.call_count, 1)
        self.assertEqual(callback_verified, [verified])
        lines = stderr.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith(probe.FAILOVER_DIAGNOSTIC_PREFIX))
        self.assertLessEqual(
            len(lines[0].encode("utf-8")),
            len(probe.FAILOVER_DIAGNOSTIC_PREFIX.encode("utf-8"))
            + probe.FAILOVER_DIAGNOSTIC_MAX_BYTES,
        )
        emitted = json.loads(lines[0][len(probe.FAILOVER_DIAGNOSTIC_PREFIX):])
        self.assertEqual(emitted["attempts"], diagnostic["attempts"])
        self.assertEqual(emitted["indexed_segments"], {"b": 2, "c": 0, "d": 0})
        self.assertEqual(
            emitted["source_deltas"],
            {
                "a": {"request_delta": 2, "active_delta": 0},
                "b": {"request_delta": 3, "active_delta": 1},
                "c": {"request_delta": 0, "active_delta": 0},
                "d": {"request_delta": 1, "active_delta": 0},
            },
        )
        self.assertEqual(
            emitted["b_stall"],
            {"started_delta": 1, "duration_ms": 2250, "null_bytes_delta": 3072},
        )
        self.assertEqual(emitted["native"], diagnostic["native"])

    def test_d_media_timeout_diagnostic_failure_keeps_original_timeout(self):
        class FakeThread:
            def __init__(self):
                self.join_timeouts = []

            @staticmethod
            def is_alive():
                return True

            def join(self, timeout):
                self.join_timeouts.append(timeout)

        class FakeRun:
            def __init__(self):
                self.thread = FakeThread()
                self.stop_calls = 0

            def stop(self):
                self.stop_calls += 1

        run = FakeRun()
        stderr = io.StringIO()

        def fail_to_build(_verified):
            raise RuntimeError("https://user:password@synthetic.invalid/private-token")

        with (
            patch.object(probe, "_verified_segments", return_value={}),
            patch.object(probe.time, "monotonic", side_effect=(0.0, 0.0, 2.0)),
            patch.object(probe.time, "sleep"),
            contextlib.redirect_stderr(stderr),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "^Synthetic recorder did not index expected source media in time$",
            ):
                try:
                    probe._wait_for_source_segments(
                        run,
                        object(),
                        "synthetic-channel",
                        "ffmpeg",
                        "ffprobe",
                        "Synthetic Source D",
                        minimum=1,
                        timeout=1,
                        timeout_diagnostic=fail_to_build,
                    )
                except RuntimeError:
                    probe._cleanup_failed_recorder_run(run)
                    raise

        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(run.stop_calls, 1)
        self.assertEqual(run.thread.join_timeouts, [20])

    def test_timeout_diagnostic_is_capped_when_snapshot_is_oversized(self):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            probe._emit_timeout_diagnostic({"fixed": "x" * probe.FAILOVER_DIAGNOSTIC_MAX_BYTES * 2})
        self.assertEqual(
            stderr.getvalue(),
            probe.FAILOVER_DIAGNOSTIC_PREFIX + '{"diagnostic":"omitted"}\n',
        )

    def test_supervisor_join_waits_without_signalling_the_task(self):
        run = probe._RecorderTaskRun(
            "synthetic-channel", startup_timeout=1, media_idle_timeout=1,
        )

        class FinishingThread:
            def __init__(self):
                self.alive_states = iter((True, True, False, False))
                self.joined_with = []

            def join(self, timeout):
                self.joined_with.append(timeout)

            def is_alive(self):
                return next(self.alive_states)

        thread = FinishingThread()
        cooperative_sleeps = []
        run.thread = thread
        run.result = {"status": "stopped"}
        run.join_after_supervisor(
            timeout=7, cooperative_sleep=cooperative_sleeps.append,
        )

        self.assertEqual(thread.joined_with, [0])
        self.assertEqual(len(cooperative_sleeps), 1)
        self.assertGreater(cooperative_sleeps[0], 0)
        self.assertLessEqual(cooperative_sleeps[0], 0.05)
        self.assertFalse(run.stop_event.is_set())

    def test_supervisor_join_requires_explicit_expected_fence_result(self):
        run = probe._RecorderTaskRun(
            "synthetic-channel", startup_timeout=1, media_idle_timeout=1,
        )
        run.result = {"status": "stale_control"}

        with patch.object(probe, "_emit_recorder_exit_diagnostic"):
            with self.assertRaisesRegex(RuntimeError, "did not finish as expected"):
                run.join_after_supervisor(timeout=1)
            run.join_after_supervisor(
                timeout=1, expected_result={"status": "stale_control"},
            )

        self.assertFalse(run.stop_event.is_set())

    def test_supervisor_join_keeps_timeout_bounded_without_signalling(self):
        run = probe._RecorderTaskRun(
            "synthetic-channel", startup_timeout=1, media_idle_timeout=1,
        )

        class StuckThread:
            def __init__(self):
                self.joined_with = []

            def join(self, timeout):
                self.joined_with.append(timeout)

            @staticmethod
            def is_alive():
                return True

        thread = StuckThread()
        run.thread = thread
        started = time.monotonic()
        with self.assertRaisesRegex(RuntimeError, "did not stop in time"):
            run.join_after_supervisor(timeout=0.02, cooperative_sleep=time.sleep)

        self.assertLess(time.monotonic() - started, 0.5)
        self.assertTrue(thread.joined_with)
        self.assertTrue(all(timeout == 0 for timeout in thread.joined_with))
        self.assertFalse(run.stop_event.is_set())

    def test_bridge_idle_wait_observes_handler_close(self):
        drained = threading.Event()
        server = SimpleNamespace(active_requests=lambda: int(not drained.is_set()))

        def finish_request():
            time.sleep(0.03)
            drained.set()

        closer = threading.Thread(target=finish_request)
        closer.start()
        try:
            active = probe._wait_for_bridge_idle(
                SimpleNamespace(server=server), timeout=1
            )
        finally:
            closer.join(timeout=1)

        self.assertFalse(closer.is_alive())
        self.assertEqual(active, 0)

    def test_bridge_idle_wait_leaves_timeout_failure_visible(self):
        server = SimpleNamespace(active_requests=lambda: 1)
        active = probe._wait_for_bridge_idle(
            SimpleNamespace(server=server), timeout=0.02
        )
        self.assertEqual(active, 1)


if __name__ == "__main__":
    unittest.main()
