import array
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

        class SourceServer:
            @staticmethod
            def snapshot():
                return (
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

        def build_diagnostic(indexed):
            callback_verified.append(indexed)
            with patch.object(
                probe, "_native_source_observations", return_value=diagnostic["native"],
            ):
                return probe._runtime_timeout_diagnostic(
                    run,
                    SourceServer(),
                    baseline,
                    stall_baseline,
                    indexed,
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
        self.assertEqual(emitted["source_deltas"]["b"], {"request_delta": 3, "active_delta": 1})
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
