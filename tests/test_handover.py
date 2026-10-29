import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from catchuparr.engine.handover import matching_completed_segments
from catchuparr.engine.recorder import (
    FFmpegCopyRecorder,
    RecorderAttemptSwitch,
    StagedRecorderInput,
    _staged_csv_rows,
)
from catchuparr.engine.store import ArchiveStore


@unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "requires FFmpeg")
class HandoverEvidenceTests(unittest.TestCase):
    def test_independent_mux_clocks_with_identical_av_match_but_other_media_does_not(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.ts"
            other = root / "other.ts"
            for path, frequency in ((source, 440), (other, 670)):
                subprocess.run([
                    "ffmpeg", "-v", "error", "-f", "lavfi", "-i",
                    "testsrc2=size=64x64:rate=25", "-f", "lavfi", "-i",
                    f"sine=frequency={frequency}:sample_rate=48000", "-t", "3",
                    "-c:v", "mpeg2video", "-g", "25", "-c:a", "mp2", "-f", "mpegts", str(path),
                ], check=True, capture_output=True, timeout=30)
            remuxed = root / "remuxed.ts"
            subprocess.run([
                "ffmpeg", "-v", "error", "-i", str(source), "-map", "0", "-c", "copy",
                "-output_ts_offset", "3", "-f", "mpegts", str(remuxed),
            ], check=True, capture_output=True, timeout=30)
            self.assertTrue(matching_completed_segments(source, remuxed))
            self.assertFalse(matching_completed_segments(source, other))
            self.assertFalse(matching_completed_segments(source, root / "missing.ts"))

    def test_clock_offsets_must_match_between_video_and_audio(self):
        video = tuple((f"SHA256:v{i}", i / 25, "K" if i == 0 else "") for i in range(30))
        audio = tuple((f"SHA256:a{i}", i / 10, "") for i in range(10))
        shifted_video = tuple((digest, timestamp + 2, flags) for digest, timestamp, flags in video)
        shifted_audio = tuple((digest, timestamp + 3, flags) for digest, timestamp, flags in audio)
        original = (("video", "mpeg2video", video), ("audio", "mp2", audio))
        changed = (("video", "mpeg2video", shifted_video), ("audio", "mp2", shifted_audio))
        with patch("catchuparr.engine.handover._packet_fingerprints", side_effect=[original, changed]):
            self.assertFalse(matching_completed_segments(Path("a"), Path("b")))

    def test_expired_sync_deadline_does_not_start_a_probe(self):
        with patch("catchuparr.engine.handover._packet_fingerprints") as probe:
            self.assertFalse(matching_completed_segments(Path("a"), Path("b"), deadline=time.monotonic() - 1))
        probe.assert_not_called()

    def test_video_without_audio_cannot_prove_an_audio_video_handover(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "video-only.ts"
            subprocess.run([
                "ffmpeg", "-v", "error", "-f", "lavfi", "-i",
                "testsrc2=size=64x64:rate=25", "-t", "2", "-an",
                "-c:v", "mpeg2video", "-g", "25", "-f", "mpegts", str(source),
            ], check=True, capture_output=True, timeout=30)
            self.assertFalse(matching_completed_segments(source, source))

    def test_matched_handover_publishes_only_media_after_proven_boundary(self):
        class Process:
            ended = False

            def poll(self):
                return 0 if self.ended else None

            def terminate(self):
                self.ended = True

            def wait(self, timeout=None):
                return 0

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stage_root = root / "stage"
            stage_root.mkdir()
            subprocess.run([
                "ffmpeg", "-v", "error", "-f", "lavfi", "-i",
                "testsrc2=size=64x64:rate=25", "-f", "lavfi", "-i",
                "sine=frequency=440:sample_rate=48000", "-t", "4",
                "-c:v", "mpeg2video", "-g", "25", "-c:a", "mp2",
                "-f", "segment", "-segment_time", "2", "-segment_format", "mpegts",
                "-segment_list", str(stage_root / "segments.csv"),
                "-segment_list_type", "csv", str(stage_root / "segment-%06d.ts"),
            ], check=True, capture_output=True, timeout=30)
            rows, _ = _staged_csv_rows(stage_root)
            self.assertEqual(2, len(rows))
            store = ArchiveStore(root / "archive")
            old = FFmpegCopyRecorder(store, "synthetic", "http://example.invalid/old", root / "work")
            target = FFmpegCopyRecorder(store, "synthetic", "http://example.invalid/new", root / "work")
            started = datetime.now(timezone.utc) - timedelta(seconds=2)
            first = store.add_segment(
                "synthetic", rows[0][0], started,
                started + timedelta(seconds=rows[0][2] - rows[0][1]),
            )
            old._current_attempt_segment_id = first.id
            old._last_commit_monotonic = time.monotonic()
            stage_process, old_process = Process(), Process()
            staged = StagedRecorderInput(target, stage_root, stage_process)
            switch = RecorderAttemptSwitch()
            with patch.object(target, "_start_staged_input", return_value=staged):
                result, _offset, count = old._try_synchronized_handover(
                    target, old_process, root / "old.csv", 0, threading.Event(), switch, lambda _: None,
                )
            self.assertIs(result, staged)
            self.assertTrue(staged.matched)
            self.assertEqual(staged.offset, rows[0][3])
            self.assertTrue(switch.is_set())
            self.assertEqual(count, 0)
            stage_process.ended = True
            # Consume the same process and skip the already archived overlap.
            with patch("catchuparr.engine.recorder.subprocess.Popen", side_effect=AssertionError("reopened")):
                resumed = target.run_candidate(threading.Event(), staged_input=staged)
            self.assertEqual(resumed.useful_segments, 1)
            recorded = store.segments("synthetic")
            self.assertEqual(len(recorded), 2)
            self.assertEqual(recorded[0].id, first.id)
            self.assertEqual(recorded[1].start_utc, first.end_utc)
            self.assertTrue(recorded[1].discontinuity)

    def test_return_matches_archived_tail_and_reuses_the_same_reader(self):
        class Process:
            ended = False

            def poll(self):
                return 0 if self.ended else None

            def terminate(self):
                self.ended = True

            def wait(self, timeout=None):
                return 0

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "stage"
            output.mkdir()
            subprocess.run([
                "ffmpeg", "-v", "error", "-f", "lavfi", "-i",
                "testsrc2=size=64x64:rate=25", "-f", "lavfi", "-i",
                "sine=frequency=440:sample_rate=48000", "-t", "4",
                "-c:v", "mpeg2video", "-g", "25", "-c:a", "mp2",
                "-f", "segment", "-segment_time", "2", "-segment_format", "mpegts",
                "-segment_list", str(output / "segments.csv"),
                "-segment_list_type", "csv", str(output / "segment-%06d.ts"),
            ], check=True, capture_output=True, timeout=30)
            rows, _ = _staged_csv_rows(output)
            self.assertEqual(len(rows), 2)
            store = ArchiveStore(root / "archive")
            target = FFmpegCopyRecorder(store, "synthetic", "http://example.invalid/new", root / "work")
            start = datetime.now(timezone.utc) - timedelta(seconds=2)
            tail = store.add_segment(
                "synthetic", rows[0][0], start, start + timedelta(seconds=rows[0][2] - rows[0][1]),
            )
            staged = StagedRecorderInput(target, output, Process())
            switch = RecorderAttemptSwitch()
            with patch.object(target, "_start_staged_input", return_value=staged) as start_input:
                result = target.prepare_synchronized_return(tail.id, threading.Event(), switch)
            start_input.assert_called_once_with()
            self.assertIs(result, staged)
            self.assertTrue(staged.matched)
            self.assertEqual(staged.offset, rows[0][3])
            self.assertFalse(switch.is_set())
            staged.process.ended = True
            with patch("catchuparr.engine.recorder.subprocess.Popen", side_effect=AssertionError("reopened")):
                resumed = target.run_candidate(threading.Event(), staged_input=staged)
            self.assertEqual(resumed.useful_segments, 1)
            recorded = store.segments("synthetic")
            self.assertEqual(len(recorded), 2)
            self.assertEqual(recorded[0].id, tail.id)
            self.assertEqual(recorded[1].start_utc, tail.end_utc)
            self.assertTrue(recorded[1].discontinuity)

    def test_return_without_match_keeps_all_staged_media_for_normal_fallback(self):
        class Process:
            def poll(self):
                return None

            def terminate(self):
                pass

            def wait(self, timeout=None):
                return 0

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "stage"
            output.mkdir()
            media = output / "segment-000000.ts"
            media.write_bytes(b"synthetic")
            (output / "segments.csv").write_text("segment-000000.ts,0,2\n", encoding="utf-8")
            store = ArchiveStore(root / "archive")
            target = FFmpegCopyRecorder(store, "synthetic", "http://example.invalid/new", root / "work")
            start = datetime.now(timezone.utc) - timedelta(seconds=2)
            tail = store.add_segment("synthetic", media, start, start + timedelta(seconds=2))
            staged = StagedRecorderInput(target, output, Process())
            with (patch.object(target, "_start_staged_input", return_value=staged),
                  patch("catchuparr.engine.recorder.SYNCHRONIZED_HANDOVER_TIMEOUT_SECONDS", 0)):
                result = target.prepare_synchronized_return(tail.id, threading.Event(), RecorderAttemptSwitch())
            self.assertIs(result, staged)
            self.assertFalse(staged.matched)
            self.assertEqual(staged.offset, 0)
            self.assertIsNone(staged.anchor)
            staged.close()

    def test_return_rejects_stale_tail_without_opening_a_reader(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            media = root / "segment.ts"
            media.write_bytes(b"synthetic")
            store = ArchiveStore(root / "archive")
            target = FFmpegCopyRecorder(store, "synthetic", "http://example.invalid/new", root / "work")
            start = datetime.now(timezone.utc) - timedelta(seconds=2)
            store.add_segment("synthetic", media, start, start + timedelta(seconds=2))
            with patch.object(target, "_start_staged_input") as start_input:
                self.assertIsNone(target.prepare_synchronized_return(
                    "stale", threading.Event(), RecorderAttemptSwitch(),
                ))
            start_input.assert_not_called()

    def test_return_interrupted_during_comparison_reaps_reader_without_reopening(self):
        class Process:
            ended = False

            def poll(self):
                return 0 if self.ended else None

            def terminate(self):
                self.ended = True

            def wait(self, timeout=None):
                return 0

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "stage"
            output.mkdir()
            media = output / "segment-000000.ts"
            media.write_bytes(b"synthetic")
            (output / "segments.csv").write_text("segment-000000.ts,0,2\n", encoding="utf-8")
            store = ArchiveStore(root / "archive")
            target = FFmpegCopyRecorder(store, "synthetic", "http://example.invalid/new", root / "work")
            start = datetime.now(timezone.utc) - timedelta(seconds=2)
            tail = store.add_segment("synthetic", media, start, start + timedelta(seconds=2))
            staged = StagedRecorderInput(target, output, Process())
            stop, switch = threading.Event(), RecorderAttemptSwitch()

            def interrupt(*_paths, **_kwargs):
                switch.request()
                return True

            with (patch.object(target, "_start_staged_input", return_value=staged),
                  patch("catchuparr.engine.handover.matching_completed_segments", side_effect=interrupt)):
                result = target.prepare_synchronized_return(tail.id, stop, switch)
            self.assertIs(result, staged)
            self.assertTrue(staged.consumed)
            self.assertTrue(staged.process.ended)
            self.assertFalse(output.exists())
            with patch("catchuparr.engine.recorder.subprocess.Popen", side_effect=AssertionError("reopened")):
                outcome = target.run_candidate(stop, staged_input=staged, attempt_switch=switch)
            self.assertEqual(outcome.status, "switch_requested")
            self.assertEqual(outcome.attempt_switch_id, switch.attempt_id)
            self.assertEqual(len(store.segments("synthetic")), 1)

    def test_stale_or_missing_proof_uses_existing_staged_input_for_fallback(self):
        class Process:
            ended = False

            def poll(self):
                return 0 if self.ended else None

            def terminate(self):
                self.ended = True

            def wait(self, timeout=None):
                return 0

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "stage"
            output.mkdir()
            (output / "segment-000000.ts").write_bytes(b"synthetic")
            (output / "segments.csv").write_text("segment-000000.ts,0,2\n", encoding="utf-8")
            store = ArchiveStore(root / "archive")
            old = FFmpegCopyRecorder(store, "synthetic", "http://example.invalid/old", root / "work")
            target = FFmpegCopyRecorder(store, "synthetic", "http://example.invalid/new", root / "work")
            staged = StagedRecorderInput(target, output, Process())
            switch = RecorderAttemptSwitch()
            with (patch.object(target, "_start_staged_input", return_value=staged),
                  patch("catchuparr.engine.recorder.SYNCHRONIZED_HANDOVER_TIMEOUT_SECONDS", 0)):
                result, _offset, _count = old._try_synchronized_handover(
                    target, Process(), root / "old.csv", 0, threading.Event(), switch, lambda _: None,
                )
            self.assertIs(result, staged)
            self.assertTrue(switch.is_set())
            self.assertFalse(staged.matched)
            self.assertEqual(staged.offset, (output / "segments.csv").stat().st_size)
            staged.close()

    def test_segment_published_while_probing_invalidates_the_overlap(self):
        class Process:
            def poll(self):
                return None

            def terminate(self):
                pass

            def wait(self, timeout=None):
                return 0

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "stage"
            output.mkdir()
            media = output / "segment-000000.ts"
            media.write_bytes(b"synthetic")
            (output / "segments.csv").write_text("segment-000000.ts,0,2\n", encoding="utf-8")
            store = ArchiveStore(root / "archive")
            old = FFmpegCopyRecorder(store, "synthetic", "http://example.invalid/old", root / "work")
            target = FFmpegCopyRecorder(store, "synthetic", "http://example.invalid/new", root / "work")
            started = datetime.now(timezone.utc)
            first = store.add_segment("synthetic", media, started, started + timedelta(seconds=2))
            old._current_attempt_segment_id = first.id
            old._last_commit_monotonic = time.monotonic()
            staged = StagedRecorderInput(target, output, Process())
            switch = RecorderAttemptSwitch()

            def later_segment(*_paths, **_kwargs):
                store.add_segment("synthetic", media, first.end_utc, first.end_utc + timedelta(seconds=2))
                return True

            with (patch.object(target, "_start_staged_input", return_value=staged),
                  patch("catchuparr.engine.handover.matching_completed_segments", side_effect=later_segment),
                  patch("catchuparr.engine.recorder.SYNCHRONIZED_HANDOVER_TIMEOUT_SECONDS", 0)):
                result, _, _ = old._try_synchronized_handover(
                    target, Process(), root / "old.csv", 0, threading.Event(), switch, lambda _: None,
                )
            self.assertIs(result, staged)
            self.assertTrue(switch.is_set())
            self.assertFalse(staged.matched)
            staged.close()

    def test_owner_stop_during_overlap_proof_reaps_the_staged_reader(self):
        class Process:
            ended = False

            def poll(self):
                return 0 if self.ended else None

            def terminate(self):
                self.ended = True

            def wait(self, timeout=None):
                return 0

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "stage"
            output.mkdir()
            media = output / "segment-000000.ts"
            media.write_bytes(b"synthetic")
            (output / "segments.csv").write_text("segment-000000.ts,0,2\n", encoding="utf-8")
            store = ArchiveStore(root / "archive")
            old = FFmpegCopyRecorder(store, "synthetic", "http://example.invalid/old", root / "work")
            target = FFmpegCopyRecorder(store, "synthetic", "http://example.invalid/new", root / "work")
            started = datetime.now(timezone.utc)
            first = store.add_segment("synthetic", media, started, started + timedelta(seconds=2))
            old._current_attempt_segment_id = first.id
            old._last_commit_monotonic = time.monotonic()
            staged = StagedRecorderInput(target, output, Process())
            stop, switch = threading.Event(), RecorderAttemptSwitch()

            def stop_during_proof(*_paths, **_kwargs):
                stop.set()
                return True

            with (patch.object(target, "_start_staged_input", return_value=staged),
                  patch("catchuparr.engine.handover.matching_completed_segments", side_effect=stop_during_proof)):
                result, _, _ = old._try_synchronized_handover(
                    target, Process(), root / "old.csv", 0, stop, switch, lambda _: None,
                )
            self.assertIsNone(result)
            self.assertFalse(switch.is_set())
            self.assertTrue(staged.consumed)
            self.assertTrue(staged.process.ended)
            self.assertFalse(output.exists())
            self.assertEqual(len(store.segments("synthetic")), 1)

    def test_staged_reader_is_reaped_when_its_buffer_limit_is_exceeded(self):
        class Process:
            ended = False

            def poll(self):
                return 0 if self.ended else None

            def terminate(self):
                self.ended = True

            def wait(self, timeout=None):
                return 0

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "stage"
            output.mkdir()
            (output / "segment-000000.ts").write_bytes(b"synthetic")
            staged = StagedRecorderInput(object(), output, Process())
            with patch("catchuparr.engine.recorder.STAGED_INPUT_MAX_BYTES", 1):
                self.assertFalse(staged.within_limits())
            self.assertTrue(staged.consumed)
            self.assertTrue(staged.process.ended)
            self.assertFalse(output.exists())

    def test_stop_before_staged_adoption_does_not_flush_unpublished_media(self):
        class Process:
            ended = False

            def poll(self):
                return 0 if self.ended else None

            def terminate(self):
                self.ended = True

            def wait(self, timeout=None):
                return 0

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "stage"
            output.mkdir()
            store = ArchiveStore(root / "archive")
            target = FFmpegCopyRecorder(store, "synthetic", "http://example.invalid/new", root / "work")
            staged = StagedRecorderInput(target, output, Process(), expected_start=2, matched=True)
            stop = threading.Event()
            stop.set()
            with patch.object(target, "_publish_csv_rows") as publish:
                result = target.run_candidate(stop, staged_input=staged)
            self.assertEqual(result.status, "stopped")
            publish.assert_not_called()
            self.assertTrue(staged.process.ended)
            self.assertFalse(output.exists())

    def test_two_running_ffmpeg_readers_continue_from_a_proven_segment_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.ts"
            subprocess.run([
                "ffmpeg", "-v", "error", "-f", "lavfi", "-i",
                "testsrc2=size=64x64:rate=25", "-f", "lavfi", "-i",
                "sine=frequency=440:sample_rate=48000", "-t", "12",
                "-c:v", "mpeg2video", "-g", "25", "-c:a", "mp2", "-f", "mpegts", str(source),
            ], check=True, capture_output=True, timeout=30)
            old_fifo, new_fifo = root / "old.pipe", root / "new.pipe"
            os.mkfifo(old_fifo)
            os.mkfifo(new_fifo)
            store = ArchiveStore(root / "archive")
            old = FFmpegCopyRecorder(store, "synthetic", str(old_fifo), root / "work", segment_seconds=4)
            target = FFmpegCopyRecorder(store, "synthetic", str(new_fifo), root / "work", segment_seconds=4)
            stop, switch = threading.Event(), RecorderAttemptSwitch()
            result = []
            feeder_processes = []

            def feed(path):
                process = subprocess.Popen([
                    "ffmpeg", "-v", "error", "-re", "-i", str(source), "-map", "0", "-c", "copy",
                    "-f", "mpegts", "-y", str(path),
                ], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                feeder_processes.append(process)

            def record():
                result.append(old.run_candidate(stop, attempt_switch=switch, poll_interval=0.1))

            worker = threading.Thread(target=record)
            worker.start()
            feed(old_fifo)
            try:
                time.sleep(0.5)
                self.assertTrue(switch.offer_synchronized_input(target))
                feed(new_fifo)
                worker.join(timeout=15)
                self.assertFalse(worker.is_alive(), "the first recorder did not yield after its staged overlap")
                self.assertEqual(len(result), 1)
                staged = result[0].staged_input
                self.assertIsNotNone(staged)
                self.assertTrue(staged.matched, "the two live processes did not prove their common A/V segment")
                before = store.segments("synthetic")
                self.assertTrue(before)
                self.assertEqual(result[0].status, "switch_requested")
                resumed = target.run_candidate(stop, staged_input=staged, poll_interval=0.1)
                self.assertGreater(resumed.useful_segments, 0)
                after = store.segments("synthetic")
                self.assertEqual(after[len(before)].start_utc, before[-1].end_utc)
                self.assertTrue(after[len(before)].discontinuity)
                for stream in ("v:0", "a:0"):
                    def packet_hashes(path):
                        probe = subprocess.run([
                            "ffprobe", "-v", "error", "-select_streams", stream,
                            "-show_packets", "-show_data_hash", "sha256",
                            "-show_entries", "packet=data_hash", "-of", "json", str(path),
                        ], check=True, capture_output=True, timeout=30)
                        return [packet["data_hash"] for packet in json.loads(probe.stdout)["packets"]]

                    recorded = [digest for segment in after for digest in packet_hashes(segment.path)]
                    self.assertEqual(recorded, packet_hashes(source), f"{stream} repeated or lost packets")
            finally:
                stop.set()
                for process in feeder_processes:
                    if process.poll() is None:
                        process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                worker.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
