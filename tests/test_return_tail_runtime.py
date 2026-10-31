import json
import math
import os
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr import sharing_tail
from catchuparr.engine.recorder import (
    FINAL_SEGMENT_MIN_SECONDS,
    FFmpegCopyRecorder,
    PipedFFmpegRecorder,
)
from catchuparr.engine.store import ArchiveStore
from catchuparr.sharing_return_tail import (
    ReturnRequest,
    TailPhase,
    plan_return,
)

CHANNEL = "00000000-0000-0000-0000-000000000012"
IDENTITY = (CHANNEL, "worker-1", "owner-1", "owner-1", "stream-1", "account-delayed", "m3u-1", "profile-1")
HAS_FFMPEG = bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))


class _ChunkSource:
    def __init__(self, chunks, fail_after=None):
        self.chunks = list(chunks)
        self.fail_after = fail_after
        self.activity = 0

    def iter_chunks(self, stop):
        for index, chunk in enumerate(self.chunks):
            if stop.is_set():
                return
            if self.fail_after is not None and index >= self.fail_after:
                raise RuntimeError("synthetic binding lost")
            yield chunk

    def record_activity(self):
        self.activity += 1


class _ProcessSource:
    """Chunks a real-time FFmpeg remux of a synthetic file, like a live buffer."""

    def __init__(self, source, seek):
        self.process = subprocess.Popen([
            "ffmpeg", "-v", "error", "-re", "-ss", str(seek), "-i", str(source), "-map", "0",
            "-c", "copy", "-f", "mpegts", "pipe:1",
        ], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)

    def iter_chunks(self, stop):
        while not stop.is_set():
            chunk = self.process.stdout.read(188 * 64)
            if not chunk:
                return
            yield chunk

    def record_activity(self):
        pass

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
        self.process.wait(timeout=5)
        self.process.stdout.close()


def _synthetic_source(path: Path, seconds: int) -> None:
    subprocess.run([
        "ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=64x64:rate=25",
        "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", str(seconds),
        "-c:v", "mpeg2video", "-g", "25", "-c:a", "mp2", "-f", "mpegts", str(path),
    ], check=True, capture_output=True, timeout=30)


def _hashes(path: Path, stream: str) -> list[str]:
    probe = subprocess.run([
        "ffprobe", "-v", "error", "-select_streams", stream, "-show_packets",
        "-show_data_hash", "sha256", "-show_entries", "packet=data_hash", "-of", "json", str(path),
    ], check=True, capture_output=True, timeout=30)
    return [packet["data_hash"] for packet in json.loads(probe.stdout)["packets"]]


class PipedRecorderTests(unittest.TestCase):
    def test_command_reads_mpegts_from_stdin(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = PipedFFmpegRecorder(
                ArchiveStore(Path(directory) / "archive"), "synthetic", _ChunkSource([]), Path(directory),
            )
            command = recorder.command(Path(directory) / "out")
        self.assertNotIn("-nostdin", command)
        index = command.index("-i")
        self.assertEqual(command[index - 2:index + 2], ["-f", "mpegts", "-i", "pipe:0"])
        self.assertNotIn("-headers", command)

    def test_source_must_provide_chunks_and_activity(self):
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(ValueError):
            PipedFFmpegRecorder(ArchiveStore(Path(directory)), "synthetic", object(), Path(directory))

    @unittest.skipUnless(HAS_FFMPEG, "requires FFmpeg")
    def test_one_process_is_fed_and_its_input_closes_when_the_source_ends(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _synthetic_source(root / "source.ts", 4)
            data = (root / "source.ts").read_bytes()
            chunks = [data[i:i + 188 * 64] for i in range(0, len(data), 188 * 64)]
            source = _ChunkSource(chunks)
            recorder = PipedFFmpegRecorder(
                ArchiveStore(root / "archive"), "synthetic", source, root / "work", segment_seconds=2,
            )
            staged = recorder._start_staged_input()
            try:
                staged.process.wait(timeout=20)
                recorder.feeder.join(timeout=5)
                self.assertEqual(source.activity, len(chunks))
                rows = (staged.output_dir / "segments.csv").read_text().splitlines()
                self.assertGreaterEqual(len(rows), 1)
                with self.assertRaises(RuntimeError):
                    recorder._popen(root / "again")
            finally:
                staged.close()

    @unittest.skipUnless(HAS_FFMPEG, "requires FFmpeg")
    def test_a_failing_source_ends_the_ffmpeg_input(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _synthetic_source(root / "source.ts", 4)
            data = (root / "source.ts").read_bytes()
            chunks = [data[i:i + 188 * 64] for i in range(0, len(data), 188 * 64)]
            recorder = PipedFFmpegRecorder(
                ArchiveStore(root / "archive"), "synthetic", _ChunkSource(chunks, fail_after=3), root / "work",
            )
            staged = recorder._start_staged_input()
            try:
                self.assertIsNotNone(staged.process.wait(timeout=20))
            finally:
                staged.close()


@unittest.skipUnless(HAS_FFMPEG, "requires FFmpeg")
class TailJoinTests(unittest.TestCase):
    def test_tail_staged_before_the_borrow_ends_joins_without_repeated_or_lost_pictures(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.ts"
            _synthetic_source(source, 14)
            fifo = root / "borrow.pipe"
            os.mkfifo(fifo)
            store = ArchiveStore(root / "archive")
            borrow = FFmpegCopyRecorder(store, "synthetic", str(fifo), root / "work", segment_seconds=4)
            borrow.final_segment_min_seconds = math.inf
            tail_source = None
            feeder = None
            stop = threading.Event()
            result = []
            worker = threading.Thread(target=lambda: result.append(
                borrow.run_candidate(stop, poll_interval=0.1)))
            worker.start()
            try:
                # The borrowed reader ends at its return deadline, mid segment.
                feeder = subprocess.Popen([
                    "ffmpeg", "-v", "error", "-re", "-i", str(source), "-t", "9", "-map", "0",
                    "-c", "copy", "-f", "mpegts", "-y", str(fifo),
                ], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                time.sleep(2.5)
                # The tail reads the same source from slightly earlier content on.
                tail_source = _ProcessSource(source, 2)
                tail = PipedFFmpegRecorder(store, "synthetic", tail_source, root / "work", segment_seconds=4)
                staged = tail._start_staged_input()
                staged.max_seconds = 60
                worker.join(timeout=30)
                self.assertFalse(worker.is_alive())
                before = store.segments("synthetic")
                self.assertTrue(before)
                self.assertTrue(tail.join_staged_after_archive_tail(staged, before[-1].id, stop, timeout=15))
                resumed = tail.run_candidate(stop, staged_input=staged, poll_interval=0.1)
                self.assertGreater(resumed.useful_segments, 0)
                after = store.segments("synthetic")
                self.assertEqual(after[len(before)].start_utc, before[-1].end_utc)
                recorded = [digest for segment in after for digest in _hashes(segment.path, "v:0")]
                reference = _hashes(source, "v:0")
                self.assertGreater(len(recorded), len(_hashes(before[-1].path, "v:0")) * len(before))
                self.assertEqual(recorded, reference[:len(recorded)], "pictures repeated or lost")
                audio = [digest for segment in after for digest in _hashes(segment.path, "a:0")]
                self.assertEqual(len(audio), len(set(audio)), "audio frames repeated")
            finally:
                stop.set()
                for process in (feeder,):
                    if process is not None and process.poll() is None:
                        process.terminate()
                        process.wait(timeout=5)
                if tail_source is not None:
                    tail_source.close()
                worker.join(timeout=5)

    def test_join_rejects_a_stale_archive_tail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ArchiveStore(root / "archive")
            tail = PipedFFmpegRecorder(store, "synthetic", _ChunkSource([]), root / "work")
            staged = SimpleNamespace()
            self.assertFalse(tail.join_staged_after_archive_tail(staged, "missing", threading.Event()))


class _FakeStaged:
    def __init__(self):
        self.max_seconds = 30.0
        self.closed = False
        self.matched = False
        self.frozen = False

    def close(self):
        self.closed = True


class _FakeReader:
    def __init__(self, close_results=(True,)):
        self.client_id = "catchuparr-client-" + "a" * 32
        self.close_results = list(close_results)
        self.closes = 0

    def close(self):
        self.closes += 1
        return self.close_results.pop(0) if self.close_results else True


class _FakeTailRecorder:
    def __init__(self, joined=True):
        self.staged = _FakeStaged()
        self.joined = joined
        self.joins = []

    def _start_staged_input(self):
        return self.staged

    def join_staged_after_archive_tail(self, staged, previous_segment_id, stop_event, *, timeout):
        self.joins.append((previous_segment_id, timeout))
        staged.matched = self.joined
        return self.joined


class ReturnTailControllerTests(unittest.TestCase):
    def make(self, *, now, deadline, reader=None, recorder=None, **overrides):
        clock = {"now": now}
        values = {
            "channel_uuid": CHANNEL, "tail_id": "tail-1", "recorder_fence": 3, "now": now,
            "return_deadline": deadline, "borrowed_account_id": "account-delayed",
            "dedicated_account_id": "account-fast", "delayed_account_ids": frozenset({"account-delayed"}),
            "max_overlap_seconds": 60, "synchronize_before_switch": True,
        }
        values.update(overrides)
        reader = reader or _FakeReader()
        recorder = recorder or _FakeTailRecorder()
        errors = []
        stop = threading.Event()
        tail = sharing_tail.ReturnTail(
            request_factory=lambda current: ReturnRequest(**{**values, "now": current}),
            make_reader=lambda _state: reader,
            make_recorder=lambda _reader: recorder,
            stop_event=stop, clock=lambda: clock["now"], on_error=errors.append,
        )
        return tail, clock, reader, recorder, errors, stop

    def test_waits_outside_the_lead_window_and_opens_inside_it(self):
        tail, clock, _reader, recorder, _errors, _stop = self.make(now=1000.0, deadline=1030.0)
        borrow = SimpleNamespace(final_segment_min_seconds=FINAL_SEGMENT_MIN_SECONDS)
        tail.poll(borrow, episode_phase="active")
        self.assertIsNone(tail.state)
        tail.poll(borrow, episode_phase="wait_return")
        self.assertIsNone(tail.state)
        clock["now"] = 1015.0
        tail.poll(borrow, episode_phase="wait_return")
        self.assertEqual(tail.state.phase, TailPhase.JOINING)
        self.assertTrue(tail.staged_ready)
        self.assertEqual(borrow.final_segment_min_seconds, math.inf)
        self.assertGreater(recorder.staged.max_seconds, 30.0)

    def test_ineligible_return_is_planned_once_and_never_opens(self):
        tail, clock, reader, _recorder, _errors, _stop = self.make(
            now=1015.0, deadline=1030.0, dedicated_account_id="account-delayed",
        )
        borrow = SimpleNamespace(final_segment_min_seconds=FINAL_SEGMENT_MIN_SECONDS)
        tail.poll(borrow, episode_phase="wait_return")
        tail.poll(borrow, episode_phase="wait_return")
        self.assertEqual(tail.state.phase, TailPhase.IDLE)
        self.assertEqual(reader.closes, 0)
        self.assertEqual(borrow.final_segment_min_seconds, FINAL_SEGMENT_MIN_SECONDS)

    def test_viewer_return_closes_the_tail_and_restores_the_final_segment(self):
        tail, clock, reader, recorder, _errors, _stop = self.make(now=1015.0, deadline=1030.0)
        borrow = SimpleNamespace(final_segment_min_seconds=FINAL_SEGMENT_MIN_SECONDS)
        tail.poll(borrow, episode_phase="wait_return")
        clock["now"] = 1020.0
        tail.poll(borrow, episode_phase="active")
        self.assertEqual(tail.state.phase, TailPhase.CLOSED)
        self.assertEqual(tail.state.outcome, "viewer_returned")
        self.assertTrue(recorder.staged.closed)
        self.assertEqual(reader.closes, 1)
        self.assertEqual(borrow.final_segment_min_seconds, FINAL_SEGMENT_MIN_SECONDS)

    def test_join_after_the_deadline_hands_publication_to_the_tail(self):
        tail, clock, _reader, recorder, _errors, _stop = self.make(now=1015.0, deadline=1030.0)
        borrow = SimpleNamespace(final_segment_min_seconds=FINAL_SEGMENT_MIN_SECONDS)
        tail.poll(borrow, episode_phase="wait_return")
        clock["now"] = 1031.0
        self.assertTrue(tail.join("return", "segment-7"))
        self.assertEqual(tail.state.phase, TailPhase.TAILING)
        self.assertEqual(recorder.joins, [("segment-7", 1030.0 + 12.0 - 1031.0)])

    def test_failed_join_or_other_outcome_closes_the_tail(self):
        for outcome, joined in (("return", False), ("stop", True), ("failure", True)):
            with self.subTest(outcome=outcome, joined=joined):
                tail, clock, reader, recorder, _errors, _stop = self.make(
                    now=1015.0, deadline=1030.0, recorder=_FakeTailRecorder(joined=joined),
                )
                tail.poll(SimpleNamespace(final_segment_min_seconds=1.0), episode_phase="wait_return")
                clock["now"] = 1031.0
                self.assertFalse(tail.join(outcome, "segment-7"))
                self.assertEqual(tail.state.phase, TailPhase.CLOSED)
                self.assertTrue(recorder.staged.closed)
                self.assertEqual(reader.closes, 1)

    def test_unacknowledged_close_stops_the_recorder(self):
        reader = _FakeReader(close_results=[False] * 100)
        tail, clock, _reader, _recorder, errors, stop = self.make(now=1015.0, deadline=1030.0, reader=reader)
        tail.poll(SimpleNamespace(final_segment_min_seconds=1.0), episode_phase="wait_return")
        clock["now"] = 1100.0

        def advance(seconds):
            clock["now"] += seconds

        with patch.object(sharing_tail.time, "sleep", side_effect=advance):
            tail.close()
        self.assertLess(reader.closes, 100)
        self.assertTrue(stop.is_set())
        self.assertIn("sharing_tail_cleanup_pending", errors)
        self.assertEqual(tail.state.phase, TailPhase.CLEANUP_PENDING)

    def test_failed_open_abandons_the_tail(self):
        def broken(_state):
            raise RuntimeError("synthetic registration failure")

        tail, clock, _reader, _recorder, _errors, _stop = self.make(now=1015.0, deadline=1030.0)
        tail.make_reader = broken
        borrow = SimpleNamespace(final_segment_min_seconds=FINAL_SEGMENT_MIN_SECONDS)
        tail.poll(borrow, episode_phase="wait_return")
        self.assertEqual(tail.state.phase, TailPhase.CLOSED)
        self.assertEqual(tail.state.outcome, "join_failed")
        self.assertEqual(borrow.final_segment_min_seconds, FINAL_SEGMENT_MIN_SECONDS)

    def running_tail(self, *, accept=True):
        class Recorder(_FakeTailRecorder):
            def run_candidate(self, stop_event, *, startup_timeout, media_idle_timeout,
                              attempt_switch, staged_input):
                self.adopted = staged_input
                while not stop_event.is_set():
                    target = attempt_switch.take_synchronized_input() if accept else None
                    if target is not None:
                        self.target = target
                        staged = _FakeStaged()
                        staged.frozen = True
                        attempt_switch.request()
                        return SimpleNamespace(status="switch_requested", staged_input=staged)
                    if attempt_switch.is_set():
                        return SimpleNamespace(status="switch_requested", staged_input=None)
                    time.sleep(0.01)
                return SimpleNamespace(status="stopped", staged_input=None)

        recorder = Recorder()
        tail, clock, reader, _recorder, errors, stop = self.make(
            now=1015.0, deadline=1030.0, recorder=recorder,
        )
        tail.poll(SimpleNamespace(final_segment_min_seconds=1.0), episode_phase="wait_return")
        clock["now"] = 1031.0
        self.assertTrue(tail.join("return", "segment-7"))
        tail.start(startup_timeout=1.0, media_idle_timeout=1.0)
        tail.borrow_cleaned()
        return tail, clock, reader, recorder, errors, stop

    def test_hand_over_returns_the_seam_input_and_closes_the_tail(self):
        tail, _clock, reader, recorder, _errors, _stop = self.running_tail()
        dedicated = SimpleNamespace(handover_overlap_seconds=0.0)
        staged = tail.hand_over(dedicated, None)
        self.assertIsNotNone(staged)
        self.assertTrue(staged.frozen)
        self.assertIs(recorder.target, dedicated)
        self.assertIs(recorder.adopted, recorder.staged)
        self.assertEqual(dedicated.handover_overlap_seconds, 60 + 20)
        self.assertEqual(reader.closes, 1)
        self.assertEqual(tail.state.phase, TailPhase.CLOSED)
        self.assertEqual(tail.state.outcome, "seam")

    def test_hand_over_before_the_token_is_refused(self):
        tail, _clock, reader, _recorder, _errors, _stop = self.running_tail()
        tail.state = tail.state.__class__(**{**tail.state.__dict__, "borrow_cleaned": False})
        self.assertIsNone(tail.hand_over(SimpleNamespace(handover_overlap_seconds=0.0), None))
        self.assertEqual(reader.closes, 1)

    def test_interrupted_hand_over_closes_the_staged_input(self):
        tail, _clock, reader, _recorder, _errors, stop = self.running_tail()
        switch = SimpleNamespace(is_set=lambda: True)
        self.assertIsNone(tail.hand_over(SimpleNamespace(handover_overlap_seconds=0.0), switch))
        self.assertEqual(reader.closes, 1)
        self.assertFalse(stop.is_set())

    def test_hand_over_without_a_running_tail_returns_nothing(self):
        tail, *_ = self.make(now=1015.0, deadline=1030.0)
        self.assertIsNone(tail.hand_over(object(), None))


class _ProofRedis:
    def __init__(self, result=1):
        self.result = result
        self.calls = []

    def eval(self, script, numkeys, *args):
        self.calls.append((numkeys, args))
        return self.result


class NativeTailProofTests(unittest.TestCase):
    def reader(self, redis, *, now=100.0, current=True):
        clock = {"now": now}
        reader = sharing_tail.NativeTailReader(
            redis_text=redis, redis_binary=None, channel_uuid=CHANNEL, identity=IDENTITY,
            configuration_generation="config-a", control_generation=2, recorder_fence=3,
            lease_value="lease-1", stats_digest="digest-1", hard_deadline=200.0,
            generation_is_current=lambda: current, tail_id="tail-1", clock=lambda: clock["now"],
        )
        return reader, clock

    def test_proof_binds_the_borrowed_connection_and_the_lease(self):
        from catchuparr.adapters.sharing_reader import validate_borrow_read_proof

        redis = _ProofRedis()
        reader, _clock = self.reader(redis)
        proof = reader._proof()
        validate_borrow_read_proof(reader.binding, proof)
        numkeys, args = redis.calls[0]
        self.assertEqual(numkeys, 5)
        self.assertEqual(args[:5], (
            f"live:channel:{CHANNEL}:owner", f"live:channel:{CHANNEL}:metadata",
            "live:worker:owner-1:heartbeat", f"catchuparr:recorder:{CHANNEL}",
            f"live:channel:{CHANNEL}:stopping",
        ))
        self.assertEqual(args[5:], ("owner-1", "owner-1", "stream-1", "m3u-1", "profile-1", "lease-1"))

    def test_proof_fails_closed(self):
        from catchuparr.adapters.sharing_reader import BorrowBindingLost

        cases = {
            "native changed": dict(redis=_ProofRedis(0)),
            "deadline": dict(redis=_ProofRedis(), now=200.0),
            "generation": dict(redis=_ProofRedis(), current=False),
        }
        for name, kwargs in cases.items():
            with self.subTest(name):
                reader, _clock = self.reader(kwargs.pop("redis"), **kwargs)
                with self.assertRaises(BorrowBindingLost):
                    reader._proof()

    def test_reader_requires_an_exact_identity(self):
        with self.assertRaises(ValueError):
            sharing_tail.NativeTailReader(
                redis_text=None, redis_binary=None, channel_uuid=CHANNEL, identity=IDENTITY[:7],
                configuration_generation="config-a", control_generation=2, recorder_fence=3,
                lease_value="lease-1", stats_digest="digest-1", hard_deadline=200.0,
                generation_is_current=lambda: True, tail_id="tail-1",
            )


class _RecoveryRedis:
    def __init__(self, record):
        self.record = record
        self.evals = []
        self.deleted = []

    def exists(self, key):
        return int(self.record is not None)

    def hgetall(self, key):
        return dict(self.record or {})

    def eval(self, script, numkeys, *args):
        self.evals.append(args)
        return 1

    def delete(self, key):
        self.deleted.append(key)


class RecoveryTests(unittest.TestCase):
    def record(self):
        return {
            "tail_id": "tail-1", "client_id": "catchuparr-client-" + "b" * 32,
            "connected_at": "123.0", "worker_id": "catchuparr-reader-1", "user_agent": "agent",
            "ip_address": "127.0.0.1", "user_id": "0", "output_format": "mpegts", "output_profile_id": "",
        }

    def test_no_record_needs_no_cleanup(self):
        self.assertTrue(sharing_tail.recover_stale_tail(_RecoveryRedis(None), CHANNEL))

    def test_stale_tail_client_is_removed_exactly(self):
        redis = _RecoveryRedis(self.record())
        with patch.object(sharing_tail, "_unmark_private", return_value=True) as unmark:
            self.assertTrue(sharing_tail.recover_stale_tail(redis, CHANNEL, clock=lambda: 50.0))
        args = redis.evals[0]
        self.assertEqual(args[3], self.record()["client_id"])
        self.assertEqual(args[4:11], ("123.0", "catchuparr-reader-1", "agent", "127.0.0.1", "0", "mpegts", ""))
        unmark.assert_called_once_with(CHANNEL, self.record()["client_id"])
        self.assertEqual(redis.deleted, [sharing_tail.tail_record_key(CHANNEL)])

    def test_incomplete_record_is_dropped_without_touching_clients(self):
        redis = _RecoveryRedis({"tail_id": "tail-1", "client_id": "ordinary"})
        self.assertTrue(sharing_tail.recover_stale_tail(redis, CHANNEL))
        self.assertEqual(redis.evals, [])
        self.assertEqual(redis.deleted, [sharing_tail.tail_record_key(CHANNEL)])

    def test_redis_failure_fails_closed(self):
        class Broken(_RecoveryRedis):
            def hgetall(self, key):
                raise ConnectionError("synthetic")

        self.assertFalse(sharing_tail.recover_stale_tail(Broken(self.record()), CHANNEL))


class PlannerContractTests(unittest.TestCase):
    def test_runtime_lead_window_matches_the_planner(self):
        request = ReturnRequest(
            channel_uuid=CHANNEL, tail_id="tail-1", recorder_fence=1, now=0.0, return_deadline=15.0,
            borrowed_account_id="a", dedicated_account_id="b", delayed_account_ids=frozenset({"a"}),
            max_overlap_seconds=60, synchronize_before_switch=True,
        )
        self.assertEqual(plan_return(request).state.phase, TailPhase.OPENING)


if __name__ == "__main__":
    unittest.main()
