import json
import random
import shutil
import subprocess
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catchuparr.engine.offset import ENVELOPE_HZ, repeated_seconds
from catchuparr.engine.recorder import FFmpegCopyRecorder, StagedRecorderInput
from catchuparr.engine.store import ArchiveStore

FFMPEG = bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))


def speech_like(seconds, seed):
    rng = random.Random(seed)
    values, level = [], 0.3
    for _ in range(int(seconds * ENVELOPE_HZ)):
        level = max(0.0, min(1.0, level + rng.uniform(-0.15, 0.15)))
        values.append(level * 1000)
    return values


class EnvelopeMatchTests(unittest.TestCase):
    def test_repeated_start_is_measured(self):
        stream = speech_like(220, 1)
        archive = stream[: 120 * ENVELOPE_HZ]
        new = stream[50 * ENVELOPE_HZ: 100 * ENVELOPE_HZ]  # starts 70 s before the archive ends
        self.assertAlmostEqual(repeated_seconds(archive, new), 70.0, delta=0.15)

    def test_short_repeat_is_measured(self):
        stream = speech_like(220, 2)
        archive = stream[: 120 * ENVELOPE_HZ]
        new = stream[113 * ENVELOPE_HZ: 160 * ENVELOPE_HZ]
        self.assertAlmostEqual(repeated_seconds(archive, new), 7.0, delta=0.15)

    def test_new_input_ahead_of_the_archive_has_no_match(self):
        stream = speech_like(300, 3)
        self.assertIsNone(repeated_seconds(stream[: 120 * ENVELOPE_HZ], stream[150 * ENVELOPE_HZ: 190 * ENVELOPE_HZ]))

    def test_silence_and_periodic_content_are_not_matched(self):
        self.assertIsNone(repeated_seconds([0.0] * 2400, [0.0] * 400))
        periodic = [(i % 40) * 10.0 for i in range(2400)]
        self.assertIsNone(repeated_seconds(periodic, periodic[:400]))


@unittest.skipUnless(FFMPEG, "requires FFmpeg")
class AudioOffsetTrimTests(unittest.TestCase):
    def packets(self, path, stream):
        out = subprocess.run([
            "ffprobe", "-v", "error", "-select_streams", stream, "-show_entries", "packet=pts_time",
            "-of", "json", str(path),
        ], check=True, capture_output=True, timeout=30)
        return [float(p["pts_time"]) for p in json.loads(out.stdout)["packets"]]

    def test_delayed_input_repeating_the_archive_is_trimmed_by_audio(self):
        class Process:
            def poll(self):
                return 0

            def terminate(self):
                pass

            def wait(self, timeout=None):
                return 0

        audio = ("anoisesrc=r=48000:d=150:a=0.3,"
                 "volume=volume='0.05+mod(abs(sin(floor(t*4)*12.9898)*43758.5453),1)':eval=frame")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.ts"
            subprocess.run([
                "ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=64x64:rate=25", "-f", "lavfi", "-i", audio,
                "-t", "150", "-c:v", "mpeg2video", "-g", "25", "-c:a", "mp2", "-f", "mpegts", str(source),
            ], check=True, capture_output=True, timeout=120)
            archive_dir = root / "old"
            archive_dir.mkdir()
            subprocess.run([
                "ffmpeg", "-v", "error", "-i", str(source), "-t", "99.9", "-map", "0", "-c", "copy",
                "-f", "segment", "-segment_time", "6", "-segment_format", "mpegts", str(archive_dir / "old-%03d.ts"),
            ], check=True, capture_output=True, timeout=60)
            store = ArchiveStore(root / "archive")
            started = datetime.now(timezone.utc) - timedelta(seconds=150)
            clock = started
            for part in sorted(archive_dir.glob("old-*.ts")):
                probe = subprocess.run([
                    "ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(part),
                ], capture_output=True, text=True, timeout=30)
                length = float(probe.stdout.strip())
                store.add_segment("synthetic", part, clock, clock + timedelta(seconds=length))
                clock += timedelta(seconds=length)
            archived_end = store.segments("synthetic")[-1].end_utc
            # A second provider: other encoder settings, 60 s behind the archive end.
            stage = root / "stage"
            stage.mkdir()
            subprocess.run([
                "ffmpeg", "-v", "error", "-ss", "40", "-i", str(source), "-map", "0:v", "-map", "0:a",
                "-c:v", "mpeg2video", "-g", "25", "-b:v", "300k", "-c:a", "mp2", "-b:a", "96k",
                "-f", "segment", "-segment_time", "6", "-segment_format", "mpegts",
                "-segment_list", str(stage / "segments.csv"), "-segment_list_type", "csv",
                str(stage / "segment-%06d.ts"),
            ], check=True, capture_output=True, timeout=120)
            target = FFmpegCopyRecorder(store, "synthetic", "http://example.invalid/new", root / "work")
            staged = StagedRecorderInput(target, stage, Process())
            resumed = target.run_candidate(threading.Event(), staged_input=staged)
            self.assertGreater(resumed.useful_segments, 0)
            segments = store.segments("synthetic")
            first_new = next(s for s in segments if s.start_utc >= archived_end)
            # Media continues where the archive ended: timeline stays continuous.
            self.assertAlmostEqual((first_new.start_utc - archived_end).total_seconds(), 0.0, delta=0.05)
            video = self.packets(first_new.path, "v:0")
            # The kept picture is the keyframe at or after 100 s of the source
            # (60 s into the delayed input); nothing earlier was archived again.
            self.assertGreaterEqual(video[0] - 1.4, 59.9)
            self.assertLessEqual(video[0] - 1.4, 62.0)


if __name__ == "__main__":
    unittest.main()
