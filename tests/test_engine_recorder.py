import shutil
import subprocess
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from catchuparr.engine import ArchiveStore
from catchuparr.engine.recorder import FFmpegCopyRecorder


class RecorderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = ArchiveStore(self.root / "archive")
        self.recorder = FFmpegCopyRecorder(self.store, "ch1", "http://dispatcharr/proxy/ts/stream/id", self.root / "work")

    def tearDown(self):
        self.temp.cleanup()

    def test_command_uses_dispatcharr_source_and_stream_copy_segment_muxer(self):
        args = self.recorder.command(self.root / "output")
        self.assertIn("http://dispatcharr/proxy/ts/stream/id", args)
        self.assertIn("copy", args)
        self.assertIn("segment", args)
        self.assertIn("-segment_list_type", args)
        self.assertNotIn("-reset_timestamps", args)
        self.assertNotIn("-vf", args)
        self.assertNotIn("-af", args)

    def test_completed_csv_rows_are_indexed_only_after_file_exists(self):
        out = self.root / "session"
        out.mkdir()
        media = out / "segment-000000.ts"
        media.write_bytes(b"synthetic ts")
        listing = out / "segments.csv"
        listing.write_text('segment-000000.ts,0.000000,6.000000\npartial,6.000000,12.000000', encoding="utf-8")
        anchor = datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()
        offset, count = self.recorder._publish_csv_rows(listing, 0, anchor)
        self.assertEqual(1, count)
        self.assertEqual(len('segment-000000.ts,0.000000,6.000000\n'), offset)
        segments = self.store.segments("ch1")
        self.assertEqual(1, len(segments))
        self.assertEqual(datetime(2026, 1, 1, tzinfo=timezone.utc), segments[0].start_utc)
        self.assertFalse(media.exists())
        offset2, count2 = self.recorder._publish_csv_rows(listing, offset, anchor)
        self.assertEqual((offset, 0), (offset2, count2))

    def test_utc_anchor_uses_closed_segment_mtime_after_proxy_open(self):
        out = self.root / "anchored-session"
        out.mkdir()
        media = out / "segment-000000.ts"
        media.write_bytes(b"synthetic ts")
        closed_at = datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc).timestamp()
        import os
        os.utime(media, (closed_at, closed_at))
        listing = out / "segments.csv"
        listing.write_text("segment-000000.ts,0.000000,6.000000\n", encoding="utf-8")
        self.recorder._mark_next_discontinuity = True
        _, count = self.recorder._publish_csv_rows(listing, 0)
        self.assertEqual(1, count)
        segment = self.store.segments("ch1")[0]
        self.assertEqual(datetime(2026, 1, 1, 0, 0, 4, tzinfo=timezone.utc), segment.start_utc)
        self.assertTrue(segment.discontinuity)

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg and ffprobe are required")
    def test_real_ffmpeg_stream_copy_keeps_pts_monotonic_across_segments(self):
        source = self.root / "synthetic.ts"
        subprocess.run([
            "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error",
            "-f", "lavfi", "-i", "testsrc=size=160x90:rate=25",
            "-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=48000",
            "-t", "6", "-c:v", "mpeg2video", "-g", "25", "-bf", "0",
            "-c:a", "mp2", "-f", "mpegts", str(source),
        ], check=True, timeout=30)
        real_recorder = FFmpegCopyRecorder(
            self.store, "ffmpeg-test", str(source), self.root / "ffmpeg-work", segment_seconds=2,
        )
        self.assertGreaterEqual(real_recorder.run_once(threading.Event()), 2)
        self.assertEqual([], list((self.root / "ffmpeg-work").iterdir()))
        segments = self.store.segments("ffmpeg-test")
        first_pts = []
        for segment in segments:
            output = subprocess.check_output([
                "ffprobe", "-v", "error", "-select_streams", "v:0",
                "-show_entries", "packet=pts_time", "-of", "csv=p=0", str(segment.path),
            ], text=True, timeout=15)
            pts = [float(line.split(",")[0]) for line in output.splitlines() if line.strip()]
            self.assertTrue(pts, f"no video PTS in {segment.path}")
            first_pts.append(pts[0])
        self.assertEqual(first_pts, sorted(first_pts))
        self.assertTrue(all(b > a for a, b in zip(first_pts, first_pts[1:])))


if __name__ == "__main__":
    unittest.main()
