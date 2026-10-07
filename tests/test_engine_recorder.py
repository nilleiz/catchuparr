import json
import shutil
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch
from datetime import datetime, timezone
from pathlib import Path

from catchuparr.engine import ArchiveStore
from catchuparr.engine.recorder import (
    FFmpegCopyRecorder,
    has_useful_transport_stream,
)


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

    def test_internal_capability_is_sent_as_header_not_url(self):
        recorder = FFmpegCopyRecorder(
            self.store,
            "ch1",
            "http://dispatcharr/catchuparr/recorder/ch1",
            self.root / "work",
            input_headers={"X-Catchuparr-Recorder": "opaque-capability"},
        )
        args = recorder.command(self.root / "output")
        header_index = args.index("-headers")
        input_index = args.index("-i")

        self.assertLess(header_index, input_index)
        self.assertEqual("X-Catchuparr-Recorder: opaque-capability\r\n", args[header_index + 1])
        self.assertEqual("http://dispatcharr/catchuparr/recorder/ch1", args[input_index + 1])
        self.assertNotIn("opaque-capability", args[input_index + 1])

    def test_input_headers_reject_line_break_injection(self):
        with self.assertRaisesRegex(ValueError, "header value"):
            FFmpegCopyRecorder(
                self.store,
                "ch1",
                "http://dispatcharr/catchuparr/recorder/ch1",
                self.root / "work",
                input_headers={"X-Catchuparr-Recorder": "cap\r\nX-Other: forged"},
            )

    @staticmethod
    def _ts_packet(pid: int, *, start: bool = False) -> bytes:
        packet = bytearray(b"\xff" * 188)
        packet[0] = 0x47
        packet[1] = ((pid >> 8) & 0x1F) | (0x40 if start else 0)
        packet[2] = pid & 0xFF
        packet[3] = 0x10
        packet[4:8] = b"\x00\x00\x01\xc0"
        return bytes(packet)

    def test_useful_media_detection_rejects_null_keepalives(self):
        media = self.root / "media.ts"
        media.write_bytes(self._ts_packet(256, start=True) + self._ts_packet(256))
        keepalive = self.root / "keepalive.ts"
        keepalive.write_bytes(self._ts_packet(0x1FFF, start=True) + self._ts_packet(0x1FFF))

        self.assertTrue(has_useful_transport_stream(media))
        self.assertFalse(has_useful_transport_stream(keepalive))

    def test_policy_attempt_indexes_useful_segments_and_discards_keepalive_segments(self):
        out = self.root / "policy-attempt"
        out.mkdir()
        useful_path = out / "useful.ts"
        useful_path.write_bytes(self._ts_packet(256, start=True) + self._ts_packet(256))
        null_path = out / "null.ts"
        null_path.write_bytes(self._ts_packet(0x1FFF, start=True) + self._ts_packet(0x1FFF))
        listing = out / "segments.csv"
        listing.write_text(
            "useful.ts,0,6\nnull.ts,6,12\n", encoding="utf-8"
        )
        _, count = self.recorder._publish_csv_rows(
            listing, 0, require_useful_media=True
        )

        self.assertEqual(1, count)
        self.assertEqual(1, len(self.store.segments("ch1")))
        self.assertFalse(null_path.exists())

    def test_single_source_attempt_requires_a_useful_closed_segment(self):
        class FinishedProcess:
            def __init__(self, command, **_kwargs):
                output_path = Path(command[-1])
                listing = Path(command[command.index("-segment_list") + 1])
                output_path.write_bytes(
                    RecorderTests._ts_packet(256, start=True)
                    + RecorderTests._ts_packet(256)
                )
                listing.write_text(f"{output_path.name},0,6\n", encoding="utf-8")

            def poll(self):
                return 0

            def wait(self, timeout=None):
                return 0

        recorder = FFmpegCopyRecorder(
            self.store,
            "ch1",
            "http://dispatcharr/catchuparr/recorder/ch1",
            self.root / "candidate-work",
            require_media_progress=True,
        )
        with patch("catchuparr.engine.recorder.subprocess.Popen", FinishedProcess):
            result = recorder.run_candidate(threading.Event())

        self.assertEqual("exited", result.status)
        self.assertEqual(1, result.useful_segments)
        self.assertEqual(1, len(self.store.segments("ch1")))

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

    def test_pts_reset_reanchors_to_close_time_after_source_switch(self):
        import os

        out = self.root / "pts-reset"
        out.mkdir()
        base = datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()
        rows = [(0, 6, 6), (6, 12, 12), (0, 6, 18)]
        listing = out / "segments.csv"
        listing.write_text(
            "".join(f"segment-{index}.ts,{start},{end}\n" for index, (start, end, _) in enumerate(rows)),
            encoding="utf-8",
        )
        for index, (_start, _end, close_second) in enumerate(rows):
            media = out / f"segment-{index}.ts"
            media.write_bytes(b"synthetic ts")
            os.utime(media, (base + close_second, base + close_second))
        _, count = self.recorder._publish_csv_rows(listing, 0)
        self.assertEqual(count, 3)
        published = self.store.segments("ch1")
        self.assertEqual([base, base + 6, base + 12], [item.start_utc.timestamp() for item in published])
        self.assertTrue(published[-1].discontinuity)

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

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg and ffprobe are required")
    def test_private_data_pid_does_not_abort_recording_or_drop_audio_tracks(self):
        data = self.root / "private-data.bin"
        data.write_bytes(b"private transport data")
        source = self.root / "source-with-data.ts"
        subprocess.run([
            "ffmpeg", "-y", "-nostdin", "-hide_banner", "-loglevel", "error",
            "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=25",
            "-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=48000",
            "-f", "lavfi", "-i", "sine=frequency=1200:sample_rate=48000",
            "-f", "data", "-i", str(data),
            "-t", "3", "-map", "0", "-map", "1", "-map", "2", "-map", "3",
            "-c:v", "mpeg2video", "-c:a", "mp2", "-c:d", "copy",
            "-f", "mpegts", str(source),
        ], check=True, timeout=30)
        recorder = FFmpegCopyRecorder(
            self.store, "data-pid", str(source), self.root / "data-work", segment_seconds=2,
        )
        self.assertGreaterEqual(recorder.run_once(threading.Event()), 1)
        recorded = self.store.segments("data-pid")
        probe = subprocess.check_output([
            "ffprobe", "-v", "error", "-show_streams", "-of", "json", str(recorded[0].path),
        ], text=True, timeout=15)
        stream_types = [stream["codec_type"] for stream in json.loads(probe)["streams"]]
        self.assertEqual(stream_types.count("video"), 1)
        self.assertEqual(stream_types.count("audio"), 2)
        self.assertNotIn("data", stream_types)


if __name__ == "__main__":
    unittest.main()
