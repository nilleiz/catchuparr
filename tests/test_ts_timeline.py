import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from catchuparr.engine.ts_timeline import (
    BASE_CLOCK,
    CLOCK_HZ,
    CLOCK_WRAP,
    PlaybackClockMap,
    SegmentClock,
    _pes_clock,
    _read_pts,
    _write_pts,
    mapped_chunks,
)


class TransportClockTests(unittest.TestCase):
    def test_pts_wrap_roundtrip_preserves_prefix_and_marker_bits(self):
        for ticks in (0, 1, CLOCK_WRAP - 1, CLOCK_WRAP + 17):
            data = bytearray(b"\x31\x00\x01\x00\x01")
            _write_pts(data, 0, ticks)
            self.assertEqual(_read_pts(data, 0), ticks % CLOCK_WRAP)
            self.assertEqual(data[0] & 0xF1, 0x31)

    def test_malformed_or_split_pes_clock_is_rejected(self):
        with self.assertRaises(ValueError):
            _read_pts(b"\x00" * 5, 0)
        with self.assertRaises(ValueError):
            _read_pts(b"\x31\x00\x01", 0)

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "requires FFmpeg")
    def test_reset_sources_keep_programme_clock_and_arbitrary_byte_ranges(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "synthetic.ts"
            subprocess.run([
                "ffmpeg", "-v", "error", "-f", "lavfi", "-i",
                "testsrc2=size=64x64:rate=25", "-f", "lavfi", "-i",
                "sine=frequency=440:sample_rate=48000", "-t", "2",
                "-c:a", "mp2", "-c:v", "mpeg2video", "-bf", "2",
                "-f", "mpegts", str(source),
            ], check=True, capture_output=True)
            original = source.read_bytes()
            first = SegmentClock.probe(source, 0)
            following = SegmentClock.probe(source, 12)
            self.assertIsNotNone(first)
            chunks = []
            for clock in (first, following):
                with source.open("rb") as file:
                    chunks.append(b"".join(mapped_chunks(file, 0, len(original), clock)))
            combined = root / "programme.ts"
            combined.write_bytes(b"".join(chunks))
            stamps = []
            for data in chunks:
                values = [clock[3] for offset in range(0, len(data), 188)
                          if (clock := _pes_clock(data[offset:offset + 188])) is not None]
                stamps.append(values[0])
            self.assertEqual(stamps, [BASE_CLOCK, BASE_CLOCK + 12 * CLOCK_HZ])
            self.assertEqual(len(chunks[0]), len(original))
            self.assertTrue(first.discontinuity_packets)
            for packet_offset in first.discontinuity_packets:
                self.assertTrue(chunks[0][packet_offset + 5] & 0x80)
                self.assertEqual(
                    chunks[0][packet_offset:packet_offset + 5],
                    original[packet_offset:packet_offset + 5],
                )
            # Markers are based on absolute file offsets, including reads that
            # begin inside a TS header, not on the HTTP chunk boundaries.
            for marker in first.discontinuity_packets:
                with source.open("rb") as file:
                    part = b"".join(mapped_chunks(file, marker + 4, 9, first))
                self.assertEqual(part, chunks[0][marker + 4:marker + 13])
            from datetime import datetime, timedelta, timezone

            from catchuparr.engine.store import ArchiveStore
            from catchuparr.security import TokenStore
            from catchuparr.ts_http import ArchiveTSPlaybackService
            store = ArchiveStore(root / "archive")
            start = datetime(2026, 1, 1, tzinfo=timezone.utc)
            stored = []
            for index, offset in enumerate((0, 12)):
                path = root / f"synthetic-copy-{index}.ts"
                path.write_bytes(original)
                stored.append(store.add_segment(
                    "synthetic", path, start + timedelta(seconds=offset),
                    start + timedelta(seconds=offset + 2), discontinuity=bool(index),
                ))
            mapper = PlaybackClockMap(start.timestamp())
            available_parts = []
            for item in stored:
                clock = mapper.append(item, item.path)
                with item.path.open("rb") as file:
                    available_parts.append(b"".join(mapped_chunks(file, 0, len(original), clock)))
            available = b"".join(available_parts)
            compact = root / "available.ts"
            compact.write_bytes(available)
            tokens = TokenStore(store.root)
            token = tokens.create("synthetic-viewer")
            service = ArchiveTSPlaybackService(
                store, tokens, authorize_user_channel=lambda *_: True,
                catchup_enabled=lambda *_: True,
            )
            raw = service.stream(token, "synthetic", start, start + timedelta(seconds=30),
                                 session_id="raw")
            self.assertEqual(raw.status, 409)
            normalized = service.stream(
                token, "synthetic", start, start + timedelta(seconds=30),
                session_id="programme", normalize_timestamps=True,
            )
            self.assertEqual(normalized.status, 200)
            self.assertEqual(normalized.headers["X-Catchuparr-Archive-Timeline"], "available-media")
            self.assertEqual(b"".join(normalized), available)
            normalized.close()
            ranged = service.stream(
                token, "synthetic", start, start + timedelta(seconds=30),
                session_id="programme-range", normalize_timestamps=True,
                range_header=f"bytes={len(original) + 191}-{len(original) + 500}",
            )
            self.assertEqual(ranged.status, 206)
            self.assertEqual(b"".join(ranged), available[len(original) + 191:len(original) + 501])
            ranged.close()
            with source.open("rb") as file:
                selected = b"".join(mapped_chunks(file, 191, len(original) - 388, following))
            self.assertEqual(selected, chunks[1][191:-197])
            result = subprocess.run([
                "ffprobe", "-v", "error", "-show_entries", "format=duration",
                "-of", "default=nw=1:nk=1", str(combined),
            ], check=True, capture_output=True, text=True)
            self.assertAlmostEqual(float(result.stdout), 14.0, delta=0.15)
            compact_duration = subprocess.run([
                "ffprobe", "-v", "error", "-show_entries", "format=duration",
                "-of", "default=nw=1:nk=1", str(compact),
            ], check=True, capture_output=True, text=True)
            self.assertAlmostEqual(float(compact_duration.stdout), 4.1, delta=0.15)
            def decoded_frames(path):
                decoded = subprocess.run([
                    "ffmpeg", "-v", "error", "-xerror", "-i", str(path), "-map", "0",
                    "-vsync", "0", "-f", "framemd5", "-",
                ], check=True, capture_output=True, text=True)
                streams = {}
                for line in decoded.stdout.splitlines():
                    if line and not line.startswith("#"):
                        fields = line.split(",")
                        streams.setdefault(fields[0].strip(), []).append(fields[-1].strip())
                return streams
            original_frames = decoded_frames(source)
            programme_frames = decoded_frames(combined)
            self.assertEqual(decoded_frames(compact), programme_frames)
            self.assertEqual(len(programme_frames["0"]), 100)
            self.assertEqual(programme_frames["0"], original_frames["0"] * 2)
            for stream, frames in original_frames.items():
                self.assertEqual(len(programme_frames[stream]), len(frames) * 2)
            for selected, muxer in (("0:a:0", "mp2"), ("0:v:0", "mpeg2video")):
                def elementary(path):
                    return subprocess.run([
                        "ffmpeg", "-v", "error", "-i", str(path), "-map", selected,
                        "-c", "copy", "-f", muxer, "-",
                    ], check=True, capture_output=True).stdout
                self.assertEqual(elementary(combined), elementary(source) * 2)
            growing_path = root / "synthetic-growing-first.ts"
            growing_path.write_bytes(original)
            store.add_segment("growing", growing_path, start, start + timedelta(seconds=2))
            growing = service.stream(
                token, "growing", start, start + timedelta(seconds=30),
                session_id="growing-programme", normalize_timestamps=True,
                live=True, live_follow_seconds=0.02,
            )
            self.assertEqual(growing.status, 200)
            self.assertNotIn("Content-Length", growing.headers)
            iterator = iter(growing)
            beginning = next(iterator)
            appended = root / "synthetic-growing-next.ts"
            appended.write_bytes(original)
            store.add_segment("growing", appended, start + timedelta(seconds=12),
                              start + timedelta(seconds=14), discontinuity=True)
            self.assertEqual(beginning + b"".join(iterator), available)
            growing.close()
            unknown = root / "clockless.ts"
            unknown.write_bytes(b"\x47" + bytes(187))
            store.add_segment("synthetic", unknown, start + timedelta(seconds=18),
                              start + timedelta(seconds=20), discontinuity=True)
            rejected = service.stream(
                token, "synthetic", start, start + timedelta(seconds=30),
                session_id="unmapped-source", normalize_timestamps=True,
            )
            self.assertEqual(rejected.status, 409)
