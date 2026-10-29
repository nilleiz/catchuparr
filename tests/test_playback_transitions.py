import shutil
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from catchuparr.engine.store import ArchiveStore, Segment
from catchuparr.engine.ts_timeline import PlaybackClockMap, mapped_chunks
from catchuparr.http import ArchiveHTTPService
from catchuparr.security import TokenStore
from catchuparr.ts_http import ArchiveTSPlaybackService


@unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "requires FFmpeg")
class PlaybackTransitionTests(unittest.TestCase):
    def test_segments_from_one_muxer_keep_their_shared_clock(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            try:
                subprocess.run([
                    "ffmpeg", "-v", "error", "-f", "lavfi", "-i",
                    "testsrc2=size=64x64:rate=25", "-f", "lavfi", "-i",
                    "sine=frequency=440:sample_rate=48000", "-t", "6",
                    "-c:v", "libx264", "-preset", "ultrafast", "-g", "25", "-threads", "1",
                    "-c:a", "aac", "-f", "segment", "-segment_time", "2",
                    "-segment_format", "mpegts", str(root / "part-%03d.ts"),
                ], check=True, capture_output=True, timeout=30)
            except subprocess.CalledProcessError as exc:
                if b"Unknown encoder 'libx264'" in exc.stderr:
                    self.skipTest("requires the FFmpeg H.264 encoder")
                raise
            start = datetime(2026, 1, 1, tzinfo=timezone.utc)
            mapper = PlaybackClockMap(start.timestamp())
            offsets, output = [], bytearray()
            parts = sorted(root.glob("part-*.ts"))
            self.assertGreaterEqual(len(parts), 2)
            for index, path in enumerate(parts):
                segment = Segment(
                    str(index), "synthetic", path,
                    start + timedelta(seconds=index * 2),
                    start + timedelta(seconds=(index + 1) * 2),
                )
                clock = mapper.append(segment, path)
                self.assertIsNotNone(clock)
                offsets.append(clock.offset)
                with path.open("rb") as source:
                    output.extend(b"".join(mapped_chunks(source, 0, path.stat().st_size, clock)))
            self.assertEqual(len(set(offsets)), 1)
            combined = root / "same-muxer.ts"
            combined.write_bytes(output)
            subprocess.run([
                "ffmpeg", "-v", "error", "-xerror", "-i", str(combined),
                "-map", "0", "-vsync", "0", "-f", "framemd5", "-",
            ], check=True, capture_output=True, timeout=30)

    def test_independent_av_sessions_cross_transition_without_replaying_the_gap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = []
            for index in range(2):
                source = root / f"source-{index}.ts"
                try:
                    subprocess.run([
                        "ffmpeg", "-v", "error", "-f", "lavfi", "-i",
                        f"testsrc2=size={64 + 64 * index}x64:rate=25", "-f", "lavfi", "-i",
                        f"sine=frequency={440 + 200 * index}:sample_rate=48000", "-t", "2",
                        "-c:v", "libx264", "-preset", "ultrafast", "-g", "25", "-threads", "1",
                        "-c:a", "aac", "-f", "mpegts", str(source),
                    ], check=True, capture_output=True, timeout=30)
                except subprocess.CalledProcessError as exc:
                    if b"Unknown encoder 'libx264'" in exc.stderr:
                        self.skipTest("requires the FFmpeg H.264 encoder")
                    raise
                sources.append(source)

            store = ArchiveStore(root / "archive")
            tokens = TokenStore(store.root)
            token = tokens.create("synthetic-viewer")
            hls = ArchiveHTTPService(
                store, tokens, authorize_user_channel=lambda *_: True,
                catchup_enabled=lambda *_: True,
            )
            xc = ArchiveTSPlaybackService(
                store, tokens, authorize_user_channel=lambda *_: True,
                catchup_enabled=lambda *_: True,
            )
            start = datetime(2026, 1, 1, tzinfo=timezone.utc)
            for label, second_start in (("adjacent", 2), ("missing", 5)):
                with self.subTest(transition=label):
                    items = []
                    for index, offset in enumerate((0, second_start)):
                        items.append(store.add_segment(
                            label, sources[index], start + timedelta(seconds=offset),
                            start + timedelta(seconds=offset + 2), discontinuity=bool(index),
                        ))
                        if index == 0:
                            initial = hls.playlist(
                                token, label, start, start + timedelta(seconds=15), live=True,
                            )
                            self.assertEqual(initial.status, 200)
                            initial_uri = next(
                                line for line in initial.body.decode().splitlines()
                                if line and not line.startswith("#")
                            )
                            initial_lease = parse_qs(urlsplit(initial_uri).query)["lease"][0]
                            initial_segment = hls.segment(token, label, items[0].id, initial_lease)
                    playlist = hls.playlist(token, label, start, start + timedelta(seconds=15))
                    self.assertEqual(playlist.status, 200)
                    text = playlist.body.decode()
                    self.assertEqual(text.count("#EXT-X-DISCONTINUITY"), 1)
                    uris = [line for line in text.splitlines() if line and not line.startswith("#")]
                    self.assertEqual(len(uris), 2)
                    self.assertEqual(uris[0], initial_uri)
                    mapped_parts = []
                    for index, (item, uri) in enumerate(zip(items, uris)):
                        lease = parse_qs(urlsplit(uri).query)["lease"][0]
                        full = hls.segment(token, label, item.id, lease)
                        self.assertEqual(full.status, 200)
                        if index == 0:
                            self.assertEqual(full.body, initial_segment.body)
                            self.assertEqual(full.headers["ETag"], initial_segment.headers["ETag"])
                        part = hls.segment(token, label, item.id, lease, range_header="bytes=191-700")
                        self.assertEqual(part.status, 206)
                        self.assertEqual(part.body, full.body[191:701])
                        self.assertEqual(item.path.read_bytes(), sources[index].read_bytes())
                        local = root / f"mapped-{label}-{index}.ts"
                        local.write_bytes(full.body)
                        mapped_parts.append(full.body)
                        text = text.replace(uri, local.name)
                    local_playlist = root / f"{label}.m3u8"
                    local_playlist.write_text(text, encoding="utf-8")
                    transport = xc.stream(
                        token, label, start, start + timedelta(seconds=15),
                        session_id=f"xc-{label}", normalize_timestamps=True,
                    )
                    self.assertEqual(transport.status, 200)
                    try:
                        body = b"".join(transport)
                    finally:
                        transport.close()
                    self.assertEqual(body, b"".join(mapped_parts))
                    transport_file = root / f"{label}.ts"
                    transport_file.write_bytes(body)
                    for playback in (local_playlist, transport_file):
                        decoded = subprocess.run([
                            "ffmpeg", "-v", "error", "-xerror", "-protocol_whitelist", "file,crypto,data",
                            "-i", str(playback), "-map", "0", "-vsync", "0", "-f", "framemd5", "-",
                        ], check=True, capture_output=True, text=True, timeout=30)
                        counts = {}
                        for line in decoded.stdout.splitlines():
                            if line and not line.startswith("#"):
                                stream = line.split(",", 1)[0].strip()
                                counts[stream] = counts.get(stream, 0) + 1
                        self.assertEqual(counts.get("0"), 100)
                        self.assertGreater(counts.get("1", 0), 180)
                    for stream, muxer in (("0:v:0", "h264"), ("0:a:0", "adts")):
                        def elementary(path):
                            return subprocess.run([
                                "ffmpeg", "-v", "error", "-i", str(path), "-map", stream,
                                "-c", "copy", "-f", muxer, "-",
                            ], check=True, capture_output=True, timeout=30).stdout
                        self.assertEqual(
                            elementary(transport_file), b"".join(elementary(item) for item in sources),
                        )
                    probe = subprocess.run([
                        "ffprobe", "-v", "error", "-show_entries", "format=duration",
                        "-of", "default=nw=1:nk=1", str(transport_file),
                    ], check=True, capture_output=True, text=True, timeout=30)
                    self.assertAlmostEqual(float(probe.stdout), 4.1, delta=0.2)


if __name__ == "__main__":
    unittest.main()
