import json
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from catchuparr.engine.splice import cut_segment, find_splice, probe_tracks

FFMPEG = bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))


def run(*args):
    subprocess.run(["ffmpeg", "-v", "error", "-y", *args], check=True, capture_output=True, timeout=60)


def encoders():
    if not FFMPEG:
        return ""
    return subprocess.run(["ffmpeg", "-hide_banner", "-encoders"], capture_output=True, text=True).stdout


@unittest.skipUnless(FFMPEG, "requires FFmpeg")
class SpliceTests(unittest.TestCase):
    def make_source(self, root, video, audio, seconds=14):
        source = root / "source.ts"
        extra = ["-bf", "2", "-x264-params", "open-gop=1:keyint=25:min-keyint=25:scenecut=0"] if video == "libx264" else []
        run("-f", "lavfi", "-i", "testsrc2=size=64x64:rate=25", "-f", "lavfi", "-i",
            "sine=frequency=440:sample_rate=48000", "-t", str(seconds), "-c:v", video, "-g", "25", *extra,
            "-c:a", audio, "-f", "mpegts", str(source))
        return source

    def tail_and_head(self, root, source, tail_seconds, head_start):
        new = root / "new.ts"
        # Like the recorder, cut the archived tail at a keyframe boundary.
        run("-i", str(source), "-map", "0", "-c", "copy", "-f", "segment", "-segment_time", str(tail_seconds),
            "-segment_format", "mpegts", str(root / "old-%02d.ts"))
        run("-ss", str(head_start), "-i", str(source), "-t", "6", "-map", "0", "-c", "copy", "-f", "mpegts", str(new))
        return root / "old-00.ts", new

    def hashes(self, path, stream):
        out = subprocess.run([
            "ffprobe", "-v", "error", "-select_streams", stream, "-show_packets", "-show_data_hash", "sha256",
            "-show_entries", "packet=data_hash", "-of", "json", str(path),
        ], check=True, capture_output=True, timeout=30)
        return [p["data_hash"] for p in json.loads(out.stdout)["packets"]]

    def check_codec(self, video, audio, tail_seconds):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.make_source(root, video, audio)
            old, new = self.tail_and_head(root, source, tail_seconds, 2)
            splice = find_splice(old, new, deadline=time.monotonic() + 20)
            self.assertIsNotNone(splice)
            payload = cut_segment(new, splice, time.monotonic() + 20)
            self.assertIsNotNone(payload)
            cut = root / "cut.ts"
            cut.write_bytes(payload)
            self.assertEqual(len(payload) % 188, 0)
            joined = self.hashes(old, "v:0") + self.hashes(cut, "v:0")
            reference = self.hashes(source, "v:0")
            self.assertEqual(joined, reference[:len(joined)])
            audio_hashes = self.hashes(old, "a:0") + self.hashes(cut, "a:0")
            self.assertEqual(len(audio_hashes), len(set(audio_hashes)))
            decoded = subprocess.run(
                ["ffmpeg", "-v", "error", "-xerror", "-i", str(cut), "-f", "null", "-"],
                capture_output=True, timeout=30,
            )
            self.assertEqual(decoded.returncode, 0, decoded.stderr)

    def test_mpeg2_overlap_is_cut_without_repeating_or_losing_pictures(self):
        self.check_codec("mpeg2video", "mp2", 5)

    @unittest.skipUnless("libx264" in encoders() and " aac " in encoders(), "requires H.264 and AAC encoders")
    def test_open_gop_h264_with_leading_pictures_is_cut_at_the_keyframe(self):
        self.check_codec("libx264", "aac", 5)

    def test_archive_tail_that_is_not_in_the_new_input_has_no_splice(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.make_source(root, "mpeg2video", "mp2")
            old, new = self.tail_and_head(root, source, 2, 6)  # new starts after the tail ends
            self.assertIsNone(find_splice(old, new, deadline=time.monotonic() + 20))

    def test_cut_inside_a_gop_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.make_source(root, "mpeg2video", "mp2")
            old, new = root / "old.ts", root / "new.ts"
            run("-i", str(source), "-t", "4.5", "-map", "0", "-c", "copy", "-f", "mpegts", str(old))  # mid-GOP
            run("-ss", "2", "-i", str(source), "-t", "6", "-map", "0", "-c", "copy", "-f", "mpegts", str(new))
            self.assertIsNone(find_splice(old, new, deadline=time.monotonic() + 20))

    def test_fully_archived_input_is_reported_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.make_source(root, "mpeg2video", "mp2")
            old, new = root / "old-00.ts", root / "new-00.ts"
            run("-i", str(source), "-map", "0", "-c", "copy", "-f", "segment", "-segment_time", "5",
                "-segment_format", "mpegts", str(root / "old-%02d.ts"))
            run("-ss", "2", "-i", str(source), "-map", "0", "-c", "copy", "-f", "segment", "-segment_time", "3",
                "-segment_format", "mpegts", str(root / "new-%02d.ts"))
            splice = find_splice(old, new, deadline=time.monotonic() + 20)
            self.assertTrue(splice is not None and splice.complete)

    def test_unreadable_media_is_not_probed(self):
        with tempfile.TemporaryDirectory() as directory:
            junk = Path(directory) / "junk.ts"
            junk.write_bytes(b"\x00" * 1880)
            self.assertIsNone(probe_tracks(junk))


if __name__ == "__main__":
    unittest.main()
