import array
import json
import math
import sys
import tempfile
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

        self.assertIsNone(probe._synthetic_source_for_frequency(750))
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

if __name__ == "__main__":
    unittest.main()
