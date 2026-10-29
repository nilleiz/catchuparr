"""Conservative content evidence for a staged recorder handover."""

from __future__ import annotations

import json
import math
import subprocess
import time
from pathlib import Path

MAX_PROBE_BYTES = 64 * 1024 * 1024
PROBE_TIMEOUT_SECONDS = 3


def _packet_fingerprints(path: Path, *, timeout: float = PROBE_TIMEOUT_SECONDS) -> tuple[tuple, ...] | None:
    """Read bounded packet hashes without logging media, paths, or probe output."""
    try:
        path = Path(path)
        if not path.is_file() or not 0 < path.stat().st_size <= MAX_PROBE_BYTES:
            return None
        result = subprocess.run(
            [
                "ffprobe", "-v", "error", "-show_streams", "-show_packets",
                "-show_data_hash", "sha256", "-show_entries",
                "stream=index,codec_type,codec_name:packet=stream_index,pts_time,flags,data_hash",
                "-of", "json", str(path),
            ],
            check=True, capture_output=True, timeout=timeout,
        )
        document = json.loads(result.stdout)
        streams = document["streams"]
        packets = document["packets"]
        if not isinstance(streams, list) or not isinstance(packets, list):
            return None
        tracks = []
        for stream in streams:
            # Subtitle, private and data tracks cannot yet be proven continuous.
            if stream.get("codec_type") not in {"video", "audio"}:
                return None
            index = stream["index"]
            kind = stream["codec_type"]
            codec = stream["codec_name"]
            if type(index) is not int or not isinstance(codec, str) or not codec:
                return None
            samples = []
            for packet in packets:
                if packet.get("stream_index") != index:
                    continue
                digest, raw_time = packet.get("data_hash"), packet.get("pts_time")
                if not isinstance(digest, str) or not digest.startswith("SHA256:"):
                    return None
                timestamp = float(raw_time)
                if not math.isfinite(timestamp):
                    return None
                samples.append((digest, timestamp, str(packet.get("flags", ""))))
            minimum = 25 if kind == "video" else 10
            distinct = 10 if kind == "video" else 5
            if (len(samples) < minimum or len({sample[0] for sample in samples}) < distinct
                    or max(sample[1] for sample in samples) - min(sample[1] for sample in samples) < 0.5
                    or (kind == "video" and "K" not in samples[0][2])):
                return None
            tracks.append((kind, codec, tuple(samples)))
        if {track[0] for track in tracks} != {"video", "audio"}:
            return None
        return tuple(tracks)
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError):
        return None


def matching_completed_segments(old_path: Path, staged_path: Path, *, deadline: float | None = None) -> bool:
    """Prove the same complete, keyframe-aligned encoded A/V packet sequences.

    Distinct input clocks are allowed only when every packet in every track has
    the same offset. Repeated static content and unsupported tracks fail closed.
    This proves the overlap, not availability of future input packets.
    """
    def probe(path):
        remaining = PROBE_TIMEOUT_SECONDS if deadline is None else deadline - time.monotonic()
        if remaining <= 0:
            return None
        return _packet_fingerprints(path, timeout=min(PROBE_TIMEOUT_SECONDS, remaining))

    previous = probe(old_path)
    staged = probe(staged_path) if previous is not None else None
    if previous is None or staged is None or len(previous) != len(staged):
        return False
    offset = None
    for old_track, new_track in zip(previous, staged):
        if old_track[:2] != new_track[:2] or len(old_track[2]) != len(new_track[2]):
            return False
        for old_packet, new_packet in zip(old_track[2], new_track[2]):
            if old_packet[0] != new_packet[0]:
                return False
            difference = old_packet[1] - new_packet[1]
            if offset is None:
                offset = difference
            elif abs(difference - offset) > 0.002:
                return False
    return offset is not None
