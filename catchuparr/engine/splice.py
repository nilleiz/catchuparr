"""Exact packet-level splice of a staged TS segment onto the archive tail.

The archive tail ends at a known encoded packet in every track. A staged
reader of the same channel normally contains those packets as well, with its
own transport clock. The splice therefore resumes each track at the packet that
follows the archive's last packet, found by comparing packet payload hashes.
Cutting is done on whole transport packets, so no media is re-encoded or
re-timed and neither a repeated nor a skipped picture or audio frame can occur.
"""

from __future__ import annotations

import json
import math
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

MAX_PROBE_BYTES = 64 * 1024 * 1024
MAX_SEGMENT_BYTES = 64 * 1024 * 1024
PROBE_TIMEOUT_SECONDS = 4
TS_PACKET = 188
# Hashes of this many consecutive packets must agree so that static or looped
# content cannot be mistaken for the archive tail.
MIN_CONTEXT_PACKETS = {"video": 6, "audio": 6}
MIN_DISTINCT_CONTEXT = 3
MAX_AUDIO_SKIP_SECONDS = 0.25


@dataclass(frozen=True)
class Packet:
    pts: float
    dts: float
    key: bool
    pos: int | None  # byte offset of the PES start; None inside a shared PES
    digest: str


@dataclass(frozen=True)
class Track:
    kind: str
    codec: str
    pid: int
    packets: tuple[Packet, ...]


@dataclass(frozen=True)
class Splice:
    """Where each track of a staged file resumes after the archive tail."""

    video_index: int
    audio_index: int
    video_pts: float
    audio_pts: float
    # Seconds between the start of the staged video track and the splice.
    cut_offset: float
    # Every video packet of the staged file is already archived.
    complete: bool = False


def _deadline_timeout(deadline: float | None) -> float:
    if deadline is None:
        return PROBE_TIMEOUT_SECONDS
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError
    return min(PROBE_TIMEOUT_SECONDS, remaining)


def probe_tracks(path: Path, deadline: float | None = None) -> dict[str, Track] | None:
    """Return the single video and audio track of a bounded TS file.

    Subtitle tracks are ignored. Several video or audio tracks, unknown track
    types and unreadable timestamps are not supported and return ``None``.
    """
    try:
        path = Path(path)
        if not path.is_file() or not 0 < path.stat().st_size <= MAX_PROBE_BYTES:
            return None
        result = subprocess.run(
            [
                "ffprobe", "-v", "error", "-show_streams", "-show_packets",
                "-show_data_hash", "sha256", "-show_entries",
                "stream=index,id,codec_type,codec_name:"
                "packet=stream_index,pts_time,dts_time,flags,pos,data_hash",
                "-of", "json", str(path),
            ],
            check=True, capture_output=True, timeout=_deadline_timeout(deadline),
        )
        document = json.loads(result.stdout)
        tracks: dict[str, Track] = {}
        by_index = {}
        for stream in document["streams"]:
            kind = stream.get("codec_type")
            if kind == "subtitle":
                continue
            if kind not in {"video", "audio"} or kind in by_index.values():
                return None
            by_index[stream["index"]] = kind
            tracks[kind] = (stream["index"], stream["codec_name"], int(str(stream["id"]), 0))
        packets: dict[str, list[Packet]] = {"video": [], "audio": []}
        for raw in document["packets"]:
            kind = by_index.get(raw.get("stream_index"))
            if kind is None:
                continue
            digest = raw["data_hash"]
            pts, dts = float(raw["pts_time"]), float(raw["dts_time"])
            pos = int(raw["pos"]) if raw.get("pos", "N/A") != "N/A" else None
            if (not isinstance(digest, str) or not digest.startswith("SHA256:")
                    or not math.isfinite(pts) or not math.isfinite(dts)
                    or (pos is not None and (pos < 0 or pos % TS_PACKET))):
                return None
            packets[kind].append(Packet(pts, dts, "K" in str(raw.get("flags", "")), pos, digest))
        if set(tracks) != {"video", "audio"} or not all(packets.values()):
            return None
        return {
            kind: Track(kind, tracks[kind][1], tracks[kind][2], tuple(packets[kind]))
            for kind in ("video", "audio")
        }
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError, TimeoutError):
        return None


def _resume_index(old: Track, new: Track) -> int | None:
    """Index in ``new`` of the first packet after the last packet of ``old``."""
    context = MIN_CONTEXT_PACKETS[old.kind]
    if len(old.packets) < context or new.codec != old.codec:
        return None
    tail = [packet.digest for packet in old.packets[-context:]]
    if len(set(tail)) < MIN_DISTINCT_CONTEXT:
        return None
    digests = [packet.digest for packet in new.packets]
    matches = [
        end for end in range(context, len(digests) + 1)
        if digests[end - context:end] == tail
    ]
    if len(matches) != 1:
        return None
    return matches[0]


def find_splice(old_path: Path, staged_path: Path, *, deadline: float | None = None) -> Splice | None:
    """Find the resume point of ``staged_path`` after the archived ``old_path``.

    Returns ``None`` unless both tracks resume at an exact, unique packet
    boundary, the first new picture is a keyframe and at least one new packet
    remains in each track.
    """
    old = probe_tracks(old_path, deadline)
    new = probe_tracks(staged_path, deadline) if old else None
    if old is None or new is None:
        return None
    indexes = {}
    for kind in ("video", "audio"):
        index = _resume_index(old[kind], new[kind])
        if index is None:
            return None
        indexes[kind] = index
    audio_tail = new["audio"].packets[indexes["audio"]:]
    if (indexes["video"] == len(new["video"].packets)
            and (not audio_tail or audio_tail[-1].pts - audio_tail[0].pts <= MAX_AUDIO_SKIP_SECONDS)):
        return Splice(indexes["video"], indexes["audio"], 0.0, 0.0, 0.0, complete=True)
    if any(indexes[kind] >= len(new[kind].packets) for kind in indexes):
        return None
    video = new["video"].packets
    cut = video[indexes["video"]]
    if not cut.key or cut.pos is None:
        return None
    # Audio frames share PES packets. Whole PES packets can be cut without
    # rewriting, so resume at the next PES start: at most one PES of audio is
    # left out, never repeated.
    audio = new["audio"].packets
    resume_pts = audio[indexes["audio"]].pts
    while indexes["audio"] < len(audio) and audio[indexes["audio"]].pos is None:
        indexes["audio"] += 1
    if indexes["audio"] >= len(audio) or audio[indexes["audio"]].pts - resume_pts > MAX_AUDIO_SKIP_SECONDS:
        return None
    # Pictures that follow the keyframe in decode order but precede it in
    # presentation order (open GOP) reference the archived tail, which this
    # splice continues exactly, so they are kept like any other packet.
    first_pts = min(packet.pts for packet in video)
    return Splice(
        indexes["video"], indexes["audio"], cut.pts,
        audio[indexes["audio"]].pts, cut.pts - first_pts,
    )


def cut_segment(staged_path: Path, splice: Splice, deadline: float | None = None) -> bytes | None:
    """Return the staged TS bytes from the splice on; media is left untouched."""
    tracks = probe_tracks(staged_path, deadline)
    if tracks is None:
        return None
    try:
        size = Path(staged_path).stat().st_size
        if not 0 < size <= MAX_SEGMENT_BYTES or size % TS_PACKET:
            return None
        data = Path(staged_path).read_bytes()
    except OSError:
        return None
    positions = (tracks["video"].packets[splice.video_index].pos, tracks["audio"].packets[splice.audio_index].pos)
    if None in positions:
        return None
    first_kept = {
        tracks["video"].pid: positions[0] // TS_PACKET,
        tracks["audio"].pid: positions[1] // TS_PACKET,
    }
    if len(first_kept) != 2 or any(index >= size // TS_PACKET for index in first_kept.values()):
        return None
    output = bytearray()
    seen = set()
    for number in range(size // TS_PACKET):
        packet = data[number * TS_PACKET:(number + 1) * TS_PACKET]
        if packet[0] != 0x47:
            return None
        pid = ((packet[1] & 0x1F) << 8) | packet[2]
        if pid in first_kept:
            if number < first_kept[pid]:
                continue
            if pid not in seen:
                if not packet[1] & 0x40:
                    return None
                seen.add(pid)
            output += packet
        elif pid == 0 or number < min(first_kept.values()):
            # PAT/PMT before the cut stay so the first kept packet is decodable.
            # Other PIDs (for example subtitles) are dropped.
            if pid == 0 or _is_psi(packet):
                output += packet
        elif _is_psi(packet):
            output += packet
    if seen != set(first_kept):
        return None
    return bytes(output)


def _is_psi(packet: bytes) -> bool:
    """True for a PMT-like section start (table id 0x02 after the pointer)."""
    if not packet[1] & 0x40:
        return False
    adaptation = (packet[3] >> 4) & 0x03
    if adaptation not in (1, 3):
        return False
    offset = 4 + (1 + packet[4] if adaptation == 3 else 0)
    if offset >= TS_PACKET:
        return False
    offset += 1 + packet[offset]
    return offset < TS_PACKET and packet[offset] == 0x02
