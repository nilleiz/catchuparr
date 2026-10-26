"""Length-preserving MPEG-TS clock mapping for a bounded archive resource.

Archive segments may originate from separate muxer runs. Map their PES and
PCR clocks onto archive time without altering encoded media or byte offsets.
Missing archive time remains a forward clock jump, never replayed live media.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

PACKET_BYTES = 188
CLOCK_HZ = 90_000
CLOCK_WRAP = 1 << 33
BASE_CLOCK = 3600 * CLOCK_HZ
PROBE_BYTES = PACKET_BYTES * 4096


def _payload(packet):
    if len(packet) != PACKET_BYTES or packet[0] != 0x47:
        raise ValueError("invalid MPEG-TS packet alignment")
    if packet[3] & 0xC0:
        raise ValueError("scrambled MPEG-TS cannot be clock mapped")
    control = (packet[3] >> 4) & 3
    if control not in (1, 3):
        return None
    offset = 4 if control == 1 else 5 + packet[4]
    return offset if offset < PACKET_BYTES else None


def _read_pts(data, at):
    if at + 5 > len(data) or any(not data[at + n] & 1 for n in (0, 2, 4)):
        raise ValueError("unsupported split or malformed PES clock")
    return (((data[at] & 14) << 29) | (data[at + 1] << 22)
            | ((data[at + 2] & 254) << 14) | (data[at + 3] << 7)
            | (data[at + 4] >> 1))


def _write_pts(data, at, ticks):
    ticks %= CLOCK_WRAP
    data[at:at + 5] = bytes((
        (data[at] & 0xF1) | ((ticks >> 29) & 14),
        (ticks >> 22) & 255, ((ticks >> 14) & 254) | 1,
        (ticks >> 7) & 255, ((ticks << 1) & 254) | 1,
    ))


def _pes_clock(packet):
    offset = _payload(packet)
    if offset is None or not packet[1] & 0x40:
        return None
    if packet[offset:offset + 3] != b"\x00\x00\x01":
        return None
    if offset + 9 > PACKET_BYTES:
        raise ValueError("unsupported split PES header")
    stream = packet[offset + 3]
    # Audio/video/private stream PES use the optional MPEG-2 PES header.
    if not (0xC0 <= stream <= 0xEF or stream == 0xBD):
        return None
    flags = packet[offset + 7] & 0xC0
    if flags not in (0x80, 0xC0):
        return None
    pts = offset + 9
    value = _read_pts(packet, pts)
    if flags == 0xC0:
        _read_pts(packet, pts + 5)
    return stream, pts, flags, value


@dataclass(frozen=True)
class SegmentClock:
    offset: int

    @classmethod
    def probe(cls, path: Path, elapsed_seconds: float):
        """Cache clock metadata for immutable files, never their media bytes."""
        path = Path(path)
        stat = path.stat()
        value = _source_clock(str(path), stat.st_size, stat.st_mtime_ns)
        if value is None:
            return None
        return cls(round(elapsed_seconds * CLOCK_HZ) + BASE_CLOCK - value)

    def transform(self, data: bytes) -> bytes:
        if len(data) % PACKET_BYTES:
            raise ValueError("unaligned MPEG-TS block")
        result = bytearray(data)
        for index in range(0, len(result), PACKET_BYTES):
            packet = result[index:index + PACKET_BYTES]
            clock = _pes_clock(packet)
            if packet[3] & 0x20 and packet[4] >= 1:
                flags = packet[5]
                at = 6
                for present in (0x10, 0x08):  # PCR and OPCR, in wire order
                    if flags & present:
                        if at + 6 > 5 + packet[4]:
                            raise ValueError("malformed adaptation clock")
                        base = (packet[at] << 25 | packet[at + 1] << 17
                                | packet[at + 2] << 9 | packet[at + 3] << 1
                                | packet[at + 4] >> 7)
                        base = (base + self.offset) % CLOCK_WRAP
                        packet[at:at + 5] = bytes((
                            base >> 25, (base >> 17) & 255,
                            (base >> 9) & 255, (base >> 1) & 255,
                            ((base & 1) << 7) | (packet[at + 4] & 127),
                        ))
                        at += 6
            if clock is not None:
                _, at, flags, value = clock
                _write_pts(packet, at, value + self.offset)
                if flags == 0xC0:
                    _write_pts(packet, at + 5, _read_pts(packet, at + 5) + self.offset)
            result[index:index + PACKET_BYTES] = packet
        return bytes(result)


@lru_cache(maxsize=8192)
def _source_clock(path: str, size: int, mtime_ns: int):
    if not size or size % PACKET_BYTES:
        return None
    fallback = None
    with Path(path).open("rb") as source:
        consumed = 0
        while consumed < min(size, PROBE_BYTES):
            data = source.read(min(64 * PACKET_BYTES, size - consumed, PROBE_BYTES - consumed))
            if not data or len(data) % PACKET_BYTES:
                return None
            consumed += len(data)
            for offset in range(0, len(data), PACKET_BYTES):
                clock = _pes_clock(data[offset:offset + PACKET_BYTES])
                if clock is None:
                    continue
                stream, _, _, value = clock
                if fallback is None:
                    fallback = value
                if 0xE0 <= stream <= 0xEF:
                    return value
    return fallback


def mapped_chunks(source, offset: int, length: int, clock: SegmentClock):
    """Map complete packets then slice, so arbitrary HTTP byte ranges agree."""
    aligned = offset - offset % PACKET_BYTES
    prefix = offset - aligned
    source.seek(aligned)
    remaining = length
    while remaining:
        count = min(348 * PACKET_BYTES,
                    ((remaining + prefix + PACKET_BYTES - 1) // PACKET_BYTES) * PACKET_BYTES)
        data = source.read(count)
        if len(data) != count:
            raise OSError("archive segment changed during mapped read")
        block = clock.transform(data)
        selected = block[prefix:prefix + remaining]
        remaining -= len(selected)
        prefix = 0
        yield selected
