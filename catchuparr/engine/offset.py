"""Find how much of a new input the archive already contains, by audio.

Two providers carrying the same channel can differ in latency by a minute or
more and never deliver identical packets, so packet hashes cannot prove the
overlap. The loudness envelope of the audio is compared instead: the start of
the new input is searched for in the contiguous archive tail. Only a clear,
unique match is accepted; anything else means "unknown" and the caller keeps
the input unchanged.
"""

from __future__ import annotations

import operator
import subprocess
import time
from array import array
from pathlib import Path

SAMPLE_RATE = 8000
ENVELOPE_HZ = 20
WINDOW_SECONDS = 20.0
MIN_OVERLAP_SECONDS = 5.0
SHORT_OVERLAP_SECONDS = 10.0
MIN_CORRELATION = 0.75
MIN_SHORT_CORRELATION = 0.92
MIN_MARGIN = 0.12
MIN_SPREAD = 0.02
DECODE_TIMEOUT_SECONDS = 8


def _timeout(deadline: float | None) -> float:
    if deadline is None:
        return DECODE_TIMEOUT_SECONDS
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError
    return min(DECODE_TIMEOUT_SECONDS, remaining)


def envelope(path: Path, *, ffmpeg: str = "ffmpeg", seconds: float | None = None,
             deadline: float | None = None) -> list[float] | None:
    """Mean absolute level of the first audio track in 1/ENVELOPE_HZ steps."""
    command = [ffmpeg, "-nostdin", "-v", "error", "-i", str(path)]
    if seconds is not None:
        command += ["-t", f"{seconds:.3f}"]
    command += ["-map", "0:a:0", "-ac", "1", "-ar", str(SAMPLE_RATE), "-f", "s16le", "pipe:1"]
    try:
        raw = subprocess.run(command, capture_output=True, check=True, timeout=_timeout(deadline)).stdout
    except (OSError, subprocess.SubprocessError, TimeoutError):
        return None
    samples = array("h")
    samples.frombytes(raw[: len(raw) // 2 * 2])
    step = SAMPLE_RATE // ENVELOPE_HZ
    return [sum(map(abs, samples[i:i + step])) / step for i in range(0, len(samples) - step + 1, step)]


def _prefix(values: list[float]) -> tuple[list[float], list[float]]:
    total, squares = [0.0], [0.0]
    for value in values:
        total.append(total[-1] + value)
        squares.append(squares[-1] + value * value)
    return total, squares


def _correlation(a: list[float], b: list[float], a_sum: float, a_sq: float, b_sum: float, b_sq: float) -> float:
    n = len(a)
    covariance = sum(map(operator.mul, a, b)) - a_sum * b_sum / n
    variance = (a_sq - a_sum * a_sum / n) * (b_sq - b_sum * b_sum / n)
    return covariance / variance ** 0.5 if variance > 1e-9 else 0.0


def repeated_seconds(archive: list[float], new: list[float]) -> float | None:
    """Seconds at the start of ``new`` that the archive tail already holds.

    ``archive`` ends where recording stops; ``new`` starts where the new input
    begins. Returns ``None`` unless exactly one earlier position matches.
    """
    minimum = int(MIN_OVERLAP_SECONDS * ENVELOPE_HZ)
    window = min(len(new), int(WINDOW_SECONDS * ENVELOPE_HZ))
    if window < minimum or len(archive) < minimum:
        return None
    new_sum, new_sq = _prefix(new[:window])
    arc_sum, arc_sq = _prefix(archive)
    scores = []
    for start in range(0, len(archive) - minimum + 1):
        length = min(window, len(archive) - start)
        a_total, a_squares = new_sum[length], new_sq[length]
        spread = (a_squares - a_total * a_total / length) / length
        if spread < MIN_SPREAD * MIN_SPREAD:
            return None  # silence or constant level cannot identify a position
        scores.append((
            _correlation(
                new[:length], archive[start:start + length], a_total, a_squares,
                arc_sum[start + length] - arc_sum[start], arc_sq[start + length] - arc_sq[start],
            ),
            start,
        ))
    best_score, best_start = max(scores)
    overlap = len(archive) - best_start
    # A short overlap leaves little to compare, so chance matches need a
    # stricter bar than a long one.
    if best_score < (MIN_SHORT_CORRELATION if overlap < SHORT_OVERLAP_SECONDS * ENVELOPE_HZ else MIN_CORRELATION):
        return None
    rivals = [score for score, start in scores if abs(start - best_start) > 2 * ENVELOPE_HZ]
    if rivals and max(rivals) > best_score - MIN_MARGIN:
        return None
    return (len(archive) - best_start) / ENVELOPE_HZ


def archive_tail_envelope(segments, *, ffmpeg: str = "ffmpeg", max_seconds: float = 170.0,
                          deadline: float | None = None) -> list[float] | None:
    """Envelope of the newest gap-free run of archived segments."""
    run = []
    for segment in reversed(segments):
        if run and run[-1].start_utc.timestamp() - segment.end_utc.timestamp() > 0.5:
            break
        run.append(segment)
        if sum(item.duration for item in run) >= max_seconds:
            break
    values: list[float] = []
    origin = run[-1].start_utc.timestamp()
    for segment in reversed(run):
        part = envelope(segment.path, ffmpeg=ffmpeg, deadline=deadline)
        if part is None:
            return None
        # Place each segment by its archive time so rounding at the segment
        # edges cannot add up to a drift across the whole tail.
        position = round((segment.start_utc.timestamp() - origin) * ENVELOPE_HZ)
        if position > len(values):
            values.extend([values[-1] if values else 0.0] * (position - len(values)))
        del values[position:]
        values.extend(part)
    return values or None


def repeated_seconds_for_input(segments, new_path: Path, *, ffmpeg: str = "ffmpeg",
                               deadline: float | None = None) -> float | None:
    """Return how long the start of ``new_path`` repeats the archive tail."""
    archive = archive_tail_envelope(segments, ffmpeg=ffmpeg, deadline=deadline)
    new = envelope(new_path, ffmpeg=ffmpeg, seconds=WINDOW_SECONDS, deadline=deadline) if archive else None
    if archive is None or new is None:
        return None
    return repeated_seconds(archive, new)
