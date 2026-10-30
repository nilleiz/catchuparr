"""FFmpeg stream-copy recorder for a Dispatcharr proxy URL."""

from __future__ import annotations

import csv
import io
import re
import shutil
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .store import ArchiveStore

DEFAULT_MEDIA_IDLE_TIMEOUT_SECONDS = 120.0
DEFAULT_RECORDER_STARTUP_TIMEOUT_SECONDS = 60.0
ARCHIVE_INDEX_FINALIZATION_GRACE_SECONDS = 5.0
LIVE_EDGE_IDLE_TIMEOUT_SECONDS = (
    DEFAULT_MEDIA_IDLE_TIMEOUT_SECONDS + ARCHIVE_INDEX_FINALIZATION_GRACE_SECONDS
)
SYNCHRONIZED_HANDOVER_TIMEOUT_SECONDS = 12.0
STAGED_INPUT_MAX_SECONDS = 30.0
STAGED_INPUT_MAX_BYTES = 256 * 1024 * 1024


@dataclass(frozen=True)
class RecorderAttemptResult:
    """Outcome of one bounded source attempt."""

    status: str
    useful_segments: int
    return_code: int | None
    attempt_switch_id: str | None = None
    staged_input: object | None = field(default=None, repr=False, compare=False)


class RecorderAttemptSwitch:
    """Signal one input attempt to end without stopping its recorder owner.

    The lock serializes the switch request with each archive segment commit. Once
    ``request()`` returns, this attempt cannot publish another segment.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requested = threading.Event()
        self._wake = threading.Event()
        self._offered = None
        self.attempt_id = uuid.uuid4().hex

    def request(self) -> None:
        with self._lock:
            self._requested.set()
            self._wake.set()

    def offer_synchronized_input(self, recorder) -> bool:
        """Offer one already-authorized input while the current one runs."""
        with self._lock:
            if self._requested.is_set() or self._offered is not None:
                return False
            self._offered = recorder
            self._wake.set()
            return True

    def take_synchronized_input(self):
        with self._lock:
            if self._requested.is_set():
                return None
            offered, self._offered = self._offered, None
            self._wake.clear()
            return offered

    def request_if_current(self, predicate: Callable[[], bool]) -> bool:
        """Freeze publication only if the previously matched segment is still last."""
        with self._lock:
            if self._requested.is_set() or not predicate():
                return False
            self._requested.set()
            self._wake.set()
            return True

    def is_set(self) -> bool:
        return self._requested.is_set()

    def wait(self, timeout: float) -> bool:
        """Wake the current attempt as soon as a source switch is requested."""
        return self._wake.wait(timeout)

    def publish_if_current(self, publish: Callable[[], None]) -> bool:
        with self._lock:
            if self._requested.is_set():
                return False
            publish()
            return True


def _wait_for_switch_or_stop(stop_event, switch, timeout):
    """Wait for either owner stop or a scoped switch without delaying either."""
    deadline = time.monotonic() + timeout
    while not stop_event.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0 or switch.wait(min(remaining, 0.1)):
            return


@dataclass(repr=False)
class StagedRecorderInput:
    """One private FFmpeg process whose output has not been published."""

    recorder: object
    output_dir: Path
    process: object
    offset: int = 0
    anchor: float | None = None
    expected_start: float | None = None
    matched: bool = False
    consumed: bool = False
    # Exact packet cut of the first published row: (CSV file name, replacement
    # path, seconds removed from the row start).
    cut: tuple[str, Path, float] | None = None
    started_at: float = field(default_factory=time.monotonic)

    def within_limits(self) -> bool:
        """Bound staging while the outgoing source waits for exact cleanup."""
        if self.consumed:
            return False
        try:
            size = sum(path.stat().st_size for path in self.output_dir.iterdir() if path.is_file())
            allowed = (
                time.monotonic() - self.started_at <= STAGED_INPUT_MAX_SECONDS
                and size <= STAGED_INPUT_MAX_BYTES
            )
        except OSError:
            allowed = False
        if not allowed:
            self.close()
        return allowed

    def close(self) -> None:
        if self.consumed:
            return
        self.consumed = True
        try:
            if self.process.poll() is None:
                try:
                    self.process.terminate()
                except ProcessLookupError:
                    pass
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    try:
                        self.process.kill()
                    except ProcessLookupError:
                        pass
                    self.process.wait()
        finally:
            shutil.rmtree(self.output_dir, ignore_errors=True)


def _staged_csv_rows(output_dir: Path, offset: int = 0):
    """Read only complete FFmpeg CSV rows from this task's staging directory."""
    path = output_dir / "segments.csv"
    try:
        with path.open("rb") as source:
            source.seek(offset)
            data = source.read()
    except FileNotFoundError:
        return [], offset
    rows = []
    for line in data.splitlines(keepends=True):
        if not line.endswith(b"\n"):
            break
        next_offset = offset + len(line)
        try:
            value = next(csv.reader(io.StringIO(line.decode("utf-8"))))
            if len(value) < 3:
                raise ValueError("incomplete staged CSV row")
            filename = Path(value[0])
            filename = filename if filename.is_absolute() else output_dir / filename
            if filename.resolve().parent != output_dir.resolve():
                raise ValueError("staged segment is outside its work directory")
            start, end = float(value[1]), float(value[2])
            if not (end > start and end - start <= 3600):
                raise ValueError("invalid staged segment interval")
            rows.append((filename, start, end, next_offset))
        except (ValueError, UnicodeError, StopIteration, OSError):
            raise ValueError("invalid staged recorder output") from None
        offset = next_offset
    return rows, offset


class FFmpegCopyRecorder:
    """Record an input URL to short MPEG-TS segments with stream copy.

    The input is expected to be a Dispatcharr proxy URL. Completed files are
    copied into ``ArchiveStore`` once FFmpeg publishes their CSV timing row.
    This class can supervise restarts, but a distributed recorder lease should
    be acquired by its caller before running it.
    """

    def __init__(
        self,
        store: ArchiveStore,
        channel_id: str,
        proxy_url: str,
        work_root: Path,
        *,
        segment_seconds: int = 6,
        ffmpeg: str = "ffmpeg",
        on_error: Callable[[str], None] | None = None,
        fencing_token: int | None = None,
        input_headers: dict[str, str] | None = None,
        require_media_progress: bool = False,
    ):
        if segment_seconds < 2:
            raise ValueError("segment_seconds must be at least 2")
        if not proxy_url:
            raise ValueError("proxy_url is required")
        self.store, self.channel_id, self.proxy_url = store, str(channel_id), proxy_url
        self.work_root = Path(work_root)
        self.segment_seconds, self.ffmpeg, self.on_error = segment_seconds, ffmpeg, on_error
        self.fencing_token = fencing_token
        self.input_headers = self._validate_input_headers(input_headers or {})
        self.require_media_progress = bool(require_media_progress)
        existing = self.store.segments(self.channel_id)
        self._last_end_epoch = max((item.end_utc.timestamp() for item in existing), default=None)
        self._session_anchor: float | None = None
        self._mark_next_discontinuity = False
        self._current_attempt_segment_id: str | None = None
        self._last_commit_monotonic: float | None = None
        self._pending_cut: tuple[str, Path, float] | None = None
        # Archive tail used to trim overlap from a staged input adopted
        # without a proven splice (for example after its predecessor ended).
        self._splice_tail = None

    def command(self, output_dir: Path) -> list[str]:
        output_dir.mkdir(parents=True, exist_ok=True)
        command = [
            self.ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "warning",
        ]
        if self.input_headers:
            header_block = "".join(f"{name}: {value}\r\n" for name, value in self.input_headers.items())
            command.extend(["-headers", header_block])
        command.extend([
            "-i", self.proxy_url,
            # Vu+/DVB transport streams may contain private data PIDs that
            # FFmpeg cannot remux. Keep every audio/video/subtitle track while
            # excluding unsupported data streams.
            "-map", "0:v?", "-map", "0:a?", "-map", "0:s?", "-c", "copy",
            "-f", "segment", "-segment_time", str(self.segment_seconds),
            "-segment_format", "mpegts",
            "-segment_list", str(output_dir / "segments.csv"),
            "-segment_list_type", "csv", str(output_dir / "segment-%06d.ts"),
        ])
        return command

    @staticmethod
    def _validate_input_headers(headers: dict[str, str]) -> dict[str, str]:
        validated = {}
        for name, value in headers.items():
            name, value = str(name), str(value)
            if not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name):
                raise ValueError("invalid FFmpeg input header name")
            if "\r" in value or "\n" in value:
                raise ValueError("invalid FFmpeg input header value")
            validated[name] = value
        return validated

    def _publish_csv_rows(
        self,
        list_path: Path,
        offset: int,
        anchor: float | None = None,
        *,
        require_useful_media: bool | None = None,
        attempt_switch: RecorderAttemptSwitch | None = None,
    ) -> tuple[int, int]:
        if not list_path.exists():
            return offset, 0
        if require_useful_media is None:
            require_useful_media = self.require_media_progress
        with list_path.open("rb") as stream:
            stream.seek(offset)
            chunk = stream.read()
        final_newline = chunk.rfind(b"\n")
        if final_newline < 0:
            return offset, 0
        complete, consumed = chunk[: final_newline + 1], final_newline + 1
        committed = 0
        for row in csv.reader(io.StringIO(complete.decode("utf-8", errors="replace"))):
            if len(row) < 3:
                continue
            name, start_s, end_s = row[0], float(row[1]), float(row[2])
            if end_s <= start_s:
                self._mark_next_discontinuity = True
                continue
            segment_path = Path(name)
            if not segment_path.is_absolute():
                segment_path = list_path.parent / segment_path
            replaced_source = None
            if self._splice_tail is not None and self._pending_cut is None:
                tail, self._splice_tail = self._splice_tail, None
                try:
                    cut = self._prepare_cut(
                        tail, None, segment_path, start_s, end_s, time.monotonic() + 10,
                        output_dir=segment_path.parent,
                    )
                except OSError:
                    cut = None
                if cut is not None and cut[1] is None:
                    # This row repeats only archived media; try the next one.
                    segment_path.unlink(missing_ok=True)
                    self._splice_tail = tail
                    self._mark_next_discontinuity = True
                    continue
                if cut is not None:
                    self._pending_cut = cut
                    if self._session_anchor is None:
                        self._session_anchor = tail.end_utc.timestamp() - (start_s + cut[2])
            if self._pending_cut is not None and segment_path.name == self._pending_cut[0]:
                replaced_source, segment_path, removed = segment_path, self._pending_cut[1], self._pending_cut[2]
                start_s += removed
                if end_s <= start_s:
                    self._mark_next_discontinuity = True
                    continue
            if require_useful_media and not has_useful_transport_stream(segment_path):
                segment_path.unlink(missing_ok=True)
                self._mark_next_discontinuity = True
                continue
            file_mtime = segment_path.stat().st_mtime
            if self._session_anchor is None:
                # The output file's mtime is written when the segment muxer
                # closes it, after the proxy input has connected. This avoids
                # anchoring a delayed stream at the earlier Popen time.
                self._session_anchor = (anchor if anchor is not None else file_mtime - end_s)
            start, end = self._session_anchor + start_s, self._session_anchor + end_s
            # A proxy failover can reset or jump the incoming MPEG-TS PTS while
            # FFmpeg keeps writing the same CSV. Re-anchor at the closed file's
            # wall-clock time so old archive slots are never reused.
            if self._last_end_epoch is not None and (
                start < self._last_end_epoch - 0.5
                or abs(end - file_mtime) > max(30, self.segment_seconds * 5)
            ):
                self._session_anchor = max(
                    file_mtime - end_s, self._last_end_epoch - start_s
                )
                start, end = self._session_anchor + start_s, self._session_anchor + end_s
                self._mark_next_discontinuity = True
            discontinuity = self._mark_next_discontinuity
            if self._last_end_epoch is not None and abs(start - self._last_end_epoch) > 0.5:
                discontinuity = True
            # The segment muxer emits a row only when that segment is closed.
            def publish_segment() -> None:
                published = self.store.add_segment(
                    self.channel_id, segment_path,
                    datetime.fromtimestamp(start, timezone.utc),
                    datetime.fromtimestamp(end, timezone.utc),
                    discontinuity=discontinuity,
                    fencing_token=self.fencing_token,
                )
                self._current_attempt_segment_id = published.id
                self._last_commit_monotonic = time.monotonic() if attempt_switch is not None else None

            if attempt_switch is not None:
                if not attempt_switch.publish_if_current(publish_segment):
                    return offset + consumed, committed
            else:
                publish_segment()
            segment_path.unlink(missing_ok=True)
            if replaced_source is not None:
                replaced_source.unlink(missing_ok=True)
                self._pending_cut = None
            self._last_end_epoch = end
            self._mark_next_discontinuity = False
            committed += 1
        return offset + consumed, committed

    def _start_staged_input(self) -> StagedRecorderInput:
        """Start a private input without letting it publish archive segments."""
        self.work_root.mkdir(parents=True, exist_ok=True)
        output_dir = self.work_root / f"{self.channel_id}-{uuid.uuid4().hex}"
        output_dir.mkdir()
        try:
            process = subprocess.Popen(
                self.command(output_dir), stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        except BaseException:
            shutil.rmtree(output_dir, ignore_errors=True)
            raise
        return StagedRecorderInput(self, output_dir, process)

    @staticmethod
    def _prepare_cut(latest, staged, path, start, end, deadline, *, output_dir=None):
        """Cut a staged row so it resumes exactly after the archive tail.

        Returns ``(name, replacement, seconds_removed)``; ``replacement`` is
        ``None`` when the whole row is already archived.
        """
        from .splice import cut_segment, find_splice

        output_dir = output_dir or staged.output_dir
        splice = find_splice(latest.path, path, deadline=deadline)
        if splice is None:
            return None
        if splice.complete:
            return path.name, None, end - start
        payload = cut_segment(path, splice, deadline)
        if (payload is None or not 0 < splice.cut_offset < end - start
                or time.monotonic() >= deadline
                or sum(item.stat().st_size for item in output_dir.iterdir() if item.is_file())
                + len(payload) > STAGED_INPUT_MAX_BYTES):
            return None
        replacement = output_dir / f".cut-{uuid.uuid4().hex}.ts"
        replacement.write_bytes(payload)
        return path.name, replacement, splice.cut_offset

    def _fallback_offset(self, rows, complete_offset: int) -> int:
        """Resume at the first staged row that does not repeat archived time.

        Without a proven splice, rows that closed before the archive's end are
        still skipped, but later complete rows are kept so the unavoidable gap
        is no longer than the time the comparison itself needed.
        """
        end = self._last_end_epoch
        if end is None:
            return complete_offset
        previous = 0
        for path, start, row_end, row_offset in rows:
            try:
                wall_start = path.stat().st_mtime - (row_end - start)
            except OSError:
                return complete_offset
            if wall_start >= end - 0.5:
                return previous
            previous = row_offset
        return complete_offset

    def _try_synchronized_handover(
        self, target, current_process, list_path, offset, stop_event, switch, notify,
    ):
        """Keep recording while a bounded, content-proven overlap is sought."""
        from ..logging_utils import event
        from .handover import matching_completed_segments

        try:
            staged = target._start_staged_input()
        except Exception:
            event("sync_handover_fallback")
            switch.request()
            return None, offset, 0
        committed_total = 0
        tested = set()
        deadline = time.monotonic() + SYNCHRONIZED_HANDOVER_TIMEOUT_SECONDS
        try:
            while not stop_event.is_set() and not switch.is_set():
                offset, committed = self._publish_csv_rows(
                    list_path, offset, require_useful_media=True, attempt_switch=switch,
                )
                if committed:
                    committed_total += committed
                    notify(committed)
                rows, complete_offset = _staged_csv_rows(staged.output_dir)
                if not staged.within_limits():
                    event("sync_handover_fallback")
                    switch.request()
                    return staged, offset, committed_total
                if current_process.poll() is None and staged.process.poll() is None:
                    previous = self.store.segments(self.channel_id)
                    latest = previous[-1] if previous else None
                    recent = (
                        self._last_commit_monotonic is not None
                        and time.monotonic() - self._last_commit_monotonic
                        <= max(12.0, self.segment_seconds * 2)
                    )
                    if latest is not None and latest.id == self._current_attempt_segment_id and recent:
                        for path, start, end, row_offset in reversed(rows[-3:]):
                            pair = (latest.id, path.name)
                            if pair in tested:
                                continue
                            tested.add(pair)
                            cut = None
                            full = (abs((end - start) - latest.duration) <= 0.025
                                    and matching_completed_segments(latest.path, path, deadline=deadline))
                            if not full:
                                cut = self._prepare_cut(latest, staged, path, start, end, deadline)
                                if cut is None:
                                    continue

                            def still_last():
                                records = self.store.segments(self.channel_id)
                                return bool(
                                    records and records[-1].id == latest.id
                                    and self._current_attempt_segment_id == latest.id
                                    and self._last_commit_monotonic is not None
                                    and time.monotonic() - self._last_commit_monotonic
                                    <= max(12.0, self.segment_seconds * 2)
                                    and not stop_event.is_set()
                                    and current_process.poll() is None
                                    and staged.process.poll() is None
                                )

                            if switch.request_if_current(still_last):
                                if cut is None:
                                    staged.offset = row_offset
                                    staged.anchor = latest.end_utc.timestamp() - end
                                    staged.expected_start = end
                                else:
                                    # Resume inside this row, at the packet after the archive tail.
                                    index = next(i for i, row in enumerate(rows) if row[0] == path)
                                    staged.offset = rows[index - 1][3] if index else 0
                                    staged.anchor = latest.end_utc.timestamp() - (start + cut[2])
                                    staged.expected_start = start
                                    staged.cut = cut
                                staged.matched = True
                                event("sync_handover_overlap_found")
                                return staged, offset, committed_total
                if (current_process.poll() is not None or staged.process.poll() is not None
                        or time.monotonic() >= deadline):
                    # The attempted comparison timed out or an input stopped.
                    # Retain this exact one-use reader for the normal handover.
                    staged.offset = self._fallback_offset(rows, complete_offset)
                    event("sync_handover_fallback")
                    switch.request()
                    return staged, offset, committed_total
                stop_event.wait(0.2)
            if switch.is_set() and not stop_event.is_set():
                _, staged.offset = _staged_csv_rows(staged.output_dir)
                return staged, offset, committed_total
        except Exception:
            # An already launched reader may have consumed its single-use
            # capability. Hand its exact process to normal cleanup; never open
            # that capability for a second time.
            staged.close()
            if not stop_event.is_set():
                event("sync_handover_fallback")
                switch.request()
                return staged, offset, committed_total
        staged.close()
        return None, offset, committed_total

    def prepare_synchronized_return(self, previous_segment_id, stop_event, switch):
        """Try an archive-tail overlap after the previous input's exact cleanup.

        This stages only the already-authorized dedicated source. When it has
        no matching overlap, its recorded media is retained for normal return.
        """
        from ..logging_utils import event
        from .handover import matching_completed_segments

        if (not isinstance(previous_segment_id, str) or not previous_segment_id
                or stop_event.is_set() or switch.is_set()):
            return None
        previous = self.store.segments(self.channel_id)
        if not previous or previous[-1].id != previous_segment_id:
            return None
        latest = previous[-1]
        try:
            staged = self._start_staged_input()
        except Exception:
            event("sync_handover_fallback")
            return None
        deadline = time.monotonic() + SYNCHRONIZED_HANDOVER_TIMEOUT_SECONDS
        tested = set()
        try:
            while not stop_event.is_set() and not switch.is_set():
                rows, _ = _staged_csv_rows(staged.output_dir)
                if not staged.within_limits():
                    event("sync_handover_fallback")
                    return staged
                current = self.store.segments(self.channel_id)
                if not current or current[-1].id != previous_segment_id:
                    break
                for path, start, end, row_offset in rows:
                    if path.name in tested:
                        continue
                    tested.add(path.name)
                    cut = None
                    if not (abs((end - start) - latest.duration) <= 0.025
                            and matching_completed_segments(latest.path, path, deadline=deadline)):
                        cut = self._prepare_cut(latest, staged, path, start, end, deadline)
                        if cut is None:
                            continue
                    current = self.store.segments(self.channel_id)
                    if (current and current[-1].id == previous_segment_id
                            and not stop_event.is_set() and not switch.is_set()
                            and staged.process.poll() is None):
                        if cut is None:
                            staged.offset = row_offset
                            staged.anchor = latest.end_utc.timestamp() - end
                            staged.expected_start = end
                        else:
                            index = next(i for i, row in enumerate(rows) if row[0] == path)
                            staged.offset = rows[index - 1][3] if index else 0
                            staged.anchor = latest.end_utc.timestamp() - (start + cut[2])
                            staged.expected_start = start
                            staged.cut = cut
                        staged.matched = True
                        event("sync_handover_overlap_found")
                        return staged
                if staged.process.poll() is not None or time.monotonic() >= deadline:
                    break
                stop_event.wait(0.2)
            if stop_event.is_set() or switch.is_set():
                staged.close()
                return staged
            event("sync_handover_fallback")
            # This reader was opened after the prior cleanup, so its earliest
            # staged segment is eligible for the normal dedicated fallback.
            return staged
        except Exception:
            staged.close()
            if not stop_event.is_set():
                event("sync_handover_fallback")
            return staged

    def run_candidate(
        self,
        stop_event: threading.Event,
        *,
        startup_timeout: float = 60.0,
        media_idle_timeout: float = DEFAULT_MEDIA_IDLE_TIMEOUT_SECONDS,
        poll_interval: float = 0.5,
        attempt_switch: RecorderAttemptSwitch | None = None,
        on_first_media: Callable[[float], bool] | None = None,
        staged_input: StagedRecorderInput | None = None,
    ) -> RecorderAttemptResult:
        """Run a single source until it stops or useful media stalls.

        Keepalive/null packets are discarded and do not count as media progress.
        A caller can then move to the next policy-approved source.
        """
        if startup_timeout <= 0 or media_idle_timeout <= 0 or poll_interval <= 0:
            raise ValueError("recorder attempt timeouts must be positive")
        if attempt_switch is not None and not isinstance(attempt_switch, RecorderAttemptSwitch):
            raise ValueError("attempt_switch must be a RecorderAttemptSwitch")
        if on_first_media is not None and not callable(on_first_media):
            raise ValueError("on_first_media must be callable")
        if staged_input is not None and (
            not isinstance(staged_input, StagedRecorderInput)
            or staged_input.recorder is not self
        ):
            raise ValueError("staged input belongs to another recorder")
        if staged_input is not None and staged_input.consumed:
            status = (
                "stopped" if stop_event.is_set()
                else "switch_requested" if attempt_switch is not None and attempt_switch.is_set()
                else "no_media"
            )
            return RecorderAttemptResult(
                status, 0, staged_input.process.poll(),
                attempt_switch_id=(attempt_switch.attempt_id if status == "switch_requested" else None),
            )
        if staged_input is None:
            self.work_root.mkdir(parents=True, exist_ok=True)
            output_dir = self.work_root / f"{self.channel_id}-{uuid.uuid4().hex}"
            output_dir.mkdir()
        else:
            output_dir = staged_input.output_dir
            # The old attempt may have published after this object was made.
            existing = self.store.segments(self.channel_id)
            self._last_end_epoch = max((item.end_utc.timestamp() for item in existing), default=None)
        list_path = output_dir / "segments.csv"
        offset = staged_input.offset if staged_input is not None else 0
        useful = 0
        self._session_anchor = staged_input.anchor if staged_input is not None else None
        self._current_attempt_segment_id = None
        self._last_commit_monotonic = None
        expected_start = staged_input.expected_start if staged_input is not None else None
        # A new muxer has a fresh MPEG clock even if wall time is contiguous.
        # Explicitly separate it from already archived transport timestamps.
        if self._last_end_epoch is not None:
            self._mark_next_discontinuity = True
        process = staged_input.process if staged_input is not None else None
        self._pending_cut = staged_input.cut if staged_input is not None else None
        self._splice_tail = None
        if staged_input is not None and not staged_input.matched and self.store.segments(self.channel_id):
            tail = self.store.segments(self.channel_id)[-1]
            if time.time() - tail.end_utc.timestamp() < 120 and tail.path.is_file():
                self._splice_tail = tail
        if staged_input is not None:
            staged_input.consumed = True
        next_input = None
        start_clock = time.monotonic()
        last_useful_at = start_clock
        status = "exited"
        return_code = None
        first_media_notified = False
        first_media_at: float | None = None
        synchronized_media_reported = False

        def notify_first_media(committed: int) -> None:
            nonlocal first_media_at, first_media_notified, synchronized_media_reported
            if (committed and staged_input is not None and staged_input.matched
                    and not synchronized_media_reported):
                from ..logging_utils import event

                event("sync_handover_media_committed")
                synchronized_media_reported = True
            if committed and not first_media_notified:
                if first_media_at is None:
                    # A retry acknowledges the first useful committed
                    # segment, not a later wall-clock time.
                    first_media_at = time.time()
                if on_first_media is None:
                    first_media_notified = True
                    return
                try:
                    # The callback commits the sharing return authority. A
                    # transient denial must be retried on a later committed
                    # segment; publishing media alone is not its ACK.
                    first_media_notified = on_first_media(first_media_at) is True
                except Exception:
                    first_media_notified = False

        try:
            if process is None:
                process = subprocess.Popen(
                    self.command(output_dir),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            while process.poll() is None and not stop_event.is_set():
                if attempt_switch is not None and attempt_switch.is_set():
                    status = "switch_requested"
                    break
                if attempt_switch is not None:
                    target = attempt_switch.take_synchronized_input()
                    if target is not None:
                        next_input, offset, additional = self._try_synchronized_handover(
                            target, process, list_path, offset, stop_event, attempt_switch,
                            notify_first_media,
                        )
                        useful += additional
                        if additional:
                            last_useful_at = time.monotonic()
                        if attempt_switch.is_set() or stop_event.is_set():
                            status = "switch_requested" if not stop_event.is_set() else "stopped"
                            break
                if expected_start is not None:
                    rows, _ = _staged_csv_rows(output_dir, offset)
                    if rows:
                        if abs(rows[0][1] - expected_start) > 0.025:
                            status = "no_media"
                            break
                        expected_start = None
                offset, committed = self._publish_csv_rows(
                    list_path, offset, require_useful_media=True,
                    attempt_switch=attempt_switch,
                )
                if committed:
                    # These rows are already committed under the switch
                    # publication barrier. Count them even if the requester
                    # sets the signal immediately after that commit.
                    useful += committed
                    last_useful_at = time.monotonic()
                    notify_first_media(committed)
                if attempt_switch is not None and attempt_switch.is_set():
                    status = "switch_requested"
                    break
                now = time.monotonic()
                if useful == 0 and now - start_clock >= startup_timeout:
                    status = "no_media"
                    break
                if useful and now - last_useful_at >= media_idle_timeout:
                    status = "media_stalled"
                    break
                if attempt_switch is None:
                    stop_event.wait(poll_interval)
                else:
                    _wait_for_switch_or_stop(stop_event, attempt_switch, poll_interval)

            if stop_event.is_set():
                status = "stopped"
            elif attempt_switch is not None and attempt_switch.is_set():
                status = "switch_requested"
            if process.poll() is None:
                if status == "exited":
                    status = "no_media" if useful == 0 else "media_stalled"
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            return_code = process.poll()
            if status != "switch_requested" and not (staged_input is not None and stop_event.is_set()):
                # A proven staged boundary must be checked before final flush.
                rows, _ = _staged_csv_rows(output_dir, offset) if expected_start is not None else ([], offset)
                if expected_start is None or (rows and abs(rows[0][1] - expected_start) <= 0.025):
                    offset, committed = self._publish_csv_rows(
                        list_path, offset, require_useful_media=True,
                        attempt_switch=attempt_switch,
                    )
                    useful += committed
                    notify_first_media(committed)
            elif status == "switch_requested":
                # A new source attempt starts a fresh mux session. Do not flush
                # any old-input rows after the switch was requested.
                self._mark_next_discontinuity = True
            if status == "exited" and useful == 0:
                status = "no_media"
            result = RecorderAttemptResult(
                status, useful, return_code,
                attempt_switch_id=(
                    attempt_switch.attempt_id
                    if status == "switch_requested" and attempt_switch is not None
                    else None
                ),
                staged_input=next_input if status == "switch_requested" else None,
            )
            if status == "switch_requested":
                next_input = None  # Ownership moved to the returned result.
            return result
        finally:
            if next_input is not None:
                next_input.close()
            if process is not None and process.poll() is None:
                process.kill()
                process.wait()
            shutil.rmtree(output_dir, ignore_errors=True)

    def run_once(self, stop_event: threading.Event) -> int:
        """Run FFmpeg until stopped or it exits; return committed segment count."""
        self.work_root.mkdir(parents=True, exist_ok=True)
        output_dir = self.work_root / f"{self.channel_id}-{uuid.uuid4().hex}"
        output_dir.mkdir()
        list_path = output_dir / "segments.csv"
        offset = count = 0
        self._session_anchor = None
        # A new muxer has a fresh MPEG clock even if wall time is contiguous.
        # Explicitly separate it from already archived transport timestamps.
        if self._last_end_epoch is not None:
            self._mark_next_discontinuity = True
        process = None
        try:
            process = subprocess.Popen(self.command(output_dir), stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            while process.poll() is None and not stop_event.wait(0.5):
                offset, committed = self._publish_csv_rows(list_path, offset)
                count += committed
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            offset, committed = self._publish_csv_rows(list_path, offset)
            count += committed
            return count
        finally:
            if process is not None and process.poll() is None:
                process.kill()
                process.wait()
            shutil.rmtree(output_dir, ignore_errors=True)

    def run_forever(self, stop_event: threading.Event, *, initial_backoff: float = 1.0, max_backoff: float = 30.0) -> None:
        """Restart failed FFmpeg processes with bounded exponential backoff."""
        backoff = initial_backoff
        while not stop_event.is_set():
            try:
                self.run_once(stop_event)
                if stop_event.is_set():
                    break
                message = "FFmpeg exited; restarting recorder"
            except Exception as exc:  # the caller owns service-level logging
                message = f"Recorder failed: {exc}"
            self._mark_next_discontinuity = True
            if self.on_error:
                self.on_error(message)
            if stop_event.wait(backoff):
                break
            backoff = min(max_backoff, backoff * 2)


def has_useful_transport_stream(path: Path) -> bool:
    """Reject FFmpeg segments made only from Dispatcharr null keepalives."""
    try:
        data = Path(path).read_bytes()
    except OSError:
        return False
    if len(data) < 188 * 2:
        return False
    counts: dict[int, int] = {}
    starts: set[int] = set()
    for offset in range(0, len(data) - 187, 188):
        packet = data[offset : offset + 188]
        if packet[0] != 0x47:
            return False
        pid = ((packet[1] & 0x1F) << 8) | packet[2]
        adaptation_control = (packet[3] >> 4) & 0x03
        if pid >= 0x1FFF or pid <= 0x20 or adaptation_control not in (1, 3):
            continue
        payload_offset = 4
        if adaptation_control == 3:
            payload_offset += 1 + packet[4]
        if payload_offset >= 188:
            continue
        counts[pid] = counts.get(pid, 0) + 1
        if packet[1] & 0x40:
            starts.add(pid)
    return any(count >= 2 and pid in starts for pid, count in counts.items())
