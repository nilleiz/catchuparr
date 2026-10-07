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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .store import ArchiveStore


@dataclass(frozen=True)
class RecorderAttemptResult:
    """Outcome of one bounded source attempt."""

    status: str
    useful_segments: int
    return_code: int | None


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
            self.store.add_segment(
                self.channel_id, segment_path,
                datetime.fromtimestamp(start, timezone.utc),
                datetime.fromtimestamp(end, timezone.utc),
                discontinuity=discontinuity,
                fencing_token=self.fencing_token,
            )
            segment_path.unlink(missing_ok=True)
            self._last_end_epoch = end
            self._mark_next_discontinuity = False
            committed += 1
        return offset + consumed, committed

    def run_candidate(
        self,
        stop_event: threading.Event,
        *,
        startup_timeout: float = 60.0,
        media_idle_timeout: float = 120.0,
        poll_interval: float = 0.5,
    ) -> RecorderAttemptResult:
        """Run a single source until it stops or useful media stalls.

        Keepalive/null packets are discarded and do not count as media progress.
        A caller can then move to the next policy-approved source.
        """
        if startup_timeout <= 0 or media_idle_timeout <= 0 or poll_interval <= 0:
            raise ValueError("recorder attempt timeouts must be positive")
        self.work_root.mkdir(parents=True, exist_ok=True)
        output_dir = self.work_root / f"{self.channel_id}-{uuid.uuid4().hex}"
        output_dir.mkdir()
        list_path = output_dir / "segments.csv"
        offset = useful = 0
        self._session_anchor = None
        process = None
        start_clock = time.monotonic()
        last_useful_at = start_clock
        status = "exited"
        return_code = None
        try:
            process = subprocess.Popen(
                self.command(output_dir),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            while process.poll() is None and not stop_event.is_set():
                offset, committed = self._publish_csv_rows(
                    list_path, offset, require_useful_media=True
                )
                if committed:
                    useful += committed
                    last_useful_at = time.monotonic()
                now = time.monotonic()
                if useful == 0 and now - start_clock >= startup_timeout:
                    status = "no_media"
                    break
                if useful and now - last_useful_at >= media_idle_timeout:
                    status = "media_stalled"
                    break
                stop_event.wait(poll_interval)

            if stop_event.is_set():
                status = "stopped"
            elif process.poll() is None and status == "exited":
                process.terminate()
                status = "no_media" if useful == 0 else "media_stalled"
            if process.poll() is None:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            return_code = process.poll()
            offset, committed = self._publish_csv_rows(
                list_path, offset, require_useful_media=True
            )
            useful += committed
            if status == "exited" and useful == 0:
                status = "no_media"
            return RecorderAttemptResult(status, useful, return_code)
        finally:
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
