"""FFmpeg stream-copy recorder for a Dispatcharr proxy URL."""

from __future__ import annotations

import csv
import io
import os
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .store import ArchiveStore, Segment


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
    ):
        if segment_seconds < 2:
            raise ValueError("segment_seconds must be at least 2")
        if not proxy_url:
            raise ValueError("proxy_url is required")
        self.store, self.channel_id, self.proxy_url = store, str(channel_id), proxy_url
        self.work_root = Path(work_root)
        self.segment_seconds, self.ffmpeg, self.on_error = segment_seconds, ffmpeg, on_error
        self.fencing_token = fencing_token

    def command(self, output_dir: Path) -> list[str]:
        output_dir.mkdir(parents=True, exist_ok=True)
        return [
            self.ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "warning",
            "-i", self.proxy_url, "-map", "0", "-c", "copy",
            "-f", "segment", "-segment_time", str(self.segment_seconds),
            "-segment_format", "mpegts", "-reset_timestamps", "1",
            "-segment_list", str(output_dir / "segments.csv"),
            "-segment_list_type", "csv", str(output_dir / "segment-%06d.ts"),
        ]

    def _publish_csv_rows(self, list_path: Path, offset: int, anchor: float) -> tuple[int, int]:
        if not list_path.exists():
            return offset, 0
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
            segment_path = Path(name)
            if not segment_path.is_absolute():
                segment_path = list_path.parent / segment_path
            start, end = anchor + start_s, anchor + end_s
            # The segment muxer emits a row only when that segment is closed.
            self.store.add_segment(
                self.channel_id, segment_path,
                datetime.fromtimestamp(start, timezone.utc),
                datetime.fromtimestamp(end, timezone.utc),
                discontinuity=False,
                fencing_token=self.fencing_token,
            )
            segment_path.unlink(missing_ok=True)
            committed += 1
        return offset + consumed, committed

    def run_once(self, stop_event: threading.Event) -> int:
        """Run FFmpeg until stopped or it exits; return committed segment count."""
        self.work_root.mkdir(parents=True, exist_ok=True)
        output_dir = self.work_root / f"{self.channel_id}-{uuid.uuid4().hex}"
        output_dir.mkdir()
        list_path = output_dir / "segments.csv"
        anchor = time.time()
        process = subprocess.Popen(self.command(output_dir), stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        offset = count = 0
        try:
            while process.poll() is None and not stop_event.wait(0.5):
                offset, committed = self._publish_csv_rows(list_path, offset, anchor)
                count += committed
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            offset, committed = self._publish_csv_rows(list_path, offset, anchor)
            count += committed
            return count
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()

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
            if self.on_error:
                self.on_error(message)
            if stop_event.wait(backoff):
                break
            backoff = min(max_backoff, backoff * 2)
