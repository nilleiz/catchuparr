"""Exercise source-policy recorder failover with synthetic media in AIO."""

from __future__ import annotations

import array
import json
import math
import os
import shutil
import subprocess
import sys
import threading
import time
import types
import uuid
from collections.abc import Mapping
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from socketserver import ThreadingMixIn
from typing import Callable
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

STARTUP_TIMEOUT = 45.0
MEDIA_IDLE_TIMEOUT = 25.0
POLL_INTERVAL = 0.2
SOURCE_DURATION = 8.0
SOURCE_CHUNK_PACKETS = 32
STALL_SOURCE_REPEATS = 4
BRIDGE_DRAIN_TIMEOUT = 5.0
SYNTHETIC_SOURCE_TONES = {
    "Synthetic Source B": 880,
    "Synthetic Source C": 660,
    "Synthetic Source D": 440,
}
TONE_MATCH_TOLERANCE_HZ = 45
TONE_SAMPLE_RATE = 48_000
FAILOVER_DIAGNOSTIC_PREFIX = "CATCHUPARR_AIO_FAILOVER_TIMEOUT "
FAILOVER_DIAGNOSTIC_MAX_BYTES = 8192
_SOURCE_ROLE_NAMES = {
    "b": "Synthetic Source B",
    "c": "Synthetic Source C",
    "d": "Synthetic Source D",
}
_NATIVE_STATE_CATEGORIES = frozenset(
    {
        "active", "buffering", "connecting", "error", "initializing",
        "stopped", "stopping", "waiting_for_clients",
    }
)
_MAX_NATIVE_WORKER_RECORDS_SCANNED = 256
_NATIVE_DIAGNOSTIC_RECORD_FIELDS = frozenset(
    {"channel_uuid", "created_at", "stream_id", "worker_id"}
)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _recorder_attempt_summary(results: list[object]) -> list[dict[str, object]]:
    """Return bounded status/count fields without exposing URLs or identifiers."""
    known_statuses = {"exited", "no_media", "media_stalled", "stopped"}
    summaries = []
    if not isinstance(results, (list, tuple)):
        return summaries
    for result in results[:16]:
        try:
            status = getattr(result, "status", None)
        except Exception:
            status = None
        if not isinstance(status, str) or status not in known_statuses:
            status = "other"
        try:
            useful_segments = max(0, int(getattr(result, "useful_segments", -1)))
        except Exception:
            useful_segments = None
        summaries.append({
            "status": status,
            "useful_segments": useful_segments,
        })
    return summaries


def _count_category(value) -> str:
    try:
        count = int(value)
    except (TypeError, ValueError, OverflowError):
        return "unavailable"
    if count <= 0:
        return "zero"
    if count == 1:
        return "one"
    return "many"


def _state_category(value) -> str:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return "other"
    if not isinstance(value, str):
        return "missing" if value is None else "other"
    normalized = value.casefold()
    return normalized if normalized in _NATIVE_STATE_CATEGORIES else "other"


def _safe_count(value) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        count = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return count


def _native_source_role_observations(
    redis_client,
    records: list[dict[str, str]],
    source_stream_ids: Mapping[str, str],
    *,
    redis_keys,
    metadata_state_field,
    native_server,
) -> dict[str, dict[str, object]]:
    """Collect fixed categories for synthetic roles without exposing native IDs."""
    from catchuparr.adapters.recorder_proxy import is_managed_worker_id

    observations: dict[str, dict[str, object]] = {}
    for role in ("a", "b", "c", "d"):
        stream_id = source_stream_ids.get(role)
        matching = [
            record for record in records
            if isinstance(record, dict) and record.get("stream_id") == stream_id
        ] if isinstance(stream_id, str) else []
        try:
            matching.sort(key=lambda record: float(record.get("created_at", "0")))
        except (TypeError, ValueError, OverflowError):
            pass
        selected = matching[-1] if matching else None
        worker_id = selected.get("worker_id") if isinstance(selected, dict) else None
        if not is_managed_worker_id(worker_id):
            observations[role] = {
                "worker_records": len(matching),
                "owner_present": None,
                "owner_matches_local": None,
                "metadata_exists": None,
                "stopping": None,
                "client_count": "unavailable",
                "state": "unavailable",
                "manager_running": None,
            }
            continue

        metadata_key = redis_keys.channel_metadata(worker_id)
        owner_key = redis_keys.channel_owner(worker_id)
        stopping_key = redis_keys.channel_stopping(worker_id)
        clients_key = redis_keys.clients(worker_id)
        try:
            metadata_exists = bool(redis_client.exists(metadata_key))
        except Exception:
            metadata_exists = None
        try:
            stopping = bool(redis_client.exists(stopping_key))
        except Exception:
            stopping = None
        try:
            client_count = _count_category(redis_client.scard(clients_key))
        except Exception:
            client_count = "unavailable"
        try:
            owner = redis_client.get(owner_key)
            owner_present = owner is not None
        except Exception:
            owner = None
            owner_present = None
        local_worker_id = getattr(native_server, "worker_id", None)
        if owner is None or local_worker_id is None:
            owner_matches_local = None
        else:
            if isinstance(owner, bytes):
                try:
                    owner = owner.decode("utf-8")
                except UnicodeDecodeError:
                    owner = None
            owner_matches_local = owner == local_worker_id if owner is not None else None
        try:
            state = _state_category(
                redis_client.hget(metadata_key, metadata_state_field)
                if metadata_exists else None
            ) if metadata_exists is not None else "unavailable"
        except Exception:
            state = "unavailable"

        try:
            if native_server is None:
                manager_running = None
            else:
                thread = native_server._get_stream_thread(worker_id)
                manager_running = bool(thread is not None and thread.is_alive())
        except Exception:
            manager_running = None
        observations[role] = {
            "worker_records": len(matching),
            "owner_present": owner_present,
            "owner_matches_local": owner_matches_local,
            "metadata_exists": metadata_exists,
            "stopping": stopping,
            "client_count": client_count,
            "state": state,
            "manager_running": manager_running,
        }
    return observations


def _native_source_observations(
    redis_client, channel_uuid: str, source_stream_ids: Mapping[str, str],
) -> dict[str, dict[str, object]]:
    unavailable = {
        role: {
            "worker_records": 0,
            "owner_present": None,
            "owner_matches_local": None,
            "metadata_exists": None,
            "stopping": None,
            "client_count": "unavailable",
            "state": "unavailable",
            "manager_running": None,
        }
        for role in ("a", "b", "c", "d")
    }
    try:
        from apps.proxy.live_proxy.constants import ChannelMetadataField
        from apps.proxy.live_proxy.redis_keys import RedisKeys
        from apps.proxy.live_proxy.server import ProxyServer

        from catchuparr.adapters.recorder_proxy import (
            PLUGIN_REDIS_PREFIX,
            is_managed_worker_id,
            worker_id_key,
        )

        records = []
        pattern = f"{PLUGIN_REDIS_PREFIX}worker:*"
        scanned = 0
        for raw_key in redis_client.scan_iter(match=pattern):
            if scanned >= _MAX_NATIVE_WORKER_RECORDS_SCANNED:
                break
            scanned += 1
            key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else str(raw_key)
            worker_id = key.rsplit(":", 1)[-1]
            if not is_managed_worker_id(worker_id):
                continue
            raw_record = redis_client.hgetall(worker_id_key(worker_id))
            record = {}
            for raw_field, raw_value in raw_record.items():
                field = raw_field.decode("utf-8") if isinstance(raw_field, bytes) else str(raw_field)
                if field not in _NATIVE_DIAGNOSTIC_RECORD_FIELDS:
                    continue
                value = raw_value.decode("utf-8") if isinstance(raw_value, bytes) else str(raw_value)
                record[field] = value
            if record and record.get("channel_uuid") == str(channel_uuid):
                records.append(record)
        native_server = getattr(ProxyServer, "_instance", None)
        return _native_source_role_observations(
            redis_client,
            records,
            source_stream_ids,
            redis_keys=RedisKeys,
            metadata_state_field=ChannelMetadataField.STATE,
            native_server=native_server,
        )
    except Exception:
        return unavailable


def _runtime_timeout_diagnostic(
    run,
    source_server,
    source_baseline,
    stall_baseline,
    verified_segments,
    redis_client,
    channel_uuid: str,
    source_stream_ids: Mapping[str, str],
) -> dict[str, object]:
    """Build a bounded, allowlisted snapshot from already observed probe state."""
    try:
        attempts = _recorder_attempt_summary(run.results)
    except Exception:
        attempts = []

    source_deltas = {}
    try:
        request_counts, active_counts = source_server.snapshot()
        for role in ("a", "b", "c", "d"):
            source_name = f"source-{role}.ts"
            base_requests = _safe_count(source_baseline[0].get(source_name))
            current_requests = _safe_count(request_counts.get(source_name))
            base_active = _safe_count(source_baseline[1].get(source_name))
            current_active = _safe_count(active_counts.get(source_name))
            source_deltas[role] = {
                "request_delta": (
                    current_requests - base_requests
                    if current_requests is not None and base_requests is not None else None
                ),
                "active_delta": (
                    current_active - base_active
                    if current_active is not None and base_active is not None else None
                ),
            }
    except Exception:
        source_deltas = {
            role: {"request_delta": None, "active_delta": None}
            for role in ("a", "b", "c", "d")
        }

    stall = {
        "started_delta": None,
        "duration_ms": None,
        "null_bytes_delta": None,
    }
    try:
        current_stall = source_server.stall_snapshot().get("source-b.ts", {})
        base_started = _safe_count(stall_baseline.get("started"))
        current_started = _safe_count(current_stall.get("started"))
        base_bytes = _safe_count(stall_baseline.get("null_bytes"))
        current_bytes = _safe_count(current_stall.get("null_bytes"))
        try:
            duration_delta = float(current_stall.get("duration_seconds", 0)) - float(
                stall_baseline.get("duration_seconds", 0)
            )
            duration_ms = round(duration_delta * 1000)
        except (TypeError, ValueError, OverflowError):
            duration_ms = None
        stall = {
            "started_delta": (
                current_started - base_started
                if current_started is not None and base_started is not None else None
            ),
            "duration_ms": duration_ms,
            "null_bytes_delta": (
                current_bytes - base_bytes
                if current_bytes is not None and base_bytes is not None else None
            ),
        }
    except Exception:
        pass

    indexed_counts = {
        role: len(verified_segments.get(source_name, ()))
        for role, source_name in _SOURCE_ROLE_NAMES.items()
    }
    try:
        native = _native_source_observations(
            redis_client, channel_uuid, source_stream_ids
        )
    except Exception:
        native = {}
    return {
        "attempts": attempts,
        "source_deltas": source_deltas,
        "b_stall": stall,
        "indexed_segments": indexed_counts,
        "native": native,
    }


def _emit_timeout_diagnostic(diagnostic: dict[str, object]) -> None:
    payload = json.dumps(diagnostic, sort_keys=True, separators=(",", ":"))
    if len(payload.encode("utf-8")) > FAILOVER_DIAGNOSTIC_MAX_BYTES:
        payload = '{"diagnostic":"omitted"}'
    print(FAILOVER_DIAGNOSTIC_PREFIX + payload, file=sys.stderr, flush=True)


def _cleanup_failed_recorder_run(run: _RecorderTaskRun) -> None:
    """Keep the existing bounded stop/join cleanup for wait failures."""
    run.stop()
    if run.thread is not None:
        run.thread.join(20)


def _make_paced_transport_stream(
    ffmpeg: str,
    ffprobe: str,
    output: Path,
    *,
    service_name: str,
    frequency: int,
) -> bytes:
    """Build a near-real-time CBR transport stream with useful A/V frames."""
    command = [
        ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i",
        f"testsrc2=size=320x240:rate=25:duration={SOURCE_DURATION}",
        "-f", "lavfi", "-i",
        f"sine=frequency={frequency}:sample_rate=48000:duration={SOURCE_DURATION}",
        "-shortest", "-c:v", "mpeg2video", "-pix_fmt", "yuv420p", "-g", "12",
        "-b:v", "700k", "-minrate", "700k", "-maxrate", "700k", "-bufsize", "1400k",
        "-c:a", "mp2", "-b:a", "128k", "-muxrate", "960k",
        "-metadata", f"service_name={service_name}",
        "-metadata", "service_provider=Synthetic Catchuparr CI",
        "-f", "mpegts", str(output),
    ]
    try:
        subprocess.run(
            command,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=30,
        )
        payload = output.read_bytes()
        _require(len(payload) >= 188 * 100, "Synthetic failover media is too small")
        _require(len(payload) % 188 == 0, "Synthetic failover media is not packet aligned")
        result = subprocess.run(
            [
                ffprobe, "-v", "error", "-count_frames", "-show_programs", "-show_streams",
                "-of", "json", str(output),
            ],
            check=True,
            capture_output=True,
            timeout=15,
        )
        details = json.loads(result.stdout.decode("utf-8"))
        _require(
            any(
                program.get("tags", {}).get("service_name") == service_name
                for program in details.get("programs", [])
            ),
            "Synthetic failover fixture lost its source identifier",
        )
        frame_counts = {
            item.get("codec_type"): int(item.get("nb_read_frames", "0") or 0)
            for item in details.get("streams", [])
        }
        _require(
            frame_counts.get("video", 0) > 0 and frame_counts.get("audio", 0) > 0,
            "Synthetic failover fixture must contain decoded audio and video",
        )
        return payload
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Synthetic failover media generation timed out") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError("Synthetic failover media generation failed") from exc


def _null_transport_stream() -> bytes:
    packet = bytes((0x47, 0x1F, 0xFF, 0x10)) + b"\xff" * 184
    return packet * 512


class _SyntheticFailoverSourceServer(ThreadingHTTPServer):
    """Serve paced fixture TS with finite and null-only stall modes."""

    daemon_threads = True
    block_on_close = False
    allow_reuse_address = True

    def __init__(self, payloads: dict[str, bytes], durations: dict[str, float]):
        self.payloads = dict(payloads)
        self.bytes_per_second = {
            name: max(1.0, len(payload) / float(durations[name]))
            for name, payload in payloads.items()
        }
        self._lock = threading.Lock()
        self._modes = {name: "continuous" for name in payloads}
        self._finite_repeats = {name: 0 for name in payloads}
        self._finite_claimed: set[str] = set()
        self._request_counts = {name: 0 for name in payloads}
        self._active_counts = {name: 0 for name in payloads}
        self._stall_started_counts = {name: 0 for name in payloads}
        self._stall_durations = {name: 0.0 for name in payloads}
        self._stall_null_bytes = {name: 0 for name in payloads}
        self.null_payload = _null_transport_stream()
        self._sockets: set[object] = set()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                name = self.path.split("?", 1)[0].lstrip("/")
                with owner._lock:
                    payload = owner.payloads.get(name)
                    if payload is None:
                        mode = "missing"
                    else:
                        mode = owner._modes[name]
                    owner._request_counts[name] = owner._request_counts.get(name, 0) + 1
                    finite_repeats = owner._finite_repeats.get(name, 0)
                    finite_already_claimed = name in owner._finite_claimed
                    finite_mode = mode in {"finite", "stall"}
                    if finite_mode and not finite_already_claimed:
                        owner._finite_claimed.add(name)
                    should_serve = mode in {"continuous", "finite", "stall"} and not (
                        finite_mode and finite_already_claimed
                    )
                    if should_serve:
                        owner._active_counts[name] = owner._active_counts.get(name, 0) + 1
                        owner._sockets.add(self.connection)

                if not should_serve:
                    self.send_error(404 if mode == "missing" else 503)
                    return

                self.send_response(200)
                self.send_header("Content-Type", "video/mp2t")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                stall_started_at = None
                try:
                    repeats = finite_repeats if mode in {"finite", "stall"} else None
                    repetition = 0
                    while repeats is None or repetition < repeats:
                        for offset in range(0, len(payload), 188 * SOURCE_CHUNK_PACKETS):
                            chunk = payload[offset:offset + 188 * SOURCE_CHUNK_PACKETS]
                            self.wfile.write(chunk)
                            self.wfile.flush()
                            time.sleep(len(chunk) / owner.bytes_per_second[name])
                        repetition += 1
                    if mode == "stall":
                        stall_started_at = time.monotonic()
                        with owner._lock:
                            owner._stall_started_counts[name] += 1
                        while True:
                            for offset in range(
                                0,
                                len(owner.null_payload),
                                188 * SOURCE_CHUNK_PACKETS,
                            ):
                                chunk = owner.null_payload[
                                    offset:offset + 188 * SOURCE_CHUNK_PACKETS
                                ]
                                self.wfile.write(chunk)
                                self.wfile.flush()
                                with owner._lock:
                                    owner._stall_null_bytes[name] += len(chunk)
                                time.sleep(len(chunk) / owner.bytes_per_second[name])
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                finally:
                    with owner._lock:
                        owner._active_counts[name] -= 1
                        owner._sockets.discard(self.connection)
                        if stall_started_at is not None:
                            owner._stall_durations[name] += (
                                time.monotonic() - stall_started_at
                            )

            def do_HEAD(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                name = self.path.split("?", 1)[0].lstrip("/")
                with owner._lock:
                    if name not in owner.payloads:
                        self.send_error(404)
                        return
                self.send_response(200)
                self.send_header("Content-Type", "video/mp2t")
                self.end_headers()

            def log_message(self, _format: str, *_args) -> None:
                return

        super().__init__(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(
            target=self.serve_forever,
            name="catchuparr-synthetic-failover-source",
            daemon=True,
        )

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.server_port}"

    def set_mode(self, name: str, mode: str, *, repeats: int = 0) -> None:
        if mode not in {"continuous", "finite", "stall", "unavailable"}:
            raise ValueError("invalid synthetic source mode")
        if mode in {"finite", "stall"} and repeats < 1:
            raise ValueError("finite or stall synthetic source requires repeats")
        with self._lock:
            self._modes[name] = mode
            self._finite_repeats[name] = int(repeats)
            self._finite_claimed.discard(name)

    def snapshot(self) -> tuple[dict[str, int], dict[str, int]]:
        with self._lock:
            return dict(self._request_counts), dict(self._active_counts)

    def stall_snapshot(self) -> dict[str, dict[str, float | int]]:
        with self._lock:
            return {
                name: {
                    "started": self._stall_started_counts[name],
                    "duration_seconds": self._stall_durations[name],
                    "null_bytes": self._stall_null_bytes[name],
                }
                for name in self.payloads
            }

    def close(self) -> None:
        if self.thread.is_alive():
            self.shutdown()
        self.server_close()
        self.thread.join(timeout=5)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline and any(self.snapshot()[1].values()):
            time.sleep(0.05)
        if any(self.snapshot()[1].values()):
            with self._lock:
                sockets = tuple(self._sockets)
            for sock in sockets:
                try:
                    sock.shutdown(2)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and any(self.snapshot()[1].values()):
                time.sleep(0.05)


class _ThreadedWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True
    block_on_close = False
    allow_reuse_address = True

    def __init__(self, *args, **kwargs):
        self._active_lock = threading.Lock()
        self._active_requests = 0
        self._request_sockets: set[object] = set()
        super().__init__(*args, **kwargs)

    def process_request_thread(self, request, client_address):
        with self._active_lock:
            self._active_requests += 1
            self._request_sockets.add(request)
        try:
            super().process_request_thread(request, client_address)
        finally:
            with self._active_lock:
                self._active_requests -= 1
                self._request_sockets.discard(request)

    def active_requests(self) -> int:
        with self._active_lock:
            return self._active_requests

    def close_active_requests(self) -> None:
        with self._active_lock:
            requests = tuple(self._request_sockets)
        for request in requests:
            try:
                request.shutdown(2)
            except OSError:
                pass
            try:
                request.close()
            except OSError:
                pass


class _QuietWSGIRequestHandler(WSGIRequestHandler):
    def log_message(self, _format: str, *_args) -> None:
        return


class _DjangoHTTPBridge:
    """Serve actual Django routes on a bounded threaded loopback listener."""

    def __init__(self):
        from django.core.handlers.wsgi import WSGIHandler

        self.server = make_server(
            "127.0.0.1",
            0,
            WSGIHandler(),
            server_class=_ThreadedWSGIServer,
            handler_class=_QuietWSGIRequestHandler,
        )
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            name="catchuparr-recorder-route-bridge",
            daemon=True,
        )

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}"

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        if self.thread.is_alive():
            self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline and self.server.active_requests():
            time.sleep(0.05)
        if self.server.active_requests():
            self.server.close_active_requests()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and self.server.active_requests():
                time.sleep(0.05)


def _wait_for_bridge_idle(bridge: _DjangoHTTPBridge, *, timeout: float) -> int:
    """Wait for the WSGI handler to finish closing its streaming response."""
    if timeout <= 0:
        raise ValueError("bridge drain timeout must be positive")
    deadline = time.monotonic() + timeout
    active = bridge.server.active_requests()
    while active and time.monotonic() < deadline:
        time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        active = bridge.server.active_requests()
    return active


def _segment_has_useful_av(ffprobe: str, path: Path) -> bool:
    try:
        result = subprocess.run(
            [
                ffprobe, "-v", "error", "-count_frames", "-show_programs", "-show_streams",
                "-of", "json", str(path),
            ],
            capture_output=True,
            timeout=10,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Indexed synthetic segment inspection timed out") from exc
    if result.returncode != 0:
        raise RuntimeError("Indexed synthetic segment was not decodable")
    details = json.loads(result.stdout.decode("utf-8"))
    frame_counts = {
        item.get("codec_type"): int(item.get("nb_read_frames", "0") or 0)
        for item in details.get("streams", [])
    }
    return frame_counts.get("video", 0) > 0 and frame_counts.get("audio", 0) > 0


def _estimate_tone_frequency(pcm: bytes, sample_rate: int = TONE_SAMPLE_RATE) -> int | None:
    """Estimate a fixture's sine tone from decoded signed 16-bit little-endian PCM."""
    usable_bytes = len(pcm) - (len(pcm) % 2)
    if usable_bytes < max(2, sample_rate // 50 * 2):
        return None
    samples = array.array("h")
    samples.frombytes(pcm[:usable_bytes])
    if sys.byteorder != "little":
        samples.byteswap()

    mean = sum(samples) / len(samples)
    rms = math.sqrt(
        sum((sample - mean) ** 2 for sample in samples) / len(samples)
    )
    if rms < 100:
        return None
    threshold = max(20.0, rms * 0.08)
    armed = False
    negative_index = 0
    negative_value = 0.0
    positive_crossings = []
    for index, sample in enumerate(samples):
        value = sample - mean
        if value <= -threshold:
            armed = True
            negative_index = index
            negative_value = value
        elif value >= threshold and armed:
            fraction = -negative_value / (value - negative_value)
            positive_crossings.append(negative_index + fraction * (index - negative_index))
            armed = False
    if len(positive_crossings) < 5:
        return None
    period_samples = (
        (positive_crossings[-1] - positive_crossings[0]) / (len(positive_crossings) - 1)
    )
    if period_samples <= 0:
        return None
    return round(sample_rate / period_samples)


def _synthetic_source_for_frequency(frequency: int | None) -> str | None:
    if frequency is None:
        return None
    source_name, expected = min(
        SYNTHETIC_SOURCE_TONES.items(),
        key=lambda item: abs(frequency - item[1]),
    )
    if abs(frequency - expected) > TONE_MATCH_TOLERANCE_HZ:
        return None
    return source_name


def _segment_tone_frequency(ffmpeg: str, path: Path) -> int | None:
    try:
        result = subprocess.run(
            [
                ffmpeg, "-hide_banner", "-loglevel", "error", "-i", str(path),
                "-map", "0:a:0", "-vn", "-ac", "1", "-ar", str(TONE_SAMPLE_RATE),
                "-t", "1.5", "-f", "s16le", "pipe:1",
            ],
            capture_output=True,
            timeout=15,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Indexed synthetic audio inspection timed out") from exc
    if result.returncode != 0:
        raise RuntimeError("Indexed synthetic segment audio could not be decoded")
    return _estimate_tone_frequency(result.stdout)


@lru_cache(maxsize=4096)
def _cached_segment_identity(
    ffmpeg: str,
    ffprobe: str,
    path_text: str,
    size: int,
    modified_ns: int,
) -> tuple[bool, str | None]:
    del size, modified_ns
    path = Path(path_text)
    useful_av = _segment_has_useful_av(ffprobe, path)
    if not useful_av:
        return False, None
    frequency = _segment_tone_frequency(ffmpeg, path)
    return True, _synthetic_source_for_frequency(frequency)


def _verified_segments(
    store, channel_uuid: str, ffmpeg: str, ffprobe: str,
) -> dict[str, list]:
    result: dict[str, list] = {}
    for segment in store.segments(channel_uuid):
        stat = segment.path.stat()
        useful_av, source_name = _cached_segment_identity(
            ffmpeg,
            ffprobe,
            str(segment.path),
            stat.st_size,
            stat.st_mtime_ns,
        )
        _require(useful_av, "Indexed failover segment lacks decoded audio/video")
        _require(
            source_name is not None,
            "Indexed failover segment has no recognized synthetic source fingerprint",
        )
        result.setdefault(source_name, []).append(segment)
    return result


class _RecorderTaskRun:
    """Run the real recorder task with bounded engine timeouts and stop control."""

    def __init__(self, channel_uuid: str, *, startup_timeout: float, media_idle_timeout: float):
        self.channel_uuid = str(channel_uuid)
        self.startup_timeout = float(startup_timeout)
        self.media_idle_timeout = float(media_idle_timeout)
        self.stop_event = threading.Event()
        self.results: list[object] = []
        self.result: dict | None = None
        self.failures: list[str] = []
        self.processes: list[object] = []
        self._lock = threading.Lock()
        self.thread: threading.Thread | None = None
        self._patchers = []
        self.expected_generation: str | None = None
        self.expected_control_generation: int | None = None

    def start(self, task_args: tuple | list | None = None) -> None:
        from unittest.mock import patch

        from catchuparr import tasks
        from catchuparr.configuration import load_active_configuration
        from catchuparr.engine import recorder as recorder_engine
        from catchuparr.engine.recorder import FFmpegCopyRecorder
        from catchuparr.recorder_control import load_recorder_control
        from catchuparr.recorder_proxy import configuration_generation

        active = load_active_configuration()
        control = load_recorder_control()
        _require(active is not None, "Synthetic recorder task requires an applied snapshot")
        _require(not control.paused, "Synthetic recorder task requires recording to be resumed")
        if task_args is None:
            task_args = (
                self.channel_uuid,
                configuration_generation(active),
                control.generation,
            )
        _require(
            len(task_args) == 3 and str(task_args[0]) == self.channel_uuid,
            "Synthetic recorder task received unexpected reconciliation arguments",
        )
        self.expected_generation = task_args[1]
        self.expected_control_generation = task_args[2]
        _require(
            isinstance(self.expected_generation, str)
            and self.expected_generation == configuration_generation(active)
            and self.expected_control_generation == control.generation,
            "Synthetic recorder task arguments do not match current applied generations",
        )

        original_run_candidate = FFmpegCopyRecorder.run_candidate
        original_popen = subprocess.Popen

        def tracked_popen(*args, **kwargs):
            process = original_popen(*args, **kwargs)
            with self._lock:
                self.processes.append(process)
            return process

        def bounded_run_candidate(recorder, stop_event, **_native_timeouts):
            try:
                result = original_run_candidate(
                    recorder,
                    stop_event,
                    startup_timeout=self.startup_timeout,
                    media_idle_timeout=self.media_idle_timeout,
                    poll_interval=POLL_INTERVAL,
                )
            except Exception as exc:
                with self._lock:
                    self.failures.append(type(exc).__name__)
                raise
            with self._lock:
                self.results.append(result)
            return result

        task_threading = types.SimpleNamespace(
            Event=lambda: self.stop_event,
            Lock=threading.Lock,
            Thread=threading.Thread,
        )
        process_api = types.SimpleNamespace(
            Popen=tracked_popen,
            DEVNULL=subprocess.DEVNULL,
            TimeoutExpired=subprocess.TimeoutExpired,
        )
        self._patchers = [
            patch.object(tasks, "threading", task_threading),
            patch.object(FFmpegCopyRecorder, "run_candidate", bounded_run_candidate),
            patch.object(recorder_engine, "subprocess", process_api),
        ]
        for patcher in self._patchers:
            patcher.start()

        def invoke() -> None:
            try:
                self.result = tasks.record_channel.run(
                    self.channel_uuid,
                    self.expected_generation,
                    self.expected_control_generation,
                )
            except Exception as exc:
                with self._lock:
                    self.failures.append(f"task:{type(exc).__name__}")

        self.thread = threading.Thread(
            target=invoke,
            name="catchuparr-synthetic-recorder-task",
            daemon=True,
        )
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()

    def join(self, timeout: float = 15.0, *, cooperative_sleep=None) -> None:
        self.stop_event.set()
        self._join_and_validate(timeout, cooperative_sleep=cooperative_sleep)

    def join_after_supervisor(
        self, timeout: float = 20.0, *, cooperative_sleep=None,
    ) -> None:
        """Wait for a pause/config/schedule fence to stop the task naturally."""
        self._join_and_validate(timeout, cooperative_sleep=cooperative_sleep)

    def _join_and_validate(self, timeout: float, *, cooperative_sleep=None) -> None:
        if self.thread is not None:
            sleep = cooperative_sleep or time.sleep
            deadline = time.monotonic() + timeout
            while self.thread.is_alive():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self.thread.join(0)
                if self.thread.is_alive():
                    sleep(min(0.05, remaining))
            _require(not self.thread.is_alive(), "Synthetic recorder task did not stop in time")
        for patcher in reversed(self._patchers):
            patcher.stop()
        self._patchers = []
        with self._lock:
            _require(not self.failures, "Synthetic recorder task raised an internal error")
            _require(self.result == {"status": "stopped"}, "Synthetic recorder task did not finish cleanly")
            _require(
                all(process.poll() is not None for process in self.processes),
                "Synthetic FFmpeg process remained after recorder stop",
            )


def _probe_schedule_and_control_gates(
    channel_uuid: str,
    settings: dict,
    active_path: Path,
    redis_client,
    source_server,
    profiles: dict[str, object],
    profile_count,
) -> None:
    """Verify real task admission rejects closed schedules and paused control."""
    from apps.m3u.connection_pool import profile_connections_key

    from catchuparr.configuration import apply_configuration, load_active_configuration
    from catchuparr.recorder_control import (
        load_recorder_control,
        pause_recorders,
        resume_recorders,
    )
    from catchuparr.recorder_proxy import configuration_generation
    from catchuparr.tasks import record_channel

    def assert_no_acquisition(before_counts, before_active, before_profiles):
        _require(
            source_server.snapshot() == (before_counts, before_active),
            "Recorder admission gate contacted a synthetic provider source",
        )
        _require(
            all(
                profile_count(redis_client, profile.id, profile_connections_key)
                == before_profiles[label]
                for label, profile in profiles.items()),
            "Recorder admission gate changed a native provider profile counter",
        )
        _require(
            not redis_client.exists(f"catchuparr:recorder:{channel_uuid}"),
            "Rejected recorder admission acquired a recorder lease",
        )

    original_yaml = settings.get("filter_config", "")
    _require("rules:\n" in original_yaml, "Synthetic schedule gate requires a YAML rules section")
    control = resume_recorders(active_path)
    _require(not control.paused, "Synthetic control setup did not resume recording")
    before_counts, before_active = source_server.snapshot()
    before_profiles = {
        label: profile_count(redis_client, profile.id, profile_connections_key)
        for label, profile in profiles.items()
    }

    off_yaml = original_yaml.replace("rules:\n", "schedule: {}\nrules:\n", 1)
    apply_configuration(dict(settings, filter_config=off_yaml), active_path=active_path)
    off_active = load_active_configuration(active_path)
    _require(off_active is not None, "Applied closed schedule was not readable")
    off_control = load_recorder_control(active_path)
    off_result = record_channel.run(
        channel_uuid,
        configuration_generation(off_active),
        off_control.generation,
    )
    _require(
        off_result == {"status": "outside_schedule"},
        "Real recorder task did not reject an empty applied schedule",
    )
    assert_no_acquisition(before_counts, before_active, before_profiles)

    apply_configuration(settings, active_path=active_path)
    active = load_active_configuration(active_path)
    _require(active is not None, "Restored continuous schedule was not readable")
    paused = pause_recorders(active_path)
    paused_result = record_channel.run(
        channel_uuid,
        configuration_generation(active),
        paused.generation,
    )
    _require(
        paused_result == {"status": "recording_paused"},
        "Real recorder task did not reject paused control state",
    )
    assert_no_acquisition(before_counts, before_active, before_profiles)

    resumed = resume_recorders(active_path)
    _require(
        not resumed.paused and resumed.generation > paused.generation,
        "Resume did not publish a newer recording-control generation",
    )


def _wait_for_source_segments(
    run: _RecorderTaskRun,
    store,
    channel_uuid: str,
    ffmpeg: str,
    ffprobe: str,
    source_name: str,
    minimum: int,
    timeout: float,
    timeout_diagnostic: Callable[[dict[str, list]], dict[str, object]] | None = None,
) -> list:
    deadline = time.monotonic() + timeout
    last_verified: dict[str, list] = {}
    while time.monotonic() < deadline:
        _require(run.thread is not None and run.thread.is_alive(), "Recorder task ended before media was indexed")
        last_verified = _verified_segments(store, channel_uuid, ffmpeg, ffprobe)
        identified = last_verified.get(source_name, [])
        if len(identified) >= minimum:
            return identified
        time.sleep(0.2)
    if timeout_diagnostic is not None:
        try:
            diagnostic = timeout_diagnostic(last_verified)
            if isinstance(diagnostic, dict):
                _emit_timeout_diagnostic(diagnostic)
        except Exception:
            # Diagnostics are best-effort. Preserve the original timeout and
            # let the caller run its existing recorder cleanup path.
            pass
    raise RuntimeError("Synthetic recorder did not index expected source media in time")


def _worker_records(redis_client, channel_uuid: str) -> list[dict[str, str]]:
    from catchuparr.adapters.recorder_proxy import PLUGIN_REDIS_PREFIX, read_worker_record

    records = []
    pattern = f"{PLUGIN_REDIS_PREFIX}worker:*"
    for raw_key in redis_client.scan_iter(match=pattern):
        key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else str(raw_key)
        worker_id = key.rsplit(":", 1)[-1]
        record = read_worker_record(redis_client, worker_id)
        if record is not None and record.get("channel_uuid") == str(channel_uuid):
            records.append(record)
    return records


def _assert_worker_cleanup(redis_client, channel_uuid: str) -> None:
    from catchuparr.adapters.recorder_proxy import reservation_credential_marker_key
    from catchuparr.recorder_proxy import CAPABILITY_PREFIX

    records = _worker_records(redis_client, channel_uuid)
    _require(bool(records), "Recorder fallback did not create native worker ledger records")
    for record in records:
        _require(
            record.get("state") in {"released", "failed"}
            and record.get("reservation_state") in {"released", "none"},
            "Recorder fallback left a native worker or profile reservation active",
        )
        digest = record.get("capability_digest")
        if digest:
            _require(
                not redis_client.exists(CAPABILITY_PREFIX + digest),
                "Recorder fallback left a private capability active",
            )
        reservation_id = record.get("reservation_id")
        if reservation_id:
            _require(
                not redis_client.exists(reservation_credential_marker_key(reservation_id)),
                "Recorder fallback left a private credential marker",
            )


def _restore_file(path: Path, existed: bool, contents: bytes | None) -> None:
    if not existed:
        path.unlink(missing_ok=True)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.restore")
    try:
        temporary.write_bytes(contents or b"")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def probe_recorder_failover(root: Path) -> None:
    """Run real AIO recorder startup, capacity, and runtime failover checks."""
    _require(
        os.environ.get("CATCHUPARR_INTEGRATION_TEST") == "1",
        "Failover probe requires a disposable integration container",
    )
    from aio_recorder_media import (
        _key_dump,
        _profile_count,
        _set_stream_default_profile,
        _stream_default_profile_id,
    )
    from apps.channels import tasks as channel_tasks
    from apps.channels.models import (
        Channel,
        ChannelProfile,
        ChannelProfileMembership,
        ChannelStream,
        Stream,
    )
    from apps.m3u.connection_pool import (
        profile_connections_key,
        profile_credential_release_key,
        release_profile_slot,
        reserve_profile_slot,
    )
    from apps.m3u.models import M3UAccount
    from apps.plugins.models import PluginConfig
    from core.models import CoreSettings, StreamProfile
    from core.utils import RedisClient

    from catchuparr.adapters.recorder_proxy import (
        core_api_supported,
    )
    from catchuparr.configuration import (
        active_settings_path,
        apply_configuration,
        load_active_configuration,
    )
    from catchuparr.engine.store import ArchiveStore
    from catchuparr.recorder_control import control_deny_path, control_state_path
    from catchuparr.recorder_proxy import configuration_generation, ranked_source_candidates

    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")
    _require(ffmpeg is not None and ffprobe is not None, "AIO failover probe requires ffmpeg and ffprobe")
    _require(core_api_supported(), "Native source proxy API is incompatible with failover probe")

    root = Path(root)
    fixture_root = root / "synthetic-recorder-failover-fixtures"
    archive_root = root / "synthetic-recorder-failover-archive"
    fixture_root.mkdir(parents=True, exist_ok=True)
    archive_root.mkdir(parents=True, exist_ok=True)

    active_path = active_settings_path()
    plugin_config = PluginConfig.objects.get(key="catchuparr")
    original_settings = dict(plugin_config.settings or {})
    recording_enabled_was_set = "recording_enabled" in original_settings
    original_recording_enabled = original_settings.get("recording_enabled")
    active_existed = active_path.exists()
    active_bytes = active_path.read_bytes() if active_existed else None
    active_lock_path = active_path.with_suffix(active_path.suffix + ".lock")
    active_lock_existed = active_lock_path.exists()
    active_lock_bytes = active_lock_path.read_bytes() if active_lock_existed else None
    reset_marker_path = active_path.with_name(".catchuparr-configuration-reset-required")
    reset_marker_existed = reset_marker_path.exists()
    reset_marker_bytes = reset_marker_path.read_bytes() if reset_marker_existed else None
    control_path = control_state_path(active_path)
    control_existed = control_path.exists()
    control_bytes = control_path.read_bytes() if control_existed else None
    control_deny = control_deny_path(active_path)
    control_deny_existed = control_deny.exists()
    control_deny_bytes = control_deny.read_bytes() if control_deny_existed else None

    source_server = None
    bridge = None
    redis_client = None
    default_profile_saved = False
    saved_default_profile = None
    original_base_url = channel_tasks.get_dvr_stream_base_url
    created_accounts = []
    created_streams = []
    created_channels = []
    created_channel_profiles = []
    harnesses: list[_RecorderTaskRun] = []
    held_a_slot = False
    profile_a = None
    profiles: dict[str, object] = {}
    cleanup_errors: list[str] = []

    try:
        payload_a = _null_transport_stream()
        payload_b = _make_paced_transport_stream(
            ffmpeg, ffprobe, fixture_root / "source-b.ts",
            service_name="Synthetic Source B", frequency=880,
        )
        payload_c = _make_paced_transport_stream(
            ffmpeg, ffprobe, fixture_root / "source-c.ts",
            service_name="Synthetic Source C", frequency=660,
        )
        payload_d = _make_paced_transport_stream(
            ffmpeg, ffprobe, fixture_root / "source-d.ts",
            service_name="Synthetic Source D", frequency=440,
        )
        source_server = _SyntheticFailoverSourceServer(
            {
                "source-a.ts": payload_a,
                "source-b.ts": payload_b,
                "source-c.ts": payload_c,
                "source-d.ts": payload_d,
            },
            {
                "source-a.ts": SOURCE_DURATION,
                "source-b.ts": SOURCE_DURATION,
                "source-c.ts": SOURCE_DURATION,
                "source-d.ts": SOURCE_DURATION,
            },
        )
        source_server.thread.start()

        accounts = {}
        for label in ("a", "b", "d"):
            account = M3UAccount.objects.create(
                name=f"Synthetic failover {label.upper()}",
                is_active=True,
                max_streams=1,
            )
            created_accounts.append(account)
            accounts[label] = account
            profile = account.profiles.filter(is_default=True).first()
            _require(profile is not None, "Synthetic failover M3U profile was not created")
            profile.max_streams = 1
            profile.is_active = True
            profile.save(update_fields=("max_streams", "is_active"))
            profiles[label] = profile
        profile_a = profiles["a"]

        streams = {}
        for label in ("a", "b", "d"):
            stream = Stream.objects.create(
                name=f"Synthetic failover stream {label.upper()}",
                url=f"{source_server.base_url}/source-{label}.ts",
                m3u_account=accounts[label],
            )
            created_streams.append(stream)
            streams[label] = stream

        channel_numbers = (91, 92, 93)
        channels = {}
        for label, number in zip(("startup", "capacity", "runtime"), channel_numbers):
            channel = Channel.objects.create(
                name=f"Synthetic failover channel {label}",
                channel_number=number,
                user_level=0,
            )
            created_channels.append(channel)
            channels[label] = channel
            for order, source_label in enumerate(("a", "b", "d")):
                ChannelStream.objects.create(
                    channel=channel,
                    stream=streams[source_label],
                    order=order,
                )

        profile_name = "Synthetic failover channel profile"
        channel_profile = ChannelProfile.objects.create(name=profile_name)
        created_channel_profiles.append(channel_profile)
        ChannelProfileMembership.objects.filter(channel_profile=channel_profile).update(
            enabled=False
        )
        for channel in channels.values():
            ChannelProfileMembership.objects.update_or_create(
                channel_profile=channel_profile,
                channel=channel,
                defaults={"enabled": True},
            )

        redirect_profile = StreamProfile.objects.filter(name__iexact="Redirect").first()
        _require(redirect_profile is not None, "Native Redirect StreamProfile is unavailable")
        saved_default_profile = _stream_default_profile_id(CoreSettings)
        default_profile_saved = True
        _set_stream_default_profile(CoreSettings, redirect_profile.id)
        _require(
            str(_stream_default_profile_id(CoreSettings)) == str(redirect_profile.id),
            "Native default StreamProfile was not set to Redirect",
        )

        settings = {
            "filter_config": (
                "version: 1\n"
                f"profile: {profile_name}\n"
                "rules:\n"
                "  - channels: {profile: all}\n"
                "    exclude: []\n"
                "    priority:\n"
                '      "Synthetic failover A": 300\n'
                '      "Synthetic failover B": 200\n'
                '      "Synthetic failover D": 100\n'
            ),
            "archive_root": str(archive_root),
            "retention_hours": 1,
            "max_storage_gib": 1,
        }
        apply_configuration(settings, active_path=active_path)
        active = load_active_configuration(active_path)
        _require(active is not None, "Applied failover policy could not be read")
        expected_ids = [str(streams[label].id) for label in ("a", "b", "d")]
        for channel in channels.values():
            candidates = ranked_source_candidates(str(channel.uuid), active)
            _require(
                candidates is not None
                and [str(item.get("id")) for item in candidates] == expected_ids,
                "Applied priority policy did not rank A, B, and D in order",
            )
        generation = configuration_generation(active)

        # Add C after Apply. The active snapshot intentionally cannot admit a
        # new account until a later Apply recompiles its known account IDs.
        account_c = M3UAccount.objects.create(
            name="Synthetic failover C",
            is_active=True,
            max_streams=1,
        )
        created_accounts.append(account_c)
        profile_c = account_c.profiles.filter(is_default=True).first()
        _require(profile_c is not None, "Synthetic C profile was not created")
        profile_c.max_streams = 1
        profile_c.is_active = True
        profile_c.save(update_fields=("max_streams", "is_active"))
        profiles["c"] = profile_c
        stream_c = Stream.objects.create(
            name="Synthetic failover stream C",
            url=f"{source_server.base_url}/source-c.ts",
            m3u_account=account_c,
        )
        created_streams.append(stream_c)
        streams["c"] = stream_c
        for channel in channels.values():
            ChannelStream.objects.create(channel=channel, stream=stream_c, order=3)

        active = load_active_configuration(active_path)
        _require(active is not None, "Applied failover policy disappeared")
        _require(
            configuration_generation(active) == generation,
            "Adding post-Apply source C unexpectedly changed the active generation",
        )
        for channel in channels.values():
            candidates = ranked_source_candidates(str(channel.uuid), active)
            _require(
                candidates is not None
                and [str(item.get("id")) for item in candidates] == expected_ids,
                "Post-Apply account C escaped the immutable source policy",
            )

        redis_client = RedisClient.get_client()
        redis_client.ping()
        profile_baselines = {
            label: _profile_count(redis_client, profile.id, profile_connections_key)
            for label, profile in profiles.items()
        }
        _require(
            all(count == 0 for count in profile_baselines.values()),
            "Synthetic provider profile counters were not empty before failover",
        )
        marker_baselines = {
            label: _key_dump(redis_client, profile_credential_release_key(profile.id))
            for label, profile in profiles.items()
        }
        _probe_schedule_and_control_gates(
            str(channels["startup"].uuid),
            settings,
            active_path,
            redis_client,
            source_server,
            profiles,
            _profile_count,
        )
        active = load_active_configuration(active_path)
        _require(active is not None, "Recorder gate probe did not restore the active config")
        generation = configuration_generation(active)
        bridge = _DjangoHTTPBridge()
        bridge.start()
        channel_tasks.get_dvr_stream_base_url = lambda: bridge.base_url

        # A cannot produce usable stream packets; B produces continuous media.
        source_server.set_mode("source-a.ts", "continuous")
        source_server.set_mode("source-b.ts", "continuous")
        source_server.set_mode("source-d.ts", "continuous")
        startup_channel = channels["startup"]
        startup_store = ArchiveStore(archive_root)
        startup_baseline, _ = source_server.snapshot()
        startup_run = _RecorderTaskRun(
            str(startup_channel.uuid),
            startup_timeout=STARTUP_TIMEOUT,
            media_idle_timeout=MEDIA_IDLE_TIMEOUT,
        )
        startup_run.start()
        harnesses.append(startup_run)
        try:
            startup_b = _wait_for_source_segments(
                startup_run,
                startup_store,
                str(startup_channel.uuid),
                ffmpeg,
                ffprobe,
                "Synthetic Source B",
                minimum=1,
                timeout=100,
            )
            hold_until = time.monotonic() + MEDIA_IDLE_TIMEOUT + 3
            while time.monotonic() < hold_until:
                _require(startup_run.thread is not None and startup_run.thread.is_alive(), "B recorder exited before stop")
                counts, active_counts = source_server.snapshot()
                _require(counts["source-d.ts"] == startup_baseline["source-d.ts"], "Lower-ranked D opened before stop")
                _require(counts["source-c.ts"] == startup_baseline["source-c.ts"], "Post-Apply C source opened")
                _require(active_counts["source-b.ts"] > 0, "Continuous B connection ended before stop")
                time.sleep(0.25)
            _require(
                len(startup_run.results) == 1
                and startup_run.results[0].status == "no_media"
                and startup_run.results[0].useful_segments == 0,
                "High-ranked A did not fail for lack of useful media before B fallback",
            )
            startup_sources = _verified_segments(
                startup_store, str(startup_channel.uuid), ffmpeg, ffprobe
            )
            startup_b = startup_sources.get("Synthetic Source B", [])
            _require(len(startup_b) >= 2, "B did not retain useful indexed media after A failed")
            _require(
                not startup_sources.get("Synthetic Source C"),
                "Startup fallback archived media from excluded source C",
            )
            counts, _ = source_server.snapshot()
            _require(counts["source-a.ts"] > startup_baseline["source-a.ts"], "High-ranked A was not attempted")
            _require(counts["source-b.ts"] > startup_baseline["source-b.ts"], "Permitted B was not attempted")
            startup_run.stop()
            startup_run.join()
        except Exception:
            _cleanup_failed_recorder_run(startup_run)
            raise
        _require(
            startup_run.results[-1].status == "stopped"
            and startup_run.results[-1].useful_segments > 0,
            "Successful B fallback did not remain active until stop",
        )
        _require(
            all(_profile_count(redis_client, profile.id, profile_connections_key) == profile_baselines[label]
                for label, profile in profiles.items()),
            "Startup fallback did not release all native profile reservations",
        )
        _assert_worker_cleanup(redis_client, startup_channel.uuid)

        # Reserve A through the real native pool first. Its slot must survive
        # the recorder's denied A attempt while the same task falls through to B.
        capacity_channel = channels["capacity"]
        capacity_store = ArchiveStore(archive_root)
        capacity_baseline, _ = source_server.snapshot()
        reserved, _count, _reason = reserve_profile_slot(profile_a, redis_client)
        _require(reserved, "Native A profile slot could not be reserved for the capacity probe")
        held_a_slot = True
        held_count = _profile_count(redis_client, profile_a.id, profile_connections_key)
        _require(held_count == profile_baselines["a"] + 1, "Native A capacity holder was not counted")
        capacity_run = _RecorderTaskRun(
            str(capacity_channel.uuid),
            startup_timeout=STARTUP_TIMEOUT,
            media_idle_timeout=MEDIA_IDLE_TIMEOUT,
        )
        capacity_run.start()
        harnesses.append(capacity_run)
        try:
            _wait_for_source_segments(
                capacity_run,
                capacity_store,
                str(capacity_channel.uuid),
                ffmpeg,
                ffprobe,
                "Synthetic Source B",
                minimum=1,
                timeout=100,
            )
            counts, active_counts = source_server.snapshot()
            _require(counts["source-a.ts"] == capacity_baseline["source-a.ts"], "Capacity-denied A opened upstream")
            _require(counts["source-c.ts"] == capacity_baseline["source-c.ts"], "Capacity probe opened excluded C")
            _require(counts["source-d.ts"] == capacity_baseline["source-d.ts"], "Capacity probe skipped successful B")
            capacity_sources = _verified_segments(
                capacity_store, str(capacity_channel.uuid), ffmpeg, ffprobe
            )
            _require(
                not capacity_sources.get("Synthetic Source C"),
                "Capacity fallback archived media from excluded source C",
            )
            _require(active_counts["source-b.ts"] > 0, "Capacity fallback did not keep B active")
            _require(
                len(capacity_run.results) == 1
                and capacity_run.results[0].status == "no_media",
                "Capacity-denied A did not fail before source media opened",
            )
            _require(
                _profile_count(redis_client, profile_a.id, profile_connections_key) == held_count,
                "A recorder cleanup consumed the native capacity holder's profile slot",
            )
            capacity_run.stop()
            capacity_run.join()
        except Exception:
            _cleanup_failed_recorder_run(capacity_run)
            raise
        _require(
            capacity_run.results[-1].status == "stopped"
            and capacity_run.results[-1].useful_segments > 0,
            "Capacity fallback did not record B until stop",
        )
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key) == held_count,
            "Recorder teardown changed the still-owned native A profile slot",
        )
        _require(
            _profile_count(redis_client, profiles["b"].id, profile_connections_key)
            == profile_baselines["b"],
            "Capacity fallback did not release B's recorder reservation",
        )
        _assert_worker_cleanup(redis_client, capacity_channel.uuid)
        release_profile_slot(profile_a.id, redis_client)
        held_a_slot = False
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines["a"],
            "Releasing the native A capacity holder did not restore its baseline",
        )

        # B emits several full native chunks, then returns no media. The real
        # recorder must preserve its B segments and mark the later D fallback.
        runtime_channel = channels["runtime"]
        runtime_store = ArchiveStore(archive_root)
        runtime_baseline, _ = source_server.snapshot()
        runtime_stall_baseline = source_server.stall_snapshot()["source-b.ts"]
        source_server.set_mode(
            "source-b.ts", "stall", repeats=STALL_SOURCE_REPEATS
        )
        source_server.set_mode("source-d.ts", "continuous")
        runtime_run = _RecorderTaskRun(
            str(runtime_channel.uuid),
            startup_timeout=STARTUP_TIMEOUT,
            media_idle_timeout=MEDIA_IDLE_TIMEOUT,
        )
        runtime_run.start()
        harnesses.append(runtime_run)

        def runtime_timeout_diagnostic(verified_segments):
            return _runtime_timeout_diagnostic(
                runtime_run,
                source_server,
                runtime_baseline,
                runtime_stall_baseline,
                verified_segments,
                redis_client,
                str(runtime_channel.uuid),
                {label: str(streams[label].id) for label in ("a", "b", "c", "d")},
            )

        try:
            _wait_for_source_segments(
                runtime_run,
                runtime_store,
                str(runtime_channel.uuid),
                ffmpeg,
                ffprobe,
                "Synthetic Source D",
                minimum=1,
                timeout=150,
                timeout_diagnostic=runtime_timeout_diagnostic,
            )
            runtime_run.stop()
            runtime_run.join()
        except Exception:
            _cleanup_failed_recorder_run(runtime_run)
            raise
        identified_runtime = _verified_segments(
            runtime_store, str(runtime_channel.uuid), ffmpeg, ffprobe
        )
        b_segments = identified_runtime.get("Synthetic Source B", [])
        d_segments = identified_runtime.get("Synthetic Source D", [])
        _require(
            not identified_runtime.get("Synthetic Source C"),
            "Runtime fallback archived media from excluded source C",
        )
        _require(len(b_segments) >= 2, "Stalled B source did not create two useful archive segments")
        _require(bool(d_segments), "Runtime fallback did not preserve indexed D media")
        runtime_counts, runtime_active_counts = source_server.snapshot()
        runtime_stall = source_server.stall_snapshot()["source-b.ts"]
        runtime_results = runtime_run.results
        runtime_diagnostics = {
            "attempts": _recorder_attempt_summary(runtime_results),
            "source_request_delta": {
                label: runtime_counts[f"source-{label}.ts"]
                - runtime_baseline[f"source-{label}.ts"]
                for label in ("a", "b", "c", "d")
            },
            "source_active": {
                label: runtime_active_counts[f"source-{label}.ts"]
                for label in ("a", "b", "c", "d")
            },
            "indexed_segments": {"b": len(b_segments), "d": len(d_segments)},
            "source_b_stall": {
                "started_delta": runtime_stall["started"] - runtime_stall_baseline["started"],
                "duration_ms": round(
                    (runtime_stall["duration_seconds"]
                     - runtime_stall_baseline["duration_seconds"]) * 1000
                ),
                "null_bytes": runtime_stall["null_bytes"]
                - runtime_stall_baseline["null_bytes"],
            },
        }
        runtime_progressed = (
            len(runtime_results) >= 3
            and runtime_results[0].status == "no_media"
            and runtime_results[1].status == "media_stalled"
            and runtime_results[1].useful_segments >= 2
            and runtime_results[-1].status == "stopped"
            and runtime_results[-1].useful_segments > 0
        )
        _require(
            runtime_stall["started"] == runtime_stall_baseline["started"] + 1
            and runtime_stall["null_bytes"] > runtime_stall_baseline["null_bytes"]
            and runtime_stall["duration_seconds"]
            - runtime_stall_baseline["duration_seconds"]
            >= MEDIA_IDLE_TIMEOUT * 0.8,
            "Runtime B did not stay on one null-only stall connection through the idle deadline; "
            f"diagnostics={json.dumps(runtime_diagnostics, sort_keys=True)}",
        )
        _require(
            runtime_counts["source-b.ts"] == runtime_baseline["source-b.ts"] + 1,
            "Runtime B stall reopened its provider connection before fallback; "
            f"diagnostics={json.dumps(runtime_diagnostics, sort_keys=True)}",
        )
        _require(
            runtime_progressed,
            "Runtime fallback did not progress through A failure, B stall, and D success; "
            f"diagnostics={json.dumps(runtime_diagnostics, sort_keys=True)}",
        )
        first_d = min(d_segments, key=lambda item: item.start_utc)
        last_b = max(b_segments, key=lambda item: item.end_utc)
        _require(first_d.discontinuity, "D fallback archive segment lacks discontinuity metadata")
        _require(
            first_d.start_utc.timestamp() > last_b.end_utc.timestamp() + 0.25,
            "Runtime source stall was not preserved as an archive time gap",
        )
        coverage = runtime_store.coverage(
            str(runtime_channel.uuid),
            min(item.start_utc for item in b_segments),
            max(item.end_utc for item in d_segments),
        )
        _require(bool(coverage.gaps), "Runtime source stall was not reported as a coverage gap")
        counts, active_counts = runtime_counts, runtime_active_counts
        _require(counts["source-a.ts"] > runtime_baseline["source-a.ts"], "Runtime A candidate was not attempted")
        _require(counts["source-b.ts"] > runtime_baseline["source-b.ts"], "Runtime B candidate was not attempted")
        _require(counts["source-d.ts"] > runtime_baseline["source-d.ts"], "Runtime D fallback was not attempted")
        _require(counts["source-c.ts"] == runtime_baseline["source-c.ts"], "Runtime fallback opened excluded C")
        _require(not any(active_counts.values()), "Runtime failover left a source HTTP connection open")
        _require(
            all(_profile_count(redis_client, profile.id, profile_connections_key) == profile_baselines[label]
                for label, profile in profiles.items()),
            "Runtime failover did not release native profile reservations",
        )
        _assert_worker_cleanup(redis_client, runtime_channel.uuid)

        bridge_active = (
            _wait_for_bridge_idle(bridge, timeout=BRIDGE_DRAIN_TIMEOUT)
            if bridge is not None else 0
        )
        _require(bridge_active == 0, "Django bridge still has a private recorder response open")
        _require(
            all(
                _key_dump(redis_client, profile_credential_release_key(profile.id))
                == marker_baselines[label]
                for label, profile in profiles.items()
            ),
            "Failover cleanup changed a native profile credential marker",
        )
        print("AIO recorder startup, capacity, runtime failover, media gaps, and cleanup passed")
    finally:
        for run in harnesses:
            if run.thread is not None and run.thread.is_alive():
                run.stop()
                run.thread.join(20)
                if run.thread.is_alive():
                    cleanup_errors.append("recorder task thread")
            for patcher in reversed(run._patchers):
                patcher.stop()
            run._patchers = []
        channel_tasks.get_dvr_stream_base_url = original_base_url
        if held_a_slot and redis_client is not None and profile_a is not None:
            try:
                release_profile_slot(profile_a.id, redis_client)
            except Exception:
                cleanup_errors.append("held profile capacity")
        if bridge is not None:
            try:
                bridge.close()
                if bridge.server.active_requests():
                    cleanup_errors.append("Django bridge requests")
            except Exception:
                cleanup_errors.append("Django bridge")
        if source_server is not None:
            try:
                source_server.close()
                if any(source_server.snapshot()[1].values()):
                    cleanup_errors.append("synthetic source requests")
            except Exception:
                cleanup_errors.append("synthetic source server")
        if default_profile_saved:
            try:
                _set_stream_default_profile(CoreSettings, saved_default_profile)
            except Exception:
                cleanup_errors.append("default stream profile")
        try:
            _restore_file(active_path, active_existed, active_bytes)
            _restore_file(active_lock_path, active_lock_existed, active_lock_bytes)
            _restore_file(reset_marker_path, reset_marker_existed, reset_marker_bytes)
            _restore_file(control_path, control_existed, control_bytes)
            _restore_file(control_deny, control_deny_existed, control_deny_bytes)
        except Exception:
            cleanup_errors.append("active configuration snapshot")
        try:
            current_settings = dict(plugin_config.settings or {})
            if recording_enabled_was_set:
                current_settings["recording_enabled"] = original_recording_enabled
            else:
                current_settings.pop("recording_enabled", None)
            plugin_config.settings = current_settings
            plugin_config.save(update_fields=("settings",))
        except Exception:
            cleanup_errors.append("recording control setting")
        for channel_profile in reversed(created_channel_profiles):
            try:
                channel_profile.delete()
            except Exception:
                cleanup_errors.append("synthetic channel profile records")
        for channel in reversed(created_channels):
            try:
                channel.delete()
            except Exception:
                cleanup_errors.append("synthetic channel records")
        for stream in reversed(created_streams):
            try:
                stream.delete()
            except Exception:
                cleanup_errors.append("synthetic stream records")
        for account in reversed(created_accounts):
            try:
                account.delete()
            except Exception:
                cleanup_errors.append("synthetic account records")
        try:
            shutil.rmtree(archive_root, ignore_errors=True)
            shutil.rmtree(fixture_root, ignore_errors=True)
        except Exception:
            cleanup_errors.append("synthetic archive files")
        if cleanup_errors:
            raise RuntimeError(
                "Failover probe cleanup did not complete: " + ", ".join(sorted(set(cleanup_errors)))
            )
