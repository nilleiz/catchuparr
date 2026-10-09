"""Synthetic native-route media acceptance probe for disposable AIO containers."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


_NATIVE_LIVE_A_INVARIANT_NAMES = (
    "owner_read_ok",
    "owner_present",
    "owner_matches",
    "stream_manager_lookup_ok",
    "stream_manager_present",
    "stream_manager_identity",
    "stream_manager_running",
    "client_manager_lookup_ok",
    "client_manager_present",
    "client_manager_identity",
    "local_client_count_read_ok",
    "local_client_count_one",
    "global_client_count_read_ok",
    "global_client_count_one",
    "metadata_state_read_ok",
    "metadata_active",
)


def _native_live_a_invariant_flags(**observations) -> dict[str, bool]:
    """Return only named boolean checks for the synthetic live-A worker."""
    return {
        name: observations.get(name) is True
        for name in _NATIVE_LIVE_A_INVARIANT_NAMES
    }


def _native_live_a_invariant_message(stage: str, flags: dict[str, bool]) -> str:
    """Format sanitized per-conjunct state without emitting Redis/native values."""
    return (
        f"Native live A invariant failed at {stage}; "
        f"flags={json.dumps(flags, sort_keys=True)}"
    )


class _SyntheticSourceServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False
    allow_reuse_address = True

    def __init__(self, payloads: dict[str, bytes]):
        self.payloads = payloads
        self._counts_lock = threading.Lock()
        self.request_counts = {name: 0 for name in payloads}
        self.active_counts = {name: 0 for name in payloads}
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def _serve(self, *, body: bool) -> None:
                name = self.path.split("?", 1)[0].lstrip("/")
                payload = owner.payloads.get(name)
                if payload is None:
                    self.send_error(404)
                    return
                with owner._counts_lock:
                    owner.request_counts[name] += 1
                    owner.active_counts[name] += 1
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "video/mp2t")
                    self.send_header("Cache-Control", "no-store")
                    self.end_headers()
                    if not body:
                        return
                    while True:
                        for offset in range(0, len(payload), 188 * 32):
                            self.wfile.write(payload[offset:offset + 188 * 32])
                            self.wfile.flush()
                            time.sleep(0.02)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                finally:
                    with owner._counts_lock:
                        owner.active_counts[name] -= 1

            def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                self._serve(body=True)

            def do_HEAD(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                self._serve(body=False)

            def log_message(self, _format: str, *_args) -> None:
                return

        super().__init__(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(
            target=self.serve_forever,
            name="catchuparr-synthetic-source",
            daemon=True,
        )

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.server_port}"

    def snapshot(self) -> tuple[dict[str, int], dict[str, int]]:
        with self._counts_lock:
            return dict(self.request_counts), dict(self.active_counts)

    def close(self) -> None:
        if self.thread.is_alive():
            self.shutdown()
        self.server_close()
        self.thread.join(timeout=5)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and any(self.snapshot()[1].values()):
            time.sleep(0.05)


def _make_transport_stream(
    ffmpeg: str,
    ffprobe: str,
    output: Path,
    *,
    color: str,
    frequency: int,
    service_name: str,
) -> bytes:
    command = [
        ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i", f"color=c={color}:s=320x240:r=25:d=8",
        "-f", "lavfi", "-i", f"sine=frequency={frequency}:sample_rate=48000:duration=8",
        "-shortest", "-c:v", "mpeg2video", "-pix_fmt", "yuv420p", "-g", "12",
        "-b:v", "250k", "-c:a", "mp2", "-b:a", "96k",
        "-metadata", f"service_name={service_name}",
        "-metadata", "service_provider=Synthetic Catchuparr CI",
        "-f", "mpegts", str(output),
    ]
    try:
        subprocess.run(
            command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=30,
        )
        payload = output.read_bytes()
        _require(len(payload) >= 188 * 100, "Synthetic TS fixture is too small")
        _require(payload[0] == 0x47, "Synthetic TS fixture is not packet aligned")
        probe = subprocess.run(
            [ffprobe, "-v", "error", "-count_frames", "-show_programs", "-show_streams", "-of", "json", str(output)],
            check=True, capture_output=True, timeout=15,
        )
        details = json.loads(probe.stdout.decode("utf-8"))
        programs = details.get("programs", [])
        _require(
            any(program.get("tags", {}).get("service_name") == service_name for program in programs),
            "Synthetic TS fixture lost its source identifier",
        )
        frame_counts = {
            item.get("codec_type"): int(item.get("nb_read_frames", "0") or 0)
            for item in details.get("streams", [])
        }
        _require(
            frame_counts.get("video", 0) > 0 and frame_counts.get("audio", 0) > 0,
            "Synthetic TS fixture must decode useful audio and video frames",
        )
        return payload
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Synthetic TS fixture generation exceeded its timeout") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError("Synthetic TS fixture generation failed") from exc


def _stream_default_profile_id(CoreSettings):
    for name in ("get_default_stream_profile_id", "get_default_stream_profile"):
        getter = getattr(CoreSettings, name, None)
        if callable(getter):
            value = getter()
            return getattr(value, "id", value)
    getter = getattr(CoreSettings, "_get_group", None)
    if callable(getter):
        try:
            group = getter("stream_settings")
        except TypeError:
            group = getter("stream_settings", {})
        if isinstance(group, dict):
            return group.get("default_stream_profile")
    raise RuntimeError("Native default StreamProfile setting cannot be inspected")


def _set_stream_default_profile(CoreSettings, profile_id) -> None:
    updater = getattr(CoreSettings, "_update_group", None)
    if not callable(updater):
        raise RuntimeError("Native StreamProfile settings API is unavailable")
    updater("stream_settings", "Stream Settings", {"default_stream_profile": profile_id})


def _key_dump(redis_client, key):
    try:
        return redis_client.dump(key)
    except AttributeError:
        kind = redis_client.type(key)
        if isinstance(kind, bytes):
            kind = kind.decode("ascii")
        if kind in (None, "none"):
            return None
        if kind == "string":
            return (kind, redis_client.get(key))
        if kind == "hash":
            return (kind, tuple(sorted(redis_client.hgetall(key).items())))
        raise RuntimeError("Unexpected Redis type on synthetic native key")


def _profile_count(redis_client, profile_id, key_builder) -> int:
    value = redis_client.get(key_builder(profile_id))
    return int(value or 0)


def _redis_text(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _constant_text(value):
    return _redis_text(getattr(value, "value", value))


class _NativeBufferYieldTracker:
    """Associate parent yielded bytes with the native Redis buffer indexes."""

    def __init__(self, buffer):
        self.buffer = buffer
        self._read_native = buffer.get_optimized_client_data
        self._pending = deque()
        self._lock = threading.Lock()
        self.last_yielded_index = None
        self.unmapped_batches = 0
        self._had_instance_method = "get_optimized_client_data" in vars(buffer)
        self._previous_instance_method = vars(buffer).get("get_optimized_client_data")
        buffer.get_optimized_client_data = self._read

    def _read(self, client_index):
        chunks, next_index = self._read_native(client_index)
        client_index = int(client_index)
        next_index = int(next_index)
        with self._lock:
            if len(chunks) != max(0, next_index - client_index):
                self.unmapped_batches += 1
                self._pending.extend((chunk, None) for chunk in chunks)
                return chunks, next_index
            self._pending.extend(
                (chunk, client_index + offset)
                for offset, chunk in enumerate(chunks, start=1)
            )
        return chunks, next_index

    def observe(self, iterator):
        for chunk in iterator:
            with self._lock:
                self.last_yielded_index = None
                matched_index = None
                for pending_index, (pending_chunk, native_index) in enumerate(self._pending):
                    if pending_chunk is chunk:
                        matched_index = pending_index
                        self.last_yielded_index = native_index
                        break
                if matched_index is not None:
                    for _ in range(matched_index + 1):
                        self._pending.popleft()
            yield chunk

    def close(self):
        if self._had_instance_method:
            self.buffer.get_optimized_client_data = self._previous_instance_method
        else:
            del self.buffer.get_optimized_client_data
        with self._lock:
            self._pending.clear()


class _NativeLiveMediaReader:
    """Continuously read one live client and retain indexed chunks for checks."""

    def __init__(self, iterator, tracker: _NativeBufferYieldTracker):
        self._iterator = iterator
        self._tracker = tracker
        self._condition = threading.Condition()
        self._chunks = deque()
        self._size = 0
        self._finished = False
        self._failure = None
        self._stop_requested = threading.Event()
        self.thread = threading.Thread(
            target=self._run,
            name="catchuparr-live-media-reader",
            daemon=True,
        )
        self.thread.start()

    def _run(self) -> None:
        try:
            while not self._stop_requested.is_set():
                try:
                    raw_chunk = next(self._iterator)
                except StopIteration:
                    break
                index = self._tracker.last_yielded_index
                if index is None:
                    continue
                chunk = bytes(raw_chunk)
                with self._condition:
                    self._chunks.append((index, chunk))
                    self._size += len(chunk)
                    while self._size > 2 * 1024 * 1024 and self._chunks:
                        _old_index, old_chunk = self._chunks.popleft()
                        self._size -= len(old_chunk)
                    self._condition.notify_all()
        except Exception as exc:  # Avoid leaking HTTP or URL details to CI output.
            with self._condition:
                self._failure = type(exc).__name__
                self._condition.notify_all()
        finally:
            with self._condition:
                self._finished = True
                self._condition.notify_all()

    def read_after(self, floor: int, *, minimum_bytes: int, timeout: float) -> tuple[bytes, int]:
        deadline = time.monotonic() + timeout
        with self._condition:
            while True:
                selected = [
                    (index, chunk) for index, chunk in self._chunks if index > floor
                ]
                total = sum(len(chunk) for _index, chunk in selected)
                if total >= minimum_bytes:
                    return b"".join(chunk for _index, chunk in selected), selected[-1][0]
                if self._failure is not None:
                    raise RuntimeError("Native live reader failed while archive recording ran")
                if self._finished:
                    raise RuntimeError("Native live reader ended before fresh media arrived")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError(
                        "Native live route did not publish fresh media before timeout"
                    )
                self._condition.wait(min(remaining, 0.25))

    def close(self, response, *, force_release=None, join_timeout: float = 5) -> None:
        self._stop_requested.set()
        self.thread.join(timeout=join_timeout)
        force_release_error = None
        if self.thread.is_alive() and force_release is not None:
            try:
                force_release()
            except Exception as exc:
                force_release_error = type(exc).__name__
            self.thread.join(timeout=join_timeout)
        _require(not self.thread.is_alive(), "Native live reader did not stop at a chunk boundary")
        response.close()
        if force_release_error is not None:
            raise RuntimeError("Native live reader fallback stop failed")


class _NativeLiveReaderSession:
    """Own exactly one indexed reader from response startup through cleanup."""

    def __init__(self, response, buffer):
        self.response = response
        self.tracker = _NativeBufferYieldTracker(buffer)
        iterator = iter(self.tracker.observe(iter(response.streaming_content)))
        try:
            self.reader = _NativeLiveMediaReader(iterator, self.tracker)
        except Exception:
            self.tracker.close()
            raise
        self._closed = False

    def close(self, *, force_release=None) -> None:
        if self._closed:
            return
        self.reader.close(self.response, force_release=force_release)
        self.tracker.close()
        self._closed = True


def _create_native_live_reader_session(response, native_server, worker_id):
    """Bind the reader to the buffer installed by successful native startup."""
    buffer = native_server.get_buffer(worker_id, profile=None)
    _require(buffer is not None, "Native live route did not install its stream buffer")
    return _NativeLiveReaderSession(response, buffer), buffer


def _assert_tone_matches(actual: int | None, expected: int, label: str) -> None:
    _require(actual is not None, f"{label} media has no decodable synthetic audio tone")
    _require(
        abs(actual - expected) <= 20,
        f"{label} media decoded to an unexpected synthetic source tone",
    )


def _verify_audio_video_tone(
    ffmpeg: str, ffprobe: str, media: bytes, expected: int, label: str,
) -> int:
    _require(
        len(media) >= 188 * 100
        and all(media[offset] == 0x47 for offset in range(0, len(media) - 187, 188)),
        f"{label} route returned no useful packet-aligned transport stream",
    )
    try:
        probe = subprocess.run(
            [
                ffprobe, "-v", "error", "-count_frames", "-f", "mpegts",
                "-show_streams", "-of", "json", "pipe:0",
            ],
            input=media,
            check=True,
            capture_output=True,
            timeout=15,
        )
        decode = subprocess.run(
            [
                ffmpeg, "-hide_banner", "-loglevel", "error", "-f", "mpegts",
                "-i", "pipe:0", "-map", "0:a:0", "-vn", "-ac", "1", "-ar", "48000",
                "-t", "1.5", "-f", "s16le", "pipe:1",
            ],
            input=media,
            check=True,
            capture_output=True,
            timeout=15,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"{label} media decoding exceeded its bounded timeout") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"{label} media was not decodable audio/video") from exc
    details = json.loads(probe.stdout.decode("utf-8"))
    frame_counts = {
        item.get("codec_type"): int(item.get("nb_read_frames", "0") or 0)
        for item in details.get("streams", [])
    }
    _require(
        frame_counts.get("video", 0) > 0 and frame_counts.get("audio", 0) > 0,
        f"{label} route returned no decoded audio/video frames",
    )
    from aio_recorder_failover import _estimate_tone_frequency

    frequency = _estimate_tone_frequency(decode.stdout)
    _assert_tone_matches(frequency, expected, label)
    return frequency


def _wait_for_native_active(
    redis_client,
    native_server,
    worker_id,
    *,
    redis_keys,
    metadata_field,
    channel_state,
    timeout: float = 45,
):
    """Verify strict native readiness after one real response client registers."""
    from time import monotonic

    owner_key = redis_keys.channel_owner(worker_id)
    metadata_key = redis_keys.channel_metadata(worker_id)
    clients_key = redis_keys.clients(worker_id)
    expected_owner = _redis_text(native_server.worker_id)
    active_state = _constant_text(channel_state.ACTIVE)
    deadline = monotonic() + timeout

    while True:
        owner = _redis_text(redis_client.get(owner_key))
        manager = native_server.stream_managers.get(worker_id)
        client_manager = native_server.client_managers.get(worker_id)
        _require(owner == expected_owner, "Native recorder parent lost its Redis owner")
        _require(manager is not None, "Native recorder parent stream manager is absent")
        _require(client_manager is not None, "Native recorder parent client manager is absent")
        local_clients = int(client_manager.get_client_count())
        global_clients = int(redis_client.scard(clients_key) or 0)
        _require(
            local_clients == 1 and global_clients == 1,
            "Native recorder parent client detached before ACTIVE readiness",
        )
        state = _redis_text(redis_client.hget(metadata_key, metadata_field.STATE))
        manager_running = bool(getattr(manager, "running", False))
        if state == active_state and manager_running:
            return expected_owner, manager, client_manager

        remaining = deadline - monotonic()
        if remaining <= 0:
            raise RuntimeError("Native recorder did not reach ACTIVE before its bounded timeout")
        # The public route's actual response iterator is already being consumed;
        # keep polling only the native readiness fields while it waits for data.
        time.sleep(min(0.1, remaining))


def _read_stream_iterator(
    iterator, *, minimum_bytes: int, timeout: float, close=None, accept_chunk=None,
) -> bytes:
    chunks = []
    total = 0
    failures = []

    def consume() -> None:
        nonlocal total
        try:
            while total < minimum_bytes:
                chunk = next(iterator)
                if accept_chunk is not None and not accept_chunk():
                    continue
                chunks.append(bytes(chunk))
                total += len(chunk)
        except StopIteration:
            return
        except Exception as exc:  # Keep exception/URL data out of CI logs.
            failures.append(type(exc).__name__)

    reader = threading.Thread(target=consume, name="catchuparr-media-reader", daemon=True)
    reader.start()
    reader.join(timeout)
    if reader.is_alive():
        if close is not None:
            try:
                close()
            except Exception:
                pass
        reader.join(3)
        raise RuntimeError("Synthetic recorder route did not deliver media before timeout")
    if failures:
        raise RuntimeError("Synthetic recorder media reader failed")
    media = b"".join(chunks)
    _require(len(media) >= minimum_bytes, "Synthetic recorder route returned too little media")
    return media


def _read_stream_response(response, *, minimum_bytes: int, timeout: float) -> bytes:
    return _read_stream_iterator(
        iter(response.streaming_content), minimum_bytes=minimum_bytes, timeout=timeout,
        close=response.close,
    )


def _verify_media_identity(ffprobe: str, media: bytes) -> None:
    _require(media[:1] == b"\x47", "Recorder response is not MPEG-TS")
    _require(
        len(media) >= 188 * 100
        and all(media[offset] == 0x47 for offset in range(0, len(media) - 187, 188)),
        "Recorder response contains no useful packet-aligned TS media",
    )
    try:
        result = subprocess.run(
            [ffprobe, "-v", "error", "-count_frames", "-f", "mpegts", "-show_programs", "-show_streams", "-of", "json", "pipe:0"],
            input=media, check=True, capture_output=True, timeout=15,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Synthetic recorder media inspection exceeded its timeout") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError("Synthetic recorder response was not decodable media") from exc
    details = json.loads(result.stdout.decode("utf-8"))
    programs = details.get("programs", [])
    _require(
        any(program.get("tags", {}).get("service_name") == "Synthetic Source B" for program in programs),
        "Recorder route returned media from a source other than the included B source",
    )
    stream_types = {item.get("codec_type") for item in details.get("streams", [])}
    _require(stream_types >= {"video", "audio"}, "Recorder route did not expose audio and video streams")
    frame_counts = {
        item.get("codec_type"): int(item.get("nb_read_frames", "0") or 0)
        for item in details.get("streams", [])
    }
    _require(
        frame_counts.get("video", 0) > 0 and frame_counts.get("audio", 0) > 0,
        "Recorder route returned no decoded audio/video frames",
    )

def _run_fresh_process_follower(
    attempt,
    *,
    channel_uuid: str,
    stream_id: str,
    account_id: str,
    profile_id: int,
    expected_profile_count: int,
    ffprobe: str,
    native_owner: str,
    parent_client_count: int,
) -> None:
    """Attach from a fresh Django process using the same private capability."""
    bootstrap = (
        "import os,runpy,sys; from pathlib import Path; "
        "sys.path.insert(0, '/data/plugins'); "
        "os.environ['DJANGO_SECRET_KEY']=Path('/data/jwt').read_text().strip(); "
        "sys.argv=['manage.py','shell']; "
        "runpy.run_path('/app/manage.py',run_name='__main__')"
    )
    route_path = f"/catchuparr/recorder/{channel_uuid}"
    child_script = f"""
import os
import sys
sys.path.insert(0, "/data/plugins")
sys.path.insert(0, "/tmp")
from django.test import Client
from apps.m3u.connection_pool import profile_connections_key
from apps.proxy.live_proxy.constants import ChannelMetadataField, ChannelState
from apps.proxy.live_proxy.redis_keys import RedisKeys
from apps.proxy.live_proxy.server import ProxyServer
from core.utils import RedisClient
from catchuparr import runtime
from catchuparr.adapters.recorder_proxy import read_worker_record
from catchuparr.configuration import load_active_configuration
from catchuparr.recorder_proxy import (
    capability_binding_current,
    configuration_generation,
    verify_recorder_capability,
)
from aio_recorder_media import _constant_text, _read_stream_response, _redis_text, _verify_media_identity

def follower_require(condition):
    if not condition:
        raise RuntimeError("synthetic follower assertion failed")

runtime.bootstrap()
token = {attempt.capability!r}
worker_id = {str(attempt.worker_id)!r}
channel_uuid = {channel_uuid!r}
stream_id = {stream_id!r}
account_id = {account_id!r}
profile_id = {int(profile_id)!r}
parent_pid = {os.getpid()!r}
follower_require(os.getpid() != parent_pid)
redis_client = RedisClient.get_client()
proxy_server = ProxyServer.get_instance()
expected_owner = {native_owner!r}
follower_require(_redis_text(proxy_server.worker_id) != expected_owner)
follower_require(_redis_text(redis_client.get(RedisKeys.channel_owner(worker_id))) == expected_owner)
follower_require(worker_id not in proxy_server.stream_managers)
metadata_key = RedisKeys.channel_metadata(worker_id)
clients_key = RedisKeys.clients(worker_id)
active_state = _constant_text(ChannelState.ACTIVE)
follower_require(
    _redis_text(redis_client.hget(metadata_key, ChannelMetadataField.STATE)) == active_state
)
follower_require(int(redis_client.scard(clients_key) or 0) == {parent_client_count!r})
binding = verify_recorder_capability(redis_client, token)
follower_require(binding is not None and capability_binding_current(redis_client, binding))
follower_require(binding.get("worker_id") == worker_id)
follower_require(binding.get("channel_uuid") == channel_uuid)
follower_require(binding.get("stream_id") == stream_id)
follower_require(binding.get("account_id") == account_id)
follower_require(binding.get("config_generation") == {str(attempt.config_generation)!r})
active = load_active_configuration()
follower_require(active is not None)
follower_require(configuration_generation(active) == binding.get("config_generation"))
record_before = read_worker_record(redis_client, worker_id)
follower_require(record_before is not None)
follower_require(record_before.get("worker_id") == worker_id)
follower_require(record_before.get("state") == "active")
follower_require(record_before.get("reservation_state") == "reserved")
follower_require(record_before.get("stream_id") == stream_id)
follower_require(record_before.get("account_id") == account_id)
follower_require(record_before.get("profile_id") == str(profile_id))
follower_require(record_before.get("config_generation") == binding.get("config_generation"))
profile_key = profile_connections_key(profile_id)
follower_require(int(redis_client.get(profile_key) or 0) == {expected_profile_count!r})

response = None
try:
    client = Client(raise_request_exception=False)
    response = client.get(
        {route_path!r},
        HTTP_HOST="localhost",
        HTTP_USER_AGENT="Synthetic recorder follower",
        HTTP_X_CATCHUPARR_RECORDER=token,
    )
    follower_require(response.status_code == 200)
    follower_require(response.get("Content-Type", "").split(";", 1)[0] == "video/mp2t")
    follower_require(not response.get("Location"))
    follower_require(getattr(response, "streaming", False))
    follower_require(worker_id not in proxy_server.stream_managers)
    media = _read_stream_response(response, minimum_bytes=188 * 512, timeout=20)
    _verify_media_identity({ffprobe!r}, media)
    client_manager = proxy_server.client_managers.get(worker_id)
    follower_require(client_manager is not None and client_manager.get_client_count() == 1)
    follower_require(_redis_text(redis_client.get(RedisKeys.channel_owner(worker_id))) == expected_owner)
    follower_require(int(redis_client.scard(clients_key) or 0) == {parent_client_count + 1!r})
finally:
    if response is not None:
        response.close()

follower_require(worker_id not in proxy_server.stream_managers)
follower_require(client_manager is not None and client_manager.get_client_count() == 0)
follower_require(_redis_text(redis_client.get(RedisKeys.channel_owner(worker_id))) == expected_owner)
follower_require(
    _redis_text(redis_client.hget(metadata_key, ChannelMetadataField.STATE)) == active_state
)
follower_require(int(redis_client.scard(clients_key) or 0) == {parent_client_count!r})
follower_require(capability_binding_current(redis_client, binding))
record_after = read_worker_record(redis_client, worker_id)
follower_require(record_after is not None)
follower_require(record_after.get("worker_id") == worker_id)
follower_require(record_after.get("state") == "active")
follower_require(record_after.get("reservation_state") == "reserved")
follower_require(record_after.get("reservation_id") == record_before.get("reservation_id"))
follower_require(record_after.get("lease_value") == binding.get("lease_value"))
follower_require(record_after.get("lease_fence") == binding.get("lease_fence"))
follower_require(record_after.get("config_generation") == binding.get("config_generation"))
follower_require(int(redis_client.get(profile_key) or 0) == {expected_profile_count!r})
print("CATCHUPARR_RECORDER_FOLLOWER_OK")
"""
    try:
        result = subprocess.run(
            [sys.executable, "-c", bootstrap],
            input=child_script.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            cwd="/app",
            timeout=45,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("Fresh-process recorder follower exceeded its bounded timeout") from None
    _require(
        result.returncode == 0
        and b"CATCHUPARR_RECORDER_FOLLOWER_OK" in result.stdout.splitlines(),
        "Fresh-process recorder follower did not attach and release cleanly",
    )

def probe_actual_recorder_media(root: Path) -> None:
    """Exercise the guarded private route against real native Dispatcharr APIs."""
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
    )
    from apps.m3u.models import M3UAccount
    from core.models import CoreSettings, StreamProfile
    from core.utils import RedisClient
    from django.test import Client
    from django.urls import resolve

    from catchuparr.adapters.recorder_proxy import (
        _release_worker_reservation,
        read_worker_record,
        worker_id_key,
    )
    from catchuparr.configuration import (
        active_settings_path,
        apply_configuration,
        load_active_configuration,
    )
    from catchuparr.engine.leases import RedisRecorderLease
    from catchuparr.engine.store import ArchiveStore
    from catchuparr.recorder_control import control_deny_path, control_state_path
    from catchuparr.recorder_proxy import (
        capability_binding_current,
        configuration_generation,
        issue_recorder_attempt,
        ranked_source_candidates,
        stop_recorder_attempt,
        verify_recorder_capability,
    )

    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")
    _require(ffmpeg is not None and ffprobe is not None, "AIO media probe requires ffmpeg and ffprobe")
    root = Path(root)
    fixture_root = root / "synthetic-recorder-sources"
    fixture_root.mkdir(parents=True, exist_ok=True)
    source_server = None
    active_path = active_settings_path()
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
    saved_default_profile = None
    default_profile_saved = False
    redis_client = None
    lease = None
    response = None
    managed_attempt = None
    attempts = []
    created_accounts = []
    created_streams = []
    created_profiles = []
    created_channel_profiles = []
    channel = None
    original_assignments = None
    profile_baselines = {}
    media_baselines = {}
    cleanup_errors = []
    native_yield_tracker = None

    try:
        payload_a = _make_transport_stream(
            ffmpeg, ffprobe, fixture_root / "source-a.ts",
            color="red", frequency=440, service_name="Synthetic Source A",
        )
        payload_b = _make_transport_stream(
            ffmpeg, ffprobe, fixture_root / "source-b.ts",
            color="blue", frequency=880, service_name="Synthetic Source B",
        )
        payload_c = _make_transport_stream(
            ffmpeg, ffprobe, fixture_root / "source-c.ts",
            color="green", frequency=660, service_name="Synthetic Source C",
        )
        source_server = _SyntheticSourceServer({
            "source-a.ts": payload_a,
            "source-b.ts": payload_b,
            "source-c.ts": payload_c,
        })
        source_server.thread.start()

        account_a = M3UAccount.objects.create(
            name="Synthetic media source A", is_active=True, max_streams=1,
        )
        account_b = M3UAccount.objects.create(
            name="Synthetic media source B", is_active=True, max_streams=1,
        )
        account_c = M3UAccount.objects.create(
            name="Synthetic media source C", is_active=True, max_streams=1,
        )
        created_accounts.extend((account_a, account_b, account_c))
        source_a = Stream.objects.create(
            name="Synthetic media A", url=f"{source_server.base_url}/source-a.ts", m3u_account=account_a,
        )
        source_b = Stream.objects.create(
            name="Synthetic media B", url=f"{source_server.base_url}/source-b.ts", m3u_account=account_b,
        )
        source_c = Stream.objects.create(
            name="Unassigned synthetic media C", url=f"{source_server.base_url}/source-c.ts",
            m3u_account=account_c,
        )
        created_streams.extend((source_a, source_b, source_c))
        channel = Channel.objects.create(
            name="Synthetic recorder media channel", channel_number=99, user_level=0,
        )
        profile_name = "Synthetic recorder media profile"
        channel_profile = ChannelProfile.objects.create(name=profile_name)
        created_channel_profiles.append(channel_profile)
        ChannelProfileMembership.objects.filter(channel_profile=channel_profile).update(
            enabled=False
        )
        ChannelProfileMembership.objects.update_or_create(
            channel_profile=channel_profile,
            channel=channel,
            defaults={"enabled": True},
        )
        ChannelStream.objects.create(channel=channel, stream=source_a, order=0)
        ChannelStream.objects.create(channel=channel, stream=source_b, order=1)
        original_assignments = list(
            ChannelStream.objects.filter(channel=channel)
            .order_by("order", "id").values_list("stream_id", "order")
        )

        profiles = []
        for account in created_accounts:
            profile = account.profiles.filter(is_default=True).first()
            _require(profile is not None, "Native M3U account default profile was not created")
            profile.max_streams = 1
            profile.is_active = True
            profile.save(update_fields=("max_streams", "is_active"))
            profiles.append(profile)
        profile_a, profile_b, profile_c = profiles
        created_profiles.extend(profiles)

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
                "    include: [Synthetic media source B]\n"
            ),
            "archive_root": str(root),
            "retention_hours": 1,
            "max_storage_gib": 1,
        }
        apply_configuration(settings, active_path=active_path)
        active = load_active_configuration(active_path)
        _require(active is not None, "Applied recorder media configuration was not readable")
        candidates = ranked_source_candidates(str(channel.uuid), active)
        _require(
            candidates is not None and [str(item.get("id")) for item in candidates] == [str(source_b.id)],
            "Applied include-only rule did not leave only source B",
        )
        generation = configuration_generation(active)

        redis_client = RedisClient.get_client()
        redis_client.ping()
        store = ArchiveStore(root)
        lease = RedisRecorderLease(
            redis_client, str(channel.uuid), ttl_seconds=120, archive_store=store,
        )
        _require(lease.acquire() is not None, "Synthetic recorder lease could not be acquired")
        profile_baselines = {
            int(profile.id): _profile_count(redis_client, profile.id, profile_connections_key)
            for profile in created_profiles
        }
        _require(
            all(value == 0 for key, value in profile_baselines.items() if isinstance(key, int)),
            "Synthetic profile counters were not empty",
        )
        for profile in created_profiles:
            marker_key = profile_credential_release_key(profile.id)
            profile_baselines[f"marker:{profile.id}"] = _key_dump(redis_client, marker_key)

        from apps.proxy.live_proxy.redis_keys import RedisKeys

        native_keys = {
            f"channel_stream:{source_a.id}",
            f"channel_stream:{source_b.id}",
            f"channel_stream:{source_c.id}",
            f"channel_stream:{channel.uuid}",
            f"channel_stream:{channel.id}",
            f"stream_profile:{source_a.id}",
            f"stream_profile:{source_b.id}",
            f"stream_profile:{channel.id}",
            RedisKeys.channel_metadata(str(channel.uuid)),
            RedisKeys.channel_owner(str(channel.uuid)),
        }
        native_snapshot = {key: _key_dump(redis_client, key) for key in native_keys}
        media_baselines = source_server.snapshot()[0]
        route_path = f"/catchuparr/recorder/{channel.uuid}"
        client = Client(raise_request_exception=False)

        def issue(candidate):
            attempt = issue_recorder_attempt(
                redis_client, lease,
                channel_uuid=str(channel.uuid),
                candidate=candidate,
                config_generation=generation,
                internal_base_url="http://127.0.0.1",
            )
            attempts.append(attempt)
            return attempt

        attempt_b = issue({"id": str(source_b.id), "account_id": str(account_b.id)})
        no_header = client.get(route_path, HTTP_HOST="localhost")
        _require(no_header.status_code == 401, "Private recorder route did not reject a missing capability")
        no_header.close()
        forged = attempt_b.capability[:-1] + ("A" if attempt_b.capability[-1] != "A" else "B")
        forged_response = client.get(
            route_path, HTTP_HOST="localhost", HTTP_X_CATCHUPARR_RECORDER=forged,
        )
        _require(forged_response.status_code == 403, "Private recorder route did not reject a forged capability")
        forged_response.close()

        public_path = f"/proxy/ts/stream/{attempt_b.worker_id}"
        match = resolve(public_path)
        _require(match.url_name == "stream", "Native public stream route could not be resolved")
        _require(
            bool(getattr(match.func, "_catchuparr_managed_id_guard", False)),
            "Native public stream route lacks the managed-worker rejection guard",
        )
        public_response = client.get(public_path, HTTP_HOST="localhost")
        _require(public_response.status_code == 404, "Native public route exposed a managed recorder ID")
        public_response.close()

        attempt_a = issue({"id": str(source_a.id), "account_id": str(account_a.id)})
        forbidden_response = client.get(
            route_path, HTTP_HOST="localhost",
            HTTP_X_CATCHUPARR_RECORDER=attempt_a.capability,
        )
        _require(forbidden_response.status_code == 403, "Include-only route accepted forbidden source A")
        forbidden_response.close()
        attempt_c = issue({"id": str(source_c.id), "account_id": str(account_c.id)})
        unassigned_response = client.get(
            route_path, HTTP_HOST="localhost",
            HTTP_X_CATCHUPARR_RECORDER=attempt_c.capability,
        )
        _require(unassigned_response.status_code == 403, "Recorder route accepted an unassigned source")
        unassigned_response.close()

        _require(
            {
                int(profile.id): _profile_count(redis_client, profile.id, profile_connections_key)
                for profile in created_profiles
            } == {key: value for key, value in profile_baselines.items() if isinstance(key, int)},
            "Denied recorder requests changed a provider profile counter",
        )
        _require(
            {profile.id: _key_dump(redis_client, profile_credential_release_key(profile.id))
             for profile in created_profiles}
            == {profile.id: profile_baselines[f"marker:{profile.id}"] for profile in created_profiles},
            "Denied recorder requests changed a provider credential marker",
        )
        _require(source_server.snapshot()[0] == media_baselines, "A denied source unexpectedly opened")

        response = client.get(
            route_path, HTTP_HOST="localhost", HTTP_USER_AGENT="Synthetic recorder integration",
            HTTP_X_CATCHUPARR_RECORDER=attempt_b.capability,
        )
        managed_attempt = attempt_b
        _require(response.status_code == 200, "Authorized private recorder route did not return HTTP 200")
        _require(response.get("Content-Type", "").split(";", 1)[0] == "video/mp2t",
                 "Authorized recorder response is not MPEG-TS")
        _require(not response.get("Location"), "Redirect profile leaked a provider redirect")
        _require(getattr(response, "streaming", False), "Recorder response is not a live stream")
        from apps.proxy.live_proxy.constants import ChannelMetadataField, ChannelState
        from apps.proxy.live_proxy.redis_keys import RedisKeys
        from apps.proxy.live_proxy.server import ProxyServer

        native_server = ProxyServer.get_instance()
        native_buffer = native_server.get_buffer(attempt_b.worker_id, profile=None)
        native_yield_tracker = _NativeBufferYieldTracker(native_buffer)
        parent_iterator = iter(
            native_yield_tracker.observe(iter(response.streaming_content))
        )
        media = _read_stream_iterator(
            parent_iterator, minimum_bytes=188 * 512, timeout=20, close=response.close,
        )
        _verify_media_identity(ffprobe, media)
        counts, _ = source_server.snapshot()
        _require(counts["source-a.ts"] == 0, "Forbidden source A was opened by the recorder")
        _require(counts["source-b.ts"] >= 1, "Included source B was not opened")
        _require(counts["source-c.ts"] == 0, "Unassigned source C was opened by the recorder")
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key) == profile_baselines[profile_b.id] + 1,
            "Authorized recorder did not reserve exactly one provider profile slot",
        )
        active_record = read_worker_record(redis_client, attempt_b.worker_id)
        _require(
            active_record is not None
            and active_record.get("profile_id") == str(profile_b.id)
            and active_record.get("stream_id") == str(source_b.id),
            "Native recorder did not reserve the expected source B profile",
        )
        _require(
            {key: _key_dump(redis_client, key) for key in native_keys} == native_snapshot,
            "Recorder source changed an unscoped native live Redis key",
        )
        _require(
            list(ChannelStream.objects.filter(channel=channel).order_by("order", "id")
                 .values_list("stream_id", "order")) == original_assignments,
            "Recorder source changed native ChannelStream assignments",
        )

        native_owner, native_manager, native_client_manager = _wait_for_native_active(
            redis_client,
            native_server,
            attempt_b.worker_id,
            redis_keys=RedisKeys,
            metadata_field=ChannelMetadataField,
            channel_state=ChannelState,
            timeout=45,
        )
        active_state = _constant_text(ChannelState.ACTIVE)
        parent_client_count = int(redis_client.scard(RedisKeys.clients(attempt_b.worker_id)) or 0)
        _require(parent_client_count == 1, "Native Redis client count is not one for the parent")
        _require(
            native_client_manager.get_client_count() == 1,
            "Native local client count is not one for the parent",
        )
        _require(native_manager.running, "Native parent stream manager is not running")
        _require(
            _redis_text(
                redis_client.hget(
                    RedisKeys.channel_metadata(attempt_b.worker_id),
                    ChannelMetadataField.STATE,
                )
            ) == active_state,
            "Native parent metadata is not ACTIVE",
        )
        counts_before_follower, active_before_follower = source_server.snapshot()
        record_before_follower = read_worker_record(redis_client, attempt_b.worker_id)
        _require(record_before_follower is not None, "Recorder worker disappeared before follower attach")
        reservation_count_before_follower = _profile_count(
            redis_client, profile_b.id, profile_connections_key,
        )
        _require(
            reservation_count_before_follower == profile_baselines[profile_b.id] + 1,
            "Recorder provider slot was not reserved before follower attach",
        )
        _require(
            active_before_follower["source-b.ts"] >= 1,
            "Synthetic B source was not active before follower attach",
        )
        _run_fresh_process_follower(
            attempt_b,
            channel_uuid=str(channel.uuid),
            stream_id=str(source_b.id),
            account_id=str(account_b.id),
            profile_id=int(profile_b.id),
            expected_profile_count=reservation_count_before_follower,
            ffprobe=ffprobe,
            native_owner=native_owner,
            parent_client_count=parent_client_count,
        )
        counts_after_follower, active_after_follower = source_server.snapshot()
        _require(
            counts_after_follower == counts_before_follower,
            "Fresh-process follower opened another synthetic provider connection",
        )
        _require(
            active_after_follower["source-b.ts"] >= 1,
            "Closing the follower stopped the parent synthetic B source",
        )
        record_after_follower = read_worker_record(redis_client, attempt_b.worker_id)
        _require(
            record_after_follower is not None
            and record_after_follower.get("worker_id") == attempt_b.worker_id
            and record_after_follower.get("state") == "active"
            and record_after_follower.get("reservation_state") == "reserved"
            and record_after_follower.get("reservation_id")
            == record_before_follower.get("reservation_id")
            and record_after_follower.get("lease_value")
            == record_before_follower.get("lease_value")
            and record_after_follower.get("lease_fence")
            == record_before_follower.get("lease_fence")
            and record_after_follower.get("config_generation")
            == record_before_follower.get("config_generation")
            and record_after_follower.get("stream_id") == str(source_b.id)
            and record_after_follower.get("profile_id") == str(profile_b.id),
            "Fresh-process follower changed the parent recorder worker or reservation",
        )
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key)
            == reservation_count_before_follower,
            "Fresh-process follower changed provider profile capacity",
        )
        _require(
            _redis_text(redis_client.get(RedisKeys.channel_owner(attempt_b.worker_id)))
            == native_owner,
            "Follower disconnect changed the native owner",
        )
        _require(
            _redis_text(
                redis_client.hget(
                    RedisKeys.channel_metadata(attempt_b.worker_id),
                    ChannelMetadataField.STATE,
                )
            ) == active_state,
            "Follower disconnect changed native ACTIVE metadata",
        )
        _require(
            native_server.stream_managers.get(attempt_b.worker_id) is native_manager,
            "Follower disconnect replaced the native parent stream manager",
        )
        _require(native_manager.running, "Follower disconnect stopped the native parent manager")
        _require(
            int(redis_client.scard(RedisKeys.clients(attempt_b.worker_id)) or 0)
            == parent_client_count,
            "Follower disconnect did not return native global clients to the parent baseline",
        )
        _require(
            native_client_manager.get_client_count() == parent_client_count,
            "Follower disconnect changed the native parent local client count",
        )

        publication_floor = int(
            native_buffer.redis_client.get(native_buffer.buffer_index_key) or 0
        )
        _require(publication_floor > 0, "Native recorder parent did not publish a buffer chunk")
        continued_media = _read_stream_iterator(
            parent_iterator,
            minimum_bytes=188 * 512,
            timeout=20,
            close=response.close,
            accept_chunk=lambda: (
                native_yield_tracker.last_yielded_index is not None
                and native_yield_tracker.last_yielded_index > publication_floor
            ),
        )
        published_head = int(
            native_buffer.redis_client.get(native_buffer.buffer_index_key) or 0
        )
        _require(
            published_head > publication_floor
            and native_yield_tracker.last_yielded_index is not None
            and native_yield_tracker.last_yielded_index > publication_floor,
            "Parent did not publish and consume a native buffer chunk newer than follower close",
        )
        _verify_media_identity(ffprobe, continued_media)
        _require(
            continued_media != media,
            "Parent recorder response did not advance to different useful B media after follower close",
        )
        _require(
            source_server.snapshot()[0] == counts_before_follower,
            "Parent recorder continuation opened another synthetic provider connection",
        )
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key)
            == reservation_count_before_follower,
            "Parent recorder continuation changed provider profile capacity",
        )
        _require(
            source_server.snapshot()[1]["source-b.ts"] >= 1,
            "Parent recorder no longer owns an active synthetic B source after follower close",
        )

        response.close()
        native_yield_tracker.close()
        native_yield_tracker = None
        response = None
        _require(stop_recorder_attempt(redis_client, attempt_b, lease), "Managed recorder worker did not stop cleanly")
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key) == profile_baselines[profile_b.id],
            "Recorder teardown did not restore provider profile capacity",
        )
        _require(
            not _release_worker_reservation(redis_client, attempt_b.worker_id),
            "Duplicate cleanup claimed a second recorder reservation",
        )
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key) == profile_baselines[profile_b.id],
            "Duplicate cleanup decremented provider profile capacity twice",
        )
        released_record = read_worker_record(redis_client, attempt_b.worker_id)
        _require(released_record is not None, "Recorder worker ledger disappeared before cleanup verification")
        from catchuparr.adapters.recorder_proxy import reservation_credential_marker_key

        _require(
            not redis_client.exists(
                reservation_credential_marker_key(released_record.get("reservation_id", ""))
            ),
            "Recorder cleanup left its private credential reservation marker",
        )
        cleanup_deadline = time.monotonic() + 5
        while time.monotonic() < cleanup_deadline:
            _counts, active_counts = source_server.snapshot()
            if not any(active_counts.values()):
                break
            time.sleep(0.05)
        _require(not any(source_server.snapshot()[1].values()), "Synthetic provider connection remained open after stop")
        _require(
            {key: _key_dump(redis_client, key) for key in native_keys} == native_snapshot,
            "Recorder teardown changed an unscoped native live Redis key",
        )
        managed_attempt = None

        # Keep a fresh, otherwise valid capability so this 403 tests only the
        # applied configuration generation check.
        generation_attempt = issue({"id": str(source_b.id), "account_id": str(account_b.id)})
        generation_binding = verify_recorder_capability(redis_client, generation_attempt.capability)
        _require(
            generation_binding is not None
            and capability_binding_current(redis_client, generation_binding),
            "Generation-check capability was not valid before changing configuration",
        )
        counts_before_generation_change = source_server.snapshot()[0]
        counters_before_generation_change = {
            profile.id: _profile_count(redis_client, profile.id, profile_connections_key)
            for profile in created_profiles
        }
        changed_settings = dict(settings, retention_hours=2)
        apply_configuration(changed_settings, active_path=active_path)
        stale_generation = client.get(
            route_path, HTTP_HOST="localhost",
            HTTP_X_CATCHUPARR_RECORDER=generation_attempt.capability,
        )
        _require(stale_generation.status_code == 403, "Recorder capability survived an applied config change")
        stale_generation.close()
        _require(
            source_server.snapshot()[0] == counts_before_generation_change,
            "Stale generation capability opened a synthetic provider source",
        )
        _require(
            {
                profile.id: _profile_count(redis_client, profile.id, profile_connections_key)
                for profile in created_profiles
            } == counters_before_generation_change,
            "Stale generation capability changed provider profile capacity",
        )
        apply_configuration(settings, active_path=active_path)
        _require(
            capability_binding_current(redis_client, generation_binding),
            "Generation-check capability did not become valid after restoring its configuration",
        )

        # Keep another otherwise-valid capability, then replace only its owner lease.
        owner_attempt = issue({"id": str(source_b.id), "account_id": str(account_b.id)})
        owner_binding = verify_recorder_capability(redis_client, owner_attempt.capability)
        _require(
            owner_binding is not None and capability_binding_current(redis_client, owner_binding),
            "Lease-check capability was not valid before replacing its owner",
        )
        counts_before_lease_change = source_server.snapshot()[0]
        counters_before_lease_change = {
            profile.id: _profile_count(redis_client, profile.id, profile_connections_key)
            for profile in created_profiles
        }
        _require(lease.release(), "Synthetic recorder lease did not release")
        lease = None
        replacement = RedisRecorderLease(redis_client, str(channel.uuid), ttl_seconds=120, archive_store=store)
        lease = replacement
        _require(replacement.acquire() is not None, "Replacement recorder lease could not be acquired")
        replayed = client.get(
            route_path, HTTP_HOST="localhost",
            HTTP_X_CATCHUPARR_RECORDER=owner_attempt.capability,
        )
        _require(replayed.status_code == 403, "Old owner capability was replayable after lease replacement")
        replayed.close()
        _require(
            source_server.snapshot()[0] == counts_before_lease_change,
            "Old owner capability opened a synthetic provider source",
        )
        _require(
            {
                profile.id: _profile_count(redis_client, profile.id, profile_connections_key)
                for profile in created_profiles
            } == counters_before_lease_change,
            "Old owner capability changed provider profile capacity",
        )
        _require(replacement.release(), "Replacement recorder lease did not release")
        lease = None

        _require(
            list(ChannelStream.objects.filter(channel=channel).order_by("order", "id")
                 .values_list("stream_id", "order")) == original_assignments,
            "Native source assignments changed during recorder cleanup",
        )
        _require(
            all(
                _profile_count(redis_client, profile.id, profile_connections_key) == profile_baselines[profile.id]
                for profile in created_profiles
            ),
            "Recorder teardown did not restore all synthetic profile counters",
        )
        _require(
            all(
                _key_dump(redis_client, profile_credential_release_key(profile.id))
                == profile_baselines[f"marker:{profile.id}"]
                for profile in created_profiles
            ),
            "Recorder teardown changed a shared profile credential marker",
        )
        print("AIO private recorder route returned identified B audio/video TS with cross-process reuse and fenced cleanup")
    finally:
        if response is not None:
            try:
                response.close()
            except Exception:
                cleanup_errors.append("response")
        if native_yield_tracker is not None:
            try:
                native_yield_tracker.close()
            except Exception:
                cleanup_errors.append("native-yield-tracker")
        if redis_client is not None:
            if managed_attempt is not None and lease is not None:
                try:
                    if not stop_recorder_attempt(redis_client, managed_attempt, lease):
                        cleanup_errors.append("managed-worker")
                except Exception:
                    cleanup_errors.append("managed-worker")
            try:
                from catchuparr.adapters.recorder_proxy import stop_managed_workers

                if not stop_managed_workers(timeout_seconds=8):
                    cleanup_errors.append("remaining-workers")
            except Exception:
                cleanup_errors.append("remaining-workers")
            for attempt in attempts:
                try:
                    attempt.revoke(redis_client)
                    record = read_worker_record(redis_client, attempt.worker_id)
                    if record and record.get("reservation_state") == "reserved":
                        _release_worker_reservation(redis_client, attempt.worker_id)
                    redis_client.delete(worker_id_key(attempt.worker_id))
                except Exception:
                    cleanup_errors.append("attempt-ledger")
            if lease is not None:
                try:
                    lease.release()
                except Exception:
                    cleanup_errors.append("lease")
            try:
                from catchuparr.adapters.recorder_proxy import managed_worker_ids

                workers_active = bool(managed_worker_ids(redis_client, active_only=True))
            except Exception:
                workers_active = True
            if workers_active:
                cleanup_errors.append("active-managed-workers")
            capacity_restored = all(
                _profile_count(redis_client, profile.id, profile_connections_key)
                == profile_baselines.get(profile.id, 0)
                for profile in created_profiles
            )
            if not capacity_restored:
                cleanup_errors.append("profile-capacity")
        if source_server is not None:
            try:
                source_server.close()
                if any(source_server.snapshot()[1].values()):
                    cleanup_errors.append("source-connections")
            except Exception:
                cleanup_errors.append("source-server")
        if default_profile_saved:
            try:
                _set_stream_default_profile(CoreSettings, saved_default_profile)
                _require(
                    str(_stream_default_profile_id(CoreSettings)) == str(saved_default_profile),
                    "Native default StreamProfile was not restored",
                )
            except Exception:
                cleanup_errors.append("default-stream-profile")
        try:
            if active_existed:
                active_path.write_bytes(active_bytes)
            else:
                active_path.unlink(missing_ok=True)
            if active_lock_existed:
                active_lock_path.write_bytes(active_lock_bytes)
            else:
                active_lock_path.unlink(missing_ok=True)
            if reset_marker_existed:
                reset_marker_path.write_bytes(reset_marker_bytes)
            else:
                reset_marker_path.unlink(missing_ok=True)
            if control_existed:
                control_path.write_bytes(control_bytes)
            else:
                control_path.unlink(missing_ok=True)
            if control_deny_existed:
                control_deny.write_bytes(control_deny_bytes)
            else:
                control_deny.unlink(missing_ok=True)
        except Exception:
            cleanup_errors.append("active-config")
        if not cleanup_errors:
            try:
                for profile in created_profiles:
                    if redis_client is not None:
                        redis_client.delete(
                            profile_connections_key(profile.id),
                            profile_credential_release_key(profile.id),
                        )
                    profile.delete()
                for stream in created_streams:
                    stream.delete()
                for account in created_accounts:
                    account.delete()
                if channel is not None:
                    channel.delete()
                for channel_profile in created_channel_profiles:
                    channel_profile.delete()
            except Exception:
                cleanup_errors.append("synthetic-database-rows")
        try:
            shutil.rmtree(fixture_root, ignore_errors=True)
        except Exception:
            cleanup_errors.append("fixture-files")
        if cleanup_errors:
            raise RuntimeError("Synthetic recorder media cleanup was incomplete")

    probe_actual_live_archive_isolation(root)


def _wait_for_indexed_tone(
    run,
    store,
    channel_uuid: str,
    ffmpeg: str,
    ffprobe: str,
    *,
    expected_hz: int,
    existing_ids: set[str],
    minimum: int,
    timeout: float,
    cooperative_sleep=None,
) -> list:
    from aio_recorder_failover import _segment_has_useful_av, _segment_tone_frequency

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        _require(
            run.thread is not None and run.thread.is_alive(),
            "Synthetic recorder task ended before expected media was indexed",
        )
        matched = []
        for segment in store.segments(channel_uuid):
            if segment.id in existing_ids:
                continue
            _require(
                _segment_has_useful_av(ffprobe, segment.path),
                "Indexed synthetic archive segment lacks decoded audio/video",
            )
            frequency = _segment_tone_frequency(ffmpeg, segment.path)
            _assert_tone_matches(frequency, expected_hz, "Indexed synthetic archive")
            matched.append(segment)
        if len(matched) >= minimum:
            return matched
        (cooperative_sleep or time.sleep)(0.2)
    raise RuntimeError("Synthetic recorder did not index the expected decoded source in time")


def _native_source_metadata(redis_client, metadata_key) -> dict[str, str]:
    raw = redis_client.hgetall(metadata_key)
    relevant = {}
    for key, value in raw.items():
        name = _redis_text(key).lower()
        if any(token in name for token in ("url", "source", "stream", "profile")):
            relevant[name] = _redis_text(value)
    return relevant


def _wait_for_native_client_count(
    redis_client,
    native_server,
    redis_keys,
    worker_id: str,
    expected: int,
    *,
    timeout: float = 15,
    reject_excess: bool = False,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        manager = native_server.client_managers.get(worker_id)
        global_count = int(redis_client.scard(redis_keys.clients(worker_id)) or 0)
        local_count = int(manager.get_client_count()) if manager is not None else -1
        if global_count == expected and local_count == expected:
            return
        if reject_excess:
            _require(
                global_count <= expected and local_count <= expected,
                "Native live route registered more clients than the fixture expected",
            )
        time.sleep(0.1)
    raise RuntimeError("Native live client count did not reach the expected bounded state")


def _probe_running_recorder_controls(
    *,
    channel_uuid: str,
    settings: dict,
    active_path: Path,
    archive_store,
    source_server,
    redis_client,
    native_server,
    native_buffer,
    native_owner: str,
    native_manager,
    native_client_manager,
    metadata_key: str,
    active_state: str,
    assignment_snapshot: dict,
    source_metadata: dict,
    live_reader,
    profile_a,
    profile_b,
    profile_baselines: dict,
    native_a_marker,
    initial_b_request_baseline: int,
    ffmpeg: str,
    ffprobe: str,
    harnesses: list,
    gevent_sleep,
    initial_run,
) -> None:
    """Exercise pause, resume and schedule fences against live native media."""
    import logging
    from datetime import datetime, timedelta, timezone
    from unittest.mock import patch

    from aio_recorder_failover import (
        _assert_worker_cleanup,
        _RecorderTaskRun,
        _worker_records,
    )
    from apps.m3u.connection_pool import (
        profile_connections_key,
        profile_credential_release_key,
    )
    from apps.plugins.models import PluginConfig
    from apps.proxy.live_proxy.constants import ChannelMetadataField
    from apps.proxy.live_proxy.redis_keys import RedisKeys

    from catchuparr import runtime, tasks
    from catchuparr.configuration import load_active_configuration
    from catchuparr.recorder_control import load_recorder_control
    from catchuparr.recorder_proxy import CAPABILITY_PREFIX, configuration_generation
    from catchuparr.schedule import schedule_from_snapshot, schedule_is_active

    def require_live_a(
        stage: str,
        expected_b_active: int,
        *,
        read_fresh_media: bool,
    ) -> None:
        observations = {}
        try:
            owner = redis_client.get(RedisKeys.channel_owner(channel_uuid))
            observations["owner_read_ok"] = True
            observations["owner_present"] = owner is not None
            observations["owner_matches"] = _redis_text(owner) == native_owner
        except Exception:
            observations.update({
                "owner_read_ok": False,
                "owner_present": False,
                "owner_matches": False,
            })

        try:
            manager = native_server.stream_managers.get(channel_uuid)
            observations["stream_manager_lookup_ok"] = True
            observations["stream_manager_present"] = manager is not None
            observations["stream_manager_identity"] = manager is native_manager
            observations["stream_manager_running"] = bool(
                getattr(manager, "running", False)
            )
        except Exception:
            observations.update({
                "stream_manager_lookup_ok": False,
                "stream_manager_present": False,
                "stream_manager_identity": False,
                "stream_manager_running": False,
            })

        try:
            client_manager = native_server.client_managers.get(channel_uuid)
            observations["client_manager_lookup_ok"] = True
            observations["client_manager_present"] = client_manager is not None
            observations["client_manager_identity"] = (
                client_manager is native_client_manager
            )
        except Exception:
            client_manager = None
            observations.update({
                "client_manager_lookup_ok": False,
                "client_manager_present": False,
                "client_manager_identity": False,
            })

        try:
            observations["local_client_count_read_ok"] = client_manager is not None
            observations["local_client_count_one"] = (
                client_manager is not None
                and int(client_manager.get_client_count()) == 1
            )
        except Exception:
            observations.update({
                "local_client_count_read_ok": False,
                "local_client_count_one": False,
            })

        try:
            global_client_count = int(
                redis_client.scard(RedisKeys.clients(channel_uuid)) or 0
            )
            observations["global_client_count_read_ok"] = True
            observations["global_client_count_one"] = global_client_count == 1
        except Exception:
            observations.update({
                "global_client_count_read_ok": False,
                "global_client_count_one": False,
            })

        try:
            state = _redis_text(
                redis_client.hget(metadata_key, ChannelMetadataField.STATE)
            )
            observations["metadata_state_read_ok"] = True
            observations["metadata_active"] = state == active_state
        except Exception:
            observations.update({
                "metadata_state_read_ok": False,
                "metadata_active": False,
            })

        flags = _native_live_a_invariant_flags(**observations)
        _require(
            all(flags.values()),
            _native_live_a_invariant_message(stage, flags),
        )
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1
            and _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id] + expected_b_active
            and bool(redis_client.exists(f"catchuparr:recorder:{channel_uuid}"))
            == bool(expected_b_active),
            "Recorder control changed native provider capacity unexpectedly",
        )
        _require(
            _key_dump(redis_client, profile_credential_release_key(profile_a.id))
            == native_a_marker,
            "Recorder control changed the native live A credential release marker",
        )
        _require(
            {key: _key_dump(redis_client, key) for key in assignment_snapshot}
            == assignment_snapshot
            and _native_source_metadata(redis_client, metadata_key) == source_metadata,
            "Recorder control changed native live A assignments or source metadata",
        )
        counts, active_counts = source_server.snapshot()
        _require(
            counts["source-a.ts"] > 0
            and active_counts["source-a.ts"] == 1
            and active_counts["source-b.ts"] == expected_b_active,
            "Recorder control interrupted live A or left the archive B source active",
        )
        if read_fresh_media:
            floor = int(native_buffer.redis_client.get(native_buffer.buffer_index_key) or 0)
            _require(floor > 0, "Native live A has no published buffer head")
            media, index = live_reader.read_after(
                floor, minimum_bytes=188 * 100, timeout=20,
            )
            _require(index > floor, "Native live A did not publish fresh media after recorder control")
            _verify_audio_video_tone(ffmpeg, ffprobe, media, 440, "Native live A during recorder control")

    def current_private_worker() -> str:
        records = [
            record for record in _worker_records(redis_client, channel_uuid)
            if record.get("state") == "active"
            and record.get("reservation_state") == "reserved"
        ]
        _require(len(records) == 1, "Expected one active private archive worker")
        worker_id = records[0].get("worker_id")
        _require(
            bool(worker_id) and worker_id != channel_uuid,
            "Archive recording did not use its dedicated worker identity",
        )
        return str(worker_id)

    def persisted_recording_enabled() -> object:
        return dict(PluginConfig.objects.get(key="catchuparr").settings or {}).get(
            "recording_enabled"
        )

    def persist_draft(draft_settings: dict) -> None:
        from django.db import transaction

        with transaction.atomic():
            row = PluginConfig.objects.select_for_update().get(key="catchuparr")
            current_settings = dict(row.settings or {})
            current_settings.update(draft_settings)
            row.settings = current_settings
            row.save(update_fields=("settings",))

    def wait_for_private_cleanup(worker_id: str, expected_b_requests: int) -> None:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            manager = native_server.stream_managers.get(worker_id)
            owner = redis_client.get(RedisKeys.channel_owner(worker_id))
            clients = int(redis_client.scard(RedisKeys.clients(worker_id)) or 0)
            active_counts = source_server.snapshot()[1]
            profile_count = _profile_count(
                redis_client, profile_b.id, profile_connections_key,
            )
            if (
                manager is None and owner is None and clients == 0
                and active_counts["source-b.ts"] == 0
                and source_server.snapshot()[0]["source-b.ts"] == expected_b_requests
                and profile_count == profile_baselines[profile_b.id]
            ):
                break
            gevent_sleep(0.1)
        _require(
            native_server.stream_managers.get(worker_id) is None
            and redis_client.get(RedisKeys.channel_owner(worker_id)) is None
            and int(redis_client.scard(RedisKeys.clients(worker_id)) or 0) == 0
            and source_server.snapshot()[1]["source-b.ts"] == 0
            and source_server.snapshot()[0]["source-b.ts"] == expected_b_requests
            and _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id],
            "Recorder supervisor did not release the private worker and provider slot",
        )
        _assert_worker_cleanup(redis_client, channel_uuid)
        _require(
            all(
                not redis_client.exists(CAPABILITY_PREFIX + str(record.get("capability_digest")))
                for record in _worker_records(redis_client, channel_uuid)
                if record.get("capability_digest")
            ),
            "Recorder supervisor left a private capability active",
        )

    def reconcile_arguments() -> tuple:
        captured = []

        def capture(*, args=None, queue=None, **_kwargs):
            captured.append((tuple(args or ()), queue))
            return None

        with patch.object(tasks.record_channel, "apply_async", side_effect=capture):
            result = tasks.reconcile_recorders.run()
        _require(result == {"queued": 1} and len(captured) == 1,
                 "Real recorder reconciliation did not queue exactly the selected channel")
        args, queue = captured[0]
        _require(queue == "dvr" and len(args) == 3 and str(args[0]) == channel_uuid,
                 "Recorder reconciliation produced unexpected task arguments")
        current_active = load_active_configuration(active_path)
        current_control = load_recorder_control(active_path)
        _require(
            current_active is not None
            and args[1] == configuration_generation(current_active)
            and args[2] == current_control.generation,
            "Recorder reconciliation queued stale applied generations",
        )
        return args

    def invoke_control_action(action, name: str) -> dict:
        dispatched = []

        def capture(*args, **kwargs):
            dispatched.append((args, kwargs))
            return None

        with patch.object(tasks.reconcile_recorders, "apply_async", side_effect=capture):
            result = action()
        _require(
            len(dispatched) == 1 and dispatched[0][1].get("queue") == "dvr",
            f"Real {name} action did not enqueue one DVR reconciliation",
        )
        return result

    def start_reconciled_run(label: str) -> _RecorderTaskRun:
        args = reconcile_arguments()
        run = _RecorderTaskRun(
            channel_uuid, startup_timeout=45, media_idle_timeout=25,
        )
        harnesses.append(run)
        run.start(task_args=args)
        _require(
            run.expected_generation == args[1]
            and run.expected_control_generation == args[2],
            f"{label} recorder did not use the generations selected by reconciliation",
        )
        return run

    persist_draft(settings)
    previous_control_generation = load_recorder_control(active_path).generation
    active = load_active_configuration(active_path)
    _require(active is not None, "Running-control probe requires an applied configuration")
    _require(
        initial_run.thread is not None and initial_run.thread.is_alive()
        and configuration_generation(active) == initial_run.expected_generation,
        "Initial B recorder is not actively using the applied configuration",
    )
    _require(
        settings.get("filter_config")
        and schedule_is_active(
            schedule_from_snapshot(active["recording_schedule"]["channels"][channel_uuid]),
            active["recording_schedule"]["timezone"],
            datetime.now(timezone.utc),
        ),
        "Running-control probe requires an active applied schedule",
    )
    initial_worker_id = current_private_worker()
    require_live_a("initial_b_active", 1, read_fresh_media=True)
    initial_counts, initial_active_counts = source_server.snapshot()
    _require(
        initial_counts["source-b.ts"] == initial_b_request_baseline + 1
        and initial_active_counts["source-b.ts"] == 1,
        "Initial running-control B recorder did not hold one provider connection",
    )
    initial_control = load_recorder_control(active_path)
    _require(
        initial_control.generation == initial_run.expected_control_generation
        and not initial_control.paused,
        "Running archive worker did not start with the current control generation",
    )

    paused_result = invoke_control_action(runtime.pause_recorders, "Pause")
    paused = load_recorder_control(active_path)
    _require(
        paused.paused and paused.generation > initial_control.generation
        and paused_result == {"paused": True, "generation": paused.generation}
        and persisted_recording_enabled() is False,
        "Real Pause action did not persist and advance recorder control",
    )
    initial_run.join_after_supervisor(timeout=25, cooperative_sleep=gevent_sleep)
    _require(
        initial_run.result == {"status": "stopped"},
        "Pause did not stop the running recorder through its supervisor",
    )
    wait_for_private_cleanup(initial_worker_id, initial_counts["source-b.ts"])
    require_live_a("after_pause_private_cleanup", 0, read_fresh_media=True)
    pause_reconcile_dispatches = []

    def capture_pause_reconcile(*, args=None, queue=None, **_kwargs):
        pause_reconcile_dispatches.append((tuple(args or ()), queue))
        return None

    with patch.object(tasks.record_channel, "apply_async", side_effect=capture_pause_reconcile):
        pause_reconcile_result = tasks.reconcile_recorders.run()
    _require(
        pause_reconcile_result == {"queued": 0} and not pause_reconcile_dispatches,
        "Paused real reconciliation queued another recorder task",
    )
    _require(
        source_server.snapshot()[0]["source-b.ts"] == initial_counts["source-b.ts"],
        "Paused recorder reopened archive B after its worker stopped",
    )

    resumed_result = invoke_control_action(runtime.resume_recorders, "Resume")
    resumed = load_recorder_control(active_path)
    _require(
        not resumed.paused and resumed.generation > paused.generation
        and resumed_result == {"paused": False, "generation": resumed.generation}
        and persisted_recording_enabled() is True,
        "Real Resume action did not publish a newer enabled control generation",
    )
    _require(
        resumed.generation > previous_control_generation,
        "Pause and Resume did not fence the pre-pause queued job generation",
    )

    resumed_baseline_ids = {
        segment.id for segment in archive_store.segments(channel_uuid)
    }
    resumed_request_baseline = source_server.snapshot()[0]["source-b.ts"]
    resumed_run = start_reconciled_run("Resumed")
    resumed_segments = _wait_for_indexed_tone(
        resumed_run, archive_store, channel_uuid, ffmpeg, ffprobe,
        expected_hz=880, existing_ids=resumed_baseline_ids, minimum=1, timeout=90,
        cooperative_sleep=gevent_sleep,
    )
    _require(
        all(segment.id not in resumed_baseline_ids for segment in resumed_segments),
        "Resume did not index fresh source B archive segments",
    )
    resumed_counts, resumed_active_counts = source_server.snapshot()
    _require(
        resumed_counts["source-b.ts"] == resumed_request_baseline + 1
        and resumed_active_counts["source-b.ts"] == 1,
        "Resume did not open exactly one new B provider connection",
    )
    resumed_worker_id = current_private_worker()
    require_live_a("after_resume_new_private_worker", 1, read_fresh_media=True)
    resumed_generation = resumed_run.expected_generation
    before_closed_apply = source_server.snapshot()[0]

    closed_settings = dict(
        settings,
        filter_config=settings["filter_config"].replace(
            "rules:\n", "schedule: {}\nrules:\n", 1,
        ),
    )
    persist_draft(closed_settings)
    closed_apply_result = runtime.apply_configuration()
    _require(
        closed_apply_result.get("applied") is True,
        "Current-row recorder Apply did not accept the closed schedule",
    )
    closed_active = load_active_configuration(active_path)
    _require(closed_active is not None, "Applied closed schedule was not readable")
    closed_generation = configuration_generation(closed_active)
    _require(
        closed_generation != resumed_generation
        and closed_active["recording_schedule"]["channels"][channel_uuid]
        == {"mode": "weekly", "intervals": []},
        "Applying a closed schedule did not publish an immutable empty channel schedule",
    )
    resumed_run.join_after_supervisor(timeout=25, cooperative_sleep=gevent_sleep)
    _require(
        resumed_run.result == {"status": "stopped"},
        "Applying a closed schedule did not stop the running recorder supervisor",
    )
    wait_for_private_cleanup(resumed_worker_id, resumed_counts["source-b.ts"])
    require_live_a("after_closed_schedule_private_cleanup", 0, read_fresh_media=True)
    _require(
        source_server.snapshot()[0]["source-b.ts"] == before_closed_apply["source-b.ts"],
        "Closed-schedule Apply reopened archive B after the running worker stopped",
    )

    from catchuparr.tasks import record_channel

    counts_before_stale = source_server.snapshot()
    profile_counts_before_stale = {
        int(profile.id): _profile_count(
            redis_client, profile.id, profile_connections_key,
        )
        for profile in (profile_a, profile_b)
    }
    current_control = load_recorder_control(active_path)
    stale_configuration = record_channel.run(
        channel_uuid, resumed_generation, current_control.generation,
    )
    stale_control = record_channel.run(
        channel_uuid, initial_run.expected_generation, previous_control_generation,
    )
    outside_schedule = record_channel.run(
        channel_uuid, closed_generation, current_control.generation,
    )
    _require(
        stale_configuration == {"status": "stale_configuration"}
        and stale_control == {"status": "stale_control"}
        and outside_schedule == {"status": "outside_schedule"},
        "Actual recorder task did not reject stale generations and a closed schedule",
    )
    _require(
        source_server.snapshot() == counts_before_stale
        and profile_counts_before_stale == {
            int(profile.id): _profile_count(
                redis_client, profile.id, profile_connections_key,
            )
            for profile in (profile_a, profile_b)
        }
        and not redis_client.exists(f"catchuparr:recorder:{channel_uuid}")
        and not any(record.get("state") == "active" for record in _worker_records(
            redis_client, channel_uuid,
        )),
        "Rejected stale or closed-schedule task changed a provider or worker resource",
    )
    require_live_a("after_stale_task_rejections", 0, read_fresh_media=True)

    window_start = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    window_end = window_start + timedelta(minutes=2 if datetime.now(timezone.utc).second < 30 else 3)
    weekday = window_start.strftime("%A").lower()
    start_clock = window_start.strftime("%H:%M")
    end_clock = window_end.strftime("%H:%M")

    timed_settings = dict(
        settings,
        filter_config=(
            settings["filter_config"].split("rules:", 1)[0]
            + "timezone: UTC\n"
            + "schedule:\n"
            + f"  {weekday}:\n"
            + f"    - {{start: \"{start_clock}\", end: \"{end_clock}\"}}\n"
            + "rules:\n"
            + "  - channels: {profile: all}\n"
            + "    include: [Synthetic archive isolation B]\n"
        ),
    )
    persist_draft(timed_settings)
    timed_apply_result = runtime.apply_configuration()
    _require(
        timed_apply_result.get("applied") is True,
        "Current-row recorder Apply did not accept the active timed schedule",
    )
    timed_active = load_active_configuration(active_path)
    _require(timed_active is not None, "Timed schedule snapshot was not readable")
    timed_generation = configuration_generation(timed_active)
    timed_schedule = schedule_from_snapshot(
        timed_active["recording_schedule"]["channels"][channel_uuid]
    )
    _require(
        schedule_is_active(timed_schedule, "UTC", datetime.now(timezone.utc)),
        "Synthetic timed schedule was not active at recorder startup",
    )
    timed_baseline_ids = {
        segment.id for segment in archive_store.segments(channel_uuid)
    }
    timed_request_baseline = source_server.snapshot()[0]["source-b.ts"]
    schedule_events = []

    class ScheduleEventCapture(logging.Handler):
        def emit(self, record):
            if (
                record.name == "catchuparr"
                and record.getMessage() == "[Catchuparr] recorder_schedule_closed"
            ):
                schedule_events.append(record.getMessage())

    logger = logging.getLogger("catchuparr")
    previous_log_level = logger.level
    schedule_event_capture = ScheduleEventCapture(logging.INFO)
    logger.setLevel(logging.INFO)
    logger.addHandler(schedule_event_capture)
    try:
        timed_run = start_reconciled_run("Timed schedule")
        timed_segments = _wait_for_indexed_tone(
            timed_run, archive_store, channel_uuid, ffmpeg, ffprobe,
            expected_hz=880, existing_ids=timed_baseline_ids, minimum=2, timeout=90,
            cooperative_sleep=gevent_sleep,
        )
        _require(bool(timed_segments), "Active timed schedule did not index useful source B media")
        timed_counts, timed_active_counts = source_server.snapshot()
        _require(
            timed_counts["source-b.ts"] == timed_request_baseline + 1
            and timed_active_counts["source-b.ts"] == 1,
            "Timed recorder did not open exactly one B provider connection",
        )
        timed_worker_id = current_private_worker()
        require_live_a("during_timed_schedule", 1, read_fresh_media=True)
        _require(
            timed_run.expected_generation == timed_generation
            and _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id] + 1
            and redis_client.exists(f"catchuparr:recorder:{channel_uuid}"),
            "Timed recorder did not hold its applied generation, lease, and provider slot",
        )
        close_deadline = time.monotonic() + 150
        while schedule_is_active(timed_schedule, "UTC", datetime.now(timezone.utc)):
            _require(
                time.monotonic() < close_deadline,
                "Synthetic weekly schedule did not reach its real-time close boundary",
            )
            _require(
                timed_run.thread is not None and timed_run.thread.is_alive()
                and source_server.snapshot()[1]["source-b.ts"] == 1
                and _profile_count(redis_client, profile_b.id, profile_connections_key)
                == profile_baselines[profile_b.id] + 1
                and redis_client.exists(f"catchuparr:recorder:{channel_uuid}"),
                "Recorder stopped before the applied weekly schedule reached its end",
            )
            gevent_sleep(0.25)
        timed_run.join_after_supervisor(timeout=20, cooperative_sleep=gevent_sleep)
        _require(
            timed_run.result == {"status": "stopped"},
            "Recorder supervisor did not stop when the active schedule window closed",
        )
        _require(
            bool(schedule_events),
            "Recorder supervisor did not report the closed schedule as its stop reason",
        )
        wait_for_private_cleanup(timed_worker_id, timed_counts["source-b.ts"])
        require_live_a("after_timed_schedule_close", 0, read_fresh_media=True)
        latest_timed_active = load_active_configuration(active_path)
        _require(
            latest_timed_active is not None
            and configuration_generation(latest_timed_active) == timed_generation
            and load_recorder_control(active_path).generation
            == timed_run.expected_control_generation,
            "Schedule-boundary stop changed configuration or control generation",
        )
        after_close_counts = source_server.snapshot()
        after_close_profiles = {
            int(profile.id): _profile_count(
                redis_client, profile.id, profile_connections_key,
            )
            for profile in (profile_a, profile_b)
        }
    finally:
        logger.removeHandler(schedule_event_capture)
        logger.setLevel(previous_log_level)

    closed_by_clock = record_channel.run(
        channel_uuid, timed_generation, timed_run.expected_control_generation,
    )
    _require(
        closed_by_clock == {"status": "outside_schedule"}
        and source_server.snapshot() == after_close_counts
        and {
            int(profile.id): _profile_count(
                redis_client, profile.id, profile_connections_key,
            )
            for profile in (profile_a, profile_b)
        } == after_close_profiles
        and not redis_client.exists(f"catchuparr:recorder:{channel_uuid}")
        and after_close_counts[0]["source-b.ts"] == initial_b_request_baseline + 3,
        "Actual closed schedule admitted a task or changed recorder resources",
    )
    print("AIO running pause, resume, schedule-close, stale-job, and live-viewer gates passed")


def probe_actual_live_archive_isolation(root: Path) -> None:
    """Keep native live A active while real recorder tasks first share A, then isolate B."""
    from aio_recorder_failover import (
        _assert_worker_cleanup,
        _DjangoHTTPBridge,
        _make_paced_transport_stream,
        _RecorderTaskRun,
        _segment_has_useful_av,
        _segment_tone_frequency,
        _SyntheticFailoverSourceServer,
        _worker_records,
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
    )
    from apps.m3u.models import M3UAccount
    from apps.plugins.models import PluginConfig
    from apps.proxy.live_proxy.constants import ChannelMetadataField, ChannelState
    from apps.proxy.live_proxy.redis_keys import RedisKeys
    from apps.proxy.live_proxy.server import ProxyServer
    from core.models import CoreSettings, StreamProfile
    from core.utils import RedisClient
    from django.test import Client
    from gevent import sleep as gevent_sleep

    from catchuparr.adapters.recorder_proxy import _release_worker_reservation
    from catchuparr.configuration import (
        active_settings_path,
        apply_configuration,
        load_active_configuration,
    )
    from catchuparr.engine.store import ArchiveStore
    from catchuparr.recorder_control import control_deny_path, control_state_path
    from catchuparr.recorder_proxy import ranked_source_candidates

    _require(
        os.environ.get("CATCHUPARR_INTEGRATION_TEST") == "1",
        "Live/archive isolation probe requires a disposable integration container",
    )
    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")
    _require(
        ffmpeg is not None and ffprobe is not None,
        "AIO live/archive probe requires ffmpeg and ffprobe",
    )

    root = Path(root)
    fixture_root = root / "synthetic-live-archive-fixtures"
    archive_root = root / "synthetic-live-archive"
    fixture_root.mkdir(parents=True, exist_ok=True)
    archive_root.mkdir(parents=True, exist_ok=True)

    active_path = active_settings_path()
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
    plugin_config = PluginConfig.objects.get(key="catchuparr")
    original_plugin_settings = dict(plugin_config.settings or {})

    original_base_url = channel_tasks.get_dvr_stream_base_url
    saved_default_profile = None
    default_profile_saved = False
    source_server = None
    bridge = None
    redis_client = None
    live_response = None
    live_reader = None
    live_session = None
    native_server = None
    native_diagnostic = None
    native_diagnostic_report = None
    channel = None
    created_accounts = []
    created_streams = []
    created_profiles = []
    created_channel_profiles = []
    harnesses = []
    profile_baselines = {}
    marker_baselines = {}
    cleanup_errors = []

    try:
        payload_a = _make_paced_transport_stream(
            ffmpeg,
            ffprobe,
            fixture_root / "source-a.ts",
            service_name="Synthetic Live Source A",
            frequency=440,
        )
        payload_b = _make_paced_transport_stream(
            ffmpeg,
            ffprobe,
            fixture_root / "source-b.ts",
            service_name="Synthetic Archive Source B",
            frequency=880,
        )
        source_server = _SyntheticFailoverSourceServer(
            {"source-a.ts": payload_a, "source-b.ts": payload_b},
            {"source-a.ts": 8.0, "source-b.ts": 8.0},
        )
        source_server.thread.start()

        account_a = M3UAccount.objects.create(
            name="Synthetic live isolation A", is_active=True, max_streams=1,
        )
        account_b = M3UAccount.objects.create(
            name="Synthetic archive isolation B", is_active=True, max_streams=1,
        )
        created_accounts.extend((account_a, account_b))
        stream_a = Stream.objects.create(
            name="Synthetic isolation source A",
            url=f"{source_server.base_url}/source-a.ts",
            m3u_account=account_a,
        )
        stream_b = Stream.objects.create(
            name="Synthetic isolation source B",
            url=f"{source_server.base_url}/source-b.ts",
            m3u_account=account_b,
        )
        created_streams.extend((stream_a, stream_b))
        channel = Channel.objects.create(
            name="Synthetic live archive isolation",
            channel_number=97,
            user_level=0,
        )
        profile_name = "Synthetic live archive profile"
        channel_profile = ChannelProfile.objects.create(name=profile_name)
        created_channel_profiles.append(channel_profile)
        ChannelProfileMembership.objects.filter(channel_profile=channel_profile).update(
            enabled=False
        )
        ChannelProfileMembership.objects.update_or_create(
            channel_profile=channel_profile,
            channel=channel,
            defaults={"enabled": True},
        )
        ChannelStream.objects.create(channel=channel, stream=stream_a, order=0)
        ChannelStream.objects.create(channel=channel, stream=stream_b, order=1)
        assignment_rows = list(
            ChannelStream.objects.filter(channel=channel)
            .order_by("order", "id")
            .values_list("stream_id", "order")
        )
        _require(
            assignment_rows == [(stream_a.id, 0), (stream_b.id, 1)],
            "Synthetic live/archive fixture did not preserve A then B assignment order",
        )

        profiles = []
        for account in created_accounts:
            profile = account.profiles.filter(is_default=True).first()
            _require(profile is not None, "Synthetic live/archive M3U profile was not created")
            profile.max_streams = 1
            profile.is_active = True
            profile.save(update_fields=("max_streams", "is_active"))
            profiles.append(profile)
        profile_a, profile_b = profiles
        created_profiles.extend(profiles)

        proxy_profile = StreamProfile.objects.filter(
            name__iexact="Proxy", locked=True, is_active=True,
        ).first()
        _require(proxy_profile is not None and proxy_profile.is_proxy(),
                 "Native non-redirect Proxy StreamProfile is unavailable")
        saved_default_profile = _stream_default_profile_id(CoreSettings)
        default_profile_saved = True
        _set_stream_default_profile(CoreSettings, proxy_profile.id)
        _require(
            str(_stream_default_profile_id(CoreSettings)) == str(proxy_profile.id),
            "Native default StreamProfile was not set to Proxy",
        )

        settings = {
            "filter_config": (
                "version: 1\n"
                f"profile: {profile_name}\n"
                "rules:\n"
                "  - channels: {profile: all}\n"
                "    exclude: []\n"
            ),
            "archive_root": str(archive_root),
            "retention_hours": 1,
            "max_storage_gib": 1,
        }
        apply_configuration(settings, active_path=active_path)
        active = load_active_configuration(active_path)
        _require(active is not None, "Applied no-rules live/archive configuration was not readable")
        _require(
            ranked_source_candidates(str(channel.uuid), active) is None,
            "No-rules configuration did not preserve native channel route selection",
        )
        from catchuparr.runtime import load_config

        runtime_config = load_config()
        _require(
            runtime_config is not None and str(channel.uuid) in runtime_config.channel_uuids,
            "Synthetic live/archive channel is absent from active recorder configuration",
        )

        redis_client = RedisClient.get_client()
        redis_client.ping()
        profile_baselines = {
            int(profile.id): _profile_count(redis_client, profile.id, profile_connections_key)
            for profile in created_profiles
        }
        marker_baselines = {
            int(profile.id): _key_dump(
                redis_client, profile_credential_release_key(profile.id),
            )
            for profile in created_profiles
        }
        _require(
            all(value == 0 for value in profile_baselines.values()),
            "Synthetic live/archive provider counters were not empty before use",
        )
        _require(
            all(value is None for value in marker_baselines.values()),
            "Synthetic live/archive credential markers were not empty before use",
        )
        store = ArchiveStore(archive_root)
        bridge = _DjangoHTTPBridge()
        bridge.start()
        channel_tasks.get_dvr_stream_base_url = lambda: bridge.base_url

        client = Client(raise_request_exception=False)
        native_server = ProxyServer.get_instance()
        worker_id = str(channel.uuid)
        from aio_native_redis_diagnostic import NativeRedisInitDiagnostic

        native_diagnostic = NativeRedisInitDiagnostic(
            worker_id=worker_id,
            redis_keys=RedisKeys,
            native_server=native_server,
        )
        native_diagnostic.start(
            redis_client=redis_client,
        )
        live_response = client.get(
            f"/proxy/ts/stream/{channel.uuid}",
            HTTP_HOST="localhost",
            HTTP_USER_AGENT="Synthetic live/archive integration",
        )
        _require(live_response.status_code == 200, "Native live A route did not return HTTP 200")
        _require(
            live_response.get("Content-Type", "").split(";", 1)[0] == "video/mp2t",
            "Native live A route did not return MPEG-TS",
        )
        _require(
            not live_response.get("Location"),
            "Native Proxy profile redirected the live A route",
        )
        _require(getattr(live_response, "streaming", False), "Native live A route was not a stream")

        # Keep one reader on the real public response through native startup.
        # On failure, outer cleanup stops the channel before joining the reader.
        live_session, native_buffer = _create_native_live_reader_session(
            live_response, native_server, worker_id,
        )
        live_reader = live_session.reader
        _wait_for_native_client_count(
            redis_client,
            native_server,
            RedisKeys,
            worker_id,
            1,
            timeout=45,
            reject_excess=True,
        )
        native_owner, native_manager, native_client_manager = _wait_for_native_active(
            redis_client,
            native_server,
            worker_id,
            redis_keys=RedisKeys,
            metadata_field=ChannelMetadataField,
            channel_state=ChannelState,
            timeout=45,
        )
        # Capture only the native startup interval. Once the real route client
        # is registered and the first ownership check has completed, restore
        # every diagnostic hook before the longer archive/control phases.
        native_diagnostic_report = native_diagnostic.close()
        diagnostic_observed = native_diagnostic_report["observed"]
        _require(
            all(
                diagnostic_observed[name]
                for name in (
                    "channel_initialize",
                    "metadata_write",
                    "native_manager_thread_start",
                    "ownership_check",
                    "ownership_allowed",
                    "client_add",
                )
            )
            and native_diagnostic_report["redis_targets_match"],
            "Native Redis startup diagnostic did not observe the complete synthetic init path",
        )
        active_state = _constant_text(ChannelState.ACTIVE)
        first_publication_floor = int(
            native_buffer.redis_client.get(native_buffer.buffer_index_key) or 0
        )
        _require(
            first_publication_floor > 0,
            "Native live A did not publish an initial buffer chunk",
        )
        first_live_media, first_live_index = live_reader.read_after(
            first_publication_floor, minimum_bytes=188 * 512, timeout=20,
        )
        _require(
            first_live_index > first_publication_floor,
            "Native live A did not deliver a newly published buffer chunk",
        )
        _verify_audio_video_tone(ffmpeg, ffprobe, first_live_media, 440, "Native live A")
        _require(native_manager.running, "Native live A stream manager is not running")
        _require(
            native_client_manager is native_server.client_managers.get(worker_id),
            "Native live A client manager changed during activation",
        )
        metadata_key = RedisKeys.channel_metadata(worker_id)
        _require(
            _redis_text(redis_client.hget(metadata_key, ChannelMetadataField.STREAM_ID))
            == str(stream_a.id),
            "Native live A metadata does not identify assigned source A",
        )
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1,
            "Native live A did not reserve exactly one A profile slot",
        )
        native_a_marker = _key_dump(
            redis_client, profile_credential_release_key(profile_a.id),
        )
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id],
            "Native live A unexpectedly reserved B profile capacity",
        )
        counts, active_counts = source_server.snapshot()
        _require(counts["source-a.ts"] >= 1 and active_counts["source-a.ts"] == 1,
                 "Native live A did not open its synthetic A source")
        _require(counts["source-b.ts"] == 0, "Native live A opened archive source B")
        _wait_for_native_client_count(redis_client, native_server, RedisKeys, worker_id, 1)

        assignment_keys = {
            f"channel_stream:{stream_a.id}",
            f"channel_stream:{stream_b.id}",
            f"channel_stream:{worker_id}",
            f"channel_stream:{channel.id}",
            f"stream_profile:{stream_a.id}",
            f"stream_profile:{stream_b.id}",
            f"stream_profile:{channel.id}",
            RedisKeys.channel_owner(worker_id),
        }
        assignment_snapshot = {
            key: _key_dump(redis_client, key) for key in assignment_keys
        }
        _require(
            assignment_snapshot[f"channel_stream:{stream_a.id}"] is not None
            and assignment_snapshot[f"stream_profile:{stream_a.id}"] is not None,
            "Native live A did not publish its channel assignment and profile keys",
        )
        source_metadata = _native_source_metadata(redis_client, metadata_key)
        _require(
            source_metadata.get("stream_id") == str(stream_a.id)
            and source_metadata.get("stream_name") == stream_a.name
            and source_metadata.get("url") == stream_a.url
            and any(
                "profile" in key and value
                for key, value in source_metadata.items()
            ),
            "Native live A source metadata is incomplete",
        )
        # With no policy, the real recorder task must attach to this same
        # native A worker and index decoded A rather than opening a second
        # provider connection.
        run_a = _RecorderTaskRun(worker_id, startup_timeout=45, media_idle_timeout=25)
        run_a.start()
        harnesses.append(run_a)
        baseline_ids = {segment.id for segment in store.segments(worker_id)}
        segments_a = _wait_for_indexed_tone(
            run_a, store, worker_id, ffmpeg, ffprobe,
            expected_hz=440, existing_ids=baseline_ids, minimum=1, timeout=90,
            cooperative_sleep=gevent_sleep,
        )
        _require(bool(segments_a), "No-rules recorder did not archive synthetic live source A")
        _wait_for_native_client_count(redis_client, native_server, RedisKeys, worker_id, 2)
        _require(
            native_server.stream_managers.get(worker_id) is native_manager
            and native_manager.running
            and _redis_text(redis_client.get(RedisKeys.channel_owner(worker_id))) == native_owner
            and _redis_text(redis_client.hget(metadata_key, ChannelMetadataField.STATE))
            == active_state,
            "No-rules archive did not share the existing native live A worker",
        )
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1,
            "No-rules archive acquired an extra A profile reservation",
        )
        counts_after_shared, active_after_shared = source_server.snapshot()
        _require(
            counts_after_shared["source-a.ts"] == counts["source-a.ts"]
            and active_after_shared["source-a.ts"] == 1
            and counts_after_shared["source-b.ts"] == 0,
            "No-rules archive opened a second provider connection or source B",
        )
        _require(
            {key: _key_dump(redis_client, key) for key in assignment_keys}
            == assignment_snapshot,
            "No-rules archive changed native live A assignment keys",
        )
        _require(
            _native_source_metadata(redis_client, metadata_key) == source_metadata,
            "No-rules archive changed native live A source metadata",
        )
        run_a.stop()
        run_a.join(cooperative_sleep=gevent_sleep)
        _wait_for_native_client_count(redis_client, native_server, RedisKeys, worker_id, 1)
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1,
            "Stopping the shared no-rules archive released live A capacity",
        )
        _require(
            _key_dump(redis_client, profile_credential_release_key(profile_a.id))
            == native_a_marker,
            "Stopping the shared no-rules archive changed the live A release marker",
        )

        # Applying B while A is live must create a dedicated recorder worker.
        settings_b = dict(
            settings,
            filter_config=(
                "version: 1\n"
                f"profile: {profile_name}\n"
                "rules:\n"
                "  - channels: {profile: all}\n"
                "    include: [Synthetic archive isolation B]\n"
            ),
        )
        apply_configuration(settings_b, active_path=active_path)
        active_b = load_active_configuration(active_path)
        _require(active_b is not None, "Applied B-only recorder configuration was not readable")
        candidates_b = ranked_source_candidates(worker_id, active_b)
        _require(
            candidates_b is not None
            and [str(candidate.get("id")) for candidate in candidates_b] == [str(stream_b.id)],
            "Applied include-only policy did not select only assigned source B",
        )
        baseline_ids_b = {segment.id for segment in store.segments(worker_id)}
        publication_floor_before_b = int(
            native_buffer.redis_client.get(native_buffer.buffer_index_key) or 0
        )
        _require(publication_floor_before_b > 0,
                 "Native live A did not publish chunks before the B archive started")
        run_b = _RecorderTaskRun(worker_id, startup_timeout=45, media_idle_timeout=25)
        run_b.start()
        harnesses.append(run_b)
        segments_b = _wait_for_indexed_tone(
            run_b, store, worker_id, ffmpeg, ffprobe,
            expected_hz=880, existing_ids=baseline_ids_b, minimum=1, timeout=90,
            cooperative_sleep=gevent_sleep,
        )
        _require(bool(segments_b), "B-only recorder did not index synthetic archive source B")
        _require(
            all(segment.id not in baseline_ids_b for segment in segments_b),
            "B-only recorder did not publish new archive segment IDs",
        )
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1
            and _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id] + 1,
            "Live A and archive B did not hold exactly one separate profile slot each",
        )
        counts_during_b, active_during_b = source_server.snapshot()
        _require(
            counts_during_b["source-a.ts"] == counts["source-a.ts"]
            and active_during_b["source-a.ts"] == 1
            and counts_during_b["source-b.ts"] == counts_after_shared["source-b.ts"] + 1
            and active_during_b["source-b.ts"] == 1,
            "Archive B disturbed live A or failed to open its own source",
        )
        _require(
            native_server.stream_managers.get(worker_id) is native_manager
            and native_manager.running
            and _redis_text(redis_client.get(RedisKeys.channel_owner(worker_id))) == native_owner,
            "Archive B replaced the existing native live A owner or manager",
        )
        _require(
            _redis_text(redis_client.hget(metadata_key, ChannelMetadataField.STATE))
            == active_state,
            "Native live A was not ACTIVE while archive B ran",
        )
        _wait_for_native_client_count(redis_client, native_server, RedisKeys, worker_id, 1)
        _require(
            _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1,
            "Dedicated archive B added a second live A profile reservation",
        )
        _require(
            _key_dump(redis_client, profile_credential_release_key(profile_a.id))
            == native_a_marker,
            "Dedicated archive B changed the live A release marker",
        )
        _require(
            {key: _key_dump(redis_client, key) for key in assignment_keys}
            == assignment_snapshot,
            "Archive B changed native live A assignment keys",
        )
        _require(
            _native_source_metadata(redis_client, metadata_key) == source_metadata,
            "Archive B changed native live A source metadata",
        )
        _require(
            list(ChannelStream.objects.filter(channel=channel).order_by("order", "id")
                 .values_list("stream_id", "order")) == assignment_rows,
            "Archive B reordered native ChannelStream rows",
        )
        active_records = [
            record for record in _worker_records(redis_client, worker_id)
            if record.get("state") == "active"
            and record.get("reservation_state") == "reserved"
        ]
        _require(
            len(active_records) == 1,
            "B archive did not have exactly one active managed worker",
        )
        active_record = active_records[0]
        b_worker_id = active_record.get("worker_id")
        _require(
            b_worker_id and b_worker_id != worker_id
            and active_record.get("stream_id") == str(stream_b.id)
            and active_record.get("profile_id") == str(profile_b.id),
            "Archive B did not use a separate managed worker and B profile",
        )
        publication_floor_during_b = int(
            native_buffer.redis_client.get(native_buffer.buffer_index_key) or 0
        )
        _require(
            publication_floor_during_b > publication_floor_before_b,
            "Native live A did not publish a newer chunk after archive B became active",
        )
        live_media_during_b, live_index_during_b = live_reader.read_after(
            publication_floor_during_b, minimum_bytes=188 * 100, timeout=20,
        )
        _require(live_index_during_b > publication_floor_during_b,
                 "Native live A did not yield a new buffer chunk while B recorded")
        _verify_audio_video_tone(
            ffmpeg, ffprobe, live_media_during_b, 440, "Native live A during archive B",
        )

        _probe_running_recorder_controls(
            channel_uuid=worker_id,
            settings=settings_b,
            active_path=active_path,
            archive_store=store,
            source_server=source_server,
            redis_client=redis_client,
            native_server=native_server,
            native_buffer=native_buffer,
            native_owner=native_owner,
            native_manager=native_manager,
            native_client_manager=native_client_manager,
            metadata_key=metadata_key,
            active_state=active_state,
            assignment_snapshot=assignment_snapshot,
            source_metadata=source_metadata,
            live_reader=live_reader,
            profile_a=profile_a,
            profile_b=profile_b,
            profile_baselines=profile_baselines,
            native_a_marker=native_a_marker,
            initial_b_request_baseline=counts_after_shared["source-b.ts"],
            ffmpeg=ffmpeg,
            ffprobe=ffprobe,
            harnesses=harnesses,
            gevent_sleep=gevent_sleep,
            initial_run=run_b,
        )
        counts_after_running_controls, _ = source_server.snapshot()

        run_b.stop()
        run_b.join(cooperative_sleep=gevent_sleep)
        all_segments_after_b_stop = store.segments(worker_id)
        segment_by_id = {segment.id: segment for segment in all_segments_after_b_stop}
        _require(
            baseline_ids_b <= set(segment_by_id),
            "B archive cleanup removed previously indexed A media",
        )
        _require(
            {segment.id for segment in segments_b} <= set(segment_by_id),
            "B archive cleanup removed an indexed B segment",
        )
        for segment in all_segments_after_b_stop:
            if segment.id in baseline_ids_b:
                _require(
                    _segment_has_useful_av(ffprobe, segment.path),
                    "Previously indexed A archive media became unreadable",
                )
                _assert_tone_matches(
                    _segment_tone_frequency(ffmpeg, segment.path),
                    440,
                    "Previously indexed live A archive",
                )
            else:
                _require(
                    _segment_has_useful_av(ffprobe, segment.path),
                    "Final indexed B archive segment lacks decoded audio/video",
                )
                _assert_tone_matches(
                    _segment_tone_frequency(ffmpeg, segment.path),
                    880,
                    "Final indexed archive B",
                )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            b_manager = native_server.stream_managers.get(b_worker_id)
            b_clients = int(redis_client.scard(RedisKeys.clients(b_worker_id)) or 0)
            _counts, b_active = source_server.snapshot()
            if (
                b_manager is None
                and redis_client.get(RedisKeys.channel_owner(b_worker_id)) is None
                and b_clients == 0
                and b_active["source-b.ts"] == 0
            ):
                break
            time.sleep(0.1)
        _require(
            native_server.stream_managers.get(b_worker_id) is None
            and redis_client.get(RedisKeys.channel_owner(b_worker_id)) is None
            and int(redis_client.scard(RedisKeys.clients(b_worker_id)) or 0) == 0
            and source_server.snapshot()[0]["source-b.ts"]
            == counts_after_running_controls["source-b.ts"]
            and source_server.snapshot()[1]["source-b.ts"] == 0,
            "Stopping archive B left its private native worker or provider source active",
        )
        _assert_worker_cleanup(redis_client, worker_id)
        _require(
            not _release_worker_reservation(redis_client, b_worker_id),
            "Duplicate archive B cleanup claimed a second profile reservation",
        )
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id]
            and _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id] + 1,
            "Stopping archive B did not release B once while preserving live A capacity",
        )
        _require(
            _key_dump(redis_client, profile_credential_release_key(profile_a.id))
            == native_a_marker,
            "Stopping archive B changed the live A release marker",
        )
        _require(
            native_server.stream_managers.get(worker_id) is native_manager
            and native_manager.running
            and _redis_text(redis_client.get(RedisKeys.channel_owner(worker_id))) == native_owner,
            "Stopping archive B stopped the native live A manager or owner",
        )
        _require(
            _redis_text(redis_client.hget(metadata_key, ChannelMetadataField.STATE))
            == active_state,
            "Native live A stopped being ACTIVE after archive B cleanup",
        )
        _wait_for_native_client_count(redis_client, native_server, RedisKeys, worker_id, 1)
        _require(
            {key: _key_dump(redis_client, key) for key in assignment_keys}
            == assignment_snapshot,
            "Stopping archive B changed native live A assignment keys",
        )
        _require(
            _native_source_metadata(redis_client, metadata_key) == source_metadata,
            "Stopping archive B changed native live A source metadata",
        )
        counts_after_b_stop, active_after_b_stop = source_server.snapshot()
        _require(
            counts_after_b_stop["source-a.ts"] == counts["source-a.ts"]
            and active_after_b_stop["source-a.ts"] >= 1,
            "Stopping archive B interrupted the existing live A source",
        )
        stop_floor = int(native_buffer.redis_client.get(native_buffer.buffer_index_key) or 0)
        _require(stop_floor > 0, "Native live A buffer head disappeared after B cleanup")
        live_media_after_b_stop, live_index_after_b_stop = live_reader.read_after(
            stop_floor, minimum_bytes=188 * 100, timeout=20,
        )
        _require(live_index_after_b_stop > stop_floor,
                 "Native live A did not yield fresh media after archive B cleanup")
        _verify_audio_video_tone(
            ffmpeg, ffprobe, live_media_after_b_stop, 440,
            "Native live A after archive B cleanup",
        )

        live_session.close()
        live_response = None
        live_reader = None
        live_session = None
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            owner = redis_client.get(RedisKeys.channel_owner(worker_id))
            manager = native_server.stream_managers.get(worker_id)
            client_manager = native_server.client_managers.get(worker_id)
            clients = int(redis_client.scard(RedisKeys.clients(worker_id)) or 0)
            counts_final, active_final = source_server.snapshot()
            if (
                owner is None
                and manager is None
                and clients == 0
                and (client_manager is None or client_manager.get_client_count() == 0)
                and active_final["source-a.ts"] == 0
                and counts_final["source-a.ts"] == counts["source-a.ts"]
                and _profile_count(redis_client, profile_a.id, profile_connections_key)
                == profile_baselines[profile_a.id]
            ):
                break
            # ClientManager schedules the owner's normal disconnect cleanup
            # onto ProxyServer's gevent hub. Drive that hub while waiting so
            # the harness observes the same cleanup path as a running web
            # worker instead of blocking it with time.sleep.
            gevent_sleep(0.1)
        _require(
            redis_client.get(RedisKeys.channel_owner(worker_id)) is None
            and native_server.stream_managers.get(worker_id) is None
            and int(redis_client.scard(RedisKeys.clients(worker_id)) or 0) == 0
            and _profile_count(redis_client, profile_a.id, profile_connections_key)
            == profile_baselines[profile_a.id]
            and source_server.snapshot()[0]["source-a.ts"] == counts["source-a.ts"]
            and source_server.snapshot()[1]["source-a.ts"] == 0,
            "Stopping the final native live A client did not release its owner and provider slot",
        )
        _require(
            _profile_count(redis_client, profile_b.id, profile_connections_key)
            == profile_baselines[profile_b.id],
            "Archive B profile capacity changed after both workers stopped",
        )
        _require(
            {
                int(profile.id): _key_dump(
                    redis_client, profile_credential_release_key(profile.id),
                )
                for profile in created_profiles
            } == marker_baselines,
            "Live/archive cleanup did not restore synthetic credential markers",
        )
        _require(
            list(ChannelStream.objects.filter(channel=channel).order_by("order", "id")
                 .values_list("stream_id", "order")) == assignment_rows,
            "Live/archive probe changed native ChannelStream rows",
        )
        print("AIO native live A remained isolated while the real recorder indexed B")
    finally:
        # On a startup failure, freeze the observed state before cleanup
        # changes Redis keys or native worker state. The successful path has
        # already captured and restored the hooks above.
        if native_diagnostic is not None and native_diagnostic_report is None:
            try:
                native_diagnostic_report = native_diagnostic.close()
            except Exception:
                # Diagnostics must not replace the probe's original result.
                pass
        for run in reversed(harnesses):
            if run.thread is not None:
                try:
                    run.stop()
                    run.join(20, cooperative_sleep=gevent_sleep)
                except Exception:
                    cleanup_errors.append("recorder-task")
        if live_session is not None:
            try:
                force_release = None
                if native_server is not None and channel is not None:
                    worker_to_release = str(channel.uuid)

                    def force_release_native() -> None:
                        from apps.proxy.live_proxy.services.channel_service import ChannelService

                        ChannelService.stop_channel(worker_to_release)

                    force_release = force_release_native
                live_session.close(force_release=force_release)
                live_reader = None
                live_session = None
            except Exception:
                cleanup_errors.append("native-live-response")
        elif live_response is not None:
            try:
                live_response.close()
            except Exception:
                cleanup_errors.append("native-live-response")
        try:
            from catchuparr.adapters.recorder_proxy import stop_managed_workers

            if not stop_managed_workers(timeout_seconds=8):
                cleanup_errors.append("managed-workers")
        except Exception:
            cleanup_errors.append("managed-workers")
        channel_tasks.get_dvr_stream_base_url = original_base_url
        if bridge is not None:
            try:
                bridge.close()
                if bridge.server.active_requests():
                    cleanup_errors.append("http-bridge")
            except Exception:
                cleanup_errors.append("http-bridge")
        if native_server is not None and channel is not None and redis_client is not None:
            try:
                worker_id = str(channel.uuid)
                if native_server.stream_managers.get(worker_id) is not None:
                    from apps.proxy.live_proxy.services.channel_service import ChannelService

                    # This is failure-path cleanup only. The successful path
                    # above still proves that closing the real response stops
                    # its own native client and leaves the worker intact until
                    # that final client disconnects.
                    ChannelService.stop_channel(worker_id)
                    deadline = time.monotonic() + 10
                    while (
                        time.monotonic() < deadline
                        and native_server.stream_managers.get(worker_id) is not None
                    ):
                        time.sleep(0.1)
                if redis_client.get(RedisKeys.channel_owner(worker_id)) is not None:
                    cleanup_errors.append("native-live-owner")
                if native_server.stream_managers.get(worker_id) is not None:
                    cleanup_errors.append("native-live-manager")
            except Exception:
                cleanup_errors.append("native-live-cleanup-check")
        if native_diagnostic is not None:
            try:
                if native_diagnostic_report is not None:
                    print(
                        "CATCHUPARR_NATIVE_REDIS_INIT_DIAGNOSTIC "
                        + json.dumps(native_diagnostic_report, sort_keys=True)
                    )
            except Exception:
                # Diagnostics must not replace the probe's original result.
                pass
        if source_server is not None:
            try:
                source_server.close()
                if any(source_server.snapshot()[1].values()):
                    cleanup_errors.append("synthetic-source-connections")
            except Exception:
                cleanup_errors.append("synthetic-source-server")
        if default_profile_saved:
            try:
                _set_stream_default_profile(CoreSettings, saved_default_profile)
                _require(
                    str(_stream_default_profile_id(CoreSettings)) == str(saved_default_profile),
                    "Native default StreamProfile was not restored",
                )
            except Exception:
                cleanup_errors.append("default-stream-profile")
        try:
            if active_existed:
                active_path.write_bytes(active_bytes)
            else:
                active_path.unlink(missing_ok=True)
            if active_lock_existed:
                active_lock_path.write_bytes(active_lock_bytes)
            else:
                active_lock_path.unlink(missing_ok=True)
            if reset_marker_existed:
                reset_marker_path.write_bytes(reset_marker_bytes)
            else:
                reset_marker_path.unlink(missing_ok=True)
            if control_existed:
                control_path.write_bytes(control_bytes)
            else:
                control_path.unlink(missing_ok=True)
            if control_deny_existed:
                control_deny.write_bytes(control_deny_bytes)
            else:
                control_deny.unlink(missing_ok=True)
        except Exception:
            cleanup_errors.append("active-config")
        try:
            latest_plugin_config = PluginConfig.objects.get(key="catchuparr")
            latest_plugin_config.settings = original_plugin_settings
            latest_plugin_config.save(update_fields=("settings",))
        except Exception:
            cleanup_errors.append("plugin-settings")
        if redis_client is not None and not cleanup_errors:
            try:
                _require(
                    all(
                        _profile_count(redis_client, profile.id, profile_connections_key)
                        == profile_baselines.get(profile.id, 0)
                        for profile in created_profiles
                    ),
                    "Live/archive cleanup did not restore provider profile capacity",
                )
                _require(
                    {
                        int(profile.id): _key_dump(
                            redis_client, profile_credential_release_key(profile.id),
                        )
                        for profile in created_profiles
                    } == marker_baselines,
                    "Live/archive cleanup did not restore credential markers",
                )
            except Exception:
                cleanup_errors.append("profile-capacity")
        if not cleanup_errors:
            try:
                for profile in created_profiles:
                    if redis_client is not None:
                        redis_client.delete(
                            profile_connections_key(profile.id),
                            profile_credential_release_key(profile.id),
                        )
                    profile.delete()
                for stream in created_streams:
                    stream.delete()
                for account in created_accounts:
                    account.delete()
                if channel is not None:
                    channel.delete()
                for channel_profile in created_channel_profiles:
                    channel_profile.delete()
            except Exception:
                cleanup_errors.append("synthetic-database-rows")
        if not cleanup_errors:
            shutil.rmtree(archive_root, ignore_errors=True)
        shutil.rmtree(fixture_root, ignore_errors=True)
        if cleanup_errors:
            details = ", ".join(sorted(set(cleanup_errors)))
            raise RuntimeError(
                f"Synthetic live/archive isolation cleanup was incomplete ({details})"
            )
