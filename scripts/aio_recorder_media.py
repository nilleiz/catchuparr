"""Synthetic native-route media acceptance probe for disposable AIO containers."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


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
                            time.sleep(0.05)
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


def _read_stream_iterator(iterator, *, minimum_bytes: int, timeout: float, close=None) -> bytes:
    chunks = []
    total = 0
    failures = []

    def consume() -> None:
        nonlocal total
        try:
            while total < minimum_bytes:
                chunk = next(iterator)
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
import sys
sys.path.insert(0, "/data/plugins")
sys.path.insert(0, "/tmp")
from django.test import Client
from apps.m3u.connection_pool import profile_connections_key
from core.utils import RedisClient
from catchuparr import runtime
from catchuparr.adapters.recorder_proxy import read_worker_record
from catchuparr.configuration import load_active_configuration
from catchuparr.recorder_proxy import (
    capability_binding_current,
    configuration_generation,
    verify_recorder_capability,
)
from aio_recorder_media import _read_stream_response, _verify_media_identity

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
    media = _read_stream_response(response, minimum_bytes=188 * 512, timeout=20)
    _verify_media_identity({ffprobe!r}, media)
finally:
    if response is not None:
        response.close()

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
    from apps.channels.models import Channel, ChannelStream, Stream
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
    channel = None
    original_assignments = None
    profile_baselines = {}
    media_baselines = {}
    cleanup_errors = []

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
            "channel_uuids": str(channel.uuid),
            "archive_root": str(root),
            "retention_hours": 1,
            "max_storage_gib": 1,
            "source_rules": '* | mode=include-only | m3u="Synthetic media source B"',
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
        parent_iterator = iter(response.streaming_content)
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

        continued_media = _read_stream_iterator(
            parent_iterator, minimum_bytes=188 * 512, timeout=20, close=response.close,
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
            except Exception:
                cleanup_errors.append("synthetic-database-rows")
        try:
            shutil.rmtree(fixture_root, ignore_errors=True)
        except Exception:
            cleanup_errors.append("fixture-files")
        if cleanup_errors:
            raise RuntimeError("Synthetic recorder media cleanup was incomplete")
