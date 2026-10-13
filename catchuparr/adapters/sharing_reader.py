"""Reader-only adapter primitives for an already-owned native TS connection.

These primitives never resolve a source or initialize, switch, acquire, or stop
a native channel. Runtime integration must supply a fresh proof callback that
binds the reader to the current native owner and recorder fences.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import math
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Mapping

_TASK_LOCAL_BUFFER_MARKER = object()


def redis_backend_signature(redis_client: Any) -> str:
    """Return an opaque signature for a supported Redis transport/database.

    The signature intentionally includes authentication and TLS configuration in
    the digest so the binary and text pools must reach the same backend. Raw
    credentials and TLS key material are never returned or logged. Decode mode
    and pool tuning are excluded because Dispatcharr deliberately uses separate
    pools for binary and text operations.
    """
    pool = getattr(redis_client, "connection_pool", None)
    kwargs = getattr(pool, "connection_kwargs", None)
    connection_class = getattr(pool, "connection_class", None)
    if not isinstance(kwargs, Mapping) or connection_class is None:
        raise BorrowReaderUnavailable("Redis transport identity is unsupported")
    class_name = getattr(connection_class, "__name__", "")
    module_name = getattr(connection_class, "__module__", "")
    if module_name != "redis.connection" or class_name not in {
        "Connection", "SSLConnection", "UnixDomainSocketConnection",
    }:
        raise BorrowReaderUnavailable("Redis transport type is unsupported")
    if type(kwargs.get("decode_responses")) is not bool:
        raise BorrowReaderUnavailable("Redis response decoding mode is unknown")
    if class_name == "UnixDomainSocketConnection":
        if not isinstance(kwargs.get("path"), str) or not kwargs["path"]:
            raise BorrowReaderUnavailable("Redis Unix socket identity is incomplete")
    elif (
        not isinstance(kwargs.get("host"), str)
        or not kwargs["host"]
        or type(kwargs.get("port")) is not int
        or not 1 <= kwargs["port"] <= 65535
    ):
        raise BorrowReaderUnavailable("Redis endpoint identity is incomplete")
    database = kwargs.get("db")
    if type(database) is not int or database < 0:
        raise BorrowReaderUnavailable("Redis database identity is incomplete")
    backend_fields = {
        "host", "port", "db", "username", "password", "path", "unix_socket_path",
        "ssl", "ssl_cert_reqs", "ssl_ca_certs", "ssl_ca_data", "ssl_ca_path",
        "ssl_keyfile", "ssl_certfile", "ssl_password", "ssl_check_hostname",
        "ssl_min_version", "ssl_max_version", "ssl_validate_ocsp",
        "ssl_validate_ocsp_stapled", "ssl_ciphers",
    }
    # These options may differ between the text and binary pools without
    # changing backend identity or authentication policy.
    harmless_tuning_fields = {
        "decode_responses", "encoding", "encoding_errors", "socket_timeout",
        "socket_connect_timeout", "socket_keepalive", "socket_keepalive_options",
        "retry_on_timeout", "retry_on_error", "health_check_interval", "client_name",
        "protocol",
    }
    allowed_fields = backend_fields | harmless_tuning_fields | {"credential_provider"}
    if set(kwargs) - allowed_fields:
        raise BorrowReaderUnavailable("Redis transport options are unsupported")
    if kwargs.get("credential_provider") is not None:
        # Dynamic credentials cannot be compared safely without invoking the
        # provider (which would be an external authentication side effect).
        raise BorrowReaderUnavailable("dynamic Redis credentials are unsupported")
    tls_options = backend_fields.intersection(kwargs) - {
        "host", "port", "db", "username", "password", "path", "unix_socket_path", "ssl",
    }
    if class_name != "SSLConnection" and any(kwargs.get(key) is not None for key in tls_options):
        raise BorrowReaderUnavailable("TLS options require a supported SSL transport")
    if class_name == "SSLConnection" and kwargs.get("ssl") is False:
        raise BorrowReaderUnavailable("Redis TLS configuration is inconsistent")
    material: dict[str, Any] = {"connection_class": class_name}
    for key in sorted(backend_fields):
        value = kwargs.get(key)
        if isinstance(value, bytes):
            value = {"bytes_sha256": hashlib.sha256(value).hexdigest()}
        elif value is not None and type(value) not in (str, int, float, bool):
            raise BorrowReaderUnavailable("Redis transport configuration is unsupported")
        material[key] = value
    return hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _validate_native_buffer_namespace(buffer: Any, channel_uuid: str) -> None:
    expected = {
        "channel_id": channel_uuid,
        "buffer_index_key": f"live:channel:{channel_uuid}:input:buffer:index",
        "buffer_prefix": f"live:channel:{channel_uuid}:input:buffer:chunk:",
        "chunk_timestamps_key": f"live:channel:{channel_uuid}:input:buffer:chunk_timestamps",
    }
    if any(getattr(buffer, name, None) != value for name, value in expected.items()):
        raise BorrowReaderUnavailable("native input buffer namespace is unsupported")


class BorrowBindingLost(RuntimeError):
    """The borrowed native identity or its recorder fence is no longer current."""


class BorrowReaderUnavailable(RuntimeError):
    """The pinned native reader/client API cannot safely serve this borrow."""


def _has_parameters(function: Any, required: set[str]) -> bool:
    if not callable(function):
        return False
    try:
        return required.issubset(inspect.signature(function).parameters)
    except (TypeError, ValueError):
        return False


def native_read_api_supported(buffer_type: Any, client_manager_type: Any) -> bool:
    """Check only the audited read and exact-client lifecycle APIs."""
    return (
        _has_parameters(buffer_type, {"channel_id", "redis_client"})
        and _has_parameters(
            getattr(buffer_type, "get_optimized_client_data", None), {"client_index"}
        )
        and _has_parameters(
            getattr(buffer_type, "find_oldest_available_chunk", None), {"client_index"}
        )
        and _has_parameters(
            client_manager_type, {"channel_id", "redis_client", "worker_id"}
        )
        and _has_parameters(
            getattr(client_manager_type, "add_client", None),
            {"client_id", "client_ip", "user_agent", "user", "output_format", "output_profile_id"},
        )
    )


def make_task_local_input_buffer(buffer_type: Any, channel_uuid: str, redis_client: Any):
    """Construct a private Redis-backed reader; never reuse/mutate a shared buffer."""
    if not isinstance(channel_uuid, str) or not channel_uuid or redis_client is None:
        raise BorrowReaderUnavailable("native input-buffer binding is incomplete")
    if not _has_parameters(buffer_type, {"channel_id", "redis_client"}):
        raise BorrowReaderUnavailable("native input-buffer constructor is unsupported")
    if not _has_parameters(
        getattr(buffer_type, "get_optimized_client_data", None), {"client_index"}
    ) or not _has_parameters(
        getattr(buffer_type, "find_oldest_available_chunk", None), {"client_index"}
    ):
        raise BorrowReaderUnavailable("native input-buffer reader API is unsupported")
    buffer = buffer_type(channel_id=channel_uuid, redis_client=redis_client)
    if type(getattr(buffer, "index", None)) is not int:
        raise BorrowReaderUnavailable("native input buffer has no valid read position")
    _validate_native_buffer_namespace(buffer, channel_uuid)
    if (
        getattr(buffer, "redis_client", None) is not redis_client
        or not callable(getattr(redis_client, "get", None))
    ):
        raise BorrowReaderUnavailable("native input buffer Redis client is unsupported")
    redis_backend_signature(redis_client)
    connection_kwargs = redis_client.connection_pool.connection_kwargs
    if connection_kwargs.get("decode_responses") is not False:
        raise BorrowReaderUnavailable("native input buffer must preserve binary Redis responses")
    chunk_size = getattr(buffer, "target_chunk_size", None)
    if type(chunk_size) is not int or chunk_size <= 0 or chunk_size % 188:
        raise BorrowReaderUnavailable("native input buffer does not preserve TS packet boundaries")
    buffer._catchuparr_task_local_reader_marker = _TASK_LOCAL_BUFFER_MARKER
    return buffer


@dataclass(frozen=True)
class BorrowReadBinding:
    """Opaque identity/fence expected for every reader proof."""

    connection_identity: tuple[str, ...]
    configuration_generation: str
    control_generation: int
    recorder_fence: int
    pending_return_deadline: float | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.connection_identity, tuple)
            or len(self.connection_identity) != 8
            or any(not isinstance(part, str) or not part for part in self.connection_identity)
        ):
            raise ValueError("connection_identity must contain eight non-empty identity fields")
        if not isinstance(self.configuration_generation, str) or not self.configuration_generation:
            raise ValueError("configuration_generation must be a non-empty string")
        if type(self.control_generation) is not int or self.control_generation < 0:
            raise ValueError("control_generation must be a non-negative integer")
        if type(self.recorder_fence) is not int or self.recorder_fence < 0:
            raise ValueError("recorder_fence must be a non-negative integer")
        if self.pending_return_deadline is not None:
            import math

            if (
                isinstance(self.pending_return_deadline, bool)
                or not isinstance(self.pending_return_deadline, (int, float))
                or not math.isfinite(float(self.pending_return_deadline))
            ):
                raise ValueError("pending_return_deadline must be a finite timestamp")


@dataclass(frozen=True)
class BorrowReadProof:
    """Sanitized, just-in-time native proof returned by the integration layer."""

    connection_identity: tuple[str, ...]
    configuration_generation: str
    control_generation: int
    recorder_fence: int
    ordinary_viewer_ids: tuple[str, ...]
    checked_at: float
    owner_matches: bool
    owner_heartbeat_healthy: bool
    native_active: bool
    assigned_source_matches: bool
    account_active: bool
    profile_active: bool
    media_compatible: bool
    useful_buffer_available: bool
    supported_worker_kind: bool
    borrowed_client_current: bool
    pending_return_deadline: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.connection_identity, tuple):
            raise ValueError("connection_identity must be a tuple")
        if not isinstance(self.configuration_generation, str) or not self.configuration_generation:
            raise ValueError("configuration_generation must be a non-empty string")
        if type(self.control_generation) is not int or self.control_generation < 0:
            raise ValueError("control_generation must be a non-negative integer")
        if type(self.recorder_fence) is not int or self.recorder_fence < 0:
            raise ValueError("recorder_fence must be a non-negative integer")
        if not isinstance(self.ordinary_viewer_ids, tuple) or any(
            not isinstance(value, str) or not value for value in self.ordinary_viewer_ids
        ):
            raise ValueError("ordinary_viewer_ids must contain non-empty strings")
        if len(set(self.ordinary_viewer_ids)) != len(self.ordinary_viewer_ids):
            raise ValueError("ordinary_viewer_ids must be unique")
        if isinstance(self.checked_at, bool) or not isinstance(self.checked_at, (int, float)):
            raise ValueError("checked_at must be a finite timestamp")
        import math

        if not math.isfinite(float(self.checked_at)):
            raise ValueError("checked_at must be a finite timestamp")
        if self.pending_return_deadline is not None and (
            isinstance(self.pending_return_deadline, bool)
            or not isinstance(self.pending_return_deadline, (int, float))
            or not math.isfinite(float(self.pending_return_deadline))
        ):
            raise ValueError("pending_return_deadline must be a finite timestamp")
        for field in (
            "owner_matches", "owner_heartbeat_healthy", "native_active",
            "assigned_source_matches", "account_active", "profile_active",
            "media_compatible", "useful_buffer_available", "supported_worker_kind",
            "borrowed_client_current",
        ):
            if type(getattr(self, field)) is not bool:
                raise ValueError(f"{field} must be boolean")


def validate_borrow_read_proof(binding: BorrowReadBinding, proof: BorrowReadProof) -> None:
    """Fail closed unless identity, ownership, policy, and media still match."""
    if not isinstance(proof, BorrowReadProof):
        raise BorrowBindingLost("native borrow proof is unavailable")
    if (
        proof.connection_identity != binding.connection_identity
        or proof.configuration_generation != binding.configuration_generation
        or proof.control_generation != binding.control_generation
        or proof.recorder_fence != binding.recorder_fence
    ):
        raise BorrowBindingLost("native connection or recorder fence changed")
    if not all((
        proof.owner_matches,
        proof.owner_heartbeat_healthy,
        proof.native_active,
        proof.assigned_source_matches,
        proof.account_active,
        proof.profile_active,
        proof.media_compatible,
        proof.useful_buffer_available,
        proof.supported_worker_kind,
    )):
        raise BorrowBindingLost("native connection is no longer safe to read")
    if proof.ordinary_viewer_ids:
        return
    deadline = binding.pending_return_deadline
    if (
        deadline is None
        or proof.pending_return_deadline != deadline
        or float(proof.checked_at) >= float(deadline)
    ):
        raise BorrowBindingLost("no ordinary viewer remains for this borrow")


_TOUCH_NATIVE_CLIENT_LUA = """
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then return 0 end
if redis.call('EXISTS', KEYS[2]) == 0 then return 0 end
if redis.call('EXISTS', KEYS[7]) ~= 0 then return 0 end
if redis.call('EXISTS', KEYS[8]) ~= 0 then return 0 end
if redis.call('GET', KEYS[3]) ~= ARGV[2] then return 0 end
if redis.call('HGET', KEYS[4], 'owner') ~= ARGV[3] then return 0 end
if redis.call('HGET', KEYS[4], 'stream_id') ~= ARGV[4] then return 0 end
if redis.call('HGET', KEYS[4], 'm3u_profile') ~= ARGV[13] then return 0 end
if redis.call('HGET', KEYS[4], 'stream_profile') ~= ARGV[14] then return 0 end
local state = redis.call('HGET', KEYS[4], 'state')
if state ~= 'active' and state ~= 'waiting_for_clients' then return 0 end
if redis.call('EXISTS', KEYS[5]) == 0 then return 0 end
if redis.call('GET', KEYS[6]) ~= ARGV[5] then return 0 end
if redis.call('HGET', KEYS[2], 'connected_at') ~= ARGV[6] then return 0 end
if redis.call('HGET', KEYS[2], 'worker_id') ~= ARGV[7] then return 0 end
if redis.call('HGET', KEYS[2], 'user_agent') ~= ARGV[8] then return 0 end
if redis.call('HGET', KEYS[2], 'ip_address') ~= ARGV[9] then return 0 end
if redis.call('HGET', KEYS[2], 'user_id') ~= ARGV[10] then return 0 end
if redis.call('HGET', KEYS[2], 'output_format') ~= ARGV[11] then return 0 end
if redis.call('HGET', KEYS[2], 'output_profile_id') ~= ARGV[12] then return 0 end
redis.call('HSET', KEYS[2], 'last_active', ARGV[15])
redis.call('EXPIRE', KEYS[2], ARGV[16])
redis.call('EXPIRE', KEYS[1], ARGV[16])
return 1
"""

_REMOVE_EXACT_NATIVE_CLIENT_LUA = """
local has_record = redis.call('EXISTS', KEYS[2])
local has_membership = redis.call('SISMEMBER', KEYS[1], ARGV[1])
if has_record == 0 then
  if has_membership == 0 then return 0 end
  -- The exact private client ID has no live record to replace. Removing only
  -- its orphaned membership cannot delete a valid replacement hash.
  redis.call('SREM', KEYS[1], ARGV[1])
  if redis.call('SCARD', KEYS[1]) == 0 then
    redis.call('SET', KEYS[3], ARGV[9], 'EX', ARGV[10])
  end
  return 1
end
if redis.call('HGET', KEYS[2], 'connected_at') ~= ARGV[2] then return 0 end
if redis.call('HGET', KEYS[2], 'worker_id') ~= ARGV[3] then return 0 end
if redis.call('HGET', KEYS[2], 'user_agent') ~= ARGV[4] then return 0 end
if redis.call('HGET', KEYS[2], 'ip_address') ~= ARGV[5] then return 0 end
if redis.call('HGET', KEYS[2], 'user_id') ~= ARGV[6] then return 0 end
if redis.call('HGET', KEYS[2], 'output_format') ~= ARGV[7] then return 0 end
if redis.call('HGET', KEYS[2], 'output_profile_id') ~= ARGV[8] then return 0 end
redis.call('SREM', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
local remaining = redis.call('SCARD', KEYS[1])
if remaining == 0 then
  redis.call('SET', KEYS[3], ARGV[9], 'EX', ARGV[10])
end
return 1
"""


class NativeClientActivityGuard:
    """CAS-update `last_active` for one existing, exactly bound native client.

    `generation_is_current` must reread the applied configuration, control
    generation, and recorder lease just before the atomic native Redis check.
    The Lua script only updates `last_active` and existing-key TTLs.
    """

    _CLIENT_FIELDS = {
        "connected_at", "worker_id", "user_agent", "ip_address", "user_id",
        "output_format", "output_profile_id",
    }

    def __init__(
        self,
        redis_client: Any,
        binding: BorrowReadBinding,
        *,
        recorder_lease_value: str,
        client_record_ttl: int,
        generation_is_current: Callable[[BorrowReadBinding], bool],
    ) -> None:
        if redis_client is None or not callable(getattr(redis_client, "eval", None)):
            raise BorrowReaderUnavailable("native Redis compare-and-set is unsupported")
        if not isinstance(recorder_lease_value, str) or not recorder_lease_value:
            raise ValueError("recorder_lease_value must be non-empty")
        if type(client_record_ttl) is not int or client_record_ttl < 1:
            raise ValueError("client_record_ttl must be a positive integer")
        if not callable(generation_is_current):
            raise ValueError("generation_is_current must be callable")
        self.redis = redis_client
        self.binding = binding
        self.recorder_lease_value = recorder_lease_value
        self.client_record_ttl = client_record_ttl
        self.generation_is_current = generation_is_current

    def __call__(
        self,
        binding: BorrowReadBinding,
        client_id: str,
        proof: BorrowReadProof,
        client_fields: Mapping[str, str],
    ) -> bool:
        if binding != self.binding:
            return False
        try:
            validate_borrow_read_proof(binding, proof)
        except BorrowBindingLost:
            return False
        if not proof.borrowed_client_current or not self.generation_is_current(binding):
            return False
        if not isinstance(client_fields, Mapping) or set(client_fields) != self._CLIENT_FIELDS:
            return False
        values = {key: client_fields[key] for key in self._CLIENT_FIELDS}
        if any(not isinstance(value, str) for value in values.values()) or any(
            not values[key]
            for key in self._CLIENT_FIELDS - {"output_profile_id"}
        ):
            return False
        channel_id, _worker_id, owner_value, metadata_owner, stream_id, *_ = binding.connection_identity
        expected_worker = values["worker_id"]
        client_set_key = f"live:channel:{channel_id}:clients"
        client_key = f"live:channel:{channel_id}:clients:{client_id}"
        owner_key = f"live:channel:{channel_id}:owner"
        channel_metadata_key = f"live:channel:{channel_id}:metadata"
        worker_heartbeat_key = f"live:worker:{metadata_owner}:heartbeat"
        recorder_lease_key = f"catchuparr:recorder:{channel_id}"
        client_stop_key = f"live:channel:{channel_id}:client:{client_id}:stop"
        channel_stopping_key = f"live:channel:{channel_id}:stopping"
        try:
            result = self.redis.eval(
                _TOUCH_NATIVE_CLIENT_LUA,
                8,
                client_set_key,
                client_key,
                owner_key,
                channel_metadata_key,
                worker_heartbeat_key,
                recorder_lease_key,
                client_stop_key,
                channel_stopping_key,
                client_id,
                owner_value,
                metadata_owner,
                stream_id,
                self.recorder_lease_value,
                values["connected_at"],
                expected_worker,
                values["user_agent"],
                values["ip_address"],
                values["user_id"],
                values["output_format"],
                values["output_profile_id"],
                binding.connection_identity[6],
                binding.connection_identity[7],
                str(float(proof.checked_at)),
                self.client_record_ttl,
            )
        except Exception:
            return False
        return type(result) is int and result == 1


class ReadOnlyTsBufferReader:
    """Read new TS chunks with a private native buffer cursor and no generator."""

    def __init__(
        self,
        buffer: Any,
        binding: BorrowReadBinding,
        proof_reader: Callable[[], BorrowReadProof],
        *,
        poll_interval: float = 0.1,
    ) -> None:
        if type(getattr(buffer, "index", None)) is not int:
            raise BorrowReaderUnavailable("native input buffer has no valid read position")
        _validate_native_buffer_namespace(buffer, binding.connection_identity[0])
        if (
            getattr(buffer, "_catchuparr_task_local_reader_marker", None)
            is not _TASK_LOCAL_BUFFER_MARKER
        ):
            raise BorrowReaderUnavailable("shared native buffers cannot be used by a recorder reader")
        if not callable(getattr(buffer, "get_optimized_client_data", None)) or not callable(
            getattr(buffer, "find_oldest_available_chunk", None)
        ):
            raise BorrowReaderUnavailable("native input buffer reader API is unsupported")
        if not callable(proof_reader):
            raise BorrowReaderUnavailable("native borrow proof callback is missing")
        redis_client = getattr(buffer, "redis_client", None)
        redis_backend_signature(redis_client)
        if redis_client.connection_pool.connection_kwargs.get("decode_responses") is not False:
            raise BorrowReaderUnavailable("native input buffer must preserve binary Redis responses")
        if (
            isinstance(poll_interval, bool)
            or not isinstance(poll_interval, (int, float))
            or poll_interval <= 0
        ):
            raise ValueError("poll_interval must be positive")
        self.buffer = buffer
        self.binding = binding
        self.proof_reader = proof_reader
        self.poll_interval = float(poll_interval)
        # The private cursor starts at the current head so a new recording begins live.
        self.local_index = buffer.index

    def _refresh_private_head(self) -> int:
        """Refresh only this task-local cursor from the native Redis index key."""
        redis_client = self.buffer.redis_client
        key = self.buffer.buffer_index_key
        try:
            value = redis_client.get(key)
            head = int(value or 0)
        except Exception as exc:
            raise BorrowReaderUnavailable("native input buffer head could not be read") from exc
        if head < 0:
            raise BorrowReaderUnavailable("native input buffer returned an invalid head")
        if head > self.buffer.index:
            # This object is private to this recorder. The proxy's shared input
            # buffer is never passed here or updated by the adapter.
            self.buffer.index = head
        return self.buffer.index

    def _verify(self) -> None:
        try:
            proof = self.proof_reader()
            validate_borrow_read_proof(self.binding, proof)
        except BorrowBindingLost:
            raise
        except Exception as exc:
            raise BorrowBindingLost("native borrow proof could not be refreshed") from exc

    def iter_chunks(self, stop: threading.Event | None = None) -> Iterator[bytes]:
        """Yield aligned TS chunks while the same current native identity is proven."""
        stop = stop or threading.Event()
        while not stop.is_set():
            self._verify()
            if stop.is_set():
                return
            self._refresh_private_head()
            chunks, next_index = self.buffer.get_optimized_client_data(self.local_index)
            if type(next_index) is not int or next_index < self.local_index:
                raise BorrowReaderUnavailable("native buffer returned an invalid read position")
            if chunks:
                prepared = []
                for chunk in chunks:
                    if not isinstance(chunk, (bytes, bytearray, memoryview)):
                        raise BorrowReaderUnavailable("native buffer returned a non-byte chunk")
                    data = bytes(chunk)
                    if not data or len(data) % 188:
                        raise BorrowReaderUnavailable("native buffer returned unaligned TS data")
                    prepared.append(data)
                # A source switch between Redis read and publication invalidates
                # the batch; none of it is allowed to escape the adapter.
                self.local_index = next_index
                for chunk in prepared:
                    if stop.is_set():
                        return
                    self._verify()
                    if stop.is_set():
                        return
                    yield chunk
                continue

            head = getattr(self.buffer, "index", self.local_index)
            if type(head) is int and head > self.local_index:
                oldest = self.buffer.find_oldest_available_chunk(self.local_index)
                if type(oldest) is int and oldest > self.local_index:
                    self.local_index = oldest
                    continue
            stop.wait(self.poll_interval)


class BorrowedClientRegistration:
    """Register/remove only this recorder's native client; never stop shared managers."""

    def __init__(
        self,
        client_manager: Any,
        binding: BorrowReadBinding,
        proof_reader: Callable[[], BorrowReadProof],
        *,
        client_id: str,
        client_ip: str,
        user_agent: str,
        owns_client_manager: bool = False,
        register_private_marker: Callable[[str, str], bool] | None = None,
        remove_private_marker: Callable[[str, str], bool] | None = None,
        activity_writer: Callable[
            [BorrowReadBinding, str, BorrowReadProof, Mapping[str, str]], bool
        ] | None = None,
        cleanup_clock: Callable[[], float] = time.time,
    ) -> None:
        if not isinstance(client_id, str) or not client_id.startswith("catchuparr-client-"):
            raise ValueError("borrowed client ID must use the private recorder prefix")
        if not isinstance(client_ip, str) or not client_ip:
            raise ValueError("client_ip must be supplied by the serving request")
        if not isinstance(user_agent, str) or not user_agent:
            raise ValueError("user_agent must be supplied by the serving request")
        if not _has_parameters(
            getattr(client_manager, "add_client", None),
            {"client_id", "client_ip", "user_agent", "user", "output_format", "output_profile_id"},
        ):
            raise BorrowReaderUnavailable("native client manager registration API is unsupported")
        if type(getattr(client_manager, "clients", None)) is not set:
            raise BorrowReaderUnavailable("native client manager has no auditable client set")
        if (
            not hasattr(client_manager, "lock")
            or type(getattr(client_manager, "_registered_clients", None)) is not set
            or not isinstance(getattr(client_manager, "last_heartbeat_time", None), dict)
        ):
            raise BorrowReaderUnavailable("native client manager registration state is unsupported")
        channel_uuid = binding.connection_identity[0]
        if (
            str(getattr(client_manager, "channel_id", "")) != channel_uuid
            or getattr(client_manager, "client_set_key", None)
            != f"live:channel:{channel_uuid}:clients"
            or getattr(client_manager, "redis_client", None) is None
            or not callable(getattr(client_manager.redis_client, "sismember", None))
            or not callable(getattr(client_manager.redis_client, "exists", None))
            or not callable(getattr(client_manager.redis_client, "hgetall", None))
        ):
            raise BorrowReaderUnavailable("native client manager namespace is unsupported")
        redis_backend_signature(client_manager.redis_client)
        if client_manager.redis_client.connection_pool.connection_kwargs.get("decode_responses") is not True:
            raise BorrowReaderUnavailable("native client manager must use text Redis responses")
        if type(owns_client_manager) is not bool:
            raise ValueError("owns_client_manager must be boolean")
        if not callable(activity_writer):
            raise BorrowReaderUnavailable("guarded recorder activity writer is missing")
        if not callable(cleanup_clock):
            raise ValueError("cleanup_clock must be callable")
        self.client_manager = client_manager
        self.binding = binding
        self.proof_reader = proof_reader
        self.client_id = client_id
        self.client_ip = client_ip
        self.user_agent = user_agent
        self.owns_client_manager = owns_client_manager
        self.register_private_marker = register_private_marker
        self.remove_private_marker = remove_private_marker
        self.activity_writer = activity_writer
        self.cleanup_clock = cleanup_clock
        self.registered = False
        self._closed = False
        self._attempted = False
        self._marker_attempted = False
        self._client_fields: dict[str, str] | None = None
        self._registration_attempted = False
        self._replacement_present = False
        self._last_activity_at: float | None = None
        self.reader: ReadOnlyTsBufferReader | None = None

    @staticmethod
    def _text(value: Any) -> str:
        return value.decode("utf-8", errors="strict") if isinstance(value, bytes) else str(value)

    def _client_keys(self) -> tuple[str, str]:
        channel_id = self.binding.connection_identity[0]
        set_key = self.client_manager.client_set_key
        metadata_key = f"live:channel:{channel_id}:clients:{self.client_id}"
        return set_key, metadata_key

    def _client_records_exist(self) -> bool:
        set_key, metadata_key = self._client_keys()
        redis = self.client_manager.redis_client
        return bool(
            self.client_id in self.client_manager.clients
            or redis.sismember(set_key, self.client_id)
            or redis.exists(metadata_key)
        )

    def _client_registration_current(self) -> bool:
        if not self.registered or self._client_fields is None:
            return False
        set_key, metadata_key = self._client_keys()
        redis = self.client_manager.redis_client
        if (
            self.client_id not in self.client_manager.clients
            or not redis.sismember(set_key, self.client_id)
            or not redis.exists(metadata_key)
        ):
            return False
        raw = redis.hgetall(metadata_key)
        fields = {self._text(key): self._text(value) for key, value in raw.items()}
        return all(fields.get(key) == value for key, value in self._client_fields.items())

    def _capture_client_fields(self) -> dict[str, str] | None:
        _, metadata_key = self._client_keys()
        raw = self.client_manager.redis_client.hgetall(metadata_key)
        fields = {self._text(key): self._text(value) for key, value in raw.items()}
        expected = {
            "user_agent": self.user_agent,
            "ip_address": self.client_ip,
            "worker_id": str(getattr(self.client_manager, "worker_id", None) or "unknown"),
            "user_id": "0",
            "output_format": "mpegts",
            "output_profile_id": "",
        }
        if not fields.get("connected_at") or not all(fields.get(k) == v for k, v in expected.items()):
            return None
        return expected | {"connected_at": fields["connected_at"]}

    def _remove_exact_client(self) -> bool:
        if self._client_fields is None:
            # No complete registration identity means no safe deletion is possible.
            return not self._client_records_exist()
        channel_id = self.binding.connection_identity[0]
        set_key, metadata_key = self._client_keys()
        disconnect_key = f"live:channel:{channel_id}:last_client_disconnect_time"
        fields = self._client_fields
        cleanup_time = self.cleanup_clock()
        if isinstance(cleanup_time, bool) or not isinstance(cleanup_time, (int, float)) or not math.isfinite(float(cleanup_time)):
            raise RuntimeError("local cleanup clock returned an invalid timestamp")
        result = self.client_manager.redis_client.eval(
            _REMOVE_EXACT_NATIVE_CLIENT_LUA,
            3,
            set_key,
            metadata_key,
            disconnect_key,
            self.client_id,
            fields["connected_at"],
            fields["worker_id"],
            fields["user_agent"],
            fields["ip_address"],
            fields["user_id"],
            fields["output_format"],
            fields["output_profile_id"],
            str(float(cleanup_time)),
            int(getattr(self.client_manager, "client_ttl", 60)),
        )
        if type(result) is not int or result not in (0, 1):
            raise RuntimeError("exact native client cleanup returned an invalid result")
        with self.client_manager.lock:
            if result == 1:
                self.client_manager.clients.discard(self.client_id)
                self.client_manager._registered_clients.discard(self.client_id)
                self.client_manager.last_heartbeat_time.pop(self.client_id, None)
            present = self.client_id in self.client_manager.clients
        raw = self.client_manager.redis_client.hgetall(metadata_key)
        current = {self._text(k): self._text(v) for k, v in raw.items()}
        # A replacement generation is not ours and must remain untouched.
        replacement = bool(current) and current.get("connected_at") != fields["connected_at"]
        self._replacement_present = replacement
        redis_own_record = bool(current) and all(current.get(k) == v for k, v in fields.items())
        member = bool(self.client_manager.redis_client.sismember(set_key, self.client_id))
        if replacement:
            return True
        return not present and not redis_own_record and not (member and not current)

    def open(self, buffer: Any) -> ReadOnlyTsBufferReader:
        if self._closed or self.registered:
            raise BorrowReaderUnavailable("borrowed client registration is not reusable")

        def current_proof() -> BorrowReadProof:
            proof = self.proof_reader()
            if self.registered and (
                not proof.borrowed_client_current
                or not self._client_registration_current()
            ):
                raise BorrowBindingLost("borrowed recorder client is no longer registered")
            if self.registered and self.client_id in proof.ordinary_viewer_ids:
                raise BorrowBindingLost("recorder client was counted as an ordinary viewer")
            return proof

        _validate_native_buffer_namespace(buffer, self.binding.connection_identity[0])
        buffer_signature = redis_backend_signature(getattr(buffer, "redis_client", None))
        manager_signature = redis_backend_signature(self.client_manager.redis_client)
        if (
            buffer_signature != manager_signature
            or buffer.redis_client.connection_pool.connection_kwargs.get("decode_responses") is not False
            or self.client_manager.redis_client.connection_pool.connection_kwargs.get("decode_responses") is not True
        ):
            raise BorrowReaderUnavailable("reader buffer does not match the native manager namespace")
        reader = ReadOnlyTsBufferReader(buffer, self.binding, current_proof)
        reader._verify()
        clients = self.client_manager.clients
        if self._client_records_exist():
            raise BorrowReaderUnavailable("borrowed client ID is already registered")
        self._attempted = True
        self._registration_attempted = True
        try:
            result = self.client_manager.add_client(
                self.client_id,
                self.client_ip,
                self.user_agent,
                user=None,
                output_format="mpegts",
                output_profile_id=None,
            )
        except Exception as exc:
            self._client_fields = self._capture_client_fields()
            try:
                self.close()
            except Exception as cleanup_error:
                raise BorrowReaderUnavailable(
                    "native recorder registration failed and cleanup remains pending"
                ) from cleanup_error
            raise BorrowReaderUnavailable("native recorder client registration failed") from exc
        # Capture exact generation immediately, including partial native adds.
        self._client_fields = self._capture_client_fields()
        if (
            type(result) is bool
            or not isinstance(result, int)
            or result < 1
            or self.client_id not in clients
        ):
            try:
                self.close()
            except Exception as cleanup_error:
                raise BorrowReaderUnavailable(
                    "native recorder registration was rejected and cleanup remains pending"
                ) from cleanup_error
            raise BorrowReaderUnavailable("native recorder client could not be registered")
        if self._client_fields is None:
            try:
                self.close()
            except Exception as cleanup_error:
                raise BorrowReaderUnavailable(
                    "native recorder client identity is incomplete and cleanup remains pending"
                ) from cleanup_error
            raise BorrowReaderUnavailable("native recorder client identity could not be verified")
        self.registered = True
        self.reader = reader
        try:
            if self.register_private_marker is not None:
                self._marker_attempted = True
                if self.register_private_marker(self.client_id, self._client_fields["connected_at"]) is not True:
                    raise BorrowReaderUnavailable("private recorder marker could not be registered")
            # Revalidate after registration before permitting the first byte.
            reader._verify()
        except Exception:
            self.close()
            raise
        return reader

    def record_published_activity(self) -> None:
        """Refresh native `last_active` only after the consumer published bytes.

        The activity writer must use an atomic compare-and-set over the exact
        native client, channel owner, and recorder lease. It may only update
        `last_active` and the existing client/set TTLs; it must never create a
        missing registration or write URL/token material.
        """
        if not self.registered or self.reader is None or self._closed:
            raise BorrowBindingLost("borrowed recorder client is not active")
        self.reader._verify()
        proof = self.proof_reader()
        if not proof.borrowed_client_current or not self._client_registration_current():
            raise BorrowBindingLost("borrowed recorder client disappeared before activity refresh")
        if self._client_fields is None:
            raise BorrowBindingLost("borrowed recorder client identity was not captured")
        now = float(proof.checked_at)
        if (
            self._last_activity_at is not None
            and self._last_activity_at <= now < self._last_activity_at + 1.0
        ):
            return
        if self.activity_writer(
            self.binding, self.client_id, proof, self._client_fields
        ) is not True:
            raise BorrowBindingLost("fenced native recorder activity refresh was rejected")
        self._last_activity_at = now

    def close(self) -> None:
        if self._closed:
            return
        error = None
        client_removed = not self._registration_attempted
        if self._registration_attempted:
            try:
                client_removed = self._remove_exact_client()
                if not client_removed:
                    raise RuntimeError("exact native client generation remains or cannot be identified")
            except Exception as exc:  # Continue marker and owned-manager cleanup.
                error = exc
        marker_removed = not self._marker_attempted
        if self._marker_attempted and client_removed and self.remove_private_marker is not None:
            try:
                removed = self.remove_private_marker(
                    self.client_id,
                    self._client_fields["connected_at"] if self._client_fields else "",
                )
                marker_removed = removed is True
                if not marker_removed:
                    raise RuntimeError("private recorder marker remains registered")
            except Exception as exc:
                error = error or exc
        if self.owns_client_manager and client_removed and marker_removed and not self._replacement_present:
            stop = getattr(self.client_manager, "stop", None)
            if callable(stop):
                try:
                    stop()
                except Exception as exc:
                    error = error or exc
        if error is not None or not client_removed or not marker_removed:
            raise BorrowReaderUnavailable("borrowed recorder client cleanup was incomplete") from error
        self.registered = False
        self._closed = True
