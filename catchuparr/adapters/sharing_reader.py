"""Reader-only adapter primitives for an already-owned native TS connection.

These primitives never resolve a source or initialize, switch, acquire, or stop
a native channel. Runtime integration must supply a fresh proof callback that
binds the reader to the current native owner and recorder fences.
"""

from __future__ import annotations

import inspect
import threading
from dataclasses import dataclass
from typing import Any, Callable, Iterator

_TASK_LOCAL_BUFFER_MARKER = object()


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
        and _has_parameters(
            getattr(client_manager_type, "remove_client", None), {"client_id"}
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
    if (
        getattr(buffer, "redis_client", None) is not redis_client
        or not isinstance(getattr(buffer, "buffer_index_key", None), str)
        or not buffer.buffer_index_key
        or not callable(getattr(redis_client, "get", None))
    ):
        raise BorrowReaderUnavailable("native input buffer head lookup is unsupported")
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
                    self._verify()
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
        register_private_marker: Callable[[str], None] | None = None,
        remove_private_marker: Callable[[str], None] | None = None,
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
        ) or not _has_parameters(
            getattr(client_manager, "remove_client", None), {"client_id"}
        ):
            raise BorrowReaderUnavailable("native client manager lifecycle API is unsupported")
        if type(getattr(client_manager, "clients", None)) is not set:
            raise BorrowReaderUnavailable("native client manager has no auditable client set")
        if type(owns_client_manager) is not bool:
            raise ValueError("owns_client_manager must be boolean")
        self.client_manager = client_manager
        self.binding = binding
        self.proof_reader = proof_reader
        self.client_id = client_id
        self.client_ip = client_ip
        self.user_agent = user_agent
        self.owns_client_manager = owns_client_manager
        self.register_private_marker = register_private_marker
        self.remove_private_marker = remove_private_marker
        self.registered = False
        self._closed = False
        self.reader: ReadOnlyTsBufferReader | None = None

    def open(self, buffer: Any) -> ReadOnlyTsBufferReader:
        if self._closed or self.registered:
            raise BorrowReaderUnavailable("borrowed client registration is not reusable")

        def current_proof() -> BorrowReadProof:
            proof = self.proof_reader()
            if self.registered and self.client_id in proof.ordinary_viewer_ids:
                raise BorrowBindingLost("recorder client was counted as an ordinary viewer")
            return proof

        reader = ReadOnlyTsBufferReader(buffer, self.binding, current_proof)
        reader._verify()
        clients = self.client_manager.clients
        if self.client_id in clients:
            raise BorrowReaderUnavailable("borrowed client ID is already registered")
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
            if self.client_id in clients:
                try:
                    self.client_manager.remove_client(self.client_id)
                except Exception:
                    pass
            raise BorrowReaderUnavailable("native recorder client registration failed") from exc
        if (
            type(result) is bool
            or not isinstance(result, int)
            or result < 1
            or self.client_id not in clients
        ):
            if self.client_id in clients:
                self.client_manager.remove_client(self.client_id)
            raise BorrowReaderUnavailable("native recorder client could not be registered")
        self.registered = True
        self.reader = reader
        try:
            if self.register_private_marker is not None:
                self.register_private_marker(self.client_id)
            # Revalidate after registration before permitting the first byte.
            reader._verify()
        except Exception:
            self.close()
            raise
        return reader

    def close(self) -> None:
        if self._closed:
            return
        error = None
        client_removed = not self.registered or self.client_id not in self.client_manager.clients
        if self.registered and not client_removed:
            try:
                self.client_manager.remove_client(self.client_id)
                client_removed = self.client_id not in self.client_manager.clients
                if not client_removed:
                    raise RuntimeError("native client manager retained the recorder client")
            except Exception as exc:  # Continue marker and owned-manager cleanup.
                error = exc
        if self.registered and client_removed and self.remove_private_marker is not None:
            try:
                self.remove_private_marker(self.client_id)
            except Exception as exc:
                error = error or exc
        if self.owns_client_manager and client_removed:
            stop = getattr(self.client_manager, "stop", None)
            if callable(stop):
                try:
                    stop()
                except Exception as exc:
                    error = error or exc
        if error is not None:
            raise BorrowReaderUnavailable("borrowed recorder client cleanup was incomplete") from error
        self.registered = False
        self._closed = True
