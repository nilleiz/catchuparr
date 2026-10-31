"""Native evidence for the return tail reader inside a real borrow return.

The media probe calls this inside its borrow-return scenario. While the borrow
waits for its return, the production :class:`NativeTailReader` opens on the
viewer channel and a consumer reads it as the piped FFmpeg input would. The
probe then requires that:

1. the tail client is classified private, so the borrow still returns at its
   native deadline;
2. opening it changes neither the sharing authority nor the operation
   pointer, so the dedicated attempt's RETURN acknowledgement stays valid;
3. after the borrowed client's exact cleanup, the native channel stays up for
   the tail alone, which keeps reading new chunks and reporting activity.

It does not test archive splicing, which the synthetic engine tests cover.
Dispatcharr modules are imported only inside the AIO.
"""

from __future__ import annotations

import threading
import time
import uuid

TAIL_PROBE_SECONDS = 180.0


def _text(value) -> str | None:
    if value is None:
        return None
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def recorder_stats_digest(redis_text, channel_uuid: str) -> str:
    """Return the Stats capability digest bound to the channel's current lease."""
    lease = _text(redis_text.get(f"catchuparr:recorder:{channel_uuid}"))
    _require(bool(lease), "Recorder lease is unavailable for the tail probe")
    prefix = "catchuparr:recorder:stats-cap:"
    matches = []
    for raw_key in redis_text.scan_iter(match=f"{prefix}*", count=100):
        key = _text(raw_key)
        if _text(redis_text.get(key)) == f"{channel_uuid}|{lease}":
            matches.append(key[len(prefix):])
    _require(len(matches) == 1, "Recorder Stats capability is not unique for the tail probe")
    return matches[0]


def authority_snapshot(redis_text, channel_uuid: str) -> tuple:
    """Capture the sharing authority and operation pointer the tail must not touch."""
    authority = redis_text.hgetall(f"catchuparr:sharing:current:{channel_uuid}") or {}
    operation = redis_text.get(f"catchuparr:sharing:operation:current:{channel_uuid}")
    return (
        tuple(sorted((_text(key), _text(value)) for key, value in authority.items())),
        _text(operation),
    )


class ReturnTailProbe:
    """The production tail reader on the borrowed channel, consumed in a thread."""

    def __init__(self, redis_text, redis_binary, channel_uuid: str, episode, *, clock=time.time):
        self.redis_text = redis_text
        self.redis_binary = redis_binary
        self.channel_uuid = channel_uuid
        self.episode = episode
        self.clock = clock
        self.reader = None
        self.consumer = None
        self.stop = threading.Event()
        self.bytes_read = 0
        self.consumer_error = None

    def _client_present(self) -> bool:
        if self.reader is None:
            return False
        channel, client = self.channel_uuid, self.reader.client_id
        return bool(
            self.redis_text.sismember(f"live:channel:{channel}:clients", client)
            and self.redis_text.exists(f"live:channel:{channel}:clients:{client}")
        )

    def _consume(self) -> None:
        try:
            for chunk in self.reader.iter_chunks(self.stop):
                self.bytes_read += len(chunk)
                self.reader.record_activity()
        except Exception as exc:  # The probe reports only the class.
            self.consumer_error = type(exc).__name__

    def start(self) -> None:
        from apps.proxy.live_proxy.client_manager import ClientManager
        from apps.proxy.live_proxy.input.buffer import StreamBuffer

        from catchuparr.adapters.sharing_native import _private_client
        from catchuparr.sharing_tail import NativeTailReader

        before = authority_snapshot(self.redis_text, self.channel_uuid)
        lease = _text(self.redis_text.get(f"catchuparr:recorder:{self.channel_uuid}"))
        self.reader = NativeTailReader(
            redis_text=self.redis_text, redis_binary=self.redis_binary,
            channel_uuid=self.channel_uuid, identity=tuple(self.episode.identity),
            configuration_generation=self.episode.configuration_generation,
            control_generation=self.episode.control_generation,
            recorder_fence=self.episode.recorder_fence, lease_value=lease,
            stats_digest=recorder_stats_digest(self.redis_text, self.channel_uuid),
            hard_deadline=self.clock() + TAIL_PROBE_SECONDS,
            generation_is_current=lambda: not self.stop.is_set(),
            tail_id=uuid.uuid4().hex,
        )
        self.reader.open(ClientManager, StreamBuffer)
        _require(self._client_present(), "Tail client was not registered natively")
        _require(_private_client(self.redis_text, self.channel_uuid, self.reader.client_id) is True,
                 "Tail client is not classified private")
        _require(authority_snapshot(self.redis_text, self.channel_uuid) == before,
                 "Opening the tail reader changed the sharing authority or operation")
        self.consumer = threading.Thread(target=self._consume, daemon=True)
        self.consumer.start()

    def require_alive(self, label: str) -> None:
        """The native channel still serves the tail after the borrow left."""
        from apps.proxy.live_proxy.redis_keys import RedisKeys

        _require(self.consumer_error is None and self.consumer is not None and self.consumer.is_alive(),
                 f"Tail reader stopped {label}: {self.consumer_error or 'ended'}")
        _require(self._client_present(), f"Tail client disappeared {label}")
        _require(self.redis_text.get(RedisKeys.channel_owner(self.channel_uuid)) is not None,
                 f"Native channel owner released with the tail client present {label}")
        _require(not self.redis_text.exists(RedisKeys.channel_stopping(self.channel_uuid)),
                 f"Native channel is stopping with the tail client present {label}")

    def require_reading(self, *, timeout: float = 15.0, sleep=time.sleep) -> None:
        """New chunks keep arriving after the borrow's cleanup."""
        deadline = self.clock() + timeout
        start = self.bytes_read
        while self.bytes_read <= start:
            _require(self.consumer_error is None, f"Tail reader failed: {self.consumer_error}")
            _require(self.clock() < deadline, "Tail reader received no chunk after the return")
            sleep(0.2)

    def close(self) -> bool:
        """Stop the consumer and remove the exact client; never stop the channel."""
        self.stop.set()
        if self.consumer is not None:
            self.consumer.join(10)
            self.consumer = None
        if self.reader is None:
            return True
        closed = self.reader.close()
        present = self._client_present()
        if closed and not present:
            self.reader = None
            return True
        return False
