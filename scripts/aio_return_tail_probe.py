"""Native evidence for the return tail reader's two Dispatcharr assumptions.

The media probe calls this inside its borrow-return scenario. While the borrow
waits for its return, a second task-local client registers on the viewer
channel with the running recorder's private Stats marker. The probe then
requires that:

1. the extra client is classified private, so the borrow still returns at its
   native deadline and the extra client blocks neither the return nor the
   classification of other clients;
2. registering it changes neither the sharing authority nor the operation
   pointer, so the dedicated attempt's RETURN acknowledgement stays valid;
3. after the borrowed client's exact cleanup, the native channel stays up for
   the extra client and its buffer keeps advancing with readable chunks.

It does not test the tail planner or archive splicing. Dispatcharr modules are
imported only inside the AIO.
"""

from __future__ import annotations

import threading
import time
import uuid

TAIL_USER_AGENT = "Catchuparr return tail probe"
ACTIVITY_INTERVAL_SECONDS = 1.0
# Refresh only an existing client hash; never recreate a removed client.
_TOUCH_EXISTING_CLIENT_LUA = """
if redis.call('EXISTS', KEYS[1]) == 1 then
  redis.call('HSET', KEYS[1], 'last_active', ARGV[1])
  return 1
end
return 0
"""


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


def buffer_head(redis_binary, buffer) -> int:
    raw = redis_binary.get(buffer.buffer_index_key)
    _require(raw is not None, "Native buffer index is unavailable to the tail probe")
    return int(_text(raw))


class ReturnTailProbe:
    """One private extra client on the borrowed channel, closed exactly once."""

    def __init__(self, redis_text, redis_binary, channel_uuid: str, *, clock=time.time):
        self.redis_text = redis_text
        self.redis_binary = redis_binary
        self.channel_uuid = channel_uuid
        self.clock = clock
        self.client_id = f"catchuparr-client-{uuid.uuid4().hex}"
        self.manager = None
        self.owned_manager = False
        self.buffer = None
        self.start_head = None
        self.marked = False
        self.registered = False
        self._stop_activity = threading.Event()
        self._activity = None

    def touch(self) -> bool:
        """Report reader activity as a reading client does, so it is no ghost."""
        key = f"live:channel:{self.channel_uuid}:clients:{self.client_id}"
        return self.redis_text.eval(_TOUCH_EXISTING_CLIENT_LUA, 1, key, str(self.clock())) == 1

    def _report_activity(self) -> None:
        while not self._stop_activity.wait(ACTIVITY_INTERVAL_SECONDS):
            try:
                if not self.touch():
                    return
            except Exception:
                return

    def _client_present(self) -> bool:
        channel, client = self.channel_uuid, self.client_id
        return bool(
            self.redis_text.sismember(f"live:channel:{channel}:clients", client)
            and self.redis_text.exists(f"live:channel:{channel}:clients:{client}")
        )

    def start(self) -> None:
        from apps.proxy.live_proxy.client_manager import ClientManager
        from apps.proxy.live_proxy.input.buffer import StreamBuffer

        from catchuparr.adapters.sharing_native import _private_client
        from catchuparr.adapters.sharing_reader import make_task_local_input_buffer
        from catchuparr.sharing_episode import select_task_local_client_manager
        from catchuparr.stats import recorder_client_registered

        before = authority_snapshot(self.redis_text, self.channel_uuid)
        digest = recorder_stats_digest(self.redis_text, self.channel_uuid)
        # Mark before registering, so no observation can count the client as a viewer.
        self.marked = recorder_client_registered(
            self.channel_uuid, self.client_id, digest, self.channel_uuid,
        )
        _require(self.marked, "Tail probe could not write its private recorder marker")
        self.manager, self.owned_manager = select_task_local_client_manager(
            None, ClientManager, channel_uuid=self.channel_uuid,
            redis_client=self.redis_text, worker_id=f"catchuparr-reader-{uuid.uuid4().hex}",
        )
        result = self.manager.add_client(
            self.client_id, "127.0.0.1", TAIL_USER_AGENT,
            user=None, output_format="mpegts", output_profile_id=None,
        )
        self.registered = True
        _require(type(result) is int and result >= 1 and self._client_present(),
                 "Tail probe client was not registered natively")
        # A marker refresh between marking and registering drops markers of
        # absent clients; mark again now that the client is present.
        _require(recorder_client_registered(self.channel_uuid, self.client_id, digest, self.channel_uuid),
                 "Tail probe could not refresh its private recorder marker")
        _require(_private_client(self.redis_text, self.channel_uuid, self.client_id) is True,
                 "Tail probe client is not classified private")
        _require(authority_snapshot(self.redis_text, self.channel_uuid) == before,
                 "Registering the tail client changed the sharing authority or operation")
        _require(self.touch(), "Tail probe could not report client activity")
        self._activity = threading.Thread(target=self._report_activity, daemon=True)
        self._activity.start()
        self.buffer = make_task_local_input_buffer(StreamBuffer, self.channel_uuid, self.redis_binary)
        self.start_head = buffer_head(self.redis_binary, self.buffer)

    def require_alive(self, label: str) -> None:
        """The native channel still serves the tail after the borrow left."""
        from apps.proxy.live_proxy.redis_keys import RedisKeys

        _require(self._client_present(), f"Tail client disappeared {label}")
        _require(self.redis_text.get(RedisKeys.channel_owner(self.channel_uuid)) is not None,
                 f"Native channel owner released with the tail client present {label}")
        _require(not self.redis_text.exists(RedisKeys.channel_stopping(self.channel_uuid)),
                 f"Native channel is stopping with the tail client present {label}")

    def require_reading(self, *, timeout: float = 15.0, sleep=time.sleep) -> None:
        """New chunks appear after the borrow's cleanup and are readable."""
        deadline = self.clock() + timeout
        head = buffer_head(self.redis_binary, self.buffer)
        while head <= self.start_head + 1:
            _require(self.clock() < deadline, "Native buffer stopped advancing for the tail client")
            sleep(0.2)
            head = buffer_head(self.redis_binary, self.buffer)
        chunks, _next = self.buffer.get_optimized_client_data(head - 1)
        _require(bool(chunks), "Tail client could not read a chunk written after the return")

    def close(self) -> bool:
        """Remove the exact client and marker; never stop the native channel."""
        from catchuparr.stats import recorder_client_removed

        self._stop_activity.set()
        if self._activity is not None:
            self._activity.join(5)
            self._activity = None
        if not (self.registered or self.marked):
            return True
        if self.registered and self.manager is not None:
            self.manager.remove_client(self.client_id)
            self.registered = False
        if self.owned_manager and self.manager is not None:
            self.manager.stop()
            self.owned_manager = False
        if self.marked:
            recorder_client_removed(self.channel_uuid, self.client_id)
            self.marked = False
        return not self._client_present()
