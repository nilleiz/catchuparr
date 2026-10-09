"""Sanitized, test-only diagnostics for one synthetic native channel startup."""

from __future__ import annotations

import hashlib
import inspect
import sys
import threading
import time
from functools import partial

_MAX_EVENTS = 96
_NATIVE_MODULE_PREFIX = "apps.proxy.live_proxy"
_METADATA_WRITES = {"HSET", "HSETNX", "HMSET"}
_METADATA_REMOVALS = {"DEL", "UNLINK"}
_METADATA_FIELD_REMOVALS = {"HDEL"}
_METADATA_EXPIRIES = {"EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT"}
_METADATA_TTL_REMOVALS = {"PERSIST"}


def _text(value) -> str | None:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    return value if isinstance(value, str) else None


class NativeRedisInitDiagnostic:
    """Observe one synthetic worker's Redis init path without changing results."""

    def __init__(self, *, worker_id: str, redis_keys, native_server):
        self.worker_id = str(worker_id)
        self.metadata_key = redis_keys.channel_metadata(self.worker_id)
        self.owner_key = redis_keys.channel_owner(self.worker_id)
        self.clients_key = redis_keys.clients(self.worker_id)
        self.native_server = native_server
        self.started_at = time.monotonic()
        self._lock = threading.RLock()
        self._target_initialization_depth = 0
        self.events: list[dict] = []
        self._event_signatures: set[tuple] = set()
        self.omitted_events = 0
        self.instrumentation_errors = 0
        self._closed = False
        self._started = False
        self._patches = []
        self._clients: dict[int, dict] = {}
        self._patched_client_objects: set[int] = set()
        self._patched_classes: set[tuple[type, str]] = set()
        self._patched_ownership_objects: set[int] = set()

    def _record(
        self, event: str, *, actor: str = "probe", redis_role: str = "unknown",
        **details,
    ) -> None:
        try:
            with self._lock:
                signature = (
                    event,
                    actor,
                    redis_role,
                    tuple(sorted(details.items())),
                )
                if signature in self._event_signatures:
                    self.omitted_events += 1
                    return
                if len(self.events) >= _MAX_EVENTS:
                    self.omitted_events += 1
                    return
                item = {
                    "t_ms": max(0, int((time.monotonic() - self.started_at) * 1000)),
                    "event": event,
                    "actor": actor,
                    "redis_role": redis_role,
                }
                for key, value in details.items():
                    if isinstance(value, (bool, int, str)) or value is None:
                        item[key] = value
                self.events.append(item)
                self._event_signatures.add(signature)
        except Exception:
            self.instrumentation_errors += 1

    def _actor(self) -> str:
        try:
            frame = sys._getframe(2)
            for _ in range(10):
                module = frame.f_globals.get("__name__", "")
                if module.startswith(_NATIVE_MODULE_PREFIX):
                    leaf = module.rsplit(".", 1)[-1]
                    function = frame.f_code.co_name
                    return f"live_proxy.{leaf}.{function}"
                frame = frame.f_back
                if frame is None:
                    break
        except Exception:
            self.instrumentation_errors += 1
        return "native-or-other"

    def _is_target_key(self, value) -> bool:
        return _text(value) == self.metadata_key

    def _command_events(self, command, args, result=None) -> list[tuple[str, dict]]:
        try:
            name = _text(command)
            name = name.upper() if name else ""
            values = tuple(args)
            if not name:
                return []

            keys = values if name in _METADATA_REMOVALS else values[:1]
            matches_metadata = any(self._is_target_key(key) for key in keys)
            if matches_metadata:
                if name in _METADATA_WRITES:
                    return [("metadata_write", {})]
                if name in _METADATA_REMOVALS:
                    return [("metadata_delete", {})]
                if name in _METADATA_FIELD_REMOVALS:
                    return [("metadata_field_delete", {})]
                if name in _METADATA_EXPIRIES:
                    return [("metadata_expire", {})]
                if name in _METADATA_TTL_REMOVALS:
                    return [("metadata_ttl_removed", {})]
                if name == "EXISTS":
                    return [("metadata_exists", {"metadata_exists": bool(result)})]
                if name == "HGET" and len(values) > 1 and _text(values[1]) == "state":
                    return [("metadata_state_read", {
                        "metadata_state_present": bool(result),
                        "metadata_state_kind": type(result).__name__,
                    })]
                if name in {"HGETALL", "HMGET"}:
                    size = len(result) if hasattr(result, "__len__") else None
                    return [("metadata_hash_read", {"metadata_result_size": size})]

            first_key = _text(values[0]) if values else None
            if name == "GET" and first_key == self.owner_key:
                return [("owner_read", {"owner_present": bool(result)})]
            if name == "SCARD" and first_key == self.clients_key:
                count = result if isinstance(result, int) and not isinstance(result, bool) else None
                return [("client_count_read", {"client_count": count})]
        except Exception:
            self.instrumentation_errors += 1
        return []

    def _record_command_result(
        self, command, args, result, *, actor: str, redis_role: str,
        raised: bool = False,
    ) -> None:
        for event, details in self._command_events(command, args, result):
            if raised:
                event = f"{event}_raised"
                details = {}
            self._record(event, actor=actor, redis_role=redis_role, **details)

    def _client_db(self, client) -> int | None:
        try:
            pool = getattr(client, "connection_pool", None)
            kwargs = getattr(pool, "connection_kwargs", None)
            db = kwargs.get("db", 0) if isinstance(kwargs, dict) else None
            if isinstance(db, bool):
                return None
            if isinstance(db, int):
                return db
            if isinstance(db, str) and db.isdecimal():
                return int(db)
        except Exception:
            self.instrumentation_errors += 1
        return None

    def _server_fingerprint(self, client) -> str | None:
        try:
            info = client.info("server")
            run_id = info.get("run_id") if isinstance(info, dict) else None
            if isinstance(run_id, bytes):
                run_id = run_id.decode("ascii", errors="ignore")
            if not isinstance(run_id, str) or not run_id:
                return None
            return hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:16]
        except Exception:
            self.instrumentation_errors += 1
            return None

    def _patch_instance(self, obj, name: str, replacement_factory) -> bool:
        try:
            original = getattr(obj, name)
            had_local = name in vars(obj)
            old_local = vars(obj).get(name)
            replacement = replacement_factory(original)
            setattr(obj, name, replacement)
            self._patches.append(("instance", obj, name, had_local, old_local))
            return True
        except Exception:
            self.instrumentation_errors += 1
            return False

    def _pipeline_role(self, pipeline) -> str:
        try:
            pool = getattr(pipeline, "connection_pool", None)
            kwargs = getattr(pool, "connection_kwargs", None)
            if isinstance(kwargs, dict):
                decoded = kwargs.get("decode_responses")
                if isinstance(decoded, bool):
                    return "decoded" if decoded else "buffer"
        except Exception:
            self.instrumentation_errors += 1
        return "unknown"

    def _patch_pipeline(self, pipeline) -> None:
        """Patch the pipeline class, without retaining each pipeline instance."""

        def execute(original, current, *args, **kwargs):
            try:
                command_stack = list(getattr(current, "command_stack", ()))
            except Exception:
                command_stack = []
                self.instrumentation_errors += 1
            role = self._pipeline_role(current)
            try:
                result = original(current, *args, **kwargs)
            except BaseException:
                for command_args, _options in command_stack:
                    if command_args:
                        self._record_command_result(
                            command_args[0], command_args[1:], None,
                            actor=self._actor(), redis_role=role, raised=True,
                        )
                raise
            results = result if isinstance(result, (list, tuple)) else ()
            for index, (command_args, _options) in enumerate(command_stack):
                if command_args:
                    command_result = results[index] if index < len(results) else None
                    self._record_command_result(
                        command_args[0], command_args[1:], command_result,
                        actor=self._actor(), redis_role=role,
                    )
            return result

        self._patch_method(type(pipeline), "execute", execute)

    def _register_client(
        self, client, *, role: str = "unknown", prestart: bool = False,
    ) -> None:
        try:
            if client is None or not callable(getattr(client, "execute_command", None)):
                return
            client_id = id(client)
            record = self._clients.get(client_id)
            if record is None:
                record = {
                    "client": client,
                    "db": self._client_db(client),
                    "server_hash": self._server_fingerprint(client) if prestart else None,
                    "roles": {role},
                }
                self._clients[client_id] = record
            else:
                record["roles"].add(role)
                if prestart and record["server_hash"] is None:
                    record["server_hash"] = self._server_fingerprint(client)
            if client_id in self._patched_client_objects:
                return
            self._patched_client_objects.add(client_id)

            def wrap_execute_command(original):
                def execute_command(command, *args, **kwargs):
                    redis_role = ",".join(sorted(record["roles"]))
                    try:
                        result = original(command, *args, **kwargs)
                    except BaseException:
                        self._record_command_result(
                            command, args, None, actor=self._actor(),
                            redis_role=redis_role, raised=True,
                        )
                        raise
                    self._record_command_result(
                        command, args, result, actor=self._actor(),
                        redis_role=redis_role,
                    )
                    return result

                return execute_command

            def wrap_pipeline(original):
                def pipeline(*args, **kwargs):
                    result = original(*args, **kwargs)
                    self._patch_pipeline(result)
                    return result

                return pipeline

            self._patch_instance(client, "execute_command", wrap_execute_command)
            if callable(getattr(client, "pipeline", None)):
                self._patch_instance(client, "pipeline", wrap_pipeline)
        except Exception:
            self.instrumentation_errors += 1

    def _discover_clients(self, *roots, role: str = "discovered") -> None:
        seen: set[int] = set()
        pending = list(roots)
        for _depth in range(3):
            next_items = []
            for obj in pending[:64]:
                if obj is None or id(obj) in seen:
                    continue
                seen.add(id(obj))
                if callable(getattr(obj, "execute_command", None)):
                    self._register_client(obj, role=role, prestart=True)
                    continue
                if isinstance(obj, dict):
                    next_items.extend(list(obj.values())[:32])
                    continue
                try:
                    values = vars(obj).values()
                except Exception:
                    continue
                next_items.extend(list(values)[:32])
            pending = next_items

    def _patch_client_factory(self, redis_client_class, name: str, role: str) -> None:
        try:
            descriptor = inspect.getattr_static(redis_client_class, name)
            raw = descriptor.__func__ if isinstance(descriptor, (staticmethod, classmethod)) else descriptor
            if not callable(raw):
                return

            def traced(*args, **kwargs):
                client = raw(*args, **kwargs)
                self._register_client(client, role=role, prestart=False)
                return client

            if isinstance(descriptor, staticmethod):
                replacement = staticmethod(traced)
            elif isinstance(descriptor, classmethod):
                replacement = classmethod(traced)
            else:
                replacement = traced
            setattr(redis_client_class, name, replacement)
            self._patches.append(("class", redis_client_class, name, descriptor))
        except Exception:
            self.instrumentation_errors += 1

    def _patch_method(self, cls: type, name: str, callback) -> None:
        owner_cls = next((base for base in cls.__mro__ if name in vars(base)), None)
        if owner_cls is None:
            return
        key = (owner_cls, name)
        if key in self._patched_classes:
            return
        try:
            descriptor = vars(owner_cls)[name]
            raw = descriptor.__func__ if isinstance(descriptor, (staticmethod, classmethod)) else descriptor
            if not callable(raw):
                return

            def wrap(original):
                def traced(*args, **kwargs):
                    return callback(original, *args, **kwargs)

                return traced

            traced = wrap(raw)
            if isinstance(descriptor, staticmethod):
                replacement = staticmethod(traced)
            elif isinstance(descriptor, classmethod):
                replacement = classmethod(traced)
            else:
                replacement = traced
            setattr(owner_cls, name, replacement)
            self._patches.append(("class", owner_cls, name, descriptor))
            self._patched_classes.add(key)
        except Exception:
            self.instrumentation_errors += 1

    def _patch_initialize(self, proxy_server_class) -> None:
        def initialize(original, *args, **kwargs):
            try:
                bound = inspect.signature(original).bind_partial(*args, **kwargs)
                channel_id = bound.arguments.get("channel_id")
            except (TypeError, ValueError):
                channel_id = kwargs.get("channel_id")
            if str(channel_id) != self.worker_id:
                return original(*args, **kwargs)

            self._target_initialization_depth += 1
            self._record("channel_initialize_enter")
            try:
                result = original(*args, **kwargs)
            except BaseException:
                self._record("channel_initialize_raised")
                raise
            finally:
                self._target_initialization_depth -= 1
            self._record("channel_initialize_return")
            self._patch_add_client()
            return result

        self._patch_method(proxy_server_class, "initialize_channel", initialize)

    def _patch_add_client(self) -> None:
        try:
            managers = getattr(self.native_server, "client_managers", {})
            get_manager = getattr(managers, "get", None)
            manager = get_manager(self.worker_id) if callable(get_manager) else None
            if manager is None or not callable(getattr(manager, "add_client", None)):
                return
            def wrap_add_client(original):
                def add_client(*args, **kwargs):
                    self._record("client_add_enter")
                    try:
                        result = original(*args, **kwargs)
                    except BaseException:
                        self._record("client_add_raised")
                        raise
                    self._record("client_add_return")
                    return result

                return add_client

            self._patch_instance(manager, "add_client", wrap_add_client)
        except Exception:
            self.instrumentation_errors += 1

    def _patch_ownership_method(self, manager) -> None:
        if id(manager) in self._patched_ownership_objects:
            return
        self._patched_ownership_objects.add(id(manager))

        def wrap_evaluate(original):
            def evaluate(*args, **kwargs):
                redis_client = kwargs.get("redis_client")
                if redis_client is None and args:
                    # The patched instance method keeps its original bound
                    # method, so the first wrapper argument is the Redis client.
                    redis_client = args[0]
                self._register_client(
                    redis_client, role="native_buffer", prestart=False,
                )
                role = self._client_roles(redis_client)
                self._record("ownership_check_enter", redis_role=role)
                try:
                    result = original(*args, **kwargs)
                except BaseException:
                    self._record("ownership_check_raised", redis_role=role)
                    raise
                self._record(
                    "ownership_check_return",
                    redis_role=role,
                    result_kind=type(result).__name__,
                    ownership_allowed=result if isinstance(result, bool) else None,
                )
                return result

            return evaluate

        self._patch_instance(manager, "_evaluate_ownership_from_redis", wrap_evaluate)

    def _client_roles(self, client) -> str:
        record = self._clients.get(id(client))
        if record is None:
            return "unknown"
        return ",".join(sorted(record["roles"]))

    def _patch_thread_start(self) -> None:
        original = threading.Thread.start

        def start(thread, *args, **kwargs):
            target = getattr(thread, "_target", None)
            while isinstance(target, partial):
                target = target.func
            owner = getattr(target, "__self__", None)
            cls = type(owner) if owner is not None else None
            module = getattr(cls, "__module__", "") if cls is not None else ""
            if (
                cls is not None
                and module.startswith(_NATIVE_MODULE_PREFIX)
                and callable(getattr(cls, "_evaluate_ownership_from_redis", None))
                and self._target_initialization_depth > 0
            ):
                self._patch_ownership_method(owner)
                self._record("native_manager_thread_start_enter")
                try:
                    result = original(thread, *args, **kwargs)
                except BaseException:
                    self._record("native_manager_thread_start_raised")
                    raise
                self._record("native_manager_thread_started")
                return result
            return original(thread, *args, **kwargs)

        threading.Thread.start = start
        self._patches.append(("attribute", threading.Thread, "start", original))

    def start(self, *, redis_client, extra_objects=()) -> None:
        if self._started or self._closed:
            return
        self._started = True
        try:
            self._register_client(redis_client, role="decoded", prestart=True)
            self._discover_clients(*extra_objects, role="native_buffer")
            from apps.proxy.live_proxy.server import ProxyServer
            from core.utils import RedisClient

            self._register_client(
                getattr(RedisClient, "_client", None),
                role="decoded",
                prestart=True,
            )
            self._register_client(
                getattr(RedisClient, "_buffer", None),
                role="buffer",
                prestart=True,
            )
            self._patch_client_factory(RedisClient, "get_client", "decoded")
            self._patch_client_factory(RedisClient, "get_buffer", "buffer")
            self._patch_initialize(ProxyServer)
            self._patch_thread_start()
            self._record("diagnostic_started", client_count=len(self._clients))
        except Exception:
            self.instrumentation_errors += 1

    def _complete_client_fingerprints(self) -> list[dict]:
        summaries = []
        for record in self._clients.values():
            if record["server_hash"] is None:
                record["server_hash"] = self._server_fingerprint(record["client"])
            summaries.append({
                "db": record["db"],
                "server_hash": record["server_hash"],
                "roles": ",".join(sorted(record["roles"])),
            })
        return summaries

    def summary(self) -> dict:
        clients = self._complete_client_fingerprints()
        targets = {
            (item["db"], item["server_hash"])
            for item in clients
            if item["db"] is not None and item["server_hash"] is not None
        }
        target_match = (
            bool(clients)
            and len(targets) == 1
            and all(
                item["db"] is not None and item["server_hash"] is not None
                for item in clients
            )
        )
        with self._lock:
            events = list(self.events)
            omitted_events = self.omitted_events
            instrumentation_errors = self.instrumentation_errors
        names = {event["event"] for event in events}
        observed = {
            "channel_initialize": "channel_initialize_enter" in names,
            "metadata_write": "metadata_write" in names,
            "native_manager_thread_start": "native_manager_thread_start_enter" in names,
            "ownership_check": "ownership_check_enter" in names,
            "client_add": "client_add_enter" in names,
            "ownership_allowed": any(
                event.get("event") == "ownership_check_return"
                and event.get("ownership_allowed") is True
                for event in events
            ),
            "metadata_delete_or_expire": any(
                name.startswith(("metadata_delete", "metadata_expire"))
                for name in names
            ),
        }
        return {
            "scope": "in_process_clients_only",
            "events": events,
            "observed": observed,
            "omitted_events": omitted_events,
            "instrumentation_errors": instrumentation_errors,
            "redis_clients": clients,
            "redis_targets_match": target_match,
        }

    def close(self) -> dict:
        if self._closed:
            return self.summary()
        self._closed = True
        for patch in reversed(self._patches):
            try:
                kind, owner, name, *original = patch
                if kind == "instance":
                    had_local, old_local = original
                    if had_local:
                        setattr(owner, name, old_local)
                    else:
                        delattr(owner, name)
                elif kind == "class":
                    setattr(owner, name, original[0])
                else:
                    setattr(owner, name, original[0])
            except Exception:
                self.instrumentation_errors += 1
        self._patches.clear()
        return self.summary()
