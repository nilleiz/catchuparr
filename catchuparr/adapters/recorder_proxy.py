"""Namespaced Dispatcharr live-proxy workers for source-policy recordings.

The adapter deliberately uses the live proxy's supported worker, URL resolver,
connection-pool, and TS generator APIs while keeping all recorder state under a
Catchuparr-owned worker ID. It never calls ``Stream.get_stream`` and therefore
never writes Dispatcharr's unscoped ``channel_stream:<stream_id>`` keys.
"""

from __future__ import annotations

import hashlib
import importlib
import inspect
import logging
import re
import time
import uuid
from typing import Any

logger = logging.getLogger(__name__)

PLUGIN_REDIS_PREFIX = "catchuparr:recorder-proxy:"
WORKER_ID_PREFIX = "catchuparr-r"
WORKER_ID_RE = re.compile(r"^catchuparr-r[0-9a-f]{40}$")
WORKER_RECORD_TTL = 30 * 24 * 60 * 60
_SOURCE_URL_RE = re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://[^\s'\"]+")

_ACTIVE_WORKER_STATES = frozenset(
    {
        "issued", "reserving", "reserved", "starting", "active", "connecting",
        "initializing", "releasing", "release_failed",
    }
)
_RELEASE_RESERVATION = """
local state = redis.call('HGET', KEYS[1], 'reservation_state')
if state ~= 'reserved' then return 0 end
redis.call('HSET', KEYS[1], 'reservation_state', 'releasing', 'state', 'releasing')
return 1
"""


def worker_id_key(worker_id: str) -> str:
    return f"{PLUGIN_REDIS_PREFIX}worker:{worker_id}"


def worker_open_lock_key(worker_id: str) -> str:
    return f"{PLUGIN_REDIS_PREFIX}open:{worker_id}"


def reservation_credential_marker_key(reservation_id: str) -> str:
    return f"{PLUGIN_REDIS_PREFIX}credential:{reservation_id}"


def is_managed_worker_id(worker_id: Any) -> bool:
    return isinstance(worker_id, str) and WORKER_ID_RE.fullmatch(worker_id) is not None


def make_worker_id(
    channel_uuid: str,
    stream_id: str,
    config_generation: str,
    lease_fence: int,
    attempt_id: str,
) -> str:
    seed = "|".join(
        (str(channel_uuid), str(stream_id), str(config_generation), str(lease_fence), str(attempt_id))
    )
    return WORKER_ID_PREFIX + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:40]


def _decode(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


def read_worker_record(redis_client, worker_id: str) -> dict[str, str] | None:
    if not is_managed_worker_id(worker_id):
        return None
    raw = redis_client.hgetall(worker_id_key(worker_id))
    if not raw:
        return None
    try:
        return {str(_decode(key)): str(_decode(value)) for key, value in raw.items()}
    except Exception:
        logger.error("Invalid Catchuparr recorder worker ledger for %s", worker_id)
        return None


def write_worker_record(redis_client, worker_id: str, values: dict[str, Any]) -> None:
    if not is_managed_worker_id(worker_id):
        raise ValueError("invalid Catchuparr recorder worker ID")
    mapping = {str(key): str(value) for key, value in values.items() if value is not None}
    if mapping:
        redis_client.hset(worker_id_key(worker_id), mapping=mapping)


class _CredentialMarkerRedisFacade:
    """Remap only the core per-profile release marker to one reservation ID."""

    def __init__(self, redis_client, profile_id: int, reservation_id: str):
        self._redis = redis_client
        self._profile_id = int(profile_id)
        self._marker = reservation_credential_marker_key(reservation_id)
        self._pipeline_seen = False
        self._pipeline_committed = False

    def _key(self, key):
        from apps.m3u.connection_pool import profile_credential_release_key

        if _decode(key) == profile_credential_release_key(self._profile_id):
            return self._marker
        return key

    def get(self, key, *args, **kwargs):
        return self._redis.get(self._key(key), *args, **kwargs)

    def set(self, key, *args, **kwargs):
        return self._redis.set(self._key(key), *args, **kwargs)

    def delete(self, *keys):
        return self._redis.delete(*(self._key(key) for key in keys))

    def pipeline(self, *args, **kwargs):
        self._pipeline_seen = True
        return _CredentialMarkerPipelineFacade(
            self._redis.pipeline(*args, **kwargs), self
        )

    def __getattr__(self, name):
        return getattr(self._redis, name)


class _CredentialMarkerPipelineFacade:
    """Remap profile release marker keys used inside core Redis transactions."""

    def __init__(self, pipeline, redis_facade: _CredentialMarkerRedisFacade):
        self._pipeline = pipeline
        self._redis_facade = redis_facade

    def __enter__(self):
        entered = self._pipeline.__enter__()
        if entered is not None:
            self._pipeline = entered
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self._pipeline.__exit__(exc_type, exc_value, traceback)

    def watch(self, *keys):
        return self._pipeline.watch(*(self._redis_facade._key(key) for key in keys))

    def get(self, key):
        return self._pipeline.get(self._redis_facade._key(key))

    def delete(self, *keys):
        remapped = []
        for key in keys:
            remapped_key = self._redis_facade._key(key)
            remapped.append(remapped_key)
            if remapped_key != key:
                # Dispatcharr 0.32 keeps its shared marker while profile slots
                # remain. Its helper calls delete only for the final slot, so
                # remove the now-stale shared marker together with our marker.
                remapped.append(key)
        return self._pipeline.delete(*remapped)

    def execute(self):
        result = self._pipeline.execute()
        self._redis_facade._pipeline_committed = True
        return result

    def __getattr__(self, name):
        return getattr(self._pipeline, name)


def _release_worker_reservation(redis_client, worker_id: str) -> bool:
    """Claim and release this worker's exact core profile reservation once."""
    record = read_worker_record(redis_client, worker_id)
    if not record or record.get("reservation_state") != "reserved":
        return False
    profile_id = record.get("profile_id")
    reservation_id = record.get("reservation_id")
    if not profile_id or not reservation_id:
        return False
    claimed = redis_client.eval(
        _RELEASE_RESERVATION, 1, worker_id_key(worker_id)
    )
    if not claimed:
        return False
    try:
        _release_core_profile_slot(redis_client, int(profile_id), reservation_id)
    except Exception:
        # Do not retry a partially applied core release: its helper is not
        # reservation-ID aware. Leaving a capacity leak is safer than stealing
        # another live worker's profile count on duplicate teardown.
        redis_client.hset(
            worker_id_key(worker_id),
            mapping={"reservation_state": "release_failed", "state": "release_failed"},
        )
        logger.exception("Could not release Catchuparr recorder reservation for %s", worker_id)
        return False
    redis_client.hset(
        worker_id_key(worker_id),
        mapping={"reservation_state": "released", "state": "released", "released_at": str(time.time())},
    )
    redis_client.expire(worker_id_key(worker_id), WORKER_RECORD_TTL)
    return True


def _release_core_profile_slot(redis_client, profile_id: int, reservation_id: str) -> None:
    """Release a reservation and remove its marker only after a confirmed commit."""
    from apps.m3u.connection_pool import release_profile_slot

    marker_facade = _CredentialMarkerRedisFacade(
        redis_client, int(profile_id), reservation_id
    )
    release_profile_slot(int(profile_id), marker_facade)
    if marker_facade._pipeline_seen and not marker_facade._pipeline_committed:
        # Dispatcharr 0.32 returns normally after exhausting its WatchError
        # retries. Preserve the reservation marker so this uncertain release
        # cannot be mistaken for a committed counter decrement.
        raise RuntimeError("Dispatcharr profile release did not commit a Redis transaction")
    redis_client.delete(reservation_credential_marker_key(reservation_id))


def _clear_worker_reservation_metadata(redis_client, worker_id: str) -> None:
    try:
        from apps.proxy.live_proxy.constants import ChannelMetadataField
        from apps.proxy.live_proxy.redis_keys import RedisKeys

        metadata_key = RedisKeys.channel_metadata(worker_id)
        redis_client.hdel(
            metadata_key,
            ChannelMetadataField.STREAM_ID,
            ChannelMetadataField.M3U_PROFILE,
            ChannelMetadataField.STREAM_PROFILE,
        )
    except Exception:
        logger.debug("Could not clear Catchuparr recorder metadata for %s", worker_id, exc_info=True)


def _release_managed_worker(proxy_server, worker_id: str) -> bool:
    redis_client = getattr(proxy_server, "redis_client", None)
    if redis_client is None:
        return False
    released = _release_worker_reservation(redis_client, worker_id)
    _clear_worker_reservation_metadata(redis_client, worker_id)
    record = read_worker_record(redis_client, worker_id) or {}
    if record.get("reservation_state") in {None, "none", "released"}:
        redis_client.hset(
            worker_id_key(worker_id),
            mapping={
                "state": "released",
                "reservation_state": "released",
                "released_at": str(time.time()),
            },
        )
        redis_client.expire(worker_id_key(worker_id), WORKER_RECORD_TTL)
    return released


def _collect_core_api_compatibility_issues() -> tuple[str, ...]:
    """Return safe module, callable, and parameter names for rejected APIs."""
    module_names = (
        "apps.channels.models",
        "apps.m3u.connection_pool",
        "apps.proxy.live_proxy.urls",
        "apps.proxy.live_proxy.constants",
        "apps.proxy.live_proxy.input.manager",
        "apps.proxy.live_proxy.output.ts.generator",
        "apps.proxy.live_proxy.redis_keys",
        "apps.proxy.live_proxy.server",
        "apps.proxy.live_proxy.services.channel_service",
        "apps.proxy.live_proxy.url_utils",
        "apps.proxy.live_proxy.views",
        "dispatcharr.utils",
    )
    modules = {}
    for name in module_names:
        try:
            modules[name] = importlib.import_module(name)
        except Exception:
            return (f"import:{name}",)

    channel_stream = getattr(modules["apps.channels.models"], "ChannelStream", None)
    pool = modules["apps.m3u.connection_pool"]
    metadata_fields = getattr(modules["apps.proxy.live_proxy.constants"], "ChannelMetadataField", None)
    input_manager = modules["apps.proxy.live_proxy.input.manager"]
    generator = modules["apps.proxy.live_proxy.output.ts.generator"]
    redis_keys = modules["apps.proxy.live_proxy.redis_keys"]
    proxy_server = getattr(modules["apps.proxy.live_proxy.server"], "ProxyServer", None)
    channel_service = getattr(
        modules["apps.proxy.live_proxy.services.channel_service"], "ChannelService", None
    )
    url_utils = modules["apps.proxy.live_proxy.url_utils"]
    proxy_views = modules["apps.proxy.live_proxy.views"]
    dispatcharr_utils = modules["dispatcharr.utils"]

    required = (
        ("reserve_profile_slot", getattr(pool, "reserve_profile_slot", None), {"profile", "redis_client"}),
        ("release_profile_slot", getattr(pool, "release_profile_slot", None), {"profile_id", "redis_client"}),
        (
            "ChannelService.initialize_channel",
            getattr(channel_service, "initialize_channel", None),
            {
                "channel_id", "stream_url", "user_agent", "transcode",
                "stream_profile_value", "stream_id", "m3u_profile_id",
                "channel_name", "stream_name",
            },
        ),
        (
            "create_stream_generator",
            getattr(generator, "create_stream_generator", None),
            {
                "channel_id", "client_id", "client_ip", "client_user_agent",
                "channel_initializing", "user", "buffer", "channel_name",
            },
        ),
        ("ProxyServer._release_stream_resources", getattr(proxy_server, "_release_stream_resources", None), {"self", "channel_id"}),
        ("ProxyServer.try_acquire_ownership", getattr(proxy_server, "try_acquire_ownership", None), {"self", "channel_id", "ttl"}),
        ("ProxyServer._get_channel_init_lock", getattr(proxy_server, "_get_channel_init_lock", None), {"self", "channel_id"}),
        ("ProxyServer._finish_channel_init_lock", getattr(proxy_server, "_finish_channel_init_lock", None), {"self", "channel_id", "lock"}),
        ("ProxyServer.get_buffer", getattr(proxy_server, "get_buffer", None), {"self", "channel_id", "profile"}),
        ("ProxyServer.initialize_channel", getattr(proxy_server, "initialize_channel", None), {"self", "url", "channel_id", "user_agent", "transcode", "stream_id"}),
        ("ChannelService.is_channel_unavailable_for_new_clients", getattr(channel_service, "is_channel_unavailable_for_new_clients", None), {"channel_id"}),
        ("ChannelService.stop_channel", getattr(channel_service, "stop_channel", None), {"channel_id"}),
        ("_resolve_live_stream_url", getattr(url_utils, "_resolve_live_stream_url", None), {"stream", "m3u_account", "m3u_profile"}),
        ("get_client_ip", getattr(dispatcharr_utils, "get_client_ip", None), {"request"}),
        ("_channel_setup_needed", getattr(proxy_views, "_channel_setup_needed", None), {"proxy_server", "channel_id"}),
        ("get_alternate_streams", getattr(input_manager, "get_alternate_streams", None), {"channel_id"}),
        ("RedisKeys.channel_metadata", getattr(getattr(redis_keys, "RedisKeys", None), "channel_metadata", None), {"channel_id"}),
        ("RedisKeys.channel_owner", getattr(getattr(redis_keys, "RedisKeys", None), "channel_owner", None), {"channel_id"}),
    )
    issues = []
    for name, function, expected in required:
        if function is None:
            issues.append(f"missing:{name}")
            continue
        try:
            parameters = set(inspect.signature(function).parameters)
        except Exception:
            issues.append(f"signature:{name}")
            continue
        missing = sorted(expected - parameters)
        if missing:
            issues.append(f"parameters:{name}:{','.join(missing)}")

    if channel_stream is None or not hasattr(channel_stream, "objects"):
        issues.append("attribute:ChannelStream.objects")
    if metadata_fields is None:
        issues.append("attribute:ChannelMetadataField")
    else:
        for field in ("STREAM_ID", "M3U_PROFILE", "STREAM_PROFILE"):
            if not hasattr(metadata_fields, field):
                issues.append(f"attribute:ChannelMetadataField.{field}")

    stream_routes = [
        route
        for route in getattr(modules["apps.proxy.live_proxy.urls"], "urlpatterns", ())
        if getattr(route, "name", None) == "stream"
    ]
    if not stream_routes:
        issues.append("route:stream")
    for index, route in enumerate(stream_routes):
        try:
            parameters = set(inspect.signature(route.callback).parameters)
        except Exception:
            issues.append(f"signature:route.stream[{index}].callback")
            continue
        if "channel_id" not in parameters:
            issues.append(f"parameters:route.stream[{index}].callback:channel_id")
    return tuple(issues)


def _core_api_compatibility_issues() -> tuple[str, ...]:
    try:
        return _collect_core_api_compatibility_issues()
    except Exception:
        return ("inspection:core-api",)


def core_api_supported() -> bool:
    """Check the small shared API surface inspected in Dispatcharr 0.31/0.32."""
    return not _core_api_compatibility_issues()


def _client_manager_api_supported(client_manager) -> bool:
    try:
        add_parameters = inspect.signature(client_manager.add_client).parameters
        required = {
            "client_id", "client_ip", "user_agent", "user",
            "output_format", "output_profile_id",
        }
        if not required <= set(add_parameters):
            return False
        if any(
            add_parameters[name].kind == inspect.Parameter.POSITIONAL_ONLY
            for name in {"user", "output_format", "output_profile_id"}
        ):
            return False
        remove_parameters = inspect.signature(client_manager.remove_client).parameters
        return "client_id" in remove_parameters and hasattr(client_manager, "clients")
    except Exception:
        return False


class _SourceURLRedactor(logging.Filter):
    def __init__(self, *, redact_all: bool = False):
        super().__init__()
        self.redact_all = redact_all

    def filter(self, record):
        try:
            message = record.getMessage()
            should_redact = self.redact_all or re.search(
                r"catchuparr-r[0-9a-f]{40}", message
            )
            if should_redact:
                safe = _SOURCE_URL_RE.sub("<provider-url>", message)
                record.msg = safe
                record.args = ()
                if record.exc_info:
                    exception = logging.Formatter().formatException(record.exc_info)
                    record.exc_text = _SOURCE_URL_RE.sub("<provider-url>", exception)
                    record.exc_info = None
        except Exception:
            return True
        return True


def install_proxyserver_cleanup_hook() -> bool:
    """Install per-process guards and exact reservation cleanup for plugin IDs."""
    compatibility_issues = _core_api_compatibility_issues()
    if compatibility_issues:
        logger.error(
            "Recorder source overrides disabled: Dispatcharr API checks failed: %s",
            ", ".join(compatibility_issues),
        )
        return False
    try:
        from apps.proxy.live_proxy import server as live_server
        from apps.proxy.live_proxy import url_utils
        from apps.proxy.live_proxy import urls as live_urls
        from apps.proxy.live_proxy.input import manager as input_manager
        from apps.proxy.live_proxy.server import ProxyServer
        from apps.proxy.live_proxy.services import channel_service
        from django.http import HttpResponseNotFound
    except Exception:
        logger.exception("Recorder source override guards could not be installed")
        return False

    if not hasattr(ProxyServer, "_catchuparr_original_release_stream_resources"):
        original = ProxyServer._release_stream_resources

        def release_guard(self, channel_id):
            if is_managed_worker_id(channel_id):
                _release_managed_worker(self, channel_id)
                return True
            return original(self, channel_id)

        ProxyServer._catchuparr_original_release_stream_resources = original
        ProxyServer._release_stream_resources = release_guard

    if not hasattr(input_manager, "_catchuparr_original_get_alternate_streams"):
        original_alternates = input_manager.get_alternate_streams

        def alternate_guard(channel_id, *args, **kwargs):
            if is_managed_worker_id(channel_id):
                return []
            return original_alternates(channel_id, *args, **kwargs)

        input_manager._catchuparr_original_get_alternate_streams = original_alternates
        input_manager.get_alternate_streams = alternate_guard

    for logger_obj, redact_all in (
        (logger, True),
        (input_manager.logger, True),
        (live_server.logger, True),
        (channel_service.logger, True),
        (url_utils.logger, True),
    ):
        if not any(getattr(flt, "_catchuparr_url_redactor", False) for flt in logger_obj.filters):
            redactor = _SourceURLRedactor(redact_all=redact_all)
            redactor._catchuparr_url_redactor = True
            logger_obj.addFilter(redactor)

    for route in getattr(live_urls, "urlpatterns", ()):
        if getattr(route, "name", None) != "stream":
            continue
        callback = route.callback
        if getattr(callback, "_catchuparr_managed_id_guard", False):
            continue

        def stream_guard(request, channel_id, *args, __original=callback, **kwargs):
            if is_managed_worker_id(channel_id):
                return HttpResponseNotFound()
            return __original(request, channel_id, *args, **kwargs)

        stream_guard._catchuparr_managed_id_guard = True
        stream_guard._catchuparr_original = callback
        route.callback = stream_guard

    return True


def uninstall_proxyserver_cleanup_hook() -> bool:
    """Remove guards only after all managed sessions have released their slots."""
    try:
        from core.utils import RedisClient

        redis_client = RedisClient.get_client()
        redis_client.ping()
        if managed_worker_ids(redis_client, active_only=True):
            return False
    except Exception:
        logger.exception("Could not verify Catchuparr recorder workers before removing cleanup guards")
        return False
    try:
        from apps.proxy.live_proxy import server as live_server
        from apps.proxy.live_proxy import url_utils
        from apps.proxy.live_proxy import urls as live_urls
        from apps.proxy.live_proxy.input import manager as input_manager
        from apps.proxy.live_proxy.server import ProxyServer
        from apps.proxy.live_proxy.services import channel_service

        original = getattr(ProxyServer, "_catchuparr_original_release_stream_resources", None)
        if original is not None:
            ProxyServer._release_stream_resources = original
            delattr(ProxyServer, "_catchuparr_original_release_stream_resources")
        original_alternates = getattr(
            input_manager, "_catchuparr_original_get_alternate_streams", None
        )
        if original_alternates is not None:
            input_manager.get_alternate_streams = original_alternates
            delattr(input_manager, "_catchuparr_original_get_alternate_streams")
        for route in getattr(live_urls, "urlpatterns", ()):
            callback = route.callback
            if getattr(callback, "_catchuparr_managed_id_guard", False):
                route.callback = callback._catchuparr_original
        for logger_obj in (
            input_manager.logger,
            live_server.logger,
            logger,
            channel_service.logger,
            url_utils.logger,
        ):
            for filter_obj in list(logger_obj.filters):
                if getattr(filter_obj, "_catchuparr_url_redactor", False):
                    logger_obj.removeFilter(filter_obj)
        return True
    except Exception:
        logger.exception("Recorder source override guards could not be removed")
        return False


def managed_worker_ids(redis_client=None, *, active_only: bool = False) -> list[str]:
    if redis_client is None:
        try:
            from core.utils import RedisClient

            redis_client = RedisClient.get_client()
        except Exception:
            raise RuntimeError("recorder worker registry is unavailable")
    result = []
    cursor = 0
    pattern = f"{PLUGIN_REDIS_PREFIX}worker:catchuparr-r*"
    try:
        while True:
            cursor, keys = redis_client.scan(cursor, match=pattern, count=100)
            for key in keys:
                key_text = str(_decode(key))
                worker_id = key_text.rsplit(":", 1)[-1]
                if not is_managed_worker_id(worker_id):
                    continue
                record = read_worker_record(redis_client, worker_id) or {}
                if active_only and record.get("state") not in _ACTIVE_WORKER_STATES:
                    continue
                result.append(worker_id)
            if cursor == 0:
                break
    except Exception:
        logger.exception("Could not inspect Catchuparr recorder worker registry")
        raise RuntimeError("recorder worker registry could not be inspected")
    return result


def stop_managed_workers(timeout_seconds: float = 5.0) -> bool:
    """Ask Dispatcharr to stop every active private worker before unhooking."""
    try:
        from apps.proxy.live_proxy.services.channel_service import ChannelService
        from core.utils import RedisClient

        redis_client = RedisClient.get_client()
        redis_client.ping()
    except Exception:
        logger.exception("Could not verify Catchuparr recorder workers during shutdown")
        return False
    for worker_id in managed_worker_ids(redis_client, active_only=True):
        try:
            ChannelService.stop_channel(worker_id)
            _finalize_if_native_worker_gone(redis_client, worker_id)
        except Exception:
            logger.exception("Could not stop Catchuparr recorder worker %s", worker_id)
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    while time.monotonic() < deadline:
        if not managed_worker_ids(redis_client, active_only=True):
            return True
        time.sleep(0.1)
    return not managed_worker_ids(redis_client, active_only=True)


def _finalize_if_native_worker_gone(redis_client, worker_id: str) -> bool:
    """Release a reservation only when no native worker can still use it."""
    if redis_client.exists(worker_open_lock_key(worker_id)):
        return False
    try:
        from apps.proxy.live_proxy.redis_keys import RedisKeys

        metadata = redis_client.hgetall(RedisKeys.channel_metadata(worker_id)) or {}
        state = _decode(metadata.get("state", ""))
        if state and str(state).lower() not in {"stopped", "error", "failed"}:
            return False
        if redis_client.exists(RedisKeys.channel_owner(worker_id)):
            return False
    except Exception:
        return False
    record = read_worker_record(redis_client, worker_id) or {}
    if record.get("reservation_state") == "reserved":
        _release_worker_reservation(redis_client, worker_id)
    current = read_worker_record(redis_client, worker_id) or {}
    if current.get("reservation_state") in {None, "none", "released"}:
        redis_client.hset(
            worker_id_key(worker_id),
            mapping={
                "state": "released",
                "reservation_state": "released",
                "released_at": str(time.time()),
            },
        )
        redis_client.expire(worker_id_key(worker_id), WORKER_RECORD_TTL)
        return True
    return False


def _reserve_source_profile(redis_client, worker_id: str, source, account):
    """Select and reserve an active provider profile without core Stream keys."""
    from apps.m3u.connection_pool import reserve_profile_slot

    profiles = list(account.profiles.filter(is_active=True).order_by("id"))
    profiles.sort(key=lambda profile: (not bool(profile.is_default), int(profile.id)))
    record = read_worker_record(redis_client, worker_id) or {}
    reservation_id = record.get("reservation_id") or uuid.uuid4().hex
    for profile in profiles:
        write_worker_record(
            redis_client,
            worker_id,
            {
                "state": "reserving",
                "reservation_state": "reserving",
                "reservation_id": reservation_id,
                "profile_id": profile.id,
                "source_id": source.id,
                "account_id": account.id,
            },
        )
        facade = _CredentialMarkerRedisFacade(redis_client, profile.id, reservation_id)
        try:
            reserved, _count, _reason = reserve_profile_slot(profile, facade)
        except Exception:
            # The core API may have updated a counter before raising. Keep the
            # reservation intent visible and fail closed rather than guessing.
            raise
        if not reserved:
            redis_client.hdel(worker_id_key(worker_id), "profile_id")
            write_worker_record(
                redis_client,
                worker_id,
                {"state": "issued", "reservation_state": "none"},
            )
            continue
        try:
            write_worker_record(
                redis_client,
                worker_id,
                {"state": "reserved", "reservation_state": "reserved"},
            )
        except Exception:
            # We know this invocation successfully reserved this slot, so it is
            # safe to immediately roll back with its private marker.
            try:
                _release_core_profile_slot(redis_client, profile.id, reservation_id)
                write_worker_record(
                    redis_client,
                    worker_id,
                    {"state": "released", "reservation_state": "released"},
                )
            except Exception:
                try:
                    write_worker_record(
                        redis_client,
                        worker_id,
                        {"state": "release_failed", "reservation_state": "release_failed"},
                    )
                except Exception:
                    pass
            raise
        return profile
    write_worker_record(
        redis_client,
        worker_id,
        {"state": "failed", "reservation_state": "none"},
    )
    return None


def _source_and_channel(channel_uuid: str, stream_id: str):
    from apps.channels.models import Channel, ChannelStream, Stream

    assignment = (
        ChannelStream.objects.select_related("channel", "stream__m3u_account")
        .filter(channel__uuid=str(channel_uuid), stream_id=int(stream_id))
        .order_by("order", "id")
        .first()
    )
    if assignment is None:
        return None, None
    channel = Channel.objects.filter(uuid=str(channel_uuid)).first()
    source = Stream.objects.select_related("m3u_account", "stream_profile").filter(
        pk=assignment.stream_id
    ).first()
    if channel is None or source is None:
        return None, None
    account = source.m3u_account
    if account is None or not getattr(account, "is_active", False):
        return None, None
    return channel, source


def _resolve_source_details(source, profile):
    from apps.proxy.live_proxy.url_utils import _resolve_live_stream_url
    from core.models import CoreSettings, UserAgent

    account = source.m3u_account
    url = _resolve_live_stream_url(source, account, profile)
    if not isinstance(url, str) or not url.strip():
        raise ValueError("source URL could not be resolved")
    try:
        user_agent = account.get_user_agent().user_agent
    except Exception:
        default_id = CoreSettings.get_default_user_agent_id()
        user_agent = UserAgent.objects.get(id=default_id).user_agent
    if not user_agent:
        user_agent = "Catchuparr recorder"
    return url, user_agent


def open_managed_source(request, worker_id: str, capability_record: dict[str, Any]):
    """Initialize/attach to one private proxy worker and stream native TS bytes."""
    from apps.proxy.live_proxy import views as core_views
    from apps.proxy.live_proxy.constants import ChannelState
    from apps.proxy.live_proxy.output.ts.generator import create_stream_generator
    from apps.proxy.live_proxy.server import ProxyServer
    from apps.proxy.live_proxy.services.channel_service import ChannelService
    from dispatcharr.utils import get_client_ip
    from django.db import close_old_connections
    from django.http import HttpResponse, StreamingHttpResponse

    proxy_server = ProxyServer.get_instance()
    redis_client = getattr(proxy_server, "redis_client", None)
    if redis_client is None:
        return HttpResponse("Recorder proxy unavailable", status=503)

    record = read_worker_record(redis_client, worker_id)
    if record is None:
        return HttpResponse("Recorder worker ledger is unavailable", status=503)
    if (
        record.get("worker_id") != str(worker_id)
        or record.get("channel_uuid") != str(capability_record.get("channel_uuid"))
        or record.get("stream_id") != str(capability_record.get("stream_id"))
        or record.get("account_id") != str(capability_record.get("account_id"))
        or record.get("config_generation") != str(capability_record.get("config_generation"))
        or record.get("lease_value") != str(capability_record.get("lease_value"))
        or record.get("lease_fence") != str(capability_record.get("lease_fence"))
        or record.get("capability_digest") != str(capability_record.get("capability_digest"))
    ):
        return HttpResponse("Recorder capability is stale", status=403)
    if record.get("state") in {"failed", "released", "release_failed", "releasing"}:
        return HttpResponse("Recorder worker is no longer available", status=410)

    open_lock = worker_open_lock_key(worker_id)
    lock_owner = uuid.uuid4().hex
    if not redis_client.set(open_lock, lock_owner, nx=True, ex=120):
        return HttpResponse("Recorder worker is initializing", status=503)

    client_id = f"catchuparr-client-{uuid.uuid4().hex}"
    registered = False
    setup_owner = False
    init_lock = None
    try:
        channel, source = _source_and_channel(
            str(capability_record["channel_uuid"]), str(capability_record["stream_id"])
        )
        if channel is None or source is None:
            return HttpResponse("Assigned source is unavailable", status=410)
        from ..recorder_proxy import capability_binding_current

        if not capability_binding_current(redis_client, capability_record):
            return HttpResponse("Recorder capability is stale", status=403)
        account = source.m3u_account
        profile_id = record.get("profile_id")
        profile = None
        if record.get("reservation_state") == "reserved" and profile_id:
            from apps.m3u.models import M3UAccountProfile

            profile = M3UAccountProfile.objects.filter(
                id=int(profile_id), m3u_account_id=account.id, is_active=True
            ).first()
        if profile is None and record.get("reservation_state") == "reserved":
            if record.get("state") in {"starting", "active", "connecting", "initializing"}:
                try:
                    ChannelService.stop_channel(worker_id)
                finally:
                    _release_worker_reservation(redis_client, worker_id)
                return HttpResponse("Recorder provider profile is no longer active", status=503)
            if not _release_worker_reservation(redis_client, worker_id):
                return HttpResponse("Recorder profile reservation could not be released", status=503)
            write_worker_record(
                redis_client,
                worker_id,
                {
                    "state": "issued",
                    "reservation_state": "none",
                    "reservation_id": uuid.uuid4().hex,
                    "profile_id": "",
                },
            )
        if profile is None:
            profile = _reserve_source_profile(redis_client, worker_id, source, account)
        if profile is None:
            return HttpResponse("No provider profile has capacity", status=503)

        stream_url, user_agent = _resolve_source_details(source, profile)
        if not core_api_supported():
            raise RuntimeError("Dispatcharr proxy API signature is unverified")

        needs_setup, channel_state, channel_initializing = core_views._channel_setup_needed(
            proxy_server, worker_id
        )
        if channel_state == ChannelState.STOPPING or ChannelService.is_channel_unavailable_for_new_clients(worker_id):
            return HttpResponse("Recorder worker is stopping", status=503)
        if needs_setup:
            init_lock = proxy_server._get_channel_init_lock(worker_id)
            init_lock.acquire()
            try:
                needs_setup, channel_state, channel_initializing = core_views._channel_setup_needed(
                    proxy_server, worker_id
                )
                if channel_state == ChannelState.STOPPING or ChannelService.is_channel_unavailable_for_new_clients(worker_id):
                    return HttpResponse("Recorder worker is stopping", status=503)
                if not needs_setup:
                    channel_initializing = channel_state in {
                        ChannelState.INITIALIZING,
                        ChannelState.CONNECTING,
                    }
                elif worker_id in proxy_server._channels_setting_up:
                    channel_initializing = True
                elif not proxy_server.try_acquire_ownership(worker_id):
                    channel_initializing = True
                else:
                    setup_owner = True
                    proxy_server._channels_setting_up.add(worker_id)
            finally:
                proxy_server._finish_channel_init_lock(worker_id, init_lock)
                init_lock = None

        if setup_owner:
            if not capability_binding_current(redis_client, capability_record):
                try:
                    ChannelService.stop_channel(worker_id)
                finally:
                    _release_worker_reservation(redis_client, worker_id)
                return HttpResponse("Recorder capability is stale", status=403)
            write_worker_record(redis_client, worker_id, {"state": "starting"})
            success = ChannelService.initialize_channel(
                worker_id,
                stream_url,
                user_agent,
                transcode=False,
                stream_profile_value=None,
                stream_id=source.id,
                m3u_profile_id=profile.id,
                channel_name=channel.name,
                stream_name=source.name,
            )
            if not success:
                write_worker_record(redis_client, worker_id, {"state": "failed"})
                _release_worker_reservation(redis_client, worker_id)
                return HttpResponse("Recorder source could not be initialized", status=503)
            write_worker_record(redis_client, worker_id, {"state": "active"})

        # Requests can land on a different web process from the owner. The
        # native service creates a local reader buffer/client manager when it
        # attaches to an already active Redis session.
        if not setup_owner and worker_id not in proxy_server.client_managers:
            if not capability_binding_current(redis_client, capability_record):
                return HttpResponse("Recorder capability is stale", status=403)
            if not ChannelService.initialize_channel(
                worker_id,
                stream_url,
                user_agent,
                transcode=False,
                stream_profile_value=None,
                stream_id=source.id,
                m3u_profile_id=profile.id,
                channel_name=channel.name,
                stream_name=source.name,
            ):
                return HttpResponse("Recorder worker could not be attached", status=503)

        if not capability_binding_current(redis_client, capability_record):
            try:
                ChannelService.stop_channel(worker_id)
            finally:
                _release_worker_reservation(redis_client, worker_id)
            return HttpResponse("Recorder capability is stale", status=403)

        client_manager = proxy_server.client_managers.get(worker_id)
        if client_manager is None or not _client_manager_api_supported(client_manager):
            return HttpResponse("Recorder worker resources unavailable", status=503)
        client_ip = get_client_ip(request) or "127.0.0.1"
        client_ua = request.META.get("HTTP_USER_AGENT", "Catchuparr recorder")
        if not client_manager.add_client(
            client_id,
            client_ip,
            client_ua,
            user=None,
            output_format="mpegts",
            output_profile_id=None,
        ):
            return HttpResponse("Recorder client registration failed", status=503)
        registered = True
        buffer = proxy_server.get_buffer(worker_id, profile=None)
        generate = create_stream_generator(
            worker_id,
            client_id,
            client_ip,
            client_ua,
            channel_initializing,
            user=None,
            buffer=buffer,
            channel_name=channel.name,
        )

        def body():
            try:
                yield from generate()
            finally:
                try:
                    manager = proxy_server.client_managers.get(worker_id)
                    if manager and client_id in manager.clients:
                        manager.remove_client(client_id)
                except Exception:
                    logger.warning("Could not unregister recorder client for %s", worker_id)
                close_old_connections()

        response = StreamingHttpResponse(body(), content_type="video/mp2t")
        response["Cache-Control"] = "no-cache, no-store"
        response["X-Content-Type-Options"] = "nosniff"
        return response
    except Exception:
        logger.exception("Catchuparr recorder proxy setup failed for %s", worker_id)
        if registered:
            try:
                manager = proxy_server.client_managers.get(worker_id)
                if manager and client_id in manager.clients:
                    manager.remove_client(client_id)
            except Exception:
                logger.exception("Could not unregister recorder proxy client for %s", worker_id)
        if setup_owner:
            try:
                ChannelService.stop_channel(worker_id)
            except Exception:
                logger.exception("Could not stop failed recorder proxy worker %s", worker_id)
        _release_worker_reservation(redis_client, worker_id)
        return HttpResponse("Recorder source setup failed", status=503)
    finally:
        if init_lock is not None:
            proxy_server._finish_channel_init_lock(worker_id, init_lock)
        try:
            redis_client.eval(
                "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end",
                1,
                open_lock,
                lock_owner,
            )
        except Exception:
            # Let the bounded TTL clear an uncertain lock. An unconditional
            # delete could erase a newer opener's lock after expiry.
            logger.debug("Could not release recorder open lock for %s", worker_id, exc_info=True)
