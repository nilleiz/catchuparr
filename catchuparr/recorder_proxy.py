"""Applied source policy routing and signed internal recorder capabilities."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import secrets
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Mapping

from .source_rules import SourcePolicy, rank_candidates

logger = logging.getLogger(__name__)

CAPABILITY_PREFIX = "catchuparr:recorder-proxy:capability:"
CAPABILITY_TTL_SECONDS = 45
CAPABILITY_HEADER = "X-Catchuparr-Recorder"


@dataclass(frozen=True)
class RecorderProxyAttempt:
    """One ranked source attempt; bearer material is excluded from repr."""

    worker_id: str
    stream_id: str
    account_id: str
    config_generation: str
    capability_key: str
    input_url: str
    capability: str = field(repr=False)

    @property
    def input_headers(self) -> dict[str, str]:
        return {CAPABILITY_HEADER: self.capability}

    def renew(self, redis_client, ttl_seconds: int = CAPABILITY_TTL_SECONDS) -> bool:
        return bool(redis_client.expire(self.capability_key, ttl_seconds))

    def revoke(self, redis_client) -> None:
        redis_client.delete(self.capability_key)


def configuration_generation(active_configuration: Mapping[str, Any]) -> str:
    serialized = json.dumps(
        active_configuration, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _decode(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


def _active_configuration():
    from .configuration import load_active_configuration

    return load_active_configuration()


def _close_database_connections() -> None:
    """Release a safe, existing default ORM connection before a long stream."""
    try:
        from django.db import connections
    except ImportError:
        return
    try:
        initialized = connections.all(initialized_only=True)
    except Exception:
        return
    default = next(
        (connection for connection in initialized if connection.alias == "default"),
        None,
    )
    if (
        default is None
        or default.connection is None
        or default.in_atomic_block
    ):
        return
    try:
        autocommit = default.get_autocommit()
    except Exception:
        return
    if autocommit:
        default.close()


def _policy_for_channel(active: Mapping[str, Any], channel_uuid: str) -> SourcePolicy | None:
    encoded = active.get("source_policies", {}).get(str(channel_uuid))
    if encoded is None:
        return None
    if not isinstance(encoded, dict):
        raise ValueError("applied source policy is invalid")
    include_ids = encoded.get("include_account_ids")
    if include_ids is not None and (
        not isinstance(include_ids, list)
        or any(not isinstance(value, (str, int)) or not str(value) for value in include_ids)
    ):
        raise ValueError("applied source include IDs are invalid")
    exclude_ids = encoded.get("exclude_account_ids")
    if not isinstance(exclude_ids, list) or any(
        not isinstance(value, (str, int)) or not str(value) for value in exclude_ids
    ):
        raise ValueError("applied source exclude IDs are invalid")
    if include_ids is not None and exclude_ids:
        raise ValueError("applied source policy has both include and exclude")
    priorities = encoded.get("priorities", [])
    if not isinstance(priorities, list):
        raise ValueError("applied source priorities are invalid")
    decoded_priorities = []
    for pair in priorities:
        if (
            not isinstance(pair, (tuple, list))
            or len(pair) != 2
            or not isinstance(pair[0], (str, int))
            or not str(pair[0])
            or isinstance(pair[1], bool)
            or not isinstance(pair[1], int)
        ):
            raise ValueError("applied source priority value is invalid")
        decoded_priorities.append((str(pair[0]), pair[1]))
    if len({account_id for account_id, _ in decoded_priorities}) != len(decoded_priorities):
        raise ValueError("applied source priorities contain duplicates")
    known = encoded.get("known_account_ids")
    if isinstance(known, list) and all(
        isinstance(value, (str, int)) and str(value) for value in known
    ):
        known_ids = frozenset(str(value) for value in known)
        if len(known_ids) != len(known):
            raise ValueError("applied known account IDs contain duplicates")
    else:
        raise ValueError("applied known account IDs are invalid")
    include_ids_set = frozenset(str(value) for value in (include_ids or ()))
    exclude_ids_set = frozenset(str(value) for value in exclude_ids)
    if (
        not include_ids_set <= known_ids
        or not exclude_ids_set <= known_ids
        or not {account_id for account_id, _ in decoded_priorities} <= known_ids
    ):
        raise ValueError("applied policy references an unknown account")
    return SourcePolicy(
        include_account_ids=include_ids_set if include_ids is not None else None,
        exclude_account_ids=exclude_ids_set,
        priorities=tuple(decoded_priorities),
        known_account_ids=known_ids,
    )


def ranked_source_candidates(
    channel_uuid: str,
    active_configuration: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]] | None:
    """Return ranked assigned sources, or None when the default live route applies."""
    active = active_configuration if active_configuration is not None else _active_configuration()
    if active is None:
        return None
    policy = _policy_for_channel(active, str(channel_uuid))
    if policy is None:
        return None
    from .configuration import source_catalog

    try:
        catalog = source_catalog()
    finally:
        # The returned catalog is fully materialized. Do not retain a pooled
        # Django connection in a task supervisor or a request thread that may
        # now spend minutes consuming a transport stream.
        _close_database_connections()
    current_accounts = {
        str(account["id"]) for account in catalog.accounts if account.get("id") is not None
    }
    if not policy.known_account_ids <= current_accounts:
        current_ids = policy.known_account_ids & current_accounts
        policy = SourcePolicy(
            include_account_ids=(
                policy.include_account_ids & current_ids
                if policy.include_account_ids is not None else None
            ),
            exclude_account_ids=policy.exclude_account_ids & current_ids,
            priorities=tuple(
                (account_id, score)
                for account_id, score in policy.priorities
                if account_id in current_ids
            ),
            known_account_ids=current_ids,
        )
    streams = catalog.streams_by_channel.get(str(channel_uuid), ())
    return [dict(candidate) for candidate in rank_candidates(policy, streams)]


def candidate_is_current(
    channel_uuid: str,
    stream_id: str,
    account_id: str,
    active_configuration: Mapping[str, Any] | None = None,
) -> bool:
    """Recheck assignment membership and the applied policy at each HTTP open."""
    try:
        active = active_configuration if active_configuration is not None else _active_configuration()
        if active is None:
            return False
        from .runtime import parse_settings

        if str(channel_uuid) not in set(parse_settings(dict(active)).channel_uuids):
            return False
        candidates = ranked_source_candidates(str(channel_uuid), active)
        if candidates is None:
            return False
        return any(
            str(candidate.get("id")) == str(stream_id)
            and str(candidate.get("account_id")) == str(account_id)
            for candidate in candidates
        )
    except Exception:
        logger.exception("Could not validate applied recorder source for channel %s", channel_uuid)
        return False


def _secret_key() -> bytes:
    from django.conf import settings

    value = getattr(settings, "SECRET_KEY", "")
    if not isinstance(value, str) or not value:
        raise RuntimeError("Dispatcharr signing key is unavailable")
    return value.encode("utf-8")


def _canonical_binding(binding: Mapping[str, Any], nonce: str) -> bytes:
    value = {key: binding[key] for key in sorted(binding)}
    value["nonce"] = nonce
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _signature(binding: Mapping[str, Any], nonce: str) -> str:
    signature = hmac.new(_secret_key(), _canonical_binding(binding, nonce), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")


def issue_recorder_attempt(
    redis_client,
    lease,
    *,
    channel_uuid: str,
    candidate: Mapping[str, Any],
    config_generation: str,
    internal_base_url: str,
) -> RecorderProxyAttempt:
    """Issue an HMAC-signed one-recorder capability and store only its digest."""
    from .adapters.recorder_proxy import make_worker_id, write_worker_record

    fence = getattr(lease, "fence", None)
    lease_value = getattr(lease, "_value", None)
    lease_key = getattr(lease, "key", None)
    if not fence or not lease_value or not lease_key:
        raise ValueError("recorder lease is not active")
    if str(lease_key) != f"catchuparr:recorder:{channel_uuid}":
        raise ValueError("recorder lease does not match the requested channel")
    stream_id = str(candidate.get("id") or candidate.get("stream_id") or "")
    account_id = str(candidate.get("account_id") or "")
    if not stream_id or not account_id:
        raise ValueError("assigned recorder source is invalid")
    attempt_id = uuid.uuid4().hex
    worker_id = make_worker_id(
        str(channel_uuid), stream_id, config_generation, int(fence), attempt_id
    )
    binding = {
        "account_id": account_id,
        "channel_uuid": str(channel_uuid),
        "config_generation": str(config_generation),
        "lease_fence": str(int(fence)),
        "lease_key": str(lease_key),
        "lease_value": str(lease_value),
        "stream_id": stream_id,
        "worker_id": worker_id,
    }
    nonce = secrets.token_urlsafe(32)
    token = f"{nonce}.{_signature(binding, nonce)}"
    digest = hashlib.sha256(token.encode("ascii")).hexdigest()
    key = CAPABILITY_PREFIX + digest
    record = {**binding, "capability_digest": digest}
    if not redis_client.set(
        key,
        json.dumps(record, sort_keys=True, separators=(",", ":")),
        nx=True,
        ex=CAPABILITY_TTL_SECONDS,
    ):
        raise RuntimeError("could not issue recorder capability")
    try:
        write_worker_record(
            redis_client,
            worker_id,
            {
                **binding,
                "worker_id": worker_id,
                "capability_digest": digest,
                "reservation_id": uuid.uuid4().hex,
                "reservation_state": "none",
                "state": "issued",
                "created_at": time.time(),
            },
        )
    except Exception:
        redis_client.delete(key)
        raise
    return RecorderProxyAttempt(
        worker_id=worker_id,
        stream_id=stream_id,
        account_id=account_id,
        config_generation=str(config_generation),
        capability_key=key,
        input_url=f"{internal_base_url.rstrip('/')}/catchuparr/recorder/{channel_uuid}",
        capability=token,
    )


def verify_recorder_capability(redis_client, token: str) -> dict[str, str] | None:
    if not isinstance(token, str) or not 48 <= len(token) <= 256:
        return None
    try:
        nonce, supplied_signature = token.split(".", 1)
        digest = hashlib.sha256(token.encode("ascii")).hexdigest()
        raw = redis_client.get(CAPABILITY_PREFIX + digest)
        if not raw:
            return None
        record = json.loads(_decode(raw))
        if not isinstance(record, dict):
            return None
        binding = {
            key: str(value)
            for key, value in record.items()
            if key != "capability_digest"
        }
        expected = _signature(binding, nonce)
        if not hmac.compare_digest(expected, supplied_signature):
            return None
        if record.get("capability_digest") != digest:
            return None
        return {key: str(value) for key, value in binding.items()} | {
            "capability_digest": digest
        }
    except Exception:
        return None


def capability_binding_current(redis_client, binding: Mapping[str, Any]) -> bool:
    """Validate current lease ownership, generation, channel, membership, and policy."""
    try:
        digest = str(binding["capability_digest"])
        raw_capability = redis_client.get(CAPABILITY_PREFIX + digest)
        if not raw_capability:
            return False
        stored = json.loads(_decode(raw_capability))
        if (
            not isinstance(stored, dict)
            or stored.get("capability_digest") != digest
        ):
            return False
        stored_binding = {
            key: str(value)
            for key, value in stored.items()
            if key != "capability_digest"
        }
        supplied_binding = {
            str(key): str(value)
            for key, value in binding.items()
            if key != "capability_digest"
        }
        if stored_binding != supplied_binding:
            return False
        if str(binding["lease_key"]) != f"catchuparr:recorder:{binding['channel_uuid']}":
            return False
        current_lease = _decode(redis_client.get(str(binding["lease_key"])))
        if current_lease != str(binding["lease_value"]):
            return False
        fence, _owner = current_lease.split(":", 1)
        if int(fence) != int(binding["lease_fence"]):
            return False
        active = _active_configuration()
        if active is None or configuration_generation(active) != str(binding["config_generation"]):
            return False
        return candidate_is_current(
            str(binding["channel_uuid"]),
            str(binding["stream_id"]),
            str(binding["account_id"]),
            active,
        )
    except Exception:
        return False


def stream_recorder_view(request, channel_uuid: str):
    """Private TS route opened only by a currently fenced recorder process."""
    from core.utils import RedisClient
    from django.http import HttpResponse

    from .adapters.recorder_proxy import (
        core_api_supported,
        install_proxyserver_cleanup_hook,
        open_managed_source,
    )
    from .configuration import load_active_configuration
    from .runtime import require_supported_version

    if request.method != "GET":
        return HttpResponse(status=405)
    try:
        require_supported_version()
    except RuntimeError:
        return HttpResponse("Recorder source proxy is unavailable", status=503)
    if not install_proxyserver_cleanup_hook() or not core_api_supported():
        return HttpResponse("Recorder source proxy is unavailable", status=503)
    token = getattr(request, "headers", {}).get(CAPABILITY_HEADER, "")
    if not token:
        return HttpResponse("Recorder capability required", status=401)
    try:
        redis_client = RedisClient.get_client()
    except Exception:
        return HttpResponse("Recorder source proxy is unavailable", status=503)
    binding = verify_recorder_capability(redis_client, token)
    if binding is None or str(binding.get("channel_uuid")) != str(channel_uuid):
        return HttpResponse("Recorder capability is invalid", status=403)
    active = load_active_configuration()
    if active is None or not capability_binding_current(redis_client, binding):
        return HttpResponse("Recorder capability is stale", status=403)

    try:
        from apps.channels.models import StreamProfile
        from core.models import PROXY_PROFILE_NAME

        if not StreamProfile.objects.filter(
            name=PROXY_PROFILE_NAME, locked=True, is_active=True
        ).exists():
            return HttpResponse("Dispatcharr Proxy profile is unavailable", status=503)
    except Exception:
        logger.exception("Could not verify Dispatcharr Proxy profile")
        return HttpResponse("Recorder source proxy is unavailable", status=503)

    try:
        response = open_managed_source(request, str(binding["worker_id"]), binding)
        if 300 <= int(getattr(response, "status_code", 200)) < 400 or "Location" in response:
            return HttpResponse("Recorder source proxy cannot redirect", status=502)
        return response
    finally:
        # The internal response is a long-lived stream. Release database
        # connections after all setup ORM reads and before the TS body starts.
        _close_database_connections()


def stop_recorder_attempt(redis_client, attempt: RecorderProxyAttempt, lease) -> bool:
    """Stop only the namespaced worker recorded for this recorder lease."""
    from .adapters.recorder_proxy import (
        _finalize_if_native_worker_gone,
        read_worker_record,
    )

    record = read_worker_record(redis_client, attempt.worker_id)
    if record is None:
        return True
    if (
        record.get("lease_value") != getattr(lease, "_value", None)
        or record.get("lease_fence") != str(getattr(lease, "fence", ""))
    ):
        return False
    try:
        from apps.proxy.live_proxy.services.channel_service import ChannelService

        ChannelService.stop_channel(attempt.worker_id)
    except Exception:
        logger.exception("Could not stop recorder proxy worker %s", attempt.worker_id)
        return False
    deadline = time.monotonic() + 6.0
    while time.monotonic() < deadline:
        current = read_worker_record(redis_client, attempt.worker_id) or {}
        if current.get("state") in {"released", "failed"}:
            return True
        if _finalize_if_native_worker_gone(redis_client, attempt.worker_id):
            return True
        time.sleep(0.1)
    current = read_worker_record(redis_client, attempt.worker_id) or {}
    return current.get("state") in {"released", "failed"}
