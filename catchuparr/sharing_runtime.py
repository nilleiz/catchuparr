"""Server-side admission for the internal read-only native sharing route.

The capability is a one-use opening claim. Every request rebuilds the small
ORM catalog once, checks the current applied/control generation and recorder
lease, then hands media reads to the native Redis-only reader session. No ORM
or SQLite operation runs in the chunk iterator.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path

from .adapters.sharing_borrow_route import (
    BorrowedReaderSession,
    SignedBorrowCapability,
)
from .adapters.sharing_native import NativeAssignedSource, observe_native_connection
from .sharing_episode import BorrowEpisodeLedger, BorrowEpisodeRejected


def _native_source_catalog(channel_uuid: str) -> tuple[tuple[NativeAssignedSource, ...], str]:
    """Materialize exact assignment/profile facts outside the media iterator."""
    from apps.channels.models import Channel, ChannelStream

    channel = Channel.objects.filter(uuid=channel_uuid).first()
    if channel is None:
        raise BorrowEpisodeRejected("sharing channel is no longer assigned")
    input_profile = channel.get_stream_profile()
    input_profile_id = str(getattr(input_profile, "id", "") or "")
    is_redirect = getattr(input_profile, "is_redirect", None)
    if not input_profile_id or not callable(is_redirect):
        raise BorrowEpisodeRejected("input profile compatibility is unavailable")
    try:
        redirect_input = is_redirect() is True
    except Exception:
        raise BorrowEpisodeRejected("input profile compatibility is unavailable") from None

    rows: list[NativeAssignedSource] = []
    assignments = ChannelStream.objects.filter(channel=channel).select_related(
        "stream", "stream__m3u_account",
    ).order_by("order", "id")
    for assignment in assignments:
        stream = assignment.stream
        account = getattr(stream, "m3u_account", None)
        account_id = str(getattr(stream, "m3u_account_id", "") or "")
        stream_id = str(getattr(stream, "id", "") or "")
        if account is None or not account_id or not stream_id:
            continue
        account_active = getattr(account, "is_active", None)
        if type(account_active) is not bool:
            raise BorrowEpisodeRejected("M3U account status is unavailable")
        profiles_manager = getattr(account, "profiles", None)
        if profiles_manager is None or not callable(getattr(profiles_manager, "all", None)):
            raise BorrowEpisodeRejected("M3U profile assignments are unavailable")
        try:
            profiles = list(profiles_manager.all().order_by("id"))
        except Exception:
            raise BorrowEpisodeRejected("M3U profile assignments are unavailable") from None
        for profile in profiles:
            profile_id = str(getattr(profile, "id", "") or "")
            active = getattr(profile, "is_active", None)
            if not profile_id or type(active) is not bool:
                raise BorrowEpisodeRejected("M3U profile status is unavailable")
            rows.append(NativeAssignedSource(
                channel_uuid=channel_uuid,
                stream_id=stream_id,
                account_id=account_id,
                m3u_profile_id=profile_id,
                input_profile_id=input_profile_id,
                assigned=True,
                account_active=account_active,
                profile_active=active,
                media_compatible=not redirect_input,
                supported_worker_kind=True,
                redirect_channel=redirect_input,
                vod_or_preview=False,
            ))
    if not rows:
        raise BorrowEpisodeRejected("no fully described M3U source assignments remain")
    canonical = [
        [row.channel_uuid, row.stream_id, row.account_id, row.m3u_profile_id,
         row.input_profile_id, row.assigned, row.account_active, row.profile_active,
         row.media_compatible, row.supported_worker_kind, row.redirect_channel,
         row.vod_or_preview]
        for row in rows
    ]
    digest = hashlib.sha256(
        json.dumps(canonical, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    ).hexdigest()
    return tuple(rows), digest


def open_borrow_session(capability: SignedBorrowCapability) -> BorrowedReaderSession:
    """Open a signed route only while its exact current native grant is valid."""
    if not isinstance(capability, SignedBorrowCapability):
        raise BorrowEpisodeRejected("signed sharing capability is invalid")
    from core.utils import RedisClient

    from .configuration import load_applied_state
    from .recorder_proxy import configuration_generation
    from .runtime import load_config

    redis_text = RedisClient.get_client()
    redis_binary = RedisClient.get_buffer()
    if redis_text is None or redis_binary is None:
        raise BorrowEpisodeRejected("sharing Redis backend is unavailable")
    active, control = load_applied_state()
    if active is None or control.paused:
        raise BorrowEpisodeRejected("sharing recorder admission is closed")
    if configuration_generation(active) != capability.configuration_generation:
        raise BorrowEpisodeRejected("sharing configuration changed")
    if control.generation != capability.control_generation:
        raise BorrowEpisodeRejected("sharing control generation changed")
    sharing = active.get("connection_sharing")
    setting = sharing.get(capability.channel_uuid) if isinstance(sharing, dict) else None
    if not isinstance(setting, dict) or setting.get("enabled") is not True:
        raise BorrowEpisodeRejected("connection sharing is disabled")

    config = load_config(active_snapshot=active)
    if config is None:
        raise BorrowEpisodeRejected("sharing archive configuration is unavailable")
    ledger = BorrowEpisodeLedger(Path(config.archive_root) / ".catchuparr-sharing.sqlite3")
    episode = ledger.read(capability.channel_uuid)
    if (
        episode.phase != "opening"
        or episode.episode_id != capability.episode_id
        or episode.version != capability.episode_version
        or episode.identity != capability.connection_identity
        or episode.configuration_generation != capability.configuration_generation
        or episode.control_generation != capability.control_generation
        or episode.recorder_fence != capability.recorder_fence
        or episode.intent_epoch != capability.intent_epoch
    ):
        raise BorrowEpisodeRejected("sharing opening no longer matches its durable episode")
    nonce = capability.nonce
    if len(nonce) != 32 or any(char not in "0123456789abcdef" for char in nonce):
        raise BorrowEpisodeRejected("sharing opening nonce is invalid")
    client_id = f"catchuparr-client-{nonce}"
    if episode.client_id != client_id:
        raise BorrowEpisodeRejected("sharing client differs from the persisted opening")

    lease_value = redis_text.get(f"catchuparr:recorder:{capability.channel_uuid}")
    if not isinstance(lease_value, str) or not lease_value:
        raise BorrowEpisodeRejected("recorder lease is unavailable")
    lease_digest = hashlib.sha256(lease_value.encode("utf-8")).hexdigest()
    if lease_digest != episode.lease_digest:
        raise BorrowEpisodeRejected("recorder lease changed")

    try:
        assigned_sources, current_catalog_generation = _native_source_catalog(
            capability.channel_uuid
        )
        if current_catalog_generation != capability.catalog_generation:
            raise BorrowEpisodeRejected("source catalog changed")
        observation = observe_native_connection(
            redis_text, capability.channel_uuid,
            catalog_generation=current_catalog_generation,
            assigned_sources=assigned_sources,
            observed_at=time.time(),
        )
    finally:
        try:
            from django.db import close_old_connections

            close_old_connections()
        except Exception:
            pass
    if (
        observation is None
        or observation.target.connection_identity != capability.connection_identity
        or not observation.ordinary_viewer_ids
    ):
        raise BorrowEpisodeRejected("native source has no current ordinary viewer authority")

    from apps.proxy.live_proxy.client_manager import ClientManager
    from apps.proxy.live_proxy.input.buffer import StreamBuffer

    # A private manager is created for this route request and is never inserted
    # into ProxyServer's channel maps. Its worker ID is an opaque local reader
    # identity, not the native source owner's ID.
    worker_id = f"catchuparr-reader-{uuid.uuid4().hex}"
    secret = _django_secret()
    return BorrowedReaderSession.open(
        capability=capability,
        capability_secret=secret,
        authenticated_user_id=capability.user_id,
        now=time.time(),
        episode=episode,
        lease_value=lease_value,
        catalog_generation=current_catalog_generation,
        ordinary_viewer_ids=observation.ordinary_viewer_ids,
        redis_text=redis_text,
        redis_binary=redis_binary,
        existing_manager=None,
        manager_type=ClientManager,
        worker_id=worker_id,
        buffer_type=StreamBuffer,
        assigned_sources=assigned_sources,
        client_id=client_id,
        client_ip="127.0.0.1",
        user_agent="Catchuparr read-only sharing reader",
        ledger=ledger,
    )


def _django_secret() -> bytes:
    from django.conf import settings

    secret = getattr(settings, "SECRET_KEY", "")
    if not isinstance(secret, str) or not secret:
        raise BorrowEpisodeRejected("sharing signing key is unavailable")
    return secret.encode("utf-8")
