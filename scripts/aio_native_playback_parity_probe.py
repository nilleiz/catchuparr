"""Focused real-Django parity probe for native and legacy archive routes."""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlsplit

def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def _lease_count(path):
    with sqlite3.connect(path) as database:
        exists = database.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            ("http_playback_sessions",),
        ).fetchone()
        if not exists:
            return 0
        return database.execute(
            "SELECT COUNT(*) FROM http_playback_sessions"
        ).fetchone()[0]


def _normalized_playlist(response):
    lines = response.content.decode("utf-8").splitlines()
    normalized = []
    for line in lines:
        if line and not line.startswith("#"):
            parts = urlsplit(line).path.strip("/").split("/")
            line = "/".join(parts[-2:])
        normalized.append(line)
    return tuple(normalized)


def probe():
    # Match the verified full AIO probe's plugin-manager checkout path before
    # importing the installed Catchuparr package.
    import sys

    sys.path.insert(0, "/data/plugins")

    from django.test import RequestFactory
    from django.utils import timezone

    from apps.accounts.models import User
    from apps.channels.models import Channel, ChannelProfile, ChannelProfileMembership
    from apps.epg.models import EPGData, ProgramData
    from apps.plugins.models import PluginConfig
    from django.urls import resolve
    from version import __version__

    from catchuparr import runtime, views
    from catchuparr.configuration import active_settings_path, load_active_configuration
    from catchuparr.engine.store import ArchiveStore
    from catchuparr.native_output_security import create_native_capability

    require(__version__ == "0.32.0", "Focused parity probe requires Dispatcharr 0.32.0")
    require(not Channel.objects.exists() and not User.objects.exists(),
            "Focused parity probe requires a clean disposable AIO database")

    user = User.objects.create_user(
        username="synthetic-parity-user",
        user_level=0,
        stream_limit=0,
        custom_properties={
            "allowed_networks": {
                "M3U_EPG": "127.0.0.0/8",
                "STREAMS": "127.0.0.0/8",
            },
        },
    )
    channel = Channel.objects.create(
        name="Synthetic parity channel", channel_number=1, user_level=0
    )
    profile = ChannelProfile.objects.create(name="Synthetic parity profile")
    ChannelProfileMembership.objects.update_or_create(
        channel_profile=profile,
        channel=channel,
        defaults={"enabled": True},
    )
    guide = EPGData.objects.create(name="Synthetic parity guide", tvg_id="parity")
    channel.epg_data = guide
    channel.save(update_fields=["epg_data"])

    archive_root = Path("/data/ci-native-parity-archive")
    archive_root.mkdir(parents=True, exist_ok=True)
    config = {
        "filter_config": (
            "version: 1\n"
            "profile: Synthetic parity profile\n"
            "rules:\n"
            "  - channels: {profile: all}\n"
        ),
        "archive_root": str(archive_root),
        "retention_hours": 12,
        "max_storage_gib": 1,
        "recording_enabled": False,
        "playback_user_id": user.id,
        "public_base_url": "http://127.0.0.1",
        "native_output_catchup": True,
    }
    PluginConfig.objects.create(
        key="catchuparr",
        name="Catchuparr",
        enabled=True,
        ever_enabled=True,
        settings=config,
    )
    runtime.bootstrap()
    applied = runtime.apply_configuration()
    require(applied.get("applied") is True,
            "Synthetic native playback configuration did not apply")
    active = load_active_configuration()
    require(active is not None and active.get("native_output_catchup") is True,
            "Native output did not activate in the disposable fixture")
    access = runtime.create_access_token(active)["access_token"]
    key_id = active["native_output_key_id"]
    capability = create_native_capability(
        active_settings_path(),
        key_id=key_id,
        mode="anonymous_admission",
        subject=user.id,
        channel=str(channel.uuid),
        profile=str(profile.id),
        nonce=uuid.uuid4().hex + uuid.uuid4().hex[:16],
    )

    store = ArchiveStore(archive_root)
    factory = RequestFactory()
    channel_id = str(channel.uuid)
    native_match = resolve("/catchuparr/native/archive")
    legacy_match = resolve("/catchuparr/archive")
    now = timezone.now().replace(microsecond=0)
    cases = (
        ("current", now - timedelta(seconds=30), now + timedelta(seconds=90),
         ((now - timedelta(seconds=30), now - timedelta(seconds=5)),)),
        ("completed", now - timedelta(minutes=3), now - timedelta(minutes=2),
         ((now - timedelta(minutes=3), now - timedelta(minutes=2)),)),
        ("gapped", now - timedelta(minutes=6), now - timedelta(minutes=5),
         ((now - timedelta(minutes=5, seconds=30), now - timedelta(minutes=5)),)),
        ("empty", now - timedelta(minutes=9), now - timedelta(minutes=8), ()),
    )
    for name, programme_start, programme_end, segments in cases:
        ProgramData.objects.create(
            epg=guide,
            title=f"Synthetic {name} programme",
            start_time=programme_start,
            end_time=programme_end,
        )
        for index, (segment_start, segment_end) in enumerate(segments):
            media = archive_root / f"{name}-{index}.ts"
            media.write_bytes(b"\x47" + bytes([index + 1]) * 187)
            store.add_segment(channel_id, media, segment_start, segment_end)

    def call(route_match, params, *, method="GET"):
        path = (
            "/catchuparr/native/archive"
            if route_match is native_match else "/catchuparr/archive"
        )
        request = factory.get(
            path,
            params,
            REMOTE_ADDR="127.0.0.1",
            HTTP_USER_AGENT="Synthetic parity probe/1",
        )
        request.method = method
        return route_match.func(request, **route_match.kwargs)

    outcomes = {}
    for name, programme_start, programme_end, _segments in cases:
        duration = int((programme_end - programme_start).total_seconds())
        params = {
            "channel_id": channel_id,
            "utc": str(programme_start.timestamp()),
            "duration": str(duration),
        }
        legacy = call(legacy_match, {**params, "token": access})
        native = call(native_match, {**params, "token": capability})
        require(legacy.status_code == native.status_code,
                f"{name} GET status differs: {legacy.status_code}/{native.status_code}")
        require(
            _normalized_playlist(legacy) == _normalized_playlist(native),
            f"{name} GET playlists differ beyond signed segment URL query strings",
        )
        if name == "empty":
            require(legacy.status_code == 404,
                    "Empty archive window must remain unavailable")
        else:
            require(legacy.status_code == 200,
                    f"{name} archive window did not produce a playlist")
        legacy_head = call(legacy_match, {**params, "token": access}, method="HEAD")
        after_legacy_head = _lease_count(archive_root / "archive.sqlite3")
        native_head = call(native_match, {**params, "token": capability}, method="HEAD")
        after_native_head = _lease_count(archive_root / "archive.sqlite3")
        require(legacy_head.status_code == native_head.status_code,
                f"{name} HEAD status differs")
        require(not legacy_head.content and not native_head.content,
                f"{name} HEAD returned a body")
        require(after_legacy_head == after_native_head,
                f"{name} native HEAD created a playback lease")
        outcomes[name] = {
            "get_status": legacy.status_code,
            "head_status": legacy_head.status_code,
            "head_created_lease": False,
        }

    missing_legacy = call(
        legacy_match,
        {"channel_id": channel_id, "utc": str(now.timestamp()), "duration": "1"},
    )
    tampered = capability[:-1] + ("A" if capability[-1] != "A" else "B")
    denied_native = call(
        native_match,
        {"channel_id": channel_id, "utc": str(now.timestamp()),
         "duration": "1", "token": tampered},
    )
    require(missing_legacy.status_code == 401,
            "Legacy playback must reject a missing access token")
    require(denied_native.status_code == 403,
            "Native playback must reject an invalid signed capability")
    print(json.dumps({
        "status": "pass",
        "dispatcharr_version": __version__,
        "cases": outcomes,
        "authorization": {
            "legacy_missing_token": missing_legacy.status_code,
            "native_invalid_capability": denied_native.status_code,
        },
        "fixture": "synthetic; one user/channel/guide; no provider sources",
    }, sort_keys=True))


probe()
