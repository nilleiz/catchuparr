"""Run through manage.py shell in an empty, disposable Dispatcharr AIO."""

import inspect
import json
import os
import signal
import sqlite3
import sys
import threading
import time
import uuid
from datetime import timedelta
from pathlib import Path
from urllib.parse import parse_qs, quote, urlsplit

from django.core.cache import cache
from django.test import RequestFactory
from django.utils import timezone


def require(condition, message="Integration check failed"):
    if not condition:
        raise RuntimeError(message)


def require_native_stream_runtime():
    """Verify the normal app-ready path created one full native runtime."""
    from apps.proxy.live_proxy.runtime import should_run_live_proxy_runtime
    from apps.proxy.live_proxy.server import ProxyServer
    from dispatcharr.db.process_label import get_process_role
    from django.apps import apps

    require(
        get_process_role() == "gunicorn" and should_run_live_proxy_runtime(),
        "Synthetic shell did not select Dispatcharr's stream-worker runtime",
    )
    native_server = ProxyServer._instance
    require(
        isinstance(native_server, ProxyServer),
        "ProxyConfig.ready did not create the native ProxyServer singleton",
    )
    proxy_config = apps.get_app_config("proxy")
    require(
        getattr(proxy_config, "live_proxy", None) is native_server,
        "ProxyConfig.ready and ProxyServer do not share the same singleton",
    )
    require(
        ProxyServer.get_instance() is native_server,
        "Native ProxyServer singleton changed after app startup",
    )
    thread_names = [thread.name for thread in threading.enumerate()]
    require(
        thread_names.count("ts-proxy-cleanup") == 1
        and thread_names.count("redis-event-listener") == 1,
        "Native stream-worker cleanup and event-listener threads did not start once",
    )
    return native_server


def require_native_runtime_drained(native_server):
    """Wait naturally for every native channel thread to exit before process exit."""
    deadline = time.monotonic() + 15
    while (
        native_server.stream_managers
        or native_server.client_managers
        or native_server._live_stream_managers
    ) and time.monotonic() < deadline:
        from gevent import sleep

        sleep(0.05)
    require(
        not native_server.stream_managers
        and not native_server.client_managers
        and not native_server._live_stream_managers,
        "Synthetic probe returned before native channel threads drained naturally",
    )


def probe_source_configuration(channel, root):
    """Exercise real ORM catalogs, published settings, and native Redis counters."""
    from apps.channels.models import (
        ChannelProfile,
        ChannelProfileMembership,
        ChannelStream,
        Stream,
    )
    from apps.m3u.connection_pool import (
        profile_connections_key,
        profile_credential_release_key,
        release_profile_slot,
        reserve_profile_slot,
    )
    from apps.m3u.models import M3UAccount, M3UAccountProfile
    from apps.proxy.live_proxy.server import ProxyServer
    from dispatcharr.utils import get_client_ip

    from catchuparr.adapters.recorder_proxy import (
        _core_api_compatibility_issues,
        _CredentialMarkerRedisFacade,
        _release_worker_reservation,
        core_api_supported,
        make_worker_id,
        reservation_credential_marker_key,
        write_worker_record,
    )
    from catchuparr.configuration import (
        apply_configuration,
        load_active_configuration,
        source_catalog,
    )
    from catchuparr.recorder_proxy import ranked_source_candidates

    require(callable(get_client_ip))
    if not core_api_supported():
        raise RuntimeError("Real source proxy API rejected: "
                           + "; ".join(_core_api_compatibility_issues()))
    first = M3UAccount.objects.create(name="Synthetic source A", max_streams=3)
    second = M3UAccount.objects.create(name="Synthetic source B", max_streams=3)
    source_a = Stream.objects.create(
        name="Source A", url="http://127.0.0.1:1/a.ts", m3u_account=first,
    )
    source_b = Stream.objects.create(
        name="Source B", url="http://127.0.0.1:1/b.ts", m3u_account=second,
    )
    Stream.objects.create(
        name="Unassigned source", url="http://127.0.0.1:1/unused.ts", m3u_account=second,
    )
    ChannelStream.objects.create(channel=channel, stream=source_a, order=0)
    ChannelStream.objects.create(channel=channel, stream=source_b, order=1)
    profile_name = "Synthetic source catalog profile"
    channel_profile = ChannelProfile.objects.create(name=profile_name)
    ChannelProfileMembership.objects.filter(channel_profile=channel_profile).update(
        enabled=False
    )
    ChannelProfileMembership.objects.update_or_create(
        channel_profile=channel_profile,
        channel=channel,
        defaults={"enabled": True},
    )
    catalog = source_catalog()
    require([row["id"] for row in catalog.streams_by_channel[str(channel.uuid)]]
            == [str(source_a.id), str(source_b.id)], "Catalog must preserve assigned order")
    settings = {
        "filter_config": (
            "version: 1\n"
            f"profile: {profile_name}\n"
            "timezone: UTC\n"
            "schedule:\n"
            "  monday:\n"
            "    - {start: '00:00', end: '24:00'}\n"
            "rules:\n"
            "  - channels: {profile: all}\n"
            "    include: [Synthetic source B]\n"
        ),
        "archive_root": str(root),
        "retention_hours": 1, "max_storage_gib": 1,
    }
    active_path = root / "synthetic-active-settings.json"
    apply_configuration(settings, active_path=active_path)
    active = load_active_configuration(active_path)
    from aio_051_configuration_probe import validate_active_v7_snapshot

    validate_active_v7_snapshot(
        active, json.loads(active_path.read_text(encoding="utf-8")),
        (str(channel.uuid),),
    )
    require(active["recording_schedule"] == {
        "timezone": "UTC",
        "channels": {
            str(channel.uuid): {"mode": "weekly", "intervals": [[0, 1440]]},
        },
    }, "Apply must persist a normalized per-channel schedule by stable UUID")
    require(set(active["source_policies"]) == {str(channel.uuid)},
            "Wildcard must persist policies only for enabled archive channels")
    require([row["id"] for row in ranked_source_candidates(str(channel.uuid), active)]
            == [str(source_b.id)], "Include policy must reject assigned source A")
    previous = active_path.read_bytes()
    invalid = dict(
        settings,
        filter_config=(
            "version: 1\n"
            f"profile: {profile_name}\n"
            "rules:\n"
            "  - channels: {profile: all}\n"
            "    include: [Missing source]\n"
        ),
    )
    try:
        apply_configuration(invalid, active_path=active_path)
    except ValueError:
        pass
    else:
        raise RuntimeError("Unknown source must fail Apply")
    require(active_path.read_bytes() == previous, "Rejected Apply must preserve active snapshot")
    apply_configuration(
        dict(
            settings,
            filter_config=(
                "version: 1\n"
                f"profile: {profile_name}\n"
                "rules:\n"
                "  - channels: {profile: all}\n"
                "    exclude: []\n"
            ),
        ),
        active_path=active_path,
    )
    require(ranked_source_candidates(str(channel.uuid), load_active_configuration(active_path))
            is None, "Cleared rules must restore the shared channel route")

    # Native release implementations differ between .31 and .32. Seed a shared
    # credential ledger to check each private marker against the real helpers,
    # including .32 WATCH/pipeline operations, in both teardown orders.
    redis_client = ProxyServer.get_instance().redis_client
    require(redis_client is not None, "Source tests require disposable Redis")
    profile = M3UAccountProfile.objects.create(
        m3u_account=first, name="Synthetic capacity", max_streams=3,
        is_active=True, search_pattern="", replace_pattern="",
    )
    profile_key = profile_connections_key(profile.id)
    marker_key = profile_credential_release_key(profile.id)
    credential_key = "catchuparr:ci:synthetic-credential-count"
    scenarios = (
        (False, (0, 1)),
        (True, ("live", 0, 1)),
        (True, (0, "live", 1)),
        (True, (1, 0, "live")),
    )
    for with_live, order in scenarios:
        redis_client.delete(profile_key, marker_key, credential_key)
        workers = []
        private_markers = []
        if with_live:
            require(reserve_profile_slot(profile, redis_client)[0], "Native slot must reserve")
        for index in range(2):
            reservation_id = uuid.uuid4().hex
            worker_id = make_worker_id(str(channel.uuid), str(source_a.id), "synthetic", 1,
                                       reservation_id)
            facade = _CredentialMarkerRedisFacade(redis_client, profile.id, reservation_id)
            require(reserve_profile_slot(profile, facade)[0], "Archive slot must reserve")
            facade.set(marker_key, credential_key)
            write_worker_record(redis_client, worker_id, {
                "profile_id": profile.id, "reservation_id": reservation_id,
                "reservation_state": "reserved", "state": "active",
            })
            workers.append(worker_id)
            private_markers.append(reservation_credential_marker_key(reservation_id))
        if with_live:
            redis_client.set(marker_key, credential_key)
        else:
            require(redis_client.get(marker_key) is None,
                    "Recorder-only scenario must not seed a native release marker")
        total = 2 + int(with_live)
        redis_client.set(credential_key, total)
        if with_live:
            require(not reserve_profile_slot(profile, redis_client)[0],
                    "Archive must respect native profile capacity")
        live_pending = with_live
        for index, slot in enumerate(order):
            if slot == "live":
                release_profile_slot(profile.id, redis_client)
                live_pending = False
            else:
                require(_release_worker_reservation(redis_client, workers[slot]),
                        "Exact archive ledger reservation must release")
                require(redis_client.get(private_markers[slot]) is None,
                        "Released archive reservation must remove its private marker")
                require(not _release_worker_reservation(redis_client, workers[slot]),
                        "Duplicate archive release must not consume another slot")
            remaining = total - index - 1
            require(int(redis_client.get(profile_key) or 0) == remaining,
                    "Teardown must release precisely one profile slot")
            require(int(redis_client.get(credential_key) or 0) == remaining,
                    "Teardown must release precisely one credential slot")
            if live_pending:
                require(redis_client.get(marker_key) is not None,
                        "Archive teardown must preserve a live reservation's release marker")
        require(redis_client.get(marker_key) is None,
                "Final teardown must remove the native release marker")
    redis_client.delete(credential_key)
    print("AIO source catalog, atomic Apply, policy ranking and native pool checks passed")


def probe():
    if os.environ.get("CATCHUPARR_INTEGRATION_TEST") != "1":
        raise RuntimeError("This probe requires a disposable integration container")
    probe_started = time.monotonic()

    def phase(name, state):
        print(json.dumps({
            "probe": "aio_integration",
            "phase": name,
            "state": state,
            "elapsed_seconds": round(time.monotonic() - probe_started, 1),
        }, sort_keys=True), flush=True)

    phase("runtime_setup", "started")
    sys.path.insert(0, "/data/plugins")
    sys.path.insert(0, "/tmp")
    from apps.accounts.models import User
    from apps.channels.models import (
        Channel,
        ChannelProfile,
        ChannelProfileMembership,
        ChannelStream,
        Stream,
    )
    from apps.epg.models import EPGData, ProgramData
    from apps.m3u.models import M3UAccount
    from apps.output import views as output
    from apps.plugins.models import PluginConfig
    from apps.timeshift import views as timeshift
    from version import __version__

    from catchuparr import runtime, views
    from catchuparr.compatibility import is_supported_dispatcharr_version
    from catchuparr.engine.store import ArchiveStore

    native_server = require_native_stream_runtime()
    phase("runtime_setup", "complete")

    # Refuse restored Dev/production databases, even if the flag was misapplied.
    require(not Channel.objects.exists(), "Integration database must have no channels")
    require(not User.objects.exists(), "Integration database must have no users")
    require(is_supported_dispatcharr_version(__version__), "Unsupported Dispatcharr version")
    expected = {
        output._xc_channel_entry: (
            "channel", "channel_num_map", "_get_default_group_id",
            "_logo_url_prefix", "_logo_url_suffix", "catchup_allowed",
        ),
        output.xc_get_epg: ("request", "user", "short"),
        output.generate_m3u: ("request", "profile_name", "user"),
        timeshift._serve_catchup: (
            "request", "user", "channel", "timestamp", "client_duration_hint",
        ),
    }
    for function, parameters in expected.items():
        require(tuple(inspect.signature(function).parameters) == parameters,
                f"Core signature changed: {function.__name__}")

    # Stop only this disposable container's Beat before enabling fixture jobs.
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            args = (proc / "cmdline").read_bytes().split(b"\0")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        if b"beat" in args and any(b"celery" in arg for arg in args):
            os.kill(int(proc.name), signal.SIGSTOP)

    xc_password = uuid.uuid4().hex
    user = User.objects.create_user(
        username="synthetic-player", user_level=0, stream_limit=1,
        custom_properties={"xc_password": xc_password},
    )
    channel = Channel.objects.create(name="Synthetic", channel_number=1, user_level=0)
    private_channel = Channel.objects.create(name="Private", channel_number=2, user_level=10)
    closed_channel = Channel.objects.create(name="Closed minute", channel_number=3, user_level=0)
    no_guide_channel = Channel.objects.create(name="Missing guide", channel_number=4, user_level=0)
    long_segment_channel = Channel.objects.create(name="Long GOP", channel_number=5, user_level=0)
    continuous_channel = Channel.objects.create(
        name="Continuous playback", channel_number=6, user_level=0,
    )
    stopped_channel = Channel.objects.create(
        name="Stopped playback", channel_number=7, user_level=0,
    )
    channel_profile = ChannelProfile.objects.create(name="Synthetic playback profile")
    ChannelProfileMembership.objects.filter(channel_profile=channel_profile).update(
        enabled=False
    )
    for selected_channel in (
        channel, closed_channel, no_guide_channel, long_segment_channel,
        continuous_channel, stopped_channel,
    ):
        ChannelProfileMembership.objects.update_or_create(
            channel_profile=channel_profile,
            channel=selected_channel,
            defaults={"enabled": True},
        )
    archive_only_account = M3UAccount.objects.create(
        name="Synthetic archive-only channel source", max_streams=1,
    )
    archive_only_stream = Stream.objects.create(
        name="Synthetic archive-only channel source",
        url="http://127.0.0.1:1/archive-only.ts",
        m3u_account=archive_only_account,
    )
    ChannelStream.objects.create(
        channel=no_guide_channel, stream=archive_only_stream, order=0,
    )
    root = Path("/data/ci-archive")
    root.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(root / "archive.sqlite3") as database:
        database.execute("""CREATE TABLE http_playback_sessions (
            lease_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, channel_id TEXT NOT NULL,
            start_utc REAL NOT NULL, end_utc REAL NOT NULL, expires_at REAL NOT NULL
        )""")
    PluginConfig.objects.create(
        key="catchuparr", name="Catchuparr", enabled=True, ever_enabled=True,
        settings={
            "filter_config": (
                "version: 1\n"
                "profile: Synthetic playback profile\n"
                "rules:\n"
                "  - channels: {profile: all}\n"
            ),
            "archive_root": str(root),
            "retention_hours": 1,
            "max_storage_gib": 1,
            "recording_enabled": True,
            "log_level": "INFO",
            "playback_user_id": user.id,
            "public_base_url": "https://media.example.test/dispatcharr",
        },
    )
    runtime.bootstrap()
    runtime.bootstrap()
    initial_settings = PluginConfig.objects.get(key="catchuparr").settings
    require(runtime.validate_configuration(initial_settings)["valid"],
            "Synthetic YAML profile config did not validate")
    runtime.apply_configuration()
    from catchuparr.configuration import active_settings_path, load_active_configuration

    active = load_active_configuration()
    from aio_051_configuration_probe import validate_active_v7_snapshot

    require(active is not None, "One Apply must make the active snapshot readable")
    active_path = active_settings_path()
    # Both overview routes must consume the committed snapshot without changing
    # configuration/control files or initializing additional state.
    from catchuparr.plugin import Plugin
    from catchuparr.recorder_control import control_state_path

    control_path = control_state_path(active_path)
    active_before = active_path.read_bytes()
    control_before = control_path.read_bytes()
    directory_before = set(active_path.parent.iterdir())
    overview_plugin = Plugin.__new__(Plugin)
    current_result = overview_plugin.run("current_recordings", {}, {})
    schedule_result = overview_plugin.run("today_schedule", {}, {})
    require(isinstance(current_result, dict) and isinstance(current_result.get("channels"), list)
            and isinstance(current_result.get("summary"), dict),
            "Current recordings action did not return the shared structured response format")
    require(all({"channel_uuid", "number", "name", "state", "mode", "reason", "source"}
                <= set(row) for row in current_result["channels"]),
            "Current recordings action omitted expected channel state fields")
    require("Current recordings" not in current_result.get("message", "")
            and "Problems:" not in current_result.get("message", "")
            and "Last confirmed progress" not in current_result.get("message", ""),
            "Current recordings action returned the expanded diagnostic message")
    require(isinstance(schedule_result, dict) and isinstance(schedule_result.get("channels"), list)
            and isinstance(schedule_result.get("summary"), dict),
            "Today's schedule action did not return the shared structured response format")
    require(all({"channel_uuid", "number", "name", "windows"} <= set(row)
                and isinstance(row["windows"], list) for row in schedule_result["channels"]),
            "Today's schedule action omitted selected channels or window lists")
    require("Today's recording schedule" not in schedule_result.get("message", "")
            and "Pause state unknown" not in schedule_result.get("message", "")
            and "No recording windows planned" not in schedule_result.get("message", ""),
            "Today's schedule action returned the expanded schedule message")
    require(active_path.read_bytes() == active_before and control_path.read_bytes() == control_before
            and set(active_path.parent.iterdir()) == directory_before,
            "Read-only overview action changed Catchuparr configuration/control files")
    persisted = json.loads(active_path.read_text(encoding="utf-8"))
    validate_active_v7_snapshot(
        active, persisted, tuple(active.get("channel_uuids", "").splitlines()),
    )
    require(
        active.get("recording_enabled") is True and active.get("log_level") == "INFO",
        "One Apply must activate the global recorder and logging settings",
    )
    from django.urls import resolve

    require(resolve("/catchuparr/m3u").url_name == "catchuparr-m3u")
    import dispatcharr.urls

    require(sum(getattr(item, "name", "") == "catchuparr-m3u"
                for item in dispatcharr.urls.urlpatterns) == 1)

    store = ArchiveStore(root)
    source = root / "fixture.ts"
    source.write_bytes(b"\x47" + bytes(187))
    start = timezone.now().replace(microsecond=0) - timedelta(seconds=12)
    store.add_segment(str(channel.uuid), source, start, start + timedelta(seconds=6))
    source.write_bytes((b"\x47" + bytes([1]) * 187) * 2)
    store.add_segment(str(channel.uuid), source, start + timedelta(seconds=6),
                      start + timedelta(seconds=12))
    live_guide = EPGData.objects.create(name="Synthetic playback guide", tvg_id="synthetic")
    channel.epg_data = live_guide
    channel.save(update_fields=["epg_data"])
    ProgramData.objects.create(epg=live_guide, title="Synthetic current",
                               start_time=start, end_time=start + timedelta(seconds=12))
    ProgramData.objects.create(epg=live_guide, title="Synthetic next",
                               start_time=start + timedelta(seconds=12),
                               end_time=start + timedelta(seconds=18))
    ProgramData.objects.create(epg=live_guide, title="Synthetic third",
                               start_time=start + timedelta(seconds=18),
                               end_time=start + timedelta(seconds=72))
    source.write_bytes(b"\x47" + bytes([3]) * 187)
    store.add_segment(str(no_guide_channel.uuid), source, start,
                      start + timedelta(seconds=6))
    long_guide = EPGData.objects.create(name="Long segment guide", tvg_id="long-gop")
    long_segment_channel.epg_data = long_guide
    long_segment_channel.save(update_fields=["epg_data"])
    long_start = start - timedelta(minutes=2)
    ProgramData.objects.create(epg=long_guide, title="Long segment",
                               start_time=long_start,
                               end_time=long_start + timedelta(seconds=16))
    source.write_bytes(b"\x47" + bytes([4]) * 187)
    store.add_segment(str(long_segment_channel.uuid), source, long_start,
                      long_start + timedelta(seconds=16))
    source.write_bytes(b"\x47" + bytes([3]) * 187)
    closed_start = start - timedelta(minutes=3)
    guide = EPGData.objects.create(name="Closed synthetic guide", tvg_id="closed-minute")
    closed_channel.epg_data = guide
    closed_channel.save(update_fields=["epg_data"])
    ProgramData.objects.create(epg=guide, title="Closed minute", start_time=closed_start,
                               end_time=closed_start + timedelta(minutes=1))
    source.write_bytes(b"\x47" + bytes([2]) * 187)
    for offset in range(0, 60, 10):
        store.add_segment(str(closed_channel.uuid), source,
                          closed_start + timedelta(seconds=offset),
                          closed_start + timedelta(seconds=offset + 10))
    continuous_guide = EPGData.objects.create(
        name="Synthetic continuous guide", tvg_id="continuous",
    )
    continuous_channel.epg_data = continuous_guide
    continuous_channel.save(update_fields=["epg_data"])
    for offset, duration, title in (
        (0, 6, "Synthetic continuous first"),
        (6, 6, "Synthetic continuous second"),
        (12, 6, "Synthetic continuous third"),
        (18, 54, "Synthetic continuous fourth"),
    ):
        ProgramData.objects.create(
            epg=continuous_guide, title=title,
            start_time=start + timedelta(seconds=offset),
            end_time=start + timedelta(seconds=offset + duration),
        )
    continuous_path = root / "continuous-initial.ts"
    continuous_path.write_bytes(b"\x47" + bytes([10]) * 187)
    store.add_segment(
        str(continuous_channel.uuid), continuous_path,
        start + timedelta(seconds=6), start + timedelta(seconds=12),
    )
    stopped_start = timezone.now().replace(microsecond=0) - timedelta(minutes=15)
    stopped_path = root / "stopped-playback.ts"
    stopped_path.write_bytes(b"\x47" + bytes([11]) * 187)
    store.add_segment(
        str(stopped_channel.uuid), stopped_path,
        stopped_start, stopped_start + timedelta(seconds=6),
    )
    phase("archive_routes", "started")
    token_result = runtime.create_access_token(active)
    token = token_result["access_token"]
    encoded_token = quote(token, safe="")
    expected_playlist_url = (
        "https://media.example.test/dispatcharr/catchuparr/m3u"
        f"?access_token={encoded_token}"
    )
    expected_xmltv_url = (
        "https://media.example.test/dispatcharr/catchuparr/xmltv"
        f"?access_token={encoded_token}"
    )
    require(
        token_result["playlist_url"] == expected_playlist_url
        and token_result["xmltv_url"] == expected_xmltv_url
        and token_result["message"]
        == f"M3U playlist URL: {expected_playlist_url}\nXMLTV EPG URL: {expected_xmltv_url}",
        "Token confirmation did not return both labeled links for the same token",
    )
    factory = RequestFactory()

    def request(path, params=None, **headers):
        return factory.get(path, params or {}, HTTP_HOST="localhost", **headers)

    def close_hls_session(response, selected_channel):
        urls = [
            line for line in response.content.decode().splitlines()
            if line and not line.startswith("#")
        ]
        if not urls:
            return
        lease = parse_qs(urlsplit(urls[-1]).query).get("lease", [None])[0]
        if lease:
            service = views._archive_service(
                request("/catchuparr/archive", params), user, runtime.load_config(),
            )
            require(service.end_session(token, str(selected_channel.uuid), lease),
                    "Synthetic HLS playback lease did not close")

    # Supported cores (0.32+) use the UTC query parameter, including 0.33.
    time_key = "utc"
    xc_params = {"username": user.username, "password": xc_password,
                 "stream": str(channel.id), "duration": "1"}
    params = {"access_token": token, "channel_id": str(channel.uuid),
              "utc": str(start.timestamp()), "duration": "12"}
    first_xc = timeshift.timeshift_proxy_query(request(
        "/streaming/timeshift.php",
        dict(xc_params, stream=str(closed_channel.id),
             **{time_key: str(int(closed_start.timestamp()))}),
        HTTP_RANGE="bytes=0-187", HTTP_USER_AGENT="Catchuparr synthetic integration",
    ))
    require(first_xc.status_code == 206,
            f"XC before first HLS request must migrate legacy sessions: {first_xc.status_code}")
    from catchuparr.xc_runtime import _ts_service

    first_xc_service = _ts_service(root, timeshift)
    with first_xc_service._connect() as database:
        first_xc_leases = database.execute(
            "SELECT lease_id FROM ts_playback_sessions WHERE user_id=? AND channel_id=?",
            (str(user.id), str(closed_channel.uuid)),
        ).fetchall()
    require(len(first_xc_leases) == 1,
            "Synthetic XC response must own exactly one stream-limit session")
    blocked_no_guide = views.archive_view(request(
        "/catchuparr/archive", dict(params, channel_id=str(no_guide_channel.uuid))
    ))
    require(
        blocked_no_guide.status_code == 403
        and blocked_no_guide.content.strip() == b"stream limit exceeded",
        "One-slot limit must block HLS while the synthetic XC response is active",
    )
    try:
        require(b"".join(first_xc.streaming_content) == b"\x47" + bytes([2]) * 187)
    finally:
        first_xc.close()
    for (lease_id,) in first_xc_leases:
        with first_xc_service._connect() as database:
            still_active = database.execute(
                "SELECT 1 FROM ts_playback_sessions WHERE lease_id=? AND user_id=? "
                "AND channel_id=?",
                (lease_id, str(user.id), str(closed_channel.uuid)),
            ).fetchone()
        if still_active is not None:
            require(first_xc_service.end_user_session(
                user.id, str(closed_channel.uuid), lease_id,
            ), "Finished synthetic XC session did not close")
    with first_xc_service._connect() as database:
        remaining_first_xc = database.execute(
            "SELECT COUNT(*) FROM ts_playback_sessions WHERE user_id=? AND channel_id=?",
            (str(user.id), str(closed_channel.uuid)),
        ).fetchone()[0]
    require(remaining_first_xc == 0,
            "Synthetic XC disconnect left a stream-limit session behind")

    require(views.m3u_view(request("/catchuparr/m3u")).status_code == 401)
    playlist = views.m3u_view(request("/catchuparr/m3u", {"access_token": token}))
    require(playlist.status_code == 200, f"Playlist status {playlist.status_code}")
    xmltv = views.xmltv_view(request("/catchuparr/xmltv", {"access_token": token}))
    require(xmltv.status_code == 200, f"XMLTV status {xmltv.status_code}")
    text = playlist.content.decode()
    require('catchup-timezone="UTC"' in text)
    require("duration={duration}" in text)
    require(str(channel.uuid) in text and str(private_channel.uuid) not in text)
    expired = views.archive_view(request(
        "/catchuparr/archive",
        dict(params, utc=str((start - timedelta(hours=2)).timestamp())),
    ))
    require(expired.status_code == 404,
            "Expired start-over must not substitute a later programme")

    missing_guide = views.archive_view(request(
        "/catchuparr/archive", dict(params, channel_id=str(no_guide_channel.uuid))
    ))
    missing_guide_service = views._archive_service(
        request("/catchuparr/archive", params), user, runtime.load_config(),
    )
    missing_guide_authorized = missing_guide_service.authorize_user_channel(
        str(user.id), str(no_guide_channel.uuid),
    )
    body_classes = {
        b"forbidden": "forbidden",
        b"stream limit exceeded": "stream_limit",
        b"no archived segments in requested range": "no_segments",
        b"archive temporarily unavailable": "archive_unavailable",
    }
    missing_guide_body_class = body_classes.get(
        missing_guide.content.strip(), "other",
    )
    require(
        no_guide_channel.epg_data_id is None
        and ChannelStream.objects.filter(channel=no_guide_channel).exists()
        and missing_guide_authorized
        and missing_guide.status_code == 200
        and "application/vnd.apple.mpegurl" in missing_guide.headers.get("Content-Type", "")
        and b"#EXTINF:" in missing_guide.content,
        "Covered no-EPG archive route failed: "
        f"status={missing_guide.status_code}, authorized={missing_guide_authorized}, "
        f"body={missing_guide_body_class}",
    )
    close_hls_session(missing_guide, no_guide_channel)
    stopped_playlist = views.archive_view(request(
        "/catchuparr/archive",
        dict(params, channel_id=str(stopped_channel.uuid),
             utc=str(stopped_start.timestamp()), duration="6"),
    ))
    require(stopped_playlist.status_code == 200
            and b"#EXT-X-ENDLIST" in stopped_playlist.content,
            "A stale recorder edge must produce a finite HLS playlist")
    close_hls_session(stopped_playlist, stopped_channel)
    store.save_program_snapshot(
        str(no_guide_channel.uuid), start, start + timedelta(seconds=6),
        "Archived before guide refresh",
    )
    restored_history = views.archive_view(request(
        "/catchuparr/archive",
        dict(params, channel_id=str(no_guide_channel.uuid), duration="6"),
    ))
    require(restored_history.status_code == 200
            and restored_history.content.count(b"#EXTINF:") == 1,
            "Historical snapshot advertised in XMLTV must remain playable")
    history_url = next(line for line in restored_history.content.decode().splitlines()
                       if line and not line.startswith("#"))
    history_lease = parse_qs(urlsplit(history_url).query)["lease"][0]
    history_service = views._archive_service(request("/catchuparr/archive", params), user,
                                             runtime.load_config())
    require(history_service.end_session(token, str(no_guide_channel.uuid), history_lease))
    long_segment = views.archive_view(request(
        "/catchuparr/archive",
        dict(params, channel_id=str(long_segment_channel.uuid),
             utc=str(int(long_start.timestamp())), duration="16"),
    ))
    require(long_segment.status_code == 200,
            "A delayed 16-second keyframe must not make HLS return 503")
    long_segment_url = next(line for line in long_segment.content.decode().splitlines()
                            if line and not line.startswith("#"))
    long_lease = parse_qs(urlsplit(long_segment_url).query)["lease"][0]
    service = views._archive_service(request("/catchuparr/archive", params), user,
                                     runtime.load_config())
    require(service.end_session(token, str(long_segment_channel.uuid), long_lease))
    from core.utils import RedisClient

    redis_client = RedisClient.get_client()
    hls_recorder_lease = f"catchuparr:recorder:{channel.uuid}"
    redis_client.set(hls_recorder_lease, "synthetic-active", ex=30)
    archive = views.archive_view(request("/catchuparr/archive", params))
    require(archive.status_code == 200, f"Archive status {archive.status_code}")
    require(archive.content.count(b"#EXTINF:") == 2
            and b"#EXT-X-ENDLIST" not in archive.content,
            "Growing playback must begin with indexed media and stay open")
    require(b"#EXT-X-START:TIME-OFFSET=" in archive.content,
            "Start-over playlist must mark the requested playback origin")
    segment_url = next(line for line in archive.content.decode().splitlines()
                       if line and not line.startswith("#"))
    parts = urlsplit(segment_url)
    query = {key: values[0] for key, values in parse_qs(parts.query).items()}
    path_parts = parts.path.rstrip("/").split("/")
    ranged = views.segment_view(request(parts.path, query, HTTP_RANGE="bytes=0-"),
                                path_parts[-2], path_parts[-1])
    require(ranged.status_code == 206 and ranged["Content-Range"].startswith("bytes 0-"))
    tail_url = [line for line in archive.content.decode().splitlines()
                if line and not line.startswith("#")][-1]
    tail_parts = urlsplit(tail_url)
    tail_query = {key: values[0] for key, values in parse_qs(tail_parts.query).items()}
    tail_path = tail_parts.path.rstrip("/").split("/")
    tail = views.segment_view(request(tail_parts.path, tail_query, HTTP_RANGE="bytes=0-"),
                              tail_path[-2], tail_path[-1])
    require(tail.status_code == 206 and len(tail.content) == 376,
            "Full Range GET of the published tail must remain readable")
    waiting = views.archive_view(request("/catchuparr/archive", params))
    require(waiting.status_code == 200 and waiting.content == archive.content,
            "Tail prefetch must not alter the programme playlist")
    for index, offset in enumerate((12, 18, 24)):
        append_path = root / f"continuous-append-{index}.ts"
        append_path.write_bytes(b"\x47" + bytes([20 + index]) * 187)
        store.add_segment(
            str(channel.uuid), append_path,
            start + timedelta(seconds=offset),
            start + timedelta(seconds=offset + 6),
        )
    bounded = views.archive_view(request("/catchuparr/archive", params))
    require(bounded.status_code == 200
            and bounded.content.count(b"#EXTINF:") == 5
            and b"#EXT-X-ENDLIST" not in bounded.content,
            "HLS reload must append media across two programme boundaries")
    require(service.end_session(token, str(channel.uuid), query["lease"]))
    seek_params = dict(params, utc=str(int(start.timestamp()) + 6))
    seek = views.archive_view(request("/catchuparr/archive", seek_params))
    require(seek.status_code == 200, f"HLS seek status {seek.status_code}")
    require(seek.content.count(b"#EXTINF:") == 4,
            "A seek must keep following indexed media across later programmes")
    seek_segment = next(line for line in seek.content.decode().splitlines()
                        if line and not line.startswith("#"))
    seek_lease = parse_qs(urlsplit(seek_segment).query)["lease"][0]
    require(service.end_session(token, str(channel.uuid), seek_lease))
    redis_client.delete(hls_recorder_lease)

    xc_playlist = output.generate_m3u(request("/get.php", xc_params), user=user)
    require(xc_playlist.status_code == 200)
    xc_text = xc_playlist.content.decode()
    require('catchup="default"' in xc_text)
    require("duration={duration:60}" in xc_text)
    require(time_key + "={utc}" in xc_text)

    def xc_playback(start_epoch, stream_id=channel.id, duration="1", *,
                    consume=True, remote_addr="127.0.0.40",
                    range_header="bytes=0-187", continuous=False):
        selected = dict(xc_params, stream=str(stream_id),
                        **{time_key: str(int(start_epoch))})
        if continuous:
            selected["playback"] = "continuous"
        if duration is None:
            selected.pop("duration")
        else:
            selected["duration"] = duration
        result = timeshift.timeshift_proxy_query(request(
            "/streaming/timeshift.php", selected, HTTP_RANGE=range_header,
            HTTP_USER_AGENT="Catchuparr synthetic integration",
            REMOTE_ADDR=remote_addr,
        ))
        require(result.status_code == 206, f"XC range status {result.status_code}")
        if not consume:
            return result
        try:
            return b"".join(result.streaming_content)
        finally:
            result.close()

    from aio_xc_timeline_probe import probe_xc_timeline

    probe_xc_timeline(
        root=root, store=store, channel=channel, start=start,
        timeshift=timeshift, request=request, xc_params=xc_params, time_key=time_key,
    )

    # Run Stats' HLS admission while the synthetic one-slot user has no
    # outstanding native TS session.  The later XC checks intentionally open
    # a live session, which correctly occupies that same stream-limit slot.
    sys.path.insert(0, "/tmp")
    from aio_stats_gevent_probe import run_actual_gevent_probe
    from aio_stats_probe import probe_actual_stats

    run_actual_gevent_probe()
    probe_actual_stats(
        root=root,
        request=request,
        user=user,
        channel=channel,
        token=token,
        start=start,
        params=params,
        xc_playback=xc_playback,
    )
    from aio_native_output_probe import probe_native_output

    probe_native_output(
        root=root,
        user=user,
        channel=channel,
        channel_profile=channel_profile,
        start=start,
        runtime=runtime,
        request=request,
    )

    require(
        xc_playback(start.timestamp(), no_guide_channel.id)
        == b"\x47" + bytes([3]) * 187,
        "XC playback must not require an EPG entry when local media is covered",
    )

    def end_fixture_xc_session(selected_channel, selected_start, *, required=False):
        # A Range request for a still-running programme intentionally retains
        # its logical session for seeking. Stop that exact test-owned session
        # before admitting another device under the one-slot account.
        from catchuparr.xc_runtime import _ts_service

        service = _ts_service(root, timeshift)
        with service._connect() as db:
            rows = db.execute(
                "SELECT lease_id FROM ts_playback_sessions "
                "WHERE user_id=? AND channel_id=? AND start_utc=? AND active=1",
                (str(user.id), str(selected_channel.uuid), selected_start.timestamp()),
            ).fetchall()
        require(len(rows) == 1 if required else len(rows) <= 1,
                "Synthetic XC cleanup did not identify its exact logical session")
        for (lease_id,) in rows:
            require(service.end_user_session(user.id, str(selected_channel.uuid), lease_id),
                    "Synthetic XC session did not stop through the supported service")

    end_fixture_xc_session(no_guide_channel, start, required=True)

    # An available start must remain locally playable even with a later gap.
    prefix_params = dict(xc_params, stream=str(continuous_channel.id), duration="1",
                         **{time_key: str(int((start + timedelta(seconds=6)).timestamp()))})
    prefix_response = timeshift.timeshift_proxy_query(request(
        "/streaming/timeshift.php", prefix_params,
        HTTP_USER_AGENT="Synthetic bounded XC client",
    ))
    require(prefix_response.status_code == 200
            and prefix_response.get("X-Catchuparr-Archive-Partial") == "true",
            "Incomplete local XC archive did not serve its marked contiguous prefix: "
            f"status={prefix_response.status_code}, "
            f"partial={prefix_response.get('X-Catchuparr-Archive-Partial') == 'true'}, "
            f"timeline={prefix_response.get('X-Catchuparr-Archive-Timeline') == 'programme'}")
    try:
        require(b"".join(prefix_response.streaming_content) == b"\x47" + bytes([10]) * 187,
                "Bounded XC did not stop at the exact available archive prefix")
    finally:
        prefix_response.close()
        end_fixture_xc_session(continuous_channel, start + timedelta(seconds=6))

    continuous_xc_params = dict(
        xc_params,
        stream=str(continuous_channel.id),
        duration="1",
        playback="continuous",
        **{time_key: str(int((start + timedelta(seconds=6)).timestamp()))},
    )
    continuous_recorder_lease = f"catchuparr:recorder:{continuous_channel.uuid}"
    redis_client.set(continuous_recorder_lease, "synthetic-active", ex=30)
    continuous_xc = timeshift.timeshift_proxy_query(request(
        "/streaming/timeshift.php", continuous_xc_params,
        HTTP_USER_AGENT="Catchuparr synthetic integration",
        REMOTE_ADDR="127.0.0.41",
    ))
    require(continuous_xc.status_code == 200,
            f"Continuous XC response status {continuous_xc.status_code}")
    require(getattr(continuous_xc, "streaming", False),
            "Continuous XC response must stay streaming")
    require("Content-Length" not in continuous_xc,
            "Live-edge XC response must not advertise a fixed body length")
    require(
        not ChannelStream.objects.filter(channel=continuous_channel).exists(),
        "Synthetic continuous playback must not use a provider stream",
    )
    continuous_iterator = iter(continuous_xc.streaming_content)
    try:
        initial_chunk = next(continuous_iterator)
        append_payloads = []
        for index, offset in enumerate((12, 18)):
            payload = b"\x47" + bytes([31 + index]) * 187
            append_path = root / f"continuous-xc-append-{index}.ts"
            append_path.write_bytes(payload)
            store.add_segment(
                str(continuous_channel.uuid), append_path,
                start + timedelta(seconds=offset),
                start + timedelta(seconds=offset + 6),
            )
            append_payloads.append(payload)
        gap_payload = b"\x47" + bytes([39]) * 187
        gap_path = root / "continuous-xc-gap.ts"
        gap_path.write_bytes(gap_payload)
        store.add_segment(
            str(continuous_channel.uuid), gap_path,
            start + timedelta(seconds=30), start + timedelta(seconds=36),
        )

        collected = []
        reader_errors = []

        def drain_continuous_xc():
            try:
                collected.extend(continuous_iterator)
            except BaseException as exc:
                reader_errors.append(type(exc).__name__)

        reader = threading.Thread(target=drain_continuous_xc, daemon=True)
        reader.start()
        reader.join(timeout=5)
        if reader.is_alive():
            continuous_xc.close()
            reader.join(timeout=1)
        require(not reader.is_alive(),
                "XC gap response exceeded the bounded five-second read")
        require(not reader_errors, "XC continuous reader failed while following appended media")
        require(
            b"".join((initial_chunk, *collected))
            == b"".join((b"\x47" + bytes([10]) * 187, *append_payloads)),
            "XC seek near the programme boundary must follow two appends and stop before a gap",
        )
    finally:
        continuous_xc.close()
        redis_client.delete(continuous_recorder_lease)
    continuous_service = _ts_service(root, timeshift)
    with sqlite3.connect(root / "archive.sqlite3") as database:
        continuous_leases = database.execute(
            "SELECT lease_id FROM ts_playback_sessions "
            "WHERE user_id=? AND channel_id=?",
            (str(user.id), str(continuous_channel.uuid)),
        ).fetchall()
    require(bool(continuous_leases), "Continuous XC session was not recorded")
    for (lease_id,) in continuous_leases:
        require(
            continuous_service.end_user_session(
                user.id, str(continuous_channel.uuid), lease_id,
            ),
            "Continuous XC playback session did not close",
        )
    with sqlite3.connect(root / "archive.sqlite3") as database:
        remaining_xc_sessions, remaining_xc_streams = database.execute(
            "SELECT (SELECT COUNT(*) FROM ts_playback_sessions WHERE channel_id=?), "
            "(SELECT COUNT(*) FROM ts_playback_streams st JOIN ts_playback_sessions s "
            "ON s.lease_id=st.lease_id WHERE s.channel_id=?)",
            (str(continuous_channel.uuid), str(continuous_channel.uuid)),
        ).fetchone()
    require(remaining_xc_sessions == 0 and remaining_xc_streams == 0,
            "XC disconnect must release its playback session and stream lease")

    require(xc_playback(start.timestamp()) == b"\x47" + bytes(187))
    require(xc_playback((start + timedelta(seconds=6)).timestamp())
            == b"\x47" + bytes([1]) * 187, "Timestamp seek must reset byte origin")
    require(xc_playback(closed_start.timestamp(), closed_channel.id)
            == b"\x47" + bytes([2]) * 187,
            "Completed minute must not require five extra minutes of archive")
    require(xc_playback(closed_start.timestamp(), closed_channel.id, duration=None)
            == b"\x47" + bytes([2]) * 187,
            "Missing duration must use the actual EPG end from core helpers")
    bad_credentials = dict(xc_params, password="invalid", **{time_key: str(int(start.timestamp()))})
    shared_xc_auth = callable(getattr(timeshift, "authorize_xc_client", None))
    require(timeshift.timeshift_proxy_query(request(
        "/streaming/timeshift.php", bad_credentials,
    )).status_code == (401 if shared_xc_auth else 403),
            "XC credentials must be rejected by the core authentication contract")
    forbidden_channel = dict(xc_params, stream=str(private_channel.id),
                             **{time_key: str(int(start.timestamp()))})
    if shared_xc_auth:
        from django.http import Http404

        from catchuparr.xc_runtime import _channel_policy_allows

        def require_hidden_channel(parameters):
            try:
                timeshift.timeshift_proxy_query(request("/streaming/timeshift.php", parameters))
            except Http404:
                return
            raise RuntimeError("Core XC must hide inaccessible channels as not found")

        require_hidden_channel(forbidden_channel)
        # Verify the shared core ACL, including rules absent from the old
        # private timeshift helper, without replacing it with a test stub.
        properties_before = dict(user.custom_properties or {})
        adult_before = channel.is_adult
        try:
            channel.is_adult = True
            channel.save(update_fields=["is_adult"])
            user.custom_properties = {**properties_before, "hide_adult_content": True}
            user.save(update_fields=["custom_properties"])
            require(not _channel_policy_allows(timeshift, user, channel),
                    "Local XC must preserve the core adult-content denial")
            require_hidden_channel(dict(xc_params, **{time_key: str(int(start.timestamp()))}))
        finally:
            channel.is_adult = adult_before
            channel.save(update_fields=["is_adult"])
            user.custom_properties = properties_before
            user.save(update_fields=["custom_properties"])
        require(_channel_policy_allows(timeshift, user, channel),
                "Local XC must allow an authorized channel after restoring the fixture")
        profile_was_assigned = user.channel_profiles.filter(pk=channel_profile.pk).exists()
        membership = ChannelProfileMembership.objects.get(
            channel_profile=channel_profile, channel=channel,
        )
        membership_enabled = membership.enabled
        try:
            user.channel_profiles.add(channel_profile)
            membership.enabled = False
            membership.save(update_fields=["enabled"])
            require(not _channel_policy_allows(timeshift, user, channel),
                    "Local XC must preserve disabled profile-membership denial")
            require_hidden_channel(dict(xc_params, **{time_key: str(int(start.timestamp()))}))
        finally:
            membership.enabled = membership_enabled
            membership.save(update_fields=["enabled"])
            if not profile_was_assigned:
                user.channel_profiles.remove(channel_profile)
        require(_channel_policy_allows(timeshift, user, channel),
                "Restored profile access must allow local XC playback")
    else:
        require(timeshift.timeshift_proxy_query(request(
            "/streaming/timeshift.php", forbidden_channel,
        )).status_code == 403)
    user.custom_properties = {**(user.custom_properties or {}), "catchup_enabled": False}
    user.save(update_fields=["custom_properties"])
    cache.clear()
    disabled = views.archive_view(request("/catchuparr/archive", params))
    require(disabled.status_code in (401, 403))
    disabled_params = dict(xc_params, **{time_key: str(int(start.timestamp()))})
    require(timeshift.timeshift_proxy_query(request(
        "/streaming/timeshift.php", disabled_params,
    )).status_code == 403)
    phase("archive_routes", "complete")
    try:
        phase("source_configuration", "started")
        probe_source_configuration(channel, root)
        phase("source_configuration", "complete")
        sys.path.insert(0, "/tmp")
        from aio_recorder_media import probe_actual_recorder_media

        # The media probe asserts both live recording modes before fixture teardown.
        phase("recorder_overviews", "started")
        phase("recorder_media", "started")
        probe_actual_recorder_media(root)
        phase("recorder_media", "complete")
        phase("recorder_overviews", "complete")
        from aio_recorder_failover import probe_recorder_failover

        phase("recorder_failover", "started")
        probe_recorder_failover(root)
        phase("recorder_failover", "complete")
        phase("native_runtime_drain", "started")
        require_native_runtime_drained(native_server)
        phase("native_runtime_drain", "complete")
    finally:
        runtime.shutdown()
    print(f"AIO integration passed: Dispatcharr {__version__}")


probe()
