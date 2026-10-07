"""Run through manage.py shell in an empty, disposable Dispatcharr AIO."""

import inspect
import os
import signal
import sqlite3
import sys
import uuid
from datetime import timedelta
from pathlib import Path

from django.core.cache import cache
from django.test import RequestFactory
from django.utils import timezone


def require(condition, message="Integration check failed"):
    if not condition:
        raise RuntimeError(message)


def probe():
    if os.environ.get("CATCHUPARR_INTEGRATION_TEST") != "1":
        raise RuntimeError("This probe requires a disposable integration container")
    sys.path.insert(0, "/data/plugins")
    from apps.accounts.models import User
    from apps.channels.models import Channel
    from apps.epg.models import EPGData, ProgramData
    from apps.output import views as output
    from apps.plugins.models import PluginConfig
    from apps.timeshift import views as timeshift
    from version import __version__

    from catchuparr import runtime, views
    from catchuparr.compatibility import is_supported_dispatcharr_version
    from catchuparr.engine.store import ArchiveStore
    from catchuparr.security import AccessTokenStore

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
    root = Path("/data/ci-archive")
    root.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(root / "archive.sqlite3") as database:
        database.execute("""CREATE TABLE http_playback_sessions (
            lease_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, channel_id TEXT NOT NULL,
            start_utc REAL NOT NULL, end_utc REAL NOT NULL, expires_at REAL NOT NULL
        )""")
    PluginConfig.objects.create(
        key="catchuparr", name="Catchuparr", enabled=True, ever_enabled=True,
        settings={"channel_uuids": f"{channel.uuid},{closed_channel.uuid},{no_guide_channel.uuid},"
                  f"{long_segment_channel.uuid}",
                  "archive_root": str(root),
                  "retention_hours": 1, "max_storage_gib": 1},
    )
    runtime.bootstrap()
    runtime.bootstrap()
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
    token = AccessTokenStore(root).create(str(user.id))
    factory = RequestFactory()

    def request(path, params=None, **headers):
        return factory.get(path, params or {}, HTTP_HOST="localhost", **headers)

    time_key = "utc" if __version__ == "0.32.0" else "start"
    xc_params = {"username": user.username, "password": xc_password,
                 "stream": str(channel.id), "duration": "1"}
    first_xc = timeshift.timeshift_proxy_query(request(
        "/streaming/timeshift.php",
        dict(xc_params, stream=str(closed_channel.id),
             **{time_key: str(int(closed_start.timestamp()))}),
        HTTP_RANGE="bytes=0-187", HTTP_USER_AGENT="Catchuparr synthetic integration",
    ))
    require(first_xc.status_code == 206,
            f"XC before first HLS request must migrate legacy sessions: {first_xc.status_code}")
    try:
        require(b"".join(first_xc.streaming_content) == b"\x47" + bytes([2]) * 187)
    finally:
        first_xc.close()

    require(views.m3u_view(request("/catchuparr/m3u")).status_code == 401)
    playlist = views.m3u_view(request("/catchuparr/m3u", {"access_token": token}))
    require(playlist.status_code == 200, f"Playlist status {playlist.status_code}")
    text = playlist.content.decode()
    require('catchup-timezone="UTC"' in text)
    require("duration={duration}" in text)
    require(str(channel.uuid) in text and str(private_channel.uuid) not in text)
    params = {"access_token": token, "channel_id": str(channel.uuid),
              "utc": str(start.timestamp()), "duration": "12"}
    from urllib.parse import parse_qs, urlsplit

    missing_guide = views.archive_view(request(
        "/catchuparr/archive", dict(params, channel_id=str(no_guide_channel.uuid))
    ))
    require(missing_guide.status_code == 404,
            "Selected archive channel without EPG must not invent a programme window")
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
    archive = views.archive_view(request("/catchuparr/archive", params))
    require(archive.status_code == 200, f"Archive status {archive.status_code}")
    require(archive.content.count(b"#EXTINF:") == 2,
            "Initial programme must exclude the next programme's segment")
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
            "Full Range GET of the published tail must count as playback progress")
    waiting = views.archive_view(request("/catchuparr/archive", params))
    require(waiting.status_code == 200 and waiting.content == archive.content,
            "Missing next segment must keep the EVENT manifest open and unchanged")
    store.add_segment(str(channel.uuid), source, start + timedelta(seconds=12),
                      start + timedelta(seconds=18))
    continued = views.archive_view(request("/catchuparr/archive", params))
    require(continued.status_code == 200 and continued.content.count(b"#EXTINF:") == 3,
            "Next indexed programme must append after a completed tail GET")
    require(continued.content.startswith(archive.content),
            "Reload must preserve the exact published EVENT prefix")
    require(service.end_session(token, str(channel.uuid), query["lease"]))
    seek_params = dict(params, utc=str(int(start.timestamp()) + 6))
    seek = views.archive_view(request("/catchuparr/archive", seek_params))
    require(seek.status_code == 200, f"HLS seek status {seek.status_code}")
    require(seek.content.count(b"#EXTINF:") == 1,
            "Seek with unchanged programme duration must stop at EPG end")
    seek_segment = next(line for line in seek.content.decode().splitlines()
                        if line and not line.startswith("#"))
    seek_lease = parse_qs(urlsplit(seek_segment).query)["lease"][0]
    require(service.end_session(token, str(channel.uuid), seek_lease))

    xc_playlist = output.generate_m3u(request("/get.php", xc_params), user=user)
    require(xc_playlist.status_code == 200)
    xc_text = xc_playlist.content.decode()
    require('catchup="default"' in xc_text)
    require("duration={duration:60}" in xc_text)
    require(time_key + "={utc}" in xc_text)

    def xc_playback(start_epoch, stream_id=channel.id, duration="1"):
        selected = dict(xc_params, stream=str(stream_id),
                        **{time_key: str(int(start_epoch))})
        if duration is None:
            selected.pop("duration")
        else:
            selected["duration"] = duration
        result = timeshift.timeshift_proxy_query(request(
            "/streaming/timeshift.php", selected, HTTP_RANGE="bytes=0-187",
            HTTP_USER_AGENT="Catchuparr synthetic integration",
        ))
        require(result.status_code == 206, f"XC range status {result.status_code}")
        try:
            return b"".join(result.streaming_content)
        finally:
            result.close()

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
    require(timeshift.timeshift_proxy_query(request(
        "/streaming/timeshift.php", bad_credentials,
    )).status_code == 403)
    forbidden_channel = dict(xc_params, stream=str(private_channel.id),
                             **{time_key: str(int(start.timestamp()))})
    require(timeshift.timeshift_proxy_query(request(
        "/streaming/timeshift.php", forbidden_channel,
    )).status_code == 403)
    user.custom_properties = {"catchup_enabled": False}
    user.save(update_fields=["custom_properties"])
    cache.clear()
    disabled = views.archive_view(request("/catchuparr/archive", params))
    require(disabled.status_code in (401, 403))
    disabled_params = dict(xc_params, **{time_key: str(int(start.timestamp()))})
    require(timeshift.timeshift_proxy_query(request(
        "/streaming/timeshift.php", disabled_params,
    )).status_code == 403)
    runtime.shutdown()
    print(f"AIO integration passed: Dispatcharr {__version__}")


probe()
