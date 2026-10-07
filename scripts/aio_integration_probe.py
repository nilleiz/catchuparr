"""Run through manage.py shell in an empty, disposable Dispatcharr AIO."""

import inspect
import os
import signal
import sys
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

    user = User.objects.create_user(username="synthetic-player", user_level=0)
    channel = Channel.objects.create(name="Synthetic", channel_number=1, user_level=0)
    private_channel = Channel.objects.create(name="Private", channel_number=2, user_level=10)
    root = Path("/data/ci-archive")
    PluginConfig.objects.create(
        key="catchuparr", name="Catchuparr", enabled=True, ever_enabled=True,
        settings={"channel_uuids": str(channel.uuid), "archive_root": str(root),
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
    start = timezone.now() - timedelta(seconds=12)
    store.add_segment(str(channel.uuid), source, start, start + timedelta(seconds=6))
    source.write_bytes((b"\x47" + bytes(187)) * 2)
    store.add_segment(str(channel.uuid), source, start + timedelta(seconds=6),
                      start + timedelta(seconds=12))
    token = AccessTokenStore(root).create(str(user.id))
    factory = RequestFactory()

    def request(path, params=None, **headers):
        return factory.get(path, params or {}, HTTP_HOST="localhost", **headers)

    require(views.m3u_view(request("/catchuparr/m3u")).status_code == 401)
    playlist = views.m3u_view(request("/catchuparr/m3u", {"access_token": token}))
    require(playlist.status_code == 200, f"Playlist status {playlist.status_code}")
    text = playlist.content.decode()
    require('catchup-timezone="UTC"' in text)
    require("duration={duration}" in text)
    require(str(channel.uuid) in text and str(private_channel.uuid) not in text)
    params = {"access_token": token, "channel_id": str(channel.uuid),
              "utc": str(start.timestamp()), "duration": "12"}
    archive = views.archive_view(request("/catchuparr/archive", params))
    require(archive.status_code == 200, f"Archive status {archive.status_code}")
    require(b"#EXT-X-ENDLIST" in archive.content)
    require(b"#EXTINF" in archive.content)
    from urllib.parse import parse_qs, urlsplit

    segment_url = next(line for line in archive.content.decode().splitlines()
                       if line and not line.startswith("#"))
    parts = urlsplit(segment_url)
    query = {key: values[0] for key, values in parse_qs(parts.query).items()}
    path_parts = parts.path.rstrip("/").split("/")
    ranged = views.segment_view(request(parts.path, query, HTTP_RANGE="bytes=0-"),
                                path_parts[-2], path_parts[-1])
    require(ranged.status_code == 206 and ranged["Content-Range"].startswith("bytes 0-"))
    user.custom_properties = {"catchup_enabled": False}
    user.save(update_fields=["custom_properties"])
    cache.clear()
    disabled = views.archive_view(request("/catchuparr/archive", params))
    require(disabled.status_code in (401, 403))
    runtime.shutdown()
    print(f"AIO integration passed: Dispatcharr {__version__}")


probe()
