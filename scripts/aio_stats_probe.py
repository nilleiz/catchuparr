"""Exercise Catchuparr Stats against the real Dispatcharr AIO runtime.

This module is copied into the existing disposable AIO integration worker.  It
uses that worker's Django URLconf, DRF views, Redis, archive store, and native
Timeshift websocket emitter; it does not provide fake dispatch implementations.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def stats_options_draft(settings: dict, *, show_archive: bool | None = None,
                        hide_recorders: bool | None = None) -> dict:
    """Copy a full recorder fixture and update only requested Stats flags."""
    draft = dict(settings)
    if show_archive is not None:
        draft["show_archive_playback_in_stats"] = show_archive
    if hide_recorders is not None:
        draft["hide_recorders_in_stats"] = hide_recorders
    return draft


def _route(path: str):
    from django.urls import resolve

    return resolve(path)


def _assert_native_route_match(match, namespace: str, name: str) -> None:
    _require(
        match.namespace == namespace,
        f"Resolved route is outside native namespace {namespace}",
    )
    _require(match.url_name == name, f"Resolved route is not native {name}")


def _assert_admin_route(path: str, namespace: str, name: str, is_admin) -> None:
    match = _route(path)
    _assert_native_route_match(match, namespace, name)
    view_class = getattr(match.func, "cls", None)
    _require(view_class is not None, f"{path} is not the registered DRF callback")
    permissions = tuple(getattr(view_class, "permission_classes", ()))
    _require(is_admin in permissions, f"Native IsAdmin permission was lost for {path}")


def _viewer_rows(database: Path, user_id: int, channel_uuid: str):
    with sqlite3.connect(database) as db:
        return db.execute(
            "SELECT display_id, playback_device_key, last_success_at, revoked, "
            "playback_lease_id, programme_start_epoch, client_ip "
            "FROM catchuparr_stats_viewers WHERE user_id=? AND channel_uuid=? "
            "ORDER BY last_success_at DESC",
            (str(user_id), str(channel_uuid)),
        ).fetchall()


def _assert_archive_projection(native: dict, projected: dict, active_display_ids: list[str],
                               display_id: str) -> None:
    native_sessions = native.get("timeshift_sessions")
    projected_sessions = projected.get("timeshift_sessions")
    _require(isinstance(native_sessions, list) and isinstance(projected_sessions, list),
             "Native Timeshift Stats session lists are missing")
    plugin_rows = [row for row in projected_sessions
                   if isinstance(row, dict) and row.get("session_id") == display_id]
    _require(len(plugin_rows) == 1,
             "Archive display ID was missing or duplicated in projected Stats")
    projected_ids = [row.get("session_id") for row in projected_sessions
                     if isinstance(row, dict)]
    _require(all(projected_ids.count(item) == 1 for item in active_display_ids),
             "An active plugin display ID was missing or duplicated in projected Stats")
    _require(len(projected_sessions) == len(native_sessions) + len(active_display_ids),
             "Archive viewer projection changed native sessions or double-counted")
    _require(projected.get("total_connections")
             == native.get("total_connections", 0) + len(active_display_ids),
             "Archive viewer projection total_connections is not truthful")


def _admin_clients(admin_user, ordinary_user):
    from rest_framework.test import APIClient
    from rest_framework_simplejwt.tokens import AccessToken

    def client_for(user):
        client = APIClient()
        token = str(AccessToken.for_user(user))
        client.credentials(HTTP_AUTHORIZATION=f"Bearer {token}")
        return client

    return client_for(admin_user), client_for(ordinary_user)


def _response_payload(response):
    _require(response.status_code == 200, f"Stats request returned {response.status_code}")
    payload = response.json()
    _require(isinstance(payload, dict), "Native Stats response was not a JSON object")
    return payload


def _assert_hook_guards(stats) -> None:
    """Check repeat installation and worker-only installation on real modules."""
    from apps.accounts.permissions import IsAdmin
    from apps.proxy import tasks as proxy_tasks
    from apps.proxy.live_proxy import channel_status
    from apps.proxy.live_proxy import urls as live_urls
    from apps.timeshift import urls as timeshift_urls
    from django.urls import reverse

    _require(stats.install_stats_hooks(), "Initial real Stats hook install failed")
    _require(stats.install_stats_hooks(), "Repeated real Stats hook install failed")
    _assert_admin_route(
        reverse("proxy:catchup:catchup_stats"),
        "proxy:catchup", "catchup_stats", IsAdmin,
    )
    _assert_admin_route(
        reverse("proxy:catchup:catchup_stop_client"),
        "proxy:catchup", "catchup_stop_client", IsAdmin,
    )

    stream_routes = [route for route in live_urls.urlpatterns if route.name == "stream"]
    _require(bool(stream_routes), "Native live stream route was not found")
    callback = stream_routes[0].callback
    wrappers = []
    seen = set()
    while callback is not None and id(callback) not in seen:
        seen.add(id(callback))
        wrappers.append(callback)
        callback = getattr(callback, "__wrapped__", None)
    _require(
        any(getattr(item, "_catchuparr_managed_id_guard", False) for item in wrappers),
        "Stats hook chain removed the native managed-ID guard",
    )

    _require(stats.uninstall_stats_hooks(), "Real Stats hook uninstall failed")
    _require(
        stats.install_stats_hooks(route_hooks=False),
        "Worker-mode Stats hook install failed",
    )
    _require(
        getattr(channel_status.ChannelStatus.get_basic_channel_info, stats.HOOK_MARKER, False),
        "Worker mode did not install native basic client-list filtering",
    )
    _require(
        getattr(channel_status.ChannelStatus.get_detailed_channel_info, stats.HOOK_MARKER, False),
        "Worker mode did not install native detailed client filtering",
    )
    _require(
        getattr(proxy_tasks.build_live_channel_stats_data, stats.HOOK_MARKER, False),
        "Worker mode did not install the native task Stats alias",
    )
    _require(stats.uninstall_stats_hooks(), "Worker-mode Stats hook uninstall failed")
    _require(stats.install_stats_hooks(), "Stats hook restore failed")

    # The original Stop callback is a variadic DRF callback.  Keep its real
    # endpoint and APIView metadata available after plugin wrapping.
    stop_route = next(route for route in timeshift_urls.urlpatterns
                      if route.name == "catchup_stop_client")
    stop_view_class = getattr(stop_route.callback, "cls", None)
    _require(stop_view_class is not None,
             "Wrapped native Stop callback lost its DRF APIView class")
    _require(stop_route.callback.__name__ == "view",
             "Wrapped native Stop callback no longer identifies the DRF view")
    _require(stop_view_class.__name__ == "stop_timeshift_session",
             "Wrapped native Stop callback no longer identifies the core Stop endpoint")


def _assert_native_websocket_event():
    """Capture the real Timeshift emitter event without starting a worker thread."""
    from unittest import mock

    import core.utils
    from apps.timeshift import views as timeshift_views
    from dispatcharr.consumers import user_may_receive_update

    sent = []
    with mock.patch.object(timeshift_views, "_spawn_background_task", lambda fn: fn()), \
            mock.patch.object(core.utils, "send_websocket_update",
                              lambda *args: sent.append(args)):
        timeshift_views._trigger_timeshift_stats_update(
            core.utils.RedisClient.get_client()
        )
    _require(len(sent) == 1, "Native Timeshift Stats websocket emitter sent no event")
    group, event_name, event = sent[0]
    _require(group == "updates" and event_name == "update",
             "Native Timeshift Stats websocket event routing changed")
    _require(event.get("success") is True and event.get("type") == "timeshift_stats",
             "Native Timeshift Stats websocket payload schema changed")
    _require(isinstance(event.get("stats"), str),
             "Native Timeshift Stats websocket payload is not JSON text")
    _require(isinstance(json.loads(event["stats"]), dict),
             "Native Timeshift Stats websocket data is not a JSON object")
    _assert_native_websocket_filter(user_may_receive_update, event)


def _assert_native_websocket_filter(user_may_receive_update, emitted_event) -> None:
    """Exercise the real consumer permission helper with the captured event."""
    from apps.accounts.models import User
    from django.contrib.auth.models import AnonymousUser

    admin = User.objects.create_user(
        username="synthetic-websocket-admin",
        user_level=User.UserLevel.ADMIN,
        stream_limit=1,
    )
    ordinary = User.objects.create_user(
        username="synthetic-websocket-user", user_level=1, stream_limit=1,
    )
    try:
        _require(user_may_receive_update(admin, emitted_event) is True,
                 "Native websocket filter did not allow admin Stats events")
        _require(user_may_receive_update(ordinary, emitted_event) is False,
                 "Native websocket filter did not block non-admin Stats events")
        _require(user_may_receive_update(AnonymousUser(), emitted_event) is False,
                 "Native websocket filter did not block anonymous Stats events")
    finally:
        admin.delete()
        ordinary.delete()


def _assert_applied_options(runtime, plugin_config, stats, *, admin_client,
                            native_paths, display_id) -> None:
    original = dict(plugin_config.settings or {})

    def assert_display_visibility(show_archive: bool) -> None:
        direct = _response_payload(admin_client.get(native_paths["catchup"]))
        combined = _response_payload(admin_client.get(native_paths["combined"]))
        direct_ids = {
            str(row.get("session_id")) for row in direct.get("timeshift_sessions", [])
            if isinstance(row, dict)
        }
        combined_ids = {
            str(row.get("session_id"))
            for row in combined.get("catchup", {}).get("timeshift_sessions", [])
            if isinstance(row, dict)
        }
        expected = show_archive is True
        _require((display_id in direct_ids) is expected,
                 "Native catch-up Stats did not reflect applied archive visibility")
        _require((display_id in combined_ids) is expected,
                 "Native combined Stats did not reflect applied archive visibility")

    try:
        initial = stats._options()
        pairs = (
            (False, False, "show_archive_playback_in_stats"),
            (False, True, "show_archive_playback_in_stats"),
            (True, False, "hide_recorders_in_stats"),
            (True, True, "hide_recorders_in_stats"),
        )
        for show_archive, hide_recorders, changed_key in pairs:
            draft = dict(plugin_config.settings or {})
            draft.update(
                show_archive_playback_in_stats=show_archive,
                hide_recorders_in_stats=hide_recorders,
            )
            plugin_config.settings = draft
            plugin_config.save(update_fields=["settings"])
            _require(stats._options() == initial,
                     "Stats options changed when only the draft was saved")
            assert_display_visibility(initial["show_archive_playback_in_stats"])
            result = runtime.apply_configuration()
            _require(result.get("applied") is True,
                     f"Unified Apply rejected draft option {changed_key}")
            applied = stats._options()
            _require(
                applied["show_archive_playback_in_stats"] is show_archive
                and applied["hide_recorders_in_stats"] is hide_recorders,
                f"Unified Apply did not activate Stats option {changed_key}",
            )
            initial = applied
            assert_display_visibility(show_archive)
    finally:
        plugin_config.settings = original
        plugin_config.save(update_fields=["settings"])
        result = runtime.apply_configuration()
        _require(result.get("applied") is True,
                 "Could not restore synthetic integration settings after Stats probe")


def probe_actual_stats(*, root, request, user, channel, token, start, params,
                       xc_playback) -> None:
    """Run real endpoint, media, projection, expiry, Stop, and event checks."""

    from apps.accounts.models import User
    from apps.plugins.models import PluginConfig
    from core.utils import RedisClient
    from django.urls import reverse
    from rest_framework.test import APIClient

    from catchuparr import runtime, stats
    from catchuparr import views as archive_views
    from catchuparr.stats import DISPLAY_ID_PREFIX

    _assert_hook_guards(stats)
    from apps.accounts.permissions import IsAdmin

    native_paths = {
        "combined": reverse("proxy:combined_stats"),
        "catchup": reverse("proxy:catchup:catchup_stats"),
        "stop": reverse("proxy:catchup:catchup_stop_client"),
    }
    _assert_admin_route(native_paths["combined"], "proxy", "combined_stats", IsAdmin)
    _assert_admin_route(
        native_paths["catchup"], "proxy:catchup", "catchup_stats", IsAdmin,
    )
    _assert_admin_route(
        native_paths["stop"], "proxy:catchup", "catchup_stop_client", IsAdmin,
    )

    admin = User.objects.create_user(
        username="synthetic-stats-admin",
        user_level=User.UserLevel.ADMIN,
        stream_limit=1,
    )
    admin_client, ordinary_client = _admin_clients(admin, user)
    for path in (native_paths["combined"], native_paths["catchup"]):
        _response_payload(admin_client.get(path))
        _require(ordinary_client.get(path).status_code == 403,
                 f"Non-admin user reached protected Stats endpoint {path}")
    _require(APIClient().get(native_paths["combined"]).status_code == 401,
             "Anonymous user reached protected Stats endpoint")
    _require(ordinary_client.post(
        native_paths["stop"], {"session_id": "ca_not-a-real-session"}, format="json",
    ).status_code == 403, "Non-admin user reached native Stop endpoint")

    # Real HLS playlist and byte-range requests create/update a single logical
    # viewer; fixed synthetic request metadata is asserted from the plugin DB.
    previous_active_ids = [row["session_id"] for row in stats._active_viewers()]
    selected = dict(params, channel_id=str(channel.uuid))
    playback_request = request(
        "/catchuparr/archive", selected, REMOTE_ADDR="198.51.100.41",
    )
    playlist = archive_views.archive_view(playback_request)
    _require(playlist.status_code == 200, "Synthetic HLS playlist did not return 200")
    playlist_url = next(
        line for line in playlist.content.decode().splitlines()
        if line and not line.startswith("#")
    )
    parts = urlsplit(playlist_url)
    query = {key: value[0] for key, value in parse_qs(parts.query).items()}
    path_parts = parts.path.rstrip("/").split("/")
    segment = archive_views.segment_view(
        request(parts.path, query, HTTP_RANGE="bytes=0-187",
                REMOTE_ADDR="198.51.100.41"),
        path_parts[-2], path_parts[-1],
    )
    _require(segment.status_code == 206, "Synthetic HLS segment did not return 206")
    reloaded = archive_views.archive_view(request(
        "/catchuparr/archive", selected, REMOTE_ADDR="198.51.100.41",
    ))
    _require(reloaded.status_code == 200, "Synthetic HLS reload did not return 200")
    seek_params = dict(selected, utc=str(int(start.timestamp()) + 6))
    seek = archive_views.archive_view(request(
        "/catchuparr/archive", seek_params, REMOTE_ADDR="198.51.100.41",
    ))
    _require(seek.status_code == 200, "Synthetic HLS seek did not return 200")

    database = root / "archive.sqlite3"
    rows = _viewer_rows(database, user.id, str(channel.uuid))
    hls_key = hashlib.sha256(token.encode("utf-8")).hexdigest()
    hls_rows = [row for row in rows if row[1] == hls_key]
    _require(len(hls_rows) == 1, "HLS reload/seek created duplicate logical viewer rows")
    (
        display_id,
        _device_key,
        last_success,
        revoked,
        old_lease,
        programme_epoch,
        client_ip,
    ) = hls_rows[0]
    _require(str(display_id).startswith(DISPLAY_ID_PREFIX),
             "HLS playback did not create an opaque plugin display ID")
    _require(client_ip == "198.51.100.41", "Stats row lost the synthetic request IP")
    _require(abs(float(programme_epoch) - (start.timestamp() + 6)) < 1,
             "Stats row lost the requested synthetic programme timestamp")
    _require(not revoked and old_lease, "HLS request did not retain its actual archive lease")

    # Use the native builder as the baseline, then verify only one plugin row
    # was added and that its 180 second display expiry follows a controlled clock.
    redis = RedisClient.get_client()
    from apps.timeshift.stats import build_timeshift_stats_data

    native_builder = build_timeshift_stats_data
    while getattr(native_builder, stats.HOOK_MARKER, False):
        native_builder = native_builder.__wrapped__
    native = native_builder(redis)
    projected = stats.project_timeshift_stats(native)
    active_rows = stats._active_viewers()
    active_display_ids = [row["session_id"] for row in active_rows]
    _require(active_display_ids.count(display_id) == 1,
             "HLS playback did not project exactly one active display row")
    expected_delta = 0 if display_id in previous_active_ids else 1
    _require(len(active_display_ids) == len(previous_active_ids) + expected_delta,
             "HLS reload/seek created or removed a logical archive viewer")
    _assert_archive_projection(native, projected, active_display_ids, display_id)
    plugin_config = PluginConfig.objects.get(key="catchuparr")
    _assert_applied_options(
        runtime,
        plugin_config,
        stats,
        admin_client=admin_client,
        native_paths=native_paths,
        display_id=display_id,
    )
    original_time = stats.time
    try:
        stats.time = SimpleNamespace(time=lambda: float(last_success) + 179)
        before_timeout = stats._active_viewers()
        _require(sum(row["session_id"] == display_id for row in before_timeout) == 1,
                 "Archive viewer expired before the 180 second timeout")
        stats.time = SimpleNamespace(time=lambda: float(last_success) + 181)
        after_timeout = stats._active_viewers()
        _require(not any(row["session_id"] == display_id for row in after_timeout),
                 "Archive viewer remained visible after the 180 second timeout")
    finally:
        stats.time = original_time

    # The native Stop callback receives only the opaque plugin ID.  Its native
    # response is preserved for other IDs; this ID revokes the actual lease.
    stopped = admin_client.post(
        native_paths["stop"], {"session_id": display_id}, format="json",
    )
    _require(stopped.status_code == 200 and stopped.json().get("success") is True,
             "Native Stop route did not revoke the plugin Stats session")
    stale_segment = archive_views.segment_view(
        request(parts.path, query, HTTP_RANGE="bytes=0-187",
                REMOTE_ADDR="198.51.100.41"),
        path_parts[-2], path_parts[-1],
    )
    _require(stale_segment.status_code in (403, 404, 410),
             "Stopped HLS lease still served media")

    # Existing XC route exercise returns native TS leases.  Stop their active
    # plugin row, reject its late first-chunk heartbeat, then accept a fresh
    # lease only after the normal native XC request has issued it.
    ts_response = xc_playback(
        start.timestamp(), consume=False, remote_addr="198.51.100.42",
        range_header="bytes=0-10000000",
    )
    ts_iterator = iter(ts_response.streaming_content)
    first_chunk = next(ts_iterator, b"")
    _require(first_chunk and first_chunk[0] == 0x47,
             "Native XC TS request did not return its first media chunk")
    ts_rows = _viewer_rows(database, user.id, str(channel.uuid))
    _require(len(ts_rows) >= 1, "Native XC TS lease did not produce a Stats row")
    ts_row = next((row for row in ts_rows
                   if row[1] != hls_key and row[4] and row[4] != old_lease), None)
    _require(ts_row is not None, "Native XC request produced no distinct TS lease")
    ts_display_id, ts_device_key, _ts_last, _ts_revoked, ts_lease, *_ = ts_row
    _require(ts_lease != old_lease, "XC did not issue a separate TS lease")
    from apps.timeshift import views as timeshift_views

    from catchuparr.xc_runtime import _ts_service

    ts_service = _ts_service(runtime.load_config().archive_root, timeshift_views)
    with ts_service._connect() as db:
        active_lease = db.execute(
            "SELECT active FROM ts_playback_sessions WHERE lease_id=?", (ts_lease,),
        ).fetchone()
    _require(active_lease is not None and active_lease[0] == 1,
             "Native XC iterator did not retain its current playback lease")
    ts_stop = admin_client.post(
        native_paths["stop"], {"session_id": ts_display_id}, format="json",
    )
    _require(ts_stop.status_code == 200,
             "Native Stop route did not stop the current XC display ID")
    with ts_service._connect() as db:
        stopped_lease = db.execute(
            "SELECT active FROM ts_playback_sessions WHERE lease_id=?", (ts_lease,),
        ).fetchone()
    _require(stopped_lease is None,
             "Native Stop did not remove the current XC playback session")
    try:
        try:
            post_stop_chunk = next(ts_iterator, b"")
        except OSError:
            post_stop_chunk = b""
        _require(not post_stop_chunk,
                 "Native XC iterator yielded media after its lease was stopped")
    finally:
        ts_response.close()
    # This invokes the exact first-chunk callback used by the real XC iterator,
    # with the lease obtained from the successful native stream above.
    stats.successful_playback(
        user.id, str(channel.uuid), ts_device_key, heartbeat=False,
        playback_lease_id=ts_lease, programme_start_epoch=start.timestamp(),
        client_ip="198.51.100.42",
    )
    stopped_row = next(row for row in _viewer_rows(database, user.id, str(channel.uuid))
                       if row[0] == ts_display_id)
    _require(bool(stopped_row[3]) and stopped_row[4] == ts_lease,
             "A late first chunk revived the stopped TS lease")
    replacement_body = xc_playback(
        start.timestamp(), remote_addr="198.51.100.42",
    )
    _require(replacement_body == b"\x47" + bytes(187),
             "Fresh native XC TS request did not return the expected byte range")
    replacement_row = next(row for row in _viewer_rows(database, user.id, str(channel.uuid))
                           if row[0] == ts_display_id)
    _require(not replacement_row[3] and replacement_row[4] != ts_lease,
             "Only a distinct current TS lease may revive the stopped display row")
    _assert_native_websocket_event()

    ordinary_client.logout()
    admin_client.logout()
    admin.delete()


def probe_actual_recorder_stats(*, channel_id, redis_client, native_server,
                                profile_a, profile_b,
                                profile_connections_key, assignment_snapshot,
                                metadata_key) -> None:
    """Prove only the signed recorder client is hidden in native live Stats."""
    from aio_recorder_media import _native_source_metadata
    from apps.accounts.models import User
    from apps.proxy.live_proxy import channel_status
    from apps.proxy.live_proxy.redis_keys import RedisKeys
    from rest_framework.test import APIClient
    from rest_framework_simplejwt.tokens import AccessToken

    from catchuparr import stats

    manager = native_server.client_managers.get(channel_id)
    _require(manager is not None, "Shared native live recorder has no ClientManager")
    channel_key = str(manager.channel_id)
    client_ids = [
        value.decode("utf-8") if isinstance(value, bytes) else str(value)
        for value in redis_client.smembers(RedisKeys.clients(channel_key))
    ]
    recorder_ids = {
        client_id for client_id in client_ids
        if stats._is_recorder_client(channel_key, client_id)
    }
    _require(bool(recorder_ids),
             "Real signed recorder request did not tag its native ClientManager client")

    admin = User.objects.create_user(
        username="synthetic-recorder-stats-admin",
        user_level=User.UserLevel.ADMIN,
        stream_limit=1,
    )
    client = APIClient()
    client.credentials(HTTP_AUTHORIZATION=f"Bearer {AccessToken.for_user(admin)}")
    added = []
    visible_ids = {f"synthetic-visible-{index:02d}" for index in range(11)}
    before_clients = set(client_ids)
    before_a = redis_client.get(profile_connections_key(profile_a.id))
    before_b = redis_client.get(profile_connections_key(profile_b.id))
    before_assignments = {key: _redis_snapshot(redis_client, key)
                          for key in assignment_snapshot}
    before_metadata = _native_source_metadata(redis_client, metadata_key)
    try:
        for index, client_id in enumerate(sorted(visible_ids)):
            result = manager.add_client(
                client_id,
                f"198.51.100.{100 + index}",
                "Synthetic recorder user-agent spoof",
                None,
            )
            _require(result, "Could not add a synthetic visible native client")
            added.append(client_id)

        current_ids = [
            value.decode("utf-8") if isinstance(value, bytes) else str(value)
            for value in redis_client.smembers(RedisKeys.clients(channel_key))
        ]
        current_recorder_ids = {
            value for value in current_ids
            if stats._is_recorder_client(channel_key, value)
        }
        _require(current_recorder_ids == recorder_ids,
                 "Forged user-agent or IP changed recorder identity")
        raw_native = channel_status.ChannelStatus.get_basic_channel_info
        native_basic = raw_native
        while getattr(native_basic, "__wrapped__", None) is not None:
            native_basic = native_basic.__wrapped__
        native_payload = native_basic(channel_key)
        _require(isinstance(native_payload, dict),
                 "Native basic client stats payload is unavailable")
        native_top = {
            str(item.get("client_id")) for item in native_payload.get("clients", [])
            if isinstance(item, dict)
        }
        beyond_native_cap = visible_ids - native_top
        _require(bool(beyond_native_cap),
                 "Synthetic fixture did not place a visible client beyond the native cap")

        from django.urls import reverse

        response = client.get(reverse("proxy:combined_stats"))
        payload = _response_payload(response)
        channels = payload.get("live", {}).get("channels", [])
        row = next((item for item in channels
                    if str(item.get("channel_id")) == channel_key), None)
        _require(isinstance(row, dict), "Native combined Stats omitted the synthetic channel")
        visible = {
            str(item.get("client_id")) for item in row.get("clients", [])
            if isinstance(item, dict)
        }
        _require(len(visible) == 10,
                 "Visible native clients were not refilled to the ten-row display cap")
        if current_recorder_ids & native_top:
            _require(bool(visible & beyond_native_cap),
                     "Filtering a capped recorder failed to refill visible clients")
        _require(not visible & current_recorder_ids,
                 "A server-verified recorder client remained visible in native Stats")
        expected_count = len(current_ids) - len(current_recorder_ids)
        _require(row.get("client_count") == expected_count,
                 "Native Stats client_count did not subtract only verified recorder clients")

        from unittest import mock

        from apps.proxy import tasks as proxy_tasks

        _require(stats.uninstall_stats_hooks(),
                 "Could not switch to native worker-mode Stats hooks")
        _require(stats.install_stats_hooks(route_hooks=False),
                 "Could not install native worker-mode Stats hooks")
        emitted = []
        try:
            worker_basic = channel_status.ChannelStatus.get_basic_channel_info(channel_key)
            worker_detail = channel_status.ChannelStatus.get_detailed_channel_info(channel_key)
            _require(
                len(worker_basic.get("clients", [])) == 10
                and not {
                    str(item.get("client_id")) for item in worker_basic.get("clients", [])
                    if isinstance(item, dict)
                } & current_recorder_ids,
                "Worker-mode basic Stats did not filter and refill before its cap",
            )
            _require(
                not _contains_client_id(worker_detail, next(iter(current_recorder_ids))),
                "Worker-mode detailed Stats exposed a verified recorder client",
            )
            with mock.patch.object(
                proxy_tasks, "send_websocket_update",
                side_effect=lambda *args, **kwargs: emitted.append((args, kwargs)),
            ):
                proxy_tasks.fetch_channel_stats()
            _require(len(emitted) == 1,
                     "Native channel Stats task did not emit its real update")
            event = emitted[0][0][2]
            _require(event.get("type") == "channel_stats",
                     "Native channel Stats task event type changed")
            worker_payload = json.loads(event["stats"])
            worker_row = next(
                (item for item in worker_payload.get("channels", [])
                 if str(item.get("channel_id")) == channel_key),
                None,
            )
            _require(isinstance(worker_row, dict),
                     "Native worker Stats task omitted the synthetic channel")
            worker_visible = {
                str(item.get("client_id")) for item in worker_row.get("clients", [])
                if isinstance(item, dict)
            }
            _require(len(worker_visible) == 10,
                     "Worker-mode Stats did not refill the visible client cap")
            _require(not worker_visible & current_recorder_ids,
                     "Worker-mode Stats exposed a verified recorder client")
            _require(worker_row.get("client_count") == expected_count,
                     "Worker-mode Stats count did not exclude verified recorder clients")
        finally:
            _require(stats.uninstall_stats_hooks(),
                     "Could not restore hooks after worker-mode Stats request")
            _require(stats.install_stats_hooks(),
                     "Could not restore full Stats hooks after worker-mode request")
    finally:
        for client_id in added:
            manager.remove_client(client_id)
        admin.delete()

    _require(
        redis_client.get(profile_connections_key(profile_a.id)) == before_a
        and redis_client.get(profile_connections_key(profile_b.id)) == before_b,
        "Native Stats probe changed a provider profile ledger",
    )
    after_clients = {
        value.decode("utf-8") if isinstance(value, bytes) else str(value)
        for value in redis_client.smembers(RedisKeys.clients(channel_key))
    }
    _require(after_clients == before_clients,
             "Native Stats probe did not restore the real client registry")
    _require(
        {key: _redis_snapshot(redis_client, key) for key in assignment_snapshot}
        == before_assignments
        and _native_source_metadata(redis_client, metadata_key) == before_metadata,
        "Native Stats probe changed provider assignment or source metadata",
    )


def _redis_snapshot(redis_client, key):
    kind = redis_client.type(key)
    if isinstance(kind, bytes):
        kind = kind.decode("ascii", errors="replace")
    if kind == "hash":
        return redis_client.hgetall(key)
    if kind == "set":
        return redis_client.smembers(key)
    return redis_client.get(key)


def _contains_client_id(value, client_id: str) -> bool:
    if isinstance(value, dict):
        if str(value.get("client_id", "")) == client_id:
            return True
        return any(_contains_client_id(item, client_id) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_client_id(item, client_id) for item in value)
    return False


def probe_actual_private_recorder_stats(*, channel_id, redis_client,
                                        expected_hidden: bool) -> None:
    """Check the real private B worker registration through Redis and Stats."""
    from apps.accounts.models import User
    from apps.proxy.live_proxy import channel_status
    from apps.proxy.live_proxy.redis_keys import RedisKeys
    from django.urls import reverse
    from rest_framework.test import APIClient
    from rest_framework_simplejwt.tokens import AccessToken

    from catchuparr import stats

    channel_key = str(channel_id)
    client_ids = {
        value.decode("utf-8") if isinstance(value, bytes) else str(value)
        for value in redis_client.smembers(RedisKeys.clients(channel_key))
    }
    recorder_ids = {
        client_id for client_id in client_ids
        if stats._is_recorder_client(channel_key, client_id)
    }
    _require(len(recorder_ids) == 1,
             "Private B native Redis client set has no unique signed recorder identity")
    recorder_id = next(iter(recorder_ids))
    metadata_key = RedisKeys.client_metadata(channel_key, recorder_id)
    _require(bool(redis_client.hgetall(metadata_key)),
             "Private B recorder client lacks native ClientManager metadata")

    admin = User.objects.create_user(
        username="synthetic-private-recorder-admin",
        user_level=User.UserLevel.ADMIN,
        stream_limit=1,
    )
    client = APIClient()
    client.credentials(HTTP_AUTHORIZATION=f"Bearer {AccessToken.for_user(admin)}")
    _require(stats._options()["hide_recorders_in_stats"] is expected_hidden,
             "Private recorder probe did not run under the expected applied option")
    basic = channel_status.ChannelStatus.get_basic_channel_info(channel_key)
    detailed = channel_status.ChannelStatus.get_detailed_channel_info(channel_key)
    combined_response = client.get(reverse("proxy:combined_stats"))
    combined = _response_payload(combined_response)
    actual = (
        _contains_client_id(basic, recorder_id),
        _contains_client_id(detailed, recorder_id),
        _contains_client_id(combined, recorder_id),
    )
    expected_visible = not expected_hidden
    _require(actual == (expected_visible, expected_visible, expected_visible),
             "Private B recorder identity did not follow the applied native Stats option")
    client.logout()
    admin.delete()
