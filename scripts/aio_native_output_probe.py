"""Exercise native output augmentation and archive authorization in pinned AIOs."""

from __future__ import annotations

import json
import sqlite3
import xml.etree.ElementTree as ET
from datetime import timedelta
from urllib.parse import parse_qs, urlsplit


def require(condition, message="Native output integration assertion failed"):
    if not condition:
        raise RuntimeError(message)


def _response_body(response):
    try:
        if getattr(response, "streaming", False):
            return b"".join(
                part.encode("utf-8") if isinstance(part, str) else bytes(part)
                for part in response.streaming_content
            )
        return bytes(response.content)
    finally:
        close = getattr(response, "close", None)
        if close is not None:
            close()


def _resolve_wsgi_path(url, resolver, make_request):
    """Resolve the decoded PATH_INFO produced by the WSGI request boundary."""
    req = make_request(url, {})
    return resolver(req.path_info)


def _native_archive_failure_message(
    response, request, channel_id, capability, root, user, views,
):
    """Classify a failed synthetic archive request without exposing secrets."""
    from catchuparr.xc_runtime import active_ts_session_count

    content = bytes(getattr(response, "content", b""))
    try:
        context_available = views._native_playback_context(
            request, capability, channel_id
        ) is not None
    except Exception:
        context_available = False
    try:
        existing_ts_session = active_ts_session_count(root, user.id) > 0
        session_count_available = True
    except Exception:
        existing_ts_session = False
        session_count_available = False
    diagnostics = {
        "context_available": bool(context_available),
        "stream_limit_category": content == b"stream limit exceeded",
        "forbidden_category": content == b"forbidden",
        "empty_denial": not content,
        "existing_ts_session": bool(existing_ts_session),
        "session_count_available": session_count_available,
    }
    return (
        f"Signed native archive request failed: {response.status_code}; "
        f"diagnostics={json.dumps(diagnostics, sort_keys=True)}"
    )


def probe_native_output(*, root, user, channel, channel_profile, start, runtime, request):
    from apps.accounts.models import User
    from apps.channels.models import (
        Channel,
        ChannelProfileMembership,
        ChannelStream,
        Stream,
    )
    from apps.m3u.connection_pool import profile_connections_key
    from apps.m3u.models import M3UAccount, M3UAccountProfile
    from apps.output import urls as output_urls
    from apps.plugins.models import PluginConfig
    from core.utils import RedisClient
    from django.core.cache import cache
    from django.urls import resolve, reverse
    from django.utils import timezone
    from rest_framework.test import APIClient
    from rest_framework_simplejwt.tokens import AccessToken

    from catchuparr import views
    from catchuparr.adapters.m3u import _parse_extinf_attributes
    from catchuparr.configuration import active_settings_path, load_active_configuration
    from catchuparr.engine.store import ArchiveStore
    from catchuparr.native_output_security import verify_native_capability

    require(bool(getattr(output_urls, "urlpatterns", None)),
            "Native output URL configuration is empty")
    m3u_url = reverse("output:m3u_endpoint", kwargs={"profile_name": channel_profile.name})
    epg_url = reverse("output:epg_endpoint", kwargs={"profile_name": channel_profile.name})
    archive_url = reverse("catchuparr-native-archive-hls")
    m3u_match = _resolve_wsgi_path(m3u_url, resolve, request)
    epg_match = _resolve_wsgi_path(epg_url, resolve, request)
    require(m3u_match.url_name == "m3u_endpoint"
            and m3u_match.kwargs.get("profile_name") == channel_profile.name,
            "Native M3U resolver did not receive decoded WSGI profile PATH_INFO")
    require(epg_match.url_name == "epg_endpoint"
            and epg_match.kwargs.get("profile_name") == channel_profile.name,
            "Native EPG resolver did not receive decoded WSGI profile PATH_INFO")
    require(_resolve_wsgi_path(archive_url, resolve, request).url_name == "catchuparr-native-archive-hls")

    # Exercise Django's actual reverse -> RequestFactory PATH_INFO -> resolver
    # boundary for spaces, literal percent signs and UTF-8 profile names. The
    # query assertion ensures this path normalization leaves QUERY_STRING data
    # to Django's normal, separate decoding path.
    for profile_name in ("Synthetic profile with spaces", "Synthetic 50% profile", "Synthetic λ profile"):
        encoded_profile_url = reverse(
            "output:m3u_endpoint", kwargs={"profile_name": profile_name}
        )
        profile_match = _resolve_wsgi_path(encoded_profile_url, resolve, request)
        require(profile_match.kwargs.get("profile_name") == profile_name,
                "Native profile URL did not resolve to its original profile name")
        query_probe = request(encoded_profile_url, {"query_probe": "literal%2Fvalue"})
        require(query_probe.GET.get("query_probe") == "literal%2Fvalue",
                "Native profile path handling changed an escaped query value")

    wrapped_names = {
        getattr(route, "name", None): getattr(route, "callback", None)
        for route in output_urls.urlpatterns
    }
    from catchuparr.native_output import _HOOK_MARKER

    require(all(getattr(wrapped_names.get(name), _HOOK_MARKER, False)
                for name in ("m3u_endpoint", "epg_endpoint")),
            "Native M3U and EPG callbacks were not both wrapped")

    output_params = {
        "output_profile": "73",
        "output_format": "hls",
        "tvg_id_source": "tvg_id",
    }

    def native_call(path, params=None, *, request_user=None, **headers):
        cache.clear()
        req = request(path, params or {}, REMOTE_ADDR="127.0.0.1",
                      HTTP_USER_AGENT="Synthetic Native Client/1", **headers)
        match = resolve(req.path_info)
        if request_user is not None:
            req.user = request_user
        return match.func(req, **match.kwargs)

    # Export a second channel with a duplicate tvg-id. The native renderer can
    # emit it, while the configured admission user hides adult content. XMLTV
    # mapping must see this collision before applying the admission ACL.
    outside = Channel.objects.create(
        name="Synthetic hidden channel",
        channel_number=81,
        user_level=0,
        is_adult=True,
        tvg_id="synthetic-native-shared",
    )
    account = M3UAccount.objects.create(name="Synthetic native output account", max_streams=2)
    stream = Stream.objects.create(
        name="Synthetic direct provider stream",
        url="http://127.0.0.1:1/synthetic.ts",
        m3u_account=account,
        tvg_id="synthetic-native-shared",
        is_catchup=True,
        catchup_days=7,
    )
    ChannelStream.objects.create(channel=outside, stream=stream, order=0)
    previous_stream_flags = list(
        Stream.objects.filter(channels=channel).values_list("id", "is_catchup", "catchup_days")
    )
    Stream.objects.filter(channels=channel).update(is_catchup=True, catchup_days=7)
    ChannelProfileMembership.objects.update_or_create(
        channel_profile=channel_profile,
        channel=outside,
        defaults={"enabled": True},
    )
    original_channel = {
        "tvg_id": channel.tvg_id,
        "is_catchup": channel.is_catchup,
        "catchup_days": channel.catchup_days,
    }
    channel.tvg_id = "synthetic-native-shared"
    channel.is_catchup = True
    channel.catchup_days = 7
    channel.save(update_fields=["tvg_id", "is_catchup", "catchup_days"])
    original_custom = dict(user.custom_properties or {})
    original_user_active = user.is_active
    selected_membership = ChannelProfileMembership.objects.get(
        channel_profile=channel_profile, channel=channel
    )
    original_membership_enabled = selected_membership.enabled
    user.custom_properties = {
        **(user.custom_properties or {}),
        "hide_adult_content": True,
    }
    user.save(update_fields=["custom_properties"])

    plugin_config = PluginConfig.objects.get(key="catchuparr")
    original_settings = dict(plugin_config.settings)
    active_before = load_active_configuration()
    require(active_before is not None and not active_before.get("native_output_catchup"),
            "Native output must start disabled in the integration fixture")

    profile_counters = {}
    redis = RedisClient.get_client()

    ChannelStream.objects.create(channel=channel, stream=stream, order=0)

    admin_user = None
    try:
        for source_profile in M3UAccountProfile.objects.all():
            profile_counters[source_profile.id] = int(
                redis.get(profile_connections_key(source_profile.id)) or 0
            )
        baseline_m3u = _response_body(native_call(m3u_url, output_params))
        baseline_epg = _response_body(native_call(epg_url, output_params))
        require(b"catchup-source=" not in baseline_m3u,
                "Disabled native output changed the original M3U")

        # Provider catch-up on a direct native URL remains untouched and is not
        # replaced by a Catchuparr capability.
        direct_baseline = _response_body(native_call(
            m3u_url, {**output_params, "direct": "true"}
        ))
        direct_m3u = direct_baseline.decode("utf-8")
        require("http://127.0.0.1:1/synthetic.ts" in direct_m3u
                and "token=" not in direct_m3u,
                "Native direct provider URL changed or gained a capability")

        settings = dict(original_settings)
        settings["native_output_catchup"] = True
        plugin_config.settings = settings
        plugin_config.save(update_fields=["settings"])
        runtime.apply_configuration()
        active = load_active_configuration()
        require(active is not None and active.get("native_output_catchup") is True,
                "Apply did not activate native output")
        key_id = active.get("native_output_key_id")
        require(key_id, "Apply did not activate a private native capability key")
        enabled_direct = _response_body(native_call(
            m3u_url, {**output_params, "direct": "true"}
        ))
        require(enabled_direct == direct_baseline,
                "Enabling archive augmentation changed native direct/provider output")

        # The HTTP caller may have a browser session, but these public native
        # endpoints render with callback user=None. Its capability therefore
        # stays portable through the configured anonymous admission account.
        m3u = _response_body(native_call(
            m3u_url, output_params, request_user=user,
        )).decode("utf-8")
        rows = m3u.splitlines()
        selected_rows = [
            (index, line)
            for index, line in enumerate(rows)
            if line.startswith("#EXTINF:")
            and _parse_extinf_attributes(line).get("tvg-id") == "synthetic-native-shared"
        ]
        require(len(selected_rows) >= 2,
                "Output profile must emit both selected and adult collision fixtures")
        cap_line, cap_url = next(
            (line, rows[index + 1]) for index, line in selected_rows
            if str(channel.uuid) in rows[index + 1]
        )
        cap_attrs = _parse_extinf_attributes(cap_line)
        require(cap_attrs.get("catchup") == "default"
                and "catchup-source" in cap_attrs,
                "Selected archived native channel did not receive catch-up metadata")
        require("output_profile=73" in cap_url and "output_format=hls" in cap_url,
                "Native stream URL lost its output-profile query parameters")
        adult_row = next(line for index, line in selected_rows
                         if str(outside.uuid) in rows[index + 1])
        require("catchup-source" not in _parse_extinf_attributes(adult_row),
                "Admission-user-hidden native channel received a capability")
        require(urlsplit(cap_attrs["catchup-source"]).path.endswith("archive.m3u8")
                and "{duration}" not in cap_attrs["catchup-source"],
                "Generic native export must identify HLS without a duration placeholder")
        cap_query = parse_qs(urlsplit(cap_attrs["catchup-source"].replace('"', "")).query)
        capability = cap_query["token"][0]
        channel_key = cap_query["channel_id"][0]
        claims = verify_native_capability(
            active_settings_path(),
            capability,
            current_key_id=key_id,
        )
        require(claims is not None and claims["channel"] == str(channel.uuid)
                and claims["profile"] == str(channel_profile.id)
                and claims["mode"] == "anonymous_admission",
                "Native capability was not bound to its selected profile and channel")
        require(m3u.splitlines()[0] == baseline_m3u.decode("utf-8").splitlines()[0]
                and "tvg_id_source=tvg_id" in m3u.splitlines()[0],
                "Native M3U header guide URL or query metadata changed")

        store = ArchiveStore(root)
        store.save_program_snapshot(
            str(channel.uuid), start, start + timedelta(seconds=12),
            "Synthetic archived programme",
        )
        epg_bytes = _response_body(native_call(epg_url, output_params))
        epg_root = ET.fromstring(epg_bytes)
        duplicate_id = "synthetic-native-shared"
        archived_titles = [
            node.findtext("title") for node in epg_root.findall("programme")
        ]
        require("Synthetic archived programme" not in archived_titles,
                "Ambiguous tvg-id attached archived history to an unsafe channel")
        outside.tvg_id = "synthetic-native-adult"
        outside.save(update_fields=["tvg_id"])
        unique_epg = ET.fromstring(_response_body(native_call(epg_url, output_params)))
        matching = [
            node for node in unique_epg.findall("programme")
            if node.get("channel") == duplicate_id
            and node.findtext("title") in {
                "Synthetic current", "Synthetic archived programme"
            }
        ]
        require(len(matching) == 1,
                "Native XMLTV did not deduplicate provider and local history by UTC instant")

        # Both network scopes and current user/channel/profile/toggle state are
        # checked again on actual native archive and segment routes.
        request_remote = request(archive_url, {
            "channel_id": channel_key,
            "token": capability,
            "utc": str(start.timestamp()),
            "duration": "12",
        }, REMOTE_ADDR="127.0.0.1", HTTP_USER_AGENT="Synthetic Native Client/1")
        archive_resolver = _resolve_wsgi_path(archive_url, resolve, request)
        archive = archive_resolver.func(request_remote, **archive_resolver.kwargs)
        if archive.status_code != 200:
            failure_message = _native_archive_failure_message(
                archive, request_remote, channel_key, capability, root, user,
                views,
            )
            require(False, failure_message)
        archive_body = archive.content.decode("utf-8")
        require("#EXT-X-ENDLIST" in archive_body,
                "Historical native catch-up must be a closed playlist")
        partial_request = request(archive_url, {
            "channel_id": channel_key, "token": capability,
            "utc": str(start.timestamp()), "duration": "{duration}",
            "lutc": str(int(timezone.now().timestamp())),
        }, REMOTE_ADDR="127.0.0.1", HTTP_USER_AGENT="Synthetic Native Client/1")
        partial_request.method = "HEAD"
        partial_archive = archive_resolver.func(partial_request, **archive_resolver.kwargs)
        require(partial_archive.status_code == 200 and not partial_archive.content,
                "Partially substituted catch-up template failed bounded HEAD validation")
        segment_url = next(line for line in archive_body.splitlines()
                           if line and not line.startswith("#"))
        segment_parts = urlsplit(segment_url)
        segment_query = {key: values[0] for key, values in parse_qs(segment_parts.query).items()}
        segment_match = _resolve_wsgi_path(segment_parts.path, resolve, request)
        segment_request = request(segment_parts.path, segment_query,
                                  REMOTE_ADDR="127.0.0.1",
                                  HTTP_USER_AGENT="Synthetic Native Client/1")
        segment = segment_match.func(
            segment_request, **segment_match.kwargs
        )
        require(segment.status_code == 200 and segment.content.startswith(b"\x47"),
                f"Native segment route failed: {segment.status_code}")
        require(segment["Cache-Control"] == "private, no-store"
                and segment["Referrer-Policy"] == "no-referrer",
                "Native archive segment response was cacheable or referable")
        range_request = request(
            segment_parts.path, segment_query, REMOTE_ADDR="127.0.0.1",
            HTTP_USER_AGENT="Synthetic Native Client/1", HTTP_RANGE="bytes=0-187",
        )
        ranged = segment_match.func(range_request, **segment_match.kwargs)
        require(ranged.status_code == 206 and len(ranged.content) == 188,
                "Native archive segment did not honor a real HTTP byte range")
        head_request = request(
            segment_parts.path, segment_query, REMOTE_ADDR="127.0.0.1",
            HTTP_USER_AGENT="Synthetic Native Client/1", HTTP_RANGE="bytes=0-187",
        )
        head_request.method = "HEAD"
        headed = segment_match.func(head_request, **segment_match.kwargs)
        require(headed.status_code == 206 and not headed.content,
                "Native archive segment HEAD did not preserve status without a body")

        user.custom_properties = {
            **original_custom,
            "allowed_networks": {
                "M3U_EPG": "127.0.0.0/8", "STREAMS": "192.0.2.0/24",
            },
        }
        user.save(update_fields=["custom_properties"])
        denied_segment = segment_match.func(segment_request, **segment_match.kwargs)
        require(denied_segment.status_code == 403,
                "Native segment ignored the current STREAMS network ACL")
        user.custom_properties = original_custom
        user.save(update_fields=["custom_properties"])
        membership = ChannelProfileMembership.objects.get(
            channel_profile=channel_profile, channel=channel
        )
        membership.enabled = False
        membership.save(update_fields=["enabled"])
        denied_segment = segment_match.func(segment_request, **segment_match.kwargs)
        require(denied_segment.status_code == 403,
                "Native segment ignored revoked current profile membership")
        membership.enabled = True
        membership.save(update_fields=["enabled"])
        user.is_active = False
        user.save(update_fields=["is_active"])
        denied_segment = segment_match.func(segment_request, **segment_match.kwargs)
        require(denied_segment.status_code == 403,
                "Native segment accepted an inactive configured admission user")
        user.is_active = True
        user.save(update_fields=["is_active"])
        user.custom_properties = original_custom
        user.save(update_fields=["custom_properties"])

        native_service = views._native_archive_service(
            request_remote, active, runtime.load_config(), user, claims,
            # Re-read the signing key through the same production path.
            __import__("catchuparr.native_output_security", fromlist=["load_native_key"])
            .load_native_key(active_settings_path(), key_id), capability,
        )
        lease = segment_query["lease"]
        with sqlite3.connect(root / "archive.sqlite3") as database:
            persisted = database.execute(
                "SELECT user_id, channel_id, device_key FROM http_playback_sessions "
                "WHERE lease_id=?",
                (lease,),
            ).fetchone()
            stat_identity = database.execute(
                "SELECT playback_device_key, playback_lease_id "
                "FROM catchuparr_stats_viewers WHERE user_id=? AND channel_uuid=? "
                "ORDER BY last_success_at DESC LIMIT 1",
                (str(user.id), str(channel.uuid)),
            ).fetchone()
        logical_identity = native_service.playback_device_identity(
            capability, str(user.id), str(channel.uuid)
        )
        require(persisted == (str(user.id), str(channel.uuid), logical_identity),
                "Native HTTP session did not use its stable scoped device identity")
        require(stat_identity == (logical_identity, lease),
                "Native successful playback Stats row did not match the exact HTTP lease")
        limit_request = request(
            archive_url,
            {"channel_id": channel_key, "token": capability,
             "utc": str(start.timestamp()), "duration": "12"},
            REMOTE_ADDR="127.0.0.2", HTTP_USER_AGENT="Synthetic Native Client/2",
        )
        limited = archive_resolver.func(limit_request, **archive_resolver.kwargs)
        require(limited.status_code == 403,
                "Native archive did not enforce the actual one-session user limit")
        admin_user = User.objects.create_user(
            username="synthetic-native-stop-admin",
            user_level=User.UserLevel.ADMIN,
            stream_limit=1,
        )
        stop_client = APIClient()
        stop_client.credentials(
            HTTP_AUTHORIZATION=f"Bearer {AccessToken.for_user(admin_user)}"
        )
        with sqlite3.connect(root / "archive.sqlite3") as database:
            display_row = database.execute(
                "SELECT display_id FROM catchuparr_stats_viewers "
                "WHERE playback_lease_id=?", (lease,),
            ).fetchone()
        require(display_row is not None, "Native playback has no Stats display identity")
        stop_response = stop_client.post(
            reverse("proxy:catchup:catchup_stop_client"),
            {"session_id": display_row[0]},
            format="json",
        )
        require(stop_response.status_code == 200
                and stop_response.json().get("success") is True,
                "Actual native Stats Stop endpoint did not revoke native playback")
        with sqlite3.connect(root / "archive.sqlite3") as database:
            still_active = database.execute(
                "SELECT 1 FROM http_playback_sessions WHERE lease_id=?", (lease,),
            ).fetchone()
            revoked = database.execute(
                "SELECT revoked FROM catchuparr_stats_viewers WHERE playback_lease_id=?",
                (lease,),
            ).fetchone()
        require(still_active is None and revoked == (1,),
                "Native Stats Stop did not remove the exact HTTP lease and revoke its row")
        stopped_segment = segment_match.func(segment_request, **segment_match.kwargs)
        require(stopped_segment.status_code in (403, 404, 410),
                "Stopped native Stats identity still served its archive segment")
        denied_ip = request(archive_url, {
            "channel_id": channel_key, "token": capability,
            "utc": str(start.timestamp()), "duration": "12",
        }, REMOTE_ADDR="127.0.0.1", HTTP_USER_AGENT="Synthetic Native Client/1")
        user.custom_properties = {
            **original_custom,
            "allowed_networks": {"M3U_EPG": "192.0.2.0/24"},
        }
        user.save(update_fields=["custom_properties"])
        denied = _resolve_wsgi_path(archive_url, resolve, request).func(denied_ip)
        require(denied.status_code == 403,
                "Native archive ignored the admission user's M3U_EPG network ACL")
        user.custom_properties = {
            **original_custom,
            "allowed_networks": {
                "M3U_EPG": "127.0.0.0/8",
                "STREAMS": "192.0.2.0/24",
            },
        }
        user.save(update_fields=["custom_properties"])
        denied = _resolve_wsgi_path(archive_url, resolve, request).func(denied_ip)
        require(denied.status_code == 403,
                "Native archive ignored the admission user's STREAMS network ACL")
        user.custom_properties = original_custom
        user.save(update_fields=["custom_properties"])

        # Current channel/profile membership and active user state are checked
        # on each native request, not only when the playlist is exported.
        membership = ChannelProfileMembership.objects.get(
            channel_profile=channel_profile, channel=channel
        )
        membership.enabled = False
        membership.save(update_fields=["enabled"])
        denied = _resolve_wsgi_path(archive_url, resolve, request).func(denied_ip)
        require(denied.status_code == 403,
                "Native archive ignored a revoked current profile membership")
        membership.enabled = True
        membership.save(update_fields=["enabled"])
        user.is_active = False
        user.save(update_fields=["is_active"])
        denied = _resolve_wsgi_path(archive_url, resolve, request).func(denied_ip)
        require(denied.status_code == 403,
                "Native archive accepted an inactive configured admission user")
        user.is_active = True
        user.save(update_fields=["is_active"])

        old_key_id = key_id
        settings["native_output_catchup"] = False
        plugin_config.settings = settings
        plugin_config.save(update_fields=["settings"])
        runtime.apply_configuration()
        denied = _resolve_wsgi_path(archive_url, resolve, request).func(denied_ip)
        require(denied.status_code == 403,
                "Disabling native output did not revoke an existing capability")
        settings["native_output_catchup"] = True
        plugin_config.settings = settings
        plugin_config.save(update_fields=["settings"])
        runtime.apply_configuration()
        reenabled = load_active_configuration()
        require(reenabled["native_output_key_id"] != old_key_id,
                "Re-enabling native output did not rotate the active signing key")
        denied = _resolve_wsgi_path(archive_url, resolve, request).func(denied_ip)
        require(denied.status_code == 403,
                "Re-enabled native output accepted a capability signed by the old key")

        # Restoring the disabled state guarantees this acceptance phase does
        # not leave the new feature active after the disposable test.
        outside.tvg_id = "synthetic-native-shared"
        outside.save(update_fields=["tvg_id"])
        settings["native_output_catchup"] = False
        plugin_config.settings = settings
        plugin_config.save(update_fields=["settings"])
        runtime.apply_configuration()
        after_m3u = _response_body(native_call(m3u_url, output_params))
        after_epg = _response_body(native_call(epg_url, output_params))
        require(after_m3u == baseline_m3u and after_epg == baseline_epg,
                "Disabling native output did not restore original native outputs")

        after_profiles = {
            profile.id: int(redis.get(profile_connections_key(profile.id)) or 0)
            for profile in M3UAccountProfile.objects.all()
        }
        require(after_profiles == profile_counters,
                "Native export or playback created extra provider-profile connections")
        require(_response_body(native_call(
            m3u_url, {**output_params, "direct": "true"}
        )) == direct_baseline,
                "Native direct/provider output changed when archive augmentation was enabled")
    finally:
        if admin_user is not None:
            admin_user.delete()
        user.is_active = original_user_active
        user.save(update_fields=["is_active"])
        user.custom_properties = original_custom
        user.save(update_fields=["custom_properties"])
        selected_membership.enabled = original_membership_enabled
        selected_membership.save(update_fields=["enabled"])
        plugin_config.settings = original_settings
        plugin_config.save(update_fields=["settings"])
        runtime.apply_configuration()
        ChannelStream.objects.filter(channel=outside).delete()
        ChannelStream.objects.filter(channel=channel, stream=stream).delete()
        channel.tvg_id = original_channel["tvg_id"]
        channel.is_catchup = original_channel["is_catchup"]
        channel.catchup_days = original_channel["catchup_days"]
        channel.save(update_fields=list(original_channel))
        for stream_id, is_catchup, catchup_days in previous_stream_flags:
            Stream.objects.filter(id=stream_id).update(
                is_catchup=is_catchup, catchup_days=catchup_days,
            )
        outside.delete()
        stream.delete()
        account.delete()
