import types
import unittest
from datetime import datetime, timedelta, timezone

from catchuparr.adapters.xc import (
    SUPPORTED_DISPATCHARR_VERSION,
    XCCallbacks,
    install_xc_hooks,
    uninstall_xc_hooks,
)


class Response:
    def __init__(self, content="", status=200):
        self.content = content
        self.status_code = status
        self.headers = {}

    def __getitem__(self, key):
        return self.headers[key]

    def __setitem__(self, key, value):
        self.headers[key] = value


class Request:
    def __init__(self, stream_id="8", *, get=None, method="GET"):
        self.GET = dict(get or {"stream_id": stream_id})
        self.method = method


class Channel:
    id = 8
    uuid = "channel-8-uuid"


def modules():
    calls = {
        "entry": 0, "epg": 0, "serve": 0, "m3u": 0,
        "epg_lookbacks": [], "programmes": None,
        "m3u_content": "#EXTM3U\n",
    }

    def _xc_channel_entry(channel, channel_num_map, _get_default_group_id,
                          _logo_url_prefix, _logo_url_suffix, *, catchup_allowed=True):
        calls["entry"] += 1
        return {"stream_id": channel.id, "tv_archive": 0, "tv_archive_duration": 0}

    def xc_get_epg(request, user, short=False):
        calls["epg"] += 1
        calls["epg_lookbacks"].append(request.GET.get("prev_days"))
        programmes = calls["programmes"] or [{
            "start": "2026-01-01 10:00:00",
            "end": "2026-01-01 11:00:00",
            "has_archive": 0,
        }]
        return {"epg_listings": programmes}

    def _serve_catchup(request, user, channel, timestamp, client_duration_hint=None):
        calls["serve"] += 1
        return Response("provider")

    def generate_m3u(request, profile_name=None, user=None):
        calls["m3u"] += 1
        return Response(calls["m3u_content"])

    output = types.SimpleNamespace(
        _xc_channel_entry=_xc_channel_entry,
        xc_get_epg=xc_get_epg,
        generate_m3u=generate_m3u,
        build_absolute_uri_with_port=lambda request, path: "https://dispatcharr.test" + path,
        is_catchup_enabled=lambda user: user.get("catchup", True),
        HttpResponse=Response,
        HttpResponseForbidden=lambda message: Response(message, 403),
    )
    timeshift = types.SimpleNamespace(
        _serve_catchup=_serve_catchup,
        is_catchup_enabled=lambda user: user.get("catchup", True),
        parse_catchup_timestamp=lambda value: value if value else None,
        HttpResponse=Response,
        HttpResponseForbidden=lambda message: Response(message, 403),
    )
    return output, timeshift, calls


class XCHookInstallTests(unittest.TestCase):
    def test_version_matrix_accepts_inspected_releases_and_rejects_unknown(self):
        for version in ("0.31.0", "0.32.0"):
            with self.subTest(version=version):
                output, timeshift, _ = modules()
                result = install_xc_hooks(
                    output, timeshift, dispatcharr_version=version, callbacks=XCCallbacks(),
                )
                self.assertTrue(result.installed)
        output, timeshift, _ = modules()
        original = output._xc_channel_entry
        result = install_xc_hooks(
            output, timeshift, dispatcharr_version="0.33.0", callbacks=XCCallbacks(),
        )
        self.assertFalse(result.installed)
        self.assertIs(output._xc_channel_entry, original)

    def test_rejects_signature_mismatch_without_partial_patch(self):
        output, timeshift, _ = modules()
        original = output.xc_get_epg
        output.xc_get_epg = lambda request, user: {}
        entry = output._xc_channel_entry
        result = install_xc_hooks(
            output, timeshift, dispatcharr_version=SUPPORTED_DISPATCHARR_VERSION,
            callbacks=XCCallbacks(),
        )
        self.assertFalse(result.installed)
        self.assertIs(output._xc_channel_entry, entry)
        self.assertIsNot(output.xc_get_epg, original)

    def test_installs_idempotently(self):
        output, timeshift, _ = modules()
        callbacks = XCCallbacks()
        first = install_xc_hooks(
            output, timeshift, dispatcharr_version=SUPPORTED_DISPATCHARR_VERSION,
            callbacks=callbacks,
        )
        wrapper = output._xc_channel_entry
        second = install_xc_hooks(
            output, timeshift, dispatcharr_version=SUPPORTED_DISPATCHARR_VERSION,
            callbacks=callbacks,
        )
        self.assertTrue(first.installed)
        self.assertTrue(second.installed)
        self.assertIs(output._xc_channel_entry, wrapper)

    def test_032_m3u_hook_signature_is_guarded_without_partial_install(self):
        output, timeshift, _ = modules()
        original_entry = output._xc_channel_entry
        output.generate_m3u = lambda request, user=None: Response("#EXTM3U\n")

        result = install_xc_hooks(
            output, timeshift, dispatcharr_version="0.32.0", callbacks=XCCallbacks(),
        )

        self.assertFalse(result.installed)
        self.assertIs(output._xc_channel_entry, original_entry)


class XCHookBehaviorTests(unittest.TestCase):
    def install(self, callbacks):
        output, timeshift, calls = modules()
        result = install_xc_hooks(
            output, timeshift, dispatcharr_version=SUPPORTED_DISPATCHARR_VERSION,
            callbacks=callbacks,
        )
        self.assertTrue(result.installed)
        return output, timeshift, calls

    def test_channel_entry_exposes_local_retention_only_when_allowed(self):
        output, _, calls = self.install(XCCallbacks(channel_archive_days=lambda channel: 9))
        args = (Channel(), {}, lambda: 1, "prefix", "suffix")
        enabled = output._xc_channel_entry(*args, catchup_allowed=True)
        disabled = output._xc_channel_entry(*args, catchup_allowed=False)
        self.assertEqual(enabled["tv_archive"], 1)
        self.assertEqual(enabled["tv_archive_duration"], 9)
        self.assertEqual(disabled["tv_archive"], 0)
        self.assertEqual(calls["entry"], 2)

    def test_epg_only_marks_fully_covered_local_programmes(self):
        output, _, calls = self.install(XCCallbacks(
            channel_uuid_for_epg_id=lambda channel_id, user: "channel-8-uuid",
            program_available=lambda channel_uuid, start, end, user:
                channel_uuid == "channel-8-uuid" and end.endswith("11:00:00"),
        ))
        result = output.xc_get_epg(Request(), {"catchup": True})
        self.assertEqual(result["epg_listings"][0]["has_archive"], 1)
        self.assertEqual(calls["epg"], 1)
        result = output.xc_get_epg(Request(), {"catchup": False})
        self.assertEqual(result["epg_listings"][0]["has_archive"], 0)

    def test_epg_annotation_does_not_mutate_core_cached_result(self):
        output, _, calls = self.install(XCCallbacks(
            channel_uuid_for_epg_id=lambda channel_id, user: "channel-8-uuid",
            program_available=lambda *args: True,
        ))
        cached_listing = {
            "start": "2026-01-01 10:00:00",
            "end": "2026-01-01 11:00:00",
            "has_archive": 0,
            "provider_metadata": {"category": "radio"},
        }
        calls["programmes"] = [cached_listing]
        result = output.xc_get_epg(Request(), {"catchup": True})
        self.assertEqual(cached_listing["has_archive"], 0)
        self.assertEqual(result["epg_listings"][0]["has_archive"], 1)
        self.assertIsNot(result["epg_listings"], calls["programmes"])
        self.assertIsNot(result["epg_listings"][0], cached_listing)
        self.assertEqual(result["epg_listings"][0]["provider_metadata"], {"category": "radio"})

    def test_epg_requests_local_lookback_on_a_copy_and_preserves_larger_setting(self):
        output, _, calls = self.install(XCCallbacks(
            channel_uuid_for_epg_id=lambda channel_id, user: "channel-8-uuid",
            epg_archive_days=lambda channel_id, user: 7,
        ))
        request = Request()
        output.xc_get_epg(request, {"catchup": True})
        self.assertEqual(calls["epg_lookbacks"], [None, "7"])
        self.assertNotIn("prev_days", request.GET)

        calls["epg_lookbacks"].clear()
        request.GET["prev_days"] = "12"
        output.xc_get_epg(request, {"catchup": True})
        self.assertEqual(calls["epg_lookbacks"], ["12", "12"])

    def test_local_epg_snapshots_fill_programmes_removed_by_core_refresh(self):
        output, _, calls = self.install(XCCallbacks(
            channel_uuid_for_epg_id=lambda channel_id, user: "channel-8-uuid",
            epg_archive_days=lambda channel_uuid, user: 5,
            epg_snapshots=lambda channel_uuid, user, days: [{
                "start": "2026-09-01 10:00:00",
                "end": "2026-09-01 11:00:00",
                "title": "c25",
                "has_archive": 0,
            }],
            program_available=lambda channel_uuid, start, end, user: True,
        ))
        result = output.xc_get_epg(Request(), {"catchup": True})
        listings = result["epg_listings"]
        self.assertEqual(len(listings), 2)
        self.assertEqual(listings[1]["title"], "c25")
        self.assertEqual(listings[1]["has_archive"], 1)
        self.assertEqual(calls["epg_lookbacks"], [None, "5"])

    def test_hook_reload_updates_callbacks_and_uninstall_restores_core(self):
        output, timeshift, _ = modules()
        originals = (output._xc_channel_entry, output.xc_get_epg, timeshift._serve_catchup)
        first = install_xc_hooks(
            output, timeshift, dispatcharr_version=SUPPORTED_DISPATCHARR_VERSION,
            callbacks=XCCallbacks(channel_archive_days=lambda channel: 4),
        )
        wrapper = output._xc_channel_entry
        reloaded = install_xc_hooks(
            output, timeshift, dispatcharr_version=SUPPORTED_DISPATCHARR_VERSION,
            callbacks=XCCallbacks(channel_archive_days=lambda channel: 8),
        )
        self.assertTrue(first.installed)
        self.assertTrue(reloaded.installed)
        self.assertIs(output._xc_channel_entry, wrapper)
        entry = output._xc_channel_entry(Channel(), {}, lambda: 1, "p", "s")
        self.assertEqual(entry["tv_archive_duration"], 8)
        removed = uninstall_xc_hooks(output, timeshift)
        self.assertTrue(removed.installed)
        self.assertEqual((output._xc_channel_entry, output.xc_get_epg, timeshift._serve_catchup), originals)

    def test_032_m3u_adds_local_template_after_core_filtering_and_preserves_provider_values(self):
        output, timeshift, calls = modules()
        provider_source = "https://provider.example/timeshift?start={utc}"
        calls["m3u_content"] = (
            "#EXTM3U\n"
            '#EXTINF:-1 tvg-name="Provider" catchup="default" catchup-days="1" '
            f'catchup-source="{provider_source}",Provider\n'
            "https://dispatcharr.test/live/viewer/secret/8\n"
            '#EXTINF:-1 tvg-name="Local",Local\n'
            "https://dispatcharr.test/live/viewer/secret/9\n"
            '#EXTINF:-1 tvg-name="Unselected",Unselected\n'
            "https://dispatcharr.test/live/viewer/secret/10\n"
        )
        callbacks = XCCallbacks(
            authorize_xc_m3u=lambda request, user: True,
            m3u_channel_archive_days=lambda user: {"8": 5, "9": 5},
        )
        self.assertTrue(install_xc_hooks(
            output, timeshift, dispatcharr_version="0.32.0", callbacks=callbacks,
        ).installed)
        request = Request(get={
            "username": "viewer", "password": "secret", "direct": "false",
        })
        user = {"catchup": True}

        response = output.generate_m3u(request, user=user)
        content = response.content

        self.assertIn(f'catchup-days="5" catchup-source="{provider_source}"', content)
        self.assertIn(
            'catchup="default" catchup-source="https://dispatcharr.test/streaming/'
            'timeshift.php?username=viewer&password=secret&stream=9&utc={utc}'
            '&duration={duration:60}" catchup-days="5"',
            content,
        )
        self.assertIn(
            '#EXTINF:-1 tvg-name="Unselected",Unselected\n'
            "https://dispatcharr.test/live/viewer/secret/10\n",
            content,
        )
        repeated = output.generate_m3u(request, user=user)
        self.assertEqual(repeated.content, content)
        self.assertEqual(content.count('catchup-source="'), 2)
        self.assertEqual(calls["m3u"], 2)

    def test_032_m3u_hook_skips_direct_and_unauthorized_requests(self):
        output, timeshift, calls = modules()
        authorization_calls = []
        callbacks = XCCallbacks(
            authorize_xc_m3u=lambda request, user: authorization_calls.append(True) or False,
            m3u_channel_archive_days=lambda user: {"8": 5},
        )
        self.assertTrue(install_xc_hooks(
            output, timeshift, dispatcharr_version="0.32.0", callbacks=callbacks,
        ).installed)
        user = {"catchup": True}
        direct = output.generate_m3u(Request(get={
            "username": "viewer", "password": "secret", "direct": "true",
        }), user=user)
        denied = output.generate_m3u(Request(get={
            "username": "viewer", "password": "secret",
        }), user=user)

        self.assertEqual(direct.content, "#EXTM3U\n")
        self.assertEqual(denied.content, "#EXTM3U\n")
        self.assertEqual(authorization_calls, [True])
        self.assertEqual(calls["m3u"], 2)

    def test_031_native_m3u_uses_core_start_query_alias(self):
        output, timeshift, calls = modules()
        calls["m3u_content"] = (
            "#EXTM3U\n#EXTINF:-1,Local\n"
            "https://dispatcharr.test/live/viewer/secret/8\n"
        )
        callbacks = XCCallbacks(
            authorize_xc_m3u=lambda request, user: True,
            m3u_channel_archive_days=lambda user: {"8": 3},
        )
        self.assertTrue(install_xc_hooks(
            output, timeshift, dispatcharr_version="0.31.0", callbacks=callbacks,
        ).installed)

        response = output.generate_m3u(Request(get={
            "username": "viewer", "password": "secret", "direct": "false",
        }), user={"catchup": True})

        self.assertIn(
            'catchup-source="https://dispatcharr.test/streaming/timeshift.php?'
            'username=viewer&password=secret&stream=8&start={utc}&duration={duration:60}"',
            response.content,
        )

    def test_running_programme_availability_receives_full_programme_window(self):
        checked = []
        output, _, calls = self.install(XCCallbacks(
            channel_uuid_for_epg_id=lambda channel_id, user: "channel-8-uuid",
            program_available=lambda channel_uuid, programme_start, coverage_end, user:
                checked.append((programme_start, coverage_end)) or True,
        ))
        # The mocked provider response is now expressed relative to wall clock as well.
        now = datetime.now(timezone.utc)
        start = (now - timedelta(minutes=20)).strftime("%Y-%m-%d %H:%M:%S")
        future_end = (now + timedelta(minutes=40)).strftime("%Y-%m-%d %H:%M:%S")
        calls["programmes"] = [{"start": start, "end": future_end, "has_archive": 0}]
        result = output.xc_get_epg(Request(), {"catchup": True})
        self.assertEqual(result["epg_listings"][0]["has_archive"], 1)
        self.assertEqual(checked[0][0], start)
        self.assertEqual(checked[0][1], future_end)

    def test_provider_path_is_preserved_when_local_programme_missing(self):
        _, timeshift, calls = self.install(XCCallbacks(playback_available=lambda *args: False))
        response = timeshift._serve_catchup(Request(), {"catchup": True}, Channel(), "2026-01-01:10-00")
        self.assertEqual(response.content, "provider")
        self.assertEqual(calls["serve"], 1)

    def test_non_xc_catchup_route_stays_on_core_path(self):
        _, timeshift, calls = self.install(XCCallbacks(
            local_playback_supported=lambda *args: False,
            playback_available=lambda *args: True,
            authorize_local_playback=lambda *args: True,
            serve_local_playback=lambda *args: Response("local"),
        ))

        response = timeshift._serve_catchup(
            Request(), {"catchup": True}, Channel(), "2026-01-01:10-00",
        )

        self.assertEqual(response.content, "provider")
        self.assertEqual(calls["serve"], 1)

    def test_local_coverage_without_security_contract_fails_closed(self):
        _, timeshift, calls = self.install(XCCallbacks(playback_available=lambda *args: True))
        response = timeshift._serve_catchup(Request(), {"catchup": True}, Channel(), "2026-01-01:10-00")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(calls["serve"], 0)

    def test_invalid_timestamp_stays_on_dispatcharr_validation_path(self):
        _, timeshift, calls = self.install(XCCallbacks(
            playback_available=lambda *args: True,
            authorize_local_playback=lambda *args: True,
            serve_local_playback=lambda *args: Response("local"),
        ))
        timeshift.parse_catchup_timestamp = lambda value: None
        response = timeshift._serve_catchup(
            Request(), {"catchup": True}, Channel(), "bad-timestamp",
        )
        self.assertEqual(response.content, "provider")
        self.assertEqual(calls["serve"], 1)

    def test_local_playback_requires_explicit_authorization(self):
        served = []
        callbacks = XCCallbacks(
            playback_available=lambda *args: True,
            authorize_local_playback=lambda *args: False,
            serve_local_playback=lambda *args: served.append(True) or Response("local"),
        )
        _, timeshift, calls = self.install(callbacks)
        response = timeshift._serve_catchup(Request(), {"catchup": True}, Channel(), "2026-01-01:10-00")
        self.assertEqual(response.status_code, 403)
        self.assertEqual(served, [])
        self.assertEqual(calls["serve"], 0)

    def test_authorized_local_playback_calls_archive_callback(self):
        observed = []
        callbacks = XCCallbacks(
            playback_available=lambda channel_uuid, timestamp, duration, user:
                observed.append((channel_uuid, duration)) or True,
            authorize_local_playback=lambda *args: True,
            serve_local_playback=lambda *args: Response("local"),
        )
        _, timeshift, calls = self.install(callbacks)
        for duration_hint in (None, 44):
            response = timeshift._serve_catchup(
                Request(), {"catchup": True}, Channel(), "2026-01-01:10-00",
                client_duration_hint=duration_hint,
            )
            self.assertEqual(response.content, "local")
        self.assertEqual(calls["serve"], 0)
        self.assertEqual(observed, [("channel-8-uuid", None), ("channel-8-uuid", 44)])

    def test_coverage_and_authorization_errors_fall_back_or_fail_closed(self):
        output, timeshift, calls = self.install(XCCallbacks(
            playback_available=lambda *args: (_ for _ in ()).throw(RuntimeError("index down")),
        ))
        response = timeshift._serve_catchup(Request(), {"catchup": True}, Channel(), "2026-01-01:10-00")
        self.assertEqual(response.content, "provider")
        self.assertEqual(calls["serve"], 1)
        # A local authorization failure cannot flow to provider or unprotected local output.
        output, timeshift, calls = self.install(XCCallbacks(
            playback_available=lambda *args: True,
            authorize_local_playback=lambda *args: (_ for _ in ()).throw(RuntimeError("policy down")),
            serve_local_playback=lambda *args: Response("local"),
        ))
        response = timeshift._serve_catchup(Request(), {"catchup": True}, Channel(), "2026-01-01:10-00")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(calls["serve"], 0)


if __name__ == "__main__":
    unittest.main()
