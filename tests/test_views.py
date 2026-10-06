import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from catchuparr.views import (
    _access_token,
    _archive_window_live,
    _catchup_epoch,
    _core_request,
    _epg_request,
    _network_allowed,
    _selected_proxy_channels,
    _trace_component,
    _trace_range,
)


class ViewBoundaryTests(unittest.TestCase):
    def test_utc_epoch_and_iso_timestamp_agree(self):
        epoch = _catchup_epoch("2026-10-05T12:00:00+02:00")
        self.assertEqual(epoch, _catchup_epoch(str(int(epoch))))

    def test_rejects_ambiguous_and_nonfinite_time(self):
        for timestamp in ("2026-10-05T12:00:00", "nan", "inf", ""):
            with self.subTest(timestamp=timestamp), self.assertRaises(ValueError):
                _catchup_epoch(timestamp)

    def test_core_request_excludes_bearer_values_from_cache_key(self):
        request = SimpleNamespace(GET={"access_token": "secret", "days": "2"})
        core = _core_request(request)
        self.assertEqual(core.GET, {"days": "2"})
        self.assertIn("access_token", request.GET)

    def test_epg_request_preserves_requested_provider_history(self):
        request = SimpleNamespace(GET={"access_token": "secret", "prev_days": "14"})
        copied = _epg_request(request, retention_hours=24)
        self.assertEqual(copied.GET, {"prev_days": "14", "days": "2"})
        self.assertEqual(request.GET["access_token"], "secret")

    def test_header_token_takes_precedence_over_url_token(self):
        request = SimpleNamespace(
            headers={"X-Catchuparr-Token": "private-header"},
            GET={"access_token": "url-value"},
        )
        self.assertEqual(_access_token(request), "private-header")

    def test_archive_requires_stream_network_permission_as_well_as_playlist_permission(self):
        checked = []

        def checker(_request, area, _user):
            checked.append(area)
            return area != "STREAMS"

        self.assertTrue(_network_allowed(None, None, checker, playback=False))
        self.assertEqual(checked, ["M3U_EPG"])
        checked.clear()
        self.assertFalse(_network_allowed(None, None, checker, playback=True))
        self.assertEqual(checked, ["M3U_EPG", "STREAMS"])

    def test_archive_authorization_does_not_depend_on_epg_ids(self):
        first = "00000000-0000-4000-8000-000000000001"
        second = "00000000-0000-4000-8000-000000000002"
        playlist = (
            f'#EXTINF:-1 tvg-id="shared",First\nhttp://host/proxy/ts/stream/{first}\n'
            f'#EXTINF:-1 tvg-id="shared",Second\nhttp://host/proxy/ts/stream/{second}\n'
            f'#EXTINF:-1,Other\nhttp://host/proxy/ts/stream/{first}\n'
        )
        self.assertEqual(_selected_proxy_channels(playlist, (first, second)), {first, second})

    def test_recently_ended_programme_waits_for_final_indexed_segment(self):
        tail = []
        service = SimpleNamespace(
            authorize_user_channel=lambda *_: True,
            catchup_enabled=lambda *_: True,
            store=SimpleNamespace(segments=lambda *_: tail),
        )
        self.assertTrue(_archive_window_live(service, "viewer", "news", 1000, 1010))
        tail.append(SimpleNamespace(end_utc=datetime.fromtimestamp(1000, timezone.utc)))
        self.assertFalse(_archive_window_live(service, "viewer", "news", 1000, 1010))
        tail.clear()
        self.assertFalse(_archive_window_live(service, "viewer", "news", 1000, 1120))

    def test_request_trace_redacts_untrusted_values(self):
        self.assertEqual(_trace_range("bytes=188-563"), "bytes=188-563")
        self.assertEqual(_trace_range("bytes=0-188,376-"), "bytes=0-188,376-")
        self.assertEqual(_trace_range("bytes=0-\nsecret"), "other")
        self.assertEqual(_trace_component("123e4567-e89b-12d3-a456-426614174000"),
                         "123e4567-e89b-12d3-a456-426614174000")
        self.assertEqual(_trace_component("token=secret"), "invalid")


if __name__ == "__main__":
    unittest.main()
