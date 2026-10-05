import unittest
from types import SimpleNamespace

from catchuparr.views import (
    _access_token,
    _catchup_epoch,
    _core_request,
    _network_allowed,
    _selected_proxy_channels,
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


if __name__ == "__main__":
    unittest.main()
