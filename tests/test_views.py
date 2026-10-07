import types
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

from catchuparr.views import (
    _access_token,
    _archive_epg_bounds,
    _archive_playback_window,
    _archive_window_live,
    _catchup_epoch,
    _core_request,
    _epg_request,
    _network_allowed,
    _playlist_segment_bounds,
    _selected_proxy_channels,
    _trace_archive_request,
    _trace_component,
    _trace_range,
    _trace_segment_request,
    archive_view,
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

    def test_shifted_tivimate_windows_stop_at_the_actual_epg_programme_end(self):
        programme_start = datetime(2026, 10, 7, 15, 15, tzinfo=timezone.utc)
        programme_end = datetime(2026, 10, 7, 16, 0, tzinfo=timezone.utc)
        next_end = datetime(2026, 10, 7, 16, 45, tzinfo=timezone.utc)

        class ProgrammeQuery:
            def __init__(self, rows):
                self.rows = rows

            def filter(self, **filters):
                rows = self.rows
                if "start_time__lte" in filters:
                    rows = [
                        row for row in rows
                        if row.start_time <= filters["start_time__lte"]
                    ]
                if "end_time__gt" in filters:
                    rows = [
                        row for row in rows
                        if row.end_time > filters["end_time__gt"]
                    ]
                return ProgrammeQuery(rows)

            def order_by(self, *_fields):
                return self

            def first(self):
                return self.rows[0] if self.rows else None

        current = SimpleNamespace(start_time=programme_start, end_time=programme_end)
        following = SimpleNamespace(start_time=programme_end, end_time=next_end)
        channel = SimpleNamespace(
            # The raw relation can differ from the override-selected guide.
            epg_data=SimpleNamespace(programs=ProgrammeQuery([])),
            effective_epg_data_obj=SimpleNamespace(
                programs=ProgrammeQuery([current, following])
            ),
        )
        manager = SimpleNamespace(
            filter=lambda **_filters: SimpleNamespace(
                select_related=lambda *_fields: SimpleNamespace(first=lambda: channel)
            )
        )
        models = types.ModuleType("apps.channels.models")
        models.Channel = SimpleNamespace(objects=manager)
        apps = types.ModuleType("apps")
        apps.__path__ = []
        apps_channels = types.ModuleType("apps.channels")
        apps_channels.__path__ = []
        with patch.dict("sys.modules", {
            "apps": apps,
            "apps.channels": apps_channels,
            "apps.channels.models": models,
        }):
            current_end, following_end = _archive_epg_bounds(
                "channel-1",
                datetime(2026, 10, 7, 15, 44, tzinfo=timezone.utc).timestamp(),
            )

        self.assertEqual(current_end, programme_end.timestamp())
        self.assertEqual(following_end, next_end.timestamp())
        start_1515 = programme_start.timestamp()
        start_1528 = datetime(2026, 10, 7, 15, 28, tzinfo=timezone.utc).timestamp()
        start_1544 = datetime(2026, 10, 7, 15, 44, tzinfo=timezone.utc).timestamp()
        self.assertEqual(
            _archive_playback_window(
                start_1515, 2700, programme_end.timestamp(), next_end.timestamp()
            ),
            (programme_end.timestamp(), next_end.timestamp()),
        )
        self.assertEqual(
            _archive_playback_window(
                start_1528, 2700, programme_end.timestamp(), next_end.timestamp()
            ),
            (
                programme_end.timestamp(),
                datetime(2026, 10, 7, 16, 13, tzinfo=timezone.utc).timestamp(),
            ),
        )
        self.assertEqual(
            _archive_playback_window(
                start_1544, 2700, programme_end.timestamp(), next_end.timestamp()
            ),
            (programme_end.timestamp(), datetime(2026, 10, 7, 16, 29, tzinfo=timezone.utc).timestamp()),
        )

    def test_missing_epg_keeps_the_requested_archive_window(self):
        start = datetime(2026, 10, 7, 15, 44, tzinfo=timezone.utc).timestamp()
        self.assertEqual(_archive_playback_window(start, 2700, None, None), (start + 2700, None))

    def test_archive_request_without_matching_epg_returns_404(self):
        class FakeResponse:
            def __init__(self, content="", status=200):
                self.content = content
                self.status_code = status
                self.headers = {}

            def __setitem__(self, key, value):
                self.headers[key] = value

        django = types.ModuleType("django")
        django.__path__ = []
        django_http = types.ModuleType("django.http")
        django_http.HttpResponse = FakeResponse
        now = datetime.now(timezone.utc)
        request = SimpleNamespace(
            method="GET",
            GET={
                "channel_id": "channel-1",
                "utc": str((now - timedelta(minutes=5)).timestamp()),
                "duration": "2700",
            },
            headers={},
        )
        user = SimpleNamespace(id="viewer")
        service = SimpleNamespace(
            authorize_user_channel=lambda *_: True,
            catchup_enabled=lambda *_: True,
            store=SimpleNamespace(),
        )
        with patch.dict("sys.modules", {"django": django, "django.http": django_http}):
            with patch("catchuparr.views._authenticate", return_value=(
                user, SimpleNamespace(retention_hours=48), "token"
            )), patch("catchuparr.views._archive_service", return_value=service), patch(
                "catchuparr.views._archive_epg_bounds", return_value=(None, None)
            ):
                response = archive_view(request)
        self.assertEqual(response.status_code, 404)

    def test_historical_xmltv_snapshot_remains_playable_after_guide_refresh(self):
        class FakeResponse:
            def __init__(self, content="", status=200):
                self.content = content
                self.status_code = status
                self.headers = {}

            def __setitem__(self, key, value):
                self.headers[key] = value

        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=2)
        end = start + timedelta(minutes=45)
        snapshot = {
            "start_utc": start,
            "end_utc": end,
            "captured_at": now - timedelta(hours=3),
        }
        store = SimpleNamespace(
            program_snapshots=lambda _channel, left, right: [
                snapshot
            ] if left < end.timestamp() and right > start.timestamp() else [],
            coverage=lambda *_args: SimpleNamespace(complete=True),
        )
        service = SimpleNamespace(
            authorize_user_channel=lambda *_: True,
            catchup_enabled=lambda *_: True,
            store=store,
            playlist=Mock(return_value=SimpleNamespace(status=200)),
        )
        request = SimpleNamespace(
            method="GET",
            GET={
                "channel_id": "channel-1",
                "utc": str(start.timestamp()),
                "duration": "2700",
            },
            headers={},
        )
        django = types.ModuleType("django")
        django.__path__ = []
        django_http = types.ModuleType("django.http")
        django_http.HttpResponse = FakeResponse
        models = types.ModuleType("apps.channels.models")
        models.Channel = SimpleNamespace(objects=SimpleNamespace(
            filter=lambda **_filters: SimpleNamespace(
                select_related=lambda *_fields: SimpleNamespace(first=lambda: SimpleNamespace(
                    effective_epg_data_obj=None
                ))
            )
        ))
        apps = types.ModuleType("apps")
        apps.__path__ = []
        apps_channels = types.ModuleType("apps.channels")
        apps_channels.__path__ = []
        with patch.dict("sys.modules", {
            "django": django,
            "django.http": django_http,
            "apps": apps,
            "apps.channels": apps_channels,
            "apps.channels.models": models,
        }), patch("catchuparr.views._authenticate", return_value=(
            SimpleNamespace(id="viewer"), SimpleNamespace(retention_hours=48), "token"
        )), patch("catchuparr.views._archive_service", return_value=service), patch(
            "catchuparr.views._archive_window_live", return_value=False
        ), patch("catchuparr.views._to_django_response", return_value=FakeResponse(
            status=200
        )):
            response = archive_view(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(service.playlist.call_args.args[2:4], (
            start.timestamp(), end.timestamp()
        ))

    def test_full_length_initial_programme_can_continue_after_boundary(self):
        start = datetime(2026, 10, 7, 15, 15, tzinfo=timezone.utc).timestamp()
        programme_end = start + 45 * 60
        following_end = programme_end + 45 * 60
        self.assertEqual(
            _archive_playback_window(
                start, 45 * 60, programme_end, following_end
            ),
            (programme_end, following_end),
        )

    def test_shifted_archive_continuation_is_capped_at_24_hours(self):
        start = datetime(2026, 10, 7, 15, 0, tzinfo=timezone.utc).timestamp()
        programme_end = start + 60 * 60
        next_end = start + 30 * 60 * 60
        self.assertEqual(
            _archive_playback_window(
                start, 24 * 60 * 60, programme_end, next_end
            ),
            (programme_end, start + 24 * 60 * 60),
        )

    def test_playlist_trace_reports_times_without_logging_segment_urls(self):
        response = SimpleNamespace(
            body=(
                b"#EXT-X-PROGRAM-DATE-TIME:2026-10-07T15:59:54.000Z\n"
                b"#EXTINF:6.000,\n/segment/news/a?token=secret&lease=secret\n"
            ),
            status=200,
        )
        self.assertEqual(
            _playlist_segment_bounds(response.body),
            (
                datetime(2026, 10, 7, 15, 59, 54, tzinfo=timezone.utc).timestamp(),
                datetime(2026, 10, 7, 16, 0, tzinfo=timezone.utc).timestamp(),
            ),
        )
        with patch.dict("os.environ", {"CATCHUPARR_TRACE_REQUESTS": "1"}):
            with self.assertLogs("catchuparr.views", level="WARNING") as logs:
                _trace_archive_request(None, 1791388794, 2700, 1791390000, response)
        self.assertIn("epg_end=1791390000.000", logs.output[0])
        self.assertNotIn("secret", logs.output[0])

    def test_request_trace_redacts_untrusted_values(self):
        self.assertEqual(_trace_range("bytes=188-563"), "bytes=188-563")
        self.assertEqual(_trace_range("bytes=0-188,376-"), "bytes=0-188,376-")
        self.assertEqual(_trace_range("bytes=0-\nsecret"), "other")
        self.assertEqual(_trace_component("123e4567-e89b-12d3-a456-426614174000"),
                         "123e4567-e89b-12d3-a456-426614174000")
        self.assertEqual(_trace_component("token=secret"), "invalid")

    def test_segment_trace_is_warning_with_only_whitelisted_fields(self):
        request = SimpleNamespace(
            method="GET",
            headers={"Range": "bytes=0-"},
            GET={"lease": "lease-secret", "token": "token-secret"},
        )
        with patch.dict("os.environ", {"CATCHUPARR_TRACE_REQUESTS": "1"}):
            with self.assertLogs("catchuparr.views", level="WARNING") as logs:
                _trace_segment_request(
                    request,
                    "123e4567-e89b-12d3-a456-426614174000",
                    "123e4567-e89b-12d3-a456-426614174001",
                    206,
                )
        self.assertIn("method=GET", logs.output[0])
        self.assertIn("range=bytes=0-", logs.output[0])
        self.assertIn("status=206", logs.output[0])
        self.assertNotIn("secret", logs.output[0])


if __name__ == "__main__":
    unittest.main()
