import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catchuparr.engine import Segment, build_hls_playlist


class HLSPlaylistTests(unittest.TestCase):
    def setUp(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.segments = [
            Segment("a", "ch", Path("a.ts"), start, start + timedelta(seconds=6)),
            Segment("b", "ch", Path("b.ts"), start + timedelta(seconds=6), start + timedelta(seconds=12), True),
        ]

    def test_live_event_is_appendable_and_has_program_date_time(self):
        text = build_hls_playlist(self.segments, live=True, uri_for=lambda seg: f"/archive/{seg.id}")
        self.assertIn("#EXT-X-PLAYLIST-TYPE:EVENT", text)
        self.assertIn("#EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:00.000Z", text)
        self.assertIn("#EXT-X-DISCONTINUITY", text)
        self.assertNotIn("#EXT-X-ENDLIST", text)
        self.assertIn("/archive/a", text)

    def test_finished_event_playlist_closes_and_rejects_newlines_in_uri(self):
        text = build_hls_playlist(self.segments, live=False)
        self.assertIn("#EXT-X-PLAYLIST-TYPE:EVENT", text)
        self.assertTrue(text.endswith("#EXT-X-ENDLIST\n"))
        growing = build_hls_playlist(self.segments, live=True)
        self.assertEqual(text, growing + "#EXT-X-ENDLIST\n")
        with self.assertRaises(ValueError):
            build_hls_playlist(self.segments[:1], live=True, uri_for=lambda _: "bad\nuri")

    def test_event_target_duration_is_fixed_and_checks_long_gop(self):
        short_playlist = build_hls_playlist(self.segments[:1], live=True, target_duration=45)
        long_gop = Segment("long", "ch", Path("long.ts"), self.segments[-1].start_utc,
                           self.segments[-1].start_utc + timedelta(seconds=40))
        later_playlist = build_hls_playlist([*self.segments, long_gop], live=True, target_duration=45)
        self.assertIn("#EXT-X-TARGETDURATION:45", short_playlist)
        self.assertIn("#EXT-X-TARGETDURATION:45", later_playlist)
        too_long = Segment("huge", "ch", Path("huge.ts"), self.segments[-1].start_utc,
                           self.segments[-1].start_utc + timedelta(seconds=46))
        with self.assertRaisesRegex(ValueError, "fixed HLS target"):
            build_hls_playlist([too_long], live=True, target_duration=45)


if __name__ == "__main__":
    unittest.main()
