from datetime import datetime, timedelta, timezone
from pathlib import Path
import unittest

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

    def test_vod_playlist_closes_and_rejects_newlines_in_uri(self):
        text = build_hls_playlist(self.segments, live=False)
        self.assertIn("#EXT-X-PLAYLIST-TYPE:VOD", text)
        self.assertTrue(text.endswith("#EXT-X-ENDLIST\n"))
        with self.assertRaises(ValueError):
            build_hls_playlist(self.segments[:1], live=True, uri_for=lambda _: "bad\nuri")


if __name__ == "__main__":
    unittest.main()
