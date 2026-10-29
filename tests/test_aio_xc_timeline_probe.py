import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catchuparr.engine.store import ArchiveStore
from catchuparr.http import ArchiveHTTPService
from catchuparr.security import TokenStore
from scripts.aio_xc_timeline_probe import _hls_transition_prefix


class NativeTimelineFixtureTests(unittest.TestCase):
    def test_continuous_playlist_checks_exact_transition_and_keeps_later_media(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "synthetic.ts"
            source.write_bytes(b"\x47" + bytes(187))
            store = ArchiveStore(root / "archive")
            start = datetime(2026, 1, 1, tzinfo=timezone.utc)
            segments = [
                store.add_segment(
                    "synthetic", source,
                    start + timedelta(seconds=offset), start + timedelta(seconds=offset + 6),
                    discontinuity=(index == 1),
                )
                for index, offset in enumerate((0, 12, 1800))
            ]
            tokens = TokenStore(store.root)
            token = tokens.create("synthetic-viewer")
            service = ArchiveHTTPService(
                store, tokens, authorize_user_channel=lambda *_: True,
                catchup_enabled=lambda *_: True,
            )
            response = service.playlist(
                token, "synthetic", start, start + timedelta(hours=24), continuous=True,
            )
            self.assertEqual(response.status, 200)
            text = response.body.decode()
            self.assertEqual(sum(bool(line) and not line.startswith("#") for line in text.splitlines()), 3)
            prefix, urls, later = _hls_transition_prefix(text, [item.id for item in segments[:2]])
            self.assertEqual(len(urls), 2)
            self.assertEqual(later, 1)
            self.assertEqual(prefix.splitlines()[-1], "#EXT-X-ENDLIST")
            self.assertNotIn(segments[2].id, prefix)
            with self.assertRaisesRegex(RuntimeError, "exact synthetic source transition"):
                _hls_transition_prefix(text.replace("#EXT-X-DISCONTINUITY\n", ""),
                                       [item.id for item in segments[:2]])
            with self.assertRaisesRegex(RuntimeError, "exact synthetic transition segments"):
                _hls_transition_prefix(text, [segments[1].id, segments[0].id])


if __name__ == "__main__":
    unittest.main()
