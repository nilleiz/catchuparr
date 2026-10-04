from datetime import datetime, timezone
from pathlib import Path
import tempfile
import threading
import unittest

from catchuparr.engine import ArchiveStore
from catchuparr.engine.recorder import FFmpegCopyRecorder


class RecorderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = ArchiveStore(self.root / "archive")
        self.recorder = FFmpegCopyRecorder(self.store, "ch1", "http://dispatcharr/proxy/ts/stream/id", self.root / "work")

    def tearDown(self):
        self.temp.cleanup()

    def test_command_uses_dispatcharr_source_and_stream_copy_segment_muxer(self):
        args = self.recorder.command(self.root / "output")
        self.assertIn("http://dispatcharr/proxy/ts/stream/id", args)
        self.assertIn("copy", args)
        self.assertIn("segment", args)
        self.assertIn("-segment_list_type", args)
        self.assertNotIn("-vf", args)
        self.assertNotIn("-af", args)

    def test_completed_csv_rows_are_indexed_only_after_file_exists(self):
        out = self.root / "session"
        out.mkdir()
        media = out / "segment-000000.ts"
        media.write_bytes(b"synthetic ts")
        listing = out / "segments.csv"
        listing.write_text('segment-000000.ts,0.000000,6.000000\npartial,6.000000,12.000000', encoding="utf-8")
        anchor = datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()
        offset, count = self.recorder._publish_csv_rows(listing, 0, anchor)
        self.assertEqual(1, count)
        self.assertEqual(len('segment-000000.ts,0.000000,6.000000\n'), offset)
        segments = self.store.segments("ch1")
        self.assertEqual(1, len(segments))
        self.assertEqual(datetime(2026, 1, 1, tzinfo=timezone.utc), segments[0].start_utc)
        self.assertFalse(media.exists())
        offset2, count2 = self.recorder._publish_csv_rows(listing, offset, anchor)
        self.assertEqual((offset, 0), (offset2, count2))


if __name__ == "__main__":
    unittest.main()
