import tempfile
import unittest
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from catchuparr.engine.store import ArchiveStore
from catchuparr.http import ArchiveHTTPService, parse_byte_range
from catchuparr.security import TokenStore


@dataclass(frozen=True)
class Segment:
    id: str
    channel_id: str
    path: Path
    start_utc: datetime
    end_utc: datetime
    discontinuity: bool = False

    @property
    def duration(self):
        return (self.end_utc - self.start_utc).total_seconds()


class FakeArchive:
    def __init__(self, root, segments):
        self.root = Path(root)
        self.items = segments
        self.leases = {}

    def segments(self, channel_id, start_utc=None, end_utc=None):
        selected = [s for s in self.items if s.channel_id == channel_id]
        if start_utc is not None:
            left = float(start_utc.timestamp()) if isinstance(start_utc, datetime) else float(start_utc)
            selected = [s for s in selected if s.end_utc.timestamp() > left]
        if end_utc is not None:
            right = float(end_utc.timestamp()) if isinstance(end_utc, datetime) else float(end_utc)
            selected = [s for s in selected if s.start_utc.timestamp() < right]
        return selected

    def begin_playback(self, channel, start, end, *, ttl_seconds):
        key = uuid.uuid4().hex
        lease = SimpleNamespace(id=key, expires_at=9999999999)
        self.leases[key] = {"channel": channel, "start": float(start), "end": float(end)}
        return lease

    def renew_playback(self, lease_id, *, ttl_seconds):
        return lease_id in self.leases

    def extend_playback(self, lease_id, end, *, ttl_seconds):
        if lease_id not in self.leases:
            return False
        self.leases[lease_id]["end"] = max(self.leases[lease_id]["end"], float(end))
        return True

    def segment(self, channel_id, segment_id):
        return next(
            (item for item in self.items if item.channel_id == channel_id and item.id == segment_id),
            None,
        )

    def end_playback(self, lease_id):
        self.leases.pop(lease_id, None)


def _builder(segments, *, live, uri_for):
    lines = ["#EXTM3U", "#EXT-X-PLAYLIST-TYPE:" + ("EVENT" if live else "VOD")]
    for item in segments:
        lines.extend([f"#EXTINF:{item.duration:.3f},", uri_for(item)])
    if not live:
        lines.append("#EXT-X-ENDLIST")
    return "\n".join(lines) + "\n"


class ArchiveHTTPTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.segment_path = self.root / "fixture.ts"
        self.segment_path.write_bytes(b"0123456789")
        self.segment_item = Segment("seg-A", "news", self.segment_path, self.start, self.start + timedelta(seconds=6))
        self.archive = FakeArchive(self.root, [self.segment_item])
        self.tokens = TokenStore(self.root)
        self.token = self.tokens.create("user-a")
        self.service = ArchiveHTTPService(
            self.archive,
            self.tokens,
            authorize_user_channel=lambda user, channel: user == "user-a" and channel == "news",
            catchup_enabled=lambda user, channel: True,
            playlist_builder=_builder,
        )

    def tearDown(self):
        self.temp.cleanup()

    def _playlist(self):
        return self.service.playlist(
            self.token,
            "news",
            self.start,
            self.start + timedelta(seconds=10),
        )

    def test_playlist_has_authenticated_segment_url_and_lease(self):
        response = self._playlist()
        self.assertEqual(response.status, 200)
        text = response.body.decode()
        self.assertIn("#EXT-X-PLAYLIST-TYPE:VOD", text)
        self.assertIn("/catchuparr/segment/news/seg-A?token=", text)
        self.assertIn("&lease=", text)
        self.assertEqual(len(self.archive.leases), 1)

    def test_segment_get_head_and_byte_range(self):
        playlist = self._playlist().body.decode()
        lease_id = playlist.split("&lease=")[1].splitlines()[0]

        response = self.service.segment(self.token, "news", "seg-A", lease_id, range_header="bytes=2-5")
        self.assertEqual(response.status, 206)
        self.assertEqual(response.body, b"2345")
        self.assertEqual(response.headers["Content-Range"], "bytes 2-5/10")
        self.assertEqual(response.headers["Accept-Ranges"], "bytes")
        self.assertEqual(response.headers["Content-Type"], "video/mp2t")

        head = self.service.segment(self.token, "news", "seg-A", lease_id, method="HEAD")
        self.assertEqual(head.status, 200)
        self.assertEqual(head.body, b"")
        self.assertEqual(head.headers["Content-Length"], "10")

    def test_invalid_range_returns_416(self):
        lease_id = self._playlist().body.decode().split("&lease=")[1].splitlines()[0]
        response = self.service.segment(self.token, "news", "seg-A", lease_id, range_header="bytes=50-")
        self.assertEqual(response.status, 416)
        self.assertEqual(response.headers["Content-Range"], "bytes */10")
        self.assertEqual(response.body, b"")

    def test_token_permissions_channel_and_lease_are_enforced(self):
        self.assertEqual(self.service.playlist("wrong", "news", 1, 2).status, 401)
        self.assertEqual(self.service.playlist(self.token, "other", 1, 2).status, 403)
        self.assertEqual(self.service.segment(self.token, "news", "seg-A", "missing").status, 403)

        playlist = self._playlist().body.decode()
        lease_id = playlist.split("&lease=")[1].splitlines()[0]
        other_token = self.tokens.create("user-b")
        self.assertEqual(self.service.segment(other_token, "news", "seg-A", lease_id).status, 403)

    def test_revoked_token_cannot_read_archive(self):
        self.tokens.revoke(self.token)
        self.assertEqual(self._playlist().status, 401)

    def test_event_reload_reuses_lease_and_appends_segments(self):
        end = self.start + timedelta(minutes=30)
        first = self.service.playlist(self.token, "news", self.start, end, live=True)
        self.assertEqual(first.status, 200)
        first_text = first.body.decode()
        first_lease = first_text.split("&lease=")[1].splitlines()[0]
        second_path = self.root / "second.ts"
        second_path.write_bytes(b"abcdefghij")
        self.archive.items.append(
            Segment("seg-B", "news", second_path, self.start + timedelta(seconds=6),
                    self.start + timedelta(seconds=12))
        )
        second = self.service.playlist(self.token, "news", self.start, end, live=True)
        second_text = second.body.decode()
        self.assertEqual(second.status, 200)
        self.assertTrue(second_text.startswith(first_text))
        self.assertIn(f"&lease={first_lease}", second_text)
        self.assertEqual(len(self.archive.leases), 1)
        self.assertEqual(self.service.segment(self.token, "news", "seg-A", first_lease).status, 200)

    def test_event_closure_preserves_segment_urls_and_lease(self):
        self.service.playlist_builder = None
        end = self.start + timedelta(minutes=30)
        growing = self.service.playlist(self.token, "news", self.start, end, live=True)
        closed = self.service.playlist(self.token, "news", self.start, end, live=False)
        self.assertEqual(growing.status, 200)
        self.assertEqual(closed.status, 200)
        self.assertEqual(closed.body, growing.body + b"#EXT-X-ENDLIST\n")
        self.assertEqual(len(self.archive.leases), 1)

    def test_new_session_limit_blocks_other_program_but_allows_reload(self):
        self.service.allow_new_session = lambda user, channel, count: count < 1
        first = self._playlist()
        self.assertEqual(first.status, 200)
        self.assertEqual(self._playlist().status, 200)
        different = self.service.playlist(
            self.token, "news", self.start + timedelta(seconds=1),
            self.start + timedelta(seconds=10),
        )
        self.assertEqual(different.status, 403)


class RangeParserTests(unittest.TestCase):
    def test_open_suffix_and_clamped_ranges(self):
        self.assertEqual(parse_byte_range("bytes=4-", 10).start, 4)
        self.assertEqual(parse_byte_range("bytes=-3", 10).start, 7)
        self.assertEqual(parse_byte_range("bytes=8-99", 10).end, 9)
        self.assertIsNone(parse_byte_range(None, 10))

    def test_bad_and_multi_ranges_are_unsatisfiable(self):
        for header in ("items=0-1", "bytes=", "bytes=0-1,4-5", "bytes=-0", "bytes=10-"):
            with self.subTest(header=header):
                with self.assertRaises(ValueError):
                    parse_byte_range(header, 10)


class RealStoreHTTPTests(unittest.TestCase):
    def test_playlist_lease_protects_cleanup_and_indexed_range_read(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.ts"
            source.write_bytes(b"0123456789")
            store = ArchiveStore(root / "archive")
            start = datetime(2026, 1, 1, tzinfo=timezone.utc)
            item = store.add_segment("news", source, start, start + timedelta(seconds=6))
            tokens = TokenStore(store.root)
            token = tokens.create("viewer")
            service = ArchiveHTTPService(
                store, tokens,
                authorize_user_channel=lambda user, channel: user == "viewer" and channel == "news",
                catchup_enabled=lambda *_: True,
            )
            playlist = service.playlist(token, "news", start, start + timedelta(seconds=10))
            self.assertEqual(playlist.status, 200)
            lease_id = playlist.body.decode().split("&lease=")[1].splitlines()[0]
            self.assertEqual(
                store.cleanup(older_than_utc=start + timedelta(days=1), max_bytes=0),
                [],
            )
            response = service.segment(token, "news", item.id, lease_id, range_header="bytes=3-6")
            self.assertEqual((response.status, response.body), (206, b"3456"))


if __name__ == "__main__":
    unittest.main()
