import multiprocessing
import sqlite3
import tempfile
import threading
import time
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
        expiry = time.time() + ttl_seconds
        lease = SimpleNamespace(id=key, expires_at=expiry)
        self.leases[key] = {
            "channel": channel, "start": float(start), "end": float(end),
            "expires_at": expiry,
        }
        return lease

    def renew_playback(self, lease_id, *, ttl_seconds):
        lease = self.leases.get(lease_id)
        if lease is None or lease["expires_at"] <= time.time():
            return False
        lease["expires_at"] = time.time() + ttl_seconds
        return True

    def extend_playback(self, lease_id, end, *, ttl_seconds):
        if lease_id not in self.leases:
            return False
        self.leases[lease_id]["end"] = max(self.leases[lease_id]["end"], float(end))
        self.leases[lease_id]["expires_at"] = time.time() + ttl_seconds
        return True

    def segment(self, channel_id, segment_id):
        return next(
            (item for item in self.items if item.channel_id == channel_id and item.id == segment_id),
            None,
        )

    def end_playback(self, lease_id):
        self.leases.pop(lease_id, None)


def _builder(segments, *, live, uri_for, start_offset=None):
    lines = ["#EXTM3U", "#EXT-X-PLAYLIST-TYPE:" + ("EVENT" if live else "VOD")]
    if start_offset is not None:
        lines.append(f"#EXT-X-START:TIME-OFFSET={start_offset:.3f}")
    for item in segments:
        lines.extend([f"#EXTINF:{item.duration:.3f},", uri_for(item)])
    if not live:
        lines.append("#EXT-X-ENDLIST")
    return "\n".join(lines) + "\n"


def _concurrent_playlist_request(root, token, barrier, results, start, end):
    store = ArchiveStore(root)
    tokens = TokenStore(root)
    service = ArchiveHTTPService(
        store,
        tokens,
        authorize_user_channel=lambda user, channel: user == "user-a" and channel == "news",
        catchup_enabled=lambda *_: True,
        allow_new_session=lambda _user, _channel, count: time.sleep(0.1) is None and count < 1,
    )
    barrier.wait(timeout=10)
    response = service.playlist(token, "news", start, end)
    results.put(response.status)


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

    def test_seek_playlist_stays_within_program_after_tail_get_or_range(self):
        self.service.playlist_builder = None
        programme_start = self.start + timedelta(hours=15, minutes=15)
        programme_end = programme_start + timedelta(minutes=45)
        seek_start = programme_start + timedelta(minutes=29)
        requested_end = seek_start + timedelta(seconds=2700)

        def add_segment(segment_id, start, end):
            path = self.root / f"{segment_id}.ts"
            path.write_bytes(b"0123456789")
            item = Segment(segment_id, "news", path, start, end)
            self.archive.items.append(item)
            return item

        add_segment(
            "seek-first",
            seek_start - timedelta(seconds=2),
            seek_start + timedelta(seconds=4),
        )
        boundary = add_segment(
            "seek-boundary",
            programme_end - timedelta(seconds=6),
            programme_end,
        )
        crossing = add_segment(
            "crossing-program-boundary",
            programme_end - timedelta(seconds=1),
            programme_end + timedelta(seconds=5),
        )
        next_program = add_segment(
            "next-program", programme_end + timedelta(seconds=5),
            programme_end + timedelta(seconds=11),
        )

        def reload():
            return self.service.playlist(
                self.token,
                "news",
                seek_start,
                requested_end,
                live=False,
                request_identity_end=requested_end,
                programme_end_utc=programme_end,
                continuation_end_utc=requested_end,
            )

        initial = reload()
        initial_text = initial.body.decode()
        self.assertEqual(initial.status, 200)
        self.assertEqual(initial_text.count("#EXTINF:"), 2)
        self.assertIn("#EXT-X-PLAYLIST-TYPE:EVENT", initial_text)
        self.assertTrue(initial_text.endswith("#EXT-X-ENDLIST\n"))
        self.assertIn("#EXT-X-START:TIME-OFFSET=2.000", initial_text)
        self.assertNotIn("next-program", initial_text)
        self.assertNotIn(crossing.id, initial_text)
        initial_lines = initial_text.splitlines()
        first_uri = next(line for line in initial_lines if "seek-first" in line)
        lease_id = first_uri.split("&lease=", 1)[1]

        # Loading the complete tail, including a full byte range, cannot
        # extend the session beyond the selected EPG programme.
        self.assertEqual(self.service.segment(
            self.token, "news", boundary.id, lease_id
        ).status, 200)
        self.assertEqual(self.service.segment(
            self.token, "news", boundary.id, lease_id, method="HEAD"
        ).status, 200)
        self.assertEqual(self.service.segment(
            self.token, "news", boundary.id, lease_id, range_header="bytes=0-"
        ).status, 206)
        self.assertEqual(
            self.service.segment(self.token, "news", crossing.id, lease_id).status,
            403,
        )
        self.assertEqual(reload().body.decode(), initial_text)
        self.assertEqual(
            self.archive.leases[lease_id]["end"], programme_end.timestamp()
        )
        self.assertNotIn(next_program.id, reload().body.decode())
        self.assertEqual(
            self.service.segment(self.token, "news", next_program.id, lease_id).status,
            403,
        )

    def test_upgrade_invalidates_sessions_that_already_crossed_program_boundary(self):
        response = self._playlist()
        lease_id = response.body.decode().split("&lease=", 1)[1].splitlines()[0]
        extended_end = (self.start + timedelta(seconds=30)).timestamp()
        with sqlite3.connect(self.root / "archive.sqlite3") as db:
            db.execute(
                "UPDATE http_playback_sessions SET end_utc=?,programme_end_utc=?,"
                "continuation_reached=1 WHERE lease_id=?",
                (extended_end, self.start.timestamp() + 6, lease_id),
            )
            db.execute("DELETE FROM http_playback_schema_migrations WHERE version=1")
        self.archive.extend_playback(lease_id, extended_end, ttl_seconds=300)

        self.service = ArchiveHTTPService(
            self.archive,
            self.tokens,
            authorize_user_channel=lambda user, channel: user == "user-a" and channel == "news",
            catchup_enabled=lambda *_: True,
        )

        with sqlite3.connect(self.root / "archive.sqlite3") as db:
            self.assertIsNone(db.execute(
                "SELECT lease_id FROM http_playback_sessions WHERE lease_id=?",
                (lease_id,),
            ).fetchone())
        self.assertNotIn(lease_id, self.archive.leases)
        self.assertEqual(
            self.service.segment(self.token, "news", "seg-A", lease_id).status,
            403,
        )

    def test_upgrade_invalidates_manifest_with_cross_boundary_segment(self):
        response = self._playlist()
        lease_id = response.body.decode().split("&lease=", 1)[1].splitlines()[0]
        programme_end = (self.start + timedelta(seconds=5)).timestamp()
        with sqlite3.connect(self.root / "archive.sqlite3") as db:
            db.execute(
                "UPDATE http_playback_sessions SET end_utc=?,programme_end_utc=?,"
                "continuation_reached=0 WHERE lease_id=?",
                (programme_end, programme_end, lease_id),
            )
            db.execute("DELETE FROM http_playback_schema_migrations WHERE version=1")

        self.service = ArchiveHTTPService(
            self.archive,
            self.tokens,
            authorize_user_channel=lambda user, channel: user == "user-a" and channel == "news",
            catchup_enabled=lambda *_: True,
        )

        with sqlite3.connect(self.root / "archive.sqlite3") as db:
            self.assertIsNone(db.execute(
                "SELECT lease_id FROM http_playback_sessions WHERE lease_id=?",
                (lease_id,),
            ).fetchone())
        self.assertNotIn(lease_id, self.archive.leases)

    def test_event_closure_preserves_segment_urls_and_lease(self):
        self.service.playlist_builder = None
        end = self.start + timedelta(minutes=30)
        growing = self.service.playlist(self.token, "news", self.start, end, live=True)
        closed = self.service.playlist(self.token, "news", self.start, end, live=False)
        self.assertEqual(growing.status, 200)
        self.assertEqual(closed.status, 200)
        self.assertEqual(closed.body, growing.body + b"#EXT-X-ENDLIST\n")
        self.assertEqual(len(self.archive.leases), 1)

        later_path = self.root / "later.ts"
        later_path.write_bytes(b"later media")
        later = Segment(
            "seg-later", "news", later_path,
            self.start + timedelta(seconds=6), self.start + timedelta(seconds=12),
        )
        self.archive.items.append(later)
        self.assertEqual(
            self.service.playlist(self.token, "news", self.start, end, live=True).body,
            closed.body,
        )
        self.archive.items.remove(self.segment_item)
        restarted = ArchiveHTTPService(
            self.archive,
            self.tokens,
            authorize_user_channel=lambda user, channel: user == "user-a" and channel == "news",
            catchup_enabled=lambda *_: True,
        )
        self.assertEqual(
            restarted.playlist(self.token, "news", self.start, end, live=True).body,
            closed.body,
        )

    def test_muxer_offset_does_not_emit_discontinuity_but_real_gap_does(self):
        self.service.playlist_builder = None
        for segment_id, offset in (("near", 6.14), ("after-gap", 12.54)):
            path = self.root / f"{segment_id}.ts"
            path.write_bytes(b"transport stream")
            self.archive.items.append(Segment(
                segment_id, "news", path,
                self.start + timedelta(seconds=offset),
                self.start + timedelta(seconds=offset + 6),
            ))
        playlist = self.service.playlist(
            self.token, "news", self.start, self.start + timedelta(seconds=20), live=True
        )
        self.assertEqual(playlist.status, 200)
        self.assertEqual(playlist.body.count(b"#EXT-X-DISCONTINUITY"), 1)

    def test_new_session_limit_blocks_other_device_but_allows_reload(self):
        self.service.allow_new_session = lambda user, channel, count: count < 1
        first = self._playlist()
        self.assertEqual(first.status, 200)
        self.assertEqual(self._playlist().status, 200)
        other_device = self.tokens.create("user-a")
        different = self.service.playlist(
            other_device, "news", self.start + timedelta(seconds=1),
            self.start + timedelta(seconds=10),
        )
        self.assertEqual(different.status, 403)

    def test_same_device_switch_replaces_active_session_and_keeps_old_urls_briefly(self):
        service = ArchiveHTTPService(
            self.archive,
            self.tokens,
            authorize_user_channel=lambda user, channel: user == "user-a" and channel == "news",
            catchup_enabled=lambda *_: True,
            allow_new_session=lambda _user, _channel, count: count < 2,
            playlist_builder=_builder,
            replacement_grace_seconds=0.5,
        )
        first = service.playlist(self.token, "news", self.start, self.start + timedelta(seconds=10))
        first_lease = first.body.decode().split("&lease=")[1].splitlines()[0]
        switched = service.playlist(
            self.token, "news", self.start + timedelta(seconds=1), self.start + timedelta(seconds=11)
        )
        self.assertEqual(switched.status, 200)
        switched_lease = switched.body.decode().split("&lease=")[1].splitlines()[0]
        self.assertNotEqual(switched_lease, first_lease)

        with sqlite3.connect(self.root / "archive.sqlite3") as db:
            rows = db.execute(
                "SELECT lease_id,grace_until FROM http_playback_sessions ORDER BY lease_id"
            ).fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            {lease_id for lease_id, grace_until in rows if grace_until is None},
            {switched_lease},
        )
        self.assertEqual({lease_id for lease_id, grace_until in rows if grace_until is not None}, {first_lease})
        old_expiry = self.archive.leases[first_lease]["expires_at"]
        self.assertLessEqual(old_expiry, time.time() + 0.5)

        reload = service.playlist(
            self.token, "news", self.start + timedelta(seconds=1), self.start + timedelta(seconds=11)
        )
        self.assertEqual(reload.status, 200)
        self.assertIn(f"&lease={switched_lease}", reload.body.decode())
        # Grace rows are excluded from admission counts, so a second device can
        # use the remaining slot even while the old segment URLs are valid.
        other_device = self.tokens.create("user-a")
        second_device = service.playlist(
            other_device, "news", self.start, self.start + timedelta(seconds=10)
        )
        self.assertEqual(second_device.status, 200)
        self.assertEqual(service.segment(self.token, "news", "seg-A", first_lease).status, 200)
        time.sleep(0.55)
        self.assertEqual(service.segment(self.token, "news", "seg-A", first_lease).status, 403)

    def test_same_device_switch_fits_within_a_one_session_limit(self):
        service = ArchiveHTTPService(
            self.archive,
            self.tokens,
            authorize_user_channel=lambda user, channel: user == "user-a" and channel == "news",
            catchup_enabled=lambda *_: True,
            allow_new_session=lambda _user, _channel, count: count < 1,
            playlist_builder=_builder,
        )
        self.assertEqual(
            service.playlist(self.token, "news", self.start, self.start + timedelta(seconds=10)).status,
            200,
        )
        switched = service.playlist(
            self.token, "news", self.start + timedelta(seconds=1), self.start + timedelta(seconds=11)
        )
        self.assertEqual(switched.status, 200)

    def test_playlist_builder_failure_releases_session_and_lease(self):
        def fail_builder(*_args, **_kwargs):
            raise RuntimeError("playlist generation failed")

        self.service.playlist_builder = fail_builder
        response = self._playlist()
        self.assertEqual(response.status, 503)
        self.assertEqual(self.archive.leases, {})
        with sqlite3.connect(self.root / "archive.sqlite3") as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM http_playback_sessions").fetchone()[0], 0
            )

    def test_failed_reload_keeps_the_previously_delivered_lease(self):
        first = self._playlist()
        old_lease = first.body.decode().split("&lease=")[1].splitlines()[0]
        self.service.playlist_builder = lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("temporary render failure")
        )
        self.assertEqual(self._playlist().status, 503)
        self.assertEqual(self.service.segment(self.token, "news", "seg-A", old_lease).status, 200)
        self.service.playlist_builder = _builder
        self.assertIn(f"&lease={old_lease}", self._playlist().body.decode())

    def test_failed_switch_restores_the_previous_active_programme(self):
        self.service.allow_new_session = lambda _user, _channel, count: count < 1
        first = self._playlist()
        old_lease = first.body.decode().split("&lease=")[1].splitlines()[0]
        self.service.playlist_builder = lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("temporary render failure")
        )
        switched = self.service.playlist(
            self.token, "news", self.start + timedelta(seconds=1),
            self.start + timedelta(seconds=10),
        )
        self.assertEqual(switched.status, 503)
        self.assertEqual(self.service.segment(self.token, "news", "seg-A", old_lease).status, 200)
        self.service.playlist_builder = _builder
        self.assertIn(f"&lease={old_lease}", self._playlist().body.decode())

    def test_slow_failed_switch_preserves_old_store_lease_past_grace_window(self):
        service = ArchiveHTTPService(
            self.archive,
            self.tokens,
            authorize_user_channel=lambda user, channel: user == "user-a" and channel == "news",
            catchup_enabled=lambda *_: True,
            allow_new_session=lambda _user, _channel, count: count < 2,
            playlist_builder=_builder,
            replacement_grace_seconds=0.05,
        )
        first = service.playlist(
            self.token, "news", self.start, self.start + timedelta(seconds=10)
        )
        old_lease = first.body.decode().split("&lease=")[1].splitlines()[0]

        entered = threading.Event()
        release = threading.Event()
        results = []
        other_started = threading.Event()
        other_results = []

        def slow_failure(*_args, **_kwargs):
            entered.set()
            if not release.wait(timeout=5):
                raise RuntimeError("test did not release failed render")
            raise RuntimeError("replacement render failed")

        service.playlist_builder = slow_failure
        worker = threading.Thread(
            target=lambda: results.append(
                service.playlist(
                    self.token, "news", self.start + timedelta(seconds=1),
                    self.start + timedelta(seconds=10),
                ).status
            )
        )
        worker.start()
        self.assertTrue(entered.wait(timeout=5))
        time.sleep(0.1)
        service.playlist_builder = _builder
        other_device = self.tokens.create("user-a")
        other_worker = threading.Thread(
            target=lambda: (
                other_started.set(),
                other_results.append(
                    service.playlist(
                        other_device, "news", self.start,
                        self.start + timedelta(seconds=10),
                    ).status
                ),
            )
        )
        with sqlite3.connect(self.root / "archive.sqlite3") as db:
            old_before = db.execute(
                "SELECT expires_at,grace_until FROM http_playback_sessions WHERE lease_id=?",
                (old_lease,),
            ).fetchone()
        other_worker.start()
        self.assertTrue(other_started.wait(timeout=5))
        time.sleep(0.1)
        with sqlite3.connect(self.root / "archive.sqlite3") as db:
            old_during = db.execute(
                "SELECT expires_at,grace_until FROM http_playback_sessions WHERE lease_id=?",
                (old_lease,),
            ).fetchone()
        self.assertEqual(old_during, old_before)
        self.assertIsNone(old_during[1])
        read_started = time.monotonic()
        self.assertEqual(service.segment(self.token, "news", "seg-A", old_lease).status, 200)
        self.assertLess(time.monotonic() - read_started, 1)
        release.set()
        worker.join(timeout=5)
        other_worker.join(timeout=5)
        self.assertFalse(worker.is_alive() or other_worker.is_alive())
        self.assertEqual(results, [503])
        self.assertEqual(other_results, [200])
        self.assertEqual(service.segment(self.token, "news", "seg-A", old_lease).status, 200)

    def test_post_commit_predecessor_failure_keeps_new_session_usable(self):
        first = self._playlist()
        old_lease = first.body.decode().split("&lease=")[1].splitlines()[0]
        original_renew = self.archive.renew_playback

        def fail_old_grace(lease_id, *, ttl_seconds):
            if lease_id == old_lease and ttl_seconds == self.service.replacement_grace_seconds:
                raise RuntimeError("predecessor renewal failed")
            return original_renew(lease_id, ttl_seconds=ttl_seconds)

        self.archive.renew_playback = fail_old_grace
        switched = self.service.playlist(
            self.token, "news", self.start + timedelta(seconds=1),
            self.start + timedelta(seconds=10),
        )
        self.assertEqual(switched.status, 200)
        new_lease = switched.body.decode().split("&lease=")[1].splitlines()[0]
        self.assertNotEqual(new_lease, old_lease)
        self.assertEqual(self.service.segment(self.token, "news", "seg-A", new_lease).status, 200)
        self.assertEqual(
            self.service.playlist(
                self.token, "news", self.start + timedelta(seconds=1),
                self.start + timedelta(seconds=10),
            ).status,
            200,
        )

    def test_missing_store_lease_is_recreated_on_reload(self):
        old_lease = self._playlist().body.decode().split("&lease=")[1].splitlines()[0]
        self.archive.end_playback(old_lease)
        reloaded = self._playlist()
        self.assertEqual(reloaded.status, 200)
        new_lease = reloaded.body.decode().split("&lease=")[1].splitlines()[0]
        self.assertNotEqual(new_lease, old_lease)
        self.assertEqual(self.service.segment(self.token, "news", "seg-A", new_lease).status, 200)

    def test_nested_switch_serializes_and_finalizes_the_original_lease(self):
        first = self._playlist()
        old_lease = first.body.decode().split("&lease=")[1].splitlines()[0]
        entered = threading.Event()
        release = threading.Event()
        later_started = threading.Event()
        first_results = []
        later_results = []

        def render(segments, *, live, uri_for, start_offset=None):
            if not entered.is_set():
                entered.set()
                if not release.wait(timeout=5):
                    raise RuntimeError("test did not release the first switch")
                raise RuntimeError("first replacement render failed")
            return _builder(
                segments, live=live, uri_for=uri_for, start_offset=start_offset
            )

        self.service.playlist_builder = render
        end = self.start + timedelta(seconds=10)
        first_switch = threading.Thread(
            target=lambda: first_results.append(
                self.service.playlist(
                    self.token, "news", self.start + timedelta(seconds=1), end
                ).status
            )
        )
        first_switch.start()
        self.assertTrue(entered.wait(timeout=5))
        later_switch = threading.Thread(
            target=lambda: (
                later_started.set(),
                later_results.append(
                    self.service.playlist(
                        self.token, "news", self.start + timedelta(seconds=2), end
                    )
                ),
            )
        )
        later_switch.start()
        self.assertTrue(later_started.wait(timeout=5))
        release.set()
        first_switch.join(timeout=5)
        later_switch.join(timeout=5)
        self.assertFalse(first_switch.is_alive() or later_switch.is_alive())
        self.assertEqual(first_results, [503])
        self.assertEqual(len(later_results), 1)
        self.assertEqual(later_results[0].status, 200)

        with sqlite3.connect(self.root / "archive.sqlite3") as db:
            old_row = db.execute(
                "SELECT expires_at,grace_until FROM http_playback_sessions WHERE lease_id=?",
                (old_lease,),
            ).fetchone()
        self.assertIsNotNone(old_row[1])
        self.assertLessEqual(old_row[0], time.time() + self.service.replacement_grace_seconds)
        self.assertLessEqual(
            self.archive.leases[old_lease]["expires_at"],
            time.time() + self.service.replacement_grace_seconds,
        )
        self.assertEqual(self.service.segment(self.token, "news", "seg-A", old_lease).status, 200)

    def test_failed_initial_render_cannot_delete_a_concurrent_successful_reload(self):
        entered = threading.Event()
        release = threading.Event()
        second_started = threading.Event()
        results = []
        second_results = []

        def render(segments, *, live, uri_for, start_offset=None):
            if not entered.is_set():
                entered.set()
                if not release.wait(timeout=5):
                    raise RuntimeError("test did not release the first render")
                raise RuntimeError("initial render failed")
            return _builder(
                segments, live=live, uri_for=uri_for, start_offset=start_offset
            )

        self.service.playlist_builder = render
        worker = threading.Thread(target=lambda: results.append(self._playlist().status))
        worker.start()
        self.assertTrue(entered.wait(timeout=5))
        second_worker = threading.Thread(
            target=lambda: (
                second_started.set(),
                second_results.append(self._playlist()),
            )
        )
        second_worker.start()
        self.assertTrue(second_started.wait(timeout=5))
        release.set()
        worker.join(timeout=5)
        second_worker.join(timeout=5)
        self.assertFalse(worker.is_alive() or second_worker.is_alive())
        self.assertEqual(results, [503])
        self.assertEqual(len(second_results), 1)
        second = second_results[0]
        self.assertEqual(second.status, 200)
        second_lease = second.body.decode().split("&lease=")[1].splitlines()[0]
        self.assertEqual(self.service.segment(self.token, "news", "seg-A", second_lease).status, 200)

    def test_existing_user_scoped_sessions_move_to_short_grace_on_upgrade(self):
        legacy_lease = "legacy-session"
        expires_at = time.time() + 4 * 60 * 60
        with sqlite3.connect(self.root / "archive.sqlite3") as db:
            db.execute("DROP TABLE http_playback_sessions")
            db.execute(
                "CREATE TABLE http_playback_sessions ("
                "lease_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,channel_id TEXT NOT NULL,"
                "request_key TEXT,start_utc REAL NOT NULL,end_utc REAL NOT NULL,expires_at REAL NOT NULL)"
            )
            db.execute(
                "INSERT INTO http_playback_sessions VALUES(?,?,?,?,?,?,?)",
                (
                    legacy_lease, "user-a", "news", "old-request", self.start.timestamp(),
                    (self.start + timedelta(seconds=10)).timestamp(), expires_at,
                ),
            )
        self.archive.leases[legacy_lease] = {
            "channel": "news", "start": self.start.timestamp(),
            "end": (self.start + timedelta(seconds=10)).timestamp(),
            "expires_at": expires_at,
        }
        service = ArchiveHTTPService(
            self.archive,
            self.tokens,
            authorize_user_channel=lambda user, channel: user == "user-a" and channel == "news",
            catchup_enabled=lambda *_: True,
            replacement_grace_seconds=3,
            playlist_builder=_builder,
        )
        with sqlite3.connect(self.root / "archive.sqlite3") as db:
            grace_until = db.execute(
                "SELECT grace_until FROM http_playback_sessions WHERE lease_id=?", (legacy_lease,)
            ).fetchone()[0]
        self.assertGreater(grace_until, time.time())
        self.assertLess(grace_until, expires_at)
        self.assertEqual(
            service.segment(self.token, "news", "seg-A", legacy_lease).status, 200
        )
        self.assertEqual(self._playlist().status, 200)


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

    def test_concurrent_processes_cannot_both_admit_different_devices_over_limit(self):
        context = multiprocessing.get_context("fork")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.ts"
            source.write_bytes(b"0123456789")
            store = ArchiveStore(root)
            start = datetime(2026, 1, 1, tzinfo=timezone.utc)
            store.add_segment("news", source, start, start + timedelta(seconds=6))
            tokens = TokenStore(root)
            device_tokens = [tokens.create("user-a"), tokens.create("user-a")]
            ArchiveHTTPService(
                store, tokens,
                authorize_user_channel=lambda *_: True,
                catchup_enabled=lambda *_: True,
                allow_new_session=lambda _user, _channel, count: count < 1,
            )
            barrier = context.Barrier(2)
            results = context.Queue()
            processes = [
                context.Process(
                    target=_concurrent_playlist_request,
                    args=(
                        str(root), token, barrier, results,
                        start.timestamp(), (start + timedelta(seconds=10)).timestamp(),
                    ),
                )
                for token in device_tokens
            ]
            for process in processes:
                process.start()
            statuses = [results.get(timeout=15) for _ in processes]
            for process in processes:
                process.join(timeout=15)
                self.assertEqual(process.exitcode, 0)
            self.assertCountEqual(statuses, [200, 403])
            with sqlite3.connect(root / "archive.sqlite3") as db:
                self.assertEqual(
                    db.execute(
                        "SELECT COUNT(*) FROM http_playback_sessions "
                        "WHERE grace_until IS NULL AND expires_at>?", (time.time(),)
                    ).fetchone()[0],
                    1,
                )


if __name__ == "__main__":
    unittest.main()
