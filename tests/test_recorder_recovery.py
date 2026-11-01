import os
import signal
import subprocess
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr.engine.recorder import STALE_WORK_DIRECTORY_SECONDS, reap_orphaned_inputs

CHANNEL = "00000000-0000-0000-0000-000000000021"
OTHER = "00000000-0000-0000-0000-000000000022"


class _FakeProc:
    """A /proc tree with killable synthetic processes."""

    def __init__(self, root: Path):
        self.root = root
        self.ignore_term = set()
        self.signals = []

    def add(self, pid, ppid, argv):
        entry = self.root / str(pid)
        entry.mkdir()
        (entry / "cmdline").write_bytes(b"\0".join(arg.encode() for arg in argv) + b"\0")
        (entry / "stat").write_text(f"{pid} (synthetic proc) S {ppid} 1 1")

    def kill(self, pid, sig):
        self.signals.append((pid, sig))
        entry = self.root / str(pid)
        if not entry.exists():
            raise ProcessLookupError
        if sig == signal.SIGTERM and pid in self.ignore_term:
            return
        for child in entry.iterdir():
            child.unlink()
        entry.rmdir()


class ReapOrphanedInputsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.work = root / "work"
        self.work.mkdir()
        (root / "proc").mkdir()
        self.proc = _FakeProc(root / "proc")

    def tearDown(self):
        self.temp.cleanup()

    def staging(self, name, *, age=0.0):
        path = self.work / name
        path.mkdir()
        (path / "segments.csv").write_text("")
        if age:
            past = time.time() - age
            os.utime(path, (past, past))
        return path

    def ffmpeg(self, pid, ppid, directory, *, name="ffmpeg"):
        self.proc.add(pid, ppid, [name, "-i", "pipe:0", "-segment_list", str(directory / "segments.csv"),
                                  str(directory / "segment-%06d.ts")])

    def reap(self):
        clock = iter(range(0, 1000))
        return reap_orphaned_inputs(
            self.work, CHANNEL, proc_root=self.proc.root, own_pid=500, kill=self.proc.kill,
            sleep=lambda _seconds: None, clock=lambda: next(clock), grace_seconds=3,
        )

    def test_only_orphans_of_this_channel_are_stopped_and_cleaned(self):
        orphan = self.staging(f"{CHANNEL}-orphan")
        stubborn = self.staging(f"{CHANNEL}-stubborn")
        own = self.staging(f"{CHANNEL}-own")
        other = self.staging(f"{OTHER}-other")
        self.ffmpeg(101, 1, orphan)
        self.ffmpeg(102, 1, stubborn)
        self.proc.ignore_term.add(102)
        self.ffmpeg(103, 500, own)
        self.ffmpeg(104, 1, other)
        self.proc.add(105, 1, ["python3", str(orphan / "segments.csv")])

        self.assertEqual(2, self.reap())

        self.assertIn((101, signal.SIGTERM), self.proc.signals)
        self.assertIn((102, signal.SIGKILL), self.proc.signals)
        self.assertNotIn(101, [pid for pid, sig in self.proc.signals if sig == signal.SIGKILL])
        self.assertFalse({103, 104, 105} & {pid for pid, _sig in self.proc.signals})
        self.assertFalse(orphan.exists())
        self.assertFalse(stubborn.exists())
        self.assertTrue(own.exists(), "a staging directory of a live child stays")
        self.assertTrue(other.exists(), "another channel is untouched")

    def test_unused_directories_are_removed_only_once_stale(self):
        stale = self.staging(f"{CHANNEL}-stale", age=STALE_WORK_DIRECTORY_SECONDS + 5)
        fresh = self.staging(f"{CHANNEL}-fresh")
        other_stale = self.staging(f"{OTHER}-stale", age=STALE_WORK_DIRECTORY_SECONDS + 5)
        self.assertEqual(0, self.reap())
        self.assertFalse(stale.exists())
        self.assertTrue(fresh.exists())
        self.assertTrue(other_stale.exists())

    def test_missing_work_root_and_proc_are_harmless(self):
        clock = iter(range(0, 10))
        self.assertEqual(0, reap_orphaned_inputs(
            Path(self.temp.name) / "absent", CHANNEL, proc_root=Path(self.temp.name) / "no-proc",
            own_pid=500, kill=self.proc.kill, sleep=lambda _s: None, clock=lambda: next(clock),
        ))

    def test_a_real_orphaned_process_is_stopped(self):
        directory = self.staging(f"{CHANNEL}-real")
        process = subprocess.Popen(
            ["ffmpeg", "-c", "import time; time.sleep(60)", str(directory / "segments.csv")],
            executable=sys.executable,
        )
        try:
            deadline = time.monotonic() + 10
            while b"segments.csv" not in Path(f"/proc/{process.pid}/cmdline").read_bytes():
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.05)
            # Pretend the caller is another process, as after a worker crash.
            reaped = reap_orphaned_inputs(self.work, CHANNEL, own_pid=-1, grace_seconds=5)
            self.assertEqual(1, reaped)
            self.assertIsNotNone(process.wait(timeout=10))
            self.assertFalse(directory.exists())
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


class _OperationRedis:
    """Records Lua calls for one prior sharing operation of a crashed recorder."""

    def __init__(self, *, status="active", worker_state="released", fence="5", begin=1, close=1):
        self.channel = CHANNEL
        self.operation_id = "op-synthetic"
        self.worker_id = "catchuparr-r" + "0" * 40
        self.digest = "a" * 64
        self.values = {f"catchuparr:sharing:attempt:{CHANNEL}:current": f"{self.operation_id}:1"}
        self.hashes = {
            f"catchuparr:sharing:attempt:{CHANNEL}:operation:{self.operation_id}": {
                "operation_id": self.operation_id, "operation_revision": "1", "recorder_fence": fence,
                "worker_id": self.worker_id, "capability_digest": self.digest,
                "status": status, "open_count": "0",
            },
            f"catchuparr:recorder-proxy:worker:{self.worker_id}": {
                "sharing_operation_id": self.operation_id, "sharing_operation_revision": "1",
                "worker_id": self.worker_id, "capability_digest": self.digest, "state": worker_state,
            },
        }
        self.results = {"begin": begin, "close": close}
        self.calls = []

    def get(self, key):
        return self.values.get(key)

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def eval(self, script, numkeys, *args):
        from catchuparr import recorder_proxy

        name = ("begin" if script == recorder_proxy._BEGIN_SHARING_OPERATION_CLOSE_LUA
                else "close" if script == recorder_proxy._CLOSE_SHARING_OPERATION_LUA else "other")
        self.calls.append((name, args[:numkeys]))
        return self.results.get(name, 0)


class PriorSharingOperationRecoveryTests(unittest.TestCase):
    def reconcile(self, redis, fence=9):
        from catchuparr.recorder_proxy import reconcile_prior_sharing_operation

        return reconcile_prior_sharing_operation(
            redis, channel_uuid=CHANNEL, current_lease_value=f"{fence}:owner", current_lease_fence=fence,
        )

    def test_unrevoked_attempt_of_a_crashed_recorder_is_revoked_then_closed(self):
        redis = _OperationRedis()
        self.assertTrue(self.reconcile(redis))
        self.assertEqual(["begin", "close"], [name for name, _keys in redis.calls])
        begin_keys = redis.calls[0][1]
        self.assertEqual(f"catchuparr:recorder-proxy:capability:{redis.digest}", begin_keys[2])

    def test_attempt_already_closing_is_only_closed(self):
        redis = _OperationRedis(status="closing")
        self.assertTrue(self.reconcile(redis))
        self.assertEqual(["close"], [name for name, _keys in redis.calls])

    def test_live_source_or_newer_attempt_is_never_revoked(self):
        cases = {
            "source still open": _OperationRedis(worker_state="active"),
            "same fence": _OperationRedis(fence="9"),
        }
        for name, redis in cases.items():
            with self.subTest(name):
                self.assertFalse(self.reconcile(redis))
                self.assertEqual([], redis.calls)

    def test_failed_revocation_keeps_the_attempt_blocked(self):
        redis = _OperationRedis(begin=0)
        self.assertFalse(self.reconcile(redis))
        self.assertEqual(["begin"], [name for name, _keys in redis.calls])


class _Query(list):
    def select_related(self, *_args):
        return self

    def order_by(self, *_args):
        return self

    def all(self):
        return self

    def first(self):
        return self[0] if self else None


class CatalogGenerationTests(unittest.TestCase):
    def catalog(self, order):
        profile = SimpleNamespace(id=7, is_active=True)
        account = SimpleNamespace(is_active=True, profiles=SimpleNamespace(all=lambda: _Query([profile])))
        streams = {
            "a": SimpleNamespace(id=11, m3u_account=account, m3u_account_id=4),
            "b": SimpleNamespace(id=12, m3u_account=account, m3u_account_id=5),
        }
        input_profile = SimpleNamespace(id=3, is_redirect=lambda: False)
        channel = SimpleNamespace(get_stream_profile=lambda: input_profile)
        models = types.ModuleType("apps.channels.models")
        models.Channel = SimpleNamespace(objects=SimpleNamespace(filter=lambda **_kw: _Query([channel])))
        models.ChannelStream = SimpleNamespace(objects=SimpleNamespace(
            filter=lambda **_kw: _Query([SimpleNamespace(stream=streams[key]) for key in order]),
        ))
        fake = {"apps": types.ModuleType("apps"), "apps.channels": types.ModuleType("apps.channels"),
                "apps.channels.models": models}
        for module in fake.values():
            module.__path__ = []
        from catchuparr.sharing_runtime import _native_source_catalog

        with patch.dict(sys.modules, fake):
            return _native_source_catalog(CHANNEL)

    def test_reordering_streams_keeps_the_catalog_generation(self):
        rows_ab, digest_ab = self.catalog("ab")
        rows_ba, digest_ba = self.catalog("ba")
        self.assertEqual(digest_ab, digest_ba)
        # The rows themselves keep the viewer order for observation.
        self.assertEqual(["11", "12"], [row.stream_id for row in rows_ab])
        self.assertEqual(["12", "11"], [row.stream_id for row in rows_ba])

    def test_a_changed_assignment_changes_the_catalog_generation(self):
        _rows, digest_ab = self.catalog("ab")
        _rows, digest_a = self.catalog("a")
        self.assertNotEqual(digest_ab, digest_a)


if __name__ == "__main__":
    unittest.main()
