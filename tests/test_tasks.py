import importlib
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr.engine import ArchiveStore


class FakeRedis:
    def __init__(self):
        self.values = {}

    def delete(self, key):
        return int(self.values.pop(key, None) is not None)

    def eval(self, script, numkeys, *args):
        keys = args[:numkeys]
        argv = args[numkeys:]
        if "local floor" in script:
            lock_key, counter_key = keys
            owner, floor, _ttl = argv
            current = int(self.values.get(counter_key, 0))
            if current < int(floor):
                current = int(floor)
                self.values[counter_key] = current
            current += 1
            self.values[counter_key] = current
            if lock_key in self.values:
                return 0
            self.values[lock_key] = f"{current}:{owner}"
            return current
        if "DEL" in script:
            key = keys[0]
            expected = argv[0]
            if self.values.get(key) == expected:
                del self.values[key]
                return 1
            return 0
        raise AssertionError("unexpected Redis Lua script")


def _fake_module(name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    module.__path__ = []
    return module


class RecorderTaskTests(unittest.TestCase):
    def test_setup_failure_releases_lease_after_durable_fence_acquisition(self):
        with tempfile.TemporaryDirectory() as temp:
            archive_root = Path(temp) / "archive"
            ArchiveStore(archive_root).register_recorder_fence("channel-1", 25)
            redis = FakeRedis()
            config = SimpleNamespace(
                channel_uuids=("channel-1",), archive_root=archive_root
            )

            celery = _fake_module(
                "celery",
                shared_task=lambda *, name: lambda function: function,
            )
            apps = _fake_module("apps")
            apps_channels = _fake_module("apps.channels")
            apps_channels_tasks = _fake_module(
                "apps.channels.tasks",
                get_dvr_stream_base_url=lambda: (_ for _ in ()).throw(
                    RuntimeError("DVR URL setup failed")
                ),
            )
            core = _fake_module("core")
            core_utils = _fake_module(
                "core.utils",
                RedisClient=SimpleNamespace(get_client=lambda: redis),
            )
            fake_modules = {
                "celery": celery,
                "apps": apps,
                "apps.channels": apps_channels,
                "apps.channels.tasks": apps_channels_tasks,
                "core": core,
                "core.utils": core_utils,
            }

            with patch.dict(sys.modules, fake_modules):
                sys.modules.pop("catchuparr.tasks", None)
                tasks = importlib.import_module("catchuparr.tasks")
                with patch("catchuparr.runtime.require_supported_version"), patch(
                    "catchuparr.runtime.load_config", return_value=config
                ):
                    with self.assertRaisesRegex(RuntimeError, "DVR URL setup failed"):
                        tasks.record_channel("channel-1")

            self.assertNotIn("catchuparr:recorder:channel-1", redis.values)
            self.assertEqual(ArchiveStore(archive_root).recorder_fence("channel-1"), 26)


if __name__ == "__main__":
    unittest.main()
