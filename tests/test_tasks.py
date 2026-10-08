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

    def exists(self, key):
        return int(key in self.values)

    def set(self, key, value, **kwargs):
        if kwargs.get("nx") and key in self.values:
            return False
        self.values[key] = value
        return True

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
    def test_reconcile_queues_the_applied_generation_with_each_channel(self):
        redis = FakeRedis()
        queued = []

        class ChannelQuery:
            def filter(self, **kwargs):
                self.kwargs = kwargs
                return self

            def values_list(self, *args, **kwargs):
                return ["channel-1"]

        class FakeStore:
            def __init__(self, _root):
                pass

            def cleanup(self, **_kwargs):
                return None

            def reconcile_orphans(self, **_kwargs):
                return None

        def task_decorator(*, name):
            def decorate(function):
                if name == "catchuparr.record_channel":
                    function.apply_async = lambda **kwargs: queued.append(kwargs)
                return function

            return decorate

        celery = _fake_module("celery", shared_task=task_decorator)
        apps = _fake_module("apps")
        apps_channels = _fake_module("apps.channels")
        apps_channels_models = _fake_module(
            "apps.channels.models",
            Channel=SimpleNamespace(objects=ChannelQuery()),
        )
        apps_channels.models = apps_channels_models
        core = _fake_module("core")
        core_utils = _fake_module(
            "core.utils", RedisClient=SimpleNamespace(get_client=lambda: redis)
        )
        fake_modules = {
            "celery": celery,
            "apps": apps,
            "apps.channels": apps_channels,
            "apps.channels.models": apps_channels_models,
            "core": core,
            "core.utils": core_utils,
        }
        config = SimpleNamespace(
            channel_uuids=("channel-1",),
            archive_root=Path("/tmp/synthetic-archive"),
            retention_hours=24,
            max_storage_bytes=1024,
        )
        with patch.dict(sys.modules, fake_modules):
            sys.modules.pop("catchuparr.tasks", None)
            tasks = importlib.import_module("catchuparr.tasks")
            with (
                patch("catchuparr.runtime.require_supported_version"),
                patch("catchuparr.configuration.reset_legacy_configuration", return_value=False),
                patch("catchuparr.runtime.load_config", return_value=config),
                patch("catchuparr.configuration.load_active_configuration", return_value={}),
                patch("catchuparr.recorder_proxy.configuration_generation", return_value="generation-7"),
                patch("catchuparr.engine.store.ArchiveStore", FakeStore),
            ):
                result = tasks.reconcile_recorders()
        self.assertEqual({"queued": 1}, result)
        self.assertEqual([{"args": ["channel-1", "generation-7"], "queue": "dvr"}], queued)

    def test_queued_recorder_does_not_start_for_a_stale_generation(self):
        redis = FakeRedis()
        config = SimpleNamespace(
            channel_uuids=("channel-1",), archive_root=Path("/tmp/synthetic-archive")
        )
        active = {"channel_uuids": "channel-1", "source_policies": {}}
        celery = _fake_module(
            "celery",
            shared_task=lambda *, name: lambda function: function,
        )
        apps = _fake_module("apps")
        apps_channels = _fake_module("apps.channels")
        apps_channels_tasks = _fake_module(
            "apps.channels.tasks",
            get_dvr_stream_base_url=lambda: "http://dispatcharr",
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
            with (
                patch("catchuparr.runtime.require_supported_version"),
                patch("catchuparr.runtime.load_config", return_value=config),
                patch("catchuparr.configuration.reset_legacy_configuration", return_value=False),
                patch("catchuparr.configuration.load_active_configuration", return_value=active),
            ):
                result = tasks.record_channel("channel-1", "older-generation")
        self.assertEqual({"status": "stale_configuration"}, result)
        self.assertEqual({}, redis.values)

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
                ), patch("catchuparr.configuration.reset_legacy_configuration", return_value=False):
                    with self.assertRaisesRegex(RuntimeError, "DVR URL setup failed"):
                        tasks.record_channel("channel-1")

            self.assertNotIn("catchuparr:recorder:channel-1", redis.values)
            self.assertEqual(ArchiveStore(archive_root).recorder_fence("channel-1"), 26)

    def test_override_capacity_or_start_failure_falls_through_ranked_sources(self):
        with tempfile.TemporaryDirectory() as temp:
            archive_root = Path(temp) / "archive"
            redis = FakeRedis()
            config = SimpleNamespace(channel_uuids=("channel-1",), archive_root=archive_root)
            active = {"channel_uuids": "channel-1", "source_policies": {}}
            attempts = []
            stopped = []
            source_candidates = [
                {"id": "44", "account_id": "12"},
                {"id": "45", "account_id": "13"},
                {"id": "46", "account_id": "14"},
            ]

            class Attempt:
                def __init__(self, candidate):
                    self.worker_id = "catchuparr-r" + candidate["id"].zfill(40)
                    self.stream_id = candidate["id"]
                    self.input_url = "http://dispatcharr/catchuparr/recorder/channel-1"
                    self.input_headers = {"X-Catchuparr-Recorder": "signed-capability"}
                    self.revoked = False

                def renew(self, redis_client):
                    return True

                def revoke(self, redis_client):
                    self.revoked = True

            class FakeRecorder:
                outcomes = [
                    RuntimeError("input startup failed"),
                    SimpleNamespace(status="no_media", useful_segments=0, return_code=1),
                    SimpleNamespace(status="stopped", useful_segments=0, return_code=-15),
                ]

                def __init__(self, _store, _channel, input_url, _work, **kwargs):
                    self.input_url = input_url
                    self.headers = kwargs["input_headers"]

                def run_candidate(self, _stop_event, **_kwargs):
                    outcome = self.outcomes.pop(0)
                    if isinstance(outcome, Exception):
                        raise outcome
                    return outcome

            celery = _fake_module(
                "celery",
                shared_task=lambda *, name: lambda function: function,
            )
            apps = _fake_module("apps")
            apps_channels = _fake_module("apps.channels")
            apps_channels_tasks = _fake_module(
                "apps.channels.tasks",
                get_dvr_stream_base_url=lambda: "http://dispatcharr",
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
                with (
                    patch("catchuparr.runtime.require_supported_version"),
                    patch("catchuparr.runtime.load_config", return_value=config),
                    patch("catchuparr.configuration.reset_legacy_configuration", return_value=False),
                    patch("catchuparr.configuration.load_active_configuration", return_value=active),
                    patch("catchuparr.recorder_proxy.ranked_source_candidates", return_value=source_candidates),
                    patch(
                        "catchuparr.recorder_proxy.issue_recorder_attempt",
                        side_effect=lambda _redis, _lease, **kwargs: (
                            attempts.append(Attempt(kwargs["candidate"])) or attempts[-1]
                        ),
                    ),
                    patch(
                        "catchuparr.recorder_proxy.stop_recorder_attempt",
                        side_effect=lambda *_args: (stopped.append(True) or True),
                    ),
                    patch("catchuparr.adapters.recorder_proxy.core_api_supported", return_value=True),
                    patch("catchuparr.adapters.recorder_proxy.install_proxyserver_cleanup_hook", return_value=True),
                    patch("catchuparr.engine.recorder.FFmpegCopyRecorder", FakeRecorder),
                ):
                    result = tasks.record_channel("channel-1")

            self.assertEqual("stopped", result["status"])
            self.assertEqual(["44", "45", "46"], [attempt.stream_id for attempt in attempts])
            self.assertTrue(all(attempt.revoked for attempt in attempts))
            self.assertEqual(3, len(stopped))
            self.assertTrue(all(attempt.input_url.endswith("/catchuparr/recorder/channel-1") for attempt in attempts))


if __name__ == "__main__":
    unittest.main()
