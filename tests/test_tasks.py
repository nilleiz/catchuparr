import importlib
import sys
import tempfile
import threading
import types
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr.engine import ArchiveStore
from catchuparr.recorder_proxy import configuration_generation


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


def _active_configuration(channel="channel-1"):
    return {
        "channel_uuids": channel,
        "source_policies": {},
        "recording_schedule": {
            "timezone": "UTC",
            "channels": {channel: {"mode": "continuous"}},
        },
    }


def _recording_task_fake_modules(redis):
    return {
        "celery": _fake_module("celery", shared_task=lambda *, name: lambda fn: fn),
        "apps": _fake_module("apps"),
        "apps.channels": _fake_module("apps.channels"),
        "apps.channels.tasks": _fake_module(
            "apps.channels.tasks", get_dvr_stream_base_url=lambda: "http://dispatcharr"
        ),
        "core": _fake_module("core"),
        "core.utils": _fake_module(
            "core.utils", RedisClient=SimpleNamespace(get_client=lambda: redis)
        ),
        "version": _fake_module("version", __version__="0.32.0"),
    }


@contextmanager
def _isolated_plugin_modules(fake_modules):
    """Keep package children aligned with sys.modules while stubbing Dispatcharr."""
    package = importlib.import_module("catchuparr")
    child_names = (
        "tasks", "runtime", "configuration", "recorder_proxy", "recorder_control",
        "schedule", "logging_utils", "adapters", "engine",
    )
    missing = object()
    previous = {name: package.__dict__.get(name, missing) for name in child_names}
    try:
        with patch.dict(sys.modules, fake_modules):
            for name in child_names:
                module_name = f"catchuparr.{name}"
                module = sys.modules.get(module_name)
                if module is None:
                    package.__dict__.pop(name, None)
                else:
                    package.__dict__[name] = module
            sys.modules.pop("catchuparr.tasks", None)
            package.__dict__.pop("tasks", None)
            yield
    finally:
        for name, value in previous.items():
            if value is missing:
                package.__dict__.pop(name, None)
            else:
                package.__dict__[name] = value


class RecorderTaskTests(unittest.TestCase):
    def test_admission_uses_one_snapshot_for_schedule_and_archive_settings(self):
        import uuid

        from catchuparr.runtime import parse_settings

        channel = str(uuid.uuid4())
        active_a = {
            **_active_configuration(channel),
            "archive_root": "/tmp/synthetic-snapshot-a",
            "retention_hours": 24,
            "max_storage_gib": 1,
        }
        active_b = {
            **active_a,
            "archive_root": "/tmp/synthetic-snapshot-b",
            "recording_schedule": {
                "timezone": "UTC",
                "channels": {channel: {"mode": "weekly", "intervals": []}},
            },
        }
        read_snapshots = []

        def config_from_snapshot(active_snapshot=None):
            read_snapshots.append(active_snapshot)
            # A legacy second read would observe an Apply that retained the same
            # selected UUID set but changed storage and schedule fields.
            return parse_settings(active_snapshot if active_snapshot is not None else active_b)

        fake_modules = {
            "celery": _fake_module("celery", shared_task=lambda *, name: lambda fn: fn),
        }
        with _isolated_plugin_modules(fake_modules):
            tasks = importlib.import_module("catchuparr.tasks")
            with (
                patch("catchuparr.configuration.load_active_configuration", return_value=active_a),
                patch("catchuparr.runtime.load_config", side_effect=config_from_snapshot),
                patch(
                    "catchuparr.recorder_control.load_recorder_control",
                    return_value=SimpleNamespace(paused=False, generation=9),
                ),
            ):
                state = tasks._recorder_admission_state(
                    channel, configuration_generation(active_a), 9
                )

        self.assertEqual("ready", state["status"])
        self.assertEqual(Path("/tmp/synthetic-snapshot-a"), state["config"].archive_root)
        self.assertEqual([active_a], read_snapshots)

    def test_running_supervisor_stops_on_pause_fence_and_schedule_close(self):
        import uuid

        from catchuparr.runtime import parse_settings

        real_event = threading.Event
        for reason in ("paused", "stale_control", "schedule_closed"):
            with self.subTest(reason=reason):
                redis = FakeRedis()
                channel = str(uuid.uuid4())
                active = {
                    **_active_configuration(channel),
                    "archive_root": "/tmp/synthetic-supervisor-archive",
                    "retention_hours": 24,
                    "max_storage_gib": 1,
                }
                state = {"deny": False}
                recorder_started = real_event()
                lease_instances = []
                stop_events = []

                class FastEvent:
                    def __init__(self):
                        self.event = real_event()

                    def set(self):
                        self.event.set()

                    def is_set(self):
                        return self.event.is_set()

                    def wait(self, timeout=None):
                        return self.event.wait(min(timeout or 0.01, 0.01))

                class FakeLease:
                    def __init__(self, *_args, **_kwargs):
                        self.released = False
                        lease_instances.append(self)

                    def acquire(self):
                        return 1

                    def renew(self):
                        return True

                    def release(self):
                        self.released = True

                class FakeRecorder:
                    def __init__(self, *_args, **_kwargs):
                        pass

                    def run_forever(self, stop_event):
                        stop_events.append(stop_event)
                        recorder_started.set()
                        while not stop_event.wait(0.1):
                            pass

                def current_control():
                    if reason == "paused":
                        return SimpleNamespace(paused=state["deny"], generation=4)
                    if reason == "stale_control":
                        return SimpleNamespace(paused=False, generation=5 if state["deny"] else 4)
                    return SimpleNamespace(paused=False, generation=4)

                def schedule_active(*_args):
                    return not (reason == "schedule_closed" and state["deny"])

                modules = _recording_task_fake_modules(redis)
                result_holder = []
                with _isolated_plugin_modules(modules):
                    tasks = importlib.import_module("catchuparr.tasks")
                    lease_module = importlib.import_module("catchuparr.engine.leases")
                    recorder_module = importlib.import_module("catchuparr.engine.recorder")
                    with (
                        patch("catchuparr.runtime.require_supported_version"),
                        patch("catchuparr.runtime.load_config", side_effect=lambda active_snapshot=None: parse_settings(active_snapshot)),
                        patch("catchuparr.configuration.reset_legacy_configuration", return_value=False),
                        patch("catchuparr.configuration.load_active_configuration", return_value=active),
                        patch("catchuparr.recorder_control.load_recorder_control", side_effect=current_control),
                        patch("catchuparr.schedule.schedule_is_active", side_effect=schedule_active),
                        patch.object(lease_module, "RedisRecorderLease", FakeLease),
                        patch("catchuparr.engine.store.ArchiveStore", return_value=object()),
                        patch.object(recorder_module, "FFmpegCopyRecorder", FakeRecorder),
                        patch("catchuparr.tasks.threading.Event", FastEvent),
                    ):
                        task_thread = threading.Thread(
                            target=lambda: result_holder.append(
                                tasks.record_channel(channel, configuration_generation(active), 4)
                            )
                        )
                        task_thread.start()
                        self.assertTrue(recorder_started.wait(2), "recorder worker did not start")
                        state["deny"] = True
                        task_thread.join(3)

                self.assertFalse(task_thread.is_alive(), "recorder supervisor did not stop")
                self.assertEqual([{"status": "stopped"}], result_holder)
                self.assertEqual(1, len(lease_instances))
                self.assertTrue(lease_instances[0].released)
                self.assertEqual(1, len(stop_events))
                self.assertTrue(stop_events[0].is_set())

    def test_post_lease_gate_blocks_media_start_for_shared_and_private_paths(self):
        import uuid

        for path in ("shared", "private"):
            with self.subTest(path=path):
                redis = FakeRedis()
                channel = str(uuid.uuid4())
                lease_instances = []
                attempts = []
                stopped_attempts = []
                recorder_constructions = []
                admissions = []
                ready_state = {
                    "status": "ready",
                    "config": SimpleNamespace(archive_root=Path("/tmp/synthetic-post-lease-archive")),
                    "active": {},
                }

                class FakeLease:
                    def __init__(self, *_args, **_kwargs):
                        self.released = False
                        lease_instances.append(self)

                    def acquire(self):
                        return 1

                    def renew(self):
                        return True

                    def release(self):
                        self.released = True

                class Attempt:
                    input_url = "http://dispatcharr/catchuparr/recorder/synthetic"
                    input_headers = {"X-Catchuparr-Recorder": "synthetic-capability"}

                    def revoke(self, _redis):
                        self.revoked = True

                class FakeRecorder:
                    def __init__(self, *_args, **_kwargs):
                        recorder_constructions.append(True)
                        raise AssertionError("media must not start after the admission fence")

                modules = _recording_task_fake_modules(redis)
                with _isolated_plugin_modules(modules):
                    tasks = importlib.import_module("catchuparr.tasks")
                    lease_module = importlib.import_module("catchuparr.engine.leases")

                    def admission(*_args):
                        admissions.append(True)
                        return ready_state if len(admissions) == 1 else {"status": "recording_paused"}

                    candidates = None if path == "shared" else [{"id": "44", "account_id": "12"}]
                    with (
                        patch("catchuparr.runtime.require_supported_version"),
                        patch("catchuparr.configuration.reset_legacy_configuration", return_value=False),
                        patch("catchuparr.tasks._recorder_admission_state", side_effect=admission),
                        patch.object(lease_module, "RedisRecorderLease", FakeLease),
                        patch("catchuparr.engine.store.ArchiveStore", return_value=object()),
                        patch("catchuparr.engine.recorder.FFmpegCopyRecorder", FakeRecorder),
                        patch("catchuparr.tasks.threading.Event", threading.Event),
                        patch("catchuparr.recorder_proxy.ranked_source_candidates", return_value=candidates),
                        patch("catchuparr.adapters.recorder_proxy.core_api_supported", return_value=True),
                        patch("catchuparr.adapters.recorder_proxy.install_proxyserver_cleanup_hook", return_value=True),
                        patch(
                            "catchuparr.recorder_proxy.issue_recorder_attempt",
                            side_effect=lambda *_args, **_kwargs: (attempts.append(Attempt()) or attempts[-1]),
                        ),
                        patch(
                            "catchuparr.recorder_proxy.stop_recorder_attempt",
                            side_effect=lambda *_args: (stopped_attempts.append(True) or True),
                        ),
                    ):
                        result = tasks.record_channel(channel, "synthetic-generation", 4)

                self.assertEqual({"status": "recording_paused"}, result)
                self.assertEqual(1, len(lease_instances))
                self.assertTrue(lease_instances[0].released)
                self.assertEqual([], recorder_constructions)
                if path == "private":
                    self.assertEqual(1, len(attempts))
                    self.assertTrue(attempts[0].revoked)
                    self.assertEqual([True], stopped_attempts)
                else:
                    self.assertEqual([], attempts)
                    self.assertEqual([], stopped_attempts)

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
            "version": _fake_module("version", __version__="0.32.0"),
        }
        active = _active_configuration()
        expected_generation = configuration_generation(active)
        control = SimpleNamespace(paused=False, generation=7)
        config = SimpleNamespace(
            channel_uuids=("channel-1",),
            archive_root=Path("/tmp/synthetic-archive"),
            retention_hours=24,
            max_storage_bytes=1024,
        )
        with _isolated_plugin_modules(fake_modules):
            tasks = importlib.import_module("catchuparr.tasks")
            with (
                patch("catchuparr.configuration.reset_legacy_configuration", return_value=False),
                patch("catchuparr.runtime.load_config", return_value=config),
                patch("catchuparr.configuration.load_active_configuration", return_value=active),
                patch("catchuparr.recorder_control.load_recorder_control", return_value=control),
                patch("catchuparr.engine.store.ArchiveStore", FakeStore),
            ):
                result = tasks.reconcile_recorders()
        self.assertEqual({"queued": 1}, result)
        self.assertEqual(
            [{"args": ["channel-1", expected_generation, 7], "queue": "dvr"}], queued
        )

    def test_queued_recorder_does_not_start_for_a_stale_generation(self):
        redis = FakeRedis()
        config = SimpleNamespace(
            channel_uuids=("channel-1",), archive_root=Path("/tmp/synthetic-archive")
        )
        active = _active_configuration()
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
            "version": _fake_module("version", __version__="0.32.0"),
        }
        with _isolated_plugin_modules(fake_modules):
            tasks = importlib.import_module("catchuparr.tasks")
            with (
                patch("catchuparr.runtime.load_config", return_value=config),
                patch("catchuparr.configuration.reset_legacy_configuration", return_value=False),
                patch("catchuparr.configuration.load_active_configuration", return_value=active),
                patch(
                    "catchuparr.recorder_control.load_recorder_control",
                    return_value=SimpleNamespace(paused=False, generation=3),
                ),
                patch("catchuparr.logging_utils.event") as log_event,
            ):
                result = tasks.record_channel("channel-1", "older-generation", 3)
                current_generation = configuration_generation(active)
                stale_control = tasks.record_channel("channel-1", current_generation, 2)
                with patch(
                    "catchuparr.recorder_control.load_recorder_control",
                    return_value=SimpleNamespace(paused=True, generation=3),
                ):
                    paused = tasks.record_channel("channel-1", current_generation, 3)
                legacy = tasks.record_channel("channel-1", current_generation)
                off_schedule = _active_configuration()
                off_schedule["recording_schedule"]["channels"]["channel-1"] = {
                    "mode": "weekly", "intervals": []
                }
                with (
                    patch("catchuparr.configuration.load_active_configuration", return_value=off_schedule),
                    patch(
                        "catchuparr.recorder_control.load_recorder_control",
                        return_value=SimpleNamespace(paused=False, generation=3),
                    ),
                ):
                    outside = tasks.record_channel(
                        "channel-1", configuration_generation(off_schedule), 3
                    )
        self.assertEqual({"status": "stale_configuration"}, result)
        self.assertEqual({"status": "stale_control"}, stale_control)
        self.assertEqual({"status": "recording_paused"}, paused)
        self.assertEqual({"status": "legacy_job"}, legacy)
        self.assertEqual({"status": "outside_schedule"}, outside)
        self.assertIn(("recorder_stale_job",), [call.args for call in log_event.call_args_list])
        self.assertIn(("recorder_schedule_closed",), [call.args for call in log_event.call_args_list])
        self.assertEqual({}, redis.values)

    def test_reconcile_skips_channels_with_an_empty_applied_schedule(self):
        redis = FakeRedis()
        queued = []

        class ChannelQuery:
            def filter(self, **kwargs):
                return self

            def values_list(self, *args, **kwargs):
                return ["channel-1"]

        class FakeStore:
            def __init__(self, _root):
                self.cleaned = False

            def cleanup(self, **_kwargs):
                self.cleaned = True

            def reconcile_orphans(self, **_kwargs):
                return None

        def task_decorator(*, name):
            def decorate(function):
                if name == "catchuparr.record_channel":
                    function.apply_async = lambda **kwargs: queued.append(kwargs)
                return function

            return decorate

        active = _active_configuration()
        active["recording_schedule"]["channels"]["channel-1"] = {
            "mode": "weekly", "intervals": []
        }
        config = SimpleNamespace(
            channel_uuids=("channel-1",),
            archive_root=Path("/tmp/synthetic-archive"),
            retention_hours=24,
            max_storage_bytes=1024,
        )
        fake_modules = {
            "celery": _fake_module("celery", shared_task=task_decorator),
            "apps": _fake_module("apps"),
            "apps.channels": _fake_module("apps.channels"),
            "apps.channels.models": _fake_module(
                "apps.channels.models", Channel=SimpleNamespace(objects=ChannelQuery())
            ),
            "core": _fake_module("core"),
            "core.utils": _fake_module(
                "core.utils", RedisClient=SimpleNamespace(get_client=lambda: redis)
            ),
            "version": _fake_module("version", __version__="0.32.0"),
        }
        with _isolated_plugin_modules(fake_modules):
            tasks = importlib.import_module("catchuparr.tasks")
            with (
                patch("catchuparr.configuration.reset_legacy_configuration", return_value=False),
                patch("catchuparr.runtime.load_config", return_value=config),
                patch("catchuparr.configuration.load_active_configuration", return_value=active),
                patch(
                    "catchuparr.recorder_control.load_recorder_control",
                    return_value=SimpleNamespace(paused=False, generation=5),
                ),
                patch("catchuparr.engine.store.ArchiveStore", FakeStore),
            ):
                result = tasks.reconcile_recorders()
        self.assertEqual({"queued": 0}, result)
        self.assertEqual([], queued)

    def test_setup_failure_releases_lease_after_durable_fence_acquisition(self):
        with tempfile.TemporaryDirectory() as temp:
            archive_root = Path(temp) / "archive"
            ArchiveStore(archive_root).register_recorder_fence("channel-1", 25)
            redis = FakeRedis()
            config = SimpleNamespace(
                channel_uuids=("channel-1",), archive_root=archive_root
            )
            active = _active_configuration()
            expected_generation = configuration_generation(active)

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
                "version": _fake_module("version", __version__="0.32.0"),
            }

            with _isolated_plugin_modules(fake_modules):
                tasks = importlib.import_module("catchuparr.tasks")
                with (
                    patch("catchuparr.runtime.load_config", return_value=config),
                    patch("catchuparr.configuration.reset_legacy_configuration", return_value=False),
                    patch("catchuparr.configuration.load_active_configuration", return_value=active),
                    patch(
                        "catchuparr.recorder_control.load_recorder_control",
                        return_value=SimpleNamespace(paused=False, generation=4),
                    ),
                ):
                    with self.assertRaisesRegex(RuntimeError, "DVR URL setup failed"):
                        tasks.record_channel("channel-1", expected_generation, 4)

            self.assertNotIn("catchuparr:recorder:channel-1", redis.values)
            self.assertEqual(ArchiveStore(archive_root).recorder_fence("channel-1"), 26)

    def test_override_capacity_or_start_failure_falls_through_ranked_sources(self):
        with tempfile.TemporaryDirectory() as temp:
            archive_root = Path(temp) / "archive"
            redis = FakeRedis()
            config = SimpleNamespace(channel_uuids=("channel-1",), archive_root=archive_root)
            active = _active_configuration()
            expected_generation = configuration_generation(active)
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
                "version": _fake_module("version", __version__="0.32.0"),
            }
            with _isolated_plugin_modules(fake_modules):
                tasks = importlib.import_module("catchuparr.tasks")
                with (
                    patch("catchuparr.runtime.load_config", return_value=config),
                    patch("catchuparr.configuration.reset_legacy_configuration", return_value=False),
                    patch("catchuparr.configuration.load_active_configuration", return_value=active),
                    patch(
                        "catchuparr.recorder_control.load_recorder_control",
                        return_value=SimpleNamespace(paused=False, generation=6),
                    ),
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
                    result = tasks.record_channel("channel-1", expected_generation, 6)

            self.assertEqual("stopped", result["status"])
            self.assertEqual(["44", "45", "46"], [attempt.stream_id for attempt in attempts])
            self.assertTrue(all(attempt.revoked for attempt in attempts))
            self.assertEqual(3, len(stopped))
            self.assertTrue(all(attempt.input_url.endswith("/catchuparr/recorder/channel-1") for attempt in attempts))


if __name__ == "__main__":
    unittest.main()
