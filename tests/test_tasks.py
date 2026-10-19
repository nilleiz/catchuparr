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

    def set(self, key, value, ex=None, nx=False):
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    def expire(self, key, seconds):
        return key in self.values

    def delete(self, key):
        return int(self.values.pop(key, None) is not None)

    def exists(self, key):
        return int(key in self.values)

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
        if "EXPIRE" in script:
            key = keys[0]
            expected = argv[0]
            return int(self.values.get(key) == expected)
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
    adapter_package = importlib.import_module("catchuparr.adapters")
    fake_modules = {
        **fake_modules,
        "catchuparr.adapters.recorder_proxy": fake_modules.get(
            "catchuparr.adapters.recorder_proxy",
            _fake_module(
                "catchuparr.adapters.recorder_proxy",
                core_api_supported=lambda: True,
                install_proxyserver_cleanup_hook=lambda: True,
            ),
        ),
    }
    child_names = (
        "tasks", "runtime", "configuration", "recorder_proxy", "recorder_control",
        "schedule", "logging_utils", "adapters", "engine", "sharing_runtime",
        "sharing_episode", "sharing_persistence",
    )
    missing = object()
    previous = {name: package.__dict__.get(name, missing) for name in child_names}
    previous_adapter_proxy = missing
    try:
        with patch.dict(sys.modules, fake_modules):
            for name in child_names:
                module_name = f"catchuparr.{name}"
                module = sys.modules.get(module_name)
                if module is None:
                    package.__dict__.pop(name, None)
                else:
                    package.__dict__[name] = module
            adapter_proxy = sys.modules.get("catchuparr.adapters.recorder_proxy")
            if adapter_proxy is not None:
                previous_adapter_proxy = getattr(
                    adapter_package, "recorder_proxy", missing,
                )
                adapter_package.recorder_proxy = adapter_proxy
            sys.modules.pop("catchuparr.tasks", None)
            package.__dict__.pop("tasks", None)
            yield
    finally:
        if adapter_package is not None:
            if previous_adapter_proxy is missing:
                adapter_package.__dict__.pop("recorder_proxy", None)
            else:
                adapter_package.recorder_proxy = previous_adapter_proxy
        for name, value in previous.items():
            if value is missing:
                package.__dict__.pop(name, None)
            else:
                package.__dict__[name] = value


class RecorderTaskTests(unittest.TestCase):
    def test_failed_first_media_diagnostic_is_remembered_and_retried(self):
        with _isolated_plugin_modules({
            "celery": _fake_module(
                "celery", shared_task=lambda *, name: lambda function: function,
            ),
        }):
            tasks = importlib.import_module("catchuparr.tasks")
            lock = threading.RLock()
            state = {"diagnostic": {
                "kind": "dedicated", "attempt": "attempt-1", "started_at": 10.0,
                "phase": "starting", "last_published": 19.0,
            }}
            with lock:
                tasks._remember_recorder_diagnostic(
                    state, "dedicated", "attempt-1", "recording", 10.0,
                    published=False, now=20.0,
                )
            self.assertEqual(state["diagnostic"]["phase"], "recording")
            self.assertEqual(state["diagnostic"]["last_published"], 0.0)
            calls = []

            def retry(_attempt, phase, *, started_at):
                calls.append((phase, started_at))
                with lock:
                    tasks._remember_recorder_diagnostic(
                        state, "dedicated", "attempt-1", phase, started_at,
                        published=True, now=21.0,
                    )

            tasks._refresh_current_recorder_diagnostic(
                lock, state, retry, lambda *_args, **_kwargs: self.fail("wrong publisher"),
                now=21.0,
            )
            self.assertEqual(calls, [("recording", 10.0)])
            self.assertEqual(state["diagnostic"]["phase"], "recording")
            self.assertEqual(state["diagnostic"]["last_published"], 21.0)

    def test_diagnostic_refresh_cannot_overwrite_concurrent_first_media_phase(self):
        with _isolated_plugin_modules({
            "celery": _fake_module(
                "celery", shared_task=lambda *, name: lambda function: function,
            ),
        }):
            tasks = importlib.import_module("catchuparr.tasks")
            state_lock = threading.RLock()
            state = {"diagnostic": {
                "kind": "dedicated", "attempt": object(), "started_at": 10.0,
                "phase": "starting", "last_published": 0.0,
            }}
            refresh_entered = threading.Event()
            release_refresh = threading.Event()
            media_ready = threading.Event()
            events = []

            def publish_dedicated(_attempt, phase, *, started_at):
                events.append(("refresh", phase, started_at))
                refresh_entered.set()
                self.assertTrue(release_refresh.wait(2))
                state["diagnostic"]["phase"] = phase
                state["diagnostic"]["last_published"] = 20.0

            def publish_borrow(_prepared, _phase, *, started_at):
                self.fail("dedicated diagnostic refresh called borrowed publisher")

            refresh = threading.Thread(
                target=tasks._refresh_current_recorder_diagnostic,
                args=(state_lock, state, publish_dedicated, publish_borrow),
                kwargs={"now": 20.0},
            )
            refresh.start()
            self.assertTrue(refresh_entered.wait(2))

            def record_first_media():
                media_ready.set()
                with state_lock:
                    events.append(("media", "recording"))
                    state["diagnostic"]["phase"] = "recording"
                    state["diagnostic"]["last_published"] = 21.0

            media = threading.Thread(target=record_first_media)
            media.start()
            self.assertTrue(media_ready.wait(2))
            release_refresh.set()
            refresh.join(timeout=2)
            media.join(timeout=2)
            self.assertFalse(refresh.is_alive())
            self.assertFalse(media.is_alive())
            self.assertEqual("recording", state["diagnostic"]["phase"])
            self.assertEqual([("refresh", "starting", 10.0), ("media", "recording")], events)

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
                patch(
                    "catchuparr.configuration.load_applied_state",
                    return_value=(active_a, SimpleNamespace(paused=False, generation=9)),
                ),
                patch("catchuparr.runtime.load_config", side_effect=config_from_snapshot),
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
                        self.owner = "synthetic-owner"
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
                        patch(
                            "catchuparr.configuration.load_applied_state",
                            side_effect=lambda: (active, current_control()),
                        ),
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
                        self.owner = "synthetic-owner"
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
                adapter_package = _fake_module("catchuparr.adapters")
                recorder_proxy_adapter = _fake_module(
                    "catchuparr.adapters.recorder_proxy",
                    core_api_supported=lambda: True,
                    install_proxyserver_cleanup_hook=lambda: True,
                )
                modules["catchuparr.adapters"] = adapter_package
                modules["catchuparr.adapters.recorder_proxy"] = recorder_proxy_adapter
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
                patch(
                    "catchuparr.configuration.load_applied_state",
                    return_value=(active, control),
                ),
                patch("catchuparr.engine.store.ArchiveStore", FakeStore),
            ):
                result = tasks.reconcile_recorders()
        self.assertEqual({"queued": 1, "cleanup_pending": 0}, result)
        self.assertEqual(
            [{"args": ["channel-1", expected_generation, 7], "queue": "dvr"}], queued
        )

    def test_reconcile_retries_channel_cleanup_before_dispatch_and_respects_live_lease(self):
        redis = FakeRedis()
        queued = []
        cleanup_calls = []
        outcomes = [(0, 1), (1, 0)]

        class ChannelQuery:
            def filter(self, **_kwargs):
                return self

            def values_list(self, *_args, **_kwargs):
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

        active = _active_configuration()
        active["connection_sharing"] = {"channel-1": {"enabled": True}}
        config = SimpleNamespace(
            channel_uuids=("channel-1",), archive_root=Path("/tmp/synthetic-archive"),
            retention_hours=24, max_storage_bytes=1024,
        )
        fake_modules = {
            "celery": _fake_module("celery", shared_task=task_decorator),
            "apps": _fake_module("apps"),
            "apps.channels": _fake_module("apps.channels"),
            "apps.channels.models": _fake_module(
                "apps.channels.models", Channel=SimpleNamespace(objects=ChannelQuery()),
            ),
            "core": _fake_module("core"),
            "core.utils": _fake_module(
                "core.utils", RedisClient=SimpleNamespace(get_client=lambda: redis),
            ),
            "version": _fake_module("version", __version__="0.32.0"),
        }
        with _isolated_plugin_modules(fake_modules):
            tasks = importlib.import_module("catchuparr.tasks")

            def cleanup(_ledger, _redis, **kwargs):
                cleanup_calls.append(kwargs.get("channel_uuid"))
                return outcomes.pop(0)

            with (
                patch("catchuparr.configuration.reset_legacy_configuration", return_value=False),
                patch("catchuparr.runtime.load_config", return_value=config),
                patch(
                    "catchuparr.configuration.load_applied_state",
                    return_value=(active, SimpleNamespace(paused=False, generation=7)),
                ),
                patch("catchuparr.engine.store.ArchiveStore", FakeStore),
                patch("catchuparr.sharing_runtime.retry_durable_open_cleanups", side_effect=cleanup),
            ):
                pending_result = tasks.reconcile_recorders()
                self.assertEqual([], queued)
                redis.values["catchuparr:dispatch:channel-1"] = "1"
                ready_result = tasks.reconcile_recorders()
                # An active recorder lease prevents reconcile from scanning
                # cleanup concurrently with its task-owned recovery path.
                redis.values["catchuparr:recorder:channel-1"] = "9:owner"
                tasks.reconcile_recorders()

        self.assertEqual({"queued": 0, "cleanup_pending": 1}, pending_result)
        self.assertEqual({"queued": 0, "cleanup_pending": 0}, ready_result)
        self.assertEqual(["channel-1", "channel-1"], cleanup_calls)
        self.assertEqual([], queued)

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
        applied_state = {"active": active, "control": SimpleNamespace(paused=False, generation=3)}
        with _isolated_plugin_modules(fake_modules):
            tasks = importlib.import_module("catchuparr.tasks")
            with (
                patch("catchuparr.runtime.load_config", return_value=config),
                patch("catchuparr.configuration.reset_legacy_configuration", return_value=False),
                patch(
                    "catchuparr.configuration.load_applied_state",
                    side_effect=lambda: (applied_state["active"], applied_state["control"]),
                ),
                patch("catchuparr.logging_utils.event") as log_event,
            ):
                result = tasks.record_channel("channel-1", "older-generation", 3)
                current_generation = configuration_generation(active)
                stale_control = tasks.record_channel("channel-1", current_generation, 2)
                applied_state["control"] = SimpleNamespace(paused=True, generation=3)
                paused = tasks.record_channel("channel-1", current_generation, 3)
                legacy = tasks.record_channel("channel-1", current_generation)
                off_schedule = _active_configuration()
                off_schedule["recording_schedule"]["channels"]["channel-1"] = {
                    "mode": "weekly", "intervals": []
                }
                applied_state["active"] = off_schedule
                applied_state["control"] = SimpleNamespace(paused=False, generation=3)
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
                patch(
                    "catchuparr.configuration.load_applied_state",
                    return_value=(active, SimpleNamespace(paused=False, generation=5)),
                ),
                patch("catchuparr.engine.store.ArchiveStore", FakeStore),
            ):
                result = tasks.reconcile_recorders()
        self.assertEqual({"queued": 0, "cleanup_pending": 0}, result)
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
                    patch(
                        "catchuparr.configuration.load_applied_state",
                        return_value=(active, SimpleNamespace(paused=False, generation=4)),
                    ),
                ):
                    with self.assertRaisesRegex(RuntimeError, "DVR URL setup failed"):
                        tasks.record_channel("channel-1", expected_generation, 4)

            self.assertNotIn("catchuparr:recorder:channel-1", redis.values)
            self.assertFalse(any(
                key.startswith("catchuparr:recorder:stats-cap:") for key in redis.values
            ))
            self.assertEqual(ArchiveStore(archive_root).recorder_fence("channel-1"), 26)

    def test_initial_existing_native_viewer_is_borrowed_before_allowed_source(self):
        with tempfile.TemporaryDirectory() as temp:
            archive_root = Path(temp) / "archive"
            redis = FakeRedis()
            config = SimpleNamespace(channel_uuids=("channel-1",), archive_root=archive_root)
            active = _active_configuration()
            active["connection_sharing"] = {"channel-1": {
                "enabled": True, "live_stability_seconds": 0,
                "last_viewer_return_seconds": 1, "min_switch_interval_seconds": 0,
            }}
            expected_generation = configuration_generation(active)
            coordinator_sources = []
            polls = []
            recorder_inputs = []
            cleanup_calls = []
            recovery_events = []

            class StopEvent:
                def __init__(self):
                    self.stopped = False

                def set(self):
                    self.stopped = True

                def is_set(self):
                    return self.stopped

                def wait(self, _timeout=None):
                    return self.stopped

            class InlineThread:
                def __init__(self, *, target, **_kwargs):
                    self.target = target
                    self.ident = None

                def start(self):
                    self.ident = 1
                    # Keep the periodic worker out of this deterministic test;
                    # it exercises the synchronous initial viewer observation.
                    return None

                def join(self, timeout=None):
                    return None

            stop_event = StopEvent()

            class FakeLease:
                _value = "synthetic-lease"
                owner = "synthetic-owner"
                fence = 26

                def __init__(self, *_args, **_kwargs):
                    pass

                def acquire(self):
                    return self.fence

                def renew(self):
                    return True

                def release(self):
                    return True

            class BorrowCoordinator:
                def __init__(self, *_args, **kwargs):
                    self.dedicated_source_available = kwargs["dedicated_source_available"]
                    coordinator_sources.append(self.dedicated_source_available)
                    self.opening = SimpleNamespace(
                        episode_id="episode-synthetic",
                        capability=SimpleNamespace(intent_epoch=1),
                    )

                def poll(self, **_kwargs):
                    polls.append(self.dedicated_source_available)
                    recovery_events.append("native_observation")
                    # Model an already-connected native viewer on an excluded
                    # source. The initial synchronous observation must claim it
                    # before any dedicated candidate is considered.
                    return self.opening if len(polls) == 1 else None

                def record_return_after_cleanup(self, _prepared, *, now, outcome):
                    cleanup_calls.append((now, outcome))
                    stop_event.set()
                    return True

                def pending_return_intent(self):
                    return None

                def viewer_return_deadline_reached(self, *, now):
                    return False

            class FakeBorrowRecorder:
                def __init__(self, _store, _channel, input_url, _work, **kwargs):
                    self.input_url = input_url
                    self.headers = kwargs["input_headers"]

                def run_candidate(self, event, **_kwargs):
                    recorder_inputs.append((self.input_url, self.headers))
                    return SimpleNamespace(status="no_media", useful_segments=0)

            celery = _fake_module(
                "celery", shared_task=lambda *, name: lambda function: function,
            )
            apps = _fake_module("apps")
            apps_channels = _fake_module("apps.channels")
            apps_channels_tasks = _fake_module(
                "apps.channels.tasks", get_dvr_stream_base_url=lambda: "http://dispatcharr",
            )
            core = _fake_module("core")
            core_utils = _fake_module(
                "core.utils", RedisClient=SimpleNamespace(get_client=lambda: redis),
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
                    patch("catchuparr.engine.leases.RedisRecorderLease", FakeLease),
                    patch("catchuparr.configuration.reset_legacy_configuration", return_value=False),
                    patch(
                        "catchuparr.configuration.load_applied_state",
                        return_value=(active, SimpleNamespace(paused=False, generation=6)),
                    ),
                    patch(
                        "catchuparr.tasks._recorder_admission_state",
                        return_value={
                            "status": "ready", "active": active, "config": config,
                            "control": SimpleNamespace(generation=6),
                        },
                    ),
                    patch(
                        "catchuparr.recorder_proxy.ranked_source_candidates",
                        side_effect=lambda _channel, _active, *, all_sources: (
                            self.assertTrue(all_sources) or [
                                {"id": "excluded-fallback", "account_id": "synthetic"}
                            ]
                        ),
                    ),
                    patch("catchuparr.tasks._issue_recorder_capability", return_value=(None, None, None)),
                    patch(
                        "catchuparr.sharing_runtime.RecorderBorrowCoordinator",
                        BorrowCoordinator,
                    ),
                    patch("catchuparr.sharing_episode.BorrowEpisodeLedger", return_value=object()),
                    patch(
                        "catchuparr.sharing_runtime.retry_durable_open_cleanups",
                        side_effect=lambda *_args, **kwargs: (
                            recovery_events.append(("cleanup", kwargs.get("channel_uuid")))
                            if "episode_id" not in kwargs else None
                        ) or (0, 0),
                    ),
                    patch("catchuparr.sharing_runtime.retry_pending_open_cleanups"),
                    patch("catchuparr.sharing_runtime.borrow_cleanup_acknowledged", return_value=True),
                    patch("catchuparr.adapters.sharing_borrow_route.encode_borrow_capability", return_value="synthetic-borrow-token"),
                    patch("catchuparr.engine.recorder.FFmpegCopyRecorder", FakeBorrowRecorder),
                    patch(
                        "catchuparr.recorder_proxy.issue_recorder_attempt",
                        side_effect=lambda *_args, **_kwargs: self.fail(
                            "Dedicated source was issued before initial viewer borrow cleanup"
                        ),
                    ),
                    patch("catchuparr.adapters.recorder_proxy.core_api_supported", return_value=True),
                    patch("catchuparr.adapters.recorder_proxy.install_proxyserver_cleanup_hook", return_value=True),
                    patch(
                        "catchuparr.tasks.threading",
                        SimpleNamespace(
                            Event=lambda: stop_event, Lock=threading.Lock,
                            RLock=threading.RLock, Thread=InlineThread,
                        ),
                    ),
                ):
                    result = tasks.record_channel("channel-1", expected_generation, 6)

            self.assertEqual({"status": "stopped"}, result)
            self.assertEqual([True], coordinator_sources)
            self.assertEqual([True], polls)
            self.assertEqual(
                [("cleanup", "channel-1"), "native_observation"], recovery_events,
            )
            self.assertEqual(1, len(recorder_inputs))
            self.assertEqual(
                "http://dispatcharr/catchuparr/sharing/channel-1",
                recorder_inputs[0][0],
            )
            self.assertEqual(
                {"X-Catchuparr-Borrow": "synthetic-borrow-token",
                 "X-Catchuparr-Stats-Recorder": None},
                recorder_inputs[0][1],
            )
            self.assertEqual(1, len(cleanup_calls))
            self.assertEqual("failure", cleanup_calls[0][1])

    def test_override_capacity_or_start_failure_falls_through_ranked_sources(self):
        with tempfile.TemporaryDirectory() as temp:
            archive_root = Path(temp) / "archive"
            redis = FakeRedis()
            config = SimpleNamespace(channel_uuids=("channel-1",), archive_root=archive_root)
            active = _active_configuration()
            active["connection_sharing"] = {"channel-1": {"enabled": True}}
            expected_generation = configuration_generation(active)
            attempts = []
            stopped = []
            lifecycle = []
            attempt_switches = []
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
                    lifecycle.append(f"revoke:{self.stream_id}")
                    return True

                def operation_needs_switch(self, _redis_client):
                    return False

                def acknowledge_reaped(self, _redis_client):
                    lifecycle.append(f"ack:{self.stream_id}")
                    return True

            class FakeRecorder:
                outcomes = [
                    RuntimeError("input startup failed"),
                    SimpleNamespace(status="no_media", useful_segments=0, return_code=1),
                    SimpleNamespace(status="stopped", useful_segments=0, return_code=-15),
                ]

                def __init__(self, _store, _channel, input_url, _work, **kwargs):
                    self.input_url = input_url
                    self.headers = kwargs["input_headers"]

                def run_candidate(self, stop_event, **kwargs):
                    attempt_switches.append(kwargs.get("attempt_switch"))
                    outcome = self.outcomes.pop(0)
                    if isinstance(outcome, Exception):
                        raise outcome
                    if outcome.status == "stopped":
                        stop_event.set()
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
                recorder_module = importlib.import_module("catchuparr.engine.recorder")
                with (
                    patch("catchuparr.runtime.load_config", return_value=config),
                    patch("catchuparr.configuration.reset_legacy_configuration", return_value=False),
                    patch(
                        "catchuparr.configuration.load_applied_state",
                        return_value=(active, SimpleNamespace(paused=False, generation=6)),
                    ),
                    patch(
                        "catchuparr.recorder_proxy.ranked_source_candidates",
                        side_effect=lambda _channel, _active, *, all_sources: (
                            self.assertTrue(all_sources) or source_candidates
                        ),
                    ),
                    patch(
                        "catchuparr.recorder_proxy.current_sharing_authority_context",
                        return_value={"authority_revision": 2, "catalog_generation": "catalog-synthetic"},
                    ),
                    patch(
                        "catchuparr.sharing_runtime.RecorderBorrowCoordinator",
                        return_value=SimpleNamespace(
                            poll=lambda **_kwargs: lifecycle.append("observe_native") or None,
                        ),
                    ),
                    patch("catchuparr.sharing_runtime.retry_durable_open_cleanups", return_value=(0, 0)),
                    patch(
                        "catchuparr.recorder_proxy.issue_recorder_attempt",
                        side_effect=lambda _redis, _lease, **kwargs: (
                            lifecycle.append(f"issue:{kwargs['candidate']['id']}")
                            or self.assertEqual(2, kwargs["sharing_authority"]["authority_revision"])
                            or attempts.append(Attempt(kwargs["candidate"])) or attempts[-1]
                        ),
                    ),
                    patch(
                        "catchuparr.recorder_proxy.stop_recorder_attempt",
                        side_effect=lambda _redis, attempt, _lease: (
                            lifecycle.append(f"stop:{attempt.stream_id}")
                            or stopped.append(True) or True
                        ),
                    ),
                    patch("catchuparr.adapters.recorder_proxy.core_api_supported", return_value=True),
                    patch("catchuparr.adapters.recorder_proxy.install_proxyserver_cleanup_hook", return_value=True),
                    patch.object(recorder_module, "FFmpegCopyRecorder", FakeRecorder),
                ):
                    result = tasks.record_channel("channel-1", expected_generation, 6)

            self.assertEqual("stopped", result["status"])
            self.assertEqual(["44", "45", "46"], [attempt.stream_id for attempt in attempts])
            self.assertTrue(all(attempt.revoked for attempt in attempts))
            self.assertEqual(3, len(stopped))
            self.assertTrue(all(attempt.input_url.endswith("/catchuparr/recorder/channel-1") for attempt in attempts))
            self.assertTrue(all(switch is not None for switch in attempt_switches))
            self.assertEqual(
                ["observe_native",
                 "issue:44", "revoke:44", "stop:44", "ack:44",
                 "issue:45", "revoke:45", "stop:45", "ack:45",
                 "issue:46", "revoke:46", "stop:46", "ack:46"],
                lifecycle,
            )


if __name__ == "__main__":
    unittest.main()
