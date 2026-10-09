import threading
import unittest
from types import SimpleNamespace

from scripts.aio_native_redis_diagnostic import NativeRedisInitDiagnostic


class _FakePipeline:
    def __init__(self):
        self.command_stack = []
        self.result = []

    def execute(self):
        return self.result


class _FakeRedis:
    def __init__(self, *, decode_responses=True):
        self.connection_pool = SimpleNamespace(
            connection_kwargs={
                "db": 4,
                "host": "synthetic-host",
                "password": "secret",
                "decode_responses": decode_responses,
            },
        )
        self.pipeline_instance = _FakePipeline()
        self.info_calls = 0

    def info(self, _section):
        self.info_calls += 1
        return {"run_id": "synthetic-secret-run-id"}

    def pipeline(self, *_args, **_kwargs):
        self.pipeline_instance.connection_pool = self.connection_pool
        return self.pipeline_instance

    def execute_command(self, _command, *_args, **_kwargs):
        return 1


class NativeRedisInitDiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.keys = SimpleNamespace(
            channel_metadata=lambda channel_id: f"metadata:{channel_id}",
            channel_owner=lambda channel_id: f"owner:{channel_id}",
            clients=lambda channel_id: f"clients:{channel_id}",
        )
        self.diagnostic = NativeRedisInitDiagnostic(
            worker_id="synthetic-worker",
            redis_keys=self.keys,
            native_server=SimpleNamespace(),
        )

    def test_only_exact_synthetic_keys_are_observed_and_values_are_redacted(self):
        client = _FakeRedis()
        buffer_client = _FakeRedis(decode_responses=False)
        original_execute = client.execute_command
        original_pipeline = client.pipeline
        self.diagnostic._register_client(client, role="decoded", prestart=True)
        self.diagnostic._register_client(buffer_client, role="buffer", prestart=True)

        self.assertEqual(client.execute_command("DEL", "metadata:other-worker"), 1)
        self.assertEqual(client.execute_command("DEL", "metadata:synthetic-worker"), 1)

        pipeline = client.pipeline()
        pipeline.command_stack = [
            (("HSET", "metadata:synthetic-worker", "state", "SECRET_STATE"), {}),
            (("DEL", "metadata:other-worker"), {}),
            (("EXPIRE", "metadata:synthetic-worker", 3600), {}),
            (("HGET", "metadata:synthetic-worker", "state"), {}),
            (("GET", "owner:synthetic-worker"), {}),
            (("SCARD", "clients:synthetic-worker"), {}),
        ]
        pipeline.result = [1, 0, 1, b"ACTIVE_SECRET", b"SECRET_OWNER", 3]
        returned = pipeline.execute()
        self.assertEqual(returned, pipeline.result)
        self.diagnostic._record_command_result(
            "EXISTS", ("metadata:synthetic-worker",), 1,
            actor="synthetic.guard", redis_role="buffer",
        )
        self.diagnostic._record_command_result(
            "EXISTS", ("metadata:synthetic-worker",), 0,
            actor="synthetic.guard", redis_role="buffer",
        )

        report = self.diagnostic.close()
        encoded = repr(report)
        self.assertIn("metadata_write", encoded)
        self.assertIn("metadata_delete", encoded)
        self.assertIn("metadata_expire", encoded)
        self.assertIn("metadata_state_present", encoded)
        self.assertIn("owner_present", encoded)
        self.assertIn("client_count", encoded)
        metadata_exists_values = {
            event.get("metadata_exists")
            for event in report["events"]
            if event["event"] == "metadata_exists"
        }
        self.assertEqual(metadata_exists_values, {True, False})
        self.assertTrue(report["redis_targets_match"])
        self.assertEqual(len(report["redis_clients"]), 2)
        self.assertEqual(
            {entry["roles"] for entry in report["redis_clients"]},
            {"decoded", "buffer"},
        )
        self.assertNotIn("SECRET_STATE", encoded)
        self.assertNotIn("ACTIVE_SECRET", encoded)
        self.assertNotIn("SECRET_OWNER", encoded)
        self.assertNotIn("synthetic-secret-run-id", encoded)
        self.assertNotIn("synthetic-host", encoded)
        self.assertNotIn("password", encoded)
        self.assertNotIn("metadata:synthetic-worker", encoded)
        self.assertEqual(client.execute_command, original_execute)
        self.assertEqual(client.pipeline, original_pipeline)
        self.assertFalse(any(owner is pipeline for _kind, owner, *_ in self.diagnostic._patches))
        self.assertEqual(self.diagnostic.close(), report)

    def test_original_command_exception_is_preserved(self):
        class CommandError(RuntimeError):
            pass

        client = _FakeRedis()
        failure = CommandError("synthetic failure")
        client.execute_command = lambda *_args, **_kwargs: (_ for _ in ()).throw(failure)
        self.diagnostic._register_client(client, prestart=True)

        with self.assertRaises(CommandError) as raised:
            client.execute_command("DEL", "metadata:synthetic-worker")

        self.assertIs(raised.exception, failure)
        self.assertIn("metadata_delete", repr(self.diagnostic.close()))

    def test_event_buffer_is_bounded(self):
        for index in range(120):
            self.diagnostic._record(f"synthetic_event_{index}", event_index=index)

        report = self.diagnostic.close()
        self.assertEqual(len(report["events"]), 96)
        self.assertEqual(report["omitted_events"], 24)

    def test_native_initialization_thread_and_client_wrappers_restore(self):
        diagnostic = self.diagnostic
        diagnostic.native_server.client_managers = {}
        manager = None
        client_manager = None

        class SyntheticNativeManager:
            def _evaluate_ownership_from_redis(self, redis_client):
                self.observed_client = redis_client
                return {"owner": "SECRET_OWNER", "state": "SECRET_STATE"}

            def run(self):
                self._evaluate_ownership_from_redis(_FakeRedis(decode_responses=False))

        SyntheticNativeManager.__module__ = "apps.proxy.live_proxy.synthetic"

        class SyntheticClientManager:
            def add_client(self, _opaque_client):
                return "registered"

        class SyntheticProxyServer:
            def initialize_channel(self, url, channel_id, **_kwargs):
                nonlocal manager, client_manager
                if str(channel_id) == "synthetic-worker":
                    manager = SyntheticNativeManager()
                    client_manager = SyntheticClientManager()
                    diagnostic.native_server.client_managers[channel_id] = client_manager
                    thread = threading.Thread(target=manager.run)
                    thread.start()
                    thread.join(timeout=1)
                return "initialized"

        original_descriptor = SyntheticProxyServer.__dict__["initialize_channel"]
        original_thread_start = threading.Thread.start
        diagnostic._patch_initialize(SyntheticProxyServer)
        diagnostic._patch_thread_start()
        proxy_server = SyntheticProxyServer()

        self.assertEqual(
            proxy_server.initialize_channel("synthetic-worker", "other-worker"),
            "initialized",
        )
        self.assertEqual(
            proxy_server.initialize_channel(
                "synthetic-url", "synthetic-worker",
            ),
            "initialized",
        )
        self.assertEqual(client_manager.add_client("opaque-client-id"), "registered")
        events = [item["event"] for item in diagnostic.events]
        self.assertIn("channel_initialize_enter", events)
        self.assertIn("native_manager_thread_start_enter", events)
        self.assertIn("ownership_check_enter", events)
        self.assertIn("ownership_check_return", events)
        self.assertFalse(diagnostic.summary()["observed"]["ownership_allowed"])
        self.assertIn("client_add_return", events)
        self.assertEqual(events.count("channel_initialize_enter"), 1)
        self.assertEqual(
            len([client for client in diagnostic._clients.values() if "native_buffer" in client["roles"]]),
            1,
        )

        report = diagnostic.close()
        self.assertNotIn("SECRET_OWNER", repr(report))
        self.assertNotIn("SECRET_STATE", repr(report))
        self.assertEqual(
            SyntheticProxyServer.__dict__["initialize_channel"],
            original_descriptor,
        )
        self.assertIs(threading.Thread.start, original_thread_start)
        self.assertNotIn("_evaluate_ownership_from_redis", vars(manager))
        self.assertNotIn("add_client", vars(client_manager))

    def test_get_buffer_factory_registers_without_init_time_fingerprint(self):
        diagnostic = self.diagnostic
        client = _FakeRedis(decode_responses=False)

        class SyntheticRedisClient:
            @classmethod
            def get_buffer(cls):
                return client

        diagnostic._patch_client_factory(SyntheticRedisClient, "get_buffer", "buffer")
        self.assertIs(SyntheticRedisClient.get_buffer(), client)
        self.assertEqual(client.info_calls, 0)
        record = diagnostic._clients[id(client)]
        self.assertEqual(record["roles"], {"buffer"})
        self.assertTrue(callable(client.execute_command))
        self.assertTrue(callable(client.pipeline))
        diagnostic.close()

    def test_ownership_guard_tracks_real_redis_argument_and_false_transition(self):
        diagnostic = self.diagnostic
        client = _FakeRedis(decode_responses=False)

        class SyntheticGuard:
            def __init__(self):
                self.results = iter((True, False))

            def _evaluate_ownership_from_redis(self, redis_client):
                self.last_client = redis_client
                return next(self.results)

        guard = SyntheticGuard()
        diagnostic._patch_ownership_method(guard)
        self.assertTrue(guard._evaluate_ownership_from_redis(client))
        self.assertFalse(guard._evaluate_ownership_from_redis(client))
        self.assertIs(guard.last_client, client)
        self.assertEqual(client.info_calls, 0)

        report = diagnostic.close()
        transitions = [
            event.get("ownership_allowed")
            for event in report["events"]
            if event["event"] == "ownership_check_return"
        ]
        self.assertEqual(transitions, [True, False])
        self.assertTrue(report["observed"]["ownership_check"])
        self.assertTrue(report["observed"]["ownership_allowed"])
        self.assertEqual(
            {event["redis_role"] for event in report["events"]},
            {"native_buffer"},
        )

    def test_two_clients_with_one_target_require_every_fingerprint(self):
        self.diagnostic._register_client(_FakeRedis(), role="decoded", prestart=True)
        self.diagnostic._register_client(
            _FakeRedis(decode_responses=False), role="buffer", prestart=True,
        )
        self.assertTrue(self.diagnostic.summary()["redis_targets_match"])

        incomplete = _FakeRedis()
        incomplete.info = lambda _section: {}
        self.diagnostic._register_client(incomplete, role="extra", prestart=True)
        self.assertFalse(self.diagnostic.summary()["redis_targets_match"])


if __name__ == "__main__":
    unittest.main()
