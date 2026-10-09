import json
import logging
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr import recorder_proxy
from catchuparr.adapters import recorder_proxy as adapter
from catchuparr.configuration import SourceCatalog


class PipelineWatchError(Exception):
    pass


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.hashes = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, **kwargs):
        if kwargs.get("nx") and key in self.values:
            return False
        self.values[key] = value
        return True

    def setex(self, key, ttl, value):
        self.values[key] = value
        return True

    def expire(self, key, _ttl):
        return key in self.values or key in self.hashes

    def delete(self, *keys):
        deleted = 0
        for key in keys:
            deleted += int(self.values.pop(key, None) is not None)
            deleted += int(self.hashes.pop(key, None) is not None)
        return deleted

    def incr(self, key):
        value = int(self.values.get(key, 0)) + 1
        self.values[key] = value
        return value

    def decr(self, key):
        value = int(self.values.get(key, 0)) - 1
        self.values[key] = value
        return value

    def hset(self, key, field=None, value=None, mapping=None):
        target = self.hashes.setdefault(key, {})
        if mapping is not None:
            target.update({str(name): str(item) for name, item in mapping.items()})
        elif field is not None:
            target[str(field)] = str(value)

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hdel(self, key, *fields):
        target = self.hashes.get(key, {})
        for field in fields:
            target.pop(field, None)

    def exists(self, key):
        return int(key in self.values or key in self.hashes)

    def eval(self, script, numkeys, *args):
        if "reservation_state" in script:
            key = args[0]
            record = self.hashes.get(key, {})
            if record.get("reservation_state") != "reserved":
                return 0
            record["reservation_state"] = "releasing"
            record["state"] = "releasing"
            return 1
        raise AssertionError("unexpected Redis script")


class FakeDatabaseConnection:
    def __init__(
        self,
        alias="default",
        *,
        opened=True,
        in_atomic_block=False,
        autocommit=True,
        on_close=None,
    ):
        self.alias = alias
        self.connection = object() if opened else None
        self.in_atomic_block = in_atomic_block
        self._autocommit = autocommit
        self.on_commit_callbacks = []
        self.on_close = on_close
        self.close_calls = 0

    def get_autocommit(self):
        return self._autocommit

    def close(self):
        self.close_calls += 1
        if self.on_close is not None:
            self.on_close()


class FakeDatabaseConnections:
    def __init__(self, initialized):
        self.initialized = initialized
        self.all_calls = []
        self.alias_lookups = 0

    def all(self, *, initialized_only=False):
        self.all_calls.append(initialized_only)
        return list(self.initialized) if initialized_only else []

    def __getitem__(self, _alias):
        self.alias_lookups += 1
        raise AssertionError("connection helper must not initialize an alias")


class PipelineFakeRedis(FakeRedis):
    def __init__(self):
        super().__init__()
        self.execute_conflicts_remaining = 0

    def pipeline(self):
        return FakeRedisPipeline(self)


class FakeRedisPipeline:
    def __init__(self, redis):
        self.redis = redis
        self.commands = []
        self.watched = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def watch(self, *keys):
        self.watched.extend(keys)

    def get(self, key):
        return self.redis.get(key)

    def multi(self):
        return None

    def decr(self, key):
        self.commands.append(("decr", key))

    def delete(self, *keys):
        self.commands.append(("delete", keys))

    def execute(self):
        if self.redis.execute_conflicts_remaining:
            self.redis.execute_conflicts_remaining -= 1
            raise PipelineWatchError("simulated concurrent Redis update")
        for command, values in self.commands:
            if command == "decr":
                self.redis.decr(values)
            elif command == "delete":
                self.redis.delete(*values)
        self.commands.clear()


def _connection_pool_module():
    module = types.ModuleType("apps.m3u.connection_pool")

    def profile_credential_release_key(profile_id):
        return f"profile_credential_release:{profile_id}"

    def reserve_profile_slot(profile, redis_client):
        profile_key = f"profile_connections:{profile.id}"
        count = redis_client.incr(profile_key)
        if profile.max_streams and count > profile.max_streams:
            redis_client.decr(profile_key)
            return False, count - 1, "profile_full"
        credential_key = getattr(profile, "credential_key", None)
        if credential_key:
            credential_count = redis_client.incr(credential_key)
            if profile.max_streams and credential_count > profile.max_streams:
                redis_client.decr(credential_key)
                redis_client.decr(profile_key)
                return False, count - 1, "credential_full"
            redis_client.set(profile_credential_release_key(profile.id), credential_key)
        return True, count, None

    def release_profile_slot(profile_id, redis_client):
        marker = profile_credential_release_key(profile_id)
        credential_key = redis_client.get(marker)
        if credential_key:
            if isinstance(credential_key, bytes):
                credential_key = credential_key.decode()
            if int(redis_client.get(credential_key) or 0) > 0:
                redis_client.decr(credential_key)
            redis_client.delete(marker)
        profile_key = f"profile_connections:{profile_id}"
        if int(redis_client.get(profile_key) or 0) > 0:
            redis_client.decr(profile_key)

    module.profile_credential_release_key = profile_credential_release_key
    module.reserve_profile_slot = reserve_profile_slot
    module.release_profile_slot = release_profile_slot
    return module


def _pipeline_connection_pool_module():
    module = _connection_pool_module()

    def release_profile_slot(profile_id, redis_client):
        release_key = module.profile_credential_release_key(profile_id)
        profile_key = f"profile_connections:{profile_id}"
        for _ in range(10):
            with redis_client.pipeline() as pipe:
                try:
                    pipe.watch(release_key, profile_key)
                    profile_count = int(pipe.get(profile_key) or 0)
                    credential_key = pipe.get(release_key)
                    if credential_key:
                        pipe.watch(credential_key)
                        credential_count = int(pipe.get(credential_key) or 0)
                    else:
                        credential_count = 0
                    pipe.multi()
                    if credential_key and profile_count > 0 and credential_count > 0:
                        pipe.decr(credential_key)
                    if profile_count <= 1:
                        pipe.delete(release_key)
                    if profile_count > 0:
                        pipe.decr(profile_key)
                    pipe.execute()
                    return
                except PipelineWatchError:
                    continue

    module.release_profile_slot = release_profile_slot
    return module


class RecorderProxyTests(unittest.TestCase):
    def test_configuration_generation_includes_applied_schedule(self):
        active = {
            "channel_uuids": "00000000-0000-0000-0000-000000000001",
            "source_policies": {},
            "recording_schedule": {
                "timezone": "UTC",
                "channels": {
                    "00000000-0000-0000-0000-000000000001": {"mode": "continuous"}
                },
            },
        }
        scheduled = {
            **active,
            "recording_schedule": {
                "timezone": "UTC",
                "channels": {
                    "00000000-0000-0000-0000-000000000001": {
                        "mode": "weekly", "intervals": []
                    }
                },
            },
        }
        self.assertNotEqual(
            recorder_proxy.configuration_generation(active),
            recorder_proxy.configuration_generation(scheduled),
        )

    def test_provider_urls_are_redacted_from_messages_and_exceptions(self):
        try:
            raise RuntimeError("provider rejected https://user:secret@example.invalid/live")
        except RuntimeError:
            exception_info = sys.exc_info()
        record = logging.LogRecord(
            "catchuparr.adapters.recorder_proxy",
            logging.ERROR,
            __file__,
            1,
            "Recorder failed: https://user:secret@example.invalid/live",
            (),
            exception_info,
        )

        self.assertTrue(adapter._SourceURLRedactor(redact_all=True).filter(record))
        formatted = logging.Formatter().format(record)
        self.assertNotIn("user:secret@example.invalid", formatted)
        self.assertNotIn("https://", formatted)
        self.assertIn("<provider-url>", formatted)

    def test_forged_or_modified_capability_is_rejected(self):
        redis = FakeRedis()
        lease = SimpleNamespace(
            fence=7,
            _value="7:owner-secret",
            key="catchuparr:recorder:00000000-0000-0000-0000-000000000001",
        )
        django = types.ModuleType("django")
        django.__path__ = []
        django_conf = types.ModuleType("django.conf")
        django_conf.settings = SimpleNamespace(SECRET_KEY="test-signing-key")
        with patch.dict(sys.modules, {"django": django, "django.conf": django_conf}):
            attempt = recorder_proxy.issue_recorder_attempt(
                redis,
                lease,
                channel_uuid="00000000-0000-0000-0000-000000000001",
                candidate={"id": "44", "account_id": "12"},
                config_generation="generation-1",
                internal_base_url="http://dispatcharr",
            )
            valid = recorder_proxy.verify_recorder_capability(redis, attempt.capability)
            worker_record = adapter.read_worker_record(redis, attempt.worker_id)
            capability_record = json.loads(redis.get(attempt.capability_key))
            modified = attempt.capability[:-1] + ("A" if attempt.capability[-1] != "A" else "B")

            self.assertIsNotNone(valid)
            self.assertEqual("issued", worker_record["state"])
            self.assertEqual("none", worker_record["reservation_state"])
            self.assertEqual(32, len(worker_record["reservation_id"]))
            self.assertNotIn(attempt.capability, worker_record.values())
            self.assertNotIn("nonce", capability_record)
            self.assertIsNone(recorder_proxy.verify_recorder_capability(redis, modified))
            self.assertIsNone(recorder_proxy.verify_recorder_capability(redis, "forged-header"))
            self.assertNotIn(attempt.capability, redis.get(attempt.capability_key))
            attempt.revoke(redis)
            self.assertIsNone(recorder_proxy.verify_recorder_capability(redis, attempt.capability))

    def test_capability_stops_working_after_lease_owner_changes(self):
        redis = FakeRedis()
        lease_key = "catchuparr:recorder:00000000-0000-0000-0000-000000000001"
        redis.set(lease_key, "7:owner-secret")
        active = {
            "channel_uuids": "00000000-0000-0000-0000-000000000001",
            "source_policies": {},
        }
        binding = {
            "channel_uuid": "00000000-0000-0000-0000-000000000001",
            "config_generation": recorder_proxy.configuration_generation(active),
            "lease_fence": "7",
            "lease_key": lease_key,
            "lease_value": "7:owner-secret",
            "stream_id": "44",
            "account_id": "12",
            "worker_id": adapter.make_worker_id("channel", "44", "gen", 7, "a"),
            "capability_digest": "digest",
        }
        redis.set(recorder_proxy.CAPABILITY_PREFIX + "digest", json.dumps({
            **binding, "capability_digest": "digest"
        }))
        with patch.object(recorder_proxy, "_active_configuration", return_value=active), patch.object(
            recorder_proxy, "candidate_is_current", return_value=True
        ):
            self.assertTrue(recorder_proxy.capability_binding_current(redis, binding))
            self.assertFalse(
                recorder_proxy.capability_binding_current(
                    redis, {**binding, "account_id": "13"}
                )
            )
            redis.set(lease_key, "8:new-owner")
            self.assertFalse(recorder_proxy.capability_binding_current(redis, binding))

    def test_candidate_policy_filters_current_assignments_and_rejects_removed_source(self):
        channel_uuid = "00000000-0000-0000-0000-000000000001"
        accounts = ({"id": "12", "name": "Included"}, {"id": "13", "name": "Other"})
        active = {
            "channel_uuids": channel_uuid,
            "source_policies": {
                channel_uuid: {
                    "include_account_ids": ["12"],
                    "exclude_account_ids": [],
                    "priorities": [],
                    "known_account_ids": ["12", "13"],
                }
            },
        }
        assigned = SourceCatalog(
            channels=({"uuid": channel_uuid, "number": "1", "name": "News", "group": ""},),
            accounts=accounts,
            streams_by_channel={channel_uuid: (
                {"id": "44", "account_id": "12", "order": 0},
                {"id": "45", "account_id": "13", "order": 1},
            )},
        )
        settings_modules = {"catchuparr.configuration": types.ModuleType("catchuparr.configuration")}
        settings_modules["catchuparr.configuration"].source_catalog = lambda: assigned
        with patch.dict(sys.modules, settings_modules):
            self.assertEqual(["44"], [
                row["id"] for row in recorder_proxy.ranked_source_candidates(channel_uuid, active)
            ])
            self.assertTrue(recorder_proxy.candidate_is_current(channel_uuid, "44", "12", active))
            self.assertFalse(recorder_proxy.candidate_is_current(channel_uuid, "45", "13", active))
            removed = SourceCatalog(
                assigned.channels,
                accounts,
                {channel_uuid: ({"id": "45", "account_id": "13", "order": 1},)},
            )
            settings_modules["catchuparr.configuration"].source_catalog = lambda: removed
            self.assertFalse(recorder_proxy.candidate_is_current(channel_uuid, "44", "12", active))

    def test_ranked_source_candidates_releases_database_after_materializing_catalog(self):
        channel_uuid = "00000000-0000-0000-0000-000000000001"
        events = []
        default_connection = FakeDatabaseConnection(
            on_close=lambda: events.append("close_default")
        )
        connections = FakeDatabaseConnections([default_connection])
        configuration_module = types.ModuleType("catchuparr.configuration")
        catalog = SourceCatalog(
            channels=({"uuid": channel_uuid, "number": "1", "name": "Synthetic", "group": ""},),
            accounts=({"id": "12", "name": "Synthetic account"},),
            streams_by_channel={channel_uuid: ({"id": "44", "account_id": "12", "order": 0},)},
        )

        def source_catalog():
            events.append("catalog")
            return catalog

        configuration_module.source_catalog = source_catalog
        django_module = types.ModuleType("django")
        django_module.__path__ = []
        django_db_module = types.ModuleType("django.db")
        django_db_module.connections = connections
        django_module.db = django_db_module
        active = {
            "source_policies": {
                channel_uuid: {
                    "include_account_ids": ["12"],
                    "exclude_account_ids": [],
                    "priorities": [],
                    "known_account_ids": ["12"],
                }
            }
        }

        with patch.dict(sys.modules, {
            "catchuparr.configuration": configuration_module,
            "django": django_module,
            "django.db": django_db_module,
        }):
            candidates = recorder_proxy.ranked_source_candidates(channel_uuid, active)

        self.assertEqual(["catalog", "close_default"], events)
        self.assertEqual([True], connections.all_calls)
        self.assertEqual(0, connections.alias_lookups)
        self.assertEqual(["44"], [candidate["id"] for candidate in candidates])

        def failed_catalog():
            events.append("catalog_failed")
            raise RuntimeError("synthetic catalog failure")

        configuration_module.source_catalog = failed_catalog
        events.clear()
        with patch.dict(sys.modules, {
            "catchuparr.configuration": configuration_module,
            "django": django_module,
            "django.db": django_db_module,
        }), self.assertRaisesRegex(RuntimeError, "synthetic catalog failure"):
            recorder_proxy.ranked_source_candidates(channel_uuid, active)
        self.assertEqual(["catalog_failed", "close_default"], events)

    def test_database_release_skips_transactions_other_aliases_and_unopened_connections(self):
        atomic_connection = FakeDatabaseConnection(
            in_atomic_block=True, on_close=lambda: self.fail("atomic connection was closed")
        )
        manual_connection = FakeDatabaseConnection(
            autocommit=False, on_close=lambda: self.fail("manual transaction was closed")
        )
        other_alias = FakeDatabaseConnection(
            alias="analytics", on_close=lambda: self.fail("other alias was closed")
        )
        unopened = FakeDatabaseConnection(
            opened=False, on_close=lambda: self.fail("unopened connection was touched")
        )
        atomic_connection.on_commit_callbacks.append(lambda: None)
        connections = FakeDatabaseConnections(
            [atomic_connection, manual_connection, other_alias, unopened]
        )
        django_module = types.ModuleType("django")
        django_module.__path__ = []
        django_db_module = types.ModuleType("django.db")
        django_db_module.connections = connections
        django_module.db = django_db_module

        with patch.dict(sys.modules, {"django": django_module, "django.db": django_db_module}):
            recorder_proxy._close_database_connections()

        self.assertEqual([True], connections.all_calls)
        self.assertEqual(0, connections.alias_lookups)
        self.assertEqual(0, atomic_connection.close_calls)
        self.assertEqual(0, manual_connection.close_calls)
        self.assertEqual(0, other_alias.close_calls)
        self.assertEqual(0, unopened.close_calls)
        self.assertEqual(1, len(atomic_connection.on_commit_callbacks))

    def test_streaming_route_releases_orm_connections_before_returning_response(self):
        events = []
        binding = {"channel_uuid": "synthetic-channel", "worker_id": "synthetic-worker"}

        class FakeStreamingResponse(dict):
            status_code = 200

        response = FakeStreamingResponse({"Content-Type": "video/mp2t"})

        class FakeHttpResponse:
            def __init__(self, _content="", status=200):
                self.status_code = status

        class FakeStreamProfileManager:
            @staticmethod
            def filter(**_kwargs):
                return SimpleNamespace(exists=lambda: True)

        django_module = types.ModuleType("django")
        django_module.__path__ = []
        django_http_module = types.ModuleType("django.http")
        django_http_module.HttpResponse = FakeHttpResponse
        django_db_module = types.ModuleType("django.db")
        default_connection = FakeDatabaseConnection(
            on_close=lambda: events.append("close_default")
        )
        django_db_module.connections = FakeDatabaseConnections([default_connection])
        django_module.http = django_http_module
        django_module.db = django_db_module
        core_module = types.ModuleType("core")
        core_module.__path__ = []
        core_utils_module = types.ModuleType("core.utils")
        core_utils_module.RedisClient = SimpleNamespace(get_client=lambda: object())
        core_module.utils = core_utils_module
        core_models_module = types.ModuleType("core.models")
        core_models_module.PROXY_PROFILE_NAME = "Proxy"
        core_module.models = core_models_module
        apps_module = types.ModuleType("apps")
        apps_module.__path__ = []
        apps_channels_module = types.ModuleType("apps.channels")
        apps_channels_module.__path__ = []
        apps_channels_models = types.ModuleType("apps.channels.models")
        apps_channels_models.StreamProfile = SimpleNamespace(
            objects=FakeStreamProfileManager()
        )
        apps_module.channels = apps_channels_module
        apps_channels_module.models = apps_channels_models
        modules = {
            "django": django_module,
            "django.http": django_http_module,
            "django.db": django_db_module,
            "core": core_module,
            "core.utils": core_utils_module,
            "core.models": core_models_module,
            "apps": apps_module,
            "apps.channels": apps_channels_module,
            "apps.channels.models": apps_channels_models,
        }
        from catchuparr import configuration, runtime

        with (
            patch.dict(sys.modules, modules),
            patch.object(runtime, "require_supported_version"),
            patch.object(configuration, "load_active_configuration", return_value={}),
            patch.object(recorder_proxy, "verify_recorder_capability", return_value=binding),
            patch.object(recorder_proxy, "capability_binding_current", return_value=True),
            patch.object(adapter, "install_proxyserver_cleanup_hook", return_value=True),
            patch.object(adapter, "core_api_supported", return_value=True),
            patch.object(
                adapter,
                "open_managed_source",
                side_effect=lambda *_args: (events.append("open_source"), response)[1],
            ),
        ):
            result = recorder_proxy.stream_recorder_view(
                SimpleNamespace(
                    method="GET",
                    headers={recorder_proxy.CAPABILITY_HEADER: "synthetic-token"},
                ),
                "synthetic-channel",
            )

        self.assertIs(result, response)
        self.assertEqual(["open_source", "close_default"], events)

    def test_plugin_cleanup_releases_only_its_pooled_profile_reservation(self):
        connection_pool = _connection_pool_module()
        apps = types.ModuleType("apps")
        apps.__path__ = []
        m3u = types.ModuleType("apps.m3u")
        m3u.__path__ = []
        m3u.connection_pool = connection_pool
        with patch.dict(sys.modules, {
            "apps": apps,
            "apps.m3u": m3u,
            "apps.m3u.connection_pool": connection_pool,
        }):
            for release_order in (("plugin", "live"), ("live", "plugin")):
                redis = FakeRedis()
                profile = SimpleNamespace(
                    id=7, max_streams=3, credential_key="server_group_connections:1:abc"
                )
                live_reserved, _, _ = connection_pool.reserve_profile_slot(profile, redis)
                reservation_id = "reservation-" + release_order[0]
                facade = adapter._CredentialMarkerRedisFacade(redis, 7, reservation_id)
                plugin_reserved, _, _ = connection_pool.reserve_profile_slot(profile, facade)
                self.assertTrue(live_reserved and plugin_reserved)
                worker_id = adapter.make_worker_id("channel", "44", "generation", 7, reservation_id)
                adapter.write_worker_record(redis, worker_id, {
                    "reservation_state": "reserved",
                    "state": "active",
                    "profile_id": "7",
                    "reservation_id": reservation_id,
                })
                live_marker = connection_pool.profile_credential_release_key(7)
                plugin_marker = adapter.reservation_credential_marker_key(reservation_id)
                redis.set("channel_stream:44", "live-source-assignment")
                redis.set("stream_profile:44", "live-profile-assignment")

                for action in release_order:
                    if action == "plugin":
                        adapter._release_worker_reservation(redis, worker_id)
                        # Duplicate teardown is an idempotent no-op.
                        adapter._release_worker_reservation(redis, worker_id)
                    else:
                        connection_pool.release_profile_slot(7, redis)

                self.assertEqual(0, int(redis.get("profile_connections:7") or 0))
                self.assertEqual(0, int(redis.get("server_group_connections:1:abc") or 0))
                self.assertIsNone(redis.get(live_marker))
                self.assertIsNone(redis.get(plugin_marker))
                self.assertEqual("live-source-assignment", redis.get("channel_stream:44"))
                self.assertEqual("live-profile-assignment", redis.get("stream_profile:44"))

    def test_plugin_cleanup_remaps_032_pipeline_release_markers(self):
        connection_pool = _pipeline_connection_pool_module()
        apps = types.ModuleType("apps")
        apps.__path__ = []
        m3u = types.ModuleType("apps.m3u")
        m3u.__path__ = []
        m3u.connection_pool = connection_pool
        with patch.dict(sys.modules, {
            "apps": apps,
            "apps.m3u": m3u,
            "apps.m3u.connection_pool": connection_pool,
        }):
            for release_order in (("plugin",), ("plugin", "live"), ("live", "plugin")):
                redis = PipelineFakeRedis()
                profile = SimpleNamespace(
                    id=7, max_streams=3, credential_key="server_group_connections:1:abc"
                )
                live_reserved = plugin_reserved = False
                if "live" in release_order:
                    live_reserved, _, _ = connection_pool.reserve_profile_slot(profile, redis)
                if "plugin" in release_order:
                    reservation_id = "reservation-" + "-".join(release_order)
                    facade = adapter._CredentialMarkerRedisFacade(redis, 7, reservation_id)
                    plugin_reserved, _, _ = connection_pool.reserve_profile_slot(profile, facade)
                    worker_id = adapter.make_worker_id(
                        "channel", "44", "generation", 7, reservation_id
                    )
                    adapter.write_worker_record(redis, worker_id, {
                        "reservation_state": "reserved",
                        "state": "active",
                        "profile_id": "7",
                        "reservation_id": reservation_id,
                    })
                self.assertEqual("live" in release_order, live_reserved)
                self.assertEqual("plugin" in release_order, plugin_reserved)
                redis.set("channel_stream:44", "live-source-assignment")
                redis.set("stream_profile:44", "live-profile-assignment")

                for action in release_order:
                    if action == "plugin":
                        adapter._release_worker_reservation(redis, worker_id)
                        self.assertFalse(adapter._release_worker_reservation(redis, worker_id))
                    else:
                        connection_pool.release_profile_slot(7, redis)

                self.assertEqual(0, int(redis.get("profile_connections:7") or 0))
                self.assertEqual(0, int(redis.get("server_group_connections:1:abc") or 0))
                self.assertIsNone(redis.get(connection_pool.profile_credential_release_key(7)))
                self.assertEqual("live-source-assignment", redis.get("channel_stream:44"))
                self.assertEqual("live-profile-assignment", redis.get("stream_profile:44"))

    def test_032_release_retry_exhaustion_preserves_reservation_marker(self):
        connection_pool = _pipeline_connection_pool_module()
        apps = types.ModuleType("apps")
        apps.__path__ = []
        m3u = types.ModuleType("apps.m3u")
        m3u.__path__ = []
        m3u.connection_pool = connection_pool
        with patch.dict(sys.modules, {
            "apps": apps,
            "apps.m3u": m3u,
            "apps.m3u.connection_pool": connection_pool,
        }):
            redis = PipelineFakeRedis()
            profile = SimpleNamespace(
                id=7, is_default=True, max_streams=3,
                credential_key="server_group_connections:1:abc"
            )
            reserved, _, _ = connection_pool.reserve_profile_slot(profile, redis)
            reservation_id = "reservation-conflict-exhaustion"
            facade = adapter._CredentialMarkerRedisFacade(redis, 7, reservation_id)
            plugin_reserved, _, _ = connection_pool.reserve_profile_slot(profile, facade)
            worker_id = adapter.make_worker_id("channel", "44", "generation", 7, reservation_id)
            adapter.write_worker_record(redis, worker_id, {
                "reservation_state": "reserved",
                "state": "active",
                "profile_id": "7",
                "reservation_id": reservation_id,
            })
            marker = adapter.reservation_credential_marker_key(reservation_id)
            self.assertTrue(reserved and plugin_reserved)
            self.assertEqual(profile.credential_key, redis.get(marker))

            redis.execute_conflicts_remaining = 10
            with self.assertLogs("catchuparr.adapters.recorder_proxy", level="ERROR"):
                self.assertFalse(adapter._release_worker_reservation(redis, worker_id))

            record = adapter.read_worker_record(redis, worker_id)
            self.assertEqual("release_failed", record["reservation_state"])
            self.assertEqual(2, int(redis.get("profile_connections:7") or 0))
            self.assertEqual(2, int(redis.get(profile.credential_key) or 0))
            self.assertEqual(profile.credential_key, redis.get(marker))
            self.assertEqual(profile.credential_key, redis.get(
                connection_pool.profile_credential_release_key(7)
            ))
            self.assertFalse(adapter._release_worker_reservation(redis, worker_id))
            self.assertEqual(2, int(redis.get("profile_connections:7") or 0))
            self.assertEqual(2, int(redis.get(profile.credential_key) or 0))

    def test_032_release_commits_after_transient_watch_conflicts(self):
        connection_pool = _pipeline_connection_pool_module()
        apps = types.ModuleType("apps")
        apps.__path__ = []
        m3u = types.ModuleType("apps.m3u")
        m3u.__path__ = []
        m3u.connection_pool = connection_pool
        with patch.dict(sys.modules, {
            "apps": apps,
            "apps.m3u": m3u,
            "apps.m3u.connection_pool": connection_pool,
        }):
            redis = PipelineFakeRedis()
            profile = SimpleNamespace(
                id=7, is_default=True, max_streams=3,
                credential_key="server_group_connections:1:abc"
            )
            reserved, _, _ = connection_pool.reserve_profile_slot(profile, redis)
            reservation_id = "reservation-transient-conflicts"
            facade = adapter._CredentialMarkerRedisFacade(redis, 7, reservation_id)
            plugin_reserved, _, _ = connection_pool.reserve_profile_slot(profile, facade)
            worker_id = adapter.make_worker_id("channel", "44", "generation", 7, reservation_id)
            adapter.write_worker_record(redis, worker_id, {
                "reservation_state": "reserved",
                "state": "active",
                "profile_id": "7",
                "reservation_id": reservation_id,
            })
            self.assertTrue(reserved and plugin_reserved)

            redis.execute_conflicts_remaining = 2
            self.assertTrue(adapter._release_worker_reservation(redis, worker_id))

            record = adapter.read_worker_record(redis, worker_id)
            self.assertEqual("released", record["reservation_state"])
            self.assertEqual(1, int(redis.get("profile_connections:7") or 0))
            self.assertEqual(1, int(redis.get(profile.credential_key) or 0))
            self.assertIsNone(redis.get(adapter.reservation_credential_marker_key(reservation_id)))
            self.assertEqual(
                profile.credential_key,
                redis.get(connection_pool.profile_credential_release_key(7)),
            )
            self.assertFalse(adapter._release_worker_reservation(redis, worker_id))

    def test_032_failed_reservation_ledger_write_keeps_exhausted_rollback_failed(self):
        connection_pool = _pipeline_connection_pool_module()
        apps = types.ModuleType("apps")
        apps.__path__ = []
        m3u = types.ModuleType("apps.m3u")
        m3u.__path__ = []
        m3u.connection_pool = connection_pool
        with patch.dict(sys.modules, {
            "apps": apps,
            "apps.m3u": m3u,
            "apps.m3u.connection_pool": connection_pool,
        }):
            redis = PipelineFakeRedis()
            profile = SimpleNamespace(
                id=7, is_default=True, max_streams=3,
                credential_key="server_group_connections:1:abc"
            )
            account = SimpleNamespace(
                id=12,
                profiles=SimpleNamespace(
                    filter=lambda **_kwargs: SimpleNamespace(order_by=lambda *_args: [profile])
                ),
            )
            source = SimpleNamespace(id=44)
            reservation_id = "reservation-rollback-exhaustion"
            worker_id = adapter.make_worker_id(
                "channel", "44", "generation", 7, reservation_id
            )
            adapter.write_worker_record(redis, worker_id, {
                "state": "issued",
                "reservation_state": "none",
                "reservation_id": reservation_id,
            })
            redis.execute_conflicts_remaining = 10
            write_record = adapter.write_worker_record

            def fail_reserved_record(redis_client, target_worker, values):
                if values.get("reservation_state") == "reserved":
                    raise RuntimeError("synthetic ledger write failure")
                write_record(redis_client, target_worker, values)

            with patch.object(adapter, "write_worker_record", side_effect=fail_reserved_record):
                with self.assertRaisesRegex(RuntimeError, "synthetic ledger write failure"):
                    adapter._reserve_source_profile(redis, worker_id, source, account)

            record = adapter.read_worker_record(redis, worker_id)
            self.assertEqual("release_failed", record["reservation_state"])
            self.assertEqual(1, int(redis.get("profile_connections:7") or 0))
            self.assertEqual(1, int(redis.get(profile.credential_key) or 0))
            self.assertEqual(
                profile.credential_key,
                redis.get(adapter.reservation_credential_marker_key(reservation_id)),
            )
            self.assertIsNone(redis.get(connection_pool.profile_credential_release_key(7)))

    def test_032_failed_reservation_ledger_write_commits_rollback_and_cleans_marker(self):
        connection_pool = _pipeline_connection_pool_module()
        apps = types.ModuleType("apps")
        apps.__path__ = []
        m3u = types.ModuleType("apps.m3u")
        m3u.__path__ = []
        m3u.connection_pool = connection_pool
        with patch.dict(sys.modules, {
            "apps": apps,
            "apps.m3u": m3u,
            "apps.m3u.connection_pool": connection_pool,
        }):
            redis = PipelineFakeRedis()
            profile = SimpleNamespace(
                id=7, is_default=True, max_streams=3,
                credential_key="server_group_connections:1:abc"
            )
            account = SimpleNamespace(
                id=12,
                profiles=SimpleNamespace(
                    filter=lambda **_kwargs: SimpleNamespace(order_by=lambda *_args: [profile])
                ),
            )
            source = SimpleNamespace(id=44)
            reservation_id = "reservation-rollback-success"
            worker_id = adapter.make_worker_id(
                "channel", "44", "generation", 7, reservation_id
            )
            adapter.write_worker_record(redis, worker_id, {
                "state": "issued",
                "reservation_state": "none",
                "reservation_id": reservation_id,
            })
            redis.execute_conflicts_remaining = 2
            write_record = adapter.write_worker_record

            def fail_reserved_record(redis_client, target_worker, values):
                if values.get("reservation_state") == "reserved":
                    raise RuntimeError("synthetic ledger write failure")
                write_record(redis_client, target_worker, values)

            with patch.object(adapter, "write_worker_record", side_effect=fail_reserved_record):
                with self.assertRaisesRegex(RuntimeError, "synthetic ledger write failure"):
                    adapter._reserve_source_profile(redis, worker_id, source, account)

            record = adapter.read_worker_record(redis, worker_id)
            self.assertEqual("released", record["reservation_state"])
            self.assertEqual(0, int(redis.get("profile_connections:7") or 0))
            self.assertEqual(0, int(redis.get(profile.credential_key) or 0))
            self.assertIsNone(redis.get(adapter.reservation_credential_marker_key(reservation_id)))
            self.assertIsNone(redis.get(connection_pool.profile_credential_release_key(7)))

    def test_capacity_selection_skips_full_default_profile(self):
        connection_pool = _connection_pool_module()
        profiles = [
            SimpleNamespace(id=1, is_default=True, max_streams=1),
            SimpleNamespace(id=2, is_default=False, max_streams=1),
        ]
        account = SimpleNamespace(id=12, profiles=SimpleNamespace(
            filter=lambda **_kwargs: SimpleNamespace(order_by=lambda *_args: profiles)
        ))
        source = SimpleNamespace(id=44)
        redis = FakeRedis()
        reserved_calls = []

        def reserve(profile, facade):
            reserved_calls.append(profile.id)
            if profile.id == 1:
                return False, 0, "profile_full"
            return True, 1, None

        connection_pool.reserve_profile_slot = reserve
        apps = types.ModuleType("apps")
        apps.__path__ = []
        m3u = types.ModuleType("apps.m3u")
        m3u.__path__ = []
        m3u.connection_pool = connection_pool
        with patch.dict(sys.modules, {
            "apps": apps,
            "apps.m3u": m3u,
            "apps.m3u.connection_pool": connection_pool,
        }):
            worker_id = adapter.make_worker_id("channel", "44", "generation", 1, "attempt")
            adapter.write_worker_record(redis, worker_id, {"reservation_id": "reservation"})
            chosen = adapter._reserve_source_profile(redis, worker_id, source, account)

        self.assertEqual([1, 2], reserved_calls)
        self.assertEqual(2, chosen.id)
        self.assertEqual("reserved", adapter.read_worker_record(redis, worker_id)["reservation_state"])


if __name__ == "__main__":
    unittest.main()
