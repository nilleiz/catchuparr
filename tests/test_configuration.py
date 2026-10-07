import json
import shutil
import sys
import tempfile
import threading
import types
import unittest
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

from catchuparr import configuration
from catchuparr.configuration import (
    SourceCatalog,
    apply_configuration,
    load_active_configuration,
    load_draft_settings,
    source_catalog,
    validate_configuration,
)
from catchuparr.runtime import load_runtime_settings


@dataclass(frozen=True)
class Policy:
    mode: str
    account_ids: frozenset[str]
    priorities: tuple[tuple[str, int], ...] = ()


class ConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.active_path = self.root / "outside-plugin" / ".catchuparr-active-settings.json"
        self.settings = {
            "channel_uuids": "",
            "archive_root": str(self.root),
            "retention_hours": 24,
            "max_storage_gib": 20,
            "source_rules": "rule text",
        }
        self.catalog = SourceCatalog(
            channels=(
                {"uuid": "channel-1", "number": "7", "name": "News", "group": "Local"},
                {"uuid": "channel-2", "number": "8", "name": "Sports", "group": "Local"},
            ),
            accounts=({"id": "12", "name": "Provider"},),
            streams_by_channel={
                "channel-1": ({"id": "44", "account_id": "12", "order": 0},)
            },
        )

    def tearDown(self):
        self.directory.cleanup()

    def _source_rules_stub(self, *, failing=False):
        module = types.ModuleType("catchuparr.source_rules")

        def compile_rules(text, channels, accounts):
            if failing:
                raise ValueError("unknown source selector")
            self.assertEqual("rule text", text)
            self.assertEqual("channel-1", channels[0]["uuid"])
            return {"channel-1": Policy("fallback", frozenset({"12"}), (("12", 0),))}

        module.compile_source_rules = compile_rules
        module.rank_candidates = lambda policy, candidates: list(candidates)
        return module

    def test_validate_preview_has_only_safe_candidate_fields(self):
        with patch.dict(sys.modules, {"catchuparr.source_rules": self._source_rules_stub()}):
            result = validate_configuration(self.settings, self.catalog)

        self.assertTrue(result["valid"])
        self.assertEqual("44", result["channels"][0]["candidates"][0]["stream_id"])
        self.assertEqual("Provider", result["channels"][0]["candidates"][0]["account_name"])
        serialized = json.dumps(result)
        self.assertNotIn("http://", serialized)
        self.assertNotIn("token", serialized)

    def test_draft_does_not_activate_until_apply_and_apply_is_portable(self):
        before = load_active_configuration(self.active_path)
        self.assertIsNone(before)

        with patch.dict(sys.modules, {"catchuparr.source_rules": self._source_rules_stub()}):
            result = apply_configuration(self.settings, self.catalog, self.active_path)

        active = load_active_configuration(self.active_path)
        self.assertTrue(result["applied"])
        self.assertEqual(["12"], active["source_policies"]["channel-1"]["account_ids"])
        serialized = self.active_path.read_text()
        self.assertNotIn("Provider", serialized)
        self.assertNotIn("url", serialized.lower())

    def test_compile_error_preserves_previous_active_file(self):
        good = self._source_rules_stub()
        with patch.dict(sys.modules, {"catchuparr.source_rules": good}):
            apply_configuration(self.settings, self.catalog, self.active_path)
        original = self.active_path.read_bytes()
        failing_parser = self._source_rules_stub(failing=True)
        with patch.dict(sys.modules, {"catchuparr.source_rules": failing_parser}):
            with self.assertRaisesRegex(ValueError, "unknown source selector"):
                apply_configuration(self.settings, self.catalog, self.active_path)
        self.assertEqual(original, self.active_path.read_bytes())

    def test_corrupt_active_snapshot_fails_closed_without_using_draft(self):
        self.active_path.parent.mkdir(parents=True)
        self.active_path.write_text("{partial", encoding="utf-8")
        with patch.object(configuration, "active_settings_path", return_value=self.active_path):
            with self.assertRaises(json.JSONDecodeError):
                load_runtime_settings({"archive_root": "/draft/path"})

    def test_active_snapshot_survives_plugin_directory_replacement(self):
        plugins_root = self.root / "plugins"
        plugin_dir = plugins_root / "catchuparr"
        plugin_dir.mkdir(parents=True)
        state_path = plugins_root / ".catchuparr-active-settings.json"
        with patch.object(configuration, "active_settings_path", return_value=state_path):
            with patch.dict(sys.modules, {"catchuparr.source_rules": self._source_rules_stub()}):
                apply_configuration(self.settings, self.catalog)
            shutil.rmtree(plugin_dir)
            active = load_active_configuration()
        self.assertTrue(state_path.is_file())
        self.assertEqual(str(self.root), active["archive_root"])
        self.assertEqual({"channel-1"}, set(active["source_policies"]))

    def test_atomic_snapshot_reads_remain_valid_during_apply(self):
        with patch.dict(sys.modules, {"catchuparr.source_rules": self._source_rules_stub()}):
            apply_configuration(self.settings, self.catalog, self.active_path)
            start = threading.Event()
            failures = []

            def writer():
                start.wait()
                try:
                    for _ in range(20):
                        apply_configuration(self.settings, self.catalog, self.active_path)
                except Exception as exc:  # surfaced in the test thread
                    failures.append(exc)

            worker = threading.Thread(target=writer)
            worker.start()
            start.set()
            for _ in range(80):
                try:
                    current = load_active_configuration(self.active_path)
                    self.assertEqual({"channel-1"}, set(current["source_policies"]))
                except Exception as exc:
                    failures.append(exc)
                    break
            worker.join()
        self.assertEqual([], failures)

    def test_catalog_uses_only_current_assignments_and_safe_stream_metadata(self):
        channel = types.SimpleNamespace(
            uuid="channel-1", channel_number=7, name="News",
            channel_group=types.SimpleNamespace(name="Local"),
        )
        unassigned_channel = types.SimpleNamespace(
            uuid="channel-2", channel_number=8, name="Sports",
            channel_group=types.SimpleNamespace(name="Local"),
        )
        account = types.SimpleNamespace(id=12, name="Provider")
        stream = types.SimpleNamespace(
            id=44, name="News HD", m3u_account_id=12, m3u_account=account,
            url="https://provider.invalid/secret?token=hidden",
        )
        assignment = types.SimpleNamespace(channel=channel, stream=stream, order=3)

        class Query:
            def select_related(self, *args):
                self.related = args
                return self

            def order_by(self, *args):
                return [assignment]

        class ChannelStream:
            objects = Query()

        class ChannelQuery:
            def select_related(self, *args):
                self.related = args
                return self

            def all(self):
                return [channel, unassigned_channel]

        class Channel:
            objects = ChannelQuery()

        class AccountQuery:
            def all(self):
                return self

            def only(self, *args):
                return [account]

        channels = types.ModuleType("apps.channels")
        channel_models = types.ModuleType("apps.channels.models")
        channel_models.Channel = Channel
        channel_models.ChannelStream = ChannelStream
        channels.models = channel_models
        m3u = types.ModuleType("apps.m3u")
        m3u_models = types.ModuleType("apps.m3u.models")
        m3u_models.M3UAccount = types.SimpleNamespace(objects=AccountQuery())
        m3u.models = m3u_models
        apps = types.ModuleType("apps")
        apps.channels = channels
        apps.m3u = m3u
        with patch.dict(sys.modules, {
            "apps": apps,
            "apps.channels": channels,
            "apps.channels.models": channel_models,
            "apps.m3u": m3u,
            "apps.m3u.models": m3u_models,
        }):
            catalog = source_catalog()

        self.assertEqual(
            ["channel-1", "channel-2"], [item["uuid"] for item in catalog.channels]
        )
        self.assertEqual("7", catalog.channels[0]["number"])
        self.assertEqual("Local", catalog.channels[0]["group"])
        self.assertEqual(
            {"id", "name", "account_id", "order"},
            set(catalog.streams_by_channel["channel-1"][0]),
        )
        self.assertNotIn("url", repr(catalog))
        self.assertEqual("12", catalog.accounts[0]["id"])
        self.assertEqual((), catalog.streams_by_channel.get("channel-2", ()))

    def test_plugin_config_row_is_preferred_to_context_fallback(self):
        plugins = types.ModuleType("apps.plugins")
        models = types.ModuleType("apps.plugins.models")

        class Manager:
            def filter(self, **kwargs):
                self.kwargs = kwargs
                return self

            def first(self):
                return types.SimpleNamespace(settings={"source_rules": "saved draft"})

        class PluginConfig:
            objects = Manager()

        models.PluginConfig = PluginConfig
        plugins.models = models
        apps = types.ModuleType("apps")
        apps.plugins = plugins
        with patch.dict(sys.modules, {
            "apps": apps,
            "apps.plugins": plugins,
            "apps.plugins.models": models,
        }):
            settings = load_draft_settings({"source_rules": "unsaved"})
        self.assertEqual("saved draft", settings["source_rules"])


if __name__ == "__main__":
    unittest.main()
