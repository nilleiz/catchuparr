import json
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from zipfile import ZipFile

from catchuparr.plugin import Plugin
from scripts.build_plugin import REQUIRED_MODULES, build


class PackageTest(unittest.TestCase):
    def test_dispatcharr_zip_layout(self):
        archive_path = build()
        with ZipFile(archive_path) as archive:
            names = archive.namelist()
            self.assertIn("catchuparr/plugin.py", names)
            self.assertIn("catchuparr/plugin.json", names)
            for module in REQUIRED_MODULES:
                self.assertIn(f"catchuparr/{module}", names)
            self.assertFalse(any("__pycache__" in name for name in names))
            manifest = json.loads(archive.read("catchuparr/plugin.json"))
            self.assertEqual(manifest["name"], "Catchuparr")
            self.assertEqual(manifest["version"], "0.2.1")
            self.assertEqual(Plugin.version, manifest["version"])
            self.assertEqual(Path(archive_path).name, "catchuparr-0.2.1.zip")
            field_ids = [field["id"] for field in manifest["fields"]]
            self.assertIn("filter_config", field_ids)
            self.assertNotIn("channel_uuids", field_ids)
            self.assertNotIn("source_rules", field_ids)
        self.assertTrue(Path(archive_path).is_file())

    def test_runtime_modules_import_from_built_zip(self):
        archive_path = build()
        import_check = textwrap.dedent(
            """
            import importlib
            import sys
            import types

            archive = sys.argv[1]
            sys.path.insert(0, archive)
            celery = types.ModuleType("celery")
            celery.shared_task = lambda *, name: lambda function: function
            sys.modules["celery"] = celery

            modules = (
                "catchuparr.plugin",
                "catchuparr.compatibility",
                "catchuparr.configuration",
                "catchuparr.source_rules",
                "catchuparr.recorder_proxy",
                "catchuparr.runtime",
                "catchuparr.tasks",
                "catchuparr.views",
                "catchuparr.security",
                "catchuparr.http",
                "catchuparr.ts_http",
                "catchuparr.xc_runtime",
                "catchuparr.adapters.m3u",
                "catchuparr.adapters.recorder_proxy",
                "catchuparr.adapters.xc",
                "catchuparr.engine.admission",
                "catchuparr.engine.leases",
                "catchuparr.engine.playlist",
                "catchuparr.engine.recorder",
                "catchuparr.engine.store",
            )
            for name in modules:
                module = importlib.import_module(name)
                if not module.__file__.startswith(archive + "/"):
                    raise AssertionError(f"{name} did not load from the plugin ZIP")
            """
        )
        with tempfile.TemporaryDirectory() as cwd:
            subprocess.run(
                [sys.executable, "-c", import_check, str(archive_path)],
                check=True,
                cwd=cwd,
                capture_output=True,
                text=True,
            )
