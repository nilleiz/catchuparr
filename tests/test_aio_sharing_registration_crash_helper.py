import importlib
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.aio_sharing_registration_crash import _setup_django


class CrashProbeBootstrapTests(unittest.TestCase):
    def test_child_imports_native_app_and_plugin_from_explicit_roots(self):
        with tempfile.TemporaryDirectory(prefix="synthetic-app-root-") as app_directory:
            with tempfile.TemporaryDirectory(prefix="synthetic-plugin-root-") as plugin_directory:
                app_root = Path(app_directory)
                plugin_root = Path(plugin_directory)
                (app_root / "dispatcharr").mkdir()
                (app_root / "dispatcharr" / "__init__.py").write_text("")
                (app_root / "dispatcharr" / "settings.py").write_text("READY = True\n")
                (app_root / "core").mkdir()
                (app_root / "core" / "__init__.py").write_text("")
                (app_root / "core" / "utils.py").write_text("READY = True\n")
                (plugin_root / "catchuparr").mkdir()
                (plugin_root / "catchuparr" / "__init__.py").write_text("READY = True\n")

                def setup_django():
                    self.assertTrue(importlib.import_module("dispatcharr.settings").READY)
                    self.assertTrue(importlib.import_module("core.utils").READY)
                    self.assertTrue(importlib.import_module("catchuparr").READY)

                fake_django = types.SimpleNamespace(setup=setup_django)
                configured_path = []
                with (
                    patch.object(sys, "path", []),
                    patch.dict(sys.modules, {"django": fake_django}),
                    patch.dict(
                        "os.environ",
                        {
                            "CATCHUPARR_APP_ROOT": str(app_root),
                            "CATCHUPARR_PLUGIN_ROOT": str(plugin_root),
                        },
                    ),
                ):
                    _setup_django()
                    configured_path = list(sys.path)

                self.assertEqual(configured_path[:2], [str(app_root), str(plugin_root)])


if __name__ == "__main__":
    unittest.main()
