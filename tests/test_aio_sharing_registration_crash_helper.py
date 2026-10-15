import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


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
                bootstrap = """
import importlib
import importlib.util
import sys
import types

django = types.ModuleType('django')
def setup():
    assert importlib.import_module('dispatcharr.settings').READY is True
    assert importlib.import_module('core.utils').READY is True
    assert importlib.import_module('catchuparr').READY is True
django.setup = setup
sys.modules['django'] = django

spec = importlib.util.spec_from_file_location('crash_probe', __import__('os').environ['PROBE_HELPER_PATH'])
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)
probe._setup_django()
assert sys.path[:2] == [__import__('os').environ['CATCHUPARR_APP_ROOT'], __import__('os').environ['CATCHUPARR_PLUGIN_ROOT']]
print('bootstrap-ok')
"""
                result = subprocess.run(
                    [sys.executable, "-c", bootstrap],
                    cwd=app_root,
                    env={
                        **os.environ,
                        "CATCHUPARR_APP_ROOT": str(app_root),
                        "CATCHUPARR_PLUGIN_ROOT": str(plugin_root),
                        "PROBE_HELPER_PATH": str(
                            Path(__file__).resolve().parents[1]
                            / "scripts"
                            / "aio_sharing_registration_crash.py"
                        ),
                    },
                    capture_output=True,
                    text=True,
                    check=False,
                )

                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), "bootstrap-ok")


if __name__ == "__main__":
    unittest.main()
