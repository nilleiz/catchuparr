import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.run_aio_integration import _manage_script_exec as _full_aio_manage_script_exec
from scripts.run_aio_native_parity import _manage_script_exec


class NativeParityRunnerTests(unittest.TestCase):
    def test_uses_full_aio_manage_entrypoint_and_globals(self):
        with tempfile.TemporaryDirectory() as directory:
            probe = Path(directory) / "probe.py"
            probe.write_text("print('synthetic probe started')\n", encoding="utf-8")
            probe_exec = _manage_script_exec(str(probe))
            self.assertEqual(probe_exec, _full_aio_manage_script_exec(str(probe)))

            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "from pathlib import Path; " + probe_exec,
                ],
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "synthetic probe started")

    def test_probe_plugin_path_resolves_modules_from_plugin_manager_root(self):
        with tempfile.TemporaryDirectory() as directory:
            plugin_root = Path(directory)
            package = plugin_root / "catchuparr"
            package.mkdir()
            (package / "__init__.py").write_text(
                "ORIGIN = 'plugin-manager-root'\n", encoding="utf-8"
            )
            source = (
                "from scripts.aio_native_playback_parity_probe import _add_plugin_root; "
                f"_add_plugin_root({str(plugin_root)!r}); "
                "import catchuparr; print(catchuparr.ORIGIN)"
            )
            result = subprocess.run(
                [sys.executable, "-c", source],
                capture_output=True,
                text=True,
                check=False,
                cwd=Path(__file__).resolve().parents[1],
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "plugin-manager-root")


if __name__ == "__main__":
    unittest.main()
