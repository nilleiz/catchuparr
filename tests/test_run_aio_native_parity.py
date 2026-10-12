import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.run_aio_native_parity import _manage_shell_source


class NativeParityRunnerTests(unittest.TestCase):
    def test_manage_shell_payload_runs_without_imported_path_global(self):
        with tempfile.TemporaryDirectory() as directory:
            probe = Path(directory) / "probe.py"
            probe.write_text("print('synthetic probe started')\n", encoding="utf-8")

            result = subprocess.run(
                [sys.executable, "-c", _manage_shell_source(str(probe))],
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "synthetic probe started")


if __name__ == "__main__":
    unittest.main()
