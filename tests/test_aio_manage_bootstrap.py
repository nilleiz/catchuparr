import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.run_aio_integration import _manage_script_exec


class AioManageBootstrapTests(unittest.TestCase):
    def test_manage_entry_keeps_stream_worker_role_during_startup(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            manage_path = root / "manage.py"
            capture_path = root / "startup.json"
            manage_path.write_text(
                "import json, os, sys\n"
                "from pathlib import Path\n"
                "Path(os.environ['AIO_BOOTSTRAP_CAPTURE']).write_text(json.dumps({\n"
                "    'argv': sys.argv, 'name': __name__, 'file': __file__,\n"
                "    'package': __package__, 'spec': __spec__,\n"
                "}))\n",
                encoding="utf-8",
            )
            environment = dict(os.environ)
            environment["AIO_BOOTSTRAP_CAPTURE"] = str(capture_path)

            # This is the original runpy entry behavior: while the manage script
            # runs, runpy replaces argv[0] with the script path.
            runpy_source = (
                "import os,runpy,sys; from pathlib import Path; "
                "sys.argv=['gunicorn','shell']; "
                f"runpy.run_path({str(manage_path)!r},run_name='__main__')"
            )
            subprocess.run(
                [sys.executable, "-c", runpy_source],
                check=True,
                env=environment,
                capture_output=True,
                text=True,
            )
            runpy_observation = json.loads(capture_path.read_text(encoding="utf-8"))
            self.assertEqual(runpy_observation["argv"], [str(manage_path), "shell"])

            capture_path.unlink()
            direct_exec_source = (
                "import os,sys; from pathlib import Path; "
                "sys.argv=['gunicorn','shell']; "
                + _manage_script_exec(str(manage_path))
            )
            subprocess.run(
                [sys.executable, "-c", direct_exec_source],
                check=True,
                env=environment,
                capture_output=True,
                text=True,
            )
            exec_observation = json.loads(capture_path.read_text(encoding="utf-8"))

        self.assertEqual(exec_observation["argv"], ["gunicorn", "shell"])
        self.assertEqual(exec_observation["name"], "__main__")
        self.assertEqual(exec_observation["file"], str(manage_path))
        self.assertIsNone(exec_observation["package"])
        self.assertIsNone(exec_observation["spec"])


if __name__ == "__main__":
    unittest.main()
