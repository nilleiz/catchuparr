import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.aio_sharing_registration_crash import _setup_django


class CrashProbeBootstrapTests(unittest.TestCase):
    def test_child_uses_explicit_installed_plugin_root_before_django_setup(self):
        with tempfile.TemporaryDirectory(prefix="synthetic-plugin-root-") as directory:
            fake_django = types.SimpleNamespace(setup=lambda: None)
            with (
                patch.dict(sys.modules, {"django": fake_django}),
                patch.dict("os.environ", {"CATCHUPARR_PLUGIN_ROOT": directory}),
            ):
                _setup_django()
            self.assertEqual(str(Path(directory).resolve()), sys.path[0])


if __name__ == "__main__":
    unittest.main()
