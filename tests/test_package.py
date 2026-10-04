import json
import unittest
from pathlib import Path
from zipfile import ZipFile

from scripts.build_plugin import build


class PackageTest(unittest.TestCase):
    def test_dispatcharr_zip_layout(self):
        archive_path = build()
        with ZipFile(archive_path) as archive:
            names = archive.namelist()
            self.assertIn("catchuparr/plugin.py", names)
            self.assertIn("catchuparr/plugin.json", names)
            self.assertFalse(any("__pycache__" in name for name in names))
            manifest = json.loads(archive.read("catchuparr/plugin.json"))
            self.assertEqual(manifest["name"], "Catchuparr")
        self.assertTrue(Path(archive_path).is_file())
