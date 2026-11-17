import importlib.util
import unittest
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "upstream_release", Path(__file__).resolve().parents[1] / "scripts/upstream_release.py"
)
upstream_release = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(upstream_release)


class UpstreamReleaseTests(unittest.TestCase):
    def test_newest_stable_skips_prereleases_drafts_and_odd_tags(self):
        releases = [
            {"tag_name": "v0.36.0-rc1", "prerelease": True},
            {"tag_name": "v0.35.0", "draft": True},
            {"tag_name": "nightly"},
            {"tag_name": "v0.34.1"},
            {"tag_name": "v0.34.0"},
            {"tag_name": "v0.9.0"},
        ]
        self.assertEqual(upstream_release.newest_stable(releases), ((0, 34, 1), "0.34.1"))

    def test_no_stable_release(self):
        self.assertEqual(upstream_release.newest_stable([]), (None, None))

    def test_highest_tested_reads_the_real_policy_file(self):
        source = (Path(__file__).resolve().parents[1] / "catchuparr/compatibility.py").read_text()
        self.assertGreaterEqual(upstream_release.highest_tested(source), (0, 33, 0))

    def test_highest_tested_picks_numeric_maximum(self):
        source = 'TESTED_DISPATCHARR_VERSIONS = ("0.9.0", "0.33.0", "0.10.2")'
        self.assertEqual(upstream_release.highest_tested(source), (0, 33, 0))


if __name__ == "__main__":
    unittest.main()
