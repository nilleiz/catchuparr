import unittest

from catchuparr.compatibility import is_supported_dispatcharr_version


class DispatcharrMinimumVersionTests(unittest.TestCase):
    def test_accepts_minimum_and_newer_stable_versions(self):
        for version in (
            "0.32.0", "0.32.1", "0.33.0", "1.0.0", "0.32.0+build.7",
            "0.33.0-alpha.2",
        ):
            with self.subTest(version=version):
                self.assertTrue(is_supported_dispatcharr_version(version))

    def test_rejects_versions_below_minimum_and_minimum_prereleases(self):
        for version in ("0.31.9", "0.32.0-alpha", "0.32.0-rc.1"):
            with self.subTest(version=version):
                self.assertFalse(is_supported_dispatcharr_version(version))

    def test_rejects_malformed_versions(self):
        for version in ("", "0.32", "v0.33.0", "0.032.0", "0.32.0-01", "0.32.0+"):
            with self.subTest(version=version):
                self.assertFalse(is_supported_dispatcharr_version(version))
        self.assertFalse(is_supported_dispatcharr_version(None))


if __name__ == "__main__":
    unittest.main()
