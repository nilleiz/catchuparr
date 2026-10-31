import unittest
from pathlib import Path

from scripts.release_notes import release_notes

ROOT = Path(__file__).resolve().parents[1]
CHANGELOG = """# Changelog

Intro.

## [1.2.10] - Prerelease

- Newer synthetic change.

## [1.2.1] - Prerelease

### Fixed

- Synthetic fix.

## [1.2.0] - 2026-01-01

- Older synthetic change.
"""


class ReleaseNotesTests(unittest.TestCase):
    def test_only_the_requested_section_is_returned(self):
        self.assertEqual(release_notes(CHANGELOG, "1.2.1"), "### Fixed\n\n- Synthetic fix.\n")

    def test_last_section_runs_to_the_end(self):
        self.assertEqual(release_notes(CHANGELOG, "1.2.0"), "- Older synthetic change.\n")

    def test_version_prefix_does_not_match_a_longer_version(self):
        self.assertEqual(release_notes(CHANGELOG, "1.2.10"), "- Newer synthetic change.\n")

    def test_missing_or_empty_section_fails(self):
        with self.assertRaises(ValueError):
            release_notes(CHANGELOG, "9.9.9")
        with self.assertRaises(ValueError):
            release_notes("## [1.0.0]\n\n## [0.9.0]\n- old\n", "1.0.0")

    def test_current_changelog_section_excludes_older_releases(self):
        version = __import__("json").loads((ROOT / "catchuparr/plugin.json").read_text())["version"]
        notes = release_notes((ROOT / "CHANGELOG.md").read_text(), version)
        self.assertNotIn("## [", notes)
        self.assertNotIn("# Changelog", notes)
        self.assertTrue(notes.strip())

    def test_release_workflow_publishes_only_the_current_section(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        self.assertIn("release_notes(changelog, version)", workflow)
        self.assertNotIn('write_text(changelog)', workflow)


if __name__ == "__main__":
    unittest.main()
