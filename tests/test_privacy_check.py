import io
import json
import os
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from scripts import privacy_check

# Assembled at runtime so this file never contains the patterns it tests.
PRIVATE_IP = ".".join(["192", "168", "1", "20"])
DOC_IP = ".".join(["192", "0", "2", "5"])
HOME = "/" + "home/someone/notes"
LOCAL_HOST = "router." + "fritz" + ".box"
MAC = ":".join(["aa"] * 6)
FOREIGN_MAIL = "me" + "@" + "private-mail.de"


def rules(text, denylist=None):
    return [rule for _, rule in privacy_check.findings(text, denylist)]


class StructuralRuleTests(unittest.TestCase):
    def test_private_addresses_are_found_documentation_ranges_are_not(self):
        self.assertEqual(["private network address"], rules(f"host {PRIVATE_IP}"))
        self.assertEqual(["private network address"], rules("net " + ".".join(["10", "1", "2", "0"]) + "/24"))
        self.assertEqual([], rules(f"example {DOC_IP} and 127.0.0.1 and 203.0.113.9"))
        self.assertEqual([], rules("version 1.2.3.4.5 and 300.1.1.1"))

    def test_paths_hosts_macs_and_mail(self):
        self.assertEqual(["home directory path"], rules(f"see {HOME}"))
        self.assertEqual([], rules("/" + "home/runner/work/repo"))
        self.assertEqual(["local host name"], rules(f"open {LOCAL_HOST}"))
        self.assertEqual([], rules("open localhost and example.org"))
        self.assertEqual(["MAC address"], rules(f"device {MAC}"))
        self.assertEqual(["e-mail address"], rules(f"write {FOREIGN_MAIL}"))
        self.assertEqual([], rules("noreply" + "@anthropic.com, a@example.org, b@host.invalid"))


class DenylistTests(unittest.TestCase):
    def test_terms_match_whole_words_case_insensitively_and_regexes_apply(self):
        denylist = privacy_check.compile_denylist("# comment\nSynthetic Box\nre:(?-i:\\bQuux\\b)\n")
        self.assertEqual(["private denylist entry 1"], rules("a synthetic box here", denylist))
        self.assertEqual([], rules("synthetic boxes", denylist))
        self.assertEqual(["private denylist entry 2"], rules("Quux played", denylist))
        self.assertEqual([], rules("quux played", denylist))

    def test_environment_wins_over_the_file_and_absence_is_reported(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "terms.txt"
            path.write_text("FromFile\n", encoding="utf-8")
            with patch.dict(os.environ, {privacy_check.DENYLIST_ENV: "FromEnv"}):
                self.assertTrue(privacy_check.load_denylist(path)[0].search("fromenv"))
            with patch.dict(os.environ, {privacy_check.DENYLIST_ENV: ""}):
                self.assertTrue(privacy_check.load_denylist(path)[0].search("fromfile"))
                self.assertIsNone(privacy_check.load_denylist(Path(root) / "missing.txt"))


class CommandTests(unittest.TestCase):
    def run_main(self, *args, env_terms=""):
        output = io.StringIO()
        with patch.dict(os.environ, {privacy_check.DENYLIST_ENV: env_terms}), redirect_stdout(output):
            code = privacy_check.main([*args, "--denylist", "/nonexistent"])
        return code, output.getvalue()

    def test_findings_name_the_place_but_never_the_private_text(self):
        with tempfile.TemporaryDirectory() as root:
            note = Path(root) / "note.md"
            note.write_text(f"fine\nwatched Synthetic Secret on {PRIVATE_IP}\n", encoding="utf-8")
            code, output = self.run_main("--file", str(note), env_terms="Synthetic Secret")
        self.assertEqual(1, code)
        self.assertIn("note.md:2: private network address", output)
        self.assertIn("note.md:2: private denylist entry 1", output)
        self.assertNotIn("Synthetic Secret", output)
        self.assertNotIn(PRIVATE_IP, output)

    def test_pull_request_text_and_commit_messages_are_checked(self):
        with tempfile.TemporaryDirectory() as root:
            event = Path(root) / "event.json"
            event.write_text(json.dumps({"pull_request": {"title": "Clean", "body": f"see {HOME}"}}), encoding="utf-8")
            code, output = self.run_main("--event", str(event))
            self.assertEqual(1, code)
            self.assertIn("pull request body:1: home directory path", output)

            repo = Path(root) / "repo"
            repo.mkdir()
            git = ["git", "-C", str(repo), "-c", "user.name=Synthetic", "-c", "user.email=s@example.org"]
            subprocess.run([*git, "init", "-q"], check=True)
            subprocess.run([*git, "commit", "-q", "--allow-empty", "-m", "first"], check=True)
            subprocess.run([*git, "commit", "-q", "--allow-empty", "-m", f"second {LOCAL_HOST}"], check=True)
            code, output = self.run_main("--root", str(repo), "--commits", "HEAD~1..HEAD")
            self.assertEqual(1, code)
            self.assertIn("message:1: local host name", output)
            code, _ = self.run_main("--root", str(repo), "--commits", "HEAD~1")
            self.assertEqual(0, code)

    def test_tracked_files_are_checked_and_binary_files_skipped(self):
        with tempfile.TemporaryDirectory() as root:
            repo = Path(root)
            subprocess.run(["git", "-C", root, "init", "-q"], check=True)
            (repo / "clean.md").write_text("nothing private\n", encoding="utf-8")
            (repo / "image.bin").write_bytes(b"\0" + PRIVATE_IP.encode())
            subprocess.run(["git", "-C", root, "add", "."], check=True)
            self.assertEqual(0, self.run_main("--root", root, "--tree")[0])
            (repo / "leak.md").write_text(f"{MAC}\n", encoding="utf-8")
            subprocess.run(["git", "-C", root, "add", "."], check=True)
            code, output = self.run_main("--root", root, "--tree")
        self.assertEqual(1, code)
        self.assertIn("leak.md:1: MAC address", output)


if __name__ == "__main__":
    unittest.main()
