import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from scripts.ci_provenance import (
    ARTIFACT_NAME,
    WORKFLOW_PATH,
    ProvenanceError,
    _archive_inventory,
    _artifact_contents,
    _compare_archive_to_source,
    _trusted_run,
    package_inventory,
    reuse_ci_artifact,
    validate_provenance,
)

REPOSITORY = "example/catchuparr"
WORKFLOW_ID = 1234
SOURCE_SHA = "a" * 40
SOURCE_TREE = "b" * 40
BASE_SHA = "d" * 40
MERGE_SHA = "c" * 40
ARCHIVE_NAME = "catchuparr-0.6.0.zip"


def _make_archive() -> tuple[bytes, dict[str, str]]:
    contents = {
        "catchuparr/plugin.py": b"class Plugin: pass\n",
        "catchuparr/plugin.json": b'{"version":"0.6.0"}\n',
        "catchuparr/logo.png": b"synthetic-logo-bytes",
    }
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, body in contents.items():
            archive.writestr(name, body)
    inventory = {name: hashlib.sha256(body).hexdigest() for name, body in contents.items()}
    return output.getvalue(), inventory


def _run() -> dict:
    return {
        "id": 4567,
        "run_attempt": 1,
        "workflow_id": WORKFLOW_ID,
        "event": "pull_request",
        "status": "completed",
        "conclusion": "success",
        "head_sha": SOURCE_SHA,
        "repository": {"full_name": REPOSITORY},
        "head_repository": {"full_name": REPOSITORY},
        "pull_requests": [{
            "number": 12,
            "head": {"sha": SOURCE_SHA, "repo": {"full_name": REPOSITORY}},
            "base": {"sha": BASE_SHA, "repo": {"full_name": REPOSITORY}},
        }],
        "jobs": [
            {"name": "test-and-package", "conclusion": "success"},
            {"name": "AIO integration (0.32.0)", "conclusion": "success"},
        ],
    }


def _provenance(archive: bytes, inventory: dict[str, str]) -> dict:
    return {
        "schema_version": 1,
        "repository": REPOSITORY,
        "event": "pull_request",
        "workflow_path": WORKFLOW_PATH,
        "workflow_ref": f"{REPOSITORY}/{WORKFLOW_PATH}@refs/pull/12/merge",
        "run_id": 4567,
        "run_attempt": 1,
        "source_sha": MERGE_SHA,
        "source_tree": SOURCE_TREE,
        "artifact_name": ARTIFACT_NAME,
        "archive_name": ARCHIVE_NAME,
        "archive_sha256": hashlib.sha256(archive).hexdigest(),
        "package_inventory": inventory,
    }


def _tested_commit(*, parents: tuple[str, str] = (BASE_SHA, SOURCE_SHA)) -> dict:
    return {
        "sha": MERGE_SHA,
        "tree": {"sha": SOURCE_TREE},
        "parents": [{"sha": parent} for parent in parents],
    }


class CIProvenanceTest(unittest.TestCase):
    def test_inventory_matches_only_package_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "catchuparr"
            root.mkdir()
            (root / "plugin.py").write_bytes(b"synthetic")
            (root / "plugin.json").write_text('{"version":"0.6.0"}\n')
            (root / "logo.png").write_bytes(b"synthetic png")
            (root / "runtime.pyc").write_bytes(b"ignored")
            self.assertEqual(
                set(package_inventory(root)),
                {"catchuparr/plugin.py", "catchuparr/plugin.json", "catchuparr/logo.png"},
            )

    def test_archive_files_are_compared_byte_for_byte_to_source(self):
        archive, inventory = _make_archive()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "catchuparr"
            root.mkdir()
            for name, body in (
                ("plugin.py", b"class Plugin: pass\n"),
                ("plugin.json", b'{"version":"0.6.0"}\n'),
                ("logo.png", b"synthetic-logo-bytes"),
            ):
                (root / name).write_bytes(body)
            _compare_archive_to_source(archive, root, inventory)
            (root / "plugin.py").write_bytes(b"changed source")
            with self.assertRaises(ProvenanceError):
                _compare_archive_to_source(archive, root, inventory)

    def test_trusted_provenance_matches_run_tree_and_package(self):
        archive, inventory = _make_archive()
        result = validate_provenance(
            _provenance(archive, inventory), repository=REPOSITORY,
            workflow_id=WORKFLOW_ID, run=_run(), tested_commit=_tested_commit(), source_tree=SOURCE_TREE,
            expected_inventory=inventory, archive_name=ARCHIVE_NAME,
            archive_bytes=archive,
        )
        self.assertEqual(result["source_tree"], SOURCE_TREE)

    def test_rejects_spoofed_run_identity(self):
        archive, inventory = _make_archive()
        provenance = _provenance(archive, inventory)
        for field, value in (
            ("repository", "attacker/catchuparr"),
            ("event", "workflow_dispatch"),
            ("workflow_path", "scripts/fake.yml"),
            ("workflow_ref", "attacker/catchuparr/.github/workflows/ci.yml@refs/pull/1/merge"),
            ("run_id", 9999),
            ("run_attempt", 2),
            ("source_sha", "e" * 40),
            ("source_tree", "d" * 40),
            ("artifact_name", "untrusted-artifact"),
        ):
            with self.subTest(field=field):
                changed = dict(provenance)
                changed[field] = value
                with self.assertRaises(ProvenanceError):
                    validate_provenance(
                        changed, repository=REPOSITORY, workflow_id=WORKFLOW_ID,
                        run=_run(), tested_commit=_tested_commit(), source_tree=SOURCE_TREE,
                        expected_inventory=inventory, archive_name=ARCHIVE_NAME,
                        archive_bytes=archive,
                    )

    def test_rejects_failed_or_untrusted_runs(self):
        archive, inventory = _make_archive()
        provenance = _provenance(archive, inventory)
        run = _run()
        run["conclusion"] = "failure"
        self.assertFalse(_trusted_run(run, REPOSITORY, WORKFLOW_ID))
        run = _run()
        run["jobs"][1]["conclusion"] = "failure"
        self.assertFalse(_trusted_run(run, REPOSITORY, WORKFLOW_ID))
        run = _run()
        run["workflow_id"] = WORKFLOW_ID + 1
        self.assertFalse(_trusted_run(run, REPOSITORY, WORKFLOW_ID))
        run = _run()
        run["head_repository"]["full_name"] = "fork/catchuparr"
        self.assertFalse(_trusted_run(run, REPOSITORY, WORKFLOW_ID))
        run = _run()
        run["jobs"][0]["conclusion"] = "failure"
        with self.assertRaises(ProvenanceError):
            validate_provenance(
                provenance, repository=REPOSITORY, workflow_id=WORKFLOW_ID,
                run=run, tested_commit=_tested_commit(), source_tree=SOURCE_TREE, expected_inventory=inventory,
                archive_name=ARCHIVE_NAME, archive_bytes=archive,
            )

    def test_rejects_archive_digest_inventory_and_paths(self):
        archive, inventory = _make_archive()
        for mutate in (
            lambda p: p.update(archive_sha256="0" * 64),
            lambda p: p.update(package_inventory={"catchuparr/plugin.py": "0" * 64}),
        ):
            provenance = _provenance(archive, inventory)
            mutate(provenance)
            with self.assertRaises(ProvenanceError):
                validate_provenance(
                    provenance, repository=REPOSITORY, workflow_id=WORKFLOW_ID,
                    run=_run(), source_tree=SOURCE_TREE,
                    tested_commit=_tested_commit(),
                    expected_inventory=inventory, archive_name=ARCHIVE_NAME,
                    archive_bytes=archive,
                )
        altered = io.BytesIO()
        with zipfile.ZipFile(altered, "w") as changed:
            changed.writestr("catchuparr/plugin.py", b"changed source")
            changed.writestr("catchuparr/plugin.json", b'{"version":"0.6.0"}\n')
            changed.writestr("catchuparr/logo.png", b"synthetic-logo-bytes")
        self.assertNotEqual(_archive_inventory(altered.getvalue()), inventory)

    def test_artifact_requires_exact_files_and_rejects_spoofed_paths(self):
        archive, inventory = _make_archive()
        provenance = json.dumps(_provenance(archive, inventory)).encode()
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as artifact:
            artifact.writestr("ci-provenance.json", provenance)
            artifact.writestr(ARCHIVE_NAME, archive)
        parsed, package = _artifact_contents(output.getvalue(), ARCHIVE_NAME)
        self.assertEqual(parsed["archive_name"], ARCHIVE_NAME)
        self.assertEqual(package, archive)

        bad = io.BytesIO()
        with zipfile.ZipFile(bad, "w") as artifact:
            artifact.writestr("../ci-provenance.json", provenance)
            artifact.writestr(ARCHIVE_NAME, archive)
        with self.assertRaises(ProvenanceError):
            _artifact_contents(bad.getvalue(), ARCHIVE_NAME)

    def test_missing_exact_tree_evidence_fails_closed_without_creating_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "catchuparr"
            root.mkdir()
            (root / "plugin.py").write_text("synthetic plugin\n")
            (root / "plugin.json").write_text('{"version":"0.6.0"}\n')
            (root / "logo.png").write_bytes(b"synthetic logo")
            output = Path(directory) / "dist"

            class API:
                def __init__(self, repository, token):
                    self.repository = repository
                    self.token = token

                def get_json(self, path):
                    if path == "actions/workflows/ci.yml":
                        return {"id": WORKFLOW_ID, "path": WORKFLOW_PATH, "state": "active"}
                    return {"workflow_runs": []}

            with patch("scripts.ci_provenance.GitHubAPI", API), patch(
                "scripts.ci_provenance._release_tree", return_value=(SOURCE_SHA, SOURCE_TREE)
            ):
                with self.assertRaisesRegex(ProvenanceError, "rerun the full pull-request CI"):
                    reuse_ci_artifact(root, output, REPOSITORY, "synthetic-token")
            self.assertFalse(output.exists())

    def test_pr_run_head_may_differ_from_tested_merge_commit(self):
        archive, inventory = _make_archive()
        result = validate_provenance(
            _provenance(archive, inventory), repository=REPOSITORY,
            workflow_id=WORKFLOW_ID, run=_run(), tested_commit=_tested_commit(),
            source_tree=SOURCE_TREE, expected_inventory=inventory,
            archive_name=ARCHIVE_NAME, archive_bytes=archive,
        )
        self.assertNotEqual(_run()["head_sha"], result["source_sha"])

    def test_rejects_tested_merge_with_spoofed_parents(self):
        archive, inventory = _make_archive()
        for parents in ((BASE_SHA, "e" * 40), ("f" * 40, SOURCE_SHA), (SOURCE_SHA,)):
            with self.subTest(parents=parents), self.assertRaises(ProvenanceError):
                validate_provenance(
                    _provenance(archive, inventory), repository=REPOSITORY,
                    workflow_id=WORKFLOW_ID, run=_run(),
                    tested_commit=_tested_commit(parents=parents), source_tree=SOURCE_TREE,
                    expected_inventory=inventory, archive_name=ARCHIVE_NAME,
                    archive_bytes=archive,
                )


if __name__ == "__main__":
    unittest.main()
