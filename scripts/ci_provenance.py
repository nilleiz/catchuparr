"""Create and verify provenance for a reusable CI plugin artifact."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath

WORKFLOW_PATH = ".github/workflows/ci.yml"
ARTIFACT_NAME = "catchuparr-plugin"
SCHEMA_VERSION = 1
REQUIRED_AIO_JOBS = {"AIO integration (0.33.0)"}
REQUIRED_JOBS = {"test-and-package", *REQUIRED_AIO_JOBS}
REQUIRED_AIO_STEP = "Synthetic playback in actual Dispatcharr AIO"
PACKAGE_SUFFIXES = {".py", ".json", ".png"}
API_ROOT = "https://api.github.com"
INFRA_CHANGE_FILES = {
    "scripts/ci_provenance.py",
    "tests/test_ci_provenance.py",
    "README.md",
    "CHANGELOG.md",
}


class ProvenanceError(Exception):
    """A safe, concise error suitable for CI logs."""


def requires_aio(paths: list[str] | None) -> bool:
    """Fail closed unless every changed path is an explicitly cheap input."""
    if not paths:
        return True
    for path in paths:
        if path in INFRA_CHANGE_FILES:
            continue
        if path.startswith("docs/") and path.endswith(".md") and ".." not in path.split("/"):
            continue
        if path.startswith(".github/workflows/") and path.endswith(".yml") and ".." not in path.split("/"):
            continue
        return True
    return False


def changed_paths(event: str, base_sha: str, head_sha: str) -> list[str] | None:
    """Return changed names, or None when the comparison cannot be trusted."""
    if event != "pull_request" or not re.fullmatch(r"[0-9a-fA-F]{40,64}", base_sha) or not re.fullmatch(
        r"[0-9a-fA-F]{40,64}", head_sha
    ):
        return None
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", "--no-renames", "-z", base_sha, head_sha],
            check=True,
            capture_output=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    try:
        return [item.decode("utf-8") for item in result.stdout.split(b"\0") if item]
    except UnicodeDecodeError:
        return None


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def package_inventory(package_root: Path) -> dict[str, str]:
    """Hash exactly the files included by scripts/build_plugin.py."""
    root = package_root.resolve()
    if not root.is_dir() or root.name != "catchuparr":
        raise ProvenanceError("Package root must be the catchuparr directory")
    result: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix not in PACKAGE_SUFFIXES:
            continue
        resolved = path.resolve()
        if root not in resolved.parents:
            raise ProvenanceError("Package source contains a path outside its root")
        name = PurePosixPath("catchuparr", *path.relative_to(root).parts).as_posix()
        result[name] = _sha256(path.read_bytes())
    if not result:
        raise ProvenanceError("Package source inventory is empty")
    return result


def _compare_archive_to_source(
    archive_bytes: bytes, package_root: Path, expected_inventory: dict[str, str],
) -> None:
    try:
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
            names = archive.namelist()
            if len(names) != len(set(names)) or set(names) != set(expected_inventory):
                raise ProvenanceError("Plugin archive file inventory differs from the release source")
            for name in names:
                relative = PurePosixPath(name).relative_to("catchuparr")
                source_path = package_root.joinpath(*relative.parts)
                if archive.read(name) != source_path.read_bytes():
                    raise ProvenanceError("Plugin archive bytes differ from the release source")
    except (zipfile.BadZipFile, KeyError, OSError, ValueError) as exc:
        raise ProvenanceError("Plugin archive could not be compared with the release source") from exc


def _tree_sha() -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD^{tree}"],
            check=True,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProvenanceError("Unable to identify the checked out source tree") from exc
    value = proc.stdout.strip().lower()
    if not re.fullmatch(r"[0-9a-f]{40,64}", value):
        raise ProvenanceError("Git returned an invalid source tree identity")
    return value


def create_provenance(
    *,
    package_root: Path,
    archive_path: Path,
    repository: str,
    event: str,
    workflow_ref: str,
    source_sha: str,
    run_id: str,
    run_attempt: str,
) -> dict:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ProvenanceError("Invalid repository identity")
    if not re.fullmatch(r"[0-9a-fA-F]{40,64}", source_sha):
        raise ProvenanceError("Invalid source commit identity")
    if not run_id.isdecimal() or not run_attempt.isdecimal():
        raise ProvenanceError("Invalid workflow run identity")
    expected_ref_prefix = f"{repository}/{WORKFLOW_PATH}@"
    if not workflow_ref.startswith(expected_ref_prefix):
        raise ProvenanceError("Workflow ref does not identify the trusted CI workflow")
    if event not in {"pull_request", "workflow_dispatch"}:
        raise ProvenanceError("Unsupported CI event")
    if not archive_path.is_file():
        raise ProvenanceError("CI package archive is missing")

    try:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"], check=True, capture_output=True,
            text=True, timeout=20,
        ).stdout.strip().lower()
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProvenanceError("Unable to identify the checked out source commit") from exc
    if head != source_sha.lower():
        raise ProvenanceError("Workflow source commit differs from the checkout")

    inventory = package_inventory(package_root)
    archive_bytes = archive_path.read_bytes()
    _compare_archive_to_source(archive_bytes, package_root, inventory)
    if _archive_inventory(archive_bytes) != inventory:
        raise ProvenanceError("Built package checksum inventory differs from source")
    return {
        "schema_version": SCHEMA_VERSION,
        "repository": repository,
        "event": event,
        "workflow_path": WORKFLOW_PATH,
        "workflow_ref": workflow_ref,
        "run_id": int(run_id),
        "run_attempt": int(run_attempt),
        "source_sha": source_sha.lower(),
        "source_tree": _tree_sha(),
        "artifact_name": ARTIFACT_NAME,
        "archive_name": archive_path.name,
        "archive_sha256": _sha256(archive_bytes),
        "package_inventory": inventory,
    }


def validate_provenance(
    provenance: object,
    *,
    repository: str,
    workflow_id: int,
    run: dict,
    tested_commit: dict,
    source_tree: str,
    expected_inventory: dict[str, str],
    archive_name: str,
    archive_bytes: bytes,
) -> dict:
    """Reject incomplete, mismatched, failed, or spoofed CI evidence."""
    if not isinstance(provenance, dict):
        raise ProvenanceError("CI provenance is not an object")
    required = {
        "schema_version", "repository", "event", "workflow_path", "workflow_ref",
        "run_id", "run_attempt", "source_sha", "source_tree", "artifact_name",
        "archive_name", "archive_sha256", "package_inventory",
    }
    if set(provenance) != required or provenance.get("schema_version") != SCHEMA_VERSION:
        raise ProvenanceError("CI provenance schema is invalid")
    if not _trusted_run(run, repository, workflow_id):
        raise ProvenanceError("Workflow run is not trusted for release reuse")
    if any(job.get("conclusion") != "success" for job in _required_job_records(run)):
        raise ProvenanceError("Required CI jobs did not all succeed")

    run_id = run.get("id")
    attempt = run.get("run_attempt")
    source_sha = str(provenance.get("source_sha", "")).lower()
    pull_requests = run.get("pull_requests")
    pr = pull_requests[0] if isinstance(pull_requests, list) and len(pull_requests) == 1 else None
    head = pr.get("head") if isinstance(pr, dict) else None
    base = pr.get("base") if isinstance(pr, dict) else None
    head = head if isinstance(head, dict) else {}
    base = base if isinstance(base, dict) else {}
    head_repo = head.get("repo")
    base_repo = base.get("repo")
    trusted_head_repo = run.get("head_repository")
    trusted_base_repo = run.get("repository")
    parents = tested_commit.get("parents") if isinstance(tested_commit, dict) else None
    tested_tree = tested_commit.get("tree") if isinstance(tested_commit, dict) else None
    tested_tree_sha = tested_tree.get("sha") if isinstance(tested_tree, dict) else None
    pr_number = pr.get("number") if isinstance(pr, dict) else None
    valid_merge = (
        isinstance(tested_commit, dict)
        and isinstance(tested_commit.get("sha"), str)
        and tested_commit["sha"].lower() == source_sha
        and isinstance(parents, list) and len(parents) == 2
        and all(isinstance(item, dict) and isinstance(item.get("sha"), str) for item in parents)
        and {item["sha"].lower() for item in parents}
        == {str(head.get("sha", "")).lower(), str(base.get("sha", "")).lower()}
        and len({str(head.get("sha", "")).lower(), str(base.get("sha", "")).lower()}) == 2
        and isinstance(tested_tree_sha, str) and tested_tree_sha.lower() == source_tree.lower()
        and type(pr_number) is int and pr_number > 0
        and isinstance(run.get("head_sha"), str)
        and run["head_sha"].lower() == str(head.get("sha", "")).lower()
        and _same_repository(head_repo, trusted_head_repo)
        and _same_repository(base_repo, trusted_base_repo)
    )
    ref_prefix = f"{repository}/{WORKFLOW_PATH}@refs/pull/"
    if (
        not isinstance(provenance.get("repository"), str)
        or provenance["repository"].casefold() != repository.casefold()
        or provenance.get("event") != "pull_request"
        or provenance.get("workflow_path") != WORKFLOW_PATH
        or not isinstance(provenance.get("workflow_ref"), str)
        or not provenance["workflow_ref"].startswith(ref_prefix)
        or provenance["workflow_ref"] != f"{repository}/{WORKFLOW_PATH}@refs/pull/{pr_number}/merge"
        or type(provenance.get("run_id")) is not int
        or provenance["run_id"] != run_id
        or type(provenance.get("run_attempt")) is not int
        or provenance["run_attempt"] != attempt
        or not valid_merge
        or provenance.get("source_tree") != source_tree
        or provenance.get("artifact_name") != ARTIFACT_NAME
        or provenance.get("archive_name") != archive_name
        or provenance.get("archive_sha256") != _sha256(archive_bytes)
        or provenance.get("package_inventory") != expected_inventory
    ):
        raise ProvenanceError("CI provenance does not match the release source or artifact")
    return provenance


def _required_job_records(run: dict) -> list[dict]:
    jobs = run.get("jobs")
    if not isinstance(jobs, list):
        return []
    by_name = {job.get("name"): job for job in jobs if isinstance(job, dict)}
    return [by_name[name] for name in REQUIRED_JOBS if name in by_name]


def _native_aio_step_succeeded(jobs: list[dict]) -> bool:
    by_name = {job.get("name"): job for job in jobs if isinstance(job, dict)}
    for name in REQUIRED_AIO_JOBS:
        steps = by_name.get(name, {}).get("steps")
        if not isinstance(steps, list) or not any(
            isinstance(step, dict)
            and step.get("name") == REQUIRED_AIO_STEP
            and step.get("conclusion") == "success"
            for step in steps
        ):
            return False
    return True


def _trusted_run(run: dict, repository: str, workflow_id: int) -> bool:
    if not isinstance(run, dict):
        return False
    repo = run.get("repository")
    head_repo = run.get("head_repository")
    if not isinstance(repo, dict) or not isinstance(head_repo, dict):
        return False
    names = (repo.get("full_name"), head_repo.get("full_name"))
    jobs_value = run.get("jobs")
    jobs = _required_job_records(run) if jobs_value is not None else None
    return (
        type(run.get("id")) is int
        and type(run.get("run_attempt")) is int
        and type(run.get("workflow_id")) is int
        and run.get("workflow_id") == workflow_id
        and run.get("event") == "pull_request"
        and run.get("status") == "completed"
        and run.get("conclusion") == "success"
        and isinstance(run.get("head_sha"), str)
        and all(isinstance(name, str) and name.casefold() == repository.casefold() for name in names)
        and (
            jobs is None
            or (
                len(jobs) == len(REQUIRED_JOBS)
                and all(job.get("conclusion") == "success" for job in jobs)
                and _native_aio_step_succeeded(jobs)
            )
        )
    )


def _same_repository(candidate: object, trusted: object) -> bool:
    """Bind minimal PR repo objects to repositories on the trusted run."""
    if not isinstance(candidate, dict) or not isinstance(trusted, dict):
        return False
    candidate_id = candidate.get("id")
    trusted_id = trusted.get("id")
    ids_match = type(candidate_id) is int and type(trusted_id) is int and candidate_id == trusted_id
    candidate_url = candidate.get("url") or candidate.get("html_url")
    trusted_url = trusted.get("url") or trusted.get("html_url")
    urls_match = (
        isinstance(candidate_url, str) and isinstance(trusted_url, str)
        and candidate_url.rstrip("/").casefold() == trusted_url.rstrip("/").casefold()
    )
    candidate_full_name = candidate.get("full_name")
    trusted_full_name = trusted.get("full_name")
    full_names_match = (
        isinstance(candidate_full_name, str) and isinstance(trusted_full_name, str)
        and candidate_full_name.casefold() == trusted_full_name.casefold()
    )
    candidate_name = candidate.get("name")
    trusted_name = trusted.get("name")
    names_match = (
        not isinstance(candidate_name, str) or not isinstance(trusted_name, str)
        or candidate_name.casefold() == trusted_name.casefold()
    )
    return (ids_match or urls_match or full_names_match) and names_match


def _bind_run_to_merged_pr(
    api: GitHubAPI, run: dict, provenance: dict, release_tree: str,
) -> None:
    """Recover PR metadata omitted from merged workflow-run records and bind it."""
    workflow_ref = provenance.get("workflow_ref")
    prefix = f"{api.repository}/{WORKFLOW_PATH}@refs/pull/"
    match = re.fullmatch(re.escape(prefix) + r"([1-9][0-9]*)/merge", workflow_ref or "")
    if match is None:
        raise ProvenanceError("CI provenance does not identify a pull request")
    number = int(match.group(1))
    pr = api.get_json(f"pulls/{number}")
    head = pr.get("head") if isinstance(pr.get("head"), dict) else {}
    base = pr.get("base") if isinstance(pr.get("base"), dict) else {}
    if (
        pr.get("number") != number
        or pr.get("state") != "closed"
        or pr.get("merged") is not True
        or not isinstance(head.get("sha"), str)
        or head["sha"].lower() != str(run.get("head_sha", "")).lower()
        or not _same_repository(head.get("repo"), run.get("head_repository"))
        or not _same_repository(base.get("repo"), run.get("repository"))
    ):
        raise ProvenanceError("Pull request identity does not match the trusted CI run")
    merge_sha = pr.get("merge_commit_sha")
    if not isinstance(merge_sha, str) or not re.fullmatch(r"[0-9a-fA-F]{40,64}", merge_sha):
        raise ProvenanceError("Merged pull request commit identity is missing")
    merged_commit = _run_commit(api, merge_sha)
    merged_tree = merged_commit.get("tree")
    merged_tree_sha = merged_tree.get("sha") if isinstance(merged_tree, dict) else None
    if not isinstance(merged_tree_sha, str) or merged_tree_sha.lower() != release_tree.lower():
        raise ProvenanceError("Merged pull request tree differs from the release tree")
    run["pull_requests"] = [{"number": number, "head": head, "base": base}]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        return None


class GitHubAPI:
    def __init__(self, repository: str, token: str):
        self.repository = repository
        self.token = token

    def get_json(self, path: str) -> dict:
        request = urllib.request.Request(
            f"{API_ROOT}/repos/{self.repository}/{path.lstrip('/')}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                value = json.loads(response.read())
        except (OSError, urllib.error.URLError, urllib.error.HTTPError, ValueError) as exc:
            raise ProvenanceError("GitHub API request failed") from exc
        if not isinstance(value, dict):
            raise ProvenanceError("GitHub API returned an unexpected response")
        return value

    def get_bytes(self, path: str) -> bytes:
        request = urllib.request.Request(
            f"{API_ROOT}/repos/{self.repository}/{path.lstrip('/')}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        opener = urllib.request.build_opener(_NoRedirect)
        try:
            with opener.open(request, timeout=30) as response:
                data = response.read()
        except urllib.error.HTTPError as exc:
            if exc.code not in {301, 302, 303, 307, 308}:
                raise ProvenanceError("GitHub artifact download failed") from exc
            location = exc.headers.get("Location")
            parsed = urllib.parse.urlparse(location or "")
            if parsed.scheme != "https" or not parsed.netloc:
                raise ProvenanceError("GitHub returned an invalid artifact redirect") from exc
            try:
                # The signed URL is sufficient; do not forward the API token to storage.
                with urllib.request.urlopen(urllib.request.Request(location), timeout=60) as response:
                    data = response.read()
            except (OSError, urllib.error.URLError, urllib.error.HTTPError) as redirect_exc:
                raise ProvenanceError("GitHub artifact download failed") from redirect_exc
        except (OSError, urllib.error.URLError) as exc:
            raise ProvenanceError("GitHub artifact download failed") from exc
        if not data:
            raise ProvenanceError("GitHub returned an empty artifact")
        return data


def _run_commit(api: GitHubAPI, sha: str) -> dict:
    commit = api.get_json(f"git/commits/{urllib.parse.quote(sha, safe='')}")
    tree = commit.get("tree")
    value = tree.get("sha") if isinstance(tree, dict) else None
    if not isinstance(commit.get("sha"), str) or commit["sha"].lower() != sha.lower() or not isinstance(value, str):
        raise ProvenanceError("GitHub commit identity did not verify")
    return commit


def _artifact_contents(artifact_zip: bytes, archive_name: str) -> tuple[dict, bytes]:
    try:
        with zipfile.ZipFile(io.BytesIO(artifact_zip)) as archive:
            names = archive.namelist()
            if len(names) != 2 or len(names) != len(set(names)):
                raise ProvenanceError("CI artifact must contain only the package and provenance")
            for name in names:
                path = PurePosixPath(name)
                if path.is_absolute() or ".." in path.parts or "\\" in name:
                    raise ProvenanceError("CI artifact contains an unsafe path")
            if set(names) != {"ci-provenance.json", archive_name}:
                raise ProvenanceError("CI artifact contents are incomplete")
            provenance = json.loads(archive.read("ci-provenance.json"))
            package = archive.read(archive_name)
    except (zipfile.BadZipFile, KeyError, json.JSONDecodeError) as exc:
        raise ProvenanceError("CI artifact archive is invalid") from exc
    if not isinstance(provenance, dict):
        raise ProvenanceError("CI provenance is invalid")
    return provenance, package


def _artifact_provenance(artifact_zip: bytes) -> dict:
    """Read provenance before choosing the versioned package member."""
    try:
        with zipfile.ZipFile(io.BytesIO(artifact_zip)) as archive:
            names = archive.namelist()
            if len(names) != 2 or len(names) != len(set(names)) or "ci-provenance.json" not in names:
                raise ProvenanceError("CI artifact contents are incomplete")
            for name in names:
                path = PurePosixPath(name)
                if path.is_absolute() or ".." in path.parts or "\\" in name:
                    raise ProvenanceError("CI artifact contains an unsafe path")
            value = json.loads(archive.read("ci-provenance.json"))
    except (zipfile.BadZipFile, KeyError, json.JSONDecodeError) as exc:
        raise ProvenanceError("CI artifact archive is invalid") from exc
    if not isinstance(value, dict):
        raise ProvenanceError("CI provenance is invalid")
    return value


def _archive_inventory(archive_bytes: bytes) -> dict[str, str]:
    try:
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
            names = archive.namelist()
            if len(names) != len(set(names)):
                raise ProvenanceError("Plugin archive contains duplicate paths")
            inventory: dict[str, str] = {}
            for name in names:
                path = PurePosixPath(name)
                if path.is_absolute() or ".." in path.parts or "\\" in name:
                    raise ProvenanceError("Plugin archive contains an unsafe path")
                if not name.startswith("catchuparr/") or Path(name).suffix not in PACKAGE_SUFFIXES:
                    raise ProvenanceError("Plugin archive contains an unexpected file")
                inventory[name] = _sha256(archive.read(name))
    except (zipfile.BadZipFile, KeyError) as exc:
        raise ProvenanceError("Plugin archive is invalid") from exc
    return inventory


def _release_tree() -> tuple[str, str]:
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], check=True, capture_output=True,
            text=True, timeout=20,
        ).stdout.strip().lower()
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProvenanceError("Unable to identify the release checkout") from exc
    return commit, _tree_sha()


def reuse_ci_artifact(package_root: Path, output_dir: Path, repository: str, token: str) -> dict:
    if not token:
        raise ProvenanceError("GitHub token is required to reuse CI evidence")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ProvenanceError("Invalid repository identity")
    source_inventory = package_inventory(package_root)
    manifest = json.loads((package_root / "plugin.json").read_text(encoding="utf-8"))
    version = manifest.get("version")
    if not isinstance(version, str) or not re.fullmatch(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", version):
        raise ProvenanceError("Release package version is invalid")
    archive_name = f"catchuparr-{version}.zip"
    release_sha, release_tree = _release_tree()
    api = GitHubAPI(repository, token)
    workflow = api.get_json("actions/workflows/ci.yml")
    workflow_id = workflow.get("id")
    if (
        type(workflow_id) is not int
        or workflow.get("path") != WORKFLOW_PATH
        or workflow.get("state") != "active"
    ):
        raise ProvenanceError("Trusted CI workflow identity did not verify")

    valid_evidence = None
    for page in range(1, 51):
        query = urllib.parse.urlencode({
            "event": "pull_request", "status": "completed",
            "per_page": 100, "page": page,
        })
        data = api.get_json(f"actions/workflows/{workflow_id}/runs?{query}")
        runs = data.get("workflow_runs")
        if not isinstance(runs, list):
            raise ProvenanceError("GitHub workflow run list is invalid")
        if not runs:
            break
        for run in runs:
            if not _trusted_run(run, repository, workflow_id):
                continue
            jobs_data = api.get_json(f"actions/runs/{run['id']}/jobs?per_page=100")
            run["jobs"] = jobs_data.get("jobs")
            if not _trusted_run(run, repository, workflow_id):
                continue
            artifacts = api.get_json(f"actions/runs/{run['id']}/artifacts?per_page=100").get("artifacts")
            if not isinstance(artifacts, list):
                continue
            artifact = next((item for item in artifacts if isinstance(item, dict)
                             and item.get("name") == ARTIFACT_NAME
                             and item.get("expired") is False), None)
            if artifact is None or type(artifact.get("id")) is not int:
                continue
            try:
                data_zip = api.get_bytes(f"actions/artifacts/{artifact['id']}/zip")
                candidate_provenance = _artifact_provenance(data_zip)
                if candidate_provenance.get("archive_name") != archive_name:
                    continue
                provenance, package_bytes = _artifact_contents(data_zip, archive_name)
                _bind_run_to_merged_pr(api, run, provenance, release_tree)
                tested_commit = _run_commit(api, str(provenance.get("source_sha", "")))
                tree_obj = tested_commit.get("tree")
                tree = tree_obj.get("sha", "").lower() if isinstance(tree_obj, dict) else ""
                if tree != release_tree:
                    continue
                validate_provenance(
                    provenance, repository=repository, workflow_id=workflow_id,
                    run=run, tested_commit=tested_commit, source_tree=tree,
                    expected_inventory=source_inventory,
                    archive_name=archive_name, archive_bytes=package_bytes,
                )
                _compare_archive_to_source(package_bytes, package_root, source_inventory)
                if _archive_inventory(package_bytes) != source_inventory:
                    continue
            except ProvenanceError:
                continue
            valid_evidence = (run, artifact, provenance, package_bytes)
            break
        if valid_evidence is not None:
            break
        if len(runs) < 100:
            break

    if valid_evidence is None:
        raise ProvenanceError(
            "No successful same-repository PR CI artifact proves this exact release tree; "
            "rerun the full pull-request CI workflow for this tree, then retry the release."
        )
    run, artifact, provenance, package_bytes = valid_evidence
    jobs_by_name = {
        job["name"]: job["conclusion"]
        for job in run["jobs"]
        if isinstance(job, dict) and isinstance(job.get("name"), str)
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_path = output_dir / archive_name
    archive_path.write_bytes(package_bytes)
    evidence_path = output_dir / "ci-provenance.json"
    evidence_path.write_text(json.dumps(provenance, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return {
        "run_id": run["id"], "run_attempt": run["run_attempt"],
        "run_url": run.get("html_url"), "source_sha": provenance["source_sha"],
        "workflow_id": workflow_id,
        "required_jobs": [
            {"name": name, "conclusion": jobs_by_name[name]}
            for name in sorted(REQUIRED_JOBS)
        ],
        "release_sha": release_sha, "source_tree": release_tree,
        "archive_name": archive_name,
        "archive_sha256": provenance["archive_sha256"],
        "artifact_id": artifact["id"],
    }


def _record(args: argparse.Namespace) -> None:
    required_env = (
        "GITHUB_REPOSITORY", "GITHUB_EVENT_NAME", "GITHUB_WORKFLOW_REF",
        "GITHUB_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT",
    )
    missing = [key for key in required_env if not os.environ.get(key)]
    if missing:
        raise ProvenanceError("Required GitHub workflow identity is missing")
    manifest = json.loads((args.package_root / "plugin.json").read_text(encoding="utf-8"))
    version = manifest.get("version")
    if not isinstance(version, str) or not re.fullmatch(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", version):
        raise ProvenanceError("Package manifest version is invalid")
    archive_path = args.dist_dir / f"catchuparr-{version}.zip"
    data = create_provenance(
        package_root=args.package_root,
        archive_path=archive_path,
        repository=os.environ["GITHUB_REPOSITORY"],
        event=os.environ["GITHUB_EVENT_NAME"],
        workflow_ref=os.environ["GITHUB_WORKFLOW_REF"],
        source_sha=os.environ["GITHUB_SHA"],
        run_id=os.environ["GITHUB_RUN_ID"],
        run_attempt=os.environ["GITHUB_RUN_ATTEMPT"],
    )
    args.output.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"event": data["event"], "source_sha": data["source_sha"],
                      "source_tree": data["source_tree"], "archive_sha256": data["archive_sha256"]}, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    classify = subparsers.add_parser("classify")
    classify.add_argument("--event", required=True)
    classify.add_argument("--base-sha", default="")
    classify.add_argument("--head-sha", default="")
    classify.add_argument("--output", default="")
    record = subparsers.add_parser("record")
    record.add_argument("--package-root", type=Path, required=True)
    record.add_argument("--dist-dir", type=Path, required=True)
    record.add_argument("--output", type=Path, required=True)
    reuse = subparsers.add_parser("reuse")
    reuse.add_argument("--package-root", type=Path, required=True)
    reuse.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "classify":
            paths = changed_paths(args.event, args.base_sha, args.head_sha)
            full = args.event != "pull_request" or requires_aio(paths)
            value = "true" if full else "false"
            if args.output:
                with open(args.output, "a", encoding="utf-8") as output:
                    output.write(f"full={value}\n")
            print(f"Native AIO required: {value}")
            if paths is None:
                print("Changed-file comparison unavailable; running full AIO.")
            elif full:
                print("At least one changed path requires full native acceptance.")
            else:
                print("All changed paths are on the infrastructure-only allowlist.")
        elif args.command == "record":
            _record(args)
        else:
            repository = os.environ.get("GITHUB_REPOSITORY", "")
            result = reuse_ci_artifact(
                args.package_root, args.output_dir, repository,
                os.environ.get("GH_TOKEN", ""),
            )
            print(json.dumps(result, sort_keys=True))
    except ProvenanceError as exc:
        print(f"CI provenance error: {exc}", file=sys.stderr)
        return 1
    except (OSError, json.JSONDecodeError) as exc:
        print(f"CI provenance error: {type(exc).__name__}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
