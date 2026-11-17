"""Find the newest stable Dispatcharr release that CI has not yet checked."""

import argparse
import json
import os
import re
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RELEASES_URL = "https://api.github.com/repos/Dispatcharr/Dispatcharr/releases?per_page=50"
_STABLE_TAG = re.compile(r"^v?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")


def parse_stable(tag: str):
    """Return (major, minor, patch) for a plain stable tag, else None."""
    match = _STABLE_TAG.fullmatch(tag or "")
    return tuple(int(part) for part in match.groups()) if match else None


def newest_stable(releases):
    """Highest published, non-prerelease release as (version tuple, version string)."""
    best = None
    for release in releases:
        if release.get("draft") or release.get("prerelease"):
            continue
        parts = parse_stable(release.get("tag_name", ""))
        if parts is not None and (best is None or parts > best):
            best = parts
    return (best, ".".join(map(str, best))) if best else (None, None)


def highest_tested(compatibility_source: str):
    """Highest version listed in TESTED_DISPATCHARR_VERSIONS."""
    block = re.search(r"TESTED_DISPATCHARR_VERSIONS\s*=\s*\(([^)]*)\)", compatibility_source)
    if block is None:
        raise ValueError("TESTED_DISPATCHARR_VERSIONS not found")
    versions = [parse_stable(v) for v in re.findall(r"[\"']([^\"']+)[\"']", block.group(1))]
    versions = [v for v in versions if v]
    if not versions:
        raise ValueError("TESTED_DISPATCHARR_VERSIONS has no stable version")
    return max(versions)


def fetch_releases():
    request = urllib.request.Request(RELEASES_URL, headers={"Accept": "application/vnd.github+json"})
    token = os.environ.get("GH_TOKEN")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", help="GITHUB_OUTPUT file; prints to stdout when omitted")
    parser.add_argument("--version", default="", help="check this stable version instead")
    args = parser.parse_args()

    tested = highest_tested((ROOT / "catchuparr/compatibility.py").read_text(encoding="utf-8"))
    if args.version:
        parts = parse_stable(args.version)
        if parts is None:
            raise SystemExit(f"Not a stable version: {args.version}")
        version, check = ".".join(map(str, parts)), True
    else:
        parts, version = newest_stable(fetch_releases())
        check = parts is not None and parts > tested
    lines = [f"version={version or ''}", f"check={'true' if check else 'false'}"]
    lines.append(f"tested={'.'.join(map(str, tested))}")
    text = "\n".join(lines) + "\n"
    if args.output:
        with open(args.output, "a", encoding="utf-8") as handle:
            handle.write(text)
    print(text, end="")


if __name__ == "__main__":
    run()
