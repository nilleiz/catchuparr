"""Extract one version's section of CHANGELOG.md as its release notes."""

from __future__ import annotations

import sys
from pathlib import Path


def release_notes(changelog: str, version: str) -> str:
    """Return the body of ``## [version]`` up to the next version heading."""
    lines = changelog.splitlines()
    heading = f"## [{version}]"
    start = next((index for index, line in enumerate(lines) if line.startswith(heading)), None)
    if start is None:
        raise ValueError(f"No {heading} section in CHANGELOG.md")
    end = next(
        (index for index in range(start + 1, len(lines)) if lines[index].startswith("## [")),
        len(lines),
    )
    body = "\n".join(lines[start + 1:end]).strip()
    if not body:
        raise ValueError(f"The {heading} section in CHANGELOG.md is empty")
    return body + "\n"


if __name__ == "__main__":
    changelog_path, version, output_path = sys.argv[1:4]
    Path(output_path).write_text(release_notes(Path(changelog_path).read_text(), version))
