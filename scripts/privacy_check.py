"""Block private setup details from repository content and pull request text.

Two kinds of rules apply:

* Structural rules ship with the repository: private network addresses, home
  directory paths, local host names, MAC addresses and e-mail addresses outside
  example domains.
* Personal terms (devices, players, hosts, providers, channels, programmes) are
  never stored in the public repository. They come from the environment
  variable ``CATCHUPARR_PRIVACY_DENYLIST`` (a GitHub secret in CI) or from a
  private file, one term per line; ``re:`` starts a regular expression.

Findings name the location and the rule, never the matched text, so a CI log
does not disclose what it protects.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import re
import subprocess
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path

DENYLIST_ENV = "CATCHUPARR_PRIVACY_DENYLIST"
DEFAULT_DENYLIST_FILE = Path("~/.config/catchuparr/privacy-denylist.txt")

IPV4 = re.compile(r"(?<![\d.])(\d{1,3}(?:\.\d{1,3}){3})(?:/\d{1,2})?(?![\d.]*\d)")
# RFC 1918 private ranges and RFC 6598 shared address space, given as integers
# so this file does not trip its own rule.
PRIVATE_NETWORKS = tuple(ipaddress.ip_network(net) for net in (
    (10 << 24, 8), (172 << 24 | 16 << 16, 12), (192 << 24 | 168 << 16, 16), (100 << 24 | 64 << 16, 10),
))
HOME_PATH = re.compile(r"/home/(?!runner/)[a-z_][a-z0-9_.-]*/|/Users/[A-Za-z0-9._-]+/|[A-Za-z]:\\Users\\")
LOCAL_HOST = re.compile(
    r"\b[a-z0-9][a-z0-9-]*\.(?:fritz\.box|home\.arpa|localdomain|lan|local)\b", re.I,
)
MAC_ADDRESS = re.compile(r"\b[0-9a-f]{2}(?::[0-9a-f]{2}){5}\b", re.I)
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@([A-Za-z0-9.-]+\.[A-Za-z]{2,})")
ALLOWED_EMAIL_DOMAIN = re.compile(
    r"(?:^|\.)(?:example(?:\.[a-z]+)?|invalid|test|localhost|users\.noreply\.github\.com)$", re.I,
)
ALLOWED_EMAILS = {"noreply@anthropic.com"}


def compile_denylist(text: str) -> list[re.Pattern]:
    patterns = []
    for raw in text.splitlines():
        term = raw.strip()
        if not term or term.startswith("#"):
            continue
        if term.startswith("re:"):
            patterns.append(re.compile(term[3:], re.I))
            continue
        body = re.escape(term)
        left = r"(?<![A-Za-z0-9])" if term[0].isalnum() else ""
        right = r"(?![A-Za-z0-9])" if term[-1].isalnum() else ""
        patterns.append(re.compile(left + body + right, re.I))
    return patterns


def load_denylist(path: Path | None) -> list[re.Pattern] | None:
    """Personal terms from the environment or a private file; ``None`` if neither exists."""
    if os.environ.get(DENYLIST_ENV, "").strip():
        return compile_denylist(os.environ[DENYLIST_ENV])
    candidate = (path or DEFAULT_DENYLIST_FILE).expanduser()
    if candidate.is_file():
        return compile_denylist(candidate.read_text(encoding="utf-8"))
    return None


def _private_address(text: str) -> bool:
    try:
        address = ipaddress.ip_address(text)
    except ValueError:
        return False
    return any(address in network for network in PRIVATE_NETWORKS)


def findings(text: str, denylist: list[re.Pattern] | None) -> Iterator[tuple[int, str]]:
    """(line number, rule) for every violation; never the matched text."""
    for number, line in enumerate(text.splitlines(), start=1):
        if any(_private_address(match.group(1)) for match in IPV4.finditer(line)):
            yield number, "private network address"
        if HOME_PATH.search(line):
            yield number, "home directory path"
        if LOCAL_HOST.search(line):
            yield number, "local host name"
        if MAC_ADDRESS.search(line):
            yield number, "MAC address"
        for match in EMAIL.finditer(line):
            if match.group(0).lower() not in ALLOWED_EMAILS and not ALLOWED_EMAIL_DOMAIN.search(match.group(1)):
                yield number, "e-mail address"
        for index, pattern in enumerate(denylist or (), start=1):
            if pattern.search(line):
                yield number, f"private denylist entry {index}"


def _text(data: bytes) -> str | None:
    if b"\0" in data[:8192]:
        return None
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return None


def tracked_files(root: Path) -> Iterable[tuple[str, str]]:
    names = subprocess.run(
        ["git", "ls-files", "-z"], cwd=root, capture_output=True, check=True,
    ).stdout.decode().split("\0")
    for name in filter(None, names):
        path = root / name
        if path.is_file():
            text = _text(path.read_bytes())
            if text is not None:
                yield name, text


def commit_messages(root: Path, revision_range: str) -> Iterable[tuple[str, str]]:
    output = subprocess.run(
        ["git", "log", "--format=%H%x00%B%x01", revision_range], cwd=root,
        capture_output=True, check=True, text=True,
    ).stdout
    for record in filter(str.strip, output.split("\x01")):
        commit, _, message = record.strip("\n").partition("\0")
        yield f"commit {commit[:12]} message", message


def event_texts(path: Path) -> Iterable[tuple[str, str]]:
    event = json.loads(path.read_text(encoding="utf-8"))
    pull = event.get("pull_request") or {}
    for field in ("title", "body"):
        if pull.get(field):
            yield f"pull request {field}", pull[field]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--tree", action="store_true", help="check every tracked text file")
    parser.add_argument("--commits", metavar="RANGE", help="check commit messages in a revision range")
    parser.add_argument("--event", type=Path, help="check a GitHub pull_request event's title and body")
    parser.add_argument("--file", type=Path, action="append", default=[], help="check a text file, repeatable")
    parser.add_argument("--denylist", type=Path, help=f"private term file (default {DEFAULT_DENYLIST_FILE})")
    parser.add_argument("--root", type=Path, default=Path("."))
    args = parser.parse_args(argv)

    denylist = load_denylist(args.denylist)
    if denylist is None:
        print(f"::notice::No private denylist ({DENYLIST_ENV} or file); only structural rules apply.")
    sources: list[tuple[str, str]] = []
    if args.tree:
        sources.extend(tracked_files(args.root))
    if args.commits:
        sources.extend(commit_messages(args.root, args.commits))
    if args.event and args.event.is_file():
        sources.extend(event_texts(args.event))
    for path in args.file:
        sources.append((str(path), path.read_text(encoding="utf-8")))

    problems = [(name, line, rule) for name, text in sources for line, rule in findings(text, denylist)]
    for name, line, rule in problems:
        print(f"{name}:{line}: {rule}")
    print(f"Privacy check: {len(sources)} sources, {len(problems)} findings.")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
