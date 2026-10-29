"""Shared Dispatcharr minimum-version compatibility policy."""

import re

MINIMUM_DISPATCHARR_VERSION = "0.33.0"
TESTED_DISPATCHARR_VERSIONS = ("0.33.0",)
_MINIMUM_PARTS = tuple(int(part) for part in MINIMUM_DISPATCHARR_VERSION.split("."))

# Compatibility aliases for callers that imported the earlier constants.
# These identify the tested baseline, not an exact allowlist.
SUPPORTED_DISPATCHARR_VERSIONS = TESTED_DISPATCHARR_VERSIONS
SUPPORTED_DISPATCHARR_VERSION = "0.33.0"

_VERSION = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-([0-9A-Za-z.-]+))?(?:\+([0-9A-Za-z.-]+))?$"
)


def is_supported_dispatcharr_version(version: str) -> bool:
    """Accept well-formed SemVer versions at or above the stable minimum."""
    if not isinstance(version, str):
        return False
    match = _VERSION.fullmatch(version)
    if match is None:
        return False
    major, minor, patch = (int(part) for part in match.group(1, 2, 3))
    prerelease = match.group(4)
    build = match.group(5)
    if not _valid_identifiers(prerelease, numeric_leading_zero=False):
        return False
    if not _valid_identifiers(build, numeric_leading_zero=True):
        return False
    current = (major, minor, patch)
    if current < _MINIMUM_PARTS:
        return False
    # A prerelease of the minimum itself sorts before its stable release.
    return current != _MINIMUM_PARTS or prerelease is None


def _valid_identifiers(value: str | None, *, numeric_leading_zero: bool) -> bool:
    if value is None:
        return True
    identifiers = value.split(".")
    return all(
        identifier
        and re.fullmatch(r"[0-9A-Za-z-]+", identifier)
        and (
            numeric_leading_zero
            or not identifier.isdigit()
            or identifier == "0"
            or not identifier.startswith("0")
        )
        for identifier in identifiers
    )
