"""Shared Dispatcharr version compatibility policy."""

SUPPORTED_DISPATCHARR_VERSIONS = ("0.31.0", "0.32.0")

# Kept for callers that imported the pre-matrix compatibility constant.
SUPPORTED_DISPATCHARR_VERSION = SUPPORTED_DISPATCHARR_VERSIONS[0]


def is_supported_dispatcharr_version(version: str) -> bool:
    """Return whether *version* is one of the explicitly inspected releases."""
    return version in SUPPORTED_DISPATCHARR_VERSIONS
