"""Shared Dispatcharr version compatibility policy."""

SUPPORTED_DISPATCHARR_VERSIONS = ("0.32.0",)

# Kept for callers that imported the pre-matrix compatibility constant.
SUPPORTED_DISPATCHARR_VERSION = "0.32.0"


def is_supported_dispatcharr_version(version: str) -> bool:
    """Return whether *version* is an explicitly supported Dispatcharr release."""
    return version in SUPPORTED_DISPATCHARR_VERSIONS
