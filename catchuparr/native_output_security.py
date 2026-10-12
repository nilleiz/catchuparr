"""Private signing-key storage for native output capabilities.

The active settings snapshot stores only a random key identifier. Key material
is kept in a separate mode-0600 file and is staged before an Apply can refer to
it, so restoring an older validated settings snapshot still has its key.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import stat
import tempfile
import uuid
from pathlib import Path
from typing import Any

KEYRING_NAME = ".catchuparr-native-output-keys.json"
KEYRING_VERSION = 1
MAX_KEYRING_BYTES = 65536
MAX_KEYRING_ENTRIES = 512
_KEY_ID = re.compile(r"^[0-9a-f]{32}$")
_HEX_NONCE = re.compile(r"^[0-9a-f]{48}$")
_CAPABILITY_DOMAIN = b"catchuparr-native-playback-v1."
_CAPABILITY_FIELDS = {
    "v", "purpose", "key_id", "mode", "subject", "channel", "profile", "nonce"
}


def native_key_id_valid(value: Any) -> bool:
    return isinstance(value, str) and _KEY_ID.fullmatch(value) is not None


def native_keyring_path(active_path: Path) -> Path:
    return Path(active_path).with_name(KEYRING_NAME)


def ensure_native_key(active_path: Path, *, preferred_id: str | None = None) -> str:
    """Return a usable key ID, creating and durably staging a fresh key if needed."""
    path = native_keyring_path(active_path)
    keys = _read_keyring(path)
    if preferred_id is not None and preferred_id in keys:
        return preferred_id
    if len(keys) >= MAX_KEYRING_ENTRIES:
        raise ValueError("Native output keyring has reached its safe rotation limit")

    key_id = uuid.uuid4().hex
    keys[key_id] = base64.urlsafe_b64encode(os.urandom(32)).decode("ascii")
    _write_keyring(path, keys)
    return key_id


def load_native_key(active_path: Path, key_id: str) -> bytes | None:
    """Read one key only when the keyring is a private, validated regular file."""
    if not native_key_id_valid(key_id):
        return None
    try:
        values = _read_keyring(native_keyring_path(active_path))
        encoded = values.get(key_id)
        if encoded is None:
            return None
        decoded = base64.urlsafe_b64decode(encoded.encode("ascii"))
        return decoded if len(decoded) == 32 else None
    except (OSError, ValueError, TypeError, UnicodeError):
        return None


def create_native_capability(
    active_path: Path,
    *,
    key_id: str,
    mode: str,
    subject: str | int,
    channel: str,
    profile: str | None,
    nonce: str,
) -> str:
    """Sign one channel-scoped native playback capability."""
    subject_text = str(subject)
    if (
        mode not in {"anonymous_admission", "trusted_native_user"}
        or not _positive_decimal(subject_text)
        or not native_key_id_valid(key_id)
        or not _HEX_NONCE.fullmatch(nonce)
        or (profile is not None and not _positive_decimal(profile))
    ):
        raise ValueError("Native output capability claims are invalid")
    try:
        canonical_channel = str(uuid.UUID(channel))
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Native output capability channel is invalid") from None
    if canonical_channel != channel:
        raise ValueError("Native output capability channel is invalid")
    payload = {
        "v": 1,
        "purpose": "native-playback",
        "key_id": key_id,
        "mode": mode,
        "subject": subject_text,
        "channel": channel,
        "profile": profile,
        "nonce": nonce,
    }
    encoded = _encode_payload(payload)
    key = load_native_key(active_path, key_id)
    if key is None:
        raise ValueError("Native output signing key is unavailable")
    signature = hmac.new(key, _CAPABILITY_DOMAIN + encoded.encode("ascii"), hashlib.sha256).digest()
    return f"{encoded}.{_b64encode(signature)}"


def verify_native_capability(
    active_path: Path, token: str, *, current_key_id: str
) -> dict[str, Any] | None:
    """Verify a bounded native-only token against the currently active key."""
    if (
        not isinstance(token, str)
        or len(token) > 4096
        or token.count(".") != 1
        or not native_key_id_valid(current_key_id)
    ):
        return None
    encoded, supplied_signature = token.split(".", 1)
    if not encoded or not re.fullmatch(r"[A-Za-z0-9_-]+", encoded):
        return None
    key = load_native_key(active_path, current_key_id)
    if key is None:
        return None
    expected_signature = hmac.new(
        key, _CAPABILITY_DOMAIN + encoded.encode("ascii"), hashlib.sha256
    ).digest()
    try:
        signature = _b64decode(supplied_signature)
        payload = json.loads(_b64decode(encoded).decode("utf-8"))
    except (ValueError, UnicodeError, json.JSONDecodeError):
        return None
    if not hmac.compare_digest(signature, expected_signature):
        return None
    if not isinstance(payload, dict) or set(payload) != _CAPABILITY_FIELDS:
        return None
    if (
        payload.get("v") != 1
        or payload.get("purpose") != "native-playback"
        or payload.get("key_id") != current_key_id
        or payload.get("mode") not in {"anonymous_admission", "trusted_native_user"}
        or not _positive_decimal(payload.get("subject"))
        or not _HEX_NONCE.fullmatch(str(payload.get("nonce", "")))
        or (payload.get("profile") is not None and (
            not _positive_decimal(payload["profile"])
        ))
    ):
        return None
    try:
        if str(uuid.UUID(payload.get("channel", ""))) != payload.get("channel"):
            return None
    except (ValueError, TypeError, AttributeError):
        return None
    return payload


def native_device_identity(key: bytes, payload: dict[str, Any], client_ip: str, user_agent: str) -> str:
    """Derive a stable opaque per-export/client session identity."""
    if not isinstance(key, bytes) or len(key) != 32:
        raise ValueError("Native output signing key is invalid")
    bounded_ip = str(client_ip)[:64]
    bounded_agent = str(user_agent)[:256]
    message = "\0".join(
        ("native-device-v1", payload["nonce"], payload["subject"], bounded_ip, bounded_agent)
    ).encode("utf-8")
    return "native:" + hmac.new(key, message, hashlib.sha256).hexdigest()


def _encode_payload(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return _b64encode(raw)


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64decode(value: str) -> bytes:
    if not value or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("Invalid base64url value")
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _positive_decimal(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) <= 20
        and re.fullmatch(r"[1-9][0-9]*", value) is not None
    )


def _read_keyring(path: Path) -> dict[str, str]:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return {}
    if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise ValueError("Native output keyring is not a private regular file")
    if hasattr(os, "getuid") and metadata.st_uid != os.getuid():
        raise ValueError("Native output keyring has an unexpected owner")
    if metadata.st_size > MAX_KEYRING_BYTES:
        raise ValueError("Native output keyring is too large")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or set(raw) != {"version", "keys"}:
        raise ValueError("Native output keyring is invalid")
    keys = raw["keys"]
    if raw["version"] != KEYRING_VERSION or not isinstance(keys, dict):
        raise ValueError("Native output keyring is invalid")
    if len(keys) > MAX_KEYRING_ENTRIES:
        raise ValueError("Native output keyring has too many keys")
    validated: dict[str, str] = {}
    for key_id, encoded in keys.items():
        if not native_key_id_valid(key_id) or not isinstance(encoded, str):
            raise ValueError("Native output keyring entry is invalid")
        try:
            decoded = base64.urlsafe_b64decode(encoded.encode("ascii"))
        except (ValueError, UnicodeError):
            raise ValueError("Native output keyring entry is invalid") from None
        if len(decoded) != 32:
            raise ValueError("Native output keyring entry is invalid")
        validated[key_id] = encoded
    return validated


def _write_keyring(path: Path, keys: dict[str, str]) -> None:
    document = json.dumps(
        {"version": KEYRING_VERSION, "keys": keys},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    descriptor, temporary = tempfile.mkstemp(prefix=f"{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(document)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except Exception:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
