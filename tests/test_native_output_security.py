import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from catchuparr.native_output_security import (
    create_native_capability,
    ensure_native_key,
    load_native_key,
    native_device_identity,
    verify_native_capability,
)

CHANNEL = "00000000-0000-0000-0000-000000000001"
NONCE = "0123456789abcdef" * 3


class NativeOutputSecurityTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.active_path = self.root / ".catchuparr-active-settings.json"
        self.key_id = ensure_native_key(self.active_path)

    def tearDown(self):
        self.directory.cleanup()

    def capability(self, **overrides):
        claims = {
            "key_id": self.key_id,
            "mode": "anonymous_admission",
            "subject": "123",
            "channel": CHANNEL,
            "profile": "7",
            "nonce": NONCE,
        }
        claims.update(overrides)
        return create_native_capability(self.active_path, **claims)

    def test_capability_verifies_only_for_current_key_and_exact_claims(self):
        token = self.capability()
        verified = verify_native_capability(
            self.active_path, token, current_key_id=self.key_id
        )
        self.assertEqual("native-playback", verified["purpose"])
        self.assertEqual(CHANNEL, verified["channel"])
        self.assertEqual("7", verified["profile"])
        other_key = ensure_native_key(self.active_path)
        self.assertIsNone(
            verify_native_capability(self.active_path, token, current_key_id=other_key)
        )

    def test_tampering_wrong_purpose_and_oversized_tokens_fail_closed(self):
        token = self.capability()
        encoded, signature = token.split(".")
        altered = encoded[:-1] + ("A" if encoded[-1] != "A" else "B")
        self.assertIsNone(
            verify_native_capability(
                self.active_path, f"{altered}.{signature}", current_key_id=self.key_id
            )
        )
        self.assertIsNone(
            verify_native_capability(self.active_path, "x" * 4097, current_key_id=self.key_id)
        )
        with self.assertRaisesRegex(ValueError, "claims are invalid"):
            self.capability(mode="legacy-access-token")

    def test_device_identity_is_opaque_and_stable_per_export_and_client(self):
        token = self.capability()
        claims = verify_native_capability(
            self.active_path, token, current_key_id=self.key_id
        )
        key = load_native_key(self.active_path, self.key_id)
        first = native_device_identity(key, claims, "192.0.2.10", "Synthetic Client/1")
        same = native_device_identity(key, claims, "192.0.2.10", "Synthetic Client/1")
        other_client = native_device_identity(key, claims, "192.0.2.11", "Synthetic Client/1")
        self.assertEqual(first, same)
        self.assertNotEqual(first, other_client)
        self.assertTrue(first.startswith("native:"))
        self.assertNotIn("192.0.2.10", first)
        self.assertNotIn("Synthetic Client", first)

    def test_capability_claims_reject_noncanonical_subject_channel_and_profile(self):
        for invalid in (
            {"subject": "00123"},
            {"channel": "not-a-uuid"},
            {"profile": "07"},
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.capability(**invalid)


if __name__ == "__main__":
    unittest.main()
