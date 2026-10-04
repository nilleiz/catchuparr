import sqlite3
import tempfile
import unittest
from pathlib import Path

from catchuparr.security import AccessTokenStore, TokenStore


class TokenStoreTests(unittest.TestCase):
    def test_issues_opaque_tokens_and_persists_only_digest(self):
        with tempfile.TemporaryDirectory() as folder:
            store = AccessTokenStore(Path(folder))
            token = store.issue("dispatcharr-user-7")
            self.assertEqual(len(token), 43)
            self.assertEqual(store.lookup(token), "dispatcharr-user-7")
            raw_db = (Path(folder) / "archive.sqlite3").read_bytes()
            self.assertNotIn(token.encode(), raw_db)

            with sqlite3.connect(Path(folder) / "archive.sqlite3") as db:
                digest, user_id = db.execute("SELECT token_hash,user_id FROM http_tokens").fetchone()
            self.assertEqual(len(digest), 64)
            self.assertEqual(user_id, "dispatcharr-user-7")

    def test_revocation_and_user_revocation(self):
        with tempfile.TemporaryDirectory() as folder:
            store = TokenStore(Path(folder))
            first = store.create("one")
            second = store.create("one")
            other = store.create("two")

            self.assertTrue(store.revoke(first))
            self.assertFalse(store.revoke(first))
            self.assertIsNone(store.lookup(first))
            self.assertEqual(store.revoke_user("one"), 1)
            self.assertIsNone(store.lookup(second))
            self.assertEqual(store.lookup(other), "two")

    def test_rejects_empty_user_id(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(ValueError):
                TokenStore(Path(folder)).create(" ")


if __name__ == "__main__":
    unittest.main()
