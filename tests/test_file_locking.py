import fcntl
import tempfile
import threading
import time
import unittest
from pathlib import Path

from catchuparr.file_locking import cooperative_flock


class CooperativeFlockTests(unittest.TestCase):
    def test_waits_until_other_descriptor_releases(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "x.lock"
            first = path.open("a+")
            second = path.open("a+")
            fcntl.flock(first.fileno(), fcntl.LOCK_EX)
            acquired = threading.Event()

            def take():
                cooperative_flock(second.fileno(), fcntl.LOCK_EX)
                acquired.set()

            thread = threading.Thread(target=take)
            thread.start()
            time.sleep(0.05)
            self.assertFalse(acquired.is_set())
            fcntl.flock(first.fileno(), fcntl.LOCK_UN)
            thread.join(2)
            self.assertTrue(acquired.is_set())
            first.close()
            second.close()

    def test_shared_locks_coexist(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "x.lock"
            with path.open("a+") as a, path.open("a+") as b:
                cooperative_flock(a.fileno(), fcntl.LOCK_SH)
                cooperative_flock(b.fileno(), fcntl.LOCK_SH)


if __name__ == "__main__":
    unittest.main()
