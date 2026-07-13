import tempfile
import threading
import unittest
from pathlib import Path

from execution_lock import ExecutionLockError, exclusive_execution_lock


class ExecutionLockTests(unittest.TestCase):
    def test_second_thread_cannot_share_execution_authority(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = str(Path(tmp) / "trades_v2.db")
            entered = threading.Event()
            release = threading.Event()
            outcomes = []

            def owner():
                with exclusive_execution_lock(ledger):
                    entered.set()
                    release.wait(timeout=2)

            thread = threading.Thread(target=owner)
            thread.start()
            self.assertTrue(entered.wait(timeout=2))
            try:
                with self.assertRaises(ExecutionLockError):
                    with exclusive_execution_lock(ledger):
                        outcomes.append("unexpected")
            finally:
                release.set()
                thread.join(timeout=2)
            self.assertEqual(outcomes, [])
            self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main(verbosity=2)
