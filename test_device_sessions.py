"""Unit tests for tg-projects device_sessions (cross-device session ownership).

Run:  python3 /home/meow/.hermes/plugins/tg-projects/test_device_sessions.py
No network, no Telegram, no getUpdates. The db file is pointed at a
temp directory so the real plugin directory is never touched.
"""
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import device_sessions as ds


class DeviceClaimTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        ds._DB_FILE = Path(self._tmp.name) / "device_sessions.db"
        ds.reset_for_tests()
        ds.clear_all()

    def tearDown(self):
        ds.reset_for_tests()
        self._tmp.cleanup()

    # ------------------------------------------------------------------ claim
    def test_claim_then_lookup(self):
        self.assertTrue(ds.claim("s1", "telegram", "7559860199:65003"))
        row = ds.lookup("s1")
        self.assertIsNotNone(row)
        self.assertEqual(row["device"], "telegram")
        self.assertEqual(row["surface"], "7559860199:65003")
        self.assertFalse(row["stale"])

    def test_claim_is_case_insensitive_on_device(self):
        ds.claim("s1", "Telegram")
        self.assertEqual(ds.lookup("s1")["device"], "telegram")

    def test_claim_replaces_other_device(self):
        # A handoff is a deliberate act: the new claim always replaces the old.
        ds.claim("s1", "desktop", "profile:default")
        ds.claim("s1", "telegram", "7559860199")
        row = ds.lookup("s1")
        self.assertEqual(row["device"], "telegram")
        self.assertEqual(row["surface"], "7559860199")

    def test_claim_empty_session_is_noop(self):
        self.assertFalse(ds.claim("", "telegram"))
        self.assertFalse(ds.claim(None, "telegram"))
        self.assertIsNone(ds.lookup(""))

    # ----------------------------------------------------------- busy_elsewhere
    def test_busy_elsewhere_when_other_device_fresh(self):
        ds.claim("s1", "desktop", "profile:default")
        busy = ds.busy_elsewhere("s1", "telegram")
        self.assertIsNotNone(busy)
        self.assertEqual(busy["device"], "desktop")

    def test_not_busy_same_device(self):
        ds.claim("s1", "telegram", "chat")
        self.assertIsNone(ds.busy_elsewhere("s1", "telegram"))

    def test_not_busy_when_idle(self):
        self.assertIsNone(ds.busy_elsewhere("s-nope", "telegram"))

    def test_not_busy_when_claim_stale(self):
        ds.claim("s1", "desktop", "p")
        conn = ds._connect()
        conn.execute(
            "UPDATE device_session_claims SET heartbeat=? WHERE session_id=?",
            (int(time.time()) - ds.HEARTBEAT_STALE_S - 60, "s1"))
        conn.commit()
        self.assertIsNone(ds.busy_elsewhere("s1", "telegram"))

    # -------------------------------------------------------------- heartbeat
    def test_heartbeat_refreshes_own_claim_only(self):
        ds.claim("s1", "desktop", "p")
        conn = ds._connect()
        conn.execute(
            "UPDATE device_session_claims SET heartbeat=? WHERE session_id=?",
            (int(time.time()) - 300, "s1"))
        conn.commit()
        old = ds.lookup("s1")["heartbeat"]
        self.assertFalse(ds.heartbeat("s1", "telegram"))  # not the owner
        self.assertTrue(ds.heartbeat("s1", "desktop"))
        self.assertGreater(ds.lookup("s1")["heartbeat"], old)

    def test_heartbeat_missing_row_is_false(self):
        self.assertFalse(ds.heartbeat("s-nope", "telegram"))

    # ----------------------------------------------------------------- release
    def test_release_drops_claim(self):
        ds.claim("s1", "telegram", "chat")
        self.assertTrue(ds.release("s1"))
        self.assertIsNone(ds.lookup("s1"))
        self.assertFalse(ds.release("s1"))  # already gone

    def test_release_only_own_device_row(self):
        ds.claim("s1", "desktop", "p")
        self.assertFalse(ds.release("s1", "telegram"))  # not the owner
        self.assertIsNotNone(ds.lookup("s1"))
        self.assertTrue(ds.release("s1", "desktop"))

    # ------------------------------------------------------------- fail-open
    def test_lookup_bad_db_returns_none(self):
        ds._DB_FILE = Path(self._tmp.name) / "no-such-dir" / "x.db"
        # _connect fails (directory missing and creation impossible via  ) —
        # actually mkdir will create it; instead point at an unreadable path:
        ds.reset_for_tests()
        # Simulate unreadable: a directory path as db file
        bad_dir = Path(self._tmp.name) / "badfile"
        bad_dir.mkdir()
        ds._DB_FILE = bad_dir  # sqlite cannot open a directory
        ds.reset_for_tests()
        self.assertIsNone(ds.lookup("s1"))
        self.assertFalse(ds.claim("s1", "telegram"))
        self.assertFalse(ds.release("s1"))
        self.assertIsNone(ds.busy_elsewhere("s1", "telegram"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
