"""Unit tests for tg-projects stale-state pruning (_prune_stale_state).

Run:  python3 /mnt/mydisk/sd1/test_cleanup.py
No network, no real state.json: _load_state/_save_state are stubbed onto an
in-memory dict (a deep copy per call, like the real re-parse each load). The
_prune_stale_state below is a VERBATIM COPY of the function inserted into
__init__.py (right after _save_state); keep the two in sync when editing.
"""
import copy
import logging
import threading
import time
import unittest

logger = logging.getLogger("hermes_plugins.tg_projects")

_CWD_LOCK = threading.RLock()

_NOW = time.time()
_DAY = 86400


# ---- VERBATIM COPY of __init__.py:_prune_stale_state — keep in sync --------
def _prune_stale_state(max_age_days: int = 7) -> int:
    """Drop topic_bindings/topic_panels entries untouched for *max_age_days*.

    Entries without a numeric ``updated_at`` are kept (age unknown); a missing
    ``topic_panels`` bucket is fine. Returns the number of removed entries;
    I/O errors are logged, never raised (returns 0).
    """
    cutoff = time.time() - max_age_days * 86400
    removed = 0
    try:
        with _CWD_LOCK:
            state = _load_state()
            changed = False
            for key in ("topic_bindings", "topic_panels"):
                bucket = state.get(key)
                if not isinstance(bucket, dict) or not bucket:
                    continue
                for entry_key, entry in list(bucket.items()):
                    if not isinstance(entry, dict):
                        continue
                    ts = entry.get("updated_at")
                    if isinstance(ts, bool) or not isinstance(ts, (int, float)):
                        continue  # no valid timestamp — keep, never guess
                    if ts < cutoff:
                        bucket.pop(entry_key, None)
                        removed += 1
                        changed = True
                if not bucket:
                    state.pop(key, None)
                    changed = True
            if changed:
                _save_state(state)
    except Exception:
        logger.warning("tg-projects: stale state prune failed", exc_info=True)
        return 0
    return removed


# ---- end copy ---------------------------------------------------------------


class PruneTests(unittest.TestCase):
    def setUp(self):
        self.data = {}
        self.saves = []
        self.fail_load = False
        self.fail_save = False

        def _load_state():
            if self.fail_load:
                raise OSError("state.json unreadable")
            return copy.deepcopy(self.data)

        def _save_state(state):
            if self.fail_save:
                raise OSError("disk full")
            self.saves.append(copy.deepcopy(state))
            self.data = copy.deepcopy(state)

        g = globals()
        self._orig = (g.get("_load_state"), g.get("_save_state"))
        g["_load_state"], g["_save_state"] = _load_state, _save_state

    def tearDown(self):
        g = globals()
        g["_load_state"], g["_save_state"] = self._orig

    def _binding(self, updated_at=None):
        entry = {"project_id": "p_1", "project_name": "X", "cwd": "/tmp/x",
                 "session_id": None}
        if updated_at is not None:
            entry["updated_at"] = updated_at
        return entry

    # ------------------------------------------------------------- behaviour
    def test_old_pruned_recent_kept(self):
        self.data = {"topic_bindings": {
            "c:1": self._binding(_NOW - 8 * _DAY),
            "c:2": self._binding(_NOW - 1 * _DAY),
        }}
        removed = _prune_stale_state()
        self.assertEqual(removed, 1)
        self.assertEqual(list(self.data["topic_bindings"]), ["c:2"])
        self.assertEqual(len(self.saves), 1)

    def test_boundary_seven_days_kept(self):
        # just under 7 days old (cutoff uses time.time() at call time)
        self.data = {"topic_bindings": {"c:1": self._binding(time.time() - 7 * _DAY + 60)}}
        self.assertEqual(_prune_stale_state(), 0)
        self.assertIn("c:1", self.data["topic_bindings"])
        self.assertEqual(self.saves, [])

    def test_just_over_seven_days_pruned(self):
        self.data = {"topic_bindings": {"c:1": self._binding(time.time() - 7 * _DAY - 60)}}
        self.assertEqual(_prune_stale_state(), 1)
        self.assertNotIn("topic_bindings", self.data)

    def test_missing_updated_at_kept(self):
        self.data = {"topic_bindings": {"c:1": self._binding(None)}}
        self.assertEqual(_prune_stale_state(), 0)
        self.assertIn("c:1", self.data["topic_bindings"])
        self.assertEqual(self.saves, [])

    def test_invalid_timestamps_kept(self):
        self.data = {"topic_bindings": {
            "c:bool": self._binding(True),
            "c:str": self._binding("1791300571"),
            "c:none": self._binding(None),
        }}
        self.assertEqual(_prune_stale_state(), 0)
        self.assertEqual(len(self.data["topic_bindings"]), 3)
        self.assertEqual(self.saves, [])

    def test_topic_panels_pruned_too(self):
        self.data = {"topic_panels": {
            "c:1": {"panel": "x", "updated_at": _NOW - 30 * _DAY},
            "c:2": {"panel": "y", "updated_at": _NOW},
        }}
        self.assertEqual(_prune_stale_state(), 1)
        self.assertEqual(list(self.data["topic_panels"]), ["c:2"])

    def test_topic_panels_absent_is_fine(self):
        self.data = {"topic_bindings": {"c:1": self._binding(_NOW)}}
        self.assertEqual(_prune_stale_state(), 0)
        self.assertEqual(self.saves, [])

    def test_custom_max_age(self):
        self.data = {"topic_bindings": {"c:1": self._binding(_NOW - 10 * _DAY)}}
        self.assertEqual(_prune_stale_state(max_age_days=30), 0)
        self.assertIn("c:1", self.data["topic_bindings"])
        self.assertEqual(_prune_stale_state(max_age_days=5), 1)
        self.assertNotIn("topic_bindings", self.data)

    def test_emptied_bucket_key_dropped(self):
        self.data = {"topic_bindings": {"c:1": self._binding(_NOW - 9 * _DAY)}}
        self.assertEqual(_prune_stale_state(), 1)
        self.assertNotIn("topic_bindings", self.data)

    def test_empty_state_no_write(self):
        self.data = {}
        self.assertEqual(_prune_stale_state(), 0)
        self.data = {"topic_bindings": {}, "topic_panels": {}}
        self.assertEqual(_prune_stale_state(), 0)
        self.assertEqual(self.saves, [])

    def test_non_dict_entry_kept(self):
        self.data = {"topic_bindings": {"c:1": "junk"}}
        self.assertEqual(_prune_stale_state(), 0)
        self.assertEqual(self.data["topic_bindings"]["c:1"], "junk")

    # ------------------------------------------------------------- I/O errors
    def test_save_failure_returns_zero_no_raise(self):
        self.data = {"topic_bindings": {"c:1": self._binding(_NOW - 9 * _DAY)}}
        self.fail_save = True
        self.assertEqual(_prune_stale_state(), 0)  # no raise, no count
        # the in-memory data is untouched (real _load_state re-parses)
        self.assertIn("c:1", self.data["topic_bindings"])

    def test_load_failure_returns_zero_no_raise(self):
        self.fail_load = True
        self.assertEqual(_prune_stale_state(), 0)
        self.assertEqual(self.saves, [])


if __name__ == "__main__":
    unittest.main()
