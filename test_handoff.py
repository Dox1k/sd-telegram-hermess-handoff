"""Unit tests for tg-projects handoff (retired cross-device gate).

Run:  python3 /home/meow/.hermes/plugins/tg-projects/test_handoff.py
No network, no Telegram, no getUpdates, no writes to the real state.db —
state.json is stubbed and the lease helpers are tested against temp sqlite
files that mimic the session_turn_leases table.

The production contract under test (HookPassThroughTests): the
``pre_gateway_dispatch`` hook ALWAYS returns None — a Telegram message is
dispatched even while the same session runs on the PC, with no Yes/No
question, no parked message, no token. The remaining classes cover the
dead legacy helpers kept for old ``tgp:ho:*`` keyboards.
"""
import asyncio
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path

PLUGIN_DIR = Path("/home/meow/.hermes/plugins/tg-projects")

# device_sessions must import cleanly standalone (it is a dependency of handoff)
sys.path.insert(0, str(PLUGIN_DIR))

# Load handoff as a standalone module (same loader trick the plugin uses).
spec = importlib.util.spec_from_file_location("tg_projects_handoff", str(PLUGIN_DIR / "handoff.py"))
handoff = importlib.util.module_from_spec(spec)
sys.modules.setdefault("tg_projects_handoff", handoff)
spec.loader.exec_module(handoff)
device_sessions = handoff.device_sessions


class _TempHome:
    """Point HERMES_HOME and the plugin dir state at temp paths."""

    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.home = Path(self.tmp.name) / "home"
        self.home.mkdir()
        handoff._STATE_FILE = Path(self.tmp.name) / "state.json"
        handoff._EVENTS_FILE = Path(self.tmp.name) / "handoff_events.jsonl"
        handoff._PLUG_DIR = Path(self.tmp.name)
        device_sessions._DB_FILE = Path(self.tmp.name) / "device_sessions.db"
        device_sessions.reset_for_tests()
        self.saved_env = os.environ.get("HERMES_HOME")
        os.environ["HERMES_HOME"] = str(self.home)

    def restore(self):
        if self.saved_env is None:
            os.environ.pop("HERMES_HOME", None)
        else:
            os.environ["HERMES_HOME"] = self.saved_env
        device_sessions.reset_for_tests()
        self.tmp.cleanup()

    def state(self) -> dict:
        if handoff._STATE_FILE.exists():
            return json.loads(handoff._STATE_FILE.read_text(encoding="utf-8"))
        return {}


def _lease_db(path: Path, rows):
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE session_turn_leases ("
                 "conversation_id TEXT PRIMARY KEY, holder TEXT NOT NULL, "
                 "acquired_at REAL NOT NULL, expires_at REAL NOT NULL)")
    for cid, holder, exp in rows:
        conn.execute("INSERT INTO session_turn_leases VALUES (?,?,?,?)",
                     (cid, holder, time.time(), exp))
    conn.commit()
    return conn


class HolderParsingTests(unittest.TestCase):
    def test_platform_token(self):
        self.assertEqual(
            handoff.parse_holder_platform(
                "pid=840408:turn=x:14f52bf3:platform=desktop"), "desktop")
        self.assertEqual(handoff.parse_holder_platform("pid=1:platform=telegram"), "telegram")
        self.assertEqual(handoff.parse_holder_platform("no-token"), "")


class ForeignLeaseTests(unittest.TestCase):
    def setUp(self):
        self._t = _TempHome()
        self.db_path = self._t.home / "state.db"

    def tearDown(self):
        self._t.restore()

    def _conn(self, rows):
        return _lease_db(self.db_path, rows)

    def test_fresh_desktop_lease_detected(self):
        conn = self._conn([("s1", "pid=1:platform=desktop", time.time() + 120)])
        lease = handoff.foreign_running_lease(conn, "s1")
        self.assertIsNotNone(lease)
        self.assertEqual(lease["platform"], "desktop")
        conn.close()

    def test_telegram_lease_is_own_surface(self):
        conn = self._conn([("s1", "pid=1:platform=telegram", time.time() + 120)])
        self.assertIsNone(handoff.foreign_running_lease(conn, "s1"))
        conn.close()

    def test_expired_lease_ignored(self):
        conn = self._conn([("s1", "pid=1:platform=desktop", time.time() - 3600)])
        self.assertIsNone(handoff.foreign_running_lease(conn, "s1"))
        conn.close()

    def test_missing_table_returns_none(self):
        conn = sqlite3.connect(":memory:")
        self.assertIsNone(handoff.foreign_running_lease(conn, "s1"))
        conn.close()


class StealLeaseTests(unittest.TestCase):
    def setUp(self):
        self._t = _TempHome()
        self.db_path = self._t.home / "state.db"

    def tearDown(self):
        self._t.restore()

    def test_steal_removes_foreign_row(self):
        conn = _lease_db(self.db_path, [("s1", "pid=9:platform=desktop", time.time() + 300)])
        conn.close()
        self.assertTrue(handoff.steal_lease("s1"))
        check = sqlite3.connect(str(self.db_path))
        self.assertIsNone(check.execute(
            "SELECT 1 FROM session_turn_leases WHERE conversation_id='s1'").fetchone())
        check.close()

    def test_steal_never_touches_own_telegram_lease(self):
        conn = _lease_db(self.db_path, [("s1", "pid=9:platform=telegram", time.time() + 300)])
        conn.close()
        self.assertFalse(handoff.steal_lease("s1"))
        check = sqlite3.connect(str(self.db_path))
        self.assertIsNotNone(check.execute(
            "SELECT 1 FROM session_turn_leases WHERE conversation_id='s1'").fetchone())
        check.close()

    def test_steal_missing_row_is_false(self):
        self.assertFalse(handoff.steal_lease("nope"))

    def test_steal_missing_db_is_false(self):
        self.assertFalse(handoff.steal_lease("s1"))


class PendingTests(unittest.TestCase):
    def setUp(self):
        self._t = _TempHome()

    def tearDown(self):
        self._t.restore()

    def test_store_and_pop_roundtrip(self):
        handoff.store_pending("sk1", "s1", "привет", {"chat_id": "5", "user_id": "7"})
        entry = handoff.pop_pending("sk1")
        self.assertEqual(entry["session_id"], "s1")
        self.assertEqual(entry["text"], "привет")
        self.assertEqual(entry["source"]["chat_id"], "5")
        # popped: a second pop is empty
        self.assertIsNone(handoff.pop_pending("sk1"))

    def test_stale_pending_dropped(self):
        handoff.store_pending("sk1", "s1", "x", {})
        state = self._t.state()
        state["handoff_pending"]["sk1"]["ts"] = int(time.time()) - 999999
        handoff._STATE_FILE.write_text(json.dumps(state), encoding="utf-8")
        self.assertIsNone(handoff.pop_pending("sk1"))

    def test_drop_pending(self):
        handoff.store_pending("sk1", "s1", "x", {})
        handoff.drop_pending("sk1")
        self.assertIsNone(handoff.pop_pending("sk1"))
        self.assertNotIn("handoff_pending", self._t.state())

    def test_empty_text_not_stored(self):
        self.assertFalse(handoff.store_pending("sk1", "s1", "   ", {}))
        self.assertFalse(handoff.store_pending("", "s1", "x", {}))


class NotifyDesktopTests(unittest.TestCase):
    def setUp(self):
        self._t = _TempHome()

    def tearDown(self):
        self._t.restore()

    def test_event_appended_as_jsonl(self):
        ok = handoff.notify_desktop_handoff("s1", "desktop", "telegram", extra={"text": "hi"})
        self.assertTrue(ok)
        line = handoff._EVENTS_FILE.read_text(encoding="utf-8").strip().splitlines()[0]
        evt = json.loads(line)
        self.assertEqual(evt["type"], "session.handoff")
        self.assertEqual(evt["session_id"], "s1")
        self.assertEqual(evt["from_device"], "desktop")
        self.assertEqual(evt["to_device"], "telegram")
        self.assertEqual(evt["text"], "hi")


class PerformHandoffTests(unittest.TestCase):
    def setUp(self):
        self._t = _TempHome()
        self.db_path = self._t.home / "state.db"
        self._lease_conn = _lease_db(
            self.db_path, [("s1", "pid=9:platform=desktop", time.time() + 300)])
        self._lease_conn.close()
        handoff.store_pending("sk1", "s1", "продолжай", {"chat_id": "5", "user_id": "7"})

    def tearDown(self):
        self._t.restore()

    def test_handoff_steals_claim_notifies(self):
        pending = asyncio.run(handoff.perform_handoff("sk1", "s1"))
        self.assertEqual(pending["text"], "продолжай")
        # lease gone
        check = sqlite3.connect(str(self.db_path))
        self.assertIsNone(check.execute(
            "SELECT 1 FROM session_turn_leases WHERE conversation_id='s1'").fetchone())
        check.close()
        # claimed for telegram
        self.assertEqual(device_sessions.lookup("s1")["device"], "telegram")
        # desktop event recorded
        self.assertIn("session.handoff", handoff._EVENTS_FILE.read_text(encoding="utf-8"))

    def test_handoff_requires_pending(self):
        self.assertIsNone(asyncio.run(handoff.perform_handoff("sk-other", "s1")))

    def test_handoff_stale_session_mismatch_is_refused(self):
        handoff.store_pending("sk2", "s-other", "x", {})
        self.assertIsNone(asyncio.run(handoff.perform_handoff("sk2", "s1")))


class CallbackRecognitionTests(unittest.TestCase):
    def test_is_handoff_callback(self):
        # New token-based format: 8-hex token, y or n answer.
        self.assertTrue(handoff.is_handoff_callback("tgp:ho:deadbeef:y"))
        self.assertTrue(handoff.is_handoff_callback("tgp:ho:deadbeef:n"))
        self.assertFalse(handoff.is_handoff_callback("tgp:s:20261003_225921_f8d1e0"))
        self.assertFalse(handoff.is_handoff_callback("tgp:p:1"))
        self.assertFalse(handoff.is_handoff_callback("tgp:ho:short:y"))  # 5 chars
        self.assertFalse(handoff.is_handoff_callback("tgp:ho:deadbeefx:y"))  # 9 chars
        self.assertFalse(handoff.is_handoff_callback("tgp:ho:DEADBEEF:y"))  # uppercase
        # Old session_id format no longer matches (24-char ids would exceed
        # Telegram's 64-byte callback_data cap once combined with the prefix).
        self.assertFalse(handoff.is_handoff_callback("tgp:ho:20261003_225921_f8d1e0:y"))

    def test_token_caps_well_within_64_bytes(self):
        # Real session ids are 24 chars; token caps them at 8 hex.
        # tgp:ho: (7) + 8 + :y (2) = 17 bytes total. Well under 64.
        for token_len in (7, 8, 9):
            data = f"tgp:ho:{'a' * token_len}:y"
            self.assertEqual(len(data.encode("utf-8")), 7 + token_len + 2)
        self.assertLessEqual(17, 64)


class TokenManagementTests(unittest.TestCase):
    def setUp(self):
        self._t = _TempHome()

    def tearDown(self):
        self._t.restore()

    def test_register_and_consume_roundtrip(self):
        token = handoff.register_token("20261006_153514_bc11eb79", "sk-lane")
        self.assertEqual(len(token), 8)
        self.assertTrue(handoff.is_handoff_callback(f"tgp:ho:{token}:y"))
        sid, sk = handoff.consume_token(token)
        self.assertEqual(sid, "20261006_153514_bc11eb79")
        self.assertEqual(sk, "sk-lane")

    def test_consume_is_destructive(self):
        token = handoff.register_token("s1", "sk1")
        self.assertIsNotNone(handoff.consume_token(token))
        self.assertIsNone(handoff.consume_token(token))

    def test_consume_unknown_returns_none(self):
        self.assertIsNone(handoff.consume_token("deadbeef"))

    def test_empty_session_id_cannot_register(self):
        self.assertEqual(handoff.register_token("", "sk1"), "")
        self.assertEqual(handoff.register_token("   ", "sk1"), "")
        self.assertEqual(handoff.register_token(None, "sk1"), "")

    def test_expired_token_returns_none(self):
        token = handoff.register_token("s1", "sk1")
        state = self._t.state()
        state["handoff_tokens"][token]["ts"] = int(time.time()) - 999999
        handoff._STATE_FILE.write_text(json.dumps(state), encoding="utf-8")
        self.assertIsNone(handoff.consume_token(token))
        # entry removed (consume is destructive even on stale)
        self.assertNotIn(token, self._t.state().get("handoff_tokens", {}))

    def test_register_prunes_stale_tokens(self):
        # Seed a stale entry plus a live one, then register a new token:
        # only the live one and the new one survive.
        stale = "00000000"
        live = "11111111"
        state = {
            "handoff_tokens": {
                stale: {"session_id": "old", "session_key": "sk-old",
                        "ts": int(time.time()) - 999999},
                live: {"session_id": "keep", "session_key": "sk-keep",
                       "ts": int(time.time())},
            }
        }
        handoff._STATE_FILE.write_text(json.dumps(state), encoding="utf-8")
        token = handoff.register_token("s-new", "sk-new")
        after = self._t.state()["handoff_tokens"]
        self.assertNotIn(stale, after)          # pruned
        self.assertIn(live, after)              # kept
        self.assertIn(token, after)             # newly registered
        self.assertEqual(len(after), 2)

    def test_consume_of_exhausted_bucket_cleans_up(self):
        token = handoff.register_token("s1", "sk1")
        handoff.consume_token(token)
        # when the bucket is empty, the key itself is dropped from state.json
        self.assertNotIn("handoff_tokens", self._t.state())


class HookPassThroughTests(unittest.TestCase):
    """on_pre_gateway_dispatch is a retired passthrough: it must return None
    (allow) for EVERY input, create no tokens/prompts/pending state, and
    never block or rewrite the dispatch of the incoming message."""

    def setUp(self):
        self._t = _TempHome()
        # Sentinels: any attempt to send a prompt or touch the native bot
        # fails the test (the retired hook must not ask questions).
        self._native_calls = []
        self._saved_get_native = handoff._get_native
        self._saved_get_adapter = handoff._get_adapter
        handoff._get_native = lambda: self._native_calls.append(1) or None
        handoff._get_adapter = lambda: self._native_calls.append(1) or None

    def tearDown(self):
        handoff._get_native = self._saved_get_native
        handoff._get_adapter = self._saved_get_adapter
        self._t.restore()

    def _event(self, text="hi", platform="telegram", chat_type="dm", internal=False):
        src = types.SimpleNamespace(
            platform=types.SimpleNamespace(value=platform),
            chat_id="5", thread_id="17", user_id="7", chat_type=chat_type)
        return types.SimpleNamespace(source=src, text=text, internal=internal, message_id="1")

    def _gateway(self, session_id="s1"):
        store = types.SimpleNamespace(
            lookup_by_session_key=lambda sk: types.SimpleNamespace(session_id=session_id))
        return types.SimpleNamespace(
            session_store=store,
            _generate_session_key=lambda src: "tg:telegram:dm:5:17",
            async_session_store=None)

    def _run(self, event, gateway, store=None):
        return asyncio.run(handoff.on_pre_gateway_dispatch(event, gateway, store))

    def _assert_no_handoff_state(self):
        state = self._t.state()
        self.assertNotIn("handoff_pending", state)
        self.assertNotIn("handoff_tokens", state)
        self.assertFalse(handoff._EVENTS_FILE.exists())
        self.assertEqual(self._native_calls, [])

    def _assert_message_dispatched(self, event, result):
        # None = normal dispatch in the core (run_inbound._hm_pre_gateway_dispatch_hook);
        # a "skip" dict would DROP the message, a "rewrite" would replace its text.
        self.assertIsNone(result)
        self.assertEqual(event.text, getattr(event, "text", ""))

    # (a) the hook returns None for every input --------------------------------

    def test_idle_message_flows_through(self):
        # no leases table at all — nothing to be busy with
        event = self._event(text="привет")
        result = self._run(event, self._gateway())
        self._assert_message_dispatched(event, result)
        self._assert_no_handoff_state()

    def test_parallel_desktop_write_flows_through(self):
        # a FRESH desktop-platform lease: the PC is writing this session
        # right now. Old behavior parked + asked; now the message must go.
        conn = _lease_db(self._t.home / "state.db",
                         [("s1", "pid=1:platform=desktop", time.time() + 300)])
        conn.close()
        event = self._event(text="продолжай без вопросов")
        result = self._run(event, self._gateway())
        self._assert_message_dispatched(event, result)
        self._assert_no_handoff_state()

    def test_message_during_busy_telegram_session_flows_through(self):
        # our own surface's lease (a TG turn is running): still just allow
        conn = _lease_db(self._t.home / "state.db",
                         [("s1", "pid=1:platform=telegram", time.time() + 300)])
        conn.close()
        event = self._event()
        result = self._run(event, self._gateway())
        self._assert_message_dispatched(event, result)
        self._assert_no_handoff_state()

    def test_subagent_lease_flows_through(self):
        conn = _lease_db(self._t.home / "state.db",
                         [("s1", "pid=1:turn=x:platform=subagent", time.time() + 300)])
        conn.close()
        self.assertIsNone(self._run(self._event(), self._gateway()))

    def test_command_not_intercepted(self):
        event = self._event(text="/stop")
        result = self._run(event, self._gateway())
        self._assert_message_dispatched(event, result)
        self._assert_no_handoff_state()

    def test_non_telegram_source_flows_through(self):
        for platform in ("discord", "desktop", "slack"):
            event = self._event(text="hi", platform=platform)
            result = self._run(event, self._gateway())
            self._assert_message_dispatched(event, result)
        self._assert_no_handoff_state()

    def test_internal_redispatch_flows_through(self):
        event = self._event(internal=True)
        result = self._run(event, self._gateway())
        self._assert_message_dispatched(event, result)
        self._assert_no_handoff_state()

    def test_group_chat_flows_through(self):
        event = self._event(text="в топике", chat_type="group")
        result = self._run(event, self._gateway())
        self._assert_message_dispatched(event, result)
        self._assert_no_handoff_state()

    def test_empty_text_flows_through(self):
        event = self._event(text="   ")
        result = self._run(event, self._gateway())
        self._assert_message_dispatched(event, result)

    def test_unresolvable_lane_flows_through(self):
        # gateway stub without a session-key generator (lane unknown)
        gateway = types.SimpleNamespace(session_store=None, async_session_store=None)
        event = self._event()
        result = self._run(event, gateway)
        self._assert_message_dispatched(event, result)
        self._assert_no_handoff_state()

    def test_missing_source_flows_through(self):
        event = types.SimpleNamespace(text="hi", internal=False)
        result = self._run(event, self._gateway())
        self.assertIsNone(result)

    def test_no_store_passed_flows_through(self):
        event = self._event()
        result = self._run(event, self._gateway(), None)
        self._assert_message_dispatched(event, result)

    # (b) no tokens / questions / parked messages are created ------------------

    def test_no_token_registered_for_busy_session(self):
        conn = _lease_db(self._t.home / "state.db",
                         [("s1", "pid=1:platform=desktop", time.time() + 300)])
        conn.close()
        self._run(self._event(), self._gateway())
        state = self._t.state()
        self.assertNotIn("handoff_tokens", state)   # no Yes/No token
        self.assertNotIn("handoff_pending", state)  # no parked message
        self.assertEqual(self._native_calls, [])    # no prompt/keyboard send
        # nothing consumed the lease either (no cross-device stop)
        check = sqlite3.connect(str(self._t.home / "state.db"))
        self.assertIsNotNone(check.execute(
            "SELECT 1 FROM session_turn_leases WHERE conversation_id='s1'").fetchone())
        check.close()

    # (c) the message is never lost ---------------------------------------------

    def test_message_text_survives_untouched(self):
        # the hook must not rewrite or drop the text: the very same string is
        # what the gateway dispatches after the hook returns None
        event = self._event(text="важное сообщение, не потерять")
        before = event.text
        result = self._run(event, self._gateway())
        self.assertIsNone(result)
        self.assertEqual(event.text, before)

    def test_event_object_not_replaced(self):
        # None (not a new event / not a dict) means the core keeps dispatching
        # the SAME event object it passed in
        event = self._event()
        result = self._run(event, self._gateway())
        self.assertIsNone(result)
        self.assertIsNot(result, event)

    def test_exceptions_fail_open(self):
        class _Boom:
            @property
            def source(self):
                raise RuntimeError("boom")
        result = asyncio.run(handoff.on_pre_gateway_dispatch(_Boom(), None, None))
        self.assertIsNone(result)

    def test_weird_event_shapes_fail_open(self):
        for weird in (None, 42, object()):
            result = asyncio.run(handoff.on_pre_gateway_dispatch(weird, None, None))
            self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main(verbosity=2)
