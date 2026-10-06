"""Stub-based unit checks for the tg-topics approval transport (tg-projects).

Run:  TGP_PLUGIN_DIR=/tmp/opencode/appr_deploy python3 test_approvals.py
No network, no real Telegram messages. Stubs replace the hermes core modules
(gateway.session_context) and ``telegram`` BEFORE the plugin loads, and a
background event loop stands in for the gateway loop, so the checks exercise
the plugin's own logic only:

* _approval_present: target resolution (bindings reverse lookup, state.db
  row, any-bound-topic fallback, owner's DM), button shape per
  allowed_choices, roundtrip once/deny, timeout fail-closed, send-failure
  fail-closed;
* _handle_approval_callback: digest-prefix mismatch rejected, expired
  request ignored, disallowed choice rejected, stale rid, waiter-missing;
* approvals_map pruning and the register() transport registration.

PLUGIN_DIR comes from TGP_PLUGIN_DIR (scratch/deploy copy with the approval
block integrated); the fallback is the live deployment directory. When the
loaded copy has no approval transport (block not integrated there yet) every
check skips instead of failing.
"""
import asyncio
import hashlib
import importlib.util
import logging
import os
import sqlite3
import sys
import tempfile
import threading
import time
import types
import unittest
from contextlib import contextmanager
from pathlib import Path

logging.getLogger("hermes_plugins.tg_projects").setLevel(logging.CRITICAL)

PLUGIN_DIR = Path(os.environ.get("TGP_PLUGIN_DIR",
                                 "/home/meow/.hermes/plugins/tg-projects"))

# ----------------------------------------------------------------- core stubs
fake_session_ctx = types.ModuleType("gateway.session_context")
_ENV: dict = {}


def _get_session_env(name, default=""):
    # Same contract as the real one: bound value first, os.environ second —
    # so these tests stay compatible with test_sessions.py when both files
    # share sys.modules under one pytest run.
    return _ENV.get(name) or os.environ.get(name, default)


fake_session_ctx.get_session_env = _get_session_env


class _Button:
    def __init__(self, text, callback_data=None):
        self.text = text
        self.callback_data = callback_data


class _Markup:
    def __init__(self, rows):
        self.rows = rows


fake_telegram = types.ModuleType("telegram")
fake_telegram.InlineKeyboardButton = _Button
fake_telegram.InlineKeyboardMarkup = _Markup
fake_telegram_ext = types.ModuleType("telegram.ext")
fake_telegram_ext.CallbackQueryHandler = object

_MODULES = {
    "gateway": types.ModuleType("gateway"),
    "gateway.session_context": fake_session_ctx,
    "telegram": fake_telegram,
    "telegram.ext": fake_telegram_ext,
}
for _name, _mod in _MODULES.items():
    sys.modules.setdefault(_name, _mod)
sys.modules["gateway"].session_context = fake_session_ctx
sys.modules["telegram"].ext = fake_telegram_ext

spec = importlib.util.spec_from_file_location(
    "tg_projects_approvals_under_test", str(PLUGIN_DIR / "__init__.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

_HAS_APPROVALS = hasattr(mod, "_approval_present")


# ------------------------------------------------------------ fakes (telegram)
class _LoopRunner:
    """A background asyncio loop standing in for the gateway's loop."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self.loop.run_forever,
                                        daemon=True, name="test-approval-loop")
        self._thread.start()

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=5)
        self.loop.close()


_LOOP = _LoopRunner()


class _FakeBot:
    def __init__(self):
        self.sends = []       # kwargs dicts of every send_message
        self.edits = []       # kwargs dicts of every edit_message_text
        self.fail_send = False
        self._next_id = 700

    async def send_message(self, **kwargs):
        if self.fail_send:
            raise RuntimeError("simulated telegram send failure")
        self.sends.append(kwargs)
        self._next_id += 1
        return types.SimpleNamespace(message_id=self._next_id)

    async def edit_message_text(self, **kwargs):
        self.edits.append(kwargs)
        return True


_BOT = _FakeBot()
_NATIVE = types.SimpleNamespace(bot=_BOT)


class _FakeQuery:
    def __init__(self, data, chat_id="7559860199", message_id=777):
        self.data = data
        self.message = types.SimpleNamespace(
            chat=types.SimpleNamespace(id=int(chat_id)),
            message_id=message_id,
        )
        self.edits = []       # (text, kwargs)
        self.answers = []     # answer texts

    async def edit_message_text(self, text, **kwargs):
        self.edits.append((text, kwargs))
        return True

    async def answer(self, text=None, **kwargs):
        self.answers.append(text)
        return True


# ------------------------------------------------------------- fakes (state.db)
# A file-backed throwaway db: the plugin opens AND CLOSES its own one-shot
# connection per query, so a shared in-memory handle would die on the first
# close. Every connection sees the same seeded rows.
_STATE_DB_FILE = tempfile.NamedTemporaryFile(suffix=".db", prefix="tgp_appr_",
                                             delete=False)
_STATE_DB_FILE.close()
_FAKE_DB_PATH = _STATE_DB_FILE.name


class _FakeStateDB:
    def __init__(self):
        self.conn = sqlite3.connect(_FAKE_DB_PATH)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY,"
            " source TEXT, cwd TEXT, started_at REAL, message_count INTEGER,"
            " title TEXT, chat_id TEXT, thread_id TEXT)")
        self.conn.commit()

    def seed(self, sid, source="telegram", cwd=None, chat_id="", thread_id=""):
        self.conn.execute(
            "INSERT INTO sessions (id, source, cwd, chat_id, thread_id,"
            " started_at, message_count) VALUES (?,?,?,?,?,?,?)",
            (sid, source, cwd, chat_id, thread_id, 1.0, 0))
        self.conn.commit()

    def reset(self):
        self.conn.execute("DELETE FROM sessions")
        self.conn.commit()


_FAKE_STATE = _FakeStateDB()


def _open_state_db_stub():
    conn = sqlite3.connect(f"file:{_FAKE_DB_PATH}?mode=rw", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


if _HAS_APPROVALS:
    mod._open_state_db = _open_state_db_stub  # fake state.db, never the real one


class _StateStub:
    """Replace state.json reads/writes with an in-memory dict."""

    def __init__(self):
        self.data = {"pending_cwd": {}, "thread_to_project": {}}

    def install(self):
        def _load():
            return dict(self.data)

        def _save(state):
            self.data = state

        mod._load_state = _load
        mod._save_state = _save


_STATE = _StateStub()
if _HAS_APPROVALS:
    _STATE.install()


# ------------------------------------------------------------------ helpers
def _rid(i=1):
    return hashlib.sha256(f"rid-{i}".encode()).hexdigest()[:32]


def _digest(i=1):
    return hashlib.sha256(f"digest-{i}".encode()).hexdigest()


class _FakeRequest:
    schema_version = 1

    def __init__(self, request_id, digest, command="rm -rf /tmp/x",
                 description="опасная команда", timeout_seconds=30,
                 allowed=("once", "session", "always", "deny"), surface="gateway"):
        self.request_id = request_id
        self.digest = digest
        self.command = command
        self.description = description
        self.pattern_key = "shell"
        self.pattern_keys = ("shell",)
        self.surface = surface
        self.timeout_seconds = timeout_seconds
        self.allowed_choices = tuple(allowed)

    def respond(self, choice):
        return ("decision", self.request_id, self.digest, choice)


@contextmanager
def _with_env(**env):
    """Set the fake session context AND os.environ for the duration."""
    saved = {k: os.environ.get(k) for k in env}
    _ENV.update(env)
    for k, v in env.items():
        os.environ[k] = v
    try:
        yield
    finally:
        for k in env:
            _ENV.pop(k, None)
            if saved[k] is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = saved[k]


def _reset_all():
    _FAKE_STATE.reset()
    _STATE.data = {"pending_cwd": {}, "thread_to_project": {}}
    _ENV.clear()
    _BOT.sends.clear()
    _BOT.edits.clear()
    _BOT.fail_send = False
    if _HAS_APPROVALS:
        mod._APPROVAL_WAITERS.clear()
        mod._NATIVE = _NATIVE
        mod._WIRE_LOOP = _LOOP.loop


def _wait_until(cond, timeout=5.0):
    """Block until cond() is truthy (the present thread publishes its state
    in stages: map entry, send result, message_id)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.02)
    return False


def _wait_for_entry(rid, timeout=5.0):
    return _wait_until(lambda: rid in
                       (_STATE.data.get(mod._APPROVALS_KEY) or {}), timeout)


def _tap(data):
    """Dispatch one callback through the real handler."""
    m = mod._APPROVAL_CB_RE.match(data)
    assert m is not None, data
    query = _FakeQuery(data)
    asyncio.run(mod._handle_approval_callback(query, m))
    return query


@unittest.skipUnless(_HAS_APPROVALS,
                     "approval transport block not present in this plugin copy")
class ApprovalCallbackDataTests(unittest.TestCase):
    """tgp:a:* parsing and the 64-byte callback_data cap."""

    def test_regex_accepts_all_choices(self):
        rid, digest = _rid(), _digest()
        for choice in ("once", "session", "always", "deny"):
            m = mod._APPROVAL_CB_RE.match(
                f"tgp:a:{choice}:{rid}:{digest[:8]}")
            self.assertIsNotNone(m, choice)
            self.assertEqual(m.group(1), choice)
            self.assertEqual(m.group(2), rid)
            self.assertEqual(m.group(3), digest[:8])

    def test_regex_rejects_stale_shapes(self):
        for bad in ("tgp:a:once",                          # no rid
                    "tgp:a:once:short",                    # short rid
                    f"tgp:a:once:{_rid()}",                # no digest prefix
                    f"tgp:a:maybe:{_rid()}:{_digest()[:8]}",  # unknown choice
                    f"tgp:a:once:{_rid()}:{_digest()[:9]}",   # long prefix
                    "tgp:a:3",                             # project index
                    f"tgp:s:{_rid()}",                     # session resume
                    "tgp:ho:deadbeef:y"):                  # handoff
            self.assertIsNone(mod._APPROVAL_CB_RE.match(bad), bad)

    def test_callback_data_fits_64_bytes(self):
        rid, digest = _rid(), _digest()
        for choice in ("once", "session", "always", "deny"):
            cb = f"tgp:a:{choice}:{rid}:{digest[:8]}"
            self.assertLessEqual(len(cb.encode("utf-8")), 64, cb)


@unittest.skipUnless(_HAS_APPROVALS,
                     "approval transport block not present in this plugin copy")
class ApprovalTargetTests(unittest.TestCase):
    """Topic resolution for the prompt."""

    def setUp(self):
        _reset_all()

    def test_binding_reverse_lookup_wins(self):
        _STATE.data["topic_bindings"] = {
            "7559860199:65008": {"project_id": "p1", "cwd": "/p",
                                 "session_id": "sess-1", "updated_at": 100},
        }
        _FAKE_STATE.seed("sess-1", chat_id="7559860199", thread_id="99999")
        with _with_env(HERMES_SESSION_ID="sess-1"):
            self.assertEqual(mod._approval_target(), ("7559860199", 65008))

    def test_state_db_row_when_no_binding_match(self):
        _STATE.data["topic_bindings"] = {
            "7559860199:65008": {"project_id": "p1", "cwd": "/p",
                                 "session_id": None, "updated_at": 100},
        }
        _FAKE_STATE.seed("sess-2", chat_id="7559860199", thread_id="65009")
        with _with_env(HERMES_SESSION_ID="sess-2"):
            self.assertEqual(mod._approval_target(), ("7559860199", 65009))

    def test_desktop_row_falls_back_to_any_bound_topic(self):
        _STATE.data["topic_bindings"] = {
            "7559860199:65008": {"project_id": "p1", "cwd": "/p",
                                 "session_id": None, "updated_at": 100},
        }
        _FAKE_STATE.seed("sess-d", source="desktop")
        with _with_env(HERMES_SESSION_ID="sess-d"):
            self.assertEqual(mod._approval_target(), ("7559860199", 65008))

    def test_no_session_no_binding_lands_in_owner_dm(self):
        with _with_env(HERMES_SESSION_ID=""):
            self.assertEqual(mod._approval_target(), ("7559860199", None))

    def test_session_bound_topic_preferred_over_plain_binding(self):
        _STATE.data["topic_bindings"] = {
            "7559860199:65001": {"cwd": "/a", "session_id": None,
                                 "updated_at": 500},
            "7559860199:65008": {"cwd": "/b", "session_id": "sess-9",
                                 "updated_at": 100},
        }
        with _with_env(HERMES_SESSION_ID=""):
            self.assertEqual(mod._approval_target(), ("7559860199", 65008))

    def test_dm_session_row_sends_without_thread(self):
        _FAKE_STATE.seed("sess-dm", chat_id="7559860199", thread_id="")
        with _with_env(HERMES_SESSION_ID="sess-dm"):
            self.assertEqual(mod._approval_target(), ("7559860199", None))


@unittest.skipUnless(_HAS_APPROVALS,
                     "approval transport block not present in this plugin copy")
class ApprovalPresentTests(unittest.TestCase):
    """_approval_present: send, buttons, roundtrip, fail-closed paths."""

    def setUp(self):
        _reset_all()

    def _run_present(self, request):
        box = []

        def _go():
            box.append(mod._approval_present(request))

        t = threading.Thread(target=_go, daemon=True, name="test-present")
        t.start()
        return t, box

    def test_send_target_and_buttons_full_choices(self):
        rid, digest = _rid(), _digest()
        _STATE.data["topic_bindings"] = {
            "7559860199:65008": {"cwd": "/p", "session_id": "sess-1",
                                 "updated_at": 100},
        }
        request = _FakeRequest(rid, digest)
        with _with_env(HERMES_SESSION_ID="sess-1"):
            t, box = self._run_present(request)
            try:
                self.assertTrue(_wait_for_entry(rid))
                self.assertTrue(_wait_until(lambda: _BOT.sends))
                self.assertEqual(len(_BOT.sends), 1)
                kwargs = _BOT.sends[0]
                self.assertEqual(kwargs["chat_id"], "7559860199")
                self.assertEqual(kwargs["message_thread_id"], 65008)
                self.assertIn("rm -rf /tmp/x", kwargs["text"])
                rows = kwargs["reply_markup"].rows
                self.assertEqual(rows[0][0].text, "✅ Одобрить")
                self.assertEqual(
                    rows[0][0].callback_data, f"tgp:a:once:{rid}:{digest[:8]}")
                self.assertEqual(
                    rows[0][1].callback_data, f"tgp:a:deny:{rid}:{digest[:8]}")
                self.assertEqual(
                    sorted(b.callback_data for b in rows[1]),
                    sorted([f"tgp:a:session:{rid}:{digest[:8]}",
                            f"tgp:a:always:{rid}:{digest[:8]}"]))
            finally:
                query = _tap(f"tgp:a:deny:{rid}:{digest[:8]}")
                t.join(10)
        self.assertFalse(t.is_alive())
        self.assertEqual(box, [("decision", rid, digest, "deny")])
        self.assertEqual(query.edits[0][0], "❌ Отклонено")
        self.assertNotIn(rid, _STATE.data.get(mod._APPROVALS_KEY, {}))
        self.assertNotIn(rid, mod._APPROVAL_WAITERS)

    def test_once_roundtrip_resolves_and_cleans_map(self):
        rid, digest = _rid(2), _digest(2)
        request = _FakeRequest(rid, digest)
        t, box = self._run_present(request)
        try:
            self.assertTrue(_wait_for_entry(rid))
            query = _tap(f"tgp:a:once:{rid}:{digest[:8]}")
            t.join(10)
        finally:
            if t.is_alive():  # fail-closed cleanup on assertion failures
                _tap(f"tgp:a:deny:{rid}:{digest[:8]}")
                t.join(10)
        self.assertFalse(t.is_alive())
        self.assertEqual(box, [("decision", rid, digest, "once")])
        self.assertEqual(query.edits[0][0], "✅ Одобрено (одноразово)")
        self.assertNotIn(rid, _STATE.data.get(mod._APPROVALS_KEY, {}))
        self.assertNotIn(rid, mod._APPROVAL_WAITERS)

    def test_limited_choices_hide_session_and_always(self):
        rid, digest = _rid(3), _digest(3)
        request = _FakeRequest(rid, digest, allowed=("once", "deny"))
        t, box = self._run_present(request)
        try:
            self.assertTrue(_wait_for_entry(rid))
            self.assertTrue(_wait_until(lambda: _BOT.sends))
            rows = _BOT.sends[0]["reply_markup"].rows
            self.assertEqual(len(rows), 1)
            self.assertEqual([b.text for b in rows[0]],
                             ["✅ Одобрить", "❌ Отклонить"])
        finally:
            _tap(f"tgp:a:deny:{rid}:{digest[:8]}")
            t.join(10)
        self.assertEqual(box, [("decision", rid, digest, "deny")])

    def test_timeout_fails_closed_to_deny(self):
        rid, digest = _rid(4), _digest(4)
        request = _FakeRequest(rid, digest, timeout_seconds=1)
        t, box = self._run_present(request)
        t.join(15)
        self.assertFalse(t.is_alive())
        self.assertEqual(box, [("decision", rid, digest, "deny")])
        self.assertNotIn(rid, _STATE.data.get(mod._APPROVALS_KEY, {}))
        # the expired prompt was withdrawn on the loop thread
        deadline = time.time() + 3
        while time.time() < deadline and not _BOT.edits:
            time.sleep(0.05)
        self.assertTrue(_BOT.edits, "expected a timeout edit_message_text")
        self.assertIn("⌛️", _BOT.edits[0]["text"])
        self.assertEqual(_BOT.edits[0]["chat_id"], "7559860199")

    def test_send_failure_fails_closed_to_deny(self):
        rid, digest = _rid(5), _digest(5)
        _BOT.fail_send = True
        request = _FakeRequest(rid, digest, timeout_seconds=30)
        t, box = self._run_present(request)
        t.join(10)
        self.assertFalse(t.is_alive())
        self.assertEqual(box, [("decision", rid, digest, "deny")])
        self.assertNotIn(rid, _STATE.data.get(mod._APPROVALS_KEY, {}))
        self.assertNotIn(rid, mod._APPROVAL_WAITERS)

    def test_no_wiring_fails_closed_to_deny(self):
        mod._WIRE_LOOP = None
        try:
            request = _FakeRequest(_rid(6), _digest(6), timeout_seconds=30)
            decision = mod._approval_present(request)
            self.assertEqual(decision[3], "deny")
        finally:
            mod._WIRE_LOOP = _LOOP.loop

    def test_map_entry_shape(self):
        rid, digest = _rid(7), _digest(7)
        request = _FakeRequest(rid, digest)
        t, box = self._run_present(request)
        try:
            self.assertTrue(_wait_for_entry(rid))
            self.assertTrue(_wait_until(
                lambda: (_STATE.data[mod._APPROVALS_KEY].get(rid) or {})
                .get("message_id")))
            entry = _STATE.data[mod._APPROVALS_KEY][rid]
            self.assertEqual(entry["digest"], digest)
            self.assertEqual(entry["allowed"],
                             ["once", "session", "always", "deny"])
            self.assertEqual(entry["chat_id"], "7559860199")
            self.assertTrue(entry["message_id"])
            self.assertGreater(entry["expires"], time.time())
        finally:
            _tap(f"tgp:a:deny:{rid}:{digest[:8]}")
            t.join(10)


@unittest.skipUnless(_HAS_APPROVALS,
                     "approval transport block not present in this plugin copy")
class ApprovalCallbackTests(unittest.TestCase):
    """_handle_approval_callback validation paths."""

    def setUp(self):
        _reset_all()

    def _seed_entry(self, rid, digest, allowed=("once", "session", "always",
                                                "deny"), expires=None):
        _STATE.data[mod._APPROVALS_KEY] = {
            rid: {"digest": digest, "allowed": list(allowed),
                  "chat_id": "7559860199", "thread_id": 65008,
                  "message_id": 777,
                  "expires": expires if expires is not None
                  else time.time() + 60}}
        waiter = {"event": threading.Event(), "request": None, "choice": None}
        mod._APPROVAL_WAITERS[rid] = waiter
        return waiter

    def test_digest_mismatch_is_refused_and_unresolved(self):
        rid, digest = _rid(), _digest()
        waiter = self._seed_entry(rid, digest)
        query = _tap(f"tgp:a:once:{rid}:{'0' * 8}")
        self.assertFalse(waiter["event"].is_set())
        self.assertIsNone(waiter["choice"])
        self.assertEqual(query.edits, [])  # prompt untouched
        self.assertTrue(query.answers)     # refusal toast
        # entry survives: a valid tap may still answer
        _tap(f"tgp:a:once:{rid}:{digest[:8]}")
        self.assertEqual(waiter["choice"], "once")

    def test_expired_request_is_ignored_and_dropped(self):
        rid, digest = _rid(2), _digest(2)
        waiter = self._seed_entry(rid, digest, expires=time.time() - 5)
        query = _tap(f"tgp:a:once:{rid}:{digest[:8]}")
        self.assertFalse(waiter["event"].is_set())
        self.assertIn("⌛️", query.edits[0][0])
        self.assertNotIn(rid, _STATE.data.get(mod._APPROVALS_KEY, {}))

    def test_disallowed_choice_is_refused(self):
        rid, digest = _rid(3), _digest(3)
        waiter = self._seed_entry(rid, digest, allowed=("once", "deny"))
        query = _tap(f"tgp:a:always:{rid}:{digest[:8]}")
        self.assertFalse(waiter["event"].is_set())
        self.assertEqual(query.edits, [])
        self.assertTrue(query.answers)
        self.assertIn(rid, _STATE.data[mod._APPROVALS_KEY])

    def test_unknown_request_id_answers_stale(self):
        rid, digest = _rid(4), _digest(4)
        query = _tap(f"tgp:a:once:{rid}:{digest[:8]}")
        self.assertEqual(len(query.edits), 1)
        self.assertIn("устарел", query.edits[0][0])

    def test_missing_waiter_closes_the_entry(self):
        rid, digest = _rid(5), _digest(5)
        self._seed_entry(rid, digest)
        mod._APPROVAL_WAITERS.clear()  # simulate restart/late cleanup
        query = _tap(f"tgp:a:once:{rid}:{digest[:8]}")
        self.assertIn("уже закрыт", query.edits[0][0])
        self.assertNotIn(rid, _STATE.data.get(mod._APPROVALS_KEY, {}))


@unittest.skipUnless(_HAS_APPROVALS,
                     "approval transport block not present in this plugin copy")
class ApprovalPruneAndRegisterTests(unittest.TestCase):
    """approvals_map pruning and register() wiring."""

    def setUp(self):
        _reset_all()

    def test_prune_drops_only_long_expired_entries(self):
        rid_old, rid_live = _rid(), _rid(2)
        _STATE.data[mod._APPROVALS_KEY] = {
            rid_old: {"digest": _digest(), "expires": time.time() - 120},
            rid_live: {"digest": _digest(2), "expires": time.time() + 60},
        }
        mod._APPROVAL_WAITERS[rid_old] = {"event": threading.Event()}
        mod._approval_prune(time.time())
        amap = _STATE.data[mod._APPROVALS_KEY]
        self.assertNotIn(rid_old, amap)
        self.assertIn(rid_live, amap)
        self.assertNotIn(rid_old, mod._APPROVAL_WAITERS)

    def test_register_wires_the_transport(self):
        recorded = {"transports": []}

        class _Ctx:
            def register_command(self, name, handler=None, **kw):
                pass

            def register_hook(self, name, handler=None, **kw):
                pass

            def register_platform_handler(self, name, handler=None, **kw):
                pass

            def register_approval_transport(self, name, fn=None, **kw):
                recorded["transports"].append((name, fn))

        with tempfile.TemporaryDirectory() as tmp:
            with _with_env(HERMES_HOME=tmp):
                mod.register(_Ctx())
        self.assertEqual(recorded["transports"],
                         [("tg-topics", mod._approval_present)])


@unittest.skipUnless(_HAS_APPROVALS,
                     "approval transport block not present in this plugin copy")
class ApprovalButtonDispatchTests(unittest.TestCase):
    """The tgp:a: branch inside _tg_on_button dispatches to the handler."""

    def setUp(self):
        _reset_all()

    def test_tg_on_button_routes_approval_tap(self):
        rid, digest = _rid(), _digest()
        request = _FakeRequest(rid, digest)
        box = []

        def _go():
            box.append(mod._approval_present(request))

        t = threading.Thread(target=_go, daemon=True, name="test-dispatch")
        t.start()
        try:
            self.assertTrue(_wait_for_entry(rid))

            class _Adapter:
                def _callback_ctx(self, query):
                    return {}

                async def _callback_authorized(self, query, cb, msg):
                    return True

                def _accept_update(self):
                    pass

            mod._ADAPTER = _Adapter()
            try:
                query = _FakeQuery(f"tgp:a:once:{rid}:{digest[:8]}")
                update = types.SimpleNamespace(callback_query=query)
                asyncio.run(mod._tg_on_button(update, None))
            finally:
                mod._ADAPTER = None
            t.join(10)
        finally:
            if t.is_alive():
                mod._APPROVAL_WAITERS.pop(rid, None)
                t.join(10)
        self.assertFalse(t.is_alive())
        self.assertEqual(box, [("decision", rid, digest, "once")])
        self.assertEqual(query.edits[0][0], "✅ Одобрено (одноразово)")


if __name__ == "__main__":
    unittest.main(verbosity=2)
