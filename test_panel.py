"""Stub-based unit checks for the tg-projects topic panel (tgp:pb:*).

Run:  python3 /home/meow/.hermes/plugins/tg-projects/test_panel.py
or:   TGP_PLUGIN_DIR=/path/to/plugin python3 test_panel.py
No network, no real Telegram messages. Stubs replace the hermes core modules
(yaml, tools.terminal_tool, hermes_cli.projects_db, gateway.session_context,
gateway.slash_access, gateway.platforms.event, telegram) and ``hermes_state``
BEFORE the plugin loads, so the checks exercise only the plugin's own logic:
panel text rendering, topic_panels bookkeeping in state.json, panel recreation
(unpin + delete + send + pin), tgp:pb:* callback routing through _tg_on_button
(stub query records edit_message_text calls), and the /menu command.

Skipped (not failed) when the loaded copy has no panel code yet, so the repo's
full pytest run stays green until the integrated __init__.py is deployed.
"""
import asyncio
import importlib.util
import os
import sqlite3
import sys
import tempfile
import time
import types
import unittest
from contextlib import contextmanager
from pathlib import Path

PLUGIN_DIR = Path(os.environ.get("TGP_PLUGIN_DIR",
                                 "/home/meow/.hermes/plugins/tg-projects"))


# ----------------------------------------------------------------- core stubs
fake_yaml = types.ModuleType("yaml")
fake_yaml.safe_load = staticmethod(lambda text: {})

fake_terminal = types.ModuleType("tools.terminal_tool")
fake_terminal._calls = []
fake_terminal._cwd = {}
fake_terminal._session_cwd = {}
fake_terminal.register_task_env_overrides = (
    lambda task_id, overrides: (fake_terminal._calls.append(("register", task_id, dict(overrides))),
                                fake_terminal._cwd.update({task_id: overrides})))
fake_terminal.record_session_cwd = (
    lambda session_key, cwd: (fake_terminal._calls.append(("record", session_key, cwd)),
                              fake_terminal._session_cwd.update({session_key: cwd})))


class _Project:
    def __init__(self, id, name, slug, primary_path):
        self.id, self.name, self.slug = id, name, slug
        self.primary_path = primary_path
        self.folders = []


PROJECTS = [
    _Project("p1", "NeiroSlop", "neiroslop", "/mnt/mydisk/comfyanonymous)"),
    _Project("p2", "Ambrozia", "ambrozia", str(PLUGIN_DIR)),
]

fake_projects = types.ModuleType("hermes_cli.projects_db")


class _FakeProjectsConn:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


fake_projects.connect_closing = _FakeProjectsConn
fake_projects.list_projects = lambda conn: list(PROJECTS)

fake_session_ctx = types.ModuleType("gateway.session_context")
_ENV = {}
fake_session_ctx.get_session_env = lambda name, default="": _ENV.get(name, default)

fake_slash_access = types.ModuleType("gateway.slash_access")
fake_slash_access.policy_for_runner_source = lambda runner, source: None


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


class _MessageEvent:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


fake_platforms_event = types.ModuleType("gateway.platforms.event")
fake_platforms_event.MessageEvent = _MessageEvent
fake_platforms_event.MessageType = types.SimpleNamespace(TEXT="text")


class _FakeStateDB:
    instances = []
    last_error = None

    def __init__(self, db_path=None, read_only=False):
        self.closed = False
        _FakeStateDB.instances.append(self)

    def update_session_cwd(self, session_id, cwd, **kw):
        if _FakeStateDB.last_error is not None:
            raise _FakeStateDB.last_error
        row = _FAKE_STATE.conn.execute(
            "SELECT 1 FROM sessions WHERE id = ?", (session_id,)).fetchone()
        if row is None:
            return None
        _FAKE_STATE.conn.execute(
            "UPDATE sessions SET cwd = ? WHERE id = ?", (cwd, session_id))
        _FAKE_STATE.conn.commit()
        return 1

    def close(self):
        self.closed = True


fake_hermes_state = types.ModuleType("hermes_state")
fake_hermes_state.SessionDB = _FakeStateDB

_MODULES = {
    "yaml": fake_yaml,
    "tools": types.ModuleType("tools"),
    "tools.terminal_tool": fake_terminal,
    "hermes_cli": types.ModuleType("hermes_cli"),
    "hermes_cli.projects_db": fake_projects,
    "gateway": types.ModuleType("gateway"),
    "gateway.session_context": fake_session_ctx,
    "gateway.slash_access": fake_slash_access,
    "gateway.platforms": types.ModuleType("gateway.platforms"),
    "gateway.platforms.event": fake_platforms_event,
    "telegram": fake_telegram,
    "telegram.ext": fake_telegram_ext,
    "hermes_state": fake_hermes_state,
}


def _install_core_stubs():
    """Hard-install this file's stubs; returns the sibling state to restore.

    Nothing is registered at import time: under pytest every test module
    imports BEFORE any test runs, and a setdefault here would win the race
    against sibling files (test_sessions imports after this one) and hand
    THEM these stubs. The plugin module has no core imports at module level,
    so the per-test swap is enough for its lazy ``_import_hermes_module``
    calls. Sibling stubs (test_sessions' own) are restored afterwards so
    their assertions keep working in any order."""
    saved = {name: sys.modules.get(name) for name in _MODULES}
    for name, module in _MODULES.items():
        sys.modules[name] = module
    sys.modules["tools"].terminal_tool = fake_terminal
    sys.modules["hermes_cli"].projects_db = fake_projects
    sys.modules["gateway"].session_context = fake_session_ctx
    sys.modules["gateway"].slash_access = fake_slash_access
    sys.modules["gateway"].platforms = _MODULES["gateway.platforms"]
    sys.modules["gateway"].platforms.event = fake_platforms_event
    sys.modules["telegram"].ext = fake_telegram_ext
    return saved


def _restore_core_stubs(saved):
    for name, module in saved.items():
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module


# in-memory state.db (sessions + messages + turn leases)
class _FakeStateDBSchema:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            "CREATE TABLE sessions (id TEXT PRIMARY KEY, source TEXT, cwd TEXT,"
            " started_at REAL, message_count INTEGER, title TEXT, chat_id TEXT,"
            " thread_id TEXT, ended_at REAL, model TEXT,"
            " input_tokens INTEGER DEFAULT 0, output_tokens INTEGER DEFAULT 0)")
        self.conn.execute(
            "CREATE TABLE messages (session_id TEXT, role TEXT, content TEXT)")
        self.conn.execute(
            "CREATE TABLE session_turn_leases (conversation_id TEXT PRIMARY KEY,"
            " holder TEXT NOT NULL, acquired_at REAL NOT NULL,"
            " expires_at REAL NOT NULL)")
        self.conn.commit()


_FAKE_STATE = _FakeStateDBSchema()


def _open_state_db_stub():
    # A close-immune proxy over the shared in-memory db: panel flows close their
    # state connections in finally blocks, and a raw sqlite3 conn would die for
    # every later test.
    return _FakeConnProxy(_FAKE_STATE.conn)


class _FakeConnProxy:
    def __init__(self, conn):
        self._conn = conn
        self.row_factory = sqlite3.Row

    def execute(self, sql, params=()):
        return self._conn.execute(sql, params)

    def close(self):
        pass


spec = importlib.util.spec_from_file_location(
    "tg_projects_panel_under_test", str(PLUGIN_DIR / "__init__.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
mod._open_state_db = _open_state_db_stub

_HAS_PANEL = hasattr(mod, "_pb_panel_text")

_STATE_DATA = {"pending_cwd": {}, "thread_to_project": {}}
mod._load_state = lambda: dict(_STATE_DATA)


def _save_state_stub(state):
    _STATE_DATA.clear()
    _STATE_DATA.update(state)


mod._save_state = _save_state_stub


@contextmanager
def _with_env(**env):
    """Bind session-context env values for the duration of the block."""
    saved = {k: os.environ.get(k) for k in env}
    _ENV.update(env)
    for k, v in env.items():
        os.environ[k] = v
    mod._CWD_REGISTERED.clear()
    mod._CWD_APPLIED.clear()
    try:
        yield
    finally:
        for k in env:
            _ENV.pop(k, None)
            if saved[k] is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = saved[k]
        mod._CWD_REGISTERED.clear()
        mod._CWD_APPLIED.clear()


def _reset_fake_db():
    conn = _FAKE_STATE.conn
    conn.execute("DELETE FROM messages")
    conn.execute("DELETE FROM sessions")
    conn.execute("DELETE FROM session_turn_leases")
    conn.commit()
    _FakeSessionDB_instances_reset()


def _FakeSessionDB_instances_reset():
    _FakeStateDB.instances.clear()
    _FakeStateDB.last_error = None


def _reset_state():
    _STATE_DATA.clear()
    _STATE_DATA.update({"pending_cwd": {}, "thread_to_project": {}})


class _StubbedTestCase(unittest.TestCase):
    """Core stubs installed per test (never at import — see _install_core_stubs)."""

    def setUp(self):
        self._saved_stubs = _install_core_stubs()
        self.addCleanup(_restore_core_stubs, self._saved_stubs)


CHAT = "7559860199"
THREAD = 65008
KEY = f"{CHAT}:{THREAD}"
SID = "20261003_062445_2d0fd7"
CWD = "/mnt/mydisk/comfyanonymous)"


def _binding_entry(session_id=None):
    return {"project_id": "p1", "project_name": "NeiroSlop", "cwd": CWD,
            "session_id": session_id, "updated_at": 1791300571}


class FakeQuery:
    """A CallbackQuery stub: records edits and answers."""

    def __init__(self, data, chat_id=int(CHAT), thread_id=THREAD):
        self.data = data
        self.message = types.SimpleNamespace(
            chat_id=chat_id, message_thread_id=thread_id,
            message_id=777,
            chat=types.SimpleNamespace(id=chat_id, type="dm", is_forum=True),
            from_user=types.SimpleNamespace(id=int(CHAT), is_bot=False,
                                            username="meow", first_name="Meow"),
        )
        self.edits = []
        self.answers = []

    async def edit_message_text(self, text, reply_markup=None, **kw):
        self.edits.append((text, reply_markup))

    async def answer(self, text=None, **kw):
        self.answers.append(text)


class FakeBot:
    """A PTB bot stub for panel send/pin/unpin/delete."""

    def __init__(self, fail_send=False):
        self.sent, self.pinned, self.unpinned, self.deleted = [], [], [], []
        self.fail_send = fail_send
        self._next_id = 500

    async def send_message(self, **kwargs):
        if self.fail_send:
            raise RuntimeError("telegram down")
        self.sent.append(kwargs)
        self._next_id += 1
        return types.SimpleNamespace(message_id=self._next_id)

    async def pin_chat_message(self, **kwargs):
        self.pinned.append(kwargs)

    async def unpin_chat_message(self, **kwargs):
        self.unpinned.append(kwargs)

    async def delete_message(self, **kwargs):
        self.deleted.append(kwargs)


class FakeAdapter:
    """The plugin-visible adapter surface used by the panel flows."""

    def __init__(self):
        self.gateway_runner = None
        self.events = []

    def _callback_ctx(self, query):
        return {"chat_id": query.message.chat_id}

    async def _callback_authorized(self, query, cb, text):
        return True

    def _accept_update(self):
        pass

    def _normalize_chat_type(self, chat_type, is_forum=False):
        return "dm"

    def build_source(self, **kwargs):
        return types.SimpleNamespace(**kwargs)

    def _source_from_message_for_auth(self, msg):
        return None

    async def handle_message(self, event):
        self.events.append(event)


def _run(coro):
    return asyncio.run(coro)


def _seed_session(sid, source="telegram", cwd=CWD, started=1791300000.0,
                  count=12, chat=CHAT, thread=str(THREAD), lease_until=None):
    conn = _FAKE_STATE.conn
    conn.execute(
        "INSERT INTO sessions (id, source, cwd, started_at, message_count,"
        " title, chat_id, thread_id) VALUES (?,?,?,?,?,?,?,?)",
        (sid, source, cwd, started, count, "", chat, thread))
    if lease_until is not None:
        conn.execute(
            "INSERT INTO session_turn_leases (conversation_id, holder,"
            " acquired_at, expires_at) VALUES (?,?,?,?)",
            (sid, "pid=42:turn=d", time.time(), lease_until))
    conn.commit()


@unittest.skipUnless(_HAS_PANEL, "deploy copy has no panel code yet")
class PanelStateTests(_StubbedTestCase):
    """topic_panels bookkeeping in state.json."""

    def setUp(self):
        super().setUp()
        _reset_state()

    def test_panel_key_format(self):
        self.assertEqual(mod._pb_panel_key(CHAT, THREAD), f"{CHAT}:{THREAD}")
        self.assertEqual(mod._pb_panel_key(CHAT, None), f"{CHAT}:0")

    def test_set_get_roundtrip(self):
        mod._pb_set_panel_message_id(CHAT, THREAD, 4321)
        self.assertEqual(mod._pb_get_panel_message_id(CHAT, THREAD), 4321)
        self.assertEqual(_STATE_DATA["topic_panels"][KEY], 4321)

    def test_missing_panel_returns_none(self):
        self.assertIsNone(mod._pb_get_panel_message_id(CHAT, THREAD))

    def test_clear_removes_panel_and_empty_bucket(self):
        mod._pb_set_panel_message_id(CHAT, THREAD, 4321)
        mod._pb_clear_panel(CHAT, THREAD)
        self.assertIsNone(mod._pb_get_panel_message_id(CHAT, THREAD))
        self.assertNotIn("topic_panels", _STATE_DATA)

    def test_garbage_panel_value_returns_none(self):
        _STATE_DATA["topic_panels"] = {KEY: "junk"}
        self.assertIsNone(mod._pb_get_panel_message_id(CHAT, THREAD))


@unittest.skipUnless(_HAS_PANEL, "deploy copy has no panel code yet")
class PanelTextTests(_StubbedTestCase):
    """The 4 panel renders."""

    def setUp(self):
        super().setUp()
        _reset_state()
        _reset_fake_db()

    def test_render_no_binding(self):
        text = mod._pb_panel_text(None)
        self.assertEqual(text, "📁  —\n🧵  —\n📂  —\n\nСначала выбери проект: [📁 Проект]")

    def test_render_binding_without_session(self):
        text = mod._pb_panel_text(_binding_entry(None))
        self.assertEqual(text, f"📁  NeiroSlop\n🧵  —\n📂  {CWD}")

    def test_render_binding_with_session_online(self):
        _seed_session(SID, lease_until=time.time() + 300)
        status = mod._pb_session_status(_FAKE_STATE.conn, SID)
        self.assertEqual(status, "online")
        text = mod._pb_panel_text(_binding_entry(SID), status)
        self.assertEqual(text, f"📁  NeiroSlop\n🧵  20261003_062… · online\n📂  {CWD}")

    def test_render_binding_with_session_idle(self):
        _seed_session(SID)  # no lease -> idle
        status = mod._pb_session_status(_FAKE_STATE.conn, SID)
        self.assertEqual(status, "idle")

    def test_sessions_subscreen_text(self):
        _seed_session(SID, lease_until=time.time() + 300)
        _seed_session("20261002_101500_0d0fd7", source="desktop", started=1791200000.0,
                      count=5)
        sessions = mod._sessions_for_cwd(_FAKE_STATE.conn, CWD, limit=mod._PB_SESSIONS_LIMIT)
        self.assertEqual(len(sessions), 2)
        text = mod._pb_sessions_text(_binding_entry(SID), sessions)
        self.assertIn("🧵 Сессии проекта NeiroSlop:", text)
        self.assertIn("• 20261003_062… · online · 12 msg", text)
        self.assertIn("🖥️ • 20261002_101… · idle · 5 msg", text)

    def test_sessions_subscreen_without_project(self):
        self.assertEqual(mod._pb_sessions_text(None, []),
                         "🧵 Сначала выбери проект: [📁 Проект].")

    def test_sessions_subscreen_without_sessions(self):
        text = mod._pb_sessions_text(_binding_entry(None), [])
        self.assertIn("(нет сессий в этом каталоге)", text)


@unittest.skipUnless(_HAS_PANEL, "deploy copy has no panel code yet")
class PanelBindingTests(_StubbedTestCase):
    """Explicit-key binding writes (the callback path has no session env)."""

    def setUp(self):
        super().setUp()
        _reset_state()

    def test_read_binding_by_explicit_key(self):
        _STATE_DATA["topic_bindings"] = {KEY: _binding_entry(SID)}
        binding = mod._pb_binding(CHAT, THREAD)
        self.assertEqual(binding["project_name"], "NeiroSlop")
        self.assertEqual(binding["session_id"], SID)

    def test_read_binding_in_dm_returns_none(self):
        _STATE_DATA["topic_bindings"] = {KEY: _binding_entry(SID)}
        self.assertIsNone(mod._pb_binding(CHAT, None))

    def test_write_binding_resets_session(self):
        _STATE_DATA["topic_bindings"] = {KEY: _binding_entry(SID)}
        self.assertTrue(mod._pb_write_binding(CHAT, THREAD, "p2", "Ambrozia",
                                              str(PLUGIN_DIR), None))
        entry = _STATE_DATA["topic_bindings"][KEY]
        self.assertEqual(entry["project_name"], "Ambrozia")
        self.assertIsNone(entry["session_id"])
        self.assertIn("updated_at", entry)

    def test_write_binding_flat_chat_lands_on_key_zero(self):
        # Topics off: thread None targets the chat's flat lane (<chat>:0).
        self.assertTrue(mod._pb_write_binding(CHAT, None, "p1", "x", "/p", None))
        self.assertEqual(_STATE_DATA["topic_bindings"][f"{CHAT}:0"]["project_id"], "p1")

    def test_write_binding_session_roundtrip(self):
        _STATE_DATA["topic_bindings"] = {KEY: _binding_entry(None)}
        self.assertTrue(mod._pb_write_binding_session(CHAT, THREAD, SID))
        self.assertEqual(_STATE_DATA["topic_bindings"][KEY]["session_id"], SID)
        mod._pb_write_binding_session(CHAT, THREAD, None)
        self.assertIsNone(_STATE_DATA["topic_bindings"][KEY]["session_id"])

    def test_write_binding_session_needs_existing_binding(self):
        self.assertFalse(mod._pb_write_binding_session(CHAT, THREAD, SID))
        self.assertNotIn("topic_bindings", _STATE_DATA)


@unittest.skipUnless(_HAS_PANEL, "deploy copy has no panel code yet")
class PanelCallbackTests(_StubbedTestCase):
    """tgp:pb:* routing through _tg_on_button (no real telegram)."""

    def setUp(self):
        super().setUp()
        _reset_state()
        _reset_fake_db()
        self.adapter = FakeAdapter()
        mod._ADAPTER = self.adapter
        self.addCleanup(self._restore)

    def _restore(self):
        mod._ADAPTER = None
        mod._NATIVE = None

    def _tap(self, data, chat=CHAT, thread=THREAD):
        query = FakeQuery(data, chat_id=int(chat), thread_id=thread)
        update = types.SimpleNamespace(callback_query=query)
        _run(mod._tg_on_button(update, None))
        return query

    def test_back_rerenders_the_panel(self):
        _STATE_DATA["topic_bindings"] = {KEY: _binding_entry(None)}
        query = self._tap("tgp:pb:back")
        self.assertEqual(query.edits[0][0], f"📁  NeiroSlop\n🧵  —\n📂  {CWD}")
        self.assertEqual(query.edits[0][1].rows[0][0].callback_data, "tgp:pb:proj")
        self.assertEqual(query.edits[0][1].rows[1][1].callback_data, "tgp:pb:more")

    def test_back_without_binding_shows_hint(self):
        query = self._tap("tgp:pb:back")
        self.assertIn("Сначала выбери проект", query.edits[0][0])

    def test_proj_screen_lists_projects(self):
        query = self._tap("tgp:pb:proj")
        self.assertIn("📁 Выбор проекта", query.edits[0][0])
        rows = query.edits[0][1].rows
        self.assertEqual(rows[0][0].callback_data, "tgp:pb:projp:1")
        self.assertEqual(rows[1][0].callback_data, "tgp:pb:projp:2")
        self.assertEqual(rows[-1][0].callback_data, "tgp:pb:back")

    def test_projp_pick_writes_binding_and_rerenders(self):
        query = self._tap("tgp:pb:projp:1")
        entry = _STATE_DATA["topic_bindings"][KEY]
        self.assertEqual(entry["project_id"], "p1")
        self.assertEqual(entry["project_name"], "NeiroSlop")
        self.assertEqual(entry["cwd"], CWD)
        self.assertIsNone(entry["session_id"])
        self.assertEqual(query.edits[-1][0], f"📁  NeiroSlop\n🧵  —\n📂  {CWD}")

    def test_projp_stale_index_shows_error(self):
        query = self._tap("tgp:pb:projp:9")
        self.assertIn("больше не в списке", query.edits[0][0])
        self.assertNotIn("topic_bindings", _STATE_DATA)

    def test_sess_screen_without_project(self):
        query = self._tap("tgp:pb:sess")
        self.assertEqual(query.edits[0][0], "🧵 Сначала выбери проект: [📁 Проект].")

    def test_sess_screen_lists_live_sessions_with_buttons(self):
        _STATE_DATA["topic_bindings"] = {KEY: _binding_entry(None)}
        _seed_session(SID, lease_until=time.time() + 300)
        _seed_session("20261002_101500_0d0fd7", source="desktop", count=5)
        query = self._tap("tgp:pb:sess")
        rows = query.edits[0][1].rows
        self.assertEqual(rows[0][0].callback_data, f"tgp:pb:sesss:{SID}")
        self.assertEqual(rows[1][0].callback_data, "tgp:pb:sesss:20261002_101500_0d0fd7")
        self.assertEqual(rows[-2][0].callback_data, "tgp:pb:new")  # ➕ Новая сессия
        self.assertEqual(rows[-1][0].callback_data, "tgp:pb:back")
        self.assertIn("online", query.edits[0][0])
        self.assertIn("🖥️", query.edits[0][0])

    def test_sess_screen_no_sessions_still_offers_new(self):
        _STATE_DATA["topic_bindings"] = {KEY: _binding_entry(None)}
        query = self._tap("tgp:pb:sess")
        rows = query.edits[0][1].rows
        self.assertEqual(rows[0][0].callback_data, "tgp:pb:new")
        self.assertEqual(rows[1][0].callback_data, "tgp:pb:back")
        self.assertIn("нет сессий", query.edits[0][0])

    def test_sesss_pick_sets_session_and_sends_resume(self):
        _STATE_DATA["topic_bindings"] = {KEY: _binding_entry(None)}
        _seed_session(SID)
        with _with_env(HERMES_HOME="/nonexistent-panel-tmp"):
            query = self._tap(f"tgp:pb:sesss:{SID}")
        self.assertEqual(_STATE_DATA["topic_bindings"][KEY]["session_id"], SID)
        self.assertTrue(any(getattr(e, "text", "") == f"/resume {SID}"
                            for e in self.adapter.events))
        # the panel re-renders after the resume
        self.assertEqual(query.edits[-1][0],
                         f"📁  NeiroSlop\n🧵  20261003_062… · idle\n📂  {CWD}")

    def test_sesss_pick_unknown_session(self):
        _STATE_DATA["topic_bindings"] = {KEY: _binding_entry(None)}
        _seed_session(SID)
        query = self._tap("tgp:pb:sesss:20269999_999999_ffffff")
        self.assertIn("не найдена", query.edits[0][0])
        self.assertIsNone(_STATE_DATA["topic_bindings"][KEY]["session_id"])
        self.assertEqual(self.adapter.events, [])

    def test_new_without_project_shows_hint(self):
        query = self._tap("tgp:pb:new")
        self.assertIn("Сначала выбери проект", query.edits[0][0])
        self.assertEqual(self.adapter.events, [])

    def test_new_pins_project_and_sends_new(self):
        _STATE_DATA["topic_bindings"] = {KEY: _binding_entry(None)}
        with _with_env(HERMES_SESSION_KEY="sk-1",
                       HERMES_HOME="/nonexistent-panel-tmp"):
            query = self._tap("tgp:pb:new")
        self.assertEqual(_STATE_DATA["pending_cwd"]["sk-1"]["cwd"], CWD)
        self.assertTrue(any(getattr(e, "text", "") == "/new"
                            for e in self.adapter.events))
        # the panel view (with buttons) is restored after the /new injection
        self.assertEqual(query.edits[-1][0], f"📁  NeiroSlop\n🧵  —\n📂  {CWD}")
        self.assertIsNotNone(query.edits[-1][1])

    def test_stop_sends_stop_command(self):
        _STATE_DATA["topic_bindings"] = {KEY: _binding_entry(SID)}
        query = self._tap("tgp:pb:stop")
        self.assertIn("Останавливаю", query.edits[0][0])
        self.assertTrue(any(getattr(e, "text", "") == "/stop"
                            for e in self.adapter.events))

    def test_stop_without_adapter_shows_note(self):
        mod._ADAPTER = None
        query = FakeQuery("tgp:pb:stop")
        _run(mod._handle_panel_callback(query, "tgp:pb:stop"))
        self.assertIn("недоступна", query.edits[0][0])
        self.assertEqual(self.adapter.events, [])

    def test_more_screen_lists_commands(self):
        query = self._tap("tgp:pb:more")
        datas = [b.callback_data for row in query.edits[0][1].rows for b in row]
        self.assertIn("tgp:pb:more:cmd:status", datas)
        self.assertIn("tgp:pb:more:cmd:diff", datas)
        self.assertIn("tgp:pb:more:cmd:agents", datas)
        self.assertIn("tgp:pb:more:cmd:help", datas)
        self.assertIn("tgp:pb:back", datas)

    def test_more_cmd_sends_the_command(self):
        query = self._tap("tgp:pb:more:cmd:status")
        self.assertIn("/status", query.edits[0][0])
        self.assertTrue(any(getattr(e, "text", "") == "/status"
                            for e in self.adapter.events))

    def test_more_cmd_rejects_unknown_command(self):
        query = self._tap("tgp:pb:more:cmd:rm_rf")
        self.assertEqual(query.edits, [])
        self.assertEqual(self.adapter.events, [])
        self.assertEqual(query.answers[-1], None)  # answered, nothing done

    def test_panel_prefix_does_not_hijack_old_callbacks(self):
        # old flows untouched: tgp:b re-renders the PROJECT LIST (not the panel)
        query = self._tap("tgp:b")
        self.assertEqual(len(query.edits), 1)
        self.assertIn("Проекты", query.edits[0][0])
        self.assertEqual(query.answers, [None])
        # and the panel prefix itself is answered without edits when unknown
        query = self._tap("tgp:pb:zzz")
        self.assertEqual(query.edits, [])
        self.assertEqual(self.adapter.events, [])

    def test_callback_data_fits_64_bytes(self):
        long_id = "a" * 46
        cb = f"tgp:pb:sesss:{long_id}"
        self.assertLessEqual(len(cb.encode("utf-8")), 64)
        self.assertIsNotNone(mod._PB_CB_SESS_RE.match(cb))
        self.assertIsNone(mod._PB_CB_SESS_RE.match("tgp:pb:sesss:" + "a" * 47))


@unittest.skipUnless(_HAS_PANEL, "deploy copy has no panel code yet")
class PanelCreateTests(_StubbedTestCase):
    """Panel recreation: unpin + delete the old message, send, pin, record."""

    def setUp(self):
        super().setUp()
        _reset_state()
        _reset_fake_db()
        self.bot = FakeBot()
        mod._NATIVE = types.SimpleNamespace(bot=self.bot)
        mod._ADAPTER = FakeAdapter()
        self.addCleanup(self._restore)

    def _restore(self):
        mod._ADAPTER = None
        mod._NATIVE = None

    def test_create_sends_pins_and_records(self):
        self.assertTrue(_run(mod._pb_create(CHAT, THREAD)))
        self.assertEqual(len(self.bot.sent), 1)
        kwargs = self.bot.sent[0]
        self.assertEqual(kwargs["chat_id"], CHAT)
        self.assertEqual(kwargs["message_thread_id"], THREAD)
        self.assertEqual(kwargs["text"], "📁  —\n🧵  —\n📂  —\n\nСначала выбери проект: [📁 Проект]")
        self.assertEqual(kwargs["reply_markup"].rows[0][0].callback_data, "tgp:pb:proj")
        new_id = self.bot.pinned[0]["message_id"]
        self.assertEqual(self.bot.pinned[0]["chat_id"], int(CHAT))
        self.assertEqual(self.bot.pinned[0]["disable_notification"], True)
        self.assertEqual(mod._pb_get_panel_message_id(CHAT, THREAD), new_id)
        self.assertEqual(_STATE_DATA["topic_panels"][KEY], new_id)

    def test_recreate_unpins_and_deletes_old_panel(self):
        mod._pb_set_panel_message_id(CHAT, THREAD, 123)
        _STATE_DATA["topic_bindings"] = {KEY: _binding_entry(SID)}
        _seed_session(SID, lease_until=time.time() + 300)
        self.assertTrue(_run(mod._pb_create(CHAT, THREAD)))
        self.assertEqual(self.bot.unpinned, [{"chat_id": int(CHAT), "message_id": 123}])
        self.assertEqual(self.bot.deleted, [{"chat_id": int(CHAT), "message_id": 123}])
        self.assertEqual(len(self.bot.sent), 1)
        self.assertEqual(self.bot.sent[0]["text"],
                         f"📁  NeiroSlop\n🧵  20261003_062… · online\n📂  {CWD}")
        self.assertNotEqual(mod._pb_get_panel_message_id(CHAT, THREAD), 123)
        self.assertEqual(self.bot.pinned[0]["message_id"],
                         mod._pb_get_panel_message_id(CHAT, THREAD))

    def test_create_without_old_panel_does_not_unpin(self):
        self.assertTrue(_run(mod._pb_create(CHAT, THREAD)))
        self.assertEqual(self.bot.unpinned, [])
        self.assertEqual(self.bot.deleted, [])

    def test_create_in_dm_omits_thread_kwarg(self):
        self.assertTrue(_run(mod._pb_create(CHAT, None)))
        self.assertNotIn("message_thread_id", self.bot.sent[0])
        self.assertEqual(_STATE_DATA["topic_panels"][f"{CHAT}:0"],
                         self.bot.pinned[0]["message_id"])

    def test_send_failure_returns_false_and_clears(self):
        mod._pb_set_panel_message_id(CHAT, THREAD, 123)
        mod._NATIVE = types.SimpleNamespace(bot=FakeBot(fail_send=True))
        self.assertFalse(_run(mod._pb_create(CHAT, THREAD)))
        self.assertIsNone(mod._pb_get_panel_message_id(CHAT, THREAD))

    def test_create_without_bot_returns_false(self):
        mod._NATIVE = None
        self.assertFalse(_run(mod._pb_create(CHAT, THREAD)))


@unittest.skipUnless(_HAS_PANEL, "deploy copy has no panel code yet")
class MenuCommandTests(_StubbedTestCase):
    """/menu: ensure the panel in the current chat/topic."""

    def setUp(self):
        super().setUp()
        _reset_state()
        _reset_fake_db()
        self.bot = FakeBot()
        mod._NATIVE = types.SimpleNamespace(bot=self.bot)
        mod._ADAPTER = FakeAdapter()
        self.addCleanup(self._restore)

    def _restore(self):
        mod._ADAPTER = None
        mod._NATIVE = None

    def test_menu_creates_panel_in_topic(self):
        _STATE_DATA["topic_bindings"] = {KEY: _binding_entry(None)}
        with _with_env(HERMES_SESSION_CHAT_ID=CHAT,
                       HERMES_SESSION_THREAD_ID=str(THREAD)):
            result = _run(mod._menu_command(""))
        self.assertIsNone(result)
        self.assertEqual(len(self.bot.sent), 1)
        self.assertEqual(self.bot.sent[0]["message_thread_id"], THREAD)
        self.assertEqual(self.bot.sent[0]["text"], f"📁  NeiroSlop\n🧵  —\n📂  {CWD}")
        self.assertEqual(self.bot.pinned[0]["chat_id"], int(CHAT))
        self.assertEqual(_STATE_DATA["topic_panels"][KEY],
                         self.bot.pinned[0]["message_id"])

    def test_menu_in_unbound_topic_shows_hint_panel(self):
        with _with_env(HERMES_SESSION_CHAT_ID=CHAT,
                       HERMES_SESSION_THREAD_ID="65099"):
            result = _run(mod._menu_command(""))
        self.assertIsNone(result)
        self.assertIn("Сначала выбери проект", self.bot.sent[0]["text"])

    def test_menu_recreates_over_old_panel(self):
        mod._pb_set_panel_message_id(CHAT, THREAD, 123)
        with _with_env(HERMES_SESSION_CHAT_ID=CHAT,
                       HERMES_SESSION_THREAD_ID=str(THREAD)):
            result = _run(mod._menu_command(""))
        self.assertIsNone(result)
        self.assertEqual(self.bot.unpinned[0]["message_id"], 123)
        self.assertEqual(self.bot.deleted[0]["message_id"], 123)
        self.assertNotEqual(mod._pb_get_panel_message_id(CHAT, THREAD), 123)

    def test_menu_with_args_returns_usage(self):
        self.assertEqual(_run(mod._menu_command("x")),
                         "Использование: /menu — без аргументов.")

    def test_menu_without_chat_returns_text(self):
        with _with_env(HERMES_SESSION_CHAT_ID="", HERMES_SESSION_THREAD_ID=""):
            result = _run(mod._menu_command(""))
        self.assertIn("не удалось определить чат", result)
        self.assertEqual(self.bot.sent, [])

    def test_menu_without_bot_returns_text(self):
        mod._NATIVE = None
        with _with_env(HERMES_SESSION_CHAT_ID=CHAT, HERMES_SESSION_THREAD_ID=""):
            result = _run(mod._menu_command(""))
        self.assertIn("недоступна", result)
        self.assertEqual(self.bot.sent, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
