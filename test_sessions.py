"""Stub-based unit checks for tg-projects phone-side session management.

Run:  python3 /home/meow/.hermes/plugins/tg-projects/test_sessions.py
No network, no real Telegram messages, no getUpdates. Stubs replace the hermes
core modules (yaml, tools.terminal_tool, hermes_cli.projects_db,
gateway.session_context, gateway.slash_access) and ``telegram`` BEFORE the plugin
loads, so the checks exercise the plugin's own logic: /pproject parsing,
thread_id -> project mapping in state.json (state.json, never config.yaml),
cross-origin session detection, /resume --all admin decision, callback_data
bounds, and the model-picker thread resolution.

A fake in-memory state.db is injected via ``_open_state_db`` so the session
queries run against controlled rows (source='desktop' vs 'telegram'), never the
real ~/.hermes/state.db.
"""
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

PLUGIN_DIR = Path(os.environ.get("TGP_PLUGIN_DIR")
                  or Path(__file__).resolve().parent)


# ----------------------------------------------------------------- core stubs
fake_yaml = types.ModuleType("yaml")


def _fake_safe_load(text):
    return {
        "platforms": {"telegram": {"extra": {"dm_topics": [
            {"chat_id": 7559860199, "topics": [
                {"name": "Ambrozia", "thread_id": 64999},
                {"name": "default", "thread_id": 65001},
            ]},
        ]}}}
    }


fake_yaml.safe_load = staticmethod(_fake_safe_load)

fake_terminal = types.ModuleType("tools.terminal_tool")
fake_terminal._calls = []
fake_terminal._cwd = {}
fake_terminal._session_cwd = {}


def _register(task_id, overrides):
    fake_terminal._calls.append(("register", task_id, dict(overrides)))
    fake_terminal._cwd[task_id] = overrides


def _record(session_key, cwd):
    fake_terminal._calls.append(("record", session_key, cwd))
    fake_terminal._session_cwd[session_key] = cwd


fake_terminal.register_task_env_overrides = _register
fake_terminal.record_session_cwd = _record

fake_projects = types.ModuleType("hermes_cli.projects_db")


class _Project:
    def __init__(self, id, name, slug, primary_path):
        self.id, self.name, self.slug = id, name, slug
        self.primary_path = primary_path
        self.folders = []


PROJECTS = [
    _Project("p1", "Ambrozia", "ambrozia", str(PLUGIN_DIR)),
    _Project("p2", "default", "default", str(PLUGIN_DIR)),
]


class _FakeProjectsConn:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _connect_closing():
    return _FakeProjectsConn()


def _list_projects(conn):
    return list(PROJECTS)


fake_projects.connect_closing = _connect_closing
fake_projects.list_projects = _list_projects

fake_session_ctx = types.ModuleType("gateway.session_context")
_ENV = {}


def _get_session_env(name, default=""):
    return _ENV.get(name, default)


fake_session_ctx.get_session_env = _get_session_env

# gateway.slash_access stub: mirrors the real SlashAccessPolicy — enabled means
# an allow_admin_from list is set for the scope (gating active); when it is not
# set, is_admin() is True for every caller (gating disabled, #121705 semantics).
ADMIN_CALLS = []


class _Policy:
    def __init__(self, enabled, admins=()):
        self.enabled = enabled
        self._admins = set(admins)

    def is_admin(self, uid):
        ADMIN_CALLS.append(uid)
        # Gating disabled -> everyone is admin, so callers can use is_admin/
        # can_run uniformly. Mirrors gateway.slash_access.SlashAccessPolicy.
        if not self.enabled:
            return True
        return bool(uid) and str(uid) in self._admins


def _policy_for_runner_source(runner, source):
    admins = getattr(runner, "_admins", ())
    # enabled == "allow_admin_from list is set": an ungated platform has no
    # admin list, so every caller passes is_admin — the core's own semantics.
    enabled = bool(admins)
    return _Policy(enabled, admins)


fake_slash_access = types.ModuleType("gateway.slash_access")
fake_slash_access.policy_for_runner_source = _policy_for_runner_source


# telegram stub: the plugin imports InlineKeyboardButton/Markup inside functions
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


# in-memory state.db with sessions + messages
class _FakeStateDB:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            "CREATE TABLE sessions (id TEXT PRIMARY KEY, source TEXT, cwd TEXT,"
            " started_at REAL, message_count INTEGER, title TEXT, chat_id TEXT,"
            " thread_id TEXT, ended_at REAL, model TEXT,"
            " input_tokens INTEGER DEFAULT 0, output_tokens INTEGER DEFAULT 0,"
            " last_activity_at REAL)")
        self.conn.execute(
            "CREATE TABLE messages (session_id TEXT, role TEXT, content TEXT)")
        self.conn.execute(
            "CREATE TABLE session_turn_leases (conversation_id TEXT PRIMARY KEY,"
            " holder TEXT NOT NULL, acquired_at REAL NOT NULL,"
            " expires_at REAL NOT NULL)")
        self.conn.commit()

    def seed(self, sid, source, cwd, started_at, count, title="",
             chat_id="", thread_id=""):
        self.conn.execute(
            "INSERT INTO sessions (id, source, cwd, started_at, message_count,"
            " title, chat_id, thread_id) VALUES (?,?,?,?,?,?,?,?)",
            (sid, source, cwd, started_at, count, title, chat_id, thread_id))

    def seed_message(self, sid, content):
        self.conn.execute(
            "INSERT INTO messages (session_id, role, content) VALUES (?,?,?)",
            (sid, "user", content))

    def conn_for_row_factory(self):
        conn = sqlite3.connect(self.conn)  # pragma: no cover - unused
        conn.close()
        return self.conn


_FAKE_STATE = _FakeStateDB()


def _open_state_db_stub():
    # A close-immune proxy over the shared in-memory db: plugin flows close
    # their state connections in finally blocks, and a raw sqlite3 conn would
    # die for every later test.
    return _FakeConnProxy(_FAKE_STATE.conn)


class _FakeConnProxy:
    def __init__(self, conn):
        self._conn = conn
        self.row_factory = sqlite3.Row

    def execute(self, sql, params=()):
        return self._conn.execute(sql, params)

    def close(self):
        pass


# ---------------------------------------------------------------- hermes_state stub
# The plugin opens its own one-shot SessionDB handle to persist sessions.cwd so
# desktop surfaces can find Telegram sessions by project directory.  A fake
# SessionDB records calls and consults _FAKE_STATE for which rows exist, so the
# "no row yet -> retry next turn" path is testable without touching the real
# ~/.hermes/state.db.
class _FakeSessionDB:
    instances = []
    last_error = None
    last_close = None

    def __init__(self, db_path=None, read_only=False):
        self.db_path = db_path
        self.read_only = read_only
        self.closed = False
        _FakeSessionDB.instances.append(self)

    def update_session_cwd(self, session_id, cwd, **kw):
        if _FakeSessionDB.last_error is not None:
            raise _FakeSessionDB.last_error
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
        _FakeSessionDB.last_close = self


fake_hermes_state = types.ModuleType("hermes_state")
fake_hermes_state.SessionDB = _FakeSessionDB


_MODULES = {
    "yaml": fake_yaml,
    "tools": types.ModuleType("tools"),
    "tools.terminal_tool": fake_terminal,
    "hermes_cli": types.ModuleType("hermes_cli"),
    "hermes_cli.projects_db": fake_projects,
    "gateway": types.ModuleType("gateway"),
    "gateway.session_context": fake_session_ctx,
    "gateway.slash_access": fake_slash_access,
    "telegram": fake_telegram,
    "telegram.ext": fake_telegram_ext,
    "hermes_state": fake_hermes_state,
}
for _name, _mod in _MODULES.items():
    sys.modules.setdefault(_name, _mod)
sys.modules["tools"].terminal_tool = fake_terminal
sys.modules["hermes_cli"].projects_db = fake_projects
sys.modules["gateway"].session_context = fake_session_ctx
sys.modules["gateway"].slash_access = fake_slash_access
sys.modules["telegram"].ext = fake_telegram_ext

spec = importlib.util.spec_from_file_location(
    "tg_projects_sessions_under_test", str(PLUGIN_DIR / "__init__.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
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
_STATE.install()


@contextmanager
def _with_env(**env):
    """Set session-context AND os.environ values for the duration of the block.

    The plugin reads session context first and os.environ second (``_session_env``),
    and HERMES_HOME comes straight from os.environ — patching both keeps the two
    consistent and means no test ever touches the real ~/.hermes/state.db.
    """
    saved = {k: os.environ.get(k) for k in env}
    _ENV.update(env)
    for k, v in env.items():
        os.environ[k] = v
    mod._CWD_REGISTERED.clear()
    mod._CWD_APPLIED.clear()
    fake_terminal._calls.clear()
    fake_terminal._cwd.clear()
    fake_terminal._session_cwd.clear()
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
        fake_terminal._calls.clear()


def _has_table(conn, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _reset_fake_db():
    conn = _FAKE_STATE.conn
    conn.execute("DELETE FROM messages")
    conn.execute("DELETE FROM sessions")
    if _has_table(conn, "session_turn_leases"):
        conn.execute("DELETE FROM session_turn_leases")
    conn.commit()
    _FakeSessionDB.instances.clear()
    _FakeSessionDB.last_error = None
    _FakeSessionDB.last_close = None


def _reset_state():
    _STATE.data = {"pending_cwd": {}, "thread_to_project": {}}


def asyncio_run(coro):
    import asyncio as _asyncio
    return _asyncio.run(coro)


class FakeSource:
    def __init__(self, user_id="111"):
        self.user_id = user_id
        self.chat_type = "dm"  # resolves the dm scope, like the real telegram source


class FakeRunner:
    def __init__(self, admin_gate=True, admins=()):
        self.config = types.SimpleNamespace(multiplex_profiles=False, platforms={})
        self._admin_gate = admin_gate
        self._admins = admins


class FakeAdapter:
    def __init__(self, runner):
        self.gateway_runner = runner


# ---------------------------------------------------------------------- tests
class CallbackDataTests(unittest.TestCase):
    """callback_data <= 64 bytes, and tgp:* parsing."""

    def test_all_project_callbacks_fit_the_64_byte_cap(self):
        max_index = str(999)
        for cb in [f"{mod.CB_PREFIX}p:{max_index}",
                   f"{mod.CB_PREFIX}n:{max_index}",
                   f"{mod.CB_PREFIX}r:{max_index}",
                   f"{mod.CB_PREFIX}l:{max_index}",
                   f"{mod.CB_PREFIX}a:{max_index}",
                   f"{mod.CB_PREFIX}d:{max_index}",
                   "tgp:np", mod._BACK_CB]:
            self.assertLessEqual(len(cb.encode("utf-8")), 64, cb)

    def test_session_callback_with_realistic_id_fits(self):
        sid = "20261003_062445_2d0fd7"
        cb = f"{mod.CB_PREFIX}s:{sid}"
        self.assertLessEqual(len(cb.encode("utf-8")), 64, cb)
        m = mod._CB_SESSION_RE.match(cb)
        self.assertEqual(m.group(1), sid)

    def test_session_callback_rejects_short_ids(self):
        self.assertIsNone(mod._CB_SESSION_RE.match("tgp:s:abc"))
        self.assertIsNone(mod._CB_SESSION_RE.match("tgp:s:"))

    def test_session_callback_rejects_injection(self):
        self.assertIsNone(mod._CB_SESSION_RE.match("tgp:s:x" * 20))
        self.assertIsNone(mod._CB_SESSION_RE.match("tgp:s:a b"))
        self.assertIsNone(mod._CB_SESSION_RE.match("tgp:s:a/"))

    def test_index_callbacks_do_not_match_session_regex(self):
        self.assertIsNone(mod._CB_SESSION_RE.match(f"{mod.CB_PREFIX}a:3"))
        self.assertIsNone(mod._CB_SESSION_RE.match(f"{mod.CB_PREFIX}p:12"))


class ParsePprojectTests(unittest.TestCase):
    """/pproject argument parsing."""

    def test_simple_name_and_path(self):
        self.assertEqual(
            mod._parse_pproject("Орк /mnt/mydisk/orc"),
            ("Орк", "/mnt/mydisk/orc", False))

    def test_flag_after_path(self):
        self.assertEqual(
            mod._parse_pproject("Орк /mnt/mydisk/orc new-folder"),
            ("Орк", "/mnt/mydisk/orc", True))

    def test_flag_before_name(self):
        self.assertEqual(
            mod._parse_pproject("new-folder Орк /mnt/mydisk/orc"),
            ("Орк", "/mnt/mydisk/orc", True))

    def test_dash_flag_variant(self):
        self.assertEqual(
            mod._parse_pproject("--new-folder Орк /mnt/mydisk/orc"),
            ("Орк", "/mnt/mydisk/orc", True))

    def test_quoted_multi_word_name(self):
        self.assertEqual(
            mod._parse_pproject('"Грок Abuse" /mnt/mydisk/grok-abuse'),
            ("Грок Abuse", "/mnt/mydisk/grok-abuse", False))

    def test_quoted_name_with_flag(self):
        self.assertEqual(
            mod._parse_pproject('"Грок Abuse" /mnt/grok new-folder'),
            ("Грок Abuse", "/mnt/grok", True))

    def test_missing_path_returns_none(self):
        self.assertIsNone(mod._parse_pproject("Орк"))
        self.assertIsNone(mod._parse_pproject(""))
        self.assertIsNone(mod._parse_pproject(None))

    def test_quoted_flag_only_returns_none(self):
        self.assertIsNone(mod._parse_pproject("new-folder"))


class ThreadProjectMappingTests(unittest.TestCase):
    """thread_id -> project lives in state.json (never config.yaml)."""

    def setUp(self):
        _reset_state()

    def test_state_wins_over_config(self):
        # 65001 = "default" in config.yaml; state.json overrides it
        _STATE.data["thread_to_project"]["65001"] = {
            "name": "Ambrozia", "cwd": "/x", "project_id": 1, "ts": 0}
        self.assertEqual(mod._topic_name_by_thread("65001"), "Ambrozia")

    def test_config_fallback_when_state_absent(self):
        self.assertEqual(mod._topic_name_by_thread("64999"), "Ambrozia")

    def test_unknown_thread(self):
        self.assertEqual(mod._topic_name_by_thread("99999"), "")
        self.assertEqual(mod._topic_name_by_thread(None), "")

    def test_save_thread_project_roundtrip(self):
        _STATE.data["thread_to_project"].clear()
        proj = _Project("p9", "Fresh", "fresh", "/mnt/mydisk/fresh")
        mod._save_thread_project("77777", proj)
        self.assertEqual(mod._topic_name_by_thread("77777"), "Fresh")
        entry = _STATE.data["thread_to_project"]["77777"]
        self.assertEqual(entry["cwd"], "/mnt/mydisk/fresh")
        self.assertEqual(entry["project_id"], "p9")

    def test_save_thread_project_ignores_bad_id(self):
        mod._save_thread_project("junk", PROJECTS[0])
        self.assertEqual(_STATE.data["thread_to_project"], {})
        mod._save_thread_project(None, PROJECTS[0])
        self.assertEqual(_STATE.data["thread_to_project"], {})

    def test_thread_cwd_from_state_json(self):
        _STATE.data["thread_to_project"]["88888"] = {
            "name": "Ambrozia", "cwd": str(PLUGIN_DIR), "project_id": 1, "ts": 0}
        with _with_env(HERMES_SESSION_THREAD_ID="88888"):
            self.assertEqual(mod._thread_cwd(), str(PLUGIN_DIR))


class SessionQueryTests(unittest.TestCase):
    """Cross-origin session listing (desktop + telegram) from state.db."""

    def setUp(self):
        _reset_fake_db()

    def test_listing_is_cross_origin(self):
        conn = _FAKE_STATE.conn
        conn.execute("INSERT INTO sessions (id, source, cwd, started_at,"
                     " message_count, last_activity_at) VALUES (?, ?, ?, ?, ?, ?)",
                     ("s-desktop", "desktop", "/p", 100.0, 5, time.time()))
        conn.execute("INSERT INTO sessions (id, source, cwd, started_at,"
                     " message_count, last_activity_at) VALUES (?, ?, ?, ?, ?, ?)",
                     ("s-tg", "telegram", "/p", 200.0, 3, time.time()))
        conn.commit()
        rows = mod._sessions_for_cwd(conn, "/p", limit=2)
        self.assertEqual([r["id"] for r in rows], ["s-tg", "s-desktop"])
        self.assertEqual(rows[1]["source"], "desktop")

    def test_session_live_status_uses_turn_lease(self):
        conn = _FAKE_STATE.conn
        conn.execute("INSERT INTO sessions (id, source, cwd, started_at,"
                     " message_count, last_activity_at) VALUES (?, ?, ?, ?, ?, ?)",
                     ("s-running", "desktop", "/p", 100.0, 5, time.time()))
        conn.execute("INSERT INTO sessions (id, source, cwd, started_at,"
                     " message_count, last_activity_at) VALUES (?, ?, ?, ?, ?, ?)",
                     ("s-idle", "telegram", "/p", 200.0, 3, time.time()))
        conn.execute(
            "INSERT INTO session_turn_leases (conversation_id, holder,"
            " acquired_at, expires_at) VALUES (?, ?, ?, ?)",
            ("s-running", "pid=42:turn=d", time.time() + 60, time.time() + 360))
        conn.commit()
        rows = mod._sessions_for_cwd(conn, "/p", limit=2)
        running = next(r for r in rows if r["id"] == "s-running")
        idle = next(r for r in rows if r["id"] == "s-idle")
        self.assertEqual(running["status"], "online")
        self.assertEqual(running["active_device"], "desktop")
        self.assertEqual(idle["status"], "idle")
        self.assertEqual(idle["active_device"], "telegram")

    def test_session_live_status_handles_expired_lease_and_missing_table(self):
        conn = _FAKE_STATE.conn
        conn.execute("INSERT INTO sessions (id, source, cwd, started_at,"
                     " message_count, last_activity_at) VALUES (?, ?, ?, ?, ?, ?)",
                     ("s-expired", "desktop", "/p", 100.0, 5, time.time()))
        conn.execute(
            "INSERT INTO session_turn_leases (conversation_id, holder,"
            " acquired_at, expires_at) VALUES (?, ?, ?, ?)",
            ("s-expired", "pid=42:turn=d", time.time() - 60, time.time() - 1))
        conn.commit()
        rows = mod._sessions_for_cwd(conn, "/p", limit=1)
        self.assertEqual(rows[0]["status"], "idle")
        self.assertEqual(rows[0]["active_device"], "desktop")
        conn.execute("DROP TABLE session_turn_leases")
        conn.commit()
        rows = mod._sessions_for_cwd(conn, "/p", limit=1)
        self.assertEqual(rows[0]["status"], "idle")
        self.assertEqual(rows[0]["active_device"], "desktop")
        conn.execute(
            "CREATE TABLE session_turn_leases (conversation_id TEXT PRIMARY KEY,"
            " holder TEXT NOT NULL, acquired_at REAL NOT NULL,"
            " expires_at REAL NOT NULL)")
        conn.commit()

    def test_sessions_lines_show_status_and_device(self):
        conn = _FAKE_STATE.conn
        conn.execute("INSERT INTO sessions (id, source, cwd, started_at,"
                     " message_count, last_activity_at) VALUES (?, ?, ?, ?, ?, ?)",
                     ("s-running", "desktop", "/p", 100.0, 5, time.time()))
        conn.execute(
            "INSERT INTO session_turn_leases (conversation_id, holder,"
            " acquired_at, expires_at) VALUES (?, ?, ?, ?)",
            ("s-running", "pid=42:turn=d", time.time() + 60, time.time() + 360))
        conn.commit()
        lines = mod._sessions_lines(conn, "/p", limit=1)
        self.assertEqual(len(lines), 1)
        self.assertIn("online", lines[0])
        self.assertIn("desktop", lines[0])


    def test_source_filter(self):
        conn = _FAKE_STATE.conn
        conn.execute("INSERT INTO sessions (id, source, cwd, started_at,"
                     " message_count, last_activity_at) VALUES (?, ?, ?, ?, ?, ?)",
                     ("s-d", "desktop", "/p", 1.0, 1, time.time()))
        conn.execute("INSERT INTO sessions (id, source, cwd, started_at,"
                     " message_count, last_activity_at) VALUES (?, ?, ?, ?, ?, ?)",
                     ("s-t", "telegram", "/p", 2.0, 1, time.time()))
        conn.commit()
        self.assertEqual([r["id"] for r in
                           mod._sessions_for_cwd(conn, "/p", source="desktop")],
                          ["s-d"])

    def test_empty_message_count_excluded(self):
        conn = _FAKE_STATE.conn
        conn.execute("INSERT INTO sessions (id, source, cwd, started_at,"
                     " message_count, last_activity_at) VALUES (?, ?, ?, ?, ?, ?)",
                     ("s-empty", "telegram", "/p", 1.0, 0, time.time()))
        conn.commit()
        self.assertEqual(mod._sessions_for_cwd(conn, "/p"), [])


class CrossOriginTests(unittest.TestCase):
    """/resume --all decision: admin policy, no auth bypass."""

    def setUp(self):
        conn = _FAKE_STATE.conn
        conn.executescript("DELETE FROM messages; DELETE FROM sessions;")
        conn.commit()
        ADMIN_CALLS.clear()
        _with_env.__enter__ if False else None
        _ENV.clear()

    def tearDown(self):
        _ENV.clear()

    def test_same_source_never_adds_all(self):
        conn = _FAKE_STATE.conn
        conn.execute("INSERT INTO sessions (id, source) VALUES (?, ?)",
                     ("sess-1", "telegram"))
        conn.commit()
        self.assertTrue(mod._session_is_same_source(conn, "sess-1", "telegram"))
        self.assertEqual(
            mod._resume_command_for("sess-1", conn, "telegram"),
            "/resume sess-1")

    def test_cross_origin_uses_caller_source_not_row_source(self):
        """Regression (review P1): the resume paths must pass the CALLER's
        source, never the target row's — ``_caller_source_value`` echoes an
        explicit session_source, so passing row["source"] made cross_origin
        always False and the IDOR guard blocked every desktop resume."""
        conn = _FAKE_STATE.conn
        conn.execute("INSERT INTO sessions (id, source) VALUES (?, ?)",
                     ("sess-d", "desktop"))
        conn.execute("INSERT INTO sessions (id, source) VALUES (?, ?)",
                     ("sess-t", "telegram"))
        conn.commit()

        class _TeleSource:
            platform = types.SimpleNamespace(value="telegram")
            chat_type = "dm"
            user_id = "111"

        caller = _TeleSource()
        # empty session_source: the caller's own platform decides
        self.assertFalse(mod._session_is_same_source(conn, "sess-d", "", caller))
        # explicit telegram session_source (the caller's): same decision
        self.assertFalse(mod._session_is_same_source(conn, "sess-d", "telegram", caller))
        # same-origin sanity: telegram row, telegram caller
        self.assertTrue(mod._session_is_same_source(conn, "sess-t", "telegram", caller))

    def test_cross_origin_admin_gets_all(self):
        conn = _FAKE_STATE.conn
        conn.execute("INSERT INTO sessions (id, source) VALUES (?, ?)",
                     ("sess-1", "desktop"))
        conn.commit()
        runner = FakeRunner(admin_gate=True, admins=("111",))
        mod._ADAPTER = FakeAdapter(runner)
        try:
            cmd = mod._resume_command_for("sess-1", conn, "telegram", FakeSource("111"))
            self.assertEqual(cmd, "/resume --all sess-1")
        finally:
            mod._ADAPTER = None

    def test_cross_origin_non_admin_gets_plain_resume(self):
        conn = _FAKE_STATE.conn
        conn.execute("INSERT INTO sessions (id, source) VALUES (?, ?)",
                     ("sess-1", "desktop"))
        conn.commit()
        runner = FakeRunner(admin_gate=True, admins=("222",))
        mod._ADAPTER = FakeAdapter(runner)
        try:
            cmd = mod._resume_command_for("sess-1", conn, "telegram", FakeSource("333"))
            first_line = cmd.split("\n", 1)[0]
            self.assertEqual(first_line, "/resume sess-1")
            self.assertIn("allow_admin_from", cmd)
            self.assertTrue(mod._ADMIN_NOTE.startswith("\n"))
            self.assertIn("allow_admin_from", mod._ADMIN_NOTE)
        finally:
            mod._ADAPTER = None

    def test_disabled_gate_makes_everyone_admin(self):
        conn = _FAKE_STATE.conn
        conn.execute("INSERT INTO sessions (id, source) VALUES (?, ?)",
                     ("sess-1", "desktop"))
        conn.commit()
        runner = FakeRunner(admin_gate=False, admins=())
        mod._ADAPTER = FakeAdapter(runner)
        try:
            self.assertTrue(mod._callers_admin(FakeSource("111")))
            self.assertEqual(
                mod._resume_command_for("sess-1", conn, "telegram", FakeSource("111")),
                "/resume --all sess-1")
        finally:
            mod._ADAPTER = None

    def test_no_adapter_fails_closed(self):
        conn = _FAKE_STATE.conn
        conn.execute("INSERT INTO sessions (id, source) VALUES (?, ?)",
                     ("sess-1", "desktop"))
        conn.commit()
        mod._ADAPTER = None
        self.assertFalse(mod._callers_admin(FakeSource("111")))
        self.assertEqual(mod._caller_source_value(), "")
        self.assertFalse(mod._session_is_same_source(conn, "sess-1", "telegram"))

    def test_unresolvable_row_fails_open(self):
        conn = _FAKE_STATE.conn
        self.assertTrue(mod._session_is_same_source(conn, "missing", "telegram"))
        conn.execute("INSERT INTO sessions (id, source) VALUES (?, ?)",
                     ("sess-null", None))
        conn.commit()
        self.assertTrue(mod._session_is_same_source(conn, "sess-null", "telegram"))

    def test_desktop_note_only_for_desktop(self):
        self.assertEqual(mod._desktop_note("telegram"), "")
        self.assertEqual(mod._desktop_note(""), "")
        note = mod._desktop_note("desktop")
        self.assertIn("двух процессов", note)
        self.assertIn("одной за раз", note)


class ProjectListPaginationTests(unittest.TestCase):
    """5+ projects paginate; tgp:p:<i> keeps the GLOBAL index across pages."""

    def _projects(self, n):
        return [_Project(f"p{i}", f"Prj{i}", f"prj{i}", f"/x/{i}") for i in range(1, n + 1)]

    def test_short_list_has_no_ellipsis(self):
        kb = mod._project_list_keyboard(self._projects(3))
        self.assertEqual([r[0].callback_data for r in kb.rows[:3]],
                         ["tgp:p:1", "tgp:p:2", "tgp:p:3"])
        self.assertNotIn("tgp:pl:5", str(kb.rows))

    def test_page_two_uses_global_indices(self):
        kb = mod._project_list_keyboard(self._projects(7), offset=5)
        self.assertEqual([r[0].callback_data for r in kb.rows[:2]],
                         ["tgp:p:6", "tgp:p:7"])
        self.assertIn("tgp:pl:0",
                      [b.callback_data for row in kb.rows for b in row])  # back to start
        rest = [b for row in kb.rows for b in row if "осталось" in str(b.text)]
        self.assertEqual(len(rest), 0)  # exactly 2 on page 2 — no [Ещё] row

    def test_six_projects_show_more_row(self):
        kb = mod._project_list_keyboard(self._projects(6))
        flat = [b.callback_data for row in kb.rows for b in row]
        self.assertIn("tgp:pl:5", flat)



class SyncHookFlatTests(unittest.TestCase):
    """pre_gateway_dispatch sync: a project-bound flat lane never blocks text."""

    def setUp(self):
        _reset_state()

    def _event(self, text):
        src = types.SimpleNamespace(
            platform=types.SimpleNamespace(value="telegram"),
            chat_type="dm", chat_id="7559860199",
            thread_id=None, user_id="7559860199")
        return types.SimpleNamespace(source=src, text=text, internal=False)

    def _bind_flat(self):
        _STATE.data["topic_bindings"] = {"7559860199:0": {
            "project_id": "p1", "project_name": "SD1",
            "cwd": "/mnt/mydisk/sd1", "session_id": None,
            "updated_at": 1791315530}}

    def test_project_without_session_lets_text_through(self):
        # Regression: binding has the project but session_id is still empty
        # (right after /new, before the first turn) — blocking here returned
        # "выбери проект" and deadlocked the first turn. The cwd has NO
        # sessions yet in this scenario: the text must flow (first turn
        # creates and binds the session via on_session_start).
        self._bind_flat()
        result = asyncio_run(mod._on_pre_gateway_dispatch_sync(self._event("привет"), None))
        self.assertIsNone(result)

    def test_project_with_sessions_but_none_picked_asks(self):
        # A cwd WITH sessions and nothing picked: the user is sent to the
        # pick screen instead of silently adopting the cwd's latest session
        # (which may be an unrelated chat that merely shares the directory).
        self._bind_flat()
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE sessions (id TEXT PRIMARY KEY, source TEXT,"
                     " cwd TEXT, started_at REAL, ended_at REAL, title TEXT,"
                     " chat_id TEXT, thread_id TEXT, message_count INTEGER,"
                     " last_activity_at REAL)")
        conn.execute("CREATE TABLE messages (session_id TEXT, role TEXT,"
                     " content TEXT)")
        recent = time.time()
        conn.execute("INSERT INTO sessions VALUES ('s_old', 'telegram',"
                     " '/mnt/mydisk/sd1', ?, NULL, 'Старая',"
                     " '7559860199', NULL, 3, ?)", (recent, recent))
        orig = mod._open_state_db
        mod._open_state_db = lambda: conn
        try:
            with _with_env(HERMES_HOME="/nonexistent-sync-tmp"):
                result = asyncio_run(
                    mod._on_pre_gateway_dispatch_sync(self._event("привет"), None))
        finally:
            mod._open_state_db = orig
            conn.close()
        # A cwd WITH sessions for this same Telegram chat: adopt that lane
        # automatically instead of forcing a redundant pick screen.
        self.assertIsNone(result)
        binding = mod._binding_at("7559860199", 0)
        self.assertEqual(binding.get("session_id"), "s_old")

    def _bind_flat_with_old_session(self, chat_id="7559860199"):
        """Binding with session_id=None AND an older session in the cwd."""
        self._bind_flat()
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE sessions (id TEXT PRIMARY KEY, source TEXT,"
                     " cwd TEXT, started_at REAL, ended_at REAL, title TEXT,"
                     " chat_id TEXT, thread_id TEXT, message_count INTEGER,"
                     " last_activity_at REAL)")
        recent = time.time()
        conn.execute("INSERT INTO sessions VALUES ('s_old', 'telegram',"
                     " '/mnt/mydisk/sd1', ?, NULL, 'Старая',"
                     " ?, NULL, 3, ?)", (chat_id, recent, recent))
        conn.execute("CREATE TABLE messages (session_id TEXT, role TEXT,"
                     " content TEXT)")
        return conn

    def test_new_session_grace_lets_first_text_through(self):
        # Regression 2026-10-07: [➕ Новая] created the gateway session, but
        # the binding's session_id stays None until the first text turn —
        # and the cwd still has OLDER sessions, so the pick-screen guard ate
        # the user's first "привет". The grace window must let it through.
        self._bind_flat()
        conn = self._bind_flat_with_old_session()
        orig = mod._open_state_db
        mod._open_state_db = lambda: conn
        try:
            mod._mark_new_session_grace("7559860199", None)
            with _with_env(HERMES_HOME="/nonexistent-sync-tmp"):
                result = asyncio_run(
                    mod._on_pre_gateway_dispatch_sync(self._event("привет"), None))
        finally:
            mod._open_state_db = orig
            conn.close()
            mod._NEW_SESSION_GRACE.clear()
        self.assertIsNone(result)

    def test_expired_grace_still_asks_for_pick(self):
        # Grace expired -> a different chat's session still requires a pick.
        self._bind_flat()
        conn = self._bind_flat_with_old_session(chat_id="other-chat")
        orig = mod._open_state_db
        orig_latest = mod._latest_session_id_for_cwd
        mod._open_state_db = lambda: conn
        mod._latest_session_id_for_cwd = (
            lambda cwd, chat_id=None: "" if chat_id else "s_old")
        try:
            mod._mark_new_session_grace("7559860199", None)
            mod._NEW_SESSION_GRACE["7559860199:0"] -= mod._NEW_SESSION_GRACE_S + 1
            with _with_env(HERMES_HOME="/nonexistent-sync-tmp"):
                result = asyncio_run(
                    mod._on_pre_gateway_dispatch_sync(self._event("привет"), None))
        finally:
            mod._open_state_db = orig
            mod._latest_session_id_for_cwd = orig_latest
            conn.close()
            mod._NEW_SESSION_GRACE.clear()
        self.assertEqual(result, {"action": "skip", "reason": "awaiting_session_pick"})

    def test_pick_clears_grace(self):
        self._bind_flat()  # _pb_write_binding_session needs an existing binding row
        mod._mark_new_session_grace("7559860199", None)
        self.assertTrue(mod._new_session_grace_active("7559860199", None))
        mod._pb_write_binding_session("7559860199", None, "sess-x")
        self.assertFalse(mod._new_session_grace_active("7559860199", None))
        mod._NEW_SESSION_GRACE.clear()

    def test_no_binding_at_all_still_redirects_to_menu(self):
        result = asyncio_run(mod._on_pre_gateway_dispatch_sync(self._event("привет"), None))
        self.assertEqual(result, {"action": "skip", "reason": "unbound_topic"})

    def test_menu_command_is_consumed_before_dispatch(self):
        # /menu is an interface command: the hook consumes it (panel refresh)
        # so a busy turn is never interrupted and the agent never sees the text.
        self._bind_flat()
        result = asyncio_run(mod._on_pre_gateway_dispatch_sync(self._event("/menu"), None))
        self.assertEqual(result, {"action": "skip", "reason": "tg_menu"})

    def test_other_commands_flow_untouched(self):
        self._bind_flat()
        self.assertIsNone(
            asyncio_run(mod._on_pre_gateway_dispatch_sync(self._event("/model"), None)))


class SessionsKeyboardTests(unittest.TestCase):
    """tgp:s:<id> continue buttons, one per live session (the single sessions view)."""

    def test_one_button_per_session_plus_new_and_back(self):
        sessions = [{"id": "20261003_062445_2d0fd7", "title": "Проверка"}, {"id": "sess-abc"}]
        kb = mod._sessions_keyboard("3", sessions)
        rows = kb.rows
        self.assertEqual(len(rows), 4)
        self.assertEqual(
            rows[0][0].callback_data, "tgp:s:20261003_062445_2d0fd7")
        self.assertEqual(rows[1][0].callback_data, "tgp:s:sess-abc")
        self.assertEqual(rows[2][0].callback_data, "tgp:n:3")  # ➕ Новая сессия
        self.assertEqual(rows[3][0].callback_data, mod._BACK_CB)

    def test_session_callbacks_within_cap(self):
        # 58-char id + 6-char prefix "tgp:s:" = 64 bytes (the cap).
        sessions = [{"id": "a" * 58}]
        kb = mod._sessions_keyboard("1", sessions)
        self.assertLessEqual(
            len(kb.rows[0][0].callback_data.encode("utf-8")), 64)
        # the 64-char id would be 70 bytes, over the cap — the regex rejects it
        import re
        self.assertIsNone(mod._CB_SESSION_RE.match("tgp:s:" + "a" * 64))
        self.assertIsNotNone(mod._CB_SESSION_RE.match("tgp:s:" + "a" * 58))

    def test_empty_session_list_still_has_new_and_back(self):
        kb = mod._sessions_keyboard("1", [])
        self.assertEqual(len(kb.rows), 2)
        self.assertEqual(kb.rows[0][0].callback_data, "tgp:n:1")  # ➕ Новая сессия
        self.assertEqual(kb.rows[1][0].callback_data, mod._BACK_CB)


class ModelPickerThreadTests(unittest.TestCase):
    """/model override must land on the topic the next turn reads."""

    def setUp(self):
        conn = _FAKE_STATE.conn
        conn.executescript("DELETE FROM messages; DELETE FROM sessions;")
        conn.commit()

    def test_hint_wins(self):
        conn = _FAKE_STATE.conn
        conn.execute("INSERT INTO sessions (id, source, chat_id, thread_id,"
                     " started_at) VALUES (?,?,?,?,?)",
                     ("s1", "telegram", "7559860199", "64999", 1.0))
        conn.commit()
        self.assertEqual(
            mod._thread_id_for_chat_from_sessions(conn, "7559860199", "65003"),
            "65003")

    def test_latest_topic_for_chat_fallback(self):
        conn = _FAKE_STATE.conn
        conn.execute("INSERT INTO sessions (id, source, chat_id, thread_id,"
                     " started_at) VALUES (?,?,?,?,?)",
                     ("s1", "telegram", "7559860199", "64999", 1.0))
        conn.execute("INSERT INTO sessions (id, source, chat_id, thread_id,"
                     " started_at) VALUES (?,?,?,?,?)",
                     ("s2", "telegram", "7559860199", "65003", 5.0))
        conn.execute("INSERT INTO sessions (id, source, chat_id, thread_id,"
                     " started_at) VALUES (?,?,?,?,?)",
                     ("s3", "telegram", "7559860199", "", 9.0))
        conn.commit()
        # s3 has no thread_id -> skipped; s2 is the newest with a thread
        self.assertEqual(
            mod._thread_id_for_chat_from_sessions(conn, "7559860199"),
            "65003")

    def test_no_topic_yields_empty(self):
        conn = _FAKE_STATE.conn
        conn.execute("INSERT INTO sessions (id, source, chat_id, thread_id,"
                     " started_at) VALUES (?,?,?,?,?)",
                     ("s1", "telegram", "7559860199", "", 1.0))
        conn.commit()
        self.assertEqual(
            mod._thread_id_for_chat_from_sessions(conn, "7559860199"), "")
        self.assertEqual(
            mod._thread_id_for_chat_from_sessions(conn, "9999999999"), "")


class CwdPersistTests(unittest.TestCase):
    """sessions.cwd backfill into state.db (desktop visibility).

    Every call in this class goes through ``_with_env(HERMES_HOME=<tmpdir>)``,
    so ``_persist_cwd_to_state_db`` opens the throwaway ``tmp/state.db`` —
    never the real ``~/.hermes/state.db``.  ``SessionDB`` itself is a stub that
    mutates the in-memory ``_FAKE_STATE`` connection.
    """

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        Path(self._tmpdir.name, "state.db").write_bytes(b"")  # make it exist
        self.addCleanup(self._tmpdir.cleanup)

    def tearDown(self):
        _reset_fake_db()
        mod._CWD_APPLIED.clear()

    def test_write_persists_cwd_when_row_exists(self):
        _reset_fake_db()
        _FAKE_STATE.conn.execute(
            "INSERT INTO sessions (id, source, cwd) VALUES (?,?,?)",
            ("sess-1", "telegram", None))
        _FAKE_STATE.conn.commit()
        with _with_env(HERMES_HOME=self._tmpdir.name):
            self.assertTrue(mod._persist_cwd_to_state_db("sess-1", "/p", "pin"))
        self.assertEqual(
            _FAKE_STATE.conn.execute(
                "SELECT cwd FROM sessions WHERE id = ?", ("sess-1",)).fetchone()[0],
            "/p")
        # own handle, closed after the write
        self.assertEqual(len(_FakeSessionDB.instances), 1)
        self.assertTrue(_FakeSessionDB.instances[0].closed)

    def test_missing_row_returns_false_and_defers(self):
        _reset_fake_db()
        with _with_env(HERMES_HOME=self._tmpdir.name):
            self.assertFalse(
                mod._persist_cwd_to_state_db("absent", "/p", "pin"))
        self.assertEqual(_FakeSessionDB.instances[0].closed, True)

    def test_missing_db_file_returns_false_without_import(self):
        _reset_fake_db()
        Path(self._tmpdir.name, "state.db").unlink()
        with _with_env(HERMES_HOME=self._tmpdir.name):
            self.assertFalse(mod._persist_cwd_to_state_db("sess-1", "/p", "pin"))
        self.assertEqual(_FakeSessionDB.instances, [])

    def test_db_error_returns_false_without_raising(self):
        _reset_fake_db()
        _FAKE_STATE.conn.execute(
            "INSERT INTO sessions (id, source, cwd) VALUES (?,?,?)",
            ("sess-1", "telegram", None))
        _FAKE_STATE.conn.commit()
        _FakeSessionDB.last_error = sqlite3.OperationalError("locked")
        try:
            with _with_env(HERMES_HOME=self._tmpdir.name):
                self.assertFalse(
                    mod._persist_cwd_to_state_db("sess-1", "/p", "pin"))
        finally:
            _FakeSessionDB.last_error = None
        self.assertTrue(_FakeSessionDB.instances[0].closed)  # closed despite error

    def test_apply_caches_only_after_successful_db_write(self):
        _reset_fake_db()
        _FAKE_STATE.conn.execute(
            "INSERT INTO sessions (id, source, cwd) VALUES (?,?,?)",
            ("sess-1", "telegram", None))
        _FAKE_STATE.conn.commit()
        with _with_env(HERMES_HOME=self._tmpdir.name):
            mod._apply_session_cwd("sess-1", "/p", "topic->project")
            self.assertEqual(mod._CWD_APPLIED.get("sess-1"), "/p")
            self.assertEqual(
                fake_terminal._session_cwd.get("sess-1"), "/p")
            self.assertEqual(fake_terminal._cwd.get("sess-1"), {"cwd": "/p"})
            # idempotent: second call is a pure dict no-op
            mod._apply_session_cwd("sess-1", "/p", "topic->project")
            self.assertEqual(len(fake_terminal._calls), 2)
            self.assertEqual(len(_FakeSessionDB.instances), 1)

    def test_apply_registered_cache_survives_db_write_failure(self):
        _reset_fake_db()
        _FAKE_STATE.conn.execute(
            "INSERT INTO sessions (id, source, cwd) VALUES (?,?,?)",
            ("sess-1", "telegram", None))
        _FAKE_STATE.conn.commit()
        with _with_env(HERMES_HOME=self._tmpdir.name):
            _FakeSessionDB.last_error = sqlite3.OperationalError("locked")
            try:
                mod._apply_session_cwd("sess-1", "/p", "topic->project")
            finally:
                _FakeSessionDB.last_error = None
            # in-memory registration happened and is stamped even though the
            # DB write failed; only the state.db persist is retried next turn
            self.assertEqual(mod._CWD_REGISTERED.get("sess-1"), "/p")
            # _CWD_APPLIED is NOT stamped until the DB write succeeds
            self.assertIsNone(mod._CWD_APPLIED.get("sess-1"))
            self.assertEqual(fake_terminal._session_cwd.get("sess-1"), "/p")
            self.assertEqual(len(fake_terminal._calls), 2)

    def test_apply_retries_db_after_failed_write(self):
        _reset_fake_db()
        _FAKE_STATE.conn.execute(
            "INSERT INTO sessions (id, source, cwd) VALUES (?,?,?)",
            ("sess-1", "telegram", None))
        _FAKE_STATE.conn.commit()
        with _with_env(HERMES_HOME=self._tmpdir.name):
            _FakeSessionDB.last_error = sqlite3.OperationalError("locked")
            try:
                mod._apply_session_cwd("sess-1", "/p", "pre_llm_call")
            finally:
                _FakeSessionDB.last_error = None
            # registration is done and cached; the DB write must still be pending
            self.assertEqual(mod._CWD_REGISTERED.get("sess-1"), "/p")
            self.assertIsNone(mod._CWD_APPLIED.get("sess-1"))
            self.assertIsNone(
                _FAKE_STATE.conn.execute(
                    "SELECT cwd FROM sessions WHERE id = ?",
                    ("sess-1",)).fetchone()[0])
            # next pre_llm_call turn: registration is NOT repeated, only the
            # pending state.db write is retried (row exists, DB healthy)
            before = len(fake_terminal._calls)
            mod._apply_session_cwd("sess-1", "/p", "pre_llm_call")
            self.assertEqual(len(fake_terminal._calls), before)
            self.assertEqual(mod._CWD_APPLIED.get("sess-1"), "/p")
            self.assertEqual(
                _FAKE_STATE.conn.execute(
                    "SELECT cwd FROM sessions WHERE id = ?",
                    ("sess-1",)).fetchone()[0], "/p")
            self.assertEqual(len(_FakeSessionDB.instances), 2)

    def test_apply_defers_when_row_not_created_yet(self):
        _reset_fake_db()
        with _with_env(HERMES_HOME=self._tmpdir.name):
            mod._apply_session_cwd("sess-new", "/p", "pending pin")
            self.assertIsNone(mod._CWD_APPLIED.get("sess-new"))
        self.assertEqual(_FakeSessionDB.instances[0].closed, True)

    def test_apply_empty_args_are_noops(self):
        _reset_fake_db()
        with _with_env(HERMES_HOME=self._tmpdir.name):
            mod._apply_session_cwd("", "/p", "x")
            mod._apply_session_cwd("sess-1", "", "x")
        self.assertEqual(fake_terminal._calls, [])
        self.assertEqual(_FakeSessionDB.instances, [])
        self.assertEqual(mod._CWD_APPLIED, {})


class TopicBindingTests(unittest.TestCase):
    """topic_bindings: the step-3 topic -> workplace map and the new
    on_session_start source priority (binding > pending pin > nothing)."""

    def setUp(self):
        _reset_fake_db()
        _STATE.data = {"pending_cwd": {}, "thread_to_project": {}}
        mod._BINDING_WARNED.clear()
        # a throwaway HERMES_HOME with a state.db file, so cwd persists
        # never touch the real ~/.hermes/state.db
        self._tmpdir = tempfile.TemporaryDirectory()
        Path(self._tmpdir.name, "state.db").write_bytes(b"")
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        _reset_fake_db()
        _STATE.data = {"pending_cwd": {}, "thread_to_project": {}}
        mod._BINDING_WARNED.clear()
        self._tmpdir.cleanup()

    def _insert_session(self, sid: str):
        _FAKE_STATE.conn.execute(
            "INSERT INTO sessions (id, source, cwd) VALUES (?,?,?)",
            (sid, "telegram", None))
        _FAKE_STATE.conn.commit()

    def test_binding_key_requires_chat_and_thread(self):
        with _with_env(HERMES_SESSION_CHAT_ID="5", HERMES_SESSION_THREAD_ID="88888"):
            self.assertEqual(mod._topic_binding_key(), "5:88888")
        # Topics off: the flat chat lane normalizes to thread 0
        with _with_env(HERMES_SESSION_CHAT_ID="5", HERMES_SESSION_THREAD_ID=""):
            self.assertEqual(mod._topic_binding_key(), "5:0")
        # No telegram chat (desktop/CLI): nothing to bind
        with _with_env(HERMES_SESSION_CHAT_ID="", HERMES_SESSION_THREAD_ID="88888"):
            self.assertIsNone(mod._topic_binding_key())

    def test_binding_key_normalizes_fractional_thread(self):
        with _with_env(HERMES_SESSION_CHAT_ID="5", HERMES_SESSION_THREAD_ID="65008.5"):
            self.assertEqual(mod._topic_binding_key(), "5:65008")

    def test_set_get_clear_roundtrip(self):
        with _with_env(HERMES_SESSION_CHAT_ID="5", HERMES_SESSION_THREAD_ID="88888"):
            self.assertTrue(mod._set_topic_binding("p1", "Ambrozia", str(PLUGIN_DIR)))
            binding = mod._get_topic_binding()
            self.assertEqual(binding["project_id"], "p1")
            self.assertEqual(binding["project_name"], "Ambrozia")
            self.assertEqual(binding["cwd"], str(PLUGIN_DIR))
            self.assertIn("updated_at", binding)
            mod._update_binding_session("sess-9")
            self.assertEqual(mod._get_topic_binding()["session_id"], "sess-9")
            mod._update_binding_session(None)
            self.assertIsNone(mod._get_topic_binding()["session_id"])
            mod._clear_topic_binding()
            self.assertIsNone(mod._get_topic_binding())
        # state bucket removed entirely when empty
        self.assertNotIn("topic_bindings", _STATE.data)

    def test_set_binding_flat_chat_lands_on_key_zero(self):
        # Topics off: a binding WITHOUT a thread targets the flat lane <chat>:0
        with _with_env(HERMES_SESSION_CHAT_ID="5", HERMES_SESSION_THREAD_ID=""):
            self.assertTrue(mod._set_topic_binding("p1", "x", "/p"))
        state = mod._load_state()
        self.assertEqual(state["topic_bindings"]["5:0"]["project_name"], "x")

    def test_update_binding_session_needs_existing_binding(self):
        with _with_env(HERMES_SESSION_CHAT_ID="5", HERMES_SESSION_THREAD_ID="88888"):
            mod._update_binding_session("sess-1")  # no binding yet: no-op
            self.assertIsNone(mod._get_topic_binding())

    def test_on_session_start_applies_binding_cwd(self):
        self._insert_session("sess-1")
        with _with_env(HERMES_SESSION_CHAT_ID="5", HERMES_SESSION_THREAD_ID="88888",
                       HERMES_HOME=self._tmpdir.name):
            mod._set_topic_binding("p1", "Ambrozia", str(PLUGIN_DIR))
            mod._on_session_start(session_id="sess-1")
            self.assertEqual(fake_terminal._cwd.get("sess-1"), {"cwd": str(PLUGIN_DIR)})
            self.assertEqual(mod._CWD_APPLIED.get("sess-1"), str(PLUGIN_DIR))
            # binding now records the working session
            self.assertEqual(mod._get_topic_binding()["session_id"], "sess-1")

    def test_on_session_start_binding_wins_over_pending_pin(self):
        self._insert_session("sess-1")
        _STATE.data["pending_cwd"] = {
            "sk-1": {"project_id": "p2", "name": "default", "slug": "default",
                     "cwd": "/nonexistent-pin", "ts": 0}}
        with _with_env(HERMES_SESSION_CHAT_ID="5", HERMES_SESSION_THREAD_ID="88888",
                       HERMES_SESSION_KEY="sk-1", HERMES_HOME=str(PLUGIN_DIR)):
            mod._set_topic_binding("p1", "Ambrozia", str(PLUGIN_DIR))
            mod._on_session_start(session_id="sess-1")
            # binding cwd applied, pin consumed but not used
            self.assertEqual(fake_terminal._cwd.get("sess-1"), {"cwd": str(PLUGIN_DIR)})
            self.assertNotIn("sk-1", _STATE.data.get("pending_cwd", {}))

    def test_on_session_start_pending_pin_legacy_fallback(self):
        self._insert_session("sess-1")
        _STATE.data["pending_cwd"] = {
            "sk-1": {"project_id": "p1", "name": "Ambrozia", "slug": "ambrozia",
                     "cwd": str(PLUGIN_DIR), "ts": 0}}
        # thread WITHOUT a binding: the pin is the fallback
        with _with_env(HERMES_SESSION_CHAT_ID="5", HERMES_SESSION_THREAD_ID="77777",
                       HERMES_SESSION_KEY="sk-1", HERMES_HOME=self._tmpdir.name):
            mod._on_session_start(session_id="sess-1")
            self.assertEqual(fake_terminal._cwd.get("sess-1"), {"cwd": str(PLUGIN_DIR)})

    def test_on_session_start_unbound_topic_applies_nothing(self):
        _reset_fake_db()
        with _with_env(HERMES_SESSION_CHAT_ID="5", HERMES_SESSION_THREAD_ID="77777",
                       HERMES_HOME=self._tmpdir.name):
            mod._on_session_start(session_id="sess-unbound")
            # no cwd registered anywhere, no state.db write, no applied stamp
            self.assertEqual(fake_terminal._calls, [])
            self.assertEqual(_FakeSessionDB.instances, [])
            self.assertNotIn("sess-unbound", mod._CWD_APPLIED)
        # the unbound-topic warning was rate-limited-stamped for this topic
        self.assertIn("5:77777", mod._BINDING_WARNED)

    def test_unbound_warning_sent_once_per_hour(self):
        with _with_env(HERMES_SESSION_CHAT_ID="5", HERMES_SESSION_THREAD_ID="77777",
                       HERMES_HOME=self._tmpdir.name):
            mod._notify_unbound_topic("s-a")
            first = dict(mod._BINDING_WARNED)
            mod._notify_unbound_topic("s-b")  # same topic: rate-limited
            self.assertEqual(mod._BINDING_WARNED, first)

    def test_pre_llm_call_applies_binding_cwd(self):
        self._insert_session("sess-1")
        with _with_env(HERMES_SESSION_CHAT_ID="5", HERMES_SESSION_THREAD_ID="88888",
                       HERMES_HOME=self._tmpdir.name):
            mod._set_topic_binding("p1", "Ambrozia", str(PLUGIN_DIR))
            mod._on_pre_llm_call(session_id="sess-1")
            self.assertEqual(fake_terminal._cwd.get("sess-1"), {"cwd": str(PLUGIN_DIR)})


if __name__ == "__main__":
    unittest.main(verbosity=2)
