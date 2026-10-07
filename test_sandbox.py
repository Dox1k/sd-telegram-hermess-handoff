"""Project-sandbox tests: _on_pre_tool_call blocks paths outside the
session's project cwd; unbound sessions and safe tools pass."""

import importlib.util
import os
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path

PLUGIN_DIR = Path(os.environ.get("TGP_PLUGIN_DIR") or Path(__file__).parent)
_HAS_PLUGIN = (PLUGIN_DIR / "__init__.py").exists()

# Minimal fakes the module import touches. These are HARD-installed per test
# (never at import time): see _install_core_stubs.
fake_yaml = types.ModuleType("yaml")
fake_yaml.safe_load = lambda *a, **k: {}
fake_yaml.safe_dump = lambda *a, **k: None
fake_terminal = types.ModuleType("tools.terminal_tool")
fake_terminal.register_task_env_overrides = lambda task_id, overrides: None
fake_projects = types.ModuleType("hermes_cli.projects_db")
fake_projects.connect_closing = type("_FakeConn", (), {
    "__enter__": lambda s: s,
    "__exit__": lambda s, *a: False,
})
fake_ctx = types.ModuleType("gateway.session_context")
fake_ctx.get_session_env = lambda name, default="": os.environ.get(name, default)
fake_telegram = types.ModuleType("telegram")
fake_telegram.InlineKeyboardButton = object
fake_telegram.InlineKeyboardMarkup = object
fake_ext = types.ModuleType("telegram.ext")
fake_ext.CallbackQueryHandler = object
fake_state = types.ModuleType("hermes_state")
fake_state.SessionDB = type("_FakeSessionDB", (), {
    "__init__": lambda s, *a, **k: None,
    "close": lambda s: None,
    "update_session_cwd": lambda s, *a, **k: 1,
})

_MODULES = {
    "yaml": fake_yaml,
    "tools": types.ModuleType("tools"),
    "tools.terminal_tool": fake_terminal,
    "hermes_cli": types.ModuleType("hermes_cli"),
    "hermes_cli.projects_db": fake_projects,
    "gateway": types.ModuleType("gateway"),
    "gateway.session_context": fake_ctx,
    "telegram": fake_telegram,
    "telegram.ext": fake_ext,
    "hermes_state": fake_state,
}
# parent -> (child attr name, child module key)
_CHILD_ATTRS = (
    ("tools", "terminal_tool"),
    ("hermes_cli", "projects_db"),
    ("gateway", "session_context"),
    ("telegram", "ext"),
)


def _install_core_stubs():
    """Hard-install this file's stubs; return the state to restore.

    Nothing is installed at import time: under pytest every test module is
    imported BEFORE any test runs, so a setdefault here would win the race
    against sibling files and hand them these stubs. test_sessions imports
    after this one, and test_approvals before it. The plugin resolves its core
    imports lazily (_import_hermes_module), so a per-test swap is enough.

    The parent-module ATTRIBUTES (gateway.session_context, hermes_cli.
    projects_db, ...) are snapshotted too, not just the sys.modules KEYS:
    sibling stubs attach their submodules to parents they created themselves,
    so restoring only the keys leaves those parents carrying our bare stubs
    (that is how test_sessions lost projects_db.connect_closing).
    """
    saved = {name: sys.modules.get(name) for name in _MODULES}
    saved_parents = {
        parent: getattr(sys.modules[parent], child, None)
        for parent, child in _CHILD_ATTRS if parent in sys.modules
    }
    for name, module in _MODULES.items():
        sys.modules[name] = module
    for parent, child in _CHILD_ATTRS:
        key = f"{parent}.{child}"
        setattr(sys.modules[parent], child, sys.modules.get(key))
    return saved, saved_parents


def _restore_core_stubs(saved, saved_parents):
    for name, module in saved.items():
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module
    for parent, child in _CHILD_ATTRS:
        key = f"{parent}.{child}"
        restored = sys.modules.get(key)
        if restored is not None:
            setattr(sys.modules[parent], child, restored)
        elif parent in sys.modules and parent in saved_parents:
            setattr(sys.modules[parent], child, saved_parents[parent])
        elif parent in sys.modules and not hasattr(sys.modules[parent], child):
            pass


if _HAS_PLUGIN:
    # Load once against OUR minimal fakes, then restore everything the sibling
    # modules installed: both the sys.modules keys and the parent attributes.
    _saved_keys, _saved_parents = _install_core_stubs()
    spec = importlib.util.spec_from_file_location(
        "tg_projects_sandbox_under_test", str(PLUGIN_DIR / "__init__.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    _restore_core_stubs(_saved_keys, _saved_parents)


class _StubbedTestCase(unittest.TestCase):
    """Core stubs installed per test (never at import - see _install_core_stubs)."""

    def setUp(self):
        self._saved_stubs = _install_core_stubs()
        self.addCleanup(
            _restore_core_stubs, *self._saved_stubs)


@unittest.skipUnless(_HAS_PLUGIN, "TGP_PLUGIN_DIR not set")
class SandboxPathTests(_StubbedTestCase):
    """_path_outside_sandbox: absolute, relative, escaping, non-paths."""

    def setUp(self):
        super().setUp()
        self.root = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.root, "sub"))

    def test_inside_absolute(self):
        self.assertFalse(mod._path_outside_sandbox(
            os.path.join(self.root, "sub", "f.py"), self.root))

    def test_outside_absolute(self):
        self.assertTrue(mod._path_outside_sandbox("/mnt/mydisk/ambrozia", self.root))

    def test_relative_stays_inside(self):
        self.assertFalse(mod._path_outside_sandbox("sub/f.py", self.root))

    def test_relative_escape_climbs_out(self):
        self.assertTrue(mod._path_outside_sandbox("../outside", self.root))

    def test_urls_and_memory_never_block(self):
        self.assertFalse(mod._path_outside_sandbox("https://x/y", self.root))
        self.assertFalse(mod._path_outside_sandbox("", self.root))
        self.assertFalse(mod._path_outside_sandbox(":memory:", self.root))


@unittest.skipUnless(_HAS_PLUGIN, "TGP_PLUGIN_DIR not set")
class SandboxTerminalTests(_StubbedTestCase):
    """_sandbox_check_terminal: cd and absolute-path tokens in commands."""

    def setUp(self):
        super().setUp()
        self.root = "/mnt/mydisk/sd1"

    def test_git_c_outside_blocked(self):
        cmd = "git -C /mnt/mydisk/ambrozia log --oneline -3"
        self.assertIsNotNone(mod._sandbox_check_terminal(cmd, self.root))

    def test_cd_outside_blocked(self):
        self.assertIsNotNone(
            mod._sandbox_check_terminal("cd /mnt/mydisk/ambrozia && git log", self.root))

    def test_cd_relative_escape_blocked(self):
        self.assertIsNotNone(mod._sandbox_check_terminal("cd ../..", self.root))

    def test_inside_paths_allowed(self):
        self.assertIsNone(mod._sandbox_check_terminal(
            "git -C /mnt/mydisk/sd1 status && ls", self.root))

    def test_plain_command_allowed(self):
        self.assertIsNone(mod._sandbox_check_terminal("ls -la && git status", self.root))


@unittest.skipUnless(_HAS_PLUGIN, "TGP_PLUGIN_DIR not set")
class SandboxHookTests(_StubbedTestCase):
    """_on_pre_tool_call end-to-end with a fake state.db cwd row."""

    def setUp(self):
        super().setUp()
        self.tmp = tempfile.mkdtemp()
        self.sid = "sandbox_sess_1"
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE sessions (id TEXT PRIMARY KEY, cwd TEXT)")
        conn.execute("INSERT INTO sessions VALUES (?, ?)", (self.sid, self.tmp))
        self.conn = conn
        self._orig = mod._open_state_db
        mod._open_state_db = lambda: conn

    def tearDown(self):
        mod._open_state_db = self._orig
        self.conn.close()

    def _call(self, tool, **args):
        return mod._on_pre_tool_call(
            tool, args, session_id=self.sid, task_id=self.sid, tool_call_id="c1")

    def test_read_file_outside_blocked(self):
        res = self._call("read_file", path="/mnt/mydisk/ambrozia/x.py")
        self.assertEqual(res["action"], "block")
        self.assertIn("песочницей", res["message"])

    def test_read_file_inside_allowed(self):
        self.assertIsNone(self._call("read_file", path=os.path.join(self.tmp, "f.py")))

    def test_terminal_outside_blocked(self):
        res = self._call("terminal", command="cd /mnt/mydisk/ambrozia && git log")
        self.assertEqual(res["action"], "block")

    def test_web_tools_allowed(self):
        self.assertIsNone(self._call("web_search", query="x"))

    def test_unknown_session_not_sandboxed(self):
        res = mod._on_pre_tool_call(
            "read_file", {"path": "/mnt/mydisk/ambrozia/x.py"},
            session_id="no_such_session", task_id="no_such_session")
        self.assertIsNone(res)

    def test_no_session_id_fails_open(self):
        self.assertIsNone(mod._on_pre_tool_call("read_file", {"path": "/etc/passwd"}))


if __name__ == "__main__":
    unittest.main()
