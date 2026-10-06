"""Stub-based unit checks for tg-projects topic->project cwd binding.

Run:  python3 /home/meow/.hermes/plugins/tg-projects/test_topic_cwd.py
No network, no real Telegram messages, no getUpdates. Stubs replace the
hermes core modules (yaml, tools.terminal_tool, hermes_cli.projects_db,
gateway.session_context) BEFORE the plugin loads, so the checks exercise the
plugin's own logic: thread->topic->project matching, pending-pin priority,
idempotency, and no-crash behaviour on empty/broken data.
"""
import importlib.util
import os
import sys
import types
import unittest
from contextlib import contextmanager
from pathlib import Path

PLUGIN_DIR = Path("/home/meow/.hermes/plugins/tg-projects")


fake_yaml = types.ModuleType("yaml")


def _fake_safe_load(text):
    return {
        "platforms": {"telegram": {"extra": {"dm_topics": [
            {"chat_id": 7559860199, "topics": [
                {"name": "Ambrozia", "thread_id": 64999},
                {"name": "default", "thread_id": 65001},
                {"name": "Grok Abuse", "thread_id": 65003},
                {"name": "BzzGirl", "thread_id": 65006},
                {"name": "NeiroSlop", "thread_id": 65008},
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
    _Project("p3", "Grok Abuse", "grok-abuse", str(PLUGIN_DIR)),
    _Project("p4", "BzzGirl", "bzzgirl", str(PLUGIN_DIR)),
    _Project("p5", "NeiroSlop", "neiroslop", str(PLUGIN_DIR)),
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

_modules = {
    "yaml": fake_yaml,
    "tools": types.ModuleType("tools"),
    "tools.terminal_tool": fake_terminal,
    "hermes_cli": types.ModuleType("hermes_cli"),
    "hermes_cli.projects_db": fake_projects,
    "gateway": types.ModuleType("gateway"),
    "gateway.session_context": fake_session_ctx,
}
for name, mod in _modules.items():
    # setdefault at import time: under `unittest discover` all test modules
    # import during discovery (alphabetical) BEFORE any test runs, so a hard
    # assignment here would clobber the sibling file's stubs. The per-test
    # swap happens inside _with_env instead.
    sys.modules.setdefault(name, mod)
sys.modules["tools"].terminal_tool = fake_terminal
sys.modules["hermes_cli"].projects_db = fake_projects
sys.modules["gateway"].session_context = fake_session_ctx

# Load the plugin as a module under its real name.
spec = importlib.util.spec_from_file_location(
    "tg_projects_under_test", str(PLUGIN_DIR / "__init__.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


@contextmanager
def _with_env(**env):
    _ENV.update(env)
    mod._CWD_APPLIED.clear()
    mod._CWD_REGISTERED.clear()
    fake_terminal._calls.clear()
    fake_terminal._cwd.clear()
    fake_terminal._session_cwd.clear()
    # Re-install THIS file's stubs over any stubs a sibling test module left in
    # sys.modules (unittest discover runs test_sessions first, and its fake
    # yaml/terminal otherwise win and break these checks). Sibling stubs are
    # restored afterwards so their own assertions keep working either order.
    _saved_modules = {name: sys.modules.get(name) for name in _modules}
    for name, m in _modules.items():
        sys.modules[name] = m
    try:
        yield
    finally:
        for k in env:
            _ENV.pop(k, None)
        mod._CWD_APPLIED.clear()
        mod._CWD_REGISTERED.clear()
        fake_terminal._calls.clear()
        for name, m in _saved_modules.items():
            if m is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = m


class _StateStub:
    """Replace state.json reads/writes with an in-memory dict."""

    def __init__(self):
        self.data = {"pending_cwd": {}, "thread_to_project": {}}

    def install(self):
        self._orig_load, self._orig_save = mod._load_state, mod._save_state

        def _load():
            return {
                "pending_cwd": dict(self.data.get("pending_cwd") or {}),
                "thread_to_project": dict(self.data.get("thread_to_project") or {}),
                "topic_bindings": dict(self.data.get("topic_bindings") or {}),
            }

        def _save(state):
            self.data = state

        mod._load_state, mod._save_state = _load, _save

    def pin(self, sk, cwd):
        self.data.setdefault("pending_cwd", {})[sk] = {
            "project_id": 99, "name": "X", "slug": "x", "cwd": cwd, "ts": 0}

    def legacy_map(self, thread_id, cwd):
        """A pre-binding thread_to_project entry (the step-3 legacy fallback)."""
        self.data.setdefault("thread_to_project", {})[str(thread_id)] = {
            "project_id": 1, "name": "L", "slug": "l", "cwd": cwd, "ts": 0}


class TopicCwdTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.state = _StateStub()
        cls.state.install()

    def setUp(self):
        # the class-level stub is shared: reset the per-topic maps so a
        # legacy_map/ pin left by an earlier test cannot leak into the next
        self.state.data["thread_to_project"] = {}
        self.state.data["topic_bindings"] = {}

    # ------------------------------------------------- thread -> project matching
    def test_thread_maps_to_project_cwd(self):
        with _with_env(HERMES_SESSION_THREAD_ID="65003"):
            self.assertEqual(mod._topic_name_by_thread("65003"), "Grok Abuse")
            self.assertEqual(mod._thread_cwd(), str(PLUGIN_DIR))

    def test_thread_case_insensitive_name_match(self):
        with _with_env(HERMES_SESSION_THREAD_ID="64999"):
            # name "Ambrozia" matches project "Ambrozia" case-insensitively
            self.assertEqual(mod._project_by_name(PROJECTS, "amBROZIA").slug, "ambrozia")
            self.assertEqual(mod._thread_cwd(), str(PLUGIN_DIR))

    def test_unmapped_thread_yields_no_cwd(self):
        with _with_env(HERMES_SESSION_THREAD_ID="12345"):
            self.assertEqual(mod._thread_cwd(), None)

    def test_empty_thread_yields_no_cwd(self):
        with _with_env():
            self.assertEqual(mod._thread_cwd(), None)

    def test_pre_llm_call_binds_topic_project(self):
        # step 3: the hook reads topic_bindings / thread_to_project legacy —
        # NOT config dm_topics. A legacy-mapped thread still gets its cwd.
        with _with_env(HERMES_SESSION_THREAD_ID="65008"):
            self.state.legacy_map("65008", str(PLUGIN_DIR))
            mod._on_pre_llm_call(session_id="sess-1", task_id="sess-1")
            self.assertEqual(fake_terminal._cwd.get("sess-1"), {"cwd": str(PLUGIN_DIR)})
            self.assertEqual(fake_terminal._session_cwd.get("sess-1"), str(PLUGIN_DIR))
            # idempotent: second call is a no-op
            before = len(fake_terminal._calls)
            mod._on_pre_llm_call(session_id="sess-1", task_id="sess-1")
            self.assertEqual(len(fake_terminal._calls), before)

    def test_pre_llm_call_ignores_unbound_topic(self):
        # a thread with neither binding nor legacy map applies NOTHING
        with _with_env(HERMES_SESSION_THREAD_ID="65008"):
            mod._on_pre_llm_call(session_id="sess-8", task_id="sess-8")
            self.assertNotIn("sess-8", fake_terminal._cwd)

    # ------------------------------------------------------- pending-pin priority
    def test_pending_pin_wins_over_topic(self):
        with _with_env(HERMES_SESSION_THREAD_ID="64999", HERMES_SESSION_KEY="k1"):
            # topic Ambrozia, but a pending pin for another cwd wins
            self.state.pin("k1", str(PLUGIN_DIR) + "/other")
            Path(str(PLUGIN_DIR) + "/other").mkdir(exist_ok=True)
            try:
                mod._on_pre_llm_call(session_id="sess-2", task_id="sess-2")
                self.assertEqual(fake_terminal._cwd.get("sess-2"), {"cwd": str(PLUGIN_DIR) + "/other"})
            finally:
                (Path(str(PLUGIN_DIR) + "/other")).rmdir()

    def test_on_session_start_prefers_pending_then_consumes(self):
        with _with_env(HERMES_SESSION_THREAD_ID="65006", HERMES_SESSION_KEY="k1"):
            # legacy map present, but the pending pin wins
            self.state.legacy_map("65006", str(PLUGIN_DIR))
            self.state.pin("k1", str(PLUGIN_DIR) + "/pin-cwd")
            Path(str(PLUGIN_DIR) + "/pin-cwd").mkdir(exist_ok=True)
            try:
                mod._on_session_start(session_id="sess-3", model="m")
                self.assertEqual(fake_terminal._cwd.get("sess-3"), {"cwd": str(PLUGIN_DIR) + "/pin-cwd"})
                # pin consumed -> next call falls back to the legacy map
                mod._on_session_start(session_id="sess-4", model="m")
                self.assertEqual(fake_terminal._cwd.get("sess-4"), {"cwd": str(PLUGIN_DIR)})
                self.assertNotIn("k1", self.state.data.get("pending_cwd") or {})
            finally:
                (Path(str(PLUGIN_DIR) + "/pin-cwd")).rmdir()

    # -------------------------------------------------------------- no-crash paths
    def test_no_thread_no_pending_no_crash(self):
        with _with_env():
            mod._on_session_start(session_id="sess-5", model="m")  # must not raise
            mod._on_pre_llm_call(session_id="sess-6", task_id="sess-6")  # must not raise
            self.assertNotIn("sess-5", fake_terminal._cwd)
            self.assertNotIn("sess-6", fake_terminal._cwd)

    def test_stale_pending_pin_is_dropped(self):
        with _with_env(HERMES_SESSION_THREAD_ID="64999", HERMES_SESSION_KEY="k1"):
            self.state.legacy_map("64999", str(PLUGIN_DIR))
            self.state.pin("k1", "/definitely/does/not/exist-xyz")
            mod._on_session_start(session_id="sess-7", model="m")
            # stale pin dropped, legacy map fallback still applies
            self.assertEqual(fake_terminal._cwd.get("sess-7"), {"cwd": str(PLUGIN_DIR)})

    def test_empty_topic_name_ignored(self):
        self.assertEqual(mod._project_by_name(PROJECTS, "   "), None)
        self.assertEqual(mod._project_by_name(PROJECTS, ""), None)

    def test_thread_id_fractional_suffix(self):
        self.assertEqual(mod._norm_thread_id("65003.123"), 65003)
        self.assertEqual(mod._norm_thread_id("junk"), None)

    def test_unknown_project_name_falls_back_to_slug(self):
        # "Grok Abuse" slugifies to grok-abuse -> matches project slug
        self.assertEqual(mod._project_by_name(PROJECTS, "Grok Abuse").id, "p3")


if __name__ == "__main__":
    unittest.main(verbosity=2)
