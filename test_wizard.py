"""Unit tests for the tg-projects project wizard (/menu → [➕ Новый] dialog).

Run:  TGP_PLUGIN_DIR=/tmp/opencode/wiz_deploy python3 /mnt/mydisk/sd1/test_wizard.py

Pattern follows test_sessions.py: core modules are stubbed in sys.modules
BEFORE the plugin loads, state.json is replaced by an in-memory dict, and
hermes_cli.projects_db is an in-memory store that records create_project
calls. No network, no Telegram, no writes to real ~/.hermes files.

Without TGP_PLUGIN_DIR the whole module skips (the repo's __init__.py does
not contain the wizard block until it is spliced in; the deployed copy at
~/.hermes/plugins/tg-projects gets it after deploy).
"""
import asyncio
import importlib.util
import os
import sys
import tempfile
import time
import types
import unittest
from contextlib import contextmanager
from pathlib import Path

_PLUGIN_DIR_ENV = os.environ.get("TGP_PLUGIN_DIR")
_HAS_PLUGIN = bool(_PLUGIN_DIR_ENV) and (Path(_PLUGIN_DIR_ENV) / "__init__.py").exists()
if not _HAS_PLUGIN:
    __unittest_skip__ = True
    __unittest_skip_why__ = (
        "TGP_PLUGIN_DIR is not set or does not point at a wizard-enabled plugin "
        "copy (e.g. /tmp/opencode/wiz_deploy)"
    )

# ----------------------------------------------------------------- core stubs
# Pure definitions — installed into sys.modules ONLY when the plugin copy is
# available: a skipped module run (no TGP_PLUGIN_DIR) must leave sys.modules
# untouched so the sibling tests' own stubs keep working in one pytest process.
class _FakeProject:
    def __init__(self, pid, name, slug, primary_path):
        self.id, self.name, self.slug = pid, name, slug
        self.primary_path = primary_path
        self.folders = []


PROJECTS = []          # in-memory projects table
CREATE_CALLS = []      # every create_project invocation, kwargs recorded
SENT = []              # every fake bot send_message, kwargs recorded
_STATE = {"data": {"pending_cwd": {}, "thread_to_project": {}}}
_ENV = {}


@contextmanager
def _connect_closing(db_path=None):
    yield None


def _list_projects(conn, include_archived=False):
    return list(PROJECTS)


def _find_by_primary_path(conn, path):
    key = os.path.normcase(os.path.abspath(str(path or "").strip()))
    for p in PROJECTS:
        if p.primary_path and os.path.normcase(os.path.abspath(p.primary_path)) == key:
            return p
    return None


def _create_project(conn, *, name, slug=None, primary_path=None, **kw):
    CREATE_CALLS.append({"name": name, "slug": slug, "primary_path": primary_path})
    pid = f"p_wiz{len(CREATE_CALLS)}"
    PROJECTS.append(_FakeProject(pid, name, slug or str(name).lower(), primary_path))
    return pid


def _get_session_env(name, default=""):
    return _ENV.get(name, default)


# telegram stub: the plugin imports InlineKeyboardButton/Markup inside functions
class _Button:
    def __init__(self, text, callback_data=None):
        self.text = text
        self.callback_data = callback_data


class _Markup:
    def __init__(self, rows):
        self.rows = rows


class _FakeBot:
    async def send_message(self, **kwargs):
        SENT.append(kwargs)
        return True


mod = None
if _HAS_PLUGIN:
    fake_projects = types.ModuleType("hermes_cli.projects_db")
    fake_projects.connect_closing = _connect_closing
    fake_projects.list_projects = _list_projects
    fake_projects.find_by_primary_path = _find_by_primary_path
    fake_projects.create_project = _create_project

    fake_session_ctx = types.ModuleType("gateway.session_context")
    fake_session_ctx.get_session_env = _get_session_env

    fake_telegram = types.ModuleType("telegram")
    fake_telegram.InlineKeyboardButton = _Button
    fake_telegram.InlineKeyboardMarkup = _Markup
    fake_telegram_ext = types.ModuleType("telegram.ext")
    fake_telegram_ext.CallbackQueryHandler = object

    # pytest imports every test module BEFORE running any test, so the
    # sibling tests' own stubs may already sit in sys.modules. The wizard
    # must run against THIS recorder-equipped projects_db, so the entries
    # are hard-set for THIS module's tests and swapped back out after each
    # one — the siblings' runtime lookups never see them.
    _SWAP_KEYS = ("hermes_cli", "hermes_cli.projects_db", "gateway",
                  "gateway.session_context", "telegram", "telegram.ext")
    _PREV_MODULES = {k: sys.modules.get(k) for k in _SWAP_KEYS}

    def _install_mine():
        sys.modules["hermes_cli"] = types.ModuleType("hermes_cli")
        sys.modules["hermes_cli"].projects_db = fake_projects
        sys.modules["hermes_cli.projects_db"] = fake_projects
        sys.modules["gateway"] = types.ModuleType("gateway")
        sys.modules["gateway"].session_context = fake_session_ctx
        sys.modules["gateway.session_context"] = fake_session_ctx
        sys.modules["telegram"] = fake_telegram
        sys.modules["telegram"].ext = fake_telegram_ext
        sys.modules["telegram.ext"] = fake_telegram_ext

    def _restore_prev():
        for k, v in _PREV_MODULES.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v

    _install_mine()  # the plugin load below sees the fakes

    _PLUGIN_DIR = Path(_PLUGIN_DIR_ENV)
    spec = importlib.util.spec_from_file_location(
        "tg_projects_wizard_under_test", str(_PLUGIN_DIR / "__init__.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["tg_projects_wizard_under_test"] = mod
    spec.loader.exec_module(mod)
    _restore_prev()

    # state.json -> in-memory dict (same trick as test_sessions._StateStub)
    def _load_state_stub():
        return dict(_STATE["data"])

    def _save_state_stub(state):
        _STATE["data"] = state

    mod._load_state = _load_state_stub
    mod._save_state = _save_state_stub

    mod._NATIVE = types.SimpleNamespace(bot=_FakeBot())
    mod._ADAPTER = None

CHAT = "7559860199"
TOPIC = "64999"
KEY = f"{CHAT}:{TOPIC}"


def _fake_query(chat_id=7559860199, thread_id=64999, user_id=7559860199):
    async def _answer(*a, **kw):
        return None
    msg = types.SimpleNamespace(
        chat=types.SimpleNamespace(id=chat_id),
        message_thread_id=thread_id,
        message_id=10,
        from_user=types.SimpleNamespace(id=user_id, username="owner"),
    )
    return types.SimpleNamespace(message=msg, from_user=types.SimpleNamespace(id=user_id),
                                 data="", answer=_answer)


def _event(text, chat_id=CHAT, thread_id=TOPIC, user_id=CHAT, platform="telegram",
           internal=False, chat_type="dm"):
    src = types.SimpleNamespace(
        platform=types.SimpleNamespace(value=platform),
        chat_id=chat_id, thread_id=thread_id, user_id=user_id, chat_type=chat_type)
    return types.SimpleNamespace(source=src, text=text, internal=internal, message_id="1")


def _dispatch(text, **kw):
    return asyncio.run(mod._on_wizard_pre_gateway_dispatch(_event(text, **kw)))


def _button(data, **kw):
    asyncio.run(mod._wizard_on_button(_fake_query(**kw), data))


def _sent():
    return SENT[-1] if SENT else {}


def _kb_btn(sent_kwargs):
    rows = getattr(sent_kwargs.get("reply_markup"), "rows", None)
    return rows[0][0] if rows and rows[0] else None


class _WizardTest(unittest.TestCase):
    """Shared fixture: my stubs swapped in, fresh state/store/sends."""

    def setUp(self):
        self.assertIsNotNone(mod, "plugin not loaded (TGP_PLUGIN_DIR)")
        _install_mine()
        _STATE["data"] = {"pending_cwd": {}, "thread_to_project": {}}
        PROJECTS.clear()
        CREATE_CALLS.clear()
        SENT.clear()
        _ENV.clear()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def tearDown(self):
        _restore_prev()

    def _start(self):
        _button("tgp:pw:start")

    def _to_path_step(self, name="SD2"):
        self._start()
        result = _dispatch(name)
        self.assertEqual(result, {"action": "skip", "reason": "project_wizard"})
        return self.tmp.name


@unittest.skipUnless(_HAS_PLUGIN, "TGP_PLUGIN_DIR not set")
class TranslitTests(_WizardTest):
    """_wizard_slug: cyrillic + latin + digits, kebab-case, caps."""

    def test_cyrillic_proekt_test(self):
        self.assertEqual(mod._wizard_slug("Проект Тест"), "proekt-test")

    def test_mixed_cyrillic_latin_digits(self):
        self.assertEqual(mod._wizard_slug("Проект SD 2"), "proekt-sd-2")
        self.assertEqual(mod._wizard_slug("SD1-backup"), "sd1-backup")

    def test_full_cyrillic_words(self):
        self.assertEqual(mod._wizard_slug("Вася Пупкин"), "vasya-pupkin")
        self.assertEqual(mod._wizard_slug("Ёжик"), "ezhik")
        self.assertEqual(mod._wizard_slug("Юрия Щукина"), "yuriya-schukina")

    def test_separators_collapse_and_strip(self):
        self.assertEqual(mod._wizard_slug("  Мой -- проект!!  "), "moy-proekt")

    def test_empty_and_garbage_falls_back(self):
        self.assertEqual(mod._wizard_slug(""), "project")
        self.assertEqual(mod._wizard_slug("!!!"), "project")

    def test_cap_at_64(self):
        self.assertEqual(len(mod._wizard_slug("a" * 80)), 64)

    def test_uppercase_input_lowered(self):
        self.assertEqual(mod._wizard_slug("ПРОЕКТ ТЕСТ"), "proekt-test")


@unittest.skipUnless(_HAS_PLUGIN, "TGP_PLUGIN_DIR not set")
class ValidationTests(_WizardTest):
    """Name and path validators, duplicate checks against projects.db."""

    def test_empty_name_rejected(self):
        self.assertIn("пустым", mod._wizard_validate_name(""))

    def test_long_name_rejected(self):
        err = mod._wizard_validate_name("x" * 101)
        self.assertIn("100", err)

    def test_name_100_chars_ok(self):
        self.assertIsNone(mod._wizard_validate_name("x" * 100))

    def test_relative_path_rejected(self):
        err = mod._wizard_path_error("SD2", "home/meow/sd2")
        self.assertIn("не абсолютный", err)

    def test_missing_dir_rejected(self):
        err = mod._wizard_path_error("SD2", "/nonexistent/wiz/xyz")
        self.assertIn("не существует", err)

    def test_duplicate_slug_rejected(self):
        PROJECTS.append(_FakeProject("p1", "Проект Тест", "proekt-test", "/mnt/other"))
        err = mod._wizard_path_error("Проект Тест", self.tmp.name)
        self.assertIn("proekt-test", err)
        self.assertIn("уже занят", err)

    def test_duplicate_primary_path_rejected(self):
        PROJECTS.append(_FakeProject("p2", "Other", "other", self.tmp.name))
        err = mod._wizard_path_error("Совсем Другое", self.tmp.name)
        self.assertIn("уже принадлежит", err)

    def test_clean_path_passes(self):
        self.assertIsNone(mod._wizard_path_error("SD2", self.tmp.name))

    def test_archived_slug_counts_as_taken(self):
        archived = _FakeProject("p3", "Old", "old-slug", "/mnt/old")
        PROJECTS.append(archived)
        err = mod._wizard_path_error("Old Slug", self.tmp.name)
        self.assertIn("old-slug", err)

    def test_create_records_and_returns(self):
        pid, slug, err = mod._wizard_create("Проект Тест", self.tmp.name)
        self.assertIsNone(err)
        self.assertEqual(slug, "proekt-test")
        self.assertEqual(pid, "p_wiz1")
        self.assertEqual(CREATE_CALLS, [{"name": "Проект Тест", "slug": "proekt-test",
                                         "primary_path": self.tmp.name}])

    def test_create_failure_returns_error_text(self):
        def _boom(conn, **kw):
            raise ValueError("folder already belongs to project 'x' (p_x)")
        orig = fake_projects.create_project
        fake_projects.create_project = _boom
        try:
            pid, slug, err = mod._wizard_create("SD2", self.tmp.name)
        finally:
            fake_projects.create_project = orig
        self.assertIsNone(pid)
        self.assertIn("projects.db", err)


@unittest.skipUnless(_HAS_PLUGIN, "TGP_PLUGIN_DIR not set")
class DialogFlowTests(_WizardTest):
    """The full dialog: start → name → path → create_project → state cleared."""

    def test_start_asks_name_with_cancel_button(self):
        self._start()
        entry = mod._wizard_get(KEY)
        self.assertEqual(entry["step"], "name")
        self.assertIsNone(entry["name"])
        self.assertIsNone(entry["project_id"])
        self.assertEqual(entry["user_id"], CHAT)
        self.assertEqual(_sent()["text"], "Название проекта?")
        self.assertEqual(_sent()["chat_id"], "7559860199")
        self.assertEqual(_sent()["message_thread_id"], 64999)
        btn = _kb_btn(_sent())
        self.assertEqual(btn.callback_data, "tgp:pw:cancel")
        self.assertEqual(btn.text, "❌ Отмена")

    def test_full_dialog_creates_project(self):
        self._to_path_step("SD2")
        # confirmation + path question
        self.assertIn("✓ Название: SD2", _sent()["text"])
        self.assertIn("Путь к каталогу", _sent()["text"])
        self.assertEqual(mod._wizard_get(KEY)["step"], "path")
        self.assertEqual(mod._wizard_get(KEY)["name"], "SD2")

        result = _dispatch(self.tmp.name)
        self.assertEqual(result, {"action": "skip", "reason": "project_wizard"})
        self.assertEqual(CREATE_CALLS, [{"name": "SD2", "slug": "sd2",
                                         "primary_path": self.tmp.name}])
        # wizard state cleared
        self.assertIsNone(mod._wizard_get(KEY))
        self.assertNotIn("wizard", _STATE["data"])
        # success message + select-project button
        self.assertEqual(_sent()["text"], f"✅ Проект создан: SD2 (sd2, {self.tmp.name})")
        btn = _kb_btn(_sent())
        self.assertEqual(btn.callback_data, "tgp:pb:proj")
        self.assertEqual(btn.text, "📂 Выбрать проект")

    def test_name_answer_is_consumed_not_dispatched(self):
        self._start()
        result = _dispatch("Мой Проект")
        self.assertEqual(result, {"action": "skip", "reason": "project_wizard"})
        self.assertEqual(mod._wizard_get(KEY)["step"], "path")

    def test_after_success_text_passes_through(self):
        self._to_path_step("SD2")
        _dispatch(self.tmp.name)
        self.assertIsNone(_dispatch("привет, работаем"))
        self.assertEqual(CREATE_CALLS[-1]["name"], "SD2")  # no second create

    def test_path_answer_accepts_quoted_path(self):
        self._to_path_step("SD2")
        result = _dispatch(f'"{self.tmp.name}"')
        self.assertEqual(result, {"action": "skip", "reason": "project_wizard"})
        self.assertEqual(CREATE_CALLS[0]["primary_path"], self.tmp.name)

    def test_cyrillic_name_transliterated_in_slug(self):
        self._to_path_step("Проект Тест")
        _dispatch(self.tmp.name)
        self.assertEqual(CREATE_CALLS[0]["slug"], "proekt-test")
        self.assertIn("proekt-test", _sent()["text"])


@unittest.skipUnless(_HAS_PLUGIN, "TGP_PLUGIN_DIR not set")
class ErrorRetryTests(_WizardTest):
    """Invalid answers: error message + repeated question, state preserved."""

    def test_long_name_repeats_question(self):
        self._start()
        result = _dispatch("x" * 120)
        self.assertEqual(result, {"action": "skip", "reason": "project_wizard"})
        self.assertIn("длиннее", _sent()["text"])
        self.assertIn("Название проекта?", _sent()["text"])
        self.assertEqual(mod._wizard_get(KEY)["step"], "name")
        # retry succeeds
        _dispatch("SD2")
        self.assertEqual(mod._wizard_get(KEY)["step"], "path")

    def test_missing_path_repeats_question(self):
        self._to_path_step("SD2")
        result = _dispatch("/nonexistent/wiz/xyz")
        self.assertEqual(result, {"action": "skip", "reason": "project_wizard"})
        self.assertIn("не существует", _sent()["text"])
        self.assertIn("Путь к каталогу", _sent()["text"])
        self.assertEqual(mod._wizard_get(KEY)["step"], "path")
        self.assertEqual(CREATE_CALLS, [])
        # retry with a real path succeeds
        _dispatch(self.tmp.name)
        self.assertEqual(len(CREATE_CALLS), 1)

    def test_duplicate_path_blocks_creation(self):
        PROJECTS.append(_FakeProject("p1", "SD1", "sd1", self.tmp.name))
        self._to_path_step("SD2")
        result = _dispatch(self.tmp.name)
        self.assertEqual(result, {"action": "skip", "reason": "project_wizard"})
        self.assertIn("уже принадлежит", _sent()["text"])
        self.assertEqual(CREATE_CALLS, [])
        self.assertIsNotNone(mod._wizard_get(KEY))


@unittest.skipUnless(_HAS_PLUGIN, "TGP_PLUGIN_DIR not set")
class CancelTests(_WizardTest):
    """Cancel button and the /menu reset helper drop the dialog state."""

    def test_cancel_button_drops_state(self):
        self._start()
        _button("tgp:pw:cancel")
        self.assertIsNone(mod._wizard_get(KEY))
        self.assertIn("отменено", _sent()["text"].lower())
        # subsequent free text passes to the session
        self.assertIsNone(_dispatch("привет"))

    def test_cancel_mid_dialog(self):
        self._to_path_step("SD2")
        _button("tgp:pw:cancel")
        self.assertIsNone(mod._wizard_get(KEY))
        self.assertIsNone(_dispatch(self.tmp.name))
        self.assertEqual(CREATE_CALLS, [])

    def test_menu_reset_drops_all_topics_of_chat(self):
        self._start()
        mod._wizard_put(f"{CHAT}:777", {"step": "name", "name": None, "updated_at": int(time.time())})
        mod._wizard_reset_chat(CHAT)
        self.assertIsNone(mod._wizard_get(KEY))
        self.assertIsNone(mod._wizard_get(f"{CHAT}:777"))
        self.assertIsNone(_dispatch("текст"))

    def test_unknown_pw_callback_is_ignored(self):
        self._start()
        _button("tgp:pw:weird")
        self.assertEqual(mod._wizard_get(KEY)["step"], "name")  # state intact

    def test_start_overwrites_previous_dialog(self):
        self._to_path_step("SD2")
        self._start()
        self.assertEqual(mod._wizard_get(KEY)["step"], "name")
        self.assertIsNone(mod._wizard_get(KEY)["name"])


@unittest.skipUnless(_HAS_PLUGIN, "TGP_PLUGIN_DIR not set")
class TimeoutTests(_WizardTest):
    """A state older than 10 minutes is ignored, not deleted."""

    def test_stale_state_passes_text_through(self):
        self._start()
        entry = mod._wizard_get(KEY)
        entry["updated_at"] = int(time.time()) - 700
        mod._wizard_put(KEY, entry)
        self.assertIsNone(_dispatch("ответ"))
        # not deleted — just dead
        self.assertIsNotNone(mod._wizard_get(KEY))

    def test_fresh_state_still_consumes(self):
        self._start()
        entry = mod._wizard_get(KEY)
        entry["updated_at"] = int(time.time()) - 500
        mod._wizard_put(KEY, entry)
        self.assertEqual(_dispatch("SD2"), {"action": "skip", "reason": "project_wizard"})


@unittest.skipUnless(_HAS_PLUGIN, "TGP_PLUGIN_DIR not set")
class GuardTests(_WizardTest):
    """The hook must fail open for everything that is not a dialog answer."""

    def test_no_state_passes_through(self):
        self.assertIsNone(_dispatch("обычное сообщение"))

    def test_commands_pass_through(self):
        self._start()
        self.assertIsNone(_dispatch("/stop"))
        self.assertIsNone(_dispatch("/menu"))
        self.assertEqual(mod._wizard_get(KEY)["step"], "name")

    def test_internal_events_pass_through(self):
        self._start()
        self.assertIsNone(_dispatch("текст", internal=True))

    def test_non_telegram_passes_through(self):
        self._start()
        self.assertIsNone(_dispatch("текст", platform="discord"))

    def test_other_sender_passes_through(self):
        self._start()
        self.assertIsNone(_dispatch("текст", user_id="666"))
        self.assertEqual(mod._wizard_get(KEY)["step"], "name")  # state intact

    def test_other_thread_passes_through(self):
        self._start()
        self.assertIsNone(_dispatch("текст", thread_id="777"))
        self.assertEqual(mod._wizard_get(KEY)["step"], "name")

    def test_group_chat_wizard_still_works(self):
        # chat_type is not a gate: the state key + sender match are the gate
        self._start()
        result = _dispatch("SD2", chat_type="group")
        self.assertEqual(result, {"action": "skip", "reason": "project_wizard"})

    def test_exception_fails_open(self):
        class _Boom:
            @property
            def source(self):
                raise RuntimeError("boom")
        self.assertIsNone(asyncio.run(mod._on_wizard_pre_gateway_dispatch(_Boom())))

    def test_none_source_fails_open(self):
        self.assertIsNone(asyncio.run(
            mod._on_wizard_pre_gateway_dispatch(types.SimpleNamespace(source=None, text="x"))))


@unittest.skipUnless(_HAS_PLUGIN, "TGP_PLUGIN_DIR not set")
class RegisterTests(_WizardTest):
    """register() wires the wizard hook BEFORE the handoff hook."""

    def test_hook_order_in_register(self):
        calls = []

        class _Ctx:
            def register_command(self, *a, **kw):
                return None

            def register_hook(self, name, cb):
                calls.append(name)

            def register_platform_handler(self, *a, **kw):
                return None

        # handoff import needs its sibling module; point it at the same dir
        sys.path.insert(0, str(Path(_PLUGIN_DIR_ENV)))
        try:
            mod.register(_Ctx())
        finally:
            sys.path.remove(str(Path(_PLUGIN_DIR_ENV)))
        pgd = [i for i, n in enumerate(calls) if n == "pre_gateway_dispatch"]
        self.assertEqual(len(pgd), 2)  # wizard + handoff
        self.assertLess(pgd[0], pgd[1])  # wizard first


if __name__ == "__main__":
    unittest.main(verbosity=2)
