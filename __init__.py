"""tg-projects — Telegram project switcher.

Commands
  /projects          project list. In Telegram the reply is an inline keyboard:
                     one button per project; pressing it opens a per-project
                     menu. The command handler returns only a str, so when the
                     Telegram handler factory has not run yet (or the chat id
                     is unknown) the same data is returned as plain text.
  /pnew <N|slug>     pin a project's primary_path as the working directory for
                     the NEXT session created on this chat (kept as a text
                     command; the "new session" button does the same).
  /pproject <name> <abs/path> [new-folder]
                     create a NEW project (hermes_cli.projects_db.create_project)
                     and a new Telegram forum topic for it (adapter's DM topic
                     helpers). [new-folder] creates the missing folder first;
                     without the flag a missing path is an error. The
                     thread_id -> project mapping the adapter does NOT have is
                     kept in this plugin's state.json (never in config.yaml);
                     the topic -> project cwd binder reads state.json first and
                     config.yaml's dm_topics as the fallback.
  /model             model picker, fed synthetically from the project menu's
                     "Модель" button so the gateway renders the inline provider
                     drill-down into this chat/topic.

Sessions (state.db is read read-only, ``WHERE cwd = ?``, no source filter — so
  desktop sessions created by the Hermes desktop app appear next to Telegram
  ones; a ``🖥️`` marks a desktop session)
  tgp:a:<i>  "все сессии проекта" — up to 8 sessions of the project's cwd
  tgp:t:<id> continue THAT session (cwd override + record_session_cwd for the
             session id, then a synthetic /resume <id>), and show the last
             1-2 user messages ("на чём остановились")
  Desktop sessions carry a warning: driving the same session from the desktop
  and the phone at once interleaves history from two processes.

Inline buttons (callback_data, all <= 64 bytes; project ids are 1-based indices
  into the /projects listing, stable within one chat; the "back" button is a
  fixed string with no id; <id> is a state.db session id)
  tgp:p:<i>     open project <i> menu
  tgp:m:<i>     project <i> menu (from the project button)
  tgp:n:<i>     new session in project <i>   (= /pnew <i> + /new)
  tgp:r:<i>     continue the last session of project <i>
  tgp:a:<i>     all sessions of project <i> (desktop + telegram)
  tgp:s:<id>    continue session <id>
  tgp:l:<i>     re-list the sessions of project <i>
  tgp:d:<i>     model picker for project <i> (/model)
  tgp:np        new-project flow (text /pproject)
  tgp:b         back to the project list

Handlers are registered with `pattern=r"^tgp:"` only — an unscoped
CallbackQueryHandler would swallow the core button flows (exec approvals, model
picker, clarify prompts). Button taps are auth-gated with the adapter's
``_callback_authorized`` (the same gate the core pickers use); a tap from a
non-allowlisted user gets a refusal, and no plugin state is touched.

New session / continue
  * "new session" pins the project exactly like /pnew (state.json, keyed by the
    chat's session key) and then feeds a synthetic ``/new`` MessageEvent into
    the gateway via ``adapter.handle_message``. The on_session_start hook
    applies the pin to the freshly created session id. ``/new`` is a bypass
    command: while a turn is running it is dispatched inline by handle_message
    and cancels the active turn first, so the injection is safe in both idle
    and busy states.
  * "continue" registers the project cwd for the target session id RIGHT AWAY
    (tools.terminal_tool.register_task_env_overrides + record_session_cwd — the
    resumed session's task_id IS its session id), feeds a synthetic
    ``/resume <id>`` into the gateway, and answers with the last 1-2 user
    messages ("на чём остановились").

State
  ~/.hermes/plugins/tg-projects/state.json
    {
     "topic_bindings": {"<chat_id>:<thread_id>": {"project_id", "project_name", "cwd", "session_id", "updated_at"}},
     "pending_cwd": {"<session_key>": {"project_id", "name", "slug", "cwd", "ts"}},
     "thread_to_project": {"<thread_id>": {"project_id", "name", "slug", "cwd", "ts"}}
    }
  topic_bindings is the topic -> workplace map (step 3): a topic is a working
  place = (project_id, cwd, optional session_id). on_session_start /
  pre_llm_call take the cwd ONLY from here; a topic without a binding gets a
  "bind this topic" warning instead of a silently default-/home directory.
  pending_cwd is the /pnew / "new session" pin, keyed by the chat's session
  key — legacy fallback for lanes that never got a topic binding.
  thread_to_project is the DEPRECATED pre-binding topic -> project map:
  read ONLY as the last-resort fallback for topics that never got a
  topic_binding (no config.yaml dm_topics reads — that is the core
  adapter's layer).

Hook wiring
  ``on_session_start`` is a real Hermes plugin hook
  (hermes_cli.plugins.VALID_HOOKS) and the core fires it from
  agent/conversation_loop.py on the first turn of a session whose durable id
  is new, with ``session_id=agent.session_id``. It does NOT run on the
  ``/new`` command itself, so the pin is stored keyed by the chat's session key
  and applied on the first turn of the new session.

No monkey patches — only public surfaces: ``ctx.register_command``,
``ctx.register_hook``, ``ctx.register_platform_handler``,
``tools.terminal_tool.register_task_env_overrides`` / ``record_session_cwd``,
``hermes_cli.projects_db.connect_closing`` / ``list_projects``,
``gateway.session_context.get_session_env``, ``yaml.safe_load`` (config.yaml
read-only), and the platform adapter's ``_callback_ctx`` /
``_callback_authorized`` / ``_accept_update`` / ``handle_message`` /
``build_source``.

Topic -> workplace cwd (step 3)
  A topic IS a working place: ``state.json["topic_bindings"]`` maps
  ``"<chat_id>:<thread_id>"`` to ``(project_id, project_name, cwd,
  session_id?)``. ``on_session_start`` and the idempotent ``pre_llm_call``
  hook apply the binding's cwd (primary), the legacy ``/pnew`` pending pin
  (fallback), or NOTHING when the topic is unbound — the user then gets a
  "bind this topic" warning in the topic instead of a silently
  default-directory session. The deprecated dm_topics/thread_to_project
  name-matching path survives only in the legacy tests.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import sys
import threading
import time
import types
from contextlib import suppress as _suppress
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("hermes_plugins.tg_projects")

_PLUG_DIR = Path(__file__).resolve().parent
_STATE_FILE = _PLUG_DIR / "state.json"

CB_PREFIX = "tgp:"
_CB_RE = re.compile(r"^tgp:([a-z]):(\d+)$")
# Session ids are timestamp-based ("20261003_062445_2d0fd7"); allow compact
# ids for future formats as well. callback_data must stay <= 64 bytes.
_CB_SESSION_RE = re.compile(r"^tgp:s:([A-Za-z0-9_\-]{8,58})$")
_BACK_CB = "tgp:b"

# Set by register() via the Telegram platform handler factory; None until the
# adapter connects, so command handlers fall back to plain text.
_NATIVE: Optional[Any] = None
_ADAPTER: Optional[Any] = None
# The gateway's event loop, captured when the Telegram factory wires in (the
# factory itself runs on the loop). Sync hooks in worker threads use it to
# schedule async sends (run_coroutine_threadsafe); None until wired.
_WIRE_LOOP: Optional[asyncio.AbstractEventLoop] = None

# --------------------------------------------------------------------- fallback text
def _fallback_notice() -> str:
    return (
        "⚠️ Кнопки Telegram сейчас недоступны (фабрика плагина ещё не подключилась) — "
        "текстовые эквиваленты: новая сессия в проекте N — /pnew N, затем /new; "
        "продолжить последнюю сессию проекта — /resume <id-сессии>; список — /projects."
    )


def _plain_projects_notice() -> str:
    return "⚠️ Клавиатура недоступна (не удалось определить чат) — текстовый вариант:\n" + _fallback_notice()


# --------------------------------------------------------------------------- helpers
def _import_hermes_module(name: str, attr: str | None = None):
    """Import a core module under the name the gateway process knows it by.

    The gateway runs from the install root, so ``tools.`` / ``gateway.`` /
    ``hermes_cli.`` resolve natively. If an import fails (plugin loaded in an
    isolated namespace), check sys.modules for a test stub, then prepend the
    repo under HERMES_HOME and retry.
    """
    try:
        module = __import__(name, fromlist=["*"])
        return getattr(module, attr) if attr else module
    except (ImportError, ModuleNotFoundError):
        stub = sys.modules.get(name)
        if stub is not None:
            return getattr(stub, attr) if attr else stub
        home = Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes"))
        repo = home / "hermes-agent"
        if str(repo) not in sys.path:
            sys.path.insert(0, str(repo))
        module = __import__(name, fromlist=["*"])
        return getattr(module, attr) if attr else module


def _hermes_home() -> Path:
    override = os.environ.get("HERMES_HOME")
    if override:
        return Path(override).expanduser()
    try:
        return Path(_import_hermes_module("hermes_constants", "get_hermes_home")())
    except Exception:
        return Path.home() / ".hermes"


def _session_env(name: str, default: str = "") -> str:
    """A HERMES_SESSION_* value: bound contextvar first, then os.environ."""
    try:
        value = _import_hermes_module("gateway.session_context", "get_session_env")(
            name, default
        )
        if value:
            return str(value)
    except Exception:
        pass
    return str(os.environ.get(name, default) or default)


def _current_session_key() -> str:
    """The chat's durable gateway session key (bound by the dispatch layer)."""
    return _session_env("HERMES_SESSION_KEY", "")


def _current_thread_id() -> str:
    """The current session's Telegram topic id (HERMES_SESSION_THREAD_ID)."""
    return _session_env("HERMES_SESSION_THREAD_ID", "")


# ------------------------------------------------------------- topic bindings
# A topic is a working place: (project_id, project_name, cwd, session_id?).
# The map lives in state.json["topic_bindings"] keyed "<chat_id>:<thread_id>".
_BINDING_WARN_EVERY_S = 3600  # rate-limit for the "unbound topic" warning
_BINDING_WARNED: Dict[str, float] = {}


def _topic_binding_key() -> Optional[str]:
    """``f"{chat_id}:{thread_id}"`` for the current session's topic, or None.

    None means the session is not in a forum topic (plain DM without a
    thread) — there is nothing to bind.
    """
    thread_id = _norm_thread_id(_current_thread_id())
    if thread_id is None:
        return None
    chat_id = str(_session_env("HERMES_SESSION_CHAT_ID", "") or "").strip()
    if not chat_id:
        return None
    return f"{chat_id}:{thread_id}"


def _get_topic_binding() -> Optional[Dict[str, Any]]:
    """The binding for the current topic (a copy), or None when unbound."""
    key = _topic_binding_key()
    if key is None:
        return None
    try:
        bindings = _load_state().get("topic_bindings")
        entry = bindings.get(key) if isinstance(bindings, dict) else None
    except Exception:
        logger.warning("tg-projects: topic_bindings unreadable", exc_info=True)
        return None
    return dict(entry) if isinstance(entry, dict) else None


def _set_topic_binding(project_id: Any = None, project_name: Any = None,
                       cwd: Any = None) -> bool:
    """Create or refresh the binding for the current topic.

    ``None`` arguments keep the stored value; the key's ``session_id`` is
    NOT touched here (use :func:`_update_binding_session`). Returns False
    when the current session is not in a topic.
    """
    key = _topic_binding_key()
    if key is None:
        return False
    with _CWD_LOCK:
        state = _load_state()
        bindings = state.setdefault("topic_bindings", {})
        entry = bindings.get(key)
        if not isinstance(entry, dict):
            entry = {}
        if project_id is not None:
            entry["project_id"] = str(project_id)
        if project_name is not None:
            entry["project_name"] = str(project_name)
        if cwd is not None:
            entry["cwd"] = str(cwd)
        entry["updated_at"] = int(time.time())
        bindings[key] = entry
        _save_state(state)
    return True


def _update_binding_session(session_id: Optional[str]) -> None:
    """Record the session now working in the current topic (None resets)."""
    key = _topic_binding_key()
    if key is None:
        return
    clean = str(session_id).strip() if session_id is not None else ""
    with _CWD_LOCK:
        state = _load_state()
        bindings = state.get("topic_bindings") or {}
        entry = bindings.get(key)
        if not isinstance(entry, dict):
            return  # nothing to update — bind the topic first
        entry["session_id"] = clean or None
        entry["updated_at"] = int(time.time())
        bindings[key] = entry
        _save_state(state)


def _clear_topic_binding() -> None:
    """Drop the current topic's binding (the topic becomes unbound)."""
    key = _topic_binding_key()
    if key is None:
        return
    with _CWD_LOCK:
        state = _load_state()
        bindings = state.get("topic_bindings") or {}
        if key in bindings:
            bindings.pop(key, None)
            if not bindings:
                state.pop("topic_bindings", None)
            _save_state(state)


def _thread_to_project_cwd() -> Optional[str]:
    """LEGACY fallback: ``thread_to_project[thread_id]["cwd"]`` from state.json.

    Read only when the current topic has NO topic_binding: the pre-binding
    map recorded by /pproject keeps working for already-mapped topics. No
    config.yaml dm_topics reads and no projects.db lookups — the entry
    carries the cwd directly.
    """
    wanted = _norm_thread_id(_current_thread_id())
    if wanted is None:
        return None
    try:
        entry = (_load_state().get("thread_to_project") or {}).get(str(wanted))
    except Exception:
        return None
    if not isinstance(entry, dict):
        return None
    cwd = str(entry.get("cwd") or "").strip()
    return cwd if cwd and os.path.isdir(cwd) else None


def _notify_unbound_topic(session_id: str) -> None:
    """Warn the user (once per topic per hour) that the topic has no binding.

    Best-effort: the hook runs in a worker thread, so the message is
    scheduled onto the gateway's event loop captured at wire time. Without
    a wired bot/loop the warning is logged only.
    """
    key = _topic_binding_key() or "no-topic"
    now = time.time()
    if now - _BINDING_WARNED.get(key, 0.0) < _BINDING_WARN_EVERY_S:
        return
    _BINDING_WARNED[key] = now
    chat_id = str(_session_env("HERMES_SESSION_CHAT_ID", "") or "").strip()
    thread_id = _norm_thread_id(_current_thread_id())
    text = ("⚠️ Этот топик не привязан к проекту. Отправь /menu чтобы выбрать.")
    logger.info("tg-projects: session %s started in unbound topic %s (cwd left NULL)",
                session_id, key)
    loop = _WIRE_LOOP
    native = _NATIVE
    if loop is None or native is None or getattr(native, "bot", None) is None \
            or not chat_id or thread_id is None:
        return
    kwargs: Dict[str, Any] = {"chat_id": chat_id, "text": text}
    if thread_id:
        kwargs["message_thread_id"] = thread_id

    async def _send() -> None:
        try:
            await native.bot.send_message(**kwargs)
        except Exception:
            logger.warning("tg-projects: unbound-topic warning send failed",
                           exc_info=True)

    try:
        asyncio.run_coroutine_threadsafe(_send(), loop)
    except Exception:
        logger.warning("tg-projects: unbound-topic warning scheduling failed",
                       exc_info=True)


# ------------------------------------------------------------------ config -> topic -> project
# In-process caches: the config mtime gate makes externally created topics visible
# without a restart, and _CWD_APPLIED keeps the binders idempotent (one write per
# session per process) so a per-turn hook never re-registers anything.
_CONFIG_TOPIC_CACHE: Dict[str, Any] = {"key": None, "mtime": None, "threads": []}
# _CWD_REGISTERED: the in-memory task-env/session-cwd registration is one-shot
# per session id, so a per-turn hook never re-registers the same cwd.
# _CWD_APPLIED: stamped only after the state.db write succeeds, so a pending
# persist (row not created yet, locked db) is retried on the next hook call.
_CWD_REGISTERED: Dict[str, str] = {}
_CWD_APPLIED: Dict[str, str] = {}


def _norm_thread_id(value: Any) -> Optional[int]:
    """A Telegram topic id as an int; cron-style fractional suffixes are ignored."""
    text = str(value or "").strip()
    if not text:
        return None
    head = text.split(".", 1)[0]
    return int(head) if head.isdigit() else None


def _config_dm_topics() -> List[Dict[str, Any]]:
    """DEPRECATED: ``platforms.telegram.extra.dm_topics`` topic list (read-only).

    Not a cwd source anymore (step 3: topic_bindings is); kept for the legacy
    name-resolution tests only.
    The gateway persists newly created topics back into this file, so the cache is
    keyed by mtime instead of being forever frozen at plugin load.
    """
    path = _hermes_home() / "config.yaml"
    cache = _CONFIG_TOPIC_CACHE
    key = str(path)
    try:
        mtime = path.stat().st_mtime_ns
    except OSError:
        return []
    if cache.get("key") == key and cache.get("mtime") == mtime:
        return cache.get("threads") or []

    topics: List[Dict[str, Any]] = []
    try:
        yaml = _import_hermes_module("yaml")
        cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        raw = (((cfg.get("platforms") or {}).get("telegram") or {}).get("extra") or {}).get("dm_topics") or []
        if isinstance(raw, list):
            for entry in raw:
                if not isinstance(entry, dict):
                    continue
                for topic in entry.get("topics") or []:
                    if isinstance(topic, dict) and topic.get("name"):
                        topics.append(dict(topic))
    except Exception:
        logger.warning("tg-projects: config.yaml dm_topics unreadable", exc_info=True)

    cache["key"], cache["mtime"], cache["threads"] = key, mtime, topics
    return topics


def _state_thread_map() -> Dict[str, Dict[str, Any]]:
    """``state.json["thread_to_project"]``: the plugin's own topic -> project map.

    Populated by /pproject for topics the adapter creates: the adapter's
    ``dm_topics`` config holds only name/thread_id/chat_id and knows nothing
    about projects, so this plugin records the association itself (never in
    config.yaml, which the gateway owns and the user asked not to touch).
    """
    try:
        data = _load_state().get("thread_to_project")
        return data if isinstance(data, dict) else {}
    except Exception:
        logger.warning("tg-projects: thread_to_project unreadable", exc_info=True)
        return {}


def _state_project_name_for_thread(thread_id: Any) -> str:
    """The project name stored for *thread_id*, or '' when unmapped."""
    wanted = _norm_thread_id(thread_id)
    if wanted is None:
        return ""
    entry = _state_thread_map().get(str(wanted))
    if not isinstance(entry, dict):
        return ""
    return str(entry.get("name") or "").strip()


def _topic_name_by_thread(thread_id: Any) -> str:
    """DEPRECATED: topic-name resolution via thread_to_project + config dm_topics.

    Superseded by topic_bindings; kept for the legacy tests.
    Resolution order: this plugin's state.json (topics created by /pproject,
    which the gateway config does not know about) then config.yaml's
    ``platforms.telegram.extra.dm_topics`` (read-only, mtime-cached).
    """
    name = _state_project_name_for_thread(thread_id)
    if name:
        return name
    wanted = _norm_thread_id(thread_id)
    if wanted is None:
        return ""
    for topic in _config_dm_topics():
        if _norm_thread_id(topic.get("thread_id")) == wanted:
            return str(topic.get("name") or "").strip()
    return ""


def _project_by_name(projects: list, name: str):
    """A project matching *name* case-insensitively, else its slugified form."""
    wanted = (name or "").strip().lower()
    if not wanted:
        return None
    for proj in projects:
        if str(getattr(proj, "name", "") or "").strip().lower() == wanted:
            return proj
    slug = re.sub(r"[^a-z0-9]+", "-", wanted).strip("-")
    if not slug:
        return None
    for proj in projects:
        if str(getattr(proj, "slug", "") or "").strip().lower() == slug:
            return proj
    return None


def _thread_cwd() -> Optional[str]:
    """DEPRECATED: the pre-binding topic -> project -> primary_path resolution
    (state thread_to_project + config dm_topics + projects.db name matching).

    Superseded by ``state.json["topic_bindings"]`` (step 3): on_session_start
    and pre_llm_call take the cwd from the binding, never from here. Kept
    only because the legacy tests exercise it; the production fallback is
    :func:`_thread_to_project_cwd` (state.json only, no config reads).
    """
    name = _topic_name_by_thread(_current_thread_id())
    if not name:
        return None
    try:
        projects = _list_projects()
    except Exception:
        logger.warning("tg-projects: topic -> project lookup failed", exc_info=True)
        return None
    project = _project_by_name(projects, name)
    if project is None:
        logger.info("tg-projects: topic %r has no matching project", name)
        return None
    cwd = _project_cwd(project)
    if not cwd:
        logger.info("tg-projects: project for topic %r has no working directory", name)
        return None
    if not os.path.isdir(cwd):
        logger.warning("tg-projects: topic %r project cwd missing on host: %r", name, cwd)
        return None
    return cwd


def _persist_cwd_to_state_db(session_id: str, cwd: str, reason: str) -> bool:
    """Write *cwd* to state.db's ``sessions`` row so the desktop (and any other
    surface) can find this session by project directory.

    Telegram sessions land in state.db with ``sessions.cwd = NULL``; without
    this backfill, the desktop's per-project session view misses them. Uses a
    one-shot ``SessionDB()`` handle (never the gateway's shared writer handle,
    which would need borrowing/refcounting that the plugin has no claim on).
    Returns True when the row was actually written — if the session row does
    not exist yet, ``update_session_cwd`` returns None and the retry happens
    on the next pre_llm_call turn. Errors are logged, never raised.
    """
    if not session_id or not cwd:
        return False
    try:
        db_path = _hermes_home() / "state.db"
        if not db_path.exists():
            return False
        db = None
        try:
            db = _import_hermes_module("hermes_state", "SessionDB")(db_path)
            result = db.update_session_cwd(session_id, cwd)
            if result is None:
                logger.info(
                    "tg-projects: state.db session %s has no row yet; cwd persist deferred to next turn (%s)",
                    session_id, reason,
                )
                return False
            logger.info("tg-projects: persisted cwd %r to state.db session %s (%s)", cwd, session_id, reason)
            return True
        finally:
            # close() on a one-shot handle tears it down (drains token deltas,
            # passive checkpoint). Errors swallowed: persistence failure must
            # never break a turn.
            if db is not None:
                with _suppress(Exception):
                    db.close()
    except Exception:
        logger.warning("tg-projects: failed to persist cwd to state.db for session %s", session_id, exc_info=True)
        return False


def _apply_session_cwd(session_id: str, cwd: str, reason: str) -> None:
    """Register *cwd* for *session_id* (gateway task_id == session id) and
    persist it into state.db so non-Telegram surfaces see the session.

    Two independent stages, each with its own cache:

    * in-memory registration (task env overrides + session cwd record) is
      ONE-SHOT per session id — ``_CWD_REGISTERED`` — because re-registering
      per turn is wasted work and would repeat the same side effect on every
      pre_llm_call hook;
    * ``_CWD_APPLIED`` is stamped ONLY after the state.db write succeeds, so
      a transient DB failure (locked db, or the session row not created yet —
      ``on_session_start`` fires before the core inserts the row) is retried
      on the next hook call. On retry only the DB write is repeated; the
      registration is not.

    A different cwd for the same session re-registers and re-persists (a
    deliberate re-pin); an identical, fully-applied one is a no-op. Failures
    are swallowed: a cwd pin must never break a turn.
    """
    if not session_id or not cwd:
        return
    with _CWD_LOCK:
        if _CWD_APPLIED.get(session_id) == cwd:
            return
        if _CWD_REGISTERED.get(session_id) != cwd:
            try:
                terminal_tool = _import_hermes_module("tools.terminal_tool")
                terminal_tool.register_task_env_overrides(session_id, {"cwd": cwd})
                terminal_tool.record_session_cwd(session_id, cwd)
                _CWD_REGISTERED[session_id] = cwd
            except Exception:
                logger.warning("tg-projects: cwd registration for session %s failed", session_id, exc_info=True)
                return
        if _persist_cwd_to_state_db(session_id, cwd, reason):
            _CWD_APPLIED[session_id] = cwd
            logger.info("tg-projects: applied project cwd %r to session %s (%s)", cwd, session_id, reason)
        elif _CWD_REGISTERED.get(session_id) == cwd:
            logger.info("tg-projects: cwd registered for session %s (%s); state.db persist deferred",
                        session_id, reason)


# --------------------------------------------------------------------- state file
def _save_thread_project(thread_id: Any, project) -> None:
    """DEPRECATED: write into thread_to_project. Superseded by
    ``_set_topic_binding``; kept for the legacy tests."""
    """Record that *thread_id* belongs to *project* (state.json, not config.yaml)."""
    wanted = _norm_thread_id(thread_id)
    if wanted is None:
        return
    cwd = _project_cwd(project)
    state = _load_state()
    state.setdefault("thread_to_project", {})[str(wanted)] = {
        "project_id": getattr(project, "id", None),
        "name": getattr(project, "name", None),
        "slug": getattr(project, "slug", None),
        "cwd": cwd,
        "ts": int(time.time()),
    }
    _save_state(state)


def _load_state() -> dict:
    try:
        if _STATE_FILE.exists():
            data = json.loads(_STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        logger.warning("tg-projects: state.json unreadable, starting fresh", exc_info=True)
    return {"pending_cwd": {}, "thread_to_project": {}}


def _save_state(state: dict) -> None:
    _PLUG_DIR.mkdir(parents=True, exist_ok=True)
    tmp = _STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, _STATE_FILE)


# --------------------------------------------------------------------------- queries
def _project_cwd(project) -> str | None:
    """A project's working directory: primary_path, else the primary/first folder.

    NOTE: a project's primary_path may contain characters that look like a path
    typo (e.g. NeiroSlop ends with ')'): the path is authoritative as stored —
    never "fix" it.
    """
    cwd = getattr(project, "primary_path", None)
    if not cwd:
        folders = getattr(project, "folders", None)
        if folders:
            cwd = next((f.path for f in folders if f.is_primary), folders[0].path)
    return cwd


def _open_state_db():
    """A read-only connection to <profile>/state.db, or None when absent."""
    path = _hermes_home() / "state.db"
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _sessions_state_conn() -> sqlite3.Connection:
    conn = _open_state_db()
    if conn is None:
        conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _messages_order_col(state_conn) -> str:
    """``timestamp DESC`` when the messages table has a timestamp column, else
    ``rowid DESC`` — a defensive fallback for older state.db schemas."""
    cols = _columns_of(state_conn, "messages")
    return "timestamp DESC" if "timestamp" in cols else "rowid DESC"


def _columns_of(state_conn, table: str) -> set:
    try:
        return {str(r[1]) for r in state_conn.execute(f"PRAGMA table_info({table})")}
    except Exception:
        return set()


def _sessions_for_cwd(state_conn, cwd: str, limit: int = 2, source: Optional[str] = None) -> list:
    """Latest sessions whose cwd equals *cwd*, each with its last user message.

    Without *source* the query is cross-origin: desktop sessions created by the
    Hermes desktop app (source='desktop') appear next to Telegram ones — that is
    the "continue where I left off on the phone" use case. ``source`` is kept
    in each row so the caller can flag them (a desktop session driven from two
    places at once interleaves history from two processes).
    """
    rows = []
    try:
        sql = ("SELECT id, title, source, started_at, message_count FROM sessions "
               "WHERE cwd = ? AND message_count > 0")
        args: list = [cwd]
        if source:
            sql += " AND source = ?"
            args.append(source)
        sql += " ORDER BY started_at DESC LIMIT ?"
        args.append(limit)
        order = _messages_order_col(state_conn)
        for row in state_conn.execute(sql, args).fetchall():
            last = state_conn.execute(
                "SELECT content FROM messages WHERE session_id = ? AND role = 'user' "
                f"ORDER BY {order} LIMIT 1",
                (row["id"],),
            ).fetchone()
            active = _session_live_status(state_conn, row["id"])
            source_name = row["source"] or ""
            rows.append(
                {
                    "id": row["id"],
                    "title": row["title"] or "(без названия)",
                    "source": source_name,
                    "status": active.get("status", "idle"),
                    "active_device": source_name,
                    "started_at": row["started_at"],
                    "message_count": row["message_count"],
                    "last_message": ((last["content"] if last else None) or ""),
                }
            )
    except Exception:
        logger.warning("tg-projects: session lookup failed", exc_info=True)
    return rows


def _session_live_status(state_conn, session_id: str) -> Dict[str, Any]:
    """Best-effort live status for *session_id*.

    Hermes state.db records a turn lease for a conversation while a turn is
    actively running.  When the lease table is missing or expired, the session
    is shown as idle.  The source column remains the surface/device label;
    no cross-surface ownership protocol is inferred from the lease holder.
    """
    status: Dict[str, Any] = {"status": "idle", "active_device": ""}
    try:
        now = time.time()
        row = state_conn.execute(
            "SELECT conversation_id, holder, expires_at FROM session_turn_leases "
            "WHERE conversation_id = ?",
            (session_id,),
        ).fetchone()
        if row is not None and row["expires_at"] > now:
            status["status"] = "online"
    except Exception:
        # Older state.db schemas or read-only schema errors must not hide sessions.
        status["status"] = "idle"
    return status


def _fmt_ts(ts) -> str:
    try:
        return time.strftime("%d.%m.%Y %H:%M", time.localtime(float(ts)))
    except Exception:
        return str(ts or "?")


def _trim(text, limit: int = 110) -> str:
    text = " ".join(str(text or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _last_user_lines(state_conn, session_id: str, limit: int = 2) -> List[str]:
    """The last *limit* user messages of a session, newest first, trimmed."""
    try:
        rows = state_conn.execute(
            "SELECT content FROM messages WHERE session_id = ? AND role = 'user' "
            f"ORDER BY {_messages_order_col(state_conn)} LIMIT ?",
            (session_id, limit),
        ).fetchall()
        return [ _trim(r["content"], 90) for r in reversed(rows) if r["content"] ]
    except Exception:
        return []


def _list_projects() -> list:
    """All non-archived projects (project rows carry primary_path + folders)."""
    with _import_hermes_module("hermes_cli.projects_db").connect_closing() as conn:
        return _import_hermes_module("hermes_cli.projects_db").list_projects(conn)


def _project_at(projects: list, index_str: str) -> Optional[Any]:
    """The 1-based project at *index_str* (button callback ids), or None."""
    try:
        idx = int(index_str)
    except (TypeError, ValueError):
        return None
    if 1 <= idx <= len(projects):
        return projects[idx - 1]
    return None


def _resolve_project(projects: list, arg: str):
    """``(project, 1-based index)`` for an index string or a slug; ``(None, None)``."""
    arg = (arg or "").strip().lower()
    if not arg:
        return None, None
    if arg.isdigit():
        idx = int(arg)
        if 1 <= idx <= len(projects):
            return projects[idx - 1], idx
        return None, None
    for i, proj in enumerate(projects, 1):
        if getattr(proj, "slug", None) == arg:
            return proj, i
    return None, None


def _pin_project(session_key: str, project) -> Optional[str]:
    """Store a pending cwd pin for *session_key* (the /pnew semantics).

    Returns None on success, or an error text to show the user.
    """
    cwd = _project_cwd(project)
    if not cwd:
        return f"У проекта «{getattr(project, 'name', '?')}» не указан рабочий каталог (primary_path пуст)."
    if not os.path.isdir(cwd):
        return f"Каталог {cwd} не существует на этом хосте."
    if not session_key:
        return "Не удалось определить чат (session key пуст) — закрепление не применимо."
    state = _load_state()
    state.setdefault("pending_cwd", {})
    state["pending_cwd"][session_key] = {
        "project_id": getattr(project, "id", None),
        "name": getattr(project, "name", None),
        "slug": getattr(project, "slug", None),
        "cwd": cwd,
        "ts": int(time.time()),
    }
    _save_state(state)
    return None


# ------------------------------------------------------------- plain-text rendering
def _sessions_lines(state_conn, cwd: str, limit: int = 2, source: Optional[str] = None) -> list:
    sessions = _sessions_for_cwd(state_conn, cwd, limit=limit, source=source)
    if not sessions:
        return ["    • (нет сессий в этом каталоге)"]
    lines = []
    for s in sessions:
        flag = "🖥️ " if s["source"] == "desktop" else ""
        status = s.get("status") or "idle"
        device = s.get("active_device") or s.get("source") or "unknown"
        lines.append(
            f"    • {flag}{s['id']} ({_fmt_ts(s['started_at'])}) "
            f"{s['message_count']} msg — {status} / {device} — "
            f"{_trim(s['last_message'], 80) or '(пусто)'}"
        )
    return lines


def _plain_projects_text(projects: list, state_conn) -> str:
    lines = ["Проекты:"]
    for i, proj in enumerate(projects, 1):
        cwd = _project_cwd(proj)
        lines.append(f"{i}. {proj.name} [{proj.slug}] — {cwd or 'каталог не указан'}")
        if not cwd:
            continue
        lines.extend(_sessions_lines(state_conn, cwd, limit=2))
    lines.append("")
    lines.append("Новая сессия в проекте: /pnew <номер> → /new")
    lines.append("Новый проект: /pproject <название> <абсолютный путь> [new-folder]")
    return "\n".join(lines)


def _plain_menu_text(proj, index: int, cwd: str | None, state_conn) -> str:
    lines = [
        f"Проект #{index}: {proj.name} [{proj.slug}]",
        f"Каталог: {cwd or 'не указан'}",
        "",
    ]
    if cwd:
        lines.extend(_sessions_lines(state_conn, cwd, limit=2))
    lines.append("")
    lines.append("Кнопки недоступны сейчас, текстовые эквиваленты:")
    lines.append(f"  новая сессия: /pnew {index}, затем /new")
    if cwd:
        first = _sessions_for_cwd(state_conn, cwd, limit=1)
        if first:
            lines.append(f"  продолжить последнюю: /resume {first[0]['id']}")
    lines.append(f"  все сессии проекта: {CB_PREFIX}a:{index} (кнопка) или /resume <id>")
    lines.append(f"  модель: {CB_PREFIX}d:{index} (кнопка) или /model")
    lines.append("  назад к списку: /projects")
    return "\n".join(lines)


def _plain_sessions_text(proj, index: int, cwd: str | None, state_conn) -> str:
    lines = [f"Сессии проекта #{index}: {proj.name}"]
    if not cwd:
        lines.append("каталог не указан")
    else:
        lines.extend(_sessions_lines(state_conn, cwd, limit=5))
    lines.append("Назад к списку: /projects")
    return "\n".join(lines)


# ------------------------------------------------------------------------ /projects
async def _projects_handler(raw_args: str) -> str | None:
    if raw_args and raw_args.strip():
        return "Использование: /projects — без аргументов."

    try:
        projects = _list_projects()
    except Exception as exc:
        return f"Не удалось прочитать projects.db: {exc}"
    if not projects:
        return "Проектов пока нет."

    state_conn = _sessions_state_conn()
    try:
        # The command handler returns only a str. When the Telegram factory has
        # run, send the inline keyboard ourselves (chat id/thread from the
        # session env); otherwise (or without a chat id) return the text form.
        # NOTE: the gateway awaits coroutine handlers (run_inbound.py:1134), so
        # this handler is async and bot.send_message is properly awaited — a
        # plain call used to die with "coroutine was never awaited".
        bot = getattr(_NATIVE, "bot", None) if _NATIVE is not None else None
        if _NATIVE is not None and bot is not None:
            chat_id = _session_env("HERMES_SESSION_CHAT_ID", "")
            thread_id = _session_env("HERMES_SESSION_THREAD_ID", "")
            if chat_id:
                from telegram import InlineKeyboardButton, InlineKeyboardMarkup

                keyboard = [
                    [InlineKeyboardButton(f"📁 {p.name} [{p.slug}]",
                                          callback_data=f"{CB_PREFIX}p:{i}")]
                    for i, p in enumerate(projects, 1)
                ]
                keyboard.append([InlineKeyboardButton("🆕 Новый проект", callback_data="tgp:np")])
                kwargs = {"chat_id": chat_id, "text": "Проекты — выберите каталог:",
                          "reply_markup": InlineKeyboardMarkup(keyboard)}
                if thread_id:
                    kwargs["message_thread_id"] = int(thread_id) if thread_id.isdigit() else thread_id
                await bot.send_message(**kwargs)
                return None  # the keyboard is the visible answer
            return _plain_projects_text(projects, state_conn) + "\n" + _plain_projects_notice()
        return _plain_projects_text(projects, state_conn)
    finally:
        try:
            state_conn.close()
        except Exception:
            pass


# ------------------------------------------------------------------------ /pnew
def _pnew_handler(raw_args: str) -> str | None:
    arg = (raw_args or "").strip()
    if not arg:
        return "Использование: /pnew <номер|slug> — например /pnew 2"

    try:
        projects = _list_projects()
    except Exception as exc:
        return f"Не удалось прочитать projects.db: {exc}"

    target, index = _resolve_project(projects, arg)
    if target is None:
        return f"Проект «{arg}» не найден. Список: /projects"

    cwd = _project_cwd(target)
    if not cwd:
        return f"У проекта «{target.name}» не указан рабочий каталог (primary_path пуст)."
    if not os.path.isdir(cwd):
        return f"Каталог {cwd} не существует на этом хосте."

    sk = _current_session_key()
    if not sk:
        return (
            "Не удалось определить текущий чат (session key пуст) — "
            "закрепление не применимо. Отправьте /pnew в Telegram-чате."
        )

    state = _load_state()
    state.setdefault("pending_cwd", {})
    state["pending_cwd"][sk] = {
        "project_id": target.id,
        "name": target.name,
        "slug": target.slug,
        "cwd": cwd,
        "ts": int(time.time()),
    }
    _save_state(state)
    return (
        f"Проект «{target.name}» ({target.slug}) → {cwd}\n"
        "Каталог применится к НОВОЙ сессии: отправьте /new — первое сообщение "
        "в новой сессии начнётся в этом каталоге."
    )


# --------------------------------------------------------------------- new project
# /pproject <name> <absolute path> [new-folder]
#   * the text form is the only entry point that creates a project + topic from
#     a button: a callback_data value is capped at 64 bytes, so it cannot carry
#     a name + path; the button therefore runs the same /pproject flow (the
#     reply text is the step-by-step guide). No monkey patching: the project is
#     created via hermes_cli.projects_db.create_project, the topic via the
#     adapter's own DM-topic helpers, and the thread_id -> project mapping is
#     kept in state.json (never in config.yaml).
def _parse_pproject(raw_args: str) -> tuple:
    """``(name, path, create_folder)`` from the /pproject args, else None.

    The create-folder flag may lead or trail the rest:
    ``/pproject [new-folder] <name> <absolute path>`` — anything else is
    treated as a name/path pair, so ``[new-folder]`` is only honored in those
    two slots (a folder literally named ``new-folder`` after the path is the
    rarest case in practice). ``name`` is quoted when it contains spaces; the
    path is the rest of the line and must be absolute.
    """
    arg = (raw_args or "").strip()
    if not arg:
        return None
    _FLAGS = {"new-folder", "newfolder", "create-folder", "mkdir", "m"}
    tokens = arg.split()
    create = False
    if tokens and tokens[0].lstrip("/-").lower() in _FLAGS:
        create = True
        tokens = tokens[1:]
    if len(tokens) > 2 and tokens[-1].lstrip("/-").lower() in _FLAGS:
        create = True
        tokens = tokens[:-1]
    if len(tokens) < 2:
        return None
    text = " ".join(tokens).strip()
    if text.startswith('"'):
        end = text.find('"', 1)
        if end <= 1:
            return None
        name = text[1:end]
        path = text[end + 1:]
    else:
        name, _, path = text.partition(" ")
    name, path = name.strip().strip('"').strip(), path.strip().strip('"').strip()
    return (name, path, create) if name and path else None


def _pproject_handler(raw_args: str) -> str:
    """Create a project, its folder (opt-in) and its Telegram topic.

    Returns a str: command handlers cannot show a keyboard, and the flow needs
    a folder-existence confirmation, so the answer is a text guide. All the
    heavy lifting is in ``_create_project_and_topic``.
    """
    parsed = _parse_pproject(raw_args)
    if parsed is None:
        return (
            "Использование: /pproject <название> <абсолютный путь> [new-folder]\n"
            "  new-folder — создать каталог, если его нет (без флага недостающий\n"
            "  каталог — ошибка, ничего не создаётся молча).\n"
            "Пример: /pproject Орк /mnt/mydisk/orc new-folder"
        )
    name, path, create = parsed
    return _create_project_and_topic(name, path, create_folder=create)


def _create_project_and_topic(name: str, path: str, *, create_folder: bool,
                              chat_id: str = "", thread_name: Optional[str] = None) -> str:
    """Create a project (no topic); returns the confirmation/error text.

    ``chat_id`` is unused for topics (topics removed). Folder creation is
    explicit (``create_folder``) — a missing path is an error by default.
    Never writes config.yaml.
    """
    path = (path or "").strip()
    if not path.startswith("/"):
        return (f"❌ Путь «{path}» не абсолютный — нужен путь от корня, например /mnt/mydisk/проект.")
    if not (name or "").strip():
        return "❌ Укажите название проекта."

    exists = os.path.isdir(path)
    if exists:
        if create_folder:
            note = "(каталог уже существует, ничего не создано)"
        else:
            note = ""
    elif create_folder:
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as exc:
            return f"❌ Не удалось создать каталог {path}: {exc}"
        note = "каталог создан"
    else:
        return (
            f"❌ Каталог {path} не существует. Я не создаю каталоги молча —\n"
            f"добавьте флаг new-folder: /pproject {name} {path} new-folder"
        )

    projects_db = _import_hermes_module("hermes_cli.projects_db")
    try:
        with projects_db.connect_closing() as conn:
            pid = projects_db.create_project(conn, name=name, primary_path=path)
    except ValueError as exc:
        return f"❌ projects.db: {exc}"
    except Exception as exc:
        return f"❌ Не удалось создать проект: {exc}"

    adapter = _ADAPTER
    chat_id = str(chat_id or "").strip() or _session_env("HERMES_SESSION_CHAT_ID", "")
    topic_note = (
        "ℹ️ Топики не создаются — проект не привязан к Telegram-топику. "
        "Используйте /pnew <номер> + /new для старта сессии в этом каталоге."
    )

    return (
        f"✅ Проект «{name}» создан → {path}\n"
        f"   {note + ' • ' if note else ''}project id {pid}\n"
        f"{topic_note}"
    )


def _thread_id_for_topic(adapter: Any, chat_id: str, topic_name: str):
    """The thread_id of *topic_name* in *chat_id*: the adapter's cache first
    (topics the gateway already knows), then a live topic creation.

    ``ensure_dm_topic`` creates the topic when missing and persists the id; it
    returns None when the chat has Threaded Mode disabled. Nothing here writes
    config.yaml except via the adapter's own ``_persist_dm_topic_thread_id``
    (which records name/thread_id only — no project data, so the mapping stays
    in this plugin's state.json).
    """
    ensure = getattr(adapter, "ensure_dm_topic", None)
    if not callable(ensure):
        return None
    thread_id = ensure(chat_id, topic_name)
    if thread_id:
        return thread_id
    create = getattr(adapter, "_create_dm_topic", None)
    if not callable(create):
        return None
    return create(int(chat_id), name=topic_name)


def _make_light_project(name: str, cwd: str, pid=None) -> Any:
    """A minimal project-shaped object for the helpers that only need name/cwd/id."""
    ns = types.SimpleNamespace(name=name, slug="", primary_path=cwd, folders=[])
    if pid is not None:
        ns.id = pid
    return ns


# --------------------------------------------------------------- on_session_start
# Pre-LLM cwd binding runs in a worker thread; one process-wide lock keeps the
# _CWD_APPLIED dict and the per-session registration consistent across threads.
_CWD_LOCK = threading.RLock()


def _on_session_start(**kwargs) -> None:
    """Apply the current session's working directory to a freshly created session.

    The core fires this once per new session id (agent/conversation_loop.py,
    first-turn path, ``session_id=agent.session_id``). Source priority is
    STRICTLY:
      1) ``state.json["topic_bindings"]["<chat_id>:<thread_id>"]["cwd"]`` —
         the topic is a working place, the binding is the single source;
      2) ``pending_cwd[session_key]`` — legacy /pnew pin fallback;
      3) no source → NO cwd is applied (``sessions.cwd`` stays NULL, the
         terminal tool keeps its own default), and the user gets a
         "bind this topic" warning in the topic itself.

    Failures are swallowed: a cwd pin must never break session start.
    """
    try:
        session_id = str(kwargs.get("session_id") or "").strip()
        if not session_id:
            return

        cwd, reason = None, ""

        # 1) topic binding — the primary source
        binding = _get_topic_binding()
        if binding is not None:
            candidate = str(binding.get("cwd") or "").strip()
            if candidate and os.path.isdir(candidate):
                cwd, reason = candidate, "topic binding"
            elif candidate:
                logger.warning(
                    "tg-projects: bound cwd %r missing on host (session %s)",
                    candidate, session_id)

        # 2) legacy pending pin — fallback when the topic has no usable
        #    binding. Consumed EITHER WAY (like the pre-binding code): a
        #    stale pin must never fire later on an unrelated session.
        sk = _current_session_key()
        state = _load_state()
        pending = state.get("pending_cwd") or {}
        entry = pending.pop(sk, None) if sk else None
        if entry is not None:
            _save_state(state)  # consume the pin either way (no retry loop)
            if cwd is None:
                candidate = str(entry.get("cwd") or "").strip()
                if candidate and os.path.isdir(candidate):
                    cwd, reason = candidate, "pending pin"
                else:
                    logger.warning("tg-projects: dropping stale pin cwd=%r", candidate)

        # 3) thread_to_project — the pre-binding legacy map, read ONLY for
        #    topics that never got a topic_binding (config dm_topics is not
        #    consulted: that is the core adapter's layer).
        if cwd is None and binding is None:
            cwd, reason = _thread_to_project_cwd(), "thread_to_project legacy"

        if cwd:
            _apply_session_cwd(session_id, cwd, reason)
            if binding is not None:
                _update_binding_session(session_id)
        else:
            # 3) unbound topic: no silent default-cwd session. The core has
            #    already created the session row (this hook has no veto in
            #    the invoke_hook contract — results are ignored), so the
            #    achievable behavior is: cwd stays NULL + the user is told.
            _notify_unbound_topic(session_id)
    except Exception:
        logger.warning("tg-projects: on_session_start failed", exc_info=True)


def _on_pre_llm_call(**kwargs) -> None:
    """Fallback cwd binder for turns ``on_session_start`` cannot see.

    The core fires on_session_start ONLY on the first turn of a session whose
    durable row is NEW (agent/conversation_loop.py ~865). A topic's very first
    session (no /new pin) and a session reopened via /resume skip that path,
    so this per-turn hook re-applies the topic binding cwd for such sessions.
    Source priority matches :func:`_on_session_start`: topic binding first,
    legacy pending pin second; unbound topics get nothing applied.

    Idempotent: _apply_session_cwd records the last applied cwd per session id
    and no-ops on repeats, so steady-state turns cost one dict lookup.
    """
    try:
        session_id = str(kwargs.get("session_id") or kwargs.get("task_id") or "").strip()
        if not session_id:
            return
        cwd, reason = None, ""

        binding = _get_topic_binding()
        if binding is not None:
            candidate = str(binding.get("cwd") or "").strip()
            if candidate and os.path.isdir(candidate):
                cwd, reason = candidate, "topic binding"

        if cwd is None:
            sk = _current_session_key()
            if sk:
                pending = (_load_state().get("pending_cwd") or {}).get(sk)
                if pending:
                    candidate = str(pending.get("cwd") or "").strip()
                    if candidate and os.path.isdir(candidate):
                        cwd, reason = candidate, "pending pin"

        # legacy thread_to_project fallback for never-bound topics
        if cwd is None and binding is None:
            cwd, reason = _thread_to_project_cwd(), "thread_to_project legacy"
        if cwd:
            _apply_session_cwd(session_id, cwd, reason)
    except Exception:
        logger.warning("tg-projects: pre_llm_call failed", exc_info=True)


# ----------------------------------------------------------------- keyboard builders
def _project_list_keyboard(projects: list):
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    keyboard = [
        [InlineKeyboardButton(f"📁 {p.name} [{p.slug}]",
                              callback_data=f"{CB_PREFIX}p:{i}")]
        for i, p in enumerate(projects, 1)
    ]
    keyboard.append([InlineKeyboardButton("🆕 Новый проект", callback_data="tgp:np")])
    return InlineKeyboardMarkup(keyboard)


def _project_menu_keyboard(index: int, has_sessions: bool, has_cwd: bool):
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    row1 = [
        InlineKeyboardButton("🆕 Новая сессия", callback_data=f"{CB_PREFIX}n:{index}"),
        InlineKeyboardButton("▶️ Продолжить", callback_data=f"{CB_PREFIX}r:{index}"),
    ]
    row2 = [
        InlineKeyboardButton("📋 Сессии", callback_data=f"{CB_PREFIX}l:{index}"),
        InlineKeyboardButton("🖥️ Все сессии", callback_data=f"{CB_PREFIX}a:{index}"),
    ]
    row3 = [
        InlineKeyboardButton("⚙️ Модель", callback_data=f"{CB_PREFIX}d:{index}"),
        InlineKeyboardButton("⬅️ Назад", callback_data=_BACK_CB),
    ]
    del has_sessions, has_cwd  # the menu is the same regardless
    return InlineKeyboardMarkup([row1, row2, row3])


def _sessions_keyboard(index: int):
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ К списку проектов", callback_data=_BACK_CB),
         InlineKeyboardButton("📋 Ещё раз", callback_data=f"{CB_PREFIX}l:{index}")],
    ])


# ---------------------------------------------------- synthetic gateway message injection
def _source_from_query(adapter: Any, query: Any, thread_id: Optional[Any] = None):
    """SessionSource of a button tap: prefer the adapter's auth source builder.

    *thread_id* (a Telegram topic id) overrides the tapped message's thread when
    given — the "new project" flow answers in the topic that owns the project,
    not the one where the button was pressed.
    """
    try:
        source = adapter._source_from_message_for_auth(query.message)
        if source is not None:
            if thread_id is not None and str(thread_id or "") not in {"", None}:
                import dataclasses
                source = dataclasses.replace(source, thread_id=str(thread_id))
            return source
    except Exception:
        logger.debug("tg-projects: source from message failed", exc_info=True)

    msg = getattr(query, "message", None)
    chat = getattr(msg, "chat", None) if msg is not None else None
    user = getattr(msg, "from_user", None) if msg is not None else None
    chat_id = getattr(chat, "id", None)
    chat_id = str(chat_id) if chat_id is not None else ""
    if hasattr(adapter, "_normalize_chat_type"):
        chat_type = adapter._normalize_chat_type(
            getattr(chat, "type", "dm"), is_forum=(getattr(chat, "is_forum", False) is True)
        )
    else:
        chat_type = "dm"
    if thread_id is not None and str(thread_id or "") not in {"", None}:
        thread_id = str(thread_id)
    else:
        thread_id = getattr(msg, "message_thread_id", None) if msg is not None else None
        if thread_id is not None:
            thread_id = str(thread_id)
    user_id = str(getattr(user, "id", "") or "").strip() or None
    user_name = (str(getattr(user, "username", "") or getattr(user, "full_name", ""))
                 or getattr(chat, "title", "")) or None
    return adapter.build_source(
        chat_id=chat_id, chat_type=chat_type, user_id=user_id, user_name=user_name,
        thread_id=thread_id, chat_name=getattr(chat, "title", None),
        is_bot=bool(getattr(user, "is_bot", False)) if user is not None else False,
    )


def _callers_admin(source) -> bool:
    """Whether *source* is an explicitly configured slash-admin (cross-origin
    /resume --all / /sessions all).

    Reads the gateway's own policy (gateway.slash_access) — no monkey patch:
    the scope's ``allow_admin_from`` decides membership, and a scope with no
    admin list at all has gating disabled, which the core treats as
    ``is_admin -> True`` for every caller. So ``--all`` is legitimately
    cross-origin for the user on an ungated platform, and only an explicitly
    listed id gets it on a gated one. Without a live adapter, or when the
    caller cannot be resolved (no SessionSource and no adapter to fall back
    to), this returns False and the caller degrades to the session-scoped
    /resume, which still works.
    """
    runner = getattr(_ADAPTER, "gateway_runner", None)
    if runner is None:
        return False
    if source is None:
        # No SessionSource for this call: fall back to the chat's current
        # caller, which is what a button tap carries. When that is not
        # resolvable either the caller is unknown and not admin.
        source = _adapter_source_for(None)
    if source is None:
        return False
    try:
        from gateway.slash_access import policy_for_runner_source
        policy = policy_for_runner_source(runner, source)
        uid = getattr(source, "user_id", None)
        return bool(policy.is_admin(uid))
    except Exception:
        return False


def _thread_id_for_chat_from_sessions(state_conn, chat_id: str, thread_hint: str = "") -> str:
    """The thread_id this chat's /model override must land on: the tapped message's
    topic when it is in a topic, otherwise the chat's most-recent session row with
    the same chat_id (what the next turn will bind the session key from).

    /model pins its override under ``_normalize_source_for_session_key(source)``; a
    lobby/empty-chat session whose thread_id gets *recovered* to the chat's active
    topic must pin on that topic, or the pin lands on the key the next message
    turn will not read (#30479)."""
    thread = str(thread_hint or "").strip()
    if thread:
        return thread
    try:
        row = state_conn.execute(
            "SELECT thread_id FROM sessions WHERE chat_id = ? AND thread_id IS NOT NULL "
            "AND thread_id != '' ORDER BY started_at DESC LIMIT 1",
            (chat_id,),
        ).fetchone()
        if row is not None:
            return str(row["thread_id"])
    except Exception:
        logger.debug("tg-projects: thread lookup for model picker failed", exc_info=True)
    return ""


def _synthetic_event(adapter: Any, query: Any, text: str, thread_id: Optional[Any] = None):
    """A real-chat MessageEvent carrying *text* as if the user had typed it.

    *thread_id* retargets the event's topic (the "new project" flow answers in
    the project's own topic, not the one where the button was tapped).
    """
    from gateway.platforms.event import MessageEvent, MessageType

    source = _source_from_query(adapter, query, thread_id=thread_id)
    msg = getattr(query, "message", None)
    return MessageEvent(
        text=text,
        message_type=MessageType.TEXT,
        source=source,
        raw_message=msg,
        message_id=str(getattr(msg, "message_id", None) or ""),
        reply_expected=True,
        allow_gateway_control=True,
        internal=False,
    )


# -------------------------------------------------------------------- callback handler
async def _tg_on_button(update: Any, context: Any) -> None:
    """CallbackQueryHandler(tgp:*) — inline keyboard taps."""
    query = update.callback_query
    data = str(getattr(query, "data", "") or "")
    if not data:
        return

    # Auth: the same gate the core pickers use; a stranger in a shared group
    # must not drive this chat's project flow. No plugin state is touched on
    # refusal — the answer() text is the whole reply.
    cb = _ADAPTER._callback_ctx(query)
    if not await _ADAPTER._callback_authorized(query, cb, "Нет доступа к кнопкам проектов"):
        return

    try:
        _ADAPTER._accept_update()
    except Exception:
        pass

    # Handoff Yes/No buttons (tgp:ho:<session_id>:y|n) — consumed before the
    # project-menu dispatch: they answer a prompt, not open a menu.
    if data.startswith("tgp:ho:"):
        consumed = await _handle_handoff_callback(query, data)
        if consumed:
            with _suppress(Exception):
                await query.answer()
            return

    m = _CB_SESSION_RE.match(data)
    if m is not None:  # tgp:s:<id> — continue a listed session
        await _do_resume_by_id(query, m.group(1))
        return

    if data == "tgp:np":  # new project: step-by-step text guide for /pproject
        await _edit_new_project_guide(query)
        return

    m = _CB_RE.match(data)
    if m is None:
        await query.answer()
        return

    action, index_str = m.group(1), m.group(2)
    try:
        projects = _list_projects()
    except Exception as exc:
        logger.warning("tg-projects: projects.db read failed: %s", exc)
        await query.answer()
        with _suppress(Exception):
            await query.edit_message_text(
                f"Не удалось прочитать проекты: {exc}",
                reply_markup=_back_keyboard(),
            )
        return

    project = _project_at(projects, index_str)
    state_conn = _sessions_state_conn()
    try:
        if action in ("p", "m"):  # project button → menu
            if project is None:
                raise _ProjectMoved(f"Проект #{index_str} больше не в списке — закройте и выберите заново.")
            await _edit_menu(query, projects, project, index_str, state_conn)
        elif action == "n":  # new session in the project
            if project is None:
                raise _ProjectMoved(f"Проект #{index_str} больше не в списке — закройте и выберите заново.")
            await _do_new_session(query, project, index_str, state_conn)
        elif action == "r":  # continue the last session
            if project is None:
                raise _ProjectMoved(f"Проект #{index_str} больше не в списке — закройте и выберите заново.")
            await _do_resume(query, project, index_str, state_conn)
        elif action == "a":  # ALL sessions of the project (desktop included)
            if project is None:
                raise _ProjectMoved(f"Проект #{index_str} больше не в списке — закройте и выберите заново.")
            await _edit_all_sessions(query, projects, project, index_str, state_conn)
        elif action == "d":  # model picker for the project
            if project is None:
                raise _ProjectMoved(f"Проект #{index_str} больше не в списке — закройте и выберите заново.")
            await _do_model_picker(query, project, index_str)
        elif action == "l":  # session list
            if project is None:
                raise _ProjectMoved(f"Проект #{index_str} больше не в списке — закройте и выберите заново.")
            await _edit_sessions(query, projects, project, index_str, state_conn)
        await query.answer()
    except _ProjectMoved as exc:
        with _suppress(Exception):
            await query.answer(str(exc))
    except Exception as exc:
        logger.warning("tg-projects: button %r failed: %s", data, exc, exc_info=True)
        with _suppress(Exception):
            await query.answer("Ошибка — см. лог hermes")
    finally:
        with _suppress(Exception):
            state_conn.close()


# ------------------------------------------------------------------- button flows
def _desktop_note(source: str) -> str:
    """The two-process warning for desktop sessions, or '' for non-desktop."""
    if source != "desktop":
        return ""
    return ("\n\n⚠️ Это сессия из десктопного приложения. Если вы продолжите её с телефона, "
            "а она открыта и на десктопе — история будет писаться из двух процессов, и "
            "могут теряться/перемешиваться ответы. Закройте сессию на одной стороне и "
            "работайте с одной за раз.")


async def _do_resume_by_id(query, session_id: str) -> None:
    """Continue a session the user picked from the 'all sessions' list.

    The id is validated against state.db (a tap only ever carries an id this
    plugin just listed; anything else is rejected). The session's recorded
    project cwd is re-registered for the resumed session id, then a synthetic
    /resume <id> is fed into the gateway and the last 1-2 user messages are
    shown ("на чём остановились").
    """
    state_conn = _sessions_state_conn()
    try:
        row = state_conn.execute(
            "SELECT id, title, source, cwd, started_at, message_count FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
        sessions_tail = _last_user_lines(state_conn, session_id, limit=2)
        if row is None:
            with _suppress(Exception):
                await query.edit_message_text(
                    f"❌ Сессия {session_id} не найдена (возможно, она была удалена). Откройте «Все сессии проекта» заново.",
                    reply_markup=_back_keyboard())
            return

        cwd = row["cwd"]
        if cwd:
            _apply_session_cwd(session_id, cwd, "resume-by-id")

        source = _adapter_source_for(query)
        # NOTE: the cross-origin check must run BEFORE the connection is
        # closed — calling it after `finally: state_conn.close()` raised
        # inside _session_is_same_source, whose fail-open returned True and
        # silently downgraded "/resume --all" to a plain "/resume" that the
        # gateway's IDOR guard then refused.
        cross_origin = not _session_is_same_source(state_conn, session_id, row["source"], source)
        admin = _callers_admin(source)
    finally:
        with _suppress(Exception):
            state_conn.close()
    if cross_origin and admin:
        cmd = f"/resume --all {session_id}"
        suffix = " (кросс-оригин: admin --all)"
    else:
        cmd = f"/resume {session_id}"
        suffix = " (если сессия из другого источника — см. примечание)"

    lines = [
        f"▶️ Продолжаю сессию {session_id} ({_fmt_ts(row['started_at'])}) — {row['message_count']} msg",
        f"Каталог: {cwd or 'не указан'}",
    ]
    if _desktop_note(row["source"]):
        lines.append(_desktop_note(row["source"]))
    if cross_origin and not admin:
        lines.append(_ADMIN_NOTE)
    if sessions_tail:
        lines.append("\nНа чём остановились:")
        lines.extend(f"  • {t}" for t in sessions_tail)
    lines.append(f"\nОтправляю {cmd} — ответ придёт как отдельное сообщение.")
    with _suppress(Exception):
        await query.edit_message_text("\n".join(lines), reply_markup=_back_keyboard())

    await _send_gateway_command(query, cmd)


def _resume_command_for(session_id: str, state_conn, session_source: str = "", query=None) -> str:
    """``/resume <id>`` — or ``/resume --all <id>`` when the session was created
    by another source (e.g. the desktop app), which is a cross-origin resume.

    The gateway's IDOR guard refuses a non-owner /resume; ``--all`` is the
    documented cross-origin switch, honored only for a ``_resume_caller_is_admin``
    caller (an explicitly configured admin, or any caller when the platform has
    no ``allow_admin_from`` list — disabled gating makes everyone pass the admin
    check). We only ADD the flag when the core's own policy says the caller is
    admin — nothing is bypassed; when the caller is not admin we still send
    plain /resume (the core will refuse it) and append a note telling the user
    what to enable.
    """
    source = _adapter_source_for(query)
    if not _session_is_same_source(state_conn, session_id, session_source, source):
        return f"/resume --all {session_id}" if _callers_admin(source) \
            else f"/resume {session_id}\n" + _ADMIN_NOTE
    return f"/resume {session_id}"


_ADMIN_NOTE = (
    "\nℹ️ Сессия создана из другого источника (desktop/telegram) — без прав "
    "администратора /resume не переключит сессию между источниками. Включите "
    "platforms.telegram.extra.allow_admin_from со своим Telegram user id, чтобы "
    "/resume --all прошёл. Без этого продолжайте сессию с той стороны, где она "
    "создана (десктоп → десктоп, топик → топик)."
)


def _adapter_source_for(query):
    """The calling SessionSource (None when query/_ADAPTER is unavailable).

    Accepts either a real Telegram CallbackQuery (has ``.message``) or a
    bare SessionSource object (used by tests that pass a caller directly).
    A CallbackQuery is unwrapped via ``_source_from_query``; a raw source is
    returned as-is.
    """
    if query is None or _ADAPTER is None:
        return None
    if not hasattr(query, "message"):
        # A raw SessionSource (not a CallbackQuery) — return it directly.
        return query
    try:
        return _source_from_query(_ADAPTER, query)
    except Exception:
        return None


def _session_is_same_source(state_conn, session_id: str, session_source: str = "",
                            source=None) -> bool:
    """Whether *session_id* was created by the same source as the caller.

    The session row stores its origin source ('desktop', 'telegram', ...) in the
    ``source`` column. Unresolvable rows/sources fail open (True) — a failed
    check must keep the plain /resume, never silently add --all.
    """
    try:
        row = state_conn.execute(
            "SELECT source FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
    except Exception:
        return True
    if row is None:
        return True
    row_source = str(row["source"] or "").strip().lower()
    if not row_source:
        return True
    caller = _caller_source_value(session_source, source)
    return not caller or caller == row_source


def _caller_source_value(session_source: str = "", source=None) -> str:
    """The caller's session-source platform value ('desktop', 'telegram', ...)."""
    explicit = str(session_source or "").strip().lower()
    if explicit:
        return explicit
    if source is not None:
        platform = getattr(source, "platform", None)
        value = getattr(platform, "value", None)
        if value:
            return str(value).lower()
    return "telegram" if _ADAPTER is not None else ""


async def _edit_all_sessions(query, projects: list, proj, index_str: str, state_conn) -> None:
    """«Все сессии проекта»: up to 8 sessions of the project cwd (desktop included),
    each with a continue button; a desktop session carries a two-process warning."""
    cwd = _project_cwd(proj)
    sessions = _sessions_for_cwd(state_conn, cwd, limit=8) if cwd else []
    lines = [f"📋 Все сессии проекта #{index_str}: {proj.name}", ""]
    if not cwd:
        lines.append("каталог не указан")
    elif not sessions:
        lines.append("(нет сессий в этом каталоге)")
    else:
        has_desktop = any(s["source"] == "desktop" for s in sessions)
        for s in sessions:
            flag = "🖥️ " if s["source"] == "desktop" else ""
            lines.append(f"{flag}{s['id']} ({_fmt_ts(s['started_at'])}) {s['message_count']} msg — {_trim(s['last_message'], 40) or '(пусто)'}")
        if has_desktop:
            lines.append("")
            lines.append("⚠️ 🖥️ — сессия создана в десктопе. Если она открыта и там, и на телефоне "
                         "одновременно, история будет писаться из двух процессов — "
                         "работайте с одной стороны за раз.")
    kb = _all_sessions_keyboard(index_str, sessions)
    with _suppress(Exception):
        await query.edit_message_text("\n".join(lines), reply_markup=kb)


async def _edit_new_project_guide(query) -> None:
    """The «Новый проект» button: step-by-step text for /pproject (callback_data
    is 64 bytes, so name + path cannot ride in the button itself)."""
    lines = [
        "🆕 Новый проект (пошагово):",
        "",
        "1. Отправьте в этом чате: /pproject <название> <абсолютный путь>",
        "   Пример: /pproject Орк /mnt/mydisk/orc",
        "2. Если каталог ещё не существует — добавьте флаг: /pproject Орк /mnt/mydisk/orc new-folder",
        "   (без флага недостающий каталог — ошибка, каталоги не создаются молча)",
        "3. Ответ: проект создаётся в projects.db, создаётся новый топик в этом чате,",
        "   каталог закрепляется, и первая сессия в топике стартует в нём.",
    ]
    with _suppress(Exception):
        await query.edit_message_text("\n".join(lines), reply_markup=_back_keyboard())
    with _suppress(Exception):
        await query.answer()


async def _do_model_picker(query, project, index_str: str) -> None:
    """«Модель» button: feed a synthetic /model into the gateway, which renders the
    inline provider→model drill-down (send_model_picker) into this chat/topic. The
    picked model is pinned to the current session by the gateway; it is NOT bound
    to the project (the project menu just opens the picker where the user is)."""
    cwd = _project_cwd(project)
    with _suppress(Exception):
        await query.edit_message_text(
            f"⚙️ Модель для проекта {index_str}: {project.name}\n"
            f"Каталог: {cwd or 'не указан'}\n"
            "Отправляю /model — ниже появятся кнопки выбора провайдера и модели.",
            reply_markup=None,
        )
    await _send_gateway_command(query, "/model")


def _back_keyboard():
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ К списку проектов", callback_data=_BACK_CB)]])


def _all_sessions_keyboard(index: int, sessions: list):
    """Continue buttons for the 'all sessions' view: one tgp:s:<id> per session,
    plus back/re-list. Session ids ride in the callback data (each <= 64 bytes;
    a timestamp-based id is ~21 chars, so tgp:s:<id> stays under the cap)."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    rows = [
        [InlineKeyboardButton(f"▶️ Продолжить {s['id'][:12]}…", callback_data=f"{CB_PREFIX}s:{s['id']}")]
        for s in sessions
    ]
    rows.append([
        InlineKeyboardButton("⬅️ К списку проектов", callback_data=_BACK_CB),
        InlineKeyboardButton("📋 Ещё раз", callback_data=f"{CB_PREFIX}a:{index}"),
    ])
    return InlineKeyboardMarkup(rows)


class _ProjectMoved(Exception):
    """Raised inside the button handler to surface a stale index to the user."""


async def _edit_menu(query, projects: list, proj, index_str: str, state_conn) -> None:
    cwd = _project_cwd(proj)
    sessions = _sessions_for_cwd(state_conn, cwd, limit=1) if cwd else []
    lines = [
        f"📁 Проект #{index_str}: {proj.name} [{proj.slug}]",
        f"Каталог: {cwd or 'не указан'}",
    ]
    if sessions:
        s = sessions[0]
        lines.append(f"Последняя сессия: {s['id']} ({_fmt_ts(s['started_at'])})")
    else:
        lines.append("Сессий в этом каталоге ещё нет.")
    text = "\n".join(lines) + "\n\n" + _menu_action_note(proj, cwd, sessions)
    kb = _project_menu_keyboard(int(index_str), bool(sessions), bool(cwd))
    await query.edit_message_text(text, reply_markup=kb)


def _menu_action_note(proj, cwd: str | None, sessions: list) -> str:
    notes = []
    notes.append("🆕 Новая сессия — старт в этом каталоге (закрепляю /new)")
    if sessions:
        notes.append(f"▶️ Продолжить — /resume {sessions[0]['id']} + каталог проекта")
    else:
        notes.append("▶️ Продолжить — у проекта пока нет сессий, ничего не будет сделано")
    notes.append("📋 Сессии — последние 5 сессий этого каталога (только сессии с сообщением)")
    notes.append("🖥️ Все сессии — до 8 сессий этого каталога, включая созданные в десктопе")
    notes.append("⚙️ Модель — пикер моделей /model для текущей сессии")
    return " | ".join(notes)


async def _do_new_session(query, project, index_str: str, state_conn) -> None:
    """Pin the project like /pnew, then feed a synthetic /new into the gateway.

    The pin is keyed by this chat's session key; on_session_start applies it to
    the freshly created session id. If /new is rejected (e.g. no session yet)
    the pin stays and the reply tells the user exactly what to do.
    """
    sk = _current_session_key() or _session_key_from_query(query)
    err = _pin_project(sk, project)
    if err:
        await query.edit_message_text(f"🆕 Новая сессия: {err}", reply_markup=_back_keyboard())
        return
    cwd = _project_cwd(project)

    # Tell the user what is happening BEFORE the injection (the reply from the
    # gateway may be replaced or delayed).
    try:
        await query.edit_message_text(
            f"🆕 Проект «{project.name}» → {cwd}\n"
            "Отправляю /new — каталог применится к новой сессии (на первом сообщении).",
            reply_markup=None,
        )
    except Exception:
        pass

    await _send_gateway_command(query, "/new")


async def _do_resume(query, project, index_str: str, state_conn) -> None:
    """Apply the project cwd to the last session of the project, then /resume it."""
    cwd = _project_cwd(project)
    if not cwd:
        await query.edit_message_text(
            "▶️ Продолжить: у проекта не указан рабочий каталог — /resume без каталога невозможен.",
            reply_markup=_back_keyboard(),
        )
        return
    sessions = _sessions_for_cwd(state_conn, cwd, limit=1)
    if not sessions:
        await query.edit_message_text(
            "▶️ Продолжить: в этом каталоге нет сессий.\n"
            f"Альтернатива: «🆕 Новая сессия» в этом меню (/pnew {index_str} + /new).",
            reply_markup=_project_menu_keyboard(int(index_str), False, True),
        )
        return
    target = sessions[0]

    # Register the project cwd for the resumed session id RIGHT AWAY: the
    # gateway's turn task_id equals the session id, so the override lands on
    # the /resume'd session without waiting for a new session. _apply_session_cwd
    # also persists sessions.cwd into state.db (deferred + retried if the row
    # is not there yet), instead of stamping _CWD_APPLIED by hand.
    _apply_session_cwd(target["id"], cwd, "resume")

    lines = [
        f"▶️ Продолжаю сессию {target['id']} ({_fmt_ts(target['started_at'])})",
        f"Каталог: {cwd}",
    ]
    if _desktop_note(target.get("source", "")):
        lines.append(_desktop_note(target.get("source", "")))
    tail = _last_user_lines(state_conn, target["id"], limit=2)
    if tail:
        lines.append("\nНа чём остановились:")
        lines.extend(f"  • {t}" for t in tail)
    lines.append(f"\nОтправляю /resume {target['id']} — ответ придёт как отдельное сообщение.")
    try:
        await query.edit_message_text("\n".join(lines), reply_markup=_back_keyboard())
    except Exception:
        pass

    await _send_gateway_command(query, f"/resume {target['id']}")


async def _edit_sessions(query, projects: list, proj, index_str: str, state_conn) -> None:
    cwd = _project_cwd(proj)
    sessions = _sessions_for_cwd(state_conn, cwd, limit=5) if cwd else []
    lines = [f"📋 Сессии проекта #{index_str}: {proj.name}", ""]
    if not cwd:
        lines.append("каталог не указан")
    elif not sessions:
        lines.append("(нет сессий в этом каталоге)")
    else:
        has_desktop = False
        for s in sessions:
            if s["source"] == "desktop":
                has_desktop = True
            flag = "🖥️ " if s["source"] == "desktop" else ""
            lines.append(f"{flag}• {s['id']} ({_fmt_ts(s['started_at'])}) {s['message_count']} msg — {_trim(s['last_message'], 60) or '(пусто)'}")
        if has_desktop:
            lines.append("")
            lines.append("⚠️ 🖥️ — сессия из десктопа; одновременная работа с двух сторон пишет историю из двух процессов.")
    lines.append("")
    lines.append(f"Продолжить последнюю: /resume {sessions[0]['id']}" if sessions else "Продолжить: сессий нет")
    lines.append(f"Больше (все десктопные, до 8): {CB_PREFIX}a:{index_str}")
    await query.edit_message_text("\n".join(lines), reply_markup=_sessions_keyboard(int(index_str)))


def _session_key_from_query(query) -> str:
    """The tapping chat's session key, derived from the callback message.

    Mirrors gateway.session.build_session_key for the Telegram source; used as
    a fallback when the HERMES_SESSION_KEY contextvar is not bound in the
    callback task (plugin callbacks don't get a session env scope — only the
    dispatch layer binds one).
    """
    adapter = _ADAPTER
    try:
        source = _source_from_query(adapter, query)
    except Exception:
        return ""
    try:
        from gateway.session import build_session_key
        config = getattr(getattr(adapter, "gateway_runner", None), "config", None)
        return build_session_key(
            source,
            group_sessions_per_user=getattr(config, "group_sessions_per_user", True) if config is not None else True,
            thread_sessions_per_user=getattr(config, "thread_sessions_per_user", False) if config is not None else False,
        )
    except Exception:
        return ""


# ------------------------------------------------------- gateway /new, /resume injection
async def _send_gateway_command(query, text: str) -> None:
    """Feed *text* (``/new`` / ``/resume <id>``) into the gateway as this chat.

    Both are bypass commands: when a turn is running, handle_message dispatches
    them inline (``/new`` cancels the active turn first); when idle they go
    through the normal command dispatch. The event is built from the real
    callback message, so source / auth / session routing match what the user
    typed. Replies are sent by the gateway to this chat/thread — the button
    message is only the progress indicator.
    """
    adapter = _ADAPTER
    event = _synthetic_event(adapter, query, text)
    await adapter.handle_message(event)


# --------------------------------------------------------------------- telegram wiring
def _telegram_wire(native: Any, adapter: Any) -> None:
    """register_platform_handler factory: scope the plugin's callback handler
    (pattern ``^tgp:``) onto the Telegram application without touching the
    core button flows. Also captures the running event loop so sync hooks
    (on_session_start runs in a worker thread) can schedule async sends."""
    global _NATIVE, _ADAPTER, _WIRE_LOOP
    _NATIVE = native
    _ADAPTER = adapter
    try:
        _WIRE_LOOP = asyncio.get_running_loop()
    except RuntimeError:
        _WIRE_LOOP = None  # factory ran off-loop; sync sends degrade to log
    try:
        from telegram.ext import CallbackQueryHandler
        native.add_handler(CallbackQueryHandler(_tg_on_button, pattern=r"^tgp:"))
        logger.info("tg-projects: telegram callback handler wired (pattern ^tgp:)")
    except Exception:
        logger.warning("tg-projects: telegram wiring failed", exc_info=True)
        _NATIVE = None
        _ADAPTER = None


# --------------------------------------------------------------- session handoff
# Cross-device continuity (Telegram ↔ desktop) lives in handoff.py: the
# pre_gateway_dispatch hook parks an inbound message while the session runs
# on the desktop and asks Yes/No; the tgp:ho:* callbacks act on the answer.
def _import_handoff():
    pkg = __package__ or ""
    if pkg:
        try:
            return __import__(f"{pkg}.handoff", fromlist=["*"])
        except ImportError:
            pass
    import importlib.util
    spec = importlib.util.spec_from_file_location("tg_projects_handoff", str(_PLUG_DIR / "handoff.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("tg_projects_handoff", module)
    spec.loader.exec_module(module)
    return module


async def _handle_handoff_callback(query, data: str) -> bool:
    """Route ``tgp:ho:<token>:(y|n)`` taps. True when consumed.

    The callback_data carries an opaque 8-hex token (not the session_id —
    that would blow the 64-byte Telegram cap for real session ids). The
    real ``session_id`` / ``session_key`` are looked up from
    ``handoff_tokens`` in state.json via ``handoff.consume_token``, which
    also deletes the row so a second tap lands on the "expired" reply.
    """
    import re as _re
    m = _re.match(r"^tgp:ho:([a-f0-9]{8}):(y|n)$", str(data or ""))
    if m is None:
        return False
    token, answer = m.group(1), m.group(2)
    handoff = _import_handoff()
    resolved = handoff.consume_token(token)
    if resolved is None:
        with _suppress(Exception):
            await query.edit_message_text(
                "⚠️ Запрос устарел (истёк или уже использован). "
                "Отправьте сообщение заново.")
        return True
    session_id, sk = resolved
    if not sk:
        sk = _current_session_key() or _session_key_from_query(query)
    if answer == "n":
        handoff.drop_pending(sk)
        with _suppress(Exception):
            await query.edit_message_text(
                "Оставил как есть — сессия продолжает работать на другом устройстве. "
                "Сообщение не отправлено.")
        return True

    pending = await handoff.perform_handoff(sk, session_id)
    if pending is None:
        with _suppress(Exception):
            await query.edit_message_text(
                "⚠️ Запрос устарел (истёк или сессия сменилась). "
                "Отправьте сообщение заново.")
        return True

    with _suppress(Exception):
        await query.edit_message_text(
            f"✅ Сессия {session_id} переключена сюда. Десктоп получит уведомление; "
            "отправляю ваше сообщение…")

    # Re-dispatch the parked message through the gateway as a real turn.
    text = str(pending.get("text") or "")
    adapter = _ADAPTER
    if text and adapter is not None:
        from gateway.platforms.event import MessageEvent, MessageType
        src = pending.get("source") or {}
        source = _ADAPTER.build_source(
            chat_id=str(src.get("chat_id") or ""),
            chat_type="dm",
            user_id=str(src.get("user_id") or "") or None,
            user_name=src.get("user_name") or None,
            thread_id=str(src.get("thread_id") or "") or None,
        )
        event = MessageEvent(
            text=text, message_type=MessageType.TEXT, source=source,
            message_id=str(src.get("message_id") or ""), reply_expected=True,
            allow_gateway_control=True, internal=False,
        )
        await adapter.handle_message(event)
    return True


# ------------------------------------------------------------------------ register
def register(ctx) -> None:
    ctx.register_command(
        "projects",
        handler=_projects_handler,
        description="Проекты и последние сессии каждого",
        args_hint="",
    )
    ctx.register_command(
        "pnew",
        handler=_pnew_handler,
        description="Проект для следующей сессии (после /new)",
        args_hint="<номер|slug>",
    )
    ctx.register_command(
        "pproject",
        handler=_pproject_handler,
        description="Создать новый проект + топик (имя, путь, new-folder — создать каталог)",
        args_hint="<название> <абсолютный путь> [new-folder]",
    )
    ctx.register_hook("on_session_start", _on_session_start)
    # Fallback for sessions on_session_start never sees (a topic's first
    # session without a pin, /resume'd sessions): applies the topic->project
    # cwd on the first turn instead. Idempotent via _CWD_APPLIED.
    ctx.register_hook("pre_llm_call", _on_pre_llm_call)
    # Cross-device continuity: park a message that arrives while the session
    # is busy on the desktop and ask Yes/No (handoff.py). Fail-open.
    try:
        _handoff = _import_handoff()
        ctx.register_hook("pre_gateway_dispatch", _handoff.on_pre_gateway_dispatch)
        logger.info("tg-projects: pre_gateway_dispatch handoff hook registered")
    except Exception:
        logger.warning("tg-projects: handoff hook registration failed; "
                       "cross-device prompts disabled", exc_info=True)
    try:
        ctx.register_platform_handler("telegram", _telegram_wire)
    except Exception:
        logger.warning("tg-projects: platform handler registration unavailable; "
                       "text commands still work", exc_info=True)
