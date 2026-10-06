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


# --------------------------------------------------------------- seamless sync
# One forum topic = one project + one active session. Free text from a bound
# topic must land in the SAME sessions.id the desktop works on (state.db is
# the shared truth; the desktop drives its own hermes serve process).
# The core already heals ITS telegram_dm_topic_bindings row
# (gateway/run_turn.py _hmwa_heal_telegram_topic_binding: read binding by
# (chat_id, thread_id), walk the compression tip, switch_session on drift).
# This hook mirrors that guarantee for the plugin's state.json binding and
# answers unbound topics. It NEVER blocks a bound topic's message.
_SYNC_WARN_EVERY_S = 60.0  # rate-limit for the "choose a project" reply
_SYNC_WARNED: Dict[str, float] = {}


def _binding_at(chat_id: Any, thread_id: Any) -> Optional[Dict[str, Any]]:
    """The topic binding for an explicit (chat_id, thread_id), or None.

    pre_gateway_dispatch runs BEFORE the dispatch layer binds the session
    env (gateway/run_inbound.py:270), so the key is built from the event's
    source, never from HERMES_SESSION_*. A topic-less DM (topics disabled)
    normalizes to thread 0 — the chat's single flat lane.
    """
    tid = _norm_thread_id(thread_id)
    if tid is None:
        tid = 0
    if not str(chat_id or "").strip():
        return None
    key = f"{chat_id}:{tid}"
    try:
        bindings = _load_state().get("topic_bindings")
        entry = bindings.get(key) if isinstance(bindings, dict) else None
    except Exception:
        return None
    return dict(entry) if isinstance(entry, dict) else None


def _update_binding_session_at(chat_id: Any, thread_id: Any,
                               session_id: Optional[str]) -> None:
    """Record the session now working in an explicit topic (None resets).

    Explicit-key variant of :func:`_update_binding_session` for callback
    contexts, which carry no session env (see _session_key_from_query).
    """
    tid = _norm_thread_id(thread_id)
    if tid is None:
        tid = 0  # topic-less DM: the chat's flat lane
    if not str(chat_id or "").strip():
        return
    key = f"{chat_id}:{tid}"
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


def _binding_session_alive(session_id: str) -> bool:
    """state.db still has a live (not ended) row for *session_id*."""
    sid = str(session_id or "").strip()
    if not sid:
        return False
    conn = _open_state_db()
    if conn is None:
        return False
    try:
        row = conn.execute(
            "SELECT 1 FROM sessions WHERE id = ? AND ended_at IS NULL", (sid,)
        ).fetchone()
        return row is not None
    except Exception:
        return False
    finally:
        with _suppress(Exception):
            conn.close()


async def _send_sync_notice(chat_id: str, thread_id: Any, text: str) -> None:
    """Best-effort text reply into the topic.

    The pre_gateway_dispatch contract (run_inbound.py:114-146) has no reply
    action — only skip/rewrite/allow — so the text is sent by the plugin
    itself, like _notify_unbound_topic does (but awaited directly: this
    hook already runs on the gateway loop).
    """
    native = _NATIVE
    bot = getattr(native, "bot", None) if native is not None else None
    if bot is None:
        return
    kwargs: Dict[str, Any] = {"chat_id": str(chat_id), "text": text}
    tid = _norm_thread_id(thread_id)
    if tid:
        kwargs["message_thread_id"] = tid
    try:
        await bot.send_message(**kwargs)
    except Exception:
        logger.warning("tg-projects: sync notice send failed", exc_info=True)


async def _on_pre_gateway_dispatch_sync(event, gateway, session_store=None, **kwargs):
    """``pre_gateway_dispatch`` — seamless topic sync (fail-open, never blocks).

    Bound topic (state.json topic_bindings["<chat>:<thread>"].session_id):
    verify the chat's lane still points at that session; on drift re-point
    it via SessionStore.switch_session (the /resume mechanism, CAS on the
    current id), then return None so the message dispatches into the
    re-pointed lane. When the lane has no entry yet (fresh process) the
    switch is a no-op and the core's own topic-binding heal covers routing.

    Unbound topic (no binding / empty session_id): reply "choose a project",
    open the topic panel, and skip the message — free text without a project
    cwd must not silently create a default-directory session.

    Returns None in EVERY other case: no parking, no questions, no rewrite
    (the retired Yes/No gate is gone; handoff.py is a passthrough).
    """
    try:
        source = getattr(event, "source", None)
        if source is None:
            return None
        if str(getattr(getattr(source, "platform", None), "value", "") or "") != "telegram":
            return None
        if bool(getattr(event, "internal", False)):
            return None
        text = str(getattr(event, "text", "") or "").strip()
        if not text or text.startswith("/"):
            return None  # commands keep the normal (interrupt-capable) path
        if str(getattr(source, "chat_type", "") or "") != "dm":
            return None  # bindings are DM-topic keyed
        chat_id = str(getattr(source, "chat_id", "") or "").strip()
        tid = _norm_thread_id(getattr(source, "thread_id", None))
        if not chat_id:
            return None
        if tid is None:
            tid = 0  # topic-less DM (topics off): the chat's single flat lane

        binding = _binding_at(chat_id, tid)
        bound_sid = str((binding or {}).get("session_id") or "").strip()
        if not bound_sid:
            key = f"{chat_id}:{tid}"
            now = time.time()
            if now - _SYNC_WARNED.get(key, 0.0) >= _SYNC_WARN_EVERY_S:
                _SYNC_WARNED[key] = now
                await _send_sync_notice(chat_id, tid, "Сначала выбери проект: /menu")
            _panel = globals().get("_ensure_topic_panel")
            if callable(_panel):
                with _suppress(Exception):
                    _panel(chat_id, tid)
            return {"action": "skip", "reason": "unbound_topic"}

        # Bound topic: steer the lane onto binding.session_id when drifted.
        store = session_store if session_store is not None else getattr(
            gateway, "session_store", None)
        gen = getattr(gateway, "_generate_session_key", None)
        sk = str(gen(source) or "") if callable(gen) else ""
        if store is None or not sk:
            return None  # cannot resolve the lane — fail-open
        try:
            entry = store.lookup_by_session_key(sk)
        except Exception:
            entry = None
        current = str(getattr(entry, "session_id", "") or "") if entry is not None else ""
        if current == bound_sid:
            return None  # already in sync — plain dispatch
        if not _binding_session_alive(bound_sid):
            # Bound session ended/deleted: dispatch normally; the core's
            # auto-reset/recover handles the dead lane.
            return None
        try:
            async_store = getattr(gateway, "async_session_store", None)
            if async_store is not None:
                await async_store.switch_session(
                    sk, bound_sid, expected_session_id=current or None)
            else:
                store.switch_session(sk, bound_sid,
                                     expected_session_id=current or None)
            logger.info(
                "tg-projects: topic %s:%s lane re-pointed to bound session %s (was %s)",
                chat_id, tid, bound_sid, current or "?")
        except Exception:
            logger.warning("tg-projects: binding sync switch failed; "
                           "dispatching unchanged", exc_info=True)
        return None
    except Exception:
        logger.warning("tg-projects: sync pre_gateway_dispatch failed", exc_info=True)
        return None


def _migrate_flat_bindings() -> int:
    """Carry each chat's newest topic binding over to the flat key ``<chat>:0``.

    Topics were switched off: the chat's only lane is the topic-less one, but
    the bindings live under ``<chat>:<old_thread>``. The newest binding per
    chat (by updated_at) is copied — not moved, the stale topic keys age out
    via _prune_stale_state. Idempotent: a chat that already has a ``:0`` key
    is never touched.
    """
    migrated = 0
    try:
        with _CWD_LOCK:
            state = _load_state()
            bindings = state.get("topic_bindings")
            if not isinstance(bindings, dict) or not bindings:
                return 0
            by_chat: Dict[str, tuple] = {}
            for key, entry in bindings.items():
                if not isinstance(entry, dict):
                    continue
                chat_s, _, thread_s = str(key).rpartition(":")
                if not chat_s or not str(thread_s).isdigit() or int(thread_s) == 0:
                    continue  # flat keys themselves / malformed rows
                rank = (int(entry.get("updated_at") or 0), str(key))
                if chat_s not in by_chat or rank > by_chat[chat_s]:
                    by_chat[chat_s] = rank
            for chat_s, (_, best_key) in by_chat.items():
                flat_key = f"{chat_s}:0"
                if flat_key in bindings:
                    continue
                entry = dict(bindings[best_key])
                # The carried-over session id belonged to the retired topic
                # lane; the flat lane starts unbound so the user picks a
                # session explicitly (a stale id would silently steer the
                # lane onto an unrelated conversation).
                entry["session_id"] = None
                entry["updated_at"] = int(time.time())
                bindings[flat_key] = entry
                migrated += 1
            if migrated:
                _save_state(state)
                logger.info("tg-projects: migrated %d topic binding(s) to flat keys",
                            migrated)
    except Exception:
        logger.warning("tg-projects: flat binding migration failed", exc_info=True)
        return 0
    return migrated


# ------------------------------------------------------- PC -> TG reply mirror
# The desktop drives the same sessions through its OWN hermes serve process;
# its replies go to the desktop UI, not the Telegram topic (delivery needs
# the TG adapter, which only the TG gateway runs — authz_mixin.py
# _delivery_adapter_for fails closed without it). This poller watches
# state.db for NEW assistant messages in topic-bound sessions and mirrors
# them into the topic. TG-side turns are skipped: their replies were already
# delivered by the adapter (turn lease holder platform=telegram).
_PC_MIRROR_INTERVAL_S = 5.0
_PC_MIRROR_TG_GRACE_S = 60.0
_PC_MIRROR_SEEN: Dict[str, int] = {}        # session_id -> last seen message id
_PC_MIRROR_TG_UNTIL: Dict[str, float] = {}  # session_id -> rows before this ts are TG-side
_PC_MIRROR_TASK: Optional["asyncio.Task"] = None


def _pc_mirror_root_session(conn, session_id: str) -> str:
    """The compression-chain root id the turn lease is keyed by.

    hermes_state walks parent_session_id markers to the conversation root and
    keys session_turn_leases by THAT id (hermes_state_compression.
    _session_turn_lease_key_on_conn); a resumed/child session's lease is never
    under its own id, so the mirror must walk the same chain.
    """
    sid = str(session_id or "")
    seen = set()
    while sid and sid not in seen:
        seen.add(sid)
        try:
            row = conn.execute(
                "SELECT parent_session_id FROM sessions WHERE id = ?", (sid,)
            ).fetchone()
        except Exception:
            return sid
        parent = str((row["parent_session_id"] if row is not None else "") or "").strip()
        if not parent:
            return sid
        sid = parent
    return sid


def _pc_mirror_lease_tg_until(session_id: str) -> Optional[float]:
    """Until when assistant rows for *session_id* count as TG-delivered.

    A live lease on the conversation root means a gateway turn owns the
    session; the holder string embeds the routing key (owner_key = the
    session key), and a TELEGRAM lane key contains "telegram" — those turns
    were already delivered by the adapter. The window is remembered
    (expires_at + grace) so the final transcript flush that lands right
    after the lease is released is not mirrored a second time. A desktop
    holder (no "telegram" in the key) never suppresses the mirror.
    """
    path = _hermes_home() / "state.db"
    if not path.exists():
        return None
    conn = None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
        conn.row_factory = sqlite3.Row
        root = _pc_mirror_root_session(conn, session_id)
        for sid in (session_id, root):
            row = conn.execute(
                "SELECT holder, expires_at FROM session_turn_leases WHERE conversation_id = ?",
                (sid,),
            ).fetchone()
            if row is None:
                continue
            holder = str(row["holder"] or "")
            if "telegram" in holder:
                return float(row["expires_at"] or 0.0) + _PC_MIRROR_TG_GRACE_S
            return None
        return None
    except Exception:
        return None
    finally:
        if conn is not None:
            with _suppress(Exception):
                conn.close()


async def _pc_reply_mirror_loop(native: Any) -> None:
    """Mirror desktop-side assistant messages of bound sessions into topics."""
    bot = getattr(native, "bot", None)
    if bot is None:
        return
    while True:
        try:
            await asyncio.sleep(_PC_MIRROR_INTERVAL_S)
            try:
                raw = _load_state().get("topic_bindings") or {}
            except Exception:
                raw = {}
            bindings: Dict[str, str] = {}
            for key, entry in raw.items():
                if not isinstance(entry, dict):
                    continue
                sid = str(entry.get("session_id") or "").strip()
                if sid:
                    bindings[sid] = str(key)  # "<chat_id>:<thread_id>"
            path = _hermes_home() / "state.db"
            if not bindings or not path.exists():
                continue
            conn = None
            rows_by_sid: Dict[str, list] = {}
            try:
                conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
                conn.row_factory = sqlite3.Row
                for sid in bindings:
                    if sid not in _PC_MIRROR_SEEN:
                        # Bootstrap: never replay history — adopt the CURRENT
                        # tail (max message id) as already seen. Fetching a
                        # limited page instead left older pages "unseen" and
                        # the mirror replayed them 20 rows per cycle.
                        try:
                            row = conn.execute(
                                "SELECT COALESCE(MAX(id), 0) FROM messages WHERE session_id = ?",
                                (sid,),
                            ).fetchone()
                            _PC_MIRROR_SEEN[sid] = int(row[0]) if row is not None else 0
                        except Exception:
                            _PC_MIRROR_SEEN[sid] = 0
                        continue
                    since = _PC_MIRROR_SEEN.get(sid, 0)
                    rows = conn.execute(
                        "SELECT id, content, timestamp, display_kind FROM messages "
                        "WHERE session_id = ? AND role = 'assistant' AND id > ? "
                        "AND (display_kind IS NULL OR display_kind = '') "
                        "ORDER BY id LIMIT 20",
                        (sid, since),
                    ).fetchall()
                    if rows:
                        rows_by_sid[sid] = rows
            except Exception:
                logger.debug("tg-projects: pc mirror db read failed", exc_info=True)
            finally:
                if conn is not None:
                    with _suppress(Exception):
                        conn.close()
            for sid, rows in rows_by_sid.items():
                until = _pc_mirror_lease_tg_until(sid)
                if until is not None:
                    _PC_MIRROR_TG_UNTIL[sid] = until
                tg_until = _PC_MIRROR_TG_UNTIL.get(sid, 0.0)
                chat_id, _, tid = bindings[sid].partition(":")
                for row in rows:
                    _PC_MIRROR_SEEN[sid] = max(_PC_MIRROR_SEEN.get(sid, 0), int(row["id"]))
                    if float(row["timestamp"] or 0) <= tg_until:
                        continue  # TG-side turn — adapter already delivered it
                    content = str(row["content"] or "").strip()
                    if not content:
                        continue
                    if content.startswith(("<invoke", "<tool", "{\"output")):
                        continue  # raw tool-call markup — not a chat reply
                    kwargs: Dict[str, Any] = {
                        "chat_id": chat_id, "text": "🖥️ " + content[:3500]}
                    if tid.isdigit():
                        kwargs["message_thread_id"] = int(tid)
                    try:
                        await bot.send_message(**kwargs)
                    except Exception:
                        logger.warning("tg-projects: pc mirror send failed",
                                       exc_info=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("tg-projects: pc mirror loop iteration failed",
                           exc_info=True)


def _start_pc_reply_mirror(native: Any) -> None:
    """Start the mirror loop once per process (idempotent)."""
    global _PC_MIRROR_TASK
    if _PC_MIRROR_TASK is not None and not _PC_MIRROR_TASK.done():
        return
    try:
        _PC_MIRROR_TASK = asyncio.get_running_loop().create_task(
            _pc_reply_mirror_loop(native))
        logger.info("tg-projects: pc reply mirror started")
    except RuntimeError:
        pass  # factory ran off-loop; mirror stays off


# The chat-whitelisted owner: the only chat the command menu is scoped to.
_OWNER_CHAT_ID = "7559860199"


def _push_owner_command_menu(native: Any) -> None:
    """Set the owner-chat-scoped Telegram command menu (idempotent, best-effort).

    Core's _register_command_menu pushes 60 commands into the Default /
    AllPrivateChats / AllGroupChats scopes; its 60-command cap drops this
    plugin's commands (/menu, /projects, ...). A BotCommandScopeChat list
    REPLACES the effective menu in that one chat, so the owner always sees
    the ten commands that matter — the "/" menu button then needs no typing.
    """
    bot = getattr(native, "bot", None)
    if bot is None:
        return
    menu: List[tuple] = [
        ("menu", "Панель топика — кнопки проекта, сессии и управления"),
        ("projects", "Проекты и последние сессии каждого"),
        ("pnew", "Проект для следующей сессии (после /new)"),
        ("pproject", "Создать проект: имя + путь"),
        ("help", "Показать доступные команды"),
        ("status", "Статус сессии: модель, токены и контекст"),
        ("model", "Выбрать модель для текущей сессии"),
        ("profile", "Активный профиль и домашний каталог"),
        ("new", "Начать новую сессию с чистой историей"),
        ("stop", "Остановить все фоновые процессы"),
    ]

    async def _set() -> None:
        try:
            from telegram import BotCommand, BotCommandScopeChat
            await bot.set_my_commands(
                [BotCommand(cmd, desc) for cmd, desc in menu],
                scope=BotCommandScopeChat(chat_id=int(_OWNER_CHAT_ID)))
            logger.info("tg-projects: owner command menu set (%d commands)",
                        len(menu))
        except Exception:
            logger.warning("tg-projects: owner command menu push failed",
                           exc_info=True)

    try:
        asyncio.get_running_loop().create_task(_set())
    except RuntimeError:
        pass


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
               "WHERE cwd = ? AND message_count > 0 AND ended_at IS NULL")
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
                kwargs = {"chat_id": chat_id, "text": "Проекты — выберите каталог:",
                          "reply_markup": _project_list_keyboard(projects)}
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
# One project-list page: with 5+ projects the list paginates (tgp:pl:<offset>).
_PROJECTS_PER_PAGE = 5
_CB_PL_RE = re.compile(r"^tgp:pl:(\d+)$")


def _project_list_keyboard(projects: list, offset: int = 0):
    """The project picker: one tgp:p:<i> button per project on the current page.

    ``tgp:p:<i>`` carries the GLOBAL 1-based index (project_at resolves it
    against the full list), so paging never shifts what a button means.
    More pages than one → an [Ещё] row; page 2+ → a back-to-start row.
    """
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    offset = max(0, int(offset or 0))
    page = projects[offset:offset + _PROJECTS_PER_PAGE]
    keyboard = [
        [InlineKeyboardButton(f"📁 {p.name} [{p.slug}]",
                              callback_data=f"{CB_PREFIX}p:{offset + i}")]
        for i, p in enumerate(page, 1)
    ]
    if not keyboard:
        keyboard = [[InlineKeyboardButton("📭 Проектов нет", callback_data=_BACK_CB)]]
    rest = len(projects) - (offset + _PROJECTS_PER_PAGE)
    if rest > 0:
        keyboard.append([InlineKeyboardButton(
            f"Ещё → (осталось {rest})", callback_data=f"{CB_PREFIX}pl:{offset + _PROJECTS_PER_PAGE}")])
    if offset > 0:
        keyboard.append([InlineKeyboardButton(
            "⬅️ В начало списка", callback_data=f"{CB_PREFIX}pl:0")])
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
        InlineKeyboardButton("⚙️ Модель", callback_data=f"{CB_PREFIX}d:{index}"),
    ]
    row3 = [
        InlineKeyboardButton("⬅️ Назад", callback_data=_BACK_CB),
    ]
    del has_sessions, has_cwd  # the menu is the same regardless
    return InlineKeyboardMarkup([row1, row2, row3])


def _sessions_keyboard(index: int, sessions: list):
    """The «Сессии» screen: one tgp:s:<id> continue button per live session
    (tap = resume + bind the topic to it), a new-session button, and back to
    the project list."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    rows = [
        [InlineKeyboardButton(f"▶️ {str(s.get('title') or '').strip() or (str(s['id'])[:12] + '…')} · {s.get('message_count', 0)} msg",
                              callback_data=f"{CB_PREFIX}s:{s['id']}")]
        for s in sessions
    ]
    rows.append([InlineKeyboardButton("➕ Новая сессия", callback_data=f"{CB_PREFIX}n:{index}")])
    rows.append([InlineKeyboardButton("⬅️ К списку проектов", callback_data=_BACK_CB)])
    return InlineKeyboardMarkup(rows)


# ---------------------------------------------------- synthetic gateway message injection
def _source_from_query(adapter: Any, query: Any, thread_id: Optional[Any] = None):
    """SessionSource of a button tap: the TAPPER's identity, not the message's.

    The keyboards are sent BY THE BOT, so ``query.message.from_user`` is the
    bot itself — building the auth source from it made the gateway reject
    every synthetic command (/new, /resume, /stop) with "Unauthorized user:
    <bot_id>". The acting user is ``query.from_user`` (the human who tapped);
    it overrides the message author on both the adapter-built and the manual
    fallback path. *thread_id* (a Telegram topic id) overrides the tapped
    message's thread when given — the "new project" flow answers in the topic
    that owns the project, not the one where the button was pressed.
    """
    tapper = getattr(query, "from_user", None)
    tap_id = str(getattr(tapper, "id", "") or "").strip() or None
    tap_name = (str(getattr(tapper, "username", "") or getattr(tapper, "full_name", ""))
                or None)
    try:
        source = adapter._source_from_message_for_auth(query.message)
        if source is not None:
            import dataclasses
            if tap_id:
                source = dataclasses.replace(
                    source, user_id=tap_id, user_name=tap_name or source.user_name,
                    is_bot=bool(getattr(tapper, "is_bot", False)))
            if thread_id is not None and str(thread_id or "") not in {"", None}:
                source = dataclasses.replace(source, thread_id=str(thread_id))
            return source
    except Exception:
        logger.debug("tg-projects: source from message failed", exc_info=True)

    msg = getattr(query, "message", None)
    chat = getattr(msg, "chat", None) if msg is not None else None
    user = tapper if tapper is not None else getattr(msg, "from_user", None)
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
        result = bool(policy.is_admin(uid))
        try:
            logger.info(
                "tg-projects: admin check uid=%r platform=%r chat_type=%r -> "
                "enabled=%s admins=%s result=%s",
                uid, getattr(getattr(source, "platform", None), "value", None),
                getattr(source, "chat_type", None), getattr(policy, "enabled", "?"),
                sorted(getattr(policy, "admin_user_ids", ()) or ()), result)
        except Exception:
            logger.info("tg-projects: admin check uid=%r -> result=%s", uid, result)
        return result
    except Exception:
        logger.warning("tg-projects: admin check failed", exc_info=True)
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

    # Approval buttons (tgp:a:<choice>:<rid>:<digest8>) — answer a tg-topics
    # transport prompt; consumed before the project-menu dispatch, like every
    # prompt-style callback.
    m_appr = _APPROVAL_CB_RE.match(data)
    if m_appr is not None:
        await _handle_approval_callback(query, m_appr)
        return

    # Topic panel buttons (tgp:pb:*) — the pinned panel's own screens; they edit
    # the panel message in place and never fall through to the project menus.
    if data.startswith("tgp:pb:"):
        await _handle_panel_callback(query, data)
        return

    # Back to the project list (tgp:b) and list pagination (tgp:pl:<offset>).
    if data == _BACK_CB:
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
        await _edit_project_list(query, projects, 0)
        await query.answer()
        return

    m_pl = _CB_PL_RE.match(data)
    if m_pl is not None:  # tgp:pl:<offset> — project list page
        try:
            projects = _list_projects()
        except Exception as exc:
            logger.warning("tg-projects: projects.db read failed: %s", exc)
            await query.answer()
            return
        await _edit_project_list(query, projects, int(m_pl.group(1)))
        await query.answer()
        return

    m = _CB_SESSION_RE.match(data)
    if m is not None:  # tgp:s:<id> — continue a listed session
        await _do_resume_by_id(query, m.group(1))
        return

    if data.startswith("tgp:pw:"):  # project wizard: [➕ Новый] / [❌ Отмена]
        await _wizard_on_button(query, data)
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
        if action == "p":  # project button → menu
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
        # Re-rendering the very same screen (the old "Ещё раз" pattern) makes
        # PTB raise BadRequest("Message is not modified") — that is success for
        # the user, not an error: swallow it instead of "см. лог hermes".
        if "not modified" not in str(exc).lower():
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
    # Seamless sync: the resumed session becomes the topic's bound session,
    # so the topic's next free text follows it (pre_gateway_dispatch sync).
    if source is not None:
        _update_binding_session_at(
            str(getattr(source, "chat_id", "") or ""),
            getattr(source, "thread_id", None), session_id)
    if cross_origin and admin:
        cmd = f"/resume --all {session_id}"
        suffix = " (кросс-оригин: admin --all)"
    else:
        cmd = f"/resume {session_id}"
        suffix = " (если сессия из другого источника — см. примечание)"
    logger.info("tg-projects: resume dispatch session=%s cross_origin=%s admin=%s cmd=%r",
                session_id, cross_origin, admin, cmd)

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
    notes.append("📋 Сессии — живые (незавершённые) сессии этого каталога")
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

    # Seamless sync: bind the topic to the resumed session so its next free
    # text lands in the SAME sessions.id (pre_gateway_dispatch sync hook).
    source = _adapter_source_for(query)
    if source is not None:
        _update_binding_session_at(
            str(getattr(source, "chat_id", "") or ""),
            getattr(source, "thread_id", None), target["id"])

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

    # Cross-origin (desktop) targets need the admin --all form, exactly like
    # _do_resume_by_id — a plain /resume hits the gateway's IDOR guard.
    cross_origin = not _session_is_same_source(
        state_conn, target["id"], target.get("source", ""), _adapter_source_for(query))
    admin = _callers_admin(_adapter_source_for(query))
    resume_cmd = (f"/resume --all {target['id']}" if cross_origin and admin
                  else f"/resume {target['id']}")
    logger.info("tg-projects: resume dispatch session=%s cross_origin=%s admin=%s cmd=%r",
                target["id"], cross_origin, admin, resume_cmd)
    await _send_gateway_command(query, resume_cmd)


async def _edit_project_list(query, projects: list, offset: int = 0) -> None:
    """The project picker screen (the tgp:b back target and tgp:pl pages):
    re-render the paginated project list in place."""
    offset = max(0, int(offset or 0))
    text = "Проекты — выберите каталог:"
    if offset:
        text = f"Проекты — страница {offset // _PROJECTS_PER_PAGE + 1}:"
    with _suppress(Exception):
        await query.edit_message_text(text,
                                      reply_markup=_project_list_keyboard(projects, offset))


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
            label = str(s.get("title") or "").strip() or str(s["id"])[:12] + "…"
            lines.append(f"{flag}• {label} ({_fmt_ts(s['started_at'])}) {s['message_count']} msg — {_trim(s['last_message'], 60) or '(пусто)'}")
        if has_desktop:
            lines.append("")
            lines.append("⚠️ 🖥️ — сессия из десктопа; одновременная работа с двух сторон пишет историю из двух процессов.")
    lines.append("")
    if sessions:
        lines.append("Тап по кнопке сессии ниже — продолжить её (и закрепить за топиком).")
    else:
        lines.append("Продолжить: сессий нет — создайте новую кнопкой ниже.")
    await query.edit_message_text("\n".join(lines),
                                  reply_markup=_sessions_keyboard(int(index_str), sessions))


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
        _start_pc_reply_mirror(native)
        _push_owner_command_menu(native)
    except Exception:
        logger.warning("tg-projects: telegram wiring failed", exc_info=True)
        _NATIVE = None
        _ADAPTER = None


# --------------------------------------------------------------- session handoff
# Cross-device continuity lives in handoff.py (legacy module, retained but
# never called: its pre_gateway_dispatch hook is a passthrough and register()
# wires the seamless-sync hook instead). The Yes/No callback flow is gone.


# ------------------------------------------------------------------- topic panel
# One pinned message per topic: state.json["topic_panels"]["<chat_id>:<thread_id>"]
# = message_id. /menu recreates it (unpin + delete the old panel, send, pin).
# Sub-screens ([📁 Проект] list, [🧵 Сессия] list, Approvals, Ещё) edit the panel
# message in place; every sub-screen carries a "⬅️ Назад" (tgp:pb:back) button
# that re-renders the panel. Binding writes here use an explicit chat/thread key
# (tgp:pb:* callbacks run on the event loop without a session-env scope, so
# _set_topic_binding's contextvar key is unavailable in a callback).
_PB_CB_PREFIX = "tgp:pb:"
_PB_SESSIONS_LIMIT = 20
_PB_CB_PROJ_RE = re.compile(r"^tgp:pb:projp:(\d+)$")
_PB_CB_SESS_RE = re.compile(r"^tgp:pb:sesss:([A-Za-z0-9_\-]{8,46})$")
_PB_CB_MORE_RE = re.compile(r"^tgp:pb:more:cmd:(status|diff|agents|help)$")


def _pb_panel_key(chat_id: Any, thread_id: Any) -> str:
    """``f"{chat_id}:{thread_id}"`` (thread 0 = plain DM panel)."""
    return f"{chat_id}:{int(thread_id or 0)}"


def _pb_get_panel_message_id(chat_id: str, thread_id: Optional[int]) -> Optional[int]:
    """The pinned panel's message_id for this chat/thread, or None."""
    try:
        value = (_load_state().get("topic_panels") or {}).get(_pb_panel_key(chat_id, thread_id))
    except Exception:
        return None
    text = str(value or "").strip()
    return int(text) if text.isdigit() else None


def _pb_set_panel_message_id(chat_id: str, thread_id: Optional[int], message_id: int) -> None:
    with _CWD_LOCK:
        state = _load_state()
        state.setdefault("topic_panels", {})[_pb_panel_key(chat_id, thread_id)] = int(message_id)
        _save_state(state)


def _pb_clear_panel(chat_id: str, thread_id: Optional[int]) -> None:
    """Forget the panel message id (called before a fresh panel is sent)."""
    with _CWD_LOCK:
        state = _load_state()
        panels = state.get("topic_panels") or {}
        if panels.pop(_pb_panel_key(chat_id, thread_id), None) is not None:
            if not panels:
                state.pop("topic_panels", None)
            _save_state(state)


def _pb_binding(chat_id: str, thread_id: Optional[int]) -> Optional[Dict[str, Any]]:
    """The topic's topic_bindings entry, read by an explicit key (a copy).

    ``thread_id=None`` (topics disabled — the chat's flat lane) normalizes
    to key ``<chat>:0``.
    """
    tid = 0 if thread_id is None else int(thread_id)
    try:
        entry = (_load_state().get("topic_bindings") or {}).get(f"{chat_id}:{tid}")
    except Exception:
        return None
    return dict(entry) if isinstance(entry, dict) else None


def _pb_write_binding(chat_id: str, thread_id: Optional[int], project_id: Any,
                      project_name: Any, cwd: Any, session_id: Any = None) -> bool:
    """Create/refresh the binding at an explicit key; the session resets to None
    (the [📁 Проект] pick semantics: _set_topic_binding + _update_binding_session(None)).
    ``thread_id=None`` writes the flat-lane key ``<chat>:0``."""
    tid = 0 if thread_id is None else int(thread_id)
    key = f"{chat_id}:{tid}"
    clean = str(session_id).strip() if session_id is not None else ""
    with _CWD_LOCK:
        state = _load_state()
        bindings = state.setdefault("topic_bindings", {})
        entry = bindings.get(key) if isinstance(bindings.get(key), dict) else {}
        entry["project_id"] = str(project_id)
        entry["project_name"] = str(project_name)
        entry["cwd"] = str(cwd)
        entry["session_id"] = clean or None
        entry["updated_at"] = int(time.time())
        bindings[key] = entry
        _save_state(state)
    return True


def _pb_write_binding_session(chat_id: str, thread_id: Optional[int],
                              session_id: Optional[str]) -> bool:
    """Record the topic's working session at an explicit key (None resets).
    ``thread_id=None`` targets the flat-lane key ``<chat>:0``."""
    tid = 0 if thread_id is None else int(thread_id)
    key = f"{chat_id}:{tid}"
    with _CWD_LOCK:
        state = _load_state()
        bindings = state.get("topic_bindings") or {}
        entry = bindings.get(key)
        if not isinstance(entry, dict):
            return False  # bind the topic first
        clean = str(session_id).strip() if session_id is not None else ""
        entry["session_id"] = clean or None
        entry["updated_at"] = int(time.time())
        bindings[key] = entry
        _save_state(state)
    return True


def _pb_session_status(state_conn, session_id: str) -> str:
    """The live status string ('online'/'idle') for a bound session."""
    try:
        return str(_session_live_status(state_conn, session_id).get("status") or "idle")
    except Exception:
        return "idle"


def _pb_short_session_id(session_id: str) -> str:
    text = str(session_id or "").strip()
    return text[:12] + "…" if len(text) > 12 else text


def _pb_session_label(s: Dict[str, Any]) -> str:
    """A short session label for buttons/lines: the Hermes title, id fallback."""
    title = str(s.get("title") or "").strip()
    sid = str(s.get("id") or "").strip()
    if title and title != "(без названия)":
        return _trim(title, 28)
    return (_pb_short_session_id(sid) if sid else "—")


def _pb_panel_text(binding: Optional[Dict[str, Any]], status: str = "") -> str:
    """The pinned panel body: project / session / cwd."""
    if not binding:
        return "📁  —\n🧵  —\n📂  —\n\nСначала выбери проект: [📁 Проект]"
    session_id = str(binding.get("session_id") or "").strip()
    session_line = f"{_pb_short_session_id(session_id)} · {status or 'idle'}" if session_id else "—"
    if session_id:
        # Prefer the session's Hermes title (state.db sessions.title) over the raw id.
        try:
            state_conn = _sessions_state_conn()
            try:
                row = state_conn.execute(
                    "SELECT title FROM sessions WHERE id = ?", (session_id,)
                ).fetchone()
                title = str((row["title"] if row is not None else "") or "").strip()
            finally:
                with _suppress(Exception):
                    state_conn.close()
            if title and title != "(без названия)":
                session_line = f"{_trim(title, 28)} · {status or 'idle'}"
        except Exception:
            pass
    return (
        f"📁  {binding.get('project_name') or '—'}\n"
        f"🧵  {session_line}\n"
        f"📂  {binding.get('cwd') or '—'}"
    )


def _pb_panel_keyboard():
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📁 Проект", callback_data=f"{_PB_CB_PREFIX}proj"),
         InlineKeyboardButton("🧵 Сессия", callback_data=f"{_PB_CB_PREFIX}sess"),
         InlineKeyboardButton("➕ Новая", callback_data=f"{_PB_CB_PREFIX}new")],
        [InlineKeyboardButton("⏹ Stop", callback_data=f"{_PB_CB_PREFIX}stop"),
         InlineKeyboardButton("⚙️ Ещё", callback_data=f"{_PB_CB_PREFIX}more")],
    ])


def _pb_back_keyboard():
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data=f"{_PB_CB_PREFIX}back")]])


async def _pb_create(chat_id: str, thread_id: Optional[int]) -> bool:
    """(Re)create the pinned panel: unpin + delete the old message, send, pin, record."""
    bot = getattr(_NATIVE, "bot", None) if _NATIVE is not None else None
    if bot is None or not chat_id:
        return False

    old_id = _pb_get_panel_message_id(chat_id, thread_id)
    if old_id is not None:
        try:
            if hasattr(bot, "unpin_chat_message"):
                await bot.unpin_chat_message(chat_id=int(chat_id), message_id=int(old_id))
        except Exception:
            logger.debug("tg-projects: old panel unpin failed", exc_info=True)
        try:
            if hasattr(bot, "delete_message"):
                await bot.delete_message(chat_id=int(chat_id), message_id=int(old_id))
        except Exception:
            logger.debug("tg-projects: old panel delete failed", exc_info=True)
        _pb_clear_panel(chat_id, thread_id)

    binding = _pb_binding(chat_id, thread_id)
    status = ""
    if binding and binding.get("session_id"):
        state_conn = _sessions_state_conn()
        try:
            status = _pb_session_status(state_conn, str(binding.get("session_id")))
        finally:
            with _suppress(Exception):
                state_conn.close()

    kwargs: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": _pb_panel_text(binding, status),
        "reply_markup": _pb_panel_keyboard(),
    }
    if thread_id:
        kwargs["message_thread_id"] = int(thread_id)
    try:
        message = await bot.send_message(**kwargs)
    except Exception:
        # A chat with topics disabled rejects a stale message_thread_id —
        # retry flat once before giving up.
        if "message_thread_id" not in kwargs:
            logger.warning("tg-projects: panel send failed", exc_info=True)
            return False
        kwargs.pop("message_thread_id", None)
        try:
            message = await bot.send_message(**kwargs)
        except Exception:
            logger.warning("tg-projects: panel send failed", exc_info=True)
            return False
    message_id = getattr(message, "message_id", None)
    if not message_id:
        return False
    _pb_set_panel_message_id(chat_id, thread_id, int(message_id))
    try:
        if hasattr(bot, "pin_chat_message"):
            await bot.pin_chat_message(chat_id=int(chat_id), message_id=int(message_id),
                                       disable_notification=True)
    except Exception:
        logger.warning("tg-projects: panel pin failed", exc_info=True)
    return True


def _ensure_topic_panel(chat_id: Any, thread_id: Any) -> None:
    """Create the topic panel when absent (sync hook's unbound-topic path).

    Sync (thread) context: the coroutine is scheduled onto the wired loop;
    a missing panel or wiring degrades silently.
    """
    chat = str(chat_id or "").strip()
    tid = _norm_thread_id(thread_id)
    if not chat:
        return
    if _pb_get_panel_message_id(chat, tid) is not None:
        return  # panel already pinned — the binding data lives on it
    native = _NATIVE
    loop = _WIRE_LOOP
    if native is None or loop is None:
        return

    async def _make() -> None:
        with _suppress(Exception):
            await _pb_create(chat, tid)

    try:
        asyncio.run_coroutine_threadsafe(_make(), loop)
    except Exception:
        logger.debug("tg-projects: panel ensure scheduling failed", exc_info=True)


async def _pb_render(query, chat_id: str, thread_id: Optional[int]) -> None:
    """Edit the panel message back to the panel view."""
    binding = _pb_binding(chat_id, thread_id)
    status = ""
    if binding and binding.get("session_id"):
        state_conn = _sessions_state_conn()
        try:
            status = _pb_session_status(state_conn, str(binding.get("session_id")))
        finally:
            with _suppress(Exception):
                state_conn.close()
    with _suppress(Exception):
        await query.edit_message_text(_pb_panel_text(binding, status),
                                      reply_markup=_pb_panel_keyboard())


def _pb_projects_text(projects: list) -> str:
    if not projects:
        return "📁 Проектов пока нет — создайте: /pproject <название> <абсолютный путь>"
    return "📁 Выбор проекта (закрепит его за этим топиком):"


async def _pb_project_screen(query, chat_id: str, thread_id: Optional[int]) -> None:
    """[📁 Проект]: the project list — one tgp:pb:projp:<i> button per project."""
    try:
        projects = _list_projects()
    except Exception as exc:
        with _suppress(Exception):
            await query.edit_message_text(f"Не удалось прочитать проекты: {exc}",
                                          reply_markup=_pb_back_keyboard())
        return
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    rows = [[InlineKeyboardButton(f"📁 {p.name} [{p.slug}]",
                                  callback_data=f"{_PB_CB_PREFIX}projp:{i}")]
            for i, p in enumerate(projects, 1)]
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"{_PB_CB_PREFIX}back")])
    with _suppress(Exception):
        await query.edit_message_text(_pb_projects_text(projects),
                                      reply_markup=InlineKeyboardMarkup(rows))


async def _pb_pick_project(query, chat_id: str, thread_id: Optional[int],
                           index_str: str) -> None:
    """A tgp:pb:projp:<i> tap: bind the topic (or the flat chat lane) to the
    project, reset the session."""
    try:
        projects = _list_projects()
    except Exception as exc:
        with _suppress(Exception):
            await query.edit_message_text(f"Не удалось прочитать проекты: {exc}",
                                          reply_markup=_pb_back_keyboard())
        return
    project = _project_at(projects, index_str)
    if project is None:
        with _suppress(Exception):
            await query.edit_message_text(f"Проект #{index_str} больше не в списке — выберите заново.",
                                          reply_markup=_pb_back_keyboard())
        return
    cwd = _project_cwd(project)
    if not cwd or not os.path.isdir(cwd):
        with _suppress(Exception):
            await query.edit_message_text(f"Каталог {cwd or 'не указан'} недоступен — проект не привязан.",
                                          reply_markup=_pb_back_keyboard())
        return
    _pb_write_binding(chat_id, thread_id, getattr(project, "id", None),
                      getattr(project, "name", None), cwd, None)
    await _pb_render(query, chat_id, thread_id)


def _pb_sessions_text(binding: Optional[Dict[str, Any]], sessions: list) -> str:
    """The [🧵 Сессия] sub-screen body (all sessions of the project cwd)."""
    if not binding or not binding.get("cwd"):
        return "🧵 Сначала выбери проект: [📁 Проект]."
    lines = [f"🧵 Сессии проекта {binding.get('project_name') or '?'}:"]
    if not sessions:
        lines.append("(нет сессий в этом каталоге)")
    else:
        for s in sessions:
            flag = "🖥️ " if s.get("source") == "desktop" else ""
            lines.append(f"{flag}• {_pb_session_label(s)} · "
                         f"{s.get('status') or 'idle'} · {s.get('message_count', 0)} msg")
    return "\n".join(lines)


async def _pb_sessions_screen(query, chat_id: str, thread_id: Optional[int]) -> None:
    """[🧵 Сессия]: every session of the binding cwd (no source filter), up to 20."""
    binding = _pb_binding(chat_id, thread_id)
    sessions: list = []
    if binding and binding.get("cwd"):
        state_conn = _sessions_state_conn()
        try:
            sessions = _sessions_for_cwd(state_conn, str(binding["cwd"]),
                                         limit=_PB_SESSIONS_LIMIT)
        finally:
            with _suppress(Exception):
                state_conn.close()
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    rows = [[InlineKeyboardButton(
                 f"▶️ {('🖥️ ' if s.get('source') == 'desktop' else '')}"
                 f"{_pb_session_label(s)} · {s.get('status') or 'idle'}",
                 callback_data=f"{_PB_CB_PREFIX}sesss:{s['id']}")]
            for s in sessions]
    rows.append([InlineKeyboardButton("➕ Новая сессия", callback_data=f"{_PB_CB_PREFIX}new")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"{_PB_CB_PREFIX}back")])
    with _suppress(Exception):
        await query.edit_message_text(_pb_sessions_text(binding, sessions),
                                      reply_markup=InlineKeyboardMarkup(rows))


async def _pb_pick_session(query, chat_id: str, thread_id: Optional[int],
                           session_id: str) -> None:
    """A tgp:pb:sesss:<id> tap: bind the session, resume it, re-render the panel."""
    state_conn = _sessions_state_conn()
    try:
        with _suppress(Exception):
            row = state_conn.execute("SELECT id FROM sessions WHERE id = ?",
                                     (session_id,)).fetchone()
        found = row is not None
    finally:
        with _suppress(Exception):
            state_conn.close()
    if not found:
        with _suppress(Exception):
            await query.edit_message_text(
                f"❌ Сессия {session_id} не найдена — обновите список: [🧵 Сессия].",
                reply_markup=_pb_back_keyboard())
        return
    _pb_write_binding_session(chat_id, thread_id, session_id)
    await _do_resume_by_id(query, session_id)
    await _pb_render(query, chat_id, thread_id)


async def _pb_new_session(query, chat_id: str, thread_id: Optional[int]) -> None:
    """[➕ Новая]: /new in the binding cwd; on_session_start records the session id."""
    binding = _pb_binding(chat_id, thread_id)
    if not binding or not binding.get("cwd") or not os.path.isdir(str(binding["cwd"])):
        with _suppress(Exception):
            await query.edit_message_text("➕ Сначала выбери проект: [📁 Проект].",
                                          reply_markup=_pb_back_keyboard())
        return
    project = _make_light_project(str(binding.get("project_name")),
                                  str(binding["cwd"]), binding.get("project_id"))
    state_conn = _sessions_state_conn()
    try:
        await _do_new_session(query, project, "", state_conn)
    finally:
        with _suppress(Exception):
            state_conn.close()
    await _pb_render(query, chat_id, thread_id)


async def _pb_stop(query) -> None:
    """[⏹ Stop]: /stop is a real gateway command (interrupt_then_dispatch), so a
    synthetic event stops the running turn of this chat's session in both idle
    and busy states."""
    if _ADAPTER is None:
        with _suppress(Exception):
            await query.edit_message_text("⏹ Остановка недоступна (адаптер не подключён).",
                                          reply_markup=_pb_back_keyboard())
        return
    with _suppress(Exception):
        await query.edit_message_text(
            "⏹ Останавливаю текущую сессию (/stop) — ответ придёт ниже.",
            reply_markup=_pb_back_keyboard())
    await _send_gateway_command(query, "/stop")


async def _pb_more_screen(query) -> None:
    """[⚙️ Ещё]: the reference-command sub-menu."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    rows = [[InlineKeyboardButton(f"/{name}", callback_data=f"{_PB_CB_PREFIX}more:cmd:{name}")]
            for name in ("status", "diff", "agents", "help")]
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"{_PB_CB_PREFIX}back")])
    with _suppress(Exception):
        await query.edit_message_text(
            "⚙️ Ещё — справочные команды (ответ придёт отдельным сообщением):",
            reply_markup=InlineKeyboardMarkup(rows))


async def _pb_run_command(query, name: str) -> None:
    """A tgp:pb:more:cmd:<name> tap: send /<name> through the gateway."""
    if _ADAPTER is None:
        with _suppress(Exception):
            await query.edit_message_text(f"⚠️ /{name} сейчас недоступен (адаптер не подключён).",
                                          reply_markup=_pb_back_keyboard())
        return
    with _suppress(Exception):
        await query.edit_message_text(f"Отправляю /{name} — ответ придёт ниже.",
                                      reply_markup=_pb_back_keyboard())
    await _send_gateway_command(query, f"/{name}")


async def _handle_panel_callback(query, data: str) -> None:
    """Route tgp:pb:* taps; every screen edits the panel message in place."""
    msg = getattr(query, "message", None)
    chat_id = str(getattr(msg, "chat_id", "") or "").strip()
    thread_id = _norm_thread_id(getattr(msg, "message_thread_id", None))
    rest = data[len(_PB_CB_PREFIX):]
    try:
        if not chat_id:
            await query.answer()
            return
        if rest == "back":
            await _pb_render(query, chat_id, thread_id)
        elif rest == "proj":
            await _pb_project_screen(query, chat_id, thread_id)
        elif rest == "sess":
            await _pb_sessions_screen(query, chat_id, thread_id)
        elif rest == "new":
            await _pb_new_session(query, chat_id, thread_id)
        elif rest == "stop":
            await _pb_stop(query)
        elif rest == "more":
            await _pb_more_screen(query)
        else:
            m = _PB_CB_PROJ_RE.match(data)
            if m is not None:
                await _pb_pick_project(query, chat_id, thread_id, m.group(1))
            else:
                m = _PB_CB_SESS_RE.match(data)
                if m is not None:
                    await _pb_pick_session(query, chat_id, thread_id, m.group(1))
                else:
                    m = _PB_CB_MORE_RE.match(data)
                    if m is not None:
                        await _pb_run_command(query, m.group(1))
        with _suppress(Exception):
            await query.answer()
    except Exception as exc:
        logger.warning("tg-projects: panel callback %r failed: %s", data, exc, exc_info=True)
        with _suppress(Exception):
            await query.answer("Ошибка — см. лог hermes")


async def _menu_command(raw_args: str) -> Optional[str]:
    """Create/refresh the pinned topic panel (unpin + delete the old one first)."""
    if (raw_args or "").strip():
        return "Использование: /menu — без аргументов."
    _prune_stale_state()  # drop bindings/panels untouched for 7 days
    _migrate_flat_bindings()  # topics off: carry the newest binding to <chat>:0
    _wizard_reset_chat(str(_session_env("HERMES_SESSION_CHAT_ID", "") or "").strip())
    chat_id = str(_session_env("HERMES_SESSION_CHAT_ID", "") or "").strip()
    thread_id = _norm_thread_id(_current_thread_id())
    if not chat_id:
        return "Панель доступна только в Telegram-чате (не удалось определить чат)."
    if _NATIVE is None or getattr(_NATIVE, "bot", None) is None:
        return ("⚠️ Панель сейчас недоступна (фабрика Telegram не подключилась). "
                "Текстом: /projects — список, /pnew <N> + /new — новая сессия.")
    if not await _pb_create(chat_id, thread_id):
        return "⚠️ Не удалось отправить панель — см. лог hermes."
    return None


# ------------------------------------------------------------- approval transport
# tg-topics: presents host-owned dangerous-command approvals as Telegram
# inline buttons. Registered via ctx.register_approval_transport but INACTIVE
# until config.yaml selects it (``security.approval.transport: tg-topics``);
# detection, allowed scopes, persistence and the fail-closed timeout stay
# host-owned (hermes_cli.approval_transport.py).
#
# Routing: the host runs present_fn on a plain daemon worker thread, so
# session ContextVars are NOT inherited there and the ApprovalRequest itself
# carries NO session_key (it is only mixed into the digest). The requesting
# session is recovered best-effort from HERMES_SESSION_ID (process env,
# re-published by the gateway each turn) and mapped to a topic through
# state.json topic_bindings (reverse lookup on session_id) and the state.db
# sessions row (chat_id / thread_id). A miss walks the fallback chain (any
# bound topic, then the owner's DM) — there is ALWAYS a place to present, so
# the transport never auto-approves and never silently denies for lack of a
# surface. A process without the Telegram adapter wired (desktop backend)
# cannot send and fails closed to deny; set ``transport_fallback: builtin``
# in config.yaml if that surface should fall back to its local prompt.
_APPROVALS_KEY = "approvals_map"
# tgp:a:<choice>:<request_id(32 hex)>:<digest prefix(8 hex)> — 55 bytes max.
_APPROVAL_CB_RE = re.compile(
    r"^tgp:a:(once|session|always|deny):([a-f0-9]{32}):([a-f0-9]{8})$")
# The chat-whitelisted owner's DM: the always-available presentation target.
_APPROVAL_FALLBACK_CHAT = "7559860199"
# Hard cap on one presentation wait (the host default is 300s anyway).
_APPROVAL_MAX_WAIT_S = 300.0
# request_id -> {"event", "request", "choice"}: in-memory waiters the
# callback handler wakes. Entries live only while _approval_present waits.
_APPROVAL_WAITERS: Dict[str, Dict[str, Any]] = {}
_APPROVAL_CHOICE_TEXT = {
    "once": "✅ Одобрено (одноразово)",
    "session": "✅ Одобрено до конца сессии",
    "always": "✅✅ Одобрено всегда",
    "deny": "❌ Отклонено",
}


def _approval_topic_from_bindings(session_id: str) -> Optional[tuple]:
    """The topic whose binding records *session_id* (reverse lookup)."""
    if not session_id:
        return None
    try:
        bindings = _load_state().get("topic_bindings") or {}
    except Exception:
        return None
    for key, entry in bindings.items():
        if not isinstance(entry, dict):
            continue
        if str(entry.get("session_id") or "").strip() == session_id:
            chat_s, _, thread_s = str(key).partition(":")
            thread = _norm_thread_id(thread_s)
            if chat_s and thread is not None:
                return (chat_s, thread)
    return None


def _approval_topic_from_state_db(session_id: str) -> Optional[tuple]:
    """The (chat_id, thread_id|None) state.db records for *session_id*."""
    if not session_id:
        return None
    conn = None
    try:
        conn = _sessions_state_conn()
        row = conn.execute(
            "SELECT chat_id, thread_id FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
    except Exception:
        return None
    finally:
        if conn is not None:
            with _suppress(Exception):
                conn.close()
    if row is None:
        return None
    chat = str(row["chat_id"] or "").strip()
    if not chat:
        return None  # desktop/CLI row: no Telegram route
    return (chat, _norm_thread_id(row["thread_id"]))


def _approval_any_bound_topic() -> Optional[tuple]:
    """Any bound topic (newest first, session-bound preferred) — the
    single-whitelist-user fallback when the requesting session is unknown."""
    try:
        bindings = _load_state().get("topic_bindings") or {}
    except Exception:
        return None
    candidates: List[tuple] = []
    for key, entry in bindings.items():
        if not isinstance(entry, dict):
            continue
        chat_s, _, thread_s = str(key).partition(":")
        thread = _norm_thread_id(thread_s)
        if not chat_s or thread is None:
            continue
        rank = 1 if entry.get("session_id") else 0
        candidates.append((rank, int(entry.get("updated_at") or 0), chat_s, thread))
    if not candidates:
        return None
    best = max(candidates)
    return (best[2], best[3])


def _approval_target() -> tuple:
    """(chat_id, thread_id|None) the approval prompt is shown in.

    Priority: the requesting session's own topic (bindings reverse lookup,
    then the state.db row), then any bound topic, then the owner's DM.
    """
    session_id = str(_session_env("HERMES_SESSION_ID", "") or "").strip()
    topic = _approval_topic_from_bindings(session_id)
    if topic is None:
        topic = _approval_topic_from_state_db(session_id)
    if topic is None:
        topic = _approval_any_bound_topic()
    if topic is not None:
        return topic
    return (_APPROVAL_FALLBACK_CHAT, None)


def _tg_send_sync(kwargs: Dict[str, Any], wait_s: float) -> Any:
    """bot.send_message on the gateway loop, waited synchronously.

    Returns the sent Message, or None (no wiring, loop gone, send error,
    wait timeout) — the caller fails closed on None.
    """
    loop, native = _WIRE_LOOP, _NATIVE
    bot = getattr(native, "bot", None) if native is not None else None
    if loop is None or bot is None:
        return None

    async def _send() -> Any:
        return await bot.send_message(**kwargs)

    try:
        return asyncio.run_coroutine_threadsafe(
            _send(), loop).result(timeout=max(float(wait_s), 1.0))
    except Exception:
        logger.warning("tg-projects: approval send failed", exc_info=True)
        return None


def _tg_edit_best_effort(chat_id: Any, message_id: Any, text: str) -> None:
    """bot.edit_message_text from a worker thread; never raises."""
    loop, native = _WIRE_LOOP, _NATIVE
    bot = getattr(native, "bot", None) if native is not None else None
    if loop is None or bot is None or not chat_id or not message_id:
        return

    async def _edit() -> None:
        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=message_id, text=text)
        except Exception:
            logger.debug("tg-projects: approval edit failed", exc_info=True)

    try:
        asyncio.run_coroutine_threadsafe(_edit(), loop)
    except Exception:
        pass


def _approval_prune(now: float) -> None:
    """Drop long-expired approvals_map entries and their waiters."""
    with _CWD_LOCK:
        try:
            state = _load_state()
        except Exception:
            return
        amap = state.get(_APPROVALS_KEY)
        if not isinstance(amap, dict) or not amap:
            return
        stale = [rid for rid, entry in amap.items()
                 if isinstance(entry, dict)
                 and float(entry.get("expires") or 0) < now - 60]
        if not stale:
            return
        for rid in stale:
            amap.pop(rid, None)
            _APPROVAL_WAITERS.pop(rid, None)
        _save_state(state)


def _approval_drop(rid: str) -> None:
    """Remove one approvals_map entry (resolved, expired or failed)."""
    with _CWD_LOCK:
        try:
            state = _load_state()
        except Exception:
            return
        amap = state.get(_APPROVALS_KEY)
        if isinstance(amap, dict) and rid in amap:
            amap.pop(rid, None)
            _save_state(state)


def _approval_message(request: Any, timeout_s: float) -> str:
    """The prompt text: description + redacted command (monospace block)."""
    command = str(getattr(request, "command", "") or "")
    if len(command) > 900:
        command = command[:900] + "…"
    description = _trim(str(getattr(request, "description", "") or ""), 300)
    lines = ["⚠️ Требуется подтверждение команды Hermes"]
    if description:
        lines += ["", description]
    lines += ["", f"```\n{command}\n```", "",
              f"⏳ Без ответа через {int(timeout_s)} с — команда будет отклонена."]
    return "\n".join(lines)


def _approval_keyboard(rid: str, digest: str, allowed) -> Any:
    """Inline buttons for the request. ``session``/``always`` only appear
    when the host put them in allowed_choices; deny is always offered."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    def cb(choice: str) -> str:
        return f"tgp:a:{choice}:{rid}:{digest[:8]}"

    rows = [[
        InlineKeyboardButton("✅ Одобрить", callback_data=cb("once")),
        InlineKeyboardButton("❌ Отклонить", callback_data=cb("deny")),
    ]]
    extra = []
    if "session" in allowed:
        extra.append(InlineKeyboardButton("✅▸ Сессия",
                                          callback_data=cb("session")))
    if "always" in allowed:
        extra.append(InlineKeyboardButton("✅✅ Всегда",
                                          callback_data=cb("always")))
    if extra:
        rows.append(extra)
    return InlineKeyboardMarkup(rows)


def _approval_present(request: Any) -> Any:
    """ApprovalRequest -> ApprovalDecision: ask in Telegram, wait for the tap.

    Fail-closed everywhere: no wiring, a failed send or a timeout returns
    ``request.respond("deny")`` — silence is never consent. Sync on the
    host's bounded daemon worker thread: one Event.wait, no polling, no
    extra threads. Two concurrent approvals are two independent waiters;
    approvals_map mutations share _CWD_LOCK with the other state writers.
    """
    try:
        timeout_s = min(max(float(getattr(request, "timeout_seconds", 0) or 0),
                            1.0), _APPROVAL_MAX_WAIT_S)
        deadline = time.monotonic() + timeout_s
        rid = str(getattr(request, "request_id", "") or "")
        digest = str(getattr(request, "digest", "") or "")
        allowed = tuple(getattr(request, "allowed_choices", ()) or ())
        if not rid or not digest:
            return request.respond("deny")
        _approval_prune(time.time())

        chat_id, thread_id = _approval_target()
        kwargs: Dict[str, Any] = {
            "chat_id": str(chat_id),
            "text": _approval_message(request, timeout_s),
            "reply_markup": _approval_keyboard(rid, digest, allowed),
        }
        if thread_id:
            kwargs["message_thread_id"] = thread_id

        # Register waiter + map BEFORE sending: once the buttons exist every
        # tap finds a live entry; a failed send drops both in ``finally``.
        entry: Dict[str, Any] = {
            "digest": digest, "allowed": [c for c in allowed],
            "chat_id": str(chat_id), "thread_id": thread_id,
            "message_id": None, "expires": time.time() + timeout_s,
        }
        waiter: Dict[str, Any] = {
            "event": threading.Event(), "request": request, "choice": None}
        with _CWD_LOCK:
            state = _load_state()
            state.setdefault(_APPROVALS_KEY, {})[rid] = entry
            _save_state(state)
        _APPROVAL_WAITERS[rid] = waiter
        try:
            message = _tg_send_sync(kwargs, wait_s=min(15.0, timeout_s))
            if message is None:
                return request.respond("deny")
            message_id = getattr(message, "message_id", None)
            if message_id is not None:
                with _CWD_LOCK:
                    state = _load_state()
                    live = (state.get(_APPROVALS_KEY) or {}).get(rid)
                    if isinstance(live, dict):
                        live["message_id"] = message_id
                        _save_state(state)

            remaining = deadline - time.monotonic()
            if remaining > 0 and waiter["event"].wait(remaining) \
                    and waiter.get("choice"):
                return request.respond(str(waiter["choice"]))
            # No (valid) tap before the deadline: withdraw the prompt and
            # fail closed — the host also denies on its own timeout.
            _tg_edit_best_effort(
                chat_id, message_id,
                "⌛️ Время истекло — команда отклонена (fail-closed).")
            return request.respond("deny")
        finally:
            _APPROVAL_WAITERS.pop(rid, None)
            _approval_drop(rid)
    except Exception:
        logger.warning("tg-projects: approval transport failed", exc_info=True)
        try:
            return request.respond("deny")
        except Exception:
            return None


async def _handle_approval_callback(query: Any, m) -> None:
    """Resolve one ``tgp:a:<choice>:<rid>:<digest8>`` tap.

    Validates against approvals_map (live request, digest prefix match, not
    expired, choice allowed) before waking the waiter. Anything stale or
    mismatched is answered in place and never resolves the request.
    """
    choice, rid, digest8 = m.group(1), m.group(2), m.group(3)
    entry = None
    try:
        with _CWD_LOCK:
            amap = _load_state().get(_APPROVALS_KEY) or {}
            raw = amap.get(rid)
            entry = dict(raw) if isinstance(raw, dict) else None
    except Exception:
        entry = None

    if entry is None:
        with _suppress(Exception):
            await query.edit_message_text(
                "⚠️ Запрос устарел, уже отвечен или перезапущен — "
                "команда отклонена.")
        with _suppress(Exception):
            await query.answer()
        return

    digest = str(entry.get("digest") or "")
    if not digest.startswith(str(digest8 or "")):
        # The tap does not bind to this exact request (replayed/forwarded
        # button): refuse without resolving anything.
        with _suppress(Exception):
            await query.answer("⚠️ Кнопка не совпадает с запросом — "
                               "ответ не принят.")
        return
    if float(entry.get("expires") or 0) < time.time():
        _approval_drop(rid)
        with _suppress(Exception):
            await query.edit_message_text(
                "⌛️ Время истекло — команда отклонена (fail-closed).")
        with _suppress(Exception):
            await query.answer()
        return
    if choice not in (entry.get("allowed") or ()):
        with _suppress(Exception):
            await query.answer("⚠️ Этот выбор недоступен для данного запроса.")
        return
    waiter = _APPROVAL_WAITERS.get(rid)
    if waiter is None:
        _approval_drop(rid)
        with _suppress(Exception):
            await query.edit_message_text(
                "⚠️ Запрос уже закрыт — команда отклонена.")
        with _suppress(Exception):
            await query.answer()
        return

    waiter["choice"] = choice
    waiter["event"].set()
    with _suppress(Exception):
        await query.edit_message_text(_APPROVAL_CHOICE_TEXT.get(choice, choice))
    with _suppress(Exception):
        await query.answer()


# ------------------------------------------------------------------ project wizard
# /menu → [📁 Проект] → [➕ Новый] (tgp:pw:start) opens a two-step dialog:
# "Название проекта?" → free text → "Путь к каталогу (абсолютный)?" → free
# text → hermes_cli.projects_db.create_project. Free-text answers are
# captured by the ``pre_gateway_dispatch`` hook BEFORE dispatch — it returns
# {"action": "skip"} (gateway/run_inbound.py drops the event), so a dialog
# answer NEVER becomes an LLM turn. The hook is registered BEFORE the sync
# hook; it returns None while no live wizard state exists, so seamless sync
# still runs for everyone else.
#
# State: state.json["wizard"]["<chat_id>:<thread_id>"] = {"step", "name",
# "project_id", "updated_at"} (+ "user_id" — the auth-gated button tapper;
# the hook runs BEFORE gateway auth, so the sender is matched against it).
# A state older than WIZARD_TTL_S is ignored, NOT deleted (the text goes to
# the session as usual). [❌ Отмена] (tgp:pw:cancel) and the /menu handler
# (via _wizard_reset_chat) drop it explicitly. Every wizard message is a
# fresh send_message into the same chat/topic — no edits, the dialog is
# alive. After success the user gets [📂 Выбрать проект] (tgp:pb:proj —
# the panel callback). Topic bindings are NEVER set implicitly here:
# binding stays an explicit action.
WIZARD_TTL_S = 600
_WIZARD_NAME_MAX = 100
_WIZARD_START_CB = "tgp:pw:start"
_WIZARD_CANCEL_CB = "tgp:pw:cancel"
_WIZARD_SELECT_CB = "tgp:pb:proj"
# A command-looking token (``/menu``, ``/stop``) passes through so the user
# can always reset/abort via commands — but a PATH answer also starts with
# "/", so only slash-free single tokens count as commands (``/mnt/disk/x``
# is an answer, ``/stop`` is a command; ``/home`` alone is an accepted miss).
_WIZARD_CMD_RE = re.compile(r"^/[^/\s]+$")

# Cyrillic → latin transliteration for slug candidates. The core's
# projects_db._slugify strips every non-[a-z0-9] char, so a Cyrillic name
# would collapse to "project" — the wizard derives the slug itself and
# passes it to create_project (which still normalizes + uniquifies).
_WIZARD_TRANSLIT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
    "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "h", "ц": "c", "ч": "ch", "ш": "sh", "щ": "sch", "ъ": "",
    "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}


def _wizard_slug(name: str) -> str:
    """Transliterate *name* into a projects.db slug candidate.

    Per-char transliteration, lowercase, non-alphanumerics collapsed to
    "-", capped at 64 chars, never empty ("project" fallback).
    """
    s = str(name or "").strip().lower()
    s = "".join(_WIZARD_TRANSLIT.get(ch, ch) for ch in s)
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-_")
    return s[:64].strip("-_") or "project"


def _wizard_key(chat_id, thread_id) -> str:
    """The state.json["wizard"] key: ``<chat_id>:<thread_id>`` ('' = no topic)."""
    return f"{str(chat_id or '').strip()}:{str(thread_id or '').strip()}"


def _wizard_get(key: str) -> Optional[Dict[str, Any]]:
    """A copy of the chat's wizard state, or None when absent."""
    try:
        entry = (_load_state().get("wizard") or {}).get(key)
    except Exception:
        return None
    return dict(entry) if isinstance(entry, dict) else None


def _wizard_put(key: str, entry: Dict[str, Any]) -> None:
    with _CWD_LOCK:
        state = _load_state()
        state.setdefault("wizard", {})[key] = entry
        _save_state(state)


def _wizard_pop(key: str) -> None:
    with _CWD_LOCK:
        state = _load_state()
        wizard = state.get("wizard") or {}
        if key in wizard:
            wizard.pop(key, None)
            if not wizard:
                state.pop("wizard", None)
            _save_state(state)


def _wizard_reset_chat(chat_id, thread_id: Any = None) -> None:
    """Drop the chat's wizard state (every topic of that chat).

    The /menu handler calls this so a freshly opened menu never resumes a
    half-finished dialog. ``thread_id`` is accepted for call-site symmetry
    and ignored — the reset covers all the chat's topics.
    """
    cid = str(chat_id or "").strip()
    if not cid:
        return
    with _CWD_LOCK:
        state = _load_state()
        wizard = state.get("wizard") or {}
        dead = [k for k in wizard if str(k).split(":", 1)[0] == cid]
        for k in dead:
            wizard.pop(k, None)
        if dead:
            if not wizard:
                state.pop("wizard", None)
            _save_state(state)


def _wizard_cancel_keyboard():
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌ Отмена", callback_data=_WIZARD_CANCEL_CB)]])


def _wizard_done_keyboard():
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("📂 Выбрать проект", callback_data=_WIZARD_SELECT_CB)]])


async def _wizard_send(chat_id: str, thread_id: str, text: str, keyboard=None) -> bool:
    """Send a wizard message into its chat/topic; never raises.

    Prefers the wired PTB bot (inline keyboards ride along), degrades to the
    adapter's plain send, then to a log line. The thread-id convention
    matches the /projects keyboard (int when numeric).
    """
    chat_id = str(chat_id or "").strip()
    if not chat_id:
        return False
    thread_id = str(thread_id or "").strip()
    thread_kwargs: Dict[str, Any] = {}
    if thread_id:
        thread_kwargs["message_thread_id"] = int(thread_id) if thread_id.isdigit() else thread_id
    bot = getattr(_NATIVE, "bot", None) if _NATIVE is not None else None
    if bot is not None:
        try:
            kwargs: Dict[str, Any] = {"chat_id": chat_id, "text": text}
            if keyboard is not None:
                kwargs["reply_markup"] = keyboard
            await bot.send_message(**{**kwargs, **thread_kwargs})
            return True
        except Exception:
            logger.warning("tg-projects wizard: bot send failed, trying adapter", exc_info=True)
    if _ADAPTER is not None:
        try:
            await _ADAPTER.send(chat_id, text,
                                metadata={"thread_id": thread_id} if thread_id else None)
            return True
        except Exception:
            logger.warning("tg-projects wizard: adapter send failed", exc_info=True)
    return False


def _wizard_validate_name(name: str) -> Optional[str]:
    """Error text for a bad project name, or None when valid."""
    name = str(name or "").strip()
    if not name:
        return "❌ Название не может быть пустым — пришлите название ещё раз."
    if len(name) > _WIZARD_NAME_MAX:
        return (f"❌ Название длиннее {_WIZARD_NAME_MAX} символов ({len(name)}). "
                "Сократите и пришлите ещё раз.")
    return None


def _wizard_path_error(name: str, path: str) -> Optional[str]:
    """Error text for a bad path (shape, existence, duplicates), or None.

    Both duplicate kinds are checked against projects.db up front: the slug
    the wizard would generate and the primary_path itself (the same check
    create_project applies via find_by_primary_path — checked first here so
    the user gets a readable message instead of a raised ValueError).
    """
    path = str(path or "").strip()
    if not path.startswith("/"):
        return f"❌ Путь «{path}» не абсолютный — нужен путь от корня, например /mnt/mydisk/sd2."
    if not os.path.isdir(path):
        return (f"❌ Каталог {path} не существует. Мастер не создаёт каталоги — "
                "укажите существующий абсолютный путь.")
    slug = _wizard_slug(name)
    try:
        projects_db = _import_hermes_module("hermes_cli.projects_db")
        with projects_db.connect_closing() as conn:
            for proj in projects_db.list_projects(conn, include_archived=True):
                if getattr(proj, "slug", None) == slug:
                    return (f"❌ Slug «{slug}» уже занят проектом "
                            f"«{getattr(proj, 'name', '?')}». Придумайте другое название.")
            existing = projects_db.find_by_primary_path(conn, path)
            if existing is not None:
                return (f"❌ Каталог {path} уже принадлежит проекту "
                        f"«{getattr(existing, 'name', '?')}» [{getattr(existing, 'slug', '')}]. "
                        "Переключитесь на него вместо создания дубликата.")
    except Exception as exc:
        return f"❌ Не удалось проверить projects.db: {exc}. Пришлите путь ещё раз."
    return None


def _wizard_create(name: str, path: str) -> tuple:
    """``(project_id, slug, error_text)`` — success or failure, never raises."""
    slug = _wizard_slug(name)
    try:
        projects_db = _import_hermes_module("hermes_cli.projects_db")
        with projects_db.connect_closing() as conn:
            pid = projects_db.create_project(conn, name=name, slug=slug, primary_path=path)
        return pid, slug, None
    except ValueError as exc:
        return None, slug, f"❌ projects.db: {exc}"
    except Exception as exc:
        return None, slug, f"❌ Не удалось создать проект: {exc}"


async def _wizard_on_button(query: Any, data: str) -> None:
    """``tgp:pw:*`` button taps: start the dialog or cancel it.

    Auth already happened in _tg_on_button (the same _callback_authorized
    gate as every other project button). The tapping user's id is recorded
    in the state so the dispatch hook only accepts THAT sender's free text.
    """
    msg = getattr(query, "message", None)
    chat = getattr(msg, "chat", None) if msg is not None else None
    chat_id = str(getattr(chat, "id", "") or "")
    raw_thread = getattr(msg, "message_thread_id", None) if msg is not None else None
    thread_id = str(raw_thread) if raw_thread is not None else ""
    user_id = str(getattr(getattr(query, "from_user", None), "id", "") or "")
    key = _wizard_key(chat_id, thread_id)

    if data == _WIZARD_START_CB:
        _wizard_put(key, {
            "step": "name",
            "name": None,
            "project_id": None,
            "user_id": user_id,
            "updated_at": int(time.time()),
        })
        await _wizard_send(chat_id, thread_id, "Название проекта?",
                           keyboard=_wizard_cancel_keyboard())
        with _suppress(Exception):
            await query.answer()
        return

    if data == _WIZARD_CANCEL_CB:
        _wizard_pop(key)
        await _wizard_send(chat_id, thread_id,
                           "❌ Создание проекта отменено. /menu → 📁 Проект, чтобы начать заново.")
        with _suppress(Exception):
            await query.answer()
        return

    with _suppress(Exception):
        await query.answer()  # unknown tgp:pw: payload — acknowledge, ignore


async def _on_wizard_pre_gateway_dispatch(event: Any, gateway: Any = None,
                                          session_store: Any = None,
                                          **kwargs) -> Optional[Dict[str, Any]]:
    """``pre_gateway_dispatch`` — the project-wizard free-text gate.

    Returns ``{"action": "skip", "reason": "project_wizard"}`` ONLY when the
    text was consumed as a dialog answer; None otherwise (fail-open — the
    sync hook and normal dispatch still run). Registered BEFORE the sync
    hook: a live wizard owns the lane's free text. The hook runs
    BEFORE gateway auth, so the sender is matched against the user id that
    started the wizard via an auth-gated button tap; anyone else's text
    passes through untouched.
    """
    try:
        return await _wizard_hook_impl(event)
    except Exception:
        logger.warning("tg-projects wizard: pre_gateway_dispatch failed", exc_info=True)
        return None


async def _wizard_hook_impl(event: Any) -> Optional[Dict[str, Any]]:
    source = getattr(event, "source", None)
    if source is None:
        return None
    if str(getattr(getattr(source, "platform", None), "value", "") or "") != "telegram":
        return None
    if bool(getattr(event, "internal", False)):
        return None  # synthetic re-dispatches never feed the wizard
    text = str(getattr(event, "text", "") or "").strip()
    if not text or _WIZARD_CMD_RE.match(text):
        return None  # commands flow through (auth, /menu, /stop keep working)
    chat_id = str(getattr(source, "chat_id", "") or "").strip()
    if not chat_id:
        return None
    thread_id = str(getattr(source, "thread_id", "") or "").strip()
    key = _wizard_key(chat_id, thread_id)
    entry = _wizard_get(key)
    if entry is None:
        return None
    if int(entry.get("updated_at") or 0) + WIZARD_TTL_S < time.time():
        return None  # stale dialog: the text goes to the session as usual
    wizard_user = str(entry.get("user_id") or "").strip()
    sender = str(getattr(source, "user_id", "") or "").strip()
    if wizard_user and sender and wizard_user != sender:
        return None  # a different sender's text is not a dialog answer

    step = str(entry.get("step") or "").strip()

    if step == "name":
        name = text
        err = _wizard_validate_name(name)
        if err is not None:
            entry["updated_at"] = int(time.time())
            _wizard_put(key, entry)
            await _wizard_send(chat_id, thread_id,
                               f"{err}\n\nНазвание проекта?",
                               keyboard=_wizard_cancel_keyboard())
            return {"action": "skip", "reason": "project_wizard"}
        entry["name"] = name
        entry["step"] = "path"
        entry["updated_at"] = int(time.time())
        _wizard_put(key, entry)
        await _wizard_send(chat_id, thread_id,
                           f"✓ Название: {name}\n\nПуть к каталогу (абсолютный)?",
                           keyboard=_wizard_cancel_keyboard())
        return {"action": "skip", "reason": "project_wizard"}

    if step == "path":
        name = str(entry.get("name") or "").strip()
        path = text.strip().strip('"')
        err = _wizard_validate_name(name)
        if err is None:
            err = _wizard_path_error(name, path)
        if err is None:
            pid, slug, err = _wizard_create(name, path)
        if err is not None:
            entry["updated_at"] = int(time.time())
            _wizard_put(key, entry)
            await _wizard_send(chat_id, thread_id,
                               f"{err}\n\nПуть к каталогу (абсолютный)?",
                               keyboard=_wizard_cancel_keyboard())
            return {"action": "skip", "reason": "project_wizard"}
        _wizard_pop(key)
        await _wizard_send(chat_id, thread_id,
                           f"✅ Проект создан: {name} ({slug}, {path})",
                           keyboard=_wizard_done_keyboard())
        return {"action": "skip", "reason": "project_wizard"}

    _wizard_pop(key)  # corrupt step — heal by dropping the state, fail open
    return None


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
    ctx.register_command(
        "menu",
        handler=_menu_command,
        description="Панель топика — закреплённое сообщение с кнопками",
        args_hint="",
    )
    ctx.register_hook("on_session_start", _on_session_start)
    # Fallback for sessions on_session_start never sees (a topic's first
    # session without a pin, /resume'd sessions): applies the topic->project
    # cwd on the first turn instead. Idempotent via _CWD_APPLIED.
    ctx.register_hook("pre_llm_call", _on_pre_llm_call)
    # Lazy prune of stale topic bindings/panels at start; never fatal.
    try:
        _prune_stale_state()
    except Exception:
        logger.warning("tg-projects: startup state prune failed", exc_info=True)
    # Topics off: migrate each chat's newest topic binding to the flat key.
    try:
        _migrate_flat_bindings()
    except Exception:
        logger.warning("tg-projects: flat binding migration failed", exc_info=True)
    # Project wizard (/menu → [➕ Новый]): consumes free-text answers via
    # pre_gateway_dispatch BEFORE the text reaches the session. Registered
    # BEFORE the sync hook so a live wizard owns the lane's free text;
    # returns None while inactive, so seamless sync still runs.
    try:
        ctx.register_hook("pre_gateway_dispatch", _on_wizard_pre_gateway_dispatch)
        logger.info("tg-projects: wizard pre_gateway_dispatch hook registered")
    except Exception:
        logger.warning("tg-projects: wizard hook registration failed", exc_info=True)
    # Seamless topic sync: free text in a bound topic follows the binding's
    # session (switch on drift); unbound topics get a "choose a project"
    # reply + the topic panel. Never blocks a bound topic's message.
    try:
        ctx.register_hook("pre_gateway_dispatch", _on_pre_gateway_dispatch_sync)
        logger.info("tg-projects: pre_gateway_dispatch sync hook registered")
    except Exception:
        logger.warning("tg-projects: sync hook registration failed; "
                       "topic routing falls back to the core heal", exc_info=True)
    # Human approval transport (tg-topics): dangerous-command prompts as
    # Telegram inline buttons. Inactive until security.approval.transport:
    # tg-topics selects it in config.yaml; fail-closed by contract.
    try:
        ctx.register_approval_transport("tg-topics", _approval_present)
        logger.info("tg-projects: approval transport registered (tg-topics)")
    except Exception:
        logger.warning("tg-projects: approval transport registration failed",
                       exc_info=True)
    try:
        ctx.register_platform_handler("telegram", _telegram_wire)
    except Exception:
        logger.warning("tg-projects: platform handler registration unavailable; "
                       "text commands still work", exc_info=True)
