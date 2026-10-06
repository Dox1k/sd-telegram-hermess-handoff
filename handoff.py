"""tg-projects — cross-device session handoff (Telegram ↔ desktop).

The Telegram gateway and the desktop app are SEPARATE processes (the desktop
spawns its own ``hermes serve`` backend). The only state they share is
``~/.hermes/state.db``. Two mechanisms bridge the gap:

* ``session_turn_leases`` (state.db, core-owned) — a cross-process write fence
  taken for every running turn. The holder string carries the platform
  (``pid=<pid>:turn=<id>:<...>:platform=desktop|telegram|subagent``), so a
  FRESH lease held by a foreign platform means "a turn is running on the
  other device right now".
* ``device_sessions`` claims (plugin-owned sqlite) — conversational ownership
  label for the menus ("📱 активна с телефона").

Handoff flow (user writes from Telegram while the desktop turn runs):
1. ``pre_gateway_dispatch`` sees a fresh foreign lease → stores the message
   as *pending* in state.json, sends a Yes/No prompt, returns skip.
2. "Yes" (callback ``tgp:ho:<session_id>:y``):
   - steal the foreign lease (DELETE the row; the desktop turn's next
     transcript write raises SessionTurnLeaseLostError and the turn dies —
     this is the only cross-process stop available without an RPC);
   - claim the session for telegram (device_sessions);
   - point this chat's lane at the session via
     ``SessionStore.switch_session`` (CAS-safe);
   - append a handoff event for the desktop notifier;
   - re-dispatch the pending message as a normal gateway turn.
3. "No" drops the pending message; nothing else changes.

Every helper fails OPEN: any error in ownership/lease logic lets the message
through unchanged — the handoff UX must never break normal chatting.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import re
import secrets
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger("hermes_plugins.tg_projects.handoff")

_PLUG_DIR = Path(__file__).resolve().parent
_STATE_FILE = _PLUG_DIR / "state.json"
_EVENTS_FILE = _PLUG_DIR / "handoff_events.jsonl"

# Pending prompts older than this are dropped on read (stale prompt).
PENDING_TTL_S = 600
# How long a "busy" prompt's Yes may act after the foreign lease disappeared
# on its own (turn finished meanwhile) — still fine to just continue.
LEASE_GRACE_S = 30
# Lifetime of a pending handoff token (a "Да/Нет" tap that never lands).
_TOKEN_TTL_S = 3600

_HOLDER_PLATFORM_RE = re.compile(r"platform=([a-z_]+)")
# 8-hex-char opaque token, so ``tgp:ho:<token>:(y|n)`` is always 9+8 = 17 bytes
# and comfortably inside Telegram's 64-byte callback_data cap. The real
# session_id / session_key live in state.json under ``handoff_tokens``.
_CB_HO_RE = re.compile(r"^tgp:ho:([a-f0-9]{8}):(y|n)$")


def _load_sibling(name: str):
    """Import a plugin-dir module whether loaded as package or by file path."""
    pkg = __package__ or ""
    if pkg:
        try:
            return __import__(f"{pkg}.{name}", fromlist=["*"])
        except ImportError:
            pass
    mod_name = f"tg_projects_{name}"
    cached = sys.modules.get(mod_name)
    if cached is not None:
        return cached
    spec = importlib.util.spec_from_file_location(mod_name, str(_PLUG_DIR / f"{name}.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)
    return module


device_sessions = _load_sibling("device_sessions")


# --------------------------------------------------------------------- helpers
def _hermes_home() -> Path:
    override = os.environ.get("HERMES_HOME")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".hermes"


def _load_state() -> dict:
    try:
        if _STATE_FILE.exists():
            data = json.loads(_STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        logger.warning("tg-projects handoff: state.json unreadable", exc_info=True)
    return {}


def _save_state(state: dict) -> None:
    _PLUG_DIR.mkdir(parents=True, exist_ok=True)
    tmp = _STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, _STATE_FILE)


# ------------------------------------------------------------ handoff tokens
def _make_token() -> str:
    """Fresh 8-char hex token for callback_data (< 64-byte Telegram cap)."""
    return secrets.token_hex(4)  # 8 hex chars = 32 bits


def register_token(session_id: str, session_key: str) -> str:
    """Store a new ``token -> (session_id, session_key)`` mapping; return the token.

    Returns an empty string when *session_id* is unusable — callers must
    treat that as "cannot register" and fall back to fail-open.
    """
    sid = str(session_id or "").strip()
    if not sid:
        return ""
    token = _make_token()
    state = _load_state()
    now = int(time.time())
    tokens = state.get("handoff_tokens") or {}
    # Prune entries older than _TOKEN_TTL_S so the bucket cannot grow
    # unboundedly across many parked-but-never-answered prompts.
    tokens = {t: e for t, e in tokens.items()
              if isinstance(e, dict) and int(e.get("ts") or 0) + _TOKEN_TTL_S > now}
    tokens[token] = {
        "session_id": sid,
        "session_key": str(session_key or ""),
        "ts": now,
    }
    state["handoff_tokens"] = tokens
    _save_state(state)
    return token


def consume_token(token: str) -> Optional[Tuple[str, str]]:
    """Return ``(session_id, session_key)`` for a live *token* and delete it.

    The row is consumed on first read so a second tap on the same button
    (double-click, or a stale keyboard in another chat) yields None and
    hits the "expired" reply in the callback handler.
    """
    tok = str(token or "").strip()
    if not tok:
        return None
    state = _load_state()
    tokens = state.get("handoff_tokens") or {}
    entry = tokens.pop(tok, None)
    if entry is not None:
        if not tokens:
            state.pop("handoff_tokens", None)
        _save_state(state)
    if not isinstance(entry, dict):
        return None
    if int(entry.get("ts") or 0) + _TOKEN_TTL_S < time.time():
        return None  # stale — user sat on the prompt too long
    return (str(entry.get("session_id") or ""), str(entry.get("session_key") or ""))


def _state_db_path() -> Path:
    return _hermes_home() / "state.db"


def _open_ro() -> Optional[sqlite3.Connection]:
    path = _state_db_path()
    if not path.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


# ------------------------------------------------------------- lease inspection
def parse_holder_platform(holder: str) -> str:
    """The platform token of a lease holder string ('' when absent)."""
    m = _HOLDER_PLATFORM_RE.search(str(holder or ""))
    return (m.group(1) if m else "").strip()


def foreign_running_lease(state_conn, session_id: str) -> Optional[Dict[str, Any]]:
    """A fresh lease for *session_id* held by a NON-telegram platform, or None.

    Only an exact ``conversation_id = session_id`` match is checked (v1): the
    conversation root usually IS the session id; compression-lineage roots are
    an accepted miss (the claim layer still labels ownership). Errors and a
    missing table return None — fail-open.
    """
    sid = str(session_id or "").strip()
    if not sid or state_conn is None:
        return None
    try:
        row = state_conn.execute(
            "SELECT holder, expires_at FROM session_turn_leases WHERE conversation_id = ?",
            (sid,),
        ).fetchone()
    except sqlite3.Error:
        return None
    if row is None:
        return None
    expires_at = float(row["expires_at"] or 0)
    if expires_at <= time.time() - LEASE_GRACE_S:
        return None  # expired (grace for clock skew)
    platform = parse_holder_platform(row["holder"])
    if platform == "telegram":
        return None  # our own surface
    return {
        "holder": str(row["holder"]),
        "platform": platform or "unknown",
        "expires_at": expires_at,
    }


def steal_lease(session_id: str) -> bool:
    """Delete the foreign turn lease so the other device's turn dies on its
    next transcript write (SessionTurnLeaseLostError).

    Only rows whose holder is NOT platform=telegram are deleted — our own
    gateway turns keep their leases. Write access is a one-shot connection
    under WAL; a busy db retries via timeout once. Never raises.
    """
    sid = str(session_id or "").strip()
    path = _state_db_path()
    if not sid or not path.exists():
        return False
    conn = None
    try:
        conn = sqlite3.connect(str(path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT holder FROM session_turn_leases WHERE conversation_id = ?", (sid,)
        ).fetchone()
        if row is None:
            return False
        platform = parse_holder_platform(row["holder"])
        if platform == "telegram":
            return False  # never steal our own surface's lease
        conn.execute("DELETE FROM session_turn_leases WHERE conversation_id = ?", (sid,))
        conn.commit()
        logger.info("tg-projects handoff: stole lease for %s (was %s)", sid, platform)
        return True
    except sqlite3.Error:
        logger.warning("tg-projects handoff: steal_lease(%s) failed", sid, exc_info=True)
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except sqlite3.Error:
                pass


# ------------------------------------------------------------ pending messages
def store_pending(session_key: str, session_id: str, text: str,
                  source: Dict[str, Any]) -> bool:
    """Remember the intercepted message so "Yes" can re-dispatch it verbatim."""
    sk = str(session_key or "").strip()
    if not sk or not str(text or "").strip():
        return False
    state = _load_state()
    state.setdefault("handoff_pending", {})[sk] = {
        "session_id": str(session_id or ""),
        "text": str(text),
        "source": {k: str(v or "") for k, v in (source or {}).items()
                   if k in ("chat_id", "thread_id", "user_id", "user_name", "message_id")},
        "ts": int(time.time()),
    }
    _save_state(state)
    return True


def pop_pending(session_key: str) -> Optional[Dict[str, Any]]:
    """Take (and remove) the lane's pending message; None when absent/stale."""
    sk = str(session_key or "").strip()
    if not sk:
        return None
    state = _load_state()
    pending = state.get("handoff_pending") or {}
    entry = pending.pop(sk, None)
    if entry is not None:
        _save_state(state)
    if not isinstance(entry, dict):
        return None
    if int(entry.get("ts") or 0) + PENDING_TTL_S < time.time():
        return None  # stale — the user waited too long to answer
    return entry


def drop_pending(session_key: str) -> None:
    sk = str(session_key or "").strip()
    if not sk:
        return
    state = _load_state()
    pending = state.get("handoff_pending") or {}
    if sk in pending:
        pending.pop(sk, None)
        if not pending:
            state.pop("handoff_pending", None)
        _save_state(state)


# --------------------------------------------------------- desktop notification
def notify_desktop_handoff(session_id: str, from_device: str, to_device: str,
                           extra: Optional[Dict[str, Any]] = None) -> bool:
    """Append a JSON event the desktop notifier plugin polls.

    The desktop app runs in its own process; the only channel the gateway
    side can push through is the shared filesystem. Format: one JSON object
    per line with a monotonically increasing ts.
    """
    event = {
        "type": "session.handoff",
        "session_id": str(session_id or ""),
        "from_device": str(from_device or ""),
        "to_device": str(to_device or ""),
        "ts": time.time(),
    }
    if extra:
        event.update({k: v for k, v in extra.items() if k not in event})
    try:
        _PLUG_DIR.mkdir(parents=True, exist_ok=True)
        with _EVENTS_FILE.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, ensure_ascii=False) + "\n")
        return True
    except OSError:
        logger.warning("tg-projects handoff: desktop event append failed", exc_info=True)
        return False


# ------------------------------------------------------------------- the hook
def should_prompt(state_conn, session_id: str, current_device: str = "telegram"
                  ) -> Optional[Dict[str, Any]]:
    """The decision core: prompt when a foreign RUNNING lease exists.

    Kept separate from the hook so tests drive it without an event loop.
    """
    if str(current_device or "").strip().lower() != "telegram":
        return None  # v1: the hook only serves the Telegram side
    return foreign_running_lease(state_conn, session_id)


async def on_pre_gateway_dispatch(event, gateway, session_store, **kwargs):
    """``pre_gateway_dispatch`` hook — the busy-on-another-device gate.

    Returns ``{"action": "skip"}`` ONLY when the message was parked behind a
    Yes/No prompt; every other path returns None (allow) — fail-open.
    Commands pass through untouched (the user must be able to /stop or /new
    even while the desktop is busy).
    """
    try:
        return await _hook_impl(event, gateway, session_store)
    except Exception:
        logger.warning("tg-projects handoff: pre_gateway_dispatch failed", exc_info=True)
        return None  # fail-open


async def _hook_impl(event, gateway, session_store):
    source = getattr(event, "source", None)
    if source is None:
        return None
    if str(getattr(getattr(source, "platform", None), "value", "") or "") != "telegram":
        return None
    if bool(getattr(event, "internal", False)):
        return None  # synthetic re-dispatches must never re-prompt
    text = str(getattr(event, "text", "") or "").strip()
    if not text or text.startswith("/"):
        return None  # commands flow through the normal (interrupt-capable) path
    if str(getattr(source, "chat_type", "") or "") != "dm":
        return None  # group lanes: keep current behavior

    session_id = _resolve_lane_session_id(gateway, session_store, source)
    if not session_id:
        return None

    conn = _open_ro()
    try:
        lease = should_prompt(conn, session_id) if conn is not None else None
    finally:
        if conn is not None:
            try:
                conn.close()
            except sqlite3.Error:
                pass
    if lease is None:
        # Idle (or ours): mark phone-side ownership and let the turn run.
        device_sessions.claim(session_id, "telegram",
                              surface=str(getattr(source, "chat_id", "") or ""))
        return None

    # Busy on another device: park the message and ask for confirmation.
    sk = _generate_session_key(gateway, source)
    if not sk:
        return None  # cannot key the pending message — fail-open
    stored = store_pending(sk, session_id, text, {
        "chat_id": getattr(source, "chat_id", ""),
        "thread_id": getattr(source, "thread_id", "") or "",
        "user_id": getattr(source, "user_id", "") or "",
        "user_name": getattr(source, "user_name", "") or "",
        "message_id": str(getattr(event, "message_id", "") or ""),
    })
    if not stored:
        return None  # state.json unwritable — fail-open
    sent = await _send_prompt(source, session_id, sk, lease)
    if not sent:
        logger.error(
            "tg-projects handoff: prompt undeliverable for session %s — message "
            "parked in state.json but the user got no buttons; they must wait "
            "for the other device to finish or /stop.", session_id)
    return {"action": "skip", "reason": "busy_elsewhere"}


def _resolve_lane_session_id(gateway, session_store, source) -> str:
    """The session id this lane currently points at ('' when unresolvable)."""
    store = session_store if session_store is not None else getattr(gateway, "session_store", None)
    if store is None:
        return ""
    try:
        sk = _generate_session_key(gateway, source)
        if not sk:
            return ""
        entry = store.lookup_by_session_key(sk)
        return str(getattr(entry, "session_id", "") or "") if entry is not None else ""
    except Exception:
        logger.warning("tg-projects handoff: lane session resolve failed", exc_info=True)
        return ""


def _generate_session_key(gateway, source) -> str:
    try:
        gen = getattr(gateway, "_generate_session_key", None)
        if callable(gen):
            return str(gen(source) or "")
    except Exception:
        pass
    return ""


async def _send_prompt(source, session_id: str, session_key: str,
                       lease: Dict[str, Any]) -> bool:
    """Send the Yes/No prompt into the chat the message came from.

    Registers a short-lived token BEFORE the buttons go out — the callback
    data only carries the token (8 hex chars, so ``tgp:ho:<token>:(y|n)``
    is always ≤ 17 bytes and safely inside Telegram's 64-byte cap). The
    real ``session_id`` / ``session_key`` are looked up from
    ``state.json['handoff_tokens']`` when the user taps.

    Uses the PTB bot directly (the plugin's wired ``_NATIVE``) so the inline
    keyboard rides along; without a wired bot it degrades to
    ``adapter.send`` plain text with instructions.

    Returns True when at least one channel delivered the prompt; False when
    both failed — caller should log the "parked with no prompt" condition.
    """
    chat_id = getattr(source, "chat_id", None)
    if chat_id is None:
        logger.warning("tg-projects handoff: prompt skipped — no chat_id on source")
        return False
    token = register_token(session_id, session_key)
    if not token:
        logger.warning("tg-projects handoff: prompt skipped — token registration "
                       "failed for %s", session_id)
        return False

    platform_label = {"desktop": "ПК", "unknown": "другом устройстве"}.get(
        lease.get("platform", "unknown"), "другом устройстве")
    text = (
        f"⚠️ Сессия {session_id} сейчас выполняется на {platform_label}.\n"
        f"Остановить её там и продолжить здесь? Ваше сообщение сохранено и будет "
        f"отправлено после переключения."
    )

    thread_id = str(getattr(source, "thread_id", "") or "").strip()
    thread_kwargs: Dict[str, Any] = ({"message_thread_id": int(thread_id)}
                                     if thread_id.isdigit() else {})

    native = _get_native()
    if native is not None and getattr(native, "bot", None) is not None:
        try:
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Да, остановить и продолжить",
                                      callback_data=f"tgp:ho:{token}:y"),
                 InlineKeyboardButton("❌ Нет", callback_data=f"tgp:ho:{token}:n")],
            ])
            await native.bot.send_message(
                chat_id=chat_id, text=text, reply_markup=keyboard, **thread_kwargs)
            return True
        except Exception:
            logger.warning("tg-projects handoff: native prompt send failed, "
                           "trying adapter fallback", exc_info=True)

    adapter = _get_adapter()
    if adapter is not None:
        try:
            # adapter.send routes forum topics via metadata["thread_id"]
            # (telegram adapter._metadata_thread_id, adapter.py:1094).
            await adapter.send(str(chat_id),
                               text + "\n(Кнопки недоступны — нажмите на панель "
                                      "топика или ответьте 'Да' вручную.)",
                               metadata={"thread_id": thread_id} if thread_id else None)
            return True
        except Exception:
            logger.warning("tg-projects handoff: adapter fallback prompt failed",
                           exc_info=True)

    return False


def _get_native():
    """The PTB Application wired by the plugin's Telegram factory (or None).

    Standalone loading (tests, loader trick) has no package parent, so the
    plugin module is resolved by the same on-disk loader handoff itself uses.
    """
    mod = _plugin_main_module()
    return getattr(mod, "_NATIVE", None) if mod is not None else None


def _get_adapter():
    mod = _plugin_main_module()
    return getattr(mod, "_ADAPTER", None) if mod is not None else None


_PLUGIN_MAIN_CACHE: Dict[str, Any] = {"key": None, "mod": None}


def _plugin_main_module():
    """The loaded plugin __init__ (package form when imported normally)."""
    pkg = __package__ or ""
    if pkg:
        mod = sys.modules.get(pkg)
        if mod is not None:
            return mod
    key = str(_PLUG_DIR / "__init__.py")
    if _PLUGIN_MAIN_CACHE.get("key") == key:
        return _PLUGIN_MAIN_CACHE.get("mod")
    mod = sys.modules.get("tg_projects_under_test") or sys.modules.get(
        "tg_projects_sessions_under_test")
    if mod is None:
        spec = importlib.util.spec_from_file_location("tg_projects_main", key)
        mod = importlib.util.module_from_spec(spec)
        sys.modules.setdefault("tg_projects_main", mod)
        try:
            spec.loader.exec_module(mod)
        except Exception:
            return None
    _PLUGIN_MAIN_CACHE.update(key=key, mod=mod)
    return mod


# ----------------------------------------------------------------- handoff act
async def perform_handoff(session_key: str, session_id: str) -> Optional[Dict[str, Any]]:
    """The "Yes" action: stop the other device, take the session, return the
    pending message (for re-dispatch) or None when nothing was pending.

    Order matters: steal the lease first (the foreign turn must die before we
    write), then claim, then re-point the lane.
    """
    pending = pop_pending(session_key)
    if pending is None:
        return None
    if str(pending.get("session_id") or "") != str(session_id):
        # The lane moved between prompt and answer — do not act on stale data.
        return None

    from_device = "desktop"
    steal_lease(session_id)
    device_sessions.claim(session_id, "telegram")
    notify_desktop_handoff(session_id, from_device, "telegram",
                           extra={"text": str(pending.get("text") or "")[:200]})

    adapter = _get_adapter()
    runner = getattr(adapter, "gateway_runner", None) if adapter is not None else None
    if runner is not None and session_key:
        try:
            store = getattr(runner, "async_session_store", None)
            if store is not None:
                await store.switch_session(session_key, session_id)
        except Exception:
            logger.warning("tg-projects handoff: switch_session failed", exc_info=True)
    return pending


def handoff_callback_regex() -> "re.Pattern[str]":
    return _CB_HO_RE


def is_handoff_callback(data: str) -> bool:
    return bool(_CB_HO_RE.match(str(data or "")))
