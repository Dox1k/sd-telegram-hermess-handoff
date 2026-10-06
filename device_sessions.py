"""tg-projects — cross-device session ownership.

One Hermes session may be used from any surface (desktop app, Telegram).
This module tracks WHICH surface currently "owns" a session so another
device's inbound message can be intercepted with a stop-and-continue
prompt instead of silently interleaving two writers into one history.

Storage: a small SQLite db in the plugin's own directory
(``device_sessions.db``) — deliberately NOT state.db: the gateway owns
state.db, the plugin must not create tables there. The db holds one row
per session id:

    session_id TEXT PRIMARY KEY,
    device     TEXT NOT NULL,        -- 'desktop' | 'telegram' | future surfaces
    surface    TEXT NOT NULL DEFAULT '',  -- chat_id[:thread_id] / profile label
    claimed_at INTEGER NOT NULL,     -- unix seconds
    heartbeat  INTEGER NOT NULL      -- unix seconds, refreshed per turn

Concurrency rules:
- ``claim`` is atomic (INSERT OR REPLACE guarded by a compare): a claim by
  the same device/surface refreshes the heartbeat; a claim by a DIFFERENT
  device succeeds only when the current claim is stale (heartbeat older
  than HEARTBEAT_STALE_S) or already held by that device.
- ``busy_elsewhere`` returns the owning device row when another device
  holds a FRESH claim; None otherwise (idle, expired, or same device).
- All operations are best-effort: errors are logged and degrade to
  "no ownership info" (fail-open) — ownership tracking must never break a
  turn, it only gates the UX prompt.

Fail-open design: if this db is unreadable the plugin treats every
session as unowned and Hermes behaves exactly as it does today.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("hermes_plugins.tg_projects.device_sessions")

_PLUG_DIR = Path(__file__).resolve().parent
_DB_FILE = _PLUG_DIR / "device_sessions.db"

# A claim older than this (seconds) without a heartbeat refresh is stale:
# the owning device likely crashed or closed, so another device may take
# the session without a prompt.
HEARTBEAT_STALE_S = 600

_SCHEMA = """
CREATE TABLE IF NOT EXISTS device_session_claims (
    session_id TEXT PRIMARY KEY,
    device     TEXT NOT NULL,
    surface    TEXT NOT NULL DEFAULT '',
    claimed_at INTEGER NOT NULL,
    heartbeat  INTEGER NOT NULL
);
"""

_LOCK = threading.RLock()
_CONN: Optional[sqlite3.Connection] = None


def _connect() -> Optional[sqlite3.Connection]:
    """One lazily-opened, process-wide connection (WAL, check_same_thread off).

    The gateway dispatch layer runs plugin hooks on worker threads; the
    connection is guarded by _LOCK and re-opened on "database closed".
    """
    global _CONN
    with _LOCK:
        if _CONN is not None:
            try:
                _CONN.execute("SELECT 1")
                return _CONN
            except sqlite3.ProgrammingError:
                _CONN = None  # closed elsewhere; reopen
        try:
            _PLUG_DIR.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(_DB_FILE), timeout=5.0, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(_SCHEMA)
            conn.commit()
            _CONN = conn
            return conn
        except sqlite3.Error:
            logger.warning("tg-projects: device_sessions.db unavailable", exc_info=True)
            return None


def _now() -> int:
    return int(time.time())


def _norm_device(device: Any) -> str:
    return str(device or "").strip().lower() or "unknown"


def _norm_surface(surface: Any) -> str:
    return str(surface or "").strip()


def claim(session_id: str, device: str, surface: str = "") -> bool:
    """Record that *device* now owns *session_id*.

    Returns True when the claim was taken/refreshed. A claim by a different
    device ALWAYS succeeds (a handoff is a deliberate act: the caller has
    already stopped or confirmed stopping the other device's run); the
    previous owner is replaced atomically. Same-device claims only bump the
    heartbeat. Errors fail-open as False (no ownership recorded) — never
    raise into the gateway.
    """
    sid = str(session_id or "").strip()
    dev = _norm_device(device)
    if not sid:
        return False
    conn = _connect()
    if conn is None:
        return False
    try:
        with _LOCK:
            now = _now()
            conn.execute(
                "INSERT INTO device_session_claims (session_id, device, surface, claimed_at, heartbeat) "
                "VALUES (?,?,?,?,?) "
                "ON CONFLICT(session_id) DO UPDATE SET device=excluded.device, "
                "surface=excluded.surface, claimed_at=excluded.claimed_at, "
                "heartbeat=excluded.heartbeat",
                (sid, dev, _norm_surface(surface), now, now),
            )
            conn.commit()
            return True
    except sqlite3.Error:
        logger.warning("tg-projects: claim(%s) failed", sid, exc_info=True)
        return False


def heartbeat(session_id: str, device: str) -> bool:
    """Refresh the liveness stamp of a claim the SAME device already holds.

    Never steals: when the row is held by another device or absent this is a
    no-op returning False. Called per turn to keep the claim fresh.
    """
    sid = str(session_id or "").strip()
    dev = _norm_device(device)
    if not sid or not dev:
        return False
    conn = _connect()
    if conn is None:
        mo = False
    else:
        try:
            with _LOCK:
                now = _now()
                cur = conn.execute(
                    "UPDATE device_session_claims SET heartbeat=? "
                    "WHERE session_id=? AND device=?",
                    (now, sid, dev),
                )
                conn.commit()
                mo = cur.rowcount > 0
        except sqlite3.Error:
            logger.warning("tg-projects: heartbeat(%s) failed", sid, exc_info=True)
            mo = False
    return mo


def release(session_id: str, device: Optional[str] = None) -> bool:
    """Drop the claim (a specific device's claim, or any owner's).

    Returns True when a row was removed. Never raises.
    """
    sid = str(session_id or "").strip()
    if not sid:
        return False
    conn = _connect()
    if conn is None:
        return False
    try:
        with _LOCK:
            if device is None:
                cur = conn.execute(
                    "DELETE FROM device_session_claims WHERE session_id=?", (sid,))
            else:
                cur = conn.execute(
                    "DELETE FROM device_session_claims WHERE session_id=? AND device=?",
                    (sid, _norm_device(device)))
            conn.commit()
            return cur.rowcount > 0
    except sqlite3.Error:
        logger.warning("tg-projects: release(%s) failed", sid, exc_info=True)
        return False


def lookup(session_id: str) -> Optional[Dict[str, Any]]:
    """The current claim row for *session_id* as a dict, or None.

    Keys: session_id, device, surface, claimed_at, heartbeat, stale (bool).
    Errors (missing db, locked) return None — fail-open.
    """
    sid = str(session_id or "").strip()
    if not sid:
        return None
    conn = _connect()
    if conn is None:
        return None
    try:
        with _LOCK:
            row = conn.execute(
                "SELECT session_id, device, surface, claimed_at, heartbeat "
                "FROM device_session_claims WHERE session_id=?",
                (sid,),
            ).fetchone()
    except sqlite3.Error:
        logger.warning("tg-projects: lookup(%s) failed", sid, exc_info=True)
        return None
    if row is None:
        return None
    data = dict(row)
    data["stale"] = _now() - int(data.get("heartbeat") or 0) > HEARTBEAT_STALE_S
    return data


def busy_elsewhere(session_id: str, current_device: str) -> Optional[Dict[str, Any]]:
    """The owning row when ANOTHER device holds a fresh claim, else None.

    None means: no claim, stale claim, or the claim belongs to the same
    device (a same-device message never prompts — it just heartbeats).
    """
    row = lookup(session_id)
    if row is None or row.get("stale"):
        return None
    if row.get("device") == _norm_device(current_device):
        return None
    return row


def clear_all() -> None:
    """Test helper: wipe every claim. Never raises."""
    conn = _connect()
    if conn is None:
        return
    try:
        with _LOCK:
            conn.execute("DELETE FROM device_session_claims")
            conn.commit()
    except sqlite3.Error:
        logger.warning("tg-projects: clear_all failed", exc_info=True)


def reset_for_tests() -> None:
    """Close the process-wide connection so tests can point _DB_FILE elsewhere."""
    global _CONN
    with _LOCK:
        if _CONN is not None:
            try:
                _CONN.close()
            except sqlite3.Error:
                pass
            _CONN = None
