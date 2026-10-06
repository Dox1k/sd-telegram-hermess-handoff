"""tg-projects — desktop notifier plugin.

Runs inside the desktop's ``hermes serve`` backend. Watches
``~/.hermes/plugins/tg-projects/handoff_events.jsonl`` (written by the
Telegram-side handoff) and shows a native OS notification when a session
moves to Telegram.

The desktop backend is a separate process from the Telegram gateway; the
only shared channel is the filesystem. This plugin polls the JSONL file
every POLL_S seconds and emits one ``ctx.os.notify(...)`` per new line.

Registration: the desktop loads plugins from ``~/.hermes/plugins/`` just
like the gateway. This module is loaded by Hermes's plugin manager when it
finds ``plugin.yaml`` with ``kind: standalone``.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("hermes_plugins.tg_projects.desktop_notifier")

_PLUG_DIR = Path(__file__).resolve().parent
_EVENTS_FILE = _PLUG_DIR / "handoff_events.jsonl"

POLL_S = 3
# How far back (seconds) a "new" event may be and still notify — protects
# against a burst of stale events on first load.
MAX_AGE_S = 120

_stop = threading.Event()
_thread: Optional[threading.Thread] = None


def _read_new_events(last_ts: float) -> tuple[list, float]:
    """Return (events newer than last_ts, new watermark). Never raises."""
    events: list = []
    newest = last_ts
    try:
        if not _EVENTS_FILE.exists():
            return events, last_ts
        for line in _EVENTS_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                evt = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = float(evt.get("ts") or 0)
            if ts <= last_ts:
                continue
            events.append(evt)
            newest = max(newest, ts)
    except OSError:
        logger.warning("tg-projects notifier: events read failed", exc_info=True)
    return events, newest


def _notify(ctx, evt: Dict[str, Any]) -> None:
    """Emit one native notification for a handoff event."""
    session_id = str(evt.get("session_id") or "")[:24]
    text = f"Сессия {session_id} перешла в Telegram. Продолжайте с телефона."
    try:
        # Desktop plugin surface: ctx.os.notify(...)
        ctx.os.notify({
            "title": "Hermes — переключение устройства",
            "body": text,
            "session_id": str(evt.get("session_id") or ""),
        })
    except Exception:
        logger.warning("tg-projects notifier: notify failed", exc_info=True)


def _watch_loop(ctx) -> None:
    """Poll the events file; notify for each new entry. Runs until stopped."""
    last_ts = time.time() - MAX_AGE_S  # only notify for recent events on start
    while not _stop.is_set():
        events, last_ts = _read_new_events(last_ts)
        for evt in events:
            _notify(ctx, evt)
        _stop.wait(POLL_S)


def register(ctx) -> None:
    """Start the notifier thread when the desktop backend loads this plugin."""
    global _thread
    if _thread is not None and _thread.is_alive():
        return
    if not hasattr(ctx, "os") or not hasattr(ctx.os, "notify"):
        logger.info("tg-projects notifier: ctx.os.notify unavailable — plugin inactive")
        return
    _stop.clear()
    _thread = threading.Thread(target=_watch_loop, args=(ctx,), daemon=True,
                               name="tg-projects-desktop-notifier")
    _thread.start()
    logger.info("tg-projects: desktop notifier started (poll %ds)", POLL_S)


def unregister() -> None:
    """Stop the notifier thread (plugin unload / tests)."""
    _stop.set()
    if _thread is not None:
        _thread.join(timeout=2.0)
