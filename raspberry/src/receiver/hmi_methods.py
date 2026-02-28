"""
HMI method handlers.

Each handler is invoked when a C2D message arrives with the matching
"method" field.  The received message is appended to ``hmi_log.txt``
in the current working directory.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_LOG_FILE = Path("hmi_log.txt")


def _append_log(method: str, message: Any) -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    line = f"[{timestamp}] {method}: {json.dumps(message, ensure_ascii=False)}\n"
    with _LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line)


def run_hmi(message: Any) -> None:
    """Handler for the 'run_hmi' command."""
    _append_log("run_hmi", message)


def stop_hmi(message: Any) -> None:
    """Handler for the 'stop_hmi' command."""
    _append_log("stop_hmi", message)


def reset_hmi(message: Any) -> None:
    """Handler for the 'reset_hmi' command."""
    _append_log("reset_hmi", message)


# Registry: method name → handler function
HANDLERS: dict[str, Any] = {
    "run_hmi": run_hmi,
    "stop_hmi": stop_hmi,
    "reset_hmi": reset_hmi,
}


def dispatch(message: Any) -> None:
    """
    Parse *message* for a ``method`` key and call the matching handler.

    Logs a warning if the method is unknown.
    """
    import logging

    logger = logging.getLogger(__name__)

    if isinstance(message, dict):
        method = message.get("method")
    else:
        logger.warning("hmi_methods.dispatch: non-dict payload ignored: %r", message)
        return

    handler = HANDLERS.get(method)
    if handler:
        logger.info("Dispatching HMI method '%s'.", method)
        handler(message)
    else:
        logger.warning("Unknown HMI method '%s'.", method)
