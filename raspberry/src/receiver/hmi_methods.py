"""
HMI method handlers.

Each handler is invoked when a C2D message arrives with the matching
"method" field.  Events are persisted to the SQLite database via
:mod:`src.infra.database`.
"""

from __future__ import annotations

import logging
from typing import Any

from ..infra.database import get_db

logger = logging.getLogger(__name__)


def run_hmi(message: Any) -> None:
    """Handler for the 'run_hmi' command."""
    get_db().log_hmi_event("run_hmi", message)


def stop_hmi(message: Any) -> None:
    """Handler for the 'stop_hmi' command."""
    get_db().log_hmi_event("stop_hmi", message)


def reset_hmi(message: Any) -> None:
    """Handler for the 'reset_hmi' command."""
    get_db().log_hmi_event("reset_hmi", message)


# Registry: method name → handler function
HANDLERS: dict[str, Any] = {
    "run_hmi": run_hmi,
    "stop_hmi": stop_hmi,
    "reset_hmi": reset_hmi,
}


def dispatch(message: Any) -> None:
    """
    Parse *message* for a ``method`` key and call the matching handler.

    Logs a warning if the method is unknown or the payload is not a dict.
    """
    if not isinstance(message, dict):
        logger.warning("hmi_methods.dispatch: non-dict payload ignored: %r", message)
        return

    method = message.get("method")
    if method is None:
        logger.warning("hmi_methods.dispatch: no 'method' key in payload: %r", message)
        return

    user = message.get("user")
    if user is None:
        logger.warning("hmi_methods.dispatch: no 'user' key in payload: %r", message)
        return

    handler = HANDLERS.get(method)
    if handler:
        logger.info("Dispatching HMI method '%s'.", method)
        # Remove the 'method' key so it isn't redundantly saved in the DB payload column
        message.pop("method", None)
        handler(message)
    else:
        logger.warning("Unknown HMI method '%s'.", method)
