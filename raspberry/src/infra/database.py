"""
SQLite database layer.

Provides a thin wrapper around ``sqlite3`` for persisting application data.
The database file lives under the ``data/`` directory (configurable via
``settings.INFRA.DB_PATH``).
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..settings import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS hmi_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp  TEXT    NOT NULL,
    method     TEXT    NOT NULL,
    user       TEXT    NOT NULL,
    payload    TEXT    NOT NULL
);
"""


# ---------------------------------------------------------------------------
# Database class
# ---------------------------------------------------------------------------


class Database:
    """
    Lightweight SQLite wrapper.

    The connection is opened immediately on construction and kept open
    for the lifetime of the object.  Call :meth:`close` for a clean shutdown.
    """

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None
        self._get_conn()  # create file + tables immediately

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.executescript(_SCHEMA)
            logger.info("Database opened: %s", self._db_path)
            self._conn = conn
        return self._conn  # type: ignore[return-value]

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            logger.info("Database closed: %s", self._db_path)

    # ------------------------------------------------------------------
    # HMI events
    # ------------------------------------------------------------------

    def log_hmi_event(self, method: str, message: Any) -> None:
        """
        Insert a single HMI event into the ``hmi_events`` table.

        Parameters
        ----------
        method:
            The HMI method name (e.g. ``"run_hmi"``).
        message:
            The full payload received (stored as JSON text).
        """
        conn = self._get_conn()
        timestamp = datetime.now(timezone.utc).isoformat()
        user = message.pop("user", "unknown")
        payload = json.dumps(message, ensure_ascii=False)
        conn.execute(
            "INSERT INTO hmi_events (timestamp, method, user, payload) VALUES (?, ?, ?, ?)",
            (timestamp, method, user, payload),
        )
        conn.commit()
        logger.debug("HMI event logged: %s by %s", method, user)

    def get_hmi_events(
        self,
        method: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Retrieve HMI events, optionally filtered by *method*.

        Returns a list of dicts with keys: ``id``, ``timestamp``,
        ``method``, ``payload``.
        """
        conn = self._get_conn()
        if method:
            rows = conn.execute(
                "SELECT id, timestamp, method, user, payload FROM hmi_events "
                "WHERE method = ? ORDER BY id DESC LIMIT ?",
                (method, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, timestamp, method, user, payload FROM hmi_events "
                "ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()

        return [
            {
                "id": r[0],
                "timestamp": r[1],
                "method": r[2],
                "user": r[3],
                "payload": json.loads(r[4]),
            }
            for r in rows
        ]


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_db: Database | None = None


def get_db() -> Database:
    """Return the application-wide :class:`Database` singleton."""
    global _db
    if _db is None:
        _db = Database(settings.INFRA.DB_PATH)
    return _db


def close_db() -> None:
    """Close the singleton database connection (call on shutdown)."""
    global _db
    if _db is not None:
        _db.close()
        _db = None
