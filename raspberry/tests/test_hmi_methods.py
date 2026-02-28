"""
Tests for src/receiver/hmi_methods.py — dispatch and database logging.

Run from raspberry/ with: pytest tests/test_hmi_methods.py -v
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from src.infra.database import Database
from src.receiver import hmi_methods


@pytest.fixture(autouse=True)
def _use_tmp_db(tmp_path: Path):
    """Use a temporary SQLite database for every test."""
    db = Database(tmp_path / "test.db")
    with patch("src.receiver.hmi_methods.get_db", return_value=db):
        yield db
    db.close()


class TestDispatchKnownMethods:
    def test_run_hmi_inserts_event(self, _use_tmp_db: Database):
        hmi_methods.dispatch({"method": "run_hmi", "data": 42})
        events = _use_tmp_db.get_hmi_events(method="run_hmi")
        assert len(events) == 1
        assert events[0]["payload"]["data"] == 42

    def test_stop_hmi_inserts_event(self, _use_tmp_db: Database):
        hmi_methods.dispatch({"method": "stop_hmi"})
        events = _use_tmp_db.get_hmi_events(method="stop_hmi")
        assert len(events) == 1

    def test_reset_hmi_inserts_event(self, _use_tmp_db: Database):
        hmi_methods.dispatch({"method": "reset_hmi", "reason": "test"})
        events = _use_tmp_db.get_hmi_events(method="reset_hmi")
        assert len(events) == 1
        assert events[0]["payload"]["reason"] == "test"


class TestDispatchUnknownMethod:
    def test_unknown_method_does_not_insert(self, _use_tmp_db: Database):
        hmi_methods.dispatch({"method": "unknown_xyz"})
        events = _use_tmp_db.get_hmi_events()
        assert len(events) == 0

    def test_unknown_method_logs_warning(self, caplog):
        with caplog.at_level("WARNING"):
            hmi_methods.dispatch({"method": "unknown_xyz"})
        assert "Unknown HMI method" in caplog.text


class TestDispatchEdgeCases:
    def test_non_dict_payload_ignored(self, _use_tmp_db: Database, caplog):
        with caplog.at_level("WARNING"):
            hmi_methods.dispatch("not a dict")
        assert "non-dict payload" in caplog.text
        assert len(_use_tmp_db.get_hmi_events()) == 0

    def test_missing_method_key_ignored(self, _use_tmp_db: Database, caplog):
        with caplog.at_level("WARNING"):
            hmi_methods.dispatch({"data": 123})
        assert "no 'method' key" in caplog.text
        assert len(_use_tmp_db.get_hmi_events()) == 0

    def test_multiple_dispatches_accumulate(self, _use_tmp_db: Database):
        hmi_methods.dispatch({"method": "run_hmi", "seq": 1})
        hmi_methods.dispatch({"method": "stop_hmi", "seq": 2})
        events = _use_tmp_db.get_hmi_events()
        assert len(events) == 2


class TestDatabase:
    def test_get_hmi_events_filtered(self, _use_tmp_db: Database):
        _use_tmp_db.log_hmi_event("run_hmi", {"a": 1})
        _use_tmp_db.log_hmi_event("stop_hmi", {"b": 2})
        assert len(_use_tmp_db.get_hmi_events(method="run_hmi")) == 1
        assert len(_use_tmp_db.get_hmi_events(method="stop_hmi")) == 1
        assert len(_use_tmp_db.get_hmi_events()) == 2

    def test_get_hmi_events_limit(self, _use_tmp_db: Database):
        for i in range(10):
            _use_tmp_db.log_hmi_event("run_hmi", {"i": i})
        assert len(_use_tmp_db.get_hmi_events(limit=3)) == 3

    def test_event_has_timestamp(self, _use_tmp_db: Database):
        _use_tmp_db.log_hmi_event("run_hmi", {})
        event = _use_tmp_db.get_hmi_events()[0]
        assert "timestamp" in event
        assert event["timestamp"]  # not empty
