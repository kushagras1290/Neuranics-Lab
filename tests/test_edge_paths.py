"""Error paths that only occur under misconfiguration, storage faults or cancellation."""

from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing
from datetime import datetime

import pytest
from pydantic import ValidationError

from morphx.config import AgentSettings, ConfigurationError
from morphx.models import StoredMeasurement
from morphx.server_store import ServerStore
from morphx.sqlite_utils import connect, transaction
from morphx.supervision import supervise


def test_non_integer_setting_fails_closed(monkeypatch) -> None:
    monkeypatch.setenv("MORPHX_SYNC_BATCH_SIZE", "lots")
    with pytest.raises(ConfigurationError, match="must be an integer"):
        AgentSettings.from_env()


def test_stored_measurement_requires_timezone_on_receipt(event_factory) -> None:
    payload = event_factory().model_dump()
    payload["received_at"] = datetime(2026, 10, 7, 8, 0)
    with pytest.raises(ValidationError, match="received_at must include a timezone"):
        StoredMeasurement.model_validate(payload)


def test_readiness_is_false_when_database_cannot_be_opened(tmp_path) -> None:
    # A directory cannot be opened as a SQLite database file.
    assert ServerStore(tmp_path).is_ready() is False


def test_transaction_tolerates_sqlite_having_already_rolled_back(temp_db) -> None:
    with closing(connect(temp_db)) as connection:
        connection.execute("CREATE TABLE t (x INTEGER)")
        with pytest.raises(RuntimeError, match="after abort"), transaction(connection):
            connection.execute("INSERT INTO t VALUES (1)")
            connection.execute("ROLLBACK")  # what SQLite does itself on e.g. SQLITE_FULL
            raise RuntimeError("failure after abort")
        assert not connection.in_transaction
        assert connection.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0


def test_transaction_rolls_back_on_error(temp_db) -> None:
    with closing(connect(temp_db)) as connection:
        connection.execute("CREATE TABLE t (x INTEGER)")
        with pytest.raises(sqlite3.IntegrityError), transaction(connection):
            connection.execute("INSERT INTO t VALUES (1)")
            raise sqlite3.IntegrityError("simulated")
        assert connection.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0


@pytest.mark.anyio
async def test_cancelling_supervisor_cancels_children() -> None:
    started = asyncio.Event()
    child_cancelled = asyncio.Event()

    async def child() -> None:
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            child_cancelled.set()
            raise

    supervisor = asyncio.create_task(supervise({"child": child()}, asyncio.Event()))
    await asyncio.wait_for(started.wait(), timeout=2)
    supervisor.cancel()
    with pytest.raises(asyncio.CancelledError):
        await supervisor
    assert child_cancelled.is_set()
