"""Outbox state transitions, quarantine, error types and schema migration."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import timedelta
from uuid import uuid4

import pytest

from morphx.agent_store import AgentStore
from morphx.errors import EventIntegrityError, OutboxStateError, StoreNotInitializedError


def _initialized(temp_db) -> AgentStore:
    store = AgentStore(temp_db, "MORPHX_SIM_001")
    store.initialize()
    return store


def test_quarantined_events_are_not_due_and_not_pending(temp_db, fixed_time, event_factory):
    store = _initialized(temp_db)
    first = store.create_event(event_factory, now=fixed_time)
    second = store.create_event(event_factory, now=fixed_time)

    store.mark_quarantined(first.event_id, error="SEQUENCE_CONFLICT", quarantined_at=fixed_time)

    due = store.due_events(now=fixed_time + timedelta(days=1), limit=10)
    assert [pending.event.event_id for pending in due] == [second.event_id]
    assert store.pending_count() == 1
    assert store.quarantined_count() == 1
    with pytest.raises(OutboxStateError, match="acknowledgement"):
        store.mark_synced(first.event_id, synced_at=fixed_time)


@pytest.mark.parametrize("method", ["mark_synced", "mark_failed", "mark_quarantined"])
def test_unknown_event_raises_outbox_state_error(temp_db, fixed_time, method) -> None:
    store = _initialized(temp_db)
    kwargs = {
        "mark_synced": {"synced_at": fixed_time},
        "mark_failed": {"error": "x", "next_attempt_at": fixed_time},
        "mark_quarantined": {"error": "x", "quarantined_at": fixed_time},
    }[method]
    with pytest.raises(OutboxStateError):
        getattr(store, method)(uuid4(), **kwargs)


def test_errors_are_sanitized_and_truncated(temp_db, fixed_time, event_factory) -> None:
    store = _initialized(temp_db)
    event = store.create_event(event_factory, now=fixed_time)
    store.mark_failed(event.event_id, error="a\r\nb" + "x" * 1000, next_attempt_at=fixed_time)
    row = store.get_outbox_row(event.event_id)
    assert row is not None
    assert row["last_error"].startswith("a  b")
    assert len(row["last_error"]) == 500


def test_create_event_requires_initialization(temp_db, fixed_time, event_factory) -> None:
    store = AgentStore(temp_db, "MORPHX_SIM_001")
    with closing(sqlite3.connect(temp_db)) as connection:
        connection.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        connection.commit()
    with pytest.raises(StoreNotInitializedError):
        store.create_event(event_factory, now=fixed_time)


def test_factory_mismatch_is_rejected_and_rolled_back(temp_db, fixed_time, event_factory) -> None:
    store = _initialized(temp_db)
    with pytest.raises(EventIntegrityError):
        store.create_event(lambda sequence: event_factory(sequence + 1), now=fixed_time)
    assert store.create_event(event_factory, now=fixed_time).sequence == 1


def test_existing_v1_database_is_migrated(temp_db, fixed_time, event_factory) -> None:
    connection = sqlite3.connect(temp_db)
    connection.executescript(
        """
        CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO metadata VALUES ('device_id', 'MORPHX_SIM_001'), ('next_sequence', '7');
        CREATE TABLE outbox (
            event_id TEXT PRIMARY KEY,
            device_id TEXT NOT NULL,
            sequence INTEGER NOT NULL CHECK (sequence > 0),
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            synced_at TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
            next_attempt_at TEXT NOT NULL,
            last_error TEXT,
            UNIQUE (device_id, sequence)
        );
        """
    )
    connection.close()

    store = _initialized(temp_db)
    event = store.create_event(event_factory, now=fixed_time)
    store.mark_quarantined(event.event_id, error="x", quarantined_at=fixed_time)

    assert event.sequence == 7
    assert store.quarantined_count() == 1
    _initialized(temp_db)  # idempotent on an already migrated database
