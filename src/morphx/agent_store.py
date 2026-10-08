"""Restart-safe local acquisition sequence and durable outbox.

Every method opens its own short-lived connection, so methods are safe to call from
worker threads (``asyncio.to_thread``) while the event loop keeps running.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final
from uuid import UUID

from morphx.errors import (
    DeviceIdentityError,
    EventIntegrityError,
    OutboxStateError,
    StoreNotInitializedError,
)
from morphx.models import MeasurementEvent, canonical_json
from morphx.sqlite_utils import configure_database, connection_scope, transaction

__all__ = ["AgentStore", "DeviceIdentityError", "PendingEvent"]

MAX_ERROR_LENGTH: Final[int] = 500

_SCHEMA: Final[str] = """
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
    event_id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    synced_at TEXT,
    quarantined_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TEXT NOT NULL,
    last_error TEXT,
    UNIQUE (device_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_outbox_due
    ON outbox (synced_at, next_attempt_at, sequence);
"""

# Columns added after the first schema version, applied to pre-existing databases.
_ADDED_OUTBOX_COLUMNS: Final[dict[str, str]] = {"quarantined_at": "TEXT"}


@dataclass(frozen=True, slots=True)
class PendingEvent:
    event: MeasurementEvent
    attempt_count: int


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _sanitize_error(error: str) -> str:
    return error.replace("\r", " ").replace("\n", " ")[:MAX_ERROR_LENGTH]


def _migrate_outbox(connection: sqlite3.Connection) -> None:
    existing = {row["name"] for row in connection.execute("PRAGMA table_info(outbox)")}
    for column, column_type in _ADDED_OUTBOX_COLUMNS.items():
        if column not in existing:
            # Identifiers come from the constant mapping above, never from input.
            connection.execute(f"ALTER TABLE outbox ADD COLUMN {column} {column_type}")


class AgentStore:
    """Local SQLite state; sequence allocation and enqueue happen atomically."""

    def __init__(self, database: Path, device_id: str) -> None:
        self.database = database
        self.device_id = device_id

    def initialize(self) -> None:
        configure_database(self.database)
        with connection_scope(self.database) as connection:
            connection.executescript(_SCHEMA)
            with transaction(connection):
                _migrate_outbox(connection)
                existing = connection.execute(
                    "SELECT value FROM metadata WHERE key = 'device_id'"
                ).fetchone()
                if existing is None:
                    connection.execute(
                        "INSERT INTO metadata (key, value) VALUES ('device_id', ?)",
                        (self.device_id,),
                    )
                    connection.execute(
                        "INSERT INTO metadata (key, value) VALUES ('next_sequence', '1')"
                    )
                elif existing["value"] != self.device_id:
                    raise DeviceIdentityError(
                        f"database belongs to {existing['value']!r}, not {self.device_id!r}"
                    )

    def create_event(
        self, factory: Callable[[int], MeasurementEvent], *, now: datetime
    ) -> MeasurementEvent:
        with connection_scope(self.database) as connection, transaction(connection):
            sequence_row = connection.execute(
                "SELECT value FROM metadata WHERE key = 'next_sequence'"
            ).fetchone()
            if sequence_row is None:
                raise StoreNotInitializedError("agent database is not initialized")
            sequence = int(sequence_row["value"])
            event = factory(sequence)
            if event.device_id != self.device_id or event.sequence != sequence:
                raise EventIntegrityError("event factory returned a mismatched device or sequence")
            timestamp = _iso(now)
            connection.execute(
                """
                INSERT INTO outbox (
                    event_id, device_id, sequence, payload_json, created_at, next_attempt_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(event.event_id),
                    event.device_id,
                    event.sequence,
                    canonical_json(event),
                    timestamp,
                    timestamp,
                ),
            )
            connection.execute(
                "UPDATE metadata SET value = ? WHERE key = 'next_sequence'",
                (str(sequence + 1),),
            )
            return event

    def due_events(self, *, now: datetime, limit: int) -> list[PendingEvent]:
        with connection_scope(self.database) as connection:
            rows = connection.execute(
                """
                SELECT payload_json, attempt_count
                FROM outbox
                WHERE synced_at IS NULL
                  AND quarantined_at IS NULL
                  AND next_attempt_at <= ?
                ORDER BY sequence ASC
                LIMIT ?
                """,
                (_iso(now), limit),
            ).fetchall()
        return [
            PendingEvent(
                event=MeasurementEvent.model_validate_json(row["payload_json"]),
                attempt_count=int(row["attempt_count"]),
            )
            for row in rows
        ]

    def _update_pending(self, sql: str, params: tuple[Any, ...], action: str) -> None:
        with connection_scope(self.database) as connection:
            cursor = connection.execute(sql, params)
            if cursor.rowcount != 1:
                raise OutboxStateError(f"pending event disappeared before {action}")

    def mark_synced(self, event_id: UUID, *, synced_at: datetime) -> None:
        self._update_pending(
            """
            UPDATE outbox
            SET synced_at = ?, last_error = NULL
            WHERE event_id = ? AND synced_at IS NULL AND quarantined_at IS NULL
            """,
            (_iso(synced_at), str(event_id)),
            "acknowledgement",
        )

    def mark_failed(self, event_id: UUID, *, error: str, next_attempt_at: datetime) -> None:
        self._update_pending(
            """
            UPDATE outbox
            SET attempt_count = attempt_count + 1,
                next_attempt_at = ?,
                last_error = ?
            WHERE event_id = ? AND synced_at IS NULL AND quarantined_at IS NULL
            """,
            (_iso(next_attempt_at), _sanitize_error(error), str(event_id)),
            "retry scheduling",
        )

    def mark_quarantined(self, event_id: UUID, *, error: str, quarantined_at: datetime) -> None:
        """Stop retrying a record the server permanently rejected; keep it for audit."""
        self._update_pending(
            """
            UPDATE outbox
            SET attempt_count = attempt_count + 1,
                quarantined_at = ?,
                last_error = ?
            WHERE event_id = ? AND synced_at IS NULL AND quarantined_at IS NULL
            """,
            (_iso(quarantined_at), _sanitize_error(error), str(event_id)),
            "quarantine",
        )

    def pending_count(self) -> int:
        return self._count("synced_at IS NULL AND quarantined_at IS NULL")

    def quarantined_count(self) -> int:
        return self._count("quarantined_at IS NOT NULL")

    def _count(self, predicate: str) -> int:
        with connection_scope(self.database) as connection:
            # Predicates are fixed literals from this class, never caller input.
            row = connection.execute(
                f"SELECT COUNT(*) AS count FROM outbox WHERE {predicate}"  # noqa: S608
            ).fetchone()
        return int(row["count"]) if row is not None else 0

    def all_events(self) -> list[MeasurementEvent]:
        with connection_scope(self.database) as connection:
            rows = connection.execute(
                "SELECT payload_json FROM outbox ORDER BY sequence ASC"
            ).fetchall()
        return [MeasurementEvent.model_validate_json(row["payload_json"]) for row in rows]

    def get_outbox_row(self, event_id: UUID) -> dict[str, Any] | None:
        """Return diagnostic state without exposing it over the network."""
        with connection_scope(self.database) as connection:
            row = connection.execute(
                "SELECT * FROM outbox WHERE event_id = ?", (str(event_id),)
            ).fetchone()
        return dict(row) if row is not None else None
