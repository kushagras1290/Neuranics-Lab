"""Restart-safe local acquisition sequence and durable outbox."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from morphx.models import MeasurementEvent
from morphx.sqlite_utils import configure_database, connect, connection_scope


class DeviceIdentityError(RuntimeError):
    """A local database was opened with a different device identity."""


@dataclass(frozen=True, slots=True)
class PendingEvent:
    event: MeasurementEvent
    attempt_count: int


def _canonical_payload(event: MeasurementEvent) -> str:
    return json.dumps(
        event.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


class AgentStore:
    """Local SQLite state; sequence allocation and enqueue happen atomically."""

    def __init__(self, database: Path, device_id: str) -> None:
        self.database = database
        self.device_id = device_id

    def initialize(self) -> None:
        configure_database(self.database)
        connection = connect(self.database)
        try:
            connection.executescript(
                """
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
                    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
                    next_attempt_at TEXT NOT NULL,
                    last_error TEXT,
                    UNIQUE (device_id, sequence)
                );

                CREATE INDEX IF NOT EXISTS idx_outbox_due
                    ON outbox (synced_at, next_attempt_at, sequence);
                """
            )
            connection.execute("BEGIN IMMEDIATE")
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
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def create_event(
        self, factory: Callable[[int], MeasurementEvent], *, now: datetime
    ) -> MeasurementEvent:
        connection = connect(self.database)
        try:
            connection.execute("BEGIN IMMEDIATE")
            sequence_row = connection.execute(
                "SELECT value FROM metadata WHERE key = 'next_sequence'"
            ).fetchone()
            if sequence_row is None:
                raise RuntimeError("agent database is not initialized")
            sequence = int(sequence_row["value"])
            event = factory(sequence)
            if event.device_id != self.device_id or event.sequence != sequence:
                raise ValueError("event factory returned a mismatched device or sequence")
            canonical = _canonical_payload(event)
            timestamp = now.astimezone(UTC).isoformat()
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
                    canonical,
                    timestamp,
                    timestamp,
                ),
            )
            connection.execute(
                "UPDATE metadata SET value = ? WHERE key = 'next_sequence'",
                (str(sequence + 1),),
            )
            connection.commit()
            return event
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def due_events(self, *, now: datetime, limit: int) -> list[PendingEvent]:
        with connection_scope(self.database) as connection:
            rows = connection.execute(
                """
                SELECT payload_json, attempt_count
                FROM outbox
                WHERE synced_at IS NULL AND next_attempt_at <= ?
                ORDER BY sequence ASC
                LIMIT ?
                """,
                (now.astimezone(UTC).isoformat(), limit),
            ).fetchall()
        return [
            PendingEvent(
                event=MeasurementEvent.model_validate_json(row["payload_json"]),
                attempt_count=int(row["attempt_count"]),
            )
            for row in rows
        ]

    def mark_synced(self, event_id: UUID, *, synced_at: datetime) -> None:
        with connection_scope(self.database) as connection:
            cursor = connection.execute(
                """
                UPDATE outbox
                SET synced_at = ?, last_error = NULL
                WHERE event_id = ? AND synced_at IS NULL
                """,
                (synced_at.astimezone(UTC).isoformat(), str(event_id)),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("pending event disappeared before acknowledgement")

    def mark_failed(self, event_id: UUID, *, error: str, next_attempt_at: datetime) -> None:
        safe_error = error.replace("\r", " ").replace("\n", " ")[:500]
        with connection_scope(self.database) as connection:
            cursor = connection.execute(
                """
                UPDATE outbox
                SET attempt_count = attempt_count + 1,
                    next_attempt_at = ?,
                    last_error = ?
                WHERE event_id = ? AND synced_at IS NULL
                """,
                (
                    next_attempt_at.astimezone(UTC).isoformat(),
                    safe_error,
                    str(event_id),
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("pending event disappeared before retry scheduling")

    def pending_count(self) -> int:
        with connection_scope(self.database) as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM outbox WHERE synced_at IS NULL"
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
