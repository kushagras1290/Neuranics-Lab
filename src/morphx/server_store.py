"""Durable, idempotent central measurement storage."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from morphx.models import MeasurementEvent, StoredMeasurement
from morphx.sqlite_utils import configure_database, connect, connection_scope


class IngestConflictError(RuntimeError):
    """The idempotency key or device sequence was reused for different data."""


@dataclass(frozen=True, slots=True)
class IngestResult:
    created: bool
    measurement: StoredMeasurement


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _canonical_payload(event: MeasurementEvent) -> str:
    return json.dumps(
        event.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def _stored_from_row(row: sqlite3.Row) -> StoredMeasurement:
    payload: dict[str, Any] = json.loads(str(row["payload_json"]))
    payload["received_at"] = row["received_at"]
    return StoredMeasurement.model_validate(payload)


class ServerStore:
    """SQLite repository. Each method owns its connection for thread safety."""

    def __init__(self, database: Path, *, clock: Any = _utc_now) -> None:
        self.database = database
        self._clock = clock

    def initialize(self) -> None:
        configure_database(self.database)
        with connection_scope(self.database) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS measurements (
                    event_id TEXT PRIMARY KEY,
                    device_id TEXT NOT NULL,
                    sample_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL CHECK (sequence > 0),
                    measured_at TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    UNIQUE (device_id, sequence)
                );

                CREATE INDEX IF NOT EXISTS idx_measurements_device_sequence
                    ON measurements (device_id, sequence);
                """
            )

    def ingest(self, event: MeasurementEvent) -> IngestResult:
        payload = _canonical_payload(event)
        received_at = self._clock().astimezone(UTC)
        connection = connect(self.database)
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM measurements WHERE event_id = ?", (str(event.event_id),)
            ).fetchone()
            if existing is not None:
                if existing["payload_json"] != payload:
                    raise IngestConflictError(
                        "event_id already exists with a different measurement payload"
                    )
                connection.commit()
                return IngestResult(created=False, measurement=_stored_from_row(existing))

            occupied_sequence = connection.execute(
                "SELECT event_id FROM measurements WHERE device_id = ? AND sequence = ?",
                (event.device_id, event.sequence),
            ).fetchone()
            if occupied_sequence is not None:
                raise IngestConflictError("device sequence already belongs to a different event_id")

            connection.execute(
                """
                INSERT INTO measurements (
                    event_id, device_id, sample_id, sequence, measured_at,
                    received_at, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(event.event_id),
                    event.device_id,
                    event.sample_id,
                    event.sequence,
                    event.measured_at.isoformat(),
                    received_at.isoformat(),
                    payload,
                ),
            )
            row = connection.execute(
                "SELECT * FROM measurements WHERE event_id = ?", (str(event.event_id),)
            ).fetchone()
            connection.commit()
            if row is None:  # pragma: no cover - defensive invariant
                raise RuntimeError("measurement insert was not visible inside its transaction")
            return IngestResult(created=True, measurement=_stored_from_row(row))
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def list_for_device(
        self, device_id: str, *, after_sequence: int, limit: int
    ) -> tuple[list[StoredMeasurement], int | None]:
        with connection_scope(self.database) as connection:
            rows = connection.execute(
                """
                SELECT * FROM measurements
                WHERE device_id = ? AND sequence > ?
                ORDER BY sequence ASC
                LIMIT ?
                """,
                (device_id, after_sequence, limit + 1),
            ).fetchall()
        has_more = len(rows) > limit
        selected = rows[:limit]
        items = [_stored_from_row(row) for row in selected]
        next_after = items[-1].sequence if has_more and items else None
        return items, next_after

    def is_ready(self) -> bool:
        try:
            with connection_scope(self.database) as connection:
                row = connection.execute("SELECT 1 AS ready").fetchone()
            return row is not None and row["ready"] == 1
        except sqlite3.Error:
            return False
