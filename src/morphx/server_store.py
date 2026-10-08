"""Durable, idempotent central measurement storage."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from morphx.errors import EventIdConflictError, SequenceConflictError, StorageError
from morphx.models import DeviceSummary, MeasurementEvent, StoredMeasurement, canonical_json
from morphx.sqlite_utils import configure_database, connection_scope, transaction

__all__ = ["IngestResult", "MeasurementFilter", "ServerStore"]

_SCHEMA: Final[str] = """
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


@dataclass(frozen=True, slots=True)
class IngestResult:
    created: bool
    measurement: StoredMeasurement


@dataclass(frozen=True, slots=True)
class MeasurementFilter:
    """Cursor and optional measured_at window for per-device retrieval."""

    after_sequence: int = 0
    limit: int = 100
    measured_from: datetime | None = None
    measured_to: datetime | None = None


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    # Stored timestamps are UTC isoformat strings. In that format "+" sorts before "."
    # and before digits, so lexicographic order equals chronological order.
    return value.astimezone(UTC).isoformat()


def _stored_from_row(row: sqlite3.Row) -> StoredMeasurement:
    payload: dict[str, Any] = json.loads(str(row["payload_json"]))
    payload["received_at"] = row["received_at"]
    return StoredMeasurement.model_validate(payload)


class ServerStore:
    """SQLite repository. Each method owns its connection for thread safety."""

    def __init__(self, database: Path, *, clock: Callable[[], datetime] = _utc_now) -> None:
        self.database = database
        self._clock = clock

    def initialize(self) -> None:
        configure_database(self.database)
        with connection_scope(self.database) as connection:
            connection.executescript(_SCHEMA)

    def ingest(self, event: MeasurementEvent) -> IngestResult:
        payload = canonical_json(event)
        received_at = self._clock()
        with connection_scope(self.database) as connection, transaction(connection):
            existing = connection.execute(
                "SELECT * FROM measurements WHERE event_id = ?", (str(event.event_id),)
            ).fetchone()
            if existing is not None:
                if existing["payload_json"] != payload:
                    raise EventIdConflictError(
                        "event_id already exists with a different measurement payload"
                    )
                return IngestResult(created=False, measurement=_stored_from_row(existing))

            occupied_sequence = connection.execute(
                "SELECT event_id FROM measurements WHERE device_id = ? AND sequence = ?",
                (event.device_id, event.sequence),
            ).fetchone()
            if occupied_sequence is not None:
                raise SequenceConflictError(
                    "device sequence already belongs to a different event_id"
                )

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
                    _iso(event.measured_at),
                    _iso(received_at),
                    payload,
                ),
            )
            row = connection.execute(
                "SELECT * FROM measurements WHERE event_id = ?", (str(event.event_id),)
            ).fetchone()
            if row is None:  # pragma: no cover - defensive invariant
                raise StorageError("measurement insert was not visible inside its transaction")
            return IngestResult(created=True, measurement=_stored_from_row(row))

    def list_for_device(
        self, device_id: str, query: MeasurementFilter
    ) -> tuple[list[StoredMeasurement], int | None]:
        clauses = ["device_id = ?", "sequence > ?"]
        params: list[Any] = [device_id, query.after_sequence]
        if query.measured_from is not None:
            clauses.append("measured_at >= ?")
            params.append(_iso(query.measured_from))
        if query.measured_to is not None:
            clauses.append("measured_at <= ?")
            params.append(_iso(query.measured_to))
        params.append(query.limit + 1)
        # Only fixed clause literals are joined; every value is a bound parameter.
        sql = (
            "SELECT * FROM measurements WHERE "  # noqa: S608
            + " AND ".join(clauses)
            + " ORDER BY sequence ASC LIMIT ?"
        )
        with connection_scope(self.database) as connection:
            rows = connection.execute(sql, params).fetchall()
        has_more = len(rows) > query.limit
        items = [_stored_from_row(row) for row in rows[: query.limit]]
        next_after = items[-1].sequence if has_more and items else None
        return items, next_after

    def list_devices(self) -> list[DeviceSummary]:
        with connection_scope(self.database) as connection:
            rows = connection.execute(
                """
                SELECT device_id,
                       COUNT(*) AS measurement_count,
                       MIN(sequence) AS first_sequence,
                       MAX(sequence) AS last_sequence,
                       MAX(measured_at) AS last_measured_at,
                       MAX(received_at) AS last_received_at
                FROM measurements
                GROUP BY device_id
                ORDER BY device_id ASC
                """
            ).fetchall()
        return [DeviceSummary.model_validate(dict(row)) for row in rows]

    def is_ready(self) -> bool:
        try:
            with connection_scope(self.database) as connection:
                row = connection.execute("SELECT 1 AS ready").fetchone()
            return row is not None and row["ready"] == 1
        except sqlite3.Error:
            return False
