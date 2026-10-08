"""SQLite connection helpers with durability-focused defaults."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Final

BUSY_TIMEOUT_SECONDS: Final[float] = 5.0


def connect(database: Path) -> sqlite3.Connection:
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database, timeout=BUSY_TIMEOUT_SECONDS, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute(f"PRAGMA busy_timeout = {int(BUSY_TIMEOUT_SECONDS * 1000)}")
    connection.execute("PRAGMA synchronous = FULL")
    return connection


@contextmanager
def connection_scope(database: Path) -> Iterator[sqlite3.Connection]:
    connection = connect(database)
    try:
        yield connection
    finally:
        connection.close()


@contextmanager
def transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Run a write-locked (``BEGIN IMMEDIATE``) transaction; roll back on any error."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield connection
    except BaseException:
        if connection.in_transaction:
            connection.rollback()
        raise
    connection.commit()


def configure_database(database: Path) -> None:
    with connection_scope(database) as connection:
        connection.execute("PRAGMA journal_mode = WAL")
