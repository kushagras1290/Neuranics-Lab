"""SQLite connection helpers with durability-focused defaults."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


def connect(database: Path) -> sqlite3.Connection:
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database, timeout=5.0, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.execute("PRAGMA synchronous = FULL")
    return connection


@contextmanager
def connection_scope(database: Path) -> Iterator[sqlite3.Connection]:
    connection = connect(database)
    try:
        yield connection
    finally:
        connection.close()


def configure_database(database: Path) -> None:
    with connection_scope(database) as connection:
        connection.execute("PRAGMA journal_mode = WAL")
