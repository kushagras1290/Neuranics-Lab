"""Black-box resilience check with real processes, HTTP and SQLite files.

Scenarios, in order, against one agent database and one server database:

1. server down       - the agent keeps acquiring into its outbox;
2. server started    - the offline backlog drains automatically, in order;
3. agent kill -9     - the agent is hard-killed and restarted: sequences continue and
                       no event is lost, duplicated or re-minted;
4. server restart    - the server is stopped mid-run and restarted on the same database.

Finally every outbox event_id must exist on the server exactly once, with contiguous
sequences starting at 1. Prints one JSON line on success; exits non-zero on failure.
"""

from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

DEVICE_ID: Final[str] = "MORPHX_SIM_001"
STARTUP_TIMEOUT_SECONDS: Final[float] = 30.0  # cold Windows imports can be slow
CONVERGE_TIMEOUT_SECONDS: Final[float] = 30.0
POLL_SECONDS: Final[float] = 0.05
OFFLINE_RECORDS: Final[int] = 3
PAGE_SIZE: Final[int] = 500


class VerificationError(RuntimeError):
    """A resilience property did not hold."""


@dataclass(frozen=True, slots=True)
class OutboxRow:
    event_id: str
    sequence: int
    synced: bool


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def read_outbox(database: Path) -> list[OutboxRow]:
    if not database.exists():
        return []
    with closing(sqlite3.connect(database, timeout=5)) as connection:
        try:
            rows = connection.execute(
                "SELECT event_id, sequence, synced_at IS NOT NULL FROM outbox ORDER BY sequence"
            ).fetchall()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return []
            raise
    return [OutboxRow(str(row[0]), int(row[1]), bool(row[2])) for row in rows]


def fetch_server_items(server_url: str) -> list[dict[str, Any]] | None:
    """Return every stored item for the device by following the cursor, or None if down."""
    items: list[dict[str, Any]] = []
    after = 0
    while True:
        url = (
            f"{server_url}/v1/devices/{DEVICE_ID}/measurements"
            f"?after_sequence={after}&limit={PAGE_SIZE}"
        )
        try:
            with urllib.request.urlopen(url, timeout=1) as response:  # noqa: S310
                page = json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError):
            return None
        items.extend(page["items"])
        if page["next_after_sequence"] is None:
            return items
        after = int(page["next_after_sequence"])


def wait_until(description: str, condition: Callable[[], bool], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(POLL_SECONDS)
    raise VerificationError(f"timed out waiting for: {description}")


class Harness:
    def __init__(self, root: Path) -> None:
        self.port = free_port()
        self.server_url = f"http://127.0.0.1:{self.port}"
        self.agent_db = root / "agent.db"
        self.env = {
            **os.environ,
            "MORPHX_AGENT_DB_PATH": str(self.agent_db),
            "MORPHX_SERVER_DB_PATH": str(root / "server.db"),
            "MORPHX_SERVER_URL": self.server_url,
            "MORPHX_SERVER_HOST": "127.0.0.1",
            "MORPHX_SERVER_PORT": str(self.port),
            "MORPHX_DEVICE_ID": DEVICE_ID,
            "MORPHX_INTERVAL_SECONDS": "0.15",
            "MORPHX_SYNC_POLL_SECONDS": "0.05",
            "MORPHX_RETRY_BASE_SECONDS": "0.05",
            "MORPHX_RETRY_MAX_SECONDS": "0.2",
            "MORPHX_REQUEST_TIMEOUT_SECONDS": "0.5",
            "MORPHX_LOG_LEVEL": "WARNING",
        }
        self.agent: subprocess.Popen[bytes] | None = None
        self.server: subprocess.Popen[bytes] | None = None

    def _spawn(self, module: str) -> subprocess.Popen[bytes]:
        creation_flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        return subprocess.Popen(  # noqa: S603 - fixed interpreter and module arguments
            [sys.executable, "-m", module],
            env=self.env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creation_flags,
        )

    def start_agent(self) -> None:
        self.agent = self._spawn("morphx.agent")

    def start_server(self) -> None:
        self.server = self._spawn("morphx.server")
        wait_until(
            "server readiness",
            lambda: fetch_server_items(self.server_url) is not None,
            STARTUP_TIMEOUT_SECONDS,
        )

    @staticmethod
    def _stop(process: subprocess.Popen[bytes] | None, *, hard: bool) -> None:
        if process is None or process.poll() is not None:
            return
        if hard:
            process.kill()  # SIGKILL on POSIX, TerminateProcess on Windows
        else:
            process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    def kill_agent(self) -> None:
        self._stop(self.agent, hard=True)

    def stop_server(self) -> None:
        self._stop(self.server, hard=False)

    def close(self) -> None:
        self._stop(self.agent, hard=True)
        self._stop(self.server, hard=False)

    def outbox_size(self) -> int:
        return len(read_outbox(self.agent_db))

    def converged(self) -> bool:
        outbox = read_outbox(self.agent_db)
        items = fetch_server_items(self.server_url)
        if not outbox or items is None or not all(row.synced for row in outbox):
            return False
        return {row.event_id for row in outbox} == {item["event_id"] for item in items}

    def wait_for_growth(self, records: int) -> None:
        target = self.outbox_size() + records
        wait_until(
            f"{records} new acquisitions",
            lambda: self.outbox_size() >= target,
            STARTUP_TIMEOUT_SECONDS,
        )

    def wait_for_convergence(self, description: str) -> None:
        wait_until(description, self.converged, CONVERGE_TIMEOUT_SECONDS)


def assert_consistent(outbox: list[OutboxRow], items: list[dict[str, Any]]) -> None:
    sequences = [item["sequence"] for item in items]
    if sequences != list(range(1, len(sequences) + 1)):
        raise VerificationError("server sequences are not contiguous and ordered from 1")
    server_ids = [item["event_id"] for item in items]
    if len(server_ids) != len(set(server_ids)):
        raise VerificationError("server stored an event_id more than once")
    by_sequence = {row.sequence: row.event_id for row in outbox}
    for item in items:
        if by_sequence.get(item["sequence"]) != item["event_id"]:
            raise VerificationError(f"event_id mismatch at sequence {item['sequence']}")
    # The server may hold a record the agent had not yet marked synced when it was
    # killed (it will be acknowledged as a duplicate on restart); the converse is a loss.
    missing = {row.event_id for row in outbox if row.synced} - set(server_ids)
    if missing:
        raise VerificationError(f"{len(missing)} acknowledged events are missing on the server")


def run(root: Path) -> dict[str, Any]:
    harness = Harness(root)
    try:
        harness.start_agent()
        harness.wait_for_growth(OFFLINE_RECORDS)
        offline_records = harness.outbox_size()

        harness.start_server()
        harness.wait_for_convergence("offline backlog to drain after server start")

        harness.kill_agent()
        sequence_before_kill = max(row.sequence for row in read_outbox(harness.agent_db))
        harness.start_agent()
        harness.wait_for_growth(OFFLINE_RECORDS)
        harness.wait_for_convergence("sync to resume after agent kill -9")
        resumed_at = min(
            row.sequence
            for row in read_outbox(harness.agent_db)
            if row.sequence > sequence_before_kill
        )
        if resumed_at != sequence_before_kill + 1:
            raise VerificationError("sequence did not resume exactly after the hard kill")

        harness.stop_server()
        harness.wait_for_growth(OFFLINE_RECORDS)
        harness.start_server()
        harness.wait_for_convergence("backlog to drain after server restart")

        harness.kill_agent()
        outbox = read_outbox(harness.agent_db)
        items = fetch_server_items(harness.server_url)
        if items is None:
            raise VerificationError("server became unreachable during final verification")
        assert_consistent(outbox, items)
        return {
            "status": "passed",
            "offline_records": offline_records,
            "sequence_before_kill": sequence_before_kill,
            "server_records": len(items),
            "scenarios": ["server_down", "server_start", "agent_kill_9", "server_restart"],
        }
    finally:
        harness.close()


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="morphx-outage-") as temp_dir:
        try:
            result = run(Path(temp_dir))
        except VerificationError as exc:
            print(json.dumps({"status": "failed", "reason": str(exc)}))
            raise SystemExit(1) from exc
    print(json.dumps(result))


if __name__ == "__main__":
    main()
