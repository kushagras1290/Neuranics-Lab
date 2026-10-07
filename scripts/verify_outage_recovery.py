"""Black-box check: acquire offline, start server, and verify automatic recovery."""

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
from pathlib import Path
from typing import Any


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def pending_count(database: Path) -> int:
    if not database.exists():
        return 0
    connection = sqlite3.connect(database)
    try:
        row = connection.execute("SELECT COUNT(*) FROM outbox WHERE synced_at IS NULL").fetchone()
        return int(row[0]) if row else 0
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return 0
        raise
    finally:
        connection.close()


def get_json(url: str) -> dict[str, Any] | None:
    try:
        with urllib.request.urlopen(url, timeout=1) as response:  # noqa: S310
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return None


def stop(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def main() -> None:
    port = free_port()
    with tempfile.TemporaryDirectory(prefix="morphx-outage-") as temp_dir:
        root = Path(temp_dir)
        agent_db = root / "agent.db"
        server_db = root / "server.db"
        server_url = f"http://127.0.0.1:{port}"
        env = {
            **os.environ,
            "MORPHX_AGENT_DB_PATH": str(agent_db),
            "MORPHX_SERVER_DB_PATH": str(server_db),
            "MORPHX_SERVER_URL": server_url,
            "MORPHX_SERVER_HOST": "127.0.0.1",
            "MORPHX_SERVER_PORT": str(port),
            "MORPHX_INTERVAL_SECONDS": "0.15",
            "MORPHX_SYNC_POLL_SECONDS": "0.05",
            "MORPHX_RETRY_BASE_SECONDS": "0.05",
            "MORPHX_RETRY_MAX_SECONDS": "0.2",
            "MORPHX_REQUEST_TIMEOUT_SECONDS": "0.1",
            "MORPHX_LOG_LEVEL": "WARNING",
        }
        creation_flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        agent = subprocess.Popen(
            [sys.executable, "-m", "morphx.agent"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creation_flags,
        )
        server: subprocess.Popen[bytes] | None = None
        try:
            # Cold Windows starts can spend several seconds importing validation
            # and HTTP packages. The assertion is about behavior after startup.
            deadline = time.monotonic() + 20
            while pending_count(agent_db) < 3 and time.monotonic() < deadline:
                time.sleep(0.05)
            offline_count = pending_count(agent_db)
            if offline_count < 3:
                raise RuntimeError("agent did not keep acquiring while the server was offline")

            server = subprocess.Popen(
                [sys.executable, "-m", "morphx.server"],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=creation_flags,
            )
            deadline = time.monotonic() + 30
            synchronized: dict[str, Any] | None = None
            while time.monotonic() < deadline:
                synchronized = get_json(
                    f"{server_url}/v1/devices/MORPHX_SIM_001/measurements?limit=100"
                )
                if (
                    synchronized
                    and len(synchronized.get("items", [])) >= offline_count
                    and pending_count(agent_db) == 0
                ):
                    break
                time.sleep(0.1)
            if not synchronized or len(synchronized.get("items", [])) < offline_count:
                raise RuntimeError("offline backlog did not synchronize after recovery")
            sequences = [item["sequence"] for item in synchronized["items"]]
            if sequences != sorted(sequences) or sequences[:offline_count] != list(
                range(1, offline_count + 1)
            ):
                raise RuntimeError("server records were not returned in acquisition order")
            print(
                json.dumps(
                    {
                        "status": "passed",
                        "offline_records": offline_count,
                        "server_records": len(sequences),
                        "pending_after_recovery": pending_count(agent_db),
                    }
                )
            )
        finally:
            stop(agent)
            if server is not None:
                stop(server)


if __name__ == "__main__":
    main()
