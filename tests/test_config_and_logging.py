from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from morphx.config import (
    AgentSettings,
    ConfigurationError,
    ServerSettings,
    parse_agent_args,
    parse_server_args,
)
from morphx.logging_utils import JsonFormatter, configure_logging

MORPHX_ENV = [
    "MORPHX_API_KEY",
    "MORPHX_SERVER_DB_PATH",
    "MORPHX_SERVER_HOST",
    "MORPHX_SERVER_PORT",
    "MORPHX_DEVICE_ID",
    "MORPHX_AGENT_DB_PATH",
    "MORPHX_SERVER_URL",
    "MORPHX_INTERVAL_SECONDS",
    "MORPHX_SYNC_POLL_SECONDS",
    "MORPHX_SYNC_BATCH_SIZE",
    "MORPHX_REQUEST_TIMEOUT_SECONDS",
    "MORPHX_RETRY_BASE_SECONDS",
    "MORPHX_RETRY_MAX_SECONDS",
    "MORPHX_RANDOM_SEED",
    "MORPHX_LOG_LEVEL",
]


@pytest.fixture(autouse=True)
def clean_morphx_environment(monkeypatch) -> None:
    for name in MORPHX_ENV:
        monkeypatch.delenv(name, raising=False)


def test_default_settings_and_cli_overrides(tmp_path: Path) -> None:
    agent = AgentSettings.from_env()
    server = ServerSettings.from_env()
    assert agent.interval_seconds == 5.0
    assert agent.server_url == "http://127.0.0.1:8000"
    assert server.port == 8000

    agent_override = parse_agent_args(
        [
            "--device-id",
            "MORPHX_SIM_009",
            "--db-path",
            str(tmp_path / "agent.db"),
            "--server-url",
            "https://central.example.test/",
            "--interval",
            "0.25",
        ]
    )
    server_override = parse_server_args(
        ["--db-path", str(tmp_path / "server.db"), "--host", "127.0.0.1", "--port", "9000"]
    )
    assert agent_override.device_id == "MORPHX_SIM_009"
    assert agent_override.server_url == "https://central.example.test"
    assert agent_override.interval_seconds == 0.25
    assert server_override.host == "127.0.0.1"
    assert server_override.port == 9000


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("MORPHX_INTERVAL_SECONDS", "zero", "must be a number"),
        ("MORPHX_INTERVAL_SECONDS", "0", "greater than zero"),
        ("MORPHX_SYNC_BATCH_SIZE", "1001", "between 1 and 1000"),
        ("MORPHX_RANDOM_SEED", "seed", "must be an integer"),
        ("MORPHX_DEVICE_ID", "not valid", "letters, numbers"),
        ("MORPHX_LOG_LEVEL", "LOUD", "standard Python log level"),
        ("MORPHX_SERVER_URL", "ftp://example.test", "absolute HTTP"),
        ("MORPHX_SERVER_URL", "https://user:pass@example.test", "credentials"),
    ],
)
def test_invalid_agent_environment_fails_closed(monkeypatch, name, value, message) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ConfigurationError, match=message):
        AgentSettings.from_env()


def test_retry_max_cannot_be_less_than_base(monkeypatch) -> None:
    monkeypatch.setenv("MORPHX_RETRY_BASE_SECONDS", "10")
    monkeypatch.setenv("MORPHX_RETRY_MAX_SECONDS", "1")
    with pytest.raises(ConfigurationError, match="greater than or equal"):
        AgentSettings.from_env()


def test_cli_validation_errors() -> None:
    with pytest.raises(SystemExit):
        parse_agent_args(["--interval", "0"])
    with pytest.raises(SystemExit):
        parse_agent_args(["--device-id", "not valid"])
    with pytest.raises(SystemExit):
        parse_server_args(["--port", "0"])


def test_json_logging_is_structured_and_sanitized() -> None:
    record = logging.LogRecord(
        "morphx.test", logging.INFO, __file__, 1, "acquired %s", ("event",), None
    )
    record.event_id = "event-1"
    payload = json.loads(JsonFormatter().format(record))
    assert payload["message"] == "acquired event"
    assert payload["event_id"] == "event-1"
    assert payload["level"] == "INFO"

    configure_logging("WARNING")
    assert logging.getLogger().level == logging.WARNING
