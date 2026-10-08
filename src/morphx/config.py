"""Strict environment and command-line configuration."""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Final
from urllib.parse import urlparse

from morphx.errors import ConfigurationError
from morphx.models import DEVICE_ID_PATTERN

__all__ = [
    "AgentSettings",
    "ConfigurationError",
    "ServerSettings",
    "parse_agent_args",
    "parse_server_args",
]

DEFAULT_SERVER_HOST: Final[str] = "127.0.0.1"
LOG_LEVELS: Final[frozenset[str]] = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default).strip()


def _float_env(name: str, default: float) -> float:
    raw = _env(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a number") from exc
    if value <= 0:
        raise ConfigurationError(f"{name} must be greater than zero")
    return value


def _int_env(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = _env(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
    return value


def _log_level(value: str) -> str:
    normalized = value.upper()
    if normalized not in LOG_LEVELS:
        raise ConfigurationError("MORPHX_LOG_LEVEL must be a standard Python log level")
    return normalized


def _is_valid_device_id(value: str) -> bool:
    return re.fullmatch(DEVICE_ID_PATTERN, value) is not None


def _validate_server_url(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigurationError("MORPHX_SERVER_URL must be an absolute HTTP(S) URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ConfigurationError(
            "MORPHX_SERVER_URL must not contain credentials, query, or fragment"
        )
    return value.rstrip("/")


@dataclass(frozen=True, slots=True)
class ServerSettings:
    db_path: Path
    host: str
    port: int
    api_key: str | None
    log_level: str

    @property
    def is_loopback_only(self) -> bool:
        return self.host in {"127.0.0.1", "::1", "localhost"}

    @classmethod
    def from_env(cls) -> ServerSettings:
        return cls(
            db_path=Path(_env("MORPHX_SERVER_DB_PATH", "./data/server.db")),
            host=_env("MORPHX_SERVER_HOST", DEFAULT_SERVER_HOST),
            port=_int_env("MORPHX_SERVER_PORT", 8000, minimum=1, maximum=65535),
            api_key=_env("MORPHX_API_KEY", "") or None,
            log_level=_log_level(_env("MORPHX_LOG_LEVEL", "INFO")),
        )


@dataclass(frozen=True, slots=True)
class AgentSettings:
    device_id: str
    db_path: Path
    server_url: str
    interval_seconds: float
    sync_poll_seconds: float
    sync_batch_size: int
    request_timeout_seconds: float
    retry_base_seconds: float
    retry_max_seconds: float
    api_key: str | None
    log_level: str
    random_seed: int | None = None
    shutdown_sync_seconds: float = 3.0

    @classmethod
    def from_env(cls) -> AgentSettings:
        device_id = _env("MORPHX_DEVICE_ID", "MORPHX_SIM_001")
        if not _is_valid_device_id(device_id):
            raise ConfigurationError(
                "MORPHX_DEVICE_ID must contain only letters, numbers, underscores, or hyphens"
            )
        retry_base = _float_env("MORPHX_RETRY_BASE_SECONDS", 1.0)
        retry_max = _float_env("MORPHX_RETRY_MAX_SECONDS", 60.0)
        if retry_max < retry_base:
            raise ConfigurationError(
                "MORPHX_RETRY_MAX_SECONDS must be greater than or equal to the base"
            )
        seed_raw = _env("MORPHX_RANDOM_SEED", "")
        try:
            seed = int(seed_raw) if seed_raw else None
        except ValueError as exc:
            raise ConfigurationError("MORPHX_RANDOM_SEED must be an integer") from exc
        return cls(
            device_id=device_id,
            db_path=Path(_env("MORPHX_AGENT_DB_PATH", "./data/agent.db")),
            server_url=_validate_server_url(_env("MORPHX_SERVER_URL", "http://127.0.0.1:8000")),
            interval_seconds=_float_env("MORPHX_INTERVAL_SECONDS", 5.0),
            sync_poll_seconds=_float_env("MORPHX_SYNC_POLL_SECONDS", 1.0),
            sync_batch_size=_int_env("MORPHX_SYNC_BATCH_SIZE", 100, minimum=1, maximum=1000),
            request_timeout_seconds=_float_env("MORPHX_REQUEST_TIMEOUT_SECONDS", 5.0),
            retry_base_seconds=retry_base,
            retry_max_seconds=retry_max,
            api_key=_env("MORPHX_API_KEY", "") or None,
            log_level=_log_level(_env("MORPHX_LOG_LEVEL", "INFO")),
            random_seed=seed,
            shutdown_sync_seconds=_float_env("MORPHX_SHUTDOWN_SYNC_SECONDS", 3.0),
        )


def parse_server_args(argv: list[str] | None = None) -> ServerSettings:
    base = ServerSettings.from_env()
    parser = argparse.ArgumentParser(description="Run the MorphX central server")
    parser.add_argument("--db-path", type=Path)
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    args = parser.parse_args(argv)
    if args.port is not None and not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    return replace(
        base,
        db_path=args.db_path or base.db_path,
        host=args.host or base.host,
        port=args.port or base.port,
    )


def parse_agent_args(argv: list[str] | None = None) -> AgentSettings:
    base = AgentSettings.from_env()
    parser = argparse.ArgumentParser(description="Run the MorphX device agent")
    parser.add_argument("--device-id")
    parser.add_argument("--db-path", type=Path)
    parser.add_argument("--server-url")
    parser.add_argument("--interval", type=float)
    args = parser.parse_args(argv)
    if args.interval is not None and args.interval <= 0:
        parser.error("--interval must be greater than zero")
    if args.device_id is not None and not _is_valid_device_id(args.device_id):
        parser.error("--device-id has an invalid format")
    return replace(
        base,
        device_id=args.device_id or base.device_id,
        db_path=args.db_path or base.db_path,
        server_url=_validate_server_url(args.server_url) if args.server_url else base.server_url,
        interval_seconds=args.interval or base.interval_seconds,
    )
