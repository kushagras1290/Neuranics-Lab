"""Small structured logging setup used by both processes."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any, Final

# Attributes every LogRecord carries; anything else was supplied through ``extra=``.
_STANDARD_RECORD_ATTRIBUTES: Final[frozenset[str]] = frozenset(
    logging.LogRecord("", logging.INFO, "", 0, "", None, None).__dict__
) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    """Emit one compact JSON object per log line, including structured extras."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            is_extra = key not in _STANDARD_RECORD_ATTRIBUTES and not key.startswith("_")
            if is_extra and value is not None:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=True, default=str)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
