"""FastAPI central server for idempotent measurement ingestion and retrieval."""

from __future__ import annotations

import hmac
import logging
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import uuid4

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status

from morphx.config import ConfigurationError, ServerSettings, parse_server_args
from morphx.logging_utils import configure_logging
from morphx.models import (
    HealthResponse,
    IngestResponse,
    IngestStatus,
    MeasurementEvent,
    MeasurementPage,
)
from morphx.server_store import IngestConflictError, ServerStore

LOGGER = logging.getLogger(__name__)
REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


def create_app(settings: ServerSettings | None = None) -> FastAPI:
    resolved = settings or ServerSettings.from_env()
    store = ServerStore(resolved.db_path)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        store.initialize()
        yield

    app = FastAPI(
        title="MorphX Central Measurement API",
        version="1.0.0",
        description="Durable and idempotent ingestion for simulated MorphX measurements.",
        lifespan=lifespan,
    )
    app.state.store = store

    @app.middleware("http")
    async def request_observability(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        supplied = request.headers.get("X-Request-ID", "")
        request_id = supplied if REQUEST_ID_PATTERN.fullmatch(supplied) else str(uuid4())
        try:
            response = await call_next(request)
        except Exception:
            LOGGER.exception(
                "request failed",
                extra={"request_id": request_id},
            )
            raise
        response.headers["X-Request-ID"] = request_id
        LOGGER.info(
            "request completed",
            extra={"request_id": request_id, "status_code": response.status_code},
        )
        return response

    def require_api_key(
        x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
    ) -> None:
        if resolved.api_key is None:
            return
        if x_api_key is None or not hmac.compare_digest(x_api_key, resolved.api_key):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"code": "UNAUTHORIZED", "message": "A valid API key is required"},
            )

    protected = Depends(require_api_key)

    @app.get("/health/live", response_model=HealthResponse, tags=["health"])
    def live() -> HealthResponse:
        return HealthResponse()

    @app.get(
        "/health/ready",
        response_model=HealthResponse,
        responses={503: {"description": "Storage is unavailable"}},
        tags=["health"],
    )
    def ready() -> HealthResponse:
        if not store.is_ready():
            raise HTTPException(status_code=503, detail="storage unavailable")
        return HealthResponse()

    @app.post(
        "/v1/measurements",
        response_model=IngestResponse,
        status_code=status.HTTP_201_CREATED,
        responses={
            200: {"model": IngestResponse, "description": "Idempotent duplicate"},
            409: {"description": "Idempotency or sequence conflict"},
        },
        dependencies=[protected],
        tags=["measurements"],
    )
    def ingest(event: MeasurementEvent, response: Response) -> IngestResponse:
        try:
            result = store.ingest(event)
        except IngestConflictError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"code": "MEASUREMENT_CONFLICT", "message": str(exc)},
            ) from exc
        ingest_status = IngestStatus.CREATED if result.created else IngestStatus.DUPLICATE
        response.status_code = status.HTTP_201_CREATED if result.created else status.HTTP_200_OK
        return IngestResponse(
            event_id=result.measurement.event_id,
            status=ingest_status,
            received_at=result.measurement.received_at,
        )

    @app.get(
        "/v1/devices/{device_id}/measurements",
        response_model=MeasurementPage,
        dependencies=[protected],
        tags=["measurements"],
    )
    def list_measurements(
        device_id: str,
        after_sequence: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> MeasurementPage:
        if re.fullmatch(r"[A-Za-z0-9_-]{1,64}", device_id) is None:
            raise HTTPException(status_code=422, detail="invalid device_id")
        items, next_after = store.list_for_device(
            device_id, after_sequence=after_sequence, limit=limit
        )
        return MeasurementPage(
            device_id=device_id,
            items=items,
            next_after_sequence=next_after,
        )

    return app


def main() -> None:
    try:
        settings = parse_server_args()
        configure_logging(settings.log_level)
        uvicorn.run(
            create_app(settings),
            host=settings.host,
            port=settings.port,
            log_config=None,
            access_log=False,
        )
    except ConfigurationError as exc:
        raise SystemExit(f"Configuration error: {exc}") from exc


if __name__ == "__main__":
    main()
