"""FastAPI central server for idempotent measurement ingestion and retrieval."""

from __future__ import annotations

import hmac
import logging
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Final
from uuid import uuid4

import uvicorn
from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Path,
    Query,
    Request,
    Response,
    status,
)

from morphx import __version__
from morphx.config import ConfigurationError, ServerSettings, parse_server_args
from morphx.errors import IngestConflictError
from morphx.logging_utils import configure_logging
from morphx.models import (
    DEVICE_ID_PATTERN,
    DeviceList,
    HealthResponse,
    IngestResponse,
    IngestStatus,
    MeasurementEvent,
    MeasurementPage,
)
from morphx.server_store import MeasurementFilter, ServerStore

LOGGER = logging.getLogger(__name__)
REQUEST_ID_PATTERN: Final = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
MAX_PAGE_SIZE: Final[int] = 500
DEFAULT_PAGE_SIZE: Final[int] = 100

RequestHandler = Callable[[Request], Awaitable[Response]]
DevicePathId = Annotated[str, Path(pattern=DEVICE_ID_PATTERN)]


async def request_observability(request: Request, call_next: RequestHandler) -> Response:
    """Propagate or mint an X-Request-ID and emit one structured line per request."""
    supplied = request.headers.get("X-Request-ID", "")
    request_id = supplied if REQUEST_ID_PATTERN.fullmatch(supplied) else str(uuid4())
    try:
        response = await call_next(request)
    except Exception:
        LOGGER.exception("request failed", extra={"request_id": request_id})
        raise
    response.headers["X-Request-ID"] = request_id
    LOGGER.info(
        "request completed",
        extra={
            "request_id": request_id,
            "status_code": response.status_code,
            "method": request.method,
            "path": request.url.path,
        },
    )
    return response


def build_api_key_dependency(expected: str | None) -> Callable[[str | None], None]:
    """Return a dependency that enforces the shared key only when one is configured."""

    def require_api_key(
        x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
    ) -> None:
        if expected is None:
            return
        if x_api_key is None or not hmac.compare_digest(x_api_key, expected):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"code": "UNAUTHORIZED", "message": "A valid API key is required"},
            )

    return require_api_key


def create_app(settings: ServerSettings | None = None) -> FastAPI:
    resolved = settings or ServerSettings.from_env()
    store = ServerStore(resolved.db_path)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        store.initialize()
        yield

    app = FastAPI(
        title="MorphX Central Measurement API",
        version=__version__,
        description="Durable and idempotent ingestion for simulated MorphX measurements.",
        lifespan=lifespan,
    )
    app.state.store = store
    app.middleware("http")(request_observability)
    protected = Depends(build_api_key_dependency(resolved.api_key))

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
            409: {"description": "EVENT_ID_CONFLICT or SEQUENCE_CONFLICT"},
        },
        dependencies=[protected],
        tags=["measurements"],
    )
    def ingest(event: MeasurementEvent, response: Response) -> IngestResponse:
        try:
            result = store.ingest(event)
        except IngestConflictError as exc:
            LOGGER.warning(
                "measurement rejected",
                extra={
                    "event_id": str(event.event_id),
                    "device_id": event.device_id,
                    "sequence": event.sequence,
                    "error_code": exc.code,
                },
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"code": exc.code, "message": str(exc)},
            ) from exc
        response.status_code = status.HTTP_201_CREATED if result.created else status.HTTP_200_OK
        return IngestResponse(
            event_id=result.measurement.event_id,
            status=IngestStatus.CREATED if result.created else IngestStatus.DUPLICATE,
            received_at=result.measurement.received_at,
        )

    @app.get(
        "/v1/devices",
        response_model=DeviceList,
        dependencies=[protected],
        tags=["measurements"],
    )
    def list_devices() -> DeviceList:
        return DeviceList(items=store.list_devices())

    @app.get(
        "/v1/devices/{device_id}/measurements",
        response_model=MeasurementPage,
        dependencies=[protected],
        tags=["measurements"],
    )
    def list_measurements(
        device_id: DevicePathId,
        after_sequence: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
        measured_from: Annotated[datetime | None, Query()] = None,
        measured_to: Annotated[datetime | None, Query()] = None,
    ) -> MeasurementPage:
        for name, value in (("measured_from", measured_from), ("measured_to", measured_to)):
            if value is not None and value.utcoffset() is None:
                raise HTTPException(status_code=422, detail=f"{name} must include a timezone")
        if measured_from and measured_to and measured_from > measured_to:
            raise HTTPException(
                status_code=422, detail="measured_from must not be after measured_to"
            )
        items, next_after = store.list_for_device(
            device_id,
            MeasurementFilter(
                after_sequence=after_sequence,
                limit=limit,
                measured_from=measured_from,
                measured_to=measured_to,
            ),
        )
        return MeasurementPage(device_id=device_id, items=items, next_after_sequence=next_after)

    return app


def warn_if_exposed_without_auth(settings: ServerSettings) -> None:
    if settings.api_key is None and not settings.is_loopback_only:
        LOGGER.warning(
            "server is listening on a non-loopback address without MORPHX_API_KEY; "
            "any host that can reach it may write measurements",
            extra={"host": settings.host, "port": settings.port},
        )


def main() -> None:
    try:
        settings = parse_server_args()
    except ConfigurationError as exc:
        raise SystemExit(f"Configuration error: {exc}") from exc
    configure_logging(settings.log_level)
    warn_if_exposed_without_auth(settings)
    uvicorn.run(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        log_config=None,
        access_log=False,
    )


if __name__ == "__main__":
    main()
