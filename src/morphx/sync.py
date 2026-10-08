"""Background at-least-once delivery with idempotent server acknowledgements.

Each server response is classified before the outbox is updated:

* ``DELIVERED`` - valid acknowledgement for the same ``event_id``; mark synced.
* ``REJECTED``  - 400/409/413/422: the record itself is unacceptable and retrying the
  identical bytes can never succeed, so it is quarantined (kept locally, not retried).
* ``BLOCKED``   - 401/403/404/405: configuration is wrong (key, URL). Every record would
  fail the same way, so the pass stops and waits the maximum retry delay.
* ``TRANSIENT`` - transport errors, timeouts, 408/429/5xx, bad acknowledgements: retry
  with capped exponential backoff. Transport failures and overload statuses also stop
  the pass, so a black-holed server costs one timeout per pass rather than one per
  pending record.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Final

import httpx
from pydantic import ValidationError

from morphx import __version__
from morphx.agent_store import AgentStore, PendingEvent
from morphx.config import AgentSettings
from morphx.errors import AcknowledgementError
from morphx.models import IngestResponse, MeasurementEvent
from morphx.supervision import RECOVERABLE_STORAGE_ERRORS, FailureBudget, wait_for_either

LOGGER = logging.getLogger(__name__)

INGEST_PATH: Final[str] = "/v1/measurements"
REJECTED_STATUSES: Final[frozenset[int]] = frozenset({400, 409, 413, 422})
BLOCKED_STATUSES: Final[frozenset[int]] = frozenset({401, 403, 404, 405})
OVERLOADED_STATUSES: Final[frozenset[int]] = frozenset({429, 502, 503, 504})
RETRY_AFTER_STATUSES: Final[frozenset[int]] = frozenset({429, 503})
MAX_BACKOFF_EXPONENT: Final[int] = 16
JITTER_FRACTION: Final[float] = 0.1
MAX_CONNECTIONS: Final[int] = 5
MAX_KEEPALIVE_CONNECTIONS: Final[int] = 2


def utc_now() -> datetime:
    return datetime.now(UTC)


class DeliveryOutcome(StrEnum):
    DELIVERED = "delivered"
    REJECTED = "rejected"
    BLOCKED = "blocked"
    TRANSIENT = "transient"


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    outcome: DeliveryOutcome
    detail: str
    status_code: int | None = None
    halt_pass: bool = False
    retry_after_seconds: float | None = None


@dataclass(frozen=True, slots=True)
class SyncStats:
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    quarantined: int = 0
    halted: bool = False
    paused: bool = False


def _error_code(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return f"HTTP_{response.status_code}"
    detail = body.get("detail") if isinstance(body, dict) else None
    if isinstance(detail, dict) and isinstance(detail.get("code"), str):
        return str(detail["code"])
    return f"HTTP_{response.status_code}"


def _retry_after_seconds(response: httpx.Response) -> float | None:
    raw = response.headers.get("Retry-After", "").strip()
    try:
        value = float(raw)
    except ValueError:
        return None  # HTTP-date form or absent: fall back to computed backoff.
    return value if value >= 0 else None


def parse_acknowledgement(response: httpx.Response, event: MeasurementEvent) -> IngestResponse:
    try:
        acknowledgement = IngestResponse.model_validate_json(response.content)
    except ValidationError as exc:
        raise AcknowledgementError("server acknowledgement was malformed") from exc
    if acknowledgement.event_id != event.event_id:
        raise AcknowledgementError("server acknowledgement event_id did not match")
    return acknowledgement


def classify_response(response: httpx.Response, event: MeasurementEvent) -> DeliveryResult:
    status_code = response.status_code
    if response.is_success:
        try:
            parse_acknowledgement(response, event)
        except AcknowledgementError as exc:
            return DeliveryResult(
                DeliveryOutcome.TRANSIENT, f"AcknowledgementError: {exc}", status_code
            )
        return DeliveryResult(DeliveryOutcome.DELIVERED, "acknowledged", status_code)

    detail = _error_code(response)
    if status_code in REJECTED_STATUSES:
        return DeliveryResult(DeliveryOutcome.REJECTED, detail, status_code)
    if status_code in BLOCKED_STATUSES:
        return DeliveryResult(DeliveryOutcome.BLOCKED, detail, status_code, halt_pass=True)
    return DeliveryResult(
        DeliveryOutcome.TRANSIENT,
        detail,
        status_code,
        halt_pass=status_code in OVERLOADED_STATUSES,
        retry_after_seconds=(
            _retry_after_seconds(response) if status_code in RETRY_AFTER_STATUSES else None
        ),
    )


class SyncService:
    """Synchronize due outbox records without owning acquisition."""

    def __init__(
        self,
        store: AgentStore,
        settings: AgentSettings,
        client: httpx.AsyncClient,
        *,
        clock: Callable[[], datetime] = utc_now,
        jitter: random.Random | None = None,
    ) -> None:
        self.store = store
        self.settings = settings
        self.client = client
        self.clock = clock
        self.jitter = jitter or random.Random()  # noqa: S311 - retry jitter only
        self.paused_until: datetime | None = None

    def _retry_delay(self, pending: PendingEvent, result: DeliveryResult) -> float:
        cap = self.settings.retry_max_seconds
        if result.outcome is DeliveryOutcome.BLOCKED:
            return cap
        exponent = min(pending.attempt_count, MAX_BACKOFF_EXPONENT)
        base = min(cap, self.settings.retry_base_seconds * float(2**exponent))
        delay = min(cap, base + self.jitter.uniform(0.0, base * JITTER_FRACTION))
        if result.retry_after_seconds is not None:
            delay = max(delay, min(cap, result.retry_after_seconds))
        return delay

    async def deliver(self, event: MeasurementEvent) -> DeliveryResult:
        try:
            response = await self.client.post(INGEST_PATH, json=event.model_dump(mode="json"))
        except httpx.TransportError as exc:
            return DeliveryResult(
                DeliveryOutcome.TRANSIENT, f"{type(exc).__name__}: {exc}", halt_pass=True
            )
        except httpx.HTTPError as exc:
            return DeliveryResult(DeliveryOutcome.TRANSIENT, f"{type(exc).__name__}: {exc}")
        return classify_response(response, event)

    async def _record(self, pending: PendingEvent, result: DeliveryResult) -> float | None:
        """Persist the outcome; return the retry delay when the record stays pending."""
        event = pending.event
        context = {
            "event_id": str(event.event_id),
            "device_id": event.device_id,
            "sequence": event.sequence,
            "status_code": result.status_code,
            "outcome": result.outcome.value,
        }
        now = self.clock()
        if result.outcome is DeliveryOutcome.DELIVERED:
            await asyncio.to_thread(self.store.mark_synced, event.event_id, synced_at=now)
            LOGGER.info("measurement synchronized", extra=context)
            return None
        if result.outcome is DeliveryOutcome.REJECTED:
            await asyncio.to_thread(
                self.store.mark_quarantined,
                event.event_id,
                error=result.detail,
                quarantined_at=now,
            )
            LOGGER.error(
                "measurement permanently rejected by server and quarantined",
                extra={**context, "error_code": result.detail},
            )
            return None

        delay = self._retry_delay(pending, result)
        await asyncio.to_thread(
            self.store.mark_failed,
            event.event_id,
            error=result.detail,
            next_attempt_at=now + timedelta(seconds=delay),
        )
        log = LOGGER.error if result.outcome is DeliveryOutcome.BLOCKED else LOGGER.warning
        log(
            "measurement synchronization deferred",
            extra={**context, "error_code": result.detail, "delay_seconds": round(delay, 3)},
        )
        return delay

    async def sync_once(self) -> SyncStats:
        now = self.clock()
        if self.paused_until is not None and now < self.paused_until:
            return SyncStats(paused=True)
        self.paused_until = None

        pending_events = await asyncio.to_thread(
            self.store.due_events, now=now, limit=self.settings.sync_batch_size
        )
        attempted = succeeded = failed = quarantined = 0
        halted = False
        for pending in pending_events:
            result = await self.deliver(pending.event)
            attempted += 1
            delay = await self._record(pending, result)
            if result.outcome is DeliveryOutcome.DELIVERED:
                succeeded += 1
            elif result.outcome is DeliveryOutcome.REJECTED:
                quarantined += 1
            else:
                failed += 1
            if result.halt_pass and delay is not None:
                self.paused_until = self.clock() + timedelta(seconds=delay)
                halted = True
                break
        return SyncStats(
            attempted=attempted,
            succeeded=succeeded,
            failed=failed,
            quarantined=quarantined,
            halted=halted,
        )

    async def run(self, stop_event: asyncio.Event, wake_event: asyncio.Event) -> None:
        budget = FailureBudget("synchronization")
        while not stop_event.is_set():
            try:
                await self.sync_once()
            except RECOVERABLE_STORAGE_ERRORS as exc:
                if budget.exhausted_after(exc):
                    raise
            else:
                budget.record_success()
            await wait_for_either(wake_event, stop_event, self.settings.sync_poll_seconds)
            wake_event.clear()


def build_http_client(settings: AgentSettings) -> httpx.AsyncClient:
    headers = {"User-Agent": f"morphx-agent/{__version__}"}
    if settings.api_key:
        headers["X-API-Key"] = settings.api_key
    return httpx.AsyncClient(
        base_url=settings.server_url,
        headers=headers,
        timeout=httpx.Timeout(settings.request_timeout_seconds),
        limits=httpx.Limits(
            max_connections=MAX_CONNECTIONS,
            max_keepalive_connections=MAX_KEEPALIVE_CONNECTIONS,
        ),
    )
