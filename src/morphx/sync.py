"""Background at-least-once delivery with idempotent server acknowledgements."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

import httpx

from morphx.agent_store import AgentStore, PendingEvent
from morphx.config import AgentSettings
from morphx.models import IngestResponse

LOGGER = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class SyncStats:
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0


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

    def _retry_delay(self, pending: PendingEvent) -> float:
        exponent = min(pending.attempt_count, 16)
        base = min(
            self.settings.retry_max_seconds,
            self.settings.retry_base_seconds * (2**exponent),
        )
        return cast(
            float,
            min(
                self.settings.retry_max_seconds,
                base + self.jitter.uniform(0.0, base * 0.1),
            ),
        )

    async def sync_once(self) -> SyncStats:
        now = self.clock()
        pending_events = self.store.due_events(now=now, limit=self.settings.sync_batch_size)
        succeeded = 0
        failed = 0
        for pending in pending_events:
            event = pending.event
            try:
                response = await self.client.post(
                    "/v1/measurements",
                    json=event.model_dump(mode="json"),
                )
                response.raise_for_status()
                acknowledgement = IngestResponse.model_validate(response.json())
                if acknowledgement.event_id != event.event_id:
                    raise ValueError("server acknowledgement event_id did not match")
                self.store.mark_synced(event.event_id, synced_at=self.clock())
                succeeded += 1
                LOGGER.info(
                    "measurement synchronized",
                    extra={
                        "event_id": str(event.event_id),
                        "device_id": event.device_id,
                        "sequence": event.sequence,
                        "status_code": response.status_code,
                    },
                )
            except (httpx.HTTPError, ValueError) as exc:
                delay = self._retry_delay(pending)
                self.store.mark_failed(
                    event.event_id,
                    error=f"{type(exc).__name__}: {exc}",
                    next_attempt_at=self.clock() + timedelta(seconds=delay),
                )
                failed += 1
                LOGGER.warning(
                    "measurement synchronization deferred",
                    extra={
                        "event_id": str(event.event_id),
                        "device_id": event.device_id,
                        "sequence": event.sequence,
                    },
                )
        return SyncStats(attempted=len(pending_events), succeeded=succeeded, failed=failed)

    async def run(self, stop_event: asyncio.Event, wake_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await self.sync_once()
            try:
                await asyncio.wait_for(wake_event.wait(), timeout=self.settings.sync_poll_seconds)
                wake_event.clear()
            except TimeoutError:
                pass


def build_http_client(settings: AgentSettings) -> httpx.AsyncClient:
    headers = {"User-Agent": "morphx-agent/1.0"}
    if settings.api_key:
        headers["X-API-Key"] = settings.api_key
    return httpx.AsyncClient(
        base_url=settings.server_url,
        headers=headers,
        timeout=httpx.Timeout(settings.request_timeout_seconds),
        limits=httpx.Limits(max_connections=5, max_keepalive_connections=2),
    )
