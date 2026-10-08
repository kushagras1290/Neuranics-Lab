"""Response classification, pass halting, quarantine and supervision of the sync loop."""

from __future__ import annotations

import asyncio
import json
import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import pytest

from morphx.agent_store import AgentStore
from morphx.models import MeasurementEvent
from morphx.sync import SyncService, SyncStats
from tests.test_sync import MutableClock, agent_settings

BASE_URL = "http://central.test"


def _ack(request: httpx.Request, received_at: datetime) -> httpx.Response:
    payload = json.loads(request.content)
    return httpx.Response(
        201,
        json={
            "event_id": payload["event_id"],
            "status": "created",
            "received_at": received_at.isoformat(),
        },
    )


def _store_with_events(
    temp_db: Path, fixed_time: datetime, event_factory, count: int
) -> tuple[AgentStore, list[MeasurementEvent]]:
    store = AgentStore(temp_db, "MORPHX_SIM_001")
    store.initialize()
    events = [store.create_event(event_factory, now=fixed_time) for _ in range(count)]
    return store, events


def _service(store: AgentStore, temp_db: Path, client: httpx.AsyncClient, clock) -> SyncService:
    return SyncService(store, agent_settings(temp_db), client, clock=clock, jitter=random.Random(0))


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=BASE_URL)


@pytest.mark.anyio
@pytest.mark.parametrize("status_code", [400, 409, 413, 422])
async def test_permanent_rejection_is_quarantined_not_retried(
    temp_db, fixed_time, event_factory, status_code
) -> None:
    store, events = _store_with_events(temp_db, fixed_time, event_factory, 1)
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            status_code, json={"detail": {"code": "SEQUENCE_CONFLICT", "message": "taken"}}
        )

    clock = MutableClock(fixed_time)
    async with _client(handler) as client:
        service = _service(store, temp_db, client, clock)
        stats = await service.sync_once()
        clock.now += timedelta(hours=1)
        again = await service.sync_once()

    row = store.get_outbox_row(events[0].event_id)
    assert stats.quarantined == 1
    assert again.attempted == 0
    assert calls == 1
    assert store.pending_count() == 0
    assert store.quarantined_count() == 1
    assert row is not None
    assert row["quarantined_at"] is not None
    assert row["last_error"] == "SEQUENCE_CONFLICT"


@pytest.mark.anyio
@pytest.mark.parametrize("body", ["conflict", "[1, 2]", '{"detail": "plain"}'])
async def test_rejection_without_structured_code_uses_status(
    temp_db, fixed_time, event_factory, body
) -> None:
    store, events = _store_with_events(temp_db, fixed_time, event_factory, 1)
    async with _client(lambda _: httpx.Response(409, text=body)) as client:
        await _service(store, temp_db, client, lambda: fixed_time).sync_once()

    row = store.get_outbox_row(events[0].event_id)
    assert row is not None
    assert row["last_error"] == "HTTP_409"


@pytest.mark.anyio
@pytest.mark.parametrize("status_code", [401, 403, 404])
async def test_configuration_failure_halts_pass_with_max_delay(
    temp_db, fixed_time, event_factory, status_code
) -> None:
    store, events = _store_with_events(temp_db, fixed_time, event_factory, 3)
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(status_code, json={"detail": {"code": "UNAUTHORIZED"}})

    async with _client(handler) as client:
        service = _service(store, temp_db, client, MutableClock(fixed_time))
        stats = await service.sync_once()
        paused = await service.sync_once()

    row = store.get_outbox_row(events[0].event_id)
    assert calls == 1
    assert stats.halted and stats.failed == 1
    assert paused.paused and paused.attempted == 0
    assert store.quarantined_count() == 0
    assert store.pending_count() == 3
    assert row is not None
    retry_max = agent_settings(temp_db).retry_max_seconds
    assert datetime.fromisoformat(row["next_attempt_at"]) == fixed_time + timedelta(
        seconds=retry_max
    )


@pytest.mark.anyio
async def test_unreachable_server_costs_one_attempt_per_pass(
    temp_db, fixed_time, event_factory
) -> None:
    store, _ = _store_with_events(temp_db, fixed_time, event_factory, 5)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ConnectTimeout("black hole", request=request)

    clock = MutableClock(fixed_time)
    async with _client(handler) as client:
        service = _service(store, temp_db, client, clock)
        first = await service.sync_once()
        assert (await service.sync_once()).paused
        clock.now += timedelta(seconds=5)
        await service.sync_once()

    assert first.attempted == 1 and first.halted
    assert calls == 2


@pytest.mark.anyio
async def test_isolated_server_error_does_not_block_later_records(
    temp_db, fixed_time, event_factory
) -> None:
    store, events = _store_with_events(temp_db, fixed_time, event_factory, 3)
    poisoned = str(events[0].event_id)

    def handler(request: httpx.Request) -> httpx.Response:
        if json.loads(request.content)["event_id"] == poisoned:
            return httpx.Response(500)
        return _ack(request, fixed_time)

    async with _client(handler) as client:
        stats = await _service(store, temp_db, client, lambda: fixed_time).sync_once()

    assert (stats.attempted, stats.succeeded, stats.failed, stats.halted) == (3, 2, 1, False)
    assert store.pending_count() == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("retry_after", "expected_delay"),
    [("0.8", 0.8), ("3600", 1.0), ("Wed, 21 Oct 2026 07:28:00 GMT", 0.1), ("-5", 0.1)],
)
async def test_retry_after_is_honoured_within_cap(
    temp_db, fixed_time, event_factory, retry_after, expected_delay
) -> None:
    store, events = _store_with_events(temp_db, fixed_time, event_factory, 2)

    class NoJitter(random.Random):
        def uniform(self, a: float, b: float) -> float:
            return 0.0

    handler = lambda _: httpx.Response(429, headers={"Retry-After": retry_after})  # noqa: E731
    async with _client(handler) as client:
        service = SyncService(
            store, agent_settings(temp_db), client, clock=lambda: fixed_time, jitter=NoJitter()
        )
        stats = await service.sync_once()

    row = store.get_outbox_row(events[0].event_id)
    assert stats.halted and stats.attempted == 1
    assert row is not None
    delay = datetime.fromisoformat(row["next_attempt_at"]) - fixed_time
    assert delay.total_seconds() == pytest.approx(expected_delay)


@pytest.mark.anyio
async def test_malformed_acknowledgement_is_retried(temp_db, fixed_time, event_factory) -> None:
    store, _ = _store_with_events(temp_db, fixed_time, event_factory, 1)
    async with _client(lambda _: httpx.Response(201, text="not json")) as client:
        stats = await _service(store, temp_db, client, lambda: fixed_time).sync_once()

    assert stats.failed == 1
    assert store.pending_count() == 1


@pytest.mark.anyio
async def test_non_transport_http_error_does_not_halt_pass(
    temp_db, fixed_time, event_factory
) -> None:
    store, _ = _store_with_events(temp_db, fixed_time, event_factory, 2)

    def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.DecodingError("garbled")

    async with _client(handler) as client:
        stats = await _service(store, temp_db, client, lambda: fixed_time).sync_once()

    assert (stats.attempted, stats.failed, stats.halted) == (2, 2, False)


@pytest.mark.anyio
async def test_sync_loop_absorbs_brief_storage_faults(temp_db, monkeypatch) -> None:
    store = AgentStore(temp_db, "MORPHX_SIM_001")
    store.initialize()
    stop = asyncio.Event()
    calls = 0

    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        service = SyncService(store, agent_settings(temp_db), client)

        async def flaky() -> SyncStats:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise sqlite3.OperationalError("database is locked")
            if calls >= 3:
                stop.set()
            return SyncStats()

        monkeypatch.setattr(service, "sync_once", flaky)
        await asyncio.wait_for(service.run(stop, asyncio.Event()), timeout=2)

    assert calls == 3


@pytest.mark.anyio
async def test_sync_loop_gives_up_on_persistent_storage_faults(temp_db, monkeypatch) -> None:
    store = AgentStore(temp_db, "MORPHX_SIM_001")
    store.initialize()

    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        service = SyncService(store, agent_settings(temp_db), client)

        async def broken() -> SyncStats:
            raise sqlite3.OperationalError("database or disk is full")

        monkeypatch.setattr(service, "sync_once", broken)
        with pytest.raises(sqlite3.OperationalError, match="disk is full"):
            await asyncio.wait_for(service.run(asyncio.Event(), asyncio.Event()), timeout=2)
