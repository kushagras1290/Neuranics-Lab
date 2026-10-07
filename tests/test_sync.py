from __future__ import annotations

import asyncio
import json
import random
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import pytest

from morphx.agent_store import AgentStore
from morphx.config import AgentSettings
from morphx.sync import SyncService, build_http_client


class MutableClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def agent_settings(db_path: Path) -> AgentSettings:
    return AgentSettings(
        device_id="MORPHX_SIM_001",
        db_path=db_path,
        server_url="http://central.test",
        interval_seconds=0.01,
        sync_poll_seconds=0.01,
        sync_batch_size=10,
        request_timeout_seconds=0.1,
        retry_base_seconds=0.1,
        retry_max_seconds=1.0,
        api_key="test-secret",
        log_level="INFO",
        random_seed=42,
    )


@pytest.mark.anyio
async def test_offline_event_retries_and_synchronizes(temp_db, fixed_time, event_factory) -> None:
    store = AgentStore(temp_db, "MORPHX_SIM_001")
    store.initialize()
    event = store.create_event(event_factory, now=fixed_time)
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectError("network offline\nretry later", request=request)
        payload = json.loads(request.content)
        return httpx.Response(
            201,
            json={
                "event_id": payload["event_id"],
                "status": "created",
                "received_at": fixed_time.isoformat(),
            },
        )

    clock = MutableClock(fixed_time)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://central.test"
    ) as client:
        service = SyncService(
            store,
            agent_settings(temp_db),
            client,
            clock=clock,
            jitter=random.Random(0),
        )
        failed = await service.sync_once()
        row = store.get_outbox_row(event.event_id)
        assert failed.failed == 1
        assert row is not None
        assert row["attempt_count"] == 1
        assert "\n" not in row["last_error"]
        assert store.pending_count() == 1

        clock.now += timedelta(seconds=2)
        recovered = await service.sync_once()

    assert recovered.succeeded == 1
    assert store.pending_count() == 0


@pytest.mark.anyio
async def test_bad_acknowledgement_stays_pending(temp_db, fixed_time, event_factory) -> None:
    store = AgentStore(temp_db, "MORPHX_SIM_001")
    store.initialize()
    store.create_event(event_factory, now=fixed_time)

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "event_id": "00000000-0000-0000-0000-000000000099",
                "status": "duplicate",
                "received_at": fixed_time.isoformat(),
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://central.test"
    ) as client:
        result = await SyncService(
            store,
            agent_settings(temp_db),
            client,
            clock=lambda: fixed_time,
            jitter=random.Random(0),
        ).sync_once()

    assert result.failed == 1
    assert store.pending_count() == 1


@pytest.mark.anyio
async def test_sync_loop_wakes_and_stops(temp_db, fixed_time) -> None:
    store = AgentStore(temp_db, "MORPHX_SIM_001")
    store.initialize()
    settings = agent_settings(temp_db)
    stop = asyncio.Event()
    wake = asyncio.Event()
    wake.set()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(500)),
        base_url="http://central.test",
    ) as client:
        service = SyncService(store, settings, client, clock=lambda: fixed_time)
        task = asyncio.create_task(service.run(stop, wake))
        await asyncio.sleep(0.02)
        stop.set()
        await asyncio.wait_for(task, timeout=1)

    assert store.pending_count() == 0


@pytest.mark.anyio
async def test_http_client_includes_optional_api_key(temp_db) -> None:
    settings = agent_settings(temp_db)
    async with build_http_client(settings) as client:
        assert client.headers["X-API-Key"] == "test-secret"

    no_key = replace(settings, api_key=None)
    async with build_http_client(no_key) as client:
        assert "X-API-Key" not in client.headers
