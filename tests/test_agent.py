from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import httpx
import pytest

import morphx.agent as agent_module
from morphx.agent_store import AgentStore
from morphx.generator import MeasurementGenerator
from tests.test_sync import agent_settings


@pytest.mark.anyio
async def test_acquisition_continues_without_a_server(temp_db, fixed_time, monkeypatch) -> None:
    settings = agent_settings(temp_db)
    store = AgentStore(temp_db, settings.device_id)
    store.initialize()
    stop = asyncio.Event()
    wake = asyncio.Event()
    ticks = iter(
        [
            fixed_time,
            fixed_time + timedelta(seconds=5),
            fixed_time + timedelta(seconds=10),
        ]
    )

    async def stop_after_three_waits(event: asyncio.Event, _: float) -> None:
        if store.pending_count() >= 3:
            stop.set()
        await asyncio.sleep(0)

    monkeypatch.setattr(agent_module, "_wait_or_timeout", stop_after_three_waits)
    await agent_module.acquisition_loop(
        store,
        MeasurementGenerator(settings.device_id, seed=1),
        settings,
        stop,
        wake,
        clock=lambda: next(ticks),
    )

    assert [event.sequence for event in store.all_events()] == [1, 2, 3]
    assert wake.is_set()


@pytest.mark.anyio
async def test_run_agent_coordinates_acquisition_and_sync(temp_db, fixed_time, monkeypatch) -> None:
    settings = agent_settings(temp_db)

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        return httpx.Response(
            201,
            json={
                "event_id": payload["event_id"],
                "status": "created",
                "received_at": fixed_time.isoformat(),
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=settings.server_url)
    monkeypatch.setattr(agent_module, "build_http_client", lambda _: client)
    stop = asyncio.Event()

    async def request_stop() -> None:
        await asyncio.sleep(0.05)
        stop.set()

    stopper = asyncio.create_task(request_stop())
    await agent_module.run_agent(settings, stop)
    await stopper

    store = AgentStore(temp_db, settings.device_id)
    store.initialize()
    assert len(store.all_events()) >= 1
    assert store.pending_count() == 0


def test_generator_is_repeatable_and_uses_explicit_units(fixed_time) -> None:
    left = MeasurementGenerator("MORPHX_SIM_001", seed=7).create(1, measured_at=fixed_time)
    right = MeasurementGenerator("MORPHX_SIM_001", seed=7).create(1, measured_at=fixed_time)

    assert left.measurements == right.measurements
    assert left.measurements.wbc.unit == "10^3/uL"
    assert left.measurements.rbc.unit == "10^6/uL"
    assert left.measurements.haemoglobin.unit == "g/dL"
