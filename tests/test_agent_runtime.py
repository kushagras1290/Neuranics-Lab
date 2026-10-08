"""Cadence, supervision, bounded shutdown and process entry point of the device agent."""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import replace
from datetime import datetime

import pytest

import morphx.agent as agent_module
from morphx.agent_store import AgentStore
from morphx.errors import AgentTaskFailedError, DeviceIdentityError
from morphx.generator import MeasurementGenerator
from morphx.supervision import FailureBudget, supervise
from morphx.sync import SyncStats
from tests.test_sync import agent_settings


class FakeMonotonic:
    def __init__(self, values: list[float]) -> None:
        self._values = iter(values)
        self.last = 0.0

    def __call__(self) -> float:
        self.last = next(self._values, self.last)
        return self.last


@pytest.mark.parametrize(
    ("previous", "now", "expected"),
    [
        (0.0, 0.3, (5.0, 0)),  # on schedule: wait for the next fixed tick
        (0.0, 5.0, (5.0, 0)),  # exactly on the boundary
        (0.0, 7.0, (10.0, 1)),  # overran one tick: skip it, do not burst
        (0.0, 16.0, (20.0, 3)),
    ],
)
def test_next_deadline_is_fixed_rate(previous, now, expected) -> None:
    assert agent_module.next_deadline(previous, 5.0, now) == expected


@pytest.mark.anyio
async def test_acquisition_waits_against_fixed_schedule(temp_db, fixed_time, monkeypatch) -> None:
    settings = replace(agent_settings(temp_db), interval_seconds=5.0)
    store = AgentStore(temp_db, settings.device_id)
    store.initialize()
    stop = asyncio.Event()
    waits: list[float] = []
    # start=100; each acquisition then reads the clock twice (schedule, then wait).
    monotonic = FakeMonotonic([100.0, 100.4, 100.4, 105.9, 105.9, 117.0, 117.0])

    async def record_wait(_: asyncio.Event, timeout: float) -> None:
        waits.append(round(timeout, 6))
        if len(waits) == 3:
            stop.set()

    monkeypatch.setattr(agent_module, "_wait_or_timeout", record_wait)
    await agent_module.acquisition_loop(
        store,
        MeasurementGenerator(settings.device_id, seed=1),
        settings,
        stop,
        asyncio.Event(),
        clock=lambda: fixed_time,
        monotonic=monotonic,
    )

    # Work time (0.4s, 0.9s) is absorbed, and the 117s overrun skips the 115 tick.
    assert waits == [4.6, 4.1, 3.0]
    assert store.pending_count() == 3


@pytest.mark.anyio
async def test_acquisition_survives_a_brief_storage_fault(temp_db, monkeypatch) -> None:
    settings = agent_settings(temp_db)
    store = AgentStore(temp_db, settings.device_id)
    store.initialize()
    stop = asyncio.Event()
    wake = asyncio.Event()
    original = store.create_event
    calls = 0

    def flaky_create(factory, *, now: datetime):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise sqlite3.OperationalError("database is locked")
        if calls == 2:
            stop.set()
        return original(factory, now=now)

    monkeypatch.setattr(store, "create_event", flaky_create)
    await agent_module.acquisition_loop(
        store, MeasurementGenerator(settings.device_id, seed=1), settings, stop, wake
    )

    assert [event.sequence for event in store.all_events()] == [1]
    assert wake.is_set()


@pytest.mark.anyio
async def test_acquisition_gives_up_on_persistent_storage_fault(temp_db, monkeypatch) -> None:
    settings = agent_settings(temp_db)
    store = AgentStore(temp_db, settings.device_id)
    store.initialize()

    def full_disk(factory, *, now: datetime):
        raise sqlite3.OperationalError("database or disk is full")

    monkeypatch.setattr(store, "create_event", full_disk)
    with pytest.raises(sqlite3.OperationalError):
        await asyncio.wait_for(
            agent_module.acquisition_loop(
                store,
                MeasurementGenerator(settings.device_id, seed=1),
                settings,
                asyncio.Event(),
                asyncio.Event(),
            ),
            timeout=2,
        )


def test_failure_budget_requires_positive_limit() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        FailureBudget("x", limit=0)


@pytest.mark.anyio
async def test_supervise_raises_when_a_task_crashes() -> None:
    stop = asyncio.Event()
    sibling_cancelled = asyncio.Event()

    async def crashes() -> None:
        raise sqlite3.IntegrityError("constraint failed")

    async def runs_forever() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise

    with pytest.raises(AgentTaskFailedError, match="crashing task failed") as caught:
        await supervise({"crashing": crashes(), "steady": runs_forever()}, stop)

    assert isinstance(caught.value.__cause__, sqlite3.IntegrityError)
    assert sibling_cancelled.is_set()


@pytest.mark.anyio
async def test_supervise_raises_when_a_task_returns_early() -> None:
    async def returns() -> None:
        return None

    with pytest.raises(AgentTaskFailedError, match="exited before shutdown"):
        await supervise({"quitter": returns()}, asyncio.Event())


@pytest.mark.anyio
async def test_supervise_returns_cleanly_on_stop() -> None:
    stop = asyncio.Event()

    async def runs_forever() -> None:
        await asyncio.Event().wait()

    async def request_stop() -> None:
        await asyncio.sleep(0.01)
        stop.set()

    stopper = asyncio.create_task(request_stop())
    await asyncio.wait_for(supervise({"steady": runs_forever()}, stop), timeout=2)
    await stopper


@pytest.mark.anyio
async def test_supervise_lets_in_flight_work_finish_then_cancels_stragglers() -> None:
    stop = asyncio.Event()
    stop.set()
    finished = asyncio.Event()
    straggler_cancelled = asyncio.Event()

    async def cooperative() -> None:
        await stop.wait()
        await asyncio.sleep(0.01)  # e.g. a commit already running in a worker thread
        finished.set()

    async def straggler() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            straggler_cancelled.set()
            raise

    await asyncio.wait_for(
        supervise(
            {"cooperative": cooperative(), "straggler": straggler()},
            stop,
            shutdown_grace_seconds=1.0,
        ),
        timeout=5,
    )
    assert finished.is_set()
    assert straggler_cancelled.is_set()


@pytest.mark.anyio
async def test_run_agent_crashes_when_acquisition_hits_a_defect(temp_db, monkeypatch) -> None:
    settings = agent_settings(temp_db)

    def corrupt(self, factory, *, now: datetime):
        raise sqlite3.IntegrityError("UNIQUE constraint failed")

    monkeypatch.setattr(AgentStore, "create_event", corrupt)
    with pytest.raises(AgentTaskFailedError, match="measurement-acquisition"):
        await asyncio.wait_for(agent_module.run_agent(settings, asyncio.Event()), timeout=5)


class _SlowSync:
    async def sync_once(self) -> SyncStats:
        await asyncio.sleep(10)
        return SyncStats()


class _BrokenSync:
    async def sync_once(self) -> SyncStats:
        raise RuntimeError("boom")


@pytest.mark.anyio
async def test_final_sync_is_bounded(caplog) -> None:
    loop = asyncio.get_running_loop()
    started = loop.time()
    await agent_module.final_sync(_SlowSync(), timeout=0.05)  # type: ignore[arg-type]
    assert loop.time() - started < 1.0
    assert "final synchronization timed out" in caplog.text


@pytest.mark.anyio
async def test_final_sync_swallows_errors(caplog) -> None:
    await agent_module.final_sync(_BrokenSync(), timeout=1)  # type: ignore[arg-type]
    assert "final synchronization attempt failed" in caplog.text


@pytest.mark.anyio
async def test_signal_wrapper_passes_a_stop_event(temp_db, monkeypatch) -> None:
    received: list[asyncio.Event] = []

    async def fake_run_agent(_settings, stop_event: asyncio.Event) -> None:
        received.append(stop_event)

    monkeypatch.setattr(agent_module, "run_agent", fake_run_agent)
    await agent_module._run_with_signals(agent_settings(temp_db))
    assert len(received) == 1 and not received[0].is_set()


def _patch_main(monkeypatch, temp_db, outcome: BaseException | None) -> list[object]:
    calls: list[object] = []
    monkeypatch.setattr(agent_module, "parse_agent_args", lambda: agent_settings(temp_db))
    monkeypatch.setattr(agent_module, "configure_logging", lambda level: calls.append(level))

    def fake_run(coro) -> None:
        coro.close()
        calls.append("run")
        if outcome is not None:
            raise outcome

    monkeypatch.setattr(agent_module.asyncio, "run", fake_run)
    return calls


def test_main_runs_agent(monkeypatch, temp_db) -> None:
    calls = _patch_main(monkeypatch, temp_db, None)
    agent_module.main()
    assert calls == ["INFO", "run"]


def test_main_exits_non_zero_after_task_failure(monkeypatch, temp_db) -> None:
    _patch_main(monkeypatch, temp_db, AgentTaskFailedError("sync task failed"))
    with pytest.raises(SystemExit) as exited:
        agent_module.main()
    assert exited.value.code == agent_module.EXIT_TASK_FAILURE


def test_main_reports_identity_mismatch(monkeypatch, temp_db) -> None:
    _patch_main(monkeypatch, temp_db, DeviceIdentityError("belongs to another device"))
    with pytest.raises(SystemExit, match="Device identity error"):
        agent_module.main()


def test_main_reports_configuration_error(monkeypatch) -> None:
    monkeypatch.setenv("MORPHX_INTERVAL_SECONDS", "never")
    with pytest.raises(SystemExit, match="Configuration error"):
        agent_module.main()
