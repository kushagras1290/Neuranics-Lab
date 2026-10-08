"""MorphX device process: acquire durably first, then sync in the background."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import signal
import time
from collections.abc import Callable
from datetime import UTC, datetime

from morphx.agent_store import AgentStore
from morphx.config import AgentSettings, ConfigurationError, parse_agent_args
from morphx.errors import AgentTaskFailedError, DeviceIdentityError
from morphx.generator import MeasurementGenerator
from morphx.logging_utils import configure_logging
from morphx.models import MeasurementEvent
from morphx.supervision import RECOVERABLE_STORAGE_ERRORS, FailureBudget, supervise
from morphx.sync import SyncService, build_http_client

LOGGER = logging.getLogger(__name__)

EXIT_TASK_FAILURE = 1


async def _wait_or_timeout(event: asyncio.Event, timeout: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(event.wait(), timeout=max(0.0, timeout))


async def acquire_once(
    store: AgentStore, generator: MeasurementGenerator, *, now: datetime
) -> MeasurementEvent:
    """Allocate a sequence and persist one measurement off the event loop."""

    def create_for_sequence(sequence: int) -> MeasurementEvent:
        return generator.create(sequence, measured_at=now)

    event = await asyncio.to_thread(store.create_event, create_for_sequence, now=now)
    LOGGER.info(
        "measurement acquired and persisted",
        extra={
            "event_id": str(event.event_id),
            "device_id": event.device_id,
            "sequence": event.sequence,
        },
    )
    return event


def next_deadline(previous: float, interval: float, now: float) -> tuple[float, int]:
    """Advance a fixed-rate schedule; return the next deadline and ticks skipped.

    Deadlines are ``start + n * interval`` so work duration never accumulates as drift.
    If acquisition overran one or more whole ticks they are skipped rather than burst.
    """
    deadline = previous + interval
    if now <= deadline:
        return deadline, 0
    skipped = math.ceil((now - deadline) / interval)
    return deadline + skipped * interval, skipped


async def acquisition_loop(
    store: AgentStore,
    generator: MeasurementGenerator,
    settings: AgentSettings,
    stop_event: asyncio.Event,
    wake_event: asyncio.Event,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    monotonic: Callable[[], float] = time.monotonic,
) -> None:
    budget = FailureBudget("acquisition")
    deadline = monotonic()
    while not stop_event.is_set():
        try:
            await acquire_once(store, generator, now=clock())
        except RECOVERABLE_STORAGE_ERRORS as exc:
            if budget.exhausted_after(exc):
                raise
        else:
            budget.record_success()
            wake_event.set()
        deadline, skipped = next_deadline(deadline, settings.interval_seconds, monotonic())
        if skipped:
            LOGGER.warning(
                "acquisition overran its interval; skipped ticks",
                extra={"skipped_ticks": skipped, "interval_seconds": settings.interval_seconds},
            )
        await _wait_or_timeout(stop_event, deadline - monotonic())


async def final_sync(synchronizer: SyncService, timeout: float) -> None:
    """Best-effort flush on shutdown, bounded so it fits the service stop grace period."""
    try:
        stats = await asyncio.wait_for(synchronizer.sync_once(), timeout=timeout)
    except TimeoutError:
        LOGGER.warning(
            "final synchronization timed out; backlog stays in the outbox",
            extra={"timeout_seconds": timeout},
        )
    except Exception:
        # The outbox remains authoritative and is retried after the next start.
        LOGGER.exception("final synchronization attempt failed")
    else:
        LOGGER.info(
            "final synchronization finished",
            extra={"attempted": stats.attempted, "succeeded": stats.succeeded},
        )


async def run_agent(settings: AgentSettings, stop_event: asyncio.Event | None = None) -> None:
    stop = stop_event or asyncio.Event()
    wake = asyncio.Event()
    store = AgentStore(settings.db_path, settings.device_id)
    await asyncio.to_thread(store.initialize)
    generator = MeasurementGenerator(settings.device_id, seed=settings.random_seed)
    LOGGER.info(
        "device agent started",
        extra={
            "device_id": settings.device_id,
            "pending": await asyncio.to_thread(store.pending_count),
            "quarantined": await asyncio.to_thread(store.quarantined_count),
        },
    )

    async with build_http_client(settings) as client:
        synchronizer = SyncService(store, settings, client)
        try:
            await supervise(
                {
                    "measurement-acquisition": acquisition_loop(
                        store, generator, settings, stop, wake
                    ),
                    "measurement-synchronization": synchronizer.run(stop, wake),
                },
                stop,
            )
        finally:
            await final_sync(synchronizer, settings.shutdown_sync_seconds)


async def _run_with_signals(settings: AgentSettings) -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_name in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # Windows Proactor event loop
            loop.add_signal_handler(signal_name, stop_event.set)
    await run_agent(settings, stop_event)


def main() -> None:
    try:
        settings = parse_agent_args()
    except ConfigurationError as exc:
        raise SystemExit(f"Configuration error: {exc}") from exc
    configure_logging(settings.log_level)
    try:
        asyncio.run(_run_with_signals(settings))
    except DeviceIdentityError as exc:
        raise SystemExit(f"Device identity error: {exc}") from exc
    except AgentTaskFailedError:
        LOGGER.critical("device agent stopping after task failure", exc_info=True)
        raise SystemExit(EXIT_TASK_FAILURE) from None
    except KeyboardInterrupt:
        LOGGER.info("device agent stopped")
    else:
        LOGGER.info("device agent stopped")


if __name__ == "__main__":
    main()
