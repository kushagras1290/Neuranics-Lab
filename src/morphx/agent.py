"""MorphX device process: acquire durably first, then sync in the background."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from collections.abc import Callable
from datetime import UTC, datetime

from morphx.agent_store import AgentStore
from morphx.config import AgentSettings, ConfigurationError, parse_agent_args
from morphx.generator import MeasurementGenerator
from morphx.logging_utils import configure_logging
from morphx.models import MeasurementEvent
from morphx.sync import SyncService, build_http_client

LOGGER = logging.getLogger(__name__)


async def _wait_or_timeout(event: asyncio.Event, timeout: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(event.wait(), timeout=timeout)


async def acquisition_loop(
    store: AgentStore,
    generator: MeasurementGenerator,
    settings: AgentSettings,
    stop_event: asyncio.Event,
    wake_event: asyncio.Event,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> None:
    while not stop_event.is_set():
        now = clock()

        def create_for_sequence(sequence: int, acquired_at: datetime = now) -> MeasurementEvent:
            return generator.create(sequence, measured_at=acquired_at)

        event: MeasurementEvent = store.create_event(
            create_for_sequence,
            now=now,
        )
        LOGGER.info(
            "measurement acquired and persisted",
            extra={
                "event_id": str(event.event_id),
                "device_id": event.device_id,
                "sequence": event.sequence,
            },
        )
        wake_event.set()
        await _wait_or_timeout(stop_event, settings.interval_seconds)


async def run_agent(settings: AgentSettings, stop_event: asyncio.Event | None = None) -> None:
    stop = stop_event or asyncio.Event()
    wake = asyncio.Event()
    store = AgentStore(settings.db_path, settings.device_id)
    store.initialize()
    generator = MeasurementGenerator(settings.device_id, seed=settings.random_seed)

    async with build_http_client(settings) as client:
        synchronizer = SyncService(store, settings, client)
        acquisition_task = asyncio.create_task(
            acquisition_loop(store, generator, settings, stop, wake),
            name="measurement-acquisition",
        )
        sync_task = asyncio.create_task(
            synchronizer.run(stop, wake), name="measurement-synchronization"
        )
        try:
            await stop.wait()
        finally:
            acquisition_task.cancel()
            sync_task.cancel()
            await asyncio.gather(acquisition_task, sync_task, return_exceptions=True)
            try:
                await synchronizer.sync_once()
            except Exception:
                # Shutdown must not discard local data or hang indefinitely. The outbox
                # remains authoritative and will retry after the next process start.
                LOGGER.exception("final synchronization attempt failed")


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
        configure_logging(settings.log_level)
        asyncio.run(_run_with_signals(settings))
    except ConfigurationError as exc:
        raise SystemExit(f"Configuration error: {exc}") from exc
    except KeyboardInterrupt:
        LOGGER.info("device agent stopped")


if __name__ == "__main__":
    main()
