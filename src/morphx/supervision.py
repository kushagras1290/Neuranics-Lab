"""Task supervision: tolerate brief storage faults, fail loudly on persistent ones."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections.abc import Coroutine, Mapping
from typing import Any, Final

from morphx.errors import AgentTaskFailedError, OutboxStateError

LOGGER = logging.getLogger(__name__)

# Faults that can clear on their own: "database is locked", a full disk being freed,
# or a row changing between read and update. Anything else is a defect and crashes.
RECOVERABLE_STORAGE_ERRORS: Final[tuple[type[BaseException], ...]] = (
    sqlite3.OperationalError,
    OutboxStateError,
)
DEFAULT_FAILURE_LIMIT: Final[int] = 5
# Time tasks get to observe the stop event and finish an in-flight step (for example a
# SQLite commit running in a worker thread) before they are cancelled.
DEFAULT_SHUTDOWN_GRACE_SECONDS: Final[float] = 1.0


class FailureBudget:
    """Count consecutive failures of one loop and decide when to stop absorbing them."""

    def __init__(self, task_name: str, limit: int = DEFAULT_FAILURE_LIMIT) -> None:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        self.task_name = task_name
        self.limit = limit
        self.consecutive_failures = 0

    def record_success(self) -> None:
        self.consecutive_failures = 0

    def exhausted_after(self, exc: BaseException) -> bool:
        """Record one failure; return True when the caller must re-raise it."""
        self.consecutive_failures += 1
        exhausted = self.consecutive_failures >= self.limit
        LOGGER.error(
            "supervised task iteration failed",
            exc_info=exc,
            extra={
                "task": self.task_name,
                "consecutive_failures": self.consecutive_failures,
                "failure_limit": self.limit,
                "giving_up": exhausted,
            },
        )
        return exhausted


async def supervise(
    coroutines: Mapping[str, Coroutine[Any, Any, None]],
    stop_event: asyncio.Event,
    *,
    shutdown_grace_seconds: float = DEFAULT_SHUTDOWN_GRACE_SECONDS,
) -> None:
    """Run long-lived tasks until ``stop_event`` is set or any task ends on its own.

    On a requested stop, tasks get ``shutdown_grace_seconds`` to exit cooperatively before
    being cancelled. A task that raises, or returns before shutdown was requested, cancels
    its siblings and surfaces as ``AgentTaskFailedError`` so the process exits non-zero and
    the service manager (Docker ``restart: unless-stopped``, systemd) restarts it.
    """
    tasks: dict[asyncio.Future[Any], str] = {
        asyncio.create_task(coro, name=name): name for name, coro in coroutines.items()
    }
    stop_waiter = asyncio.create_task(stop_event.wait(), name="stop-signal")
    try:
        done, _ = await asyncio.wait({*tasks, stop_waiter}, return_when=asyncio.FIRST_COMPLETED)
        if stop_waiter in done:
            await asyncio.wait(tasks, timeout=shutdown_grace_seconds)
    finally:
        for task in (*tasks, stop_waiter):
            task.cancel()
        await asyncio.gather(*tasks, stop_waiter, return_exceptions=True)

    for task in done:
        if task is stop_waiter or task.cancelled():
            continue
        name = tasks[task]
        failure = task.exception()
        if failure is not None:
            raise AgentTaskFailedError(f"{name} task failed") from failure
        if not stop_event.is_set():
            raise AgentTaskFailedError(f"{name} task exited before shutdown was requested")


async def wait_for_either(first: asyncio.Event, second: asyncio.Event, timeout: float) -> None:
    """Return when either event is set or ``timeout`` elapses, whichever is first."""
    waiters = {asyncio.ensure_future(first.wait()), asyncio.ensure_future(second.wait())}
    try:
        await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
