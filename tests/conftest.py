from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from morphx.models import (
    HaemoglobinMeasurement,
    MeasurementEvent,
    Measurements,
    RbcMeasurement,
    WbcMeasurement,
)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def fixed_time() -> datetime:
    return datetime(2026, 10, 7, 8, 0, tzinfo=UTC)


@pytest.fixture
def event_factory(fixed_time: datetime):
    def factory(
        sequence: int = 1,
        *,
        event_id: UUID | None = None,
        device_id: str = "MORPHX_SIM_001",
        wbc: float = 7.25,
    ) -> MeasurementEvent:
        return MeasurementEvent(
            event_id=event_id or UUID(int=sequence),
            device_id=device_id,
            sample_id=f"SAMPLE_{sequence:08d}",
            sequence=sequence,
            measured_at=fixed_time,
            measurements=Measurements(
                wbc=WbcMeasurement(value=wbc),
                rbc=RbcMeasurement(value=5.1),
                haemoglobin=HaemoglobinMeasurement(value=14.2),
            ),
        )

    return factory


@pytest.fixture
def temp_db(tmp_path: Path) -> Path:
    return tmp_path / "state.db"
