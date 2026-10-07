"""Synthetic measurement generation for the simulated device."""

from __future__ import annotations

import random
from datetime import UTC, datetime
from uuid import uuid4

from morphx.models import (
    HaemoglobinMeasurement,
    MeasurementEvent,
    Measurements,
    RbcMeasurement,
    WbcMeasurement,
)


class MeasurementGenerator:
    """Generate plausible-looking values for software testing only."""

    def __init__(self, device_id: str, *, seed: int | None = None) -> None:
        self.device_id = device_id
        self._random = random.Random(seed)  # noqa: S311 - synthetic test data, not security

    def create(self, sequence: int, *, measured_at: datetime | None = None) -> MeasurementEvent:
        timestamp = (measured_at or datetime.now(UTC)).astimezone(UTC)
        return MeasurementEvent(
            event_id=uuid4(),
            device_id=self.device_id,
            sample_id=f"SAMPLE_{self.device_id}_{sequence:08d}",
            sequence=sequence,
            measured_at=timestamp,
            measurements=Measurements(
                wbc=WbcMeasurement(value=round(self._random.uniform(4.0, 11.0), 2)),
                rbc=RbcMeasurement(value=round(self._random.uniform(4.2, 6.1), 2)),
                haemoglobin=HaemoglobinMeasurement(
                    value=round(self._random.uniform(12.0, 17.5), 2)
                ),
            ),
        )
