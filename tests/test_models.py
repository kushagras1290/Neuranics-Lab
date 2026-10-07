from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from morphx.models import MeasurementEvent


def test_measurement_requires_timezone(event_factory) -> None:
    payload = event_factory().model_dump()
    payload["measured_at"] = datetime(2026, 10, 7, 8, 0)

    with pytest.raises(ValidationError, match="timezone"):
        MeasurementEvent.model_validate(payload)


def test_measurement_rejects_wrong_unit(event_factory) -> None:
    payload = event_factory().model_dump(mode="json")
    payload["measurements"]["wbc"]["unit"] = "mg/dL"

    with pytest.raises(ValidationError):
        MeasurementEvent.model_validate(payload)


def test_measurement_rejects_extra_fields(event_factory) -> None:
    payload = event_factory().model_dump(mode="json")
    payload["patient_name"] = "must not be accepted"

    with pytest.raises(ValidationError):
        MeasurementEvent.model_validate(payload)
