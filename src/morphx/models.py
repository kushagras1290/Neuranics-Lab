"""Validated wire models shared by the agent and central server."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

DeviceId = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")]
SampleId = Annotated[str, Field(min_length=1, max_length=96, pattern=r"^[A-Za-z0-9_-]+$")]


class WbcMeasurement(BaseModel):
    """White blood cell measurement with its explicit unit."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    value: Annotated[float, Field(ge=0, le=1000)]
    unit: Literal["10^3/uL"] = "10^3/uL"


class RbcMeasurement(BaseModel):
    """Red blood cell measurement with its explicit unit."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    value: Annotated[float, Field(ge=0, le=100)]
    unit: Literal["10^6/uL"] = "10^6/uL"


class HaemoglobinMeasurement(BaseModel):
    """Haemoglobin measurement with its explicit unit."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    value: Annotated[float, Field(ge=0, le=100)]
    unit: Literal["g/dL"] = "g/dL"


class Measurements(BaseModel):
    """Synthetic blood-count measurements for a single test."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    wbc: WbcMeasurement
    rbc: RbcMeasurement
    haemoglobin: HaemoglobinMeasurement


class MeasurementEvent(BaseModel):
    """Stable, retry-safe device measurement payload."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID
    device_id: DeviceId
    sample_id: SampleId
    sequence: Annotated[int, Field(ge=1)]
    measured_at: datetime
    measurements: Measurements

    @field_validator("measured_at")
    @classmethod
    def require_timezone_and_normalize_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("measured_at must include a timezone")
        return value.astimezone(UTC)


class StoredMeasurement(MeasurementEvent):
    """Server representation including receipt time."""

    received_at: datetime

    @field_validator("received_at")
    @classmethod
    def normalize_receipt_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("received_at must include a timezone")
        return value.astimezone(UTC)


class IngestStatus(StrEnum):
    CREATED = "created"
    DUPLICATE = "duplicate"


class IngestResponse(BaseModel):
    event_id: UUID
    status: IngestStatus
    received_at: datetime


class MeasurementPage(BaseModel):
    device_id: DeviceId
    items: list[StoredMeasurement]
    next_after_sequence: int | None


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"
