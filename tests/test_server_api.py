from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient

from morphx.config import ServerSettings
from morphx.server import create_app


def settings(db_path: Path, *, api_key: str | None = None) -> ServerSettings:
    return ServerSettings(
        db_path=db_path,
        host="127.0.0.1",
        port=8000,
        api_key=api_key,
        log_level="INFO",
    )


def test_ingest_is_idempotent_and_persists_across_restart(temp_db, event_factory) -> None:
    payload = event_factory().model_dump(mode="json")
    with TestClient(create_app(settings(temp_db))) as client:
        created = client.post("/v1/measurements", json=payload)
        duplicate = client.post("/v1/measurements", json=payload)

    assert created.status_code == 201
    assert created.json()["status"] == "created"
    assert duplicate.status_code == 200
    assert duplicate.json()["status"] == "duplicate"
    assert duplicate.json()["received_at"] == created.json()["received_at"]

    with TestClient(create_app(settings(temp_db))) as restarted_client:
        page = restarted_client.get("/v1/devices/MORPHX_SIM_001/measurements")
    assert page.status_code == 200
    assert [item["sequence"] for item in page.json()["items"]] == [1]


def test_conflicting_event_id_and_sequence_return_409(temp_db, event_factory) -> None:
    original = event_factory()
    changed_payload = event_factory(event_id=original.event_id, wbc=9.9)
    sequence_collision = event_factory(event_id=uuid4())

    with TestClient(create_app(settings(temp_db))) as client:
        assert (
            client.post("/v1/measurements", json=original.model_dump(mode="json")).status_code
            == 201
        )
        changed = client.post("/v1/measurements", json=changed_payload.model_dump(mode="json"))
        collision = client.post("/v1/measurements", json=sequence_collision.model_dump(mode="json"))

    assert changed.status_code == 409
    assert collision.status_code == 409


def test_retrieval_is_ordered_and_cursor_paginated(temp_db, event_factory) -> None:
    with TestClient(create_app(settings(temp_db))) as client:
        for sequence in (3, 1, 2):
            response = client.post(
                "/v1/measurements",
                json=event_factory(sequence).model_dump(mode="json"),
            )
            assert response.status_code == 201

        first_page = client.get(
            "/v1/devices/MORPHX_SIM_001/measurements", params={"limit": 2}
        ).json()
        second_page = client.get(
            "/v1/devices/MORPHX_SIM_001/measurements",
            params={"after_sequence": first_page["next_after_sequence"], "limit": 2},
        ).json()

    assert [item["sequence"] for item in first_page["items"]] == [1, 2]
    assert first_page["next_after_sequence"] == 2
    assert [item["sequence"] for item in second_page["items"]] == [3]
    assert second_page["next_after_sequence"] is None


def test_optional_api_key_protects_measurement_routes(temp_db, event_factory) -> None:
    protected_settings = replace(settings(temp_db), api_key="correct-secret")
    payload = event_factory().model_dump(mode="json")
    with TestClient(create_app(protected_settings)) as client:
        missing = client.post("/v1/measurements", json=payload)
        accepted = client.post(
            "/v1/measurements",
            json=payload,
            headers={"X-API-Key": "correct-secret"},
        )
        health = client.get("/health/ready")

    assert missing.status_code == 401
    assert accepted.status_code == 201
    assert health.status_code == 200


def test_invalid_inputs_are_rejected(temp_db) -> None:
    with TestClient(create_app(settings(temp_db))) as client:
        invalid_device = client.get("/v1/devices/not%20valid/measurements")
        invalid_limit = client.get("/v1/devices/MORPHX_SIM_001/measurements", params={"limit": 501})
    assert invalid_device.status_code == 422
    assert invalid_limit.status_code == 422


def test_readiness_fails_when_storage_is_unavailable(temp_db, monkeypatch) -> None:
    app = create_app(settings(temp_db))
    with TestClient(app) as client:
        monkeypatch.setattr(app.state.store, "is_ready", lambda: False)
        response = client.get("/health/ready")
    assert response.status_code == 503
