"""Device listing, time-window retrieval, conflict codes and server entry point."""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

import morphx.server as server_module
from morphx.server import create_app, warn_if_exposed_without_auth
from tests.test_server_api import settings

DEVICE_URL = "/v1/devices/MORPHX_SIM_001/measurements"


def _post(client: TestClient, event) -> None:
    response = client.post("/v1/measurements", json=event.model_dump(mode="json"))
    assert response.status_code == 201


def test_list_devices_summarizes_each_device(temp_db, event_factory) -> None:
    with TestClient(create_app(settings(temp_db))) as client:
        assert client.get("/v1/devices").json() == {"items": []}
        for sequence in (1, 2, 3):
            _post(client, event_factory(sequence))
        _post(client, event_factory(1, event_id=uuid4(), device_id="MORPHX_SIM_002"))
        devices = client.get("/v1/devices").json()["items"]

    assert [device["device_id"] for device in devices] == ["MORPHX_SIM_001", "MORPHX_SIM_002"]
    first = devices[0]
    assert (first["measurement_count"], first["first_sequence"], first["last_sequence"]) == (
        3,
        1,
        3,
    )


def test_measurements_can_be_filtered_by_measured_time(temp_db, fixed_time, event_factory) -> None:
    with TestClient(create_app(settings(temp_db))) as client:
        for sequence in range(1, 5):
            event = event_factory(sequence).model_copy(
                update={"measured_at": fixed_time + timedelta(minutes=sequence)}
            )
            _post(client, event)
        window = client.get(
            DEVICE_URL,
            params={
                "measured_from": (fixed_time + timedelta(minutes=2)).isoformat(),
                "measured_to": (fixed_time + timedelta(minutes=3)).isoformat(),
            },
        )
        open_ended = client.get(
            DEVICE_URL,
            params={"measured_from": (fixed_time + timedelta(minutes=3, seconds=30)).isoformat()},
        )

    assert [item["sequence"] for item in window.json()["items"]] == [2, 3]
    assert [item["sequence"] for item in open_ended.json()["items"]] == [4]


@pytest.mark.parametrize(
    "params",
    [
        {"measured_from": "2026-10-07T08:00:00"},
        {"measured_to": "2026-10-07T08:00:00"},
        {"measured_from": "2026-10-07T09:00:00Z", "measured_to": "2026-10-07T08:00:00Z"},
    ],
)
def test_invalid_time_window_is_rejected(temp_db, params) -> None:
    with TestClient(create_app(settings(temp_db))) as client:
        assert client.get(DEVICE_URL, params=params).status_code == 422


def test_conflicts_carry_specific_codes(temp_db, event_factory) -> None:
    original = event_factory()
    with TestClient(create_app(settings(temp_db))) as client:
        _post(client, original)
        changed = client.post(
            "/v1/measurements",
            json=event_factory(event_id=original.event_id, wbc=9.9).model_dump(mode="json"),
        )
        reused = client.post(
            "/v1/measurements", json=event_factory(event_id=uuid4()).model_dump(mode="json")
        )

    assert changed.json()["detail"]["code"] == "EVENT_ID_CONFLICT"
    assert reused.json()["detail"]["code"] == "SEQUENCE_CONFLICT"


def test_device_listing_requires_api_key_when_configured(temp_db) -> None:
    with TestClient(create_app(replace(settings(temp_db), api_key="secret"))) as client:
        assert client.get("/v1/devices").status_code == 401
        assert client.get("/v1/devices", headers={"X-API-Key": "secret"}).status_code == 200


def test_request_id_is_propagated_or_minted(temp_db) -> None:
    with TestClient(create_app(settings(temp_db))) as client:
        echoed = client.get("/health/live", headers={"X-Request-ID": "trace-123"})
        minted = client.get("/health/live", headers={"X-Request-ID": "bad id!"})
    assert echoed.headers["X-Request-ID"] == "trace-123"
    assert minted.headers["X-Request-ID"] != "bad id!"


def test_unhandled_errors_are_logged_with_request_id(temp_db, monkeypatch, caplog) -> None:
    app = create_app(settings(temp_db))
    with TestClient(app, raise_server_exceptions=False) as client:

        def explode() -> list[object]:
            raise RuntimeError("storage exploded")

        monkeypatch.setattr(app.state.store, "list_devices", explode)
        response = client.get("/v1/devices", headers={"X-Request-ID": "trace-500"})

    assert response.status_code == 500
    failures = [record for record in caplog.records if record.getMessage() == "request failed"]
    assert failures and failures[0].request_id == "trace-500"  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("host", "api_key", "warned"),
    [("0.0.0.0", None, True), ("0.0.0.0", "k", False), ("127.0.0.1", None, False)],  # noqa: S104
)
def test_exposed_server_without_auth_is_warned(temp_db, caplog, host, api_key, warned) -> None:
    caplog.set_level(logging.WARNING)
    warn_if_exposed_without_auth(replace(settings(temp_db), host=host, api_key=api_key))
    assert ("without MORPHX_API_KEY" in caplog.text) is warned


def test_main_starts_uvicorn_with_settings(temp_db, monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(server_module, "parse_server_args", lambda: settings(temp_db))
    monkeypatch.setattr(server_module, "configure_logging", lambda level: None)
    monkeypatch.setattr(
        server_module.uvicorn, "run", lambda app, **kwargs: captured.update(kwargs, app=app)
    )
    server_module.main()
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 8000
    assert captured["access_log"] is False


def test_main_reports_configuration_error(monkeypatch) -> None:
    monkeypatch.setenv("MORPHX_SERVER_PORT", "70000")
    with pytest.raises(SystemExit, match="Configuration error"):
        server_module.main()


def test_server_defaults_to_loopback(monkeypatch) -> None:
    monkeypatch.delenv("MORPHX_SERVER_HOST", raising=False)
    assert server_module.ServerSettings.from_env().host == "127.0.0.1"
