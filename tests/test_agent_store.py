from __future__ import annotations

from datetime import timedelta

import pytest

from morphx.agent_store import AgentStore, DeviceIdentityError


def test_sequence_and_outbox_survive_restart(temp_db, fixed_time, event_factory) -> None:
    first_store = AgentStore(temp_db, "MORPHX_SIM_001")
    first_store.initialize()
    first = first_store.create_event(event_factory, now=fixed_time)

    restarted_store = AgentStore(temp_db, "MORPHX_SIM_001")
    restarted_store.initialize()
    second = restarted_store.create_event(event_factory, now=fixed_time + timedelta(seconds=5))

    assert [first.sequence, second.sequence] == [1, 2]
    assert restarted_store.pending_count() == 2
    assert [event.event_id for event in restarted_store.all_events()] == [
        first.event_id,
        second.event_id,
    ]


def test_database_cannot_be_reassigned_to_another_device(temp_db) -> None:
    AgentStore(temp_db, "MORPHX_SIM_001").initialize()

    with pytest.raises(DeviceIdentityError, match="database belongs"):
        AgentStore(temp_db, "MORPHX_SIM_002").initialize()


def test_failed_factory_does_not_consume_sequence(temp_db, fixed_time, event_factory) -> None:
    store = AgentStore(temp_db, "MORPHX_SIM_001")
    store.initialize()

    def broken_factory(sequence: int):
        raise RuntimeError(f"cannot create {sequence}")

    with pytest.raises(RuntimeError):
        store.create_event(broken_factory, now=fixed_time)

    event = store.create_event(event_factory, now=fixed_time)
    assert event.sequence == 1
