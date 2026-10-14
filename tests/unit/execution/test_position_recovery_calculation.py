"""Recovery calculation acceptance without storage or an ExecutionBook owner."""

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.ports import (
    DecisionCommitConflict,
    DurableExecutionPositionState,
    ExecutionHeadSnapshot,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.projection_codec import encode_position_key
from crypto_momentum_lab.domain.execution.position_recovery import (
    recover_durable_position,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    AccountFacts,
    DurableJournalCut,
)


@pytest.fixture
def state():
    key = PositionKey("live", "recovery-account", "TESTUSDT", FuturesPositionSide.LONG)
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="hub", stream_epoch="epoch"
    )
    position_book = PositionBook.from_facts(
        AccountFacts(position_key=key, stream_scope=scope)
    )
    facts = position_book.current_facts
    cut = DurableJournalCut(
        scope=scope, facts=facts, revision=0, as_of=datetime(2026, 9, 30, tzinfo=UTC)
    )
    payload = dict(
        schema_version=3,
        position_key=encode_position_key(key),
        stream_scope={"stream_id": "hub", "stream_epoch": "epoch"},
        position_state=position_book.encode_current_state(),
        active_reservation_ids=["existing-reservation"],
        recovery_command_ids=[],
        external_recovery_ids=[],
        last_sequence=42,
    )
    return DurableExecutionPositionState(
        scope,
        cut,
        ExecutionHeadSnapshot(3, "hub", "epoch", "durable-token", payload),
        (),
        (),
        (),
    )


def test_recovery_returns_candidate_without_mutating_durable_input(state):
    before = deepcopy(state.head.state_payload)
    recovered = recover_durable_position(state)
    assert recovered.head_revision == 3
    assert recovered.last_sequence == 42
    assert recovered.reservation_ids == frozenset({"existing-reservation"})
    assert recovered.book.get_view().projection_version == "durable-token"
    assert recovered.diagnostics == ()
    assert state.head.state_payload == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("position_key", {}),
        ("stream_scope", {}),
        ("position_state", {}),
        ("active_reservation_ids", [None]),
        ("active_reservation_ids", None),
        ("active_reservation_ids", ("reservation",)),
        ("active_reservation_ids", "reservation"),
        ("active_reservation_ids", [""]),
        ("active_reservation_ids", [1]),
        ("recovery_command_ids", None),
        ("recovery_command_ids", [""]),
        ("recovery_command_ids", [1]),
        ("external_recovery_ids", None),
        ("external_recovery_ids", [""]),
        ("external_recovery_ids", [1]),
        ("last_sequence", -1),
        ("last_sequence", True),
        ("last_sequence", False),
        ("last_sequence", 1.0),
        ("last_sequence", "42"),
        ("last_sequence", []),
        ("last_sequence", {}),
    ],
)
def test_malformed_head_cannot_produce_recovery_candidate(state, field, value):
    payload = dict(state.head.state_payload, **{field: value})
    with pytest.raises(RuntimeError, match="malformed"):
        recover_durable_position(
            replace(state, head=replace(state.head, state_payload=payload))
        )


def test_legacy_head_requires_explicit_format_migration(state):
    payload = dict(state.head.state_payload, schema_version=1)
    with pytest.raises(DecisionCommitConflict, match="format is unsupported"):
        recover_durable_position(
            replace(state, head=replace(state.head, state_payload=payload))
        )


def test_missing_sequence_cannot_produce_recovery_candidate(state):
    payload = dict(state.head.state_payload)
    payload.pop("last_sequence")
    with pytest.raises(RuntimeError, match="malformed"):
        recover_durable_position(
            replace(state, head=replace(state.head, state_payload=payload))
        )


@pytest.mark.parametrize("field", ["recovery_command_ids", "external_recovery_ids"])
def test_missing_recovery_id_lists_cannot_produce_recovery_candidate(state, field):
    payload = dict(state.head.state_payload)
    payload.pop(field)
    with pytest.raises(RuntimeError, match="malformed"):
        recover_durable_position(
            replace(state, head=replace(state.head, state_payload=payload))
        )


def test_headless_recovery_has_no_invented_revision_or_reservations(state):
    recovered = recover_durable_position(replace(state, head=None))
    assert recovered.head_revision == 0
    assert recovered.last_sequence is None
    assert not recovered.reservation_ids and not recovered.diagnostics


@pytest.mark.parametrize("sequence", [None, 0, 1, 2**63])
def test_valid_sequence_preserves_value_without_diagnostics(state, sequence):
    payload = dict(state.head.state_payload, last_sequence=sequence)
    recovered = recover_durable_position(
        replace(state, head=replace(state.head, state_payload=payload))
    )
    assert recovered.last_sequence == sequence
    assert recovered.diagnostics == ()
    assert payload["last_sequence"] == sequence


@pytest.mark.parametrize("reservations", [[], ["a", "a", "b"]])
def test_valid_reservation_list_recovers_unique_ids_without_mutation(
    state, reservations
):
    payload = dict(state.head.state_payload, active_reservation_ids=reservations)
    before = deepcopy(payload)
    recovered = recover_durable_position(
        replace(state, head=replace(state.head, state_payload=payload))
    )
    assert recovered.reservation_ids == frozenset(reservations)
    assert recovered.diagnostics == ()
    assert payload == before
