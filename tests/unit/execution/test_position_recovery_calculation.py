"""Recovery calculation acceptance without storage or an ExecutionBook owner."""

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.evidence_digest import view_projection_digest
from crypto_momentum_lab.domain.execution.ports import (
    DurableExecutionPositionState,
    ExecutionHeadSnapshot,
)
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.position_recovery import (
    recover_durable_position,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut


@pytest.fixture
def state():
    key = PositionKey("live", "recovery-account", "TESTUSDT", "LONG")
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="hub", stream_epoch="epoch"
    )
    facts = AccountFacts(position_key=key, stream_scope=scope)
    cut = DurableJournalCut(
        scope=scope, facts=facts, revision=0, as_of=datetime(2026, 9, 30, tzinfo=UTC)
    )
    view = PositionBook(AccountJournal.from_durable_cut(cut)).get_view()
    payload = dict(
        schema_version=1,
        position_key=dict(
            environment=key.environment,
            account_label=key.account_label,
            symbol=key.symbol,
            position_side="LONG",
        ),
        stream_scope=dict(stream_id="hub", stream_epoch="epoch"),
        facts_hash=facts.compute_facts_hash(),
        projection_digest=PositionRecoveryCodec.compute_projection_digest(
            PositionLedger(key).project(facts)
        ),
        view_digest=view_projection_digest(view),
        recovery_checkpoint=None,
        journal_revision=0,
        active_reservation_ids=["existing-reservation"],
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
    "field", ["facts_hash", "projection_digest", "view_digest", "recovery_checkpoint"]
)
def test_existing_migration_policy_returns_diagnostic_and_recomputed_digest(
    state, field
):
    payload = dict(state.head.state_payload, **{field: "old-codec-value"})
    changed = replace(state, head=replace(state.head, state_payload=payload))
    recovered = recover_durable_position(changed)
    assert len(recovered.diagnostics) == 1
    assert recovered.diagnostics[0][0].endswith("_migrated")
    assert recovered.reservation_ids == frozenset({"existing-reservation"})
    assert recovered.projection_digest == state.head.state_payload["projection_digest"]
    assert changed.head.state_payload[field] == "old-codec-value"


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", 2),
        ("position_key", {}),
        ("stream_scope", {}),
        ("journal_revision", True),
        ("active_reservation_ids", [None]),
    ],
)
def test_malformed_head_cannot_produce_recovery_candidate(state, field, value):
    payload = dict(state.head.state_payload, **{field: value})
    with pytest.raises(RuntimeError, match="malformed"):
        recover_durable_position(
            replace(state, head=replace(state.head, state_payload=payload))
        )


def test_legacy_invalid_sequence_normalization_is_explicit(state):
    payload = dict(state.head.state_payload, last_sequence=-1)
    recovered = recover_durable_position(
        replace(state, head=replace(state.head, state_payload=payload))
    )
    assert recovered.last_sequence == 0
    assert recovered.diagnostics == (
        ("durable_execution_head_sequence_invalid", {"sequence": -1}),
    )


def test_headless_recovery_has_no_invented_revision_or_reservations(state):
    recovered = recover_durable_position(replace(state, head=None))
    assert recovered.head_revision == 0
    assert recovered.last_sequence is None
    assert not recovered.reservation_ids and not recovered.diagnostics
