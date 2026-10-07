from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.ports import (
    DecisionCommitConflict,
    ExecutionEvidenceIdentity,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    ExecutionTransaction,
)

NOW = datetime(2026, 10, 7, tzinfo=UTC)
KEY = ExecutionScope(
    environment="live", account_label="retirement-test", symbol="BTCUSDT"
).to_position_key()


def transaction(*, retired=False, receipt=None, head=None):
    async def get(model, identity, **kwargs):
        if model.__name__ == "ExecutionEvidenceReceiptRow":
            return receipt
        if model.__name__ == "ExecutionRetiredStreamRow":
            return SimpleNamespace(retired_at=NOW) if retired else None
        if model.__name__ == "ExecutionBookHeadRow":
            return head
        return None

    session = SimpleNamespace(get=AsyncMock(side_effect=get), add=Mock())
    return ExecutionTransaction(
        session,
        journal_store=AsyncMock(),
        command_repository=AsyncMock(),
        reservation_repository=AsyncMock(),
    )


@pytest.mark.parametrize("sequence", [None, 1])
async def test_deleted_receipt_cannot_readmit_retired_stream(sequence):
    tx = transaction(retired=True)
    with pytest.raises(DecisionCommitConflict, match="retired"):
        await tx.record_evidence(
            key=KEY,
            stream_id="source",
            stream_epoch="old",
            evidence=ExecutionEvidenceIdentity("evidence", "digest", NOW, sequence),
        )
    tx.session.add.assert_not_called()


async def test_retired_stream_cannot_become_current_head_again():
    tx = transaction(retired=True)
    with pytest.raises(DecisionCommitConflict, match="retired"):
        await tx.persist_head(
            key=KEY,
            stream_id="source",
            stream_epoch="old",
            expected_revision=0,
            projection_version="projection",
            state_payload={},
            updated_at=NOW,
            is_flat_adoption=True,
        )
    tx.session.add.assert_not_called()


async def test_validated_rollover_seals_previous_stream_atomically():
    head = SimpleNamespace(stream_id="source", stream_epoch="old", revision=1)
    tx = transaction(head=head)
    await tx.persist_head(
        key=KEY,
        stream_id="source",
        stream_epoch="new",
        expected_revision=1,
        projection_version="projection",
        state_payload={},
        updated_at=NOW,
        is_flat_adoption=True,
    )
    seals = [
        call.args[0]
        for call in tx.session.add.call_args_list
        if type(call.args[0]).__name__ == "ExecutionRetiredStreamRow"
    ]
    assert len(seals) == 1
    assert seals[0].stream_epoch == "old"
    assert head.stream_epoch == "new"


async def test_retirement_keeps_exact_duplicate_receipt_semantics():
    tx = transaction(
        retired=True, receipt=SimpleNamespace(payload_digest="digest", sequence=1)
    )
    assert not await tx.record_evidence(
        key=KEY,
        stream_id="source",
        stream_epoch="old",
        evidence=ExecutionEvidenceIdentity("evidence", "digest", NOW, 1),
    )
