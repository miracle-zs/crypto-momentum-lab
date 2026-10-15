"""Tests for controlled stream epoch rollover of non-flat positions in ExecutionBook."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_book import (
    ExecutionBook,
)
from crypto_momentum_lab.domain.execution.observation_models import (
    EvidenceConflict,
    EvidencePendingReason,
    WaitingForEvidence,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.ports import ExecutionHeadSnapshot
from crypto_momentum_lab.live_rollout.exits import managed_live_positions_from_views


class _DurableTx:
    def __init__(self) -> None:
        self.head: ExecutionHeadSnapshot | None = None
        self.trade_ids: set[str] = set()

    async def load_head(self, key: Any) -> ExecutionHeadSnapshot | None:
        del key
        return self.head

    async def load_checkpoint_from_head(self, **kwargs: Any) -> None:
        del kwargs
        return None

    async def record_evidence(self, **kwargs: Any) -> bool:
        del kwargs
        return True

    async def record_trade(self, **kwargs: Any) -> bool:
        trade = kwargs["trade"]
        if trade.trade_id in self.trade_ids:
            return False
        self.trade_ids.add(trade.trade_id)
        return True

    async def persist_watermark(self, **kwargs: Any) -> None:
        del kwargs

    async def persist_head(self, **kwargs: Any) -> int:
        self.head = ExecutionHeadSnapshot(
            revision=int(kwargs.get("expected_revision") or 0) + 1,
            stream_id=kwargs["stream_id"],
            stream_epoch=kwargs["stream_epoch"],
            projection_version=kwargs["projection_version"],
            state_payload=kwargs["state_payload"],
        )
        return self.head.revision


class _DurableUow:
    def __init__(self) -> None:
        self.tx = _DurableTx()

    @asynccontextmanager
    async def transaction(self, key: Any):
        del key
        yield self.tx


@pytest.mark.parametrize("durable", [False, True])
@pytest.mark.asyncio
async def test_matching_nonflat_snapshot_adopts_epoch_and_keeps_exit_batch(
    durable: bool,
) -> None:
    """A matching snapshot continues a healthy existing lot without a history scan."""
    uow = _DurableUow() if durable else None
    book = ExecutionBook(execution_unit_of_work=uow)
    book._persistence_failed = False
    scope = ExecutionScope(
        environment="live",
        account_label="primary",
        symbol="GRASSUSDT",
        position_side=FuturesPositionSide.LONG,
    )
    start = datetime(2026, 9, 29, 6, 7, tzinfo=UTC)

    # 1. Establish initial non-flat position under epoch-1
    fill = AccountFillEvent(
        environment=scope.environment,
        account_label=scope.account_label,
        symbol=scope.symbol,
        trade_id="trade-1",
        order_id="order-1",
        side="BUY",
        price=Decimal("0.72483"),
        quantity=Decimal("137.6"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.05"),
        fee_asset="USDT",
        trade_at=start,
        raw_payload={"positionSide": "LONG"},
    )
    obs_1 = await book.observe(
        ExecutionEvidence(
            evidence_id="evidence-initial-fill",
            scope=scope,
            observed_at=start,
            fills=(fill,),
            stream_id="account_event_hub",
            stream_epoch="epoch-1",
            sequence=1,
        )
    )
    assert not isinstance(obs_1, EvidenceConflict)
    if durable:
        assert uow is not None and "trade-1" in uow.tx.trade_ids

    # 2. Confirm that the original stream reconciled the current quantity.
    snapshot = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="GRASSUSDT",
        position_side="LONG",
        position_amt=Decimal("137.6"),
        entry_price=Decimal("0.72483"),
        mark_price=Decimal("0.72483"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("99.7366"),
        leverage=5,
        margin_type="cross",
        observed_at=datetime(2026, 9, 29, 14, 0, tzinfo=UTC),
        raw_payload={"positionSide": "LONG"},
    )
    original_stream_snapshot = replace(
        snapshot,
        observed_at=start + timedelta(hours=1),
    )
    obs_original_snapshot = await book.observe(
        ExecutionEvidence(
            evidence_id="evidence-snapshot-epoch-1",
            scope=scope,
            observed_at=original_stream_snapshot.observed_at,
            snapshot=original_stream_snapshot,
            stream_id="account_event_hub",
            stream_epoch="epoch-1",
            sequence=2,
        )
    )
    assert not isinstance(obs_original_snapshot, (EvidenceConflict, WaitingForEvidence))
    view_epoch_1 = await book.read(
        scope, stream_id="account_event_hub", stream_epoch="epoch-1"
    )
    assert view_epoch_1.total_quantity == Decimal("137.6")
    assert view_epoch_1.discrepancy is None
    assert view_epoch_1.reconciliation_gap in (None, Decimal("0"))
    assert view_epoch_1.unallocated_quantity == Decimal("0")

    if not durable:
        book._head_revisions[scope.to_position_key().canonical_id] = 1
    # 3. Register new stream epoch (simulating execution-account container restart).
    book.register_active_stream(
        environment="live",
        account_label="primary",
        stream_id="account_event_hub",
        stream_epoch="epoch-2",
    )
    if durable:
        # Simulate restart: the durable trade identity remains, while the
        # in-memory seen-id set has not yet been rebuilt.
        book._seen_trade_ids.clear()

    evidence_new_epoch = ExecutionEvidence(
        evidence_id="evidence-snapshot-epoch-2",
        scope=scope,
        observed_at=snapshot.observed_at,
        snapshot=snapshot,
        fills=(fill,) if durable else (),
        stream_id="account_event_hub",
        stream_epoch="epoch-2",
        sequence=1,
    )

    obs_2 = await book.observe(evidence_new_epoch)

    assert not isinstance(obs_2, (EvidenceConflict, WaitingForEvidence))
    adopted = await book.read(
        scope, stream_id="account_event_hub", stream_epoch="epoch-2"
    )
    assert adopted.stream_scope is not None
    assert adopted.stream_scope.stream_epoch == "epoch-2"
    assert adopted.total_quantity == view_epoch_1.total_quantity
    assert len(adopted.batches) == len(view_epoch_1.batches) == 1
    managed = managed_live_positions_from_views((adopted,))
    assert len(managed) == 1
    assert managed[0].symbol == "GRASSUSDT"
    assert managed[0].quantity == Decimal("137.6")


@pytest.mark.asyncio
async def test_different_epoch_quantity_waits_until_source_history_is_complete() -> (
    None
):
    """Different quantities alone cannot prove a conflict across a stream gap."""
    book = ExecutionBook()
    scope = ExecutionScope(
        environment="live",
        account_label="primary",
        symbol="GRASSUSDT",
        position_side=FuturesPositionSide.LONG,
    )
    start = datetime(2026, 9, 29, 6, 7, tzinfo=UTC)

    fill = AccountFillEvent(
        environment=scope.environment,
        account_label=scope.account_label,
        symbol=scope.symbol,
        trade_id="trade-1",
        order_id="order-1",
        side="BUY",
        price=Decimal("0.72483"),
        quantity=Decimal("137.6"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.05"),
        fee_asset="USDT",
        trade_at=start,
        raw_payload={"positionSide": "LONG"},
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="evidence-initial-fill",
            scope=scope,
            observed_at=start,
            fills=(fill,),
            stream_id="account_event_hub",
            stream_epoch="epoch-1",
            sequence=1,
        )
    )

    book.register_active_stream(
        environment="live",
        account_label="primary",
        stream_id="account_event_hub",
        stream_epoch="epoch-2",
    )

    # Disagreeing quantity: 100.0 instead of 137.6
    snapshot_mismatch = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="GRASSUSDT",
        position_side="LONG",
        position_amt=Decimal("100.0"),
        entry_price=Decimal("0"),
        mark_price=Decimal("0.72483"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("72.483"),
        leverage=5,
        margin_type="cross",
        observed_at=datetime(2026, 9, 29, 14, 0, tzinfo=UTC),
        raw_payload={"positionSide": "LONG"},
    )

    obs_mismatch = await book.observe(
        ExecutionEvidence(
            evidence_id="evidence-snapshot-mismatch",
            scope=scope,
            observed_at=snapshot_mismatch.observed_at,
            snapshot=snapshot_mismatch,
            stream_id="account_event_hub",
            stream_epoch="epoch-2",
            sequence=1,
        )
    )

    assert isinstance(obs_mismatch, WaitingForEvidence)
    assert obs_mismatch.reason is EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED
    preserved = await book.read(
        scope, stream_id="account_event_hub", stream_epoch="epoch-1"
    )
    assert preserved.total_quantity == Decimal("137.6")


@pytest.mark.asyncio
async def test_durable_empty_flat_rollover_resets_sequence_monotonicity() -> None:
    """When a new stream epoch is adopted under durable UoW, sequence monotonicity

    must NOT compare the new stream's sequence (e.g. 11) against the old stream's
    prior sequence (e.g. 463).
    """
    uow = _DurableUow()
    book = ExecutionBook(execution_unit_of_work=uow)
    book._persistence_failed = False
    scope = ExecutionScope(
        environment="live",
        account_label="primary",
        symbol="GRASSUSDT",
        position_side=FuturesPositionSide.LONG,
    )
    start = datetime(2026, 9, 29, 6, 7, tzinfo=UTC)

    # Establish an empty position under the old stream with sequence 463.
    obs_1 = await book.observe(
        ExecutionEvidence(
            evidence_id="evidence-initial-fill-463",
            scope=scope,
            observed_at=start,
            stream_id="account_event_hub",
            stream_epoch="epoch-1",
            sequence=463,
        )
    )
    assert not isinstance(obs_1, EvidenceConflict)

    # 2. Container restarts: active stream becomes epoch-2
    book.register_active_stream(
        environment="live",
        account_label="primary",
        stream_id="account_event_hub",
        stream_epoch="epoch-2",
    )

    # 3. Snapshot arrives under epoch-2 with sequence 11 (which is < 463!)
    snapshot = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="GRASSUSDT",
        position_side="LONG",
        position_amt=Decimal("0"),
        entry_price=Decimal("0"),
        mark_price=Decimal("0.72483"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("99.7366"),
        leverage=5,
        margin_type="cross",
        observed_at=datetime(2026, 9, 29, 14, 0, tzinfo=UTC),
        raw_payload={"positionSide": "LONG"},
    )

    obs_2 = await book.observe(
        ExecutionEvidence(
            evidence_id="evidence-snapshot-epoch-2-seq-11",
            scope=scope,
            observed_at=snapshot.observed_at,
            snapshot=snapshot,
            stream_id="account_event_hub",
            stream_epoch="epoch-2",
            sequence=11,
        )
    )

    # The new epoch must not compare sequence 11 against the old sequence 463.
    assert not isinstance(obs_2, EvidenceConflict), f"Observation failed: {obs_2}"

    view = await book.read(scope, stream_id="account_event_hub", stream_epoch="epoch-2")
    assert view.total_quantity == Decimal("0")
    assert view.stream_scope.stream_epoch == "epoch-2"
