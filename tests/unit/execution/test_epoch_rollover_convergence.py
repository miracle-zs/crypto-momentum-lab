"""Tests for controlled stream epoch rollover of non-flat positions in ExecutionBook."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.execution_book import (
    EvidenceConflict,
    ExecutionBook,
    ExecutionEvidence,
    ExecutionScope,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide


@pytest.mark.asyncio
async def test_non_flat_position_controlled_epoch_adoption_when_quantity_matches() -> None:
    """When an active stream epoch changes, a non-flat position whose physical snapshot

    matches the ledger total quantity and has no in-flight orders should safely
    adopt the new stream epoch without deadlock or losing position facts.
    """
    book = ExecutionBook()
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

    view_epoch_1 = await book.read(
        scope, stream_id="account_event_hub", stream_epoch="epoch-1"
    )
    assert view_epoch_1.total_quantity == Decimal("137.6")
    assert view_epoch_1.is_ready_for_trade

    # 2. Register new stream epoch (simulating execution-account container restart)
    book.register_active_stream(
        environment="live",
        account_label="primary",
        stream_id="account_event_hub",
        stream_epoch="epoch-2",
    )

    # 3. New stream pushes an account snapshot with the exact matching quantity (137.6)
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

    evidence_new_epoch = ExecutionEvidence(
        evidence_id="evidence-snapshot-epoch-2",
        scope=scope,
        observed_at=snapshot.observed_at,
        snapshot=snapshot,
        stream_id="account_event_hub",
        stream_epoch="epoch-2",
        sequence=1,
    )

    obs_2 = await book.observe(evidence_new_epoch)

    # Invariant: Must NOT be rejected as a conflict
    assert not isinstance(obs_2, EvidenceConflict), f"Observation failed: {obs_2}"

    # Invariant: Reading under the new epoch MUST succeed and return ready
    view_epoch_2 = await book.read(
        scope, stream_id="account_event_hub", stream_epoch="epoch-2"
    )
    assert view_epoch_2.total_quantity == Decimal("137.6")
    assert view_epoch_2.is_ready_for_trade
    assert view_epoch_2.stream_scope.stream_epoch == "epoch-2"


@pytest.mark.asyncio
async def test_non_flat_position_epoch_adoption_rejected_when_quantity_diverges() -> None:
    """If the new stream's snapshot quantity disagrees with the ledger (actual desync),

    epoch rollover must be rejected with an EvidenceConflict to protect data integrity.
    """
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
        entry_price=Decimal("0.72483"),
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

    assert isinstance(obs_mismatch, EvidenceConflict)
