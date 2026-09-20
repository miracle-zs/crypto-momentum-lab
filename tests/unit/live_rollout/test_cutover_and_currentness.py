"""Unit tests for context currentness verification and primary cutover paths."""

import os
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from crypto_momentum_lab.domain.execution import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_batches import (
    ManagedLivePositionBatch,
)
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.execution_account.sync import AccountSnapshot
from crypto_momentum_lab.live_rollout.postgres_runtime import (
    PostgresLiveContextProvider,
    _build_position_batches,
)


def test_is_context_current_rejects_missing_realtime_snapshot_when_active() -> None:
    provider = PostgresLiveContextProvider.__new__(PostgresLiveContextProvider)
    provider._cache_epoch = 1
    provider._realtime_account_sequence = 5

    now = datetime.now(UTC)
    context_without_snapshot = SimpleNamespace(
        context_epoch=1,
        account_snapshot=None,
        account_observed_at=now,
    )

    # When realtime account sequence is > 0, missing snapshot MUST be
    # marked stale (False)
    assert (
        provider.is_context_current(
            context_without_snapshot  # type: ignore[arg-type]
        )
        is False
    )


def test_is_context_current_rejects_mismatched_cache_epoch() -> None:
    provider = PostgresLiveContextProvider.__new__(PostgresLiveContextProvider)
    provider._cache_epoch = 2
    provider._realtime_account_sequence = 0

    stale_context = SimpleNamespace(
        context_epoch=1,
        account_snapshot=None,
    )

    assert (
        provider.is_context_current(
            stale_context  # type: ignore[arg-type]
        )
        is False
    )


def test_is_context_current_accepts_matching_realtime_snapshot() -> None:
    provider = PostgresLiveContextProvider.__new__(PostgresLiveContextProvider)
    provider._cache_epoch = 2
    provider._realtime_account_sequence = 10

    fresh_context = SimpleNamespace(
        context_epoch=2,
        account_snapshot=MagicMock(spec=AccountSnapshot),
        account_snapshot_version=10,
        account_observed_at=datetime.now(UTC),
    )

    assert (
        provider.is_context_current(
            fresh_context  # type: ignore[arg-type]
        )
        is True
    )


def test_build_position_batches_respects_cutover_flag() -> None:
    t0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)
    position = SimpleNamespace(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_amt=Decimal("10"),
        entry_price=Decimal("60000"),
    )

    from crypto_momentum_lab.domain.execution.order_state import (
        ExchangeOrderState,
    )
    from crypto_momentum_lab.domain.execution.position_batches import (
        PositionOrderFact,
    )

    # Order that generated the position
    order = PositionOrderFact(
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
        side="BUY",
        reduce_only=False,
        order_type="MARKET",
        quantity=Decimal("10"),
        executed_quantity=Decimal("10"),
        state=ExchangeOrderState.FILLED,
        client_order_id="c_1",
        exchange_order_id="e_1",
        created_at=t0,
        updated_at=t0,
        price=Decimal("60000"),
    )

    # 1. Default (flag disabled / not set): returns legacy rebuilt batches
    with patch.dict(os.environ, {"CML_POSITION_LEDGER_PRIMARY_ENABLED": "0"}):
        batches = _build_position_batches(
            position=position,  # type: ignore[arg-type]
            side=StrategySide.LONG,
            position_side=FuturesPositionSide.BOTH,
            matching_orders=[order],  # type: ignore[arg-type]
            fill_times={"e_1": t0},
            fill_prices={"e_1": Decimal("60000")},
        )
        assert len(batches) == 1
        assert batches[0].quantity == Decimal("10")
        assert isinstance(batches[0], ManagedLivePositionBatch)

    # 2. Cutover enabled: returns PositionLedger active batches
    # mapped to ManagedLivePositionBatch
    with patch.dict(os.environ, {"CML_POSITION_LEDGER_PRIMARY_ENABLED": "1"}):
        batches = _build_position_batches(
            position=position,  # type: ignore[arg-type]
            side=StrategySide.LONG,
            position_side=FuturesPositionSide.BOTH,
            matching_orders=[order],  # type: ignore[arg-type]
            fill_times={"e_1": t0},
            fill_prices={"e_1": Decimal("60000")},
        )
        assert len(batches) == 1
        assert batches[0].quantity == Decimal("10")
        assert batches[0].entry_price == Decimal("60000")
        assert batches[0].opened_at == t0
        assert isinstance(batches[0], ManagedLivePositionBatch)


def test_position_ledger_shadow_comparator_detects_lot_attribution_mismatch() -> None:
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        PositionKey,
        PositionLedgerBatch,
        PositionLedgerProjection,
    )
    from crypto_momentum_lab.live_rollout.position_ledger_shadow import (
        PositionLedgerShadowComparator,
        ShadowDiffCategory,
    )

    t0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)
    t1 = datetime(2026, 9, 20, 11, 0, tzinfo=UTC)
    position_key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )

    # Legacy: 7 @ 100, 10 @ 200 (total 17)
    legacy_batches = (
        ManagedLivePositionBatch(
            batch_id="b1",
            quantity=Decimal("7"),
            entry_price=Decimal("100"),
            opened_at=t0,
        ),
        ManagedLivePositionBatch(
            batch_id="b2",
            quantity=Decimal("10"),
            entry_price=Decimal("200"),
            opened_at=t1,
        ),
    )

    # Ledger: 5 @ 100, 12 @ 200 (total 17)
    ledger_batches = (
        PositionLedgerBatch(
            batch_id="b1",
            episode_id="ep1",
            quantity=Decimal("5"),
            original_quantity=Decimal("5"),
            entry_price=Decimal("100"),
            opened_at=t0,
        ),
        PositionLedgerBatch(
            batch_id="b2",
            episode_id="ep1",
            quantity=Decimal("12"),
            original_quantity=Decimal("12"),
            entry_price=Decimal("200"),
            opened_at=t1,
        ),
    )

    projection = PositionLedgerProjection(
        position_key=position_key,
        active_episode=None,
        active_batches=ledger_batches,
        total_active_quantity=Decimal("17"),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        high_watermark_trade_at=t1,
    )

    report = PositionLedgerShadowComparator.compare(
        position_key=position_key,
        legacy_batches=legacy_batches,
        ledger_projection=projection,
    )

    # In Round 5 critique, this was misidentified as exact_match=True.
    # Now it must be detected as LOT_ATTRIBUTION_MISMATCH with is_concordant=False.
    assert report.is_concordant is False
    assert report.category == ShadowDiffCategory.LOT_ATTRIBUTION_MISMATCH
    assert "Batch quantity mismatch at index 0" in report.details


def test_build_position_batches_short_position_cutover() -> None:
    t0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)
    # Short position: position_amt is negative in exchange snapshot
    position = SimpleNamespace(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_amt=Decimal("-10"),
        entry_price=Decimal("60000"),
    )

    from crypto_momentum_lab.domain.execution.order_state import (
        ExchangeOrderState,
    )
    from crypto_momentum_lab.domain.execution.position_batches import (
        PositionOrderFact,
    )

    # SELL order that opened the short position
    order = PositionOrderFact(
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
        side="SELL",
        reduce_only=False,
        order_type="MARKET",
        quantity=Decimal("10"),
        executed_quantity=Decimal("10"),
        state=ExchangeOrderState.FILLED,
        client_order_id="c_short_1",
        exchange_order_id="e_short_1",
        created_at=t0,
        updated_at=t0,
        price=Decimal("60000"),
    )

    # When cutover is enabled, short position with negative position_amt
    # should NOT trigger quantity mismatch fallback and successfully
    # return ledger batches
    with patch.dict(os.environ, {"CML_POSITION_LEDGER_PRIMARY_ENABLED": "1"}):
        batches = _build_position_batches(
            position=position,  # type: ignore[arg-type]
            side=StrategySide.SHORT,
            position_side=FuturesPositionSide.BOTH,
            matching_orders=[order],  # type: ignore[arg-type]
            fill_times={"e_short_1": t0},
            fill_prices={"e_short_1": Decimal("60000")},
        )
        assert len(batches) == 1
        assert batches[0].quantity == Decimal("10")
        assert batches[0].entry_price == Decimal("60000")
        assert batches[0].opened_at == t0
        assert isinstance(batches[0], ManagedLivePositionBatch)


def test_position_ledger_shadow_comparator_detects_reconciliation_gap() -> None:
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        PositionKey,
        PositionLedgerBatch,
        PositionLedgerProjection,
    )
    from crypto_momentum_lab.live_rollout.position_ledger_shadow import (
        PositionLedgerShadowComparator,
        ShadowDiffCategory,
    )

    t0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)
    position_key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )

    batches = (
        ManagedLivePositionBatch(
            batch_id="b1",
            quantity=Decimal("10"),
            entry_price=Decimal("100"),
            opened_at=t0,
        ),
    )
    ledger_batches = (
        PositionLedgerBatch(
            batch_id="b1",
            episode_id="ep1",
            quantity=Decimal("10"),
            original_quantity=Decimal("10"),
            entry_price=Decimal("100"),
            opened_at=t0,
        ),
    )

    projection_with_gap = PositionLedgerProjection(
        position_key=position_key,
        active_episode=None,
        active_batches=ledger_batches,
        total_active_quantity=Decimal("10"),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("2.0"),
        high_watermark_trade_at=t0,
    )

    report = PositionLedgerShadowComparator.compare(
        position_key=position_key,
        legacy_batches=batches,
        ledger_projection=projection_with_gap,
    )

    assert report.is_concordant is False
    assert report.category == ShadowDiffCategory.RECONCILIATION_GAP_DETECTED
    assert report.reconciliation_gap == Decimal("2.0")
    assert "reconciliation gap is non-zero" in report.details


def test_position_ledger_shadow_comparator_detects_unallocated_qty() -> None:
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        PositionKey,
        PositionLedgerBatch,
        PositionLedgerProjection,
    )
    from crypto_momentum_lab.live_rollout.position_ledger_shadow import (
        PositionLedgerShadowComparator,
        ShadowDiffCategory,
    )

    t0 = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)
    position_key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )

    batches = (
        ManagedLivePositionBatch(
            batch_id="b1",
            quantity=Decimal("10"),
            entry_price=Decimal("100"),
            opened_at=t0,
        ),
    )
    ledger_batches = (
        PositionLedgerBatch(
            batch_id="b1",
            episode_id="ep1",
            quantity=Decimal("10"),
            original_quantity=Decimal("10"),
            entry_price=Decimal("100"),
            opened_at=t0,
        ),
    )

    projection_with_unallocated = PositionLedgerProjection(
        position_key=position_key,
        active_episode=None,
        active_batches=ledger_batches,
        total_active_quantity=Decimal("10"),
        unallocated_quantity=Decimal("1.5"),
        reconciliation_gap=Decimal("0"),
        high_watermark_trade_at=t0,
    )

    report = PositionLedgerShadowComparator.compare(
        position_key=position_key,
        legacy_batches=batches,
        ledger_projection=projection_with_unallocated,
    )

    assert report.is_concordant is False
    assert report.category == ShadowDiffCategory.UNALLOCATED_QUANTITY_DETECTED
    assert report.unallocated_quantity == Decimal("1.5")
    assert "unallocated quantity is non-zero" in report.details
