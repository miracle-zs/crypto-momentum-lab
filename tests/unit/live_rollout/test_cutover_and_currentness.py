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
