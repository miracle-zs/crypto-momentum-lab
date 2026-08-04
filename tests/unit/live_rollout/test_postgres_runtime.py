from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.risk import StrategyLiveState
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.live_rollout.postgres_runtime import (
    _classify_live_positions,
    _resolve_strategy_live_state,
)

NOW = datetime(2026, 8, 4, 0, 0, tzinfo=UTC)


def test_classifies_position_opened_by_current_run_as_managed() -> None:
    managed, unmanaged = _classify_live_positions(
        [_position()],
        [_order(reduce_only=False, side="BUY")],
    )

    assert unmanaged == frozenset()
    assert len(managed) == 1
    assert managed[0].side is StrategySide.LONG
    assert managed[0].quantity == Decimal("0.5")
    assert managed[0].closing_order_filled is False


def test_marks_external_position_as_unmanaged() -> None:
    managed, unmanaged = _classify_live_positions([_position()], [])

    assert managed == ()
    assert unmanaged == frozenset({"BTCUSDT"})


def test_filled_close_suppresses_duplicate_exit_during_account_sync_lag() -> None:
    managed, unmanaged = _classify_live_positions(
        [_position()],
        [
            _order(
                reduce_only=True,
                side="SELL",
                updated_at=NOW + timedelta(seconds=10),
            ),
            _order(reduce_only=False, side="BUY"),
        ],
    )

    assert unmanaged == frozenset()
    assert managed[0].closing_order_filled is True


def test_draining_control_survives_a_later_operational_halt() -> None:
    assert (
        _resolve_strategy_live_state("draining", "halted")
        is StrategyLiveState.DRAINING
    )


def _position():
    return SimpleNamespace(
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("0.5"),
        entry_price=Decimal("100"),
    )


def _order(
    *,
    reduce_only: bool,
    side: str,
    updated_at: datetime = NOW,
):
    return SimpleNamespace(
        state=ExchangeOrderState.FILLED.value,
        reduce_only=reduce_only,
        symbol="BTCUSDT",
        position_side="LONG",
        side=side,
        updated_at=updated_at,
    )
