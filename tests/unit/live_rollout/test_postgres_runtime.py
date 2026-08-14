from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.live_rollout import (
    LIVE_APPROVAL_CONFIRMATION,
    LiveOperatorApproval,
)
from crypto_momentum_lab.domain.risk import RiskConfigSnapshot, StrategyLiveState
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.live_rollout.postgres_runtime import (
    _classify_live_positions,
    _resolve_strategy_live_state,
    live_limits_from_approval,
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


def test_live_limits_preserve_unbounded_capacity() -> None:
    risk_config = RiskConfigSnapshot(
        environment="live",
        account_label="primary",
        max_order_notional=None,
        max_gross_notional=None,
        max_daily_loss=Decimal("25"),
        max_open_positions=None,
        max_market_state_age_seconds=30,
        max_account_state_age_seconds=30,
        allow_reduce_only_while_draining=True,
        created_at=NOW,
    )
    approval = LiveOperatorApproval(
        approval_id="approval-1",
        account_label="primary",
        strategy_name="orderflow_impulse",
        strategy_config_hash="a" * 64,
        risk_config_hash=risk_config.config_hash,
        git_commit_hash="abc123",
        database_migration_revision="20260814_0015",
        approved_notional_cap=None,
        approved_max_open_positions=None,
        approved_max_daily_loss=Decimal("25"),
        approver_name="operator",
        approval_text=LIVE_APPROVAL_CONFIRMATION,
        expires_at=None,
        created_at=NOW,
    )

    limits = live_limits_from_approval(
        approval=approval,
        risk_config=risk_config,
    )

    assert limits == (None, None, Decimal("25"), None)


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
