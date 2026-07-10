from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    RiskDecision,
    RiskEvaluation,
    RiskHalt,
    StrategyLiveState,
    StrategyLiveStateRecord,
    TradingLease,
    TradingLeaseState,
)
from crypto_momentum_lab.persistence.postgres.risk_repository import (
    risk_config_row,
    risk_evaluation_row,
    risk_halt_row,
    strategy_live_state_row,
    trading_lease_row,
)

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


def test_trading_lease_row_preserves_owner_and_expiration() -> None:
    lease = _lease()

    row = trading_lease_row(lease)

    assert row["state"] == "active"
    assert row["owner"] == "worker-1"
    assert row["expires_at"] == NOW + timedelta(minutes=1)


def test_risk_config_row_preserves_numeric_limits() -> None:
    config = _config()

    row = risk_config_row(config)

    assert row["config_hash"] == config.config_hash
    assert row["max_order_notional"] == Decimal("100.00")
    assert row["max_daily_loss"] == Decimal("25.00")


def test_risk_evaluation_row_preserves_rejection_reason_and_inputs() -> None:
    evaluation = RiskEvaluation(
        evaluation_id="evaluation-1",
        candidate_id="candidate-1",
        decision=RiskDecision.REJECTED,
        reason="max_order_notional_exceeded",
        evaluated_at=NOW,
        details={"desired_notional": "125.50", "limit": "100.00"},
    )

    row = risk_evaluation_row(evaluation)

    assert row["decision"] == "rejected"
    assert row["reason"] == "max_order_notional_exceeded"
    assert row["details"] == {
        "desired_notional": "125.50",
        "limit": "100.00",
    }


def test_halt_and_strategy_state_rows_use_enum_values() -> None:
    halt = RiskHalt(
        halt_id="halt-1",
        environment="live",
        account_label="primary",
        reason="operator_stop",
        active=True,
        created_at=NOW,
        details={},
    )
    state = StrategyLiveStateRecord(
        environment="live",
        account_label="primary",
        strategy_name="compression_breakout",
        state=StrategyLiveState.DRAINING,
        changed_at=NOW,
        reason="operator_stop",
    )

    assert risk_halt_row(halt)["active"] is True
    assert strategy_live_state_row(state)["state"] == "draining"


def _lease() -> TradingLease:
    return TradingLease(
        lease_id="lease-1",
        environment="live",
        account_label="primary",
        strategy_name="compression_breakout",
        owner="worker-1",
        state=TradingLeaseState.ACTIVE,
        acquired_at=NOW,
        expires_at=NOW + timedelta(minutes=1),
    )


def _config() -> RiskConfigSnapshot:
    return RiskConfigSnapshot(
        environment="live",
        account_label="primary",
        max_order_notional=Decimal("100.00"),
        max_gross_notional=Decimal("500.00"),
        max_daily_loss=Decimal("25.00"),
        max_open_positions=1,
        max_market_state_age_seconds=30,
        max_account_state_age_seconds=30,
        allow_reduce_only_while_draining=True,
        created_at=NOW,
    )
