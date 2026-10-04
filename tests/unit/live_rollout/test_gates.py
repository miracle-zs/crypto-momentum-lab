from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.live_rollout import (
    LiveGateStatus,
)
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
)
from crypto_momentum_lab.live_rollout.gates import LiveGateContext, evaluate_live_gate

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


def test_live_gate_rejects_when_live_submit_disabled() -> None:
    decision = evaluate_live_gate(replace(_context(), live_submit_enabled=False))

    assert "live_submit_disabled" in decision.reasons


def test_enabled_trading_is_admitted() -> None:
    decision = evaluate_live_gate(_context())

    assert decision.status is LiveGateStatus.APPROVED
    assert decision.reasons == ()












def _context() -> LiveGateContext:
    return LiveGateContext(
        live_submit_enabled=True,
        account_label="primary",
        strategy_name="compression_breakout",
        strategy_config_hash="a" * 64,
    )


def _risk_config() -> RiskConfigSnapshot:
    return RiskConfigSnapshot(
        environment="live",
        account_label="primary",
        max_order_notional=Decimal("25"),
        max_gross_notional=Decimal("25"),
        max_daily_loss=Decimal("10"),
        max_open_positions=1,
        max_market_state_age_seconds=30,
        max_account_state_age_seconds=30,
        allow_reduce_only_while_draining=True,
        created_at=NOW,
    )
