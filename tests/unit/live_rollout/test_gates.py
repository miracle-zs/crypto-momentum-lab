from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState
from crypto_momentum_lab.domain.risk import RiskConfigSnapshot
from crypto_momentum_lab.live_rollout.gates import (
    LiveGateContext,
    has_entry_order_conflict,
    order_state_is_uncertain,
)

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


def test_order_state_is_uncertain() -> None:
    assert order_state_is_uncertain(ExchangeOrderState.INTENT_APPROVED) is True
    assert order_state_is_uncertain(ExchangeOrderState.FILLED) is False
    assert order_state_is_uncertain(ExchangeOrderState.CANCELED) is False


def test_has_entry_order_conflict_empty() -> None:
    assert has_entry_order_conflict("BTCUSDT", orders=(), states=()) is False












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
        allow_reduce_only_while_draining=True,
        created_at=NOW,
    )
