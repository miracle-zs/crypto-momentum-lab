from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.risk.models import (
    RiskConfigSnapshot,
    TradingLease,
    TradingLeaseState,
)


def test_risk_config_hash_is_deterministic() -> None:
    first = _risk_config(max_order_notional=Decimal("100"))
    second = _risk_config(max_order_notional=Decimal("100.0"))

    assert first.config_hash == second.config_hash


def test_trading_lease_rejects_invalid_state() -> None:
    with pytest.raises(ValueError, match="state must be a TradingLeaseState"):
        TradingLease(
            lease_id="lease-1",
            environment="live",
            account_label="primary",
            strategy_name="compression_breakout",
            owner="worker-1",
            state="active",
            acquired_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
            expires_at=datetime(2026, 7, 4, 0, 1, tzinfo=UTC),
        )


def test_trading_lease_requires_expiration_after_acquisition() -> None:
    with pytest.raises(ValueError, match="expires_at must be after acquired_at"):
        TradingLease(
            lease_id="lease-1",
            environment="live",
            account_label="primary",
            strategy_name="compression_breakout",
            owner="worker-1",
            state=TradingLeaseState.ACTIVE,
            acquired_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
            expires_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        )


def _risk_config(max_order_notional: Decimal) -> RiskConfigSnapshot:
    return RiskConfigSnapshot(
        environment="live",
        account_label="primary",
        max_order_notional=max_order_notional,
        max_gross_notional=Decimal("500"),
        max_daily_loss=Decimal("25"),
        max_open_positions=1,
        max_market_state_age_seconds=30,
        max_account_state_age_seconds=30,
        allow_reduce_only_while_draining=True,
        created_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )
