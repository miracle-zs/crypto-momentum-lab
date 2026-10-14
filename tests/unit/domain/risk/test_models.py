from datetime import UTC, datetime
from decimal import Decimal


from crypto_momentum_lab.domain.risk.models import (
    RiskConfigSnapshot,
)


def test_risk_config_hash_is_deterministic() -> None:
    first = _risk_config(max_order_notional=Decimal("100"))
    second = _risk_config(max_order_notional=Decimal("100.0"))

    assert first.config_hash == second.config_hash


def test_risk_config_allows_unbounded_capacity_limits() -> None:
    config = RiskConfigSnapshot(
        environment="live",
        account_label="primary",
        max_order_notional=None,
        max_gross_notional=None,
        max_daily_loss=None,
        max_open_positions=None,
        allow_reduce_only_while_draining=True,
        created_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )

    assert config.max_order_notional is None
    assert config.max_gross_notional is None
    assert config.max_open_positions is None
    assert config.max_daily_loss is None






def _risk_config(
    max_order_notional: Decimal | None,
    allow_reduce_only_while_draining: bool = True,
) -> RiskConfigSnapshot:
    return RiskConfigSnapshot(
        environment="live",
        account_label="primary",
        max_order_notional=max_order_notional,
        max_gross_notional=Decimal("500"),
        max_daily_loss=Decimal("25"),
        max_open_positions=1,
        allow_reduce_only_while_draining=allow_reduce_only_while_draining,
        created_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )
