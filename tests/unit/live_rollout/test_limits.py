from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.live_rollout.limits import (
    FixedLiveLimits,
    LiveLimitContext,
    evaluate_fixed_live_limits,
)

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


def test_fixed_notional_limit_caps_entry_size() -> None:
    decision = evaluate_fixed_live_limits(_limits(), _context())

    assert decision.allowed is True
    assert decision.capped_notional == Decimal("25")


def test_one_position_limit_blocks_second_symbol() -> None:
    decision = evaluate_fixed_live_limits(
        _limits(),
        replace(_context(), open_position_symbols=frozenset({"ETHUSDT"})),
    )

    assert decision.reason == "max_open_positions_exceeded"


def test_unresolved_order_halts_new_entries() -> None:
    decision = evaluate_fixed_live_limits(
        _limits(),
        replace(_context(), has_unresolved_order=True),
    )

    assert decision.reason == "unresolved_order_uncertainty"


def test_unbounded_capacity_limits_keep_strategy_requested_notional() -> None:
    decision = evaluate_fixed_live_limits(
        replace(
            _limits(),
            notional_cap=None,
            max_open_positions=None,
            max_gross_exposure=None,
        ),
        replace(
            _context(),
            open_position_symbols=frozenset({"ETHUSDT", "SOLUSDT"}),
            gross_exposure=Decimal("10000"),
        ),
    )

    assert decision.allowed is True
    assert decision.capped_notional == Decimal("100")


def test_unbounded_daily_loss_does_not_reject_entry() -> None:
    decision = evaluate_fixed_live_limits(
        replace(
            _limits(),
            max_daily_loss=None,
        ),
        replace(
            _context(),
            realized_pnl=Decimal("-100000"),
            unrealized_pnl=Decimal("-100000"),
        ),
    )

    assert decision.allowed is True


def _limits() -> FixedLiveLimits:
    return FixedLiveLimits(
        notional_cap=Decimal("25"),
        max_open_positions=1,
        max_daily_loss=Decimal("10"),
        max_gross_exposure=Decimal("25"),
        max_spread=Decimal("2"),
        cooldown_seconds=300,
        max_account_age_seconds=30,
        max_market_age_seconds=30,
    )


def _context() -> LiveLimitContext:
    return LiveLimitContext(
        now=NOW,
        symbol="BTCUSDT",
        requested_notional=Decimal("100"),
        open_position_symbols=frozenset(),
        last_entry_at=None,
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        gross_exposure=Decimal("0"),
        spread=Decimal("1"),
        min_notional=Decimal("5"),
        account_observed_at=NOW - timedelta(seconds=5),
        market_observed_at=NOW - timedelta(seconds=5),
        has_unresolved_order=False,
    )
