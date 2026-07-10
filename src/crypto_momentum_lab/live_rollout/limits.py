from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class FixedLiveLimits:
    notional_cap: Decimal
    max_open_positions: int
    max_daily_loss: Decimal
    max_gross_exposure: Decimal
    max_spread: Decimal
    cooldown_seconds: int
    max_account_age_seconds: float
    max_market_age_seconds: float


@dataclass(frozen=True, slots=True)
class LiveLimitContext:
    now: datetime
    symbol: str
    requested_notional: Decimal | None
    open_position_symbols: frozenset[str] | None
    last_entry_at: datetime | None
    realized_pnl: Decimal | None
    unrealized_pnl: Decimal | None
    gross_exposure: Decimal | None
    spread: Decimal | None
    min_notional: Decimal | None
    account_observed_at: datetime | None
    market_observed_at: datetime | None
    has_unresolved_order: bool


@dataclass(frozen=True, slots=True)
class LiveLimitDecision:
    allowed: bool
    reason: str
    capped_notional: Decimal | None


def evaluate_fixed_live_limits(
    limits: FixedLiveLimits,
    context: LiveLimitContext,
) -> LiveLimitDecision:
    missing = _missing_reason(context)
    if missing is not None:
        return LiveLimitDecision(False, missing, None)
    assert context.requested_notional is not None
    assert context.open_position_symbols is not None
    assert context.realized_pnl is not None
    assert context.unrealized_pnl is not None
    assert context.gross_exposure is not None
    assert context.spread is not None
    assert context.min_notional is not None
    assert context.account_observed_at is not None
    assert context.market_observed_at is not None
    if context.has_unresolved_order:
        return LiveLimitDecision(False, "unresolved_order_uncertainty", None)
    if (
        len(context.open_position_symbols) >= limits.max_open_positions
        and context.symbol not in context.open_position_symbols
    ):
        return LiveLimitDecision(False, "max_open_positions_exceeded", None)
    if context.last_entry_at is not None and context.now - context.last_entry_at < (
        timedelta(seconds=limits.cooldown_seconds)
    ):
        return LiveLimitDecision(False, "symbol_cooldown_active", None)
    daily_pnl = context.realized_pnl + context.unrealized_pnl
    if daily_pnl <= -limits.max_daily_loss:
        return LiveLimitDecision(False, "max_daily_loss_reached", None)
    if context.gross_exposure >= limits.max_gross_exposure:
        return LiveLimitDecision(False, "max_gross_exposure_reached", None)
    if context.spread > limits.max_spread:
        return LiveLimitDecision(False, "spread_too_wide", None)
    if _age(context.now, context.account_observed_at) > limits.max_account_age_seconds:
        return LiveLimitDecision(False, "stale_account_state", None)
    if _age(context.now, context.market_observed_at) > limits.max_market_age_seconds:
        return LiveLimitDecision(False, "stale_market_state", None)
    capped = min(context.requested_notional, limits.notional_cap)
    remaining_exposure = limits.max_gross_exposure - context.gross_exposure
    capped = min(capped, remaining_exposure)
    if capped < context.min_notional:
        return LiveLimitDecision(False, "below_min_notional", None)
    return LiveLimitDecision(True, "approved", capped)


def _missing_reason(context: LiveLimitContext) -> str | None:
    required = {
        "requested_notional": context.requested_notional,
        "open_position_symbols": context.open_position_symbols,
        "realized_pnl": context.realized_pnl,
        "unrealized_pnl": context.unrealized_pnl,
        "gross_exposure": context.gross_exposure,
        "spread": context.spread,
        "min_notional": context.min_notional,
        "account_observed_at": context.account_observed_at,
        "market_observed_at": context.market_observed_at,
    }
    for field_name, value in required.items():
        if value is None:
            return f"missing_{field_name}"
    return None


def _age(now: datetime, observed_at: datetime) -> float:
    return (now - observed_at).total_seconds()
