from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class FixedLiveLimits:
    notional_cap: Decimal | None
    max_open_positions: int | None
    max_daily_loss: Decimal | None
    max_gross_exposure: Decimal | None


@dataclass(frozen=True, slots=True)
class LiveLimitContext:
    symbol: str
    requested_notional: Decimal | None
    open_position_symbols: frozenset[str] | None
    realized_pnl: Decimal | None
    unrealized_pnl: Decimal | None
    gross_exposure: Decimal | None
    min_notional: Decimal | None
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
    assert context.min_notional is not None
    if context.has_unresolved_order:
        return LiveLimitDecision(False, "unresolved_order_uncertainty", None)
    if (
        limits.max_open_positions is not None
        and len(context.open_position_symbols) >= limits.max_open_positions
        and context.symbol not in context.open_position_symbols
    ):
        return LiveLimitDecision(False, "max_open_positions_exceeded", None)
    daily_pnl = context.realized_pnl + context.unrealized_pnl
    if limits.max_daily_loss is not None and daily_pnl <= -limits.max_daily_loss:
        return LiveLimitDecision(False, "max_daily_loss_reached", None)
    if (
        limits.max_gross_exposure is not None
        and context.gross_exposure >= limits.max_gross_exposure
    ):
        return LiveLimitDecision(False, "max_gross_exposure_reached", None)
    capped = context.requested_notional
    if limits.notional_cap is not None:
        capped = min(capped, limits.notional_cap)
    if limits.max_gross_exposure is not None:
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
        "min_notional": context.min_notional,
    }
    for field_name, value in required.items():
        if value is None:
            return f"missing_{field_name}"
    return None
