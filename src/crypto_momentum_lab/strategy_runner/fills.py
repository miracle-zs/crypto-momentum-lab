from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import OrderIntentCandidate, StrategySide


class SimulatedFillStatus(StrEnum):
    FILLED = "filled"
    EXPIRED = "expired"
    REJECTED = "rejected"
    PENDING = "pending"


@dataclass(frozen=True, slots=True)
class ReplayExecutionConfig:
    latency_buckets: int = 1
    state_interval_seconds: int = 15
    taker_fee_rate: Decimal = Decimal("0.0004")
    slippage_bps: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        if self.latency_buckets < 0:
            raise ValueError("latency_buckets must be non-negative")
        if self.state_interval_seconds <= 0:
            raise ValueError("state_interval_seconds must be positive")
        if self.taker_fee_rate < 0:
            raise ValueError("taker_fee_rate must be non-negative")
        if self.slippage_bps < 0:
            raise ValueError("slippage_bps must be non-negative")
        if self.slippage_bps >= Decimal("10000"):
            raise ValueError("slippage_bps must be less than 10000")


@dataclass(frozen=True, slots=True)
class SimulatedFill:
    fill_id: str
    candidate_id: str
    signal_id: str
    symbol: str
    side: StrategySide
    status: SimulatedFillStatus
    target_fill_at: datetime
    filled_at: datetime | None
    requested_notional: Decimal | None
    filled_notional: Decimal | None
    quantity: Decimal | None
    reference_midpoint: Decimal | None
    spread: Decimal | None
    fill_price: Decimal | None
    fee: Decimal
    total_cost: Decimal
    cost_bps: Decimal | None
    reason: str | None

    def __post_init__(self) -> None:
        if not self.fill_id:
            raise ValueError("fill_id must not be empty")
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
        if not self.signal_id:
            raise ValueError("signal_id must not be empty")
        if not self.symbol:
            raise ValueError("symbol must not be empty")
        if not _is_aware(self.target_fill_at):
            raise ValueError("target_fill_at must be timezone-aware")
        if self.filled_at is not None and not _is_aware(self.filled_at):
            raise ValueError("filled_at must be timezone-aware")
        if self.fee < 0:
            raise ValueError("fee must be non-negative")
        if self.total_cost < 0:
            raise ValueError("total_cost must be non-negative")


type FillSummaryValue = int | Decimal


def deterministic_fill_id(*, candidate_id: str) -> str:
    if not candidate_id:
        raise ValueError("candidate_id must not be empty")
    return f"fill_{uuid5(NAMESPACE_URL, candidate_id)}"


def candidate_target_fill_at(
    candidate: OrderIntentCandidate,
    execution: ReplayExecutionConfig,
) -> datetime:
    return candidate.created_at + timedelta(
        seconds=execution.latency_buckets * execution.state_interval_seconds
    )


def simulate_candidate_fills(
    *,
    candidates: tuple[OrderIntentCandidate, ...],
    ordered_states: tuple[MarketState15s, ...],
    execution: ReplayExecutionConfig | None,
) -> tuple[SimulatedFill, ...]:
    if execution is None:
        return ()
    states_by_symbol: dict[str, list[MarketState15s]] = {}
    for state in ordered_states:
        states_by_symbol.setdefault(state.symbol, []).append(state)
    return tuple(
        simulate_candidate_fill(
            candidate=candidate,
            states=tuple(states_by_symbol.get(candidate.symbol, ())),
            execution=execution,
        )
        for candidate in candidates
    )


def simulate_candidate_fill(
    *,
    candidate: OrderIntentCandidate,
    states: tuple[MarketState15s, ...],
    execution: ReplayExecutionConfig,
) -> SimulatedFill:
    target_fill_at = candidate_target_fill_at(candidate, execution)
    if target_fill_at > candidate.expires_at:
        return _unfilled(
            candidate=candidate,
            status=SimulatedFillStatus.EXPIRED,
            target_fill_at=target_fill_at,
            reason="candidate_expired",
        )
    fill_state = next(
        (
            state
            for state in states
            if target_fill_at <= state.bucket_start <= candidate.expires_at
        ),
        None,
    )
    if fill_state is None:
        return _unfilled(
            candidate=candidate,
            status=SimulatedFillStatus.EXPIRED,
            target_fill_at=target_fill_at,
            reason="no_market_state_before_expiry",
        )
    if candidate.desired_notional is None:
        return _unfilled(
            candidate=candidate,
            status=SimulatedFillStatus.REJECTED,
            target_fill_at=target_fill_at,
            reason="missing_desired_notional",
        )
    quote = _marketable_quote(fill_state, candidate.side)
    if quote is None:
        return _unfilled(
            candidate=candidate,
            status=SimulatedFillStatus.REJECTED,
            target_fill_at=target_fill_at,
            reason="missing_fill_price",
        )
    fill_price, midpoint, spread = quote
    fill_price = _apply_slippage(
        fill_price,
        side=candidate.side,
        slippage_bps=execution.slippage_bps,
    )
    if fill_price <= 0:
        return _unfilled(
            candidate=candidate,
            status=SimulatedFillStatus.REJECTED,
            target_fill_at=target_fill_at,
            reason="invalid_fill_price",
        )

    requested_notional = candidate.desired_notional
    quantity = requested_notional / fill_price
    fee = requested_notional * execution.taker_fee_rate
    market_cost = _market_cost(
        fill_price=fill_price,
        midpoint=midpoint,
        quantity=quantity,
        side=candidate.side,
    )
    total_cost = fee + market_cost
    return SimulatedFill(
        fill_id=deterministic_fill_id(candidate_id=candidate.candidate_id),
        candidate_id=candidate.candidate_id,
        signal_id=candidate.signal_id,
        symbol=candidate.symbol,
        side=candidate.side,
        status=SimulatedFillStatus.FILLED,
        target_fill_at=target_fill_at,
        filled_at=fill_state.bucket_start,
        requested_notional=requested_notional,
        filled_notional=requested_notional,
        quantity=quantity,
        reference_midpoint=midpoint,
        spread=spread,
        fill_price=fill_price,
        fee=fee,
        total_cost=total_cost,
        cost_bps=(total_cost / requested_notional) * Decimal("10000"),
        reason="filled",
    )


def pending_candidate_fill(
    *,
    candidate: OrderIntentCandidate,
    execution: ReplayExecutionConfig,
    reason: str,
) -> SimulatedFill:
    if not reason:
        raise ValueError("reason must not be empty")
    return _unfilled(
        candidate=candidate,
        status=SimulatedFillStatus.PENDING,
        target_fill_at=candidate_target_fill_at(candidate, execution),
        reason=reason,
    )


def fill_summary(
    simulated_fills: tuple[SimulatedFill, ...],
) -> dict[str, dict[str, FillSummaryValue]]:
    by_status = Counter(fill.status.value for fill in simulated_fills)
    filled_notional_by_symbol: dict[str, Decimal] = {}
    fee_by_symbol: dict[str, Decimal] = {}
    cost_by_symbol: dict[str, Decimal] = {}
    for fill in simulated_fills:
        if fill.status is not SimulatedFillStatus.FILLED:
            continue
        filled_notional_by_symbol[fill.symbol] = (
            filled_notional_by_symbol.get(fill.symbol, Decimal("0"))
            + (fill.filled_notional or Decimal("0"))
        )
        fee_by_symbol[fill.symbol] = (
            fee_by_symbol.get(fill.symbol, Decimal("0")) + fill.fee
        )
        cost_by_symbol[fill.symbol] = (
            cost_by_symbol.get(fill.symbol, Decimal("0")) + fill.total_cost
        )
    return {
        "fills_by_status": dict(sorted(by_status.items())),
        "filled_notional_by_symbol": dict(sorted(filled_notional_by_symbol.items())),
        "fee_by_symbol": dict(sorted(fee_by_symbol.items())),
        "cost_by_symbol": dict(sorted(cost_by_symbol.items())),
    }


def _unfilled(
    *,
    candidate: OrderIntentCandidate,
    status: SimulatedFillStatus,
    target_fill_at: datetime,
    reason: str,
) -> SimulatedFill:
    return SimulatedFill(
        fill_id=deterministic_fill_id(candidate_id=candidate.candidate_id),
        candidate_id=candidate.candidate_id,
        signal_id=candidate.signal_id,
        symbol=candidate.symbol,
        side=candidate.side,
        status=status,
        target_fill_at=target_fill_at,
        filled_at=None,
        requested_notional=candidate.desired_notional,
        filled_notional=None,
        quantity=None,
        reference_midpoint=None,
        spread=None,
        fill_price=None,
        fee=Decimal("0"),
        total_cost=Decimal("0"),
        cost_bps=None,
        reason=reason,
    )


def _marketable_quote(
    state: MarketState15s,
    side: StrategySide,
) -> tuple[Decimal, Decimal, Decimal | None] | None:
    bid = state.last_bid_price
    ask = state.last_ask_price
    spread = state.spread
    midpoint = state.midpoint
    if midpoint is None and bid is not None and ask is not None:
        midpoint = (bid + ask) / Decimal("2")
    if spread is None and bid is not None and ask is not None:
        spread = ask - bid
    if midpoint is not None and spread is not None:
        half_spread = spread / Decimal("2")
        if bid is None:
            bid = midpoint - half_spread
        if ask is None:
            ask = midpoint + half_spread
    if midpoint is None or midpoint <= 0:
        return None
    if side is StrategySide.LONG:
        if ask is None or ask <= 0:
            return None
        return ask, midpoint, spread
    if bid is None or bid <= 0:
        return None
    return bid, midpoint, spread


def _apply_slippage(
    price: Decimal,
    *,
    side: StrategySide,
    slippage_bps: Decimal,
) -> Decimal:
    multiplier = Decimal("1") + (slippage_bps / Decimal("10000"))
    if side is StrategySide.SHORT:
        multiplier = Decimal("1") - (slippage_bps / Decimal("10000"))
    return price * multiplier


def _market_cost(
    *,
    fill_price: Decimal,
    midpoint: Decimal,
    quantity: Decimal,
    side: StrategySide,
) -> Decimal:
    if side is StrategySide.LONG:
        raw_cost = (fill_price - midpoint) * quantity
    else:
        raw_cost = (midpoint - fill_price) * quantity
    return max(raw_cost, Decimal("0"))


def _is_aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None
