from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.strategy_runner.fills import (
    SimulatedFill,
    SimulatedFillStatus,
)


class PaperPositionStatus(StrEnum):
    OPEN = "open"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class PaperExitConfig:
    take_profit_pct: Decimal = Decimal("0.02")
    stop_loss_pct: Decimal = Decimal("0.01")
    max_holding_buckets: int = 80
    state_interval_seconds: int = 15
    initial_balance: Decimal = Decimal("1000")

    def __post_init__(self) -> None:
        if self.take_profit_pct <= 0:
            raise ValueError("take_profit_pct must be positive")
        if self.stop_loss_pct <= 0:
            raise ValueError("stop_loss_pct must be positive")
        if self.max_holding_buckets <= 0:
            raise ValueError("max_holding_buckets must be positive")
        if self.state_interval_seconds <= 0:
            raise ValueError("state_interval_seconds must be positive")
        if self.initial_balance <= 0:
            raise ValueError("initial_balance must be positive")


@dataclass(frozen=True, slots=True)
class PaperPosition:
    position_id: str
    run_id: str
    entry_fill_id: str
    signal_id: str
    symbol: str
    side: StrategySide
    status: PaperPositionStatus
    opened_at: datetime
    closed_at: datetime | None
    entry_price: Decimal
    exit_price: Decimal | None
    quantity: Decimal
    entry_notional: Decimal
    entry_fee: Decimal
    exit_fee: Decimal
    last_mark_price: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal | None
    return_pct: Decimal | None
    close_reason: str | None
    updated_at: datetime


def deterministic_position_id(entry_fill_id: str) -> str:
    if not entry_fill_id:
        raise ValueError("entry_fill_id must not be empty")
    return f"position_{uuid5(NAMESPACE_URL, entry_fill_id)}"


def position_from_entry_fill(
    run_id: str,
    fill: SimulatedFill,
) -> PaperPosition | None:
    if fill.status is not SimulatedFillStatus.FILLED:
        return None
    if (
        fill.filled_at is None
        or fill.fill_price is None
        or fill.quantity is None
        or fill.filled_notional is None
    ):
        raise ValueError("filled entry is missing execution values")
    return PaperPosition(
        position_id=deterministic_position_id(fill.fill_id),
        run_id=run_id,
        entry_fill_id=fill.fill_id,
        signal_id=fill.signal_id,
        symbol=fill.symbol,
        side=fill.side,
        status=PaperPositionStatus.OPEN,
        opened_at=fill.filled_at,
        closed_at=None,
        entry_price=fill.fill_price,
        exit_price=None,
        quantity=fill.quantity,
        entry_notional=fill.filled_notional,
        entry_fee=fill.fee,
        exit_fee=Decimal("0"),
        last_mark_price=fill.fill_price,
        unrealized_pnl=-fill.fee,
        realized_pnl=None,
        return_pct=None,
        close_reason=None,
        updated_at=fill.filled_at,
    )


def mark_positions(
    *,
    positions: tuple[PaperPosition, ...],
    state: MarketState15s,
    config: PaperExitConfig,
    taker_fee_rate: Decimal,
) -> tuple[PaperPosition, ...]:
    mark_price = state.close_price or state.mark_price
    if mark_price is None or mark_price <= 0:
        return ()
    updates: list[PaperPosition] = []
    for position in positions:
        if (
            position.status is not PaperPositionStatus.OPEN
            or position.symbol != state.symbol
            or state.bucket_start <= position.opened_at
        ):
            continue
        gross_pnl = _gross_pnl(position, mark_price)
        unrealized_pnl = gross_pnl - position.entry_fee
        gross_return = gross_pnl / position.entry_notional
        close_reason = _close_reason(
            gross_return=gross_return,
            held_until=state.bucket_start,
            position=position,
            config=config,
        )
        if close_reason is None:
            updates.append(
                replace(
                    position,
                    last_mark_price=mark_price,
                    unrealized_pnl=unrealized_pnl,
                    updated_at=state.bucket_start,
                )
            )
            continue
        exit_notional = position.quantity * mark_price
        exit_fee = exit_notional * taker_fee_rate
        realized_pnl = gross_pnl - position.entry_fee - exit_fee
        updates.append(
            replace(
                position,
                status=PaperPositionStatus.CLOSED,
                closed_at=state.bucket_start,
                exit_price=mark_price,
                exit_fee=exit_fee,
                last_mark_price=mark_price,
                unrealized_pnl=Decimal("0"),
                realized_pnl=realized_pnl,
                return_pct=realized_pnl / position.entry_notional,
                close_reason=close_reason,
                updated_at=state.bucket_start,
            )
        )
    return tuple(updates)


def _gross_pnl(position: PaperPosition, mark_price: Decimal) -> Decimal:
    price_delta = mark_price - position.entry_price
    if position.side is StrategySide.SHORT:
        price_delta = -price_delta
    return position.quantity * price_delta


def _close_reason(
    *,
    gross_return: Decimal,
    held_until: datetime,
    position: PaperPosition,
    config: PaperExitConfig,
) -> str | None:
    if gross_return >= config.take_profit_pct:
        return "take_profit"
    if gross_return <= -config.stop_loss_pct:
        return "stop_loss"
    maximum_holding = timedelta(
        seconds=config.max_holding_buckets * config.state_interval_seconds
    )
    if held_until >= position.opened_at + maximum_holding:
        return "max_holding_period"
    return None
