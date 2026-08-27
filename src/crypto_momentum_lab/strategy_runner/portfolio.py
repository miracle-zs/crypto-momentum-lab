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
from crypto_momentum_lab.strategy_runner.position_exit import (
    ClosedCandle15m,
    PositionExitMode,
    PositionExitPolicy,
    first_candle_start_after_entry,
    position_exit_reason,
)

__all__ = ["ClosedCandle15m"]


class PaperPositionStatus(StrEnum):
    OPEN = "open"
    CLOSED = "closed"


PaperExitMode = PositionExitMode


@dataclass(frozen=True, slots=True)
class PaperExitConfig:
    take_profit_pct: Decimal = Decimal("0.02")
    stop_loss_pct: Decimal = Decimal("0.01")
    max_holding_buckets: int = 80
    state_interval_seconds: int = 15
    initial_balance: Decimal = Decimal("1000")
    exit_mode: PaperExitMode = PaperExitMode.FIXED
    require_executable_quote: bool = False
    candle_minimum_holding_buckets: int = 0
    candle_confirmation_count: int = 1
    candle_grace_bars: int = 0
    candle_grace_profit_pct: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        if not isinstance(self.exit_mode, PaperExitMode):
            object.__setattr__(self, "exit_mode", PaperExitMode(self.exit_mode))
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
        if self.candle_minimum_holding_buckets < 0:
            raise ValueError(
                "candle_minimum_holding_buckets must not be negative"
            )
        if self.candle_confirmation_count <= 0:
            raise ValueError("candle_confirmation_count must be positive")
        if self.candle_grace_bars < 0:
            raise ValueError("candle_grace_bars must not be negative")
        if not Decimal("0") <= self.candle_grace_profit_pct < Decimal("1"):
            raise ValueError(
                "candle_grace_profit_pct must be in the range [0, 1)"
            )


@dataclass(slots=True)
class _OfficialCandleAccumulator:
    candle_start: datetime
    open_price: Decimal
    close_price: Decimal
    closed_minute_starts: set[datetime]


class Candle15mAggregator:
    def __init__(self) -> None:
        self._candles: dict[str, _OfficialCandleAccumulator] = {}
        self._last_closed_candle_start: dict[str, datetime] = {}

    def observe(self, state: MarketState15s) -> ClosedCandle15m | None:
        if (
            state.closed_kline_1m_open_time is None
            or state.closed_kline_1m_open_price is None
            or state.closed_kline_1m_close_price is None
        ):
            return None

        minute_start = state.closed_kline_1m_open_time
        candle_start = _candle_start_15m(minute_start)
        last_closed_start = self._last_closed_candle_start.get(state.symbol)
        if last_closed_start is not None and candle_start <= last_closed_start:
            return None
        current = self._candles.get(state.symbol)
        if current is None:
            current = _new_official_candle_accumulator(
                candle_start=candle_start,
                open_price=state.closed_kline_1m_open_price,
                close_price=state.closed_kline_1m_close_price,
            )
            self._candles[state.symbol] = current
        elif candle_start < current.candle_start:
            return None
        elif candle_start > current.candle_start:
            current = _new_official_candle_accumulator(
                candle_start=candle_start,
                open_price=state.closed_kline_1m_open_price,
                close_price=state.closed_kline_1m_close_price,
            )
            self._candles[state.symbol] = current

        if minute_start in current.closed_minute_starts:
            return None
        current.closed_minute_starts.add(minute_start)
        current.close_price = state.closed_kline_1m_close_price
        final_minute_start = candle_start + timedelta(minutes=14)
        if minute_start != final_minute_start:
            return None

        expected_minutes = {
            candle_start + timedelta(minutes=offset)
            for offset in range(15)
        }
        if current.closed_minute_starts != expected_minutes:
            return None
        closed = ClosedCandle15m(
            symbol=state.symbol,
            candle_start=candle_start,
            candle_end=candle_start + timedelta(minutes=15),
            open_price=current.open_price,
            close_price=current.close_price,
        )
        self._last_closed_candle_start[state.symbol] = candle_start
        self._candles.pop(state.symbol, None)
        return closed


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
    grace_exit_started_at: datetime | None
    grace_exit_deadline: datetime | None
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
        grace_exit_started_at=None,
        grace_exit_deadline=None,
        updated_at=fill.filled_at,
    )


def mark_positions(
    *,
    positions: tuple[PaperPosition, ...],
    state: MarketState15s,
    config: PaperExitConfig,
    taker_fee_rate: Decimal,
    closed_candle: ClosedCandle15m | None = None,
    closed_candles: tuple[ClosedCandle15m, ...] = (),
) -> tuple[PaperPosition, ...]:
    updates: list[PaperPosition] = []
    observed_at = state.bucket_end
    for position in positions:
        if (
            position.status is not PaperPositionStatus.OPEN
            or position.symbol != state.symbol
            or observed_at <= position.opened_at
        ):
            continue
        mark_price = _position_mark_price(
            state,
            position.side,
            require_executable_quote=config.require_executable_quote,
        )
        if mark_price is None:
            continue
        gross_pnl = _gross_pnl(position, mark_price)
        unrealized_pnl = gross_pnl - position.entry_fee
        gross_return = gross_pnl / position.entry_notional
        if (
            config.exit_mode is PaperExitMode.CANDLE_15M
            and config.candle_grace_bars > 0
        ):
            grace_update = _apply_candle_grace_exit(
                position=position,
                state=state,
                mark_price=mark_price,
                unrealized_pnl=unrealized_pnl,
                closed_candle=closed_candle,
                config=config,
                taker_fee_rate=taker_fee_rate,
            )
            if grace_update is not None:
                updates.append(grace_update)
                continue
        close_reason = _close_reason(
            gross_return=gross_return,
            held_until=observed_at,
            position=position,
            config=config,
            closed_candle=closed_candle,
            closed_candles=closed_candles,
        )
        if close_reason is None:
            updates.append(
                replace(
                    position,
                    last_mark_price=mark_price,
                    unrealized_pnl=unrealized_pnl,
                    updated_at=observed_at,
                )
            )
            continue
        exit_price = mark_price
        closed_at = observed_at
        if close_reason.startswith("candle_15m_"):
            if closed_candle is None:
                raise AssertionError("candle exit requires a closed candle")
            exit_price = closed_candle.close_price
            closed_at = closed_candle.candle_end
            gross_pnl = _gross_pnl(position, exit_price)
        exit_notional = position.quantity * exit_price
        exit_fee = exit_notional * taker_fee_rate
        realized_pnl = gross_pnl - position.entry_fee - exit_fee
        updates.append(
            replace(
                position,
                status=PaperPositionStatus.CLOSED,
                closed_at=closed_at,
                exit_price=exit_price,
                exit_fee=exit_fee,
                last_mark_price=exit_price,
                unrealized_pnl=Decimal("0"),
                realized_pnl=realized_pnl,
                return_pct=realized_pnl / position.entry_notional,
                close_reason=close_reason,
                updated_at=closed_at,
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
    closed_candle: ClosedCandle15m | None,
    closed_candles: tuple[ClosedCandle15m, ...],
) -> str | None:
    if (
        config.exit_mode is PaperExitMode.CANDLE_15M
        and config.candle_grace_bars > 0
    ):
        if held_until >= position.opened_at + timedelta(
            seconds=config.max_holding_buckets * config.state_interval_seconds
        ):
            return "max_holding_period"
        return None
    return position_exit_reason(
        gross_return=gross_return,
        held_until=held_until,
        opened_at=position.opened_at,
        symbol=position.symbol,
        side=position.side,
        policy=PositionExitPolicy(
            take_profit_pct=config.take_profit_pct,
            stop_loss_pct=config.stop_loss_pct,
            max_holding_seconds=(
                config.max_holding_buckets * config.state_interval_seconds
            ),
            mode=config.exit_mode,
            minimum_holding_seconds=(
                config.candle_minimum_holding_buckets
                * config.state_interval_seconds
            ),
            candle_confirmation_count=config.candle_confirmation_count,
        ),
        closed_candle=closed_candle,
        closed_candles=closed_candles,
    )


def _apply_candle_grace_exit(
    *,
    position: PaperPosition,
    state: MarketState15s,
    mark_price: Decimal,
    unrealized_pnl: Decimal,
    closed_candle: ClosedCandle15m | None,
    config: PaperExitConfig,
    taker_fee_rate: Decimal,
) -> PaperPosition | None:
    """Apply the profitable-close or grace path after the first adverse candle.

    A zero grace value keeps the original B0 behavior.  For B1/B8, an adverse
    candle only arms a recovery limit when neither the official candle close
    nor the current executable mark can realize a net profit.  A quote touch
    closes at the executable mark once the configured recovery threshold is
    reached; if the configured number of subsequent candles elapses first,
    the position is closed at that mark.
    """

    started_at = position.grace_exit_started_at
    deadline = position.grace_exit_deadline
    if started_at is None:
        if not _is_adverse_candle(position, closed_candle):
            return None
        assert closed_candle is not None
        profitable_exit_price = _first_adverse_profit_exit_price(
            position=position,
            mark_price=mark_price,
            closed_candle=closed_candle,
            taker_fee_rate=taker_fee_rate,
        )
        if profitable_exit_price is not None:
            return _close_at_price(
                position=position,
                closed_at=closed_candle.candle_end,
                exit_price=profitable_exit_price,
                close_reason=_adverse_candle_reason(position),
                taker_fee_rate=taker_fee_rate,
            )
        started_at = closed_candle.candle_end
        deadline = started_at + timedelta(
            minutes=15 * config.candle_grace_bars
        )
    elif deadline is None:
        deadline = started_at + timedelta(minutes=15 * config.candle_grace_bars)

    if state.bucket_end >= position.opened_at + timedelta(
        seconds=config.max_holding_buckets * config.state_interval_seconds
    ):
        return _close_at_price(
            position=position,
            closed_at=state.bucket_end,
            exit_price=mark_price,
            close_reason="max_holding_period",
            taker_fee_rate=taker_fee_rate,
        )

    recovery_limit = _grace_recovery_limit_price(
        position=position,
        profit_pct=config.candle_grace_profit_pct,
    )
    if _entry_limit_touched(position.side, mark_price, recovery_limit):
        return _close_at_price(
            position=position,
            closed_at=state.bucket_end,
            exit_price=mark_price,
            close_reason=(
                f"candle_15m_grace_limit_{config.candle_grace_bars}"
            ),
            taker_fee_rate=taker_fee_rate,
        )

    if (
        closed_candle is not None
        and deadline is not None
        and closed_candle.candle_end >= deadline
    ):
        return _close_at_price(
            position=position,
            closed_at=state.bucket_end,
            exit_price=mark_price,
            close_reason=(
                f"candle_15m_grace_timeout_{config.candle_grace_bars}"
            ),
            taker_fee_rate=taker_fee_rate,
        )

    return replace(
        position,
        last_mark_price=mark_price,
        unrealized_pnl=unrealized_pnl,
        grace_exit_started_at=started_at,
        grace_exit_deadline=deadline,
        updated_at=state.bucket_end,
    )


def _first_adverse_profit_exit_price(
    *,
    position: PaperPosition,
    mark_price: Decimal,
    closed_candle: ClosedCandle15m,
    taker_fee_rate: Decimal,
) -> Decimal | None:
    """Return the first adverse-candle price that is net profitable.

    B0 prices candle exits from the official 15m close.  Prefer that same
    close for a profitable first warning so the paired accounts share the
    warning decision.  If the official close is not profitable but the
    current executable mark has recovered into profit, exit at the mark.
    """

    if (
        _realized_pnl_at_price(
            position=position,
            exit_price=closed_candle.close_price,
            taker_fee_rate=taker_fee_rate,
        )
        > 0
    ):
        return closed_candle.close_price
    if (
        _realized_pnl_at_price(
            position=position,
            exit_price=mark_price,
            taker_fee_rate=taker_fee_rate,
        )
        > 0
    ):
        return mark_price
    return None


def _adverse_candle_reason(position: PaperPosition) -> str:
    return (
        "candle_15m_bearish"
        if position.side is StrategySide.LONG
        else "candle_15m_bullish"
    )


def _is_adverse_candle(
    position: PaperPosition,
    candle: ClosedCandle15m | None,
) -> bool:
    if candle is None or candle.symbol != position.symbol:
        return False
    if candle.candle_start < first_candle_start_after_entry(position.opened_at):
        return False
    if position.side is StrategySide.LONG:
        return candle.close_price < candle.open_price
    return candle.close_price > candle.open_price


def _entry_limit_touched(
    side: StrategySide,
    mark_price: Decimal,
    limit_price: Decimal,
) -> bool:
    if side is StrategySide.LONG:
        return mark_price >= limit_price
    return mark_price <= limit_price


def _grace_recovery_limit_price(
    *,
    position: PaperPosition,
    profit_pct: Decimal,
) -> Decimal:
    multiplier = (
        Decimal("1") + profit_pct
        if position.side is StrategySide.LONG
        else Decimal("1") - profit_pct
    )
    return position.entry_price * multiplier


def _realized_pnl_at_price(
    *,
    position: PaperPosition,
    exit_price: Decimal,
    taker_fee_rate: Decimal,
) -> Decimal:
    exit_fee = position.quantity * exit_price * taker_fee_rate
    return _gross_pnl(position, exit_price) - position.entry_fee - exit_fee


def _close_at_price(
    *,
    position: PaperPosition,
    closed_at: datetime,
    exit_price: Decimal,
    close_reason: str,
    taker_fee_rate: Decimal,
) -> PaperPosition:
    exit_notional = position.quantity * exit_price
    exit_fee = exit_notional * taker_fee_rate
    realized_pnl = _realized_pnl_at_price(
        position=position,
        exit_price=exit_price,
        taker_fee_rate=taker_fee_rate,
    )
    return replace(
        position,
        status=PaperPositionStatus.CLOSED,
        closed_at=closed_at,
        exit_price=exit_price,
        exit_fee=exit_fee,
        last_mark_price=exit_price,
        unrealized_pnl=Decimal("0"),
        realized_pnl=realized_pnl,
        return_pct=realized_pnl / position.entry_notional,
        close_reason=close_reason,
        grace_exit_started_at=None,
        grace_exit_deadline=None,
        updated_at=closed_at,
    )


def _new_official_candle_accumulator(
    *,
    candle_start: datetime,
    open_price: Decimal,
    close_price: Decimal,
) -> _OfficialCandleAccumulator:
    return _OfficialCandleAccumulator(
        candle_start=candle_start,
        open_price=open_price,
        close_price=close_price,
        closed_minute_starts=set(),
    )


def _position_mark_price(
    state: MarketState15s,
    side: StrategySide,
    *,
    require_executable_quote: bool,
) -> Decimal | None:
    if require_executable_quote:
        price = (
            state.last_bid_price
            if side is StrategySide.LONG
            else state.last_ask_price
        )
        return price if price is not None and price > 0 else None
    mark_price = state.close_price or state.mark_price
    return mark_price if mark_price is not None and mark_price > 0 else None


def _candle_start_15m(value: datetime) -> datetime:
    return value.replace(
        minute=value.minute - value.minute % 15,
        second=0,
        microsecond=0,
    )
