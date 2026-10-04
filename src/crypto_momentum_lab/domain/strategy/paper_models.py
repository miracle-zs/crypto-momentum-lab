from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.strategy.models import (
    OrderIntentCandidate,
    StrategyCheckpoint,
    StrategyRunIdentity,
    StrategySide,
    StrategySignal,
)
from crypto_momentum_lab.domain.strategy.position_exit import PositionExitMode


class SimulatedFillStatus(StrEnum):
    FILLED = "filled"
    EXPIRED = "expired"
    REJECTED = "rejected"
    PENDING = "pending"


@dataclass(frozen=True, slots=True)
class ReplayExecutionConfig:
    latency_buckets: int = 1
    state_interval_seconds: int = 15
    taker_fee_rate: Decimal = Decimal("0.0005")
    slippage_bps: Decimal = Decimal("0")
    require_market_quote: bool = False

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


class PaperPositionStatus(StrEnum):
    OPEN = "open"
    CLOSED = "closed"


PaperExitMode = PositionExitMode


@dataclass(frozen=True, slots=True)
class PaperExitConfig:
    max_holding_buckets: int = 80
    state_interval_seconds: int = 15
    initial_balance: Decimal = Decimal("1000")
    exit_mode: PaperExitMode = PaperExitMode.CANDLE_15M
    require_executable_quote: bool = False
    candle_minimum_holding_buckets: int = 0
    candle_confirmation_count: int = 1
    candle_grace_bars: int = 0
    candle_grace_profit_pct: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        if self.max_holding_buckets <= 0:
            raise ValueError("max_holding_buckets must be positive")
        if self.state_interval_seconds <= 0:
            raise ValueError("state_interval_seconds must be positive")
        if self.initial_balance <= 0:
            raise ValueError("initial_balance must be positive")
        if self.candle_minimum_holding_buckets < 0:
            raise ValueError("candle_minimum_holding_buckets must not be negative")
        if self.candle_confirmation_count <= 0:
            raise ValueError("candle_confirmation_count must be positive")
        if self.candle_grace_bars < 0:
            raise ValueError("candle_grace_bars must not be negative")
        if not Decimal("0") <= self.candle_grace_profit_pct < Decimal("1"):
            raise ValueError("candle_grace_profit_pct must be in the range [0, 1)")


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
    last_candle_end: datetime | None = None


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


@dataclass(frozen=True, slots=True)
class PaperEntryFilterConfig:
    allow_long: bool = True
    allow_short: bool = True
    max_abs_aggressive_imbalance: Decimal | None = None
    max_cluster_trade_count: int | None = None
    require_price_above_ema5: bool = False
    require_price_above_ema10: bool = False

    def __post_init__(self) -> None:
        if not self.allow_long and not self.allow_short:
            raise ValueError("entry filter must allow at least one side")
        if self.max_abs_aggressive_imbalance is not None and not Decimal(
            "0"
        ) < self.max_abs_aggressive_imbalance <= Decimal("1"):
            raise ValueError("max_abs_aggressive_imbalance must be in (0, 1]")
        if (
            self.max_cluster_trade_count is not None
            and self.max_cluster_trade_count <= 0
        ):
            raise ValueError("max_cluster_trade_count must be positive")


@dataclass(frozen=True, slots=True)
class PaperTradingRunReport:
    schema_version: int
    generated_at: datetime
    run: StrategyRunIdentity
    execution_config: ReplayExecutionConfig
    source_description: str
    input_state_count: int
    processed_symbol_count: int
    signals: tuple[StrategySignal, ...]
    candidates: tuple[OrderIntentCandidate, ...]
    paper_fills: tuple[SimulatedFill, ...]
    pending_candidate_count: int
    rejection_summary: dict[str, dict[str, int]]
    final_checkpoint: StrategyCheckpoint
    summary_counts: dict[str, dict[str, int]]
    fill_summary: dict[str, dict[str, FillSummaryValue]]
    portfolio_config: PaperExitConfig = field(default_factory=PaperExitConfig)
    paper_positions: tuple[PaperPosition, ...] = ()


def _is_aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None
