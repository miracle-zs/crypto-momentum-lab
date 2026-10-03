"""Shared market and risk fixtures for Live and execution tests."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.execution.order_rules import SymbolTradingRules
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    RiskHalt,
    StrategyLiveState,
    TradingLease,
    TradingLeaseState,
)
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategyCheckpoint,
    StrategyDecision,
    StrategySide,
    StrategySignal,
)

NOW = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class RiskFixtureContext:
    now: datetime
    active_lease: TradingLease | None
    account_state: ExecutionAccountStatus
    open_position_symbols: frozenset[str]
    active_halts: tuple[RiskHalt, ...]
    risk_config: RiskConfigSnapshot
    strategy_state: StrategyLiveState
    trading_rules: dict[str, SymbolTradingRules]


class FakeStrategy:
    def warm_market_state(self, state) -> None:
        pass

    def reset_symbol(self, symbol: str) -> None:
        pass

    def required_data(self) -> None:
        return None

    def on_market_state(self, state: MarketState15s) -> StrategyDecision:
        return StrategyDecision(
            signals=(_signal(),),
            candidates=(_intent(),),
            rejections=(),
            checkpoint=StrategyCheckpoint({}, {}, {}, {}),
        )

    def checkpoint(
        self, *, include_market_state_buffers: bool = True
    ) -> StrategyCheckpoint:
        return StrategyCheckpoint({}, {}, {}, {})


def _context(
    *,
    active_lease: TradingLease | None | object = "default",
    account_state: ExecutionAccountStatus = ExecutionAccountStatus.READY_READONLY,
    now: datetime = NOW,
) -> RiskFixtureContext:
    lease = _lease() if active_lease == "default" else active_lease
    return RiskFixtureContext(
        now=now,
        active_lease=lease,
        account_state=account_state,
        open_position_symbols=frozenset(),
        active_halts=(),
        risk_config=RiskConfigSnapshot(
            environment="live",
            account_label="primary",
            max_order_notional=Decimal("100"),
            max_gross_notional=Decimal("500"),
            max_daily_loss=Decimal("25"),
            max_open_positions=1,
            max_market_state_age_seconds=30,
            max_account_state_age_seconds=30,
            allow_reduce_only_while_draining=True,
            created_at=NOW,
        ),
        strategy_state=StrategyLiveState.ACTIVE,
        trading_rules={
            "BTCUSDT": SymbolTradingRules(
                symbol="BTCUSDT",
                tick_size=Decimal("0.1"),
                step_size=Decimal("0.001"),
                min_quantity=Decimal("0.001"),
                max_quantity=Decimal("100"),
                min_notional=Decimal("5"),
            )
        },
    )


def _lease() -> TradingLease:
    return TradingLease(
        lease_id="lease-1",
        environment="live",
        account_label="primary",
        strategy_name="compression_breakout",
        owner="shadow-worker",
        code_generation="test-generation",
        state=TradingLeaseState.ACTIVE,
        acquired_at=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(minutes=5),
    )


def _intent() -> OrderIntentCandidate:
    return OrderIntentCandidate(
        candidate_id="candidate-1",
        signal_id="signal-1",
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v1",
        config_hash="a" * 64,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("100"),
        reduce_only=False,
        expires_at=NOW + timedelta(seconds=30),
        created_at=NOW,
        reason="test",
        features={},
    )


def _signal() -> StrategySignal:
    return StrategySignal(
        signal_id="signal-1",
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v1",
        config_hash="a" * 64,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        detected_at=NOW,
        source_state_at=NOW,
        reason="test",
        features={},
        reference_prices={},
    )


def _state() -> MarketState15s:
    start = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="live",
        symbol="BTCUSDT",
        bucket_start=start,
        bucket_end=start + timedelta(seconds=15),
        open_price=Decimal("30000"),
        high_price=Decimal("30000"),
        low_price=Decimal("30000"),
        close_price=Decimal("30000"),
        trade_count=1,
        trade_notional=Decimal("100"),
        aggressive_buy_notional=Decimal("60"),
        aggressive_sell_notional=Decimal("40"),
        last_bid_price=Decimal("29999"),
        last_ask_price=Decimal("30001"),
        spread=Decimal("2"),
        midpoint=Decimal("30000"),
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=Decimal("30000"),
        closed_kline_count=0,
        source_event_count=1,
        first_received_at=start,
        last_received_at=start + timedelta(seconds=15),
    )
