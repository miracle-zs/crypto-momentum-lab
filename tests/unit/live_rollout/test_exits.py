from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution import FuturesPositionSide
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitConfig,
    LiveExitManager,
    ManagedLivePosition,
)
from crypto_momentum_lab.strategy_runner.position_exit import (
    ClosedCandle15m,
    PositionExitMode,
    PositionExitPolicy,
)
from tests.unit.shadow_operation.test_service import _state


async def test_fixed_exit_closes_exact_position_quantity() -> None:
    state = replace(
        _state(),
        bucket_start=datetime(2026, 7, 4, 0, 5, tzinfo=UTC),
        bucket_end=datetime(2026, 7, 4, 0, 5, 15, tzinfo=UTC),
        last_bid_price=Decimal("98"),
        mark_price=Decimal("98"),
        close_price=Decimal("98"),
    )
    manager = LiveExitManager(config=_config(PositionExitMode.FIXED))

    requests = await manager.requests_for_state(state, (_long_position(),))

    assert len(requests) == 1
    request = requests[0]
    assert request.quantity == Decimal("1.25")
    assert request.candidate.reduce_only is True
    assert request.candidate.side is StrategySide.LONG
    assert request.candidate.reason == "stop_loss"


async def test_candle_exit_retries_until_a_close_order_is_observed() -> None:
    candle = ClosedCandle15m(
        symbol="BTCUSDT",
        candle_start=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        candle_end=datetime(2026, 7, 4, 0, 15, tzinfo=UTC),
        open_price=Decimal("100"),
        close_price=Decimal("101"),
    )
    loader = FakeCandleLoader((candle,))
    manager = LiveExitManager(
        config=_config(PositionExitMode.CANDLE_15M),
        candle_loader=loader,
    )
    state = replace(
        _state(),
        bucket_start=datetime(2026, 7, 4, 0, 15, tzinfo=UTC),
        bucket_end=datetime(2026, 7, 4, 0, 15, 15, tzinfo=UTC),
        last_ask_price=Decimal("101"),
        mark_price=Decimal("101"),
        close_price=Decimal("101"),
    )
    position = replace(
        _long_position(),
        side=StrategySide.SHORT,
        position_side=FuturesPositionSide.SHORT,
    )

    first = await manager.requests_for_state(state, (position,))
    second = await manager.requests_for_state(state, (position,))

    assert len(first) == 1
    assert first[0].candidate.reason == "candle_15m_bullish"
    assert len(second) == 1
    assert second[0].candidate.candidate_id == first[0].candidate.candidate_id
    assert loader.calls == 2


class FakeCandleLoader:
    def __init__(self, candles: tuple[ClosedCandle15m, ...]) -> None:
        self._candles = candles
        self.calls = 0

    async def load_closed_candles(self, **kwargs) -> tuple[ClosedCandle15m, ...]:
        del kwargs
        self.calls += 1
        return self._candles


def _config(mode: PositionExitMode) -> LiveExitConfig:
    return LiveExitConfig(
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v1",
        strategy_config_hash="a" * 64,
        policy=PositionExitPolicy(
            take_profit_pct=Decimal("0.02"),
            stop_loss_pct=Decimal("0.01"),
            max_holding_seconds=86400,
            mode=mode,
        ),
    )


def _long_position() -> ManagedLivePosition:
    return ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("1.25"),
        entry_price=Decimal("100"),
        opened_at=datetime(2026, 7, 4, 0, 1, tzinfo=UTC),
    )
