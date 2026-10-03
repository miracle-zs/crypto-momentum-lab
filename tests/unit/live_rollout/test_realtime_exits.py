from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.market.models import RealtimeMarketQuote
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.domain.strategy.position_exit import (
    PositionExitMode,
    PositionExitPolicy,
)
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitConfig,
    LiveExitManager,
    ManagedLivePosition,
)


async def test_requests_for_quote_returns_empty_tuple() -> None:
    manager = LiveExitManager(
        config=LiveExitConfig(
            run_id="run-1",
            strategy_name="strategy",
            strategy_version="v1",
            strategy_config_hash="hash",
            policy=PositionExitPolicy(
                mode=PositionExitMode.CANDLE_15M,
            ),
        ),
    )
    opened_at = datetime(2026, 8, 23, 0, 0, tzinfo=UTC)
    quote = RealtimeMarketQuote(
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        event_at=opened_at + timedelta(minutes=1),
        received_at=opened_at + timedelta(minutes=1),
        bid_price=Decimal("90"),
        ask_price=Decimal("91"),
    )
    position = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("1"),
        entry_price=Decimal("100"),
        opened_at=opened_at,
    )

    requests = await manager.requests_for_quote(quote, (position,))

    assert requests == ()


async def test_candle_exit_mode_ignores_realtime_quotes() -> None:
    manager = LiveExitManager(
        config=LiveExitConfig(
            run_id="run-1",
            strategy_name="strategy",
            strategy_version="v1",
            strategy_config_hash="hash",
            policy=PositionExitPolicy(
                mode=PositionExitMode.CANDLE_15M,
            ),
        ),
    )
    opened_at = datetime(2026, 8, 23, 0, 0, tzinfo=UTC)
    position = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("1"),
        entry_price=Decimal("100"),
        opened_at=opened_at,
    )
    take_profit_quote = RealtimeMarketQuote(
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        event_at=opened_at + timedelta(seconds=1),
        received_at=opened_at + timedelta(seconds=1),
        bid_price=Decimal("102.01"),
        ask_price=Decimal("102.02"),
    )
    stop_loss_quote = RealtimeMarketQuote(
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        event_at=opened_at + timedelta(seconds=2),
        received_at=opened_at + timedelta(seconds=2),
        bid_price=Decimal("98.99"),
        ask_price=Decimal("99.00"),
    )

    take_profit_requests = await manager.requests_for_quote(
        take_profit_quote,
        (position,),
    )
    stop_loss_requests = await manager.requests_for_quote(
        stop_loss_quote,
        (position,),
    )

    assert take_profit_requests == ()
    assert stop_loss_requests == ()
