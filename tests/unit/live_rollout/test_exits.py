from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution import (
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.market.models import RealtimeMarketQuote
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitCancellationRequest,
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
        candle_start=datetime(2026, 7, 4, 0, 15, tzinfo=UTC),
        candle_end=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
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
        bucket_start=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
        bucket_end=datetime(2026, 7, 4, 0, 30, 15, tzinfo=UTC),
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


async def test_live_candle_exit_ignores_the_entry_candle() -> None:
    entry_candle = ClosedCandle15m(
        symbol="BTCUSDT",
        candle_start=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        candle_end=datetime(2026, 7, 4, 0, 15, tzinfo=UTC),
        open_price=Decimal("100"),
        close_price=Decimal("99"),
    )
    manager = LiveExitManager(
        config=_config(PositionExitMode.CANDLE_15M),
        candle_loader=FakeCandleLoader((entry_candle,)),
    )
    state = replace(
        _state(),
        bucket_start=datetime(2026, 7, 4, 0, 15, tzinfo=UTC),
        bucket_end=datetime(2026, 7, 4, 0, 15, 15, tzinfo=UTC),
        last_bid_price=Decimal("99"),
        mark_price=Decimal("99"),
        close_price=Decimal("99"),
    )

    requests = await manager.requests_for_state(state, (_long_position(),))

    assert requests == ()


async def test_b1_adverse_candle_places_only_recovery_limit() -> None:
    candle = ClosedCandle15m(
        symbol="BTCUSDT",
        candle_start=datetime(2026, 7, 4, 0, 15, tzinfo=UTC),
        candle_end=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
        open_price=Decimal("100"),
        close_price=Decimal("99"),
    )
    manager = LiveExitManager(
        config=_config(
            PositionExitMode.CANDLE_15M,
            candle_grace_bars=1,
            candle_grace_profit_pct=Decimal("0.0088"),
        ),
        candle_loader=FakeCandleLoader((candle,)),
    )
    state = replace(
        _state(),
        bucket_start=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
        bucket_end=datetime(2026, 7, 4, 0, 30, 15, tzinfo=UTC),
        last_bid_price=Decimal("99"),
        mark_price=Decimal("99"),
        close_price=Decimal("99"),
    )

    requests = await manager.requests_for_state(state, (_long_position(),))

    assert len(requests) == 1
    recovery = requests[0]
    assert recovery.candidate.entry_type is EntryType.LIMIT
    assert recovery.candidate.limit_price == Decimal("100.8800")
    assert recovery.candidate.reason.endswith("grace_limit_1")


async def test_b1_adverse_candle_uses_direct_market_close_at_target() -> None:
    candle = ClosedCandle15m(
        symbol="BTCUSDT",
        candle_start=datetime(2026, 7, 4, 0, 15, tzinfo=UTC),
        candle_end=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
        open_price=Decimal("105"),
        close_price=Decimal("102"),
    )
    manager = LiveExitManager(
        config=_config(
            PositionExitMode.CANDLE_15M,
            candle_grace_bars=1,
            candle_grace_profit_pct=Decimal("0.0088"),
        ),
        candle_loader=FakeCandleLoader((candle,)),
    )
    state = replace(
        _state(),
        bucket_start=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
        bucket_end=datetime(2026, 7, 4, 0, 30, 15, tzinfo=UTC),
        last_bid_price=Decimal("101"),
        mark_price=Decimal("101"),
        close_price=Decimal("101"),
    )

    requests = await manager.requests_for_state(state, (_long_position(),))

    assert len(requests) == 1
    direct = requests[0]
    assert direct.candidate.entry_type is EntryType.MARKET
    assert direct.candidate.limit_price is None
    assert direct.candidate.reason == "candle_15m_bearish"


async def test_b1_uses_lower_decision_threshold_and_keeps_recovery_target() -> None:
    candle = ClosedCandle15m(
        symbol="BTCUSDT",
        candle_start=datetime(2026, 7, 4, 0, 15, tzinfo=UTC),
        candle_end=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
        open_price=Decimal("105"),
        close_price=Decimal("102"),
    )
    manager = LiveExitManager(
        config=_config(
            PositionExitMode.CANDLE_15M,
            candle_grace_bars=1,
            candle_grace_decision_profit_pct=Decimal("0.001"),
            candle_grace_profit_pct=Decimal("0.0088"),
        ),
        candle_loader=FakeCandleLoader((candle,)),
    )
    state = replace(
        _state(),
        bucket_start=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
        bucket_end=datetime(2026, 7, 4, 0, 30, 15, tzinfo=UTC),
        last_bid_price=Decimal("100.20"),
        mark_price=Decimal("100.20"),
        close_price=Decimal("100.20"),
    )

    requests = await manager.requests_for_state(state, (_long_position(),))

    assert len(requests) == 1
    direct = requests[0]
    assert direct.candidate.entry_type is EntryType.MARKET
    assert direct.candidate.limit_price is None


async def test_b1_grace_timeout_cancels_limit_before_market_close() -> None:
    recovery_plan = OrderExecutionPlan(
        intent_id="recovery-intent",
        run_id="run-1",
        client_order_id="recovery-client",
        symbol="BTCUSDT",
        side="SELL",
        order_type="LIMIT",
        quantity=Decimal("1.25"),
        price=Decimal("100.88"),
        reduce_only=True,
        created_at=datetime(2026, 7, 4, 0, 15, 15, tzinfo=UTC),
        position_side=FuturesPositionSide.LONG,
        quantized=True,
    )
    manager = LiveExitManager(
        config=_config(
            PositionExitMode.CANDLE_15M,
            candle_grace_bars=1,
            candle_grace_profit_pct=Decimal("0.0088"),
        ),
        candle_loader=FakeCandleLoader(()),
    )
    position = replace(
        _long_position(),
        recovery_order_client_id=recovery_plan.client_order_id,
        recovery_order_created_at=recovery_plan.created_at,
        recovery_order_plan=recovery_plan,
    )
    state = replace(
        _state(),
        bucket_start=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
        bucket_end=datetime(2026, 7, 4, 0, 30, 15, tzinfo=UTC),
        last_bid_price=Decimal("98"),
        mark_price=Decimal("98"),
        close_price=Decimal("98"),
    )

    requests = await manager.requests_for_state(state, (position,))

    assert len(requests) == 1
    assert isinstance(requests[0], LiveExitCancellationRequest)
    assert requests[0].cancel_plan.client_order_id == "recovery-client"
    assert requests[0].fallback_candidate.entry_type is EntryType.MARKET
    assert requests[0].fallback_candidate.reason == "candle_15m_grace_timeout_1"


async def test_grace_timeout_timer_does_not_need_a_new_market_state() -> None:
    created_at = datetime(2026, 7, 4, 0, 15, 15, tzinfo=UTC)
    recovery_plan = OrderExecutionPlan(
        intent_id="recovery-intent",
        run_id="run-1",
        client_order_id="recovery-client",
        symbol="BTCUSDT",
        side="SELL",
        order_type="LIMIT",
        quantity=Decimal("1.25"),
        price=Decimal("100.88"),
        reduce_only=True,
        created_at=created_at,
        position_side=FuturesPositionSide.LONG,
        quantized=True,
    )
    position = replace(
        _long_position(),
        recovery_order_client_id=recovery_plan.client_order_id,
        recovery_order_created_at=created_at,
        recovery_order_plan=recovery_plan,
    )
    manager = LiveExitManager(
        config=_config(
            PositionExitMode.CANDLE_15M,
            candle_grace_bars=1,
            candle_grace_profit_pct=Decimal("0.0088"),
        )
    )
    state = replace(
        _state(),
        bucket_end=created_at,
        last_bid_price=Decimal("98"),
        close_price=Decimal("98"),
        mark_price=Decimal("98"),
    )
    now = datetime(2026, 7, 4, 0, 30, 16, tzinfo=UTC)

    requests = await manager.requests_for_grace_timeout(
        now=now,
        state=state,
        positions=(position,),
    )

    assert len(requests) == 1
    assert isinstance(requests[0], LiveExitCancellationRequest)
    assert requests[0].fallback_candidate.created_at == now
    assert requests[0].fallback_candidate.features["trigger_at"] == (
        "2026-07-04T00:30:16+00:00"
    )


async def test_closed_candle_path_does_not_require_a_rest_loader() -> None:
    manager = LiveExitManager(config=_config(PositionExitMode.CANDLE_15M))
    candle = ClosedCandle15m(
        symbol="BTCUSDT",
        candle_start=datetime(2026, 7, 4, 0, 15, tzinfo=UTC),
        candle_end=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
        open_price=Decimal("100"),
        close_price=Decimal("99"),
    )
    quote = RealtimeMarketQuote(
        exchange="binance-usdm",
        environment="live",
        symbol="BTCUSDT",
        event_at=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
        received_at=datetime(2026, 7, 4, 0, 30, 0, 100000, tzinfo=UTC),
        bid_price=Decimal("99"),
        ask_price=Decimal("99.1"),
    )

    requests = await manager.requests_for_closed_candle(
        candle,
        (_long_position(),),
        latest_quote=quote,
        received_at=quote.received_at,
    )

    assert len(requests) == 1
    assert requests[0].candidate.reason == "candle_15m_bearish"
    assert requests[0].candidate.created_at == quote.received_at
    assert requests[0].candidate.features["trigger_at"] == (
        "2026-07-04T00:30:00+00:00"
    )


async def test_stale_recovery_limit_does_not_block_a_new_position_episode() -> None:
    recovery_created_at = datetime(2026, 7, 4, 0, 15, tzinfo=UTC)
    position = replace(
        _long_position(),
        opened_at=datetime(2026, 7, 4, 0, 20, tzinfo=UTC),
        recovery_order_client_id="stale-recovery",
        recovery_order_created_at=recovery_created_at,
        recovery_order_plan=OrderExecutionPlan(
            intent_id="recovery-intent",
            run_id="run-1",
            client_order_id="stale-recovery",
            symbol="BTCUSDT",
            side="SELL",
            order_type="LIMIT",
            quantity=Decimal("1.25"),
            price=Decimal("100.88"),
            reduce_only=True,
            created_at=recovery_created_at,
            position_side=FuturesPositionSide.LONG,
            quantized=True,
        ),
    )
    manager = LiveExitManager(
        config=_config(
            PositionExitMode.CANDLE_15M,
            candle_grace_bars=1,
            candle_grace_profit_pct=Decimal("0.0088"),
        )
    )
    candle = ClosedCandle15m(
        symbol="BTCUSDT",
        candle_start=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
        candle_end=datetime(2026, 7, 4, 0, 45, tzinfo=UTC),
        open_price=Decimal("100"),
        close_price=Decimal("99"),
    )

    requests = await manager.requests_for_closed_candle(candle, (position,))

    assert len(requests) == 1
    assert requests[0].candidate.reason.endswith("grace_limit_1")

    timeout_requests = await manager.requests_for_grace_timeout(
        now=datetime(2026, 7, 4, 0, 30, 1, tzinfo=UTC),
        state=replace(
            _state(),
            bucket_end=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
            last_bid_price=Decimal("99"),
            mark_price=Decimal("99"),
            close_price=Decimal("99"),
        ),
        positions=(position,),
    )

    assert len(timeout_requests) == 1
    assert isinstance(timeout_requests[0], LiveExitCancellationRequest)
    assert timeout_requests[0].cancel_plan.client_order_id == "stale-recovery"


async def test_closed_candle_path_ignores_the_entry_candle() -> None:
    manager = LiveExitManager(config=_config(PositionExitMode.CANDLE_15M))
    entry_candle = ClosedCandle15m(
        symbol="BTCUSDT",
        candle_start=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        candle_end=datetime(2026, 7, 4, 0, 15, tzinfo=UTC),
        open_price=Decimal("100"),
        close_price=Decimal("99"),
    )

    requests = await manager.requests_for_closed_candle(
        entry_candle,
        (_long_position(),),
    )

    assert requests == ()


class FakeCandleLoader:
    def __init__(self, candles: tuple[ClosedCandle15m, ...]) -> None:
        self._candles = candles
        self.calls = 0

    async def load_closed_candles(self, **kwargs) -> tuple[ClosedCandle15m, ...]:
        del kwargs
        self.calls += 1
        return self._candles


def _config(
    mode: PositionExitMode,
    *,
    candle_grace_bars: int = 0,
    candle_grace_decision_profit_pct: Decimal | None = None,
    candle_grace_profit_pct: Decimal = Decimal("0"),
) -> LiveExitConfig:
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
        candle_grace_bars=candle_grace_bars,
        candle_grace_decision_profit_pct=candle_grace_decision_profit_pct,
        candle_grace_profit_pct=candle_grace_profit_pct,
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
