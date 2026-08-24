import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    QualityCategory,
    RawEnvelope,
)
from crypto_momentum_lab.market_data.agg_trade_recovery import (
    AggTradeGapRecoverer,
    agg_trade_gap_quality_event,
)
from crypto_momentum_lab.market_data.binance.rest import BinanceAggTrade


class FakeAggTradeHistory:
    def __init__(self, trades: tuple[BinanceAggTrade, ...]) -> None:
        self.trades = trades
        self.calls: list[tuple[str, int, int]] = []

    async def fetch_agg_trades(
        self,
        symbol: str,
        *,
        from_id: int,
        limit: int,
    ) -> tuple[BinanceAggTrade, ...]:
        self.calls.append((symbol, from_id, limit))
        return tuple(
            trade
            for trade in self.trades
            if trade.aggregate_trade_id >= from_id
        )[:limit]


class SlowAggTradeHistory:
    async def fetch_agg_trades(
        self,
        symbol: str,
        *,
        from_id: int,
        limit: int,
    ) -> tuple[BinanceAggTrade, ...]:
        del symbol, from_id, limit
        await asyncio.sleep(1)
        return ()


async def test_recoverer_inserts_missing_trades_before_live_event() -> None:
    history = FakeAggTradeHistory((_trade(11), _trade(12)))
    recoverer = AggTradeGapRecoverer(history)

    first = await recoverer.expand((_envelope(10),))
    recovered = await recoverer.expand((_envelope(13),))

    assert [item.exchange_sequence for item in first.envelopes] == ["10"]
    assert [item.exchange_sequence for item in recovered.envelopes] == [
        "11",
        "12",
        "13",
    ]
    assert [item.recovered for item in recovered.envelopes] == [True, True, False]
    assert recovered.unrecovered_gaps == ()
    assert history.calls == [("BTCUSDT", 11, 2)]


async def test_recoverer_marks_gap_when_history_is_incomplete() -> None:
    recoverer = AggTradeGapRecoverer(FakeAggTradeHistory((_trade(11),)))
    await recoverer.expand((_envelope(10),))

    result = await recoverer.expand((_envelope(13),))

    assert [item.exchange_sequence for item in result.envelopes] == ["13"]
    assert len(result.unrecovered_gaps) == 1
    gap = result.unrecovered_gaps[0]
    assert gap.symbol == "BTCUSDT"
    assert gap.previous_id == 10
    assert gap.current_id == 13
    assert gap.missing_count == 2


async def test_recoverer_classifies_unrecovered_cross_session_gap() -> None:
    recoverer = AggTradeGapRecoverer(FakeAggTradeHistory(()))
    await recoverer.expand((_envelope(10),))

    result = await recoverer.expand(
        (replace(_envelope(13), connection_session_id=UUID(int=2)),)
    )

    gap = result.unrecovered_gaps[0]
    event = agg_trade_gap_quality_event(gap)
    assert gap.reason == "reconnect_history_incomplete"
    assert event.category is QualityCategory.RECONNECT_GAP
    assert event.details["missing_count"] == 2


async def test_recoverer_resets_symbol_after_intentional_unsubscribe() -> None:
    history = FakeAggTradeHistory(())
    recoverer = AggTradeGapRecoverer(history)
    recoverer.set_monitored_symbols(frozenset({"BTCUSDT"}))
    await recoverer.expand((_envelope(10),))

    recoverer.set_monitored_symbols(frozenset())
    recoverer.set_monitored_symbols(frozenset({"BTCUSDT"}))
    resumed = await recoverer.expand((_envelope(100),))

    assert [item.exchange_sequence for item in resumed.envelopes] == ["100"]
    assert resumed.unrecovered_gaps == ()
    assert history.calls == []
    assert recoverer.metrics.detected_gap_count == 0


async def test_recoverer_bounds_rest_recovery_latency() -> None:
    recoverer = AggTradeGapRecoverer(
        SlowAggTradeHistory(),
        recovery_timeout_seconds=0.001,
    )
    await recoverer.expand((_envelope(10),))

    result = await recoverer.expand((_envelope(13),))

    assert result.unrecovered_gaps[0].reason == "history_timeout"


async def test_recoverer_stops_before_exceeding_rest_request_budget() -> None:
    history = FakeAggTradeHistory((_trade(11), _trade(13)))
    recoverer = AggTradeGapRecoverer(
        history,
        max_requests_per_minute=1,
    )
    await recoverer.expand((_envelope(10),))
    first = await recoverer.expand((_envelope(12),))

    second = await recoverer.expand((_envelope(14),))

    assert first.unrecovered_gaps == ()
    assert second.unrecovered_gaps[0].reason == "request_budget_exhausted"
    assert history.calls == [("BTCUSDT", 11, 1)]


def _trade(aggregate_trade_id: int) -> BinanceAggTrade:
    event_at = datetime(2026, 8, 23, 14, 43, tzinfo=UTC) + timedelta(
        milliseconds=aggregate_trade_id
    )
    return BinanceAggTrade(
        aggregate_trade_id=aggregate_trade_id,
        price=Decimal("100"),
        quantity=Decimal("1"),
        first_trade_id=aggregate_trade_id,
        last_trade_id=aggregate_trade_id,
        event_at=event_at,
        buyer_is_maker=False,
    )


def _envelope(aggregate_trade_id: int) -> RawEnvelope:
    trade = _trade(aggregate_trade_id)
    return RawEnvelope(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        route=CaptureRoute.MARKET,
        stream=CaptureStream.AGG_TRADE,
        symbol="BTCUSDT",
        exchange_event_at=trade.event_at,
        received_at=trade.event_at,
        received_monotonic_ns=aggregate_trade_id,
        connection_session_id=UUID(int=1),
        local_sequence=aggregate_trade_id,
        exchange_sequence=str(aggregate_trade_id),
        subscription_generation=1,
        raw_payload={
            "e": "aggTrade",
            "E": int(trade.event_at.timestamp() * 1000),
            "s": "BTCUSDT",
            "a": aggregate_trade_id,
            "p": "100",
            "q": "1",
            "f": aggregate_trade_id,
            "l": aggregate_trade_id,
            "T": int(trade.event_at.timestamp() * 1000),
            "m": False,
        },
    )
