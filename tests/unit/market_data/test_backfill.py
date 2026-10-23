"""Synthesize promotion backfill states from public aggTrades."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.market_data.backfill import (
    PromotionHistoryBackfiller,
    synthesize_states_from_trades,
)
from crypto_momentum_lab.market_data.binance.rest import BinanceAggTrade


def _trade(i: int, *, at: datetime, maker: bool) -> BinanceAggTrade:
    return BinanceAggTrade(
        aggregate_trade_id=i,
        price=Decimal("1") + Decimal(i) / Decimal("100"),
        quantity=Decimal("2"),
        first_trade_id=i,
        last_trade_id=i,
        event_at=at,
        buyer_is_maker=maker,
    )


def test_synthesize_states_buckets_trades_into_15s() -> None:
    t0 = datetime(2026, 9, 16, 12, 0, 0, tzinfo=UTC)
    trades = (
        _trade(1, at=t0 + timedelta(seconds=1), maker=False),
        _trade(2, at=t0 + timedelta(seconds=5), maker=True),
        _trade(3, at=t0 + timedelta(seconds=20), maker=False),
    )
    states = synthesize_states_from_trades("BTCUSDT", trades, environment="research")
    assert len(states) == 2
    assert states[0].bucket_start == t0
    assert states[0].trade_count == 2
    assert states[0].data_complete is False
    assert states[0].is_backfill is True
    assert states[0].aggressive_buy_notional > 0
    assert states[0].aggressive_sell_notional > 0
    assert states[1].bucket_start == t0 + timedelta(seconds=15)
    assert states[1].trade_count == 1
    assert states[1].is_backfill is True


class _Client:
    def __init__(self) -> None:
        self.requested_symbols: list[str] = []

    async def fetch_agg_trades_window(self, symbol, *, start, end, limit=1000):
        self.requested_symbols.append(symbol)
        if symbol != "S1USDT":
            return ()
        t0 = datetime(2026, 9, 16, 12, 0, 0, tzinfo=UTC)
        return (_trade(1, at=t0 + timedelta(seconds=1), maker=False),)


class _Publisher:
    def __init__(self) -> None:
        self.batches = []

    async def publish(self, states):
        self.batches.append(states)


async def test_backfill_publishes_oldest_first_states() -> None:
    client = _Client()
    publisher = _Publisher()
    backfiller = PromotionHistoryBackfiller(
        client=client,
        publisher=publisher,
        environment="research",
        lookback=timedelta(minutes=35),
    )
    await backfiller.backfill_symbols(
        ["S1USDT", "S2USDT"],
        now=datetime(2026, 9, 16, 12, 35, tzinfo=UTC),
    )
    assert client.requested_symbols == ["S1USDT", "S2USDT"]
    assert len(publisher.batches) == 1
    assert len(publisher.batches[0]) == 1
    assert publisher.batches[0][0].symbol == "S1USDT"
