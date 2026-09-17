"""REST backfill of 15s market states for symbols that promote into T0/T1.

The live aggregator drops events that arrive after the durable close, so a
symbol that jumps from the watch tier cannot warm its order-flow buffer from
late WebSocket traffic.  This module fetches a bounded window of public
aggTrades and synthesizes closed 15s states that are published straight to the
Hub, oldest first, so live-strategy can warm up before the first live bucket.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.market_data.binance.rest import BinanceAggTrade

log = structlog.get_logger()

_BUCKET_SECONDS = 15
_DEFAULT_LOOKBACK = timedelta(minutes=35)
_MAX_PAGES_PER_SYMBOL = 8


class AggTradeWindowClient(Protocol):
    async def fetch_agg_trades_window(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> tuple[BinanceAggTrade, ...]: ...


class HistoricalStatePublisher(Protocol):
    async def publish(
        self,
        states: tuple[MarketState15s, ...],
        entered_symbols: frozenset[str] = frozenset(),
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class SymbolBackfillReport:
    symbol: str
    buckets: int
    trades: int
    failure: str | None = None


def _bucket_start(event_at: datetime) -> datetime:
    normalized = event_at.astimezone(UTC)
    return normalized.replace(
        second=(normalized.second // _BUCKET_SECONDS) * _BUCKET_SECONDS,
        microsecond=0,
    )


def synthesize_states_from_trades(
    symbol: str,
    trades: tuple[BinanceAggTrade, ...],
    *,
    environment: str,
) -> tuple[MarketState15s, ...]:
    """Build closed 15s states from public aggTrades (oldest first)."""

    if not trades:
        return ()
    ordered = sorted(trades, key=lambda item: item.event_at)
    buckets: dict[datetime, dict[str, object]] = {}
    for trade in ordered:
        key = _bucket_start(trade.event_at)
        acc = buckets.setdefault(
            key,
            {
                "trade_count": 0,
                "trade_notional": Decimal("0"),
                "aggressive_buy_notional": Decimal("0"),
                "aggressive_sell_notional": Decimal("0"),
                "open_price": trade.price,
                "high_price": trade.price,
                "low_price": trade.price,
                "close_price": trade.price,
                "first_received_at": trade.event_at,
                "last_received_at": trade.event_at,
                "liquidation_count": 0,
                "liquidation_notional": Decimal("0"),
            },
        )
        notional = trade.price * trade.quantity
        acc["trade_count"] = int(acc["trade_count"]) + 1
        acc["trade_notional"] = Decimal(acc["trade_notional"]) + notional
        if trade.buyer_is_maker:
            acc["aggressive_sell_notional"] = (
                Decimal(acc["aggressive_sell_notional"]) + notional
            )
        else:
            acc["aggressive_buy_notional"] = (
                Decimal(acc["aggressive_buy_notional"]) + notional
            )
        acc["high_price"] = max(Decimal(acc["high_price"]), trade.price)
        acc["low_price"] = min(Decimal(acc["low_price"]), trade.price)
        acc["close_price"] = trade.price
        acc["last_received_at"] = max(
            trade.event_at, acc["last_received_at"]  # type: ignore[arg-type]
        )

    states: list[MarketState15s] = []
    for key in sorted(buckets):
        acc = buckets[key]
        bucket_end = key + timedelta(seconds=_BUCKET_SECONDS)
        states.append(
            MarketState15s(
                schema_version=1,
                exchange="binance-usdm",
                environment=environment,
                symbol=symbol.upper(),
                bucket_start=key,
                bucket_end=bucket_end,
                open_price=acc["open_price"],  # type: ignore[arg-type]
                high_price=acc["high_price"],  # type: ignore[arg-type]
                low_price=acc["low_price"],  # type: ignore[arg-type]
                close_price=acc["close_price"],  # type: ignore[arg-type]
                trade_count=acc["trade_count"],  # type: ignore[arg-type]
                trade_notional=acc["trade_notional"],  # type: ignore[arg-type]
                aggressive_buy_notional=acc["aggressive_buy_notional"],  # type: ignore[arg-type]
                aggressive_sell_notional=acc["aggressive_sell_notional"],  # type: ignore[arg-type]
                last_bid_price=None,
                last_ask_price=None,
                spread=None,
                midpoint=None,
                liquidation_count=0,
                liquidation_notional=Decimal("0"),
                mark_price=None,
                closed_kline_count=0,
                source_event_count=acc["trade_count"],  # type: ignore[arg-type]
                first_received_at=acc["first_received_at"],  # type: ignore[arg-type]
                last_received_at=acc["last_received_at"],  # type: ignore[arg-type]
                data_complete=False,
                missing_agg_trade_count=0,
                is_backfill=True,
            )
        )
    return tuple(states)


class PromotionHistoryBackfiller:
    """Backfill closed 15s states when a symbol newly joins the trade tier."""

    def __init__(
        self,
        *,
        client: AggTradeWindowClient,
        publisher: HistoricalStatePublisher,
        environment: str,
        lookback: timedelta = _DEFAULT_LOOKBACK,
        max_symbols_per_batch: int = 4,
    ) -> None:
        if lookback <= timedelta(0):
            raise ValueError("lookback must be positive")
        if max_symbols_per_batch <= 0:
            raise ValueError("max_symbols_per_batch must be positive")
        self._client = client
        self._publisher = publisher
        self._environment = environment
        self._lookback = lookback
        self._max_symbols_per_batch = max_symbols_per_batch

    async def backfill_symbols(
        self,
        symbols: Collection[str],
        *,
        now: datetime | None = None,
    ) -> tuple[SymbolBackfillReport, ...]:
        ordered = sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})
        if not ordered:
            return ()
        batch = ordered[: self._max_symbols_per_batch]
        if len(ordered) > self._max_symbols_per_batch:
            log.warning(
                "promotion_backfill_batch_truncated",
                requested=len(ordered),
                running=len(batch),
            )
        end = (datetime.now(UTC) if now is None else now.astimezone(UTC))
        start = end - self._lookback
        reports: list[SymbolBackfillReport] = []
        for symbol in batch:
            try:
                trades = await self._fetch_window(symbol, start=start, end=end)
                states = synthesize_states_from_trades(
                    symbol,
                    trades,
                    environment=self._environment,
                )
                if states:
                    await self._publisher.publish(
                        states,
                        entered_symbols=frozenset({symbol}),
                    )
                reports.append(
                    SymbolBackfillReport(
                        symbol=symbol,
                        buckets=len(states),
                        trades=len(trades),
                    )
                )
                log.info(
                    "promotion_backfill_completed",
                    symbol=symbol,
                    buckets=len(states),
                    trades=len(trades),
                    lookback_seconds=self._lookback.total_seconds(),
                )
            except Exception as error:
                log.warning(
                    "promotion_backfill_failed",
                    symbol=symbol,
                    error_type=type(error).__name__,
                    error=str(error),
                )
                reports.append(
                    SymbolBackfillReport(
                        symbol=symbol,
                        buckets=0,
                        trades=0,
                        failure=type(error).__name__,
                    )
                )
        return tuple(reports)

    async def _fetch_window(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
    ) -> tuple[BinanceAggTrade, ...]:
        collected: list[BinanceAggTrade] = []
        cursor = start
        for _page in range(_MAX_PAGES_PER_SYMBOL):
            page = await self._client.fetch_agg_trades_window(
                symbol,
                start=cursor,
                end=end,
                limit=1000,
            )
            if not page:
                break
            collected.extend(page)
            newest = max(item.event_at for item in page)
            if len(page) < 1000 or newest <= cursor:
                break
            cursor = newest + timedelta(milliseconds=1)
            if cursor >= end:
                break
        # De-duplicate by aggregate id in case pages overlap.
        by_id: dict[int, BinanceAggTrade] = {
            item.aggregate_trade_id: item for item in collected
        }
        return tuple(by_id[key] for key in sorted(by_id))
