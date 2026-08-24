from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import (
    AggressorSide,
    MarketState15s,
    NormalizedAggTrade,
    NormalizedBookTicker,
    NormalizedKline1m,
    NormalizedLiquidation,
    NormalizedMarketEvent,
    NormalizedMarkPrice,
)

_BUCKET_SECONDS = 15


def bucket_start_15s(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("value must be timezone-aware")
    utc_value = value.astimezone(UTC)
    second = (utc_value.second // _BUCKET_SECONDS) * _BUCKET_SECONDS
    return utc_value.replace(second=second, microsecond=0)


def aggregate_market_states_15s(
    events: Iterable[NormalizedMarketEvent],
    *,
    initial_quotes: Mapping[str, tuple[Decimal, Decimal]] | None = None,
) -> tuple[MarketState15s, ...]:
    buckets: dict[tuple[str, datetime], MarketState15sAccumulator] = {}
    for event in events:
        start = bucket_start_15s(event.event_at)
        key = (event.symbol, start)
        accumulator = buckets.get(key)
        if accumulator is None:
            accumulator = MarketState15sAccumulator.for_bucket(event)
            buckets[key] = accumulator
        accumulator.observe(event)
    previous_quotes = dict(initial_quotes or {})
    states: list[MarketState15s] = []
    for _, accumulator in sorted(
        buckets.items(),
        key=lambda item: (item[0][1], item[0][0]),
    ):
        snapshot = accumulator.snapshot(
            initial_quote=previous_quotes.get(accumulator.symbol)
        )
        state = snapshot.state
        if state.last_bid_price is not None and state.last_ask_price is not None:
            previous_quotes[state.symbol] = (
                state.last_bid_price,
                state.last_ask_price,
            )
        states.append(state)
    return tuple(states)


@dataclass(frozen=True, slots=True)
class MarketState15sSnapshot:
    state: MarketState15s
    input_sequence_min: int
    input_sequence_max: int


@dataclass(slots=True)
class MarketState15sAccumulator:
    schema_version: int
    exchange: str
    environment: str
    symbol: str
    bucket_start: datetime
    bucket_end: datetime
    open_price: Decimal | None
    high_price: Decimal | None
    low_price: Decimal | None
    close_price: Decimal | None
    trade_count: int
    trade_notional: Decimal
    aggressive_buy_notional: Decimal
    aggressive_sell_notional: Decimal
    last_bid_price: Decimal | None
    last_ask_price: Decimal | None
    liquidation_count: int
    liquidation_notional: Decimal
    mark_price: Decimal | None
    closed_kline_count: int
    closed_kline_1m_open_time: datetime | None
    closed_kline_1m_close_time: datetime | None
    closed_kline_1m_open_price: Decimal | None
    closed_kline_1m_close_price: Decimal | None
    source_event_count: int
    first_received_at: datetime | None
    last_received_at: datetime | None
    input_sequence_min: int | None
    input_sequence_max: int | None

    @classmethod
    def for_bucket(
        cls,
        event: NormalizedMarketEvent,
    ) -> "MarketState15sAccumulator":
        start = bucket_start_15s(event.event_at)
        return cls(
            schema_version=2,
            exchange=event.exchange,
            environment=event.environment,
            symbol=event.symbol,
            bucket_start=start,
            bucket_end=start + timedelta(seconds=_BUCKET_SECONDS),
            open_price=None,
            high_price=None,
            low_price=None,
            close_price=None,
            trade_count=0,
            trade_notional=Decimal("0"),
            aggressive_buy_notional=Decimal("0"),
            aggressive_sell_notional=Decimal("0"),
            last_bid_price=None,
            last_ask_price=None,
            liquidation_count=0,
            liquidation_notional=Decimal("0"),
            mark_price=None,
            closed_kline_count=0,
            closed_kline_1m_open_time=None,
            closed_kline_1m_close_time=None,
            closed_kline_1m_open_price=None,
            closed_kline_1m_close_price=None,
            source_event_count=0,
            first_received_at=None,
            last_received_at=None,
            input_sequence_min=None,
            input_sequence_max=None,
        )

    def observe(self, event: NormalizedMarketEvent) -> None:
        self._validate_event(event)
        self.source_event_count += 1
        if self.first_received_at is None:
            self.first_received_at = event.received_at
        self.last_received_at = event.received_at
        self.input_sequence_min = (
            event.source_local_sequence
            if self.input_sequence_min is None
            else min(self.input_sequence_min, event.source_local_sequence)
        )
        self.input_sequence_max = (
            event.source_local_sequence
            if self.input_sequence_max is None
            else max(self.input_sequence_max, event.source_local_sequence)
        )

        if isinstance(event, NormalizedAggTrade):
            self._update_trade(event)
            return
        if isinstance(event, NormalizedBookTicker):
            self.last_bid_price = event.bid_price
            self.last_ask_price = event.ask_price
            return
        if isinstance(event, NormalizedLiquidation):
            self.liquidation_count += 1
            self.liquidation_notional += event.notional
            return
        if isinstance(event, NormalizedMarkPrice):
            self.mark_price = event.mark_price
            return
        if isinstance(event, NormalizedKline1m):
            if event.closed:
                self.closed_kline_count += 1
                if (
                    self.closed_kline_1m_open_time is None
                    or event.open_time >= self.closed_kline_1m_open_time
                ):
                    self.closed_kline_1m_open_time = event.open_time
                    self.closed_kline_1m_close_time = event.close_time
                    self.closed_kline_1m_open_price = event.open_price
                    self.closed_kline_1m_close_price = event.close_price
            return
        raise TypeError(f"unsupported normalized event: {type(event)!r}")

    def snapshot(
        self,
        *,
        initial_quote: tuple[Decimal, Decimal] | None = None,
        latest_quote: NormalizedBookTicker | None = None,
        data_complete: bool = True,
        missing_agg_trade_count: int = 0,
    ) -> MarketState15sSnapshot:
        if missing_agg_trade_count < 0:
            raise ValueError("missing_agg_trade_count must be non-negative")
        if latest_quote is not None:
            self._validate_event(latest_quote)
        last_bid_price = self.last_bid_price
        last_ask_price = self.last_ask_price
        source_event_count = self.source_event_count
        first_received_at = self.first_received_at
        last_received_at = self.last_received_at
        sequence_min = self.input_sequence_min
        sequence_max = self.input_sequence_max
        if latest_quote is not None:
            last_bid_price = latest_quote.bid_price
            last_ask_price = latest_quote.ask_price
            source_event_count += 1
            if first_received_at is None:
                first_received_at = latest_quote.received_at
            last_received_at = latest_quote.received_at
            sequence_min = (
                latest_quote.source_local_sequence
                if sequence_min is None
                else min(sequence_min, latest_quote.source_local_sequence)
            )
            sequence_max = (
                latest_quote.source_local_sequence
                if sequence_max is None
                else max(sequence_max, latest_quote.source_local_sequence)
            )
        if (
            initial_quote is not None
            and (last_bid_price is None or last_ask_price is None)
        ):
            last_bid_price, last_ask_price = initial_quote
        spread: Decimal | None = None
        midpoint: Decimal | None = None
        if last_bid_price is not None and last_ask_price is not None:
            spread = last_ask_price - last_bid_price
            midpoint = (last_bid_price + last_ask_price) / Decimal("2")
        if sequence_min is None or sequence_max is None:
            raise RuntimeError("cannot snapshot an empty market-state bucket")
        return MarketState15sSnapshot(
            state=MarketState15s(
                schema_version=self.schema_version,
                exchange=self.exchange,
                environment=self.environment,
                symbol=self.symbol,
                bucket_start=self.bucket_start,
                bucket_end=self.bucket_end,
                open_price=self.open_price,
                high_price=self.high_price,
                low_price=self.low_price,
                close_price=self.close_price,
                trade_count=self.trade_count,
                trade_notional=self.trade_notional,
                aggressive_buy_notional=self.aggressive_buy_notional,
                aggressive_sell_notional=self.aggressive_sell_notional,
                last_bid_price=last_bid_price,
                last_ask_price=last_ask_price,
                spread=spread,
                midpoint=midpoint,
                liquidation_count=self.liquidation_count,
                liquidation_notional=self.liquidation_notional,
                mark_price=self.mark_price,
                closed_kline_count=self.closed_kline_count,
                closed_kline_1m_open_time=self.closed_kline_1m_open_time,
                closed_kline_1m_close_time=self.closed_kline_1m_close_time,
                closed_kline_1m_open_price=self.closed_kline_1m_open_price,
                closed_kline_1m_close_price=self.closed_kline_1m_close_price,
                source_event_count=source_event_count,
                first_received_at=first_received_at,
                last_received_at=last_received_at,
                data_complete=data_complete,
                missing_agg_trade_count=missing_agg_trade_count,
            ),
            input_sequence_min=sequence_min,
            input_sequence_max=sequence_max,
        )

    def _validate_event(self, event: NormalizedMarketEvent) -> None:
        if (
            event.exchange != self.exchange
            or event.environment != self.environment
            or event.symbol != self.symbol
            or bucket_start_15s(event.event_at) != self.bucket_start
        ):
            raise ValueError("event does not belong to this market-state bucket")

    def _update_trade(self, event: NormalizedAggTrade) -> None:
        self.trade_count += 1
        self.trade_notional += event.notional
        if event.aggressor_side is AggressorSide.BUY:
            self.aggressive_buy_notional += event.notional
        else:
            self.aggressive_sell_notional += event.notional

        if self.open_price is None:
            self.open_price = event.price
            self.high_price = event.price
            self.low_price = event.price
        else:
            assert self.high_price is not None
            assert self.low_price is not None
            self.high_price = max(self.high_price, event.price)
            self.low_price = min(self.low_price, event.price)
        self.close_price = event.price
