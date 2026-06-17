from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from crypto_momentum_lab.domain.market.models import (
    AggressorSide,
    CaptureRoute,
    CaptureStream,
    MarketDataState,
    MarketState15s,
    NormalizedAggTrade,
    RawEnvelope,
    transition_market_data_state,
)


def test_raw_envelope_requires_aware_receive_time() -> None:
    with pytest.raises(ValueError, match="received_at"):
        RawEnvelope(
            schema_version=1,
            exchange="binance-usdm",
            environment="research",
            route=CaptureRoute.MARKET,
            stream=CaptureStream.AGG_TRADE,
            symbol="BTCUSDT",
            exchange_event_at=None,
            received_at=datetime(2026, 6, 15, 2, 0),
            received_monotonic_ns=1,
            connection_session_id=UUID(int=1),
            local_sequence=1,
            exchange_sequence="42",
            subscription_generation=1,
            raw_payload={"e": "aggTrade"},
        )


def test_halted_state_requires_explicit_recovery() -> None:
    assert (
        transition_market_data_state(
            MarketDataState.READY,
            MarketDataState.HALTED,
        )
        is MarketDataState.HALTED
    )
    with pytest.raises(ValueError, match="HALTED"):
        transition_market_data_state(
            MarketDataState.HALTED,
            MarketDataState.READY,
        )
    assert (
        transition_market_data_state(
            MarketDataState.HALTED,
            MarketDataState.SYNCING,
            recovery=True,
        )
        is MarketDataState.SYNCING
    )


def test_raw_envelope_accepts_aware_event_times() -> None:
    envelope = RawEnvelope(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        route=CaptureRoute.MARKET,
        stream=CaptureStream.AGG_TRADE,
        symbol="BTCUSDT",
        exchange_event_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        received_at=datetime(2026, 6, 15, 2, 0, 1, tzinfo=UTC),
        received_monotonic_ns=1,
        connection_session_id=UUID(int=1),
        local_sequence=1,
        exchange_sequence="42",
        subscription_generation=1,
        raw_payload={"e": "aggTrade"},
    )

    assert envelope.symbol == "BTCUSDT"


def test_normalized_agg_trade_preserves_decimal_fields() -> None:
    trade = NormalizedAggTrade(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        event_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        received_at=datetime(2026, 6, 15, 2, 0, 1, tzinfo=UTC),
        source_connection_session_id=UUID(int=1),
        source_local_sequence=7,
        source_stream=CaptureStream.AGG_TRADE,
        trade_id="42",
        price=Decimal("100.25"),
        quantity=Decimal("0.5"),
        notional=Decimal("50.125"),
        aggressor_side=AggressorSide.BUY,
    )

    assert trade.price == Decimal("100.25")
    assert trade.notional == Decimal("50.125")
    assert trade.aggressor_side is AggressorSide.BUY


def test_market_state_15s_requires_aware_bucket_start() -> None:
    with pytest.raises(ValueError, match="bucket_start"):
        MarketState15s(
            schema_version=1,
            exchange="binance-usdm",
            environment="research",
            symbol="BTCUSDT",
            bucket_start=datetime(2026, 6, 15, 2, 0),
            bucket_end=datetime(2026, 6, 15, 2, 0, 15, tzinfo=UTC),
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
            spread=None,
            midpoint=None,
            liquidation_count=0,
            liquidation_notional=Decimal("0"),
            mark_price=None,
            closed_kline_count=0,
            source_event_count=0,
            first_received_at=None,
            last_received_at=None,
        )
