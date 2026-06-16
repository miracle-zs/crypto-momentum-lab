from datetime import UTC, datetime
from uuid import UUID

import pytest

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    MarketDataState,
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
