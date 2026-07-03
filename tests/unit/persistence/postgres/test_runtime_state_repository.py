from dataclasses import fields
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    runtime_state_row,
    validate_closed_states,
)


def test_runtime_state_row_preserves_market_state_values() -> None:
    state = fixture_state("BTCUSDT", 0)

    row = runtime_state_row(
        state,
        source_watermark_at=datetime(2026, 7, 3, 0, 0, 45, tzinfo=UTC),
        input_sequence_min=1,
        input_sequence_max=3,
    )

    assert row["environment"] == "research"
    assert row["symbol"] == "BTCUSDT"
    assert row["bucket_start"] == datetime(2026, 7, 3, 0, 0, tzinfo=UTC)
    assert row["close_price"] == Decimal("100")
    assert row["source_watermark_at"] == datetime(
        2026,
        7,
        3,
        0,
        0,
        45,
        tzinfo=UTC,
    )
    assert row["closure_reason"] == "watermark_elapsed"
    assert row["input_sequence_min"] == 1
    assert row["input_sequence_max"] == 3


def test_validate_closed_states_rejects_duplicate_primary_key() -> None:
    state = fixture_state("BTCUSDT", 0)

    with pytest.raises(ValueError, match="duplicate runtime market state"):
        validate_closed_states((state, state))


def test_validate_closed_states_rejects_naive_timestamp() -> None:
    state = fixture_state("BTCUSDT", 0)
    naive = object.__new__(MarketState15s)
    for field in fields(state):
        object.__setattr__(naive, field.name, getattr(state, field.name))
    object.__setattr__(naive, "bucket_start", datetime(2026, 7, 3, 0, 0))

    with pytest.raises(ValueError, match="bucket_start must be timezone-aware"):
        validate_closed_states((naive,))


def fixture_state(symbol: str, bucket_index: int) -> MarketState15s:
    start = datetime(2026, 7, 3, 0, 0, tzinfo=UTC) + timedelta(
        seconds=15 * bucket_index
    )
    end = start + timedelta(seconds=15)
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol=symbol,
        bucket_start=start,
        bucket_end=end,
        open_price=Decimal("100"),
        high_price=Decimal("101"),
        low_price=Decimal("99"),
        close_price=Decimal("100"),
        trade_count=2,
        trade_notional=Decimal("200"),
        aggressive_buy_notional=Decimal("120"),
        aggressive_sell_notional=Decimal("80"),
        last_bid_price=Decimal("99.99"),
        last_ask_price=Decimal("100.01"),
        spread=Decimal("0.02"),
        midpoint=Decimal("100"),
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=Decimal("100"),
        closed_kline_count=0,
        source_event_count=3,
        first_received_at=start,
        last_received_at=end,
    )
