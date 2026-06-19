from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.persistence.parquet import (
    read_market_states_15s_dataset,
    write_market_states_15s_dataset,
)


def test_read_market_states_15s_dataset_reconstructs_typed_states(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "raw.jsonl.zst"
    input_path.write_bytes(b"raw-input")
    state = _state("BTCUSDT", 0, close_price=Decimal("100.25"))
    derived_root = tmp_path / "derived"

    write_market_states_15s_dataset(
        root=derived_root,
        states=(state,),
        input_paths=(input_path,),
    )

    states = read_market_states_15s_dataset(
        (derived_root / "market_states_15s",)
    )

    assert states == (state,)


def test_read_market_states_15s_dataset_sorts_by_symbol_and_time(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "raw.jsonl.zst"
    input_path.write_bytes(b"raw-input")
    later = _state("ETHUSDT", 1, close_price=Decimal("200.5"))
    earlier = _state("BTCUSDT", 0, close_price=Decimal("100.25"))
    derived_root = tmp_path / "derived"

    write_market_states_15s_dataset(
        root=derived_root,
        states=(later, earlier),
        input_paths=(input_path,),
    )

    states = read_market_states_15s_dataset(
        tuple((derived_root / "market_states_15s").rglob("*.parquet"))
    )

    assert states == (earlier, later)


def _state(
    symbol: str,
    bucket_index: int,
    *,
    close_price: Decimal,
) -> MarketState15s:
    bucket_start = datetime(2026, 6, 19, 0, 0, 15 * bucket_index, tzinfo=UTC)
    bucket_end = datetime(2026, 6, 19, 0, 0, 15 * (bucket_index + 1), tzinfo=UTC)
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol=symbol,
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        open_price=close_price,
        high_price=close_price,
        low_price=close_price,
        close_price=close_price,
        trade_count=3,
        trade_notional=Decimal("301"),
        aggressive_buy_notional=Decimal("201"),
        aggressive_sell_notional=Decimal("100"),
        last_bid_price=close_price - Decimal("0.01"),
        last_ask_price=close_price + Decimal("0.01"),
        spread=Decimal("0.02"),
        midpoint=close_price,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=close_price,
        closed_kline_count=0,
        source_event_count=4,
        first_received_at=bucket_start,
        last_received_at=bucket_end,
    )
