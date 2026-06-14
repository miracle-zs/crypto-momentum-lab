from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.universe.models import MarketCandidate
from crypto_momentum_lab.domain.universe.ranking import rank_utc_day_returns


def candidate(
    symbol: str,
    open_price: str | None,
    current_price: str | None,
) -> MarketCandidate:
    now = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    return MarketCandidate(
        symbol=symbol,
        open_price=None if open_price is None else Decimal(open_price),
        current_price=None if current_price is None else Decimal(current_price),
        price_time=now,
    )


def test_ranks_gainers_and_losers_with_deterministic_ties() -> None:
    result = rank_utc_day_returns(
        [
            candidate("CCCUSDT", "100", "110"),
            candidate("AAAUSDT", "100", "110"),
            candidate("BBBUSDT", "100", "90"),
            candidate("DDDUSDT", "100", "95"),
        ],
        top_count=2,
        ranking_depth=2,
    )

    assert [entry.symbol for entry in result.gainers] == ["AAAUSDT", "CCCUSDT"]
    assert [entry.symbol for entry in result.losers] == ["BBBUSDT", "DDDUSDT"]
    assert result.target_symbols == frozenset(
        {"AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT"}
    )


def test_excludes_missing_or_non_positive_prices_with_reason() -> None:
    result = rank_utc_day_returns(
        [
            candidate("GOODUSDT", "10", "11"),
            candidate("NOOPENUSDT", None, "11"),
            candidate("NOPRICEUSDT", "10", None),
            candidate("ZEROOPENUSDT", "0", "1"),
        ],
        top_count=20,
        ranking_depth=30,
    )

    assert result.target_symbols == frozenset({"GOODUSDT"})
    assert result.exclusions == {
        "NOOPENUSDT": "missing_open_price",
        "NOPRICEUSDT": "missing_current_price",
        "ZEROOPENUSDT": "non_positive_open_price",
    }


def test_small_population_deduplicates_target_union() -> None:
    result = rank_utc_day_returns(
        [
            candidate("AAAUSDT", "100", "101"),
            candidate("BBBUSDT", "100", "99"),
        ],
        top_count=20,
        ranking_depth=30,
    )

    assert result.target_symbols == frozenset({"AAAUSDT", "BBBUSDT"})


def test_retention_ranking_keeps_more_entries_than_target() -> None:
    result = rank_utc_day_returns(
        [
            candidate(f"S{index}USDT", "100", str(100 + index))
            for index in range(4)
        ],
        top_count=1,
        ranking_depth=3,
    )

    assert len(result.gainers) == 3
    assert len(result.losers) == 3
    assert result.target_symbols == frozenset({"S0USDT", "S3USDT"})


def test_rejects_ranking_depth_smaller_than_target_count() -> None:
    with pytest.raises(ValueError, match="ranking_depth"):
        rank_utc_day_returns(
            [candidate("AAAUSDT", "100", "101")],
            top_count=2,
            ranking_depth=1,
        )
