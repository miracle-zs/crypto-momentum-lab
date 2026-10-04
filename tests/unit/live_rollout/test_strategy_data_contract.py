import pytest

from tests.fixtures.runtime_strategy import FakeStrategy


@pytest.mark.parametrize("gap, expected", [(30, []), (31, ["BTCUSDT"])])
def test_gap_reset_uses_explicit_symbol_capability(gap, expected) -> None:
    from datetime import UTC, datetime, timedelta

    from crypto_momentum_lab.live_rollout.market_loop import _reset_strategy_for_gap

    strategy = FakeStrategy()
    at = datetime(2026, 9, 30, tzinfo=UTC)
    _reset_strategy_for_gap(
        strategy=strategy,
        symbol="BTCUSDT",
        current_at=at + timedelta(seconds=gap),
        last_processed_at=at,
        max_gap_seconds=30,
    )
    assert strategy.reset_symbols == expected


def test_missing_reset_capability_cannot_skip_gap_reset() -> None:
    from datetime import UTC, datetime, timedelta

    from crypto_momentum_lab.live_rollout.market_loop import _reset_strategy_for_gap

    at = datetime(2026, 9, 30, tzinfo=UTC)
    with pytest.raises(AttributeError):
        _reset_strategy_for_gap(
            strategy=object(),
            symbol="BTCUSDT",
            current_at=at + timedelta(seconds=31),
            last_processed_at=at,
            max_gap_seconds=30,
        )
