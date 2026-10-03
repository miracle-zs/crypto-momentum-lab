from dataclasses import replace

import pytest

from crypto_momentum_lab.live_rollout.market_loop import (
    _strategy_max_gap_seconds,
    _strategy_state_interval_seconds,
)
from tests.fixtures.runtime_strategy import FakeStrategy


class SlowerStrategy(FakeStrategy):
    def required_data(self):
        return replace(
            super().required_data(), base_state_interval_seconds=60, max_gap_seconds=120
        )


def test_reads_explicit_strategy_intervals() -> None:
    strategy = SlowerStrategy()
    assert _strategy_max_gap_seconds(strategy) == 120
    assert _strategy_state_interval_seconds(strategy) == 60


@pytest.mark.parametrize(
    "reader", [_strategy_max_gap_seconds, _strategy_state_interval_seconds]
)
def test_missing_strategy_requirement_does_not_silently_default(reader) -> None:
    with pytest.raises(AttributeError):
        reader(object())


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
