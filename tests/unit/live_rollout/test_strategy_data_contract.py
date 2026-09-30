from dataclasses import replace

import pytest

from crypto_momentum_lab.live_rollout.market_loop import (
    _strategy_max_gap_seconds,
    _strategy_state_interval_seconds,
)
from tests.unit.strategy_runner.test_daemon import FakeStrategy


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
