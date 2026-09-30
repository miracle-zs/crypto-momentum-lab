import pytest

from crypto_momentum_lab.domain.strategy import StrategyDataRequirement
from crypto_momentum_lab.live_rollout.startup_recovery import (
    _recovery_state_limit,
    _required_warmup_buckets,
    live_warmup_seconds,
)


class Strategy:
    def __init__(self, requirement: StrategyDataRequirement | None) -> None:
        self.requirement = requirement

    def required_data(self) -> StrategyDataRequirement | None:
        return self.requirement


@pytest.mark.parametrize(
    "requirement, seconds, buckets, limit",
    [
        (None, 240, 1, 420000),
        (StrategyDataRequirement(15, 1, ("close_price",), 30, False), 255, 1, 420000),
        (StrategyDataRequirement(60, 4, ("close_price",), 120, False), 1200, 4, 120000),
    ],
)
def test_startup_uses_explicit_data_requirement(requirement, seconds, buckets, limit):
    strategy = Strategy(requirement)
    assert live_warmup_seconds(strategy) == seconds
    assert _required_warmup_buckets(strategy) == buckets
    assert (
        _recovery_state_limit(
            strategy=strategy, symbol_count=10000, lookback_seconds=600
        )
        == limit
    )


def test_startup_does_not_default_missing_requirement_method() -> None:
    with pytest.raises(AttributeError):
        live_warmup_seconds(object())
