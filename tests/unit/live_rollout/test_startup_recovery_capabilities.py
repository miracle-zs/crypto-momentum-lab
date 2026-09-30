from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.domain.strategy.models import StrategyCheckpoint
from crypto_momentum_lab.live_rollout.startup_recovery import (
    restore_live_strategy_from_checkpoint,
    warm_live_strategy,
)


async def test_warmup_checks_capability_before_database_access() -> None:
    with pytest.raises(AttributeError, match="warm_market_state"):
        await warm_live_strategy(
            strategy=object(),
            repository=object(),
            environment="research",
            now=datetime(2026, 9, 30, tzinfo=UTC),
        )


async def test_restore_checks_warm_capability_before_database_access() -> None:
    checkpoint = StrategyCheckpoint({}, {}, {}, {})
    with pytest.raises(AttributeError, match="warm_market_state"):
        await restore_live_strategy_from_checkpoint(
            strategy=object(),
            checkpoint=checkpoint,
            repository=object(),
            environment="research",
        )


async def test_restore_requires_clear_before_replaying() -> None:
    class WarmOnlyStrategy:
        def warm_market_state(self, state):
            pytest.fail("must not warm before clearing derived buffers")

    checkpoint = StrategyCheckpoint({}, {}, {}, {})
    with pytest.raises(AttributeError, match="clear_market_state_buffers"):
        await restore_live_strategy_from_checkpoint(
            strategy=WarmOnlyStrategy(),
            checkpoint=checkpoint,
            repository=object(),
            environment="research",
        )
