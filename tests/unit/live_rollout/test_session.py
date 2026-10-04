from datetime import UTC, datetime

from crypto_momentum_lab.domain.live_rollout import (
    LiveSessionState,
    LiveSessionTransition,
)
from crypto_momentum_lab.live_rollout.session import (
    LiveSessionConfig,
    LiveSessionLifecycle,
)

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


async def test_session_lifecycle_persists_shared_transition_contract() -> None:
    repository = FakeTransitionRepository()
    lifecycle = LiveSessionLifecycle(
        repository=repository,
        config=LiveSessionConfig(
            session_id="live-1",
            operator="operator",
            strategy_config_hash="a" * 64,
            risk_config_hash="b" * 64,
        ),
        clock=lambda: NOW,
    )

    transition = await lifecycle.transition(
        LiveSessionState.PREFLIGHT,
        reason="startup",
    )

    assert transition.state is LiveSessionState.PREFLIGHT
    assert transition.reason == "startup"
    assert transition.session_id == "live-1"
    assert lifecycle.state is LiveSessionState.PREFLIGHT
    assert repository.items == [transition]


class FakeTransitionRepository:
    def __init__(self) -> None:
        self.items: list[LiveSessionTransition] = []

    async def save_transition(self, transition: LiveSessionTransition) -> None:
        self.items.append(transition)
