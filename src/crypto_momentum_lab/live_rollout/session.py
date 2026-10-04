from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.live_rollout import (
    LiveSessionState,
    LiveSessionTransition,
)


class LiveTransitionRepository(Protocol):
    async def save_transition(self, transition: LiveSessionTransition) -> None:
        pass


@dataclass(frozen=True, slots=True)
class LiveSessionConfig:
    session_id: str
    operator: str
    strategy_config_hash: str
    risk_config_hash: str


class LiveSessionLifecycle:
    """Persist the shared live-session transition contract.

    Long-running daemons use this transition shape.
    Keeping transition construction here prevents the composition root from
    growing a second, subtly different session-state implementation.
    """

    def __init__(
        self,
        *,
        repository: LiveTransitionRepository,
        config: LiveSessionConfig,
        clock: Callable[[], datetime],
    ) -> None:
        self._repository = repository
        self._config = config
        self._clock = clock
        self._state: LiveSessionState | None = None

    @property
    def state(self) -> LiveSessionState | None:
        return self._state

    async def transition(
        self,
        state: LiveSessionState,
        reason: str | None = None,
    ) -> LiveSessionTransition:
        occurred_at = self._clock()
        transition = LiveSessionTransition(
            transition_id=str(
                uuid5(
                    NAMESPACE_URL,
                    f"live-transition:{self._config.session_id}:"
                    f"{state.value}:{occurred_at.isoformat()}",
                )
            ),
            session_id=self._config.session_id,
            state=state,
            occurred_at=occurred_at,
            operator=self._config.operator,
            strategy_config_hash=self._config.strategy_config_hash,
            risk_config_hash=self._config.risk_config_hash,
            reason=reason,
            details={},
        )
        await self._repository.save_transition(transition)
        self._state = state
        return transition


__all__ = [
    "LiveSessionConfig",
    "LiveSessionLifecycle",
    "LiveTransitionRepository",
]
