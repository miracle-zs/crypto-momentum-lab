"""Interpret durable operating state without owning database access."""

from typing import Protocol

from crypto_momentum_lab.domain.live_rollout import LiveSessionState


class LiveSessionStateReader(Protocol):
    async def load_latest_operating_state(self, session_id: str) -> str | None:
        """Latest transition excluding preflight."""
        ...


async def session_is_draining(reader: LiveSessionStateReader, session_id: str) -> bool:
    draining_state: str = LiveSessionState.DRAINING.value
    return (
        await reader.load_latest_operating_state(session_id)
        == draining_state
    )
