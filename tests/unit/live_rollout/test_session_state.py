import asyncio

import pytest

from crypto_momentum_lab.live_rollout.session_state import session_is_draining


class StateReader:
    def __init__(self, state: str | None) -> None:
        self.state = state
        self.sessions: list[str] = []

    async def load_latest_operating_state(self, session_id: str) -> str | None:
        self.sessions.append(session_id)
        return self.state


@pytest.mark.parametrize(
    "state, expected",
    [
        ("draining", True),
        ("live_enabled", False),
        ("halted", False),
        ("completed", False),
        (None, False),
        ("future_state", False),
    ],
)
async def test_draining_requires_exact_durable_state(
    state: str | None, expected: bool
) -> None:
    reader = StateReader(state)
    assert await session_is_draining(reader, "session-3") is expected
    assert reader.sessions == ["session-3"]


@pytest.mark.parametrize(
    "error", [ConnectionError("database unavailable"), asyncio.CancelledError()]
)
async def test_read_failure_and_cancellation_propagate(error: BaseException) -> None:
    class FailingReader:
        async def load_latest_operating_state(self, session_id: str) -> str | None:
            raise error

    with pytest.raises(type(error)):
        await session_is_draining(FailingReader(), "session-3")
