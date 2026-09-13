from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.live_rollout.market_loop import (
    LiveMarketLoop,
    LiveMarketStateContinuityError,
)
from crypto_momentum_lab.live_rollout.runtime_orchestrator import (
    _LiveHubCursorState,
)
from crypto_momentum_lab.live_rollout.startup_recovery import (
    load_live_market_state_gap,
    wait_for_durable_market_state_cutover,
)
from crypto_momentum_lab.market_data.hub import MarketStateBatch


class _DurableBoundaryRepository:
    def __init__(self, latest_buckets: tuple[datetime | None, ...]) -> None:
        self._latest_buckets = iter(latest_buckets)
        self.calls = 0

    async def load_latest_bucket(self, *, environment: str) -> datetime | None:
        assert environment == "research"
        self.calls += 1
        return next(self._latest_buckets)


class _GapRepository:
    def __init__(self, states: tuple[object, ...]) -> None:
        self.states = states
        self.calls = 0

    async def load_after(self, **kwargs: object) -> tuple[object, ...]:
        assert kwargs["environment"] == "research"
        assert kwargs["symbols"] == ("ALCHUSDT",)
        self.calls += 1
        return self.states


class _RecoveringStrategy:
    def __init__(self) -> None:
        self.warmed: list[object] = []

    def required_data(self) -> object:
        return SimpleNamespace(required_fields=("close_price",))

    def warm_market_state(self, state: object) -> None:
        self.warmed.append(state)


class _RecoveryCoordinator:
    def __init__(self) -> None:
        self.recovered: list[object] = []

    def record_recovered_state(self, state: object, *, saved_at: datetime) -> None:
        assert saved_at.tzinfo is not None
        self.recovered.append(state)


@pytest.mark.asyncio
async def test_startup_waits_until_requested_cutover_is_durable() -> None:
    requested = datetime(2026, 9, 13, 5, 53, 15, tzinfo=UTC)
    repository = _DurableBoundaryRepository(
        (
            requested - timedelta(seconds=15),
            requested,
        )
    )

    cutover = await wait_for_durable_market_state_cutover(
        repository=repository,  # type: ignore[arg-type]
        environment="research",
        requested_cutover=requested,
        timeout_seconds=0.1,
        poll_interval_seconds=0.001,
    )

    assert cutover == requested
    assert repository.calls == 2


@pytest.mark.asyncio
async def test_gap_loader_reads_only_canonical_intermediate_buckets() -> None:
    previous = datetime(2026, 9, 13, 5, 53, 0, tzinfo=UTC)
    current = previous + timedelta(seconds=30)
    missing = SimpleNamespace(
        symbol="ALCHUSDT",
        bucket_start=previous + timedelta(seconds=15),
    )
    repository = _GapRepository((missing,))

    states = await load_live_market_state_gap(
        repository=repository,  # type: ignore[arg-type]
        environment="research",
        symbol="ALCHUSDT",
        previous_at=previous,
        current_at=current,
        interval_seconds=15,
    )

    assert states == (missing,)
    assert repository.calls == 1


@pytest.mark.asyncio
async def test_market_loop_warms_a_complete_gap_without_resetting_the_symbol() -> None:
    previous = datetime(2026, 9, 13, 5, 53, 0, tzinfo=UTC)
    current = previous + timedelta(seconds=30)
    missing = SimpleNamespace(
        symbol="ALCHUSDT",
        bucket_start=previous + timedelta(seconds=15),
        bucket_end=previous + timedelta(seconds=30),
        close_price=1,
        data_complete=True,
    )
    strategy = _RecoveringStrategy()
    coordinator = _RecoveryCoordinator()
    loop = object.__new__(LiveMarketLoop)
    loop._strategy = strategy
    loop._checkpoint_coordinator = coordinator
    loop._run_id = "run-1"
    loop._clock = lambda: current
    loop._recover_market_state_gap = lambda _error: _missing_state(missing)

    error = LiveMarketStateContinuityError(
        symbol="ALCHUSDT",
        previous_at=previous,
        current_at=current,
        expected_interval_seconds=15,
    )

    recovered = await loop._recover_gap(error)

    assert recovered == (missing,)
    assert strategy.warmed == [missing]
    assert coordinator.recovered == [missing]


async def _missing_state(state: object) -> tuple[object, ...]:
    return (state,)


def test_hub_cursor_is_committed_only_after_the_entire_batch_is_processed() -> None:
    start = datetime(2026, 9, 13, 5, 53, 15, tzinfo=UTC)
    first = SimpleNamespace(symbol="ALCHUSDT", bucket_start=start)
    second = SimpleNamespace(
        symbol="XPINUSDT",
        bucket_start=start,
    )
    cursor = _LiveHubCursorState()
    cursor.observe_batch(
        MarketStateBatch(
            sequence=17,
            published_at=start,
            environment="live",
            states=(first, second),  # type: ignore[arg-type]
            stream_id="stream-a",
        )
    )

    cursor.acknowledge_state(first)  # type: ignore[arg-type]
    assert cursor.snapshot() is None
    cursor.acknowledge_state(second)  # type: ignore[arg-type]

    assert cursor.snapshot() == {
        "stream_id": "stream-a",
        "sequence": 17,
    }
