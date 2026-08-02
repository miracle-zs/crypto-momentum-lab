import asyncio
from datetime import UTC, datetime

from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    RuntimeStateCursor,
)
from crypto_momentum_lab.strategy_runner.live_source import (
    AsyncPostgresRuntimeStateLoader,
    PaperLiveSourceConfig,
    PostgresPaperMarketStateSource,
)
from tests.unit.persistence.postgres.test_runtime_state_repository import (
    fixture_state,
)


class FakeLoader:
    def __init__(self, batches) -> None:
        self.batches = list(batches)
        self.cursors: list[RuntimeStateCursor] = []

    def load_after(
        self,
        *,
        cursor: RuntimeStateCursor,
        limit: int,
    ):
        self.cursors.append(cursor)
        if not self.batches:
            return ()
        return self.batches.pop(0)

    def close(self) -> None:
        pass

class LoopRecordingRepository:
    def __init__(self) -> None:
        self.loops: list[asyncio.AbstractEventLoop] = []

    async def load_after(
        self,
        *,
        environment: str,
        cursor: RuntimeStateCursor,
        limit: int,
    ):
        del environment, cursor, limit
        self.loops.append(asyncio.get_running_loop())
        return ()


class FakeUniverseRepository:
    async def load_active_memberships(self):
        return {"BTCUSDT": object(), "ETHUSDT": object()}

    async def load_active_memberships_at(self, observed_at):
        assert observed_at == datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
        return {"BTCUSDT": object()}


def test_async_loader_reuses_one_event_loop_for_pooled_database_connections() -> None:
    repository = LoopRecordingRepository()
    loader = AsyncPostgresRuntimeStateLoader(
        repository=repository,
        environment="research",
    )

    loader.load_after(cursor=RuntimeStateCursor(), limit=10)
    loader.load_after(cursor=RuntimeStateCursor(), limit=10)

    assert repository.loops[0] is repository.loops[1]
    loader.close()


def test_async_loader_reads_active_entry_symbols_on_its_event_loop() -> None:
    loader = AsyncPostgresRuntimeStateLoader(
        repository=LoopRecordingRepository(),
        environment="research",
        universe_repository=FakeUniverseRepository(),
    )

    assert loader.load_active_symbols() == frozenset(
        {"BTCUSDT", "ETHUSDT"}
    )
    loader.close()


def test_async_loader_reads_entry_symbols_at_historical_state_time() -> None:
    observed_at = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    loader = AsyncPostgresRuntimeStateLoader(
        repository=LoopRecordingRepository(),
        environment="research",
        universe_repository=FakeUniverseRepository(),
    )

    assert loader.load_active_symbols_at(observed_at) == frozenset({"BTCUSDT"})
    loader.close()


def test_postgres_paper_source_yields_in_order_and_advances_cursor() -> None:
    first = fixture_state("BTCUSDT", 0)
    second = fixture_state("ETHUSDT", 0)
    loader = FakeLoader(
        [
            tuple(
                sorted(
                    (second, first),
                    key=lambda item: (item.bucket_start, item.symbol),
                )
            ),
            (),
        ]
    )
    source = PostgresPaperMarketStateSource(
        loader=loader,
        config=PaperLiveSourceConfig(
            environment="research",
            start_at=None,
            poll_interval_seconds=0,
            idle_timeout_seconds=0,
            max_states=3,
            batch_size=10,
        ),
    )

    states = tuple(source)

    assert tuple(state.symbol for state in states) == ("BTCUSDT", "ETHUSDT")
    assert loader.cursors[0] == RuntimeStateCursor()
    assert loader.cursors[-1] == RuntimeStateCursor(
        bucket_start=second.bucket_start,
        symbol="ETHUSDT",
    )


def test_postgres_paper_source_stops_after_idle_timeout() -> None:
    loader = FakeLoader([()])
    source = PostgresPaperMarketStateSource(
        loader=loader,
        config=PaperLiveSourceConfig(
            environment="research",
            start_at=datetime(2026, 7, 3, 0, 0, tzinfo=UTC),
            poll_interval_seconds=0,
            idle_timeout_seconds=0,
            max_states=10,
            batch_size=10,
        ),
    )

    assert tuple(source) == ()
    assert loader.cursors == [
        RuntimeStateCursor(
            bucket_start=datetime(2026, 7, 3, 0, 0, tzinfo=UTC),
            symbol="",
        )
    ]


def test_paper_live_source_has_no_historical_resume_interface() -> None:
    assert "resume_run_ids" not in PaperLiveSourceConfig.__dataclass_fields__
