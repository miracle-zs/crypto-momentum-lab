import asyncio
from datetime import UTC, datetime

import pytest

import crypto_momentum_lab.strategy_runner.live_source as live_source
from crypto_momentum_lab.domain.market.runtime_state_models import RuntimeStateCursor
from crypto_momentum_lab.persistence.postgres.runtime_state_loader import (
    AsyncPostgresRuntimeStateLoader,
)
from crypto_momentum_lab.strategy_runner.live_source import (
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


class RecordingLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict[str, object]]] = []

    def error(self, event: str, **kwargs: object) -> None:
        self.events.append(("error", event, kwargs))


class WakeupLoader(FakeLoader):
    def __init__(self, batches) -> None:
        super().__init__(batches)
        self.prepared = 0
        self.waits: list[float] = []

    def prepare_wakeup(self) -> None:
        self.prepared += 1

    def wait_for_data(self, timeout_seconds: float) -> None:
        self.waits.append(timeout_seconds)


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
    async def load_active_entry_symbols_at(self, observed_at):
        if observed_at is None:
            return frozenset({"BTCUSDT", "ETHUSDT"})
        assert observed_at == datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
        return frozenset({"BTCUSDT"})

    async def load_positive_gainer_symbols_at(self, observed_at, *, top_count):
        assert observed_at == datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
        assert top_count == 1
        return frozenset({"BTCUSDT"})


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

    assert loader.load_active_symbols() == frozenset({"BTCUSDT", "ETHUSDT"})
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


def test_postgres_paper_source_stops_after_idle_timeout(monkeypatch) -> None:
    loader = FakeLoader([()])
    logger = RecordingLogger()
    monkeypatch.setattr(live_source, "log", logger)
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
    assert len(logger.events) == 1
    level, event, fields = logger.events[0]
    assert (level, event) == (
        "error",
        "paper_market_state_source_idle_timeout",
    )
    assert fields["environment"] == "research"
    assert fields["idle_timeout_seconds"] == 0
    assert fields["elapsed_idle_seconds"] >= 0
    assert fields["yielded_state_count"] == 0
    assert (
        fields["cursor_bucket_start"]
        == datetime(2026, 7, 3, 0, 0, tzinfo=UTC).isoformat()
    )
    assert fields["cursor_symbol"] == ""
    assert fields["action"] == "exit_for_container_restart"


def test_postgres_paper_source_backs_off_only_while_idle(monkeypatch) -> None:
    state = fixture_state("BTCUSDT", 0)
    loader = FakeLoader([(), (), (), (state,)])
    source = PostgresPaperMarketStateSource(
        loader=loader,
        config=PaperLiveSourceConfig(
            environment="research",
            start_at=None,
            poll_interval_seconds=1.0,
            idle_timeout_seconds=10.0,
            max_states=1,
            batch_size=1,
        ),
    )
    now = 0.0
    sleeps: list[float] = []

    def monotonic() -> float:
        return now

    def sleep(seconds: float) -> None:
        nonlocal now
        sleeps.append(seconds)
        now += seconds

    monkeypatch.setattr(live_source.time, "monotonic", monotonic)
    monkeypatch.setattr(live_source.time, "sleep", sleep)

    assert tuple(source) == (state,)
    assert sleeps == [1.0, 2.0, 3.0]


def test_postgres_paper_source_uses_durable_wakeup_when_available() -> None:
    state = fixture_state("BTCUSDT", 0)
    loader = WakeupLoader([(), (state,)])
    source = PostgresPaperMarketStateSource(
        loader=loader,
        wakeup=loader,
        config=PaperLiveSourceConfig(
            environment="research",
            start_at=None,
            poll_interval_seconds=1.0,
            idle_timeout_seconds=30.0,
            max_states=1,
            batch_size=1,
        ),
    )

    assert tuple(source) == (state,)
    assert loader.prepared == 1
    assert len(loader.waits) == 1
    assert 29.0 < loader.waits[0] <= 30.0


def test_paper_live_source_has_no_historical_resume_interface() -> None:
    assert "resume_run_ids" not in PaperLiveSourceConfig.__dataclass_fields__


@pytest.mark.parametrize("mode", ["enabled", "disabled", "failure"])
def test_separate_wakeup_and_polling_fallback(monkeypatch, mode):
    state = fixture_state("BTCUSDT", 0)
    loader = FakeLoader([(), (state,)])
    events = []

    class Wakeup:
        def prepare_wakeup(self):
            events.append("prepare")
            if mode == "failure":
                raise RuntimeError("listen unavailable")
            return mode == "enabled"

        def wait_for_data(self, timeout_seconds):
            assert 0 < timeout_seconds <= 30
            events.append("wait")

    monkeypatch.setattr(
        live_source.time, "sleep", lambda seconds: events.append("poll")
    )
    source = PostgresPaperMarketStateSource(
        loader=loader,
        wakeup=Wakeup(),
        config=PaperLiveSourceConfig(
            environment="research",
            start_at=None,
            poll_interval_seconds=1,
            idle_timeout_seconds=30,
            max_states=1,
            batch_size=1,
        ),
    )
    assert tuple(source) == (state,)
    assert events == ["prepare", "wait" if mode == "enabled" else "poll"]


def test_universe_ranked_gainers_are_read_at_the_requested_cut():
    loader = AsyncPostgresRuntimeStateLoader(
        repository=LoopRecordingRepository(),
        environment="research",
        universe_repository=FakeUniverseRepository(),
    )
    try:
        assert loader.load_positive_gainer_symbols_at(
            datetime(2026, 7, 4, tzinfo=UTC), top_count=1
        ) == frozenset({"BTCUSDT"})
    finally:
        loader.close()


def test_absent_universe_reader_preserves_empty_symbol_sets():
    loader = AsyncPostgresRuntimeStateLoader(
        repository=LoopRecordingRepository(), environment="research"
    )
    try:
        assert loader.load_active_symbols() == frozenset()
        assert (
            loader.load_active_symbols_at(datetime(2026, 7, 4, tzinfo=UTC))
            == frozenset()
        )
        assert (
            loader.load_positive_gainer_symbols_at(
                datetime(2026, 7, 4, tzinfo=UTC), top_count=1
            )
            == frozenset()
        )
    finally:
        loader.close()
