from datetime import UTC, datetime, timedelta

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import StrategyCheckpoint, StrategyDecision
from crypto_momentum_lab.strategy_runner.daemon import (
    PaperLiveDaemonConfig,
    PaperLiveDaemonRepository,
    PaperLiveDaemonResult,
    RuntimeStrategy,
    run_paper_live_daemon,
)
from tests.unit.persistence.postgres.test_runtime_state_repository import (
    fixture_state,
)


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now


class FakeRepository(PaperLiveDaemonRepository):
    def __init__(self, checkpoint: StrategyCheckpoint | None = None) -> None:
        self.loaded_checkpoint = checkpoint
        self.saved_checkpoints: list[tuple[str, StrategyCheckpoint, datetime]] = []
        self.events: list[object] = []

    async def save_runtime_event(self, event: object) -> None:
        self.events.append(event)

    async def save_checkpoint(
        self,
        run_id: str,
        checkpoint: StrategyCheckpoint,
        saved_at: datetime,
    ) -> None:
        self.saved_checkpoints.append((run_id, checkpoint, saved_at))

    async def load_checkpoint(self, run_id: str) -> StrategyCheckpoint | None:
        return self.loaded_checkpoint


class FakeStrategy(RuntimeStrategy):
    def __init__(self) -> None:
        self.restored_checkpoint: StrategyCheckpoint | None = None
        self.processed: list[MarketState15s] = []

    def restore_checkpoint(self, checkpoint: StrategyCheckpoint) -> None:
        self.restored_checkpoint = checkpoint

    def on_market_state(self, state: MarketState15s) -> StrategyDecision:
        self.processed.append(state)
        checkpoint = StrategyCheckpoint(
            last_processed_at_by_symbol={state.symbol: state.bucket_start},
            warmup_buckets_by_symbol={state.symbol: len(self.processed)},
            cooldown_buckets_remaining_by_symbol={state.symbol: 0},
            payload={"last_symbol": state.symbol},
        )
        return StrategyDecision(
            signals=(),
            candidates=(),
            rejections=(),
            checkpoint=checkpoint,
        )


def test_daemon_saves_checkpoint_after_state_count_threshold() -> None:
    states = (fixture_state("BTCUSDT", 0), fixture_state("BTCUSDT", 1))
    repository = FakeRepository()
    strategy = FakeStrategy()

    result = run_paper_live_daemon(
        source=states,
        strategy=strategy,
        repository=repository,
        config=_config(checkpoint_every_states=2),
        clock=FakeClock(states[-1].bucket_end + timedelta(seconds=1)),
    )

    assert result == PaperLiveDaemonResult(
        processed_state_count=2,
        halt_reason=None,
        final_cursor=datetime(2026, 7, 3, 0, 0, 15, tzinfo=UTC),
        final_checkpoint_saved_at=datetime(2026, 7, 3, 0, 0, 31, tzinfo=UTC),
    )
    assert len(repository.saved_checkpoints) == 1
    assert repository.saved_checkpoints[0][0] == "run-1"


def test_daemon_resumes_from_checkpoint_cursor() -> None:
    first = fixture_state("BTCUSDT", 0)
    second = fixture_state("BTCUSDT", 1)
    checkpoint = StrategyCheckpoint(
        last_processed_at_by_symbol={"BTCUSDT": first.bucket_start},
        warmup_buckets_by_symbol={"BTCUSDT": 1},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 0},
        payload={},
    )
    repository = FakeRepository(checkpoint)
    strategy = FakeStrategy()

    result = run_paper_live_daemon(
        source=(first, second),
        strategy=strategy,
        repository=repository,
        config=_config(checkpoint_every_states=1),
        clock=FakeClock(second.bucket_end + timedelta(seconds=1)),
    )

    assert result.processed_state_count == 1
    assert strategy.restored_checkpoint == checkpoint
    assert strategy.processed == [second]


def test_daemon_halts_on_stale_market_state() -> None:
    stale_state = fixture_state("BTCUSDT", 0)
    repository = FakeRepository()

    result = run_paper_live_daemon(
        source=(stale_state,),
        strategy=FakeStrategy(),
        repository=repository,
        config=_config(max_market_state_age_seconds=10),
        clock=FakeClock(stale_state.bucket_end + timedelta(seconds=11)),
    )

    assert result.processed_state_count == 0
    assert result.halt_reason == "stale_market_state"
    assert repository.saved_checkpoints == []


def _config(
    *,
    checkpoint_every_states: int = 10,
    max_market_state_age_seconds: float = 120.0,
) -> PaperLiveDaemonConfig:
    return PaperLiveDaemonConfig(
        run_id="run-1",
        strategy_name="compression_breakout",
        environment="research",
        checkpoint_every_states=checkpoint_every_states,
        checkpoint_every_seconds=999,
        max_market_state_age_seconds=max_market_state_age_seconds,
    )
