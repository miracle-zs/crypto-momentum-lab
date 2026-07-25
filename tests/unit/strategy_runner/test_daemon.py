from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    RunMode,
    StrategyCheckpoint,
    StrategyDecision,
    StrategyRunIdentity,
    StrategySide,
    StrategySignal,
)
from crypto_momentum_lab.strategy_runner.daemon import (
    PaperLiveArtifactRepository,
    PaperLiveDaemonConfig,
    PaperLiveDaemonRepository,
    PaperLiveDaemonResult,
    RuntimeStrategy,
    run_paper_live_daemon,
)
from crypto_momentum_lab.strategy_runner.fills import (
    ReplayExecutionConfig,
    SimulatedFill,
    SimulatedFillStatus,
)
from crypto_momentum_lab.strategy_runner.portfolio import (
    PaperExitConfig,
    PaperPosition,
    position_from_entry_fill,
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


class SignalStrategy(FakeStrategy):
    def __init__(self, identity: StrategyRunIdentity) -> None:
        super().__init__()
        self._identity = identity

    def on_market_state(self, state: MarketState15s) -> StrategyDecision:
        decision = super().on_market_state(state)
        if len(self.processed) > 1:
            return decision
        signal = StrategySignal(
            signal_id="signal-1",
            run_id=self._identity.run_id,
            strategy_name=self._identity.strategy_name,
            strategy_version=self._identity.strategy_version,
            config_hash=self._identity.config_hash,
            symbol=state.symbol,
            side=StrategySide.LONG,
            detected_at=state.bucket_start,
            source_state_at=state.bucket_start,
            reason="compression_breakout",
            features={},
            reference_prices={"close": str(state.close_price)},
        )
        candidate = OrderIntentCandidate(
            candidate_id="candidate-1",
            signal_id=signal.signal_id,
            run_id=self._identity.run_id,
            strategy_name=self._identity.strategy_name,
            strategy_version=self._identity.strategy_version,
            config_hash=self._identity.config_hash,
            symbol=state.symbol,
            side=signal.side,
            entry_type=EntryType.MARKET,
            limit_price=None,
            desired_notional=Decimal("25"),
            reduce_only=False,
            expires_at=state.bucket_start + timedelta(seconds=60),
            created_at=state.bucket_start,
            reason=signal.reason,
            features={},
        )
        return StrategyDecision(
            signals=(signal,),
            candidates=(candidate,),
            rejections=(),
            checkpoint=decision.checkpoint,
        )


class FakeArtifactRepository(PaperLiveArtifactRepository):
    def __init__(self) -> None:
        self.initialized: list[tuple[StrategyRunIdentity, str]] = []
        self.decisions: list[StrategyDecision] = []
        self.fills: list[SimulatedFill] = []
        self.positions: dict[str, PaperPosition] = {}
        self.portfolio_updates: list[tuple[PaperPosition, ...]] = []

    async def initialize_run(
        self,
        identity: StrategyRunIdentity,
        source_description: str,
        execution: ReplayExecutionConfig,
        portfolio: PaperExitConfig,
    ) -> None:
        del execution, portfolio
        self.initialized.append((identity, source_description))

    async def load_pending_candidates(
        self,
        run_id: str,
    ) -> tuple[OrderIntentCandidate, ...]:
        del run_id
        return ()

    async def save_decision(self, decision: StrategyDecision) -> None:
        self.decisions.append(decision)

    async def save_fills(
        self,
        run_id: str,
        fills: tuple[SimulatedFill, ...],
    ) -> tuple[PaperPosition, ...]:
        self.fills.extend(fills)
        positions = tuple(
            position
            for fill in fills
            if (position := position_from_entry_fill(run_id, fill)) is not None
        )
        self.positions.update(
            {position.position_id: position for position in positions}
        )
        return positions

    async def load_open_positions(
        self,
        run_id: str,
    ) -> tuple[PaperPosition, ...]:
        del run_id
        return tuple(self.positions.values())

    async def save_portfolio(
        self,
        run_id: str,
        positions: tuple[PaperPosition, ...],
        observed_at: datetime,
        config: PaperExitConfig,
    ) -> None:
        del run_id, observed_at, config
        self.portfolio_updates.append(positions)


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


def test_daemon_persists_signal_candidate_and_virtual_fill() -> None:
    states = (fixture_state("BTCUSDT", 0), fixture_state("BTCUSDT", 1))
    identity = _identity()
    artifacts = FakeArtifactRepository()

    result = run_paper_live_daemon(
        source=states,
        strategy=SignalStrategy(identity),
        repository=FakeRepository(),
        artifact_repository=artifacts,
        config=_config(
            run_identity=identity,
            execution=ReplayExecutionConfig(latency_buckets=1),
        ),
        clock=FakeClock(states[-1].bucket_end + timedelta(seconds=1)),
    )

    assert result.processed_state_count == 2
    assert artifacts.initialized == [(identity, "postgres-runtime-states:research")]
    assert len(artifacts.decisions) == 1
    assert artifacts.decisions[0].signals[0].signal_id == "signal-1"
    assert len(artifacts.fills) == 1
    assert artifacts.fills[0].status is SimulatedFillStatus.FILLED
    assert artifacts.fills[0].filled_notional == Decimal("25")


def _config(
    *,
    checkpoint_every_states: int = 10,
    max_market_state_age_seconds: float = 120.0,
    run_identity: StrategyRunIdentity | None = None,
    execution: ReplayExecutionConfig | None = None,
) -> PaperLiveDaemonConfig:
    return PaperLiveDaemonConfig(
        run_id="run-1",
        strategy_name="compression_breakout",
        environment="research",
        checkpoint_every_states=checkpoint_every_states,
        checkpoint_every_seconds=999,
        max_market_state_age_seconds=max_market_state_age_seconds,
        run_identity=run_identity,
        source_description="postgres-runtime-states:research",
        execution=execution or ReplayExecutionConfig(),
    )


def _identity() -> StrategyRunIdentity:
    return StrategyRunIdentity(
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v0",
        config_hash="config-hash",
        run_mode=RunMode.PAPER,
        code_commit="unknown",
        created_at=datetime(2026, 7, 3, 0, 0, tzinfo=UTC),
        source_paths=("postgres-runtime-states:research",),
    )
