from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import JsonValue, MarketState15s
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    RunMode,
    StrategyCheckpoint,
    StrategyDataRequirement,
    StrategyDecision,
    StrategyRunIdentity,
    StrategySide,
    StrategySignal,
)
from crypto_momentum_lab.strategy_runner.daemon import (
    PairedPaperLiveAccount,
    PaperEntryFilterConfig,
    PaperLiveArtifactRepository,
    PaperLiveDaemonConfig,
    PaperLiveDaemonRepository,
    PaperLiveDaemonResult,
    RuntimeStrategy,
    run_paired_paper_live_daemon,
    run_paper_live_daemon,
)
from crypto_momentum_lab.strategy_runner.fills import (
    ReplayExecutionConfig,
    SimulatedFill,
    SimulatedFillStatus,
)
from crypto_momentum_lab.strategy_runner.portfolio import (
    ClosedCandle15m,
    PaperExitConfig,
    PaperExitMode,
    PaperPosition,
    PaperPositionStatus,
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


class FakeClosedCandleSource:
    def __init__(self, candles: tuple[ClosedCandle15m, ...]) -> None:
        self._candles = candles
        self.calls: list[tuple[str, datetime, datetime]] = []

    def load_closed_candles(
        self,
        *,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> tuple[ClosedCandle15m, ...]:
        self.calls.append((symbol, start, end))
        return tuple(
            candle
            for candle in self._candles
            if candle.symbol == symbol
            and candle.candle_start >= start
            and candle.candle_end <= end
        )


class FakeRepository(PaperLiveDaemonRepository):
    def __init__(self, checkpoint: StrategyCheckpoint | None = None) -> None:
        self.loaded_checkpoint = checkpoint
        self.saved_checkpoints: list[tuple[str, StrategyCheckpoint, datetime]] = []

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
        self.reset_symbols: list[str] = []
        self._checkpoint: StrategyCheckpoint | None = None
        self.checkpoint_calls = 0

    def reset_symbol(self, symbol: str) -> None:
        self.reset_symbols.append(symbol)

    def required_data(self) -> StrategyDataRequirement:
        return StrategyDataRequirement(
            base_state_interval_seconds=15,
            warmup_buckets=1,
            required_fields=("close_price",),
            max_gap_seconds=30,
            allow_entries_before_warmup=False,
        )

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
        self._checkpoint = checkpoint
        return StrategyDecision(
            signals=(),
            candidates=(),
            rejections=(),
            checkpoint=checkpoint,
        )

    def checkpoint(self) -> StrategyCheckpoint:
        self.checkpoint_calls += 1
        if self._checkpoint is None:
            raise AssertionError("checkpoint requested before processing a state")
        return self._checkpoint


class SignalStrategy(FakeStrategy):
    def __init__(
        self,
        identity: StrategyRunIdentity,
        *,
        side: StrategySide = StrategySide.LONG,
        features: dict[str, JsonValue] | None = None,
    ) -> None:
        super().__init__()
        self._identity = identity
        self._side = side
        self._features = {} if features is None else features

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
            side=self._side,
            detected_at=state.bucket_end,
            source_state_at=state.bucket_start,
            reason="compression_breakout",
            features=self._features,
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
            expires_at=state.bucket_end + timedelta(seconds=60),
            created_at=state.bucket_end,
            reason=signal.reason,
            features=self._features,
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
        entry_filter: PaperEntryFilterConfig,
    ) -> None:
        del execution, portfolio, entry_filter
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
    assert strategy.checkpoint_calls == 1


def test_daemon_does_not_build_checkpoint_for_each_state() -> None:
    states = tuple(fixture_state("BTCUSDT", index) for index in range(3))
    strategy = FakeStrategy()

    run_paper_live_daemon(
        source=states,
        strategy=strategy,
        repository=FakeRepository(),
        config=_config(checkpoint_every_states=100),
        clock=FakeClock(states[-1].bucket_end + timedelta(seconds=1)),
    )

    assert strategy.checkpoint_calls == 1


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


def test_daemon_skips_stale_market_state_without_halting() -> None:
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
    assert result.halt_reason is None
    assert repository.saved_checkpoints == []


def test_daemon_skips_stale_state_and_processes_fresh_state() -> None:
    stale_state = fixture_state("BTCUSDT", 0)
    fresh_state = fixture_state("BTCUSDT", 1)
    repository = FakeRepository()
    strategy = FakeStrategy()

    result = run_paper_live_daemon(
        source=(stale_state, fresh_state),
        strategy=strategy,
        repository=repository,
        config=_config(
            max_market_state_age_seconds=10,
        ),
        clock=FakeClock(fresh_state.bucket_end + timedelta(seconds=1)),
    )

    assert result.processed_state_count == 1
    assert result.halt_reason is None
    assert strategy.reset_symbols == ["BTCUSDT"]


def test_daemon_resets_restored_symbol_after_data_gap() -> None:
    previous = fixture_state("BTCUSDT", 0)
    fresh = fixture_state("BTCUSDT", 4)
    checkpoint = StrategyCheckpoint(
        last_processed_at_by_symbol={"BTCUSDT": previous.bucket_start},
        warmup_buckets_by_symbol={"BTCUSDT": 1},
        cooldown_buckets_remaining_by_symbol={"BTCUSDT": 0},
        payload={},
    )
    strategy = FakeStrategy()

    result = run_paper_live_daemon(
        source=(fresh,),
        strategy=strategy,
        repository=FakeRepository(checkpoint),
        config=_config(),
        clock=FakeClock(fresh.bucket_end + timedelta(seconds=1)),
    )

    assert result.processed_state_count == 1
    assert strategy.reset_symbols == ["BTCUSDT"]
    assert strategy.processed == [fresh]


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


def test_daemon_fills_new_candidate_on_current_state_without_extra_latency() -> None:
    state = fixture_state("BTCUSDT", 0)
    identity = _identity()
    artifacts = FakeArtifactRepository()

    run_paper_live_daemon(
        source=(state,),
        strategy=SignalStrategy(identity),
        repository=FakeRepository(),
        artifact_repository=artifacts,
        config=_config(
            run_identity=identity,
            execution=ReplayExecutionConfig(latency_buckets=0),
        ),
        clock=FakeClock(state.bucket_end + timedelta(seconds=1)),
    )

    assert len(artifacts.fills) == 1
    assert artifacts.fills[0].status is SimulatedFillStatus.FILLED
    assert artifacts.fills[0].target_fill_at == state.bucket_end
    assert artifacts.fills[0].filled_at == state.bucket_end


def test_daemon_requires_entry_price_above_both_15m_emas() -> None:
    state = fixture_state("BTCUSDT", 0)
    identity = _identity()
    artifacts = FakeArtifactRepository()

    run_paper_live_daemon(
        source=(state,),
        strategy=SignalStrategy(identity),
        repository=FakeRepository(),
        artifact_repository=artifacts,
        config=_config(
            run_identity=identity,
            execution=ReplayExecutionConfig(latency_buckets=0),
            entry_filter=PaperEntryFilterConfig(
                require_price_above_ema5=True,
                require_price_above_ema10=True,
            ),
        ),
        clock=FakeClock(state.bucket_end + timedelta(seconds=1)),
        entry_filter_context_loader=lambda _state: PaperEntryFilterContext(
            entry_price=Decimal("100.01"),
            ema5=Decimal("100"),
            ema10=Decimal("100"),
        ),
    )

    assert len(artifacts.decisions) == 1
    assert len(artifacts.fills) == 1


def test_daemon_rejects_equal_or_missing_ema_entry_filter_values() -> None:
    state = fixture_state("BTCUSDT", 0)
    identity = _identity()
    artifacts = FakeArtifactRepository()

    run_paper_live_daemon(
        source=(state,),
        strategy=SignalStrategy(identity),
        repository=FakeRepository(),
        artifact_repository=artifacts,
        config=_config(
            run_identity=identity,
            entry_filter=PaperEntryFilterConfig(
                require_price_above_ema5=True,
                require_price_above_ema10=True,
            ),
        ),
        clock=FakeClock(state.bucket_end + timedelta(seconds=1)),
        entry_filter_context_loader=lambda _state: PaperEntryFilterContext(
            entry_price=Decimal("100.01"),
            ema5=Decimal("100.01"),
            ema10=None,
        ),
    )

    assert artifacts.decisions == []
    assert artifacts.fills == []


def test_daemon_expires_pending_candidate_after_deadline() -> None:
    states = (fixture_state("BTCUSDT", 0), fixture_state("BTCUSDT", 5))
    identity = _identity()
    artifacts = FakeArtifactRepository()

    run_paper_live_daemon(
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

    assert [fill.status for fill in artifacts.fills] == [
        SimulatedFillStatus.EXPIRED
    ]


def test_daemon_throttles_open_position_mark_persistence() -> None:
    states = tuple(fixture_state("BTCUSDT", index) for index in range(5))
    identity = _identity()
    artifacts = FakeArtifactRepository()
    position = position_from_entry_fill(
        identity.run_id,
        _filled_entry(
            symbol="BTCUSDT",
            filled_at=states[0].bucket_start - timedelta(seconds=1),
        ),
    )
    assert position is not None
    artifacts.positions[position.position_id] = position

    run_paper_live_daemon(
        source=states,
        strategy=FakeStrategy(),
        repository=FakeRepository(),
        artifact_repository=artifacts,
        config=_config(run_identity=identity),
        clock=FakeClock(states[-1].bucket_end + timedelta(seconds=1)),
    )

    assert len(artifacts.portfolio_updates) == 3
    assert artifacts.portfolio_updates[0] == ()
    assert artifacts.portfolio_updates[1][0].updated_at == states[3].bucket_end
    assert artifacts.portfolio_updates[2] == ()


def test_paired_daemon_calculates_entries_once_and_fans_out_accounts() -> None:
    states = (fixture_state("BTCUSDT", 0), fixture_state("BTCUSDT", 1))
    first_identity = _identity()
    second_identity = _identity("run-2")
    third_identity = _identity("run-3")
    strategy = SignalStrategy(first_identity)
    first_artifacts = FakeArtifactRepository()
    second_artifacts = FakeArtifactRepository()
    third_artifacts = FakeArtifactRepository()
    first_config = _config(
        run_identity=first_identity,
        execution=ReplayExecutionConfig(latency_buckets=0),
    )
    second_config = _config(
        run_id="run-2",
        run_identity=second_identity,
        execution=ReplayExecutionConfig(latency_buckets=0),
    )
    third_config = _config(
        run_id="run-3",
        run_identity=third_identity,
        execution=ReplayExecutionConfig(latency_buckets=0),
        portfolio=PaperExitConfig(
            exit_mode=PaperExitMode.CANDLE_15M,
            max_holding_buckets=5760,
            candle_minimum_holding_buckets=180,
        ),
    )

    result = run_paired_paper_live_daemon(
        source=states,
        strategy=strategy,
        accounts=(
            PairedPaperLiveAccount(
                repository=FakeRepository(),
                artifact_repository=first_artifacts,
                config=first_config,
            ),
            PairedPaperLiveAccount(
                repository=FakeRepository(),
                artifact_repository=second_artifacts,
                config=second_config,
            ),
            PairedPaperLiveAccount(
                repository=FakeRepository(),
                artifact_repository=third_artifacts,
                config=third_config,
            ),
        ),
        clock=FakeClock(states[-1].bucket_end + timedelta(seconds=1)),
    )

    assert [item.processed_state_count for item in result.account_results] == [
        2,
        2,
        2,
    ]
    assert len(strategy.processed) == 2
    assert strategy.checkpoint_calls == 1
    assert len(first_artifacts.decisions) == 1
    assert len(second_artifacts.decisions) == 1
    assert len(third_artifacts.decisions) == 1
    first_signal = first_artifacts.decisions[0].signals[0]
    second_signal = second_artifacts.decisions[0].signals[0]
    third_signal = third_artifacts.decisions[0].signals[0]
    assert first_signal.run_id == first_identity.run_id
    assert second_signal.run_id == second_identity.run_id
    assert third_signal.run_id == third_identity.run_id
    assert first_signal.features == second_signal.features
    assert first_signal.features == third_signal.features
    assert first_signal.signal_id != second_signal.signal_id
    assert first_signal.signal_id != third_signal.signal_id
    assert second_signal.signal_id != third_signal.signal_id
    assert first_artifacts.fills[0].filled_notional == Decimal("25")
    assert second_artifacts.fills[0].filled_notional == Decimal("25")
    assert third_artifacts.fills[0].filled_notional == Decimal("25")
    assert first_artifacts.fills[0].filled_at == states[0].bucket_end
    assert second_artifacts.fills[0].filled_at == states[0].bucket_end
    assert third_artifacts.fills[0].filled_at == states[0].bucket_end


def test_paired_entry_filters_preserve_b2_and_c1_subsets() -> None:
    state = fixture_state("BTCUSDT", 0)
    baseline_identity = _identity()
    b2_identity = _identity("run-b2")
    c1_identity = _identity("run-c1")
    baseline_artifacts = FakeArtifactRepository()
    b2_artifacts = FakeArtifactRepository()
    c1_artifacts = FakeArtifactRepository()

    run_paired_paper_live_daemon(
        source=(state,),
        strategy=SignalStrategy(
            baseline_identity,
            features={"aggressive_imbalance": "0.80"},
        ),
        accounts=(
            PairedPaperLiveAccount(
                repository=FakeRepository(),
                artifact_repository=baseline_artifacts,
                config=_config(
                    run_identity=baseline_identity,
                    execution=ReplayExecutionConfig(latency_buckets=0),
                ),
            ),
            PairedPaperLiveAccount(
                repository=FakeRepository(),
                artifact_repository=b2_artifacts,
                config=_config(
                    run_id="run-b2",
                    run_identity=b2_identity,
                    execution=ReplayExecutionConfig(latency_buckets=0),
                    entry_filter=PaperEntryFilterConfig(allow_short=False),
                ),
            ),
            PairedPaperLiveAccount(
                repository=FakeRepository(),
                artifact_repository=c1_artifacts,
                config=_config(
                    run_id="run-c1",
                    run_identity=c1_identity,
                    execution=ReplayExecutionConfig(latency_buckets=0),
                    entry_filter=PaperEntryFilterConfig(
                        allow_short=False,
                        max_abs_aggressive_imbalance=Decimal("0.7113"),
                    ),
                ),
            ),
        ),
        clock=FakeClock(state.bucket_end + timedelta(seconds=1)),
    )

    assert len(baseline_artifacts.decisions) == 1
    assert len(b2_artifacts.decisions) == 1
    assert c1_artifacts.decisions == []
    assert len(b2_artifacts.fills) == 1
    assert c1_artifacts.fills == []


def test_paired_long_only_filter_rejects_short_signal() -> None:
    state = fixture_state("BTCUSDT", 0)
    baseline_identity = _identity()
    long_only_identity = _identity("run-long-only")
    baseline_artifacts = FakeArtifactRepository()
    long_only_artifacts = FakeArtifactRepository()

    run_paired_paper_live_daemon(
        source=(state,),
        strategy=SignalStrategy(
            baseline_identity,
            side=StrategySide.SHORT,
            features={"aggressive_imbalance": "-0.60"},
        ),
        accounts=(
            PairedPaperLiveAccount(
                repository=FakeRepository(),
                artifact_repository=baseline_artifacts,
                config=_config(run_identity=baseline_identity),
            ),
            PairedPaperLiveAccount(
                repository=FakeRepository(),
                artifact_repository=long_only_artifacts,
                config=_config(
                    run_id="run-long-only",
                    run_identity=long_only_identity,
                    entry_filter=PaperEntryFilterConfig(allow_short=False),
                ),
            ),
        ),
        clock=FakeClock(state.bucket_end + timedelta(seconds=1)),
    )

    assert len(baseline_artifacts.decisions) == 1
    assert long_only_artifacts.decisions == []


def test_single_daemon_filters_liquidation_trade_count() -> None:
    state = fixture_state("BTCUSDT", 0)
    identity = _identity()
    artifacts = FakeArtifactRepository()

    run_paper_live_daemon(
        source=(state,),
        strategy=SignalStrategy(
            identity,
            features={"cluster_trade_count": 1001},
        ),
        repository=FakeRepository(),
        artifact_repository=artifacts,
        config=_config(
            run_identity=identity,
            entry_filter=PaperEntryFilterConfig(
                max_cluster_trade_count=1000
            ),
        ),
        clock=FakeClock(state.bucket_end + timedelta(seconds=1)),
    )

    assert artifacts.decisions == []
    assert artifacts.fills == []


def test_paired_daemon_only_reads_the_latest_closed_candle() -> None:
    state = fixture_state("BTCUSDT", 180)
    checkpoint_at = state.bucket_start - timedelta(seconds=15)
    checkpoint = StrategyCheckpoint(
        last_processed_at_by_symbol={state.symbol: checkpoint_at},
        warmup_buckets_by_symbol={state.symbol: 1},
        cooldown_buckets_remaining_by_symbol={state.symbol: 0},
        payload={},
    )
    fixed_identity = _identity()
    candle_identity = _identity("run-2")
    fixed_artifacts = FakeArtifactRepository()
    candle_artifacts = FakeArtifactRepository()
    position = position_from_entry_fill(
        candle_identity.run_id,
        replace(
            _filled_entry(
                symbol=state.symbol,
                filled_at=state.bucket_start - timedelta(minutes=25),
            ),
            side=StrategySide.SHORT,
        ),
    )
    assert position is not None
    candle_artifacts.positions[position.position_id] = position
    candle = ClosedCandle15m(
        symbol=state.symbol,
        candle_start=state.bucket_start - timedelta(minutes=15),
        candle_end=state.bucket_start,
        open_price=Decimal("100"),
        close_price=Decimal("101"),
    )
    candle_source = FakeClosedCandleSource((candle,))

    run_paired_paper_live_daemon(
        source=(state,),
        strategy=FakeStrategy(),
        accounts=(
            PairedPaperLiveAccount(
                repository=FakeRepository(checkpoint),
                artifact_repository=fixed_artifacts,
                config=_config(run_identity=fixed_identity),
            ),
            PairedPaperLiveAccount(
                repository=FakeRepository(checkpoint),
                artifact_repository=candle_artifacts,
                config=_config(
                    run_id="run-2",
                    run_identity=candle_identity,
                    portfolio=PaperExitConfig(
                        exit_mode=PaperExitMode.CANDLE_15M,
                        max_holding_buckets=5760,
                    ),
                ),
            ),
        ),
        clock=FakeClock(state.bucket_end + timedelta(seconds=1)),
        candle_source=candle_source,
    )

    expected_start = state.bucket_start - timedelta(minutes=15)
    assert candle_source.calls == [
        (
            state.symbol,
            expected_start,
            state.bucket_start,
        )
    ]
    closed = candle_artifacts.portfolio_updates[0][0]
    assert closed.status is PaperPositionStatus.CLOSED
    assert closed.close_reason == "candle_15m_bullish"
    assert closed.closed_at == candle.candle_end
    assert closed.exit_price == candle.close_price


def test_daemon_uses_protected_symbol_for_exit_without_opening_new_trade() -> None:
    state = fixture_state("BTCUSDT", 80)
    identity = _identity()
    artifacts = FakeArtifactRepository()
    position = position_from_entry_fill(
        identity.run_id,
        _filled_entry(
            symbol=state.symbol,
            filled_at=state.bucket_start - timedelta(hours=1),
        ),
    )
    assert position is not None
    artifacts.positions[position.position_id] = position

    result = run_paper_live_daemon(
        source=(state,),
        strategy=SignalStrategy(identity),
        repository=FakeRepository(),
        artifact_repository=artifacts,
        config=_config(
            run_identity=identity,
            portfolio=PaperExitConfig(max_holding_buckets=1),
        ),
        clock=FakeClock(state.bucket_end + timedelta(seconds=1)),
        entry_symbol_loader=lambda _observed_at: frozenset({"ETHUSDT"}),
    )

    assert result.processed_state_count == 1
    assert artifacts.decisions == []
    assert artifacts.fills == []
    closed = artifacts.portfolio_updates[0][0]
    assert closed.status is PaperPositionStatus.CLOSED
    assert closed.close_reason == "max_holding_period"


def test_daemon_loads_entry_symbols_for_current_state_time() -> None:
    first = fixture_state("BTCUSDT", 80)
    second = fixture_state("BTCUSDT", 82)
    observed_at: list[datetime] = []

    def load_symbols(state_at: datetime) -> frozenset[str]:
        observed_at.append(state_at)
        return frozenset({"BTCUSDT"})

    result = run_paper_live_daemon(
        source=(first, second),
        strategy=FakeStrategy(),
        repository=FakeRepository(),
        config=_config(),
        clock=FakeClock(second.bucket_end + timedelta(seconds=1)),
        entry_symbol_loader=load_symbols,
    )

    assert result.processed_state_count == 2
    assert observed_at == [first.bucket_start, second.bucket_start]


def _config(
    *,
    run_id: str = "run-1",
    checkpoint_every_states: int = 10,
    max_market_state_age_seconds: float = 120.0,
    run_identity: StrategyRunIdentity | None = None,
    execution: ReplayExecutionConfig | None = None,
    portfolio: PaperExitConfig | None = None,
    entry_filter: PaperEntryFilterConfig | None = None,
) -> PaperLiveDaemonConfig:
    return PaperLiveDaemonConfig(
        run_id=run_id,
        strategy_name="compression_breakout",
        environment="research",
        checkpoint_every_states=checkpoint_every_states,
        checkpoint_every_seconds=999,
        max_market_state_age_seconds=max_market_state_age_seconds,
        run_identity=run_identity,
        source_description="postgres-runtime-states:research",
        execution=execution or ReplayExecutionConfig(),
        portfolio=portfolio or PaperExitConfig(),
        entry_filter=entry_filter or PaperEntryFilterConfig(),
    )


def _identity(run_id: str = "run-1") -> StrategyRunIdentity:
    return StrategyRunIdentity(
        run_id=run_id,
        strategy_name="compression_breakout",
        strategy_version="v0",
        config_hash="config-hash",
        run_mode=RunMode.PAPER,
        code_commit="unknown",
        created_at=datetime(2026, 7, 3, 0, 0, tzinfo=UTC),
        source_paths=("postgres-runtime-states:research",),
    )


def _filled_entry(*, symbol: str, filled_at: datetime) -> SimulatedFill:
    return SimulatedFill(
        fill_id=f"fill-{symbol}",
        candidate_id=f"candidate-{symbol}",
        signal_id=f"signal-{symbol}",
        symbol=symbol,
        side=StrategySide.LONG,
        status=SimulatedFillStatus.FILLED,
        target_fill_at=filled_at,
        filled_at=filled_at,
        requested_notional=Decimal("25"),
        filled_notional=Decimal("25"),
        quantity=Decimal("0.25"),
        reference_midpoint=Decimal("100"),
        spread=None,
        fill_price=Decimal("100"),
        fee=Decimal("0.01"),
        total_cost=Decimal("0.01"),
        cost_bps=Decimal("4"),
        reason="filled",
    )
