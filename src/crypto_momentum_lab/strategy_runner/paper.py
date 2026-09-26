import json
from collections import Counter, deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from crypto_momentum_lab.domain.decision import (
    DecisionEngine,
    EffectivePolicy,
    FillModel,
    FrozenDecisionInputs,
    PolicyState,
    SimulationExecutionAdapter,
    build_decision_input,
    map_decision_rejection_reason,
)
from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionHealthStatus,
    PositionKey,
    PositionLedgerBatch,
    PositionView,
)
from crypto_momentum_lab.domain.market.market_book import compute_market_state_hash
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.revision_models import (
    MarketEnvelope,
    MarketRevisionRef,
    MarketVisibilityMode,
)
from crypto_momentum_lab.domain.runtime.capability_evaluator import (
    CapabilityEvaluator,
    CapabilityEvidence,
    SystemAction,
)
from crypto_momentum_lab.domain.runtime.runtime_plan import (
    RuntimePlan,
    RuntimePlanCompiler,
)
from crypto_momentum_lab.domain.strategy import (
    OrderIntentCandidate,
    RejectionReason,
    RunMode,
    StrategyCheckpoint,
    StrategyRejection,
    StrategyRunIdentity,
    StrategySignal,
    deterministic_config_hash,
)
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
)
from crypto_momentum_lab.strategies.liquidation_cascade import (
    LiquidationCascadeConfig,
)
from crypto_momentum_lab.strategies.order_flow_impulse import OrderFlowImpulseConfig
from crypto_momentum_lab.strategy_runner.fills import (
    FillSummaryValue,
    ReplayExecutionConfig,
    SimulatedFill,
    SimulatedFillStatus,
    fill_summary,
    pending_candidate_fill,
    resolve_candidate_fill_at_state,
    simulate_candidate_fill,
)
from crypto_momentum_lab.strategy_runner.portfolio import (
    Candle15mAggregator,
    ClosedCandle15m,
    PaperExitConfig,
    PaperExitMode,
    PaperPosition,
    PaperPositionStatus,
    mark_positions,
    position_from_entry_fill,
)
from crypto_momentum_lab.strategy_runner.registry import (
    StrategyRegistryError,
    build_runtime_config,
    build_runtime_strategy,
)
from crypto_momentum_lab.strategy_runner.serialization import jsonable


class PaperRunnerError(RuntimeError):
    pass


class PaperMarketStateSource(Protocol):
    @property
    def description(self) -> str:
        pass

    def __iter__(self) -> Iterator[MarketState15s]:
        pass


@dataclass(frozen=True, slots=True)
class InMemoryPaperMarketStateSource:
    states: tuple[MarketState15s, ...]
    description: str = "memory"

    def __iter__(self) -> Iterator[MarketState15s]:
        return iter(self.states)


@dataclass(frozen=True, slots=True)
class PaperRunnerConfig:
    strategy_name: str
    run_id: str
    code_commit: str
    generated_at: datetime
    compression_breakout: CompressionBreakoutConfig
    candidate_notional: Decimal | None
    candidate_ttl_buckets: int
    signal_interval_seconds: int = 300
    initial_cash_balance: Decimal = Decimal("10000.00")
    order_flow_impulse: OrderFlowImpulseConfig | None = None
    liquidation_cascade: LiquidationCascadeConfig | None = None
    execution: ReplayExecutionConfig = field(default_factory=ReplayExecutionConfig)
    max_states: int | None = None
    reset_on_gap: bool = True
    portfolio: PaperExitConfig = field(default_factory=PaperExitConfig)

    def __post_init__(self) -> None:
        if not self.strategy_name:
            raise ValueError("strategy_name must not be empty")
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if not self.code_commit:
            raise ValueError("code_commit must not be empty")
        if not _is_aware(self.generated_at):
            raise ValueError("generated_at must be timezone-aware")
        if self.candidate_notional is not None and self.candidate_notional <= 0:
            raise ValueError("candidate_notional must be positive")
        if self.initial_cash_balance < Decimal("0"):
            raise ValueError("initial_cash_balance must not be negative")
        if self.candidate_ttl_buckets <= 0:
            raise ValueError("candidate_ttl_buckets must be positive")
        if self.signal_interval_seconds <= 0:
            raise ValueError("signal_interval_seconds must be positive")
        if self.max_states is not None and self.max_states <= 0:
            raise ValueError("max_states must be positive")
        if not isinstance(self.reset_on_gap, bool):
            raise TypeError("reset_on_gap must be a bool")


@dataclass(frozen=True, slots=True)
class PaperTradingRunReport:
    schema_version: int
    generated_at: datetime
    run: StrategyRunIdentity
    execution_config: ReplayExecutionConfig
    source_description: str
    input_state_count: int
    processed_symbol_count: int
    signals: tuple[StrategySignal, ...]
    candidates: tuple[OrderIntentCandidate, ...]
    paper_fills: tuple[SimulatedFill, ...]
    pending_candidate_count: int
    rejection_summary: dict[str, dict[str, int]]
    final_checkpoint: StrategyCheckpoint
    summary_counts: dict[str, dict[str, int]]
    fill_summary: dict[str, dict[str, FillSummaryValue]]
    portfolio_config: PaperExitConfig = field(default_factory=PaperExitConfig)
    paper_positions: tuple[PaperPosition, ...] = ()
    runtime_plan: RuntimePlan | None = None


def run_paper_trading(
    *,
    source: PaperMarketStateSource,
    config: PaperRunnerConfig,
) -> PaperTradingRunReport:
    runtime_plan = RuntimePlanCompiler.compile(
        environment="paper",
        account_label="paper_account",
        strategy_name=config.strategy_name,
        git_commit=config.code_commit,
        overrides={
            "target_notional": config.candidate_notional or Decimal("500.00"),
        },
    )
    evaluator = CapabilityEvaluator()
    evidence = CapabilityEvidence(
        evidence_version="ev_paper_init",
        market_freshness_seconds=1.0,
        is_account_concordant=True,
        is_account_identity_verified=True,
        is_approval_valid=True,
        is_lease_active=True,
    )
    sim_eval = evaluator.evaluate(
        action=SystemAction.ENTER,
        evidence=evidence,
        plan=runtime_plan,
    )
    if not sim_eval.allowed:
        raise PaperRunnerError(
            f"CapabilityEvaluator blocked simulation: {sim_eval.reason}"
        )
    _sim_adapter = SimulationExecutionAdapter(
        default_fill_model=FillModel(fee_rate=config.execution.taker_fee_rate),
    )
    _decision_engine = DecisionEngine()
    try:
        runtime_config = build_runtime_config(
            config.strategy_name,
            config=_runtime_config_payload(config),
        )
    except StrategyRegistryError as error:
        raise PaperRunnerError(str(error)) from error
    identity = StrategyRunIdentity(
        run_id=config.run_id,
        strategy_name=config.strategy_name,
        strategy_version="v0",
        config_hash=deterministic_config_hash(runtime_config),
        run_mode=RunMode.PAPER,
        code_commit=config.code_commit,
        created_at=config.generated_at,
        source_paths=(source.description,),
    )
    strategy = build_runtime_strategy(
        config.strategy_name,
        config=_runtime_config_payload(config),
        identity=identity,
    )

    signals: list[StrategySignal] = []
    candidates: list[OrderIntentCandidate] = []
    rejections: list[StrategyRejection] = []
    paper_fills: list[SimulatedFill] = []
    pending_candidates: list[OrderIntentCandidate] = []
    positions_by_id: dict[str, PaperPosition] = {}
    journals_by_symbol: dict[str, AccountJournal] = {}
    candle_aggregator = (
        Candle15mAggregator()
        if config.portfolio.exit_mode is PaperExitMode.CANDLE_15M
        else None
    )
    candle_history_by_symbol: dict[str, deque[ClosedCandle15m]] = {}
    last_processed_at_by_symbol: dict[str, datetime] = {}
    max_gap_seconds = strategy.required_data().max_gap_seconds
    input_state_count = 0
    policy_state = PolicyState()

    for state in source:
        if config.max_states is not None and input_state_count >= config.max_states:
            break
        _validate_state(state, last_processed_at_by_symbol)
        input_state_count += 1
        last_processed_at = last_processed_at_by_symbol.get(state.symbol)
        if (
            config.reset_on_gap
            and last_processed_at is not None
            and (state.bucket_start - last_processed_at).total_seconds()
            > max_gap_seconds
        ):
            strategy.reset_symbol(state.symbol)

        closed_candle = (
            None if candle_aggregator is None else candle_aggregator.observe(state)
        )
        candle_history: deque[ClosedCandle15m] | None = None
        if closed_candle is not None:
            candle_history = candle_history_by_symbol.setdefault(
                state.symbol,
                deque(maxlen=max(2, config.portfolio.candle_confirmation_count)),
            )
            if (
                not candle_history
                or candle_history[-1].candle_start
                != closed_candle.candle_start
            ):
                candle_history.append(closed_candle)
        else:
            candle_history = candle_history_by_symbol.get(state.symbol)
        position_updates = mark_positions(
            positions=tuple(
                position
                for position in positions_by_id.values()
                if position.status is PaperPositionStatus.OPEN
            ),
            state=state,
            config=config.portfolio,
            taker_fee_rate=config.execution.taker_fee_rate,
            closed_candle=closed_candle,
            closed_candles=(
                () if candle_history is None else tuple(candle_history)
            ),
        )
        for position in position_updates:
            positions_by_id[position.position_id] = position

        market_ref = MarketRevisionRef(
            scope="paper",
            symbol=state.symbol,
            interval="15s",
            bucket_start=state.bucket_start,
            bucket_end=state.bucket_end,
            revision_id=f"rev_{state.symbol}_{int(state.bucket_start.timestamp())}",
            content_hash=compute_market_state_hash(state),
            published_at=state.bucket_end,
            source_epoch=f"ep_{config.run_id}",
            visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
        )
        envelope = MarketEnvelope(ref=market_ref, state=state)
        pos_key = PositionKey(
            environment="paper",
            account_label=config.run_id,
            symbol=state.symbol,
            position_side=FuturesPositionSide.BOTH,
        )
        journal = journals_by_symbol.setdefault(state.symbol, AccountJournal(pos_key))
        open_positions = [
            p
            for p in positions_by_id.values()
            if p.symbol == state.symbol and p.status is PaperPositionStatus.OPEN
        ]
        batches = tuple(
            PositionLedgerBatch(
                batch_id=f"batch_{p.position_id}",
                episode_id=f"ep_{p.position_id}",
                quantity=p.quantity,
                original_quantity=p.quantity,
                entry_price=p.entry_price,
                opened_at=p.opened_at,
            )
            for p in open_positions
        )
        pos_view = PositionView(
            key=pos_key,
            projection_version=f"pv_{input_state_count}",
            input_revision=input_state_count,
            event_cut=state.bucket_end,
            policy_version="v1",
            schema_version="v1",
            coverage=None,
            active_episode=None,
            batches=batches,
            unallocated_quantity=Decimal("0"),
            reconciliation_gap=Decimal("0"),
            health_status=PositionHealthStatus.READY,
        )
        frozen = FrozenDecisionInputs(
            position_view=pos_view,
            cash_balance=config.initial_cash_balance,
            policy_state=policy_state,
            universe_version="univ_v1",
            risk_config_version="risk_v1",
        )
        dec_input = build_decision_input(
            state=state,
            frozen=frozen,
            clock_sequence=input_state_count,
            scope="paper",
            source_epoch=f"ep_{config.run_id}",
        )
        decision = strategy.on_market_state(state)
        signals.extend(decision.signals)

        evaluated_candidates = decision.candidates if decision.candidates else [None]
        for raw_cand in evaluated_candidates:
            policy = EffectivePolicy(
                policy_id=f"policy_{config.strategy_name}",
                strategy_name=config.strategy_name,
                target_notional=config.candidate_notional or Decimal("500.00"),
                candidate_generator=(
                    lambda inp, st, _c=raw_cand: _c
                ),
            )
            dec_res = _decision_engine.evaluate(dec_input, policy_state, policy)
            policy_state = dec_res.next_policy_state

            if dec_res.intent is not None:
                candidates.append(dec_res.intent)
                pending_candidates.append(dec_res.intent)
            elif raw_cand is not None:
                label = map_decision_rejection_reason(dec_res.rejection_reason)
                rejections.append(
                    StrategyRejection(
                        reason=getattr(
                            RejectionReason, label, RejectionReason.NO_SIGNAL
                        ),
                        symbol=state.symbol,
                        bucket_start=state.bucket_start,
                        details={
                            "decision_id": dec_res.decision_id,
                            "raw_reason": (
                                dec_res.rejection_reason
                                or "decision_engine_rejected"
                            ),
                            "candidate_id": raw_cand.candidate_id,
                        },
                    )
                )
        rejections.extend(decision.rejections)
        # The strategy consumes a closed state.  Resolve after the decision so
        # a zero-latency candidate can fill at this state's bucket_end, never
        # at bucket_start where the state was still incomplete.
        pending_candidates, fills = _resolve_pending_candidates(
            pending_candidates=tuple(pending_candidates),
            state=state,
            execution=config.execution,
        )
        paper_fills.extend(fills)
        for fill in fills:
            opened = position_from_entry_fill(config.run_id, fill)
            if opened is not None:
                positions_by_id[opened.position_id] = opened
                matching_cand = next(
                    (c for c in candidates if c.candidate_id == fill.candidate_id),
                    None,
                )
                if matching_cand is not None:
                    try:
                        _sim_adapter.execute_entry(matching_cand, envelope, journal)
                    except Exception:
                        pass
        last_processed_at_by_symbol[state.symbol] = state.bucket_start

    if input_state_count == 0:
        raise PaperRunnerError("no market states to paper trade")
    checkpoint = strategy.checkpoint()

    shutdown_fills = _finalize_pending_candidates(
        pending_candidates=tuple(pending_candidates),
        last_processed_at_by_symbol=last_processed_at_by_symbol,
        execution=config.execution,
    )
    paper_fills.extend(shutdown_fills)
    for fill in shutdown_fills:
        reopened = position_from_entry_fill(config.run_id, fill)
        if reopened is not None:
            positions_by_id[reopened.position_id] = reopened

    signal_tuple = tuple(signals)
    candidate_tuple = tuple(candidates)
    fill_tuple = tuple(paper_fills)
    position_tuple = tuple(
        sorted(
            positions_by_id.values(),
            key=lambda position: (position.opened_at, position.position_id),
        )
    )
    _validate_unique_ids(signal_tuple, candidate_tuple, fill_tuple)
    _validate_candidate_references(signal_tuple, candidate_tuple)
    _validate_position_references(
        position_tuple,
        identity.run_id,
        signal_tuple,
        fill_tuple,
    )
    return PaperTradingRunReport(
        schema_version=2,
        generated_at=config.generated_at,
        run=identity,
        execution_config=config.execution,
        source_description=source.description,
        input_state_count=input_state_count,
        processed_symbol_count=len(last_processed_at_by_symbol),
        signals=signal_tuple,
        candidates=candidate_tuple,
        paper_fills=fill_tuple,
        pending_candidate_count=sum(
            1 for fill in fill_tuple if fill.status is SimulatedFillStatus.PENDING
        ),
        rejection_summary=_rejection_summary(tuple(rejections)),
        final_checkpoint=checkpoint,
        summary_counts=_summary_counts(signal_tuple, position_tuple),
        fill_summary=fill_summary(fill_tuple),
        portfolio_config=config.portfolio,
        paper_positions=position_tuple,
        runtime_plan=runtime_plan,
    )


def write_paper_trading_report(
    report: PaperTradingRunReport,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(jsonable(report), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _resolve_pending_candidates(
    *,
    pending_candidates: tuple[OrderIntentCandidate, ...],
    state: MarketState15s,
    execution: ReplayExecutionConfig,
) -> tuple[list[OrderIntentCandidate], list[SimulatedFill]]:
    remaining: list[OrderIntentCandidate] = []
    fills: list[SimulatedFill] = []
    for candidate in pending_candidates:
        if candidate.symbol != state.symbol:
            remaining.append(candidate)
            continue
        fill = resolve_candidate_fill_at_state(
            candidate=candidate,
            state=state,
            execution=execution,
        )
        if fill is None:
            remaining.append(candidate)
        else:
            fills.append(fill)
    return remaining, fills


def _finalize_pending_candidates(
    *,
    pending_candidates: tuple[OrderIntentCandidate, ...],
    last_processed_at_by_symbol: dict[str, datetime],
    execution: ReplayExecutionConfig,
) -> tuple[SimulatedFill, ...]:
    fills: list[SimulatedFill] = []
    for candidate in pending_candidates:
        last_processed_at = last_processed_at_by_symbol.get(candidate.symbol)
        if last_processed_at is not None and last_processed_at >= candidate.expires_at:
            fills.append(
                simulate_candidate_fill(
                    candidate=candidate,
                    states=(),
                    execution=execution,
                )
            )
            continue
        fills.append(
            pending_candidate_fill(
                candidate=candidate,
                execution=execution,
                reason="source_ended_before_fill",
            )
        )
    return tuple(fills)


def _validate_state(
    state: MarketState15s,
    last_processed_at_by_symbol: dict[str, datetime],
) -> None:
    if not _is_aware(state.bucket_start):
        raise PaperRunnerError("bucket_start must be timezone-aware")
    if not _is_aware(state.bucket_end):
        raise PaperRunnerError("bucket_end must be timezone-aware")
    previous = last_processed_at_by_symbol.get(state.symbol)
    if previous is not None and state.bucket_start < previous:
        raise PaperRunnerError("state moved backward for symbol")


def _runtime_config_payload(config: PaperRunnerConfig) -> dict[str, object]:
    payload: dict[str, object] = {
        "candidate_notional": config.candidate_notional,
        "candidate_ttl_buckets": config.candidate_ttl_buckets,
        "signal_interval_seconds": config.signal_interval_seconds,
        "compression_breakout": config.compression_breakout,
    }
    if config.order_flow_impulse is not None:
        payload["order_flow_impulse"] = config.order_flow_impulse
    if config.liquidation_cascade is not None:
        payload["liquidation_cascade"] = config.liquidation_cascade
    return payload


def _validate_unique_ids(
    signals: tuple[StrategySignal, ...],
    candidates: tuple[OrderIntentCandidate, ...],
    fills: tuple[SimulatedFill, ...],
) -> None:
    signal_ids = tuple(signal.signal_id for signal in signals)
    candidate_ids = tuple(candidate.candidate_id for candidate in candidates)
    fill_ids = tuple(fill.fill_id for fill in fills)
    if len(signal_ids) != len(set(signal_ids)):
        raise PaperRunnerError("duplicate signal_id produced")
    if len(candidate_ids) != len(set(candidate_ids)):
        raise PaperRunnerError("duplicate candidate_id produced")
    if len(fill_ids) != len(set(fill_ids)):
        raise PaperRunnerError("duplicate fill_id produced")


def _validate_candidate_references(
    signals: tuple[StrategySignal, ...],
    candidates: tuple[OrderIntentCandidate, ...],
) -> None:
    signal_ids = {signal.signal_id for signal in signals}
    for candidate in candidates:
        if candidate.signal_id not in signal_ids:
            raise PaperRunnerError("candidate references unknown signal_id")


def _validate_position_references(
    positions: tuple[PaperPosition, ...],
    run_id: str,
    signals: tuple[StrategySignal, ...],
    fills: tuple[SimulatedFill, ...],
) -> None:
    signal_ids = {signal.signal_id for signal in signals}
    fills_by_id = {fill.fill_id: fill for fill in fills}
    position_ids = tuple(position.position_id for position in positions)
    if len(position_ids) != len(set(position_ids)):
        raise PaperRunnerError("duplicate position_id produced")
    for position in positions:
        if position.run_id != run_id:
            raise PaperRunnerError("position run_id does not match report")
        if position.entry_fill_id not in fills_by_id:
            raise PaperRunnerError("position references unknown fill_id")
        fill = fills_by_id[position.entry_fill_id]
        if fill.status is not SimulatedFillStatus.FILLED:
            raise PaperRunnerError("position references non-filled fill")
        if position.signal_id not in signal_ids:
            raise PaperRunnerError("position references unknown signal_id")
        if fill.signal_id != position.signal_id:
            raise PaperRunnerError("position signal_id does not match fill")


def _rejection_summary(
    rejections: tuple[StrategyRejection, ...],
) -> dict[str, dict[str, int]]:
    counts: dict[str, Counter[str]] = {}
    for rejection in rejections:
        counts.setdefault(rejection.reason.value, Counter())
        counts[rejection.reason.value][rejection.symbol] += 1
    return {
        reason: dict(sorted(symbol_counts.items()))
        for reason, symbol_counts in sorted(counts.items())
    }


def _summary_counts(
    signals: tuple[StrategySignal, ...],
    positions: tuple[PaperPosition, ...] = (),
) -> dict[str, dict[str, int]]:
    by_side = Counter(signal.side.value for signal in signals)
    by_symbol = Counter(signal.symbol for signal in signals)
    summary = {
        "signals_by_side": dict(sorted(by_side.items())),
        "signals_by_symbol": dict(sorted(by_symbol.items())),
    }
    positions_by_status = Counter(position.status.value for position in positions)
    exits_by_reason = Counter(
        position.close_reason
        for position in positions
        if position.status is PaperPositionStatus.CLOSED
        and position.close_reason is not None
    )
    summary["positions_by_status"] = dict(sorted(positions_by_status.items()))
    summary["exits_by_reason"] = dict(sorted(exits_by_reason.items()))
    return summary


def _is_aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None
