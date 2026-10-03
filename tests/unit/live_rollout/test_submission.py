from dataclasses import fields, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import pytest

from crypto_momentum_lab.domain.execution.order_rules import SymbolTradingRules
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    PreparedOrderSubmission,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
from crypto_momentum_lab.execution_account.orders.coordinator import (
    CoordinatedOrderExecutionPort,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.exits import (
    ManagedLivePosition,
    ManagedLivePositionBatch,
)
from crypto_momentum_lab.live_rollout.limits import FixedLiveLimits
from crypto_momentum_lab.live_rollout.submission import (
    LiveCandidateSubmission,
    LiveSubmissionConfig,
)
from crypto_momentum_lab.risk.gateway import RiskGateway
from tests.unit.live_rollout.test_daemon import _runtime_context
from tests.fixtures.live_market import _intent, _state

NOW = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC)


class RecordingPreparedRepository:
    def __init__(self) -> None:
        self.prepare_calls: list[dict[str, Any]] = []
        self.saved_intents = 0

    async def save_approved_intent(self, intent, evaluation) -> None:
        del intent, evaluation
        self.saved_intents += 1

    async def prepare_submission(self, **kwargs):
        self.prepare_calls.append(kwargs)
        plan = cast(OrderExecutionPlan, kwargs["plan"])
        return PreparedOrderSubmission(
            plan=plan,
            submitting_event=ExchangeOrderEvent(
                event_id="event-1",
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.SUBMITTING,
                occurred_at=NOW,
                exchange_order_id=None,
                details={},
            ),
        )


class RecordingCoordinator:
    def __init__(self) -> None:
        self.events: list[str] = []

    def configure_submission(self, repository, *, admission=None, clock=lambda: NOW):
        self.repository = repository
        self.admission = admission
        self.clock = clock

    async def prepare_and_execute(self, plan, *, preparation):
        self.events.append("prepare")
        if self.admission is not None and self.admission.rejection_reason(plan, preparation):
            return None
        values = {f.name: getattr(preparation, f.name) for f in fields(preparation)
                  if f.name != "context_token"}
        prepared = await self.repository.prepare_submission(
            plan=plan, prepared_at=self.clock(), **values,
        )
        if prepared is None:
            return None
        self.events.append("exchange")
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="exchange-1", plan=plan,
            prepared_at=prepared.submitting_event.occurred_at,
        )


def _submission(
    *,
    repository,
    state_machine,
    limits: FixedLiveLimits | None = None,
    entry_enabled=lambda: True,
    context_is_current=lambda context: True,
) -> LiveCandidateSubmission:
    from crypto_momentum_lab.live_rollout.entry_control import LiveEntryControlGate
    from crypto_momentum_lab.live_rollout.submission_admission import (
        LiveSubmissionAdmission,
    )

    class TestGate(LiveEntryControlGate):
        @property
        def entry_enabled(self):
            return entry_enabled()

    gate = TestGate(run_id="run-1", state_machine=state_machine)
    state_machine.configure_submission(
        repository, admission=LiveSubmissionAdmission(gate, context_is_current),
        clock=lambda: NOW,
    )
    return LiveCandidateSubmission(
        risk_gateway=RiskGateway(),
        limits=limits
        or FixedLiveLimits(
            notional_cap=Decimal("25"),
            max_open_positions=1,
            max_daily_loss=Decimal("10"),
            max_gross_exposure=Decimal("25"),
        ),
        state_machine=cast(CoordinatedOrderExecutionPort, state_machine),
        config=LiveSubmissionConfig(
            run_id="run-1",
            account_label="account-1",
            resize_tolerance=Decimal("0.20"),
            hedge_mode=False,
            entry_order_type=EntryType.MARKET,
            entry_limit_ttl_seconds=900,
        ),
        clock=lambda: NOW,
        entry_enabled=entry_enabled,
        entry_enabled_reason=lambda: "ready",
        context_is_current=context_is_current,
        pending_entry_reservation=lambda orders: (
            Decimal("0"),
            frozenset(),
        ),
        remember_pending_entry=lambda plan, result: None,
        record_signal_candidate=lambda **kwargs: None,
    )


async def test_submission_prepares_with_fencing_before_coordinator_exchange() -> None:
    repository = RecordingPreparedRepository()
    coordinator = RecordingCoordinator()
    submission = _submission(
        repository=repository,
        state_machine=coordinator,
    )
    context = _runtime_context()
    candidate = replace(_intent(), desired_notional=Decimal("20"))

    result = await submission.execute(
        candidate,
        requested_quantity=None,
        state=_state(),
        context=context,
    )

    assert result is not None
    assert result.state is ExchangeOrderState.ACKNOWLEDGED
    assert coordinator.events == ["prepare", "exchange"]
    assert repository.saved_intents == 0
    assert len(repository.prepare_calls) == 1
    call = repository.prepare_calls[0]
    assert call["environment"] == "live"
    assert call["account_label"] == "primary"
    assert call["strategy_name"] == "compression_breakout"
    assert call["required_lease_owner"] == "live-worker"
    assert call["required_lease_id"] == "lease-1"
    assert call["required_code_generation"] == "abc123"
    assert call["required_session_id"] == "run-1"


async def test_submission_preserves_policy_quantized_quantity() -> None:
    repository = RecordingPreparedRepository()
    coordinator = RecordingCoordinator()
    submission = _submission(
        repository=repository,
        state_machine=coordinator,
    )
    context = _runtime_context()
    rules = dict(context.trading_rules)
    rules["ETHUSDT"] = SymbolTradingRules(
        symbol="ETHUSDT",
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.01"),
        min_quantity=Decimal("0.01"),
        max_quantity=Decimal("100"),
        min_notional=Decimal("5"),
    )
    context = replace(context, trading_rules=rules)
    candidate = replace(
        _intent(),
        symbol="ETHUSDT",
        desired_notional=Decimal("20"),
        features={"position_side": "BOTH", "quantized_quantity": "0.02"},
    )
    reference_price = Decimal("1111.11")
    state = replace(
        _state(),
        symbol="ETHUSDT",
        mark_price=reference_price,
        close_price=reference_price,
    )

    result = await submission.execute(
        candidate,
        requested_quantity=None,
        state=state,
        context=context,
        reference_price=reference_price,
    )

    assert result is not None
    assert result.plan.quantity == Decimal("0.02")
    assert result.plan.quantity * reference_price == Decimal("22.2222")


async def test_submission_strictly_obeys_requested_quantity() -> None:
    repository = RecordingPreparedRepository()
    coordinator = RecordingCoordinator()
    submission = _submission(
        repository=repository,
        state_machine=coordinator,
    )
    # Total position on BTCUSDT is 0.0007 BTC.
    # Caller requests 0.0004 BTC.
    # Submission layer must NEVER silently absorb dust or inflate requested quantities!
    # Sizing/dust absorption decisions belong strictly to ExitAllocator.
    pos = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        quantity=Decimal("0.0007"),
        entry_price=Decimal("10000"),
        opened_at=NOW,
    )
    candidate = replace(
        _intent(),
        reduce_only=True,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        features={"position_side": "BOTH"},
    )
    context = replace(
        _runtime_context(),
        managed_positions=(pos,),
        open_position_symbols=frozenset({"BTCUSDT"}),
    )
    state = replace(
        _state(),
        symbol="BTCUSDT",
        mark_price=Decimal("10000"),
        close_price=Decimal("10000"),
    )

    result = await submission.execute(
        candidate,
        requested_quantity=Decimal("0.0004"),
        state=state,
        context=context,
        reference_price=Decimal("10000"),
    )

    assert result is not None
    assert result.plan is not None
    assert result.plan.quantity == Decimal("0.0004")


async def test_submission_does_not_absorb_dust_when_multiple_batches_exist() -> None:
    repository = RecordingPreparedRepository()
    coordinator = RecordingCoordinator()
    submission = _submission(
        repository=repository,
        state_machine=coordinator,
    )
    # Total position on BTCUSDT is 0.0007 BTC with two distinct batches:
    # Batch A (older): 0.0004 BTC
    # Batch B (newer): 0.0003 BTC
    # Exit batch tries to close Batch A (0.0004 BTC).
    # Dust remainder is 0.0003 BTC ($3.00 < $5 min_notional).
    # Because Batch B exists and is active, dust absorption MUST NOT absorb Batch B!
    # Plan quantity must strictly remain 0.0004 BTC.
    batch_a = ManagedLivePositionBatch(
        batch_id="batch-a",
        quantity=Decimal("0.0004"),
        entry_price=Decimal("10000"),
        opened_at=NOW - timedelta(minutes=60),
    )
    batch_b = ManagedLivePositionBatch(
        batch_id="batch-b",
        quantity=Decimal("0.0003"),
        entry_price=Decimal("10000"),
        opened_at=NOW - timedelta(minutes=1),
    )
    pos = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        quantity=Decimal("0.0007"),
        entry_price=Decimal("10000"),
        opened_at=NOW - timedelta(minutes=60),
        batches=(batch_a, batch_b),
    )
    candidate = replace(
        _intent(),
        reduce_only=True,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        features={"position_side": "BOTH", "batch_id": "batch-a"},
    )
    context = replace(
        _runtime_context(),
        managed_positions=(pos,),
        open_position_symbols=frozenset({"BTCUSDT"}),
    )
    state = replace(
        _state(),
        symbol="BTCUSDT",
        mark_price=Decimal("10000"),
        close_price=Decimal("10000"),
    )

    result = await submission.execute(
        candidate,
        requested_quantity=Decimal("0.0004"),
        state=state,
        context=context,
        reference_price=Decimal("10000"),
    )

    assert result is not None
    assert result.plan is not None
    # Must NOT absorb Batch B (0.0003) - plan quantity must strictly be 0.0004!
    assert result.plan.quantity == Decimal("0.0004")


async def test_submission_shadow_trade_command_evaluation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = RecordingPreparedRepository()
    coordinator = RecordingCoordinator()
    submission = _submission(
        repository=repository,
        state_machine=coordinator,
    )
    candidate = replace(
        _intent(),
        reduce_only=False,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        features={"position_side": "BOTH"},
    )
    context = _runtime_context()
    state = _state()

    shadow_calls: list[object] = []
    from crypto_momentum_lab.execution_account.orders.trade_command_executor import (
        TradeCommandExecutor,
    )

    original_plan = TradeCommandExecutor.plan_execution

    def fake_plan(*args: object, **kwargs: object) -> object:
        res = original_plan(*args, **kwargs)
        shadow_calls.append(res)
        return res

    monkeypatch.setattr(TradeCommandExecutor, "plan_execution", staticmethod(fake_plan))

    result = await submission.execute(
        candidate,
        requested_quantity=Decimal("0.001"),
        state=state,
        context=context,
        reference_price=Decimal("10000"),
    )

    assert result is not None
    assert len(shadow_calls) == 1
    assert shadow_calls[0].plan is not None
    assert shadow_calls[0].plan.quantity == Decimal("0.001")


async def test_submission_enforces_max_concurrency_per_symbol_per_batch() -> None:
    repository = RecordingPreparedRepository()
    coordinator = RecordingCoordinator()
    limits = FixedLiveLimits(
        notional_cap=Decimal("100"),
        max_open_positions=5,
        max_daily_loss=Decimal("50"),
        max_gross_exposure=Decimal("100"),
        max_concurrency_per_symbol=2,
    )
    submission = _submission(
        repository=repository,
        state_machine=coordinator,
        limits=limits,
    )
    candidate_btc = replace(
        _intent(),
        reduce_only=False,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        desired_notional=Decimal("10"),
        entry_type=EntryType.MARKET,
        features={"position_side": "BOTH"},
    )
    rules = dict(_runtime_context().trading_rules)
    rules["ETHUSDT"] = SymbolTradingRules(
        symbol="ETHUSDT",
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.001"),
        min_quantity=Decimal("0.001"),
        max_quantity=Decimal("100"),
        min_notional=Decimal("5"),
    )
    state = replace(
        _state(),
        symbol="BTCUSDT",
        mark_price=Decimal("10000"),
        close_price=Decimal("10000"),
    )

    # 1. No open position -> 1st order allowed
    context0 = replace(_runtime_context(), trading_rules=rules)
    res1 = await submission.execute(
        candidate_btc,
        requested_quantity=Decimal("0.001"),
        state=state,
        context=context0,
        reference_price=Decimal("10000"),
    )
    assert res1 is not None

    # 2. Position has 1 batch with entry_order_count = 1 -> 2nd order allowed
    b1_active = ManagedLivePositionBatch(
        batch_id="btc-b1",
        quantity=Decimal("0.001"),
        entry_price=Decimal("10000"),
        opened_at=NOW,
        entry_order_count=1,
    )
    pos_btc_1 = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        quantity=Decimal("0.001"),
        entry_price=Decimal("10000"),
        opened_at=NOW,
        batches=(b1_active,),
    )
    context1 = replace(context0, managed_positions=(pos_btc_1,))
    res2 = await submission.execute(
        candidate_btc,
        requested_quantity=Decimal("0.001"),
        state=state,
        context=context1,
        reference_price=Decimal("10000"),
    )
    assert res2 is not None

    # 3. Position has 1 batch with entry_order_count = 2 (max reached!)
    # -> 3rd order REJECTED!
    b1_full = ManagedLivePositionBatch(
        batch_id="btc-b1",
        quantity=Decimal("0.002"),
        entry_price=Decimal("10000"),
        opened_at=NOW,
        entry_order_count=2,
    )
    pos_btc_2 = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        quantity=Decimal("0.002"),
        entry_price=Decimal("10000"),
        opened_at=NOW,
        batches=(b1_full,),
    )
    context2 = replace(context0, managed_positions=(pos_btc_2,))
    res3 = await submission.execute(
        candidate_btc,
        requested_quantity=Decimal("0.001"),
        state=state,
        context=context2,
        reference_price=Decimal("10000"),
    )
    assert res3 is None  # Blocked!

    # 4. Batch 1 submits exit order (exit_order_submitted_at set)
    # -> batch ended, new batch 1st order ALLOWED!
    b1_exited = replace(b1_full, exit_order_submitted_at=NOW + timedelta(minutes=5))
    pos_btc_exited = replace(pos_btc_2, batches=(b1_exited,))
    context3 = replace(context0, managed_positions=(pos_btc_exited,))
    res4 = await submission.execute(
        candidate_btc,
        requested_quantity=Decimal("0.001"),
        state=state,
        context=context3,
        reference_price=Decimal("10000"),
    )
    assert res4 is not None  # Allowed for new batch!

    # 5. Different symbol (ETHUSDT) is NOT restricted by BTCUSDT
    candidate_eth = replace(
        _intent(),
        candidate_id="eth-1",
        reduce_only=False,
        symbol="ETHUSDT",
        side=StrategySide.LONG,
        desired_notional=Decimal("10"),
        entry_type=EntryType.MARKET,
        features={"position_side": "BOTH"},
    )
    state_eth = replace(
        _state(),
        symbol="ETHUSDT",
        mark_price=Decimal("2000"),
        close_price=Decimal("2000"),
    )
    # Even when BTCUSDT is at full concurrency (pos_btc_2), ETHUSDT can execute!
    res_eth = await submission.execute(
        candidate_eth,
        requested_quantity=Decimal("0.005"),
        state=state_eth,
        context=context2,
        reference_price=Decimal("2000"),
    )
    assert res_eth is not None


async def test_submission_entry_trade_command_carries_projection_version() -> None:
    repository = RecordingPreparedRepository()
    coordinator = RecordingCoordinator()
    submission = _submission(
        repository=repository,
        state_machine=coordinator,
    )
    candidate = replace(
        _intent(),
        reduce_only=False,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        features={
            "position_side": "BOTH",
            "projection_version": "pv_entry_token_123",
        },
    )
    command = submission._build_trade_command(
        candidate=candidate,
        reference_price=Decimal("100"),
        requested_quantity=Decimal("0.001"),
    )
    assert command is not None
    assert command.expected_projection_version == "pv_entry_token_123"


async def test_queued_entry_crossing_schedule_boundary_never_prepares_or_posts():
    from crypto_momentum_lab.live_rollout.entry_control import LiveEntryControlGate
    from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
        ScheduledRiskWindowConfig,
    )

    current = [datetime(2026, 7, 3, 23, 44, 59, tzinfo=UTC)]
    repository = RecordingPreparedRepository()
    gate = LiveEntryControlGate(run_id="run-1", state_machine=object(),
        scheduled_risk_window=ScheduledRiskWindowConfig(), clock=lambda: current[0])

    class BoundaryCoordinator(RecordingCoordinator):
        async def prepare_and_execute(self, plan, *, preparation):
            assert gate.entry_enabled
            current[0] += timedelta(seconds=1)
            return await super().prepare_and_execute(plan, preparation=preparation)

    submission = _submission(
        repository=repository, state_machine=BoundaryCoordinator(),
        entry_enabled=lambda: gate.entry_enabled,
    )
    result = await submission.execute(
        replace(_intent(), desired_notional=Decimal("20")), requested_quantity=None,
        state=_state(), context=_runtime_context())
    assert result is None
    assert not repository.prepare_calls
    assert repository.saved_intents == 0


async def test_queued_entry_with_invalidated_context_never_prepares_or_posts():
    repository = RecordingPreparedRepository()
    current = [True]
    posts = []

    class DelayedCoordinator(RecordingCoordinator):
        async def prepare_and_execute(self, plan, *, preparation):
            current[0] = False
            result = await super().prepare_and_execute(plan, preparation=preparation)
            if result is not None:
                posts.append(plan)
            return result

    submission = _submission(
        repository=repository, state_machine=DelayedCoordinator(),
        context_is_current=lambda context: current[0],
    )
    result = await submission.execute(
        replace(_intent(), desired_notional=Decimal("20")),
        requested_quantity=None,
        state=_state(),
        context=_runtime_context(),
    )
    assert result is None
    assert repository.prepare_calls == []
    assert posts == []
