from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionPort,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
    PreparedOrderSubmission,
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
from tests.unit.shadow_operation.test_service import _intent, _state

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

    async def prepare_and_execute(self, plan, *, prepare_submission):
        self.events.append("prepare")
        prepared = await prepare_submission()
        assert prepared is not None
        self.events.append("exchange")
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="exchange-1",
            plan=plan,
        )


class LegacyStateMachine:
    def __init__(self) -> None:
        self.events: list[str] = []

    async def execute_approved_intent(self, plan, *, prepared_submission=None):
        del prepared_submission
        self.events.append("exchange")
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="exchange-1",
            plan=plan,
        )


def _submission(*, repository, state_machine) -> LiveCandidateSubmission:
    return LiveCandidateSubmission(
        risk_gateway=RiskGateway(),
        limits=FixedLiveLimits(
            notional_cap=Decimal("25"),
            max_open_positions=1,
            max_daily_loss=Decimal("10"),
            max_gross_exposure=Decimal("25"),
        ),
        repository=repository,
        state_machine=cast(OrderExecutionPort, state_machine),
        config=LiveSubmissionConfig(
            run_id="run-1",
            resize_tolerance=Decimal("0.20"),
            hedge_mode=False,
            entry_order_type=EntryType.MARKET,
            entry_limit_ttl_seconds=900,
        ),
        clock=lambda: NOW,
        entry_enabled=lambda: True,
        entry_enabled_reason=lambda: "ready",
        context_is_current=lambda context: True,
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


async def test_submission_keeps_legacy_save_then_exchange_fallback() -> None:
    class LegacyRepository:
        def __init__(self) -> None:
            self.events: list[str] = []

        async def save_approved_intent(self, intent, evaluation) -> None:
            del intent, evaluation
            self.events.append("save")

    repository = LegacyRepository()
    state_machine = LegacyStateMachine()
    submission = _submission(
        repository=repository,
        state_machine=state_machine,
    )

    result = await submission.execute(
        replace(_intent(), desired_notional=Decimal("20")),
        requested_quantity=None,
        state=_state(),
        context=_runtime_context(),
    )

    assert result is not None
    assert repository.events == ["save"]
    assert state_machine.events == ["exchange"]


async def test_submission_strictly_obeys_requested_quantity_without_implicit_dust_expansion() -> None:
    repository = RecordingPreparedRepository()
    coordinator = RecordingCoordinator()
    submission = _submission(
        repository=repository,
        state_machine=coordinator,
    )
    # Total position on BTCUSDT is 0.0007 BTC.
    # Caller requests 0.0004 BTC.
    # Submission layer must NEVER silently absorb dust or inflate requested quantities!
    # Sizing/dust absorption decisions belong strictly to ExitAllocator at the decision layer.
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


