import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.risk import RiskGateway
from crypto_momentum_lab.domain.risk.limits import FixedLiveLimits
from crypto_momentum_lab.domain.strategy import EntryType
from crypto_momentum_lab.live_rollout.entry_orders import LiveLimitOrderLifecycle
from crypto_momentum_lab.live_rollout.pending_entries import (
    LivePendingEntryRegistry,
    TerminalOrderBarrier,
)
from crypto_momentum_lab.live_rollout.position_lifecycle import PositionLifecycleLocks
from crypto_momentum_lab.live_rollout.submission import (
    LiveCandidateSubmission,
    LiveSubmissionConfig,
)
from tests.fixtures.live_market import _intent, _state
from tests.unit.live_rollout.test_daemon import _runtime_context

NOW = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)


async def test_limit_entry_is_cancelled_when_its_gtd_window_expires() -> None:
    cancelled = asyncio.Event()
    cancelled_plans: list[OrderExecutionPlan] = []

    async def cancel_order(plan: OrderExecutionPlan) -> OrderExecutionResult:
        cancelled_plans.append(plan)
        cancelled.set()
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            exchange_order_id="exchange-1",
        )

    lifecycle = LiveLimitOrderLifecycle(
        cancel_order=cancel_order,
        clock=lambda: NOW,
    )
    plan = _plan(expires_at=NOW + timedelta(milliseconds=10))

    await lifecycle.track(plan, _acknowledged(plan))
    await asyncio.wait_for(cancelled.wait(), timeout=1)
    await lifecycle.stop()

    assert cancelled_plans == [plan]


async def test_stopping_worker_does_not_cancel_live_entry_on_exchange() -> None:
    cancelled = False

    async def cancel_order(_plan: OrderExecutionPlan) -> OrderExecutionResult:
        nonlocal cancelled
        cancelled = True
        return OrderExecutionResult(
            client_order_id=_plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            exchange_order_id="exchange-1",
        )

    lifecycle = LiveLimitOrderLifecycle(
        cancel_order=cancel_order,
        clock=lambda: NOW,
    )
    plan = _plan(expires_at=NOW + timedelta(hours=1))

    await lifecycle.track(plan, _acknowledged(plan))
    await lifecycle.stop()

    assert cancelled is False


async def test_terminal_order_event_removes_expiry_timer() -> None:
    cancelled = False

    async def cancel_order(_plan: OrderExecutionPlan) -> OrderExecutionResult:
        nonlocal cancelled
        cancelled = True
        return OrderExecutionResult(
            client_order_id=_plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            exchange_order_id="exchange-1",
        )

    lifecycle = LiveLimitOrderLifecycle(
        cancel_order=cancel_order,
        clock=lambda: NOW,
    )
    plan = _plan(expires_at=NOW + timedelta(milliseconds=20))

    await lifecycle.track(plan, _acknowledged(plan))
    lifecycle.observe(
        plan,
        ExchangeOrderEvent(
            event_id="event-1",
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.FILLED,
            occurred_at=NOW,
            exchange_order_id="exchange-1",
            details={},
        ),
    )
    await asyncio.sleep(0.05)
    await lifecycle.stop()

    assert cancelled is False


def _limits() -> FixedLiveLimits:
    return FixedLiveLimits(
        notional_cap=Decimal("10000"),
        max_open_positions=5,
        max_gross_exposure=Decimal("50000"),
        max_daily_loss=Decimal("5000"),
    )


async def test_a2c_terminal_order_does_not_recreate_expiry_timer_on_late_ack() -> None:
    cancelled = False

    async def cancel_order(_plan: OrderExecutionPlan) -> OrderExecutionResult:
        nonlocal cancelled
        cancelled = True
        return OrderExecutionResult(
            client_order_id=_plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            exchange_order_id="exchange-1",
        )

    barrier = TerminalOrderBarrier()
    lifecycle = LiveLimitOrderLifecycle(
        cancel_order=cancel_order,
        clock=lambda: NOW,
        terminal_barrier=barrier,
    )
    registry = LivePendingEntryRegistry(
        clock=lambda: NOW,
        terminal_barrier=barrier,
    )

    rest_started = asyncio.Event()
    release_rest = asyncio.Event()
    submitted_plan: list[OrderExecutionPlan] = []

    class MockStateMachine:
        async def prepare_and_execute(
            self,
            plan: OrderExecutionPlan,
            *,
            preparation: Any,
        ) -> OrderExecutionResult:
            submitted_plan.append(plan)
            rest_started.set()
            await release_rest.wait()
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.ACKNOWLEDGED,
                exchange_order_id="exchange-1",
                executed_quantity=Decimal("0"),
                plan=plan,
            )

    config = LiveSubmissionConfig(
        run_id="run-1",
        account_label="primary",
        resize_tolerance=Decimal("0.20"),
        hedge_mode=False,
        entry_order_type=EntryType.LIMIT,
        entry_limit_ttl_seconds=900,
    )
    submission = LiveCandidateSubmission(
        risk_gateway=RiskGateway(
            limits=FixedLiveLimits(
                notional_cap=Decimal("25"),
                max_open_positions=5,
                max_daily_loss=Decimal("10"),
                max_gross_exposure=Decimal("500"),
                max_concurrency_per_symbol=2,
            )
        ),
        state_machine=MockStateMachine(),
        config=config,
        clock=lambda: NOW,
        pending_entry_reservation=registry.reservation,
        pending_entry_plans=registry.admission_snapshot,
        pending_entry_uncertainty=registry.has_uncertain_entry,
        remember_pending_entry=registry.remember,
        entry_order_lifecycle=lifecycle,
        record_signal_candidate=lambda *args, **kwargs: None,
        position_locks=PositionLifecycleLocks(),
    )

    context = replace(
        _runtime_context(),
        risk_config=replace(
            _runtime_context().risk_config, max_gross_notional=Decimal("500")
        ),
    )
    cand = replace(
        _intent(),
        candidate_id="cand-a2c",
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=60),
    )

    # 1. Start execution through real submission pipeline
    submit_task = asyncio.create_task(
        submission.execute(
            cand,
            requested_quantity=None,
            state=_state(),
            context=context,
        )
    )

    # 2. Wait until Mock REST is in-flight (timeout to prevent hung task)
    await asyncio.wait_for(rest_started.wait(), timeout=5.0)
    assert len(submitted_plan) == 1
    plan = submitted_plan[0]

    # 3. While REST is in-flight, a terminal event arrives from WS (e.g. CANCELED)
    ws_terminal_event = ExchangeOrderEvent(
        event_id="ws-event-term",
        client_order_id=plan.client_order_id,
        state=ExchangeOrderState.CANCELED,
        occurred_at=NOW,
        exchange_order_id="exchange-1",
        details={},
    )
    registry.observe_order_event(plan, ws_terminal_event)
    lifecycle.observe(plan, ws_terminal_event)

    # 4. Release Mock REST ACK
    release_rest.set()
    result = await asyncio.wait_for(submit_task, timeout=5.0)

    # 5. Assertions as specified by A2c:
    assert result is not None
    assert result.state is ExchangeOrderState.ACKNOWLEDGED

    # - Order is NOT re-added to pending / uncertain reservations
    assert len(registry.snapshot()) == 0
    assert not registry.has_uncertain_entry("BTCUSDT")
    notional, symbols = registry.reservation(())
    assert notional == Decimal("0")
    assert "BTCUSDT" not in symbols

    # - No GTD TTL expiry task was created in entry_order_lifecycle
    assert plan.client_order_id not in lifecycle._tasks

    # - No cancellation or duplicate cancel POST is triggered
    await asyncio.sleep(0.02)
    await lifecycle.stop()
    assert cancelled is False


async def test_late_rest_ack_after_barrier_eviction_does_not_resurrect_pending_or_timer(
) -> None:
    cancelled = False

    async def cancel_order(plan: OrderExecutionPlan) -> OrderExecutionResult:
        nonlocal cancelled
        cancelled = True
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            exchange_order_id="exchange-1",
        )

    durable_terminal_ids: set[str] = set()

    async def check_durable(cid: str) -> tuple[bool, Decimal] | None:
        if cid in durable_terminal_ids:
            return (True, Decimal("0"))
        return None

    # Barrier with capacity=1 so any prior entry is evicted when another order arrives
    barrier = TerminalOrderBarrier(
        capacity=1,
        durable_async_checker=check_durable,
    )

    lifecycle = LiveLimitOrderLifecycle(
        cancel_order=cancel_order,
        clock=lambda: NOW,
        terminal_barrier=barrier,
    )
    registry = LivePendingEntryRegistry(
        clock=lambda: NOW,
        terminal_barrier=barrier,
    )

    executed_plans: list[OrderExecutionPlan] = []

    class MockStateMachine:
        async def prepare_and_execute(
            self,
            plan: OrderExecutionPlan,
            *,
            preparation: Any,
        ) -> OrderExecutionResult:
            executed_plans.append(plan)
            # While REST was in flight, the order reached terminal in durable DB
            durable_terminal_ids.add(plan.client_order_id)
            # Another terminal order arrives, evicting the order from memory cache
            barrier.record_terminal("other-order-id", Decimal("0"))
            assert not barrier.is_terminal(plan.client_order_id)
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.ACKNOWLEDGED,
                exchange_order_id="exchange-1",
                executed_quantity=Decimal("0"),
                plan=plan,
            )

    config = LiveSubmissionConfig(
        run_id="run-1",
        account_label="primary",
        resize_tolerance=Decimal("0.20"),
        hedge_mode=False,
        entry_order_type=EntryType.LIMIT,
        entry_limit_ttl_seconds=900,
    )
    submission = LiveCandidateSubmission(
        risk_gateway=RiskGateway(
            limits=FixedLiveLimits(
                notional_cap=Decimal("25"),
                max_open_positions=5,
                max_daily_loss=Decimal("10"),
                max_gross_exposure=Decimal("500"),
                max_concurrency_per_symbol=2,
            )
        ),
        state_machine=MockStateMachine(),
        config=config,
        clock=lambda: NOW,
        pending_entry_reservation=registry.reservation,
        pending_entry_plans=registry.admission_snapshot,
        pending_entry_uncertainty=registry.has_uncertain_entry,
        remember_pending_entry=registry.remember,
        entry_order_lifecycle=lifecycle,
        terminal_barrier=barrier,
        record_signal_candidate=lambda *args, **kwargs: None,
        position_locks=PositionLifecycleLocks(),
    )

    context = replace(
        _runtime_context(),
        risk_config=replace(
            _runtime_context().risk_config, max_gross_notional=Decimal("500")
        ),
    )
    cand = replace(
        _intent(),
        candidate_id="cand-evicted",
        created_at=NOW,
        expires_at=NOW + timedelta(seconds=60),
    )

    # Late REST ACK executes:
    result = await submission.execute(
        cand,
        requested_quantity=None,
        state=_state(),
        context=context,
    )

    assert result is not None
    assert result.state is ExchangeOrderState.ACKNOWLEDGED

    # Invariant: Evicted terminal order checked durable storage and was not revived
    assert len(registry.snapshot()) == 0
    assert not registry.has_uncertain_entry("BTCUSDT")
    notional, symbols = registry.reservation(())
    assert notional == Decimal("0")
    assert "BTCUSDT" not in symbols

    # Invariant: No GTD TTL expiry timer created
    assert len(executed_plans) == 1
    assert executed_plans[0].client_order_id not in lifecycle._tasks

    await asyncio.sleep(0.02)
    await lifecycle.stop()
    assert cancelled is False


def _plan(*, expires_at: datetime) -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id="intent-1",
        run_id="run-1",
        client_order_id="client-1",
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=Decimal("0.001"),
        price=Decimal("30000"),
        reduce_only=False,
        created_at=NOW,
        quantized=True,
        time_in_force="GTD",
        expires_at=expires_at,
    )


def _acknowledged(plan: OrderExecutionPlan) -> OrderExecutionResult:
    return OrderExecutionResult(
        client_order_id=plan.client_order_id,
        state=ExchangeOrderState.ACKNOWLEDGED,
        exchange_order_id="exchange-1",
    )
