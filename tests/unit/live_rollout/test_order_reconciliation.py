import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.hub import AccountEvent
from crypto_momentum_lab.live_rollout.order_reconciliation import (
    LiveOrderReconciliation,
)


@pytest.mark.asyncio
async def test_incomplete_account_event_marks_only_matching_order_uncertain() -> None:
    plans = [
        SimpleNamespace(client_order_id="entry-1", run_id="run-1"),
        SimpleNamespace(client_order_id="entry-2", run_id="run-1"),
    ]
    reconciled: list[object] = []

    class Repository:
        async def load_order(self, client_order_id: str):
            return next(
                SimpleNamespace(plan=plan, state=ExchangeOrderState.ACKNOWLEDGED)
                for plan in plans
                if plan.client_order_id == client_order_id
            )

    class StateMachine:
        async def mark_reconciliation_pending(self, plan) -> None:
            reconciled.append(plan)

    reconciliation = LiveOrderReconciliation(
        order_repository=Repository(),  # type: ignore[arg-type]
        state_machine=StateMachine(),  # type: ignore[arg-type]
        run_id="run-1",
    )

    await reconciliation.reconcile_account_event(
        SimpleNamespace(
            client_order_id="entry-2",
            order_update=None,
            exchange_event_at=None,
            event_at=datetime(2026, 10, 1, tzinfo=UTC),
        )  # type: ignore[arg-type]
    )

    assert reconciled == [plans[1]]


@pytest.mark.asyncio
async def test_incomplete_account_event_marks_persisted_order_uncertain() -> None:
    plan = SimpleNamespace(client_order_id="entry-1", run_id="run-1")
    reconciled: list[object] = []

    class Repository:
        async def load_unresolved_orders(self, _run_id: str):
            return ()

        async def load_order(self, client_order_id: str):
            assert client_order_id == "entry-1"
            return SimpleNamespace(
                plan=plan,
                state=ExchangeOrderState.ACKNOWLEDGED,
            )

    class StateMachine:
        async def mark_reconciliation_pending(self, reconciled_plan) -> None:
            reconciled.append(reconciled_plan)

    reconciliation = LiveOrderReconciliation(
        order_repository=Repository(),  # type: ignore[arg-type]
        state_machine=StateMachine(),  # type: ignore[arg-type]
        run_id="run-1",
    )

    await reconciliation.reconcile_account_event(
        SimpleNamespace(
            client_order_id="entry-1",
            order_update=None,
            exchange_event_at=None,
            event_at=datetime(2026, 10, 1, tzinfo=UTC),
        )  # type: ignore[arg-type]
    )

    assert reconciled == [plan]


@pytest.mark.asyncio
async def test_account_event_missing_from_local_journal_requests_snapshot_recovery() -> (
    None
):
    recovery_reasons: list[str] = []

    class Repository:
        async def load_unresolved_orders(self, _run_id: str):
            return ()

        async def load_order(self, _client_order_id: str):
            return None

    reconciliation = LiveOrderReconciliation(
        order_repository=Repository(),  # type: ignore[arg-type]
        state_machine=object(),  # type: ignore[arg-type]
        run_id="run-1",
        on_unknown_order=recovery_reasons.append,
    )

    await reconciliation.reconcile_account_event(
        SimpleNamespace(client_order_id="unknown-1")  # type: ignore[arg-type]
    )

    assert recovery_reasons == ["account_event_order_missing_from_local_journal"]


@pytest.mark.asyncio
async def test_reconcile_all_reconciles_every_unresolved_order() -> None:
    plans = [
        SimpleNamespace(client_order_id="entry-1", run_id="run-1"),
        SimpleNamespace(client_order_id="entry-2", run_id="run-1"),
    ]
    reconciled: list[object] = []

    class Repository:
        async def load_unresolved_orders(self, run_id: str):
            return tuple(SimpleNamespace(plan=plan) for plan in plans)

    class StateMachine:
        async def reconcile_order(self, plan) -> None:
            reconciled.append(plan)

    reconciliation = LiveOrderReconciliation(
        order_repository=Repository(),  # type: ignore[arg-type]
        state_machine=StateMachine(),  # type: ignore[arg-type]
        run_id="run-1",
    )

    await reconciliation.reconcile_all()

    assert reconciled == plans


@pytest.mark.asyncio
async def test_periodic_reconcile_is_cancelled_between_attempts() -> None:
    scanned = asyncio.Event()

    class Repository:
        async def load_unresolved_orders(self, run_id: str):
            scanned.set()
            return ()

    reconciliation = LiveOrderReconciliation(
        order_repository=Repository(),
        state_machine=object(),
        run_id="run-1",
        interval_seconds=0.01,
    )
    worker = asyncio.create_task(reconciliation.run_periodically())
    try:
        await asyncio.wait_for(scanned.wait(), timeout=1)
    finally:
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker


def _ws_order_case():
    now = datetime(2026, 10, 1, tzinfo=UTC)
    plan = OrderExecutionPlan(
        intent_id="intent",
        run_id="run-1",
        client_order_id="entry-1",
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("2"),
        price=None,
        reduce_only=False,
        created_at=now,
        quantized=True,
    )
    order = PersistedExchangeOrder(plan, ExchangeOrderState.ACKNOWLEDGED, "123", now)
    fact = {
        "c": "entry-1",
        "s": "BTCUSDT",
        "S": "BUY",
        "o": "MARKET",
        "q": "2",
        "p": "0",
        "R": False,
        "ps": "BOTH",
        "i": 123,
        "X": "FILLED",
        "z": "2",
        "ap": "100",
        "T": int((now + timedelta(seconds=1)).timestamp() * 1000),
    }
    event = AccountEvent(
        environment="live",
        account_label="primary",
        event_type="ORDER_TRADE_UPDATE",
        event_id="trade",
        event_at=now,
        received_at=now,
        client_order_id="entry-1",
        order_update=fact,
    )
    return order, event


@pytest.mark.asyncio
@pytest.mark.parametrize("order_type", ["MARKET", "market"])
@pytest.mark.parametrize(
    "status,quantity",
    [
        ("NEW", "0"),
        ("PARTIALLY_FILLED", "1"),
        ("FILLED", "2"),
        ("CANCELED", "1"),
        ("EXPIRED", "0"),
    ],
)
async def test_complete_ws_order_uses_existing_state_machine_without_rest(
    status, quantity, order_type
):
    order, event = _ws_order_case()
    order = replace(order, plan=replace(order.plan, order_type=order_type))
    event.order_update.update(X=status, z=quantity)
    applied = []

    class Repository:
        async def load_order(self, client_order_id):
            assert client_order_id == order.plan.client_order_id
            return order

    class StateMachine:
        async def reconcile_order(self, _plan):
            pytest.fail("complete WS order must not query REST")

        async def apply_observed_snapshot(self, plan, snapshot):
            applied.append((plan, snapshot))

    runtime = LiveOrderReconciliation(Repository(), StateMachine(), "run-1")
    await runtime.reconcile_account_event(event)
    assert len(applied) == 1
    assert applied[0][0] == order.plan
    assert applied[0][1].executed_quantity == Decimal(quantity)
    assert applied[0][1].average_price == Decimal("100")
    assert applied[0][1].exchange_order_id == "123"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("s", "ETHUSDT"),
        ("c", "other"),
        ("S", "SELL"),
        ("o", "LIMIT"),
        ("ps", "SHORT"),
        ("R", True),
        ("q", "3"),
        ("i", 999),
    ],
)
async def test_ws_identity_conflict_cannot_apply_or_fall_back_to_rest(field, value):
    order, event = _ws_order_case()
    event.order_update[field] = value

    class Repository:
        async def load_order(self, _client_order_id):
            return order

    runtime = LiveOrderReconciliation(Repository(), object(), "run-1")
    with pytest.raises(ValueError, match="conflicts with its durable identity"):
        await runtime.reconcile_account_event(event)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("z", "NaN"),
        ("ap", "0"),
        ("z", "3"),
        ("z", "1"),
        ("T", True),
        ("i", -1),
        ("X", "unsupported"),
        ("R", "false"),
        ("ap", "Infinity"),
    ],
)
async def test_incomplete_ws_fact_marks_uncertainty_without_inline_rest(field, value):
    order, event = _ws_order_case()
    event.order_update[field] = value
    reconciled = []

    class Repository:
        async def load_order(self, _client_order_id):
            return order

    class StateMachine:
        async def mark_reconciliation_pending(self, plan):
            reconciled.append(plan)

    runtime = LiveOrderReconciliation(Repository(), StateMachine(), "run-1")
    await runtime.reconcile_account_event(event)
    assert reconciled == [order.plan]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["terminal", "lower_quantity", "older_same_quantity"])
async def test_ws_replay_does_not_rollback_durable_order(case):
    order, event = _ws_order_case()
    if case == "terminal":
        order = replace(
            order, state=ExchangeOrderState.FILLED, executed_quantity=Decimal("2")
        )
    elif case == "lower_quantity":
        order = replace(order, executed_quantity=Decimal("1.5"))
        event.order_update.update(X="PARTIALLY_FILLED", z="1")
    else:
        order = replace(
            order,
            updated_at=event.event_at + timedelta(seconds=2),
            executed_quantity=Decimal("2"),
        )

    class Repository:
        async def load_order(self, _client_order_id):
            return order

    # No state-machine method may be called for a replay.
    runtime = LiveOrderReconciliation(Repository(), object(), "run-1")
    await runtime.reconcile_account_event(event)


@pytest.mark.asyncio
async def test_same_millisecond_ws_partial_fills_and_replay_use_durable_idempotence():
    from crypto_momentum_lab.execution_account.orders.state_machine import (
        OrderExecutionStateMachine,
        SubmitPolicy,
    )

    order, event = _ws_order_case()
    inserted = {}
    notified = []

    class Repository:
        async def load_order(self, _client_order_id):
            return order

        async def append_order_event(self, observation):
            if observation.event_id in inserted:
                return False
            inserted[observation.event_id] = observation
            return True

    async def on_event(_plan, observation):
        notified.append(observation)

    repository = Repository()
    # No exchange interface is available: this path must use only WS facts.
    machine = OrderExecutionStateMachine(
        exchange=object(),
        repository=repository,
        event_repository=repository,
        submit_policy=SubmitPolicy.LIVE_SUBMIT,
        live_submit_enabled=True,
        on_event=on_event,
    )
    runtime = LiveOrderReconciliation(repository, machine, "run-1")
    for quantity in ("0.5", "1", "1", "2"):
        status = "FILLED" if quantity == "2" else "PARTIALLY_FILLED"
        fact = dict(event.order_update, X=status, z=quantity)
        await runtime.reconcile_account_event(replace(event, order_update=fact))
    assert [item.details["executed_quantity"] for item in notified] == ["0.5", "1", "2"]
    assert len(inserted) == 3


@pytest.mark.asyncio
async def test_terminal_cancel_does_not_hide_late_additional_ws_fill():
    order, event = _ws_order_case()
    order = replace(
        order, state=ExchangeOrderState.CANCELED, executed_quantity=Decimal("1")
    )
    applied = []

    class Repository:
        async def load_order(self, _client_order_id):
            return order

    class StateMachine:
        async def apply_observed_snapshot(self, plan, snapshot):
            applied.append(snapshot)

    runtime = LiveOrderReconciliation(Repository(), StateMachine(), "run-1")
    await runtime.reconcile_account_event(event)
    assert applied[0].state is ExchangeOrderState.FILLED
    assert applied[0].executed_quantity == Decimal("2")


@pytest.mark.asyncio
async def test_incomplete_event_wakes_existing_worker_and_does_not_block_next_fact():
    order, event = _ws_order_case()
    uncertain = asyncio.Event()
    rest_started = asyncio.Event()
    release_rest = asyncio.Event()
    applied = []
    scans = 0
    second_scan = asyncio.Event()

    class Repository:
        async def load_order(self, _client_order_id):
            return order

        async def load_unresolved_orders(self, _run_id):
            nonlocal scans
            scans += 1
            if scans == 2:
                second_scan.set()
            return (order,)

    class StateMachine:
        async def mark_reconciliation_pending(self, plan):
            assert plan == order.plan
            uncertain.set()

        async def reconcile_order(self, _plan):
            rest_started.set()
            await release_rest.wait()

        async def apply_observed_snapshot(self, _plan, snapshot):
            applied.append(snapshot)

    reconciliation = LiveOrderReconciliation(
        Repository(), StateMachine(), "run-1", interval_seconds=3600
    )
    worker = asyncio.create_task(reconciliation.run_periodically())
    try:
        await asyncio.wait_for(
            reconciliation.reconcile_account_event(replace(event, order_update=None)),
            timeout=1,
        )
        assert uncertain.is_set()
        await asyncio.wait_for(rest_started.wait(), timeout=1)
        # A complete event continues even while the existing REST worker is waiting.
        await asyncio.wait_for(reconciliation.reconcile_account_event(event), timeout=1)
        assert len(applied) == 1
        # A new request arriving during REST must not disappear when it finishes.
        await reconciliation.reconcile_account_event(replace(event, order_update=None))
        release_rest.set()
        await asyncio.wait_for(second_scan.wait(), timeout=1)
    finally:
        release_rest.set()
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker


@pytest.mark.asyncio
async def test_incomplete_earlier_run_order_is_repaired_by_existing_worker():
    order, event = _ws_order_case()
    order = replace(order, plan=replace(order.plan, run_id="earlier-run"))
    repaired = []
    scopes = []

    class Repository:
        async def load_order(self, _client_order_id):
            return order

        async def load_unresolved_orders(self, run_id):
            scopes.append(run_id)
            return (order,) if run_id == "earlier-run" else ()

    class StateMachine:
        async def mark_reconciliation_pending(self, _plan):
            pass

        async def reconcile_order(self, plan):
            repaired.append(plan)

    reconciliation = LiveOrderReconciliation(Repository(), StateMachine(), "run-1")
    await reconciliation.reconcile_account_event(replace(event, order_update=None))
    await reconciliation.reconcile_all()
    assert repaired == [order.plan]
    assert set(scopes) == {"earlier-run", "run-1"}
