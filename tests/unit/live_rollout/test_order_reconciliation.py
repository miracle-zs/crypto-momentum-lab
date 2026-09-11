import asyncio
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.live_rollout.order_reconciliation import (
    LiveOrderReconciliation,
)


@pytest.mark.asyncio
async def test_account_event_reconciles_only_the_matching_unresolved_order() -> None:
    plans = [
        SimpleNamespace(client_order_id="entry-1"),
        SimpleNamespace(client_order_id="entry-2"),
    ]
    reconciled: list[object] = []

    class Repository:
        async def load_unresolved_orders(self, run_id: str):
            assert run_id == "run-1"
            return tuple(SimpleNamespace(plan=plan) for plan in plans)

    class StateMachine:
        async def reconcile_order(self, plan) -> None:
            reconciled.append(plan)

    reconciliation = LiveOrderReconciliation(
        order_repository=Repository(),  # type: ignore[arg-type]
        state_machine=StateMachine(),  # type: ignore[arg-type]
        run_id="run-1",
    )

    await reconciliation.reconcile_account_event(
        SimpleNamespace(client_order_id="entry-2")  # type: ignore[arg-type]
    )

    assert reconciled == [plans[1]]


@pytest.mark.asyncio
async def test_reconcile_all_reconciles_every_unresolved_order() -> None:
    plans = [
        SimpleNamespace(client_order_id="entry-1"),
        SimpleNamespace(client_order_id="entry-2"),
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
    calls = 0
    delays: list[float] = []

    class Repository:
        async def load_unresolved_orders(self, run_id: str):
            nonlocal calls
            calls += 1
            return ()

    async def controlled_sleep(delay: float) -> None:
        delays.append(delay)
        if len(delays) == 2:
            raise asyncio.CancelledError

    reconciliation = LiveOrderReconciliation(
        order_repository=Repository(),  # type: ignore[arg-type]
        state_machine=object(),  # type: ignore[arg-type]
        run_id="run-1",
        interval_seconds=60,
    )

    with pytest.raises(asyncio.CancelledError):
        await reconciliation.run_periodically(sleep=controlled_sleep)

    assert calls == 1
    assert delays == [60, 60]
