"""Reconcile restored commands even when the order read model is terminal."""

from collections.abc import Awaitable, Callable

from crypto_momentum_lab.domain.execution.command_models import DispatchState
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.order_read_repository import (
    OrderReadRepository,
)
from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)

OrderRecoveryLookup = Callable[[OrderExecutionPlan], Awaitable[object]]


async def recover_restored_commands(
    *,
    book: ExecutionBook,
    coordinator: OrderExecutionCoordinator,
    orders: OrderReadRepository,
    reconcile_order: OrderRecoveryLookup,
) -> bool:
    pending = False
    for entry in book.list_outbox():
        requires_recovery = book.command_requires_recovery(entry.command_id)
        if (
            entry.state in {DispatchState.TERMINAL, DispatchState.REJECTED}
            and not requires_recovery
        ):
            continue
        persisted = await orders.load_order(entry.command_id)
        if persisted is not None:
            if persisted.terminal_receipt is not None:
                await coordinator.observe_recovered_receipt(
                    persisted.plan, persisted.terminal_receipt
                )
            elif requires_recovery:
                # Missing priced facts require exchange lookup, never a new submit.
                await reconcile_order(persisted.plan)
        pending = book.command_requires_recovery(entry.command_id) or pending
    return pending
