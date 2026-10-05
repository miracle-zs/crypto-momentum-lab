"""Reconcile restored commands even when the order read model is terminal."""

from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
from crypto_momentum_lab.domain.execution.ports import (
    OrderReadRepository,
)
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.domain.execution.order_execution_port import (
    CoordinatedOrderExecutionPort,
)


def _synthesize_order_plan_from_outbox(entry: OutboxEntry) -> OrderExecutionPlan:
    opening_buy = entry.command.side is StrategySide.LONG
    should_buy = not opening_buy if entry.command.reduce_only else opening_buy
    exchange_side = "BUY" if should_buy else "SELL"
    return OrderExecutionPlan(
        intent_id=entry.command.command_id,
        run_id="recovered_command",
        client_order_id=entry.command_id,
        symbol=entry.scope.symbol,
        side=exchange_side,
        order_type=entry.command.order_type.value.upper(),
        quantity=entry.command.requested_quantity,
        price=entry.command.limit_price,
        reduce_only=entry.command.reduce_only,
        created_at=entry.created_at,
        position_side=entry.scope.position_side,
        quantized=True,
    )


async def recover_restored_commands(
    *,
    book: ExecutionBook,
    coordinator: CoordinatedOrderExecutionPort,
    orders: OrderReadRepository,
) -> tuple[bool, tuple[OrderExecutionPlan, ...]]:
    """Apply durable receipts and return uncertainty for the worker to query."""
    pending = False
    plans: list[OrderExecutionPlan] = []
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
            elif persisted.state.terminal or requires_recovery:
                # Missing priced facts require exchange lookup, never a new submit.
                plans.append(persisted.plan)
        else:
            # Command exists in outbox but has no persisted order record.
            if entry.state == DispatchState.PREPARED and not entry.attempt_count:
                await book.mark_rejected(
                    entry.command_id,
                    reason="prepared_unsubmitted_before_restart",
                )
            elif (
                entry.state in {DispatchState.UNKNOWN, DispatchState.DISPATCHING}
                or requires_recovery
                or entry.attempt_count > 0
            ):
                plan = _synthesize_order_plan_from_outbox(entry)
                plans.append(plan)
        pending = book.command_requires_recovery(entry.command_id) or pending
    return pending, tuple(plans)
