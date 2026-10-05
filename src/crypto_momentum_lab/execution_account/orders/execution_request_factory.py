"""Pure conversion from an order plan to the execution-book command request."""

from __future__ import annotations

from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.execution_action_models import (
    ExecutionRequest,
)
from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitPolicyMode,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import StrategySide


def build_execution_request(
    plan: OrderExecutionPlan,
    *,
    environment: str,
    account_label: str,
    strategy_name: str,
) -> ExecutionRequest:
    """Compile one validated execution plan into a deterministic Book request."""
    if (
        plan.reduce_only
        and plan.batch_id
        and (
            str(plan.batch_id).startswith(f"batch_{plan.symbol}_")
            or str(plan.batch_id) in ("batch_default", "batch_synthetic")
        )
    ):
        raise OrderPreSubmissionError(
            f"Account {account_label}: synthetic batch {plan.batch_id} is prohibited"
        )
    projection_version = plan.projection_version
    if not projection_version or not projection_version.strip():
        kind = "exit" if plan.reduce_only else "entry"
        raise OrderPreSubmissionError(
            f"{kind} {plan.client_order_id} has no Book projection token"
        )
    scope = ExecutionScope(
        environment=environment,
        account_label=account_label,
        symbol=plan.symbol,
        position_side=plan.position_side,
    )
    opening_buy = (plan.side == "BUY") != plan.reduce_only
    side = StrategySide.LONG if opening_buy else StrategySide.SHORT
    if not plan.reduce_only:
        return ExecutionRequest(
            request_id=plan.client_order_id,
            scope=scope,
            strategy_name=strategy_name,
            run_id=plan.run_id,
            decision_ref=plan.client_order_id,
            expected_view_token=projection_version,
            action=TradeCommandType.ENTRY,
            requested_quantity=plan.quantity,
            side=side,
            order_type=plan.order_type,
            limit_price=plan.price,
            created_at=plan.created_at,
        )
    allocations = plan.allocations
    if allocations:
        target_batch_ids = tuple(item.batch_id for item in allocations)
        batch_quantities = {
            item.batch_id: item.allocated_quantity for item in allocations
        }
    elif plan.batch_id:
        target_batch_ids = (str(plan.batch_id),)
        batch_quantities = {str(plan.batch_id): plan.quantity}
    else:
        raise OrderPreSubmissionError(
            f"Exit order {plan.client_order_id} has no allocated batches or batch_id"
        )
    return ExecutionRequest(
        request_id=plan.client_order_id,
        scope=scope,
        strategy_name=strategy_name,
        run_id=plan.run_id,
        decision_ref=plan.client_order_id,
        expected_view_token=projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=plan.quantity,
        side=side,
        order_type=plan.order_type,
        limit_price=plan.price,
        reduce_only=True,
        target_batch_ids=target_batch_ids,
        batch_quantities=batch_quantities,
        exit_policy_mode=ExitPolicyMode.TARGET_BATCHES_ONLY,
        created_at=plan.created_at,
    )
