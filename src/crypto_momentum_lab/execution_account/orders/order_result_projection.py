"""Project exchange receipts into durable execution evidence."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from decimal import Decimal

import structlog

from crypto_momentum_lab.domain.account.models import AccountFillEvent
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionCumulativeOrderReport,
    ExecutionEvidence,
)
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
    EvidenceConflict,
    WaitingForEvidence,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.reservation_registry import (
    ExecutionReadinessError,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)

log = structlog.get_logger(__name__)


async def project_order_result(
    coordinator: object,
    plan: OrderExecutionPlan,
    res: OrderExecutionResult | None,
    *,
    settlement_fills: tuple[AccountFillEvent, ...] = (),
) -> None:
    if res is None:
        return
    scope = ExecutionScope(
        environment=coordinator._environment,
        account_label=coordinator._account_label,
        symbol=plan.symbol,
        position_side=plan.position_side,
    )
    now_dt = datetime.now(UTC)
    cumulative_quantity = res.executed_quantity
    if cumulative_quantity < Decimal("0"):
        raise ValueError("exchange cumulative executed quantity cannot be negative")
    average_price = res.average_price
    if cumulative_quantity > Decimal("0") and average_price <= Decimal("0"):
        if res.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
            # Quantity is retained by the durable pending order event, but
            # cannot settle a reservation until its quote is authoritative.
            if coordinator._execution_book.get_outbox(res.client_order_id) is not None:
                await coordinator._execution_book.mark_unknown(
                    res.client_order_id,
                    reason="cumulative_fill_price_pending",
                )
            return
        raise RuntimeError(
            "positive cumulative fill has no positive cumulative average price; "
            "execution facts require recovery"
        )
    cumulative_quote = cumulative_quantity * average_price
    position_side = plan.position_side.value
    identity = "\x1f".join(
        (
            coordinator._account_label,
            plan.symbol,
            position_side,
            res.client_order_id,
            str(res.exchange_order_id or ""),
            res.state.value,
            str(cumulative_quantity),
            str(cumulative_quote),
        )
    )
    if settlement_fills:
        from crypto_momentum_lab.domain.execution.evidence_digest import (
            trade_payload_digest,
        )

        identity += "\x1f" + "\x1f".join(
            trade_payload_digest(fill)
            for fill in sorted(settlement_fills, key=lambda item: item.trade_id)
        )
    identity_hash = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    order_ev = ExchangeOrderEvent(
        event_id=f"order_{identity_hash}",
        client_order_id=res.client_order_id,
        state=res.state,
        occurred_at=now_dt,
        exchange_order_id=res.exchange_order_id,
        details={
            "account_label": coordinator._account_label,
            "symbol": plan.symbol,
            **({"side": plan.side} if settlement_fills else {}),
            "executed_quantity": str(cumulative_quantity),
            "cumulative_quote_quantity": str(cumulative_quote),
            "average_price": str(average_price) if average_price is not None else None,
            "limit_price": str(plan.price) if plan.price is not None else None,
            "is_reduce_only": plan.reduce_only,
            "position_side": position_side,
        },
    )
    try:
        stream_id: str | None = None
        stream_epoch: str | None = None
        if coordinator._execution_book.has_execution_unit_of_work:
            current_view = await coordinator._execution_book.read(scope)
            stream_scope = current_view.stream_scope
            if stream_scope is not None:
                stream_id = stream_scope.stream_id
                stream_epoch = stream_scope.stream_epoch
            else:
                active = coordinator._execution_book.get_active_stream(
                    coordinator._environment, coordinator._account_label
                )
                if active is not None:
                    stream_id, stream_epoch = active
                elif coordinator._active_stream is not None:
                    stream_id, stream_epoch = coordinator._active_stream
                else:
                    raise RuntimeError(
                        "cumulative order report has no restored Book stream identity "
                        "and no active stream scope is registered"
                    )
        result = await coordinator._execution_book.observe(
            ExecutionEvidence(
                evidence_id=order_ev.event_id,
                scope=scope,
                observed_at=now_dt,
                order_event=order_ev,
                stream_id=stream_id,
                stream_epoch=stream_epoch,
                cumulative_order=ExecutionCumulativeOrderReport(
                    order_id=res.client_order_id,
                    cumulative_quantity=cumulative_quantity,
                    cumulative_quote=cumulative_quote,
                    observed_at=now_dt,
                ),
                settlement_fills=settlement_fills,
            )
        )
        if isinstance(result, WaitingForEvidence):
            if res.state.terminal:
                coordinator._execution_book.require_command_recovery(
                    res.client_order_id
                )
                log.warning(
                    "order_terminal_evidence_waiting_for_recovery",
                    client_order_id=res.client_order_id,
                    state=res.state.value,
                    reason=result.reason.value,
                )
                return
            raise ExecutionReadinessError(
                "cumulative order evidence is waiting for recovery: "
                + result.reason.value
            )
        if isinstance(result, EvidenceConflict):
            raise RuntimeError(
                "cumulative order evidence was rejected: " + result.reason
            )
    except Exception as observe_err:
        if coordinator._execution_book.get_outbox(res.client_order_id) is not None:
            try:
                await coordinator._execution_book.mark_unknown(
                    res.client_order_id,
                    reason=(f"exchange result could not be persisted: {observe_err}"),
                )
            except Exception as transition_err:
                raise RuntimeError(
                    "exchange returned a result, fact persistence failed, and "
                    "the UNKNOWN outbox transition also failed: "
                    f"{transition_err}"
                ) from transition_err
        raise
    if isinstance(result, Applied) and result.recovery_required:
        coordinator._execution_book.require_command_recovery(res.client_order_id)
        log.warning(
            "order_facts_applied_reservation_recovery_required",
            client_order_id=res.client_order_id,
            diagnostics=result.diagnostics,
        )
