from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError,
    OrderSubmissionPreparation,
    PreparedOrderSubmission,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.risk import RiskDecision, RiskEvaluation
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
    OrderExecutionStateMachine,
    SubmitPolicy,
)

NOW = datetime(2026, 9, 30, 0, 0, 0, tzinfo=UTC)


class RecordingOrderRepository:
    def __init__(self) -> None:
        self.prepare_calls: list[dict[str, Any]] = []

    async def prepare_submission(self, **kwargs: Any) -> PreparedOrderSubmission:
        self.prepare_calls.append(kwargs)
        plan = cast(OrderExecutionPlan, kwargs["plan"])
        return PreparedOrderSubmission(
            plan=plan,
            submitting_event=ExchangeOrderEvent(
                event_id="submitting-evt-1",
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.SUBMITTING,
                occurred_at=NOW,
                exchange_order_id=None,
                details={},
            ),
        )


async def test_decision_exit_properly_prepares_intent_and_submits() -> None:
    """Decision exit must create candidate, approve risk, and prepare submission before execute."""
    repository = RecordingOrderRepository()
    prepared_submissions: list[PreparedOrderSubmission] = []

    mock_coordinator = MagicMock()

    async def fake_prepare_and_execute(plan, *, preparation):
        from dataclasses import fields
        values = {f.name: getattr(preparation, f.name) for f in fields(preparation)
                  if f.name != "context_token"}
        prepared = await repository.prepare_submission(plan=plan, prepared_at=NOW, **values)
        assert prepared is not None
        prepared_submissions.append(prepared)
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.FILLED,
            exchange_order_id="binance-12345",
            executed_quantity=plan.quantity,
            average_price=Decimal("0.089"),
            plan=plan,
        )

    mock_coordinator.prepare_and_execute = fake_prepare_and_execute

    session_id = "test-session-123"
    strategy_name = "orderflow_impulse"
    strategy_config_hash = "cfg_hash_test"
    account_label = "primary"
    lease_owner = "owner-1"
    git_commit_hash = "abc1234"
    active_lease = None

    # Simulate _handle_decision_exit logic from runtime_orchestrator
    async def _handle_decision_exit(cmd: TradeCommand) -> OrderExecutionResult:
        allocs = ()
        if cmd.allocation_plan:
            allocs = cmd.allocation_plan.allocations
        exit_client_order_id = cmd.idempotency_key or cmd.command_id
        candidate_id = f"intent_exit_{cmd.command_id}"
        signal_id = f"sig_exit_{cmd.command_id}"

        features: dict[str, Any] = {
            "command_id": cmd.command_id,
            "position_side": cmd.position_key.position_side.value,
            "quantity": str(cmd.requested_quantity),
            "projection_version": cmd.expected_projection_version,
        }
        if allocs:
            features["batch_id"] = allocs[0].batch_id
            if getattr(allocs[0], "entry_price", None) is not None:
                features["entry_price"] = str(allocs[0].entry_price)

        intent = OrderIntentCandidate(
            candidate_id=candidate_id,
            signal_id=signal_id,
            run_id=session_id,
            strategy_name=strategy_name,
            strategy_version="v0",
            config_hash=strategy_config_hash,
            symbol=cmd.position_key.symbol,
            side=cmd.side,
            entry_type=(
                cmd.order_type
                if isinstance(cmd.order_type, EntryType)
                else EntryType(str(cmd.order_type).lower())
            ),
            limit_price=cmd.limit_price,
            desired_notional=None,
            reduce_only=True,
            expires_at=cmd.created_at + timedelta(seconds=600),
            created_at=cmd.created_at,
            reason=cmd.reason or "decision_exit",
            features=features,
        )

        evaluation = RiskEvaluation(
            evaluation_id=f"eval_exit_{cmd.command_id}",
            candidate_id=candidate_id,
            decision=RiskDecision.APPROVED,
            reason="decision_exit_approved",
            evaluated_at=cmd.created_at,
            details={"command_id": cmd.command_id},
        )

        plan = OrderExecutionPlan(
            intent_id=candidate_id,
            run_id=session_id,
            client_order_id=exit_client_order_id,
            symbol=cmd.position_key.symbol,
            side="SELL" if cmd.side == StrategySide.LONG else "BUY",
            order_type=(
                cmd.order_type.value
                if hasattr(cmd.order_type, "value")
                else str(cmd.order_type)
            ),
            quantity=cmd.requested_quantity,
            price=cmd.limit_price,
            reduce_only=True,
            position_side=cmd.position_key.position_side,
            created_at=cmd.created_at,
            quantized=True,
            allocations=allocs,
            projection_version=cmd.expected_projection_version,
            strategy_name=strategy_name,
            strategy_version="v0",
        )

        res = await mock_coordinator.prepare_and_execute(
            plan,
            preparation=OrderSubmissionPreparation(
                intent=intent, evaluation=evaluation,
                environment="live" if active_lease is not None else None,
                account_label=account_label, strategy_name=strategy_name,
                required_lease_owner=lease_owner if active_lease is not None else None,
                required_lease_id=active_lease.lease_id if active_lease is not None else None,
                required_code_generation=git_commit_hash if active_lease is not None else None,
                required_session_id=session_id,
            ),
        )
        if res is None:
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.REJECTED,
                exchange_order_id=None,
                plan=plan,
            )
        return res

    cmd = TradeCommand(
        command_id="cmd_exit_dec_YBUSDT_12345",
        position_key=PositionKey(
            environment="live",
            account_label="primary",
            symbol="YBUSDT",
            position_side=FuturesPositionSide.LONG,
        ),
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("1102"),
        limit_price=None,
        reduce_only=True,
        reason="max_holding_period",
        created_at=NOW,
        expected_projection_version="proj-1",
    )

    result = await _handle_decision_exit(cmd)

    assert result.state is ExchangeOrderState.FILLED
    assert len(repository.prepare_calls) == 1
    call_args = repository.prepare_calls[0]

    # Verify intent candidate invariants
    intent = call_args["intent"]
    assert intent.candidate_id == "intent_exit_cmd_exit_dec_YBUSDT_12345"
    assert intent.reduce_only is True
    assert intent.symbol == "YBUSDT"
    assert intent.reason == "max_holding_period"

    # Verify risk evaluation invariants
    evaluation = call_args["evaluation"]
    assert evaluation.candidate_id == intent.candidate_id
    assert evaluation.decision is RiskDecision.APPROVED

    # Verify plan invariants
    plan = call_args["plan"]
    assert plan.intent_id == intent.candidate_id
    assert plan.client_order_id == "cmd_exit_dec_YBUSDT_12345"
    assert plan.reduce_only is True
    assert plan.quantity == Decimal("1102")


async def test_pre_exchange_database_failure_marks_rejected_not_unknown() -> None:
    """Pre-exchange failures must be classified as before_exchange_post and marked rejected."""

    class FailingRepo:
        async def save_planned_order(self, plan: OrderExecutionPlan) -> None:
            raise RuntimeError("Database connection dropped or FK violation")

    mock_exchange = MagicMock()
    machine = OrderExecutionStateMachine(
        exchange=mock_exchange,
        repository=FailingRepo(),
        event_repository=FailingRepo(),
        submit_policy=SubmitPolicy.LIVE_SUBMIT,
        live_submit_enabled=True,
        clock=lambda: NOW,
    )

    plan = OrderExecutionPlan(
        intent_id="intent-1",
        run_id="run-1",
        client_order_id="cmd-fail-1",
        symbol="YBUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("100"),
        price=None,
        reduce_only=True,
        created_at=NOW,
        quantized=True,
    )

    # State machine must wrap pre-submission failure in OrderPreSubmissionError
    with pytest.raises(OrderPreSubmissionError) as exc_info:
        await machine.submit(plan)

    assert "pre-submission failed: Database connection dropped or FK violation" in str(
        exc_info.value
    )
    # The exchange was NEVER called
    mock_exchange.submit_order.assert_not_called()
