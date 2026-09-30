from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.execution import ExchangeOrderState, OrderExecutionPlan
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError,
)
from crypto_momentum_lab.domain.risk import RiskDecision, RiskEvaluation
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.persistence.postgres.order_plan_repository import (
    PostgresOrderPlanRepository,
)
from crypto_momentum_lab.persistence.postgres.order_submission_repository import (
    PostgresOrderSubmissionRepository,
)

NOW = datetime(2026, 9, 30, tzinfo=UTC)


def database(scalar_values):
    transaction = AsyncMock()
    session = AsyncMock()
    session.__aenter__.return_value = session
    session.begin = Mock(return_value=transaction)
    session.scalar.side_effect = scalar_values
    factory = Mock(return_value=session)
    return PostgresOrderSubmissionRepository(factory), session, transaction, factory


async def prepare(repository, **kwargs):
    intent = _intent()
    return await repository.prepare_submission(
        intent=intent,
        evaluation=_evaluation(intent, "approved"),
        plan=_plan(),
        prepared_at=NOW,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_submission_uses_one_transaction_for_intent_order_and_event():
    repository, session, transaction, factory = database([_plan().client_order_id])
    result = await prepare(repository)
    assert result.plan == _plan()
    assert result.submitting_event.state is ExchangeOrderState.SUBMITTING
    factory.assert_called_once_with()
    session.begin.assert_called_once_with()
    tables = [call.args[0].table.name for call in session.execute.await_args_list]
    assert tables == [
        "order_intents",
        "exchange_order_events",
        "order_intents",
    ]
    assert session.scalar.await_args.args[0].table.name == "exchange_orders"
    transaction.__aexit__.assert_awaited_once_with(None, None, None)
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("identity_conflict", [False, True])
async def test_existing_order_rolls_back_pending_intent(identity_conflict):
    plan = _plan()
    existing = SimpleNamespace(
        intent_id=plan.intent_id,
        run_id=plan.run_id,
        symbol=plan.symbol,
        side=plan.side,
        order_type=plan.order_type,
        quantity=plan.quantity + (Decimal("1") if identity_conflict else Decimal("0")),
        price=plan.price,
        time_in_force=plan.time_in_force,
        expires_at=plan.expires_at,
        reduce_only=plan.reduce_only,
        position_side=plan.position_side.value,
        created_at=plan.created_at,
        state=ExchangeOrderState.FILLED.value,
    )
    repository, session, transaction, _ = database([None, existing])
    if identity_conflict:
        with pytest.raises(ValueError, match="different order"):
            await prepare(repository)
    else:
        assert await prepare(repository) is None
    assert transaction.__aexit__.await_args.args[0] is not None
    assert session.execute.await_count == 1


@pytest.mark.asyncio
async def test_event_write_failure_propagates_to_transaction_exit():
    repository, session, transaction, _ = database([_plan().client_order_id])
    failure = RuntimeError("event write failed")
    session.execute.side_effect = [None, failure]
    with pytest.raises(RuntimeError, match="event write failed"):
        await prepare(repository)
    assert transaction.__aexit__.await_args.args[1] is failure
    assert session.execute.await_count == 2


@pytest.mark.asyncio
async def test_missing_lease_blocks_before_intent_or_order_write():
    repository, session, transaction, _ = database([None])
    with pytest.raises(OrderPreSubmissionError, match="fencing"):
        await prepare(
            repository,
            environment="live",
            account_label="account-3",
            strategy_name="compression_breakout",
            required_lease_owner="worker",
            required_lease_id="lease",
            required_code_generation="version",
        )
    session.execute.assert_not_awaited()
    assert session.scalar.await_args.args[0]._for_update_arg is not None
    assert transaction.__aexit__.await_args.args[0] is OrderPreSubmissionError


@pytest.mark.asyncio
@pytest.mark.parametrize("won", [False, True])
async def test_claim_updates_intent_only_for_winning_worker(won):
    repository, session, transaction, _ = database(["candidate-1" if won else None])
    assert (
        await repository.claim_intent(
            "candidate-1",
            "worker",
            NOW,
            NOW + timedelta(minutes=1),
        )
        is won
    )
    assert session.execute.await_count == (2 if won else 1)
    transaction.__aexit__.assert_awaited_once_with(None, None, None)


@pytest.mark.asyncio
async def test_rejected_risk_evaluation_does_not_open_transaction():
    repository, _, _, factory = database([])
    intent = _intent()
    rejected = replace(_evaluation(intent, "rejected"), decision=RiskDecision.REJECTED)
    with pytest.raises(ValueError, match="approve"):
        await repository.save_approved_intent(intent, rejected)
    factory.assert_not_called()


def test_order_repository_no_longer_owns_submission_capabilities():
    for method in ("prepare_submission", "claim_intent", "save_approved_intent"):
        assert not hasattr(PostgresOrderPlanRepository, method)


def _evaluation(
    intent: OrderIntentCandidate,
    evaluation_id: str,
) -> RiskEvaluation:
    return RiskEvaluation(
        evaluation_id=evaluation_id,
        candidate_id=intent.candidate_id,
        decision=RiskDecision.APPROVED,
        reason="approved",
        evaluated_at=NOW,
        details={},
    )


def _intent() -> OrderIntentCandidate:
    return OrderIntentCandidate(
        candidate_id="candidate-1",
        signal_id="signal-1",
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v1",
        config_hash="a" * 64,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("100"),
        reduce_only=False,
        expires_at=NOW + timedelta(seconds=30),
        created_at=NOW,
        reason="test",
        features={},
    )


def _plan() -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id="candidate-1",
        run_id="run-1",
        client_order_id="cml_12345678901234567890123456789012",
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("0.001"),
        price=None,
        reduce_only=False,
        created_at=NOW,
        quantized=True,
    )
