from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock

from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderSubmissionPreparation,
)
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.entry_control import LiveEntryControlGate
from crypto_momentum_lab.live_rollout.submission_admission import (
    LiveSubmissionAdmission,
)

NOW = datetime(2026, 9, 12, 12, 0, tzinfo=UTC)


def _make_plan(
    symbol: str = "BTCUSDT",
    reduce_only: bool = False,
) -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id=f"intent_{symbol}",
        run_id="run-1",
        client_order_id=f"cid_{symbol}",
        symbol=symbol,
        side="BUY",
        order_type="LIMIT",
        quantity=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=reduce_only,
        created_at=NOW,
        quantized=True,
    )


def test_submission_admission_allows_reduce_only_when_symbol_blocked() -> None:
    gate = LiveEntryControlGate(run_id="run-1", state_machine=object())
    gate.set_exit_failure("BTCUSDT", "network_timeout")

    admission = LiveSubmissionAdmission(gate, context_is_current=lambda ctx: True)

    plan = _make_plan("BTCUSDT", reduce_only=True)
    prep = Mock(spec=OrderSubmissionPreparation)
    prep.context_token = None

    reason = admission.rejection_reason(plan, prep)
    assert reason is None


def test_submission_admission_enforces_per_symbol_gate() -> None:
    gate = LiveEntryControlGate(run_id="run-1", state_machine=object())
    gate.set_exit_failure("BTCUSDT", "api_error")

    admission = LiveSubmissionAdmission(gate, context_is_current=lambda ctx: True)

    # BTCUSDT entry is rejected
    btc_plan = _make_plan("BTCUSDT", reduce_only=False)
    btc_prep = Mock(spec=OrderSubmissionPreparation)
    btc_prep.context_token = None
    expected_btc_reason = "exit_failure:BTCUSDT:api_error"
    assert admission.rejection_reason(btc_plan, btc_prep) == expected_btc_reason

    # ETHUSDT entry is admitted
    eth_plan = _make_plan("ETHUSDT", reduce_only=False)
    eth_prep = Mock(spec=OrderSubmissionPreparation)
    eth_prep.context_token = None
    assert admission.rejection_reason(eth_plan, eth_prep) is None


def test_submission_admission_rejects_invalidated_context() -> None:
    gate = LiveEntryControlGate(run_id="run-1", state_machine=object())
    admission = LiveSubmissionAdmission(gate, context_is_current=lambda ctx: False)

    plan = _make_plan("ETHUSDT", reduce_only=False)
    prep = Mock(spec=OrderSubmissionPreparation)
    prep.context_token = Mock(spec=LiveDaemonRuntimeContext)

    assert admission.rejection_reason(plan, prep) == "submission_context_invalidated"
