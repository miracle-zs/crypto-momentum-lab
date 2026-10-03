import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.live_rollout import execution_runtime as runtime


def arguments():
    callback = AsyncMock()
    return dict(
        sessions=async_sessionmaker(),
        exchange=Mock(),
        order_repository=Mock(),
        event_repository=Mock(),
        account_label="account-3",
        strategy_name="momentum",
        callbacks=runtime.LiveExecutionCallbacks(
            on_event=callback,
            on_before_submit=callback,
            on_before_exchange_submit=callback,
            on_exchange_request=callback,
            on_exchange_response=callback,
        ),
    )


@pytest.mark.asyncio
async def test_recovery_completes_before_submission_coordinator_exists(monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    restored_accounts = []

    async def restore(book, *, account_label):
        restored_accounts.append(account_label)
        entered.set()
        await release.wait()

    monkeypatch.setattr(runtime.ExecutionBook, "restore", restore)
    constructor = Mock(wraps=runtime.OrderExecutionCoordinator)
    monkeypatch.setattr(runtime, "OrderExecutionCoordinator", constructor)
    args = arguments()
    task = asyncio.create_task(runtime.build_live_execution_runtime(**args))
    await asyncio.wait_for(entered.wait(), timeout=1)
    try:
        constructor.assert_not_called()
        assert not task.done()
    finally:
        release.set()
    result = await task
    try:
        assert restored_accounts == ["account-3"]
        assert result.coordinator._execution_book is result.book
        call = constructor.call_args.kwargs
        assert call["environment"] == "live"
        assert call["domain_coordinator"] is result.book.coordinator
        backend = call["backend"]
        assert backend._submit_policy is runtime.SubmitPolicy.LIVE_SUBMIT
        assert backend._live_submit_enabled is True
        assert backend._lock is None
        assert backend._clock().utcoffset().total_seconds() == 0
        assert backend._exchange is args["exchange"]
        assert backend._repository is args["order_repository"]
        assert backend._event_repository is args["event_repository"]
        assert (
            backend._on_before_exchange_submit
            is args["callbacks"].on_before_exchange_submit
        )
        assert backend._on_event is args["callbacks"].on_event
        assert backend._on_before_submit is args["callbacks"].on_before_submit
        assert backend._on_exchange_request is args["callbacks"].on_exchange_request
        assert backend._on_exchange_response is args["callbacks"].on_exchange_response
        uow = result.book._execution_unit_of_work
        assert uow._session_factory is args["sessions"]
        assert uow._command_repository is result.book._command_repo
        assert uow._reservation_repository is call["reservation_repository"]
    finally:
        await result.coordinator.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [RuntimeError("recovery failed"), asyncio.CancelledError()]
)
async def test_failed_or_cancelled_restore_never_exposes_submission(monkeypatch, error):
    monkeypatch.setattr(runtime.ExecutionBook, "restore", AsyncMock(side_effect=error))
    constructor = Mock()
    monkeypatch.setattr(runtime, "OrderExecutionCoordinator", constructor)
    args = arguments()
    with pytest.raises(type(error)):
        await runtime.build_live_execution_runtime(**args)
    constructor.assert_not_called()
    args["exchange"].assert_not_called()
    assert not args["exchange"].method_calls


def test_compile_live_runtime_plan_applies_overrides() -> None:
    from decimal import Decimal
    from unittest.mock import Mock

    from crypto_momentum_lab.domain.risk import RiskConfigSnapshot, TradingLease
    from crypto_momentum_lab.live_rollout.runtime_config import LiveRuntimeConfig

    config = Mock(spec=LiveRuntimeConfig)
    config.strategy = Mock()
    config.strategy.market_orders = False

    risk_cfg = Mock(spec=RiskConfigSnapshot)
    risk_cfg.max_open_positions = 5
    risk_cfg.max_gross_notional = Decimal("50000")
    risk_cfg.max_account_drawdown = "0.05"
    risk_cfg.max_order_notional = Decimal("10000")

    lease = Mock(spec=TradingLease)
    lease.fencing_token = 42

    plan = runtime.compile_live_runtime_plan(
        config=config,
        target_notional=Decimal("10000"),
        risk_config=risk_cfg,
        account_label="primary",
        strategy_name="orderflow_impulse",
        git_commit_hash="abc1234",
        migration_revision="20261003_0001",
        active_lease=lease,
    )

    assert plan.account_label == "primary"
    assert plan.fencing_epoch == 42
    assert plan.effective_policy.target_notional == Decimal("10000")
    assert plan.effective_policy.order_type == "limit"
    assert plan.effective_policy.max_open_positions == 5

    # Exercise production approval evidence with the real compiled plan.
    from datetime import UTC, datetime
    from types import SimpleNamespace
    from crypto_momentum_lab.live_rollout.execution_runtime import LIVE_APPROVAL_CONFIRMATION

    now = datetime.now(UTC)
    approval = SimpleNamespace(
        approval_text=LIVE_APPROVAL_CONFIRMATION,
        expires_at=None,
        account_label="primary",
        strategy_name="orderflow_impulse",
    )
    provider = runtime.build_capability_evidence_provider(
        account_label="primary",
        runtime_plan=plan,
        get_active_lease=lambda: None,
        is_entry_enabled=lambda: True,
        get_context=lambda: None,
        get_market_age=lambda: 0.5,
        has_api_key=lambda: True,
        get_approval=lambda: approval,
    )
    assert provider(SimpleNamespace(symbol="BTCUSDT", client_order_id="cid"), now).is_approval_valid
    approval.strategy_name = "different_strategy"
    assert not provider(SimpleNamespace(symbol="BTCUSDT", client_order_id="cid"), now).is_approval_valid


def test_capability_evidence_provider_evaluates_runtime_facts() -> None:
    from datetime import UTC, datetime, timedelta
    from unittest.mock import Mock

    from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
    from crypto_momentum_lab.domain.risk import TradingLease
    from crypto_momentum_lab.domain.runtime import RuntimePlan

    mock_plan = Mock(spec=RuntimePlan)
    mock_plan.plan_hash = "hash123"
    mock_plan.runtime_generation = "gen1"
    mock_plan.fencing_epoch = 1
    mock_plan.declared_schema_compatibility = "v1"
    mock_plan.observed_database_revision = "rev1"

    now = datetime.now(tz=UTC)
    active_lease = Mock(spec=TradingLease)
    active_lease.expires_at = now + timedelta(seconds=60)

    ctx = Mock()
    ctx.unmanaged_position_symbols = ()
    ctx.unresolved_orders = ()

    provider = runtime.build_capability_evidence_provider(
        account_label="acc1",
        runtime_plan=mock_plan,
        get_active_lease=lambda: active_lease,
        is_entry_enabled=lambda: True,
        get_context=lambda: ctx,
        get_market_age=lambda: 2.5,
        has_api_key=lambda: True,
    )

    order_plan = Mock(spec=OrderExecutionPlan)
    order_plan.symbol = "BTCUSDT"
    order_plan.client_order_id = "cid1"

    evidence = provider(order_plan, now)

    assert evidence.market_freshness_seconds == 2.5
    assert evidence.is_account_concordant is True
    assert evidence.is_account_identity_verified is True
    assert evidence.is_lease_active is True
    assert evidence.is_approval_valid is True
    assert evidence.unresolved_inflight_orders_count == 0


def test_capability_evidence_provider_missing_facts_fail_closed() -> None:
    from datetime import UTC, datetime, timedelta
    from unittest.mock import Mock

    from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
    from crypto_momentum_lab.domain.risk import TradingLease
    from crypto_momentum_lab.domain.runtime import RuntimePlan

    mock_plan = Mock(spec=RuntimePlan)
    mock_plan.plan_hash = "hash123"
    mock_plan.runtime_generation = "gen1"
    mock_plan.fencing_epoch = 1
    mock_plan.declared_schema_compatibility = "v1"
    mock_plan.observed_database_revision = "rev1"
    mock_plan.strategy_name = "test_strat"

    now = datetime.now(tz=UTC)
    active_lease = Mock(spec=TradingLease)
    active_lease.expires_at = now + timedelta(seconds=60)

    # Missing market age -> float("inf")
    # Missing approval -> False
    # None context -> discordant
    provider_missing = runtime.build_capability_evidence_provider(
        account_label="acc1",
        runtime_plan=mock_plan,
        get_active_lease=lambda: active_lease,
        is_entry_enabled=lambda: False,
        get_context=lambda: None,
        get_market_age=lambda: None,
        has_api_key=lambda: True,
    )
    order_plan = Mock(spec=OrderExecutionPlan)
    order_plan.symbol = "BTCUSDT"
    order_plan.client_order_id = "cid1"

    ev1 = provider_missing(order_plan, now)
    assert ev1.market_freshness_seconds == float("inf")
    assert ev1.is_account_concordant is False
    assert ev1.is_approval_valid is False
    assert ev1.unresolved_inflight_orders_count == 1

    # Context with pending position sync on order symbol -> discordant
    ctx_pending = Mock()
    ctx_pending.pending_position_symbols = {"BTCUSDT"}
    ctx_pending.unmanaged_position_symbols = ()
    ctx_pending.unresolved_orders = ()

    provider_pending = runtime.build_capability_evidence_provider(
        account_label="acc1",
        runtime_plan=mock_plan,
        get_active_lease=lambda: active_lease,
        is_entry_enabled=lambda: True,
        get_context=lambda: ctx_pending,
        get_market_age=lambda: 1.0,
        has_api_key=lambda: True,
    )
    ev2 = provider_pending(order_plan, now)
    assert ev2.is_account_concordant is False

    # But ETHUSDT on same context is concordant
    order_plan_eth = Mock(spec=OrderExecutionPlan)
    order_plan_eth.symbol = "ETHUSDT"
    order_plan_eth.client_order_id = "cid2"
    ev3 = provider_pending(order_plan_eth, now)
    assert ev3.is_account_concordant is True
