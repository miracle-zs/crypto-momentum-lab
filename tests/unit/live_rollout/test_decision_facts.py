"""Live decision facts come from account context, never invented."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountOpenOrderSnapshot,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionHealthStatus,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import StrategyLiveState
from crypto_momentum_lab.execution_account.sync import AccountSnapshot
from crypto_momentum_lab.live_rollout.decision_facts import (
    LiveDecisionFactSource,
    frozen_decision_inputs_from_context,
)


def _state() -> MarketState15s:
    start = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
    return MarketState15s(
        schema_version=1,
        exchange="binance",
        environment="live",
        symbol="BTCUSDT",
        bucket_start=start,
        bucket_end=datetime(2026, 9, 25, 8, 0, 15, tzinfo=UTC),
        open_price=Decimal("100"),
        high_price=Decimal("101"),
        low_price=Decimal("99"),
        close_price=Decimal("100"),
        trade_count=1,
        trade_notional=Decimal("1"),
        aggressive_buy_notional=Decimal("1"),
        aggressive_sell_notional=Decimal("0"),
        last_bid_price=Decimal("100"),
        last_ask_price=Decimal("100"),
        spread=Decimal("0"),
        midpoint=Decimal("100"),
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=Decimal("100"),
        closed_kline_count=0,
        source_event_count=1,
        first_received_at=start,
        last_received_at=start,
    )


class _Risk:
    created_at = datetime(2026, 9, 25, tzinfo=UTC)


class _Ctx:
    def __init__(self, **kwargs: object) -> None:
        self.account_snapshot = kwargs.get("account_snapshot")
        self.account_state = kwargs.get("account_state")
        self.account_observed_at = kwargs.get("account_observed_at")
        self.managed_positions = kwargs.get("managed_positions", ())
        self.pending_position_symbols = kwargs.get(
            "pending_position_symbols", frozenset()
        )
        self.unmanaged_position_symbols = kwargs.get(
            "unmanaged_position_symbols", frozenset()
        )
        self.unresolved_orders = kwargs.get("unresolved_orders", ())
        self.strategy_state = kwargs.get("strategy_state", StrategyLiveState.ACTIVE)
        self.active_halts = kwargs.get("active_halts", ())
        self.risk_config = _Risk()
        self.context_epoch = kwargs.get("context_epoch", 1)
        self.account_snapshot_version = kwargs.get("account_snapshot_version", 7)
        self.coverage_by_symbol = kwargs.get("coverage_by_symbol", {})


def _snapshot(cash: str = "1000") -> AccountSnapshot:
    observed_at = datetime(2026, 9, 25, 8, 0, 15, tzinfo=UTC)
    bal = AccountBalanceSnapshot(
        environment="live",
        account_label="primary",
        asset="USDT",
        wallet_balance=Decimal(cash),
        available_balance=Decimal(cash),
        unrealized_pnl=Decimal("0"),
        observed_at=observed_at,
        raw_payload={},
    )
    return AccountSnapshot(
        config=AccountConfigSnapshot(
            environment="live",
            account_label="primary",
            multi_assets_mode=False,
            hedge_mode=False,
            fee_tier=None,
            observed_at=observed_at,
            raw_payload={},
        ),
        balances=(bal,),
        positions=(),
        open_orders=(),
    )


def test_missing_snapshot_yields_none() -> None:
    out = frozen_decision_inputs_from_context(
        _Ctx(account_snapshot=None),
        _state(),
        account_label="primary",
    )
    assert out is None


def test_real_cash_and_versions_are_used() -> None:
    out = frozen_decision_inputs_from_context(
        _Ctx(account_snapshot=_snapshot("250.5")),
        _state(),
        account_label="primary",
    )
    assert out is not None
    assert out.cash_balance == Decimal("250.5")
    # Without proven coverage the facts must not claim READY.
    assert out.position_view.health_status == PositionHealthStatus.CATCHING_UP
    assert "primary" in out.position_view.projection_version
    assert out.position_view.projection_version.endswith("_7")


def test_current_flat_account_snapshot_confirms_live_zero_position() -> None:
    snapshot = _snapshot()
    context = _Ctx(
        account_snapshot=snapshot,
        account_state=ExecutionAccountStatus.READY_READONLY,
        account_observed_at=snapshot.config.observed_at,
    )

    out = frozen_decision_inputs_from_context(
        context,
        _state(),
        account_label="primary",
    )

    assert out is not None
    assert out.position_view.health_status == PositionHealthStatus.READY
    assert out.position_view.zero_position_snapshot_confirmed is True
    assert out.position_view.is_ready_for_trade is True


def test_open_order_prevents_zero_position_snapshot_confirmation() -> None:
    snapshot = _snapshot()
    open_order = AccountOpenOrderSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        order_id="order_1",
        client_order_id="client_order_1",
        side="BUY",
        order_type="LIMIT",
        status="NEW",
        price=Decimal("100"),
        original_quantity=Decimal("1"),
        executed_quantity=Decimal("0"),
        reduce_only=False,
        observed_at=snapshot.config.observed_at,
        raw_payload={},
    )
    context = _Ctx(
        account_snapshot=AccountSnapshot(
            config=snapshot.config,
            balances=snapshot.balances,
            positions=snapshot.positions,
            open_orders=(open_order,),
        ),
        account_state=ExecutionAccountStatus.READY_READONLY,
        account_observed_at=snapshot.config.observed_at,
    )

    out = frozen_decision_inputs_from_context(
        context,
        _state(),
        account_label="primary",
    )

    assert out is not None
    assert out.position_view.health_status == PositionHealthStatus.CATCHING_UP
    assert out.position_view.zero_position_snapshot_confirmed is False
    assert out.position_view.is_ready_for_trade is False


def test_non_active_strategy_is_not_ready() -> None:
    out = frozen_decision_inputs_from_context(
        _Ctx(
            account_snapshot=_snapshot(),
            strategy_state=StrategyLiveState.DRAINING,
        ),
        _state(),
        account_label="primary",
    )
    assert out is not None
    assert out.position_view.health_status == PositionHealthStatus.CATCHING_UP


def test_fact_source_requires_bound_context() -> None:
    src = LiveDecisionFactSource("primary")
    assert src.build(_state()) is None
    src.bind_context(_Ctx(account_snapshot=_snapshot()))
    assert src.build(_state()) is not None


def test_proven_coverage_yields_ready_position_health() -> None:
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        CoverageEvidence,
        FactCoverageStatus,
    )

    state = _state()
    evidence = CoverageEvidence(
        fill_cursor_id="cursor_1",
        fill_load_start=state.bucket_start - timedelta(hours=1),
        fill_checked_through=state.bucket_end + timedelta(minutes=1),
        checkpoint_id="chk_1",
        checkpoint_event_cut=state.bucket_end + timedelta(minutes=1),
    )
    ctx = _Ctx(
        account_snapshot=_snapshot("500"),
        coverage_by_symbol={state.symbol: evidence},
    )
    out = frozen_decision_inputs_from_context(
        ctx,
        state,
        account_label="primary",
    )
    assert out is not None
    assert out.position_view.health_status == PositionHealthStatus.READY
    assert out.position_view.coverage is not None
    assert out.position_view.coverage.status == FactCoverageStatus.CONFIRMED
    assert out.position_view.is_ready_for_trade is True


def test_fact_source_on_decision_result_updates_policy_state() -> None:
    from crypto_momentum_lab.domain.decision.decision_engine import (
        DecisionResult,
        PolicyState,
    )

    src = LiveDecisionFactSource("primary")
    assert src._policy_state.policy_version == 1

    next_st = PolicyState(policy_version=7)
    res = DecisionResult(
        decision_id="dec_test",
        input_hash="hash_test",
        intent=None,
        exit_command=None,
        next_policy_state=next_st,
        rejection_reason=None,
        evaluated_at=datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
    )
    src.on_decision_result(res)
    assert src._policy_state.policy_version == 7
