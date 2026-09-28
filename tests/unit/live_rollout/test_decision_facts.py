"""Live decision facts come from account context, never invented."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.domain.account.models import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.domain.decision.decision_engine import (
    DecisionResult,
    PolicyState,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionHealthStatus,
    PositionKey,
    PositionView,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.revision_models import DecisionTrace
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
        self.account_state = kwargs.get(
            "account_state", ExecutionAccountStatus.READY_READONLY
        )
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


def _position_view(
    *,
    symbol: str = "BTCUSDT",
    position_side: FuturesPositionSide = FuturesPositionSide.BOTH,
    account_label: str = "primary",
    health_status: PositionHealthStatus = PositionHealthStatus.READY,
    zero_position_snapshot_confirmed: bool = True,
    is_comparable: bool = True,
) -> PositionView:
    return PositionView(
        key=PositionKey("live", account_label, symbol, position_side),
        projection_version=f"pv_test_{account_label}_7",
        input_revision=1,
        event_cut=None,
        policy_version="1",
        schema_version="1",
        coverage=None,
        active_episode=None,
        batches=(),
        unallocated_quantity=Decimal("0"),
        zero_position_snapshot_confirmed=zero_position_snapshot_confirmed,
        health_status=health_status,
        is_comparable=is_comparable,
    )


def test_missing_snapshot_yields_none() -> None:
    out = frozen_decision_inputs_from_context(
        _Ctx(account_snapshot=None),
        _state(),
        account_label="primary",
        position_view=_position_view(),
    )
    assert out is None


def test_missing_position_view_yields_none() -> None:
    out = frozen_decision_inputs_from_context(
        _Ctx(account_snapshot=_snapshot()),
        _state(),
        account_label="primary",
        position_view=None,
    )
    assert out is None


def test_unready_position_view_yields_none() -> None:
    out = frozen_decision_inputs_from_context(
        _Ctx(account_snapshot=_snapshot()),
        _state(),
        account_label="primary",
        position_view=_position_view(health_status=PositionHealthStatus.CATCHING_UP),
    )
    assert out is None


def test_non_active_strategy_yields_none() -> None:
    out = frozen_decision_inputs_from_context(
        _Ctx(
            account_snapshot=_snapshot(),
            strategy_state=StrategyLiveState.DRAINING,
        ),
        _state(),
        account_label="primary",
        position_view=_position_view(),
    )
    assert out is None


def test_real_cash_and_versions_are_used() -> None:
    view = _position_view()
    out = frozen_decision_inputs_from_context(
        _Ctx(account_snapshot=_snapshot("250.5")),
        _state(),
        account_label="primary",
        position_view=view,
    )
    assert out is not None
    assert out.cash_balance == Decimal("250.5")
    assert out.position_view.is_ready_for_trade is True
    assert out.universe_version == "univ_1"


async def test_fact_source_requires_bound_context() -> None:
    src = LiveDecisionFactSource("primary")
    assert await src.build(_state()) is None


async def test_fact_source_degrades_only_mismatched_restored_stream() -> None:
    class FakeBook:
        async def read(self, *_args, **_kwargs):
            raise ValueError(
                "requested account stream does not match the restored position"
            )

    src = LiveDecisionFactSource(
        "primary", execution_book=FakeBook(), hedge_mode=False
    )
    src.bind_context(_Ctx(account_snapshot=_snapshot()))
    src.bind_account_stream(stream_id="current", stream_epoch="epoch-2", sequence=1)

    assert await src.build(_state()) is None


async def test_fact_source_does_not_hide_other_book_errors() -> None:
    class FakeBook:
        async def read(self, *_args, **_kwargs):
            raise ValueError("corrupt projection")

    src = LiveDecisionFactSource(
        "primary", execution_book=FakeBook(), hedge_mode=False
    )
    src.bind_context(_Ctx(account_snapshot=_snapshot()))
    src.bind_account_stream(stream_id="current", stream_epoch="epoch-2", sequence=1)

    with pytest.raises(ValueError, match="corrupt projection"):
        await src.build(_state())


async def test_fact_source_commit_decision_updates_policy_state() -> None:
    class FakeUoW:
        async def commit_decision(self, commit):
            return SimpleNamespace(
                policy_revision=commit.expected_policy_revision + 1,
                next_state_digest="digest_test",
                decision_id="dec_test",
            )

    src = LiveDecisionFactSource("primary", decision_unit_of_work=FakeUoW())
    assert src.current_policy_state.policy_version == 1

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
    trace = DecisionTrace(
        decision_id="dec_test",
        account_label="primary",
        strategy_name="orderflow_impulse",
        decision_time=datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
        intent_produced=False,
        frame_digest="frame_digest",
        evaluated_market_refs=(
            SimpleNamespace(bucket_start=datetime(2026, 9, 25, 8, 0, tzinfo=UTC)),
        ),
    )
    receipt = await src.commit_decision(trace, res, None)
    assert receipt.decision_id == "dec_test"
    assert src.current_policy_state.policy_version == 7
    assert src.policy_revision == 1
