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
from crypto_momentum_lab.domain.account.snapshot_models import (
    AccountSnapshot,
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


def test_account_stream_registration_is_explicit_and_rebound_with_book() -> None:
    from unittest.mock import Mock

    class Reader:
        @property
        def register_active_stream(self):
            raise AssertionError("reader capability was probed")

        async def read(self, *_args, **_kwargs):
            raise AssertionError("registration must not read positions")

    initial = Mock()
    replacement = Mock()
    source = LiveDecisionFactSource(
        "primary", execution_book=Reader(), register_account_stream=initial
    )
    source.bind_account_stream(stream_id="accounts", stream_epoch="epoch", sequence=2)
    initial.assert_called_once_with(
        environment="live",
        account_label="primary",
        stream_id="accounts",
        stream_epoch="epoch",
    )
    source.set_execution_book(Reader(), register_account_stream=replacement)
    with pytest.raises(ValueError, match="regressed"):
        source.bind_account_stream(
            stream_id="accounts", stream_epoch="epoch", sequence=1
        )
    replacement.assert_not_called()
    source.bind_account_stream(stream_id="accounts", stream_epoch="epoch", sequence=3)
    replacement.assert_called_once_with(
        environment="live",
        account_label="primary",
        stream_id="accounts",
        stream_epoch="epoch",
    )
    source.set_execution_book(Reader())
    source.bind_account_stream(stream_id="accounts", stream_epoch="epoch", sequence=4)
    assert initial.call_count == replacement.call_count == 1


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


async def test_hedge_side_discovery_reads_only_the_candidate_symbol() -> None:
    class FakeBook:
        def __init__(self) -> None:
            self.sides = []

        async def read(self, scope, **_kwargs):
            self.sides.append(scope.position_side)
            return _position_view(position_side=scope.position_side)

        async def list_position_views(self, **_kwargs):
            raise AssertionError("hedge side discovery scanned every historical symbol")

    book = FakeBook()
    src = LiveDecisionFactSource("primary", execution_book=book, hedge_mode=True)
    src.bind_context(_Ctx(account_snapshot=_snapshot()))
    src.bind_account_stream(stream_id="current", stream_epoch="epoch-2", sequence=1)

    assert await src.build(_state()) is None
    assert book.sides == [FuturesPositionSide.LONG, FuturesPositionSide.SHORT]


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


def _pending_exit_case(*, request_exit_recovery=lambda: None):
    from unittest.mock import AsyncMock, create_autospec

    from crypto_momentum_lab.domain.decision.ports import DecisionUnitOfWorkPort
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
    from crypto_momentum_lab.domain.execution.order_state import ExitAllocation
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
    )
    from crypto_momentum_lab.domain.execution.trade_command import (
        ExitAllocationPlan,
        ExitPolicyMode,
        TradeCommand,
        TradeCommandType,
    )
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

    key = PositionKey("live", "incident-account", "TESTUSDT", FuturesPositionSide.LONG)
    scope = AccountFactStreamScope.for_position_key(
        key,
        stream_id="account_event_hub",
        stream_epoch="current-epoch",
    )
    plan = ExitAllocationPlan(
        position_key=key,
        allocations=(ExitAllocation("batch-1", Decimal("1")),),
        total_allocated_quantity=Decimal("1"),
        policy=ExitPolicyMode.FULL_POSITION_CLOSE,
        projection_version="pv_expected",
    )
    command = TradeCommand(
        command_id="incident-exit",
        position_key=key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("1"),
        reduce_only=True,
        allocation_plan=plan,
        expected_projection_version="pv_expected",
        created_at=datetime(2026, 9, 29, 10, tzinfo=UTC),
    )
    uow = create_autospec(DecisionUnitOfWorkPort, instance=True, spec_set=True)
    uow.load_pending_exits.return_value = (("incident-decision", command),)
    uow.mark_exit_dispatched.return_value = True
    book = create_autospec(ExecutionBook, instance=True, spec_set=True)
    view = SimpleNamespace(
        stream_scope=scope,
        is_ready_for_trade=True,
        projection_version="pv_expected",
    )
    book.read.return_value = view
    source = LiveDecisionFactSource(
        "incident-account",
        decision_unit_of_work=uow,
        execution_book=book,
        register_account_stream=book.register_active_stream,
        request_exit_recovery=request_exit_recovery,
    )
    handler = AsyncMock(return_value=SimpleNamespace(state="submitted"))
    source.set_exit_handler(handler)
    source.bind_account_stream(
        stream_id="account_event_hub", stream_epoch="current-epoch", sequence=1
    )
    return source, uow, book, view, handler, command


async def test_pending_exit_recovers_after_book_becomes_ready() -> None:
    source, uow, book, _, handler, command = _pending_exit_case()
    book.read.side_effect = ValueError(
        "requested account stream does not match the restored position"
    )
    await source.recover_pending_exits()
    handler.assert_not_awaited()
    uow.mark_exit_dispatched.assert_not_awaited()

    book.read.side_effect = None
    await source.recover_pending_exits()
    handler.assert_awaited_once_with(command)
    uow.mark_exit_dispatched.assert_awaited_once_with(
        "incident-decision", command.command_id
    )


@pytest.mark.parametrize(
    "mismatch",
    ["projection", "not_ready", "epoch", "dispatch_recovery", "dispatch_unknown"],
)
async def test_newly_committed_exit_defers_without_stopping_market_consumer(
    mismatch: str,
) -> None:
    from unittest.mock import Mock

    request = Mock()
    source, uow, book, view, handler, command = _pending_exit_case(
        request_exit_recovery=request
    )
    if mismatch == "projection":
        view.projection_version = "pv_newer"
    elif mismatch == "not_ready":
        view.is_ready_for_trade = False
    elif mismatch == "epoch":
        book.read.side_effect = ValueError(
            "requested account stream does not match the restored position"
        )
    elif mismatch == "dispatch_recovery":
        handler.side_effect = RuntimeError(
            "ExecutionBook applied order facts but reservation settlement "
            "requires recovery: Filled terminal lacks complete account trade facts"
        )
    else:
        handler.side_effect = OSError("exchange response is unknown")
    uow.commit_decision.return_value = SimpleNamespace(
        policy_revision=1,
        next_state_digest="next_digest",
        decision_id="incident-decision",
        is_replay=False,
    )
    next_state = PolicyState(policy_version=2)
    result = DecisionResult(
        decision_id="incident-decision",
        input_hash="input",
        intent=None,
        exit_command=command,
        next_policy_state=next_state,
        rejection_reason=None,
        evaluated_at=command.created_at,
    )
    trace = DecisionTrace(
        decision_id="incident-decision",
        account_label="incident-account",
        strategy_name="orderflow_impulse",
        decision_time=command.created_at,
        intent_produced=False,
        frame_digest="frame",
        evaluated_market_refs=(SimpleNamespace(bucket_start=command.created_at),),
    )

    receipt = await source.commit_decision(trace, result, None)

    assert receipt.decision_id == "incident-decision"
    request.assert_called_once_with()
    assert source.current_policy_state == next_state
    assert source.policy_revision == 1
    if mismatch.startswith("dispatch_"):
        handler.assert_awaited_once_with(command)
    else:
        handler.assert_not_awaited()
    uow.mark_exit_dispatched.assert_not_awaited()
    uow.mark_exit_superseded.assert_not_awaited()

    book.read.side_effect = None
    handler.side_effect = None
    handler.reset_mock()
    view.projection_version = "pv_expected"
    view.is_ready_for_trade = True
    await source.recover_pending_exits()
    handler.assert_awaited_once_with(command)
    uow.mark_exit_dispatched.assert_awaited_once_with(
        "incident-decision", command.command_id
    )


@pytest.mark.parametrize("mismatch", ["epoch", "projection", "allocation", "not_ready"])
async def test_stale_pending_exit_is_never_dispatched_or_silently_acknowledged(
    mismatch: str,
) -> None:
    from dataclasses import replace

    source, uow, _, view, handler, command = _pending_exit_case()
    if mismatch == "epoch":
        view.stream_scope = replace(view.stream_scope, stream_epoch="obsolete-epoch")
    elif mismatch == "projection":
        view.projection_version = "pv_newer"
    elif mismatch == "allocation":
        command = replace(
            command,
            allocation_plan=replace(
                command.allocation_plan, projection_version="pv_other"
            ),
        )
        uow.load_pending_exits.return_value = (("incident-decision", command),)
    else:
        view.is_ready_for_trade = False
    for _ in range(3):
        await source.recover_pending_exits()
    handler.assert_not_awaited()
    uow.mark_exit_dispatched.assert_not_awaited()
    # Characterization: there is no exchange reconciliation at this seam that
    # would justify declaring an unknown submission superseded.
    uow.mark_exit_superseded.assert_not_awaited()


@pytest.mark.parametrize("result_state", ["rejected", "unknown"])
async def test_unconfirmed_exit_does_not_advance_durable_status(
    result_state: str,
) -> None:
    source, uow, _, _, handler, _ = _pending_exit_case()
    handler.return_value = SimpleNamespace(state=result_state)
    await source.recover_pending_exits()
    handler.assert_awaited_once()
    uow.mark_exit_dispatched.assert_not_awaited()


async def test_unrelated_book_corruption_is_not_swallowed_as_epoch_recovery() -> None:
    source, _, book, _, handler, _ = _pending_exit_case()
    book.read.side_effect = ValueError("invalid recovery payload")
    with pytest.raises(ValueError, match="invalid recovery payload"):
        await source.recover_pending_exits()
    handler.assert_not_awaited()


async def test_dispatch_does_not_swallow_book_corruption() -> None:
    source, uow, book, _, handler, command = _pending_exit_case()
    book.read.side_effect = ValueError("invalid recovery payload")
    with pytest.raises(ValueError, match="invalid recovery payload"):
        await source._dispatch_exit("incident-decision", command)
    handler.assert_not_awaited()
    uow.mark_exit_dispatched.assert_not_awaited()


async def test_dispatch_cancellation_still_stops_the_task() -> None:
    import asyncio

    source, uow, _, _, handler, command = _pending_exit_case()
    handler.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await source._dispatch_exit("incident-decision", command)
    uow.mark_exit_dispatched.assert_not_awaited()


async def test_fact_source_commit_decision_heals_diverged_prior_digest() -> None:
    class FakeUoW:
        async def commit_decision(self, commit):
            return SimpleNamespace(
                policy_revision=commit.expected_policy_revision + 1,
                next_state_digest="digest_healed",
                decision_id="dec_heal",
            )

    src = LiveDecisionFactSource("primary", decision_unit_of_work=FakeUoW())
    # Fabricate a diverged in-memory policy digest
    src._policy_digest = "diverged_in_memory_digest"

    next_st = PolicyState(policy_version=2)
    res = DecisionResult(
        decision_id="dec_heal",
        input_hash="hash_heal",
        intent=None,
        exit_command=None,
        next_policy_state=next_st,
        rejection_reason=None,
        evaluated_at=datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
    )
    trace = DecisionTrace(
        decision_id="dec_heal",
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
    assert receipt.decision_id == "dec_heal"
    assert src.current_policy_state.policy_version == 2
    assert src.policy_revision == 1


async def test_fact_source_commit_decision_heals_skipped_revision() -> None:
    class FakeUoW:
        async def commit_decision(self, commit):
            # UoW returns a revision ahead by 5
            return SimpleNamespace(
                policy_revision=commit.expected_policy_revision + 5,
                next_state_digest="digest_advanced",
                decision_id="dec_skip",
            )

    src = LiveDecisionFactSource("primary", decision_unit_of_work=FakeUoW())
    next_st = PolicyState(policy_version=3)
    res = DecisionResult(
        decision_id="dec_skip",
        input_hash="hash_skip",
        intent=None,
        exit_command=None,
        next_policy_state=next_st,
        rejection_reason=None,
        evaluated_at=datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
    )
    trace = DecisionTrace(
        decision_id="dec_skip",
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
    assert receipt.decision_id == "dec_skip"
    assert src.policy_revision == 5
    assert src.current_policy_state.policy_version == 3


async def test_flat_book_does_not_prove_pending_exit_was_never_submitted():
    source, uow, book, view, handler, command = _pending_exit_case()
    view.total_quantity = Decimal("0")
    await source.recover_pending_exits()
    uow.mark_exit_superseded.assert_not_awaited()
    uow.mark_exit_dispatched.assert_not_awaited()
    handler.assert_not_awaited()
