"""SYSTEM.md section 8 acceptance: exit identity, cutover safety, restart.

Every case drives real engine, parser, settlement and ledger code.  Only
external boundaries are substituted: the exchange HTTP transport, the durable
storage adapter used as an observation sink, and explicit fixture clocks.

Row "Hedge及BOTH，队列溢出/关闭，行情与私有断线"
    Hedge business ``reduce_only`` is decoupled from the WS ``R`` flag: a legal
    hedge close whose wire ``R`` is false is still accepted, a hedge close
    request never carries ``reduceOnly``, BOTH keeps the strict consistency
    check, and every other identity/quantity check still rejects mismatches.
    Observation-queue overflow and close are counted and routed to command
    recovery instead of being dropped, and closing does not hang.  A stream gap
    is not cleared just because a connection succeeded again.

    Existing field-level protocol cases live in
    ``tests/unit/execution_account/test_user_data.py`` (a4a/a4b) and
    ``tests/unit/execution_account/test_binance_client.py`` (a4a).  This file
    keeps the row-level proof and extends it with ``order_type``/``quantity``/
    ``price`` identity negatives, the one-way ``reduceOnly=true`` request, and
    the requirement that a ``reduce_only=True`` plan can never be POSTed as a
    position-increasing order (recorded as an EXPECTED FAIL, see below).

Row "停入场→原规则退出→无遗留→切换与回退"
    Stopping entries is not an emergency flatten; entry stop, timed-flatten
    start and admission restore are independent boundaries.  A single flat
    snapshot does not prove exit responsibility ended.

Row "旧历史查询、活动责任保护与安全切换" and
"带仓重启保留批次、成本、订单身份和原宽限截止"
    A restart with an open position keeps its batches, cost basis, order
    identity and its ORIGINAL grace deadline; a retry does not reset it, and
    older history stays readable while the active view moves on.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import parse_qs
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account import (
    ExecutionAccountStatus,
)
from crypto_momentum_lab.domain.account.models import (
    AccountConfigSnapshot,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.account.snapshot_models import AccountSnapshot
from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    ExitOrderSubmissionFact,
    PositionKey,
)
from crypto_momentum_lab.domain.strategy.position_exit import PositionExitMode
from crypto_momentum_lab.execution_account.binance.client import BinanceUsdMTradeClient
from crypto_momentum_lab.execution_account.binance.user_data_parser import (
    order_snapshot_from_update,
)
from crypto_momentum_lab.execution_account.hub import AccountEvent
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.live_rollout.control_plane import LiveControlPlaneRuntime
from crypto_momentum_lab.live_rollout.entry_control import (
    LiveEntryControlGate,
    TradingGateState,
)
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitManager,
    managed_live_positions_from_views,
)
from crypto_momentum_lab.live_rollout.postgres_runtime import (
    PostgresLiveContextProvider,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
)
from crypto_momentum_lab.persistence.postgres.models import (
    PositionFactJournalEventRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from tests.fixtures.live_market import _state
from tests.integration.persistence.test_authority_book_transactions import (
    _book,
    _evidence,
)
from tests.unit.execution.test_position_recovery import (
    _fill as _ledger_fill,
)
from tests.unit.execution.test_position_recovery import (
    _key as _ledger_key,
)
from tests.unit.execution.test_position_recovery import (
    _scope as _ledger_scope,
)
from tests.unit.execution.test_position_recovery import (
    _time as _ledger_time,
)
from tests.unit.live_rollout.test_exits import _config as _exit_config
from tests.unit.live_rollout.test_postgres_runtime import _runtime_context

pytestmark = pytest.mark.contract

NOW = datetime(2026, 10, 12, 18, 0, tzinfo=UTC)


# --------------------------------------------------------------------------
# Row 1a: hedge reduce_only is decoupled from the WS R flag.
# --------------------------------------------------------------------------


def _plan(
    *,
    side: str,
    position_side: FuturesPositionSide,
    reduce_only: bool,
    quantity: str = "2",
    price: Decimal | None = None,
    order_type: str = "MARKET",
    client_order_id: str = "cml-exit-1",
) -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id="intent-1",
        run_id="run-1",
        client_order_id=client_order_id,
        symbol="BTCUSDT",
        side=side,
        order_type=order_type,
        quantity=Decimal(quantity),
        price=price,
        reduce_only=reduce_only,
        created_at=NOW,
        position_side=position_side,
        quantized=True,
        time_in_force="GTC" if price is not None else None,
    )


def _ws_order(
    plan: OrderExecutionPlan,
    *,
    r_flag: object,
    at_ms: int = 1_790_000_000_000,
) -> dict[str, object]:
    """A complete Binance ORDER_TRADE_UPDATE ``o`` object for ``plan``."""
    return {
        "c": plan.client_order_id,
        "s": plan.symbol,
        "S": plan.side,
        "o": plan.order_type,
        "q": format(plan.quantity, "f"),
        "p": format(plan.price if plan.price is not None else Decimal("0"), "f"),
        "R": r_flag,
        "ps": plan.position_side.value,
        "i": 424242,
        "X": "NEW",
        "z": "0",
        "ap": "0",
        "T": at_ms,
    }


def test_hedge_close_without_the_ws_reduce_only_flag_is_accepted() -> None:
    """A hedge close is identified by side+positionSide, never by wire ``R``."""
    plans = (
        _plan(side="SELL", position_side=FuturesPositionSide.LONG, reduce_only=True),
        _plan(side="BUY", position_side=FuturesPositionSide.SHORT, reduce_only=True),
    )

    for plan in plans:
        snapshot = order_snapshot_from_update(_ws_order(plan, r_flag=False), plan)
        assert snapshot is not None, (
            "a legal hedge close carrying R=false was rejected: "
            f"{plan.position_side.value} {plan.side}"
        )
        assert snapshot.state is ExchangeOrderState.ACKNOWLEDGED
        assert snapshot.client_order_id == plan.client_order_id
        assert snapshot.executed_quantity == Decimal("0")

    # R is fully decoupled in hedge mode: an opening order whose wire R is true
    # is still classified by side+positionSide and accepted as an opening.
    hedge_open = _plan(
        side="BUY",
        position_side=FuturesPositionSide.LONG,
        reduce_only=False,
    )
    opened = order_snapshot_from_update(_ws_order(hedge_open, r_flag=True), hedge_open)
    assert opened is not None


def test_both_accounts_still_require_the_reduce_only_flag_to_match() -> None:
    """One-way mode keeps the strict ``R == plan.reduce_only`` consistency check."""
    both_close = _plan(
        side="SELL",
        position_side=FuturesPositionSide.BOTH,
        reduce_only=True,
    )

    with pytest.raises(ValueError, match="durable identity"):
        order_snapshot_from_update(_ws_order(both_close, r_flag=False), both_close)

    consistent = order_snapshot_from_update(
        _ws_order(both_close, r_flag=True), both_close
    )
    assert consistent is not None
    assert consistent.state is ExchangeOrderState.ACKNOWLEDGED


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda o: o.update(c="other-client"), id="client_order_id"),
        pytest.param(lambda o: o.update(s="ETHUSDT"), id="symbol"),
        pytest.param(lambda o: o.update(S="BUY"), id="side"),
        pytest.param(lambda o: o.update(o="LIMIT"), id="order_type"),
        pytest.param(lambda o: o.update(ps="SHORT"), id="position_side"),
        pytest.param(lambda o: o.update(q="3"), id="quantity"),
    ],
)
def test_hedge_close_identity_and_quantity_checks_are_not_weakened(mutate) -> None:
    """Dropping the hedge ``R`` check must not relax any other validation."""
    plan = _plan(
        side="SELL",
        position_side=FuturesPositionSide.LONG,
        reduce_only=True,
    )
    order = _ws_order(plan, r_flag=False)
    mutate(order)

    with pytest.raises(ValueError, match="durable identity"):
        order_snapshot_from_update(order, plan)


def test_hedge_close_negatives_and_non_boolean_r_are_still_rejected() -> None:
    """An opening order marked reduce-only and a non-bool ``R`` stay rejected."""
    opening = _plan(
        side="BUY",
        position_side=FuturesPositionSide.LONG,
        reduce_only=True,
    )
    with pytest.raises(ValueError, match="durable identity"):
        order_snapshot_from_update(_ws_order(opening, r_flag=False), opening)

    legal = _plan(
        side="SELL",
        position_side=FuturesPositionSide.LONG,
        reduce_only=True,
    )
    assert (
        order_snapshot_from_update(_ws_order(legal, r_flag="false"), legal) is None
    ), "a non-boolean wire reduce-only flag must not become an accepted fact"

    limit_close = _plan(
        side="SELL",
        position_side=FuturesPositionSide.LONG,
        reduce_only=True,
        price=Decimal("99.5"),
        order_type="LIMIT",
    )
    wrong_price = _ws_order(limit_close, r_flag=False)
    wrong_price["p"] = "98.0"
    with pytest.raises(ValueError, match="durable identity"):
        order_snapshot_from_update(wrong_price, limit_close)


@pytest.mark.parametrize(
    "position_side",
    [FuturesPositionSide.LONG, FuturesPositionSide.SHORT, FuturesPositionSide.BOTH],
)
async def test_hedge_close_request_never_carries_reduce_only(
    position_side: FuturesPositionSide,
) -> None:
    """The wire request separates hedge mode (positionSide) from one-way mode."""
    side = "SELL" if position_side is not FuturesPositionSide.SHORT else "BUY"
    plan = _plan(side=side, position_side=position_side, reduce_only=True)
    bodies: list[dict[str, list[str]]] = []

    async def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/fapi/v1/order"
        bodies.append(parse_qs(request.content.decode()))
        return httpx.Response(
            200,
            json={
                "clientOrderId": plan.client_order_id,
                "orderId": 424242,
                "status": "NEW",
                "executedQty": "0",
                "avgPrice": "0",
            },
        )

    client = BinanceUsdMTradeClient(
        api_key="test-key",
        api_secret="test-secret",
        environment="live",
        account_label="primary",
        live_submit_enabled=True,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handle), base_url="https://fapi.binance.com"
        ),
        clock=lambda: NOW,
    )
    try:
        snapshot = await client.submit_order(plan)
    finally:
        await client.aclose()

    assert snapshot.state is ExchangeOrderState.ACKNOWLEDGED
    assert len(bodies) == 1
    body = bodies[0]
    assert body["quantity"] == ["2"]
    if position_side is FuturesPositionSide.BOTH:
        assert body["reduceOnly"] == ["true"]
        assert "positionSide" not in body
    else:
        assert body["positionSide"] == [position_side.value]
        assert body["side"] == [side]
        assert "reduceOnly" not in body, (
            "a hedge close must not send reduceOnly; Binance does not accept it in "
            "hedge mode"
        )


@pytest.mark.parametrize(
    ("position_side", "side"),
    [(FuturesPositionSide.LONG, "BUY"), (FuturesPositionSide.SHORT, "SELL")],
)
async def test_hedge_reduce_only_plan_that_would_increase_exposure_is_refused(
    position_side: FuturesPositionSide,
    side: str,
) -> None:
    """Hedge decoupling must not silently drop the reduce-only guarantee.

    ``reduceOnly`` is absent in hedge mode, so the exchange classifies the
    order purely from ``side`` + ``positionSide``.  A plan marked
    ``reduce_only=True`` whose side already agrees with its position side is
    therefore a position-increasing order wearing a close's identity; the
    engine must refuse it instead of POSTing it.
    """
    plan = _plan(side=side, position_side=position_side, reduce_only=True)
    bodies: list[dict[str, list[str]]] = []

    async def handle(request: httpx.Request) -> httpx.Response:
        bodies.append(parse_qs(request.content.decode()))
        return httpx.Response(
            200,
            json={
                "clientOrderId": plan.client_order_id,
                "orderId": 424242,
                "status": "NEW",
                "executedQty": "0",
                "avgPrice": "0",
            },
        )

    client = BinanceUsdMTradeClient(
        api_key="test-key",
        api_secret="test-secret",
        environment="live",
        account_label="primary",
        live_submit_enabled=True,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handle), base_url="https://fapi.binance.com"
        ),
        clock=lambda: NOW,
    )
    try:
        await client.submit_order(plan)
    except Exception:
        # Refusing the plan locally is an acceptable fix.
        pass
    finally:
        await client.aclose()

    closing_side = {
        FuturesPositionSide.LONG: "SELL",
        FuturesPositionSide.SHORT: "BUY",
    }[position_side]
    increasing = [body for body in bodies if body.get("side") != [closing_side]]
    assert increasing == [], (
        "a reduce_only hedge plan was POSTed as a position-increasing order: "
        f"{increasing}"
    )


# --------------------------------------------------------------------------
# Row 1b: observation-queue overflow and close are counted, never dropped.
# --------------------------------------------------------------------------


class _StallingBook:
    """Durable observation boundary that parks inside its first commit.

    It decides nothing about queueing or recovery: the coordinator's real
    accounting runs and only the storage commit is stalled.
    """

    has_execution_unit_of_work = True

    def __init__(self) -> None:
        self.recovery_requests: list[str] = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0
        self._view = SimpleNamespace(
            stream_scope=SimpleNamespace(stream_id="ws", stream_epoch="epoch-1")
        )

    def require_command_recovery(self, command_id: str) -> None:
        self.recovery_requests.append(command_id)

    def get_outbox(self, _command_id: str) -> None:
        return None

    def get_active_stream(self, *_args: object) -> tuple[str, str]:
        return ("ws", "epoch-1")

    async def read(self, _scope: object) -> object:
        return self._view

    async def observe(self, _evidence: object) -> object:
        self.calls += 1
        if self.calls == 1:
            self.entered.set()
            await self.release.wait()
        return SimpleNamespace()


def _entry_plan(index: int) -> OrderExecutionPlan:
    return _plan(
        side="BUY",
        position_side=FuturesPositionSide.BOTH,
        reduce_only=False,
        client_order_id=f"entry-{index}",
    )


def _coordinator(capacity: int, book: object) -> OrderExecutionCoordinator:
    """A real coordinator whose exchange boundary echoes ACKED-zero results."""
    backend = AsyncMock()

    async def apply_observed_snapshot(
        plan: OrderExecutionPlan, _snapshot: object
    ) -> OrderExecutionResult:
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-1",
            executed_quantity=Decimal("0"),
            plan=plan,
        )

    backend.apply_observed_snapshot.side_effect = apply_observed_snapshot
    return OrderExecutionCoordinator(
        backend=backend,
        environment="live",
        account_label="primary",
        execution_book=book,
        order_observation_capacity=capacity,
    )


async def _fill_observation_queue(
    coordinator: OrderExecutionCoordinator,
    book: _StallingBook,
    count: int,
) -> None:
    """Fill the real observation queue while its consumer is parked."""
    await coordinator.apply_observed_snapshot(_entry_plan(0), MagicMock())
    await asyncio.wait_for(book.entered.wait(), timeout=1.0)

    for index in range(1, count + 1):
        await coordinator.apply_observed_snapshot(_entry_plan(index), MagicMock())


async def test_observation_queue_overflow_is_counted_and_routed_to_recovery() -> None:
    """A full observation queue must request recovery, not silently drop."""
    capacity = 4
    book = _StallingBook()
    coordinator = _coordinator(capacity, book)
    await _fill_observation_queue(coordinator, book, capacity)

    assert coordinator.order_observation_metrics.depth == capacity
    assert coordinator.order_observation_metrics.overflow_count == 0

    await coordinator.apply_observed_snapshot(_entry_plan(capacity + 1), MagicMock())

    metrics = coordinator.order_observation_metrics
    assert metrics.overflow_count == 1, (
        f"the overflowing observation was not counted: {metrics}"
    )
    assert book.recovery_requests == [f"entry-{capacity + 1}"], (
        "the overflowing observation must be routed to command recovery: "
        f"{book.recovery_requests}"
    )
    # entry-0 is already in flight, entry-1..capacity wait in the queue.
    assert metrics.enqueued_count == capacity + 1
    assert metrics.dequeued_count == 1

    book.release.set()
    await coordinator.aclose()


async def test_observation_queue_close_does_not_hang_and_keeps_every_fact() -> None:
    """Closing the queue drains pending facts into recovery and returns."""
    capacity = 3
    book = _StallingBook()
    coordinator = _coordinator(capacity, book)
    await _fill_observation_queue(coordinator, book, capacity)

    assert coordinator.order_observation_metrics.depth == capacity
    await asyncio.wait_for(coordinator.aclose(), timeout=2.0)

    assert sorted(book.recovery_requests) == sorted(
        f"entry-{index}" for index in range(capacity + 1)
    ), (
        "closing the observation queue silently dropped pending exchange facts: "
        f"{book.recovery_requests}"
    )
    assert coordinator.order_observation_metrics.depth == 0

    with pytest.raises(RuntimeError, match="closed"):
        await coordinator.apply_observed_snapshot(_entry_plan(0), MagicMock())


# --------------------------------------------------------------------------
# Row 1c: a stream gap is not cleared by a successful reconnection.
# --------------------------------------------------------------------------


def _account_snapshot() -> AccountSnapshot:
    return AccountSnapshot(
        config=AccountConfigSnapshot(
            environment="live",
            account_label="primary",
            multi_assets_mode=False,
            hedge_mode=False,
            fee_tier=0,
            observed_at=NOW,
            raw_payload={},
        ),
        balances=(),
        positions=(),
        open_orders=(),
    )


def _account_event(*, snapshot: AccountSnapshot | None, sequence: int) -> AccountEvent:
    return AccountEvent(
        environment="live",
        account_label="primary",
        event_type="ACCOUNT_UPDATE",
        event_id=f"event-{sequence}",
        event_at=NOW,
        received_at=NOW,
        account_state=(
            ExecutionAccountStatus.READY_READONLY if snapshot is not None else None
        ),
        account_snapshot=snapshot,
        snapshot_kind="full" if snapshot is not None else "notification",
        sequence=sequence,
    )


def test_market_stream_reconnect_alone_does_not_restore_entry_admission() -> None:
    """Socket recovery is not strategy recovery (section 6)."""
    gate = LiveEntryControlGate(
        run_id="run-1",
        state_machine=_coordinator(8, _StallingBook()),
        clock=lambda: NOW,
    )
    control_plane: LiveControlPlaneRuntime

    def refresh_entry_gate() -> None:
        # Mirrors the composition root: the gate reads live control-plane
        # availability rather than a copy published alongside it.
        gate.refresh_entry_prerequisites(
            session_draining=False,
            market_state_available=control_plane.market_state_available,
            market_state_unavailable_reason=(
                control_plane.market_state_unavailable_reason
            ),
            strategy_warmup_ready=control_plane.strategy_warmup_ready,
            strategy_warmup_reason=control_plane.strategy_warmup_reason,
        )

    control_plane = LiveControlPlaneRuntime(
        session_id="live-1",
        context_provider=SimpleNamespace(
            update_account_snapshot=lambda *args, **kwargs: None,
            invalidate_account_snapshot=lambda: None,
        ),
        market_state_available=True,
        strategy_warmup_ready=True,
        notify_market_state_gap=lambda reason: None,
        refresh_entry_gate=refresh_entry_gate,
        clock=lambda: NOW,
    )
    refresh_entry_gate()
    assert gate.is_symbol_entry_allowed("BTCUSDT") == (True, "entry_allowed")

    control_plane.on_market_connection_change(False, "market_state_stream_reset")
    assert gate.is_symbol_entry_allowed("BTCUSDT")[0] is False
    assert gate.trading_gate_state is TradingGateState.EXIT_ONLY

    # The socket comes back.  Nothing was replayed and no warmup was republished.
    control_plane.on_market_connection_change(True, None)

    assert gate.is_symbol_entry_allowed("BTCUSDT") == (
        False,
        "market_state_epoch_rewarming",
    ), "a successful reconnect alone reopened entry admission"

    # Only a fresh warmup publication reopens the strategy boundary.
    control_plane.set_strategy_warmup_ready(True, reason="strategy_warmup_ready")
    assert gate.is_symbol_entry_allowed("BTCUSDT") == (True, "entry_allowed")
    assert gate.trading_gate_state is TradingGateState.ACTIVE


def test_private_stream_gap_is_not_cleared_by_a_reconnect_notification() -> None:
    """Account facts stay invalid until a complete snapshot is republished."""
    provider = PostgresLiveContextProvider(
        session_factory=lambda: None,
        account_label="primary",
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_config_hash="a" * 64,
    )
    control_plane = LiveControlPlaneRuntime(
        session_id="live-1",
        context_provider=provider,
        market_state_available=True,
        notify_market_state_gap=lambda reason: None,
        refresh_entry_gate=lambda: None,
        clock=lambda: NOW,
    )
    snapshot = _account_snapshot()

    control_plane.on_account_snapshot(_account_event(snapshot=snapshot, sequence=7))
    current = replace(
        _runtime_context(),
        account_snapshot=snapshot,
        account_snapshot_version=7,
        context_epoch=None,
    )
    assert provider.is_current(current) is True
    assert control_plane.account_snapshot_available is True

    control_plane.on_account_snapshot_recovery("account_event_sequence_gap")
    assert provider.is_current(current) is False
    assert control_plane.account_snapshot_available is False

    # The private stream reconnects and delivers a notification-only frame.
    control_plane.on_account_snapshot(_account_event(snapshot=None, sequence=8))
    assert control_plane.account_snapshot_available is False, (
        "a reconnect notification cleared the account-fact gap"
    )
    assert provider.is_current(current) is False, (
        "a reconnect notification made the stale account facts current again"
    )

    refreshed = replace(
        _runtime_context(),
        account_snapshot=snapshot,
        account_snapshot_version=8,
        context_epoch=None,
    )
    control_plane.on_account_snapshot(_account_event(snapshot=snapshot, sequence=8))
    assert control_plane.account_snapshot_available is True
    assert provider.is_current(refreshed) is True


# --------------------------------------------------------------------------
# Row 2: entry stop, timed flatten and admission restore are independent.
# --------------------------------------------------------------------------


def test_entry_stop_timed_flatten_and_admission_restore_are_independent() -> None:
    """Stopping entries is not a flatten and not a halted exit channel."""
    gate = LiveEntryControlGate(
        run_id="run-1",
        state_machine=_coordinator(8, _StallingBook()),
        clock=lambda: NOW,
    )
    assert gate.entry_enabled is True
    assert gate.trading_gate_state is TradingGateState.ACTIVE

    # Boundary 1: persistent entry stop.  New risk is refused while the exit
    # channel stays open, so this is not an emergency flatten.
    gate.set_entry_enabled(False, reason="cutover_entry_stop")
    assert gate.entry_enabled is False
    assert gate.entry_enabled_reason == "cutover_entry_stop"
    assert gate.is_symbol_entry_allowed("BTCUSDT") == (False, "cutover_entry_stop")
    assert gate.trading_gate_state is TradingGateState.EXIT_ONLY

    # Boundary 2: the timed flatten window starts on its own schedule.
    gate.set_scheduled_entry_blocked(True, reason="scheduled_risk_window")
    assert gate.entry_enabled_reason == "scheduled_risk_window"

    # Boundary 3: restoring admission must not clear the flatten window.
    gate.set_entry_enabled(True, reason="operator_resume_entry")
    assert gate.entry_enabled is False
    assert gate.entry_enabled_reason == "scheduled_risk_window"

    # Clearing the window restores admission, because the entry stop was
    # independently restored rather than inferred from the window.
    gate.set_scheduled_entry_blocked(False, reason="window_closed")
    assert gate.entry_enabled is True
    assert gate.entry_enabled_reason == "operator_resume_entry"
    assert gate.trading_gate_state is TradingGateState.ACTIVE

    # A halt disables the exit channel too; it is a third, separate boundary.
    gate.set_halted(True, reason="emergency_stop")
    assert gate.trading_gate_state is TradingGateState.HALTED
    assert gate.entry_enabled is False


def test_a_single_flat_snapshot_does_not_prove_responsibility_ended() -> None:
    """A flat exchange snapshot alone neither zeroes nor clears responsibility."""
    key: PositionKey = _ledger_key()
    scope = _ledger_scope(key)
    boundary_at = _ledger_time(2)
    journal = AccountJournal(key, stream_scope=scope)
    journal.append_fill(_ledger_fill(key, "open-1", "BUY", "2", "100", _ledger_time(1)))
    journal.record_boundary(
        ExitOrderSubmissionFact(
            order_id="ex-exit-1",
            submitted_at=boundary_at,
            symbol=key.symbol,
            position_side=key.position_side,
            client_order_id="cml-grace-limit-1",
        )
    )
    journal.record_snapshot(
        AccountPositionSnapshot(
            environment=key.environment,
            account_label=key.account_label,
            symbol=key.symbol,
            position_side=key.position_side.value,
            position_amt=Decimal("0"),
            entry_price=Decimal("0"),
            mark_price=Decimal("100"),
            unrealized_pnl=Decimal("0"),
            notional=Decimal("0"),
            leverage=1,
            margin_type="cross",
            observed_at=_ledger_time(3),
            raw_payload={"include_flat": True},
        )
    )

    view = PositionBook(journal).get_view()

    assert view.zero_position_snapshot_confirmed is False, (
        "a single flat snapshot was treated as proof that the exit responsibility ended"
    )
    assert view.is_ready_for_trade is False
    assert view.total_quantity == Decimal("2"), (
        "the flat snapshot alone erased the ledger responsibility"
    )
    assert view.batches[0].exit_order_submitted_at == boundary_at


def test_old_history_and_the_active_view_are_independently_readable() -> None:
    """Older cuts stay readable while the active ledger moves on."""
    key = _ledger_key()
    scope = _ledger_scope(key)
    journal = AccountJournal(key, stream_scope=scope)
    journal.append_fill(_ledger_fill(key, "open-1", "BUY", "2", "100", _ledger_time(1)))
    journal.append_fill(_ledger_fill(key, "open-2", "BUY", "3", "120", _ledger_time(5)))
    book = PositionBook(journal)

    current = book.get_view()
    assert current.total_quantity == Decimal("5")
    # A same-batch add-on weights the real quantity, never a simple average.
    assert current.batches[0].entry_price == Decimal("112")
    assert current.batches[0].original_quantity == Decimal("5")

    historical = book.get_view(_ledger_time(2))
    assert historical.total_quantity == Decimal("2")
    assert len(historical.batches) == 1
    assert historical.batches[0].entry_price == Decimal("100")
    assert historical.batches[0].batch_id == current.batches[0].batch_id

    # Reading old history does not disturb the active responsibility.
    assert book.get_view().total_quantity == Decimal("5")


# --------------------------------------------------------------------------
# Row 3: restart keeps batches, cost, order identity and the grace deadline.
# --------------------------------------------------------------------------


def _grace_manager(account_label: str, *, grace_bars: int) -> LiveExitManager:
    return LiveExitManager(
        config=_exit_config(
            PositionExitMode.CANDLE_15M,
            candle_grace_bars=grace_bars,
            candle_grace_profit_pct=Decimal("0.0088"),
            candle_grace_decision_profit_pct=Decimal("0.0088"),
            account_label=account_label,
        )
    )


async def test_restart_with_open_position_keeps_batches_and_grace_deadline(
    async_database_url: str,
) -> None:
    """A restart keeps batches, cost, and the ORIGINAL grace deadline."""
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"exit-restart-{uuid4().hex}"
    scope = ExecutionScope("live", account, "BTCUSDT", FuturesPositionSide.BOTH)
    opened_at = _evidence(account, "1", "2").observed_at
    exit_submitted_at = opened_at + timedelta(minutes=15)
    grace_bars = 8
    deadline = exit_submitted_at + timedelta(minutes=15 * grace_bars)
    try:
        book = _book(factory)
        await book.restore(account_label=account)
        await book.observe(_evidence(account, "1", "2"))
        before = await book.read(scope)
        assert len(before.batches) == 1
        batch_id = before.batches[0].batch_id
        assert before.batches[0].entry_price == Decimal("100")
        assert before.batches[0].quantity == Decimal("2")

        await book.observe(
            ExecutionEvidence(
                evidence_id="exit-boundary-1",
                scope=scope,
                observed_at=exit_submitted_at,
                boundary=ExitOrderSubmissionFact(
                    order_id="ex-exit-1",
                    submitted_at=exit_submitted_at,
                    symbol="BTCUSDT",
                    position_side=FuturesPositionSide.BOTH,
                    client_order_id="cml-grace-limit-1",
                    target_batch_id=batch_id,
                ),
                stream_id="trades",
                stream_epoch="one",
                sequence=2,
            )
        )
        submitted = await book.read(scope)
        assert submitted.batches[0].exit_order_submitted_at == exit_submitted_at

        # Restart at the grace deadline: a brand new Book instance rebuilds from
        # PostgreSQL only, using the wall clock of the restart.
        restarted = _book(factory)
        await restarted.restore(
            account_label=account, as_of=deadline + timedelta(minutes=1)
        )
        after = await restarted.read(scope)

        assert len(after.batches) == 1
        assert after.batches[0].batch_id == batch_id
        assert after.batches[0].entry_price == Decimal("100")
        assert after.batches[0].quantity == Decimal("2")
        assert after.batches[0].exit_order_submitted_at == exit_submitted_at, (
            "the restart lost or reset the original grace deadline"
        )
        assert after.projection_version == submitted.projection_version

        positions = managed_live_positions_from_views((after,))
        assert len(positions) == 1
        position = positions[0]
        assert position.quantity == Decimal("2")
        assert position.entry_price == Decimal("100")
        assert position.batches[0].batch_id == batch_id

        manager = _grace_manager(account, grace_bars=grace_bars)
        state = replace(
            _state(),
            bucket_end=deadline,
            last_bid_price=Decimal("99"),
            mark_price=Decimal("99"),
            close_price=Decimal("99"),
        )

        early = await manager.requests_for_grace_timeout(
            now=deadline - timedelta(seconds=1),
            state=state,
            positions=positions,
        )
        assert early == (), (
            "the restart shifted the original grace deadline: an exit was built "
            "before the persisted deadline"
        )

        on_time = await manager.requests_for_grace_timeout(
            now=deadline,
            state=state,
            positions=positions,
        )
        assert len(on_time) == 1
        assert on_time[0].quantity == Decimal("2")
        assert on_time[0].candidate.reduce_only is True
        assert on_time[0].candidate.reason == "candle_15m_grace_timeout_8"

        retry = await manager.requests_for_grace_timeout(
            now=deadline + timedelta(minutes=5),
            state=state,
            positions=positions,
        )
        assert len(retry) == 1
        assert retry[0].candidate.candidate_id == on_time[0].candidate.candidate_id, (
            "a grace-timeout retry produced a different order identity after the "
            "restart"
        )
        assert retry[0].candidate.signal_id == on_time[0].candidate.signal_id
    finally:
        await engine.dispose()


async def test_grace_retry_keeps_the_original_boundary_without_a_database() -> None:
    """The rebuilt fallback identity follows the durable boundary, not ``now``."""
    key = _ledger_key()
    scope = _ledger_scope(key)
    boundary_at = _ledger_time(2)
    journal = AccountJournal(key, stream_scope=scope)
    journal.append_fill(_ledger_fill(key, "open-1", "BUY", "2", "100", _ledger_time(1)))
    journal.record_boundary(
        ExitOrderSubmissionFact(
            order_id="ex-exit-1",
            submitted_at=boundary_at,
            symbol=key.symbol,
            position_side=key.position_side,
            client_order_id="cml-grace-limit-1",
        )
    )
    view = PositionBook(journal).get_view()
    positions = managed_live_positions_from_views((view,))
    assert len(positions) == 1
    batch_views = positions[0].batch_views()
    assert len(batch_views) == 1
    assert batch_views[0].recovery_exit_started_at == boundary_at

    grace_bars = 8
    deadline = boundary_at + timedelta(minutes=15 * grace_bars)
    manager = _grace_manager("primary", grace_bars=grace_bars)
    state = replace(
        _state(),
        bucket_end=deadline,
        last_bid_price=Decimal("99"),
        mark_price=Decimal("99"),
        close_price=Decimal("99"),
    )

    assert (
        await manager.requests_for_grace_timeout(
            now=deadline - timedelta(minutes=1),
            state=state,
            positions=positions,
        )
        == ()
    )

    first = await manager.requests_for_grace_timeout(
        now=deadline, state=state, positions=positions
    )
    second = await manager.requests_for_grace_timeout(
        now=deadline + timedelta(minutes=30), state=state, positions=positions
    )
    assert len(first) == 1 and len(second) == 1
    assert first[0].candidate.candidate_id == second[0].candidate.candidate_id
    assert first[0].quantity == second[0].quantity == Decimal("2")


async def test_incompatible_durable_state_stops_restore_instead_of_wiping_it(
    async_database_url: str,
) -> None:
    """A durable format this version cannot read must halt, not reset, state."""
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"exit-incompatible-{uuid4().hex}"
    scope = ExecutionScope("live", account, "BTCUSDT", FuturesPositionSide.BOTH)
    try:
        book = _book(factory)
        await book.restore(account_label=account)
        await book.observe(_evidence(account, "1", "2"))
        assert (await book.read(scope)).total_quantity == Decimal("2")

        # A newer, incompatible writer stored an unknown head format version.
        async with factory() as session, session.begin():
            head = (
                await session.execute(
                    select(ExecutionBookHeadRow).where(
                        ExecutionBookHeadRow.account_label == account
                    )
                )
            ).scalar_one()
            payload = dict(head.state_payload)
            payload["schema_version"] = 99
            await session.execute(
                update(ExecutionBookHeadRow)
                .where(ExecutionBookHeadRow.account_label == account)
                .values(state_payload=payload)
            )

        with pytest.raises(RuntimeError, match="malformed"):
            await _book(factory).restore(account_label=account)

        async with factory() as session:
            preserved_head = (
                await session.execute(
                    select(ExecutionBookHeadRow).where(
                        ExecutionBookHeadRow.account_label == account
                    )
                )
            ).scalar_one()
            assert preserved_head.state_payload["schema_version"] == 99
            assert preserved_head.revision >= 1
            assert preserved_head.state_payload["facts_hash"]
            facts = (
                (
                    await session.execute(
                        select(PositionFactJournalEventRow).where(
                            PositionFactJournalEventRow.account_label == account
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert facts, "the incompatible restore wiped the durable facts"
    finally:
        await engine.dispose()
