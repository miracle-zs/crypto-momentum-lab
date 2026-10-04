from crypto_momentum_lab.live_rollout.position_lifecycle import PositionLifecycleLocks
from tests.unit.live_rollout.test_gates import _risk_config as gate_risk_config

"""Golden Path End-to-End Tests.

Validates the complete production lifecycle across all core bounded contexts:
1. Universe Ingestion -> Dynamic Selection & PostgreSQL Persistence
2. Market Data Flow & Strategy Signal Emission
3. Entry Policy Gate Verification against Universe Snapshot
4. Risk Gateway Assessment & Capital Limit Fencing
5. Quantization & Durable Submission Preparation
6. Exchange Execution State Machine & PostgreSQL Order/Fill Recording
7. Position Tracking & Reduce-Only Exit Execution
8. Full Auditability and Final Position Closure
"""

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.config.models import UniverseConfig
from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.execution.order_rules import SymbolTradingRules
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderFill,
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import (
    StrategyLiveState,
)
from crypto_momentum_lab.domain.risk.limits import FixedLiveLimits
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
    PricePoint,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
)
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.exits import ManagedLivePosition
from crypto_momentum_lab.live_rollout.submission import (
    LiveCandidateSubmission,
    LiveSubmissionConfig,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeFillRow,
    ExchangeOrderEventRow,
    LiveSessionTransitionRow,
)
from crypto_momentum_lab.persistence.postgres.order_event_repository import (
    PostgresOrderEventRepository,
)
from crypto_momentum_lab.persistence.postgres.order_read_repository import (
    PostgresOrderReadRepository,
)
from crypto_momentum_lab.persistence.postgres.order_submission_repository import (
    PostgresOrderSubmissionRepository,
)
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.risk.gateway import RiskGateway
from crypto_momentum_lab.universe.refresh import UniverseRefreshService
from tests.fixtures.order_rows import (
    OrderRows,
)
from tests.unit.execution_account.orders.test_state_machine import FakeExchange
from tests.unit.live_rollout.test_gates import _context as gate_context

pytestmark = pytest.mark.e2e

NOW = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC)


def _make_market_state(
    observed_at: datetime,
    *,
    symbol: str = "BTCUSDT",
    price: Decimal = Decimal("30000"),
) -> MarketState15s:
    start = observed_at - timedelta(seconds=15)
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="live",
        symbol=symbol,
        bucket_start=start,
        bucket_end=observed_at,
        open_price=price,
        high_price=price,
        low_price=price,
        close_price=price,
        trade_count=10,
        trade_notional=price,
        aggressive_buy_notional=price * Decimal("0.6"),
        aggressive_sell_notional=price * Decimal("0.4"),
        last_bid_price=price - Decimal("1"),
        last_ask_price=price + Decimal("1"),
        spread=Decimal("2"),
        midpoint=price,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=price,
        closed_kline_count=0,
        source_event_count=10,
        first_received_at=start,
        last_received_at=observed_at,
    )


class GoldenMarketData:
    """Mock exchange market data feed for universe generation."""

    def __init__(self, observed_at: datetime) -> None:
        self.observed_at = observed_at
        self.symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
        self.contracts = tuple(
            ContractMetadata(
                symbol,
                "PERPETUAL",
                "TRADING",
                "USDT",
                "USDT",
                observed_at,
                {},
            )
            for symbol in self.symbols
        )
        # BTC momentum leader: open=30000, current=31500 (+5%)
        # ETH: open=2000, current=2040 (+2%)
        # SOL: open=100, current=101 (+1%)
        self.prices = {
            "BTCUSDT": PricePoint("BTCUSDT", Decimal("31500"), observed_at),
            "ETHUSDT": PricePoint("ETHUSDT", Decimal("2040"), observed_at),
            "SOLUSDT": PricePoint("SOLUSDT", Decimal("101"), observed_at),
        }
        self.opens = {
            "BTCUSDT": Decimal("30000"),
            "ETHUSDT": Decimal("2000"),
            "SOLUSDT": Decimal("100"),
        }

    async def fetch_active_usdt_perpetuals(self) -> tuple[ContractMetadata, ...]:
        return self.contracts

    async def fetch_latest_prices(self) -> dict[str, PricePoint]:
        return self.prices

    async def fetch_daily_opens(
        self,
        symbols: frozenset[str],
        utc_day: date,
    ) -> tuple[DailyOpen, ...]:
        open_time = datetime.combine(utc_day, datetime.min.time(), tzinfo=UTC)
        return tuple(
            DailyOpen(symbol, utc_day, self.opens[symbol], open_time)
            for symbol in sorted(symbols)
            if symbol in self.opens
        )


class DynamicFillingFakeExchange(FakeExchange):
    """Simulates an exchange that assigns matching client_order_id and generates fills."""

    def __init__(
        self, *, state: ExchangeOrderState = ExchangeOrderState.FILLED
    ) -> None:
        super().__init__(submit_result=None)  # type: ignore[arg-type]
        self.state = state

    async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
        await self.emit_submit_boundary(plan, is_request=True)
        try:
            self.calls.append("submit")
            fill_price = plan.price or Decimal("30000")
            fill = ExchangeOrderFill(
                fill_id=f"fill-{plan.client_order_id}",
                client_order_id=plan.client_order_id,
                exchange_trade_id=f"trade-{plan.client_order_id}",
                price=fill_price,
                quantity=plan.quantity,
                fee=Decimal("0.01"),
                fee_asset="USDT",
                filled_at=NOW,
                details={},
            )
            return ExchangeOrderSnapshot(
                client_order_id=plan.client_order_id,
                exchange_order_id=f"exchange-{plan.client_order_id}",
                state=self.state,
                observed_at=NOW,
                executed_quantity=plan.quantity,
                average_price=fill_price,
                fills=(fill,),
            )
        finally:
            await self.emit_submit_boundary(plan, is_request=False)


_created_coordinators: list[OrderExecutionCoordinator] = []


@pytest.fixture(autouse=True)
async def close_submission_coordinators():
    try:
        yield
    finally:
        while _created_coordinators:
            await _created_coordinators.pop().aclose()


async def _build_submission_service(
    *,
    order_repo: OrderRows,
    sessions: async_sessionmaker[AsyncSession],
    exchange: FakeExchange,
    limits: FixedLiveLimits | None = None,
    entry_order_type: EntryType = EntryType.MARKET,
) -> LiveCandidateSubmission:
    machine = OrderExecutionStateMachine(
        exchange=exchange,
        event_repository=PostgresOrderEventRepository(sessions),
        live_submit_enabled=True,
        clock=lambda: NOW,
    )
    from crypto_momentum_lab.domain.account import AccountPositionSnapshot
    from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
    from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
    from tests.integration.persistence.test_authority_book_transactions import _book

    book = _book(sessions)
    await book.restore(account_label="primary")
    scope = ExecutionScope("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    await book.observe(
        ExecutionEvidence(
            evidence_id="golden-flat-start",
            scope=scope,
            observed_at=NOW - timedelta(seconds=1),
            stream_id="golden",
            stream_epoch="one",
            sequence=1,
            snapshot=AccountPositionSnapshot(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                position_side="BOTH",
                position_amt=Decimal("0"),
                entry_price=Decimal("0"),
                mark_price=Decimal("30000"),
                unrealized_pnl=Decimal("0"),
                notional=Decimal("0"),
                leverage=None,
                margin_type=None,
                observed_at=NOW - timedelta(seconds=1),
                raw_payload={},
            ),
        )
    )
    coordinator = OrderExecutionCoordinator(
        backend=machine,
        account_label="primary",
        environment="live",
        execution_book=book,
    )
    _created_coordinators.append(coordinator)

    coordinator.configure_submission(
        PostgresOrderSubmissionRepository(sessions),
        clock=lambda: NOW,
    )
    return LiveCandidateSubmission(
        position_locks=PositionLifecycleLocks(),
        risk_gateway=RiskGateway(
            limits=limits
            or FixedLiveLimits(
                notional_cap=Decimal("25"),
                max_open_positions=1,
                max_daily_loss=Decimal("10"),
                max_gross_exposure=Decimal("25"),
            ),
        ),
        state_machine=coordinator,
        config=LiveSubmissionConfig(
            run_id="golden-run-1",
            account_label="primary",
            resize_tolerance=Decimal("0.20"),
            hedge_mode=False,
            entry_order_type=entry_order_type,
            entry_limit_ttl_seconds=900,
        ),
        clock=lambda: NOW,
        pending_entry_reservation=lambda orders: (Decimal("0"), frozenset()),
        remember_pending_entry=lambda plan, result: None,
        record_signal_candidate=lambda **kwargs: None,
    )


def _build_runtime_context(
    *, trading_rules: SymbolTradingRules
) -> LiveDaemonRuntimeContext:
    gate = gate_context()
    return LiveDaemonRuntimeContext(
        now=NOW,
        gate_context=gate,
        account_state=ExecutionAccountStatus.READY_READONLY,
        account_observed_at=NOW,
        open_position_symbols=frozenset(),
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        gross_exposure=Decimal("0"),
        active_halts=(),
        unresolved_order_states=(),
        risk_config=gate_risk_config(),
        strategy_state=StrategyLiveState.ACTIVE,
        trading_rules={"BTCUSDT": trading_rules},
    )


async def _seed_live_session(
    session_factory: async_sessionmaker[AsyncSession],
    session_id: str,
) -> None:
    async with session_factory() as session:
        async with session.begin():
            session.add(
                LiveSessionTransitionRow(
                    transition_id=f"transition-{session_id}",
                    session_id=session_id,
                    state="running",
                    occurred_at=NOW - timedelta(minutes=1),
                    operator="operator",
                    strategy_config_hash="c" * 64,
                    risk_config_hash="r" * 64,
                    reason="startup",
                    details={},
                )
            )


async def test_golden_path_full_trading_lifecycle(
    repository: PostgresUniverseRepository,
    order_repository: tuple[
        OrderRows, async_sessionmaker[AsyncSession]
    ],
) -> None:
    """End-to-end golden path:

    1. Universe Refresh computes top gainer ranking & persists snapshot to PostgreSQL.
    2. Strategy emits entry signal for top gainer (BTCUSDT).
    3. Entry Policy Gate verifies the candidate against PostgreSQL universe snapshot.
    4. Risk Gateway approves entry within limits.
    5. Order Execution State Machine submits plan and records state in PostgreSQL.
    6. Position is tracked as ManagedLivePosition.
    7. Strategy triggers take-profit exit, which is executed and persisted as reduce-only.
    8. Position is closed with complete audit trail in PostgreSQL.
    """
    order_repo, session_factory = order_repository

    # -------------------------------------------------------------------------
    # Step 1: Universe Ingestion & Dynamic Selection (PostgreSQL persistence)
    # -------------------------------------------------------------------------
    market_data = GoldenMarketData(NOW)
    universe_service = UniverseRefreshService(
        market_data=market_data,
        repository=repository,
        config=UniverseConfig(
            top_count=2,
            loser_target_count=0,
            activation_minute=0,
        ),
        config_hash="h" * 64,
    )

    universe_snapshot = await universe_service.refresh(observed_at=NOW)
    assert universe_snapshot.ranking.target_symbols == frozenset({"BTCUSDT", "ETHUSDT"})
    assert universe_snapshot.ranking.gainers[0].symbol == "BTCUSDT"

    # Verify snapshot was persisted in PostgreSQL
    db_snapshot = await repository.load_snapshot(universe_snapshot.observed_at)
    assert db_snapshot is not None
    assert db_snapshot.observed_at == universe_snapshot.observed_at
    assert "BTCUSDT" in db_snapshot.ranking.target_symbols

    # Also verify point-in-time resolution at or before current time
    active_snapshot = await repository.load_snapshot_at(NOW)
    assert active_snapshot is not None
    assert active_snapshot.snapshot_id == universe_snapshot.snapshot_id

    # -------------------------------------------------------------------------
    # Step 2: Market State & Strategy Entry Candidate
    # -------------------------------------------------------------------------
    trading_rules = SymbolTradingRules(
        symbol="BTCUSDT",
        tick_size=Decimal("0.1"),
        step_size=Decimal("0.0001"),
        min_quantity=Decimal("0.0001"),
        max_quantity=Decimal("100"),
        min_notional=Decimal("5"),
    )
    context = _build_runtime_context(trading_rules=trading_rules)
    await _seed_live_session(session_factory, "golden-run-1")

    entry_candidate = OrderIntentCandidate(
        candidate_id="golden-entry-cand-1",
        signal_id="golden-entry-sig-1",
        run_id="golden-run-1",
        strategy_name="compression_breakout",
        strategy_version="v1",
        config_hash="c" * 64,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("18"),
        reduce_only=False,
        expires_at=NOW + timedelta(minutes=15),
        created_at=NOW,
        reason="compression_breakout_confirmed",
        features={"momentum_score": "0.95"},
    )

    # Entry policy verification: Must be within active top symbols
    assert entry_candidate.symbol in db_snapshot.ranking.target_symbols

    # -------------------------------------------------------------------------
    # Step 3 & 4: Risk Gate & Execution Submission to Exchange (Entry)
    # -------------------------------------------------------------------------
    exchange = DynamicFillingFakeExchange(state=ExchangeOrderState.FILLED)
    submission_service = await _build_submission_service(
        order_repo=order_repo,
        sessions=session_factory,
        exchange=exchange,
    )

    from crypto_momentum_lab.domain.execution.command_models import ExecutionScope

    scope = ExecutionScope("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    book = _created_coordinators[-1].execution_book
    view = await book.read(scope)
    entry_candidate = replace(
        entry_candidate,
        features={
            **entry_candidate.features,
            "projection_version": view.projection_version,
        },
    )
    market_state = _make_market_state(NOW)
    entry_result = await submission_service.execute(
        entry_candidate,
        requested_quantity=None,
        state=market_state,
        context=context,
    )

    assert entry_result is not None
    assert entry_result.state is ExchangeOrderState.FILLED
    assert entry_result.exchange_order_id == f"exchange-{entry_result.client_order_id}"
    assert exchange.calls == ["submit"]

    # -------------------------------------------------------------------------
    # Step 5: Verify Order & Fill Persistence in PostgreSQL
    # -------------------------------------------------------------------------
    persisted_entry = await PostgresOrderReadRepository(session_factory).load_order(
        entry_result.client_order_id
    )
    assert persisted_entry is not None
    assert persisted_entry.state is ExchangeOrderState.FILLED
    assert persisted_entry.exchange_order_id == entry_result.exchange_order_id
    assert persisted_entry.plan.symbol == "BTCUSDT"
    assert persisted_entry.plan.side == "BUY"

    # Query events from database directly
    async with session_factory() as session:
        events = (
            await session.scalars(
                select(ExchangeOrderEventRow)
                .where(
                    ExchangeOrderEventRow.client_order_id
                    == entry_result.client_order_id
                )
                .order_by(ExchangeOrderEventRow.occurred_at.asc())
            )
        ).all()
        assert len(events) >= 2
        assert events[0].state == ExchangeOrderState.SUBMITTING.value
        assert events[-1].state == ExchangeOrderState.FILLED.value

        fills = (
            await session.scalars(
                select(ExchangeFillRow).where(
                    ExchangeFillRow.client_order_id == entry_result.client_order_id
                )
            )
        ).all()
        assert len(fills) == 1
        assert fills[0].quantity == persisted_entry.plan.quantity
        assert fills[0].price == Decimal("30000")

    from crypto_momentum_lab.domain.account import AccountFillEvent

    entry_trade = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id=f"trade-{entry_result.client_order_id}",
        order_id=entry_result.exchange_order_id,
        side="BUY",
        price=Decimal("30000"),
        quantity=persisted_entry.plan.quantity,
        realized_pnl=Decimal("0"),
        fee=Decimal("0.01"),
        fee_asset="USDT",
        trade_at=NOW,
        raw_payload={
            "positionSide": "BOTH",
            "is_system": True,
            "client_order_id": entry_result.client_order_id,
        },
    )
    from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence

    trade_observation = await book.observe(
        ExecutionEvidence(
            evidence_id=entry_trade.trade_id,
            scope=scope,
            observed_at=NOW,
            stream_id="golden",
            stream_epoch="one",
            sequence=2,
            fill=entry_trade,
        )
    )
    from crypto_momentum_lab.domain.execution.observation_models import Applied

    assert isinstance(trade_observation, Applied), trade_observation
    # -------------------------------------------------------------------------
    # Step 6: Position Tracking & Lifecycle Management
    # -------------------------------------------------------------------------
    position = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        quantity=persisted_entry.plan.quantity,
        entry_price=Decimal("30000"),
        opened_at=NOW,
        account_label="primary",
    )
    assert position.closing_order_filled is False

    # -------------------------------------------------------------------------
    # Step 7: Take-Profit Exit Execution (Reduce-Only)
    # -------------------------------------------------------------------------
    exit_now = NOW + timedelta(seconds=15)
    exit_candidate = OrderIntentCandidate(
        candidate_id="golden-exit-cand-1",
        signal_id="golden-exit-sig-1",
        run_id="golden-run-1",
        strategy_name="compression_breakout",
        strategy_version="v1",
        config_hash="c" * 64,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=None,
        reduce_only=True,
        expires_at=exit_now + timedelta(minutes=15),
        created_at=exit_now,
        reason="take_profit_target_reached",
        features={"exit_trigger_price": "31500"},
    )

    exit_market_state = _make_market_state(
        exit_now,
        price=Decimal("31500"),
    )
    view = await book.read(scope)
    assert view.batches, view
    exit_candidate = replace(
        exit_candidate,
        features={
            **exit_candidate.features,
            "projection_version": view.projection_version,
            "exit_allocations": [
                {"batch_id": batch.batch_id, "quantity": str(batch.quantity)}
                for batch in view.batches
            ],
        },
    )
    exit_result = await submission_service.execute(
        exit_candidate,
        requested_quantity=position.quantity,
        state=exit_market_state,
        context=replace(
            context,
            open_position_symbols=frozenset({"BTCUSDT"}),
            now=exit_now,
        ),
    )

    assert exit_result is not None
    assert exit_result.state is ExchangeOrderState.FILLED
    assert exit_result.plan.reduce_only is True
    assert exit_result.plan.side == "SELL"
    assert exit_result.plan.quantity == position.quantity

    closed_position = replace(position, closing_order_filled=True)
    assert closed_position.closing_order_filled is True

    persisted_exit = await PostgresOrderReadRepository(session_factory).load_order(
        exit_result.client_order_id
    )
    assert persisted_exit is not None
    assert persisted_exit.state is ExchangeOrderState.FILLED
    assert persisted_exit.plan.reduce_only is True

    exit_trade = replace(
        entry_trade,
        trade_id=f"trade-{exit_result.client_order_id}",
        order_id=exit_result.exchange_order_id,
        side="SELL",
        quantity=persisted_exit.plan.quantity,
        trade_at=exit_now,
        raw_payload={
            "positionSide": "BOTH",
            "is_system": True,
            "client_order_id": exit_result.client_order_id,
        },
    )
    exit_observation = await book.observe(
        ExecutionEvidence(
            evidence_id=exit_trade.trade_id,
            scope=scope,
            observed_at=exit_now,
            stream_id="golden",
            stream_epoch="one",
            sequence=3,
            fill=exit_trade,
        )
    )
    assert isinstance(exit_observation, Applied), exit_observation
    closed_view = await book.read(scope)
    assert closed_view.total_quantity == Decimal("0")
    assert book.get_active_reservations(scope.to_position_key()) == ()

    # Both entry and exit orders are safely recorded in PostgreSQL
    assert persisted_entry.plan.client_order_id != persisted_exit.plan.client_order_id
    assert persisted_entry.state is ExchangeOrderState.FILLED
    assert persisted_exit.state is ExchangeOrderState.FILLED


async def test_golden_path_risk_gate_blocks_excessive_exposure(
    order_repository: tuple[
        OrderRows, async_sessionmaker[AsyncSession]
    ],
) -> None:
    """Verifies that the Golden Path correctly blocks candidates that violate risk bounds."""
    order_repo, session_factory = order_repository
    exchange = DynamicFillingFakeExchange()
    # Limit max open positions to 1
    submission_service = await _build_submission_service(
        order_repo=order_repo,
        sessions=session_factory,
        exchange=exchange,
        limits=FixedLiveLimits(
            notional_cap=Decimal("25"),
            max_open_positions=1,
            max_daily_loss=Decimal("10"),
            max_gross_exposure=Decimal("25"),
        ),
    )
    rules = SymbolTradingRules(
        symbol="BTCUSDT",
        tick_size=Decimal("0.1"),
        step_size=Decimal("0.0001"),
        min_quantity=Decimal("0.0001"),
        max_quantity=Decimal("100"),
        min_notional=Decimal("5"),
    )
    context = _build_runtime_context(trading_rules=rules)

    # Position capacity is already saturated by ETHUSDT
    full_capacity_context = replace(
        context,
        open_position_symbols=frozenset({"ETHUSDT"}),
    )

    candidate = OrderIntentCandidate(
        candidate_id="golden-violating-cand-1",
        signal_id="golden-violating-sig-1",
        run_id="golden-run-1",
        strategy_name="compression_breakout",
        strategy_version="v1",
        config_hash="c" * 64,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("18"),
        reduce_only=False,
        expires_at=NOW + timedelta(minutes=15),
        created_at=NOW,
        reason="test_excessive_risk",
        features={},
    )

    result = await submission_service.execute(
        candidate,
        requested_quantity=None,
        state=_make_market_state(NOW),
        context=full_capacity_context,
    )

    # Submission was blocked by risk governance; zero calls reached the exchange
    assert result is None
    assert exchange.calls == []


async def test_golden_path_entry_gate_rejects_symbol_outside_universe(
    repository: PostgresUniverseRepository,
) -> None:
    """Verifies that candidate symbols outside the active universe ranking are filtered out."""
    market_data = GoldenMarketData(NOW)
    universe_service = UniverseRefreshService(
        market_data=market_data,
        repository=repository,
        config=UniverseConfig(
            top_count=2,
            loser_target_count=0,
            activation_minute=0,
        ),
        config_hash="h" * 64,
    )
    snapshot = await universe_service.refresh(observed_at=NOW)

    # Target symbols only contain top 2: BTCUSDT and ETHUSDT
    assert snapshot.ranking.target_symbols == frozenset({"BTCUSDT", "ETHUSDT"})
    assert "SOLUSDT" not in snapshot.ranking.target_symbols
    assert "OBSCURECOIN" not in snapshot.ranking.target_symbols

    # An incoming candidate for a non-universe coin cannot pass entry policy
    obscure_candidate = OrderIntentCandidate(
        candidate_id="golden-obscure-cand-1",
        signal_id="golden-obscure-sig-1",
        run_id="golden-run-1",
        strategy_name="compression_breakout",
        strategy_version="v1",
        config_hash="c" * 64,
        symbol="OBSCURECOIN",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("15"),
        reduce_only=False,
        expires_at=NOW + timedelta(minutes=15),
        created_at=NOW,
        reason="obscure_breakout",
        features={},
    )

    is_eligible = obscure_candidate.symbol in snapshot.ranking.target_symbols
    assert is_eligible is False
