import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.live_rollout import (
    LIVE_APPROVAL_CONFIRMATION,
    LiveOperatorApproval,
)
from crypto_momentum_lab.domain.risk import RiskConfigSnapshot, StrategyLiveState
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.execution_account.orders.quantization import (
    SymbolTradingRules,
)
from crypto_momentum_lab.live_rollout.daemon import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.postgres_runtime import (
    PostgresLiveContextProvider,
    _classify_live_positions,
    _resolve_strategy_live_state,
    live_limits_from_approval,
    poll_live_market_states,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    RuntimeStateCursor,
)
from tests.unit.live_rollout.test_gates import _context as gate_context
from tests.unit.shadow_operation.test_service import _context as shadow_context

NOW = datetime(2026, 8, 4, 0, 0, tzinfo=UTC)


def test_classifies_position_opened_by_current_run_as_managed() -> None:
    managed, unmanaged = _classify_live_positions(
        [_position()],
        [_order(reduce_only=False, side="BUY")],
    )

    assert unmanaged == frozenset()
    assert len(managed) == 1
    assert managed[0].side is StrategySide.LONG
    assert managed[0].quantity == Decimal("0.5")
    assert managed[0].closing_order_filled is False


def test_marks_external_position_as_unmanaged() -> None:
    managed, unmanaged = _classify_live_positions([_position()], [])

    assert managed == ()
    assert unmanaged == frozenset({"BTCUSDT"})


def test_filled_close_suppresses_duplicate_exit_during_account_sync_lag() -> None:
    managed, unmanaged = _classify_live_positions(
        [_position()],
        [
            _order(
                reduce_only=True,
                side="SELL",
                updated_at=NOW + timedelta(seconds=10),
            ),
            _order(reduce_only=False, side="BUY"),
        ],
    )

    assert unmanaged == frozenset()
    assert managed[0].closing_order_filled is True


def test_prior_exit_does_not_suppress_a_reopened_position() -> None:
    old_entry_at = NOW
    new_entry_at = NOW + timedelta(seconds=20)
    old_exit_created_at = NOW + timedelta(seconds=10)
    old_exit_filled_at = NOW + timedelta(seconds=30)

    managed, unmanaged = _classify_live_positions(
        [_position()],
        [
            _order(
                reduce_only=True,
                side="SELL",
                created_at=old_exit_created_at,
                updated_at=old_exit_filled_at,
            ),
            _order(
                reduce_only=False,
                side="BUY",
                created_at=new_entry_at,
                updated_at=new_entry_at,
            ),
            _order(
                reduce_only=False,
                side="BUY",
                created_at=old_entry_at,
                updated_at=old_entry_at,
            ),
        ],
    )

    assert unmanaged == frozenset()
    assert managed[0].opened_at == new_entry_at
    assert managed[0].closing_order_filled is False


def test_partial_entry_fill_is_managed_and_refreshes_latest_exit_anchor() -> None:
    old_entry_at = NOW
    partial_entry_fill_at = NOW + timedelta(seconds=25)

    managed, unmanaged = _classify_live_positions(
        [_position()],
        [
            _order(
                reduce_only=False,
                side="BUY",
                updated_at=old_entry_at,
                state=ExchangeOrderState.FILLED.value,
                exchange_order_id="old-order",
            ),
            _order(
                reduce_only=False,
                side="BUY",
                updated_at=NOW + timedelta(seconds=30),
                state=ExchangeOrderState.PARTIALLY_FILLED.value,
                exchange_order_id="partial-order",
            ),
        ],
        entry_fill_times={
            "old-order": old_entry_at,
            "partial-order": partial_entry_fill_at,
        },
    )

    assert unmanaged == frozenset()
    assert managed[0].opened_at == partial_entry_fill_at
    assert managed[0].closing_order_filled is False


def test_late_fill_after_an_exit_is_a_new_position_episode() -> None:
    pending_entry_created_at = NOW + timedelta(seconds=5)
    pending_entry_fill_at = NOW + timedelta(seconds=25)

    managed, unmanaged = _classify_live_positions(
        [_position()],
        [
            _order(
                reduce_only=True,
                side="SELL",
                created_at=NOW + timedelta(seconds=10),
                updated_at=NOW + timedelta(seconds=15),
            ),
            _order(
                reduce_only=False,
                side="BUY",
                created_at=pending_entry_created_at,
                updated_at=pending_entry_fill_at,
                state=ExchangeOrderState.PARTIALLY_FILLED.value,
                exchange_order_id="late-entry",
            ),
        ],
        entry_fill_times={"late-entry": pending_entry_fill_at},
    )

    assert unmanaged == frozenset()
    assert managed[0].opened_at == pending_entry_fill_at
    assert managed[0].closing_order_filled is False


def test_draining_control_survives_a_later_operational_halt() -> None:
    assert (
        _resolve_strategy_live_state("draining", "halted")
        is StrategyLiveState.DRAINING
    )


def test_live_limits_preserve_unbounded_capacity() -> None:
    risk_config = RiskConfigSnapshot(
        environment="live",
        account_label="primary",
        max_order_notional=None,
        max_gross_notional=None,
        max_daily_loss=None,
        max_open_positions=None,
        max_market_state_age_seconds=30,
        max_account_state_age_seconds=30,
        allow_reduce_only_while_draining=True,
        created_at=NOW,
    )
    approval = LiveOperatorApproval(
        approval_id="approval-1",
        account_label="primary",
        strategy_name="orderflow_impulse",
        strategy_config_hash="a" * 64,
        risk_config_hash=risk_config.config_hash,
        git_commit_hash="abc123",
        database_migration_revision="20260814_0016",
        approved_notional_cap=None,
        approved_max_open_positions=None,
        approved_max_daily_loss=None,
        approver_name="operator",
        approval_text=LIVE_APPROVAL_CONFIRMATION,
        expires_at=None,
        created_at=NOW,
    )

    limits = live_limits_from_approval(
        approval=approval,
        risk_config=risk_config,
    )

    assert limits == (None, None, None, None)


async def test_symbol_rules_use_the_market_session_factory(monkeypatch) -> None:
    execution_sessions = object()
    market_sessions = object()
    provider = object.__new__(PostgresLiveContextProvider)
    provider._sessions = execution_sessions
    provider._market_sessions = market_sessions
    provider._cached_rules = {}
    provider._cached_rules_at = {}
    expected = _runtime_context().trading_rules["BTCUSDT"]
    seen: dict[str, object] = {}

    async def load_trading_rules(sessions, symbols):
        seen["sessions"] = sessions
        seen["symbols"] = symbols
        return {"BTCUSDT": expected}

    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.postgres_runtime._load_trading_rules",
        load_trading_rules,
    )

    result = await provider._load_symbol_rules("BTCUSDT", NOW)

    assert result is expected
    assert seen == {"sessions": market_sessions, "symbols": {"BTCUSDT"}}


async def test_symbol_rule_load_is_single_flight(monkeypatch) -> None:
    provider = object.__new__(PostgresLiveContextProvider)
    provider._sessions = object()
    provider._market_sessions = object()
    provider._cached_rules = {}
    provider._cached_rules_at = {}
    expected = _runtime_context().trading_rules["BTCUSDT"]
    started = asyncio.Event()
    release = asyncio.Event()
    load_count = 0

    async def load_trading_rules(_sessions, _symbols):
        nonlocal load_count
        load_count += 1
        started.set()
        await release.wait()
        return {"BTCUSDT": expected}

    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.postgres_runtime._load_trading_rules",
        load_trading_rules,
    )

    first = asyncio.create_task(provider._load_symbol_rules("BTCUSDT", NOW))
    second = asyncio.create_task(provider._load_symbol_rules("BTCUSDT", NOW))
    await asyncio.wait_for(started.wait(), timeout=1)
    assert load_count == 1
    release.set()

    assert await asyncio.gather(first, second) == [expected, expected]
    assert load_count == 1


def test_context_invalidation_preserves_symbol_rules() -> None:
    provider = object.__new__(PostgresLiveContextProvider)
    expected = _runtime_context().trading_rules["BTCUSDT"]
    provider._cache_epoch = 3
    provider._cached_bucket_start = NOW
    provider._cached_context = _runtime_context()
    provider._cached_rules = {"BTCUSDT": expected}
    provider._cached_rules_at = {"BTCUSDT": NOW}

    provider.invalidate_cache()

    assert provider._cache_epoch == 4
    assert provider._cached_context is None
    assert provider._cached_rules == {"BTCUSDT": expected}
    assert provider._cached_rules_at == {"BTCUSDT": NOW}


async def test_delayed_state_reuses_newer_cached_context(monkeypatch) -> None:
    state = SimpleNamespace(
        symbol="BTCUSDT",
        bucket_start=NOW,
    )
    cached = _runtime_context()
    expected_rule = cached.trading_rules["BTCUSDT"]
    provider = object.__new__(PostgresLiveContextProvider)
    provider._account_label = "primary"
    provider._strategy_name = cached.gate_context.strategy_name
    provider._strategy_config_hash = cached.gate_context.strategy_config_hash
    provider._git_commit_hash = cached.gate_context.git_commit_hash
    provider._migration_revision = cached.gate_context.database_migration_revision
    provider._lease_owner = cached.gate_context.required_lease_owner
    provider._approval_id = cached.gate_context.approval.approval_id
    provider._cached_bucket_start = NOW + timedelta(minutes=1)
    provider._cached_context = cached
    provider._cached_rules = {"BTCUSDT": expected_rule}
    provider._cached_rules_at = {"BTCUSDT": NOW}
    provider._cache_epoch = 0

    full_loads = 0
    rule_loads = 0

    async def full_context_load(**_kwargs):
        nonlocal full_loads
        full_loads += 1
        return cached.gate_context.approval

    class RiskRepository:
        async def load_active_lease(self, *_args):
            nonlocal full_loads
            full_loads += 1
            return cached.active_lease

        async def load_active_halts(self, *_args):
            nonlocal full_loads
            full_loads += 1
            return ()

    async def load_symbol_rules(_symbol, _now):
        nonlocal rule_loads
        rule_loads += 1
        return expected_rule

    async def load_positions():
        nonlocal full_loads
        full_loads += 1
        return (
            (),
            (
                NOW,
                frozenset(),
                Decimal("0"),
                Decimal("0"),
                (),
                frozenset(),
            ),
        )

    async def latest_risk_config(_sessions, _account_label):
        nonlocal full_loads
        full_loads += 1
        return cached.risk_config

    async def latest_account_state(_sessions, _account_label):
        nonlocal full_loads
        full_loads += 1
        return cached.account_state

    async def realized_pnl(_now):
        nonlocal full_loads
        full_loads += 1
        return Decimal("0")

    async def strategy_state():
        nonlocal full_loads
        full_loads += 1
        return cached.strategy_state

    provider._live_repository = SimpleNamespace(
        load_active_approval=full_context_load,
    )
    provider._risk_repository = RiskRepository()
    provider._load_symbol_rules = load_symbol_rules
    provider._load_unresolved_and_position_view = load_positions
    provider._daily_realized_pnl = realized_pnl
    provider._strategy_live_state = strategy_state

    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.postgres_runtime._latest_risk_config",
        latest_risk_config,
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.postgres_runtime._latest_account_state",
        latest_account_state,
    )

    result = await provider(state)

    assert result.trading_rules == {"BTCUSDT": expected_rule}
    assert full_loads == 0
    assert rule_loads == 1


async def test_next_market_bucket_reuses_fresh_context_cache() -> None:
    cached_loaded_at = datetime.now(tz=UTC)
    cached = _runtime_context()
    expected_rule = cached.trading_rules["BTCUSDT"]
    provider = object.__new__(PostgresLiveContextProvider)
    provider._cached_bucket_start = cached_loaded_at
    provider._cached_loaded_at = cached_loaded_at
    provider._cached_context = cached
    provider._cached_rules = {"BTCUSDT": expected_rule}
    provider._cached_rules_at = {"BTCUSDT": cached_loaded_at}
    provider._cache_epoch = 0

    async def load_symbol_rules(_symbol, _now):
        return expected_rule

    provider._load_symbol_rules = load_symbol_rules
    state = SimpleNamespace(
        symbol="BTCUSDT",
        bucket_start=cached_loaded_at + timedelta(seconds=15),
    )

    result = await provider(state)

    assert result.trading_rules == {"BTCUSDT": expected_rule}
    assert result.gate_context.now == result.now


async def test_live_market_poll_skips_a_backlog_to_the_latest_closed_bucket() -> None:
    latest_bucket = NOW + timedelta(minutes=5)
    stale_state = SimpleNamespace(symbol="BTCUSDT", bucket_start=NOW)
    latest_state = SimpleNamespace(symbol="BTCUSDT", bucket_start=latest_bucket)

    class Repository:
        def __init__(self) -> None:
            self.cursors: list[RuntimeStateCursor] = []

        async def load_latest_bucket(self, *, environment: str):
            assert environment == "research"
            return latest_bucket

        async def load_after(self, *, environment, cursor, limit):
            assert environment == "research"
            assert limit == 500
            self.cursors.append(cursor)
            if cursor.bucket_start == latest_bucket - timedelta(microseconds=1):
                return (latest_state,)
            return (stale_state,)

    repository = Repository()
    observed = []
    async for state in poll_live_market_states(
        repository=repository,
        environment="research",
        max_runtime_seconds=1,
        poll_interval_seconds=0.01,
        cursor=RuntimeStateCursor(bucket_start=NOW, symbol=""),
        max_state_lag_seconds=45,
    ):
        observed.append(state)
        break

    assert observed == [latest_state]
    assert repository.cursors == [
        RuntimeStateCursor(
            bucket_start=latest_bucket - timedelta(microseconds=1),
            symbol="",
        )
    ]


async def test_context_reload_survives_cache_invalidation_during_rule_load(
    monkeypatch,
) -> None:
    state = SimpleNamespace(symbol="BTCUSDT", bucket_start=NOW)
    cached = _runtime_context()
    provider = object.__new__(PostgresLiveContextProvider)
    provider._account_label = "primary"
    provider._strategy_name = cached.gate_context.strategy_name
    provider._strategy_config_hash = cached.gate_context.strategy_config_hash
    provider._git_commit_hash = cached.gate_context.git_commit_hash
    provider._migration_revision = cached.gate_context.database_migration_revision
    provider._lease_owner = cached.gate_context.required_lease_owner
    provider._approval_id = cached.gate_context.approval.approval_id
    provider._lease_ttl_seconds = 300
    provider._lease_renew_before_seconds = 120
    provider._cached_bucket_start = state.bucket_start
    provider._cached_context = cached
    provider._cached_rules = {}
    provider._cached_rules_at = {}
    provider._sessions = object()

    class LiveRepository:
        async def load_active_approval(self, **_kwargs):
            return cached.gate_context.approval

    class RiskRepository:
        async def load_active_lease(self, *_args):
            return cached.active_lease

        async def load_active_halts(self, *_args):
            return ()

    provider._live_repository = LiveRepository()
    provider._risk_repository = RiskRepository()

    async def latest_risk_config(_sessions, _account_label):
        return cached.risk_config

    async def latest_account_state(_sessions, _account_label):
        return cached.account_state

    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.postgres_runtime._latest_risk_config",
        latest_risk_config,
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.postgres_runtime._latest_account_state",
        latest_account_state,
    )

    rule_loads = 0

    async def load_symbol_rules(_symbol, _now):
        nonlocal rule_loads
        rule_loads += 1
        if rule_loads == 1:
            provider.invalidate_cache()
        return cached.trading_rules["BTCUSDT"]

    async def load_positions():
        return (
            (),
            (
                NOW,
                frozenset(),
                Decimal("0"),
                Decimal("0"),
                (),
                frozenset(),
            ),
        )

    provider._load_symbol_rules = load_symbol_rules
    provider._load_unresolved_and_position_view = load_positions
    provider._daily_realized_pnl = lambda _now: _realized_pnl()
    provider._strategy_live_state = lambda: _strategy_state()

    result = await provider(state)

    assert result.account_state is cached.account_state
    assert result.trading_rules == cached.trading_rules
    assert rule_loads == 2


async def _realized_pnl() -> Decimal:
    return Decimal("0")


async def _strategy_state() -> StrategyLiveState:
    return StrategyLiveState.ACTIVE


def _runtime_context() -> LiveDaemonRuntimeContext:
    gate = gate_context()
    shadow = shadow_context(now=NOW)
    rules = {
        "BTCUSDT": SymbolTradingRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            max_quantity=Decimal("100"),
            min_notional=Decimal("5"),
        )
    }
    return LiveDaemonRuntimeContext(
        now=NOW,
        gate_context=gate,
        active_lease=gate.active_lease,
        account_state=gate.account_state,
        account_observed_at=NOW,
        open_position_symbols=frozenset(),
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        gross_exposure=Decimal("0"),
        active_halts=(),
        unresolved_order_states=(),
        risk_config=gate.risk_config,
        strategy_state=shadow.strategy_state,
        trading_rules=rules,
    )


def _position():
    return SimpleNamespace(
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("0.5"),
        entry_price=Decimal("100"),
    )


def _order(
    *,
    reduce_only: bool,
    side: str,
    updated_at: datetime = NOW,
    created_at: datetime | None = None,
    state: str = ExchangeOrderState.FILLED.value,
    exchange_order_id: str | None = None,
):
    return SimpleNamespace(
        state=state,
        reduce_only=reduce_only,
        symbol="BTCUSDT",
        position_side="LONG",
        side=side,
        created_at=updated_at if created_at is None else created_at,
        updated_at=updated_at,
        exchange_order_id=exchange_order_id,
    )
