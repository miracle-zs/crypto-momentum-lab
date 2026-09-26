"""Live decision facts come from account context, never invented."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import AccountBalanceSnapshot
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
        self.managed_positions = kwargs.get("managed_positions", ())
        self.strategy_state = kwargs.get("strategy_state", StrategyLiveState.ACTIVE)
        self.active_halts = kwargs.get("active_halts", ())
        self.risk_config = _Risk()
        self.context_epoch = kwargs.get("context_epoch", 1)
        self.account_snapshot_version = kwargs.get("account_snapshot_version", 7)
        self.coverage_by_symbol = kwargs.get("coverage_by_symbol", {})


def _snapshot(cash: str = "1000") -> AccountSnapshot:
    bal = AccountBalanceSnapshot(
        environment="live",
        account_label="primary",
        asset="USDT",
        wallet_balance=Decimal(cash),
        available_balance=Decimal(cash),
        unrealized_pnl=Decimal("0"),
        observed_at=datetime(2026, 9, 25, tzinfo=UTC),
        raw_payload={},
    )
    return AccountSnapshot(
        config=None,  # type: ignore[arg-type]
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
