"""Unit tests for build_position_account_facts and PositionLedger projection."""

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.execution.position_batches import PositionOrderFact
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionHealthStatus,
    PositionKey,
)
from crypto_momentum_lab.live_rollout.order_identity_adapter import (
    build_position_account_facts,
)
from tests.fixtures.b2_anonymized_timeline import (
    get_b2_account_fill_events,
    get_b2_position_observation,
    get_b2_system_order_facts,
)


def test_orders_without_trades_do_not_generate_fills() -> None:
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    system_orders = get_b2_system_order_facts(symbol="BTCUSDT")
    obs = replace(
        get_b2_position_observation(symbol="BTCUSDT", position_amt=Decimal("120")),
        observed_at=datetime(2026, 10, 4, tzinfo=UTC),
    )

    facts = build_position_account_facts(
        position_key=key,
        orders=system_orders,
        observation=obs,
    )

    assert facts.position_key == key
    assert facts.fills == ()
    assert not facts.has_synthetic_fills
    assert len(facts.snapshots) == 1
    assert facts.snapshots[0].position_amt == Decimal("120")


def test_position_ledger_direct_projection_scenario() -> None:
    """Verify that pure system orders project cleanly with PositionLedger."""
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="B2USDT",
        position_side=FuturesPositionSide.BOTH,
    )

    system_orders = get_b2_system_order_facts()[:1]
    obs = get_b2_position_observation(position_amt=Decimal("120"))
    fills = get_b2_account_fill_events(account_label="primary")[:1]

    facts = build_position_account_facts(
        position_key=key,
        orders=system_orders,
        fills=fills,
        observation=obs,
    )
    ledger = PositionLedger(key)
    projection = ledger.project(facts)

    assert projection.health_status is PositionHealthStatus.READY
    assert projection.total_active_quantity == Decimal("120")
    assert len(projection.active_batches) == 1


def test_position_ledger_handles_external_fill_timeline() -> None:
    """Verify PositionLedger isolates episodes cleanly with external fills."""
    key = PositionKey(
        environment="live",
        account_label="account-3",
        symbol="B2USDT",
        position_side=FuturesPositionSide.BOTH,
    )
    system_orders = get_b2_system_order_facts()[:5]
    obs = get_b2_position_observation(position_amt=Decimal("172"))

    all_fills_up_to_post_zero = get_b2_account_fill_events()[:6]
    facts = build_position_account_facts(
        position_key=key,
        orders=system_orders,
        fills=all_fills_up_to_post_zero,
        observation=obs,
    )
    ledger = PositionLedger(key)
    projection = ledger.project(facts)

    assert projection.total_active_quantity == Decimal("172")
    assert projection.health_status is PositionHealthStatus.READY


def test_legacy_order_identity_adapter_isolates_hedge_mode_position_side() -> None:
    """Verify that to_account_facts isolates fills by position_side in Hedge mode."""
    now = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    long_key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.LONG,
    )
    short_key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.SHORT,
    )

    long_fill = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_long",
        order_id="ord_long",
        side="BUY",
        price=Decimal("60000"),
        quantity=Decimal("1.5"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.01"),
        fee_asset="USDT",
        trade_at=now,
        raw_payload={"positionSide": "LONG", "is_system": True},
    )
    short_fill = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_short",
        order_id="ord_short",
        side="SELL",
        price=Decimal("60000"),
        quantity=Decimal("2.0"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.01"),
        fee_asset="USDT",
        trade_at=now,
        raw_payload={"positionSide": "SHORT", "is_system": True},
    )

    all_fills = (long_fill, short_fill)

    long_facts = build_position_account_facts(
        position_key=long_key,
        orders=(),
        fills=all_fills,
    )
    assert len(long_facts.fills) == 1
    assert long_facts.fills[0].trade_id == "t_long"

    short_facts = build_position_account_facts(
        position_key=short_key,
        orders=(),
        fills=all_fills,
    )
    assert len(short_facts.fills) == 1
    assert short_facts.fills[0].trade_id == "t_short"


def test_to_account_facts_filters_stale_pre_episode_fills() -> None:
    """Verify prior closed episode fills (>5m before earliest order) discarded."""
    t_old = datetime(2026, 9, 19, 20, 0, tzinfo=UTC)
    t_new_entry = datetime(2026, 9, 20, 14, 0, tzinfo=UTC)
    t_new_fill = datetime(2026, 9, 20, 14, 0, 10, tzinfo=UTC)

    key = PositionKey(
        environment="live",
        account_label="account-3",
        symbol="CELRUSDT",
        position_side=FuturesPositionSide.BOTH,
    )

    current_order = PositionOrderFact(
        symbol="CELRUSDT",
        position_side=FuturesPositionSide.BOTH,
        side="BUY",
        reduce_only=False,
        order_type="LIMIT",
        quantity=Decimal("22799"),
        executed_quantity=Decimal("22799"),
        state=ExchangeOrderState.FILLED,
        client_order_id="c_celr_new",
        exchange_order_id="e_celr_new",
        created_at=t_new_entry,
        updated_at=t_new_entry,
        price=Decimal("0.004386"),
    )

    stale_fill = AccountFillEvent(
        environment="live",
        account_label="account-3",
        symbol="CELRUSDT",
        trade_id="t_stale_33388",
        order_id="e_celr_old_sell",
        side="SELL",
        price=Decimal("0.003022"),
        quantity=Decimal("33388"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.01"),
        fee_asset="USDT",
        trade_at=t_old,
        raw_payload={"positionSide": "BOTH", "is_system": True},
    )

    current_fill = AccountFillEvent(
        environment="live",
        account_label="account-3",
        symbol="CELRUSDT",
        trade_id="t_current_22799",
        order_id="e_celr_new",
        side="BUY",
        price=Decimal("0.004386"),
        quantity=Decimal("22799"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.01"),
        fee_asset="USDT",
        trade_at=t_new_fill,
        raw_payload={"positionSide": "BOTH", "is_system": True},
    )

    facts = build_position_account_facts(
        position_key=key,
        orders=[current_order],
        fills=[stale_fill, current_fill],
    )

    assert len(facts.fills) == 1
    assert facts.fills[0].trade_id == "t_current_22799"


def test_order_without_price_does_not_invent_a_unit_price():
    key = PositionKey(environment="live", account_label="primary", symbol="BTCUSDT")
    order = replace(get_b2_system_order_facts(symbol="BTCUSDT")[0], price=None)
    facts = build_position_account_facts(
        position_key=key, orders=(order,)
    )
    assert facts.fills == ()
    assert not facts.has_synthetic_fills


def test_observation_without_timestamp_does_not_invent_snapshot_time():
    key = PositionKey(environment="live", account_label="primary", symbol="BTCUSDT")
    observation = replace(get_b2_position_observation(symbol="BTCUSDT", position_amt=Decimal("120")), observed_at=None)
    facts = build_position_account_facts(position_key=key, orders=(), observation=observation)
    assert facts.snapshots == ()
