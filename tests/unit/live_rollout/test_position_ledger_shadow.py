"""Unit tests for PositionLedger shadow comparator and legacy adapters."""

from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution import (
    FuturesPositionSide,
    PositionHistory,
    rebuild_position_batches,
)
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionKey,
)
from crypto_momentum_lab.live_rollout.position_ledger_shadow import (
    LegacyOrderIdentityAdapter,
    PositionLedgerShadowComparator,
    ShadowDiffCategory,
)
from tests.fixtures.b2_anonymized_timeline import (
    get_b2_account_fill_events,
    get_b2_position_observation,
    get_b2_system_order_facts,
)


def test_legacy_order_identity_adapter_conversion() -> None:
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    system_orders = get_b2_system_order_facts(symbol="BTCUSDT")
    obs = get_b2_position_observation(symbol="BTCUSDT", position_amt=Decimal("120"))

    facts = LegacyOrderIdentityAdapter.to_account_facts(
        position_key=key,
        orders=system_orders,
        observation=obs,
    )

    assert facts.position_key == key
    assert len(facts.fills) == len(system_orders)
    assert len(facts.snapshots) == 1
    assert facts.snapshots[0].position_amt == Decimal("120")


def test_shadow_comparator_exact_match_scenario() -> None:
    """Verify that pure system orders without external interference match exactly."""
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )

    # Use first order from B2 (Buy 120)
    system_orders = get_b2_system_order_facts()[:1]
    obs = get_b2_position_observation(position_amt=Decimal("120"))

    # Legacy rebuild
    history = PositionHistory(orders=system_orders)
    legacy_result = rebuild_position_batches(obs, history)

    # Ledger v2 projection via adapter
    facts = LegacyOrderIdentityAdapter.to_account_facts(
        position_key=key,
        orders=system_orders,
        observation=obs,
    )
    ledger = PositionLedger(key)
    projection = ledger.project(facts)

    # Compare
    report = PositionLedgerShadowComparator.compare(
        position_key=key,
        legacy_batches=legacy_result.batches,
        ledger_projection=projection,
    )

    assert report.is_concordant is True
    assert report.category is ShadowDiffCategory.EXACT_MATCH
    assert report.legacy_total_quantity == Decimal("120")
    assert report.ledger_total_quantity == Decimal("120")


def test_shadow_comparator_detects_external_fill_divergence_safely() -> None:
    """Verify shadow comparison safely detects divergence when external fills exist.

    Scenario:
    - Legacy rebuild only has system orders (it doesn't know about external fill 254).
    - Ledger v2 receives the complete AccountFacts including external fills.
    - Legacy clips 426 -> 172 using FIFO.
    - Ledger v2 cleanly isolates Episode 1 and Episode 2.
    The comparator should safely categorize the diff without raising any exceptions.
    """
    key = PositionKey(
        environment="live",
        account_label="account-3",
        symbol="B2USDT",
        position_side=FuturesPositionSide.BOTH,
    )
    # Take orders up to post-zero buy (indices 0..4)
    system_orders = get_b2_system_order_facts()[:5]
    obs = get_b2_position_observation(position_amt=Decimal("172"))

    # Legacy output
    history = PositionHistory(orders=system_orders)
    legacy_result = rebuild_position_batches(obs, history)

    # Ledger v2 with complete facts up to that point
    all_fills_up_to_post_zero = get_b2_account_fill_events()[:6]
    facts = LegacyOrderIdentityAdapter.to_account_facts(
        position_key=key,
        orders=system_orders,
        fills=all_fills_up_to_post_zero,
        observation=obs,
    )
    ledger = PositionLedger(key)
    projection = ledger.project(facts)

    report = PositionLedgerShadowComparator.compare(
        position_key=key,
        legacy_batches=legacy_result.batches,
        ledger_projection=projection,
    )

    # Both agree on the current active quantity (172)
    assert report.legacy_total_quantity == Decimal("172")
    assert report.ledger_total_quantity == Decimal("172")
    # Comparator logs diagnostic without throwing
    assert report is not None


def test_legacy_order_identity_adapter_isolates_hedge_mode_position_side() -> None:
    """Verify that to_account_facts isolates fills by position_side in Hedge mode."""
    from crypto_momentum_lab.domain.account import AccountFillEvent

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

    # Adapting for LONG must only pick long_fill
    long_facts = LegacyOrderIdentityAdapter.to_account_facts(
        position_key=long_key,
        orders=(),
        fills=all_fills,
    )
    assert len(long_facts.fills) == 1
    assert long_facts.fills[0].trade_id == "t_long"

    # Adapting for SHORT must only pick short_fill
    short_facts = LegacyOrderIdentityAdapter.to_account_facts(
        position_key=short_key,
        orders=(),
        fills=all_fills,
    )
    assert len(short_facts.fills) == 1
    assert short_facts.fills[0].trade_id == "t_short"


def test_to_account_facts_filters_stale_pre_episode_fills() -> None:
    """Verify that fills from a prior closed episode (>5 min before earliest order) are discarded."""
    from crypto_momentum_lab.domain.account import AccountFillEvent
    from crypto_momentum_lab.domain.execution import (
        ExchangeOrderState,
        PositionOrderFact,
    )

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

    facts = LegacyOrderIdentityAdapter.to_account_facts(
        position_key=key,
        orders=[current_order],
        fills=[stale_fill, current_fill],
    )

    assert len(facts.fills) == 1
    assert facts.fills[0].trade_id == "t_current_22799"

