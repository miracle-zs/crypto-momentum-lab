"""Contract test suite for B2USDT timeline fixture.

Verifies:
1. Exact quantity conservation across all 8 phases of the incident.
2. PositionLedger projection when external close orders are captured.
3. Zero-crossing boundary behavior: pre-zero entries must not taint post-zero batches.
"""

from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionKey,
)
from crypto_momentum_lab.live_rollout.position_ledger_shadow import (
    LegacyOrderIdentityAdapter,
)
from tests.fixtures.b2_anonymized_timeline import (
    B2_TIMELINE,
    get_b2_account_fill_events,
    get_b2_position_observation,
    get_b2_system_order_facts,
)


def test_b2_timeline_quantity_conservation() -> None:
    """Verify that the raw event timeline satisfies exact quantity conservation."""
    running_qty = Decimal("0")
    for entry in B2_TIMELINE:
        if entry.event_type == "fill":
            if entry.side == "BUY":
                running_qty += entry.quantity
            elif entry.side == "SELL":
                running_qty -= entry.quantity
        assert running_qty == entry.position_after, (
            f"Quantity mismatch at {entry.timestamp}: running={running_qty}, "
            f"expected={entry.position_after}, note={entry.note}"
        )

    # Final balance must be zero
    assert running_qty == Decimal("0")


def test_b2_account_fills_capture_external_trades() -> None:
    """Verify that get_b2_account_fill_events includes external manual orders."""
    fills = get_b2_account_fill_events()
    assert len(fills) == 12

    # External manual close of 254
    ext_254 = [f for f in fills if f.order_id == "b2_ext_ord_823890995"]
    assert len(ext_254) == 1
    assert ext_254[0].quantity == Decimal("254")
    assert ext_254[0].side == "SELL"

    # External manual dust close of 7
    ext_7 = [f for f in fills if f.order_id == "b2_ext_ord_837473466"]
    assert len(ext_7) == 1
    assert ext_7[0].quantity == Decimal("7")


def test_b2_system_order_facts_exclude_external_trades() -> None:
    """Verify that get_b2_system_order_facts only contains system-generated orders."""
    system_orders = get_b2_system_order_facts()
    assert len(system_orders) == 10

    # No external orders in system orders
    order_ids = {o.exchange_order_id for o in system_orders}
    assert "b2_ext_ord_823890995" not in order_ids
    assert "b2_ext_ord_837473466" not in order_ids


def test_b2_post_zero_projection_with_position_ledger() -> None:
    """Demonstrate PositionLedger projection for post-zero position (amt=172).

    When observation is 172 (right after post-zero buy of 172) and complete fills
    include the external manual close of 254:
    PositionLedger cleanly closes Episode 1 at the zero crossing, and begins
    Episode 2 with exactly 172 units at the clean entry price and timestamp.
    """
    key = PositionKey("live", "account-3", "B2USDT", FuturesPositionSide.BOTH)
    system_orders = get_b2_system_order_facts()[:5]
    all_fills = get_b2_account_fill_events()[:6]
    obs = get_b2_position_observation(position_amt=Decimal("172"))

    facts = LegacyOrderIdentityAdapter.to_account_facts(
        position_key=key,
        orders=system_orders,
        fills=all_fills,
        observation=obs,
    )
    ledger = PositionLedger(key)
    proj = ledger.project(facts)

    assert proj.total_active_quantity == Decimal("172")
    assert len(proj.active_batches) == 1
    assert proj.active_batches[0].quantity == Decimal("172")
    assert proj.active_batches[0].opened_at == datetime(
        2026, 9, 19, 15, 59, 30, tzinfo=UTC
    )


def test_b2_dust_state_projection_contract() -> None:
    """Verify PositionLedger projection at the 7-coin dust remainder state."""
    key = PositionKey("live", "account-3", "B2USDT", FuturesPositionSide.BOTH)
    system_orders = get_b2_system_order_facts()
    all_fills = get_b2_account_fill_events()[:11]
    obs = get_b2_position_observation(position_amt=Decimal("7"))

    facts = LegacyOrderIdentityAdapter.to_account_facts(
        position_key=key,
        orders=system_orders,
        fills=all_fills,
        observation=obs,
    )
    ledger = PositionLedger(key)
    proj = ledger.project(facts)

    assert proj.total_active_quantity == Decimal("7")
    assert len(proj.active_batches) == 1
    assert proj.active_batches[0].quantity == Decimal("7")
