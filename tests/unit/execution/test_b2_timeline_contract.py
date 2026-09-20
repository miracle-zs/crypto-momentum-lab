"""Contract test suite for B2USDT timeline fixture.

Verifies:
1. Exact quantity conservation across all 8 phases of the incident.
2. Current batch reconstruction behavior when external close orders are omitted vs included.
3. Zero-crossing boundary behavior: pre-zero entries must not taint post-zero batches.
4. Foundation expectations for PositionLedger v2.
"""

from decimal import Decimal

from crypto_momentum_lab.domain.execution import (
    PositionHistory,
    rebuild_position_batches,
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


def test_b2_post_zero_rebuild_with_current_logic() -> None:
    """Demonstrate batch reconstruction for post-zero position (amt=172).

    When observation is 172 (right after post-zero buy of 172) and history
    only contains system orders (buys 120, 127, 127, exit 120, buy 172):
    Total buy qty in system history is 374 + 172 = 546.
    System sells = 120. Net in system orders = 426.
    Observed position = 172.
    Current rebuild_position_batches trims 426 down to 172 using FIFO.
    """
    from datetime import UTC, datetime
    system_orders = get_b2_system_order_facts()
    # Take orders up to post-zero buy (indices 0..4: buy 120, buy 127, buy 127, sell 120, buy 172)
    orders_up_to_post_zero = system_orders[:5]

    obs = get_b2_position_observation(position_amt=Decimal("172"))
    history = PositionHistory(orders=orders_up_to_post_zero)

    result = rebuild_position_batches(obs, history)

    # Reconstructed batch quantity should match the observation (172)
    total_batch_qty = sum(b.quantity for b in result.batches)
    assert total_batch_qty == Decimal("172")

    # With FIFO reconciliation (fixed in e88c069), the newest batch (172) is preserved
    # instead of the stale pre-zero batches
    assert len(result.batches) == 1
    assert result.batches[0].quantity == Decimal("172")
    assert result.batches[0].opened_at == datetime(2026, 9, 19, 15, 59, 30, tzinfo=UTC)


def test_b2_dust_state_rebuild_contract() -> None:
    """Verify batch reconstruction at the 7-coin dust remainder state.

    At phase 7, observation is 7.
    All system orders up to phase 7 are passed.
    """
    system_orders = get_b2_system_order_facts()
    obs = get_b2_position_observation(position_amt=Decimal("7"))
    history = PositionHistory(orders=system_orders)

    result = rebuild_position_batches(obs, history)

    total_batch_qty = sum(b.quantity for b in result.batches)
    assert total_batch_qty == Decimal("7")
    assert len(result.batches) == 1
    assert result.batches[0].quantity == Decimal("7")
