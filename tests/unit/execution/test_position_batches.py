from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.execution.position_batches import (
    ManagedLivePositionBatch,
    count_active_symbol_batch_concurrency,
)

NOW = datetime(2026, 9, 10, 0, 0, tzinfo=UTC)


def test_managed_live_position_batch_validation() -> None:
    batch = ManagedLivePositionBatch(
        batch_id="b1",
        quantity=Decimal("10"),
        entry_price=Decimal("1.5"),
        opened_at=NOW,
        entry_order_count=1,
    )
    assert batch.batch_id == "b1"
    assert batch.quantity == Decimal("10")
    assert batch.entry_price == Decimal("1.5")


def test_count_active_symbol_batch_concurrency_scenarios() -> None:
    class MockOrder:
        def __init__(
            self, symbol: str, reduce_only: bool, client_order_id: str | None = None
        ) -> None:
            self.symbol = symbol
            self.reduce_only = reduce_only
            self.client_order_id = client_order_id

    class MockPosition:
        def __init__(
            self, symbol: str, batches: tuple[ManagedLivePositionBatch, ...] = ()
        ) -> None:
            self.symbol = symbol
            self.batches = batches
            self.recovery_exit_started_at = None
            self.closing_order_filled = False

    # Case 1: No positions, no orders -> 0
    assert count_active_symbol_batch_concurrency("ACUUSDT", (), ()) == 0

    # Case 2: 1 pending entry order -> 1
    pending_o1 = MockOrder("ACUUSDT", False, "pending-1")
    assert count_active_symbol_batch_concurrency("ACUUSDT", (), (pending_o1,)) == 1

    # Case 3: 2 pending entry orders -> 2
    pending_o2 = MockOrder("ACUUSDT", False, "pending-2")
    assert (
        count_active_symbol_batch_concurrency("ACUUSDT", (), (pending_o1, pending_o2))
        == 2
    )

    # Case 4: Active batch with 1 entry order, 0 pending -> 1
    batch_1 = ManagedLivePositionBatch(
        batch_id="b1",
        quantity=Decimal("10"),
        entry_price=Decimal("1.5"),
        opened_at=NOW,
        entry_order_count=1,
        entry_client_order_ids=frozenset({"e1"}),
    )
    pos = MockPosition("ACUUSDT", (batch_1,))
    assert count_active_symbol_batch_concurrency("ACUUSDT", (pos,), ()) == 1

    # Case 5: Active batch with 2 entry orders (consolidated), 0 pending -> 2
    batch_2 = ManagedLivePositionBatch(
        batch_id="b1",
        quantity=Decimal("20"),
        entry_price=Decimal("1.5"),
        opened_at=NOW,
        entry_order_count=2,
        entry_client_order_ids=frozenset({"e1", "e2"}),
    )
    pos2 = MockPosition("ACUUSDT", (batch_2,))
    assert count_active_symbol_batch_concurrency("ACUUSDT", (pos2,), ()) == 2

    # Case 6: Partial fill deduplication (order in batch & unresolved) -> 1, not 2
    order_in_flight = MockOrder("ACUUSDT", False, "e1")
    assert (
        count_active_symbol_batch_concurrency("ACUUSDT", (pos,), (order_in_flight,))
        == 1
    )

    # Case 7: Batch 1 has exit order submitted -> treated as ENDED, does NOT count!
    batch_ended = ManagedLivePositionBatch(
        batch_id="b1",
        quantity=Decimal("20"),
        entry_price=Decimal("1.5"),
        opened_at=NOW,
        entry_order_count=2,
        exit_order_submitted_at=NOW + timedelta(minutes=10),
    )
    pos_ended = MockPosition("ACUUSDT", (batch_ended,))
    # Active batch concurrency must be 0, allowing new batch to open!
    assert count_active_symbol_batch_concurrency("ACUUSDT", (pos_ended,), ()) == 0

    # Case 8: Batch 1 has exit order submitted, Batch 2 is active with 1 entry -> 1
    batch_new = ManagedLivePositionBatch(
        batch_id="b2",
        quantity=Decimal("10"),
        entry_price=Decimal("1.7"),
        opened_at=NOW + timedelta(minutes=15),
        entry_order_count=1,
    )
    pos_multi_batch = MockPosition("ACUUSDT", (batch_ended, batch_new))
    assert count_active_symbol_batch_concurrency("ACUUSDT", (pos_multi_batch,), ()) == 1

    # Case 9: Different symbol (e.g. BTCUSDT) -> 0
    assert count_active_symbol_batch_concurrency("BTCUSDT", (pos2,), ()) == 0
