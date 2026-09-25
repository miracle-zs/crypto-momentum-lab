"""Read-only minimal regression probe; run from repo root with PYTHONPATH=src.

This models missing external-close evidence in the legacy order input, not a
complete export of production inputs. A nonzero exit means the defect persists.
"""

from datetime import UTC, datetime
from decimal import Decimal as D

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
    FuturesPositionSide,
    PositionHistory,
    PositionObservation,
    PositionOrderFact,
    rebuild_position_batches,
)
from crypto_momentum_lab.domain.strategy import StrategySide


def entry(identity: str, day: int, quantity: str, price: str) -> PositionOrderFact:
    at = datetime(2026, 9, day, tzinfo=UTC)
    return PositionOrderFact(
        symbol="AKEUSDT", position_side=FuturesPositionSide.BOTH,
        side="BUY", reduce_only=False, order_type="LIMIT",
        quantity=D(quantity), executed_quantity=D(quantity),
        state=ExchangeOrderState.FILLED, client_order_id=identity,
        exchange_order_id=None, created_at=at, updated_at=at, price=D(price),
    )


if __name__ == "__main__":
    old = entry("old", 20, "944", ".105886")
    new = entry("new", 25, "2618", ".038197")
    observation = PositionObservation(
        symbol="AKEUSDT", side=StrategySide.LONG,
        position_side=FuturesPositionSide.BOTH,
        position_amt=D("2618"), entry_price=D(".038197"),
    )
    for label, orders in (("control", (new,)), ("missing_external_close", (old, new))):
        result = rebuild_position_batches(observation, PositionHistory(orders=orders))
        batch = result.batches[0]
        print(f"{label}: qty={batch.quantity} price={batch.entry_price} id={batch.batch_id}")
        assert batch.quantity == D("2618")
        assert batch.entry_price == D(".038197"), "closed episode contaminated current batch"
