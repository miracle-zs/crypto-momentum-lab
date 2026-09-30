"""Pure time bounds for order and position evidence reads."""

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Protocol


class OrderTimeObservation(Protocol):
    @property
    def created_at(self) -> datetime | None: ...


class PositionTimeObservation(Protocol):
    @property
    def observed_at(self) -> datetime | None: ...


def _resolve_symbol_fill_horizon(
    orders: Sequence[OrderTimeObservation],
    active: Sequence[PositionTimeObservation],
) -> datetime | None:
    """Resolve the lower bound for symbol-based fill scanning.

    Order IDs are always queried exactly without any wall-clock cutoff.
    For broader symbol-level scans (capturing external/manual fills), anchor to
    24h before the earliest known order of the active positions. If no orders
    are recorded (e.g. unmanaged external positions), fall back to 7 days before
    the snapshot observation time.
    """
    order_times: list[datetime] = [
        order.created_at for order in orders if order.created_at is not None
    ]
    if order_times:
        return min(order_times) - timedelta(hours=24)
    active_times: list[datetime] = [
        row.observed_at for row in active if row.observed_at is not None
    ]
    if active_times:
        return min(active_times) - timedelta(days=7)
    return None
