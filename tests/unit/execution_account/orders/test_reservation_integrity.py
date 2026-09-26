"""Capacity fail-closed, conflict identity, and partial-fill attribution."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.execution_coordinator import (
    InMemoryPositionReservationRepository,
    ReservationConflictError,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation
from crypto_momentum_lab.persistence.postgres.position_reservation_repository import (
    _adopt_or_reject_existing,
    _require_capacity,
)


def _key() -> PositionKey:
    return PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )


def _res(
    reservation_id: str,
    *,
    batch_id: str = "batch_1",
    qty: str = "10",
    created_at: datetime | None = None,
) -> PositionReservation:
    return PositionReservation(
        reservation_id=reservation_id,
        command_id="cmd-1",
        position_key=_key(),
        batch_id=batch_id,
        reserved_quantity=Decimal(qty),
        created_at=created_at or datetime.now(UTC),
    )


def test_require_capacity_fails_closed_on_missing_snapshot() -> None:
    with pytest.raises(ReservationConflictError, match="snapshot missing"):
        _require_capacity(
            reserved_quantity=Decimal("1"),
            total_active=Decimal("0"),
            pos_amt=None,
        )


def test_require_capacity_fails_closed_on_zero_position() -> None:
    with pytest.raises(ReservationConflictError, match="zero quantity"):
        _require_capacity(
            reserved_quantity=Decimal("1"),
            total_active=Decimal("0"),
            pos_amt=Decimal("0"),
        )


def test_require_capacity_rejects_over_reserving() -> None:
    with pytest.raises(ValueError, match="exceeded"):
        _require_capacity(
            reserved_quantity=Decimal("5"),
            total_active=Decimal("6"),
            pos_amt=Decimal("10"),
        )


def test_require_capacity_allows_within_limit() -> None:
    _require_capacity(
        reserved_quantity=Decimal("5"),
        total_active=Decimal("4"),
        pos_amt=Decimal("10"),
    )


def test_adopt_or_reject_rejects_batch_mismatch() -> None:
    existing = _res("r1", batch_id="batch_1", qty="10")
    requested = _res("r1", batch_id="batch_2", qty="10")
    with pytest.raises(ReservationConflictError, match="already exists"):
        _adopt_or_reject_existing(existing, requested)


def test_adopt_or_reject_rejects_qty_mismatch() -> None:
    existing = _res("r1", batch_id="batch_1", qty="10")
    requested = _res("r1", batch_id="batch_1", qty="5")
    with pytest.raises(ReservationConflictError, match="already exists"):
        _adopt_or_reject_existing(existing, requested)


def test_adopt_or_reject_accepts_matching_identity() -> None:
    existing = _res("r1", batch_id="batch_1", qty="10")
    requested = _res("r1", batch_id="batch_1", qty="10")
    _adopt_or_reject_existing(existing, requested)


def test_inmemory_save_conflict_on_same_id_different_identity() -> None:
    repo = InMemoryPositionReservationRepository()
    repo.save_reservation(_res("r1", batch_id="batch_1", qty="10"))
    with pytest.raises(ReservationConflictError, match="already exists"):
        repo.save_reservation(_res("r1", batch_id="batch_1", qty="5"))


def test_inmemory_save_adopts_matching_retry() -> None:
    repo = InMemoryPositionReservationRepository()
    first = _res("r1", batch_id="batch_1", qty="10")
    repo.save_reservation(first)
    repo.save_reservation(_res("r1", batch_id="batch_1", qty="10"))
    assert repo.load_reservation("r1") == first


def test_load_active_reservations_is_stable_ordered() -> None:
    repo = InMemoryPositionReservationRepository()
    t0 = datetime.now(UTC)
    repo.save_reservation(_res("r2", batch_id="batch_2", qty="1", created_at=t0))
    repo.save_reservation(
        _res("r1", batch_id="batch_1", qty="1", created_at=t0 - timedelta(seconds=1))
    )
    repo.save_reservation(
        _res("r3", batch_id="batch_3", qty="1", created_at=t0 + timedelta(seconds=1))
    )
    ordered = [r.reservation_id for r in repo.load_active_reservations(_key())]
    assert ordered == ["r1", "r2", "r3"]


def test_inmemory_rejects_terminal_same_id() -> None:
    repo = InMemoryPositionReservationRepository()
    res = _res("r1", batch_id="batch_1", qty="10")
    repo.save_reservation(res)
    repo.update_reservation(res.release(Decimal("10")))
    with pytest.raises(ReservationConflictError, match="terminal"):
        repo.save_reservation(_res("r1", batch_id="batch_1", qty="10"))


def test_inmemory_rejects_position_key_mismatch() -> None:
    repo = InMemoryPositionReservationRepository()
    repo.save_reservation(_res("r1", batch_id="batch_1", qty="10"))
    other_key = PositionKey(
        environment="live",
        account_label="other",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    hijack = PositionReservation(
        reservation_id="r1",
        command_id="cmd-1",
        position_key=other_key,
        batch_id="batch_1",
        reserved_quantity=Decimal("10"),
    )
    with pytest.raises(ReservationConflictError):
        repo.save_reservation(hijack)


def test_inmemory_batch_capacity() -> None:
    repo = InMemoryPositionReservationRepository()
    repo.save_reservation(
        _res("r1", batch_id="batch_1", qty="8"),
        batch_quantity=Decimal("10"),
    )
    with pytest.raises(ReservationConflictError, match="over-reserved"):
        repo.save_reservation(
            _res("r2", batch_id="batch_1", qty="3"),
            batch_quantity=Decimal("10"),
        )


def test_save_reservations_is_all_or_nothing() -> None:
    repo = InMemoryPositionReservationRepository()
    with pytest.raises(ReservationConflictError):
        repo.save_reservations(
            (
                _res("r1", batch_id="batch_1", qty="8"),
                _res("r2", batch_id="batch_1", qty="3"),
            ),
            batch_quantities={"batch_1": Decimal("10")},
        )
    assert repo.load_reservation("r1") is None
    assert repo.load_reservation("r2") is None
