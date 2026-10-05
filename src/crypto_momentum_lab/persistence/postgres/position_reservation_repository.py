"""PostgreSQL persistence adapter for batch-level PositionReservation.

Obeys Astra Architecture Blueprint 2026-09-25 (P2):
- Durable, transactional storage of batch-level lot reservations;
- Enforces CAS and optimistic reservation persistence across process restarts;
- Supports crash recovery rehydration of active in-flight reservations;
- Implements asynchronous reservation writes and recovery reads.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution.reservation_registry import (
    ReservationConflictError,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation
from crypto_momentum_lab.persistence.postgres.models import (
    AccountPositionSnapshotRow,
    PositionReservationRow,
)


def _insert_rowcount(result: Any) -> int:
    return int(result.rowcount)


def _row_to_reservation(r: PositionReservationRow) -> PositionReservation:
    pos_key = PositionKey(
        environment=r.environment,
        account_label=r.account_label,
        symbol=r.symbol,
        position_side=FuturesPositionSide(r.position_side),
    )
    return PositionReservation(
        reservation_id=r.reservation_id,
        command_id=r.command_id,
        position_key=pos_key,
        batch_id=r.batch_id,
        reserved_quantity=r.reserved_quantity,
        consumed_quantity=r.consumed_quantity,
        released_quantity=r.released_quantity,
        created_at=r.created_at,
    )


async def _acquire_reservation_lock_async(session: AsyncSession, lock_key: str) -> None:
    bind = session.get_bind()
    if bind is not None and bind.dialect.name != "postgresql":
        return
    try:
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
            {"lock_key": lock_key},
        )
    except Exception as lock_err:
        raise ReservationConflictError(
            f"reservation lock unavailable: {lock_err}"
        ) from lock_err


async def _load_position_amt_async(
    session: AsyncSession,
    reservation: PositionReservation,
) -> Decimal | None:
    snap_res = await session.execute(
        select(AccountPositionSnapshotRow)
        .where(
            AccountPositionSnapshotRow.environment
            == reservation.position_key.environment,
            AccountPositionSnapshotRow.account_label
            == reservation.position_key.account_label,
            AccountPositionSnapshotRow.symbol == reservation.position_key.symbol,
            AccountPositionSnapshotRow.position_side
            == reservation.position_key.position_side.value,
        )
        .order_by(AccountPositionSnapshotRow.observed_at.desc())
        .limit(1)
    )
    pos_snap = snap_res.scalars().first()
    return pos_snap.position_amt if pos_snap is not None else None


def _require_capacity(
    *,
    reserved_quantity: Decimal,
    total_active: Decimal,
    pos_amt: Decimal | None,
) -> None:
    """Fail closed when available position quantity cannot be proven.

    Missing snapshot, zero position, or unknown amount must not silently
    skip the capacity guard — that path is how over-reservation slips in.
    """
    if pos_amt is None:
        raise ReservationConflictError(
            "position snapshot missing; refusing to reserve without proven capacity"
        )
    if abs(pos_amt) <= Decimal("0"):
        raise ReservationConflictError(
            "position snapshot has zero quantity; refusing to reserve "
            "against an empty position"
        )
    max_qty = abs(pos_amt)
    if total_active + reserved_quantity > max_qty:
        raise ValueError(
            f"Database reservation quantity exceeded: requested "
            f"{reserved_quantity}, already reserved {total_active}, "
            f"available position {max_qty}"
        )


def _adopt_or_reject_existing(
    existing: PositionReservation,
    requested: PositionReservation,
) -> None:
    """Same ID may be adopted only when identity fully matches the plan.

    Terminal rows must never be treated as a live reservation. Position key
    and command identity must match so a retry cannot hijack another lot.
    """
    if existing.position_key.canonical_id != requested.position_key.canonical_id:
        raise ReservationConflictError(
            f"reservation {requested.reservation_id} already exists for "
            f"{existing.position_key.canonical_id}, retry targeted "
            f"{requested.position_key.canonical_id}"
        )
    if existing.command_id != requested.command_id:
        raise ReservationConflictError(
            f"reservation {requested.reservation_id} already exists for "
            f"command {existing.command_id}, retry used "
            f"{requested.command_id}"
        )
    if (
        existing.batch_id != requested.batch_id
        or existing.reserved_quantity != requested.reserved_quantity
    ):
        raise ReservationConflictError(
            f"reservation {requested.reservation_id} already exists with "
            f"batch {existing.batch_id} qty {existing.reserved_quantity}, "
            f"retry planned batch {requested.batch_id} qty "
            f"{requested.reserved_quantity}"
        )


def _require_batch_capacity(
    *,
    batch_id: str,
    reserved_quantity: Decimal,
    total_active_for_batch: Decimal,
    batch_quantity: Decimal,
) -> None:
    """Fail closed when a single batch would be over-reserved."""
    if batch_quantity <= Decimal("0"):
        raise ReservationConflictError(f"batch {batch_id} quantity must be positive")
    if total_active_for_batch + reserved_quantity > batch_quantity:
        raise ReservationConflictError(
            f"batch {batch_id} over-reserved: requested {reserved_quantity}, "
            f"already reserved {total_active_for_batch}, "
            f"batch quantity {batch_quantity}"
        )


class AsyncPostgresPositionReservationRepository:
    """Async PostgreSQL repository for durable lot reservations in async daemons."""

    def __init__(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        strategy_name: str,
    ) -> None:
        if not strategy_name or not strategy_name.strip():
            raise ValueError("strategy_name must not be empty")
        self._session_maker = session_maker
        self._strategy_name = strategy_name.strip()

    async def save_reservations(
        self,
        reservations: Sequence[PositionReservation],
        *,
        batch_quantities: Mapping[str, Decimal],
    ) -> None:
        if not reservations:
            return
        now = datetime.now(UTC)
        first = reservations[0]
        strat = self._strategy_name
        async with self._session_maker() as session, session.begin():
            await _acquire_reservation_lock_async(
                session, f"res_{first.position_key.canonical_id}"
            )
            active_res = await session.execute(
                select(PositionReservationRow).where(
                    PositionReservationRow.environment
                    == first.position_key.environment,
                    PositionReservationRow.account_label
                    == first.position_key.account_label,
                    PositionReservationRow.symbol == first.position_key.symbol,
                    PositionReservationRow.position_side
                    == first.position_key.position_side.value,
                    PositionReservationRow.status == "ACTIVE",
                )
            )
            active_rows = active_res.scalars().all()
            batch_active: dict[str, Decimal] = {}
            total_active = Decimal("0")
            for r in active_rows:
                remaining = (
                    r.reserved_quantity - r.consumed_quantity - r.released_quantity
                )
                batch_active[r.batch_id] = (
                    batch_active.get(r.batch_id, Decimal("0")) + remaining
                )
                total_active += remaining

            pending_batch: dict[str, Decimal] = {}
            for reservation in reservations:
                existing_row = await session.get(
                    PositionReservationRow, reservation.reservation_id
                )
                if existing_row is not None:
                    if existing_row.status != "ACTIVE":
                        raise ReservationConflictError(
                            f"reservation {reservation.reservation_id} already "
                            f"exists in terminal status {existing_row.status}"
                        )
                    _adopt_or_reject_existing(
                        _row_to_reservation(existing_row), reservation
                    )
                    continue
                if reservation.batch_id not in batch_quantities:
                    raise ReservationConflictError(
                        f"batch {reservation.batch_id} capacity unknown or "
                        "missing from batch_quantities"
                    )
                already = pending_batch.get(reservation.batch_id, Decimal("0"))
                _require_batch_capacity(
                    batch_id=reservation.batch_id,
                    reserved_quantity=reservation.reserved_quantity,
                    total_active_for_batch=batch_active.get(
                        reservation.batch_id, Decimal("0")
                    )
                    + already,
                    batch_quantity=batch_quantities[reservation.batch_id],
                )
                pending_batch[reservation.batch_id] = (
                    already + reservation.reserved_quantity
                )
                try:
                    pos_amt = await _load_position_amt_async(session, reservation)
                except Exception as snap_err:
                    raise ReservationConflictError(
                        f"position snapshot capacity check failed: {snap_err}"
                    ) from snap_err
                _require_capacity(
                    reserved_quantity=reservation.reserved_quantity,
                    total_active=total_active,
                    pos_amt=pos_amt,
                )
                total_active += reservation.reserved_quantity

                stmt = (
                    insert(PositionReservationRow)
                    .values(
                        reservation_id=reservation.reservation_id,
                        environment=reservation.position_key.environment,
                        account_label=reservation.position_key.account_label,
                        strategy_name=strat,
                        symbol=reservation.position_key.symbol,
                        position_side=(reservation.position_key.position_side.value),
                        batch_id=reservation.batch_id,
                        command_id=reservation.command_id,
                        client_order_id=None,
                        reserved_quantity=reservation.reserved_quantity,
                        consumed_quantity=reservation.consumed_quantity,
                        released_quantity=reservation.released_quantity,
                        status="ACTIVE",
                        created_at=reservation.created_at,
                        updated_at=now,
                    )
                    .on_conflict_do_nothing()
                )
                result = await session.execute(stmt)
                if _insert_rowcount(result) == 0:
                    existing_row = await session.get(
                        PositionReservationRow, reservation.reservation_id
                    )
                    if existing_row is not None and existing_row.status == "ACTIVE":
                        _adopt_or_reject_existing(
                            _row_to_reservation(existing_row), reservation
                        )
                        continue
                    raise ReservationConflictError(
                        f"reservation {reservation.reservation_id} insert "
                        "conflicted but no active row was found"
                    )

    async def save_reservations_in_session(
        self,
        session: AsyncSession,
        reservations: Sequence[PositionReservation],
        *,
        batch_quantities: Mapping[str, Decimal],
        proven_position_quantity: Decimal,
    ) -> None:
        """Save reservations on the transaction owned by an execution UoW.

        Live ExecutionBook callers provide the projection's proven total and
        batch capacities. The transaction uses this quantity without querying
        an independent account snapshot.
        """
        if not reservations:
            return
        first = reservations[0]
        for reservation in reservations:
            if reservation.position_key.canonical_id != first.position_key.canonical_id:
                raise ReservationConflictError(
                    "one reservation transaction cannot span position scopes"
                )
        await _acquire_reservation_lock_async(
            session, f"res_{first.position_key.canonical_id}"
        )
        result = await session.execute(
            select(PositionReservationRow).where(
                PositionReservationRow.environment == first.position_key.environment,
                PositionReservationRow.account_label
                == first.position_key.account_label,
                PositionReservationRow.symbol == first.position_key.symbol,
                PositionReservationRow.position_side
                == first.position_key.position_side.value,
                PositionReservationRow.status == "ACTIVE",
            )
        )
        active_rows = result.scalars().all()

        active_by_batch: dict[str, Decimal] = {}
        active_total = Decimal("0")
        for row in active_rows:
            remaining = (
                row.reserved_quantity - row.consumed_quantity - row.released_quantity
            )
            active_by_batch[row.batch_id] = (
                active_by_batch.get(row.batch_id, Decimal("0")) + remaining
            )
            active_total += remaining

        pending_by_batch: dict[str, Decimal] = {}
        now = datetime.now(UTC)
        for reservation in reservations:
            existing = await session.get(
                PositionReservationRow,
                reservation.reservation_id,
                with_for_update=True,
            )
            if existing is not None:
                if existing.status != "ACTIVE":
                    raise ReservationConflictError(
                        f"reservation {reservation.reservation_id} is terminal"
                    )
                _adopt_or_reject_existing(_row_to_reservation(existing), reservation)
                continue
            if reservation.batch_id not in batch_quantities:
                raise ReservationConflictError(
                    f"batch {reservation.batch_id} has no proven capacity"
                )
            already = pending_by_batch.get(reservation.batch_id, Decimal("0"))
            _require_batch_capacity(
                batch_id=reservation.batch_id,
                reserved_quantity=reservation.reserved_quantity,
                total_active_for_batch=active_by_batch.get(
                    reservation.batch_id, Decimal("0")
                )
                + already,
                batch_quantity=batch_quantities[reservation.batch_id],
            )
            position_amount = proven_position_quantity
            if not position_amount.is_finite() or position_amount <= Decimal("0"):
                raise ReservationConflictError(
                    "authoritative position quantity must be finite and positive"
                )
            _require_capacity(
                reserved_quantity=reservation.reserved_quantity,
                total_active=active_total,
                pos_amt=position_amount,
            )
            active_total += reservation.reserved_quantity
            pending_by_batch[reservation.batch_id] = (
                already + reservation.reserved_quantity
            )
            session.add(
                PositionReservationRow(
                    reservation_id=reservation.reservation_id,
                    environment=reservation.position_key.environment,
                    account_label=reservation.position_key.account_label,
                    strategy_name=self._strategy_name,
                    symbol=reservation.position_key.symbol,
                    position_side=reservation.position_key.position_side.value,
                    batch_id=reservation.batch_id,
                    command_id=reservation.command_id,
                    client_order_id=None,
                    reserved_quantity=reservation.reserved_quantity,
                    consumed_quantity=reservation.consumed_quantity,
                    released_quantity=reservation.released_quantity,
                    status="ACTIVE",
                    created_at=reservation.created_at,
                    updated_at=now,
                )
            )

    async def update_reservation_in_session(
        self,
        session: AsyncSession,
        reservation: PositionReservation,
        release_reason: str | None = None,
    ) -> None:
        """Apply a monotonic reservation settlement on the shared transaction."""
        row = await session.get(
            PositionReservationRow,
            reservation.reservation_id,
            with_for_update=True,
        )
        if row is None:
            raise ReservationConflictError(
                f"reservation {reservation.reservation_id} is missing"
            )
        if (
            row.command_id != reservation.command_id
            or row.environment != reservation.position_key.environment
            or row.account_label != reservation.position_key.account_label
            or row.symbol != reservation.position_key.symbol
            or row.position_side != reservation.position_key.position_side.value
            or row.batch_id != reservation.batch_id
            or row.reserved_quantity != reservation.reserved_quantity
        ):
            raise ReservationConflictError(
                f"reservation {reservation.reservation_id} identity changed"
            )
        if (
            reservation.consumed_quantity < row.consumed_quantity
            or reservation.released_quantity < row.released_quantity
            or reservation.consumed_quantity + reservation.released_quantity
            > row.reserved_quantity
        ):
            raise ReservationConflictError(
                f"reservation {reservation.reservation_id} settlement regressed "
                "or exceeded its reserved quantity"
            )
        now = datetime.now(UTC)
        status = (
            "ACTIVE"
            if reservation.active_quantity > Decimal("0")
            else (
                "COMMITTED"
                if reservation.consumed_quantity > Decimal("0")
                else "RELEASED"
            )
        )
        row.consumed_quantity = reservation.consumed_quantity
        row.released_quantity = reservation.released_quantity
        row.status = status
        row.updated_at = now
        row.released_at = now if status == "RELEASED" else None
        row.release_reason = release_reason

    async def update_reservation(
        self,
        reservation: PositionReservation,
        release_reason: str | None = None,
    ) -> None:
        now = datetime.now(UTC)
        status = "ACTIVE"
        if reservation.active_quantity <= Decimal("0"):
            status = (
                "COMMITTED"
                if reservation.consumed_quantity > Decimal("0")
                else "RELEASED"
            )

        async with self._session_maker() as session, session.begin():
            stmt = (
                update(PositionReservationRow)
                .where(
                    PositionReservationRow.reservation_id == reservation.reservation_id
                )
                .values(
                    consumed_quantity=reservation.consumed_quantity,
                    released_quantity=reservation.released_quantity,
                    status=status,
                    updated_at=now,
                    released_at=now if status == "RELEASED" else None,
                    release_reason=release_reason,
                )
            )
            await session.execute(stmt)

    async def load_active_reservations(
        self,
        key: PositionKey | None = None,
    ) -> tuple[PositionReservation, ...]:
        async with self._session_maker() as session:
            query = (
                select(PositionReservationRow)
                .where(PositionReservationRow.status == "ACTIVE")
                .order_by(
                    PositionReservationRow.created_at,
                    PositionReservationRow.reservation_id,
                )
            )
            if key is not None:
                query = query.where(
                    PositionReservationRow.environment == key.environment,
                    PositionReservationRow.account_label == key.account_label,
                    PositionReservationRow.symbol == key.symbol,
                    PositionReservationRow.position_side == key.position_side.value,
                )
            result = await session.scalars(query)
            rows = result.all()

        reservations: list[PositionReservation] = []
        for r in rows:
            res = _row_to_reservation(r)
            if res.active_quantity > Decimal("0"):
                reservations.append(res)
        return tuple(reservations)

    async def load_reservation(self, reservation_id: str) -> PositionReservation | None:
        async with self._session_maker() as session:
            row = await session.get(PositionReservationRow, reservation_id)
            if row is None:
                return None
            return _row_to_reservation(row)
