"""PostgreSQL persistence adapter for batch-level PositionReservation.

Obeys Astra Architecture Blueprint 2026-09-25 (P2):
- Durable, transactional storage of batch-level lot reservations;
- Enforces CAS and optimistic reservation persistence across process restarts;
- Supports crash recovery rehydration of active in-flight reservations;
- Implements synchronous PositionReservationRepository protocol
  as well as async helpers.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session, sessionmaker

from crypto_momentum_lab.domain.execution.execution_coordinator import (
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
    return int(getattr(result, "rowcount", 0) or 0)


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


def _acquire_reservation_lock(session: Session, lock_key: str) -> None:
    """Take the per-position advisory lock; fail closed on PostgreSQL errors.

    Non-PostgreSQL test binds (SQLite) have no advisory locks — skip only
    there, never on a real database.
    """
    bind = session.get_bind()
    if bind is not None and bind.dialect.name != "postgresql":
        return
    try:
        session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
            {"lock_key": lock_key},
        )
    except Exception as lock_err:
        raise ReservationConflictError(
            f"reservation lock unavailable: {lock_err}"
        ) from lock_err


async def _acquire_reservation_lock_async(
    session: AsyncSession, lock_key: str
) -> None:
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


def _load_position_amt_sync(
    session: Session,
    reservation: PositionReservation,
) -> Decimal | None:
    """Return the latest signed position amount, or None if missing."""
    pos_snap = session.scalars(
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
    ).first()
    return pos_snap.position_amt if pos_snap is not None else None


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
            "position snapshot missing; refusing to reserve without "
            "proven capacity"
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
    batch_quantity: Decimal | None,
) -> None:
    """Fail closed when a single batch would be over-reserved."""
    if batch_quantity is None:
        return
    if batch_quantity <= Decimal("0"):
        raise ReservationConflictError(
            f"batch {batch_id} quantity must be positive"
        )
    if total_active_for_batch + reserved_quantity > batch_quantity:
        raise ReservationConflictError(
            f"batch {batch_id} over-reserved: requested {reserved_quantity}, "
            f"already reserved {total_active_for_batch}, "
            f"batch quantity {batch_quantity}"
        )


class PostgresPositionReservationRepository:
    """Synchronous PostgreSQL repository implementing PositionReservationRepository."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        strategy_name: str = "default",
    ) -> None:
        self._session_factory = session_factory
        self._strategy_name = strategy_name

    def save_reservation(
        self,
        reservation: PositionReservation,
        expected_projection_version: str | None = None,
        expires_at: datetime | None = None,
        batch_quantity: Decimal | None = None,
    ) -> None:
        """Persists a new PositionReservation into PostgreSQL.

        Fails closed on lock failure, unproven capacity, or ID conflict with
        a different batch/quantity. Same-ID ACTIVE retries that match the
        plan are adopted, never double-inserted. Terminal rows are rejected.
        """
        self.save_reservations(
            (reservation,),
            expected_projection_version=expected_projection_version,
            expires_at=expires_at,
            batch_quantities=(
                {reservation.batch_id: batch_quantity}
                if batch_quantity is not None
                else None
            ),
        )

    def save_reservations(
        self,
        reservations: Sequence[PositionReservation],
        expected_projection_version: str | None = None,
        expires_at: datetime | None = None,
        batch_quantities: Mapping[str, Decimal] | None = None,
    ) -> None:
        """Insert a set of reservations in one transaction, or not at all."""
        if not reservations:
            return
        now = datetime.now(UTC)
        first = reservations[0]
        strat = getattr(first.position_key, "strategy_name", self._strategy_name)
        batch_quantities = batch_quantities or {}
        with self._session_factory() as session, session.begin():
            _acquire_reservation_lock(
                session, f"res_{first.position_key.canonical_id}"
            )
            active_rows = session.scalars(
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
            ).all()
            if expected_projection_version is not None:
                for row in active_rows:
                    if (
                        row.expected_projection_version is not None
                        and row.expected_projection_version
                        != expected_projection_version
                    ):
                        raise ReservationConflictError(
                            f"projection version mismatch: expected "
                            f"{expected_projection_version}, active "
                            f"{row.expected_projection_version}"
                        )
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
                existing_row = session.get(
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
                already = pending_batch.get(reservation.batch_id, Decimal("0"))
                _require_batch_capacity(
                    batch_id=reservation.batch_id,
                    reserved_quantity=reservation.reserved_quantity,
                    total_active_for_batch=batch_active.get(
                        reservation.batch_id, Decimal("0")
                    )
                    + already,
                    batch_quantity=batch_quantities.get(reservation.batch_id),
                )
                pending_batch[reservation.batch_id] = (
                    already + reservation.reserved_quantity
                )
                total_active += reservation.reserved_quantity

                try:
                    pos_amt = _load_position_amt_sync(session, reservation)
                except Exception as snap_err:
                    raise ReservationConflictError(
                        f"position snapshot capacity check failed: {snap_err}"
                    ) from snap_err
                _require_capacity(
                    reserved_quantity=reservation.reserved_quantity,
                    total_active=total_active
                    - reservation.reserved_quantity,
                    pos_amt=pos_amt,
                )
                stmt = (
                    insert(PositionReservationRow)
                    .values(
                        reservation_id=reservation.reservation_id,
                        environment=reservation.position_key.environment,
                        account_label=reservation.position_key.account_label,
                        strategy_name=strat,
                        symbol=reservation.position_key.symbol,
                        position_side=(
                            reservation.position_key.position_side.value
                        ),
                        batch_id=reservation.batch_id,
                        command_id=reservation.command_id,
                        client_order_id=None,
                        reserved_quantity=reservation.reserved_quantity,
                        consumed_quantity=reservation.consumed_quantity,
                        released_quantity=reservation.released_quantity,
                        expected_projection_version=(
                            expected_projection_version
                        ),
                        status="ACTIVE",
                        created_at=reservation.created_at,
                        updated_at=now,
                        expires_at=expires_at,
                    )
                    .on_conflict_do_nothing()
                )
                result = session.execute(stmt)
                if _insert_rowcount(result) == 0:
                    existing_row = session.get(
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

    def update_reservation(
        self,
        reservation: PositionReservation,
        release_reason: str | None = None,
    ) -> None:
        """Updates consumed/released quantities and status of a reservation."""
        now = datetime.now(UTC)
        status = "ACTIVE"
        if reservation.active_quantity <= Decimal("0"):
            status = (
                "COMMITTED"
                if reservation.consumed_quantity > Decimal("0")
                else "RELEASED"
            )

        with self._session_factory() as session, session.begin():
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
            session.execute(stmt)

    def load_active_reservations(
        self,
        key: PositionKey | None = None,
    ) -> tuple[PositionReservation, ...]:
        """Loads all active reservations from PostgreSQL in stable order.

        Stable ordering keeps partial-fill consumption attributable to the
        same batch sequence regardless of heap/page layout.
        """
        with self._session_factory() as session:
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
            rows = session.scalars(query).all()

        reservations: list[PositionReservation] = []
        for r in rows:
            res = _row_to_reservation(r)
            if res.active_quantity > Decimal("0"):
                reservations.append(res)
        return tuple(reservations)

    def load_reservation(self, reservation_id: str) -> PositionReservation | None:
        """Loads a single reservation by ID."""
        with self._session_factory() as session:
            row = session.get(PositionReservationRow, reservation_id)
            if row is None:
                return None
            return _row_to_reservation(row)

    def release_expired_reservations(self, before: datetime) -> int:
        """Releases active reservations whose expires_at is older than before."""
        now = datetime.now(UTC)
        with self._session_factory() as session, session.begin():
            stmt = (
                update(PositionReservationRow)
                .where(
                    PositionReservationRow.status == "ACTIVE",
                    PositionReservationRow.expires_at.is_not(None),
                    PositionReservationRow.expires_at < before,
                )
                .values(
                    released_quantity=PositionReservationRow.reserved_quantity
                    - PositionReservationRow.consumed_quantity,
                    status="RELEASED",
                    updated_at=now,
                    released_at=now,
                    release_reason="EXPIRED",
                )
            )
            result = session.execute(stmt)
            return _insert_rowcount(result)


class AsyncPostgresPositionReservationRepository:
    """Async PostgreSQL repository for durable lot reservations in async daemons."""

    def __init__(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        strategy_name: str = "default",
    ) -> None:
        self._session_maker = session_maker
        self._strategy_name = strategy_name

    async def save_reservation(
        self,
        reservation: PositionReservation,
        expected_projection_version: str | None = None,
        expires_at: datetime | None = None,
        batch_quantity: Decimal | None = None,
    ) -> None:
        await self.save_reservations(
            (reservation,),
            expected_projection_version=expected_projection_version,
            expires_at=expires_at,
            batch_quantities=(
                {reservation.batch_id: batch_quantity}
                if batch_quantity is not None
                else None
            ),
        )

    async def save_reservations(
        self,
        reservations: Sequence[PositionReservation],
        expected_projection_version: str | None = None,
        expires_at: datetime | None = None,
        batch_quantities: Mapping[str, Decimal] | None = None,
    ) -> None:
        if not reservations:
            return
        now = datetime.now(UTC)
        first = reservations[0]
        strat = getattr(first.position_key, "strategy_name", self._strategy_name)
        batch_quantities = batch_quantities or {}
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
            if expected_projection_version is not None:
                for row in active_rows:
                    if (
                        row.expected_projection_version is not None
                        and row.expected_projection_version
                        != expected_projection_version
                    ):
                        raise ReservationConflictError(
                            f"projection version mismatch: expected "
                            f"{expected_projection_version}, active "
                            f"{row.expected_projection_version}"
                        )
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
                already = pending_batch.get(reservation.batch_id, Decimal("0"))
                _require_batch_capacity(
                    batch_id=reservation.batch_id,
                    reserved_quantity=reservation.reserved_quantity,
                    total_active_for_batch=batch_active.get(
                        reservation.batch_id, Decimal("0")
                    )
                    + already,
                    batch_quantity=batch_quantities.get(reservation.batch_id),
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
                        position_side=(
                            reservation.position_key.position_side.value
                        ),
                        batch_id=reservation.batch_id,
                        command_id=reservation.command_id,
                        client_order_id=None,
                        reserved_quantity=reservation.reserved_quantity,
                        consumed_quantity=reservation.consumed_quantity,
                        released_quantity=reservation.released_quantity,
                        expected_projection_version=(
                            expected_projection_version
                        ),
                        status="ACTIVE",
                        created_at=reservation.created_at,
                        updated_at=now,
                        expires_at=expires_at,
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

    async def load_reservation(
        self, reservation_id: str
    ) -> PositionReservation | None:
        async with self._session_maker() as session:
            row = await session.get(PositionReservationRow, reservation_id)
            if row is None:
                return None
            return _row_to_reservation(row)
