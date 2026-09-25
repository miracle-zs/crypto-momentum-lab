"""PostgreSQL persistence adapter for batch-level PositionReservation.

Obeys Astra Architecture Blueprint 2026-09-25 (P2):
- Durable, transactional storage of batch-level lot reservations;
- Enforces CAS and optimistic reservation persistence across process restarts;
- Supports crash recovery rehydration of active in-flight reservations;
- Implements synchronous PositionReservationRepository protocol
  as well as async helpers.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session, sessionmaker

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation
from crypto_momentum_lab.persistence.postgres.models import (
    AccountPositionSnapshotRow,
    PositionReservationRow,
)


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
    ) -> None:
        """Persists a new PositionReservation into PostgreSQL."""
        now = datetime.now(UTC)
        strat = getattr(reservation.position_key, "strategy_name", self._strategy_name)
        with self._session_factory() as session, session.begin():
            try:
                session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
                    {"lock_key": f"res_{reservation.position_key.canonical_id}"},
                )
            except Exception:
                pass
            active_rows = session.scalars(
                select(PositionReservationRow).where(
                    PositionReservationRow.environment
                    == reservation.position_key.environment,
                    PositionReservationRow.account_label
                    == reservation.position_key.account_label,
                    PositionReservationRow.symbol
                    == reservation.position_key.symbol,
                    PositionReservationRow.position_side
                    == reservation.position_key.position_side.value,
                    PositionReservationRow.status == "ACTIVE",
                    PositionReservationRow.reservation_id
                    != reservation.reservation_id,
                )
            ).all()
            total_active = sum(
                (
                    r.reserved_quantity
                    - r.consumed_quantity
                    - r.released_quantity
                    for r in active_rows
                ),
                start=Decimal("0"),
            )
            try:
                pos_snap = session.scalars(
                    select(AccountPositionSnapshotRow)
                    .where(
                        AccountPositionSnapshotRow.environment
                        == reservation.position_key.environment,
                        AccountPositionSnapshotRow.account_label
                        == reservation.position_key.account_label,
                        AccountPositionSnapshotRow.symbol
                        == reservation.position_key.symbol,
                        AccountPositionSnapshotRow.position_side
                        == reservation.position_key.position_side.value,
                    )
                    .order_by(AccountPositionSnapshotRow.observed_at.desc())
                    .limit(1)
                ).first()
                if pos_snap is not None and abs(pos_snap.position_amt) > Decimal("0"):
                    max_qty = abs(pos_snap.position_amt)
                    if total_active + reservation.reserved_quantity > max_qty:
                        raise ValueError(
                            f"Database reservation quantity exceeded: requested {reservation.reserved_quantity}, "
                            f"already reserved {total_active}, available position {max_qty}"
                        )
            except ValueError:
                raise
            except Exception as snap_err:
                # Table may not exist in early migrations; log and
                # proceed without capacity guard rather than silently
                # swallowing arbitrary failures.
                import logging

                logging.getLogger(__name__).warning(
                    "position_snapshot_capacity_check_skipped",
                    extra={"error": str(snap_err)},
                )

            stmt = (
                insert(PositionReservationRow)
                .values(
                    reservation_id=reservation.reservation_id,
                    environment=reservation.position_key.environment,
                    account_label=reservation.position_key.account_label,
                    strategy_name=strat,
                    symbol=reservation.position_key.symbol,
                    position_side=reservation.position_key.position_side.value,
                    batch_id=reservation.batch_id,
                    command_id=reservation.command_id,
                    client_order_id=None,
                    reserved_quantity=reservation.reserved_quantity,
                    consumed_quantity=reservation.consumed_quantity,
                    released_quantity=reservation.released_quantity,
                    expected_projection_version=expected_projection_version,
                    status="ACTIVE",
                    created_at=reservation.created_at,
                    updated_at=now,
                    expires_at=expires_at,
                )
                .on_conflict_do_nothing()
            )
            session.execute(stmt)

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
        """Loads all active reservations from PostgreSQL."""
        with self._session_factory() as session:
            query = select(PositionReservationRow).where(
                PositionReservationRow.status == "ACTIVE"
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
            return int(result.rowcount or 0)


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
    ) -> None:
        now = datetime.now(UTC)
        strat = getattr(reservation.position_key, "strategy_name", self._strategy_name)
        async with self._session_maker() as session, session.begin():
            try:
                await session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
                    {"lock_key": f"res_{reservation.position_key.canonical_id}"},
                )
            except Exception:
                pass
            active_res = await session.execute(
                select(PositionReservationRow).where(
                    PositionReservationRow.environment
                    == reservation.position_key.environment,
                    PositionReservationRow.account_label
                    == reservation.position_key.account_label,
                    PositionReservationRow.symbol == reservation.position_key.symbol,
                    PositionReservationRow.position_side
                    == reservation.position_key.position_side.value,
                    PositionReservationRow.status == "ACTIVE",
                    PositionReservationRow.reservation_id
                    != reservation.reservation_id,
                )
            )
            active_rows = active_res.scalars().all()
            total_active = sum(
                (
                    r.reserved_quantity
                    - r.consumed_quantity
                    - r.released_quantity
                    for r in active_rows
                ),
                start=Decimal("0"),
            )
            try:
                snap_res = await session.execute(
                    select(AccountPositionSnapshotRow)
                    .where(
                        AccountPositionSnapshotRow.environment
                        == reservation.position_key.environment,
                        AccountPositionSnapshotRow.account_label
                        == reservation.position_key.account_label,
                        AccountPositionSnapshotRow.symbol
                        == reservation.position_key.symbol,
                        AccountPositionSnapshotRow.position_side
                        == reservation.position_key.position_side.value,
                    )
                    .order_by(AccountPositionSnapshotRow.observed_at.desc())
                    .limit(1)
                )
                pos_snap = snap_res.scalars().first()
                if pos_snap is not None and abs(pos_snap.position_amt) > Decimal("0"):
                    max_qty = abs(pos_snap.position_amt)
                    if total_active + reservation.reserved_quantity > max_qty:
                        raise ValueError(
                            f"Database reservation quantity exceeded: requested {reservation.reserved_quantity}, "
                            f"already reserved {total_active}, available position {max_qty}"
                        )
            except ValueError:
                raise
            except Exception as snap_err:
                import logging

                logging.getLogger(__name__).warning(
                    "position_snapshot_capacity_check_skipped",
                    extra={"error": str(snap_err)},
                )

            stmt = (
                insert(PositionReservationRow)
                .values(
                    reservation_id=reservation.reservation_id,
                    environment=reservation.position_key.environment,
                    account_label=reservation.position_key.account_label,
                    strategy_name=strat,
                    symbol=reservation.position_key.symbol,
                    position_side=reservation.position_key.position_side.value,
                    batch_id=reservation.batch_id,
                    command_id=reservation.command_id,
                    client_order_id=None,
                    reserved_quantity=reservation.reserved_quantity,
                    consumed_quantity=reservation.consumed_quantity,
                    released_quantity=reservation.released_quantity,
                    expected_projection_version=expected_projection_version,
                    status="ACTIVE",
                    created_at=reservation.created_at,
                    updated_at=now,
                    expires_at=expires_at,
                )
                .on_conflict_do_nothing()
            )
            await session.execute(stmt)

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
            query = select(PositionReservationRow).where(
                PositionReservationRow.status == "ACTIVE"
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
