"""Unit tests for durable PositionReservation and crash-recovery.

Obeys Astra Architecture Blueprint 2026-09-25 (P2):
- Invariant 3: Side-effects must have single authoritative executor;
- Reservations must survive process restarts;
- Verifies durable repository persistence of lot reservations;
- Verifies crash recovery rehydration of active in-flight reservations;
- Verifies anti-double-dipping conflict detection across restarts.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ExecutionCoordinator,
    InMemoryPositionReservationRepository,
    ReservationConflictError,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    FactCoverageInterval,
    FactCoverageStatus,
    PositionHealthStatus,
    PositionKey,
    PositionLedgerBatch,
    PositionView,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocation,
    ExitAllocationPlan,
    ExitPolicyMode,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide


def _create_ready_view(
    symbol: str = "SANDUSDT",
    batch_qty: Decimal = Decimal("2189.0"),
    entry_price: Decimal = Decimal("0.04568"),
    version: str = "pv_sand_v1",
) -> PositionView:
    pos_key = PositionKey(
        environment="live",
        account_label="account-3",
        symbol=symbol,
        position_side=FuturesPositionSide.BOTH,
    )
    batch = PositionLedgerBatch(
        batch_id="batch_sand_001",
        episode_id="ep_sand_001",
        quantity=batch_qty,
        original_quantity=batch_qty,
        entry_price=entry_price,
        opened_at=datetime(2026, 9, 25, 7, 17, 15, tzinfo=UTC),
    )
    return PositionView(
        key=pos_key,
        projection_version=version,
        input_revision=1,
        event_cut=datetime(2026, 9, 25, 7, 20, 0, tzinfo=UTC),
        policy_version="v1",
        schema_version="v1",
        coverage=FactCoverageInterval(
            start_at=datetime(2026, 9, 25, 7, 0, 0, tzinfo=UTC),
            end_at=datetime(2026, 9, 25, 7, 20, 0, tzinfo=UTC),
            status=FactCoverageStatus.CONFIRMED,
        ),
        active_episode=None,
        batches=(batch,),
        unallocated_quantity=Decimal("0"),
        health_status=PositionHealthStatus.READY,
        reconciliation_gap=Decimal("0"),
    )


def test_durable_reservation_and_crash_recovery() -> None:
    # Shared durable repository (simulating persistent database)
    shared_repo = InMemoryPositionReservationRepository()

    # Instance 1: Create coordinator and make a reservation
    coord1 = ExecutionCoordinator(repository=shared_repo)
    view = _create_ready_view(batch_qty=Decimal("2189.0"), version="pv_sand_v1")

    command1 = TradeCommand(
        command_id="cmd_exit_partial_1",
        position_key=view.key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.SHORT,
        order_type=EntryType.LIMIT,
        requested_quantity=Decimal("1000.0"),
        limit_price=Decimal("0.04600"),
        expected_projection_version="pv_sand_v1",
        allocation_plan=ExitAllocationPlan(
            position_key=view.key,
            allocations=(
                ExitAllocation(
                    batch_id="batch_sand_001",
                    allocated_quantity=Decimal("1000.0"),
                    entry_price=Decimal("0.04568"),
                ),
            ),
            total_allocated_quantity=Decimal("1000.0"),
            policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
            projection_version="pv_sand_v1",
        ),
    )

    reservations = coord1.reserve_exit(command1, view)
    assert len(reservations) == 1
    res1 = reservations[0]
    assert res1.reserved_quantity == Decimal("1000.0")
    assert res1.active_quantity == Decimal("1000.0")

    # Available quantity on coord1 should now be 1189.0
    avail1 = coord1.get_available_batch_quantity(view, "batch_sand_001")
    assert avail1 == Decimal("1189.0")

    # --- SIMULATE PROCESS CRASH & RESTART ---
    # Instance 2: New coordinator spins up with same shared repository
    coord2 = ExecutionCoordinator(repository=shared_repo)

    # In-flight reservation must be recovered on startup!
    recovered = coord2.get_active_reservations(view.key)
    assert len(recovered) == 1
    assert recovered[0].reservation_id == res1.reservation_id
    assert recovered[0].active_quantity == Decimal("1000.0")

    avail2 = coord2.get_available_batch_quantity(view, "batch_sand_001")
    assert avail2 == Decimal("1189.0")

    # Instance 2 tries to reserve 1500.0 on the same batch -> Must be rejected!
    command2 = TradeCommand(
        command_id="cmd_exit_partial_2",
        position_key=view.key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.SHORT,
        order_type=EntryType.LIMIT,
        requested_quantity=Decimal("1500.0"),
        limit_price=Decimal("0.04600"),
        expected_projection_version="pv_sand_v1",
        allocation_plan=ExitAllocationPlan(
            position_key=view.key,
            allocations=(
                ExitAllocation(
                    batch_id="batch_sand_001",
                    allocated_quantity=Decimal("1500.0"),
                    entry_price=Decimal("0.04568"),
                ),
            ),
            total_allocated_quantity=Decimal("1500.0"),
            policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
            projection_version="pv_sand_v1",
        ),
    )

    with pytest.raises(
        ReservationConflictError, match="insufficient available quantity"
    ):
        coord2.reserve_exit(command2, view)


def test_fill_reconciliation_updates_persistent_reservation() -> None:
    shared_repo = InMemoryPositionReservationRepository()
    coord = ExecutionCoordinator(repository=shared_repo)
    view = _create_ready_view()

    command = TradeCommand(
        command_id="cmd_exit_1",
        position_key=view.key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.SHORT,
        order_type=EntryType.LIMIT,
        requested_quantity=Decimal("500.0"),
        allocation_plan=ExitAllocationPlan(
            position_key=view.key,
            allocations=(
                ExitAllocation(
                    batch_id="batch_sand_001",
                    allocated_quantity=Decimal("500.0"),
                    entry_price=Decimal("0.04568"),
                ),
            ),
            total_allocated_quantity=Decimal("500.0"),
            policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
        ),
    )

    reservations = coord.reserve_exit(command, view)
    res_id = reservations[0].reservation_id

    # Partial fill of 300
    coord.reconcile_fill(res_id, Decimal("300.0"))

    # Verify repository has updated consumed quantity
    persisted = shared_repo.load_reservation(res_id)
    assert persisted is not None
    assert persisted.consumed_quantity == Decimal("300.0")
    assert persisted.active_quantity == Decimal("200.0")

    # Complete fill of remaining 200
    coord.reconcile_fill(res_id, Decimal("200.0"))
    persisted_complete = shared_repo.load_reservation(res_id)
    assert persisted_complete is not None
    assert persisted_complete.active_quantity == Decimal("0")


def test_reservation_release_on_cancellation_updates_repository() -> None:
    shared_repo = InMemoryPositionReservationRepository()
    coord = ExecutionCoordinator(repository=shared_repo)
    view = _create_ready_view()

    command = TradeCommand(
        command_id="cmd_exit_cancel_me",
        position_key=view.key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.SHORT,
        order_type=EntryType.LIMIT,
        requested_quantity=Decimal("600.0"),
        allocation_plan=ExitAllocationPlan(
            position_key=view.key,
            allocations=(
                ExitAllocation(
                    batch_id="batch_sand_001",
                    allocated_quantity=Decimal("600.0"),
                    entry_price=Decimal("0.04568"),
                ),
            ),
            total_allocated_quantity=Decimal("600.0"),
            policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
        ),
    )

    reservations = coord.reserve_exit(command, view)
    res_id = reservations[0].reservation_id

    # Order cancelled -> release reservation
    coord.release_reservation(res_id)

    persisted = shared_repo.load_reservation(res_id)
    assert persisted is not None
    assert persisted.released_quantity == Decimal("600.0")
    assert persisted.active_quantity == Decimal("0")

    # Full batch quantity (2189.0) should be available again
    assert coord.get_available_batch_quantity(view, "batch_sand_001") == Decimal(
        "2189.0"
    )


def test_postgres_position_reservation_repository_sync_contract_with_coordinator() -> None:
    """Regression test: PostgresPositionReservationRepository must match synchronous ExecutionCoordinator protocol."""
    from unittest.mock import patch

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from crypto_momentum_lab.persistence.postgres.base import Base
    from crypto_momentum_lab.persistence.postgres.models import PositionReservationRow
    from crypto_momentum_lab.persistence.postgres.position_reservation_repository import (
        PostgresPositionReservationRepository,
    )

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[PositionReservationRow.__table__])
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)

    pg_repo = PostgresPositionReservationRepository(
        session_factory, strategy_name="orderflow_impulse"
    )
    coord1 = ExecutionCoordinator(repository=pg_repo)
    view = _create_ready_view(batch_qty=Decimal("2000.0"), version="pv_sand_v1")

    command = TradeCommand(
        command_id="cmd_pg_exit_1",
        position_key=view.key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.SHORT,
        order_type=EntryType.LIMIT,
        requested_quantity=Decimal("800.0"),
        allocation_plan=ExitAllocationPlan(
            position_key=view.key,
            allocations=(
                ExitAllocation(
                    batch_id="batch_sand_001",
                    allocated_quantity=Decimal("800.0"),
                    entry_price=Decimal("0.04568"),
                ),
            ),
            total_allocated_quantity=Decimal("800.0"),
            policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
        ),
    )

    # SQLite cannot host the JSONB snapshot table; capacity guard is
    # covered separately. Here we only prove the repository protocol.
    with (
        patch(
            "crypto_momentum_lab.persistence.postgres"
            ".position_reservation_repository._require_capacity"
        ),
        patch(
            "crypto_momentum_lab.persistence.postgres"
            ".position_reservation_repository._load_position_amt_sync",
            return_value=Decimal("2000.0"),
        ),
    ):
        reservations = coord1.reserve_exit(command, view)
        assert len(reservations) == 1
        res_id = reservations[0].reservation_id

        # Restart coordinator against the same Postgres repository
        coord2 = ExecutionCoordinator(repository=pg_repo)
        assert coord2.get_available_batch_quantity(
            view, "batch_sand_001"
        ) == Decimal("1200.0")

        # Reconcile fill
        coord2.reconcile_fill(res_id, Decimal("800.0"))
    loaded = pg_repo.load_reservation(res_id)
    assert loaded is not None
    assert loaded.consumed_quantity == Decimal("800.0")
    assert loaded.active_quantity == Decimal("0")

