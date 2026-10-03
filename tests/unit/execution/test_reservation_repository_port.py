"""Awaited reservation recovery and identity failure blocking."""

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.execution_book import (
    Blocked,
    ExecutionBook,
    ExecutionRequest,
)
from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ExecutionCoordinator,
    InMemoryPositionReservationRepository,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.trade_command import (
    PositionReservation,
    TradeCommandType,
)

NOW = datetime(2026, 9, 30, tzinfo=UTC)


@pytest.fixture
def reservation():
    scope = ExecutionScope("live", "account-3", "TESTUSDT", FuturesPositionSide.LONG)
    return PositionReservation(
        "r1",
        "command-1",
        scope.to_position_key(),
        "batch-1",
        Decimal("2"),
        created_at=NOW,
    )


async def test_async_repository_requires_awaited_restore(
    reservation,
):
    class AsyncRepository:
        def __init__(self):
            self.loads = 0

        async def load_active_reservations(self):
            self.loads += 1
            return (reservation,)

    repo = AsyncRepository()
    book = ExecutionBook(reservation_repository=repo)
    assert repo.loads == 0
    await book.restore(account_label="account-3")
    assert repo.loads == 1
    assert book.get_active_reservations(reservation.position_key) == (reservation,)


async def test_identity_lookup_failure_blocks_before_save_or_outbox(reservation):
    repo = AsyncMock()
    repo.load_reservation.side_effect = RuntimeError("database read unavailable")
    book = ExecutionBook(reservation_repository=repo)
    scope = ExecutionScope("live", "account-3", "TESTUSDT", FuturesPositionSide.LONG)
    request = ExecutionRequest(
        request_id="request-1",
        scope=scope,
        strategy_name="strategy",
        strategy_version="1",
        run_id="run",
        decision_ref="decision",
        expected_view_token="*",
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("1"),
        target_batch_ids=("batch-1",),
        created_at=NOW,
    )
    result = await book.act(request)
    assert isinstance(result, Blocked)
    assert "identity lookup failed" in result.reason
    assert book._persistence_failed
    assert request.request_id in book._recovery_required_commands
    repo.save_reservations.assert_not_awaited()
    assert book.get_outbox(request.request_id) is None


def test_candidate_lifecycle_preserves_live_repository(reservation) -> None:
    repo = InMemoryPositionReservationRepository()
    repo.save_reservation(reservation)
    coordinator = ExecutionCoordinator(repository=repo)
    candidate = coordinator.copy_for_transaction()
    changed = replace(reservation, released_quantity=Decimal("1"))
    candidate.update_reservation(changed)

    assert candidate.get_reservation(reservation.reservation_id) == changed
    assert coordinator.get_reservation(reservation.reservation_id) == reservation
    assert repo.load_reservation(reservation.reservation_id) == reservation

    coordinator.publish_from(candidate)
    assert coordinator.get_reservation(reservation.reservation_id) == changed
    assert repo.load_reservation(reservation.reservation_id) == reservation

    coordinator.clear_reservations()
    assert coordinator.get_active_reservations() == ()
    assert coordinator.get_reservation(reservation.reservation_id) is None
    assert repo.load_reservation(reservation.reservation_id) == reservation
    assert coordinator.recover() == 1
    assert coordinator.get_active_reservations() == (reservation,)
