"""Reservation port adaptation, awaited recovery and identity failure blocking."""

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
    InMemoryPositionReservationRepository,
)
from crypto_momentum_lab.domain.execution.legacy_reservation_repository import (
    LegacyReservationRepositoryAdapter,
    assemble_legacy_execution_book,
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


def test_explicit_sync_assembly_preserves_constructor_recovery(reservation):
    repo = InMemoryPositionReservationRepository()
    repo.save_reservation(reservation)
    book = assemble_legacy_execution_book(reservation_repository=repo)
    assert book.get_active_reservations(reservation.position_key) == (reservation,)


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


async def test_explicit_legacy_async_assembly_does_not_attempt_sync_recovery(
    reservation,
):
    class AsyncRepository:
        async def load_active_reservations(self):
            return (reservation,)

    book = assemble_legacy_execution_book(reservation_repository=AsyncRepository())
    assert not book.get_active_reservations(reservation.position_key)
    await book.restore(account_label="account-3")
    assert book.get_active_reservations(reservation.position_key) == (reservation,)


async def test_single_save_legacy_adapter_preserves_version_and_update_reason(
    reservation,
):
    class SingleSave:
        def __init__(self):
            self.saved = []

        def save_reservation(self, value, expected_projection_version=None):
            self.saved.append((value, expected_projection_version))

        def update_reservation(self, value, release_reason=None):
            self.updated = value, release_reason

    repo = SingleSave()
    adapter = LegacyReservationRepositoryAdapter(repo)
    assert await adapter.load_reservation("missing") is None
    await adapter.save_reservations(
        (reservation,),
        expected_projection_version="token",
        batch_quantities={"batch-1": Decimal("2")},
    )
    assert repo.saved == [(reservation, "token")]
    released = reservation.release(Decimal("2"))
    await adapter.update_reservation(released, release_reason="terminal")
    assert repo.updated == (released, "terminal")


async def test_missing_save_capability_never_reports_success(reservation):
    with pytest.raises(RuntimeError, match="save_reservation"):
        await LegacyReservationRepositoryAdapter(object()).save_reservations(
            (reservation,)
        )


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


async def test_missing_update_failure_preserves_original_error(reservation):
    with pytest.raises(RuntimeError, match="update_reservation"):
        await LegacyReservationRepositoryAdapter(object()).update_reservation(
            reservation
        )
