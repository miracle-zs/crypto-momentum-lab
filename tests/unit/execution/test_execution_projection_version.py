"""The exit acceptance seam must distinguish fresh observations from new exposure."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent, AccountPositionSnapshot
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_book import (
    Accepted,
    ExecutionBook,
    ExecutionRequest,
    StaleView,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    FactCoverageInterval,
    FactCoverageStatus,
)
from crypto_momentum_lab.domain.execution.trade_command import TradeCommandType
from crypto_momentum_lab.domain.strategy import StrategySide


async def seeded_position():
    book = ExecutionBook()
    scope = ExecutionScope("live", "primary", "ORCAUSDT", FuturesPositionSide.LONG)
    t0 = datetime(2026, 10, 9, 3, 29, 59, tzinfo=UTC)
    fill = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="ORCAUSDT",
        trade_id="open-1",
        order_id="order-1",
        side="BUY",
        price=Decimal("2.455"),
        quantity=Decimal("40.7"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG", "is_system": True},
    )
    snapshot = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="ORCAUSDT",
        position_side="LONG",
        position_amt=Decimal("40.7"),
        entry_price=Decimal("2.455"),
        mark_price=Decimal("2.47965848"),
        unrealized_pnl=Decimal("1.00360013"),
        notional=Decimal("100.92210013"),
        leverage=5,
        margin_type="cross",
        observed_at=t0,
        raw_payload={},
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="seed",
            scope=scope,
            observed_at=t0,
            fill=fill,
            snapshot=snapshot,
            coverage=FactCoverageInterval(
                start_at=t0, end_at=t0, status=FactCoverageStatus.CONFIRMED
            ),
        )
    )
    view = await book.read(scope)
    return book, scope, view, fill, snapshot


def exit_request(scope, view):
    return ExecutionRequest(
        request_id="exit-orca",
        scope=scope,
        strategy_name="test",
        run_id="run",
        decision_ref="bearish-candle",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("40.7"),
        side=StrategySide.LONG,
    )


@pytest.mark.asyncio
async def test_exit_candidate_survives_equal_snapshot_refresh():
    book, scope, before, fill, snapshot = await seeded_position()
    refreshed = replace(
        snapshot, observed_at=snapshot.observed_at + timedelta(seconds=3)
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="refresh",
            scope=scope,
            observed_at=refreshed.observed_at,
            snapshot=refreshed,
        )
    )
    after = await book.read(scope)
    assert after.input_revision > before.input_revision
    result = await book.act(exit_request(scope, before))
    assert isinstance(result, Accepted)
    assert sum(r.reserved_quantity for r in result.receipt.reservations) == Decimal(
        "40.7"
    )


@pytest.mark.asyncio
async def test_new_fill_still_rejects_old_exit_candidate():
    book, scope, before, fill, snapshot = await seeded_position()
    newer = replace(
        fill,
        trade_id="open-2",
        order_id="order-2",
        quantity=Decimal("1"),
        trade_at=fill.trade_at + timedelta(seconds=3),
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="new-fill",
            scope=scope,
            observed_at=newer.trade_at,
            fill=newer,
            snapshot=replace(
                snapshot, position_amt=Decimal("41.7"), observed_at=newer.trade_at
            ),
        )
    )
    assert isinstance(await book.act(exit_request(scope, before)), StaleView)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("position_amt", Decimal("40.6")),
        ("entry_price", Decimal("2.456")),
        ("leverage", 6),
        ("margin_type", "isolated"),
    ],
)
async def test_changed_exposure_or_risk_still_rejects_old_candidate(field, value):
    book, scope, before, fill, snapshot = await seeded_position()
    refreshed = replace(
        snapshot,
        observed_at=snapshot.observed_at + timedelta(seconds=3),
        **{field: value},
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="changed",
            scope=scope,
            observed_at=refreshed.observed_at,
            snapshot=refreshed,
        )
    )
    assert isinstance(await book.act(exit_request(scope, before)), StaleView)


@pytest.mark.asyncio
async def test_quote_only_change_and_decimal_scale_keep_execution_version():
    book, scope, before, fill, snapshot = await seeded_position()
    refreshed = replace(
        snapshot,
        observed_at=snapshot.observed_at + timedelta(seconds=3),
        position_amt=Decimal("40.7000"),
        entry_price=Decimal("2.4550"),
        mark_price=Decimal("2.48"),
        notional=Decimal("100.936"),
        unrealized_pnl=Decimal("1.0175"),
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="quote",
            scope=scope,
            observed_at=refreshed.observed_at,
            snapshot=refreshed,
        )
    )
    assert isinstance(await book.act(exit_request(scope, before)), Accepted)


@pytest.mark.asyncio
async def test_equal_refresh_preserves_restored_legacy_head_token():
    book, scope, before, fill, snapshot = await seeded_position()
    position_book = book._ensure_book(scope.to_position_key())
    position_book.use_durable_projection_version(
        "pv_legacy", event_cut=before.event_cut
    )
    before = await book.read(scope)
    refreshed = replace(
        snapshot, observed_at=snapshot.observed_at + timedelta(seconds=3)
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="refresh",
            scope=scope,
            observed_at=refreshed.observed_at,
            snapshot=refreshed,
        )
    )
    assert (await book.read(scope)).projection_version == "pv_legacy"
    assert isinstance(await book.act(exit_request(scope, before)), Accepted)


@pytest.mark.asyncio
async def test_changed_reservation_cannot_overbook_an_equal_refresh():
    from crypto_momentum_lab.domain.execution.execution_book import Blocked

    book, scope, before, fill, snapshot = await seeded_position()
    first = await book.act(exit_request(scope, before))
    assert isinstance(first, Accepted)
    refreshed = replace(
        snapshot, observed_at=snapshot.observed_at + timedelta(seconds=3)
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="refresh",
            scope=scope,
            observed_at=refreshed.observed_at,
            snapshot=refreshed,
        )
    )
    second = await book.act(
        replace(exit_request(scope, before), request_id="second-exit")
    )
    assert isinstance(second, Blocked)
    assert len(book.coordinator.get_active_reservations(scope.to_position_key())) == 1


@pytest.mark.asyncio
async def test_exit_boundary_still_invalidates_old_candidate():
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        ExitOrderSubmissionFact,
    )

    book, scope, before, fill, snapshot = await seeded_position()
    at = snapshot.observed_at + timedelta(seconds=3)
    boundary = ExitOrderSubmissionFact(
        order_id="prior-exit",
        submitted_at=at,
        symbol="ORCAUSDT",
        position_side=FuturesPositionSide.LONG,
        target_batch_id=before.batches[0].batch_id,
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="boundary", scope=scope, observed_at=at, boundary=boundary
        )
    )
    assert isinstance(await book.act(exit_request(scope, before)), StaleView)


@pytest.mark.asyncio
async def test_coverage_loss_still_invalidates_old_candidate():
    book, scope, before, fill, snapshot = await seeded_position()
    at = snapshot.observed_at + timedelta(seconds=3)
    await book.observe(
        ExecutionEvidence(
            evidence_id="gap",
            scope=scope,
            observed_at=at,
            coverage=FactCoverageInterval(
                start_at=snapshot.observed_at,
                end_at=at,
                status=FactCoverageStatus.PENDING,
                has_known_gaps=True,
            ),
        )
    )
    assert isinstance(await book.act(exit_request(scope, before)), StaleView)
