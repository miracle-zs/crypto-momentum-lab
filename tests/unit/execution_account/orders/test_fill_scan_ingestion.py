from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from crypto_momentum_lab.domain.account.models import (
    AccountFillLoadScan,
    AccountFillPageScan,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.observation_models import Applied
from crypto_momentum_lab.domain.execution.snapshot_encoding import (
    stable_snapshot_anchor_id,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)

NOW = datetime(2026, 10, 1, tzinfo=UTC)


def snapshot(at=NOW):
    return AccountPositionSnapshot(
        "live",
        "primary",
        "BTCUSDT",
        "LONG",
        Decimal("0"),
        Decimal("0"),
        Decimal("10"),
        Decimal("0"),
        Decimal("0"),
        5,
        "cross",
        at,
        {},
    )


def scan():
    anchor = snapshot(NOW - timedelta(seconds=30))
    return AccountFillLoadScan(
        "live",
        "primary",
        "BTCUSDT",
        "LONG",
        AccountFillPageScan(
            "BTCUSDT",
            "full-scan",
            int(anchor.observed_at.timestamp() * 1000),
            None,
            1,
            True,
            False,
            NOW,
        ),
        NOW,
        stable_snapshot_anchor_id(anchor),
        anchor.observed_at,
        "zero_snapshot",
        source_anchor_snapshot=anchor,
    )


def coordinator():
    book = MagicMock()
    book.has_execution_unit_of_work = True
    book.observe = AsyncMock(return_value=Applied("e", "v"))
    book.load_recovery_checkpoint = AsyncMock(return_value=None)
    return OrderExecutionCoordinator(
        backend=MagicMock(),
        environment="live",
        account_label="primary",
        execution_book=book,
    ), book


async def test_real_coordinator_carries_verified_scan_into_book():
    runtime, book = coordinator()
    source = scan()
    await runtime.observe_account_snapshot(
        snapshot(),
        stream_id="hub",
        stream_epoch="epoch",
        sequence=1,
        fill_load_scans=(source,),
    )
    evidence = book.observe.await_args.args[0]
    assert evidence.source_anchor_snapshot == source.source_anchor_snapshot
    assert evidence.fill_load_provenance.load_id == "full-scan"
    assert evidence.coverage_evidence.proves_complete(
        source.source_anchor_event_cut,
        NOW,
        expected_scope=evidence.fill_load_provenance.stream_scope,
    )


async def test_scan_is_not_suppressed_by_repeated_flat_snapshot_cache():
    runtime, book = coordinator()
    await runtime.observe_account_snapshot(
        snapshot(), stream_id="hub", stream_epoch="epoch", sequence=1
    )
    await runtime.observe_account_snapshot(
        snapshot(),
        stream_id="hub",
        stream_epoch="epoch",
        sequence=2,
        fill_load_scans=(scan(),),
    )
    assert book.observe.await_count == 2


async def test_truncated_scan_never_proves_complete():
    runtime, book = coordinator()
    source = scan()
    source = replace(
        source,
        page_scan=replace(
            source.page_scan, page_exhausted=False, truncated=True, checked_through=None
        ),
    )
    await runtime.observe_account_snapshot(
        snapshot(),
        stream_id="hub",
        stream_epoch="epoch",
        sequence=1,
        fill_load_scans=(source,),
    )
    evidence = book.observe.await_args.args[0]
    assert not evidence.coverage_evidence.proves_complete(
        source.source_anchor_event_cut, NOW
    )


async def test_scan_without_matching_explicit_snapshot_is_rejected():
    runtime, book = coordinator()
    with pytest.raises(ValueError, match="snapshot"):
        await runtime.observe_account_snapshot(
            None,
            stream_id="hub",
            stream_epoch="epoch",
            sequence=1,
            fill_load_scans=(scan(),),
        )
    book.observe.assert_not_awaited()


def test_scan_baseline_round_trips_through_real_hub_codec():
    from crypto_momentum_lab.execution_account.hub import (
        AccountEvent,
        decode_account_event,
        encode_account_event,
    )

    source = scan()
    event = AccountEvent(
        "live",
        "primary",
        "snapshot",
        "snapshot-proof",
        NOW,
        NOW,
        fill_load_scans=(source,),
    )
    decoded = decode_account_event(
        encode_account_event(event, sequence=1),
        expected_environment="live",
        expected_account_label="primary",
    )
    assert decoded.fill_load_scans == (source,)
    assert (
        decoded.fill_load_scans[0].source_anchor_snapshot
        == source.source_anchor_snapshot
    )
