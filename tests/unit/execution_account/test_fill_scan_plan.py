from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account.models import (
    AccountFillSourceAnchor,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.snapshot_encoding import (
    stable_snapshot_anchor_id,
)
from crypto_momentum_lab.execution_account.fill_scan_plan import plan_fill_scan

OBSERVED_AT = datetime(2026, 10, 1, 12, 0, 0, 123000, tzinfo=UTC)


def position(amount="1"):
    return AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol=" btcusdt ",
        position_side=" long ",
        position_amt=Decimal(amount),
        entry_price=Decimal("10"),
        mark_price=Decimal("10"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("10"),
        leverage=5,
        margin_type="CROSSED",
        observed_at=OBSERVED_AT,
        raw_payload={},
    )


def anchor(cut):
    return AccountFillSourceAnchor(
        symbol="BTCUSDT",
        position_side="LONG",
        checkpoint_id="checkpoint-1",
        event_cut=cut,
        stream_id="stream-1",
        stream_epoch="epoch-2",
    )


@pytest.mark.parametrize("amount", ["0", "1", "-1"])
def test_missing_checkpoint_cannot_create_a_scan(amount):
    assert plan_fill_scan(position(amount), None) is None


@pytest.mark.parametrize("offset", [timedelta(0), timedelta(microseconds=1)])
def test_equal_or_future_checkpoint_cut_cannot_create_a_scan(offset):
    assert plan_fill_scan(position(), anchor(OBSERVED_AT + offset)) is None


@pytest.mark.parametrize("amount", ["0", "1"])
def test_checkpoint_provenance_is_preserved_even_for_flat_position(amount):
    cut = OBSERVED_AT - timedelta(seconds=1, microseconds=456)
    plan = plan_fill_scan(position(amount), anchor(cut))
    assert plan is not None
    assert plan.symbol == "BTCUSDT"
    assert plan.position_side == "LONG"
    assert plan.start_time_ms == 1790855999122
    assert plan.checked_through == OBSERVED_AT
    assert plan.source_anchor_id == "checkpoint-1"
    assert plan.source_anchor_event_cut == cut
    assert plan.source_anchor_kind == "recovery_checkpoint"
    assert plan.source_stream_id == "stream-1"
    assert plan.source_stream_epoch == "epoch-2"


def test_earlier_cut_in_same_millisecond_keeps_existing_scan_semantics():
    cut = OBSERVED_AT - timedelta(microseconds=1)
    plan = plan_fill_scan(position(), anchor(cut))
    assert plan is not None
    assert plan.start_time_ms == 1790856000122
    assert plan.source_anchor_event_cut == cut


def test_zero_snapshot_anchor_retains_pre_extraction_digest():
    assert stable_snapshot_anchor_id(position("0")) == (
        "psnap_15ae10caf60f749c54609056d7ec1a83ecc042e655f04406fb93d704b51e34a9"
    )


def test_loaded_zero_baseline_constructs_actual_transport_scan():
    from dataclasses import replace

    from crypto_momentum_lab.domain.account.models import (
        AccountFillLoadScan,
        AccountFillPageScan,
    )

    baseline = replace(
        position("0"),
        symbol="BTCUSDT",
        position_side="LONG",
        observed_at=OBSERVED_AT - timedelta(minutes=1),
    )
    loaded = AccountFillSourceAnchor(
        "BTCUSDT",
        "LONG",
        stable_snapshot_anchor_id(baseline),
        baseline.observed_at,
        "exchange_snapshot",
        "persisted-row-id",
        zero_snapshot=baseline,
    )
    target = replace(position("0"), symbol="BTCUSDT", position_side="LONG")
    plan = plan_fill_scan(target, loaded)
    scan = AccountFillLoadScan(
        "live",
        "primary",
        "BTCUSDT",
        "LONG",
        AccountFillPageScan(
            "BTCUSDT",
            "load",
            plan.start_time_ms,
            None,
            1,
            True,
            False,
            target.observed_at,
        ),
        target.observed_at,
        plan.source_anchor_id,
        plan.source_anchor_event_cut,
        plan.source_anchor_kind,
        plan.source_stream_id,
        plan.source_stream_epoch,
        source_anchor_snapshot=plan.source_anchor_snapshot,
    )
    assert scan.source_stream_id is None and scan.source_stream_epoch is None
    assert scan.source_anchor_snapshot is baseline
