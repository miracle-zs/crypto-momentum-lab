"""Pure selection of bounded account fill provenance scans."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import (
    AccountFillSourceAnchor,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.snapshot_encoding import (
    stable_snapshot_anchor_id,
)


@dataclass(frozen=True, slots=True)
class FillScanPlan:
    symbol: str
    position_side: str
    start_time_ms: int
    checked_through: datetime
    source_anchor_id: str
    source_anchor_event_cut: datetime
    source_anchor_kind: str
    source_stream_id: str | None
    source_stream_epoch: str | None


def plan_fill_scan(
    position: AccountPositionSnapshot,
    source_anchor: AccountFillSourceAnchor | None,
) -> FillScanPlan | None:
    """Require a verified cut and skip empty or inverted scan intervals."""
    if source_anchor is not None:
        source_anchor_id = source_anchor.checkpoint_id
        source_anchor_cut = source_anchor.event_cut
        source_anchor_kind = "recovery_checkpoint"
        source_stream_id = source_anchor.stream_id
        source_stream_epoch = source_anchor.stream_epoch
    elif position.position_amt == Decimal("0"):
        source_anchor_id = stable_snapshot_anchor_id(position)
        source_anchor_cut = position.observed_at
        source_anchor_kind = "zero_snapshot"
        source_stream_id = None
        source_stream_epoch = None
    else:
        return None
    if source_anchor_cut >= position.observed_at:
        return None
    origin_ms = int(source_anchor_cut.timestamp() * 1000)
    if origin_ms > int(position.observed_at.timestamp() * 1000):
        return None
    return FillScanPlan(
        symbol=position.symbol.strip().upper(),
        position_side=position.position_side.strip().upper(),
        start_time_ms=origin_ms,
        checked_through=position.observed_at,
        source_anchor_id=source_anchor_id,
        source_anchor_event_cut=source_anchor_cut,
        source_anchor_kind=source_anchor_kind,
        source_stream_id=source_stream_id,
        source_stream_epoch=source_stream_epoch,
    )
