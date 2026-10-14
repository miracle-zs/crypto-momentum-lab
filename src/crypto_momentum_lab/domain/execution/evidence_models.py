"""Account evidence values independent of the execution orchestrator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderEvent
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    CoverageEvidence,
    ExitOrderSubmissionFact,
    FactCoverageInterval,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    StreamCheckpointAdoption,
)


@dataclass(frozen=True, slots=True)
class ExecutionEvidence:
    evidence_id: str
    scope: ExecutionScope
    observed_at: datetime
    fill: AccountFillEvent | None = None
    snapshot: AccountPositionSnapshot | None = None
    boundary: ExitOrderSubmissionFact | None = None
    order_event: ExchangeOrderEvent | None = None
    coverage: FactCoverageInterval | None = None
    coverage_evidence: CoverageEvidence | None = None
    fill_load_provenance: AccountFillLoadProvenance | None = None
    fills: tuple[AccountFillEvent, ...] = ()
    stream_checkpoint_adoption: StreamCheckpointAdoption | None = None
    stream_id: str | None = None
    stream_epoch: str | None = None
    sequence: int | None = None
    cumulative_order: ExecutionCumulativeOrderReport | None = None
    source_anchor_snapshot: AccountPositionSnapshot | None = None

    def __post_init__(self) -> None:
        if self.source_anchor_snapshot is not None:
            from crypto_momentum_lab.domain.execution.snapshot_encoding import (
                stable_snapshot_anchor_id,
            )

            anchor = self.source_anchor_snapshot
            provenance = self.fill_load_provenance
            key = self.scope.to_position_key()
            if (
                provenance is None
                or provenance.source_anchor_kind != "zero_snapshot"
                or anchor.position_amt != 0
                or anchor.observed_at != provenance.source_anchor_event_cut
                or stable_snapshot_anchor_id(anchor) != provenance.source_anchor_id
            ):
                raise ValueError(
                    "execution zero anchor must match typed source provenance"
                )
            if (
                anchor.environment,
                anchor.account_label,
                anchor.symbol,
                anchor.position_side,
            ) != (
                key.environment,
                key.account_label,
                key.symbol,
                key.position_side.value,
            ):
                raise ValueError("execution zero anchor position scope mismatch")
        if self.sequence is not None and self.sequence < 0:
            raise ValueError("execution evidence sequence must be non-negative")
        if (self.stream_id is None) != (self.stream_epoch is None):
            raise ValueError("stream_id and stream_epoch must be supplied together")
        if self.fill_load_provenance is not None:
            if self.stream_id is None or self.stream_epoch is None:
                raise ValueError("fill-load provenance requires a scoped event")
            expected_scope = AccountFactStreamScope.for_position_key(
                self.scope.to_position_key(),
                stream_id=self.stream_id,
                stream_epoch=self.stream_epoch,
            )
            if self.fill_load_provenance.stream_scope != expected_scope:
                raise ValueError("fill-load provenance does not match the event scope")
            if (
                self.coverage_evidence is not None
                and self.coverage_evidence.load_provenance != self.fill_load_provenance
            ):
                raise ValueError("coverage and fill-load provenance disagree")
        if self.stream_checkpoint_adoption is not None:
            adoption = self.stream_checkpoint_adoption
            if self.stream_id is None or self.stream_epoch is None:
                raise ValueError("stream checkpoint adoption requires stream identity")
            expected_scope = AccountFactStreamScope.for_position_key(
                self.scope.to_position_key(),
                stream_id=self.stream_id,
                stream_epoch=self.stream_epoch,
            )
            if adoption.target_scope != expected_scope:
                raise ValueError(
                    "stream checkpoint adoption target does not match evidence"
                )
            if self.fill_load_provenance != adoption.fill_load_provenance:
                raise ValueError(
                    "adoption provenance does not match execution evidence"
                )
            if (
                self.coverage_evidence is None
                or self.coverage_evidence.load_provenance
                != adoption.fill_load_provenance
                or self.coverage_evidence.checkpoint_event_cut
                != adoption.target_event_cut
            ):
                raise ValueError(
                    "adoption requires matching complete coverage evidence"
                )
        if self.fill is not None and self.fills:
            raise ValueError("supply either fill or fills, not both")
        trade_ids = [fill.trade_id for fill in self.fills]
        if len(trade_ids) != len(set(trade_ids)):
            raise ValueError("one account event cannot repeat a trade id")


@dataclass(frozen=True, slots=True)
class ExecutionCumulativeOrderReport:
    """Cumulative exchange order quantities used for settlement only.

    This is not an account trade fact and must never be appended to the
    position ledger. Only exchange trade identities alter projected holdings.
    """

    order_id: str
    cumulative_quantity: Decimal
    cumulative_quote: Decimal
    observed_at: datetime

    def __post_init__(self) -> None:
        if not self.order_id.strip():
            raise ValueError("cumulative order id must not be empty")
        if self.observed_at.tzinfo is None:
            raise ValueError("cumulative order observed_at must be timezone-aware")
        if (
            not self.cumulative_quantity.is_finite()
            or not self.cumulative_quote.is_finite()
            or self.cumulative_quantity < 0
            or self.cumulative_quote < 0
            or (self.cumulative_quantity == 0 and self.cumulative_quote != 0)
            or (self.cumulative_quantity > 0 and self.cumulative_quote <= 0)
        ):
            raise ValueError("cumulative order report quantities are invalid")
