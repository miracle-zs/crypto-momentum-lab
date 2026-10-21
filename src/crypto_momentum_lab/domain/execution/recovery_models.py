"""Versioned domain values used to persist and restore position facts."""

from __future__ import annotations

from dataclasses import dataclass

from crypto_momentum_lab.domain.account.models import (
    AccountFillEvent,
    AccountFillReconciliationCursor,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactConflict,
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    ExitOrderSubmissionFact,
    FactCoverageInterval,
    PositionKey,
)


@dataclass(frozen=True, slots=True)
class AccountFacts:
    """Normalized immutable account facts for a given position key."""

    position_key: PositionKey
    fills: tuple[AccountFillEvent, ...] = ()
    snapshots: tuple[AccountPositionSnapshot, ...] = ()
    exit_boundaries: tuple[ExitOrderSubmissionFact, ...] = ()
    coverage: FactCoverageInterval | None = None
    has_synthetic_fills: bool = False
    conflicting_fills: tuple[AccountFillEvent, ...] = ()
    has_late_events: bool = False
    stream_scope: AccountFactStreamScope | None = None
    fact_conflicts: tuple[AccountFactConflict, ...] = ()
    integrity_issues: tuple[str, ...] = ()
    late_fills: tuple[AccountFillEvent, ...] = ()
    fill_cursor_provenance: AccountFillReconciliationCursor | None = None
    fill_load_provenance: AccountFillLoadProvenance | None = None
    prefix_facts_complete: bool = True

    def __post_init__(self) -> None:
        if type(self.prefix_facts_complete) is not bool:
            raise ValueError("prefix_facts_complete must be a boolean")
        if (
            self.fill_load_provenance is None
            and self.coverage is not None
            and self.coverage.load_provenance is not None
        ):
            object.__setattr__(
                self, "fill_load_provenance", self.coverage.load_provenance
            )
        if self.fill_load_provenance is not None and (
            self.fill_load_provenance.stream_scope != self.stream_scope
        ):
            raise ValueError("fill load provenance scope does not match account facts")
        if self.coverage is not None:
            if (
                self.stream_scope is not None
                and self.coverage.stream_scope != self.stream_scope
            ):
                raise ValueError("coverage scope does not match account facts")


class RecoverySchemaError(ValueError):
    """Stored recovery data uses an unsupported or invalid schema."""
