"""Versioned domain values used to persist and restore position facts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

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
    PositionLedgerProjection,
)
from crypto_momentum_lab.domain.execution.projection_codec import (
    compute_projection_digest,
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
    recovery_checkpoint: PositionRecoveryCheckpoint | None = None
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

POSITION_RECOVERY_CHECKPOINT_SCHEMA_VERSION = 4


class RecoverySchemaError(ValueError):
    """Stored recovery data uses an unsupported or invalid schema."""


@dataclass(frozen=True, slots=True)
class PositionRecoveryCheckpoint:
    """Complete projected state at an inclusive event-time cut.

    The projection carries the active episode, exact remaining batches,
    reductions, and already archived episodes. ``facts_hash`` binds the
    checkpoint to all immutable inputs up to ``event_cut``. Coverage is copied
    exactly as proven at creation; checkpoint existence does not make coverage
    complete.
    """

    key: PositionKey
    stream_scope: AccountFactStreamScope
    event_cut: datetime
    projection: PositionLedgerProjection
    facts_hash: str
    source_revision: int
    coverage: FactCoverageInterval | None = None
    checkpoint_id: str = ""
    schema_version: int = POSITION_RECOVERY_CHECKPOINT_SCHEMA_VERSION
    has_conflicts: bool = False
    has_synthetic_fills: bool = False
    has_late_events: bool = False
    integrity_issues: tuple[str, ...] = ()
    projection_digest: str = ""
    parent_checkpoint_id: str | None = None
    parent_facts_hash: str | None = None
    parent_projection_digest: str | None = None
    parent_event_cut: datetime | None = None
    suffix_facts_hash: str | None = None
    parent_stream_scope: AccountFactStreamScope | None = None

    def __post_init__(self) -> None:
        if not self.checkpoint_id:
            object.__setattr__(self, "checkpoint_id", f"prc_{uuid4().hex}")
        if type(self.schema_version) is not int:
            raise RecoverySchemaError("checkpoint schema version must be an integer")
        if self.schema_version != POSITION_RECOVERY_CHECKPOINT_SCHEMA_VERSION:
            raise RecoverySchemaError(
                f"unsupported position recovery checkpoint schema {self.schema_version}"
            )
        if self.event_cut.tzinfo is None:
            raise ValueError("event_cut must be timezone-aware")
        if self.stream_scope is None or not self.stream_scope.matches(self.key):
            raise ValueError("checkpoint stream scope does not match position key")
        if self.projection.stream_scope != self.stream_scope:
            raise ValueError(
                "checkpoint projection scope does not match checkpoint scope"
            )
        if self.projection.position_key.canonical_id != self.key.canonical_id:
            raise ValueError("checkpoint projection key does not match position key")
        if (
            self.projection.event_cut is not None
            and self.projection.event_cut > self.event_cut
        ):
            raise ValueError("checkpoint projection event cut is after checkpoint cut")
        if (
            self.projection.high_watermark_trade_at is not None
            and self.projection.high_watermark_trade_at > self.event_cut
        ):
            raise ValueError("checkpoint projection contains a fill after its cut")
        _validate_projection_cut(self.projection, self.event_cut)
        if self.projection.active_episode is not None:
            if not self.projection.active_episode.is_active:
                raise ValueError("checkpoint active episode is marked inactive")
            if (
                self.projection.active_episode.position_key.canonical_id
                != self.key.canonical_id
            ):
                raise ValueError("checkpoint active episode position key mismatch")
        if any(episode.is_active for episode in self.projection.archived_episodes):
            raise ValueError("checkpoint archived episode is marked active")
        if any(
            episode.position_key.canonical_id != self.key.canonical_id
            for episode in self.projection.archived_episodes
        ):
            raise ValueError("checkpoint archived episode position key mismatch")
        if self.projection.unallocated_quantity < 0:
            raise ValueError("checkpoint unallocated quantity must be non-negative")
        batch_ids = [
            batch.batch_id
            for episode in (
                *self.projection.archived_episodes,
                *(
                    (self.projection.active_episode,)
                    if self.projection.active_episode
                    else ()
                ),
            )
            for batch in episode.batches
        ]
        if len(batch_ids) != len(set(batch_ids)):
            raise ValueError("checkpoint contains duplicate batch identities")
        if self.coverage is not None and self.coverage.end_at > self.event_cut:
            raise ValueError("checkpoint coverage extends beyond checkpoint cut")
        if (
            self.coverage is not None
            and self.coverage.evidence_observed_at is not None
            and self.coverage.evidence_observed_at > self.event_cut
        ):
            raise ValueError("checkpoint coverage evidence is after checkpoint cut")
        if (
            self.coverage is not None
            and self.coverage.checkpoint_event_cut is not None
            and self.coverage.checkpoint_event_cut > self.event_cut
        ):
            raise ValueError("coverage source checkpoint is after recovery cut")
        if (
            self.coverage is not None
            and self.coverage.load_provenance is not None
            and self.coverage.load_provenance.observed_at > self.event_cut
        ):
            raise ValueError("fill-load provenance is after recovery cut")
        if (
            self.coverage is not None
            and self.coverage.stream_scope != self.stream_scope
        ):
            raise ValueError(
                "checkpoint coverage scope does not match checkpoint scope"
            )
        if type(self.source_revision) is not int or self.source_revision < 0:
            raise ValueError("source_revision must be non-negative")
        if any(
            type(value) is not bool
            for value in (
                self.has_conflicts,
                self.has_synthetic_fills,
                self.has_late_events,
            )
        ):
            raise ValueError("checkpoint status flags must be booleans")
        if len(self.facts_hash) != 64 or any(
            char not in "0123456789abcdef" for char in self.facts_hash.lower()
        ):
            raise ValueError("facts_hash must be a 64-character SHA-256 digest")
        if not self.checkpoint_id.strip():
            raise ValueError("checkpoint_id must not be empty")
        if any(
            not isinstance(issue, str) or not issue.strip()
            for issue in self.integrity_issues
        ):
            raise ValueError("checkpoint integrity issues must be non-empty strings")
        expected_projection_digest = compute_projection_digest(self.projection)
        if not self.projection_digest:
            object.__setattr__(self, "projection_digest", expected_projection_digest)
        elif self.projection_digest != expected_projection_digest:
            raise RecoverySchemaError("checkpoint projection digest mismatch")
        parent_fields = (
            self.parent_checkpoint_id,
            self.parent_facts_hash,
            self.parent_projection_digest,
            self.parent_event_cut,
            self.suffix_facts_hash,
        )
        if any(value is not None for value in parent_fields):
            if (
                self.parent_checkpoint_id is None
                or self.parent_facts_hash is None
                or self.parent_projection_digest is None
                or self.parent_event_cut is None
                or self.suffix_facts_hash is None
            ):
                raise RecoverySchemaError("checkpoint parent chain is incomplete")
            if not self.parent_checkpoint_id or not self.parent_checkpoint_id.strip():
                raise RecoverySchemaError("parent checkpoint id must not be empty")
            if (
                self.parent_event_cut is None
                or self.parent_event_cut.tzinfo is None
                or self.parent_event_cut.utcoffset() is None
            ):
                raise RecoverySchemaError(
                    "parent checkpoint cut must be timezone-aware"
                )
            if self.parent_event_cut >= self.event_cut:
                raise RecoverySchemaError(
                    "parent checkpoint cut must precede new checkpoint cut"
                )
            if self.parent_stream_scope is None:
                raise RecoverySchemaError("checkpoint parent stream scope is missing")
            parent_scope = self.parent_stream_scope
            if not parent_scope.matches(self.key):
                raise RecoverySchemaError(
                    "parent checkpoint stream scope has a different position key"
                )
            for name, digest in (
                ("parent_facts_hash", self.parent_facts_hash),
                ("parent_projection_digest", self.parent_projection_digest),
                ("suffix_facts_hash", self.suffix_facts_hash),
            ):
                if len(digest) != 64 or any(
                    character not in "0123456789abcdef" for character in digest.lower()
                ):
                    raise RecoverySchemaError(f"{name} must be a SHA-256 digest")
            expected_chain_hash = compute_checkpoint_chain_hash(
                scope=self.stream_scope,
                parent_stream_scope=parent_scope,
                event_cut=self.event_cut,
                parent_checkpoint_id=self.parent_checkpoint_id,
                parent_facts_hash=self.parent_facts_hash,
                parent_projection_digest=self.parent_projection_digest,
                parent_event_cut=self.parent_event_cut,
                suffix_facts_hash=self.suffix_facts_hash,
                schema_version=self.schema_version,
            )
            if self.facts_hash != expected_chain_hash:
                raise RecoverySchemaError("checkpoint parent chain hash mismatch")
        elif self.parent_stream_scope is not None:
            raise RecoverySchemaError(
                "checkpoint has a parent scope without parent checkpoint fields"
            )


def compute_checkpoint_chain_hash(
    *,
    scope: AccountFactStreamScope,
    parent_stream_scope: AccountFactStreamScope,
    event_cut: datetime,
    parent_checkpoint_id: str,
    parent_facts_hash: str,
    parent_projection_digest: str,
    parent_event_cut: datetime,
    suffix_facts_hash: str,
    schema_version: int = POSITION_RECOVERY_CHECKPOINT_SCHEMA_VERSION,
) -> str:
    """Bind a rolling checkpoint to its validated parent and suffix facts."""
    material = [schema_version, scope.canonical_id]
    if schema_version >= 3:
        material.append(parent_stream_scope.canonical_id)
    material.extend(
        (
            event_cut.isoformat(),
            parent_checkpoint_id,
            parent_facts_hash,
            parent_projection_digest,
            parent_event_cut.isoformat(),
            suffix_facts_hash,
        )
    )
    encoded = json.dumps(material, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _validate_projection_cut(
    projection: PositionLedgerProjection,
    event_cut: datetime,
) -> None:
    episodes = list(projection.archived_episodes)
    if projection.active_episode is not None:
        episodes.append(projection.active_episode)
    for episode in episodes:
        if episode.opened_at > event_cut:
            raise ValueError("checkpoint episode opened after checkpoint cut")
        if episode.closed_at is not None and episode.closed_at > event_cut:
            raise ValueError("checkpoint episode closed after checkpoint cut")
        for batch in episode.batches:
            if batch.opened_at > event_cut:
                raise ValueError("checkpoint batch opened after checkpoint cut")
            if (
                batch.exit_order_submitted_at is not None
                and batch.exit_order_submitted_at > event_cut
            ):
                raise ValueError("checkpoint batch boundary is after checkpoint cut")
        for reduction in episode.reductions:
            if reduction.reduced_at > event_cut:
                raise ValueError("checkpoint reduction is after checkpoint cut")
    discrepancy = projection.discrepancy
    if discrepancy is not None:
        for occurred_at in (
            discrepancy.first_seen_at,
            discrepancy.last_seen_at,
            discrepancy.event_cut,
            discrepancy.snapshot_at,
        ):
            if occurred_at is not None and occurred_at > event_cut:
                raise ValueError("checkpoint discrepancy is after checkpoint cut")
