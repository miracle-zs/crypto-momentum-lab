"""Versioned domain values used to persist and restore position facts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from crypto_momentum_lab.domain.account.models import AccountFillReconciliationCursor
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactConflict,
    AccountFacts,
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    FactCoverageInterval,
    PositionKey,
    PositionLedgerProjection,
)
from crypto_momentum_lab.domain.execution.projection_codec import (
    compute_projection_digest,
)

POSITION_RECOVERY_CHECKPOINT_SCHEMA_VERSION = 3


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
            if (
                self.projection.active_batches
                != self.projection.active_episode.active_batches
            ):
                raise ValueError(
                    "checkpoint active batches disagree with active episode"
                )
        elif self.projection.active_batches:
            raise ValueError("checkpoint batches exist without an active episode")
        if any(episode.is_active for episode in self.projection.archived_episodes):
            raise ValueError("checkpoint archived episode is marked active")
        if any(
            episode.position_key.canonical_id != self.key.canonical_id
            for episode in self.projection.archived_episodes
        ):
            raise ValueError("checkpoint archived episode position key mismatch")
        expected_quantity = sum(
            (batch.quantity for batch in self.projection.active_batches),
            start=Decimal("0"),
        )
        if expected_quantity != self.projection.total_active_quantity:
            raise ValueError("checkpoint total quantity disagrees with active batches")
        if self.projection.total_active_quantity < 0:
            raise ValueError("checkpoint total active quantity must be non-negative")
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


@dataclass(frozen=True, slots=True)
class StreamCheckpointAdoption:
    """Verified prior-epoch checkpoint plus complete suffix into a new stream."""

    parent_checkpoint: PositionRecoveryCheckpoint
    target_scope: AccountFactStreamScope
    target_event_cut: datetime
    fill_load_provenance: AccountFillLoadProvenance

    def __post_init__(self) -> None:
        parent = self.parent_checkpoint
        provenance = self.fill_load_provenance
        if not self.target_scope.matches(parent.key):
            raise ValueError("adoption target scope has a different position key")
        if self.target_scope == parent.stream_scope:
            raise ValueError("stream checkpoint adoption requires a new stream scope")
        if self.target_event_cut.tzinfo is None or (
            self.target_event_cut.utcoffset() is None
        ):
            raise ValueError("adoption target event cut must be timezone-aware")
        if self.target_event_cut <= parent.event_cut:
            raise ValueError("adoption target cut must follow the parent checkpoint")
        if provenance.stream_scope != self.target_scope:
            raise ValueError("adoption fill provenance scope does not match target")
        if provenance.source_anchor_kind != "recovery_checkpoint":
            raise ValueError("adoption requires a recovery-checkpoint source anchor")
        if (
            provenance.source_anchor_id != parent.checkpoint_id
            or provenance.source_anchor_event_cut != parent.event_cut
        ):
            raise ValueError("adoption fill provenance does not bind the parent")
        if provenance.scan_origin_start_time_ms is None:
            raise ValueError("adoption requires a time-origin suffix scan")
        origin = provenance.origin_start_at
        if origin is None or origin > parent.event_cut:
            raise ValueError("adoption suffix scan starts after the parent cut")
        if provenance.request_from_id is not None:
            raise ValueError("adoption suffix scan must start without a cursor")
        if (
            not provenance.is_complete
            or provenance.checked_through != self.target_event_cut
            or provenance.observed_at != self.target_event_cut
        ):
            raise ValueError("adoption suffix scan is not complete through target cut")
        if (
            parent.has_conflicts
            or parent.has_synthetic_fills
            or parent.has_late_events
            or parent.integrity_issues
        ):
            raise ValueError("parent checkpoint contains unresolved facts")
        if (
            not parent.projection.is_comparable
            or parent.projection.health_status.value != "READY"
            or parent.projection.reconciliation_gap != Decimal("0")
            or parent.projection.unallocated_quantity != Decimal("0")
        ):
            raise ValueError("parent checkpoint projection is not fully reconciled")
        if (
            parent.coverage is None
            or parent.coverage.stream_scope != parent.stream_scope
            or not parent.coverage.is_authoritative
            or not parent.coverage.covers(parent.event_cut)
        ):
            raise ValueError("parent checkpoint has no verified source coverage")


@dataclass(frozen=True, slots=True)
class DurableJournalCut:
    """Facts and checkpoint durably read for one exact point-in-time cut."""

    scope: AccountFactStreamScope
    as_of: datetime
    facts: AccountFacts
    checkpoint: PositionRecoveryCheckpoint | None = None
    revision: int = 0
    conflicts: tuple[AccountFactConflict, ...] = ()
    integrity_issues: tuple[str, ...] = ()
    cursor_provenance: AccountFillReconciliationCursor | None = None

    def __post_init__(self) -> None:
        if self.as_of.tzinfo is None or self.as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        if not self.scope.matches(self.facts.position_key):
            raise ValueError("journal cut scope does not match fact position key")
        if self.facts.stream_scope != self.scope:
            raise ValueError("journal cut facts do not preserve requested stream scope")
        if any(fill.trade_at > self.as_of for fill in self.facts.fills):
            raise ValueError("journal cut contains a fill after as_of")
        if any(snapshot.observed_at > self.as_of for snapshot in self.facts.snapshots):
            raise ValueError("journal cut contains a snapshot after as_of")
        if any(
            boundary.submitted_at > self.as_of
            for boundary in self.facts.exit_boundaries
        ):
            raise ValueError("journal cut contains a boundary after as_of")
        if self.facts.coverage is not None:
            coverage = self.facts.coverage
            if coverage.end_at > self.as_of:
                raise ValueError("journal cut coverage extends beyond as_of")
            if (
                coverage.evidence_observed_at is not None
                and coverage.evidence_observed_at > self.as_of
            ):
                raise ValueError("journal cut coverage evidence is after as_of")
            if (
                coverage.checkpoint_event_cut is not None
                and coverage.checkpoint_event_cut > self.as_of
            ):
                raise ValueError("journal cut coverage checkpoint is after as_of")
            if coverage.stream_scope != self.scope:
                raise ValueError("journal cut coverage scope does not match scope")
        if (
            self.facts.fill_load_provenance is not None
            and self.facts.fill_load_provenance.observed_at > self.as_of
        ):
            raise ValueError("journal cut fill-load provenance is after as_of")
        if self.checkpoint is not None and self.checkpoint.event_cut > self.as_of:
            raise ValueError("journal cut contains a checkpoint after as_of")
        if self.checkpoint is not None:
            if self.checkpoint.stream_scope != self.scope:
                raise ValueError(
                    "journal checkpoint scope does not match requested scope"
                )
            if self.facts.recovery_checkpoint != self.checkpoint:
                raise ValueError("journal cut checkpoint fields disagree")
        elif self.facts.recovery_checkpoint is not None:
            raise ValueError("journal facts contain an unbounded recovery checkpoint")
        if type(self.revision) is not int or self.revision < 0:
            raise ValueError("revision must be non-negative")
        if self.cursor_provenance is not None:
            cursor = self.cursor_provenance
            if (
                cursor.environment != self.scope.environment
                or cursor.account_label != self.scope.account_label
                or cursor.symbol != self.scope.symbol
            ):
                raise ValueError("fill cursor provenance does not match journal scope")


@dataclass(frozen=True, slots=True)
class JournalPersistResult:
    """Summary of facts accepted by the durable journal transaction."""

    inserted_count: int
    duplicate_count: int
    conflict_count: int
    revision: int

    @property
    def has_conflicts(self) -> bool:
        return self.conflict_count > 0


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
