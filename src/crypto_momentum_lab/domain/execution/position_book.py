"""Current position state and its immutable read view."""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum

from crypto_momentum_lab.domain.account.models import (
    AccountFillEvent,
    AccountFillReconciliationCursor,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactConflict,
    AccountFillLoadProvenance,
    ExitOrderSubmissionFact,
)
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    FactCoverageInterval,
    FactCoverageStatus,
    FreshnessRequirement,
    PositionHealthStatus,
    PositionKey,
    PositionLedgerProjection,
    PositionView,
)
from crypto_momentum_lab.domain.execution.projection_codec import (
    encode_projection,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.recovery_models import (
    AccountFacts,
    PositionRecoveryCheckpoint,
)


@dataclass(frozen=True, slots=True)
class _CurrentFacts:
    """Small verification state kept beside the current position projection."""

    coverage: FactCoverageInterval | None = None
    latest_snapshot: AccountPositionSnapshot | None = None
    source_anchor_snapshot: AccountPositionSnapshot | None = None
    fill_cursor_provenance: AccountFillReconciliationCursor | None = None
    fill_load_provenance: AccountFillLoadProvenance | None = None
    recovery_checkpoint: PositionRecoveryCheckpoint | None = None
    exit_boundaries: tuple[ExitOrderSubmissionFact, ...] = ()
    conflicting_fill: AccountFillEvent | None = None
    fact_conflict: AccountFactConflict | None = None
    has_synthetic_fills: bool = False
    has_late_events: bool = False
    integrity_issue: str | None = None


class PositionBook:
    """Own one current projection; event history belongs to durable identities."""

    def __init__(
        self,
        position_key: PositionKey,
        *,
        stream_scope: AccountFactStreamScope | None = None,
        ledger: PositionLedger | None = None,
        policy_version: str = "v1",
        schema_version: str = "v1",
    ) -> None:
        if stream_scope is not None and not stream_scope.matches(position_key):
            raise ValueError("stream scope does not match position key")
        self._position_key = position_key
        self._ledger = ledger or PositionLedger(position_key)
        self._policy_version = policy_version
        self._schema_version = schema_version
        self._stream_scope = stream_scope
        self._facts = _CurrentFacts()
        self._revision = 0
        self._projection = self._ledger.project(
            AccountFacts(
                position_key=position_key,
                stream_scope=stream_scope,
                prefix_facts_complete=True,
            )
        )
        self._durable_projection_version: str | None = None
        self._durable_projection_event_cut: datetime | None = None
        self._durable_execution_state_version: str | None = None
        self._zero_confirmed_override = False

    @classmethod
    def from_facts(
        cls,
        facts: AccountFacts,
        *,
        ledger: PositionLedger | None = None,
        policy_version: str = "v1",
        schema_version: str = "v1",
    ) -> PositionBook:
        book = cls(
            facts.position_key,
            stream_scope=facts.stream_scope,
            ledger=ledger,
            policy_version=policy_version,
            schema_version=schema_version,
        )
        book.replace_from_facts(facts)
        return book

    @property
    def position_key(self) -> PositionKey:
        return self._position_key

    @property
    def stream_scope(self) -> AccountFactStreamScope | None:
        return self._stream_scope

    @property
    def projection(self) -> PositionLedgerProjection:
        return self._projection

    @property
    def latest_trade_at(self) -> datetime | None:
        return self._projection.high_watermark_trade_at

    def adopt_stream_scope(self, scope: AccountFactStreamScope) -> None:
        if not scope.matches(self._position_key):
            raise ValueError("stream scope does not match position key")
        self._stream_scope = scope
        self._projection = replace(self._projection, stream_scope=scope)
        self._revision += 1
        self._view_version_changed()

    def remove_exit_boundaries(self, client_order_id: str) -> None:
        boundaries = tuple(
            item
            for item in self._facts.exit_boundaries
            if item.client_order_id != client_order_id
        )
        if boundaries == self._facts.exit_boundaries:
            return
        self._facts = replace(self._facts, exit_boundaries=boundaries)
        self._revision += 1
        self._view_version_changed()

    def encode_current_state(self) -> dict[str, object]:
        facts = self.current_facts
        if facts.fills or facts.late_fills:
            raise RuntimeError("current position state must not contain fill history")
        return {
            "projection": encode_projection(self._projection, include_derived=False),
            "episode_sequence": self._projection.episode_sequence,
            "batch_sequence": self._projection.batch_sequence,
            "verification": PositionRecoveryCodec.encode_facts(facts),
        }

    @classmethod
    def from_current_state(
        cls,
        *,
        position_key: PositionKey,
        stream_scope: AccountFactStreamScope,
        projection: PositionLedgerProjection,
        verification: AccountFacts,
        episode_sequence: int,
        batch_sequence: int,
        policy_version: str = "v1",
        schema_version: str = "v1",
    ) -> PositionBook:
        if projection.position_key != position_key:
            raise ValueError("projection position key does not match book")
        if projection.stream_scope != stream_scope:
            raise ValueError("projection stream scope does not match book")
        if verification.position_key != position_key:
            raise ValueError("verification position key does not match book")
        if verification.stream_scope != stream_scope:
            raise ValueError("verification stream scope does not match book")
        if verification.fills or verification.late_fills:
            raise ValueError("current position state must not contain fill history")
        if type(episode_sequence) is not int or episode_sequence < 0:
            raise ValueError("episode sequence must be a non-negative integer")
        if type(batch_sequence) is not int or batch_sequence < 0:
            raise ValueError("batch sequence must be a non-negative integer")
        book = cls(
            position_key,
            stream_scope=stream_scope,
            policy_version=policy_version,
            schema_version=schema_version,
        )
        book._projection = _current_projection(
            replace(
                projection,
                episode_sequence=episode_sequence,
                batch_sequence=batch_sequence,
            )
        )
        book._facts = _merge_current_facts(_CurrentFacts(), verification)
        book._revision += 1
        return book

    @property
    def current_facts(self) -> AccountFacts:
        """Return the bounded metadata needed to verify the current projection."""
        snapshots = _current_snapshots(self._facts)
        return AccountFacts(
            position_key=self._position_key,
            snapshots=snapshots,
            exit_boundaries=self._facts.exit_boundaries,
            coverage=self._facts.coverage,
            has_synthetic_fills=self._facts.has_synthetic_fills,
            conflicting_fills=(
                (self._facts.conflicting_fill,)
                if self._facts.conflicting_fill is not None
                else ()
            ),
            has_late_events=self._facts.has_late_events,
            stream_scope=self._stream_scope,
            recovery_checkpoint=self._facts.recovery_checkpoint,
            fact_conflicts=(
                (self._facts.fact_conflict,)
                if self._facts.fact_conflict is not None
                else ()
            ),
            integrity_issues=(
                (self._facts.integrity_issue,)
                if self._facts.integrity_issue is not None
                else ()
            ),
            fill_load_provenance=self._facts.fill_load_provenance,
            fill_cursor_provenance=self._facts.fill_cursor_provenance,
            prefix_facts_complete=False,
        )

    def reset_stream(self, scope: AccountFactStreamScope) -> None:
        if not scope.matches(self._position_key):
            raise ValueError("stream scope does not match position key")
        self._stream_scope = scope
        self._facts = _CurrentFacts()
        self._projection = self._ledger.project(
            AccountFacts(
                position_key=self._position_key,
                stream_scope=scope,
                prefix_facts_complete=True,
            )
        )
        self._revision += 1
        self._zero_confirmed_override = False
        self._view_version_changed()

    def replace_from_facts(self, facts: AccountFacts) -> None:
        self._validate_facts(facts)
        self._stream_scope = facts.stream_scope
        self._projection = _current_projection(self._ledger.project(facts))
        self._facts = _merge_current_facts(_CurrentFacts(), facts)
        self._revision += 1
        self._zero_confirmed_override = False
        self._view_version_changed()

    def apply_delta(self, facts: AccountFacts) -> None:
        self._validate_facts(facts)
        if self._stream_scope is not None and facts.stream_scope != self._stream_scope:
            raise ValueError("fact delta stream scope does not match current state")
        self._stream_scope = facts.stream_scope or self._stream_scope
        high_watermark = self._projection.high_watermark_trade_at
        checkpoint = self._facts.recovery_checkpoint
        late_fill_arrived = any(
            (high_watermark is not None and fill.trade_at < high_watermark)
            or (checkpoint is not None and fill.trade_at <= checkpoint.event_cut)
            for fill in facts.fills
        )
        facts = replace(
            facts,
            has_synthetic_fills=(
                facts.has_synthetic_fills
                or any(
                    bool(fill.raw_payload.get("synthetic_from_order", False))
                    for fill in facts.fills
                )
            ),
            has_late_events=facts.has_late_events or late_fill_arrived,
        )
        current_facts = _merge_current_facts(self._facts, facts)
        delta = AccountFacts(
            position_key=self._position_key,
            fills=facts.fills,
            snapshots=_current_snapshots(current_facts),
            exit_boundaries=current_facts.exit_boundaries,
            coverage=current_facts.coverage,
            has_synthetic_fills=current_facts.has_synthetic_fills,
            conflicting_fills=(
                (current_facts.conflicting_fill,)
                if current_facts.conflicting_fill is not None
                else ()
            ),
            has_late_events=current_facts.has_late_events,
            stream_scope=self._stream_scope,
            fact_conflicts=(
                (current_facts.fact_conflict,)
                if current_facts.fact_conflict is not None
                else ()
            ),
            integrity_issues=(
                (current_facts.integrity_issue,)
                if current_facts.integrity_issue is not None
                else ()
            ),
            late_fills=facts.late_fills,
            fill_cursor_provenance=(
                facts.fill_cursor_provenance or current_facts.fill_cursor_provenance
            ),
            fill_load_provenance=current_facts.fill_load_provenance,
            prefix_facts_complete=False,
        )
        self._projection = _current_projection(
            self._ledger.apply_delta(self._projection, delta)
        )
        self._facts = current_facts
        self._revision += 1
        self._zero_confirmed_override = False
        self._view_version_changed()

    def use_durable_projection_version(
        self,
        token: str | None,
        *,
        event_cut: datetime | None = None,
    ) -> None:
        self._view_version_changed()
        if token is None:
            self._durable_projection_version = None
            self._durable_projection_event_cut = None
            self._durable_execution_state_version = None
            return
        if not token.strip():
            raise ValueError("durable projection version must not be empty")
        if event_cut is not None and (
            event_cut.tzinfo is None or event_cut.utcoffset() is None
        ):
            raise ValueError("durable projection event cut must be timezone-aware")
        self._durable_execution_state_version = self._execution_version()
        self._durable_projection_version = token
        self._durable_projection_event_cut = event_cut

    def seed_durable_view(
        self,
        *,
        projection: PositionLedgerProjection,
        view_coverage: FactCoverageInterval | None = None,
        zero_confirmed: bool = False,
        stream_scope: AccountFactStreamScope | None = None,
        projection_version: str | None = None,
        event_cut: datetime | None = None,
    ) -> None:
        if projection.position_key != self._position_key:
            raise ValueError("durable projection position key does not match book")
        self._projection = _current_projection(projection)
        self._stream_scope = stream_scope or projection.stream_scope
        self._facts = _CurrentFacts(coverage=view_coverage)
        self._revision += 1
        self._zero_confirmed_override = zero_confirmed
        version_id = projection_version or projection.projection_version or "v1"
        self._durable_projection_version = version_id
        self._durable_projection_event_cut = event_cut or projection.event_cut
        self._durable_execution_state_version = self._execution_version()

    def copy_for_transaction(self) -> PositionBook:
        return copy.copy(self)

    def get_view(
        self,
        cut: datetime | None = None,
        requirement: FreshnessRequirement | None = None,
        now: datetime | None = None,
    ) -> PositionView:
        projection = self._projection
        if (
            cut is not None
            and projection.event_cut is not None
            and cut < projection.event_cut
        ):
            raise ValueError("historical position cuts are not held in memory")

        facts = self.current_facts
        coverage_is_verified = _coverage_anchor_is_verified(facts)
        view_coverage = facts.coverage
        health_status = projection.health_status
        is_comparable = projection.is_comparable
        diagnostics = list(projection.diagnostics)
        if (
            facts.stream_scope is not None
            and facts.stream_scope.environment == "live"
            and facts.coverage is not None
            and facts.coverage.status == FactCoverageStatus.CONFIRMED
            and not coverage_is_verified
        ):
            view_coverage = replace(facts.coverage, status=FactCoverageStatus.PENDING)
            health_status = PositionHealthStatus.INCOMPLETE
            is_comparable = False
            diagnostics.append(
                "Coverage source anchor is missing or does not match this position cut"
            )

        zero_confirmed = self._zero_confirmed_override or _zero_is_confirmed(
            self._position_key,
            projection,
            facts,
        )
        version = self._execution_version(
            health_status=health_status,
            is_comparable=is_comparable,
            coverage=view_coverage,
            zero_confirmed=zero_confirmed,
        )
        durable_cut_is_current = cut is None or (
            self._durable_projection_event_cut is not None
            and cut >= self._durable_projection_event_cut
        )
        version_id = version
        if (
            self._durable_projection_version is not None
            and durable_cut_is_current
            and version == self._durable_execution_state_version
        ):
            version_id = self._durable_projection_version

        now_dt = now or datetime.now(UTC)
        if requirement is not None:
            if requirement.min_event_cut is not None and (
                projection.event_cut is None
                or projection.event_cut < requirement.min_event_cut
            ):
                health_status = PositionHealthStatus.CATCHING_UP
                diagnostics.append(
                    f"Event cut ({projection.event_cut}) is behind minimum required "
                    f"cut ({requirement.min_event_cut})"
                )
            if projection.event_cut is not None:
                staleness = now_dt - projection.event_cut
                if (
                    staleness > requirement.max_staleness
                    and health_status == PositionHealthStatus.READY
                ):
                    health_status = PositionHealthStatus.CATCHING_UP
                    diagnostics.append(
                        f"Projection staleness ({staleness.total_seconds():.1f}s) "
                        f"exceeds max allowed ({requirement.max_staleness.total_seconds():.1f}s)"
                    )
            if requirement.require_comparable and not is_comparable:
                if health_status == PositionHealthStatus.READY:
                    health_status = PositionHealthStatus.CATCHING_UP

        return PositionView(
            key=self._position_key,
            projection_version=version_id,
            input_revision=self._revision,
            event_cut=projection.event_cut,
            policy_version=self._policy_version,
            schema_version=self._schema_version,
            coverage=view_coverage,
            active_episode=projection.active_episode,
            batches=projection.active_batches,
            unallocated_quantity=projection.unallocated_quantity,
            reconciliation_status=(
                "OK" if health_status == PositionHealthStatus.READY else health_status.value
            ),
            reconciliation_gap=(
                projection.reconciliation_gap if is_comparable else None
            ),
            health_status=health_status,
            diagnostics=tuple(diagnostics),
            discrepancy=projection.discrepancy,
            is_comparable=is_comparable,
            zero_position_snapshot_confirmed=zero_confirmed,
            stream_scope=self._stream_scope,
        )

    def _validate_facts(self, facts: AccountFacts) -> None:
        if facts.position_key != self._position_key:
            raise ValueError("facts position key does not match book")
        if facts.stream_scope is not None and not facts.stream_scope.matches(
            self._position_key
        ):
            raise ValueError("fact stream scope does not match book")

    def _execution_version(
        self,
        *,
        health_status: PositionHealthStatus | None = None,
        is_comparable: bool | None = None,
        coverage: FactCoverageInterval | None = None,
        zero_confirmed: bool | None = None,
    ) -> str:
        facts = self.current_facts
        view_coverage = facts.coverage if coverage is None else coverage
        status = health_status or self._projection.health_status
        comparable = (
            self._projection.is_comparable
            if is_comparable is None
            else is_comparable
        )
        confirmed_zero = (
            self._zero_confirmed_override
            if zero_confirmed is None
            else zero_confirmed
        )
        return _execution_state_version(
            facts=facts,
            projection=self._projection,
            health_status=status,
            is_comparable=comparable,
            coverage=view_coverage,
            zero_confirmed=confirmed_zero,
            policy_version=self._policy_version,
            schema_version=self._schema_version,
        )

    def _view_version_changed(self) -> None:
        self._durable_projection_version = None
        self._durable_projection_event_cut = None
        self._durable_execution_state_version = None


def _current_projection(
    projection: PositionLedgerProjection,
) -> PositionLedgerProjection:
    """Keep only active batches and the counters needed for the next update."""
    active_episode = projection.active_episode
    if active_episode is not None:
        active_batches = tuple(batch for batch in active_episode.batches if batch.quantity > 0)
        active_episode = replace(
            active_episode,
            batches=active_batches,
            reductions=(),
        )
    else:
        active_batches = ()
    return replace(
        projection,
        active_episode=active_episode,
        archived_episodes=(),
    )


def _merge_current_facts(current: _CurrentFacts, facts: AccountFacts) -> _CurrentFacts:
    latest_snapshot = current.latest_snapshot
    source_anchor = current.source_anchor_snapshot
    if facts.snapshots:
        latest_snapshot = max(facts.snapshots, key=lambda item: item.observed_at)
    provenance = facts.fill_load_provenance or current.fill_load_provenance
    if provenance is not None and facts.snapshots:
        from crypto_momentum_lab.domain.execution.snapshot_encoding import (
            stable_snapshot_anchor_id,
        )

        source_anchor = next(
            (
                snapshot
                for snapshot in facts.snapshots
                if snapshot.observed_at == provenance.source_anchor_event_cut
                and stable_snapshot_anchor_id(snapshot) == provenance.source_anchor_id
            ),
            source_anchor,
        )
    boundaries = {item.order_id: item for item in current.exit_boundaries}
    boundaries.update({item.order_id: item for item in facts.exit_boundaries})
    return _CurrentFacts(
        coverage=facts.coverage or current.coverage,
        latest_snapshot=latest_snapshot,
        source_anchor_snapshot=source_anchor,
        fill_load_provenance=provenance,
        fill_cursor_provenance=(
            facts.fill_cursor_provenance or current.fill_cursor_provenance
        ),
        recovery_checkpoint=facts.recovery_checkpoint or current.recovery_checkpoint,
        exit_boundaries=tuple(boundaries.values()),
        conflicting_fill=(
            facts.conflicting_fills[-1]
            if facts.conflicting_fills
            else current.conflicting_fill
        ),
        fact_conflict=(
            facts.fact_conflicts[-1]
            if facts.fact_conflicts
            else current.fact_conflict
        ),
        has_synthetic_fills=current.has_synthetic_fills or facts.has_synthetic_fills,
        has_late_events=current.has_late_events or facts.has_late_events,
        integrity_issue=(
            facts.integrity_issues[-1]
            if facts.integrity_issues
            else current.integrity_issue
        ),
    )


def _current_snapshots(facts: _CurrentFacts) -> tuple[AccountPositionSnapshot, ...]:
    anchor = facts.source_anchor_snapshot
    latest = facts.latest_snapshot
    if anchor is None:
        return (latest,) if latest is not None else ()
    if latest is None or latest.observed_at == anchor.observed_at:
        return (anchor,)
    return anchor, latest


def _zero_is_confirmed(
    key: PositionKey,
    projection: PositionLedgerProjection,
    facts: AccountFacts,
) -> bool:
    latest = max(facts.snapshots, key=lambda item: item.observed_at, default=None)
    return bool(
        not projection.active_batches
        and latest is not None
        and latest.environment == key.environment
        and latest.account_label == key.account_label
        and latest.symbol == key.symbol
        and latest.position_side == key.position_side.value
        and latest.position_amt == Decimal("0")
        and (projection.event_cut is None or latest.observed_at >= projection.event_cut)
        and (facts.stream_scope is None or facts.stream_scope.matches(key))
    )


def _coverage_anchor_is_verified(facts: AccountFacts) -> bool:
    coverage = facts.coverage
    provenance = facts.fill_load_provenance
    if coverage is None or provenance is None:
        return False
    if (
        coverage.stream_scope != facts.stream_scope
        or provenance.stream_scope != facts.stream_scope
        or coverage.load_provenance != provenance
        or not provenance.is_complete
        or provenance.checked_through is None
    ):
        return False
    if provenance.source_anchor_kind == "zero_snapshot":
        from crypto_momentum_lab.domain.execution.snapshot_encoding import (
            stable_snapshot_anchor_id,
        )

        if any(
            snapshot.environment == facts.position_key.environment
            and snapshot.account_label == facts.position_key.account_label
            and snapshot.symbol == facts.position_key.symbol
            and snapshot.position_side == facts.position_key.position_side.value
            and snapshot.position_amt == Decimal("0")
            and snapshot.observed_at == provenance.source_anchor_event_cut
            and stable_snapshot_anchor_id(snapshot) == provenance.source_anchor_id
            for snapshot in facts.snapshots
        ):
            return True
        checkpoint = facts.recovery_checkpoint
        return bool(
            checkpoint is not None
            and (
                checkpoint.coverage == coverage
                or (
                    checkpoint.coverage is not None
                    and checkpoint.coverage.covers_range(
                        provenance.source_anchor_event_cut,
                        provenance.checked_through,
                    )
                )
            )
            and checkpoint.event_cut >= provenance.source_anchor_event_cut
        )
    if provenance.source_anchor_kind == "recovery_checkpoint":
        checkpoint = facts.recovery_checkpoint
        return bool(
            checkpoint is not None
            and (
                (
                    checkpoint.checkpoint_id == provenance.source_anchor_id
                    and checkpoint.event_cut == provenance.source_anchor_event_cut
                )
                or (
                    checkpoint.parent_checkpoint_id == provenance.source_anchor_id
                    and checkpoint.parent_event_cut == provenance.source_anchor_event_cut
                )
            )
        )
    return False


def _execution_state_version(
    *,
    facts: AccountFacts,
    projection: PositionLedgerProjection,
    health_status: PositionHealthStatus,
    is_comparable: bool,
    coverage: FactCoverageInterval | None,
    zero_confirmed: bool,
    policy_version: str,
    schema_version: str,
) -> str:
    latest = max(facts.snapshots, key=lambda item: item.observed_at, default=None)
    episode = projection.active_episode
    material = {
        "version": 1,
        "position": facts.position_key.canonical_id,
        "stream": asdict(facts.stream_scope) if facts.stream_scope else None,
        "policy": policy_version,
        "schema": schema_version,
        "batches": [asdict(batch) for batch in projection.active_batches],
        "episode": None
        if episode is None
        else {
            name: getattr(episode, name)
            for name in (
                "episode_id",
                "side",
                "opened_at",
                "closed_at",
                "is_active",
                "cumulative_bought",
                "cumulative_sold",
                "peak_quantity",
            )
        },
        "quantity": projection.total_active_quantity,
        "unallocated": projection.unallocated_quantity,
        "gap": projection.reconciliation_gap,
        "last_trade_at": projection.high_watermark_trade_at,
        "health": health_status,
        "comparable": is_comparable,
        "zero_confirmed": zero_confirmed,
        "snapshot": None
        if latest is None
        else {
            name: getattr(latest, name)
            for name in ("position_amt", "entry_price", "leverage", "margin_type")
        },
        "coverage": None
        if coverage is None
        else {
            "status": coverage.status,
            "gaps": coverage.has_known_gaps,
            "authoritative": coverage.is_authoritative,
        },
    }
    if health_status != PositionHealthStatus.READY or not is_comparable:
        material["current_fact_flags"] = {
            "synthetic": facts.has_synthetic_fills,
            "late": facts.has_late_events,
            "conflicts": bool(facts.conflicting_fills or facts.fact_conflicts),
            "integrity_issue": bool(facts.integrity_issues),
        }
    raw = json.dumps(
        material, sort_keys=True, separators=(",", ":"), default=_version_scalar
    ).encode()
    return "pv_exec1_" + hashlib.sha256(raw).hexdigest()[:55]


def _version_scalar(value: object) -> str:
    if isinstance(value, Decimal):
        if value == 0:
            return "0"
        encoded = format(value, "f")
        return encoded.rstrip("0").rstrip(".") if "." in encoded else encoded
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat(timespec="microseconds")
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"Unsupported execution version field: {type(value).__name__}")
