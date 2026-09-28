"""PositionBook domain service providing authoritative, point-in-time PositionView.

Obays RFC 2026-09-25:
1. Translates immutable account facts into deterministic PositionView;
2. Strict separation between facts, strategy attribution, and observations;
3. No silent quantity clipping or heuristics;
4. Enforces freshness requirements before approving execution readiness.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    AccountFactStreamScope,
    FactCoverageInterval,
    FactCoverageStatus,
    FreshnessRequirement,
    PositionHealthStatus,
    PositionKey,
    PositionLedgerProjection,
    PositionView,
)


@dataclass(frozen=True, slots=True)
class _CachedBaseView:
    projection: PositionLedgerProjection
    version_id: str
    input_revision: int
    base_health_status: PositionHealthStatus
    is_comparable: bool
    base_diagnostics: tuple[str, ...]
    view_coverage: FactCoverageInterval | None
    zero_confirmed: bool
    stream_scope: AccountFactStreamScope | None


class PositionBook:
    """Domain service managing lifecycle projection and authoritative PositionView."""

    def __init__(
        self,
        journal: AccountJournal,
        *,
        ledger: PositionLedger | None = None,
        policy_version: str = "v1",
        schema_version: str = "v1",
    ) -> None:
        self._journal = journal
        self._position_key = journal.position_key
        self._ledger = ledger or PositionLedger(self._position_key)
        self._policy_version = policy_version
        self._schema_version = schema_version
        self._durable_projection_version: str | None = None
        self._durable_projection_revision: int | None = None
        self._durable_projection_event_cut: datetime | None = None
        self._durable_projection_facts_hash: str | None = None
        self._view_cache: dict[tuple[object, ...], _CachedBaseView] = {}

    @property
    def position_key(self) -> PositionKey:
        return self._position_key

    def use_durable_projection_version(
        self,
        token: str | None,
        *,
        event_cut: datetime | None = None,
    ) -> None:
        """Keep the durable CAS token stable for an unchanged restored journal."""
        self._view_cache.clear()
        if token is None:
            self._durable_projection_version = None
            self._durable_projection_revision = None
            self._durable_projection_event_cut = None
            self._durable_projection_facts_hash = None
            return
        if not token.strip():
            raise ValueError("durable projection version must not be empty")
        if event_cut is not None and (
            event_cut.tzinfo is None or event_cut.utcoffset() is None
        ):
            raise ValueError("durable projection event cut must be timezone-aware")
        self._durable_projection_version = token
        self._durable_projection_revision = self._journal.revision
        self._durable_projection_event_cut = event_cut
        self._durable_projection_facts_hash = (
            self._journal.read_cut().compute_facts_hash()
        )

    def get_view(
        self,
        cut: datetime | None = None,
        requirement: FreshnessRequirement | None = None,
        now: datetime | None = None,
    ) -> PositionView:
        """Projects the authoritative PositionView at an explicit event cut."""
        latest = getattr(self._journal, "latest_event_at", None)
        cov = getattr(self._journal, "_coverage", None)
        cov_end = cov.end_at if cov is not None else None
        max_ts = latest
        if cov_end is not None:
            max_ts = max(max_ts, cov_end) if max_ts is not None else cov_end

        effective_cut = None if (cut is not None and (max_ts is None or cut >= max_ts)) else cut
        cache_key = (
            getattr(self._journal, "revision", 0),
            effective_cut,
            self._policy_version,
            self._schema_version,
            self._durable_projection_version,
        )
        cached = self._view_cache.get(cache_key)
        if cached is None:
            facts = self._journal.read_cut(effective_cut)
            projection = self._ledger.project(facts)

            facts_hash = facts.compute_facts_hash()
            version_id = f"pv_{facts_hash[:60]}"
            durable_cut_is_current_or_later = effective_cut is None or (
                self._durable_projection_event_cut is not None
                and effective_cut >= self._durable_projection_event_cut
            )
            if (
                self._durable_projection_version is not None
                and self._durable_projection_revision == self._journal.revision
                and durable_cut_is_current_or_later
                and self._durable_projection_facts_hash == facts_hash
            ):
                version_id = self._durable_projection_version
            input_revision = getattr(self._journal, "revision", 0)

            health_status = projection.health_status
            is_comparable = projection.is_comparable
            diagnostics = list(projection.diagnostics)
            coverage_is_verified = _coverage_anchor_is_verified(facts)
            view_coverage = facts.coverage
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

            latest_snapshot = max(
                facts.snapshots,
                key=lambda snapshot: snapshot.observed_at,
                default=None,
            )
            zero_confirmed = bool(
                len(projection.active_batches) == 0
                and latest_snapshot is not None
                and latest_snapshot.environment == self._position_key.environment
                and latest_snapshot.account_label == self._position_key.account_label
                and latest_snapshot.symbol == self._position_key.symbol
                and latest_snapshot.position_side == self._position_key.position_side.value
                and latest_snapshot.position_amt == Decimal("0")
                and (
                    projection.event_cut is None
                    or latest_snapshot.observed_at >= projection.event_cut
                )
                and (
                    facts.stream_scope is None
                    or (
                        facts.coverage is not None
                        and facts.coverage.status == FactCoverageStatus.CONFIRMED
                        and not facts.coverage.has_known_gaps
                        and facts.coverage.stream_scope == facts.stream_scope
                        and facts.coverage.evidence_observed_at is not None
                        and facts.coverage.covers(latest_snapshot.observed_at)
                        and coverage_is_verified
                        and (
                            projection.event_cut is None
                            or facts.coverage.covers(projection.event_cut)
                        )
                    )
                )
            )
            cached = _CachedBaseView(
                projection=projection,
                version_id=version_id,
                input_revision=input_revision,
                base_health_status=health_status,
                is_comparable=is_comparable,
                base_diagnostics=tuple(diagnostics),
                view_coverage=view_coverage,
                zero_confirmed=zero_confirmed,
                stream_scope=facts.stream_scope,
            )
            if len(self._view_cache) >= 16:
                self._view_cache.pop(next(iter(self._view_cache)))
            self._view_cache[cache_key] = cached

        health_status = cached.base_health_status
        is_comparable = cached.is_comparable
        diagnostics = list(cached.base_diagnostics)

        # Freshness evaluation if requested
        now_dt = now or datetime.now(UTC)
        if requirement is not None:
            if requirement.min_event_cut is not None:
                if (
                    cached.projection.event_cut is None
                    or cached.projection.event_cut < requirement.min_event_cut
                ):
                    health_status = PositionHealthStatus.CATCHING_UP
                    diagnostics.append(
                        f"Event cut ({cached.projection.event_cut}) is behind minimum "
                        f"required cut ({requirement.min_event_cut})"
                    )

            if cached.projection.event_cut is not None:
                staleness = now_dt - cached.projection.event_cut
                if (
                    staleness > requirement.max_staleness
                    and health_status == PositionHealthStatus.READY
                ):
                    health_status = PositionHealthStatus.CATCHING_UP
                    diagnostics.append(
                        f"Projection staleness ({staleness.total_seconds():.1f}s)"
                        "exceeds"
                        f"max allowed"
                        f"({requirement.max_staleness.total_seconds():.1f}s)"
                    )

            if requirement.require_comparable and not is_comparable:
                if health_status == PositionHealthStatus.READY:
                    health_status = PositionHealthStatus.CATCHING_UP

        reconciliation_status = (
            "OK" if health_status == PositionHealthStatus.READY else health_status.value
        )

        return PositionView(
            key=self._position_key,
            projection_version=cached.version_id,
            input_revision=cached.input_revision,
            event_cut=cached.projection.event_cut,
            policy_version=self._policy_version,
            schema_version=self._schema_version,
            coverage=cached.view_coverage,
            active_episode=cached.projection.active_episode,
            batches=cached.projection.active_batches,
            unallocated_quantity=cached.projection.unallocated_quantity,
            reservations=(),
            observation_id=None,
            reconciliation_status=reconciliation_status,
            reconciliation_gap=cached.projection.reconciliation_gap if is_comparable else None,
            health_status=health_status,
            diagnostics=tuple(diagnostics),
            discrepancy=cached.projection.discrepancy,
            is_comparable=is_comparable,
            zero_position_snapshot_confirmed=cached.zero_confirmed,
            stream_scope=cached.stream_scope,
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
    ):
        return False
    if provenance.source_anchor_kind == "zero_snapshot":
        from crypto_momentum_lab.domain.execution.recovery_codec import (
            PositionRecoveryCodec,
        )

        if any(
            snapshot.environment == facts.position_key.environment
            and snapshot.account_label == facts.position_key.account_label
            and snapshot.symbol == facts.position_key.symbol
            and snapshot.position_side == facts.position_key.position_side.value
            and snapshot.position_amt == Decimal("0")
            and snapshot.observed_at == provenance.source_anchor_event_cut
            and PositionRecoveryCodec.stable_snapshot_anchor_id(snapshot)
            == provenance.source_anchor_id
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
                    and checkpoint.parent_event_cut
                    == provenance.source_anchor_event_cut
                )
            )
        )
    return False
