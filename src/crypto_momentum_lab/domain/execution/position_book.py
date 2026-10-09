"""PositionBook domain service providing authoritative, point-in-time PositionView.

Obays RFC 2026-09-25:
1. Translates immutable account facts into deterministic PositionView;
2. Strict separation between facts, strategy attribution, and observations;
3. No silent quantity clipping or heuristics;
4. Enforces freshness requirements before approving execution readiness.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum

from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
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
from crypto_momentum_lab.domain.execution.recovery_models import (
    AccountFacts,
    DurableJournalCut,
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
        self._durable_projection_event_cut: datetime | None = None
        self._durable_execution_state_version: str | None = None
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
        """Preserve a restored CAS token while execution state remains equivalent."""
        self._view_cache.clear()
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
        self._durable_execution_state_version = None
        self._durable_projection_version = None
        self._durable_execution_state_version = self.get_view().projection_version
        self._view_cache.clear()
        self._durable_projection_version = token
        self._durable_projection_event_cut = event_cut

    def get_historical_view(
        self,
        durable_cut: DurableJournalCut,
        *,
        event_cut: datetime,
        requirement: FreshnessRequirement | None = None,
        now: datetime | None = None,
    ) -> PositionView:
        """Project a persisted cut with this book's rules and independent view state."""
        historical = PositionBook(
            AccountJournal.from_durable_cut(durable_cut),
            ledger=self._ledger,
            policy_version=self._policy_version,
            schema_version=self._schema_version,
        )
        return historical.get_view(cut=event_cut, requirement=requirement, now=now)

    def copy_for_transaction(
        self, journal: AccountJournal | None = None
    ) -> PositionBook:
        """Copy publication state for a transaction candidate.

        The ledger is stateless and cached projections are immutable, so the
        candidate only needs its own view-cache mapping, plus the candidate's
        journal, to stay isolated from the published book. Passing ``journal``
        keeps ``book._journal`` pointing at the copy instead of the published
        journal.
        """
        candidate = copy.copy(self)
        if journal is not None:
            candidate._journal = journal
        candidate._view_cache = dict(self._view_cache)
        return candidate

    def get_view(
        self,
        cut: datetime | None = None,
        requirement: FreshnessRequirement | None = None,
        now: datetime | None = None,
    ) -> PositionView:
        """Projects the authoritative PositionView at an explicit event cut."""
        max_ts = self._journal.latest_event_at

        effective_cut = (
            None if (cut is not None and (max_ts is None or cut >= max_ts)) else cut
        )
        cache_key = (
            self._journal.revision,
            # Applying a recovery checkpoint changes the facts without moving the
            # revision, so the generation must be part of the identity.
            self._journal.facts_generation,
            effective_cut,
            self._policy_version,
            self._schema_version,
            self._durable_projection_version,
        )
        cached = self._view_cache.get(cache_key)
        if cached is None:
            facts = self._journal.read_cut(effective_cut)
            projection = self._ledger.project(facts)

            input_revision = self._journal.revision

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
                view_coverage = replace(
                    facts.coverage, status=FactCoverageStatus.PENDING
                )
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
                and latest_snapshot.position_side
                == self._position_key.position_side.value
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
            execution_state_version = _execution_state_version(
                facts=facts,
                projection=projection,
                health_status=health_status,
                is_comparable=is_comparable,
                coverage=view_coverage,
                zero_confirmed=zero_confirmed,
                policy_version=self._policy_version,
                schema_version=self._schema_version,
            )
            version_id = execution_state_version
            durable_cut_is_current_or_later = effective_cut is None or (
                self._durable_projection_event_cut is not None
                and effective_cut >= self._durable_projection_event_cut
            )
            if (
                self._durable_projection_version is not None
                and durable_cut_is_current_or_later
                and execution_state_version == self._durable_execution_state_version
            ):
                version_id = self._durable_projection_version
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
            reconciliation_gap=cached.projection.reconciliation_gap
            if is_comparable
            else None,
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
                    and checkpoint.parent_event_cut
                    == provenance.source_anchor_event_cut
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
    """CAS identity for execution economics, independent of observation timestamps.

    Journal revision and the complete facts hash remain the audit and persistence
    identities. Reservation capacity is checked independently in the locked
    acceptance transaction. Unhealthy facts retain their full identity.
    """
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
            for name in (
                "position_amt",
                "entry_price",
                "leverage",
                "margin_type",
            )
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
        material["unhealthy_facts_hash"] = facts.compute_facts_hash()
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
