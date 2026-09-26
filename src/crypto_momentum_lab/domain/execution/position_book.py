"""PositionBook domain service providing authoritative, point-in-time PositionView.

Obays RFC 2026-09-25:
1. Translates immutable account facts into deterministic PositionView;
2. Strict separation between facts, strategy attribution, and observations;
3. No silent quantity clipping or heuristics;
4. Enforces freshness requirements before approving execution readiness.
"""

from __future__ import annotations

from datetime import UTC, datetime

from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    FreshnessRequirement,
    PositionHealthStatus,
    PositionKey,
    PositionView,
)


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
        self._projection_counter = 0

    @property
    def position_key(self) -> PositionKey:
        return self._position_key

    def get_view(
        self,
        cut: datetime | None = None,
        requirement: FreshnessRequirement | None = None,
        now: datetime | None = None,
    ) -> PositionView:
        """Projects the authoritative PositionView at an explicit event cut."""
        self._projection_counter += 1
        version_id = f"pv_{self._position_key.symbol}_{self._projection_counter}"

        facts = self._journal.read_cut(cut)
        projection = self._ledger.project(facts)

        health_status = projection.health_status
        is_comparable = projection.is_comparable
        diagnostics = list(projection.diagnostics)

        # Freshness evaluation if requested
        now_dt = now or datetime.now(UTC)
        if requirement is not None:
            if requirement.min_event_cut is not None:
                if (
                    projection.event_cut is None
                    or projection.event_cut < requirement.min_event_cut
                ):
                    health_status = PositionHealthStatus.CATCHING_UP
                    diagnostics.append(
                        f"Event cut ({projection.event_cut}) is behind minimum "
                        f"required cut ({requirement.min_event_cut})"
                    )

            if projection.event_cut is not None:
                staleness = now_dt - projection.event_cut
                if (
                    staleness > requirement.max_staleness
                    and health_status == PositionHealthStatus.READY
                ):
                    health_status = PositionHealthStatus.CATCHING_UP
                    diagnostics.append(
                        f"Projection staleness ({staleness.total_seconds():.1f}s)"
                        "exceeds"
                        f"max allowed"
                        "({requirement.max_staleness.total_seconds():.1f}s)"
                    )

            if requirement.require_comparable and not is_comparable:
                if health_status == PositionHealthStatus.READY:
                    health_status = PositionHealthStatus.CATCHING_UP

        reconciliation_status = (
            "OK" if health_status == PositionHealthStatus.READY else health_status.value
        )

        return PositionView(
            key=self._position_key,
            projection_version=version_id,
            input_revision=self._projection_counter,
            event_cut=projection.event_cut,
            policy_version=self._policy_version,
            schema_version=self._schema_version,
            coverage=facts.coverage,
            active_episode=projection.active_episode,
            batches=projection.active_batches,
            unallocated_quantity=projection.unallocated_quantity,
            reservations=(),
            observation_id=None,
            reconciliation_status=reconciliation_status,
            reconciliation_gap=projection.reconciliation_gap if is_comparable else None,
            health_status=health_status,
            diagnostics=tuple(diagnostics),
            discrepancy=projection.discrepancy,
            is_comparable=is_comparable,
        )
