"""Operational overview queries for the operator dashboard.

This module owns the read model for the dashboard's operational overview:
health, collector state, live-account summaries, service freshness, and the
active universe.  Other dashboard domains may reuse the account-summary seam,
but do not need to know how the append-only process and lease rows are joined.
"""

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import Select, and_, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.domain.operational import (
    HealthDimensionName,
    OperationalView,
    aggregate_operational_views,
    evaluate_standard_health,
    read_health,
)
from crypto_momentum_lab.operator_dashboard.collector_status import (
    read_research_collector_status,
)
from crypto_momentum_lab.operator_dashboard.fact_integrity_queries import (
    load_fact_integrity,
)
from crypto_momentum_lab.operator_dashboard.schemas import (
    LiveAccountsResponse,
    LiveAccountSummaryResponse,
    ResearchCollectorResponse,
    ServiceStatusResponse,
    StreamReadinessDetailResponse,
    SystemOverviewResponse,
    SystemReadinessResponse,
    TradeabilityDetailResponse,
    UniverseStatusResponse,
)
from crypto_momentum_lab.operator_dashboard.status import (
    OperationalStatus,
    freshness_status,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountReconciliationHeadRow,
    ExecutionAccountProcessStateRow,
    LiveSessionTransitionRow,
    MonitoringMembershipRow,
    RiskHaltRow,
    RuntimeMarketState15sRow,
    StrategyLiveStateRow,
    StrategyRuntimeCheckpointRow,
    StrategyRuntimeEventRow,
    TradingLeaseRow,
    UniverseEntryRow,
    UniverseSnapshotRow,
)


def published_tradeability(account: LiveAccountSummaryResponse, now: datetime) -> TradeabilityDetailResponse | None:
    observed = account.runtime_observed_at
    if observed is None or not 0 <= (now - observed).total_seconds() <= 180:
        return None
    return account.runtime_tradeability


def latest_live_account_process_statement() -> Select[Any]:
    """Load one current process state per live account.

    The dashboard treats account labels as separate operational units. A
    grouped latest-row lookup keeps the fleet endpoint bounded even when the
    append-only process-state table has accumulated a long history.
    """
    latest = (
        select(
            ExecutionAccountProcessStateRow.account_label,
            func.max(ExecutionAccountProcessStateRow.occurred_at).label(
                "latest_occurred_at"
            ),
        )
        .where(ExecutionAccountProcessStateRow.environment == "live")
        .group_by(ExecutionAccountProcessStateRow.account_label)
        .subquery("latest_live_account_process")
    )
    return (
        select(ExecutionAccountProcessStateRow)
        .join(
            latest,
            and_(
                ExecutionAccountProcessStateRow.account_label == latest.c.account_label,
                ExecutionAccountProcessStateRow.occurred_at
                == latest.c.latest_occurred_at,
                ExecutionAccountProcessStateRow.environment == "live",
            ),
        )
        .order_by(ExecutionAccountProcessStateRow.account_label)
    )


def account_label_sort_key(account_label: str) -> tuple[int, int | str]:
    """Keep the canonical fleet order while allowing custom labels."""
    if account_label == "primary":
        return (0, 0)
    suffix = account_label.removeprefix("account-")
    return (1, int(suffix)) if suffix.isdigit() else (2, account_label)


def live_account_status(
    state: str | None,
    *,
    observed_at: datetime | None = None,
    now: datetime | None = None,
    max_age_seconds: float = 180.0,
) -> OperationalStatus:
    if state is None:
        return OperationalStatus.UNKNOWN
    if observed_at is not None and now is not None:
        if (now - observed_at).total_seconds() > max_age_seconds:
            return OperationalStatus.STALE
    if state in ("running", "ready_readonly"):
        return OperationalStatus.READY
    if state == "syncing":
        return OperationalStatus.DEGRADED
    return OperationalStatus.HALTED


def live_account_fleet_status(
    accounts: Sequence[LiveAccountSummaryResponse],
) -> OperationalStatus:
    if not accounts:
        return OperationalStatus.NO_DATA
    if any(account.status is OperationalStatus.HALTED for account in accounts):
        return OperationalStatus.HALTED
    if any(account.status is OperationalStatus.STALE for account in accounts):
        return OperationalStatus.STALE
    if any(account.status is OperationalStatus.UNKNOWN for account in accounts):
        return OperationalStatus.UNKNOWN
    if any(account.status is OperationalStatus.DEGRADED for account in accounts):
        return OperationalStatus.DEGRADED
    return (
        OperationalStatus.READY
        if all(account.status is OperationalStatus.READY for account in accounts)
        else OperationalStatus.DEGRADED
    )


def live_account_summaries(
    processes: Sequence[ExecutionAccountProcessStateRow],
    strategy_states: Sequence[StrategyLiveStateRow],
    leases: Sequence[TradingLeaseRow],
    *,
    now: datetime | None = None,
) -> list[LiveAccountSummaryResponse]:
    """Join current process, strategy, and lease state by account label."""
    process_by_account = {row.account_label: row for row in processes}
    strategy_by_account: dict[str, StrategyLiveStateRow] = {}
    for row in sorted(
        strategy_states,
        key=lambda item: item.changed_at,
        reverse=True,
    ):
        strategy_by_account.setdefault(row.account_label, row)
    lease_by_account = {row.account_label: row for row in leases}
    account_labels = sorted(
        set(process_by_account) | set(strategy_by_account) | set(lease_by_account),
        key=account_label_sort_key,
    )
    summaries: list[LiveAccountSummaryResponse] = []
    for account_label in account_labels:
        process_row = process_by_account.get(account_label)
        strategy = strategy_by_account.get(account_label)
        lease = lease_by_account.get(account_label)
        occurred_at = process_row.occurred_at if process_row is not None else None
        state = process_row.state if process_row is not None else None
        summaries.append(
            LiveAccountSummaryResponse(
                account_label=account_label,
                environment=(
                    process_row.environment if process_row is not None else "live"
                ),
                status=live_account_status(
                    state,
                    observed_at=occurred_at,
                    now=now,
                ),
                readiness=(state if state is not None else "missing"),
                observed_at=occurred_at,
                strategy_name=(
                    strategy.strategy_name
                    if strategy is not None
                    else lease.strategy_name
                    if lease is not None
                    else None
                ),
                strategy_state=(strategy.state if strategy is not None else None),
                lease_expires_at=(lease.expires_at if lease is not None else None),
            )
        )
    return summaries


class OverviewQueries:
    """Deep query module for the dashboard's operational overview."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime],
        stale_after_seconds: float,
        research_collector_root: Path,
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock
        self._stale_after_seconds = stale_after_seconds
        self._research_collector_root = research_collector_root

    async def health(self) -> dict[str, str]:
        try:
            async with self._session_factory() as session:
                await session.execute(text("SELECT 1"))
            return {"app_status": "UP", "database_status": "UP"}
        except Exception:
            return {"app_status": "UP", "database_status": "DOWN"}

    async def operational_health(self) -> dict[str, Any]:
        """Evaluates authoritative operational read model (R6 / Section 12.3)."""
        now = self._clock()
        liveness = await self.health()
        db_up = liveness.get("database_status") == "UP"

        try:
            accounts_resp = await self.live_accounts()
        except Exception as exc:
            view = evaluate_standard_health(
                scope="system",
                liveness_ok=db_up,
                liveness_details=f"query_error: {exc}",
                lag_seconds=999.0,
                fact_gaps_count=None,
                capability_permitted=False,
                capability_reason=f"query_error: {exc}",
                reconciliation_matched=False,
                reconciliation_details=f"query_error: {exc}",
                observed_at=now,
            )
            return {
                "scope": view.scope,
                "overall_status": view.overall_status.value,
                "is_execution_ready": view.is_execution_ready,
                "source_as_of": view.source_as_of.isoformat(),
                "evaluated_at": view.evaluated_at.isoformat(),
                "dimensions": [
                    {
                        "name": d.name.value,
                        "status": d.status.value,
                        "details": d.details,
                        "observed_at": d.observed_at.isoformat(),
                        "metric_value": d.metric_value,
                    }
                    for d in view.dimensions
                ],
                "details": view.details,
            }

        heads_by_account: dict[str, AccountReconciliationHeadRow] = {}
        try:
            async with self._session_factory() as session:
                heads = (
                    await session.scalars(
                        select(AccountReconciliationHeadRow).where(
                            AccountReconciliationHeadRow.environment == "live"
                        )
                    )
                ).all()
                heads_by_account = {h.account_label: h for h in heads}
        except Exception:
            pass

        fact_integrity = {}
        try:
            fact_integrity = await load_fact_integrity(
                self._session_factory, accounts=heads_by_account, now=now
            )
        except Exception:
            # An unavailable or malformed proof cannot be reported as complete.
            pass
        account_views: list[OperationalView] = []
        for acc in accounts_resp.accounts:
            observed = acc.observed_at or now
            lag = max(0.0, (now - observed).total_seconds())

            head = heads_by_account.get(acc.account_label)
            if head is not None:
                recon_matched = (
                    head.status in ("OK", "reconciled", "matched", "ready")
                    and head.mismatch_count == 0
                )
                head_st = head.status.lower()
                recon_details = (
                    f"reconciliation_{head_st}_mismatches_{head.mismatch_count}"
                )
            else:
                recon_matched = False
                recon_details = "unconfirmed_reconciliation_head_absent"

            runtime_gate = published_tradeability(acc, now)
            capability_reason = (
                runtime_gate.entry_gate_reason if runtime_gate is not None
                else "runtime_readiness_missing_or_stale"
            )
            cap_ok = runtime_gate is not None and runtime_gate.entry_gate_open

            acc_view = evaluate_standard_health(
                scope=f"account:{acc.account_label}",
                liveness_ok=db_up and acc.status != OperationalStatus.HALTED,
                liveness_details=f"account_status_{acc.status.value}",
                lag_seconds=lag,
                max_lag_seconds=90.0,
                # Only a verified current ledger checkpoint establishes coverage.
                fact_gaps_count=(
                    fact_integrity[acc.account_label].gaps
                    if acc.account_label in fact_integrity
                    else None
                ),
                capability_permitted=cap_ok,
                capability_reason=capability_reason,
                reconciliation_matched=recon_matched,
                reconciliation_details=recon_details,
                observed_at=observed,
            )
            summary = fact_integrity.get(acc.account_label)
            if summary is not None and summary.observed_at is not None:
                acc_view = read_health(
                    acc_view.scope,
                    tuple(
                        replace(dimension, observed_at=summary.observed_at)
                        if dimension.name == HealthDimensionName.FACT_INTEGRITY
                        else dimension
                        for dimension in acc_view.dimensions
                    ),
                    evidence_cut=now,
                )
            account_views.append(acc_view)

        if account_views:
            composite_view = aggregate_operational_views(
                tuple(account_views),
                composite_scope="system",
                evaluated_at=now,
            )
        else:
            composite_view = evaluate_standard_health(
                scope="system",
                liveness_ok=db_up,
                lag_seconds=0.0,
                fact_gaps_count=0,
                capability_permitted=False,
                capability_reason="no_live_accounts_configured",
                reconciliation_matched=True,
                observed_at=now,
            )

        return {
            "scope": composite_view.scope,
            "overall_status": composite_view.overall_status.value,
            "is_execution_ready": composite_view.is_execution_ready,
            "source_as_of": composite_view.source_as_of.isoformat(),
            "evaluated_at": composite_view.evaluated_at.isoformat(),
            "dimensions": [
                {
                    "name": d.name.value,
                    "status": d.status.value,
                    "details": d.details,
                    "observed_at": d.observed_at.isoformat(),
                    "metric_value": d.metric_value,
                }
                for d in composite_view.dimensions
            ],
            "details": composite_view.details,
        }

    async def readiness(self) -> SystemReadinessResponse:
        now = self._clock()
        try:
            liveness = await self.health()
            accounts_resp = await self.live_accounts()
            overview_resp = await self.overview()
        except Exception as exc:
            return SystemReadinessResponse(
                status=OperationalStatus.DEGRADED,
                observed_at=now,
                liveness={"app_status": "UP", "database_status": "DOWN"},
                tradeability=TradeabilityDetailResponse(
                    mode="UNKNOWN",
                    entry_gate_open=False,
                    entry_gate_reason=f"readiness_query_failed: {exc}",
                    exit_gate_open=False,
                    exit_gate_reason=f"readiness_query_failed: {exc}",
                    unmanaged_risk_clear=False,
                    halt_active=False,
                ),
                stream_readiness=StreamReadinessDetailResponse(
                    overall="DOWN",
                    streams={},
                ),
                accounts=[],
            )

        has_halt = overview_resp.active_halt_count > 0 or any(
            a.status == OperationalStatus.HALTED for a in accounts_resp.accounts
        )
        observed_halt = has_halt
        database_up = liveness.get("database_status") == "UP"

        streams_dict: dict[str, str] = {}
        for s in overview_resp.services:
            if s.name == "database":
                continue
            streams_dict[s.name] = (
                "READY"
                if s.status
                in (
                    OperationalStatus.FRESH,
                    OperationalStatus.READY,
                    OperationalStatus.LIVE,
                )
                else "RECOVERING"
            )
        streams_all_ready = bool(
            streams_dict and all(v == "READY" for v in streams_dict.values())
        )

        published = [published_tradeability(a, now) for a in accounts_resp.accounts]
        confirmed = bool(published) and all(item is not None for item in published)
        runtime_running = confirmed and any(
            item.mode == "RUNNING" for item in published if item is not None
        )
        if confirmed:
            gates = [item for item in published if item is not None]
            blocked = next((item for item in gates if not item.entry_gate_open), gates[0])
            entry_gate_open = all(item.entry_gate_open for item in gates)
            exit_gate_open = all(item.exit_gate_open for item in gates)
            entry_gate_reason = blocked.entry_gate_reason
            exit_gate_reason = next((item.exit_gate_reason for item in gates
                                     if not item.exit_gate_open), gates[0].exit_gate_reason)
            unmanaged_risk_clear = all(item.unmanaged_risk_clear for item in gates)
            has_halt = any(item.halt_active for item in gates)
            if runtime_running:
                mode = "RUNNING"
            elif has_halt:
                mode = "HALTED"
            elif all(item.mode == "FULLY_TRADEABLE" for item in gates):
                mode = "FULLY_TRADEABLE"
            else:
                mode = "RUNNING"
            status = (
                OperationalStatus.HALTED
                if has_halt
                else (
                    OperationalStatus.READY
                    if (entry_gate_open or any(item.mode == "RUNNING" for item in gates))
                    else OperationalStatus.DEGRADED
                )
            )
        else:
            status = OperationalStatus.UNKNOWN
            mode = "UNKNOWN"
            entry_gate_open = exit_gate_open = unmanaged_risk_clear = False
            entry_gate_reason = exit_gate_reason = "runtime_readiness_missing_or_stale"
            has_halt = False
        if not database_up:
            status = OperationalStatus.DOWN
            mode = "UNKNOWN"
            entry_gate_open = exit_gate_open = False
            entry_gate_reason = exit_gate_reason = "runtime_readiness_unavailable"

        if observed_halt:
            status = OperationalStatus.HALTED
            mode = "RUNNING" if runtime_running else "HALTED"
            has_halt = True
            entry_gate_open = exit_gate_open = unmanaged_risk_clear = False
            entry_gate_reason = exit_gate_reason = "halt_active"

        stream_overall = (
            "READY"
            if streams_all_ready and bool(accounts_resp.accounts)
            else "RECOVERING"
        )

        return SystemReadinessResponse(
            status=status,
            observed_at=now,
            liveness=liveness,
            tradeability=TradeabilityDetailResponse(
                mode=mode,
                entry_gate_open=entry_gate_open,
                entry_gate_reason=entry_gate_reason,
                exit_gate_open=exit_gate_open,
                exit_gate_reason=exit_gate_reason,
                unmanaged_risk_clear=unmanaged_risk_clear,
                halt_active=has_halt,
            ),
            stream_readiness=StreamReadinessDetailResponse(
                overall=stream_overall,
                streams=streams_dict,
            ),
            accounts=accounts_resp.accounts,
        )

    async def research_collector(self) -> ResearchCollectorResponse:
        return await asyncio.to_thread(
            read_research_collector_status,
            self._research_collector_root,
            now=self._clock(),
        )

    async def live_accounts(self) -> LiveAccountsResponse:
        """Return a small, bounded operational snapshot for every live account."""
        now = self._clock()
        async with self._session_factory() as session:
            processes = (
                await session.scalars(latest_live_account_process_statement())
            ).all()
            strategy_states = (
                await session.scalars(
                    select(StrategyLiveStateRow).where(
                        StrategyLiveStateRow.environment == "live"
                    )
                )
            ).all()
            leases = (
                await session.scalars(
                    select(TradingLeaseRow)
                    .where(
                        TradingLeaseRow.environment == "live",
                        TradingLeaseRow.state == "active",
                        TradingLeaseRow.expires_at > now,
                    )
                    .order_by(TradingLeaseRow.expires_at.desc())
                )
            ).all()
        accounts = live_account_summaries(
            processes,
            strategy_states,
            leases,
            now=now,
        )
        async with self._session_factory() as session:
            for account in accounts:
                row = await session.scalar(select(StrategyRuntimeEventRow).where(
                    StrategyRuntimeEventRow.event_type == "runtime_readiness",
                    StrategyRuntimeEventRow.occurred_at >= now - timedelta(seconds=180),
                    StrategyRuntimeEventRow.occurred_at <= now,
                    StrategyRuntimeEventRow.details["account_label"].astext == account.account_label,
                ).order_by(StrategyRuntimeEventRow.occurred_at.desc()).limit(1))
                if row is None:
                    continue
                payload = row.details
                lease = next((l for l in leases if l.account_label == account.account_label), None)
                if (payload.get("schema_version") != 1 or lease is None
                        or payload.get("code_commit") != lease.code_generation
                        or payload.get("session_id") != row.run_id):
                    continue
                try:
                    account.runtime_tradeability = TradeabilityDetailResponse.model_validate(payload["tradeability"], strict=True)
                    account.runtime_observed_at = row.occurred_at
                except (ValueError, KeyError, TypeError):
                    continue

        return LiveAccountsResponse(
            status=live_account_fleet_status(accounts),
            accounts=accounts,
        )

    async def overview(self) -> SystemOverviewResponse:
        now = self._clock()
        async with self._session_factory() as session:
            market_at = await session.scalar(
                select(RuntimeMarketState15sRow.bucket_end)
                .order_by(RuntimeMarketState15sRow.bucket_start.desc())
                .limit(1)
            )
            account_rows = (
                await session.scalars(latest_live_account_process_statement())
            ).all()
            account = max(
                account_rows,
                key=lambda row: row.occurred_at,
                default=None,
            )
            strategy_at = await session.scalar(
                select(StrategyRuntimeCheckpointRow.saved_at)
                .order_by(StrategyRuntimeCheckpointRow.saved_at.desc())
                .limit(1)
            )
            halt_count = await session.scalar(
                select(func.count(RiskHaltRow.halt_id)).where(
                    RiskHaltRow.active.is_(True)
                )
            )
            strategy_states = (
                await session.scalars(
                    select(StrategyLiveStateRow).where(
                        StrategyLiveStateRow.environment == "live"
                    )
                )
            ).all()
            leases = (
                await session.scalars(
                    select(TradingLeaseRow)
                    .where(
                        TradingLeaseRow.environment == "live",
                        TradingLeaseRow.state == "active",
                        TradingLeaseRow.expires_at > now,
                    )
                    .order_by(TradingLeaseRow.expires_at.desc())
                )
            ).all()
            primary_lease = next(
                (item for item in leases if item.account_label == "primary"),
                None,
            )
            lease = primary_lease or (leases[0] if leases else None)
            live = await session.scalar(
                select(LiveSessionTransitionRow)
                .order_by(LiveSessionTransitionRow.occurred_at.desc())
                .limit(1)
            )
            live_heartbeat_at = None
            live_started_at = None
            if live is not None:
                # A runtime checkpoint is meaningful for the current session
                # only after the daemon has entered live_enabled. During
                # preflight retries, an older checkpoint would make a healthy
                # retry loop look dead in the dashboard.
                live_heartbeat_at = await session.scalar(
                    select(StrategyRuntimeCheckpointRow.saved_at).where(
                        StrategyRuntimeCheckpointRow.run_id == live.session_id
                    )
                )
                live_started_at = await session.scalar(
                    select(LiveSessionTransitionRow.occurred_at)
                    .where(
                        LiveSessionTransitionRow.session_id == live.session_id,
                        LiveSessionTransitionRow.state == "live_enabled",
                    )
                    .order_by(LiveSessionTransitionRow.occurred_at.desc())
                    .limit(1)
                )
        account_at = None if account is None else account.occurred_at
        account_statuses = live_account_summaries(
            account_rows,
            strategy_states=strategy_states,
            leases=leases,
            now=now,
        )
        services = [
            service("market-data", now, market_at, self._stale_after_seconds),
            service(
                "execution-account",
                now,
                account_at,
                self._stale_after_seconds,
            ),
            service(
                "strategy-runner",
                now,
                strategy_at,
                self._stale_after_seconds,
            ),
            ServiceStatusResponse(
                name="database",
                status=OperationalStatus.READY,
                observed_at=now,
                age_seconds=0,
            ),
        ]
        if live is not None:
            live_status = (
                OperationalStatus.LIVE
                if live.state == "live_enabled"
                else OperationalStatus.HALTED
                if live.state == "halted"
                else OperationalStatus.SHADOW
            )
            live_observed_at, heartbeat_source = live_observation(
                state=live.state,
                runtime_checkpoint_at=live_heartbeat_at,
                transition_at=live.occurred_at,
            )
            live_details: dict[str, JsonValue] = {
                "state": live.state,
                "session_id": live.session_id,
                "heartbeat_source": heartbeat_source,
            }
            if live_started_at is not None:
                live_details["started_at"] = live_started_at.isoformat()
            services.append(
                ServiceStatusResponse(
                    name="live-rollout",
                    status=live_status,
                    observed_at=live_observed_at,
                    age_seconds=age(now, live_observed_at),
                    details=live_details,
                )
            )
        return SystemOverviewResponse(
            generated_at=now,
            database_status=OperationalStatus.READY,
            services=services,
            active_halt_count=int(halt_count or 0),
            active_lease=None
            if lease is None
            else {
                "account_label": lease.account_label,
                "strategy_name": lease.strategy_name,
                "owner": lease.owner,
                "expires_at": lease.expires_at.isoformat(),
            },
            active_leases=[
                {
                    "account_label": item.account_label,
                    "strategy_name": item.strategy_name,
                    "owner": item.owner,
                    "expires_at": item.expires_at.isoformat(),
                }
                for item in leases
            ],
            account_statuses=account_statuses,
        )

    async def universe(self) -> UniverseStatusResponse:
        async with self._session_factory() as session:
            snapshot = await session.scalar(
                select(UniverseSnapshotRow)
                .where(UniverseSnapshotRow.activated.is_(True))
                .order_by(UniverseSnapshotRow.observed_at.desc())
                .limit(1)
            )
            if snapshot is None:
                return UniverseStatusResponse(
                    status=OperationalStatus.NO_DATA,
                    observed_at=None,
                    gainers=[],
                    losers=[],
                    monitored_symbols=[],
                )
            entries = (
                await session.scalars(
                    select(UniverseEntryRow).where(
                        UniverseEntryRow.snapshot_id == snapshot.snapshot_id
                    )
                )
            ).all()
            memberships = (
                await session.scalars(
                    select(MonitoringMembershipRow).where(
                        MonitoringMembershipRow.snapshot_id == snapshot.snapshot_id
                    )
                )
            ).all()
            entries_by_symbol = {entry.symbol: entry for entry in entries}
        gainers = sorted(
            (entry for entry in entries if entry.gainer_rank is not None),
            key=lambda item: item.gainer_rank or 999,
        )[:20]
        return UniverseStatusResponse(
            status=freshness_status(
                now=self._clock(),
                observed_at=snapshot.observed_at,
                stale_after_seconds=3600,
            ),
            observed_at=snapshot.observed_at,
            gainers=[universe_entry(row, "gainer") for row in gainers],
            # Loser ranks remain in the database for historical/research
            # compatibility, but the operational dashboard is gainer-only.
            losers=[],
            monitored_symbols=[
                universe_membership(row, entries_by_symbol.get(row.symbol))
                for row in sorted(
                    memberships,
                    key=lambda row: (
                        {"target": 0, "retained": 1, "forced": 2}.get(
                            row.status,
                            99,
                        ),
                        {"gainer": 0, "loser": 1}.get(row.side or "", 2),
                        row.symbol,
                    ),
                )
            ],
        )


def service(
    name: str,
    now: datetime,
    observed_at: datetime | None,
    stale_after_seconds: float,
) -> ServiceStatusResponse:
    return ServiceStatusResponse(
        name=name,
        status=freshness_status(
            now=now,
            observed_at=observed_at,
            stale_after_seconds=stale_after_seconds,
        ),
        observed_at=observed_at,
        age_seconds=None if observed_at is None else age(now, observed_at),
    )


def live_observation(
    *,
    state: str,
    runtime_checkpoint_at: datetime | None,
    transition_at: datetime,
    max_checkpoint_lead_seconds: float = 30.0,
) -> tuple[datetime, str]:
    """Choose a heartbeat that belongs to the live session's current state."""
    if (
        state == "live_enabled"
        and runtime_checkpoint_at is not None
        and runtime_checkpoint_at
        >= transition_at - timedelta(seconds=max_checkpoint_lead_seconds)
    ):
        return runtime_checkpoint_at, "runtime_checkpoint"
    return transition_at, "state_transition"


def age(now: datetime, observed_at: datetime) -> float:
    return max(0.0, (now - observed_at).total_seconds())


def universe_entry(row: UniverseEntryRow, side: str) -> dict[str, JsonValue]:
    rank = row.gainer_rank if side == "gainer" else row.loser_rank
    return {
        "symbol": row.symbol,
        "rank": rank,
        "utc_day_return": None
        if row.utc_day_return is None
        else str(row.utc_day_return),
        "current_price": None if row.current_price is None else str(row.current_price),
    }


def universe_membership(
    row: MonitoringMembershipRow,
    entry: UniverseEntryRow | None,
) -> dict[str, JsonValue]:
    rank = None
    utc_day_return = None
    current_price = None
    if entry is not None:
        rank = (
            entry.gainer_rank
            if row.side == "gainer"
            else entry.loser_rank
            if row.side == "loser"
            else None
        )
        utc_day_return = (
            None if entry.utc_day_return is None else str(entry.utc_day_return)
        )
        current_price = (
            None if entry.current_price is None else str(entry.current_price)
        )
    return {
        "symbol": row.symbol,
        "status": row.status,
        "side": row.side,
        "rank": rank,
        "utc_day_return": utc_day_return,
        "current_price": current_price,
    }


__all__ = [
    "OverviewQueries",
    "account_label_sort_key",
    "age",
    "latest_live_account_process_statement",
    "live_account_fleet_status",
    "live_account_status",
    "live_account_summaries",
    "live_observation",
    "service",
    "universe_entry",
    "universe_membership",
]
