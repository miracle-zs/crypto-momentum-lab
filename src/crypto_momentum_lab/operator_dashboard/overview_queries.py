"""Operational overview queries for the operator dashboard.

This module owns the read model for the dashboard's operational overview:
health, collector state, live-account summaries, service freshness, and the
active universe.  Other dashboard domains may reuse the account-summary seam,
but do not need to know how the append-only process and lease rows are joined.
"""

import asyncio
from collections.abc import Callable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import Select, and_, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.operator_dashboard.collector_status import (
    read_research_collector_status,
)
from crypto_momentum_lab.operator_dashboard.schemas import (
    LiveAccountsResponse,
    LiveAccountSummaryResponse,
    ResearchCollectorResponse,
    ServiceStatusResponse,
    SystemOverviewResponse,
    UniverseStatusResponse,
)
from crypto_momentum_lab.operator_dashboard.status import (
    OperationalStatus,
    freshness_status,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExecutionAccountProcessStateRow,
    LiveSessionTransitionRow,
    MonitoringMembershipRow,
    RiskHaltRow,
    RuntimeMarketState15sRow,
    StrategyLiveStateRow,
    StrategyRuntimeCheckpointRow,
    TradingLeaseRow,
    UniverseEntryRow,
    UniverseSnapshotRow,
)


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
                ExecutionAccountProcessStateRow.account_label
                == latest.c.account_label,
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


def live_account_status(state: str | None) -> OperationalStatus:
    if state is None:
        return OperationalStatus.UNKNOWN
    return (
        OperationalStatus.READY
        if state == "ready_readonly"
        else OperationalStatus.HALTED
    )


def live_account_fleet_status(
    accounts: Sequence[LiveAccountSummaryResponse],
) -> OperationalStatus:
    if not accounts:
        return OperationalStatus.NO_DATA
    return (
        OperationalStatus.READY
        if all(account.status is OperationalStatus.READY for account in accounts)
        else OperationalStatus.HALTED
    )


def live_account_summaries(
    processes: Sequence[ExecutionAccountProcessStateRow],
    strategy_states: Sequence[StrategyLiveStateRow],
    leases: Sequence[TradingLeaseRow],
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
        set(process_by_account)
        | set(strategy_by_account)
        | set(lease_by_account),
        key=account_label_sort_key,
    )
    summaries: list[LiveAccountSummaryResponse] = []
    for account_label in account_labels:
        strategy = strategy_by_account.get(account_label)
        lease = lease_by_account.get(account_label)
        summaries.append(
            LiveAccountSummaryResponse(
                account_label=account_label,
                environment=(
                    process_by_account[account_label].environment
                    if account_label in process_by_account
                    else "live"
                ),
                status=live_account_status(
                    None
                    if account_label not in process_by_account
                    else process_by_account[account_label].state
                ),
                readiness=(
                    process_by_account[account_label].state
                    if account_label in process_by_account
                    else "missing"
                ),
                observed_at=(
                    process_by_account[account_label].occurred_at
                    if account_label in process_by_account
                    else None
                ),
                strategy_name=(
                    strategy.strategy_name
                    if strategy is not None
                    else lease.strategy_name
                    if lease is not None
                    else None
                ),
                strategy_state=(
                    strategy.state
                    if strategy is not None
                    else None
                ),
                lease_expires_at=(
                    lease.expires_at
                    if lease is not None
                    else None
                ),
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
        async with self._session_factory() as session:
            await session.execute(text("SELECT 1"))
        return {"app_status": "UP", "database_status": "UP"}

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
        accounts = live_account_summaries(processes, strategy_states, leases)
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
            lease = leases[0] if leases else None
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
        losers = sorted(
            (entry for entry in entries if entry.loser_rank is not None),
            key=lambda item: item.loser_rank or 999,
        )[:20]
        return UniverseStatusResponse(
            status=freshness_status(
                now=self._clock(),
                observed_at=snapshot.observed_at,
                stale_after_seconds=3600,
            ),
            observed_at=snapshot.observed_at,
            gainers=[universe_entry(row, "gainer") for row in gainers],
            losers=[universe_entry(row, "loser") for row in losers],
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
) -> tuple[datetime, str]:
    """Choose a heartbeat that belongs to the live session's current state."""
    if state == "live_enabled" and runtime_checkpoint_at is not None:
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
            None
            if entry.utc_day_return is None
            else str(entry.utc_day_return)
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
