import asyncio
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.apps.shadow_operation.main import (
    _latest_account_state,
    _latest_risk_config,
    _load_trading_rules,
)
from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.live_rollout import LiveOperatorApproval
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import RiskConfigSnapshot, StrategyLiveState
from crypto_momentum_lab.execution_account.orders.state_machine import SubmitPolicy
from crypto_momentum_lab.live_rollout.daemon import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.gates import LiveGateContext
from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    PostgresLiveRolloutRepository,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillEventRow,
    AccountPositionSnapshotRow,
    ExchangeOrderRow,
    ExecutionAccountProcessStateRow,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PostgresOrderRepository,
)
from crypto_momentum_lab.persistence.postgres.risk_repository import (
    PostgresRiskRepository,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
    RuntimeStateCursor,
)


class PostgresLiveContextProvider:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        account_label: str,
        strategy_name: str,
        strategy_config_hash: str,
        git_commit_hash: str,
        migration_revision: str,
        lease_owner: str,
        approval_id: str,
    ) -> None:
        self._sessions = session_factory
        self._account_label = account_label
        self._strategy_name = strategy_name
        self._strategy_config_hash = strategy_config_hash
        self._git_commit_hash = git_commit_hash
        self._migration_revision = migration_revision
        self._lease_owner = lease_owner
        self._approval_id = approval_id
        self._risk_repository = PostgresRiskRepository(session_factory)
        self._live_repository = PostgresLiveRolloutRepository(session_factory)
        self._order_repository = PostgresOrderRepository(session_factory)

    async def __call__(self, state: MarketState15s) -> LiveDaemonRuntimeContext:
        now = datetime.now(tz=UTC)
        approval = await self._live_repository.load_active_approval(
            account_label=self._account_label,
            strategy_name=self._strategy_name,
            now=now,
        )
        if approval is not None and approval.approval_id != self._approval_id:
            approval = None
        risk_config = await _latest_risk_config(
            self._sessions,
            self._account_label,
        )
        lease = await self._risk_repository.load_active_lease(
            "live",
            self._account_label,
            now,
        )
        halts = await self._risk_repository.load_active_halts(
            "live",
            self._account_label,
        )
        unresolved = await self._order_repository.load_unresolved_orders()
        account_state = await _latest_account_state(
            self._sessions,
            self._account_label,
        )
        account_observed_at, position_symbols, unrealized, gross = (
            await self._account_position_view()
        )
        realized = await self._daily_realized_pnl(now)
        rules = await _load_trading_rules(self._sessions, {state.symbol})
        last_entries = await self._last_entry_times()
        unresolved_states = tuple(item.state for item in unresolved)
        gate_context = LiveGateContext(
            now=now,
            live_submit_enabled=True,
            account_label=self._account_label,
            strategy_name=self._strategy_name,
            strategy_config_hash=self._strategy_config_hash,
            git_commit_hash=self._git_commit_hash,
            database_migration_revision=self._migration_revision,
            required_lease_owner=self._lease_owner,
            requested_submit_policy=SubmitPolicy.LIVE_SUBMIT,
            active_lease=lease,
            risk_config=risk_config,
            approval=approval,
            account_state=account_state,
            active_halts=halts,
            unresolved_order_states=unresolved_states,
        )
        return LiveDaemonRuntimeContext(
            now=now,
            gate_context=gate_context,
            active_lease=lease,
            account_state=account_state,
            account_observed_at=account_observed_at,
            open_position_symbols=position_symbols,
            realized_pnl=realized,
            unrealized_pnl=unrealized,
            gross_exposure=gross,
            active_halts=halts,
            unresolved_order_states=unresolved_states,
            risk_config=risk_config,
            strategy_state=StrategyLiveState.ACTIVE,
            trading_rules=rules,
            last_entry_at_by_symbol=last_entries,
        )

    async def _account_position_view(
        self,
    ) -> tuple[datetime | None, frozenset[str], Decimal, Decimal]:
        async with self._sessions() as session:
            process_at = await session.scalar(
                select(ExecutionAccountProcessStateRow.occurred_at)
                .where(
                    ExecutionAccountProcessStateRow.environment == "live",
                    ExecutionAccountProcessStateRow.account_label
                    == self._account_label,
                )
                .order_by(ExecutionAccountProcessStateRow.occurred_at.desc())
                .limit(1)
            )
            rows = (
                await session.scalars(
                    select(AccountPositionSnapshotRow)
                    .where(
                        AccountPositionSnapshotRow.environment == "live",
                        AccountPositionSnapshotRow.account_label
                        == self._account_label,
                    )
                    .order_by(AccountPositionSnapshotRow.observed_at.desc())
                    .limit(500)
                )
            ).all()
        latest: dict[tuple[str, str], AccountPositionSnapshotRow] = {}
        for row in rows:
            latest.setdefault((row.symbol, row.position_side), row)
        active = [row for row in latest.values() if row.position_amt != 0]
        return (
            process_at,
            frozenset(row.symbol for row in active),
            sum((row.unrealized_pnl for row in active), start=Decimal("0")),
            sum((abs(row.notional) for row in active), start=Decimal("0")),
        )

    async def _daily_realized_pnl(self, now: datetime) -> Decimal:
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        async with self._sessions() as session:
            rows = (
                await session.scalars(
                    select(AccountFillEventRow).where(
                        AccountFillEventRow.environment == "live",
                        AccountFillEventRow.account_label == self._account_label,
                        AccountFillEventRow.trade_at >= day_start,
                    )
                )
            ).all()
        return sum((row.realized_pnl for row in rows), start=Decimal("0"))

    async def _last_entry_times(self) -> dict[str, datetime]:
        async with self._sessions() as session:
            rows = (
                await session.scalars(
                    select(ExchangeOrderRow)
                    .where(ExchangeOrderRow.state == ExchangeOrderState.FILLED.value)
                    .order_by(ExchangeOrderRow.updated_at.desc())
                    .limit(500)
                )
            ).all()
        entries: dict[str, datetime] = {}
        for row in rows:
            if not row.reduce_only:
                entries.setdefault(row.symbol, row.updated_at)
        return entries


async def poll_live_market_states(
    *,
    repository: PostgresRuntimeMarketStateRepository,
    environment: str,
    max_runtime_seconds: float,
    poll_interval_seconds: float,
    batch_size: int = 500,
) -> AsyncIterator[MarketState15s]:
    cursor = RuntimeStateCursor(
        bucket_start=datetime.now(tz=UTC),
        symbol="",
    )
    deadline = time.monotonic() + max_runtime_seconds
    while time.monotonic() < deadline:
        batch = await repository.load_after(
            environment=environment,
            cursor=cursor,
            limit=batch_size,
        )
        if not batch:
            await asyncio.sleep(poll_interval_seconds)
            continue
        for state in batch:
            yield state
            cursor = RuntimeStateCursor(
                bucket_start=state.bucket_start,
                symbol=state.symbol,
            )


def live_limits_from_approval(
    *,
    approval: LiveOperatorApproval,
    risk_config: RiskConfigSnapshot,
) -> tuple[Decimal, int, Decimal, Decimal]:
    return (
        min(approval.approved_notional_cap, risk_config.max_order_notional),
        min(approval.approved_max_open_positions, risk_config.max_open_positions),
        min(approval.approved_max_daily_loss, risk_config.max_daily_loss),
        risk_config.max_gross_notional,
    )
