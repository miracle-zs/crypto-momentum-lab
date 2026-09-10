from decimal import Decimal

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.risk import RiskConfigSnapshot
from crypto_momentum_lab.execution_account.orders.quantization import (
    SymbolTradingRules,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ContractMetadataRow,
    ExecutionAccountProcessStateRow,
    RiskConfigSnapshotRow,
)


async def load_latest_account_state(
    factory: async_sessionmaker[AsyncSession],
    account_label: str,
) -> ExecutionAccountStatus:
    async with factory() as session:
        state = await session.scalar(
            select(ExecutionAccountProcessStateRow.state)
            .where(
                ExecutionAccountProcessStateRow.environment == "live",
                ExecutionAccountProcessStateRow.account_label == account_label,
            )
            .order_by(ExecutionAccountProcessStateRow.occurred_at.desc())
            .limit(1)
        )
    return ExecutionAccountStatus(state) if state else ExecutionAccountStatus.DEGRADED


async def load_latest_risk_config(
    factory: async_sessionmaker[AsyncSession],
    account_label: str,
) -> RiskConfigSnapshot:
    async with factory() as session:
        row = await session.scalar(
            select(RiskConfigSnapshotRow)
            .where(
                RiskConfigSnapshotRow.environment == "live",
                RiskConfigSnapshotRow.account_label == account_label,
            )
            .order_by(RiskConfigSnapshotRow.created_at.desc())
            .limit(1)
        )
    if row is None:
        raise RuntimeError("no persisted risk config for account")
    return RiskConfigSnapshot(
        environment=row.environment,
        account_label=row.account_label,
        max_order_notional=row.max_order_notional,
        max_gross_notional=row.max_gross_notional,
        max_daily_loss=row.max_daily_loss,
        max_open_positions=row.max_open_positions,
        max_market_state_age_seconds=float(row.max_market_state_age_seconds),
        max_account_state_age_seconds=float(row.max_account_state_age_seconds),
        allow_reduce_only_while_draining=row.allow_reduce_only_while_draining,
        created_at=row.created_at,
    )


async def load_trading_rules(
    factory: async_sessionmaker[AsyncSession],
    symbols: set[str] | None,
) -> dict[str, SymbolTradingRules]:
    async with factory() as session:
        latest_query = select(
            ContractMetadataRow.symbol,
            func.max(ContractMetadataRow.effective_at).label("effective_at"),
        ).group_by(ContractMetadataRow.symbol)
        if symbols is not None:
            latest_query = latest_query.where(
                ContractMetadataRow.symbol.in_(symbols)
            )
        latest_effective_at = latest_query.subquery()
        statement = (
            select(ContractMetadataRow)
            .join(
                latest_effective_at,
                and_(
                    ContractMetadataRow.symbol == latest_effective_at.c.symbol,
                    ContractMetadataRow.effective_at
                    == latest_effective_at.c.effective_at,
                ),
            )
            .order_by(ContractMetadataRow.symbol)
        )
        rows = (await session.scalars(statement)).all()
    rules: dict[str, SymbolTradingRules] = {}
    for row in rows:
        if row.symbol in rules:
            continue
        parsed = _rules_from_exchange_info(row.symbol, row.raw_payload)
        if parsed is not None:
            rules[row.symbol] = parsed
    return rules


def _rules_from_exchange_info(
    symbol: str,
    payload: dict[str, object],
) -> SymbolTradingRules | None:
    filters_value = payload.get("filters")
    if not isinstance(filters_value, list):
        return None
    filters = {
        str(item.get("filterType")): item
        for item in filters_value
        if isinstance(item, dict)
    }
    price_filter = filters.get("PRICE_FILTER")
    lot_filter = filters.get("MARKET_LOT_SIZE") or filters.get("LOT_SIZE")
    notional_filter = filters.get("MIN_NOTIONAL")
    if not price_filter or not lot_filter or not notional_filter:
        return None
    return SymbolTradingRules(
        symbol=symbol,
        tick_size=Decimal(str(price_filter["tickSize"])),
        step_size=Decimal(str(lot_filter["stepSize"])),
        min_quantity=Decimal(str(lot_filter["minQty"])),
        max_quantity=Decimal(str(lot_filter["maxQty"])),
        min_notional=Decimal(
            str(notional_filter.get("notional", notional_filter.get("minNotional")))
        ),
    )
