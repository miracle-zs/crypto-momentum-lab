"""Read-only divergence report: it must never write and must name evidence."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.persistence.postgres.models import (
    AccountPositionSnapshotRow,
    ExchangeFillRow,
    ExchangeOrderRow,
    OrderIntentExecutionRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from crypto_momentum_lab.tools.diagnose_position_divergence import (
    collect_scope_evidence,
    render,
)

NOW = datetime(2026, 10, 12, 18, 0, tzinfo=UTC)
ACCOUNT = "diag-account"
SYMBOL = "WUSDT"


@pytest.fixture
async def sessions(
    async_database_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            await session.execute(
                delete(ExchangeFillRow).where(
                    ExchangeFillRow.client_order_id.like("diag-%")
                )
            )
            await session.execute(
                delete(ExchangeOrderRow).where(
                    ExchangeOrderRow.client_order_id.like("diag-%")
                )
            )
            await session.execute(
                delete(OrderIntentExecutionRow).where(
                    OrderIntentExecutionRow.intent_id.like("intent-diag-%")
                )
            )
            await session.execute(
                delete(AccountPositionSnapshotRow).where(
                    AccountPositionSnapshotRow.account_label == ACCOUNT
                )
            )
    yield factory
    async with factory() as session:
        async with session.begin():
            await session.execute(
                delete(ExchangeFillRow).where(
                    ExchangeFillRow.client_order_id.like("diag-%")
                )
            )
            await session.execute(
                delete(ExchangeOrderRow).where(
                    ExchangeOrderRow.client_order_id.like("diag-%")
                )
            )
            await session.execute(
                delete(OrderIntentExecutionRow).where(
                    OrderIntentExecutionRow.intent_id.like("intent-diag-%")
                )
            )
            await session.execute(
                delete(AccountPositionSnapshotRow).where(
                    AccountPositionSnapshotRow.account_label == ACCOUNT
                )
            )
    await engine.dispose()


async def _seed_order(
    factory: async_sessionmaker[AsyncSession],
    *,
    client_order_id: str,
    executed_quantity: Decimal,
    filled_quantities: tuple[Decimal, ...],
) -> None:
    async with factory() as session:
        async with session.begin():
            session.add(
                OrderIntentExecutionRow(
                    intent_id=f"intent-{client_order_id}",
                    candidate_id=f"candidate-{client_order_id}",
                    run_id="diag-run",
                    risk_evaluation_id=f"risk-{client_order_id}",
                    strategy_name="diag",
                    symbol=SYMBOL,
                    state="approved",
                    approved_at=NOW,
                    details={},
                )
            )
            # The order FK is DB-level only, so flush the intent first.
            await session.flush()
            session.add(
                ExchangeOrderRow(
                    client_order_id=client_order_id,
                    intent_id=f"intent-{client_order_id}",
                    run_id="diag-run",
                    exchange_order_id=f"ex-{client_order_id}",
                    symbol=SYMBOL,
                    side="BUY",
                    order_type="MARKET",
                    quantity=Decimal("10"),
                    price=None,
                    time_in_force=None,
                    expires_at=None,
                    executed_quantity=executed_quantity,
                    reduce_only=False,
                    position_side="BOTH",
                    state="filled",
                    created_at=NOW,
                    updated_at=NOW,
                )
            )
            # The fill FK is DB-level only, so flush the order before its fills.
            await session.flush()
            for index, quantity in enumerate(filled_quantities):
                session.add(
                    ExchangeFillRow(
                        fill_id=f"{client_order_id}-fill-{index}",
                        client_order_id=client_order_id,
                        exchange_trade_id=f"trade-{client_order_id}-{index}",
                        price=Decimal("100"),
                        quantity=quantity,
                        fee=Decimal("0"),
                        fee_asset="USDT",
                        filled_at=NOW + timedelta(seconds=index),
                        details={},
                    )
                )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_divergence_report_names_orders_versus_fills(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await _seed_order(
        sessions,
        client_order_id="diag-divergent",
        executed_quantity=Decimal("5"),
        filled_quantities=(Decimal("2"), Decimal("1")),
    )

    evidence = await collect_scope_evidence(
        sessions, account_label=ACCOUNT, symbol=SYMBOL, position_side="BOTH"
    )
    verdicts = evidence.verdicts()

    # The missing structural proof is named, and so is the real disagreement.
    assert "no_execution_head_for_scope" in verdicts
    assert "orders_vs_fills_mismatch(orders=5, fills=3)" in verdicts
    assert "orders_vs_fills_mismatch" in render(evidence)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_divergence_report_is_quiet_when_evidence_agrees(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await _seed_order(
        sessions,
        client_order_id="diag-agreeing",
        executed_quantity=Decimal("3"),
        filled_quantities=(Decimal("2"), Decimal("1")),
    )

    evidence = await collect_scope_evidence(
        sessions, account_label=ACCOUNT, symbol=SYMBOL, position_side="BOTH"
    )
    verdicts = evidence.verdicts()

    assert verdicts == ("no_execution_head_for_scope",)
    assert evidence.order_executed_total == evidence.fill_quantity_total
    assert "findings:" in render(evidence)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_divergence_report_never_writes(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """The tool is read-only: facts must be identical after a report."""
    await _seed_order(
        sessions,
        client_order_id="diag-readonly",
        executed_quantity=Decimal("1"),
        filled_quantities=(Decimal("1"),),
    )
    async with sessions() as session:
        async with session.begin():
            session.add(
                AccountPositionSnapshotRow(
                    snapshot_id=uuid4(),
                    environment="live",
                    account_label=ACCOUNT,
                    symbol=SYMBOL,
                    position_side="BOTH",
                    position_amt=Decimal("1"),
                    entry_price=Decimal("100"),
                    mark_price=Decimal("100"),
                    unrealized_pnl=Decimal("0"),
                    notional=Decimal("100"),
                    leverage=1,
                    margin_type="cross",
                    observed_at=NOW,
                    raw_payload={},
                )
            )

    before = await _fingerprint(sessions, "diag-readonly")
    await collect_scope_evidence(
        sessions, account_label=ACCOUNT, symbol=SYMBOL, position_side="BOTH"
    )
    after = await _fingerprint(sessions, "diag-readonly")

    assert before == after


async def _fingerprint(
    sessions: async_sessionmaker[AsyncSession], client_order_id: str
) -> tuple[object, ...]:
    """Read-only snapshot of the durable facts the report must not change."""
    async with sessions() as session:
        order = (
            await session.execute(
                select(
                    ExchangeOrderRow.executed_quantity,
                    ExchangeOrderRow.updated_at,
                    ExchangeOrderRow.state,
                ).where(ExchangeOrderRow.client_order_id == client_order_id)
            )
        ).one_or_none()
        fills = await session.scalar(
            select(func.count())
            .select_from(ExchangeFillRow)
            .where(ExchangeFillRow.client_order_id == client_order_id)
        )
        return (tuple(order or ()), fills)
