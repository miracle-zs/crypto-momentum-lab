"""Audit and record legacy exchange-order identity collisions.

Older live runs could reuse one client order ID for multiple exchange orders.
The exchange-order table can only retain one snapshot for that client ID, but
the immutable event and account-fill ledgers still contain enough evidence for
many collisions to be reconstructed.  This module deliberately does not
rewrite either ledger.  It records an idempotent reconciliation event so the
repair is reviewable and lets runtime reconstruction use the original evidence.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillEventRow,
    ExchangeOrderEventRow,
    ExchangeOrderRow,
    ExecutionReconciliationEventRow,
)


def _decimal_or_zero(value: object) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError):
        return Decimal("0")


def _event_executed_quantity(event: ExchangeOrderEventRow) -> Decimal:
    details = event.details
    if not isinstance(details, Mapping):
        return Decimal("0")
    quantity = _decimal_or_zero(details.get("executed_quantity"))
    return max(Decimal("0"), quantity)


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _attempt_report(
    exchange_order_id: str,
    events: Sequence[ExchangeOrderEventRow],
    fill_quantities: Mapping[str, Decimal],
) -> dict[str, object]:
    event_quantity = max(
        (_event_executed_quantity(event) for event in events),
        default=Decimal("0"),
    )
    fill_quantity = max(
        Decimal("0"),
        _decimal_or_zero(fill_quantities.get(exchange_order_id)),
    )
    return {
        "exchange_order_id": exchange_order_id,
        "event_count": len(events),
        "first_event_at": _iso(
            min((event.occurred_at for event in events), default=None)
        ),
        "last_event_at": _iso(
            max((event.occurred_at for event in events), default=None)
        ),
        "event_executed_quantity": str(event_quantity),
        "account_fill_quantity": str(fill_quantity),
        "reconstructed_quantity": str(max(event_quantity, fill_quantity)),
    }


def build_legacy_identity_report(
    orders: Sequence[ExchangeOrderRow],
    events: Sequence[ExchangeOrderEventRow],
    fills: Sequence[AccountFillEventRow],
    *,
    run_id: str,
    account_label: str,
    environment: str,
) -> list[dict[str, object]]:
    """Build a JSON-safe report without changing database state."""

    events_by_client: dict[str, list[ExchangeOrderEventRow]] = defaultdict(list)
    for event in events:
        if event.exchange_order_id:
            events_by_client[event.client_order_id].append(event)
    fill_quantities: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for fill in fills:
        fill_quantities[fill.order_id] += max(Decimal("0"), fill.quantity)
    order_by_client = {order.client_order_id: order for order in orders}
    report: list[dict[str, object]] = []
    for client_order_id, client_events in sorted(events_by_client.items()):
        exchange_order_ids = tuple(
            sorted(
                {
                    event.exchange_order_id
                    for event in client_events
                    if event.exchange_order_id
                }
            )
        )
        if len(exchange_order_ids) < 2:
            continue
        attempts = [
            _attempt_report(
                exchange_order_id,
                [
                    event
                    for event in client_events
                    if event.exchange_order_id == exchange_order_id
                ],
                fill_quantities,
            )
            for exchange_order_id in exchange_order_ids
        ]
        row = order_by_client.get(client_order_id)
        reconstructible = all(
            _decimal_or_zero(attempt["reconstructed_quantity"]) > 0
            for attempt in attempts
        )
        report.append(
            {
                "run_id": run_id,
                "environment": environment,
                "account_label": account_label,
                "client_order_id": client_order_id,
                "symbol": None if row is None else row.symbol,
                "side": None if row is None else row.side,
                "reduce_only": None if row is None else row.reduce_only,
                "row_exchange_order_id": (
                    None if row is None else row.exchange_order_id
                ),
                "row_quantity": (
                    None if row is None else str(row.quantity)
                ),
                "row_executed_quantity": (
                    None if row is None else str(row.executed_quantity)
                ),
                "exchange_order_ids": list(exchange_order_ids),
                "attempts": attempts,
                "reconstructible": reconstructible,
            }
        )
    return report


async def load_legacy_identity_report(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    run_id: str,
    account_label: str,
    environment: str,
) -> list[dict[str, object]]:
    async with session_factory() as session:
        ambiguous_client_order_ids = tuple(
            (
                await session.scalars(
                    select(ExchangeOrderEventRow.client_order_id)
                    .join(
                        ExchangeOrderRow,
                        ExchangeOrderRow.client_order_id
                        == ExchangeOrderEventRow.client_order_id,
                    )
                    .where(
                        ExchangeOrderRow.run_id == run_id,
                        ExchangeOrderEventRow.exchange_order_id.is_not(None),
                    )
                    .group_by(ExchangeOrderEventRow.client_order_id)
                    .having(
                        func.count(
                            func.distinct(
                                ExchangeOrderEventRow.exchange_order_id
                            )
                        )
                        > 1
                    )
                )
            ).all()
        )
        if not ambiguous_client_order_ids:
            return []
        orders = (
            await session.scalars(
                select(ExchangeOrderRow).where(
                    ExchangeOrderRow.run_id == run_id,
                    ExchangeOrderRow.client_order_id.in_(
                        ambiguous_client_order_ids
                    ),
                )
            )
        ).all()
        client_order_ids = tuple(ambiguous_client_order_ids)
        events = (
            await session.scalars(
                select(ExchangeOrderEventRow).where(
                    ExchangeOrderEventRow.client_order_id.in_(client_order_ids)
                )
            )
        ).all()
        exchange_order_ids = tuple(
            sorted(
                {
                    event.exchange_order_id
                    for event in events
                    if event.exchange_order_id
                }
            )
        )
        fills: Sequence[AccountFillEventRow] = ()
        if exchange_order_ids:
            fills = (
                await session.scalars(
                    select(AccountFillEventRow).where(
                        AccountFillEventRow.environment == environment,
                        AccountFillEventRow.account_label == account_label,
                        AccountFillEventRow.order_id.in_(exchange_order_ids),
                    )
                )
            ).all()
    return build_legacy_identity_report(
        orders,
        events,
        fills,
        run_id=run_id,
        account_label=account_label,
        environment=environment,
    )


def _reconciliation_event_id(
    *,
    run_id: str,
    client_order_id: str,
    account_label: str,
) -> str:
    digest = hashlib.sha256(
        f"{run_id}:{account_label}:{client_order_id}".encode()
    ).hexdigest()
    return f"legacy-order-identity-repair:{digest}"


async def apply_legacy_identity_report(
    session_factory: async_sessionmaker[AsyncSession],
    report: Sequence[dict[str, object]],
) -> int:
    """Persist one idempotent reconciliation marker per collision."""

    inserted = 0
    async with session_factory() as session:
        async with session.begin():
            for item in report:
                run_id = str(item["run_id"])
                account_label = str(item["account_label"])
                client_order_id = str(item["client_order_id"])
                event_id = _reconciliation_event_id(
                    run_id=run_id,
                    client_order_id=client_order_id,
                    account_label=account_label,
                )
                existing = await session.scalar(
                    select(ExecutionReconciliationEventRow).where(
                        ExecutionReconciliationEventRow.reconciliation_event_id
                        == event_id
                    )
                )
                if existing is not None:
                    continue
                outcome = (
                    "legacy_order_identity_reconciled"
                    if item["reconstructible"]
                    else "legacy_order_identity_unresolved"
                )
                session.add(
                    ExecutionReconciliationEventRow(
                        reconciliation_event_id=event_id,
                        client_order_id=client_order_id,
                        outcome=outcome,
                        occurred_at=datetime.now(UTC),
                        details=dict(item),
                    )
                )
                inserted += 1
    return inserted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--account-label", default="primary")
    parser.add_argument("--environment", default="live")
    parser.add_argument(
        "--database-url",
        default=None,
        help="Async PostgreSQL URL; defaults to CML_DATABASE_URL.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist idempotent reconciliation markers after printing the report.",
    )
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> None:
    database_url = args.database_url or os.environ.get("CML_DATABASE_URL")
    if not database_url:
        raise RuntimeError("CML_DATABASE_URL or --database-url is required")
    engine = create_async_engine(database_url, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        report = await load_legacy_identity_report(
            session_factory,
            run_id=args.run_id,
            account_label=args.account_label,
            environment=args.environment,
        )
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        if args.apply:
            inserted = await apply_legacy_identity_report(
                session_factory,
                report,
            )
            print(
                json.dumps(
                    {
                        "applied": True,
                        "reconciliation_events_inserted": inserted,
                    },
                    ensure_ascii=False,
                )
            )
        else:
            print("DRY RUN: no database rows changed; rerun with --apply")
    finally:
        await engine.dispose()


def main() -> None:
    asyncio.run(_run(parse_args()))


if __name__ == "__main__":
    main()
