"""Dry-run by default; archive and prune only durably retired execution epochs."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.config import resolve_database_url
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
)
from crypto_momentum_lab.persistence.postgres.execution_receipt_retention import (
    PostgresExecutionReceiptRetention,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionRetiredStreamRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_maintenance_database_engine,
)


async def run(args: argparse.Namespace) -> None:
    if args.minimum_age_hours < 24 or not 1 <= args.batch_size <= 1000:
        raise ValueError("minimum age >=24 hours and batch size 1..1000 required")
    if not 1 <= args.max_scopes <= 1000:
        raise ValueError("max scopes must be 1..1000")
    if not 1 <= args.max_runtime_seconds <= 600:
        raise ValueError("runtime budget must be 1..600 seconds")
    database_url = resolve_database_url(None, "CML_DATABASE_URL")
    if database_url is None:
        raise ValueError("CML_DATABASE_URL is required")
    engine = create_maintenance_database_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    before = datetime.now(UTC) - timedelta(hours=args.minimum_age_hours)
    retention = PostgresExecutionReceiptRetention(factory, args.archive_root)
    deadline = time.monotonic() + args.max_runtime_seconds
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET LOCAL statement_timeout = '5s'"))
            rows = list(
                await session.scalars(
                    select(ExecutionRetiredStreamRow)
                    .where(
                        ExecutionRetiredStreamRow.receipts_archived_at.is_(None),
                        ExecutionRetiredStreamRow.retired_at < before,
                    )
                    .order_by(ExecutionRetiredStreamRow.retired_at)
                    .limit(args.max_scopes)
                )
            )
        deleted = 0
        checked = 0
        for row in rows:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                print(
                    json.dumps({"event": "runtime_limit_reached", "deleted": deleted}),
                    flush=True,
                )
                return
            scope = AccountFactStreamScope(
                row.environment,
                row.account_label,
                row.symbol,
                FuturesPositionSide(row.position_side),
                row.stream_id,
                row.stream_epoch,
            )
            try:
                result = await asyncio.wait_for(
                    retention.prune_retired_stream(
                        scope,
                        before=before,
                        batch_size=args.batch_size,
                        apply=args.apply,
                    ),
                    timeout=remaining,
                )
            except TimeoutError:
                print(
                    json.dumps({"event": "runtime_limit_reached", "deleted": deleted}),
                    flush=True,
                )
                return
            deleted += int(result["deleted"])
            checked += 1
            print(json.dumps({"scope": scope.canonical_id, **result}), flush=True)
            await asyncio.sleep(0.1)
        print(
            json.dumps(
                {
                    "event": "round_completed",
                    "mode": "applied" if args.apply else "dry_run",
                    "scopes_checked": checked,
                    "deleted": deleted,
                }
            ),
            flush=True,
        )
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--minimum-age-hours", type=int, default=72)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-scopes", type=int, default=50)
    parser.add_argument("--max-runtime-seconds", type=float, default=45)
    parser.add_argument("--archive-root", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
