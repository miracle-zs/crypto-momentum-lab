"""Read-only replay of the production checkpoint/snapshot recovery incident.

Run inside a configured application container:
    docker exec -i <execution-account-container> python - < this_file.py

No exchange requests or writes. Output includes scoped domain facts only.
"""

import asyncio
import json
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.apps.live_rollout.main import _execution_database_url
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
)
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_execution_database_engine,
)


async def main():
    engine = create_execution_database_engine(
        _execution_database_url(None), pool_size=1
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    store = PostgresAccountJournalStore()
    checkpoint_count = 0
    blocked_count = 0
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout='5000'"))
            heads = (
                (
                    await session.execute(
                        text(
                            "SELECT environment,account_label,symbol,position_side,"
                            "stream_id,stream_epoch FROM execution_book_heads "
                            "WHERE environment='live' "
                            "AND account_label IN ('primary','account-2') "
                            "AND symbol IN ('CAPUSDT','FLUIDUSDT',"
                            "'MAGMAUSDT','PUMPBTCUSDT') "
                            "ORDER BY account_label,symbol"
                        )
                    )
                )
                .mappings()
                .all()
            )
            for h in heads:
                scope = AccountFactStreamScope(
                    environment=h["environment"],
                    account_label=h["account_label"],
                    symbol=h["symbol"],
                    position_side=FuturesPositionSide(h["position_side"]),
                    stream_id=h["stream_id"],
                    stream_epoch=h["stream_epoch"],
                )
                cut = await store.load_recovery_in_session(
                    session, scope=scope, as_of=datetime.now(UTC)
                )
                projection = PositionLedger(cut.facts.position_key).project(cut.facts)
                if cut.checkpoint is not None:
                    checkpoint_count += 1
                    blocked_count += int(not projection.is_comparable)
                print(
                    json.dumps(
                        {
                            "account": h["account_label"],
                            "symbol": h["symbol"],
                            "position_side": h["position_side"],
                            "stream_epoch": scope.stream_epoch,
                            "checkpoint_cut": None
                            if cut.checkpoint is None
                            else cut.checkpoint.event_cut.isoformat(),
                            "revision": cut.revision,
                            "conflicts": [
                                {
                                    "kind": c.event_kind,
                                    "id": c.event_id,
                                    "details": c.details,
                                }
                                for c in cut.facts.fact_conflicts
                            ],
                            "issues": cut.integrity_issues,
                            "health": projection.health_status.value,
                            "comparable": projection.is_comparable,
                            "quantity": str(projection.total_active_quantity),
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
    finally:
        await engine.dispose()
    if not checkpoint_count:
        raise RuntimeError("incident replay found no checkpoint samples")
    return int(blocked_count > 0)


raise SystemExit(asyncio.run(main()))
