"""Lossless bounded backfill and collection of shared decision state evidence."""

import argparse
import asyncio
import json
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, text, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.config import resolve_database_url
from crypto_momentum_lab.persistence.postgres.decision_trace_repository import (
    _load_policy_evidence,
)
from crypto_momentum_lab.persistence.postgres.decision_trace_storage import (
    share_policy_states,
)
from crypto_momentum_lab.persistence.postgres.models import (
    DecisionPolicyEvidenceRow,
    DecisionTraceRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


async def backfill_batch(session, batch_size: int, after=None) -> tuple[int, int]:
    """Scan a bounded indexed page and rewrite only its storage encoding."""
    await session.execute(text("SET LOCAL lock_timeout='5s'"))
    await session.execute(text("SET LOCAL statement_timeout='30s'"))
    query = select(DecisionTraceRow)
    if after is not None:
        query = query.where(
            tuple_(DecisionTraceRow.created_at, DecisionTraceRow.decision_id) > after
        )
    rows = (
        (
            await session.execute(
                query.order_by(
                    DecisionTraceRow.created_at, DecisionTraceRow.decision_id
                )
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        )
        .scalars()
        .all()
    )
    if rows:
        session.info["backfill_cursor"] = (rows[-1].created_at, rows[-1].decision_id)
    updates = []
    states = {}
    for row in rows:
        payload, shared = share_policy_states(row.trace_payload)
        if shared:
            updates.append((row, payload))
            states.update(shared)
    if states:
        existing = await _load_policy_evidence(session, set(states))
        missing = [
            {
                "state_digest": digest,
                "state_payload": state,
                "created_at": datetime.now(UTC),
            }
            for digest, state in states.items()
            if digest not in existing
        ]
        if missing:
            await session.execute(
                insert(DecisionPolicyEvidenceRow)
                .values(missing)
                .on_conflict_do_nothing(index_elements=["state_digest"])
            )
            existing = await _load_policy_evidence(session, set(states))
        if existing != states:
            raise ValueError("Immutable shared state conflict")
        for row, payload in updates:
            row.trace_payload = payload
    return len(rows), len(updates)


async def collect_batch(session, batch_size: int, retention_days: int = 7) -> int:
    """Lock orphan candidates before rechecking references in a fresh snapshot."""
    await session.execute(text("SET LOCAL lock_timeout='5s'"))
    await session.execute(text("SET LOCAL statement_timeout='30s'"))
    orphan = """NOT EXISTS (SELECT 1 FROM decision_traces t
        WHERE t.trace_payload ? 'policy_state_refs' AND
          (t.trace_payload->'policy_state_refs'->>'prior'=decision_policy_evidence.state_digest
           OR t.trace_payload->'policy_state_refs'->>'next'=decision_policy_evidence.state_digest))"""
    rows = (
        (
            await session.execute(
                select(DecisionPolicyEvidenceRow.state_digest)
                .where(
                    DecisionPolicyEvidenceRow.created_at
                    < datetime.now(UTC) - timedelta(days=retention_days),
                    text(orphan),
                )
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        )
        .scalars()
        .all()
    )
    if not rows:
        return 0
    result = await session.execute(
        text(
            "DELETE FROM decision_policy_evidence WHERE state_digest=ANY(:ids) AND "
            + orphan
        ),
        {"ids": rows},
    )
    return result.rowcount


async def run(args) -> None:
    url = resolve_database_url(
        None, "CML_OBSERVABILITY_DATABASE_URL", "CML_DATABASE_URL"
    )
    engine = create_async_database_engine(url, pool_size=1, max_overflow=0)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        cursor = None
        for sequence in range(args.max_batches):
            async with factory() as session, session.begin():
                if args.collect:
                    count = await collect_batch(session, args.batch_size)
                    scanned = count
                else:
                    scanned, count = await backfill_batch(
                        session, args.batch_size, cursor
                    )
                    cursor = session.info.get("backfill_cursor", cursor)
            print(
                json.dumps(
                    {
                        "batch": sequence,
                        "scanned": scanned,
                        "rows": count,
                        "collect": args.collect,
                    }
                ),
                flush=True,
            )
            if scanned == 0:
                break
            await asyncio.sleep(0.2)
    finally:
        await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collect", action="store_true")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--max-batches", type=int, default=20)
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 500 or not 1 <= args.max_batches <= 1000:
        parser.error("batch size must be 1..500; max batches must be 1..1000")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
