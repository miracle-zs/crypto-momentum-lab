"""Compact historical normal-hold evidence and reclaim its unused revisions.

This is an explicitly destructive, one-time maintenance tool for deployments
without cold evidence storage.  It converts only the high-volume
``holding_position_no_exit`` traces to the same tamper-evident hot summary used
by the runtime.  It never compacts executed, exited, rejected, or exceptional
decisions.

After trace compaction, non-canonical market revisions referenced only by
summary traces are no longer required for replay.  The optional purge protects
all full traces, all dataset manifests, and canonical revisions before deleting
the remaining unreferenced rows.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from collections.abc import Sequence
from typing import Any

import asyncpg  # type: ignore[import-untyped]

from crypto_momentum_lab.config import resolve_database_url

_NORMAL_HOLD_OUTCOME = "holding_position_no_exit"
_SUMMARY_LEVEL = "summary"


def build_summary_payload(
    original_payload: dict[str, Any], market_refs: list[dict[str, str]]
) -> dict[str, Any]:
    """Build the exact compact representation written by the live runtime."""
    canonical = json.dumps(
        original_payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return {
        "evidence_level": _SUMMARY_LEVEL,
        "summary_schema_version": 1,
        "frame_digest": str(original_payload.get("frame_digest", "")),
        "input_hash": str(original_payload.get("input_hash", "")),
        "original_payload_sha256": hashlib.sha256(
            canonical.encode("utf-8")
        ).hexdigest(),
        "outcome": _NORMAL_HOLD_OUTCOME,
        "market_refs": market_refs,
    }


async def compact_normal_hold_evidence(
    connection: asyncpg.Connection[Any], *, batch_size: int, dry_run: bool
) -> int:
    """Compact every un-compacted normal hold, in bounded transactions."""
    if dry_run:
        count = await connection.fetchval(
            """
            SELECT count(*)
            FROM decision_traces
            WHERE rejection_reason = $1
              AND coalesce(trace_payload ->> 'evidence_level', '') <> $2
            """,
            _NORMAL_HOLD_OUTCOME,
            _SUMMARY_LEVEL,
        )
        return int(count or 0)

    compacted = 0
    while True:
        async with connection.transaction():
            trace_rows = await connection.fetch(
                """
                SELECT decision_id, trace_payload::text AS trace_payload,
                       evaluated_revision_ids::text AS revision_ids
                FROM decision_traces
                WHERE rejection_reason = $1
                  AND coalesce(trace_payload ->> 'evidence_level', '') <> $2
                ORDER BY created_at, decision_id
                LIMIT $3
                FOR UPDATE SKIP LOCKED
                """,
                _NORMAL_HOLD_OUTCOME,
                _SUMMARY_LEVEL,
                batch_size,
            )
            if not trace_rows:
                break

            revision_ids = {
                revision_id
                for row in trace_rows
                for revision_id in json.loads(str(row["revision_ids"]))
            }
            revision_rows = await connection.fetch(
                """
                SELECT revision_id, scope, symbol, interval, bucket_start,
                       bucket_end, content_hash, published_at, source_epoch,
                       visibility_mode, lineage::text AS lineage
                FROM market_revision_refs
                WHERE revision_id = ANY($1::text[])
                """,
                list(revision_ids),
            )
            refs_by_id = {
                str(row["revision_id"]): _summary_ref_from_row(row)
                for row in revision_rows
            }
            missing = sorted(revision_ids.difference(refs_by_id))
            if missing:
                raise RuntimeError(
                    "refusing to compact trace with missing market revisions: "
                    + ", ".join(missing[:10])
                )

            updates: list[tuple[str, str]] = []
            for row in trace_rows:
                original = json.loads(str(row["trace_payload"]))
                ordered_refs = [
                    refs_by_id[revision_id]
                    for revision_id in json.loads(str(row["revision_ids"]))
                ]
                summary = build_summary_payload(original, ordered_refs)
                updates.append(
                    (
                        str(row["decision_id"]),
                        json.dumps(summary, ensure_ascii=True, separators=(",", ":")),
                    )
                )
            await connection.executemany(
                """
                UPDATE decision_traces
                SET trace_payload = $2::jsonb
                WHERE decision_id = $1
                  AND rejection_reason = 'holding_position_no_exit'
                  AND coalesce(trace_payload ->> 'evidence_level', '') <> 'summary'
                """,
                updates,
            )
            compacted += len(updates)
        print(f"compacted normal-hold traces: {compacted}", flush=True)
    return compacted


def _summary_ref_from_row(row: asyncpg.Record) -> dict[str, str]:
    lineage = json.loads(str(row["lineage"]))
    observed_at = lineage.get("observed_at")
    return {
        "scope": str(row["scope"]),
        "symbol": str(row["symbol"]),
        "interval": str(row["interval"]),
        "bucket_start": row["bucket_start"].isoformat(),
        "bucket_end": row["bucket_end"].isoformat(),
        "revision_id": str(row["revision_id"]),
        "content_hash": str(row["content_hash"]),
        "published_at": row["published_at"].isoformat(),
        "source_epoch": str(row["source_epoch"]),
        "visibility_mode": str(row["visibility_mode"]),
        "observed_at": str(observed_at) if observed_at else "",
    }


async def purge_unreferenced_revisions(
    connection: asyncpg.Connection[Any], *, batch_size: int, dry_run: bool
) -> int:
    """Remove only non-canonical revisions not needed by full evidence."""
    await connection.execute(
        """
        CREATE TEMP TABLE protected_market_revision_ids (
            revision_id text PRIMARY KEY
        ) ON COMMIT PRESERVE ROWS
        """
    )
    await connection.execute(
        """
        INSERT INTO protected_market_revision_ids (revision_id)
        SELECT DISTINCT jsonb_array_elements_text(evaluated_revision_ids)
        FROM decision_traces
        WHERE coalesce(trace_payload ->> 'evidence_level', '') <> 'summary'
        """
    )
    await connection.execute(
        """
        INSERT INTO protected_market_revision_ids (revision_id)
        SELECT DISTINCT jsonb_array_elements_text(revision_ids)
        FROM dataset_manifests
        ON CONFLICT DO NOTHING
        """
    )

    if dry_run:
        return int(
            await connection.fetchval(
                """
                SELECT count(*)
                FROM market_revision_refs AS revisions
                LEFT JOIN protected_market_revision_ids AS protected
                  ON protected.revision_id = revisions.revision_id
                WHERE revisions.is_canonical = false
                  AND protected.revision_id IS NULL
                """
            )
            or 0
        )

    deleted = 0
    while True:
        result = await connection.execute(
            """
            WITH doomed AS (
                SELECT revisions.ctid
                FROM market_revision_refs AS revisions
                LEFT JOIN protected_market_revision_ids AS protected
                  ON protected.revision_id = revisions.revision_id
                WHERE revisions.is_canonical = false
                  AND protected.revision_id IS NULL
                LIMIT $1
            )
            DELETE FROM market_revision_refs AS revisions
            USING doomed
            WHERE revisions.ctid = doomed.ctid
            """,
            batch_size,
        )
        batch = int(result.rsplit(" ", 1)[-1])
        deleted += batch
        if batch == 0:
            break
        print(f"purged unreferenced market revisions: {deleted}", flush=True)
    return deleted


def _database_url(value: str) -> str:
    return value.replace("postgresql+asyncpg://", "postgresql://", 1)


async def _run(args: argparse.Namespace) -> int:
    database_url = resolve_database_url(
        args.database_url,
        "CML_OBSERVABILITY_DATABASE_URL",
        "CML_DATABASE_URL",
    )
    if not database_url:
        raise ValueError("database URL must be provided or configured")
    connection = await asyncpg.connect(_database_url(database_url))
    try:
        compacted = await compact_normal_hold_evidence(
            connection, batch_size=args.batch_size, dry_run=args.dry_run
        )
        print(
            f"{'would compact' if args.dry_run else 'compacted'} "
            f"{compacted} normal-hold traces"
        )
        if args.purge_unreferenced_revisions:
            purged = await purge_unreferenced_revisions(
                connection, batch_size=args.batch_size, dry_run=args.dry_run
            )
            print(
                f"{'would consider' if args.dry_run else 'purged'} "
                f"{purged} non-canonical market revisions"
            )
    finally:
        await connection.close()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="irreversibly replace complete normal-hold evidence with summaries",
    )
    parser.add_argument("--purge-unreferenced-revisions", action="store_true")
    args = parser.parse_args(argv)
    if args.batch_size < 1 or args.batch_size > 1_000:
        parser.error("--batch-size must be between 1 and 1000")
    if not args.dry_run and not args.apply:
        parser.error(
            "destructive mode requires --apply; use --dry-run to inspect only"
        )
    if args.purge_unreferenced_revisions and not args.apply and not args.dry_run:
        parser.error("--purge-unreferenced-revisions requires --apply")
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
