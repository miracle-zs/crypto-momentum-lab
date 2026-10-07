"""Bounded online retention for superseded position-recovery state.

The newest checkpoint in each stream scope and every checkpoint currently
bound by an execution head are retained. Older state events are removed only
when a retained checkpoint covers them. Immutable fills, exit boundaries,
conflicts, and integrity facts are never selected for deletion.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg  # type: ignore[import-untyped]

from crypto_momentum_lab.config import resolve_database_url

_COMPACTABLE_KINDS = (
    "facts_state",
    "snapshot",
    "coverage",
    "fill_load_provenance",
)
_ADVISORY_LOCK_NAME = "cml:prune_position_recovery_history"


async def _prepare_protected_checkpoints(
    connection: asyncpg.Connection[Any],
) -> None:
    """Snapshot each scope's newest checkpoint and all execution-head bindings."""
    await connection.execute(
        """
        CREATE TEMP TABLE protected_recovery_checkpoints (
            environment text NOT NULL,
            account_label text NOT NULL,
            symbol text NOT NULL,
            position_side text NOT NULL,
            stream_id text NOT NULL,
            stream_epoch text NOT NULL,
            checkpoint_id text NOT NULL,
            PRIMARY KEY (
                environment, account_label, symbol, position_side,
                stream_id, stream_epoch, checkpoint_id
            )
        ) ON COMMIT PRESERVE ROWS
        """
    )
    await connection.execute(
        """
        INSERT INTO protected_recovery_checkpoints
        SELECT DISTINCT ON (
            environment, account_label, symbol, position_side,
            stream_id, stream_epoch
        )
            environment, account_label, symbol, position_side,
            stream_id, stream_epoch, checkpoint_id
        FROM position_recovery_checkpoints
        ORDER BY
            environment, account_label, symbol, position_side,
            stream_id, stream_epoch, event_cut DESC, source_revision DESC,
            recorded_at DESC, checkpoint_id DESC
        """
    )
    await connection.execute(
        """
        INSERT INTO protected_recovery_checkpoints (
            environment, account_label, symbol, position_side,
            stream_id, stream_epoch, checkpoint_id
        )
        SELECT
            heads.environment, heads.account_label, heads.symbol,
            heads.position_side, heads.stream_id, heads.stream_epoch,
            heads.state_payload -> 'recovery_checkpoint' ->> 'checkpoint_id'
        FROM execution_book_heads AS heads
        WHERE heads.state_payload -> 'recovery_checkpoint' ->> 'checkpoint_id'
              IS NOT NULL
        ON CONFLICT DO NOTHING
        """
    )


async def _materialize_checkpoint_candidates(
    connection: asyncpg.Connection[Any], *, cutoff: datetime, limit: int
) -> int:
    await connection.execute(
        """
        CREATE TEMP TABLE position_recovery_checkpoint_candidates (
            environment text NOT NULL,
            account_label text NOT NULL,
            symbol text NOT NULL,
            position_side text NOT NULL,
            stream_id text NOT NULL,
            stream_epoch text NOT NULL,
            checkpoint_id text NOT NULL,
            PRIMARY KEY (
                environment, account_label, symbol, position_side,
                stream_id, stream_epoch, checkpoint_id
            )
        ) ON COMMIT PRESERVE ROWS
        """
    )
    await connection.execute(
        """
        INSERT INTO position_recovery_checkpoint_candidates
        SELECT
            checkpoints.environment, checkpoints.account_label,
            checkpoints.symbol, checkpoints.position_side,
            checkpoints.stream_id, checkpoints.stream_epoch,
            checkpoints.checkpoint_id
        FROM position_recovery_checkpoints AS checkpoints
        LEFT JOIN protected_recovery_checkpoints AS protected
          ON protected.environment = checkpoints.environment
         AND protected.account_label = checkpoints.account_label
         AND protected.symbol = checkpoints.symbol
         AND protected.position_side = checkpoints.position_side
         AND protected.stream_id = checkpoints.stream_id
         AND protected.stream_epoch = checkpoints.stream_epoch
         AND protected.checkpoint_id = checkpoints.checkpoint_id
        WHERE checkpoints.recorded_at < $1
          AND protected.checkpoint_id IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM execution_book_heads AS heads
              WHERE heads.environment = checkpoints.environment
                AND heads.account_label = checkpoints.account_label
                AND heads.symbol = checkpoints.symbol
                AND heads.position_side = checkpoints.position_side
                AND heads.stream_id = checkpoints.stream_id
                AND heads.stream_epoch = checkpoints.stream_epoch
                AND heads.state_payload -> 'recovery_checkpoint' ->>
                    'checkpoint_id' = checkpoints.checkpoint_id
          )
        ORDER BY checkpoints.recorded_at, checkpoints.checkpoint_id
        LIMIT $2
        """,
        cutoff,
        limit,
    )
    return int(
        await connection.fetchval(
            "SELECT count(*) FROM position_recovery_checkpoint_candidates"
        )
        or 0
    )


async def _delete_checkpoint_batches(
    connection: asyncpg.Connection[Any], *, batch_size: int
) -> int:
    deleted = 0
    while True:
        batch = int(
            await connection.fetchval(
                """
                WITH doomed AS (
                    SELECT candidates.environment, candidates.account_label,
                           candidates.symbol, candidates.position_side,
                           candidates.stream_id, candidates.stream_epoch,
                           candidates.checkpoint_id
                    FROM position_recovery_checkpoint_candidates AS candidates
                    ORDER BY candidates.checkpoint_id
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                ), removed AS (
                    DELETE FROM position_recovery_checkpoints AS checkpoints
                    USING doomed
                    WHERE checkpoints.environment = doomed.environment
                      AND checkpoints.account_label = doomed.account_label
                      AND checkpoints.symbol = doomed.symbol
                      AND checkpoints.position_side = doomed.position_side
                      AND checkpoints.stream_id = doomed.stream_id
                      AND checkpoints.stream_epoch = doomed.stream_epoch
                      AND checkpoints.checkpoint_id = doomed.checkpoint_id
                      AND NOT EXISTS (
                          SELECT 1
                          FROM execution_book_heads AS heads
                          WHERE heads.environment = checkpoints.environment
                            AND heads.account_label = checkpoints.account_label
                            AND heads.symbol = checkpoints.symbol
                            AND heads.position_side = checkpoints.position_side
                            AND heads.stream_id = checkpoints.stream_id
                            AND heads.stream_epoch = checkpoints.stream_epoch
                            AND heads.state_payload -> 'recovery_checkpoint' ->>
                                'checkpoint_id' = checkpoints.checkpoint_id
                      )
                    RETURNING checkpoints.environment,
                              checkpoints.account_label, checkpoints.symbol,
                              checkpoints.position_side, checkpoints.stream_id,
                              checkpoints.stream_epoch, checkpoints.checkpoint_id
                ), removed_candidates AS (
                    DELETE FROM position_recovery_checkpoint_candidates AS candidates
                    USING removed
                    WHERE candidates.environment = removed.environment
                      AND candidates.account_label = removed.account_label
                      AND candidates.symbol = removed.symbol
                      AND candidates.position_side = removed.position_side
                      AND candidates.stream_id = removed.stream_id
                      AND candidates.stream_epoch = removed.stream_epoch
                      AND candidates.checkpoint_id = removed.checkpoint_id
                    RETURNING 1
                )
                SELECT count(*) FROM removed
                """,
                batch_size,
            )
            or 0
        )
        deleted += batch
        if batch == 0:
            break
    return deleted


async def _prepare_retained_checkpoint_cuts(
    connection: asyncpg.Connection[Any],
) -> None:
    """Cache one latest checkpoint cut per scope for the event purge pass."""
    await connection.execute(
        """
        CREATE TEMP TABLE retained_recovery_checkpoint_cuts
        ON COMMIT PRESERVE ROWS AS
        SELECT DISTINCT ON (
            environment, account_label, symbol, position_side,
            stream_id, stream_epoch
        )
            environment, account_label, symbol, position_side,
            stream_id, stream_epoch, event_cut, source_revision
        FROM position_recovery_checkpoints
        ORDER BY
            environment, account_label, symbol, position_side,
            stream_id, stream_epoch, event_cut DESC, source_revision DESC,
            recorded_at DESC, checkpoint_id DESC
        """
    )
    await connection.execute(
        """
        CREATE UNIQUE INDEX retained_recovery_checkpoint_cuts_scope_idx
        ON retained_recovery_checkpoint_cuts (
            environment, account_label, symbol, position_side,
            stream_id, stream_epoch
        )
        """
    )


async def _materialize_state_event_candidates(
    connection: asyncpg.Connection[Any], *, cutoff: datetime, limit: int
) -> int:
    """Select old covered events once; retain one old row per scope and kind."""
    await connection.execute(
        """
        CREATE TEMP TABLE position_recovery_state_event_candidates (
            event_record_id text PRIMARY KEY
        ) ON COMMIT PRESERVE ROWS
        """
    )
    await connection.execute(
        """
        INSERT INTO position_recovery_state_event_candidates (event_record_id)
        SELECT eligible.event_record_id
        FROM (
            SELECT events.event_record_id, events.recorded_at,
                   row_number() OVER (
                       PARTITION BY
                           events.environment, events.account_label,
                           events.symbol, events.position_side,
                           events.stream_id, events.stream_epoch,
                           events.event_kind
                       ORDER BY events.occurred_at DESC,
                                events.source_revision DESC,
                                events.recorded_at DESC,
                                events.event_record_id DESC
                   ) AS occurred_order_rank,
                   row_number() OVER (
                       PARTITION BY
                           events.environment, events.account_label,
                           events.symbol, events.position_side,
                           events.stream_id, events.stream_epoch,
                           events.event_kind
                       ORDER BY events.source_revision DESC,
                                events.recorded_at DESC,
                                events.event_record_id DESC
                   ) AS revision_order_rank
            FROM position_fact_journal_events AS events
            JOIN retained_recovery_checkpoint_cuts AS checkpoints
              ON checkpoints.environment = events.environment
             AND checkpoints.account_label = events.account_label
             AND checkpoints.symbol = events.symbol
             AND checkpoints.position_side = events.position_side
             AND checkpoints.stream_id = events.stream_id
             AND checkpoints.stream_epoch = events.stream_epoch
            WHERE events.event_kind = ANY($2::text[])
              AND events.recorded_at < $1
              AND events.occurred_at <= checkpoints.event_cut
              AND events.source_revision <= checkpoints.source_revision
        ) AS eligible
        WHERE eligible.occurred_order_rank > 1
          AND eligible.revision_order_rank > 1
        ORDER BY eligible.recorded_at, eligible.event_record_id
        LIMIT $3
        """,
        cutoff,
        list(_COMPACTABLE_KINDS),
        limit,
    )
    return int(
        await connection.fetchval(
            "SELECT count(*) FROM position_recovery_state_event_candidates"
        )
        or 0
    )


async def _delete_state_event_batches(
    connection: asyncpg.Connection[Any], *, batch_size: int
) -> int:
    deleted = 0
    while True:
        batch = int(
            await connection.fetchval(
                """
                WITH doomed AS (
                    SELECT candidates.event_record_id
                    FROM position_recovery_state_event_candidates AS candidates
                    ORDER BY candidates.event_record_id
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                ), removed AS (
                    DELETE FROM position_fact_journal_events AS events
                    USING doomed
                    WHERE events.event_record_id = doomed.event_record_id
                    RETURNING events.event_record_id
                ), removed_candidates AS (
                    DELETE FROM position_recovery_state_event_candidates AS candidates
                    USING removed
                    WHERE candidates.event_record_id = removed.event_record_id
                    RETURNING 1
                )
                SELECT count(*) FROM removed
                """,
                batch_size,
            )
            or 0
        )
        deleted += batch
        if batch == 0:
            break
    return deleted


async def _run(args: argparse.Namespace) -> int:
    database_url = resolve_database_url(
        args.database_url,
        "CML_OBSERVABILITY_DATABASE_URL",
        "CML_DATABASE_URL",
    )
    if not database_url:
        raise ValueError("database URL must be provided or configured")
    cutoff = datetime.now(UTC) - timedelta(hours=args.minimum_age_hours)
    connection = await asyncpg.connect(
        database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    )
    try:
        await connection.execute(
            f"SET lock_timeout = '{args.lock_timeout_seconds}s'"
        )
        await connection.execute("SET statement_timeout = '300s'")
        acquired = await connection.fetchval(
            "SELECT pg_try_advisory_lock(hashtext($1))", _ADVISORY_LOCK_NAME
        )
        if not acquired:
            raise RuntimeError("position recovery retention is already running")
        try:
            await _prepare_protected_checkpoints(connection)
            candidate_checkpoints = await _materialize_checkpoint_candidates(
                connection,
                cutoff=cutoff,
                limit=args.max_checkpoints,
            )
            if args.dry_run:
                deleted_checkpoints = 0
            else:
                deleted_checkpoints = await _delete_checkpoint_batches(
                    connection, batch_size=args.batch_size
                )

            await _prepare_retained_checkpoint_cuts(connection)
            candidate_events = await _materialize_state_event_candidates(
                connection,
                cutoff=cutoff,
                limit=args.max_events,
            )
            if args.dry_run:
                deleted_events = 0
            else:
                deleted_events = await _delete_state_event_batches(
                    connection, batch_size=args.batch_size
                )

            if args.dry_run:
                print(
                    "would delete "
                    f"checkpoints={candidate_checkpoints} "
                    f"state_events={candidate_events}"
                )
            else:
                print(
                    "deleted "
                    f"checkpoints={deleted_checkpoints} "
                    f"state_events={deleted_events}"
                )
        finally:
            await connection.execute(
                "SELECT pg_advisory_unlock(hashtext($1))", _ADVISORY_LOCK_NAME
            )
    finally:
        await connection.close()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url")
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--dry-run", action="store_true")
    operation.add_argument("--apply", action="store_true")
    parser.add_argument("--minimum-age-hours", type=int, default=72)
    parser.add_argument("--max-checkpoints", type=int, default=100_000)
    parser.add_argument("--max-events", type=int, default=500_000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--lock-timeout-seconds", type=int, default=5)
    args = parser.parse_args(argv)
    if min(
        args.minimum_age_hours,
        args.max_checkpoints,
        args.max_events,
        args.batch_size,
        args.lock_timeout_seconds,
    ) < 1:
        parser.error("retention limits must be positive")
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
