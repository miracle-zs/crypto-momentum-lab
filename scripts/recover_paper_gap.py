#!/usr/bin/env python3
"""Rewind paper-account artifacts to a durable cursor before gap replay.

The command is intentionally conservative: it refuses to mutate a database
unless its actual name matches --expected-database and --apply is present.
It preserves every trade and equity snapshot at or before the cursor.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import Connection, Engine, create_engine, delete, func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL, make_url
from sqlalchemy.orm import Session

from crypto_momentum_lab.domain.strategy import (
    RunMode,
    StrategyCheckpoint,
    StrategyRunIdentity,
)
from crypto_momentum_lab.persistence.postgres.models import (
    OrderIntentCandidateRow,
    PaperEquitySnapshotRow,
    PaperFillRow,
    PaperPositionRow,
    RuntimeMarketState15sRow,
    StrategyRunRow,
    StrategyRuntimeCheckpointRow,
    StrategyRuntimeEventRow,
    StrategySignalRow,
)
from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
    checkpoint_row_values,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    market_state_from_row,
)
from crypto_momentum_lab.strategies.compression_breakout.event_study import (
    CompressionBreakoutConfig,
)
from crypto_momentum_lab.strategy_runner.registry import build_runtime_strategy


@dataclass(frozen=True, slots=True)
class PairSpec:
    strategy_name: str
    fixed_run_id: str
    candle_run_id: str


PAIR_SPECS = (
    PairSpec(
        strategy_name="compression_breakout",
        fixed_run_id="paper-account-01-compression-v1",
        candle_run_id="paper-account-04-compression-candle15m-v1",
    ),
    PairSpec(
        strategy_name="orderflow_impulse",
        fixed_run_id="paper-account-02-orderflow-v1",
        candle_run_id="paper-account-05-orderflow-candle15m-v1",
    ),
    PairSpec(
        strategy_name="liquidation_cascade",
        fixed_run_id="paper-account-03-liquidation-v1",
        candle_run_id="paper-account-06-liquidation-candle15m-v1",
    ),
)
RUN_IDS = tuple(
    run_id
    for pair in PAIR_SPECS
    for run_id in (pair.fixed_run_id, pair.candle_run_id)
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare six paper accounts for deterministic gap replay."
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("CML_DATABASE_URL"),
    )
    parser.add_argument(
        "--database-name",
        help="Override only the database component of the connection URL.",
    )
    parser.add_argument("--expected-database", required=True)
    parser.add_argument("--cutoff", required=True, help="Last processed 15s bucket.")
    parser.add_argument("--warmup-hours", type=int, default=4)
    parser.add_argument("--environment", default="research")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("--database-url or CML_DATABASE_URL is required")

    cutoff = _parse_timestamp(args.cutoff)
    if cutoff.microsecond or cutoff.second % 15:
        raise SystemExit("--cutoff must be aligned to a 15-second bucket")
    if args.warmup_hours < 2:
        raise SystemExit("--warmup-hours must be at least 2")

    database_url = make_url(args.database_url)
    if args.database_name:
        database_url = database_url.set(database=args.database_name)
    engine = create_engine(_sync_url(database_url))
    try:
        database_name = _database_name(engine)
        if database_name != args.expected_database:
            raise SystemExit(
                f"database guard failed: expected {args.expected_database!r}, "
                f"connected to {database_name!r}"
            )
        before = _account_summaries(engine)
        checkpoints, state_counts = _build_checkpoints(
            engine=engine,
            cutoff=cutoff,
            warmup_start=cutoff - timedelta(hours=args.warmup_hours),
            environment=args.environment,
        )
        report: dict[str, Any] = {
            "database": database_name,
            "cutoff": cutoff.isoformat(),
            "mode": "apply" if args.apply else "dry-run",
            "warmup_state_counts": state_counts,
            "before": before,
        }
        if not args.apply:
            print(json.dumps(report, indent=2, sort_keys=True))
            return

        with engine.begin() as connection:
            deletion_counts = {
                run_id: _rewind_run(connection, run_id=run_id, cutoff=cutoff)
                for run_id in RUN_IDS
            }
            for pair in PAIR_SPECS:
                checkpoint = checkpoints[pair.strategy_name]
                _save_checkpoint(
                    connection,
                    run_id=pair.fixed_run_id,
                    checkpoint=checkpoint,
                    saved_at=cutoff + timedelta(seconds=15),
                )
                _save_checkpoint(
                    connection,
                    run_id=pair.candle_run_id,
                    checkpoint=checkpoint,
                    saved_at=cutoff + timedelta(seconds=15),
                )
            for run_id in RUN_IDS:
                _refresh_run_counts(connection, run_id)

        report["deleted"] = deletion_counts
        report["after"] = _account_summaries(engine)
        print(json.dumps(report, indent=2, sort_keys=True))
    finally:
        engine.dispose()


def _sync_url(value: str | URL) -> URL:
    url = make_url(value)
    if not url.drivername.startswith("postgresql"):
        raise SystemExit("--database-url must use PostgreSQL")
    return url.set(drivername="postgresql+psycopg")


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SystemExit("timestamp must include a UTC offset")
    return parsed.astimezone(UTC)


def _database_name(engine: Engine) -> str:
    with engine.connect() as connection:
        value = connection.scalar(text("SELECT current_database()"))
    if not isinstance(value, str):
        raise RuntimeError("could not resolve current database")
    return value


def _build_checkpoints(
    *,
    engine: Engine,
    cutoff: datetime,
    warmup_start: datetime,
    environment: str,
) -> tuple[dict[str, StrategyCheckpoint], dict[str, int]]:
    checkpoints: dict[str, StrategyCheckpoint] = {}
    state_counts: dict[str, int] = {}
    for pair in PAIR_SPECS:
        with Session(engine) as session:
            run = session.get(StrategyRunRow, pair.fixed_run_id)
            if run is None:
                raise RuntimeError(f"missing strategy run {pair.fixed_run_id}")
            identity = StrategyRunIdentity(
                run_id=run.run_id,
                strategy_name=run.strategy_name,
                strategy_version=run.strategy_version,
                config_hash=run.config_hash,
                run_mode=RunMode(run.run_mode),
                code_commit=run.code_commit,
                created_at=run.created_at,
                source_paths=tuple(run.source_paths),
            )
            strategy = build_runtime_strategy(
                pair.strategy_name,
                config=_runtime_config_payload(),
                identity=identity,
            )
            statement = (
                select(RuntimeMarketState15sRow)
                .where(
                    RuntimeMarketState15sRow.environment == environment,
                    RuntimeMarketState15sRow.bucket_start >= warmup_start,
                    RuntimeMarketState15sRow.bucket_start <= cutoff,
                )
                .order_by(
                    RuntimeMarketState15sRow.bucket_start,
                    RuntimeMarketState15sRow.symbol,
                )
                .execution_options(yield_per=2000)
            )
            state_count = 0
            latest_bucket: datetime | None = None
            for row in session.scalars(statement):
                state = market_state_from_row(row)
                strategy.on_market_state(state)
                state_count += 1
                latest_bucket = state.bucket_start
            if latest_bucket != cutoff:
                raise RuntimeError(
                    f"{pair.strategy_name} warmup ended at {latest_bucket}, "
                    f"expected {cutoff}"
                )
            checkpoints[pair.strategy_name] = strategy.checkpoint()
            state_counts[pair.strategy_name] = state_count
    return checkpoints, state_counts


def _runtime_config_payload() -> dict[str, object]:
    return {
        "candidate_notional": Decimal("25"),
        "candidate_ttl_buckets": 4,
        "signal_interval_seconds": 300,
        "compression_breakout": CompressionBreakoutConfig(
            compression_window_buckets=20,
            max_range_width_pct=Decimal("0.025"),
            min_breakout_pct=Decimal("0.003"),
            acceptance_buckets=1,
            cooldown_buckets=12,
            forward_horizon_buckets=(1,),
        ),
    }


def _rewind_run(
    connection: Connection,
    *,
    run_id: str,
    cutoff: datetime,
) -> dict[str, int]:
    artifact_cutoff = cutoff + timedelta(seconds=15)
    deleted_signals = connection.execute(
        delete(StrategySignalRow).where(
            StrategySignalRow.run_id == run_id,
            StrategySignalRow.detected_at > artifact_cutoff,
        )
    ).rowcount
    deleted_candidates = connection.execute(
        delete(OrderIntentCandidateRow).where(
            OrderIntentCandidateRow.run_id == run_id,
            OrderIntentCandidateRow.created_at > artifact_cutoff,
        )
    ).rowcount
    deleted_fills = connection.execute(
        delete(PaperFillRow).where(
            PaperFillRow.run_id == run_id,
            PaperFillRow.target_fill_at > cutoff,
        )
    ).rowcount
    deleted_positions = connection.execute(
        delete(PaperPositionRow).where(
            PaperPositionRow.run_id == run_id,
            PaperPositionRow.opened_at > cutoff,
        )
    ).rowcount
    reopened_positions = connection.execute(
        text(
            """
            WITH active AS (
                SELECT
                    p.position_id,
                    COALESCE(mark.mark_price, p.entry_price) AS mark_price,
                    COALESCE(mark.bucket_start, p.opened_at) AS marked_at
                FROM paper_positions AS p
                LEFT JOIN LATERAL (
                    SELECT
                        CASE
                            WHEN p.side = 'long' THEN s.last_bid_price
                            ELSE s.last_ask_price
                        END AS mark_price,
                        s.bucket_start
                    FROM runtime_market_states_15s AS s
                    WHERE s.environment = :environment
                      AND s.symbol = p.symbol
                      AND s.bucket_start > p.opened_at
                      AND s.bucket_start <= :cutoff
                      AND CASE
                            WHEN p.side = 'long' THEN s.last_bid_price > 0
                            ELSE s.last_ask_price > 0
                          END
                    ORDER BY s.bucket_start DESC
                    LIMIT 1
                ) AS mark ON TRUE
                WHERE p.run_id = :run_id
                  AND p.opened_at <= :cutoff
                  AND (p.closed_at IS NULL OR p.closed_at > :cutoff)
            )
            UPDATE paper_positions AS p
            SET status = 'open',
                closed_at = NULL,
                exit_price = NULL,
                exit_fee = 0,
                last_mark_price = active.mark_price,
                unrealized_pnl = (
                    p.quantity
                    * (active.mark_price - p.entry_price)
                    * CASE WHEN p.side = 'long' THEN 1 ELSE -1 END
                ) - p.entry_fee,
                realized_pnl = NULL,
                return_pct = NULL,
                close_reason = NULL,
                updated_at = active.marked_at
            FROM active
            WHERE p.position_id = active.position_id
            """
        ),
        {"environment": "research", "run_id": run_id, "cutoff": cutoff},
    ).rowcount
    deleted_snapshots = connection.execute(
        delete(PaperEquitySnapshotRow).where(
            PaperEquitySnapshotRow.run_id == run_id,
            PaperEquitySnapshotRow.observed_at > cutoff,
        )
    ).rowcount
    deleted_events = connection.execute(
        delete(StrategyRuntimeEventRow).where(
            StrategyRuntimeEventRow.run_id == run_id,
            (
                (StrategyRuntimeEventRow.bucket_start > cutoff)
                | (
                    StrategyRuntimeEventRow.bucket_start.is_(None)
                    & (StrategyRuntimeEventRow.occurred_at > artifact_cutoff)
                )
            ),
        )
    ).rowcount
    return {
        "signals": deleted_signals,
        "candidates": deleted_candidates,
        "fills": deleted_fills,
        "positions": deleted_positions,
        "positions_reopened": reopened_positions,
        "equity_snapshots": deleted_snapshots,
        "runtime_events": deleted_events,
    }


def _save_checkpoint(
    connection: Connection,
    *,
    run_id: str,
    checkpoint: StrategyCheckpoint,
    saved_at: datetime,
) -> None:
    values = checkpoint_row_values(
        run_id=run_id,
        checkpoint=checkpoint,
        saved_at=saved_at,
    )
    statement = insert(StrategyRuntimeCheckpointRow).values(values)
    connection.execute(
        statement.on_conflict_do_update(
            index_elements=[StrategyRuntimeCheckpointRow.run_id],
            set_={key: value for key, value in values.items() if key != "run_id"},
        )
    )


def _refresh_run_counts(connection: Connection, run_id: str) -> None:
    counts = connection.execute(
        text(
            """
            SELECT
                (SELECT count(*) FROM strategy_signals WHERE run_id = :run_id),
                (SELECT count(*) FROM order_intent_candidates WHERE run_id = :run_id),
                (SELECT count(*) FROM paper_fills WHERE run_id = :run_id),
                (
                    SELECT count(*)
                    FROM order_intent_candidates AS c
                    WHERE c.run_id = :run_id
                      AND NOT EXISTS (
                          SELECT 1 FROM paper_fills AS f
                          WHERE f.candidate_id = c.candidate_id
                      )
                )
            """
        ),
        {"run_id": run_id},
    ).one()
    connection.execute(
        text(
            """
            UPDATE strategy_runs
            SET signal_count = :signals,
                candidate_count = :candidates,
                fill_count = :fills,
                pending_candidate_count = :pending
            WHERE run_id = :run_id
            """
        ),
        {
            "run_id": run_id,
            "signals": counts[0],
            "candidates": counts[1],
            "fills": counts[2],
            "pending": counts[3],
        },
    )


def _account_summaries(engine: Engine) -> dict[str, dict[str, int]]:
    with Session(engine) as session:
        summaries: dict[str, dict[str, int]] = {}
        for run_id in RUN_IDS:
            summaries[run_id] = {
                "positions": session.scalar(
                    select(func.count()).select_from(PaperPositionRow).where(
                        PaperPositionRow.run_id == run_id
                    )
                )
                or 0,
                "closed_positions": session.scalar(
                    select(func.count()).select_from(PaperPositionRow).where(
                        PaperPositionRow.run_id == run_id,
                        PaperPositionRow.status == "closed",
                    )
                )
                or 0,
                "signals": session.scalar(
                    select(func.count()).select_from(StrategySignalRow).where(
                        StrategySignalRow.run_id == run_id
                    )
                )
                or 0,
                "equity_snapshots": session.scalar(
                    select(func.count())
                    .select_from(PaperEquitySnapshotRow)
                    .where(PaperEquitySnapshotRow.run_id == run_id)
                )
                or 0,
            }
    return summaries


if __name__ == "__main__":
    main()
