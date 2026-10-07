"""Move old market revision payloads to verified zstd JSONL archives.

By default this command only reports eligible 15-minute windows. ``--apply``
archives bounded batches and replaces just the large JSON payload with a
content-addressed archive pointer; all market revision metadata remains in
PostgreSQL.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import Integer, cast, func, select, text, update
from sqlalchemy.orm import Session, sessionmaker

from crypto_momentum_lab.config import resolve_database_url
from crypto_momentum_lab.domain.market.market_book import compute_market_state_hash
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.state_codec import market_state_from_payload
from crypto_momentum_lab.persistence.postgres.market_revision_archive import (
    ZstdMarketRevisionPayloadArchive,
    market_revision_archive_partition,
    write_market_revision_archive,
)
from crypto_momentum_lab.persistence.postgres.models import MarketRevisionRefRow
from crypto_momentum_lab.persistence.postgres.session import create_sync_engine

_ARCHIVE_ADVISORY_LOCK = (601219, 15)
_DEFAULT_ARCHIVE_ROOT = Path("/app/market-revision-archive")
_BATCH_SIZE = 1000
_LEGACY_V1_HASH_FIELDS = (
    "schema_version",
    "environment",
    "exchange",
    "symbol",
    "bucket_start",
    "bucket_end",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "trade_count",
    "trade_notional",
    "aggressive_buy_notional",
    "aggressive_sell_notional",
    "last_bid_price",
    "last_ask_price",
    "spread",
    "midpoint",
    "liquidation_count",
    "liquidation_notional",
    "mark_price",
    "closed_kline_count",
    "source_event_count",
    "data_complete",
    "missing_agg_trade_count",
    "is_backfill",
)


@dataclass(frozen=True, slots=True)
class _Partition:
    scope: str
    hour_start: datetime
    quarter: int

    @property
    def start(self) -> datetime:
        return self.hour_start + timedelta(minutes=self.quarter * 15)

    @property
    def end(self) -> datetime:
        return self.start + timedelta(minutes=15)


def _market_state_hash_scheme(
    state: MarketState15s,
    expected_hash: str,
) -> str:
    """Match current hashes or the original v1 revision hash contract."""
    current_hash = compute_market_state_hash(state)
    if current_hash == expected_hash:
        return "current"

    legacy_payload: dict[str, object] = {}
    for name in _LEGACY_V1_HASH_FIELDS:
        value = getattr(state, name)
        if isinstance(value, Decimal):
            legacy_payload[name] = str(value)
        elif isinstance(value, datetime):
            legacy_payload[name] = value.isoformat()
        else:
            legacy_payload[name] = value
    legacy_hash = hashlib.sha256(
        json.dumps(
            legacy_payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    if legacy_hash == expected_hash:
        return "legacy_v1"

    raise RuntimeError(
        "market revision payload hash mismatch: "
        f"stored={expected_hash}, current={current_hash}, "
        f"legacy_v1={legacy_hash}"
    )


def _partition_expressions() -> tuple[Any, Any]:
    hour_start = func.date_trunc("hour", MarketRevisionRefRow.bucket_start)
    minute = cast(func.extract("minute", MarketRevisionRefRow.bucket_start), Integer)
    quarter = cast(func.floor(minute / 15), Integer)
    return hour_start, quarter


def _eligible_partitions(
    session: Session, *, cutoff: datetime, limit: int
) -> list[_Partition]:
    hour_start, quarter = _partition_expressions()
    statement = (
        select(
            MarketRevisionRefRow.scope,
            hour_start.label("hour_start"),
            quarter.label("quarter"),
        )
        .where(
            MarketRevisionRefRow.bucket_start < cutoff,
            MarketRevisionRefRow.payload.is_not(None),
            MarketRevisionRefRow.payload_archive_path.is_(None),
        )
        .group_by(MarketRevisionRefRow.scope, hour_start, quarter)
        .order_by(hour_start.asc(), quarter.asc(), MarketRevisionRefRow.scope.asc())
        .limit(limit)
    )
    return [
        _Partition(str(scope), hour, int(qtr))
        for scope, hour, qtr in session.execute(statement).all()
    ]


def _partition_stats(
    session: Session, *, partition: _Partition, cutoff: datetime
) -> tuple[int, int]:
    result = session.execute(
        select(
            func.count(),
            func.coalesce(
                func.sum(func.pg_column_size(MarketRevisionRefRow.payload)), 0
            ),
        ).where(
            MarketRevisionRefRow.scope == partition.scope,
            MarketRevisionRefRow.bucket_start >= partition.start,
            MarketRevisionRefRow.bucket_start < partition.end,
            MarketRevisionRefRow.bucket_start < cutoff,
            MarketRevisionRefRow.payload.is_not(None),
            MarketRevisionRefRow.payload_archive_path.is_(None),
        )
    ).one()
    return int(result[0]), int(result[1])


def _load_batch(
    session: Session,
    *,
    partition: _Partition,
    cutoff: datetime,
    after_revision_id: str | None,
    batch_size: int,
) -> list[MarketRevisionRefRow]:
    statement = (
        select(MarketRevisionRefRow)
        .where(
            MarketRevisionRefRow.scope == partition.scope,
            MarketRevisionRefRow.bucket_start >= partition.start,
            MarketRevisionRefRow.bucket_start < partition.end,
            MarketRevisionRefRow.bucket_start < cutoff,
            MarketRevisionRefRow.payload.is_not(None),
            MarketRevisionRefRow.payload_archive_path.is_(None),
        )
        .order_by(MarketRevisionRefRow.revision_id.asc())
        .limit(batch_size)
    )
    if after_revision_id is not None:
        statement = statement.where(
            MarketRevisionRefRow.revision_id > after_revision_id
        )
    return list(session.execute(statement).scalars().all())


def _archive_batch(
    session_factory: sessionmaker[Session],
    *,
    root: Path,
    partition: _Partition,
    rows: list[MarketRevisionRefRow],
) -> dict[str, object]:
    records: list[dict[str, object]] = []
    hash_scheme_counts = {"current": 0, "legacy_v1": 0}
    for row in rows:
        if row.payload is None:
            raise RuntimeError(f"revision {row.revision_id} has no database payload")
        payload = dict(row.payload)
        state = market_state_from_payload(payload)
        hash_scheme = _market_state_hash_scheme(state, row.content_hash)
        hash_scheme_counts[hash_scheme] += 1
        records.append(
            {
                "revision_id": row.revision_id,
                "content_hash": row.content_hash,
                "payload": payload,
            }
        )

    archive = write_market_revision_archive(
        root=root,
        partition=market_revision_archive_partition(partition.scope, partition.start),
        records=records,
    )
    expected_by_id = {str(record["revision_id"]): record for record in records}
    ids = list(expected_by_id)

    # The archive is durable before its pointers are committed. Re-read and
    # lock the bounded set so a concurrent rewrite cannot silently redirect a
    # revision to stale bytes.
    with session_factory.begin() as session:
        current_rows = list(
            session.execute(
                select(MarketRevisionRefRow)
                .where(MarketRevisionRefRow.revision_id.in_(ids))
                .with_for_update()
            )
            .scalars()
            .all()
        )
        if len(current_rows) != len(ids):
            raise RuntimeError("market revision rows changed while archiving")
        for current in current_rows:
            expected = expected_by_id[current.revision_id]
            if (
                current.payload != expected["payload"]
                or current.content_hash != expected["content_hash"]
                or current.payload_archive_path is not None
            ):
                raise RuntimeError(
                    f"market revision {current.revision_id} changed while archiving"
                )
        updated_ids = list(
            session.execute(
                update(MarketRevisionRefRow)
                .where(
                    MarketRevisionRefRow.revision_id.in_(ids),
                    MarketRevisionRefRow.payload.is_not(None),
                    MarketRevisionRefRow.payload_archive_path.is_(None),
                )
                .values(
                    payload=None,
                    payload_archive_path=archive.relative_path,
                    payload_archive_sha256=archive.sha256,
                )
                .returning(MarketRevisionRefRow.revision_id)
            )
            .scalars()
            .all()
        )
        if len(updated_ids) != len(ids):
            raise RuntimeError(
                "market revision archive pointer update affected an unexpected "
                f"number of rows: {len(updated_ids)} != {len(ids)}"
            )

    return {
        "scope": partition.scope,
        "window_start": partition.start.isoformat(),
        "window_end": partition.end.isoformat(),
        "rows": archive.record_count,
        "archive_path": archive.relative_path,
        "archive_sha256": archive.sha256,
        "compressed_bytes": archive.compressed_bytes,
        "uncompressed_bytes": archive.uncompressed_bytes,
        "content_hash_schemes": hash_scheme_counts,
    }


def archive_market_revision_payloads(
    *,
    session_factory: sessionmaker[Session],
    archive_root: Path,
    cutoff: datetime,
    max_chunks: int,
    batch_size: int = _BATCH_SIZE,
    apply: bool = False,
) -> dict[str, object]:
    """Report or archive a bounded number of oldest market payload batches."""
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("cutoff must include a timezone")
    if max_chunks <= 0 or batch_size <= 0:
        raise ValueError("max_chunks and batch_size must be positive")

    cutoff = cutoff.astimezone(UTC)
    with session_factory() as session:
        partitions = _eligible_partitions(session, cutoff=cutoff, limit=max_chunks)
        dry_run = []
        candidate_rows = 0
        estimated_payload_bytes = 0
        for partition in partitions:
            row_count, payload_bytes = _partition_stats(
                session, partition=partition, cutoff=cutoff
            )
            candidate_rows += row_count
            estimated_payload_bytes += payload_bytes
            dry_run.append(
                {
                    "scope": partition.scope,
                    "window_start": partition.start.isoformat(),
                    "window_end": partition.end.isoformat(),
                    "rows": row_count,
                    "payload_bytes": payload_bytes,
                }
            )
        if not apply:
            return {
                "mode": "dry_run",
                "cutoff": cutoff.isoformat(),
                "candidate_windows": dry_run,
                "candidate_rows": candidate_rows,
                "estimated_payload_bytes": estimated_payload_bytes,
            }

    results: list[dict[str, object]] = []
    archived_rows = 0
    for partition in partitions:
        after_revision_id: str | None = None
        while len(results) < max_chunks:
            with session_factory() as session:
                candidate_batch = _load_batch(
                    session,
                    partition=partition,
                    cutoff=cutoff,
                    after_revision_id=after_revision_id,
                    batch_size=batch_size,
                )
            if not candidate_batch:
                break
            after_revision_id = candidate_batch[-1].revision_id
            archive_rows = [
                row
                for row in candidate_batch
                if row.payload is not None and "schema_version" in row.payload
            ]
            if not archive_rows:
                continue
            archived_rows += len(archive_rows)
            results.append(
                _archive_batch(
                    session_factory,
                    root=archive_root,
                    partition=partition,
                    rows=archive_rows,
                )
            )
    return {
        "mode": "applied",
        "cutoff": cutoff.isoformat(),
        "archived_chunks": len(results),
        "archived_rows": archived_rows,
        "archives": results,
    }


def restore_market_revision_payloads(
    *,
    session_factory: sessionmaker[Session],
    archive_root: Path,
    max_chunks: int,
    batch_size: int = _BATCH_SIZE,
) -> dict[str, object]:
    """Restore a bounded number of archived payload batches into PostgreSQL."""
    if max_chunks <= 0 or batch_size <= 0:
        raise ValueError("max_chunks and batch_size must be positive")
    archive = ZstdMarketRevisionPayloadArchive(archive_root)
    restored_chunks = 0
    restored_rows = 0

    while restored_chunks < max_chunks:
        with session_factory() as session:
            rows = list(
                session.execute(
                    select(MarketRevisionRefRow)
                    .where(
                        MarketRevisionRefRow.payload.is_(None),
                        MarketRevisionRefRow.payload_archive_path.is_not(None),
                        MarketRevisionRefRow.payload_archive_sha256.is_not(None),
                    )
                    .order_by(MarketRevisionRefRow.revision_id.asc())
                    .limit(batch_size)
                )
                .scalars()
                .all()
            )
        if not rows:
            break

        payloads: dict[str, dict[str, object]] = {}
        pointers: dict[str, tuple[str, str, str]] = {}
        for row in rows:
            assert row.payload_archive_path is not None
            assert row.payload_archive_sha256 is not None
            payload = archive.load_payload(
                relative_path=row.payload_archive_path,
                expected_sha256=row.payload_archive_sha256,
                revision_id=row.revision_id,
                expected_content_hash=row.content_hash,
            )
            state = market_state_from_payload(payload)
            _market_state_hash_scheme(state, row.content_hash)
            payloads[row.revision_id] = payload
            pointers[row.revision_id] = (
                row.payload_archive_path,
                row.payload_archive_sha256,
                row.content_hash,
            )

        with session_factory.begin() as session:
            current_rows = list(
                session.execute(
                    select(MarketRevisionRefRow)
                    .where(MarketRevisionRefRow.revision_id.in_(payloads))
                    .with_for_update()
                )
                .scalars()
                .all()
            )
            if len(current_rows) != len(payloads):
                raise RuntimeError("market revision rows changed while restoring")
            for current in current_rows:
                if (
                    current.payload is not None
                    or current.payload_archive_path is None
                    or current.payload_archive_sha256 is None
                    or pointers[current.revision_id]
                    != (
                        current.payload_archive_path,
                        current.payload_archive_sha256,
                        current.content_hash,
                    )
                ):
                    raise RuntimeError(
                        f"market revision {current.revision_id} changed while restoring"
                    )
                current.payload = payloads[current.revision_id]
                current.payload_archive_path = None
                current.payload_archive_sha256 = None

        restored_chunks += 1
        restored_rows += len(payloads)

    return {
        "mode": "restored",
        "restored_chunks": restored_chunks,
        "restored_rows": restored_rows,
    }


def _database_url(value: str | None) -> str:
    url = resolve_database_url(value, "CML_MARKET_DATABASE_URL", "CML_DATABASE_URL")
    if not url:
        raise ValueError(
            "Database URL must be provided or configured via CML_DATABASE_URL"
        )
    if url.startswith("postgresql+asyncpg://"):
        return url.replace("postgresql+asyncpg://", "postgresql+psycopg://")
    return url


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Archive old market revision payloads to verified zstd JSONL."
    )
    parser.add_argument("--db-url", default=None)
    parser.add_argument("--archive-dir", type=Path, default=None)
    parser.add_argument(
        "--retention-days",
        type=int,
        default=1,
        help="Only archive buckets older than this many days (default: 1)",
    )
    parser.add_argument("--max-chunks", type=int, default=24)
    parser.add_argument("--batch-size", type=int, default=_BATCH_SIZE)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write archives and replace database payloads with pointers",
    )
    parser.add_argument(
        "--restore",
        action="store_true",
        help="Restore archived payloads into PostgreSQL (for downgrade/recovery)",
    )
    args = parser.parse_args()
    if args.apply and args.restore:
        parser.error("--apply and --restore are mutually exclusive")
    if args.retention_days < 1:
        parser.error("--retention-days must be at least 1")
    archive_root = args.archive_dir or Path(
        __import__("os").environ.get(
            "CML_MARKET_REVISION_ARCHIVE_DIR", str(_DEFAULT_ARCHIVE_ROOT)
        )
    )
    engine = create_sync_engine(_database_url(args.db_url))
    session_factory: sessionmaker[Session] = sessionmaker(
        engine, expire_on_commit=False
    )
    cutoff = datetime.now(UTC) - timedelta(days=args.retention_days)
    try:
        with engine.connect() as connection:
            acquired = bool(
                connection.execute(
                    text("SELECT pg_try_advisory_lock(:namespace, :lock_id)"),
                    {
                        "namespace": _ARCHIVE_ADVISORY_LOCK[0],
                        "lock_id": _ARCHIVE_ADVISORY_LOCK[1],
                    },
                ).scalar_one()
            )
            if not acquired:
                raise RuntimeError("another market revision archiver is running")
            try:
                if args.restore:
                    result = restore_market_revision_payloads(
                        session_factory=session_factory,
                        archive_root=archive_root,
                        max_chunks=args.max_chunks,
                        batch_size=args.batch_size,
                    )
                else:
                    result = archive_market_revision_payloads(
                        session_factory=session_factory,
                        archive_root=archive_root,
                        cutoff=cutoff,
                        max_chunks=args.max_chunks,
                        batch_size=args.batch_size,
                        apply=args.apply,
                    )
            finally:
                connection.execute(
                    text("SELECT pg_advisory_unlock(:namespace, :lock_id)"),
                    {
                        "namespace": _ARCHIVE_ADVISORY_LOCK[0],
                        "lock_id": _ARCHIVE_ADVISORY_LOCK[1],
                    },
                )
        print(json.dumps(result, sort_keys=True, indent=2))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
