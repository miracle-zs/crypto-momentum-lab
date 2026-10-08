"""Verify cold metadata and payloads before removing bounded hot copies."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import delete, select, text
from sqlalchemy.orm import sessionmaker

from crypto_momentum_lab.config import resolve_database_url
from crypto_momentum_lab.persistence.postgres.market_revision_archive import (
    ZstdMarketRevisionPayloadArchive,
)
from crypto_momentum_lab.persistence.postgres.market_revision_metadata_archive import (
    SqliteMarketRevisionMetadataArchive,
    rebuild_metadata_catalog,
)
from crypto_momentum_lab.persistence.postgres.models import MarketRevisionRefRow
from crypto_momentum_lab.persistence.postgres.session import create_sync_engine

_LIVE_REFERENCE_FILTER = """NOT EXISTS (
 SELECT 1 FROM decision_traces t
 WHERE coalesce(t.trace_payload->>'evidence_level','') <> 'summary'
 AND t.evaluated_revision_ids ? market_revision_refs.revision_id)"""


def archive_batch(
    session, *, catalog, payload_archive, cutoff, batch_size=500, apply=False
):
    session.execute(text("SET LOCAL lock_timeout='5s'"))
    session.execute(text("SET LOCAL statement_timeout='30s'"))
    query = (
        select(MarketRevisionRefRow)
        .where(
            MarketRevisionRefRow.payload.is_(None),
            MarketRevisionRefRow.payload_archive_path.is_not(None),
            MarketRevisionRefRow.payload_archive_sha256.is_not(None),
            MarketRevisionRefRow.bucket_start < cutoff,
            MarketRevisionRefRow.published_at < cutoff,
            text(_LIVE_REFERENCE_FILTER),
        )
        .order_by(MarketRevisionRefRow.bucket_start, MarketRevisionRefRow.revision_id)
        .limit(batch_size)
    )
    if apply:
        query = query.with_for_update(skip_locked=True)
    rows = list(session.scalars(query).all())
    if not apply or not rows:
        return len(rows), 0
    groups = defaultdict(dict)
    for row in rows:
        groups[(row.payload_archive_path, row.payload_archive_sha256)][
            row.revision_id
        ] = row.content_hash
    for (path, digest), members in groups.items():
        payload_archive.verify_members(
            relative_path=path, expected_sha256=digest, members=members
        )
    catalog.store(rows)
    # Fresh snapshot recheck plus row locks and writer key-share locks preserve
    # full decision references. Failed file/metadata checks leave hot rows intact.
    result = session.execute(
        delete(MarketRevisionRefRow)
        .where(
            MarketRevisionRefRow.revision_id.in_([row.revision_id for row in rows]),
            text(_LIVE_REFERENCE_FILTER),
        )
        .execution_options(synchronize_session=False)
    )
    return len(rows), result.rowcount


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--rebuild-index", action="store_true")
    parser.add_argument("--retention-days", type=int, default=7)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-batches", type=int, default=40)
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=Path(
            os.environ.get(
                "CML_MARKET_REVISION_ARCHIVE_DIR", "/app/market-revision-archive"
            )
        ),
    )
    args = parser.parse_args()
    if (
        args.retention_days < 7
        or not 1 <= args.batch_size <= 1000
        or not 1 <= args.max_batches <= 1000
    ):
        parser.error(
            "retention must be >=7 days; batch size 1..1000; max batches 1..1000"
        )
    args.archive_root.mkdir(parents=True, exist_ok=True)
    lock = (args.archive_root / ".metadata-maintenance.lock").open("a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(json.dumps({"skipped": "metadata maintenance already running"}))
        return
    if args.rebuild_index:
        count = rebuild_metadata_catalog(args.archive_root)
        print(json.dumps({"rebuilt_snapshot_rows": count}))
        return
    url = resolve_database_url(None, "CML_MARKET_DATABASE_URL", "CML_DATABASE_URL")
    engine = create_sync_engine(
        url.replace("postgresql+asyncpg://", "postgresql+psycopg://")
    )
    factory = sessionmaker(engine, expire_on_commit=False)
    catalog = SqliteMarketRevisionMetadataArchive(args.archive_root)
    payloads = ZstdMarketRevisionPayloadArchive(args.archive_root, cache_max_bytes=0)
    cutoff = datetime.now(UTC) - timedelta(days=args.retention_days)
    try:
        for index in range(args.max_batches):
            with factory.begin() as session:
                selected, archived = archive_batch(
                    session,
                    catalog=catalog,
                    payload_archive=payloads,
                    cutoff=cutoff,
                    batch_size=args.batch_size,
                    apply=args.apply,
                )
            print(
                json.dumps(
                    {
                        "batch": index,
                        "selected": selected,
                        "archived": archived,
                        "apply": args.apply,
                    }
                ),
                flush=True,
            )
            if selected == 0 or not args.apply:
                break
            time.sleep(0.2)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
