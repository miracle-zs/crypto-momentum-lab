"""CLI and programmatic tool to catalog, inspect, and verify DatasetManifests.

Obeys Astra Architecture Blueprint Sections 9, 10 & 16:
- Compiles immutable DatasetManifests with interval coverage and holes proof;
- Persists reproducible manifests to Postgres `dataset_manifests` table;
- Verifies cryptographic manifest hash and complete revision references;
- Ensures reproducible market data streams for research and strategy replay.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from crypto_momentum_lab.config import resolve_database_url
from crypto_momentum_lab.domain.market.market_book import (
    DatasetCatalog,
    MarketBook,
)
from crypto_momentum_lab.domain.market.revision_models import (
    MarketVisibilityMode,
)
from crypto_momentum_lab.persistence.postgres.market_book_repository import (
    PostgresMarketBookRepository,
)
from crypto_momentum_lab.persistence.postgres.session import create_sync_engine


def get_catalog_and_repo(
    database_url: str | None = None,
) -> tuple[DatasetCatalog, PostgresMarketBookRepository]:
    """Builds a DatasetCatalog wired to PostgresMarketBookRepository."""
    url = resolve_database_url(
        database_url,
        "CML_MARKET_DATABASE_URL",
        "CML_DATABASE_URL",
    )
    if not url:
        raise ValueError(
            "Database URL must be provided or configured via CML_DATABASE_URL"
        )
    if url.startswith("postgresql+asyncpg://"):
        sync_url = url.replace("postgresql+asyncpg://", "postgresql+psycopg://")
    else:
        sync_url = url
    engine = create_sync_engine(sync_url)
    session_factory: sessionmaker[Session] = sessionmaker(
        engine, expire_on_commit=False
    )
    repo = PostgresMarketBookRepository(session_factory)
    book = MarketBook(repo)
    catalog = DatasetCatalog(book, repo)
    return catalog, repo


def catalog_auto_daily(
    catalog: DatasetCatalog,
    repo: PostgresMarketBookRepository,
    scope: str = "research",
    interval: str = "15s",
    save: bool = True,
    overwrite: bool = False,
) -> list[dict[str, Any]]:
    """Scan distinct dates in revisions and catalog completed daily datasets."""
    date_symbols = repo.get_distinct_dates_and_symbols(
        scope=scope, interval=interval
    )
    existing_manifests = {
        m.manifest_id: m for m in catalog.list_manifests(scope=scope, limit=1000)
    }
    results: list[dict[str, Any]] = []
    now_dt = datetime.now(UTC)

    for d, symbols in date_symbols:
        day_start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=UTC)
        day_end = day_start + timedelta(days=1)
        is_today = day_end > now_dt
        if is_today:
            sec = (now_dt.second // 15) * 15
            day_end = now_dt.replace(second=sec, microsecond=0)

        if day_end <= day_start:
            continue

        manifest_id = f"ds_{scope}_{d.strftime('%Y%m%d')}_{interval}"
        if not overwrite and not is_today and manifest_id in existing_manifests:
            existing = existing_manifests[manifest_id]
            results.append(
                {
                    "manifest_id": existing.manifest_id,
                    "scope": existing.scope,
                    "date": d.isoformat(),
                    "symbols_count": len(existing.symbols),
                    "revisions_count": len(existing.revision_refs),
                    "coverage_ratio": str(existing.coverage_ratio),
                    "holes_count": len(existing.holes),
                    "manifest_hash": existing.manifest_hash,
                    "saved": False,
                    "skipped": True,
                }
            )
            continue

        manifest = catalog.build_dataset(
            manifest_id=manifest_id,
            scope=scope,
            symbols=symbols,
            interval=interval,
            start_time=day_start,
            end_time=day_end,
            visibility_mode=MarketVisibilityMode.CANONICAL,
        )
        if save:
            repo.save_manifest(manifest)

        results.append(
            {
                "manifest_id": manifest.manifest_id,
                "scope": manifest.scope,
                "date": d.isoformat(),
                "symbols_count": len(manifest.symbols),
                "revisions_count": len(manifest.revision_refs),
                "coverage_ratio": str(manifest.coverage_ratio),
                "holes_count": len(manifest.holes),
                "manifest_hash": manifest.manifest_hash,
                "saved": save,
            }
        )
    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Catalog, inspect, and verify reproducible DatasetManifests."
    )
    parser.add_argument(
        "--db-url",
        dest="db_url",
        default=None,
        help="Database URL (defaults to CML_DATABASE_URL)",
    )
    parser.add_argument(
        "--scope",
        default="research",
        help="Dataset scope (e.g. research, live)",
    )
    parser.add_argument(
        "--interval",
        default="15s",
        help="Bar interval (default: 15s)",
    )
    parser.add_argument(
        "--list",
        dest="action_list",
        action="store_true",
        help="List registered dataset manifests in the database",
    )
    parser.add_argument(
        "--verify",
        dest="verify_manifest_id",
        default=None,
        help="Verify cryptographic integrity of a DatasetManifest by ID",
    )
    parser.add_argument(
        "--auto-daily",
        action="store_true",
        help="Scan revisions and build dataset manifests for all distinct days",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        default=True,
        help="Persist built manifests into postgres dataset_manifests table",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Force overwrite already cataloged manifests",
    )

    args = parser.parse_args()

    try:
        catalog, repo = get_catalog_and_repo(args.db_url)
    except Exception as exc:
        print(f"Error initializing catalog: {exc}", file=sys.stderr)
        sys.exit(2)

    if args.action_list:
        manifests = catalog.list_manifests(scope=args.scope)
        output = [
            {
                "manifest_id": m.manifest_id,
                "scope": m.scope,
                "symbols_count": len(m.symbols),
                "interval": m.interval,
                "start_time": m.start_time.isoformat(),
                "end_time": m.end_time.isoformat(),
                "coverage_ratio": str(m.coverage_ratio),
                "holes_count": len(m.holes),
                "manifest_hash": m.manifest_hash,
                "created_at": m.created_at.isoformat(),
            }
            for m in manifests
        ]
        print(json.dumps(output, indent=2))
        sys.exit(0)

    if args.verify_manifest_id:
        result = catalog.verify_manifest(args.verify_manifest_id)
        print(json.dumps(result, indent=2))
        sys.exit(0 if result.get("verified") else 1)

    if args.auto_daily:
        results = catalog_auto_daily(
            catalog,
            repo,
            scope=args.scope,
            interval=args.interval,
            save=args.save,
            overwrite=args.overwrite,
        )
        print(json.dumps(results, indent=2))
        sys.exit(0)

    parser.print_help()


if __name__ == "__main__":
    main()
