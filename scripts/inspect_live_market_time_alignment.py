#!/usr/bin/env python3
"""Read-only checks for live market-state timestamp and bucket alignment."""

from __future__ import annotations

import asyncio
import os

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def main() -> None:
    engine = create_async_engine(os.environ["CML_DATABASE_URL"])
    queries = {
        "states": text(
            """
            SELECT
                COUNT(*) AS rows,
                MIN(bucket_start) AS first_bucket,
                MAX(bucket_start) AS last_bucket,
                COUNT(*) FILTER (
                    WHERE MOD(EXTRACT(EPOCH FROM (bucket_start AT TIME ZONE 'UTC'))::bigint, 15) <> 0
                ) AS non_15s_boundaries,
                COUNT(*) FILTER (WHERE NOT data_complete) AS incomplete,
                COUNT(*) FILTER (WHERE source_watermark_at < bucket_end) AS watermark_before_close
            FROM runtime_market_states_15s
            WHERE environment = 'research'
            """
        ),
        "archives": text(
            """
            SELECT
                stream,
                COUNT(*) AS manifests,
                SUM(row_count) AS rows,
                COUNT(*) FILTER (WHERE first_exchange_event_at IS NULL) AS no_exchange_time,
                MIN(first_exchange_event_at) AS first_exchange_event,
                MAX(last_exchange_event_at) AS last_exchange_event,
                MIN(first_received_at) AS first_received,
                MAX(last_received_at) AS last_received
            FROM raw_archive_manifests
            WHERE environment = 'research'
            GROUP BY stream
            ORDER BY stream
            """
        ),
    }
    async with engine.connect() as connection:
        for name, query in queries.items():
            result = await connection.execute(query)
            print(name)
            for row in result.mappings():
                print(dict(row))
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
