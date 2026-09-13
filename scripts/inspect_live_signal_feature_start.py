#!/usr/bin/env python3
"""Read-only inspection of the first complete live orderflow feature signal."""

from __future__ import annotations

import asyncio
import os

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def main() -> None:
    engine = create_async_engine(os.environ["CML_DATABASE_URL"])
    query = text(
        """
        SELECT
            COUNT(*) AS total,
            MIN(detected_at) AS first_any,
            MAX(detected_at) AS last_any,
            COUNT(*) FILTER (
                WHERE features->>'impulse_return_pct' IS NOT NULL
                  AND features->>'aggressive_imbalance' IS NOT NULL
                  AND features->>'notional_intensity' IS NOT NULL
            ) AS complete_count,
            MIN(detected_at) FILTER (
                WHERE features->>'impulse_return_pct' IS NOT NULL
                  AND features->>'aggressive_imbalance' IS NOT NULL
                  AND features->>'notional_intensity' IS NOT NULL
            ) AS first_complete,
            MAX(detected_at) FILTER (
                WHERE features->>'impulse_return_pct' IS NOT NULL
                  AND features->>'aggressive_imbalance' IS NOT NULL
                  AND features->>'notional_intensity' IS NOT NULL
            ) AS last_complete
        FROM live_strategy_signals
        WHERE account_label = 'primary'
          AND strategy_name = 'orderflow_impulse'
          AND signal_kind = 'strategy_signal'
        """
    )
    async with engine.connect() as connection:
        row = (await connection.execute(query)).mappings().one()
        print(dict(row))
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
