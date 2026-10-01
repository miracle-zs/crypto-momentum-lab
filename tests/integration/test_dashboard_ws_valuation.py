"""Read-only synthetic account/mark history; no production tables are written."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.ext.asyncio import create_async_engine

from crypto_momentum_lab.operator_dashboard.live_account_metrics_queries import (
    account_equity_statement,
)

FIXTURE = "WITH account_balance_snapshots(environment,account_label,asset,observed_at,wallet_balance,unrealized_pnl,raw_payload) AS (VALUES ('live','fixture','USDT','2026-10-01 00:00:00+00'::timestamptz,100::numeric,999::numeric,'{}'::jsonb)),\naccount_position_snapshots(environment,account_label,symbol,position_side,observed_at,position_amt,entry_price) AS (VALUES ('live','fixture','BTCUSDT','LONG','2026-10-01 00:00:00+00'::timestamptz,2::numeric,100::numeric),('live','fixture','BTCUSDT','LONG','2026-10-01 00:00:45+00'::timestamptz,0::numeric,0::numeric)),\nruntime_market_states_15s(environment,symbol,bucket_start,bucket_end,mark_price) AS (VALUES ('research','BTCUSDT','2026-10-01 00:00:00+00'::timestamptz,'2026-10-01 00:00:15+00'::timestamptz,110::numeric),('research','BTCUSDT','2026-10-01 00:00:15+00'::timestamptz,'2026-10-01 00:00:30+00'::timestamptz,120::numeric)),\naccount_reconciliation_runs(environment,account_label,status,observed_at,position_count,details,reconciliation_id) AS (VALUES ('live','fixture','ready','2026-10-01 00:00:00+00'::timestamptz,1,'{\"position_keys\":[{\"symbol\":\"BTCUSDT\",\"position_side\":\"LONG\"}]}'::jsonb,'a'),('live','fixture','ready','2026-10-01 00:00:45+00'::timestamptz,0,'{\"position_keys\":[]}'::jsonb,'b'))\n"


@pytest.mark.parametrize(
    "missing_marks, historical_rest", [(False, False), (True, False), (True, True)]
)
async def test_ws_valuation_carries_balance_and_closes_position(
    async_database_url: str, missing_marks: bool, historical_rest: bool
) -> None:
    start = datetime(2026, 10, 1, tzinfo=UTC)
    statement = account_equity_statement(
        environment="live",
        account_label="fixture",
        asset="USDT",
        window_start=start,
        window_end=start + timedelta(seconds=45),
        interval_seconds=15,
    )
    fixture = FIXTURE.replace("'research'", "'unrelated'") if missing_marks else FIXTURE
    if historical_rest:
        fixture = fixture.replace("'{}'::jsonb", '\'{"crossUnPnl":"999"}\'::jsonb')
    sql = fixture + str(
        statement.compile(dialect=dialect(), compile_kwargs={"literal_binds": True})
    )
    sql = sql.replace("\nWITH position_history", "\n, position_history", 1)
    engine = create_async_engine(async_database_url)
    try:
        async with engine.connect() as connection:
            rows = (await connection.execute(text(sql))).all()
    finally:
        await engine.dispose()
    assert [row.wallet_balance for row in rows] == [Decimal("100")] * len(rows)
    assert [row.unrealized_pnl for row in rows] == (
        [Decimal("999"), Decimal("0")]
        if historical_rest
        else [Decimal("0")]
        if missing_marks
        else [Decimal("20"), Decimal("40"), Decimal("0")]
    )
    assert rows[-1].observed_at == start + timedelta(seconds=45)
