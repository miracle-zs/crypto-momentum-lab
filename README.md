# Crypto Momentum Lab

Research and trading infrastructure for independent short-horizon momentum
strategies on Binance USD-M perpetual futures.

The approved architecture is documented in
`docs/superpowers/specs/2026-06-14-project-architecture-design.md`.

## Local Setup

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e ".[dev]"
docker compose up -d postgres
export CML_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml
.venv/bin/alembic upgrade head
```

## One-Shot Universe Refresh

```bash
export CML_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml
export CML_ENVIRONMENT_CONFIG=configs/environments/research.yaml
.venv/bin/cml-market-data refresh-universe
```

## Market Data Service

```bash
docker compose up --build market-data
```

This runs the hourly universe refresh and Binance USD-M WebSocket raw capture
together. Raw archives are written under `data/raw` through the Compose volume:

```bash
find data/raw -name '*.jsonl.zst' -type f
```

The UTC 00:01 universe snapshot is recorded but not activated. The previous
day's 23:01 universe remains active until the 01:01 snapshot succeeds.

## Live Smoke Test

```bash
docker compose up -d postgres
export CML_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml
.venv/bin/alembic upgrade head
export CML_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml
.venv/bin/python scripts/run_market_data_smoke.py --seconds 1800
CML_TEST_ASYNC_DATABASE_URL="$CML_DATABASE_URL" \
  .venv/bin/python -m pytest tests/smoke/test_live_capture_manifest.py -m live -v
```

The smoke run should produce non-empty `aggTrade`, `bookTicker`,
`markPrice@1s`, and `kline_1m` archives with matching PostgreSQL manifests.
`forceOrder` is subscribed but can legitimately remain empty during quiet
market periods.
