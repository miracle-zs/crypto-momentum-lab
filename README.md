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

## Strategy Replay

```bash
.venv/bin/cml-strategy-runner replay \
  --strategy compression_breakout \
  --states-root data/derived/market_states_15s \
  --output reports/compression-breakout-replay.json \
  --execution-latency-buckets 1 \
  --taker-fee-rate 0.0004 \
  --slippage-bps 0
```

Replay reports are deterministic JSON artifacts. They include standardized
signals, order-intent candidates, and cost-aware simulated fills using the
configured latency, taker fee, spread, and slippage assumptions. Use
`--no-simulate-fills` to produce a signal/candidate-only report.

## Paper Trading Runner

```bash
.venv/bin/cml-strategy-runner paper \
  --strategy compression_breakout \
  --states-root data/derived/market_states_15s \
  --output reports/compression-breakout-paper.json \
  --execution-latency-buckets 1 \
  --taker-fee-rate 0.0004 \
  --slippage-bps 0
```

Paper mode reuses the same strategy core and writes a local JSON report with
signals, order-intent candidates, and simulated paper fills. It does not connect
to a Binance account or submit real orders.

PostgreSQL persistence is opt-in. The default command above writes only the
local JSON report. To persist the paper-run artifacts after a successful JSON
write:

```bash
.venv/bin/cml-strategy-runner paper \
  --strategy compression_breakout \
  --states-root data/derived/market_states_15s \
  --output reports/compression-breakout-paper.json \
  --execution-latency-buckets 1 \
  --taker-fee-rate 0.0004 \
  --slippage-bps 0 \
  --persist \
  --database-url "$CML_DATABASE_URL"
```

### Paper Live Source

After `market-data` is writing closed runtime states to PostgreSQL, run a
bounded paper session directly from those rows:

```bash
.venv/bin/cml-strategy-runner paper-live-source \
  --strategy compression_breakout \
  --database-url "$CML_DATABASE_URL" \
  --environment research \
  --output reports/compression-breakout-paper-live-source.json \
  --max-states 1000 \
  --idle-timeout-seconds 60 \
  --persist
```

This is still simulated paper execution. It does not connect to Binance private
APIs, read account state, submit real orders, or enforce a live risk engine.

## Server Paper Deployment

The production-style paper stack includes PostgreSQL, migrations, an initial
universe refresh, public Binance market-data capture, three independent paper
strategy daemons, and the read-only operator dashboard. It never receives
Binance private API credentials and cannot place orders.

The server compression profile evaluates 20 closed 5-minute bars, representing
a 100-minute compression window. The order-flow and liquidation profiles use
15-second states. Raw 15-second states remain the execution and risk-monitoring
clock for all three accounts.

```bash
cp .env.server.example .env.server
# Replace the placeholder with a random alphanumeric PostgreSQL password.
docker compose --env-file .env.server -f compose.server.yaml up -d --build
```

See `docs/runbooks/server-paper-deployment.md` for Nginx setup and verification.
