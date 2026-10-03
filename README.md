# Crypto Momentum Lab

Research and trading infrastructure for independent short-horizon momentum
strategies on Binance USD-M perpetual futures.

The current implementation is summarized in [docs/current-state.md](docs/current-state.md).
See the [documentation index](docs/README.md) for contracts, runbooks, and research designs.
The [system refactoring blueprint](docs/architecture/system-refactor-blueprint-20260925.md)
is a dated design and implementation record; its production claims apply only to
the snapshots identified in that document. Check `docs/current-state.md` for the
repository baseline and the dates and limits of production observations.

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

This runs the 15-minute universe refresh and Binance USD-M WebSocket raw capture
together. Raw archives are written under `data/raw` through the Compose volume:

```bash
find data/raw -name '*.jsonl.zst' -type f
```

Every successfully persisted universe snapshot is activated, including the
UTC 00:01 rollover snapshot. The server refreshes the ranking every five
minutes, so the active universe updates normally across UTC midnight.

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

## Local Orderflow Research

The active research workflow lives in `local_optimization/`: parameter search,
scenario evaluation, high-frequency MTM equity, and Live/replay reconciliation.
See [the local research runbook](docs/runbooks/local-full-data-optimization.md).

The old Research, Replay/Paper, and standalone Shadow CLIs were retired on
2026-10-03. Historical implementations are available at Git baseline
`02e6581f3bc71feac0f91f84fa405460ea26730f`; they are no longer installed commands.
See [the cleanup plan and verification](docs/plans/2026-10-03-dormant-code-cleanup.md).

## Server Deployment

The base server stack contains PostgreSQL, migration/bootstrap jobs, public
market data, the research collector, and the read-only operator dashboard.
No Paper runner is deployed. The explicit `live` profile adds the primary
account synchronizer and gated Orderflow strategy; `compose.live.accounts.yaml`
adds accounts 2–4.

See [server deployment and updates](docs/runbooks/server-paper-deployment.md)
and [the Live runbook](docs/runbooks/small-capital-live-session.md) for release
identity, approval, preflight, account isolation, and verification.
