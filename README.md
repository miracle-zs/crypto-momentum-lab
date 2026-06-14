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

## Hourly Service

```bash
docker compose up --build market-data
```

The UTC 00:01 snapshot is recorded but not activated. The previous day's
23:01 universe remains active until the 01:01 snapshot succeeds.
