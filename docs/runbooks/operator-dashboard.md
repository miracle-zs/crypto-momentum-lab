# Operator Dashboard

Start the local read-only dashboard with:

```bash
cml-operator-dashboard --database-url "$CML_DATABASE_URL" --host 127.0.0.1 --port 8765
```

Open `http://127.0.0.1:8765/`. The dashboard is anonymous unless both
`CML_DASHBOARD_USERNAME` and `CML_DASHBOARD_PASSWORD` are configured. Review system freshness, the UTC+0 momentum
universe, selected strategy, read-only account state, risk/execution state,
ambiguous orders, and paper/shadow/live reports.

The paper-account section displays six accounts: one Compression account and
five Orderflow accounts. Equity curves use a shared
rolling 24-hour window and the latest snapshot from each UTC six-minute bucket,
up to 240 points. Pair charts compare only buckets available to both accounts,
normalize both accounts to zero at the common start, and use one y-axis. The
closed-trade table shows the latest 30 rows; its total count and win rate are
calculated from the full run history.

The strategy section also exposes a `统一起点权益金额变化` panel. It uses the
latest first valid 15-minute bucket among the selected accounts as the shared
start, carries each account's latest observation forward on a common 15-minute
grid, and plots cash-flow-adjusted equity deltas in USDT from zero. The known
live-account deposit of 200 USDT on 2026-08-21 is excluded by default. Future
cash-flow corrections can be supplied with
`CML_DASHBOARD_LIVE_CASH_FLOWS_JSON`, for example:

```json
[{"account_label":"primary","effective_at":"2026-08-21T09:41:19.895915Z","amount":"200","cash_flow_type":"deposit"}]
```

An explicit `[]` disables the default correction. This is a read-only derived
view; it does not rewrite the underlying equity snapshots.

Status meanings:

- `UNKNOWN`: required telemetry is missing.
- `STALE`: the last observation exceeds its freshness threshold.
- `HALTED`: a risk halt, failed service, or ambiguous order blocks entry.
- `SHADOW`: live data path is active but exchange writes are suppressed.
- `LIVE`: an explicitly approved live session is enabled.

The dashboard browser never calls Binance directly and never receives API keys,
secrets, or credential environment names. It reads only the local FastAPI API,
which reads PostgreSQL. Write actions remain disabled; every future halt, drain,
cancel, flatten, or lease-release action requires a backend command record and
durable audit event before a route may be enabled.
