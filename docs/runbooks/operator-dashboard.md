# Operator Dashboard

Start the local read-only dashboard with:

```bash
cml-operator-dashboard --database-url "$CML_DATABASE_URL" --host 127.0.0.1 --port 8765
```

Open `http://127.0.0.1:8765/`. Review system freshness, the UTC+0 momentum
universe, selected strategy, read-only account state, risk/execution state,
ambiguous orders, and paper/shadow/live reports.

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
