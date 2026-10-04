# 告警、Server 酱与只读看板

核对日期：2026-10-04。监控不参与交易准入；自动定向重启仅用于已说明的心跳失效策略。

The single-host deployment runs a small host-side monitor instead of adding a
Prometheus stack to the trading machine. It samples Docker lifecycle/memory
state, recent structured application logs, and PostgreSQL telemetry freshness.
Alerts are JSON records in journald. An HTTPS webhook can be enabled through
`/etc/crypto-momentum-lab/ops-monitor.env`; no webhook is required for the
trading services to run.

The monitor alerts on:

- PostgreSQL/container OOM and high cgroup memory usage;
- `live_runtime_telemetry_persist_failed` batches;
- stale live checkpoint, current session activity or local heartbeat for the configured live
  run; expired leases and code-identity differences are not readiness evidence;
- stale local heartbeat on a live strategy: the monitor raises an
  account-specific critical alert and, by default, restarts only that
  `live-strategy[-account]` service;
- `market_data_connection_task_not_alive` records;
- container memory growth of at least 64 MiB over the retained 30-minute
  trend window for three consecutive checks; the sample prefers cgroup
  `memory.current` and falls back to Docker's working-set value. Trend
  samples are scoped to the current container ID, so a Compose recreation
  starts a fresh baseline instead of inheriting the previous container's
  warm-up spike;
- cgroup memory-pressure counter advances, including current swap and peak
  values in the alert details;
- missing `pg_stat_statements`, disabled I/O timing, or re-enabled parallel
  maintenance;
- `live_local_fact_inconsistency`: local projection or position repair blocked/failed
  (scoped to specific account and symbol, avoids global halt);
- `live_exit_evaluation_deferred`: exit evaluation repeatedly deferred or waiting for sync;
- `live_candidate_expired`: candidate signal expired before execution (zero exchange POST);
- `live_order_command_terminal_mismatch`: exchange order reached terminal state (`CANCELED`/`FILLED`)
  while execution command remains non-terminal (`ACKNOWLEDGED`);
- `live_unknown_orders`: age-based severity escalation (warning when < 60s, critical when >= 60s,
  bypassing cooldown on escalation). Recovery triggers automatically upon database state convergence,
  emitting a single recovery notification with cumulative occurrence counts.

To deliver the alert and recovery messages through Server酱, put the SendKey
in `/etc/crypto-momentum-lab/ops-monitor.env` as `SERVERCHAN_SENDKEY`. The
monitor supports both the `SCT...` Turbo key and the `sctp...` Server酱³ key;
the key is never written to Git or logs. A configured Server酱 key takes
precedence over `CML_ALERT_WEBHOOK_URL`.

For host-loss detection, configure an HTTPS endpoint outside the trading host:

```dotenv
CML_OPS_EXTERNAL_HEARTBEAT_URL=https://monitor.example.net/cml/heartbeat
CML_OPS_EXTERNAL_HEARTBEAT_TOKEN=<dedicated-heartbeat-token>
CML_OPS_EXTERNAL_HEARTBEAT_TIMEOUT_SECONDS=5
```

The monitor sends one authenticated `POST` per check with a short JSON status
payload and a `Bearer` token in the header. The token is never included in the
JSON payload or logs. The external checker should return a 2xx response, verify
the token, alert after at least three missed intervals, and have no credentials
or write access to PostgreSQL, Docker, Binance, or the operator API. A warning
or critical status in an otherwise delivered heartbeat should also alert; the
heartbeat is a liveness signal, not a replacement for local diagnosis.

Server酱消息使用北京时间，并分成“告警”和“恢复”两类。告警正文先给出
账户/服务、影响和已执行的处置，再附上内部事件编号和 JSON 技术详情；恢复消息
会给出恢复时间和本次异常持续时长。例如：

```text
CML | 严重 | account-2 | 实时策略心跳过期

[严重] account-2：实时策略心跳过期
- 发生时间：2026-09-11 18:15:00（北京时间）
- 影响：该账户的行情处理和开平仓任务可能已经停止。
- 处置：已触发定向重启（第 1 次），等待健康检查恢复。
- 事件编号：live_heartbeat_stale:account-2
```

告警同时保留在 journald 中。Server酱负责通知，不代表每一条普通交易日志或
策略信号都会单独推送；整机断电/断网仍需要 trading host 之外的第二个监控源。

Live heartbeat recovery is bounded per account and per stale incident: it
waits 15 minutes between restart attempts and stops after three attempts. A
successful health check clears the stale/recovery alerts and resets that
account's restart budget. Set
`CML_AUTO_RESTART_STALE_LIVE_SERVICES=false` in
`/etc/crypto-momentum-lab/ops-monitor.env` to keep this path alert-only; the
cooldown and attempt limit can be changed with
`CML_LIVE_RESTART_COOLDOWN_SECONDS` and `CML_LIVE_RESTART_MAX_ATTEMPTS`.

Install or refresh it after pulling a release:

```bash
install -D -m 0755 deploy/ops/cml_ops_monitor.py \
  /opt/crypto-momentum-lab/deploy/ops/cml_ops_monitor.py
install -D -m 0644 deploy/ops/cml-ops-monitor.service \
  /etc/systemd/system/cml-ops-monitor.service
install -d -m 0750 /etc/crypto-momentum-lab
# Copy the example to ops-monitor.env, set SERVERCHAN_SENDKEY, then protect it:
test -e /etc/crypto-momentum-lab/ops-monitor.env || \
  install -D -m 0600 deploy/ops/ops-monitor.env.example \
    /etc/crypto-momentum-lab/ops-monitor.env
# Edit the installed file and add the SendKey; do not put it in Git.
systemctl daemon-reload
systemctl enable --now cml-ops-monitor.service
```

Inspect the latest checks with:

```bash
journalctl -u cml-ops-monitor.service -n 100 --no-pager
systemctl status cml-ops-monitor.service --no-pager
```

The monitor keeps a small state file at
`/var/lib/crypto-momentum-lab/ops-monitor.json` for alert de-duplication,
container-memory trend samples (including their container IDs), cgroup
pressure counters, and per-account restart budgets. Container high-memory and
pressure checks remain independent of the trend-baseline reset. It changes
Docker state only by restarting the affected live strategy when the bounded
recovery path above is enabled; it never changes PostgreSQL state.

This monitor runs on the trading server itself. It can notify when the
`live-strategy[-account]` container is missing, unhealthy, OOM-killed, or no
longer producing a fresh live checkpoint. If the entire server loses power or
network connectivity, a second monitor outside this host is required to send
that notification.

## Decision SLO telemetry

This runbook describes the low-cardinality telemetry plane used to answer
whether the live decision path is healthy. It deliberately reuses
`strategy_runtime_events`; it does not add Prometheus, a second metrics store,
or a synchronous database call to the trading path.

### Dashboard query

The read-only dashboard exposes:

```text
GET /api/decision-slo?window=1h|6h|24h|7d
```

The response contains:

- `phase_latency`: p50/p95/max milliseconds and sample count for the four
  decision transitions;
- `consumers`: observed events, recovery count, unavailable count, lag-event
  count, last availability, and the last recovery reason for each hub;
- `terminal_reasons`: counts grouped by lane, trigger source, and reason;
- `persisted_event_count` and `truncated`, so a bounded query cannot be
  mistaken for a complete export.

`recovery_count` means a consumer became available after being unavailable.
`unavailable_event_count` counts the preceding degraded transitions; a
historical lag event is not treated as a currently unhealthy consumer when a
later recovery is present.

### Persistence boundary

All live phase points remain available in the bounded in-process telemetry
trace and latency rollup. Only sparse order-lifecycle events are durable. The
events for `candidate_accepted`, `intent_saved`, and the first submit request
carry the completed decision-SLO transition samples:

```text
market_state_received → context_ready
context_ready → candidate_accepted
candidate_accepted → intent_saved
intent_saved → exchange_request_started
```

This preserves the historical decision path without writing every 15-second
market or strategy event to PostgreSQL. `consumer_health` and the low-cardinality
`terminal_reason` rollup are also durable through the best-effort observability
writer. A database outage or a full telemetry queue can still lose diagnostic
events; it must not block or change order execution.

The query reads all persisted runtime events in the selected time window. It
is bounded at 50,000 rows and uses `truncated=true` when more rows are
available. The window is based on `occurred_at`, not dashboard request time.
Migration `20260911_0036` adds the standalone `occurred_at` index used by this
time-bounded query; apply it to every configured observability database before
enabling the endpoint in a deployment.

### Verification

Run the focused checks before a release:

```bash
.venv/bin/python -m pytest -q \
  tests/unit/live_rollout/test_telemetry.py \
  tests/unit/live_rollout/test_telemetry_source.py \
  tests/unit/operator_dashboard/test_queries.py \
  tests/unit/operator_dashboard/test_api.py \
  tests/unit/apps/operator_dashboard/test_main.py
.venv/bin/ruff check src/crypto_momentum_lab/live_rollout/telemetry.py \
  src/crypto_momentum_lab/operator_dashboard \
  tests/unit/live_rollout/test_telemetry.py \
  tests/unit/live_rollout/test_telemetry_source.py
```

A missing or stale SLO response is an observability failure. Telemetry is not
a trading gate; order persistence, genuine quantity reservations and unknown
order recovery retain their own responsibilities.

## Operator Dashboard

Start the local read-only dashboard with:

```bash
cml-operator-dashboard --database-url "$CML_DATABASE_URL" --host 127.0.0.1 --port 8765
```

Open `http://127.0.0.1:8765/`. The dashboard is anonymous unless both
`CML_DASHBOARD_USERNAME` and `CML_DASHBOARD_PASSWORD` are configured. Review system freshness, the UTC+0 momentum
universe, selected strategy, read-only account state, risk/execution state,
ambiguous orders and current live reports. Retained Paper tables describe historical data;
no Paper or Shadow runtime is active.

The strategy section also exposes a `统一起点权益金额变化` panel. Its shared
start is fixed at 2026-08-21 02:45 UTC (北京时间 10:45), carries each account's
latest observation forward on a common grid, and
plots cash-flow-adjusted equity deltas in USDT from zero. The grid starts at
15-minute resolution and widens as needed to keep the history within 240
points. The known live-account deposit of 200 USDT on 2026-08-21 is excluded by
default. Future cash-flow corrections can be supplied with
`CML_DASHBOARD_LIVE_CASH_FLOWS_JSON`, for example:

```json
[{"account_label":"primary","effective_at":"2026-08-21T09:41:19.895915Z","amount":"200","cash_flow_type":"deposit"}]
```

An explicit `[]` disables the default correction. This is a read-only derived
view; it does not rewrite the underlying equity snapshots.

The account page's `四账户资金与风险时序` panel anchors each selected range at
the first daily 08:00 Asia/Shanghai boundary within that range. The API and
browser use the same anchor for bucket alignment, so equity, margin, and
drawdown comparisons share one time origin. Current status uses session/account identity, recent runtime observations and
actual health. Historical leases or an image-commit mismatch do not override a
fresh healthy runtime. Missing observations and stale observations remain
visible; a current alarm is not a synchronized stop instruction.

The dashboard browser never calls Binance directly and never receives API keys,
secrets, or credential environment names. It reads only the local FastAPI API,
which reads PostgreSQL. Dashboard write actions remain disabled. The live CLI
`disable-new-entries` path writes the durable transition first and then pushes
a low-volume RiskControlHub notification. The `cancel-all-open-entries` and
`request-flatten` CLI paths likewise write `live_rollback_commands` first and
are executed by the live worker through its existing order/exit lanes.
