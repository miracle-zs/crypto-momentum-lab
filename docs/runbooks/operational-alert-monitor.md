# Operational alert monitor

The single-host deployment runs a small host-side monitor instead of adding a
Prometheus stack to the trading machine. It samples Docker lifecycle/memory
state, recent structured application logs, and PostgreSQL telemetry freshness.
Alerts are JSON records in journald. An HTTPS webhook can be enabled through
`/etc/crypto-momentum-lab/ops-monitor.env`; no webhook is required for the
trading services to run.

The monitor alerts on:

- PostgreSQL/container OOM and high cgroup memory usage;
- `live_runtime_telemetry_persist_failed` batches;
- stale live checkpoint, session transition, or lease for the configured live
  run (order-lifecycle telemetry is intentionally not used as a heartbeat);
- stale local heartbeat on a live strategy: the monitor raises an
  account-specific critical alert and, by default, restarts only that
  `live-strategy[-account]` service;
- `market_data_connection_task_not_alive` records;
- RSS/cgroup memory growth of at least 64 MiB in a 30-minute window;
- missing `pg_stat_statements`, disabled I/O timing, or re-enabled parallel
  maintenance.

To deliver the alert and recovery messages through Server酱, put the SendKey
in `/etc/crypto-momentum-lab/ops-monitor.env` as `SERVERCHAN_SENDKEY`. The
monitor supports both the `SCT...` Turbo key and the `sctp...` Server酱³ key;
the key is never written to Git or logs. A configured Server酱 key takes
precedence over `CML_ALERT_WEBHOOK_URL`.

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
`/var/lib/crypto-momentum-lab/ops-monitor.json` for alert de-duplication and
RSS trend samples and per-account restart budgets. It changes Docker state
only by restarting the affected live strategy when the bounded recovery path
above is enabled; it never changes PostgreSQL state.

This monitor runs on the trading server itself. It can notify when the
`live-strategy[-account]` container is missing, unhealthy, OOM-killed, or no
longer producing a fresh live checkpoint. If the entire server loses power or
network connectivity, a second monitor outside this host is required to send
that notification.
