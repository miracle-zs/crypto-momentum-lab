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
- `market_data_connection_task_not_alive` records;
- RSS/cgroup memory growth of at least 64 MiB in a 30-minute window;
- missing `pg_stat_statements`, disabled I/O timing, or re-enabled parallel
  maintenance.

Install or refresh it after pulling a release:

```bash
install -D -m 0755 deploy/ops/cml_ops_monitor.py \
  /opt/crypto-momentum-lab/deploy/ops/cml_ops_monitor.py
install -D -m 0644 deploy/ops/cml-ops-monitor.service \
  /etc/systemd/system/cml-ops-monitor.service
install -d -m 0750 /etc/crypto-momentum-lab
# Copy the example to ops-monitor.env and add a webhook only if desired.
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
RSS trend samples. It never changes Docker or PostgreSQL state.
