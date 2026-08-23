# PostgreSQL checkpoint latency guardrails

These settings are constraints for the live trading database. The objective is
to reduce the amount of dirty data and application write amplification before
tuning the checkpointer.

## Container shared memory

The server PostgreSQL container reserves `shm_size: 256m`.  Docker's default
64 MiB `/dev/shm` is too small for some `VACUUM (ANALYZE)` and index-maintenance
plans, causing a misleading `No space left on device` even when the data
volume has free space.  Keep parallel maintenance disabled explicitly for a
one-off operation if the plan still approaches the limit; do not compensate
by raising application command timeouts.

## Account snapshot retention

`execution-account-live` retains seven days of balance, position, account
configuration, and reconciliation snapshots by default. The retention task
runs once per hour, deletes at most 1,000 rows per batch and 10,000 rows per
table per cycle, and always preserves the newest row for each account/asset or
account/position key. `account_fill_events` is the execution audit trail and
is not deleted by this task.

The policy is controlled in `compose.server.yaml`:

```text
CML_ACCOUNT_SNAPSHOT_RETENTION_DAYS=7
CML_ACCOUNT_SNAPSHOT_RETENTION_INTERVAL_SECONDS=3600
```

After the first logical cleanup, inspect table sizes and run `VACUUM
(ANALYZE)` during a quiet period. Use `VACUUM FULL` or `pg_repack` only with
an explicit maintenance window because it rewrites the table and takes a
strong lock. Retention is deliberately isolated from the account event and
order submission path.

## Keep the checkpoint write rate smooth

Verify the production values before and after each database change:

```sql
SHOW checkpoint_timeout;
SHOW checkpoint_completion_target;
SHOW max_wal_size;
```

Keep `checkpoint_completion_target = 0.9`. A shorter `checkpoint complete`
log line obtained by lowering this value means a higher write burst, which can
increase intent-to-submit latency on the same cloud disk.

## Measure before changing one variable

Capture deltas over the same representative window:

```sql
SELECT checkpoints_timed, checkpoints_req,
       checkpoint_write_time, checkpoint_sync_time,
       buffers_checkpoint, buffers_clean, buffers_backend,
       maxwritten_clean
FROM pg_stat_bgwriter;

SELECT wal_bytes, wal_fpi, wal_buffers_full
FROM pg_stat_wal;
```

Enable `track_io_timing` and `track_wal_io_timing` only during a planned,
measured change. Compare cloud-disk IOPS, throughput, utilization, and await at
10-second resolution around an actual application timeout.

## Candidate trials after application write reduction

Run one trial at a time and retain the setting only when backend-written pages
and checkpoint latency improve without worsening order-lifecycle latency:

- test `bgwriter_lru_maxpages = 400` while keeping the multiplier at `2.0`;
- at a planned restart, test `shared_buffers = 256MB` and `wal_buffers = 16MB`;
- test `wal_compression = lz4` if the server package supports it.

Do not disable `fsync` or `full_page_writes`, and do not use a larger command
timeout as the primary performance fix.
