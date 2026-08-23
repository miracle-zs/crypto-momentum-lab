# Runtime market-state partitioning runbook

This is the second phase of the PostgreSQL write-amplification work. It is
intentionally not an automatic Alembic migration: the current table is a live
1+ GB relation, and converting it in place would require a long rewrite or a
carefully controlled dual-write cutover.

## Preconditions

- The compact live checkpoint and recovery-window tests are deployed and have
  been observed for at least one representative trading hour.
- `runtime_market_states_15s` has a durable archive/replay source outside the
  operational table.
- The live process can recover its bounded warmup window without reading rows
  older than the configured recovery horizon.
- A low-traffic maintenance window and a tested rollback/restore path exist.

## Target shape

Partition `runtime_market_states_15s` by `bucket_start`, using six-hour
partitions as the initial operating point. Keep the existing primary-key
columns and the polling/latest-bucket indexes required by the live readers.
Do not recreate the redundant `(environment, symbol, bucket_start)` index.

Create future partitions before they are needed. At retention time, detach the
expired partition, verify it is outside the recovery horizon, and drop the
detached table. This replaces continuous 1,000-row deletes plus vacuum/index
churn with metadata-level removal.

## Cutover outline

1. Stop or drain market-state writers and record the latest durable watermark.
2. Create the partitioned replacement with the same columns, constraints, and
   only the required indexes.
3. Copy rows newer than the recovery horizon in bounded batches, validating
   counts and `(environment, symbol, bucket_start)` uniqueness.
4. Pause writers for the short rename/cutover lock; rename the old table and
   replacement, then resume writers.
5. Keep the old table detached and recoverable until one full retention cycle
   and one restart drill have passed.
6. Replace `prune_runtime_market_states` with detach/drop of expired
   partitions. Do not run both row deletes and partition drops for the same
   relation.

The exact DDL and cutover timing must be generated from the deployed schema and
verified with `EXPLAIN` in staging before production execution.
