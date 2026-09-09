\set ON_ERROR_STOP on
BEGIN READ ONLY;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '1s';
SELECT now() AS sampled_at;
SELECT s.relname, pg_size_pretty(pg_total_relation_size(s.relid)) AS total_size,
       s.n_live_tup, s.n_dead_tup, c.reltuples, c.reloptions,
       50 + 0.2 * c.reltuples AS default_vacuum_threshold,
       s.last_autovacuum, s.last_autoanalyze, s.autovacuum_count
FROM pg_stat_user_tables s JOIN pg_class c ON c.oid = s.relid
WHERE s.relname IN ('account_balance_snapshots', 'account_position_snapshots',
                   'universe_entries', 'strategy_runtime_events',
                   'paper_positions', 'exchange_orders')
ORDER BY pg_total_relation_size(s.relid) DESC;
SELECT name, setting, unit FROM pg_settings
WHERE name IN ('autovacuum_vacuum_threshold','autovacuum_vacuum_scale_factor',
               'autovacuum_analyze_threshold','autovacuum_analyze_scale_factor',
               'autovacuum_work_mem','maintenance_work_mem');
SELECT pid, state, wait_event_type, wait_event, now()-xact_start AS transaction_age
FROM pg_stat_activity WHERE xact_start < now()-interval '1 minute';
SELECT * FROM pg_stat_progress_vacuum;
-- SELECT-only equivalent of hourly thinning candidate selection, not DELETE.
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
SELECT candidate.ctid
FROM account_balance_snapshots AS candidate
WHERE candidate.environment = 'live' AND candidate.account_label = 'primary'
  AND candidate.observed_at >= now()-interval '370 days'
  AND candidate.observed_at < now()-interval '7 days'
  AND EXISTS (
    SELECT 1 FROM account_balance_snapshots AS newer
    WHERE newer.environment = candidate.environment
      AND newer.account_label = candidate.account_label
      AND newer.asset = candidate.asset
      AND date_trunc('hour', newer.observed_at AT TIME ZONE 'UTC') =
          date_trunc('hour', candidate.observed_at AT TIME ZONE 'UTC')
      AND newer.observed_at >= now()-interval '370 days'
      AND newer.observed_at < now()-interval '7 days'
      AND newer.observed_at > candidate.observed_at
  )
ORDER BY candidate.observed_at LIMIT 250;
COMMIT;
