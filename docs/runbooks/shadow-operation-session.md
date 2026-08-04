# Shadow Operation Session

Shadow operation exercises live market data, the selected runtime strategy,
read-only account state, the risk gateway, Binance exchange metadata,
quantization, persistence, and reconciliation. No Binance write endpoint is
allowed in this phase.

## Preflight

Record and review:

- current Git commit and strategy config hash;
- Alembic database migration head;
- execution account label and `READY_READONLY` state;
- active trading lease, selected strategy, and required lease owner;
- current risk config hash and numeric limits;
- absence of active global halts and unresolved exchange orders.

Run the read-only account synchronization first:

```bash
cml-execution-account sync-once \
  --account-label primary \
  --hedge-mode \
  --database-url "$CML_DATABASE_URL"
```

Run a bounded shadow session:

```bash
cml-shadow-operation run \
  --account-label primary \
  --strategy compression_breakout \
  --market-environment research \
  --run-id "$RUN_ID" \
  --database-url "$CML_DATABASE_URL" \
  --max-runtime-seconds 7200 \
  --require-lease-owner shadow-preflight \
  --hedge-mode \
  --json
```

Generate the report:

```bash
cml-shadow-operation report \
  --run-id "$RUN_ID" \
  --database-url "$CML_DATABASE_URL" \
  --json
```

Review signal count, approved and rejected intents, rejection reasons,
would-submit and suppression counts, stale/account/risk blocks, min-notional
blocks, latency percentiles, unresolved plans, and drill outcomes. Store the
JSON output with the operator notes before considering small-capital trading.

## Drills

Run halt and restart-related drills against the same session:

```bash
cml-shadow-operation drill --run-id "$RUN_ID" --drill stale_market_data --database-url "$CML_DATABASE_URL"
cml-shadow-operation drill --run-id "$RUN_ID" --drill process_restart_with_active_lease --database-url "$CML_DATABASE_URL"
cml-shadow-operation drill --run-id "$RUN_ID" --drill order_submission_ambiguity --database-url "$CML_DATABASE_URL"
```

Any unexpected exchange write attempt, missing suppression, stale account,
expired lease, unresolved plan, failed drill, or active halt fails the session.
