# Small-Capital Live Session

This runbook controls real-money Binance USD-M Futures execution. Do not run it
until the shadow report is accepted, the account is dedicated to one selected
strategy, and the operator is prepared to stop and reconcile the session.

## Pre-Session Checklist

- Record the Git commit, Alembic revision, strategy config hash, and risk config hash.
- Confirm the execution account is `READY_READONLY`, fresh, and uses the expected margin mode.
- Confirm one active lease is owned by the live worker for the selected strategy.
- Confirm there are no active halts or unresolved/ambiguous exchange orders.
- Confirm fixed notional, one-position, daily-loss, gross-exposure, spread,
  min-notional, cooldown, account-freshness, and market-freshness limits.
- Review a completed shadow session from the previous 24 hours with matching strategy hash.

## Approval And Preflight

The confirmation text is exactly `ENABLE SMALL LIVE TRADING`.

```bash
cml-live-rollout preflight \
  --database-url "$CML_DATABASE_URL" \
  --account-label primary \
  --strategy compression_breakout

# Set STRATEGY_CONFIG_HASH and RISK_CONFIG_HASH from the preflight JSON.
cml-live-rollout approve \
  --database-url "$CML_DATABASE_URL" \
  --account-label primary \
  --strategy compression_breakout \
  --strategy-config-hash "$STRATEGY_CONFIG_HASH" \
  --risk-config-hash "$RISK_CONFIG_HASH" \
  --git-commit-hash "$GIT_COMMIT" \
  --migration-revision 20260704_0010 \
  --notional-cap 25 --max-open-positions 1 --max-daily-loss 10 \
  --approver "$OPERATOR" \
  --confirmation "ENABLE SMALL LIVE TRADING"

cml-live-rollout preflight --database-url "$CML_DATABASE_URL" --account-label primary --strategy compression_breakout
```

## Live Run

`run` continuously consumes newly closed 15-second market states from
PostgreSQL. For every state it reloads approval, lease, account health, halts,
unresolved orders, positions, PnL, exposure, trading rules, and cooldowns before
the strategy can submit an order. It restores and saves the strategy checkpoint
under the session ID. The following flag is the real-money confirmation:
`--i-understand-this-places-real-orders`.

```bash
cml-live-rollout run \
  --database-url "$CML_DATABASE_URL" \
  --account-label primary --strategy compression_breakout \
  --session-id "$SESSION_ID" --operator "$OPERATOR" \
  --lease-owner live-worker \
  --strategy-config-hash "$STRATEGY_CONFIG_HASH" \
  --git-commit-hash "$GIT_COMMIT" \
  --migration-revision 20260704_0010 \
  --max-runtime-seconds 3600 \
  --state-stale-after-seconds 30 \
  --checkpoint-every-states 100 \
  --max-spread 5 --cooldown-seconds 300 \
  --i-understand-this-places-real-orders

cml-live-rollout status --database-url "$CML_DATABASE_URL" --session-id "$SESSION_ID"
```

The separate `submit-plan` command exists for a controlled single-order probe.
Its quantized plan must reference an already persisted risk-approved intent; it
uses the same live gate and explicit real-money confirmation flag.

## Drain And Emergency Controls

First disable new entries and allow only reduce-only orders:

```bash
cml-live-rollout disable-new-entries \
  --database-url "$CML_DATABASE_URL" --session-id "$SESSION_ID" \
  --operator "$OPERATOR" --strategy-config-hash "$STRATEGY_CONFIG_HASH" \
  --risk-config-hash "$RISK_CONFIG_HASH"
```

Cancel and emergency flatten operations require persisted audited command
records with exact confirmations `CANCEL ALL OPEN ORDERS` and
`EMERGENCY FLATTEN LIVE ACCOUNT`. Flatten plans must be reduce-only. Never
release the trading lease until local and Binance positions/open orders agree
and are flat.

## Post-Session

1. Disable the live-submit configuration immediately after the session.
2. Synchronize balances, positions, open orders, and fills from Binance.
3. Confirm account flat, no unresolved orders, and zero reconciliation mismatch.
4. Release the lease only after flat reconciliation.
5. Export the final report and review fees, slippage, PnL, drawdown, halts,
   order states, account-flat confirmation, and lease-release confirmation.

```bash
cml-live-rollout report --database-url "$CML_DATABASE_URL" --session-id "$SESSION_ID"
```

Do not increase capital until multiple reviewed sessions complete without
reconciliation mismatches, unresolved orders, failed rollback controls, or
risk-limit breaches.
