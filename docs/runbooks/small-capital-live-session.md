# Small-Capital Live Session

This runbook enables one selected strategy on one dedicated Binance USD-M
Futures account. The existing paper account for that strategy keeps running
from the same `research` market-state feed. Entries use the same strategy
configuration; execution, balances, positions, and exits remain isolated.

Do not enable the `live` Compose profile on the current 2 GB server. Upgrade it
to at least 4 GB RAM first. The default Compose deployment remains paper-only.

## Binance Account

Create one HMAC API key pair and store it only in an untracked server file or a
secret manager. Enable Futures read/trade, disable withdrawals, and restrict the
key to the server's public IP. Set USD-M Futures to Hedge Mode and make the
account flat before the first session. One key pair is enough for one account.
New entry symbols are explicitly set to the configured leverage, default 5x,
before an order is submitted; a failed leverage confirmation blocks the order.

Create `/opt/crypto-momentum-lab/.env.live` with mode `0600`, using
`.env.live.example` as the field list. Never paste the secret into a command,
Git commit, dashboard, or chat transcript.

All commands below use both environment files:

```bash
set -a
. ./.env.server
. ./.env.live
set +a
COMPOSE="docker compose --env-file .env.server --env-file .env.live -f compose.server.yaml"
```

## 1. Start Read-Only Account Sync

Build the exact Git commit, apply migrations, and start only the authenticated
read-only account service. This does not submit orders.

```bash
$COMPOSE --profile live build
$COMPOSE --profile live up -d execution-account-live
$COMPOSE --profile live ps execution-account-live
```

The service checks Binance account and Hedge Mode every 5 seconds, persists only
active positions, and reconciles fills every 60 seconds. It must report
`healthy` before continuing.

## 2. Prepare Risk Gates

Compute the stable entry-strategy hash, then create a short-lived lease and a
risk snapshot. `prepare` does not call a Binance write endpoint.

```bash
STRATEGY_CONFIG_HASH="$($COMPOSE --profile live run --rm --no-deps \
  live-strategy strategy-config-hash --strategy "$CML_LIVE_STRATEGY")"

$COMPOSE --profile live run --rm --no-deps live-strategy prepare \
  --account-label "$CML_LIVE_ACCOUNT_LABEL" \
  --strategy "$CML_LIVE_STRATEGY" \
  --lease-owner "$CML_LIVE_LEASE_OWNER" \
  --lease-ttl-seconds 1800 \
  --max-order-notional unlimited \
  --max-gross-notional unlimited \
  --max-daily-loss unlimited \
  --max-open-positions unlimited \
  --confirmation "PREPARE LIVE RISK GATES"
```

Record the returned `risk_config_hash`, `strategy_config_hash`, `lease_id`, and
expiry. The live daemon renews its five-minute lease while healthy; if it stops,
the lease expires without another process taking over.

## 3. Run Matching Shadow

The shadow run reads the same `research` states, generates Hedge Mode plans,
and persists each order as terminal `suppressed`. It cannot call a Binance write
endpoint.

```bash
$COMPOSE --profile live run --rm --no-deps \
  --entrypoint cml-shadow-operation live-strategy run \
  --account-label "$CML_LIVE_ACCOUNT_LABEL" \
  --strategy "$CML_LIVE_STRATEGY" \
  --market-environment research \
  --run-id "shadow-${CML_LIVE_SESSION_ID}" \
  --max-runtime-seconds 7200 \
  --require-lease-owner "$CML_LIVE_LEASE_OWNER" \
  --hedge-mode --json
```

Review the report and record all three drills from
`docs/runbooks/shadow-operation-session.md`. A completed matching shadow session
must be less than 24 hours old; this is only a preflight evidence window, not
an order holding-time limit.

## 4. Approve And Preflight

Set `CML_LIVE_STRATEGY_CONFIG_HASH` in `.env.live` to the value from step 2.
Use the exact Git commit and Alembic head from the deployed image. The approval
confirmation is exactly `ENABLE SMALL LIVE TRADING`.

```bash
$COMPOSE --profile live run --rm --no-deps live-strategy approve \
  --account-label "$CML_LIVE_ACCOUNT_LABEL" \
  --strategy "$CML_LIVE_STRATEGY" \
  --strategy-config-hash "$CML_LIVE_STRATEGY_CONFIG_HASH" \
  --risk-config-hash "$RISK_CONFIG_HASH" \
  --git-commit-hash "$CML_CODE_COMMIT" \
  --migration-revision "$CML_LIVE_MIGRATION_REVISION" \
  --notional-cap unlimited --max-open-positions unlimited --max-daily-loss unlimited \
  --approver "$CML_LIVE_OPERATOR" \
  --confirmation "ENABLE SMALL LIVE TRADING"

$COMPOSE --profile live run --rm --no-deps live-strategy preflight \
  --account-label "$CML_LIVE_ACCOUNT_LABEL" \
  --strategy "$CML_LIVE_STRATEGY"
```

## 5. Start Live

The checked-in live Compose profile is wired for the B1 long-only variant:
`orderflow_impulse`, Hedge Mode, one 15-minute grace candle, and a `0.88%`
recovery LIMIT. On the first adverse official closed candle it places a
reduce-only LIMIT at `entry * (1 + 0.0088)` for a long; if that order is still
open at the next 15-minute close, the executor cancels it and market-closes the
remaining quantity. No protective stop is placed after an entry fill.

The following execution protections are intentionally absent from the live
path: maximum holding time (including the old 24-hour fallback), spread
threshold, same-symbol execution cooldown, and market/account data-age gates.
The B1 strategy also sets its event cooldown to zero, so a same-symbol signal
is not suppressed by a hidden two-bucket strategy cooldown. Exchange quantity
and price precision, Hedge Mode/leverage confirmation, the lease/approval
binding, account readiness, and the fail-closed ambiguous-order guard remain
explicit operational controls. Multiple entries for a symbol are allowed when
the strategy emits them; Hedge Mode keeps each position side explicit.

Omitting `--expires-in-minutes` records a non-expiring approval. It remains
bound to the exact strategy config, risk config, Git commit, and migration
revision, so any of those changes require a new approval. `unlimited` removes
the execution-layer order, gross-notional, and open-position-count caps; the
B1 strategy still emits a fixed 100 USDT desired notional for each entry.

The live executor still accepts `fixed` or `candle_15m` for other local runs;
set the corresponding `CML_LIVE_*` values before using a different profile.

```bash
$COMPOSE --profile live up -d live-strategy
$COMPOSE --profile live ps execution-account-live live-strategy
$COMPOSE --profile live logs --tail=200 live-strategy
```

The profile includes the mandatory
`--i-understand-this-places-real-orders` flag. Any manual `run` invocation must
also provide that exact flag; omission fails before credentials are used.

The daemon rejects unowned/manual positions, allows another strategy entry while
the same symbol is already held, restores its checkpoint by stable session ID,
reconciles non-terminal orders by Binance client order ID, and warms a new
session from two hours of historical states without submitting those historical
signals. Active live position symbols remain in the 15-second market-data
subscription even after leaving the momentum pool. Approval, lease, migration,
commit, account readiness, and explicit risk-halt mismatches still stop new
entries; a confirmed resting order does not.

## Drain And Stop

Disable new entries first; managed reduce-only exits continue while the process
is in `draining` state. Draining is sticky for that session ID across container
restarts; use a new session ID only after the account is reconciled flat.

Emergency flatten is a separate audited operator action and requires the exact
confirmation `EMERGENCY FLATTEN LIVE ACCOUNT`. Do not substitute a normal entry
order or disable Hedge Mode while positions are open.

```bash
$COMPOSE --profile live run --rm --no-deps live-strategy disable-new-entries \
  --session-id "$CML_LIVE_SESSION_ID" \
  --operator "$CML_LIVE_OPERATOR" \
  --strategy-config-hash "$CML_LIVE_STRATEGY_CONFIG_HASH" \
  --risk-config-hash "$RISK_CONFIG_HASH"
```

Do not stop account sync or release the lease until Binance positions and open
orders are flat and local reconciliation agrees. Review the final transition:

```bash
$COMPOSE --profile live run --rm --no-deps live-strategy report \
  --session-id "$CML_LIVE_SESSION_ID"
```

Disable the live-submit configuration immediately after the session by stopping
the `live-strategy` service once the account is reconciled flat. Keep the
read-only account sync running until the post-session report is complete.

Start with one position and materially less than the 100 USDT paper notional.
Increase exposure only after several reviewed live sessions have no unresolved
orders, reconciliation mismatch, unexpected exit, or operational halt.
