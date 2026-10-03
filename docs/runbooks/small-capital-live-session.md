# Small-Capital Live Session

This runbook enables Orderflow on a dedicated Binance USD-M Futures account.
The base Compose stack runs data collection and the read-only dashboard; Live
accounts require the explicit `live` profile. No Paper runner or standalone
Shadow CLI remains after the 2026-10-03 cleanup. Account-specific execution,
balances, positions, and exits remain isolated.

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

## Database Planes

The live deployment uses three logical database planes with bounded connection
pools: execution (orders, account, risk, and lease), market (market states and
universe), and observability (checkpoints and runtime telemetry). The default
configuration intentionally points all three at the same PostgreSQL service;
this is connection-pool and write-path isolation, not a second disk.

Leave `CML_EXECUTION_DATABASE_URL`, `CML_MARKET_DATABASE_URL`, and
`CML_OBSERVABILITY_DATABASE_URL` blank on the current cloud server. If a future
deployment gets separate PostgreSQL endpoints, set those variables only after
running the same Alembic migrations against each endpoint. High-frequency
market/strategy telemetry remains in memory for latency summaries; durable
telemetry contains sparse order lifecycle samples plus low-cardinality hub
health and terminal-reason events, and uses best-effort commits.

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
expiry. The live daemon renews its five-minute lease while healthy. If a feed or
worker restart lets that lease expire, a session that has already reached
`live_enabled` may automatically reacquire a short lease after all other gates
still pass; a first startup still requires `prepare`, and an operator-draining
session is never auto-restarted.

Before a planned restart, extend the existing lease for one hour. This command
checks the account, owner, and strategy binding and fails closed if any value is
wrong or the lease is missing:

```bash
$COMPOSE --profile live run --rm --no-deps -T live-strategy renew-lease \
  --account-label "$CML_LIVE_ACCOUNT_LABEL" \
  --strategy "$CML_LIVE_STRATEGY" \
  --lease-owner "$CML_LIVE_LEASE_OWNER" \
  --lease-ttl-seconds 3600 \
  --confirmation "RENEW LIVE RISK LEASE" </dev/null
```

## 3. Validate Before Approval

Use the local research/reconciliation workflow and the fault-injection release
gates in `docs/paper-live-replay-execution-semantics.md`. The standalone Shadow
CLI has been retired. Existing matching-shadow evidence and its advisory
preflight check remain readable; missing evidence follows the existing
acknowledgement rules below. Approval, lease, risk, and real-order confirmation
gates remain mandatory.

## 4. Approve And Preflight

Set `CML_LIVE_STRATEGY_CONFIG_HASH` in `.env.live` to the value from step 2.
Use the exact Git commit and Alembic head from the deployed image. The approval
confirmation is exactly `ENABLE SMALL LIVE TRADING`.

The one-off `preflight` command resolves the Live lane's
`CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT` and `CML_LIVE_ENTRY_POLICY_MODE`
environment values and reports three separate fingerprints:
`runtime_strategy_config_hash` (calculated from those inputs),
`configured_strategy_config_hash` (the value passed to the long-running
service), and `approved_strategy_config_hash` (the database approval). Review
both `runtime_strategy_config_matches_configured` and
`runtime_strategy_config_matches_approval`; do not treat the library defaults
as the production runtime configuration.

When a completed matching Shadow session is intentionally unavailable during
an already-approved temporary rollout, pass
`--acknowledge-missing-shadow-preflight` to the Live `run` command. This does
not create a Shadow record or bypass the Live gate; it changes the advisory
log to an explicit, auditable `info` event. Keep the acknowledgment temporary
and still run a real Shadow preflight before the next rollout change.

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

The checked-in live Compose profile is wired for the Top30/B8 long-only variant:
`orderflow_impulse`, the positive UTC-day gainers ranked 1-30, Hedge Mode, no
EMA5/EMA10 entry filters, eight 15-minute grace candles, a `0.10%` first-candle
direct-close threshold, and a `0.88%` recovery target. On the
first adverse official closed candle, a long whose current executable bid has
already reached `entry * (1 + 0.001)` is closed directly with a reduce-only
MARKET order. Otherwise the executor places a reduce-only LIMIT at
`entry * (1 + 0.0088)`; if it is still open at the next 15-minute close, the
executor cancels it and market-closes the remaining quantity. The 15-minute
candle containing the entry is observation-only and is not eligible to trigger
this logic; evaluation starts with the next complete 15-minute candle. No
protective stop is placed after an entry fill.

The following execution protections are intentionally absent from the live
path: maximum holding time (including the old 24-hour fallback), spread
threshold, same-symbol execution cooldown, and market/account data-age gates.
The Top30/B8 strategy also sets its event cooldown to zero, so a same-symbol signal
is not suppressed by a hidden two-bucket strategy cooldown. Exchange quantity
and price precision, Hedge Mode/leverage confirmation, the lease/approval
binding, account readiness, and the fail-closed ambiguous-order guard remain
explicit operational controls. Multiple entries for a symbol are allowed when
the strategy emits them; Hedge Mode keeps each position side explicit.

Omitting `--expires-in-minutes` records a non-expiring approval. It remains
bound to the exact strategy config, risk config, Git commit, and migration
revision, so any of those changes require a new approval. `unlimited` removes
the execution-layer order, gross-notional, and open-position-count caps; the
Top30/B8 strategy still emits a fixed 100 USDT desired notional for each entry.

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

The command commits the `DRAINING` transition before attempting the
low-latency RiskControlHub push. Set `CML_RISK_CONTROL_HUB_TOKEN` in the
operator environment when the account service is configured with a publish
token. If the push is unavailable, the command reports the PostgreSQL fallback;
the live worker still closes entries when its next durable context refresh
observes the transition.

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

## Multi-account Live rollout

This runbook adds `account-2`, `account-3`, and `account-4` while keeping one
shared `market-data` process. The shared process publishes the same market
state and quote hubs to every Live strategy; each account still gets its own
read-only account synchronizer, trade credential, lease, session, risk
configuration, approval, and account-event hub.

The additional services are defined in
`compose.live.accounts.yaml`. Use it together with `compose.server.yaml`:

```bash
COMPOSE="docker compose --env-file .env.server \
  -f compose.server.yaml -f compose.live.accounts.yaml --profile live"
```

The market-data process protects the union of the configured startup hints and
the labels discovered from each account's latest ready PostgreSQL
reconciliation. A stopped account with an open position therefore remains
protected, while an account whose latest ready reconciliation reports zero
positions is removed from the protected set. `CML_LIVE_POSITION_ACCOUNT_LABELS`
can still be set to provide immediate startup hints for accounts that have not
yet produced their first reconciliation; it no longer has to be kept as an
exhaustive list.

### Account profiles

The strategy parameters are 15-second buckets unless stated otherwise:

| account | impulse window | confirmation | min return | min imbalance | min intensity | min 5m/30m notional | cooldown |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| primary | 2 | 1 | 0.005 | 0.30 | 4.0 | 1.50 | 0 |
| account-2 | 2 | 1 | 0.005 | 0.30 | 4.0 | 1.50 | 0 |
| account-3 | 3 | 1 | 0.015 | 0.30 | 1.5 | 0.00 | 0 |
| account-4 | 3 | 1 | 0.015 | 0.30 | 1.5 | 0.00 | 0 |

The `min 5m/30m notional` value compares the latest 20 consecutive 15-second
states with the immediately preceding 120 states. A value of `0` disables
this optional seventh dimension.

Every profile value is included in the runtime strategy hash. Account 3 and
account 4 may therefore have the same strategy hash because their strategy
profiles are identical; their approvals remain separate because their account
labels, risk limits, credentials, and sessions are different.

### Hash and gate preparation

After the target image is built, generate each additional account's hash from
the account-specific service environment. Do not copy the primary hash by
hand:

```bash
for account in 2 3 4; do
  $COMPOSE run --rm --no-deps live-strategy-account-$account \
    strategy-config-hash \
    --account-label account-$account \
    --runtime-manifest /app/deploy/live-runtime.yaml
done
```

The command derives the hash from the account's `strategy_config` in the
manifest, while the account-specific environment still supplies the manifest's
explicit `${CML_LIVE_*}` references. Store the returned values as
`CML_LIVE_STRATEGY_CONFIG_HASH_ACCOUNT_2/3/4`. Then, one account at a time:

1. validate the desired runtime identity against the checked-in manifest:

   ```bash
   $COMPOSE run --rm --no-deps live-strategy-account-2 \
     preflight --account-label account-2 \
     --runtime-manifest /app/deploy/live-runtime.yaml --strict
   ```

   Use the corresponding service and account label for primary, account-3, or
   account-4. This check compares the selected strategy, image commit,
   migration revision, lease owner, and computed strategy hash before touching
   the live session.
2. run `prepare` with that account's risk limits and the same
   `--runtime-manifest /app/deploy/live-runtime.yaml` option;
3. record an approval with the account hash, risk hash, exact image commit,
   migration revision, and that account's notional/position/loss caps;
4. run the normal `preflight` checks and require runtime/configured/approved
   hashes to match;
5. start only that account's `execution-account` and `live-strategy` pair;
6. observe health, reconciliation, lease renewal, submit/cancel audit pairs,
   and the absence of unexpected entries before moving to the next account.

The long-running `live-strategy` commands include
`--runtime-manifest /app/deploy/live-runtime.yaml`. At startup, `run` loads the
account's typed strategy inputs from that file, derives the strategy hash, and
uses the manifest's session, lease owner, image commit, and migration revision.
An explicitly supplied conflicting value stops the worker before it reaches the
database or exchange.

For example, the first account should be started with explicit service names:

```bash
$COMPOSE up -d \
  execution-account-live-account-2 \
  live-strategy-account-2
```

Do not run `$COMPOSE up -d` without service names during the rollout; that
would enable and start every profile-enabled Live service at once.

The account-specific read and trade variables are:

```text
BINANCE_READ_API_KEY_ACCOUNT_2
BINANCE_READ_API_SECRET_ACCOUNT_2
BINANCE_TRADE_API_KEY_ACCOUNT_2
BINANCE_TRADE_API_SECRET_ACCOUNT_2
```

Use the analogous suffixes for accounts 3 and 4. Do not put secret values in
the repository, and do not use one account's key pair for another account.

Do not start all three new Live strategies merely because the Compose file
validates. A missing hash, approval, lease, account readiness state, or
protected-position label must keep that account stopped without affecting the
other accounts.
