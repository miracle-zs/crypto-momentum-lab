# Multi-account Live rollout

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

Before starting any additional service, set
`CML_LIVE_POSITION_ACCOUNT_LABELS=primary,account-2,account-3,account-4` in the
server environment. The market-data process must protect the union of all
four accounts' open-position symbols. Leaving this as the old single-label
value is not safe for a multi-account rollout.

## Account profiles

The strategy parameters are 15-second buckets unless stated otherwise:

| account | impulse window | confirmation | min return | min imbalance | min intensity | cooldown |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| primary | 3 | 1 | 0.01 | 0.40 | 2 | 0 |
| account-2 | 2 | 1 | 0.005 | 0.50 | 3.0 | 0 |
| account-3 | 2 | 1 | 0.005 | 0.50 | 2 | 0 |
| account-4 | 2 | 1 | 0.005 | 0.50 | 2 | 0 |

Every profile value is included in the runtime strategy hash. Account 3 and
account 4 may therefore have the same strategy hash because their strategy
profiles are identical; their approvals remain separate because their account
labels, risk limits, credentials, and sessions are different.

## Hash and gate preparation

After the target image is built, generate each additional account's hash from
the account-specific service environment. Do not copy the primary hash by
hand:

```bash
for account in 2 3 4; do
  $COMPOSE run --rm --no-deps live-strategy-account-$account \
    strategy-config-hash --strategy orderflow_impulse
done
```

Store the returned values as
`CML_LIVE_STRATEGY_CONFIG_HASH_ACCOUNT_2/3/4`. Then, one account at a time:

1. run `prepare` with that account's risk limits;
2. record an approval with the account hash, risk hash, exact image commit,
   migration revision, and that account's notional/position/loss caps;
3. run `preflight` and require runtime/configured/approved hashes to match;
4. start only that account's `execution-account` and `live-strategy` pair;
5. observe health, reconciliation, lease renewal, submit/cancel audit pairs,
   and the absence of unexpected entries before moving to the next account.

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
