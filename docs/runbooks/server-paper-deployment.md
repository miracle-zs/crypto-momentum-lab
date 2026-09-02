# Server Paper Deployment

This deployment consumes Binance public USD-M market data and runs two active
strategy families in paper mode. The default profile does not accept
Binance credentials and cannot place orders. The opt-in `live` profile is
documented separately in `small-capital-live-session.md`.

The server profile subscribes to `aggTrade`, `bookTicker`, and `forceOrder`.
`aggTrade` feeds the active strategies, `bookTicker` supplies executable bid/ask
prices, and `forceOrder` remains captured for liquidation research and risk
context even though no Liquidation trading account is active. Candle exits load
immutable official UTC-aligned 15-minute klines from Binance REST only when
positions require them; one-minute klines are not continuously subscribed or
archived.

The compression-breakout daemon keeps 15-second states for execution and risk
monitoring, while entry signals use the frozen one-minute shadow profile:

- 60 one-minute buckets, or 60 minutes, in the frozen compression range;
- maximum range width of 2.5%;
- minimum breakout distance of 0.3%;
- two closed one-minute buckets for acceptance;
- 60 one-minute buckets, or 60 minutes, of per-symbol cooldown.

The eight active virtual accounts are isolated by run ID and each starts with
1,000 USDT:

- `paper-account-05-orderflow-candle15m-v1`: `orderflow_impulse`, existing first adverse 15M close account;
- `paper-account-10-orderflow-b2-long-candle15m-v1`: B2 long-only signals, first adverse 15M close;
- `paper-account-12-orderflow-b1-long-candle15m-v1`: B1 long-only signals, entry-price limit for one 15M bar after the first adverse close;
- `paper-account-13-orderflow-b8-long-candle15m-v1`: B8 long-only signals, entry-price limit for eight 15M bars after the first adverse close.
- `paper-account-14-orderflow-b1-gainer100-v1`: positive Top100 gainer, long-only, B1 exit;
- `paper-account-15-orderflow-b1-gainer100-ema-v1`: positive Top100 gainer, long-only, EMA5/EMA10 filter, B1 exit;
- `paper-account-16-orderflow-b8-gainer10-imbalance040-v1`: positive Top10 gainer, long-only, minimum aggressive imbalance `0.40`, B8 exit;
- `paper-account-17-orderflow-b1-gainer10-imbalance040-v1`: positive Top10 gainer, long-only, minimum aggressive imbalance `0.40`, B1 exit.

These versioned run IDs replace the previous `0.50`-threshold runs so their
configuration hashes and performance histories remain separate. The previous
run IDs are retained for historical analysis.

For all `candle_15m` exits, the candle containing the entry is observation-only;
the first eligible exit candle is the next complete 15-minute candle.

The previously deployed Compression, 45-minute, and C1 imbalance accounts are
kept in the database for historical analysis but are no longer active runners.

No Liquidation trading account is deployed. The preregistered C0/C1/C2 replay
found no candidate that passed both train and validation gates; see
`docs/research/liquidation-entry-replay-study-2026-08-11.md`.

The B1, B2, and B8 filters are applied after the shared baseline Orderflow decision.
Rejected signals still advance the baseline strategy cooldown, so these accounts
remain strict subsets of the same signal stream used by the historical filter
study. Accounts 16 and 17 additionally share one positive Top10 gainer entry
universe and a `0.40` minimum aggressive imbalance, which makes their
B8-versus-B1 comparison synchronous.

The standalone account 14 runner (`paper-account-14-orderflow-b1-gainer100-v1`)
uses a `0.40` minimum aggressive imbalance and explicitly disables EMA5/EMA10
entry filters. Account 15 keeps the EMA5/EMA10 filters. Their previous run
metadata must be migrated or versioned when the strategy configuration hash
changes; do not silently reuse a mismatched run.

## Deploy

1. Install Docker Engine with the Compose plugin.
2. Copy the repository to `/opt/crypto-momentum-lab`.
3. Resolve the exact commit that will be deployed and create
   `/opt/crypto-momentum-lab/.env.server` with mode `0600`:

   ```text
   CML_POSTGRES_PASSWORD=<random-alphanumeric-password>
   CML_CODE_COMMIT=<exact-git-commit-used-for-the-image>
   ```

   Run `git rev-parse HEAD` in the checkout to obtain the commit value. The
   compose build passes it into the image and the paper runners persist it in
   their runtime identity; deployment fails closed when it is omitted.

4. Build and start the stack for the first deployment:

   ```bash
   docker compose --env-file .env.server -f compose.server.yaml up -d --build
   ```

   For later upgrades, build first and recreate services in stages. Keep the
   old strategy containers running while market data restarts, wait for market
   data to become healthy, then recreate each strategy service:

   ```bash
   docker compose --env-file .env.server -f compose.server.yaml build
   docker compose --env-file .env.server -f compose.server.yaml up -d --no-deps market-data
   docker compose --env-file .env.server -f compose.server.yaml ps market-data
  docker compose --env-file .env.server -f compose.server.yaml up -d --no-deps \
     paper-orderflow-pair dashboard
   ```

   Do not run the final command until `market-data` reports `healthy`. Each
   strategy runner starts at its process creation time, restores open positions
   and its latest strategy checkpoint, and consumes only newly closed market
   states. A gap resets that symbol's warm-up cache; the online service never
   synthesizes historical entries or exits.

   The server capture subscribes to `aggTrade`, `bookTicker`, and `forceOrder`
   for live state generation, but raw-file archival is limited to
   `forceOrder`. Strategy runners consume the aggregated 15-second PostgreSQL
   states, so excluding the two high-volume streams from raw archival does not
   change entries, fills, position marking, or exits.

5. Add `deploy/nginx/crypto-momentum-lab.conf` inside the existing HTTPS
   server block, validate with `nginx -t`, and reload Nginx. The dashboard is
   anonymous by default, so expose it only over TLS or a private tunnel/VPN.

## Verify

```bash
docker compose --env-file .env.server -f compose.server.yaml ps
docker compose --env-file .env.server -f compose.server.yaml logs --tail=200 \
  market-data paper-orderflow-pair
curl -fsS http://127.0.0.1:8765/api/health
curl -fsS http://127.0.0.1/momentum/api/health
```

The `market-data` healthcheck requires a recent 15-second market-state row.
Each paper runner healthcheck requires a recent durable checkpoint for its run
ID. Docker's restart policy reacts to process exit, not health status alone; an
alive container that becomes `unhealthy` must be investigated and explicitly
restarted. The application exits on its own watchdog failures so the restart
policy can handle normal market-data stalls.

The market-data process fails and lets Docker restart it when a 15-minute
universe refresh exceeds 120 seconds, when no live market state arrives within
120 seconds after startup, or when the latest market-state watermark becomes
more than 120 seconds old. Shutdown first cancels subscription-management
tasks, closes WebSocket connections concurrently, keeps the archive consumer
running until its bounded queue is empty, and then finalizes open writers. This
cleanup is capped at 55 seconds inside Compose's 60-second stop grace period.
A restart scans and recovers any interrupted raw archives before opening live
subscriptions; on a large archive this startup phase can take several minutes.
The market-data healthcheck has a 15-minute startup period for archive recovery
and rejects `ready` records written before the current container started. The
process handles both `SIGTERM` and `SIGINT` through this shutdown path. Do not
use `SIGKILL` for planned deployments.

The remote console is available at `https://<server>/momentum/`. The
exchange-account panel remains empty because this stack intentionally has no
Binance private-account credentials; the five active paper-account panels
remain active.

## Paper Artifacts

The paper daemon persists strategy signals, order-intent candidates, and
simulated fills in PostgreSQL. Pending candidates are reloaded after a daemon
restart, and repeated writes are idempotent.

Realtime paper commands use zero additional execution buckets: after a
strategy consumes a newly closed 15-second state, a market candidate is filled
immediately using that state's executable bid or ask. This matches the live
order path. It does not remove the inherent 15-second aggregation delay; a
signal that depends on a bucket is only known when that bucket closes.

The dashboard separates the five active paper accounts by strategy and exit mode into:

- account equity and balance history;
- currently open positions with mark price and unrealized PnL;
- closed trades with net realized PnL;
- a lifecycle ledger labeled `开多`, `开空`, `平多`, or `平空`.

The overview response stays bounded for normal polling. Select an account and
use `查看全部历史` to load its complete closed-trade and lifecycle history on
demand.

Each account starts with 1,000 USDT of virtual equity and opens 100 USDT per
filled entry. A filled entry opens a paper position. The B0 and B2 accounts use
the first adverse completed 15-minute candle as their primary exit. B1 and B8
first close profitably at the warning candle's official close (or the current
executable mark if it has recovered into net profit). Only a net-losing warning
arms a reduce-only recovery limit at 0.58% above entry for long positions (or
0.58% below entry for short positions); a quote touching that limit closes at
the executable quote, and otherwise the account exits at the first executable
mark on the one-bar or eight-bar timeout. All retain the existing 24-hour
maximum-holding safeguard.

PnL includes both entry and exit taker fees. All paper accounts evaluate the
closed state's trade close, rather than intrabucket high/low.

The market-data service subscribes to the 40-symbol momentum universe plus
symbols with open positions in the paper runs listed by
`CML_PAPER_EXIT_RUN_IDS`. Strategy runners continue to allow entries only for
the active 40-symbol universe; protected symbols are consumed only so existing
positions can be marked and exited. Keep the environment variable in
`compose.server.yaml` aligned whenever a paper account is added or renamed.

The server profile has no private account connection, so virtual fills require
the latest bid/ask and use the marketable side of that quote. Candle exits are
triggered only after all 15 official one-minute klines in the UTC-aligned
15-minute interval have reported `closed=true`. No order is sent to Binance.

Inspect persisted artifact counts with:

```bash
docker compose --env-file .env.server -f compose.server.yaml exec -T postgres \
  psql -U cml -d cml -c \
  'select run_id, signal_count, candidate_count, fill_count, pending_candidate_count from strategy_runs order by created_at desc limit 5;'
```
