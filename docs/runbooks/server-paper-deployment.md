# Server Paper Deployment

This deployment consumes Binance public USD-M market data and runs three
independent strategies in paper mode. The default profile does not accept
Binance credentials and cannot place orders. The opt-in `live` profile is
documented separately in `small-capital-live-session.md`.

The server profile subscribes to `aggTrade`, `bookTicker`, and `forceOrder`.
`aggTrade` feeds all three strategies, `bookTicker` supplies executable bid/ask
prices, and `forceOrder` feeds the liquidation strategy. Candle exits load
immutable official UTC-aligned 15-minute klines from Binance REST only when
positions require them; one-minute klines are not continuously subscribed or
archived.

The compression-breakout daemon keeps 15-second states for execution and risk
monitoring, and evaluates the original 15-second breakout profile:

- 20 15-second buckets, or 5 minutes, in the frozen compression range;
- maximum range width of 0.5%;
- minimum breakout distance of 0.1%;
- one closed 15-second bucket for acceptance;
- 8 15-second buckets, or 2 minutes, of per-symbol cooldown.

The eight virtual accounts are isolated by run ID and each starts with 1,000
USDT. Every strategy has one fixed-exit account and two independent
15-minute candle-exit variants:

- `paper-account-01-compression-original-fixed-v1`: `compression_breakout`, fixed TP/SL;
- `paper-account-02-compression-original-candle15m-v1`: `compression_breakout`, first adverse 15M close;
- `paper-account-03-orderflow-fixed-v1`: `orderflow_impulse`, fixed TP/SL;
- `paper-account-04-orderflow-candle15m-v1`: `orderflow_impulse`, first adverse 15M close;
- `paper-account-05-orderflow-candle45m-v1`: `orderflow_impulse`, first adverse 15M close after 45 minutes;
- `paper-account-06-liquidation-fixed-v1`: `liquidation_cascade`, fixed TP/SL;
- `paper-account-07-liquidation-candle15m-v1`: `liquidation_cascade`, first adverse 15M close;
- `paper-account-08-liquidation-candle2confirm-v1`: `liquidation_cascade`, two consecutive adverse 15M closes.

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
   data to become healthy, then recreate each strategy pair:

   ```bash
   docker compose --env-file .env.server -f compose.server.yaml build
   docker compose --env-file .env.server -f compose.server.yaml up -d --no-deps market-data
   docker compose --env-file .env.server -f compose.server.yaml ps market-data
   docker compose --env-file .env.server -f compose.server.yaml up -d --no-deps \
     paper-compression-pair paper-orderflow-pair paper-liquidation-pair dashboard
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
  market-data paper-compression-pair paper-orderflow-pair paper-liquidation-pair
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
Binance private-account credentials; the eight paper-account panels remain
active.

## Paper Artifacts

The paper daemon persists strategy signals, order-intent candidates, and
simulated fills in PostgreSQL. Pending candidates are reloaded after a daemon
restart, and repeated writes are idempotent.

Realtime paper commands use zero additional execution buckets: after a
strategy consumes a newly closed 15-second state, a market candidate is filled
immediately using that state's executable bid or ask. This matches the live
order path. It does not remove the inherent 15-second aggregation delay; a
signal that depends on a bucket is only known when that bucket closes.

The dashboard separates the eight paper accounts by strategy and exit mode into:

- account equity and balance history;
- currently open positions with mark price and unrealized PnL;
- closed trades with net realized PnL;
- a lifecycle ledger labeled `开多`, `开空`, `平多`, or `平空`.

The overview response stays bounded for normal polling. Select an account and
use `查看全部历史` to load its complete closed-trade and lifecycle history on
demand.

Each account starts with 1,000 USDT of virtual equity and opens 100 USDT per
filled entry. A filled entry opens a paper position. The original Compression
fixed account closes on the first closed 15-second state that reaches one of
these rules:

- take profit at 2%;
- stop loss at 1%;
- maximum holding period of 80 execution buckets, or 20 minutes.

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
