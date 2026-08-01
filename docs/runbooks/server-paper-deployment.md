# Server Paper Deployment

This deployment consumes Binance public USD-M market data and runs three
independent strategies in paper mode. It does not accept Binance credentials
and cannot place orders.

The server profile subscribes to `aggTrade`, `bookTicker`, `forceOrder`, and
`kline_1m`. `aggTrade` feeds all three strategies, `bookTicker` supplies the
executable bid/ask for paper fills and marks, `forceOrder` feeds the
liquidation cascade strategy, and closed one-minute klines are aggregated into
official UTC-aligned 15-minute candles for candle exits.

The compression-breakout daemon keeps 15-second states for execution and risk
monitoring, but aggregates them into closed UTC-aligned 5-minute signal bars.
Its initial medium-frequency profile is:

- 20 signal bars, or 100 minutes, in the frozen compression range;
- maximum range width of 2.5%;
- minimum breakout distance of 0.3%;
- one closed 5-minute bar for acceptance;
- 12 signal bars, or 60 minutes, of per-symbol cooldown.

The six virtual accounts are isolated by run ID and each starts with 1,000
USDT. Every strategy has one fixed-exit account and one 15-minute candle-exit
account:

- `paper-account-01-compression-v1`: `compression_breakout`;
- `paper-account-02-orderflow-v1`: `orderflow_impulse`;
- `paper-account-03-liquidation-v1`: `liquidation_cascade`;
- `paper-account-04-compression-candle15m-v1`: `compression_breakout`;
- `paper-account-05-orderflow-candle15m-v1`: `orderflow_impulse`;
- `paper-account-06-liquidation-candle15m-v1`: `liquidation_cascade`.

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

   Do not run the final command until `market-data` reports `healthy`. Strategy
   runners replay every durable state after their checkpoint, so an orderly
   restart catches up instead of silently skipping the interval.

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
more than 120 seconds old. Connection cleanup is capped at 30 seconds so a
stuck socket cannot prevent restart. A restart scans and recovers the durable
raw archive before opening live subscriptions; on a large archive this startup
phase can take several minutes. Compose grants 60 seconds for graceful stop so
archive writers can finalize instead of being killed after Docker's default
10-second grace period. The market-data healthcheck has a 15-minute startup
period for archive recovery.

The remote console is available at `https://<server>/momentum/`. The
exchange-account panel remains empty because this stack intentionally has no
Binance private-account credentials; the six paper-account panels remain
active.

## Paper Artifacts

The paper daemon persists strategy signals, order-intent candidates, and
simulated fills in PostgreSQL. Pending candidates are reloaded after a daemon
restart, and repeated writes are idempotent.

The dashboard separates the three paper accounts into:

- account equity and balance history;
- currently open positions with mark price and unrealized PnL;
- closed trades with net realized PnL;
- a lifecycle ledger labeled `开多`, `开空`, `平多`, or `平空`.

The overview response stays bounded for normal polling. Select an account and
use `查看全部历史` to load its complete closed-trade and lifecycle history on
demand.

Each account starts with 1,000 USDT of virtual equity. A filled entry opens a
paper position. The compression account closes on the first closed 15-second
state that reaches one of these rules:

- take profit at 3%;
- stop loss at 1.5%;
- maximum holding period of 480 execution buckets, or 2 hours.

PnL includes both entry and exit taker fees. All three accounts evaluate the
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
