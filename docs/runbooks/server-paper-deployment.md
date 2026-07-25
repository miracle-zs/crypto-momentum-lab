# Server Paper Deployment

This deployment consumes Binance public USD-M market data and runs the selected
strategy in paper mode. It does not accept Binance credentials and cannot place
orders.

The small-server profile subscribes only to Binance `aggTrade`, which is the
stream required to build OHLC states for compression breakout. Deploy other
strategies with a separate capture profile sized for their required streams.

## Deploy

1. Install Docker Engine with the Compose plugin.
2. Copy the repository to `/opt/crypto-momentum-lab`.
3. Create `/opt/crypto-momentum-lab/.env.server` with mode `0600`:

   ```text
   CML_POSTGRES_PASSWORD=<random-alphanumeric-password>
   ```

4. Build and start the stack:

   ```bash
   docker compose --env-file .env.server -f compose.server.yaml up -d --build
   ```

5. Add `deploy/nginx/crypto-momentum-lab.conf` inside the existing port 80
   server block, validate with `nginx -t`, and reload Nginx.

## Verify

```bash
docker compose --env-file .env.server -f compose.server.yaml ps
docker compose --env-file .env.server -f compose.server.yaml logs --tail=200 market-data paper-trader
curl -fsS http://127.0.0.1:8765/api/health
curl -fsS http://127.0.0.1/momentum/api/health
```

The remote console is available at `http://<server>/momentum/`. Account panels
remain empty because this stack intentionally has no Binance private-account
credentials.

## Paper Artifacts

The paper daemon persists strategy signals, order-intent candidates, and
simulated fills in PostgreSQL. Pending candidates are reloaded after a daemon
restart, and repeated writes are idempotent.

The dashboard separates the paper account into:

- account equity and balance history;
- currently open positions with mark price and unrealized PnL;
- closed trades with net realized PnL;
- a lifecycle ledger labeled `开多`, `开空`, `平多`, or `平空`.

The server profile starts with 1,000 USDT of virtual equity. A filled entry opens
a paper position. Positions close on the first closed 15-second state that
reaches one of these rules:

- take profit at 2%;
- stop loss at 1%;
- maximum holding period of 80 buckets, or 20 minutes.

PnL includes both entry and exit taker fees. The small-server profile evaluates
the closed state's trade close, rather than intrabucket high/low, because it
subscribes only to Binance `aggTrade`.

The small-server `aggTrade` profile has no order-book quotes, so virtual fills
use the closed 15-second state's trade close as the executable reference price.
No order is sent to Binance.

Inspect persisted artifact counts with:

```bash
docker compose --env-file .env.server -f compose.server.yaml exec -T postgres \
  psql -U cml -d cml -c \
  'select run_id, signal_count, candidate_count, fill_count, pending_candidate_count from strategy_runs order by created_at desc limit 5;'
```
