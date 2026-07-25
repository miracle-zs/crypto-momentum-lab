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
