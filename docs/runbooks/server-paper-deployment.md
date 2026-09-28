# Server Paper Deployment

This deployment consumes Binance public USD-M market data and runs one active
strategy family with two paper accounts in paper mode. The default profile does not accept
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

The two active virtual accounts are isolated by run ID and each starts with
1,000 USDT:

- `paper-account-16-orderflow-b8-gainer10-imbalance040-v1`: positive Top10 gainer, long-only, minimum aggressive imbalance `0.40`, B8 exit;
- `paper-account-17-orderflow-b1-gainer10-imbalance040-v1`: positive Top10 gainer, long-only, minimum aggressive imbalance `0.40`, B1 exit.

The other paper run IDs remain in PostgreSQL for historical analysis, but their
Compose services use the `retired-paper` profile and are not part of the
default server stack.

For all `candle_15m` exits, the candle containing the entry is observation-only;
the first eligible exit candle is the next complete 15-minute candle.
The configured Binance REST source is authoritative for these exits. If its
request, response, or completeness check fails, the runner keeps the position
open, records the source error, and retries on the next backoff window; it does
not synthesize a partial candle or fall back to a different close price.
Positions created before the candle cursor migration have no replay boundary;
the runner checks only the latest complete 15-minute window, persists the first
successful candle as the new cursor, and emits a one-time legacy-cursor warning.

The previously deployed Compression, 45-minute, and C1 imbalance accounts are
kept in the database for historical analysis but are no longer active runners.

No Liquidation trading account is deployed. The preregistered C0/C1/C2 replay
found no candidate that passed both train and validation gates.

The active B1 and B8 filters are applied after the shared baseline Orderflow
decision. Rejected signals still advance the baseline strategy cooldown, so
accounts 16 and 17 remain strict subsets of the same signal stream used by the
historical filter study. They share one positive Top10 gainer entry universe
and a `0.40` minimum aggressive imbalance, which makes their B8-versus-B1
comparison synchronous.

The retired Top100 and baseline variants remain available in the database for
comparison, but they are not restarted by the normal deployment path.

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

   For later upgrades, follow [Fast Server Update](#fast-server-update) below.
   It covers release identity, health-gated service recreation, Live rollout
   fencing, verification, and rollback. Do not use `--remove-orphans`; Live
   account overlays can appear as orphans when only this Compose file is selected.

5. Add `deploy/nginx/crypto-momentum-lab.conf` inside the existing HTTPS
   server block, validate with `nginx -t`, and reload Nginx. The dashboard is
   anonymous by default, so expose it only over TLS or a private tunnel/VPN.

### Deployment timing and efficient verification

The 2026-09-08 deployment took approximately 13 minutes end to end, including
operator checks. Image building took about 82 seconds and recreating the active
paper container plus dashboard took about 23 seconds. Paper readiness took
roughly 2–3 minutes, including the first checkpoint and subsequent probe.
Repeated serial checks and interactive SSH calls that waited after command
completion added avoidable time. Total deployment duration is not service
downtime: existing containers continue running during the build.

For a comparable incremental release, aim for approximately 3–5 minutes; this
is a planning target, not a timeout or availability guarantee. Cold builds,
archive recovery, database load, and checkpoint restoration can take longer.

- Complete code review and local tests before the deployment window. On the
  server, combine independent preflight checks into one bounded SSH operation.
- Build once, restart market-data, and wait for its health before restarting
  affected paper services. Poll health every 5–10 seconds with a bounded
  deadline; inspect logs on failure or timeout instead of restarting repeatedly.
- Collect all affected service health states and image identities together,
  then check recent logs, checkpoint progress, and the HTTP health endpoint.
  Avoid reopening source files or repeating successful checks without new
  evidence. Configure the SSH runner to return when the command exits.
- `start_period: 15m` is the market-data startup failure grace period, not a
  mandatory delay. A successful probe can mark it healthy immediately. Paper
  readiness depends on a successful checkpoint; allow for probe scheduling
  after that checkpoint rather than sleeping for the entire grace period.

## Verify

```bash
docker compose --env-file .env.server -f compose.server.yaml ps
docker compose --env-file .env.server -f compose.server.yaml logs --tail=200 \
  market-data paper-orderflow-gainer10-pair
curl -fsS http://127.0.0.1:8765/api/health
curl -fsS http://127.0.0.1/momentum/api/health
```

The `market-data` healthcheck requires a recent 15-second market-state row.
Each paper runner healthcheck requires a recent durable checkpoint for its run
ID. Docker's restart policy reacts to process exit, not health status alone; an
alive container that becomes `unhealthy` must be investigated and explicitly
restarted. The application exits on its own watchdog failures so the restart
policy can handle normal market-data stalls.

Paper runtime-state readers keep the configured one-second poll interval while
processing a batch and back off only when the durable table is idle, up to three
seconds. This bounds the additional durable-state lag while avoiding repeated
empty queries across the paper processes.

The server Compose manifest runs the market-data, live-account, and live-strategy
database probes every 60 seconds, the research collector probe every 90 seconds,
and paper probes every 60 seconds. The dashboard probe uses the local HTTP
endpoint through Python's standard library with `-S`, so it does not import
the application or database driver for each check.

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
Binance private-account credentials; the two active paper-account panels
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

The dashboard separates the two active paper accounts by strategy and exit mode into:

- account equity and balance history;
- currently open positions with mark price and unrealized PnL;
- closed trades with net realized PnL;
- a lifecycle ledger labeled `开多`, `开空`, `平多`, or `平空`.

The overview response stays bounded for normal polling. Select an account and
use `查看全部历史` to load its complete closed-trade and lifecycle history on
demand.

Each account starts with 1,000 USDT of virtual equity and opens 100 USDT per
filled entry. A filled entry opens a paper position. The active B1 and B8
accounts first close profitably at the warning candle's official close (or the
current executable mark if it has recovered into net profit). Only a net-losing
warning arms a reduce-only recovery limit at 0.58% above entry for long
positions (or 0.58% below entry for short positions); a quote touching that
limit closes at the executable quote, and otherwise the account exits at the
first executable mark on the one-bar or eight-bar timeout. Both retain the
existing 24-hour maximum-holding safeguard.

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

## Fast Server Update

This runbook records the repeatable update path for the server at
`/opt/crypto-momentum-lab`. It reduces operator wait time by validating the
target once, building one image, waiting on Docker health directly, and
avoiding needless container recreation. It does not bypass Live approvals,
leases, reconciliation, or the fail-closed entry gate.

### Why an update can take several minutes

The application image is built on the server. A cold dependency build is the
largest variable cost; the Dockerfile keeps third-party dependencies in a layer
keyed only by `pyproject.toml`, and BuildKit keeps the pip download cache
outside the release layer, so source-only changes rebuild a small local wheel.
Healthchecks retain their 60/90-second steady-state intervals to keep
probe CPU low, but use a 5-second `start_interval` (15 seconds for the long
market-data recovery window) while a container is starting. Stateless
research, paper, and dashboard services stop after 20 seconds; Live services
retain a longer grace period for state and exchange cleanup. The deployment
script treats that grace period as a maximum: it explicitly stops the old Live
containers, polls their captured IDs until they are no longer running, waits
one additional second, and then creates the replacements. It does not add a
fixed wait after an early shutdown. Updating eight Live containers one by one
used to add several minutes even when the code build was cached.

The host must have the Ubuntu `docker-buildx` package installed once so Compose
can use BuildKit/Bake and retain the dependency cache. The package install does
not restart Docker or any application container.

Approval state remains fail-closed. For an ordinary Live update, the existing
approval must already reference the target commit. When the target image is
ready and the active approval should keep its current limits, use the explicit
`--refresh-approvals` option described below; it refreshes only active accounts
and preserves the stored limits and operator fields.

### Normal update

Push the desired commit first and note its full SHA. Use an SSH key or agent
when available. For a password-only host, export `CML_SSH_PASSWORD` in the
current shell; the deployment script passes it to `sshpass` through the
environment and never stores it in the repository or command arguments. From a
workstation with the repository checkout, run:

```bash
deploy/ops/update_server.sh 43.167.191.253 <commit-sha>
```

For a password-only connection:

```bash
export CML_SSH_PASSWORD='your-password'
deploy/ops/update_server.sh 43.167.191.253 <commit-sha> --live
unset CML_SSH_PASSWORD
```

The script acquires a repository-local deployment lock. A second invocation for
the same host exits with code 75 while the first deployment is still running.
It records the last target and completed phase inside `.git`; if an earlier
invocation reached the target checkout but failed during build, health, or
restart, rerun the same command and the empty Git diff is treated as a recovery
run. A completed non-Live target remains a no-op on a later repeat; an explicit
`--live` invocation still performs its approval and lease reconciliation.

New deployment state records the original base commit. Retries classify that
same range, so a failed dashboard-only update does not restart market-data,
research, Paper, or Live. A newer target arriving during a failed rollout also
includes the unfinished range. Legacy state without a base uses the conservative
recovery behavior once. Healthy Live account pairs already using the target
image skip reconciliation on a normal repeat, but every recovery run rechecks
their lease and strict preflight; `--refresh-approvals` additionally refreshes
the approval binding.
All timed external commands receive closed stdin to protect the SSH script input.

The script:

1. fetches the target and requires a clean `main` checkout on the server;
2. fast-forwards to a newer target or safely resets to an explicit ancestor for
   a rollback, then verifies that `HEAD` equals the requested commit;
3. classifies the changed paths and skips the build/restart when a commit only
   changes docs, tests, or operator tooling;
4. resolves the target runtime commit and dashboard image as process-level
   Compose overrides; `.env.server` is not changed yet;
5. validates the merged Compose graph and builds the image once using the
   dependency cache;
6. ensures PostgreSQL is healthy and runs the one-shot migration only when the
   target range changes `alembic.ini` or `alembic/`, then performs the
   volume-ownership check before restarting services;
7. recreates Dashboard and `market-data` in one Compose start wave when needed,
   while keeping their separate health budgets, then updates only the affected
   research and Paper consumers;
8. runs a read-only Live approval precheck after migrations and before any
   non-Live service restart; an explicit approval refresh remains deferred to
   the final Live gate;
9. runs the final Live approval/preflight gate after the non-Live services
   converge, with strict preflight completing before any Live lease is renewed;
10. restarts Live execution services and strategies only after that gate passes;
11. verifies the image and health state of every service it updated, then
   persists the runtime commit and dashboard image to `.env.server`;
12. prints separate remote and client-side timings, the checkout/runtime/image
   commits, and the container health summary.

In addition to phase and Docker-operation timings, the output includes one
`service-timing` record for each affected service during health waiting,
restart, Live graceful stop, and final verification. Each record contains the
phase, operation, service name, status, and elapsed seconds. Services in the
same parallel wave share the wave start time, so use the phase/wave timing for
wall-clock cost and use the service records to identify the slow or failed
member; do not add parallel service timings together.

The script uses bounded Compose operations and an explicit health wait. A
healthy service with the expected image is left in place; a service that must
be updated is recreated with `--force-recreate --no-deps` after migrations and
the volume-ownership check complete. Dashboard and market-data are the only
independent application start wave; research and Paper retain the
market-data-health barrier. The volume initializer only runs its recursive
`chown` when the mounted data directories are not owned by `cml`.

The default health wait is 300 seconds for the dashboard, PostgreSQL,
research/Paper, and Live services. `market-data` gets 900 seconds because its
healthcheck has a 15-minute startup window. Individual Docker operations are
bounded to 300 seconds and image builds to 900 seconds. Override them with
`CML_DEPLOY_WAIT_TIMEOUT_SECONDS`, `CML_MARKET_DATA_WAIT_TIMEOUT_SECONDS`,
`CML_CONSUMER_WAIT_TIMEOUT_SECONDS`, `CML_LIVE_WAIT_TIMEOUT_SECONDS`,
`CML_LIVE_STOP_TIMEOUT_SECONDS`, `CML_DEPLOY_OPERATION_TIMEOUT_SECONDS`, and
`CML_DEPLOY_BUILD_TIMEOUT_SECONDS` when a host needs different limits. A
broken operation or healthcheck now fails with diagnostics instead of waiting
indefinitely. A container in `exited`, `restarting`, `paused`, or another
non-running state fails immediately. The script also records each container's
restart count before Compose recreation and fails immediately when the count
increases or Docker reports an active restart; the health-wait timeout applies
only while the container is running but its healthcheck is still `starting`, and
the timeout names the first service still pending health.
It also requires the dashboard by default: if the dashboard is stopped or
unhealthy, the script starts it and verifies both its Compose healthcheck and
`127.0.0.1:8765/api/health`, plus the local reverse-proxy endpoint
`http://127.0.0.1/momentum/api/health`, before reporting success. Set
`CML_DASHBOARD_REQUIRED=0` only on a host where the Nginx dashboard route is
intentionally disabled. On a host whose proxy uses another local URL, set
`CML_DASHBOARD_PROXY_URL` for that invocation.
The deployment-side health poll runs once per second; Docker's own healthcheck
intervals remain the source of truth for when a service becomes healthy.

### Live update

`--refresh-approvals` is the one-command path for an active account whose
approval should follow the target runtime. It requires an explicit commit
argument and `--live`; after the image is built it derives the strategy hash
from that account's environment, reads the latest risk hash, keeps the existing
limits/operator/text/expiry, and updates only the commit and migration binding.
It fails if an active approval is missing, so it cannot create Live authority
from nothing. Run `prepare` separately when a lease is missing.

For a target that already has matching approvals:

```bash
deploy/ops/update_server.sh 43.167.191.253 <commit-sha> --live
```

To refresh the existing approvals and deploy in one explicit command:

```bash
deploy/ops/update_server.sh 43.167.191.253 <commit-sha> \
  --live --refresh-approvals
```

Set `CML_LIVE_CONCURRENCY=1` before the command for a serialized rollout, or
leave the default `2` to use two bounded restart waves. The approval,
preflight, and lease control-plane operations use a separate
`CML_LIVE_CONTROL_CONCURRENCY` setting (default `4`), so increasing control
parallelism does not increase the number of Live containers restarted or the
number of market-data connections.

The Live path builds the target image, applies any required migration, and runs
a lightweight read-only approval-binding check for every currently running
strategy before restarting any non-Live service. The check only verifies an
active approval and its target commit/migration binding, so it catches a
target/approval mismatch without first recreating Dashboard, `market-data`,
research, or Paper. When
`--refresh-approvals` is supplied, this early target check is skipped because
the explicit refresh is intentionally deferred until the final Live gate; the
old worker is never left under a newly refreshed approval while unrelated
services are still converging.

After the non-Live services converge, the Live path recomputes the active
account set and runs the final read-only strict `preflight` before renewing any
lease. On a recovery run it rechecks even pairs that already use the target
image. This keeps a rejected Live approval from changing leases or forcing a
second non-Live recovery deployment. Lease renewal and read-only preflight run
in bounded parallel batches using `CML_LIVE_CONTROL_CONCURRENCY`. The checks
cover the approval, runtime strategy hash,
risk snapshot, target commit, migration revision, account readiness, and lease
presence. If any check fails, the command exits before restarting Live
services, leaving `.env.server` at the previous committed runtime identity. It
then updates the active execution services in a bounded parallel wave and the
strategies in a second bounded wave. The default concurrency is two; set
`CML_LIVE_CONCURRENCY=1` for a more
conservative rollout or `=4` when the host has headroom:

1. execution services (up to two at a time);
2. matching strategy services (up to two at a time);
3. wait for every service in each wave to report healthy before starting the
   next wave.

Before each Live restart wave, the script captures the old container IDs and
executes `docker compose stop --timeout`. It polls those same IDs until Docker
confirms they are stopped, waits one second, and only then invokes
`up --force-recreate`. `CML_LIVE_STOP_TIMEOUT_SECONDS` (default `90`) is the
maximum graceful-stop budget, not a fixed sleep. If a container does not stop
within the budget, the deployment fails closed and does not start its
replacement.

Services that are not currently running are skipped, so the script does not
enable a disabled Live account accidentally. The dashboard is handled
separately because Nginx routes `/momentum/` to it: a missing or unhealthy
dashboard is started and verified, while a healthy dashboard is left in place.
Do not remove the `--live` flag to make an approval failure disappear; fix the
approval or lease and rerun the preflight. Each phase prints its own elapsed
seconds, including dashboard health, approval refresh, lease renewal,
preflight, execution restart, and strategy restart.

The lower-level command remains available for an account-specific manual
operation. It derives the runtime hashes, but its limit flags intentionally
create a new approval rather than preserving the previous one:

```bash
account_suffix=2
strategy_service="live-strategy-account-${account_suffix}"
migration_var="CML_LIVE_MIGRATION_REVISION_ACCOUNT_${account_suffix}"
target_commit="$(git rev-parse origin/main)"

docker compose --env-file .env.server \
  -f compose.server.yaml -f compose.live.accounts.yaml --profile live \
  run --rm --no-deps "$strategy_service" approve-runtime \
  --account-label "account-${account_suffix}" \
  --strategy orderflow_impulse \
  --git-commit-hash "$target_commit" \
  --migration-revision "${!migration_var}" \
  --notional-cap 10000 \
  --max-open-positions 500 \
  --max-daily-loss 10000 \
  --approver operator \
  --confirmation "ENABLE SMALL LIVE TRADING"
```

Use `live-strategy` and the primary environment variables for `primary`.
Confirm the risk hash and limits from the latest risk snapshot before creating
an approval; do not guess them from defaults.

### Generation-fence release rehearsal

The `20260911_0035` migration makes the deployed code generation part of the
live lease. The deployment script must therefore keep this order:

1. apply migrations while the old worker may still be running;
2. let the migration expire active leases that cannot be attributed safely;
3. renew a lease and run strict `preflight` with the target commit;
4. restart the execution-account pair and then the strategy pair;
5. verify the new worker has the target generation and that the old worker has
   no active lease before considering the rollout complete.

Run this rehearsal on a disposable database or a deliberately drained,
small-capital account before the first production rollout that includes the
generation fence. Record the old worker image/commit, lease ID, target commit,
and migration head. During the migration-to-restart window, confirm with a
read-only query that the old active lease is expired:

```sql
SELECT account_label, lease_id, owner, state, code_generation
FROM trading_leases
WHERE environment = 'live'
ORDER BY expires_at DESC;
```

The old image must fail closed at its durable `prepare_submission` boundary;
it must not reach an exchange write after its lease has been expired or its
generation no longer matches. The target image must then acquire a fresh lease
and pass strict `preflight` before any Live container is restarted. If either
worker can submit during this window, stop the rehearsal and do not continue
the rollout.

After the new pair is healthy, exercise the existing stop path once: send
`SIGTERM` during a controlled restart, verify the entry gate closes, confirm
open entries are reconciled/cancelled, and check that a restarted worker
recovers only with the target generation. Pair this with the five local fault
injection gates in `docs/paper-live-replay-execution-semantics.md#fault-injection-release-gates`; the release is not
accepted based on container health alone.

### Verification and rollback

After the script completes, verify the exact image and health state:

```bash
docker compose --env-file .env.server \
  -f compose.server.yaml -f compose.live.accounts.yaml --profile live ps
git -C /opt/crypto-momentum-lab rev-parse --short HEAD
grep -E '^(CML_CODE_COMMIT|CML_DASHBOARD_IMAGE)=' /opt/crypto-momentum-lab/.env.server
```

For a rollback, pass the previous deployed commit explicitly. The checkout must
be clean; the script moves the local `main` ref back to that ancestor with
`git reset --keep`, rebuilds the requested image, and verifies the resulting
`HEAD` and service image. A Live rollback still requires approvals whose commit
hash matches that previous image. Use `--refresh-approvals` when the existing
approval should keep its limits while changing its commit binding; otherwise
the script stops before restarting Live services when preflight detects the
mismatch.

If any phase fails, the script prints the checkout, Compose service state, and
the matching container image/status. The remote phase timer starts after SSH
connects; the `phase=client-total` line includes SSH setup, authentication, and
the complete remote command.

Healthcheck frequency controls failure detection and Docker wait time; it does
not determine the freshness of the market data consumed by the strategies.
Lease renewal is only for a planned restart and cannot create a missing lease
or change its account/strategy owner. The script uses `-T` and a closed stdin
for one-off Compose commands so a batch SSH session cannot consume input and
silently skip later accounts.
