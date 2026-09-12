# Fast Server Update

This runbook records the repeatable update path for the server at
`/opt/crypto-momentum-lab`. It reduces operator wait time by validating the
target once, building one image, waiting on Docker health directly, and
avoiding needless container recreation. It does not bypass Live approvals,
leases, reconciliation, or the fail-closed entry gate.

## Why an update can take several minutes

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

## Normal update

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

## Live update

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
leave the default `2` to use two bounded restart waves.

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
in bounded parallel batches using
`CML_LIVE_CONCURRENCY`. The checks cover the approval, runtime strategy hash,
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

## Generation-fence release rehearsal

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
injection gates in `docs/runbooks/fault-injection-gates.md`; the release is not
accepted based on container health alone.

## Verification and rollback

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
