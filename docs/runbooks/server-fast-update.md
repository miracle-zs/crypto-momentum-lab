# Fast Server Update

This runbook records the repeatable update path for the server at
`/opt/crypto-momentum-lab`. It reduces operator wait time by validating the
target once, building one image, waiting on Docker health directly, and
avoiding needless container recreation. It does not bypass Live approvals,
leases, reconciliation, or the fail-closed entry gate.

## Why an update can take several minutes

The application image is built on the server. A cold dependency build is the
largest variable cost; the Dockerfile keeps third-party dependencies in a layer
keyed only by `pyproject.toml`, so source-only changes rebuild a small local
wheel. Healthchecks retain their 60/90-second steady-state intervals to keep
probe CPU low, but use a 5-second `start_interval` (15 seconds for the long
market-data recovery window) while a container is starting. Stateless
research, paper, and dashboard services stop after 20 seconds; market-data and
Live services retain a 60-second grace period for state and exchange cleanup.
Updating eight Live containers one by one used to add several minutes even
when the code build was cached.

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

The script:

1. fetches the target and requires a clean `main` checkout on the server;
2. classifies the changed paths and skips the build/restart when a commit only
   changes docs, tests, or operator tooling;
3. updates the runtime-only `CML_CODE_COMMIT` and dashboard image in
   `.env.server`;
4. validates the merged Compose graph;
5. builds the image once using the dependency cache;
6. waits for `market-data`, then updates only the affected research, paper, and
   dashboard consumers;
7. prints phase timings, the deployed commit, and the container health summary.

It uses `docker compose up -d --wait`. Compose recreates a service when its
image or configuration changed, so the normal path does not need
`--force-recreate`.

The deployment script accepts `CML_DEPLOY_WAIT_TIMEOUT_SECONDS` (default 600)
so a broken healthcheck fails with diagnostics instead of waiting forever.

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

The Live path builds the target image, optionally refreshes active approvals,
renews active leases, then runs strict `preflight` for every currently running
strategy before restarting any consumer or Live container. Lease renewal and
read-only preflight run in bounded parallel batches using
`CML_LIVE_CONCURRENCY`. The checks cover the approval, runtime strategy hash,
risk snapshot, target commit, migration revision, account readiness, and lease
presence. If any check fails, the command exits before restarting services. It then updates the active execution
services in a bounded parallel wave and the strategies in a second bounded
wave. The default concurrency is two; set `CML_LIVE_CONCURRENCY=1` for a more
conservative rollout or `=4` when the host has headroom:

1. execution services (up to two at a time);
2. matching strategy services (up to two at a time);
3. wait for every service in each wave to report healthy before starting the
   next wave.

Services that are not currently running are skipped, so the script does not
enable a disabled Live account accidentally. Do not remove the `--live` flag to
make an approval failure disappear; fix the approval or lease and rerun the
preflight. Each phase prints its own elapsed seconds, including approval
refresh, lease renewal, preflight, execution restart, and strategy restart.

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

## Verification and rollback

After the script completes, verify the exact image and health state:

```bash
docker compose --env-file .env.server \
  -f compose.server.yaml -f compose.live.accounts.yaml --profile live ps
git -C /opt/crypto-momentum-lab rev-parse --short HEAD
grep -E '^(CML_CODE_COMMIT|CML_DASHBOARD_IMAGE)=' /opt/crypto-momentum-lab/.env.server
```

For a rollback, pass the previous deployed commit to the same script. A Live
rollback still requires approvals whose commit hash matches that previous
image; the script intentionally stops before restarting Live services when
that preflight does not pass.

Healthcheck frequency controls failure detection and Docker wait time; it does
not determine the freshness of the market data consumed by the strategies.
Lease renewal is only for a planned restart and cannot create a missing lease
or change its account/strategy owner. The script uses `-T` and a closed stdin
for one-off Compose commands so a batch SSH session cannot consume input and
silently skip later accounts.
