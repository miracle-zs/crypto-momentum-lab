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

Approval and lease preparation is a separate safety operation. Run it before
restarting Live containers. If approval or lease state is wrong, a Live worker
must remain fail-closed; its retry backoff can otherwise add another few
minutes while an operator diagnoses the mismatch.

## Normal update

Push the desired commit first and note its full SHA. Use an SSH key or agent;
never put a server password in a script or repository. From a workstation with
the repository checkout, run:

```bash
deploy/ops/update_server.sh 43.167.191.253 <commit-sha>
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

## Live update

Before the Live step, update each account's approval to the exact image commit
and migration revision, and run `prepare` when a lease is missing. Keep the
existing strategy and risk hashes and limits. Run the same script with the
explicit Live flag:

```bash
deploy/ops/update_server.sh 43.167.191.253 <commit-sha> --live
```

Set `CML_LIVE_CONCURRENCY=1` before the command for a serialized rollout, or
leave the default `2` to use two bounded restart waves.

The Live path first renews every active lease to one hour, checking its owner
and strategy binding, then runs `preflight` for every currently running
strategy. If any approval, hash, migration, account readiness, or lease check
fails, no Live container is restarted. It then updates the active execution
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
preflight.

The following is an account-2 example. Replace `2` in the service name,
account label, and environment variable names for account 3 or 4. The risk hash
must come from that account's latest risk snapshot.

```bash
account_suffix=2
strategy_service="live-strategy-account-${account_suffix}"
strategy_hash_var="CML_LIVE_STRATEGY_CONFIG_HASH_ACCOUNT_${account_suffix}"
migration_var="CML_LIVE_MIGRATION_REVISION_ACCOUNT_${account_suffix}"

docker compose --env-file .env.server \
  -f compose.server.yaml -f compose.live.accounts.yaml --profile live \
  run --rm --no-deps "$strategy_service" approve \
  --account-label "account-${account_suffix}" \
  --strategy orderflow_impulse \
  --strategy-config-hash "${!strategy_hash_var}" \
  --risk-config-hash "$RISK_CONFIG_HASH" \
  --git-commit-hash "$CML_CODE_COMMIT" \
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
