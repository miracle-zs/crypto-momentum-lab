# Fast Server Update

This runbook records the repeatable update path for the server at
`/opt/crypto-momentum-lab`. It reduces operator wait time by validating the
target once, building one image, waiting on Docker health directly, and
avoiding needless container recreation. It does not bypass Live approvals,
leases, reconciliation, or the fail-closed entry gate.

## Why an update can take several minutes

The application image is built on the server. A cold dependency build is the
largest variable cost; normal updates reuse BuildKit layers. Each Compose
service also has a 60-second stop grace period so it can flush telemetry and
close exchange connections cleanly. The execution healthcheck has a 30-second
startup period, while Live strategy services have a two-minute startup period
and a 60-second probe interval. Updating eight Live containers one by one
therefore adds several minutes even when the code build is cached.

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
2. updates the runtime-only `CML_CODE_COMMIT` and dashboard image in
   `.env.server`;
3. validates the merged Compose graph;
4. builds the image once using the existing cache;
5. waits for `market-data`, then updates research, paper, and dashboard
   consumers in one Compose invocation;
6. prints the deployed commit and container health summary.

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

The Live path first runs `preflight` for every currently running strategy. If
any approval, hash, migration, account readiness, or lease check fails, no Live
container is restarted. It then updates each active account in this order:

1. execution service;
2. matching strategy service;
3. wait for both services to report healthy before moving on.

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

Do not shorten the service stop grace period or health startup windows as a
generic speed fix. They protect exchange connection cleanup and state
recovery. Healthcheck frequency controls failure detection and Docker wait
time; it does not determine the freshness of the market data consumed by the
strategies.
