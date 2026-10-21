#!/usr/bin/env bash

set -Eeuo pipefail

usage() {
  cat <<'USAGE'
Usage: update_server.sh <server-host|local> [git-ref]

Deploys the selected commit to the server. Checks all four exchange accounts
and waits until none have positions or open orders, then stops the running
application services, updates the checkout, runs Compose migrations, and starts
the same services again. With CML_DEPLOY_SCOPE=dashboard, builds and replaces
only the dashboard, without waiting for flat accounts or running migrations.

Environment:
  CML_SERVER_USER  SSH user (default: root)
  CML_REMOTE_DIR   server checkout (default: /opt/crypto-momentum-lab)
  CML_SSH_PASSWORD optional sshpass password; SSH keys are preferred
  CML_DEPLOY_SCOPE full (default) or dashboard (no migration/account restart)
  CML_FLAT_WAIT_SECONDS maximum flat wait (default: 1800; 0 checks once)
USAGE
}

if (( $# < 1 || $# > 2 )); then
  usage >&2
  exit 64
fi

server_host="$1"
target_ref="${2:-origin/main}"
if [[ "$target_ref" == --* ]]; then
  usage >&2
  exit 64
fi
server_user="${CML_SERVER_USER:-root}"
remote_dir="${CML_REMOTE_DIR:-/opt/crypto-momentum-lab}"

if [[ "$server_host" == "--help" || "$server_host" == "-h" ]]; then
  usage
  exit 0
fi

runner=()
if [[ "$server_host" == local || "$server_host" == localhost || \
  ( "$server_host" == 127.0.0.1 && -z "${CML_FORCE_SSH:-}" ) ]]; then
  runner=(bash -s --)
else
  ssh_command=(ssh)
  ssh_options=(-o ConnectTimeout=15 -o ServerAliveInterval=15 -o ServerAliveCountMax=6)
  if [[ -n "${CML_SSH_PASSWORD:-}" ]]; then
    if ! command -v sshpass >/dev/null 2>&1; then
      echo "CML_SSH_PASSWORD is set but sshpass is not installed" >&2
      exit 69
    fi
    export SSHPASS="$CML_SSH_PASSWORD"
    ssh_command=(sshpass -e ssh)
  else
    ssh_options+=(-o BatchMode=yes)
  fi
  runner=("${ssh_command[@]}" "${ssh_options[@]}" "${server_user}@${server_host}" bash -s --)
fi

deploy_scope="${CML_DEPLOY_SCOPE:-full}"
flat_wait_seconds="${CML_FLAT_WAIT_SECONDS:-1800}"
case "$deploy_scope" in full|dashboard) ;; *) echo "Invalid CML_DEPLOY_SCOPE" >&2; exit 64 ;; esac
if [[ ! "$flat_wait_seconds" =~ ^[0-9]+$ || ${#flat_wait_seconds} -gt 8 ]]; then
  echo "CML_FLAT_WAIT_SECONDS must be a nonnegative integer (at most 8 digits)" >&2
  exit 64
fi
flat_wait_seconds=$((10#$flat_wait_seconds))
client_started_at="$(date +%s)"
if "${runner[@]}" "$remote_dir" "$target_ref" "$deploy_scope" "$flat_wait_seconds" <<'REMOTE_SCRIPT'; then
set -Eeuo pipefail

remote_dir="$1"
target_ref="$2"
deploy_scope="$3"
flat_wait_seconds="$4"
cd "$remote_dir"

if [[ ! -f .env.server ]]; then
  echo "Refusing deployment: .env.server is missing" >&2
  exit 1
fi
if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
  echo "Refusing deployment: tracked changes exist in $remote_dir" >&2
  git status --short
  exit 1
fi
if [[ "$(git branch --show-current)" != main ]]; then
  echo "Refusing deployment: checkout must be on main" >&2
  exit 1
fi

git_dir="$(git rev-parse --git-dir)"
exec 9>"$git_dir/cml-deploy.lock"
if ! flock -n 9; then
  echo "Refusing deployment: another deployment is already running" >&2
  exit 75
fi

compose=(docker compose --env-file .env.server -f compose.server.yaml
  -f compose.next_gen.yaml --profile nextgen)
git fetch --prune origin main
previous_commit="$(git rev-parse HEAD)"
target_commit="$(git rev-parse "$target_ref^{commit}")"
if [[ "$target_commit" != "$previous_commit" ]] && \
  [[ "$(git merge-base "$previous_commit" "$target_commit")" != "$previous_commit" ]]; then
  echo "Refusing deployment: target must fast-forward main" >&2
  exit 1
fi

set_env_value() {
  local key="$1" value="$2"
  if grep -q "^${key}=" .env.server; then
    sed -i "s#^${key}=.*#${key}=${value}#" .env.server
  else
    printf '\n%s=%s\n' "$key" "$value" >> .env.server
  fi
}

set_maintenance_image() {
  local image="$1"
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "Maintenance image is not available locally: image=$image" >&2
    return 1
  fi
  set_env_value CML_MAINTENANCE_IMAGE "$image"
}

if [[ "$deploy_scope" == dashboard ]]; then
  current_code="$(sed -n 's/^CML_CODE_COMMIT=//p' .env.server | tail -n 1)"
  if [[ -z "$current_code" ]] || ! git diff --quiet "$current_code" "$target_commit" -- alembic alembic.ini; then
    echo "Dashboard-only deployment requires unchanged database migrations; use full deployment" >&2
    exit 1
  fi
fi

# Build the target image in the background while checking whether all accounts
# are flat. The outage then covers only service stop, migration, and restart.
build_parent="$(mktemp -d "${TMPDIR:-/tmp}/cml-deploy-build.XXXXXX")"
build_dir="$build_parent/source"
build_pid=""
cleanup_build_worktree() {
  if [[ -n "$build_pid" ]]; then
    kill "$build_pid" >/dev/null 2>&1 || true
    wait "$build_pid" >/dev/null 2>&1 || true
  fi
  if [[ -f "$build_dir/.git" ]]; then
    git worktree remove "$build_dir" >/dev/null 2>&1 || true
  fi
  rmdir "$build_parent" >/dev/null 2>&1 || true
}
trap cleanup_build_worktree EXIT
git worktree add --detach "$build_dir" "$target_commit"
# Fast-forward the checkout before the flat check so every Compose invocation
# sees the target service definitions. Running containers keep their current
# images until the explicit stop below.
if [[ "$target_commit" != "$previous_commit" ]]; then
  git merge --ff-only "$target_commit"
fi
build_compose=(docker compose --project-directory "$build_dir" \
  --env-file "$remote_dir/.env.server" \
  -f "$build_dir/compose.server.yaml" \
  -f "$build_dir/compose.next_gen.yaml" --profile nextgen)
target_image="crypto-momentum-lab-app:${target_commit}"
if ! docker image inspect "$target_image" >/dev/null 2>&1; then
  CML_CODE_COMMIT="$target_commit" CML_DASHBOARD_IMAGE="$target_image" \
    "${build_compose[@]}" build migrate &
  build_pid=$!
fi

finish_target_build() {
  if [[ -z "$build_pid" ]]; then
    return 0
  fi
  local pid="$build_pid" status=0
  build_pid=""
  if wait "$pid"; then
    return 0
  else
    status=$?
  fi
  echo "Target image build failed: commit=$target_commit status=$status" >&2
  return "$status"
}

check_target_build() {
  if [[ -n "$build_pid" ]] && ! kill -0 "$build_pid" 2>/dev/null; then
    finish_target_build
  fi
}

if [[ "$deploy_scope" == dashboard ]]; then
  finish_target_build
  # Explicit service plus --no-deps avoids migrations and all trading services.
  CML_DASHBOARD_IMAGE="$target_image" "${compose[@]}" up -d --no-deps \
    --force-recreate --wait --wait-timeout 180 dashboard
  container="$("${compose[@]}" ps -q dashboard)"
  image="$(docker inspect -f '{{.Config.Image}}' "$container")"
  if [[ "$image" != "$target_image" ]]; then
    echo "Dashboard deployment failed: unexpected image=$image" >&2
    exit 1
  fi
  curl --fail --silent --show-error --max-time 10 \
    http://127.0.0.1:8765/api/health >/dev/null
  set_env_value CML_DASHBOARD_IMAGE "$target_image"
  set_maintenance_image "$target_image"
  chmod 600 .env.server
  echo "deployed_commit=$target_commit scope=dashboard"
  "${compose[@]}" ps dashboard
  exit 0
fi

accounts=(
  "primary:nextgen-strategy-primary"
  "account-2:nextgen-strategy-account-2"
  "account-3:nextgen-strategy-account-3"
  "account-4:nextgen-strategy-account-4"
)
flat_check_python=$(cat <<'PY'
import asyncio, json, os, sys
from crypto_momentum_lab.next_gen.client import BinanceRestClient

async def main():
    account = sys.argv[1]
    client = BinanceRestClient(
        api_key=os.environ["BINANCE_READ_API_KEY"],
        api_secret=os.environ["BINANCE_READ_API_SECRET"],
        environment="live",
    )
    try:
        account_snapshot, orders = await asyncio.gather(
            client.fetch_account_information(), client.fetch_open_orders()
        )
        open_positions = [
            {"symbol": p.symbol, "side": p.position_side, "quantity": str(p.position_amt)}
            for p in account_snapshot.positions if p.position_amt != 0
        ]
        open_orders = [{"symbol": order["symbol"]} for order in orders]
        print(json.dumps({"account": account, "positions": open_positions, "orders": open_orders}))
        return 0 if not open_positions and not open_orders else 3
    except Exception as error:
        print(json.dumps({"account": account, "query_error": type(error).__name__}))
        return 4
    finally:
        await client.close()

raise SystemExit(asyncio.run(main()))
PY
)

flat_deadline=$(( $(date +%s) + flat_wait_seconds ))
while :; do
  check_target_build
  all_flat=1
  for item in "${accounts[@]}"; do
    account="${item%%:*}"
    service="${item#*:}"
    container="$(docker ps -q --filter label=com.docker.compose.project=crypto-momentum-lab \
      --filter "label=com.docker.compose.service=$service" | head -n 1)"
    if [[ -n "$container" ]]; then
      query=(docker exec "$container" python -c "$flat_check_python" "$account")
    else
      # A one-off probe needs the target image, which may still be building in
      # the background.  Wait once here; live-container probes keep overlapping.
      finish_target_build
      query=(env CML_CODE_COMMIT="$target_commit" "${compose[@]}" run -T --rm \
        --no-deps --entrypoint python "$service" -c "$flat_check_python" "$account")
    fi
    if output="$("${query[@]}" 2>&1 < /dev/null)"; then
      echo "$output"
    else
      status=$?
      echo "$output"
      if (( status != 3 && status != 4 )); then
        echo "Exchange state query failed to run: account=$account status=$status" >&2
        exit "$status"
      fi
      all_flat=0
    fi
    check_target_build
  done
  if (( all_flat == 1 )); then
    break
  fi
  if (( $(date +%s) >= flat_deadline )); then
    echo "Deployment stopped: accounts not proven flat within ${flat_wait_seconds}s; no application services stopped" >&2
    exit 1
  fi
  echo "Waiting for every account to have no positions or open orders; retrying in 30 seconds."
  remaining=$(( flat_deadline - $(date +%s) ))
  if (( remaining > 0 )); then
    sleep "$(( remaining < 30 ? remaining : 30 ))"
  fi
done
finish_target_build

running_services=()
for service in dashboard market-data research-collector \
  nextgen-strategy-primary nextgen-strategy-account-2 nextgen-strategy-account-3 nextgen-strategy-account-4; do
  if docker ps -q --filter label=com.docker.compose.project=crypto-momentum-lab \
    --filter "label=com.docker.compose.service=$service" | grep -q .; then
    running_services+=("$service")
  fi
done

# Start shared services even after a previous deployment attempt stopped them,
# and always start the four NextGen account services.
services=(dashboard market-data research-collector)
for item in "${accounts[@]}"; do
  services+=("${item#*:}")
done

if (( ${#running_services[@]} > 0 )); then
  if "${compose[@]}" stop --timeout 90 "${running_services[@]}"; then
    :
  else
    stop_status=$?
    echo "Deployment failed while stopping services: status=$stop_status" >&2
    exit "$stop_status"
  fi
fi

for item in "${accounts[@]}"; do
  account="${item%%:*}"
  service="${item#*:}"
  if ! env CML_CODE_COMMIT="$target_commit" "${compose[@]}" run -T --rm \
      --no-deps --entrypoint python "$service" -c "$flat_check_python" "$account" \
      < /dev/null; then
    echo "Deployment stopped: account=$account is not proven flat after shutdown" >&2
    exit 1
  fi
done

if command -v systemctl >/dev/null 2>&1; then
  timer_unit="cml-execution-receipt-retention.timer"
  service_unit="cml-execution-receipt-retention.service"
  timer_path="/etc/systemd/system/$timer_unit"
  service_path="/etc/systemd/system/$service_unit"
  if systemctl cat "$timer_unit" >/dev/null 2>&1; then
    systemctl disable --now "$timer_unit"
  fi
  if systemctl cat "$service_unit" >/dev/null 2>&1; then
    systemctl stop "$service_unit"
    systemctl reset-failed "$service_unit" >/dev/null 2>&1 || true
  fi
  if [[ -e "$timer_path" || -L "$timer_path" || -e "$service_path" || -L "$service_path" ]]; then
    rm -f "$timer_path" "$service_path"
    systemctl daemon-reload
    systemctl reset-failed "$timer_unit" "$service_unit" >/dev/null 2>&1 || true
  fi
fi

if [[ "$target_commit" != "$previous_commit" ]]; then
  git merge --ff-only "$target_commit"
fi
set_env_value CML_CODE_COMMIT "$target_commit"
set_env_value CML_DASHBOARD_IMAGE "crypto-momentum-lab-app:${target_commit}"
set_maintenance_image "$target_image"
chmod 600 .env.server
"${compose[@]}" up -d postgres
if (( ${#services[@]} > 0 )); then
  # Compose runs migration and volume initialization before the app services.
  "${compose[@]}" up -d --force-recreate "${services[@]}"
fi

deadline=$(( $(date +%s) + 900 ))
while :; do
  all_healthy=1
  for service in "${services[@]}"; do
    container="$("${compose[@]}" ps -q "$service")"
    if [[ -z "$container" ]]; then
      echo "Deployment failed: service=$service has no container" >&2
      exit 1
    fi
    state="$(docker inspect -f '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container")"
    if [[ "$state" == running\|unhealthy || "$state" == exited\|* || "$state" == dead\|* ]]; then
      echo "Deployment failed: service=$service state=$state" >&2
      "${compose[@]}" logs --no-color --tail=120 "$service" >&2 || true
      exit 1
    fi
    if [[ "$state" != running\|healthy && "$state" != running\|none ]]; then
      all_healthy=0
    fi
    image="$(docker inspect -f '{{.Config.Image}}' "$container")"
    if [[ "$image" != "crypto-momentum-lab-app:${target_commit}" ]]; then
      echo "Deployment failed: service=$service image=$image" >&2
      exit 1
    fi
  done
  if (( all_healthy == 1 )); then
    break
  fi
  if (( $(date +%s) >= deadline )); then
    echo "Deployment failed: service health wait timed out" >&2
    exit 1
  fi
  sleep 10
done

if [[ " ${services[*]} " == *" dashboard "* ]]; then
  curl --fail --silent --show-error --max-time 10 \
    http://127.0.0.1:8765/api/health >/dev/null
fi
if command -v systemctl >/dev/null 2>&1 && \
  systemctl cat cml-ops-monitor.service >/dev/null 2>&1; then
  systemctl restart cml-ops-monitor.service
fi
echo "deployed_commit=$target_commit"
"${compose[@]}" ps
REMOTE_SCRIPT
  elapsed_seconds=$(( $(date +%s) - client_started_at ))
  echo "deployment_finished elapsed_seconds=$elapsed_seconds"
else
  status=$?
  elapsed_seconds=$(( $(date +%s) - client_started_at ))
  echo "deployment_failed status=$status elapsed_seconds=$elapsed_seconds" >&2
  exit "$status"
fi
