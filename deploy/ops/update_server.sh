#!/usr/bin/env bash

set -Eeuo pipefail

usage() {
  cat <<'USAGE'
Usage: update_server.sh <server-host> [git-ref] [--live]

Environment:
  CML_SERVER_USER  SSH user (default: root)
  CML_REMOTE_DIR   checkout on the server (default: /opt/crypto-momentum-lab)
  CML_LIVE_CONCURRENCY  maximum parallel Live services (default: 2)
  CML_SSH_PASSWORD  optional password for sshpass; prefer an SSH key

The live profile is never touched unless --live is supplied. Live updates run
preflight for every currently running account before restarting any live
container. The SSH connection uses an agent/key by default. When
CML_SSH_PASSWORD is set, sshpass reads it from the environment; the password
is never a command-line argument, remote argument, or repository value.
USAGE
}

if [[ $# -lt 1 || $# -gt 3 ]]; then
  usage >&2
  exit 64
fi

server_host="$1"
target_ref="${2:-origin/main}"
live_update=0
if [[ "${3:-}" == "--live" ]]; then
  live_update=1
elif [[ -n "${3:-}" ]]; then
  usage >&2
  exit 64
fi

server_user="${CML_SERVER_USER:-root}"
remote_dir="${CML_REMOTE_DIR:-/opt/crypto-momentum-lab}"
live_concurrency="${CML_LIVE_CONCURRENCY:-2}"

ssh_opts=( -o ConnectTimeout=15 )
ssh_command=(ssh)
if [[ -n "${CML_SSH_PASSWORD:-}" ]]; then
  if ! command -v sshpass >/dev/null 2>&1; then
    echo "CML_SSH_PASSWORD is set but sshpass is not installed" >&2
    exit 69
  fi
  export SSHPASS="$CML_SSH_PASSWORD"
  ssh_command=(sshpass -e ssh)
else
  ssh_opts+=( -o BatchMode=yes )
fi

"${ssh_command[@]}" "${ssh_opts[@]}" "${server_user}@${server_host}" bash -s -- \
  "$remote_dir" "$target_ref" "$live_update" "$live_concurrency" <<'REMOTE_SCRIPT'
set -Eeuo pipefail

remote_dir="$1"
target_ref="$2"
live_update="$3"
live_concurrency="$4"
cd "$remote_dir"
deploy_started_at="$(date +%s)"

# Refuse to overwrite tracked operator changes. Ignored runtime files such as
# .env.server and its backups are allowed and are updated below.
if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
  echo "Refusing deployment: tracked changes exist in $remote_dir" >&2
  git status --short
  exit 1
fi

git fetch --prune origin main
if [[ "$(git branch --show-current)" != "main" ]]; then
  echo "Refusing deployment: checkout must be on main" >&2
  exit 1
fi

previous_commit="$(git rev-parse HEAD)"
target_commit="$(git rev-parse "$target_ref")"
git merge --ff-only "$target_commit"

# Classify the commit range before building. Documentation, tests, and
# operator-only changes update the checkout without rebuilding or restarting
# services. Unknown runtime paths are treated conservatively as affecting all
# application consumers.
runtime_changed=0
market_changed=0
research_changed=0
paper_changed=0
dashboard_changed=0
live_changed=0
changed_files="$(git diff --name-only "$previous_commit" "$target_commit")"
while IFS= read -r changed_path; do
  [[ -z "$changed_path" ]] && continue
  case "$changed_path" in
    Dockerfile|pyproject.toml|alembic.ini|alembic/*|configs/*|compose*.yaml)
      runtime_changed=1
      market_changed=1
      research_changed=1
      paper_changed=1
      dashboard_changed=1
      live_changed=1
      ;;
    src/crypto_momentum_lab/live_rollout/*|\
    src/crypto_momentum_lab/apps/live_rollout/*|\
    src/crypto_momentum_lab/execution_account/*)
      runtime_changed=1
      live_changed=1
      ;;
    src/crypto_momentum_lab/operator_dashboard/*|\
    src/crypto_momentum_lab/apps/operator_dashboard/*)
      runtime_changed=1
      dashboard_changed=1
      ;;
    src/crypto_momentum_lab/research/*|\
    src/crypto_momentum_lab/apps/research*/*)
      runtime_changed=1
      research_changed=1
      ;;
    src/crypto_momentum_lab/market_data/*|\
    src/crypto_momentum_lab/apps/market_data/*|\
    src/crypto_momentum_lab/persistence/*)
      runtime_changed=1
      market_changed=1
      research_changed=1
      paper_changed=1
      dashboard_changed=1
      live_changed=1
      ;;
    src/crypto_momentum_lab/strategy/*|\
    src/crypto_momentum_lab/apps/strategy_runner/*|\
    src/crypto_momentum_lab/domain/*)
      runtime_changed=1
      paper_changed=1
      live_changed=1
      ;;
    src/*)
      runtime_changed=1
      market_changed=1
      research_changed=1
      paper_changed=1
      dashboard_changed=1
      live_changed=1
      ;;
    *)
      ;;
  esac
done <<<"$changed_files"

if [[ "$runtime_changed" == 0 ]]; then
  echo "runtime_unchanged=1"
  echo "deployed_checkout=$target_commit"
  exit 0
fi

if [[ ! -f .env.server ]]; then
  echo "Refusing deployment: .env.server is missing" >&2
  exit 1
fi

set_env_value() {
  local key="$1"
  local value="$2"
  if grep -q "^${key}=" .env.server; then
    sed -i "s#^${key}=.*#${key}=${value}#" .env.server
  else
    printf '\n%s=%s\n' "$key" "$value" >> .env.server
  fi
}

set_env_value CML_CODE_COMMIT "$target_commit"
set_env_value CML_DASHBOARD_IMAGE "crypto-momentum-lab-app:${target_commit}"
chmod 600 .env.server

compose=(
  docker compose
  --env-file .env.server
  -f compose.server.yaml
  -f compose.live.accounts.yaml
  --profile live
)

# Resolve the full graph before stopping anything. This also catches missing
# account credentials and malformed environment overrides early.
"${compose[@]}" config --quiet

# Build once. The Dockerfile keeps dependency installation in a layer keyed by
# pyproject.toml, so ordinary source changes only rebuild the application.
build_started_at="$(date +%s)"
"${compose[@]}" build
echo "phase=build elapsed_seconds=$(( $(date +%s) - build_started_at ))"

# market-data must be ready before research and strategy consumers restart.
if [[ "$market_changed" == 1 ]]; then
  market_started_at="$(date +%s)"
  "${compose[@]}" up -d --no-deps --wait market-data
  echo "phase=market-data elapsed_seconds=$(( $(date +%s) - market_started_at ))"
fi

consumer_services=()
if [[ "$research_changed" == 1 ]]; then
  consumer_services+=(research-collector)
fi
if [[ "$paper_changed" == 1 ]]; then
  consumer_services+=(
    paper-orderflow-pair
    paper-orderflow-gainer10-pair
    paper-b1-gainer100
    paper-b1-gainer100-ema
  )
fi
if [[ "$dashboard_changed" == 1 ]]; then
  consumer_services+=(dashboard)
fi
if (( ${#consumer_services[@]} > 0 )); then
  consumers_started_at="$(date +%s)"
  "${compose[@]}" up -d --no-deps --wait "${consumer_services[@]}"
  echo "phase=consumers elapsed_seconds=$(( $(date +%s) - consumers_started_at ))"
fi

is_running() {
  local service="$1"
  local container_id
  local state
  container_id="$("${compose[@]}" ps -q "$service" 2>/dev/null || true)"
  [[ -n "$container_id" ]] || return 1
  state="$(docker inspect -f '{{.State.Status}}' "$container_id" 2>/dev/null || true)"
  [[ "$state" == "running" ]]
}

if [[ "$live_update" == 1 && "$live_changed" == 1 ]]; then
  live_pairs=(
    "primary:execution-account-live:live-strategy"
    "account-2:execution-account-live-account-2:live-strategy-account-2"
    "account-3:execution-account-live-account-3:live-strategy-account-3"
    "account-4:execution-account-live-account-4:live-strategy-account-4"
  )

  if ! [[ "$live_concurrency" =~ ^[1-4]$ ]]; then
    echo "Invalid CML_LIVE_CONCURRENCY: $live_concurrency" >&2
    exit 64
  fi

  env_value() {
    local key="$1"
    local fallback="$2"
    local value
    value="$(sed -n "s/^${key}=//p" .env.server | tail -n 1)"
    printf '%s' "${value:-$fallback}"
  }

  lease_owner_for_account() {
    local account="$1"
    case "$account" in
      primary) env_value CML_LIVE_LEASE_OWNER live-worker ;;
      account-2) env_value CML_LIVE_LEASE_OWNER_ACCOUNT_2 live-worker-account-2 ;;
      account-3) env_value CML_LIVE_LEASE_OWNER_ACCOUNT_3 live-worker-account-3 ;;
      account-4) env_value CML_LIVE_LEASE_OWNER_ACCOUNT_4 live-worker-account-4 ;;
      *) echo "unknown account: $account" >&2; return 64 ;;
    esac
  }

  active_pairs=()
  for pair in "${live_pairs[@]}"; do
    IFS=: read -r account execution_service strategy_service <<<"$pair"
    if is_running "$strategy_service"; then
      active_pairs+=("$pair")
    fi
  done

  # Extend each active lease before any preflight or restart. The command
  # checks both owner and strategy, so a stale or cross-account lease fails
  # closed instead of being silently extended.
  for pair in "${active_pairs[@]}"; do
    IFS=: read -r account execution_service strategy_service <<<"$pair"
    lease_owner="$(lease_owner_for_account "$account")"
    echo "renew lease $account"
    "${compose[@]}" run --rm --no-deps -T "$strategy_service" renew-lease \
      --account-label "$account" \
      --strategy orderflow_impulse \
      --lease-owner "$lease_owner" \
      --lease-ttl-seconds 3600 \
      --confirmation "RENEW LIVE RISK LEASE" </dev/null >/dev/null
  done

  # Preflight all active strategies before the first live restart. Approvals,
  # hashes, migration revision, account readiness, and leases must already
  # match the target commit. A failed preflight leaves live containers intact.
  for pair in "${active_pairs[@]}"; do
    IFS=: read -r account execution_service strategy_service <<<"$pair"
    echo "preflight $account"
    "${compose[@]}" run --rm --no-deps -T "$strategy_service" preflight \
      --account-label "$account" \
      --strategy orderflow_impulse </dev/null >/dev/null
  done

  # Compose performs each wave concurrently while respecting the configured
  # bound. Re-check strategy processes before the second wave so a service that
  # drained during execution restarts is not accidentally enabled.
  live_started_at="$(date +%s)"
  execution_services=()
  for pair in "${active_pairs[@]}"; do
    IFS=: read -r account execution_service strategy_service <<<"$pair"
    if is_running "$strategy_service"; then
      execution_services+=("$execution_service")
    fi
  done
  if (( ${#execution_services[@]} > 0 )); then
    echo "update execution wave (${#execution_services[@]} services)"
    "${compose[@]}" --parallel "$live_concurrency" up -d --no-deps --wait \
      "${execution_services[@]}"
  fi

  strategy_services=()
  for pair in "${active_pairs[@]}"; do
    IFS=: read -r account execution_service strategy_service <<<"$pair"
    if is_running "$strategy_service"; then
      strategy_services+=("$strategy_service")
    fi
  done
  if (( ${#strategy_services[@]} > 0 )); then
    echo "update strategy wave (${#strategy_services[@]} services)"
    "${compose[@]}" --parallel "$live_concurrency" up -d --no-deps --wait \
      "${strategy_services[@]}"
  fi
  echo "phase=live elapsed_seconds=$(( $(date +%s) - live_started_at ))"
fi

echo "phase=total elapsed_seconds=$(( $(date +%s) - deploy_started_at ))"

echo "deployed_commit=$target_commit"
docker ps --format '{{.Names}}|{{.Image}}|{{.Status}}' \
  | grep -E 'crypto-momentum-lab-(dashboard|market-data|research-collector|paper-|execution-account-live|live-strategy)' \
  | sort
REMOTE_SCRIPT
