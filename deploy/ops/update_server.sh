#!/usr/bin/env bash

set -Eeuo pipefail

usage() {
  cat <<'USAGE'
Usage: update_server.sh <server-host> [git-ref] [--live]

Environment:
  CML_SERVER_USER  SSH user (default: root)
  CML_REMOTE_DIR   checkout on the server (default: /opt/crypto-momentum-lab)

The live profile is never touched unless --live is supplied. Live updates run
preflight for every currently running account before restarting any live
container. The SSH connection must use an agent/key; credentials are not read
from this script.
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

ssh_opts=(
  -o BatchMode=yes
  -o ConnectTimeout=15
)

ssh "${ssh_opts[@]}" "${server_user}@${server_host}" bash -s -- \
  "$remote_dir" "$target_ref" "$live_update" <<'REMOTE_SCRIPT'
set -Eeuo pipefail

remote_dir="$1"
target_ref="$2"
live_update="$3"
cd "$remote_dir"

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

target_commit="$(git rev-parse "$target_ref")"
git merge --ff-only "$target_commit"

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

# Build once. BuildKit reuses the dependency layers on normal code updates.
"${compose[@]}" build

# market-data must be ready before research and strategy consumers restart.
"${compose[@]}" up -d --no-deps --wait market-data
"${compose[@]}" up -d --no-deps --wait \
  research-collector \
  paper-orderflow-pair \
  paper-orderflow-gainer10-pair \
  paper-b1-gainer100 \
  paper-b1-gainer100-ema \
  dashboard

is_running() {
  local service="$1"
  local container_id
  local state
  container_id="$("${compose[@]}" ps -q "$service" 2>/dev/null || true)"
  [[ -n "$container_id" ]] || return 1
  state="$(docker inspect -f '{{.State.Status}}' "$container_id" 2>/dev/null || true)"
  [[ "$state" == "running" ]]
}

if [[ "$live_update" == 1 ]]; then
  live_pairs=(
    "primary:execution-account-live:live-strategy"
    "account-2:execution-account-live-account-2:live-strategy-account-2"
    "account-3:execution-account-live-account-3:live-strategy-account-3"
    "account-4:execution-account-live-account-4:live-strategy-account-4"
  )

  # Preflight all active strategies before the first live restart. Approvals,
  # hashes, migration revision, account readiness, and leases must already
  # match the target commit. A failed preflight leaves live containers intact.
  for pair in "${live_pairs[@]}"; do
    IFS=: read -r account execution_service strategy_service <<<"$pair"
    if is_running "$strategy_service"; then
      echo "preflight $account"
      "${compose[@]}" run --rm --no-deps "$strategy_service" preflight \
        --account-label "$account" \
        --strategy orderflow_impulse >/dev/null
    fi
  done

  # Keep the live blast radius bounded: execution first, then its strategy;
  # move to the next account only after both services report healthy.
  for pair in "${live_pairs[@]}"; do
    IFS=: read -r account execution_service strategy_service <<<"$pair"
    if is_running "$strategy_service"; then
      echo "update $account"
      "${compose[@]}" up -d --no-deps --wait "$execution_service"
      "${compose[@]}" up -d --no-deps --wait "$strategy_service"
    fi
  done
fi

echo "deployed_commit=$target_commit"
docker ps --format '{{.Names}}|{{.Image}}|{{.Status}}' \
  | grep -E 'crypto-momentum-lab-(dashboard|market-data|research-collector|paper-|execution-account-live|live-strategy)' \
  | sort
REMOTE_SCRIPT
