#!/usr/bin/env bash

set -Eeuo pipefail

usage() {
  cat <<'USAGE'
Usage: update_server.sh <server-host> [git-ref] [--live] [--refresh-approvals]

Environment:
  CML_SERVER_USER  SSH user (default: root)
  CML_REMOTE_DIR   checkout on the server (default: /opt/crypto-momentum-lab)
  CML_LIVE_CONCURRENCY  maximum parallel Live services (default: 2)
  CML_DEPLOY_WAIT_TIMEOUT_SECONDS  Compose health wait timeout (default: 600)
  CML_DASHBOARD_REQUIRED  require the dashboard endpoint (default: 1)
  CML_DASHBOARD_PROXY_URL  local reverse-proxy health URL (default: http://127.0.0.1/momentum/api/health)
  CML_SSH_PASSWORD  optional password for sshpass; prefer an SSH key

The live profile is never touched unless --live is supplied. Live updates run
preflight for every currently running account before restarting any live
container. --refresh-approvals is an explicit opt-in that refreshes active
approvals from the target runtime while preserving their existing limits and
operator fields; it requires --live and an explicit git-ref. The SSH connection
uses an agent/key by default. When
CML_SSH_PASSWORD is set, sshpass reads it from the environment; the password
is never a command-line argument, remote argument, or repository value.
USAGE
}

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 64
fi

if [[ $# -eq 1 && ( "$1" == "--help" || "$1" == "-h" ) ]]; then
  usage
  exit 0
fi

server_host="$1"
shift
target_ref="origin/main"
target_ref_set=0
live_update=0
refresh_approvals=0
while (( $# > 0 )); do
  case "$1" in
    --live)
      live_update=1
      ;;
    --refresh-approvals)
      refresh_approvals=1
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --*)
      usage >&2
      exit 64
      ;;
    *)
      if [[ "$target_ref_set" == 1 ]]; then
        usage >&2
        exit 64
      fi
      target_ref="$1"
      target_ref_set=1
      ;;
  esac
  shift
done

if [[ "$refresh_approvals" == 1 && "$live_update" != 1 ]]; then
  echo "--refresh-approvals requires --live" >&2
  exit 64
fi

if [[ "$refresh_approvals" == 1 && "$target_ref_set" == 0 ]]; then
  echo "--refresh-approvals requires an explicit git-ref" >&2
  usage >&2
  exit 64
fi

server_user="${CML_SERVER_USER:-root}"
remote_dir="${CML_REMOTE_DIR:-/opt/crypto-momentum-lab}"
live_concurrency="${CML_LIVE_CONCURRENCY:-2}"
deploy_wait_timeout="${CML_DEPLOY_WAIT_TIMEOUT_SECONDS:-600}"
dashboard_required="${CML_DASHBOARD_REQUIRED:-1}"
dashboard_proxy_url="${CML_DASHBOARD_PROXY_URL:-http://127.0.0.1/momentum/api/health}"

if [[ "$dashboard_required" != 0 && "$dashboard_required" != 1 ]]; then
  echo "Invalid CML_DASHBOARD_REQUIRED: $dashboard_required" >&2
  exit 64
fi

if [[ -z "$dashboard_proxy_url" ]]; then
  echo "Invalid CML_DASHBOARD_PROXY_URL: value must not be empty" >&2
  exit 64
fi

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

client_started_at="$(date +%s)"
if "${ssh_command[@]}" "${ssh_opts[@]}" "${server_user}@${server_host}" bash -s -- \
  "$remote_dir" "$target_ref" "$live_update" "$live_concurrency" \
  "$deploy_wait_timeout" "$refresh_approvals" "$dashboard_required" \
  "$dashboard_proxy_url" <<'REMOTE_SCRIPT'
set -Eeuo pipefail

remote_dir="$1"
target_ref="$2"
live_update="$3"
live_concurrency="$4"
deploy_wait_timeout="$5"
refresh_approvals="$6"
dashboard_required="$7"
dashboard_proxy_url="$8"
if ! [[ "$deploy_wait_timeout" =~ ^[1-9][0-9]*$ ]]; then
  echo "Invalid CML_DEPLOY_WAIT_TIMEOUT_SECONDS: $deploy_wait_timeout" >&2
  exit 64
fi
if [[ "$refresh_approvals" != 0 && "$refresh_approvals" != 1 ]]; then
  echo "Invalid refresh approvals flag: $refresh_approvals" >&2
  exit 64
fi
if [[ "$dashboard_required" != 0 && "$dashboard_required" != 1 ]]; then
  echo "Invalid dashboard required flag: $dashboard_required" >&2
  exit 64
fi
if [[ -z "$dashboard_proxy_url" ]]; then
  echo "Invalid dashboard proxy URL: value must not be empty" >&2
  exit 64
fi
cd "$remote_dir"
deploy_started_at="$(date +%s)"

git_dir="$(git rev-parse --git-dir)"
deploy_lock_file="$git_dir/cml-deploy.lock"
deploy_state_file="$git_dir/cml-deploy-state"
if ! command -v flock >/dev/null 2>&1; then
  echo "Refusing deployment: flock is required for the deployment lock" >&2
  exit 69
fi
exec 9>"$deploy_lock_file"
if ! flock -n 9; then
  echo "Refusing deployment: another deployment is already running for $remote_dir" >&2
  exit 75
fi

deploy_state_target=""
deploy_state_status=""
if [[ -f "$deploy_state_file" ]]; then
  deploy_state_target="$(sed -n 's/^target_commit=//p' "$deploy_state_file" | tail -n 1)"
  deploy_state_status="$(sed -n 's/^status=//p' "$deploy_state_file" | tail -n 1)"
fi

write_deploy_state() {
  local status="$1"
  local phase="$2"
  local state_tmp="${deploy_state_file}.tmp"
  umask 077
  {
    printf 'target_commit=%s\n' "${target_commit:-}"
    printf 'status=%s\n' "$status"
    printf 'phase=%s\n' "$phase"
    printf 'live_update=%s\n' "$live_update"
    printf 'updated_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } > "$state_tmp"
  mv -f "$state_tmp" "$deploy_state_file"
}

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
git cat-file -e "$target_commit^{commit}"
if ! git merge --ff-only "$target_commit"; then
  # A previously deployed commit may be an intentional rollback target. The
  # checkout is clean above, so reset --keep moves only the local branch ref
  # and tracked files needed to reach that explicit commit.
  if [[ "$(git merge-base "$previous_commit" "$target_commit")" == "$target_commit" ]]; then
    git reset --keep "$target_commit"
  else
    echo "Refusing deployment: target is not an ancestor and cannot be fast-forwarded or rolled back safely" >&2
    exit 1
  fi
fi
if [[ "$(git rev-parse HEAD)" != "$target_commit" ]]; then
  echo "Refusing deployment: checkout did not reach target commit $target_commit" >&2
  exit 1
fi
write_deploy_state running checkout

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
    src/crypto_momentum_lab/apps/market_data/*)
      runtime_changed=1
      market_changed=1
      research_changed=1
      paper_changed=1
      dashboard_changed=1
      live_changed=1
      ;;
    src/crypto_momentum_lab/persistence/postgres/order_repository.py)
      runtime_changed=1
      research_changed=1
      live_changed=1
      ;;
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

# If the previous attempt reached the target checkout but failed before all
# services converged, the next invocation has an empty commit diff. Treat it
# as a recovery run so the already-running services are reconciled again.
if [[ "$target_commit" == "$previous_commit" \
  && ( "$deploy_state_target" != "$target_commit" || "$deploy_state_status" != "success" ) ]]; then
  runtime_changed=1
  market_changed=1
  research_changed=1
  paper_changed=1
  dashboard_changed=1
  if [[ "$live_update" == 1 ]]; then
    live_changed=1
  fi
  echo "recovery_run=1 reason=target_checkout_already_present"
fi

if [[ "$runtime_changed" == 0 ]]; then
  echo "runtime_unchanged=1"
  if [[ "$live_update" != 1 ]]; then
    write_deploy_state success unchanged
    echo "deployed_checkout=$target_commit"
    exit 0
  fi
  # A second --live invocation is an intentional retry path after an
  # operator updates approvals. Reconcile only the active Live services even
  # when the target checkout and image were already deployed.
  live_changed=1
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
deploy_phase=compose

print_failure_context() {
  local status="$1"
  echo "deployment_failed=1 exit_code=$status checkout=$(git rev-parse HEAD 2>/dev/null || echo unknown)" >&2
  echo "failure_service_status:" >&2
  "${compose[@]}" ps >&2 || true
  echo "failure_container_status:" >&2
  docker ps --format '{{.Names}}|{{.Image}}|{{.Status}}' \
    | grep -E 'crypto-momentum-lab-(dashboard|market-data|research-collector|paper-|execution-account-live|live-strategy)' \
    | sort >&2 || true
}

on_deploy_exit() {
  local status="$?"
  if (( status != 0 )); then
    write_deploy_state failed "${deploy_phase:-unknown}" || true
    print_failure_context "$status"
  fi
  exit "$status"
}
trap on_deploy_exit EXIT

# Resolve the full graph before stopping anything. This also catches missing
# account credentials and malformed environment overrides early.
"${compose[@]}" config --quiet

is_running() {
  local service="$1"
  local container_id
  local state
  container_id="$("${compose[@]}" ps -q "$service" 2>/dev/null || true)"
  [[ -n "$container_id" ]] || return 1
  state="$(docker inspect -f '{{.State.Status}}' "$container_id" 2>/dev/null || true)"
  [[ "$state" == "running" ]]
}

is_healthy() {
  local service="$1"
  local container_id
  local status
  container_id="$("${compose[@]}" ps -q "$service" 2>/dev/null || true)"
  [[ -n "$container_id" ]] || return 1
  status="$(docker inspect -f '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_id" 2>/dev/null || true)"
  [[ "$status" == "running|healthy" ]]
}

verify_service_target() {
  local service="$1"
  local expected_image="crypto-momentum-lab-app:${target_commit}"
  local container_id state image
  container_id="$("${compose[@]}" ps -q "$service" 2>/dev/null || true)"
  if [[ -z "$container_id" ]]; then
    echo "verification failed: service $service has no container" >&2
    return 1
  fi
  state="$(docker inspect -f '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_id" 2>/dev/null || true)"
  image="$(docker inspect -f '{{.Config.Image}}' "$container_id" 2>/dev/null || true)"
  if [[ "$state" != "running|healthy" || "$image" != "$expected_image" ]]; then
    echo "verification failed: service=$service state=$state image=$image expected_image=$expected_image" >&2
    return 1
  fi
}

live_preflight_complete=0
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

  migration_revision_for_account() {
    local account="$1"
    case "$account" in
      primary) env_value CML_LIVE_MIGRATION_REVISION 20260822_0018 ;;
      account-2) env_value CML_LIVE_MIGRATION_REVISION_ACCOUNT_2 20260831_0029 ;;
      account-3) env_value CML_LIVE_MIGRATION_REVISION_ACCOUNT_3 20260831_0029 ;;
      account-4) env_value CML_LIVE_MIGRATION_REVISION_ACCOUNT_4 20260831_0029 ;;
      *) echo "unknown account: $account" >&2; return 64 ;;
    esac
  }

  wait_for_batch() {
    local failure=0
    local pid
    for pid in "$@"; do
      if ! wait "$pid"; then
        failure=1
      fi
    done
    return "$failure"
  }

  refresh_approval_for_pair() {
    local pair="$1"
    local account execution_service strategy_service
    IFS=: read -r account execution_service strategy_service <<<"$pair"
    echo "refresh approval $account"
    "${compose[@]}" run --rm --no-deps -T "$strategy_service" \
      refresh-approval-runtime \
      --account-label "$account" \
      --strategy orderflow_impulse \
      --git-commit-hash "$target_commit" \
      --migration-revision "$(migration_revision_for_account "$account")" \
      </dev/null
  }

  renew_lease_for_pair() {
    local pair="$1"
    local account execution_service strategy_service lease_owner
    IFS=: read -r account execution_service strategy_service <<<"$pair"
    lease_owner="$(lease_owner_for_account "$account")"
    echo "renew lease $account"
    "${compose[@]}" run --rm --no-deps -T "$strategy_service" renew-lease \
      --account-label "$account" \
      --strategy orderflow_impulse \
      --lease-owner "$lease_owner" \
      --lease-ttl-seconds 3600 \
      --confirmation "RENEW LIVE RISK LEASE" </dev/null >/dev/null
  }

  preflight_pair() {
    local pair="$1"
    local account execution_service strategy_service
    IFS=: read -r account execution_service strategy_service <<<"$pair"
    echo "preflight $account"
    "${compose[@]}" run --rm --no-deps -T "$strategy_service" preflight \
      --account-label "$account" \
      --strategy orderflow_impulse \
      --strict \
      --expected-git-commit "$target_commit" \
      --expected-migration-revision "$(migration_revision_for_account "$account")" \
      </dev/null
  }

  run_parallel_pairs() {
    local action="$1"
    shift
    local pair
    local -a pids=()
    for pair in "$@"; do
      case "$action" in
        refresh) refresh_approval_for_pair "$pair" & ;;
        renew) renew_lease_for_pair "$pair" & ;;
        preflight) preflight_pair "$pair" & ;;
        *) echo "unknown parallel action: $action" >&2; return 64 ;;
      esac
      pids+=("$!")
      if (( ${#pids[@]} >= live_concurrency )); then
        if ! wait_for_batch "${pids[@]}"; then
          return 1
        fi
        pids=()
      fi
    done
    if (( ${#pids[@]} > 0 )); then
      wait_for_batch "${pids[@]}"
    fi
  }

  active_pairs=()
  for pair in "${live_pairs[@]}"; do
    IFS=: read -r account execution_service strategy_service <<<"$pair"
    if is_running "$strategy_service"; then
      active_pairs+=("$pair")
    fi
  done

fi

# Build once. The Dockerfile keeps dependency installation in a layer keyed by
# pyproject.toml, so ordinary source changes only rebuild the application.
deploy_phase=build
write_deploy_state running "$deploy_phase"
if [[ "$runtime_changed" == 1 ]]; then
  build_started_at="$(date +%s)"
  "${compose[@]}" build
  echo "phase=build elapsed_seconds=$(( $(date +%s) - build_started_at ))"
else
  echo "phase=build skipped runtime_unchanged=1"
fi

# Nginx exposes the dashboard on the host's 8765 port. Keep an already
# healthy dashboard in place, but recover a Created, stopped, or unhealthy
# dashboard before reporting a successful application deployment. This does
# not enable a disabled Live account; it protects the configured operator UI.
dashboard_needs_start=0
deploy_phase=dashboard
write_deploy_state running "$deploy_phase"
if [[ "$dashboard_required" == 1 ]] && ! is_healthy dashboard; then
  dashboard_needs_start=1
fi
if [[ "$dashboard_changed" == 1 || "$dashboard_needs_start" == 1 ]]; then
  dashboard_started_at="$(date +%s)"
  "${compose[@]}" up -d --no-deps --wait --wait-timeout "$deploy_wait_timeout" dashboard
  echo "phase=dashboard elapsed_seconds=$(( $(date +%s) - dashboard_started_at ))"
fi

dashboard_check_required=0
if [[ "$dashboard_required" == 1 || "$dashboard_changed" == 1 ]]; then
  dashboard_check_required=1
fi
if [[ "$dashboard_check_required" == 1 ]]; then
  dashboard_health_started_at="$(date +%s)"
  if ! is_healthy dashboard; then
    echo "dashboard is not healthy; refusing to report a successful deployment" >&2
    exit 1
  fi
  if ! command -v curl >/dev/null 2>&1; then
    echo "dashboard verification requires curl on the server" >&2
    exit 69
  fi
  if ! curl --fail --silent --show-error --max-time 10 \
    http://127.0.0.1:8765/api/health >/dev/null; then
    echo "dashboard health endpoint is unavailable on 127.0.0.1:8765" >&2
    exit 1
  fi
  if [[ "$dashboard_required" == 1 ]]; then
    if ! curl --fail --silent --show-error --location --max-time 10 \
      "$dashboard_proxy_url" >/dev/null; then
      echo "dashboard reverse-proxy health endpoint is unavailable: $dashboard_proxy_url" >&2
      exit 1
    fi
  fi
  echo "phase=dashboard-health elapsed_seconds=$(( $(date +%s) - dashboard_health_started_at ))"
fi

if [[ "$live_update" == 1 && "$live_changed" == 1 ]]; then
  deploy_phase=live-preflight
  write_deploy_state running "$deploy_phase"
  # Validate approvals with the freshly built image before restarting any
  # consumer or execution service. A bad approval now fails in seconds after
  # the build instead of waiting for a Live healthcheck to turn unhealthy.
  if [[ "$refresh_approvals" == 1 ]]; then
    approval_refresh_started_at="$(date +%s)"
    if ! run_parallel_pairs refresh "${active_pairs[@]}"; then
      echo "approval refresh failed; Live services were not restarted" >&2
      exit 1
    fi
    echo "phase=approval-refresh elapsed_seconds=$(( $(date +%s) - approval_refresh_started_at ))"
  fi

  lease_started_at="$(date +%s)"
  if ! run_parallel_pairs renew "${active_pairs[@]}"; then
    echo "lease renewal failed; Live services were not restarted" >&2
    exit 1
  fi
  echo "phase=lease-renew elapsed_seconds=$(( $(date +%s) - lease_started_at ))"

  preflight_started_at="$(date +%s)"
  if ! run_parallel_pairs preflight "${active_pairs[@]}"; then
    echo "preflight failed; Live services were not restarted" >&2
    exit 1
  fi
  echo "phase=preflight elapsed_seconds=$(( $(date +%s) - preflight_started_at ))"
  live_preflight_complete=1
fi

# Preserve the durable research cursor before the Hub's stream epoch changes.
# Otherwise an old collector can observe the new Hub first, reset its cursor,
# and make the new collector look like a fresh subscriber with no gap to heal.
if [[ "$market_changed" == 1 && "$research_changed" == 1 ]] \
  && is_running research-collector; then
  deploy_phase=research-stop
  write_deploy_state running "$deploy_phase"
  research_stop_started_at="$(date +%s)"
  "${compose[@]}" stop --timeout 60 research-collector
  echo "phase=research-stop elapsed_seconds=$(( $(date +%s) - research_stop_started_at ))"
fi

# market-data must be ready before research and strategy consumers restart.
if [[ "$market_changed" == 1 ]]; then
  deploy_phase=market-data
  write_deploy_state running "$deploy_phase"
  market_started_at="$(date +%s)"
  "${compose[@]}" up -d --no-deps --wait --wait-timeout "$deploy_wait_timeout" market-data
  echo "phase=market-data elapsed_seconds=$(( $(date +%s) - market_started_at ))"
fi

consumer_services=()
verification_services=()
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
if (( ${#consumer_services[@]} > 0 )); then
  deploy_phase=consumers
  write_deploy_state running "$deploy_phase"
  consumers_started_at="$(date +%s)"
  "${compose[@]}" up -d --no-deps --wait --wait-timeout "$deploy_wait_timeout" "${consumer_services[@]}"
  echo "phase=consumers elapsed_seconds=$(( $(date +%s) - consumers_started_at ))"
fi
if [[ "$dashboard_changed" == 1 || "$dashboard_needs_start" == 1 ]]; then
  verification_services+=(dashboard)
fi
if [[ "$market_changed" == 1 ]]; then
  verification_services+=(market-data)
fi
verification_services+=("${consumer_services[@]}")

if [[ "$live_update" == 1 && "$live_changed" == 1 ]]; then
  deploy_phase=live-restart
  write_deploy_state running "$deploy_phase"
  if [[ "$live_preflight_complete" != 1 ]]; then
    echo "live preflight did not complete" >&2
    exit 1
  fi

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
    execution_started_at="$(date +%s)"
    echo "update execution wave (${#execution_services[@]} services)"
    "${compose[@]}" --parallel "$live_concurrency" up -d --no-deps --wait \
      --wait-timeout "$deploy_wait_timeout" \
      "${execution_services[@]}"
    echo "phase=execution elapsed_seconds=$(( $(date +%s) - execution_started_at ))"
  else
    echo "phase=execution skipped no_active_services=1"
  fi

  strategy_services=()
  for pair in "${active_pairs[@]}"; do
    IFS=: read -r account execution_service strategy_service <<<"$pair"
    if is_running "$strategy_service"; then
      strategy_services+=("$strategy_service")
    fi
  done
  if (( ${#strategy_services[@]} > 0 )); then
    strategy_started_at="$(date +%s)"
    echo "update strategy wave (${#strategy_services[@]} services)"
    "${compose[@]}" --parallel "$live_concurrency" up -d --no-deps --wait \
      --wait-timeout "$deploy_wait_timeout" \
      "${strategy_services[@]}"
    echo "phase=strategy elapsed_seconds=$(( $(date +%s) - strategy_started_at ))"
  else
    echo "phase=strategy skipped no_active_services=1"
  fi
  verification_services+=("${execution_services[@]}" "${strategy_services[@]}")
  echo "phase=live elapsed_seconds=$(( $(date +%s) - live_started_at ))"
fi

verification_started_at="$(date +%s)"
deploy_phase=verify
write_deploy_state running "$deploy_phase"
for service in "${verification_services[@]}"; do
  verify_service_target "$service"
done
echo "phase=verify elapsed_seconds=$(( $(date +%s) - verification_started_at )) services=${#verification_services[@]}"

write_deploy_state success complete
echo "phase=total elapsed_seconds=$(( $(date +%s) - deploy_started_at ))"

echo "deployed_commit=$target_commit"
docker ps --format '{{.Names}}|{{.Image}}|{{.Status}}' \
  | grep -E 'crypto-momentum-lab-(dashboard|market-data|research-collector|paper-|execution-account-live|live-strategy)' \
  | sort
REMOTE_SCRIPT
then
  ssh_status=0
else
  ssh_status=$?
fi
echo "phase=client-total elapsed_seconds=$(( $(date +%s) - client_started_at ))"
exit "$ssh_status"
