#!/usr/bin/env bash

set -Eeuo pipefail

usage() {
  cat <<'USAGE'
Usage: update_server.sh <server-host> [git-ref] [--live] [--refresh-approvals]

Environment:
  CML_SERVER_USER  SSH user (default: root)
  CML_REMOTE_DIR   checkout on the server (default: /opt/crypto-momentum-lab)
  CML_LIVE_CONCURRENCY  maximum parallel Live services (default: 2)
  CML_DEPLOY_WAIT_TIMEOUT_SECONDS  general health wait timeout (default: 300)
  CML_MARKET_DATA_WAIT_TIMEOUT_SECONDS  market-data health timeout (default: 900)
  CML_CONSUMER_WAIT_TIMEOUT_SECONDS  Paper/research health timeout (default: 300)
  CML_LIVE_WAIT_TIMEOUT_SECONDS  Live health timeout (default: 300)
  CML_DEPLOY_OPERATION_TIMEOUT_SECONDS  individual Docker operation timeout (default: 300)
  CML_DEPLOY_BUILD_TIMEOUT_SECONDS  image build timeout (default: 900)
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
deploy_wait_timeout="${CML_DEPLOY_WAIT_TIMEOUT_SECONDS:-300}"
market_data_wait_timeout="${CML_MARKET_DATA_WAIT_TIMEOUT_SECONDS:-900}"
consumer_wait_timeout="${CML_CONSUMER_WAIT_TIMEOUT_SECONDS:-300}"
live_wait_timeout="${CML_LIVE_WAIT_TIMEOUT_SECONDS:-300}"
deploy_operation_timeout="${CML_DEPLOY_OPERATION_TIMEOUT_SECONDS:-300}"
deploy_build_timeout="${CML_DEPLOY_BUILD_TIMEOUT_SECONDS:-900}"
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

for timeout_value in \
  "$deploy_wait_timeout" \
  "$market_data_wait_timeout" \
  "$consumer_wait_timeout" \
  "$live_wait_timeout" \
  "$deploy_operation_timeout" \
  "$deploy_build_timeout"; do
  if ! [[ "$timeout_value" =~ ^[1-9][0-9]*$ ]]; then
    echo "Invalid deployment timeout: $timeout_value" >&2
    exit 64
  fi
done

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
  "$deploy_wait_timeout" "$market_data_wait_timeout" \
  "$consumer_wait_timeout" "$live_wait_timeout" \
  "$deploy_operation_timeout" "$deploy_build_timeout" \
  "$refresh_approvals" "$dashboard_required" "$dashboard_proxy_url" \
  <<'REMOTE_SCRIPT'
set -Eeuo pipefail

remote_dir="$1"
target_ref="$2"
live_update="$3"
live_concurrency="$4"
deploy_wait_timeout="$5"
market_data_wait_timeout="$6"
consumer_wait_timeout="$7"
live_wait_timeout="$8"
deploy_operation_timeout="$9"
deploy_build_timeout="${10}"
refresh_approvals="${11}"
dashboard_required="${12}"
dashboard_proxy_url="${13}"
for timeout_name in \
  CML_DEPLOY_WAIT_TIMEOUT_SECONDS \
  CML_MARKET_DATA_WAIT_TIMEOUT_SECONDS \
  CML_CONSUMER_WAIT_TIMEOUT_SECONDS \
  CML_LIVE_WAIT_TIMEOUT_SECONDS \
  CML_DEPLOY_OPERATION_TIMEOUT_SECONDS \
  CML_DEPLOY_BUILD_TIMEOUT_SECONDS; do
  case "$timeout_name" in
    CML_DEPLOY_WAIT_TIMEOUT_SECONDS) timeout_value="$deploy_wait_timeout" ;;
    CML_MARKET_DATA_WAIT_TIMEOUT_SECONDS) timeout_value="$market_data_wait_timeout" ;;
    CML_CONSUMER_WAIT_TIMEOUT_SECONDS) timeout_value="$consumer_wait_timeout" ;;
    CML_LIVE_WAIT_TIMEOUT_SECONDS) timeout_value="$live_wait_timeout" ;;
    CML_DEPLOY_OPERATION_TIMEOUT_SECONDS) timeout_value="$deploy_operation_timeout" ;;
    CML_DEPLOY_BUILD_TIMEOUT_SECONDS) timeout_value="$deploy_build_timeout" ;;
  esac
  if ! [[ "$timeout_value" =~ ^[1-9][0-9]*$ ]]; then
    echo "Invalid $timeout_name: $timeout_value" >&2
    exit 64
  fi
done
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
if ! command -v timeout >/dev/null 2>&1; then
  echo "Refusing deployment: timeout is required for bounded operations" >&2
  exit 69
fi
cd "$remote_dir"
deploy_started_at="$(date +%s)"

run_with_timeout() {
  local label="$1"
  local timeout_seconds="$2"
  shift 2
  local started_at status
  started_at="$(date +%s)"
  echo "operation=start name=$label timeout_seconds=$timeout_seconds"
  if timeout --foreground --kill-after=30s "$timeout_seconds" "$@" </dev/null; then
    status=0
  else
    status=$?
  fi
  echo "operation=end name=$label status=$status elapsed_seconds=$(( $(date +%s) - started_at ))"
  return "$status"
}

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
deploy_state_checkout=""
deploy_state_runtime=""
deploy_state_image=""
deploy_state_status=""
deploy_state_phase=""
deploy_state_base=""
if [[ -f "$deploy_state_file" ]]; then
  deploy_state_target="$(sed -n 's/^target_commit=//p' "$deploy_state_file" | tail -n 1)"
  deploy_state_checkout="$(sed -n 's/^checkout_commit=//p' "$deploy_state_file" | tail -n 1)"
  deploy_state_runtime="$(sed -n 's/^runtime_commit=//p' "$deploy_state_file" | tail -n 1)"
  deploy_state_image="$(sed -n 's/^image_commit=//p' "$deploy_state_file" | tail -n 1)"
  deploy_state_status="$(sed -n 's/^status=//p' "$deploy_state_file" | tail -n 1)"
  deploy_state_phase="$(sed -n 's/^phase=//p' "$deploy_state_file" | tail -n 1)"
  deploy_state_base="$(sed -n 's/^base_commit=//p' "$deploy_state_file" | tail -n 1)"
fi

write_deploy_state() {
  local status="$1"
  local phase="$2"
  local state_tmp="${deploy_state_file}.tmp"
  umask 077
  {
    printf 'target_commit=%s\n' "${target_commit:-}"
    printf 'base_commit=%s\n' "${deployment_base_commit:-}"
    printf 'checkout_commit=%s\n' "${target_commit:-}"
    printf 'runtime_commit=%s\n' "${runtime_commit:-}"
    printf 'image_commit=%s\n' "${runtime_image_commit:-}"
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

run_with_timeout "git-fetch" "$deploy_operation_timeout" git fetch --prune origin main
if [[ "$(git branch --show-current)" != "main" ]]; then
  echo "Refusing deployment: checkout must be on main" >&2
  exit 1
fi

previous_commit="$(git rev-parse HEAD)"
target_commit="$(git rev-parse "$target_ref")"
git cat-file -e "$target_commit^{commit}"
if [[ "$target_commit" == "$previous_commit" ]]; then
  echo "checkout already at target commit $target_commit"
elif [[ "$(git merge-base "$previous_commit" "$target_commit")" == "$previous_commit" ]]; then
  git merge --ff-only "$target_commit"
elif [[ "$(git merge-base "$previous_commit" "$target_commit")" == "$target_commit" ]]; then
  # A previously deployed commit may be an intentional rollback target. The
  # checkout is clean above, so reset --keep moves only the local branch ref
  # and tracked files needed to reach that explicit commit.
  git reset --keep "$target_commit"
else
  echo "Refusing deployment: target is not an ancestor and cannot be fast-forwarded or rolled back safely" >&2
  exit 1
fi
if [[ "$(git rev-parse HEAD)" != "$target_commit" ]]; then
  echo "Refusing deployment: checkout did not reach target commit $target_commit" >&2
  exit 1
fi

env_runtime_commit=""
if [[ -f .env.server ]]; then
  env_runtime_commit="$(sed -n 's/^CML_CODE_COMMIT=//p' .env.server | tail -n 1)"
fi
runtime_commit="${deploy_state_runtime:-${env_runtime_commit:-${deploy_state_target:-$previous_commit}}}"
runtime_image_commit="${deploy_state_image:-$runtime_commit}"
previous_runtime_commit="$runtime_commit"
deployment_base_commit="$previous_commit"
# Keep the original diff across retries (including a newer target arriving
# during an incomplete rollout), rather than treating every service as changed.
if [[ "$deploy_state_status" != success && -n "$deploy_state_base" ]]; then
  git cat-file -e "$deploy_state_base^{commit}"
  deployment_base_commit="$deploy_state_base"
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
recovery_run=0
resume_from_phase=""
changed_files="$(git diff --name-only "$deployment_base_commit" "$target_commit")"
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
    src/crypto_momentum_lab/strategies/*|\
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
state_checkout_commit="${deploy_state_checkout:-$deploy_state_target}"
if [[ "$target_commit" == "$previous_commit" \
  && ( "$state_checkout_commit" != "$target_commit" || "$deploy_state_status" != "success" ) ]]; then
  recovery_run=1
  resume_from_phase="${deploy_state_phase:-checkout}"
  if [[ -n "$deploy_state_base" ]]; then
    : # The persisted base above restores the exact affected service groups.
  elif [[ "$runtime_commit" == "$target_commit" ]]; then
    runtime_changed=1
    market_changed=1
    research_changed=1
    paper_changed=1
    dashboard_changed=1
    if [[ "$live_update" == 1 ]]; then
      live_changed=1
    fi
  elif [[ "$live_update" == 1 ]]; then
    live_changed=1
  fi
  echo "recovery_run=1 reason=target_checkout_already_present resume_from_phase=$resume_from_phase runtime_commit=$runtime_commit"
fi

if [[ "$runtime_changed" == 1 ]]; then
  runtime_commit="$target_commit"
  runtime_image_commit="$target_commit"
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

set_env_value CML_CODE_COMMIT "$runtime_commit"
current_dashboard_image="$(sed -n 's/^CML_DASHBOARD_IMAGE=//p' .env.server | tail -n 1)"
if [[ -z "$current_dashboard_image" \
  || "$current_dashboard_image" == "crypto-momentum-lab-app:${previous_runtime_commit}" ]]; then
  set_env_value CML_DASHBOARD_IMAGE "crypto-momentum-lab-app:${runtime_commit}"
else
  echo "dashboard_image_preserved=1"
fi
chmod 600 .env.server

compose=(
  docker compose
  --env-file .env.server
  -f compose.server.yaml
)
if [[ "$live_update" == 1 ]]; then
  compose+=(
    -f compose.live.accounts.yaml
    --profile live
  )
fi
dashboard_image="$(sed -n 's/^CML_DASHBOARD_IMAGE=//p' .env.server | tail -n 1)"
if [[ -z "$dashboard_image" ]]; then
  dashboard_image="crypto-momentum-lab-app:${runtime_commit}"
fi
deploy_phase=compose

phase_rank() {
  case "$1" in
    checkout) echo 0 ;;
    compose) echo 1 ;;
    build) echo 2 ;;
    migrate) echo 3 ;;
    volume-init) echo 4 ;;
    dashboard) echo 5 ;;
    live-preflight) echo 6 ;;
    research-stop) echo 7 ;;
    market-data) echo 8 ;;
    consumers) echo 9 ;;
    live-restart) echo 10 ;;
    verify) echo 11 ;;
    complete) echo 12 ;;
    *) echo 0 ;;
  esac
}

should_run_phase() {
  local phase="$1"
  if [[ "$recovery_run" != 1 ]]; then
    return 0
  fi
  local current_rank resume_rank
  current_rank="$(phase_rank "$phase")"
  resume_rank="$(phase_rank "$resume_from_phase")"
  if (( current_rank >= resume_rank )); then
    return 0
  fi
  return 1
}

image_exists() {
  docker image inspect "crypto-momentum-lab-app:${runtime_image_commit}" \
    >/dev/null 2>&1
}

expected_image_for_service() {
  if [[ "$1" == "dashboard" ]]; then
    printf '%s' "$dashboard_image"
  else
    printf 'crypto-momentum-lab-app:%s' "$runtime_image_commit"
  fi
}

failure_service=""

print_service_logs() {
  local service="$1"
  echo "failure_logs_service=$service" >&2
  "${compose[@]}" logs --no-color --tail=200 "$service" >&2 || true
}

print_failure_context() {
  local status="$1"
  echo "deployment_failed=1 exit_code=$status checkout=$(git rev-parse HEAD 2>/dev/null || echo unknown)" >&2
  echo "failure_service_status:" >&2
  "${compose[@]}" ps >&2 || true
  echo "failure_container_status:" >&2
  docker ps --format '{{.Names}}|{{.Image}}|{{.Status}}' \
    | grep -E 'crypto-momentum-lab-(dashboard|market-data|research-collector|paper-|execution-account-live|live-strategy)' \
    | sort >&2 || true
  if [[ -n "$failure_service" ]]; then
    print_service_logs "$failure_service"
  fi
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
if should_run_phase compose; then
  deploy_phase=compose
  write_deploy_state running "$deploy_phase"
  run_with_timeout "compose-config" "$deploy_operation_timeout" \
    "${compose[@]}" config --quiet
else
  echo "phase=compose skipped resume_from_phase=$resume_from_phase"
fi

service_status() {
  local service="$1"
  local container_id
  container_id="$("${compose[@]}" ps -q "$service" 2>/dev/null || true)"
  if [[ -z "$container_id" ]]; then
    printf 'missing|missing\n'
    return 0
  fi
  docker inspect -f '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
    "$container_id" 2>/dev/null || printf 'missing|missing\n'
}

is_running() {
  local service="$1"
  [[ "$(service_status "$service")" == "running|"* ]]
}

is_healthy() {
  local service="$1"
  [[ "$(service_status "$service")" == "running|healthy" ]]
}

service_is_converged() {
  local service="$1"
  local container_id state image expected_image
  container_id="$("${compose[@]}" ps -q "$service" 2>/dev/null || true)"
  [[ -n "$container_id" ]] || return 1
  state="$(service_status "$service")"
  image="$(docker inspect -f '{{.Config.Image}}' "$container_id" 2>/dev/null || true)"
  expected_image="$(expected_image_for_service "$service")"
  [[ "$state" == "running|healthy" \
    && "$image" == "$expected_image" ]]
}

wait_for_services_healthy() {
  local timeout_seconds="$1"
  shift
  if ! [[ "$timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
    echo "Invalid health wait timeout: $timeout_seconds" >&2
    return 64
  fi
  local deadline=$(( $(date +%s) + timeout_seconds ))
  local service status state health all_ready
  if (( $# == 0 )); then
    return 0
  fi
  while :; do
    all_ready=1
    for service in "$@"; do
      status="$(service_status "$service")"
      state="${status%%|*}"
      health="${status#*|}"
      if [[ "$state" != running || "$health" == unhealthy ]]; then
        failure_service="$service"
        echo "service failed during health wait: service=$service status=$status" >&2
        print_service_logs "$service"
        return 1
      fi
      if [[ "$state" != running \
        || ( "$health" != healthy && "$health" != none ) ]]; then
        all_ready=0
      fi
    done
    if (( all_ready == 1 )); then
      return 0
    fi
    if (( $(date +%s) >= deadline )); then
      failure_service="${service:-unknown}"
      echo "service health wait timed out: service=$failure_service timeout_seconds=$timeout_seconds" >&2
      print_service_logs "$failure_service"
      return 1
    fi
    sleep 5
  done
}

up_and_wait() {
  local health_timeout="$1"
  shift
  if (( $# == 0 )); then
    return 0
  fi
  failure_service="$1"
  run_with_timeout "compose-up:$*" "$deploy_operation_timeout" \
    "${compose[@]}" up -d --force-recreate --no-deps "$@"
  wait_for_services_healthy "$health_timeout" "$@"
}

up_and_wait_parallel() {
  local health_timeout="$1"
  local parallel="$2"
  shift 2
  if (( $# == 0 )); then
    return 0
  fi
  failure_service="$1"
  run_with_timeout "compose-up:$*" "$deploy_operation_timeout" \
    "${compose[@]}" --parallel "$parallel" up -d --force-recreate --no-deps "$@"
  wait_for_services_healthy "$health_timeout" "$@"
}

verify_service_target() {
  local service="$1"
  local expected_image
  local container_id state image
  expected_image="$(expected_image_for_service "$service")"
  container_id="$("${compose[@]}" ps -q "$service" 2>/dev/null || true)"
  if [[ -z "$container_id" ]]; then
    echo "verification failed: service $service has no container" >&2
    return 1
  fi
  state="$(service_status "$service")"
  image="$(docker inspect -f '{{.Config.Image}}' "$container_id" 2>/dev/null || true)"
  if [[ "$state" != "running|healthy" || "$image" != "$expected_image" ]]; then
    echo "verification failed: service=$service state=$state image=$image expected_image=$expected_image" >&2
    return 1
  fi
}

live_preflight_complete=0
if [[ "$recovery_run" == 1 ]] \
  && (( $(phase_rank "$resume_from_phase") > $(phase_rank live-preflight) )); then
  live_preflight_complete=1
fi
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

  position_label_is_configured() {
    local required_label="$1"
    local configured_labels="$2"
    local label
    local -a labels=()
    IFS=',' read -r -a labels <<<"$configured_labels"
    for label in "${labels[@]}"; do
      # Match the same comma-separated, whitespace-tolerant format accepted
      # by market-data. Empty entries are ignored here; the application parser
      # still rejects malformed values before starting the service.
      label="${label#${label%%[![:space:]]*}}"
      label="${label%${label##*[![:space:]]}}"
      if [[ "$label" == "$required_label" ]]; then
        return 0
      fi
    done
    return 1
  }

  validate_live_position_labels() {
    local pair account execution_service strategy_service required_label
    local configured_labels
    configured_labels="$(env_value CML_LIVE_ACCOUNT_LABEL primary),$(env_value CML_LIVE_POSITION_ACCOUNT_LABELS '')"
    for pair in "${live_pairs[@]}"; do
      IFS=: read -r account execution_service strategy_service <<<"$pair"
      if ! is_running "$strategy_service" && ! is_running "$execution_service"; then
        continue
      fi
      if [[ "$account" == primary ]]; then
        required_label="$(env_value CML_LIVE_ACCOUNT_LABEL primary)"
      else
        required_label="$account"
      fi
      if ! position_label_is_configured "$required_label" "$configured_labels"; then
        echo "Refusing live update: running account $account ($required_label) is absent from CML_LIVE_POSITION_ACCOUNT_LABELS" >&2
        return 1
      fi
    done
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
    run_with_timeout "refresh-approval:$account" "$deploy_operation_timeout" \
      "${compose[@]}" run --rm --no-deps -T "$strategy_service" \
        refresh-approval-runtime \
        --account-label "$account" \
        --strategy orderflow_impulse \
        --git-commit-hash "$runtime_commit" \
        --migration-revision "$(migration_revision_for_account "$account")" \
        </dev/null
  }

  renew_lease_for_pair() {
    local pair="$1"
    local account execution_service strategy_service lease_owner
    IFS=: read -r account execution_service strategy_service <<<"$pair"
    lease_owner="$(lease_owner_for_account "$account")"
    echo "renew lease $account"
    run_with_timeout "renew-lease:$account" "$deploy_operation_timeout" \
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
    run_with_timeout "preflight:$account" "$deploy_operation_timeout" \
      "${compose[@]}" run --rm --no-deps -T "$strategy_service" preflight \
        --account-label "$account" \
        --strategy orderflow_impulse \
        --strict \
        --expected-git-commit "$runtime_commit" \
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
      if [[ "$refresh_approvals" != 1 ]] \
        && service_is_converged "$execution_service" \
        && service_is_converged "$strategy_service"; then
        echo "phase=live account=$account skipped converged=1"
      else
        active_pairs+=("$pair")
      fi
    fi
  done
  if ! validate_live_position_labels; then
    exit 1
  fi

fi

# Build once. The Dockerfile keeps dependency installation in a layer keyed by
# pyproject.toml, so ordinary source changes only rebuild the application.
deploy_phase=build
if should_run_phase build; then
  write_deploy_state running "$deploy_phase"
  if [[ "$runtime_changed" == 1 ]]; then
    if image_exists; then
      echo "phase=build skipped target_image_exists=1"
    else
      build_started_at="$(date +%s)"
      run_with_timeout "compose-build" "$deploy_build_timeout" \
        "${compose[@]}" build
      echo "phase=build elapsed_seconds=$(( $(date +%s) - build_started_at ))"
    fi
  else
    echo "phase=build skipped runtime_unchanged=1"
  fi
else
  echo "phase=build skipped resume_from_phase=$resume_from_phase"
fi

# Apply schema changes before any service is restarted with --no-deps. The
# execution-account processes can touch newly added tables during startup, so
# running migrations only through Compose dependency ordering is not enough
# when a deployment resumes after a partial rollout.
deploy_phase=migrate
if should_run_phase migrate && [[ "$runtime_changed" == 1 ]]; then
  write_deploy_state running "$deploy_phase"
  migration_started_at="$(date +%s)"
  failure_service=migrate
  run_with_timeout "compose-up:postgres" "$deploy_operation_timeout" \
    "${compose[@]}" up -d postgres
  failure_service=postgres
  wait_for_services_healthy "$deploy_wait_timeout" postgres
  failure_service=migrate
  run_with_timeout "migration" "$deploy_operation_timeout" \
    "${compose[@]}" run --rm --no-deps migrate </dev/null
  echo "phase=migrate elapsed_seconds=$(( $(date +%s) - migration_started_at ))"
else
  echo "phase=migrate skipped runtime_unchanged=$runtime_changed"
fi

# Initialize the named data volumes before any service is restarted with
# --no-deps.  In particular, execution-account needs to create the shared
# Binance request-pacer lock as the unprivileged cml user.
deploy_phase=volume-init
if should_run_phase volume-init && [[ "$runtime_changed" == 1 ]]; then
  write_deploy_state running "$deploy_phase"
  volume_init_needed=1
  if run_with_timeout "volume-init-check" "$deploy_operation_timeout" \
    "${compose[@]}" run --rm --no-deps -T --entrypoint sh volume-init -c '
      for path in /app/data /app/research-data /run/cml/binance-rest-pacer; do
        if [ ! -d "$path" ]; then
          exit 0
        fi
        if find "$path" -maxdepth 1 \( ! -user cml -o ! -group cml \) -print -quit | grep -q .; then
          exit 0
        fi
      done
      exit 1
    ' </dev/null; then
    volume_init_needed=1
  else
    volume_check_status=$?
    if (( volume_check_status == 1 )); then
      volume_init_needed=0
    else
      echo "volume ownership check failed status=$volume_check_status" >&2
      exit "$volume_check_status"
    fi
  fi
  if (( volume_init_needed == 1 )); then
    volume_init_started_at="$(date +%s)"
    failure_service=volume-init
    run_with_timeout "volume-init" "$deploy_operation_timeout" \
      "${compose[@]}" run --rm --no-deps volume-init </dev/null
    echo "phase=volume-init elapsed_seconds=$(( $(date +%s) - volume_init_started_at ))"
  else
    echo "phase=volume-init skipped ownership=correct"
  fi
else
  echo "phase=volume-init skipped runtime_unchanged=$runtime_changed"
fi

# Nginx exposes the dashboard on the host's 8765 port. Keep an already
# healthy dashboard in place, but recover a Created, stopped, or unhealthy
# dashboard before reporting a successful application deployment. This does
# not enable a disabled Live account; it protects the configured operator UI.
dashboard_needs_start=0
deploy_phase=dashboard
if should_run_phase dashboard; then
  write_deploy_state running "$deploy_phase"
  if [[ "$dashboard_required" == 1 ]] && ! is_healthy dashboard; then
    dashboard_needs_start=1
  fi
  if [[ "$dashboard_changed" == 1 || "$dashboard_needs_start" == 1 ]] \
    && ! service_is_converged dashboard; then
    dashboard_started_at="$(date +%s)"
    up_and_wait "$deploy_wait_timeout" dashboard
    echo "phase=dashboard elapsed_seconds=$(( $(date +%s) - dashboard_started_at ))"
  else
    echo "phase=dashboard skipped converged=1"
  fi
else
  echo "phase=dashboard skipped resume_from_phase=$resume_from_phase"
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

if [[ "$live_update" == 1 && "$live_changed" == 1 ]] \
  && should_run_phase live-preflight; then
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
if should_run_phase research-stop \
  && [[ "$market_changed" == 1 && "$research_changed" == 1 ]] \
  && is_running research-collector; then
  deploy_phase=research-stop
  write_deploy_state running "$deploy_phase"
  research_stop_started_at="$(date +%s)"
  run_with_timeout "research-stop" "$deploy_operation_timeout" \
    "${compose[@]}" stop --timeout 60 research-collector
  echo "phase=research-stop elapsed_seconds=$(( $(date +%s) - research_stop_started_at ))"
fi

# market-data must be ready before research and strategy consumers restart.
if should_run_phase market-data && [[ "$market_changed" == 1 ]]; then
  deploy_phase=market-data
  write_deploy_state running "$deploy_phase"
  if service_is_converged market-data; then
    echo "phase=market-data skipped converged=1"
  else
    market_started_at="$(date +%s)"
    up_and_wait "$market_data_wait_timeout" market-data
    echo "phase=market-data elapsed_seconds=$(( $(date +%s) - market_started_at ))"
  fi
fi

consumer_candidates=()
verification_services=()
if [[ "$research_changed" == 1 ]]; then
  consumer_candidates+=(research-collector)
fi
if [[ "$paper_changed" == 1 ]]; then
  consumer_candidates+=(
    paper-orderflow-pair
    paper-orderflow-gainer10-pair
    paper-b1-gainer100
    paper-b1-gainer100-ema
  )
fi
consumer_services=()
for service in "${consumer_candidates[@]}"; do
  if service_is_converged "$service"; then
    echo "phase=consumers service=$service skipped converged=1"
  else
    consumer_services+=("$service")
  fi
done
if should_run_phase consumers && (( ${#consumer_services[@]} > 0 )); then
  deploy_phase=consumers
  write_deploy_state running "$deploy_phase"
  consumers_started_at="$(date +%s)"
  up_and_wait "$consumer_wait_timeout" "${consumer_services[@]}"
  echo "phase=consumers elapsed_seconds=$(( $(date +%s) - consumers_started_at ))"
fi
if [[ "$dashboard_changed" == 1 || "$dashboard_needs_start" == 1 ]]; then
  verification_services+=(dashboard)
fi
if [[ "$market_changed" == 1 ]]; then
  verification_services+=(market-data)
fi
verification_services+=("${consumer_candidates[@]}")

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
  execution_candidates=()
  execution_services=()
  for pair in "${active_pairs[@]}"; do
    IFS=: read -r account execution_service strategy_service <<<"$pair"
    if is_running "$strategy_service"; then
      execution_candidates+=("$execution_service")
      if service_is_converged "$execution_service"; then
        echo "phase=execution service=$execution_service skipped converged=1"
      else
        execution_services+=("$execution_service")
      fi
    fi
  done
  if (( ${#execution_services[@]} > 0 )); then
    execution_started_at="$(date +%s)"
    echo "update execution wave (${#execution_services[@]} services)"
    up_and_wait_parallel "$live_wait_timeout" "$live_concurrency" "${execution_services[@]}"
    echo "phase=execution elapsed_seconds=$(( $(date +%s) - execution_started_at ))"
  else
    echo "phase=execution skipped no_active_services=1"
  fi

  strategy_candidates=()
  strategy_services=()
  for pair in "${active_pairs[@]}"; do
    IFS=: read -r account execution_service strategy_service <<<"$pair"
    if is_running "$strategy_service"; then
      strategy_candidates+=("$strategy_service")
      if service_is_converged "$strategy_service"; then
        echo "phase=strategy service=$strategy_service skipped converged=1"
      else
        strategy_services+=("$strategy_service")
      fi
    fi
  done
  if (( ${#strategy_services[@]} > 0 )); then
    strategy_started_at="$(date +%s)"
    echo "update strategy wave (${#strategy_services[@]} services)"
    up_and_wait_parallel "$live_wait_timeout" "$live_concurrency" "${strategy_services[@]}"
    echo "phase=strategy elapsed_seconds=$(( $(date +%s) - strategy_started_at ))"
  else
    echo "phase=strategy skipped no_active_services=1"
  fi
  verification_services+=(
    "${execution_candidates[@]}"
    "${strategy_candidates[@]}"
  )
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
echo "deployed_checkout=$target_commit"
echo "deployed_runtime=$runtime_commit"
echo "deployed_image=$runtime_image_commit"
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
