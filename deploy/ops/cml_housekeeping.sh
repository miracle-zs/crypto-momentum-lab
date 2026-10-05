#!/usr/bin/env bash
# Keep host-only operational artefacts bounded without touching PostgreSQL data
# or the cold-data archive.  This is intentionally a separate daily task: a
# deployment should not spend its critical path deleting large cache trees.
set -Eeuo pipefail

crash_log_directory="${CML_CRASH_LOG_DIRECTORY:-/var/lib/crypto-momentum-lab/crash-logs}"
crash_log_retention_days="${CML_CRASH_LOG_RETENTION_DAYS:-7}"
app_image_retention_count="${CML_APP_IMAGE_RETENTION_COUNT:-3}"
build_cache_retention_hours="${CML_BUILD_CACHE_RETENTION_HOURS:-168}"
journal_max_size="${CML_JOURNAL_MAX_SIZE:-300M}"
readonly app_repository="crypto-momentum-lab-app"

for numeric_setting in \
  "$crash_log_retention_days" \
  "$app_image_retention_count" \
  "$build_cache_retention_hours"; do
  if ! [[ "$numeric_setting" =~ ^[0-9]+$ ]]; then
    echo "retention settings must be non-negative integers" >&2
    exit 64
  fi
done

if [[ "$app_image_retention_count" == 0 ]]; then
  echo "CML_APP_IMAGE_RETENTION_COUNT must retain at least one image" >&2
  exit 64
fi

# Docker logs archived by the operational monitor are incident evidence.  Keep
# a recent investigation window, but never let restarts turn this directory
# into unbounded host storage.  -xdev prevents an accidental traversal into a
# mounted filesystem below the configured directory.
if [[ -d "$crash_log_directory" ]]; then
  find "$crash_log_directory" -xdev -type f -name "*.log" \
    -mtime "+$crash_log_retention_days" -delete
fi

# A running container is an invariant, not a heuristic: retain every image
# referenced by one, then retain the newest N application image IDs for a fast
# rollback.  Image IDs rather than tags avoid deleting a multi-tagged image.
declare -A keep_image_ids=()
while IFS= read -r image_id; do
  [[ -n "$image_id" ]] && keep_image_ids["$image_id"]=1
done < <(docker ps -q | xargs -r docker inspect --format '{{.Image}}')

mapfile -t app_image_ids < <(
  docker image ls "$app_repository" --no-trunc --format '{{.ID}}' | awk '!seen[$0]++'
)
for ((index = 0; index < app_image_retention_count && index < ${#app_image_ids[@]}; index++)); do
  keep_image_ids["${app_image_ids[$index]}"]=1
done

for image_id in "${app_image_ids[@]}"; do
  if [[ -n "${keep_image_ids[$image_id]:-}" ]]; then
    continue
  fi
  # An image can have more than one historical application tag.  Untag all of
  # those tags, but never force-remove an ID that another repository retains.
  while IFS= read -r tag; do
    [[ "$tag" == "$app_repository":* ]] && docker image rm "$tag"
  done < <(docker image inspect --format '{{range .RepoTags}}{{println .}}{{end}}' "$image_id")
done

docker image prune --force
docker builder prune --all --force --filter "until=${build_cache_retention_hours}h"
docker volume prune --force
journalctl --vacuum-size="$journal_max_size"
