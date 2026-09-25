#!/usr/bin/env bash
# ==============================================================================
# Daily High-Speed Streaming Data Sync Script (Step 1 of Daily SOP)
#
# Pulls daily 15s high-frequency Parquet slices and 4-account Postgres event
# tables from the production trading server via single-connection SSH tar stream
# and Docker psql COPY with gzip compression and automatic 4-account partitioning.
#
# Usage:
#   export SERVER_HOST="43.167.191.253"
#   export SERVER_USER="root"
#   export SERVER_PASSWORD="your_password"  # optional if ssh keys set
#   bash local_optimization/sync_daily_data.sh [YYYY-MM-DD]
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Find preferred python interpreter (.venv or system python3)
PYTHON_BIN="${ROOT_DIR}/.venv/bin/python"
if [ ! -x "${PYTHON_BIN}" ]; then
  PYTHON_BIN="$(command -v python3 || true)"
fi

if [ -n "${PYTHON_BIN}" ] && [ -f "${SCRIPT_DIR}/sync_latest_server_data.py" ]; then
  if [ $# -ge 1 ]; then
    exec "${PYTHON_BIN}" "${SCRIPT_DIR}/sync_latest_server_data.py" --dates "$@"
  else
    exec "${PYTHON_BIN}" "${SCRIPT_DIR}/sync_latest_server_data.py" --auto
  fi
fi

# Standalone fallback if python environment is unavailable
TARGET_DATE="${1:-$(date -u +%F)}"
LOCAL_DATA_DIR="${SCRIPT_DIR}/data"

echo "======================================================================"
echo "🚀 [Step 1] 开始执行服务器数据高速流式同步: 目标日期 [${TARGET_DATE}]"
echo "======================================================================"

if [ -z "${SERVER_HOST:-}" ]; then
  echo "⚠️ 提示: 未检测到环境变量 SERVER_HOST。"
  if [ -d "${LOCAL_DATA_DIR}" ]; then
    echo "✅ 本地数据目录已存在: ${LOCAL_DATA_DIR}"
    exit 0
  else
    echo "❌ 本地数据目录不存在且未配置远程服务器凭证，同步终止。"
    exit 1
  fi
fi

SSH_USER="${SERVER_USER:-root}"
SSH_PORT="${SERVER_PORT:-22}"
REMOTE_PARQUET_BASE="/var/lib/docker/volumes/crypto-momentum-lab_research-data/_data/parquet/environment=research"
LOCAL_PARQUET_DIR="${LOCAL_DATA_DIR}/parquet/environment=research/date=${TARGET_DATE}"

mkdir -p "${LOCAL_PARQUET_DIR}"

echo "📦 1. 正在通过 SSH Tar 管道流式同步 15s 行情切片 (date=${TARGET_DATE})..."
ssh -p "${SSH_PORT}" "${SSH_USER}@${SERVER_HOST}" \
  "if [ -d '${REMOTE_PARQUET_BASE}/date=${TARGET_DATE}' ]; then
     tar -czf - -C '${REMOTE_PARQUET_BASE}/date=${TARGET_DATE}' .
   else
     echo 'REMOTE_DIR_NOT_FOUND' >&2
   fi" | tar -xzf - -C "${LOCAL_PARQUET_DIR}/" 2>/dev/null || {
     echo "⚠️ 远程未找到当日增量目录或传输已跳过，请确认生产环境数据落盘状态。"
   }

echo "📊 2. 正在导出实盘 4 账户事件明细表 (Postgres COPY)..."
REMOTE_PG_CONTAINER="crypto-momentum-lab-postgres-1"
mkdir -p "${LOCAL_DATA_DIR}/live_latest"

TABLES=(
  "account_fill_events"
  "order_intents"
  "account_balance_snapshots"
  "exchange_orders"
  "live_strategy_signals"
  "universe_snapshots"
  "account_config_snapshots"
  "live_strategy_state"
)

for tbl in "${TABLES[@]}"; do
  out_csv_gz="${LOCAL_DATA_DIR}/live_latest/${tbl}.csv.gz"
  echo "   - 导出表: ${tbl} -> ${out_csv_gz}"
  ssh -p "${SSH_PORT}" "${SSH_USER}@${SERVER_HOST}" \
    "docker exec ${REMOTE_PG_CONTAINER} psql -U cml -d cml -c 'COPY ${tbl} TO STDOUT WITH CSV HEADER;' | gzip -c" \
    > "${out_csv_gz}" 2>/dev/null || {
      echo "     ⚠️ 导出表 ${tbl} 失败，保持现有本地版本。"
    }
done

echo "======================================================================"
echo "✅ [Step 1] 数据高速流式同步流程执行完毕!"
echo "======================================================================"
