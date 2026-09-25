# 服务器采集数据：本地全量寻优操作手册

> 2026-09-18 设计更新：后续规范化方案见[本地参数寻优、稳定性跟踪与实盘校验设计](../superpowers/specs/2026-09-18-local-parameter-optimization-design.md)。本文仍记录旧脚本的实际用法，不代表新设计已实现。旧脚本的已平仓 PnL 回撤、Top10 代理及分别导出的表存在该设计所列限制；不能据此认定盯市回撤或逐笔实盘一致性已通过验证。

本手册用于反复执行下面这条研究流水线：

```text
服务器采集数据 → 本地时间戳快照 → 全量寻优 A-G → 冻结旧参数前向验证 → 实盘/策略对比图
```

它针对的是 `orderflow_impulse` 的本地研究回放，不会修改服务器配置、重启服务或发起真实交易。

## 1. 先固定口径

每次运行都遵守以下规则：

1. 所有时间使用 UTC；目录使用本次拉取的时间戳，不能覆盖历史结果。
2. “全量寻优”必须显式使用 `--selection-scope full`。脚本默认是验证集寻优，不能依赖默认值。
3. 只调整下面六个参数；实盘执行、退出、仓位和手续费设置由
   [`optimize_local_live_constrained.py`](../../scripts/optimize_local_live_constrained.py) 固定。
4. 保证金上限是参数组合的“自然峰值保证金”事前约束：整段回放中该组合本来需要的峰值必须不超过上限；不是运行到上限后再拒绝开仓。
5. 研究曲线是固定每笔 `100U` 名义仓位的绝对模拟 PnL。杠杆只影响初始保证金，不把 PnL 再乘一次。
6. 本地研究导出没有完整的历史 Top10 排名快照，回放使用本地采集币种生成的因果 Top10 代理；结果不能宣称与交易所实盘逐笔完全相同。

### 参数记法

图表和报告中常用 `I/C/R/B/N/CD` 表示：

| 缩写 | 脚本字段 | 单位/含义 |
|---|---|---|
| `I` | `impulse_window_buckets` | 15 秒状态窗口数 |
| `C` | `confirmation_buckets` | 确认窗口数 |
| `R` | `min_return_pct` | 报告按百分数存储；`1.00` 表示 1%，原始小数写法是 `0.01` |
| `B` | `min_imbalance` | 最小方向性不平衡 |
| `N` | `min_intensity` | 相对基准成交强度 |
| `CD` | `cooldown_buckets` | 每个 bucket 为 15 秒；`0` 表示无 cooldown |

当前 A-G 的约束定义如下：

| 策略 | 约束 | 目标 |
|---|---|---|
| A | 无保证金上限 | 全量绝对 PnL |
| B | 自然峰值保证金 ≤350U，自由 cooldown | 全量绝对 PnL |
| C | 自然峰值保证金 ≤350U，`cooldown=0` | 全量绝对 PnL |
| D | 自然峰值保证金 ≤280U，自由 cooldown | 全量绝对 PnL |
| E | 自然峰值保证金 ≤280U，`cooldown=0` | 全量绝对 PnL |
| F | 自然峰值保证金 ≤280U，`cooldown=0` | 全量 PnL − `0.10 ×` 全量最大回撤 |
| G | 自然峰值保证金 ≤280U，自由 cooldown | 全量 PnL − `0.10 ×` 全量最大回撤 |

## 2. 建立本次运行变量

在仓库根目录执行。服务器密码不要写入脚本、文档、shell history 或提交记录；优先使用 SSH key，密码登录时让 `ssh` 交互式提示。

```bash
set -euo pipefail

CML_SERVER_HOST="${CML_SERVER_HOST:-43.167.191.253}"
CML_SERVER_APP="${CML_SERVER_APP:-/opt/crypto-momentum-lab}"
CML_PULL_TAG="${CML_PULL_TAG:-$(date -u +%Y%m%d-%H%M%S)}"
CML_DATA_ROOT="server_exports/cml-research-data-${CML_PULL_TAG}"
CML_LIVE_ROOT="server_exports/cml-live-current-${CML_PULL_TAG}"
CML_PY=".venv/bin/python"

mkdir -p "$CML_DATA_ROOT" "$CML_LIVE_ROOT"
```

如果服务器代码不在 `/opt/crypto-momentum-lab`，先修改 `CML_SERVER_APP`，不要猜 Docker volume 在宿主机上的实际路径。

## 3. 拉取服务器研究数据

先检查研究采集器、市场数据和数据库服务：

```bash
ssh "root@${CML_SERVER_HOST}" \
  "cd '${CML_SERVER_APP}' && docker compose --env-file .env.server -f compose.server.yaml ps postgres market-data research-collector"
```

研究数据位于 Docker named volume 的 `/app/research-data`，不是仓库工作树。通过正在运行的 `research-collector` 容器打包到本地，避免依赖未经确认的 Docker volume 名称：

```bash
ssh "root@${CML_SERVER_HOST}" \
  "cd '${CML_SERVER_APP}' && docker compose --env-file .env.server -f compose.server.yaml exec -T research-collector tar -C /app/research-data -czf - ." \
  | tar -xzf - -C "$CML_DATA_ROOT"
```

完成后应存在：

```text
${CML_DATA_ROOT}/parquet/
${CML_DATA_ROOT}/checkpoints/
${CML_DATA_ROOT}/spool/
```

如果 `research-collector` 没有运行，先处理服务健康状态，不要临时启动第二个采集器来导出数据。

## 4. 拉取实盘对照数据

实盘权益和保证金对照需要从 PostgreSQL 导出以下四张表。下面命令导出完整历史，保证本地每次都是自洽快照；如果数据量明显增大，再单独实现按时间增量导出。

```bash
CML_TABLES=(
  account_balance_snapshots
  account_fill_events
  exchange_orders
  live_strategy_signals
)

for CML_TABLE in "${CML_TABLES[@]}"; do
  ssh "root@${CML_SERVER_HOST}" \
    "cd '${CML_SERVER_APP}' && docker compose --env-file .env.server -f compose.server.yaml exec -T postgres psql -X -q -v ON_ERROR_STOP=1 -U cml -d cml -c 'COPY public.${CML_TABLE} TO STDOUT WITH (FORMAT csv, HEADER true);'" \
    | gzip > "${CML_LIVE_ROOT}/${CML_TABLE}.csv.gz"
done
```

其中：

- `account_balance_snapshots` 是实盘账户权益曲线的来源；
- `account_fill_events` 和 `exchange_orders` 用于重建实盘保证金占用；
- `live_strategy_signals` 只用于记录实盘信号元数据和配置 hash。

## 5. 拉取后检查

不要仅看文件大小。至少检查 Parquet、checkpoint、实盘 CSV 都存在，并确认时间范围由报告实际读取出来：

```bash
test -d "${CML_DATA_ROOT}/parquet"
test -s "${CML_DATA_ROOT}/checkpoints/research.json"
test -s "${CML_LIVE_ROOT}/account_balance_snapshots.csv.gz"
test -s "${CML_LIVE_ROOT}/account_fill_events.csv.gz"
test -s "${CML_LIVE_ROOT}/exchange_orders.csv.gz"
test -s "${CML_LIVE_ROOT}/live_strategy_signals.csv.gz"

du -sh "$CML_DATA_ROOT" "$CML_LIVE_ROOT"
rg --files "${CML_DATA_ROOT}/parquet" -g '*.parquet' | wc -l
```

原始 Parquet 起点可能早于可用于寻优的起点。脚本会跳过首个不完整 UTC 日，并在报告的 `optimization_window.start` 中写出有效寻优起点；不要因为原始数据从更早时间开始，就手工把 Top10 生效时间提前。

## 6. 在全量数据上运行 A-G 寻优

下面是当前完整的七次寻优。每笔 entry 名义仓位、5x 杠杆、做多、正收益 Top10、限价单 TTL、15 分钟 K 线退出、手续费等固定设置不会随 A-G 改变。

```bash
CML_PARQUET_ROOT="${CML_DATA_ROOT}/parquet"
CML_LIVE_SIGNALS="${CML_LIVE_ROOT}/live_strategy_signals.csv.gz"

run_full_optimization() {
  CML_LABEL="$1"
  shift
  "$CML_PY" scripts/optimize_local_live_constrained.py \
    --input-root "$CML_PARQUET_ROOT" \
    --live-signals "$CML_LIVE_SIGNALS" \
    --output-dir "${CML_DATA_ROOT}/optimization-full-pnl-current-${CML_LABEL}-${CML_PULL_TAG}" \
    --selection-scope full \
    "$@"
}

run_full_optimization A \
  --drawdown-weight 0

run_full_optimization B \
  --drawdown-weight 0 \
  --max-initial-margin-usdt 350

run_full_optimization C \
  --drawdown-weight 0 \
  --max-initial-margin-usdt 350 \
  --fixed-cooldown-buckets 0

run_full_optimization D \
  --drawdown-weight 0 \
  --max-initial-margin-usdt 280

run_full_optimization E \
  --drawdown-weight 0 \
  --max-initial-margin-usdt 280 \
  --fixed-cooldown-buckets 0

run_full_optimization F \
  --drawdown-weight 0.10 \
  --max-initial-margin-usdt 280 \
  --fixed-cooldown-buckets 0

run_full_optimization G \
  --drawdown-weight 0.10 \
  --max-initial-margin-usdt 280
```

每个输出目录应至少包含：

```text
optimization_report.json   # 参数、目标、数据窗口、固定设置和结果
optimization_report.md     # 人类可读摘要
grid_results.csv           # 全部候选参数及其分数
top_candidates.csv         # 排名靠前的候选
best_candidate_events.csv  # 最优组合的事件明细
baseline_events.csv        # 当前实盘基线的事件明细
equity_series.csv          # 绝对模拟 PnL 序列
```

特别检查 `optimization_report.json`：

- `selection_scope` 必须是 `full`；
- `selection_objective` 必须是全量绝对 PnL，或全量 PnL 减回撤惩罚；
- `best_validation.config` 是历史兼容字段，真正的选优范围由 `selection_scope` 决定；
- 有上限的组合要检查 `margin_constraint_feasible=true` 和自然峰值保证金不超过上限。

## 7. 用上一次参数做新增数据前向验证（可选但推荐）

全量重新寻优会把新增数据也用于选参，适合发现新候选，但不适合证明参数具有前瞻性。要验证参数是否能在新增数据上继续工作，应先冻结上一次的 A-G 参数，再把它们回放到本次数据。

现在可以通过 `--source-template` 指向上一次全量结果。`{label}` 会由脚本替换为 `A` 到 `G`：

```bash
# 指向上一次全量寻优的目录；将值改成实际上一次的目录后缀
CML_PREVIOUS_TAG="20260905"
CML_PREVIOUS_ROOT="server_exports/cml-research-data-${CML_PREVIOUS_TAG}"
CML_FROZEN_ROOT="${CML_DATA_ROOT}/frozen-replay-from-${CML_PREVIOUS_TAG}-${CML_PULL_TAG}"

"$CML_PY" scripts/replay_frozen_local_live_strategies.py \
  --input-root "$CML_PARQUET_ROOT" \
  --source-root "$CML_PREVIOUS_ROOT" \
  --source-template "optimization-full-pnl-current-{label}-${CML_PREVIOUS_TAG}" \
  --live-signals "$CML_LIVE_SIGNALS" \
  --output-root "$CML_FROZEN_ROOT"
```

这一步没有重新寻优。它的价值是把旧参数放到新增数据上，作为真正的 forward/holdout 检查。

## 8. 生成实盘与新旧 A-G 对比图

需要同时存在：

- 当前全量寻优结果：`${CML_DATA_ROOT}/optimization-full-pnl-current-A-${CML_PULL_TAG}` … `G`；
- 上一步冻结回放结果：`${CML_FROZEN_ROOT}/replay-A` … `replay-G`；
- 当前实盘对照目录：`${CML_LIVE_ROOT}`。

```bash
CML_COMPARISON_ROOT="${CML_DATA_ROOT}/comparison-full-pnl-current-vs-previous-${CML_PULL_TAG}"

"$CML_PY" scripts/build_seven_vs_seven_visual.py \
  --new-root "$CML_DATA_ROOT" \
  --new-dir-prefix "optimization-full-pnl-current-" \
  --new-dir-suffix "$CML_PULL_TAG" \
  --old-root "$CML_FROZEN_ROOT" \
  --live-dir "$CML_LIVE_ROOT" \
  --output-dir "$CML_COMPARISON_ROOT" \
  --comparison-start "2026-09-04T00:00:00Z"
```

图中应使用：

- 实盘真实账户权益：`account_balance_snapshots.csv.gz`；
- 实盘保证金：由真实 fill/order 记录重建；
- 研究策略：绝对模拟 PnL，从比较起点的实盘权益锚定；
- 新旧参数：同一数据窗口，避免把“换参数”和“换样本期”混在一起。

如果只想保持历史图的可比性，`--comparison-start` 继续使用固定的 `2026-09-04T00:00:00Z`；如果研究另一段时期，必须显式记录新的 UTC 起点，不能让脚本或浏览器自动选择。

## 9. 常见错误检查

### 把验证集寻优误当成全量寻优

症状：结果报告里的参数看起来合理，但用户要求的“全量最优”无法复现。

处理：确认命令中有 `--selection-scope full`，并检查 JSON 中的 `selection_scope`。脚本的 `best_validation` 字段名为历史兼容名称，不代表当前一定按验证集选取。

### 把保证金上限写成运行时拦截

正确含义是：完整回放中自然峰值保证金超过上限的参数组合不参与排名。选出组合后不会再在达到上限时偷偷删除后续信号。

### 起点混用

原始数据起点、Top10 代理有效起点、图表比较起点可能不同。报告中的 `data_start`、`optimization_window.start` 和图表的 `comparison-start` 要分别记录。

### 把研究 PnL 当成真实账户权益

研究曲线使用固定名义仓位和模拟成交；实盘曲线必须来自账户快照。资金费、真实延迟、未成交限价单、已有仓位和交易所风险规则可能造成差异。

### 覆盖旧结果或泄露凭证

每次使用新的 `CML_PULL_TAG`。不要运行 `rm -rf` 清理整个 `server_exports`，不要把 SSH 密码写入本文档、命令脚本或提交记录。

## 10. 当前实现入口

- 全量寻优：[`scripts/optimize_local_live_constrained.py`](../../scripts/optimize_local_live_constrained.py)
- 冻结参数前向回放：[`scripts/replay_frozen_local_live_strategies.py`](../../scripts/replay_frozen_local_live_strategies.py)
- 实盘与新旧 A-G 可视化：[`scripts/build_seven_vs_seven_visual.py`](../../scripts/build_seven_vs_seven_visual.py)
- 服务器研究采集器配置：[`compose.server.yaml`](../../compose.server.yaml)

最后一次已验证的本地样本（仅作参考）是原始数据约从 `2026-09-03T07:19:45Z` 到 `2026-09-05T15:00:00Z`，有效寻优窗口从 `2026-09-04T00:00:00Z` 开始。后续以本次 `optimization_report.json` 实际记录为准。
