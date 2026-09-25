# 存储层重构与数据保留策略：分阶段方案

- **调研时间**：2026-09-16 实机
- **数据库**：单库 `cml`，**2039 MB**
- **现状一句话**：控制面（订单/租约/风控）表很小且健康；**膨胀与写入压力几乎全来自「可归档的观测/审计类表」**，其中 `strategy_runtime_events` 一张表占了全库 40%。

---

## 1. 现状盘点（实测）

### 1.1 体积 Top（public schema）

| 表 | 大小 | 行数 | 分区 | 归档任务 | 角色 |
|---|---:|---:|---|---|---|
| **strategy_runtime_events** | **812 MB** | 97.5 万 | 无 | 有（2 天） | 策略运行事件日志 |
| account_balance_snapshots | 77 MB | 17.1 万 | 无 | 有（2 天） | 资金快照 |
| monitoring_memberships | 70 MB | **101 万** | 无 | 无（靠 FK CASCADE） | 宇宙成员关系 |
| account_position_snapshots | 76 MB | 8.5 万 | 无 | 有（2 天） | 持仓快照 |
| runtime_market_states_15s | ~160 MB 活跃 | 38.3 万 | **6h 分区** | 12h 保留 | 执行时钟 |
| market_data_quality_events | 52 MB | 9.5 万 | 无 | **无** | 行情质量事件 |
| universe_entries | 47 MB | 33.7 万 | 无 | 有（2 天） | 宇宙条目 |
| live_strategy_signals | 38 MB | 0.6 万 | 无 | 有（2 天） | 信号（jsonb 重） |
| execution_account_process_states | 24 MB | 28.7 万 | 无 | **无** | 执行账户状态机 |
| order_intent_candidates | 19 MB | 1.7 万 | 无 | 无 | 意图候选 |
| exchange_orders / fills / events | ~6 MB | 小 | 无 | 部分 | **交易控制面** |
| trading_leases / risk_* / approvals | <1 MB | 小 | 无 | 无 | **交易控制面** |

### 1.2 已经做对的事

1. **`runtime_market_states_15s` 按 6h 分区**，12h 保留，>24h 行数 = 0
2. **`cml-archive-trim` 每日 08:20 跑**：先归档再校验 manifest 再删，7 张表，`--retention-days 2`
3. **归档产物**：`/var/lib/crypto-momentum-lab/table-archive/` 已 345 MB（CSV/JSONL.zst + manifest）
4. **raw 行情** 走独立 parquet/jsonl 卷，7 天保留
5. **控制面表本身很小**，不是问题源

### 1.3 真正的缺口

| # | 问题 | 证据 | 影响 |
|---|---|---|---|
| A | `strategy_runtime_events` **未分区**，靠 DELETE | 日增 **39–47 万行 / ~250MB details**；96% 是 `strategy_output_observed` | 全库 40%；VACUUM/checkpoint 压力；DELETE 残留 |
| B | `monitoring_memberships` **101 万行 / 70MB** | 只靠 `universe_snapshots` FK CASCADE；snapshots 从 7/25 至今 1.1 万条未见裁剪 | 慢膨胀，无独立保留 |
| C | `execution_account_process_states` **28.7 万行**，日增 ~1.2 万 | **不在 archive_and_trim 列表** | 持续涨 |
| D | `market_data_quality_events` **52MB** | 不在归档列表；近 2 日突增（每日 1 万+） | 无保留策略 |
| E | 归档导出 COPY 很重 | `account_balance_snapshots` COPY **150–350 秒** | 早高峰 IO/内存尖峰 |
| F | 控制面轮询极频繁 | `BEGIN/ROLLBACK` **680 万次**；lease/risk/approval 各 **37 万次** | 不是存储问题，是连接/churn 问题 |

### 1.4 增长速率（实测）

```
strategy_runtime_events : ~400k 行/天  ≈ 250–280 MB details/天
runtime_market_states   : ~55 MB / 6h  ≈ 220 MB/天（已有分区+12h 保留，稳态）
universe_entries        : 被 2 天保留压住
monitoring_memberships  : 随 universe_snapshots 累积，无上限
```

**结论**：不重构的话，即使 2 天 DELETE，`strategy_runtime_events` 会永远停在 ~800MB 级，并持续制造死元组与 checkpoint 压力。

---

## 2. 目标架构（分层）

```
┌─────────────────────────────────────────────────────────────────┐
│  L0 交易控制面（PostgreSQL，永不轻易删）                          │
│     exchange_orders / fills / order_events                      │
│     trading_leases / risk_* / live_operator_approvals           │
│     strategy_checkpoints / live_session_transitions             │
│     account_open_orders / exit_episode_reservations             │
└─────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────┐
│  L1 执行热数据（PostgreSQL 分区表，小时级保留）                    │
│     runtime_market_states_15s     [已做 6h 分区，12–24h]        │
│     live_strategy_signals         [按天分区，3–7 天]            │
│     account_*_snapshots           [按天分区，2–7 天]            │
└─────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────┐
│  L2 观测/审计（PostgreSQL 分区表 → DROP，或直接不进 PG）          │
│     strategy_runtime_events       [按天分区，2 天]  ← 最大头    │
│     market_data_quality_events    [按天分区，3 天]              │
│     execution_account_process_states [按天分区或只留最新]        │
│     monitoring_memberships        [随 universe_snapshots 裁剪]  │
└─────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────┐
│  L3 冷归档（文件，不在 PG）                                       │
│     table-archive/*.csv.zst / jsonl.zst   [已有 345MB]          │
│     raw market data parquet/jsonl          [已有，7 天]          │
│     研究用：DuckDB 直接读文件，不连生产库                         │
└─────────────────────────────────────────────────────────────────┘
```

**原则**：
- 控制面表：小、强一致、行级更新 —— 留 PG，不按天删
- 热执行数据：分区 + `DROP PARTITION`，不用 DELETE
- 观测日志：要么分区 DROP，要么根本不进 PG（写文件）
- 研究查询：永远不要打生产 PG

---

## 3. 分阶段实施建议

### Phase 0 — 止血（本周，纯运维，不改代码）

**目标**：把已经明显超标的表压下去，稳住 checkpoint/内存。

| # | 动作 | 预期收益 | 风险 |
|---|---|---|---|
| 0.1 | 确认 `cml-archive-trim` 正常（已在跑，retention=2） | events 表维持 ~800MB 不再涨到 2GB | 无 |
| 0.2 | 手动归档+清理 `monitoring_memberships` 中对应已删 universe_snapshots 的孤儿（或裁剪旧 snapshots） | 释放 ~70MB+ | 低；需确认 CASCADE 路径 |
| 0.3 | 把 `execution_account_process_states`、`market_data_quality_events` **加入 archive_and_trim TABLES** | 阻止无界增长 | 低 |
| 0.4 | 观察 COPY 时长：若 balance 归档仍 >3 分钟，把归档窗改到 **低峰（如 03:20 UTC）** | 降低早高峰 IO 尖峰 | 无 |
| 0.5 | `work_mem` 16MB→4MB（上次已建议，与存储无关但同源） | 削最坏内存路径 | 低 |

**验收**：PG 总大小 ≤ 1.8GB；checkpoint write p95 下降；archive job 无失败。

---

### Phase 1 — 把最大表改成分区（1–2 周，需迁移窗）

**目标**：`strategy_runtime_events` 从「DELETE 大表」变成「DROP PARTITION」。

| # | 动作 | 说明 |
|---|---|---|
| 1.1 | 设计按天分区：`occurred_at` RANGE，预建 7 天，自动 drop >2 天 | 与 states 表 6h 分区同一套模式复用 `runtime_state_partitions.py` |
| 1.2 | 迁移：新建分区表 → 双写或一次性 COPY 进新表 → 切换 → 删旧表 | 维护窗约 10–20 分钟（800MB） |
| 1.3 | 归档改为 **导出分区 → 校验 → DROP**，不再 DELETE | 消灭 events 表死元组 |
| 1.4 | 评估 `strategy_output_observed`（96% 行）是否值得进 PG | 见 Phase 2 |

**验收**：events 表稳态 < 400MB（2 天）；归档 job 从「小时级 COPY+DELETE」变成「秒级 DROP」。

**可选并行**：`live_strategy_signals`、`account_*_snapshots` 同样按天分区（体积中等，收益次之）。

---

### Phase 2 — 削减写入量（2–4 周，改代码）

**目标**：不是「存得更快」，而是「少存」。

| # | 动作 | 理由 |
|---|---|---|
| 2.1 | **`strategy_output_observed` 降采样或改文件** | 94.2 万/97.5 万 = 96% 的行。每 15s×多 symbol 全量写 PG 没必要。改为：仅「有信号/有变化」才写 PG；完整轨迹写本地 JSONL.zst（已有 raw 通道） |
| 2.2 | `market_data_quality_events` 只保留 warning+，info 级进文件 | 近 2 日日增 1 万行，多为噪声 |
| 2.3 | `execution_account_process_states` 只保留每账户最新 N 条 + 异常迁移 | 28.7 万行里绝大多数是例行 state 刷新 |
| 2.4 | `monitoring_memberships` 与 `universe_snapshots` 明确保留策略（如 14 天） | 101 万行成员关系无独立 TTL |
| 2.5 | 控制面轮询频率复核（lease/risk/approval 37 万次调用） | 与存储正交，但同属「写/读风暴」 |

**预期**：PG 稳态 **< 1GB**，checkpoint 压力显著下降。

---

### Phase 3 — 研究路径出库（升配后或并行）

**目标**：研究/回测/前端历史图不再打生产 PG。

| 选项 | 适用 | 动作 |
|---|---|---|
| **A. DuckDB + 已有 archive** | 现在就能做 | 研究脚本改读 `table-archive/*.zst` 与 raw parquet |
| **B. 导出 Parquet 日更** | 中期 | archive 任务顺带产出 Parquet（比 CSV 更适合分析） |
| **C. ClickHouse / QuestDB** | 升到 ≥8G 内存后 | 热历史（7–30 天）进 CH，前端 dashboard 查 CH |

**现在不要上 CH**：3.6G 机器加 CH 会和实盘抢内存，得不偿失。

---

### Phase 4 — 结构性（资金/规模到阈值后）

| # | 动作 |
|---|---|
| 4.1 | PG 与策略进程分机，或至少 paper/collector 迁出 |
| 4.2 | 控制面主从（或至少 WAL 归档 + 只读副本给 dashboard） |
| 4.3 | 对象存储（S3/MinIO）承接 table-archive 与 raw，本机只留 7–14 天 |

---

## 4. 表级保留策略建议（目标态）

| 表 | 保留 | 清理方式 | 归档 |
|---|---|---|---|
| exchange_orders / fills / order_events | **永久或 ≥1 年** | 不自动删 | 定期冷备 |
| trading_leases / risk_halts / approvals | **永久**（审计） | 不自动删 | 冷备 |
| strategy_checkpoints | 保留当前 session + N | 覆盖写 | 否 |
| runtime_market_states_15s | **12–24h**（现状 12h） | DROP 分区 | 可选 parquet |
| live_strategy_signals | **3–7 天** | DROP 分区 | 已有 jsonl.zst |
| account_balance/position_snapshots | **2–7 天**（现状 2） | DROP 分区 | 已有 csv.zst |
| **strategy_runtime_events** | **2 天** | **DROP 分区** | jsonl.zst |
| market_data_quality_events | **3 天** | DROP 分区 | 可选 |
| execution_account_process_states | **3 天** 或只留最新 | DROP / bounded delete | 可选 |
| universe_entries | **2–7 天** | DROP 分区 | 已有 |
| universe_snapshots + memberships | **14 天** | DELETE parent + CASCADE | 可选 |
| order_intent_candidates | 7 天 | bounded delete | 否 |
| paper_* | 随 paper 会话 | 会话结束归档 | 已有 |

---

## 5. 查询模式侧的额外发现（非存储但相关）

`pg_stat_statements` 显示：

| 模式 | 调用量 | 含义 |
|---|---:|---|
| BEGIN/ROLLBACK | **680 万** | 事务 churn 极高，多来自空轮询 |
| trading_leases / risk_halts / approvals / session_transitions | 各 **37 万** | 控制面被高频轮询 |
| exchange_orders by client_order_id | **38 万** | 热点查询，应有索引（已有） |
| COPY account_balance_snapshots | **150–350 秒** | 归档导出过重 |

**建议**：Phase 0/1 之外，单独立项看「控制面轮询能否合并/拉长间隔」——这能同时降 CPU、连接数和内存，和分区改造互补。

---

## 6. 建议执行顺序（汇总）

```
本周     P0.2–0.4  补齐无保留表 + 归档窗避峰
下周     P1        strategy_runtime_events 按天分区迁移
两周内   P2.1      砍掉/降采样 strategy_output_observed（最大杠杆）
并行     P3.A      研究改读 DuckDB + archive，禁打生产 PG
升配后   P3.C / P4 CH 或分机
```

**最大杠杆只有一个**：`strategy_output_observed` 占了 events 表 96% 的写入。  
不处理它，分区只能把「膨胀」变成「每天 DROP 800MB」；处理了它，PG 可以回到「纯控制面」的体量。

---

## 7. 证据索引

- 表体积/行数：`\dt+`、`count(*)`（2026-09-16 12:30 CST）
- 分区：`pg_inherits`、`pg_get_expr(relpartbound)`，6h 粒度
- 归档：`cml-archive-trim.timer` 每日 08:20，`--retention-days 2`，输出 345MB
- 增长：`strategy_runtime_events` 按日 39–47 万行；`strategy_output_observed` 94.2/97.5 万
- 查询：`pg_stat_statements` top by calls / total time / rows
- 代码：`deploy/ops/archive_and_trim.py` TABLES 列表；`operational_retention.py`；`runtime_state_partitions.py`
