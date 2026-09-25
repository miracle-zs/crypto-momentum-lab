# 四维度系统体检：存储与状态 / 通信与传输 / 计算与并发 / 可观测与弹性

日期：2026-09-24。基线：本地 HEAD = 生产镜像 = `86a89a911f3ae9b3385d1d7deced1c7b8beb261e`。
范围：`src/crypto_momentum_lab` 关键热路径 + 服务器 `43.167.191.253` 运行态只读采样（容器、PostgreSQL、日志、主机资源）。
方法：静态代码阅读、`pg_stat_statements` / `pg_stat_bgwriter` / checkpoint 日志、docker logs、主机负载。未改生产配置，未下单，未做故障注入。

**总判断：系统能跑，核心不变量（订单幂等、fencing、分池、fail-closed 保留期、影子账本）已经立住。但仍有一批明确的性能瓶颈、架构债和在线正确性风险。当前最急的不是再拆模块，而是磁盘/checkpoint、主机超卖，以及账本对账缺口这条正确性线。**

优先级约定：P0 = 正在拖垮或可能直接伤交易正确性；P1 = 高概率在下一轮故障/扩容时爆；P2 = 债务，按计划还。

---

## 0. 运行态快照（采样时刻）

| 项 | 实测 |
| --- | --- |
| 主机 | 2 vCPU，3.6 GiB RAM，swap 1.9G 已用 237M，load 2.7–3.7 |
| 容器 | 4 live-strategy + 4 execution-account + market-data + research-collector + dashboard + postgres，全部 healthy |
| market-data | CPU ~19%，RSS ~385M / 上限 640M，net ~2.7GB in，`event_loop_lag` 尖峰 50–140 ms |
| postgres | 1.75 GB 库，51 连接，block IO 120G read / 101G write |
| checkpoint | **write=251–437 s**（约 2.5–4.4k buffers ≈ 20–34 MB），sync 正常 |
| 健康接口 | `{"app_status":"UP","database_status":"UP"}` |
| 在线异常样例 | HUSDT `reconciliation_gap=1501` 后主路径回退 legacy；HOMEUSDT `unmanaged_live_positions`；VELVETUSDT `pending_live_positions` 超时 |

---

## 1. 存储与状态（Storage & State）

### P0-1 PostgreSQL checkpoint 写盘极慢

**证据**（生产日志，连续多次）：

```text
checkpoint complete: wrote 3049 buffers (9.3%); write=306.003 s, sync=0.019 s
checkpoint complete: wrote 4363 buffers (13.3%); write=437.123 s, sync=0.027 s
checkpoint complete: wrote 2512 buffers (7.7%);  write=251.743 s, sync=0.022 s
```

`pg_stat_bgwriter.checkpoint_write_time` 累计约 **102 小时**。写 20–34 MB 花 4–7 分钟，说明云盘吞吐/延迟或 IO 争用已到病态；sync 正常说明不是 fsync 语义问题，是 **脏页刷盘带宽** 问题。

**影响**：checkpoint 期间与 market-data / 账户快照写入争抢磁盘，推高决策到提交延迟；`checkpoint_timeout=900s` 下 checkpoint 几乎“刚写完又开始”，形成持续 IO 压力。

**建议**：
1. 先度量：`iostat -x 1`、云盘限速指标、`checkpoint_completion_target` 实际值。
2. 减少脏页产生：合并高频小批量 `strategy_runtime_events` 写入；账户快照按现有 sparsify 再压一档；临时导出（见 P1-3）移出高峰。
3. 若确认是云盘限速，优先升盘/独立数据盘，而不是继续调大 `work_mem`（当前 16MB，在 2 核机上已经偏激进）。
4. 把 `checkpoint write duration > 30s` 做成告警，而不是只留日志。

### P1-1 账本事实覆盖仍是时间窗启发式

`postgres_runtime.py:1380-1394` 在没有显式 `since` 时用 `min(order.created_at) - 24h`，订单时间缺失则回退 30 天。长持仓、停机跨窗、窗口外手动成交会被截掉，只能回退 legacy 重建。

**建议**：把 `durable fill cursor` / episode coverage 做成账本输入的硬边界；窗口查询只允许作为“加速缓存”，不允许决定“事实是否存在”。

### P1-2 过期对账结果连同 fills 一起丢弃

`execution_account/sync.py:822-826`：`snapshot.config.observed_at < self._latest_observation_at` 时直接 `return`。快照新旧判定是合理的，但 **同一结果里的 fills 可能是首次出现的事实**，被一起吞掉。

**建议**：拆成两条准入——快照可因过期忽略；fills 按 `trade_id` 幂等接纳，永不因“快照旧”丢弃。

### P1-3 全表 COPY 导出压垮观测面

`pg_stat_statements` 头部：

```text
2 calls | mean 1857323 ms | COPY (SELECT * FROM account_config_snapshots ORDER BY observed_at) TO STDOUT
1 call  | mean  353964 ms | COPY ... account_balance_snapshots ...
```

单次全表导出 3–6 分钟，产生 69MB 临时文件，直接和在线写路径抢盘。

**建议**：导出改分页/按日分区、加 `WHERE observed_at`、或落异步只读副本；禁止在交易时段对热表做无界 `COPY`。

### P2 存储侧其余

| 项 | 现状 | 建议 |
| --- | --- | --- |
| `universe_entries` 350MB / 1.27M 行 | 最大表，保留策略不清晰 | 明确保留天数 + 按日分区或归档 |
| `DEFAULT_LIVE_CASH_FLOW_ADJUSTMENTS`（`operator_dashboard/queries.py:148`） | 硬编码 primary / 200 入金 | 迁入可审计 cash_flow 表 |
| 服务器镜像 13GB + build cache 7GB | 59G 盘已用 47% | 发布后清理旧 tag，限制保留 N 个 |
| 保留期消费者水位 | account 路径已 fail-closed；market-data 路径仍偏 cutoff | 统一 `RetentionPlan` 真正传入 consumer requirements |

**做得对的（不要回退）**：用途隔离连接池、账户快照 sparsify、归档 journal + 单 writer、保留期异常 fail-closed、runtime/event 分区表。

---

## 2. 通信与传输（Transport）

### P1-1 Account Event Hub 可用性计时仍有边角

`execution_account/hub.py:791` 把 `unavailable_since` 初始化为进程/迭代起点；只有“非 full-snapshot 恢复握手成功”或“成功 materialize 事件”才清空。

剩余问题：首连或 `require_full_snapshot=True` 的会话，在收到第一笔事件前若网络抖动，会把 **从进程启动起的墙钟** 算进 unavailable budget，直接抛 `account-event hub unavailable beyond timeout`——即使连接其实刚建立不久。

同类结构仍存在于 `market_data/quote_hub.py`、`execution_account/risk_control_hub.py`（历史审计已点名，未见统一收敛）。

**建议**：抽一个共享的 `StreamAvailabilityClock`——只有“成功消费”清零；连接成功但尚未消费时，budget 从**本次故障开始**计，不从历史起点累计。

### P1-2 在线对账缺口：HUSDT gap=1501

生产日志（primary，UTC 15:33）：

```text
position_ledger_shadow_comparison category=reconciliation_gap_detected
  gap=1501 ... ledger_total=1552 position_amt=3053 unallocated_quantity=0
position_ledger_primary_fallback ... symbol=HUSDT
```

随后同标的又出现 `exact_match / primary_active`（数量 3053），说明缺口被“对齐”过，但 **差额 1501 的来源没有被事实解释**。按模块不变量：差额必须可解释，禁止靠裁剪/回退抹平。

**建议**：把 `reconciliation_gap != 0` 升为结构化事故（原因码 + 可查询证据），在 gap 清零前禁止该 symbol 新开仓；补“外部成交 / 晚到 fill / 快照截断”三类根因判定。

### P2 传输侧其余

| 项 | 现状 | 建议 |
| --- | --- | --- |
| bookTicker 合并丢弃 | 0.4s 阈值下 `simulated_close_drop_count≈9500` | 对 quote 可接受，但要在决策侧声明“决策可见版本”可能缺尾部 quote |
| 多账户扇出 | 4 个 live-strategy 各自连 market-state / quote hub | 可接受；若 CPU 再紧张，考虑单订阅多路复用 |
| REST pacer | 读/命令分锁，0.5s 间隔 | 合理保留；多账户扩容前先核预算 |
| 连接池 `max_queue=16` | 账户事件客户端接收队列偏小，溢出触发 full snapshot recovery | 评估提高或改为按序号缺口局部补采 |

---

## 3. 计算与并发（Compute & Concurrency）

### P0-1 主机超卖：2 核跑 12 个 Python 进程 + Postgres

load 2.7–3.7（2 核），swap 已用 237M，`vmstat` 持续有块 IO。market-data 一家就吃掉约 1/5 CPU，4 个 live-rollout 各约 1.4% 但各有独立重建/检查点任务。

**影响**：`event_loop_lag` 尖峰 50–140 ms（告警线 50 ms）、聚合 `processing_max_ms=2499`（极可能是历史峰值但一直未复位）、checkpoint 与查询互相拖慢。

**建议**：
1. 短期：给 market-data / postgres 提高 CPU 权重；限制 dashboard 全表导出；关闭非必要 sidecar。
2. 中期：升到 4 vCPU / 8GB，或把 research-collector + dashboard 拆到另一台。
3. 把 `event_loop_lag` 与 checkpoint write duration 做成看板主指标。

### P1-1 交易热路径反复全量投影账本

`postgres_runtime.py` 在 context reload 时：拉订单窗口 + 全量（有界）fills + 事件 → 旧重建 + PositionLedger 投影 + 影子比较。每次持仓上下文刷新都做一遍 O(事实量) 重放，而不是增量。

**建议**：引入 `ledger_watermark`；仅当新事实跨越水位才增量重放；全量投影只在恢复/对账时跑。

### P1-2 执行路径双算（legacy quantize + shadow TradeCommand）

`submission.py:351-375` 先 `quantize_order_plan`，再 `_shadow_evaluate_trade_command`，一致才替换。这是正确的渐进切换，但：

- 计算量 ×2；
- 不一致时静默回 legacy，主路径行为依赖“审计是否 concordant”，可解释性差。

**建议**：给 shadow 分歧打点计数并暴露到 dashboard；连续 N 次一致后切单一执行器，删除双算。

### P2 并发侧其余

| 项 | 现状 | 建议 |
| --- | --- | --- |
| checkpoint phase 错峰 | 0/15/30/45s | 保留，有效 |
| `max_parallel_workers_per_gather=1` | 正确（2 核） | 保留 |
| `work_mem=16MB` | 在慢盘 + 复杂排序下可能再次临时文件 | 与导出治理一起评估，勿单独调大 |
| 聚合路径同步锁 | `BoundedEnvelopeQueue` 单锁 | 当前吞吐够用；瓶颈更可能在宿主 CPU |
| `research/datasets.py` 全量 tuple 化 | 大区间研究会爆内存 | 流式化（P3，非在线路径） |

---

## 4. 可观测与弹性（Observability & Resilience）

### P1-1 健康语义过粗，掩盖进度债务

`/api/health` 只有 `app_status/database_status=UP`。同一时刻线上同时存在：

- `reconciliation_gap_detected`（账本事实不完整）
- `unmanaged_live_positions:HOMEUSDT` → `live_account_event_exit_degraded`
- `pending_live_positions:VELVETUSDT` → grace timeout

这些都比 “UP” 更接近真实可交易性。

**建议**：健康契约改为三层（与 lifecycle 契约对齐）：
`INDEPENDENT_EXECUTABLE / PROGRESS_LAGGING / STALLED`，并附 `reasons[]`（gap、unmanaged、pending、stale market、checkpoint lag）。

### P1-2 退出降级可观测，但根因闭环不足

`unmanaged_live_positions` / `pending_live_positions` 触发退出降级是正确保护，但：

- 同一原因会重复打 3 条 error 日志（HOMEUSDT 三次 `live_account_event_exit_degraded`）；
- 没有自动恢复条件与告警升级；
- 人工需翻日志才知道“现在能不能开仓”。

**建议**：聚合为单一状态机事件（enter/leave degraded），带 `since/reason/symbol_set`，推到 dashboard 与告警。

### P2 可观测侧其余

| 项 | 现状 | 建议 |
| --- | --- | --- |
| checkpoint 时长 | 只在 PG 日志 | 抽成指标 + 告警（>30s warn，>120s critical） |
| market_data_health_snapshot | 单行巨型 JSON | 拆 metric + 采样落库，日志只留摘要 |
| 影子比较结果 | 日志 info | 落表，支持“近 24h 分歧分类”查询 |
| 本地 heartbeat 文件 | 正确保留 | 文档继续强调“进程活着 ≠ 事实完整” |
| 配置/发布指纹 | 有 hash | 补 `ReleasePlan` 矩阵（各容器 commit 对齐可见） |

**做得对的**：checkpoint commit/durable token 区分、ShutdownResult 语义映射、容量注入可测、tracemalloc 可选、json-file 日志轮转、影子账本 fail-closed 切换。

---

## 5. 按优先级的行动清单

### 立刻（P0）

1. **磁盘/Checkpoint**：度量云盘 → 治理全表 COPY 与高频小写 → 告警 checkpoint write duration。
2. **主机资源**：至少升配到 4C/8G，或把 research-collector/dashboard 迁走；在此之前限制导出与非关键任务。
3. **HUSDT 对账缺口**：查清 gap=1501 来源（外部成交？晚到 fill？窗口截断？），清零前冻结该 symbol 开仓。

### 近期（P1）

4. 修 Account/Quote/Risk Hub 统一 `StreamAvailabilityClock`。
5. fills 与 snapshot 准入拆分（`persist_reconciliation_result`）。
6. 账本增量投影 + durable fill cursor，去掉 24h 启发式作为事实边界。
7. 健康契约改为三级 + reasons，退出降级事件聚合。
8. shadow 分歧可观测化，准备收敛双算执行路径。

### 计划内（P2）

9. 现金流记录替代硬编码调整；`MetricResult` 口径元数据。
10. `universe_entries` 保留/分区；镜像/build cache 清理策略。
11. market-data 保留期真正消费 `RetentionConsumerRequirement`。
12. 研究路径流式读取。

---

## 6. 边界说明

- 本次是只读体检：没有改代码/配置，没有下单，没有压测。
- checkpoint 慢的根因（云盘限速 vs 应用写放大 vs 两者叠加）**尚未用 iostat/云监控闭环**，P0-1 的“先度量再动刀”不能跳过。
- `processing_max_ms=2499` 为进程生命周期内峰值，不等于当前每秒都在卡 2.5s。
- 此前多轮重构 review（`system-refactor-review-20260920.md` 等）中的 R1–R5 架构闭环结论仍然有效；本文在其之上补的是**当前生产运行态**的性能与弹性证据。
