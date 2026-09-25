# 代码审查报告：Bug 与冗余设计

- 审查日期：2026-03-19
- 审查范围：`src/crypto_momentum_lab/**`、`apps/**`、`configs/`、`compose*.yaml`、`deploy/`、`Dockerfile`、`docker/`
- 审查方式：并行模块审查（执行/订单、策略/回测、行情/持久化、架构冗余、部署脚本）+ 人工交叉核验

整体架构分层清晰、领域校验偏严，但实盘路径上存在会直接导致**交易所与本地状态分叉**的真 bug，以及一批会随账户规模放大的结构债。

---

## P0 — 会直接造成实盘事故

### 1. 下单网络错误未包装，订单永久卡在 `SUBMITTING`

**位置**：`execution_account/binance/client.py:770-789`  
**对照**：`orders/state_machine.py:284-339`

`submit_order` 只处理了 `httpx.TimeoutException` 和 `HTTPStatusError`。`ConnectError` / `ReadError` 等 `RequestError` 子类会原样抛出。

对比同文件的 `query_order_by_client_id` 和 `cancel_order_by_client_id`，它们都正确映射到了 unknown 异常。

**后果**：
1. 写前日志已落库 `SUBMITTING`
2. HTTP 请求可能已到达 Binance（TCP 已建立但响应丢失）
3. 本地既不查单也不进入 `UNKNOWN_PENDING_RECONCILIATION`
4. daemon 的 `_recover_pending_exit_orders` 只扫 `UNKNOWN`，**不会**恢复 `SUBMITTING`
5. 交易所侧可能有活动订单/仓位，本地永不处理

**建议**：在 `submit_order` 中增加 `except httpx.RequestError` 并映射为 unknown 语义；状态机对 submit 阶段未识别异常统一落 `UNKNOWN_PENDING_RECONCILIATION`。

---

### 2. 撤单被拒被记为终态 `REJECTED`，挂单可能还在交易所

**位置**：`orders/state_machine.py:531-542`  
**对照**：`binance/client.py:1008-1074`

撤单请求若因 `-1021`（时间戳）、权限、参数校验失败被拒，直接落 `REJECTED`（terminal）。这只说明「撤单请求失败」，**不说明原订单不存在**。

`ExchangeOrderState.terminal` 包含 `REJECTED`，之后：
- `load_unresolved_orders` 不再返回它
- exit recovery 只扫 `UNKNOWN`，不扫它
- 真实挂单留在交易所，本地认为已关闭

**建议**：撤单失败且未证明订单消失时，应落 `UNKNOWN_PENDING_RECONCILIATION`（或保持 `CANCELING`）。`REJECTED` 只应表示「下单被拒」。

---

### 3. GTD 预检抛裸 `ValueError`，同样卡在 `SUBMITTING`

**位置**：`binance/client.py:760-766`  
**调用顺序**：`prepare_submission`（事务已提交 `SUBMITTING`）→ `execute_approved_intent` → `submit_order`

GTD 订单若 `expires_at <= now+600s` 抛 `ValueError`。状态机只处理三种异常，`ValueError` 直接穿透。

`entry_limit_ttl_seconds` 默认 900、最小校验 601，但 pacer 排队 + leverage/margin 预热 + coordinator 队列延迟超过 300 秒就很容易触发。

**建议**：该检查应抛 `OrderPreSubmissionError`（状态机已有分支，会落 REJECTED）。

**三者共同点**：写前日志已提交但交换结果未知时 fail-open。

---

## P1 — 高风险正确性问题

### 执行层

#### 4. Coordinator 按 key 创建调度器存在竞态

**位置**：`orders/coordinator.py:264-280`

`get`/创建/写回之间无锁。两个协程对同一 `(account, symbol, position_side)` 首次并发调度时：
1. 双方都读到 `None`
2. 各自创建 `_KeyCommandScheduler`
3. 后写者覆盖 dict，前一个 scheduler 及其 worker task 泄漏
4. 后续命令可能落到不同 worker 上并发执行

**直接违反**类注释中的接口不变量：「commands for the same account/symbol/position side execute one at a time」。入口/平仓并发时可能重复下单或并发撤/下。

**建议**：用 `asyncio.Lock` 保护 get-or-create。

---

#### 5. Scheduler close 与 submit 竞态：调用方 future 永久挂起

**位置**：`coordinator.py:88-125`

交错序列：
1. `submit` 通过 `_closed` 检查
2. `close` 置 `_closed=True`，放入哨兵
3. worker 处理哨兵后 `return`
4. `submit` 再 `put` 操作项，`await future` 永远不会完成

进程关闭路径上会卡死平仓/撤单命令。`close` 也不 drain 队列中哨兵之后的 future。

**建议**：`close` 时：置 closed → 拒绝新 submit → drain 剩余 item 并对每个未完成 future `set_exception` → 再放哨兵。

---

#### 6. `append_order_event` 只保护 `FILLED`，其它终态可被回退

**位置**：`persistence/postgres/order_repository.py:274-280`

`CANCELED` / `ABSENT_RECONCILED` / `EXPIRED` / `REJECTED` / `SUPPRESSED` 都不在此保护内。

场景：订单已 `CANCELED`（`updated_at=T2`），重连后旧 REST 快照带 `PARTIALLY_FILLED`、`occurred_at=T3>T2` 到达，`advance_state` 为真，订单行从终态回退为未终态。恢复扫描再次把它当活单，可能重复撤单/重复平仓。

**建议**：禁止从任何 terminal 状态推进到非 terminal 状态（除非显式允许的 `UNKNOWN → observed` 解析路径）。

---

#### 7. 用户数据流 `_seen_trade_ids` 无界增长

**位置**：`execution_account/user_data_sync.py:70`

与之对比，`_seen_event_ids` 有 `deque(maxlen=4096)`，Hub 的 `_seen_fill_keys` 有 `_FILL_KEY_CACHE_SIZE=8192`。`replace_snapshot` 也不清理该 set。

实盘进程设计为长驻，每个成交 `(symbol, trade_id)` 永久驻留。高频账户数月后会占用可观内存。

**建议**：改成与 event id 相同的 `deque + set` 双结构，或固定容量 LRU。

---

### 策略 / 纸交易

#### 8. 官方 15m K 线源异常未捕获，可直接打崩 paper-live daemon

**位置**：`strategy_runner/daemon.py:1025-1055, 1215-1221`  
**对照**：`candle_source.py:278-287, 325-347`

`_load_latest_closed_candle_for_positions` 直接调用 `source.load_closed_candles`。Binance 刚收盘尚未发布该 15m K 线、分页不足、缺 candle 时会抛 `ClosedCandleSourceError`。

daemon 主循环与 CLI **都没有 try/except**，异常会终止整个 paper 进程。EMA 路径反而 `except Exception: return None`（fail-closed），平仓路径没有对等保护。

**建议**：平仓 K 线加载与 EMA 一样 fail-soft：本轮返回 `None`，下个 state 重试；对 `last_candle_end` 做退避。

---

#### 9. 成对账户 EMA 入场过滤在无 context 时静默拒绝全部信号

**位置**：`strategy_runner/daemon.py:690-734, 766-802`

`run_paired_paper_live_daemon` 用 `_decision_for_account` → `_signal_passes_entry_filter(signal, entry_filter)`，**从不传 `context`**。而单账户 daemon 走 `_filter_decision(..., entry_filter_context=...)`。

当 `require_price_above_ema5/10=True` 且 `context is None` 时，函数直接 `return False`，成对模式下**所有入场被静默丢弃**，且无日志。

**建议**：成对路径接入与单账户相同的 EMA context loader；或在配置校验时禁止启用 EMA 过滤并 fail-fast。

---

#### 10. 成对账户共享策略冷却，被过滤信号仍消耗全员 cooldown

**位置**：`strategy_runner/daemon.py:478-492`  
**策略侧**：`order_flow_impulse/runtime.py:260-262`、`liquidation_cascade/runtime.py:192-194`

`strategy.on_market_state` 在产出信号时**立即**写入共享 `_cooldown_remaining`，随后各账户才按自己的 `entry_filter` 丢弃信号。

若账户 A 因 long_only / imbalance 过滤掉 SHORT 信号，冷却仍已设置，会挡住账户 A 后续本可成交的 LONG 信号。

**建议**：冷却只在「至少有一个账户接受该信号」时提交；或按账户维护 cooldown。

---

#### 11. 纸交易批处理不做缺口重置，与 replay/daemon 语义不一致

**位置**：`strategy_runner/paper.py:156-175`  
**对照**：`replay.py:238-246`、`daemon.py:398-405, 1551-1562`

`run_paper_trading` 对每个 state 只做时区/单调性校验，从不按 `max_gap_seconds` 调用 `strategy.reset_symbol`。数据有缺口时，warmup 缓冲里会混入不连续行情，可能产生 replay/daemon 不会产生的假信号。

**建议**：paper 路径复用同一 gap 重置逻辑。

---

#### 12. paper-live 强制 `latency_buckets=0`，与 replay 默认 1 系统性不一致

**位置**：`apps/strategy_runner/main.py:1065-1068, 1574-1577`  
**对照**：`fills.py:21`（默认 latency=1）

daemon/pair CLI 把成交延迟写死为 0，即在**触发信号的同一根已收盘 bar 的 bucket_end** 成交。replay 默认下一根 bar 成交。

结果是 paper 相对 replay/实盘系统性偏乐观（更早、更优价格），且 CLI 无法配置 fee/slippage/latency。

**建议**：paper-live 默认至少 latency=1，或 CLI 暴露 latency/fee/slippage。

---

#### 13. 批处理 paper 只产出 fills，完全不做持仓/平仓边界

**位置**：`strategy_runner/paper.py:148-211`

`PaperTradingRunReport` 只有 `paper_fills`，没有 open/closed position、TP/SL、15m candle exit、grace。`paper` / `paper-live-source` 命令无法验证 CONTEXT.md 定义的「持仓批次 / 平仓边界」，与 daemon 结果不可比。

**建议**：批处理 paper 复用 `mark_positions` + `Candle15mAggregator`，或明确文档降级为 "fills-only dry run"。

---

### 行情 / 持久化

#### 14. WebSocket reader 同步 await 实时 sink，ingress 溢出直接拆连接并静默丢消息

**位置**：`market_data/binance/websocket.py:545-553`

reader 设计上声称与下游解耦，但实际在读循环里同步 `await self._on_realtime_envelope(envelope)`。该 sink 最终调用 `quote_hub.publish`（加锁 + JSON 编码）。

ingress 队列满时不是丢弃单条，而是 `raise CaptureQueueFull`，导致整个 reader 任务死亡 → 连接重建 → `data_queue` 中未派发的 envelope 全部丢失，且不产生 quality event。

**建议**：realtime sink 改为 fire-and-forget；ingress 满时按流分级丢弃，而不是 tear down 连接；至少应对丢失区间发 quality event。

---

#### 15. `DiskSpaceGuard` 完全未接线，磁盘写满无保护

**位置**：`market_data/capture/service.py:24-48`；`apps/market_data/main.py:853-857`

`DiskSpaceGuard` 被构造并注入 `MarketDataCaptureService`，但全仓库无任何 `disk_guard.evaluate(...)` 调用。`CaptureMetricsSnapshot.disk_free_bytes` 永远是初始值 0。磁盘写满时 archive 持续失败，不会 HALT。

**建议**：在 capture run loop 或 archive append 前周期性调用 `evaluate(free_bytes)`，HALT 时停止写入并写 `save_process_state`。

---

#### 16. `PendingManifestJournal` 只写不回放，DB 故障期间的 manifest 永久丢失

**位置**：`apps/market_data/main.py:729-733`；`persistence/raw_files/journal.py:38-51`

manifest 写库失败会落到 journal 文件，但生产代码从不调用 `manifest_journal.replay()`（仅测试调用）。重启后这些 manifest 不会补写 DB，归档文件变成孤儿，retention 也扫不到。

**建议**：启动时（`recover_archive_root` 之后）执行 `await manifest_journal.replay(save_manifest)`。

---

#### 17. AggTrade 恢复：先推进 `last_seen`，1 秒超时内失败则永久丢缺口

**位置**：`market_data/agg_trade_recovery.py:84, 163-178, 235-236`

1. `recovery_timeout_seconds` 默认 1.0s，却要按 1000/页拉最多 10000 笔
2. 超时范围包含 semaphore 等待（8 并发排队也吃同一秒）
3. 检测到 gap 后无条件推进 `last_seen`，恢复失败后该 gap 不会再被检测

**建议**：恢复成功后再更新 `last_seen`；超时至少覆盖 `pages * rtt`；semaphore 等待移出 timeout。

---

#### 18. `BoundedEnvelopeQueue.put` 背压无超时，下游卡死会级联拖垮整条链路

**位置**：`market_data/capture/queue.py:114-135`；`runtime_states.py:692`；`coordinator.py:136`

durable 路径 `put()` 在容量不足时无限 `await self._capacity_available.wait()`。

链条：DB 挂 → `_persist_loop` 无限重试 → durable queue 满 → `observe` 阻塞 → coordinator 阻塞 → websocket `data_queue` 满 → `CaptureQueueFull` 拆连接。全程无背压超时、无 HALT。

**建议**：背压等待加上限，超时后 HALT 或对非关键流降级。

---

#### 19. Archive group-commit flush 失败不关闭 writer，后续写入可能落在半损坏文件上

**位置**：`persistence/raw_files/archive.py:362-375, 265-282`

`_commit_locked` 中 `_flush_block`（fsync）失败只给 pending futures set_exception 并 re-raise，writer 仍 `_closed=False`。下一次 `append` 继续写同一个 zstd 流。已 fsync 失败的块与后续块混在同一文件，manifest sha256 与内容一致性无保证。

**建议**：flush 失败后将 writer 置为 broken 并从 `_writers` 摘除，强制 `abort()` 后由新 writer 接手。

---

#### 20. 质量/恢复/实时三条路径共享同一 market DB pool(2)，互相饿死

**位置**：`persistence/postgres/session.py:29-32`；`apps/market_data/main.py:634-641`

`create_market_database_engine` 只有 2 连接，却同时服务：
- universe repository（refresh 全量 metadata/opens/snapshot）
- capture repository（quality events + manifests + process state）
- runtime state repository（15s 批量 INSERT + pg_notify）
- paper/account protected-symbol 查询
- operational retention 循环

retention 用 `command_timeout=5s` 的同一 engine，一次慢查询即可占满连接，导致 runtime state 持久化失败并触发背压级联。

**建议**：retention/paper 查询迁到 maintenance pool；quality 写入用 observability pool。

---

#### 21. `save_portfolio`：N+1 SELECT，且内存缓存在事务 commit 前写入

**位置**：`persistence/postgres/paper_daemon_repository.py:499-534`

1. 对每个 position 单独 `session.scalar(select(...))`
2. `self._portfolio_stats[run_id] = next_stats` 写在 `async with session.begin()` **内部**；若 commit 失败，缓存已污染，后续 equity 计算基于未落库的 stats

**建议**：一次 `WHERE position_id IN (...)`；缓存赋值移到 commit 之后。

---

#### 22. `BinanceConnectionPool` 无锁，`apply_symbols` 与 `stop` 竞态

**位置**：`market_data/binance/connection_pool.py:79-162`

`apply_symbols` 迭代/修改 `_connections`、`_active_subscriptions`，`stop` 并发 `clear()`。market-data 关停时 scheduler 被 cancel 但可能正处在 `refresh → observer → apply_symbols` 中途。

**建议**：加 pool 级锁，或 `stop` 后置 `_stopped` 标志让 `apply_symbols` 快速失败。

---

#### 23. Coordinator 对每个 envelope 建 task + 每条 quality event 单独写库（N+1）

**位置**：`market_data/capture/coordinator.py:171-208`；`persistence/postgres/capture_repository.py:77-86`

batch 最大 1000 条时创建 1000 个 `asyncio.Task`，每个 task 内部逐条 `save_quality_event`（每条一次 session + INSERT + commit）。market pool 仅 2，quality 写入会挤占行情持久化连接。

**建议**：quality events 收集后批量 INSERT；archive 串行或用固定 worker 数。

---

## P2 — 值得修但不紧急

### 逻辑边角

| # | 位置 | 问题 |
|---|------|------|
| 24 | `portfolio.py:107-130` | `Candle15mAggregator` 缺任一 1m 分钟 → 整根 15m 静默丢弃，candle/grace 平仓边界丢失 |
| 25 | `daemon.py:1025-1055` | 宕机恢复只回放最新一根 15m，`confirmation_count>1` 和 grace 历史会漂移 |
| 26 | `portfolio.py:356-451` | grace 路径忽略 `candle_minimum_holding_seconds` |
| 27 | `apps/strategy_runner/main.py:1026-1031` | EMA 入场 context 对 SHORT 也用 ask 价，空头过滤偏松 |
| 28 | `daemon.py:1172-1179` | stale state 跳过时不 mark 持仓，数据中断会冻结平仓 |
| 29 | `orders/quantization.py:146` | LIMIT 卖单 `_round_down` 使卖价更激进（应 round up 或按 side 分支） |
| 30 | `binance/client.py:1077-1110` | 紧急撤单路径复制了不完整的错误处理，缺少 absent 证明和限流 unknown 语义 |
| 31 | `hub.py:257-260` | `_latest_bucket_start` 被覆盖而非取 max，监控指标失真 |
| 32 | `daemon.py:1006-1054` | 成功重连后 `still_missing` fill 跟踪丢失 |
| 33 | `execution_account/main.py:122-127` | 数据库 URL 校验死代码（`_execution_database_url` 已 raise） |
| 34 | `order_repository.py:155-185` | `prepare_submission` 在 order 冲突时仍 commit intent 插入 |

### 冗余实现

| # | 位置 | 问题 |
|---|------|------|
| 35 | `hub.py:1008-1128` vs `quote_hub.py:509-552` | 协议编解码大段拷贝且已开始漂移（`_decode_object` 行为不一致） |
| 36 | `order_repository.py:495-512` vs `account_repository.py:446-463` | `_jsonable` 逐字重复 |
| 37 | `paper.py:225-257` vs `daemon.py:1395-1427` | `_resolve_pending_candidates` 两份实现，写法已分叉 |
| 38 | `paper.py:341-384` vs `replay.py:661-704` | `_rejection_summary` / `_summary_counts` / `_jsonable` / `_is_aware` 逐字重复 |
| 39 | `state_machine.py:186-208` | `serialize_commands=True` 与 Coordinator 双重串行，新调用点会静默变成全账户串行 |
| 40 | `coordinator.py:181-189` | `execute_approved_intent` 与 `submit` 同一实现、两套名字 |
| 41 | `service.py:177-185` | `MarketDataCaptureService.submit` / 溢出 HALT 路径是死代码 |
| 42 | `repository.py:245-254` | `load_active_entry_symbols_at` 全量拉 membership 再在 Python 过滤 |
| 43 | `runtime_states.py:288-303, 597-609` | realtime 关闭后仍保留 accumulator 直到 durable 关闭；bookTicker 最新值三份缓存 |

---

## 架构冗余（按维护风险排序）

### A1. 双 healthcheck + 生产都不用

**位置**：
- `apps/healthcheck.py`（276 行，SQLAlchemy）
- `apps/healthcheck_fast.py`（366 行，psycopg）
- 生产：`compose.server.yaml` / `compose.live.accounts.yaml` 全部用 `cml-local-healthcheck`（shell 文件标记）
- 另有 `health/local.py`、`research_collector/health.py`

两文件的 `--service` 选项、`_market_data_ready` / `_paper_ready` / `_live_ready` / `_execution_account_ready`、`_fresh` / `_as_utc` / `_process_started_at` 逻辑完全同构，SQL 语句只差占位符风格。`_sync_database_url` 实现还不一致。

**风险**：改 live readiness 条件必须改两处；生产已切到 local 文件标记后，这两套 DB 探针成为「仍被单测维护但无人调用」的影子实现。

**建议**：生产路径只保留 `health/local.py` + shell 探针。二选一保留 DB 探针（建议 `healthcheck_fast`），另一删掉或降为测试夹具。

---

### A2. Live 从 shadow CLI import 私有函数

**位置**：
- `apps/live_rollout/main.py:25-28`
- `live_rollout/postgres_runtime.py:12-16`
- 源：`apps/shadow_operation/main.py` 的 `_latest_account_state`、`_latest_risk_config`、`_load_trading_rules`

Live 生产 daemon 依赖 Shadow CLI 入口文件里的下划线私有函数。这些是账户状态、风险配置、交易规则读取——domain/persistence 能力，却住在某个 app 入口里。

**风险**：改 shadow CLI 结构可能打断 live 上线；live（写路径）与 shadow（抑制写路径）语义耦合。

**建议**：下沉到 `persistence/postgres` 旁的查询服务或 `execution_account` 侧模块；shadow 与 live 都从那里 import。禁止 `apps.*` 互相 import 私有符号。

---

### A3. 三个策略 runtime 的 warmup/cooldown/checkpoint 状态机逐行复制

**位置**：
- `strategies/order_flow_impulse/runtime.py`
- `strategies/liquidation_cascade/runtime.py`
- `strategies/compression_breakout/runtime.py`（变体）

`order_flow_impulse` 与 `liquidation_cascade` 的以下方法几乎逐字相同（各约 150 行）：
- `__init__` 的 `_buffers/_warmup/_cooldown_remaining/_last_processed/_signal_sequence`
- `restore` / `restore_checkpoint` / `warm_market_state` / `reset_symbol`
- `on_market_state`（缺价 → warmup → cooldown → 查事件 → 出信号）
- `checkpoint` / `_decision` / `_build_signal_and_candidate`

`compression_breakout` 用 `signal_buffers` 而非 `market_state_buffers`，是第三套变体。

**风险**：cooldown 语义、checkpoint 兼容、warm 预热逻辑任一处修改都可能只落在一个策略上；compression 与另外两个的 restore 键名不同，恢复行为易 silently drift。

**建议**：抽 `BufferedStrategyRuntime` 基类或组合对象；子类只提供 `required_data()`、`find_events()`、`reason` 字段映射。统一 checkpoint payload 键。

---

### A4. paper-live-pair 第 3–7 账户序数化复制粘贴

**位置**：`apps/strategy_runner/main.py`，`paper_live_pair_command`（约 L1109–1750）

- 7 段几乎相同的 `build_runtime_identity_for_cli(...)`（fixed/candle/third/fourth/fifth/sixth/seventh）
- 7 组 CLI 选项（`--third-run-id` … `--seventh-run-id`、各自 long_only/grace 参数）
- fixed / candle / third 各自手写一份 `PaperLiveDaemonConfig`；fourth–seventh 又在 `filtered_accounts` 循环里再写一份

**风险**：加第 8 个账户要改 CLI、identity 构建、config 组装、healthcheck run-ids 至少 4 处。

**建议**：改为 `list[PaperAccountSpec]`（yaml/json 或重复 `--account name=...,exit=...,long_only=...`），循环 build identity + config；去掉序数命名。

---

### A5. compose 配置漂移面

#### 5a. live accounts 三份整段拷贝

**位置**：`compose.live.accounts.yaml`：account-2/3/4 的 execution + live-strategy；`compose.server.yaml`：primary 的同类 command 块

四个 live 账户的 command 结构完全相同，仅 `account-label`、session/lease、策略参数默认值不同。

**建议**：用 compose `extends` / 生成式 yaml / 一个 `live-account` 模板 + 仅差分变量。策略参数只从 env 读，command 不再重复 argv。

#### 5b. paper run-id 列表三处硬编码同步

**位置**：`compose.server.yaml`
- `market-data.CML_PAPER_EXIT_RUN_IDS`（8 个 id）
- `dashboard.CML_PAPER_ACCOUNT_RUN_IDS`（同 8 个）
- `paper-orderflow-pair.CML_HEALTHCHECK_RUN_IDS`（其中 4 个）

**风险**：新 paper 账户只更新一处：market-data 不保护持仓、dashboard 不展示、healthcheck 不检查——属于生产事故级配置漂移。

**建议**：单一源：`configs/paper_accounts.yaml` 或 env 文件，由 entrypoint/脚本展开；或 compose 用 `x-paper-run-ids` 锚点 + YAML 别名引用同一串。

---

### A6. `MarketState15s` 三套字段映射

**位置**：
- `persistence/postgres/runtime_state_repository.py`：`runtime_state_row()`、`market_state_from_row()`
- `persistence/postgres/models.py`：`RuntimeMarketState15sRow`
- `strategies/runtime_checkpoint.py`：`market_state_payload()` / `market_state_from_payload()`

domain 一个 dataclass，被手工映射为 ORM dict（约 40 字段）和 checkpoint JSON（约 25 字段）。checkpoint payload 已缺少部分 kline 字段。

**风险**：给 `MarketState15s` 加字段要改 4+ 处；漏改 checkpoint 会导致重启恢复的策略状态与 DB 状态不一致，且无编译期保护。

**建议**：以 domain dataclass 为唯一源；至少把 payload 字段集与 Row 字段集做成同一 tuple 并加单测断言对齐。

---

### A7. 各 app 入口各自实现 database URL 解析（5+ 套）

**位置**：
- `apps/live_rollout/main.py`：`_execution/_market/_observability/_database_url`
- `apps/execution_account/main.py`：`_execution_database_url`
- `apps/shadow_operation/main.py`：`_database_url`
- `apps/market_data/main.py`：`_market_database_url`
- `apps/strategy_runner/main.py`：4 处内联 `os.environ.get("CML_DATABASE_URL")`
- `apps/operator_dashboard/main.py`：argparse default
- `apps/research_collector/main.py`：market→default fallback

同一概念（CLI flag → plane env → `CML_DATABASE_URL`），错误文案、是否允空、fallback 顺序各写一套。

**建议**：提供 `config/database_url.py`：`resolve_database_url(plane=..., cli=..., required=True)`，所有入口调用。

---

### A8. 未被调用的 repository 方法 / 空壳 port

| 项 | 位置 | 现状 |
|---|---|---|
| `load_run_summary` | `strategy_run_repository.py:174` | 仅定义，无调用方 |
| `load_paper_report_artifacts` | 同上 :186 | 仅定义 |
| `count_quality_events` / `latest_process_state` | `capture_repository.py:105,112` | 仅定义 |
| `save_command` | `live_rollout_repository.py:104` | 仅定义（rollback 命令无落盘调用） |
| `MonitoringObligationProvider` / `NoMonitoringObligations` | `domain/universe/ports.py` | 全库只使用默认空实现 |
| `UniverseRepository.load_snapshot` | ports + repo | 调用方全用 `load_snapshot_at` |

**建议**：删除无调用方法；`MonitoringObligationProvider` 要么接到 paper/live 的 protected symbols 上，要么删掉 port。

---

### A9. capture 配置双份 + environment 文件薄壳

**位置**：
- `configs/capture/binance_usdm.yaml` vs `configs/capture/server_paper.yaml`
- `configs/environments/research.yaml` vs `server_paper.yaml`

约 20 个共享键完全重复，仅 streams/队列字节/realtime delay/book_ticker 策略不同。

**建议**：`configs/capture/base.yaml` + 两个 overlay 用 loader 深合并。

---

### A10. `liquidation_cascade` 全量 runtime 接入但无生产部署

完整策略栈（runtime、event_study、registry、paper report）为 research-only 策略付出维护成本；registry 的 `supported_strategy_names()` 对外暗示可跑，但 compose 中无任何 liquidation paper/live 服务。

**建议**：明确策略生命周期：research-only 策略从 `build_runtime_strategy` 主路径剥离或标记 `experimental`。

---

### A11. `apps/market_data/main.py` 启动/收尾双份

`run_market_data` vs `run_market_data_for`：两函数均做 LocalHealthWriter 回调、build runtime、start hub/quote/publisher/capture、universe 启动刷新、scheduler/retention/subscription/health 任务组、finally 里 cancel + stop。

**建议**：抽出 `async def _start_market_data_stack(runtime) -> tasks` + `_stop_stack(...)`；两个入口只差停止条件。

---

## 建议修复顺序

### 1. 立刻（实盘安全）
- P0 #1：submit 网络错误包装
- P0 #2：撤单失败语义改为 UNKNOWN
- P0 #3：GTD 预检异常类型改为 OrderPreSubmissionError

共同消除「写前日志已提交但交换结果未知时 fail-open」。

### 2. 本迭代
- Coordinator 锁/close 竞态（#4、#5）
- 终态回退保护（#6）
- 15m K 线源 fail-soft（#8）
- 成对账户 EMA/cooldown（#9、#10）
- manifest journal 回放（#16）
- aggTrade 恢复时序（#17）
- DiskSpaceGuard 接线（#15）
- `_seen_trade_ids` 内存泄漏（#7）
- paper gap reset（#11）、latency 一致性（#12）

### 3. 结构债
- 策略 runtime 基类（A3）
- healthcheck 收敛（A1）
- shadow 私有函数下沉（A2）
- paper 账户列表化（A4）
- 配置单源：run-id、live compose、database_url、MarketState 映射（A5、A6、A7）
- 删死代码/port（A8、#41）
- capture overlay、liquidation 定位、market_data 双入口（A9、A10、A11）

---

## 部署脚本审查

审查范围：`deploy/ops/update_server.sh`、`deploy/ops/cml_ops_monitor.py`、`deploy/ops/cml-ops-monitor.service`、`deploy/nginx/crypto-momentum-lab.conf`、`Dockerfile`、`docker/local-healthcheck`、`compose.server.yaml`、`compose.live.accounts.yaml`、`.env.server.example`。

部署链路整体比应用层扎实（flock 锁、分阶段恢复、live preflight、health 本地化都做得不错），但有几处会直接影响发布安全或造成监控盲区。

### D1. [高] `update_server.sh` 无条件加载 live overlay + `--profile live`

**位置**：`deploy/ops/update_server.sh:463-469`

```bash
compose=(
  docker compose
  --env-file .env.server
  -f compose.server.yaml
  -f compose.live.accounts.yaml
  --profile live
)
```

脚本注释写「live 仅在 `--live` 时触碰」，但 compose 数组从第一步 `compose config` 起就永远带上 live overlay 和 live profile。

`compose.live.accounts.yaml` 对 account-2/3/4 的凭证是强制的（`:?set`），而 `.env.server.example` 里这些值是空字符串。空值在 `:?` 下会失败。

**后果**：只要没配齐四套 live 凭证，连 **paper-only 更新** 的 `compose config` 都会直接失败，部署脚本无法使用。

**建议**：按 `--live` 决定是否拼入 `compose.live.accounts.yaml` 和 `--profile live`。

---

### D2. [高] 变更分类路径写错：`strategy/*` ≠ `strategies/*`

**位置**：`deploy/ops/update_server.sh:375`

```bash
src/crypto_momentum_lab/strategy/*|\
```

实际目录是 `src/crypto_momentum_lab/strategies/`（复数）。策略 runtime 改动会落入兜底的 `src/*`，把 `market/research/paper/dashboard/live` 全部标脏。

**后果**：只改策略逻辑也会强制重启 market-data / research / dashboard / live。market-data 重启会换 Hub epoch、触发 research cursor 重置，是整条链路里最贵、最容易出数据缺口的操作。

**建议**：改成 `src/crypto_momentum_lab/strategies/*`，并单独列出 `strategy_runner/*`。

---

### D3. [高] ops-monitor 只盯 primary，account-2/3/4 完全失明

**位置**：
- `deploy/ops/cml-ops-monitor.service:13`（只加载 `compose.server.yaml`）
- `deploy/ops/cml_ops_monitor.py:27-32`（默认服务列表仅 primary）

systemd 单元不加载 `compose.live.accounts.yaml`，默认服务列表也没有 account-2/3/4 的 execution/strategy。DB 侧 checkpoint/lease 也只查 `live-primary-v1`。

**后果**：`live-strategy-account-2` OOM 或 unhealthy 时监控静默。

**建议**：systemd 支持多 compose 文件；服务列表与 live 账户清单对齐，或从 compose 动态发现。

---

### D4. [高] primary live 凭证是「可空」，account-2/3/4 是「强制」

**位置**：
- `compose.server.yaml:451-452`（primary read：`${BINANCE_READ_API_KEY:-}`）
- `compose.server.yaml:514-515`（primary trade：`${BINANCE_TRADE_API_KEY:-}`）
- `compose.live.accounts.yaml`（account-2/3/4：`:?set` 强制）

**后果**：误开 live profile 时，primary 可能带着空 key 启动，在运行期才失败；其他账户则在 compose 解析期就拦下。fail-closed 策略不一致。

**建议**：primary 与 account-2/3/4 统一为 `:?` 强制；paper-only 部署不要启用 live profile（与 D1 一起修）。

---

### D5. [中] 多账户持仓保护依赖手工维护的 labels 列表

**位置**：`compose.server.yaml:145-146`

```yaml
CML_LIVE_POSITION_ACCOUNT_LABEL: ${CML_LIVE_ACCOUNT_LABEL:?set CML_LIVE_ACCOUNT_LABEL}
CML_LIVE_POSITION_ACCOUNT_LABELS: ${CML_LIVE_POSITION_ACCOUNT_LABELS:-}
```

example 里 `CML_LIVE_POSITION_ACCOUNT_LABELS` 是注释掉的。若上线 account-2 却忘填列表，market-data **不会保护** 该账户持仓对应的 symbol——属于静默敞口风险。

**建议**：live profile 启动时校验「所有 running live 账户 ⊆ position labels」；或从 compose live 服务自动推导 labels。

---

### D6. [中] primary 与 account-2/3/4 的 `persist-exchange-operations` 默认值不一致

| 账户 | 默认 |
|------|------|
| primary | `${CML_LIVE_PERSIST_EXCHANGE_OPERATIONS:-}` → 空（legacy 全量持久化） |
| account-2/3/4 | `submit,cancel` |

同一次部署里，四个账户的交易所遥测落库策略不同，排障时对不上账。

---

### D7. [中] live-strategy 参数同时写在 env 和 argv

primary / account-2/3/4 的 `impulse-window-buckets`、`min-return-pct` 等在 `environment` 和 `command` 各写一份。改 env 不改 argv（或反过来）会导致进程实际用的是 argv 值，env 形同虚设。

**建议**：只保留一条注入路径（优先 argv 由 env 展开，删掉重复的 environment 键）。

---

### D8. [中] nginx 反代无应用层鉴权

**位置**：`deploy/nginx/crypto-momentum-lab.conf:5-11`

dashboard 绑在 `127.0.0.1:8765`，经 nginx `/momentum/` 暴露。location 内无 `auth_basic` / mTLS / IP allowlist。若 server 块层面没有额外防护，运维面板等于公开。

---

### D9. [中] 部署清单测试未覆盖 `paper-orderflow-gainer10-pair`

`tests/smoke/test_server_deployment_manifest.py` 断言了 3 个 paper 服务，compose 里实际有 4 个。gainer10-pair 的配置漂移不会被测试拦住。

---

### D10. [中] 无备份/恢复编排

有 `postgres-data` volume 和 retention，但 `deploy/` 下没有备份 cron、WAL 归档或恢复演练脚本。live 账户状态、lease、order 事件都在这一个库上。

---

### 部署侧小问题

- `volume-init-check` 用 exit 0=需要 init、1=正常，语义反直觉，建议改成显式输出
- ops-monitor `User=root`，可再加 `ProtectSystem=strict` 等 hardening
- account-3/4 的 `min_notional_5m_vs_30m` 默认 0（关闭过滤），与 primary/account-2 的 1.50 不同——测试里是 intentional，但账户间策略差异容易被忽略
- market-data 冷启动 `start_period: 15m`，paper 全体依赖其 healthy，首次拉起会等很久（有意为之，但要有心理预期）

### 部署侧做得好的地方

- `flock` 部署锁 + state 文件分阶段恢复，支持断点续跑
- live 更新前先 renew lease + preflight，失败不重启
- research-collector 先停再换 market-data，保护 durable cursor
- healthcheck 走本地文件标记，探针不打 DB
- Dockerfile 依赖层与源码层分离，`CML_CODE_COMMIT` 后置
- volume-init 先检查 ownership 再 chown
- rollback 用 `reset --keep` 且校验祖先关系

### 部署修复优先级

1. **立刻**：D1 live overlay 按需加载；D4 primary 凭证 fail-closed
2. **本迭代**：D2 `strategies/*` 路径修正；D3 ops-monitor 覆盖全部 live 账户；D5 position labels 校验
3. **结构债**：D6/D7 配置单源；D8 nginx 鉴权；D9 补齐部署测试；D10 备份编排

---

## 附：设计点说明（非 bug）

| 点 | 说明 |
|----|------|
| fill 幂等 | `save_fills` + `_insert_idempotent`，checkpoint 滞后重放同 candidate 不会双开仓（DB 层去重） |
| 信号 bar 收盘成交 | latency=0 在「已收盘 bar」上成交是注释写明的意图；问题在于默认值与 replay 不一致（见 #12） |
| 持仓批次 | paper 每个 FILLED entry 独立 `PaperPosition`，符合 CONTEXT.md 批次模型，非 netting bug |
| event_study 确认 | `_confirmed_detection_index` 只使用 ≤ detection 的 bar，runtime 过滤 `detected_at == state.bucket_start`，未见前视 |
| domain 校验 | 大量 `__post_init__` 时区/正数/非空校验，风格一致且偏严，是优点 |
| 风控 gateway | `RiskGateway` 逻辑本身正确，但未接入 paper daemon（见审查 P2 #12） |
