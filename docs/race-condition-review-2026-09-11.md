# 竞态条件审查报告

- 审查日期：2026-09-11
- 审查范围：实盘执行路径、账户同步、行情 hub、租约/风控、持久化层的并发正确性
- 审查方式：并行模块审查（订单生命周期、live rollout、持久化、行情/账户同步）+ 人工交叉核验源码
- 对照前置：[code-review-2026-03-19.md](./code-review-2026-03-19.md) 与 [code-review-2026-03-19-repair-plan.md](./code-review-2026-03-19-repair-plan.md)
- 代码基线：当前工作树；合并前应补充固定 commit SHA，避免审查结论随未提交变更漂移
- 初始回归核验：相关单测 `95 passed, 1 skipped`；跳过项为需要本机 loopback 权限的 `test_hub.py`
- 修复后回归核验：`tests/unit` 为 `1031 passed, 4 skipped`，`tests/integration` 为 `61 passed`；4 个跳过项均为需要本机 loopback 权限的 hub 测试
- 整仓回归核验：`1128 passed, 4 skipped, 1 failed`；唯一失败为需要外部 live 数据库环境变量的 `tests/smoke/test_live_capture_manifest.py`（未设置 `CML_TEST_ASYNC_DATABASE_URL/CML_DATABASE_URL`）

## 总体判断

3 月评审中的 P1 #4–#7（Coordinator get-or-create 竞态、scheduler close 挂起、终态回退、成交去重无界增长）以及 P2 #22（连接池无锁）在当前源码中可见对应保护；最终“已修复”仍应以固定 commit 和回归测试结果作为证据。

订单投影层和决策临界区的主要修复已经落地：prepare/execute 已进入同一 Coordinator 操作，退出通道有共享锁和 durable episode reservation，入场有数据库敞口 claim，Risk 通过后的上下文与 POST 前的 entry gate 也会再次校验。

本轮已把此前剩余的代码缺口收口：用户数据事件带交易所时间和可用更新水位，Hub/客户端暴露背压与恢复计数，活动仓位查询改为单语句一致性读，Coordinator 增加入场排空闸门并接入关停与 schedule flatten。滚动发布仍需要在真实环境验证旧版本 worker、SIGTERM 和交易所撤单/归零，但这些属于发布演练，不再是代码中未实现的保护。

本次复核重新校准了严重性：#1–#7 的代码保护已落地；#8 机制成立但主要是 fail-closed 可用性风险；#16 为误报。以上是修复前的审查结论，当前工作树的修复进度见“本轮修复状态”。

### 证据分级

| 标签 | 含义 |
|---|---|
| Confirmed | 当前代码存在可构造的交错，且不依赖未实现的未来机制 |
| Conditional | 需要多进程、特定故障、特定时序或未来重放机制才成立 |
| Not reproducible as-is | 报告描述的当前控制流与源码不一致；保留为设计提醒，不作为当前缺陷 |

## 本轮修复状态

以下状态对应当前工作树中的实现与测试；合并前仍需绑定最终 commit SHA。

| 条目 | 状态 | 已落地内容 / 剩余边界 |
|---|---|---|
| #1 | 已修复 | 对账/恢复期间使用有界 deferred buffer，按交易所事件时间、`event_at` 等字段重放；缓冲溢出触发完整 recovery；覆盖初始对账、周期对账和 snapshot 窗口 |
| #2 | 已修复（按可用水位 fail-closed） | `BinanceUserDataEvent` 保留交易所事件/事务时间，并解析可选 `u/pu` 更新水位；订单按交易所时间排序，更新水位回退或断档触发 recovery。Binance 未提供序列号的事件类型使用交易所时间兜底，不再把本地 `received_at` 当唯一顺序 |
| #3 | 已修复 | Coordinator 内新增 `prepare_and_execute`，prepare 与 exchange submit 在同一 per-key scheduler 操作中完成 |
| #4 | 已修复（新提交路径） | quote/candle/grace/recovery 共用 exit decision lock；同一 live position episode 新增数据库 durable reservation，终态释放；旧版本遗留未记账订单仍需滚动发布验收 |
| #5 | 已修复 | context load 在 epoch/account sequence 变化后重试；各退出 lane 和提交前均复检 context currentness；lease 更新使 cache 失效 |
| #6 | 已修复 | lease heartbeat 失败后 entry fail-closed；RiskGateway 校验 owner/lease/account/strategy；exchange POST 前再次做 lease/halt fencing |
| #7 | 已修复（新提交路径） | `prepare_submission` 事务内锁定并重读 active lease/halt；同一账户/策略用 advisory lock 原子检查 max positions、daily loss、gross exposure 并写入 durable exposure claim，终态释放；混跑窗口仍需发布前置控制 |
| #8 | 已修复 | per-environment publish lock 下先完成 encode，再分配 sequence、写 replay buffer 和扇出 |
| #9 | 已修复 | fill cursor 按 `last_checked_at` 单调 fencing；ID/time 两种游标切换时清空另一列并保持 check constraint |
| #10 | 已修复 | recovery generation 防止恢复期间的新 recovery 请求被 clear 吞掉 |
| #17–#20 | 已修复 | paper 计数器与 run 初始化改为原子写；checkpoint 和 operator state 加时间守卫；open-order snapshot 使用 advisory lock + observation fence |
| #22 | 已修复 | 新增持久化 `runtime_market_state_gaps`，gap 幂等记录并在状态插入时重新应用完整性标记 |
| #23 | 已修复 | `exchange_fills(client_order_id, exchange_trade_id)` 增加唯一索引和迁移 |
| #24 | 已修复 | `strategy_live_states` upsert 只接受不早于当前行的 `changed_at` |
| #11 | 已修复 | Hub 记录发布量、订阅者队列溢出和 replay 请求；客户端记录队列溢出、snapshot recovery 次数及最近原因；原有 fail-closed recovery 保持不变 |
| #12 | 已修复 | 未知 exchange-visible 入场单先写入 synthetic intent/order，再经 Coordinator/state machine 撤单，不再直调 client cancel |
| #13 | 已修复 | risk telemetry await 后、submission scheduler 内和 POST 前均复检 context/entry gate；数据库 claim 再提供跨 writer 仲裁 |
| #14 | 已修复（代码路径） | Coordinator 关停先阻断新入场、等待已在执行 operation，再关闭 scheduler；caller 取消后的 in-flight 等待已有回归测试。真实 SIGTERM/超时演练仍是发布验收 |
| #15 | 已修复（代码路径） | schedule gate 同时阻断 Coordinator 入场、等待在途入场归零、再撤单/flatten/交易所归零确认；真实交易所切换仍需发布演练 |
| #21 | 已修复 | `load_active_position_symbols` 用单条 SQL 将最新 ready run 与不晚于该 run 的 position snapshot 绑定，避免多语句读混代 |

本轮没有按缺陷修改 #16；它仍是当前单事件循环模型下不可复现的问题。

补充修复：行情 WebSocket 正常停止时先关闭连接并有界排空 reader/dispatch 队列，再取消控制任务，避免已进入 socket 缓冲区的最后一批行情在关停时丢失；对应市场数据 E2E 已通过。

### 滚动发布硬前置

- 先执行 `20260911_0034` migration，再启动使用 durable exit/exposure claim 的新 worker；
- overlap 期间必须保证旧 worker 已停止下单或已失去 active lease，不能让旧版本绕过新 claim 继续 POST；
- 旧 worker 遗留的 `SUBMITTING`/open entry 和 exit order 必须先完成 reconcile/adopt，再宣布新 worker 接管完成；
- 回滚代码时不能回滚 schema，也不能在 claim 表存在 active row 时让旧版本恢复独占写入。

因此，代码层面的遗留项已清零；本次实盘滚动上线仍必须保留三项运行验收：先迁移再启动新 worker、确认旧 worker 已失去下单资格、以及演练 SIGTERM/撤单确认/flatten 归零。它们验证的是部署状态与外部交易所行为，不能仅靠单测替代。

---

## P0 — 已确认风险的修复证据

### 1. REST 对账窗口内静默丢弃用户数据事件

**状态/置信度**：已修复；以下交错是修复前证据，保留用于说明为什么必须使用有界 freeze/replay。

**位置**：`execution_account/daemon.py:512-513`  
**相关**：`daemon.py:871-899`（`_reconcile` 置 `_accept_events=False` 后 drain）

```python
if not self._accept_events or self._state is None:
    return
```

**交错**：

1. `_reconcile` / `_snapshot` 将 `_accept_events=False`，drain 事件队列与 persistence 队列；
2. REST `fetch_*` 在途（几十到几百毫秒）；
3. Binance 推送 `ORDER_TRADE_UPDATE` / `ACCOUNT_UPDATE`；
4. `_on_event` 在第 512 行直接 `return`——事件**既不入队也不延迟**；
5. REST 完成 → `replace_snapshot` → `_accept_events=True`。

**后果**：窗口内的成交、仓位、挂单变更可能同时缺失于内存态和 account-event hub。后续 REST 对账最终可能修复部分状态，但在下一次对账或 fill 审计前，实盘策略可能基于陈旧仓位/余额做决策。

**修复后行为**：`_on_event` 在对账/恢复窗口进入有界 deferred buffer；缓冲溢出会触发完整 recovery，恢复完成后按交易所时间重放，而不是静默丢弃。

**修复实现**：采用带上限的 freeze/replay 方案：冻结期间保留事件；REST 完成后按交易所时间/可用流水位重放；缓冲溢出或序列不连续时触发完整快照恢复。

**验收标准**：在 REST fetch 被 `asyncio.Event` 阻塞期间注入 `ACCOUNT_UPDATE` 和带成交的 `ORDER_TRADE_UPDATE`，恢复后 account state、account-event hub 和 fill 持久化均可观察到该事件，且同一 trade 不重复。

---

## 已修复的账户事件水位边界

### 2. 跨对账边界的订单事件被 `received_at` 守卫吞掉

**状态/置信度**：已修复；Binance 未提供更新序列的事件类型仍明确使用交易所事件/事务时间兜底，并在时间回退时 fail-closed 触发对账。

**位置**：`execution_account/user_data_sync.py:85-87, 269-271`  
**相关**：`daemon.py:941`（`replace_snapshot` 调用点）

**已落地**：

1. `BinanceUserDataEvent` 保存交易所事务/事件时间 `exchange_event_at`，可用时解析 `u/pu` 更新水位；
2. deferred replay 按交易所时间排序；订单状态不再以本地 `received_at` 作为唯一顺序，旧交易所事件会被拒绝并请求 recovery；
3. 更新水位回退或 `pu` 不连续直接抛出 `UserDataStateError`，由账户 daemon 进入完整 REST recovery；
4. 增加时间倒退、序列断档和跨 Hub wire encode/decode 的回归测试。

Binance 的部分 user-data 事件本身没有流序号，因此不能凭空制造交易所序列；这不是未修复项，而是当前实现明确记录的外部接口边界。此类事件只能用交易所 `E/T` 做保守水位判断，异常时走 recovery。

---

## P1 — 决策临界区与上下文一致性的修复证据

### 3. `prepare_submission` 在 Coordinator 锁外，reconcile 可先把订单标成 UNKNOWN

**状态/置信度**：Confirmed；建议定为 High P1。当前证据证明存在 `SUBMITTING → UNKNOWN` 的窗口，但“双单”还需要 recovery、超时或再次决策等后续条件，不应表述为必然结果。

**位置**：`live_rollout/daemon.py:3357-3390`  
**相关**：`apps/live_rollout/main.py:3525-3532`（`_reconcile_run_orders`）  
`orders/state_machine.py:242-288`（`execute_approved_intent` 对 `prepared_submission` 不重读 durable state）

**交错**：

1. `_execute_candidate` 在**不持有** `(account, symbol, position_side)` 调度锁时调用 `prepare_submission`，DB 写入 `SUBMITTING`；
2. 同一时刻周期 reconcile 或账户 WS reconcile 的 `load_unresolved_orders` 看到该 `SUBMITTING` 单；
3. reconcile 进入 Coordinator（低优先级，但此时 execute 尚未入队），`query_order_by_client_id` 查不到 → 重试后写入 `UNKNOWN_PENDING_RECONCILIATION`；
4. 原 `_execute_candidate` 才调用 `execute_approved_intent(prepared_submission=...)`，直接 `submit_order`；
5. 交易所上出现真实订单，本地却是 UNKNOWN；reduce-only 路径随后可能触发 `_recover_unknown_exit` 再下一张恢复单。

**后果**：本地可能在交易所订单尚未可见时进入 `UNKNOWN_PENDING_RECONCILIATION`，造成短暂状态分叉；在 recovery 介入时可能形成原单和恢复单的重复提交。一次正常 submit 成功后可能修复本地状态，因此“长期错误”不是必然结果。

**现有缓解**：

- `prepare_submission` 唯一 `client_order_id` 插入防止同一计划二次 prepare（集成测试 `test_concurrent_prepare_grants_only_one_submission`）；
- Coordinator 同 key 串行 execute/cancel/reconcile。

**均无法覆盖「prepare 已提交、execute 未入队」窗口。**

**建议修复**：

- 提供一个由 Coordinator 串行化的 `prepare_and_submit` 工作流，或在同一 key 锁内完成 prepare、重读 durable state 和 submit；
- execute 持锁重读订单行：只有仍为本次 intent 的 `SUBMITTING` 才允许 POST；`UNKNOWN`/其他非预期状态必须 fail-closed，不能仅靠时间判断继续提交；
- reconcile 对刚创建的 `SUBMITTING` 可短暂退让，但这只能降低概率，不能替代 durable claim/fencing；
- 增加测试：精确卡在 prepare commit 与 execute 入队之间，断言交易所 POST 次数和 durable state 轨迹。

---

### 4. Quote 退出通道与 Candle/Grace 通道用不同锁，可对同一仓位双下 reduce-only 单

**状态/置信度**：Confirmed/Conditional；建议定为 High P1。只有同一仓位的两条通道在新鲜状态传播前同时生成决策时才会触发，不是所有 quote/candle 重叠都必然双单。

**位置**：`live_rollout/daemon.py:2601-2624, 2641-2668, 2704-2735`  
**相关**：`live_rollout/exits.py:538-579, 810-821`  
账户事件直调 quote 路径：`daemon.py:1003-1008`

**交错**：

1. Market/Candle worker 持 `_exit_symbol_locks[symbol]`，基于 `context.managed_positions` 生成 candle/grace 退出请求；
2. Quote worker 或账户事件 `ORDER_TRADE_UPDATE` 同时持 `_quote_symbol_locks[symbol]`，基于可能过期的同一 context 生成 realtime stop-loss/TP 请求；
3. 两边 candidate identity 不同：
   - quote：`identity_trigger_at = position.opened_at`（`exits.py:575`）
   - candle：`identity_trigger_at = candle.candle_end`
4. `prepare_submission` 只按 `client_order_id` 去重 → 两张都拿到 grant；
5. Coordinator 只串行「提交动作」，不阻止两张不同 ID 的单先后 POST。

代码注释明确「锁故意分开」（`daemon.py:2712-2715`），只依赖 Coordinator 串行**命令**，不串行**决策**。

**后果**：可能出现同一仓位的两张不同 identity 的 reduce-only 平仓单。交易所可能拒绝第二张，也可能在部分成交窗口产生重复费用或数量竞争；不能直接断言两张都会完整成交。

**现有缓解**：reduce-only 防止反向开仓，不能防止双平仓意图；recovery 路径有 `_exit_recovery_locks`，但普通 quote/candle 双通道没有共享决策锁。

**建议修复**：

- quote/candle/grace/recovery 共用按 account、symbol、position_side、position episode 划分的 **exit-decision** 锁；
- 更可靠的方案是持久化 exit reservation/episode claim，让不同 identity 也能被仲裁；短 TTL 内存 set 只能作为额外防线；
- Coordinator 继续负责 I/O 顺序，但不能代替决策层去重。

---

### 5. `_load_context` 在 epoch 失效后仍返回陈旧账户快照

**状态/置信度**：Confirmed；建议定为 High P1。

**位置**：`live_rollout/postgres_runtime.py:241-392`  
**相关**：`update_account_snapshot`（`:394-424`）会 `invalidate_cache()`（`_cache_epoch += 1`）

**交错**：

1. Quote/candle/grace lane 调用 `PostgresLiveContextProvider.__call__` → `_load_context`，捕获 `realtime_account_snapshot = snapshot_A`（`:242-246`），然后 `await asyncio.gather(...)` 做 DB I/O；
2. 等待期间账户事件通道执行 `update_account_snapshot(snapshot_B)`，换掉内存快照并 `invalidate_cache()`；
3. 账户事件路径对 `snapshot_B` 正确处理退出；
4. Step-1 的 gather 结束。`:382-388` 的 epoch 检查**只跳过写缓存**，方法仍在 `:392` `return context`（基于 `snapshot_A`）。

`LiveStrategyDaemon._states_with_prefetched_context` 的 generation 计数（`daemon.py:1750, 2241`）保护的是 **market entry loop**；exit lanes 直接调 provider，**无 generation 复查**：

- `process_market_quote`（`daemon.py:1037`）
- `process_closed_candle`（`daemon.py:1074`）
- `process_grace_timeout`（`daemon.py:1105`）

**后果**：

- 退出决策（TP/SL/15m 收盘）基于陈旧 `managed_positions` / 数量；
- `_publish_managed_position_symbols`（`daemon.py:734-750`）会把陈旧 symbol 集合推给 `closed_candle_feed.set_symbols`，新开仓 symbol 可能被挤出 15m 订阅，直到下一次新鲜 publish 才恢复。

**现有缓解**：reduce-only 交易所约束；账户事件 lane 自身正确；下一次 quote/candle 通常会重载。

**建议修复**：`_load_context` 在 gather 后若 `_cache_epoch != cache_epoch` 则循环重载而不是返回；或给 context 打上 `account_snapshot_sequence`，exit lane 发现更新序号则放弃本次决策。

**验收标准**：在 context 的 DB gather 阶段更新账户快照，退出 lane 不得使用旧 snapshot 生成订单，也不得用旧 symbol 集合覆盖 candle 订阅。

---

## P1（续）— 租约、风控与限价的修复证据

### 6. Lease 心跳失败不早关闸；RiskGateway 不校验 owner

**状态/置信度**：Confirmed/Conditional；建议定为 P1。`evaluate_live_gate` 已有 owner/account/strategy 检查，缺口在最终 `RiskGateway`、心跳 fail-closed 以及旧 context 覆盖新 lease 的路径；只有 lease 过期、抢占或时钟/缓存偏差等条件叠加时才会形成实际越权提交。

**位置**：

- `live_rollout/lease.py:135-145`（连续失败只 backoff + callback）
- `apps/live_rollout/main.py:2455-2481`（`on_error` 仅记日志）
- `risk/gateway.py:42-52`（只检查 `ACTIVE` + `expires_at`，**不检查 `lease.owner`**）
- `postgres_runtime.py:433-450`（`update_lease` 可能被在途 `_load_context` 用旧 lease 覆盖缓存）

**交错**：

1. 心跳从 `expires_at - 120s` 开始续约；续约连续失败（DB 抖动 / heartbeat pool 超时）；
2. `entry_enabled` 不会清除；lease task 存活，supervisor 不拆进程；
3. 预取/缓存 context 仍携带旧 `TradingLease`（`expires_at` 仍在本地未来）；
4. 决策持续到本地 wall-clock `expires_at`；
5. 另一 worker/operator 路径在 DB 侧过期后 `acquire_lease`（部分唯一索引在过期后允许新 ACTIVE 行）；
6. 时钟偏移时，RiskGateway 对仍显示未过期的旧 lease 继续 APPROVE。

**DB 侧已有**：`renew_lease`/`release_lease` 用 `with_for_update`；部分唯一索引 `uq_trading_leases_active_account`；`evaluate_live_gate` 校验 `lease.owner == required_lease_owner`（`gates.py:88-100`）。

**缺口**：`_execute_candidate` 最后一道是 RiskGateway，**缺 owner/account/strategy 匹配**；心跳失败不 fail-closed。不能把已有的 `evaluate_live_gate` owner 检查描述成不存在。

**建议修复**：

- 连续 N 次续约失败后 `daemon.set_entry_enabled(False, reason="lease_heartbeat_degraded")`；
- `RiskContext` 带 `required_lease_owner`，owner 不匹配直接 REJECT；
- `update_lease` 后 bump `_cache_epoch` 或使用 lease generation，避免在途 load 用旧 lease 覆盖；只有同一 lease/owner 才允许合并 `expires_at`，不能跨 owner 盲目取 max；
- 在实际 exchange POST 前保留一次轻量 lease fencing 校验，明确已在途订单的处理语义。

---

### 7. Halt / 风险限额在 evaluate → prepare 之间是 check-then-act

**状态/置信度**：Confirmed/Conditional；建议定为 P1。单进程单 writer 时窗口较短，但跨进程、lease 抢占或 operator halt 时会放大为真实风控缺口。

**位置**：

- `risk/gateway.py:115-137`
- `live_rollout/limits.py:32-69`
- `live_rollout/daemon.py:3241-3303`
- `persistence/postgres/order_repository.py:112-238`（`prepare_submission` 事务不重读 halts/lease）
- `persistence/postgres/risk_repository.py:209-224`（`save_halt` 独立提交）

**交错**：

1. Context 快照：`open_position_symbols = {A,B,C}`，`max_open_positions=3`；
2. Candidate D：limits + RiskGateway 均 APPROVED；
3. `prepare_submission` / 交易所提交之前，另一路径已开 D，或 operator `save_halt(active=true)` 已提交；
4. 订单仍带着过期的 APPROVED evaluation 被提交。

`max_daily_loss` / `max_gross_exposure` 只对账户快照 + 内存 pending 判断，**没有 DB 行扣减**。跨进程（租约被抢 / split-brain）时无 advisory lock / exposure claim。

**现有缓解**：进程内 `_pending_entry_plans` + `unresolved_orders` 折入 reservation；`has_unresolved_order` 对不确定态 fail-closed；单进程候选循环顺序执行；设计上依赖单 lease writer。

**注意**：仅在 `prepare_submission` 事务内对 halt/lease 做 `FOR SHARE`，不能覆盖事务提交后到 exchange POST 之间的窗口；它最多保证检查期间的 DB 一致性。

**建议修复**：

- 在 `prepare_submission` 同一事务内重读 active halt/lease，并创建带 `lease_generation`/fencing token 的 intent；任一不满足则 abort；
- 持久化 exposure claim（account, symbol, position_side, notional, lease_id/generation）与 intent 同事务插入，按剩余 cap 条件写入，终态订单时释放；
- exchange POST 前验证 fencing token；token 失效时禁止新 POST，并对已在途请求按明确的 UNKNOWN/reconcile 规则处理；
- 对 max positions、daily loss、gross exposure 分别补充并发测试，而不是只测试单进程 `_pending_entry_plans`。

---

### 8. MarketStateHub 先分配 sequence，再 encode，再写 replay buffer

**状态/置信度**：Conditional；机制成立，但建议从 P1 降为 P2/可用性风险。当前路径 fail-closed，不构成静默数据丢失。

**位置**：`market_data/hub.py:270-286, 427-458`

```python
sequence = self._sequence_by_environment.get(environment, 0) + 1
self._sequence_by_environment[environment] = sequence
message = await asyncio.to_thread(encode_market_state_batch, ...)  # 事件循环可切换
replay_buffer.append((sequence, message))
```

**交错**：

1. `publish` 写入 `sequence = N`；
2. `await asyncio.to_thread(encode...)`，其他任务可运行；
3. 订阅者重连；`_replay_snapshot` 读到 `latest_sequence = N`，但 `_replay_buffers` 尚未包含 N；
4. `replay_available=False` fail-closed；
5. encode 完成后 append，live 订阅者才收到。

**后果**：本身 fail-closed（不静默丢数据）。与 `_enqueue_latest` 满队列“只保留最新”叠加后，慢消费者可能 SequenceGap → 重连 → 再次撞窗口，形成 replay unavailable 和恢复风暴；主要影响可用性和延迟，不是静默正确性错误。

**建议修复**：先 encode，再在 per-environment publish lock 内完成“分配 sequence + append replay buffer + 扇出”的无 await 临界区；如果架构明确保证单 publisher，也应增加并发断言和 encode 阻塞测试，防止未来调用方破坏该前提。

**验收标准**：在 encode 阻塞期间用 `last_sequence=N-1` 重连，系统只能返回一致的 replay/unavailable 结果；恢复后不得出现序列倒退或重复消息。

---

## P2 — 有界窗口 / 进程不变量依赖

下列条目多数依赖多进程、failover、多个 writer 或特定故障时序。它们是需要收敛的条件性风险/维护债务，不应与 #1、#3–#7 以同一实盘阻断等级处理。

### 执行账户同步

| # | 位置 | 问题 | 影响 | 现有缓解 |
|---|---|---|---|---|
| 9 | `execution_account/sync.py:616, 796-804` + `daemon.py:800-825, 919-953` | 延迟 persist 的 fill cursor 用旧快照推进，B 轮可能基于 C0 计算并回写，回退 `from_id` | 重复拉 fill 或（重叠窗口不足时）漏 fill | `_known_fill_keys` 去重；`_FILL_FETCH_OVERLAP_MS=60s` |
| 10 | `daemon.py:831-845, 694-732` | `_recover_pipeline` 过程中新触发的 recovery 信号被 `clear()` 吞掉 | 流仍可能 overflow，最迟等下一个 heartbeat/snapshot 周期（~30s） | 后续 overflow 再次抬升指标并触发 |
| 11 | `execution_account/hub.py:366-384, 507-517, 567-575` | 订阅者队列满时 `_enqueue_latest` 丢弃 bootstrap + replay，只保留最新 live 消息 | 可能触发 SequenceGap → 全量 snapshot recovery 风暴；这是当前的 fail-closed backpressure 策略，不等同于静默数据丢失 | 客户端 `_materialize_event` 检测不连续 |

### Live rollout

| # | 位置 | 问题 | 影响 | 现有缓解 |
|---|---|---|---|---|
| 12 | `apps/live_rollout/main.py:2031-2047` | `cancel_unfilled_live_entry_orders` 对未知 exchange 可见单直接 `client.cancel_order_by_client_id`，绕过 Coordinator，无 `CANCELING` 日志 | 与 submit/reconcile 并发时本地/交易所状态分叉 | 仅调度 flatten 窗口内使用 |
| 13 | `daemon.py:2241-2549, 3206-3409` | 入场授权后多个 await（telemetry、EMA、recorder）之间不复检 generation；账户事件可使 context 失效 | 可能多开一个 `max_open_positions`/`max_gross_exposure` 槽位 | `_pending_entry_plans` reservation 强；uncertain 状态 fail-closed |
| 14 | `apps/live_rollout/main.py:2873-2983` + `coordinator.py:120-137` | 关停时 cancel 卡在 submit 中途的任务；caller future 已取消但 `operation()` 仍可能跑完 | 订单可能落在 `SUBMITTING`/`ACKNOWLEDGED`；若进程在结果落盘前退出，则依赖下次启动 reconcile | 写前日志 + 下次启动 `_reconcile_run_orders`（`main.py:2050`） |
| 15 | `daemon.py:1228-1238, 1343-1418` | 调度 flatten 与在途入场 submit 交错：cancel 扫描后入场才落地 | flatten 窗口内短暂出现挂单，下一 tick 可能才撤掉；可能成交 | 下一 schedule 迭代 cancel；flatten 验证要求零仓位 |
| 16 | `binance/client.py:131-150` | **当前结论：不成立。** `aclose` 在单一 asyncio event loop 中无 await 地设置 `_closed`、取消 queued/active futures，并等待 worker 结束 | 未发现永久挂起路径；不列入修复计划 | 已有 `test_command_pacer_closes_pending_waiter` 覆盖 pending waiter 被取消 |

### 持久化

| # | 位置 | 问题 | 影响 | 现有缓解 |
|---|---|---|---|---|
| 17 | `paper_daemon_repository.py:362-438, 760-766` | 计数器 SELECT 后内存 `+=`，无 `FOR UPDATE` | 双写丢失 update（paper 路径） | 单线程 loop；假设每 `run_id` 单写者 |
| 18 | `paper_daemon_repository.py:289-298` | `initialize_run` SELECT-then-INSERT | 并发启动同一 `run_id` 时未映射的 `IntegrityError` | PK on `run_id` |
| 19 | checkpoint upsert（paper/live） | `on_conflict_do_update` 无 `saved_at` / version fencing | failover 双写 last-writer-wins，游标回退导致重复信号/跳 bucket | 进程内 `CheckpointWriter._write_lock` |
| 20 | `account_repository.py:121-169` | open orders DELETE-all + INSERT，无 snapshot fencing token | 双进程时陈旧快照可覆盖新快照 | 进程内 `_rest_sync_lock`；假设单 account-sync daemon |
| 21 | `account_repository.py:262-303` | `load_active_position_symbols` 多语句读，无 REPEATABLE READ | 混合 reconciliation 与 position 代际，universe 订阅可能不准 | 非 live 风险主路径（用 hub snapshot） |
| 22 | `runtime_state_repository.py:190-220` | `mark_incomplete` 只 UPDATE 已存在行 | 进程重启后 gap 丢失，残缺 15s state 被当成完整 | 同进程 FIFO actor；插入时 conflict check |
| 23 | `models.py:1032-1050` + `order_repository.py:395-414` | `exchange_fills` 无 `(client_order_id, exchange_trade_id)` 唯一约束 | 若 `fill_id` 非确定性，同一 trade 可双记 | 当前 Binance `_order_snapshot` 返回空 fills，live 路径很少命中 |
| 24 | `risk_repository.py:245-267` | `strategy_live_states` upsert 无 `changed_at` 守卫 | 多操作者控制写入时 last-writer-wins | 主要为运维路径；lease 限制交易写者 |

---

## 已核实为安全的模式（非竞态）

| 模式 | 位置 | 为何安全 |
|---|---|---|
| Coordinator 同 key get-or-create | `orders/coordinator.py:169, 294-301` | `_scheduler_lock` 保护 |
| Scheduler close 与 submit | `coordinator.py:104-118` | `_state_lock` + drain + sentinel |
| ClientOrderId 分配 | `orders/ids.py:8-14` | 确定性 `uuid5`，无共享计数器 |
| `prepare_submission` 并发 | `order_repository.py:193-234` | DB 唯一插入仲裁 |
| `append_order_event` 乱序/终态 | `order_repository.py:332-375` | SQL CASE + FILLED 优先 + `greatest` |
| Lease renew/release | `risk_repository.py:140-168` | `with_for_update` + owner 检查 |
| Active lease 唯一性 | `models.py:837-844` | 部分唯一索引 `state == 'active'` |
| Account fills 幂等 | `account_fill_events` PK | 含 `trade_id` |
| Rollback 命令幂等 | `idempotency_key` unique | 重复命令抑制 |
| 进程内同步 dict（state/quote cache） | `daemon.py` `_latest_market_states` 等 | asyncio 单线程，无 await 即原子 |
| `CheckpointWriter` 进程内合并 | `checkpoint_writer.py` | `_write_lock` + coalescing |
| Entry-filter / EMA 缓存 | market_data 侧 | 专用刷新循环写；读为同步 lookup |
| Paper `run_paired_paper_live_daemon` | `strategy_runner/daemon.py` | 单线程状态循环，非并发写者 |

---

## 修复计划（按依赖关系排序）

### Phase 0：先建立可复现的并发测试基线

目标：所有 P0/P1 修复先有能稳定制造交错的测试，避免只靠日志或代码注释验收。

- 为账户同步、订单 Coordinator、exit lane、lease、risk claim 增加 `asyncio.Event`/fake clock 控制点；
- 记录当前工作树对应的 commit SHA，并将本报告中的测试命令固定下来；
- 每个测试至少断言：交易所 POST/cancel 次数、`client_order_id`、durable state 转移、account-event/fill 是否可见。

### Phase 1：修复账户事件冻结窗口和交易所水位（#1、#2）

依赖：无。优先级最高。

- 引入有界事件缓冲和 REST 快照窗口；冻结期间保留事件，不再静默 return；
- REST 完成后按交易所事件时间、event time 和 event id 稳定重放事件；可用的 `u/pu` 水位不连续或回退时强制完整快照恢复；
- 在同一批测试中覆盖 `ACCOUNT_UPDATE`、带成交的 `ORDER_TRADE_UPDATE`、队列满和 recovery；
- 对没有交易所序列号的 Binance 事件使用 `E/T` 保守兜底；本地 `received_at` 不再作为唯一顺序证据。

状态：已落地并通过 REST 阻塞期间事件重放、交易所时间回退和可用更新水位断档回归测试。

### Phase 2：把订单提交和退出决策变成可仲裁的原子流程（#3、#4）

依赖：Phase 0；#4 可与 Phase 1 并行，但最终要使用稳定的账户/仓位水位。

- 为 Coordinator 增加等价的 `prepare_and_execute` per-key 原子工作流；prepare、durable state 和 exchange POST 已被同一调度操作包住，并由 POST 前 guard 做最后 fencing；
- reconcile 遇到刚创建的 `SUBMITTING` 可以短暂退让，但对 `UNKNOWN` 必须 fail-closed，不能用时间阈值替代状态仲裁；
- quote/candle/grace/recovery 已共享 symbol 级 exit-decision 锁；数据库 durable episode reservation 已让不同 candidate identity 在跨进程场景下也能仲裁；短 TTL 内存 set 仍只能作为额外防线；
- 用两个并发测试分别证明：prepare/reconcile 不会错误 POST 两次；quote/candle 同一 episode 只产生一张 reduce-only 单。

状态：同一 intent 的 prepare/reconcile 交错、同一 episode 的跨 worker reservation 竞争和终态释放均已有集成测试；UNKNOWN 的二次恢复仍需按生产 exchange 语义单独验收。

### Phase 3：补齐 context generation 和 lease/risk fencing（#5、#6、#7）

依赖：Phase 2 的 durable intent/claim 接口。

- `_load_context` 在 epoch/sequence 变化后重载或放弃本次决策；exit lane 在生成订单前再次验证 snapshot sequence；
- lease heartbeat 连续失败时 fail-closed；`RiskGateway` 校验 owner、account、strategy 和 lease id；`update_lease` 只接受同一 owner 的 lease 并使缓存失效；
- `prepare_submission` 事务内已重读并锁定 halt/lease；exchange POST 前再次验证 fencing token；
- 持久化 exposure claim 已落地：在同一事务中锁定 scope、重读 active claims 并检查 max positions/daily loss/gross exposure，订单终态释放 claim；token 失效时在途订单的细化 UNKNOWN/reconcile 规则仍需部署演练；
- 为 stale snapshot、lease 抢占、halt 在途变化、max positions/gross exposure/daily loss 并发分别补测试。

状态：旧 lease/context 无法通过当前提交门；active halt 会阻止后续新 POST；跨 writer 限额仲裁已完成新提交路径，但混合版本 worker 仍必须由发布编排禁止继续写单。

### Phase 4：收敛 MarketStateHub 的序列/replay 原子性（#8）

依赖：可独立实施，优先级低于账户和订单正确性。

- 已先 encode，再在 per-environment lock 内完成 sequence 分配、replay append 和扇出；
- 增加 encode 阻塞期间重连测试；Hub 和客户端均暴露慢消费者、队列溢出与 recovery 计数；
- 保持 fail-closed，不要为了避免 unavailable 而发送不连续的数据。

状态：序列分配/replay 原子性已落地并通过单测；慢消费者的 recovery 风暴指标仍属于后续运维验证。

### Phase 5：处理 P2 条件性风险和持久化 fencing（#9–#15、#17–#24）

依赖：前面几阶段稳定后，按实际部署拓扑决定是否提升优先级。

- #9：已使用单调 upsert/time fencing，避免延迟 persist 回退游标；
- #10/#11：已记录队列溢出、replay 请求、SequenceGap 和 snapshot recovery 次数；生产仍需设置告警阈值；
- #12：未知 exchange-visible entry 已先 adopt 到 durable order journal，再经 Coordinator/state machine cancel；
- #13：已补 risk await 后和 submission scheduler 内的 generation/entry gate 检查；
- #14–#15：Coordinator 会先阻断新入场并等待 in-flight operation，schedule gate 再执行撤单、flatten 和交易所归零确认；真实 SIGTERM/滚动切换以及 flatten 归零仍需发布演练；
- 行情 WebSocket 的本地 reader/dispatch 关停排空已落地；live submit/cancel/shutdown 交错仍需按部署语义单独验收；
- #17–#20、#22–#24：已对 paper/live checkpoint、open orders snapshot、runtime state、exchange fills 和 operator state 增加 version/timestamp fencing 或唯一约束；#21 已改为单语句一致性读；
- #16 不安排修复，除非未来支持跨线程调用 pacer，届时再明确同步模型。

验收：多 writer/failover 场景不会回退游标、覆盖新快照或重复记录同一 trade；单 writer 场景不因额外 fencing 破坏现有吞吐。

---

## 与 3 月评审的关系

| 3 月条目 | 状态 | 本报告 |
|---|---|---|
| P1 #4 Coordinator 创建竞态 | 已修复（`_scheduler_lock`） | 不再列出 |
| P1 #5 Scheduler close/submit | 已修复（drain + sentinel） | 不再列出；#14 是关停与 account lane 的另一层 |
| P1 #6 终态回退 | 已修复 | 不再列出 |
| P1 #7 `_seen_trade_ids` 无界 | 已修复（deque + set） | 不再列出 |
| P2 #22 ConnectionPool 无锁 | 已修复 | 不再列出 |
| P0 #1–#3 下单 unknown 语义 | 已修复 | 不再列出；#3 是 prepare/reconcile 窗口，是新问题 |
| 设计文档中的 fencing_token | 设计有、代码无 | 并入 #6/#7 |

---

## 验收建议

每项修复对应：

1. 可复现的交错单测（`asyncio.Event` / fake clock 制造窗口）；
2. 断言交易所 mock 只收到一次 POST / cancel；
3. 断言 durable state 的 UNKNOWN 只在明确的 ambiguous/reconcile 路径产生，且不会因此生成第二个 client_order_id；
4. 对 #1 及未来重放设计（#2）：覆盖 REST 窗口内的 fill 仍出现在 account-event hub；
5. 对 #4：quote 与 candle 并发只产生一张 reduce-only 单。

不以「字符串/注释存在」作为验收标准；与维修计划既有约定一致。
