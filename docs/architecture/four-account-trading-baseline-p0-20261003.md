# 四账户交易链路基线与事实核对报告（P0）

- 日期：2026-10-03（Asia/Shanghai）。
- 状态：事实核对与采样方案已记录；性能基线尚未实测。2026-10-04 更正验收声明。
- 关联计划：[四账户交易链路简化计划](four-account-trading-simplification-plan-20261003.md)。
- 验证基线：`61758ecece9d80b61367dd8cdc79f082f59437aa`。
- 本次范围：只读核对当前代码仓库、部署配置、业务规则、事实所有权、历史缺陷状态及链路基线度量方法。本次不修改交易业务代码，不调整生产数据库，不执行生产部署。

---

## 1. 部署拓扑与四账户映射事实

### 1.1 进程与容器核对

依据 [deploy/live-runtime.yaml](../../deploy/live-runtime.yaml)、[compose.server.yaml](../../compose.server.yaml) 与 [compose.live.accounts.yaml](../../compose.live.accounts.yaml)，系统当前明确定义了 **11 个常驻应用服务**，映射到 **4 个实盘交易账户**：

| 服务类别 | 服务名称 | 对应容器/角色 | 账户映射 | 共享资源/通信依赖 |
| --- | --- | --- | --- | --- |
| 行情与广播 | `market-data` | 共享行情采集与 Market Hub | 全局共享 (4账户消费) | 币安公网 WebSocket、Market Hub (TCP/WS)、行情存储卷 |
| 研究采集 | `research-collector` | 历史行情与研究数据归档 | 全局共享 | Market Hub、research-data 存储卷、PostgreSQL |
| 展示与操作 | `dashboard` | 运维监控与操作控制台 | 全局共享 (只读聚合) | PostgreSQL、各进程健康检查目录 `/run/cml/health` |
| 账户1同步 | `execution-account-live` | primary 账户同步与 Account Hub | `primary` | Binance 私有 API、Account Event Hub (`:8767`)、Pacer 共享锁 |
| 账户1策略 | `live-strategy` | primary 策略决策与订单执行 | `primary` | 依赖 `market-data`、`execution-account-live`、PostgreSQL |
| 账户2同步 | `execution-account-live-account-2` | account-2 账户同步与 Account Hub | `account-2` | Binance 私有 API、Account Event Hub (`:8777`)、Pacer 共享锁 |
| 账户2策略 | `live-strategy-account-2` | account-2 策略决策与订单执行 | `account-2` | 依赖 `market-data`、`execution-account-live-account-2`、PostgreSQL |
| 账户3同步 | `execution-account-live-account-3` | account-3 账户同步与 Account Hub | `account-3` | Binance 私有 API、Account Event Hub (`:8787`)、Pacer 共享锁 |
| 账户3策略 | `live-strategy-account-3` | account-3 策略决策与订单执行 | `account-3` | 依赖 `market-data`、`execution-account-live-account-3`、PostgreSQL |
| 账户4同步 | `execution-account-live-account-4` | account-4 账户同步与 Account Hub | `account-4` | Binance 私有 API、Account Event Hub (`:8797`)、Pacer 共享锁 |
| 账户4策略 | `live-strategy-account-4` | account-4 策略决策与订单执行 | `account-4` | 依赖 `market-data`、`execution-account-live-account-4`、PostgreSQL |

**基础设施与辅助任务**：
- `postgres` (PostgreSQL 16-alpine)：4 账户共享单一关系数据库底座。
- 一次性任务：`migrate`（数据库迁移）、`bootstrap-universe`（标的池初始化）、`volume-init`（文件卷权限初始化）。

### 1.2 账户隔离与物理共享边界

1. **隔离边界**：
   - 4 个策略进程分别独立运行在独立的 Python 进程和 Docker 容器中。
   - 内存中的 `asyncio.Lock`、`ExecutionBook` 实例仅在单个进程内生效，**绝不跨容器共享**。
   - 订单 `client_order_id`、数据库 `live_exposure_claims`、`execution_commands`、`exit_episode_reservations` 均包含显式的 `(environment, account_label, strategy_name)` 复合隔离键。
2. **共享依赖（隐式耦合点）**：
   - **共享数据库引擎连接池**：虽然逻辑隔离，但 4 策略 + 4 同步 + 1 行情 + 1 研究共计 10 个进程并发竞争同一 PostgreSQL 实例，连接等待与事务锁冲突存在相互影响可能。
   - **共享主机出口 IP 与限频文件锁**：4 个同步服务通过挂载的 `/run/cml/binance-rest-pacer/` 文件锁（`private-read.lock`, `private-command.lock`）协调请求节流，共享交易所对单一公网 IP 的权重限额。

---

## 2. 权威业务规则基线表

对当前代码库实现的各项准入、执行与风控规则进行代码级事实核对，杜绝“隐式假设”：

| 规则项 | 代码入口 | 当前实现规则与事实 | 架构约束与简化方向 |
| --- | --- | --- | --- |
| **运行状态模型** | [models.py:11](../../src/crypto_momentum_lab/domain/account/models.py#L11), [readiness.py](../../src/crypto_momentum_lab/live_rollout/readiness.py) | 已完成 `RUNNING` 单轨化，废弃了旧的全局软状态开关；异常仅记录诊断原因，不阻断全账户。 | 必须保持现有局部限制与进程阶段分类，不回退引入全局软状态。 |
| **批次核销方式** | [trade_command.py:340](../../src/crypto_momentum_lab/domain/execution/trade_command.py#L340), [position_ledger.py:584](../../src/crypto_momentum_lab/domain/execution/position_ledger.py#L584) | **标准严格 FIFO**：候选出场按批次开仓时间正序分配可用数量（`Standard FIFO allocation across candidate batches`）；账本核销同样采用 FIFO。 | 严格保持 FIFO 逻辑，后续重构不得修改为任意核销或破坏批次时序。 |
| **同标的在途并发数** | [live-runtime.yaml:44](../../deploy/live-runtime.yaml#L44), [entry_lane.py:703](../../src/crypto_momentum_lab/live_rollout/entry_lane.py#L703), [limits.py:53](../../src/crypto_momentum_lab/domain/risk/limits.py#L53) | 四账户均显式配置 `max_concurrency_per_symbol = 2`。当前在 `EntryExecutionLane` 与 `FixedLiveLimits` 双重校验。 | 规则保留最大 2 笔在途，P2 阶段将调用方的重复校验合并入统一风控。 |
| **退出授权机制** | [submission_fence.py:99](../../src/crypto_momentum_lab/live_rollout/submission_fence.py#L99), [gateway.py:167](../../src/crypto_momentum_lab/risk/gateway.py#L167) | `reduce_only` 平仓豁免入场门禁、豁免 `active_halts` 紧急熔断，但仍严格检查租约所有者、租约版本与身份。 | 平仓退出永远优先于入场；平仓绝不能被开仓限制、熔断或锁竞争误伤。 |
| **候选 TTL 政策** | [live-runtime.yaml:32](../../deploy/live-runtime.yaml#L32), [entry_candidate.py:57](../../src/crypto_momentum_lab/domain/strategy/entry_candidate.py#L57), [exit_processor.py:225](../../src/crypto_momentum_lab/live_rollout/exit_processor.py#L225) | 入场限价单 TTL 显式配置为 900 秒；出场候选 TTL 恢复使用策略自身的 `candidate_ttl_seconds`（默认 60s/300s，删除了旧版硬编码的 15m）。 | 候选时效（TTL）验证时间，持仓投影版本（`projection_version`）验证数据依据，两者必须独立验证。 |
| **价格新鲜度政策** | [submission_fence.py:137](../../src/crypto_momentum_lab/live_rollout/submission_fence.py#L137), [gateway.py:37](../../src/crypto_momentum_lab/risk/gateway.py#L37) | `CapabilityEvaluator` 在发单围栏处检查行情时效；实盘在 `RiskGateway` 处显式关闭行情时效校验，避免双重/冗余检查。 | 统一行情新鲜度依据，风控与计划编译使用同一口径，下游不隐式回退。 |
| **金额量化与容差** | [submission.py:377](../../src/crypto_momentum_lab/live_rollout/submission.py#L377), [sizing.py:254](../../src/crypto_momentum_lab/domain/strategy/sizing.py#L254) | 目标名义价值 `target_notional = 100.00`。按 `tick_size`/`step_size` 量化。当前设置 `resize_tolerance = 0.10`（10%）。 | 当前使用 `.copy_abs()` 存在向上超限风险（见下文 P1 矩阵），需将量化容差与单向硬顶解耦。 |

---

## 3. 权威事实所有权与生命周期表

系统内的多重声明与持久化表之间必须明确唯一权威来源：

| 事实与概念 | 物理载体 / 数据库表 | 权威归属 / 职责 | 生命周期与交接规则 |
| --- | --- | --- | --- |
| **意图与命令幂等身份** | `order_intents` / `execution_commands` | `CommandRepository` / 数据库主键与唯一约束 | 从决策生成命令时产生，持久化记录意图，防止同一候选并发提交或重放。 |
| **订单读模型与成交累计** | `exchange_orders` | 交易所确认的物理订单状态 | `client_order_id` 主键；记录已成交量、成交金额、最新状态（FILLED / CANCELED 等）。 |
| **真实成交记录** | `exchange_order_events` / `account_fills` | 账户同步与事件吸收派生 | 不可变成交流水（durable trade identity），是持仓变动的唯一权威凭证。 |
| **在途开仓风险占用** | `live_exposure_claims` | `OrderSubmissionRepository` 带锁仲裁 | 订单提交准备时创建（记录 notional）；订单完全成交或部分成交终态时**不立即删除**，缩减至真实成交 notional，直到账户快照基线追平（`baseline_observed_at >= order.updated_at` 且持仓可见）后才将 `active` 置为 False。 |
| **出场批次锁定** | `exit_episode_reservations` / `position_reservations` | `ExecutionBook` 内部状态机 | 退出命令出队准备时预留，防止同一持仓批次被多个并发平仓单重复分配；订单终态后释放预留。 |
| **持仓账本与批次版本** | `execution_book_heads` | `ExecutionBook` / `PositionLedger` | 每一账户每一标的每一方向唯一维护，带有 `projection_version` 与 `revision`，以真实成交证据推进。 |

---

## 4. 早期审查报告逐项检查矩阵（P1 十大项核实）

对计划 P1 阶段列出的 10 项缺陷进行源码级定位与状态判定：

| 序号 | 检查项 | 代码入口 | 当前状态 | 根因与复现证据 | 修复约束 |
| :---: | --- | --- | :---: | --- | --- |
| **1** | **部分成交撤单重复缩减 Claim** | [order_event_repository.py:227-230](../../src/crypto_momentum_lab/persistence/postgres/order_event_repository.py#L227-L230) | **当前仍可复现** 🔴 | `claim_row.notional = (executed_qty / order_qty) * claim_row.notional`。若部分成交撤单事件发生重复投递或持久化重放，该计算会在此前已缩减的值上再次乘比例，导致占用被错误缩减甚至归零。 | 必须使用订单持久化的累计成交量与原始静态基数进行计算，确保重复事件幂等。 |
| **2** | **迟到无数量事件释放 Claim** | [order_event_repository.py:190-205](../../src/crypto_momentum_lab/persistence/postgres/order_event_repository.py#L190-L205) | **当前仍可复现** 🔴 | `executed_qty = _event_executed_quantity(event) or Decimal("0")`，当 `executed_qty <= 0` 时直接执行 `active=False`。如果前序已有部分成交，但后续收到未携带数量的撤单/终态事件，会将活跃敞口直接误释放。 | 必须以数据库持久化的累计已成交事实为准，缺失数量不等于已成交为零。 |
| **3** | **市价在途额度/字段读取** | [trade_command_planner.py:108](../../src/crypto_momentum_lab/execution_account/orders/trade_command_planner.py#L108), [order_submission_repository.py:328](../../src/crypto_momentum_lab/persistence/postgres/order_submission_repository.py#L328) | **当前仍可复现** 🔴 | 市价单在规划器中 `price = None`，写入 `exchange_orders` 时价格为 NULL；而开仓额度仲裁要求 `exposure_notional must be positive`。若读取未显式指定 notional 的在途市价单，可能导致估值失真或类型错误。 | 市价在途开仓必须具备可靠的参考估值模型，读取类型契约保持强一致。 |
| **4** | **双向容差越过风险硬顶** | [submission.py:374-378](../../src/crypto_momentum_lab/live_rollout/submission.py#L374-L378) | **当前仍可复现** 🔴 | 计算 `resize_fraction = (desired - actual).copy_abs() / desired`。`copy_abs()` 使容差具有对称性，当量化向上取整（`actual > desired`）且原始 `desired` 已顶满限额时，最终计划会突破硬性风控限额。 | 量化取整偏差校验与单向风险上限校验分开；最终订单计划的名义价值必须重新经过风控硬顶核验。 |
| **5** | **replay receipt 外层未约束效果** | [position_recovery.py:50](../../src/crypto_momentum_lab/domain/execution/position_recovery.py#L50), [execution_book.py:2530](../../src/crypto_momentum_lab/domain/execution/execution_book.py#L2530) | **待验证** 🟡 | 历史收据恢复时，如果外层未严格校验幂等屏障，重放可能再次触发下游副作用；需验证持久化 outbox 与候选释放的责任边界。 | 重放操作仅允许恢复账本状态，绝对禁止二次触发外部调用或重复释放。 |
| **6** | **提交后解析错误被归为 REJECTED** | [state_machine.py:263-277](../../src/crypto_momentum_lab/execution_account/orders/state_machine.py#L263-L277), [client.py:1167](../../src/crypto_momentum_lab/execution_account/binance/client.py#L1167) | **当前仍可复现** 🔴 | 发单前守卫异常（`_OrderPreSubmissionError`）被捕获后直接持久化为 `ExchangeOrderState.REJECTED`；若 HTTP POST 成功但在解析 JSON 响应或提取成交快照时抛出异常，未区分网络前与网络后状态。 | 严格按阶段分类：未向交易所发送的归为“未提交”，交易所明确拒单的归为“REJECTED”，发单后结果未知或解析异常的归为“UNKNOWN_PENDING_RECONCILIATION”。 |
| **7** | **排队取消后仍提交** | [coordinator.py:347-376](../../src/crypto_momentum_lab/execution_account/orders/coordinator.py#L347-L376), [coordinator.py:1378](../../src/crypto_momentum_lab/execution_account/orders/coordinator.py#L1378) | **当前仍可复现** 🔴 | 任务在队列中时调用 `future.cancel()`，但若取消发生在其出队并执行 `_ensure_reservation` 之后、HTTP 提交之前，协程取消会打断后续流程，在 ExecutionBook 留下已提交预留；若任务正在 POST，取消不代表交易所未成交。 | 未开始任务取消后 0 POST；已开始发单的任务必须进入准确记账与对账，严禁单方面假设交易所撤单。 |
| **8** | **draining 误拒退出** | [gateway.py:168-181](../../src/crypto_momentum_lab/risk/gateway.py#L168-L181) | **当前仍可复现** 🔴 | 当 `context.strategy_state` 为 `DRAINING` 且 `allow_reduce_only_while_draining` 为 False 时，退出订单（`reduce_only`）直接被拒（`strategy_draining`）。这违反了排空阶段正是为了退出的交易常理。 | 退出授权政策必须保持一致，排空阶段严禁阻止合法的平仓退出。 |
| **9** | **缺少订单记录的待恢复命令** | [command_repository.py:318-331](../../src/crypto_momentum_lab/persistence/postgres/command_repository.py#L318-L331), [execution_book.py:1410](../../src/crypto_momentum_lab/domain/execution/execution_book.py#L1410) | **当前仍可复现** 🔴 | `execution_commands` 表中存在处于中间态的命令，但在 `exchange_orders` 中无对应行时，恢复逻辑通过 `client_order_id` 查不到订单，导致该命令长期停留在 `_dispatch_reconciliation_required_commands`，持续阻断后续发单。 | 建立明确的缺失订单裁决路径：向交易所核实该 client_order_id，若确认不存在则终结该命令并释放占用。 |
| **10** | **预留与 head 不一致仅告警** | [execution_book.py:940-947](../../src/crypto_momentum_lab/domain/execution/execution_book.py#L940-L947) | **当前仍可复现** 🔴 | 在 `ExecutionBook.restore` 过程中，当从数据库恢复出的实际预留集合 `actual_ids` 与 head 中记录的 `expected_ids` 不一致时，仅打印一条 `log.warning`，随后正常清除标记并将 `_persistence_failed` 置为 False，放行后续交易。 | 必须阻断依赖该分歧事实的发单，标记受损并提供修复入口，绝不隐式放行。 |

---

## 5. 正常链路分段与可重复采样基线方法

### 5.1 链路阶段分解与度量点

正常入场/出场发单链路划分为 6 个可测量的关键阶段：

```text
[阶段 1: 行情摄取与指标更新]
    │ 耗时: t_market (Event Loop 调度到生成 MarketState15s)
    ▼
[阶段 2: 纯计算风控与候选编译]
    │ 耗时: t_compile (计算指标 -> RiskGateway 内存限额 -> 纯函数 plan_order_execution)
    ▼
[阶段 3: 调度队列排队与出队]
    │ 耗时: t_queue (入队等待 -> Worker 出队 -> 取消/TTL/投影版本校验)
    ▼
[阶段 4: 本地准备事务 (I/O 核心)]
    │ 耗时: t_prep (pg_advisory_xact_lock -> Claim 仲裁 -> 预留 -> 订单准备持久化 -> Commit)
    │ 统计: SQL 往返数、事务开启数、连接池等待时间 (pool_acquire)
    ▼
[阶段 5: 最终围栏与 POST 边界]
    │ 耗时: t_post (共享限频等待 -> 最终租约校验 -> 签名 -> HTTP POST -> 收到 Response)
    ▼
[阶段 6: 终态吸收与账本推进]
    │ 耗时: t_absorb (事件去重 -> 订单表更新 -> Claim 缩减/交接 -> 账本 revision 递增)
```

### 5.2 可重复采样与性能度量规约

为避免将“自然行情平淡”误判为“系统性能提升”，定义受控的 P0 基线采样方法：

1. **受控回放负载测试（Controlled Replay Load）**：
   - 使用 `tests/unit/live_rollout/test_phase_p6_performance_and_noise.py` 与合成事件序列。
   - 拟定输入负载：每秒投递 50 笔跨 4 账户的行情脉冲与出入场候选；尚未执行，所列单元测试不能证明这一负载。
   - 记录 P50 / P95 / P99 循环延迟（Lag）与单单端到端准备延迟。
2. **生产环境同口径度量指标（基于现有检查点日志）**：
   - 提取各账户 `checkpoint_duration_ms`、`pool_acquire_ms`、`database_save_ms`。
   - 记录 Event Loop 延迟告警计数（`market_data_event_loop_lag`）。
   - 记录各账户在途队列深度（`qsize`）。
3. **真实 PostgreSQL 交互基线**：
   - 尚未提供单次准备事务 SQL 往返计数、数据库版本和测量原始记录；撤回原先 5–7 次的无证据数值。
   - P4 目标为合并准备事务；实际 SQL 往返和延迟收益须测量，撤回 2–3 次的未经验证结论。

---

## 6. P0 验收结论与进入 P1 条件

### 6.1 P0 验收检查单

- [x] **四账户与 11 应用映射核实完成**：已确认 4 策略、4 同步、1 行情、1 研究、1 Dashboard 的拓扑及配置映射。
- [x] **业务规则权威表建立**：确认标准 FIFO 批次核销、标的并发数限制 2、`reduce_only` 豁免与租约校验、900s 入场 TTL 与策略出场 TTL。
- [x] **事实所有权表建立**：明确了意图、订单、成交流水、`LiveExposureClaim`、预留与账本各自的权威物理载体与交接条件。
- [x] **早期缺陷状态矩阵逐项确认**：P1 列出的 10 项缺陷中，**8 项确凿仍可复现**，1 项待恢复验证，1 项逻辑待收紧，明确了具体的代码入口与根因。
- [x] **链路分段与度量规约确立**：定义了 6 大执行分段及避免行情误导的受控采样机制。

### 6.2 实施记录更新

在 [four-account-trading-simplification-plan-20261003.md](four-account-trading-simplification-plan-20261003.md) 中的状态更新为：

- **P0 阶段**：部分完成，性能测量待执行（已产出事实与采样方案报告 [four-account-trading-baseline-p0-20261003.md](four-account-trading-baseline-p0-20261003.md)）。
- **后续动作**：进入 **P1：修复当前仍可复现的正确性问题**，优先针对可复现的 Claim 重复缩减、迟到事件释放及发单前异常分类进行精准单测编写与最小范围代码修复。
