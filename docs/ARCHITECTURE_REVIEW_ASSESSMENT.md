# 对 `ARCHITECTURE_REVIEW.md` 的复核意见（初版复核记录）

- **复核日期**：2026-09-04
- **复核对象**：[`docs/ARCHITECTURE_REVIEW.md`](ARCHITECTURE_REVIEW.md)
- **代码基线**：`ef85b97`（2026-09-03，`feat: add research market-state collector`）
- **工作树状态**：dirty；本复核只读，不把已有修改归因于本次工作

> 本文前半部分记录对初版评审的复核。Gemini 根据这份意见更新评审后，判断依次见文末“对修订版的二次复核”、“对定稿版的第三次复核”和“对最终核准版的第四次复核”；最新一节覆盖此前结论，但保留前文作为审计记录。

## 结论

这份评审的**问题感知是对的，当前状态描述和改造路线不够可靠**。

它准确抓住了两个真实问题：

1. `live_rollout` 的编排模块过重，外部依赖和生命周期状态过多；
2. Paper 与 Live 并非端到端同一条执行管道，尤其是过滤、数据源、退出和持久化路径仍有差异。

但报告把若干“历史观察”写成了“已经确认的当前根因”，并且漏掉了 8 月 23 日以后已经落地的改动。因此它适合做讨论起点，不适合原样作为实施计划。最需要修正的三处是：

- 不应把 500 秒延迟直接归因于同步数据库等待；
- 不应把下单前的订单日志事务改成普通异步队列；
- 不应未经基准测试就用 Redis Pub/Sub 替换现有 Hub。

## 为什么说报告有时效性问题

报告引用的交接材料把 `767a281`（8 月 22 日）作为基线，而当前 HEAD 已经是 9 月 3 日。其间已经有 checkpoint 解耦、运行态分区、Hub 读写解耦、账户快照恢复、限价单和退出恢复等提交。

此外，报告称当前实盘是“Top100 EMA”，但当前 Compose 默认值是 Top10，并显式关闭 EMA5/EMA10：[`compose.server.yaml`](../compose.server.yaml:540)。这是策略配置事实，不是架构解释上的小偏差。

## 逐项判断

| 报告判断 | 结论 | 复核依据 |
|---|---|---|
| `main.py` / `daemon.py` 是上帝模块 | **基本正确** | 当前文件约 3314 / 2957 行，`LiveStrategyDaemon` 仍同时处理市场循环、候选执行、退出工作、恢复和遥测。 |
| 研究、纸盘、实盘严格同构 | **只在策略核心层成立** | Live 和 Paper 都通过 registry 构造同一个 runtime strategy；但 Paper 从 PostgreSQL 轮询，Live 默认从 Hub 消费，且过滤、退出、订单持久化不同。见 [`strategy_runner/main.py`](../src/crypto_momentum_lab/apps/strategy_runner/main.py:720) 和 [`live_rollout/main.py`](../src/crypto_momentum_lab/apps/live_rollout/main.py:1513)。 |
| Paper/Live 双轨会造成对账困难 | **方向正确，但证据不能这样解释** | 过去确有 `paper 87 / live 69`，但交接材料明确说这个窗口跨越重启、持仓和运行状态差异，不能直接当成 20 笔漏单。[`HANDOFF.md`](HANDOFF.md:34) |
| 500 秒延迟根因是同步数据库 I/O | **未证实，且当前实现已改善** | 交接材料仍要求用阶段时间戳定位；当前上下文有 30 秒缓存、并发查询和一状态预取。[`postgres_runtime.py`](../src/crypto_momentum_lab/live_rollout/postgres_runtime.py:70) [`postgres_runtime.py`](../src/crypto_momentum_lab/live_rollout/postgres_runtime.py:240) [`daemon.py`](../src/crypto_momentum_lab/live_rollout/daemon.py:924) |
| EMA 预热是主事件循环里的 CPU 密集计算 | **判断不准确** | EMA 预热由后台 cache 执行，实际包含 Binance REST candle 读取，并通过 `asyncio.to_thread` 运行；2.2–10.4 秒首先应拆成 REST、缓存锁、解析和 EMA 计算再判断。 [`entry_cache.py`](../src/crypto_momentum_lab/live_rollout/entry_cache.py:362) [`candle_source.py`](../src/crypto_momentum_lab/strategy_runner/candle_source.py:43) |
| PostgreSQL 存在时序/OLTP 写放大 | **历史问题真实，但报告漏掉已落地修复** | 策略 checkpoint 已压缩为控制状态并由后台 writer 写入；运行态市场表已有分区保留实现。[`checkpoint_writer.py`](../src/crypto_momentum_lab/live_rollout/checkpoint_writer.py:39) [`runtime_state_partitions.py`](../src/crypto_momentum_lab/persistence/postgres/runtime_state_partitions.py:1) |
| 三个自研 Hub 的 ACK/分片是同一类 IPC 问题 | **概念混淆** | ACK 和分片的主要复杂度来自 Binance 上游行情连接；本地 Hub 还提供序列、epoch、重放和缺口 fail-closed 语义。 [`HANDOFF.md`](HANDOFF.md:20) [`market-state-hub.md`](../docs/runbooks/market-state-hub.md:45) |
| 进入/退出物理通道完全隔离 | **应改称逻辑隔离** | 当前是同一 Live 进程中的独立任务、队列和优先级；共享 Python 事件循环、数据库和执行协调器，并非物理独立进程。 |
| 所有持久化都应异步化 | **需要区分数据类型** | telemetry、signal、checkpoint 已有有界后台队列；但订单日志在 REST 前原子写入 intent、planned order 和 `SUBMITTING`，是崩溃恢复的安全边界。[`order_repository.py`](../src/crypto_momentum_lab/persistence/postgres/order_repository.py:82) |
| 改造后 Tick-to-Trade 可稳定低于 20ms | **没有证据** | 当前项目设计明确不追求亚秒竞速；应根据当前策略 TTL、行情新鲜度和生产 p95 设 SLO，而不是先写一个未经测量的数字。 |

## 对报告优点的保留

### 1. 单机基础设施选择是合理的

原始架构设计明确选择 PostgreSQL 控制面、原始压缩文件和 Parquet 研究面，并明确“不需要外部消息 broker”。在单机和有限内存条件下，这个约束仍然成立。引入 Redis、Dragonfly 或 Kafka 不能自动提高可靠性，反而会增加一个需要监控、备份和故障恢复的进程。

### 2. 租约、幂等订单和退出优先级设计是强项

有限 TTL 租约、确定性 `client_order_id`、超时后按 client id 查询，以及 reduce-only 优先级，都是值得保留的安全设计。需要注意措辞：它们降低 split-brain 和重复下单风险，但不能把“任何故障下绝不会出错”当作可证明性质。

### 3. 可靠性 Hub 的语义比“传输协议”更重要

当前 Hub 不只是发送 JSON：它维护有界队列、序列连续性、stream epoch、有限重放窗口、全量快照和缺口后的 fail-closed。删除 WebSocket 传输后，这些行为仍然必须存在；这说明真正的 seam 是“状态流接口”，而不是简单替换成 Redis Pub/Sub。

## 对建议路线的修正

### P0：先做当前版本的阶段级延迟审计

报告要求补齐阶段遥测，这个方向正确；但当前代码已经有 `market_state_received`、`context_ready`、`gate_evaluated`、`candidate_accepted`、`risk_approved`、`intent_saved`、交易所请求和账户成交等阶段。下一步应先按当前 HEAD 统计：

- `market_state_received -> context_ready`；
- `context_ready -> candidate_accepted`；
- `candidate_accepted -> intent_saved`；
- `intent_saved -> exchange_request_started`；
- `exchange_request_started -> exchange_response_received`；
- 按 entry、account-exit、quote-exit 分 lane 统计 p50/p95/max。

只有确认瓶颈后，才决定是批量上下文读取、缓存失效策略、事件循环阻塞、REST 竞争还是数据库写入。

### P1：共享“决策政策”，不要强行共享整条 daemon

策略 runtime 已经共享，真正重复的是外围政策：long-only、TopN universe、EMA 资格、候选拒绝原因和某些 Paper/Live 的时间语义。更合适的 seam 是一个小而纯的 `EntryEligibilityPolicy`（名称可调整），输入标准化 signal 与内存快照，输出资格和拒绝原因。

Paper 与 Live 仍可各自保留撮合、退出、风险门禁和持久化流程，并增加相同输入下的 contract test。这样能消除决策漂移，而不会把两个不同的执行环境塞进一个更大的上帝模块。

### P1：收窄 Live 编排接口，而不是只按文件切类

报告建议的 `LiveLeaseWatchdog`、`LiveContextCache`、`LiveExecutionBridge` 和退出协调器，在当前代码中已经分别对应 `LiveLeaseHeartbeat`、`PostgresLiveContextProvider`/`LiveEntryFilterCache`、`OrderExecutionCoordinator`/state machine 和 `_ExitExecutionLane`。再次切分前应先减少 `LiveStrategyDaemon` 的依赖参数和状态耦合。

`main.py` 更像一个过大的 composition root：CLI、启动、资源构造、后台任务监督混在一起。优先抽出“运行配置解析”和“进程监督器”，保留小而清晰的组装入口；不要把每个现有方法机械搬到新类里。

### P1：保留订单日志的同步安全边界

可以把市场状态消费、策略计算、遥测、signal 记录、checkpoint 和普通 reconcile 做成非阻塞或后台任务；但执行交易前至少要保留一个有界、可观测、失败即 fail-closed 的持久化步骤：

```text
risk approved
  -> durable intent + planned order + SUBMITTING
  -> Binance REST
  -> exchange/account reconciliation
```

如果未来要把这一步移到别的存储或进程，必须先证明它仍然覆盖“提交前崩溃”和“提交响应丢失”两个恢复窗口。

### P2：暂不把 Redis Pub/Sub 作为默认方案

只有在生产指标证明当前 Hub 的维护成本或故障率已经超过可接受范围时，才值得做传输层替换评估。评估对象至少应包括：

- 序列和 epoch 如何生成；
- 重连后如何恢复全量快照或有限重放；
- 消费者落后时是丢旧值、阻塞还是 fail-closed；
- Redis 重启和网络分区时如何恢复；
- 上游 Binance ACK/分片问题是否仍然存在。

如果这些语义需要在 Redis 客户端重新实现，那么“换 Redis 降复杂度”的收益就不能按删除 WebSocket 代码行数估计。

### P2：CPU 与 PostgreSQL 优化都应先测再改

EMA 任务先拆分 REST 等待和本地计算时间；只有本地计算占主导时才考虑进程池。PostgreSQL 方面，紧凑 checkpoint、独立 checkpoint pool 和运行态分区已经是正确方向，后续应根据 WAL、backend-written buffers、表膨胀和订单生命周期延迟的同一时间窗口数据继续调整。

## 建议保留的最终路线图

1. **观测**：用当前 HEAD 重新测阶段延迟和事件循环 lag，建立生产 SLO。
2. **收敛决策**：共享纯 entry policy、过滤上下文和拒绝原因，补 Paper/Live contract tests。
3. **收窄编排**：拆 composition root 和 supervisor，减少 `LiveStrategyDaemon` 的外部接口与状态耦合。
4. **守住执行安全边界**：订单日志先于 REST，异步化只用于可降级数据。
5. **按指标演进传输**：只有当前 Hub 的可靠性语义已经有替代设计和基准结果，才评估 Redis Streams、UDS 或其他实现。
6. **继续验证数据库写形状**：观察 checkpoint payload、分区淘汰和订单路径，不以“checkpoint 日志很慢”单独推断交易延迟根因。

## 复核验证

本次没有修改原始评审或业务代码。针对 checkpoint、entry cache、Live context、Live daemon、两个行情 Hub、账户 Hub 和运行态分区的现有测试结果为：

```text
95 passed, 4 skipped
```

4 个跳过项是当前环境不允许本地 loopback socket，不是断言失败。

## 对修订版的二次复核（2026-09-04）

### 总评

修订版已经真正吸收了上次复核，而不只是改几个词：它承认延迟根因尚未证实，保留下单前同步持久化的安全边界，放弃把 Redis Pub/Sub 当成默认替代品，并把“严格同构”收窄为策略核心和准入政策的同构。

截至当前代码基线，我给修订版的评价是：**问题判断和演进方向约 8.5/10，可以作为架构纲领；仍不宜直接当作逐项实施设计。**

### 已正确吸收的关键修正

- 将历史 500 秒延迟改成待阶段级遥测确认的问题，不再把数据库 I/O 写成确定根因；
- 明确订单意图、计划订单和 `SUBMITTING` 必须在 Binance REST 之前持久化；
- 将 Paper/Live 的共享范围限定在 Runtime Strategy 和纯准入 Policy，而不是合并两个宿主 daemon；
- 将进入/退出描述为逻辑 lane，并保留现有 Hub 的序列、epoch、重放和 fail-closed 语义；
- 接受 checkpoint 后台写入和运行态表分区已经落地，不再把它们列为未完成的 P0 改造。

### 修订版仍需更正的事实和术语

1. **拓扑图仍然错误。** 图中保留了 `MSH --> PAPER`，但当前 Paper 从 PostgreSQL 轮询运行态，并通过 `PostgresPaperDaemonRepository` 写回 PostgreSQL。应改成 `PG <--> PAPER`，或明确区分未来的 Hub paper subscriber 与当前生产 Compose。

2. **Paper 持久化描述仍不准确。** “本地 JSON / 离线报表或批量持久化”会误导读者；当前 paper-live daemon 的决策、成交、组合和 checkpoint 都有 PostgreSQL repository 路径。

3. **Outbox 的因果叙述仍缺少证据。** 当前实现确实绕过了原设计中的跨进程 Outbox 执行 seam，但仓库没有基准数据证明“Outbox 轮询无法满足延迟”是转向进程内执行的已确认根因。建议写成架构选择及其权衡，而不是性能事实。

4. **EMA 任务不应称作高并发 HTTP。** 预取任务使用 `asyncio.to_thread`，但 provider 内部的锁会保护可变 HTTP/cache 实现，实际请求并非无限并发。应拆分测量 REST 等待、响应解析、EMA 计算和线程/GIL 影响。

5. **`healthcheck_fast` 的描述要收窄。** 它仍由 Docker 启动新的 Python 解释器并建立数据库连接；收益来自更轻的 psycopg 路径和更低的探测频率，而不是“避免启动 Python”。

6. **“订单 WAL”不是合适的名称。** PostgreSQL WAL 是存储实现细节。架构需要保护的是“下单前持久化日志/事务屏障”，即 `durable intent + planned order + SUBMITTING -> REST` 的顺序。

7. **模块拆分建议还要经过 seam 检验。** 当前 `LiveStrategyDaemon` 已经接收抽象的 `context_provider`、repository 和 execution port；真正的问题是依赖包和状态耦合，而不是完全没有抽象。`LiveProcessSupervisor` 可能形成深模块，但 `LiveRolloutConfigParser` 可能只是浅层转发，应先设计小接口再决定是否新建类。

8. **“决策逻辑 100% 对齐”仍然过强。** 能够保证的是：在相同的规范化 signal、universe、EMA 和时间快照下，纯 Policy 输出一致；真实 Paper/Live 的数据到达时间、账户状态和风险上下文不同，不能承诺端到端 100% 对齐。

### 当前应保留的路线图

1. 用当前部署 commit 的阶段遥测建立 `market_received -> context_ready -> intent_saved -> exchange_request -> fill` 的 p50/p95/max 基线；
2. 共享纯 `EntryEligibilityPolicy`、过滤上下文和拒绝原因，并用 contract tests 验证相同输入下的结果；
3. 先收窄 composition root 和 `LiveStrategyDaemon` 的外部接口，再做文件/类拆分；
4. 保留下单前的 durable journal，只有 telemetry、signal、checkpoint 等可降级数据走异步队列；
5. 只有在指标证明 Hub 的可靠性或维护成本不可接受后，才评估 Redis Streams、UDS 等传输替代方案；
6. 持续按同一时间窗口观察 PostgreSQL checkpoint payload、分区淘汰、WAL 和订单生命周期延迟。

### 二次复核结论

修订版已经从“带有若干过强断言的架构诊断”提升为“基本可执行的架构方向”。在修正拓扑图、Paper 持久化、Outbox 因果和上述术语后，可以把它作为后续设计讨论的基线；在此之前，不建议根据其中的数字目标或类名直接开工。

## 对定稿版的第三次复核（2026-09-04）

### 总评

这次“定稿版”已经吸收了上一轮列出的主要事实修正：拓扑图和 Paper 的 PostgreSQL 路径已校准，Outbox 不再被写成已证实的性能根因，EMA/healthcheck/WAL 的表述也明显收窄，模块拆分增加了 seam 检验，Paper/Live 的“一致性”被限制在规范化输入下的 Policy 输出。

按架构评审和 deep-module/seam 两个维度，我会给它 **9/10（作为评审）**；若把它当作可以逐项照抄的实施设计，则仍约 **8/10**。路线已经可靠，但“定稿”还需要补齐下面几项边界，否则实施时仍可能把历史现象或设计愿望误当成当前事实。

### 第三次复核仍需修正的事项

1. **认证交易边界的变更必须正式记录为架构决策。** 原设计规定策略不能直接调用 Binance 交易 API，且 `execution-account` 是认证连接的唯一持有者（[`设计规范`](superpowers/specs/2026-06-14-project-architecture-design.md#L60)、[`认证连接职责`](superpowers/specs/2026-06-14-project-architecture-design.md#L232)）；当前实现则把交易密钥同时注入 `execution-account-live` 和 `live-strategy`（[`compose.server.yaml`](../compose.server.yaml#L434)、[`compose.server.yaml`](../compose.server.yaml#L496)），由后者直接持有交易客户端。报告把它称为务实权衡是事实描述，但还缺一份 ADR/规范修订，明确新的凭证最小权限、租约、fail-closed、对账和密钥轮换契约。否则旧设计与当前运行态会继续冲突。

2. **P0 遥测计划的 lane 维度还没有和实现对齐。** 报告要求按 `entry`、`account-exit`、`quote-exit` 统计（[`定稿版第 184 行`](ARCHITECTURE_REVIEW.md#L184)），但当前 telemetry 只有 `entry`、`exit`、`unknown` 三个 lane（[`telemetry.py`](../src/crypto_momentum_lab/live_rollout/telemetry.py#L30)），而 `context_ready` 也固定记录在 entry lane（[`context_ready 实现`](../src/crypto_momentum_lab/live_rollout/telemetry.py#L356)）。要实现报告中的三路 p50/p95/max，必须先增加退出触发源、source-received 和 context-ready 时间戳；否则应把计划收窄为现有 `entry/exit` 维度。

3. **当前生产配置和历史 EMA 观测仍有混用。** 报告把“刷新 100 个标的、峰值 2.2～10.4 秒”列为现行痛点（[`定稿版第 163 行`](ARCHITECTURE_REVIEW.md#L163)），但当前 Compose 显式传入 `--no-entry-price-above-ema5` 与 `--no-entry-price-above-ema10`（[`compose.server.yaml`](../compose.server.yaml#L540)）；代码只有在 EMA provider 与 symbol loader 同时存在时才启用 `LiveEntryFilterCache`（[`main.py`](../src/crypto_momentum_lab/apps/live_rollout/main.py#L1638)）。因此这组耗时应标为“历史 Top100+EMA 部署观测”，不能直接当作当前 Top10/no-EMA 的瓶颈基线。

4. **退出拓扑仍写得过窄，隔离程度也不应绝对化。** 报告称平仓由 8767 账户事件独立调度（[`定稿版第 100 行`](ARCHITECTURE_REVIEW.md#L100)），实际还有 market-state、quote、闭合 15m candle 和 grace-timeout 入口；现有 `_ExitExecutionLane` 也明确维护 account、market、quote 队列（[`daemon.py`](../src/crypto_momentum_lab/live_rollout/daemon.py#L277)），并暴露账户、报价和闭合 K 线处理方法（[`daemon.py`](../src/crypto_momentum_lab/live_rollout/daemon.py#L736)、[`daemon.py`](../src/crypto_momentum_lab/live_rollout/daemon.py#L784)、[`daemon.py`](../src/crypto_momentum_lab/live_rollout/daemon.py#L816)）。准确说法应是“退出工作在队列层面不排在开仓 market loop 后面”，而不是完全不受同一事件循环、共享数据库或 Binance 客户端阻塞影响。

5. **“显式时钟注入”不准确。** Runtime Strategy 的接口是 `on_market_state(state)`，策略在构造信号时使用 `state.bucket_end`（[`runtime.py`](../src/crypto_momentum_lab/strategies/order_flow_impulse/runtime.py#L123)、[`runtime.py`](../src/crypto_momentum_lab/strategies/order_flow_impulse/runtime.py#L192)）；wall-clock 的 `clock` 注入发生在 daemon/外围模块。建议改为“策略消费显式事件时间，外围生命周期使用可注入 wall clock”。

6. **安全和性能收益仍有几处过强断言。** CheckpointWriter 的代码证明它把 checkpoint I/O 移出决策循环并提供 coalescing/flush（[`checkpoint_writer.py`](../src/crypto_momentum_lab/live_rollout/checkpoint_writer.py#L39)），但仓库没有足够基准证明已经“大幅缓解”数据库竞争；同样，持久化屏障、确定性 client id 和 reconcile 是降低重复/孤儿风险并支持恢复，不是数学意义上的“杜绝”。这些句子应改为“设计目标/风险降低/异常可恢复”，并保留生产指标作为验证条件。

7. **路线图中的持久化屏障应标成不变量，而不是等待 P0 后才执行的步骤。** 当前 [`prepare_submission`](../src/crypto_momentum_lab/persistence/postgres/order_repository.py#L82) 已经在 REST 前原子写入 intent、planned order 和 `SUBMITTING`；后续工作是守住并测试这个 seam，而不是把它当成测量完成后的新改造阶段。这样路线图的时间顺序才不会误导实施者。

8. **一处小的配置措辞需要改正。** “显式开启 `--no-entry-price-above-ema5/10`”容易理解成开启 EMA 过滤；准确表述应是“显式传入禁用 EMA5/10 价格准入过滤的选项”。

### 第三次复核后的落地顺序

1. 先补 ADR/设计规范，明确当前 live-strategy 直接交易的安全和凭证边界；
2. 为 telemetry 增加退出触发源和 source/context 时间戳，再建立可持续保存的生产 p50/p95/max 基线；
3. 把 Top100+EMA 的 2.2～10.4 秒标成历史窗口，重新测当前 Top10/no-EMA 路径；
4. 继续把 `EntryEligibilityPolicy` 作为真正的深模块候选，先用 Paper/Live contract tests 固化接口，再收窄 `LiveStrategyDaemon`；
5. 将 `LiveProcessSupervisor` 视为待验证的 seam，不按文件大小机械搬运代码；
6. 把持久化屏障作为长期不变量，只有 telemetry、signal、checkpoint 等可降级数据继续异步化；
7. 只有生产指标证明 Hub 的可靠性或维护成本不可接受时，才评估 Redis/UDS 等传输替代方案。

### 第三次复核结论

Gemini 的“定稿版”已经可以作为架构讨论和下一轮设计的基线，且主要方向是对的；但在 ADR、退出遥测、当前/历史配置分层以及几处绝对化措辞修正前，不建议把它当作无需再解释的最终实施规范。最值得保留的原则是：先测量、共享纯 Policy、守住下单前持久化屏障、再按 seam 和指标逐步收窄模块。

## 对最终核准版的第四次复核（2026-09-04）

### 总评

这次版本已经把上一轮提出的实质问题写进了正文：新增了认证交易边界 ADR，补充了退出遥测扩展计划，明确了 Top100+EMA 是历史观测，修正了事件时间和下单前持久化屏障的表述，也把退出描述改成多源触发。

因此，作为架构评审它已经达到 **9/10**，足以作为后续设计讨论的基线；作为可以逐项照抄的实施规范仍约 **8.5/10**。剩余问题主要是“实现细节与文案仍有一处错位”，不是方向性错误。

### 第四次复核仍需修正的事项

1. **直接下单的性能因果仍应收窄。** 报告称 live-strategy 直连 REST “消除了订单意图跨进程流转的等待”（[`定稿版`](ARCHITECTURE_REVIEW.md#L31)）。它确实移除了一个跨进程协调路径，但并不等于已经证明端到端延迟下降；建议改成“移除一跳协调开销，收益由阶段基准验证”。

2. **退出流仍未完全等同于 `_ExitExecutionLane`。** 报告称账户、报价、闭合 K 线和 grace 超时都进入该 lane（[`定稿版`](ARCHITECTURE_REVIEW.md#L109)），但当前代码中 account/market/quote 有队列，closed candle 和 grace timeout 直接调用 `_process_closed_candle_work` / `_process_grace_timeout_work`（[`daemon.py`](../src/crypto_momentum_lab/live_rollout/daemon.py#L816)、[`daemon.py`](../src/crypto_momentum_lab/live_rollout/daemon.py#L854)）。建议明确哪些入口共享 lane、哪些入口只共享退出锁和执行协调器。

3. **“实际连接拓扑”仍遗漏只读依赖。** 图已修正 Paper 的 PG 路径，但 `research-collector` 还直接连接 PostgreSQL 做 universe 选择和 Hub gap backfill（[`research_collector/main.py`](../src/crypto_momentum_lab/apps/research_collector/main.py#L277)），`live-strategy` 还直接访问 Binance 公共 REST/WS 做 24h volume 和 15m candle（[`live_rollout/main.py`](../src/crypto_momentum_lab/apps/live_rollout/main.py#L1347)、[`live_rollout/main.py`](../src/crypto_momentum_lab/apps/live_rollout/main.py#L1561)）。如果图只表达核心链路，应标注“省略只读辅助依赖”；如果声称是实际拓扑，应补上这些边。

4. **策略确定性收益不应写成机制保证。** “registry 从机制上排除未来函数和系统时钟漂移”（[`定稿版`](ARCHITECTURE_REVIEW.md#L120)）仍然过强。registry 加上 event-time 约束可以降低风险、支持同一输入下的确定性回放；未来函数还取决于数据截止边界、特征构造和 replay source 的顺序。

5. **`EntryEligibilityPolicy` 的收益和接口还需具体化。** “彻底消除代码漂移”（[`定稿版`](ARCHITECTURE_REVIEW.md#L235)）应改为“消除重复政策实现、降低漂移风险”。Policy 的不可变输入至少应包含 `observed_at`/候选过期时间、entry gate、方向和 universe/EMA 快照；账户风险与执行状态则继续留在外围模块，避免把 Policy 做成新的浅层参数转发器。

6. **“最终核准”与文档内容存在时间语义冲突。** 文档标题已经写成最终核准，但正文仍把 ADR、退出遥测和生产基准列为 P0 待办。更准确的标题应是“评审文档定稿版”，并注明“架构决策和遥测扩展尚待落地”。

### 第四次复核后的落地顺序

1. 把下单直连和双凭证边界写成正式 ADR，并同步修订 2026-06 设计规范；
2. 明确退出入口的队列/锁/直接调用三种语义，再实现 source-aware telemetry；
3. 给拓扑图标注完整依赖或明确省略范围，避免把核心链路误读成全部网络连接；
4. 用当前 Top10/no-EMA 配置建立生产基线，把历史 Top100+EMA 数据单独归档；
5. 用包含时间与 gate 语义的小接口实现 `EntryEligibilityPolicy`，通过 Paper/Live contract tests 固化行为；
6. 在上述不变量和观测闭环稳定后，再继续做 supervisor/daemon 的 seam 重构和中间件评估。

### 第四次复核结论

Gemini 的最终版本已经基本正确，且比最初报告可靠很多。当前最需要防止的是把“移除一跳”写成“延迟已改善”、把“队列隔离”写成“所有退出都由同一 lane 执行”，以及把“核心链路图”误当成完整网络拓扑。修正这些细节后，文档就可以作为稳定的架构基线使用。

## 对评审基线定稿版的第五次复核（2026-09-04）

### 总评

这次版本已经逐项回应了第四次复核提出的六类问题，而且不再把“文档定稿”冒充成“工程事项已经落地”：标题改为“评审基线定稿版”，并明确认证交易边界 ADR、退出遥测扩展和性能基准仍属于后续实施工作（[`ARCHITECTURE_REVIEW.md:15`](ARCHITECTURE_REVIEW.md#L15)）。

我的结论是：**作为架构评审文档约 9.5/10；作为可以直接逐项执行的实施规范仍约 8.5/10。** 剩余问题主要是当前凭证配置、少数退出快路径和几处治理/确定性措辞的精确度，不是架构方向错误。

### 本次已正确吸收的修正

- 直接 REST 下单现在只表述为移除一跳协调开销，并明确端到端收益必须由基准数据验证（[`ARCHITECTURE_REVIEW.md:35`](ARCHITECTURE_REVIEW.md#L35)）。
- 退出数据流明确区分了 account/market/quote 的 `_ExitExecutionLane` 队列，与闭合 15m candle、Grace timeout 的独立任务和标的锁路径（[`ARCHITECTURE_REVIEW.md:117`](ARCHITECTURE_REVIEW.md#L117)）。
- 拓扑图补充了“核心链路范围”说明，并列出 research-collector 的 PostgreSQL 读依赖及 live-strategy 的币安公共只读依赖（[`ARCHITECTURE_REVIEW.md:109`](ARCHITECTURE_REVIEW.md#L109)）。
- 事件时间段落同时写明了 Runtime Strategy 使用 `state.bucket_end`、外围生命周期使用注入的 Wall Clock，并承认未来函数仍依赖特征窗口和回放源截断（[`ARCHITECTURE_REVIEW.md:129`](ARCHITECTURE_REVIEW.md#L129)）。
- `EntryEligibilityPolicy` 已给出不可变输入、纯输出、外部隔离和 Paper/Live contract tests 的接口方向，避免把两个宿主 daemon 合并成超级模块（[`ARCHITECTURE_REVIEW.md:243`](ARCHITECTURE_REVIEW.md#L243)）。
- 路线图把下单前持久化屏障作为现存不变量，并把退出遥测、ADR、Policy 和 supervisor 重构排成可讨论的演进路径，而不是宣称已经完成。

### 仍需收敛的事实和边界

1. **凭证复用应写成当前风险和迁移项，而不只是目标配置。** 当前 Compose 将同一组 `BINANCE_API_KEY/SECRET` 变量注入 `execution-account-live` 和 `live-strategy`（[`compose.server.yaml:434`](../compose.server.yaml#L434)、[`compose.server.yaml:496`](../compose.server.yaml#L496)）。报告提出“双密钥”是正确的目标，但 ADR 还应明确记录“当前存在凭证复用”，并给出读账户密钥、交易密钥、最小权限和轮换迁移方案；否则读者容易把目标边界误认为现状。

2. **退出队列描述仍有一个快路径例外。** 报告概括为 account、market、quote 事件推入 `_ExitExecutionLane`，但 `process_account_event(quote=...)` 会直接调用 `_process_quote_work`，刻意绕过会合并最新 ticker 的 quote 队列（[`daemon.py:759`](../src/crypto_momentum_lab/live_rollout/daemon.py#L759)、[`daemon.py:768`](../src/crypto_momentum_lab/live_rollout/daemon.py#L768)）。建议改成“常规 account/market/quote 事件进入队列；account 携带 quote 时走不可丢失更新的直接快路径”，这样既保留设计意图，也与实现逐入口一致。

3. **事件确定性不应扩展成全系统时钟保证。** “从机制上排除了系统时钟漂移风险”仍偏绝对（[`ARCHITECTURE_REVIEW.md:131`](ARCHITECTURE_REVIEW.md#L131)）。registry 和 `bucket_end` 确实让策略决策核心不依赖隐式系统时间，但 lease、watchdog、Grace timeout 和事件到达仍受 Wall Clock 及调度影响。建议改成“降低策略核心对系统时钟漂移的敏感度，并为相同输入提供确定性回放基础”。

4. **Policy 接口应把快照契约写成可验证类型。** `Entry Gate 评估结果` 作为不可变输入没有问题，前提是 Policy 只消费结果而不在内部调用外部 gate。实施前最好固定一个 `PolicyInputSnapshot`（至少包含 `observed_at`、candidate expiry、gate result、direction、universe revision/ranking、EMA snapshot/version），并让 contract tests 对同一快照断言纯输出；这样“输入相同则输出一致”才不会退化成只比较一个布尔值。

5. **ADR 尚未批准前不宜宣布旧规范已废止。** 建议将“废止旧规范中策略绝不调用交易接口的条款”改为“提议修订/替换该条款”。当前 2026-06 设计规范仍是现行规范，ADR 的职责是解释为何接受例外、定义新的凭证与租约契约，并在批准后同步更新规范，而不是由评审报告单方面改变规范状态。

6. **拓扑范围已经说明，但标题还可避免歧义。** 章节称“已校准核心链路与实际连接”，随后又说明省略只读辅助依赖。建议改成“已校准核心链路（只读辅助依赖见注）”，或在图中用虚线补充这些边，避免读者把它当作完整网络拓扑。

7. **路线图的 P0 顺序可以按风险重新表达。** 先做 telemetry 再做 ADR 并非技术错误，但认证交易边界是安全治理前置项，不应让读者理解成必须等待性能测量完成后才可明确凭证权限。两者可以并行，或将 ADR 与 telemetry 并列为 P0。

### 第五次复核后的实施顺序

1. 立即把当前同一组凭证注入两个进程的事实写入 ADR，确定读/交易密钥最小权限、单主租约、Fail-Closed、熔断、重启对账和密钥轮换；同时保留下单前 `prepare_submission` 作为不可变不变量。
2. 为退出入口补齐触发源、source-received、context-ready 等阶段时间戳，分别测量队列路径、直接快路径、闭合 K 线和 Grace timeout 的 p50/p95/max。
3. 固化带版本的 `PolicyInputSnapshot` 与 `EntryEligibilityPolicy` 纯接口，再用 Paper/Live contract tests 验证同一快照下的政策结果。
4. 在接口和观测稳定后，按 seam 检验收窄 composition root 与 `LiveStrategyDaemon`，不要按文件大小机械搬运代码。
5. 继续把完整拓扑和历史 Top100+EMA 数据与当前 Top10/no-EMA 生产路径分层记录；只有指标证明 Hub 或后台任务确有瓶颈时，才评估传输层或计算层替换。

### 第五次复核结论

这版已经可以作为稳定的架构讨论基线，Gemini 的主要技术判断现在基本成立。下一步不需要再做方向性重写，重点是把当前凭证复用、退出快路径、Policy 快照契约和 ADR 的规范生效时点写得可执行、可审计。本文档本次已追加本轮复核；原始 `ARCHITECTURE_REVIEW.md` 与业务代码未修改。

## 对评审基线定稿版的第六次复核（2026-09-04）

### 总评

最新版本已经把第五次复核指出的治理和实现边界补进正文：当前凭证复用被明确标为运行态风险，ADR 改为“提议修订且审批后同步规范”，退出快路径被单独描述，拓扑标题限定为核心链路，P0-A/P0-B 改为并行前置项，事件时间也不再被描述成全系统时钟保证（[`ARCHITECTURE_REVIEW.md:36`](ARCHITECTURE_REVIEW.md#L36)、[`ARCHITECTURE_REVIEW.md:41`](ARCHITECTURE_REVIEW.md#L41)、[`ARCHITECTURE_REVIEW.md:49`](ARCHITECTURE_REVIEW.md#L49)、[`ARCHITECTURE_REVIEW.md:200`](ARCHITECTURE_REVIEW.md#L200)）。

因此，作为架构评审文档我仍给 **9.5/10**；作为可以直接照抄的工程实施规范约 **8.5/10**。现在没有方向性错误，剩余主要集中在共享 Policy 接口的命名/可选状态，以及一处对快路径行为的过度承诺。

### 本次已正确吸收的修正

- **安全治理**：报告明确记载 `execution-account-live` 与 `live-strategy` 当前复用同一组凭证，并将双凭证最小权限、租约、Fail-Closed、熔断、对账和轮换纳入待审批 ADR，而不是把目标配置写成现状（[`ARCHITECTURE_REVIEW.md:36`](ARCHITECTURE_REVIEW.md#L36)、[`ARCHITECTURE_REVIEW.md:227`](ARCHITECTURE_REVIEW.md#L227)）。
- **退出入口**：队列异步路径、account 携带 quote 的直接快路径、闭合 K 线与 Grace timeout 的独立任务已逐入口区分，和 `LiveStrategyDaemon` 的实际 seam 对齐（[`ARCHITECTURE_REVIEW.md:118`](ARCHITECTURE_REVIEW.md#L118)）。
- **确定性边界**：策略核心使用 `bucket_end`，外围生命周期使用 Wall Clock，并明确协程调度和宿主时钟仍会影响外围行为（[`ARCHITECTURE_REVIEW.md:130`](ARCHITECTURE_REVIEW.md#L130)）。
- **拓扑范围**：章节标题和注释已经共同说明这是核心决策/发单链路，研究 PostgreSQL 和币安公共只读连接属于省略的辅助依赖（[`ARCHITECTURE_REVIEW.md:49`](ARCHITECTURE_REVIEW.md#L49)、[`ARCHITECTURE_REVIEW.md:108`](ARCHITECTURE_REVIEW.md#L108)）。
- **演进顺序**：凭证 ADR 与退出遥测被列为并行 P0，强类型 `PolicyInputSnapshot` 进入 P1，且下单前持久化屏障保持为现存不变量（[`ARCHITECTURE_REVIEW.md:198`](ARCHITECTURE_REVIEW.md#L198)、[`ARCHITECTURE_REVIEW.md:210`](ARCHITECTURE_REVIEW.md#L210)）。

### 仍需收敛的接口和措辞

1. **`LiveGateResult` 会把共享 Policy 的接口拉回 Live 宿主。** 文档将 `entry_gate_result` 写成 `LiveGateResult`，但当前代码中并不存在这个类型，而且 `EntryEligibilityPolicy` 计划被 Paper 与 Live 共同调用。建议使用环境无关的 `EntryGateResult`/`GateEvaluation`，并明确它是 Policy 的不可变输入，而不是 Policy 内部通过外部依赖重新计算的门禁。

2. **`ema_snapshot` 不能默认成为必填的有效 EMA。** 当前生产 Compose 显式禁用 EMA5/10 价格准入过滤；如果 `PolicyInputSnapshot` 强制要求 `EmaSnapshot`，无 EMA 路径就必须伪造一个值。更好的接口是 `EmaPolicyState`（`disabled` / `unavailable` / `valid(snapshot)`）或可选的 `ema_snapshot` 加上明确的禁用状态，并让快照本身携带版本/截面时间。

3. **直接快路径的效果描述仍略强。** 报告说它“确保成交回报触发的退出决策永不丢失中间最新行情”（[`ARCHITECTURE_REVIEW.md:120`](ARCHITECTURE_REVIEW.md#L120)）。实现真正保证的是：account 携带 quote 时不会被会合并 ticker 的 quote 队列丢弃；它不保证该 quote 是全局最新值，也不保证并发到达的后续行情不会改变决策上下文。建议改成前者的精确表述。

4. **遥测维度应保持稳定的 lane 与可扩展的 trigger source 分离。** 当前实现只有 `entry`/`exit`/`unknown` lane，新增 account、quote、candle、grace 时，建议保留 `lane=exit`，把来源作为 `trigger_source` 字段，而不是继续膨胀 lane 枚举；这样既能按来源切分 p50/p95/max，又不会破坏现有聚合和告警契约（[`ARCHITECTURE_REVIEW.md:235`](ARCHITECTURE_REVIEW.md#L235)）。

5. **`PolicyInputSnapshot` 的“不可变”需要落实到嵌套快照。** 顶层字段已经列出 `observed_at`、candidate expiry、universe 和 EMA，但实施时还应规定时区、截面/版本标识、排名并列时的稳定 tie-break，以及 disabled/unavailable 的序列化形式；否则“相同输入”仍可能因隐含默认值而不相同。

### 第六次复核后的实施顺序

1. 先在 ADR 中记录当前凭证复用事实，完成读账户 Key 与交易 Key 的最小权限分离和可回滚轮换方案；在 ADR 获批前保留旧规范为现行规范。
2. 按 `lane=exit + trigger_source` 补齐退出各入口的 source/context 时间戳和阶段耗时，建立当前 Top10/no-EMA 路径的生产基线。
3. 将 `PolicyInputSnapshot`、环境无关的 `EntryGateResult` 和带 disabled 状态的 EMA 输入固化为小而深的 Policy 接口，用 Paper/Live contract tests 验证纯输出。
4. 观测和接口稳定后，再按 seam 收窄 composition root 与 `LiveStrategyDaemon`；不要把新增类型变成宿主状态的浅层转发器。
5. 继续以指标决定 Hub、EMA 后台计算和 PostgreSQL 优化，维持下单前持久化屏障不变量。

### 第六次复核结论

Gemini 的这版已经可以作为稳定的架构讨论基线，且对上一轮意见的吸收是实质性的。剩余两项最值得在实施前修正：将 `LiveGateResult` 改成环境无关的 Policy 输入类型，并为当前 no-EMA 生产路径定义显式 disabled 语义；同时收窄“快路径保证最新行情”的措辞。除此之外，文档与当前代码的主要事实已经对齐。本次已将复核追加到本 assessment 文档，未修改原始评审或业务代码。

## 对评审基线定稿版的第七次复核（2026-09-04）

### 总评

这次更新已经实质性解决了第六次复核的主要问题：Policy 输入改为环境无关的 `EntryGateResult`，EMA 明确建模为 `disabled / unavailable / valid(snapshot)`，快路径不再声称保证全局最新行情，遥测也明确采用稳定 `lane` 加正交 `trigger_source` 的方式（[`ARCHITECTURE_REVIEW.md:120`](ARCHITECTURE_REVIEW.md#L120)、[`ARCHITECTURE_REVIEW.md:235`](ARCHITECTURE_REVIEW.md#L235)、[`ARCHITECTURE_REVIEW.md:249`](ARCHITECTURE_REVIEW.md#L249)）。

我的评价保持不变：**作为架构评审文档约 9.5/10；作为可直接执行的工程规范约 8.5/10。** 现在没有方向性错误，剩余的是遥测入口和延迟分段如何精确落到当前代码接口上的问题。

### 本次已正确吸收的修正

- `PolicyInputSnapshot` 使用环境无关的 `EntryGateResult`，并为 UTC、Universe 版本/稳定 tie-break 和 EMA 三态建立了可测试的输入轮廓。
- account 携带 quote 的直接路径现在只承诺避免该行情被普通 quote 队列丢弃，没有把它扩大成“始终使用全局最新行情”的保证。
- 退出遥测保留现有 `entry`/`exit`/`unknown` lane，把触发来源放在独立的 `trigger_source` 字段中，兼顾兼容性和按来源统计能力。
- P0-B 路线图已经把 `lane=exit + trigger_source` 写入基准目标，和当前遥测枚举的演进方式一致。

### 仍需收敛的实现边界

1. **`trigger_source` 枚举遗漏 market。** 当前 `_ExitExecutionLane` 明确有 account、market、quote 三个队列（[`daemon.py`](../src/crypto_momentum_lab/live_rollout/daemon.py#L302)–[`daemon.py`](../src/crypto_momentum_lab/live_rollout/daemon.py#L304)），但文档只列 `account | quote | candle | grace`（[`ARCHITECTURE_REVIEW.md:237`](ARCHITECTURE_REVIEW.md#L237)）。应加入 `market` 或统一命名为 `market_state`，否则 market-state 退出的延迟会落入未知或错误来源。

2. **market 退出的“独立性”需要按入口时机说明。** `_run_market_loop` 在完成当前状态的开仓处理后才调用 `submit_market`（[`daemon.py`](../src/crypto_momentum_lab/live_rollout/daemon.py#L1395)–[`daemon.py`](../src/crypto_momentum_lab/live_rollout/daemon.py#L1403)）。因此准确的保证是“入队后由独立 worker 执行，account/quote 可以独立抢先入队”，而不是所有 market 退出从事件到达起就完全脱离开仓路径。

3. **`EmaPolicyState` 三态仍需固定决策语义。** 文档列出了 `disabled / unavailable / valid`，但尚未规定 Policy 对每一态的确定性结果。建议明确：过滤关闭时 `disabled` 直接跳过；过滤开启但数据缺失时 `unavailable` 必须 fail-closed；只有 `valid` 才进行 EMA 比较，并将这三种分支写入 contract tests（[`ARCHITECTURE_REVIEW.md:257`](ARCHITECTURE_REVIEW.md#L257)）。

4. **`context_ready` 的遥测接口还没有体现退出维度。** 当前 `LiveTelemetrySink.context_ready()` 仍没有 lane/trigger source 参数，具体实现也固定用 entry lane（[`telemetry.py`](../src/crypto_momentum_lab/live_rollout/telemetry.py#L356)–[`telemetry.py`](../src/crypto_momentum_lab/live_rollout/telemetry.py#L368)）。P0-B 计划要统计退出的 `source_received -> context_ready`，就必须在接口和 trace id 设计中同时传递 `lane=exit`、`trigger_source` 及对应的事件时间。

5. **阶段基准可以进一步拆开风险门禁与持久化屏障。** 当前指标把 `candidate_accepted -> intent_saved` 合并为一个阶段（[`ARCHITECTURE_REVIEW.md:240`](ARCHITECTURE_REVIEW.md#L240)–[`ARCHITECTURE_REVIEW.md:243`](ARCHITECTURE_REVIEW.md#L243)），但遥测协议已有 `RISK_APPROVED` 阶段。若目标是定位热点，建议至少拆成 `candidate_accepted -> risk_approved` 与 `risk_approved -> intent_saved`，否则风险计算耗时和数据库事务屏障耗时无法分别归因。

### 第七次复核后的实施顺序

1. 先确定 `trigger_source` 的完整集合（包含 `market`/`market_state`），并扩展 telemetry 的 `context_ready` 与 trace 接口以携带退出 lane、来源和事件时间。
2. 为 `EmaPolicyState` 固化 disabled/unavailable/valid 的 fail-closed 语义，使用不可变 `PolicyInputSnapshot` 编写 Paper/Live contract tests。
3. 建立按入口和阶段拆分的生产基准，明确 market 入队等待、queue worker 执行、直接快路径和 candle/grace 任务的时间范围；同时拆开 risk approval 与 `prepare_submission` 延迟。
4. 在安全、观测和 Policy seam 稳定后，再进行 composition root 与 `LiveStrategyDaemon` 的深模块化重构。
5. 继续以指标决定 Hub、EMA 后台计算和 PostgreSQL 优化，维持下单前持久化屏障不变量。

### 第七次复核结论

Gemini 的最新版已经可以作为可靠的架构基线，上一轮提出的 Policy 命名、no-EMA 建模和快路径过度承诺问题均已得到有效修正。当前最重要的不是再改总体方向，而是把 `market` 触发源、退出 `context_ready` trace 以及风险门禁/持久化的阶段拆分落实到 telemetry 接口和 contract tests 中。原始评审与业务代码仍未修改，本 assessment 文档已记录本轮复核。

## 对评审基线定稿版的第八次复核（2026-09-04）

### 总评

这次版本已经把第七次复核的五项建议全部写入正文：`market` 被加入 `trigger_source`，market 退出的入队时机被单独说明，EMA 三态有了明确的 Fail-Closed 分支，`context_ready` 计划支持退出 lane/source，风险审批与持久化屏障也被拆成独立阶段（[`ARCHITECTURE_REVIEW.md:119`](ARCHITECTURE_REVIEW.md#L119)、[`ARCHITECTURE_REVIEW.md:237`](ARCHITECTURE_REVIEW.md#L237)、[`ARCHITECTURE_REVIEW.md:261`](ARCHITECTURE_REVIEW.md#L261)）。

我的评价保持为：**架构评审文档 9.5/10；工程实施规范 8.5/10。** 目前剩余的是 telemetry 事件身份和 Policy 语义的最后一层契约，不再是总体拓扑或演进方向问题。

### 本次已正确吸收的修正

- 完整的退出来源集合现在包括 `account`、`quote`、`market`、`candle`、`grace`，且核心 lane 枚举保持兼容。
- 文档承认 market 事件只有在本轮开仓评估完成后才入队，避免把“独立 worker”误写成“从入口开始完全并行”。
- `EmaPolicyState` 已明确三态，数据缺失时的交易安全倾向被写为拒绝候选；Policy 快照也固定了 UTC、版本截面和稳定 tie-break。
- 延迟基准已经拆成 `candidate_accepted -> risk_approved -> intent_saved`，能分别观察风险计算和下单前事务屏障。

### 仍需收敛的最后几项契约

1. **Trace ID 仍可能碰撞。** 当前代码的 `state_trace_id()` 只由 `lane:symbol:bucket_start` 组成（[`telemetry.py`](../src/crypto_momentum_lab/live_rollout/telemetry.py#L234)）。同一标的、同一 15 秒桶内可以出现多次 account、quote、market、candle 或 grace 触发；仅新增 `trigger_source` 字段并不能阻止这些事件共享同一 `_Trace`，阶段时间会互相覆盖。应为每次 source ingress 引入唯一的 `source_event_id`/序列号，并把它纳入 Trace key 或建立明确的 parent/child trace 关系。

2. **`source_received` 需要通用事件接口，而不只是扩展 `context_ready`。** 当前 `LiveTelemetrySink.market_state_received()` 的接口仍只接受 `MarketState15s` 和 lane（[`telemetry.py`](../src/crypto_momentum_lab/live_rollout/telemetry.py#L89)），但 account、quote、candle、grace 的 source 并不都是 market-state。P0-B 应增加通用的 `source_received` 事件/输入模型，或为现有方法补齐 source metadata、trigger source 和唯一 Trace；否则退出来源的第一阶段时间戳仍无法可靠记录。

3. **`disabled` 不应被解释为整个 Policy“直接通过”。** 文档目前写成“跳过均线检查直接通过”（[`ARCHITECTURE_REVIEW.md:262`](ARCHITECTURE_REVIEW.md#L262)）。正确语义是只跳过 EMA 这一谓词，之后仍须检查候选过期、Entry Gate、Universe、方向等其他准入条件；`unavailable` 在启用过滤时 fail-closed，`valid` 才执行均线比较。三态应分别进入 contract tests。

4. **退出 `context_ready` 的可选参数需要条件不变量。** 计划中的 `trigger_source: str | None = None` 便于兼容 entry，但对于 `lane=exit` 必须要求非空且属于完整枚举；否则即使扩展了接口，仍会产生无来源或不可关联的退出 Trace。该约束应在 telemetry sink 和调用方两侧都校验。

5. **保留一个文档清理项。** 退出快路径描述中仍有“被覆盖覆盖”的重复字样（[`ARCHITECTURE_REVIEW.md:120`](ARCHITECTURE_REVIEW.md#L120)），不影响架构判断，但应在基线发布前修正。

### 第八次复核后的实施顺序

1. 先定义统一的 source ingress 事件和唯一 Trace 身份，覆盖五种退出来源，并在 `lane=exit` 时强制校验 `trigger_source`。
2. 将 `source_received`、`context_ready`、candidate、risk、intent 和 exchange 阶段全部挂到同一 source trace，避免同桶事件相互覆盖。
3. 将 `EmaPolicyState` 的三态语义固化为“跳过单项检查 / fail-closed 拒绝 / 执行比较”，用 Paper/Live contract tests 验证其他准入条件不被 disabled 状态短路。
4. 修正文档重复字，并在当前 Top10/no-EMA 配置下验证 telemetry schema 和 Policy 快照序列化。
5. 其余 supervisor、Hub 和后台任务演进继续遵循先测量、再按 seam 深化模块的原则。

### 第八次复核结论

Gemini 的最新版已经接近可发布的架构基线，上一轮提出的路径边界、来源集合和阶段拆分都已正确回应。真正还需补齐的是“每个退出触发实例如何唯一追踪”以及“disabled EMA 只跳过一条规则而非整条 Policy”的契约。把这两点落实后，文档就不只适合讨论，也足以作为 telemetry 与 Policy 实现的验收标准。本 assessment 文档已记录本轮复核，原始评审和业务代码未修改。

## 第一批实施切片后的复核（历史记录，2026-09-04）

按照 [`docs/REFACTOR_PLAN.md`](REFACTOR_PLAN.md) 的第一批低风险切片，已把第八次复核中最明确的两个契约先落成代码，但仍保留旧主路径：

- [`docs/adr/0001-live-trading-credential-boundary.md`](adr/0001-live-trading-credential-boundary.md) 记录当前双服务复用凭证的事实、读/交易角色、迁移与回滚；同时 [`config/models.py`](../src/crypto_momentum_lab/config/models.py) 已增加只保存环境变量名的 `BinanceCredentialConfig` schema。ADR 仍为 Proposed，schema 尚未接入运行时，未切换生产密钥。
- [`live_rollout/telemetry.py`](../src/crypto_momentum_lab/live_rollout/telemetry.py) 增加 `SourceIngress`、`TraceKey` 和通用 `source_received`。退出车道缺少或使用未知 `trigger_source` 会 fail fast；source event time 与本地 received time 分开保存。现有 `state_trace_id`、退出调用方和 dashboard 尚未迁移，因此不会改变交易行为。
- [`domain/strategy/entry_policy.py`](../src/crypto_momentum_lab/domain/strategy/entry_policy.py) 增加纯计算的 `PolicyInputSnapshot`、环境无关 `EntryGateResult`、方向感知 Universe 快照、三态 `EmaPolicyState` 和 `EntryEligibilityPolicy`。disabled 只跳过 EMA 谓词，unavailable 拒绝，valid 校验新鲜度、symbol 和严格大于 EMA；Live/Paper 尚未接管。

新增定向测试 26 个全部通过；相关模块的 `ruff` 和严格 `mypy` 通过，整个 Python 包的严格 `mypy` 也通过（175 个源文件）。全量 pytest 为 **831 passed / 4 skipped / 4 failed / 36 errors**：失败和错误集中在当前沙箱无法访问本地 PostgreSQL/loopback 的集成、E2E 与 smoke 测试，未见本次新增切片的失败。

这一步只证明了 seam 和输入契约可以独立测试，尚不能证明生产遥测已经覆盖五种退出入口，也不能替代凭证权限灰度、source trace 接线和 Policy compare-only。下一步仍应先做调用方适配与阶段基线，再考虑 Supervisor 拆分。

## 第二批实施切片后的复核（2026-09-04）

这次已把第八次复核要求的 source ingress 接到真实退出调用方，整体方向正确，且没有触碰交易语义：

- `account`、`quote`、`candle`、`grace` 四个外部入口以及 daemon 内的 `market` 入口现在都创建稳定 `SourceIngress`；source/context/candidate/risk/intent 阶段沿同一 source trace 关联，候选 trace 以 source trace 为 parent。
- `account`、`market`、`quote` 的队列/合并语义不变；account 携带 quote 的直接快路径、candle/grace 的独立任务和既有标的锁仍然保留。market 仍在本轮开仓检查之后入队，只是 source 在状态刚被观察时先记录，因此不会把 worker 独立性误读成入口级并行。
- 适配层保留 `lane=exit`，把五种来源放在正交 `trigger_source`；退出 ingress 缺来源或未知来源会在模型层 fail fast。报价模型暂无上游 update id，当前使用来源时间、价格和桶组成的内容指纹（本地 `received_at` 不参与身份），后续若 Hub 提供稳定序列应优先替换。
- `prepare_submission`、租约 Fail-Closed、reduce-only、退出锁、队列 coalescing 和 checkpoint 均未改动；无 `source_ingress` 的旧调用仍走兼容路径。

### 还不能宣称完成的部分

1. `source_received`/`context_ready` 默认仍主要存在于内存 trace；高频 quote source 没有直接加入默认持久化集合。若要形成可审计的生产基线，需要先按容量和 SLO 决定全量、采样或异常落盘策略。
2. 未到达候选或后续阶段的 trace 尚未统一写入 terminal reason；当前切片能保留“只到 source/context”的事实，但还不能仅凭持久化订单事件解释所有拒绝、熔断和异常。
3. 当前验证是退出、遥测和应用适配的 **82 个定向测试通过**，加上全包严格 `mypy` 和相关 `ruff` 检查；排除一个已有的 `deploy` 导入路径问题后，完整 `tests/unit` 为 **807 passed / 4 skipped**。此前完整 pytest 的 PostgreSQL/loopback 环境错误仍需在可用依赖环境重跑，不能用定向测试替代生产阶段基线。

### 结论与下一步

这一步已经把“唯一 Trace 只停留在模型层”的主要风险降为真实调用方的可观测 seam，足以进入 P0-B 的数据策略和阶段基线设计；不建议现在就做 Supervisor 或大规模 Daemon 拆分。下一步应先确定 source/context 的持久化与 terminal reason 契约，在当前 Top10/no-EMA 配置下采集五类退出来源的 p50/p95/max，再进行凭证 ADR 灰度和 Policy compare-only。

## 终止原因切片后的复核（2026-09-04）

### 结论

第三批小切片已经把上一节指出的“只到 source/context、无法解释为何没有后续 phase”推进到一个可测试的 telemetry seam。`TRACE_TERMINATED` 现在挂在 `SourceIngress.trace_id` 上，主要退出入口会记录禁用、上下文失败、无请求、风险/量化拒绝、交易所拒绝、待对账和处理异常等低基数原因；这一步没有把原因塞进执行状态机，也没有改变下单前持久化屏障。

### 这次判断成立的依据

- `LiveTelemetrySink.trace_terminated()` 的接口只有 ingress、时间、reason 和可选 details；调用方不需要了解 `_Trace`、延迟采样或持久化队列，seam 足够小且可通过 `LiveRuntimeTelemetry` 测试。
- daemon 的 source/context/worker 终止记录覆盖 account、quote、market、closed candle、Grace 五类退出触发；请求评估、recovery 和执行异常也在进入 worker 的失败返回前留下原因。latest-value coalescing 替换掉的 market/quote source 会在提交新 work 时被标记为 `coalesced_by_newer_source`，不会留下只有 `source_received` 的悬空 trace。
- child order 的 `candidate_accepted`、exchange boundary 和 account fill 事件也保留 `source_trace_id`/source metadata，因此 source trace 不会在进入订单状态机后丢失因果链。
- terminal/source/context 仍不加入默认 `PERSISTED_ORDER_TELEMETRY_EVENTS`，因此高频 quote 的观测不会自动转化为执行库写放大；这同时意味着当前结果是运行态内存诊断，不是重启后可审计日志。
- terminal helper 对 sink 异常采用 best-effort，保证 telemetry 故障不会改变退出结果；这与“可降级观测，不可阻断交易”的不变量一致。

### 尚未宣称完成的部分

1. 终止原因目前覆盖主要 source-aware 退出路径，但 entry 仍主要使用旧的 `state_trace_id`，不能宣称所有策略 trace 都统一到同一 source ingress。
2. 同一个 source id 的重试可以产生多个 `TRACE_TERMINATED` 尝试事件；是否需要 attempt 序列、幂等写入或去重，要等生产重试率和容量数据，不应先凭空引入状态。
3. 默认不持久化 terminal/source/context，仍无法仅靠数据库在进程重启后重建所有“未下单/被拒绝”原因；需要下一步按低基数、采样和异常落盘策略补齐。
4. 生产 Top10/no-EMA 基线、凭证权限灰度和 Policy compare-only 仍未完成；本地完整单测仍需排除已有 `deploy` 导入路径问题，集成/loopback 结果不能由定向测试替代。

### 复核后的下一步

先在可用部署环境按 `lane=exit + trigger_source` 观察 terminal reason 分布和阶段延迟，再决定持久化策略；保持 `prepare_submission`、租约 Fail-Closed、退出锁和 reduce-only 执行 seam 不动。只有这些数据和凭证 ADR 演练稳定后，才进入 Policy compare-only 与 Supervisor seam 设计。

## 终止原因汇总切片后的复核（2026-09-04）

### 结论

第四批小切片的方向是对的：在 `TRACE_TERMINATED` 事件之上增加 `terminal_reason_summary()`，让生产基线先有一个小而深的读取 seam，而不是立即把高频 source/context/terminal 写入执行库。当前代码已经足以支持“按退出 lane、触发源和原因看占比”的只读诊断，但仍不能把它描述成重启后审计日志或跨实例指标系统。

### 这次改动成立的依据

- `TerminalReasonSummary` 只暴露 `lane -> trigger_source -> reason -> count`，且返回值可直接 JSON 序列化；调用方不需要接触 `_Trace`、后台 writer 或事件持久化细节，这是 telemetry 模块内部实现复杂度的良好封装。
- 计数在终止事件成功进入有界内存 trace 后更新，返回值是脱离内部字典的新快照；读取不会改变交易路径或 recorder 状态。
- symbol、source event id、candidate/client order id 等高基数身份没有进入汇总键，详细因果链仍由 `recent_events` 保留；因此这一步为生产采样提供了可控的基线入口。
- 定向测试达到 **87 passed**，排除既有 `deploy` 导入路径问题的完整单元测试达到 **812 passed / 4 skipped**，telemetry 的 `ruff` 与严格 `mypy` 均通过。

### 仍需明确的限制

1. reason 是调用方提供的字符串，低基数目前是契约约束而非类型系统或持久化 schema 的硬限制；若未来把订单号、symbol 或异常文本直接塞入 reason，汇总键仍会膨胀。动态细节应继续放入 `details`，后续落盘前应固定枚举或增加容量上限。
2. 计数是单进程、单 run 的内存状态，进程重启、滚动部署和多实例不会自动合并；它只能作为导出前的本地聚合，不能替代审计日志或监控时序库。
3. 同一 source id 的重试仍按多次 terminal attempt 计数，当前没有幂等去重或 attempt 序列；这保留了事实但需要生产重试率数据来决定是否演进。

### 复核后的下一步

在可用部署环境定期读取并导出该汇总，同时记录五类退出来源的阶段延迟、coalescing 和重试样本；用实际 reason 基数与重启审计要求决定全量、采样或异常持久化。固定 reason 契约和容量策略、完成凭证 ADR 演练后，再推进 Policy compare-only；不要因为已有汇总接口就提前拆分 Supervisor 或改动下单前持久化屏障。

## 服务器只读基线后的复核（2026-09-04）

### 结论

服务器只读数据让下一步更具体了：当前主要需要验证和收窄的是 exchange telemetry 的写入形状，而不是继续猜测策略计算或 checkpoint 导致的 500 秒级延迟。最近 24 小时 `exchange_response_received` 有 1,218 条，其中 1,141 条为 `query`，约占 93.7%；候选→风险和风险→意图各 68 条，p50 分别为 0.157 ms 和 0.124 ms。另有 7 次 telemetry persistence timeout 日志。这个证据支持“先拆 query 持久化策略”，不支持立刻重写执行状态机。

### 证据边界

- 服务器运行的是 `ef85b97` 旧镜像，尚未包含第四批 source/terminal 汇总，因此无法从生产库统计 trigger source 或 terminal reason；当前只能验证旧 schema 的订单/交易 exchange 事件。
- `live-strategy`、account sync、market-data 和 PostgreSQL 均 healthy，live-strategy restart count 为 1；这说明服务当前可观测，但不能把健康状态等同于遥测无丢失。
- 遥测表约 96 MB、约 139k live rows，dead rows 仅 3；持久化超时更像需要按 operation/批次继续定位的观测写入问题，尚未构成 PostgreSQL 交易根因的证据。
- 服务器 `.env.server`（0600）仍同时包含 Binance key/secret 配置键且没有 `.env.live`，与凭证 ADR 的风险判断一致；没有读取或记录 secret 值，也没有改变远端配置。

### 复核后的实施顺序

1. 先把当前未提交的 source/terminal telemetry 变更做成可回滚镜像，在隔离或无真实下单路径验证 stop 汇总和 reason 基数；不要直接覆盖正在运行的 `ef85b97`。
2. 在本地为 durable telemetry 增加 operation-aware 的策略 seam（至少区分 `query`、`submit`、`cancel`），先用现有事件回放/故障注入测试证明默认订单审计不丢失，再考虑服务器灰度。
3. 服务器灰度期间同时采集 source/terminal 分布、dropped/persist failures、query/submit/cancel RTT 和重启窗口；数据稳定后才决定全量、采样或异常落盘。
4. 凭证权限、租约失效和重启对账演练完成后，才推进 Policy compare-only；Supervisor/Daemon 拆分继续后置。

## 服务器基线后的第五批复核：operation-aware durable telemetry seam（2026-09-04）

### 结论

针对服务器上 `query` exchange response 占比约 93.7% 且出现 persistence timeout
的证据，本轮先在本地把 durable telemetry 的 operation 选择收敛为显式、可回滚的
接口。这个方向是合理的：它缩小了观测写入的变化面，没有把查询结果误当成订单状态机
问题，也没有改动 `prepare_submission`、租约、退出锁或 reduce-only 语义。

### 实施结果

- `LiveRuntimeTelemetry` 新增可选 `persist_exchange_operations` allow-list，仅作用于
  `exchange_request_started` 和 `exchange_response_received`，按 `details.operation`
  匹配；其他事件仍按原有 `persist_event_types` 规则处理。
- 默认值 `None` 保持兼容：省略参数时 `query`、`submit`、`cancel` 以及未知的未来
  operation 都照常进入 durable queue。显式集合可以在灰度时只保留 `submit`/`cancel`；
  显式空集合则关闭 exchange boundary 的 durable 写入，但不影响内存 trace 或延迟统计。
- allow-list 在构造时规范化并拒绝空名称；缺失或未知 operation 在启用 allow-list 时
  不落盘。stop 日志输出当前配置，便于判断实际写入形状。
- `cml-live-rollout run` 现在接受该参数并传入 daemon；服务器 Compose 也预留了
  `CML_LIVE_PERSIST_EXCHANGE_OPERATIONS` 插值，环境文件未设置时仍为 `None`。因此
  这只是本地 seam 和配置接线，不是服务器行为变更。

### 证据与边界

- telemetry/source/daemon/live app 定向测试 **96 passed**；排除既有 `deploy` 导入路径
  问题后的完整 `tests/unit` 为 **821 passed / 4 skipped**。新增测试证明 query 会被
  过滤、submit/cancel 的 request/response 审计保留，且默认不传 allow-list 时 query
  仍会持久化；全包严格 `mypy`（175 个源文件）通过。
- 该 seam 尚未证明过滤 query 会降低数据库超时；服务器灰度还必须同时观察批次耗时、
  dropped/persist failures、submit/cancel 审计完整性和重启窗口。
- 因为 `source_received`、`context_ready`、`trace_terminated` 仍未加入默认 durable
  集合，本轮也不能宣称退出来源/终止原因已具备重启后审计能力。

### 复核后的下一步

在隔离或影子环境设置 `CML_LIVE_PERSIST_EXCHANGE_OPERATIONS=submit,cancel`，先验证
`submit`/`cancel` 不丢、query 过滤比例与
telemetry failure 指标，再决定是否为生产增加配置字段和回滚开关。若写入超时不随
operation 过滤改善，应回到 sink/批次/连接池证据，而不是扩大到 Supervisor 或执行状态
机重写；凭证权限、租约失效和重启对账演练仍先于 Policy compare-only。

## 直接生产上线后的复核（2026-09-04）

上面的“先隔离/影子、尚未改变服务器行为”已被后续上线取代：用户选择直接部署，
服务器现运行 release `c165a82944b3d9e0155f9fe197d34def86f2eabb`，并设置
`CML_LIVE_PERSIST_EXCHANGE_OPERATIONS=submit,cancel`。这次变更没有关闭 query 调用，
只是把 query 的 exchange boundary 从 durable queue 过滤掉；内存 trace、日志、延迟
采样和订单状态机仍照常工作。

### 生产证据

- `live-strategy` 从 `2026-09-04 14:05:06 UTC` 起运行超过一小时，持续 healthy，
  restart count 为 0；租约成功接管后按周期续租。
- 接管时旧 lease 尚未过期，出现 3 次 `missing_active_lease` 的 Fail-Closed 重试，
  没有发单；旧 lease 过期后才成功 `prepare` 并进入自动续租，说明部署切换没有绕过
  租约安全门。
- 观察窗口内 `submit` request/response 各 9 条，`exchange_filled` 7 条、`account_fill`
  12 条，且 candidate/risk/intent/submitting 各 9 条；没有 query durable 行，符合
  allow-list。没有 cancel 样本，因此 cancel 的完整性仍需真实撤单样本或故障注入覆盖。
- 账户 reconciliation 为 `ready`、mismatch 0（3 positions、3 open orders）。
- 观察到一条 `live_strategy_signal_persist_failed (TimeoutError)`，随后恢复；没有观察到
  submit/cancel 审计断裂、live latency telemetry persist/drop failure、ERROR/CRITICAL 或
  lease renewal failure。单次旁路超时是需要告警的信号，不足以把 PostgreSQL 判定为交易
  路径根因。

### 复核后的判断

operation-aware seam 已获得初步生产证据，且没有改变交易不变量；因此不必再把“必须先
影子验证才能开启过滤”作为当前 release 的阻塞项。但证据仍有边界：运行窗口短、没有
cancel 样本，无法证明长期数据库收益、重启后 query 诊断完整性或凭证隔离已经完成。
后续应先固定告警与回滚条件，继续观察写入超时趋势，再做凭证 ADR、租约失效/重启对账
演练，最后才进入 Policy compare-only；不要借此提前拆分 Supervisor 或重写执行状态机。

## P0-A 凭证解析 seam 的实施复核（2026-09-04）

在生产观测稳定后，下一步已落成一个不改变运行态的配置模块：
`resolve_binance_credentials()` 按显式角色解析环境变量，缺失或空白即 Fail-Closed，
并通过 `ResolvedBinanceCredentials.metadata()` 提供不含 secret 的启动诊断。该接口把
凭证选择和安全显示规则集中在一个可替换 seam，调用方不必各自复制环境读取逻辑。

定向凭证测试 **10 passed**，`ruff` 和严格 `mypy` 通过。当前两个长驻服务尚未接入该 seam，
服务器仍使用旧的 `BINANCE_API_KEY/BINANCE_API_SECRET` 映射；因此这一步不能宣称已经完成
凭证隔离，也没有理由现在重启或重新部署线上服务。

下一步应接入 `live-strategy` 与 `execution-account-live` 的显式 read/trade role，保留
一个有名称且可审计的兼容开关，然后用假 adapter 验证 read role 的写接口拒绝、trade role
的下单路径和权限错误 Fail-Closed。凭证轮换、租约失效、重启对账演练及 ADR 批准完成前，
不把这部分运行时接线推到生产。

## P0-A 长驻入口角色接入复核（2026-09-04）

上述门槛已在本地实现：`execution-account-live` 的长驻同步入口默认使用 read role，
`live-strategy run` 默认使用 trade role；旧共享变量只通过显式
`--allow-legacy-credential-fallback` 兼容开关可用。Compose 仅把角色变量注入对应容器，
并暂时保留该开关以维持迁移期间的可回滚性。

相关配置、入口和 manifest 测试共 **58 passed**，`ruff` 与严格 `mypy` 通过。生产仍运行
旧镜像/旧环境映射，本轮没有远端重启或凭证切换；因此还不能宣称双密钥隔离完成。下一步
是用假 Binance adapter 验证 read role 的写接口拒绝、trade role 的交易路径和权限错误
Fail-Closed，再完成密钥 provision、轮换回滚、租约失效和重启对账演练。ADR 批准后移除
兼容开关，最后才逐个服务切换生产变量。

## 双角色凭证生产预检（2026-09-05）

服务器 `.env.server` 已配置四个角色变量，文件权限为 `0600`，且没有在检查输出中暴露
secret。两组凭证分别签名调用 Binance Futures `GET /fapi/v2/account`，均返回
`HTTP 200`/`binance_code=ok`；因此网络、签名和账户读取路径已具备上线前提。

这不是写权限验收：本次没有发送真实 submit/cancel/order 请求，read role 是否拒绝写接口仍
要靠假 adapter 和 Binance 权限配置验证。角色接入镜像也尚未部署，生产仍运行
`c165a82`；应先做聚焦 commit/push，再按 execution-account → live-strategy 顺序滚动重启并
观察健康、对账和租约，稳定后再删除兼容 fallback。
