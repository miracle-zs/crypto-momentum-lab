# 执行引擎重构规范与任务契约

执行方：Antigravity；验收方：Codex。基线：`b73da9a9a864788ec4f62ae30d1378bc83902ec9` 的审计工作树，2026-10-10。[审计报告](docs/architecture/execution-state-audit.md)是缺陷证据；本文是实施与 Review 契约。

范围：P1-1～P1-4、P2-1～P2-2及四维状态收敛。P1-3 是条件性退出延迟，P1-4 的解析行为已复现但真实交易所报文未核验，P2-2 是容量风险；不得将它们描述为已证实的生产事故。保留策略参数、批次分配、真实成交归属、命令幂等、GTD 和数据库事务语义。不得部署、操作实盘、清理历史数据或改写既有审计结论。

路径约定：下文 `S/` 指 `src/crypto_momentum_lab/`；函数签名中的 `…` 仅省略现有参数。新增私有辅助函数允许，现有公开入口参数和返回类型保持兼容，明确列出的新增参数除外。

## 1. 系统不变式（System Invariants）

| ID | 硬性红线 | 可验收定义 |
|---|---|---|
| I1 | **终态不可逆屏障** | 同一命令／订单身份一旦由有效证据确认终态，迟到 ACK、部分成交、SUBMITTING、UNKNOWN 或失败回调均不得重新激活订单、Outbox、入场 pending 或已释放预留。累计成交量单调不减；终态间只允许有证据的结果细化，如确认 FILLED，不能重新活动。重复终态可以补充缺失成交事实，不能重复消费／释放。 |
| I2 | **I/O 绝不持锁** | 引擎内应用互斥锁只保护内存快照、决策和发布；其持锁调用树不得等待 HTTP、REST 反查、退避、数据库、上下文读取、遥测或文件 I/O。数据库自身事务／行锁属于持久化原子性机制，不受此应用锁禁令替代，且不得跨 HTTP 或退避。采用内存快照／版本 → 锁外 I/O 与 CAS 提交 → 短锁发布；提交冲突须重新读取或返回等待，禁止覆盖更新版本。 |
| I3 | **Hedge 模式字段解耦** | `plan.reduce_only` 表达业务关闭意图；WS `R` 是交易所字段。`BOTH` 模式保持两者一致校验；`LONG/SHORT` 模式根据 side＋positionSide 校验开／平关系，允许合法关闭回报 `R=false`，且请求不发送 `reduceOnly`。不得削弱 client ID、order ID、symbol、side、positionSide、类型、数量、价格的身份校验。 |

I1 的终态证据必须经过身份与数值校验；缺失字段、超时、单次 GET 不存在、本地 TTL 到期及 unresolved 列表缺席均不构成终态证明。真实持仓只由真实成交与账户证据形成；订单终态不代表仓位归零。

## 2. 改动清单（Exact Files & Function Signatures）

### P1-1：终态被混合时钟过滤

- 文件／入口：`S/live_rollout/order_reconciliation.py`：`async reconcile_account_event(event: AccountEvent) -> None`。
- 行为：先校验身份；有效终态相对非终态具有优先级，不因 WS 交易所时间早于 REST 本地观察时间而丢弃。非终态重放仍按成交水位过滤；累计量不得回退。已终态而账本事实未完整时，允许补事实或恢复原回执。
- 禁止：删除全部去重；用本地接收时间冒充交易所时间；接受身份冲突；让 FILLED 回退；因为较低成交量而覆盖更高水位。相互矛盾的数量须保留既有水位并请求事实恢复。
- 联动核验：`S/persistence/postgres/order_event_repository.py::record_order_observation` 的终态优先与数量单调规则。已有正确规则保留，不以重写存储层代替入口修复。

### P1-2：迟到 REST ACK 重建入场预留

- 文件／入口：`S/live_rollout/pending_entries.py`：`remember(plan, result) -> None`、`observe_order_event(plan, event) -> None`、`sync(context) -> None`；`S/live_rollout/submission.py`：`async execute(candidate, *, …) -> OrderExecutionResult | None`。
- 行为：统一维护同身份的终态屏障；所有观察入口按 I1 合并。`remember` 收到迟到非终态不得重建 `_pending/_uncertain`；已成交但尚未进入批次视图的数量仍保留结算占用，批次观察到后只计一次。数据库缺席本身不能释放未确认订单。
- 禁止：清空全部 pending；用 TTL 或符号级状态代替订单身份；将部分成交撤单视为零成交；新增无界终态缓存。缓存回收后，迟到结果仍须由已有持久化身份／终态证明阻止回退。
- 联动文件：`S/live_rollout/entry_orders.py`：`async track(plan, result) -> None`、`observe(plan, event) -> None`。已确认终态后，迟到 ACK 不得重建到期撤单任务；复用终态屏障，不能另造第二份终态权威。
- Outbox 屏障：`S/domain/execution/command_lifecycle.py::plan_command_transition`、`evidence_lifecycle.py::plan_order_event`；终态后的 mark-ack／mark-unknown／迟到失败均满足 I1，不重复释放预留。

### P1-3：持锁 I/O 与同 key 反查阻塞退出

- 业务入口：`S/live_rollout/submission.py::async execute(…)`；`exit_processor.py::async handle_trigger(…)`、`process_requests(…)`、`_execute_requests(…)`、`_recover_unknown_exit(…)`、`_apply_exit_recovery_observation(…)`。
- 调度入口：`S/execution_account/orders/coordinator.py::prepare_and_execute`、`_execute_prepared_submission`、`_KeyCommandScheduler.submit/_run`；`state_machine.py::submit`、`_query_order_with_retry`；`S/execution_account/binance/client.py::submit_order`、`_resolve_filled_order_snapshot_with_retry`、`_reconcile_submit_parse_failure`、`_ensure_entry_margin_type`、`warm_entry_margin_type`。
- 事务与决策入口：`S/domain/execution/execution_action_transaction.py::accept_action_transaction`、`execution_evidence_transaction.py::observe_evidence_transaction`；`execution_book.py::repair_position/reconcile_reservation_divergence/reload_position/_durable_command_mutation`；`S/live_rollout/decision_facts.py::commit_decision/_dispatch_exit`。
- 行为：按 I2 拆分决策、I/O、发布。任何锁外间隔后通过已有 projection/head/policy revision、数据库 CAS 和原子预留验证新旧版本；事务提交前不发布 candidate。不得因去锁而共享可变的 `_active_transaction` 或把旧 staged copy 整体覆盖新状态。
- 行为：每个命令仍先原子准备，再在唯一 POST 前记录 DISPATCHING／SENDING，派发次数只增加一次。POST 进入不确定结果后持久化 UNKNOWN，释放同 key 派发 worker；反查／退避交给现有 orders／exits 恢复 worker。同 key 退出可在后台 GET 未完成时继续派发。已发送且尚未返回的 POST 不强制抢占；保留明确有限的 HTTP 超时。
- 行为：缺少成交价、响应不可读、调用方在 POST 后取消均保留身份与未知结果；查原 ID，不生成价格或重发 POST。退出重试只能在原单／其他退出单及剩余持仓证据满足现有规则后创建。
- 禁止：只去外层锁、保留 worker 内循环 GET 退避；取消实际 POST 后当成未发送；提前释放未知预留；绕过版本／批次／风险检查；引入新的常驻调度服务。
- 保留 `S/live_rollout/position_lifecycle.py::hold(key)` 的短临界区接口；`S/live_rollout/order_reconciliation.py::run_requested/reconcile_all` 接管恢复，保留超时、工作预算与重试轮转。

### P1-4：Hedge 关闭回报 R=false 被误判身份冲突

- 文件／入口：`S/execution_account/binance/user_data_parser.py::order_snapshot_from_update(order: Mapping[str, object], plan: OrderExecutionPlan) -> ExchangeOrderSnapshot | None`；`client.py::async submit_order(plan) -> ExchangeOrderSnapshot`。
- 行为：按 I3 校验。Hedge 关闭：`SELL+LONG`、`BUY+SHORT`；开仓：`BUY+LONG`、`SELL+SHORT`。关闭意图与方向冲突必须拒绝。WS `R` 仍要求合法布尔类型；只解除 Hedge 模式下与内部关闭意图的直接相等约束。
- 联动核验：`S/live_rollout/order_reconciliation.py::reconcile_account_event`、`account_channel.py::async _process_event(event: AccountEvent) -> Exception | None`、`_process_event_with_retries`；合法关闭回报继续订单及账户事实处理，不触发身份冲突恢复。真实身份冲突仍进入现有保护路径。
- 禁止：Hedge 请求强行添加 `reduceOnly`；全模式忽略 `R`；吞掉全部 ValueError；修改缺失字段及累计量／价格校验。

### P2-1：停止入场与开始平仓时间混用

- 文件／入口：`S/live_rollout/scheduled_risk_window.py::phase(value: datetime) -> ScheduledRiskWindowPhase`、`is_entry_allowed(value) -> bool`；`scheduled_controller.py::async process(*, now=None) -> str | None`、`_next_poll_delay(now) -> float`。
- 行为：新增仅供日程内部使用的 `ENTRY_BLOCKED` 阶段。`[entry_stop, flatten_start)` 停止入场、撤未完成入场单，但不发定时平仓；`[flatten_start, deadline)` 才进入 FLATTENING。等号边界使用后一阶段；默认两项时间相同时无空档。宽限／策略自身退出照常运行。
- 禁止：改默认时间、时区或交易参数；把验证未通过的仓位在 reopen 时放行；以新的全局业务状态表达日程阶段。

### P2-2：无界订单观察队列

- 文件／入口：`S/execution_account/orders/coordinator.py::__init__(…, order_observation_capacity: int = 256, on_projection_recovery_required: Callable[[str], None] | None = None)`；`_defer_order_projection(plan, result) -> None`、`async _consume_order_observations() -> None`、`async aclose() -> None`。
- 接线：`S/live_rollout/execution_runtime.py::build_live_execution_runtime`、`runtime_orchestrator.py::run_live_daemon`；恢复：`command_receipt_recovery.py::recover_restored_commands`、`order_reconciliation.py::request_order_recovery/run_requested/reconcile_all`。
- 行为：容量为正整数，默认 256、可注入测试。保留 ACK 返回不等待次级投影。队满时不等待队列、不使已成功 POST 变成失败；订单回执已持久化，标记该命令待恢复并以同步回调唤醒现有恢复 worker。回调异常不改变交易结果，保留持久化恢复依据。恢复须从已有持久化命令／回执补投影，不能被默认跳过 ACK 的规则挡住。
- 行为：零成交 ACK 可走此异步队列；部分成交、终态、真实成交与结算证据保留原有可靠处理路径。关闭期间停止生产、排空已接收项；未处理项保持可从持久化恢复。每个消费项恰好一次 `task_done`。
- 禁止：静默丢弃；另建无界 list／dict／每项 Task；用队满触发重发 POST；为了入队阻塞同 key 退出；在回调中做 I/O。生产接线必须提供恢复回调。

## 3. 状态收敛定义（State Matrix）

四维分别维护，命令按身份、持仓按 `(environment, account, symbol, positionSide)`。状态由既有事实派生并实际用于读模型、准入／派发校验；不得只声明未使用的 Enum。

| 维度 | 唯一允许的核心状态 | 来源与合法流转 |
|---|---|---|
| 命令执行，5 态 | `PREPARED / SENDING / WORKING / UNKNOWN / TERMINAL` | 已持久化且未派发 → 发送中 → 活动中或终态；发送／活动结果不明 → UNKNOWN；恢复 → WORKING／TERMINAL。TERMINAL 不离开终态。 |
| 持仓，2 态 | `FLAT / OPEN` | 当前持仓事实数量为零／大于零；FLAT 是否可信另看事实维度。入场挂单不等于 OPEN。 |
| 事实可信度，3 态 | `READY / PENDING / CONFLICT` | 原健康度 READY 且满足覆盖／可比较条件 → READY；CATCHING_UP／INCOMPLETE／未知结算 → PENDING；身份／事实冲突 → CONFLICT。完整证据恢复后才可变 READY。 |
| 交易门控，3 态 | `ACTIVE / EXIT_ONLY / HALTED` | 正常准入／只允许通过证据与数量检查的退出／暂停交易。暂停不能停止接收、持久化及恢复事实。预热、风险、日程、流状态保留为门控原因。 |

映射：现有 `DispatchState.PREPARED→PREPARED`、`DISPATCHING→SENDING`、`ACKNOWLEDGED→WORKING`、`UNKNOWN→UNKNOWN`、`REJECTED/TERMINAL→TERMINAL`。Exchange ACK／PARTIALLY_FILLED 为 WORKING；CANCELING 表示撤单动作在途，不新增命令态。终态原因、已成交量、未完成量及批次分配独立保留。

落点：`S/domain/execution/command_models.py` 定义命令维度及兼容映射；`position_ledger_models.py::PositionView` 暴露持仓／事实维度；`S/live_rollout/entry_control.py::LiveEntryControlGate` 暴露门控维度并供现有入口使用。允许新增只读属性，现有持久化字段值与 API 保持兼容；本轮无数据库枚举重命名、历史记录重写或 schema migration。

禁止引入全局单链大枚举、组合枚举如 OPEN_AND_PENDING、通用 EventBus、新 Manager／Dispatcher 代理层。允许 OPEN＋活动入场单、TERMINAL＋PENDING 事实、部分成交撤单＋OPEN、EXIT_ONLY＋恢复进行中。

## 4. 分阶段执行清单（Task Breakdown）

依赖严格为 **T1 → T2 → T3 → T4**。每 Task 提供独立可审查差异、对应验收 ID、测试命令及真实输出；前一 Task 验收通过后才进入下一 Task。文件集合以第 2 节为准，仅增补必要调用点与指定测试，禁止夹带策略／存储治理改动。

| Task | 修改文件／入口集合 | 必须产出与完成条件 |
|---|---|---|
| T1：状态基础与终态屏障 | §3 三个状态落点；§2 P1-1、P1-2 的入口及联动文件 | 四维可用且兼容；入口、本地预留、Outbox、计时器的终态规则一致。A1、A2、G1、G2 通过；P1-1/P1-2 回归用例先在基线失败、改后通过。 |
| T2：Hedge 协议边界 | §2 P1-4 的 parser、client，必要的账户入口调用点 | BOTH／Hedge 分支及开／平方向校验，合法回报完整进入账户事实处理。A4 通过，身份冲突负例仍失败。 |
| T3：锁与恢复职责 | §2 P1-3 的业务、调度、事务、决策、恢复入口 | 列明每个被修改锁的临界区与锁外 I/O；保留 CAS、原子预留及 commit 后发布。超时／缺价反查离开 key worker；A3、G3、G4 通过。 |
| T4：日程与容量边界 | §2 P2-1、P2-2 的入口与接线文件 | 分离停入场和平仓边界；有界 ACK 队列、溢出唤醒、恢复与关闭路径。A5、A6 通过；所有 A／G 用例及受影响现有测试全部通过。 |

## 5. 单元测试与验收矩阵（Acceptance Criteria）

测试使用真实被测入口、真实解析／合并／调度规则；只 Mock 交易所、时钟、遥测和存储边界。异步顺序使用 `asyncio.Event`／屏障，不靠真实 sleep 竞争；所有等待设超时用于检测挂死，不把机器耗时当业务断言。金额与数量用 Decimal。不得通过跳过、删除、宽化断言或只测试新辅助函数满足验收。

### A1：P1-1，WS 终态与 REST 倒挂

测试落点：`tests/unit/live_rollout/test_order_reconciliation.py`；存储回归：`tests/integration/persistence/test_order_terminal_convergence.py`。

| ID | 输入事件流／Mock | 预期断言 |
|---|---|---|
| A1a | repo 返回 ACK，cum=0、REST 观察 t2；有效 WS CANCELED，cum=0、T=t1<t2；spy 实际协调入口 | `apply_observed_snapshot` 收到 CANCELED 恰好一次；不能返回“旧事件”分支。 |
| A1b | 将 A1a 改为 PARTIALLY_FILLED cum=3，再 CANCELED cum=3、t1<t2；计划量 10 | 终态接受，累计量仍为 3；余量 7 的撤单不消除已成交仓位。 |
| A1c | 后续迟到 ACK cum=0，再重放相同终态；另给非终态较低累计量、错误 order ID | 终态不回退，数量不减、不重复结算；较低非终态不应用；错误身份进入冲突保护。 |

### A2：P1-2，终态后的迟到 ACK

测试落点：`tests/unit/live_rollout/test_pending_entries.py`、`test_submission.py`、`test_entry_orders.py`、`tests/unit/execution/test_command_lifecycle.py`。

| ID | 输入事件流／Mock | 预期断言 |
|---|---|---|
| A2a | 数量 10、价格 100；先 `observe_order_event(CANCELED,cum=0)`，再 `remember(ACK,cum=0)`，再 `sync` 空 unresolved／positions | pending=0、uncertain=false、预留名义金额=0、不占槽位；只针对该订单，另一 pending 保留。 |
| A2b | 终态换为 FILLED cum=10，或 CANCELED cum=3；随后迟到 ACK，再提供包含真实对应 entry ID 的批次 | 不重建 pending；批次未到前已成交量保留结算占用，到达后只计一次；部分撤单不保留未成交量 7。 |
| A2c | 通过真实 submission 发起请求；Mock REST 等待 Event，期间送终态，再释放 ACK；GTD 计时器 spy | 不重新记入 pending，不创建终态订单 TTL 任务，不发生再次撤单／POST。 |
| A2d | Outbox 已 TERMINAL，迟到 mark-ack／mark-unknown／失败回调；另测未知订单 TTL 到期且 unresolved 缺席 | Outbox 保持终态、消费／释放不重复；无终态证据的未知订单仍保留预留。 |

### A3：P1-3，I/O 不持锁且恢复不阻塞退出

测试落点：`tests/unit/live_rollout/test_submission.py`、`test_exit_processor.py`、`test_position_lifecycle_locks.py`、`test_decision_exit_lifecycle.py`；`tests/unit/execution_account/orders/test_coordinator.py`、`test_state_machine.py`；事务回归见 G3。

| ID | 输入事件流／Mock | 预期断言 |
|---|---|---|
| A3a | 已有 LONG 仓位；真实 submission＋coordinator＋state machine；entry POST 抛超时，恢复 GET 在 Event 上挂起；提交同 symbol／positionSide 的合法退出 | GET 未释放时退出 POST 已到达 Mock；入场 POST 恰好一次，入场 UNKNOWN 及预留保留，锁可获取；退出量不超过可用持仓。不得用不同 key 代替。 |
| A3b | Mock HTTP、context、telemetry、UoW 查询／提交、退避在入口检查应用锁；分别执行入场、退出、决策退出、观察／修复及 marginType 预热／确认 | 所有 I/O 入口断言应用锁未持有；每条事务仍先 commit 后 publish。 |
| A3c | 同批次数量 1，两个退出并发；或 snapshot v1 后锁外读取期间注入 v2 | 只有可原子预留的命令 POST，累计预留≤1；v1 不覆盖 v2；旧计划冲突时 POST=0 或等待重建，不用通配版本跳过。 |
| A3d | POST 已开始后取消调用方；另一用例为 filled qty>0 但价格缺失，反查 Event 挂起 | POST 总次数仍为 1；未知身份与数量保留；不释放预留、不合成价格；不因迟到结果覆盖新事实。 |

### A4：P1-4，Hedge 字段映射

测试落点：`tests/unit/execution_account/test_user_data.py`、`test_binance_client.py`；`tests/unit/live_rollout/test_order_reconciliation.py`及 `test_account_channel.py`。

| ID | 输入事件流／Mock | 预期断言 |
|---|---|---|
| A4a | 关闭计划 `SELL+LONG+reduce_only=True`，有效 FILLED 报文 `R=false`；对称 `BUY+SHORT`；Mock HTTP 捕获请求体 | parser 返回 FILLED，不报身份冲突；Hedge 请求含正确 positionSide、不含 reduceOnly。 |
| A4b | `BOTH` 关闭计划，分别给 R=true／false；Hedge 关闭计划给错误 side／positionSide、symbol、client ID、已绑定 order ID；R 非布尔 | BOTH true 通过，false 拒绝；Hedge 真实身份／方向冲突仍拒绝；非法类型仍按原规则拒绝。不能取消整段身份判断。 |
| A4c | 经真实 account runtime 输入 A4a 的订单更新及真实 trade ID／quantity／price，再送账户持仓归零 | 订单与成交事实处理均执行，无 identity-conflict recovery；真实成交归属正确，重复 trade ID 不重复入账。不能只断言 parser 通过。 |

### A5／A6：P2 边界

| ID | 输入事件流／Mock | 预期断言／测试落点 |
|---|---|---|
| A5 | Shanghai，stop=07:45、flatten=07:50、deadline=07:55、verify=07:58、reopen=09:00；逐个边界前后及相等值；默认 stop=flatten 对照 | 07:46 禁入场、允许撤入场单、定时平仓 POST=0；07:50 才平仓；09:00 验证未通过仍禁入场；默认无额外等待。`test_scheduled_risk_window.py`、`test_scheduled_controller.py`。 |
| A6a | capacity=2；阻塞 consumer，再产生超容量 ACK；Mock 恢复回调与持久化回执；加入合法退出 | qsize≤2，无额外无界缓冲；溢出命令可从 durable receipt 恢复、回调触发；ACK 返回不等待投影、退出继续，无重复 POST。`orders/test_coordinator.py`、`test_order_reconciliation.py`。 |
| A6b | 消费／恢复回调失败、队列未空时关闭、关闭后调用、模拟重启恢复；capacity=0／负值 | task_done 成对，关闭无挂死，未完成命令可恢复，投影／回调失败不改写成功交易结果；非法容量拒绝，关闭后不接收。与 A6a 同落点，接线由 `test_runtime_orchestrator_lifecycle.py` 覆盖。 |

### G：跨阶段回归与 Review 门槛

| ID | 必须证明的性质 | 现有测试入口 |
|---|---|---|
| G1 | 四维映射实际使用；OPEN＋活动入场、TERMINAL＋PENDING、部分撤单＋OPEN 合法；FLAT 不等于零仓已证实 | `tests/unit/execution/test_execution_book.py`、`tests/unit/live_rollout/test_entry_control.py`，状态模型测试可在同目录增补。 |
| G2 | 同 request/client ID 幂等、累计量与消费／释放单调、真实 trade 去重、未知不释放 | `tests/unit/execution/test_command_lifecycle.py`、`test_evidence_lifecycle.py`、`test_terminal_settlement.py`及订单 coordinator／state machine 测试。 |
| G3 | 数据库 CAS 冲突或 commit 失败不发布；并发命令不超预留；跨进程更新不被旧内存覆盖；事务上下文不串用 | `tests/integration/persistence/test_authority_book_transactions.py`、`test_authority_trace_transactions.py`；对应单元 Mock 控制提交顺序。 |
| G4 | fake-exchange 完整入场→部分成交→撤单→退出→零仓恢复；旧版持久化状态重启兼容；队满积压重启可收敛 | `tests/e2e/test_order_execution_fake_exchange.py`、`test_golden_path_lifecycle.py`及恢复相关测试。 |

交付必须附 A1～A6、G1～G4 与测试名／命令／结果的对应表；新增回归用例先失败后通过的记录；受影响现有测试、Ruff 及相关集成／fake-exchange 测试结果。任何 I1～I3 违反、测试被跳过、未声明接口／schema 变化、恢复依赖未接线、只声明新状态而不使用，均为 Review 不通过。本文不代表这些测试已经编写或执行。
