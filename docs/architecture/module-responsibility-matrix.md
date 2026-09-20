# 全系统核心模块责任与不变量矩阵 (Module Responsibility Matrix)

依据 [system-module-first-principles-review-20260920.md](file:///Users/zhangshuai/PycharmProjects/crypto-momentum-lab/docs/architecture/system-module-first-principles-review-20260920.md) 的第一性原理分析，本文形式化沉淀系统重构后各核心模块的职责边界、必须守住的不变量、唯一权威写入者、输入/输出保证及故障恢复契约。

---

## 1. 模块责任矩阵总表

| 模块名称 | 必须守住的不变量 (Invariants) | 唯一权威写入者 (Authoritative Writer) | 身份维度 (Identity Keys) | 关键输入与版本 | 输出保证 (Output Guarantees) | 断点恢复与回放契约 (Recovery & Replay) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`PositionLedger`**<br>(R1 账户事实账本与批次投影) | 交易所实际持仓量 = 已归属活跃批次总量 + 未分配量/差额；差额必须可解释，严禁通过快照静默裁剪抹平事实。 | `PositionLedger`<br>(通过不可变成交事实重放) | `account_label`<br>`symbol`<br>`position_side`<br>`episode_id`<br>`batch_id` | 规范化 `AccountFillEvent` 序列、带时戳的 `PositionObservation`、覆盖区间 `coverage` | 输出耐久的活跃批次投影、历史生命周期归档及差额对账证据。 | 以 Zero-crossing（归零/反手事件）或已确认的 Checkpoint 水位为锚点向后重放；外部成交不可丢。 |
| **`CommandPlanning` & `ExitAllocator`**<br>(R2 交易命令与退出分配) | 退出命令数量必须严格等于明确分配的批次数量总和；执行层不得擅自因 dust 或尾数扩大业务下单量。 | `ExitAllocator`<br>(决策分配)<br>`TradeCommandExecutor`<br>(执行状态机) | `client_order_id`<br>`fencing_token`<br>(`lease_id` + `code_generation`)<br>`idempotency_key` | 业务意图、分配策略、活跃批次快照、账户风险限额 | 幂等的订单提交意向，状态机严格推进（PENDING -> SUBMITTED -> FILLED/REJECTED）。 | 进程重启或超时后，优先通过 exchange 查询或对账流恢复未决订单真实状态，严禁并发重发。 |
| **`MarketData`**<br>(R3 数据版本、完整性与进度) | 同一时间桶的实时决策可见版本（`observed`）与最终归档修订版本（`canonical`）必须带独立 Revision 标识，不可互相冒充。 | 实时 Hub<br>(流式推送写入)<br>Window Materializer<br>(权威落盘写入) | `symbol`<br>`bucket_start`<br>`revision_id`<br>`progress_stage`<br>(observed / durable / materialized) | WS 实时事件流、REST 补洞数据源、时钟关闭信号 | 实时管道保证低延迟推送；物化管道保证 Parquet 窗口原子去重落盘。 | 补洞或重放明确声明模式：重现决策所见（Mode Decision-Visible）或全面清洗数据（Mode Canonical）。 |
| **`RuntimeSession`**<br>(R4 生命周期与终态语义) | 每个系统资源有且仅有一次权威注册；检查点落盘失败（`checkpoint_durable=False`）绝对不得报告业务 `COMPLETED`。 | `RuntimeSession` | `run_id`<br>`session_state`<br>`shutdown_result` | 任务 Supervisor、资源所有权注册表、检查点保存回调、终态转换回调 | 严格执行有序 4 阶段停机（DRAINING -> PERSISTING -> CLOSING -> STOPPED），产出结构化 `ShutdownResult`。 | 检查点失败强制转入 `HALTED(reason="final_checkpoint_failed")`，下次启动强制进入恢复分支。 |
| **`PolicyEngine`**<br>(R5 统一决策政策核心) | 相同输入数据版本 + 相同政策配置指纹 = 唯一确定的一致性 `DecisionTrace`；模拟执行与实盘执行的区别仅落在执行适配器。 | 领域纯函数 / `PolicyEngine` | `strategy_id`<br>`policy_version`<br>`decision_id` | 版本化行情特征、活跃批次投影、有效风控规则 | 输出包含完整理由、输入版本、目标批次及候选数量的 `DecisionTrace`。 | 纯函数无内部隐式状态，通过历史决策输入夹具可实现 100% 离线确定性重放。 |
| **`ResearchCollector & Retention`**<br>(R6 归档、物化与保留期) | 数据保留清理（Trimming）必须满足下游消费者的最小恢复水位（Consumer Recovery Watermark），未覆盖前严禁删除。 | `ArchiveJournal`<br>`ParquetWindowSink`<br>`RetentionPlan` | `stream_id`<br>`record_id`<br>`receipt_token`<br>`retention_epoch` | 实时接收事件、已物化确认收据、消费者最小恢复依赖凭证 | 先写 Journal WAL，再单 Writer 批量物化至分层 Parquet 文件。 | 崩溃重启时扫描 Spool/Pending 与未确认 Journal 记录，原子重建物化窗口，保证幂等去重。 |
| **`AccountPerformance`**<br>(R7 收益口径与健康读模型) | 出入金/资金划转（Cash Flow）必须与策略交易盈亏严格解耦；指标必须携带数据观察时间、口径版本与覆盖率。 | 指标投影服务 / 读模型构建器 | `metric_id`<br>`account_label`<br>`source_asof`<br>`metric_version` | 成交事实、资金流水审计表、带时戳的权益快照 | 读模型只读输出，明确区分未调整权益变化与现金流调整后时间加权收益率。 | 数据缺失时显式标记 `status=UNKNOWN` 或 `NO_DATA`，严禁隐式退化为零。 |
| **`ConfigCompiler`**<br>(R8 有效配置与部署状态) | 运行配置在启动时一次性编译为不可变 `EffectiveRuntimeConfig`；策略参数指纹与部署环境指纹解耦。 | `ConfigCompiler` | `effective_config_hash`<br>`strategy_fingerprint`<br>`deployment_fingerprint` | Base YAML 配置、Profile 覆盖、环境变量、CLI 入参 | 不可变的强类型配置对象，明确各字段解析来源与继承覆盖链条。 | 启动阶段校验当前配置指纹与数据库中最后一次成功运行指纹的兼容性，不兼容则阻断启动。 |

---

## 2. 第一性原理核查六问（模块实现自检清单）

任何针对上述模块的新建代码或重构 PR，必须通过以下 6 个问题的审查：

1. **职责分离**：该模块是在处理不可变事实（Fact）、派生视图（View/Projection）、业务决策（Decision），还是外部副作用（Side-effect）？是否混在同一个类/函数中？
2. **单一权威写入者**：该状态的唯一法定权威是谁？是否存在其他模块通过“启发式”、“窗口补丁”或“反向修改”试图修饰它的行为？
3. **身份完整性**：身份键是否完整？是否丢失了 `account`、`side`、`stream_epoch`、`policy_version` 或 `data_revision` 中的任意一项？
4. **成功语义可信度**：返回值为 `True` 或执行结束，意味着已被接收（Received）、已持久化落盘（Durable）、已应用到内存（Applied），还是已完成全部外部副作用（Completed）？调用方是否忽略了返回值？
5. **任意断点可恢复**：在任何 `await`、数据库提交或外部 API 请求前后突然断电/kill -9，能否依据现有持久化事实无歧义恢复？
6. **改动爆炸半径**：修改一条业务规则（如外部平仓归属、最小减仓量量化），需要同步修改几个模块？是否依然依赖在调用方打补丁？
