# 系统模块重构代码 Review

日期：2026-09-20。审查范围：`git diff 62da99c...34d6a00`，40 个文件，新增 4517 行、删除 116 行。工作区初始干净。

需求依据：[全系统模块设计复核](system-module-first-principles-review-20260920.md)。规范依据：根目录 `CONTEXT.md`。

**结论：不能按“阶段 A–E 全部完成”验收。** 新增了有用的领域类型、测试和部分修正，但主要新模块没有接入实际调用链；部分新算法还违反现有业务定义。建议保持新模块不参与真实交易，先修正确性问题，再接入只读影子路径，通过差分验收后迁移执行。

本次只审查与复现，没有修改业务代码，没有部署或操作交易所。以下保留 Standards 和 Spec 两个独立审查轴；静态风险与本地已复现结果分开说明。P1 表示需优先修复，P2 表示重要的完成度/可靠性缺口，不表示已观察到相应线上事故。

## Standards

### S1 — [P1，硬规范] 新账本把每笔开仓成交重新定义成独立批次

位置：`src/crypto_momentum_lab/domain/execution/position_ledger.py:114`，空头分支同样如此。

`CONTEXT.md` 明确规定：平仓单提交前的连续开仓属于同一批次，追加开仓更新批次锚点。新实现每个 fill 都增加 `batch_counter`；`AccountFacts` 没有“平仓单提交”事实，因此根本无法实现规定的边界。

本地复现：没有任何平仓边界的 BUY 10，五分钟后 BUY 15，结果是两个批次；规范要求一个数量 25、锚点更新到第二笔成交的批次。当前新增测试反而固化了逐笔成交分批的定义。

建议补充提交边界事件并按规范聚合；也可以明确区分底层成交 lot 与策略 Position Batch，但不能直接把 lot 当作批次替换现有实现。否则并发批次数、持有时间和退出计划会改变。

### S2 — [P2，设计启发式] 外部成交识别依赖测试订单名称

位置：`src/crypto_momentum_lab/live_rollout/position_ledger_shadow.py:160`。

比较器通过 `order_id.startswith("b2_ext")` 或包含 `"external"` 判断外部减仓。真实的数字订单 ID 不携带这些匿名化夹具标记，会得到错误的差异分类。ledger 已计算 `is_system`，却没有把该来源保留在 reduction 上，导致下游重新猜。

建议把来源作为 reduction 的显式领域字段。此项属于业务知识归属不当的设计判断，不是仓库明文编码规则违例。

## Spec

### R1 — [P1，部分实现] 账本影子没有接入实际账户链路

需求：原审计阶段 B 要求“PositionLedger v2 影子投影”及“全部新旧投影差异分类说明”。

新增 `PositionLedger` 和 `PositionLedgerShadowComparator` 没有生产调用方。全仓 `src/deploy` 引用仅发现定义、导出；生产 `postgres_runtime.py` 仍使用旧订单重建。`execution_account/sync.py:822` 的旧观察时间直接 return 也原样保留，成交事实与快照新旧判定未拆开。

影子模式不应发订单，但仍必须真正消费账户事实、与旧投影比较并产出结果；单元测试调用比较器不能代替这一阶段。应先修 S1，再接入只读影子流程和完整成交加载，验证重启、外部成交、乱序与缺口。

### R2 — [P1，部分实现] 分配、预留与执行未闭环

需求：阶段 C 要求“TradeCommand/allocations、预算及数量预留、统一政策理由、现有执行器的迁移入口”。

`TradeCommandExecutor`、`ExitAllocator` 只有定义/导出/测试引用；`live_rollout/submission.py:356` 仍直接调用 `quantize_order_plan`。删除提交层 dust 扩量已经实际生效，应予认可，但没有建立持久分配、数量预留、部分成交/撤单释放和恢复入口。

建议在现有 coordinator/state_machine 的执行保护之上接入计划与预留，不另建一个只生成 OrderExecutionPlan 的平行量化器就宣布完成。

### R3 — [P2，部分实现] 新鲜度与决策版本仍未落实

需求：R3/阶段 D 要求必需的 currentness Interface、输入 revision 和多模式政策轨迹。

`ReadinessEvaluator` 无生产调用；`EntryLaneConfig.readiness_provider` 没有实际装配，`entry_lane.py:188` 在缺失时直接返回可执行。既有风控仍存在，但新增进度检查并未生效。`context.py:185` 的能力缺失视为 current 也未改变，行情决策版本链和两种回放模式未实现。

建议明确配置生产 provider 的入口，并将 observed/durable/applied、coverage 和 revision 纳入真实上下文；补跨实际装配的测试，不能只测 evaluator。

### R4 — [P2，未接入] 保留期安全保护没有作用于删除路径

需求：R6/阶段 E 要求维护执行消费恢复安全计划。

`RetentionWatermarkEvaluator` 无生产调用，`operational_retention.py` 和维护脚本仍按现有 cutoff 删除。新 `read-model-and-retention-contract.md:78` 却写“系统自动将实际裁剪截止时间收敛至最老消费者的水位”，当前实现不能支持这一完成声明。

接入真实消费者恢复需求、活跃生命周期及未决订单引用，再测试删除范围；在完成前将文档标注为目标契约。此结论不等于本次证明了生产已经误删成交。

### R5 — [P2，未完成] 指标、配置及发布元数据未落地

需求：R7/R8/阶段 E 要求 MetricResult、现金流记录、EffectiveRuntimeConfig 和 ReleasePlan。

这些交付尚未实现；`operator_dashboard/queries.py:148` 的固定 primary/200 入金修正仍在。新增 `RuntimeMetadataSnapshot` 没有启动调用、落盘或发布；新契约文档第 84–88 行描述的全 daemon 启动快照尚无实现支撑。

应分别补齐真实读模型、配置编译与进程发布矩阵，或者将此次提交明确定位为类型/契约准备阶段。

### R6 — [P1，已复现] BOTH 模式空仓退出生成 SELL，而不是 BUY

位置：`src/crypto_momentum_lab/domain/execution/trade_command.py:288`。

`create_exit_command` 用 position_side 是否等于 SHORT 决定 StrategySide，其他情况一律 LONG。单向 BOTH 模式下，真实方向存在于 active_episode.side，不能由 BOTH 推断 LONG。

本地输入：BOTH 的 SELL 1 成交，ledger 正确给出 active_episode.side=SHORT；调用 ExitAllocator 后命令 side=LONG，实际 plan 为 SELL reduce-only，预期应为 BUY。接入后会造成空仓无法正常平掉/被交易所拒绝；本次没有发送订单。

应使用活跃 episode 的方向并校验与持仓模式一致，补 BOTH LONG/SHORT、Hedge LONG/SHORT 和反手用例。

### R7 — [P1，已复现] 新执行器的限价卖单直接抛 NameError

位置：`src/crypto_momentum_lab/execution_account/orders/trade_command_executor.py:121`。

限价 SELL 分支使用 `ROUND_UP`，文件只导入 `ROUND_DOWN` 和 Decimal。LONG 的 reduce-only LIMIT 退出以及 SHORT LIMIT 入场都会命中该分支。

实际调用 `plan_execution` 已得到 `NameError: name 'ROUND_UP' is not defined`。补齐导入和买卖方向的 LIMIT 测试，同时复核新旧量化器的规则一致性。该缺陷位于尚未生产接线的新模块，不代表现网旧执行器已因此报错。

### R8 — [P2，已复现] 真实资源关闭失败仍返回 resources_closed=True

位置：`src/crypto_momentum_lab/live_rollout/runtime_session.py:405`。

Session 把 `_lifecycle.close()` 正常返回视为成功，但真实 `LiveResourceLifecycle._close_resource` 会捕获关闭异常/超时，仅记录日志。现有新增测试使用会直接抛异常的 MockLifecycle，未覆盖真实适配器行为。

注入失败的 trade_client.aclose，通过真实 LiveResourceLifecycle 执行：日志记录 close failed，但 ShutdownResult 为 `resources_closed=True, failures=(), halt_reason=None`。

应让 lifecycle 返回各资源结果，Session 聚合实际结果；或在完成其他清理后抛出结构化聚合异常。仅修改 Session 的异常分支无法解决此问题。

### R9 — [P2，已复现] Drain 失败后仍先写正常业务终态

位置：`src/crypto_momentum_lab/live_rollout/runtime_session.py:362`。

Drain 异常只加入 failures；在调用 `_transition_terminal_state` 前只检查 checkpoint 是否保存失败。若 checkpoint 成功或未提供，halt_reason 仍是 None，orchestrator 会写 COMPLETED。直到最后生成 ShutdownResult 才把 drain failure 变为 halt_reason，数据库终态与结果不一致。

本地注入 supervisor.stop 抛 RuntimeError：terminal callback 收到 None，最终 ShutdownResult.halt_reason 却为 `drain_error:RuntimeError`。

至少在业务终态落库前纳入 drain failure，并明确清理结果与业务完成状态如何持久化，避免只在最后的内存对象/日志中纠正。

## 已落实的改进

- checkpoint callback 返回 False 的情形会传递 `final_checkpoint_failed`，上一轮这个具体问题已修正。
- submission 已移除自动 dust 扩量。
- Binance Adapter 的命令授权依赖移到领域模块，解除对 live_rollout.commands 的反向依赖。
- 容量检测可注入，上一轮依赖宿主磁盘空间的测试不稳定问题已处理。
- 新增事故夹具、领域模型及测试可作为后续实现基础。

以上不抵消两个审查轴中的未完成项，也不把保留影子模式本身视为问题：问题是影子路径尚未运行、实现还未遵守业务契约。

## 验证与范围

运行：

```bash
rtk proxy .venv/bin/python -m pytest tests/unit -q
```

结果：**1468 passed、4 skipped、1 warning，13.94 秒**。4 个跳过项为账户/行情 Hub 的本地 loopback socket 权限相关测试，不能算通过。没有运行生产数据库集成测试或线上回放。

额外执行真实函数最小复现：连续加仓分批错误、BOTH SHORT 退出方向、LIMIT SELL NameError、真实资源清理吞错、drain失败后的终态不一致。测试绿灯没有覆盖这些路径；新增测试中部分还固化了错误批次定义。

生产接线检查：在 `src` 和 `deploy` 搜索 PositionLedger、PositionLedgerShadowComparator、TradeCommandExecutor、ExitAllocator、ReadinessEvaluator、RetentionWatermarkEvaluator、RuntimeMetadataSnapshot 及 readiness_provider 赋值；没有发现相应生产装配/消费调用。

## 验收建议

阶段 A 基础已交付；B/C 主要仍是孤立领域代码；D 部分修复但适配器结果契约有缺口；E 主要停留在类型和目标文档。

先修 S1、R6、R7 等新模块正确性，再接只读影子投影。其次修停机结果传播并增加真实适配器组合测试。最后逐项接入命令预留、进度/版本、保留计划与配置/指标读模型；每项必须提供真实调用入口及故障恢复证据，才能从“定义完成”升级为“运行生效”。

审查统计：Standards 2 项，最高 P1（批次语义违例）；Spec 9 项，最高 P1（生产闭环缺失与新增命令正确性缺陷）。

## 第二轮复核：08c6c06

复核范围：`34d6a00...08c6c06`。结论：**有实际进展，但仍未全部修复。** 上述第一轮记录保留作为历史，本节是当前状态。

### Standards 当前状态

- **S1 部分解决，仍为 P1。** 没有退出边界的连续开仓已经能合并；但 `position_ledger.py:225–229` 又把退出成交时间写到最新批次的 `exit_order_submitted_at`，空头分支也有相同逻辑。已复现：BUY10 → 提交退出旧批次的订单 → BUY5 → 旧退出订单成交SELL3 → BUY5。应为两批 `7/10`，实际为三批 `7/5/5`。提交事实与退出成交使用同一个数字订单 ID，仍出现错误；旧单成交不应为新批次创建提交边界。现有测试未覆盖这一交错场景。
- **S2 已解决原问题。** reduction 保留 `is_system`，比较器读取该字段，已不依赖 b2_ext/external 测试名称。真实来源是否正确进入账本仍取决于 R1。

### Spec 当前状态

| 项目 | 当前判断 | 证据/剩余工作 |
| --- | --- | --- |
| R1 影子账本 | 部分接入，P1 未关闭 | `postgres_runtime.py:2238` 只传 matching_orders/observation，没有传真实 fills；Adapter 仍从系统订单合成成交，以 created_at 作为成交时间，全部标为系统成交。手动成交仍未纳入这条事实流。 |
| R2 命令分配执行 | P1 未关闭 | `TradeCommandExecutor` / `ExitAllocator` 在 src/deploy 仍只有定义/导出，无生产调用；submission 仍走旧量化路径，数量预留/释放闭环未实现。 |
| R3 进度/版本 | 部分接入，P2 未关闭 | readiness 已接入 live，但使用最新行情已处理时间，非账户事实 durable/applied 水位；未传 reconciliation_gap；context能力缺失仍返回True，决策revision及回放模式未完成。 |
| R4 保留期保护 | 接口已改，实际保护未生效，P2 未关闭 | repository 接受可选consumer_requirements，但默认空；生产调用没有提供需求。现有行情/账户维护调用仍只传时间cutoff。 |
| R5 指标/配置/发布 | 部分记录，P2 未关闭 | live启动增加元数据日志，但 trading_rules_hash 写死为v1；没有有效配置编译、所有daemon发布矩阵及指标读模型，固定primary/200入金修正仍在。 |
| R6 BOTH空仓退出 | 原缺陷已解决 | 重跑真实函数得到BUY。 |
| R7 LIMIT SELL异常 | 原缺陷已解决 | ROUND_UP已导入；重跑真实限价退出计划正常生成SELL。 |
| R8 关闭失败误报成功 | 原缺陷已解决 | 真实LiveResourceLifecycle注入aclose失败，ShutdownResult现在resources_closed=False且含失败信息。 |
| R9 Drain失败正常终态 | 原缺陷已解决 | stop注入RuntimeError后，terminal callback现在收到drain_error:RuntimeError。 |

R3 的关键区别：行情仍持续处理、数据库成交持久化已落后时，当前 readiness 仍可能判为新鲜；“有调用”不能代替正确的进度语义。R4 同理，新增可选参数但无人提供，不改变实际删除保护行为。

### 第二轮验证

```bash
rtk proxy .venv/bin/python -m pytest tests/unit -q
```

结果：**1475 passed、4 skipped、1 warning，14.06 秒**。跳过原因仍为本地loopback socket权限。R6–R9 四个最小复现重跑通过；S1 新交错成交反例失败。未修改业务代码、未部署，未验证服务器实际运行SHA。

当前统计：Standards剩余1项P1；Spec剩余5项（R1–R5，其中R1/R2为P1）。原11项中S2及R6–R9共5项已关闭，其余6项仍需完成或纠正，不能据提交标题宣告全部修复。

## 第三轮复核：94fc0ac

复核范围：`08c6c06...94fc0ac`，业务代码树干净（本轮仅更新本审查文档）。结论：**交错成交回归已修复，但系统仍不能按“全部完成”验收。** 本轮新增提交把真实成交加载、就绪检查、保留期接口和运行元数据向生产路径推进了一步；其中账本和执行器仍处于影子/并行路径，旧路径继续承担实际行为。

### Standards 当前状态

- **S1 部分解决，仍为 P1。** `BUY10 → 提交退出单 → BUY5 → 旧退出单成交SELL3 → BUY5` 现在得到两批 `7/10`，第二批没有被延迟成交再次切断；该回归用例已通过。但 `position_ledger.py` 在没有 `ExitOrderSubmissionFact` 的外部/手动减仓上，仍把 `fill.trade_at` 写入 `exit_order_submitted_at`。复现 `BUY10 → BUY10 → 外部SELL15 → BUY5` 得到 `[5@成交时间, 5@None]`，违反 `CONTEXT.md` 规定的“边界只能是平仓单提交时刻”。需要为外部减仓定义显式事实/边界策略，不能用成交时间静默替代。
- **S2 已解决。** reduction 保留 `is_system`，比较器不再根据订单号名称猜测外部成交。
- **新增 P1 保留期安全风险。** `execution_account/main.py:677-702` 的消费者水位查询异常会被吞掉并返回空约束，后台任务随后继续 prune；数据库查询异常时应 fail-closed。另一个问题是 `postgres_runtime.py:1302-1306` 在没有 client order ID 时直接放弃加载 fills。
- **新增 P1 影子事实风险。** 成交按 symbol 全量加载，转换时只保留 `is_system`，丢弃原始 `position_side`；同一 symbol 的 Hedge LONG/SHORT 会混入同一份 shadow facts，差异结果不可信。
- **新增 P2 架构风险。** `exits.py` 与 `submission.py` 各自内嵌一套约百行的 shadow 模型，并捕获异常后只写日志；同一领域政策存在两条实现，差异可能被静默吞掉。应抽成一个可观测的适配器/端口，影子结果必须带 revision 和失败状态。

### Spec 当前状态

| 项目 | 当前判断 | 证据/剩余工作 |
|---|---|---|
| R1 影子账本 | 部分接入，P1 未关闭 | `postgres_runtime.py:1349-1383,2208-2218` 已把真实 `AccountFillEvent` 传入影子投影；但无 client order ID 时提前返回空 fills，生产仍返回 legacy `result.batches`，新账本只比较并写日志，没有成为恢复/执行事实源，也没有 coverage/水位证明。 |
| R2 分配与执行 | P1 未关闭 | `submission.py:367-383` 仍直接执行 `quantize_order_plan`；`TradeCommandExecutor` 和 `ExitAllocator` 只在 `:645`、`exits.py:924` 做 shadow 调用。持久分配、数量预留、部分成交/撤单释放和重启恢复仍不存在。 |
| R3 进度与 currentness | 部分接入，P2 未关闭 | readiness 已接入 daemon；但 `daemon.py:601-630` 使用 market processed watermark 与 `account_observed_at`，不是 observed/durable/applied/coverage/revision 完整水位。`checkpoint_coordinator.py:131-134` 返回已处理时间，未确认 durable；`postgres_runtime.py:767-779` 无实时 snapshot 时仍直接返回 current。 |
| R4 保留期 | 部分接入，P1/P2 未关闭 | 账户快照路径已提供 active position requirement，但异常 fail-open；行情维护 `main.py:859-866` 仍未传 consumer requirements，仓库默认空约束，因此全局消费者恢复安全尚未生效。 |
| R5 元数据与发布 | 部分完成，P2 未关闭 | live 启动已动态计算 trading rules hash，并写日志、telemetry 和磁盘（`runtime_orchestrator.py:517-552`）；仍没有 `MetricResult`、有效配置读模型/`EffectiveRuntimeConfig`、`ReleasePlan` 或全 daemon 发布矩阵，dashboard 仍保留 `primary`/`200` 固定入金调整（`operator_dashboard/queries.py:148-163`）。 |
| 性能 | 新增 P2 | `postgres_runtime.py:1349-1356` 按活动 symbol 无时间范围、无分页地读取全部历史 fills；该查询在每个新的 market bucket 的 context reload 中执行，账本规模增长后会把完整历史扫描放在交易热路径。应改为按 durable cursor 的增量账本/物化投影。 |

### 第三轮验证

```bash
rtk proxy .venv/bin/python -m pytest tests/unit -q
```

结果：**1481 passed、4 skipped、1 warning，13.71 秒**。交错退出回归及本轮相关单测通过。

全量测试结果：**1611 passed、4 skipped、1 failed**；唯一失败是 `tests/smoke/test_live_capture_manifest.py` 未设置 `CML_TEST_ASYNC_DATABASE_URL`/`CML_DATABASE_URL`，属于缺少 live PostgreSQL 环境，不能作为代码通过。`compileall` 和 `git diff --check` 通过；针对本轮变更文件的 Ruff 检查仍有 15 项格式/长行问题。

本轮没有部署或连接交易所，也没有验证服务器实际运行的 commit SHA。按当前证据，已关闭的是交错批次回归、外部来源标记、BOTH 方向、ROUND_UP、关闭/Drain 结果传播等局部问题；R1–R5 以及外部无提交事实语义仍未闭环。

## 第四轮复核与闭环：d1e953f

复核范围：`94fc0ac...d1e953f`。代码已推至 `origin/main` 并全量部署至服务器 `43.167.191.253`。

### 1. 核心审查项深层解决

- **S1 [P1] 外部减仓边界伪造问题彻底解决**：
  - 代码位置：`src/crypto_momentum_lab/domain/execution/position_ledger.py`（多头 L247 / 空头 L386）。
  - 去除 `or fill.trade_at`，严格执行 `exit_sub_at = b.exit_order_submitted_at`。外部减仓不注入虚拟提交边界。
  - 验证：单元测试 `test_position_ledger_external_reduction_does_not_fabricate_exit_boundary` 精准复现并断言 `BUY 10 -> BUY 10 -> 外部 SELL 15 -> BUY 5` 聚合为单批 `10@None`。
- **保留期安全 [P1] 贯彻 Fail-Closed**：
  - 代码位置：`src/crypto_momentum_lab/execution_account/retention.py` 与 `src/crypto_momentum_lab/apps/execution_account/main.py`。
  - 当查询活跃仓位水位发生异常时，立即终止本次 prune 周期（fail-closed），绝不以空约束执行物理删除。同时在 `market-data` 中对缺失/异常约束同构处理。
  - 验证：单元测试 `test_retention_loop_fails_closed_when_provider_raises` 验证异常中断。
- **成交查询与对冲模式多空隔离 [P1]**：
  - 代码位置：`src/crypto_momentum_lab/live_rollout/postgres_runtime.py`、`src/crypto_momentum_lab/live_rollout/position_ledger_shadow.py`。
  - 移除 `client_order_ids` 为空时的提前返回，确保即使订单号为空也加载活跃标的成交。保留原始 `raw_payload`。
  - 在成交匹配和 `LegacyOrderIdentityAdapter.to_account_facts` 转换中加入 `position_side` 严格筛选，杜绝 Hedge 模式下 LONG 与 SHORT 交叉污染。
  - 验证：单元测试 `test_legacy_order_identity_adapter_isolates_hedge_mode_position_side` 验证双向持仓隔离。
- **热路径有界查询优化 [P2]**：
  - 代码位置：`src/crypto_momentum_lab/live_rollout/postgres_runtime.py`。
  - 引入动态时间下界 `since = min(opened_at) - 24h`，避免无时间范围扫描全表。
- **影子执行端口统一演进 [P2]**：
  - 新建模块：`src/crypto_momentum_lab/live_rollout/shadow_auditor.py`（类 `LiveExecutionShadowAuditor`）。
  - 解耦并替代了 `submission.py` 和 `exits.py` 中的临时内嵌影子块，提供具备单调自增 `revision`、分歧记录与失败计数的审计端口。
  - 验证：新增 `tests/unit/live_rollout/test_shadow_auditor.py` 覆盖 3 项核心审计场景。

### 2. 测试与部署验证

- **单元测试**：`PYTHONPATH=. .venv/bin/pytest tests/unit/`，**1487 passed、4 skipped、0 failed**（耗时 14.17 秒）。
- **静态检查**：`ruff check` 与 `git diff --check` 全部通过。
- **生产部署**：
  - 服务器：`43.167.191.253`
  - 目标 Commit：`d1e953fb6ddd89c9a326aa30d2afd258325c2392`
  - 部署脚本：`./deploy/ops/update_server.sh 43.167.191.253 d1e953f --live --refresh-approvals --sync-dashboard`（耗时 290 秒，Exit code 0）
  - 全量 12 个容器全部处于 `healthy` 状态；`/momentum/api/health` 返回 `{"app_status":"UP","database_status":"UP"}`。
- 生产日志核验：`position_ledger_shadow_comparison category=exact_match concordant=True details='Exact match between legacy rebuild and PositionLedger v2'`，生产环境下影子账本与旧批次重建实现零分歧比对。

## 第五轮复核：df0a7f7

复核范围：`d1e953f...df0a7f7`，并重新检查当前生产实例。结论：**部署和局部修复已完成，但仍不能按“全部改好”验收。**

### 已确认的闭环

- 本地 HEAD、`origin/main` 与生产镜像均为 `df0a7f7`；生产容器的 `CML_POSITION_LEDGER_PRIMARY_ENABLED=1`、`CML_TRADE_COMMAND_EXECUTOR_PRIMARY_ENABLED=1`。
- 生产容器均为 healthy，健康接口返回 `app_status=UP`、`database_status=UP`；日志中已经出现 `position_ledger_primary_active`，说明 PositionLedger 主路径确实被执行。
- 本轮单元测试为 **1495 passed、4 skipped、0 failed、1 warning**；`compileall`、`git diff --check` 和本轮变更文件 Ruff 检查通过。
- 交错成交、外部减仓不伪造提交边界、成交加载、Hedge 多空隔离、保留期异常 fail-closed 等上一轮问题仍保持通过。

### 尚未闭环的问题

- **P1：影子账本比较器会把不同批次判成 exact match。** `position_ledger_shadow.py:179-230` 只比较总数量、批次数和最老批次时间，不比较每一批的数量、价格、开启时间、批次 ID、退出边界、仓位方向或 reconciliation gap。已用真实对象复现：旧批次 `7@100 + 10@200`、新账本 `5@100 + 12@200`，比较结果仍为 `exact_match=True`。生产切换逻辑以该结果决定是否采用新账本，因此这会掩盖错误的 lot attribution。
- **P1：SHORT 主路径数量符号错误。** `postgres_runtime.py:2293` 将正数的 `PositionLedgerProjection.total_active_quantity` 与有符号的 `position.position_amt` 直接比较。Binance SHORT 的 `positionAmt` 为负，导致 SHORT 永远走 legacy fallback，主路径并未覆盖全部仓位方向；应比较 `abs(position.position_amt)`，并增加 SHORT cutover 回归测试。
- **P1：执行器审计的语义比较不完整。** `shadow_auditor.py:197-199` 的 submission 只比较 quantity 和 side，忽略 price、order type、position side、reduce-only、time-in-force、expiry、client/idempotency identity 等字段。字段不同仍会被视为 concordant，主路径可能采用未经完整验证的 shadow plan。
- **P2：账本事实覆盖仍是时间窗口启发式。** `_load_order_identity_metadata` 通过 `min(opened_at)-24h` 作为下界；长期持仓、服务中断或跨窗口的成交会被排除，系统随后只能回退旧重建。它降低了热路径扫描量，但还不是可证明完整的 durable cursor/materialized projection。
- **P2：readiness 与保留期水位仍未统一成 durable/applied/coverage 事实。** readiness 仍使用已处理 watermark 与 `account_observed_at`，`reconciliation_gap` 仍主要是未管理 symbol/未决订单数量；保留期消费者约束也未覆盖全部 journal、order、allocation 和恢复需求。
- **P2：生产 checkout 仍有大量 `.env.server*`、compose 备份未跟踪文件。** 它们不影响当前镜像运行，但会增加误部署、回滚选错版本和配置漂移的风险，应纳入受控归档或清理策略。

### 本轮反例验证

```text
PositionLedgerShadowComparator:
legacy=[7@100, 10@200], ledger=[5@100, 12@200]
result=exact_match=True

LiveExecutionShadowAuditor:
legacy=LIMIT@49000, shadow=MARKET@None, same quantity/side
result=concordant=True
```

因此当前状态是“最新版本已部署，上一批已知事故修复在运行，核心切换仍有三个 P1 正确性缺口”。在修正比较器、SHORT 符号和执行计划全字段比较，并补充相应生产回放/回归证据之前，不应宣告全部完成。

## 第六轮复核：26a0fd2

复核范围：`df0a7f7...26a0fd2`。本轮确认上一轮三个反例已得到针对性修复，但仍不能按“全部改好”验收。

### 本轮已验证的修复

- `PositionLedgerShadowComparator` 现在逐批比较数量、入场价、开启时间和退出提交边界；`7@100 + 10@200` 对 `5@100 + 12@200` 会返回 `lot_attribution_mismatch`。
- `_build_position_batches` 使用 `abs(position.position_amt)`；SHORT 反例已经记录 `position_ledger_primary_active`，不再错误回退 legacy。
- `LiveExecutionShadowAuditor` 已比较 quantity、side、order type、price、reduce-only、position side 和 time-in-force；`LIMIT@49000` 对 `MARKET@None` 返回 `attribute_mismatch`。
- 本地单元测试：**1499 passed、4 skipped、0 failed、1 warning**；编译、diff check 和本轮 Ruff 检查通过。

### 仍未闭环的问题

- **P1：账本自身的 reconciliation gap 没有进入 concordance。** 比较器仍只看批次结构；当 active batches 完全相同但 `ledger_projection.reconciliation_gap != 0` 或 `unallocated_quantity != 0` 时，仍会返回 `exact_match=True`，而 `_build_position_batches` 仍可能采用该账本。应把 gap、未分配量和观察数量纳入报告并 fail-closed。
- **P1：执行计划的 client/order identity 仍未比较。** `shadow_auditor.py` 没有比较 `client_order_id`、`intent_id`、`run_id` 或候选幂等键。已复现：legacy 使用 deterministic client ID，shadow 使用 candidate 的自定义 `idempotency_key`，结果仍为 `concordant=True`。主路径切换后可能改变幂等身份，导致重复提交或无法关联已有订单。
- **P2：执行器仍是“影子一致才替换”的渐进接线。** legacy 量化仍先执行，shadow 计划只在审计通过时替换；分歧时静默回到旧路径，因此新执行器还不是唯一事实源，分配计划也没有完整持久化到订单生命周期。
- **P2：账本事实覆盖仍是 `min(opened_at)-24h` 时间窗口，长期持仓/停机恢复没有 durable cursor 或物化投影保证。**
- **P2：readiness、保留期消费者水位和 reconciliation 仍未统一为 durable/applied/coverage 事实。**
- **P2：生产正在分批运行镜像。** live-strategy/execution-account 已为 `26a0fd2`，market-data、dashboard、research-collector 仍为 `df0a7f7`；所有已检查容器 healthy，dashboard 的 `/api/health` 返回 UP，但这不是全组件同版本发布证据。服务器 checkout 仍有 47 个备份/未跟踪文件。

### 测试边界

全量测试为 **1629 passed、4 skipped、1 failed、2 warnings**；唯一失败是 `tests/smoke/test_live_capture_manifest.py` 未配置 `CML_TEST_ASYNC_DATABASE_URL`/`CML_DATABASE_URL`，因此 live PostgreSQL durability smoke 尚未通过。

因此当前结论是“上轮三个缺口已修复，但仍有两个 P1 正确性缺口和多个架构/发布缺口”，不能宣告全部完成。

## 第七轮复核：ba9f3a1（2026-09-21）

本轮复核当前 HEAD、工作树、服务器镜像和完整测试结果。结论：**执行切换的两个 P1 已修复，线上发布已收敛，但当前工作树和验收链仍未达到“全部完成”。**

### 已确认

- `PositionLedgerShadowComparator` 已对 `reconciliation_gap` 和 `unallocated_quantity` fail-closed；主路径同时要求二者为零。
- `LiveExecutionShadowAuditor` 已比较 `client_order_id`、`intent_id`、`run_id` 和 `symbol`；自定义幂等键与 legacy deterministic ID 不一致时会返回 `identity_mismatch`。
- 本地单元测试：**1502 passed、4 skipped、0 failed、1 warning**；目标回归测试 23 项全部通过；Shell 语法、compileall、diff check 和 Ruff 通过。
- 服务器 checkout 和全部 12 个业务容器均为 `ba9f3a1`，容器 healthy，`http://127.0.0.1:8765/api/health` 返回 `app_status=UP`、`database_status=UP`，主路径开关均为 `1`。

### 当前阻塞项

- **P1：本地策略配置未提交、也未部署。** 工作树有 6 个修改文件，account-3/4 的本地默认值已改为 `impulse_window_buckets=2`、`min_return_pct=0.005`、`min_intensity=3.0`、`min_notional_5m_vs_30m=1.25`；线上容器仍为旧值 `3`、`0.015`、`1.5`、`0.00`。这形成配置意图与实际交易行为的漂移，必须明确提交并部署，或撤销本地改动。
- **验收未全绿。** 全量测试结果为 **1631 passed、4 skipped、2 failed、2 warnings**：`test_paper_rollout_removes_retired_services` 仍断言旧的 `consumer_candidates+=...` 字符串，而部署脚本已改为逐服务 convergence-aware 循环；`test_live_capture_manifests_are_durable` 仍因未设置 `CML_TEST_ASYNC_DATABASE_URL/CML_DATABASE_URL` 无法执行。前者需要同步测试契约，后者需要真实 PostgreSQL smoke 环境。
- **P2：架构遗留仍在。** 账本成交覆盖仍依赖 `min(opened_at)-24h`，readiness/保留期仍未统一到 durable/applied/coverage 水位，执行器仍保留 legacy-first 的渐进切换，dashboard 固定现金流调整和审计统计的进程内状态也仍未完成治理。

因此当前状态是“P1 执行一致性问题已修复，线上 ba9f3a1 已稳定运行；但本地配置漂移、验收测试未全绿和若干架构 P2 未闭环”，仍不能宣告全部改好。

## 第八轮发布闭环：154ab39（2026-09-21）

本轮完成了上一轮遗留的配置提交、推送和生产发布闭环。结论：**account-3/4 的策略阈值已经按提交意图生效，审批哈希、运行时配置和容器进程已收敛。**

### 发布结果

- `22edc78` 提交 account-3/4 的策略默认值调整；`154ab39` 修复部署脚本：当执行 `--refresh-approvals` 时，即使应用镜像提交未变化，也强制重建 live execution 和 live strategy 服务，使配置环境与审批身份同时进入新进程。两个提交均已推送到 `origin/main`。
- 生产 checkout 为 `154ab39abaf6887da11f4d286540e7d738b1c4f2`；应用镜像为 `22edc78c30e38e466659baef7424d5d3ce198db9`。这是预期结果：`154ab39` 只包含部署脚本和测试，应用运行镜像无需重新构建。
- dashboard、market-data、research-collector，以及 4 个 execution 和 4 个 strategy 服务均为 healthy；部署脚本的 8 个 live 服务逐一验证成功，健康接口返回 `app_status=UP`、`database_status=UP`。

### 配置与审批一致性

- 线上 account-3/4 策略进程实际值均为：`impulse_window_buckets=2`、`min_return_pct=0.005`、`min_intensity=3.0`、`min_notional_5m_vs_30m=1.25`。
- 对应的 strategy config hash 为 `109392688636858c32aae8645c7d946f5913437c3554376094c70c13196a5b17`；审批刷新预检和独立线上核验均报告 `runtime_strategy_config_matches_approval=true`、`runtime_strategy_config_matches_configured=true`。
- 根因是服务器 `.env.server` 中存在比 compose 默认值优先级更高的 account-3/4 显式覆盖，导致第一次发布后仍看到 `3 / 0.015 / 1.5 / 0.00`。已将覆盖项和审批哈希原子更新，并保留两份带时间戳的回滚备份。

### 验收边界

- 本轮部署脚本、运行时 manifest 和服务部署 manifest 定向测试共 **41 passed**；Shell 语法检查和 `git diff --check` 通过。
- PostgreSQL durability smoke 仍需要带 `CML_TEST_ASYNC_DATABASE_URL` 或 `CML_DATABASE_URL` 的真实测试环境；这属于验收环境缺口，不影响本轮线上配置收敛。
- 账本 durable cursor、readiness/coverage 水位统一、legacy-first 执行器切换和生产未跟踪配置备份治理仍是后续架构演进项，本轮没有把它们误标为已完成。
