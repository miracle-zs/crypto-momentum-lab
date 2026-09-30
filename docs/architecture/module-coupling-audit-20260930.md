# 模块化、内聚度与耦合审查

审查基准：当前 `main` 工作树，提交 `a18d262`
审查日期：2026-09-30

## 审查范围与方法

- 静态扫描 `src/crypto_momentum_lab` 下 314 个 Python 模块，解析内部 import 关系，并按顶层包检查依赖方向。
- 检查 Python 文件行数、主要类/函数跨度，以及端口、构造器和数据库访问接口。
- 未运行测试、服务或生产查询；结论反映当前提交可见的静态结构，不代表运行时行为已经验证。
- 对循环依赖的表述区分了顶层包依赖、模块级引用和延迟导入。静态依赖环本身不证明运行时导入失败。

### 当前依赖图结论

当前扫描未发现 `domain` 对 `persistence`、`market_data`、`execution_account`、`live_rollout`、`apps`、`tools` 或 `strategy_runner` 的出站 import。扫描也未发现跨顶层业务包的循环依赖。

扫描发现一个位于 `domain.execution` 内的模块级强连通分量：`position_ledger_models`、`recovery_models` 和 `recovery_codec`。其中一条依赖来自 `TYPE_CHECKING`，另一条经由函数内延迟导入；当前代码没有因此出现运行时故障的证据。将包 `__init__.py` 纳入模块图后，还存在 `operator_dashboard → api → queries → operator_dashboard` 静态引用环。2026-09-30 在独立 Python 进程中分别导入 Dashboard 包、API、queries 及 recovery models/codec 均成功；该检查只验证导入，不验证业务运行。

因此，旧审查中“`domain ↔ persistence`、`domain ↔ market_data` 包级循环”的结论不适用于本次审查所见的 `a18d262`。当前证据显示边界问题主要发生在 `live_rollout` 应用代码与 SQLAlchemy/持久化实现之间。行数也按当前工作树重新统计，例如 `src/crypto_momentum_lab/live_rollout/postgres_runtime.py:1-1602`。

## 1. 架构原则评价（主观评分）

以下分数是维护性判断，不是通过量化标准计算出的质量指标。文件长度、方法数量只能说明实现规模，不能独立证明低内聚、运行故障或重构收益。

| 维度 | 评分 | 依据 |
| --- | ---: | --- |
| 模块化架构（Modularity） | **7/10** | 顶层包职责可辨，领域层对外设有端口；实时编排模块仍同时承担用例编排与数据库适配。 |
| 代码内聚度（Cohesion） | **4/10** | 多个关键类和入口覆盖多个生命周期；最大领域服务 `ExecutionBook` 实现规模较大，但其统一状态所有权具有明确设计依据，需进一步评估内部职责拆分。 |
| 模块低耦合度（Coupling） | **6/10** | 领域包依赖方向良好；部分 `live_rollout` 模块直接依赖 SQLAlchemy、ORM 行和具体 Postgres 实现，另有动态能力探测。 |

## 2. 关键代码实证

### 良好模块化示例

- **领域端口隔离持久化能力**：`src/crypto_momentum_lab/domain/execution/ports.py:72` 定义 `ExecutionTransactionPort`，`src/crypto_momentum_lab/domain/execution/ports.py:161` 定义 `ExecutionUnitOfWorkPort`。`ExecutionBook` 以该端口声明 UoW 依赖：`src/crypto_momentum_lab/domain/execution/execution_book.py:569`。
- **市场数据端口按能力划分**：`src/crypto_momentum_lab/domain/market/ports.py:14` 的 `RawArchive` 与 `src/crypto_momentum_lab/domain/market/ports.py:23` 的 `CaptureRepository` 将归档和采集状态存储分别抽象。
- **HTTP 层通过查询协议注入**：`src/crypto_momentum_lab/operator_dashboard/api.py:262` 定义 `DashboardQueryProtocol`；`src/crypto_momentum_lab/operator_dashboard/api.py:330` 的 `queries` 参数接收该接口实现。查询实现集中在 `src/crypto_momentum_lab/operator_dashboard/queries.py:252`；路由通过查询接口委托，例如 `src/crypto_momentum_lab/operator_dashboard/api.py:608-612`。

### 高内聚实现示例

- **风险评估职责集中**：`src/crypto_momentum_lab/risk/gateway.py:37` 的 `RiskGateway.evaluate` 接收意图和 `RiskContext`，返回风险评估；它依赖领域类型，不访问 ORM 或网络客户端。
- **订单状态规则独立**：`src/crypto_momentum_lab/execution_account/orders/state_machine.py:161` 定义 `OrderExecutionStateMachine`；实时上下文适配器在 `src/crypto_momentum_lab/live_rollout/postgres_runtime.py:45` 引用 `SubmitPolicy`。该状态机可作为继续维持独立边界的例子。

### 解耦/抽象接口示例

- **持仓执行用例与存储分离**：`src/crypto_momentum_lab/domain/execution/ports.py:161` 暴露 `ExecutionUnitOfWorkPort`，避免领域模块导入 Postgres 实现。
- **Dashboard 查询可替换**：`src/crypto_momentum_lab/operator_dashboard/api.py:262` 声明查询协议，应用装配处注入 `queries`，便于替换实现或隔离 HTTP 层。

## 3. 典型违例清单

### 1. 实现规模过大 / 内部职责与编排复杂度

- `src/crypto_momentum_lab/domain/execution/execution_book.py:551-3843`：`ExecutionBook` 共 3,293 行、48 个直接方法。它统一维护位置簿与 journal，并覆盖恢复、读视图、执行请求、outbox 状态迁移、预留数量结算和账户证据应用；`restore`、`act`、`observe` 分别从 `:1246`、`:1865`、`:2699` 开始。所有能力围绕执行状态。[已有架构蓝图](system-refactor-blueprint-20260925.md) 第 5 节明确选择由 `read/act/observe` 封装事实、批次、预留、命令和恢复；因此不能仅按长度认定低内聚或上帝类。维护风险在于内部实现规模与路径复杂度，应通过不变量归属、变更影响和测试边界评估协作者提取。
- `src/crypto_momentum_lab/live_rollout/runtime_orchestrator.py:342-1880`：`run_live_daemon` 跨 1,539 行，负责装配并驱动数据库仓储、账户/交易所适配器、daemon、行情源和监督任务。运行编排与资源装配、生命周期控制集中在一个函数，增加理解和局部修改成本。
- `src/crypto_momentum_lab/apps/live_rollout/main.py:331`：CLI 文件共 2,592 行，包含 `prepare`、`renew-lease`、`approve`、`preflight`、`resolve-missing-order` 等命令入口；对应处理函数从 `:332`、`:557`、`:600`、`:952`、`:1063` 起。入口层承担了较多应用流程，不利于独立复用命令背后的用例。
- `src/crypto_momentum_lab/live_rollout/postgres_runtime.py:184-1385`：`PostgresLiveContextProvider` 共约 1,202 行、26 个直接方法；上下文装载、账户持仓投影、交易规则和缓存失效等职责集中在同一适配器中。文件本身还包含 SQLAlchemy 导入（`:10-11`）、运行时轮询及其他辅助函数。

### 2. 强耦合 / 抽象缺失 / 模块级循环

- `src/crypto_momentum_lab/live_rollout/position_self_healing.py:21-22`：用例模块直接导入 SQLAlchemy `select`、`AsyncSession`；`:37-43` 导入具体 Postgres journal store 和私有转换/摘要函数；`:47-52` 直接依赖 ORM 行模型；`:59-63` 将 `AsyncSession` 和 Postgres store 放进用例函数签名。`:76-82` 起直接通过 session 执行查询。数据库映射和重建策略因此无法独立替换。
- `src/crypto_momentum_lab/live_rollout/decision_facts.py:43-48`：实时决策协调代码直接依赖 `AsyncPostgresDecisionUnitOfWork`、`DecisionCommit` 和 `DecisionCommitReceipt`。决策用例与持久化实现及其数据类型绑定。
- `src/crypto_momentum_lab/live_rollout/order_facts_loader.py:12-19`：加载器直接依赖 `AsyncSession` 和 SQLAlchemy ORM 行，函数签名见 `:61-67`；`src/crypto_momentum_lab/live_rollout/position_classification.py:45-52` 和 `src/crypto_momentum_lab/live_rollout/order_identity.py:20-26` 继续把 ORM 行及持久化 DTO 带入实时业务包。持久化结构已成为上层分类与身份解析逻辑的输入契约。
- `src/crypto_momentum_lab/domain/execution/execution_book.py:567-584`：仓储参数被标成 `Any | None`，构造器用 `getattr` 和 `inspect.iscoroutinefunction` 判断能力，再将传给 coordinator 的局部 `coordinator_repository` 设为 `None`；原仓储仍保存在 `self._reservation_repo`。这种动态形状探测让依赖契约不明确，也增加排查实际装配路径的难度。重构应使用必需的具体 Protocol 类型，由装配点选择唯一实现，不保留运行时探测或兜底分支。
- `src/crypto_momentum_lab/domain/execution/position_ledger_models.py:32-35` 通过 `TYPE_CHECKING` 引入 `recovery_models`；`src/crypto_momentum_lab/domain/execution/recovery_models.py:14-22` 反向引用 ledger models，且 `:411-419` 在 digest 函数中导入 codec；`src/crypto_momentum_lab/domain/execution/recovery_codec.py:29-39` 又依赖 recovery models，`:1271-1276` 再延迟导入 digest 函数。依赖关系虽然通过延迟导入运行，但投影摘要职责在模型和 codec 之间往返。

### 3. 模块边界击穿 / 数据访问与用例编排混杂

- `src/crypto_momentum_lab/live_rollout/postgres_runtime.py:10-11` 直接导入 SQLAlchemy，`:45-46` 又依赖账户同步与订单策略类型；同一文件中的 `PostgresLiveContextProvider` 负责 SQL 上下文装载和业务运行时状态缓存（`:184-1385`），文件后段还定义运行时轮询（`:1505-1512`）。数据库适配与 live rollout 用例的边界不够清楚。
- `src/crypto_momentum_lab/persistence/postgres/order_repository.py:270-1502` 的 `PostgresOrderRepository` 共约 1,233 行、20 个直接方法。范围从意图与提交准备（`:277`、`:305`）延伸到订单事件与成交（`:857`、`:992`）、执行水位（`:1221`）及对账事件（`:1473`）；一个仓储同时承载多个存储生命周期。
- `src/crypto_momentum_lab/operator_dashboard/queries.py:14-35` 通过 `crypto_momentum_lab.operator_dashboard` 包命名空间导入多个同级 query 模块，而 `src/crypto_momentum_lab/operator_dashboard/__init__.py:1` 又重导出 API，API 在 `src/crypto_momentum_lab/operator_dashboard/api.py:22` 导入 `queries`。此写法涉及包门面初始化，包级重导出形成静态引用环。改为 `from . import ...` 仍依赖同一包，不能保证消环。应优先减少 `__init__.py` 对 API 的急切重导出，并将协议与应用装配分开；当前独立进程导入成功，未证实运行时失败。

## 4. 最小代价重构方案

重构约束：保持当前公开用例入口及状态不变量，优先抽离纯计算与适配逻辑，避免仅按文件行数拆分。接口迁移按调用链一次完成，依赖必须显式且具名；不保留兼容包装、可选实现、运行时能力探测或兜底分支。构造器和用例签名使用确切 Protocol 与 DTO。

### P1：抽离 live rollout 用例中的 SQL 和 ORM；一致性缺陷优先处理

先核查 `position_self_healing` 与正常执行写入是否共享锁、事务和 revision 冲突协议，再处理 `decision_facts`、order facts/classification 的接口迁移。只有发现明确的数据一致性或交易安全缺陷时，才按其影响与紧迫程度提高优先级；直接依赖 SQLAlchemy 本身不足以定为 P0。

分类、身份解析与修复计算应使用不包含 ORM 的应用 DTO。Postgres adapter 负责 `AsyncSession`、ORM 转换及序列化；runtime factory 等装配点可以依赖具体实现。`postgres_runtime` 作为明确的 Postgres adapter 依赖 SQLAlchemy 并非自动构成违例，需要拆的是其中混入的业务判断和用例编排。

自愈端口必须规定：读取修复事实、校验当前 head/epoch、写入去重身份与 journal、更新 head 均在同一持仓事务中完成；所有写入路径使用一致的持仓锁键，并保留 revision 冲突校验、幂等性与失败回滚。现有正常执行 UoW 使用 `execution_position:{key.canonical_id}` 事务锁；不能仅新增独立锁而让正常写入绕开它。

以下是目标契约示意，不是可直接替换的完整实现：

```python
class PositionRepairTransaction(Protocol):
    async def load_repair_facts(self, key: PositionKey) -> PositionRepairFacts: ...

    async def persist_repair(
        self, repair: PositionRepair, *, expected_revision: int
    ) -> PositionRepairReceipt: ...


class PositionRepairUnitOfWork(Protocol):
    # Adapter acquires the shared position lock before yielding.
    # Commit on successful exit; rollback on error or revision conflict.
    def transaction(
        self, key: PositionKey
    ) -> AsyncContextManager[PositionRepairTransaction]: ...


async def heal_position(
    key: PositionKey, uow: PositionRepairUnitOfWork
) -> PositionRepairReceipt:
    async with uow.transaction(key) as tx:
        facts = await tx.load_repair_facts(key)
        repair = build_position_repair(key, facts)
        receipt = await tx.persist_repair(
            repair, expected_revision=facts.head_revision
        )
    return receipt
```

revision 冲突后必须重新读取事实并重算，使用有界重试，不能直接重放旧 repair。数据库提交后，还须调用完整的 Book 重载校验并发布内存状态，才可宣告 healed；提交成功但重载失败必须保留可观测的恢复阻塞，禁止提前宣告 ready。验收覆盖并发正常 observe、自愈重复执行、同/跨 epoch、事务回滚以及提交后重载失败和重启恢复。

`decision_facts` 的端口须覆盖当前消费者实际使用的 `load_or_import_policy_state`、`commit_decision`、`load_pending_exits`、`mark_exit_superseded` 和 `mark_exit_dispatched`，可按读取、原子决策提交、退出恢复能力划分。`DecisionCommit`、receipt 和政策快照是应用契约值，虽当前定义在 persistence 文件中，并非 ORM；应移至不依赖 Postgres 的契约模块，保留校验。决策提交仍须原子保存政策状态、trace、依赖及退出 outbox，并保留 policy revision、摘要冲突检查与重放幂等语义。

端口与 DTO 放在不依赖应用编排或 Postgres 的契约模块；Postgres adapter 通过结构化 Protocol 满足契约，避免为声明实现关系而新增 `persistence → live_rollout` 包依赖环。所有调用点同步切换，不添加旧签名转发层。

### P1：把 ExecutionBook 拆为显式协作者

让 `ExecutionBook` 保留用例入口和状态协调，把恢复校验、命令/outbox 生命周期、证据应用分别迁入专职协作者。让协作者通过 `ExecutionUnitOfWorkPort` 和其他明确的 Protocol 协作。移除 `Any | None`、`getattr` 和协程形状探测；装配处必须提供唯一、类型明确的实现。纯内存实现也应显式实现相同契约，避免隐藏默认行为。

协作者不得各自开启独立事务或持有可分叉的权威状态。`ExecutionBook` 保留统一 mutation lock、候选状态隔离及提交成功后发布语义；facts、reservation、outbox、watermark 和 head 的原子边界不得因提取协作者而拆开。公开 `read/act/observe` 入口继续作为唯一状态协调入口。

建议顺序：先迁移恢复/检查点计算，再迁移 outbox 与 reservation 生命周期，最后迁移 observe 中的证据归并；每一步同时改调用点和接口，不保留双路径。

### P1：拆分 Postgres 聚合仓储与 CLI/运行时装配

- 将 `PostgresOrderRepository` 按意图提交、订单事件/成交、执行命令/水位与对账职责拆成独立仓储，并通过显式 Unit of Work 保留需要原子提交的边界。
- 将 `PostgresLiveContextProvider` 中的 SQL 查询和 ORM 到领域值的映射移动到 `persistence/postgres`；live rollout 保留上下文流程与缓存策略。
- 将 `run_live_daemon` 中的对象构造移入明确的 runtime factory，daemon 函数仅负责生命周期和事件编排。CLI 命令函数委托给可直接调用的应用用例。
- 减少 Dashboard `__init__.py` 对 API 的急切重导出，分离查询协议与应用装配，并验证常用导入顺序；相对导入只能改善可读性，不作为消环验收依据。

### 当前优先级

| 优先级 | 动作 | 预期边界变化 |
| --- | --- | --- |
| 先核查；按风险定级 | 自愈与正常执行的锁、事务、revision、重载一致性 | 若发现缺陷，先修复并发/恢复契约，再做结构迁移 |
| P1 | 抽离分类、身份解析和决策用例中的 ORM/具体 Postgres 依赖 | 用例通过明确端口与应用 DTO 协作，装配点保留具体实现 |
| P1 | 拆分 `ExecutionBook` 的恢复、outbox、证据应用职责 | 核心执行状态变化具备清晰的局部所有权 |
| P1 | 拆分 Postgres 仓储和 live runtime 装配 | 数据读写生命周期与 daemon/CLI 生命周期分开 |
| P2 | 清理模块级延迟循环和 Dashboard 包门面导入 | import 方向更直接，模型/codec 职责更清楚 |

## 5. 同版本服务器核查

2026-09-30 北京时间 10:14–10:17 已完成同版本服务器只读核查。部署身份与本文一致，但容器 healthy 不代表执行链路完整；当前观察到 checkpoint 停滞、重复自愈和事件队列溢出。详细事实、口径限制与处理顺序见[服务器状态核查](server-state-check-20260930.md)。生产采样不改变本报告静态审查的证据边界。

后续已修正当前持仓分类与历史决策截点混用、回放预取实时上下文，以及健康视图原因/事实缺口误报。后续补充了退出派发等待不终止消费的修复，运行版本 `259c8e0` 已发布；业务与部署回归合计 328 个测试通过。四账户消费与 checkpoint 已恢复，但事实覆盖冲突、策略状态发布缺失仍存在（账户 3 的三条旧退出已依据交易所证据定点恢复），不能宣告系统整体恢复。结构性 SQL/ORM 迁移与大类内部拆分仍待按本文契约实施。


## 6. 实施进度

从 `8eb059d` 开始实施首批结构解耦：决策提交契约与 Protocol、恢复订单 DTO、订单身份 SQL 适配与分类输入迁移，以及 Dashboard/strategy_runner 包门面的急切导入清理。全部仓库内调用点同步切换，没有保留旧签名转发层。最终单元及部署 smoke 共 2052 项通过，另行开启的网络测试通过；真实 Postgres 事务测试受本地连接超时影响，尚未验收。本批尚未发布到生产。

第二批已完成自愈计算/事务接口分离，复用正常执行持仓锁与 head CAS，保留 epoch 保护并在提交后严格重载才报告 healed；仅本地完成，真实 Postgres 并发与重启验收仍待补齐。第三批已提取 ExecutionBook 恢复/检查点计算，保留正常恢复的迁移规则和 Book 的事务/状态发布职责。第四批已提取命令状态及 reservation 计算，Book 保留持仓事务与失败封锁；第五批已提取命令/outbox 耐久编解码及恢复校验；第六批已明确异步命令仓储接口，将旧调用兼容移至显式适配器。第七批已提取异步 reservation 仓储接口与显式兼容装配，并阻塞 identity 读取异常。第八批已提取证据模型、身份/coverage 规则及累计 fill/order 水位计算。第九批已从订单仓储独立执行命令/恢复/水位与对账仓储，UoW 保持同 session 写入。第十批已提取实盘执行 runtime factory，保留先恢复、后暴露提交 coordinator 的启动顺序。第十一批已独立意图占用与原子提交仓储，保持 fencing、额度占用及订单/事件写入的同事务规则。第十二批已独立订单事件/成交仓储，状态机显式注入事件端口，终态更新与占用释放保持同事务。第十三批已独立订单读取仓储与领域读取接口，对账类型不再依赖整个 Postgres 订单仓储。第十四批已独立外部订单接管仓储，撤单用例沿用单方法接口及先接管、后撤单的顺序。第十五批已将剩余订单仓储收敛为计划写入，独立 shadow suppression 接口并删除重复保存和转发适配器。第十六批已提取成交身份/恢复前缀计划、退出结算判定和真实账户成交水位计算，Book 保留 journal、reservation 与事务发布。第十七批已提取订单证据驱动的 outbox/释放/恢复状态计划，Book 保留执行顺序和事务发布。第十八批已提取累计报告结算与水位持久化计划，保持报告与真实成交去重、退出结算权限及原写入顺序。第十九批已提取候选证据分组协调及独立观察结果模型，Book 继续持有 mutation lock、UoW、head CAS 与提交后发布。第二十批已提取耐久证据准入与变化水位写入计划，核心事务与状态所有权仍集中在 Book。第二十一批已集中投影编码与摘要，解除恢复模型/codec 的摘要循环，旧 checkpoint 编码与摘要兼容。第二十二批已删除 execution 包门面的急切重导出，活跃调用点改为所属模块显式导入，纯模型不再牵动 Book/协调器/codec。第二十三批已独立 Hub 游标恢复与整批确认状态，运行编排保留消费与 checkpoint 写入职责。第二十四批已分离 daemon 的提交仓储与 checkpoint 保存接口，删除聚合转发适配器，保存后的数据库健康标记由 writer 执行并保留原失败语义。第二十五批已独立行情运行契约，删除 daemon 的兼容别名与转发，清理 live_rollout 包急切 gates 导入及会话的注解依赖。第二十六批已独立启动恢复的行情读取端口，将分页游标迁至领域模型，恢复规则与原生 SQL 保持。第二十七批已分离研究采集补回源的两项分页读取能力，清理采集包急切导入，保留原游标/截止时间/分组规则。第二十八批已分离策略纯行情消费与 Postgres 同步读取/LISTEN 适配，保留通知降级及关闭顺序。第二十九批已明确可选唤醒与 Universe 选币读取接口，生产直接注入原生能力并保留轮询降级。第三十批已将资源关闭模块改为依赖实际关闭能力，移除具体交易与数据库类型导入，关闭顺序及预算保持。第三十一批已明确 entry runtime 快照读取接口，Postgres 创建留在运行装配，选币预热与缓存规则保持。第三十二批已独立人工确认缺失订单的纯安全规则并删除 CLI 转发，八项拒绝与原执行流程保持。第三十三批已分离租约恢复用例与耐久会话读取接口，具体查询归原生会话仓储，恢复准入与连接池归属保持。完整本地回归 2395 项通过。其他运行装配与其他延迟环仍需核查，真实数据库验收仍未补齐。完整变化、验证口径与后续顺序见[模块解耦实施进度](module-decoupling-progress-20260930.md)。本文前述代码行号与静态结论仍对应原始 `a18d262` 审查基线。
