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

第二批已完成自愈计算/事务接口分离，复用正常执行持仓锁与 head CAS，保留 epoch 保护并在提交后严格重载才报告 healed；仅本地完成，真实 Postgres 并发与重启验收仍待补齐。第三批已提取 ExecutionBook 恢复/检查点计算，保留正常恢复的迁移规则和 Book 的事务/状态发布职责。第四批已提取命令状态及 reservation 计算，Book 保留持仓事务与失败封锁；第五批已提取命令/outbox 耐久编解码及恢复校验；第六批已明确异步命令仓储接口，将旧调用兼容移至显式适配器。第七批已提取异步 reservation 仓储接口与显式兼容装配，并阻塞 identity 读取异常。第八批已提取证据模型、身份/coverage 规则及累计 fill/order 水位计算。第九批已从订单仓储独立执行命令/恢复/水位与对账仓储，UoW 保持同 session 写入。第十批已提取实盘执行 runtime factory，保留先恢复、后暴露提交 coordinator 的启动顺序。第十一批已独立意图占用与原子提交仓储，保持 fencing、额度占用及订单/事件写入的同事务规则。第十二批已独立订单事件/成交仓储，状态机显式注入事件端口，终态更新与占用释放保持同事务。第十三批已独立订单读取仓储与领域读取接口，对账类型不再依赖整个 Postgres 订单仓储。第十四批已独立外部订单接管仓储，撤单用例沿用单方法接口及先接管、后撤单的顺序。第十五批已将剩余订单仓储收敛为计划写入，独立 shadow suppression 接口并删除重复保存和转发适配器。第十六批已提取成交身份/恢复前缀计划、退出结算判定和真实账户成交水位计算，Book 保留 journal、reservation 与事务发布。第十七批已提取订单证据驱动的 outbox/释放/恢复状态计划，Book 保留执行顺序和事务发布。第十八批已提取累计报告结算与水位持久化计划，保持报告与真实成交去重、退出结算权限及原写入顺序。第十九批已提取候选证据分组协调及独立观察结果模型，Book 继续持有 mutation lock、UoW、head CAS 与提交后发布。第二十批已提取耐久证据准入与变化水位写入计划，核心事务与状态所有权仍集中在 Book。第二十一批已集中投影编码与摘要，解除恢复模型/codec 的摘要循环，旧 checkpoint 编码与摘要兼容。第二十二批已删除 execution 包门面的急切重导出，活跃调用点改为所属模块显式导入，纯模型不再牵动 Book/协调器/codec。第二十三批已独立 Hub 游标恢复与整批确认状态，运行编排保留消费与 checkpoint 写入职责。第二十四批已分离 daemon 的提交仓储与 checkpoint 保存接口，删除聚合转发适配器，保存后的数据库健康标记由 writer 执行并保留原失败语义。第二十五批已独立行情运行契约，删除 daemon 的兼容别名与转发，清理 live_rollout 包急切 gates 导入及会话的注解依赖。第二十六批已独立启动恢复的行情读取端口，将分页游标迁至领域模型，恢复规则与原生 SQL 保持。第二十七批已分离研究采集补回源的两项分页读取能力，清理采集包急切导入，保留原游标/截止时间/分组规则。第二十八批已分离策略纯行情消费与 Postgres 同步读取/LISTEN 适配，保留通知降级及关闭顺序。第二十九批已明确可选唤醒与 Universe 选币读取接口，生产直接注入原生能力并保留轮询降级。第三十批已将资源关闭模块改为依赖实际关闭能力，移除具体交易与数据库类型导入，关闭顺序及预算保持。第三十一批已明确 entry runtime 快照读取接口，Postgres 创建留在运行装配，选币预热与缓存规则保持。第三十二批已独立人工确认缺失订单的纯安全规则并删除 CLI 转发，八项拒绝与原执行流程保持。第三十三批已分离租约恢复用例与耐久会话读取接口，具体查询归原生会话仓储，恢复准入与连接池归属保持。第三十四批已将启动、心跳与单次计划的排空判断统一到耐久会话读取接口，删除编排重复 SQL 与 CLI 旧查询注入。第三十五批已将 shadow 演练证据查询归 shadow 仓储，告警依赖明确读取接口，删除 CLI 旧注入及编排直接查询。第三十六批已将批准金额查询归订单读取仓储，单次计划直接使用原生读取所有者，删除 CLI 三项数据库回调注入。第三十七批已独立运行通道与退出处理器的身份冲突分类，保留不同判定范围并删除旧私有函数。第三十八批已统一行情、编排与启动的共用瞬时异常分类，保留启动专属重试规则。第三十九批已将可恢复 gate 阻塞规则归 gate 所有者，删除行情循环私有判定并保留原等待恢复行为。第四十批已明确策略 required_data 接口，删除行情循环能力探测，保留显式 None 的默认行为并拒绝缺失方法。第四十一批已明确策略预热和单币重置调用，移除恢复能力探测并完成 required_data 恢复校验迁移。第四十二批已将启动恢复的数据要求读取改为同一显式策略接口，保留 None 默认语义。第四十三批已明确启动恢复模型类型所有者和分页参数 TypedDict，五文件定向类型检查关闭上一批三项验收缺口。第四十四批已明确启动预热 symbols 查询接口，删除能力探测，保留显式空集合与默认查询的区别。第四十五批已将启动预热与清空能力改为直接消费策略契约，入口检查与先清空后重放顺序保持。第四十六批已让 checkpoint 协调器直接消费 writer 耐久进度与时钟接口，保留提交后进度发布规则。第四十七批已删除 checkpoint 签名探测和手工裁剪备用路径，直接请求策略 compact checkpoint。第四十八批已删除缓存维护未消费的 telemetry 构造依赖及无效指标变量，策略缓存行为保持。第四十九批已将策略缓存保护/清理能力改为 daemon 装配时的显式可选回调，缓存维护不再探测方法。第五十批已让缓存维护只消费明确回调与指标值，删除策略 object 依赖，动态指标读取留在装配处。第五十一批已明确 entry EMA 读取/清理能力接口，删除具体 provider 类型依赖及清理能力探测。第五十二批已让 entry runtime 消费实际交易预热与 EMA 能力，移除具体客户端/provider 类型依赖。第五十三批已独立消费者健康 telemetry 能力，控制平面仅依赖单方法接口，完整 sink 继承原能力。第五十四批已让账户事件通道只依赖异步成交记录能力，保留去重/对账/快照顺序。第五十五批已让账户通道依赖单方法退出处理接口，退出通道的注解导入不再急切加载 daemon。第五十六批已独立退出通道四项处理能力，移除具体 daemon 类型依赖。第五十七批已将退出通道身份冲突通知改为显式可选回调，保持通知/失败上报顺序。第五十八批已让退出通道消费异步事件流，移除具体行情源类型依赖，保留消费与重试行为。第五十九批已将退出通道 quote 类型归领域模型，并通过禁止导入两种具体行情源的独立进程检查。第六十批已让账户通道仅消费运行标识和单事件对账能力，移除完整对账实现导入，保留先对账后发布账户快照的顺序。第六十一批已将事件流恢复的 Hub 异常导入限定到对应消费路径，账户通道不再急切加载无关行情及风控 Hub。第六十二批已让风控运行复用消费者健康上报能力，删除完整 telemetry 类型依赖，保持命令与 gate 更新顺序。第六十三批已让 quote/grace 退出通道直接读取接口要求的托管持仓集合，删除动态探测及缺失默认分支。第六十四批已独立共享退出失败规则，账户通道不再因规则引用加载退出运行循环。第六十五批已明确成交量快照异步读取与同步停止能力，删除具体 WebSocket 源类型依赖。第六十六批已将入场预期发布器创建移至两个生产装配入口，注册器只消费已有发布能力。第六十七批已明确订单事件记录与两个同步观察能力，删除三个具体协作者导入，保持 finally 通知顺序。第六十八批已让限价生命周期消费两个订单进度属性，恢复直接使用持久化订单，删除具体状态机结果依赖及临时结果构造。第六十九批已让提交围栏直接使用领域计划必需的订单标识，删除动态探测和隐式转换，缺失身份在耐久读取前拒绝。第七十批已独立上下文就绪与 gate 判定记录能力，行情准入不再依赖完整 telemetry。第七十一批已修正上下文预取异步生成器注解，四个相关模块联合定向类型检查通过，关闭上一批记录的类型缺口。第七十二批已将行情准入缓存失效改为显式可选回调，方法兼容选择移到 daemon 装配，保持原优先级与异常传播。第七十三批已让行情循环直接调用准入模块的失效契约，删除重复能力探测，未配置回调仍由准入保持无操作。第七十四批已隔离退出事件协调模块的实现注解导入，并验证禁止五个相关模块仍能加载；协调模块定向类型验收仍有四处 Any 返回缺口。第七十五批已明确退出协调处理、通道及失败结果读取契约，协调与接口联合类型检查通过，关闭上一批四处 Any 返回缺口。第七十六批已删除退出协调未消费的管理器依赖，仅由启用回调提供可用性判断，并覆盖四种禁用事件。第七十七批已隔离定时风控控制器的执行端口及上下文注解导入，保持撤单与仓位验证顺序。第七十八批已让定时风控只依赖撤单与结果状态读取能力，删除完整订单执行端口类型依赖。第七十九批已补齐定时控制器、上下文及领域行情模型三文件联合定向类型验收，不新增运行代码。第八十批已让定时控制器只消费单方法平仓规划能力，删除完整退出管理器类型引用。第八十一批已将定时撤单前等待入场提交结束改为显式可选回调，保持等待失败封锁与先等待后撤单顺序。第八十二批已让编排的启动行情、风控和账户消费函数接收异步事件流，具体源创建仍归原装配。第八十三批已让账户编排入口复用退出处理、单事件对账及成交记录能力，保留原生对账创建分支。第八十四批已让历史 grace 编排入口复用退出处理契约并显式接收冲突通知，生产原通知绑定保持。第八十五批已补齐历史 grace 入口有/无显式通知及通知顺序验收，无新增运行代码。第八十六批已隔离上下文模型的持仓/账户快照注解导入，加载不再牵动退出管理和账户同步实现。第八十七批已将上下文失效兼容方法绑定到构造阶段，失效路径不再逐次探测，保留 generation 更新及旧 cache 调用兼容。第八十八批已补齐上下文失效方法优先级、旧 cache 参数兼容与构造绑定稳定性验收，无新增运行代码。第八十九批已将上下文新鲜度检查绑定到构造阶段，保留旧名兼容、默认时间规则与失败拒绝。第九十批已删除上下文运行未读取的 reader/provider 属性，并修正 daemon 构造参数以移除一处类型忽略。第九十一批已让 daemon 水位与就绪度消费缓存读取回调，生产显式绑定公共缓存属性，旧调用者兼容保留在构造处。第九十二批已补齐显式缓存回调绕过公共/私有属性及读取动态快照的水位/就绪度验收。第九十三批已让预热覆盖校验直接使用领域桶时间，删除缺字段默认值及跳过连续性检查的分支。第九十四批已补齐乱序连续窗口、缺桶及缺失时间戳三种预热覆盖验收，无新增运行代码。第九十五批已隔离决策事实模块的执行簿及上下文注解导入，并通过同时禁止两模块的检查。完整本地回归 2507 项通过。第九十六批将决策事实的仓位读取依赖收窄为单方法 DecisionPositionReader，删除 ExecutionBook 具体注解引用，保留可选流注册兼容路径；完整本地回归仍为 2507 项通过。第九十七批将决策事实的账户流注册改为显式回调装配，删除读取对象属性探测，完整本地回归 2508 项通过。第九十八批删除执行协调器账户流注册与读取的可选能力探测，沿用具体执行簿必需契约，补查活跃流 epoch 切换，完整本地回归仍为 2508 项通过。第九十九批删除执行协调器关闭阶段 drain 方法探测，验证在途提交完成后调用及重复关闭幂等性，完整本地回归 2508 项通过。第一百批以 PositionContextBook 替代运行上下文执行簿 Any 注解，复用修复重载契约；完整本地回归 2508 项通过，定向类型检查仍有领域模型及导入跳过错误，未完成类型验收。第一百零一批修正覆盖防御分支的不存在枚举及领域模型类型错误，接口与两个领域模型联合定向类型检查通过，完整本地回归 2511 项通过；不代表 provider 或全仓类型验收。第一百零二批修复未知账户敞口的集合操作与缓存仓位泄漏路径，五文件联合上下文类型检查通过，完整本地回归 2513 项通过；未发布生产或完成真实数据库验收。第一百零三批明确执行簿读取的成对非空流身份条件，新增八项拒绝输入验收，完整本地回归 2521 项通过；包含实际执行簿的联合类型检查仍剩九项错误。第一百零四批为耐久恢复与命令变更私有用例显式传入必需 UoW，确认 outbox 仓储非空路径，完整本地回归 2521 项通过；实际执行簿联合类型检查剩六项 Any 返回错误。第一百零五批将同步 reservation 仓储契约正式声明为 Protocol，纳入实际协作者后十文件类型检查及原生执行簿接口赋值检查通过，完整本地回归 2521 项通过；不代表全仓类型或真实数据库验收。第一百零六批将全量活跃 reservation 查询归属协调器公共契约，删除执行簿该查询的私有字典读取与方法探测，完整本地回归 2521 项通过。第一百零七批补齐两公开查询入口的全量排序、仓位过滤、释放排除和账户隔离验收，完整本地回归 2523 项通过。第一百零八批将 reservation 候选复制、发布接管与恢复清空封装到协调器，清除执行簿对其私有登记字典的访问和内存仓储装配依赖，完整本地回归 2523 项通过。第一百零九批补齐候选隔离、发布保留 live 仓储与清空后恢复验收，完整本地回归 2524 项通过；真实数据库验收仍待完成。第一百一十批将持久化历史视图重建交回 PositionBook，删除执行簿对三项私有配置的读取，完整本地回归 2524 项通过。第一百一十一批验证空仓历史视图的 policy/schema 保留、耐久 token 隔离及当前视图不变，完整本地回归 2526 项通过；非空真实数据库历史读取仍待验收。第一百一十二批以实际成交模型与 fake UoW 覆盖非空仓位的持久化历史读取分支，历史数量 1 与当前数量 2 隔离，完整本地回归 2527 项通过；真实数据库路径仍待验收。第一百一十三批补齐 AccountJournal/PositionBook 独立进程导入隔离，确认不加载执行协调栈或持久化实现，完整本地回归 2529 项通过。第一百一十四批删除事实处理的日记证明方法探测及 coverage 属性回退，十一文件联合类型检查通过，完整本地回归 2529 项通过。第一百一十五批删除 outbox 两条持久化路径对命令类型的动态探测与字符串回退，使用领域枚举契约，完整本地回归 2529 项通过。第一百一十六批纳入实际命令模型完成十二文件类型检查，删除退出方向构建对 episode/side 的动态探测，完整本地回归 2529 项通过。第一百一十七批将 persist_facts 返回收窄为 JournalPersistResult，删除冲突/版本动态探测，完整本地回归 2529 项通过；扩大的十四文件类型检查仍有六项可空字段错误。第一百一十八批显式收窄恢复父链与覆盖截止时间非空条件，十四文件联合类型检查通过，完整本地回归 2529 项通过；不代表全仓或真实数据库验收。第一百一十九批为 Postgres 事务事实写入明确领域参数/返回类型，四文件检查及事务接口赋值检查通过，完整本地回归 2529 项通过；journal_store 宽泛类型和真实数据库验收仍待处理。第一百二十批以五方法 ExecutionJournalStore 替代事务与 UoW 的 journal_store Any 参数，五文件类型检查及完整本地回归 2529 项通过；具体 journal store 所有者静态验收及真实数据库验收仍待补齐。第一百二十一批修复具体 store 已校验状态标志与时间列表的类型收窄，六文件检查剩两项 codec Any 返回，完整本地回归 2529 项通过；恢复 codec 扩大范围仍未验收。第一百二十二批保留成交证明解码后整数/布尔字段的明确类型，七文件 codec 扩大检查由二十五项降至十八项，完整本地回归 2529 项通过。第一百二十三批收窄覆盖与批次六个已校验字段，七文件类型错误降至十二项，完整本地回归 2529 项通过。第一百二十四批关闭余下 codec 字段与错误所有者导入问题，七文件类型检查及原生 journal store 接口赋值验收通过，完整本地回归 2529 项通过；真实数据库验收仍待完成。第一百二十五批清理 Postgres 包急切重导出并迁移两个应用入口，journal store 接口独立于 SQLAlchemy 和具体仓储加载，两项导入守卫及七文件类型检查通过，完整本地回归 2531 项通过；真实数据库验收仍待完成。第一百二十六批收窄 Postgres 事务和 UoW 三处恢复读取返回类型，七文件类型检查及原生事务领域接口赋值检查通过，完整本地回归 2531 项通过；真实数据库验收仍待完成。第一百二十七批明确 reservation 领域参数并提取两方法同 session 写入接口，移除执行事务/UoW 对具体 reservation 仓储类的依赖；原生接口赋值及联合类型检查通过，新增导入守卫，完整本地回归 2532 项通过；真实数据库验收仍待完成。第一百二十八批提取命令 outbox 单方法同 session 写入接口并明确六字段，解除事务/UoW 对具体命令仓储类的依赖；含 Book 调用方的二十一文件类型检查与原生接口赋值通过，新增导入守卫，完整本地回归 2533 项通过；真实数据库验收仍待完成。第一百二十九批在生产装配代码中保留原生 UoW 到领域接口的显式赋值，三个具体仓储、端口与 Book 调用方二十四文件联合类型检查通过，完整本地回归 2533 项通过；真实数据库验收仍待完成。第一百三十批导入守卫发现并解除持仓上下文接口对修复计算/恢复 codec 的隐式依赖，重载接口迁至接口所有者并更新自愈调用方；新增四项导入验收及六文件类型检查通过，完整本地回归 2537 项通过；真实数据库验收仍待完成。第一百三十一批将修复值模型与事务契约迁至独立模块，更新实际消费点并新增两项导入守卫，七文件类型检查通过，完整本地回归 2539 项通过；真实数据库验收仍待完成。第一百三十二批解除 Postgres 修复适配器对具体 ExecutionTransaction 类的依赖，复用领域事务并明确同 session 查询能力；原生执行事务、修复事务及修复 UoW 接口赋值验收通过，新增导入守卫，完整本地回归 2540 项通过；真实数据库验收仍待完成。第一百三十三批将共享成交行转换移至独立所有者，解除修复适配器对 journal store 私有函数的依赖，并以真实 ORM 行验证转换和方向过滤；新增导入守卫、十二文件类型检查通过，完整本地回归 2541 项通过；真实数据库验收仍待完成。第一百三十四批独立共享证据摘要并解除成交摘要对恢复 codec 的加载依赖，新增两项导入守卫和迁移前摘要固定值验收；六文件类型检查通过，完整本地回归 2544 项通过；真实数据库验收仍待完成。第一百三十五批迁移投影摘要并明确 checkpoint 公共绑定接口，删除父链 scope 动态回退，八文件类型检查与完整本地回归 2544 项通过；扩大至 ledger 的十二文件类型检查仍有六项错误，真实数据库验收仍待完成。第一百三十六批以明确父链参数替代 checkpoint 宽泛字典展开，并从实际账户模型所有者导入类型，关闭 ledger 六项错误；十二文件类型检查与完整本地回归 2544 项通过，真实数据库验收仍待完成。第一百三十七批清理执行领域与 journal store 剩余账户模型门面导入，并保留恢复 head 校验后的列表/sequence 类型；含 Postgres 修复适配器的二十四文件联合类型检查与完整本地回归 2544 项通过，真实数据库验收仍待完成。第一百三十八批新增十七项恢复 head 边界验收，覆盖异常 reservation 拒绝、sequence 降级及合法值和输入隔离；定向 29 项与完整本地回归 2561 项通过，真实数据库验收仍待完成。第一百三十九批明确成交 client order 标识来自 payload，删除不存在字段的动态探测并将非文本标识归为 None；七项新增边界验收、四文件类型检查与完整本地回归 2568 项通过，真实数据库验收仍待完成。第一百四十批修复两条空仓迁移路径对不存在 OutboxEntry.key 的检查，使用实际 scope 阻止有同持仓命令的快捷迁移，并复用精确持仓匹配；新增两项验收与完整本地回归 2570 项通过，真实数据库验收仍待完成。第一百四十一批补齐 read 跨流迁移的命令归属验收，覆盖同持仓拒绝、其他账户与相反方向不误挡；新增三项验收与完整本地回归 2573 项通过，真实数据库验收仍待完成。第一百四十二批将 outbox 迁移保护扩展为六状态/持仓归属三十项验收，保持终态记录同样保护迁移的既有规则；完整本地回归 2598 项通过，真实数据库验收仍待完成。第一百四十三批命令编码直接使用三个领域枚举值，删除动态探测与字符串回退，固定文本兼容验收、五文件类型检查通过，完整本地回归 2599 项通过；真实数据库验收仍待完成。第一百四十四批命令恢复 codec 入口改为对象映射并明确 watermark scope 校验，新增七项异常身份拒绝验收，五文件类型检查与完整本地回归 2606 项通过；真实数据库验收仍待完成。第一百四十五批校验三项命令恢复可空文本字段，非法类型按既有不可解析策略跳过，新增二十一项边界验收；五文件类型检查与完整本地回归 2627 项通过，真实数据库验收仍待完成。第一百四十六批将不可解析活动命令的运行恢复策略收紧为失败封锁，codec 结果接口保持，新增三项恢复入口验收，完整本地回归 2630 项通过；真实数据库验收仍待完成。第一百四十七批补齐不可解析命令失败期间 act 封锁、记录纠正后的同实例重试与命令/身份重新装载验收，完整本地回归 2630 项通过；真实数据库验收仍待完成。第一百四十八批活动命令改为整批解析成功后再装载，防止后续解析失败留下前序命令及对账标记；扩展失败/重试验收与完整本地回归 2630 项通过，真实数据库验收仍待完成。第一百四十九批恢复命令按 command_id 去重、不同恢复结果失败封锁，整批共享恢复时间避免中断状态假冲突；新增四项验收与完整本地回归 2634 项通过，真实数据库验收仍待完成。第一百五十批将整批命令解析与去重/冲突判断收敛至 codec，Book 保留装载与持久化职责；五文件类型检查与完整本地回归 2634 项通过，真实数据库验收仍待完成。第一百五十一批补齐整批解析独立接口的迭代输入、顺序、过滤、去重、失败隔离与空输入验收，新增四项与完整本地回归 2638 项通过，真实数据库验收仍待完成。第一百五十二批原生命令仓储恢复记录返回类型对齐领域对象映射契约，原生接口赋值及八文件类型检查通过，完整本地回归 2638 项通过；真实数据库验收仍待完成。第一百五十三批实际装配中保留原生命令仓储到领域接口的赋值检查，同一仓储对象供 UoW 和 Book 使用；含实际 Book/装配的十九文件检查与完整本地回归 2638 项通过，真实数据库验收仍待完成。其他运行装配与其他延迟环仍需核查，真实数据库验收仍未补齐。完整变化、验证口径与后续顺序见[模块解耦实施进度](module-decoupling-progress-20260930.md)。本文前述代码行号与静态结论仍对应原始 `a18d262` 审查基线。
