# 交易系统架构调整计划

日期：2026-10-01。状态：执行单向化和共享设施迁移已本地完成；真实数据库与生产验收尚未完成。

源码基线：`242fabec23c47a1dcd9481fef8e4199ddf70f01f`，编写前工作树干净。本文是当前 checkout 的结构调整提案，不代表生产状态或已完成验收。

## 目标与边界

减少正常交易路径上的兼容入口、往返回调和跨层类型依赖，使收到行情、策略决策、风险授权、订单提交的职责清晰可追踪。保持既有策略算法、风险限额、订单行为和数据库事实语义。

不以减少文件数或调用层数作为唯一目标。不新增总协调器、通用框架、事件总线或第二套门禁。订单调度、幂等、账户隔离、reduce-only、订单状态及未知结果处理继续保留。

已有[简化修复计划](simplification-repair-plan-20261001.md)和[模块解耦实施记录](module-decoupling-progress-20260930.md)包含已完成事项。实施前对照代码核实，不重复清理已删除的旧签名、退出控制或行情桶对账路径。

## 阶段 0：固定主路径与行为基线

范围：实盘装配、LiveStrategyDaemon、LiveMarketLoop、EntryExecutionLane、LiveCandidateSubmission、订单协调器、状态机及交易所客户端。

1. 核对生产装配实际注入的对象和提交入口，以及 Live、Shadow、测试替身各自需要的能力。
2. 清点兼容入口和运行时能力探测的全部 src/tests/scripts/deploy 调用方，区分实际调用和纯重导出。
3. 从现有测试选择正常开仓、重复提交、风控拒绝、撤单与发单竞争、未知结果、停机等待的行为基线，执行相关定向测试。
4. 补充顶层依赖图与核心提交时序的可复核基线；方法调用数只是选定路径的统计，不宣称等于完整调用栈深度。

验收：能明确说明唯一正常提交路径、兼容路径的消费者和关键行为基线；未确定消费者的入口不直接删除。

## 阶段 1：删除纯透传兼容入口

范围：live_rollout/commands.py、strategy_runner/position_exit.py、OrderExecutionCoordinator.execute_approved_intent，以及 daemon 的纯转发方法。

1. 两个重导出模块的仓库内调用方改为引用领域符号所属模块，再删除兼容模块。
2. 核对 OrderExecutionPort 和 Live/Shadow 调用方后，统一协调器提交命名；消除协调器 execute_approved_intent → submit 的兼容别名。状态机内部同名方法有真实职责，不按名字一并删除。
3. 将 daemon 的 _run_market_loop 透传改为直接绑定现有 market loop 方法。
4. 仅删除无必要消费者的 daemon 转发；外部运行控制所需的窄入口保留，不把内部组件全部暴露给 apps。

验收：旧兼容引用归零，正常开仓与 Shadow 行为保持一致；无新包装类或替代透传层。

## 阶段 2：统一提交契约，拉直提交准备关系

范围：LiveCandidateSubmission、OrderExecutionCoordinator、提交仓储协议和 apps 装配。

1. 定义调用方真正需要的唯一异步提交契约，优先复用现有协议，不新增平行协议体系。
2. 将 prepare_and_execute、先 prepare 后 execute、旧式直接 execute 的能力选择移出热路径；仓库内装配和测试替身一次迁移。只有确有消费者的外部旧接口才在装配边界适配。
3. 协调器通过提交准备端口直接调用仓储，替代 Coordinator → Submission.prepare_for_execution → Repository 的往返控制流。
4. 明确当前准备回调中的职责：事务落库归仓储；队列执行时的开仓许可和上下文有效性检查仍在正确时间执行；提交后的遥测、限价跟踪、缓存失效归原业务所有者。
5. 不把 SUBMITTING 落库移到队列外，不把风险授权与临发单校验混成一次提前检查。

验收：只有一个正常提交契约；准备和发单仍在同一 key 的调度任务内，撤单或对账不能插入两者之间；重复准备不重复 POST，准备失败不发单，停机与禁止开仓行为保持一致。

这一阶段风险最高，单独成批。若回调承担的有效性检查尚不能被现有端口表达，先完成契约统一，暂保留回调；不为消除箭头引入更大的框架。

## 阶段 3：解除持久化对账户接入类型的依赖

范围：persistence/postgres/account_repository.py 对 baseline_checkpoint、binance/user_data_models、event_journal 的引用。

1. 区分通用账户事实、检查点契约和 Binance 协议类型。
2. 通用值类型移到已有 domain/account 或适当的共享契约模块，接入和持久化共同引用；禁止复制出两份同名模型。
3. Binance 专有类型在账户接入边界转换，仓储接收通用事实，不把交易所原始协议搬进 domain。
4. 同步迁移序列化、调用方和测试；不改变表结构、检查点编码、事件顺序及完整性证明。

验收：account_repository 不再导入 execution_account；原检查点和账户事件数据可按既有语义读取与往返写入；领域契约可在不加载 Postgres/HTTP 客户端时导入。

## 阶段 4：收敛门禁状态与共享运行契约

范围：LiveMarketStateAdmission、LiveEntryControlGate、EntryExecutionLane、OrderExecutionCoordinator，以及 live_rollout → strategy_runner 的引用。

1. 清点每个开仓检查的输入、状态所有者、检查时机和拒绝原因，先识别重复可变状态，不机械合并不同时间的检查。
2. 让入口检查与队列发单前检查读取同一许可来源；保留最终发单检查，不新增中央门禁服务。
3. 将实盘和模拟确实共享的规则或值契约归到已有共同所属模块；仅回放使用的实现留在 strategy_runner。
4. 删除已经没有消费者的同步状态和回调，保持行情接收与慢执行分离。

验收：许可来源和拒绝原因可追踪；排队后禁止开仓仍能阻止 POST；已有实际使用模式保持行为；实盘不再引用模拟专属实现。

## 验证与交付方式

每阶段独立提交可审阅的 diff，说明删除入口、迁移消费者、行为是否改变和验证结果。上一阶段检查通过后再进入下一阶段；本计划不包含推送或生产部署。

- 阶段 1：相关 daemon、Shadow、提交与领域引用测试，加导入检查。
- 阶段 2：现有订单提交、协调器、状态机及假交易所闭环测试；重点验证重复提交、队列竞争、准备失败、超时未知和停机等待。涉及事务行为时执行真实 Postgres 集成测试，无法执行则明确保留未验收项。
- 阶段 3：账户仓储、日志、检查点恢复与序列化相关测试，以及导入隔离检查。
- 阶段 4：开仓门禁、排队后阻塞、账户事件、正常开仓到退出的已有行为测试。
- 每批执行修改范围的 lint/type check 和 git diff --check；全部完成后运行相关完整单元、集成及 smoke 回归。只在现有测试不能验证关键行为时新增测试，不写逐字段透传或文件布局测试。

完成后更新架构图和调用计数。目标是兼容入口减少、提交契约唯一、跨层引用消失、许可来源清晰；不预设必须压到多少层，也不以本地测试通过替代生产交易闭环验收。

## 推荐实施顺序

阶段 0 → 阶段 1 → 阶段 2 → 阶段 3 → 阶段 4。

第一批从纯兼容模块和 _run_market_loop 透传开始，风险低、容易核对。订单协调器、状态机、调度器、风控和计划生成器保留其真实职责；整体合并不在本轮范围。

## 第一批实施记录（2026-10-01）

已全仓检索代码调用方，删除 live_rollout/commands.py 和 strategy_runner/position_exit.py 两个纯重导出模块。src/tests 调用方直接引用 domain/live_rollout/authorization.py 与 domain/strategy/position_exit.py；实盘因此不再经模拟运行器引用持仓退出规则。

LiveDaemonLifecycle 直接接收 self._market_loop.run，删除 LiveStrategyDaemon._run_market_loop 透传方法。生命周期类持有的 _run_market_loop 回调仍承担运行接线，保留。协调器提交别名和提交准备回调尚未迁移，不宣称阶段 1 全部完成。

验证：修改前后执行同一组 live_rollout、strategy_runner、实盘 apps、领域策略、订单执行单元测试及两个假交易所 E2E 文件，均为 **1050 passed**。无新增结构测试，无交易所连接或生产部署。

修改文件导入排序检查通过。Ruff F 检查发现 portfolio.py 两个既有未使用导入，已通过 HEAD 源码验证，本批保留。daemon、runtime_config、candle_source 三个文件 mypy 通过；加入 runtime_options 与 portfolio 后有 11 条诊断，通过临时源码副本以相同 follow-imports=silent 配置对照 HEAD，诊断内容相同，无新增。git diff --check 通过；旧模块引用在 src/tests/scripts/deploy 中归零。

下一批：核实 OrderExecutionPort、实盘和 Shadow 的提交消费者，统一协调器提交命名与契约，再处理提交准备回调。当前修改尚未提交或部署。

## 第二批实施记录（2026-10-01）

核实实盘候选提交、run_live_plan 会话、Shadow 服务、直接状态机测试和协调器 backend 调用方后，OrderExecutionPort 的公开执行方法统一为 submit。删除协调器 execute_approved_intent → submit 的十行兼容方法；状态机原公开方法仅改名为 submit，锁、实际执行方法与状态生命周期保留。所有仓库内调用方及测试替身同步迁移，旧公开名称在 src/tests/scripts/deploy 中归零。

prepare_and_execute 保留其队列内准备与发单语义，没有把准备搬到队列外。本批只统一公开提交命名；LiveCandidateSubmission 中的三条能力选择路径和 prepare_for_execution 回调仍在，阶段 2 尚未完成。没有新增接口适配类、转发层或兼容别名；外部仓库若调用旧公开名称，需要自行迁移。

验证：修改前后相同的 live_rollout、订单、Shadow、相关 apps 单元测试和三个假交易所 E2E 文件均为 **920 passed**。另执行故障注入文件中其余五项，**5 passed**；账户 overflow/deferred 测试在第二批改动前已复现失败（期望 deferred 数为 1，实际为 0），本批未扩大范围修复。首次更广基线运行包含依赖 Postgres 的 golden path，等待后中止，未取得该项通过证据；不报告完整 E2E 验收通过。

第二批五个修改源文件 mypy（follow-imports=silent）通过；第二批源文件和相关测试 Ruff F/I 通过；git diff --check 通过。未连接交易所、未提交或部署。

下一批：以实际实盘装配的 prepare_and_execute 为正式提交契约，核对旧式直提及先准备后提交路径的消费者，再在装配边界完成迁移；临发单许可、上下文有效性和耐久准备的时序必须保持。

## 第三批实施记录（2026-10-01）

实际实盘装配已使用 OrderExecutionCoordinator 和 PostgresOrderSubmissionRepository。旧式 save 后直提、先 prepare 再直提主要由测试替身和 golden path 的直接状态机装配消费。本批统一 LiveCandidateSubmission：直接调用 repository.prepare_submission 与 state_machine.prepare_and_execute，删除能力探测、动态 cast 和两条回退路径；提交仓储协议删除本用例不再需要的 save_approved_intent 要求。

CoordinatedOrderExecutionPort 在既有执行协议上声明 prepare_and_execute，实盘 daemon 使用该明确契约；backend 和 Shadow 继续使用 submit，不要求状态机实现队列调度。未新增运行时包装器。

prepare_for_execution 回调仍保留，继续在队列执行时检查开仓许可和上下文有效性。迁移测试使用真实协调器，统一提供准备仓储；删除只验证旧回退的测试。补充排队后上下文失效时不准备、不发单的行为测试；既有跨调度窗口测试继续验证同一保护。

真实协调器测试暴露内存装配的准备抑制边界：无 outbox 时不再无条件 mark_rejected；存在 outbox 时继续记录拒绝，耐久装配缺少 outbox 的提交前检查仍保留。重复准备不发单测试通过。测试 fixture 负责关闭协调器，避免队列任务残留；golden path 装配同步迁移并增加关闭清理。

验证：相关完整回归 **924 passed、1 deselected**，排除项为第二批已记录的账户 overflow 测试；新增上下文失效测试及最后简化后，提交/daemon/协调器/账户渠道定向回归 **128 passed**。回归有一项账户渠道异步生成器 aclose 未 await 的 RuntimeWarning，未将其算作通过的资源清理证明。三个修改核心模块 mypy、七个修改源/测试文件 Ruff F/I、git diff --check 通过。数据库 golden path 三项收集成功，未取得执行通过证据。

本批未提交、部署或执行真实交易。下一批审查准备回调的仓储与业务职责，只有在保持队列内最终检查且不引入新框架的前提下才继续拉直；随后处理 persistence → execution_account 的共享类型依赖。

## 第四批实施记录（2026-10-01）

复查准备回调后决定保留：它在协调器队列实际执行时复查开仓许可和上下文有效性，再准备耐久提交。直接移到队列外会改变时序；为了消除回调而引入新的参数对象、提供器或协调层没有足够收益。本轮不继续包装这条路径。

将 execution_account/snapshot_models.py 与 baseline_checkpoint.py 原样迁至 domain/account，所有 src/tests 的导入和架构测试目标同步迁移；删除旧位置，不留兼容转出口。账户快照、差量、检查点校验与编码结构保持原样。账户接入、实盘上下文、协调器和 Postgres 仓储共同引用领域契约，检查点不再使仓储依赖账户同步层。

范围明确：account_repository 仍依赖 BinanceUserDataEvent 与 AccountEventJournalEntry，尚未消除整个 persistence → execution_account 关系。下一批分别核对交易所专有事件与通用持久化日志的职责，不把 Binance 协议类型直接改名搬入领域层。

验证：迁移前后同一组账户、实盘、账户 apps、架构及账户仓储单元测试均为 **1412 passed、3 skipped、1 warning**。三项 Hub 网络测试默认跳过；账户渠道 aclose 警告在本批修改前已存在。两个迁移契约和账户仓储 mypy、核心文件 Ruff F/I、所有修改文件导入排序、git diff --check 通过。独立 Python 进程禁止导入整个 execution_account、persistence、SQLAlchemy 和 HTTP 客户端时，两个领域契约仍可加载。旧模块引用在 src/tests/scripts/deploy 中归零。

没有表结构、检查点编码或策略算法修改；未执行真实 Postgres 集成、生产部署或交易所下单。下一批处理持久化账户日志的数据边界。

## 第五批实施记录（2026-10-01）

domain/account/event_journal.py 定义 AccountEventReceipt 与 AccountEventJournalEntry。收据保留原始 payload、事件 ID、接收/事件时间及可选交换端顺序证据，不解释 Binance 的余额、订单或持仓字段。BinanceUserDataEvent 继续属于接入层，在 ExecutionAccountSyncService.record_user_data_event 的持久化边界显式调用 to_receipt 转换。

AccountSyncRepository 与 PostgresAccountRepository 接收通用收据，日志查询返回通用收据；不再构造 Binance 事件。原 execution_account/event_journal.py 删除，不保留转出口。现有日志和检查点集成测试迁移输入与断言，不改变数据库字段、事件摘要、幂等键、顺序分配、锁或事务。

全 persistence 源码检索已无 execution_account 引用；独立进程禁止导入整个 execution_account 时，PostgresAccountRepository 成功导入。至此本计划发现的 persistence → execution_account 顶层依赖已解除。

验证：账户、账户仓储和架构相关 **670 passed、3 skipped**；补充收据完整证据保留测试后，相关定向 **59 passed**。五个核心模块 mypy、修改文件 Ruff F/I、git diff --check 通过。两个真实 Postgres 集成测试收集成功，尚未执行，不能视为数据库事务验收；三项 Hub 网络测试默认跳过。

未提交、部署或执行交易所下单。下一批按阶段 4 核对开仓许可的状态所有者与检查时机，优先删除确实重复的状态，不合并不同时间的保护检查。

## 第六批实施记录（2026-10-01）

门禁职责核对：MarketAdmission 检查运行上下文与准入；EntryLane 进行候选筛选；LiveEntryControlGate 拥有开仓许可与阻塞原因；Submission 读取同一 gate 并在队列实际执行时复查；Coordinator 保留调度隔离、停机及最终提交阻塞。这些检查时机不同，不合成一个提前检查。

删除 ScheduledRiskWindowController 的 _scheduled_entry_blocked 副本和仅转发的 _set_scheduled_entry_blocked 方法。窗口已核实可恢复时，每次调度轮询请求实际 gate 恢复；gate 自身负责去重和失败保持关闭。旧副本在 gate 恢复失败时仍会设为 false，后续轮询可能停止重试；删减后失败重试由实际 gate 状态决定。

修正 gate 的调度恢复：风控仍阻塞时调用协调器 block，而非无条件 unblock；解除调度阻塞不解除风控阻塞。新增两项行为测试验证交叉阻塞和恢复失败后的下一次轮询重试。未新建中央门禁服务或第二套许可状态。

验证：改动前相关定向 **134 passed**，新增两项后 **136 passed**；实盘、订单及实盘 apps 完整相关回归 **908 passed**，保留此前记录的账户渠道 aclose 警告。两个核心模块 mypy、修改文件 Ruff F/I 与 git diff --check 通过。

未提交、部署或执行真实交易。下一批应进行累计改动的整体验收和架构图更新，核对剩余 live_rollout → strategy_runner 引用的实际用途，再决定是否还有值得删除的边界；不继续为了减少层数扩大重构。

## 第七批：累计本地验收与架构快照（2026-10-01）

运行 `.venv/bin/python -m pytest -q tests/unit tests/smoke`：**3125 passed、5 skipped、2 warnings**。四项 Hub 网络测试默认跳过，一项 live capture smoke 未配置数据库而跳过；警告为测试客户端弃用及此前记录的账户渠道生成器 aclose。git diff --check 通过。

AST 扫描未发现顶层包导入循环；persistence 的项目内跨顶层依赖只剩 domain。剩余 live_rollout → strategy_runner 引用集中在共享 registry 和 candle_source，提供实际策略构造、收盘行情与 EMA 设施，不属于逐订单热路径的空心层；本批保留，未为调整归属继续搬移文件。

[调整后的架构快照](trading-architecture-after-adjustment-20261001.md)包含模块依赖图、核心下单时序、明确的调用统计口径和剩余边界。正常提交主通道仍为七个类；删减的是兼容入口、两条回退、跨层类型引用和调度状态副本，不把文件移动报告为调用层数下降。

本轮已确认的结构删减完成，后续优先做真实数据库及正常交易闭环验收。已知账户 overflow 故障注入失败和账户渠道资源清理警告仍未修复；不宣称所有 E2E 或全项目检查无问题。全部改动仍在本地工作树，未提交、推送或部署。


## 第八批：执行单向化与共享设施迁移（2026-10-01）

基线为已推送的 85005a9e。本批按用户进一步明确的职责归属方案实施，替代此前保留准备回调与共享设施位置的决定。

- domain/execution/order_submission 定义纯数据 OrderSubmissionPreparation、提交仓储及最终准入协议。Coordinator 在装配阶段持有仓储、准入规则和时钟；请求不包含准备函数或绑定 Submission 的方法。
- 队列出队后，Coordinator 直接读取 LiveSubmissionAdmission 的最新开仓许可和上下文有效性，再调用仓储完成原子准备，然后调用状态机 submit。prepare_for_execution 和回调式 prepare_submission 参数全部删除，包括决策退出路径。
- Submission 不再持有仓储，保留候选风险评估、计划生成、遥测和提交后记账。返回结果携带仓储真实准备时间，遥测不再靠回调修改上游局部变量。EntryExecutionLane 独立保留。
- 策略工厂迁入 strategies/registry.py；收盘行情/EMA 设施迁入 market_data/candle_source.py；所有代码、测试与脚本导入同步迁移，不留旧位置兼容别名。live_rollout 无 strategy_runner 引用。

事务语义未改变：耐久准备由 Postgres 仓储在既有事务内完成；返回时事务已提交。发单在数据库事务外，但与准备处于同一 key 的调度任务。账本预留仍沿用自己的事务，不宣称整个链路是单一数据库事务。

验证：完整单元与 smoke **3128 passed、5 skipped、2 warnings**；最后新增的五项独立导入防护 **5 passed**（禁用 strategy_runner 时加载策略工厂、行情设施和实盘装配）。真实队列定向 **57 passed**，包含出队许可变化、上下文拒绝、准备失败不发单、准备与对账串行及准备时间回传。相关实盘/决策退出/故障注入定向 **127 passed、1 deselected**；排除仍为已知账户 overflow 失败。七个修改核心模块 mypy 通过。迁移的 registry 有一条既有 tuple(object) 类型诊断，文件内容与 HEAD 旧位置完全一致；不宣称全部类型检查通过。修改文件 Ruff F/I、git diff --check 通过；AST 顶层依赖无循环，execution_account 不导入 live_rollout，live_rollout 不依赖 strategy_runner。

数据库 golden path 三项收集成功但未执行；四项默认关闭的 Hub 测试和数据库 live smoke 跳过，既有客户端弃用与账户渠道 aclose 警告保留。架构快照已更新。本批未提交、推送或部署。
