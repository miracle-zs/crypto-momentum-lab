# 模块解耦实施进度（2026-09-30）

实施基线：`8eb059d`。本地已完成十九批结构拆分，尚未完成审计文档中的全部重构。这些结构改动未发布到生产；此前生产运行版本为 `259c8e0`，本批未重新采样服务器。

## 已实施

| 原有依赖 | 改动后的接口与归属 |
| --- | --- |
| decision_facts 直接依赖 AsyncPostgresDecisionUnitOfWork 与 persistence 中定义的提交类型 | `domain/decision/commit_models.py` 定义 DecisionCommit、DecisionCommitReceipt、DurablePolicySnapshot；`domain/decision/ports.py` 定义消费者实际使用的五项 UoW 能力。运行装配仍负责注入 Postgres 实现，业务测试按 Protocol 创建替身。 |
| 恢复订单 DTO 定义在聚合仓储，运行上下文与订单用例都因此导入 Postgres | PersistedExchangeOrder 移至 `domain/execution/order_read_models.py`，所有仓储、用例、装配及测试调用点同步切换，原仓储和包门面不保留旧 DTO 重导出。 |
| 分类、身份解析直接使用 ORM 行，order_facts_loader 混合时间规则与 SQL 查询 | OrderObservation、OrderIdentityEvent、PositionObservation 为不包含 ORM 的数据值；数据库与账户事件两条上下文路径都在适配处转换。SQL、ORM 映射和完整账户成交加载位于 `persistence/postgres/order_identity_repository.py`；live_rollout/order_facts_loader 仅保留纯时间范围计算。 |
| 决策构造器保留丢弃不用的 trace_repository、retention_authority；退出处理器为 Any 并探测是否协程 | 删除无效参数，明确异步 ExitDispatchHandler 与结果接口，直接 await；现有异步运行装配和测试同步迁移。 |
| Dashboard 包急切重导出 API，queries 导入包时回到 API；查询协议位于 HTTP 文件 | 包初始化不加载 HTTP 应用，应用工厂从 api 模块显式导入；查询协议移至 ports.py。 |
| 导入 strategy_runner 纯持仓计算会加载 Postgres 和数据集适配器 | 取消包门面的急切重导出，CLI 与全部仓库内调用点从功能所属模块显式导入。 |

契约迁移没有增加第二条提交路径或旧签名转发层。Postgres 原子决策提交实现仍在同一 session/事务内保存 policy state、trace、依赖与 exit outbox；policy revision、摘要检查、重放幂等、同步提交及失败回滚逻辑未拆开。订单身份加载仍保留双轨查询：已知 order ID 精确查询不受时间窗口限制，symbol 扫描使用有界时间范围；完整账户成交与订单状态不相互伪造。

构造器和包导入路径发生明确变化，所有仓库内调用点已迁移。没有数据库结构或数据迁移，没有改变实盘审批、风险限额和持仓事实。

## 验证证据

- 初始决策、退出生命周期及 Dashboard 相关测试基线：175 passed。
- 最终 `rtk proxy .venv/bin/python -m pytest -q tests/unit tests/smoke/test_deployment_script.py`：**2052 passed，4 skipped**（默认关闭的网络测试），20.15 秒。
- 开启 `CML_RUN_HUB_NETWORK_TESTS=1` 后，账户与行情 hub 两个文件：**49 passed**，其中包含上述四个默认跳过的测试。
- 新增独立 Python 进程导入检查：禁止导入 sqlalchemy 和整个 persistence 包时，decision_facts、position_classification、Dashboard ports 均可以加载；Dashboard/strategy_runner 包初始化不加载 HTTP/数据库适配器。该检查在改动前能捕获 strategy_runner 包门面的间接数据库依赖。
- 新增适配器测试覆盖 ORM 事件与完整账户成交转为普通数据值、事件 details 与 ORM 脱离，以及缺失/畸形成交数量证据仍无法重建身份；现有分类用例改用真实 DTO，覆盖历史订单碰撞、平仓同步延迟与重复退出抑制。
- 新模块、决策模块与新增测试 Ruff 检查通过，git diff --check 通过。
- 本批 AST 依赖扫描：319 个模块，未发现跨顶层包循环；原 Dashboard 静态环已移除。剩余模块级强连通分量仍为 position_ledger_models、recovery_models、recovery_codec，包含 TYPE_CHECKING/延迟引用，不宣称已清理。
- 实际 Postgres 并发与回滚测试已尝试；本地 127.0.0.1:54329 接受 TCP 连接但数据库握手超时，五秒独立连接探针返回 TimeoutError，停止该测试。**本批未取得真实 Postgres 事务验收通过的证据**；未使用生产数据库运行测试。

## 第二批：自愈事务与修复计算

第一批提交为 `57b840f`；第二批在同一 `codex/module-decoupling-20260930` 分支继续实施，仅本地变更，未部署或重启生产。

核查确认旧自愈直接写 journal/head，未使用正常执行的持仓事务锁和 head CAS；归属查询也可能借用其他 run 的同币种订单，跨 epoch 可直接覆盖 head。此次同时修复这些契约缺口，而非仅移动 SQL。

- `domain/execution/position_repair.py` 定义修复请求、事实、计划、回执与小型事务接口，纯计算检查持仓、stream、实际数量和当前开仓 episode 的订单归属。仅有历史策略订单不足以托管当前手工开仓；混合手工加仓也保持阻塞。
- `persistence/postgres/position_repair.py` 复用 `AsyncPostgresExecutionUnitOfWork.transaction`，因此使用同一 `execution_position:{canonical_id}` 锁、同步提交、事务回滚和 head revision CAS。身份去重、事实增量、head 更新均调用正常事务实现，未创建独立 SQL 写入路径。
- 读取订单严格限定 run、symbol、position_side 与非 reduce-only；账户成交严格限定 environment、account、symbol，再按确切 hedge side 过滤。缺失 side、归属或完整可交易投影均无法修复；不再猜测 LONG。
- 同 epoch 修复保留 last_sequence 与 reservation links；跨 epoch 必须走正常验证恢复，自愈不覆盖 epoch。冲突最多三次，每次重新获取事务、读取事实并重算；重复修复不增加事实或 head revision。
- `live_rollout/position_self_healing.py` 仅协调事务和提交后重载，不导入 Postgres/ORM。提交返回后，Book 在 mutation lock 内验证 head、checkpoint、投影摘要、数量与 reservation links，全部通过才发布并报告 healed；失败保留阻塞，不提前报告 ready。
- `domain/execution/evidence_codec.py` 集中既有摘要函数，保持原始编码与 hash 算法，修复与正常执行共用它们；没有数据库 schema 变更。自愈不再无证据清除执行命令的 last_error。

第二批验收：完整单元与部署 smoke（开启 hub 网络测试）**2073 passed**（19.83 秒）；其中修复用例测试文件 **21 passed**，包括账户/run/hedge side 读取约束。覆盖有界冲突重算、重复幂等、事务接口回滚、正常写入 CAS 调用、外部/混合开仓、跨 epoch 阻塞，以及摘要/epoch/数量/reservation 异常时拒绝发布内存状态。独立进程验证自愈与修复领域模块在禁用 persistence/sqlalchemy 时可导入。新模块 Ruff 检查通过。

真实 Postgres 并发、数据库失败原子回滚及完整进程重启恢复尚未验收：此前本地数据库握手超时的限制仍在；上述事务测试使用替身，不能作为真实数据库并发的证明。新的归属与恢复校验更严格，证据不足的持仓会继续保持 unmanaged，需要补齐正确事实或正常恢复，不能用自愈伪造托管状态。

## 第三批：ExecutionBook 恢复计算提取

第二批提交为 `fcdd46e`；第三批仍在同一分支本地实施，未发布生产。

`domain/execution/position_recovery.py` 集中两项恢复能力：

- `create_verified_recovery_checkpoint` 接收 journal、scope、完整成交证明、加载 provenance 与 stream adoption，保留源锚点、分页完整性、checkpoint 血缘、coverage、投影健康与最新账户快照数量校验；不依赖 ExecutionEvidence 或 ExecutionBook，不负责持久化与发布。
- `recover_durable_position` 接收耐久状态，构建新 journal/PositionBook，检查 head 结构并返回 revision、重算摘要、reservation links、sequence 与迁移诊断。它不会改写输入 head 或 Book 所有者。既有正常启动恢复的摘要迁移容忍规则保持不变；第二批自愈的严格重载校验也保持独立，不因此放宽。

Book 继续负责读取耐久状态、mutation lock、候选状态、持仓事务、提交后发布、恢复 reservation/command 与水位。恢复计算不增加第二条写入路径。第三批还修正上一批单持仓 reload 的 evidence ID 发布：补齐 position/stream/epoch 前缀，与启动恢复及正常 observe 的去重键一致；不同 epoch 的同名证据保持隔离。

第三批验证：执行链路、自愈与导入隔离相关 **165 passed**；新增独立恢复计算 **12 passed**，覆盖输入不被改写、既有摘要迁移诊断、畸形 head 拒绝、旧 sequence 规则及无 head 恢复。完整单元及部署 smoke（开启 hub 网络测试）**2086 passed**，21.52 秒。新模块 Ruff 与 git diff --check 通过。真实数据库及进程重启验收限制沿用第二批，不把纯计算/替身测试当成生产验收。

## 第四批：命令状态与 reservation 计算提取

第三批提交为 `85eb844`；第四批继续本地实施，未部署生产。

- `domain/execution/command_models.py` 定义 DispatchState、ExecutionScope、OutboxEntry。原 Book 中的类型定义移除，仓库内所有直接从 Book 导入这些类型的调用点（含集成测试）迁至所属模块；原 domain.execution 包门面从新归属模块导入，保留现有包接口，不额外增加转发层。
- `domain/execution/command_lifecycle.py` 接收不可变命令与 reservation 数据值，返回状态转换、释放与成交扣减计划；模块不导入 Book、数据库或运行装配。UNKNOWN 保留 capacity 并要求 reconciliation；TERMINAL/REJECTED 不会被 UNKNOWN 重开。dispatch 仅允许 PREPARED，attempt_count 按原规则递增；确认与终态的 error metadata 规则保持不变。
- reservation 结算保持原有链接顺序，只消耗 active_quantity，超额或无链接时返回 recovery_required 与既有诊断。释放只释放未消费的余量，不改写 consumed_quantity。累计订单数量仍不是持仓成交事实，计算模块不写 journal。
- Book 继续负责 plan 的持久化、reservation coordinator 更新、UNKNOWN 写入失败后的封锁、正常持仓事务锁与 head revision 校验，以及提交后的候选状态发布。五种 mark_* 调用集中到同一转换协调路径，移除动态 mutator 方法名与 Any 参数转发，恢复后的 UNKNOWN 也使用这一接口。

验证：完整单元与部署 smoke（开启 hub 网络测试）**2100 passed**，21.69 秒。新增纯接口测试 **13 passed**，覆盖禁止重复 dispatch、UNKNOWN 保留、终态不重开、多批 reservation 顺序消费、超额/缺失链接、释放余量及输入不被改写；既有 POST 后 UNKNOWN 写入失败封锁测试、成交归因与恢复测试也包含于完整回归。新增独立进程导入检查确保 lifecycle 模块不引入 persistence/sqlalchemy。新模块 Ruff、相关文件导入检查与 git diff --check 通过。

集成测试仅迁移类型导入，本批未运行真实 Postgres 事务验收，沿用此前连接超时限制。outbox/命令耐久数据映射、旧仓储适配能力探测与命令恢复装配仍在 Book，不能把本批纯规则提取描述为整个命令/outbox 模块已彻底解耦。

## 第五批：命令耐久编解码与恢复校验

第四批提交为 `e748aeb`；第五批仍在同一分支本地实施，未部署生产。

`domain/execution/command_codec.py` 定义 outbox details 编码、active command 恢复与 order watermark 恢复接口，输入为普通数据值/原始耐久映射，输出为不可变恢复结果或明确的跳过诊断。它不依赖 Book、事务或数据库实现。

- 编码保留原来的 scope、request、attempt、外部订单、错误、数量、方向、订单类型、projection version、reservation links 与累计数量/金额字段，不改变 JSON 结构或 Decimal 字符串表示。
- 恢复保留 command/client order identity、账户过滤、数量、bool reduce_only、reservation links、时间和 attempt 校验；DISPATCHING 返回 UNKNOWN 结果及需要写回的标记，Book 仍在原先事务阶段写回并封锁再次提交。
- 原有“部分 payload 解析失败时记录警告并跳过”的策略现在返回 `SkippedCommand`，Book 负责记录日志；身份和 scope 等结构错误仍使恢复失败。本次没有把既有跳过策略改成静默接受或全面失败，亦不宣称已解决所有旧记录完整性问题。
- 水位校验保留有限/非负数量与金额、零数量必须零金额、正数量必须正金额规则。Book 仍负责 max 合并水位；累计订单水位不会变成 journal 成交。
- Book 继续拥有数据加载、恢复顺序、耐久位置/命令/reservation 状态发布、UNKNOWN 延后写回、reservation 对账及 persistence_failed 解封。旧仓储能力探测和同步/异步兼容装配尚未提取。

第五批相关执行与订单测试 **202 passed**；新增 codec 往返、恢复封锁、畸形记录、账户过滤与水位测试 **21 passed**；codec 与架构导入检查合计 **31 passed**。完整单元与部署 smoke（开启 hub 网络测试）**2122 passed**，21.57 秒。新模块 Ruff 与 git diff --check 通过。真实 Postgres 并发、失败原子回滚及完整进程重启验收仍待补齐，未在生产运行测试。

## 第六批：命令仓储接口与显式兼容装配

第五批提交为 `105285a`；第六批继续本地实施，未部署生产。

- `domain/execution/command_repository.py` 定义 Book 实际使用的五项异步能力：outbox 写入、active command 加载、event/trade identity 加载及 order watermark 加载。构造器的 command_repository 由 Any 改为 Protocol；原生 Postgres 仓储已满足异步调用形态，生产与集成测试装配继续直接传入它。
- Book 直接 await 这些能力，不再 getattr/callable 探测命令方法、不检查签名、不按结果判断是否需要 await。耐久模式的 identities/watermarks 继续随 position heads 恢复，明确跳过旧仓储对应读取，不再抛出并吞掉 LookupError 控制流程。
- `domain/execution/legacy_command_repository.py` 为旧同步/异步仓储提供显式适配器；仅此处探测 legacy 方法、account_label 参数及 awaitable。旧测试替身装配显式传入适配器，Book 不自动包装。缺失能力与读取异常向上传播，不伪造成空记录；命令解码、账户过滤与失败封锁仍由既有 Book/codec 路径处理。
- 新接口的 details 使用既有领域 JsonValue 类型，兼容 Postgres 写入契约；第五批编码结果也标注为同一 JSON 值类型，持久化字段没有变化。Legacy 适配器的原始返回值在接口处做类型归属，内容合法性继续由 codec 校验。

验证：完整单元及部署 smoke（开启 hub 网络测试）**2130 passed**，22.21 秒。新增接口/适配器测试 **7 passed**，验证同步读取、scope 转发、异步异常、缺失能力、写入字段保持，以及直接异步接口在禁止签名探测时完成 Book restore；与 codec/导入隔离检查合计 **39 passed**。新增两个仓储模块的定向 `mypy --follow-imports=skip` 检查通过，不代表全仓库类型检查；新模块 Ruff 与 git diff --check 通过。

reservation_repository 仍保留旧同步/异步适配探测、保存/更新能力分支；本批只完成命令仓储契约，不能宣称全部执行仓储已解耦。真实 Postgres 并发与完整进程重启验收仍未完成，未使用生产数据库测试。

## 第七批：reservation 仓储接口与显式装配

第六批提交为 `d6d11eb`；第七批继续本地实施，未部署生产。

- `domain/execution/reservation_repository.py` 定义 Book 使用的四项异步能力：identity lookup、active restore、批量保存与更新。构造器的 reservation_repository 从 Any 改为 Protocol；Postgres 的异步仓储直接满足调用形态，生产耐久 Book 装配保持直接注入。
- Book 构造器默认只创建内存 coordinator，不探测 async loader、不查询外部仓储；异步恢复明确在 restore 中 await。所有仓储操作直接 await，Book 中 `_maybe_await`、inspect 及仓储方法 getattr/callable 分支移除；事务候选仍不携带独立 reservation 仓储，沿用正常 UoW 的原子写入路径。
- `domain/execution/legacy_reservation_repository.py` 提供显式兼容适配与 `assemble_legacy_execution_book`：同步旧仓储保留构造时恢复，异步旧仓储不在同步构造器中读取。OrderExecutionCoordinator 的兼容 fallback 以及旧测试装配明确使用工厂；原生耐久运行装配不经过它。
- 旧仓储的 batch-save/single-save 兼容集中在适配器；历史缺失 lookup 的 single-save 仓储保留返回 None 的旧策略，真实读取异常不被转换为 None。没有保存能力会明确报错，不再静默接受。单条保存 fallback 不提供跨多条 reservation 的数据库原子性，本次未把它当成生产事务路径。
- 核查发现 Book 原先吞掉 reservation lookup 读取异常，随后按不存在继续保存。本批改为返回 Blocked、标记 persistence_failed/recovery_required，不保存新的 outbox；必须恢复成功后才能继续。既有保存、outbox 失败释放和更新失败封锁仍保留。

验证：完整单元及部署 smoke（开启 hub 网络测试）**2138 passed**，22.30 秒。相关执行/订单回归 **230 passed**，新增接口/装配测试 **7 passed**，覆盖同步构造恢复、异步 awaited restore、旧单条保存/version/release_reason、缺失能力失败，以及 identity 读取失败时阻塞且不写 outbox。新增两个模块定向 `mypy --follow-imports=skip` 通过（不是全仓库类型验收）；新模块 Ruff、受影响文件导入检查与 git diff --check 通过。

本批没有生产部署或数据库 schema 变更；真实 Postgres 并发、原子失败回滚和完整进程重启仍未验收。执行 coordinator 的内存职责、证据归并、聚合仓储和运行 factory 仍待进一步拆分。

## 第八批：证据模型、身份与累计水位计算

第七批提交为 `566dc30`；第八批继续本地实施，未部署生产。

- `domain/execution/evidence_models.py` 定义 ExecutionEvidence 与 ExecutionCumulativeOrderReport，原 Book 中定义移除，仓库内直接导入路径及包门面迁至所属模块。保留 sequence、stream、provenance、checkpoint adoption、重复 trade ID 与累计报告数量/金额校验，不改变耐久数据结构。
- `domain/execution/evidence_rules.py` 负责 canonical evidence payload、position/stream/epoch 身份与 coverage 归一化。运输观察时间继续从去重摘要中排除，实际账户成交/快照内容保留；provenance 与 stream 不一致仍拒绝，分页不完整仍只能生成 PENDING coverage。
- `domain/execution/evidence_settlement.py` 计算累计 fill 的增量数量与增量价格，以及累计 order report 的结算差量/下一水位。旧报告不回退水位，同数量异金额、增量金额不增长、复用 trade identity 增大累计量仍报冲突。order 计算在模块内按 order ID 筛选真实成交，其他订单不能抬高目标数量。
- 计算结果不写 journal、不修改 reservation 或 Book；累计订单报告仍不是账户成交。Book 按既有顺序完成候选事实归并、reservation 扣减、事件去重、事务持久化和提交后发布；跨 epoch、持仓批次和平仓边界规则保持现有约束。

验证：完整单元及部署 smoke（开启 hub 网络测试）**2152 passed**，22.89 秒。执行及订单相关 **237 passed**；新增纯计算测试 **11 passed**，覆盖增量价格、输入不变、旧/重复报告、数量金额冲突、trade identity 复用、非有限值、匹配订单筛选、订单报告无增量及 epoch 隔离/运输时间摘要。三个新模块定向 `mypy --follow-imports=skip`、新模块 Ruff 和 git diff --check 通过；导入隔离检查包含 evidence models/rules/settlement。

三个模型/规则模块不直接依赖 Book、运行装配或 Postgres；原 domain.execution 包仍保留既有公开门面。Book 的状态写入与复杂成交归因/命令事件协调仍在原模块，不把本批描述为整个执行引擎已拆完。真实数据库并发与完整进程重启验收仍待补齐，未部署或重启生产。

## 第九批：订单聚合仓储中的执行命令职责分离

第八批提交为 `81e3b4f`；第九批继续本地实施，未部署生产。

- `persistence/postgres/command_repository.py` 定义 PostgresCommandRepository，承接命令保存/upsert、事务内 outbox 更新、active command 加载、event/trade 去重 identity 加载、累计水位恢复和 reconciliation 记录。相关查询、身份检查、历史水位恢复规则从订单仓储迁移，不改变数据库表、查询过滤与水位判定。
- PostgresOrderRepository 删除这些方法，不保留继承、旧签名转发或向新仓储委派的门面；继续负责订单提交、意图占用、订单事件/成交、外部撤单与其他订单职责。
- AsyncPostgresExecutionUnitOfWork 和 ExecutionTransaction 改为注入 command_repository，消除仅为 outbox 更新而依赖整个 OrderRepository 的问题。事务内 upsert 仍使用 UoW 的同一 session，保留 SELECT FOR UPDATE、身份冲突拒绝及 caller-owned commit/rollback；原生独立命令写入方法仍按原规则拥有自身事务。
- runtime、position repair、missing order resolution 和集成测试装配同步切换。Book 与执行 UoW 使用新的命令仓储；订单执行状态机继续使用订单仓储。人工 missing-order 用例的原有记录/事件写入顺序与事务划分没有合并或改变。
- 旧水位仓储测试迁至 test_command_repository.py，原文件不再测试已移出的职责。运行装配中补齐两个原有缺失的类型导入，不改变交易行为。

验证：完整单元及部署 smoke（开启 hub 网络测试）**2156 passed**，22.44 秒。命令仓储测试 **12 passed**，其中新增四项覆盖事务内新记录不自行 commit/rollback、耐久命令身份冲突不改写状态、ExecutionTransaction 向新仓储透传同一个 session，以及订单仓储不再暴露命令方法。新模块定向 `mypy --follow-imports=skip`、Ruff 与 git diff --check 通过。session/锁/事务验证使用替身，不作为真实数据库并发或原子失败回滚的验收证据。

真实 Postgres 测试仍未验收；集成装配已迁移但未在生产数据库执行。订单意图/提交聚合、事件/成交持久化及 runtime factory 仍需继续拆分；本批仅拆出命令/恢复/水位仓储。

## 第十批：实盘执行装配与恢复工厂

第九批提交为 `87061ce`；第十批继续本地实施，未部署生产。

- `live_rollout/execution_runtime.py` 提供异步 build_live_execution_runtime，统一装配订单状态机、命令/reservation 仓储、正常执行 UoW、Book 与账户 coordinator。orchestrator 注入已有订单仓储、交易客户端、session factory、账户/策略身份及具名异步回调，不再直接依赖执行命令仓储、reservation 仓储或执行 UoW 的装配细节。
- 工厂先 await Book 的耐久恢复，成功后才构造并返回提交 coordinator；恢复异常或取消直接向上传播，不暴露可提交的 coordinator。保持既有 live policy、提交前风控检查、entry expectation、订单事件、请求/响应 telemetry，以及 UTC 时钟和按 key 调度规则。
- 命令和 reservation 仓储仍与正常执行 UoW 共用 session factory/仓储实例，不增加事务或第二条执行路径。外部交易客户端和数据库连接由 caller 所有；返回后沿用原 ownership registry 注册 coordinator.aclose，事实源绑定与后续启动、退出处理/关闭顺序保持原位置。

验证：完整单元及部署 smoke（开启 hub 网络测试）**2159 passed**，22.94 秒。新增工厂测试 **3 passed**，覆盖耐久恢复等待时不构造 coordinator、恢复失败及取消均阻止暴露提交能力、不调用交易所，以及同一仓储实例/回调与原 live 设置保持。测试只替换 restore，不以此声称真实持仓恢复或数据库原子性已验收。新模块定向 `mypy --follow-imports=skip`、受影响代码 Ruff 与 git diff --check 通过。

本批只提取实盘执行子系统的装配；整个 daemon 的市场数据、风控、策略、账户通道及 CLI 用例仍在既有运行装配路径。订单意图/事件仓储、复杂成交归因与真实数据库并发/重启验收仍未完成，未重启生产。

## 第十一批：意图占用与原子提交仓储

第十批提交为 `aafa162`；第十一批继续本地实施，未部署生产。

- `persistence/postgres/order_submission_repository.py` 定义 PostgresOrderSubmissionRepository，承接 approved intent 保存、worker claim 和 prepare_submission。提交准备仍在同一个 session.begin 中处理租约/version fencing、风险 halt、会话控制、exposure advisory lock/额度占用、exit episode reservation、订单唯一插入与 SUBMITTING 事件/意图更新；没有将事务拆成多个仓储调用。
- 重复 client order ID 继续拒绝再次授予提交：相同身份及 active reduce-only 重报价返回 None，并通过事务异常回滚该次新写入；不同身份明确报错。claim 仍由唯一插入决定 worker 胜出，只有胜者更新 CLAIMED。
- PostgresOrderRepository 删除以上三项能力，无继承或转发门面。继续负责订单计划、外部撤单 adoption、事件/成交写入、shadow suppression 与订单读取；终态事件仍在原事务里释放 exposure/exit episode reservation、更新意图状态。提交与事件模块因此共享同一数据库状态约束，未宣称表之间完全独立。
- `submission_identity.py` 只承接提交与外部订单 adoption 共用的耐久身份比较，两个仓储均直接使用，互不导入。原 SQL 和分支计算迁移前后 AST 核对一致，仅增加已有 baseline 检查保证的三个非空类型断言。
- 实盘退出准备、daemon 提交适配和 shadow approved intent 装配切换到新仓储，状态机继续使用订单仓储。集成 fixture 同时注入独立订单/提交仓储及原 session factory，调用点和重启对象同步迁移。

验证：完整单元及部署 smoke（开启 hub 网络测试）**2168 passed**，23.46 秒。新增提交仓储测试 **9 passed**，覆盖单事务内 intent/order/event 写入、重复与身份冲突向事务退出传播、事件写失败传播、缺失租约在写入前拒绝、claim 胜负、风险拒绝及旧仓储无提交能力。两个新模块定向 `mypy --follow-imports=skip`、受影响文件 Ruff 与 git diff --check 通过。数据库集成文件 **17 tests collected**，仅验证迁移后可导入/收集，未执行真实数据库事务；替身的事务退出验证不代表数据库并发、原子回滚与重启已验收。

本批没有 schema 变更、生产发布或服务器重启。事件/成交仓储、复杂成交归因和其他运行装配仍待继续拆分，真实数据库验收仍待补齐。

## 第十二批：订单事件/成交仓储与显式事件端口

第十一批提交为 `9f0c0e3`；第十二批继续本地实施，未部署生产。

- `persistence/postgres/order_event_repository.py` 定义 PostgresOrderEventRepository，承接 append_order_event 与 save_fill。事件唯一插入、订单状态/累计量更新、exit episode 状态与终态释放、live exposure claim 释放仍在同一个 session.begin 中完成；成交写入按既有唯一约束返回是否插入，不伪造持仓成交。
- 保留原 SQL 的状态/身份保护：迟到 ACK 不重新打开终态订单，FILLED 不被后续 cancel/reject 覆盖，累计成交量不回退；冲突的 exchange identity 仍入事件日志但不覆盖订单，不触发无效订单更新后的占用释放。迁移方法及累计数量解析 helper 的 AST 与原实现核对一致。
- PostgresOrderRepository 删除事件与成交方法，不保留转发层。OrderExecutionStateMachine 的 OrderStateRepository 收缩为 plan/shadow 写入，新增独立 OrderEventRepository 两项异步能力；构造器必须显式注入 event_repository，不自动包装旧仓储、不探测方法或 fallback。
- 实盘执行 factory、single-plan runner、shadow runtime、missing-order resolution 及所有仓库内状态机装配迁移。生产两项接口由不同仓储实例承担；旧内存测试替身显式注入两项接口。Shadow 适配器删除事件/成交转发，仅保留计划与 suppression 写入。
- 数据库集成 fixture 增加独立事件仓储，订单终态测试直接调用该仓储。Golden-path 数据库测试 helper 同步注入 native 事件与提交仓储，补齐此前提交迁移的该测试装配；该文件本批仍未执行真实数据库用例。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2181 passed**，22.93 秒。新增事件仓储测试 **9 passed**，覆盖重复事件、更新未命中不释放占用、按 durable state 判断终态释放、同一事务与释放失败传播、fill 冲突返回及旧仓储无事件/成交能力。独立状态机端口测试 **2 passed**，用不带事件方法的订单接口执行带成交的订单，验证事件/fill 只使用事件接口，重复插入结果不会通知回调。新仓储与运行 factory 定向 `mypy --follow-imports=skip`、受影响生产代码/新增测试 Ruff、其他迁移测试 F/I 检查、git diff --check 通过。数据库集成文件 **17 tests collected**，未执行真实事务；替身的事务退出和 SQL 逻辑核对不作为真实数据库并发/回滚验收。

没有数据库 schema 变更、生产发布或服务器重启。PostgresOrderRepository 仍承担计划、外部订单 adoption、读取与旧 suppression 保存；本批不宣称订单生命周期中的所有共享数据库约束已消除。复杂成交归因、其他运行装配与真实数据库/重启验收仍待完成。

## 第十三批：订单读取仓储与领域读取接口

第十二批提交为 `39a086f`；第十三批继续本地实施，未部署生产。

- `domain/execution/order_read_repository.py` 定义 OrderReadRepository，仅包含 load_order 与 load_unresolved_orders 两项异步能力，返回已有领域 PersistedExchangeOrder DTO，不包含 ORM 或 Postgres。
- `persistence/postgres/order_read_repository.py` 定义 PostgresOrderReadRepository，承接两项查询及 ORM→DTO 映射。保留终态排除、可选 run 过滤、updated_at/client_order_id 排序与原累计执行数量 baseline；不存在返回 None，数据库读取异常不转换为空集。查询不打开写事务。
- PostgresOrderRepository 删除两项读取方法，不继承或委派新读取仓储；继续负责计划、外部订单接管和旧 suppression 写入。提交、事件和读取使用独立仓储实例，共用原 session factory，数据库表及生命周期约束不变。
- LiveOrderReconciliation 和账户事件通道参数改用领域读取接口，不再为对账类型导入具体 PostgresOrderRepository；原兼容 load_order 探测分支保持，未在本批改变旧替身策略。实盘 gate、上下文中的订单读取、single-plan runner、人工缺单核查及 CLI 未完成订单读取明确装配新的 SQL 读取仓储。
- 数据库集成 fixture 加入独立读取仓储，终态/adoption 查询调用点同步迁移；Golden-path helper 的写入与查询分别使用对应仓储。读取方法及 DTO 映射迁移前后 AST 一致，未改变单笔查询的身份范围或过滤规则。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2188 passed**，21.64 秒。新增读取仓储测试 **7 passed**，覆盖 DTO 字段与空累计数量、精确 client ID、未完成查询终态过滤/run 范围/排序、不启动写事务、数据库读取失败传播及旧仓储不暴露读取能力。两个新模块定向 `mypy --follow-imports=skip`、受影响生产代码/新增测试 Ruff、迁移测试 F/I 检查和 git diff --check 通过。订单集成与 Golden-path 数据库用例合计 **20 tests collected**，仅收集未执行，不作为真实事务与进程重启验收。

没有 schema 变更、生产发布或服务器采样。订单生命周期的跨表事务仍由写入仓储所有，复杂成交归因、剩余运行装配及真实数据库验收仍未完成。

## 第十四批：外部订单接管与撤单仓储接口

第十三批提交为 `77bb837`；第十四批继续本地实施，未部署生产。

- `persistence/postgres/order_adoption_repository.py` 定义 PostgresOrderAdoptionRepository，仅承接 adopt_external_order_for_cancellation，保存 exchange-visible orphan 的合成意图和订单。两项写入仍位于同一 session.begin，保留唯一插入、冲突后读取和原耐久身份核对；已有同身份订单不重新写入，不同身份或冲突后记录缺失仍报错。
- 接管输入继续要求非空 exchange_order_id 与 timezone-aware observed_at；接管方法迁移前后 AST 一致，未扩大校验范围或改变正常撤单顺序。接管不代替交易所撤单，也不授予新的开仓提交权限。
- LiveEntryOrderCanceller 已有的单方法 EntryOrderCancellationRepository 接口直接由新仓储满足，不新增兼容转发。runtime 明确注入接管仓储；canceller await 接管完成后才通过既有 coordinator 撤单，异常向上传播。
- PostgresOrderRepository 删除接管能力，剩余订单计划与 suppression 写入；不保留旧方法、继承或委派。共享身份比较仍由 submission_identity 所有，接管与提交仓储互不依赖。
- 数据库集成 fixture 加入独立接管仓储，外部订单接管/后续读取/事件写入分别调用相应仓储，沿用同一 session factory 与原数据库生命周期约束。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2197 passed**，21.79 秒。新增接管仓储/用例测试 **9 passed**，覆盖单事务合成意图/订单写入、已有同身份订单、不同身份与缺失冲突记录、无效输入不打开事务、订单写入失败向事务退出传播、接管失败不调用撤单，以及旧仓储无接管能力。新模块定向 `mypy --follow-imports=skip`、受影响文件 Ruff 与 git diff --check 通过。数据库订单集成文件 **17 tests collected**，没有执行真实数据库并发/回滚，替身事务验证不作为实际数据库验收。

没有数据库 schema 变更、生产发布或服务器采样。剩余计划/suppression 写入接口、复杂成交归因及其他运行装配仍需按实际调用关系检查；真实数据库与进程重启验收仍待补齐。

## 第十五批：订单计划与 shadow suppression 接口分离

第十四批提交为 `f956820`；第十五批继续本地实施，未部署生产。

- 原聚合订单仓储已只剩 plan/suppression 写入。本批将其收敛为 `order_plan_repository.py` 的 PostgresOrderPlanRepository，仅保存计划并更新 PLANNED 意图状态；两项写入仍在同一 session.begin，方法 AST 与原实现一致。原 order_repository.py 删除，仓库内引用和 package 显式导出同步改名，不保留旧类或模块门面。
- 状态机的 OrderStateRepository 收缩并改名为 OrderPlanRepository，只有 save_planned_order；独立 ShadowSuppressionRepository 只有 save_shadow_suppression。实盘不需要提供 shadow 仓储；选择 SHADOW_SUPPRESS 时构造器必须显式提供，缺失立即失败，不再通过订单接口探测/获取 shadow 能力。
- 删除订单仓储中的重复 suppression 保存及其 private insert helper；suppression SQL 继续由原 PostgresShadowRepository 所有，未改变已有序列化、唯一约束或数据库表。
- Shadow 装配直接注入计划、事件与 shadow 三项仓储，删除只负责转发计划/suppression 的 _ShadowOrderRepositoryAdapter。Shadow 执行保持保存计划→保存 suppression→追加 SUPPRESSED 事件→直接返回的顺序，不调用交易所；失败继续按 pre-submission 失败向上传播。三项写入的原事务划分未合并，不宣称 shadow 全路径原子性。
- 实盘 factory/single-plan/runtime 及数据库/旧内存测试装配同步迁移，旧测试替身明确注入 shadow 能力。历史审计代码基线保持原文，不因仓储改名改写旧版本结论。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2203 passed**，21.94 秒。新增接口/仓储测试 **6 passed**，覆盖缺失 shadow 能力时构造拒绝、独立 suppression 接口成功/失败均不访问交易所、计划与意图状态的同事务成功/失败传播，以及计划仓储不暴露 suppression 能力。计划仓储和运行 factory 定向 `mypy --follow-imports=skip`、核心受影响生产代码/新增测试 Ruff、其他迁移文件 F/I 检查及 git diff --check 通过。订单集成、Golden-path 与 shadow suppression 数据库用例合计 **21 tests collected**，仅收集未执行；真实数据库并发/原子回滚/重启仍未验收。

第九至十五批已将原订单聚合的命令、提交、事件/成交、读取、接管、计划与 shadow 写入归至各自接口和实现；跨表状态约束仍由相应写入事务负责。后续重点回到 ExecutionBook 的复杂成交归因及其他运行用例，模块解耦整体尚未完成；没有生产发布或服务器采样。

## 第十六批：成交身份归因与真实成交水位计算

第十五批提交为 `1ffc191`；第十六批继续本地实施，未部署生产。

- `domain/execution/fill_attribution.py` 提供 plan_fill_observation，返回不可变 FillObservationPlan：原/增量成交、可入账数量、reservation 结算数量、累计数量/水位、累计/新 trade 与恢复前缀标志。仅依赖传入成交、同 trade journal 记录、已见 identity、历史水位与 caller 明确传入的已验证前缀条件，不访问 Book、journal、数据库或仓储。
- 重复真实成交仍按 quantity/price/side/symbol 核对，大小写 side 保持归一化；同 journal trade 不再次结算。全局已见但 journal 缺失仍拒绝并要求恢复，只有原 UoW/stream scope 已验证条件成立的恢复前缀可重新入账，结算数量保持零。累计成交复用第八批 delta 算法，保持冲突校验与增量价格，不改变耐久 payload/digest。
- is_exit_fill 抽离 position side、active episode、active reservation 与 reduce-only 证据判定。它只决定是否应尝试退出结算，不分配持仓批次或写 reservation；既有 BOTH/显式 LONG/SHORT 及事件/成交标志规则保持。
- `evidence_settlement.account_trade_delta` 在模块内筛选匹配订单的真实账户成交，计算相对历史水位的剩余结算差量与下一水位。订单报告领先真实成交时不回退、不再扣一次 reservation；数量推进而金额不推进仍报冲突，不制造账户成交。
- Book 明确读取现有状态并调用纯函数，再沿原流程 append journal、记录已见 trade、await reservation 结算和处理累计报告。真实成交水位计算仍位于 journal append 之后；候选状态、mutation lock、事务、失败封锁及提交后发布仍归 Book 所有，恢复前缀验证未移到纯函数内。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2225 passed**，21.92 秒。新增纯归因/水位测试 **21 passed**，覆盖普通与重复真实成交、四类 payload 冲突、缺失 journal 的已见 trade、已验证恢复前缀、累计增量价格、匹配订单与报告领先水位、金额冲突，以及 position/episode/reservation/reduce-only 退出识别。与架构导入检查合计 **37 passed**；架构检查纳入 fill_attribution，验证新模块导入不加载运行装配/持久化。两个计算模块定向 `mypy --follow-imports=skip`、新模块/计算模块/新增测试 Ruff、Book 的 F/I 检查与 git diff --check 通过，不代表全仓库类型验收。

本批完成成交身份/退出结算判定的纯计算提取，未将整个成交归因或证据事务协调移出 Book。批次投影仍由既有 PositionLedger 等领域模块负责；真实数据库并发、原子回滚与完整重启仍未验收，没有生产发布或服务器采样。

## 第十七批：订单证据驱动的命令状态计划

第十六批提交为 `33467a9`；第十七批继续本地实施，未部署生产。

- `domain/execution/evidence_lifecycle.py` 提供 plan_order_event，返回不可变 OrderEventPlan：下一 outbox、reservation 释放原因、恢复诊断与 dispatch 对账标志。不访问 Book/journal/仓储、不写状态，Book 明确传入现有 outbox、订单事件、observed_at、真实账户成交与耐久模式。
- ACK/SUBMITTED 只将 PREPARED/DISPATCHING/UNKNOWN 转 ACKNOWLEDGED；UNKNOWN_PENDING_RECONCILIATION 保留 reservation 并标记需要对账；已 TERMINAL/REJECTED 或缺失 outbox 保持无操作，迟到事件不再次释放。
- CANCELED/EXPIRED/REJECTED/ABSENT_RECONCILED 保留各自原终态、last_error 与释放原因。FILLED 保留原 last_error，并只按匹配 client order ID 的真实账户成交判定：耐久模式不足请求数量时转终态、保留 reservation 并要求恢复；完整成交则释放，旧非耐久模式继续原兼容释放规则。
- Book 调用计划后沿原顺序执行：UNKNOWN 对账标记在持久化前设置，transition 持久化后才释放 reservation/设置恢复状态；终态 dispatch 对账标志仍在水位写入之后清理。snapshot/coverage/boundary、累计水位、去重记录与最终 projection 发布保持在原证据处理路径。
- 新模块与第四批 command_lifecycle 分工明确：本模块解释订单证据，原模块继续处理显式派发命令及 reservation 计算；未将两个不同触发规则合并，未扩大可提交状态范围。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2251 passed**，22.01 秒。新增纯计划测试 **25 passed**，覆盖 ACK 触发矩阵、UNKNOWN 标记、四类非 FILLED 终态、FILLED 在耐久/非耐久与完整/不完整成交下的释放规则、其他订单成交隔离、终态迟到事件、缺失 outbox 与部分成交事件无隐式转换。与架构导入检查合计 **42 passed**；导入隔离新增 evidence_lifecycle。新模块定向 `mypy --follow-imports=skip`、新模块/新增测试 Ruff、Book 的 F/I 检查与 git diff --check 通过，不代表完整仓库类型验收。

本批提取订单证据的状态判定，Book 保留事务执行、异常传播、累计报告协调及提交后发布；不宣称整个证据协调已拆完。真实数据库并发、原子失败回滚和进程重启验收仍未完成，没有生产发布或服务器采样。

## 第十八批：累计报告结算与水位持久化计划

第十七批提交为 `cf644c1`；第十八批继续本地实施，未部署生产。

- `domain/execution/cumulative_report.py` 提供 plan_cumulative_report，复用既有 cumulative_order_delta，集中生成下一数量/金额水位、退出 reservation 结算差量及 reported_quantity。显式传入历史水位、真实账户成交、现有 outbox 与 active reservation；只在存在退出占用或 reduce-only 命令时产生结算数量，开仓报告仍可推进水位但不做退出结算。
- 报告与匹配订单的真实成交共同决定目标水位，不将两者相加；真实成交领先报告时采用真实数量/金额，旧报告不回退、数量增加而金额不增加仍拒绝。纯计划不生成成交、journal 记录或持仓批次，不变更 reservation。
- plan_watermark_publication 集中处理水位对应命令的原有 identity 选择顺序（cumulative report→order event→single fill→fills 首项→空身份）、最新 outbox 选择及缺失累计成交 outbox 的恢复诊断。Book 在事件 transition 之后传入当前 outbox 映射，故持久化的是更新后命令；无 outbox 的普通成交或单独报告保留原策略，不新增恢复误报。
- Book 先计算累计报告计划、再 await reservation 结算；随后按原顺序处理 snapshot/coverage/boundary 和订单事件，更新内存水位、执行 outbox 持久化/缺失累计成交恢复标记，最后清理已对账命令标志与记录 evidence identity。锁、候选状态、失败封锁、同事务持久化和提交后发布未移动。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2268 passed**，22.10 秒。新增纯协调测试 **16 passed**，覆盖报告与真实成交两种先后顺序只结算一次、退出/开仓权限、真实成交领先报告与其他订单隔离、旧水位/金额冲突、命令来源优先级、累计成交缺少 outbox 恢复、最新 outbox 不变性，以及单独报告不制造成交。与架构导入检查合计 **34 passed**；导入隔离纳入 cumulative_report。新模块定向 `mypy --follow-imports=skip`、新模块/新增测试 Ruff、Book 的 F/I 检查与 git diff --check 通过。

本批完成累计报告结算和水位持久化的纯判定；Book 仍执行证据事务协调，未新增提交路径或修改持仓批次边界。真实数据库并发、原子失败回滚与完整重启仍未验收，没有生产发布或服务器采样。

## 第十九批：候选证据分组协调与观察结果模型

第十八批提交为 `b0c64dc`；第十九批继续本地实施，未部署生产。

- `domain/execution/observation_models.py` 承接 Applied、Duplicate、EvidenceConflict 与 ExecutionObserveResult。Book、账户执行 coordinator、包门面及仓库内直接导入调用点同步指向模型所有者；协调器不需要通过 Book 获取结果类型。模型字段/类型身份与既有行为保持，Book 仍在自身实现中使用这些类型。
- `domain/execution/evidence_grouping.py` 提供 observe_evidence_group，接受单条证据处理回调与内部 identity 清理回调。协调器不接收整个 Book，不访问锁、仓储、数据库或已发布状态；回调必须绑定同一个 caller-owned candidate。
- 单条/多条成交按原顺序分别生成 internal trade evidence，剥离 snapshot/boundary/订单事件/coverage/provenance/adoption/累计报告；成交处理后清理内部 evidence identity，再处理保留原证据身份的 remainder。首个成交冲突立即停止并回报原 evidence ID，异常/取消直接向上传播；不伪造部分成功结果。
- 聚合消费/释放数量、recovery 标志及按首次出现顺序去重的 diagnostics 保持原规则；remainder 的 Duplicate/Conflict 保持直接返回。协调函数体在绑定显式 callbacks 后 AST 与原 Book 方法一致，未重写失败或重复证据策略。
- Book 删除 _observe_grouped，非耐久路径直接注入自身回调，耐久路径在原 mutation lock/UoW 内注入 staged candidate 回调。事务持久化、head CAS、失败封锁、candidate commit 后发布和非耐久 journal delta 清理保持原位置；没有引入第二个事务 owner 或另一条提交路径。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2279 passed**，22.53 秒。新增分组协调测试 **9 passed**，覆盖无成交直传、两笔成交后处理报告/聚合、首笔/后续冲突停止、remainder Duplicate/Conflict、异常和取消传播、重复成交不重复累计。与架构导入检查合计 **29 passed**，导入隔离新增 grouping/observation models。两个新模块定向 `mypy --follow-imports=skip`、新模块/新增测试 Ruff、迁移文件 F/I 检查与 git diff --check 通过。

本批独立的是证据分组协调；耐久证据的数据库事务流程和状态所有权仍由 Book 管理，不宣称事务协调全部拆完。真实数据库并发、原子失败回滚与进程重启仍未验收，集成测试只迁移模型导入，没有生产发布或服务器采样。

## 后续实施顺序

1. 补齐真实 Postgres 并发、原子回滚与完整进程重启验收；第二批已完成自愈事务迁移及提交后重载契约。
2. ExecutionBook 内部协作者：第三批已提取恢复/检查点计算；第四批已提取命令状态与 reservation 计算；第五批已提取命令/outbox 编解码与恢复校验；第六批已明确命令仓储接口并提取显式兼容适配；第七批已提取 reservation 仓储接口与显式兼容装配；第八批已提取证据模型、去重/覆盖规则与累计水位计算；第十六批已提取成交 identity/恢复前缀计划、退出结算判定及真实账户成交水位增量；第十七批已提取订单证据的 outbox/释放/恢复判定；第十八批已提取累计报告结算与水位持久化计划；第十九批已提取候选证据分组协调及观察结果模型；耐久事务与状态所有权仍由 Book 管理，按风险继续简化。统一 mutation lock、候选状态、事务与提交后发布仍由 Book 所有。
3. 聚合仓储与运行装配：第九批已拆出执行命令/恢复/水位/对账仓储；第十一批已拆出意图占用/原子提交仓储，第十二批已独立事件/成交仓储与状态机事件端口，第十三批已独立订单读取仓储及领域读取接口，第十四批已独立外部订单接管仓储，第十五批已分离计划与 shadow suppression 接口，继续处理运行用例的装配，第十批已提取实盘执行 runtime factory，继续处理其他运行子系统与可调用的 CLI 用例。
4. 恢复模型/codec 的静态环：集中摘要计算与投影编码职责，保留既有 checkpoint digest 与跨 epoch 保护。

生产账户事实覆盖冲突和策略状态发布缺失属于尚未关闭的运行问题；本次结构迁移不能作为这些问题已经修复的证据。
