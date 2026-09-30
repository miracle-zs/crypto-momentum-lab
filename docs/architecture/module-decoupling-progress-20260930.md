# 模块解耦实施进度（2026-09-30）

实施基线：`8eb059d`。本地已完成三十七批结构拆分，尚未完成审计文档中的全部重构。这些结构改动未发布到生产；此前生产运行版本为 `259c8e0`，本批未重新采样服务器。

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

## 第二十批：耐久证据准入与水位写入计划

第十九批提交为 `091dc64`；第二十批继续本地实施，未部署生产。

- `domain/execution/durable_evidence.py` 提供 prepare_durable_evidence，独立核对 stream identity、拒绝 cumulative fill 进入真实账户成交 journal、按既有 coverage helper 绑定 scope/proof，并要求已确认 live coverage 的 typed pagination provenance。返回规范化 evidence 与 stream scope，不读取/写入 Book、仓储或数据库。
- DurableEvidenceConflict 明确表示原来返回 EvidenceConflict 的准入拒绝；Book 只转换这类结果。原 scope 构造的参数校验异常继续抛出，不因为提取而一律伪装成 source rejection；coverage proof 的原错误消息保持。
- changed_order_watermarks 接受当前 position key 与提交前/候选数量金额映射，筛选该持仓已变化的订单，按 order ID 排序并生成原 ExecutionWatermark 值。数量或金额变化均写入，缺省金额仍为零；未变化、其他持仓、删除的数量 key 和无数量的 quote-only key 保持原处理规则，不扩展写入范围。
- Book 先调用准入计算再登记 stream/进入原 mutation lock；候选 journal 与事实持久化之后计算水位列表，在同一 tx 中依次 persist_watermark，之后 persist_head。head CAS、checkpoint 验证、异常封锁和 commit 后 candidate 发布保持原位置，没有新事务 owner 或额外提交路径。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2289 passed**，22.72 秒。新增纯准入/写入计划测试 **9 passed**，覆盖累计报告可准入但不制造成交、缺少 stream、累计成交标志/cum_qty 拒绝、已确认 live 覆盖的来源证明、PENDING 不升级、无效空 stream 的原异常类型、水位持仓隔离/排序/数量金额变化，以及未变化/删除/quote-only key 不产生写入。与架构导入检查合计 **30 passed**，导入隔离纳入 durable_evidence。新模块定向 `mypy --follow-imports=skip`、新模块/新增测试 Ruff、Book 的 F/I 检查与 git diff --check 通过。

本批抽离耐久事务前后可独立验证的规则，核心事务流程仍统一由 Book 所有。真实数据库并发、原子失败回滚与进程重启仍未验收，没有 schema 变更、生产发布或服务器采样；结构测试不能替代生产状态验收。

## 第二十一批：恢复投影编码与摘要的依赖方向

第二十批提交为 `47ebde6`；第二十一批继续本地实施，未部署生产。

- `domain/execution/projection_codec.py` 集中投影、scope、position key、batch、episode、reduction、discrepancy 的七项编码及完整投影摘要，仅依赖 ledger 数据值和标准库。恢复模型直接使用此摘要，删除计算摘要时反向导入 recovery codec 的延迟循环。
- PositionRecoveryCodec 保留现有编解码接口，编码方法直接绑定同一规范函数，摘要也直接绑定；严格解码、checkpoint schema 与链哈希规则保持原实现。Decimal/时间格式化复用同一函数，没有复制另一套编码规则或增加转发函数。
- 摘要继续使用完整投影的排序 compact JSON SHA-256，仅排除 projection_version；保留 Decimal 小数位、时区偏移、Unicode JSON 默认规则、列表顺序和空值。改动前保存的历史投影样本包含活动/归档 episode、退出 reduction、stream scope 与中文 diagnostics，编码及固定摘要保持一致。
- 七项编码函数体去除 class binding 后与原实现 AST 一致。恢复模型的直接依赖已解除；execution 包门面仍急切加载 Book 与 codec，模型导入隔离测试明确先初始化包再移除模型/codec，禁止把这项检查解释为包级急切导入已解决。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2299 passed**，23.36 秒。新增编码兼容测试 **9 passed**，覆盖改动前固定投影/摘要、fact token 排除、四类状态变化绑定、Decimal scale/时区 roundtrip、无时区拒绝和模型直接依赖隔离；恢复及架构定向回归合计 **55 passed**。导入隔离纳入 projection_codec。新模块定向 `mypy --follow-imports=skip`、所有本批 Python 文件 Ruff 和 git diff --check 通过。

本批无 schema 变更、生产发布或服务器采样。真实数据库并发、原子失败回滚与进程重启仍未验收；下一步可清理 execution 包门面的急切导入，避免纯模型导入牵动整个执行协调栈。

## 第二十二批：execution 包初始化与显式所属模块导入

第二十一批提交为 `6c7b8d6`；第二十二批继续本地实施，未部署生产。

- execution/__init__.py 删除 66 项急切重导出，初始化仅保留模块说明。订单枚举、持仓模型等纯数据导入不再顺带加载 ExecutionBook、execution coordinator 和 recovery codec；未引入 __getattr__、延迟重导出或旧签名兼容层。
- src、tests 内的活跃包级导入全部改为从所属模块显式导入，包括运行装配、订单状态机、Postgres 仓储、Dashboard 查询、fake-service E2E、数据库集成测试和嵌套测试导入。模块命名空间导入也指向完整模块路径；scripts/deploy 未发现需要迁移的包级导入。调用方必须使用所属模块路径，原包级名称不再是公开入口。
- 迁移调用点的非 import AST 与原实现一致，订单提交/恢复/退出行为和状态所有权没有修改。导入排序和必要格式调整不构成行为重写。
- 新增七项独立进程架构检查：分别导入 execution 包、order_state、command_models、position_ledger_models、observation_models、recovery_models、projection_codec，同时禁止 Book、领域 coordinator、recovery codec 和 persistence。第二十一批恢复模型检查取消先初始化包/移除缓存的绕行，改为直接独立导入。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2306 passed**，23.13 秒。架构及投影兼容定向测试 **38 passed**；integration/e2e **108 项成功收集**，其中数据库测试未执行，收集不代表数据库行为验收。本批核心文件完整 Ruff、迁移文件 F/I、git diff --check 通过；live_rollout/position_batches.py 保留基线已有两个 F821（AccountPositionSnapshot/AccountPositionSnapshotRow 注解未导入），该文件 F/I 检查仅豁免 F821，不能宣称全仓 lint 通过。

文档目录的历史 repro_batch_price_contamination.py 使用基线已不存在的 rebuild_position_batches，属于旧缺陷复现资料，未作为活跃可运行脚本迁移或验收。本批不修改这段旧复现行为。真实数据库并发、原子失败回滚与进程重启仍未验收，无 schema 变更、生产发布或服务器采样。后续继续核查其他运行子系统的装配与模块级延迟环。

## 第二十三批：行情 Hub 游标恢复与确认状态

第二十二批提交为 `fa058f6`；第二十三批继续本地实施，未部署生产。

- `live_rollout/hub_cursor.py` 承接 LiveHubCursorState、hub_cursor_for_startup 与 checkpoint 原始游标读取。模块管理游标及待确认批次，不持有 daemon、数据库、消费任务或 checkpoint 仓储；MarketStateBatch 仅在 TYPE_CHECKING 中导入。
- 同一批次全部已登记行情状态确认之后才更新游标；按 symbol/bucket_start 关联状态，重复或未登记确认保持 no-op，同一 stream 的旧批次不回退 sequence，完成新 stream 的批次可替换旧 stream。新入池符号跨批次保留并在首次消费后移除，沿用原行为。
- checkpoint 游标字段与校验保持：stream_id 必须是非空字符串，sequence 必须是非负整数且拒绝 bool。无 checkpoint 或 requires_market_recovery 时不恢复旧游标，避免耐久行情重建后沿用旧 Hub epoch；非法 checkpoint 游标继续记录原警告并忽略，显式 restore 仍抛出 ValueError。
- runtime_orchestrator 删除原内部实现并从所有者显式导入；startup recovery 测试同步迁移，不保留原私有名称转发。提取内容及剩余编排代码在名称迁移后 AST 与原实现一致。运行编排继续负责状态处理、确认调用时序和 checkpoint 保存，未新增提交路径。
- 静态双向引用核查中 position_ledger_models 对 recovery_models 的引用受 TYPE_CHECKING 保护，不把类型注解关系误报为运行时循环；其他复杂或动态导入仍需按实际依赖继续核查。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2325 passed**，22.97 秒。新增游标测试 **18 passed**，覆盖八种非法恢复输入、四类非 mapping payload、零游标与 snapshot 不共享状态、重复/未知确认、同 stream 旧批次、新 stream 接管、无 stream/空批次原行为；与现有 startup recovery 和架构测试合计 **56 passed**。架构无数据库导入检查新增 hub_cursor，新模块定向 `mypy --follow-imports=skip`、新模块/新增测试 Ruff、编排及迁移测试 F/I、git diff --check 通过。

本批独立游标状态的实现与验证，不改变原有乱序批次策略、消费执行顺序或 checkpoint 提交时机，也不构成实际 Hub/数据库重启验收。真实数据库并发、原子失败回滚与进程重启仍未验收；无 schema 变更、生产发布或服务器采样。下一步继续核查运行编排内其他状态所有者及仓储适配的归属。

## 第二十四批：提交仓储与 checkpoint 持久化接口分离

第二十三批提交为 `61a0c81`；第二十四批继续本地实施，未部署生产。

- 删除 runtime_orchestrator 内 _LiveDaemonRepositoryAdapter：原适配器仅把 approved intent、原子 prepare_submission 转发到提交仓储，再把 checkpoint 转发到另一仓储。删除 daemon 内将两类能力聚合的 LiveDaemonRepository，不把转发代码搬到新文件。
- LiveStrategyDaemon 显式接受 submission_repository: LiveSubmissionRepository 和 persist_checkpoint: PersistCheckpoint。提交用例直接使用原生提交仓储；CheckpointWriter 使用独立保存回调，run_id 仍来自同一 daemon config，writer/coordinator 的启动、周期提交、关键 flush 和停止顺序不变。生产装配及全部 daemon 构造调用点同步迁移，没有旧参数兼容分支。
- CheckpointWriter 增加可选同步 on_persist_success，生产用于保存成功后的数据库健康标记。回调只在 await persist 返回之后执行；异步 writer 路径仍在同一 write lock 内，未启动 writer 的直接 save_now 保留原直接执行路径。计数、token、持续时间与成功发布时间均在回调之后更新，回调不作为 best-effort 吞掉。
- 保存异常或取消不调用成功回调。回调异常沿用旧适配器语义：周期写入重试，已启动 writer 的关键写入返回 False，未启动 writer 的直接保存向上传播，取消继续传播。数据库保存已完成而健康回调失败可能再次写入 checkpoint，此行为原先已存在，本批不改变幂等或重试口径。
- 提交仓储 SQL、fencing、额度占用、intent/order/event 原子事务及 checkpoint SQL 实现均未改动；两个持久化方向没有合并事务或增加另一条提交路径。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2335 passed**，22.98 秒。新增 **10 项**测试：两种 writer 生命周期下保存完成前后通知顺序、失败/取消不通知、回调异常的关键写入语义、周期通知失败重试与 token 不提前发布，以及提交与 checkpoint 使用完全独立实现的 daemon 执行。writer/coordinator/daemon 定向回归 **86 passed**。writer 定向 `mypy --follow-imports=skip`、writer/新增测试完整 Ruff、daemon/编排/迁移测试 F/I、git diff --check 通过；不代表全仓类型检查。

本批无 schema 变更、生产发布或服务器采样。真实数据库并发、原子失败回滚与进程重启仍未验收；下一步继续核查运行用例和 daemon 中剩余的兼容重导出及状态归属。

## 第二十五批：行情运行契约与生命周期导入方向

第二十四批提交为 `762244a`；第二十五批继续本地实施，未部署生产。

- `live_rollout/market_runtime_contracts.py` 承接 LiveRuntimeStrategy Protocol、LiveDaemonResult、LiveMarketStateContinuityError 与 MarketStateGapRecovery。结果字段、策略方法、gap 异常属性/消息及回调类型保持原定义；纯契约不依赖 daemon、market loop 或具体持久化/交易实现。
- daemon 删除 LiveDaemonResult/LiveRuntimeStrategy 兼容别名以及 _is_transient_live_gate 转发函数；market_loop 删除原契约定义及相关 __all__ 重导出。两个实现模块以契约模块命名空间使用类型，活跃调用点从所有者显式导入；瞬态 gate 测试直接调用 market_loop 原实现。未建立另一个兼容入口。
- 运行编排、CLI、startup recovery/resilience、daemon lifecycle、supervisor、session 及对应测试全部同步迁移。独立契约使生命周期管理不必为结果类型导入行情执行循环；具体 runtime_config 的同名配置数据类保留原归属，未与运行策略 Protocol 混淆。
- live_rollout 包初始化删除 gates 的两项急切重导出，活跃代码已直接从 gates 导入能力；子模块正常导入保持。runtime_session 对具体资源生命周期和 supervisor 的导入移入 TYPE_CHECKING，这些类仅用于注解，运行时继续使用调用方注入的对象；资源关闭、超时和停止顺序不变。
- 契约定义、daemon/market_loop/runtime_session 的运行函数体在所属模块名称规范化后 AST 与原实现一致。恢复/缺口处理/关闭行为没有改写，不改变 checkpoint、订单提交或状态发布时机。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2340 passed**，22.83 秒。新增 **5 项**架构检查：契约模块可禁止数据库导入，以及 live_rollout 包、契约、runtime_supervisor、runtime_session 可同时禁止 daemon、market_loop、sqlalchemy、persistence 和 execution_account 导入。现有策略恢复、gap、daemon 与生命周期回归继续通过。契约模块定向 `mypy --follow-imports=skip`、新模块/包初始化/架构测试完整 Ruff、所有迁移 Python 文件 F/I 和 git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。startup recovery 仍直接使用具体 Postgres 行情读取类型；后续可独立其实际读取接口，不能因契约独立就宣称恢复模块已完全解耦。真实数据库并发、原子失败回滚与进程重启仍未验收。

## 第二十六批：启动恢复的行情读取端口与领域分页游标

第二十五批提交为 `8f68f60`；第二十六批继续本地实施，未部署生产。

- `domain/market/runtime_state_repository.py` 定义 RuntimeMarketStateReadRepository，只包含 startup recovery 实际消费的 load_latest_bucket、load_after、load_recovery_window 三项异步读取能力。启动恢复不再导入具体 Postgres 仓储，生产继续注入原生实现，现有恢复测试继续直接使用读取替身；未增加仓储转发适配器。
- RuntimeStateCursor 移到 domain/market/runtime_state_models.py，保留 frozen/slots、bucket_start/symbol 字段与默认 None。活跃调用点包括 live rollout、研究行情源、策略数据源、shadow CLI、仓储和测试均切换到领域所有者；Postgres 仓储以私有类型名使用，Postgres 包门面删除原游标重导出。
- 读取接口明确 load_after 的排他 (bucket_start, symbol) 游标、bucket/symbol 排序、inclusive upper_bound，以及无 symbol 过滤与空 symbol 集合的差异。原生 SQL、session 生命周期、恢复窗口读取与字段映射保持原样，不引入另一个查询策略。
- startup recovery 的耐久截点等待、缺口读取、分页 warmup、按 checkpoint 重建、限制和重试规则均未改写。启动恢复及原生仓储在类型归属规范化后 AST 与原实现一致；SQL 事务和行情快照边界仍由原适配器负责。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2343 passed**，25.30 秒。startup recovery 与架构定向回归 **46 passed**；新增 startup_recovery、行情读取端口、游标模型三项禁止 sqlalchemy/persistence 的独立进程导入检查。integration/e2e **108 项成功收集**，数据库行为未执行，收集不是事务验收。两个领域模块定向 `mypy --follow-imports=skip`、领域模块/架构测试完整 Ruff、所有迁移文件 F/I、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库并发、原子失败回滚与进程重启仍未验收；其他行情消费用例仍有自身的具体仓储依赖，后续按它们实际读取能力继续拆分，不能把启动恢复端口等同于整个行情持久化已解耦。

## 第二十七批：研究采集补回源的分页读取接口

第二十六批提交为 `ad3dace`；第二十七批继续本地实施，未部署生产。

- domain/market/runtime_state_repository.py 提取 RuntimeMarketStatePageReader，两项能力为 load_latest_bucket 与 load_after。启动恢复的 RuntimeMarketStateReadRepository 继承这两项并继续提供 load_recovery_window；研究补回源只接受分页读取能力，不要求 checkpoint 恢复窗口接口。
- PostgresMarketStateBackfillSource 更名为 RuntimeMarketStateBackfillSource，依赖领域分页接口。生产 CLI 仍注入原生 PostgresRuntimeMarketStateRepository，collector 的注解同步迁移；删除旧类名且未保留别名或转发层，SQL 与原生仓储未变更。
- 补回继续按排他 bucket/symbol 游标读取，在内存按 until 做 inclusive 过滤，并按 bucket 分组。数据库 page 可拆分同一 bucket，继续由 collector 自然键去重保证安全；没有因结构迁移改成全窗口加载或新增 upper_bound 查询参数。短页、空页或部分行超过截止时间按原规则结束。
- research_collector 包初始化删除模型/服务急切重导出，活跃代码已从所属模块显式导入，模块级 health 导入保持正常。source/models 不再为了包初始化加载服务、Parquet 存储或数据库；不把 Parquet 存储自身的必要依赖误当成业务查询接口。
- 补回源、collector、生产 CLI 在类名/接口名规范化后运行 AST 与原实现一致。SourceKind、checkpoint 内容、自然键去重、窗口落盘和收据规则保持原样，没有增加另一条持久化路径。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2357 passed**，24.30 秒。新增分页行为测试 **11 passed**，覆盖构造校验、最新时间、同 bucket 跨页及游标、截止时间、空/短页、非法时区、读取异常和取消传播；source/service/startup recovery/架构定向合计 **69 passed**。新增 source、research_collector 包及模型三项禁止 sqlalchemy/persistence 的独立进程导入检查。分页接口与 source 定向 `mypy --follow-imports=skip`、核心文件/新增测试完整 Ruff、迁移装配 F/I、git diff --check 通过。

本批无 schema 变更、生产发布或服务器采样。策略行情源仍包含自身的具体行情/Universe 仓储及 asyncpg 通知连接，后续按其读取和通知职责继续核查。真实数据库并发、原子失败回滚与进程重启仍未验收；本批补回替身测试不能替代实际 Postgres 与通知链验收。

## 第二十八批：策略行情消费与 Postgres 通知适配归属

第二十七批提交为 `7dabd42`；第二十八批继续本地实施，未部署生产。

- `persistence/postgres/runtime_state_loader.py` 承接 AsyncPostgresRuntimeStateLoader 及内部通知 wakeup。该适配器统一持有私有 event loop、asyncpg LISTEN connection、原生行情/Universe 读取和调用方提供的 shutdown；strategy_runner/live_source 保留同步 RuntimeStateLoader Protocol、消费配置与游标迭代，不再导入 Postgres、asyncpg 或数据库通知 channel。
- CLI 装配与原生 loader 测试同步从新所有者导入，原消费模块不保留 loader 类重导出或转发。PostgresPaperMarketStateSource 既有名字和消费接口保持，本批仅明确其实现依赖的归属，未因类名历史含义改写消费行为。
- 监听注册/复用、SQLAlchemy DSN 转换、channel/environment payload 过滤、单次消费 notification、连接失败的有界等待与轮询降级保持原规则；取消继续传播。移除 listener 失败仍继续关闭 connection，loader 仍先关闭通知、再执行 shutdown，最后关闭自己持有的 loop。
- 行情分页、恢复窗口、Universe 获取及原有 getattr 能力检测保留原实现。原生 SQL、通知 channel、重试秒数和关闭失败语义没有修改；新模块没有另一个转发层或提交路径。
- 两个移动类及剩余纯消费代码 AST 与原实现一致。适配器的事件循环/连接归属明确后，纯消费模块可以在禁止数据库与 persistence 导入的独立进程内加载；实际 Postgres LISTEN 不作为已验收能力。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2372 passed**，25.88 秒。新增通知适配测试 **14 passed**，覆盖三种 DSN、连接复用/幂等关闭、注册失败清理、六类 channel/payload、通知消费/超时、取消、连接失败的 bounded retry、listener 移除失败继续关闭；与原消费及架构测试合计 **64 passed**。新增 live_source 无数据库导入检查。新适配器定向 `mypy --follow-imports=skip --disable-error-code=unused-ignore` 通过，禁用项仅处理跳过具体仓储导入后原 kwargs 注释被判 unused 的情形，不代表全仓类型验收；核心文件/新增测试完整 Ruff、CLI/迁移测试 F/I 与 git diff --check 通过。

本批无 schema 变更、生产发布或服务器采样。策略源的可选唤醒检测、Universe 读取能力探测仍需按实际接口继续核查；真实 Postgres 通知、并发、原子失败回滚与进程重启仍未验收，连接替身测试不能替代这些验收。

## 第二十九批：显式行情唤醒与 Universe 选币读取接口

第二十八批提交为 `a2100eb`；第二十九批继续本地实施，未部署生产。

- live_source 定义 RuntimeStateWakeup，提供 prepare_wakeup 与 wait_for_data；行情源显式接收可选 wakeup，默认仅轮询。生产 CLI 注入同一个原生 loader 作为读取与唤醒实现，删除消费循环中对 loader 的 getattr/callable 探测。准备返回 False 或异常继续轮询降级，None 保留原启用语义；通知只是提示，耐久游标读取继续是事实来源。
- domain/universe/ports.py 增加 UniverseSymbolReader，只要求 load_active_entry_symbols_at 和 load_positive_gainer_symbols_at。原生同步 loader 依赖这两项异步能力，直接在自己的 loop 执行；None 仓储仍返回空集合，最新读取传 None，历史读取传实际截点，排名读取保留 top_count。
- 删除旧 membership 方法和可选排名方法探测。生产原生 Universe 仓储原本已实现两项能力，SQL/选币行为不变；旧替身必须实现明确接口，已同步迁移，不保留兼容转发。无接口能力的非原生旧实现不再静默降级，这是明确的调用契约迁移。
- 消费与 loader 的关闭归属保持，仍由行情源 finally 调用 loader.close；独立 wakeup 接口不接管资源关闭，生产通知 connection 继续由同一 loader 所有。没有新增数据库查询、提交路径或扩大活跃 entry symbols 范围。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2377 passed**，24.84 秒。新增 **5 项**测试覆盖独立唤醒的启用/拒绝/异常降级、指定历史截点的排名读取及无 Universe 时的空集合；消费/通知适配/架构定向 **69 passed**。核心文件及测试完整 Ruff、接口/原生 loader 定向 `mypy --follow-imports=skip --disable-error-code=unused-ignore`、git diff --check 通过；沿用上一批 kwargs 检查口径，不代表全仓类型检查。

本批无 schema 变更、生产发布或服务器采样。真实 Postgres 通知、并发、原子失败回滚与完整重启仍未验收；后续应继续核对接口迁移后的整体依赖和剩余仓储消费点，不能用替身回归宣称生产异常已关闭。

## 第三十批：资源关闭能力接口与生命周期依赖

第二十九批提交为 `cd870de`；第三十批继续本地实施，未部署生产。

- live_rollout/resource_ports.py 定义 AsyncStoppable、AsyncClosable、SyncClosable、AsyncDisposable、HealthStopMarker，分别表达生命周期实际调用的 stop、aclose、close、dispose、stopped。真实运行对象和关闭测试替身均结构化满足接口，不新增转发实现。
- LiveResourceLifecycle 删除仅为类型注解而导入的交易客户端、执行协调器、数据库 engine、entry runtime、行情源、signal recorder、telemetry 与具体 health 实现。原参数名称、可选性与注入点保持，只按实际关闭能力标注；原 volume-cache 单方法协议并入共用 stop 能力。
- 关闭的运行 AST 在移除注解差异后与原实现一致：entry runtime/entry orders/coordinator/trade client 仍依次关闭，独立资源继续并行，再并行 dispose engines；两个共享 candle source 按对象 identity 只关闭一次。总预算/单资源超时、错误记录、取消传播以及 finally 健康停止标记保持原代码。
- 资源测试移除原来冒充具体生产类型的 arg-type 忽略，FakeResource/HangingResource 直接满足所需接口；不通过 Any 或强制转换绕过接口。生产装配未改变资源创建、注册和关闭归属。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2380 passed**，25.22 秒。资源关闭/生命周期归属/会话及架构定向回归 **69 passed**，覆盖关闭顺序、共享对象单次关闭和挂起清理预算。新增资源端口和生命周期两项禁止数据库导入检查，以及生命周期禁止 daemon/market_loop/sqlalchemy/persistence/execution_account 的检查。接口、生命周期及关闭测试一起定向 `mypy --follow-imports=skip` 通过；所有本批 Python 文件完整 Ruff、git diff --check 通过，不代表全仓类型检查。

本批无 schema 变更、生产发布或服务器采样。结构隔离不能替代真实客户端/engine 关闭及进程重启验收；真实数据库通知、并发、原子失败回滚与完整重启仍待补齐，其他运行装配与仓储消费点仍需按实际归属继续核查。

## 第三十一批：entry runtime 快照读取与仓储装配分离

第三十批提交为 `76bdc63`；第三十一批继续本地实施，未部署生产。

- domain/universe/ports.py 增加单方法 UniverseSnapshotReader，表达按 observed_at 读取最新已激活快照。LiveEntryRuntime 显式接收该读取器，不再接收 SQLAlchemy session factory 或在内部构造 Postgres 仓储。
- runtime_orchestrator 按原 positive-gainer 配置条件创建 PostgresUniverseRepository 并注入；未开启选币池时传 None，保持原先不创建该仓储、不建立池缓存的规则。生产 SQL、session factory、快照截点与激活判断保持原生实现。
- 启用选币池却没有读取器时构造立即抛出 ValueError，避免直接注入遗漏后静默跳过池缓存/交易预热。该校验是明确接口的新配置错误路径；生产装配和测试调用点均已同步迁移，无旧 session-factory 参数兼容层。
- 具体交易客户端/EMA provider 仅作为 TYPE_CHECKING 注解导入；纯 entry runtime 的加载不再牵动数据库实现。现有缓存选择、刷新时间、EMA 回调、正涨幅过滤、margin/leverage warmup、停止流程等所有非构造方法 AST 与原实现一致。
- entry runtime 测试直接注入快照替身，删除猴子补丁 Postgres 构造器和虚假 session factory；保留生产使用的相同读取接口，未制造另一种快照数据来源策略。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2382 passed**，25.02 秒。entry runtime/entry cache/架构定向 **57 passed**，新增缺失读取器拒绝和 entry runtime 无 sqlalchemy/persistence 导入两项检查。Universe 端口、entry runtime 与它依赖的 entry_cache 类型所有者一起定向 `mypy --follow-imports=skip` 通过；核心文件/迁移测试完整 Ruff、编排 F/I、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子失败回滚和完整重启仍待补齐；运行装配仍负责创建具体实现，这类合理依赖不应为减少导入数量而再包一层转发。后续继续按业务实际消费能力核查剩余耦合与整体验收缺口。

## 第三十二批：人工缺失订单确认的纯安全规则

第三十一批提交为 `6caa9bd`；第三十二批继续本地实施，未部署生产。

- domain/execution/missing_order_rules.py 承接 validate_missing_order_resolution，纯规则只依赖订单状态与标准库。保留待对账状态、reduce-only、未记录交易所 ID、零成交、最小缺失年龄、交易所查无订单/无匹配挂单、受保护持仓数量未变的八项拒绝规则及原异常消息/顺序。
- missing_order_resolution 的运行用例通过领域模块命名空间使用规则，删除原模块的规则重导出。CLI 删除 _validate_missing_order_resolution 兼容转发，安全规则测试从 CLI 测试移到领域测试并直接使用同一函数，不保留另一套校验实现。
- 原订单查询、交易所只读核查、立即二次本地读取、证据 ID、命令/对账记录和 ABSENT_RECONCILED 事件写入与 cleanup 保持原运行 AST。没有交易所写入、人工操作或生产数据修改；现有写入事务边界及并发防护未在本批改造，也不能借纯校验提取宣称它们已完成额外验收。
- CLI 的确认字符串、参数和运行命令保留原流程。规则定义 AST 与原实现一致，本批新增的是隔离测试面与明确归属，未放宽人工恢复条件。

验证：完整单元及部署 smoke（开启 hub 网络测试）加两项 fake-service/fake-exchange 端到端测试 **2386 passed**，25.85 秒。规则/CLI/架构定向 **128 passed**；领域规则单独 **9 passed**，在迁移的六项用例上补齐非待对账状态、已有 exchange order ID、非零已成交数量三项拒绝。新增 missing_order_rules 无 sqlalchemy/persistence 导入检查。领域模块定向 `mypy --follow-imports=skip`、规则/运行用例/新增测试/架构完整 Ruff、CLI 及迁移测试 F/I、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子失败回滚和进程重启仍待补齐；运行装配中的具体仓储依赖继续保留在实际创建处，后续需要核对余下业务依赖与整体验收缺口。

## 第三十三批：租约恢复用例与耐久会话读取接口

第三十二批提交为 `b3d2589`；第三十三批继续本地实施，未部署生产。

- live_rollout/lease_recovery.py 独立拥有自动租约恢复与准入判定，定义 LiveSessionStateReader 和 LiveLeaseAcquirer；只依赖实际读取和获取租约能力，不导入 SQLAlchemy、ORM 或 Postgres。startup_resilience 保留启动重试与具体异常分类，删除旧恢复函数与重导出。
- PostgresLiveRolloutRepository.load_latest_operating_state 承接原查询：同 session、排除 preflight/shadow_preflight、按 occurred_at 降序取一条。SQL statement AST 保持；返回原始耐久状态字符串或 None，未知状态仍不能准入，不引入枚举转换错误。
- 运行装配显式注入原生会话仓储与风险仓储；首次启动和心跳分别保留 execution/heartbeat session factory。未拆分 acquire_lease 的原生事务或新增写入路径。
- gate 已通过或已有租约仍直接返回；此前已启用实盘、未排空且唯一阻塞为 missing_active_lease 才恢复。TTL、owner、code generation、随机 lease ID 与日志保持。原排空查询暂留运行编排，本批不宣称所有启动 SQL 已移除。

验证：最终完整本地回归 **2395 passed**；新增恢复用例 **7 项**覆盖非活跃/缺失耐久状态拒绝、排空与额外 gate 阻塞、成功租约字段及已有租约不读取/写入；新增仓储 SQL 编译测试检查过滤、排序、limit 与未知状态保留，属于替身查询验收；新增无数据库导入检查。核心文件与新增测试完整 Ruff、编排与 CLI 测试 F/I、lease recovery 定向 mypy --follow-imports=skip、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍未验收，替身测试与 SQL 编译不能替代真实 Postgres 验收。

## 第三十四批：排空判断共用耐久会话读取

第三十三批提交为 `27127a7`；第三十四批继续本地实施，未部署生产。

- live_rollout/session_state.py 拥有 LiveSessionStateReader 与排空判断；lease_recovery 从该所有者引用读取协议，不保留旧协议重导出。只有最新运行状态严格等于 draining 才返回 True，读取错误和取消继续传播。
- runtime_orchestrator 删除 _session_is_draining 及 LiveSessionTransitionRow 导入，首次启动、风险控制轮询与租约恢复分别调用既有 execution/heartbeat 会话仓储。旧排空查询与上一批 load_latest_operating_state 查询 statement AST 一致；同 session、排除 preflight/shadow_preflight、occurred_at 降序及 limit 1 均保持。
- 单次实盘计划使用自己已构造的会话仓储，通过同一判断为风险准入提供 is_draining 回调。删除 SessionDrainingLoader 参数和 CLI 从运行编排导入/注入旧私有查询的路径，未增加仓储或兼容转发层。
- 不合并事务内订单提交状态校验，也不替换 Postgres 运行上下文的其他会话查询：它们的过滤语义和事务归属与此处不同。排空读取与租约读取仍各自执行，不以缓存改变原时序；本批不能视为并发问题已解决。

验证：完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2404 passed**，25.60 秒，保留一项现有 Starlette/httpx 警告。状态/租约/仓储/CLI/架构定向 **138 passed**。新增状态测试 **8 项**覆盖 draining、live_enabled、halted、completed、缺失与未知状态，以及查询失败、取消传播；新增独立进程无 sqlalchemy/persistence 导入检查。session_state、lease_recovery 两模块定向 mypy --follow-imports=skip，核心文件/测试完整 Ruff、编排/单次计划/CLI F/I 和 git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍未验收，运行编排其他 SQL 与不同语义的上下文查询继续按实际职责核查。

## 第三十五批：shadow 演练证据读取与告警归属

第三十四批提交为 `b01ae69`；第三十五批继续本地实施，未部署生产。

- PostgresShadowRepository.has_matching_completed_session 承接编排中的 shadow 查询，按 strategy_name、strategy_config_hash 和 completed 状态过滤、ended_at 降序取一条。不新增时间窗口，旧匹配记录仍可通过；SQL statement AST 与原查询一致。
- live_rollout/shadow_preflight.py 定义 CompletedShadowSessionReader 并拥有缺失记录告警。匹配时不记录缺失事件；未匹配时仍 warning，acknowledged 时仍 info；事件名、详情字段及错误/取消传播保持，不新增提交阻塞。
- daemon 与单次计划在原装配处创建 shadow 仓储，分别使用原 execution/session factory。删除 CLI 注入旧告警函数、单次计划的 ShadowPreflightWarning 参数和编排的两个旧私有函数，无旧接口转发或重导出。
- runtime_orchestrator 不再包含直接 select 查询或 ORM 行导入，但仍负责具体运行装配，并依赖 session/engine 和其他原生适配器；不能据此宣称全部编排已解耦。
- 原三个 CLI shadow 测试迁至新职责所有者，保留原替身查询与日志断言，补充显式读取替身的匹配无告警、连接错误与取消传播测试。

验证：完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2408 passed**，25.87 秒，一项现有 Starlette/httpx 警告。新增所有者测试 **6 passed**；迁移三项测试后 CLI/shadow/架构定向 **122 passed**，后续补充的三项测试已纳入完整回归。新增 shadow_preflight 独立进程无数据库导入检查。核心文件/所有者测试完整 Ruff、编排/单次计划/CLI/迁移 CLI 测试 F/I、shadow_preflight 定向 mypy --follow-imports=skip、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐；shadow SQL 替身测试不替代真实数据库验收。

## 第三十六批：单次实盘计划读取归属与 CLI 查询移除

第三十五批提交为 `5d5fda2`；第三十六批继续本地实施，未部署生产。

- PostgresOrderReadRepository.load_approved_intent_notional 承接 CLI 中的耐久批准金额查询，按 intent_id 读取 details.desired_notional。原 SQL 和解析 AST 保持：非 dict、缺失或 None 返回 None，其他值按 Decimal(str(value)) 转换；非法金额仍抛异常，不静默忽略。
- 单次计划通过已创建的订单读取仓储获取批准金额，风险配置与账户状态直接引用既有 persistence/postgres/runtime_context 读取函数。plan_runner 本就是具体装配所有者，本批不再让 CLI 提供相同原生能力，也不新增适配转发层。
- 删除 LatestRiskConfigLoader、LatestAccountStateLoader、ApprovedIntentNotionalLoader 三个回调参数/类型与 CLI 注入点，移除 CLI 的旧金额函数、select 和 OrderIntentExecutionRow 导入。CLI 仍保留命令入口、资源创建和其他用例装配，不宣称其全部纯化。
- 原读取顺序、session factory、live gate、缺失批准金额拒绝、当前风险 cap 与 operator-approved cap 检查保持，不新增批准或订单提交路径，不改变提交事务。

验证：完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2416 passed**，25.67 秒，一项现有 Starlette/httpx 警告。金额读取及 CLI 定向 **77 passed**；新增 **8 项**替身测试覆盖缺失/非字典/空字段、字符串/零/浮点金额、非法金额，并检查查询 intent_id。金额查询与解析 AST 等价检查通过。订单读取仓储定向 mypy --follow-imports=skip、仓储及新增测试完整 Ruff、单次计划/CLI F/I、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实 Postgres 通知、并发、原子回滚与完整进程重启仍待补齐；新增金额替身测试不替代真实数据库或实盘执行验收。

## 第三十七批：订单身份冲突分类的明确归属

第三十六批提交为 `b5e3b19`；第三十七批继续本地实施，未部署生产。

- live_rollout/order_identity_errors.py 独立拥有 is_runtime_order_identity_conflict 与 is_durable_order_identity_conflict。两套判定范围不同，保留分别命名的原规则，不将 reservation/type-name 分类扩大到退出处理器。
- 运行通道继续识别五类耐久消息、ReservationConflictError 类名及 OrderPreSubmissionError 的特定消息，并递归检查 cause；退出处理器继续只识别五类耐久消息和嵌套 cause。原特殊消息常量由 runtime_config 移到分类所有者。
- 编排与退出处理器通过模块命名空间直接消费分类规则，删除各自旧私有函数；原测试直接从规则所有者导入，不保留旧函数重导出。通道回调注入、错误恢复、退出提交和重试流程保持。
- 两个函数在名称及递归引用归一化后 AST 与原实现一致。现有基于错误消息/类名的分类没有改为新的异常体系，本批不宣称解决所有错误分类或并发问题。

验证：完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2422 passed**，26.11 秒，一项现有 Starlette/httpx 警告。规则/退出处理器/身份 epoch/架构定向 **68 passed**。新增 **5 项**覆盖 reservation 类名及嵌套 cause 仅影响运行通道、三类嵌套耐久消息同时被两套规则识别；新增独立进程无 sqlalchemy/persistence 导入检查。规则及新增测试完整 Ruff、迁移文件 F/I、规则定向 mypy --follow-imports=skip、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 后续实施顺序

1. 补齐真实 Postgres 并发、原子回滚与完整进程重启验收；第二批已完成自愈事务迁移及提交后重载契约。
2. ExecutionBook 内部协作者：第三批已提取恢复/检查点计算；第四批已提取命令状态与 reservation 计算；第五批已提取命令/outbox 编解码与恢复校验；第六批已明确命令仓储接口并提取显式兼容适配；第七批已提取 reservation 仓储接口与显式兼容装配；第八批已提取证据模型、去重/覆盖规则与累计水位计算；第十六批已提取成交 identity/恢复前缀计划、退出结算判定及真实账户成交水位增量；第十七批已提取订单证据的 outbox/释放/恢复判定；第十八批已提取累计报告结算与水位持久化计划；第十九批已提取候选证据分组协调及观察结果模型；第二十批已提取耐久证据准入与水位写入计划；锁、核心事务和状态所有权继续由 Book 统一管理，不为缩短类而拆开事务。统一 mutation lock、候选状态、事务与提交后发布仍由 Book 所有。
3. 聚合仓储与运行装配：第九批已拆出执行命令/恢复/水位/对账仓储；第十一批已拆出意图占用/原子提交仓储，第十二批已独立事件/成交仓储与状态机事件端口，第十三批已独立订单读取仓储及领域读取接口，第十四批已独立外部订单接管仓储，第十五批已分离计划与 shadow suppression 接口，继续处理运行用例的装配，第十批已提取实盘执行 runtime factory，第二十三批已独立 Hub 游标恢复与整批确认状态；第二十四批已分离 daemon 提交与 checkpoint 接口并删除聚合转发适配器；第二十五批已独立行情运行契约并清理 daemon/live_rollout 门面及生命周期注解依赖，第二十六批已独立 startup recovery 的三项行情读取接口与领域分页游标，第二十七批已独立研究采集补回的两项分页接口并清理采集包急切导入，第二十八批已将策略的同步读取/通知具体适配移至 Postgres 所有者，第二十九批已明确可选唤醒与 Universe 读取接口，第三十批已独立资源关闭能力接口，第三十一批已分离 entry runtime 的快照读取与 Postgres 装配，第三十二批已独立人工缺失订单确认的纯安全规则并删除 CLI 转发，继续核对整体依赖及剩余仓储消费点。
4. 第二十一批已解除恢复模型/codec 的摘要循环，保留既有 checkpoint digest 与跨 epoch 保护；第二十二批已清理 execution 包门面的急切导入并迁移活跃调用点，继续核查其他模块级延迟环。

生产账户事实覆盖冲突和策略状态发布缺失属于尚未关闭的运行问题；本次结构迁移不能作为这些问题已经修复的证据。
