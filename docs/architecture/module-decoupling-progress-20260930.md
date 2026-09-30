# 模块解耦实施进度（2026-09-30）

实施基线：`8eb059d`。本地已完成九十三批实施与验收，尚未完成审计文档中的全部重构。这些结构改动未发布到生产；此前生产运行版本为 `259c8e0`，本批未重新采样服务器。

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

## 第三十八批：共用瞬时运行异常分类

第三十七批提交为 `da12a7d`；第三十八批继续本地实施，未部署生产。

- live_rollout/runtime_errors.py 拥有共用 is_transient_runtime_error，统一 SQLAlchemyError、TimeoutError、ConnectionError 与 OSError 四类原规则。模块明确依赖原生数据库异常类型，不标记为无数据库依赖的纯领域模块。
- market_loop 与 runtime_orchestrator 删除重复私有分类函数，直接使用共享规则；通道回调注入仍保持。startup_resilience 引用同一规则，启动专属 BinanceRateLimitError 和 live gate blocked 前缀仍保留原分支。
- 两个原运行期函数体与共享实现 AST 一致。分类不递归扩展 cause，不把 gate/configuration 错误引入运行期重试，也不改变现有重试预算、通道终止或启动退避行为。

验证：完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2430 passed**，25.73 秒，一项现有 Starlette/httpx 警告。规则/启动/daemon/CLI 定向 **140 passed**；新增 **8 项**覆盖四类共用瞬时异常、三个运行期不可重试错误与包裹 cause 不扩大分类。规则及新增测试完整 Ruff、迁移文件 F/I、规则定向 mypy --follow-imports=skip、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第三十九批：可恢复 gate 阻塞规则归属

第三十八批提交为 `3f19b51`；第三十九批继续本地实施，未部署生产。

- 既有 live_rollout/gates.py 拥有 is_transient_live_gate，行情循环通过模块命名空间消费；删除 market_loop 私有判定及 daemon 测试对其导入，不新增转发模块。
- 规则保持非空 reasons 且全部属于 missing_active_lease、inactive_or_expired_lease、account_not_ready、unresolved_order_uncertainty 才允许原等待恢复分支。空、未知或混合 active_risk_halt 均返回 False；重复原因保持原 set 语义。
- 函数在名称归一化后 AST 与原实现一致，行情暂停、checkpoint、租约恢复、实际订单提交及重试时序未改。此处只明确 gate 规则的所有者，不代表所有 gate 阻塞都会自动恢复。
- 原 daemon 私有函数测试迁至 gate 所有者并扩大为十组判定；既有 daemon pending reconciliation 的真实运行替身测试继续保留。

验证：gate/daemon 定向 **82 passed**；最终完整本地回归 **2439 passed**。十组判定覆盖四种单原因、组合、重复、空集合、风险停机及未知原因。gate 模块及其测试完整 Ruff、行情循环/daemon 测试 F/I、gate 定向 mypy --follow-imports=skip、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第四十批：行情策略数据要求的显式接口

第三十九批提交为 `e529a9d`；第四十批继续本地实施，未部署生产。

- LiveRuntimeStrategy 增加 required_data() -> StrategyDataRequirement | None。生产策略原本实现该方法并返回正式领域模型；行情循环直接读取 max_gap_seconds 与 base_state_interval_seconds，删除方法/字段 getattr/callable 能力探测。
- 显式返回 None 表示沿用原无最大 gap 限制和 15 秒默认间隔；缺少方法的实现现在抛 AttributeError，不再静默默认为可运行，这是明确的调用契约迁移。方法仍在原两个读取点调用，不以合并读取改变时序。
- 共享 shadow FakeStrategy 补齐显式 None；GapAwareFakeStrategy 使用正式 StrategyDataRequirement，保留 30 秒 gap 和 15 秒间隔。其余生产与仓库内活跃调用通过回归验证。正间隔校验与现有 gap/暂停/checkpoint 行为保持。
- 新增三项契约测试覆盖非默认 60/120 秒要求及两项缺失方法拒绝；首次回归暴露旧替身缺失接口，补齐后重新完整执行通过，未把失败回归作为验收证据。

验证：完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2442 passed**，25.82 秒，一项现有 Starlette/httpx 警告。daemon 单独 **61 passed**。契约及新增测试完整 Ruff、行情循环及迁移替身 F/I、契约定向 mypy --follow-imports=skip、git diff --check 通过，不代表全仓类型验收。

同时检查真实数据库验收环境：docker version 守护进程探针三秒超时；PATH 未找到 postgres/initdb/pg_ctl，常见 Homebrew PostgreSQL 目录也未找到。本批未连接生产或重复探测已知握手异常的 54329 数据库，未取得真实 Postgres 验收证据。真实通知、并发、原子回滚与完整重启仍待补齐。

## 第四十一批：显式策略预热与单币恢复能力

第四十批提交为 `40843b6`；第四十一批继续本地实施，未部署生产。

- LiveRuntimeStrategy 增加 reset_symbol；既有 warm_market_state 接口在行情回补与 gap 恢复中改为直接调用。生产三种策略原本均具备这些方法，删除对方法的 getattr/callable 探测，缺失能力不再静默跳过。
- 入池重置、行情连续性缺口、market gap generation、预热失败清理与最大 gap 重置统一直接调用实际策略方法。原阈值、单币范围、checkpoint forget/record 的先后关系与生产有效实现路径保持；不满足接口的外部旧实现现在失败，是明确调用契约变化。
- gap 恢复完整性校验直接读取 required_data；显式 None 仍没有额外字段要求，正式模型使用 required_fields。按字段名检查行情值的 getattr 保留，因为字段列表是数据要求本身；没有把它误当方法能力探测移除。
- 共享 shadow FakeStrategy 显式提供原默认场景的无操作预热/重置；原 production 与 fake E2E 流程通过完整回归。没有新 SQL、写入事务或生产数据修改。

验证：修改后的完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2442 passed**，25.53 秒，一项现有 Starlette/httpx 警告；daemon **61 passed**。随后补充三项重置契约测试，契约文件合计 **6 passed**，覆盖阈值等于/超过、缺失 reset 失败；新增三项未重新计入完整回归的数量。契约/新增测试完整 Ruff、行情循环及迁移替身 F/I、契约定向 mypy --follow-imports=skip、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第四十二批：启动恢复共用显式策略数据要求

第四十一批提交为 `1e44c8f`；第四十二批继续本地实施，未部署生产。

- startup_recovery 的 warmup 秒数、最低 bucket 数、恢复行数预算、覆盖校验及完整 warmup symbols 判断直接调用 required_data()。删除该能力与其采样/字段属性的动态探测，与实时恢复共同消费既有 LiveRuntimeStrategy 契约。
- 显式 None 保留原默认语义：15 秒间隔、最低一 bucket、零基础 warmup 加 16 buffer 的历史窗口、覆盖校验跳过及目标 symbols 视为完整。正式要求使用模型字段，保留 max(1, interval)、最小历史窗口、全 symbols 行数预算与连续 bucket 校验。缺少方法不再静默跳过，是此前已明确的接口迁移。
- 原恢复查询、cutover 等待、持仓 symbols 合集、checkpoint 清空/预热/发布顺序未改。repository 的可选 symbols 能力与预热方法的旧错误提示仍保留，本批不宣称清理所有动态探测。
- CLI 测试的一处旧部分数据要求替身补齐原默认的间隔和空字段；首次失败回归不作为验收证据，修正后完整重跑。新增四项测试覆盖显式 None、15/60 秒要求、warmup 数量/窗口/跨 symbols 行数预算及缺失方法失败。

验证：最终完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2449 passed**，26.57 秒，一项现有 Starlette/httpx 警告。启动契约/恢复/CLI 定向 **81 passed**；新增文件 **4 passed**。恢复模块与新增测试完整 Ruff、迁移 CLI 测试 F/I、git diff --check 通过。单独 startup_recovery 的 mypy --follow-imports=skip 未通过：两处 checkpoint 返回值 no-any-return 和一处旧 unused-ignore；不将此项记为通过，也不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第四十三批：启动恢复类型契约验收缺口关闭

第四十二批提交为 `ef8d42c`；第四十三批继续本地实施，未部署生产。

- market_runtime_contracts 与 startup_recovery 直接从 domain/strategy/models 导入实际模型，不再通过 strategy 包重导出获取这些注解，明确模型类型所有者。此处不宣称 Python 加载子模块会绕过包初始化。
- startup_recovery 内部 _WarmupPageArguments TypedDict 明确 environment、cursor、limit、upper_bound 与 NotRequired symbols。保留原动态字典的运行值及 symbols 仅在显式 warmup_symbols 时传入的规则，删除 load_after 的 arg-type 忽略，没有 Any/cast 或错误码禁用。
- 除 load_kwargs 注解差异外，所有 startup_recovery 函数 AST 与上一批一致。查询、预热、checkpoint、预算和错误路径均不改；本批补类型契约，不新增行为测试。

验证：五个实际类型所有者/消费者一同定向 mypy --follow-imports=skip **通过**：startup_recovery、market_runtime_contracts、domain/strategy/models、domain/market/runtime_state_repository、domain/market/runtime_state_models。第四十二批报告的三项单文件类型错误在此明确口径下关闭；不代表全仓类型验收，也不把忽略依赖的单文件检查作为完整证据。核心文件完整 Ruff、git diff --check 通过。启动契约/恢复/CLI/架构定向 **132 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2449 passed**，25.41 秒，一项现有 Starlette/httpx 警告。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第四十四批：启动预热 symbols 的明确读取能力

第四十三批提交为 `b938ebf`；第四十四批继续本地实施，未部署生产。

- RuntimeMarketStateReadRepository 增加 load_symbols_at(environment, observed_at)，表达指定时间及之前最新耐久 bucket 的 symbols。分页基础接口不变；原生 PostgresRuntimeMarketStateRepository 已具备该方法，SQL 无修改。
- load_live_warmup_symbols 删除 getattr/callable 探测，直接使用明确读取能力。显式 symbols 包括空集合仍直接返回，不查询；未指定时按原环境和时间截点读取，过滤空白 symbol。缺失方法现在失败，不再静默当作空预热集合，这是调用契约迁移。
- 旧 CLI 恢复替身显式实现空 symbols 查询，保持它们此前依赖缺失能力时的默认测试场景，不修改生产数据范围或引入新选币来源。原预热、cutover、checkpoint、事务和提交路径保持。

验证：完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2455 passed**。新增 **6 项**覆盖精确查询截点、空白过滤、显式空/非空 symbols 跳过查询、缺失能力拒绝、连接错误和取消传播。symbols/恢复/CLI/架构定向 **134 passed**。与第四十三批相同的五文件定向 mypy --follow-imports=skip 通过；核心文件及新增测试完整 Ruff、迁移 CLI 测试 F/I、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐；既有 SQL 的替身接口验收不能替代真实数据库验收。

## 第四十五批：启动恢复直接消费策略恢复能力

第四十四批提交为 `56eef7d`；第四十五批继续本地实施，未部署生产。

- warm_live_strategy 直接绑定 strategy.warm_market_state；restore_live_strategy_from_checkpoint 直接绑定 warm_market_state 与 clear_market_state_buffers。删除这些已在 LiveRuntimeStrategy 中声明的方法的 getattr/callable 探测，不新增接口转发或另一套恢复实现。
- 绑定仍发生在入口、读取仓储之前；恢复仍先清空旧派生缓存，再查询耐久历史并预热，最后验证覆盖并生成 compact checkpoint。生产策略原本实现两项方法，正常路径和 cutover/query/提交顺序保持。
- 不符合契约的旧实现缺失属性时现在抛 AttributeError，替代原自定义 RuntimeError 提示；非可调用属性调用时失败。这是明确的接口错误路径变化，不宣称错误类型完全不变。

验证：完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2458 passed**。恢复能力/启动恢复/CLI/架构定向 **131 passed**；新增三项能力测试覆盖预热入口缺失、checkpoint 恢复缺失、缺少清空能力均在查询/重放前拒绝。五文件定向 mypy --follow-imports=skip（恢复、运行策略契约及实际 strategy/market 模型与读取接口）通过；核心文件和新增测试完整 Ruff、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐；结构性接口迁移不能代替生产异常验收。

## 第四十六批：checkpoint writer 进度接口直接消费

第四十五批提交为 `70a8d5a`；第四十六批继续本地实施，未部署生产。

- LiveCheckpointCoordinator 原构造契约已要求 CheckpointWriter，现在直接调用 attach_clock 并读取 last_persisted_token、last_persisted_monotonic，删除 hasattr/getattr 能力探测。没有创建新转发接口或改变 writer 所有权。
- writer 仍在耐久持久化完成后发布 token/完成时间；协调器只在 token 前进时同步进度。初始化/重置中的 None 时间回退保持，保存成功后也明确处理 None 回退当前 perf_counter，避免把可空时间赋给 float。原生 writer 保存成功本来提供完成时间，因此正常成功路径不变。
- 协调器的 checkpoint 模型注解直接引用实际 models 所有者。缺失 writer 能力不再静默降级；writer coalescing、重试、同步保存、提交后回调与关闭未改。策略 checkpoint 参数签名探测仍保留，本批不宣称所有兼容探测均移除。

验证：协调器既有回归 **13 passed**，涵盖耐久进度、dirty 预算和保存/失败场景；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2458 passed**。checkpoint_coordinator、checkpoint_writer、domain/strategy/models 三文件定向 mypy --follow-imports=skip 通过，未禁用错误码；核心文件完整 Ruff、git diff --check 通过，不代表全仓类型验收。本批没有新增镜像实现的测试，沿用真实 writer 的既有回归。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第四十七批：紧凑 checkpoint 契约直接调用

第四十六批提交为 `b90f8f2`；第四十七批继续本地实施，未部署生产。

- _checkpoint_for_persistence 直接调用已声明的 checkpoint(include_market_state_buffers=False)。删除 inspect.signature/Parameter 探测与旧无参数调用后手工删除 market_state_buffers/signal_buffers 的兼容分支，compact checkpoint 的实现由实际策略所有。
- 生产策略已支持此参数，正常日志、Hub cursor 附加、保存调度与 writer 事务规则保持。旧无参数策略实现现在失败而非走备用路径，是明确的接口迁移；共享 shadow FakeStrategy 补齐相同参数。
- 本批不新增备用适配或第二种 checkpoint 编码，保留既有策略 compact 行为与身份/epoch 规则。

验证：checkpoint 协调器及 shadow service 定向 **17 passed**，原协调器测试继续断言 include_market_state_buffers=False。checkpoint_coordinator、checkpoint_writer、domain/strategy/models 三文件定向 mypy --follow-imports=skip 通过；核心文件完整 Ruff、迁移替身 F/I、git diff --check 通过，不代表全仓类型验收。完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2458 passed**。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第四十八批：缓存维护移除无效 telemetry 依赖

第四十七批提交为 `d063f6d`；第四十八批继续本地实施，未部署生产。

- LiveRuntimeCacheMaintenance 删除未消费的 telemetry 构造参数与 self._telemetry，daemon 删除对应注入；缓存测试删除无效 telemetry 替身及传参。
- 删除未使用的 _evicted_telemetry_series 赋值，并修正类说明为实际的策略缓存维护。除该无用赋值外，prune 函数 AST 与原实现一致。
- 当前币、活跃池、托管持仓/订单、pending entries 和策略自身保护集的合并规则、分钟清理节奏、15 分钟冷缓存期限及内存日志保持。telemetry 在其他运行模块的实际职责不变，不新增其清理或指标。
- 策略缓存能力仍是部分生产策略具备的可选能力；没有将其他策略强制改为支持缓存清理，不把合法可选接口误作已删除能力。

验证：缓存/daemon 定向 **63 passed**；runtime_cache 定向 mypy --follow-imports=skip 通过；核心文件及缓存测试完整 Ruff、daemon F/I、git diff --check 通过，不代表全仓类型验收。完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2458 passed**。本批是无效依赖清理，沿用保护集/节奏/日志既有测试，无新增镜像实现测试。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第四十九批：缓存维护可选能力显式注入

第四十八批提交为 `e9f423a`；第四十九批继续本地实施，未部署生产。

- LiveRuntimeCacheMaintenance 显式接收可选 strategy_protected_symbols 与 StrategyCachePruner，后者以 Protocol 声明 now/protected_symbols/inactive_after 关键字参数及退出 symbols 返回值。运行清理不再探测策略方法。
- daemon 在构造缓存维护时检查实际策略能力并绑定回调；只有部分生产策略具备清理能力，两项回调独立可选，缺失/非 callable 仍按原无该能力处理。能力在装配时确定，不支持实例运行中猴子补丁方法后再探测，这是明确的生命周期契约变化。
- strategy 对象仍仅用于可选缓存数量指标读取，保留 getattr 指标读取及其 None 默认；本批不宣称所有 object 依赖已经移除。保护 symbols 合集、清理预算/分钟间隔、冷缓存期限、日志与 volume metrics 行为保持。
- 测试直接注入实际策略绑定方法，新增无回调时即使对象存在同名方法也不会隐式调用的检查。无新的策略实现转发层或缓存清理事务。

验证：缓存/daemon 定向 **64 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2459 passed**。runtime_cache 和缓存测试一起定向 mypy --follow-imports=skip 通过；核心文件/测试完整 Ruff、daemon F/I、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第五十批：缓存维护移除策略对象依赖

第四十九批提交为 `02f6535`；第五十批继续本地实施，未部署生产。

- runtime_cache 定义不可变 StrategyCacheMetrics，明确 buffered_symbol_count 与 buffered_state_count 两个可空计数。缓存维护接收可选 strategy_metrics_provider，删除 strategy: object 与 self._strategy。
- daemon 装配指标读取函数，将实际策略的两项可选属性转换为指标值；原缺失属性 None 保持。不在构造时冻结指标，内存 snapshot 和 cache-pruned 每条日志各重新读取，保留动态计数来源。
- 缓存维护只消费上一批的保护/清理回调与本批指标读取能力，不再对策略方法/属性做动态探测。未提供指标读取器时使用两个 None；指标读取异常仍传播，未为其增加吞错逻辑。
- 缓存保护合集、分钟节奏、15 分钟期限、volume 指标降级及日志字段保持。内部只负责消费数据，实际策略能力探测留在已有 daemon 装配处，没有新适配转发层或缓存所有权迁移。

验证：缓存/daemon 定向 **64 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2459 passed**，25.36 秒，一项现有 Starlette/httpx 警告。缓存模块与其测试一起定向 mypy --follow-imports=skip 通过；核心文件/测试完整 Ruff、daemon F/I、git diff --check 通过，不代表全仓类型验收。保留现有计数/保护/日志回归，无新增镜像实现测试。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐；累计五十批本地结构迁移不表示整个重构或生产异常验收已经完成。

## 第五十一批：entry EMA 缓存读取与清理能力接口

第五十批提交为 `d66369d`；第五十一批继续本地实施，未部署生产。

- entry_cache 定义 EntryEmaProvider，明确同步 load(symbol, observed_at) 与 prune(now, protected_symbols, inactive_after, max_boundaries_per_symbol) 两项实际使用能力。缓存不再依赖具体 ClosedCandleEmaProvider 类型，EMA snapshot 值类型仍引用原所有者。
- _prune_ema_provider 直接在原 provider lock 中调用 prune，删除动态能力探测。原生产 provider 已实现两项方法，无新转发实现；缺失清理能力的外部旧实现不再静默跳过，是明确的契约变化。
- 三个缓存测试 provider 显式实现原无缓存清理场景的 prune 返回零，删除其构造点的 arg-type 忽略。读取/后台 warmup/取消/停止流程未改，保护 symbols、一小时期限和每币 32 个边界保持。

验证：entry cache/runtime/架构定向 **62 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2459 passed**，25.11 秒，一项现有 Starlette/httpx 警告。entry_cache、entry_runtime、domain/universe/ports 三文件定向 mypy --follow-imports=skip 通过；核心文件及迁移测试完整 Ruff、git diff --check 通过，不代表全仓类型验收。实际 provider 的既有单元清理回归包含于完整单元测试；不代表真实外部 candle 读取验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第五十二批：entry runtime 消费实际预热与 EMA 能力

第五十一批提交为 `fdb0fd1`；第五十二批继续本地实施，未部署生产。

- EntryExchangeWarmup Protocol 声明 warm_entry_margin_type、warm_entry_leverage 与 configured_margin_type_count，LiveEntryRuntime 不再标注具体 BinanceUsdMTradeClient。原生客户端的同名方法与属性直接满足接口，未增加转发实现。
- EMA 参数沿用 EntryEmaProvider，删除具体 ClosedCandleEmaProvider 类型导入。entry runtime 与缓存共享实际能力契约，不新增另一套 provider 协议。
- 删除 TYPE_CHECKING 中的具体客户端/provider 导入；LiveEntryRuntime 在去除注解后 AST 与上一批一致。持仓池读取、margin/leverage 预热顺序、背景 cache 配置、EMA 获取及停止流程保持。
- 测试客户端与 EMA 替身删除旧 arg-type 忽略；EMA 替身显式实现 prune 并返回正式 ClosedCandleEmaSnapshot，保留原数值与来源 ID。

验证：entry runtime/cache/架构定向 **62 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2459 passed**，25.82 秒，一项现有 Starlette/httpx 警告。entry_cache、entry_runtime、domain/universe/ports 三文件定向 mypy --follow-imports=skip 通过；核心文件和迁移测试完整 Ruff、git diff --check 通过，不代表全仓类型验收。本批仅调用能力类型迁移，沿用现有行为回归。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第五十三批：控制平面消费者健康记录能力

第五十二批提交为 `4dfc2a8`；第五十三批继续本地实施，未部署生产。

- telemetry_ports.py 定义 ConsumerHealthSink，仅暴露同步 consumer_health 及原 consumer/available/occurred_at/reason/recovery/lag/sequence 参数、默认值。原 LiveTelemetrySink 继承该接口，删除重复声明；完整 sink 其他能力不变。
- LiveControlPlaneRuntime 依赖 ConsumerHealthSink，不再导入完整 telemetry 模块。实际 telemetry recorder 与 FakeTelemetry 直接满足单方法能力，无新增转发或记录实现。
- 控制平面函数在去除注解后 AST 与原实现一致；账户快照可用性、行情 gap 标记、lease 更新与健康事件发布顺序均保持。同步记录/有界队列/实际 telemetry 生命周期未迁移。

验证：控制平面/telemetry/架构定向 **78 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2461 passed**，26.15 秒，一项现有 Starlette/httpx 警告。新增 telemetry_ports、control_plane 两项独立进程禁止 sqlalchemy/persistence 导入检查。telemetry_ports、control_plane、实际 context 接口三文件一起定向 mypy --follow-imports=skip 通过；仅前两文件检查时 context 被跳过会出现 Any 基类错误，已通过纳入实际所有者处理，未禁用错误码。核心文件完整 Ruff、telemetry F/I、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第五十四批：账户通道依赖成交记录能力

第五十三批提交为 `631f8d9`；第五十四批继续本地实施，未部署生产。

- telemetry_ports 增加 AccountFillSink，仅声明原异步 account_fill(event, occurred_at)。AccountEvent 仅 TYPE_CHECKING 引用，接口模块不急切加载事件通道实现。
- LiveTelemetrySink 继承 AccountFillSink，删除重复方法声明；LiveAccountEventRuntime 仅依赖这一项能力，不再导入完整 telemetry 模块。实际 recorder 与现有 fill 替身直接满足能力，无新的记录转发实现。
- 账户 runtime 在移除注解差异后 AST 与上一批一致。fill identity 去重、异步成交记录、订单对账、账户快照应用和退出决策顺序保持；telemetry recorder 队列与持久化逻辑未迁移。

验证：account channel/telemetry/架构定向 **81 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2461 passed**。telemetry_ports 定向 mypy --follow-imports=skip 通过；核心接口/账户通道完整 Ruff、telemetry F/I、git diff --check 通过，不代表账户通道或全仓类型验收。沿用现有成交去重及快照顺序行为回归，无新增镜像实现测试。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第五十五批：账户通道退出处理能力与 daemon 导入隔离

第五十四批提交为 `30ea917`；第五十五批继续本地实施，未部署生产。

- account_event_ports.py 定义 AccountEventExitProcessor，仅要求异步 process_account_event(state, quote) -> str | None，LiveAccountEventRuntime 改为依赖此能力，删除具体 LiveStrategyDaemon 导入。原 daemon 与账户测试替身直接满足接口，没有新增处理转发实现。
- exit_channels 的 daemon 导入移至 TYPE_CHECKING，并开启延迟注解；退出通道仍标注其原使用类型。本批不宣称退出通道全部能力已收窄，目标是解除账户通道经共享 retry helpers 的间接 daemon 导入。
- 账户事件去重、记录、对账、快照应用和 process_account_event 调用/重试顺序不变；退出通道循环及重试逻辑未改。

验证：生产改动后的完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2461 passed**，26.11 秒，一项现有 Starlette/httpx 警告。随后补充三项导入检查：账户退出能力无 sqlalchemy/persistence 导入，以及 account_channel/exit_channels 不加载 daemon 实现；全部纳入账户/退出通道/架构定向 **66 passed**。新增三项未计入前述完整回归数量。接口定向 mypy --follow-imports=skip、核心文件和架构测试完整 Ruff、git diff --check 通过，不代表所有消费模块或全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第五十六批：独立退出通道处理能力接口

第五十五批提交为 `869e46e`；第五十六批继续本地实施，未部署生产。

- exit_channel_ports.py 定义 ExitChannelProcessor，明确托管持仓 symbols 与 market quote、closed candle、grace timeout 三种异步处理，原参数/返回失败原因能力保持。事件和 quote 类型仅 TYPE_CHECKING 引用。
- LiveExitChannelRuntime 删除具体 daemon 的类型导入，改为消费四项所需能力；原 daemon 直接满足接口，没有新增处理转发实现。运行循环、重试、错误通知与关闭不变。
- note_order_identity_conflict 的既有可选 hasattr 检查保留，不在本批强制旧替身支持该能力；不宣称退出通道所有动态探测均完成迁移。

验证：退出通道/CLI/架构定向 **129 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2465 passed**。包含第五十五批后补三项导入检查及本批一项新接口无 sqlalchemy/persistence 导入检查。新接口定向 mypy --follow-imports=skip、核心文件与架构测试完整 Ruff、git diff --check 通过，不代表消费模块或全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第五十七批：退出身份冲突通知显式注入

第五十六批提交为 `b735aa1`；第五十七批继续本地实施，未部署生产。

- LiveExitChannelRuntime 接收可选 on_order_identity_conflict 回调，quote/candle/grace 三条异常路径不再探测 daemon 的 note_order_identity_conflict。未配置仍跳过通知，通知先于原 on_exit_failure 发布。
- 生产编排直接绑定 daemon.note_order_identity_conflict；历史 grace 测试/CLI 装配 helper 在构造时获取旧可选通知并传入，保留已有轻量替身行为。该装配处 getattr 仍存在，不宣称全仓动态探测已清零；退出循环中的方法能力探测已移除。
- 原冲突分类、重试/降级/日志、symbol 选择与退出处理顺序保持。运行时替换 daemon 方法不再被逐事件重新发现，通知绑定属于构造生命周期契约；未新增通知实现或第二条提交路径。

验证：退出通道/CLI/架构定向 **131 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2467 passed**，26.95 秒，一项现有 Starlette/httpx 警告。新增两项 quote 冲突测试覆盖有/无显式通知，验证不探测 daemon 方法和通知先于失败回调。exit_channels 与 exit_channel_ports 两文件定向 mypy --follow-imports=skip 通过；核心文件/新增测试完整 Ruff、编排 F/I、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第五十八批：退出通道消费异步事件流

第五十七批提交为 `6a38b34`；第五十八批继续本地实施，未部署生产。

- quote 与 closed candle 通道分别消费 AsyncIterable[RealtimeMarketQuote] 和 AsyncIterable[ClosedCandle15mEvent]，删除具体 WebSocketMarketQuoteSource、BinanceClosedCandle15mFeed 类型依赖。两个通道只需要异步迭代能力，实际行情源及现有测试替身直接满足，无新增转发适配器。
- 事件类型仅 TYPE_CHECKING 引用。生产源创建、连接与生命周期继续归运行装配；缓存仍有自身的行情类型依赖，本批不宣称全部间接导入已隔离。
- 去除参数注解后全部函数 AST 与上一批一致，消费、重试、缓存更新与冲突通知顺序保持。

验证：完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2467 passed**，26.69 秒，一项现有 Starlette/httpx 警告。exit_channels 与 exit_channel_ports 两文件定向 mypy --follow-imports=skip、核心文件完整 Ruff、git diff --check 通过，不代表全仓类型验收。复用现有异步源替身与行为测试，未增加镜像实现测试。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第五十九批：退出通道行情模型归属与源导入隔离

第五十八批提交为 `1683b29`；第五十九批继续本地实施，未部署生产。

- exit_channels 与 exit_channel_ports 的 RealtimeMarketQuote 类型引用改为领域模型所有者，删除经 quote_hub 获取领域类型的依赖。ClosedCandle15mEvent 仍由原 feed 模块拥有，仅 TYPE_CHECKING 引用；未搬迁事件或改变生命周期。
- 新增两个独立进程导入检查，禁止加载 quote_hub 和 closed_candle_feed 后，退出循环模块及处理能力接口仍能成功导入。这补齐上一批异步事件流改动的运行时导入隔离证据。
- 检查确认缓存已经直接引用领域模型，本批未额外创建缓存转发接口。两生产模块全部函数 AST 与上一批完全一致，消费、缓存与重试行为不变。

验证：退出通道/架构定向 **64 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2469 passed**，26.66 秒，一项现有 Starlette/httpx 警告。exit_channels 与 exit_channel_ports 两文件定向 mypy --follow-imports=skip、两核心文件与架构测试完整 Ruff、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第六十批：账户通道依赖事件对账能力

第五十九批提交为 `dd0cec8`；第六十批继续本地实施，未部署生产。

- account_event_ports 增加 AccountEventOrderReconciler，仅声明只读 run_id 与异步 reconcile_account_event(event)。账户通道删除完整 LiveOrderReconciliation 导入，原生产对账对象及现有替身直接满足接口，没有新增对账转发实现。
- 该接口的 AccountEvent 仅 TYPE_CHECKING 引用；原退出能力使用的 RealtimeMarketQuote 改为直接引用领域模型。账户输入已是 AsyncIterable，本批不再重复拆分。
- 去除参数注解后账户通道全部函数 AST 与上一批一致。先对账、后账户快照发布及退出处理、fill 去重、失败恢复通知、run_id 选择和重试顺序保持。实际订单读取与状态机协调仍归原对账模块。

验证：账户通道/架构定向 **68 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2471 passed**，26.76 秒，一项现有 Starlette/httpx 警告。新增两项独立进程检查，禁止导入具体 order_reconciliation 后，账户通道及能力模块仍可加载。account_event_ports 定向 mypy --follow-imports=skip、两核心文件与架构测试完整 Ruff、git diff --check 通过，不代表账户通道或全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第六十一批：事件流恢复按消费路径加载 Hub

第六十批提交为 `5a1f112`；第六十一批继续本地实施，未部署生产。

- stream_recovery 的事件类型移至 TYPE_CHECKING，四条流分别在执行时导入自己所需的 Hub 异常类型。账户通道不再因共用重试模块急切加载行情 state/quote 与风控 Hub。
- 共用 _resilient_stream 仍统一管理重连、日志与等待，无新增转发模块或第二套重试规则。去除局部导入后全部函数 AST 与上一批一致；行情 replay/epoch 致命错误、取消传播、正常结束、默认延迟和错误分类保持。
- 此改动消除无关模块的导入时耦合，实际运行对应流仍依赖该 Hub 的原异常类型；缺失模块的导入失败会在该流执行时暴露，不宣称运行依赖已消失。

验证：重连/致命错误及新导入隔离检查定向 **5 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2472 passed**，27.06 秒，一项现有 Starlette/httpx 警告。新增独立进程检查：禁止三种无关 Hub 后账户通道仍可加载。stream_recovery 定向 mypy --follow-imports=skip、核心文件与架构测试完整 Ruff、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第六十二批：风控运行消费健康上报能力

第六十一批提交为 `22dc3d0`；第六十二批继续本地实施，未部署生产。

- LiveRiskControlRuntime 已通过 dispatch、状态读取、上下文失效和 gate 刷新回调装配，不依赖具体 daemon；本批不新增重复处理接口。
- 其 telemetry 仅调用 consumer_health，改为复用已有 ConsumerHealthSink，删除完整 LiveTelemetrySink 的类型引用。原 recorder 与现有测试替身直接满足接口，无新增记录转发实现。
- 去除参数注解后 risk_control 全部函数 AST 与上一批一致。风控事件派发、耐久命令 claim/complete、消费者可用性与 gate 刷新顺序、恢复任务与重试规则保持。

验证：风控/架构定向 **68 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2473 passed**，26.91 秒，一项现有 Starlette/httpx 警告。新增 risk_control 独立进程禁止 sqlalchemy/persistence 导入检查。risk_control 与 telemetry_ports 两文件定向 mypy --follow-imports=skip、核心文件与架构测试完整 Ruff、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第六十三批：退出通道直接读取托管持仓能力

第六十二批提交为 `ccd4f77`；第六十三批继续本地实施，未部署生产。

- ExitChannelProcessor 已要求 managed_position_symbols；quote 和 grace 通道改为直接读取，删除 getattr/hasattr 与缺失能力默认分支。grace 每轮只读取一次该属性，不再先 hasattr 后再次取值。
- 保留真实 daemon 的动态集合读取，不在构造时冻结 symbols；托管持仓撤除后的 retry 清理、失败状态清除和原缓存选择行为保持。显式空集合沿用原缓存查询语义，本批未修改空集合所代表的读取行为。
- 旧 quote 替身补齐必需集合。接口不完整的调用者现在明确抛出 AttributeError；新增检查证明缺失能力时在 quote 缓存写入前拒绝，停止用静默默认值掩盖装配错误。无新增接口或转发实现。

验证：退出通道/CLI/架构定向 **138 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2474 passed**，27.60 秒，一项现有 Starlette/httpx 警告。exit_channels 与 exit_channel_ports 两文件定向 mypy --follow-imports=skip、核心文件与退出测试完整 Ruff、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第六十四批：共享退出失败规则脱离运行循环

第六十三批提交为 `541b8a8`；第六十四批继续本地实施，未部署生产。

- exit_failure_policy 独立拥有 pending position 失败判定、重试耗尽后的原因提升，以及原重试延迟和身份冲突原因常量。账户与退出通道直接引用这一所有者，账户通道不再为了共享规则导入 exit_channels 运行循环。
- 删除 exit_channels 的这些规则导出声明，原规则测试改为直接调用实际所有者。未增加转发函数或第二套策略；规则函数/常量及两个通道类 AST 与迁移前一致，重试与日志时序保持。
- 扩展账户导入隔离检查，同时禁止退出循环模块及无关 Hub；新增纯规则模块禁止 sqlalchemy/persistence 导入检查。

验证：账户/退出通道/架构定向 **77 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2475 passed**，27.49 秒，一项现有 Starlette/httpx 警告。exit_failure_policy、exit_channels 与 exit_channel_ports 三文件定向 mypy --follow-imports=skip、三核心文件与相关测试完整 Ruff、git diff --check 通过，不代表账户通道或全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第六十五批：成交量快照源消费能力

第六十四批提交为 `3094d33`；第六十五批继续本地实施，未部署生产。

- volume 定义 QuoteVolumeSnapshotSource，组合 AsyncIterable[QuoteVolume24hSnapshot] 与同步 stop()；WebSocketQuoteVolumeProvider 删除具体 WebSocketMarketQuoteVolumeSource 类型依赖，原源与现有测试替身直接满足接口，无新增转发适配器。
- 原成交量类去除参数注解后的 AST 完全一致。先 source.stop()、后取消并等待后台任务的顺序、因果快照选择、缓存指标与一秒重连保持。具体源创建与连接生命周期继续归运行装配。
- 新增独立进程检查，禁止导入 quote_hub 后 volume 消费模块仍可加载。原 REST ticker 类型依赖保留，本批不宣称所有行情适配依赖均移除。

验证：成交量/架构定向 **68 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2476 passed**，27.85 秒，一项现有 Starlette/httpx 警告。volume 定向 mypy --follow-imports=skip、核心文件与架构测试完整 Ruff、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第六十六批：入场预期发布器显式装配

第六十五批提交为 `1ab9efd`；第六十六批继续本地实施，未部署生产。

- LiveEntryExpectationRegistrar 仅接收必需 PositionExpectationPublisher 和 account_label，删除具体 WebSocket 发布器导入、URL 参数及内部默认创建。已有接口与原测试替身复用，无新增转发实现。
- 长驻运行编排与单次 plan_runner 两个生产入口显式创建原 WebSocketAccountPositionExpectationPublisher，保留 live 环境、账户标签、URL 空值检查和入场提交前注册。注册器保留账户标签校验、计划到预期转换、失败日志及 OrderPreSubmissionError 封锁。
- 构造接口不再接受缺省或 None publisher，也不再按发布器真假值选择备用对象；生产仍绑定原生发布器。源创建归装配，注册器不再知道连接地址。

验证：注册器/CLI/架构定向 **138 passed**；最终完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2478 passed**，一项现有 Starlette/httpx 警告。新增注册器禁止 sqlalchemy/persistence 与禁止具体账户 Hub 导入两项独立进程检查。entry_expectations 定向 mypy --follow-imports=skip、核心注册器及相关测试完整 Ruff、两装配文件 F/I、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第六十七批：订单事件记录与观察能力

第六十六批提交为 `a460130`；第六十七批继续本地实施，未部署生产。

- telemetry_ports 定义单方法 OrderEventSink，完整 LiveTelemetrySink 继承该能力，删除重复声明；实际 recorder 实现不变。
- order_event_runtime 定义 EntryOrderLifecycleObserver 与 EntryOrderEventObserver，仅要求原同步 observe 与 observe_entry_order_event。runtime 删除具体 daemon、entry_orders 与完整 telemetry 导入，现有生产对象/测试替身直接满足接口，无新增转发实现。
- 去除参数/属性注解后原 runtime 类 AST 完全一致。先尽力记录 telemetry，再 finally 内依次更新限价生命周期和 daemon 的顺序保持；观察者错误传播规则与可选装配时机不变。

验证：订单事件/架构定向 **69 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2479 passed**，28.56 秒，一项现有 Starlette/httpx 警告。新增 order_event_runtime 禁止 sqlalchemy/persistence 导入检查，并独立进程验证同时禁止三个具体协作者模块时仍可导入。order_event_runtime 与 telemetry_ports 两文件定向 mypy --follow-imports=skip、核心文件与架构测试完整 Ruff、telemetry F/I、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第六十八批：限价生命周期消费订单进度

第六十七批提交为 `9407f94`；第六十八批继续本地实施，未部署生产。

- entry_orders 定义只读 EntryOrderProgress，仅暴露 ExchangeOrderState 与 executed_quantity。track 和撤单回调返回值依赖该能力，删除具体 OrderExecutionResult/状态机导入；原状态机结果与持久化订单直接满足能力。
- restore 直接将持久化订单传给 track，删除仅为读取两个字段而构造完整执行结果的 _result_from_persisted。track 同步读取这两个属性后创建原计时任务，不保留进度对象，恢复判断与任务所有权保持。
- GTD 到期判断、reduce-only 排除、terminal/已全部成交排除、撤单日志与停止/观察者取消顺序保持。未新增撤单转发实现，实际撤单仍使用原回调。

验证：限价生命周期/架构定向 **72 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2480 passed**，28.67 秒，一项现有 Starlette/httpx 警告。新增 entry_orders 禁止 sqlalchemy/persistence 导入检查，并独立进程验证禁止具体 state_machine 时仍可导入。entry_orders 定向 mypy --follow-imports=skip、核心文件与架构测试完整 Ruff、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第六十九批：提交围栏使用必需订单标识

第六十八批提交为 `621390c`；第六十九批继续本地实施，未部署生产。

- 核对确认 LiveSubmissionFence 的耐久读取已依赖 LiveRiskStateReader，不新增重复仓储接口。
- validate 直接读取 OrderExecutionPlan.client_order_id 并按领域字符串契约检查空白，删除 getattr、缺失默认值和 str 转换。真实计划的标识已是必需字符串，正常路径保持；缺失字段现在明确抛出 AttributeError，None/非字符串也不再经静默跳过或隐式转换获准。
- 保留空白标识的 OrderPreSubmissionError、reduce-only 与入场分支、租约/epoch 检查、halt 读取及能力评估顺序。新增缺失身份检查证明在耐久读取前拒绝不完整计划，未新增数据库写入或提交路径。

验证：提交围栏定向 **8 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2481 passed**，28.89 秒，一项现有 Starlette/httpx 警告。submission_fence 定向 mypy --follow-imports=skip、核心文件与围栏测试完整 Ruff、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第七十批：行情准入记录能力

第六十九批提交为 `eac09ab`；第七十批继续本地实施，未部署生产。

- telemetry_ports 定义 MarketAdmissionSink，拥有原 context_ready 与 gate_evaluated 完整参数/default 契约；完整 LiveTelemetrySink 继承并删除重复声明，recorder 实现不变。
- LiveMarketStateAdmission 仅依赖这两项记录能力，删除完整 telemetry 类型导入。上下文 generation 校验、同步待入场计划、上下文记录、持仓发布、gate 判定及结果记录顺序保持，无新增转发实现。
- 保留 context_ready 可选 SourceIngress 参数，该模型仍在 telemetry 所有者，仅 TYPE_CHECKING 引用；本批不宣称所有类型引用已脱离 telemetry。上下文失效的既有动态探测未在本批迁移。

验证：完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2482 passed**，28.53 秒，一项现有 Starlette/httpx 警告。新增 market_admission 独立进程禁止 sqlalchemy/persistence 导入检查。telemetry_ports、market_admission 与 context 三文件定向 mypy --follow-imports=skip 通过；尝试同时纳入 context_prefetch 时发现原 AsyncGenerator 三类型参数错误，该文件未修改，仍属类型验收缺口。核心文件与架构测试完整 Ruff、telemetry F/I、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第七十一批：上下文预取类型验收闭合

第七十批提交为 `1c5e71d`；第七十一批继续本地实施，未部署生产。

- 修正 LiveContextPrefetcher.stream 的 AsyncGenerator 返回注解，从错误的三个参数改为 PrefetchedContext/None 两个参数。Python 异步生成器没有同步 Generator 的 return 类型参数。
- 除返回注解外，整个预取模块 AST 与上一批一致。generation 捕获、backfill 跳过上下文读取、容量二队列、状态顺序、错误随状态传递与所有任务取消/等待保持。无新增运行接口或测试镜像。
- 第七十批记录的 context_prefetch 类型验收缺口已关闭；不修改该批的历史验收记录。

验证：预取顺序/取消定向 **2 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2482 passed**，28.29 秒，一项现有 Starlette/httpx 警告。telemetry_ports、market_admission、context、context_prefetch 四文件一起定向 mypy --follow-imports=skip 通过，核心文件完整 Ruff、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第七十二批：行情准入缓存失效显式注入

第七十一批提交为 `17e26c2`；第七十二批继续本地实施，未部署生产。

- LiveMarketStateAdmission 接收可选 invalidate_context 回调，invalidate_context_cache 不再探测 provider 方法；未注入仍不执行失效操作。
- daemon 装配时选择 provider.invalidate_cache，若不可调用则尝试 invalidate，两者均不可调用则注入 None；保留原优先级和调用错误传播，未替换为 ContextRuntime.invalidate，因此不引入额外 generation 更新或异常吞并。
- 回调属于构造生命周期绑定，不再每次失效发现后续方法替换。准入 prepare、预取、持仓发布、gate 判定顺序保持；market_loop 对 admission 方法的既有探测仍保留，不宣称所有动态探测清零。

验证：准入显式有/无回调与预取定向 **4 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2484 passed**，29.67 秒，一项现有 Starlette/httpx 警告。新测试证明即使 provider 暴露两种失效方法，准入仍只调用显式回调。telemetry_ports、market_admission、context、context_prefetch 四文件一起定向 mypy --follow-imports=skip 通过，核心文件与新测试完整 Ruff、daemon F/I、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第七十三批：行情循环直接调用准入失效契约

第七十二批提交为 `38544d3`；第七十三批继续本地实施，未部署生产。

- LiveMarketLoop 已接收 LiveMarketStateAdmission，未托管持仓防抖路径直接调用 invalidate_context_cache()，删除 getattr/callable 与方法缺失时静默跳过分支。
- 准入自身仍拥有可选失效回调，未配置回调继续无操作；无需在循环重复判断能力。真实装配方法存在，原失效位置、错误传播、持仓防抖/过期判断和 halt 时序保持。不完整准入对象不再被默许。
- 无新增接口、包装器或实现镜像测试，复用现有未托管持仓与回调行为验收。

验证：未托管持仓/失效回调定向 **5 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2484 passed**，28.44 秒，一项现有 Starlette/httpx 警告。telemetry_ports、market_admission、context、context_prefetch 四文件联合定向 mypy --follow-imports=skip 通过，不包含 market_loop 类型验收。market_loop F/I、git diff --check 通过；完整 Ruff 发现该文件既有第 26 行长导入 E501，未改动该行，不宣称完整 Ruff 通过。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第七十四批：退出事件协调注解导入隔离

第七十三批提交为 `4afc464`；第七十四批继续本地实施，未部署生产。

- exit_event_coordinator 将具体退出处理器、执行通道、退出管理器、K 线事件及上下文类型导入移至 TYPE_CHECKING；这些类型仅用于注解，实际对象仍由原装配提供。运行方法逻辑未改，未新增转发接口。
- 首次隔离检查发现 context 经 ManagedLivePosition 间接加载 exits，进一步将协调模块自身的 context 注解导入也移至 TYPE_CHECKING。独立进程同时禁止 context、exits、exit_processor、exit_lane、closed_candle_feed 后协调模块可加载。实际运行仍依赖注入对象，不宣称能力接口全部收窄。
- 账户/quote/candle/grace 路由、上下文读取与失效、成交处理及错误发布顺序保持。

验证：退出协调/架构定向 **75 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2485 passed**，29.06 秒，一项现有 Starlette/httpx 警告。完整回归之后进一步调整 context 的纯注解导入，最终代码重新通过上述定向检查与五模块导入隔离检查。新增协调模块禁止 sqlalchemy/persistence 导入检查。核心文件与架构测试完整 Ruff、git diff --check 通过。协调模块与 context 的 mypy --follow-imports=skip 检查有四处协作者 outcome.failure 的 Any 返回错误，未忽略错误码；本批不宣称协调模块类型验收通过。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第七十五批：退出协调处理与通道能力契约

第七十四批提交为 `3aac002`；第七十五批继续本地实施，未部署生产。

- exit_event_ports 定义只读 ExitFailureResult.failure；ExitEventProcessor 声明协调实际调用的 state/quote/candle/grace 四方法，ExitEventLane 声明 start、submit_account 与 submit_quote，保持原参数和 wait 默认值。
- LiveExitEventCoordinator 改为消费这些接口，删除具体处理器及通道类型引用。原 ExitLaneOutcome 仍由通道拥有，实际处理器/通道与现有替身无需新增转发实现；协调只需要失败原因，不获得队列、任务或结算状态所有权。
- 去除参数注解后协调全部函数 AST 与上一批一致。事件路由、上下文发布、失效及结果失败原因返回顺序保持。

验证：退出协调/架构定向 **76 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2486 passed**，28.91 秒，一项现有 Starlette/httpx 警告。新增接口模块禁止 sqlalchemy/persistence 导入检查。exit_event_ports、exit_event_coordinator、context 三文件联合定向 mypy --follow-imports=skip 通过，关闭上一批协调模块四处 Any 返回缺口；此范围未对完整 exit_processor/exit_lane 实现作类型验收。核心文件与架构测试完整 Ruff、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第七十六批：退出协调仅消费启用判断

第七十五批提交为 `1c8ee09`；第七十六批继续本地实施，未部署生产。

- 核对协调模块从不调用 exit_manager，仅检查非 None。删除其构造参数、存储属性与类型引用，四种事件统一消费已有 exit_enabled 回调。
- daemon 装配的回调同时检查管理器存在和原 exit_control.enabled；保留短路判断顺序。管理器继续由 daemon/处理器拥有，不向协调暴露。管理器引用的读取现在属于启用回调；现有生命周期没有替换管理器的路径。
- 增加四种禁用事件测试，证明账户/quote/candle/grace 在退出不可用时不读取上下文、不启动执行通道、不失效缓存。原事件路由、结果处理及失效顺序保持，无新增转发实现。

验证：退出协调/架构定向 **80 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2490 passed**，29.01 秒，一项现有 Starlette/httpx 警告。exit_event_ports、exit_event_coordinator、context 三文件联合定向 mypy --follow-imports=skip 通过，核心协调文件与测试完整 Ruff、daemon F/I、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第七十七批：定时风控注解导入隔离

第七十六批提交为 `71d5b88`；第七十七批继续本地实施，未部署生产。

- scheduled_controller 的 OrderExecutionPort、LiveContextProvider 与 LiveDaemonRuntimeContext 仅 TYPE_CHECKING 引用，运行时不再为这些注解加载执行协调器和上下文模块。
- 保留 exits 的运行导入：控制器实际用 LiveExitCancellationRequest 做 isinstance 判断，本批不迁移请求模型，也不宣称退出管理能力已全部收窄。
- 所有函数 AST 与上一批完全一致，定时窗口、撤单、flatten 请求、交易所仓位验证与重新开放入场顺序保持。无新增转发模块或第二套执行规则。

验证：定时控制器/架构定向 **77 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2492 passed**，29.81 秒，一项现有 Starlette/httpx 警告。新增定时控制器禁止 sqlalchemy/persistence 导入检查，并新增独立进程同时禁止执行协调器和 context 的导入检查。核心文件与架构测试完整 Ruff、git diff --check 通过，本批未作定时控制器或全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第七十八批：定时风控只消费撤单能力

第七十七批提交为 `780ae31`；第七十八批继续本地实施，未部署生产。

- scheduled_controller 定义 ScheduledOrderCanceller，仅声明原异步 cancel_order(plan)；其结果使用只读 ScheduledCancellationResult.state。控制器删除完整 OrderExecutionPort 类型引用，不再要求提交、恢复或其他观察能力。
- 原协调器结果与现有测试替身直接满足所需能力，无新增转发实现。读取 state 的 terminal/value 与 REJECTED 判定保持原行为，未重复定义订单状态枚举。
- 去除参数注解后整个控制器类 AST 与上一批一致，两处撤单执行、异常处理、未确认封锁、计数与窗口状态推进顺序保持。

验证：定时控制器/架构定向 **77 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2492 passed**，29.50 秒，一项现有 Starlette/httpx 警告。核心文件完整 Ruff、git diff --check 通过，沿用上一批导入隔离检查；本批未作定时控制器或全仓类型验收，不将行为回归等同于实现的结构类型检查。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第七十九批：定时风控定向类型验收闭合

第七十八批提交为 `697bd7a`；第七十九批补齐本地验收，未新增运行代码改动，未部署生产。

- 定时控制器与 context 两文件检查最初在 _scheduled_reference_price 报一处 Any 返回：--follow-imports=skip 跳过了实际 MarketState15s 所有者，不能据此判定参考价格实现错误。
- 纳入 domain/market/models.py 后，scheduled_controller、context、领域行情模型三个文件联合定向 mypy --follow-imports=skip 通过，无新增类型断言、cast 或忽略错误码。关闭第七十七/七十八批未做控制器类型验收的范围缺口。
- 此范围没有纳入完整执行协调器/退出管理器实现，不能作为其结构类型兼容或全仓类型验收证明。历史批次验收口径保留。

验证：定时控制器/架构定向 **77 passed**，核心文件与架构测试完整 Ruff、git diff --check 通过。本批只更新验收记录，不重复完整回归；最近一次完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归为第七十八批 **2492 passed**。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第八十批：定时风控消费平仓规划能力

第七十九批提交为 `ac34076`；第八十批继续本地实施，未部署生产。

- ScheduledFlattenPlanner 明确单方法 requests_for_scheduled_flatten，保留 positions、now、symbol、reference_prices、attempt 参数和默认值。控制器删除 LiveExitManager 类型引用，原管理器与现有替身直接满足所需能力，无新增转发层。
- 请求模型和 ManagedLivePosition 仍由 exits 所有，运行时撤单请求 isinstance 判断保留，本批不宣称整个 exits 模块依赖已移除。
- 去除参数注解后控制器类 AST 与上一批一致。规划请求、撤单分类、平仓重试、权威仓位验证和入场恢复顺序保持。wait_for_entry_submissions_idle 的既有动态探测仍是待收窄点。

验证：定时控制器/架构定向 **77 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2492 passed**，29.43 秒，一项现有 Starlette/httpx 警告。scheduled_controller、context、领域行情模型三文件联合定向 mypy --follow-imports=skip、核心文件完整 Ruff、git diff --check 通过，不代表完整管理器实现或全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第八十一批：定时撤单前等待能力显式注入

第八十批提交为 `256c32a`；第八十一批继续本地实施，未部署生产。

- ScheduledRiskWindowController 接收可选 wait_for_entry_submissions_idle 异步回调，撤单前不再 getattr/callable 探测执行对象。缺省 None 沿用不等待行为。
- daemon 构造时绑定现有可调用等待方法，不支持该能力的旧执行替身注入 None；兼容选择仍留在装配，不宣称全仓探测清零。等待绑定属于构造生命周期，不再逐次发现方法替换。
- 等待成功后才读取待入场计划并撤单；等待异常仍返回原 scheduled_entry_submission_drain_failed 原因，CancelledError 原样传播。未新增等待实现或第二条撤单路径。

验证：控制器定向 **6 passed**，包含新增未配置/成功/失败三项显式等待行为；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2495 passed**，28.75 秒，一项现有 Starlette/httpx 警告。scheduled_controller、context、领域行情模型三文件联合定向 mypy --follow-imports=skip、核心文件与控制器测试完整 Ruff、daemon F/I、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第八十二批：编排消费函数接收异步事件流

第八十一批提交为 `e2607b1`；第八十二批继续本地实施，未部署生产。

- 核对 StartupMarketStateBuffer 已仅依赖领域状态，不新增缓冲接口。编排的启动行情收集、风控事件消费、账户事件消费函数分别接收 AsyncIterable[MarketState15s/RiskControlEvent/AccountEvent]，删除三处具体 WebSocket 源参数限制。
- 原源与已有异步替身直接满足迭代能力，无新增事件流转发。具体源的创建、连接和生命周期仍由编排拥有，其实现导入继续保留，不宣称编排模块已隔离原生适配器。
- 去除参数注解后编排全部函数 AST 与上一批一致，启动 buffer.close 的错误传播、重连、账户快照及退出处理顺序保持。

验证：编排/启动缓冲定向 **73 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2495 passed**，29.07 秒，一项现有 Starlette/httpx 警告。编排 F/I、git diff --check 通过，本批未作编排或全仓类型验收。复用现有行为检查，无新增镜像实现测试。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第八十三批：账户编排入口复用消费能力

第八十二批提交为 `26a1f11`；第八十三批继续本地实施，未部署生产。

- _run_account_event_channel 参数改为 AccountEventExitProcessor、AccountEventOrderReconciler 与 AccountFillSink，对齐账户 runtime 的实际消费能力，删除该入口完整 daemon/具体对账对象/完整 telemetry 类型限制。
- 保留缺省对账对象时由订单读取仓储、执行端口和 run_id 创建原 LiveOrderReconciliation 的分支；该入口仍有实际装配职责，不移除必要原生实现导入。编排不再引用完整 LiveTelemetrySink 类型。
- 去除参数注解后编排全部函数 AST 与上一批一致，账户事件流恢复、成交记录、先对账后发布快照、退出重试及回调顺序保持，无新增转发实现。

验证：编排/账户通道定向 **76 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2495 passed**，29.29 秒，一项现有 Starlette/httpx 警告。account_event_ports 与 telemetry_ports 两文件定向 mypy --follow-imports=skip、编排 F/I、git diff --check 通过，不代表编排或全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第八十四批：grace 编排入口显式退出契约

第八十三批提交为 `999ecc1`；第八十四批继续本地实施，未部署生产。

- 历史 _run_grace_timeout_channel 入口的 daemon 参数改为 ExitChannelProcessor，对齐已有退出运行接口。
- 增加可选 on_order_identity_conflict 回调，删除入口对 daemon 通知方法的 getattr 发现；未传入继续跳过通知。此历史入口只有测试调用，生产直接装配退出 runtime 并显式绑定原通知，不变更生产通知。
- 原 grace 循环、失败上报、瞬时异常分类、重试与间隔保持。旧入口调用者若希望通知，需显式传入回调，不再靠同名方法自动发现。

验证：编排/退出通道定向 **75 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2495 passed**。首次检查因遗漏接口导入在收集阶段失败，补齐导入后重新通过；未保留错误码忽略。编排 F/I、git diff --check 通过，不代表编排或全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第八十五批：grace 入口通知迁移验收

第八十四批提交为 `ba2a3f9`；第八十五批补齐本地测试覆盖，未修改运行代码，未部署生产。

- 将历史 grace 身份冲突测试扩展为显式有/无通知两种配置。daemon 替身暴露同名通知方法但调用即失败，证明入口不再自动发现通知。
- 配置回调时验证身份通知先于失败上报；未配置只发布原失败原因。保留单轮后 CancelledError 停止循环的验收，验证原降级结果不变。

验证：grace 入口定向 **2 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2496 passed**。测试 F/I、git diff --check 通过，本批不新增类型验收或运行结构迁移。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第八十六批：上下文模型注解导入隔离

第八十五批提交为 `4a55669`；第八十六批继续本地实施，未部署生产。

- context 的 ManagedLivePosition 与 AccountSnapshot 仅 TYPE_CHECKING 引用，不再为了两个字段注解急切加载退出管理和账户同步实现。字段默认值、运行上下文模型和失效逻辑未改。
- 独立进程同时禁止 exits 与 execution_account.sync 后 context 可加载；新增 context 禁止 sqlalchemy/persistence 导入检查。
- LiveContextRuntime 对旧 provider 的 currentness/失效兼容探测仍保留，本批不收窄旧接口或改变 generation、异常日志规则。

验证：上下文/架构定向 **80 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2497 passed**，29.41 秒，一项现有 Starlette/httpx 警告。context、context_prefetch、market_admission、telemetry_ports 四文件联合定向 mypy --follow-imports=skip、核心文件与架构测试完整 Ruff、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第八十七批：上下文失效方法构造时绑定

第八十六批提交为 `3ed947c`；第八十七批继续本地实施，未部署生产。

- LiveContextRuntime 构造时绑定 invalidate 或备用 invalidate_cache，优先级不变；invalidate 不再逐次 hasattr 探测 reader 方法。
- 保留先递增 generation、主方法传 event、旧 cache 方法 TypeError 后无参重试，以及调用失败日志。未提供方法时仍无操作；构造时绑定后不再发现后续方法替换。属性描述器异常会在构造时暴露；显式 None 方法视为未配置，不再在失效时记录调用 None 的错误。
- currentness 的旧 provider 探测保留，不宣称所有兼容探测已移除。未新增转发模块，缓存失效实现仍归 provider。

验证：上下文定向 **5 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2497 passed**，30.01 秒，一项现有 Starlette/httpx 警告。context 定向 mypy --follow-imports=skip、核心文件完整 Ruff、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第八十八批：上下文失效绑定行为验收

第八十七批提交为 `ab8b08b`；第八十八批补齐本地测试覆盖，未修改运行代码，未部署生产。

- 新增 primary/legacy_event/legacy_noarg 三种失效绑定测试。两种方法同时存在时验证 invalidate 优先；legacy cache 支持原 event 调用与 TypeError 后无参调用。
- 在构造后替换两种 provider 方法，验证运行模块继续使用构造时绑定的原方法；每次失效 generation 递增一。测试覆盖实际兼容行为，不新增转发实现。

验证：上下文定向 **8 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2500 passed**，29.48 秒，一项现有 Starlette/httpx 警告。上下文测试完整 Ruff、git diff --check 通过，本批不新增类型验收或运行结构迁移。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第八十九批：上下文新鲜度检查构造时绑定

第八十八批提交为 `791e6f2`；第八十九批继续本地实施，未部署生产。

- 构造时优先绑定 is_current，其次 is_context_current；运行检查不再逐次 hasattr 探测。保留 bool 结果转换、无方法时原 60 秒规则、调用异常日志与 False 返回。
- 显式存在但不可调用的方法仍在检查时触发原失败拒绝，不因 None 自动走默认时间规则。属性描述器错误会在构造阶段暴露；构造后替换同名方法不改变绑定。
- 新增两项新/旧方法绑定测试，验证主方法优先与原绑定稳定性。不新增上下文读取或缓存实现。

验证：上下文定向 **10 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2502 passed**。context 定向 mypy --follow-imports=skip、核心文件与上下文测试完整 Ruff、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第九十批：清理上下文运行的无用对象依赖

第八十九批提交为 `80a049e`；第九十批继续本地实施，未部署生产。

- 能力绑定后 LiveContextRuntime 不再读取 _context_reader/_context_provider，删除两项重复保存属性。仍保留构造兼容输入和已绑定方法，不新增转发实现。
- daemon 通过已有 context_provider 参数传入 callable provider，删除把该对象塞入严格 reader 参数时的 arg-type 忽略；resolved 选择结果不变。
- 上下文 generation、新鲜度、失效优先级、托管符号发布及 provider 生命周期行为保持。不宣称 daemon 全仓类型验收完成。

验证：上下文/daemon 定向 **71 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2502 passed**。context、context_prefetch、market_admission、telemetry_ports 四文件联合定向 mypy --follow-imports=skip、context 完整 Ruff、daemon F/I、git diff --check 通过。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第九十一批：水位与就绪度消费缓存读取回调

第九十批提交为 `2c9df05`；第九十一批继续本地实施，未部署生产。

- daemon 接收可选 cached_context_provider 同步读取回调，latest_watermark 和 evaluate_readiness 两处只调用该能力，不再直接探测上下文 provider 的缓存属性。
- 生产运行编排显式绑定 PostgresLiveContextProvider.cached_context 公共属性，每次调用读取最新缓存，不在构造时冻结快照。旧调用者缺省回调仍通过构造处兼容闭包按公共缓存/私有缓存回退，私有读取未全仓清零。
- 水位候选取最小、未托管/未解决订单计数和 readiness 计算规则保持，原缓存所有权与生命周期不变，无新增数据库读取或记录转发。

验证：水位/就绪度定向 **2 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2502 passed**，29.23 秒，一项现有 Starlette/httpx 警告。daemon/编排 F/I、git diff --check 通过，本批未作 daemon/编排或全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第九十二批：显式缓存读取动态快照验收

第九十一批提交为 `2ff61c7`；第九十二批补齐本地测试覆盖，未修改生产代码，未部署生产。

- daemon 测试装配 helper 支持显式 cached_context_provider。新增回调动态快照测试：provider 公共/私有缓存属性一旦读取即失败，证明显式回调覆盖兼容读取。
- 先返回滞后账户快照，验证最小水位与 PROGRESS_LAGGING；随后更新同一回调返回的快照，验证水位更新及滞后判定解除，没有在构造时冻结缓存。

验证：水位/就绪度定向 **3 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2503 passed**。daemon 测试 F/I、git diff --check 通过，本批不新增类型验收或运行结构迁移。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 第九十三批：启动预热使用必需桶时间契约

第九十二批提交为 `2cb7ac4`；第九十三批继续本地实施，未部署生产。

- validate_live_warmup_coverage 直接使用 MarketState15s.bucket_start 排序，删除缺失字段时用 cutover_at 代替的回退；始终校验窗口相邻间隔及末桶截止时间，删除字段存在探测。
- 完整领域状态的排序、缺桶判定及错误文本不变；不完整的旧状态替身不再靠缺字段跳过连续性校验，而明确失败。按策略 required_fields 动态检查可选行情字段的逻辑保留。
- 预热读取、分页、清空/重放及启动顺序未变，不新增数据库或状态转发实现。

验证：启动恢复定向 **8 passed**；完整单元、部署 smoke（开启 hub 网络测试）及两项 fake 端到端回归 **2503 passed**，29.89 秒，一项现有 Starlette/httpx 警告。startup_recovery、market_runtime_contracts、领域策略模型、行情模型及行情读取接口五文件联合定向 mypy --follow-imports=skip、核心文件完整 Ruff、git diff --check 通过，不代表全仓类型验收。

本批无 schema 变更、生产发布或服务器采样。真实数据库通知、并发、原子回滚与完整进程重启仍待补齐。

## 后续实施顺序

1. 补齐真实 Postgres 并发、原子回滚与完整进程重启验收；第二批已完成自愈事务迁移及提交后重载契约。
2. ExecutionBook 内部协作者：第三批已提取恢复/检查点计算；第四批已提取命令状态与 reservation 计算；第五批已提取命令/outbox 编解码与恢复校验；第六批已明确命令仓储接口并提取显式兼容适配；第七批已提取 reservation 仓储接口与显式兼容装配；第八批已提取证据模型、去重/覆盖规则与累计水位计算；第十六批已提取成交 identity/恢复前缀计划、退出结算判定及真实账户成交水位增量；第十七批已提取订单证据的 outbox/释放/恢复判定；第十八批已提取累计报告结算与水位持久化计划；第十九批已提取候选证据分组协调及观察结果模型；第二十批已提取耐久证据准入与水位写入计划；锁、核心事务和状态所有权继续由 Book 统一管理，不为缩短类而拆开事务。统一 mutation lock、候选状态、事务与提交后发布仍由 Book 所有。
3. 聚合仓储与运行装配：第九批已拆出执行命令/恢复/水位/对账仓储；第十一批已拆出意图占用/原子提交仓储，第十二批已独立事件/成交仓储与状态机事件端口，第十三批已独立订单读取仓储及领域读取接口，第十四批已独立外部订单接管仓储，第十五批已分离计划与 shadow suppression 接口，继续处理运行用例的装配，第十批已提取实盘执行 runtime factory，第二十三批已独立 Hub 游标恢复与整批确认状态；第二十四批已分离 daemon 提交与 checkpoint 接口并删除聚合转发适配器；第二十五批已独立行情运行契约并清理 daemon/live_rollout 门面及生命周期注解依赖，第二十六批已独立 startup recovery 的三项行情读取接口与领域分页游标，第二十七批已独立研究采集补回的两项分页接口并清理采集包急切导入，第二十八批已将策略的同步读取/通知具体适配移至 Postgres 所有者，第二十九批已明确可选唤醒与 Universe 读取接口，第三十批已独立资源关闭能力接口，第三十一批已分离 entry runtime 的快照读取与 Postgres 装配，第三十二批已独立人工缺失订单确认的纯安全规则并删除 CLI 转发，继续核对整体依赖及剩余仓储消费点。
4. 第二十一批已解除恢复模型/codec 的摘要循环，保留既有 checkpoint digest 与跨 epoch 保护；第二十二批已清理 execution 包门面的急切导入并迁移活跃调用点，继续核查其他模块级延迟环。

生产账户事实覆盖冲突和策略状态发布缺失属于尚未关闭的运行问题；本次结构迁移不能作为这些问题已经修复的证据。
