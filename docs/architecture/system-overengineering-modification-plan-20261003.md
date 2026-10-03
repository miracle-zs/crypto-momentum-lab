# 交易执行安全与架构简化修改计划

- 状态：待实施。本文是修改计划，未据此修改运行代码或执行部署。
- 日期：2026-10-03（Asia/Shanghai）。
- 代码基线：`040b4eaacf932901d53642669019cd1ed63506b0`；编写时 `src/`、`tests/` 无未提交修改。
- 评审依据：[架构审视与设计边界诊断](system-overengineering-architecture-review-20261003.md)及本次两轮源码复核。
- 顺序：先修正文档证据，再收紧发单前检查位置和验证敞口交接，最后做类型导入与装配简化。

## 1. 目标与必须保留的契约

目标是让发单检查覆盖已知的等待窗口，让订单终态、敞口声明与账户基线之间的交接可验证，并降低装配代码的维护成本。

以下契约贯穿各批修改：

1. 命令身份、`client_order_id`、批次归属、真实成交证据与投影版本继续由现有所有者维护。
2. 按 `account_label + symbol + position_side` 串行调度命令；账户级敞口仲裁必须覆盖不同 symbol 和 LONG/SHORT Worker 的并发。明确核对现有风险锁与 claims 的 strategy scope，不借拆分队列改变风控范围。
3. 数据库事务中完成事实提交、预留与仲裁；交易所和账户 Hub 网络请求保持在事务外。
4. `reduce_only` 继续豁免入场开关和围栏的 active-halt 检查，同时保留租约、身份、批次一致性和既有退出能力检查。
5. 网络请求结果未知时进入既有对账/恢复路径，不自动重发订单 POST，不凭超时或空仓释放未知命令。
6. 复用现有 Book、UoW、恢复任务和有界遥测队列。接口是否改善，以调用方减少了多少必须了解的顺序和规则判断。

本计划不以文件数量或行数作为验收目标。`TradeCommandExecutor` 纯函数化列为可选项；不安排合并 Book、协调器与状态机，或迁移整个 `domain/runtime`。

## 2. 分批顺序

| 批次 | 修改内容 | 主要产物 | 进入下一批的条件 |
| --- | --- | --- | --- |
| A | 修正评审文档与验证记录 | 一致的正文、图、表和复现命令 | 当前事实与待验证风险区分清楚 |
| B | 将最终围栏落实到订单 POST 边界 | 最小提交接口调整与交错回归 | Hub 注册、冷配置、限速等待后重新检查；无重复 POST |
| C | 验证敞口交接与终态恢复，修复已复现缺口 | 交错矩阵、真实 PostgreSQL 回归、必要的最小修复 | 无漏算；成交/撤销/重放/重启的恢复证据完整 |
| D | 清理纯类型运行期导入 | `paper_models.py` 的小改动 | 运行期子包图无环；类型和报告消费者通过 |
| E | 分步提取装配职责 | 复用现有装配模块的局部提取 | 启动失败、资源所有权与停机顺序保持正确 |

B 与 C 的复现可以分别开展，但实现提交独立，避免混入类型和装配调整。每批记录实际检查命令、结果和未覆盖范围。未复现的问题保留为风险或待验证项，不直接写成已确认生产缺陷。

## 3. 批次 A：修正文档和复现记录

修改对象：[评审文档](system-overengineering-architecture-review-20261003.md)。

- 统一摘要、时序图和成本表中的连接池表述：围栏共享 `heartbeat_engine`，与心跳及控制面查询竞争单连接。
- 删除“极小间隙”“拦截绝大多数异常”等无测量依据的保证。保留“发单前读取控制状态，缩小已知等待造成的风险窗口”。
- 补齐实际调用链：围栏检查 → 账户 Hub 预期持仓注册 → 请求边界通知 → `submit_order` → 必要的保证金/杠杆确认 → 命令限速等待 → 订单 POST。这是修改前顺序，不能画成最终目标已实现。
- 将“Symbol 串行队列”改为完整调度 key；将暂时性门禁行为写为“继续计算和记录信号/候选，跳过订单计划及提交流程”。
- 更正名称：`OrderFlowImpulseRuntimeStrategy`、`build_live_execution_runtime`。
- 将“6 环归零”统一限定为排除 `TYPE_CHECKING` 分支的运行期子包聚合图；另列类型依赖图，不将其写成运行期导入故障。
- 给既有 85 个单测记录补充基线和命令，明确这些测试未证明敞口交接、生产崩溃恢复或最终 POST 边界无窗口。

本次额外核实的代码入口：

| 事实 | 代码入口 |
| --- | --- |
| 状态机先调用围栏，再调用入场注册 | `execution_account/orders/state_machine.py::_exchange_call` |
| 注册等待 WebSocket 连接、发送与 Hub 确认 | `live_rollout/entry_expectations.py`、`execution_account/hub.py::WebSocketAccountPositionExpectationPublisher.register` |
| 注册的连接和确认等待分别使用默认 10 秒超时 | `execution_account/hub.py::AccountEventHubConfig` |
| 冷配置时可能调用保证金配置查询/修改及杠杆修改 | `execution_account/binance/client.py::submit_order`、`_ensure_entry_margin_type`、`_ensure_entry_leverage` |
| 真正 POST 之前还等待命令限速器 | `execution_account/binance/client.py::_signed_post` |

验收：源码名称可检索；正文与两个图、成本表没有互相矛盾；所有“已验证”表述有命令或代码入口支持。

## 4. 批次 B：收紧最终订单 POST 检查位置

### 4.1 先复现窗口

扩展现有 `tests/unit/execution_account/orders/test_state_machine.py`、`tests/unit/live_rollout/test_submission_fence.py`、`tests/unit/execution_account/test_binance_client.py` 和 `tests/unit/live_rollout/test_entry_expectations.py`。

用 `asyncio.Event` 或可控传输明确暂停边界，避免依赖短暂 sleep 制造竞争。每个场景都断言实际 `/fapi/v1/order` 调用次数和最终命令状态。

| 暂停位置 | 等待期间的变化 | 应验证的行为 |
| --- | --- | --- |
| Hub 注册等待确认 | 租约撤销/替换或新增 halt | 注册返回后拒绝入场订单 POST |
| Hub 注册超时或失败 | 无额外变化 | 未发订单，走提交前失败收口 |
| 保证金或杠杆冷配置 | 租约过期、halt 或入场关闭 | 配置完成后重新检查，拒绝订单 POST |
| 命令限速等待 | 租约/上下文/能力事实变更 | 获得限速许可后重新检查；使用当前时间 |
| 同标的 LONG/SHORT 或不同标的并发 | 一个任务等待，另一个推进控制状态 | 不能依赖单个 Worker 队列获得账户级安全保证 |
| reduce-only 提交 | 入场关闭或 active halt，退出其他条件有效 | 保持退出策略，不借修复添加入场限制 |
| HTTP 已发出但回执丢失 | 随后出现 halt 或撤销 | 保留 UNKNOWN 与恢复，不重新 POST |

### 4.2 推荐修改

将可能发生网络等待的准备工作放在最终控制检查之前。推荐目标顺序为：

```text
Coordinator 准入 / Book 接受 / 数据库提交准备
  → 账户 Hub 预期持仓注册（入场）
  → 交易客户端必要的保证金、杠杆确认（入场）
  → 订单请求限速许可
  → 最终 LiveSubmissionFence 检查
  → 参数签名与订单 HTTP POST
```

最终围栏通过窄的提交前 hook 传递到现有订单传输路径，在 `/fapi/v1/order` 获得限速许可后、实际 HTTP 调用前执行。交易客户端只调用 hook，不导入 `LiveSubmissionFence`、Live 上下文或数据库仓储。hook 绑定当前订单计划；保证金配置、杠杆配置及取消/查询继续遵循各自既有政策。

优先移动既有最终围栏的位置，不无条件再加一组重复数据库查询。上游快速准入和数据库带锁仲裁继续保留。hook 在实现上应有一个明确调用位置，并保证每次实际订单 POST 都经过它。

具体实现还需处理：

- 时间：最终检查使用完成准备和限速等待后的当前时间；数据库读取耗时也需纳入租约有效性判断，不复用候选生成时的 `context.now`。
- 回调：请求起始遥测复用现有有界入队能力；最终检查后不再安排 Hub、数据库或其他业务网络等待。被围栏拒绝的计划不得产生虚假的“交易所请求已发出”记录。
- 失败：hook 拒绝属于确定的订单 POST 前失败；Book/outbox/预留正确收口。Hub 已注册但随后拒绝时，确认现有预期持仓提示的 TTL/清理语义，不将提示当作成交或持仓事实。
- 取消：在 HTTP 前和 HTTP 发出后的取消分别验证，不仅凭异常类型推断交易所没有收到请求。
- 连接池：保留现有 heartbeat 共享池配置；是否需要隔离或增容由 checkout、心跳延迟和查询时长观测决定。

验收：上述交错场景可重复通过；正常计划最多一次订单 POST；入场与 reduce-only 的既有能力规则保持；遥测阶段和失败类别与真实请求状态一致。

限制：数据库检查与外部交易所接单仍非同一事务。HTTP 连接池、网络和交易所处理仍会产生时间差；本批关闭可定位的准备/限速窗口，不承诺跨系统原子撤销。

## 5. 批次 C：敞口交接和终态恢复

### 5.1 先建立完整事实顺序

核对 `submission.py` 的风险基线来源、`postgres_runtime.py` 的账户版本、`order_submission_repository.py` 的锁和 claims 查询，以及 `order_event_repository.py` 的终态释放。

基线金额、持仓 symbols、账户观察版本及 claims 需处于可解释的事实范围。账本的单仓位 `projection_version` 不能直接代替账户级敞口版本；本地时间戳和数量恰好相等也不能证明某笔成交已纳入账户基线。

需要成立的交接不变式是：在准备新的入场命令时，已产生但尚未进入被采用账户基线的成交敞口，不因终态 claim 释放而从风险计算中消失；基线追平后，同一成交不应长期重复占用额度。日损与按当前市场价格计算的总敞口继续遵循明确的快照新鲜度政策。

现有 `test_live_entry_exposure_claim_is_atomic_and_released_on_terminal` 验证的是零成交撤单后释放额度，不能替代 FILLED 与账户基线延迟更新的交接验证。

### 5.2 在真实 PostgreSQL 中验证交错

扩展 `tests/integration/persistence/test_order_repository.py`，并复用 `test_authority_book_transactions.py`、`test_exit_receipt_recovery.py` 的恢复夹具。

| 场景 | 验收要求 |
| --- | --- |
| A 入场已 FILLED，claim 已处理，账户基线仍旧；B 尝试入场 | 根据可证明的现有敞口阻止超限，或拒绝使用旧基线 |
| A 的账户基线先更新，终态/claim 处理后到 | 追平后额度计算正确，不长期重复计算 A |
| 部分成交后撤单/过期 | 已成交部分进入交接，未成交部分释放；不能按零成交撤单处理 |
| 零成交撤单/拒绝 | 可释放未执行额度，保留既有行为 |
| 不同标的和 LONG/SHORT Worker 使用旧基线并发准备 | 真实连接与事务验证仲裁范围，不能只用 Python mock 锁证明 |
| 重复、乱序终态及迟到 ACK | 不回滚订单终态、不重复结算、不重新激活已核销声明 |

先使用测试环境造数和可控提交屏障复现。记录是否确实漏算、是否被现有上下文 fencing 拦截，以及各层实际依赖的版本；不以仓储局部绕过上层的结果直接宣称完整实盘链路有漏洞。

### 5.3 只修复被证明的缺口

若完整链路证明存在漏算，首选在现有 claims/账户基线交接机制中保留“已成交但尚未被该基线覆盖”的风险占用，直到取得可验证的覆盖证据后再完成转移。复用现有账户 journal/coverage/版本，不用时间延迟推断成交已被吸收。

若基线无法证明覆盖关系，应拒绝新的入场并请求已有恢复任务，而不是把 claims 直接清空。禁止以“多保留所有 claims”作为最终方案：它会导致长期重复计算、额度无法恢复，并可能掩盖交接契约缺失。

如果需要持久字段，单独给出字段语义、现有行的保守处理方式及迁移回滚条件；未证明必需之前不预设新表、全局锁或第二套敞口账本。

### 5.4 覆盖提交与恢复之间的故障点

按实际顺序注入失败：订单/claim 事务提交后 → A/B 本地观察者之后 → Book 结算之前或提交之后 → 账户上下文发布之前。

恢复需要证明：同一订单和真实成交只结算一次；Book 预留、claims、退出 episode 状态能收敛；恢复收据直接进入 Book 时不依赖进程已丢失的本地计时器；新进程不会重复发单。精确区分真实进程退出重启测试与单进程异常注入，不将后者描述成前者。

本批继承[事实一致性计划](entry-fact-consistency-plan-20261002.md)的 coverage、pending 和批次归属规则，不恢复已淘汰的时间窗判冲突、合成批次或行情循环全量对账。

验收：交错矩阵和数据库测试记录齐全；修复前后的行为差异可复现；旧基线不能漏算风险；证据追平后额度可恢复；没有未知订单被错误终结。

## 6. 批次 D：清理类型导入

修改 `domain/strategy/paper_models.py`：增加延迟注解，使用 `TYPE_CHECKING` 包装 `RuntimePlan` 导入。保留 `PaperTradingRunReport.runtime_plan` 字段和序列化语义。

验证独立进程导入、报告构造及现有持久化消费者；检索是否存在运行期 `get_type_hints`/注解反射消费者，如存在则明确处理类型解析，不直接隐藏导入。

用可复现扫描分别输出运行期子包图和包含类型导入的图：前者 6 环归零，后者仍有类型依赖。扫描器必须明确处理 `TYPE_CHECKING`；不写仅匹配某个 AST 结构的脆弱测试。

验收：报告消费者与定向类型检查通过；既有字段和数据格式保持；没有扩大为 RuntimePlan 或 CapabilityEvaluator 的整体迁移。

## 7. 批次 E：逐步提取装配职责

先复用已有 `execution_runtime.py`、`runtime_options.py`、`startup_recovery.py`、`runtime_supervisor.py`、`resource_lifecycle.py` 等模块。原评审列出的三个 assembler 文件名作为候选，不要求全部新建。

建议三个独立小批次：

1. 提取数据库引擎、session 和仓储装配，保持 execution/market/observability/checkpoint/heartbeat 的实际池配置及用途。
2. 提取运行计划、能力证据提供和执行运行时接线，明确依赖和共享 Book/Coordinator 实例。
3. 提取行情与后台通道接线，保留既有启动恢复、就绪判定及监督关系。

每个提取模块返回调用方实际需要的资源或运行对象，不构建通用 DI 容器、跨层大 context 或工厂继承树。新接口应隐藏装配顺序，避免把大量闭包变量改成同样数量的公开参数。

验收重点：初始化中途失败时已创建资源被清理；正常装配后资源所有权正确转移；关闭时先停生产者/执行任务，再关客户端和引擎；取消、shutdown 超时、启动恢复失败继续走原收口。优先扩展现有 lifecycle/startup/app 入口测试，不为文件移动或逐字段转发增加测试。

## 8. 验证命令与完成记录

以下是实施时的验证入口。第一组曾在两轮评审前的代码基线上获得 `85 passed`；本文没有重新运行，也不把其结果扩展为 B/C 已验收。

```sh
rtk proxy .venv/bin/python -m pytest -q \
  tests/unit/live_rollout/test_submission_fence.py \
  tests/unit/live_rollout/test_submission.py \
  tests/unit/live_rollout/test_order_event_runtime.py \
  tests/unit/execution_account/orders/test_coordinator.py \
  tests/unit/execution/test_trade_command_executor.py \
  tests/unit/runtime/test_capability_evaluator.py
```

批次 B 增量验证：

```sh
rtk proxy .venv/bin/python -m pytest -q \
  tests/unit/execution_account/orders/test_state_machine.py \
  tests/unit/execution_account/test_binance_client.py \
  tests/unit/live_rollout/test_entry_expectations.py

rtk proxy env CML_RUN_HUB_NETWORK_TESTS=1 .venv/bin/python -m pytest -q \
  tests/unit/execution_account/test_hub.py
```

批次 C 使用现有测试库隔离配置（`CML_TEST_DATABASE_URL`，由测试夹具核对测试库身份），启动对应测试服务并完成所需迁移后运行：

```sh
rtk proxy .venv/bin/python -m pytest -q \
  tests/integration/persistence/test_order_repository.py \
  tests/integration/persistence/test_authority_book_transactions.py \
  tests/integration/persistence/test_exit_receipt_recovery.py
```

每批对实际修改文件运行 Ruff 和适当范围的 mypy；检查失败时记录新增问题和基线问题。相关检查通过后，只有新改动或未解决疑点才扩大或重复测试。

| 批次 | 状态 | 实施 commit / 测试命令 / 结果 / 未覆盖范围 |
| --- | --- | --- |
| A 文档修订 | 已完成 ✅ | 对齐评审文档与修改计划事实（完整调度 key、`OrderFlowImpulseRuntimeStrategy`、`build_live_execution_runtime`、阶段6修改前物理调用链、收紧已知等待风险窗口表述）；验证 85 个基线单测通过（`85 passed in 0.84s`）；明确单测未覆盖生产交接/恢复与最终 POST 边界 |
| B 最终 POST 围栏 | 已完成 ✅ | 将最终围栏收紧至限速等待之后、实际 HTTP POST 之前；Hub 预期持仓注册置于发单准备前；若围栏在限速后拒绝发单，不发起网络请求且不发射虚假 request_started 遥测；全量回归：3043 单元测试通过，增量 82 个相关测试通过（`82 passed in 3.92s`），Ruff 及 Mypy 静态检查 0 错误 |
| C 敞口交接与恢复 | 已完成 ✅ | 真实 PostgreSQL 容器验证与交错修复：<br>1. `order_event_repository.py`: FILLED 与部分成交 terminal 保留 active claim 并缩减至实际成交 notional，零成交取消立即释放；<br>2. `order_submission_repository.py`: 在 advisory lock 下校验基线覆盖证据（`open_position_symbols` 且 `baseline_observed_at >= order.updated_at`），已覆盖则安全核销 claim（无长期重复计算），未覆盖则保留敞口占用阻止超限；<br>3. 贯通 `baseline_observed_at` 于 `OrderSubmissionPreparation`、`coordinator.py` 及 `submission.py`；<br>4. 扩展 `tests/integration/persistence/test_order_repository.py`，新增覆盖 6 组交错测试（旧基线超限拦截、基线覆盖后核销无重复计算、部分成交缩减与保留、零成交即时释放、真实连接并发仲裁、乱序/重复事件幂等）；<br>全量回归：46 个持久化集成测试全部通过（`46 passed in 5.94s`），3059 个单元测试全量通过（`3059 passed in 39.75s`），Ruff 与 Mypy 静态检查 0 错误 |
| D 类型导入 | 已完成 ✅ | 修改 domain/strategy/paper_models.py，添加 from __future__ import annotations 与 TYPE_CHECKING 保护 RuntimePlan 导入；保持 PaperTradingRunReport 字段与序列化语义；新增架构单测 tests/unit/domain/test_subpackage_dependencies.py 验证运行期 6 个子包环全部归零、独立子进程导入隔离、类型检查保留 6 环；3046 个单元测试全部通过 |
| E 装配提取 | 已完成 ✅ | 分三小批次完成提取：<br>1. `E.1`: 提取 `live_rollout/database_assembly.py`，统一装配 5 组独立连接池引擎、session 工厂及 12 个仓储；与 `ResourceOwnershipRegistry` 绑定保证逆序销毁与中途失败回收；新增 `test_database_assembly.py`；<br>2. `E.2`: 增强 `live_rollout/execution_runtime.py`，提取 `compile_live_runtime_plan`、`build_capability_evidence_provider` 与 `build_live_submission_fence`；扩充 `test_execution_runtime.py`；<br>3. `E.3`: 提取 `live_rollout/market_assembly.py`，统一装配 24h quote volume、15m closed candle feeds、startup market buffer 及多通道 WebSocket 源；新增 `test_market_assembly.py`；<br>`runtime_orchestrator.py` 单文件行数从 2049 行精简至 1802 行（净减 247 行代码）；全量 827 个 `live_rollout` 单元测试全部通过 |

本地完成要求：相关行为回归、数据库交错、类型与静态检查有明确结果，评审图表与最终实现一致。生产验收另记部署 commit/镜像、运行配置和观测时间，覆盖实际提交、确认与恢复行为；容器 healthy 或历史单测全绿不替代这些证据。

可选的 `TradeCommandExecutor` 函数化只有在上述工作完成且能证明接口更清楚时再安排，默认保留现有封装。
