# 重构进度审查（2026-09-27）

审查基线：`42d95a1`；最终代码快照：`9c15a7589c510c4a0755b5607d792b4051d8e6a5`。比较命令：`git diff 42d95a1...9c15a75`，共 20 个提交。审查期间新增的灰度接线、能力评估接线和 dataset catalog 已纳入代码核对。

结论：领域建模与部分生产接线已有实质进展，但目前不能把 R1/R2/R3 标为完整完成。已复现两个功能缺陷和一个审计误报；持久化闭环、状态所有权仍存在明显缺口。提交标题中的“authoritative”“reproducible”“atomic”不能代替验收证据。

本次仅审查并记录，不修改实现、不部署、不改变服务器状态。没有重新连接服务器；以下“接线”指本地代码接线，不证明线上正在运行该快照。

## 进度与验收

成熟度沿用蓝图：L1 领域原型，L2 生产接入，L3 真实持久化/并发/故障恢复验收，L4 旧权威路径删除。

| 范围 | 已有进展 | 本次可确认的界限 |
| --- | --- | --- |
| R1 执行 | ExecutionBook、预留、outbox 状态机、account-4 灰度 observe 接线 | L1 + 部分 L2；outbox/receipt/去重仍为内存，旧 coordinator 直接写内部状态；存在累计成交重复消费。不能认定 L3/L4 |
| R2 输入重现 | MarketRevisionRef、DecisionFrame、Trace 仓库及 Live 保存回调、审计 CLI、dataset catalog | L1 + 部分 L2；实际决策 hash 漏参数，CLI 不重放却认证成功，Trace 可覆盖。不能认定重现验收通过 |
| R3 政策 | 共用 transition、多空/持仓时钟、next policy state | 领域实现有进展；Live 状态只在内存更新，Trace 异步另存。未达到 next state/trace/accepted command 原子提交要求 |
| 定量/分配 | sizing、lot 量化和 allocation 模型 | 已有领域实现；本次未做交易所规则与完整订单路径的专项验收，不据此标 L3 |
| R4 清理/数据集 | RetentionAuthority、DatasetId 锁、结构化结果、catalog 与批量/流式查询 | 已有实现；本次未运行真实 PG 双连接清理竞态、崩溃与恢复验收。不能据单测确认受保护数据不会误删 |
| R5 能力/发布 | RuntimePlan、CapabilityEvaluator、fence 接线及部署协议 | 代码接线有进展；未做实际部署接管、旧 writer fencing、故障恢复演练 |
| R6 收益/运营 | receipt、资产/环境隔离、计算方法、funding 和运营模型 | 已有模型与测试；本次没有认证真实流水来源扫描完整性、资金时点估值和线上收益结果 |

后四行是验收证据边界，不等同于已发现功能错误。也不应把此次未测试的部分自动判为失败。

## Standards

### S1 · P2：灰度适配器直接修改 ExecutionBook 私有状态，状态所有权没有收敛

位置：`src/crypto_momentum_lab/execution_account/orders/coordinator.py:613`、`:625`。

`_ensure_reservation` 在旧 coordinator 中创建 command，并直接写入 `_execution_book._outbox_by_command_id` 和 `_command_reservations`，绕过 ExecutionBook 的 `act`、请求去重、版本校验和接受回执。随后仍由旧 coordinator 保存和结算预留。

规则：`module-responsibility-matrix.md:27` 要求确定“该状态的唯一法定权威”；蓝图 §7 规定由 ExecutionBook 接受请求并事务提交 command/reservation/outbox。这里是职责契约违反，同时符合 Feature Envy/重复状态管理的启发式特征。

影响：新旧实现共同维护状态，未来修复预留、幂等或恢复仍需改两处；不能把这次灰度接线视为旧权威已删除。应让适配器调用公开协议，由单一服务拥有事务和状态。

### S2 · P2：Trace 后台任务没有生命周期所有者

位置：`src/crypto_momentum_lab/live_rollout/decision_facts.py:267–285`。

`record_trace` 直接 `loop.create_task`，没有保存任务、交给 supervisor 或提供 drain/close；保存错误只记录 warning。

规则：`lifecycle-ownership-contract.md` §一第 1/2 条要求后台 Task 有唯一父节点，创建者编排清理。这里是明确契约违反。

触发：决策后立即停机，或连接池先关闭。任务可能尚未写入数据库，正常关闭流程无法确认最后一批 Trace 是否耐久。应接入受监督队列及有时限的 drain；核心决策证据还需采用下述原子提交协议。

## Spec

### F1 · P1：重复查单把累计成交再次消费，提前释放退出预留

位置：`src/crypto_momentum_lab/execution_account/orders/coordinator.py:727`；调用路径包括 `reconcile_order` 和 `apply_observed_snapshot`。

每次用完整 `res.executed_quantity` 作为本次消费量，没有减去该订单已结算的累计量。

蓝图 §7.2 明确要求：“累计成交回报 3 → 3 → 5，只消耗 5”。

纯内存探针：预留 10，连续传 PARTIALLY_FILLED 累计量 3、3、5，得到 consumed=3、6、10，active=7、4、0。最后真实成交 5，却认为全部预留已经消费。新 observe 的 fill 去重无法修复此前 coordinator 已写入的错误消费。

修复要求：按订单维护耐久累计结算水位，计算非负增量；去重、水位更新、批次结算同一事务。验证重复、乱序、跨批次、部分成交后取消以及进程重启。

### F2 · P1：实际决定使用的 hash 未绑定完整参数，同 ID 可产生相反结果

位置：`src/crypto_momentum_lab/domain/decision/policy_transition.py:119–133`、`:215`；`decision_engine.py:476–478`、`:591–593`。

实际 `decide` 调用 transition 的 hash，内容仅 frame digest、policy ID/version 与 prior state version。Frame 的 code/parameters/state digest 仍以版本号拼字符串。新增的较完整 `compute_decision_input_hash` 没有用于该返回结果。

蓝图 §8 要求代码、参数、状态摘要分开，并且“不存在相同 id/version 可以有不同内容的兼容规则”。

纯本地探针：价格 65500，入场阈值 65000 改为 66000；前者生成 intent，后者 `below_entry_threshold`，但 `same_hash=True`、`same_id=True`。

修复要求：对真实冻结政策、状态及所有语义输入做规范序列化；只保留一个权威 hash 入口。测试实际 decide 返回身份，而非只测试独立 hash helper。

### F3 · P1：重现 CLI 没有执行重现却返回认证成功

位置：`src/crypto_momentum_lab/tools/reproduce_decision.py:74–81`。

工具只加载 Trace、列出引用并读取保存的输出；没有重新执行 transition、校验内容/输入摘要或比较输出，即返回 `VERIFIED_REPRODUCIBLE`、`reproduced=True`，CLI 随之退出成功。

蓝图 §8/P2 验收要求从真实 runner 决策在新进程“逐字段重现”，不能以读到记录替代。

隔离 mock 探针：提供 input_hash=WRONG、frame_digest=WRONG、空 refs 的已加载对象，工具仍返回 VERIFIED_REPRODUCIBLE。该探针验证审计器无验证逻辑，不代表真实仓库允许空 refs。

修复要求：加载真实政策工件和冻结状态/输入，校验 refs 内容并重放比较；证据不全返回不可重现或未验证，不能成功认证。

### F4 · P1：同一 decision_id 的 Trace 可以被不同结果覆盖

位置：`src/crypto_momentum_lab/persistence/postgres/decision_trace_repository.py:143–159`。

插入冲突时更新 intent/rejection/payload，却保留旧 evaluated_revision_ids、strategy/account/time。F2 的同 ID 不同结果会直接触发覆盖；其他 ID 冲突也会制造“旧引用 + 新输出”的混合记录。

蓝图 §8 规定不可变身份与内容，历史决策所见不能被修订结果冒充。仓库自身 docstring 也声明 immutable。

证据：SQL 构造明确采用 ON CONFLICT DO UPDATE，更新列表没有内容一致性校验。这是代码核对结论；本次未在真实 PG 执行冲突探针。

修复要求：同 ID 同内容返回原记录；不同内容显式冲突并阻断，不能覆盖审计证据。引用由权威已持久 revision 解析，不能用简化占位 payload 补出看似可解析的证据。

### F5 · P1：执行恢复和政策提交仍未形成持久化闭环

位置：`src/crypto_momentum_lab/domain/execution/execution_book.py:222–228`；`live_rollout/decision_facts.py:253`、`:273`、`:293`。

请求、回执、evidence/trade 去重、outbox、command-reservation 链接和累计成交水位仍放在 Python dict/set。恢复预留不等于恢复上述协议状态。Live 的 policy state 初始化为默认状态，在回调中先改内存，再另起 Task 保存 Trace；没有 next state、accepted command、reservation、input cursor 的共同事务。

蓝图 §7.3 要求事务提交 command+reservation+outbox；§9 要求“next state、trace 和已接受命令必须共用原子提交”。

触发：进程退出或崩溃后，outbox/去重/回执和冷却等状态消失；交易所副作用与数据库预留可能仍存在，新进程不能根据当前协议可靠判断原请求是否已接受/发送。此项为代码持久化路径核对，尚未做真实崩溃演练。

修复要求：先建立耐久 inbox/outbox/receipt/head/checkpoint 及事务 API，再做灰度接管；覆盖提交前后崩溃、发送后 ACK 丢失、重复回报、恢复时原请求重试。不能仅给内存状态机继续加 callback 来宣布完成。

## 验证记录与下一步

最新快照相关测试：169 passed in 1.14s。选择了 decision、ExecutionBook、订单 coordinator、runtime、operational、performance、Live facts/fence、dataset catalog 单测，排除 integration/live，显式移除数据库测试环境变量。测试通过并未覆盖上述重复累计回报与实际决策参数身份缺陷。

补充探针均在本地运行，无数据库/交易所请求，无实现修改：累计成交重复消费、实际 decide 阈值变化同 ID、CLI 假认证，三项均复现。

建议先处理 F1/F2/F3/F4，并补针对性行为测试；接着用一个持久化事务协议解决 F5/S1，接入 S2 的任务生命周期。随后再完成真实 PG 并发与故障门槛，才重新评定 L3。R4/R5/R6 需另做真实证据验收，不能靠当前单测结果代签。

Standards：2 项，最高 P2（状态所有权/任务生命周期）；Spec：5 项，最高 P1（累计成交结算、决策身份与审计证据）。
