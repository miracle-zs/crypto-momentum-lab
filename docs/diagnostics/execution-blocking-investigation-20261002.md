# 2026-10-02 CPU 与执行阻塞追查

## 后续修复与验证

### primary USUSDT 为什么在 11:35 平仓

实际平仓单 `cml_7664cc0e04718015b0fb9176c5d2feec` 的 order_intents.details.reason 与 durable_decision_exits.command_payload.reason 均为 `max_holding_period`。开仓成交时间 UTC 03:15:03 左右；退出意图生成于 UTC 03:35:15，约 20 分钟后。

实盘 RuntimeOrchestrator 给决策策略补入 `max_holding_seconds=1200`，而独立 K 线退出管理器使用 `None`，导致两个执行入口使用不同规则。主账户及账户 2、3 的平仓均来自这个时间上限；账户 4 也生成同样意图，只是被身份结算阻塞拦住。这笔退出不是预期的 K 线/宽限期退出。

修复后：实盘决策策略显式禁用该时间上限；RuntimePlanCompiler 区分“没有配置”与“明确配置 None”。已有 `max_holding_period` 待处理意图走只读回执核对，只有无本地订单、无活动预留、无交易所挂单且 client_order_id 查询确认不存在时才标记 SUPERSEDED；未知或已发出的订单继续保留恢复流程。

### 实现收敛

- Book 在处理成交前绑定并持久化交易所订单 ID；真实成交字段不改写，命令水位和预留按关联身份结算。覆盖回执先到、成交先到、重复事件和跨进程恢复。
- 修复流程归入 `ExecutionBook.repair_position()`；读取、数据库事务提交、严格重载发布共享 Book 写锁，锁顺序统一为内存锁 → 数据库锁，保留数据库 CAS。
- 外部成交恢复原因按 PositionKey 隔离并保存到 durable head。完整成交覆盖及后续一致的账户快照确认后才解除；未确认或仍有活动预留时继续阻止该位置交易。
- 带明确交易所身份的 ACK/部分成交可解除未知报单保护，保留未成交部分的预留；部分成交不能伪造完整持仓成交。
- 交易所终态事实可以推进时间戳稍晚的非终态 REST 回执；FILLED 不被取消、ACK 或部分成交覆盖，更新时间保持单调。
- 单位置重载使用 `load_position(key)`；Journal、历史快照、成交查询在 SQL 中按位置过滤，避免全账户逐位置重载。
- 待真实成交的终态命令与恢复门禁跨进程恢复；从已有规范化 ExchangeOrderRow 补读旧版本缺失的外部身份。

### 验证

- 真实 PostgreSQL 上，旧 SQL 可重现毫秒级 ACK/终态错序问题；修复后订单集成测试 20 项通过。
- Book 事务、跨 epoch 恢复、退出回执恢复集成测试 33 项通过；另有新增的终态身份跨进程恢复与定向 SQL 查询测试通过。
- 三个离线诊断脚本改为执行对应永久回归测试，不再断言旧缺陷应该存在。原始失败输出保留在下方事故证据中。
- PostgreSQL 集成测试使用独立临时测试库，只复制结构，不修改生产交易数据。上述真实数据库验证合计 54 项通过。
- 完整单元测试（启用 Hub 本地网络测试）：3,108 项通过；保留两项已有测试依赖/资源回收警告。变更文件 Ruff F/I 检查及 git diff --check 通过。
- 线上部署与运行观察另行记录；这里的测试结果不等同于线上平仓验收。

### 首轮上线验收发现与补充修复

- `130d83d` 首轮上线后三个账户的结构化 readiness 显示 FULLY_TRADEABLE；primary 恢复遗留 CVXUSDT 命令 `cml_036b4aeada766b5d63fb247b51c563be` 时失败。该仓位已有旧 journal、没有 durable head，恢复时将 DISPATCHING 转为 UNKNOWN 却误把首次建 head 当作版本冲突。
- 命令变更现在仅允许已经恢复的本地 head revision=0 与不存在的数据库 head 配对；在同一事务内用 expected_revision=0 首次创建 head。已存在 head 缺失、版本不符或 stream 改变继续封闭 Book 并暴露异常；UNKNOWN 对账门禁保留，不重发订单。
- 本场景先通过真实 Book 单测重现失败，再修复。完整单测更新为 3,110 项通过；真实 PostgreSQL Book 集成检查 4 项通过，包含首次恢复和第二次进程恢复。加上此前覆盖，独立 PostgreSQL 验证累计 55 项。
- 首轮部署同时恢复四策略，启动窗口 CPU 饱和。`CML_LIVE_CONCURRENCY=1` 原本只限制 Compose Docker API 并行操作，未限制 Python 恢复并发。部署脚本改为分批停止/启动，并等待每批健康后再启动下一批；50 项部署脚本检查通过，新增行为测试验证并发限制 1/2 的实际启动顺序及健康失败后停止后续批次。
- 研究采集服务在重负载窗口曾因行情 Hub 超过 120 秒不可用而重启，两次重启无 cgroup OOM。该观察不能等同于研究采集内存泄漏或死循环。
- UTC 04:41:30–32（北京时间 12:41），账户 Hub 最新快照显示四个账户均无持仓、无挂单。最近持仓已在旧运行时阶段退出，不能把当前空仓视为新版本退出链路的自然成交验收。
- 第二轮 preflight 的账户 2 在 UTC 04:57:06 为 fills_catching_up、04:58:06 恢复 ready_readonly；旧三次重试过早耗尽。仅针对结构化结果中唯一 account_ready 错误、其余审批/配置检查全部通过的 syncing 状态，增加为最多 20 次重试，并共享一个原始操作超时预算。数据库、配置、审批、未知错误及命令超时仍立即失败；51 项部署脚本检查验证短暂恢复、持续不就绪、超时总预算和错误类别。
- `bdea896d` 按批部署成功，四策略与四执行服务全部健康。进一步核对命令发现 primary 的 CVX/AZTEC/ZEST 三笔历史真实成交仍为 UNKNOWN，SUPER 的取消单仍为 ACK，而订单读模型均已终结。只扫描 unresolved orders 会遗漏这些恢复命令，容器健康及 FULLY_TRADEABLE 快照不能证明 Book 命令门禁已经解除。
- 恢复扫描增加 Book 命令路径：对已有终态订单读取不可变领域回执；成交数量与价格必须来自数量精确匹配的持久化回执、真实订单成交或按账户/位置隔离的 account fills。沿用既有 positionSide 提取规则处理旧嵌套 payload。通过 Coordinator 的正常 Book 观察/结算重放，未获得真实成交时继续保留 settlement gate；缺少完整价格事实时只查询交易所，绝不重发订单。
- 新增真实 Book 测试验证“订单表已终态但命令 UNKNOWN”的遗漏、跨 session 恢复、零成交取消、缺真实成交继续保护；真实 PostgreSQL 覆盖规范化回执、数量/价格精确性、跨账户隔离、嵌套旧 payload、无 head 首次恢复及终态重放后的再次启动。最终完整单测 3,113 项通过，独立 PostgreSQL 检查累计 64 项通过；临时测试库和角色均已删除。
- 历史仓位的连续扫描/epoch 证明与订单终态回执属于不同证据。回执恢复不放宽 `source-anchored fill scan` 要求，不把旧未确认 coverage 强制改为 READY。
- `18cee108` 部署完成后，primary 的 CVX/AZTEC/ZEST 三笔成交命令及 SUPER 取消命令均通过真实持久化事实重放恢复为 TERMINAL，并保留真实交易所订单 ID。
- CVX 的旧 head 仍为 `legacy-postgres-account/unversioned`：扫描目标只选择新 trade identities、非零 journal 快照或恢复 checkpoint，漏掉了旧 account tables 的真实历史。现在将 legacy head 纳入源扫描集合；这不直接赋予完整性，不完整扫描仍拒绝 epoch adoption。真实 PostgreSQL 回归先复现漏选，再验证完整扫描可采用新 epoch 并跨重启恢复，不完整扫描不能解除保护。
- UTC 05:36/05:46 研究采集重复重启并非 OOM。实际 traceback 是 Hub replay recovery → `_flush_all_buffers` → `drain_queue` 的固定 10 秒超时，慢速 Parquet 落盘使恢复游标未能推进、再次启动后重复补齐。恢复和 stream 切换现在等待持久化队列完成，暂停上游消费以形成背压；落盘异常独立唤醒等待者并立即暴露，不自动重启失败 worker、不把未落盘 journal 标为完成。健康检查及停机继续使用有限等待。
- 最后完整单测 3,115 项通过（含 Hub 本地网络检查），研究采集子集 60 项通过；保留两项既有警告。新增 PostgreSQL 完整/不完整 legacy source 扫描检查均通过，独立数据库检查累计 66 项；补充上线结果见下方最终验收。
- 补充内存/CPU 采样定位到 `ArchiveJournal.commit_materialization → read_resolutions`：每次提交全量解析 137 MB、44,164 行历史 JSONL 并构建字典列表。192 MiB 容器已使用约 163 MB Swap，线程处于 `folio_wait_bit_common`，不是 Python 忙等。将恢复和提交改为逐行严格校验，只缓存最多 131,072 个 SHA-256 身份键；同一已校验文件不重复解析，文件替换/大小/修改时间变化时重新校验，超限退回仅匹配当前待恢复/待提交键的流式扫描。原审计文件保留，append/fsync → manifest → 删除 pending 的顺序保留。
- 内存回归使用 16 MB 历史审计负载：旧实现峰值约 17.7 MB、测试失败；修复要求额外峰值低于 4 MiB。另覆盖重启、外部追加、缓存超限及文件变坏时保留待落盘记录。完整单测更新为 3,119 项通过。

检查窗口：北京时间 11:19–11:41。服务器：43.167.191.253；线上代码 c0697e7189f78dc0a84058e961f0045b6ef7b076。

最初诊断只读取进程、容器日志、Postgres 和账户 Hub，并进行非阻塞 Python 采样、本地离线复现。以下第 1–5 节保留当时证据；末尾记录随后授权的修复与验证。时间窗口中的持仓和阻塞状态不代表部署后的状态。

## 诊断时状态（11:40）

11:40 的账户 Hub 快照（不是历史数据库快照）显示：

| 账户 | 非零持仓 | 挂单数量 |
| --- | --- | --- |
| primary | TOWNSUSDT LONG 42283 | 0 |
| account-2 | TOWNSUSDT LONG 42283 | 0 |
| account-3 | TOWNSUSDT LONG 42283 | 0 |
| account-4 | USUSDT LONG 2583 | 0 |

11:35–11:36 日志仍显示 account-4 的 USUSDT 退出命令被 `Execution reservation settlement requires recovery` 拦截，diagnostics 指向开仓命令 `cml_2058b3355c7749cf5cd853f00beb8b8b`。account-2 曾短暂出现 USUSDT 的 unmanaged/repair-context-advanced 日志，但数据库已记录其 11:35:16 真实 SELL 总量 2583；primary、account-3 也已退出 USUSDT。不能把 account-2 的该段日志等同于账户 4 当前未退出的状态。

## 1. 当前账户 4 阻塞：真实成交与命令身份没有对齐

数据库证据：

- client_order_id：`cml_2058b3355c7749cf5cd853f00beb8b8b`。
- exchange_order_id：`1332041709`。
- 开仓 quantity / executed_quantity 都为 2583。
- account_fill_events 已保存该交易所订单的 34 笔 BUY，合计 2583，成交时间 UTC 03:15:03.340–03:15:03.345。
- execution_commands 已是 terminal，累计数量 2583，但 details.external_order_id 为 null。
- 后续退出在 UTC 03:35:47–03:36:05 仍被该开仓命令的结算恢复保护拦住。

代码原因：`domain/execution/evidence_lifecycle.py` 的 `plan_order_event` 按 `event.client_order_id` 汇总成交，`terminal_settlement_is_confirmed` 按 `outbox.command_id` 汇总成交；真实 `AccountFillEvent.order_id` 是交易所 ID。事件路径也没有把 exchange_order_id 保存进 Outbox 的 external_order_id。该路径没有建立可用于结算的两种 ID 关联。

离线差分复现使用真实 ExecutionBook：同样的 BUY 数量、订单终态和累计数量，只有 fill.order_id 不同：

```text
TRADE_ORDER_ID entry       OUTBOX_EXTERNAL_ID None SETTLED True  BLOCKING_COMMANDS []
TRADE_ORDER_ID 1332041709  OUTBOX_EXTERNAL_ID None SETTLED False BLOCKING_COMMANDS ['entry']
```

复现脚本：`scripts/diagnostics/cml_order_identity_20261002.py`。现有部分结算测试直接把 fill.order_id 设置为 entry/exit 等命令 ID，未覆盖真实交易所 ID 与命令 ID 不同的契约。

修复方向：显式、按账户和位置隔离地绑定 client_order_id 与 exchange_order_id；保留真实成交身份，用关联键结算命令、累计水位与预留。不能通过伪造成交、改写真实 trade_id 或直接清除保护解决。

## 2. 账本 head 冲突：同一进程内修复提交与重载存在窗口

UTC 03:16:51.335，三个账户的 TOWNSUSDT 开仓各成交 42283。约一秒后，三个账户都出现 unmanaged TOWNSUSDT 和 `atomic_execution_observation_failed`，原因是 `execution head changed in another process; restore required`。后续账户通道异常退出；策略容器在北京时间 11:17:31–33 重启。主账户容器事件记录 exitCode=1。

普通 Book 写入共享 `_global_mutation_lock`。但 `live_rollout/position_self_healing.py:auto_heal_unmanaged_position` 先调用独立 repair UoW 提交 head，再调用 `ExecutionBook.reload_position` 获取 Book 锁。

允许的交错：

```text
修复持有数据库锁并推进 head
账户事件获得 Book 内存锁，等待数据库锁
修复提交，释放数据库锁，尝试重载但等待 Book 锁
账户事件读到新 durable head，却仍持有旧内存版本
账户事件报错，Book 设置 persistence_failed
```

本地仅一个 ExecutionBook、一个进程，使用实际修复用例和 observe 路径，复现结果：

```text
atomic_execution_observation_failed ... execution head changed in another process; restore required
unmanaged_position_auto_healed_success ... head_revision=1
RESULTS ['True', 'execution evidence was not durably accepted: execution head changed in another process; restore required']
```

复现脚本：`scripts/diagnostics/cml_repair_race_20261002.py`。

这是确定存在的设计缺陷，且与线上异常形态一致。线上没有逐次 head 写入审计，因此不能仅凭现有日志断言事故中最后一次写入必然来自该修复；也不能根据报错文本认定有第二进程。

修复方向：修复读取、准备、数据库提交和 Book 重载发布必须属于同一内存串行边界；保留数据库事务锁和 CAS，统一内存锁与数据库锁的获取顺序。不能放宽 head 校验或把异常静默吞掉。

## 3. 外部 SELL 导致账户级保护，缺少无 Outbox 的解除路径

账户 4 在 UTC 03:05:31 提交 TOWNSUSDT 时被 diagnostics=`1459898055` 拦截。数据库反查该 ID 是 UTC 00:15:09.904 的龙虾USDT SELL，共 12 笔成交；本地 exchange_orders 和 execution_commands 都没有对应记录。不是当前 TOWNSUSDT 订单查不到，也不是 CLOUSDT 的历史预留直接导致该次拦截。

`plan_reservation_settlement` 将无预留的 SELL 标为需要恢复，Book 把订单 ID 加入 `_recovery_required_commands`；`_act_mutating` 在检查具体位置之前，用该集合阻止账户内全部新命令。普通解除循环只处理存在 Outbox 的命令；该外部订单没有 Outbox，无法通过这条解除路径收敛。restore 会清空内存集合，但这不等于有证据的业务恢复。

离线复现：外部 SELL 被正确入账，同时 unrelated OTHERUSDT 的命令也被该外部订单阻止。

复现脚本：`scripts/diagnostics/cml_external_exit_latch_20261002.py`。

修复方向：区分受管命令结算与外部真实成交，建立可审计的外部成交恢复路径，按位置记录恢复原因并根据完整扫描、账户敞口和受管预留验证消解。不能把所有未知 SELL 都视作可无条件接受，也不能依赖重启清除保护。

## 4. 订单读取状态与已保存终态事实不一致

同一账户 4 USUSDT 订单，exchange_orders.state 是 acknowledged，executed_quantity 已为 2583；事件表存在 filled（UTC 03:15:03.345）和随后返回的 acknowledged（UTC 03:15:03.348681、executed_quantity=0）。

`order_event_repository.py` 的状态推进同时要求 updated_at 不晚于事件 occurred_at。ACK 的时间来自本地回执观察时刻，成交事件时间来自交易所。ACK 若先落库，稍后处理但交易所时间更早的 FILLED 会被时间条件拒绝，累计数量仍因 greatest 而更新。因此出现 ACKNOWLEDGED + 全部成交数量的矛盾读取模型。此处是读模型与事实优先级问题，与第 1 项身份关联问题分开处理。

修复方向：统一时间语义，明确终态事实的优先级与数量一致性规则，覆盖 REST ACK 与 WS FILLED 交错；不能让本地接收时间压制已确认终态。

## CPU 结论与恢复开销

- 服务器 2 核、约 3.7 GiB 内存。
- 初次 vmstat 出现整机 100% 忙碌；同期四策略容器合计约 138% 单核 CPU、Postgres 31%、Market Data 17%。Docker 百分比按单核计算。
- 后续 45 秒逐秒 /proc 采样：整机平均 26.9%，峰值 81.8%，没有 >=90% 的样本；四策略进程各平均约 3.6%–3.7% 单核 CPU。
- Postgres 当次阻塞会话 0、87 个 idle 连接，容器无 OOM 标记；有 Swap 使用与监控换页压力告警，不能据此宣称完全没有内存压力。
- 三个账户启动恢复至 order_state_reconciled 各约 46–49 秒。当前各账户 head 数量约 1510–1747。
- `reload_position` 调用 `load_positions(account_label=...)` 后再过滤单个 key；`load_positions` 对每个位置分别读取 head、恢复事实、trade identities、evidence 和 watermarks。单位置重载会触发全账户逐位置查询，放大恢复开销并长时间持有 Book 内存锁。
- 非阻塞 py-spy 快照以等待线程为主，没有捕获故障期间确定的 CPU 热函数，不能按其等待线程采样占比计算 CPU 归因。

异常退出、全账户恢复和行情计算的负载叠加，与 CPU 峰值时间重叠；目前没有持续死循环的采样证据。优先修复身份关联和提交/发布边界，再为单位置重载提供按 PositionKey 的查询，测量正常阶段和恢复阶段性能。

## 离线复现运行

从仓库根目录执行下列命令。脚本使用测试夹具，不连接交易所或生产数据库；现已改为运行永久回归检查，上方失败输出保留为原始事故证据。

```bash
rtk proxy env PYTHONPATH=.:src .venv/bin/python scripts/diagnostics/cml_repair_race_20261002.py
rtk proxy env PYTHONPATH=.:src .venv/bin/python scripts/diagnostics/cml_external_exit_latch_20261002.py
rtk proxy env PYTHONPATH=.:src .venv/bin/python scripts/diagnostics/cml_order_identity_20261002.py
```
