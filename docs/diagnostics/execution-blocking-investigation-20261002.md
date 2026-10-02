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

从仓库根目录执行下列命令。脚本使用测试夹具，不连接交易所或生产数据库；断言的是已记录的缺陷仍能出现，不能当作修复后的通过测试。

```bash
rtk proxy env PYTHONPATH=.:src .venv/bin/python scripts/diagnostics/cml_repair_race_20261002.py
rtk proxy env PYTHONPATH=.:src .venv/bin/python scripts/diagnostics/cml_external_exit_latch_20261002.py
rtk proxy env PYTHONPATH=.:src .venv/bin/python scripts/diagnostics/cml_order_identity_20261002.py
```
