# 从事实一致性出发解决新开仓后的退出降级

## 结论与范围

21:30 MAGICUSDT 事件暴露的首要问题是**未完成同步被当成已确认的业务异常**。交易所分开发送仓位和成交事实，领域投影依赖时间差推断一致性，上层又把“没有可用批次”折叠成 unmanaged，退出调度因此进入故障路径。这是具体的事实契约缺口，不需要合并所有类或重建交易框架。

本文归档的是实施前的只读生产核对、离线复现和调整计划；下文失败结果为修复前证据。后续已落实代码修复并通过回归，实施和部署验收见 [落地记录](../diagnostics/entry-fact-fix-rollout-20261002.md)。资源观测见 [十分钟复测](../diagnostics/resource-observation-followup-20261002-2121.md)，原始事件及探针结果见 [归档证据](../diagnostics/entry-fact-order-evidence-20261002.json)。

## 1. 必须成立的业务不变式

1. **仓位归属由实际成交、订单身份和批次边界证明。** 已有订单或累计回报说明应继续追踪，但不能直接合成逐笔成交，也不能用快照数量创造一个策略批次。
2. **只有可比较的事实截面才能确认数量冲突。** 本地接收顺序、同毫秒时间戳、数量恰好相等或等待三秒，均不能独立证明跨消息、跨 REST/WS 的完整截面。
3. **等待事实与执行失败是不同状态。** 等待时不授权受影响仓位发单，保留原始退出触发；事实追平后，用当前批次及版本重新计算，不沿用旧数量和旧分配。
4. **事实提交先于发布与唤醒。** 每个仓位键的事务/CAS 防止过期写入；事务外发布版本通知。不会把交易所网络请求放进数据库事务，也不会用本地锁假装外部两种消息原子到达。
5. **故障范围与已证明的不确定范围一致。** 仓位键是 environment/account/symbol/position_side。账户级风险不确定可以阻止全账户开仓；单仓位投影滞后不能无条件阻断其他正常仓位的退出。

这些原则与既有 [批次一致性设计](position-batch-consistency-20260925.md) 相符。已有 PositionView 的 CATCHING_UP、INCOMPLETE、CONFLICT、coverage、projection_version 等字段，应先兑现其语义。

## 2. 生产事件顺序与证据限度

从 account_user_data_journal 读取 21:30:40–21:31:10 的 MAGICUSDT 原始事件。primary 的顺序为：

| 本地接收时间（北京时间） | 日志序号 | 事件 | 内容 |
| --- | ---: | --- | --- |
| 21:30:47.272331 | 1210 | ORDER_TRADE_UPDATE / NEW | 系统开仓订单，交易所 ID 5692805944 |
| 21:30:52.125317 | 1215 | ACCOUNT_UPDATE | LONG 仓位 1671.4 |
| 21:30:52.125493 | 1219 | ORDER_TRADE_UPDATE / FILLED | trade 295264282，实际成交 1671.4 |
| 21:30:53（日志秒级） | — | live_closed_candle_exit_degraded | unmanaged_live_positions:MAGICUSDT |
| 21:30:54 | — | auto_healed_success | 补入事实后发布投影 |
| 21:30:57 | — | tradeability_recovered | 重新允许开仓 |

两条事实消息的交易所时间相同，本地接收相差 0.176 ms；其间可以执行数据库、投影、缓存更新及策略调度，不能假设消费者看不到中间态。其他三个账户也都是仓位消息先于对应成交消息；account-3 还有两笔分批成交与两次仓位更新交错。

原始日志序号表示本地持久日志的顺序，不能证明交易所全局顺序。received_at 是记录的接收时间，不是提交完成时间；execution_trade_identities.first_seen_at 也不能拿来计算 Book 的处理延迟。现有证据尚不能把约一秒后的降级精确归因于某一条 SQL。

末次核对四张 entry 命令均 terminal，各成交 1671.4、成交额 99.999862 USDT，恢复 gate 为零。account-4 开仓暂停 13 秒后恢复。没有证据证明当时存在应执行却永久丢失的平仓订单；确认的问题是退出评估通道短暂降级。

## 3. 已复现的实现缺口

### A. 领域层仍以时间差推断冲突

`domain/execution/position_ledger.py` 的快照比较分支：

- 快照领先成交且距离最后成交不超过三秒时，给 CATCHING_UP。
- 没有最后成交或超过三秒时，给 CONFLICT。
- 部分分支以数量相等确认可比较；随后其他覆盖检查又可能将 is_comparable 置为 false。

离线复刻“系统订单已 ACK，ACCOUNT_UPDATE 先于成交”得到 `health=CONFLICT, is_comparable=false`。这个特定夹具没有完整 coverage，也没有结构身份冲突；目前的数量冲突结论缺少完整性证明。

**修复边界：** 数量差可以作为待解释的观测，但在缺少可验证截面时，公开差额保持未知，状态为 CATCHING_UP/INCOMPLETE；只有完整、同范围、可比较事实仍不守恒时，才生成 QUANTITY_MISMATCH 冲突。订单身份冲突、重复事实载荷冲突及非法跨 epoch 等真实结构冲突仍独立成立。不能把所有 CONFLICT 统一改成 pending。

### B. 上层丢弃了同步状态与命令上下文

`live_rollout/postgres_runtime.py::_with_execution_book` 使用：

```python
unmanaged = (old_unmanaged_symbols | open_position_symbols) - active_symbols
pending_position_symbols = frozenset()
```

实际 Book 已知系统 entry 回报，并通过 command_requires_recovery 保留“等待真实成交”的保护，但它没有可用批次。当前适配层仍把该仓位分类为 unmanaged。已有 CATCHING_UP 信息也没有影响这次分类。

**修复边界：** ExecutionBook 对同一个 PositionKey 提供包含投影健康、已知命令/结算等待原因及事实进度的操作视图；上下文适配器只映射明确状态。具体使用已有 PositionView 字段，补齐其中缺少的结构化 pending 原因/命令身份，避免上层读取私有字典或另做历史归属算法。单纯知道某张订单存在不是归属证明，只足以进入受保护的同步等待。

| 可证明的情况 | 操作状态 | 执行行为 |
| --- | --- | --- |
| 当前事实对齐、批次与归属可信 | managed / ready | 生成计划，出队继续检查版本、预留与风控 |
| 同键系统订单或结算证据正在追平 | pending / catching up | 暂缓该仓位决策，保留触发，继续 ingest/reconcile |
| 历史起点、coverage 或通知切面尚不能证明 | incomplete | 保持保护，补齐对应事实；不授权订单 |
| 完整事实证实外部仓位或无策略归属 | unmanaged | 隔离并走现有验证修复/人工处理政策 |
| 完整可比较事实仍冲突，或结构证据冲突 | conflict | 隔离、保留证据并进入恢复 |

分类必须至少按完整仓位键验证，不能仅凭相同 symbol 上另一个方向存在批次就消除未知敞口。缓存应绑定账户观察版本与 Book 投影/进度版本，并保留读取后的当前性检查。

### C. 事实更新没有统一解除等待

`live_rollout/exit_channels.py::note_account_facts_changed` 仅设置 candle Event。quote/grace 的重试时间保存在各自局部字典中，与事实通知无关联；`_record_result` 将 pending_live_positions 等等待也加入指数退避。

离线 probe 连续提交两次报价：第一次等待事实，然后调用 facts_changed，再提交第二次报价。当前仅评估第一条，第二条仍被旧的截止时间跳过。证明存在额外等待机制，但不能据此断言它独自造成生产的全部 13 秒延迟。

**修复边界：** 复用现有通道，以仓位事实 generation 统一唤醒：有新提交事实时，允许一次按新版本重评，保留最新报价、原始闭合 K 线及原策略期限。没有新版本时有界等待，重复通知合并，不忙循环。网络故障的退避继续独立管理；事实变化不能直接清除真实执行失败，只有当前版本重评成功才能清除该失败。

## 4. 修复顺序与事务边界

### 第一批：修正事实契约，停止正常成交进入“异常修复”

1. 先写真实 Book → context → exit channel 的集成回归，固定 ACK/receipt/ACCOUNT_UPDATE/分笔成交的交错顺序。
2. PositionLedger 以 coverage 和来源截面决定数量是否可比较，替换仅凭三秒时间窗判错的规则。
3. ExecutionBook 操作读携带同键命令等待状态；provider 保留 pending，不从空 batches 推导 unmanaged，也不借用 legacy 订单扫描补造归属。
4. 正常事实追平由已有账户 ingest 和订单 reconciliation 完成。缺消息或等待超出预算时，补取对应订单/覆盖范围，保持 fail-closed；不要仅等待下一条行情，不能无限 pending。
5. 将误分类触发的 full repair 留给确证的非正常状态。真实修复必须继续核对归属、实际敞口、epoch、CAS 和预留，不能为消告警降低验证要求。

建议的业务判断顺序如下，表示契约而非已落地代码：

```python
facts = await book.read_operational_position(key, observation_version)
if facts.has_structural_conflict:
    return CONFLICT
if facts.known_command_waits_for_facts:
    return PENDING
if not facts.has_verified_comparison_cut:
    return INCOMPLETE
if facts.proven_quantity_conflict:
    return CONFLICT
if facts.proven_external_or_unowned_exposure:
    return UNMANAGED
return READY
```

这不把快照复制为成交，不凭累计回报生成批次，最后 READY 仍须满足既有 coverage、归属、版本和 freshness 条件。

### 第二批：按事实版本恢复退出评估

1. 事实事务提交并发布新视图后，向已有运行时发布 key/version 通知。
2. candle、quote、grace 消费同一进度变化，重评一次并继续复用各自队列/最新值合并；不要新增一个同时持有所有策略和仓储的总控类。
3. 修复发布成功也走同一通知路径，避免只 invalidate cache 而调度通道仍休眠。
4. 清除旧失败时校验当前 key/version/失败代数，不能让较早完成的任务覆盖较新的失败。
5. 新计划必须读取当前批次与数量；排队后的 Coordinator 继续最终准备检查，事务提交后在事务外发单。退出提交前后形成的不同批次继续遵循 CONTEXT.md。

先保持现有账户级新开仓保护，明确待同步理由；将其进一步缩小到单键是独立的风险政策决策。其他健康仓位的 reduce-only 退出不应被无关键的等待阻断。

### 第三批：测量剩余性能问题

先移除上述误分类导致的额外修复与查询，再在覆盖真实开仓、分批成交和收盘的相同采样方法下测量。这些改动能消除明确的多余工作，不能预先承诺整机 CPU 数值下降。

- 将事件接收、原始日志提交、事实提交、投影发布和退出重评分别记录持续时间/进度，辨别 CPU 计算、数据库 checkout、SQL 和锁等待。
- 正常投影成本应随新增事实数增长；出现反复历史重建时统计触发原因与扫描数量。对同 key 的恢复请求合并，用既有历史起点和覆盖证明限定范围。
- checkpoint 保持独立低优先级写入和最新值合并；单次 session.connection 耗时包含 checkout/连接初始化及调度时间，420 ms 的一次测量不等于已证明连接池耗尽。先测等待者数量与持有时长。
- 在无容器内诊断进程干扰的窗口，跟踪服务 anon/RSS、cgroup file cache、Swap 流量与 PSI。近期已有诊断开销，不能用一次 working set 峰值判内存泄漏。
- 据持续工作集加主机余量确定资源预算，区分长期泄漏、热数据增长和冷页换出。当前尚未复现具体泄漏或行情 150 ms 告警的单一根因，不据猜测直接调整风控、连接池或服务资源限制。

## 5. 可执行验证与上线门槛

离线诊断命令（无外部数据库、网络或下单）：

```sh
rtk proxy .venv/bin/python scripts/diagnostics/cml_entry_fact_order_20261002.py
```

在当前实现上运行约 0.5 秒，退出码 1：两个控制场景通过，三个目标不变式失败。覆盖真实 Book/上下文/调度实现，持久化使用既有内存 UoW fixture；它不模拟真实 PostgreSQL 并发提交，也不是已完成修复的回归测试。

```text
receipt_before_trade: pending=[], unmanaged=[BTCUSDT]       FAIL
snapshot_before_trade: health=CONFLICT, is_comparable=false FAIL
external_position: remains protected as unmanaged         PASS
trade_applied: known owned position becomes managed        PASS
facts_change_before_quote_retry_deadline: evaluated=[100]   FAIL
2 controls/probes passed, 3 invariants failed
```

现有相关基线：

```sh
rtk proxy .venv/bin/pytest -q \
  tests/unit/live_rollout/test_book_owned_context.py \
  tests/unit/live_rollout/test_exit_channels.py \
  tests/unit/execution/test_terminal_settlement.py \
  tests/unit/execution/test_repair_publication_race.py
```

结果为 38 passed（0.30 秒）。它们与探针的失败同时成立，说明缺少这些交错场景的业务断言，不能用“测试全绿”反驳复现，也不能把这些仍保护真实外部仓位/并发修复的测试删掉。

实施后还需验收：

- 首笔成交、逐笔成交先到/后到、累计回报先到、同时间戳分批成交及重复消息；解释未完整时不误报数量冲突，不生成订单副作用。
- 完整事实证明真实外部仓位或数量冲突时仍保持隔离；跨账户、方向和 epoch 的证据不能借用。
- 原始 candle 等待期间保留，事实变化后恰好重评；quote/grace 重评不等旧退避截止，也不由重复通知产生订单风暴。
- 修复与实时成交交错、进程在事实提交/通知之间崩溃、UNKNOWN 订单查证，真实 PostgreSQL 验证事务/CAS 和恢复；无二次重复下单。
- 同一版本持续等待时无全历史忙扫；确证待处理事实不丢失；新退出批次不被旧分配授权。
- 生产观察覆盖实际触发的退出及确认，不以健康探针或只跨过收盘时钟代替退出验收。

实施完成后再提交并发布。本文归档的是根因定位和具体修复设计，不是部署完成声明。
