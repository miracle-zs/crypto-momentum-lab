# 工程原则专项审查：事实、资金、订单与运行可信度

审查日期：2026-09-25（Asia/Shanghai）；服务器采样从 2026-09-24 23:00 UTC 开始。

本地基准：`e79fbce4dad6d732a76fbfb056c28ac6786d5556`。

服务器：`43.167.191.253`；运行代码及应用镜像：`86a89a911f3ae9b3385d1d7deced1c7b8beb261e`。

标准来源：本次用户提供的 11 条工程原则，以及仓库现行生命周期、模块职责和读模型契约。

## 1. 结论与边界

项目已经具备实质性的交易保护：Decimal 量化、按 client order ID 恢复不确定订单、数据库提交前 fencing、用途隔离连接池、闭合 K 线消费、有界行情队列、checkpoint/journal 持久化和大量故障路径测试。不能把它评价为“没有状态机、没有幂等、没有测试”。

主要缺口是**事实覆盖与就绪状态之间仍存在不可靠转换**：未物化的行情修订被确认完成；账户只读就绪被推断成可交易；缺失的风险数据被前端当作无风险；旧正常缓存没有明确过期标签。研究对账工具另有浮点金额和持仓方向丢失问题。迁移可以从空库执行，但模型、迁移和实际生产索引并不完全一致。

本次确认 **14 项问题：6 项 P1、8 项 P2**。没有获得足以证明正在发生重复下单、资金损失或生产数据损坏的 P0 证据。P1 表示应优先修复的正确性、操作判断或数据安全风险；P2 表示需要具体条件触发的可靠性、研究正确性或部署维护风险。优先级不等于生产事故已发生。

审查覆盖关键运行链路、持久化、迁移、Dashboard、测试和部分 `local_optimization` 对账代码。未逐行审查每个历史分析脚本、所有生成报告、第三方静态资源或所有历史提交；没有声称穷尽全部缺陷。采用静态调用链复核、真实组件本地复现、现有测试、隔离 PostgreSQL 迁移和服务器只读采样。未部署、未重启服务、未修改实盘配置、未提交订单、未执行生产迁移或清理；密码及账户秘密不写入本文。

本文所有 `路径:行号` 相对仓库根目录，行号对应上述本地基准；生产状态单独标注，不能将本地修复当作已经上线。

## 2. 11 条原则覆盖矩阵

| 原则 | 已有机制与评价 | 本次缺口 / 验收方向 |
| --- | --- | --- |
| 1 事实来源、时间、版本 | UTC aware 时间、原始 envelope、15s 投影、官方闭合 15m K 线已有分层 | F01 修订凭证错误；F02/F04/F05 新鲜度与来源状态失真；F06 成交覆盖未闭合 |
| 2 金额与业务身份 | 实盘量化和 PostgreSQL Numeric 使用 Decimal；账户成交有环境、账户、symbol、trade_id | F07 研究资金计算使用 float；F08 对账方向与账户身份转换不完整 |
| 3 可对账订单状态机 | 提交前持久授权、fencing、唯一 client ID、超时查询、未知状态保留 | 未发现并证实盲目重发 POST；F06 事实覆盖不足不能由 ready 状态替代 |
| 4 清晰模块边界 | domain / application / adapter / persistence 已分层，有明确 DTO | F02 readiness 重复推导；F08 研究层重新实现不完整持仓语义；大模块按职责而非行数治理 |
| 5 异步与资源所有权 | 分池、队列 maxsize、主要归档 I/O to_thread、关闭流程和 owner 已存在 | F10 ingest 同步全目录扫描；F11 cache/lock 无界；另有订单 scheduler 容量待治理 |
| 6 有界故障处理 | 外部请求超时、有限重试、取消传播及 UNKNOWN_PENDING_RECONCILIATION | F03 halt 变 500；F05 后台刷新吞异常；F07 坏金额转零 |
| 7 迁移与兼容 | 空库升级成功，生产 Alembic revision 与本地 head 一致 | F12 schema drift；F13 分区回退不成立；需规模化回填和分区恢复验证 |
| 8 按风险验证 | 单元、集成、fake exchange E2E 和前端测试齐备 | F09 可销毁库身份未校验；版本/就绪反例没有被现有绿测阻止；前端有 1 项失败 |
| 9 可观测且不误导 | 结构化日志、账户标签、健康文件、配置指纹、secret repr=False | F02–F05 readiness 不可信；F14 固定“完整100%”；生产和本地版本不同 |
| 10 前端与依赖 | 原生模块、超时、in-flight 去重、部分 requestId 防旧响应已有 | F04 安全区轮询与新鲜度；不需为低频轮询机械增加 rAF 或框架 |
| 11 决策记录与性能 | 已有架构契约、ADR、SLO、checkpoint 错峰与运维文档 | F12 部分索引变更未形成可重建迁移；旧性能审查对 checkpoint 时长的因果判断需修正 |

## 3. 已确认问题及整改验收

### F01 [P1] 同键新修订未写入，却被确认已物化

**原则：1、6、8。证据等级：真实组件复现。**

`src/crypto_momentum_lab/research_collector/storage.py:354-390` 对相同 `(environment, symbol, bucket_start)` 的不同 payload，只在来源优先级提高时替换；相同来源的变化保留旧值。`materializer.py:94-98,133-138` 仅以这个自然键判断 receipt 已覆盖，没有 revision 或内容哈希校验。

因此，同源修订即使被 sink 丢弃，flush 仍可能推进对应 materialized sequence，移除 pending journal。真实 `ArchiveJournal → WindowMaterializer → ParquetWindowSink` 临时目录实验：先写 seq1、close=100，再写 seq2、close=102，得到：

```text
expected_incoming_close='102'
stored_close='100'
materialized_sequence=2
pending_records=0
committed_receipts=1
```

这不是仅仅缺少字段，而是完成凭证与已写内容不一致。生产 `runtime_state_repository.py:212-245` 的 `mark_incomplete()` 会修改既有桶，数据质量修订是实际存在的输入。`tests/fixtures/market_data_dual_revision.py` 的 revision envelope 只在测试 fixture 中构造，不能证明生产路径保存了该版本。

**整改：**以来源、revision 或稳定内容哈希标识版本，receipt 核验具体版本；区分 `materialized / superseded / rejected / quarantined`，不能用自然键存在替代物化完成。另保存决策可见版本，避免 canonical 回补覆盖决策历史。

**验收：**真实 journal/sink 链覆盖同源变更、HUB→PG 升级、`data_complete=true→false`、重启重放；每条 materialized receipt 都可追溯到对应持久版本或显式替代关系。

### F02 [P1] 账户只读状态被推导为 FULLY_TRADEABLE，未接入真实门禁

**原则：1、3、4、9。证据等级：本地真实函数+假依赖复现。**

`operator_dashboard/overview_queries.py:105-115,206-271` 将 `ready_readonly` 转成 READY，再仅按账户是否 READY、是否存在 halt 推断 `FULLY_TRADEABLE`。它没有要求账户新鲜、有效 lease、策略 entry gate、未决订单清零或账本覆盖完整。stream 的 overall 也来自账户 readiness，不来自各 stream 的状态。

复现输入为：一天前的 `ready_readonly` 账户记录、无策略状态、无 lease、market-data=STALE、无 halt。真实函数输出：

```json
{"status":"READY","tradeability":{"mode":"FULLY_TRADEABLE","entry_gate_open":true},"stream_readiness":{"overall":"READY","streams":{"market-data":"RECOVERING"}}}
```

此外，`live_rollout/readiness.py:495,508` 新增 `update_stream_readiness()` 和 `update_tradeability()`，在生产源码搜索中仅有定义，没有调用；字段存在和单元测试通过不等于已经与运行状态接线。

**影响界限：**已确认是运维状态误报；没有证据说明此 HTTP 返回直接绕过真实订单风险门禁。

**整改：**让执行运行时发布带账户、run/session、来源时间和有效期的权威 gate/stream 快照；Dashboard 仅做显式转换。缺失、过期、未接线状态为 UNKNOWN/STALE，不能以只读同步成功推断交易授权；明确 EXIT_ONLY 的实际退出条件。

**验收：**陈旧账户、lease 过期、策略停机、未决订单、缺流、缺账户、退出通道不可用等反例均不能 FULLY_TRADEABLE；测试跨越运行时发布与真实查询层。

### F03 [P1] readiness 在 halt 分支引用不存在的枚举，故障时接口失效

**原则：6、8、9。证据等级：确定性异常复现。**

`operator_dashboard/overview_queries.py:230-232` 使用 `OperationalStatus.UNHEALTHY`，但 `operator_dashboard/status.py:5-16` 没有该成员。存在 active halt 时，`await OverviewQueries.readiness()` 抛出：

```text
AttributeError: type object 'OperationalStatus' has no attribute 'UNHEALTHY'
```

HTTP 路由会走错误路径，而非返回结构化 halt 原因；F05 的缓存还可能先遮蔽该错误。若 `health()` 自身抛数据库异常，则更早失败，不能说所有数据库异常都会到达这个枚举分支。

**整改及验收：**统一状态枚举和 API schema，明确 HALTED/DOWN/DEGRADED 的意义；分别覆盖 active halt、数据库异常及正常恢复。真实 OverviewQueries 的故障分支测试不可仅由 mock API response 替代。

### F04 [P1] 前端缺少风险/账户数据时仍显示“关键读数正常”

**原则：1、9、10。证据等级：真实 JavaScript 函数复现。**

`operator_dashboard/static/dashboard.js:117-142` 仅遍历已加载分区；缺 risk 时 ambiguous 默认为 0，缺 account 时 mismatch 归约为 0。`:157-179` 在 overview 存在且这些零值未触发异常时返回 READY。`:403-417` 轮询 overview 与当前页面；没有访问风险/账户标签不等于对应风险不存在，离开标签后的旧数据也没有独立失效门禁。

用 Node 执行真实 `globalReadinessModel`：仅提供正常 overview、LIVE 模式，不提供 risk/account，得到 `status=READY`、`detail=实盘链路运行中 · 关键读数正常`、`uncertain=0`、`ambiguous=0`、`reconciliation=—`。

**整改：**优先消费修正后的权威 readiness；若仍聚合，规定必需分区集合及每项有效期，未加载和过期都阻止 READY，关键状态轮询不能依赖当前标签。

**验收：**首次只加载 overview、风险请求未完成/失败、切离账户页后过期、未识别状态均显示 UNKNOWN/STALE；旧请求不得覆盖新账户状态。保留现有 AbortSignal 和 requestId 保护。

### F05 [P1] 安全接口的旧正常缓存没有 STALE 标识，刷新异常被吞掉

**原则：1、6、9。证据等级：静态完整调用链。**

`operator_dashboard/api.py:82-92` 在 TTL 过期但 stale-while-revalidate grace 内原样返回旧对象；`:145-148` 后台刷新异常直接 return。readiness TTL 为 5 秒并有 60 秒 grace；live accounts TTL 为 15 秒并有同样 grace，risk-execution 也启用该机制。

故上次成功后约 65/75 秒内，数据已变化或查询失败仍可返回 HTTP 200 和旧 READY；没有 `cache_age / stale / refresh_error`。保留旧 observed_at 是必要元数据，但不能代替显式降级，尤其 F04 不据此失效全局状态。

**整改：**交易安全状态禁用 SWR，或返回明确缓存年龄和 STALE/UNKNOWN，不能把旧门禁当作当前授权；历史曲线可保留 SWR。刷新错误结构化记录，loader 有总时限。

**验收：**READY 后注入 halt、数据库错误和超时，过 TTL 后不得继续声称当前 READY；grace 后故障仍可诊断，任务取消正确传播。

### F06 [P2] 成交补采只取一页，覆盖未完成却标记本轮 ready

**原则：1、3、6。证据等级：静态完整调用链。**

`execution_account/binance/client.py:492-529` 每 symbol 只执行一次 `userTrades(limit=1000)`，没有分页循环或 truncated/has_more 返回值。`execution_account/sync.py:741-765` 推进 cursor 并更新 `last_checked_at`，`:778-791` 返回 `READY_READONLY`、`mismatch_count=0`，持久化的 reconciliation 也记为 ready。

断线恢复或历史回补超过 1000 笔时，首次仅持久化一页；余页留待后续轮询，非活跃历史 symbol 受默认 6 小时重扫间隔影响。**没有证据表明 cursor 永久跳过余页；问题是覆盖不足却表现已就绪和恢复延迟。**

**整改：**有限预算分页；达到短页或明确覆盖水位才完成。预算耗尽保存 continuation，标 `catching_up/incomplete`；“检查过”与“覆盖到”分开记录。

**验收：**假交易所返回 1000+1 笔、跨页失败、重启和关闭持仓后仍待补页；cursor 可推进，但完成状态不得提前。

### F07 [P2] 研究对账以 float 计算资金，坏金额被替换为零

**原则：1、2、6。证据等级：代码及小输入复现。**

`local_optimization/run_live_reconciliation.py:44-54,132-135,339-344` 将价格、数量、realized PnL、fee 转为 float 后求和和差值；非法/空值默认 0。`local_optimization/reconciliation.py:422-480` 同样以 float 计算成本、手续费分配和净收益。

实际函数输出 `to_float('not-a-number') == 0.0`，字符串 `0.1+0.2` 归约为 `0.30000000000000004`。这些不是图表最终坐标转换，而是对账金额计算。最终 round 不能恢复已丢失的精度，也不能区分缺失手续费与真实零费用。

**整改：**账务与对账金额使用从原始字符串构造的 Decimal；按资产/交易所规则量化；坏值形成带行号、字段及来源的 INVALID/NO_DATA。统计优化中非资金指标可保留 float，但不得把它作为金额权威来源。

**验收：**小数累加、大额抵消、极小成交、多资产费用、空值和非法值；汇总与逐笔精确账务可对齐，失败不能产生看似有效的零收益。

### F08 [P2] 研究成交配对把 BUY 固定当开多，丢失真实持仓方向

**原则：2、3、4、9。证据等级：真实函数复现。**

`local_optimization/reconciliation.py:397-421` 聚合只识别 account_id，没有显式接收生产 account_label/environment/position_side；`:438-491` 按账户与 symbol FIFO，BUY 入队、SELL 平仓，输出 side 固定 BUY。生产导出读取路径 `local_optimization/dashboard.py:294-307` 直接传入 CSV 行。

输入同一账户两笔 SHORT 成交：先 SELL 1@110，后 BUY 1@100（平仓 realized_pnl=10、每笔 fee=0.1）。输出只剩 SELL 的 `N/A (Carry-In)`，side=BUY、account_id 为空、net_pnl=0，没有形成正确的已平空头 9.8 净收益记录。

按账户目录分别调用提供了一部分外层隔离，因此不能据此断言当前四账户页面已经互相串账；但函数自身的身份和方向契约不足，不能安全用于跨账户/双向持仓归并。若工具只支持 long，应显式拒绝不支持输入，而不是静默生成报告。

**整改：**边界映射并保留 environment/account_label、position_side、成交标识、订单 ID 与 batch；复用已存在的领域成交/持仓语义。窗口外 carry-in 与真实零收益分开表达。

**验收：**SHORT 先卖后买、LONG/SHORT 同 symbol、不同账户相同 order_id、窗口前持仓、部分平仓和手续费按批次分配。

### F09 [P1] 测试数据库可被任意 URL 覆盖，随后执行无条件清表

**原则：8。证据等级：静态清理路径；未对危险目标执行。**

`tests/conftest.py:162-167` 无条件接受 `CML_TEST_ASYNC_DATABASE_URL`。`:174-194,203-218` 对策略、checkpoint、信号、行情等模型执行无 WHERE 的 DELETE 并提交；`tests/integration/persistence/test_order_repository.py:65-82` 同样清理订单相关表。默认 localhost 是已有保护，但变量命名含 TEST 并不验证数据库身份。

**触发：**开发或 CI 将测试 URL 指向持久业务数据库。后果可能是业务历史和恢复状态被删除，与是否使用假交易所无关。

**整改：**测试进程创建并拥有独立可销毁数据库/schema，使用专用受限角色和身份哨兵；所有迁移、清理入口先验证目标，失败即停止。外网与私有交易适配器默认禁用，受控 E2E 显式放行。

**本次措施及验收：**本次新建本地 `cml_review_20260925` 后才运行集成测试，未使用现有 cml 数据库。补测试验证错误库名、无哨兵和远端目标在 DELETE 前拒绝；仅字符串含 test 不够可靠。

### F10 [P2] collector 每个接收批次在事件循环同步扫描历史目录

**原则：5、11。证据等级：阻塞调用链已确认，生产影响未量化。**

`research_collector/service.py:436,958-962` 的 async ingest 直接调用 `_ensure_capacity()`；`storage.py:624-626,650-660,821-831` 经 `CapacityGuard` 对 root 递归 rglob 并逐文件 stat。目录越大，每批接收的扫描成本越大，阻塞其他接收、健康与取消任务。health 路径已经 to_thread，不代表 ingest 也已隔离。

**整改：**单一 owner 以受控频率在线程扫描，发布带采样时间的容量快照；必要时按写入增量估计并周期校正。快照过期/扫描失败必须显式降级，不能无限使用旧容量。不要未经测量直接增加扫描并发。

**验收和测量：**1万/10万/100万文件、预期 batch 速率下，记录 scan latency、loop lag p95/p99、queue age、取消耗时；注入慢文件系统时 heartbeat 和取消仍及时。本文未声称这一调用已造成当前服务器丢数据。

### F11 [P2] Dashboard 动态缓存和 key 锁无界保留

**原则：5、11。证据等级：代码生命周期确认。**

`operator_dashboard/api.py:67-69,93,103,133-142` 的 entries/locks 字典没有容量或淘汰逻辑。带 run_id/account_label 的动态端点不断生成 key；loader 失败仍留下 lock；后台 refresh 仅按 key 去重，无总任务上限。

**整改：**有界 TTL/LRU、协调清理空闲锁、不存在对象短缓存或不缓存、刷新全局并发预算。认证降低恶意请求面，但不解决长期查询历史 run 的增长。

**验收：**大量不同/无效 key 后 entries、locks、task count 有界；淘汰不能破坏同 key 互斥；测 RSS 和请求延迟，不以 DB pool 有界替代任务有界。

### F12 [P2] 空库迁移成功，但模型与迁移及生产索引不一致

**原则：7、11。证据等级：真实隔离库 Alembic 检查及生产只读索引查询。**

隔离 PostgreSQL 从空库升级到 `20260918_0039` 成功；随后 `alembic check` 退出码 255，提示新增升级操作。应区分真实差异和比较噪声，不能照单自动生成并应用：

| 差异 | 证据与解释 |
| --- | --- |
| `ix_exchange_orders_unresolved_partial` | 模型 `models.py:1164-1172` 定义，迁移链未找到创建；空库缺失，生产实际存在。新环境不能仅靠 Alembic 复现该性能结构 |
| `ix_universe_entries_price_time` | 模型 `models.py:94` 定义，迁移链未找到创建；空库缺失，生产存在 |
| `ix_runtime_market_states_15s_symbol_time` | 模型 `models.py:245` 仍定义；`20260823_0019` 明确删除与 PK 等价的索引。不能用自动生成把已删除冗余索引加回来 |
| account config / reconciliation 索引 | 数据库存在、模型未完整描述，autogenerate 建议删除；要人工保留/解释用途 |
| hour expression、exit episode unique 名称 | 有表达式规范化和名称差异，不能直接解释为约束语义缺失 |

**整改：**统一模型、迁移和允许的运维扩展契约；实际必需索引补幂等迁移，冗余模型声明清理；生产规模索引评估 concurrent 创建、锁等待、失败恢复和部署顺序。

**验收：**空库、上一生产版本、已人工创建索引三种起点均达等价结构；`alembic check` 仅允许逐项说明的差异；用目标规模 EXPLAIN 和延迟数据判断性能效果。72 项绿测没有替代索引一致性检查。

### F13 [P2] 0037 downgrade 无法用于实际分区表

**原则：7。证据等级：真实 PostgreSQL 复现；生产表型已只读确认。**

`alembic/versions/20260918_0037_strategy_runtime_events_composite_pk.py:40-56` 将 `(event_id, occurred_at)` 主键改回单列 event_id。生产 `strategy_runtime_events` 的 relkind 为 p（分区表）。

本次在隔离库的事务内创建同结构最小分区表，实际调用该 migration 的 downgrade，返回：

```text
NotSupportedError: unique constraint on partitioned table must include all partitioning columns
DETAIL: PRIMARY KEY ... lacks column "occurred_at" which is part of the partition key.
```

实验事务已回滚；未对生产执行。普通表还需考虑升级后跨时间重复 event_id 导致回退失败。

**整改及验收：**明确此步骤的不可逆边界并在修改前给出清晰 guard，或设计独立非分区回迁流程；不能把默认 downgrade 当发布回滚。保留兼容应用版本的 roll-forward/恢复方案，并在代表性分区结构及重复数据上验证。

### F14 [P2] 性能页面把延迟配置显示成“完整100%”

**原则：1、9。证据等级：确定性渲染条件。**

`operator_dashboard/static/sections/performance.js:108-110` 只要 `realtime_closure_delay_seconds` 非空，就渲染 `闭桶交付 ...ms · 完整100%`，未依据实际 missing rows、dropped batches 或覆盖分母计算完整率。延迟参数的存在不能证明数据完整；行情错误或缺行时也可得到该文案。

**整改及验收：**移除固定百分比，或从带时间范围、应有/实有数量、来源的覆盖指标计算；未知显示 UNKNOWN。测试非零 missing/dropped、空覆盖、STALE 状态。现有前端测试期待旧文案“闭桶水位 400ms”，已失败；应修正真实语义后更新测试，不能只改断言让它变绿。

## 4. 模块边界、权威来源与必须保留的保护

### 4.1 当前事实链和建议契约

| 数据 | 当前职责位置 | 权威及必要身份 | 待补强 |
| --- | --- | --- | --- |
| 原始行情事件 | market_data / RawEnvelope / raw archive | exchange、environment、stream、symbol、连接 session、交易所时间与接收时间 | 从原始事件到决策/物化版本的可追溯引用 |
| 15s 市场状态 | runtime_states / PG runtime 表 / hub | environment、symbol、bucket_start；状态是派生投影 | 同键修订身份、来源优先级与决策版本分离（F01） |
| 官方 15m 闭合蜡烛 | closed_candle_feed / REST backfill | symbol、UTC candle_start/end、closed 标记 | 保持 x=true、15m 长度校验；不以实时观察替代闭合值 |
| 订单 | orders/state_machine / order_repository | client_order_id、账户、环境、run、symbol、position_side、exchange_order_id | 请求、交易所确认、DB 持久、业务完成分别记录 |
| 成交与账户快照 | exchange account API/user stream → account_repository | environment、account_label、symbol、trade_id；快照 observed_at | 快照新旧不能决定 fill 是否接纳；成交覆盖水位独立于快照时间（F06） |
| 持仓及策略批次 | domain/execution position ledger / batches | 来源成交、position_side、策略、批次及退出分配 | 研究端不可自造简化 FIFO 取代领域语义（F08） |
| readiness / 看板 | runtime publisher → query DTO → JS | account、run/session、source time、valid_until、reason | 只读状态、业务 gate、交易授权分离；默认 UNKNOWN（F02–F05） |

行情、成交最终外部事实源与本地持久化凭证不能互相替代：交易所事实是对账依据，本地数据库是已持久接纳事实与命令的记录；Dashboard 是读模型，不能反过来成为交易授权来源。

### 4.2 不应回退的现有设计

- `orders/quantization.py` 用 Decimal、tick/step 和显式舍入，发送前格式化精确字符串；不要为了研究代码统一而改成 float。
- `orders/state_machine.py` 提交超时后查询 client ID，保留 UNKNOWN_PENDING_RECONCILIATION；没有证实盲目重复 POST。取消失败也先核实交易所状态。
- `order_repository.prepare_submission` 在事务内校验 lease owner/id/code generation、halt 和唯一提交身份；`append_order_event` 有状态与 executed quantity 单调保护。
- 最新本地提交已将晚到 fills 接纳与过期 snapshot 拒绝解耦，成交与 cursor 有原子路径；这部分在生产仍未部署。
- 官方 candle feed 丢弃未关闭 K 线、验证窗口长度、去重并提供显式恢复；异常最终窗口队列有上限。
- 连接池按 execution/account/market/observability/checkpoint/dashboard 分用途配置超时；collector 与账户事件主队列已有 maxsize 和背压行为。
- 主前端请求有 12 秒超时、in-flight 去重和端点变化检查；账户明细有 requestId/isConnected 防旧响应覆盖。保留原生 JS 模块及本地资源，无需为本次修复加新框架或 Python 依赖。
- credentials repr 隐藏 secret，部署脚本有 live 显式开关和 preflight/readiness 检查。本次未做全仓历史秘密扫描，不能据此保证所有脚本/历史提交无泄漏。

### 4.3 大模块及容量债务：按职责治理，不按行数裁切

抽样大模块为 `live_rollout/postgres_runtime.py` 2841 行、`strategy_runner/daemon.py` 2135 行、`execution_account/hub.py` 1966 行、`live_rollout/runtime_orchestrator.py` 1859 行。行数本身不构成缺陷。优先消除多处推导 readiness、研究端重建账本等语义重复，再按“事实装载/覆盖判定/持仓投影”“资源装配/运行编排”检查接口。

另一个明确容量缺口是 `execution_account/orders/coordinator.py:76-77` 的无界 PriorityQueue，以及按 key 常驻 scheduler worker。已有每 key 串行和优先级保护，应保留；进一步规定入口预算、队列最大等待、过载时拒绝入场及退出优先。本文未复现其在当前负载下发生堆积，因此列为容量治理项，不额外计入已确认事故风险条数。

## 5. 服务器只读核验

### 5.1 本次观测

| 项目 | 2026-09-24 23:00–23:04 UTC 附近采样 |
| --- | --- |
| Git / 镜像 | 均为 `86a89a9`；比本地少最新一项修复提交 |
| 容器 | 4 strategy、4 execution-account、market-data、research-collector、dashboard、PostgreSQL，共12个，全部 healthy |
| 主机 | 2 vCPU；内存3723 MiB，可用约1050 MiB；swap已用681 MiB |
| load | 一次 uptime 采样 1.32 / 1.75 / 2.11；不能当作持续负载结论 |
| 磁盘 | 59G，总用27G，可用31G，47% |
| vmstat | 后两次1秒采样 idle=81%/50%、iowait=0、si/so=0；首行是启动以来平均，不能作为瞬时证据 |
| market-data | 一次 docker stats 约32% CPU、333 MiB/640 MiB；仅瞬时值 |
| PostgreSQL | 库大小1822 MB；51 idle、1 active连接（包含本次查询），其余后台状态5 |
| revision | `20260918_0039`，与本地 Alembic head 一致 |
| 表型/索引 | runtime_market_states_15s 与 strategy_runtime_events 均为分区表；生产有 F12 中两个空库缺失索引 |
| `/api/health` | app_status=UP、database_status=UP |
| `/api/readiness` | 404；这是旧生产版本未具备接口，不是本地 F03 的500复现 |
| `/api/overview` | market/account/strategy 数据新鲜，active_halt_count=0；live-rollout状态LIVE |
| `/api/live-accounts` | 四账户READY/ready_readonly，strategy_state=null，有有效期内lease；不能仅据此断言每个entry gate开启 |
| 近期日志抽样 | primary strategy/account及market-data各取最近30分钟、最多1200行，解析JSON后指定warning/error/degraded/gap类别未命中；非JSON和截断范围不在结论内，不能解释为全系统无异常 |

服务器检查只读取 Git、容器状态、资源、白名单 API 字段和有限日志。SQL 使用 `BEGIN READ ONLY` 与 `statement_timeout=5000`，没有导出成交或全表数据，没有读取 env 文件内容。

### 5.2 修正旧报告的性能因果判断

此前 `docs/architecture/four-dimension-review-20260924.md` 把 checkpoint 写入数百秒直接解读为“云盘吞吐病态”并列 P0。本次看到类似 write=273.728/287.087/481.160 秒，但还查到 `checkpoint_timeout=900s`、`checkpoint_completion_target=0.9`。

PostgreSQL 会将 checkpoint 写入摊开到目标时间，因此总 write duration 不等于连续磁盘忙碌时间，不能用“写了多少 MB / 该时长”估算磁盘最大吞吐，也不能单凭该字段下磁盘故障结论。[PostgreSQL 16 官方配置说明](https://www.postgresql.org/docs/16/runtime-config-wal.html#GUC-CHECKPOINT-COMPLETION-TARGET)

这并不证明磁盘健康。应联合 iostat await/util、云盘 throttling、WAL/事务提交延迟、checkpoint write/sync、队列年龄和交易路径 p95/p99，进行足够时长的相关采样。当前几秒 vmstat、少量 swap 占用和12个进程，也不足以证明持续“主机超卖”。旧报告的相关 P0 定级应改成待验证假设，而非直接据此升配或增加并发。

旧报告中“过期 snapshot 丢弃 fills”“hub 可用性计时”和“账本窗口”部分已被本地 `e79fbce4` 修改，不能原样当成本地现存缺陷；但生产仍运行旧版本，部署差异必须显式跟踪。最新提交新增 readiness 的问题也应先修复再评估发布，不能因为名称是 fix 就视为可直接上线。

## 6. 验证记录与局限

| 验证 | 结果 | 范围与解释 |
| --- | --- | --- |
| `pytest tests/unit tests/smoke -m 'not live' -q --tb=short` | 1586 passed、4 skipped、1 deselected | 4项因 `CML_RUN_HUB_NETWORK_TESTS` 未开启；1项live排除；不是1586覆盖全部行为 |
| 设置 `CML_RUN_HUB_NETWORK_TESTS=1` 后两组hub unit及全部 `tests/e2e` | 64 passed | 已覆盖前述4项跳过；有部分与首轮重叠，不相加作唯一测试数 |
| `pytest tests/integration/persistence/parquet` | 2 passed | 与后续全integration重叠 |
| 独立临时库 `alembic upgrade head` | 成功到 `20260918_0039` | 空库升级，未证明生产大表锁时长或回填容量 |
| 独立临时库 `pytest tests/integration -m 'not live'` | 72 passed | 实际本地PostgreSQL事务、仓储和接口；非生产数据库 |
| 同一隔离库 `alembic check` | **失败，exit 255** | F12真实schema差异及部分表达式/命名噪声，未自动生成或应用迁移 |
| `pytest local_optimization/tests -q --tb=short` | 238 passed | 现有测试未阻止F07/F08反例 |
| `node --test tests/frontend/*.test.mjs` | **33 passed、1 failed** | `dashboard-modules.test.mjs:845` 期待“闭桶水位400ms”，渲染为“闭桶交付400ms · 完整100%”；F14记录其语义问题 |
| 归档修订 / readiness / 本地对账最小复现 | 确认F01/F02/F03/F07/F08 | 无外部交易调用；使用临时文件或假依赖 |
| 真实0037 downgrade最小分区表实验 | 预期复现失败，事务回滚 | 确认F13，不曾在生产降级 |

Python测试有 Starlette/httpx 弃用警告，WebSocket E2E另有 ConnectionClosed.code 弃用警告；不是测试失败。本次未自动添加新依赖。额外子审查定向29/30项测试与主套件重叠，不重复计数。

测试数据库通过本地 Docker 中现有 PostgreSQL新建，三个 URL 均显式指向 `127.0.0.1:54329/cml_review_20260925`；不是将生产导入测试。审查结束清理该新建数据库。未运行 live 标记测试、真实交易写入、生产故障注入、大规模压测、全量迁移历史回退、备份恢复演习或长周期 soak test；这些均为未执行，不能写成通过。

定向复现方法足够小，可转为回归测试：F02 将 OverviewQueries 的 health/live_accounts/overview 依赖替换为上述假输入，调用真实 readiness；F03同输入设置 active_halt_count=1；F01使用真实 journal和sink顺序写入同键不同close；F08直接调用 pair_round_trip_trades 输入两笔 SHORT 成交。建议把这些反例加入对应正式测试，而不是仅把本文作为证明。

## 7. 整改顺序与完成标准

1. **优先封住误导和破坏边界：**修F03枚举异常；将F02/F04/F05/F14的缺失/过期/未知状态显式呈现；F09加入测试库身份保护。完成标准为故障场景不能显示“可交易/完整100%”，错误测试URL在写入前拒绝。
2. **闭合事实与完成凭证：**修F01版本物化与F06分页覆盖，统一 snapshot freshness、fill cursor、materialized version 和 business readiness 的关系。完成标准为每个完成状态都有可核查事实，并能在中断后恢复。
3. **修正研究对账：**F07 Decimal及坏数据状态、F08方向与身份。对既有历史报告标明算法版本及受影响范围；重算前保留旧结果和来源，不覆盖后失去对比依据。
4. **补齐部署可复现性：**F12梳理索引、模型及迁移差异；F13明示回退边界。以空库、现有分区库、历史记录三类起点验证；生产发布前核对代码、镜像、migration、策略配置指纹及readiness。
5. **按负载治理资源：**F10/F11和订单scheduler容量先设测量方案，再实现有界扫描、缓存和任务预算。验收同时看延迟、内存、恢复和取消，不能只看吞吐。

重要设计决策中，“行情版本和物化凭证”“业务readiness权威来源”“持仓身份及研究复用边界”“分区迁移兼容”适合长期ADR；具体枚举修复、短期缓存参数和单个渲染文案用测试/变更说明即可，不需要机械新增ADR。每个暂存兼容路径写明原因、适用数据版本和移除条件。

这份报告记录发现及建议，不代表上述缺陷已修复。后续每项应以对应反例测试、迁移/恢复证据和部署版本完成闭环。

## 8. 修复提交复核（2026-09-25）

复核对象：`e79fbce4dad6d732a76fbfb056c28ac6786d5556..9d81b0c5bff6e118a471e3700ad18e35132c163f`。HEAD 提交说明称处理了 F01–F14；本次按当前代码、测试和隔离 PostgreSQL 复核，结论是**没有全部改完**。这里不对修复者身份作推断。

| 编号 | 复核状态 | 复核结论 |
| --- | --- | --- |
| F01 | 部分完成 | 增加了内容版本键和落盘 receipt 核验；同源同优先级的冲突仍保留旧行，incoming receipt 因找不到对应版本而可能一直 pending。digest 又排除了 `source_kind` 等来源字段，来源修订不能由版本键区分。 |
| F02 | 未完成 | 加入账户 freshness、lease、strategy 检查；但 readiness 只把 `FRESH` 服务映射成 READY，overview 又总包含 `database=READY`，所以 `streams_all_ready` 会被数据库项恒定挡住，正常状态也不能进入 `FULLY_TRADEABLE`。 |
| F03 | 部分完成 | active halt 的无效枚举引用已修；健康检查或数据库调用异常仍可能从 readiness 冒泡，而不是全部映射为结构化降级状态。 |
| F04 | 部分完成 | LIVE 路径增加了依赖加载检查和主动轮询，但相关视图仍缺少完整的数据年龄与来源状态表达。 |
| F05 | 未完成 | `/api/readiness` 关闭了 stale-while-revalidate；overview、账户及风险读接口仍可在 grace 窗口内交付旧缓存，响应没有明确的 `STALE` 标记。 |
| F06 | 未完成 | 成交分页和 `SYNCING/catching_up` 已实现；daemon 仍只接受 `READY_READONLY`，超页预算时会关闭事件接收并请求 pipeline recovery。Dashboard 也把非 READY_READONLY 映射成 HALTED、关闭 exit gate。 |
| F07/F08 | 当前提交未包含 | `local_optimization/` 被 `.gitignore` 整体排除。工作区文件虽有 Decimal 和方向处理改动，但 `reconciliation.to_decimal("not-a-number")` 实测仍返回 0；LONG 与 SHORT 在 FIFO 配对时按环境/账户/币种合组，没有把 `position_side` 纳入分组。最小复现将 SHORT 平仓配到了 LONG 入场，输出方向为 BUY。 |
| F09 | 部分完成 | 远端数据库和常见生产库名会被拒绝；本地测试端口 `54329` 会放行任意库名（包括 `cml`）。集成测试同步 URL fixture 也没有同等 guard，虽然当前只读迁移检查使用它。 |
| F10 | 部分完成 | 全目录扫描已移出高频路径并定期刷新；增量字节只记录 journal 写入，Parquet 新落盘在下一次全量扫描前未计入。容量读数因此会短时低估。单测退出还出现一次 materializer worker pending-task 诊断。 |
| F11 | 大体完成 | 缓存条目、后台刷新任务和锁增加了上限及清理；后台刷新异常会记入日志。仍应确认 stale 响应如何向操作人员标识（见 F05）。 |
| F12 | 未完成 | 空库 `alembic upgrade head` 成功，但对该空库执行 `alembic check` 仍以 exit 255 失败：检测到表达式索引定义差异、未由模型描述的账户/对账索引，以及唯一约束命名差异。迁移可执行不代表模型与 schema 契约已对齐。 |
| F13 | 部分完成 | 分区表 downgrade 会明确拒绝；非分区表回退仍可能因重复 `event_id` 无法重建唯一主键。 |
| F14 | 完成 | 前端移除了无依据的“完整100%”表述；前端测试 34 项通过。 |

复核还发现订单调度器的新边界问题：`cancel_order()` 对非 `reduce_only` 计划使用 entry priority，因此 entry 队列达到容量门槛时，取消仍打开的普通入场单也会被拒绝；队列等待超时只在 worker 取出命令后检查，前序 backend 操作挂起时，后续调用方仍会无限等待。两项都应加入拥塞/取消测试。

### 8.1 本次验证

| 验证 | 结果 | 解释 |
| --- | --- | --- |
| `pytest tests/unit tests/smoke -m 'not live' -q --tb=short` | 1608 passed、4 skipped、1 deselected | 4 项需要本地 loopback socket 权限，live 测试显式排除；首轮有一个行情序列测试短暂失败，单测重跑和随后全量重跑均通过。通过不覆盖上述未测边界。 |
| `pytest tests/integration -m 'not live' -q --tb=short` | 72 passed | 使用本次新建的本地隔离 PostgreSQL 库，非生产库。 |
| 隔离库 `alembic upgrade head` | 成功到 `20260925_0040` | 空库迁移路径可执行。 |
| 隔离库 `alembic check` | **失败，exit 255** | 存在上表所述的索引/约束 metadata drift。 |
| `pytest local_optimization/tests -q --tb=short` | 241 passed | 目录被 Git 忽略，结果只说明当前工作区文件；这些实现和测试不在上述修复提交里。 |
| `node --test tests/frontend/*.test.mjs` | 34 passed | 验证当前 dashboard 静态资源测试。 |

单元测试全量退出时另打印 `Task was destroyed but it is pending!`，指向 `ResearchStateCollector._materializer_worker()`。测试退出码为 0，但该异步清理诊断说明关闭路径仍应单独验证取消、等待和状态保存。

本次没有修改应用代码、没有连接真实交易账户或下单。服务器没有在本次复核中重新读取；本文上一轮服务器采样记录的 SHA 仍为 `86a89a9...`，不能据此确认服务器已部署 `9d81b0c`。因此，代码修复和线上部署状态需分开确认。

## 9. 最新提交复核（HEAD `a67c21a`）

复核时 `HEAD` 与 `origin/main` 均为 `a67c21a`，应用代码没有未提交改动；本次把复核结果追加到本文后，工作区只剩本文的文档改动。此前 `24d305b` 曾把整个 `local_optimization/` 纳入版本控制；最新 `a67c21a` 又将该目录撤出并加入 `.gitignore`。用户随后明确 `local_optimization` 可保持本地、不要求纳入版本控制。因此该目录未被 Git 跟踪是预期工作方式，不作为缺陷或验收失败；F07/F08 仍按本机实际代码与测试结果审查。

本次再次确认已修正的部分：F02 readiness 排除了 database 服务项并检查运行流状态；F03 对 readiness 查询故障返回降级结果；F05 API 增加 `X-Cache-Status: STALE`；F08 的本机对账函数按 `position_side` 分组；F09 两套数据库测试 fixture 均检查 URL；F12 模型索引与约束同步后，空库 `alembic check` 返回 `No new upgrade operations detected`；F13 对分区表和重复 `event_id` 的 downgrade 都增加了拒绝条件；F14 前端不再显示无依据的完整百分比。

仍未闭环的事项：

- **F01：**同优先级冲突版本被记为 `dropped_version_keys`，但 `WindowMaterializer` 把没有 accepted key 的 receipt 放入 `_empty_receipts`，flush 后仍提交并删除 journal。这样序列不再卡住，但没有持久的 `rejected/superseded` 结果，checkpoint 会前进而 Parquet 保留旧版本；审计端无法从持久状态区分该修订被拒绝还是已物化。
- **F04：**`RiskExecutionQueries` 每次查询都用 `datetime.now(UTC)`、`source_status="LIVE"`、`data_age_seconds=0.0` 填充元数据，没有从订单、风控决策和 halt 记录计算实际来源时间；仪表盘的“数据年龄”因此可能把旧记录标成实时。
- **F05：**`X-Cache-Status` 已区分 STALE，但静态 dashboard 代码没有读取或展示该响应头，操作人员页面仍看不到旧缓存状态。
- **F06：**分页超预算状态现作为可用快照让事件流水继续运行；但其后 `_publish_heartbeat()` 调用 `publish_user_data_heartbeat()`，该函数无条件把持久状态写为 `READY_READONLY`。补采尚未完成时，心跳仍可能覆盖 `SYNCING`。
- **F07：**本机目录里的非法 Decimal 字符串现在会报错，但缺失金额/数量仍在调用处默认成零；Round-trip 结果随后转成 `float`，`match_per_symbol_trades()` 继续用浮点数计算滑点与 PnL 差值。
- **F10：**容量统计现同时计入 journal 和 Parquet 写入；`scan()` 持锁重置增量计数，`record_written_bytes()` 不取同一把锁，扫描和写入并发时仍可能丢失一次增量，直至后续扫描校正。

本机目录里另外出现了 DuckDB/Polars 读取路径；其 fallback 用宽泛的 `except Exception: pass` 静默切换路径，也没有在这轮改动中提供目标负载、前后延迟或内存基准。该路径是否纳入版本控制不属于验收条件；若继续依赖这项性能改动，应界定可接受的 fallback 异常并补充性能证据。

### 9.1 最新状态验证

| 验证 | 结果 | 范围 |
| --- | --- | --- |
| `pytest tests/unit tests/smoke -m 'not live' -q --tb=short` | 1614 passed、4 skipped、1 deselected | 当前主项目 HEAD；4 项需要 loopback socket 权限，live 测试排除。 |
| `node --test tests/frontend/*.test.mjs` | 34 passed | 当前主项目静态 dashboard 资源。 |
| `pytest tests/integration -m 'not live' -q --tb=short` | 72 passed | 本地临时 PostgreSQL 库，执行后已删除。 |
| 空库 `alembic upgrade head` + `alembic check` | 到 `20260925_0040`；`No new upgrade operations detected` | 迁移和模型在本次隔离空库上对齐；不替代生产大表迁移演练。 |
| `pytest local_optimization/tests -q --tb=short` | 251 passed | 验证本机 `local_optimization` 工作树；该目录按用户要求保持本地，不以 clone/checkout 可复现性作为验收条件。 |

本轮对该节对应提交后的补充复核见下节。

## 10. 最新提交复核（HEAD `5cc8d6f`）

复核时 `HEAD` 与 `origin/main` 均为 `5cc8d6f342e1f9c9af2923968dc1c779e2f8b7dc`，开始审查时代码工作区干净。用户已明确 `local_optimization/` 保持本地即可；本节照常评估该目录的实现，但不要求 Git 跟踪它。

上一轮的主要未闭环项中，F05 的 API `X-Cache-Status: STALE` 现在被 dashboard 读取，分区状态和全局 readiness 都会把 stale 缓存计为不确定；F06 的 account daemon 会把 `SYNCING` 或 `fills_catching_up` 状态传给 heartbeat 持久化，sync service 也不再无条件覆盖成 `READY_READONLY`。F10 的写入计数与容量扫描现使用同一把锁，旧的“扫描期间写入增量可能被重置丢失”问题已修复。F01 为已拒绝/空批次增加了 `resolutions.jsonl` 持久记录；真实 journal/materializer 单测能区分已物化与被拒绝版本。

仍不能认定所有事项都已改完：

- **F01 [P2，部分完成]：**拒绝结果现在可持久审计，但 `commit_materialization()` 先从内存索引删除记录并 unlink journal，随后才 append+fsync resolution，最后更新 manifest。若在删除与 resolution 落盘之间崩溃，Parquet 仍保留旧版本、journal 已消失、拒绝原因也未落盘；如果在 resolution 与 manifest 间崩溃，恢复逻辑也没有据 resolution 重建 checkpoint。需要以 receipt 身份幂等写入决议，并让恢复流程能安全完成/重放该提交顺序。
- **F04 [P1，未完全修复]：**现在能给出数据年龄，但把行情、决策、halt、订单更新时间取最大值作为整体 `observed_at`。我用真实 `RiskExecutionQueries` 加隔离 mock 复现：行情时间落后 240 秒，同时有刚更新的 `filled` 订单时，接口返回 `READY / LIVE / age=0`。新订单活动会掩盖陈旧行情或陈旧决策。应按权威来源分别计算 freshness，并由关键输入的最差状态决定整体 readiness，或明确哪些来源不参与决策门禁。
- **F07 [P2，仍未完成]：**无效非空 Decimal 已报错，配对的核心运算也使用 Decimal；但成交对账入口仍用 `float` 解析 fill 价格/数量并把缺失值当零，费用与 realized PnL 的缺失字段也默认零。Round-trip 输出先将价格、数量、手续费和 PnL 舍入后转成 `float`，下游再转回 Decimal，原精度已不可恢复。这些是本机目录中的实际实现问题，与是否纳入 Git 无关。
- **F10 [P2，部分完成]：**共享锁修复了增量丢失，但扫描期间新增文件可能同时进入目录扫描基数和增量计数，导致重复计数。我用 350 字节文件在扫描回调中创建并记录，实际目录为 350 字节而 `CapacityGuard.scan()` 报 700 字节。这个方向是保守的，但接近阈值时可能提前把 collector 降级/暂停；应建立一致快照边界，或明确并验证可接受的保守偏差。
- **F06 [验证缺口]：**状态传递的实现已接线，但新增测试主要验证 sync service 自身保留 `SYNCING`，没有直接验证 daemon 在补采状态下实际向 service 传递 `state=SYNCING`。另外 `_publish_heartbeat()` 用宽泛 `except TypeError` 兼容旧签名，会把被调用方法内部的 TypeError 也误判为签名不兼容并再次调用；建议更新所有实现后移除此回退，至少增加 daemon 级状态转换测试。

F08 的已知 `LONG/SHORT` 持仓方向分组合并入对账逻辑，针对性测试通过；`position_side` 缺失时仍按 BUY/SELL 和未来成交做启发式配对，结果应标为不确定而不是静默视为确定身份。F02/F03/F09/F12/F13/F14 的既有修复未被本提交触碰，沿用上一节的复核结论；F11 仍是大体完成，缓存键与任务清理行为没有在本轮重新压力验证。

### 10.1 验证结果

| 验证 | 结果 | 范围 |
| --- | --- | --- |
| `pytest tests/unit tests/smoke -m 'not live' -q --tb=short` | 1616 passed、4 skipped、1 deselected | 当前 `5cc8d6f` 工作树；4 项需要 loopback socket 权限，live 测试排除。 |
| `node --test tests/frontend/*.test.mjs` | 34 passed | 当前 dashboard 静态资源。 |
| `pytest local_optimization/tests -q --tb=short` | 251 passed | 本机本地目录；不把 Git 跟踪作为要求。 |
| `pytest tests/integration/persistence/parquet/test_writer.py tests/integration/raw_files/test_journal.py -q --tb=short` | 4 passed | 当前 journal/Parquet 文件持久化测试。 |
| 全量 PostgreSQL integration 与 Alembic 检查 | 本轮未重跑 | 上一轮 `a67c21a` 的隔离库验证通过；本提交未改 ORM 模型或迁移。 |

本轮只读登录服务器并查看容器列表：实盘相关容器仍使用镜像 `crypto-momentum-lab-app:86a89a911f3ae9b3385d1d7deced1c7b8beb261e`，明显早于本地 `5cc8d6f`。所以本地代码修复不能视为已部署；本轮未读取环境变量、账户明细或密钥，没有调用写 API，也没有下单。服务器部署状态是独立未完成项。
