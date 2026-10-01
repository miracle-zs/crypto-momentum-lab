# 服务器系统状态核查（2026-09-30）

## 结论与范围

北京时间 2026-09-30 10:14–10:17，对 `43.167.191.253` 进行只读核查。服务器 checkout 与常驻应用容器 image tag 均为 `a18d2624f5beb26c2f314b5405df097b577c0f1e`，对应本次[模块耦合审查](module-coupling-audit-20260930.md)。未逐文件校验容器镜像内容与 checkout 的一致性。

常驻容器均 healthy，HTTP 与数据库存活正常，但系统尚不能认定为完全健康：Dashboard readiness 为 DEGRADED / EXIT_ONLY，策略 checkpoint 约 35 分钟未更新；四个策略反复对同一持仓自愈，账户事件消费者出现队列溢出；主机 CPU 满载且内存余量有限。租约过期提示另有明确的监控原因归类错误。

检查使用 Docker 状态/资源采样、当前容器近 30 分钟日志、本机 Dashboard GET API，以及 PostgreSQL `BEGIN READ ONLY`、`SET LOCAL statement_timeout='5s'`、`ROLLBACK`。未部署、重启、改生产文件或数据，未续租、审批、发单或运行修复脚本。日志窗口有 `--tail 3000` 上限；下述事件数是采样记录数，不是独立事故数。未检查归档日志或做逐笔订单/成交对账。

## 基础设施

| 项目 | 采样结果 |
| --- | --- |
| CPU | 2 核；load average 7.19 / 6.82 / 6.36；两次 vmstat 瞬时 CPU idle 为 0% |
| 内存 | 总量 3723 MiB，available 394 MiB；swap 已用 479 MiB |
| 根盘 | 59 GiB，总使用率 76%，可用约 14 GiB |
| 常驻容器 | 四个 live strategy、四个 execution account、market-data、research collector、dashboard、postgres 均 healthy |
| 一次性容器 | bootstrap-universe、volume-init、migrate 均 Exited(0)，符合一次性任务状态 |
| 存活 API | `/api/health` 返回 app_status=UP、database_status=UP |

四个策略 Docker CPU 采样分别约 32%–41%；market-data 约 24%，Postgres 约 28%。该采样说明资源紧张，不能单凭相关性证明所有业务异常均由 CPU 引起。

当前容器累计 RestartCount：market-data=8，research collector=3，四个策略分别 1–2；其他常驻容器=0。当前 OOMKilled 均 false。RestartCount 不区分所有历史触发原因，不能据此认定全部为崩溃或内存溢出。

## 账户、持仓与退出

- 10:15:32 的数据库只读快照中，四个账户 reconciliation head 均 ready、各 1 个持仓、0 个 open order、mismatch_count=0。后续 10:16–10:17 采样仍全部 ready_readonly；primary 曾在 API 采样中短暂显示 syncing / DEGRADED，随后恢复 READY。
- `durable_decision_exits` 当前 PENDING=0。97 条记录全部为 SUPERSEDED：account-2=26、account-3=27、account-4=26、primary=18，disposition_reason 均为 position_already_flat。昨天报告中的长期 PENDING 现象已不再出现在该表，但这些状态不独立证明每笔退出实际成交或终态处理正确。
- `risk_halts` 当前 active=true 的 live 记录为 0。
- 四个 active trading lease 的 code_generation 均为本次提交，到期时间为北京时间 2026-10-09 02:49–02:50。不能把当前问题归因于租约到期。

## 当前异常与证据

### 1. 策略持久 checkpoint 停滞，Dashboard 仅声明退出可用

10:17:15 `/api/overview` 中 strategy-runner 为 STALE，最新 checkpoint 是 UTC 01:42:27（北京时间 09:42:27），age=2087.8 秒；四个 live run 的 checkpoint 分别停在 UTC 01:40:24–01:42:27。行情和账户采集仍 FRESH。

`/api/readiness` 返回 DEGRADED、EXIT_ONLY、entry_gate_open=false、reason=strategy_runner_not_ready、exit_gate_open=true、halt_active=false。live-rollout 同时显示 LIVE，但 heartbeat 也来自约 35 分钟前的 checkpoint。策略日志仍持续刷新标的缓存，因此不是进程完全停止。

这是读侧 readiness 声明，尚未验证实际下单路径是否使用同一 gate；不能把 API 的 entry_gate_open=false 直接当作交易所侧不会新增订单的证明。也未证明退出保护可以成功完成，应继续核查 checkpoint 写入与真实执行路径。

### 2. 自愈持续重复，未表现为一次修复后稳定收敛

近 30 分钟采样中，四个策略对 PHAROSUSDT 的 `unmanaged_position_auto_healed_success` 约 26/30/30/29 次，记录均为 new_facts=0。primary 最新示例 total_quantity=132。数据库四个 PHAROSUSDT LONG head 在采样时持续更新，revision 为 40–44。

上述证据说明系统反复进入自愈并重写 head，而非新增事实持续到达；它提示分类、Book 内存恢复或持仓管理衔接需要检查。尚不能仅凭这些日志断定具体根因。优先追踪 self-heal → reload → managed classification → 后续 read/observe，确认同一持仓修复后不再重复触发，且失败时不会提前标记成功。

### 3. 消费队列溢出与行情事件循环延迟

四个策略的 live-exit 消费者各出现一条 `account_event_hub_client_queue_overflow`，时间为北京时间 10:00:27–10:02:02；四个策略也各出现一次 `live_candidate_expired_before_execution`。近 30 分钟 market-data 样本有 38 条 `market_data_event_loop_lag`；10:16:28 lag_ms=974，超过日志配置的 critical_threshold_ms=500。

与此同时，行情最新健康日志的连接 active/ready 均正常、ack_mismatch_count=0。这说明连接存活与消费及时性是不同维度。当前证据不证明事件永久丢失；需核查溢出后的 replay/recovery 是否闭环。

### 4. Operational health 的原因与事实完整性指标失真

`/api/health/operational` 为 degraded，四个账户均提示 lease_expired_or_inactive；但只读查询证实 active lease 均未过期，而 `strategy_live_states` 在 live 环境没有记录，API strategy_state 均为 null。

本地对应提交 `operator_dashboard/overview_queries.py:277` 要求 strategy_state 为 active/running；当该条件失败但 reconciliation 正常时，`:308` 附近仍返回 lease_expired_or_inactive。因此该原因字符串错误地把策略状态缺失归为租约问题。同文件 readiness 判定又允许 strategy_state=None 且 strategy_name 非空，两个健康口径不一致。

该实现还用 account status 是否 READY 合成 fact_gaps_count=0/1（约 `:304`），不是读取具体事实缺口证据。primary 一次 syncing 被展示为 1_fact_gaps_detected，不足以证明真实账本事实缺失。应分别报告状态缺失、同步中、租约失效和已确认事实缺口。

### 5. 研究采集处于磁盘容量告警区

research collector 为 DEGRADED / capacity_state=warning，因为 disk_free_bytes 约 13.88 GiB，低于告警阈值 15 GiB，但仍高于暂停阈值 10 GiB。采集目录自身约 441 MiB，低于其 soft/hard 限额。当前 stale=false、pending_spool_files=0、parquet_gap_count=0，最新 parquet 写入距采样约 5 秒。

因此当前是整机剩余空间告警，尚非采集停机；需要定位磁盘占用与增长来源，再制定容量或保留策略。

## 处理顺序建议

1. 优先排查策略 checkpoint 停滞、反复自愈和队列溢出的共同调用链；验证持仓管理与退出保护能实际收敛。
2. 修正健康视图的原因分类及虚构 fact gap 指标，统一策略状态缺失时的判断，并区分进程存活、消费进度与交易许可。
3. 核查 CPU 热点与磁盘增长来源，按证据安排资源调整或保留策略；本次没有执行清理。
4. 再按模块审查的事务约束推进结构重构，避免用健康提示变化代替业务一致性验收。


## 后续诊断与代码修复

继续检查发现：`strategy_checkpoint_persisted` / `live_checkpoint_persisted` 日志持续出现，但表中 saved_at 保持启动时刻。Postgres 仓储 UPSERT 使用 `existing.saved_at <= excluded.saved_at`，旧回放的 saved_at 可能被忽略，因此“写入成功”日志不能独立证明持久进度推进。前文 checkpoint 停滞指数据库记录及业务进度，不等于 checkpoint writer 已停止运行。当前没有放宽时间比较或用当前时间覆盖旧进度以制造健康状态。

独立进程只读重建四账户 PHAROSUSDT LONG 当前 durable cut，均得到 quantity=132、1 个 active batch，并可生成 1 个 ManagedLivePosition。由此排除了“当前持久事实完全没有可生成的持仓批次”作为这些账户的统一解释；但不能据此证明运行中内存 Book 与该 cut 相同。

已用回归测试确认并修正两条调用链缺陷：

1. `LiveContextPrefetcher` 对 is_backfill=True 的历史状态也调用实时上下文 provider。后续 market loop 只对该状态预热，完全不使用所加载的账户上下文。修复后历史状态按原顺序预热，不访问或修复实时账户；实时状态仍保留预取、失败传播与取消清理。64 条历史状态后接 1 条实时状态的 daemon 测试只调用一次实时 provider、只提交实时决策。
2. `PostgresLiveContextProvider._with_execution_book` 用历史行情 bucket_end 读取 Book，却与当前账户 snapshot 比较。若当前持仓在该截点之后产生，正确的当前持仓会被误判为 unmanaged，触发自愈；修复后重载仍按同一旧 cut 读取，因而无法收敛。运行上下文的持仓分类与 drift 诊断现在读取当前 Book；`LiveDecisionFactSource` 的历史冻结读取保持原样。真实 AccountJournal / PositionBook / ExecutionBook 测试覆盖“成交晚于行情截点，当前账户已有仓位”的连续两次分类，确认无需修复，并确认历史决策仍读到零仓位。

另外修正健康视图：有效租约下策略状态缺失报告 strategy_state_unconfirmed，明确非 active 状态报告 strategy_not_active；syncing/READY 不再生成假的 1/0 个事实缺口，缺乏账本覆盖证据时报告 UNKNOWN / fact_integrity_unconfirmed。readiness 不再把租约提供的策略名称当作策略运行状态确认。

验证命令：

```bash
rtk proxy .venv/bin/python -m pytest -q   tests/unit/live_rollout/test_context_prefetch.py   tests/unit/live_rollout/test_daemon.py   tests/unit/live_rollout/test_postgres_runtime.py   tests/unit/live_rollout/test_decision_facts.py   tests/unit/execution/test_execution_book.py   tests/unit/operator_dashboard/test_overview_health.py   tests/unit/operator_dashboard/test_queries.py   tests/unit/operational/test_operational_read_model.py
# 242 passed in 2.38s
```

这些测试确认代码缺陷及修复行为，不能单独证明线上队列溢出、CPU 延迟或所有重复自愈均已消除。清理 SQL/ORM 边界和 ExecutionBook 的内部拆分仍属于后续结构工作，未在这次故障修复中全部实施。

### 首次发布后发现的消费中断

用户明确批准生产发布后，先发布 `ed7ec747a15c5fedfaae6758bcf1e65a9205669a`。第一次预检因账户 2、3 的瞬时 `syncing` 失败，脚本没有重启实盘服务；审批版本已刷新。降低控制操作并发到 1，从失败阶段恢复后，四账户全部通过严格预检，脚本完成镜像、租约和服务更新。审批风险配置与限额保持原值，租约原来的更晚到期时间未缩短。

容器和结构化就绪检查通过，但后续业务验收发现 `live_runtime_market_task_failed` / `session_run_failed`。异常为：退出决策已持久化，派发前当前 Book 的投影或就绪状态已变化，`LiveDecisionFactSource._dispatch_exit` 抛出 RuntimeError，导致行情消费任务退出。此前重启后的 healthy 和 `entry_enabled=true` 快照没有证明消费链持续工作。

补充修复 `aa776cd06b650c0bc1748216fd650c38a4ab6e2c`：当前 Book 与退出单绑定的投影、epoch 或就绪状态不匹配时，保持 durable outbox 为 PENDING，不派发、不确认，也不终止行情消费；由既有恢复循环重新校验。缺失派发处理器和无关 Book 数据损坏仍抛错，没有放宽交易校验。新增三个回归用例在修复前因同一 RuntimeError 失败，修复后验证持续消费的调用可以返回、策略状态正常推进，以及 Book 恢复后原待处理退出单可以派发。相关测试合计 **245 passed**。

同一观察窗口还出现 `stream epoch changed without a complete source-anchored fill scan` 的账户快照冲突；不能把该日志与上述 RuntimeError 的直接原因混为一谈。只读重建也发现部分 Book 尚未跟随账户平仓状态，需单独验收事实恢复，不能通过跳过 epoch 校验或把未知覆盖标成完整来解决。

### 已确认的真实退出成交

发布后的数据库成交与订单证据如下，均为 PHAROSUSDT 的系统 reduce-only SELL 订单，state=filled、quantity=executed_quantity=132。

| 账户 | 订单成交状态更新时间（北京时间） | 交易所订单 ID |
| --- | --- | --- |
| account-4 | 11:00:09 | 201133468 |
| account-3 | 11:09:45 | 201148019 |
| account-2 | 11:22:38 | 201177707 |
| primary | 11:22:47 | 201177962 |

后续只读查询确认四账户对账均为 ready、position_count=0、open_order_count=0、mismatch_count=0，账户成交表也各记录 SELL 合计 132。这确认采样时四账户原有持仓的实际退出均已发生，不能推广为未来所有退出路径或账本恢复均已正确。没有手工提交测试交易或修改持仓事实。

### 连续验收发现的成交结算等待

`aa776cd` 发布后，新进程首先完成了四账户 checkpoint 写入，但 primary / account-2 随后再次终止消费。11:25 左右连续采样的失败断言为 account-2 的 market watermark age=207.6s（要求小于 180s）。日志明确指出：`ExecutionBook applied order facts but reservation settlement requires recovery: Filled terminal lacks complete account trade facts; active reservation is retained for recovery`。此时交易所成交已经发生，领域层保留 reservation 等待账户真实成交事实是保护措施；消费进程不应因此终止，阻断所等待的事实到达。

补充修复 `259c8e0`：durable 退出单派发处理器报告结算等待或提交结果未知时，记录 deferred，保留待处理行及 reservation，继续消费，由既有恢复路径校验。Book 读取损坏、缺失退出处理器、取消任务仍保留原来的失败/取消语义。新增结算等待与未知响应的红绿回归，并验证 Book 损坏和任务取消没有被吞掉。连同执行协调器测试共 **290 passed in 2.78s**。该修复没有用订单累计量伪造完整账户成交事实，也没有直接清除 reservation。

### 最终发布与验收（北京时间 11:51 起）

最终运行版本为 `259c8e02896ef3d954f228cb5743afe5c7d473d5`，四策略于 11:43:55 启动。12 个常驻容器全部 healthy，重启计数为 0、未被 OOM 杀死。部署脚本另外增加严格有界重试：仅当预检唯一失败项为 account_ready、账户状态为 syncing 且其他检查全通过时，最多尝试三次；配置不匹配、未知失败和超时立即失败。部署 smoke 38 项通过，连同业务测试 **328 passed in 5.00s**，shell 语法检查通过。

11:51:37 的只读数据库采样确认四账户 checkpoint 年龄 7.4–35.4 秒，消费水位年龄 22.9–37.9 秒。启动至该时刻四策略均未出现重复自愈成功、队列溢出、候选过期、market task failed 或 session failed；各已持久化 19–22 次 checkpoint。账户 3 的退出派发仍因 Book 未就绪而 deferred，消费任务保持工作。CPU 后续三秒采样空闲 71–91%，一分钟负载 1.07，较初查明显下降；market-data 最近三分钟仍记录 1 次 event-loop lag，不能声称延迟完全消失。

交易与账本验收分开：四账户最新对账均 ready、持仓数/挂单数/不匹配数均为 0；PHAROSUSDT reservation 均 COMMITTED，未结算数量为 0。账户 3 仍有 **3 条 PENDING** durable 退出，其余账户无 PENDING。四账户仍持续出现“stream epoch changed without a complete source-anchored fill scan”，事实恢复不能判定为通过。

面板 API 健康为 UP，但 readiness 仍 DEGRADED、strategy_state_unconfirmed，事实完整性 UNKNOWN。四策略自身快照 entry_enabled=true、FULLY_TRADEABLE，面板的 EXIT_ONLY 是缺少状态证据时的读模型判断，**不能当作实盘入场已被关闭的证明**。策略状态发布与账户事实覆盖仍需修复，磁盘初查容量警告也未通过本次修复消除。

本次验收结论：发布、当前行情消费、checkpoint 持续性和原有持仓退出分别核验；全系统健康与账本一致性仍未全部通过。没有修改 SQL 持仓/成交事实，没有跳过 epoch 或事实完整性保护，没有手工发起测试交易。

11:53:04 第二次独立数据库采样：四账户 saved_at 与行情水位均较 11:51:37 严格前进，checkpoint 年龄 4.0–49.0 秒、行情水位年龄 19.5–64.5 秒，全部满足小于 180 秒的验收阈值。这是约 9 分钟运行观察中的两次持续性采样，不代表无限时长稳定性保证。

### 账户 3 三条待处理退出的定点恢复（北京时间 12:01:49）

用户要求处理剩余三条退出记录后，先建立只读验收断言：账户 3 PENDING 数量必须为 0；处理前该断言失败。三条记录都绑定同一 LONG 批次 `ep_PHAROSUSDT_20260930013716_3_b1`、退出量 132。当前进程 stream epoch 与该旧账本 head 的 epoch 不同，自动恢复无法取得匹配视图，持续 deferred。这次只恢复退出单终态，未解决 source-anchored fill scan 的覆盖冲突。

使用既有 Binance 查询客户端与 `AsyncPostgresDecisionUnitOfWork` 做定点恢复，脚本为 `deploy/ops/reconcile_account3_exits_20260930.py`（默认只读，必须显式 --apply 才写回，交易提交功能关闭）。写入前完成所有校验：交易所 V2 明确 LONG 数量为 0、PHAROS 无挂单、账户成交事实 SELL 共 132、reservation 未结算数量为 0、前两条无本地订单且确定性 client ID 在交易所不存在；最后一条的实际 client ID 对应订单 201148019，交易所再次确认 FILLED、executedQty=132。

| decision 后缀 | 交易所证据 | 写回状态 |
| --- | --- | --- |
| 4e49289bcf7bbced | cml_e83b7ae14d777d7a528b9643fc9ebfe6 不存在 | SUPERSEDED |
| 68de065b55de733c | cml_cc9a1e1096b8bd8ee69b9411085bc23b 不存在 | SUPERSEDED |
| 5ee41e9b75650f1c | cml_7333c2e2178ce394076e7785a42fae95，订单 201148019 已成交 | DISPATCHED（补记派发回执） |

前两条保留处置原因 `exchange_absence_reconciled_original_batch_closed_201148019`；最后一条补记 command 回执，dispatched_at 是回执补写时间，不是原始交易所提交时间。没有把实际成交的第三条误标为未派发或失效，没有删除记录、改写持仓/成交事实或直接执行 UPDATE SQL。处理后的独立只读验收断言通过：账户 3 PENDING=0，对账 ready，持仓/挂单/不匹配均为 0。服务未重启、未额外下单。

12:03 复验三条终态与交易所证据仍一致，PENDING 仍为 0；四账户 checkpoint 和行情水位继续前进，采样年龄均小于 180 秒。


## 2026-10-01 生产版本更新

二百批结构改动已在用户授权下发布，当前生产运行版本为 `658a628b`，包含真实运行装配缺参修复。12 个常驻容器健康，四账户 checkpoint 两次采样均前进；整体 readiness 仍 DEGRADED，事实覆盖与策略状态发布未闭环，全局 PENDING 退出为 89 条。以上为新采样结果，前文保留各阶段历史记录；详见[生产发布与验收](module-decoupling-release-20261001.md)。
