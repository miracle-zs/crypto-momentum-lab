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


## 后续诊断与本地修复（尚未发布）

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

这些测试确认代码缺陷及修复行为，尚不能证明线上队列溢出、CPU 延迟或所有重复自愈均已消除。生产代码未更新；发布后应观察实际消费水位与 checkpoint 持续推进、自愈次数收敛、队列恢复及真实退出保护。清理 SQL/ORM 边界和 ExecutionBook 的内部拆分仍属于后续结构工作，未在这次故障修复中全部实施。
