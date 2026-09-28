# 43.167.191.253 运行异常诊断

检查时间：2026-09-28 10:36–10:40（Asia/Shanghai）。服务器与本地代码 HEAD 均为 `2e8c751`。

本次做了只读资源/日志/数据库检查与短时 py-spy 采样，未修改生产代码、配置、订单、仓位或重启服务。服务器 /tmp 下保留两份采样文件。

## 结论

已确认两个发生在策略事件循环上的 CPU 热点：整本 ExecutionBook 深拷贝，以及持仓视图的全量重算/重复事实哈希。它们是当前首要修复对象。机器 CPU 饱和、WebSocket 心跳超时、上下文读取超时与 EXIT_ONLY 反复切换同期发生；尚未部署修复，因此不宣称已通过干预验证整个因果链。

数据库连接回收异常也持续存在，具体泄漏分配点尚未锁定。看板全历史聚合查询是另一个已定位的负载来源。

## 现场证据

- 2 vCPU，内存总量 3.6 GiB；10:36 load 6.64/5.56/5.70，10:39 load 6.07/5.79/5.76。
- vmstat 连续两次 CPU idle=0%，iowait=0%；四个策略进程是主要 CPU 使用者。
- 磁盘使用 64%，剩余约 21 GiB；当时没有容器 OOMKilled，当前容器 RestartCount 均为 0。不能据此排除更早被替换容器的问题。
- 首页 HTTP 200，约 12 ms；/api/health 返回 app/database UP。/api/readiness 某一时点 READY，但后续日志仍出现异常，故是反复降级而非持续宕机。
- 最近 5 分钟四个策略日志均检测到 `non-checked-in connection`，以及连接/上下文异常或 EXIT_ONLY；最近 3 分钟复查仍存在。
- 02:33:06 UTC checkpoint：pool_acquire_ms=5233，SQL 执行 35 ms；02:35:07 pool_acquire_ms=5917，SQL 执行 24 ms。连接等待墙钟时间很长，不应直接等同于 SQL 本身很慢。
- PostgreSQL 抽样约 80–90 个 backend（含系统进程），max_connections=100；有 6–8 个 idle in transaction，部分持续 12–23 秒。没有证据证明当时已触及连接总上限。

## Bug 1：每个仓位证据复制整个账户账本

路径：`runtime_orchestrator.py:1476` → `orders/coordinator.py:558` → `ExecutionBook.observe:2481` → `_staged_copy:610`。

`src/crypto_momentum_lab/domain/execution/execution_book.py:613` 对整个 `_books` 和 `_journals` 做 `copy.deepcopy`，随后复制多个全局字典、集合和 reservation。`observe_account_snapshot` 按 position key 循环，每个 key 都会触发一次完整复制。复制在主事件循环线程同步执行，代价随全账户历史增长。

证据：`py-spy dump --pid 2211958` 抓到此栈；12 秒、25 Hz 采样中，主线程 299 个样本里 287 个（96.0%）含 `_staged_copy`，281 个（94.0%）含 deepcopy。这是采样窗口内的主线程栈占比，不是整机 CPU 百分比。

修复：实现按 PositionKey 的事务写时复制，只复制本次会变更的 journal/book 和相关索引；不可变事实可共享，含可变 raw_payload 的对象不能无条件共享。保留提交后发布、失败回滚、全局去重、跨 key reservation 等语义。不要简单改成全量浅拷贝，也不要仅移入线程池掩盖总计算量。

## Bug 2：持仓读取重复投影与哈希，外层缓存覆盖不足

- `domain/execution/execution_book.py:1498` 先 `book.get_view()`，常见返回分支在 1516 又执行 `book.get_view(cut=...)`。
- `domain/execution/position_book.py:86–89` 每次读取都 read_cut、project，再 compute_facts_hash。
- `domain/execution/position_ledger.py:1202` 在 project 内部已计算同一事实哈希。
- `domain/execution/position_ledger_models.py:646` 对全部 fills、snapshots 等递归转换、排序、JSON 序列化并哈希。
- `execution_book.py:1559` 的 list_position_views 遍历账户全部已建账本，包括历史 key。
- `live_rollout/postgres_runtime.py:589` 外层缓存仅按单个 bucket_end 与 unresolved_orders 命中；不同事件时间的调用会替换缓存，账户失效事件也会清除它。它没有消除底层重复计算。

证据：账户 2 和账户 3 的现场栈都落在上述路径。账户 2 的 10 秒、20 Hz 采样，主线程 198 个样本中 list_position_views 占 75.3%，compute_facts_hash 占 72.7%，_staged_copy 占 24.7%；这些包含关系的百分比不能相加。

修复：在 PositionBook 按 journal revision、cut、policy/schema version 缓存不可变投影，限制缓存容量；将依赖 now/requirement 的新鲜度判断留在每次读取阶段。复用一次算出的事实哈希。当前视图与历史 cut 分支避免无意义的双次投影。账户列表使用 revision-aware 增量失效并合并同 key/cut 并发加载；保留历史回放、迟到事实、已平仓历史 key 的正确语义。

## Bug 3：看板行情覆盖查询跨全部历史分区

`src/crypto_momentum_lab/operator_dashboard/risk_execution_queries.py:228` 用 `GROUP BY symbol, max(bucket_end)` 查询 runtime_market_states_15s，只有 environment/data_complete/symbol 条件，没有分区时间限制。

数据库日志同一查询单次耗时 2712、2945、3682、4731、4753 ms；客户端 172.18.0.5 已映射到 dashboard 容器。EXPLAIN 显示 Append 扫描多个日期分区，包含 9 月 20 日以来的旧分区。

修复：将最新完整行情维护成按 environment/symbol 唯一的 latest-state 表或复用可靠的当前状态源；短期可以先查近期分区、对缺失 symbol 单独回退历史查找，保留最后一次行情时间语义。根据 EXPLAIN 再设计支持按 symbol 取最新记录的索引。避免把任意时间截断直接当作无行情。

## 仍需单独定位：数据库连接未归还

四个策略在已包含 `90fcd38` 清理补丁的当前版本仍打印 SQLAlchemy `non-checked-in connection`。代码改过 sibling task 清理不代表问题已解决。

修复工作应加入可控超时/取消回归测试，覆盖 context 并行读取、外层取消、session rollback/close 与关闭过程中再次取消；检查 checkout/checkin 是否回到基线。需要定向连接分配/释放追踪才能确认具体遗失路径。不要将增大连接池当作根治；现有服务器总连接预算已接近 100。

## 已排除为当前首因的旧故障

- cml-archive-trim.service 失败记录来自 9 月 25 日，旧 `_scalar` 对空输出执行 splitlines()[0]，产生 IndexError。
- 当前 `deploy/ops/archive_and_trim.py:200` 已处理空输出；timer 当前 inactive/disabled。这是未恢复的运维状态，不能解释当前主线程高 CPU。验证归档保留和依赖策略后再恢复调度。
- 02:28 UTC 的 lease_code_generation_mismatch 是启动阶段错误，当前策略已运行且出现恢复日志，不能把旧错误当作持续唯一故障。

## 修复与验收顺序

1. 优先修复账本复制和投影/哈希两个热点，在本地用接近生产历史规模的数据测事件循环最大延迟。
2. 回归覆盖提交失败不污染已发布状态、同 key 并发、跨 key 隔离、revision/cut 缓存失效、迟到事实、历史回放、随时间过期的新鲜度判定。
3. 定向修复连接释放；优化看板最新行情查询。增大超时/连接池只能作为测量后有边界的临时措施。
4. 经验证后按单账户灰度部署，遵循已有租约/审批机制，保留平仓与账户同步能力；不能通过绕过风险门禁证明修复成功。
5. 观察覆盖多个 15 分钟 K 线边界、账户更新和 checkpoint 周期：无新的连接遗失告警，池占用回落，无内部流 ping timeout 导致反复 EXIT_ONLY，CPU 有余量，看板查询耗时明显下降。外部偶发断线与内部计算阻塞要区分。

## 可复查命令

```sh
vmstat 1 3
py-spy dump --pid <当前策略宿主机PID>
docker logs --since 5m crypto-momentum-lab-live-strategy-1 2>&1 | grep -E 'non-checked-in connection|live_runtime_context_degraded|mode=EXIT_ONLY'
```

本次已执行四账户日志断言：每账户上述错误计数任一大于零即 FAIL，四账户均 FAIL；再次在 3 分钟窗口复查仍有告警。此检测适合现场症状验收，属于滚动窗口，并非确定性的单元复现。

采样原件在服务器：`/tmp/cml-diagnosis-20260928-primary.txt`、`/tmp/cml-diagnosis-20260928-account2.txt`。


## 12:32–12:41 续查：连接与“行情未就绪”

服务器代码为 `0926080`，下述本地修复尚未上线。生产策略容器仍继续运行。

### 连接未归还：确认一个确定的代码错误

`live_rollout/postgres_runtime.py` 的 `_account_position_view()` 在 `async with self._sessions() as session` 退出后，又在旧 session 上执行 `AccountFillReconciliationCursorRow` 查询（原 1236 行）。触发条件是无实时账户快照且账户有活跃仓位，或已完成 ready 对账。SQLAlchemy 默认关闭 session 只重置它；之后执行查询会自动开启新事务并签出连接。新事务不再被原 `async with` 关闭。

有两层复现：

1. 本地回归测试把退出作用域的 session 标记为关闭；旧代码必现 `query used an AsyncSession after its context exited`，改为在新的受管 session 内查询后通过。
2. 在服务器策略镜像中用独立只读进程和独立大小为 1 的连接池运行 `SELECT 1`：退出 `async with` 后 checked_out=0；复用旧 session 查询后 checked_out=1；显式关闭后 checked_out=0。没有打印连接字符串。

本地已修复为给游标查询单独使用 `async with`。相关测试 105 项通过。生产主账户在 10 分钟窗口内有 741 条连接未归还告警，但此代码错误尚不能解释全部告警：主账户当时是否持续走无实时快照路径尚未证明；其他账户也有零星告警。不能把修复本地代码当作已修好全部连接问题。需要部署后对照四账户每分钟告警率、`pg_stat_activity` idle in transaction 和连接池 checkedout，再继续查取消时的清理路径。官方 SQLAlchemy 文档说明 session close 默认可复用、再次执行会自动开始事务。

### “行情未就绪”：看板原因标签错误，主账户另有消费落后

12:32 看板 `/api/readiness` 为 DEGRADED，`market_data_not_ready`；同一响应内 `market-data=READY`、`strategy-runner=RECOVERING`。`operator_dashboard/overview_queries.py` 原逻辑把任一 stream 不 READY 都写成 `market_data_not_ready`，所以标签错误。本地改为指出第一个未就绪服务名，并增加回归测试。此改动仅影响看板原因文本，不修改交易门禁。

12:39 数据库最新研究行情 bucket_end 为 04:39:45 UTC，而主账户策略 checkpoint 的最新处理 bucket 为 04:19:15 UTC，落后约 20 分钟。其他三个账户 checkpoint 均在 04:39 左右。主账户日志连续出现 `live_candidate_expired_before_execution`，候选时间较执行时间早约 3 分钟；可判定主账户处理旧市场状态，行情服务本身正在供数。主账户从 04:31 的最新状态 03:44:30 UTC 推进到 04:39 的 04:19:15 UTC，说明在追赶，而不是完全卡死。

12:40 看板 stream 均 READY，却显示 `account_readonly_mode`。与此同时各账户策略进程的本地 readiness 为 `EXIT_ONLY`，原因是各自的 `exit_failure:<symbol>:order_identity_conflict`；主账户为 ZECUSDT。看板全局状态与实际策略门禁并非同一数据源，也不能用看板的 READY 判定恢复交易。多个账户、多个币种报出场订单身份冲突，其原始异常在 `exit_processor.py` 捕获后归一成 `order_identity_conflict`，细节未记录；必须单独追踪原始异常与订单/意图 ID 后再修复，不能绕过此门禁。

### 后续验证

生产上线本地修复后，至少覆盖一个完整账户快照丢失/恢复周期，确认上述连接告警是否归零。若仍出现，按连接池 checkout/checkin 事件记录分配栈及 task 名称，定位剩余路径；同时对主账户测实际处理速率与待消费状态数，区分启动回放积压和持续吞吐不足。出场身份冲突需要记录被归类前的异常类型和去敏后的冲突标识，再与持久化订单状态核对。


### 04:22–04:36 UTC 时间关联补证

用 `docker logs --timestamps` 按分钟统计主账户连接告警：04:29–04:36 分别为 62、59、82、119、109、109、120、70 条；04:37 以后本次检查窗口未再出现。04:24:48 记录 `live_account_snapshot_recovery_requested reason=account_event_queue_overflow`，这会使实时账户快照失效并触发数据库回退路径；04:36:44 出现账户更新和上下文缓存失效记录。高频告警与快照恢复区间吻合，支持上述无快照分支是主账户连接遗失的主要来源。04:22 也有一次 89 条告警高峰，说明不能排除其他快照切换或独立遗漏。

主账户 04:40 的 checkpoint 最新处理 bucket 为 04:19:45 UTC，另三个账户在 04:39:30–04:39:45；行情表最新 bucket 为 04:39:45。主账户仍在回放旧行情，导致出场候选过期。它的本地 readiness 还明确显示 `exit_failure:ZECUSDT:order_identity_conflict`，而看板在所有 stream READY 时仅显示 `account_readonly_mode`。这两个看板原因均未准确传达真实门禁。

### 13:00–13:10 CST 后续修复与状态

`exit_processor.py` 又发现一个独立误分类：它将任意 `OrderPreSubmissionError` 和 `ReservationConflictError` 当作订单身份冲突，尽管这些异常还可能表示租约失效、风控停止或仓位视图过期。现已在本地收窄为真正的持久化订单身份错误消息，并在该错误发生时记录异常类型、文本与候选 ID。还需上线后获取原始异常，才能确定并修复造成多个账户出场失败的下层原因。

截至 05:10 UTC，行情表最新状态为 05:09:45，四账户 checkpoint 最新状态分别约为 05:09:00、05:09:15、05:08:30、05:06:45。主账户从先前落后 20 分钟缩小到约 3 分钟，但仍有候选过期与连接未归还告警；不能宣称恢复。服务器仍运行 `0926080`，本地修复没有部署。四个策略容器在本次检查时为 Docker healthy，该状态不能替代交易门禁检查。当前本地相关测试 114 项通过。
