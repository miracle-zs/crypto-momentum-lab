# 架构实施复核与下一轮优化（2026-09-18 晚）

结论：**已有明显进展，但不能认定“全部改完”。账户发现性能已验证改善；持仓重建、上下文接口、通道修复已落地；归档和生命周期目前仍存在可复现的正确性缺口。下一步优先完成恢复与持久化保证，再优化吞吐，不宜继续扩大抽象层。**

## 1. 核查基线

- 检查时间：北京时间约 19:24–19:30。
- 本地与服务器 checkout：`b4c61a57b4a29f6d3ff9a2aed5d8c1b851fbface`。
- dashboard 镜像 `b4c61a5`；实盘与账户进程 `91d4429`；市场与采集器 `0128173`。后三个基线都包含本轮架构提交；不能直接拿 checkout HEAD 代替所有运行镜像版本。
- 主机 2 vCPU / 3723 MiB，available 978 MiB；swap 使用 299 MiB；load 0.63/0.58/0.73。没有当前资源耗尽证据，不能凭这一点证明长期无内存增长。
- 只读检查服务器、现有代码和日志；生产数据库查询设只读事务与 statement timeout；故障复现在本地临时目录与隔离 Adapter 上完成。
- 本次没有部署、重启、变更生产数据或修复业务代码。

## 2. 上一轮工作逐项状态

| 项目 | 证据 | 结论 |
|---|---|---|
| Market/Account/Quote/Risk-control 超时补漏 | `b8fc769` 覆盖原漏修通道并增加健康运行后断线测试；本次相关测试通过 | 已修复并部署，持续稳定性单独验收 |
| entry cache 取消与启动 flatten pending | `cc13b90` 已在运行镜像内；实盘最近 1 小时未见 warning/error | 局部修复已落地，不等于整个停机流程完成 |
| 账户发现覆盖索引 | `1844315`、迁移 0038 | 已落地，当前主要收益来自 head 投影 |
| 最新有效对账 head | `ec038e0`、迁移 0039；四账户投影与历史一致；查询执行 0.048 ms | 当前账户发现热点已闭环 |
| 纯 PositionBatchRebuilder | `903d6b5`，领域值和计算从 PostgreSQL 文件提取 | 已实施；历史样本对照和全部交易语义并非本次全量验证 |
| LiveContextReader / 显式失效原因 | `3be496b` 已接入；兼容 `getattr` / 老接口仍在 | 主接口已建立，兼容层清理未完成 |
| ArchiveJournal / WindowMaterializer | `0cbaeaa` 已接入，但 ingest 仍串行等待 stage/flush/checkpoint | 文件职责拆分已完成，流水线异步解耦未完成；另有新恢复 bug |
| RuntimeSession | `0128173` 已接入；ownership registry 未被生产装配调用 | 外层 session 已接入，统一 ownership 和有界清理尚未闭环 |
| 独立 checkpoint 调度 | 新调度与 max dirty age 已接入 | 提交时间仍冒充持久化时间，验收未闭环 |
| 数据库双读覆盖、崩溃矩阵、回退演练、24 h 观察 | 现有测试未覆盖下述复现；本次未发现足以确认完成的证据 | 不标记完成 |

原[架构设计](../architecture-evolution-20260918.md)中的“已建 module”与“接口保证已满足”需要分开评估。提交标题不能替代调用链和故障测试。

## 3. 当前最优先问题

### P1-A：归档回补记录的清理键不唯一（本地完整链路已复现）

位置：[journal.py:263](../../src/crypto_momentum_lab/research_collector/journal.py#L263)、`research_collector/source.py`。

`commit_materialization()` 用 `(source_kind, stream_id, sequence)` 选择要删除的 journal 文件。但 PostgreSQL 回补产生的每批都是 `(postgres_backfill, None, 0)`，不是唯一身份。

复现采用真实 ArchiveJournal + WindowMaterializer + ParquetWindowSink：接受位于两个 15 秒窗口的回补批次，只封存第一个窗口，再从磁盘恢复 journal。

```text
before pending 2
flushed rows 1 committed receipts 1 remaining journal 0 parquet files 1
```

第二批未写入 Parquet，其 journal 却已被第一批完成通知删除。正常进程可能继续从内存完成第二批；但在此间崩溃会失去这一份已接纳的恢复副本。**这是已复现的耐久性缺陷，不代表已证明生产历史数据丢失。**

修复契约：每次接纳有稳定唯一 record_id / 内容身份；完成通知只回收该记录，校验所有自然键已覆盖。record_id 不应把每次重试生成的 `accepted_at` 当作幂等身份的一部分。回补无 Hub sequence 时使用来源、范围/内容摘要等稳定身份。

同文件 `materialized_sequence` 当前使用 `max(committed)`，没有计算连续已覆盖前缀；恢复用 `max(pending)` 也未验证缺口。需要补“较大序列先落盘、较小序列仍 pending”“中间记录丢失”的测试后才能认可连续游标保证。本次对这两项做了代码核验，未将其单独宣称为生产事故。

### P1-B：空选择 receipt 导致重启恢复失败（已复现）

位置：[service.py:219](../../src/crypto_momentum_lab/research_collector/service.py#L219)、[service.py:709](../../src/crypto_momentum_lab/research_collector/service.py#L709)。

合法的空选择 receipt 以 `states=()` 存入 journal。初始化恢复对每个 record 调用原始市场批次校验，后者拒绝空 states。

复现：选择 BTC；接收同窗口 BTC batch 1 和未选中的 ETH batch 2；在窗口关闭前重新构建 collector 并 initialize：

```text
pending records before restart 2
CollectorStateConflict: collection batch must not be empty
```

修复契约：区分“网络收到的非法空批次”与“合法 durable skip receipt”；空 receipt 保留来源和游标校验，但不走非空 market state 校验。验收覆盖空 receipt 在 fsync 后、物化前的崩溃恢复。

### P1-C：采集器现网累计重启 21 次，原因已定位到持仓选择链路

当前采集器 RestartCount=21，最近启动 UTC 09:15:32（北京时间 17:15:32）；最近 1 小时没有继续出现该错误。

保留的 7 小时日志中 21 次 traceback 均以以下错误结束：

```text
RuntimeError: ready reconciliation is missing active position snapshots
selector.selection_at → _load_position_symbols
→ account_repository.load_active_position_symbols
```

这与原来 Hub 超时故障不同，也不是本地复现的 P1-A/P1-B 的生产证据。需要继续核对异常时的 ready run、position 快照覆盖以及快照合并/保留规则，才能确定数据缺失或语义不一致的根因。

架构方向：将“账户摘要存在”与“该摘要对应的持仓集合可用”形成显式状态；unknown/degraded 不等于空仓。采集器选择依赖短暂不可用时应有有界恢复，避免进程重启循环。不能简单捕获异常后返回空标的，这会取消应保留的持仓数据。

第一版 head 投影只解决账户标签发现，没有解决 symbol 集合的一致性；下一步值得推进带版本的 protected-symbol projection，但须证明完整快照及轻量 delta 语义，不能直接对每次增量做 replace-all。

### P1-D：RuntimeSession 的持久化阶段可以无限等待（已复现）

位置：[runtime_session.py:259](../../src/crypto_momentum_lab/live_rollout/runtime_session.py#L259)。

该阶段计算 `persist_timeout`，传给 final checkpoint callback；但没有用 timeout 包住整个阶段，后续 `transition_terminal_state()` 没有 deadline。若该调用阻塞，总停机预算无效。

隔离复现：总预算 0.05 秒，terminal callback 永不返回；外层观察 1.15 秒后仍为 persisting，资源尚未 close。代码会把预算下限扩到约 1 秒，复现等待已超过这个下限。

取消该 close 后再次调用 close，仍不会清理，因为 `_closed=True` 在流程开始时已经设置。

```text
shutdown after 1.15s: done=False budget=.05s state=persisting resources_closed=False
after retry close: resources_closed=False state=persisting
```

修复契约：持久化整个阶段受剩余总预算限制；close 用共享 completion task/future 让并发调用等待同一个完成结果；“关闭中”和“已关闭”分开。外部取消后必需清理仍按明确 owner/deadline 执行，不能仅用一个提前设置的布尔值禁止重试。

此外 `ResourceOwnershipRegistry` 目前只出现在定义、re-export 和测试中，尚未被 production orchestrator 使用；`request_stop` 设置的 event 没有参与 RuntimeSession.run 的等待。现有 SIGTERM 有 supervisor 的独立通路，不能因此声称现网 SIGTERM 全部失效，但新接口的合作停止契约尚未完成。

### P2-E：checkpoint 把提交成功当作持久化成功（已复现）

位置：[checkpoint_coordinator.py:171](../../src/crypto_momentum_lab/live_rollout/checkpoint_coordinator.py#L171)、`:190`、`:204`。

`writer.submit()` 仅入队，紧接着却清 dirty 并更新 `_last_persisted_monotonic`。真正 persist 在后台。

使用真实 CheckpointWriter，暂时阻塞其数据库 callback，记录一个触发 checkpoint 的 state：

```text
persisted_count=0 dirty=False save_final=True
```

因此 durable_age 不能代表最后成功落盘年龄，`save_final=True` 也不能单独证明 pending/in-flight 已 durable。writer.stop 另有 flush 路径，**本复现不证明整个停机必然丢 checkpoint**；但设计中的持久化年龄与显式最终保存保证尚未实现。

修复方向：writer 返回/发布已提交 token；区分 last_submitted 与 last_committed；final flush 等待目标 token，失败保留 pending 和准确状态。和 RuntimeSession deadline 一起验收。

## 4. 性能核查与可继续优化的位置

### 4.1 已确认改善：账户发现

现网迁移到 `20260918_0039`。head 查询读取 4 行、shared hit=1，Execution Time=0.048 ms。使用单条 SQL 同快照比较所有最新 ready 历史与 head，4 个账户、0 个差异。

这是当前样本的执行时间，不是长期 P95。不过已经验证热路径没有再扫描数万条历史。无需继续针对旧窗口查询盲目调参数。

补充防护：当前代码只有整个环境 heads 为空时才回退历史；部分账户 head 缺失会被静默忽略。现网当前无缺失；下一步加覆盖清单/投影就绪状态，针对部分缺失回退或 fail-degraded，配合停止账户用例。

### 4.2 归档：先可靠，再真正并发

`_consume_source_once()` 仍 `await ingest()`；ingest 仍等待 stage_record、flush_ready、save_checkpoint。`asyncio.to_thread()` 防止阻塞事件循环，但调用方仍等待它完成，所以新增两个类不等于两段流水线。

建议 P1-A/B 修好后再拆独立 materializer task：Ingress 只等待 durable accept；单写者物化；由已提交 receipt 通知回收。增加字节配额、journal oldest age 和等待时间；明确 journal 状态的单线程/消息所有权，避免把当前可变 dict 直接放进多个并发线程。

优化指标：accept P95、stage/flush 耗时、峰值 queue bytes、积压消退速度、窗口 rewrite bytes。不能只看“没有 overflow”。

### 4.3 checkpoint：错峰已存在，写入突发和尾延迟仍值得优化

最近 1 小时日志样本：

| 账户 | 样本数 | total 中位数 ms | total P95 ms | acquire 中位数 ms |
|---|---:|---:|---:|---:|
| primary | 242 | 140.41 | 331.397 | 50.82 |
| account-2 | 244 | 142.37 | 279.243 | 50.44 |
| account-3 | 254 | 118.82 | 234.677 | 45.77 |
| account-4 | 248 | 123.53 | 238.015 | 44.90 |

P95 为排序数组经验分位数；与上午不同负载，不能直接宣称回归。primary/account-2 未满足原提案的 250 ms 候选目标。

primary 的真实启动参数仍为 1000 states / 60 s / phase 0；另取约 10 分钟日志有 70 次写入，部分集中在分钟开始后数秒。需要标注 submit trigger（phase/state-count/recovery/final）和内容 token，区分回放引起的重复提交与必要写入，再合并非关键周期写入。不能减掉交易屏障、lease 或安全停机必须落盘的写入。

连接获取时间包含 pre-ping/事件循环调度/建连等，不等同于池排队。不能只依据 51 个 idle 连接就扩大连接池。先测 submit→writer 开始→checkout→SQL→commit，各阶段与恢复突发相关性。

### 4.4 Dashboard：指标可信度先于更快的图表

`operator_dashboard/telemetry_queries.py:80–108` 直接丢弃四类阶段中 >5000 ms 的 latency。仅凭耗时大不能判断其为脏数据；真实阻塞也可能被隐藏，P95/P99 因而更好看。

建议依据 trace/run/epoch 和已知“挂单等待”语义分类；保留 raw、有效样本、排除计数和原因。已确认的 resting-limit wait 可单独报告；不能以幅度阈值替代来源证明。

`PerformanceQueries` 的 checkpoint 部分读取跨账户最新 100 条，不按所选 window 过滤。它是 recent samples，不是完整 6h/24h 统计；应明确展示实际样本区间，或按账户/时间做轻量 rollup。保留已有 30 秒缓存，避免新增同内容的后台反复扫描。

一次 docker stats 中 dashboard CPU 为 34.59%（Docker 的单核口径），不构成持续瓶颈证据；优先测缓存 miss 请求耗时及 query/反序列化占比后再决定是否预聚合。

### 4.5 资源和数据库边界

实盘容器工作集约 208–217 MiB / 512 MiB，market-data 331 MiB / 640 MiB，PostgreSQL 679 MiB / 1280 MiB。短采样未看到锁等待。

本次对累计耗时最高的 150 个 statement 做约 49 秒差分，观察到的活跃 SQL 单次均值较低；该集合不覆盖所有新 query，不能当作全数据库无慢查询证明。旧累计榜的数百秒 COPY/窗口查询不能作为当前问题证据。

当前没有支持扩容、分库或引入消息中间件的充分依据。先把恢复正确性、归档流水线、checkpoint 突发和指标口径收敛。

## 5. 下一轮架构任务，按收益与风险排序

| 顺序 | 工作包 | 完成标准 |
|---|---|---|
| 1 | journal identity + 空 receipt + 连续游标 | 跨窗口回补、空选择、乱序完成、fsync/删除/检查点之间崩溃后内容与无故障基线一致 |
| 2 | 持仓选择的版本与不完整状态 | 重现 missing snapshots 的数据组合；明确恢复路径；不返回假空仓、不形成重启风暴 |
| 3 | 已提交 token + 有界 RuntimeSession.close | final flush 能证明目标 token durable；terminal callback 阻塞仍按总期限清理；并发 close 等待同一结果 |
| 4 | 真正的 ingress/materializer 流水线 | accept 后不等待物化；内存/磁盘都有上限；单写者避免竞态；压力下可恢复 |
| 5 | head 覆盖协议与 protected-symbol projection | 部分缺失不漏账户；完整快照与 delta 分开，零仓和停止账户语义一致 |
| 6 | ContextReader/RuntimeSession 兼容层清理 | 所有 production 调用跨已定义接口；没有只在测试中存在的 ownership 机制或无效 request_stop |
| 7 | checkpoint 触发归因、SLO rollup 与原始慢样本 | 同负载验证尾延迟下降；采样窗口透明；大延迟不被静默过滤 |

这些是上一轮架构的完成工作，不是再建一层抽象。每个工作包至少一个具体故障场景或运行指标作验收依据。

## 6. 验证范围

- 第一组：collector、runtime session、checkpoint、context、entry cache、市场/账户 Hub、账户 repository 单元测试：104 passed。
- 第二组：持仓重建、risk-control Hub、quote Hub、resource lifecycle、runtime supervisor：32 passed。
- 合计 **136 项定向测试通过**，loopback 网络测试显式启用。不是全量测试，未运行需测试数据库的全部集成测试。
- 额外隔离复现：回补 journal 误删、空 receipt 恢复、RuntimeSession 超期/取消后清理、checkpoint 未 durable 即 final-success，共四个场景均复现上述问题。通过现有测试不代表这些新场景已覆盖。
- 未做生产 fault injection、压力测试或历史数据完整性全量审计；不把潜在数据丢失写成已发生事故。

本报告是本地文件；现有 `.gitignore` 忽略新 `/docs/` 文件，尚未暂存。上次设计的方向仍有效，但当前必须按本报告的实际状态验收，不能仅据功能提交名称将整轮标为完成。
