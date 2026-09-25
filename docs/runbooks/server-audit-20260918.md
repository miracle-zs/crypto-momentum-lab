# 服务器性能、架构与缺陷审计（2026-09-18）

## 范围与结论

- 服务器：43.167.191.253；采样时间约北京时间 00:24–00:29。
- 方式：SSH 只读检查、只读数据库查询及 EXPLAIN ANALYZE、本地代码审阅和隔离复现。未更改服务器配置、重启服务或执行交易。
- 现网主服务及本地 HEAD：`230d4d4311d558e60432321364a11edd3343efb4`。dashboard 使用独立旧镜像 `72051f1`，配置本身允许独立发布，未将其认定为 bug。
- **优先修复共享 Hub 客户端的不可用计时错误，其次处理采集器的突发消费能力，再优化账户最新状态查询。当前没有资源耗尽证据。**
- 本次是定向审计，不是全部交易逻辑的正确性证明；未进行生产压测或故障注入。

## 当前运行情况

| 项目 | 实测 |
|---|---|
| 宿主机 | 2 vCPU，3723 MiB 内存 |
| CPU | 两个后续 vmstat 样本 idle 75–78%，iowait 3% |
| 内存 | available 1557 MiB；swap 已用约 272 MiB，后续两个样本 si/so 均为 0 |
| 磁盘 | 59 GiB，已用 38%，剩余约 36 GiB |
| 容器 | 12 个运行中且 healthy |
| 重启 | research-collector RestartCount=3；其他当前容器为 0 |
| 数据库 | 932 MB；47 个 idle 连接；采样时没有业务查询等待锁 |
| 行情 | 样本队列丢弃为 0，连接均 ready；最新样本 event loop lag 2 ms |

实盘和行情容器在审计前约 25 分钟刚重建。当前低内存占用不能证明之前的内存增长已彻底解决，也不应据此立即下调限制。`OOMKilled=false` 仅是当前容器状态，不代表历史从未 OOM。旧 journal 在 23 点仍有内存及行情延迟告警，属于本次发布前证据。

## P1：正常运行时间被误算为 Hub 不可用时间（已复现）

位置：`src/crypto_momentum_lab/market_data/hub.py:690–798`，`WebSocketMarketStateSource._iterate_batches`。

握手成功时设置 `unavailable_since`，之后成功接收、交付 batch 不更新该值。第一次断线、序列错误或队列溢出时，代码将当前时刻减去握手时间，与 120 秒不可用预算比较。因此运行超过 120 秒的健康连接，一次瞬时异常即可直接抛出 `MarketStateHubError`，绕过重连。

生产对应证据：

- 16:01:01 UTC 开始出现 collector queue overflow，16:01:06 报 consumer lagged / sequence gap。
- 16:06:01、16:16:01 再次出现 overflow，随后退出；容器累计重启 3 次。
- 三次 traceback 最终均为 `MarketStateHubError: market-state hub unavailable for 120.0 seconds`。

本地隔离复现使用真实 `_iterate_batches`，只替换网络连接、输入队列和单调时钟：

1. t=0 握手并收到 batch 1。
2. t=121 正常收到 batch 2。
3. 紧接着注入第一次瞬时 OSError。
4. 实际：立刻报已不可用 120 秒，连接尝试次数仍为 1。

这证明计时缺陷；并不单独证明所有生产重启只有这一个原因。共享客户端还服务实盘行情消费，因此应优先修复，但本次未观察到现版本实盘因该原因退出。

建议：显式记录连续故障区间，在开始不可用时开启计时，达到明确的可用条件时清除；握手成功但持续缺序列不能无限重置预算。增加“长期健康后首次短断线”“持续连接失败”“握手成功但回放持续失败”三类回归场景。

验证：现有 `tests/unit/market_data/test_hub.py` 为 20 passed、1 skipped（测试主动要求 loopback 权限开关）；现有测试通过并未覆盖上述长期健康场景。

## P1：采集器复用低延迟队列策略，突发回放导致溢出和反复恢复

位置：

- `market_data/hub.py:37`：客户端接收队列固定容量 **2 个 batch**。
- `market_data/hub.py:840` 起：满队列被清空并替换成 overflow 标记。
- `research_collector/service.py:361` 起：取一个 batch 后串行等待完整 ingest，再取下一个。
- `research_collector/service.py:261–283`：ingest 包含 spool 写入、Parquet sink append、flush、checkpoint。
- `apps/research_collector/main.py:321` 起：collector 使用相同 Source，虽开启 preserve_sequence_on_overflow，但不会阻止队列溢出。

约 35 分钟日志中计数：5 次客户端 overflow、29 次 market-state gap、475 次 state conflict kept existing。第一批 durable 日志在两次启动后分别约 578 秒和 296 秒出现；collector 按 15 分钟窗口落盘，因此这些 durable 等待时间不能直接当作处理耗时。

**已确认**突发消费溢出和重启；**待量化**每个 ingest 阶段的耗时及 backlog。不能仅凭日志将瓶颈归为磁盘或网络。

建议：为归档消费者提供独立的有界可靠消费策略，先持久化 spool，再由单写者批量合并窗口；保留明确的容量上限、恢复游标和回放缺口处理。实时策略消费者继续保留低延迟及缺口保护。避免只把队列调得很大，以内存增长掩盖吞吐问题。

475 次冲突是现有保留旧值规则的日志，不能据此认定数据已损坏。建议按窗口汇总计数并保留有限差异样本，用于判断来源优先级是否符合回测数据语义。

## P2：账户最新状态查询全表排序（现网执行计划已验证）

位置：`persistence/postgres/account_repository.py:398`，调用方 `apps/market_data/main.py:252`。

为发现仍有持仓的账户，使用 `row_number() over(partition by account_label order by observed_at desc, reconciliation_id desc)`。

现网只读 EXPLAIN ANALYZE：

- 顺序扫描并排序 34,256 行；窗口计算最终得到 4 个账户。
- 排序内存 4678 kB；shared hit=953。
- 本次执行 **58.249 ms**。
- 累计 18,728 次调用，均值 59.91 ms，总执行时间约 1122 秒。
- pg_stat_statements 自 2026-09-14 13:23:55 UTC 累计，不能将这些累计数字归于最近一次发布。

现有索引 `(environment, account_label, status, observed_at)` 没有直接覆盖该过滤及排序组合。建议先在副本验证匹配排序的 ready 部分索引，再评估账户维度的最新状态投影或按完整账户注册表进行索引 top-1 查询；用实际执行计划决定，不承诺未经测量的收益。

重构必须保留原语义：停止服务但最后有效状态仍有持仓的账户也需要行情保护，不能仅枚举当前运行的 4 个容器。

## P2：checkpoint 延迟有优化空间，尚未证明是连接池容量不足

最近约 20 分钟，每账户 40 个日志样本：

| 账户 | total 中位数 ms | total P95 ms | 获取连接中位数 ms |
|---|---:|---:|---:|
| primary | 83.80 | 235.136 | 51.91 |
| account-2 | 91.45 | 233.477 | 55.58 |
| account-3 | 68.18 | 176.845 | 42.32 |
| account-4 | 60.91 | 210.385 | 34.43 |

P95 为排序样本的经验分位数。日志 `pool_acquire_ms` 包含 `await session.connection()` 的完整时间；连接池 pre-ping、建连和事件循环调度均可能参与，不能直接等同于排队等连接。

建议在现有分阶段计时上增加 checkout 等待、连接新建、事件循环延迟指标，检查多账户周期工作是否集中触发。若证实同相突发，可错开非关键 checkpoint / reporting 周期；保留交易提交前持久化屏障。当前无需凭猜测扩大连接池或调大 work_mem。

## P2：架构演进优先围绕恢复和数据职责边界

当前已有行情 Hub、账户 Hub、持久化屏障及按角色划分的连接池，不建议推倒重写。

1. **分开实时消费与可靠归档策略**：这是本次有生产故障支持的首要边界，统一协议解析，分别定义缓冲、超时、背压和恢复。
2. **将账户当前状态与历史审计分开**：历史对账记录继续追加，当前状态投影事务性维护；用于账户发现和保护标的，减少热路径反复扫描历史。
3. **继续收窄运行时装配接口**：`postgres_runtime.py` 2672 行、`runtime_orchestrator.py` 1756 行、CLI 2561 行。优先把资源生命周期、恢复游标、账户上下文加载形成独立契约，并用恢复场景测试约束。行数只是维护压力指标，本身不是 bug。

实施顺序：共享超时 bug → 采集器突发回放测试与可靠消费 → 查询执行计划优化 → checkpoint 归因 → 其余结构拆分。

## 边界

没有修改或部署修复；未执行数据库 DDL、VACUUM、清理、配置调整及交易请求。完整恢复正确性、重启期间是否存在未补齐的数据缺口、长时间内存稳态仍需后续专项验证。本次没有依据将旧报告中的历史仓位、旧内存峰值或累计数据库临时文件量当作当前故障。
