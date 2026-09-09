# 健康检查、K 线缓存与数据库维护评估

日期：2026-09-09。服务器只读核查时间约 22:25–22:33 CST。

本轮完成本地代码修改和验证；线上仅执行有超时限制的只读查询，没有发布镜像、重启容器、执行 VACUUM/DELETE/ALTER 或改变监控服务。基准对照版本为 `b9d703e7251e444ed96ffa7ed6eb4da7c646bf88`。

## 已实现

### 采集器健康检查

- Collector 在 checkpoint 成功写入之后，将进度和已有容量检查结果原子写入 `研究目录/health/research.json`；使用运行配置的实际容量限制，不再由探针重新构造默认限制。
- Docker 探针改为 `python -S /app/src/crypto_momentum_lab/research_collector/health.py`，仅依赖 Python 标准库，读取一个小 JSON，不扫描研究目录、不连接数据库、不加载 SQLAlchemy/PyArrow 等应用依赖。
- 原有 `cml-research-collector health` 命令读取相同状态。启动时清除旧进程的状态；checkpoint 与容量结果均须在 120 秒内。缺失、损坏、未来时间、过期、容量暂停或停止状态均判定为不健康；warning 状态可继续运行。
- 容量暂停立即撤销就绪状态，恢复处理后重新发布。保留写入路径原有容量保护与检查频率，本轮不通过延长保护间隔换取速度。
- 仅在实际处理进度之后刷新状态，不添加一个独立定时心跳来掩盖采集循环卡住的问题。

### K 线缓存

- 从全局 `(symbol, candle_start)` 字典改为 `symbol -> candle_start -> candle`，读取与裁剪仅访问目标币种。
- 同一币种的补齐、读取、缺口验证与裁剪都在同一把锁内；不同币种仍可独立处理。
- 保留区间排序、历史回补、分页、重试、缺失 K 线拒绝返回和保留期限语义。

## 本地性能对照

基准在开发机运行，不代表线上端到端订单提速。旧代码从上述 commit 读取，新代码来自当前工作区。

| 测试 | 修改前 p50 | 修改后 p50 |
|---|---:|---:|
| 健康探针，目录内 2,000 个文件，5 次独立进程 | 590.611ms | 39.479ms |
| 缓存查询，1 币种 × 200 根 K 线，100 次 | 0.1313ms | 0.1195ms |
| 缓存查询，100 币种 × 200 根 K 线，100 次 | 3.0085ms | 0.1183ms |
| 缓存查询，500 币种 × 200 根 K 线，100 次 | 19.4743ms | 0.1184ms |

K 线测试通过 MockTransport 预热每个币种，固定时钟与完整区间，然后重复调用实际 `load_closed_candles()`；测量包含读取、完整性校验和裁剪，不包含网络请求。保留期限设为 3 天，以覆盖 200 根 15 分钟 K 线。健康测试使用真实 checkpoint 和就绪状态文件，对比旧 CLI 与新的标准库独立探针。

原始结果：[K 线基准](../../reports/performance-optimization-2026-09-09/candle-benchmark.json)、[健康检查基准](../../reports/performance-optimization-2026-09-09/health-benchmark.json)。

## 高频表维护评估

查询采用 `BEGIN READ ONLY`，单条 statement timeout 为 5 秒、lock timeout 为 1 秒。死元组、活元组及 reltuples 都是估计值，不能等同物理膨胀率。

| 表 | 总大小（含索引） | 死元组估计 | 当前默认 vacuum 触发估计 | 建议 |
|---|---:|---:|---:|---|
| account_balance_snapshots | 580 MB | 40,597 | 44,831 | 优先评估表级阈值，保留现有小批量清理 |
| account_position_snapshots | 288 MB | 12,884 | 54,610 | 先观察增长和清理周期 |
| universe_entries | 898 MB | 3,143 | 972,690 | 重点是历史保留，不是紧急 vacuum |
| strategy_runtime_events | 553 MB | 1,160 | 162,919 | 重点是历史保留，不是紧急 vacuum |
| paper_positions | 23 MB | 1,884 | 3,043 | 自动清理仍工作，不需全局调参 |
| exchange_orders | 约 6.4 MB | 889 | 1,067 | 小表，继续观测，不优先处理 |

默认阈值由 `50 + 0.2 × pg_class.reltuples` 得到；核查时上述表均无表级覆盖配置。没有发现超过一分钟的事务，也没有正在执行的 vacuum。这只是当时状态，不保证所有高峰都没有阻塞。

### 余额快照：适合小范围调优，但还有更具体的清理成本

现有代码已经按小时执行保留任务，每个事务最多 250 行、每表每轮最多 5,000 行、每轮最多 45 秒。默认保留近 7 天明细，较老余额压缩为每 UTC 小时一条，长期保留 370 天；各键最新快照受到保护。不要重新增加另一个重复清理任务。

可在低峰试验仅对余额表将 `autovacuum_vacuum_scale_factor` 从 0.2 降至 0.1，预计触发点从约 44,831 降至约 22,440。比较一到两天内 dead tuples 曲线、vacuum 耗时、IO PSI 与订单延迟，收益明确后再决定是否需要 0.05。当前 `maintenance_work_mem` 和 `autovacuum_work_mem` 都为 32MB，保持不变。该参数变更本轮仅评估，未执行。

对 primary 账户执行了现有“按小时压缩”候选选择的 **SELECT 等价查询**（没有执行 DELETE）：

- 返回 250 条候选，外层实际扫描 4,377 行，内层索引探测 4,377 次。
- 首次执行 2,252.683ms，shared read 2,841 块，读时间 2,162.558ms。
- 重复执行 41.394ms，shared hit 14,117，没有新增 shared read。
- 使用了现有账户时间索引与小时表达式索引，内层 Heap Fetches 为 0。因此没有证据要求立即添加一个重复索引；对这次查询而言，vacuum 也不能消除其重复探测成本。

这证明特定维护查询对缓存冷热敏感，而不是整个数据库一直缺缓存。后续若要进一步优化，优先评估按账户/小时保存清理进度，跳过已经压缩完成的历史前缀，并保留迟到数据重查、最新快照保护及事务超时。仅增大缓存不能解决每批从历史开头重新扫描的问题。

### Universe 与事件表：先明确研究依赖

两表主要是活数据体积。Universe 删除还涉及 `universe_entries` 和 `monitoring_memberships` 对 `universe_snapshots` 的级联外键，不能只根据文件大小删除。应先确定回放、归因和研究所需时间范围，再设计归档与小批量过期删除；本轮没有缩短数据保留期，也没有删除订单、成交或研究历史。

原始证据：[只读 SQL](../../reports/performance-optimization-2026-09-09/maintenance-readonly.sql)、[首次采样和计划](../../reports/performance-optimization-2026-09-09/maintenance-evidence.txt)、[重复采样和计划](../../reports/performance-optimization-2026-09-09/maintenance-repeat-evidence.txt)。

官方说明：[PostgreSQL 16 vacuum 阈值与表级设置](https://www.postgresql.org/docs/16/routine-vacuuming.html)。

## 保留峰值监控

线上核查确认 `cml-ops-monitor.service`、`sysstat.service`、`sysstat-collect.timer` 均 active；sysstat 文件持续写入 `/var/log/sysstat/sa09`，定时器每 10 分钟采样。本轮未停用、重启或修改这些服务。

现有 ops monitor 覆盖容器健康、市场数据日志和实盘 checkpoint 等状态；sysstat 保留主机历史。当前配置不能提供秒级 CPU/PSI/每容器尖峰的完整历史，不能把“保留现有监控”说成“已补齐峰值可观测性”。

发布后应对照观察 07:50–08:20、22:00–23:00 的 CPU/run queue、MemAvailable、swap-in/out、IO、市场队列及订单 p95/p99。必要时在这些窗口做短间隔、有限时长采样；不要只比较 load 或累计 swap 占用。现有告警通知目标和通知策略保持不变。

## 验证与发布边界

- 新增探针独立进程测试，确认 `python -S` 可运行；覆盖陈旧/损坏/缺失状态、暂停、恢复、停止和启动状态清除。
- K 线覆盖跨币种读取、裁剪后历史回补、排序、同币种并发 single-flight；原有缺口拒绝和重试测试保留。
- `pytest -q tests/unit/strategy_runner tests/unit/research_collector tests/smoke/test_server_deployment_manifest.py`：126 passed。相关模块通过 Ruff 与 Mypy 检查。
- 扩大到全部 `tests/unit/apps` 时，未改动的 market-data 测试 `test_run_market_data_keeps_consumer_alive_while_capture_stops` 因 fake runtime 不接受 `on_durable_state_persisted` 参数而挂起；中断前 183 个测试通过。因此不声称全仓测试通过。
- 新探针依赖新版本采集器写入状态文件，发布时必须让新镜像和新的 Compose healthcheck 一起生效，不能先单独替换探针。
- 当前为本地待发布改动；线上性能收益和新镜像启动状态仍须在实际发布后验证。
