# PostgreSQL 容量、连接、保留期与延迟

核对日期：2026-10-04。本文整合历史容量记录中的有效操作原则，配置以 Compose 和运行数据库为准；历史删除量与参数试验不作为当前默认值。

## 连接与观测

执行、行情、观测使用独立有界连接池，默认连接同一 PostgreSQL。先查实例连接预算、长事务、等待、云盘延迟及实际 SQL，再调整池大小；增加连接不能修复全历史扫描或磁盘拥塞。

```sql
SELECT state, wait_event_type, count(*) FROM pg_stat_activity GROUP BY 1, 2;
SELECT pid, state, now() - xact_start AS transaction_age, wait_event_type, wait_event
FROM pg_stat_activity WHERE xact_start IS NOT NULL ORDER BY xact_start;
SELECT pg_size_pretty(pg_database_size(current_database()));
SHOW checkpoint_timeout;
SHOW checkpoint_completion_target;
SHOW max_wal_size;
```

部署启用 pg_stat_statements 和 I/O 计时。按当前 PostgreSQL 版本查看统计视图，用同负载窗口的增量比较连接获取、SQL、commit、检查点写入；各阶段 P95 不能相加。时间过滤使用稳定 cutoff 参数或适合索引的 now()，避免逐行 clock_timestamp() 导致不必要扫描；SELECT 返回当前时间不属于同一个问题。

### 当前全实盘连接预算

同一 PostgreSQL 实例上的“逻辑平面”是隔离池，不是隔离实例。按四个实盘账户全部运行的 Compose 配置计算，连接池上限为：四个策略进程各 `9`（执行 `4`、行情 `2`、遥测 `1`、检查点 `1`、心跳 `1`）共 `36`；四个账户同步进程各 `2` 共 `8`；行情 `3`；看板 `6`，合计 **53**。迁移与 bootstrap 只在部署窗口额外使用少量连接。

生产实例 `max_connections=100` 时，应用连接上限按 **80** 管理，至少预留 20 个连接给迁移、运维和故障诊断。增加账户、worker 或某个 pool 的 size/overflow 前，必须重新计算这张表并在低峰执行：

```sql
SHOW max_connections;
SELECT application_name, state, count(*)
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY 1, 2
ORDER BY 3 DESC, 1, 2;
```

所有应用连接会同时设置驱动 `command_timeout` 与 PostgreSQL 的
`statement_timeout`、`idle_in_transaction_session_timeout`。前者限制调用方等待，后者防止断开的调用继续消耗数据库；它们不是放宽慢查询的理由。超过预算应先减少并发或修复查询计划，不能直接提高 `max_connections`。

## 账户快照保留

当前账户服务默认高频快照保留 7 天，余额历史按 UTC 小时稀疏保留 370 天；每键最新记录保留。任务有批大小、行数和运行时间预算，独立于发单路径。真实 account_fill_events 不由快照保留任务删除。

```text
CML_ACCOUNT_SNAPSHOT_RETENTION_DAYS=7
CML_ACCOUNT_EQUITY_RETENTION_DAYS=370
CML_ACCOUNT_SNAPSHOT_RETENTION_INTERVAL_SECONDS=3600
CML_ACCOUNT_SNAPSHOT_RETENTION_BATCH_SIZE=250
CML_ACCOUNT_SNAPSHOT_RETENTION_MAX_ROWS_PER_TABLE=5000
CML_ACCOUNT_SNAPSHOT_RETENTION_MAX_RUNTIME_SECONDS=45
CML_ACCOUNT_HISTORICAL_FILL_RECONCILIATION_BATCH_SIZE=10
```

历史成交常规补查按游标分批，活动标的另行处理；增大批量前测四账户请求预算、心跳和 DB 等待。初始化仍有历史发现成本，尚未完全转为后台增量。

## 归档与裁剪

[archive_and_trim.py](../../deploy/ops/archive_and_trim.py) 对将删除的范围归档并核对数量；仍需核对活动批次、未知订单、结算和恢复消费者水位。保留期不等于“交易事实超过一天即可删”。预建未来分区范围也不等于保留期。

2026-09-25 旧 timer 曾删除 51356 条 account_position_snapshots 和 10105 条 exchange_order_events 后失败；随后观测为停止/禁用。该历史状态不证明 timer 此刻仍禁用。本轮未启用 timer 或删除数据。

先只读核对调度及 dry-run：

```bash
systemctl status cml-archive-trim.timer --no-pager
systemctl cat cml-archive-trim.service
python3 deploy/ops/archive_and_trim.py --dry-run --retention-days 7
```

实际 unit 当前写有 --retention-days 1，脚本默认 7；两者不同，不能按文档假定安全或直接恢复调度。真正删除前必须验证归档可读取、范围连续、消费者覆盖且有可恢复副本。

DELETE 后文件不立即缩小：普通 VACUUM 回收可复用页，VACUUM (ANALYZE) 更新统计；VACUUM FULL/重写需要维护窗口和强锁，不在交易繁忙时执行。删数据也不保证 RSS 下降，区分共享缓冲、文件缓存和匿名内存。

### 行情版本回收

`cml-market-revision-purge.timer` 每天 03:35（Asia/Shanghai）执行一次有
边界的回收。它只处理满足以下全部条件的 revision：

- 非 canonical；
- 发布时间至少早于 72 小时；
- 不被任何完整 `decision_traces` 或 `dataset_manifests` 引用。

每次最多删除 10,000 条、每批 500 条；删除前会再次检查实时引用。任务仅回收
逻辑行，普通 VACUUM 后空间可复用；若需将空间还给文件系统，仍须安排
`VACUUM FULL` 维护窗口。

```bash
systemctl status cml-market-revision-purge.timer --no-pager
systemctl status cml-market-revision-purge.service --no-pager
journalctl -u cml-market-revision-purge.service -n 100 --no-pager
```

### 持仓恢复历史回收

`cml-position-recovery-retention.timer` 每天 02:15（Asia/Shanghai）清理超过
72 小时、且已由保留检查点覆盖的恢复状态副本。每次最多处理 100,000 个旧检查点
和 500,000 条状态事件，每批最多删除 500 条。最新的每流检查点与执行头当前绑定的
检查点始终保留；成交、退出边界、冲突和完整性事实不会被这个任务删除。

状态事件清理只涉及 `facts_state`、`snapshot`、`coverage`、
`fill_load_provenance`。每流每类至少留一条旧事件；只有事件时间和来源修订号都已
被保留检查点覆盖的记录才会进入候选集。任务每次运行最多 20 分钟，锁等待最多 5
秒，候选扫描单条语句最多 5 分钟；失败会由运维监控告警。

72 小时以外的精确逐事件重建不再保证；当前持仓仍通过保留检查点恢复，永久成交与
边界事实仍在。删除只是把表页变成 PostgreSQL 可复用空间，不会立即缩小数据库文件。
不要为了把空间还给文件系统而在实盘运行时执行 `VACUUM FULL`。

```bash
docker exec crypto-momentum-lab-dashboard-1 \
  python -m crypto_momentum_lab.tools.prune_position_recovery_history --dry-run
systemctl status cml-position-recovery-retention.timer --no-pager
systemctl status cml-position-recovery-retention.service --no-pager
journalctl -u cml-position-recovery-retention.service -n 100 --no-pager
```

### 磁盘水位与数据库增速

运维监控每分钟检查 `/var/lib/docker` 所在文件系统，使用率达到 75% 告警、85% 严重
告警；PostgreSQL 数据库和四张大表每 5 分钟采样一次，基于至少 1 小时的窗口估算日
增长。数据库超过 512 MiB/天会告警、1 GiB/天严重告警；单表分别以 256 MiB/天和
512 MiB/天为告警、严重阈值。样本只写入监控服务自己的 JSON 状态文件，不写入业务
数据库。

## 参数与维护

服务器 Compose 设置 shm_size 256m，避免维护时默认 64 MiB 共享内存不足；已限制并行维护。检查磁盘空间与 /dev/shm，不因 No space left on device 就认定数据盘满。

先减少应用写放大、无界扫描和重复检查点构建，再单变量试验 checkpointer/bgwriter/WAL 设置；记录实际版本和完整检查点周期。不要关闭 fsync/full_page_writes，也不要用扩大交易超时代替性能修复。历史数值只保留在 Git，不复制为新实例标准。
