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

### 执行回执的安全回收（需单独受控上线）

`archive_execution_receipts` 默认 dry-run，默认最小年龄 72 小时，每轮最多检查
50 个已退役流、每流最多归档 500 条，硬上限每批 1,000 条、默认运行预算 45 秒，
使用单连接维护池。这里只处理同一仓位
已经完成 epoch 切换的旧流：切换事务同时写入永久 `execution_retired_streams`
标记，旧 epoch 不能再次被采纳；回执被裁剪后，旧输入仍被拒绝。退役标记永不裁剪，
不根据“不是当前 epoch”猜测历史流已退役，历史无标记的流也不自动补标记。

活动流、无序号回执、真实成交身份、累计成交水位、恢复检查点与退出 outbox 都不删除。
归档采用同机 JSONL + zstd，数据文件和 manifest 原子落盘并 fsync、读回验证；
文件 IO 不持有实盘仓位锁。删除前再次以非等待锁核对退役标记、当前状态头和整批
内容，任何缺失、变化或错误都会保留数据库记录。归档成功但数据库提交失败时可能
留下未引用归档文件，安全重试即可，不自动删除这些文件。

`durable_policy_commits` 仍永久保留决策 ID、policy key、revision、时间和完整
`commit_digest`。该摘要已包含前后状态摘要、决策输入、依赖与退出命令，因此删除
两列重复的状态摘要不会丢失相同决策的重试或冲突检测。它不是按日期删去重身份，
也不宣称表大小能停止增长。删列不立即释放已分配磁盘空间，不为此在线重写表。

迁移 `20261007_0053` 不兼容旧应用且不能安全直接降级。部署脚本会拒绝在线
`--live` 切换，也拒绝不带 `--live`、但旧应用容器仍运行的迁移。先确认维护窗口、
保存可恢复备份，停妥应用消费者，再迁移并统一切换所有写入者；不能混跑旧写入者
后启动回收。部署脚本不自动安装本回收 timer；完成统一升级、dry-run 和一次
`--apply` 验证后，再显式安装 `cml-execution-receipt-retention.service/timer`。
timer 每小时 35 分附近执行一轮，复用部署锁和既有归档器资源限制；新封存流需
等待 72 小时，初次检查为零并不表示历史无标记回执被回收。

受控上线后可复用现有维护归档器的 **0.5 CPU / 512 MiB** 上限，只增加本任务的
归档挂载；同机归档不是异机备份。先演练并核对结果，再单独加 `--apply`：

```bash
docker compose --env-file .env.server -f compose.server.yaml --profile maintenance \
  run --rm --no-deps --entrypoint python \
  -v /var/lib/crypto-momentum-lab/table-archive/execution_receipts:/app/execution-receipts \
  market-revision-archiver -m crypto_momentum_lab.tools.archive_execution_receipts \
  --archive-root /app/execution-receipts --minimum-age-hours 72 --batch-size 500
```

`round_completed` 只表示一轮结束，不表示全部历史回执已回收。`busy`、`protected`、
`changed` 均未删除该批；`completed` 只表示该退役流已无有序号回执，无序号回执仍保留。

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

### 市场版本 payload 的 zstd 归档

`market_revision_refs` 的市场状态 payload 可单独移入 JSONL + zstd 归档；PostgreSQL
保留 revision 身份、canonical 标记、时间和哈希，历史回放通过归档读取。归档文件按
内容寻址，伴随 manifest，并在数据库更新指针前校验压缩文件哈希、解压和行数。默认只
演练；必须显式传 `--apply` 才会把 payload 替换为归档指针。payload 校验识别当前与历史 v1
hash 规则，不重写 revision ID 或 content hash；两种规则均不匹配时仍拒绝归档。生产 timer 每小时最多归档
20 批、每批最多 10,000 条，只把最近 1 天的 payload 留在 PostgreSQL；更老的 revision
仍可精确回放，但需从 zstd 文件读取。

Dashboard 以非 root 用户读取归档；归档器将分区目录设为 `0755`、数据和 manifest 文件设为
`0644`。这些归档只含公开行情状态，不含账户凭证；不要收紧这些权限，否则历史回放会因
`PermissionError` 失败。

待归档部分索引按 `(bucket_start, scope, revision_id)` 排序，批次发现先定位最老待处理
桶，再列出该 15 分钟窗口内的 scope；避免对整个历史积压分组排序。归档更新显式写入
SQL `NULL`，不把 JSON `null` 当作清空 payload；回滚检查以归档指针为准。

该表的 autovacuum 使用 1% + 5,000 条死行阈值，插入清理阈值为 2% + 5,000 条，
cost delay 为 5ms、cost limit 为 200。首次归档后可执行限速普通 vacuum，允许线上读写：

```sql
SET vacuum_cost_delay = '5ms';
SET vacuum_cost_limit = 200;
SET maintenance_work_mem = '32MB';
VACUUM (ANALYZE, TRUNCATE FALSE, PARALLEL 0) market_revision_refs;
```

`TRUNCATE FALSE` 避免尾部截断阶段申请强锁；观察 `pg_stat_progress_vacuum` 和死行数量
下降来验证回收，不以 `df` 立即下降作为普通 vacuum 成功标准。归档产生 MVCC 旧版本，
首次追赶期间物理文件可能暂时增长；应分别核对积压、清理结果和增长窗口。

部署带有归档读取器和数据库迁移的版本后，`cml-market-revision-archive.timer` 会自动
执行有界归档。首次追赶历史积压可能需要多轮；每轮之后核对归档数量和数据库增长告警，
不要通过增大批次上限绕过数据库负载约束。手动演练和执行仍可使用：

```bash
docker compose --profile maintenance run --rm market-revision-archiver
docker compose --profile maintenance run --rm market-revision-archiver \
  --retention-days 1 --max-chunks 20 --batch-size 10000 --apply
```

如需迁移回滚或把 payload 放回 PostgreSQL，可分批执行到输出
`restored_rows: 0`；归档文件不会被删除：

```bash
docker compose --profile maintenance run --rm market-revision-archiver \
  --restore --max-chunks 1000
```

归档位于 `/var/lib/crypto-momentum-lab/table-archive/market_revision_refs`，与数据库
在同一服务器和文件系统上；它是压缩冷数据，不是异机备份。备份/恢复时必须同时保留
该目录及其 manifest，不要手动删除
manifest 或 `.jsonl.zst`。可以逐次提高 `--max-chunks`，观察数据库写入延迟、磁盘 I/O
和归档增长后再决定是否加定时任务。payload 变为 NULL 后，普通 VACUUM 只能让 PostgreSQL
复用页面，不会立即把已分配文件空间还给操作系统；不要在实盘运行期间执行 `VACUUM FULL`。

### 加速追赶历史市场版本

`deploy/ops/catch_up_market_revisions.py` 在服务器上连续执行有界归档轮次，默认每轮最多
20 批、每批 10,000 条，轮次之间暂停 30 秒；复用 Compose 的 0.5 CPU / 512 MiB
归档器限制。默认只检查，不写入；启用执行时默认最多运行 6 小时、200 轮。

```bash
python3 deploy/ops/catch_up_market_revisions.py
python3 deploy/ops/catch_up_market_revisions.py --apply --max-rounds 1
```

每轮前检查所有当前已部署的核心服务、活动/待确认告警（允许现有数据库增长告警）、
监控新鲜度、磁盘余量、系统负载、内存/IO PSI、数据库锁等待和长期事务。死行达到
100,000 条时等待 autovacuum；最低磁盘余量为 8 GiB。检查失败或压力升高时暂停，
不重启服务、不提高资源上限、不清空告警。运行日志为 JSON，每轮包含条数、耗时和暂停原因。

确认一轮成功后可启动一次性后台任务：

```bash
systemd-run --unit=cml-market-revision-catchup \
  --property=Type=exec --property=Nice=10 --property=IOSchedulingClass=idle \
  --property=KillMode=process --property=TimeoutStopSec=60 \
  /usr/bin/python3 /opt/crypto-momentum-lab/deploy/ops/catch_up_market_revisions.py --apply
journalctl -u cml-market-revision-catchup.service -f
systemctl stop cml-market-revision-catchup.service
```

任务与定时归档/部署共用 Git 目录中的部署锁，每轮后释放；另有独占追赶锁防止重复启动。
停止时只终止本轮创建的归档容器，已提交归档保持可读。没有待归档数据时输出
`completed` 并退出；达到时间/轮次上限只表示本轮运行结束，可再次启动，不表示已追赶完成。
归档单轮失败则退出报错。小时归档 timer 保持原配置。

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

有新鲜磁盘容量观测时，短窗口超严重增长预算仍会告警，但仅在完整日窗口也超严重
预算、按近期数据库增速预计 7 天内耗尽可用空间，或磁盘达到严重水位时升级严重。
容量观测缺失或过期时保留原有保守升级行为。容量情景是假设近期增速持续且没有
回收的推算，未包含其他文件增长；磁盘水位告警独立生效。

### 决策证据热保留与冷重放

完整决策的大份策略状态使用带 SHA-256 的无损压缩；相同的前后状态只存一份，
仓库读取时还原。小状态和既有普通持仓摘要保持原格式。新格式无需修改交易准入、
策略状态或下单规则。定仓状态按 symbol 覆盖，退出/清除 symbol 时移除，不按任意
条数删除当前策略状态。

每日 `cml-archive-trim.timer` 归档并裁剪 `decision_traces`，热窗口至少 7 天，
不受其他诊断表 1 天窗口影响。当前 durable policy head 与未完成的 durable exit
逐行排除，停止账户的旧检查点也受保护，不阻塞其他历史行裁剪。保留原有计划/依赖版本围栏、归档
文件 SHA-256、行数与内容指纹校验；校验失败不删行。

决策 JSONL.zst 归档附带有序行情修订身份，完整 market state 仍在 trace payload
中，避免后续行情 revision purge 破坏冷重放。归档永久保留于
`/var/lib/crypto-momentum-lab/table-archive/decision_traces`；这里只约束热数据库，
不宣称总磁盘增长归零。冷归档容量仍由独立磁盘水位告警覆盖。

```bash
python3 deploy/ops/archive_and_trim.py --table decision_traces --dry-run
python -m crypto_momentum_lab.tools.reproduce_decision <decision-id> \
  --archive-manifest /path/to/decision_traces_<from>_<to>.manifest.json
```

冷重放只读本地压缩文件并验证文件 SHA-256，不要求把历史写回线上数据库。
既有普通持仓摘要仍返回 `SUMMARY_ONLY`，不会因归档宣称能够精确重放。
裁剪释放的是 PostgreSQL 可复用页，通常不会立即缩小文件；实盘期间不运行
`VACUUM FULL`。

## 参数与维护

### 行情状态的恢复保护与策略退役

行情状态正常保留 12 小时，按 6 小时分区回收。检查点和当前未平仓腿所需的
历史可以把边界向前限缩。不要把计划 `COMPLETED` 等同于已经删除历史：还要核对
`effective_cutoff`、依赖原因及实际删除分区数。

`market-data` 的 `CML_RETIRED_STRATEGY_RUN_IDS` 是操作员明确退役的完整 run ID
列表，不支持通配符，不按检查点年龄或心跳自动退役。服务器 Compose 的名单记录了
2026-10-07 已确认永久停用的 10 个旧 Paper 策略；它们的检查点保留用于审计，但不再
阻塞行情清理。恢复这些 run ID 运行前必须先移除退役声明，不能承诺已回收行情仍可续跑。
该名单不取消其他显式注册的恢复依赖，也不删除 Paper 历史持仓或订阅保护。

当前持仓保护从账户 reconciliation head 获取持仓腿，再按账户、symbol 和
position side 查本轮持仓的历史：最后一条零持仓记录之后的首条非零快照；没有零持仓
标记时保守保留该腿全部已知历史。已平仓腿不再用旧非零快照永久阻塞回收。
账户头未就绪、配置账户缺少头记录、当前腿缺少快照时中止清理，不推断为空仓。
本地保护先注册为 `market_data_operational_retention` 依赖，再生成并执行计划，
保证计划与执行边界一致。检查失败回执和 `operational_database_retention_failed`。

服务器 Compose 设置 shm_size 256m，避免维护时默认 64 MiB 共享内存不足；已限制并行维护。检查磁盘空间与 /dev/shm，不因 No space left on device 就认定数据盘满。

先减少应用写放大、无界扫描和重复检查点构建，再单变量试验 checkpointer/bgwriter/WAL 设置；记录实际版本和完整检查点周期。不要关闭 fsync/full_page_writes，也不要用扩大交易超时代替性能修复。历史数值只保留在 Git，不复制为新实例标准。
