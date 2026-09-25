# PostgreSQL 容量、保留与内存分析

日期：2026-09-14
主机：`43.167.191.253`（4 核 / 3723 MB，13 个容器）

## 起因

线上监控报 postgres 内存压力，实测：

```
cache_hit_ratio      58.04 %
load                 2.69 / 2.29 / 2.19
postgres 内存        593 MiB / 1024 MiB（贴顶）
swap                 556 MB
```

追查发现：**6 张表里没有任何一张有可用的 retention**。其中
`strategy_runtime_events`（65 万行/天，09-08 单日 325,415 条 candidate）是主要来源，
`universe_entries`、`paper_equity_snapshots`、`exchange_order_events` 也都在无限增长。

## 一、清理（5,901,628 行）

保留窗口最初定为 7 天，随后收窄到 **2 天**（依据见第三节）；第二次收窄归档删除
1,309,186 行（`universe_entries` 768,240 / `account_position_snapshots` 189,391 /
`account_balance_snapshots` 161,020 / `paper_equity_snapshots` 121,516 /
`exchange_order_events` 36,112 / `strategy_runtime_events` 32,907）。归档先于删除——这些表在 PostgreSQL 之外
没有第二份副本，retention 等同于真删。

```
                          删除行数      归档产物
strategy_runtime_events    152,200      jsonl
universe_entries         4,499,695      csv
exchange_order_events      738,042      22.1 MiB  jsonl
paper_equity_snapshots     504,356      24.1 MiB  csv
account_balance_snapshots    7,053       0.3 MiB  jsonl
account_position_snapshots     282       0.0 MiB  jsonl
                        ───────────     ────────
                         5,901,628      260 MiB
```

工具：`deploy/ops/archive_table.py`（格式按列类型自动选择）、
`deploy/ops/archive_and_trim.py`（归档 → 校验 manifest 行数 → 分批 DELETE 5000 行 → VACUUM ANALYZE）。
两者都在服务器本地运行，SQL 不经过 ssh/shell 引号往返。

**结果**：

```
cache_hit_ratio   58.04 %  →  63.49 %
db_size           4638 MB  →  4308 MB
load              2.69     →  0.87
```

## 二、归档格式

| 表 | 格式 | 原因 |
|---|---|---|
| `universe_entries`、`paper_equity_snapshots`、`monitoring_memberships` | **CSV + zstd** | 全标量列 |
| `strategy_runtime_events`、`account_*_snapshots`、`exchange_order_events`、`live_strategy_signals` | **JSONL + zstd** | 含 jsonb，CSV 会把它压成转义字符串 |

`live_strategy_signals` 有 6 个 jsonb 列（`account_context`、`candidate_context`、
`features`、`filter_context`、`market_context`、`reference_prices`），必须用 JSONL。

**压缩比**：`universe_entries` 5,642,815 行 → 113.3 MiB（≈9:1）。
归档文件带同名 `.manifest.json`（行数、字节、sha256、时间范围、格式）。

**未做**：归档与数据库仍在**同一块磁盘**（`/dev/vda2`），因此它是"删除前的存证"
而非异地备份。若需容灾，需移出本机。

## 三、保留窗口的依据

前端（operator_dashboard）确实读这些表，但**只读近期**：

| 表 | dashboard 的需求 | 保留 |
|---|---|---|
| `strategy_runtime_events` | `decision_slo` 窗口最大 **7d** | **2 天（有意收窄）** |
| `universe_entries` | 按 `snapshot_id` 关联**当前快照** | 2 天 |
| `account_balance_snapshots` | 只查 `max(observed_at)`，**最新** | 2 天 |
| `account_position_snapshots` | **最新** | 2 天 |
| `live_strategy_signals` | **最新** | 2 天 |
| `paper_equity_snapshots` | `equity_range` 有 **30d / 1y** 选项 | **2 天（有意收窄）** |
| `exchange_order_events` | **前端不读**（仅 execution_account 写） | **2 天（有意收窄）** |

> `paper_equity_snapshots` 原先按 `_EQUITY_WINDOW`/370 天保留，正是为了撑住
> dashboard 的 1y 图。收窄到 **2 天**后，"30d / 1y"视图只会剩最近 7 天的点。
> `exchange_order_events` 收窄后，库内不再可查历史订单（归档文件里还有）。
>
> **缩短窗口不会降低 postgres 占的内存**——`shared_buffers` 256 MiB 是固定分配，
> 连接数也与行数无关（见第六节）。它提升的是命中率、降低的是 I/O 压力。

## 四、索引

`strategy_runtime_events` 曾挂 4 个索引。用 `EXPLAIN` 逐条核对哪条查询走哪个索引：

```
monitor 的 delay 查询          → ix_..._type_time (event_type, occurred_at)  ✅ 在用
monitor 的 consistency 查询    → ix_..._time      (occurred_at)              ✅ 在用
pk                             → scans = 260,914                            ✅
ix_..._run_time                → scans = 1，89 MB                            ❌ 无人使用
```

已执行（`CONCURRENTLY`，不阻塞写入，耗时 0.32 s）：

```sql
DROP INDEX CONCURRENTLY ix_strategy_runtime_events_run_time;
```

回收 89 MB（关系 901 → 812 MB）。**注意：命中率没有变化**——删一个从不被扫描的
索引省的是磁盘，不是缓存热度。

## 五、表内空洞（重要）

`VACUUM`（非 `FULL`）只把死元组标记为可复用，**不重写文件**，所以删完行数掉了 80%，
文件大小几乎不变。`pgstattuple` 实测：

```
                          文件      有效数据    空洞      空洞占比
universe_entries         560 MB     107 MB    443 MB     79.2 %
strategy_runtime_events  600 MB     278 MB    315 MB     52.5 %
exchange_order_events    189 MB     9.5 MB    178 MB     94.4 %  ⚠️
paper_equity_snapshots   138 MB      31 MB    105 MB     76.2 %
                        ───────    ───────    ───────
                        1487 MB     425 MB   1041 MB
```

`exchange_order_events` 7 天只写 4.8 万行，那 178 MB 空洞要很久才会被写入复用。

### 空洞对性能的实际影响（实测）

**索引扫描不受影响**（索引直接指向元组 TID），**顺序扫描和不 VACUUM 受严重影响**：

```
exchange_order_events  文件 189 MB / 有效数据 2.6 MB / 空洞 98.2%

Seq Scan 实测:  shared read=24,000 页 (189 MB)   I/O 63.9 ms   Execution 81.7 ms
```

**读了 24,000 页，而有效数据只需约 320 页——浪费 98.7% 的 I/O。**
PostgreSQL（16.14）的顺序扫描不会跳过空页，这与 InnoDB 不同。

（另：`VACUUM` 每天要扫完整个文件含空页，是一项持续付出的隐性成本。）

### 方案选择：`VACUUM FULL`，不做 `pg_repack`

`pg_repack` 的"在线不阻塞"优势在这台机器上被抵消了：**postgres 用的是官方镜像
`postgres:16-alpine`**（项目 Dockerfile 只给 app 用），要用它必须先写 postgres 的
Dockerfile、把 compose 从 `image:` 改成 `build:`、**重建 postgres 容器**——而重建
本身就是一次停机。既然都要停一次，不如用零安装、可重跑的 `VACUUM FULL`。

关键技巧是用**会话级** `maintenance_work_mem`（不重启、不影响其他连接）：

```bash
PGOPTIONS='-c maintenance_work_mem=256MB' psql -c "VACUUM (FULL, ANALYZE) <table>"
```

默认只有 32 MB，而 `universe_entries` 的索引有 484 MB——不提上去的话索引重建要
分成多个批次落盘排序，锁持有时间会显著变长。

### 执行结果（2026-09-14）

分两批做：低写入表先做，高写入表（`universe_entries` 由 market-data 写、
`strategy_runtime_events` 是实盘写入路径）确认后再做。

| 表 | 前 | 后 | 锁持有 |
|---|---|---|---|
| `exchange_order_events` | 336 MB | **4.4 MB** | 0.78 s |
| `paper_equity_snapshots` | 276 MB | **12 MB** | 1.00 s |
| `account_balance_snapshots` | 300 MB | **75 MB** | 4.71 s |
| `account_position_snapshots` | 375 MB | **63 MB** | 4.45 s |
| `universe_entries` | 1044 MB | **70 MB** | 7.81 s |
| `strategy_runtime_events` | 812 MB | **377 MB** | 15.18 s |
| **合计** | **3143 MB** | **601 MB** | **34 s** |

行数全部不变。`db_size` 4248 → **1714 MB**，磁盘 24 GB → 21 GB。

`strategy_runtime_events` 那 15 秒锁是全程最长的（它是实盘写入路径，写入量 31 万行），
事后查 `live-strategy` 日志**无错误输出**。其余五张的锁都在 8 秒以内。

## 六、内存构成（第一性原理）

**"数据库占的内存"是 452 MiB，不是 531 MiB。** 后者含可回收的 page cache。

```
shared_buffers        256 MiB   ← 配置的固定分配，与数据量无关
anon（59 个连接）     196 MiB   ← 每个连接一个后端进程
                     ─────────
真实占用              452 MiB

file（page cache）    733 MiB   ← 内核缓存；Docker 计入 MemUsage 但可随时回收
  inactive_file       343 MB       ← 标记为可回收
  active_file         464 MB
```

### 为什么删数据不会降低内存

**`shared_buffers = 256 MiB` 是启动时分配的固定值**——库里 100 行还是 1 亿行，它都是 256 MiB。
删掉 564 万行，这 256 MiB 一点不动。这正是"内存降不下来"的根本原因。

（`effective_cache_size = 1 GiB` 那个参数**不占内存**，它只是给查询规划器的提示。）

### 关于 page cache

实测 45 秒内 `file` 是 **1103 MB → 1099 MB**——**稳定，不是增长**：

```
file 大小 = min(读过的数据文件页总数, 容器可用内存上限)
```

它不是泄漏，是内核对空闲内存的再利用。**要"降下来"只能 `drop_caches`，而它会立刻涨回来**
（读操作在持续）。**这也不该降**——让内核不缓存文件，等于每次读盘都变成真实磁盘访问。

本次观测到的读盘来源：

```
heap 读        +47 页 / 45s        ← 几乎不读表
索引读        +269 页 / 40s
总读盘     +65,944 页 / 45s        ← 差额在临时文件
temp_files = 20,  temp_bytes = 1643 MB,  blk_read_time = 3,023,556 ms
```

临时文件的大户是 `COPY ... ORDER BY`（归档 `universe_entries`：115 万行 × 500 B ≈ 575 MB
排序溢出，`work_mem` 仅 16 MB）——**这是一次性操作，已结束**。持续的是 WAL（3240 MB）。

## 七、连接池：9 个连接的来源

```
59 个连接 = 53 客户端 + 6 后台进程
53 客户端中，live-strategy 的 4 个容器各占 9 个 = 36（68%）
```

**9 不是配置错误，是 5 个 engine 各自的池之和**（`runtime_orchestrator.py:352-424`）：

```
execution_engine      pool_size=4   max_overflow=0
market_engine         pool_size=2   max_overflow=0
observability_engine  pool_size=1   max_overflow=0
checkpoint_engine     pool_size=1   max_overflow=0
heartbeat_engine      pool_size=1   max_overflow=0
                     ─────────────
                      9 个连接
```

因为 `max_overflow` 全是 0，这 9 个是硬底，不会自己缩小。其余容器同样对得上号：
`execution-account-*` → 2（`_ACCOUNT_POOL_SIZE`）、`dashboard-1` → 2（`_DASHBOARD_POOL_SIZE`）、
`market-data-1` → 4（market 2 + obs 1 + ckpt 1）。

**可选的优化（本轮未做）**：

| 做法 | 省 | 风险 |
|---|---|---|
| 合并 5 个 engine 为 1–2 个 | ~48 连接 / ~100 MB | 需改架构；5 处池语义（timeout 各不同）要重设计 |
| 各池 `pool_size` 减半 | ~20 连接 / ~40 MB | `execution` 是**实盘下单路径**，变浅可能在并发下单时等 3 秒 |

**决定：先不改。** `execution` 那 4 个连接属于交易路径，这类改动应单独评估、单独回归，
不与运维数据清理混做。

## 八、查询写法：`clock_timestamp()` 陷阱（收益最大的一处）

### 症状

清理之后，命中率上去了，但系统仍有 **8.6 MB/s 的持续读盘**，`pg_stat_database`
的 `blk_read_time` 累计 **3,023,556 ms**（50 分钟），容器 page cache 停在 1.1 GB。

### 定位

用 `pg_stat_statements` 按"每返回一行要读多少页"排序，第一条是：

```
1,221 calls | shared_blks_read=62,724,193 | rows=1,359,191
            | pages_per_row=46.1 | hit%=4.2
```

一条 monitor 的查询，累计读盘 **496 GB**，命中率只有 **4.2%**。

### 根因

```sql
-- 有问题的写法
AND occurred_at >= clock_timestamp() - (300 * interval '1 second')
```

`now()` 是**事务开始时间**——在事务内是常量，规划器能把它折叠进索引范围条件。
`clock_timestamp()` 是**实时时钟**，**每一行求值都可能变**，规划器无法将其视为常量，
于是只能**全表扫描再逐行判断**。

同一查询、同一数据，只换这一个函数：

```
clock_timestamp():  Parallel Seq Scan    shared read=74,825   Execution 3721 ms
now():              Index Scan           shared hit=924        Execution    9 ms
                    (ix_strategy_runtime_events_type_time)
```

**约 400 倍差距，且读盘从 585 MB 降到 0。**

### 共 5 处（`deploy/ops/cml_ops_monitor.py`）

| 表 | 调用点 |
|---|---|
| `trading_leases` | `expires_at > clock_timestamp()` |
| `live_strategy_signals` | `source_state_at >= clock_timestamp() - ...` ×2 |
| `strategy_runtime_events` | `occurred_at >= clock_timestamp() - ...` |
| `exchange_orders` | `created_at >= clock_timestamp() - ...` |

已全部改为 `now()`。**这不会改变行为**：这些查询都是单条 autocommit 语句，
`now()`、`statement_timestamp()`、`clock_timestamp()` 三者相差不到 1 毫秒。

**另有 6 处 `clock_timestamp()` 保留不动**——它们在 SELECT 列表里，只用于
`EXTRACT(EPOCH FROM (clock_timestamp() - occurred_at))` 计算延迟，
不参与 WHERE 范围判断，因此不影响索引选择。

> **给后续写查询的人的提醒**：在 WHERE 里对时间列做范围过滤时，用 `now()` 或
> `statement_timestamp()`，**不要用 `clock_timestamp()`**。后者的语义（逐行实时）
> 听起来更"准"，代价却是索引完全失效。

### 附带修复：`universe_entries` 缺 `price_time` 索引

归档脚本每次运行都会执行 `min(price_time)` 和 `WHERE price_time < cutoff`，
而这张表原本只有 `(snapshot_id, symbol)` 和 `(snapshot_id, is_target)` 两条索引，
**没有任何以 `price_time` 打头的索引**：

```
5 calls | read=358,052 | rows=5 | pages_per_row=71,610   ← min(price_time)
4 calls | read=286,086 | rows=4 | pages_per_row=66,819   ← count(*)
```

已建 `ix_universe_entries_price_time`（并同步声明进 `models.py`），代价 9 MB：

```
Index Only Scan using ix_universe_entries_price_time
  Heap Fetches: 0     Buffers: shared hit=4     Execution 0.228 ms
```

### 效果

```
monitor 查询读盘     74,825 页/次  →  25–117 页
该查询命中率              4.2 %    →  96 %
系统持续读盘           8.6 MB/s    →  ~0
```

`pg_stat_statements` 重置后重新观察 150 秒，**top-5 里已全是 INSERT**——
那些 SELECT 查询不再出现在读盘榜上。

## 九、遗留事项

1. ~~retention 尚未定时化~~ **已完成**：`cml-archive-trim.service` + `.timer`，
   `OnCalendar=*-*-* 08:20:00 Asia/Shanghai`，`Persistent=true`，`--retention-days 2`。
   单元是 oneshot、无 `[Install]`，由 timer 驱动；重复运行会跳过已落在窗口内的表，是幂等的。
2. **归档需移出本机**才算备份。
2b. ~~`latest_reconciliation` 与 `account_fill_events` 两处读放大待分析~~ **已核查**：
   两者都不是问题。`account_fill_events` 的 `p/r=342` 是历史统计（该表已被清理到
   304 行，现在走 `Index Only Scan`，1 ms / 1 页）；`latest_reconciliation` 现在
   97 ms 且 `shared hit=0 read`，完全从缓存跑。它们的 `pages_per_row` 高是因为
   `pg_stat_statements` 是累计口径，包含清理前的时期。
3. ~~表内空洞未回收~~ **已完成**：六张表 `VACUUM (FULL, ANALYZE)`，回收 2542 MB（见第五节）。
4. `ix_account_position_latest`（`scans=2`）未被 dashboard 使用；`VACUUM FULL` 后
   已缩到 3.9 MB，不值得再单独处理。
6. ~~`cml_ops_monitor.py` 的 `memory_mb` 改动未部署~~ **已部署**（`39941d5`，
   `runtime_unchanged=1`，`systemctl restart cml-ops-monitor` 后生效）。
5. `pk_universe_entries` 曾是全库最大索引（421 MB）；`VACUUM FULL` 后为 18 MB，
   已不是问题。
7. `runtime_market_states_15s` 分区家族曾是全库最大（678 MB），现已收窄：
   **`_RUNTIME_STATE_RETENTION_HOURS` 48 → 12**（`b9e11e9`），家族 433 MB → **125 MB**。
   依据是所有读取方都只要"最新"或"最近的预热窗口"：`load_latest_bucket`（最新一桶）、
   `load_symbols_at`（某时刻的最新桶）、`load_recovery_window`（预热 ≈ 34 分钟）、
   `load_after`（游标增量）。**没有一个读取方需要超过 1 小时的历史。**

   它由 market-data 每 5 分钟驱动一次 `prune_runtime_market_states`，两处常量同在
   `apps/market_data/main.py`。删除逻辑只删"整块完全过期"的分区
   （`start + RUNTIME_STATE_PARTITION_INTERVAL <= cutoff`，分区粒度 6 小时），
   所以最多多留 6 小时，不是缺陷。
   **注意 `RUNTIME_STATE_PARTITION_LOOKAHEAD = 7 days` 不是保留期**——它是
   "提前创建未来 7 天的空分区"，与保留无关（曾据此误判为 7 天窗口）。
   策略读取它的窗口只有预热所需：`(warmup_buckets + 16) * 15s ≈ 34 分钟`，
   因此**唯一可缩的方向是把 48 小时改成更短（如 12 小时，约省 500 MB）**，
   代价是容器停机超过该窗口后启动恢复无法填满预热状态。**当前决定：不改。**

   它此前有 4 个索引，其中 `ix_..._partitioned_created (environment, created_at)`
   在**全部 37 个分区上累计扫描数为 0**，已随本次会话删除
   （`DROP INDEX` 父表定义，级联所有分区，耗时 0.20 秒，回收 77 MB；
   注意 PG 不支持在分区索引上使用 `CONCURRENTLY`）。剩余三个都有实际用途：
   `pk`（400,004 次扫描）、`polling`（低频非零）、`latest_bucket`（`INCLUDE (bucket_end)`）。

   该表**没有 jsonb 列**（37 列全是标量），所以它与"归档用 JSONL 还是 CSV"无关——
   那是文件格式的选择，不影响表的存储方式；而且它由分区 `DROP` 清理，
   不走 `archive_and_trim.py` 的归档路径。

8. **`RUNTIME_STATE_PARTITION_LOOKAHEAD` 已由 7 天改为 2 天**（`01b47f9`）。

   > **两个常量的分工**：`LOOKAHEAD` 管"提前建多少空分区"（兼作停机容忍度，见下）；
   > `RETENTION_HOURS` 管"保留多久数据"（决定占用空间）。**改空间要动后者。**

   它**不是保留期**，而是**预建范围**——同时兼作**停机容忍度**：补建只发生在
   retention 循环里（`prune_runtime_market_states`，每 5 分钟一次），所以主机停机
   超过该跨度后重启，没有任何分区覆盖 `now`，写入会失败，直到下一轮补建
   （最多 5 分钟）。

   收益**不在空间**：28 个空分区合计只有 **672 kB**（每个 24 kB = 零 heap + 三个索引的
   空根页）。收益在**对象数量**——6 小时粒度下 7 天 = 28 个空分区，整个分区家族
   约 148 个 `pg_class` 对象，每次规划剪枝、每条 DDL、每次备份都要遍历。

   已删除 20 个超出 `now + 2 天` 的空分区（条件式 `DROP`，只删 `reltuples <= 0`
   且起点超窗口的）：

   ```
   分区总数 37 → 17      空分区 28 → 8      家族大小 433 MB（不变，符合预期）
   ```

   注意：改小常量只影响"以后建多少"，**不会清理已经建多的**——那 20 个需要单独
   `DROP`。顺序上必须先重启 market-data 让新常量生效，否则旧代码会立即重建它们。

## 十、`raw_files`（原始行情归档）与空目录清理

**位置**：`/var/lib/docker/volumes/crypto-momentum-lab_market-data/_data/raw/`
（`exchange=` / `date=` / `stream=` / `symbol=` / `hour=` 分区，JSONL + zstd）

**它是回测的唯一原料来源**，不是可选的缓存：

```
raw_files（7 天滚动，retention_days=7 @ config/models.py:144）
   → replay_envelopes → normalize_binance_envelope
   → aggregate_market_states_15s
   → parquet（research-data 卷，无 retention）
```

`research/datasets.py` 的 `derive_market_datasets` 完全靠它重建 15 秒状态，
所以 **7 天决定了"能重算多久的历史"**；缩短会失去可重算性（parquet 已导出的不受影响）。
**当前决定：保持 7 天。**

### 空目录浪费（已清理）

`retention` 只 `unlink` 文件，**从不删目录**：

```python
# apps/market_data/main.py:582
manifest_paths = await repository.load_manifest_paths_before(cutoff_date)
# → delete_archive_files() 只删文件
```

于是每天都在留空目录壳。实测：

```
清理前   空目录 55,672   目录总 76,872   raw 356 MB   inode 644,408
清理后   空目录      0   目录总 12,934   raw 105 MB   inode 580,471
```

**释放 251 MB**（每个空目录约 4.5 KB 的目录块），**文件数不变**（12,458），
有数据的目录结构完好。用 `find <root> -type d -empty -delete` 清理即可
（`-delete` 隐含 `-depth`，会自底向上处理嵌套的空目录树）。

### 待查

磁盘上有 **12,458 个文件，而 `raw_archive_manifests` 只记录 12,166**——差额 262 个
"孤儿文件"不在 DB 里，因此 `load_manifest_paths_before` 永远不会返回它们，
retention 清理也看不见。成因未查，确认后可删。

## 十一、优化前后对比

三个时点的实测对照：起点（09-14 上午）→ 21:59（索引与查询修复后）→
22:44（`VACUUM FULL` 后）。

| 指标 | 上午起点 | 21:59 | 22:44（最终） | 总变化 |
|---|---|---|---|---|
| **db_size** | 4638 MB | 4248 MB | **1717 MB** | **-63 %** |
| **postgres swap** | 172 MiB | 20.5 MiB | **20.0 MiB** | **-88 %** |
| **持续读盘** | **8.6 MB/s** | 0.07 MB/s | **0.23 MB/s** | **-97 %** |
| **postgres file cache** | 1012 MiB | 876 MiB | **669 MiB** | **-343 MiB** |
| cache_hit_ratio | 58.04 % | 63.01 % | **63.62 %** | +5.58 pt |
| postgres 限额占用 | 593/1024 MiB（58 %） | 603/1280（47 %） | 639/1280 MiB（50 %） | 不再贴顶 |
| 系统 swap | 556 MB | 399 MB | 396 MB | -29 % |
| load (1m) | 2.69 | 3.07 | **2.07** | -23 % |
| 磁盘 | 41 % | ~38 % | 38 % | -3 pt |

**`file` cache 从 1012 降到 669 MiB（-343 MiB）是 `VACUUM FULL` 的直接结果**：
数据集变小后，需要缓存的东西也少了。此前那 1.1 GB 的 page cache 里有很大一部分
是在读 98 % 是空洞的文件。

**`anon` 的数值不可单独作判据**——它在 196~249 MiB 之间波动（内核换入/换出冷页），
判断内存压力是否回来要看 `swap`，它稳定在 20 MiB。

**最该看的是前两个数字**：`postgres swap` 172 → 20.5 MiB 是"内存压力真正下降"
的直接证据；持续读盘 8.6 → 0.07 MB/s 表明那 8.6 MB/s 确实是 `clock_timestamp()`
造成的，改动后消失。

**查询层面**（统计重置后的最耗时前 5 条）：

```
   6710ms |  150 calls | 0 read    SELECT anon_1.account_label FROM (...account_rec...
   3743ms |  906 calls | 0 read    SELECT exchange_orders.client_order_id...
   3261ms |   10 calls | 2796 read SELECT execution_account_process_states...
   2978ms |   36 calls | 0 read    WITH latest_reconciliation AS (SELECT DISTINCT ON...
   1753ms |  136 calls | 1240 read INSERT INTO account_balance_snapshots...
```

前 5 名里 3 条 `shared_blks_read = 0`，包括第九节列为待查的 `latest_reconciliation`
——它现在完全从缓存跑，不必再优化。

### 哪些降了、哪些降不了

**降了**：

```
postgres swap    172 MiB → 20.5 MiB     ← 内存压力
file cache      1012 MiB → 876 MiB
持续读盘           8.6 → 0.07 MB/s
db_size         4638 MB → 4248 MB
```

**降不了**（与会话开始时完全相同）：

```
shared_buffers   256 MiB   ← 配置的固定分配，与数据量无关
连接内存         ~100 MiB  ← 53 个 idle 连接，架构问题，本轮决定不改
```

`anon` 反而从 196 MiB 涨到 227 MiB——**这不是退化**：dashboard 从"被换出 65 MiB"
回到"驻留 93 MiB"，说明内核不再需要驱逐冷页。净效果是 swap 少用 152 MiB，
且换出/换入的开销消失。

**贡献排序**（对读盘而言）：

1. `clock_timestamp()` → `now()`（496 GB 累计读盘消除）—— **主要**
2. 清理 721 万行（数据集变小，缓存覆盖更充分）
3. `ix_universe_entries_price_time`（归档脚本不再全表扫）

## 十二、PostgreSQL 与内核参数评估

### 已有的调优（不是本次会话做的）

`postgresql.auto.conf` 里已有 12 项覆盖，说明这个库被认真调过：

```
checkpoint_timeout = 15min          max_wal_size = 2GB
checkpoint_completion_target = 0.9  bgwriter_lru_maxpages = 400
wal_buffers = 16MB                  wal_compression = lz4
log_temp_files = 64MB               log_min_duration_statement = 2000ms
log_connections / disconnections = off
shared_buffers = 256MB              effective_cache_size = 1GB
```

**但 `random_page_cost` 与 `effective_io_concurrency` 不在其中**——两项都与"磁盘类型"有关，
而恰好是它们被漏掉了（很可能是按默认值建库后没有针对 SSD 调整）。

### 已修改

| 参数 | 原值 | 现值 | 依据 | 生效方式 |
|---|---|---|---|---|
| **`random_page_cost`** | 4 | **1.1** | 4 是机械盘默认值；本机实测 `Seq Scan` 189 MB / 63.9 ms ≈ **3 GB/s**（SSD）。值偏高会让规划器倾向顺序扫描而非索引扫描 | `ALTER SYSTEM` + `pg_reload_conf()`，**不重启** |
| **`vm.swappiness`** | 60 | **10** | 内核默认 60 在非压力下也倾向换出匿名页。`market-data` 的 `memory.events.max = 0`（从未触及内存上限）却有 51 MB 被换出，说明换出并非压力驱动 | `sysctl -w` + `/etc/sysctl.d/99-cml-swappiness.conf` 持久化 |
| **`effective_io_concurrency`** | 1 | **32** | 见下"虚拟化云盘"一节。**不是 200**——那是裸机 NVMe 的经验值 | `ALTER SYSTEM` + `pg_reload_conf()` |

**观察基线**（改后立即记录）：

```
Swap 已用 884 MB     pswpout 累计 2,216,446 页     可用内存 931 MB
```

### 未修改（有意保留）

| 参数 | 值 | 原因 |
|---|---|---|
| **`max_parallel_maintenance_workers`** | **0** | **保持 0。** 本机只有 **2 个 CPU 核心**（`nproc = 2`），并行维护 worker 会与 `market-data` 和 4 个 `live-strategy` 抢 CPU，而行情与决策是延迟敏感的。此外 `cml_ops_monitor.py` 里有**硬编码护栏告警** `database_parallel_maintenance_enabled`（"数据库并行维护超过护栏"），说明这是有意设定的边界，不是待放宽的旧约束 |
| **`maintenance_work_mem`** | **32 MB** | **保持 32 MB（顶多 64 MB）。** 全局调大会让常态维护承担偶发运维的成本；偶发的大操作（`VACUUM FULL`、大表重建索引）应使用**会话级** `SET maintenance_work_mem = '256MB'`，用完即释放——本会话的 `VACUUM FULL` 正是这么做的（0.78 秒完成）。注：`autovacuum` 用的是单独设的 `autovacuum_work_mem`（同为 32 MB），**不受此参数影响** |
| `work_mem` | 16 MB | 合理 |
| `shared_buffers` | 256 MB | 合理（库 1.2 GB，配合 OS page cache 足够） |
| `effective_cache_size` | 1 GB | 合理 |
| `max_connections` | 100 | 不是问题（实际用 ~50，且它是上限不是预留） |

### 为什么 `effective_io_concurrency` 是 32 而不是 200

"SSD 设 200"针对的是**独立物理机 + 企业级多通道 NVMe**（几万～十几万 IOPS）。
本机是**云服务器上的虚拟化云硬盘**（设备 `/dev/vda`）：

```
nproc = 2                     Intel Xeon Gold 6133
/dev/vda  rotational = 1      nr_requests = 256
```

`effective_io_concurrency` 决定 PostgreSQL 执行 Bitmap Scan 时**一次性抛出的异步 I/O 请求数**。
在虚拟磁盘上设成 200，会把 200 个请求塞进 hypervisor 的队列，**而实盘交易的 WAL 顺序写
（订单提交、状态落库）排在它们后面等**——用"加快查询"换来了"拖慢交易"。

**32 是在这台机器上的折中**：能利用多队列，又不会冲垮虚拟磁盘队列。

### 三个被忽略的适用前提（本会话的判断失误记录）

本会话曾给出三条参数建议，**全部被外部意见纠正**，错因是同一个：**引用了通用最佳实践，
却没有检查它的适用前提在这台机器上是否成立**。

| 曾经的错误建议 | 实际 | 被忽略的前提 |
|---|---|---|
| `effective_io_concurrency` 改 **200** | 应为 **32** | 那是**裸机 NVMe** 的经验值；本机是**虚拟化云盘** |
| `maintenance_work_mem` 全局改 **128 MB** | 应保持 **32 MB** | "偶发大操作"应由**会话级 `SET`** 承担，不该写进全局 |
| `max_parallel_maintenance_workers` "理由已减弱" | 应保持 **0** | 只算了**内存**维度，漏了 **CPU 只有 2 核**这一更硬的约束 |

**教训**：参数的"推荐值"总是绑定一组前提（硬件类型、核心数、负载特征）。
**先确认前提，再引用结论**——本次会话里多次出错的都是这一步。

### 效果验证（已做）

**`random_page_cost` 4 → 1.1 —— 已验证，实测有提升**

构造"边缘选择性"查询（`count(*)` 不行：它走 Index Only Scan 不回表，索引永远更优；
必须取大列 `details` 强制回表才行）。在 `strategy_runtime_events` 上逐点对比：

```
N       rpc=4（旧）      rpc=1.1（新）
24h     Index Scan       Index Scan
26h     Seq Scan         Index Scan      ← 参数改变了选择
28h     Seq Scan         Index Scan      ←
30h     Seq Scan         Index Scan      ←
32h     Seq Scan         Index Scan      ←
34h     Seq Scan         Index Scan      ←
36h     Seq Scan         Index Scan      ←
40h     Seq Scan         Seq Scan
50h     Seq Scan         Seq Scan

切换阈值：约 25h  →  约 37h
```

`EXPLAIN (ANALYZE, BUFFERS)` 在 30h 上的实测：

```
rpc=4   → Seq Scan    hit=7,884    read=58,546   5,048.746 ms
rpc=1.1 → Index Scan  hit=231,097  read=52,416   2,955.081 ms   (-41%)
```

**决定性的是磁盘读，不是缓存命中**——`Index Scan` 命中数高得多（回表），
但少读 6,000 页磁盘，因此快 2 秒。

**适用范围要说清楚**：这个提升发生在"26–36 小时"窗口的查询上。而 `cml_ops_monitor`
用 5 分钟窗口、dashboard 取最新值——**这类长窗口查询的实际调用频率未查证**。
准确的说法是"参数改对了、机制验证了、收益 41%"，**不是"整体提速 41%"。**

**`vm.swappiness` 60 → 10 —— 已验证**

```
换出速率   ~3,900 页/60s（改前基线）  →  1,858 页/60s   (-52%)
```

减少了"非压力驱动的换出"，但**没有减少 swap 总量**（872 MB）——因为换出过的旧页
不会因此收回。**它只减少新的无用换出，不创造内存。**

**`effective_io_concurrency` 1 → 32 —— 未验证**

它影响 `Bitmap Heap Scan` 的并行预取，而本机数据大量命中缓存
（`shared hit` 约为 `read` 的 4 倍），**"位图扫描 + 冷缓存"的组合在当前负载下构造不出来**。
**记为"方向已修正，收益未验证"**，不硬造不真实的测试条件。
