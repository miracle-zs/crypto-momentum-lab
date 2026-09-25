# 服务器运维复盘：2026-09-12～09-13 异常分析

- **主机**：`43.167.191.253`（`VM-0-9-ubuntu`，腾讯云 2C 级小机，物理内存约 3.6 GiB）
- **业务**：Crypto Momentum Lab 实盘/模拟盘（Docker Compose + PostgreSQL）
- **分析窗口**：约 2026-09-12 09:00 ～ 2026-09-13 09:00 CST（并延伸核对至 09-13 上午实测）
- **对照代码（文档生成时）**：本地仓库 HEAD `bd036f5`（与服务器实盘镜像 `crypto-momentum-lab-app:bd036f559fee…` 一致；paper/market-data 镜像为 `6a08300`，落后 2 个提交；当前本地 HEAD 已推进到 `34b8410`）
- **核对方式**：容器日志、syslog/journal、ops monitor 告警、`docker inspect`/`docker stats`、PostgreSQL `SHOW` / `pg_stat_bgwriter` / `postgresql.auto.conf`

---

## 1. 执行摘要

窗口内**未发现宿主机宕机或 kernel OOM 记录**；采样到的容器均为 `OOMKilled=false`、`memory.events oom_kill=0`，磁盘正常（约 39%）。旧实例已经被替换，因此这里不把当前 `docker inspect` 当成对旧实例整个生命周期的绝对证明。真正需要关注的是三条主线：

1. **Primary 实盘策略 checkpoint 停滞约 45 分钟**（critical），最终靠人工重启恢复；
2. **容器内存顶到 cgroup limit**（live-strategy 达 99%，market-data 达 95%），整机 swap 已用近 1 GiB；
3. **PostgreSQL checkpoint 写盘持续偏慢**（单次 write 常见 200–400s），`shared_buffers` 实测仍为 128MB，尚未按本机资源做调优。

代码侧事后补了「unhealthy 自动重启」与订单批次恢复逻辑，但服务器实测显示：

- 自动重启**经常因 15s 命令超时失败**，重试预算会被烧光；
- 09-13 早上的 checkpoint 停滞**并未触发** `live_heartbeat_stale` / 自动重启（health 仍为 healthy）。

因此：**长时间假活风险仍在，资源紧平衡根因未消。**

---

## 2. 异常清单（按严重度）

### 2.1 Critical：primary checkpoint 停滞

| 项 | 内容 |
|----|------|
| 告警 | `live_checkpoint_stale:primary` |
| 时间 | 约 07:44 最后一次有效策略 checkpoint；07:59 触发；08:42 人工重启后恢复 |
| 细节 | age 906s → 1806s → 2706s（阈值 900s） |
| 范围 | 仅 `live-b1-long-100u-5x-v1`（primary）；account-2/3/4 无同类告警 |

**处置**：08:41–08:43 人工 stop/start live 策略与 execution 容器（`hasBeenManuallyStopped=true`）。新实例 `live_strategy_checkpoint_recovered`（`state_count=1346`），并在风险窗口执行 scheduled flatten（提交 3 张平仓单）。

**影响**：约 45 分钟实盘状态未持久化；若期间进程崩溃会丢该窗口状态。重启后已通过 checkpoint 恢复 + flatten 对齐仓位。

**高概率诱因判断（日志尚不能证明单一根因）**：

1. 容器内存顶限（01:32 live-strategy 内存 99.13% of 512MB limit）拖死写路径或事件循环；
2. PostgreSQL 写路径慢（checkpoint write 200–400s）放大策略 checkpoint 延迟；
3. 旧实例卡死前日志已随容器替换不可见，无法从现网容器日志 100% 定死单一代码 bug。

### 2.2 Critical / Warning：容器内存顶限

| 时间（约） | 服务 | 严重度 | 指标 |
|------------|------|--------|------|
| 00:16 | market-data | critical | 639MB / 640MB = **95.3%**，`memory.events max=8` |
| 00:17 | market-data | warning | 30 分钟增长约 146MB |
| 01:32 | live-strategy | critical | 532MB / 512MB = **99.1%**，swap 约 9.5MB |
| 全天多次 | postgres | warning | 监控采样 current 高位约 878MB / 1GiB；cgroup `memory.peak` 曾到约 1GiB；30 分钟可涨 100–350MB；`max=68287` |

**说明**：采样期间未见 `oom_kill`，内存压力主要由 cgroup 回收和少量 swap 承担。live-strategy 99% 与后续策略 checkpoint 停滞时间线重合，是首要嫌疑，但仍属于相关性证据。

### 2.3 Warning：PostgreSQL checkpoint 写盘慢

近 24h 典型日志：

- `checkpoint complete: wrote 2000–3300 buffers; write=200–400s`
- `pg_stat_bgwriter`（服务器 `stats_reset=2026-08-28 09:36:54+08:00`，覆盖本次窗口）：`checkpoints_timed=1501`，`checkpoints_req=4`；`checkpoint_write_time` 累计约 108 小时 → 平均单次 write 约 **260s**；`buffers_backend=1.43M`

**待验证因果链**：写压力大 → 脏页多 → 刷盘慢 → 应用侧策略 checkpoint/事务延迟可能被放大。

另有 00:47 人工 `psql` 误操作（不影响服务）：`role "postgres" does not exist`、`column orig_qty/account_id does not exist`。

### 2.4 Warning：market-data 事件循环延迟与行情缺口

- `market_data_event_loop_lag`：多次超过 warning 50ms，常见 50–200ms；00:41 出现 **532ms（超过 critical 500ms）**。
- aggTrade 健康快照：`detected_gap_count=4461`，`unrecovered_gap_count≈4430`，`missing_trade_count≈516 万`；延迟直方图 `>10s` 约 5.8k 条。
- 00:01 两条 `binance_websocket_session_ended` close_code=1000，属正常重订阅。

**日志支持的解释**：全量符号订阅 + 高吞吐 + 事件循环卡顿时缺口放大；forceOrder/bookTicker 相对正常。这里仍是解释性判断，不是单一根因证明。

### 2.5 Warning：重启后一次性业务告警（可接受）

08:43 重启后 primary 出现：

| 事件 | 含义 |
|------|------|
| `live_legacy_order_identity_conflict` | 两个 client_order_id 歧义；一个 zero-fill 终态；已 reconstructed，`unresolved=[]` |
| `live_exit_batch_binding_reassigned`（龙虾USDT） | 退出单与批次绑定不一致，4595 数量拆到 3 个 fallback 批次 |
| `binance_user_data_pipeline_recovery_requested` | `account_config_update` 触发 user data stream 重连后恢复 |
| entry gate 短暂 blocked | risk_control_stream_unavailable / scheduled_risk_window，秒级恢复 |

对应代码：`postgres_runtime.py` 中 `_repair_legacy_exit_batch_bindings` 与 fallback reassign，属**历史 stale-exit 的设计内自愈**，不是新引入的未修 bug。

### 2.6 低优先级噪声

| 项目 | 说明 |
|------|------|
| networkd-dispatcher veth not found | Docker 启停系统噪声 |
| sshd `kex_exchange_identification` reset | 公网扫描，窗口内约 3 次 |
| `live_strategy_warmup_symbols_deferred` | 含 `我踏马来了USDT` 等，warmup 延后，非错误 |
| 磁盘 / 负载 | 22G/59G（39%）；load 约 1.4–1.7，偏紧但可接受 |

---

## 3. 高概率因果链（仍需进一步验证）

下面的链路由时间线和资源告警支持，但不能替代对旧实例代码路径的直接取证：

```text
整机内存紧（3.6 GiB，swap 已用近 1 GiB）
        │
        ├─► market-data 全量订阅 → 顶到 640MB limit → event loop lag / 行情缺口
        │
        ├─► postgres 写盘慢（checkpoint write ~200–400s）+ cgroup 内存频繁回收
        │
        └─► live-strategy（×4）内存逼近/顶到 512MB limit
                    │
                    └─► primary checkpoint 停滞 ~45 min（critical）
                              │
                              ├─ Docker health 仍 healthy → 自动重启未触发
                              └─ 08:42 人工重启 → 恢复 + flatten 对齐
```

---

## 4. 当前代码与参数是否仍存在这些问题

对照文档生成时本地 HEAD `bd036f5` 与 `compose.server.yaml` / `compose.live.accounts.yaml` / `deploy/ops/cml_ops_monitor.py`。

| 问题 | 代码/配置现状 | 是否还会复现 |
|------|----------------|----------------|
| checkpoint 停滞 45 分钟靠人工救 | 已有本地 health marker 与 ops 自动重启；`2e6f542` 主要新增 unhealthy 自动重启，marker wiring 在 live runtime | **闭环有，但实战半残**（见 §5） |
| live-strategy / market-data 内存顶限 | limit 仍为 **512m / 640m** | **会** |
| Postgres checkpoint 慢 | trial 参数在 auto.conf；**shared_buffers 实测仍 128MB** | **很可能仍会** |
| exit batch reassign / legacy conflict | 设计内恢复，日志仍会打 | 会再出现，属预期 |
| market-data lag / gap | limit 与订阅规模未收 | **会** |
| warmup 延后符号 | `79cdcc1` 已容忍 | 可能再出现，无害 |

### 4.1 有改善的部分

- **自动重启骨架**：`CML_AUTO_RESTART_STALE_LIVE_SERVICES` 默认 `true`；按账户记录 restart state，带 cooldown / max attempts。
- **健康检查与数据库成功路径挂钩**：策略 checkpoint 成功、lease renewal 等数据库成功路径都会刷新本地 database marker；`docker/local-healthcheck` 要求 status 与 database 两个 marker 都在窗口内（live 窗口 **300s**）。因此 marker 健康不等价于最近一次策略 checkpoint 一定成功。
- **内存告警更准**：`1d86672` 趋势感知、`957f812` 容器重建后重置 baseline。
- **订单归属**：`ac7070d` / `e04fd44` / `7d41077` 等修复与 fallback reassign。

### 4.2 未改或改得不够的部分

- **内存 limit 数字未变**；compose 中 live-strategy 旁注释 “Observed usage is below 200 MiB” 与事故峰值 500MB+ **不符**。
- **Postgres `shared_buffers` 未写入 compose**，实测为 `postgresql.conf` 中的 128MB，未针对本机调优；`work_mem` 默认 4MB；`effective_cache_size` 仍为 4GB。后者是 planner 的缓存估算，不是实际分配的 4GB，但对 3.6G 主机可能偏高。
- **market-data** 仍全量监控（事故时 `monitoring_symbols=127`，desired_subscriptions 100+27）。
- **自动重启路径只在 Docker health 为 `unhealthy/dead` 时触发**；独立的 `live_checkpoint_stale` 只产生告警，不直接重启；且重启命令超时默认 15s（见下节）。

---

## 5. 服务器实测核对（2026-09-13 上午）

### 5.1 ops-monitor

| 项 | 结果 |
|----|------|
| 服务 | `cml-ops-monitor` **active / enabled**（`/etc/systemd/system/cml-ops-monitor.service`） |
| 进程 | `/usr/bin/python3 /opt/crypto-momentum-lab/deploy/ops/cml_ops_monitor.py` |
| 配置文件 | `/etc/crypto-momentum-lab/ops-monitor.env`：Server酱 SendKey、session/label/lease、`CML_ALERT_COOLDOWN_SECONDS=900` |
| 自动重启 | env **未覆盖** `CML_AUTO_RESTART_STALE_LIVE_SERVICES` → 代码默认 **`True`** |

#### 问题 A：自动重启命令 15s 超时（半残）

journal 中多次：

```text
live_heartbeat_restart_failed
error: docker compose ... restart live-strategy
timed out after 15.0 seconds
```

- `_DEFAULT_COMMAND_TIMEOUT_SECONDS = 15.0`
- live 容器 `stop_grace_period: 90s`
- 优雅未结束即判失败 → `restart_attempts` 烧光 → `live_heartbeat_restart_suppressed`
- 09-12 上午曾出现 `restart_count=22` 仍 suppressed

#### 问题 B：策略 checkpoint 停滞未触发自动重启（检测缺口）

09-13 07:59–08:42 **仅有** `live_checkpoint_stale`，**无** `live_heartbeat_stale` / `live_heartbeat_auto_restarted`。

含义：数据库中的**策略状态 checkpoint** 已停，但 Docker health 仍为 healthy（lease renewal 等路径仍可能刷新 database marker）。这不是 PostgreSQL 自身 checkpoint 停止。自动重启只依赖 health，**对“假活但策略状态不落盘”场景不生效**。

### 5.2 PostgreSQL 实测

`postgresql.auto.conf`（仍在）：

```text
checkpoint_timeout = '15min'
max_wal_size = '2GB'
checkpoint_completion_target = '0.9'
bgwriter_lru_maxpages = '400'
wal_buffers = '16MB'
wal_compression = 'lz4'
log_min_duration_statement = '2000ms'
...
```

`SHOW` 关键项：

| 参数 | 当前值 | 来源 | 判定 |
|------|--------|------|------|
| shared_buffers | **128MB** | `postgresql.conf` | **未按本机资源提升，短板** |
| work_mem | 4MB | 默认 | 偏小 |
| effective_cache_size | **4GB** | default（planner estimate） | 高于物理内存；属于规划估算，不是实际内存分配 |
| wal_buffers | 16MB | auto.conf | trial 已生效 |
| wal_compression | lz4 | auto.conf | 已生效 |
| bgwriter_lru_maxpages | 400 | auto.conf | 已生效 |
| checkpoint_timeout / completion_target / max_wal_size | 15min / 0.9 / 2GB | auto.conf | 已生效 |
| maintenance_work_mem / autovacuum_work_mem | 32MB | compose command | 已生效 |
| max_parallel_maintenance_workers | 0 | compose | 已生效 |

### 5.3 当前内存瞬时快照

| 容器 | 用量 / limit | 占比 | memory.events max |
|------|----------------|------|-------------------|
| live-strategy ×4 | ~280MiB / **512MiB** | ~55% | 0（本代实例尚未再顶） |
| market-data | 306MiB / **640MiB** | ~48% | **8** |
| postgres | 315MiB / **1GiB** | ~31% | **68287** |
| 宿主机 | used 2.7G / 3.6G；**swap used ~993Mi** | 紧 | — |

当前快照全部容器 health=healthy，采样容器 `OOMKilled=false`、`oom_kill=0`；旧实例已被替换，历史结论应理解为“窗口内未发现 OOM kill 证据”。PostgreSQL 的 878MB 是监控采样 current 高位，cgroup `memory.peak` 曾到约 1GiB。资源紧张条件仍在。

---

## 6. 建议行动

### 6.1 立即可做（不碰交易逻辑）

1. **调大 ops-monitor 重启命令超时**（覆盖 90s stop_grace + 余量）：

   ```bash
   # 当前代码不读取 CML_COMMAND_TIMEOUT_SECONDS。
   # 修改 /etc/systemd/system/cml-ops-monitor.service 的 ExecStart：
   ExecStart=/usr/bin/python3 /opt/crypto-momentum-lab/deploy/ops/cml_ops_monitor.py --command-timeout-seconds 120
   systemctl daemon-reload
   systemctl restart cml-ops-monitor
   ```

   现有代码支持 `--command-timeout-seconds`，但没有 `CML_COMMAND_TIMEOUT_SECONDS` 环境变量解析；只写入 `ops-monitor.env` 不会改变 15s 默认值。

2. **确认并保持** `CML_AUTO_RESTART_STALE_LIVE_SERVICES=true`（可显式写入 env，避免依赖默认值被误读）。

3. **监控** `live_checkpoint_stale` 与 `live_heartbeat_*` 是否成对出现；只出现前者时立即人工介入。

### 6.2 维护窗口（需短暂停写）

1. **PostgreSQL**：

   ```sql
   ALTER SYSTEM SET shared_buffers = '256MB';
   -- 重启 postgres 容器后 SHOW shared_buffers; 验证 checkpoint write 是否下降
   ```

   评估将 `effective_cache_size` 调至 1GB、`work_mem` 提至 8MB（前者是 planner 估算，后者按排序/哈希操作和并发连接累加；一次只改一个并观察订单延迟）。

2. **Compose 内存标定**：
   - live-strategy：按峰值（>500MB）重新评估 512m，或升配；
   - market-data：评估加大 limit / 明确 memswap；
   - 非关键 paper 容器可降配或停用以腾出宿主机缓冲。

3. **硬件**：评估升至 **4C8G**（与既有 performance runbook 一致）。

### 6.3 代码/产品改进

1. **checkpoint stale → 自动重启**：在 ops monitor 中对连续 N 次 `live_checkpoint_stale:{account}` 触发与 heartbeat 相同的 `compose restart`，或增加独立的策略 checkpoint marker；不要只依赖共享的 database marker 和 Docker health。
2. **修正 compose 中 live-strategy 内存注释**，与实测峰值一致，避免误判。
3. 若龙虾USDT 等在每次重启都稳定刷 `live_exit_batch_binding_reassigned`，再清理历史 `order_intent_executions` 绑定数据，而不是削弱 fallback 逻辑。

---

## 7. 当前状态（文档撰写时）

- 08:43 重启后全部容器 healthy；策略 checkpoint 正常恢复写入；无新的 checkpoint_stale critical。
- ops-monitor 在跑，自动重启逻辑在，但 **15s 超时问题未修**。
- 资源紧平衡与 PG `shared_buffers=128MB` **未修**。
- 内存顶限与慢 checkpoint **预计会再次出现**；“假活 45 分钟”依赖 health 的路径**仍可能漏检**。

---

## 8. 参考

| 路径 | 说明 |
|------|------|
| `compose.server.yaml` / `compose.live.accounts.yaml` | 服务 mem_limit、healthcheck、PG command |
| `deploy/ops/cml_ops_monitor.py` | 告警、自动重启、timeout 默认值 |
| `docker/local-healthcheck` | 本地 marker 探针 |
| `src/crypto_momentum_lab/live_rollout/postgres_runtime.py` | 批次绑定修复 / reassign |
| `docs/runbooks/postgresql-checkpoint-latency-guardrails.md` | PG checkpoint 护栏与 trial 记录 |
| `docs/runbooks/server-performance-audit-and-optimization-20260908.md` | 机器规格与扩容建议（当前为本地 ignored 文档，分享前需确认可访问） |
| `docs/runbooks/operational-alert-monitor.md` | ops monitor 运维说明 |
| 服务器 `/etc/crypto-momentum-lab/ops-monitor.env` | 线上告警/自动重启 env |
| 服务器 `postgresql.auto.conf` | 线上 ALTER SYSTEM 持久化参数 |

---

*本文档由 2026-09-13 现网日志排查 + 仓库代码对照 + 服务器实测汇总而成，供后续运维与改造决策使用。*
