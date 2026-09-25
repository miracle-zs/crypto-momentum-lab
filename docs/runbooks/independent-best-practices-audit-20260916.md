# 独立实盘审计：行业最佳实践对照（2026-09-16）

- **审计方式**：不依赖仓库既有 review 文档，直接 SSH 实机采集 `43.167.191.253` 运行态 + 本地代码/compose 对照
- **采集时间**：2026-09-16 11:08–11:12 CST（UTC 03:08–03:12）
- **系统**：Binance USD-M 永续，`orderflow_impulse`，4 个实盘账户，15s 状态时钟
- **结论先行**：交易语义层（fail-closed、lease、checkpoint、entry gate）已达工业常识；**当前最大风险不在策略代码，而在「仓位积压 + 单机资源贴顶 + 安全面敞开 + 无站外死人开关」四件事同时成立**。

---

## 0. 现网事实快照（自采）

### 0.1 拓扑与账户

| 项目 | 实测值 |
|---|---|
| 宿主机 | 2 vCPU / 3.6 GiB RAM / 59G 盘（用 38%），Ubuntu，Docker Compose |
| 容器数 | 13 个全部 healthy |
| 实盘账户 | primary / account-2 / account-3 / account-4 |
| 策略 | `orderflow_impulse`，commit `eaea27d4…`，config hash 两组（B1 与 B8 变体） |
| 执行参数 | 5x / CROSSED / long-only / hedge / TP 2% / SL 1% / `candle_15m` / grace 8 bars |
| 会话命名 | `live-b1-long-100u-5x-v1`（约 100 USDT 名义） |
| 并行件 | market-data、research-collector、paper-orderflow pair、dashboard(127.0.0.1:8765)、ops-monitor |

### 0.2 资金与持仓（UTC 03:10）

| 账户 | wallet USDT | available | uPnL | 观察到的持仓特征 |
|---|---:|---:|---:|---|
| primary | 97.22 | 16.81 | -0.57 | 与 account-2 同配置，仓位重 |
| account-2 | 89.03 | **8.73** | -0.33 | **31 个多头持仓**，available 仅剩 9.8% |
| account-3 | 214.07 | 193.96 | -0.16 | 仓位轻 |
| account-4 | 220.67 | 200.56 | -0.16 | 仓位轻 |

account-2 持仓按快照年龄分桶：

| 年龄 | 持仓数 | 名义 (USDT) | uPnL |
|---|---:|---:|---:|
| <6h | 6 | 883 | **-18.39** |
| 6–24h | 15 | 1776 | +3.55 |
| 24–48h | 10 | 1463 | -16.88 |

四个账户「超过 24h 未刷新快照」的持仓各 10 个，合计浮亏约 **-48.5 USDT**。单笔重亏可见：`IDOLUSDT -16.81`、`4USDT -6.40`、`哈基米USDT -5.01`。

### 0.3 24h 关键告警（ops-monitor journal）

| 类型 | 证据 | 含义 |
|---|---|---|
| `live_exit_processing_degraded` | `龙虾USDT`，持续约 16 分钟 | **平仓通道劣化，持仓可能平不掉** |
| `live_entry_lane_disabled` | 因 exit failure 禁开仓 ~6.6 分钟 | fail-closed 生效（好），但说明退出路径有故障 |
| `live_signal_divergence` | primary vs account-2，`MIRAUSDT` | 同配置账户信号不一致 |
| `live_position_intent_divergence` | 同 symbol 订单数 1 vs 2 | 意图指纹分叉 |
| `container_missing` | `execution-account-live-account-4` 短暂消失 | 执行账户容器被拉起/重建过 |
| `live_market_state_delay` | 24h 内 16 次，延迟 32–80s | 行情→策略路径周期性超预算 |
| `container_memory_*` | **24h 内 98 次** | 内存告警噪声淹没真 page |

primary live-strategy `RestartCount=5`（今日 07:24 UTC 附近重启过）。

### 0.4 资源与数据库

```
Mem: 3.6Gi total, 3.0Gi used, 685Mi available
Swap: 1.9Gi total, 986Mi used   ← 交易路径已在吃 swap
Load: 1.50 / 2 cores（持续）
```

| 组件 | limit | 当前占用 | 备注 |
|---|---:|---:|---|
| live-strategy ×4 | 512m | ~265m（52%），峰值告警 75%+ | RSS 缓冲 17k states / 120 symbols |
| market-data | 640m | 238m，CPU 25% | event_loop_lag 稳态 **45–150ms**（warning=50） |
| postgres | 1280m | 616m，swap 78–255m | Block I/O 读 82G / 写 36G |
| paper + collector + dashboard | ~700m 合计 | ~195m | 与实盘争抢同一台 2C/3.6G |

PostgreSQL 关键参数（已调过，与小机自洽）：

- `shared_buffers=256MB`、`effective_cache_size=1GB`、`max_wal_size=2GB`、`checkpoint_completion_target=0.9`、`work_mem=16MB`
- DB 总大小 **2012 MB**
- `strategy_runtime_events`：**1021 MB / 951k 行，未分区**（库内最大表）
- `runtime_market_states_15s`：**已按 6h 分区**（做得对）
- `pg_stat_bgwriter`：`checkpoint_write_time` 累计 **108,978s（约 30 小时）**，`buffers_backend=886,665` → 后台刷盘被迫代偿

策略 checkpoint 延迟样本（15s 一次）：`total_ms` 常见 **120–390ms**，`pool_acquire_ms` 常 70–200ms——连接获取本身就贵。

Market-data 聚合尖峰：`processing_max_ms=2709ms`；aggTrade 迟到 >1s 约 2.4%（98k/4.1M）。

### 0.5 安全面（自采）

| 检查项 | 实测 |
|---|---|
| SSH | `PermitRootLogin yes` + `PasswordAuthentication yes` |
| fail2ban | **inactive** |
| UFW | **inactive** |
| 端口暴露 | `0.0.0.0:22`、`0.0.0.0:80`；dashboard 仅 127.0.0.1:8765（好） |
| 站外死人开关 | `.env.server` / live example **无 EXTERNAL HEARTBEAT / HEALTHCHECKS 配置** |
| NTP | chrony 同步正常 |
| 口令 | 本次任务中以明文口令登录（口令本身强度弱） |

---

## 1. 行业最佳实践对照（按优先级）

### 1.1 交易与仓位治理 —— 差距等级：**高**

| 行业实践 | CML 现状 | 差距 |
|---|---|---|
| 单账户并发持仓硬上限 + 名义预算 | account-2 已 31 仓、available 仅 8.73，仍在产生新信号（24h strategy_signal 1130） | **缺硬顶** |
| 退出优先于入场；退出失败必须阻断新开 | exit degraded → entry lane disabled，方向正确 | 机制在，但 **退出故障本身反复出现** |
| 仓位最大持有时间 / 资金费率感知退出 | `candle_15m` + grace 8 bars；仍有 >24h 快照持仓 | 持有超时与超时强平需复核是否真生效 |
| 同构账户确定性对齐 | 出现 signal/intent divergence（订单数 1 vs 2） | 需根因，不能只靠告警消化 |
| 小市值 alt 流动性折扣 | 持仓含 `4USDT`/`IDOLUSDT`/`哈基米USDT` 等低流动性标的 | 冲击成本与退出滑点风险高 |

**建议（P0）**
1. 为每个账户配置 `max_open_positions`（建议先 8–10）与 `max_total_notional`（建议 ≤ 1.5× wallet），在 entry gate 硬拒。
2. 复核 `龙虾USDT` 类 exit degraded 的根因（限价 TTL、minNotional、symbol 精度、reduce-only 校验）。
3. 对 >12h 未平仓启动「强制审查」：要么走 scheduled flatten，要么书面接受。
4. 对 divergence 做一次人工复盘：是时钟/行情分叉，还是状态机非确定性。

### 1.2 容量与延迟 —— 差距等级：**高**

| 行业实践 | CML 现状 | 差距 |
|---|---|---|
| 关键服务 limit ≈ p99×1.3–1.5，交易稳态不进 swap | live-strategy 峰值 75%+ limit；host swap 986Mi；postgres 进 swap | **贴顶** |
| 行情 IO 与计算解耦、分级订阅 | 已有 hub + 队列（利用率极低，好）；仍 100+ 流全量 aggTrade | 分级订阅未做 |
| 事件循环 lag 作为入场门 | lag 稳态 45–150ms，长期 warning；聚合尖峰 2.7s | 门存在，但未阻断入场 |
| 容量留白 30%+ | 2C/3.6G 跑 13 容器 + paper + 研究 | **规格不足** |

**建议（P0/P1）**
1. **升配优先于继续精调**：至少 4C/8G，或把 paper/研究/collector 迁出实盘机。这是当前收益/风险比最高的动作。
2. 升配前的临时缓解：停 paper pair、降 research-collector 采集密度、给 postgres 更明确的 `memory.swap` 政策。
3. 把 `event_loop_lag > 100ms` 与 `market_state_delay > 30s` 映射为 **禁止新开仓**（保持退出）。
4. `strategy_runtime_events` 按天分区 + `DROP PARTITION` 保留策略（states 表已经分区，events 表没有，是最大单表）。

### 1.3 可观测性与自愈 —— 差距等级：**高**

| 行业实践 | CML 现状 | 差距 |
|---|---|---|
| 站外 dead-man（Healthchecks.io / 异地探针） | 代码/文档有钩子痕迹，**生产 env 未配置** | **致命盲区** |
| Page 可行动、低噪声 | 24h 98 次 memory warning，critical 多次「自愈」 | **告警疲劳** |
| 业务新鲜度 ≠ 进程存活 | healthcheck 有本地 marker；exit degraded 曾持续 16 分钟才被看到 | 深度探针未把「退出通道可用」纳入 liveness |
| 自愈闭环可靠 | ops-monitor 已修 command-timeout=120s；live RestartCount=5 | 需确认重启是否与对账/仓位安全联动 |

**建议（P0）**
1. **立刻接站外死人开关**：live 主循环每 30s ping 外部 URL，带 `last_checkpoint_at`、`open_positions`、`unresolved_orders`；90s 无心跳 → 电话/短信。这是单机架构下唯一能救「整机假活」的手段。
2. 把 memory warning 降噪：仅当 **连续窗口且 fraction>阈值** 才 page；单次 blip 只进 dashboard。目标：page < 5 条/天。
3. `/healthz` 增加业务不变量：`now-last_checkpoint<120s`、`unresolved_reconciliation==0`、`exit_lane_ok==true`。
4. 对「exit degraded → entry disabled」做一次故障注入演练，验证自动恢复与仓位对账。

### 1.4 安全 —— 差距等级：**严重（应立即处理）**

| 行业实践 | CML 现状 | 风险 |
|---|---|---|
| 禁 root 密码登录，仅密钥 | root + password | 暴力破解 / 撞库 |
| 主机防火墙 + fail2ban | 两者皆关 | SSH 与 80 全网可达 |
| 口令不入聊天/工单 | 本次任务口令已出现在对话中 | **应视为已泄露** |
| 交易所 API：无提现、IP 白名单 | 未在本机验证（需到币安控制台人工确认） | 凭证被盗可提币 |
| 单机风险书面化 | 未见 runbook 明确「主机故障=全账户中断」 | 事故响应慢 |

**建议（本周内）**
1. **立刻改 SSH**：`PasswordAuthentication no`、`PermitRootLogin no`（或 `prohibit-password`），仅保留密钥；启用 `ufw` 仅放行 22（限源 IP 更佳）+ 80；启用 fail2ban。
2. **轮换本次暴露的 root 口令**；检查 `authorized_keys` 是否有多余公钥。
3. 登录币安确认：提现关闭、API Key 绑定本机出口 IP、仅 Futures 交易权限。
4. 写一页 `docs/runbooks/single-host-failure.md`：主机失联时如何冻结交易、从备份恢复、对账步骤。

### 1.5 研究—实盘一致性 —— 差距等级：中

| 行业实践 | CML 现状 | 评价 |
|---|---|---|
| Paper 与 Live 并行对照 | paper-orderflow pair 在跑 | 好 |
| Point-in-time universe | universe 15m 刷新 + 分区 states | 基本具备 |
| 成本模型（fee+spread+slip） | replay 支持 latency/fee/slip | 好 |
| 纯函数策略内核 | 研究/回放/实盘共享 strategy core | 好 |
| 实盘 vs 回放对账 | `server_exports` 有 reconcile 产物 | 已有流程，应例行化 |

---

## 2. 与「通用教程」的边界提醒

本机是 **2C/3.6G + 13 容器 + 单 PG**，不是专用 DB 机：

- 不要再按「shared_buffers=25% RAM」调（当前 256MB 是 cgroup 1.25G 下的合理值）。
- 不要为「八股文」去动 `checkpoint_completion_target=0.9` / `max_wal_size=2GB`——问题在 **写入量与盘**，不在 completion target。
- 统一连接池会杀死交易路径隔离：live 已有细分池方向，应保持。

**真正的瓶颈排序（按对资金安全的影响）**：
1. 退出通道故障 / 仓位积压（直接亏钱）
2. 无站外死人开关（整机假活无人知）
3. SSH/防火墙裸奔（凭证与主机沦陷）
4. 内存 swap + 单机过载（放大 1–3）
5. 大表未分区 / 告警噪声（慢性）

---

## 3. 建议行动清单（可执行）

### P0 — 本周（安全 + 资金）

| # | 动作 | 验收 |
|---|---|---|
| 1 | 关闭 SSH 密码登录、开 UFW/fail2ban、轮换 root 口令 | `PasswordAuthentication no`；外部扫 22 仅密钥 |
| 2 | 配置站外 dead-man（Healthchecks.io 或第二台小 VPS） | 断网 90s 收到电话/短信 |
| 3 | 账户级 `max_open_positions` + `max_total_notional` 硬门 | account-2 类账户无法扩到 30+ 仓 |
| 4 | 复盘 `龙虾USDT` exit degraded 与 4 账户分歧告警 | 有根因结论 + 修复或接受说明 |
| 5 | 盘点 >24h 未平仓，决定 flatten 或书面持有 | stale 持仓数清零或有记录 |

### P1 — 两周内（容量 + 数据）

| # | 动作 | 验收 |
|---|---|---|
| 6 | 升配 4C/8G **或** 迁走 paper/collector | host swap used 稳态 <100Mi；available >1.5G |
| 7 | `strategy_runtime_events` 按天分区 + 保留策略 | 最大单表 <200MB；checkpoint write p95 下降 |
| 8 | 行情 lag/delay → 禁开仓门 | lag>100ms 时无新开仓成交 |
| 9 | 告警分级降噪（memory/page 表） | page ≤5/day，critical 必须可行动 |

### P2 — 一个月（架构）

| # | 动作 |
|---|---|
| 10 | 分级订阅：Top-N+持仓高频，候选池低频（释放 market-data CPU） |
| 11 | 时序冷数据出 PG → Parquet/对象存储；研究走 DuckDB |
| 12 | 评估第二可用区/热备（资金规模扩大后） |
| 13 | 假活演练 + 重启后仓位对账例行化 |

---

## 4. 做得好的地方（应保持）

1. **Fail-closed 入场门**：exit 失败自动 `entry_lane_disabled`——这是行业正确方向。
2. **Lease + checkpoint + 持久化屏障**：15s checkpoint 在写，lease 在续。
3. **states 表分区**与 PG 参数在小机上的自洽调参。
4. **结构化 JSON 告警 + ops-monitor** 自动化骨架（超时已提到 120s）。
5. **Dashboard 仅绑 127.0.0.1**，未对公网裸奔。
6. **Paper 与 Live 并行**、有对账导出物，研究—生产同核。

---

## 5. 证据索引

- 容器与资源：`docker ps` / `docker stats` / `free -h`（2026-09-16 11:09 CST）
- 持仓与余额：`account_position_snapshots` / `account_balance_snapshots` 最新 DISTINCT ON
- 告警：`journalctl -u cml-ops-monitor.service --since "24 hours ago"`
- 延迟：`docker logs crypto-momentum-lab-market-data-1` health_snapshot
- Checkpoint：`docker logs crypto-momentum-lab-live-strategy-1` `strategy_checkpoint_persisted`
- 安全：`sshd_config` / `ufw status` / `fail2ban` / `ss -tlnp`
- 参数：`deploy/live-runtime.yaml`、compose mem_limit、PG `SHOW`

---

*本报告仅基于 2026-09-16 实机快照与代码/compose 对照，不引用仓库内既有 review 结论作为事实来源。*
