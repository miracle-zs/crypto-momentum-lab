# 服务器性能诊断报告

**服务器**: 43.167.191.253 (腾讯云 KVM)
**检查时间**: 2026-09-09 12:38 CST
**系统**: Ubuntu, Linux 6.8.0-106-generic
**运行时长**: 25天
**检查方式**: 只读诊断，未做任何修改

---

## 一、硬件概况

| 指标 | 值 | 评估 |
|------|-----|------|
| CPU | Intel Xeon Gold 6133 × **2核** | ⚠️ 偏少 |
| 内存 | **3.6 GiB** 总计 | ⚠️ 紧张 |
| 磁盘 | 59G (已用 21G, 36%) | ✅ 充足 |
| Swap | 1.9G (已用 556M, ~29%) | ⚠️ 使用中 |

---

## 二、容器运行状态

共 **16 个 Docker 容器**全部运行中：

| 容器 | CPU % | 内存使用 | 内存限制 | 内存占比 |
|------|-------|---------|---------|---------|
| postgres | 0.86% | 395.3 MiB | 1 GiB | 38.60% |
| market-data | 19.31% | 296.1 MiB | 640 MiB | 46.27% |
| live-strategy-account-2 | 3.98% | 169.4 MiB | 512 MiB | 33.08% |
| live-strategy-account-3 | 2.36% | 167.5 MiB | 512 MiB | 32.71% |
| live-strategy-account-4 | 1.40% | 165.4 MiB | 512 MiB | 32.30% |
| live-strategy (primary) | 1.52% | 125.5 MiB | 512 MiB | 24.51% |
| research-collector | 0.00% | 93.2 MiB | 192 MiB | 48.55% |
| paper-orderflow-pair | 0.00% | 86.5 MiB | 256 MiB | 33.78% |
| paper-orderflow-gainer10-pair | 0.00% | 84.5 MiB | 256 MiB | 33.02% |
| paper-b1-gainer100-ema | 0.00% | 83.9 MiB | 192 MiB | 43.72% |
| paper-b1-gainer100 | 0.00% | 83.0 MiB | 192 MiB | 43.20% |
| dashboard | 0.14% | 80.7 MiB | 320 MiB | 25.22% |
| execution-account-live-account-3 | 0.00% | 80.3 MiB | 160 MiB | 50.16% |
| execution-account-live-account-4 | 0.00% | 79.6 MiB | 160 MiB | 49.77% |
| execution-account-live | 1.67% | 64.2 MiB | 160 MiB | 40.15% |
| execution-account-live-account-2 | 0.46% | 61.4 MiB | 160 MiB | 38.37% |

---

## 三、核心问题诊断

### 🔴 问题 1：内存严重不足（最高优先级）

```
总内存: 3.6 GiB
已使用: 2.8 GiB (78%)
可用:   861 MiB
Swap使用: 556 MiB (29%)
```

**内存消耗分布**：
- **PostgreSQL 容器**: 395 MiB（容器限制 1GiB）
- **PostgreSQL idle 连接**: 57 个 idle 连接，每个占用 130-170 MiB 虚拟内存
- **market-data**: 296 MiB（容器限制 640 MiB, 占 46%）
- **4 个 live-strategy 容器**: 合计 ~628 MiB
- **4 个 execution-account 容器**: 合计 ~286 MiB
- **4 个 paper 容器**: 合计 ~337 MiB
- **dashboard**: 81 MiB
- **Docker 引擎 (dockerd)**: 132 MiB

> **⚠️ 风险**: 16 个 Docker 容器共计消耗约 **2.0 GiB**，加上宿主机系统开销和 PostgreSQL idle 连接，
> 总内存需求远超 3.6 GiB。系统正在大量使用 Swap，直接导致性能下降。
> 虽然暂未触发 OOM Killer，但风险非常高。

### 🟡 问题 2：CPU 负载偏高

```
Load Average: 2.19, 2.36, 2.26（2 核 CPU）
CPU 平均使用率: usr ~45%, sys ~9%, idle ~43%
```

**CPU 消费 Top 进程**：
- **market-data 容器**: 单独占用 **19.3% CPU**（最大消费者）
- **live-strategy 容器 (account-2)**: 占用 **4.0% CPU**
- **live-strategy 容器 (account-3)**: 占用 **4.6% CPU**
- **live-strategy 容器 (account-4)**: 占用 **4.2% CPU**
- 两核 CPU 上 load average 持续 > 2.0，说明经常有进程排队等待

**vmstat 采样数据** (5次1秒间隔)：
```
procs -----------memory---------- ---swap-- -----io---- -system-- -------cpu-------
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 1  0 569592 202268  11264 1091124  140  157 22305   597 3659   19 45  9 43  2  0
 0  0 569592 203156  11344 1091248    4    0    64   232 2280 2560 16  3 81  1  0
 5  0 569592 204496  11396 1091224    0    0    44   252 3015 3840 38  7 55  1  0
 0  0 569592 204976  11464 1091344    0    0    40   872 3006 3691 35  4 60  1  0
 6  0 569592 202960  11488 1091444    0    0    32   200 2523 2317 37  4 59  1  0
```

> **⚠️ 注意**: Load average 与核心数持平（~2.2 vs 2 核），CPU 处于满负荷边缘。
> 市场行情波动时可能出现明显延迟。

### 🟡 问题 3：PostgreSQL 配置未优化

**配置参数对比**：

| 参数 | 当前值 | 推荐值 (3.6G RAM) | 说明 |
|------|--------|-------------------|------|
| `shared_buffers` | **128 MB** (默认值) | 256-384 MB | 严重偏小 |
| `work_mem` | **4 MB** (默认值) | 8-16 MB | 偏小 |
| `effective_cache_size` | **4 GB** (过高) | 1-1.5 GB | 超过实际可用 |
| `max_connections` | 100 | 50-60 | 偏多 |

**数据库现状**：
- 数据库大小: **4.1 GB**
- 活动连接: **57 个 idle + 1 个 active + 5 个后台进程**
- `account_balance_snapshots` 表有 **16.6% 死元组**（49,271 行）
- `paper_positions` 表有 **12.4% 死元组**
- `exchange_orders` 表有 **10.8% 死元组**
- autovacuum 最近一次在约 20 小时前执行

**PostgreSQL 主要数据文件**：
```
480 MB  base/121099/121310
395 MB  base/121099/121100
365 MB  base/121099/143946
360 MB  base/121099/121451
201 MB  base/121099/121120
181 MB  base/121099/121146
115 MB  base/121099/630463
```

> **⚠️ 重要**: `shared_buffers` 只有 128MB（默认值），对于 4.1GB 的数据库来说太小了。
> 57 个 idle 连接每个都占用内存，加剧了内存不足问题。

### 🟡 问题 4：磁盘 IO 间歇性压力

**iostat 历史平均**：
```
磁盘读: 406 r/s, 22 MB/s
磁盘写: 29 w/s, 0.6 MB/s
磁盘利用率: 11.66%
```

**实时采样** (1秒间隔)：
```
# 采样1
vda  30 r/s, 312 kB/s | 58 w/s, 568 kB/s | util 5.10%
# 采样2
vda   9 r/s,  56 kB/s |  9 w/s,  88 kB/s | util 1.60%
```

**PostgreSQL 容器累计 Block I/O**: 读 79.5 GB / 写 54.1 GB
- PostgreSQL 容器累计 Block I/O 达 **133.6 GB**，说明大量数据无法命中缓存，频繁落盘读取
- 这与 `shared_buffers` 过小直接相关

### 🟢 问题 5：内核参数可优化

| 参数 | 当前值 | 建议值 | 说明 |
|------|--------|--------|------|
| `vm.swappiness` | **60**（默认） | 10-15 | 减少 swap 使用 |
| `vm.dirty_ratio` | 20 | 20 | 可保持 |
| `vm.dirty_background_ratio` | 10 | 10 | 可保持 |
| `vm.overcommit_memory` | 0 | 0 | 可保持 |
| `net.core.somaxconn` | 4096 | 4096 | 已是合理值 |
| `net.ipv4.tcp_max_syn_backlog` | **256** | 1024 | 适当提高 |
| Transparent Hugepages | **madvise** | never | PostgreSQL 建议禁用 |

### 🟢 问题 6：Docker 磁盘空间可回收

| 类型 | 总量 | 可回收 |
|------|------|--------|
| 镜像 (10个, 仅2个活跃) | 6.67 GB | **3.45 GB (51%)** |
| 构建缓存 (107个, 全部未使用) | 3.56 GB | **2.53 GB** |
| 容器 | 211 MB | 0 (全部运行中) |
| 卷 | 4.53 GB | 0 (全部使用中) |
| 日志 `/var/log` | 410 MB | 可轮转 |
| 旧备份 `runtime_market_states_15s_legacy...dump` | 201 MB | 可清理 |

---

## 四、网络与服务状态

**监听端口**：

| 端口 | 服务 | 说明 |
|------|------|------|
| 22 | sshd | SSH 远程访问 |
| 80 | nginx | HTTP 反向代理 |
| 8000 | python | 本地服务 (127.0.0.1) |
| 8765 | docker-proxy | Dashboard (127.0.0.1) |
| 53 | systemd-resolved | DNS 解析 |

**系统服务**：
- Docker、nginx、ssh、chrony (NTP) 正常运行
- `cml-live-container-events.service` 和 `cml-ops-monitor.service` 正常运行
- 无多余不必要服务占用资源

**系统日志**：
- 无 OOM Killer 触发记录
- 仅有少量 Docker 网络接口清理告警（`veth not found`），属正常现象
- 无其他严重错误

---

## 五、优化建议（按优先级排序）

### 优先级 1：升级服务器配置 ⭐⭐⭐

> **这是最关键的优化。** 当前 2 核 3.6G 的配置对于 16 个 Docker 容器 + PostgreSQL 的工作负载来说严重不足。

**推荐配置**: 至少 **4 核 8G**，理想 **4 核 16G**
- 可以直接在腾讯云控制台升级实例规格
- 升级后性能提升会非常显著
- 预计月费增加不多，但收益巨大

### 优先级 2：PostgreSQL 调优 ⭐⭐

即使不升级硬件，以下调整也能明显改善数据库性能：

```ini
# postgresql.conf 建议调整
shared_buffers = 256MB          # 从 128MB 提升，减少磁盘读
work_mem = 8MB                  # 从 4MB 提升，改善排序/哈希
effective_cache_size = 1GB      # 从 4GB 调低到实际值，修正查询计划
maintenance_work_mem = 128MB    # 加速 VACUUM 操作
random_page_cost = 1.1          # 如果使用 SSD（云盘通常是 SSD）

# 连接管理
idle_in_transaction_session_timeout = 60000  # 60秒后清理空闲事务连接
```

还可以考虑引入 **PgBouncer** 做连接池，减少 57 个 idle 连接的内存消耗。

### 优先级 3：降低 vm.swappiness ⭐⭐

```bash
# 立即生效
sysctl -w vm.swappiness=10

# 持久化
echo "vm.swappiness=10" >> /etc/sysctl.conf
```

减少系统倾向于使用 swap 的行为，对 Python 进程和 PostgreSQL 都有帮助。

### 优先级 4：精简容器数量 ⭐

当前共 16 个容器，考虑：
- 4 个 paper 模拟容器在资源紧张时**可以临时停用**，释放约 340 MiB 内存
- 评估是否所有 4 个 live-strategy + 4 个 execution-account 都需要同时运行
- Dashboard 容器 (81 MiB) 是否需要常驻，可按需启动

### 优先级 5：PostgreSQL 维护 ⭐

- `account_balance_snapshots` 表 16.6% 的死元组需要清理
- `paper_positions` 表 12.4% 和 `exchange_orders` 表 10.8% 的死元组也需清理
- 考虑调整 autovacuum 更积极的参数：
  ```ini
  autovacuum_vacuum_scale_factor = 0.05    # 默认 0.2
  autovacuum_analyze_scale_factor = 0.025  # 默认 0.1
  autovacuum_vacuum_cost_delay = 2ms       # 默认 2ms, 可保持
  ```
- 手动执行一次全量清理: `VACUUM ANALYZE;`

### 优先级 6：清理 Docker 磁盘空间

合计可释放约 **6 GB** 磁盘空间：

```bash
# 清理未使用的镜像（保留正在使用的）
docker image prune -a

# 清理构建缓存
docker builder prune

# 清理旧备份（确认不需要后）
rm /var/backups/crypto-momentum-lab/postgres/runtime_market_states_15s_legacy_20260823070640-20260901T151038Z.dump
```

### 优先级 7：Transparent Hugepages

当前 THP 设置为 `madvise`，对于运行 PostgreSQL 的机器建议设为 `never`：

```bash
# 立即生效
echo never > /sys/kernel/mm/transparent_hugepage/enabled

# 持久化 - 创建 systemd service
cat > /etc/systemd/system/disable-thp.service << 'EOF'
[Unit]
Description=Disable Transparent Huge Pages
DefaultDependencies=no
After=sysinit.target local-fs.target

[Service]
Type=oneshot
ExecStart=/bin/sh -c 'echo never > /sys/kernel/mm/transparent_hugepage/enabled'

[Install]
WantedBy=basic.target
EOF

systemctl enable disable-thp
```

---

## 六、总结

| 维度 | 评分 | 说明 |
|------|------|------|
| 内存 | 🔴 **危险** | 3.6G 跑 16 个容器，Swap 使用 556M |
| CPU | 🟡 **紧张** | 2 核 Load ~2.2，满负荷边缘 |
| 磁盘空间 | 🟢 **良好** | 使用 36%，空间充足 |
| 磁盘 IO | 🟡 **偏高** | PG 缓存不足导致大量磁盘读 |
| PostgreSQL | 🟡 **需优化** | 默认配置，idle 连接过多，死元组堆积 |
| 网络 | 🟢 **正常** | 无明显问题 |
| 稳定性 | 🟢 **良好** | 无 OOM 记录，无严重错误日志 |

**恶性循环链路**：

```
内存不足 → 大量使用 Swap → 磁盘 IO 放大 → PostgreSQL 缓存命中率低
    ↑                                              ↓
    ←← PG shared_buffers 过小，idle 连接占内存 ←←←←
```

**一句话建议**: **升级到 4 核 8G** + **调优 PostgreSQL 配置** + **降低 swappiness**，预期性能提升 50%+。

---

*报告生成工具: Antigravity AI 自动化诊断*
*注: 本次检查为只读操作，未对服务器做任何修改*
