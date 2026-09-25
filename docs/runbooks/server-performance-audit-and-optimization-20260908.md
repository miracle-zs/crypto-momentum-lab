# 生产服务器性能全景审计与深度优化方案 · 2026-09-08 晚间

> **审计时间**：2026-09-08 22:20 - 22:50 CST  
> **审计目标**：腾讯云东京节点生产服务器（`43.167.191.253`）  
> **审计原则**：**严格只读**，无任何配置变更、无进程重启、无写入与干扰。

---

## 1. 硬件规格与晚间运行状态概览

| 检查维度 | 指标 / 当前值 | 状态评估与校准说明 |
| :--- | :--- | :--- |
| **云服务商 / 地域** | 腾讯云 CVM / 日本东京（`ap-tokyo`，实例 `ins-dhz0aptu`） | 临近交易所机房，Binance REST API 连接延迟仅 **~5.28ms** |
| **CPU 规格** | 2 vCPU（Intel Xeon Gold 6133 @ 2.50GHz） | **偏紧**：当前负载 `2.68, 2.52, 1.79`（高于 2.0 临界线），高峰短暂达到 `3.19` |
| **上下文切换** | `sar -w` 均值 **~4,046 次/秒**（峰值 5,218 次/秒） | 短时采样中曾捕获过 7,700~8,393 次/秒脉冲，但**历史均值表明无持续调度饱和** |
| **物理内存** | 3.64 GiB（4GB 档位） | **较紧**：已用 2.6GiB，空闲约 139MiB，系统 Commit 在 100%~103% 边缘 |
| **Swap 交换区** | 2.0 GiB（存量占用约 290MiB） | **低速率换页**：`sar -W` 22:20 换出仅 `0.04 pages/s`，22:30 峰值 `150 pages/s`（约 **0.59 MiB/s**）；瞬时采样偶现脉冲，非持续高强度换页 |
| **磁盘存储** | 系统盘 60GB（已用 43GB，使用率 **76%**，剩余 14GB） | **空间告警**：Docker 镜像及 Build Cache 冗余积压达 **22.6 GB** |
| **磁盘 I/O** | `sar -b` 显示平均每秒读 700 块、写 964 块 | 瞬时采样曾见 `iowait` 脉冲，但全天 `sar -u` 均值 `iowait` 仅 0.94% |
| **运行服务** | 16 个 Docker 容器 + 宿主机 Nginx、Uvicorn、iperf3 | 2C4G 承载 16+ 容器与混部服务，资源处于紧平衡 |

---

## 2. 晚间与早间（06:50）运行指标对比

对比本日早晨审计（06:45~06:54）与晚间高峰（22:20~22:50）的演变态势：

| 关键指标 | 早晨（06:50） | 晚间（22:20~22:50） | 趋势与客观解读 |
| :--- | :--- | :--- | :--- |
| **系统 Load 1m** | 1.04 | **2.68（峰值 3.19）** | 晚间行情波动导致计算和事件处理上升，CPU 负载处于满负荷上限 |
| **上下文切换率** | 约 3,200/s | **4,046/s（稳态）/ 5,218/s（高峰）** | 轮询与网络事件增多，偶发 8,000+ 脉冲，整体调度仍可控 |
| **磁盘已用空间** | 37 GiB（可用 20 GiB） | **43 GiB（可用 14 GiB）** | 磁盘剩余跌破 25%，主要为 Docker 镜像构建缓存积攒 |
| **Docker 可回收空间** | 未分解（早间命令超时） | **22.66 GB（镜像 11.2G + 缓存 11.4G）** | 明确为历史废弃镜像和 Docker 构建缓存，清理收益高且确定 |
| **活跃容器数** | 16 个（版本分立） | 16 个（全部运行 `3e17348` 统一镜像） | 镜像版本已完成统一部署 |
| **数据库大小** | 3.41 GiB | **3.95 GiB** | `universe_entries` 增至 474 万行，时序表持续自然增长 |
| **Swap 换出速率** | ~0.3 MiB/s | **0.04 ~ 0.59 MiB/s（稳态）** | 均值较低，早前观察到的 7.5~8MB/s 属于瞬时采样脉冲 |

---

## 3. 核心瓶颈与优化空间分析

### 瓶颈 1：容器内存限额超配（Overcommit）但稳态 Swap 影响有限
* **现状分析**：
  * 宿主机可用物理内存仅 **3.64 GiB**。
  * `compose.server.yaml` 与 `compose.live.accounts.yaml` 中各容器设置的 `mem_limit` 累加达 **5.76 GiB**。
  * 虽然稳态换出速率并不高（`sar -W` 峰值约 0.59 MiB/s），但系统 Commit 长期处于 100%~103% 边缘，一旦突发流量或数据库并发增加，仍易引发瞬时换页脉冲与 I/O 抖动。
* **建议**：
  * 适当收敛非必要的容器 `mem_limit` 预设，为宿主机留出合理的内存缓冲。
  * 调整 `vm.swappiness=10`，避免内核在内存充裕时过早换出数据。

### 瓶颈 2：CPU 密集计算与 250ms 轮询频率
* **容器 CPU 消耗分布**：
  * `crypto-momentum-lab-dashboard-1`：**37.4%**（瞬时聚合消耗）
  * `crypto-momentum-lab-paper-orderflow-pair-1`：**26.6%**
  * `crypto-momentum-lab-market-data-1`：**22.7% ~ 25.3%**
  * `crypto-momentum-lab-postgres-1`：**22.5%**
  * 4 个实盘策略进程：合计约 **10% ~ 15%**
* **分析**：
  * 多个容器配置了 `--poll-interval-seconds 0.25`，单台 2 核服务器在行情剧烈波动时会产生明显的 CPU 排队（Load 超过 2.5）。适当调优非核心服务的轮询间隔可直接释放 CPU。

### 瓶颈 3：磁盘碎片与空间挤占（高确信度优化项）
* `docker system df` 明确显示出巨额可回收资产：
  * **Images**：43 个镜像（活跃仅 3 个），占用 **31.06 GB**，其中 **11.25 GB** 可直接回收。
  * **Build Cache**：312 个缓存层，占用 **15.33 GB**，其中 **11.41 GB** 可直接回收。
  * **合计可释放磁盘达 22.66 GB**，可将磁盘占用从 76% 降至 35% 左右。

### 瓶颈 4：PostgreSQL 时序表增长与数据生命周期（Retention）现状
* **数据现状**：
  * `universe_entries`：**4,746,799 行**（869 MB）
  * `strategy_runtime_events`：**730,124 行**（486 MB）
  * `account_balance_snapshots`：**291,089 行**（580 MB）
  * 数据库总体积 **3.95 GB**。
* **现有 Retention 机制说明**：
  * 代码库在 `execution_account/retention.py` 中**已经配置了账户快照的定期清理机制**（默认 `retention_days = 7`，`equity_retention_days = 370`，每小时批次稀释并清理过期快照，同时保留每种资产的最新一条记录）。
  * 目前真正缺少自动化生命周期裁剪的是 **Universe 数据**（`universe_snapshots` 及级联的 `universe_entries`）与 **策略事件**（`strategy_runtime_events`）。
* **表结构与关联特征**：
  * `universe_entries` 本身**没有 `timestamp` 字段**，其时间记录在父表 `universe_snapshots.observed_at` 中，通过 `snapshot_id` 外键级联（`ondelete="CASCADE"`）。
  * `strategy_runtime_events` 的时间字段为 **`occurred_at`**（非 `event_time`）。

### 瓶颈 5：网络协议栈优化空间
* **现状**：内核仍采用传统的 `cubic` 拥塞控制算法。在东京至交易所跨机房通信时，遇偶发丢包或抖动，`BBR` 能维持更平稳的拥塞窗口并降低重传耗时。

### 瓶颈 6：混部与公网端口安全隐患
* **iperf3 测速端暴露**：宿主机长期运行 `/usr/bin/iperf3 --server --interval 0`，公开监听在 `0.0.0.0:5201` 且无认证，存在被恶意压测和刷流量的风险。
* **混部应用**：宿主机同时跑有 `smart-food-tracker`（Python/Uvicorn，端口 8000），与生产量化服务共用底层系统资源。

---

## 4. 优化实施路线图（精准修正版）

### 阶段一：即时生效（安全无副作用，不重启实盘交易容器）

#### 1.1 清理历史 Docker 镜像与构建缓存（释放 ~22.6GB 磁盘）
```bash
# 安全清理历史构建缓存与 48 小时前未引用的旧镜像
docker image prune -a --filter "until=48h" -f
docker builder prune -a -f

# 验证清理效果
df -h /
docker system df
```

#### 1.2 关闭公网暴露的 iperf3 测速服务（消除安全隐患）
```bash
systemctl stop iperf3
systemctl disable iperf3
# 检查端口监听
ss -tulpn | grep 5201
```

#### 1.3 调优系统内存换页参数（Swappiness）
```bash
# 降低内核换页积极性
sysctl -w vm.swappiness=10
# 持久化
echo "vm.swappiness=10" >> /etc/sysctl.conf
```

#### 1.4 启用 Linux 内核 TCP BBR 算法
```bash
modprobe tcp_bbr
echo "tcp_bbr" >> /etc/modules-load.d/modules.conf

sysctl -w net.core.default_qdisc=fq
sysctl -w net.ipv4.tcp_congestion_control=bbr

echo "net.core.default_qdisc=fq" >> /etc/sysctl.conf
echo "net.ipv4.tcp_congestion_control=bbr" >> /etc/sysctl.conf
```

---

### 阶段二：应用与容器调优（维护窗口执行）

#### 2.1 停用或降频闲置的模拟盘容器
* `paper-orderflow-pair-1` 单个容器占用 CPU 达 **26.6%**。如相关模拟盘已完成验证，可在 Compose 中暂时注释或停止该容器；如需保留，可将轮询间隔提升至 `0.5s ~ 1.0s`。

#### 2.2 适度收缩容器内存上限配额
* 当前物理内存 3.64GB，各容器 `mem_limit` 累加达 5.76GB。可在 Compose 中将 Postgres 调至 768M、实盘策略容器调至 384M，整体配额收缩至 2.8GB 左右，留出 800MB 系统缓冲。

---

### 阶段三：数据库生命周期裁剪方案（Schema 校验版）

> [!CAUTION]
> Universe 数据关联策略回放、冷却判断及重启冷启动语义；执行裁剪前请务必确认业务不需要回溯超出周期的旧快照，且建议先做冷归档或在低峰期分批执行。

#### 3.1 Universe 快照裁剪（基于外键级联）
`universe_entries` 关联于 `universe_snapshots.snapshot_id`，删除父表记录会自动通过 `ON DELETE CASCADE` 清理子表明细：
```sql
-- 分批删除 30 天前的 Universe 快照（示例每次删除 1000 个快照）
WITH doomed_snapshots AS (
    SELECT snapshot_id 
    FROM universe_snapshots
    WHERE observed_at < NOW() - INTERVAL '30 days'
    LIMIT 1000
)
DELETE FROM universe_snapshots
WHERE snapshot_id IN (SELECT snapshot_id FROM doomed_snapshots);
```

#### 3.2 策略运行时事件裁剪
`strategy_runtime_events` 时间字段为 `occurred_at`：
```sql
-- 分批删除 14 天前的运行时事件（建议单事务控制在 5000 条以内）
WITH doomed_events AS (
    SELECT event_id 
    FROM strategy_runtime_events
    WHERE occurred_at < NOW() - INTERVAL '14 days'
    LIMIT 5000
)
DELETE FROM strategy_runtime_events
WHERE event_id IN (SELECT event_id FROM doomed_events);
```

---

## 5. 硬件升级评估

* **当前规格（2C4G）**：在跑满 4 个实盘策略、行情分发、模拟盘及数据库的情况下，CPU 与内存处于**紧平衡上限**。
* **推荐升级规格（4C8G）**：如后续新增实盘账户或扩展更多复杂策略，升级至 4C8G 规格能从根本上消除 CPU 满载排队并解除内存顾虑。
