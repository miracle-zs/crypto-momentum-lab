# 运行时生命周期、读模型与保留期契约规范

**版本**: 1.0.0  
**状态**: 现行生效  
**制定日期**: 2026-09-20  
**覆盖模块**: 全系统（`research_collector`、`market_data`、`strategy_runner`、`live_rollout`、`execution_account`、`ops`）

---

## 一、 核心第一性原理

在分布式、多进程与高并发实盘交易系统中，资源泄露、悬空后台任务、伪完成终态以及跨模块进度依赖死锁，是导致系统不可维护与偶发性灾难的根本原因。为彻底收敛此类问题，系统严格推行以下生命周期不变量：

1. **单一权威所有者（Single Authoritative Owner）**：
   - 任何物理/网络资源（数据库连接池、HTTP Client、WebSocket 流、后台 Supervisor Task）在运行期必须且只能由**唯一的父节点**拥有；
   - 严禁同一资源的生命周期在多个管理器中重复注册与重复析构。

2. **创建者负责销毁（Creator Owns Teardown）**：
   - 谁负责分配资源实例，谁就必须负责编排其清理流水线；
   - 若资源所有权转移给子运行时（例如从 `runtime_orchestrator` 转移给 `RuntimeSession`），必须显式交付，外部不得保留平行清理逻辑。

3. **统筹时限预算（Bounded Budgeted Shutdown）**：
   - 停机流程由最外层赋予全局截止时间（`total_deadline`）；
   - 各阶段（`DRAINING` -> `PERSISTING` -> `CLOSING` -> `STOPPED`）依序消费剩余时间预算，任何阶段超时直接跳入下一阶段，严禁单点无界挂死。

4. **解耦的进度契约（Decoupled Progress Contract）**：
   - 区分**独立可执行（Independent Executable）**与**进度滞后（Progress Lagging）**两级状态；
   - 依赖项的滞后只能触发**局部降级（只减风险，禁止新开仓）**，绝不得跨模块阻塞主事件循环或产生锁等待。

---

## 二、 资源所有权树形拓扑 (Ownership Tree)

```
[Daemon / CLI Entrypoint] (进程根所有者，监听系统信号 SIGTERM/SIGINT)
        │
        ▼
 [RuntimeSession] (运行时权威所有者)
   ├── Supervisor (协调监控所有后台核心协程)
   │     ├── Heartbeat Task
   │     ├── Candle Poll Loop
   │     └── User Data Stream Listener
   ├── Checkpoint Engine (持久化检查点写入者)
   └── LiveResourceLifecycle (底层网络与数据库资源集合)
         ├── Database Connection Pools (AsyncEngine / SQLAlchemy)
         ├── Exchange API HTTP Clients (BinanceClient / ResilientClient)
         ├── Market Data Stream Transports
         └── Telemetry / Health Writers
```

### 规约：
- `RuntimeSession` 是运行时资源的唯一权威收敛点。
- 当 `RuntimeSession.run()` 退出或被请求停止时，按严格的拓扑逆序关闭：
  1. **Phase 1: DRAINING**：停止接收新业务指令，通知 Supervisor 停工；
  2. **Phase 2: PERSISTING**：持久化最终检查点，并记录业务终态；
  3. **Phase 3: CLOSING**：关闭网络连接、数据库池与文件句柄；
  4. **Phase 4: STOPPED**：释放内部锁，标记健康状态为 STOPPED，生成不可变 `ShutdownResult`。

---

## 三、 结构化停机凭证 (ShutdownResult Contract)

任何停机操作必须返回强类型不可变凭证，禁止以 void 或忽略布尔值的方式结束：

```python
@dataclass(frozen=True, slots=True)
class ShutdownResult:
    run_id: str
    drained: bool
    checkpoint_durable: bool
    terminal_recorded: bool
    resources_closed: bool
    failures: tuple[str, ...]
    duration_seconds: float
```

### 语义映射表：

| `checkpoint_durable` | `failures` 是否为空 | 业务终态映射 | 含义解释 |
| :---: | :---: | :---: | :--- |
| **True** | 是 | `COMPLETED` | 正常平稳退役，所有状态与未决命令均已安全持久化 |
| **False** | 任意 | `HALTED` / `RECOVERY_REQUIRED` | **检查点失败**。必须要求人工或恢复流程介入，绝对杜绝伪 COMPLETED |
| **True** | 否（如 close_timeout） | `HALTED` | 状态已保存，但资源关闭超时，记录告警排查 |

---

## 四、 跨模块进度契约 (Progress Contract)

### 1. 三级就绪度定义

```
   ┌─────────────────────────────────────────────────────────────┐
   │                  INDEPENDENT_EXECUTABLE                     │
   │  - 行情烛线在有效新鲜度 SLA 内 (如 <= 60s)                     │
   │  - 成交对账与事实覆盖无未决缺口                                │
   │  --> 行为：允许全量执行（开仓 + 平仓 + 撤单）                  │
   └──────────────────────────────┬──────────────────────────────┘
                                  │
                       依赖落后 / 超过 SLA
                                  │
                                  ▼
   ┌─────────────────────────────────────────────────────────────┐
   │                     PROGRESS_LAGGING                        │
   │  - 行情延迟，或对账流等待补采数据                              │
   │  - 主事件循环绝对不阻塞！不进行无界 await                        │
   │  --> 行为：安全降级（禁止任何新开仓，仅允许合法平仓与撤单）     │
   └──────────────────────────────┬──────────────────────────────┘
                                  │
                       致命异常 / 严重超时 / 心跳丢失
                                  │
                                  ▼
   ┌─────────────────────────────────────────────────────────────┐
   │                          STALLED                            │
   │  - 触发熔断保护，会话进入 HALTED                              │
   └─────────────────────────────────────────────────────────────┘
```

### 2. 核心不变量：
- **无死锁保证**：策略决策主循环不得同步等待上游补齐历史数据。若数据陈旧，直接评定为 `PROGRESS_LAGGING`，瞬时跳过开仓并继续轮询。
- **减风险特权**：在 `PROGRESS_LAGGING` 甚至部分网络异常状态下，退出通道（`ExitLane` / `ExitAllocator`）始终享有第一优先级执行特权，防止因行情滞后而无法止损。

## 读模型分工与保留期水位契约

**版本**: 1.0.0  
**状态**: 现行生效  
**制定日期**: 2026-09-20  
**覆盖范围**: 全系统数据流（`research_collector`、`persistence`、`operator_dashboard`、`ops`、`live_rollout`）

---

### 一、 数据三态分离架构 (Data Triad Architecture)

本系统严格确立数据的三种形态及其边界职责，严禁职责混淆：

```
       [实盘交易 / 采集流]
              │
              ▼ 权威追加写入 (Single Writer)
   ┌────────────────────────────────────────────────────────┐
   │ 1. 在线运行态事实 (Online Operational Facts)           │
   │    - 存储介质：PostgreSQL 运行表 (account_fills, etc.) │
   │    - 访问特征：权威、低延迟、强一致性                   │
   │    - 作用域：当前 Session 重放、对账与故障恢复          │
   │    - 生命周期：受控保留期 (Bounded Retention)           │
   └──────────────────────────────┬─────────────────────────┘
                                  │
         ┌────────────────────────┴────────────────────────┐
         │                                                 │
         ▼ 异步/按需物化投影                               ▼ 连续归档
┌──────────────────────────────────────┐  ┌──────────────────────────────────────┐
│ 2. 读模型与指标 (Aggregated Read Models)│  │ 3. 离线冷归档 (Archived Lakehouse)   │
│    - 存储介质：物化状态快照、预聚合统计 │  │    - 存储介质：Parquet 分区日志      │
│    - 访问特征：轻量、无锁读取、高吞吐    │  │    - 访问特征：只读、防篡改、全历史  │
│    - 作用域：Operator Dashboard / Grafana│  │    - 作用域：长期回测、审计回放、灾备│
│    - 约束：严禁直接扫大表原始交易事实   │  │    - 约束：校验和证明、覆盖连续性证明│
└──────────────────────────────────────┘  └──────────────────────────────────────┘
```

#### 核心规约：
1. **在线表严禁充当无限存储队列**：
   - 数据库在线表只为运行态控制平面服务。若不设上限，B-Tree 索引膨胀将直接摧毁撮合与持久化尾部延迟。
2. **Dashboard 严禁大范围扫描原始成交全表**：
   - 任何面向运维界面的 API 只能查询物化聚合读模型或带时间边界的窄索引，杜绝无分页的全表 SELECT。
3. **归档必须具备“可回放性证明”**：
   - 数据落入 Parquet 并不等同于完成归档；必须具备校验和完整性与时间线连续性，方可标记归档完成。

---

### 二、 保留期消费者水位保护契约 (Retention Watermark Safety Contract)

#### 1. 传统基于时间删除（Naive Time Cutoff）的致命缺陷
过去使用类似 `DELETE WHERE observed_at < NOW() - 7 days` 的固定策略存在严重第一性原理漏洞：
- 若用户在 8 天前建立了一个仓位，且该持仓当前依然活跃（Active Episode）；
- 或者上游网络中断导致归档消费者的水位落后；
- 粗暴的时间裁剪会**直接抹除该活跃持仓的开仓成交事实**，导致系统下次崩溃重启重放时，因缺失事实而将合法持仓判定为“未对齐异常”或直接归零。

#### 2. 水位约束保护机制 (Watermark Protection Gating)

在对任何在线表执行截断或批量清理前，必须调用领域服务 `RetentionWatermarkEvaluator`：

$$\text{Effective Cutoff} = \min\left(\text{Requested Policy Cutoff}, \min_{c \in \text{Consumers}}(\text{Consumer Watermark}_c)\right)$$

```
  时间轴: ─────────┬───────────────────────┬──────────────────────────────▶ 现在
                   │                       │
         [活跃持仓最早成交水位]       [策略计划裁剪 Cutoff]
          (Consumer Watermark)       (Requested Cutoff)
                   │
                   ▼
         【安全裁剪保护边界 (Effective Cutoff)】
         ◀── 允许安全物理删除 ──┤─── 严格保护，禁止删除 ────────────────▶
```

#### 3. 必须注册的消费者约束项：
1. **`active_position_batches`**：系统中所有未平仓 Position Episode 的最早批次 `opened_at`；
2. **`unresolved_orders`**：所有未终结（非 FILLED/CANCELED/REJECTED）订单的 `created_at`；
3. **`archive_journal_flushed`**：未压缩并持久化至冷存储的最新 Journal 水位。

若任何消费者约束早于策略计划 Cutoff，系统**自动将实际裁剪截止时间收敛至最老消费者的水位**，并记录保留理由，绝对阻止活跃业务事实被意外物理删除。

---

### 三、 运行期配置真实性透明化 (Runtime Configuration Transparency)

为了消除“多容器/多进程配置分支不一致”导致的幽灵问题，系统规约所有守护进程必须在启动时输出结构化的不可变元数据快照 [`RuntimeMetadataSnapshot`](../../src/crypto_momentum_lab/domain/operational/runtime_metadata.py)：
- 固化环境 (`environment`) 与账户 (`account_label`)；
- 固化源码 Git Commit SHA 与代码世代号 (`code_generation`)；
- 固化策略输入哈希 (`strategy_config_hash`)、风控限制哈希 (`risk_config_hash`) 与交易所交易规则哈希 (`trading_rules_hash`)。
- 启动元数据落盘并在启动事件中发布，作为运行期所有决策轨迹（`DecisionTrace`）的基准凭证。
