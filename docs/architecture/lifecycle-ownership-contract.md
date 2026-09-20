# 运行时资源生命周期所有权与进度契约规范 (Lifecycle Ownership & Progress Contract)

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
