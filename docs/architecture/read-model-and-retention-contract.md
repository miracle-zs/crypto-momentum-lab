# 读模型分工与保留期水位契约规范 (Read Model Architecture & Retention Watermark Contract)

**版本**: 1.0.0  
**状态**: 现行生效  
**制定日期**: 2026-09-20  
**覆盖范围**: 全系统数据流（`research_collector`、`persistence`、`operator_dashboard`、`ops`、`live_rollout`）

---

## 一、 数据三态分离架构 (Data Triad Architecture)

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

### 核心规约：
1. **在线表严禁充当无限存储队列**：
   - 数据库在线表只为运行态控制平面服务。若不设上限，B-Tree 索引膨胀将直接摧毁撮合与持久化尾部延迟。
2. **Dashboard 严禁大范围扫描原始成交全表**：
   - 任何面向运维界面的 API 只能查询物化聚合读模型或带时间边界的窄索引，杜绝无分页的全表 SELECT。
3. **归档必须具备“可回放性证明”**：
   - 数据落入 Parquet 并不等同于完成归档；必须具备校验和完整性与时间线连续性，方可标记归档完成。

---

## 二、 保留期消费者水位保护契约 (Retention Watermark Safety Contract)

### 1. 传统基于时间删除（Naive Time Cutoff）的致命缺陷
过去使用类似 `DELETE WHERE observed_at < NOW() - 7 days` 的固定策略存在严重第一性原理漏洞：
- 若用户在 8 天前建立了一个仓位，且该持仓当前依然活跃（Active Episode）；
- 或者上游网络中断导致归档消费者的水位落后；
- 粗暴的时间裁剪会**直接抹除该活跃持仓的开仓成交事实**，导致系统下次崩溃重启重放时，因缺失事实而将合法持仓判定为“未对齐异常”或直接归零。

### 2. 水位约束保护机制 (Watermark Protection Gating)

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

### 3. 必须注册的消费者约束项：
1. **`active_position_batches`**：系统中所有未平仓 Position Episode 的最早批次 `opened_at`；
2. **`unresolved_orders`**：所有未终结（非 FILLED/CANCELED/REJECTED）订单的 `created_at`；
3. **`archive_journal_flushed`**：未压缩并持久化至冷存储的最新 Journal 水位。

若任何消费者约束早于策略计划 Cutoff，系统**自动将实际裁剪截止时间收敛至最老消费者的水位**，并记录保留理由，绝对阻止活跃业务事实被意外物理删除。

---

## 三、 运行期配置真实性透明化 (Runtime Configuration Transparency)

为了消除“多容器/多进程配置分支不一致”导致的幽灵问题，系统规约所有守护进程必须在启动时输出结构化的不可变元数据快照 [`RuntimeMetadataSnapshot`](file:///Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/domain/operational/runtime_metadata.py)：
- 固化环境 (`environment`) 与账户 (`account_label`)；
- 固化源码 Git Commit SHA 与代码世代号 (`code_generation`)；
- 固化策略输入哈希 (`strategy_config_hash`)、风控限制哈希 (`risk_config_hash`) 与交易所交易规则哈希 (`trading_rules_hash`)。
- 启动元数据落盘并在启动事件中发布，作为运行期所有决策轨迹（`DecisionTrace`）的基准凭证。
