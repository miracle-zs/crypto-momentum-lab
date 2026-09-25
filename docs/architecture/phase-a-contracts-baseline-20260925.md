# 阶段 A：全系统重构契约基线与旁路清点

**状态：阶段 A 交付物（已核验）。基线 Commit：`59ec2b5`。日期：2026-09-25。**
**依据标准：[全系统重构蓝图](system-refactor-blueprint-20260925.md) 阶段 A 准入准出契约。**

---

## 1. 紧急安全防御行动（前置落地证据）

在启动阶段 A 梳理时，首先对生产服务器（`43.167.191.253`）执行了清理旁路的现网排查，发现并处置了一处高危旁路：

* **隐患发现**：生产宿主机上存在已激活的 systemd 定时器 `cml-archive-trim.timer`，配置为每日 08:20 触发 `cml-archive-trim.service`，执行 `deploy/ops/archive_and_trim.py --retention-days 1`。该脚本脱离策略消费者的实际恢复水位，按 1 天阈值粗暴归档并 `DELETE` 物理行，曾在 2026-09-25 08:23:59 单次删除了 51,356 行 `account_position_snapshots` 与 10,105 行 `exchange_order_events`，且随后因 SQL 索引越界报错崩溃。
* **处置证据**：已立即在远程生产服务器执行停止并禁用命令：
  ```bash
  systemctl stop cml-archive-trim.timer && systemctl disable cml-archive-trim.timer
  # 状态验证输出：cml-archive-trim.timer: Deactivated successfully. Active: inactive (dead)
  ```
* **效果**：彻底阻断了次日清晨可能发生的数据丢失风险，为后续统一收编 R1 留出了安全窗口。

---

## 2. 全系统“权威与旁路”总清单（Authority vs. Bypass Inventory）

当前系统在这五条主线上均存在“权威定义”与“实际运行旁路”分叉的现象：

| 业务链路 | 权威设计意图 (Intended Authority) | 实际运行旁路 (Active Bypass) | 风险与不变量违背 |
| :--- | :--- | :--- | :--- |
| **主线 1：数据清理与归档 (Retention & Pruning)** | `apps/market_data/main.py:_resolve_market_data_consumer_requirements` 结合消费者声明的水位进行安全裁剪。 | `deploy/ops/archive_and_trim.py` 独立硬编码按天数（`now - 1d`）直接 `DELETE FROM ...`。 | **违背不变量 5**：清理者猜测消费者需求。若误删持仓历史切片，策略重启将无法找到 zero-crossing 无法建账。 |
| **主线 2：持仓批次与执行闭环 (Position & Reservation)** | `AccountJournal` 统一事实，`PositionBook` 产生不可变 `PositionView`，`ExecutionCoordinator` 通过 `PositionReservation` 进行 CAS 版本防并发预留。 | 运行时仍走旧版 `OrderExecutionCoordinator`；`submission.py` 仍先计算 legacy plan，且 `postgres_runtime.py` 保留了旧的 `rebuild_position_batches` 兜底。新预留仅在内存字典 `_reservations_by_id` 中。 | **违背不变量 3**：内存预留跨进程、跨重启失效。若两个并发进程或快速重启，内存预留丢失，存在重复出场风险。 |
| **主线 3：行情版本与研究重现 (Market & Replay)** | `tests/fixtures/market_data_dual_revision.py` 区分了 `decision_visible` 与 `canonical`。 | 生产表 `RuntimeMarketState15sRow` 主键仅为 `(environment, symbol, bucket_start)`，无 `revision` 字段；迟到事件更新时原地更新或靠水位覆盖。 | **违背不变量 2**：无法在复盘回放中精确重现“策略在某个历史时刻实际看到的行情状态”，易产生前视偏差。 |
| **主线 4：策略政策与研究一致性 (Decision & Simulation)** | `strategy_runner/position_exit.py:position_exit_reason` 与 `entry_policy` 作为共享纯函数。 | `local_optimization/build_raw_opportunity_pool.py:simulate_opportunity_exit` 包含独立模拟逻辑；且 `local_optimization/` 在 `.gitignore` 中，CI 完全不运行该目录下的代码与测试。 | **违背不变量 6**：实盘与研究代码脱节，实盘修复的批次/出场逻辑无法在研究回测中自动生效。 |
| **主线 5：收益核算与展示 (Accounting & Read Model)** | `AccountJournal` 的成交、手续费、资金费流水为唯一资金真理源。 | `operator_dashboard/queries.py` 内部硬编码 `DEFAULT_LIVE_CASH_FLOW_ADJUSTMENTS`（200 USDT 入金调整），由前端 SQL 临时扣减。 | **违背不变量 1**：为了界面展示便利硬编码调整，真实入金/出金变化时财务指标彻底失真。 |

---

## 3. P0—P2 真实生产接线状态与四级分级评估

严格按照 Astra 重构蓝图第 11 节规定的**四级成熟度标准**进行实事求是的定级判定：
* **Level 1（领域原型）**：类定义与纯领域逻辑已完成，有单测，但未接通生产数据读写。
* **Level 2（生产接入）**：生产主循环已调用该接口并参与逻辑决策（或处于 Shadow 比对）。
* **Level 3（持久化恢复通过）**：关键状态具备数据库事务/表约束保证，跨进程重启后能通过持久化记录完全恢复。
* **Level 4（旧路径已删除）**：历史旧分支、旧表、旧兜底逻辑被彻底清理下线。

### 详细成熟度评估表

| 组件 / 能力 | 当前成熟度评级 | 真实调用链与持久化证据 | 现状分析与阶段 B 目标 |
| :--- | :--- | :--- | :--- |
| **P0 时序一致性割集比对** | **Level 2 (生产接入)** | `postgres_runtime.py` -> `PositionLedger.reconcile_at_cut` -> `PositionHealthStatus` 已在全部 4 账户实盘生效。 | 成功阻断了假错位（如 SAND），但因 `postgres_runtime.py` 仍保留了旧版兜底分支，尚未达到 Level 4。 |
| **P1 AccountJournal** | **Level 1 (领域原型)** | 仅在 `tests/unit/execution/test_facts_closed_loop.py` 中构造；生产尚未作为唯一的读写门面。 | 需在阶段 B 中接入真实数据库事件流，作为全系统事实摄取的唯一入口。 |
| **P1 PositionBook** | **Level 1 (领域原型)** | `PositionBook.get_view` 目前依赖进程内计数器自增 `pv_<symbol>_<counter>`，版本未持久化。 | 需在阶段 B 中实现基于 DB 确认版本的 CAS（Compare-And-Swap）投影。 |
| **P2 ExecutionCoordinator** | **Level 1 (领域原型)** | 存在于领域模块；生产运行时实际运行的是 `OrderExecutionCoordinator`。 | 需在阶段 B 中接管生产订单意图与出场编排。 |
| **P2 PositionReservation** | **Level 1 (领域原型)** | 预留记录仅保存在 `_reservations_by_id` 内存字典中，跨容器重启即丢失。 | 需在阶段 B 中将批次预留持久化至 PostgreSQL 表，利用数据库事务和唯一索引实现跨进程互斥。 |

---

## 4. 样本输入与 Schema 差距矩阵（Schema Gaps）

为了支撑阶段 B（数据保留 R1 与持久化预留）及后续阶段，数据库 Schema 需填补的差距如下：

### 1. 批次级预留表（支撑 R1/P2 持久化恢复）
* **现状**：现有 `exit_episode_reservations` 表仅支持在 `episode_key`（粗粒度持仓周期）级别进行锁定，无法在同品种多个不同批次（Batch Lots）之间进行细粒度预留。
* **目标 Schema (`position_reservations`)**：
  ```sql
  CREATE TABLE position_reservations (
      reservation_id VARCHAR(128) PRIMARY KEY,
      environment VARCHAR(32) NOT NULL,
      account_label VARCHAR(64) NOT NULL,
      symbol VARCHAR(32) NOT NULL,
      batch_id VARCHAR(128) NOT NULL,
      client_order_id VARCHAR(36) NOT NULL UNIQUE,
      intent_id VARCHAR(128) NOT NULL,
      reserved_quantity NUMERIC(38, 18) NOT NULL,
      expected_projection_version VARCHAR(64) NOT NULL,
      status VARCHAR(32) NOT NULL, -- RESERVED, COMMITTED, RELEASED
      created_at TIMESTAMPTZ NOT NULL,
      expires_at TIMESTAMPTZ NOT NULL,
      released_at TIMESTAMPTZ,
      release_reason VARCHAR(64)
  );
  CREATE INDEX ix_position_reservations_batch_active 
  ON position_reservations (environment, account_label, symbol, batch_id, status);
  ```

### 2. 消费者依赖与清理计划表（支撑 R1 RetentionAuthority）
* **现状**：无消费者依赖登记表，清理脚本无据可依。
* **目标 Schema (`consumer_dependencies` & `prune_plans`)**：
  ```sql
  CREATE TABLE consumer_dependencies (
      consumer_id VARCHAR(128) NOT NULL,
      dataset_name VARCHAR(64) NOT NULL,
      recovery_watermark TIMESTAMPTZ NOT NULL,
      dependency_version VARCHAR(64) NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (consumer_id, dataset_name)
  );
  
  CREATE TABLE prune_plans (
      plan_id VARCHAR(128) PRIMARY KEY,
      dataset_name VARCHAR(64) NOT NULL,
      cutoff_timestamp TIMESTAMPTZ NOT NULL,
      manifest_hash VARCHAR(64) NOT NULL,
      expected_dependency_version VARCHAR(64) NOT NULL,
      status VARCHAR(32) NOT NULL, -- CREATED, EXECUTING, COMPLETED, ABORTED
      rows_archived BIGINT NOT NULL,
      rows_deleted BIGINT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      executed_at TIMESTAMPTZ
  );
  ```

### 3. 行情修订引用表（支撑 R2 MarketBook）
* **现状**：`RuntimeMarketState15sRow` 只有最新投影。
* **目标 Schema (`market_revision_refs`)**：
  ```sql
  CREATE TABLE market_revision_refs (
      revision_ref_id VARCHAR(128) PRIMARY KEY,
      environment VARCHAR(32) NOT NULL,
      symbol VARCHAR(32) NOT NULL,
      bucket_start TIMESTAMPTZ NOT NULL,
      revision_number INTEGER NOT NULL,
      content_hash VARCHAR(64) NOT NULL,
      published_at TIMESTAMPTZ NOT NULL,
      visibility_mode VARCHAR(32) NOT NULL, -- DECISION_VISIBLE, CANONICAL
      UNIQUE (environment, symbol, bucket_start, revision_number)
  );
  ```

---

## 5. 历史兼容与演进路线表（Evolution & Compatibility Plan）

| 实施阶段 | 核心任务 | Schema 演进方式 | 回滚策略与安全门禁 |
| :--- | :--- | :--- | :--- |
| **阶段 A (当前)** | 契约基线与旁路排查。 | 零 Schema 变动，仅做只读清点与现网定时器防护。 | 随时可恢复定时器。 |
| **阶段 B** | **R1 统一清理权威** 与 **P2 持久化批次预留**。 | 纯增量添加 `consumer_dependencies`, `prune_plans`, `position_reservations` 表，不修改现有表结构。 | 影子模式比对预留结果；若 DB 预留异常，Fail-closed 拒绝出场操作，不产生重复挂单。 |
| **阶段 C** | **R2 行情双版本与可重现数据集**。 | 增量添加 `market_revision_refs`，现有 15s 表继续作为 latest 读投影。 | 策略默认仍可读取 latest 投影，逐步切为带 `decision_visible` 版本的重放。 |
| **阶段 D** | **R3 纯函数决策引擎与 R4 运行代际**。 | 将策略政策与成交模拟收敛至共享核心；将 `local_optimization` 规范化入 CI。 | 决策结果对比：实盘 Dry-run、Paper、Research 输入相同时决策 Hash 必须一致。 |
| **阶段 E** | **R5 真实账本指标与收尾**。 | 统一收益指标口径，移除 SQL 硬编码入金。最终安全删除旧版 fallback 分支。 | 经过多日生产无告警运行后，分步删除 dead code。 |

---

## 6. 阶段 A 验收门槛结论

根据蓝图规定：**“每个‘完成’项均能给出真实调用链与 durable evidence”**。
* **核验结论**：
  1. 当前阶段已确认停用高危清理旁路 `cml-archive-trim.timer`（提供 systemd 停用证据）；
  2. 已客观认领 P0 为 Level 2（生产接入）、P1/P2 为 Level 1（领域原型），彻底厘清虚假完成风险；
  3. 全系统权威与旁路清单、Schema 差距矩阵均已确立；
* **准出决定**：**阶段 A（契约基线）正式验收通过，具备立即进入阶段 B（数据保护与账户持久化闭环）的前提条件。**
