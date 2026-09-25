# 2026-09-18 审计修复复核

本次复核结论：**原始行情 Hub 计时缺陷已修复并部署；采集器已有缓解；查询与 checkpoint 性能尚未全部闭环。可以开始架构演进，但应先完成恢复语义和生命周期的补漏。**

## 1. 基线与方法

- 采样时间：北京时间 2026-09-18 08:00–08:06 附近（服务器 UTC 00:00 起）。
- 本地 HEAD、服务器 `/opt/crypto-momentum-lab` HEAD、行情/采集器/账户/实盘镜像均为 `2c141687aa59322f0be68ef81d34dfa48fcffa14`。
- dashboard 仍为独立镜像 `72051f1`；这符合原部署设计。
- 只读 SSH、Docker 元数据和日志、只读事务中的 EXPLAIN ANALYZE、本地定向测试和隔离复现。
- 工作区已有未提交的 dashboard、checkpoint 观测展示改动；本次未改动这些文件。运行态结论以服务器日志为准，不能将未提交代码当成已部署功能。
- 未改动生产配置、数据库结构或交易状态，未执行生产故障注入。

原报告：[server-audit-20260918.md](server-audit-20260918.md)。下一步设计：[架构演进实施设计](../architecture-evolution-20260918.md)。

## 2. 逐项结案状态

| 编号 | 原问题 | 代码与部署证据 | 结论 |
|---|---|---|---|
| V1 | MarketState Hub 健康运行时间计入不可用预算 | `24dc45c` 将计时改为可清空的故障区间，成功消费后清空；新增长期健康后短断线、连续不可用测试；已部署 | **原复现已修复**；其他 Hub 不能自动视作一并修复 |
| V2 | research-collector 两个 batch 队列溢出 | 同提交增加可配置队列，collector 默认 128；当前容器自 07:31:44 CST 启动、RestartCount=0，约半小时日志未见 overflow | **缓解已部署**；串行 ingest 和共用丢弃/恢复逻辑仍在，可靠归档演进未完成 |
| V3 | 账户发现查询全表排序 | 改为 `DISTINCT ON`；现网仍 `Seq Scan → Sort → Unique`，扫描 35,445 行，排序约 4583 kB，实测 172.205 ms | **未闭环**；修改 SQL 写法没有消除历史扫描 |
| V4 | checkpoint 延迟归因与同相写入 | `6cec498` 增加 event loop、池状态、新建连接字段；`2c14168` 部署 60 秒周期与 0/15/30/45 秒 phase，状态数阈值 1000 | **功能已部署、性能验收待完成**；不能声称四账户 P95 均改善 |
| V5 | 模块职责与恢复策略拆分 | `WebSocketMarketStateSource` 仍同时承接实时策略与归档；账户发现仍读历史；运行时装配继续依赖具体资源 | **尚未实施**；详见演进设计 |

“当前 healthy”仅代表探针通过，不代表停机恢复、档案无缺口、长期内存稳定均已验收。

## 3. 新确认的漏修点：AccountEvent Hub 同类计时缺陷

位置：`src/crypto_momentum_lab/execution_account/hub.py`，`WebSocketAccountEventSource._iterate`，约 790–902 行。

该客户端仍在握手时设 `unavailable_since`，成功消费不清空。使用真实迭代循环，替换网络、事件输入和模块内时钟进行隔离复现：

```text
event 1
event 2 virtual_time 121
REPRODUCED account-event hub unavailable beyond timeout attempts 1
```

条件：t=0 成功建立连接并收到事件 1；t=121 仍正常收到事件 2；随后注入第一次 OSError。实际直接抛出超时，连接次数仍为 1。

生产关联证据：account-4 在 UTC 23:45:02 队列溢出，23:45:03 触发 snapshot recovery，并记录 `live_account_event_stream_retry`，原因 `account-event hub unavailable beyond timeout`。外层已有重试，因此不能把它描述为 account-4 已崩溃，也不能据此声称丢单；但共享超时修复并未覆盖账户通道。

另外，`market_data/quote_hub.py` 的两处客户端和 `execution_account/risk_control_hub.py` 仍存在“握手设置起点、异常时比较”的同形逻辑。本次未逐一复现，将其列为必须覆盖的候选点，不直接标记为已确认生产故障。

## 4. 生命周期补充证据

- account-3 当前容器 RestartCount=1，最近启动 UTC 23:45:15。
- 保留日志包含 `runtime_supervisor.stop → stop_entry_caches → entry_runtime.stop → entry_symbol_cache.stop → await task → CancelledError`。
- 启动后 UTC 23:45:25–30 出现 4 次 `scheduled_flatten_market_state_unavailable` 及对应 action failed。

这些事实说明取消传播和恢复就绪次序需要专项验收。**尚不能认定该 traceback 是重启的唯一根因，也不能认定四次启动期错误一直持续。** 演进设计要求在扩大切换范围前复现、确定取消所有权，并核对最终恢复状态。

## 5. checkpoint 实测

统计各容器当前启动以来日志，包含启动期，时间窗口不同；P95 取排序数组下标 `min(n-1, floor(0.95*n))`，仅用于描述本次样本。

| 账户 | 样本数 | total 中位数 ms | total P95 ms | acquire 中位数 ms | yield 调度延迟中位数 ms |
|---|---:|---:|---:|---:|---:|
| primary | 69 | 87.09 | 307.365 | 35.30 | 4.01 |
| account-2 | 71 | 108.29 | 248.242 | 46.71 | 4.57 |
| account-3 | 47 | 62.71 | 114.583 | 24.35 | 3.95 |
| account-4 | 70 | 91.77 | 180.885 | 35.15 | 4.25 |

核实四账户参数分别为 phase 0/15/30/45，时间阈值 60 秒、数量阈值 1000。数量阈值仍可额外触发写入，所以不能把 phase 理解为所有写入严格间隔 15 秒。

`event_loop_lag_ms` 实际测量一次 `asyncio.sleep(0)` 的恢复延迟；它不等同于全程 event-loop lag 探针。`pool_acquire_ms` 仍包含完整连接获取过程；`is_new_connection` 基于计数差，只适合辅助归因。优化验收应比较同版本、同负载、剔除启动恢复阶段的相同长度窗口。

## 6. 查询复核

在 `BEGIN READ ONLY` 和 5 秒 statement timeout 下执行当前 DISTINCT ON 查询的 EXPLAIN ANALYZE。返回 4 个最新账户，外层筛选出 1 个有持仓账户；shared hit=953，仍有全表排序。

现有索引为主键、`(environment, account_label, status, observed_at)` 和 `(environment, account_label, observed_at)`，未发现匹配完整排序的新索引。

172 ms 与旧报告 58 ms 属于不同负载时刻，**不能据单次采样宣称变慢 3 倍**。能够确认的是历史扫描仍然存在，因此该项不能结案。

## 7. 本次验证命令与结果

在仓库根目录执行：

```bash
rtk proxy env CML_RUN_HUB_NETWORK_TESTS=1 .venv/bin/python -m pytest \
  tests/unit/market_data/test_hub.py \
  tests/unit/live_rollout/test_checkpoint_coordinator.py \
  tests/unit/live_rollout/test_runtime_options.py \
  tests/unit/research_collector \
  tests/unit/persistence/postgres/test_account_repository.py \
  tests/smoke/test_server_deployment_manifest.py -q
# 62 passed in 1.06s

rtk proxy env CML_RUN_HUB_NETWORK_TESTS=1 .venv/bin/python -m pytest \
  tests/unit/execution_account/test_hub.py \
  tests/unit/live_rollout/test_runtime_supervisor.py \
  tests/unit/live_rollout/test_resource_lifecycle.py \
  tests/unit/live_rollout/test_context.py -q
# 28 passed in 0.30s
```

现有测试共 90 项通过，包含显式启用的 loopback 网络测试；不是全量测试。AccountEvent 的新隔离复现仍然失败于期望的自动重连行为，说明已有测试未覆盖这一情形。本次未执行需要测试数据库的集成测试。

## 8. 可进入下一阶段的判定

现在可以开始编写契约测试、提炼恢复计时、拆分纯持仓重建逻辑、实现只读对照路径。账户通道计时补漏、停机取消复现、查询方案验证列为第一批工作；在这些事项闭环前，不把“全系统稳定验收通过”作为后续架构发布的前提事实。

本文件与设计文件是本地文档；仓库当前忽略 `/docs/` 下的新文件，尚未加入 Git 索引。后续提交时应显式纳入所需文档，不改动现有全局忽略策略。
