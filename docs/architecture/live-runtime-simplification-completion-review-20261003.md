# 实盘简化改造完成度检查

- 检查日期：2026-10-03。
- 比较范围：`db3c3761...f8e5a300`，P0–P6 七个实施提交。
- 原始检查结论：有实现和回归，但未满足全部计划验收条件。下列原始发现已完成本地修正，详见文末逐步修正记录；生产验收仍待完成。
- 本次只检查及记录，没有修改交易代码、部署、发送通知或下单。

## 计划落实检查（Spec）

1. **P3 未贯通：未知订单仍触发全账户准入拒绝。** `live_rollout/gates.py:58` 对全账户 `unresolved_order_states` 执行 `any`；`market_admission.py:91` 将这些状态原样传入；`market_loop.py:358` 在 transient gate 分支中直接跳过提交。即使 A 的未知订单风险可以界定，B 也到不了 Submission 新增的局部准入逻辑。启动装配路径仍使用同一全局门禁。需要从实际行情入口验证跨标的继续提交，不能只测试 Submission。
2. **P5 改变了未约定的策略时效。** `exit_processor.py:225` 硬编码 `candle_end + 15 minutes` 后失效；`exits.py:506` 等处用当前时间生成候选，`:reval:` 时间戳参与身份，候选 TTL 从重评时间开始。旧策略有 `candidate_ttl_seconds`，本次未找到原有的 15 分钟信号有效窗口依据。计划要求按既有策略时效重评、不暗改策略规则；应明确原规则或单独记录并确认新规则，再验证重复/重启/未知结果下的幂等。
3. **P5 的 S04/S05 测试缺少实际执行链路。** `test_closed_candle_decoupling_replay.py:72` 等测试用假 daemon 手写候选身份、过期和投影更新行为；它验证了通道调度，不能证明真实 ExitProcessor → ExitManager → Submission → Coordinator 的零重复 POST、旧分配拒绝及新分配提交。
4. **P4 仍有旧状态写入和运行中的模式管理。** `execution_account/sync.py:500` 在 `persist=True` 同步失败时仍保存 DEGRADED，`:213` 仍发布 SYNCING；`risk/gateway.py:198` 和 `gates.py:51` 对非 RUNNING/READY_READONLY 返回 account_not_ready。运行中的实时 fetch-only 恢复不一定经过该写入，不能声称每次 REST 故障都会降级；但旧状态仍不是仅历史读取兼容。`readiness.py` 仍创建并使用 TradeabilitySnapshot、TradeabilityAlertManager，且 `not exit_gate_open` 生成 HALTED。计划要求删除该额外业务模式及恢复管理，尚未完整落实。
5. **P6 的后台归档保存失败未保留待处理项。** `apps/market_data/main.py:1310` 捕获保存异常后仅日志记录，随后 task_done，未重试或持久保留；停机等待 worker 完成无法证明元数据保存成功。对生产源码内两个实际嵌套函数做 AST 提取故障注入：保存调用 1 次，队列最终 0 项，worker 继续运行，停机成功返回，失败项没有保留。队列满时 `:1320` 又同步 await save_manifest，慢数据库下仍可阻塞生产者，不能宣称已彻底消除该等待。

此外，P6 尚无同负载前后延迟测量；Server 酱真实送达、恢复通知、生产版本及至少一个真实收盘周期未在本次检查中验收。计划顶部仍写“待实施”，实施表却全写“已完成”，应按上述缺口统一状态和证据。

## 结构检查（Standards）

未发现额外仓库编码规范，没有据此认定的硬性规范违规。以下是判断性结构建议：

1. `exit_channels.py:463` 与 `:468` 连续记录完全相同的 live_closed_candle_exit_degraded，一次失败会产生两条日志，污染噪声和事件次数统计。
2. `domain/execution/execution_book.py:2132` 与 `:2650` 重复未成交终态及 reservation 归属判定；建议共享纯函数，避免持久化与内存路径语义漂移。
3. `submission_fence.py:112` 与 `:193` 重复租约到期和时区处理；建议共用校验函数，保留两个必要检查时机。

以上 source 路径均相对 `src/crypto_momentum_lab/`；测试路径相对 `tests/unit/live_rollout/`。

## 本次验证

实际重新执行完整本地回归，启用 Hub 网络测试与本地 PostgreSQL，排除 live 标记：

```text
3356 passed, 1 deselected, 3 warnings in 65.50s
```

3 个警告分别为 Starlette/httpx 弃用、websockets 弃用，以及测试中 aclose 协程未 await 的资源警告。`git diff --check db3c3761...HEAD` 通过。

测试绿色不能补足未覆盖的真实入口和失败路径。Spec 共 5 项，最关键是全局 UNKNOWN 门禁仍挡在局部准入之前；Standards 共 3 项，最直接的实际影响是重复错误日志。

## 逐步修正记录

以下为复查后的工作区修正，保留上文审计发现作为历史证据：

| 原发现 | 修正与验证 |
| --- | --- |
| 全账户 UNKNOWN 门禁 | 前置门禁不聚合未知订单；候选根据具体订单身份、作用标的和可界定风险决定。真实主行情链路覆盖跨标的允许、同标的拒绝、缺失价格拒绝、额度不足拒绝。 |
| 策略时效改变 | 恢复原事件 received_at 和既有 candidate_ttl_seconds；删除 15 分钟硬编码及当前时间重评身份/续期。默认 60 秒 TTL 在等待 224 秒后明确失效，已有 300 秒配置内才允许评估，不改变策略配置。 |
| 假 daemon 回归 | 替换为真实 ExitManager/ExitProcessor/Submission/Coordinator，边界只模拟仓储或交易所；验证旧投影零 POST、新事实数量/allocations 和事件重放零重复提交。 |
| 旧模式管理与状态写入 | 删除 TradeabilityMode、TradeabilitySnapshot、TradeabilityAlertManager。同步故障保留原进程阶段并记录原因；历史软状态不作为总门禁。Dashboard 不把新鲜 RUNNING 证据改写为 HALTED，明确动作停止和异常事实仍保留。 |
| 归档元数据失败 | 复用既有持久化 PendingManifestJournal，生产者只等待本地日志；数据库保存成功才删除。失败、停机超时记录可重启重放；删除 queue-full 同步写库回退及错误的 manifest.path 日志字段。 |
| 重复逻辑 | 删除重复错误日志；共享未成交终态/预留归属判断，以及租约到期校验，保留开仓和减仓两个检查时机。 |

待生产验收：Server 酱测试消息及真实恢复通知、目标版本部署观测、至少一个自然收盘周期、相同负载下的延迟对比。本地测试通过不能替代这些项目。本次没有部署、修改业务数据库或实际下单。

静态验证：8 个核心源文件（含 Dashboard overview）mypy 通过；另外 8 个源文件的检查由 HEAD 基线 17 项降为 16 项，消除归档 manifest.path 错误，其余为既有问题。27 个修改的 Python 文件 Ruff 诊断由基线 117 项降为 89 项，没有增加。包含 TYPE_CHECKING 引用的顶层包和 domain 子包 AST 依赖检查均为 0 环。

最终重新运行完整本地回归（Hub 网络测试和本地 PostgreSQL 启用，排除实盘标记）：**3365 passed, 1 deselected, 3 warnings in 65.47s**。3 个警告与原始检查相同。`git diff --check` 通过。另有实际退出重评/超期/重复投递/投影更新/单次错误事件 6 项测试通过，以及 journal 失败重放、慢数据库与停机超时覆盖。

### 受影响文件

**源代码**

- `src/crypto_momentum_lab/apps/market_data/main.py`
- `src/crypto_momentum_lab/domain/execution/execution_book.py`
- `src/crypto_momentum_lab/execution_account/sync.py`
- `src/crypto_momentum_lab/live_rollout/daemon.py`
- `src/crypto_momentum_lab/live_rollout/exit_channels.py`
- `src/crypto_momentum_lab/live_rollout/exit_processor.py`
- `src/crypto_momentum_lab/live_rollout/exits.py`
- `src/crypto_momentum_lab/live_rollout/gates.py`
- `src/crypto_momentum_lab/live_rollout/plan_runner.py`
- `src/crypto_momentum_lab/live_rollout/readiness.py`
- `src/crypto_momentum_lab/live_rollout/runtime_orchestrator.py`
- `src/crypto_momentum_lab/live_rollout/submission.py`
- `src/crypto_momentum_lab/live_rollout/submission_fence.py`
- `src/crypto_momentum_lab/operator_dashboard/overview_queries.py`
- `src/crypto_momentum_lab/persistence/raw_files/journal.py`
- `src/crypto_momentum_lab/risk/gateway.py`

**测试**

- `tests/integration/raw_files/test_journal.py`
- `tests/unit/execution_account/test_sync.py`
- `tests/unit/live_rollout/test_closed_candle_decoupling_replay.py`
- `tests/unit/live_rollout/test_daemon.py`
- `tests/unit/live_rollout/test_gates.py`
- `tests/unit/live_rollout/test_phase_p6_performance_and_noise.py`
- `tests/unit/live_rollout/test_readiness.py`
- `tests/unit/live_rollout/test_submission.py`
- `tests/unit/live_rollout/test_submission_fence.py`
- `tests/unit/operator_dashboard/test_overview_health.py`
- `tests/unit/risk/test_gateway.py`

**文档**

- `docs/architecture/live-runtime-simplification-plan-20261003.md`
- `docs/architecture/live-runtime-simplification-completion-review-20261003.md`（本报告）

