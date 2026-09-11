# Replay / Paper / Live execution semantics

这份文档把 Replay、Paper 和 Live 的执行语义固定成发布前必须知道的
差异清单。它不是要求三条路径共享一条执行实现；它定义哪些差异是故意的，
以及哪些结论不能从模拟环境外推到真实交易。

## 范围与判定口径

- **Replay**：`strategy_runner/replay.py` 通过
  `strategy_runner/fills.py::simulate_candidate_fills` 对一组历史
  `MarketState15s` 做确定性回放。
- **Paper**：包括批处理 `strategy_runner/paper.py::run_paper_trading`，
  以及在线 Paper 的
  `strategy_runner/daemon.py::run_paired_paper_live_daemon`。两者共享
  `ReplayExecutionConfig`、`SimulatedFill` 和 `PaperPosition`，但在线版本
  还会持久化 checkpoint、candidate、fill 和 portfolio。
- **Live**：`live_rollout/entry_lane.py`、
  `live_rollout/submission.py`、
  `execution_account/orders/state_machine.py` 与
  `live_rollout/exits.py` 的真实交易路径。PostgreSQL 是执行屏障和恢复所需
  的 durable adapter，Binance 是订单状态的外部权威。

这里的“等价”只表示对同一个概念提供相同的安全保证；不把模拟成交价格、
模拟费用或模拟持仓记录误称为交易所订单生命周期。

## 语义对照表

| 语义 | Replay | Paper | Live | 发布含义 |
|---|---|---|---|---|
| 候选到期 / GTD 本地撤销 | `candidate.expires_at` 只决定模拟 fill 是 `EXPIRED`；没有订单，也没有 cancel | 与 Replay 相同；批处理在输入结束时可能留下 `PENDING`，在线 Paper 会在后续 state 中把过期候选解析为 `EXPIRED`；没有 exchange cancel | LIMIT entry 在提交时转成 `GTD + expires_at`；`LiveLimitOrderLifecycle` 在本地启动/恢复计时器，到期调用 cancel。Binance 的 GTD 是最终权威，本地 cancel 是 prompt best-effort | 模拟只能证明 TTL/候选筛选；不能证明撤单、撤单未知结果或交易所过期行为 |
| 持仓批次 / 平仓边界 | 没有 portfolio/position；报告只有 signals、candidates、simulated fills | `PaperPosition` 按每个完整 `SimulatedFill` 建立，支持 open/closed、TP/SL、15m candle 与 grace recovery；不按 reduce-only order boundary 重建交易所聚合仓位 | `ManagedLivePosition` 保留账户聚合视图，`ManagedLivePositionBatch` 按 entry fill 与 reduce-only boundary 分批，分别保留 entry anchor、remaining quantity 和 recovery 状态 | Paper 的 position PnL 可用于策略比较，但不能作为 Live batch attribution 或平仓数量正确性的证明 |
| Partial fill / recovery | `SimulatedFill` 只有 filled/expired/rejected/pending，没有 executed quantity 的增量订单状态 | 同一 `SimulatedFill` 模型；只有完整 fill 才生成 `PaperPosition`。candle grace 的 recovery limit 是价格规则模拟，不是 partial-fill order recovery | `ExchangeOrderState.PARTIALLY_FILLED`、fill ledger 和 `executed_quantity` 会持久化；exit recovery 只覆盖剩余仓位，`recovery_order_remaining_quantity` 由 Live batch 保留 | 任何 partial fill、未知提交结果、重启后 reconcile 的结论都必须由 Live / fake exchange 测试提供 |
| exposure claim / max positions 仲裁 | 无账户风险状态、无 max-position 仲裁 | `PaperEntryFilterConfig` 与 portfolio 负责模拟过滤和持仓展示；没有 Live 的 DB exposure claim 事务 | Live pre-submit 同时使用内存 pending/open view 与 PostgreSQL `live_exposure_claims`；按 account/strategy 的 advisory lock 原子仲裁 max positions、gross exposure、daily loss，并在 terminal order state 释放 claim | Paper 通过不代表并发 Live entry 能通过 exposure arbitration |
| 信号衰减 / 成交时 edge 复核 | 只按 `latency_buckets` 找到目标 state；不重新评估 signal edge | 候选在产生时经过 entry filter；后续 `resolve_candidate_fill_at_state` 只解析模拟成交，不重新执行 entry policy 或 edge threshold | 提交前检查 candidate expiry、context generation、risk gate 和 durable lease/claim；没有在实际成交时重新运行 orderflow edge。GTD 到期后不再保护，但挂单成交本身不重新验证原信号 | 当前契约是“提交时安全 + TTL”，不是“成交时 edge 仍然成立”；若产品需要后者，必须单独定义 threshold 与拒单/撤单语义 |
| 成交延迟与价格 | `ReplayExecutionConfig` 显式控制 latency、fee、slippage、quote requirement；默认 latency 为 1 个 state bucket | 使用同一模拟执行配置；某些 Paper profile 可以显式设为 0 latency，因此不能只看类默认值 | 使用真实 wall-clock、pacer、交易所响应和实际 fill；不存在一个可直接等价的 simulated latency 参数 | 对比报告必须记录 execution config；Paper/Replay 的价格优势不能外推为 Live edge |

## 已确认的实现不变量

### 候选到期不是撤单等价物

Replay/Paper 的 `simulate_candidate_fill` 在目标时间没有可用 state 时返回
模拟的 `EXPIRED` 或 `PENDING`。这表示“模拟器不再产生 fill”，不会向任何
交易所发出请求，也不会产生 `CANCELING`、`UNKNOWN_PENDING_RECONCILIATION`
或 `ABSENT_RECONCILED` 等真实订单状态。

Live LIMIT entry 的调用顺序则是：

1. `EntryExecutionLane` 在执行前拒绝已经过期的 candidate；
2. `LiveCandidateSubmission` 生成带 `GTD` 和 `expires_at` 的 quantized plan；
3. durable submission 之后由 `LiveLimitOrderLifecycle` 跟踪未完成订单；
4. 本地 timer 到期调用 state machine 的 cancel，并保留未知结果等待 reconcile。

因此，Paper 的“expired fill”与 Live 的“exchange order expired/canceled”是
两个不同的观察结果，runbook 不得用前者替代后者。

### Paper 的 recovery 与 Live 的 recovery 不是同一层

Paper portfolio 中的 candle grace recovery 会根据 mark/candle 是否触及恢复价
或超时，直接生成一个关闭后的 `PaperPosition`。它验证的是退出决策规则。

Live exit recovery 面向已经存在的交易所仓位：它需要知道当前 aggregate quantity、
每个 batch 的 entry anchor、历史 reduce-only order、已成交数量和 recovery limit
的剩余覆盖量，然后只对 uncovered quantity 生成新的 recovery order。它验证的是
订单状态、持仓边界和恢复编排。

### Exposure claim 只有 Live 有跨协程/跨进程仲裁语义

Paper 可在一个模拟账户内保存多个 `PaperPosition`，但不会在提交前以数据库事务
占用一个可恢复的 exposure claim。Live 的 claim 位于 order preparation transaction
中，并与 advisory lock、当前 open symbols、active claims 和 risk limits 一起判断。
这是从单进程模拟到多 worker/重启场景的关键语义增量。

## 发布门禁

以下结论可以由 Replay/Paper 证据支持：

- strategy signal/candidate 的确定性；
- candidate TTL、模拟 latency、fee/slippage 和 quote 缺失时的结果；
- Paper portfolio 的 TP/SL、candle confirmation、grace 与 checkpoint 恢复。

以下结论必须由 Live 或 fake-exchange 证据支持，不能用 Paper 绿灯替代：

- GTD 到期后的本地撤单、撤单未知结果与重启恢复；
- `PARTIALLY_FILLED` 后的 fill ledger、剩余数量和 recovery order；
- 多次 entry fill 与 reduce-only boundary 的 batch attribution；
- 并发 entry 的 exposure claim / max positions 仲裁；
- 提交时通过、成交时信号已经衰减的当前处理契约。

当前选择是保留这些执行语义差异，而不是强行让 Paper 假装成 Live。任何
需要“Paper 与 Live 可互换”的产品结论，都必须先补齐对应的 execution adapter
和测试契约。

## 现有测试证据

- `tests/unit/strategy_runner/test_fills.py`、`test_paper.py`、`test_replay.py`：
  模拟 latency、expiry、fill、pending/rejected 与 Paper position 行为。
- `tests/unit/live_rollout/test_entry_orders.py`、`test_daemon.py`、`test_exits.py`：
  GTD timer、过期 candidate、batch/recovery exit 与 partial/reconcile 相关行为。
- `tests/unit/execution_account/orders/test_state_machine.py`：
  partial fill、cancel 后的执行数量和未知订单结果。
- `tests/integration/persistence/test_order_repository.py`：
  Live exposure claim 的原子占用与 terminal release。
