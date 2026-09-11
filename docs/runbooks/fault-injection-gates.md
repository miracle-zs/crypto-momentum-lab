# Fault-Injection Release Gates

本 runbook 固定一组可重复、进程内的故障剧本。它们复用 Hub、订单状态机、Live submission 和账户同步的既有 Adapter seam，不依赖真实交易所、真实 PostgreSQL 或不可控的 chaos 平台。

## 执行命令

```bash
.venv/bin/python -m pytest -q tests/e2e/test_fault_injection_scenarios.py
```

发布前要求 5 个场景全部通过。单独运行某个场景时，可追加 `-k` 和测试名；如果测试需要修改，必须保持同一故障条件和安全结果，不要把断言改成“没有抛异常”这种弱门禁。

## 固定场景与通过条件

| 场景 | 注入点 | 必须证明的结果 |
|---|---|---|
| market Hub 断连、序列空洞、replay 窗口外 | fake WebSocket 先发送 `1 → 3`，重连时返回不可重放窗口 | 消费者保留最后连续序列 `1`，随后抛出 `MarketStateHubReplayUnavailable`；不接受新的序列起点 |
| `SUBMITTING` 中途 SIGTERM | 在 exchange submit await 期间取消任务 | durable 状态停在 `SUBMITTING`；重启后的新 state machine 只做 `query`，查询到 `FILLED` 后收敛，不重复 submit |
| operator halt 与 entry POST 并发 | candidate 已通过前置检查，在 coordinator prepare 边界前切换 entry gate | 不写 `prepare_submission`，不触发 exchange write，返回 `None` |
| 账户 WS 溢出、deferred buffer、full recovery | 让账户接收队列溢出，同时在 REST reconcile 期间送达用户数据事件 | 溢出触发 full-snapshot recovery；full snapshot 清除 recovery 状态；reconcile 期间的事件进入 deferred buffer，并在恢复后只 replay 一次 |
| exchange event time 倒退 | 先应用较新的订单事件，再送达本地接收时间更晚但 exchange time 更旧的事件 | 旧事件不覆盖新订单状态，并返回 `needs_reconciliation= True` / `stale_exchange_event` |

## 故障后的操作解释

- `MarketStateHubReplayUnavailable`、`AccountEventHubSequenceGap` 或 `stale_exchange_event` 是 fail-closed 信号，不是允许继续交易的软告警。
- `SUBMITTING` 是可恢复的 durable 中间态；恢复流程必须先查询既有 `client_order_id`，不能凭“submit 请求被取消”推断订单不存在。
- operator halt 只需要阻止尚未跨过 prepare seam 的 entry；已经进入 durable `SUBMITTING` 的订单仍按订单状态机和 reconcile 规则处理。
- 任何 gate 失败都应保留可观察的 sequence、recovery reason 或 order event，方便与运行时 telemetry 对齐。

## 代码入口

- 测试：`tests/e2e/test_fault_injection_scenarios.py`
- 市场 Hub seam：`src/crypto_momentum_lab/market_data/hub.py`
- 账户 Hub seam：`src/crypto_momentum_lab/execution_account/hub.py`
- 订单恢复 seam：`src/crypto_momentum_lab/execution_account/orders/state_machine.py`
- live entry seam：`src/crypto_momentum_lab/live_rollout/submission.py`
