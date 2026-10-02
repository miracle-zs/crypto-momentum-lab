# 宽限限价平仓缺少 time_in_force：2026-10-02

## 生产证据

只读查询 `exchange_orders` 与 `exchange_order_events`，发现 UTC 15:15 的四账户 VELVETUSDT reduce-only LIMIT（quantity=1477、price=0.06825）均被拒绝；account-2 UTC 15:00 的 FLUIDUSDT LIMIT（quantity=61.2、price=1.648）同样被拒绝。

事件 reason 均为 `Limit order ... is missing required time_in_force`。这是 `BinanceUsdMTradeClient.submit_order` 在 `_signed_post` 前的本地契约检查，不是交易所拒单，也不是数据库锁或调度阻塞。拒绝后的 recovery gate 为零不能证明订单成功送达。

证据摘要保存在 `grace-limit-tif-evidence-20261002.json`。观察过程中没有人工发单、改价、调整宽限期或风险限额。

## 根因与修复

`LiveExitManager` 按原策略产生宽限 LIMIT 候选。`TradeCommandExecutor.plan_execution` 构造 `OrderExecutionPlan` 时只填价格和数量，没有填 time_in_force；`quantize_order_plan` 的限价生成路径也有同样遗漏。开仓 Submission 会明确覆盖为 GTD，因此之前普通限价开仓没有暴露该缺陷，宽限退出未得到该字段。

在两个计划生产者明确设置 LIMIT 的 `time_in_force="GTC"`，MARKET 保持 None。宽限限价单继续由原有宽限定时器撤单并市价兜底；没有把宽限退出改为立即市价，也没有让交易所适配器静默补默认值。开仓 Submission 的 GTD/到期时间设置保持原逻辑。未修改持久化格式、命令身份、分配数量、事务边界或交易参数。

## 回归验收

新增 `tests/unit/live_rollout/test_grace_limit_exchange_contract.py`：真实不利收盘蜡烛生成宽限退出，经 command/intent 两个实际计划生产者，再调用真实 Binance 客户端与 httpx.MockTransport。修改前两例均失败于与生产一致的缺失 time_in_force；修改后收到领域 ACKNOWLEDGED，模拟请求带 LIMIT、SELL、LONG、GTC、原目标价格，没有 GTD 到期字段。测试不会接触交易所。

之前单测的 RecordingCoordinator 等替身会直接返回 ACKNOWLEDGED，没有执行交易所客户端参数校验；单独的客户端测试又使用人工填好的计划，因此未覆盖生产者与消费者之间的字段遗漏。本次新增测试覆盖这条跨模块契约，而非削弱客户端检查或删除旧断言。

- 退出、Submission、计划生成器、量化与客户端定向 87 项通过。
- 开启 `CML_RUN_HUB_NETWORK_TESTS=1` 的完整 unit/smoke：3243 passed、1 skipped、2 项现有 warning，40.61 秒。唯一跳过项是未配置测试数据库的 live smoke。
- 变更文件 Ruff 与 `git diff --check` 通过。

## 上线与实际交易验收

待部署后记录版本与真实订单状态。正常策略重评和对账负责后续退出；没有人工重放失败命令或重发订单。代码契约测试通过不等于现存仓位已经平掉。
