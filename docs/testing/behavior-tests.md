# 测试与交易行为回归

测试应约束用户可见结果、交易不变量和故障恢复，不绑定私有调用次数、缓存布局或特定实现结构。完整约束见[系统架构与交易契约](../architecture/overview.md)。

|必须验证的行为|主要测试位置|
|---|---|
|排队取消不发单；已发包取消和网络超时保留原订单身份，未知结果不盲目重试|`tests/unit/execution_account/orders/test_coordinator.py`、`tests/unit/execution_account/test_binance_client.py`|
|慢对账不阻断同标的退出或其他标的下单|`tests/unit/execution_account/orders/test_coordinator.py`|
|成交幂等、累计成交水位不倒退；订单与预留原子落库，失败不污染内存|`tests/unit/execution/test_execution_book.py`、`tests/integration/persistence/test_authority_book_transactions.py`|
|多批次成交归属、预留守恒、外部平仓和重启恢复正确|`tests/unit/execution_account/orders/test_coordinator.py`、`tests/unit/execution/test_execution_book.py`|
|宽限到期时，聚合仓位仍可见的旧批次退出预留先撤销，再生成兜底退出|`tests/unit/live_rollout/test_grace_reservation_visibility.py`、`tests/unit/live_rollout/test_exits.py`|
|启动恢复失败不开放发单；恢复查询失败后继续轮转|`tests/unit/execution_account/orders/` 下恢复与协调器测试|

在仓库根目录运行常规回归：

```bash
CML_RUN_HUB_NETWORK_TESTS=1 .venv/bin/python -m pytest tests/unit tests/smoke tests/e2e -q
.venv/bin/python -m pytest tests/integration -q
node --test tests/frontend/*.test.mjs
```

集成测试使用独立本地测试库，安全边界见 [`tests/conftest.py`](../../tests/conftest.py)。本地研究测试位于 Git 忽略的 `local_optimization/tests/`，不包含在新 checkout 中。测试结果应关联提交和运行时间；历史通过数量不代表当前提交刚通过。通过回归不等于交易所验收，线上仍须核对订单、成交、剩余仓位和实际部署版本。


## 执行与恢复验收矩阵

以下是长期验收要求，不是旧重构任务清单，也不表示测试已通过。用真实公开入口和完整链路验证，只 Mock 外部边界；异步交错用 Event/屏障和有界等待，不靠 sleep 竞争。数量与金额使用 Decimal。

|输入与故障|必须断言|
|---|---|
|入场候选过期或正常投影竞争，确认尚未 POST；另测同身份已终态、已有准备预留|放弃本次机会，POST=0、不自动刷新候选、不启动事实恢复；未发送准备责任幂等结清，既有终态不改写；其他持仓及退出责任保留。|
|WS 终态早于 REST 本地时间，随后迟到 ACK、低累计量及重复终态|接受有效终态；累计量不回退；pending、Outbox、TTL 任务不复活；真实成交与释放只计一次；错误身份仍拒绝。|
|数量 10 的订单部分成交 3 后撤单；批次视图迟到|持仓计 3；未成交 7 仅在有终态和结算证明后释放；视图未到前保留实际结算责任，不因列表缺席判零。|
|POST 超时、GET 挂起，随后同 key 合法退出；调用方取消|入场 POST 仅一次；未知身份与预留保留；合法退出不等慢 GET；HTTP、数据库、上下文、遥测与退避入口均不持应用锁。|
|两个退出竞争同一份可用数量；锁外期间版本变化；CAS/commit 失败|不超预留；旧候选不覆盖新版本；失败不发布；成功提交后才发布；跨账户/key 与事务上下文隔离。|
|投影冲突发生时，同身份命令已终态；失败补偿再次运行|保护终态和已提交事实；可恢复冲突不经二次清理异常杀死 worker；真实持久化故障可观测。|
|Hedge SELL+LONG 或 BUY+SHORT 关闭收到 R=false；BOTH/错误身份负例|合法关闭完整进入订单及成交归账；Hedge 请求不带 reduceOnly；BOTH 一致性及其他身份校验保留。|
|stop、flatten、deadline、verify、reopen 独立时间边界|停入场不提前触发定时平仓；到达 flatten 才启动；未通过恢复验证不能因 reopen 时间到而准入。|
|队列容量 2，consumer 挂起后溢出、回调失败、关闭和重启|容量不突破；无额外无界缓冲；未完成观察可由持久回执恢复；不重复 POST；关闭不挂死、不改写成功执行。|
|入场→部分成交→撤单→退出→零仓，期间重启及迟到真实成交|事实、批次、预留与数量守恒；迟到成交能补账；持久状态重载兼容；零仓需实际证明。|

每次交付列出场景对应的实际测试名、命令、结果与未覆盖项；不能通过删除测试、宽化断言、跳过失败项或只测新辅助函数声称满足契约。涉及恢复留存时另验证裁剪前后恢复、去重和终态保护等价。
