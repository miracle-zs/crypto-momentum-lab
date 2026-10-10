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
