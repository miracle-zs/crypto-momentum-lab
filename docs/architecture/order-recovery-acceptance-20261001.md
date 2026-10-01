# 下单恢复验收（2026-10-01）

本轮使用假交易所，未连接实盘或触发真实订单。

## 已运行验证

- 实际 Binance 客户端通过 httpx.MockTransport 发出请求，模拟交易所接单后 ReadTimeout：按同一 client_order_id 查询恢复 FILLED。
- 重新创建执行器再次对账仍为 FILLED；请求计数为一次 POST、两次 GET。
- 无交易所记录时恢复保持 UNKNOWN_PENDING_RECONCILIATION，只查询、不补单、不直接取消。
- 故障用例、状态机与 Binance 客户端相关测试合计 65 passed。

## 数据库用例与运行限制

新增 test_committed_preparation_survives_process_exit_before_dispatch：子进程提交 prepare_submission 后 os._exit(73)，父进程通过独立会话检查 SUBMITTING 和唯一事件，再检查重复准备被拒绝。用例已成功收集，尚未运行；本地 Postgres 54329 握手超时，Docker 服务探针未返回。该用例验证持久性和重复准备保护，不代表完整启动恢复流程或资金预留释放已验收。

数据库可用并完成测试库迁移后运行：

```sh
rtk proxy .venv/bin/pytest -q tests/integration/persistence/test_order_repository.py -k committed_preparation_survives
```

测试库必须符合现有 conftest 的本地与 test/review/temp/ci 命名防护；fixture 会清空相关交易测试表。

## 仍需验收的边界

查不到订单不能直接等同于交易所未接单。当前开仓恢复保持未知，需要继续核验独立的缺席证据、状态收敛及预留释放政策。不能仅凭最终查询为空自动标记 CANCELLED。队列串行化只覆盖同进程同键，跨进程 fencing 与真实数据库事务仍需集成验证。
