# 浅层模块与删除候选审查（2026-10-04）

已按用户要求执行本报告的优先清理：删除 4 个生产模块、3 个废弃测试文件（11 个测试函数），迁移订单 ID 的 3 个行为测试导入；删除 Dashboard 的 49 个顶层私有别名与 1 个测试专用类属性。相关测试 176 项通过；本次涉及文件未新增 Ruff 问题（仍有 14 项既有行长问题）。本次尚未执行 Git 提交。

工作区同时存在其他聊天的修改；以下候选依据执行前的静态审查，验证结果仅覆盖本次相关测试。

## 方法与范围

扫描 `src/crypto_momentum_lab` 的 371 个 Python 文件，再核对候选实现、生产调用、测试、CLI 入口、配置、部署文件及历史文档。文件短和单一调用方仅用于发现候选，不作为删除依据。

应用 codebase-design 的 deletion test：删除 Module 后，复杂度消失则考虑删除；复杂度回到调用方则保留，或者将实现收回所属深 Module 的内部。Interface 包含调用约束、错误行为和生命周期，不仅是方法参数。静态搜索不能排除仓库外调用或任意动态导入。

## 优先候选：3 个无生产调用模块 + 1 个纯重导出模块

| 优先级 | Module | 引用证据 | 删除测试的结果 | 建议及测试处理 |
| --- | --- | --- | --- | --- |
| 高 | `live_rollout/rollback.py:7` | 三个公开名称仅定义处及 `tests/unit/live_rollout/test_rollback.py` 引用，无生产调用 | 删除这份独立旧模型和检查函数，不需要在生产调用方补逻辑 | 删除模块及对应 2 个测试函数。实际运行中的 draining、订单控制、lease 管理应继续保留；同名 rollback 数据表和数据库事务回滚与本模块不同 |
| 高 | `live_rollout/reports.py:6` | `LiveReportInput`、`LiveFinalReport`、`build_live_final_report` 仅本文件与测试使用；CLI report 和 Dashboard 报告走其他实现 | 没有调用方接收其 18 个输入字段，删除后这套旧 Interface 和重复统计实现一并消失 | 删除模块及对应 2 个测试函数；不删除当前性能统计与报告实现 |
| 高 | `persistence/postgres/runtime_state_loader.py:26` | `AsyncPostgresRuntimeStateLoader`、内部 wakeup 仅本文件、专属测试、20260930 历史文档引用；无生产构造处 | 私有事件循环、LISTEN 连接、同步包装、轮询回退及关闭约束整体消失 | 删除模块及对应 7 个测试函数。这是失去调用方的旧 Adapter，不代表全部 Postgres 行情读取过时；当前 async 读取、startup recovery 和 `poll_live_market_states` 保留 |
| 高 | `execution_account/orders/ids.py:1` | 仅 `orders/quantization.py` 与 `test_ids.py` 从此路径导入；文件仅 import + `__all__` | 改为直接导入 `domain/execution/order_state.py` 后，所有 ID 校验、哈希和长度规则仍由原实现承担，转发层消失 | 删除转发文件，改 1 个生产导入及测试导入；保留 3 个 ID 行为测试，不应因删除包装而丢失规则覆盖 |

前三个模块合计 389 行源码、11 个测试函数；加入重导出模块，共 402 行源码、4 个源码文件。行数是清理规模，不是 Depth 评分，也不是精确净 diff（需要改导入）。

## 优先候选：Dashboard 的 49 个旧私有重导出

位置：`operator_dashboard/queries.py:84` 至 `:148` 的顶层别名赋值。

AST 核对得到：49 个私有别名在 `queries.py` 自身没有加载使用，在生产代码中没有发现对这些顶层别名的直接引用；26 个有测试直接引用，23 个未发现直接外部引用。字符串形式 monkeypatch 和仓库外调用不属于这一统计。

删除测试的结果：删除别名并迁移相关测试后，真正业务实现仍位于 `common_equity.py`、`overview_queries.py`、`paper_account_queries.py`、`paper_equity_queries.py`、`live_account_metrics_queries.py`、`risk_execution_queries.py` 等所属模块。消失的是维护旧导入路径的 Interface，不是权益计算、SQL 或状态判定逻辑。

执行方式：

1. 清掉未直接引用的别名，核对字符串 monkeypatch 和同名类属性。
2. 对测试引用的别名，将必要纯规则测试迁往实现所属 Module；有 Interface 行为测试覆盖时，删除重复内部测试。
3. 删除对应别名，保留 `DashboardQueries` 的正常业务 Interface。

`DashboardQueries._live_account_summaries` 类属性（`:253`）也是测试入口，需单独核对；它不是上面统计的顶层别名。

不要直接删除整个 `DashboardQueries`：它还集中完成多个查询实现的依赖组装、统一配置，并自己实现 reports 与 account_performance；整个删掉会把组装复杂度转移给 HTTP 调用方。它的多数转发方法属于进一步缩小 Interface 的候选，需要先决定调用方如何获取查询能力。

## 可进一步合并的小包装

`execution_account/balance_history.py:19` 的 `_balance_has_value` 仅返回 `_balance_value_is_nonzero(balance_value(balance))`，仅模块内一个调用。删除此函数并直接组合现有函数可以消除一个名字，规则仍留在同一 Module；无需删除整个 `balance_history.py`。该模块保留非零余额和归零过渡的规则有实际价值。

`execution_account/reconciliation_records.py:62` 的 `reconciliation_run` 基本透传，但隐去了 environment、account_label、observed_at 等重复赋值，并被 `sync.py` 三处调用。整体删除会把同一身份规则分散回调用方，收益低；若重构，宜收为 account sync 的内部构造函数，不应直接展开到三处。

## 删除测试未通过：建议保留的模块

- `execution_account/fill_scan_plan.py:29`：隐藏源锚点选择、零仓位判断、扫描时间区间和来源身份；删除会把这些约束塞回 sync。一个生产调用方不等于浅层。
- `execution_account/position_history.py:14`、`balance_history.py:27`：封装归零过渡和稀疏历史保留规则，删除后判断回到同步实现；保留业务行为测试。
- `live_rollout/health_monitor.py:18`：run 的小 Interface 隐藏健康降级、循环、取消传播和写入失败隔离；具有 Depth。
- `live_rollout/order_event_runtime.py:30`：封装 telemetry 失败后仍执行本地观察和恢复请求的顺序约束；直接删除会让 orchestrator 承担这些规则。
- `live_rollout/session_state.py:14`：虽然函数很小，但 runtime_orchestrator 与 plan_runner 多处使用统一的 draining 判定；删除会把判定散到调用方，保留。
- `operator_dashboard/ports.py:27`：有生产 DashboardQueries 与测试 FakeQueries 两种 Adapter，是实际 Seam；不应仅因 Protocol 不含 Implementation 删除。
- PostgreSQL 的 `journal_store_ports.py`、`command_store_ports.py`、`reservation_store_ports.py`：暴露 caller-owned session 的内部 Seam，避免事务实现依赖整个仓储。透传 session 关系到原子性；不要因方法短而删掉事务行为。可讨论合并文件，但它只减少文件数。

## 验证与落地次序

已运行 Dashboard、Dashboard 应用及本地端到端、订单 ID 与量化相关测试，176 项通过；没有重跑全项目测试。候选结论来自静态引用和逐项实现核对。建议先清理 3 个无生产调用模块及其 11 个测试，再删除 ids 转发层但保留 3 个行为测试，最后清理 Dashboard 重导出和重复内部测试。当前并发写入和兼容实现被恢复造成的失败需先收敛，再在稳定源码上运行回归和提交。

## 附录：49 个候选顶层别名

- `_decision_slo_response`：测试直接引用
- `_split_exchange_orders`：测试直接引用
- `_exchange_order`：未发现直接外部引用
- `_EquityObservation`：测试直接引用
- `_common_equity_interval_seconds`：测试直接引用
- `_build_common_equity_curve`：测试直接引用
- `_build_common_equity_result`：未发现直接外部引用
- `_live_account_equity_point`：测试直接引用
- `_live_cash_flow_payload`：未发现直接外部引用
- `_common_equity_note`：未发现直接外部引用
- `_paper_equity_observations`：未发现直接外部引用
- `_paper_equity_observations_from_values`：未发现直接外部引用
- `_live_equity_observations`：测试直接引用
- `_live_aggregated_equity_observations`：未发现直接外部引用
- `_apply_live_cash_flow_adjustments`：未发现直接外部引用
- `_AccountEquityPoint`：未发现直接外部引用
- `_account_equity_range`：测试直接引用
- `_account_equity_statement`：测试直接引用
- `_account_margin_statement`：测试直接引用
- `_live_account_metrics_window_start`：测试直接引用
- `_live_account_metric_points`：测试直接引用
- `_latest_live_account_process_statement`：测试直接引用
- `_account_label_sort_key`：未发现直接外部引用
- `_live_account_status`：未发现直接外部引用
- `_live_account_fleet_status`：未发现直接外部引用
- `_live_account_summaries`：未发现直接外部引用
- `_service`：未发现直接外部引用
- `_live_observation`：测试直接引用
- `_age`：未发现直接外部引用
- `_universe_entry`：未发现直接外部引用
- `_universe_membership`：测试直接引用
- `_PaperEquitySummaryPoint`：未发现直接外部引用
- `_downsample_equity_snapshots`：测试直接引用
- `_is_dashboard_paper_run`：测试直接引用
- `_paper_account_summary`：测试直接引用
- `_paper_exit_label`：测试直接引用
- `_json_mapping`：未发现直接外部引用
- `_json_value`：未发现直接外部引用
- `_PaperEquityPoint`：未发现直接外部引用
- `_paper_run_values`：未发现直接外部引用
- `_paper_first_equity_statement`：测试直接引用
- `_paper_latest_equity_statement`：测试直接引用
- `_paper_common_equity_statement`：测试直接引用
- `_paper_equity_statement`：测试直接引用
- `_live_common_equity_statement`：测试直接引用
- `_AccountFillAggregate`：未发现直接外部引用
- `_aggregate_account_fills`：测试直接引用
- `_live_strategy_signal`：测试直接引用
- `_order_intent_reason`：未发现直接外部引用
