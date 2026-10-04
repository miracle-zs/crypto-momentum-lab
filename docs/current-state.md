# 仓库现状

核对日期：2026-10-04。基线 `f2ffacf6410cf3fa383a6c3c940b1928dbc1be98`，包含工作区尚未提交的架构简化、测试和文档整理。本文不报告服务器此刻状态。

## 当前运行范围

- Binance USD-M 永续合约：公共行情采集、Orderflow 实盘、账户同步、研究采集器和只读看板。
- 安装入口共五个：`cml-market-data`、`cml-execution-account`、`cml-research-collector`、`cml-live-rollout`、`cml-operator-dashboard`；以 [pyproject.toml](../pyproject.toml) 为准。
- [基础 Compose](../compose.server.yaml) 与[账户 overlay](../compose.live.accounts.yaml) 支持四账户：primary、account-2、account-3、account-4。共享行情、采集器、看板与 PostgreSQL；每账户独立账户同步和策略进程，共 11 个应用进程加 PostgreSQL。
- 每个账户策略内部使用 asyncio；订单按账户、symbol、position_side 串行，同批次追加开仓与退出按真实成交和预留归属。没有每个信号启动一个持仓专用线程。
- 当前交易主链路不以 Shadow、CapabilityEvaluator、租约、Git/migration 比对或对账整体状态作为运行证明。旧行政命令和数据库字段仍可能存在，不代表热路径依赖它们。
- 旧 Research、Replay/Paper、独立 Shadow CLI 和 Compression/Liquidation 运行实现已退役；历史模型、数据库迁移及部分历史展示保留。
- 本地研究使用 `local_optimization/`，该目录不受 Git 跟踪。旧实现可在清理前基线 `02e6581f3bc71feac0f91f84fa405460ea26730f` 查看。

## 本地验证

最近测试清理后的回归：Python unit/smoke/e2e 2990 通过、1 跳过（缺真实采集环境）；PostgreSQL 集成 172 通过；Node 前端 63 通过。修改文件 Ruff F/I 和差异空白检查通过，不等同于全仓库 Ruff/mypy 全绿。详见[测试说明](testing/behavior-tests.md)。本次文档整理检查全部本地链接，核对 Live/部署及六个本地研究 CLI；smoke 49 通过、1 因缺真实采集环境跳过，前端 63 通过。

## 生产证据边界

本地修改尚未部署。此前 `f2ffacf6` 的部署十分钟观察报告无新增重启或交易运行错误，但没有新订单样本；启动期间数据库读取超时后重试恢复。不能由此推断新改动的实盘延迟改善或宽限退出已在该窗口实际发生。

保留的 JSON/CSV 记录较早版本的事故与观测，统一见[故障记录](diagnostics/incidents.md)。历史记录不作为实时故障列表。要确认当前镜像、账户持仓、订单、告警或性能，必须重新读取服务器；本轮文档整理没有连接服务器或执行交易。

## 剩余工作

[简化状态](architecture/simplification.md) 集中列出订单结算状态收敛、最小启动恢复、增量账本发布、类型依赖及性能测量。没有将这些未完成工作标成已完成。
