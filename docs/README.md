# 文档索引

核对日期：2026-10-05。本文档树只保留当前可执行说明和经过归纳的历史结论；逐次扫描、原始服务器导出和改造流水账可从 Git 历史获取。

|用途|文档|
|---|---|
|安装与服务入口|[项目 README](../README.md)|
|当前范围、验证与生产边界|[仓库现状](current-state.md)|
|模块职责、依赖和下单链路|[系统架构](architecture/overview.md)|
|订单、成交、批次和退出约束|[执行契约](architecture/execution-contracts.md)、[批次术语](../CONTEXT.md)|
|凭证职责边界|[ADR-0001](adr/0001-live-trading-credential-boundary.md)|
|部署与更新|[服务器手册](runbooks/server-deployment.md)|
|实盘账户操作|[实盘手册](runbooks/small-capital-live-session.md)|
|行情与账户 Hub|[Hub 手册](runbooks/market-state-hub.md)|
|Server 酱、监控和看板|[告警手册](runbooks/operational-alert-monitor.md)|
|PostgreSQL 运维|[PostgreSQL 手册](runbooks/postgres-operations.md)|
|本地研究与评价|[研究手册](runbooks/local-full-data-optimization.md)、[评价口径](research/evaluation.md)、[计算性能](research/performance.md)|
|回归测试|[测试说明](testing/behavior-tests.md)|
|已确认的历史事故与教训|[故障记录](diagnostics/incidents.md)|
|日常工具和归档脚本|[脚本索引](../scripts/README.md)|

运行手册只描述当前代码可执行的步骤。生产结论必须注明版本、时间窗口和样本数；测试通过、容器健康和 ACK 都不等同于实盘交易验收。`local_optimization/`、`server_exports/`、`runs/`、`reports/` 是被 Git 忽略的本地源码或实验产物，独立 checkout 无法恢复。
