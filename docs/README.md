# 文档索引

核对日期：2026-10-04；源码基线 `f2ffacf6` 加本地未提交修改。仓库现状和生产观测分别记录。

|用途|文档|
|---|---|
|安装与入口|[项目 README](../README.md)|
|当前能力、部署与验证边界|[仓库现状](current-state.md)|
|模块职责、依赖与下单流程|[系统架构](architecture/overview.md)|
|订单、成交、批次与退出约束|[执行契约](architecture/execution-contracts.md)、[批次术语](../CONTEXT.md)|
|已实施的简化与剩余工作|[简化状态](architecture/simplification.md)|
|凭证角色决策|[ADR-0001](adr/0001-live-trading-credential-boundary.md)|
|部署、更新与回退|[服务器手册](runbooks/server-deployment.md)|
|实盘启动、停止与四账户|[实盘手册](runbooks/small-capital-live-session.md)|
|行情与账户事件传输|[Hub 手册](runbooks/market-state-hub.md)|
|Server 酱、监控与看板|[告警手册](runbooks/operational-alert-monitor.md)|
|数据库容量、连接与保留期|[PostgreSQL 手册](runbooks/postgres-operations.md)|
|本地研究操作|[寻优手册](runbooks/local-full-data-optimization.md)|
|研究数据、权益与前向评价|[研究评价口径](research/evaluation.md)、[计算性能实践](research/performance.md)|
|安全回归与测试清理|[测试说明](testing/behavior-tests.md)|
|故障根因与生产证据|[故障记录](diagnostics/incidents.md)|
|日常工具与历史脚本|[脚本索引](../scripts/README.md)|

每个主题只维护一个入口。已完成计划、逐批进展、旧架构图和重复验收报告从工作树删除，历史版本仍可通过 Git 查看。诊断 JSON/CSV 保留为原始证据，不参与运行门禁。

操作手册描述当前代码可执行的步骤；设计建议必须标明未实现。生产结论须注明版本、时间窗口和样本数，容器健康或测试通过不能替代真实交易验收。`local_optimization/`、`server_exports/`、`runs/`、`reports/` 属于被 Git 忽略的本地源码或实验产物，单独 checkout 本仓库不能恢复它们。
