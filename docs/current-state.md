# 仓库现状

核对日期：2026-10-05。本文描述当前工作树，不代表服务器正在运行的镜像或账户状态。

## 当前范围

- Binance USD-M 永续合约的公共行情、Orderflow 实盘、账户同步、研究采集和只读看板。
- 五个安装入口：`cml-market-data`、`cml-execution-account`、`cml-research-collector`、`cml-live-rollout`、`cml-operator-dashboard`；以 [pyproject.toml](../pyproject.toml) 为准。
- PostgreSQL 是单体事实库。基础 Compose 与账户 overlay 支持 primary、account-2、account-3、account-4；每个账户有独立同步与策略进程，共享行情、采集器、看板和数据库。
- 每个策略进程使用 asyncio。订单按账户、标的、持仓方向串行；没有“每个信号一个线程”或 Shadow 运行链路。
- 开仓只经过上下文事实、内存硬限额和单次数据库终审；对账与通知异步执行。`reduce_only` 退出不被对账状态阻断。
- 5 倍杠杆被交易所拒绝后，客户端按 4 倍、3 倍继续尝试；这是明确策略，不是兼容回退。

## 已移除的旧机制

CapabilityEvaluator、重复准入围栏、Shadow、运行期 Git/migration 比对、三阶段订单补偿、旧 client-order-id 重建、旧账户表恢复、旧命令成交水位重建、协议字段默认值和保证金模式别名均不在当前交易链路。风险配置不再保存未执行的状态年龄限制；历史影子运行表已删除。订单网络超时仅保留原订单反查；账户与仓位事实由后台同步校准。

## 验证与生产边界

当前工作树最近完整回归为 `3224 passed, 5 skipped, 2 warnings`。跳过项需要真实数据库或本机回环权限；两个警告来自 Starlette/httpx 弃用和一个既有测试协程清理问题。

本地改动尚未部署。历史服务器观测、已发生事故和不能从健康检查推导出的结论见 [故障记录](diagnostics/incidents.md)。确认当前镜像、订单、仓位、告警或性能时必须重新读取服务器。

## 文档边界

系统职责与调用链见 [架构](architecture/overview.md)，订单状态与恢复底线见 [执行契约](architecture/execution-contracts.md)。历史扫描表、原始 JSON/CSV 导出和逐批改造日志不再维护在工作树；Git 历史仍可追溯。
