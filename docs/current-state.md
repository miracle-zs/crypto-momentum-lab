# 仓库实现现状

核对日期：2026-09-25。范围：本地 checkout 中的源码、README 和 Compose 配置。本页是仓库可见现状摘要，不代表服务器当前正在运行的镜像或账户状态；涉及实盘的结论必须以部署元数据和线上观测复核。

## 仓库能确认的系统轮廓

- 项目面向 Binance USD-M 永续合约，提供行情采集、策略研究、Replay、Paper、账户同步、受控实盘执行和运维看板能力。根 [README](../README.md) 给出本地启动、数据采集、Replay、Paper 和服务器 Paper 部署入口。
- 系统由同一个 Python 项目中的多个进程/Compose 服务组成。`compose.server.yaml` 定义服务器 Paper 栈和显式 `live` profile；市场数据服务与策略、账户和看板服务分开运行。
- `market-data` 使用 Binance 公共行情，刷新交易标的池并采集 WebSocket 数据。README 描述的原始归档格式为压缩 JSONL；项目同时使用 PostgreSQL 保存运行态数据。
- Replay 和 Paper 使用策略运行器；Paper 可读取本地派生状态，也可通过 `paper-live-source` 从 PostgreSQL 读取运行态状态。Paper 命令只产生模拟执行结果，不提交真实订单。
- 服务器 Paper 栈中的单个 Paper 策略守护进程会把同一入场决策分发给 B8 与 B1 Top10-gainer 两个 Paper 账户；账户持仓和退出独立管理。配置说明见 [README 的服务器 Paper 部署部分](../README.md#server-paper-deployment)。
- 实盘路径按账户拆分只读 `execution-account` 与交易 `live-strategy`。基础 Compose 配置定义 primary；[实盘账户 overlay](../compose.live.accounts.yaml) 增加 account-2、account-3、account-4。多账户操作方式见[多账户实盘手册](runbooks/multi-live-accounts.md)。这些文件证明仓库支持该拓扑，不证明所有账户已经启动。
- 凭证解析和部署配置区分 `BINANCE_READ_API_KEY` 与 `BINANCE_TRADE_API_KEY`。代码仍保留需显式启用的 legacy fallback。此仓库无法证明线上实际使用了不同密钥，也无法验证 Binance 上配置的权限；见 [ADR-0001](adr/0001-live-trading-credential-boundary.md)。

## 不应从仓库文档直接推断的线上事实

- 生产服务器实际部署的 commit、镜像、Compose profile 和运行服务数。
- 实盘账户当前是否启用、正在使用的策略和参数、账户余额、持仓、挂单及健康状态。
- 生产环境是否使用不同的读/交易密钥，以及密钥在 Binance 上的实际权限。
- 日期化审查中记录的告警或缺陷现在是否仍存在。

确认这些事实需要核对服务器当前部署信息、运行元数据和实时观测；旧审查报告只说明其记录日期和基线下的发现。

## 关键历史文档的阅读边界

[2026-06-14 架构设计](superpowers/specs/2026-06-14-project-architecture-design.md)记录早期目标，其中“单账户、单实盘策略”是当时范围，不再完整描述仓库现有的多账户配置。[2026-09-18 架构演进文档](architecture-evolution-20260918.md)明确标注为未实施提案。其余带日期的审查文档应按各自基线阅读，不应当作当前问题清单。
