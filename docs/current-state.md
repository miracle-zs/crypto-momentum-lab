# 仓库实现现状

文档核对日期：2026-09-29。仓库代码基线：`616096dec29e6779c5c24d8bc5b5d233adad8bd4`（HEAD 提交于 2026-09-28）。最近一份生产快照记录至 2026-09-28 16:43（Asia/Shanghai）：当时四个 Live 容器已恢复运行，策略镜像为服务器提交 `bb7d5635ca71d1244980e60334969a1ac7b33ba3`（诊断记录映射到本地提交 `18adfee`），看板镜像为 `b655aa4`。这些镜像与当前仓库 HEAD 不同；快照不证明 9 月 29 日的线上状态。

## 2026-10-03 本地仓库清理更新

以下更新描述本地工作树，不代表生产已部署：旧 Research、Replay/Paper、Shadow CLI
及 Compression/Liquidation 策略已经退役。服务器 Compose 不再定义 Paper runner。
保留 Orderflow 实盘、本地 `local_optimization/`、研究采集器、行情、账户与看板能力；
保留历史账本和数据库迁移。下方的 9 月观测及旧能力描述应按原日期理解。
详见[执行计划](plans/2026-10-03-dormant-code-cleanup.md)。

## 仓库能确认的系统轮廓

- 项目面向 Binance USD-M 永续合约，提供行情采集、策略研究、Replay、Paper、账户同步、受控实盘执行和运维看板能力。根 [README](../README.md) 给出本地启动、数据采集、Replay、Paper 和服务器 Paper 部署入口。
- 系统由同一个 Python 项目中的多个进程/Compose 服务组成。`compose.server.yaml` 定义服务器 Paper 栈和显式 `live` profile；市场数据服务与策略、账户和看板服务分开运行。
- `market-data` 使用 Binance 公共行情，刷新交易标的池并采集 WebSocket 数据。README 描述的原始归档格式为压缩 JSONL；项目同时使用 PostgreSQL 保存运行态数据。
- Replay 和 Paper 使用策略运行器；Paper 可读取本地派生状态，也可通过 `paper-live-source` 从 PostgreSQL 读取运行态状态。Paper 命令只产生模拟执行结果，不提交真实订单。
- 服务器 Paper 栈中的单个 Paper 策略守护进程会把同一入场决策分发给 B8 与 B1 Top10-gainer 两个 Paper 账户；账户持仓和退出独立管理。配置说明见 [README 的服务器 Paper 部署部分](../README.md#server-paper-deployment)。
- 实盘路径按账户拆分只读 `execution-account` 与交易 `live-strategy`。基础 Compose 配置定义 primary；[实盘账户 overlay](../compose.live.accounts.yaml) 增加 account-2、account-3、account-4。多账户操作方式见[多账户实盘手册](runbooks/small-capital-live-session.md)。这些文件证明仓库支持该拓扑，不证明所有账户已经启动。
- 凭证解析和部署配置区分 `BINANCE_READ_API_KEY` 与 `BINANCE_TRADE_API_KEY`。代码仍保留需显式启用的 legacy fallback。此仓库无法证明线上实际使用了不同密钥，也无法验证 Binance 上配置的权限；见 [ADR-0001](adr/0001-live-trading-credential-boundary.md)。

## 2026-09-26 服务器只读快照

13:58—14:06（Asia/Shanghai）核对 `43.167.191.253`：服务器 checkout 与应用镜像均为上述基线。运行 4 个 Live、4 个账户同步、market-data、research-collector、dashboard 和 PostgreSQL，共 12 个容器；该时刻均为 healthy，没有运行 Paper 容器。PostgreSQL Alembic revision 为 `20260925_0043`；`cml-archive-trim.timer` 已停用。此前 9 月 25 日该 timer 曾以一天保留期删除 51,356 行 `account_position_snapshots` 和 10,105 行 `exchange_order_events`，随后在 SQL 输出处理处崩溃；timer 已停止并禁用，9 月 26 日复核为 inactive。不能把这次删除当作清理成功，也不能在没有消费者水位验证时恢复调度。

新的行情修订和持仓预留已经真实写入，但不能据此判定整条重构完成：41 条预留均缺投影版本且使用合成批次身份，8 条 ACTIVE 预留关联的订单已是 filled；新 DecisionTrace/DatasetManifest 表仍为空，代码追踪也未发现对应生产追踪闭环。这里记录的是持久状态脱节，不推断实际重复交易或资金损失。

详细证据、已实现保护、尚未验证的部分和迁移方案见[全系统第一性原理重构方案](architecture/system-refactor-blueprint-20260925.md)。本次未修改服务器配置、重启服务、清理数据或执行交易操作。

## 后续服务器与交接快照

2026-09-27 13:18–13:21 的故障快照记录四个策略容器循环重启，AIOUSDT 退出尝试在交易所提交前被门禁拒绝。这是后续部署前的状态，不代表 9 月 28 日的最终快照。

2026-09-28 早期本地接管基线 `ec836c3` 冻结时仍未完成、不可部署。阶段验证虽包含 PostgreSQL 核心用例和迁移往返通过，但全量单测、跨 epoch 恢复、Hub 接线及 Ruff 门禁未通过；具体缺口汇总见[重构蓝图第 19 节](architecture/system-refactor-blueprint-20260925.md)。

稍后的 9 月 28 日生产恢复快照显示：四账户严格预检通过并报告 `FULLY_TRADEABLE`，容器 healthy、重启次数为 0；两分钟 CPU 样本中单账户约 4.54%，四账户各约 4.67%–7.18%，主机 idle 均值约 67.71%。最后五分钟没有记录 `live_runtime_failed` 或 `non-checked-in connection`。这些都只描述当时状态。

尚未闭环的是历史账本：Book-only 旧仓差异仍有主账户 196、账户 2 为 81、账户 3 为 58、账户 4 为 71 项；诊断记录明确没有清理或迁移这些旧事实。服务健康和可交易门禁通过不等于历史账本已修复。详细栈采样与逐步诊断保留在 Git 历史。

## 仍需按观测时刻确认的线上事实

- 后续时刻实际部署的 commit、镜像、Compose profile 和运行服务数。
- 实盘账户当前是否启用、正在使用的策略和参数、账户余额、持仓、挂单及健康状态。
- 生产环境是否使用不同的读/交易密钥，以及密钥在 Binance 上的实际权限。
- 日期化审查中记录的告警或缺陷现在是否仍存在。

确认这些事实需要核对服务器当前部署信息、运行元数据和实时观测；旧审查报告只说明其记录日期和基线下的发现。

## 关键历史文档的阅读边界

早期架构文档假定“单账户、单实盘策略”，已不适用于仓库现有的多账户配置，因此已从工作树清理；旧实施计划和过时审查可从 Git 历史追溯。重构蓝图与保留的风险快照应按各自的代码和服务器基线阅读，不应当作当前问题清单。

2026-09-27/28 的全局隐式兜底审计路线图早于蓝图记录的 L4 清理完成状态，且未作为当前索引入口；它不再是现状或待办清单，原稿可从 Git 历史追溯。
