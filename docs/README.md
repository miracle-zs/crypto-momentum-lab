# 文档索引

## 当前入口

- [仓库现状](current-state.md)：仓库基线、服务轮廓，以及按日期限定的生产观测。
- [项目 README](../README.md)：安装、数据采集、本地 Orderflow 研究和部署入口。
- [持仓批次上下文](../CONTEXT.md)：批次身份、平仓边界和追加开仓术语。
- [重构蓝图](architecture/system-refactor-blueprint-20260925.md)：设计依据和带基线的实施记录；不代表当前生产状态。
- [持仓批次一致性提案](architecture/position-batch-consistency-20260925.md)：事实账本与退出分配闭环设计。
- [交易系统架构调整计划](architecture/trading-architecture-adjustment-plan-20261001.md)：六批结构调整与累计本地验收记录；数据库及生产验收尚未完成。
- [调整后的交易架构](architecture/trading-architecture-after-adjustment-20261001.md)：累计改动的模块图、下单时序与剩余边界；基于本地工作树。
- [架构审视与设计边界诊断](architecture/system-overengineering-architecture-review-20261003.md)：模块依赖关系图、六阶段防御时序、风控门禁职责精准核对与高价值重构路径。
- [交易执行安全与架构简化修改计划](architecture/system-overengineering-modification-plan-20261003.md)：文档修订、最终 POST 围栏、敞口交接验证及分批架构简化；执行契约、公共候选工具、统一风控和类型消环已落地，含完整文件清单、回归结果与剩余验收记录。
- [运行版本问题审计与新版本对照](architecture/server-runtime-audit-20261003.md)：当前运行版本部署以来的日志、订单和就绪状态，逐项核对新版本修复范围及启动回归。

## 约束与操作

- [运行时与数据契约](architecture/runtime-and-data-contracts.md)：目标约束；实现状态需另查代码和验证记录。
- [ADR-0001：实盘凭证边界](adr/0001-live-trading-credential-boundary.md)：代码和 Compose 中的读/交易凭证分工；不证明生产密钥权限。
- [服务器部署与更新手册](runbooks/server-paper-deployment.md)、[小资金实盘手册（含多账户）](runbooks/small-capital-live-session.md)：操作前核对当前镜像、配置、审批和密钥。
- [执行语义与故障注入门禁](paper-live-replay-execution-semantics.md)：Replay、Paper、Live 的差异和发布故障剧本。
- [告警、SLO 与看板](runbooks/operational-alert-monitor.md)：服务端告警、决策 SLO 和只读操作看板。
- [Market-state Hub 手册](runbooks/market-state-hub.md) 与[本地优化脚本手册](runbooks/local-full-data-optimization.md)：后者记录现有旧脚本用法，结果解释以新设计为准。
- [PostgreSQL 容量与保留期记录](postgres-capacity-and-retention-2026-09-14.md)：`cml-archive-trim.service` 引用的日期化运维依据，执行前需复核当前水位和配置。
- [清理执行计划](plans/2026-10-03-dormant-code-cleanup.md)：分阶段清理与验证记录。
- [沉睡代码与清理清单](dormant-code-and-cleanup-inventory.md)：已核实的退役范围、归档路径与保留边界。
- [性能优化与算法实践指南](performance-optimization-and-algorithm-best-practices.md)：回测撮合、因果对账、MTM 连续估值与网格寻优的高性能算法与工程架构实践。

## 研究与历史证据

- [短周期动量策略研究设计](research/short-horizon-momentum-strategy-research-design.md)：订单流、压缩突破和爆仓瀑布三条独立研究路线。
- [本地寻优设计](superpowers/specs/2026-09-18-local-parameter-optimization-design.md)：集中保存指标依据、复利扩展和成交量窗口结果。
- [机会池与 Walk-forward 修复设计](superpowers/specs/2026-09-21-opportunity-pool-and-walk-forward-repair-design.md)：明确参数无关机会池、连续持仓和 OOS 评价；尚未实现。
- `superpowers/specs/` 仅保留以上两份设计；旧实施计划及过时设计已从工作树清理，Git 历史仍可追溯。
- 工作树只保留有持续价值的操作手册、契约和少量近期风险快照；一次性审查及被后续结论覆盖的报告已清理，旧版本仍可从 Git 历史查阅。
- 研究报告说明实验条件，不自动成为当前策略参数或获准交易配置。

## 文档维护

新增现状文档需注明 checkout/commit；生产证据需注明观测时间、运行基线和只读范围。契约、操作手册、提案、审查快照和历史研究应标明各自状态。不要把容器健康、单元测试通过或代码中存在配置写成完整生产验收。

项目文档纳入 Git。`reports/`、`server_exports/`、`runs/` 和 `local_optimization/` 由 `.gitignore` 排除，其内容属于运行或实验产物，不替代本索引中的规范文档。
