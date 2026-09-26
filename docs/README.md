# 文档索引

本页区分仓库现状、系统约束、操作步骤、设计提案和历史审查，避免把旧目标或某次审查结论当成当前实现。

## 从这里开始

- [全系统第一性原理重构方案（2026-09-26 修订）](architecture/system-refactor-blueprint-20260925.md)：以 `42d95a1` 和服务器只读核验为基线，说明已实现基础、尚未闭合的业务链、目标模块、事务/恢复协议、分阶段迁移与删除验收；保留原文件作为统一入口。

- [仓库实现现状](current-state.md)：从当前代码与 Compose 配置能确认的系统轮廓，以及仅凭仓库无法确认的线上事实。
- [项目 README](../README.md)：安装、数据采集、Replay、Paper 和服务器 Paper 部署入口。
- [术语与持仓批次上下文](../CONTEXT.md)：持仓批次、平仓边界、追加开仓的定义。
- [持仓批次反复不匹配：证据与解决方案](architecture/position-batch-consistency-20260925.md)：2026-09-25 生产核验、最小复现、事实账本与退出分配闭环提案。

## 按问题查文档

| 要确认什么 | 优先查看 | 阅读口径 |
| --- | --- | --- |
| 当前 checkout 包含哪些服务和部署能力 | [仓库实现现状](current-state.md)、`compose.server.yaml`、`compose.live.accounts.yaml` | 配置存在不代表服务正在生产运行。 |
| 生命周期和进度应遵守什么约束 | [生命周期所有权契约](architecture/lifecycle-ownership-contract.md) | 文档标注为 2026-09-20 现行规范；实现情况仍需查代码。 |
| 在线事实、读模型和归档如何分工 | [读模型与保留期契约](architecture/read-model-and-retention-contract.md) | 文档标注为 2026-09-20 现行规范；不要把目标约束当作全部已验收。 |
| 账户凭证边界 | [ADR-0001](adr/0001-live-trading-credential-boundary.md) | 应用和 Compose 已有读/交易角色；生产密钥权限未由仓库验证。 |
| Paper 部署和实盘操作 | [Paper 部署手册](runbooks/server-paper-deployment.md)、[小资金实盘手册](runbooks/small-capital-live-session.md)、[多账户实盘手册](runbooks/multi-live-accounts.md) | 运行前核对镜像、配置、审批、密钥和操作环境；手册不是线上状态证明。 |
| 某天发现了哪些性能或正确性问题 | 日期化的 architecture、code-review、server-audit 文档 | 它们是带基线的检查快照；先确认问题是否已修复或重现。 |
| 为什么曾经这样设计、实施如何拆分 | `superpowers/specs/` 和 `superpowers/plans/` | 设计与计划记录意图和历史过程，不是当前代码说明。 |
| 策略研究结论 | `research/` 与研究报告 | 研究结果不自动等于线上策略配置或已批准交易参数。 |

## 文档状态约定

新增或更新系统文档时，在开头标注以下一种状态，并写明日期和适用范围：

- **仓库现状**：描述指定 checkout 中可由代码、配置或数据验证的事实；必要时记录 commit 和未提交变更。
- **现行约束**：系统应遵守的契约或 ADR；与实现差异应单独列出。
- **操作手册**：可执行的步骤；写明适用环境、前置条件和最近核验日期。
- **提案**：尚未实施的目标、设计或计划。
- **审查快照**：一次检查的范围、基线、日期、结论和后续状态。
- **历史/研究**：保留决策背景或实验结果，不作为当前运行配置。

审查报告中的问题只有在后续证据确认后才能标记为已解决。提案或计划完成后，在原文顶部加实施状态和对应变更链接；保留有历史价值的原文，不靠复制多个“最新版”来表达状态。

## 版本控制与生成物

项目文档属于源码资料，可以正常纳入 Git。`reports/`、`server_exports/`、`runs/` 和 `local_optimization/` 当前由 `.gitignore` 排除，主要用于运行结果、导出数据和实验产物；其中的报告不应替代本索引中的系统文档。
