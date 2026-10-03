# 旧代码清理清单与执行边界

核对日期：2026-10-03。清理前代码基线：
`02e6581f3bc71feac0f91f84fa405460ea26730f`。
本清单描述本地工作树；不宣称服务器已经部署或历史账本已经修复。

## 已核实的运行范围

当前保留的入口是 Orderflow 实盘、账户同步、行情服务、研究采集器、
运维看板，以及 `local_optimization/` 的寻优、场景回放、MTM 与实盘对账。
“Orderflow 研究”原来还包含一个旧事件研究 CLI；它不属于上述本地研究流水线。

不参与这两条主流程的代码可以作为退役候选，但“没有业务调用”与“没有导入依赖”
必须分别核查。Compression/Liquidation 原来仍被 registry 导入；旧 Shadow/Paper
测试原来提供 Live 测试 fixture。这些依赖已在删除前解除。

## 本次清理

| 内容 | 处理 | 边界 |
| --- | --- | --- |
| 旧 `research/` 与 `apps/research/` | 删除实现、CLI 注册与专属测试 | 新 `research_collector/` 保留 |
| 旧 `strategy_runner/` 与应用入口 | 删除 Replay/Paper 实现、CLI 注册与专属测试 | `local_optimization/` 回放与对账保留 |
| 独立 `shadow_operation/` 与应用入口 | 删除独立 CLI、演练与报告实现 | Live shadow preflight、抑制能力与历史模型保留 |
| Compression/Liquidation 策略 | 删除实现及专属测试，registry 仅支持 Orderflow | 历史策略名称仍可出现在数据库、看板和账本测试中 |
| `read_market_states_15s_dataset` | 删除读取接口、专属解析函数及测试 | Parquet 写入与采集物化保留 |
| 四个 retired Paper Compose 服务 | 删除定义及 `x-paper` 模板 | 历史账户查询、保护配置与升级清除旧容器逻辑保留 |
| 日期化修库与历史身份修复工具 | 移至 `scripts/maintenance_archive/` | 不推断修复已完成；审计测试可单独运行 |
| 两个 20260924 review repro | 移至 `scripts/review_archive/` | 正式修复测试保留在 `local_optimization/tests/` |
| `scripts/research_archive/` | 保留归档，说明历史复现基线 | 专属回放测试退出默认测试集 |

删除的旧实现可以从上述 Git 基线获取；不在当前运行包中保留兼容壳。
旧 CLI 不再安装，当前虚拟环境须重新安装项目以更新命令入口。

## 明确保留的内容

- `strategies/order_flow_impulse/`、`live_rollout/`、账户同步、行情与运维看板。
- `research_collector/` 和 Parquet 写入/物化；不能把旧 `research/` 与采集器混为一谈。
- 数据库模型、Alembic 迁移、历史交易账本和可独立验证的持久层测试。
- 历史 Paper ID 在看板和行情保护配置中的可见性；本次不改变保留期或清理数据。
- `six_scenarios_equity_comparison.html`：日常寻优的默认看板产物，不是废弃文件。
- `baseline_15s_mtm_equity_series.csv`：仍供看板读取，不按体积判断为废弃。
- `concurrency_equity_curves.html` 等其他研究产物：本次没有足够的删除依据，不清理。

## 证据修正

旧 runbook 的“没有 Liquidation 账户部署、候选未通过门禁”不能证明“从未上线、
彻底停止研发”。Compression 曾部署，不能称为仅 V0 演示代码。
归档测试原本仍验证回放边界和检测一致性；移出默认测试集是退役决策，
不是证明测试本身没有价值。

实盘和日常本地研究不调用旧 Parquet 读取函数，但原来的 Orderflow 事件研究、
本地 Paper、Replay CLI 仍调用它。只有成套退役这些入口后才删除该函数。

计划顺序、阶段验证和最终测试结果见
[清理执行计划](plans/2026-10-03-dormant-code-cleanup.md)。
