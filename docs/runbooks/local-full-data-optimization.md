# 本地数据同步、寻优与实盘对照

核对日期：2026-10-04。当前入口为 local_optimization；旧 scripts/optimize_local_live_constrained.py 等 A–G 命令已不在仓库，本文不再保留不可执行步骤。

local_optimization、server_exports 与研究产物不受 Git 跟踪。只 checkout 仓库不能恢复本地研究源码/缓存；先确认目录存在。研究运行不修改实盘策略配置或发送交易订单。

## 入口与步骤

|步骤|当前入口|
|---|---|
|服务器数据同步|local_optimization/sync_latest_server_data.py|
|参数无关机会池|local_optimization/build_raw_opportunity_pool.py|
|每日寻优与对照|local_optimization/run_daily_local_optimization.py|
|六场景 MTM 看板|local_optimization/generate_six_scenarios_dashboard.py|
|冻结窗口评价|local_optimization/run_walk_forward_analysis.py|
|实盘逐层对照|local_optimization/run_live_reconciliation.py|

先看本地版本 --help，显式传入数据、缓存、参数网格、输出目录及时间边界，避免沿用脚本中的历史默认日期。以下命令只显示帮助：

```bash
.venv/bin/python -m local_optimization.sync_latest_server_data --help
.venv/bin/python -m local_optimization.build_raw_opportunity_pool --help
.venv/bin/python -m local_optimization.run_daily_local_optimization --help
.venv/bin/python -m local_optimization.generate_six_scenarios_dashboard --help
.venv/bin/python -m local_optimization.run_walk_forward_analysis --help
.venv/bin/python -m local_optimization.run_live_reconciliation --help
```

六场景看板接受 --data-dir、--grid-csv、--cache-file、--output-html、--artifact-dir、--workers；--verify-depth 0 表示不截断合规候选。--scheduled-window 才启用每日定时窗口，不要将其默认时间混入宽限规则。

每日流程接受 --date、--data-dir、--grid-csv、--cache-file、--live-dir、--baseline-csv、--report-dir 等。实盘对照接受 --account、--live-dir、--baseline-csv、--start-date、--end-date 和输出路径；本地 acc01/acc02/acc03 名称需检查映射，不能直接按后缀猜生产账户。

## 输入与结果核对

每次运行保存新目录和 UTC 快照身份，核对 Parquet、官方 K 线、账户余额、成交、订单和信号的共同覆盖；文件存在或变大不足以证明完整。同步需先检查服务器/路径/凭证来源，SSH 密码不写入命令、文档或 shell history；生产导出按只读职责执行。

固定搜索空间、成本与退出设置，记录是否使用排名代理和真实可执行价格。用冻结旧参数评价新增数据，再与重新选参结果并排展示。自然保证金约束、MTM、期初/期末持仓与数据选择偏差见[研究评价口径](../research/evaluation.md)。

six_scenarios_equity_comparison.html 和 baseline_15s_mtm_equity_series.csv 仍是有效研究产物；不按体积清除。新输出避免覆盖旧结果，成功标志是报告实际记录的覆盖和结果，不是 HTML 能打开。

## 本地验证

```bash
.venv/bin/python -m pytest local_optimization/tests -q
```

该套测试及数据只在具备本地研究源码的工作区运行，独立于 Git 跟踪的交易系统默认回归。历史研究和手动复现工具见[脚本索引](../../scripts/README.md)。
