# Research Archive (`scripts/research_archive/`)

本目录保存项目早期各研究课题、阶段性事件研究（Event Study）与历史回放分析脚本。

2026-10-03 清理后，本目录只作为历史研究档案，不承诺每个脚本与当前代码兼容。
旧 Research、Replay/Paper、Shadow CLI 以及 Compression/Liquidation 实现已从
运行包移除。依赖这些实现的历史脚本（例如 `backfill_paper_grace_exits.py`）
须在清理前基线 `02e6581f3bc71feac0f91f84fa405460ea26730f` 的独立 checkout
中复现；不应为档案脚本恢复当前运行包中的旧依赖。其专属回放测试也已退出默认测试集。

> ⚠️ **说明**：
> 1. 本目录下脚本属于历史研究成果沉淀与复现存档，不再承担日常生产运维与持续部署职能。
> 2. 正式参数寻优（8D 网格、Pareto 前沿、多场景稳健推荐）已统一迁移至模块化流水线：[`local_optimization/generate_six_scenarios_dashboard.py`](../../local_optimization/generate_six_scenarios_dashboard.py)。
> 3. 正式实盘与回放 6 层逐层对账已统一由 [`local_optimization/run_live_reconciliation.py`](../../local_optimization/run_live_reconciliation.py) 执行。

---

## 归档课题分类

### 1. 订单流特征与假信号研究 (Orderflow)
- `analyze_orderflow_core_features.py`: 核心订单流特征分布与信息比率分析
- `analyze_orderflow_15m_false_signals.py`: 15m 级别假突破与噪声过滤研究
- `analyze_orderflow_b0_high_returns.py`: B0 桶高收益信号样本特征归纳
- `analyze_orderflow_daily_regime.py`: 订单流在日级别多空市场状态下的表现
- `analyze_orderflow_forward_filters.py`: 前向收益过滤规则研究
- `analyze_orderflow_structural_stop.py`: 结构化止损规则对比
- `build_orderflow_candidate_dataset.py`: 订单流候选特征集构建
- `build_orderflow_capital_series.py`: 资金曲线重构
- `build_orderflow_optimization_visual.py`: 可视化报告生成
- `compare_orderflow_b1_exit_rules.py`: B1 平仓规则对比
- `generate_orderflow_grace_svgs.py` / `generate_orderflow_grace_visualization.py`: 宽限期 SVG 生成
- `replay_orderflow_break_even.py`: 保本止损规则回放
- `update_orderflow_visualization.py`: 历史可视化刷新

### 2. 爆仓级联事件研究 (Liquidation)
- `analyze_liquidation_daemon_gaps.py`: 爆仓守护进程漏单与事件间隙分析
- `analyze_liquidation_version_transition.py`: 爆仓策略版本迁移差异
- `audit_liquidation_candidate_gates.py`: 准入过滤门控审计
- `generate_liquidation_replay_visualization.py`: 爆仓回放可视化
- `replay_liquidation_entry_variants.py`: 爆仓入场变体策略回放

### 3. K线边界与趋势过滤研究 (Candle & Trend)
- `analyze_b1_trend_filters.py`: B1 趋势过滤条件有效性
- `analyze_breakout_acceptance.py`: 突破有效性接受域分析
- `analyze_candle_entry_boundary.py`: K线入场时间边界效应
- `analyze_cycle_sensitivity.py`: 周期敏感度分析
- `analyze_paper_b1_ema_filters.py`: EMA 均线过滤对比
- `analyze_three_day_momentum.py`: 3日动量衰减特征
- `replay_candle_entry_boundary.py`: 入场边界回放
- `replay_candle_exits_server.py`: 出场逻辑回放
- `report_live_ema_entry_relation.py`: EMA 入场关系报告
- `research_volume_filters.py`: 成交量比率阈值研究
- `select_parameter_candle15m.py`: 15m K线参数挑选

### 4. 历史涨幅榜与基线回测 (Gainer & Baseline Backtests)
- `analyze_live_baseline.py` / `audit_live_baseline_replay.py`: 早期实盘基线回测与审计
- `analyze_live_gainer_topn.py`: 涨幅榜 TopN 过滤策略
- `analyze_live_limit_entry.py`: 限价挂单成交概率
- `analyze_live_orderflow_winner_features.py`: 胜者特征
- `analyze_live_rank_filters.py`: 榜单排名过滤
- `analyze_live_trading_data.py`: 实盘成交流水解析
- `analyze_server_paper_accounts.py`: 早期模拟账户分析
- `analyze_server_pool_labels.py`: 早期动量池标签分析
- `backfill_paper_grace_exits.py`: 宽限期平仓回补
- `backtest_live_exit_grace.py`: 出场宽限期回测
- `backtest_live_gainer_top20*.py`: 涨幅榜 Top20 系列回测
- `backtest_paper05_gainer_long_b1_088_official.py`: Paper05 官方回测
- `backtest_signal_filters.py`: 信号过滤回测
- `compare_parameter_equity.py`: 参数权益曲线对比
- `diagnose_research_vs_live.py`: 研究与实盘偏离初诊
- `generate_server_equity_visualization.py`: 服务器权益可视化
- `reconcile_live_account_pnl.py`: 早期 PnL 对账脚本
