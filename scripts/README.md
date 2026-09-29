# Operational & Data Scripts (`scripts/`)

本目录存放系统日常运维、离线数据获取与回补、巡检诊断工具。

---

## 目录索引与工具职责

| 脚本 | 职责分类 | 说明 |
|---|---|---|
| `download_official_15m_klines.py` | 数据获取 | 从 Binance 官方 REST API 拉取 15m K线历史 |
| `download_tradingview_futures_15m_klines.py` | 数据获取 | 从 TradingView 获取合约 15m K线辅助校验 |
| `fetch_official_15m_via_server.py` | 数据获取 | 通过生产服务器代理通道拉取 Binance 15m K线 |
| `merge_15m_kline_tail.py` | 数据清洗 | 拼接合并增量 K线尾部数据，填补断点 |
| `merge_runtime_state_exports.py` | 数据清洗 | 合并分散的运行时状态数据导出切片 |
| `backfill_binance_15s_from_aggtrades.py` | 数据重构 | 从 aggTrades 高频归档生成 15s 级聚合行情 |
| `repair_server_state_20260929.py` | 生产运维 | 针对服务器 PostgreSQL 历史状态元数据的专项修复 |
| `materialize_server_paper_snapshot.py` | 生产运维 | 固化服务器模拟运行快照与日志 |
| `build_daily_account_alignment.py` | 巡检对账 | 生成多账户每日对齐基线数据 |
| `inspect_live_market_time_alignment.py` | 巡检对账 | 检查行情数据与服务器时钟时间戳的一致性 |
| `inspect_live_signal_feature_start.py` | 巡检对账 | 检查实盘信号特征启动对齐点与数据连续性 |
| `bundle_dashboard_css.py` | 前端构建 | 操作台 CSS 打包与依赖内联 |
| `run_market_data_smoke.py` | 冒烟测试 | 行情服务 30 分钟稳定性与延迟冒烟脚本 |
| `load_test_market_data_pipeline.py` | 压力测试 | 行情摄取管道高吞吐与消息压力测试 |

---

## 历史研究归档 (`research_archive/`)

早期课题阶段探索性研究脚本（Orderflow 特征分析、爆仓级联、K线边界回放、阶段性回测）已统一归档于 [`research_archive/`](research_archive/README.md)。
正式参数寻优与对账分析请统一使用 [`local_optimization/`](../local_optimization/) 模块。
