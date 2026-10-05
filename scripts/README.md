# 运维、数据与诊断脚本

日常入口以本目录实际文件和各脚本 --help 为准；不会因保留历史脚本就重新启用已退役交易能力。

|用途|入口|
|---|---|
|官方/辅助 15m K 线获取|download_official_15m_klines.py、download_tradingview_futures_15m_klines.py、fetch_official_15m_via_server.py|
|行情回补与导出合并|backfill_binance_15s_from_aggtrades.py、merge_15m_kline_tail.py、merge_runtime_state_exports.py|
|只读核对|build_daily_account_alignment.py、inspect_live_market_time_alignment.py、inspect_live_signal_feature_start.py|
|前端 CSS 构建|bundle_dashboard_css.py|
|行情 smoke / 压力测试|run_market_data_smoke.py、load_test_market_data_pipeline.py|
|事故回归与历史采样|diagnostics/；[事故证据](../docs/diagnostics/incidents.md)|

部署和告警工具在 deploy/ops；本地研究流程在被 Git 忽略的 local_optimization，见[研究手册](../docs/runbooks/local-full-data-optimization.md)。materialize_server_paper_snapshot.py 面向历史模拟快照，不是当前 Paper 服务入口。

## 历史工具

- [research_archive](research_archive/README.md)：旧研究/回放；部分依赖退役代码。
- [review_archive](review_archive/README.md)：手动复现；正式安全回归在当前测试中。

归档不进入默认测试集。旧运行源码可在基线 02e6581f3bc71feac0f91f84fa405460ea26730f 查看；本地研究源码和数据另需备份，Git 无法恢复被忽略文件。
