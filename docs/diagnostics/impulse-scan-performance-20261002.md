# 行情延迟与策略重复扫描：2026-10-02

## 证据与归因范围

本轮在 43.167.191.253 采集两段 132 秒数据，没有手动发单、修改风险限额或交易参数。行情服务仍为 7d4f6f04，四策略为 bdfa8a0f。12 个容器开始采样时均健康。

- UTC 15:07:49–15:10:01：对行情主进程用 py-spy `--gil --nonblocking --rate 49 --duration 130` 采样，并每秒读取 `/proc/stat`、主线程 `schedstat` 与 cgroup CPU。行情单核 CPU 平均 23.84%；主线程单秒累计运行队列等待最高 506.93 ms，整机 CPU 同期 97.45%；没有 CPU throttling。窗口内 lag 告警 141/84 ms。
- UTC 15:11:28–15:13:40：对 primary 策略使用同样的 profiler，主机及 12 个容器每 250 ms 采样。行情没有 profiler；仍出现 288/232 ms lag。UTC 15:13:01.255 附近，整机 CPU 100%；四策略同时运行，单核 CPU 合计约 125.76%；行情主线程在约 258 ms 窗口累计等 CPU 180.30 ms。多次等待峰值位于 15 秒行情桶关闭后的集中处理窗口。
- primary 424 个有效栈样本中，`find_order_flow_impulses` 调用占约 23.17%，其 `_impulse_metrics` 是最高的业务自耗时函数。行情采样显示日常采集、规范化、聚合、WebSocket 与持久化开销分散，不能据此认定某个行情函数独自解释全部平均 CPU。

这证明存在桶边界 CPU 争用，并定位了一段可消除的策略重复工作；它不证明全部 lag 都由这个函数导致。`market_data_event_loop_lag` 是报告窗口内采样最大值，日志时间不是停顿发生时刻。py-spy 两轮分别报告 269/45 个采样错误，非阻塞与 GIL 筛选存在偏差，栈样本占比不等于整机 CPU 占比。第二轮 sampler 自耗 CPU 1.76 秒，profiler 平均约 1.16% 单核 CPU，尖峰仍可能受观测影响。

## 修复

实时策略每收到一个桶，调用批量事件扫描器重算整个滚动缓冲。启用 5 分钟/30 分钟成交额过滤后，确认点不足 140 桶的候选必然被 `_build_event` 拒绝；旧代码仍先计算这些候选的脉冲、基准、突破与确认。

只收敛扫描起点：

```python
first_index = max(baseline_window + impulse_window - 1, breakout_window)
if volume_filter_enabled:
    first_index = max(first_index, 140 - confirmation_buckets)
```

成交额过滤在确认点计算，故必须减去确认桶数，不能简单把触发点推到第 140 桶。此前不合格候选不会产出事件，也不会启动扫描器冷却；跳过它们保留原来的确认、冷却、历史事件排序、前瞻收益与运行时信号语义。过滤关闭时保持原起点。没有引入新的缓存、线程或跨账户共享状态。

## 验证

- 实际运行时入口的回归测试修改前失败：156 桶缓冲执行 150 次候选计算；修改后 17 次以内。
- 16 组参数与 7 类输入逐项对比旧扫描结果，包括确认 1/2/5/145 桶、冷却 0/4、成交额过滤开/关、多空、139/140 桶边界、缺桶、缺价格、乱序多标的。所有事件字段完全相同。
- 定向策略测试 38 项通过；完整 unit/smoke 3237 项通过、5 项跳过、2 项现有警告。另行开启 `CML_RUN_HUB_NETWORK_TESTS=1` 补跑 4 项原本跳过的本地 Hub 网络测试，全部通过。真实数据库 smoke 仍因未配置测试数据库跳过；本次未改变持久化或事务代码。
- 变更文件 Ruff 和 `git diff --check` 通过。
- 离线基准命令：`.venv/bin/python scripts/diagnostics/cml_impulse_scan_benchmark_20261002.py`。Python 3.13/macOS，156 桶、实盘默认脉冲配置、无信号输入，7 轮交错顺序，每轮 430 次：候选点 151→17，中位单标的耗时 **0.6633→0.08549 ms，约 7.76 倍**。这是局部函数耗时，不是整机提速倍数或端到端订单延迟。

摘要见同目录 `impulse-scan-performance-20261002.json`。原始文件保留在服务器 `/tmp/cml-market-profile-20261002T150749Z.*` 和 `/tmp/cml-boundary-profile-20261002T151128Z.*`；本机 `/tmp/cml-market-profile.*` 与 `/tmp/cml-boundary-profile.*`。

## 部署后验收

待部署后记录实际版本、健康状态与同样口径的资源复测。不得用离线基准代替实盘验证；桶边界争用、行情其余开销及长期内存趋势仍需继续观察。
