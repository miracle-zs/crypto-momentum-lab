# Entry Policy compare-only 固定回放记录（2026-09-05）

## 目的

验证 `EntryEligibilityPolicy` 与旧准入规则在真实历史输入形状下的比较结果，重点观察
EMA 快照的正常、过期和未来时间语义。此次运行只读取本地导出文件，不连接 Binance、PostgreSQL
或任何下单接口，也不改变 Paper/Live 的执行结果。

## 固定窗口与输入

- 状态来源：`data/server-paper-accounts-api-20260815T134515Z/runtime_market_states_15s_full_latest.csv.gz`
- 取该文件按原始顺序的前 **100,000** 条 15 秒状态；窗口为
  `2026-08-07T00:56:15Z` 至 `2026-08-07T08:57:30Z`
- 覆盖 116 个 symbol，策略核心产生 11 个 signal / 11 个 entry candidate
- EMA 来源：同一导出目录的 `official_plus_klines_15m_latest.csv.gz`，按候选时间之前的
  已闭合 15 分钟 candle 计算 EMA5/EMA10，使用 200 根 lookback 的配置形状
- 比较 adapter：`domain/strategy/entry_policy_compare.py`

## 结果

| 场景 | 旧规则可入场 | Policy 可入场 | 布尔差异 | Policy 原因 |
|---|---:|---:|---:|---|
| EMA/universe 未启用（基线） | 11 | 11 | 0 | 无 |
| 使用真实闭合 EMA5/EMA10 | 5 | 5 | 0 | 6 个 `ema_filter_failed` |
| 同一 EMA 值，但快照人为回拨 16 分钟 | 5 | 0 | 5 | 11 个 `ema_stale`，其中 5 个改变资格 |
| 同一 EMA 值，但快照人为放到未来 1 分钟 | 5 | 4 | 1 | 2 个 `ema_snapshot_from_future`，其中 1 个改变资格 |

“回拨/放到未来”是故障注入，用来验证分类，不是生产数据修正。它证明旧的 EMA 规则只看
数值，而 Policy 还检查来源时间；若直接切换主路径，这类差异可能静默改变下单资格，正是
compare-only 需要在切换前暴露的风险。

## 初始边界（CLI 接入前）

本次证据使用了共享纯 adapter，但离线 `cml-strategy-runner replay` 当前仍只运行策略核心，
没有正式的 `--entry-policy-compare-only` 选项，也没有把 EMA/universe 快照作为 Replay 报告
输入。因此不能把这次手工比较称为已完成的 Replay CLI 集成。

Replay 的显式输入契约、快照文件解析和可序列化报告接口现已落地：
`EntryPolicyComparisonRequest` 收敛候选、旧规则结果、EMA/universe 快照和 source trace，
`build_entry_policy_replay_report()` 拒绝缺失、重复或未知候选输入，避免部分比较被误报为
干净结果；`cml-strategy-runner replay` 通过 `--entry-policy-compare-only` 生成独立的
`*-entry-policy.json` 报告。候选的订单身份仍绑定本次新生成的 Replay 报告，不从输入文件
读取，因此旧报告或被修改的候选 payload 不会静默混入。此功能不改变策略核心、simulated
fill 或 Paper/Live 主路径，也没有部署生产 compare-only。

CLI 输入的每条 request 必须包含 `candidate_id`、`source_trace_id`、旧规则拒绝原因、
gate/entry 配置、`entry_price`/`ema5`/`ema10`、`observed_at`、EMA 元数据和
`universe_snapshot`（没有值时显式写 `null`）；Decimal 使用字符串，时间使用带时区的 ISO
字符串。输入 schema 只接受 `schema_version=1` 和已定义字段，缺字段或多余字段都会拒绝。

随后用同一固定窗口正式执行 CLI：Replay 读取 100,000 条状态并生成 11 个 candidate，
compare-only 报告为 `matched=11`、`mismatched=0`、`legacy_eligible=5`、
`policy_eligible=5`，全部 Policy reason 为 6 个 `ema_filter_failed`。这次结果与前面的
共享 adapter 手工验证一致，说明输入文件解析、候选绑定、报告写入和策略 Replay 的组合根
已经闭合；Arrow 在沙箱中打印的 sysctl 权限提示不影响运行结果。

Replay 之外，Paper/Live 的 compare-only adapter 也已改为先构造同一个不可变
`EntryPolicyComparisonRequest`，再调用共享 Policy adapter；因此三条路径不再各自拼接一套
参数。运行时现在额外输出有界 `EntryPolicyComparisonSummary`，其计数和
`policy_reasons`/`mismatch_reasons` 与 Replay 报告一致，逐候选详情仍保留 source trace 和
candidate ID。该输出只进入 Paper 结构化日志或 Live signal recorder filter context，默认关闭，
不参与任何 submit/cancel 决策。

随后为使固定 Replay 与 Paper 的候选集合一致，Replay 默认启用 per-symbol gap reset：相邻
状态间隔超过策略 `max_gap_seconds` 时调用 `reset_symbol()`。旧报告若需复现可使用
`--no-reset-on-gap`；运行选项会写入 Replay 和 compare-only 报告。固定窗口在对齐后产生 10
个候选，Paper 与 Replay 的 compare-only 结果均为 `matched=10`、`mismatched=0`。

同一 gap-reset 对齐后的候选集合又在 Paper daemon 中接入历史闭合 EMA snapshot 运行；结果为
`legacy_eligible=5`、`policy_eligible=5`、`matched=10`、`mismatched=0`，5 个拒绝均为
`ema_filter_failed`。这说明 request seam、snapshot 元数据和 Paper 旧过滤在正常历史输入下
可以闭合；下一步才是注入 stale/future 时间故障，验证 Policy 的时间语义分类。

同一 Paper runtime adapter 的时间故障注入结果与 Replay 语义一致：回拨 16 分钟得到
`ema_stale` 5 个资格差异；前移 1 分钟得到 1 个 `ema_snapshot_from_future` 资格差异；
移除 `ema_observed_at` 得到 5 个 `ema_unavailable` 资格差异。其余同样原因但旧规则本来就
拒绝的候选只计入 `policy_reasons`，不计入 `mismatch_reasons`。

Paper compare-only 还新增了显式的本地 JSONL observer sink。它保存每个 batch 的完整低基数
汇总和有限数量的逐候选详情，默认不启用，且不写 Paper artifact/order/fill 表；这为下一步
连续观察和阈值告警提供了输入，但当前仍未部署或启用生产 Live。

在此之上，`entry-policy-observation-report` 可以对 JSONL 做窗口级汇总，并用最大 mismatch
数量/比例产生 `ok` 或 `alert` 状态；输入损坏会 fail-closed。该报告命令仍是本地只读工具，
没有改变 Paper/Live 主路径。

固定窗口的 Paper sink 基线已完成：100,000 条状态生成 10 个 candidate batch，报告为
`matched=10`、`mismatched=0`、`status=ok`。期间确认观测 clock 必须跟随历史 bucket 推进；
固定在窗口末尾会把 candidate expiry 误报为 Policy mismatch。

同一 misaligned clock 文件在零容忍阈值加 `--fail-on-alert` 下返回退出码 2；对齐基线返回
`ok`。因此时间对齐错误会被观测报告显式阻断，而不会静默进入“无差异”结论。

之后用本地历史 CSV 前 200,000 条状态做长窗口 Paper sink 观测：正常基线为 32 个 candidate、
`matched=32`、`mismatched=0`；stale、future、missing EMA 故障分别经 JSONL/report 链路得到
`ema_stale`、`ema_snapshot_from_future`、`ema_unavailable` 告警。所有运行仍是内存 Paper、
无数据库/交易所/订单副作用。
