# Paper compare-only 非下单观测记录（2026-09-05）

## 目的

验证真实 Paper daemon 是否能使用与 Replay 相同的
`EntryPolicyComparisonRequest`，并确认 compare-only 不创建订单、不保存 fill，也不改变旧的
Paper filter 结果。

## 运行边界

- 输入：本地固定 Replay 窗口的 100,000 条 15 秒 Parquet 状态，116 个 symbol；
- 环境：`research`，内存 repository，未连接 PostgreSQL、Binance 或生产服务器；
- 配置：Paper `entry_policy_compare_only=True`，entry symbol pool 为固定全量 symbol，
  universe snapshot 为本地合成的全量快照，EMA filter disabled；
- 顺序：按 `bucket_start, symbol`，与固定 Replay 的时间顺序一致；
- 结果文件：临时文件 `/private/tmp/cml-entry-policy-replay.iWzK4t/paper-entry-policy-observation-global.json`，
  未写入生产或数据库。

## 结果

Paper daemon 处理 100,000 条状态，产生 10 个可比较的 entry candidate：

```text
candidates=10
matched=10
mismatched=0
legacy_eligible=10
policy_eligible=10
policy_reasons={}
mismatch_reasons={}
```

这证明在共同的 10 个候选上，Paper adapter 使用共享 request seam 后没有产生资格差异。

## 发现的候选集合边界

固定 Replay 报告产生 11 个 candidate，其中额外的一个是 `VELVETUSDT`。Paper 没有这个
candidate，不是 Policy 拒绝，而是运行时 gap 语义不同：

- `VELVETUSDT` 在 `2026-08-07T03:31:30Z` 到 `2026-08-07T05:01:30Z` 之间缺少
  **5,400 秒**状态；
- Paper daemon 依据策略的 `max_gap_seconds=30` 执行 symbol gap reset，清空该 symbol 的
  warmup buffer，因此 `05:09:45Z` 的信号不会生成；
- Replay 核心目前直接把状态交给策略，没有执行同样的 gap reset，所以多生成了一个候选。

因此本次不能把 Replay 的 11 个候选直接与 Paper 的 10 个候选合并统计。Policy 的
`matched=10/10` 只对共同候选有效；候选集合必须先对齐，才能用 Paper/Replay 报告证明完整
的等价性。

## 下一门槛

1. 候选集合对齐后，再接入真实闭合 EMA snapshot，观察 `ema_unavailable`、`ema_stale` 和
   `ema_snapshot_from_future`；
2. 继续保持 compare-only 默认关闭，不启用生产 Live，也不让 Policy 参与 submit/cancel。

## Gap reset 对齐结果

Replay 已增加 `reset_on_gap` 选项，默认开启；每个 symbol 的相邻状态间隔超过策略声明的
`max_gap_seconds` 时，Replay 调用策略的 `reset_symbol()`，与 Paper daemon 的行为一致。
`--no-reset-on-gap` 仅用于需要复现旧报告的历史兼容场景，报告和 compare-only 报告都会记录
`replay_options`。

固定窗口在该语义下重新运行：Replay 与 Paper 都产生 10 个候选，随后 compare-only CLI 报告为
`matched=10`、`mismatched=0`。这解决了候选集合边界问题；下一步才适合接入真实 EMA 快照。

## 真实 EMA snapshot 观测

在同一 100,000 条状态窗口上，Paper adapter 接入历史闭合 EMA5/EMA10 snapshot（包括
snapshot observed_at、snapshot id 和 config hash），entry filter 同时启用 EMA5/EMA10：

```text
candidates=10
matched=10
mismatched=0
legacy_eligible=5
policy_eligible=5
policy_reasons={"ema_filter_failed": 5}
mismatch_reasons={}
```

两个候选的 EMA 数值缺失仍按原始输入保留，没有填充伪造值；在有 snapshot 时间但 EMA 数值
为空的情况下，Policy 将其归入 `ema_filter_failed`。本次运行仍是内存 Paper、无订单/无 fill，
且与 gap-reset 对齐后的 Replay compare-only 结果一致。

下一步对同一 adapter 注入过期和未来 snapshot，确认 `ema_stale` 与
`ema_snapshot_from_future` 在运行时也能被稳定分类；compare-only 继续默认关闭。

## EMA 时间故障注入结果

在同一 Paper daemon、同一 100,000 条状态窗口上，只改变 EMA snapshot 的来源时间：

| 场景 | candidates | legacy eligible | policy eligible | matched | mismatched | mismatch reason |
|---|---:|---:|---:|---:|---:|---|
| 回拨 16 分钟 | 10 | 5 | 0 | 5 | 5 | `ema_stale: 5` |
| 前移 1 分钟 | 10 | 5 | 4 | 9 | 1 | `ema_snapshot_from_future: 1` |
| 缺少 `ema_observed_at` | 10 | 5 | 0 | 5 | 5 | `ema_unavailable: 5` |

全部故障都被 Policy 稳定分类，且没有下单副作用。注意 `policy_reasons` 会统计全部候选的
原因，所以未来场景有 2 个 `ema_snapshot_from_future`，其中只有 1 个改变了资格；另一个
候选旧规则本来就因 EMA 数值拒绝，因此计入 `matched`。

这组结果说明运行时 snapshot 元数据已经足以区分“策略值不通过”和“来源时间不可信”；下一步
可以把相同的有界汇总接入非下单长期观测，而不是直接改变生产准入。

## 非下单 JSONL sink

Paper compare-only observer 现在提供独立的 `PaperEntryPolicyComparisonJsonlSink`。通过
`paper-live-daemon --entry-policy-compare-only --entry-policy-compare-output PATH` 可将每个
state/candidate batch 追加到本地 JSONL；不开启这两个选项时没有文件写入。记录包含完整的
低基数 summary，以及最多 128 条逐候选详情；超出上限时保留完整 summary，并用
`comparison_detail_count`/`comparisons_truncated` 标记详情截断。该文件不是订单或成交账本，
也不复用 Paper artifact repository。
没有候选的 market state 不写空 JSONL 记录，因此连续运行时文件只包含实际需要比较的 batch。

本地单元测试已验证目录自动创建、JSONL schema、summary 不因详情截断而失真、重复 close 和
关闭后写入保护。下一步是把该文件作为非下单观察运行的输入，按时间窗口聚合 mismatch reason，
设定阈值和告警/回滚演练；本 sink 尚未接入生产默认配置。

## 窗口汇总与阈值判定

新增 `cml-strategy-runner entry-policy-observation-report`，读取 JSONL 后输出单独的窗口报告：

```bash
cml-strategy-runner entry-policy-observation-report \
  --input entry-policy.jsonl \
  --output entry-policy-window.json \
  --max-mismatches 0 \
  --max-mismatch-rate 0 \
  --fail-on-alert
```

报告合并所有 batch 的低基数 summary，保留首末观测时间、mismatch rate、触发原因和
`ok/alert` 状态。默认任意 mismatch 都会进入 `alert`；`--fail-on-alert` 才会以退出码 2
结束，允许先保留报告再决定是否阻断自动流程。读取到坏 JSON、错误 schema 或非法计数时直接
失败，不把损坏数据当成“零差异”。

## 固定窗口 sink 基线

使用本地 `market_states_15s` 固定窗口做了一次端到端运行：100,000 条状态、116 个 symbol，
内存 Paper daemon、无 artifact repository；sink 生成 10 条候选 batch，汇总结果为：

```text
records=10
candidates=10
matched=10
mismatched=0
mismatch_rate=0
status=ok
```

试跑时发现，如果把 clock 固定在窗口末尾，Policy 会因候选相对该 clock 已过期而制造 10 个
假 mismatch。最终运行改为每处理一个历史 state 就将 clock 推进到该 state 的 bucket_end，
与 Paper/Replay 的 observed_at 语义一致；该时间对齐是观测 harness 的必要前提。

## 关闭开关的回滚验证

补充 contract test：`entry_policy_compare_only=False` 时，即使 daemon 收到 observer，也不会
调用 observer；旧 Paper 路径可以在不写观测 JSONL 的情况下独立运行。该验证仍是本地控制流
测试，没有改变生产配置。

## 200,000 状态长窗口与故障链路

进一步读取本地历史 CSV 的前 200,000 条状态，覆盖约 36 小时、138 个 symbol。使用同一
state-aligned clock 和内存 Paper daemon，正常基线结果为：

```text
records=32
candidates=32
matched=32
mismatched=0
mismatch_rate=0
status=ok
```

在相同窗口中让 EMA 数值保持通过、只注入来源时间/元数据故障，并让每份 JSONL 都经过
`entry-policy-observation-report`：

| 故障 | candidates | mismatched | reason | status |
|---|---:|---:|---|---|
| snapshot 回拨 16 分钟 | 32 | 32 | `ema_stale` | `alert` |
| snapshot 前移 1 分钟 | 32 | 32 | `ema_snapshot_from_future` | `alert` |
| 缺少 `ema_observed_at` | 32 | 32 | `ema_unavailable` | `alert` |

故障测试刻意让旧规则全部放行，因此全量 mismatch 是预期结果；它证明的是 sink、汇总和
阈值告警链路能稳定区分三类原因，不是生产 EMA 数据的结论。长窗口正常基线和故障链路均未
连接数据库、交易所或订单系统。

对第一次固定 clock 结果执行 `--fail-on-alert` 演练：10 个 mismatch 在零容忍阈值下输出
`status=alert`，进程返回退出码 2；state-aligned 基线在相同阈值下为 `status=ok`。这只验证
本地告警判定，不代表已经接入生产通知或自动切换。
