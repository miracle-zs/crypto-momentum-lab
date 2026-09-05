# Paper Entry Policy 观测验收清单（2026-09-05）

这份清单只用于 compare-only 的非下单观测，不授权切换生产 Policy、修改 Live 配置或发送真实订单。

## 当前证据

| 门槛 | 结果 | 证据 |
|---|---|---|
| 固定窗口候选集合可重现 | 通过 | Replay/Paper 都为 10 个 candidate，gap reset 已对齐 |
| Paper 与 Policy 正常基线无差异 | 通过 | 100,000 状态、116 symbol：`matched=10`、`mismatched=0` |
| 观测时间语义正确 | 通过 | state-aligned clock 后为 `status=ok`；固定末尾 clock 会被识别为假告警 |
| JSONL 观测有界且可解析 | 通过 | schema=1、空 batch 不写、逐候选详情最多 128 条 |
| 汇总阈值可触发告警 | 通过 | 零容忍阈值下 misaligned 样本返回退出码 2 |
| compare-only 可关闭并回滚 | 通过 | `entry_policy_compare_only=False` 时 observer 不被调用 |
| stale/future/missing EMA 可分类 | 通过 | 200,000 状态观测均经 sink/report 分类为对应 reason 并触发 alert |
| 长时间连续观测 | 通过 | 200,000 状态、约 36 小时、138 symbol，正常基线重复为零差异 |

## 继续观测的固定配置

每次观测必须记录：

- 输入数据路径、窗口起止时间和文件校验信息；
- `reset_on_gap`、策略 `max_gap_seconds` 和 state-aligned clock 规则；
- `entry_policy_compare_only=True`；
- JSONL sink 路径及 schema 版本；
- `--max-mismatches`、`--max-mismatch-rate` 阈值；
- `artifact_repository=None`，不连接 PostgreSQL、Binance 或执行账户。

## 判定规则

### 通过条件

- 正常窗口 `mismatched=0`；
- 所有 mismatch reason 都能映射到已知的故障注入或输入质量问题；
- 观测报告可重复生成，损坏 JSONL 会 fail-closed；
- 告警演练返回非零退出码，关闭 compare-only 后旧路径不调用 observer；
- 至少完成一个更长窗口的重复采样，结果与固定基线一致。

### 禁止动作

- 不把 `status=ok` 等同于 Policy 已获准接管主路径；
- 不在 compare-only 阶段启用生产 Live 配置；
- 不把观测 JSONL 当作订单、成交或 submit/cancel 审计账本；
- 不因单次 `alert` 直接修改规则，先定位时钟、快照或候选集合原因。

## 下一执行项

下一步可以整理验收签字材料，并决定是否在更长的本地窗口继续采样；生产默认配置仍保持关闭，
不部署、不切换、不触碰生产凭证。
