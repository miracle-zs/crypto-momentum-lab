# Paper Entry Policy 观测验收记录（2026-09-05）

## 结论

本地 compare-only、非下单观测链路已完成验收证据收集：正常窗口零 mismatch，受控的
stale/future/missing EMA 故障均能经过 JSONL sink、汇总报告和阈值退出码被识别。

这只证明观测链路和回滚 seam 可用，不代表 Policy 已获准接管生产主准入路径，也不授权
修改生产 Live 配置、连接生产账户或发送真实订单。

## 证据摘要

| 项目 | 结果 |
|---|---|
| 固定窗口正常基线 | 100,000 状态、116 symbols，`matched=10`，`mismatched=0` |
| 长窗口正常基线 | 200,000 状态、约 36 小时、138 symbols，`matched=32`，`mismatched=0` |
| stale EMA 故障 | 32/32 分类为 `ema_stale`，状态 `alert` |
| future EMA 故障 | 32/32 分类为 `ema_snapshot_from_future`，状态 `alert` |
| missing EMA 时间戳 | 32/32 分类为 `ema_unavailable`，状态 `alert` |
| 告警退出码 | 零容忍阈值下返回 2；正常基线返回 0 |
| 回滚 seam | compare-only 关闭时 observer 不被调用，旧 Paper 路径独立运行 |
| 生产影响 | 无数据库、交易所、执行账户或真实订单访问 |

详细执行步骤和固定配置见
[`paper-entry-policy-acceptance-checklist-2026-09-05.md`](paper-entry-policy-acceptance-checklist-2026-09-05.md)。

## 待人工确认

- [ ] 复核正常基线与三类故障报告的输入路径、窗口时间和阈值参数。
- [ ] 确认继续保持 `entry_policy_compare_only=False` 的生产默认值。
- [ ] 确认后续若扩大观测，仍使用 `artifact_repository=None`、不连接生产账户。
- [ ] 若要规划生产灰度，另行批准灰度范围、回滚负责人和停机条件；不得从本记录推断授权。

## 当前决策

默认决策：**保持观测实现合入代码但关闭生产开关，不部署、不切换、不触碰生产凭证。**

人工签字后，下一项工作只能二选一：

1. 继续本地非下单长窗口采样并补充重复性证据；或
2. 单独建立生产灰度变更计划，先完成审批和可回滚演练，再讨论是否实施。
