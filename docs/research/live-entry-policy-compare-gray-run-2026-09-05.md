# Live Entry Policy compare-only 灰度记录（2026-09-05）

## 授权与范围

用户已明确确认开启 Live compare-only。本次只给 `live-strategy` 增加
`--entry-policy-compare-only`：旧规则仍负责实际 entry eligibility、submit/cancel 和订单状态；
Policy 只生成同一 `EntryPolicyComparisonRequest` 的比较结果并写入既有 Live signal 记录。

本次不修改：

- `execution-account-live`、market-data、Postgres 或任何 Paper 服务；
- `submit/cancel` 审计 allow-list、交易 key、lease、风险配置和订单执行状态机；
- Policy 主准入路径。compare-only 不会替换 `effective_entry_candidates`。

## 发布与回滚

- Compose 配置提交后只重建 `live-strategy`；执行账户保持原容器和版本。
- 回滚优先移除 `--entry-policy-compare-only` 并恢复已验证的 Live 镜像
  `fa991f6`；不删除数据库记录，不修改订单状态。
- 立即停止条件：Live/exec 健康失败、lease 丢失、对账 mismatch、compare 记录异常增长或
  任何 submit/cancel 行为变化。

## 观察指标

首个窗口检查 Live/exec 容器健康和重启次数、lease 续租、`ready_readonly`、
`account_reconciliation_runs.mismatch_count`、订单生命周期审计，以及 Live signal 记录中的
`entry_policy_compare_only`、`entry_policy_comparison_summary` 和 skip reason。只读汇总不把
Policy 比较结果当作订单或成交审计。
