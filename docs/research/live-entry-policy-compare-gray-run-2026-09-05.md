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

## 实际发布记录

- 初次用 `a75248d` 重启 Live 时，镜像中尚未包含 Live CLI 实现，进程报
  `No such option: --entry-policy-compare-only`；已立即回滚到 `fa991f6`，没有修改
  execution account、Paper 服务或订单状态。
- 修复提交 `8e2a7b2` 补齐 Live compare-only CLI、daemon 比较记录、EMA/Universe 快照元数据，
  并增加回归测试；相关 121 个单元测试通过。
- 2026-09-05 12:43 UTC 只重启 `live-strategy`，镜像为 `crypto-momentum-lab-app:8e2a7b2`，
  命令含 `--entry-policy-compare-only`，健康检查通过，重启次数为 0。
- 为绑定新代码提交，按用户确认创建了临时 approval
  `approval-a8599d15-5dd2-46ba-95c7-e066b0d7daae`，有效至
  2026-09-05 14:42 UTC；策略哈希、风险哈希、迁移版本和原有上限未改变。
- execution account 仍为 `fa991f6`，重启次数为 0；4 个 Paper 容器保持原镜像且健康。
- 2026-09-05 12:45 UTC 最新账户对账为 `ready`，`mismatch_count=0`，余额 11、持仓 0、
  未结订单 1、成交 0。发布后查询窗口内尚未产生 Live candidate signal，因此暂没有可统计的
  Policy mismatch；这不等同于 mismatch=0，需在有候选信号后继续观察比较记录。
