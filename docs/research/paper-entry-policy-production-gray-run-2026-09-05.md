# Paper Entry Policy 生产灰度执行记录（2026-09-05）

## 阶段 A 结果

- Paper 服务先发布 `3a6cc95`，发现 EMA daemon 在启动后因快照来源元数据缺失重启。
- 已按回滚条件恢复到 `fa991f6`，定位并修复为 `9ce5fb3`（保留 `observed_at`、snapshot id 和 config hash）。
- `9ce5fb3` 已重新构建并只重启四个 Paper 服务；Live、execution-account、market-data、Postgres 未重启。
- 当前确认：四个 Paper 与 Live/执行账户健康，重启次数为 0；EMA daemon 已连续写入
  `strategy_checkpoint_persisted`；Live/执行账户仍为 `fa991f6`。
- `entry_policy_compare_only` 在本阶段保持关闭，Policy 没有参与订单资格决定。

## 阶段 B 范围

本提交只在两个 `paper-live-daemon` 服务开启非下单 compare-only：

- `paper-b1-gainer100` → `/app/research-data/entry-policy-observations/paper-account-14-entry-policy.jsonl`
- `paper-b1-gainer100-ema` → `/app/research-data/entry-policy-observations/paper-account-15-entry-policy.jsonl`

两个 `paper-live-pair` 服务、Live strategy、execution-account-live 均不加开关。JSONL
写入 `research-data` volume，只记录有候选的 compare-only 观测，不写订单、fill、position 或
`submit/cancel` 审计；Paper 原有 simulated fill/decision 路径保持不变。

## 观察与回滚

每个完整策略周期检查：容器健康、重启次数、checkpoint、观测文件增长、候选数、
`matched/mismatched`、低基数 reason 和报告退出码。报告命令示例：

```bash
cml-strategy-runner entry-policy-observation-report \
  --input /app/research-data/entry-policy-observations/paper-account-14-entry-policy.jsonl \
  --max-mismatches 0 --max-mismatch-rate 0 --fail-on-alert
```

任一 Paper 健康失败、观测写入阻塞、无法解释的差异或数据库/市场数据异常，立即移除两个
compare-only 参数并恢复 `CML_CODE_COMMIT=9ce5fb3` 的默认关闭路径。不得重启 Live 或发送
验证下单。
