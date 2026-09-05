# Paper Entry Policy 生产灰度执行记录（2026-09-05）

## 阶段 A 结果

- Paper 服务先发布 `3a6cc95`，发现 EMA daemon 在启动后因快照来源元数据缺失重启。
- 已按回滚条件恢复到 `fa991f6`，定位并修复为 `9ce5fb3`（保留 `observed_at`、snapshot id 和 config hash）。
- `9ce5fb3` 已重新构建并只重启四个 Paper 服务；Live、execution-account、market-data、Postgres 未重启。
- 修复版本确认：四个 Paper 与 Live/执行账户健康，重启次数为 0；EMA daemon 连续写入
  `strategy_checkpoint_persisted`；Live/执行账户仍为 `fa991f6`。
- `entry_policy_compare_only` 在本阶段保持关闭，Policy 没有参与订单资格决定。

## 阶段 B 部署范围

提交 `0ad7250` 只在两个 `paper-live-daemon` 服务开启非下单 compare-only：

- `paper-b1-gainer100` → `/app/research-data/entry-policy-observations/paper-account-14-entry-policy.jsonl`
- `paper-b1-gainer100-ema` → `/app/research-data/entry-policy-observations/paper-account-15-entry-policy.jsonl`

两个 `paper-live-pair` 服务、Live strategy、execution-account-live 均未加开关。JSONL
写入 `research-data` volume，只记录有候选的 compare-only 观测，不写订单、fill、position 或
`submit/cancel` 审计；Paper 原有 simulated fill/decision 路径保持不变。

## 完整周期结果

初始观察时间约为 18 分钟（11:17:15–11:35:15 UTC），覆盖一个 15 分钟策略周期。随后
继续积累延长基线。使用 `--max-mismatches 0 --max-mismatch-rate 0 --fail-on-alert` 运行
严格报告：

| Paper 服务 | records | candidates | matched | mismatched | 状态/退出码 |
|---|---:|---:|---:|---:|---|
| b1-gainer100 | 7 | 7 | 7 | 0 | `ok` / 0 |
| b1-gainer100-ema | 4 | 4 | 4 | 0 | `ok` / 0 |

EMA 报告中的 `ema_filter_failed` 只是 Policy reason 统计，不是 mismatch；两份报告的
`mismatch_reasons` 均为空。

延长基线结果：普通 Paper 覆盖 `11:17:15–12:01:30 UTC`，累计 `records=15`、
`candidates=15`、`matched=15`、`mismatched=0`；EMA Paper 覆盖
`11:24:45–11:58:30 UTC`，累计 `records=10`、`candidates=10`、`matched=10`、
`mismatched=0`。两份报告仍为 `status=ok`、严格退出码 0。

最终服务检查：两个灰度 daemon 使用 `0ad7250`、均 `running/0/healthy`；两个 Paper pair
仍为 `9ce5fb3`、均 `running/0/healthy`；Live strategy 和 execution-account-live 仍为
`fa991f6`、均 `running/0/healthy`。

## 下一步与回滚

阶段 B 已通过一个完整周期及延长基线的观测门槛，下一步是人工验收并决定是否扩大 Paper
覆盖；这不等于批准 Policy 接管 Live。任何健康失败、观测写入阻塞、无法解释的差异或数据库/市场数据异常，立即移除
两个 compare-only 参数并恢复 `CML_CODE_COMMIT=9ce5fb3` 的默认关闭路径。不得重启 Live 或
发送验证下单；Policy 仍不得接管 Live 主准入路径。
