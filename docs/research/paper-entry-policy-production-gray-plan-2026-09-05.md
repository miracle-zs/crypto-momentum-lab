# Paper Entry Policy 生产灰度计划（2026-09-05）

## 当前状态

代码已推送到 `origin/main`，当前灰度配置对应提交为 `0ad7250`。本计划记录发布边界和执行结果，
不等于允许 Policy 接管 Live 主路径。

Live 与未纳入灰度的服务必须保持默认关闭：

- Live strategy、execution-account-live 和两个 `paper-live-pair` 保持
  `entry_policy_compare_only=false`；
- Policy 不参与订单资格决定；
- 不连接新的真实下单路径；
- 不把 compare-only JSONL 当作订单、成交或 `submit/cancel` 审计账本。

验收证据见 [`paper-entry-policy-acceptance-signoff-2026-09-05.md`](paper-entry-policy-acceptance-signoff-2026-09-05.md)。

## 已执行状态（2026-09-05）

- 阶段 A 的 `3a6cc95` 发布曾触发 EMA daemon 重启，已按条件回滚并修复为
  `9ce5fb3`；修复保留 EMA snapshot 的 `observed_at`、snapshot id 和 config hash。
- 阶段 A 修复版本只重启 Paper，Live/执行账户仍运行 `fa991f6`；随后两个 Paper daemon
  以 `0ad7250` 开启 compare-only，pair、Live 和 execution-account 未重启。
- 完整观察窗口（约 18 分钟，覆盖一个 15 分钟策略周期）报告：普通 Paper
  `candidates=7, matched=7, mismatched=0`；EMA Paper
  `candidates=4, matched=4, mismatched=0`，两份报告均 `status=ok`、严格零容忍退出码 0。
- 延长基线继续保持零差异：普通 Paper 覆盖 `11:17:15–12:01:30 UTC`，累计
  `candidates=15, matched=15, mismatched=0`；EMA Paper 覆盖
  `11:24:45–11:58:30 UTC`，累计 `candidates=10, matched=10, mismatched=0`，
  两份严格报告仍为 `status=ok`、退出码 0。
- 阶段 C 只读预验收：Live 最新对账为 `status=ready`、`mismatch_count=0`，执行账户进程为
  `ready_readonly`；活动 lease 为 1 个。最近 2 小时的 exchange order event 审计持续存在，
  未见待处理队列或 pending fill；Live/执行账户容器仍 `running/0/healthy`。
- 两个灰度容器均 `running/0/healthy`；观测写入独立 JSONL，不改变 Paper simulated fill 或
  Live submit/cancel 路径。

## 发布前检查

1. 确认远端构建产物的源码版本为 `3a6cc95`，并完成 CI/镜像构建。
2. 只读检查服务器当前运行版本、Compose 配置、容器健康、live lease、账户对账和最近的
   `submit/cancel` 审计；不得输出凭证值。
3. 确认 `BINANCE_READ_*` 与 `BINANCE_TRADE_*` 角色变量已配置且权限边界通过预检。
4. 保存当前镜像/Compose 版本作为回滚点；不覆盖数据库，不删除历史审计。
5. 确认 Paper 观测输出目录可写且有磁盘配额；写入失败必须只产生告警，不得改变旧决策。

## 灰度顺序

### 阶段 A：代码更新，开关关闭

只更新代码并重启必要的 Paper 服务；Live 保持旧决策路径，明确检查：

- `entry_policy_compare_only=false`；
- submit 数量、cancel 数量、持仓和账户对账与更新前一致；
- lease、User Data Stream、Hub gap 和进程健康无异常；
- 没有把 query 类 durable telemetry 误写入生产审计。

### 阶段 B：非下单 Paper compare-only

只在 Paper/非下单路径开启 compare-only，使用独立 JSONL sink 和固定窗口汇总命令。观察窗口
至少覆盖一个完整策略周期，记录候选数、matched/mismatched、低基数 reason、首末时间和
报告退出码。该阶段不得连接真实交易账户或触发订单。

### 阶段 C：人工判定

只有同时满足以下条件，才可以讨论下一步灰度：

- 正常窗口 mismatch 为零，或每个差异都有已记录的输入质量解释；
- stale/future/missing EMA 能被分类并触发告警；
- 关闭 compare-only 后旧 Paper 路径不调用 observer；
- Live 的 submit/cancel 审计、账户对账、lease 和持仓状态无回归；
- 已指定回滚负责人和停止条件。

本计划不包含让 Policy 接管 Live 主准入路径；那需要另行审批和独立变更。

## 立即回滚条件

出现以下任一情况，立即关闭 compare-only 并恢复上一版本：健康检查失败、lease 丢失或不明、
账户对账异常、submit/cancel 审计缺失、Hub gap 未按预期 Fail-Closed、观测写入阻塞交易循环、
或出现无法解释的资格差异。

回滚只允许恢复已验证的上一版本和默认关闭配置；不得通过强制 Git 推送、删除数据库记录或
修改订单状态来“修复”观测结果。

## 明确禁止

- 未经单独批准，不在生产 Live 上打开 `entry_policy_compare_only`；
- 不在生产机器上运行本地故障注入脚本；
- 不使用生产 key 做“验证下单”；
- 不把一次 `status=ok` 报告解释为 Policy 已获准接管主路径。
