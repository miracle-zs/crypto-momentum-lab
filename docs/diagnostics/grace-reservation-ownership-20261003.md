# 平仓预留归属与启动恢复：2026-10-03

## 生产复现

GTC 补全后，四账户 VELVET 宽限退出均取得币安确认，但 UTC 16:00 下一根正式收盘使 primary/account-2/account-3 生成重复退出。Book 正确拒绝重复占用已经全部预留的 1477 数量，返回 ReservationConflictError；Coordinator 将其泛化成 OrderPreSubmissionError，收盘任务发生致命异常，策略退出并由 Docker 重启。

恢复阶段还出现 `cannot configure submission after execution starts`：启动 `reconcile_all(include_confirmed=True)` 已创建按键队列，Daemon 随后安装提交仓储与最终准入规则，旧 guard 把对账队列存在误判为已经开始发单。重复启动集中重放历史、建立数据库连接并热身，引发 CPU 连续高负载。

UTC 15:59:54.320–16:09:54.619 的 600 秒观察未通过：CPU 加权平均 75.37%、P95 100%，>=90% 累计 276.92 秒、最长 109.58 秒；primary/account-2/account-3 出现重启，未发生 OOM。可用内存最低 824.12 MiB，swap-in 35.21 MiB、swap-out 19.56 MiB。重启导致 cgroup CPU 计数清零，这些策略的整窗口平均 CPU 不可比较，摘要明确置空，不能使用负数均值或宣称性能通过。

失败摘要与窗口日志见 `grace-reservation-failed-observation-20261003.json`。

## 根因与边界收敛

1. `ExchangeOrderRow` 存储交易所字段，没有 batch_id/allocations；其领域解码得到的 plan 因而没有批次关联。`managed_live_positions_from_views` 只按 plan 分配关联，而当前 `ExecutionBook.read` 未暴露已提交的活动预留。即便强制刷新上下文，已确认退出也会在策略视图中消失。
2. 当前账本读取附上既有的活动预留；历史 event_cut 读取保持原语义。适配器按精确 client_order_id=command_id 将挂单与 Book 的批次预留关联，各批次剩余数量取自 committed reservation。没有按标的、数量、时间接近度猜归属，没有放开预留规则，没有重复 POST。
3. 将已知 ReservationConflictError 转为既有 OrderProjectionConflictError：调用方刷新重评，保留正式收盘待处理事件，网络、数据库及其他系统异常仍暴露。正常路径已消除重复请求，这条处理只覆盖最终准入期间的竞争。
4. 提交配置只在首次 submit/prepare_and_execute 调用后冻结，不能在开始发单后更换仓储或准入。只读启动对账创建队列不冻结配置，取消/对账也不会偷换提交所有权。

## 回归

`tests/unit/live_rollout/test_grace_reservation_visibility.py` 使用真实 Book、真实 Coordinator 与真实 order-row 解码，不用人工填好的 allocations 绕过边界。修改前复现下一根收盘生成重复请求；修复后不重复发单，原 GTC 宽限到期仍产生撤单与市价兜底请求。队列预留竞争修改前抛致命异常，修复后返回 pending_live_context，仓储和 backend 各只接收一次实际提交。

另一个测试修改前复现先对账再配置抛错，修复后通过；首次发单后继续配置仍被拒绝。定向原有退出/协调器/账本 160 项通过。开启 CML_RUN_HUB_NETWORK_TESTS=1 的最终 unit/smoke：3247 passed、1 skipped、2 项现有 warning，41.46 秒；跳过项为未配置真实数据库的 smoke。Ruff 除源文件既有 E501 外通过，新测试完整 Ruff 通过，新增行没有超长问题，git diff --check 通过。没有修改风险额度、策略阈值或账户仓位，没有人工重发订单。

## 上线验收

`8fafa30f50ba6421dd6cec6023182b35f197378d` 已提交、推送并在 UTC 16:25:14 完成滚动部署；四账户服务和四策略均为该镜像。行情、研究和面板保持 7d4f6f04。account-2 曾 syncing，经既有前置检查重试后通过，没有绕过风控或准入。

UTC **16:25:28.503–16:35:28.740**，600.238 秒、595 主机样本，观察期间无部署或 profiler：

| 指标 | 本轮结果 |
|---|---:|
| CPU 加权平均 / P95 | 32.63% / 80.31% |
| CPU >=90% 累计 / 最长 | 25.19 秒 / 15.07 秒 |
| 可用内存开始 / 结束 / 最低 | 927.61 / 853.26 / 824.83 MiB |
| swap-in / swap-out | 68.25 / 15.59 MiB |
| 容器健康 / 重启 / OOM | 12 全健康 / 0 / 0 |
| command / external recovery gate | 四账户均为 0 / 0 |
| DB 锁等待 / 长 idle transaction | 0 / 0 |
| 窗口内新增拒单 | 0 |

四策略匿名内存变化 -2.99 到 +0.82 MiB；Postgres +39.63 MiB、dashboard +53.95 MiB，执行账户 +4.36 到 +7.13 MiB。十分钟不能证明长期无泄漏；swap 与短时 CPU 峰值仍存在，平均回落不能替代峰值验收。

实际订单证据：四账户 VELVET 的原 GTC 订单均自然成交 1477，最新仓位记录为 0；没有人工平仓。account-2 的 FLUID/CAP 仍为 ACKNOWLEDGED 等待目标价。primary 在 UTC 16:30:00.373 的正式收盘处理后新建 CAPUSDT GTC 退出，1253 数量、0.08027 限价，UTC 16:30:01.626 获得币安订单 `471219311` 确认。这证明收盘退出仍工作。四策略跨 16:30 无预留冲突、致命通道失败或重启，旧问题没有在该窗口复现。遥测 candle ingress 查询没有持久化结果，不将其冒充处理证据。

## 未通过的完整性能项

行情仍有 4 次 lag 告警：253、145、120、**2004 ms**。整机 UTC 16:34:01.955–16:34:17.027 连续约 15 秒 CPU >=90%；5 秒 cgroup 读数显示 primary/account-2 各达到约 52–54% 单核 CPU，行情与其余策略也集中运行。这个窗口没有重启，说明重启风暴已经消除，但剩余集中计算/换页造成的峰值仍需单独采样定位，不能宣称全系统性能问题解决。

account-4 在 UTC 16:25:54 首次大快照（1840 个位置记录）出现 31 条 `durable flat history requires a verified source-anchored scan` 归并诊断。源代码对应旧空仓历史向新流迁移需要核验锚点的拒绝；四账户随后 readiness.entry_enabled=true、恢复门控为零。未发现当前订单因此卡住，但这条历史迁移诊断仍保留，不能删除校验或把日志隐藏掉。

最终摘要、日志分类、前后只读 DB 查询及限制说明见 `grace-reservation-final-observation-20261003.json`。原始采样留在服务器 `/tmp/cml-resources-20261002T162528Z.json` 和本机 `/tmp/cml-grace-final-observation-raw.json`。前一轮失败窗口保留为独立证据。
