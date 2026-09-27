# 服务器故障核查（2026-09-27）

核查时间：北京时间 13:18–13:21；服务器 43.167.191.253。只读 SSH、Docker 状态/日志和 PostgreSQL SELECT；没有重启、改库、下单或部署。SSH 已退出。

## 主要故障：四个策略容器循环重启，持仓退出被阻断

运行镜像：`9256a76c013dc4a5381b86c9839cd6f45b083704`；宿主 checkout 是 `663eeaf`，两者不同，诊断运行行为以镜像为准。

13:18 初次采样时 primary/account-2/account-4 RestartCount=3，account-3=4；13:20:56 时四者均为 5，证明并非一次正常部署重启。容器短暂 healthy 不能代表策略正常。

四个账户均存在 AIOUSDT LONG 持仓。最新持仓快照为 13:20:32–13:20:40，每账户 quantity=2375，mark price约0.04148–0.04152，浮动盈亏约 -1.35～-1.44 USDT。此处是账户服务落库快照，不是本次直接访问交易所的查仓结果。

四笔 SELL/reduce_only 退出命令创建于 13:15:02，状态均为 rejected、executed_quantity=0：

| 账户 | client_order_id |
| --- | --- |
| primary | cml_d7c4d5cef48adcdf4f2bf955554c8996 |
| account-2 | cml_2cefe940f6083db7dbf08faead69733e |
| account-3 | cml_c8870cf1ae56c1645608d57de2911b1c |
| account-4 | cml_0c4959f683c01285d659c1b3f5970fe6 |

拒绝事件 details 四者相同：`phase=before_exchange_submit`，`reason=capability_evaluator_blocked: unresolved_inflight_orders_present`。因此这是系统自身能力门禁在交易所提交前拒绝，不能称为 Binance 拒单。

对应预留 reserved=2375、consumed=0、released=2375、status=RELEASED；释放原因 `order_finished_residual_release_rejected`。后续 grace-timeout 退出复用同一命令/预留 ID，创建预留失败：`already exists in terminal status RELEASED`。

primary 日志展示完整链：13:15:03、13:16:31、13:17:47、13:18:57 预留冲突 → `live_runtime_critical_task_failed`（grace timeout exit channel）→ `session_run_failed` → `live_runtime_failed` → Docker 自动重启。其他账户同类错误。

可确认根因链：能力门禁先拒绝退出；终态预留与重试身份协议不兼容；异常向上逃逸导致退出关键任务失败并循环重启。最初的 unresolved_count 为何非零尚未完全确认。镜像代码的 evidence provider 按 symbol 统计 context.unresolved_orders，没有排除当前命令，也未在该处刷新数据库；存在把当前/过期上下文订单计入的可能。13:21 核查数据库时 AIOUSDT 已无非终态订单，不能用此时结果反推13:15不存在未决订单。

建议优先处理：核对最初门禁证据和订单身份；制定明确的“已拒绝命令原回执 + 新退出尝试”协议，避免原终态ID再次建预留；将可预期提交拒绝收敛为业务结果，不能杀死持仓退出监督任务。恢复后应验证四账户实际持仓及退出通道。不能通过盲删 RELEASED 预留或仅反复重启解决。

## 次要异常

- 策略日志出现 execution pool size=2/overflow=0/timeout=2s 和 observability pool size=1/overflow=0/timeout=1s 超时。
- 出现 SQLAlchemy 未归还连接的垃圾回收警告；数据库采样为 idle in transaction=4、idle=42、active=1。连接泄漏/事务退出异常需继续定位；这些是确认存在的症状，尚不能认定是首次退出失败的原因。
- Trace 保存失败，意味着该时间段审计记录可能缺失。
- primary 13:19:20 停机报告 `closed_candle_feed:timeout`，虽 checkpoint_durable=True，仍有 close_error。
- `cml-archive-trim.service` 处于 failed，错误发生在9月25日08:24，`_scalar(...).splitlines()[0]` 对空结果抛 IndexError；timer仍 disabled。属于历史清理故障，不是本次循环重启原因。

## 基础设施状态

行情、research collector、四账户执行服务、dashboard、PostgreSQL采样时在运行且健康，账户服务无容器重启。策略容器无 OOM 标志。内存3.6Gi，总可用约1.3Gi；swap使用314Mi/1.9Gi；磁盘57%，剩余25Gi。负载4.87/2.91/1.89偏高，但没有磁盘耗尽或 OOM 证据。

结论：P1运行故障，四账户策略循环重启，AIOUSDT 已有持仓的自动退出尝试未完成；基础数据库和行情仍存活。需要修复退出门禁/重试协议，而非把容器健康状态视为恢复。
