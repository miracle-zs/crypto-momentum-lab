# 业务异常恢复：事实、状态与退出回执（2026-10-01）

## 第一性原理

持仓数量必须由可信零仓或持仓 checkpoint 加其后的真实有向成交推导，并与交易所明确快照一致。游标前进、REST 成功、数量相同、容器健康都不能独立证明成交历史完整。退出指令的终态必须依据该指令的回执；当前零仓既可能是已成交，也可能是从未提交，不能混为 SUPERSEDED。运行状态必须由真实生命周期事件发布，不能由读端默认 active。

## 已复现的根因与修复

1. 策略状态表没有运行时写入，四账户 Dashboard 为 strategy_state_unconfirmed。操作状态转换与账户/策略状态投影现在同事务落库，忽略临时预检，防止较旧事件覆盖新状态。
2. 成交扫描证明没有装配到生产账户同步，也没有经 Hub 传入 Book。新增当前 head 绑定的可信 checkpoint/实际零仓基线加载器，透传完整扫描、基线和成交；在 Book 事务内建立事实证明，跨 epoch 使用明确 checkpoint adoption。后续轮询重新加载新 checkpoint，查询限定绑定 ID，避免历史全表扫描。仅数量相同的非零仓不再允许无证明 rollover。
3. 待退出恢复曾仅以本地 Book 零仓标 SUPERSEDED。新增只读回执核验：明确新鲜零仓、无相关挂单、无 ACTIVE reservation、精确 client order ID 查询、指令时间界限与本地订单身份；确认 FILLED 必须有同账户/订单完整成交数量，标 DISPATCHED；确认近期不存在且本地无订单才标 SUPERSEDED；其余保留 PENDING。恢复使用短时账户快照复用和错误退避，运维工具默认 dry run 并共享私有 API 限速。
4. 事实健康读端原先固定 UNKNOWN。现在读取当前 head 绑定的 checkpoint 全部摘要、scope、完整覆盖、健康与时间切面，核验非零仓数量与账户 head 一致，证据缺失/过期保持 UNKNOWN。展示使用证明本身的 cut，不用心跳时间替代。

5. 发布前复查发现旧账户 4 XVS 持仓 CONFLICT 导致 13 次重启：`ExecutionReadinessError` 经退出提交保护包装后未被退出通道识别。新增回归在修复前真实抛出该异常；修复仅按异常 cause 类型把保护拒绝转为 position_not_ready，恢复支路保留原回执、退避且不消耗未发生的 POST 次数，其他数据错误继续上报。

6. 首次业务修复部署在旧策略停止确认阶段失败。主机监控的维护窗口只过滤告警，检查中已执行的自愈 Compose restart 仍可能与部署冲突；新增真实监控 runner 回归确认维护期间发生重启，修复在重启动作前检查窗口。停机窗口暂停自愈用于本次恢复，随后恢复新监控。

7. 第二轮验收发现新增零仓 loader 的来源标记被误传为历史 stream，真实 `AccountFillLoadScan` 因 zero snapshot belongs to target stream 校验而失败。新增 loader→plan→transport 构造回归在修复前失败；修复使零仓基线只携带原始快照/身份摘要，历史 stream 仅用于 checkpoint adoption。先通过只读账户恢复部署，再执行策略发布。

8. 第三轮真实策略验收发现 Hub 丢弃零仓基线 `raw_payload`，而稳定身份摘要包含原始载荷，导致 typed source provenance 校验失败。使用非空实际形状载荷的 Hub→Coordinator 回归先失败；修复只对证明基线透传原始载荷，普通快照仍保持原传输大小。真实 PostgreSQL 回归加入 loader→plan→Hub→Coordinator→Book 全路径，4 项通过（1.90 秒）。

9. 追查历史零仓证明缺口发现持久化稀疏策略省略不变零仓，同步仅为当前非零仓复用上一轮 REST 零仓，已跟踪且已平仓品种无法生成完整扫描证明。新增两次真实零仓切面的生产者回归先得到 0 条 scan；修复对已跟踪品种使用上一轮明确零仓作为基线，完整核验两次真实 cut 之间的成交，不把游标或当前零仓视为历史完整。未返回的品种不补零。

10. 上一项真实部署又暴露扫描范围扩大：历史跟踪品种为 primary 358、账户 2 为 193、账户 3 为 177、账户 4 为 180，全部转为每轮立即零仓扫描导致启动等待。修复让 loader 明确返回当前 Book 的准确恢复键；无可信 anchor 的键用 `None` 表示证据缺失，由上一轮真实 REST 零仓补足。只立即恢复当前 head 与真实非零仓，其余历史品种保留原有增量批次。含额外历史品种的回归在旧同步代码上失败，限定范围后通过；4 项真实 PostgreSQL 恢复回归再次通过（6.99 秒）。

11. 继续实盘验收复现无成交区间的合法证明被拒绝：旧账本仅允许零仓 anchor 与 checkpoint cut 相同，且误把扫描 load ID 必须等同源 snapshot ID。修复要求源切面至目标切面的完整可信覆盖、精确基线摘要和目标明确零仓。后续 checkpoint 统一使用经过验证的 parent 加严格 event-time suffix，既支持无成交区间，也防止将完整旧前缀当作新 suffix 重放。新增真实 PostgreSQL 空 suffix/完整或截断组合，6 项恢复回归通过（2.33 秒）。

12. 退出保护的原始 `ExecutionReadinessError` 在 Book 转成通用 `Blocked` 返回值时丢失 exception cause。新增真实 Book→Coordinator→ExitProcessor 的回归复现进程异常；Book 返回 `PositionNotReady` 子类型，Coordinator 恢复类型 cause，退出处理仍按类型延期并保留原回执，没有预留或 POST。未知数据异常仍传播。

13. `seen_trade_count` 是 Book 的全局诊断计数，不能当作某个 head 存在成交的证据。新增隔离数据库回归用无成交 ETH head 的旧全局计数 999 复现多余恢复范围。loader 和事实健康读端改为同一键的不可变成交身份、真实非零快照日志或 head 绑定 checkpoint，使用有索引的关联 EXISTS，不重写旧生产计数或历史事实。

14. 生产最终验收仍发现账户 4 XVS 的 26.6 非零仓无法跨 epoch：快照表没有开仓前零仓，但不可变事实日志保存了 2026-09-28 03:32:52.722 UTC 的明确零仓。loader 增加日志来源，核验原始载荷哈希、准确账户/品种/方向和观察时间，仅作为真实成交扫描的基线；仍要求完整扫描与当前非零仓一致后才创建 checkpoint。隔离 PostgreSQL 回归先复现 anchor 缺失，修复后 12 项通过（5.79 秒），覆盖非零仓恢复、截断拒绝、日志哈希损坏和错账户拒绝。

15. Hub 的全局成交去重混淆了实时指标与恢复事实：重复完整扫描仍携带正确 provenance，但此前见过的成交被删掉，非零仓重建变成空成交输入。真实服务器只读订阅复现 XVS 有 20 条扫描但成交为 0；Publisher 回归先失败，修复保留扫描携带的完整成交集合，实时通知仍保持原去重，账本按不可变成交身份处理幂等。将实际 Publisher→编码→解码补入 PostgreSQL 的非零仓恢复回归：12 项通过（34.75 秒），Hub 网络与相关 Coordinator 回归 31 项通过，Hub/loader mypy 通过。

16. 完整扫描终于携带 XVS 的 4 笔真实成交后，服务器明确拒绝 trade 127206945：旧不可变身份摘要包含 WebSocket 原始载荷与 Decimal 显示精度，REST 重放同一成交时摘要不同。真实 PostgreSQL 回归先复现 durable identity conflict；新成交摘要只使用准确账户/品种/方向、成交与订单 ID、时间、数量、价格、损益及手续费，数值去除无意义尾零。旧身份不重写：仅当原始不可变日志载荷哈希、准确 scope/成交时间、旧摘要和新的完整业务摘要全部核验成功，才承认幂等重放。手续费变化与日志哈希损坏仍拒绝恢复。24 项隔离 PostgreSQL 回归通过（12.33 秒）；541 项账本/协调器/Hub 单元与网络回归通过（1.24 秒）。摘要模块 mypy 通过，UoW 与修改前的同文件基线均有相同 12 项既存类型错误，未宣称全项目类型检查通过。

## 发布前证据

- 原错误由运行状态发布、Hub 证明传输、真实 Book epoch 恢复、平仓 outbox 边界回归复现。
- 真实 PostgreSQL 使用独立测试数据库；覆盖事务状态投影、完整恢复/截断拒绝、重启、后续 checkpoint 延续、事实健康过期和精确成交回执。生产事实表未被测试改写。
- 服务器只读逐条预核验：89 条 PENDING 中 85 条满足未落单且退出已无必要；4 条有已成交回执与完整本地成交，分别 primary 订单 3767181461、账户 3 订单 1639704348/3767181235、账户 4 订单 947357539。后续处置必须重新核验，不能直接使用该预核验结果更新状态。
- 批量 GET 曾触发限流，停止该轮查询，工具接入共享限速并降低重复账户查询后重新核验成功。没有发送试验交易、取消交易、删除退出记录或降低事实保护。

- 本地单元、Hub 网络、fake-service e2e 与部署脚本回归 2957 项通过（33.31 秒）；部署脚本/服务清单 smoke 44 项通过；独立 PostgreSQL 12 项通过（6.56 秒）；6 个相关关键模块 mypy 通过；本轮加载器 mypy 单独通过，整个项目仍有既存类型错误。改动行 Ruff 无问题、git diff --check 通过；Book 原有长行不作为本次新增 lint 问题。

- 最后一轮完整本地回归 2958 项通过（33.35 秒），仅现有 Starlette/httpx 弃用告警。最终成交身份兼容恢复集 24 项真实 PostgreSQL 回归通过。

## 生产验收

代码版本 `7320b3f7a558f89be29a95d0872d57a00a857964` 已提交、推送并发布。基础服务发布成功（159.0 秒），四账户只读服务恢复发布成功（66.9 秒），批准刷新、风险租约续租与严格 preflight 全部通过后，四策略恢复成功（132.5 秒）。先正常停止策略再更新基础行情，避免行情 stream 切换中策略继续消费旧 epoch。主机监控已恢复 active，隔离测试容器及测试数据库已移除。

2026-10-01 11:14:25（北京时间）采样：11 个应用容器及 PostgreSQL 均 healthy，应用版本一致，无重启、无 OOM。四策略均 active，账户对账均 ready，实际持仓、挂单及 mismatch 均为 0。四账户共 80 个相关 head 全部绑定当前 scope 的 checkpoint，事实健康均 zero_fact_gaps；checkpoint 证明年龄约 31–80 秒，未证明的历史成交 head 为 0。

XVS 的 26.6 LONG 已由正常策略退出，准确订单 `2007256789` 的 2 笔真实成交合计 26.6；持仓 reservation 为 COMMITTED，consumed_quantity=26.6。订单回执早于完整成交事实曾触发恢复拒绝，随后由原有恢复通道核验成交回执标记 DISPATCHED；未提交的另一退出准确核验为 SUPERSEDED。后续完整扫描创建父子 checkpoint，已平仓数量与交易所一致，未手工强平或伪造零仓。

最初 89 条待退出记录已逐条重新核验：85 条 SUPERSEDED、4 条 DISPATCHED，原 PENDING=0；XVS 新产生的退出记录随后也自动完成，全库 PENDING=0。账户 3 当前 DISPATCHED=3（含一条原有终态）、SUPERSEDED=472，无 PENDING。

总 readiness 在周期 REST 对账的 syncing 阶段仍为 DEGRADED/EXIT_ONLY；同期行情/账户/策略/批准四项 stream readiness 均 READY，事实健康全部 healthy。应区分短暂同步门禁与原先策略不运行、证明缺失及退出积压，不能将总状态强改为绿色，也不能将 active 解释为任何时刻允许入场。计划风险窗口和同步期间的原有保护继续生效。

监控恢复后 11:14:25 与 11:15:56（北京时间）两次采样，间隔 91 秒。全部容器仍 healthy、0 restart、无 OOM；四账户均 active，原退出及新退出均无 PENDING，80/80 head 的 checkpoint 全部推进且年龄 20–84 秒。四账户运行 checkpoint 的行情 stream 均为 `18787efd-6f1b-4567-9534-061377c06ed5`，消费序号分别推进如下：

| 账户 | 相关 head / 绑定 checkpoint | 证明 cut 推进（UTC） | 行情游标推进 |
| --- | --- | --- | --- |
| primary | 20 / 20 | 03:13:21.910 → 03:14:36.646 | 59 → 64 |
| account-2 | 21 / 21 | 03:13:48.415 → 03:15:12.998 | 60 → 64 |
| account-3 | 19 / 19 | 03:13:06.952 → 03:15:40.502 | 57 → 65 |
| account-4 | 20 / 20 | 03:13:56.904 → 03:15:20.017 | 58 → 66 |

独立检查自监控恢复时刻起，四策略新日志的 Traceback、queue overflow、账本冲突、事务持久化异常、market/session failed 均为 0。账户 4 最近两分钟无 reconnect_requested，pending_fill_count=0；早期历史 REST 成交补扫告警已按既有超时机制收敛，没有持续自愈。

完整脱敏采样见 [生产验收证据](business-anomaly-recovery-evidence-20261001.json)。这证明本次窗口中运行状态、事实恢复、退出终态和消费链路已恢复；不把短时验收扩展为永久运行保证。
