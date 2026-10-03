# 2026-10-03 运行版本问题审计与新版本对照

## 结论

代码已提交并推送：`af7ba86a0aec82de070f251a51e7f8743deb8a9f`，分支 `codex/performance-optimization-2026-10-03`。服务器仍运行 `040b4eaacf932901d53642669019cd1ed63506b0`，本次仅只读审计，没有执行部署或修改业务记录。

**新版本完成了架构解耦，也包含最终 POST 围栏和敞口声明处理改动；但没有针对本次观察到的持仓同步等待、退出候选过期、命令终态不一致和行情延迟进行修复。另外，新版本启动异常清理存在已本地复现的回归。不能据此认定线上问题已全部解决。**

截至最终采样北京时间 **17:29:02**（09:29:02 UTC），12 个容器均 healthy，重启数均为 0，无 OOM；四个策略实际 readiness 均为 FULLY_TRADEABLE、entry_enabled=true，行情年龄约 0.77 秒。此前发生的 EXIT_ONLY 已恢复，但最终绿色状态不证明所有历史订单或退出状态都已收敛。

## 范围和证据

- 服务目录 `/opt/crypto-momentum-lab`，服务器 checkout 为 main、工作树干净，11 个应用镜像和配置中的代码身份均为 `040b4eaa`。
- 基础应用本次启动于 03:57 UTC 左右；四个账户服务分别于 04:00/04:01 UTC 启动；四个策略于 04:02/04:04 UTC 启动。北京时间对应 11:57、12:00/12:01、12:02/12:04。
- 逐行扫描本次容器保留日志，主统计窗口为 **03:57:07–09:22:06 UTC**；各应用日志都保留了本次启动开头，无该窗口内轮转缺失。PostgreSQL 日志按同一时间窗口过滤。数据库窗口覆盖滚动更新全过程，最初几分钟的部分遥测来自尚未替换的旧进程，不能全部归为新容器启动之后。
- PostgreSQL 查询使用 `BEGIN READ ONLY`、8–10 秒 statement_timeout，以运行期事件、账户状态、订单、命令、对账、敞口声明和 readiness 交叉验证。订单统计按窗口内创建筛选；当前订单终态是查询时的状态，并非严格的窗口结束快照。
- 除日志主窗口外，17:24、17:25 和 17:29 北京时间分别核对实际 readiness；检查点推进到 17:26–17:26:45。没有触发 Binance 下单、撤单、主动修复或重新对账。
- 汇总证据：[JSON](../diagnostics/server-runtime-audit-20261003.json)。没有保存密码、API 凭证或完整环境变量。

## 事件及新版本是否解决

| 事件 | 本次证据与后续状态 | `af7ba86a` 对照 |
| --- | --- | --- |
| 持仓同步导致退出反复延后 | 四策略 `live_exit_evaluation_deferred` 共 **598 次**：primary 53、账户2 255、账户3 164、账户4 126；另有收盘 K 线同步等待 162 次。主要涉及 SAND、MERL、IMX、龙虾等。05:05–09:18 UTC 有实际延后，期间多次 EXIT_ONLY。最终 readiness 恢复 | **没有针对性修复**。`exit_channels.py`、`exit_processor.py`、`entry_control.py` 原样；风控门面统一不能解决事实发布和退出等待问题 |
| 账户反复进入 syncing / fills_catching_up | 排除启动前段后，账户2/3/4/primary 分别识别出 46/50/50/49 次 syncing 段；已结束段平均约 60/60/52/60 秒，最长均约 60 秒。不是把每条重复状态记录都当一次切换。无需据此推断每个 syncing 段都禁止退出或全局开仓 | **没有针对性修复**。账户同步模块未变；这些周期性等待仍需核对触发原因与影响 |
| 账户4 IMX 修复拒绝 | 07:15:07 UTC 出现 5 次 `position_repair_blocked`，原因 `repair projection does not match actual account exposure`，伴随 3 次退出 Lane 可恢复失败。该账户 IMX 退出订单此前于 07:15:02 已 FILLED；因此不能将这些拒绝描述为未能平掉 IMX 或永久失去持仓 | **没有针对性修复**。`position_self_healing.py` 未变；保护性拒绝仍有效，处理短暂投影错位的效率未改善 |
| 账户2 IMX 退出候选过期 | 候选来自 07:15 收盘，07:16:00.385 过期，07:18:44.841 执行时拒绝，晚于有效期约 **164.5 秒**。随后 07:30 创建另一笔退出单，08:53:58 FILLED；不能将后一笔成交认作过期候选成功执行 | **没有针对性修复**。Submission 的前置候选到期判定保留；纯函数化规划器没有改变退出等待/重新评估机制 |
| 旧持仓投影下的退出分配被拒绝 | 账户2 / MARSCOIN 在 08:00:06 的命令被 REJECTED，`last_error` 明确为 stale position projection，attempt_count=0。后续另一个 MARSCOIN 退出订单被接受 | **保护行为已在旧版存在**。新版本没有针对性修复投影刷新时序；拒绝旧分配不能当成发单故障或风控绕过 |
| CLO 订单和命令终态不一致 | 账户2 / CLO 订单 `cml_b583a2924e8b2b399f601114421d4b49` 于 08:45:31 已 CANCELED；09:26 查询对应 `execution_commands.status` 仍为 ACKNOWLEDGED，`last_error` 为空。Book head 未标记 recovery_command_ids。**已证实表间不一致**；是否进一步阻塞下一次命令需要独立复现，不能仅凭此断言资金泄漏 | **没有针对性修复**。`command_repository.py` 未变；新的敞口 claim 逻辑没有统一 Book/outbox 命令终态 |
| 两笔退出结果暂不确定 | 08:00:05，账户2/4 的龙虾退出结果待对账。对应订单分别于 08:00:09.939、08:00:12.368 FILLED | **旧版已有恢复成功证据**。没有持续 UNKNOWN；新版本不能把这段已恢复事件记作自己修复 |
| 8 笔退出订单持续 ACK | 四账户各有 MERL、SAND 两笔，08:15 创建。账户开放订单快照均为 NEW；类型为 LIMIT/GTC，expires_at 为空 | **当前证据符合真实未成交挂单**，不能认定 ACK 卡死。新版本没有改变该挂单策略 |
| 行情事件循环延迟 | **167 次** `market_data_event_loop_lag`，最大 **1554 ms**，其中 3 次超过 1000 ms；另有一次 save_manifest 慢调用 565 ms。最后健康快照：重连、ACK mismatch、丢弃队列和 aggTrade gap 计数均为 0 | **没有针对性修复**。`market_data/observability.py` 等相关代码未变；暂未证明延迟根因，不以主机内存或 swap 采样直接作因果归因 |
| 研究采集历史桶内容冲突 | **738 次** `research_collector_state_conflict_kept_existing`，发生于 03:58–04:25 UTC，incoming/kept 都是 hub；保留原先归档内容，服务保持 healthy，没有观察到后续持续冲突 | **旧版冲突处理在工作，未新增修复**。`research_collector/storage.py` 未变；日志本身不能证明归档哪一版内容更准确 |
| 账户配置更新触发恢复和 WS 重连 | 共 **7 次** ERROR 级 recovery_requested，全部 reason=account_config_update，队列大小均为 0；对应 primary/账户2/3/4 为 3/1/1/2 次，集中在启动预热前段，之后已有正常账户快照和成交消费 | **已自行恢复，非进程崩溃**。账户配置事件策略未变；新版本 POST 前围栏没有消除配置变更触发的恢复 |
| 缺少 shadow preflight 记录 | 四策略启动各一次，共 **4 次** `live_shadow_preflight_missing`，不阻止此次运行；是否满足所需人工操作流程需另核对审批记录 | **未修改** `shadow_preflight.py`，不能称记录缺失已解决 |
| 启动和滚动更新短暂断流 | 持久化事件中四账户各出现一次连接拒绝和一次 Hub stream reset；研究 collector 已补回 71 行启动缺口。行情/报价/风险流连接后恢复。另有启动执行 head view/facts 迁移告警 | **已有恢复路径生效**，属于启动事件；不能按 WARN 数量认定运行故障。新版本没有为这些事件增加专门修复 |

PostgreSQL 窗口内唯一捕获的 ERROR 是 03:57:20 的交互式 psql 查询使用不存在的 `order_id` 列；不是应用进程 SQL 调用。Dashboard 日志未检出 HTTP 5xx 或 traceback。四策略检查点保存的 pool_acquire 最大 521.815 ms，数据库保存 total_ms 最大 756.693 ms；端到端检查点 duration_ms 最大 2686.779 ms，未发现该指标超过 5 秒。没有从一次静态资源采样推断性能根因。

## 新版本真正覆盖的改动

相对于服务器 `040b4eaa`，`af7ba86a` 同时包含上一批 `73f9b135` 的变更：

1. 将最终发单围栏移动至命令限速等待之后、实际订单 POST 之前；Hub 注册和必要配置准备先执行。相关本地回归存在，但本次审计没有发现能单独证明旧版发生该窗口越权提交的生产事件，也没有新版线上验证。
2. FILLED/部分成交终态保留必要敞口 claim，并根据账户基线覆盖条件交接。真实 PostgreSQL 本地测试通过；这修的是潜在敞口漏算窗口，**不是**持仓同步 pending、Book 投影刷新或命令终态不一致的直接修复。既有生产交接验收要求仍见修改计划，不据局部回归宣布所有覆盖语义已验收。
3. 抽取执行契约、候选公共工具、统一候选风控、纯函数规划、内联构造及报告类型消环。订单规划和候选准备的处理主体通过 AST 对照保持一致，这些结构调整本身不改变上述业务时序。

本地既有回归结果为 3313 passed、1 个公网 live 测试被排除；此结果不能替代新版本启动失败路径及真实业务验收。

## 新版本额外发现的启动回归

### 装配失败覆盖原异常并漏掉清理（本地已复现）

位置：`live_rollout/runtime_orchestrator.py` 当前约 1021、1725 行。

- 旧版在进入启动装配前初始化 `hub_source: ... | None = None`。
- 新版仅在行情装配成功后赋值 `hub_source = startup_market_assembly.hub_source`；失败清理仍读取它。
- 本地复用现有 RuntimeConfig 测试配置，patch `assemble_live_persistence`：先向真实 ResourceOwnershipRegistry 注册 mock 资源，再抛出自定义装配异常。所有数据库及网络装配均被截断，没有访问生产服务。
- 实际运行 `run_live_daemon`，输出如下：

```text
propagated_error: UnboundLocalError
message: cannot access local variable 'hub_source' where it is not associated with a value
original_error: InjectedAssemblyFailure
registered_resource_cleanup_calls: 0
```

这说明新版清理异常覆盖了装配异常，并跳过了注册资源的 teardown。这是新版本本地复现的缺陷，不是服务器当前版本日志中已发生的事件。本次任务未修改该代码。

### 运行期错误仍被按启动阶段分类（源码确认）

旧版在进入运行监督前执行 `startup_phase = False`；新版只保留 `startup_phase = True`，未再清除。因此进入 `session.run()` 后的可重试类型异常仍可能走启动重试分类。删除事实已通过 AST 对比确认；本次没有注入完整运行监督故障，不能把后果标成已复现生产故障。

## 后续优先级

1. **上线新版前先修启动清理回归和阶段分类**，对真实装配失败与资源所有权转移建立回归，保留原始异常。
2. 独立复现 CLO 订单取消后命令仍 ACK 的传播链，统一订单终态、Book 和 outbox 更新，验证不会让恢复扫描或后续调度长期处理已结束命令。
3. 对账户2 IMX 收盘事件、同步等待、候选有效期和下一轮退出评估做本地轨迹回放；以本次实际超期场景作为反馈，不放宽过期或旧投影保护来消除告警。
4. 查明反复约 60 秒 syncing 和退出延后的触发条件，再处理等待/事实发布；行情延迟另行测量，不能用架构清理代替性能诊断。

本次审计仅提供日志事实、查询结果、代码差异和本地故障注入证据；没有清理订单/持仓/claims，没有绕过风控、刷新审批、重启服务或部署新版本。
