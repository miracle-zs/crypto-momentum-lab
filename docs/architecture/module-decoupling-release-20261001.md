# 模块解耦生产发布与验收（2026-10-01）

## 发布范围与结果

用户授权提交、推送并发布服务器验证。结构改动提交至 `bfd6fe4c`，推送分支 `codex/module-decoupling-20260930`；未合并 GitHub main。服务器 main checkout 经部署脚本快进到明确目标提交。

首次 `bfd6fe4c` 发布失败：新策略启动时，`PostgresPositionRepairUnitOfWork` 构造 reservation 仓储漏传必需的 `strategy_name`。部署脚本检测到重启循环并退出，未报告成功。该故障可在本地直接构造真实运行上下文复现；既有事务测试采用 `object.__new__` 绕过装配，因此未覆盖此构造链。

修复提交 `658a628b2c3ce902e703e8e79fcb327aacd5f66a` 将运行上下文的策略名传至持仓修复 UoW 和 reservation 仓储，未硬编码策略。新增两项真实装配回归，覆盖当前及另一策略名；修复前测试失败，修复后完整本地回归 **2926 项通过**（33.57 秒，一项既有弃用警告）。发布前部署脚本与清单 smoke **44 项通过**。

修复版使用既有 `update_server.sh --live --refresh-approvals` 发布，四账户审批绑定更新并保留原额度、操作员与风险配置，预检与租约续期通过。无 schema 迁移。最终脚本退出 0，耗时 585 秒，checkout/runtime/image 均为 `658a628b`，11 个应用服务健康；Postgres 亦健康。

## 独立复验

北京时间 2026-10-01 07:55:43、07:56:31 与 07:59:37 采样容器、Dashboard GET API、策略日志及数据库只读事务（5 秒语句超时，ROLLBACK）。日志窗口限每容器最近 3000 行，不代表完整历史。

- 07:56:31 所有 12 个常驻容器 healthy，修复版应用重启计数均为 0，OOMKilled 均 false。
- 新策略采样日志无 Traceback、ImportError、ModuleNotFoundError、队列溢出、重复自愈成功或消费/会话失败匹配；截至 07:59:37，每账户有五条 source-anchored fill scan 不完整的匹配记录，问题仍在持续。
- API 存活 UP/UP，流 readiness READY；整体 readiness 仍 DEGRADED/EXIT_ONLY，原因 strategy_state_unconfirmed，事实完整性 UNKNOWN。07:56:31 账户 3 API 瞬时 syncing，07:59:37 四账户 API 均 READY，数据库最近对账 head 均 ready。
- 数据库四账户最近对账均 ready、挂单 0、不匹配 0；账户 4 有 1 个持仓，其余 0。此处为本地账户对账读模型，不是逐笔交易所独立核验。
- primary、账户 2、账户 3 checkpoint 由较旧或无游标记录更新为当前流游标，07:56:31 年龄分别 13.35、9.55、7.15 秒；账户 4 为 86.09 秒、游标 null，尚未证明持续前进。
- 07:59:37 复验四账户 saved_at 均较前次严格前进，并都有当前流游标；primary、账户 2、账户 3、账户 4 游标 sequence 分别为 49、50、51、48，checkpoint 年龄分别为 37.12、22.58、7.59、52.58 秒，均小于 180 秒。账户 4 的早期无游标状态已推进。此次几分钟观察不能证明长期稳定。
- 全局 durable 退出统计 PENDING=89、SUPERSEDED=2351、DISPATCHED=1；未做逐条处置或推断每条归属。
- 部署脚本四策略结构化 readiness 表示 entry_enabled=false，原因为 scheduled_risk_window，warmup=29/30、deferred=1，行情年龄 1.9–51 秒。该入场限制来自策略自身计划风险窗口，不能用 Dashboard EXIT_ONLY 独立证明交易路径状态。

## 剩余问题

发布流程与启动装配修复已验证，但事实恢复、待处理退出、策略状态发布尚未闭环；四账户 checkpoint 已通过本次两次前进采样，长期稳定性仍需观察。结构解耦不能作为原运行异常已修复的证据。本次没有手工发起测试交易、删除退出记录、改写持仓/成交事实或放宽恢复保护。
