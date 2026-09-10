# 代码审查维修计划与进度

更新时间：2026-09-10

本计划以当前工作区的代码事实为准。P2、架构项和部署项的逐项验收分别见：

- [P2 与架构验收](./code-review-2026-03-19-p2-architecture-assessment.md)
- [部署项验收](./code-review-2026-03-19-deployment-assessment.md)

## 第一批：已完成

目标是先切断交易所与本地状态分叉、行情捕获失控和部署监控盲区。

- P0 #1–#3：下单网络未知结果、撤单未证实失败、GTD 预检分别进入 unknown 或明确的 pre-submission rejection 语义。
- P1 #4–#7：同 key 调度器创建与关闭竞态、终态回退、成交去重缓存无界增长。
- P1 #8–#9、#11–#12：官方 candle 源退避、成对 daemon 的 EMA context、paper gap reset、paper-live latency 默认值。
- P1 #14–#19：WebSocket 实时旁路有界异步化、durable 背压超时、磁盘保护接线、manifest journal 回放、aggTrade 恢复游标、archive 坏 writer 隔离。
- P1 #21–#23：portfolio 批量查询与 commit 后缓存、connection pool 关闭锁、quality event 批量写入。
- P2 #26、#27、#29、#33、#34：grace 平仓遵守最短持仓时间；EMA 入场过滤按 LONG/SHORT 选择 ask/bid；LIMIT 价格按最终交易所 BUY/SELL 方向向外量化；删除不可达 URL 校验；重复 client order 冲突会回滚整笔 prepare 事务。
- P2 #24（第一阶段）：本地 15m 聚合器对不完整窗口、缺分钟和跳过窗口生成有界 gap 事件；paper daemon 输出告警，仍拒绝合成残缺 K 线。权威官方 candle source 的历史补取继续作为部署配置项。
- P2 #25（第一阶段）：paper position 持久化 `last_candle_end` 游标，重启后按游标顺序补取并逐根处理官方闭合 candle；多根确认历史不再只依赖当前最近一根。回补仍是 paper 模型事件，使用官方 candle 的结束时间和收盘价，不宣称为宕机期间真实交易所成交。
- P2 #28（第一阶段）：陈旧 paper market state 现在按 symbol 只告警一次，并在恢复新鲜 state 时记录恢复事件；陈旧期间明确跳过策略和持仓标记，退出延后到新鲜行情，避免用陈旧价格模拟成交。PostgreSQL source 的 idle timeout 也会记录结构化 error 并退出，交由 Compose `restart: unless-stopped` 拉起新实例；恢复后的退出仍以新鲜 state/candle 为准。
- P2 #32（第一阶段）：REST 发现但 WS 尚未出现的 fill key 保留为待核对状态；重连请求成功只记录请求时间，按 60 秒节流重试，直到 WS 指标真正看到该 key，避免把“请求返回”误判为恢复或形成重连风暴。
- D1–D3、D5、D9：按需选择 live compose、修正 strategies 路径、多账户 ops monitor、live position label 预检、补齐 gainer10 的部署断言。

验收方式：相关 unit/smoke/e2e 测试、strategy runner 与 PostgreSQL persistence unit（146 passed）、execution-account watchdog unit（4 passed）、PostgreSQL `tests/integration`（50 passed）、ruff、compileall 和 `git diff --check` 已通过；迁移 `20260910_0032` 已在同一 PostgreSQL 容器上用本地 `.venv` 的 Alembic 升级到 head。Docker Compose 的 `migrate` 一次性任务仍受 Docker Hub BuildKit frontend 拉取 EOF 影响；一个既有的 market-data cleanup 测试会挂起，已单独隔离。未连接真实服务器，也未执行容器重启。

## 第二批：下一步实施

1. **策略一致性**：按账户提交 cooldown；为批处理 paper 增加 position/exit 结果，建立与 daemon 的边界对照测试。
2. **数据库资源隔离**：为 runtime state、quality/manifest、maintenance 查询定义并发预算，先以观测数据确认连接池大小，再拆 maintenance pool。
3. **P2 正确性收敛**：继续处理 24–34，优先剩余边界；#24 的本地缺口可观测性、#25 的游标化官方 candle 顺序回补、#28 的陈旧状态告警与 source idle watchdog、#32 的 fill 待核对与重连节流、#26/#27/#29/#33/#34 已在第一批收敛。
4. **架构收敛**：先抽取无业务语义差异的序列化/解析 helper；A2、A4、A6、A7 在补齐 round-trip 和调用方测试后再重构，暂不做大规模 compose 或入口重写。
5. **部署条件项**：确认真实 nginx 上层鉴权和数据库备份/恢复演练证据；若证据不足，再分别补 health probe 兼容的应用鉴权和可演练的备份作业。

每一项都需要对应的回归测试、运行指标或部署隔离复现；不以“字符串存在”作为验收标准。
