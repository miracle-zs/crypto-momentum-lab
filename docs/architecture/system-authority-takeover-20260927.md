# 历史事实、统一权威链与原子提交接管

日期：2026-09-27。实现基线：`ec836c3`，包含上一轮已推送的 `0ffd779` 及之后四笔修复。三名实现子代理均为 GPT-6 Luna / max，主代理审核和集成。本轮按用户最新要求收敛：以主链路完整跑通为停止条件，随后提交 Git；不要求本轮完成所有架构验收。允许删除确认已被替代且无运行调用的旧模块及其专属测试，保留有效行为与新链路回归测试。最终状态以文末验收记录为准。

## 必须守住的契约

| 接管 | 权威与输入 | 成功条件 | 失败／恢复 |
| --- | --- | --- | --- |
| 历史事实恢复 | 按 environment/account/symbol/side/stream epoch 记录规范化 fills、snapshots、order boundaries、coverage 和版本化 checkpoint；PositionLedger 应用 checkpoint 后的真实后缀 | 重启前后非零批次身份、数量、成本与事实 cut 一致；历史 cut 不使用未来事实；coverage 证明缺口状态 | 未证明覆盖不得 READY；缺身份、冲突、历史断档明确拒绝或保持不可比；不得用零值或截断窗口补齐 |
| 统一权威事实链 | ExecutionBook 是决策和执行共同的 PositionView 来源；context 承载运营／风险姿态，不独立合成交易事实 | DecisionFrame 与 act 使用同一 scope、projection token、事实 cut；退出分配也来自该投影 | 新事实使旧 token 失效；恢复失败阻断启动；账户事件事实应用完成后方可决策 |
| 执行原子提交 | 同一 PostgreSQL transaction 保存 journal、dedupe／累计 quantity+quote 水位、reservation 与 outbox | SQL commit 成功之后才发布内存 candidate；同 command 幂等且冲突内容拒绝；并发版本锁／CAS | SQL 或 commit 失败没有半条持久状态、半扣预留或内存事实；交易所结果未知保留可恢复订单身份 |
| 决策原子提交 | 同一 transaction 保存完整 Trace、market refs、policy state、retention dependencies 和 accepted exit intent | await 耐久 receipt 后更新运行态并允许 entry／exit 副作用；policy prior CAS 防倒退 | 不吞错误、不以后台 task 完成替代耐久；重启从同一 commit 恢复 policy 和待派发 exit，幂等派发 |

耐久事务使用同步 commit。观测用的 best-effort Trace 写入不能充当发单授权。数据库事务不能覆盖交易所网络请求，已发送而未确认的订单由 durable outbox 与 reconciliation 恢复，不能重发。

## 文件责任与集成

| 负责人 | 独占实现范围 | 接口协作 |
| --- | --- | --- |
| history_takeover | AccountJournal、PositionLedger／PositionBook、checkpoint／recovery 模型、共享 session 的 journal store、事实迁移与测试 | store 不自行 commit；ExecutionBook/UoW 调用，runtime 传入真实账户事件 |
| atomic_takeover | ExecutionBook、execution/decision UoW、订单／预留／Trace／retention repositories、事务模型和迁移、测试 | 使用 history codec/store；返回真实耐久 receipt；提供统一 Book read 接口 |
| authority_takeover | LiveDecisionFactSource、runtime orchestrator、account channel、order coordinator、market loop 与必要 decision filter、测试 | 删除 context 合成权威 view 和平行恢复；await decision commit；接入真实 fills/cursor/epoch |
| 主代理 | 本文、复查报告、跨模块审核、隔离真实 PostgreSQL 验收 | 逐项检查生产 wiring、故障语义和被替代路径是否删除 |

## 验收要求

1. checkpoint 加 suffix fills 重启时 LONG/SHORT/BOTH 身份、活跃批次、数量、成本及边界一致；cut 早于 checkpoint 时不可泄漏未来状态；缺 coverage 和已知 gap 不假 READY。
2. 真实 PostgreSQL 事务在 journal、reservation、outbox、水位、decision state/Trace/dependency/exit 各阶段故障注入：全部提交或全部回滚；内存保持提交前状态。
3. 两个 writer 的旧版本写入被锁／CAS 拒绝；重复 evidence/command/decision 幂等，不吞内容冲突。
4. 决策提交失败不更新 policy，不发 entry/exit；提交后重启可恢复 accepted exit；未知交易所结果不重 POST。
5. 生产 runtime 只从 Book 恢复／读取，真实账户事实与 coverage 进入同一来源；不存在旧 context 事实 fallback 继续交易。
6. 新迁移只有一个 head，干净 PostgreSQL 升级通过；迁移回滚与旧数据不完整的诊断有证据。

审核补充的具体约束：真实 trade 事实与订单累计回报分开，累计回报只结算预留；REST/WS 顺序互换不得重复持仓或重复扣预留。交易身份和订单水位不得因 stream epoch 变化重置，epoch 接管需验证 checkpoint。投影摘要覆盖完整语义，零仓只由当前 cut 下的最新可信证据确认。预留容量只使用权威投影和批次数量。策略内容摘要之外使用独立提交序号；已提交决策重放不倒退策略状态、不重复派发。

交易所分页边界也必须进入证据：[Binance Account Trade List 官方文档](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade#account-trade-list) 当前规定，未提供 start/end 时默认最近 7 天，时间查询单个窗口不超过 7 天，fromId 不能与 start/end 同传，只支持过去 3 个月。由此，`fromId=0` 与空末页均不能单独证明账户从零起点的完整历史。非零恢复必须连接已证明的零仓／checkpoint 和连续后缀；范围外历史需真实归档，不能只靠净数量匹配快照推断完整。

## 实施与验收记录

当前：实现和集成验收中，尚未全部完成。隔离本地 PostgreSQL 16 已建；不会使用生产数据库进行故障注入。最终证据将在实现完成后补入。

阶段验证（不代表三类接管全部完成）：

- 基线单元测试：1836 passed、4 skipped、1 warning。
- 基线 PostgreSQL 集成测试：59 passed、1 failed；失败是退出批次读取将字典按二元返回值解包，修正后定点测试 1 passed。
- 主代理新增真实 PostgreSQL Trace 事务验证：2 passed。覆盖两个并发决策共享同一市场版本、同批次重复引用，以及内容冲突时整个事务回滚；不是 mock 数据库测试。
- 旧 Trace mock 单测目前 3 passed、2 failed，需适配插入后重读核验的新契约；不得通过删除耐久核验使测试通过。
- `0044`、冻结后的 `0045` 升级成功，迁移只有一个 head；`0045 → 0044 → 0045` 真实 PostgreSQL 回滚再升级成功。
- 主代理的跨模块真实 PostgreSQL 测试现共 8 项通过：Trace／决策 4 项、历史 2 项、完整 Book 2 项。补充覆盖策略状态不变时旧 revision 并发拒绝、决策重试不递增、Trace 写后回滚、二代 checkpoint 恢复、caller journal 回滚、Book 提交前故障不发布候选、重启非零持仓／token／evidence 幂等保留。
- 主代理 checkpoint／读取独立契约 5 项通过；历史单元阶段 8 项通过（包含 3 项 checkpoint 契约）。
- 集成中全量单元快照曾为 1768 passed、79 failed、4 skipped、26 warnings。失败已分配，包含真实水位兼容回归及旧接口／fixture 迁移；这不是最终验收结果，不能据部分数据库测试宣称完成。

### 2026-09-28 收敛验证

- 新隔离空数据库从第一项迁移升级至 `0045` 成功；`0045 → 0043 → 0045` 回滚再升级成功。
- 主代理跨模块 PostgreSQL 核心复验最新 **11 passed**；测试账户使用独立 UUID，重复运行不依赖清空数据库。
- 子代理历史恢复交接：execution 单元 **98 passed**，额外决策／simulation **14 passed**，历史 PostgreSQL **5 passed**；最终集成仍须主代理复验。
- 非零跨 epoch 恢复使用版本化 checkpoint adoption，绑定旧 scope、checkpoint ID、事实摘要、投影摘要、cut 与连续后缀证据；不改写旧 checkpoint 身份。
- REST／WS 事实入口和旧路径清理正在集成，上述通过数尚不代表生产启动链已完整跑通。

## 最终收尾：按用户要求立即冻结（2026-09-28）

用户最后要求“现在就想收尾，最快收尾”。已中断全部子代理，停止修复与旧模块删除；本次为未完成集成的阶段提交，不是可部署版本，不宣称完整跑通。

| 检查 | 冻结时结果 |
| --- | --- |
| Python 编译 | src 与本轮迁移 compileall 通过 |
| git diff --check | 通过 |
| 全量 unit | 1797 passed、54 failed、4 skipped、14 warnings（15.61s） |
| execution unit | 98 passed |
| 核心真实 PostgreSQL | 11 passed（Trace、history、Book、provenance） |
| 新跨 epoch PG E2E | 2 项未通过；checkpoint 压缩后原零仓锚点验证仍待修，不能据域层通过宣称重启接管完成 |
| sync／Hub 定点 | 39 passed、1 failed、3 skipped；首次 new_fill_keys 契约未迁移 |
| Ruff | 78 项未清理（包括格式、import 等）；本轮未为清零扩大修复 |
| 新迁移 | 干净库0→0045、0045→0043→0045通过 |

### 后续从这里继续

1. 完成 REST 分页证据生成、WS 真实成交转换、Hub→Book 实际接线与启动 READY 回归；plan_runner 耐久入口未完成。
2. 修复压缩 checkpoint 的 coverage anchor 验证，跑通非零跨 epoch PG 重启用例。
3. 迁移 LiveDecisionFactSource 的 async 测试与 coordinator 的纯内存／耐久模式契约；清理真正无调用的旧持仓 classifier 与其专属测试。当前未执行该删除，避免匆忙按失败名单删测试。
4. 策略 exit/pending exit 运行链和 legacy policy import 完整重放接入需要最终审核；Ruff 和全量测试需通过后才可部署。

没有服务器部署、实盘发单或生产数据库迁移。本地隔离测试数据库用于上述验证。
