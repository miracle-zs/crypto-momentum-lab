# 系统缺陷修复计划与验收记录（2026-09-27）

代码基线：ef95854d8ac544bba0a4605ff552cb5fad7681ed；对照9667e32复查报告。用户授权当前代码复核、修复与子代理委派；全部子代理为GPT-6 Luna / max，主代理审核。共享工作区按文件分工，不部署、不改服务器数据。

## 修复原则

先复现当前缺陷，再修改唯一职责出口；已修旧问题撤销“待修”状态。不得伪造coverage、跳过安全检查、吞持久化异常或以healthy代替业务验证。数据库原子性和实盘部署需独立证据，单测不替代。

## 分工与执行顺序

| 批次 | 负责人 | 范围 | 验收 |
| --- | --- | --- | --- |
| A：执行事实与耐久状态 | execution_fix，Luna max | F1/F5/F6、S1–S3；ExecutionBook与订单coordinator | 快照类型/账户/side与就绪；累计重复/乱序不丢不重；写失败不发单；关联恢复；定向测试 |
| B：决策身份与重放 | decision_fix，Luna max | F7–F9；完整sizing/hash、Live金额与trace replay | 非默认政策与前态；相同输入严格重现，篡改输出失败；数量与notional一致；定向测试 |
| C：前端状态更新 | frontend_fix，Luna max | F2–F4；真实动态更新、多root及同root滚动/高度 | 动态金额/持仓/订单正确、事件绑定有效、焦点/滚动稳定；Node与可用浏览器验证 |
| D：运行时集成 | 主代理 | account snapshot callback顺序与任务生命周期，跨模块验收 | 事实输入完成后再放行事件消费；错误进入恢复；无悬空任务；集成定向测试 |
| E：文档与审核 | 主代理 | 修正旧报告、逐项审补丁、全量unit与frontend检查 | 已修/残留/未验收分开；记录命令结果和限制 |

## 现阶段状态

- ef95854已有大量修复，原F1–F9不能按历史结论直接重复修。
- 当前基线全量unit：1801 passed、4 skipped、1 warning（14.87s），排除integration/live且移除数据库环境变量。
- 初审发现snapshot callback以无监督create_task运行，后续账户事件可先于事实应用且异常被吞；需要顺序消费与监督。
- F10本地replace导入已修，线上是否部署不在此次代码测试中认证。F11为timer运行状态；F12–F14及长期接管项目按证据复核，不因改几行配置就宣称完成架构接管。

本轮修复与审核已完成；最终结果见文末。

## 主代理已完成的集成修复（最终全量复验通过）

1. account_channel支持并等待异步快照回调，先应用ExecutionBook事实，再发布control-plane上下文；删除无owner后台任务及吞异常分支。失败触发已有恢复流程；成功/失败两个回归先红后绿。
2. Book恢复失败直接阻断启动。新增15秒预算的真实仓位引导；PrivateReadClient的include_flat=True明确使用V2实际返回的零仓行，默认V3接口不变，不从缺失符号造零。HTTP测试先因缺参数失败，修后通过；bootstrap顺序/空响应/读失败测试通过。
3. 删除LiveDecisionFactSource第二次不完整Trace构建，完整Trace唯一由engine recorder保存；旧回调覆盖行为先红后绿。
4. market-data账户标签改为可选；基础/多账户Compose配置均通过config --quiet（仅测试占位变量，无真实secret输出）。删除4处固定shadow确认，恢复缺证据warning。真正shadow硬门禁不是现有功能，需明确部署政策另行实现。

凭据隔离：ADR明确许可迁移期fallback，本轮不将未证实线上权限当功能bug强行移除。归档timer、镜像部署是现场状态，不通过本地修改冒充已恢复。

## 必须保持显式未完成的架构验收

- 非零持仓的AccountJournal历史成交/checkpoint与coverage耐久恢复；只能依据真实覆盖证据开放，不能用ready标志或合成零仓绕过。
- policy state、Trace、retention dependency及accepted exits的跨模块原子提交；任务跟踪不等于事务完成。
- 实际Postgres并发/kill恢复、exchange side effect、部署后重启与归档timer验收。

Binance接口依据：[官方Trade REST说明](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade)：V3省略无持仓/挂单的符号；V2支持实际零仓记录。实现只使用返回的记录，不利用省略推断历史coverage。

## 剩余接管的完整实现步骤

| 顺序 | 输入／实现边界 | 完成凭证与验收 |
| --- | --- | --- |
| 1 权威事实恢复 | AccountJournal增加版本化checkpoint；从已确认zero-crossing或checkpoint恢复规范化fills、snapshots、order boundaries；coverage携带账户/side/stream epoch及连续区间，接入同一ExecutionBook | 有持仓重启投影与重启前一致；缺任何区间保持不可比；reconnect缺口与乱序重放不会假READY |
| 2 耐久observe | journal、成交去重/累计水位、reservation结算、outbox状态用同一数据库事务；提交完成后发布内存投影。取消跨命令scope兜底 | 对每个await注入崩溃，重启后成交数量、预留余额与命令状态一致；重复回报无副作用 |
| 3 决策耐久提交 | 冻结input/prior/policy生成完整Trace；policy state、retention dependency、accepted exit command与Trace关联采用事务或有明确恢复协议的durable outbox | Trace保存失败不会冒充耐久决定；policy重启恢复不倒退；清理不能删除未注册依赖的输入 |
| 4 部署闭环 | DeploymentCoordinator维护部署身份、schema兼容、writer fencing、preflight证据/显式例外，并记录部署镜像commit | 单writer交接与回滚测试；shadow政策明确后再决定硬门禁；凭据角色权限由安全核验凭证证明 |
| 5 现场验收 | 对最终镜像进行canary，确认首单/部分成交/重启恢复、浏览器布局、归档调度与停机凭证 | 不能只凭healthy或unit通过。保留业务处理计数、coverage与shutdown failures证据 |

上述步骤仍属于后续架构接管；本轮局部行为修复不替代这些验收。

## 主审中间复验（不是最终验收）

运行时集成与Binance读取定向62项通过；前端Node44项通过。第一轮在途全量unit为17 failed、1805 passed、4 skipped：包含新增红测和旧契约测试。执行端未await状态迁移、已POST后的UNKNOWN保护、累计平均价差额、严格restore；决策金额canonical与唯一Trace入口；前端两项旧静态契约断言，均已交回对应子代理。真实Chrome另暴露无锚点滚动漂移及图表实例疑点，要求保留失败场景修复。以上失败必须在最终审核清零，不能引用中间通过数宣称整轮完成。

## 已交接补丁与主审结果

| 部分 | 代码结果 | 验证 | 未涵盖边界 |
| --- | --- | --- | --- |
| 决策／Luna max | 完整versioned policy/state；零剩余量；Decimal与负小数timedelta；完整intent/exit/next-state及重建digest比较；多market refs重放；签名适配不重复回调；Trace recorder错误传播 | 子代理决策/提交59 passed，Ruff通过；主审扩大决策/提交/前端静态90 passed | 历史代码内容和风险artifact未冻结；旧trace缺证据返回EVIDENCE_INSUFFICIENT；Live异步数据库写入原子提交另列后续 |
| 前端／Luna max | 同结构轮询更新详情/图表；keyed DOM保留行和disclosure；保留ECharts实例运行时属性并setOption；同root/双root高度释放；无anchor补偿；新用户focus优先；失败保留last-good | Node44、静态31、Chrome7场景通过；主审独立内置浏览器同7场景通过 | 合成数据本地验证；尚未部署线上。harness与结果见tests/frontend/live-account-refresh.browser.* |

浏览器复验：仓库根目录执行 `rtk proxy python3 -m http.server 18765 --bind 127.0.0.1`，访问 `http://127.0.0.1:18765/tests/frontend/live-account-refresh.browser.html`。只提供本地静态文件，测试使用合成fetch数据，不调用实盘后端。

主审额外发现并交修：非累计trade_id在不同evidence_id下重复上报时仍重复结算预留；旧终态水位缺失不可静默跳过（否则重启默认零水位污染journal）。这两项不属于原报告已验证结论，需以本轮回归单独证明。订单已POST后本地ACK/UNKNOWN持久化双失败应保留预留并封闭继续派发；其重启恢复仍需reconciliation。

交接后再次审核发现：restore的DISPATCHING→UNKNOWN恢复闩锁不可永久保留，必须在耐久终态确认后解除dispatch专属需求，并保持事实缺口与持久失败封锁。仓储兼容恢复已扩为真实events/fills累计quantity/quote，验证冲突、缺身份或缺事实时明确migration/recovery required；完整历史journal/coverage仍不由这些水位替代。

## 最终修复与验收（主代理审核）

全部子代理均为GPT-6 Luna / max。主代理审核差异、发回缺陷修正并独立复验，未提交commit或部署服务器。

| 部分 | 最终结果 |
| --- | --- |
| 执行／Luna max | typed scoped snapshots与side/cut identity；删除缺符号造零；首次outbox关联完整；状态迁移inline await；持久失败拒绝发单；POST结果不确定保留预留；ACK与UNKNOWN双写失败本地sealed；明确command关联结算；数量与quote水位单调；累计均价差额形成真实delta成本；普通trade去重；终态不倒退；restart水位及dispatch闩锁恢复。执行两文件57 passed，主审最终全量通过 |
| 仓储恢复／Luna max | 含终态的水位恢复；已知其他账户先过滤；旧记录从真实events/fills恢复quantity/quote/身份；冲突、缺证据和正数量零quote显式拒绝；仓储目录53 passed，主审独立复跑通过 |
| 前端 | 44项Node、31项静态契约及7项真实DOM/图表/滚动场景通过；Chrome由子代理测试，主审独立内置浏览器复验同7项通过 |
| 决策 | 子代理59项定向通过；主审扩大决策/提交/静态90项通过。完整policy/state与输出重放、digest自校验、multi-ref及缺证据拒绝已验收 |
| 主审最终unit | `rtk proxy env -u CML_DATABASE_URL -u CML_TEST_DATABASE_URL -u CML_TEST_ASYNC_DATABASE_URL .venv/bin/python -m pytest tests/unit -m 'not integration and not live' -q --tb=short` → **1834 passed、4 skipped、1 warning，15.17s** |
| 静态检查 | 修改及新增的23个Python文件Ruff check和format --check通过；git diff --check通过；两套Compose config --quiet通过 |

4项skip为既有hub loopback guard；1个warning为Starlette/httpx弃用。未运行真实PostgreSQL integration、交易所live、服务器部署或实际订单验证。中间失败结果已清零，不作为最终未修项。

### 本轮明确保留的边界

1. 非零持仓AccountJournal历史/checkpoint/coverage未完整耐久恢复；决策context生成的PositionView与ExecutionBook尚未形成同一权威恢复链。
2. reservation与command watermark/outbox仍在不同事务中保存，进程崩溃可能留下不一致。本地seal与补偿不构成事务；policy/Trace/dependency/accepted exit同样需要完整提交协议。
3. 没有仓储的纯内存兼容路径仅在Book与coordinator均无持久仓储且无outbox时跳过派发迁移；生产明确接入command/reservation仓储，缺accepted outbox会拒绝发单。该兼容路径不能用于宣称耐久完成。
4. 旧命令缺可信身份或累计事实时恢复会明确阻断并要求迁移；部署前应只读检查旧数据并执行有证据的迁移，不允许批量补零。
5. policy_code_digest与risk_plan_digest仍主要是版本标签；当前输出可重现不证明历史代码内容冻结。部署镜像、writer fencing、归档timer和角色权限仍需现场验收。

前端“跳到最上面”的具体修复已落盘并通过本地真实浏览器场景：不回放过期scrollY，按当前阅读锚点/无锚点高度释放补偿，多root分别恢复minHeight，用户新交互优先；保留keyed DOM和ECharts完整运行时surface。线上仍运行旧镜像时不会自动获得此修复。
