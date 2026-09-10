# P2 与架构条目逐项复核

复核日期：2026-09-10。对象：docs/code-review-2026-03-19.md。依据为当前工作区代码及配置，不代表报告日期对应的历史版本或线上部署状态。仅新增本验收记录，未修改业务代码。

“成立”表示代码事实及问题方向有依据；“部分成立”表示事实存在，但影响、适用范围或建议需要修正；“不作为缺陷”表示属于有意设计或原结论证据不足。结构重复不等于运行错误。

## P2：逻辑边角

| 编号 | 结论 | 证据、适用范围与处理意见 |
|---|---|---|
| #24 | 部分成立 | [Candle15mAggregator](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/portfolio.py:85)要求最终分钟到达时收齐 15 个分钟；不足返回 None，进入下一窗口后旧窗口被替换。确实无缺口通知，也不能可靠处理最终分钟先到、缺失分钟后补到的情况。不过拒绝合成不完整 K 线是正确约束，不应放宽为用残缺数据平仓。影响限于本地聚合路径；daemon 配置官方 candle source 时不使用此聚合器。建议补缺口可观测性与官方数据补取。 |
| #25 | 成立，收窄 grace 描述 | [加载函数](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/daemon.py:1025)只请求当前最近闭合的一个 15m 窗口；[重启初始化](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/daemon.py:1141)清空 candle history，未按持仓退出进度补齐离线区间。因此多根确认历史丢失，停机期间首次 adverse candle 也可能漏掉。但已经持久化的 grace started_at/deadline 会[随持仓恢复](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/persistence/postgres/paper_daemon_repository.py:258)，不是所有 grace 状态都丢失。应保存退出处理游标并顺序补取历史；回补后的成交时间、价格也需明确，不能伪装成宕机期间真实成交。 |
| #26 | 成立，已复现 | [grace 分支](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/portfolio.py:356)及其 adverse 判断未检查最短持仓时间，而[非 grace 路径](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/portfolio.py:339)会传 minimum_holding_seconds。准确配置名为 candle_minimum_holding_buckets。使用相同持仓和 K 线、最短持仓 15000 秒、实际持仓 1800 秒：grace=0 为 OPEN，grace=1 已 CLOSED。建议修复组合配置语义，并补最短持仓与 grace 的交叉测试。 |
| #27 | 成立，有启用条件 | [context loader](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/strategy_runner/main.py:1026)无论信号方向都选 ask；[过滤器](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/daemon.py:766)对 SHORT 仍判断 entry_price > EMA。在 bid ≤ EMA < ask 时，空头实际可执行入场价不满足条件却能通过。仅影响单账户、允许 SHORT 且启用 above-EMA 过滤的配置；不是所有空头或当前 long-only 部署都受影响。context 应包含 bid/ask，由信号方向选执行价。 |
| #28 | 部分成立，不能直接取消 stale 检查 | [循环](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/daemon.py:1172)在 mark_positions 前跳过陈旧行情，确实会延后平仓及最长持仓判断。但完全无行情时根本不会进入循环；使用陈旧价格模拟当前成交也不正确。这是行情中断时退出策略不完整，不是“continue 本身必然错误”。建议独立监控超时、明确是否获取新鲜报价以及恢复后的退出规则。 |
| #29 | 成立，取决于 limit 的价格约束 | [价格量化](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/execution_account/orders/quantization.py:138)统一向下取整。若 SELL limit 表示最低接受价，100.05 在 tick=0.1 时变成 100.0，确实低于授权价格边界；应按最终 BUY/SELL 方向量化，BUY 向下、SELL 向上，并覆盖 reduce-only 的方向转换。如果输入只是可调参考价，则需另行明确策略语义。 |
| #30 | 部分成立，未找到当前生产调用 | [授权撤单方法](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/execution_account/binance/client.py:1077)只包装 Timeout/RequestError，不处理 HTTPStatusError，确实不同于 cancel_order_by_client_id。但全 src 中未找到这个带 command 参数方法的调用；当前 live 撤单走 coordinator/state_machine 或 cancel_order_by_client_id。因此不能写成已触达的紧急撤单事故。若保留此 API，应在授权后复用统一撤单实现。 |
| #31 | 部分成立，属于指标语义 | [publish](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/market_data/hub.py:257)取本批次最大 bucket_start 后覆盖旧值，较旧批次可以令数值下降。若指标定义为“历史最大进度”，应累积 max；若定义为“最近发布批次的进度”，当前行为合法且可能帮助暴露倒序。未见它改变订单执行或回放游标。应先明确指标定义，避免直接写成数据正确性 bug。 |
| #32 | 事实成立，影响是健康监测 | [missing fill 检测](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/execution_account/daemon.py:1008)在 request_reconnect 正常返回后，从 pending 集合移除 still_missing；甚至并未等待“成功重连”，只是重连请求返回。无 request_reconnect 方法时也会移除。原 fill 已由 REST 同步发现，此处不是成交记录丢失。若意图只触发一次重连，清理可以防止旧 fill 永远不会通过 WS 重播而引起重连风暴。建议明确一次性告警/重试语义，不能简单永远保留 still_missing。 |
| #33 | 成立，低优先级清理 | [_execution_database_url](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/execution_account/main.py:642)无值必抛异常，返回值保证非空；调用后的[再次判断](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/execution_account/main.py:122)不可达。同文件另一调用点也有重复判断。删除冗余判断即可。 |
| #34 | 部分成立，需身份冲突前提 | [事务](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/persistence/postgres/order_repository.py:155)中 return None 是正常退出，会提交前面的 intent INSERT。但普通同一 intent 重试时，该 INSERT 自身也因冲突 no-op，不会新增孤儿。只有不同 intent 复用已占用的 client_order_id 等不一致输入才会留下新 SUBMITTING intent；正常量化路径的 ID 由 run/candidate 决定。应验证已存在订单所属身份，并对不一致冲突回滚或显式拒绝，不能宣称所有幂等重试均污染数据。 |

## P2：冗余实现

| 编号 | 结论 | 证据与处理意见 |
|---|---|---|
| #35 | 重复部分成立，漂移不等于 bug | [state hub helpers](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/market_data/hub.py:1008)与[quote hub helpers](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/market_data/quote_hub.py:509)确有相似 JSON/字段校验。quote 接受已解析 dict，decimal 限字符串；state 接受更宽数字类型，并具有 sequence/replay 协议，两者并非完全相同协议。可抽取小型纯解析 helper，但必须保留各自输入契约、异常类型，不宜整段协议统一。 |
| #36 | 成立，纯维护问题 | [order _jsonable](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/persistence/postgres/order_repository.py:495)和[account _jsonable](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/persistence/postgres/account_repository.py:446)经 AST 比较完全一致。可移至 persistence 内部序列化 helper；未发现由此造成的当前错误。 |
| #37 | 重复成立，当前语义未分叉 | [paper resolver](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/paper.py:225)与[daemon resolver](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/daemon.py:1395)写法不同，但先排除过期后，target ≤ bucket_end ≤ expires 与剩余分支 bucket_end ≥ target 等价。可收敛，但不能用“写法分叉”推导成交结果已不同。 |
| #38 | 成立，纯维护问题 | [paper helpers](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/paper.py:341)和[replay helpers](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/replay.py:661)的四个函数经 AST 比较一致。适合共享 reporting/serialization helper，不需要扩大成业务重构。 |
| #39 | 当前生产双重串行不成立 | state machine [默认开启全实例锁](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/execution_account/orders/state_machine.py:186)属实，但两个 live 构建点均显式 serialize_commands=False：[一次性执行](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/live_rollout/main.py:1403)、[daemon](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/live_rollout/main.py:1982)。shadow 不经同样的 coordinator 包装时保留默认锁有合理性。未来调用者误配属于 API 易用性风险，可用构造工厂约束，不是当前 live 的性能 bug。 |
| #40 | 不作为缺陷 | [execute_approved_intent](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/execution_account/orders/coordinator.py:181)只是转发给 submit，注释明确为兼容旧调用名。没有第二份算法。除非完成所有调用方迁移，否则删除会破坏兼容性；此项最多是后续 API 收敛。 |
| #41 | 生产未接线成立，“全无用途”不准确 | [service.submit](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/market_data/capture/service.py:177)仍是可调用 API；生产 websocket 实际接[coordinator.submit](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/market_data/main.py:802)，因此 service 中的溢出 HALT 包装不保护该入口。可清理过时入口或明确职责，但不能由此推断系统其他 HALT 路径也失效。 |
| #42 | 成立，规模描述需收窄 | [load_active_entry_symbols_at](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/persistence/postgres/repository.py:245)先加载 membership，再 Python 排除 EXTENDED。但上游查询限制了单个已激活快照，并非全历史表扫描。SQL 直接 SELECT symbol/过滤 status 会减少传输和对象构造；是否优先优化取决于调用频率与集合规模。 |
| #43 | 不作为冗余缺陷，应保留 | [双 watermark 逻辑](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/market_data/runtime_states.py:560)明确允许 realtime 发布后继续接收迟到数据，再构建 durable 状态；到 durable 关闭时才[删除 accumulator 和 bucket quote](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/market_data/runtime_states.py:662)。按 bucket 最新 quote、realtime 前值、durable 前值服务不同时间边界，合并会令一条路径混入另一条路径的报价。若担忧缓存回收应单独审计生命周期，而不是以“三份缓存”判定可删。 |

## 架构项

| 编号 | 结论 | 证据与处理意见 |
|---|---|---|
| A1 | 已完成第一阶段 | 两个未被 Compose 使用的 PostgreSQL readiness CLI（SQLAlchemy 与 psycopg）及其测试已删除；Compose 继续只使用不启动 Python、不连接数据库的 `cml-local-healthcheck`。同时移除只供旧 DB 探针读取的 `CML_HEALTHCHECK_RUN_ID(S)` 环境变量。research_collector 的独立健康检查保留，因为它检查的是 collector 自己的状态文件。 |
| A2 | 成立，值得重构 | [live CLI](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/live_rollout/main.py:25)与[live postgres runtime](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/live_rollout/postgres_runtime.py:12)导入 shadow CLI 的查询私有函数，形成下层依赖 app 入口的耦合。下沉账户/风险/规则查询服务有明确收益。不过导入的是读取 helper，不能推导 live 因而获得 shadow 的“抑制写”语义。 |
| A3 | 已完成第一阶段 | [orderflow](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategies/order_flow_impulse/runtime.py:49)与[liquidation](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategies/liquidation_cascade/runtime.py:46)现在组合使用[StrategyRuntimeState](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategies/runtime_state.py:17)，共用 rolling buffer、warmup/cooldown、reset、checkpoint 恢复和事件前状态机；策略仍各自提供 required_data、事件发现和 signal/candidate 特征构造。payload 键仍由策略显式指定并保持 `market_state_buffers` 兼容；compression 的 `signal_buffers` 与 pending_signal_states 继续独立，不做基类或 checkpoint 迁移。 |
| A4 | 成立，收益明确 | [pair CLI](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/strategy_runner/main.py:1440)确实七次构造 identity，并有序数化参数和多段 account config。适合内部先引入账户列表/统一构造器，再为新配置格式保留旧 CLI 转换层。不是必须一次性破坏部署命令。 |
| A5a | 已完成第一阶段 | 对 YAML 解析后核对的命令重复已收敛：base 与附加账户 compose 各自用一个 `x-execution-account-command` 序列锚点，账户名移到显式 `CML_ACCOUNT_LABEL` 环境变量，CLI `--account-label` 仍保留并优先；live strategy 的 profile/top-N 参数改由既有环境解析器读取，账户、会话、hub、风险和退出差异仍显式保留。这样减少了可安全消除的复制，暂不引入跨文件生成器或无边界的 env 参数化。 |
| A5b | 已完成第一阶段 | [market-data 列表](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/compose.server.yaml:175)和[dashboard 列表](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/compose.server.yaml:608)共用 `x-paper-account-run-ids` 锚点，保留原有两个环境变量名和八个 run-id。旧的 `CML_HEALTHCHECK_RUN_ID(S)` 环境变量及其 DB 探针消费者已删除，因此原“漏更新变量导致当前生产漏检查”的因果不再存在。 |
| A6 | 映射丢字段成立，已复现；生产影响需限定 | [checkpoint payload](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategies/runtime_checkpoint.py:8)漏 6 个 domain 字段：4 个 closed_kline_1m 字段，以及 data_complete、missing_agg_trade_count。复现质量信息 (False,7) 往返变成 (True,0)。[DB 映射](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/persistence/postgres/runtime_state_repository.py:94)保留这些字段。但 paper daemon [持久化紧凑 checkpoint](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/daemon.py:1456)明确剔除 buffers，再从市场数据恢复，因此不能声称所有生产重启必受该 JSON 丢失影响。应补序列化 round-trip 契约；ORM 还包含额外持久化元数据，不能要求 ORM/JSON/domain 字段集机械完全相等。 |
| A7 | 重复成立，统一时必须保留优先级差异 | 多个 app 解析 database URL 属实：[live](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/live_rollout/main.py:4384)、[execution](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/execution_account/main.py:642)、[market](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/market_data/main.py:164)、[research collector](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/research_collector/main.py:239)。但 market 收到的是 config 默认值，其 plane env 优先于默认值；有些入口没有 CLI URL，不能一概称相同 CLI→env 链。可集中解析原语并保留每入口“显式覆盖/默认值/必填”规则，避免重构改变数据库平面选择。 |
| A8 | 已完成验收，暂不删除 | 逐方法搜索与集成/E2E 测试确认：run summary、paper artifacts、quality count、latest process state 和 exact-time universe snapshot 都有测试契约；`MonitoringObligationProvider` 有真实的强制 symbol 端口和 Fake 实现。唯一没有调用证据的是 `save_command`，但它对应 rollback command 审计表和交易所写操作授权链；鉴权及其运营语义按当前要求暂缓，因此保留，不以删除 repository 方法冒充修复。 |
| A9 | 已完成第一阶段 | 两 capture 配置确有共享字段，但差异还包括 `archive.streams`、rotation 大小、writer 数、磁盘阈值、realtime delay 和队列容量；两个 environment 文件也只是选择不同 universe/capture 组合。没有引入会改变列表覆盖和默认继承语义的深合并 overlay，新增配置一致性测试锁定共享传输/归档默认值，并显式断言环境差异。 |
| A10 | 不作为缺陷 | 当前 compose 没有 liquidation 服务属实，但[registry](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/registry.py:65)表示运行器支持哪些策略，不表示哪些策略已部署或已验证适合实盘。liquidation 确实有可运行的 runtime，也用于研究/测试，因此 supported 列表并未虚假承诺。可补生命周期标签及文档，不能仅因无常驻生产服务就从 build_runtime_strategy 删除，避免破坏研究与 paper 工作流。 |
| A11 | 重复成立，而且已有实际差异 | [run_market_data](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/market_data/main.py:936)与[run_market_data_for](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/market_data/main.py:1133)大段重复，但不是“仅停止条件不同”：正式入口监控 FIRST_COMPLETED、freshness、recovery metrics，先 cancel/drain 辅助任务再带 timeout 停 capture；定时入口 sleep 等待，先 stop capture 再取消 scheduler 等任务，health 参数也不同。建议让定时入口用停止事件复用正式入口，并保留“后台任务提前失败”传播，不只是抽两个机械 start/stop 函数。 |

### A8 方法级核实

| 项 | 当前调用证据 | 验收 |
|---|---|---|
| load_run_summary | tests/integration/persistence/test_strategy_run_repository.py:54；test_paper_daemon_artifacts.py:163 | 有真实集成/daemon artifact 测试调用；保留 repository 查询接口。 |
| load_paper_report_artifacts | tests/integration/persistence/test_strategy_run_repository.py:80 | 有真实集成测试调用；保留完整 artifact 读取接口。 |
| count_quality_events | tests/integration/persistence/test_capture_repository.py:53 | 集成测试读取质量事件总数；保留为 capture repository 查询契约。 |
| latest_process_state | tests/integration/persistence/test_capture_repository.py:55；tests/e2e/test_market_data_process.py:78 | 集成与 E2E 都使用；保留进程状态读取接口。 |
| save_command | src/crypto_momentum_lab/persistence/postgres/live_rollout_repository.py:104；全 src/tests/scripts 搜索未见调用 | 无调用证据成立，但它对应 rollback command 审计表和授权模型；鉴权/运营语义暂缓，保留并标记为后续接线项。 |
| MonitoringObligationProvider / NoMonitoringObligations | UniverseRefreshService 实际调用 forced_symbols；tests/unit/universe/test_refresh.py:176 注入 FakeObligations | 端口和 Fake 实现都有契约；生产默认空实现不等于全库死代码。生产持仓保护另经 protected-symbol loader/订阅 observer 接入。 |
| UniverseRepository.load_snapshot | tests/integration/persistence/test_repository.py:73；tests/e2e/test_universe_refresh.py:103 | exact-time 查询由集成/E2E 使用；与 `load_snapshot_at` 的“截至时点”语义不同，保留两者。 |

### 本轮第二批维护收敛

- **A1 已完成第一阶段**：删除未被 Compose 使用的两个 DB readiness CLI、对应测试及旧的 `CML_HEALTHCHECK_RUN_ID(S)` 配置；生产只保留本地文件心跳探针。
- **A3 已完成第一阶段**：orderflow 与 liquidation 共用组合式 runtime state 和 warmup/cooldown 状态机；保留各策略事件模型及 `market_state_buffers` checkpoint 兼容，compression 变体不强行合并。
- **A5 已完成第一阶段**：live execution command 在 base/overlay 内各自收敛到 YAML 序列锚点，账户名通过 `CML_ACCOUNT_LABEL` 注入并保留 CLI 覆盖；live strategy 的 profile/top-N 选项通过已有环境解析器读取；market-data/dashboard 的八个 paper run-id 共用一个锚点。配置 manifest、CLI fallback 和 compose 展开回归均已补齐。
- **A8 已完成验收**：原审查把测试/E2E 使用的 repository 查询误报为死代码；逐方法搜索后保留这些查询和 universe/monitoring port。`save_command` 没有调用方，但属于 rollback 审计/授权链，鉴权与运营语义暂缓，后续再决定接线或删除。
- **A9 已完成第一阶段**：保留 research/server capture 的环境差异，不引入跨文件深合并；新增配置一致性测试，锁定两份配置的共享连接、队列和归档默认值，并锁定 `forceOrder` archive 与 realtime delay 等有意差异。
- **#35 已完成第一阶段**：state/quote hub 共用字符串和 datetime 字段解析 helper；state 仍接受数值型 decimal、quote 仍只接受 decimal string，两个 hub 继续抛出各自协议异常。
- **#36 已完成**：`order_repository` 与 `account_repository` 共用 PostgreSQL `jsonable` helper，保留原有枚举、Decimal、时区 datetime、容器和 fallback 字符串语义。
- **#37 已完成**：paper 与 daemon 共用候选成交边界解析函数，统一目标时间、过期时间和闭合状态的判断；原有 paper/daemon 行为测试保持通过。
- **#38 已完成**：paper 与 replay 共用 strategy report serialization helper，避免四个相同转换函数继续漂移。
- **#41 已完成**：配置了 coordinator 的 `MarketDataCaptureService.submit` 现在经过 coordinator，保留 service 的磁盘保护与队列溢出处理，同时让生产入口使用 coordinator 的 symbol 过滤路径。
- **#42 已完成**：`load_active_entry_symbols_at` 直接在数据库端选择非 `EXTENDED` membership，保留最新 activated snapshot 与 `observed_at` 截止语义。

本轮没有处理鉴权 D8；A8 的 `save_command` 仅保留待运营语义确认，不作为删除项。

## 验证记录与建议顺序

- 静态检查了以上全部 #24–#43、A1–A11，并将 A5a/A5b 和 A8 子项分别验收；对调用关系搜索范围包括 src、tests、scripts 和当前 compose 配置。
- AST 比较确认 #36、#38 完全相同，#37 为不同写法的等价分支。
- 本地只读样例复现 #26 最短持仓被 grace 绕过，以及 A6 质量字段往返丢失。
- A1 删除后的健康检查回归由 Compose manifest、local shell probe 和相关应用启动测试覆盖；不再保留独立 DB readiness CLI 测试。
- A3 的 orderflow、liquidation、compression 与 runtime state/strategy runner 回归共 158 项通过；新增共享 runtime state 三个源文件的 mypy 检查通过。
- A5 的 compose manifest、execution-account/live-rollout CLI 与 profile 解析回归 69 项通过；base/overlay `docker compose config --quiet` 展开通过，相关源文件 mypy 与 Ruff 通过。
- A8 的 repository method 调用搜索覆盖 src/tests/scripts；strategy-run、capture、universe 的相关集成测试 10 项和 unit/rollback/universe 测试 18 项通过，未发现可安全删除的方法。`test_market_data_runtime_archives_and_updates_subscriptions` 两次单独运行都在既有行情归档收尾处缺少 ETHUSDT 文件，未归因于 A8，暂不混入本项修改。
- A9 的配置一致性回归 6 项通过；共享字段和有意环境差异均由 `tests/unit/config/test_loader.py` 覆盖。
- 本轮优先修正确性：#26、#25、启用相应配置时的 #27、价格边界契约明确后的 #29；A6 补 round-trip 并核查使用完整 buffer checkpoint 的路径。
- 维护重构优先 A2、A4、A11，其次小型纯 helper 收敛。#39、#40、#43、A10 不应按当前运行 bug 修；A8 和 A1 的原描述应修订。

额外边界：A11 的正式入口已先取消并等待 scheduler 后停 capture，这也意味着原 #22 所述“正式生产关停中 scheduler 与 stop 并发”的特定推演不能照用；定时入口顺序不同，风险须分别分析。
