# P2 与架构条目逐项复核

复核日期：2026-09-10。对象：docs/code-review-2026-03-19.md。依据为当前工作区代码及配置，不代表报告日期对应的历史版本或线上部署状态。本轮更新 #24–#34、A2、A4、A6、A7、A11 的验收记录并包含对应的本地修复。

“成立”表示代码事实及问题方向有依据；“部分成立”表示事实存在，但影响、适用范围或建议需要修正；“不作为缺陷”表示属于有意设计或原结论证据不足。结构重复不等于运行错误。

## P2：逻辑边角

| 编号 | 结论 | 证据、适用范围与处理意见 |
|---|---|---|
| #24 | 第一阶段已完成，官方补取仍是边界 | [Candle15mAggregator](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/portfolio.py:85)现在对不完整、缺分钟和跳过窗口记录有界 gap 事件；即使最后一分钟先到，后续分钟补齐后也会按第 0/14 分钟修正 15m 开收盘并只发出完整 K 线。仍不会用残缺数据平仓，也不会在本地聚合器内自动调用官方历史接口；daemon 配置官方 candle source 时不使用此聚合器。官方历史补取与回放/部署语义仍需单独明确。 |
| #25 | 第一阶段已完成，遗留空游标仍是边界 | [paper position](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/persistence/postgres/paper_daemon_repository.py:233)现在持久化 `last_candle_end`；[回补函数](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/daemon.py:1203)按该游标请求区间并逐根处理，覆盖 `confirmation_count>1` 与 grace 的历史顺序，回补成交时间使用官方 candle 结束时间、价格使用官方收盘价。迁移前已存在且游标为 NULL 的持仓仍只能从当前最近闭合 candle 开始，不能凭空恢复旧历史；如需覆盖这类存量持仓，需另定初始化游标/运营语义。 |
| #26 | 已完成 | [grace 分支](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/portfolio.py:459)现在统一按 `candle_minimum_holding_buckets × state_interval_seconds` 门控首次盈利平仓、恢复价触发、grace 超时和最长持仓；非 grace 路径继续使用相同的最短持仓策略。`test_candle_grace_respects_minimum_holding_period` 与普通 candle 最短持仓测试覆盖了 grace/非 grace 交叉边界。 |
| #27 | 已完成 | [context loader](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/strategy_runner/main.py:1032)分别提供 LONG 的 ask 与 SHORT 的 bid；[过滤器](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/daemon.py:1020)按信号方向选择对应执行价，再比较 EMA。`test_short_ema_filter_uses_bid_side_entry_price` 覆盖 bid ≤ EMA < ask 时拒绝空头、bid 上穿 EMA 时放行。 |
| #28 | 第一阶段已完成，退出仍等待新鲜行情 | [paper daemon stale 分支](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/daemon.py:1386)按 symbol 一次记录陈旧事件、重置策略并跳过策略与持仓标记；恢复后记录 `paper_market_state_recovered` 再继续处理，避免用陈旧价格模拟成交。[PostgreSQL source](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/live_source.py:118)在 idle timeout 记录结构化 error 并结束迭代，交由 Compose `restart: unless-stopped` 拉起。完全无行情时仍不会凭空生成退出成交；若需独立于 source 的强制退出或交易所保护单，需另定运营语义。 |
| #29 | 已完成 | [价格量化](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/execution_account/orders/quantization.py:114)现在按最终交易所方向选择 ROUND_DOWN/ROUND_UP：BUY 向下、SELL 向上；`_exchange_side` 同时考虑 reduce-only，因此平仓卖单也向上量化。`test_limit_price_rounds_outward_for_exchange_side` 覆盖普通多/空开仓与 reduce-only 平仓。 |
| #30 | 部分成立，未找到当前生产调用 | [授权撤单方法](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/execution_account/binance/client.py:1077)只包装 Timeout/RequestError，不处理 HTTPStatusError，确实不同于 cancel_order_by_client_id。但全 src 中未找到这个带 command 参数方法的调用；当前 live 撤单走 coordinator/state_machine 或 cancel_order_by_client_id。因此不能写成已触达的紧急撤单事故。若保留此 API，应在授权后复用统一撤单实现。 |
| #31 | 已完成，指标定义为全局历史最大进度 | [publish](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/market_data/hub.py:262)现在仅在批次最大 `bucket_start` 超过已有值时更新 `latest_bucket_start`，跨环境或乱序批次不会回拨监控进度；该字段只用于 hub metrics，不参与订单执行或回放游标。`test_market_state_hub_metrics_keep_latest_bucket_start_monotonic` 覆盖旧批次回拨场景。 |
| #32 | 第一阶段已完成，影响限定为健康监测 | [missing fill 检测](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/execution_account/daemon.py:1008)现在把 REST 新发现但 WS 未见的 fill key 保留在 pending，只有 stream 指标真正出现该 key 才清除；重连请求按 60 秒节流并记录请求时间，避免重复重连风暴。REST 已同步的成交记录不因 WS 缺失而丢失；若未来要求“重连成功”而非“stream 看到 key”作为确认，需另定义协议。 |
| #33 | 已完成 | [_execution_database_url](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/execution_account/main.py:639)通过共享 resolver 解析显式值和环境变量，无值立即抛出 `BadParameter` 并返回非空 `str`；两个 CLI 调用点均直接使用该结果，旧的调用后 `None` 判断已删除。数据库 URL resolver 与 execution account CLI 测试通过。 |
| #34 | 已完成 | [prepare_submission](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/persistence/postgres/order_repository.py:181)在唯一键冲突后读取现有订单并核对完整订单身份。同一 intent/同一计划仍返回 `None` 作为幂等结果；不同 intent 或计划字段不一致时抛出显式冲突并回滚同一事务中的新 intent。PostgreSQL 集成测试覆盖并发幂等与跨 intent 复用 ID。 |

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
| A2 | 已完成第一阶段 | [live CLI](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/live_rollout/main.py:170)与[live postgres runtime](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/live_rollout/postgres_runtime.py:57)不再导入 shadow CLI 私有函数；账户状态、风险配置和交易规则查询已下沉到[`runtime_context.py`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/persistence/postgres/runtime_context.py:1)。shadow 仍可复用同一持久化查询原语，但 live 不会因此获得 shadow 的写入/抑制语义。后续若需更深领域封装可另行安排，不属于当前耦合缺陷。 |
| A3 | 已完成第一阶段 | [orderflow](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategies/order_flow_impulse/runtime.py:49)与[liquidation](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategies/liquidation_cascade/runtime.py:46)现在组合使用[StrategyRuntimeState](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategies/runtime_state.py:17)，共用 rolling buffer、warmup/cooldown、reset、checkpoint 恢复和事件前状态机；策略仍各自提供 required_data、事件发现和 signal/candidate 特征构造。payload 键仍由策略显式指定并保持 `market_state_buffers` 兼容；compression 的 `signal_buffers` 与 pending_signal_states 继续独立，不做基类或 checkpoint 迁移。 |
| A4 | 已完成第一阶段 | [pair CLI](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/strategy_runner/main.py:1486)现在先生成 `_PairedAccountSpec`，再由[`_build_paired_account`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/strategy_runner/main.py:1768)统一构造 daemon/account；固定、candle、filtered 账户的 portfolio/filter 差异仍显式保留，原有序数 CLI 参数和 run-id 约束未改变。pair 构建测试已覆盖。 |
| A5a | 已完成第一阶段 | 对 YAML 解析后核对的命令重复已收敛：base 与附加账户 compose 各自用一个 `x-execution-account-command` 序列锚点，账户名移到显式 `CML_ACCOUNT_LABEL` 环境变量，CLI `--account-label` 仍保留并优先；live strategy 的 profile/top-N 参数改由既有环境解析器读取，账户、会话、hub、风险和退出差异仍显式保留。这样减少了可安全消除的复制，暂不引入跨文件生成器或无边界的 env 参数化。 |
| A5b | 已完成第一阶段 | [market-data 列表](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/compose.server.yaml:175)和[dashboard 列表](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/compose.server.yaml:608)共用 `x-paper-account-run-ids` 锚点，保留原有两个环境变量名和八个 run-id。旧的 `CML_HEALTHCHECK_RUN_ID(S)` 环境变量及其 DB 探针消费者已删除，因此原“漏更新变量导致当前生产漏检查”的因果不再存在。 |
| A6 | 已完成第一阶段，紧凑 checkpoint 边界保留 | [checkpoint payload](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategies/runtime_checkpoint.py:8)现在包含 4 个 `closed_kline_1m` 字段、`data_complete`、`missing_agg_trade_count`；缺省旧 payload 仍分别回退到 `None`、`True`、`0`。round-trip 测试覆盖质量字段与旧 payload 兼容。paper daemon 的[紧凑 checkpoint](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/daemon.py:1456)仍有意不保存 market buffers，重启从市场数据恢复，不能据此要求 JSON、ORM、domain 字段机械完全相等。 |
| A7 | 已完成第一阶段 | 各 app 现在复用[`resolve_database_url`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/config/database_url.py:4)，显式 URL 仍优先于按声明顺序的 plane/shared 环境变量；market 保留 config 默认值 fallback，execution/live/research 等入口继续各自执行必填校验或错误类型转换。7 个 resolver/CLI 回归测试覆盖优先级、fallback 和缺失值，不把不同入口强行改成同一必填语义。 |
| A8 | 已完成验收，暂不删除 | 逐方法搜索与集成/E2E 测试确认：run summary、paper artifacts、quality count、latest process state 和 exact-time universe snapshot 都有测试契约；`MonitoringObligationProvider` 有真实的强制 symbol 端口和 Fake 实现。唯一没有调用证据的是 `save_command`，但它对应 rollback command 审计表和交易所写操作授权链；鉴权及其运营语义按当前要求暂缓，因此保留，不以删除 repository 方法冒充修复。 |
| A9 | 已完成第一阶段 | 两 capture 配置确有共享字段，但差异还包括 `archive.streams`、rotation 大小、writer 数、磁盘阈值、realtime delay 和队列容量；两个 environment 文件也只是选择不同 universe/capture 组合。没有引入会改变列表覆盖和默认继承语义的深合并 overlay，新增配置一致性测试锁定共享传输/归档默认值，并显式断言环境差异。 |
| A10 | 不作为缺陷 | 当前 compose 没有 liquidation 服务属实，但[registry](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategy_runner/registry.py:65)表示运行器支持哪些策略，不表示哪些策略已部署或已验证适合实盘。liquidation 确实有可运行的 runtime，也用于研究/测试，因此 supported 列表并未虚假承诺。可补生命周期标签及文档，不能仅因无常驻生产服务就从 build_runtime_strategy 删除，避免破坏研究与 paper 工作流。 |
| A11 | 已完成第一阶段 | [run_market_data](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/market_data/main.py:1038)现在是唯一的生命周期监督入口；[run_market_data_for](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/market_data/main.py:1235)只创建定时 stop event 并复用它，因此正式入口的 FIRST_COMPLETED、freshness/health/recovery 监控、任务取消和 capture/publisher/hub 有序收尾保持一致。生命周期测试覆盖定时停止和 capture 退出边界。 |

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
