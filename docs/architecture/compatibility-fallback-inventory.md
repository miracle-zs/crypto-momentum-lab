# 兼容与兜底分支清点（2026-10-04）

基线清点针对 2026-10-04 的工作区代码，包含此前未提交的改动。后续清理状态记录在下节；此前没有连接服务器。后续已只读核查生产表结构和流记录，结果见当前处理状态。

## 范围与计数口径

- 扫描 473 个第一方 Python 文件（src 349、scripts 77、alembic 47），以及 37 个第一方 JS、1 个 MJS、部署和配置文件。测试、虚拟环境、第三方 ECharts、数据导出和备份不计入。
- AST 扫描属性探测、显式 get 默认值、or、条件表达式和异常处理；文本扫描 legacy/fallback/compat 名称、前端替代操作符和部署默认值。
- 共 6406 条候选记录，覆盖 355 个命中文件。其中 242 条为 Python 属性默认/接口探测；37 条是没有默认值的反射，通常不是兜底。
- **6406 不是实际兼容分支数量，也不是可删除数量。** 同一业务分支可能出现多个记录，普通布尔表达式、配置默认、异常重抛和注释也会命中。
- 未把普通 if 分支、每个隐含 None 的 dict.get、运行时动态注入、反射生成代码全部穷举为语义分支。因此这是可追溯的全范围候选基线，不是“所有真实兜底都已确认”的证明。

逐条机器清单：[compatibility-fallback-candidates.csv](compatibility-fallback-candidates.csv)。列包括文件、起止行、所属函数、类别、表达式和核查状态；属性探测家族及部分src表达式候选已回填当前结果，其余仍保留基线候选状态。

## 当前处理状态

本清单保留清理前扫描快照；CSV 的位置及表达式保留基线，属性探测及部分表达式对照状态已回填。6406 条候选和 242 条属性探测不是当前剩余数量；文件行号可能随修改移动。候选包括正常缺省值、异常处理与真实协议分支，不能全部按兼容废料删除。

已完成的家族：

- 显式留存装配，删除生产 Mock 识别；运行时正式字段、必有报价 Hub、分区检查与 ScalarResult 返回接口统一。
- 删除失效 legacy_checkpoint 解码调用、订单合成成交生成分支及虚构观察时间、不存在的仓库私有规则缓存。
- 批次并发统计按正式模型读取，修复持久化在途订单未取 plan 导致漏计。
- 策略状态更新、参数、决策哈希、枚举与轨迹字段去结构兼容；规则仅接受SymbolLotRules，由实盘边界按symbol解析并冻结；删除函数/字典形状解析和默认规则猜测，规则进入参数哈希。策略参数与状态外层统一数据类序列化。
- 采集批量写入、成交量指标、行情观测、收盘流生命周期及遥测接口统一；流溢出指标、容量字节数、队列排空和数据库池接口明确；主采集源停止/游标/恢复与可关闭生成器契约明确。
- 账户 token、事件记账、重连、实时同步、后台持久化和心跳状态统一；客户端提交边界回调成为明确契约。
- 持久化覆盖证据、订单窗口、rowcount、命令字段与预留归属明确；仪表盘性能读模型明确。
- 回放统一 ReplayEvaluation，数据集目录按统一仓库校验接口调用。

当前重新扫描 src 为 3 处属性默认/接口探测，均位于数据库错误诊断 `_extract_sqlstate`：SQLAlchemy 包装异常的 orig，以及不同 DBAPI 驱动的 pgcode/sqlstate。保留这一外部驱动适配。这个数字只覆盖 src，不与初始全仓 242 条直接比较。真实历史订单恢复、未知订单反查、外部协议解析及可选配置保留；生产只读核查：runtime_market_states_15s 和 strategy_runtime_events 的 relkind 均为 p；position_fact_journal_events 中仍有 4 条 legacy-postgres-account 事件。新建数据库迁移仍创建普通表，因此普通表留存路径保留。

- 策略配置只保留正式配置对象；缓存维护直接接收正式策略接口，删除可选回调及指标壳。前端图表时间字段、overview 就绪状态字段统一，WebSocket 关闭码使用正式帧接口。

进一步核查：删除同步决策工厂、回调结果列表套层和仅转调decide的DecisionEngine类，实盘直接await单一决策提交；保留同批逐笔读取最新事实。数据集解析只接受名称，DatasetScope只持有DatasetId；删除留存锁失败继续执行及订单参考价格查询失败静默返回的异常分支；检查点仓库直接使用绑定引擎及连接池，删除监听注册吞异常。RetentionAuthority 仓库必须显式注入，删除默认内存仓库；成交payload、订单身份details及仪表盘遥测按正式字典字段读取。账户快照和采集 sink 统一异步回调，删除可等待性猜测；同步 RetentionAuthority 有 DecisionTraceService 的实际依赖登记调用，异步入口用于后台留存，两个调用方式暂保留。执行事务的确认仓位数量改为必需输入，删除回查独立快照。

242条原属性探测候选已回填：238条原表达式不再存在，3条SQLAlchemy/DBAPI诊断适配保留，1条所在失效归档脚本已删除。表达式消失不等同于所在文件所有分支审计完成。

无调用方入口扫描进一步清理：删除重复恢复编码方法、策略配置读取包装、13个无引用属性、2个闲置数据模型、5个影子ORM及无消费者的绩效ORM；删除信号记录器只写不读的4096条缓存及配置、两项只写状态和失效批次污染重现脚本。真实成交补查、REST失败时仍按已知仓位退出、未知订单反查及宽限到期市价退出保留；杠杆降档实际改变交易政策，不能按死兼容代码机械删除。

下单编译收敛：策略身份只取正式candidate，不再回退context或硬编码值；退出截断统一流程，删除不可到达的补量分支并保留批次入场价格。共享仓位锁必须显式注入，退出账户标识不再回退run_id；删除采集GC析构兜底，保留显式await stop。

src显式默认值/or/条件表达式的AST对照补充：132条基线表达式已不在当前文件，2681条仍能匹配，9条无法按独立表达式解析；均不等于兼容分支计数。CSV保留原位置，并对132条回填表达式消失状态。

同symbol并发上限只保留RiskGateway组合FixedLiveLimits的提交检查，删除EntryLane重复计数/门禁、重复配置和透传；正数校验移至FixedLiveLimits单一配置来源。原3项旧入场包装/门禁测试删除，领域并发统计和实际提交风控测试保留。

恢复回调全部显式装配，删除6处默认空lambda。退出分配改为plan_exit_allocations顶层纯函数，删除仅测试使用的命令工厂及随机身份/默认方向推断；ExecutionRequest订单类型只使用正式str契约。

采集仅保留正在使用的ArchiveJournal，删除无消费者LocalBatchSpool/SpoolRecord及spool参数；连接池删除闲置控制频率参数和空start。真实WebSocket限速参数保留，由连接自身使用。

退出请求直接使用计划中明确的本次分配，删除批次容量替代、缺字段均摊和已有分配的重复FIFO编译。删除只有测试调用的quantize_order_plan旧入口及旧接口测试，QuantizationRejection归入当前trade_command_planner；宽限挂单交易所契约仍验证当前生产入口。

执行请求必须明确提供策略side，Book删除从已有episode、position_side或默认LONG推导方向。生产协调器从已确定的交易所方向及reduce_only转换，不在BOTH模式猜测持仓方向。

退出分配只接收运行时实际使用的PositionView，删除只有测试使用的旧投影模型兼容分支；预留可用量按模型守恒约束直接计算，删除负数归零。

删除无生产选择者的灰尘吸收策略、absorbed_dust字段与序列化兜底、2项旧测试；全量/未指定数量的分配统一循环，保留实际全仓退出策略。

闲置枚举清理：移除无人构造的领域紧急清仓类型（独立真实清仓入口保留）、重复CONSOLIDATE_ELIGIBLE默认、无人使用CashFlowType、无实现SHARPE_RATIO及无生产者的租约/规则失效原因。外部事件和持久化恢复使用的字符串枚举不按静态引用数量机械删除。

绩效配置删除未使用benchmark/annualization_factor，unit/version改为关键词；修正仪表盘单位字符串误传版本，实际计算结果元数据回归通过。

删除8项无消费者字段：模拟排队延迟/资金费率、覆盖结束条件、闲置费用值、采集回执提交序号、回放时间和核验数量。真实Journal提交水位与现金流事实路径保留。

删除无调用方/CLI的运行事件影子表准备与切换工具、报告模型及专用索引/旧表名工具；保留真实日分区创建、过期分区清理和普通表留存，不修改数据库物理表。

绩效覆盖按正式现金流字段直接算哈希，删除单调用方反射/猜类型包装及6处无时区时间补UTC；坏证据仍返回未核验，不影响交易通道。

删除无消费者的ExecutionRequest策略版本及协调器对应v0兜底；删除未适配当前Book/原子提交的submit-plan手动入口、加载器与plan_runner，保留实际daemon和账户清仓。

删除仅旧测试使用的LiveRolloutSession单次发单外壳、结果和执行协议，以及session对runtime_session的重复导出；生产会话生命周期仍保留。

提交请求策略名称显式传入：原子提交取已批准意图身份，直接提交取新计划身份，删除硬编码策略名回退；退出编排直接使用正式订单枚举。取消与反查路径不增加提交条件。

内部ManagedLivePosition与OrderExecutionPlan只接收正式方向枚举，删除3处字符串转换；保留订单数据库读取、仓位分类与交易所孤单取消的外部解析边界。

退出策略模式和事实覆盖状态只接收正式枚举，删除2处内部重复解码；trace_audit和recovery_codec的外部字符串解析保留。

完整修改记录见 [simplification.md](simplification.md)。

## 扫描分类

|类别|记录数|
|---|---:|
|名称/注释线索（不是分支计数）|242|
|显式默认值读取（含正常配置/协议解析）|589|
|or 候选（含正常布尔条件）|1834|
|部署环境默认值（通常为正常配置）|337|
|条件替代候选（含正常业务分支）|1853|
|异常路径（含重新抛出/正常取消）|812|
|属性默认/接口探测|242|
|无默认值反射（通常不是兜底）|37|
|前端/脚本文本候选（需人工判断）|460|

## 重点分支家族与处置方向

下面是清理前的主要家族快照，供追溯原问题；不是当前待办。已删除和已统一的家族见上节。位置与描述保留基线原样，不用于判断当前代码是否仍有该分支。

|类别|位置|现有兼容或兜底|判断与调用证据|
|---|---|---|---|
|优先核查事实风险|[src/crypto_momentum_lab/live_rollout/order_identity_adapter.py:88](../../src/crypto_momentum_lab/live_rollout/order_identity_adapter.py#L88)|无成交明细时，用订单生成合成成交；价格可回退到持仓均价或 1.0，数量可回退到订单总量，时间可回退到订单创建时间。|position_batches.py:111 有真实调用。不能把假定价格当作交易所成交事实；先核查触发条件和存量。|
|优先收敛测试兼容|[src/crypto_momentum_lab/domain/operational/retention_authority.py:702](../../src/crypto_momentum_lab/domain/operational/retention_authority.py#L702)|探测公开/私有 session_factory；识别 Mock/AsyncMock/MagicMock；不可用时改用内存 RetentionAuthority。|apps/market_data/main.py:884 有真实调用。应显式装配持久化或测试适配器，移除生产 Mock 检测。|
|接口兼容候选|[src/crypto_momentum_lab/domain/decision/policy_transition.py:345](../../src/crypto_momentum_lab/domain/decision/policy_transition.py#L345)|sizing_model 缺失即跳过；lot rules 支持 callable、Mapping、直接对象；属性齐全便临时转成 SymbolLotRules。|先统一有效策略与规则接口；缺规则拒绝下单是实际底线，不能一起删除。|
|接口兼容候选|[src/crypto_momentum_lab/domain/decision/policy_transition.py:509](../../src/crypto_momentum_lab/domain/decision/policy_transition.py#L509)|退出策略、冷却/宽限期、position_mode、金额、策略名称、版本和 order_type 多处默认；状态更新在多个方法间探测或跳过。|需区分正式策略可选项与缺字段替身；不能把所有默认项认定为旧兼容。|
|序列化形状兼容|[src/crypto_momentum_lab/domain/decision/policy_transition.py:176](../../src/crypto_momentum_lab/domain/decision/policy_transition.py#L176)|策略/状态支持 dataclass、__dict__、__slots__ 的多形状序列化。|统一对象模型后可收敛；先确认回放及参数哈希是否需要多模型。|
|接口兼容候选|[src/crypto_momentum_lab/domain/decision/decision_engine.py:450](../../src/crypto_momentum_lab/domain/decision/decision_engine.py#L450)|market ref scope、closed_candles、state.environment、trade_count、candidate_generator、market_envelope 等存在性探测。|区别可选注入与旧字段兼容，不能机械删除全部反射。|
|并发计数默认值|[src/crypto_momentum_lab/domain/execution/position_batches.py:109](../../src/crypto_momentum_lab/domain/execution/position_batches.py#L109)|仓位/批次缺字段时默认空 symbol、未退出、未成交、entry_order_count=1、空订单身份。|开仓数量限制依赖此函数，优先要求完整输入，避免缺字段静默影响计数。|
|执行客户端能力探测|[src/crypto_momentum_lab/execution_account/orders/state_machine.py:119](../../src/crypto_momentum_lab/execution_account/orders/state_machine.py#L119)|探测 set_exchange_boundary_callbacks；客户端支持时委托，否则由状态机记录边界。|先核对所有执行客户端实现，统一回调职责。|
|回执默认值|[src/crypto_momentum_lab/execution_account/orders/coordinator.py:1274](../../src/crypto_momentum_lab/execution_account/orders/coordinator.py#L1274)|recovery_required 缺失默认 False、diagnostics 缺失默认空。|应核对返回结果联合类型，按正式结果读取。|
|账户流接口兼容|[src/crypto_momentum_lab/execution_account/daemon.py:527](../../src/crypto_momentum_lab/execution_account/daemon.py#L527)|continuity_token、事件记录、request_reconnect、持久化、snapshot、实时同步和指标均有属性/方法探测。|涉及账户连续性与重连；先统一流和同步接口，再删结构兼容。|
|策略缓存能力探测|[src/crypto_momentum_lab/live_rollout/daemon.py:225](../../src/crypto_momentum_lab/live_rollout/daemon.py#L225)|cache_protected_symbols、prune_inactive_symbols、缓存指标和 volume_metrics 可缺失。|缓存维护是实际职责；可将可选能力明确化。|
|行情模型字段兜底|[src/crypto_momentum_lab/live_rollout/market_loop.py:256](../../src/crypto_momentum_lab/live_rollout/market_loop.py#L256)|market_state_progress、策略指标、data_complete 和预热必需字段默认。|区分完整性校验与缺字段兼容。|
|实时装配能力探测|[src/crypto_momentum_lab/live_rollout/runtime_orchestrator.py:534](../../src/crypto_momentum_lab/live_rollout/runtime_orchestrator.py#L534)|telemetry.record 探测；规则缺失时读取 live_repository 私有缓存。|规则来源与遥测接口可统一，避免跨层访问私有缓存。|
|同步状态字段兜底|[src/crypto_momentum_lab/live_rollout/postgres_runtime.py:1005](../../src/crypto_momentum_lab/live_rollout/postgres_runtime.py#L1005)|对账 status、成交游标 from_id/start_time_ms/last_checked_at、checkpoint id/time 探测。|缺游标与尚未对账可是真实情况；只删除结构探测，保留业务未知状态。|
|行情/成交外部协议默认|[src/crypto_momentum_lab/execution_account/binance/rest_parser.py:29](../../src/crypto_momentum_lab/execution_account/binance/rest_parser.py#L29)|symbol 缺失时使用已请求标的；REST/WS 多名称字段解析与时间默认。|外部接口适配，不属于内部过度设计；必须按交易所协议判断。|
|杠杆降档行为|[src/crypto_momentum_lab/execution_account/binance/client.py:920](../../src/crypto_momentum_lab/execution_account/binance/client.py#L920)|leverage_fallback_steps 控制设杠杆失败后的降档尝试，默认 2 次。|真实改变账户交易配置；是否保留应由交易政策决定，不能当作无效代码直接删。|
|提交归因证据兜底|[src/crypto_momentum_lab/persistence/postgres/order_submission_repository.py:237](../../src/crypto_momentum_lab/persistence/postgres/order_submission_repository.py#L237)|价格取 plan.price、reference_price；必要时用 intent.desired_notional 推导预留相关量。|涉及交易事实和资金预留，须检查市价单及缺价格场景。|
|订单 ORM 字段兼容|[src/crypto_momentum_lab/persistence/postgres/position_order_window.py:121](../../src/crypto_momentum_lab/persistence/postgres/position_order_window.py#L121)|state/created_at/updated_at 缺字段默认，position_side 缺失默认 BOTH。|可收敛为正式 ORM/观察模型；必须确认老行是否存在空值。|
|预留策略归属兼容|[src/crypto_momentum_lab/persistence/postgres/position_reservation_repository.py:287](../../src/crypto_momentum_lab/persistence/postgres/position_reservation_repository.py#L287)|PositionKey.strategy_name 不存在时使用仓库策略名称；rowcount 缺失默认 0。|前者需明确归属来源；后者需核对驱动返回类型。|
|历史账本兼容|[src/crypto_momentum_lab/persistence/postgres/account_journal_store.py:360](../../src/crypto_momentum_lab/persistence/postgres/account_journal_store.py#L360)|没有新恢复数据时加载旧恢复；legacy_checkpoint 与当前 checkpoint 二选一。|真实读取历史数据。生产账本仍含 4 条旧流事件，历史恢复不能认定可删除。|
|历史订单身份兼容|[src/crypto_momentum_lab/live_rollout/order_identity.py:30](../../src/crypto_momentum_lab/live_rollout/order_identity.py#L30)|按历史事件展开订单身份、识别冲突、补累计成交状态/数量。|position_classification.py 有真实调用；先核对旧数据迁移与仍持仓批次。|
|历史退出绑定兼容|[src/crypto_momentum_lab/live_rollout/position_classification.py:482](../../src/crypto_momentum_lab/live_rollout/position_classification.py#L482)|将旧退出批次绑定修复为最近此前入场批次；歧义/不可重建分支。|真实历史恢复，不能仅凭 legacy 名称删除。|
|失效历史恢复调用|[src/crypto_momentum_lab/persistence/postgres/account_journal_store.py:826](../../src/crypto_momentum_lab/persistence/postgres/account_journal_store.py#L826)|读取 legacy_checkpoint 时调用 PositionRecoveryCodec.decode_legacy_checkpoint。|当前编码器不定义此方法；本地导入确认属性不存在。若命中旧记录将抛 AttributeError。未查数据库，尚未确认触发。|
|历史分区与留存|[src/crypto_momentum_lab/persistence/postgres/runtime_state_partitions.py:317](../../src/crypto_momentum_lab/persistence/postgres/runtime_state_partitions.py#L317)|旧非分区表、legacy_table、切表/归档路径；事件分区模块也有同类路径。|迁移与维护职责；确认服务器表结构后才能去旧路径。|
|历史分区与留存|[src/crypto_momentum_lab/persistence/postgres/operational_retention.py:229](../../src/crypto_momentum_lab/persistence/postgres/operational_retention.py#L229)|未分区表使用小批删除。|真实数据库形态兼容，和交易热路径无关。|
|MarketBook 仓库能力探测|[src/crypto_momentum_lab/domain/market/market_book.py:387](../../src/crypto_momentum_lab/domain/market/market_book.py#L387)|批量 canonical refs、verify_manifest、list_manifests 可无方法。|明确所有仓库适配器后收敛接口；当前不能宣称只存在一种实现。|
|回放输出形状兼容|[src/crypto_momentum_lab/domain/market/decision_trace_service.py:166](../../src/crypto_momentum_lab/domain/market/decision_trace_service.py#L166)|evaluator 输出支持 intent/候选等多种形状，金额读 desired_notional 或 target_notional。|回放工具接口可统一；历史决策不可用时的 UnreproducibleError 应保留。|
|采集仓库批量/逐条兼容|[src/crypto_momentum_lab/market_data/capture/coordinator.py:238](../../src/crypto_momentum_lab/market_data/capture/coordinator.py#L238)|save_quality_events 方法探测及替代写入路径。|统一仓库接口后可删。|
|行情观测字段兜底|[src/crypto_momentum_lab/market_data/observability.py:111](../../src/crypto_momentum_lab/market_data/observability.py#L111)|连接、队列、采集、恢复指标大量默认 None/0/空列表。|部分会把未知显示成正常零值；先区分对象缺失与字段缺失。|
|行情运行时能力探测|[src/crypto_momentum_lab/apps/market_data/main.py:915](../../src/crypto_momentum_lab/apps/market_data/main.py#L915)|分区检查、dependency.all、维护仓库、quote hub、prefetcher、留存会话等探测。|核对装配对象及真实可选运行模式。|
|研究采集接口兼容|[src/crypto_momentum_lab/research_collector/service.py:156](../../src/crypto_momentum_lab/research_collector/service.py#L156)|后台任务可缺失；source.stop/resume_after_recovery/set_resume_cursor/aclose 探测；读取队列私有 unfinished_tasks。|区分可选源适配器能力与绕过初始化的测试兼容。|
|仪表盘后端模型默认|[src/crypto_momentum_lab/operator_dashboard/performance_builder.py:111](../../src/crypto_momentum_lab/operator_dashboard/performance_builder.py#L111)|时间、correction_id、evidence_hash、approval_ref、effective_at 多处默认。|展示层应区分未知与真实零值，不能静默遮住模型错误。|
|仪表盘后端其他默认|[src/crypto_momentum_lab/operator_dashboard/risk_execution_queries.py:1](../../src/crypto_momentum_lab/operator_dashboard/risk_execution_queries.py#L1)|风险/执行查询的枚举、字段或数据为空时的后备路径。|具体位置见 CSV；优先统一正式读模型。|
|数据库诊断适配|[src/crypto_momentum_lab/persistence/postgres/paper_daemon_repository.py:269](../../src/crypto_momentum_lab/persistence/postgres/paper_daemon_repository.py#L269)|bind/sync_engine/pool 和 checkedin/checkedout 能力探测；枚举 value 或原始值。|驱动诊断有真实差异；枚举兼容可以单独收敛。|
|真实退出流程|[src/crypto_momentum_lab/live_rollout/exits.py:678](../../src/crypto_momentum_lab/live_rollout/exits.py#L678)|宽限限价单到期，先取消后按剩余仓位发市场退出；exit_processor.py 处理取消结果与剩余量。|用户要求的业务主流程，应保留；fallback 命名不等于兼容废料。|
|真实后台恢复|[src/crypto_momentum_lab/execution_account/sync.py:368](../../src/crypto_momentum_lab/execution_account/sync.py#L368)|缺少已知 symbol/游标时补查成交；user_data_sync 的恢复与异常通知。|防漏记成交与仓位校准职责，保留。|
|真实采集恢复|[src/crypto_momentum_lab/apps/market_data/main.py:1258](../../src/crypto_momentum_lab/apps/market_data/main.py#L1258)|数据库写入不可用时 spool/恢复写入，以及 WS 断流补齐。|数据可靠性机制，保留或另行评估容量，不能当字段兼容删除。|
|启动时间边界替代|[src/crypto_momentum_lab/live_rollout/startup_recovery.py:98](../../src/crypto_momentum_lab/live_rollout/startup_recovery.py#L98)|启动恢复缺少某些锚点时选择替代 cutover；预热检查完整字段。|必须按可重放范围判断，不能随意改当前时间。|
|通知失效替代通道|[src/crypto_momentum_lab/apps/live_rollout/main.py:1327](../../src/crypto_momentum_lab/apps/live_rollout/main.py#L1327)|风险控制推送不可用时保留 PostgreSQL 后备传播通道。|真实通知/控制可靠性，保留或明确选择一个持久通道。|

## 配置与前端替代值

- strategies/registry.py 的旧扁平字段构造和二次覆盖已删除。实盘装配与账户 profile 测试统一提供 OrderFlowImpulseConfig，配置哈希回归保持稳定。
- config/database_url.py、live_rollout/runtime_options.py 及 compose 的环境变量默认属于配置来源选择。哪些默认可删取决于部署是否显式提供值，本轮未读取服务器环境。
- 前端图表时间别名与 overview 状态别名已统一。空数据占位、格式默认和网络失败显示最近成功结果仍属真实展示降级。CSV 保留清理前命中的第一方前端快照。
- scripts/maintenance_archive/legacy_order_identity_repair.py 属于维护归档，不在正常发单流程。alembic 的历史迁移不是运行时门禁，本轮没有改写迁移历史。

## 不应按关键词删除的命中

无默认 getattr 在字段循环、ORM 比较和 dataclass 序列化中是正常反射；外部 REST/WS 字段名称差异是协议适配；正常取消、重抛异常、正式可空字段、配置默认也不是过度设计证据。unknown 订单反查、实际仓位校准和宽限超时市场退出应分别评估，不能和旧模型兼容混删。

## 已确认处置与保留依据

- 已删除不存在的 `decode_legacy_checkpoint` 调用、订单合成成交、虚构观察时间、内部对象结构探测、生产 Mock 识别和无引用协议。
- 账户快照及采集 sink 只接受异步回调；Hub 的持仓预期登记只接受同步回调，实际装配为 `AccountPositionExpectationRegistry.register`。删除同步/异步可等待性猜测。
- 留存服务的同步依赖登记由 `DecisionTraceService` 使用，异步入口服务于后台维护，两种实际调用方式保留。
- 历史账本事件、订单身份与退出批次修复仍用于持仓恢复；生产仍有旧流事件，不能把恢复事实一并删除。
- 分区切换是维护任务；普通表留存适用于新建迁移库。分区清理失败后转普通删除的异常兜底已删除。
- 市价订单参考价格、外部 REST/WS 字段差异、杠杆降档、未知订单反查、后台仓位校准、行情补齐及宽限期退出属于实际业务或外部协议职责。
- 6406 条 CSV 记录仍是初始候选快照，未逐条完成语义处置，不能据此宣称全系统审核完成。

## 全部命中文件索引

以下索引包括候选误报，供逐文件审核。正文尚未逐一验证这些文件内每个候选的生产调用关系。

|文件|候选记录|属性默认/探测|异常路径|
|---|---:|---:|---:|
|[alembic/versions/20260911_0035_lease_code_generation_fence.py](../../alembic/versions/20260911_0035_lease_code_generation_fence.py)|1|0|0|
|[alembic/versions/20260918_0037_strategy_runtime_events_composite_pk.py](../../alembic/versions/20260918_0037_strategy_runtime_events_composite_pk.py)|3|0|0|
|[compose.live.accounts.yaml](../../compose.live.accounts.yaml)|161|0|0|
|[compose.server.yaml](../../compose.server.yaml)|83|0|0|
|[deploy/live-runtime.yaml](../../deploy/live-runtime.yaml)|93|0|0|
|[scripts/backfill_binance_15s_from_aggtrades.py](../../scripts/backfill_binance_15s_from_aggtrades.py)|20|0|4|
|[scripts/build_daily_account_alignment.py](../../scripts/build_daily_account_alignment.py)|32|0|1|
|[scripts/close_all_positions.py](../../scripts/close_all_positions.py)|14|0|4|
|[scripts/diagnostics/cml_checkpoint_recovery_20261003.py](../../scripts/diagnostics/cml_checkpoint_recovery_20261003.py)|1|0|0|
|[scripts/diagnostics/cml_entry_fact_order_20261002.py](../../scripts/diagnostics/cml_entry_fact_order_20261002.py)|3|0|0|
|[scripts/diagnostics/cml_impulse_scan_benchmark_20261002.py](../../scripts/diagnostics/cml_impulse_scan_benchmark_20261002.py)|10|0|0|
|[scripts/download_official_15m_klines.py](../../scripts/download_official_15m_klines.py)|8|0|2|
|[scripts/download_tradingview_futures_15m_klines.py](../../scripts/download_tradingview_futures_15m_klines.py)|10|0|1|
|[scripts/emergency_flatten_positions.py](../../scripts/emergency_flatten_positions.py)|17|0|5|
|[scripts/fetch_official_15m_via_server.py](../../scripts/fetch_official_15m_via_server.py)|3|0|1|
|[scripts/maintenance_archive/legacy_order_identity_repair.py](../../scripts/maintenance_archive/legacy_order_identity_repair.py)|20|0|1|
|[scripts/materialize_server_paper_snapshot.py](../../scripts/materialize_server_paper_snapshot.py)|20|0|0|
|[scripts/merge_runtime_state_exports.py](../../scripts/merge_runtime_state_exports.py)|3|0|0|
|[scripts/research_archive/analyze_b1_trend_filters.py](../../scripts/research_archive/analyze_b1_trend_filters.py)|22|0|0|
|[scripts/research_archive/analyze_breakout_acceptance.py](../../scripts/research_archive/analyze_breakout_acceptance.py)|88|0|2|
|[scripts/research_archive/analyze_candle_entry_boundary.py](../../scripts/research_archive/analyze_candle_entry_boundary.py)|40|0|1|
|[scripts/research_archive/analyze_cycle_sensitivity.py](../../scripts/research_archive/analyze_cycle_sensitivity.py)|6|0|0|
|[scripts/research_archive/analyze_liquidation_daemon_gaps.py](../../scripts/research_archive/analyze_liquidation_daemon_gaps.py)|9|0|0|
|[scripts/research_archive/analyze_liquidation_version_transition.py](../../scripts/research_archive/analyze_liquidation_version_transition.py)|5|0|0|
|[scripts/research_archive/analyze_live_baseline.py](../../scripts/research_archive/analyze_live_baseline.py)|22|0|1|
|[scripts/research_archive/analyze_live_ema_filters.py](../../scripts/research_archive/analyze_live_ema_filters.py)|19|0|0|
|[scripts/research_archive/analyze_live_gainer_topn.py](../../scripts/research_archive/analyze_live_gainer_topn.py)|35|0|3|
|[scripts/research_archive/analyze_live_limit_entry.py](../../scripts/research_archive/analyze_live_limit_entry.py)|106|0|3|
|[scripts/research_archive/analyze_live_orderflow_winner_features.py](../../scripts/research_archive/analyze_live_orderflow_winner_features.py)|16|0|1|
|[scripts/research_archive/analyze_live_rank_filters.py](../../scripts/research_archive/analyze_live_rank_filters.py)|9|0|0|
|[scripts/research_archive/analyze_live_trading_data.py](../../scripts/research_archive/analyze_live_trading_data.py)|102|0|2|
|[scripts/research_archive/analyze_orderflow_15m_false_signals.py](../../scripts/research_archive/analyze_orderflow_15m_false_signals.py)|38|0|1|
|[scripts/research_archive/analyze_orderflow_b0_high_returns.py](../../scripts/research_archive/analyze_orderflow_b0_high_returns.py)|35|0|1|
|[scripts/research_archive/analyze_orderflow_core_features.py](../../scripts/research_archive/analyze_orderflow_core_features.py)|2|0|0|
|[scripts/research_archive/analyze_orderflow_daily_regime.py](../../scripts/research_archive/analyze_orderflow_daily_regime.py)|4|0|0|
|[scripts/research_archive/analyze_orderflow_forward_filters.py](../../scripts/research_archive/analyze_orderflow_forward_filters.py)|18|0|0|
|[scripts/research_archive/analyze_orderflow_structural_stop.py](../../scripts/research_archive/analyze_orderflow_structural_stop.py)|40|2|5|
|[scripts/research_archive/analyze_paper_b1_ema_filters.py](../../scripts/research_archive/analyze_paper_b1_ema_filters.py)|14|0|0|
|[scripts/research_archive/analyze_server_paper_accounts.py](../../scripts/research_archive/analyze_server_paper_accounts.py)|70|0|2|
|[scripts/research_archive/analyze_server_pool_labels.py](../../scripts/research_archive/analyze_server_pool_labels.py)|9|0|0|
|[scripts/research_archive/analyze_three_day_momentum.py](../../scripts/research_archive/analyze_three_day_momentum.py)|54|0|2|
|[scripts/research_archive/audit_liquidation_candidate_gates.py](../../scripts/research_archive/audit_liquidation_candidate_gates.py)|2|0|0|
|[scripts/research_archive/audit_live_baseline_replay.py](../../scripts/research_archive/audit_live_baseline_replay.py)|28|0|7|
|`scripts/research_archive/backfill_paper_grace_exits.py`（已删除）|17|1|2|
|[scripts/research_archive/backtest_live_exit_grace.py](../../scripts/research_archive/backtest_live_exit_grace.py)|43|0|1|
|[scripts/research_archive/backtest_live_gainer_top20.py](../../scripts/research_archive/backtest_live_gainer_top20.py)|8|0|1|
|[scripts/research_archive/backtest_live_gainer_top20_server_window.py](../../scripts/research_archive/backtest_live_gainer_top20_server_window.py)|21|0|1|
|[scripts/research_archive/backtest_live_top20_full_official.py](../../scripts/research_archive/backtest_live_top20_full_official.py)|25|0|1|
|[scripts/research_archive/backtest_paper05_gainer_long_b1_088_official.py](../../scripts/research_archive/backtest_paper05_gainer_long_b1_088_official.py)|31|0|1|
|[scripts/research_archive/backtest_signal_filters.py](../../scripts/research_archive/backtest_signal_filters.py)|37|0|0|
|[scripts/research_archive/build_orderflow_candidate_dataset.py](../../scripts/research_archive/build_orderflow_candidate_dataset.py)|10|0|0|
|[scripts/research_archive/build_orderflow_capital_series.py](../../scripts/research_archive/build_orderflow_capital_series.py)|6|0|0|
|[scripts/research_archive/compare_orderflow_b1_exit_rules.py](../../scripts/research_archive/compare_orderflow_b1_exit_rules.py)|36|0|1|
|[scripts/research_archive/compare_parameter_equity.py](../../scripts/research_archive/compare_parameter_equity.py)|28|0|5|
|[scripts/research_archive/diagnose_research_vs_live.py](../../scripts/research_archive/diagnose_research_vs_live.py)|3|0|0|
|[scripts/research_archive/generate_orderflow_grace_svgs.py](../../scripts/research_archive/generate_orderflow_grace_svgs.py)|8|0|0|
|[scripts/research_archive/generate_orderflow_grace_visualization.py](../../scripts/research_archive/generate_orderflow_grace_visualization.py)|9|0|0|
|[scripts/research_archive/generate_server_equity_visualization.py](../../scripts/research_archive/generate_server_equity_visualization.py)|1|0|0|
|[scripts/research_archive/reconcile_live_account_pnl.py](../../scripts/research_archive/reconcile_live_account_pnl.py)|2|0|0|
|[scripts/research_archive/replay_candle_entry_boundary.py](../../scripts/research_archive/replay_candle_entry_boundary.py)|43|0|5|
|[scripts/research_archive/replay_candle_exits_server.py](../../scripts/research_archive/replay_candle_exits_server.py)|57|0|5|
|[scripts/research_archive/replay_liquidation_entry_variants.py](../../scripts/research_archive/replay_liquidation_entry_variants.py)|78|0|2|
|[scripts/research_archive/replay_orderflow_break_even.py](../../scripts/research_archive/replay_orderflow_break_even.py)|131|0|1|
|[scripts/research_archive/report_live_ema_entry_relation.py](../../scripts/research_archive/report_live_ema_entry_relation.py)|4|0|0|
|[scripts/research_archive/research_volume_filters.py](../../scripts/research_archive/research_volume_filters.py)|10|0|0|
|[scripts/research_archive/select_parameter_candle15m.py](../../scripts/research_archive/select_parameter_candle15m.py)|4|0|0|
|[scripts/research_archive/update_orderflow_visualization.py](../../scripts/research_archive/update_orderflow_visualization.py)|7|0|0|
|[scripts/review_archive/review_round2_20260924_repro.py](../../scripts/review_archive/review_round2_20260924_repro.py)|2|0|1|
|[src/crypto_momentum_lab/apps/execution_account/main.py](../../src/crypto_momentum_lab/apps/execution_account/main.py)|13|0|4|
|[src/crypto_momentum_lab/apps/live_rollout/main.py](../../src/crypto_momentum_lab/apps/live_rollout/main.py)|51|0|14|
|[src/crypto_momentum_lab/apps/live_rollout/risk_control_cli.py](../../src/crypto_momentum_lab/apps/live_rollout/risk_control_cli.py)|5|0|1|
|[src/crypto_momentum_lab/apps/market_data/main.py](../../src/crypto_momentum_lab/apps/market_data/main.py)|73|8|19|
|[src/crypto_momentum_lab/apps/operator_dashboard/main.py](../../src/crypto_momentum_lab/apps/operator_dashboard/main.py)|4|0|0|
|[src/crypto_momentum_lab/apps/research_collector/main.py](../../src/crypto_momentum_lab/apps/research_collector/main.py)|6|0|3|
|[src/crypto_momentum_lab/build_info.py](../../src/crypto_momentum_lab/build_info.py)|4|0|0|
|[src/crypto_momentum_lab/config/credentials.py](../../src/crypto_momentum_lab/config/credentials.py)|4|0|0|
|[src/crypto_momentum_lab/config/database_url.py](../../src/crypto_momentum_lab/config/database_url.py)|1|0|0|
|[src/crypto_momentum_lab/domain/account/baseline_checkpoint.py](../../src/crypto_momentum_lab/domain/account/baseline_checkpoint.py)|4|0|0|
|[src/crypto_momentum_lab/domain/account/event_journal.py](../../src/crypto_momentum_lab/domain/account/event_journal.py)|4|0|0|
|[src/crypto_momentum_lab/domain/account/models.py](../../src/crypto_momentum_lab/domain/account/models.py)|20|0|0|
|[src/crypto_momentum_lab/domain/account/snapshot_models.py](../../src/crypto_momentum_lab/domain/account/snapshot_models.py)|1|0|0|
|[src/crypto_momentum_lab/domain/decision/decision_engine.py](../../src/crypto_momentum_lab/domain/decision/decision_engine.py)|60|22|2|
|[src/crypto_momentum_lab/domain/decision/decision_frame.py](../../src/crypto_momentum_lab/domain/decision/decision_frame.py)|1|0|0|
|[src/crypto_momentum_lab/domain/decision/policy_transition.py](../../src/crypto_momentum_lab/domain/decision/policy_transition.py)|72|43|1|
|[src/crypto_momentum_lab/domain/decision/ports.py](../../src/crypto_momentum_lab/domain/decision/ports.py)|1|0|0|
|[src/crypto_momentum_lab/domain/decision/simulation_execution.py](../../src/crypto_momentum_lab/domain/decision/simulation_execution.py)|13|1|0|
|[src/crypto_momentum_lab/domain/decision/trace_audit.py](../../src/crypto_momentum_lab/domain/decision/trace_audit.py)|42|4|2|
|[src/crypto_momentum_lab/domain/execution/account_journal.py](../../src/crypto_momentum_lab/domain/execution/account_journal.py)|32|0|0|
|[src/crypto_momentum_lab/domain/execution/command_codec.py](../../src/crypto_momentum_lab/domain/execution/command_codec.py)|12|0|1|
|[src/crypto_momentum_lab/domain/execution/command_lifecycle.py](../../src/crypto_momentum_lab/domain/execution/command_lifecycle.py)|2|0|0|
|[src/crypto_momentum_lab/domain/execution/cumulative_report.py](../../src/crypto_momentum_lab/domain/execution/cumulative_report.py)|12|0|0|
|[src/crypto_momentum_lab/domain/execution/durable_evidence.py](../../src/crypto_momentum_lab/domain/execution/durable_evidence.py)|10|0|1|
|[src/crypto_momentum_lab/domain/execution/evidence_codec.py](../../src/crypto_momentum_lab/domain/execution/evidence_codec.py)|2|0|0|
|[src/crypto_momentum_lab/domain/execution/evidence_digest.py](../../src/crypto_momentum_lab/domain/execution/evidence_digest.py)|2|0|0|
|[src/crypto_momentum_lab/domain/execution/evidence_grouping.py](../../src/crypto_momentum_lab/domain/execution/evidence_grouping.py)|4|0|0|
|[src/crypto_momentum_lab/domain/execution/evidence_lifecycle.py](../../src/crypto_momentum_lab/domain/execution/evidence_lifecycle.py)|10|0|0|
|[src/crypto_momentum_lab/domain/execution/evidence_models.py](../../src/crypto_momentum_lab/domain/execution/evidence_models.py)|9|0|0|
|[src/crypto_momentum_lab/domain/execution/evidence_rules.py](../../src/crypto_momentum_lab/domain/execution/evidence_rules.py)|2|0|0|
|[src/crypto_momentum_lab/domain/execution/evidence_settlement.py](../../src/crypto_momentum_lab/domain/execution/evidence_settlement.py)|7|0|0|
|[src/crypto_momentum_lab/domain/execution/exchange_contract.py](../../src/crypto_momentum_lab/domain/execution/exchange_contract.py)|1|0|0|
|[src/crypto_momentum_lab/domain/execution/execution_book.py](../../src/crypto_momentum_lab/domain/execution/execution_book.py)|129|0|29|
|[src/crypto_momentum_lab/domain/execution/execution_coordinator.py](../../src/crypto_momentum_lab/domain/execution/execution_coordinator.py)|12|0|0|
|[src/crypto_momentum_lab/domain/execution/fill_attribution.py](../../src/crypto_momentum_lab/domain/execution/fill_attribution.py)|7|0|0|
|[src/crypto_momentum_lab/domain/execution/order_read_models.py](../../src/crypto_momentum_lab/domain/execution/order_read_models.py)|2|0|0|
|[src/crypto_momentum_lab/domain/execution/order_rules.py](../../src/crypto_momentum_lab/domain/execution/order_rules.py)|1|0|0|
|[src/crypto_momentum_lab/domain/execution/order_state.py](../../src/crypto_momentum_lab/domain/execution/order_state.py)|1|0|0|
|[src/crypto_momentum_lab/domain/execution/position_batches.py](../../src/crypto_momentum_lab/domain/execution/position_batches.py)|14|11|0|
|[src/crypto_momentum_lab/domain/execution/position_book.py](../../src/crypto_momentum_lab/domain/execution/position_book.py)|16|0|0|
|[src/crypto_momentum_lab/domain/execution/position_ledger.py](../../src/crypto_momentum_lab/domain/execution/position_ledger.py)|85|0|2|
|[src/crypto_momentum_lab/domain/execution/position_ledger_models.py](../../src/crypto_momentum_lab/domain/execution/position_ledger_models.py)|32|0|0|
|[src/crypto_momentum_lab/domain/execution/position_recovery.py](../../src/crypto_momentum_lab/domain/execution/position_recovery.py)|15|0|0|
|[src/crypto_momentum_lab/domain/execution/position_repair.py](../../src/crypto_momentum_lab/domain/execution/position_repair.py)|17|0|0|
|[src/crypto_momentum_lab/domain/execution/position_repair_models.py](../../src/crypto_momentum_lab/domain/execution/position_repair_models.py)|2|0|0|
|[src/crypto_momentum_lab/domain/execution/projection_codec.py](../../src/crypto_momentum_lab/domain/execution/projection_codec.py)|8|0|0|
|[src/crypto_momentum_lab/domain/execution/recovery_codec.py](../../src/crypto_momentum_lab/domain/execution/recovery_codec.py)|44|0|2|
|[src/crypto_momentum_lab/domain/execution/recovery_models.py](../../src/crypto_momentum_lab/domain/execution/recovery_models.py)|21|0|0|
|[src/crypto_momentum_lab/domain/execution/snapshot_encoding.py](../../src/crypto_momentum_lab/domain/execution/snapshot_encoding.py)|1|0|0|
|[src/crypto_momentum_lab/domain/execution/trade_command.py](../../src/crypto_momentum_lab/domain/execution/trade_command.py)|4|0|0|
|[src/crypto_momentum_lab/domain/live_rollout/models.py](../../src/crypto_momentum_lab/domain/live_rollout/models.py)|1|0|0|
|[src/crypto_momentum_lab/domain/market/decision_trace_service.py](../../src/crypto_momentum_lab/domain/market/decision_trace_service.py)|10|5|2|
|[src/crypto_momentum_lab/domain/market/market_book.py](../../src/crypto_momentum_lab/domain/market/market_book.py)|15|3|1|
|[src/crypto_momentum_lab/domain/market/models.py](../../src/crypto_momentum_lab/domain/market/models.py)|9|0|0|
|[src/crypto_momentum_lab/domain/market/revision_models.py](../../src/crypto_momentum_lab/domain/market/revision_models.py)|2|0|0|
|[src/crypto_momentum_lab/domain/market/state_codec.py](../../src/crypto_momentum_lab/domain/market/state_codec.py)|9|0|3|
|[src/crypto_momentum_lab/domain/operational/operational_read_model.py](../../src/crypto_momentum_lab/domain/operational/operational_read_model.py)|11|0|0|
|[src/crypto_momentum_lab/domain/operational/retention_authority.py](../../src/crypto_momentum_lab/domain/operational/retention_authority.py)|11|3|3|
|[src/crypto_momentum_lab/domain/operational/retention_contract.py](../../src/crypto_momentum_lab/domain/operational/retention_contract.py)|1|0|0|
|[src/crypto_momentum_lab/domain/operational/retention_models.py](../../src/crypto_momentum_lab/domain/operational/retention_models.py)|9|0|0|
|[src/crypto_momentum_lab/domain/operational/runtime_metadata.py](../../src/crypto_momentum_lab/domain/operational/runtime_metadata.py)|1|1|0|
|[src/crypto_momentum_lab/domain/performance/account_performance.py](../../src/crypto_momentum_lab/domain/performance/account_performance.py)|10|0|0|
|[src/crypto_momentum_lab/domain/performance/metric_models.py](../../src/crypto_momentum_lab/domain/performance/metric_models.py)|5|0|0|
|[src/crypto_momentum_lab/domain/risk/models.py](../../src/crypto_momentum_lab/domain/risk/models.py)|2|0|0|
|[src/crypto_momentum_lab/domain/strategy/entry_candidate.py](../../src/crypto_momentum_lab/domain/strategy/entry_candidate.py)|2|1|0|
|[src/crypto_momentum_lab/domain/strategy/entry_policy.py](../../src/crypto_momentum_lab/domain/strategy/entry_policy.py)|7|0|0|
|[src/crypto_momentum_lab/domain/strategy/entry_policy_evaluation.py](../../src/crypto_momentum_lab/domain/strategy/entry_policy_evaluation.py)|3|0|0|
|[src/crypto_momentum_lab/domain/strategy/models.py](../../src/crypto_momentum_lab/domain/strategy/models.py)|3|0|2|
|[src/crypto_momentum_lab/domain/strategy/paper_models.py](../../src/crypto_momentum_lab/domain/strategy/paper_models.py)|1|0|0|
|[src/crypto_momentum_lab/domain/strategy/position_exit.py](../../src/crypto_momentum_lab/domain/strategy/position_exit.py)|4|0|0|
|[src/crypto_momentum_lab/domain/strategy/sizing.py](../../src/crypto_momentum_lab/domain/strategy/sizing.py)|3|0|0|
|[src/crypto_momentum_lab/domain/universe/membership.py](../../src/crypto_momentum_lab/domain/universe/membership.py)|1|0|0|
|[src/crypto_momentum_lab/domain/universe/models.py](../../src/crypto_momentum_lab/domain/universe/models.py)|1|0|0|
|[src/crypto_momentum_lab/execution_account/balance_history.py](../../src/crypto_momentum_lab/execution_account/balance_history.py)|1|0|0|
|[src/crypto_momentum_lab/execution_account/binance/client.py](../../src/crypto_momentum_lab/execution_account/binance/client.py)|102|3|38|
|[src/crypto_momentum_lab/execution_account/binance/exit_recovery_rules.py](../../src/crypto_momentum_lab/execution_account/binance/exit_recovery_rules.py)|4|0|2|
|[src/crypto_momentum_lab/execution_account/binance/order_status.py](../../src/crypto_momentum_lab/execution_account/binance/order_status.py)|1|0|1|
|[src/crypto_momentum_lab/execution_account/binance/request_rules.py](../../src/crypto_momentum_lab/execution_account/binance/request_rules.py)|4|0|1|
|[src/crypto_momentum_lab/execution_account/binance/response_rules.py](../../src/crypto_momentum_lab/execution_account/binance/response_rules.py)|5|0|3|
|[src/crypto_momentum_lab/execution_account/binance/rest_parser.py](../../src/crypto_momentum_lab/execution_account/binance/rest_parser.py)|32|0|0|
|[src/crypto_momentum_lab/execution_account/binance/user_data.py](../../src/crypto_momentum_lab/execution_account/binance/user_data.py)|18|0|11|
|[src/crypto_momentum_lab/execution_account/binance/user_data_models.py](../../src/crypto_momentum_lab/execution_account/binance/user_data_models.py)|4|0|0|
|[src/crypto_momentum_lab/execution_account/binance/user_data_parser.py](../../src/crypto_momentum_lab/execution_account/binance/user_data_parser.py)|18|0|5|
|[src/crypto_momentum_lab/execution_account/daemon.py](../../src/crypto_momentum_lab/execution_account/daemon.py)|67|10|30|
|[src/crypto_momentum_lab/execution_account/expectations.py](../../src/crypto_momentum_lab/execution_account/expectations.py)|6|0|0|
|[src/crypto_momentum_lab/execution_account/fill_progress.py](../../src/crypto_momentum_lab/execution_account/fill_progress.py)|7|0|1|
|[src/crypto_momentum_lab/execution_account/fill_scan_plan.py](../../src/crypto_momentum_lab/execution_account/fill_scan_plan.py)|4|0|0|
|[src/crypto_momentum_lab/execution_account/hub.py](../../src/crypto_momentum_lab/execution_account/hub.py)|95|0|17|
|[src/crypto_momentum_lab/execution_account/orders/coordinator.py](../../src/crypto_momentum_lab/execution_account/orders/coordinator.py)|71|2|25|
|`src/crypto_momentum_lab/execution_account/orders/quantization.py`（已删除）|11|0|0|
|[src/crypto_momentum_lab/execution_account/orders/recovery.py](../../src/crypto_momentum_lab/execution_account/orders/recovery.py)|2|0|0|
|[src/crypto_momentum_lab/execution_account/orders/state_machine.py](../../src/crypto_momentum_lab/execution_account/orders/state_machine.py)|31|2|10|
|[src/crypto_momentum_lab/execution_account/orders/trade_command_planner.py](../../src/crypto_momentum_lab/execution_account/orders/trade_command_planner.py)|10|0|0|
|[src/crypto_momentum_lab/execution_account/retention.py](../../src/crypto_momentum_lab/execution_account/retention.py)|5|0|2|
|[src/crypto_momentum_lab/execution_account/risk_control_hub.py](../../src/crypto_momentum_lab/execution_account/risk_control_hub.py)|45|0|17|
|[src/crypto_momentum_lab/execution_account/snapshot_changes.py](../../src/crypto_momentum_lab/execution_account/snapshot_changes.py)|6|0|0|
|[src/crypto_momentum_lab/execution_account/sync.py](../../src/crypto_momentum_lab/execution_account/sync.py)|39|0|2|
|[src/crypto_momentum_lab/execution_account/sync_models.py](../../src/crypto_momentum_lab/execution_account/sync_models.py)|3|0|0|
|[src/crypto_momentum_lab/execution_account/user_data_fields.py](../../src/crypto_momentum_lab/execution_account/user_data_fields.py)|7|0|2|
|[src/crypto_momentum_lab/execution_account/user_data_sync.py](../../src/crypto_momentum_lab/execution_account/user_data_sync.py)|27|0|0|
|[src/crypto_momentum_lab/health/memory.py](../../src/crypto_momentum_lab/health/memory.py)|7|0|4|
|[src/crypto_momentum_lab/health/stream_availability.py](../../src/crypto_momentum_lab/health/stream_availability.py)|7|0|0|
|[src/crypto_momentum_lab/live_rollout/account_channel.py](../../src/crypto_momentum_lab/live_rollout/account_channel.py)|8|0|3|
|[src/crypto_momentum_lab/live_rollout/checkpoint_coordinator.py](../../src/crypto_momentum_lab/live_rollout/checkpoint_coordinator.py)|8|0|2|
|[src/crypto_momentum_lab/live_rollout/checkpoint_writer.py](../../src/crypto_momentum_lab/live_rollout/checkpoint_writer.py)|8|0|6|
|[src/crypto_momentum_lab/live_rollout/closed_candle_feed.py](../../src/crypto_momentum_lab/live_rollout/closed_candle_feed.py)|19|3|5|
|[src/crypto_momentum_lab/live_rollout/command_receipt_recovery.py](../../src/crypto_momentum_lab/live_rollout/command_receipt_recovery.py)|5|0|0|
|[src/crypto_momentum_lab/live_rollout/context.py](../../src/crypto_momentum_lab/live_rollout/context.py)|7|0|4|
|[src/crypto_momentum_lab/live_rollout/context_prefetch.py](../../src/crypto_momentum_lab/live_rollout/context_prefetch.py)|4|0|4|
|[src/crypto_momentum_lab/live_rollout/control_plane.py](../../src/crypto_momentum_lab/live_rollout/control_plane.py)|6|0|0|
|[src/crypto_momentum_lab/live_rollout/daemon.py](../../src/crypto_momentum_lab/live_rollout/daemon.py)|12|5|1|
|[src/crypto_momentum_lab/live_rollout/daemon_lifecycle.py](../../src/crypto_momentum_lab/live_rollout/daemon_lifecycle.py)|12|0|11|
|[src/crypto_momentum_lab/live_rollout/decision_facts.py](../../src/crypto_momentum_lab/live_rollout/decision_facts.py)|23|0|9|
|[src/crypto_momentum_lab/live_rollout/entry_cache.py](../../src/crypto_momentum_lab/live_rollout/entry_cache.py)|21|0|10|
|[src/crypto_momentum_lab/live_rollout/entry_control.py](../../src/crypto_momentum_lab/live_rollout/entry_control.py)|2|0|0|
|[src/crypto_momentum_lab/live_rollout/entry_expectations.py](../../src/crypto_momentum_lab/live_rollout/entry_expectations.py)|1|0|1|
|[src/crypto_momentum_lab/live_rollout/entry_lane.py](../../src/crypto_momentum_lab/live_rollout/entry_lane.py)|28|1|5|
|[src/crypto_momentum_lab/live_rollout/entry_order_cancellation.py](../../src/crypto_momentum_lab/live_rollout/entry_order_cancellation.py)|4|0|1|
|[src/crypto_momentum_lab/live_rollout/entry_orders.py](../../src/crypto_momentum_lab/live_rollout/entry_orders.py)|4|0|2|
|[src/crypto_momentum_lab/live_rollout/entry_runtime.py](../../src/crypto_momentum_lab/live_rollout/entry_runtime.py)|16|0|7|
|[src/crypto_momentum_lab/live_rollout/exit_channels.py](../../src/crypto_momentum_lab/live_rollout/exit_channels.py)|30|0|7|
|[src/crypto_momentum_lab/live_rollout/exit_event_coordinator.py](../../src/crypto_momentum_lab/live_rollout/exit_event_coordinator.py)|4|0|0|
|[src/crypto_momentum_lab/live_rollout/exit_failure_policy.py](../../src/crypto_momentum_lab/live_rollout/exit_failure_policy.py)|1|0|0|
|[src/crypto_momentum_lab/live_rollout/exit_lane.py](../../src/crypto_momentum_lab/live_rollout/exit_lane.py)|10|0|4|
|[src/crypto_momentum_lab/live_rollout/exit_processor.py](../../src/crypto_momentum_lab/live_rollout/exit_processor.py)|73|0|11|
|[src/crypto_momentum_lab/live_rollout/exit_receipt_recovery.py](../../src/crypto_momentum_lab/live_rollout/exit_receipt_recovery.py)|10|1|2|
|[src/crypto_momentum_lab/live_rollout/exits.py](../../src/crypto_momentum_lab/live_rollout/exits.py)|73|0|0|
|[src/crypto_momentum_lab/live_rollout/gates.py](../../src/crypto_momentum_lab/live_rollout/gates.py)|2|0|0|
|[src/crypto_momentum_lab/live_rollout/health_monitor.py](../../src/crypto_momentum_lab/live_rollout/health_monitor.py)|3|0|2|
|[src/crypto_momentum_lab/live_rollout/hub_cursor.py](../../src/crypto_momentum_lab/live_rollout/hub_cursor.py)|7|0|0|
|[src/crypto_momentum_lab/live_rollout/market_assembly.py](../../src/crypto_momentum_lab/live_rollout/market_assembly.py)|7|0|4|
|[src/crypto_momentum_lab/live_rollout/market_cache.py](../../src/crypto_momentum_lab/live_rollout/market_cache.py)|2|0|0|
|[src/crypto_momentum_lab/live_rollout/market_loop.py](../../src/crypto_momentum_lab/live_rollout/market_loop.py)|22|4|6|
|[src/crypto_momentum_lab/live_rollout/missing_order_resolution.py](../../src/crypto_momentum_lab/live_rollout/missing_order_resolution.py)|1|0|0|
|[src/crypto_momentum_lab/live_rollout/order_event_runtime.py](../../src/crypto_momentum_lab/live_rollout/order_event_runtime.py)|1|0|1|
|[src/crypto_momentum_lab/live_rollout/order_identity.py](../../src/crypto_momentum_lab/live_rollout/order_identity.py)|30|0|4|
|[src/crypto_momentum_lab/live_rollout/order_identity_adapter.py](../../src/crypto_momentum_lab/live_rollout/order_identity_adapter.py)|26|0|0|
|[src/crypto_momentum_lab/live_rollout/order_identity_errors.py](../../src/crypto_momentum_lab/live_rollout/order_identity_errors.py)|4|1|0|
|[src/crypto_momentum_lab/live_rollout/order_reconciliation.py](../../src/crypto_momentum_lab/live_rollout/order_reconciliation.py)|11|0|5|
|[src/crypto_momentum_lab/live_rollout/pending_entries.py](../../src/crypto_momentum_lab/live_rollout/pending_entries.py)|3|0|0|
|[src/crypto_momentum_lab/live_rollout/position_batches.py](../../src/crypto_momentum_lab/live_rollout/position_batches.py)|21|0|1|
|[src/crypto_momentum_lab/live_rollout/position_classification.py](../../src/crypto_momentum_lab/live_rollout/position_classification.py)|58|0|2|
|[src/crypto_momentum_lab/live_rollout/position_lifecycle.py](../../src/crypto_momentum_lab/live_rollout/position_lifecycle.py)|1|0|0|
|[src/crypto_momentum_lab/live_rollout/position_self_healing.py](../../src/crypto_momentum_lab/live_rollout/position_self_healing.py)|8|0|2|
|[src/crypto_momentum_lab/live_rollout/postgres_runtime.py](../../src/crypto_momentum_lab/live_rollout/postgres_runtime.py)|31|8|2|
|[src/crypto_momentum_lab/live_rollout/profile.py](../../src/crypto_momentum_lab/live_rollout/profile.py)|5|0|2|
|[src/crypto_momentum_lab/live_rollout/readiness.py](../../src/crypto_momentum_lab/live_rollout/readiness.py)|7|0|3|
|[src/crypto_momentum_lab/live_rollout/resource_lifecycle.py](../../src/crypto_momentum_lab/live_rollout/resource_lifecycle.py)|7|0|5|
|[src/crypto_momentum_lab/live_rollout/risk_control.py](../../src/crypto_momentum_lab/live_rollout/risk_control.py)|17|0|6|
|[src/crypto_momentum_lab/live_rollout/runtime_cache.py](../../src/crypto_momentum_lab/live_rollout/runtime_cache.py)|2|0|1|
|[src/crypto_momentum_lab/live_rollout/runtime_manifest.py](../../src/crypto_momentum_lab/live_rollout/runtime_manifest.py)|25|0|7|
|[src/crypto_momentum_lab/live_rollout/runtime_options.py](../../src/crypto_momentum_lab/live_rollout/runtime_options.py)|38|0|7|
|[src/crypto_momentum_lab/live_rollout/runtime_orchestrator.py](../../src/crypto_momentum_lab/live_rollout/runtime_orchestrator.py)|46|2|12|
|[src/crypto_momentum_lab/live_rollout/runtime_session.py](../../src/crypto_momentum_lab/live_rollout/runtime_session.py)|19|0|16|
|[src/crypto_momentum_lab/live_rollout/runtime_supervisor.py](../../src/crypto_momentum_lab/live_rollout/runtime_supervisor.py)|20|0|11|
|[src/crypto_momentum_lab/live_rollout/scheduled_controller.py](../../src/crypto_momentum_lab/live_rollout/scheduled_controller.py)|33|0|19|
|[src/crypto_momentum_lab/live_rollout/scheduled_risk_window.py](../../src/crypto_momentum_lab/live_rollout/scheduled_risk_window.py)|3|0|1|
|[src/crypto_momentum_lab/live_rollout/signal_recorder.py](../../src/crypto_momentum_lab/live_rollout/signal_recorder.py)|19|3|9|
|[src/crypto_momentum_lab/live_rollout/startup_market_buffer.py](../../src/crypto_momentum_lab/live_rollout/startup_market_buffer.py)|5|0|2|
|[src/crypto_momentum_lab/live_rollout/startup_recovery.py](../../src/crypto_momentum_lab/live_rollout/startup_recovery.py)|20|2|1|
|[src/crypto_momentum_lab/live_rollout/startup_resilience.py](../../src/crypto_momentum_lab/live_rollout/startup_resilience.py)|5|1|2|
|[src/crypto_momentum_lab/live_rollout/stream_recovery.py](../../src/crypto_momentum_lab/live_rollout/stream_recovery.py)|2|0|2|
|[src/crypto_momentum_lab/live_rollout/submission.py](../../src/crypto_momentum_lab/live_rollout/submission.py)|35|0|2|
|[src/crypto_momentum_lab/live_rollout/telemetry.py](../../src/crypto_momentum_lab/live_rollout/telemetry.py)|54|2|8|
|[src/crypto_momentum_lab/live_rollout/volume.py](../../src/crypto_momentum_lab/live_rollout/volume.py)|5|0|2|
|[src/crypto_momentum_lab/market_data/agg_trade_recovery.py](../../src/crypto_momentum_lab/market_data/agg_trade_recovery.py)|17|0|7|
|[src/crypto_momentum_lab/market_data/aggregation/state_15s.py](../../src/crypto_momentum_lab/market_data/aggregation/state_15s.py)|13|0|0|
|[src/crypto_momentum_lab/market_data/backfill.py](../../src/crypto_momentum_lab/market_data/backfill.py)|4|0|1|
|[src/crypto_momentum_lab/market_data/binance/connection_pool.py](../../src/crypto_momentum_lab/market_data/binance/connection_pool.py)|1|0|0|
|[src/crypto_momentum_lab/market_data/binance/rest.py](../../src/crypto_momentum_lab/market_data/binance/rest.py)|8|0|3|
|[src/crypto_momentum_lab/market_data/binance/websocket.py](../../src/crypto_momentum_lab/market_data/binance/websocket.py)|34|1|13|
|[src/crypto_momentum_lab/market_data/candle_source.py](../../src/crypto_momentum_lab/market_data/candle_source.py)|16|0|5|
|[src/crypto_momentum_lab/market_data/capture/coordinator.py](../../src/crypto_momentum_lab/market_data/capture/coordinator.py)|7|1|2|
|[src/crypto_momentum_lab/market_data/capture/queue.py](../../src/crypto_momentum_lab/market_data/capture/queue.py)|5|0|3|
|[src/crypto_momentum_lab/market_data/capture/service.py](../../src/crypto_momentum_lab/market_data/capture/service.py)|4|0|1|
|[src/crypto_momentum_lab/market_data/capture/subscriptions.py](../../src/crypto_momentum_lab/market_data/capture/subscriptions.py)|2|0|0|
|[src/crypto_momentum_lab/market_data/hub.py](../../src/crypto_momentum_lab/market_data/hub.py)|54|2|21|
|[src/crypto_momentum_lab/market_data/normalization/binance.py](../../src/crypto_momentum_lab/market_data/normalization/binance.py)|13|0|3|
|[src/crypto_momentum_lab/market_data/observability.py](../../src/crypto_momentum_lab/market_data/observability.py)|43|32|0|
|[src/crypto_momentum_lab/market_data/protocol_parsing.py](../../src/crypto_momentum_lab/market_data/protocol_parsing.py)|3|0|1|
|[src/crypto_momentum_lab/market_data/quality/tracker.py](../../src/crypto_momentum_lab/market_data/quality/tracker.py)|12|0|1|
|[src/crypto_momentum_lab/market_data/quote_hub.py](../../src/crypto_momentum_lab/market_data/quote_hub.py)|29|0|17|
|[src/crypto_momentum_lab/market_data/quote_volume.py](../../src/crypto_momentum_lab/market_data/quote_volume.py)|7|0|3|
|[src/crypto_momentum_lab/market_data/runtime_states.py](../../src/crypto_momentum_lab/market_data/runtime_states.py)|31|0|13|
|[src/crypto_momentum_lab/operator_dashboard/account_queries.py](../../src/crypto_momentum_lab/operator_dashboard/account_queries.py)|41|0|0|
|[src/crypto_momentum_lab/operator_dashboard/api.py](../../src/crypto_momentum_lab/operator_dashboard/api.py)|42|0|11|
|[src/crypto_momentum_lab/operator_dashboard/collector_status.py](../../src/crypto_momentum_lab/operator_dashboard/collector_status.py)|17|0|1|
|[src/crypto_momentum_lab/operator_dashboard/common_equity.py](../../src/crypto_momentum_lab/operator_dashboard/common_equity.py)|8|0|0|
|[src/crypto_momentum_lab/operator_dashboard/fact_integrity_queries.py](../../src/crypto_momentum_lab/operator_dashboard/fact_integrity_queries.py)|6|0|0|
|[src/crypto_momentum_lab/operator_dashboard/live_account_metrics_queries.py](../../src/crypto_momentum_lab/operator_dashboard/live_account_metrics_queries.py)|8|0|1|
|[src/crypto_momentum_lab/operator_dashboard/overview_queries.py](../../src/crypto_momentum_lab/operator_dashboard/overview_queries.py)|48|0|6|
|[src/crypto_momentum_lab/operator_dashboard/paper_account_queries.py](../../src/crypto_momentum_lab/operator_dashboard/paper_account_queries.py)|56|0|3|
|[src/crypto_momentum_lab/operator_dashboard/paper_equity_queries.py](../../src/crypto_momentum_lab/operator_dashboard/paper_equity_queries.py)|16|0|0|
|[src/crypto_momentum_lab/operator_dashboard/performance_builder.py](../../src/crypto_momentum_lab/operator_dashboard/performance_builder.py)|24|10|0|
|[src/crypto_momentum_lab/operator_dashboard/performance_queries.py](../../src/crypto_momentum_lab/operator_dashboard/performance_queries.py)|33|0|3|
|[src/crypto_momentum_lab/operator_dashboard/queries.py](../../src/crypto_momentum_lab/operator_dashboard/queries.py)|17|0|3|
|[src/crypto_momentum_lab/operator_dashboard/risk_execution_queries.py](../../src/crypto_momentum_lab/operator_dashboard/risk_execution_queries.py)|28|3|2|
|[src/crypto_momentum_lab/operator_dashboard/static/app/jump-probe.js](../../src/crypto_momentum_lab/operator_dashboard/static/app/jump-probe.js)|6|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/app/poller.js](../../src/crypto_momentum_lab/operator_dashboard/static/app/poller.js)|16|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/app/readiness.js](../../src/crypto_momentum_lab/operator_dashboard/static/app/readiness.js)|7|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/app/runtime-badge.js](../../src/crypto_momentum_lab/operator_dashboard/static/app/runtime-badge.js)|2|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/app/section-state.js](../../src/crypto_momentum_lab/operator_dashboard/static/app/section-state.js)|2|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/app/shell.js](../../src/crypto_momentum_lab/operator_dashboard/static/app/shell.js)|14|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/app/wire-widgets.js](../../src/crypto_momentum_lab/operator_dashboard/static/app/wire-widgets.js)|10|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/core/dom-reconcile.js](../../src/crypto_momentum_lab/operator_dashboard/static/core/dom-reconcile.js)|10|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/core/dom-update.js](../../src/crypto_momentum_lab/operator_dashboard/static/core/dom-update.js)|3|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/core/scroll-keep.js](../../src/crypto_momentum_lab/operator_dashboard/static/core/scroll-keep.js)|10|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/core/view-state.js](../../src/crypto_momentum_lab/operator_dashboard/static/core/view-state.js)|35|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/dashboard-chart-engine.js](../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-chart-engine.js)|55|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/dashboard-charts.js](../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-charts.js)|50|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/dashboard-formatters.js](../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-formatters.js)|10|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/dashboard-readiness.js](../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-readiness.js)|1|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/dashboard-rendering.js](../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-rendering.js)|3|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/dashboard-ui.js](../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-ui.js)|8|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/sections/account/constants.js](../../src/crypto_momentum_lab/operator_dashboard/static/sections/account/constants.js)|1|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/sections/account/index.js](../../src/crypto_momentum_lab/operator_dashboard/static/sections/account/index.js)|16|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/sections/account/loaders.js](../../src/crypto_momentum_lab/operator_dashboard/static/sections/account/loaders.js)|17|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/sections/account/render-detail.js](../../src/crypto_momentum_lab/operator_dashboard/static/sections/account/render-detail.js)|37|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/sections/account/render-fleet.js](../../src/crypto_momentum_lab/operator_dashboard/static/sections/account/render-fleet.js)|23|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/sections/account/render-metrics.js](../../src/crypto_momentum_lab/operator_dashboard/static/sections/account/render-metrics.js)|2|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/sections/account/signals.js](../../src/crypto_momentum_lab/operator_dashboard/static/sections/account/signals.js)|11|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/sections/account/state.js](../../src/crypto_momentum_lab/operator_dashboard/static/sections/account/state.js)|4|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/sections/collector.js](../../src/crypto_momentum_lab/operator_dashboard/static/sections/collector.js)|12|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/sections/overview.js](../../src/crypto_momentum_lab/operator_dashboard/static/sections/overview.js)|10|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/sections/performance.js](../../src/crypto_momentum_lab/operator_dashboard/static/sections/performance.js)|25|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/sections/reports.js](../../src/crypto_momentum_lab/operator_dashboard/static/sections/reports.js)|3|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/sections/risk.js](../../src/crypto_momentum_lab/operator_dashboard/static/sections/risk.js)|11|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/sections/strategy.js](../../src/crypto_momentum_lab/operator_dashboard/static/sections/strategy.js)|37|0|0|
|[src/crypto_momentum_lab/operator_dashboard/static/sections/universe.js](../../src/crypto_momentum_lab/operator_dashboard/static/sections/universe.js)|9|0|0|
|[src/crypto_momentum_lab/operator_dashboard/telemetry_queries.py](../../src/crypto_momentum_lab/operator_dashboard/telemetry_queries.py)|12|0|0|
|[src/crypto_momentum_lab/persistence/parquet/datasets.py](../../src/crypto_momentum_lab/persistence/parquet/datasets.py)|1|0|0|
|[src/crypto_momentum_lab/persistence/postgres/account_journal_store.py](../../src/crypto_momentum_lab/persistence/postgres/account_journal_store.py)|65|0|1|
|[src/crypto_momentum_lab/persistence/postgres/account_repository.py](../../src/crypto_momentum_lab/persistence/postgres/account_repository.py)|15|0|0|
|[src/crypto_momentum_lab/persistence/postgres/capture_repository.py](../../src/crypto_momentum_lab/persistence/postgres/capture_repository.py)|8|1|1|
|[src/crypto_momentum_lab/persistence/postgres/command_repository.py](../../src/crypto_momentum_lab/persistence/postgres/command_repository.py)|29|3|1|
|[src/crypto_momentum_lab/persistence/postgres/decision_trace_repository.py](../../src/crypto_momentum_lab/persistence/postgres/decision_trace_repository.py)|12|1|1|
|[src/crypto_momentum_lab/persistence/postgres/decision_unit_of_work.py](../../src/crypto_momentum_lab/persistence/postgres/decision_unit_of_work.py)|40|0|0|
|[src/crypto_momentum_lab/persistence/postgres/execution_unit_of_work.py](../../src/crypto_momentum_lab/persistence/postgres/execution_unit_of_work.py)|24|0|0|
|[src/crypto_momentum_lab/persistence/postgres/fill_recovery_sources.py](../../src/crypto_momentum_lab/persistence/postgres/fill_recovery_sources.py)|4|0|0|
|[src/crypto_momentum_lab/persistence/postgres/live_rollout_repository.py](../../src/crypto_momentum_lab/persistence/postgres/live_rollout_repository.py)|6|1|0|
|[src/crypto_momentum_lab/persistence/postgres/market_book_repository.py](../../src/crypto_momentum_lab/persistence/postgres/market_book_repository.py)|16|1|0|
|[src/crypto_momentum_lab/persistence/postgres/operational_retention.py](../../src/crypto_momentum_lab/persistence/postgres/operational_retention.py)|9|0|3|
|[src/crypto_momentum_lab/persistence/postgres/order_adoption_repository.py](../../src/crypto_momentum_lab/persistence/postgres/order_adoption_repository.py)|2|0|0|
|[src/crypto_momentum_lab/persistence/postgres/order_event_repository.py](../../src/crypto_momentum_lab/persistence/postgres/order_event_repository.py)|6|0|2|
|[src/crypto_momentum_lab/persistence/postgres/order_identity_repository.py](../../src/crypto_momentum_lab/persistence/postgres/order_identity_repository.py)|2|1|0|
|[src/crypto_momentum_lab/persistence/postgres/order_read_repository.py](../../src/crypto_momentum_lab/persistence/postgres/order_read_repository.py)|15|0|2|
|[src/crypto_momentum_lab/persistence/postgres/order_submission_repository.py](../../src/crypto_momentum_lab/persistence/postgres/order_submission_repository.py)|17|4|0|
|[src/crypto_momentum_lab/persistence/postgres/paper_daemon_repository.py](../../src/crypto_momentum_lab/persistence/postgres/paper_daemon_repository.py)|29|6|3|
|[src/crypto_momentum_lab/persistence/postgres/position_order_window.py](../../src/crypto_momentum_lab/persistence/postgres/position_order_window.py)|20|6|0|
|[src/crypto_momentum_lab/persistence/postgres/position_repair.py](../../src/crypto_momentum_lab/persistence/postgres/position_repair.py)|1|0|0|
|[src/crypto_momentum_lab/persistence/postgres/position_reservation_repository.py](../../src/crypto_momentum_lab/persistence/postgres/position_reservation_repository.py)|46|3|4|
|[src/crypto_momentum_lab/persistence/postgres/repository.py](../../src/crypto_momentum_lab/persistence/postgres/repository.py)|8|0|0|
|[src/crypto_momentum_lab/persistence/postgres/retention_repository.py](../../src/crypto_momentum_lab/persistence/postgres/retention_repository.py)|7|0|1|
|[src/crypto_momentum_lab/persistence/postgres/risk_repository.py](../../src/crypto_momentum_lab/persistence/postgres/risk_repository.py)|4|0|1|
|[src/crypto_momentum_lab/persistence/postgres/runtime_context.py](../../src/crypto_momentum_lab/persistence/postgres/runtime_context.py)|4|0|0|
|[src/crypto_momentum_lab/persistence/postgres/runtime_state_partitions.py](../../src/crypto_momentum_lab/persistence/postgres/runtime_state_partitions.py)|23|0|1|
|[src/crypto_momentum_lab/persistence/postgres/runtime_state_repository.py](../../src/crypto_momentum_lab/persistence/postgres/runtime_state_repository.py)|9|0|0|
|[src/crypto_momentum_lab/persistence/postgres/runtime_telemetry_repository.py](../../src/crypto_momentum_lab/persistence/postgres/runtime_telemetry_repository.py)|4|0|0|
|[src/crypto_momentum_lab/persistence/postgres/serialization.py](../../src/crypto_momentum_lab/persistence/postgres/serialization.py)|2|0|0|
|[src/crypto_momentum_lab/persistence/postgres/session.py](../../src/crypto_momentum_lab/persistence/postgres/session.py)|5|0|2|
|[src/crypto_momentum_lab/persistence/postgres/strategy_run_repository.py](../../src/crypto_momentum_lab/persistence/postgres/strategy_run_repository.py)|8|0|0|
|[src/crypto_momentum_lab/persistence/postgres/strategy_runtime_event_partitions.py](../../src/crypto_momentum_lab/persistence/postgres/strategy_runtime_event_partitions.py)|23|0|1|
|[src/crypto_momentum_lab/persistence/postgres/submission_identity.py](../../src/crypto_momentum_lab/persistence/postgres/submission_identity.py)|1|0|0|
|[src/crypto_momentum_lab/persistence/raw_files/archive.py](../../src/crypto_momentum_lab/persistence/raw_files/archive.py)|17|0|11|
|[src/crypto_momentum_lab/persistence/raw_files/journal.py](../../src/crypto_momentum_lab/persistence/raw_files/journal.py)|6|0|3|
|[src/crypto_momentum_lab/persistence/raw_files/reader.py](../../src/crypto_momentum_lab/persistence/raw_files/reader.py)|4|0|2|
|[src/crypto_momentum_lab/persistence/raw_files/recovery.py](../../src/crypto_momentum_lab/persistence/raw_files/recovery.py)|7|0|6|
|[src/crypto_momentum_lab/persistence/raw_files/retention.py](../../src/crypto_momentum_lab/persistence/raw_files/retention.py)|4|0|2|
|[src/crypto_momentum_lab/research_collector/health.py](../../src/crypto_momentum_lab/research_collector/health.py)|3|0|1|
|[src/crypto_momentum_lab/research_collector/journal.py](../../src/crypto_momentum_lab/research_collector/journal.py)|33|0|14|
|[src/crypto_momentum_lab/research_collector/materializer.py](../../src/crypto_momentum_lab/research_collector/materializer.py)|7|0|0|
|[src/crypto_momentum_lab/research_collector/models.py](../../src/crypto_momentum_lab/research_collector/models.py)|3|0|0|
|[src/crypto_momentum_lab/research_collector/selection.py](../../src/crypto_momentum_lab/research_collector/selection.py)|3|0|0|
|[src/crypto_momentum_lab/research_collector/service.py](../../src/crypto_momentum_lab/research_collector/service.py)|71|10|23|
|[src/crypto_momentum_lab/research_collector/source.py](../../src/crypto_momentum_lab/research_collector/source.py)|2|0|0|
|[src/crypto_momentum_lab/research_collector/storage.py](../../src/crypto_momentum_lab/research_collector/storage.py)|41|1|10|
|[src/crypto_momentum_lab/risk/gateway.py](../../src/crypto_momentum_lab/risk/gateway.py)|10|0|0|
|[src/crypto_momentum_lab/strategies/order_flow_impulse/event_study.py](../../src/crypto_momentum_lab/strategies/order_flow_impulse/event_study.py)|17|0|0|
|[src/crypto_momentum_lab/strategies/order_flow_impulse/runtime.py](../../src/crypto_momentum_lab/strategies/order_flow_impulse/runtime.py)|2|0|0|
|[src/crypto_momentum_lab/strategies/registry.py](../../src/crypto_momentum_lab/strategies/registry.py)|4|0|2|
|[src/crypto_momentum_lab/strategies/runtime_checkpoint.py](../../src/crypto_momentum_lab/strategies/runtime_checkpoint.py)|2|0|0|
|[src/crypto_momentum_lab/strategies/runtime_state.py](../../src/crypto_momentum_lab/strategies/runtime_state.py)|5|0|1|
|[src/crypto_momentum_lab/tools/catalog_dataset.py](../../src/crypto_momentum_lab/tools/catalog_dataset.py)|2|0|1|
|[src/crypto_momentum_lab/tools/reproduce_decision.py](../../src/crypto_momentum_lab/tools/reproduce_decision.py)|1|0|0|
|[src/crypto_momentum_lab/universe/daily_open_prefetch.py](../../src/crypto_momentum_lab/universe/daily_open_prefetch.py)|7|0|4|
|[src/crypto_momentum_lab/universe/refresh.py](../../src/crypto_momentum_lab/universe/refresh.py)|4|0|0|
|[src/crypto_momentum_lab/universe/scheduler.py](../../src/crypto_momentum_lab/universe/scheduler.py)|2|0|2|

退出策略模式/覆盖状态重复转换删除后完整回归3134项通过、1项实盘采集缺环境跳过、2个既有警告。继续删除PositionKey和AccountFactStreamScope内部字符串持仓方向转换；23处直接生产构造及ExecutionScope来源核查，旧账本list_scopes读取将字符串解析移到持久化边界，其余外部恢复解码保持显式枚举。15处测试构造输入改为正式枚举；最终完整回归执行中。

仓位键/事实流内部转换删除后完整回归3134项通过、1跳过、2既有警告。另核查UniverseRankingEntry和PolicyInputSnapshot：生产构造由entry_policy_evaluation显式提供StrategySide，测试辅助函数亦使用正式类型，删除两处重复StrategySide转换；标的规范化、时效和参数约束保留。最终完整回归执行中。

入场方向重复转换删除后完整回归3134项通过、1跳过、2既有警告。EmaPolicyState全仓仅disabled/unavailable/valid三种正式工厂构造，均提供EmaPolicyStatus，删除构造器重复状态转换和局部中转变量；保留有效EMA所需数据与时效约束。

本轮新增清理7处内部枚举重复转换（退出模式、覆盖状态、仓位键、事实流方向、两项入场方向、EMA状态），外部解析留在读取边界。前6处完整回归3134通过、1跳过、2既有警告；最后EMA状态简化后相关策略/决策/实盘848项通过、1既有警告，F/I及diff检查通过。全系统其余兼容/兜底候选语义审核仍在继续，未提交、推送或部署。

EMA状态兼容删除后的最终完整回归3134通过、1跳过、2既有警告。证据分组返回类型仅Applied/Duplicate/WaitingForEvidence/EvidenceConflict，末尾last_result兜底被前置四种分支完全覆盖；删除该不可达补救分支和last_result维护，剩余Applied直接汇总实际数量。恢复来源推导仍涉及检查点与时间切片，尚不可按语法直接删除；入场池None也存在正常启动尚未加载状态，不仅是legacy。

证据分组不可达兜底删除后，执行领域446项通过；最终完整unit/smoke/e2e及Postgres集成3134通过（2962+172）、1项缺实盘采集环境跳过、2个既有警告，修改文件F/I与diff检查通过。候选表两项恢复需求布尔汇总已核查保留：它们汇总多笔实际成交结果，不是兼容分支。全系统语义审核尚未完成，继续按实际生产调用确认；未提交、推送或部署。

检查点调度收敛：MarketState15s.bucket_end为正式必填datetime，删除回退saved_at与ref_dt中转；CheckpointWriter全仓clock仅默认或显式lambda注入，无None调用，改为函数参数直接默认perf_counter，删除None双形状与or回退。首次尚无成功持久化时的monotonic基线仍有实际用途，不删除。检查点协调/写入23项通过（第二项变更后待最终复验）。

检查点两处兼容简化后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；修改文件F/I与diff检查通过，候选表对应表达式标记已删除。全仓时钟None默认/内部or回退仍有约16处，下一轮须核对装配透传参数后统一收敛，不把正常时钟注入和外部恢复功能误删。全系统语义审核继续，未提交、推送或部署。

普通时钟注入统一收敛：12个文件（仪表盘、入场订单/控制/缓存、闭合K线、行情成交量、日开盘预取、实盘daemon/装配、订单协调器/状态机、币安客户端）将clock/submission_clock直接默认UTC函数，删除内部or回退。全仓AST核查生产与测试调用，无显式None；装配execution_runtime及币安子类透传同步更新。测试辅助函数仍自行提供正式lambda，不影响时间注入。相关目录Ruff F/I及diff检查通过，完整回归执行中；StreamAvailabilityClock是有状态可用性监控对象，另行审核，不和时间函数混删。

普通时钟参数12文件统一后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；相关5个源代码目录Ruff F/I全通过，diff检查通过。全仓普通clock/submission_clock的None默认及内部or回退不再存在；候选表12个对应表达式标记已删除（另有2处只透传的参数已同步更新）。可用性监控对象和其可选注入属于独立状态契约，仍需按实际初始化时机核查。全系统语义审核尚未完成，未提交、推送或部署。

可用性监控参数收敛：StreamAvailabilityClock.mark_connected/mark_recovering/mark_disrupted的reason从未被保存或消费，删除3处空参数与全仓客户端/测试透传；账户及风控客户端仍记录恢复原因、发出实际回调和日志。删除2个仅为str(error)透传而绑定的异常变量，不改异常捕获范围。market-state延迟初始化保留：时钟构造即记录startup_since，本地恢复在迭代消费前，提前构造会错误计算本地恢复为网络超时。修改文件F/I及diff检查通过，完整回归执行中。

可用性状态3个空reason参数删除后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；修改文件F/I与diff检查通过。异常工厂和自定义超时消息有5处真实客户端调用，不按空参数删除；剩余超时预算计算有重复路径，需进一步按状态计时连续性收敛。全系统语义审核尚未完成，未提交、推送或部署。

可用性超时计算拍平：check_timeout直接使用remaining_budget，不再重复3份elapsed/阈值比较、2份时间戳回退及3份异常工厂分支；保留3种原消息内容和客户端异常类型，统一一次构造并抛出。启动抖动按启动预算、恢复跨断连保留恢复开始时间、就绪无限预算等实际契约保持。健康/行情/账户721通过、4项本地网络开关跳过；最终全量回归开启Hub网络测试执行中，修改文件F/I与diff检查通过。

可用性超时单一预算计算后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；F/I和diff检查通过。候选表5项重复时间戳/消息选择已标记删除或合并，预算入口剩余时间戳状态需进一步核查。全系统语义审核仍未完成，未提交、推送或部署。

Hub配置注入收敛：四种冻结配置仅含标量/不可变元组，11个Hub/客户端/发布者构造入口直接默认正式配置对象，删除config=None和内部or创建；全仓AST调用仅省略配置或传显式构造，无None/变量透传。行情、报价、账户、风控模块同步修改，保留分项超时配置继承及可用性启动时机。F/I通过，最终完整回归执行中。

Hub配置11入口简化后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；修改文件F/I与diff检查通过，候选表11项对应补配置表达式标记已删除。运行状态发布器配置和队列参数仍有同类默认适配，下一轮继续核查；账户同步_save_state的config默认关联当前实例，不能机械改成全局默认。全系统语义审核尚未完成，未提交、推送或部署。

行情发布配置收敛：ClosedMarketStatePublisherConfig手写__init__仅重复字段赋值，旧单延迟兼容注释已失效，删除该初始化器并用标准冻结kw_only dataclass，默认0.4/3.0/128/1.0与全部校验保持。发布器直接默认正式不可变配置，删除None补配置；WebSocketMarketStateSource接收队列直接默认整数，删除None回退和冗余条件，保留正值约束。生产collector参数为int默认128，全仓调用未传None；修改文件F/I通过，完整回归执行中。

行情发布配置和队列适配删除后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；F/I及diff检查通过。全仓类级dataclass init=False手写初始化不再存在（字段缓存/异步事件init=False仍有真实用途）；入场缓存尚有2处配置None回退，继续核查。全系统语义审核尚未完成，未提交、推送或部署。

入场缓存输入单向化：生产entry_runtime两处缓存构造均使用universe_loader，symbol_loader仅7个旧测试构造（含运行监督器）。LiveEntryFilterCache/LiveEntrySymbolCache统一必填UniverseLoader，删除SymbolLoader类型、2组互斥检查、私有字段和2份备用加载分支；测试加载器直接返回LiveEntryUniverseData，保留后台预热/无IO读取/取消处理行为。删除两项不可能None的写入条件，配置直接默认冻结EntryFilterCacheConfig，删除2处补配置。入场缓存/装配12项通过，F/I和diff通过；全量回归执行中。

入场缓存统一UniverseLoader后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；首次全量发现监督器旧symbol_loader夹具，已同步当前契约并完整复验。全仓AST10处缓存构造（2生产+8测试）均提供正式universe_loader，SymbolLoader备用路径不再存在。修改文件F/I及diff检查通过；全系统兼容/兜底语义审核尚未完成，未提交、推送或部署。

账户事件身份收敛：生产两处LiveAccountEventRuntime已显式传run_id，改为必填字符串，删除从order_reconciliation.run_id推导及unknown回退的私有属性；事件通道直接使用装配身份，测试遗漏输入补为明确run-1，不增加退出/恢复准入检查。修改文件F/I通过，完整回归执行中。

账户事件run_id显式化后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；F/I通过，测试新增参数换行后diff检查复验。此改动删除对账器身份反向读取与unknown猜测，不改变账户事实处理和退出重试流程。全系统语义审核尚未完成，未提交、推送或部署。

信号记录枚举收敛：StrategySignal.side及OrderIntentCandidate.side/entry_type均为正式枚举，4处记录调用直接读取.value，删除_enum_value的任意对象str适配；扩展features/account/filter上下文JSON序列化仍按真实可序列化数据边界处理。信号记录2项通过，F/I通过；完整回归执行中。

信号记录正式枚举直接读取后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；F/I及diff通过。下一项身份异常分类仍按异常类型名称字符串识别，已找到正式ReservationConflictError/OrderPreSubmissionError契约，继续核对替换；全系统语义审核尚未完成，未提交、推送或部署。

订单身份异常分类收敛：ReservationConflictError与OrderPreSubmissionError按正式契约isinstance识别，删除类名字符串兼容；删除已被通用消息匹配覆盖的ValueError精确分支和专用常量，5条共享消息合并为一份；异常cause不再重复None判断。运行通道预留冲突策略与持久化退出策略保持分离；旧测试本地同名异常改为正式领域异常，38项分类/退出测试通过。F/I及diff通过，完整回归执行中。

订单身份异常按正式类型识别后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；F/I与diff检查通过。src不再存在以type(...).__name__字符串作异常类型判断的匹配表达式；持久化身份消息仍由当前明确异常生产者使用，后续须核查能否类型化，不能直接删除冲突识别。全系统语义审核尚未完成，未提交、推送或部署。

身份消息来源复核：5项共享消息中already exists in terminal status全仓生产无产生位置，仅2项旧测试使用；删除该旧消息兼容，退出分类测试改为当前non-dispatchable state，预留异常测试改为正式仓位预留身份冲突消息。另核查runtime_state_partitions准备/切换工具有market-data CLI实际注册和调用，且新建历史schema仍为普通表，不按无调用工具删除。修改文件F/I通过，完整回归执行中。

旧终态文本兼容删除后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；F/I与diff通过。剩余4条身份消息均找到明确生产产生位置；本轮确认CLI分区转换为有效运维路径，不作无调用删除。全系统语义审核尚未完成，未提交、推送或部署。

数据库数值去双重默认：count_quality_events的COUNT、分区准备/切换3个COUNT及paper组合统计5个已有SQL COALESCE聚合，删除9处Python or 0；空集仍由数据库返回0，不再将异常缺值默认为正常0。外部驱动rowcount与可空历史字段需另行按契约核查，不混删。修改文件F/I通过，完整回归执行中。

数据库9处重复补零删除后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；F/I及diff通过，候选表9项更新。账户journal MAX空集返回None的0游标有真实语义，保留；订单读取executed_quantity ORM已为非空，下一轮须核对迁移及测试构造再删除对应默认。全系统语义审核尚未完成，未提交、推送或部署。

订单已成交数量去缺值猜测：exchange_orders.executed_quantity的0026迁移为nullable=False/server_default=0，当前ORM同为非空Decimal；删除order_read_repository和position_order_window两处or Decimal(0)，读取正式字段。FILLED但迁移旧行数量0的历史归属处理仍另行核查，不与非空保证混为一谈。F/I通过，完整回归执行中。

订单读取非空数量直接使用后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；首次全量识别旧None补零用例，改为正式0与0.5成交数量保存验证并完整复验。F/I及diff通过，候选表2项更新。历史零成交状态修复和其他外部可空字段仍按实际语义核查，全系统审核尚未完成，未提交、推送或部署。

订单归属时间检查收敛：0008迁移与ExchangeOrderRow创建/更新时间均非空，删除_is_pre_zero_order中两个时间字段None放行分支、局部中转和bool包装，直接比较正式时间与终态。zero_at缺失仍表示尚无清零边界，不作为同类删除。F/I及diff通过，完整回归执行中。

订单时间缺值适配删除后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；修改文件F/I与diff通过。仓位清零键仍有symbol/position_side两种形状，后续按生产调用核查是否只需单一正式形状；全系统语义审核尚未完成，未提交、推送或部署。

仓位清零键统一：生产构造和3项有清零时间测试均为(symbol, position_side)，删除_lookup_zero_at对symbol单键的旧读取、Any键型、None映射及方向BOTH回退。正式查询函数必填Mapping[tuple[str,str],datetime]，无清零测试明确传空映射；3处消费者直接按规范化双键get。数据库仓位/订单方向均非空，保留方向规范化和无清零边界的正常语义，F/I通过，完整回归执行中。

仓位清零边界双键统一后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；F/I及diff通过，旧查询适配/Any映射/BOTH猜测不再存在。按方向清零过滤、无边界空映射与退出窗口行为已由现有回归覆盖；全系统剩余候选语义审核尚未完成，未提交、推送或部署。

候选清单现态核销：对待判定Python表达式按AST与当前文件核对，89条原表达式已不存在，标为静态核销，不宣称所在文件语义评完；待判定从5927降至5838。配置模块5项逐项读源码确认外部环境注入、读/交易角色分离、缺失凭据校验及URL显式优先级，标记保留，余5833。凭据单调用方_raise_missing空包装内联，删除未使用reference参数和NoReturn导入，错误内容仍仅环境名称/不含密钥。配置22项通过、F/I及diff检查通过；本轮未重复全量回归，全系统其余候选审核继续，未提交推送部署。

Universe候选12项语义复核：删除NoMonitoringObligations/NoUniverseSnapshotObserver两类和默认对象包装，恒True activated与对应重复分支拍平，真实监控标的/快照通知保持；首次完整回归3134通过、1跳过、2既有警告。再删除预取与调度3处纯CancelledError重抛（>=3.12不被Exception捕获），无清理动作；相关unit及真实刷新/黄金链路e2e20通过，F/I及diff通过。12条候选中5条删除、7条按跨日/预取重试/外部缺数据/时间校验保留；全系统其余候选审核继续，未提交推送部署。

纯取消重抛批量清理：按AST验证handler仅裸raise、其余handler均为Exception/TimeoutError/ValueError/OSError且至少一项有效handler，32文件删除67处空CancelledError分支。动态异常类型/其他tuple/有清理归因动作不混删，finally原封保留。删除2项因此无用的asyncio导入；修改文件F/I通过，全src另3项此前I排序问题未在本轮修复，不声称全仓lint绿。候选仅在文件已无CancelledError时更新，避免同文件保留分支被误标；完整回归执行中。

67处空取消分支删除后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告。随后仅整理此前3个文件导入排序，全src Ruff F/I通过、diff通过，不等于全规则或mypy绿。候选表43项明确删除，其余同文件仍有实际取消处理者按原分支继续核查。真正的发包取消归因/清理动作与finally仍保留；全系统其余候选审核继续，未提交推送部署。

补充：决策退出命令恢复仅接受正式完整 payload，保留显式 None；采集恢复器统一 bypass_network 关键字契约，移除捕获内部 TypeError 后按旧签名重复调用的兼容路径。

补充：删除无生产调用的 ContinuousAccountSyncDaemon 配置、结果、旧测试及不生效的账户同步轮询 CLI/Compose 参数；保留 UserData 实时同步与后台历史成交补查。退出恢复删除反查前未使用的上下文读取，只在反查后获取当前事实。

补充：删除 prepare 未使用的 migration_revision CLI 选项；检查点信号序号取消缺失/非法/负数归零，正式整数恢复与首次运行初始化保留。坏检查点拒绝发生在运行时状态修改之前。相关候选状态已回填；当前完整回归 3139 passed、1 skipped。

补充：运行清单的未知策略不再恢复为 unset 哈希。两个策略运行协议合并为领域 RuntimeStrategy，移除重复定义及旧名称，工厂与实盘直接依赖领域契约。完整回归 3140 passed、1 skipped。
