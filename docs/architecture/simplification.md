# 架构简化状态与剩余工作

核对日期：2026-10-04。基线 `3c652c4d` 加本次本地修改；本地完成不等于部署验收。

## 已实施

- Client 回调/异常下沉到执行契约；候选公共工具不再属于 Lane 私有实现。
- RiskGateway 组合 FixedLiveLimits，纯函数 plan_order_execution 代替薄执行外壳；删除无意义装配包装。
- 删除 CapabilityEvaluator、影子运行及运行循环中的 Git/migration 静态证明；对账改为后台任务，局部问题不形成全局退出门禁。
- 准备数据在同数据库事务内落库；订单主表提供交易所状态，发包未知继续保留身份反查。
- 同账户/symbol/position_side 串行，退出优先；取消排队任务不再继续发包。
- 退出薄入口合并为 LiveExitProcessor.handle_trigger；后台订单/退出/仓位恢复已有一个 run_requested 调度所有者。
- 监控、看板不再以租约和 commit 相等证明当前可运行；产品报告删除影子分支，历史表保留。
- 活动命令和订单用一次连接查询，减少重复读取；单仓位提交只推进参与的 journal。
- PrefetchedContext 和共享遥测类型归位，解除 context/prefetch、telemetry/ports 的回向类型引用。
- 单元测试按公开行为改写，删除源码拼写、对象身份、固定内部调用顺序和缓存布局约束，安全回归保留。

- Submission 合并 execute_entry/execute；生命周期锁改为 hold 上下文，删除退出触发与恢复应用的回调壳，保持整段操作互斥。
- StateMachine 删除 serialize_commands 和 submit/reconcile/cancel 三个可选锁包装；命令串行统一归 Coordinator。
- Coordinator 的嵌套 partial 与提交回调改为直接执行；入场暂停及活动计数用一个上下文管理器维护。
- OrderEventRuntime 直接观察 PendingEntryRegistry，不再持有整个 Daemon；Coordinator 通过 Book 的恢复接口维护责任，不写其私有集合。
- 缓存提供者维护唯一失效版本，预取直接读取它；账户事件直接失效也能触发预取重载。
- AccountFacts 和 CanonicalFactCache 归入 recovery_models，基础仓位模型的反向类型引用已删除；349 个源码模块静态扫描无模块环。

- 三个独立仓储协议文件并入 execution/ports，保留契约，删除碎片化文件与旧导入。
- WebSocketQuoteVolumeProvider 直接维护因果历史，删除无人使用的 REST 24h 缓存和内部透传对象。
- 删除未接线的 SourceIngress/终止原因追踪分支；保留真实行情、候选和订单遥测。
- RuntimeSession 正常状态统一为 RUNNING；保留实际关机阶段，删除无消费者的恢复/就绪切换。
- 多批次退出预留一次批量持久化，成功后再统一发布内存；消费与释放复用预留值对象。
- Readiness 删除未接线的流状态/退出状态更新入口及必填 commit/migration 字段；保留预热、开仓控制与行情推进报告。
- 删除无消费者的检查点调度、旧成对运行入口、同步保留仓储、盲目预留 TTL 释放、批量检查点入口及其他闲置辅助函数；历史表和实际恢复职责保留。

- 删除 RuntimePlan/RuntimePlanCompiler 及无人消费的多组哈希、部署代次、来源链和密钥引用；直接构建 EffectivePolicy，保留真实决策使用的 policy_id。
- 删除 Paper 报告中未使用的 runtime_plan 字段和 RuntimePlanMetadata 协议；历史报告持久化结构不变。
- 执行装配直接接收四个真实回调，删除 LiveExecutionCallbacks 包装。
- 实盘 run 配置与 CLI 移除 lease_owner、migration_revision，以及租约匹配检查；四账户 Compose 参数同步；清单元数据不再必填，部署清单 commit 环境引用提供 unknown 默认。运行状态发布的定时器改为 status heartbeat 命名，删除闲置租约常量。
- 删除未使用的流状态计时接口和其影子时间戳，拍平连接状态切换；真实断流/重连预算保留。

- 订单恢复函数直接返回未完成标记与待查询订单；恢复器读取账本后统一去重、预算和轮转，删除装配回调和逐订单异步收集回调。
- 订单协调器只从 ExecutionBook 获取领域协调器；删除第二份引用、恒真 is_execution_book_enabled 开关及账本不存在的不可达分支。真实持久化、取消和成交结算分支保留。

- LiveContextRuntime 只接受完整 LiveContextReader；删除纯加载函数兼容、第二套代次和默认 60 秒判断。生产提供者负责唯一失效版本和事实时效，测试加载器使用完整读者替身。
- 客户端必须实现杠杆/保证金预热查询；删除缺接口即视为预热完成的默认放行。
- LiveExitConfig 明确要求账户与首根逆向 K 线决策利润阈值；Daemon 不再写退出管理器私有配置，决策阈值不再借用挂单利润阈值。
- 交易所明确空仓时不再用旧本地状态重建定时退出，不等待行情；删除锁定旧等待逻辑的两个测试。
- 定时撤单必须等待真实开仓任务结束，取消可选等待接口和 Daemon 的方法探测；未实现接口不能静默略过。

- 仓位重建明确接收 environment/account_label，由数据库读取方传入；删除缺账户默认 live/primary，输出 ManagedLivePosition 保留真实账户。快照无观察时间时不生成基于猜测墙钟的覆盖证明。
- 订单观察转换直接读取完整 OrderObservation 字段；删除缺字段借用计划身份/数量/时间的兼容路径。真实持久化状态和累计成交合并仍保留。
- 恢复器直接调用契约中的 observe_recovered_receipt，不再因方法探测失败切换处理路径。

本次评审列出的入口包装、调度回调、Daemon 反馈引用、上下文重复代次与领域类型环已完成处理。订单事实与结算责任保持单向，彻底删除派发/结算状态不属于本轮可直接删除的空心层。上述列表也汇总此前累计工作。当前模块关系见[系统架构](overview.md)。

## 尚未完成

|优先级|工作|完成条件|
|---|---|---|
|P1|核对真实宽限截止和退出耗时|明确首次边界、8 根截止、撤单与剩余市价结果；区分计划生成、POST、成交，不用每日定时平仓代替|
|P2|收窄命令与订单重复状态|逐消费者迁移；终态但缺成交结算、UNKNOWN 和真实预留责任仍可恢复，不能直接删除 DispatchState|
|P2|最小活动恢复与后台历史补齐分离|恢复活动订单/预留/批次后可工作；仍发现停机期间已开已平标的，不丢成交水位|
|P2|全局索引增量发布|去除不必要全局复制，同时提交失败不污染内存；已减少 journal 遍历不代表该项完成|
|P3|观测开销和性能验收|减少重复诊断查询，四账户同口径测量正常/恢复阶段、DB 等待、CPU 峰值和端到端样本|
|P3|研究归档载荷冲突|定位历史 Parquet 冲突来源与覆盖，保留原事实，不覆盖旧记录来消除告警|

## 验证口径

清理前 Git 跟踪 Python 共 257619 行，其中 tests 98116、scripts 34536；它不是交易热路径体积。行数减少不能证明延迟下降。

最近完整回归见[测试说明](../testing/behavior-tests.md)。当前工作区未部署，尚无此次本地简化的性能 A/B。历史事故和成功/失败窗口见[故障记录](../diagnostics/incidents.md)，不得将短窗口健康、无订单样本或单函数基准写成整条交易性能通过。

### 上下文读取器与字段契约收敛

- PostgresLiveContextProvider 直接使用构造时初始化的缓存、快照、规则加载任务和锁，删除 38 处字段存在性探测及两个锁获取薄封装；漂移扫描状态也统一在构造时初始化；执行账本修订号直接按 PositionContextBook 接口读取。
- 测试和诊断脚本通过正常构造函数创建读取器，删除绕过初始化的对象构造。
- 引用行情时间固定取 MarketState15s.bucket_end；候选并发统计、量化、仓位分类与订单事实转换直接读取模型字段。
- TradeCommand 不定义策略名称或版本，计划编译不再探测这两个不存在的字段。取消检查直接使用当前 Python 版本提供的 Task.cancelling()。
- 保留实际历史订单归属恢复、交易所未知结果查询、成交价格证据合并及共享规则任务的取消隔离；这些处理对应真实的数据和网络状态。

### 退出数量与恢复依赖收敛

- 删除 LiveExitManager 的 _allocate_exit_quantity：它曾构造虚拟 PositionKey、批次、episode、projection 和临时 TradeCommand，却只返回数量。退出请求直接限制为当前持仓及目标批次容量，真实分配与预留仍由执行账本完成。
- 先确定最终数量再生成候选身份，避免不同请求数量截断为同一最终数量后仍产生不同身份；数量与候选 metadata 保持一致。
- 退出重建及账本视图转换直接读取完整模型，删除缺少仓位字段、枚举字段或订单 plan 时的结构兼容。
- LiveOrderReconciliation 使用已显式注入的 execution_book，删除向状态机动态寻找属性或调用访问器的分支。
- ExitAllocator 明确接受 PositionView 与 PositionLedgerProjection 两种实际模型，不再使用 Any 与属性探测推断形状或默认数量零值；批次字段按正式模型读取。

### 删除无调用方校验与关闭接口兼容

- 删除 validate_exit_allocation_plan：仓库内没有生产调用方，只有专属测试。随之删除该测试；模型的分配总量约束和执行账本真实容量检查继续保留。
- PositionBook 直接读取 AccountJournal 的 latest_event_at、revision 与 facts_generation。日志在覆盖区间更新及恢复时已将区间末端纳入最新事件时间，无需再读取私有 _coverage 重复计算。
- 成交覆盖证据直接读取正式 proof 的 page_exhausted 和 not_truncated，不再默认缺失字段为 False。
- 运行时单笔金额只读取 config.execution.target_notional，删除旧策略字段 target_notional_usdt 的后备来源；订单与分配元数据直接读取正式模型字段。
- 资源关闭只使用 close() 返回的失败元组，删除属性探测、多返回形状兼容及重复 close_failures 属性。测试适配器同步遵守这个返回契约。

### 决策事实与轨迹字段收敛

- LiveDecisionFactSource 直接读取账户资产、风险配置哈希、仓位总量和提交回执的 is_replay；删除按配置时间生成后备身份、缺总量视为可退出、缺回放标记视为新决策等结构兼容。缺少真实仓位视图仍可延后恢复，零仓位仍不重复发单。
- 决策轨迹按 PositionLedgerBatch 正式字段序列化：数量统一来自 quantity，symbol/side 来自仓位 key，时间来自正式 datetime 字段；保留现有持久化轨迹字段名称。
- 异步决策过滤器明确要求提交结果提供 is_replay，使用最小只读接口，避免决策引擎反向依赖包含 PolicyState 的持久化提交模型。
- 策略装配直接调用 RuntimeStrategyProtocol.required_data，行情恢复间隔直接读取已验证的 StrategyDataRequirement，不再检查方法存在性或补 15 秒默认值。
- 账户订单期望直接使用正式 position_side 枚举；测试替身补齐回执、风险哈希和仓位总量字段。

### 留存装配、订单计数与状态接口去兼容

- 留存任务显式注入 RetentionAuthority；生产装配指定 Postgres 实现，测试显式选择内存实现。删除识别 Mock 后自动切换实现的工厂，配置留存但缺会话工厂时直接报告装配错误。
- 删除 legacy_checkpoint 的失效读取：当前没有写入方，引用的解码方法也不存在。保留现行 checkpoint 与实际历史订单恢复。
- 合成成交无法取得有效价格时不生成成交事件，删除 1.0 默认价；保留已有价格证据与合成标识。
- 批次并发统计直接读取正式仓位接口，调用方显式传入持久化订单的 plan，修复旧字段探测将真实在途订单漏计的问题；增加对应回归。
- PolicyState 直接执行退出、持仓期限、锚点和量化状态更新，删除方法缺失时静默不更新或改走旧方法的分支。
- 清理前候选清单保留为基线快照，并标注本轮处理状态；普通缺省值和真实网络恢复不等同于过时兼容。

### 采集、观察结果与决策哈希继续去兼容

- 采集协调器直接等待正式 save_quality_events 接口，删除逐条降级和同步/异步结果探测；测试仓库同步实现批量接口。
- 订单协调器按 Applied 结果类型读取 recovery_required 与 diagnostics；Duplicate 等其他结果不再通过默认属性猜测。
- 运行时元数据直接调用已装配遥测对象的 record，保留写入异常不影响交易的处理。
- 决策输入哈希直接读取 PositionLedgerBatch.quantity、身份、价格、时间与正式枚举；输入 scope、closed_candles 和 market_envelope 按 DecisionInput 契约读取。测试使用真实模型验证零数量差异，不再添加不存在的批次字段。
- DatasetCatalog 直接调用正式批量 canonical refs 与 manifest 列表接口，删除单点查询替代及接口缺失返回空列表的分支。

- 账户流连续性 token 与事件记账纳入正式接口；收事件和发心跳直接读取真实 token，删除缺字段默认连通及缺记账方法直接跳过。重连直接等待正式异步方法，不再探测或兼容同步结果；真实断线与记账失败恢复保留。
- 账户心跳状态统一为 AccountUserDataState，删除裸 AccountSnapshot 分支与 snapshot 方法探测；测试也通过正式状态对象构造。

### 行情观测与账户同步契约收敛

- 行情观测直接读取 CaptureMetricsSnapshot、BinanceConnectionPoolMetricsSnapshot 和连接快照的正式字段，删除 30 处字段默认；恢复指标明确为 AggTradeRecoveryMetrics，仅保留未装配恢复采样器时的 None 分支。测试构造真实快照。
- 账户同步服务明确实时同步与后台持久化两个正式方法，删除能力探测与同步落库替代路径；已有状态使用实时同步，初次启动仍完成持久化同步。后台持久化直接调用服务，不再接受方法回调参数。
- 仪表盘性能构建器明确现金流与权益行接口，字段按正式模型读取，删除缺字段时默认 unknown/None；真实空值、证据内容校验、权益窗口缺口仍显式处理。
- 策略转换直接读取现行 EffectivePolicy、DecisionInput 和 DecisionFrame 的 23 个字段，删除后备策略参数与版本默认；规则解析只接受正式 SymbolLotRules，删除属性齐全便转换的猜测与异常吞掉分支。真实对象、映射及回调来源暂保留，均有当前调用方。
- 删除运行时从 PostgresLiveRolloutRepository 私有 _cached_rules 查规则的后备分支：该仓库根本没有这个缓存；策略规则只读取明确加载的规则表。

### 持久化窗口与回放输出去形状兼容

- 覆盖证据按正式成交游标与对账行读取；订单窗口按 ExchangeOrderRow 读取状态、时间、position_side，删除模型缺字段时的默认及 updated_at 回退。真实缺源与无覆盖证据仍保留。
- 轨迹审计和数据库轨迹写入直接读取 MarketRevisionRef 字段及 visibility_mode 枚举；数据库 manifest 的 visibility_mode 按字符串列处理。
- 回放策略输出统一为 ReplayEvaluation，删除 DecisionResult/任意元组/布尔值的形状猜测及实时金额字段别名。测试在回放边界显式组装结果；历史 trace_payload 的字段解析保留。
- 成交身份查询、提交预留与候选序列化直接读 ORM/正式模型字段。预留策略归属统一来自仓库装配配置，不再寻找 PositionKey 上不存在的 strategy_name。
- 数据库 INSERT/DELETE/UPDATE 结果直接读取 rowcount，不再缺字段默认零；真实并发冲突的影响行数判断保留。
- 收盘 K 线流明确生命周期事件类型，行情循环直接调用正式遥测进度方法并读取行情 data_complete 字段。
- 交易规则哈希只接受正式 SymbolTradingRules 映射，删除任意字符串/字典/猜测对象的多形状序列化；开仓报价与模拟退出直接读取正式方向枚举，删除透传枚举辅助函数。

### 行情装配与执行客户端契约收敛

- MarketDataRuntime 正式字段直接读取；实际可选 publisher、prefetcher 和留存配置仍以 None 表达。生产必有报价 Hub，删除启动/停止时缺 Hub 跳过的分支。
- 留存依赖查询直接迭代 SQLAlchemy ScalarResult.all()；删除方法探测、异步返回兼容与列表形状判断。分区检查成为留存仓库必需接口；测试替身同步实现真实接口。
- 信号记录器直接调用 QuoteVolume24hProvider.metrics_snapshot，行情质量字段与候选 reduce_only 直接按正式模型读取。
- OrderExchangeClient 明确边界回调装配方法；首次提交内联执行一次装配，删除探测与单独 ensure 包装。提交物理边界统一由客户端负责，查询/取消边界仍由状态机记录。只吸收 WS 事实的路径不触发客户端装配。测试客户端按正式职责发出提交边界。
- 规则哈希通过局部字段接口表达输入，避免 operational 反向引用 execution 造成子包依赖环；域子包的运行时与类型依赖环测试通过。
- 决策引擎环境、行情序号和候选生成器按正式模型读取；轨迹中的方向/健康枚举直接取 value，删除枚举薄辅助函数。行情批次引用与命令仓库也直接读取正式 symbol/command 字段。
- 数据集校验统一为 MarketBookRepository.verify_manifest：现有内存校验逻辑归入内存适配器，Postgres 保持自身 SQL 校验；DatasetCatalog 删除能力探测与结果不是字典时回退的分支。

### 流指标、容量输入与采集队列收敛

- 账户流指标明确为 BinanceUserDataStreamMetrics，直接读取溢出计数，删除字段探测与整数薄封装；测试流明确实现指标接口。
- 入场轨迹直接转换已声明的字符串枚举，删除 _enum_text 包装；纸面仓位状态按字符串列/字符串枚举处理。订单冲突直接读取异常 __cause__；运行时直接读取已注入信号记录器的成交量指标。
- CapacityGuard 注入接口统一为 disk_free_bytes_fn，直接返回空闲字节数，删除对象与整数双形状和专用测试壳。
- 采集排空使用 Queue.join()；空队列且没有正在处理的 worker 时直接返回，删除四处 _unfinished_tasks 私有计数探测，避免借助缺字段默认零误判已完成。
- 数据库池从 AsyncEngine 的正式接口取得，指标只对 QueuePool 读取；删除绑定属性、sync_engine/pool、checkedin/checkedout 的方法探测与提取异常吞掉。真实无绑定或非队列池继续表示无相应指标。
- 仪表盘采集状态的容量注入同步迁到 disk_free_bytes_fn；实际 shutil.disk_usage 适配仍读取其标准 free 字段，测试只在注入边界提供整数。
- 策略参数序列化外层只读取现行数据类字段，删除 _policy_object_fields、多形状外层与 None 默认；所有生产/回放调用方使用 EffectivePolicy。规则及量化模型的内部参数仍按其实际数据形状序列化。
- 策略状态序列化外层同步统一为 PolicyState 数据类，删除把 Mapping 当 custom_state 和 None 视为空状态的兼容。
- 主 CollectionSource 明确停止、设置游标和恢复方法，删除三处能力探测。生产主源只有 WebSocketMarketStateSource，数据库回填由独立 backfill_source 承担；测试源同步实现契约。
- 主采集源明确返回可关闭 AsyncGenerator，Collector 和 Hub 直接调用 aclose，删除可关闭性探测。Hub 真实并发关闭重试继续保留。

### 策略配置与缓存接口单一化

- 实盘配置只提供已构造的 OrderFlowImpulseConfig；删除重复扁平字段、注册器旧字段构造和再次覆盖转换。账户策略差异仍由正式 profile 构造，配置哈希行为回归保持稳定。
- 策略缓存维护直接接收正式策略对象，删除三路可选回调、StrategyCacheMetrics 数据壳及探测后静默跳过。缓存指标直接读取；删掉锁定缺回调行为的旧测试，保留保护集合、清理周期和日志行为测试。
- 图表渲染边界统一 domainStart/domainEnd，权益点用 atMs、对比点用 at；删除多字段时间别名。overview 就绪状态只读取正式 database_status。
- WebSocket 关闭码按 ConnectionClosed 的正式接收帧读取，删除任意异常 code 探测和弃用属性访问；无关闭帧继续表示异常断开 1006。
- 策略 required_data 成为必需契约，删除无数据要求时默认间隔、放行预热、默认桶数等路径。测试策略显式提供正式数据要求，删除锁住无要求默认的参数化用例。
- 参数序列化的对象分支只接受数据类，删除任意 __dict__/__slots__ 猜测；生产固定金额与权益比例量化模型均为数据类。原始值、映射、集合等真实参数类型仍保留。
- 监控脚本删除无调用方的 evaluate_rss_growth 旧单样本兼容函数，保留生产使用的按窗口与连续采样评估。
- systemd 监控统一解析已明确请求的 Unix 时间戳，删除默认本地文本格式后备解析及其旧兼容测试，保留缺失/非法时间的未知状态。

本批验证：Python 单元/冒烟/端到端 2965 项通过、1 项因未配置实库环境跳过；数据库集成 172 项通过；前端 63 项通过；架构约束 122 项通过。Ruff 和 diff 检查通过，349 个模块含类型导入扫描为 0 环，文档本地链接无失效。删除了 3 项只固定旧兼容行为的测试，核心业务测试保留。

剩余 src 属性探测候选 15 处，集中在异常限流提示、数据库驱动诊断、初始化失败清理与数据字段检查。全范围候选基线仍包括历史身份恢复、分区维护和其他业务替代路径；尚未完成每条候选的语义核查，不能把本批通过当作全仓兼容清理完成。

### 限流接口与留存异常路径收敛

- HTTP 查询/取消、账户同步退避直接读取正式响应 Retry-After；启动与退出恢复只接收明确异常类型的退避字段，删除任意异常临时字段兼容。账户限流测试使用真实 HTTPStatusError 响应。
- 只读核查生产表结构：行情与运行事件表均为分区表；账本仍有 4 条 legacy-postgres-account 事件，当前新流 checkpoint 124178 条、journal 635435 条。以上为核查时快照，不是部署后状态。
- 新建数据库迁移仍创建普通表，普通表小批删除是现行初始化路径，保留。删除分区维护抛错后继续行删除的异常替代路径，错误由后台留存循环报告。
- 候选遥测直接读取 OrderIntentCandidate 的正式 candidate_id、signal_id，删除泛型字段探测与文本透传函数。
- 归档研究报告统一 live_entry_order_rows，生成器和分析器同步删除 live_reconstructed_rows 别名；成交量报告删除旧事件排序回撤指标及展示列，保留按平仓时间计算的回撤。

本批最终验证：Python 单元/冒烟/端到端 2967 项通过、1 项未配置实库环境跳过；数据库集成 172 项通过。Ruff 与 diff 检查通过，349 个模块含类型导入扫描 0 环，文档本地链接无失效。新增 2 项留存异常路径回归；没有提交、推送或部署。

当时待处理、现已在下一节完成：order_identity_adapter 在缺少成交明细时仍按订单生成合成成交（含数量、价格与时间推断）；它只标注为非权威输入，但与真实成交共用同一事实模型。需检查快照恢复路径与测试数据后清理生成分支，不能宣称全范围清理已经完成。8 处剩余属性探测是数据库驱动诊断、动态必需字段验证和构造失败后的任务清理。

### 恢复输入停止生成假成交

- 删除 order_identity_adapter 的订单推断成交分支（价格、数量、成交时间、费用/盈亏占位），LegacyOrderIdentityAdapter 单方法壳改为 build_position_account_facts 普通函数；只接收已存在的 AccountFillEvent。
- 删除仅用于合成成交的价格汇总、均价计算及跨层参数传递。历史已持久化的 synthetic 标记仍由恢复模型读取，不把旧事实洗成权威事实。
- 仓位观察没有 observed_at 时不生成快照，删除当前时间/订单更新时间替代观察时间。真实成交、带时间的仓位观察和退出提交边界继续保留。
- 生命周期测试显式提供成交事件 fixture，不再依赖生产层合成；保留真实批次、部分成交、重开仓、退出及旧身份冲突行为验证。新增未知观察时间不造快照的回归。

本批完整回归：2968 项通过、1 项因未配置实库环境跳过；数据库集成 172 项通过。Ruff、diff 检查通过；已删除类、合成成交 ID、均价推断函数和旧参数在 src/tests/scripts 中均无引用。服务器未部署此批变更。全范围候选的最终处置审计继续进行，不以这批通过宣称全部结束。

### 预留数量与资源初始化收敛

- 执行事务的 proven_position_quantity 成为必需参数；ExecutionBook 传入确认的数量，删除事务预留写入缺数量时重新查独立账户快照的分支。独立后台预留接口继续使用其自身数据来源。
- 采集器后台任务字段在构造器执行文件操作前明确初始化；析构直接遍历这两个字段取消任务，删除构造失败时的字段探测，保留清理异常不外抛。
- 预热与行情连续性直接读取正式 MarketState15s 的策略必需字段；保留值为 None 的不完整行情判断，删除字段不存在时也当普通缺口处理的兼容。正式策略声明的字段均存在于现行行情模型。
- LiveMarketLoop 的决策过滤器统一为异步契约，生产装配只调用 create_authoritative_async_decision_filter；删除同步结果/可等待结果的运行时探测。研究独立纯计算接口不变。
- 留存执行器结果统一为 PruneOutcome，删除二元组结果转换；异步执行入口只等待异步执行器，同步入口直接调用同步执行器。生产异步调用和测试同步调用分别遵守明确契约。

本批最终完整回归：2968 项通过、1 项未配置实库环境跳过；执行/持久化/集成组合 740 项通过（其中集成 172 项），采集器 63 项通过，留存及装配 69 项通过。Ruff 和 diff 检查通过。

src 剩余 3 处属性默认探测都位于 SQLSTATE/pgcode 诊断，属于外部 DBAPI/SQLAlchemy 异常适配。仍需核对账户快照回调和采集 sink 的同步/异步联合接口，不能仅凭属性探测数量推断全仓已完成。

### 账户与采集回调契约统一

- 账户快照回调统一为 Awaitable[None]；生产只有异步组合回调，删除返回值可等待性探测。测试以明确 AsyncMock 回调验证记账顺序、失败与退出事件流。
- 采集 acknowledgement/envelope/gap sink 统一为异步回调，直接等待；生产行情发布器和缺口处理均为异步函数。保留批量处理的主动让出，防止立即完成的回调长期占用事件循环。
- 删除 domain.market 中无调用方的 CaptureRepository、ArchiveAcknowledgementSink 和重复 ArchiveManifestSink 导出；归档模块继续使用自身真实回调契约，RawArchive 保留实际消费者。
- 删除无调用方、仅导出的 StrategyPolicy 协议；当前决策引擎直接调用纯函数 execute_policy_transition。EnvelopeRecoveryBatch 有字符串前向类型引用，核查后保留。

补充收敛：事件 Hub 的持仓预期登记统一为实际同步注册接口，删除可等待性猜测；归档研究脚本直接按 HTTPStatusError 读取状态码，删除两层属性探测。无引用的市场仓库/回调协议与 StrategyPolicy 导出已删除。兼容清单明确区分清理前快照和当前处置，保留真实恢复与外部驱动适配的依据。

最新验证：完整回归2968项通过、1项缺数据库环境跳过；数据库集成172项通过；事件Hub及装配35项通过；diff空白检查通过。全工作区Ruff扩展检查发现脚本、运维及测试存在存量问题，不能把本次验证表述为全仓Ruff通过。最终候选语义审计仍未完成，未提交或部署。

显式模型输入继续收敛：RetentionAuthority 的仓库参数必需，删除自动回退内存仓库；生产保持现有显式Postgres装配，四组测试显式注入内存仓库（59项通过）。AccountFillEvent.raw_payload 按必需字典读取，删除账本及仓位处理中的空字典替代。订单身份适配和仪表盘遥测读取正式ORM字典字段，删除字段类型错误时返回空事实的分支；内联只有一个调用方的成交payload包装。删除测试中不符合ORM字段契约的None/列表兼容用例，保留显式空证据与真实成交分离验证。

补充：PaperExitConfig全部实际构造使用正式枚举/默认值，删除内部字符串转枚举兼容；归档宽限重放摘要直接比较StrEnum，删除status.value属性探测。原242条属性探测候选已回填：238条原表达式不再存在，3条数据库驱动诊断适配保留，1条归档状态探测已删除；不把表达式消失等同于全部6406条候选语义审核完成。最新完整单元/smoke/e2e及数据库集成3139项通过、1项采集环境缺失跳过；349模块含类型导入无循环，修改核心文件Ruff F/I与diff检查通过。

最后的PaperExitConfig构造收敛追加验证：相关持久化单元与数据库集成8项通过；最终diff检查通过。

归档入口实际运行核查：backfill_paper_grace_exits.py的--help因已删除strategy_runner.portfolio报ModuleNotFoundError，原mark_positions实现也已不存在。删除这一失效、仍可回写数据库的旧重放脚本，不恢复旧交易算法；清单保留路径文本和已删除状态，移除失效链接。前述该脚本的状态枚举收敛已被整文件删除取代。

脚本导入审核：删除引用已移除私有摘要/成交函数的固定事故补丁repair_server_state_20260929.py，旧源码仍可Git追溯。load_test_market_data_pipeline.py直接导入领域RuntimeStateSequenceRange，Noop采集仓库只实现当前批量质量事件接口。实跑1000条、4symbol、128队列容量：7658条/秒，丢弃0，质量事件0，未恢复缺口0；该小规模本地结果不代表实盘性能对比。

留存及查询异常路径收敛：DatasetScope.dataset_id只接受DatasetId，解析入口只接受实际调用的名称字符串，删除对象透传/覆盖与id_value薄包装。留存数据库锁失败直接传播，合并单事务与独立登记的锁调用，删除日志后继续写的兜底（新增3项验证失败时不写依赖、不提交）。订单读取的敞口声明查询失败不再伪装成缺参考价格（新增单条/批量2项验证）；已落库声明作为历史市价单参考来源仍保留。留存及分区54项、读取与锁失败15项通过；完整回归和集成3144项通过、1项采集环境缺失跳过。

检查点仓库使用装配好的AsyncEngine绑定及真实连接池，删除引擎类型猜测、可缺池分支、监听注册吞异常；测试替身显式提供NullPool接口。持久化、数据库集成与资源释放11项通过。

删除仪表盘MarketDataPerformanceResponse中的simulated_close_drop_count：API固定返回0，前端及其他调用方不使用该字段；采集端真实的分阈值延迟计数仍保留，未将真实计数当兼容废料删除。连接池装配收敛后的完整回归与集成3144项通过、1项采集环境缺失跳过；全docs本地链接0失效。

末尾验证：仪表盘及API136项通过；数据集解析去掉与枚举解析完全重复的account_snapshots特判，留存39项通过；本批核心代码F/I及diff检查通过。最终6406候选语义审核仍未全部完成，未提交、推送或部署。

规则来源收敛：EffectivePolicy只接收SymbolLotRules，删除callable/Mapping/直接对象三形状解析。实盘异步过滤入口要求effective_policy_provider，每次冻结该symbol的规则；删除异步入口的金额/默认策略双来源。规则纳入参数序列化与哈希，不再跳过动态规则函数；删除旧形状4项测试，增加实际规则冻结验证。删除生产default_symbol_lot_rules前缀猜测及两处导出，只保留测试BTC规则夹具。同步过滤工厂目前仅被异步适配器和测试引用，没有发现真实paper/研究调用，继续列为下一项收敛候选。

带规则回放验证发现Decimal字符串尾零导致下一策略状态不一致：固定金额的resize_fraction为0.0175/0.01750，权益比例的fraction_of_equity同类。量化模型生成时统一规范数值文本，不增加回放兼容分支；固定金额及权益比例的完整决策回放现均通过。决策/行情77项通过；此前完整回归和集成3141项通过、1项环境缺失跳过，新增模型回放后的完整验证正在执行。历史未冻结规则的trace不补造参数，不能据新规则宣称旧记录可重现。

本批最终验证：完整单元/smoke/e2e与数据库集成3143项通过、1项真实采集环境缺失跳过，2个既有警告；Ruff F/I及diff检查通过。下一项为没有独立生产调用方的同步决策工厂/回调包装；全系统候选语义审核未完成，未提交或部署。

决策过滤链路拍平：删除没有独立生产调用方的create_authoritative_decision_filter同步工厂及导出；异步过滤直接取得事实、固定候选身份、调用decide、生成trace并await单一durable_decision_commit。删除两组回调、临时trace/result列表、数量比对、默认100金额策略与重复身份绑定。删除仅转调decide的DecisionEngine类及无调用方map_decision_rejection_reason导出。旧同步接口测试改为真实异步契约；原“有candidate”夹具实际为空，已改为包含真实signal/candidate以验证缺事实与symbol不匹配；保留提交失败不重试的验证。新增同批两笔候选测试证明facts1→commit1→facts2→commit2且第二笔读到更新后的策略版本。相关77项通过，核心Ruff F/I及diff检查通过。

本批最终完整unit/smoke/e2e与Postgres集成3143项通过、1项真实采集环境缺失跳过、2个既有警告；diff检查通过。单行转调扫描还发现其他门面和适配器候选，需按真实边界职责核查，尚不宣称全系统候选语义审核完成。未提交、推送或部署。

无调用方入口清理：删除PositionRecoveryCodec重复的stable_snapshot_anchor_id类方法，保留所有调用方正在使用的snapshot_encoding领域函数。行情循环一次读取required_data，删除两个仅取属性的包装和3个镜像测试，保留缺口重预热测试。全src/tests/scripts名称扫描后删除13个未引用属性、未使用PositionHistory与ArchiveProgress；连带删除只写不读的行情时间、启动连接原因和信号recent_records的4096条重复缓存及配置。删除5个无运行时消费者的Shadow ORM类和端到端夹具中的旧影子清理；保留历史Alembic迁移和物理表。删除已经引用不存在rebuild_position_batches的旧事故重现脚本docs/runbooks/repro_batch_price_contamination.py，不恢复旧算法。恢复及行情局部126项通过；完整回归正在执行。

本批完整unit/smoke/e2e与Postgres集成3140项通过（2968+172）、1项缺真实采集环境跳过、2个既有警告。修改文件Ruff F/I及diff检查通过。随后进一步删除仅存在声明和历史迁移、无任何运行时查询/写入的AccountPerformanceMetricRow，保留迁移物理表。剩余候选仍按实际业务用途判断，不将普通缺省值或真实恢复全部称为兼容废料。

绩效闲置ORM删除后的持久化模型与仪表盘138项通过，F/I及diff检查通过。扩大扫描到src/tests/scripts/docs/deploy/alembic后，单次出现且无引用的私有函数候选为0；这是词法引用检查，不证明所有语义兜底均已审完。

发单编译简化：退出批次截断合并成一套流程，删除单批/多批套层和在TradeCommand总量约束下不可到达的补足分支；用replace保留入场价格元数据，修复旧重建对象丢入场价格，新增单批、多批、末批全截断3项验证。计划策略身份直接使用已经校验非空的candidate名称/版本，删除context及硬编码orderflow_impulse/v0回退、计划身份比较和再覆盖。删除重复分配总量校验，统一由TradeCommand执行；敞口预留使用实际量化金额，不再回退原始期望金额。清除闲置_NEW_EXECUTION_FIELDS历史配置表和runtime_options未使用Git哈希长度常量。局部29项、执行协调/宽限契约70项通过；完整unit/smoke/e2e与集成3143项通过、1项真实采集环境缺失跳过、2个既有警告。

采集生命周期：删除__del__吞异常取消任务兜底；生产入口已有finally显式await collector.stop()，依赖显式生命周期而非GC时机。采集回归继续验证。

显式仓位生命周期装配：LiveCandidateSubmission、LiveExitProcessor与LiveDaemonLifecycle都必须传入同一个PositionLifecycleLocks，删除提交/退出各自默认建锁和生命周期可不排空锁的分支；LiveExitProcessor账户必须显式传入，不再回退run_id。生产daemon本来即共用同一实例；4处测试装配改为显式提供真实锁。提交/退出/锁/生命周期及黄金链路51项通过，采集63项通过。正在执行包含本批全部修改的最终完整回归。

并发门禁拍平：EntryLaneConfig不再持有max_concurrency_per_symbol，删除入场层重复统计/策略结论覆盖、_count_symbol_concurrency包装及runtime_orchestrator的重复参数；提交时RiskGateway组合FixedLiveLimits继续依据当前持仓和在途计划做统一核验。配置正数检查归位FixedLiveLimits。删除3项旧入场层测试（其中2项仅靠提前拦截才能绕过无异步执行契约的替身），保留领域并发统计与真正提交上限测试。入场/提交/daemon/风控/批次109项通过；包含锁装配和GC清理的此前完整3143项通过，本项后的最终完整回归正在执行。CSV对src3类表达式补充AST对照：132条原表达式已不存在、2681条可匹配、9条不能独立解析；不据此宣称2681条均需删除或全系统审核完成。

本轮最终完整unit/smoke/e2e与Postgres集成3140项通过（2968+172）、1项因缺真实采集环境跳过、2个既有警告。本轮代码Ruff F/I及diff检查通过，docs本地链接0失效。待继续核查的实际来源包括退出恢复通知的默认no-op回调、指标查询的后备范围和其他基线异常路径；尚未宣称全系统兼容/兜底语义审核完成。未提交、推送或部署。

恢复通知去空回调：LiveStrategyDaemon、LiveMarketLoop、LiveExitProcessor、LiveDecisionFactSource及LiveOrderEventRuntime必须显式提供退出恢复/订单清理回调，删除6处默认lambda no-op。生产原本显式注入，测试5处文件显式提供Mock观察回调；相关150项通过。

退出分配收敛：ExitAllocator类只剩生产使用的plan_exit，把它改为顶层纯函数plan_exit_allocations。删除没有生产调用方的create_exit_command工厂及其随机UUID、方向默认推断和旧导出，删除2项只锁住旧工厂的测试；成交归因/并发预留测试明确构造TradeCommand，一方向SHORT退出BUY行为改由真实plan_order_execution验证。领域与执行Book相关109项通过。ExecutionRequest.order_type按正式str读取，删除str/枚举双形状兼容；分配计划始终存在，不再写if alloc_plan/else None。完整回归正在执行。

未使用状态清理：删除ResearchCollectorService仅存储不使用的spool注入参数、LocalBatchSpool及SpoolRecord重复旧持久化实现（实际运行ArchiveJournal）；删除1项旧spool测试，保留当前journal重启恢复/背压测试。删除BinanceConnectionPool仅存储不使用的control_messages_per_second参数及无人调用的空start方法；真实WebSocket连接仍直接取得控制频率配置并执行限速。采集/连接池/WebSocket/真实平仓事实引导93项通过。完整回归发现1处测试把FuturesPositionSide从trade_command间接导入，已改为正式order_state模块，不恢复无效兼容出口。src/tests/scripts/docs/deploy/alembic第一方import名称静态核查0缺失，当前最终完整回归执行中。

本轮最终完整unit/smoke/e2e与Postgres集成3138项通过（2966+172）、1项缺真实采集环境跳过、2个既有警告；修改文件Ruff F/I及diff检查通过。第一方import名称缺失0、未引用私有函数候选0、只写私有字段候选0；这些静态结果不等同于全系统语义审核已完成。下一项重点为执行Book在既有订单计划之后再次编译退出分配、未有本地批次时均摊数量的兜底，以及协调器策略身份默认回退；需确认计划分配与批次容量的实际区别，保留正常退出通道。未提交、推送或部署。

退出分配数量来源：协调器构造ExecutionRequest时只从plan.allocations取实际本次退出量；单批计划取plan.quantity，不再用plan.batch_quantities（完整批次容量）顶替本单分配数量。执行Book在本地无活动批次时只接受明确的批次数量，删除缺字段/无数量时均摊requested_quantity的虚构归因。现有多批测试增加容量10/20而实际退出2/3的输入，确保预留仍为2/3；相关143项及增强回归通过。执行Book优先采用明确分配，删除已有计划之后再次按FIFO分配；新增实际两批仓位按0.2/0.8退出的测试，确认命令及预留均保留计划分配。订单身份查询失败测试补齐正式批次数量，不删除其失败保护断言。完整回归3139项通过、1项缺真实采集环境跳过、2个既有警告。

旧量化入口清理：quantize_order_plan无生产调用，仅旧单元测试和宽限挂单测试调用，删除quantization.py及8项旧接口测试。正在使用的QuantizationRejection移到trade_command_planner；宽限挂单交易所测试取消旧入口参数分支，仅验证当前plan_order_execution。当前入口已有向下截断、上下限、reduce-only最小金额豁免、价格精度、单向SHORT退出BUY及分配保真验证，相关23项通过；最终完整回归正在执行。

本轮最终完整unit/smoke/e2e与Postgres集成3130项通过（2958+172）、1项缺真实采集环境跳过、2个既有警告；修改文件Ruff F/I及diff检查通过。旧量化模块11条基线候选已回填整文件删除状态。未提交、推送或部署；剩余候选的语义审核继续。

执行方向去推导兜底：ExecutionRequest增加必需的关键词side，协调器从正式订单BUY/SELL及reduce_only转换策略方向，Book直接使用请求方向，删除从已有episode/position_side/默认LONG猜测。更新生产两处构造及测试正式输入；新增单向LONG/SHORT开仓/退出4项转换测试，空仓无head用例同时验证LONG和SHORT，确认命令方向不被BOTH默认值改写。执行相关560项、增强方向回归148项通过；修改文件F/I及diff检查通过，最终完整回归执行中。

本轮方向简化后的最终完整unit/smoke/e2e与Postgres集成3135项通过（2963+172）、1项缺真实采集环境跳过、2个既有警告；修改文件F/I及diff检查通过。全src/scripts/deploy的ExecutionRequest构造只在协调器两处，均显式提供side；未提交、推送或部署，其他候选继续语义核查。

退出分配输入单一化：plan_exit_allocations生产唯一调用方传PositionView，旧PositionLedgerProjection分支只有测试使用；删除Union和isinstance分支，旧分配测试改用实际PositionView，保留FIFO、灰尘策略、批次预留与数量守恒测试。PositionReservation在构造时已验证消耗+释放不超预留，active_quantity直接计算，删除max(0)遮蔽；consume/release用replace只更新对应数量，保留身份和创建时间。执行领域及订单553项通过，完整回归执行中。

本轮最终完整unit/smoke/e2e与Postgres集成3135项通过（2963+172）、1项缺真实采集环境跳过、2个既有警告；修改文件F/I及diff检查通过。进一步引用核查发现ABSORB_DUST_SINGLE_BATCH只在分配函数及2项旧测试使用，需下一步连同实际序列化消费者确认；FULL_POSITION_CLOSE仍由决策policy_transition产生，不能一并删除。未提交、推送或部署。

灰尘策略清理：ABSORB_DUST_SINGLE_BATCH只被2项旧测试选择，删除枚举值、算法分支、价格/最小金额参数、absorbed_dust模型字段及始终写0的序列化/解码默认。删除2项旧测试；保留FULL_POSITION_CLOSE实际决策调用，将全量和未指定数量合并至同一个容量分配循环，预留可用量一次计算后复用，删除重复结果构造和循环内已证明为正的分配判定。全量退出用例扩展验证明确小数量仍按全量策略退出，以及目标批次未指定量的正常行为。领域/决策/持久化632项通过；修改文件F/I通过，最终完整回归执行中。

本轮灰尘策略删除与分配合并后的完整unit/smoke/e2e及Postgres集成3135项通过（2963+172）、1项缺真实采集环境跳过、2个既有警告；修改文件F/I及diff检查通过。枚举引用核查还发现TradeCommandType.EMERGENCY_FLATTEN仅有声明，实际紧急清仓为独立授权命令/客户端方法，后续需核对编码恢复消费者，不删除真实紧急退出入口。全系统语义审核继续，未提交、推送或部署。

闲置概念清理：删除无构造消费者的TradeCommandType.EMERGENCY_FLATTEN，实际紧急清仓仍为独立授权命令和客户端方法；删除只作Book默认、无独立分配语义的CONSOLIDATE_ELIGIBLE，默认统一当前生产使用的TARGET_BATCHES_ONLY。Book命令恢复payload不存退出policy，决策持久化生产产生FULL_POSITION_CLOSE，保留该实际策略。执行/决策/真实紧急清仓授权507项通过。删除仅导出而无人使用的CashFlowType、无实现/请求消费者的SHARPE_RATIO指标选项，以及无事件生产者的LEASE_CHANGE/RULES_CHANGE失效原因；绩效/缓存/提交/仪表盘160项通过。外部风控Hub事件的action枚举仍由协议字符串构造、session状态从持久化恢复，不能凭静态成员无显式引用一律删除。最终完整回归执行中。

本轮闲置类型清理后的最终完整unit/smoke/e2e与Postgres集成3135项通过（2963+172）、1项缺真实采集环境跳过、2个既有警告；修改文件F/I及diff检查通过。进一步绩效字段核查发现MetricSpec.benchmark/annualization_factor无消费者，performance_builder传入的第三个USDT/ratio位置参数实际对应version而非unit，待下一轮统一正式配置并验证元数据。全系统审核尚未完成，未提交、推送或部署。

绩效配置瘦身：MetricSpec删除无读取方的benchmark/annualization_factor，version及unit仅允许关键词传参。修正仪表盘5个指标构造中USDT/ratio被误当version的位置参数，明确unit并保留v1版本。新增真实summary→calculate回归验证5个计算结果的单位/版本及权益增量；绩效和仪表盘148项通过，修改文件F/I通过。最终完整回归执行中。

本轮绩效配置收敛后的最终完整unit/smoke/e2e与Postgres集成3136项通过（2964+172）、1项缺真实采集环境跳过、2个既有警告；修改文件F/I及diff检查通过。全src/tests/scripts数据类字段扫描发现8项仅声明候选，包括FillModel.queue_delay_seconds/funding_rate_hourly、CoverageReceipt.end_condition、AccountEquityCut.fees_paid/funding_fees、CollectionReceipt.committed_sequence、ReplayResult.replayed_at、FactIntegritySummary.verified_position_count；后者还通过位置参数写入，需结合实际序列化和读接口核查，不能仅凭名称出现一次证明删除安全。全系统审核继续，未提交、推送或部署。

8项闲置字段确认并删除：FillModel.queue_delay_seconds/funding_rate_hourly未参与模拟，删除并修正文档注释；CoverageReceipt.end_condition固定cursor_exhausted但无实际游标核验，删除；AccountEquityCut.fees_paid/funding_fees从未输入或读取，真实现金流手续费路径继续保留；CollectionReceipt.committed_sequence始终None，归档Journal真实highest_committed_sequence不变；ReplayResult.replayed_at无人读取/输出；FactIntegritySummary.verified_position_count只写不读，删除返回时第三个位置参数。全src/tests/scripts/deploy含前端的字段名称与相关类消费者核查未发现序列化读方，相关287项通过，修改文件F/I及diff检查通过，最终完整回归执行中。

旧分区转换工具清理：全仓引用核查prepare_event_partition/cutover_event_partition仅定义、无CLI和测试调用；删除影子表建立/数据复制/表重命名约230行、2个报告模型、专用索引/旧表名工具及常量。正在使用的ensure_event_partitions/drop_expired_event_partitions与分区识别、普通表留存保持原行为；不改物理表或历史迁移。运行事件分区/留存/行情装配47项通过，最终完整回归执行中。

本轮8项字段及旧影子分区转换工具清理后的最终完整unit/smoke/e2e与Postgres集成3136项通过（2964+172）、1项缺真实采集环境跳过、2个既有警告；修改文件F/I与diff检查通过。静态公共顶层入口扫描剩余单次名称均为CLI装饰器注册函数，需依实际CLI检查而非名称次数删除；全系统兼容候选语义审核仍未完成。未提交、推送或部署。

绩效覆盖读取收敛：删除单调用方_row_content_hash_matches及字段反射字典、类型探测、str/Decimal双转换、None三态包装，直接按CashFlowRow正式字段计算并比较内容哈希。哈希和审批标识直接按字符串读取，删除None/任意对象字符串兼容。删除6处无时区时间补UTC分支，沿用Postgres带时区字段和领域估值点契约；删除采样间隔估计中的不可能None判断。不完整窗口、缺证据、占位审批、越界和内容哈希不匹配仍返回未核验，未新增交易门禁。仪表盘/绩效148项通过，修改文件F/I及diff检查通过，最终完整回归执行中。

本轮绩效覆盖适配删除后的最终完整unit/smoke/e2e与Postgres集成3136项通过（2964+172）、1项缺真实采集环境跳过、2个既有警告；修改文件F/I及diff检查通过。仍确认有协调器strategy_name/strategy_version默认回退及退出恢复缺原候选时v0来源，需从新计划构造与历史恢复分别确认，不能直接提高退出准入门槛。全系统语义审核继续，未提交、推送或部署。

请求策略版本清理：ExecutionRequest.strategy_version无读取或持久化消费者，删除必填字段并更新测试及2处生产构造输入，删除协调器对应v0回退。订单计划和候选策略版本仍记录实际策略身份，不将其删除。执行565项及完整3136项通过。

失效手动发单入口清理：submit-plan→_load_plan不读取Book必需projection_version/批次归属，plan_runner创建无当前执行UoW的独立协调器，按现契约无法完成提交。全仓只有CLI导入/调用，测试仅锁定help中的命令名称。删除该CLI入口、JSON加载器和约194行plan_runner，更新help测试的有效命令列表；保留daemon、订单反查、入场取消和真实账户清仓入口。CLI/daemon装配/session相关78项通过；修改文件F/I及diff检查通过，最终完整回归执行中。

单次发单会话外壳清理：手动plan_runner删除后LiveRolloutSession仅2项旧测试使用，删除该类、LiveSessionResult和LivePlanExecutor；删除2项仅锁住旧单次执行/门禁的测试，保留生产使用的LiveSessionLifecycle持久化契约测试。session不再重复导出runtime_session的资源类型；runtime_orchestrator、market_assembly和测试直接引用正式runtime_session。装配/资源/生命周期26项通过，核心F/I及diff检查通过，最终完整回归执行中。

本轮请求版本、失效手动发单和单次会话外壳清理后的最终完整unit/smoke/e2e与Postgres集成3134项通过（2962+172）、1项缺真实采集环境跳过、2个既有警告；修改文件F/I及diff检查通过。策略名称仍用于与敞口事务一致的锁域，协调器名称默认需下一轮核对全部新计划调用方，历史订单查询/取消不应被增加提交条件。全系统语义审核继续，未提交、推送或部署。

提交策略身份收敛：_build_execution_request要求显式strategy_name，删除从plan读取再strip/硬编码orderflow_impulse的回退；原子提交从已批准preparation.intent取正式策略名称，直接提交必须由新计划提供名称。取消/反查/历史凭据恢复不调用这个提交构造器，不增加其准入条件。现有单向方向回归同时验证plan无身份时显式请求身份仍正确，4项通过；相关订单/提交/装配121项通过。退出运行器直接使用TradeCommand.order_type正式EntryType，删除枚举/字符串兼容。修改文件F/I及diff检查通过，最终完整回归执行中。

本轮提交身份显式化和枚举兼容删除后的最终完整unit/smoke/e2e与Postgres集成3134项通过（2962+172）、1项缺真实采集环境跳过、2个既有警告；修改文件F/I及diff检查通过。原协调器orderflow_impulse名称回退和运行器订单类型双形状表达式均不再存在。下一轮继续按实际构造方核对ManagedLivePosition/OrderExecutionPlan等内部模型的字符串枚举适配与剩余候选；全系统语义审核尚未完成，未提交、推送或部署。

内部模型枚举去双形状：生产ManagedLivePosition两处构造均来自已解析的仓位分类/PositionView，OrderExecutionPlan的5处生产构造均提供FuturesPositionSide。删除ManagedLivePosition策略方向/持仓方向及OrderExecutionPlan持仓方向3处字符串转枚举；测试daemon与闭合K线回放共12处字符串方向夹具改为正式StrategySide。数据库订单读取和交易所孤单取消边界仍显式解析外部字符串，不恢复内部兼容。实盘/订单/读取887项通过，修改文件F/I及diff检查通过，最终完整回归执行中。

本轮内部方向枚举兼容删除后的最终完整unit/smoke/e2e与Postgres集成3134项通过（2962+172）、1项缺真实采集环境跳过、2个既有警告；修改文件F/I及diff检查通过。下一项核查PositionKey/AccountFactStreamScope/FactCoverageInterval及退出策略的枚举转换来源；类型拒绝验证、不可变字典保护和真实外部解码不按转换语法一律删除。全系统语义审核尚未完成，未提交、推送或部署。

退出策略和覆盖状态去重复解码：PositionExitPolicy生产构造来自正式运行配置或trace_audit已解析的PositionExitMode；FactCoverageInterval生产构造来自正式证明/账本或recovery_codec已解析FactCoverageStatus。删除两个模型的字符串转枚举分支，保留回放/检查点外部解码和字段有效性约束；未发现测试或归档脚本直接传字符串mode/status。执行/决策/行情/策略570项通过，修改文件F/I及diff检查通过，最终完整回归执行中。

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

剩余纯取消重抛清理：复核ConnectionClosed/httpx异常和4种Hub异常均属于Exception，CollectorPaused继承RuntimeError；stream_recovery的动态异常参数契约为type[Exception]且4个生产调用均为正式Hub错误。7文件删除余下8个仅裸raise的CancelledError分支，源码已无此类单独空分支。Binance本处是保证金类型预热读取，不是订单POST归因；有实际取消清理/订单处理及finally保持。全src Ruff F/I通过，候选表按原表达式精确核销，完整回归执行中。

余下8处空取消分支删除后的完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；全src Ruff F/I与diff检查通过。候选表32条原空取消表达式精确核销（含前批同文件保留其他取消动作而尚未标记的行），不按整文件宣布完成。定时退出REST失败时利用缓存继续平仓的路径有明确离场作用，保持；StreamAvailabilityClock两处可空阶段计时与真实重连状态转换需继续按状态不变量审核，全系统语义审核尚未完成，未提交推送部署。

Hub写入与监控池旧字段收敛：4个Hub的_write_messages只有while发送及ConnectionClosed/CancelledError裸重抛，删除整个try/except外壳，异常仍自然传播。TrackedMembership.left_target_at现有生成器始终None，所有源码消费者只搬运、不参与策略/展示；删除领域字段、ORM映射、生成和仓库读写传递，同步12处旧测试构造及展示测试无用字段。保留历史迁移的可空物理列，不做删列；新快照省略该列，历史快照读取不再适配字段。清理脚本遇到旧测试真实时间参数中止后，停止首个测试进程，补全构造修改再重新运行完整回归；不计中止运行成功。全src和修改测试F/I通过，完整回归执行中。

Hub空发送异常外壳与left_target_at旧模型字段移除后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；全src及修改测试F/I与diff通过。src/tests已无left_target_at引用，历史迁移保留可空列。域内关键词复核：行情effective_price处理真实无成交桶而采用报价/标记价；evidence哈希省略空settlement_fills有真实持久化重放身份语义，不能按旧字段无消费者同类删除。全系统候选语义审核尚未完成，未提交推送部署。

持久化序列化去重复：paper_daemon/risk/strategy_run三个仓库的_jsonable与既有postgres.serialization.jsonable逐行相同，删除3份实现并直接调用公共函数；paper_daemon/strategy_run的_normalize_for_compare也完全相同，收敛为serialization.normalize_for_compare，删除2份实现，不保留旧名别名。Decimal/UTC时间/枚举/嵌套序列规则及原未知对象行为保持；runtime_telemetry使用Mapping/时间原格式/Decimal不同，未混合。全src Ruff F/I通过，候选按5个旧函数更新，完整回归执行中。

持久化5份重复转换函数合并后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；测试结束后仅清理2文件末尾空行，全src Ruff F/I与diff检查通过。原仓库私有函数名及别名均不保留；未知类型字符串转换是否还可去除，需要按各调用方实际payload契约继续核查，不因函数合并直接改存储输出。全系统候选语义审核尚未完成，未提交推送部署。

排名与事务预留去默认：Universe读取先按非空收益/排名构建RankEntry，直接按.rank排序，删除2处or 0及重复排名筛选，仓库集成+Universe/e2e22通过。ExecutionTransaction/ExecutionReservationStore/Postgres事务实现统一batch_quantities为必填Mapping；唯一生产Book调用本就传current_view.batches容量映射，删除在会话内写入的None/空映射适配及空映射跳过容量分支，批次不存在仍明确冲突，容量直接索引。独立同步/异步仓库旧API的可选契约另行核查，不混称全部容量路径统一。全src F/I通过，完整回归执行中。

排名读取与事务容量必填后的完整回归3134通过、1项缺实盘采集环境跳过、2既有警告。随后补充真实PostgreSQL的空容量映射/容量不足两个拒绝并验证无残留预留用例，所在事务集成文件12通过；新测试不锁旧可选行为。全src/修改测试F/I与diff通过，集成全套重新验证中；全系统其余兼容路径继续审核，未提交推送部署。

新增容量契约测试后的集成全套174通过、1既有依赖警告，测试基线更新为unit/smoke/e2e2962与集成174；新增2项未以整套3136同次执行冒称结果。源代码最终变更此前完整3134回归已通过，后续仅增测试与文档。全系统其他兼容/兜底候选尚未审核完，未提交推送部署。

剩余预留写入入口收敛：全src无save_reservation调用，删除同步协议声明及InMemory/同步Postgres/异步Postgres3个单笔转批量包装；当前生产coordinator与Book批量调用均显式传容量，将剩余批量协议/实现容量设为必填关键词参数，删除3处空映射转换、has_capacity_limits及_require_batch_capacity(None)跳过路径。重复身份采纳/终态拒绝、批次容量和仓位总量限制仍保留，测试单笔初始化改走正式批量接口，78项核心预留/协调器测试通过，全src/修改测试F/I与diff通过，完整回归执行中。

剩余批量预留必填容量后的完整回归3136通过、1项缺实盘采集环境跳过、2既有警告。继续全仓搜索同步PostgresPositionReservationRepository，仅定义和1项SQLite协议测试存在，生产装配/修复使用Async实现；删除同步类及专属同步锁/仓位读取共287行，删除屏蔽容量检查的SQLite旧协议测试。真实异步事务/恢复接口和PostgreSQL容量拒绝/回滚测试保留。清除因此无用Session/sessionmaker导入，F/I通过，最终完整回归执行中。

同步Postgres预留旧路径删除后的最终完整回归3135通过、1项缺实盘采集环境跳过、2既有警告；测试基线为unit/smoke/e2e2961、Postgres集成174。全src/修改测试Ruff F/I和diff通过，src/scripts不再存在同步Postgres预留类、专属同步仓位读取或单笔保存定义。事务暂存使用InMemory仓库有当前copy_for_transaction调用，未作为无用类删除；其他未判定兼容候选继续审核，未提交推送部署。

预留闲置期限字段删除：全仓核查save_reservations的expires_at仅声明与写入，没有生产传入、读取或释放使用；同步协议/内存参考、Async仓库独立及事务写入删除参数，PositionReservationRow移除映射，历史可空列保留。测试异步仓库及协调器替身同步去参数/转传，不改变订单真实超时或宽限离场机制。清除无用datetime测试导入前停止首个回归进程，修改后重新启动，不计中断测试成功；完整回归执行中。

预留无消费者expires_at参数/ORM映射删除后的最终完整回归3135通过、1项缺实盘采集环境跳过、2既有警告；全src及相关测试F/I与diff通过。生产预留写入不再携带闲置到期字段，订单expires_at/宽限退出时间没有混删；候选表对此无原分支命中，不虚增核销数。后续预留版本检查是否与Book事务的事实版本校验重复，需要继续核对真实调用/并发契约，全系统审核尚未完成，未提交推送部署。

事务预留去重复版本门禁：Book._act_with_transaction先锁Head并核对revision与projection_version，_act_mutating再按当前视图检查请求token；save_reservations_in_session又比较活动预留旧版本并查询历史latest，可能把不同事实版本下仍合法的预留误判。删除事务内39行重复检查及无活动预留时的额外SELECT，保留写入版本来源元数据、Head/请求事实版本校验、身份去重和实际批次/总量容量。新增真实Postgres用例证明pv_1/pv_2两个各0.25预留能共存且总量0.5，事务集成文件13通过，F/I与diff通过，完整回归执行中；独立非事务旧版本解析路径仍需另行核查，未称全仓版本校验统一。

事务预留39行重复版本关卡删除后的最终完整回归3136通过、1项缺实盘采集环境跳过、2既有警告；基线unit/smoke/e2e2961、Postgres集成175。新增跨版本活动预留用例覆盖旧分支误拒场景，同时空/不足批次容量拒绝回滚用例仍通过；全src/修改测试F/I与diff通过。独立仓库非事务写入中旧版本检查仍存在，需继续与其实际调用约束核查；全系统兼容/兜底审核尚未完成，未提交推送部署。

独立预留版本兼容删除：PositionLedger当前版本是pv_<事实hash>，不是数字序号；删除独立save_reservations中旧活动行版本相等检查/历史latest查询，以及_parse_version_rank/_is_stale_projection_version两函数，共56行。上层Book请求视图检查与事务Head校验保留；仓库幂等身份、批次容量、真实持仓快照总量保护保留。跨版本预留Postgres回归扩展到事务/独立两路径，独立路径种真实仓位快照而非mock容量，集成文件14通过，F/I与diff通过，完整回归执行中。

独立预留56行旧版本判断删除后的最终完整回归3137通过、1项缺实盘采集环境跳过、2既有警告；unit/smoke/e2e2961、Postgres集成176。全src及修改测试F/I与diff通过，预留仓库不再解析数字版本或要求历史活动预留版本等于当前版本。下一项装配核查：生产OrderExecutionCoordinator仅live执行装配1处，已经显式execution_book，构造器仍可自行创建无事务Book及同时接受重复依赖；83项测试构造需按正式依赖改写，尚未修改，不称完成。全系统兼容/兜底审核继续，未提交推送部署。

协调器装配去备选路径：OrderExecutionCoordinator唯一生产装配已传Book，execution_book改为必填，删除自动创建Book、domain_coordinator重复构造输入和initial_reservations构造导入。初始预留恢复由Book/仓库负责；18测试文件中的83个具名构造及2个旧别名漏传调用改为显式创建Book，2处初始预留场景在测试中明确注册，不添加生产兼容工厂。预留仓库字段仍参与真实预留与持久化判断，不混删。相关69项通过，全src/tests Ruff F/I及diff通过，完整回归执行中。

协调器Book显式装配后的最终完整回归3137通过、1项缺实盘采集环境跳过、2既有警告。全src/tests Ruff F/I及diff通过；AST核对src/tests中所有OrderExecutionCoordinator及旧测试别名直接构造均显式传Book，domain_coordinator/initial_reservations旧构造输入为0。生产唯一装配仍使用已恢复的持久化Book；预留仓库字段当前仍作为实际预留与持久化条件输入，后续需继续收敛，不冒称重复依赖已全删。全系统兼容/兜底审核尚未完成，未提交推送部署。

协调器重复预留依赖删除：原仓库字段只用作是否配置的布尔条件，真实读写在Book，公开reservation_repository getter全src/tests无消费者。删除协调器构造参数、字段及getter，Book暴露实际配置的has_reservation_repository供2个判断使用，生产装配只传Book；测试不再分别注入两份可能不一致的仓库，旧Book空配置而协调器单独传仓库的6个测试改为Book持有正式异步内存仓库。相关145通过（修正测试导入别名后消除4个新收集警告），全src/tests F/I通过；完整回归执行中，不宣称布尔判断本身删除。

协调器预留依赖归Book后的最终完整回归3137通过、1项缺实盘采集环境跳过、2既有警告；没有新增收集/未清理任务警告。全src/tests Ruff F/I及diff通过；AST核对src/tests协调器直接构造中重复reservation_repository输入为0，生产装配Book仍连接实际AsyncPostgres预留仓库和执行事务。全系统其他兼容/兜底候选继续审核，未提交推送部署。

状态机回调装配去懒状态：唯一生产StateMachine装配已经拥有交易所/回调，set_exchange_boundary_callbacks为正式同步接口；移到构造时配置，删除_exchange_configured字段及每次submit前重复判断。真实发包/响应回调、预提交预期仓位登记、网络未知单归因保留。事实提交并发测试由裸object交易所改为正式FakeExchange；恢复失败测试核查submit/query/cancel网络接口未调用，允许本地回调装配。相关30通过，全src/tests F/I与diff通过，完整回归执行中。

状态机回调装配去懒状态后的最终完整回归3137通过、1项缺实盘采集环境跳过、2既有警告；首次全量发现4项旧裸object/不完整交易所替身，改为正式autospec或声明回调接口后完整复验。全src/tests F/I与diff通过，源码已无_exchange_configured状态。候选清单按现态AST复核无新增原表达式消失条目，不虚增核销数。已核对生产提交走prepare_and_execute，而状态机/协调器仍保留prepared_submission=None的分拆落库旧入口，需下一步统一提交契约与测试，全系统审核尚未完成，未提交推送部署。

提交契约统一预落库：唯一生产提交走prepare_and_execute和已有原子准备事务；StateMachine/Coordinator.submit及执行端口prepared_submission设为必填，删除StateMachine独立保存计划/补写SUBMITTING旧分支、Coordinator不带预落库转发分支。StateMachine删除OrderPlanRepository协议/构造依赖，执行装配删除order_repository输入，编排不再传闲置仓库。网络/调度测试在测试专用fixture明确构造预落库输入，不添加生产兼容默认；相关88通过，全src/tests F/I与diff通过，完整回归执行中。遗留Postgres计划仓库及装配是否可完全删需另行核查。

提交契约只接受预落库结果后的最终完整回归3136通过、1项缺实盘采集环境跳过、2既有警告；unit/smoke/e2e2960、Postgres集成176。首轮全量发现3处仅导入_machine的E2E旧调用，改为显式预落库输入；SIGTERM用例在POST前明确安排已保存计划/SUBMITTING事实，再验证重启只查询不重发。删除1项只验证已移除StateMachine计划写入分支的旧数据库失败测试，原子UoW SQL失败不发布/准备事务测试保留。全src/tests F/I与diff通过，StateMachine计划仓库依赖及提交None兜底已无；旧Postgres计划仓库装配残留尚需清理，全系统审核继续，未提交推送部署。

旧计划仓库及装配删除：提交契约已统一PreparedOrderSubmission，PostgresOrderPlanRepository无生产消费者，仅LiveRepositories闲置字段/创建和数据库测试初始化。删除生产模块、装配字段/创建/导入及编排测试无用替身；删除两项只验证旧计划+意图分拆写入事务的测试。数据库读/事件/恢复测试改用tests.fixtures.order_rows.OrderRows.seed_order，直接插入测试订单初态，不保留旧upsert/意图状态更新生产逻辑或旧名别名；旧StateMachine假仓库planned方法/计数也删除。全src/tests F/I与diff通过，完整回归执行中。

旧计划仓库与装配残留删除后的最终完整回归3134通过、1项缺实盘采集环境跳过、2既有警告；unit/smoke/e2e2958、Postgres集成176。全src/tests F/I与diff通过；src无PostgresOrderPlanRepository/order_plan_repository/save_planned_order引用，读取/恢复数据库测试使用简单订单行数据夹具，未把旧业务事务搬进测试。全系统其他兼容/兜底候选继续语义审核，未提交推送部署。

Live执行装配提交仓库必填：唯一生产build_live_execution_runtime调用已明确传submission_repository，去掉工厂None/default契约，恢复装配测试arguments显式提供仓库。执行装配/编排生命周期/数据库装配13通过，全src/tests F/I和diff通过；本轮未重复完整回归，上一源码基线完整3134通过。configure_submission仍由daemon真实调用并注入clock，不按无调用删除；需继续统一时钟/装配后再移除后配置状态。另确认预留expected_projection_version只剩参数/写入无读者，后续需与请求正式版本区分后删除，不混删Book/TradeCommand的真实版本校验。全系统兼容/兜底审核尚未完成，未提交推送部署。

### 预留版本元数据去除

删除预留接口、ExecutionRequest、事务透传及 PositionReservationRow 的 expected_projection_version 映射；该字段已没有读取者。保留 TradeCommand 的真实版本校验、Book 的视图令牌检查及批次容量锁定。历史数据库可空列不执行破坏性删除。删除仅断言版本透传的旧测试，保留两个 PostgreSQL 批次容量共用场景。全量回归：3133 passed、1 skipped、2 个既有警告；src/tests 的 Ruff F/I 与 git diff --check 通过。

### 超时钟去除失效计时兜底

StreamAvailabilityClock 中断计时改由进入 DISRUPTED 状态初始化，删除已处于该状态却缺少起始时间时回退当前时间的不可到达分支，以及三项无生产读者的配置/名称/历史就绪属性。保留启动、中断和跨重连恢复的独立预算。现有行为测试补充重复中断通知不会重置预算、恢复后再次断开获得新预算。启用网络测试后相关模块 727 passed，1 个既有警告；行情池未配置分支仍有生产装配路径，因此保留。

### 决策命令恢复与采集接口统一

决策退出命令按唯一写入器的正式 payload 读取，删除缺失字段补零、补空字符串及错误形状当作无分配计划的兼容分支。明确保存的 None 仍保留语义；新增完整退出命令恢复及残缺价格/计划字段拒绝测试。该阶段全量回归 3135 passed、1 skipped、2 个既有警告。随后采集 EnvelopeRecovery 契约加入正式 bypass_network 参数，删除 TypeError 后使用旧签名再次调用的分支；新增内部 TypeError 不重复执行且队列完成记账的回归，采集协调器与真实恢复器 23 passed。Ruff F/I 与 diff 检查通过。

### 无用参数与旧账户轮询移除

删除仓位可见性判断中的未使用 fill_times 参数、正式时间字段的重复类型兜底；相关 54 项回归通过。删除未知退出恢复的未使用 context 参数及反查前无用的上下文读取，保留反查后获取最新上下文；删除缓存清理 latest_bucket、Book 状态更新 previous、账户快照 symbols 无用参数。该阶段全量 3136 passed、1 skipped、2 个既有警告。随后删除无生产调用的 ContinuousAccountSyncDaemon/Config/Result 及两项专属旧测试，并清理不生效的账户 sync CLI interval_seconds/fill_interval_seconds 和两个 Compose 参数。实际 UserDataAccountSyncDaemon、历史成交补查、持久化与留存不变；相关 42 项测试通过，CLI 帮助/正式参数与两份 Compose 命令核验通过，Ruff F/I 与 diff 检查通过。

### 准备入口与检查点序号收敛

删除 prepare CLI 中未使用的 migration_revision 选项，实际参数表核验通过。策略检查点 signal_sequence 只接受正式写入器保存的非负整数；删除缺字段默认零、字符串转整数、异常归零与负数截零，避免恢复时悄悄重置信号身份。首次运行仍由新运行时正常初始化为零。坏检查点在改动运行时状态前拒绝；5 个回归覆盖缺字段、负值、字符串、None 和 bool。完整工作区回归 3139 passed、1 skipped、2 个既有警告；src/tests Ruff F/I 与 git diff --check 通过。

### 策略运行契约单一化

删除运行清单遇到未知策略时返回 unset 哈希的兜底，转换为明确 RuntimeManifestError。策略工厂 RuntimeStrategyProtocol 与实盘 LiveRuntimeStrategy 重复协议合并为 domain/strategy/runtime.py 的 RuntimeStrategy；工厂、行情装配、守护程序、行情循环、缓存、启动恢复直接引用，无旧名称兼容别名。共享契约仅依赖领域模型与标准库，不引用策略实现、实盘或持久化。相关 143 项回归通过，全量 3140 passed、1 skipped、2 个既有警告；整理导入及强化环境覆盖测试后相关 16 项复核通过，Ruff F/I 与 diff 检查通过。

### 内部映射重复兜底去除

仓位分类入口已归一化 identity_events 与 fill_quantities，内部 _normalise_position_orders 改为必需 Mapping，移除重复 None 分支与逐行空字典替代。缓存 volume_metrics_provider 正式返回 dict，移除重复 or {}；保留指标故障隔离。相关 53 项测试、src/tests Ruff F/I 与 diff 检查通过。后续扫描确认风险配置的行情/账户最大年龄字段只做存储和透传，没有风控读取者，需要继续核对数据库非空列与配置哈希后处理，未将其误报为已完成。
