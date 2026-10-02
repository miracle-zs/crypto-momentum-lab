# 后端完整架构全景文档

归档日期：2026-10-02。依据当前工作区源码，基线提交 `72d40fd4`。本文描述架构与事实契约，不代表实盘健康检查或性能测量结果。图中省略内部算法、日志与遥测调用。

## 1. 静态组件图

箭头表示代码依赖：调用者或引用者指向被依赖模块。Apps 包括 CLI 组合根，以及 `live_rollout`、`strategy_runner` 等应用运行时；这不是把这些运行时物理归入 `apps` 包。

```mermaid
graph TD
    subgraph Apps[Apps：组合根与应用编排]
        CLI[apps：CLI / 服务启动]
        Live[live_rollout：实时交易编排]
        Runner[strategy_runner：策略运行与回放]
        CLI --> Live
        CLI --> Runner
    end
    subgraph MD[Market Data]
        Market[行情归一化 / 聚合 / Hub / Candle / EMA]
    end
    subgraph ST[Strategies]
        Strategy[策略工厂 / 策略实现]
    end
    subgraph RK[Risk]
        Gateway[RiskGateway]
    end
    subgraph EX[Execution]
        Account[execution_account：账户同步 / 用户流 / Hub]
        Coordinator[OrderExecutionCoordinator]
        Machine[OrderExecutionStateMachine]
        Client[BinanceUsdMTradeClient]
        Coordinator --> Machine
        Machine --> Client
    end
    subgraph PS[Persistence]
        Store[Postgres 仓储 / Execution UoW / 文件存储]
    end
    subgraph DM[Domain]
        Contracts[行情 / 策略 / 风控 / 订单事实与端口]
        Book[ExecutionBook / AccountJournal / PositionBook]
        Book --> Contracts
    end
    CLI --> Account
    CLI --> Store
    Live --> Market
    Live --> Strategy
    Live --> Gateway
    Live --> Coordinator
    Live --> Account
    Live --> Store
    Live --> Book
    Runner --> Market
    Runner --> Strategy
    Runner --> Contracts
    Market --> Contracts
    Strategy --> Contracts
    Gateway --> Contracts
    Account --> Contracts
    Coordinator --> Book
    Coordinator --> Contracts
    Machine --> Contracts
    Client --> Contracts
    Store --> Contracts
```

应用层组装具体仓储与客户端，并将其注入领域端口。执行层经端口访问仓储，因此图中没有 `Execution → Persistence` 的具体包依赖；Persistence 实现 Domain 定义的契约。源码聚合导入检查显示，其余五个业务/基础设施模块均依赖 Domain，Domain 没有反向引用这些模块。

`live_rollout` 直接引用 `strategies` 和 `market_data`，不再经 `strategy_runner` 获取策略工厂或行情设施。`ExecutionBook` 位于 Domain，负责账本不变式；应用层负责它的生命周期与基础设施装配。

## 2. 三大核心时序

### 2.1 场景 A：行情触发到报单确认

```mermaid
sequenceDiagram
    participant Loop as LiveMarketLoop
    participant Strategy as Strategy
    participant Lane as EntryExecutionLane
    participant Submission as LiveCandidateSubmission
    participant Risk as RiskGateway
    participant Planner as TradeCommandExecutor
    participant Coord as OrderExecutionCoordinator
    participant Book as ExecutionBook
    participant Admission as LiveSubmissionAdmission
    participant Repo as OrderSubmissionRepository
    participant Machine as OrderExecutionStateMachine
    participant Client as BinanceUsdMTradeClient
    Loop->>Strategy: on_market_state()
    Strategy-->>Loop: StrategyDecision
    Loop->>Lane: process()
    Note over Lane: 池过滤、就绪检查、开仓准入与熔断
    Lane->>Submission: execute()
    Submission->>Risk: evaluate()
    Risk-->>Submission: RiskEvaluation
    alt 风控拒绝或前置条件不满足
        Submission-->>Lane: 无订单提交结果
    else 风控批准
        Submission->>Planner: plan_execution()
        Planner-->>Submission: OrderExecutionPlan
        Submission->>Coord: prepare_and_execute()
        Note over Coord: 按账户、环境、交易对、持仓方向排队后出队
        Coord->>Book: read() / act()
        Book-->>Coord: 接受命令与 Outbox
        Coord->>Book: 标记 DISPATCHING
        Coord->>Admission: rejection_reason()
        Admission-->>Coord: 最终准入结果
        alt 最终准入拒绝
            Coord->>Book: mark_rejected()
            Coord-->>Submission: 无提交结果
        else 最终准入通过
            Coord->>Repo: prepare_submission()
            Note over Repo: 一个事务检查 fencing / 限额 / 唯一性<br/>写入 intent、order、SUBMITTING 事件
            Repo-->>Coord: 提交事务后返回 PreparedOrderSubmission
            alt 仓储拒绝准备
                Coord->>Book: mark_rejected()
                Coord-->>Submission: 无提交结果
            else 准备成功
                Coord->>Machine: submit()
                Machine->>Client: submit_order()
                Note over Machine,Client: 交易所网络请求发生在准备事务之外
                Client-->>Machine: ExchangeOrderSnapshot
                Machine->>Repo: 经订单事件/成交仓储端口持久化回执
                Machine-->>Coord: OrderExecutionResult
                Coord->>Book: 观察订单结果并更新命令状态
                Coord-->>Submission: OrderExecutionResult
                Submission-->>Lane: 提交结果
            end
        end
    end
```

策略评估发生在 Lane 之前；Lane 负责开仓管道控制，Submission 负责风控与命令组装，Coordinator 负责串行执行和出队后的最终准备。Coordinator 不回调 Submission。

图中的 Repo 汇总显示准备、事件及成交仓储端口，实际是不同接口。Book 命令接受事务与订单准备事务也不是同一个全局事务。准备与网络发单处于同一队列任务，同一执行键的撤单/对账任务不能插入两者之间；这不等于所有账户或进程全局串行化。

正常主调用路径为 `Loop → Lane → Submission → Coordinator → StateMachine → Client`，共六个对象、五次跨对象主链调用；策略、风控、规划、准入和仓储是旁支调用，不应把所有箭头相加当作栈深或性能结论。交易所 NEW 被规范化为 ACKNOWLEDGED；报单确认不等于真实成交入账。发包超时进入同一 `client_order_id` 的查询/对账流程，不自动重发 POST。

### 2.2 场景 B：WebSocket 成交事件到 ExecutionBook

```mermaid
sequenceDiagram
    participant WS as Binance 用户数据 WebSocket
    participant Sync as UserDataAccountSyncDaemon
    participant Service as ExecutionAccountSyncService
    participant Hub as AccountEventHub
    participant Runtime as LiveAccountEventRuntime
    participant Recon as LiveOrderReconciliation
    participant Coord as OrderExecutionCoordinator
    participant Book as ExecutionBook
    participant UoW as ExecutionUnitOfWorkPort
    participant Context as 事实上下文与退出通道
    WS->>Sync: 订单 / 成交事件
    Sync->>Service: record_user_data_event()
    Service-->>Sync: 原始用户流日志持久化完成
    Sync->>Sync: 更新账户内存状态并规范化事实
    Sync->>Hub: 发布 AccountEvent
    Note over Sync,Service: 账户快照与成交表的物化写入由后台队列继续处理
    Hub->>Runtime: 传递同一事件信封
    Runtime->>Recon: reconcile_account_event()
    Recon->>Coord: apply_observed_snapshot()
    Note over Recon,Coord: 已知订单更新订单事件；不完整事件请求 REST 恢复
    Runtime->>Coord: observe_account_snapshot()
    Coord->>Book: observe()
    Book->>UoW: 事务内校验并保存事实、账本 head 与相关命令状态
    UoW-->>Book: commit
    Book->>Book: 发布新的内存账本视图
    Book-->>Coord: 观察结果
    Coord-->>Runtime: 账本处理结果
    Runtime->>Context: 绑定流版本、发布上下文、通知退出评估
    Note over Book,Context: 可交易视图需满足覆盖、版本与持仓一致性约束
```

生产端先保存原始用户流日志，再发布规范化 `AccountEvent`；Hub 发布不等待账户物化表全部写完。消费端先处理订单状态，再提交账户真实成交事实到 Book。`AccountJournal` 保存事实，`PositionBook` 推导持仓视图，`ExecutionBook` 统一控制接受、持久化与发布。

订单累计成交数量与账户成交事实用途不同：前者确认订单状态，后者用于批次、成本和持仓账本，不能仅凭订单回执制造成交事实。事件信封保留流 epoch 与 sequence；瞬态失败重试同一信封，证据冲突或覆盖不足仍会限制可交易性。

### 2.3 场景 C：崩溃重启后的恢复与对账

```mermaid
sequenceDiagram
    participant Apps as Apps / RuntimeOrchestrator
    participant Store as Postgres 仓储与 UoW
    participant Book as ExecutionBook
    participant Sync as 账户同步服务
    participant REST as Binance 私有 REST
    participant Hub as AccountEventHub
    participant Recon as LiveOrderReconciliation
    participant Coord as OrderExecutionCoordinator
    participant Repair as 持仓修复 / 退出回执恢复
    participant Gate as 交易就绪门禁
    Apps->>Gate: 启动期间限制交易
    Apps->>Book: restore()
    Book->>Store: 读取 head、checkpoint、事实后缀、命令与预留
    Store-->>Book: 持久化恢复状态
    Note over Apps,Book: Book 恢复失败时不暴露发单 Coordinator
    Apps->>Store: 恢复策略检查点、策略状态与待处理退出事实
    Apps->>Sync: 以可信成交恢复锚点启动
    Sync->>REST: 读取账户、持仓、挂单与分页成交历史
    REST-->>Sync: 账户快照及成交覆盖证据
    Sync->>Hub: 发布快照、成交与扫描证据；启动用户流
    Hub->>Coord: 通过账户运行时提交恢复事实
    Coord->>Book: observe()
    Book->>Store: 验证锚点与覆盖后事务提交
    Apps->>Recon: reconcile_all(include_confirmed=True)
    Recon->>Store: 加载需恢复的订单
    loop 每个待对账订单
        Recon->>Coord: reconcile_order()
        Coord->>REST: 经 StateMachine / Client 按 client_order_id 查询
        alt 查到订单
            REST-->>Coord: 规范化订单快照
            Coord->>Store: 持久化订单事实
            Coord->>Book: 更新命令观察状态
        else 订单未找到或结果不明确
            Coord->>Store: 保持 UNKNOWN_PENDING_RECONCILIATION
            Recon->>Repair: 请求独立恢复 / 缺席证明
        end
    end
    Repair->>Book: repair_position(request, uow)
    Note over Book,Store: 修复读取、事务提交、定向重载发布共享 Book 写锁
    Book->>Store: 按 PositionKey 读取、修复与 load_position()
    Repair->>Coord: 证据充分时消解未知退出结果
    Note over Repair,Coord: 单次未找到不是 ABSENT_RECONCILED 的充分条件
    Apps->>Gate: 检查 lease、fencing、流版本、账本与策略预热
    Gate-->>Apps: 满足条件后开放相应交易范围
```

图把账户生产服务与策略交易进程的协作按逻辑顺序展开，具体部署可分进程运行。恢复不是简单加载最后一条账户快照：可信检查点、成交覆盖、流版本、未完成命令与资产预留必须一起校验。普通零持仓快照不能直接抹掉既有持久化账本。

“准备事务已提交、POST 尚未发送”与“POST 已发送、响应丢失”在重启时都可能表现为 SUBMITTING/未知状态。恢复依据同一 `client_order_id` 查询，证据充分才消解；不能盲目重新发单。健康 WebSocket 期间也保留周期性完整成交扫描，补齐乱序或遗漏事实。

## 3. 订单生命周期状态机

下图包含业务阶段与代码订单状态。中文业务节点不是新增持久化枚举；主链的原子准备直接写入 SUBMITTING，INTENT_APPROVED、CLAIMED、PLANNED 属于契约中保留的阶段，并非本链路每次必经。

```mermaid
stateDiagram-v2
    state "创建候选意图（业务阶段）" as Candidate
    state "排队等待（业务阶段）" as Queued
    state "不发单结束（业务阶段）" as NoSubmit
    state "系统异常暴露并等待恢复（业务阶段）" as Failure
    [*] --> Candidate
    Candidate --> INTENT_APPROVED: 风控批准
    Candidate --> NoSubmit: 风控拒绝 / 意图过期 / 门禁拒绝
    INTENT_APPROVED --> Queued: 完成计划组装并入队
    INTENT_APPROVED --> CLAIMED: 分阶段契约：领取意图
    CLAIMED --> PLANNED: 分阶段契约：持久化计划
    PLANNED --> Queued: 等待执行
    Queued --> NoSubmit: 出队准入拒绝 / 唯一性或 fencing 不通过
    Queued --> SUBMITTING: 出队后准备事务提交
    Queued --> Failure: 系统异常且尚未发包
    PLANNED --> SUPPRESSED: shadow 策略禁止网络提交
    SUBMITTING --> ACKNOWLEDGED: 交易所接受；NEW 规范化
    SUBMITTING --> SUBMITTED: 契约保留的提交状态
    SUBMITTED --> ACKNOWLEDGED: 收到确认事实
    SUBMITTING --> PARTIALLY_FILLED: 回执或后续事实显示部分成交
    SUBMITTING --> FILLED: 回执或后续事实显示全部成交
    SUBMITTING --> REJECTED: 明确拒单 / 已知发包前失败
    SUBMITTING --> CANCELED: 查询或回执确认已取消
    SUBMITTING --> EXPIRED: 查询或回执确认已失效
    SUBMITTING --> UNKNOWN_PENDING_RECONCILIATION: 响应超时 / 崩溃后结果未知
    ACKNOWLEDGED --> PARTIALLY_FILLED: 收到部分成交事实
    ACKNOWLEDGED --> FILLED: 收到全部成交事实
    PARTIALLY_FILLED --> PARTIALLY_FILLED: 后续部分成交
    PARTIALLY_FILLED --> FILLED: 累计全部成交
    ACKNOWLEDGED --> CANCELING: 发起撤单
    PARTIALLY_FILLED --> CANCELING: 发起剩余量撤单
    CANCELING --> CANCELED: 交易所确认取消
    CANCELING --> FILLED: 撤单竞争期间已全部成交
    CANCELING --> PARTIALLY_FILLED: 查询显示仍部分成交且未终结
    CANCELING --> UNKNOWN_PENDING_RECONCILIATION: 撤单结果未知
    ACKNOWLEDGED --> CANCELED: 用户流 / 对账确认取消
    PARTIALLY_FILLED --> CANCELED: 剩余量取消
    ACKNOWLEDGED --> EXPIRED: 交易所确认失效
    PARTIALLY_FILLED --> EXPIRED: 剩余量失效
    UNKNOWN_PENDING_RECONCILIATION --> ACKNOWLEDGED: 查询确认仍挂单
    UNKNOWN_PENDING_RECONCILIATION --> PARTIALLY_FILLED: 查询确认部分成交
    UNKNOWN_PENDING_RECONCILIATION --> FILLED: 查询确认全部成交
    UNKNOWN_PENDING_RECONCILIATION --> CANCELED: 查询确认已取消
    UNKNOWN_PENDING_RECONCILIATION --> REJECTED: 获得明确拒绝事实
    UNKNOWN_PENDING_RECONCILIATION --> EXPIRED: 获得明确失效事实
    UNKNOWN_PENDING_RECONCILIATION --> ABSENT_RECONCILED: 独立缺席证明充分
    UNKNOWN_PENDING_RECONCILIATION --> UNKNOWN_PENDING_RECONCILIATION: 查询无结果或证据不足
    Failure --> UNKNOWN_PENDING_RECONCILIATION: 已有持久化订单且需恢复
    Failure --> NoSubmit: 确认未发包且完成命令拒绝收敛
    NoSubmit --> [*]
    FILLED --> [*]
    CANCELED --> [*]
    REJECTED --> [*]
    EXPIRED --> [*]
    SUPPRESSED --> [*]
    ABSENT_RECONCILED --> [*]
```

这是按事件与恢复条件整理的生命周期视图，不表示代码有一张强制执行所有边的集中转移表。没有通用持久化 `ABORTED` 状态；明确未发包的失败收敛为不发单/拒绝，不能确定是否发包的失败必须保留未知状态。ABSENT_RECONCILED 表示经过独立证明消解缺席，不表示交易所发回取消回执。

ExecutionBook 的命令 Outbox 使用独立 `DispatchState`：

| 状态 | 含义 |
| --- | --- |
| PREPARED | 命令已被 Book 接受 |
| DISPATCHING | 出队开始执行，随后进行最终准入和仓储准备 |
| ACKNOWLEDGED | 已观察到命令对应的接受结果 |
| REJECTED | 命令被明确拒绝 |
| UNKNOWN | 执行结果未确定，需要恢复 |
| TERMINAL | 命令已观察到终态 |

订单终态、Outbox 终态与持仓账本成交覆盖是三个不同条件；退出预留与恢复门禁不能仅凭一个状态字符串清除。

## 4. 核心领域模型

业务名称与源码对应：TradeIntent = `OrderIntentCandidate`；ExecutionPlan = `OrderExecutionPlan`；Order = `PersistedExchangeOrder`；Position 的交易读取契约 = `PositionView`，其成交周期与批次由 `PositionEpisode`、`PositionLedgerBatch` 描述。下图使用实际类名，并列出架构相关字段，完整字段以源码为准。

```mermaid
classDiagram
    class OrderIntentCandidate {
        <<value object>>
        +str candidate_id
        +str signal_id
        +str run_id
        +str strategy_name
        +str strategy_version
        +str config_hash
        +str symbol
        +StrategySide side
        +EntryType entry_type
        +Decimal limit_price_optional
        +Decimal desired_notional_optional
        +bool reduce_only
        +datetime created_at
        +datetime expires_at
    }
    class OrderExecutionPlan {
        <<value object>>
        +str intent_id
        +str run_id
        +str client_order_id
        +str symbol
        +str side
        +str order_type
        +Decimal quantity
        +Decimal price_optional
        +bool reduce_only
        +FuturesPositionSide position_side
        +bool quantized
        +str time_in_force_optional
        +datetime expires_at_optional
        +str projection_version_optional
        +tuple allocations
    }
    class PersistedExchangeOrder {
        <<order read model>>
        +OrderExecutionPlan plan
        +ExchangeOrderState state
        +str exchange_order_id_optional
        +Decimal executed_quantity
        +datetime updated_at
    }
    class ExchangeOrderSnapshot {
        <<normalized exchange fact>>
        +str client_order_id
        +str exchange_order_id
        +ExchangeOrderState state
        +Decimal executed_quantity
        +Decimal average_price
        +datetime observed_at
        +tuple fills
    }
    class PositionKey {
        <<value object>>
        +str environment
        +str account_label
        +str symbol
        +FuturesPositionSide position_side
    }
    class PositionView {
        <<immutable projection>>
        +PositionKey key
        +str projection_version
        +int input_revision
        +datetime event_cut_optional
        +PositionEpisode active_episode_optional
        +tuple batches
        +Decimal unallocated_quantity
        +tuple reservations
        +PositionHealthStatus health_status
        +str reconciliation_status
        +AccountFactStreamScope stream_scope_optional
        +Decimal total_quantity_computed
    }
    class PositionEpisode {
        +str episode_id
        +PositionKey position_key
        +datetime opened_at
        +datetime closed_at_optional
        +bool is_active
        +Decimal cumulative_bought
        +Decimal cumulative_sold
        +tuple batches
    }
    class PositionLedgerBatch {
        +str batch_id
        +str episode_id
        +Decimal quantity
        +Decimal original_quantity
        +Decimal entry_price
        +datetime opened_at
        +str client_order_id_optional
        +bool is_external
    }
    class TradeCommand {
        <<value object>>
        +str command_id
        +PositionKey position_key
        +TradeCommandType command_type
        +Decimal requested_quantity
        +bool reduce_only
        +str expected_projection_version_optional
        +str reservation_id_optional
        +str idempotency_key_optional
    }
    class PositionReservation {
        +str reservation_id
        +str command_id
        +PositionKey position_key
        +str batch_id
        +Decimal reserved_quantity
        +Decimal consumed_quantity
        +Decimal released_quantity
    }
    class OutboxEntry {
        +str command_id
        +str request_id
        +TradeCommand command
        +DispatchState state
        +int attempt_count
        +str external_order_id_optional
        +str last_error_optional
    }
    class AccountJournal {
        <<account fact journal>>
    }
    class PositionBook {
        <<position projection>>
    }
    class ExecutionBook {
        <<stateful domain service>>
        +read()
        +act()
        +observe()
        +restore()
        +reload_position()
        +repair_position()
        +get_active_reservations()
    }
    OrderIntentCandidate ..> OrderExecutionPlan : 风控批准后规划
    TradeCommand ..> OrderExecutionPlan : 转换为报单计划
    PersistedExchangeOrder *-- OrderExecutionPlan : 保存计划
    ExchangeOrderSnapshot ..> PersistedExchangeOrder : 更新订单事实
    OrderExecutionPlan ..> PositionView : projection_version 校验
    PositionView --> PositionKey
    PositionView --> PositionEpisode
    PositionView o-- PositionLedgerBatch
    PositionEpisode o-- PositionLedgerBatch
    TradeCommand --> PositionKey
    OutboxEntry *-- TradeCommand
    PositionReservation --> TradeCommand : command_id
    PositionReservation --> PositionLedgerBatch : batch_id
    PositionView o-- PositionReservation
    ExecutionBook *-- AccountJournal
    ExecutionBook *-- PositionBook
    ExecutionBook o-- OutboxEntry
    ExecutionBook o-- PositionReservation
    PositionBook ..> PositionView : 提供读取视图
    AccountJournal ..> PositionBook : 成交事实推导
```

字段名中的 `_optional`、`_computed` 是图示标记，不是源码字段名。`Decimal` 表达数量与价格；报单计划在进入执行时必须已量化。`client_order_id` 是稳定的订单查询与幂等键，`projection_version` 防止依据过时位置决策，`PositionKey` 隔离环境、账户、交易对与持仓方向。

命令与事实载体主要使用 `frozen=True` dataclass；但 frozen 只冻结属性绑定，包含 dict/Mapping 的字段不自动获得深度不可变性。ExecutionBook 是有状态领域服务，维护事实、视图、预留和 Outbox，不应标为不可变实体。

Binance 专有状态先规范化为 `ExchangeOrderSnapshot` / 账户事件，再进入状态机与 Book。当前契约没有名为 `OrderAck` 或 `OrderReceipt` 的返回类，文档不引入这些不存在的类型。

## 源码索引

- 组合与主流程：`apps/`、`live_rollout/runtime_orchestrator.py`、`live_rollout/market_loop.py`、`entry_lane.py`、`submission.py`、`execution_runtime.py`。
- 执行与交易所隔离：`execution_account/orders/coordinator.py`、`state_machine.py`、`trade_command_executor.py`、`execution_account/binance/client.py`、`order_status.py`。
- 账户事实与恢复：`execution_account/daemon.py`、`user_data_sync.py`、`sync.py`、`live_rollout/account_channel.py`、`order_reconciliation.py`、持仓恢复/修复模块。
- 领域契约：`domain/strategy/models.py`、`domain/execution/order_state.py`、`execution_book.py`、位置、命令、预留与 Outbox 模型。
- 事务实现：`persistence/postgres/order_submission_repository.py`、Execution UoW、账户日志与持仓检查点仓储。
