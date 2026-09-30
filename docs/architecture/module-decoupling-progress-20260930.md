# 模块解耦实施进度（2026-09-30）

实施基线：`8eb059d`。本次完成第一批“决策契约、订单事实与分类输入解耦”，尚未完成审计文档中的全部重构。本批未发布到生产，生产运行版本仍为 `259c8e0`。

## 已实施

| 原有依赖 | 改动后的接口与归属 |
| --- | --- |
| decision_facts 直接依赖 AsyncPostgresDecisionUnitOfWork 与 persistence 中定义的提交类型 | `domain/decision/commit_models.py` 定义 DecisionCommit、DecisionCommitReceipt、DurablePolicySnapshot；`domain/decision/ports.py` 定义消费者实际使用的五项 UoW 能力。运行装配仍负责注入 Postgres 实现，业务测试按 Protocol 创建替身。 |
| 恢复订单 DTO 定义在聚合仓储，运行上下文与订单用例都因此导入 Postgres | PersistedExchangeOrder 移至 `domain/execution/order_read_models.py`，所有仓储、用例、装配及测试调用点同步切换，原仓储和包门面不保留旧 DTO 重导出。 |
| 分类、身份解析直接使用 ORM 行，order_facts_loader 混合时间规则与 SQL 查询 | OrderObservation、OrderIdentityEvent、PositionObservation 为不包含 ORM 的数据值；数据库与账户事件两条上下文路径都在适配处转换。SQL、ORM 映射和完整账户成交加载位于 `persistence/postgres/order_identity_repository.py`；live_rollout/order_facts_loader 仅保留纯时间范围计算。 |
| 决策构造器保留丢弃不用的 trace_repository、retention_authority；退出处理器为 Any 并探测是否协程 | 删除无效参数，明确异步 ExitDispatchHandler 与结果接口，直接 await；现有异步运行装配和测试同步迁移。 |
| Dashboard 包急切重导出 API，queries 导入包时回到 API；查询协议位于 HTTP 文件 | 包初始化不加载 HTTP 应用，应用工厂从 api 模块显式导入；查询协议移至 ports.py。 |
| 导入 strategy_runner 纯持仓计算会加载 Postgres 和数据集适配器 | 取消包门面的急切重导出，CLI 与全部仓库内调用点从功能所属模块显式导入。 |

契约迁移没有增加第二条提交路径或旧签名转发层。Postgres 原子决策提交实现仍在同一 session/事务内保存 policy state、trace、依赖与 exit outbox；policy revision、摘要检查、重放幂等、同步提交及失败回滚逻辑未拆开。订单身份加载仍保留双轨查询：已知 order ID 精确查询不受时间窗口限制，symbol 扫描使用有界时间范围；完整账户成交与订单状态不相互伪造。

构造器和包导入路径发生明确变化，所有仓库内调用点已迁移。没有数据库结构或数据迁移，没有改变实盘审批、风险限额和持仓事实。

## 验证证据

- 初始决策、退出生命周期及 Dashboard 相关测试基线：175 passed。
- 最终 `rtk proxy .venv/bin/python -m pytest -q tests/unit tests/smoke/test_deployment_script.py`：**2052 passed，4 skipped**（默认关闭的网络测试），20.15 秒。
- 开启 `CML_RUN_HUB_NETWORK_TESTS=1` 后，账户与行情 hub 两个文件：**49 passed**，其中包含上述四个默认跳过的测试。
- 新增独立 Python 进程导入检查：禁止导入 sqlalchemy 和整个 persistence 包时，decision_facts、position_classification、Dashboard ports 均可以加载；Dashboard/strategy_runner 包初始化不加载 HTTP/数据库适配器。该检查在改动前能捕获 strategy_runner 包门面的间接数据库依赖。
- 新增适配器测试覆盖 ORM 事件与完整账户成交转为普通数据值、事件 details 与 ORM 脱离，以及缺失/畸形成交数量证据仍无法重建身份；现有分类用例改用真实 DTO，覆盖历史订单碰撞、平仓同步延迟与重复退出抑制。
- 新模块、决策模块与新增测试 Ruff 检查通过，git diff --check 通过。
- 本批 AST 依赖扫描：319 个模块，未发现跨顶层包循环；原 Dashboard 静态环已移除。剩余模块级强连通分量仍为 position_ledger_models、recovery_models、recovery_codec，包含 TYPE_CHECKING/延迟引用，不宣称已清理。
- 实际 Postgres 并发与回滚测试已尝试；本地 127.0.0.1:54329 接受 TCP 连接但数据库握手超时，五秒独立连接探针返回 TimeoutError，停止该测试。**本批未取得真实 Postgres 事务验收通过的证据**；未使用生产数据库运行测试。

## 后续实施顺序

1. 自愈事务迁移：先验证并统一 `execution_position:{canonical_id}` 锁、revision、epoch、事务回滚与提交后完整重载，再将修复计算与 Postgres 写入适配分离。不能只把 SQL 换文件而保留并发缺口。
2. ExecutionBook 内部协作者：先拆恢复/检查点计算，再拆命令/outbox 与 reservation 生命周期，最后拆证据归并。统一 mutation lock、候选状态、事务与提交后发布仍由 Book 所有。
3. 聚合仓储与运行装配：按意图提交、订单事件/成交、执行命令/水位、对账划分仓储；提取 runtime factory 和可调用的 CLI 用例。
4. 恢复模型/codec 的静态环：集中摘要计算与投影编码职责，保留既有 checkpoint digest 与跨 epoch 保护。

生产账户事实覆盖冲突和策略状态发布缺失属于尚未关闭的运行问题；本次结构迁移不能作为这些问题已经修复的证据。
