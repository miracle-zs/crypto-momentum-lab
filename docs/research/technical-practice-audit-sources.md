# 技术实践审计：官方标准基线与来源

整理日期：2026-10-05。本文是当前项目技术审计所用的**一手资料基线**，不是对当前实现的合规结论。每一项“符合/不符合”均须回到代码、配置、迁移记录、运行指标和测试证据核验。

## FastAPI / Pydantic v2

- 对外 HTTP 契约应显式声明输入类型、`response_model` 与状态码；FastAPI 会据此校验和过滤输出、生成 OpenAPI schema。这既是客户端契约，也是避免内部字段意外泄漏的边界。来源：[FastAPI Response Model](https://fastapi.tiangolo.com/tutorial/response-model/)、[Response Status Code](https://fastapi.tiangolo.com/tutorial/response-status-code/)。
- 路由的响应 DTO 不应直接等同于 ORM/领域对象；输入与输出模型在字段可见性、校验与版本演进上各自承担契约责任。FastAPI 官方明确将 `response_model` 的输出过滤作为安全用途。来源：[FastAPI Response Model](https://fastapi.tiangolo.com/tutorial/response-model/)。
- Pydantic 模型以类型标注定义验证、序列化与 JSON Schema；不可信边界数据须经过模型验证后再进入业务流程。其保证的是处理后的模型符合声明类型与约束，而非“原始输入天然正确”。来源：[Pydantic Models](https://docs.pydantic.dev/latest/concepts/models/)。
- Pydantic v2 默认忽略额外字段；面向不应静默接受未知字段的命令/API 边界，应按契约选择 `extra='forbid'` 并为变更建立版本策略。来源：[Pydantic extra data configuration](https://docs.pydantic.dev/latest/concepts/models/#extra-data)。
- 对 JSON 原始负载优先使用 `model_validate_json()`，不要先手动 `json.loads()` 再校验；`model_construct()` 绕过验证，只能用于已验证或可信数据，且不应未经 profiling 假设其更快。来源：[Pydantic validation modes and model_construct](https://docs.pydantic.dev/latest/concepts/models/#validating-data)。
- FastAPI 单进程可并发服务多个客户端；多 worker 是独立进程和独立内存，应显式评估副作用（内存、连接池、后台任务与单例状态），而不是把 worker 数当作无条件吞吐优化。来源：[FastAPI Deployment Concepts](https://fastapi.tiangolo.com/deployment/concepts/)。
- 进程级客户端、连接池和后台资源应由 `lifespan` 的统一启动/关闭路径管理，而不是在模块导入或路由内创建；可复用的鉴权、会话与服务装配应通过 `Depends` 及其子依赖表达。来源：[FastAPI Lifespan Events](https://fastapi.tiangolo.com/advanced/events/)、[FastAPI Dependencies](https://fastapi.tiangolo.com/tutorial/dependencies/)。
- 异步 API 测试应使用 async test、AnyIO 与 `httpx.AsyncClient`/ASGI transport；若被测应用依赖 lifespan，测试需要显式确保其执行。来源：[FastAPI Async Tests](https://fastapi.tiangolo.com/advanced/async-tests/)。

## asyncio / PEP 492 与 PEP 525

- `async with` 是资源生命周期的异步协议（`__aenter__` / `__aexit__`）；会话、事务、连接、锁等取得后必须在此类可取消边界中成对释放。来源：[PEP 492 — async context managers](https://peps.python.org/pep-0492/#asynchronous-context-managers-and-async-with)。
- 只有 awaitable I/O 客户端调用适合置于 `async def`；同步阻塞库应使用普通 `def`（由 FastAPI 在线程池执行）或显式隔离，CPU 密集型工作需要进程/worker 并行，不能指望 async 消除 CPU 阻塞。来源：[FastAPI async and await](https://fastapi.tiangolo.com/async/)。
- `async for` 的迭代器必须实现返回 awaitable 的 `__anext__` 并以 `StopAsyncIteration` 结束；流式行情/事件源应以该协议明确背压、结束和异常语义。来源：[PEP 492 — async iterators](https://peps.python.org/pep-0492/#asynchronous-iterators-and-async-for)。
- 并发任务应有明确所有者和收敛点。Python 标准库的 `TaskGroup` 在上下文退出时等待所有子任务，并以取消实现结构化并发；协程不得吞掉 `CancelledError`，否则会破坏 `TaskGroup`/超时等机制。来源：[asyncio TaskGroup and cancellation](https://docs.python.org/3/library/asyncio-task.html#task-groups)。
- 异步生成器可携带 `try/finally` 与 `async with`；被提前停止消费时仍须被正确终结，运行时通过 `aclose()`/loop finalization 支持这一点。长期行情订阅或游标读取需要关闭/取消路径的测试。来源：[PEP 525 — Finalization](https://peps.python.org/pep-0525/#finalization)。

## PostgreSQL 16

- 优化从真实工作负载的 `EXPLAIN (ANALYZE, BUFFERS)` 开始，并比较估算行数、实际行数、扫描与 I/O；`EXPLAIN ANALYZE` 本身带 profiling 开销，不应把其耗时直接当作普通查询时延。来源：[PostgreSQL 16 EXPLAIN](https://www.postgresql.org/docs/16/sql-explain.html)、[Using EXPLAIN](https://www.postgresql.org/docs/16/using-explain.html)。
- 索引应由实际查询谓词、排序与连接路径驱动。先 `ANALYZE` 保证统计信息，再判断索引是否被使用；不能以关闭顺序扫描等 planner 开关代替建模/统计问题的诊断。来源：[Examining Index Usage](https://www.postgresql.org/docs/16/indexes-examine.html)。
- `ANALYZE` 的统计信息被 planner 用于选计划；高写入或分区层级场景需要验证 autovacuum/analyze 是否及时覆盖，必要时制定表级阈值或人工 `ANALYZE`。特别是 partitioned parent 不会被 autovacuum 自动处理。来源：[PostgreSQL 16 ANALYZE](https://www.postgresql.org/docs/16/sql-analyze.html)、[Automatic Vacuuming](https://www.postgresql.org/docs/16/runtime-config-autovacuum.html)。
- 高频更新表必须把 dead tuples、autovacuum 延迟、事务 ID 老化和索引膨胀纳入运维指标。普通 `VACUUM` 可并发运行；`VACUUM FULL` 会重写表且持有 `ACCESS EXCLUSIVE` 锁，不能作为常规线上修复手段。来源：[PostgreSQL 16 VACUUM](https://www.postgresql.org/docs/16/sql-vacuum.html)、[Routine Maintenance](https://www.postgresql.org/docs/16/maintenance.html)。
- `work_mem`、maintenance/autovacuum 内存和 worker 数必须按**并发总量**计算后调优，不能照搬单查询 benchmark 数值；官方特别指出 autovacuum 可能按 worker 数重复分配相关内存。来源：[PostgreSQL 16 Resource Consumption](https://www.postgresql.org/docs/16/runtime-config-resource.html)。

## Apache Arrow / Parquet

- Arrow 是语言无关的内存列式规范；其连续列布局面向扫描、向量化和零拷贝传输，但以更昂贵的随机更新为代价。因此将 Arrow 用于分析/批处理边界是合理的，不应把它当作可变事务状态的默认存储。来源：[Apache Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html)。
- Arrow schema（包含精度、时间单位/时区、nullability 与嵌套类型）应作为数据契约版本化；生产链路不得靠隐式 dtype 推断跨进程/跨语言传递时间、金额或枚举。Arrow 格式将类型与物理布局明确规范化。来源：[Apache Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html)。
- Parquet 文件由 row group、column chunk、page 组成：row group 是横向分区，column chunk 按列连续存放，page 是编码/压缩单位。数据分区、排序、row group 大小和读取谓词必须围绕实际扫描模式设计。来源：[Parquet Concepts](https://parquet.apache.org/docs/concepts/)。
- row group 越大越利于顺序 I/O，但写入需要更多缓冲，且读取往往要覆盖整个 row group；官方给出的 512MB–1GB 建议是 HDFS 批处理背景，不可直接套用于小型、本地或低延迟交易数据。应以工作负载 benchmark 决定。来源：[Parquet Configurations](https://parquet.apache.org/docs/file-format/configurations/)。
- 对需要选择性扫描的数据，分区/排序应使 min/max 统计、column/page index 可以跳过无关页；使用新编码、Data Page V2 或逻辑类型前，必须测试读写端兼容性。来源：[Parquet Page Index](https://parquet.apache.org/docs/file-format/pageindex/)、[Parquet Format Versions](https://parquet.apache.org/docs/file-format/versions/)。
- Parquet 是不可变的分析文件格式；写入应以临时路径完成后再原子发布，记录 schema、writer 版本、分区与数据快照，避免读者观察到部分文件或不兼容格式。其格式兼容性由实际启用的 feature 决定，不能仅凭文件 metadata version 推断。来源：[Parquet Format Versions](https://parquet.apache.org/docs/file-format/versions/)。

## 审计使用方式

1. 先以端到端下单/行情/研究链路为单位列出数据、事务、取消、错误与重试边界。
2. 再把每条代码事实映射到上述基线：符合、部分符合、不符合或证据不足；“未使用某项技术”不自动构成问题。
3. 优先修复会影响资金安全、数据正确性、资源泄漏或恢复语义的问题；性能项必须附 `EXPLAIN`、指标或 benchmark 证据。
