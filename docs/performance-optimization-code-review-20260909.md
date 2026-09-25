# 性能优化分析报告（代码 + 架构）

**检查时间**: 2026-09-09 22:52 CST  
**方法**: 服务器指标采集 + 全代码库扫描（只读，未做修改）

---

## 一、优化后服务器状态

与中午 12:38 的采样对比：

| 指标 | 中午 12:38 | 晚间 22:52 | 变化 |
|------|-----------|-----------|------|
| Load Average | 2.19, 2.36, 2.26 | **0.55**, 1.67, 1.96 | ✅ 1min 明显下降 |
| 内存可用 | 861 MiB | **1.0 GiB** | ✅ +160 MiB |
| Swap 使用 | 556 MiB | **199 MiB** | ✅ -357 MiB |
| Swap 活动 (si/so) | 0~4/0 | **0/0** | ✅ 无活跃交换 |
| Memory PSI | - | **0.00** (some/full) | ✅ 无内存压力 |
| CPU PSI some | - | **12.57** avg10 | ⚠️ 有 CPU 等待 |
| market-data CPU | 19.31% | **15.69%** | ✅ 降低 3.6pp |
| market-data 内存 | 296 MiB | **157.5 MiB** | ✅ **减少 47%** |
| PG buffer hit rate | - | **97.75%** | ✅ 良好 |
| PG idle 连接 | 57 | **47** | ✅ -10 |

> 改进显著：market-data 容器内存从 296 MiB 降到 157.5 MiB（-47%），swap 使用从 556M 降到 199M。说明之前的代码优化已经生效。

---

## 二、代码级优化发现（按影响排序）

以下每项发现都基于具体代码，包含精确的文件位置和行号。

---

### 🔴 发现 1：quote_hub 每条 quote 都加锁并 O(N) 扫描 subscriber

**影响**: ⭐⭐⭐ 高 — book ticker 是最高频的数据流

**位置**: [quote_hub.py:151-167](file:///Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/market_data/quote_hub.py#L151-L167)

```python
async def publish(self, quote: ...):
    quotes = (quote,) if isinstance(quote, ...) else quote
    for item in quotes:                              # 每条 quote
        message = encode_market_quote(item)          # json.dumps 在主线程
        async with self._subscriber_lock:            # 每条加锁
            subscribers = tuple(
                subscriber
                for subscriber in self._subscribers.values()
                if subscriber.environment == item.environment  # O(N) 过滤
            )
        for subscriber in subscribers:
            self._enqueue_latest(subscriber, message)
```

**问题**：
1. 每条 quote 都获取一次 `subscriber_lock`，高频下造成锁竞争
2. 每条 quote 都做 O(N) subscriber 过滤
3. `encode_market_quote` 内部是 `json.dumps`，在事件循环线程同步执行

**优化方案**：
- 按 environment 维护 subscriber 索引 `dict[str, set[_Subscriber]]`，O(1) 查找
- 整个 batch 加一次锁，而非每条 quote 加一次
- quote 编码考虑用 `orjson` 或延迟到 writer task

---

### 🔴 发现 2：N+1 数据库插入模式

**影响**: ⭐⭐⭐ 高 — 事务持锁时间线性增长

**位置**: [strategy_run_repository.py:314-321](file:///Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/persistence/postgres/strategy_run_repository.py#L314-L321)

```python
async def _insert_many_idempotent(session, model, values, conflict_message):
    for item in values:                                    # ← 逐条循环
        await _insert_idempotent(session, model, item, ...)  # ← 每条一次 DB round-trip
```

同样的模式也出现在 [runtime_state_repository.py:344](file:///Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/persistence/postgres/runtime_state_repository.py#L344)

**问题**：批量保存 signals/candidates/fills 时，每行一个 `INSERT ... ON CONFLICT DO NOTHING`，N 行就是 N 次网络往返，事务持锁时间与行数成线性关系。

**优化方案**：改为 `insert(model).values(all_rows).on_conflict_do_nothing()`，一次网络调用完成整个批次。

---

### 🔴 发现 3：策略评估路径的冗余分组与排序

**影响**: ⭐⭐⭐ 高 — 每个 symbol 的每个 15s 状态都触发

**位置**: [runtime.py:360-369](file:///Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategies/order_flow_impulse/runtime.py#L360-L369) 调用 [event_study.py:107-114](file:///Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/strategies/order_flow_impulse/event_study.py#L107-L114)

```python
# runtime.py L239-243
event = _latest_event_for_state(
    tuple(buffer),    # ← 每次从 deque 复制为 tuple (O(N) 分配)
    self._config.event_config,
    state,
)

# runtime.py L360-369
def _latest_event_for_state(states, config, state):
    events = find_order_flow_impulses(states, config)  # ← 走通用入口
    for event in reversed(events):
        if event.symbol == state.symbol and ...:
            return event

# event_study.py L107-114
def find_order_flow_impulses(states, config):
    for symbol_states in _states_by_symbol(states).values():  # ← 分组（冗余）
        events.extend(_find_symbol_events(symbol_states, config))
    return tuple(sorted(events, key=...))  # ← 排序（冗余）
```

**问题**：`self._buffers[state.symbol]` 的 deque 只包含单个 symbol 的数据。但调用链经过：
1. `tuple(buffer)` — 分配新 tuple
2. `_states_by_symbol()` — `defaultdict(list)` 分组，每个 list 再 `.sort()` 
3. `find_order_flow_impulses` — 最终结果再 `sorted()`
4. `_latest_event_for_state` — 反向遍历找匹配

对单 symbol 来说，分组和排序完全是浪费。

**优化方案**：在 `on_market_state` 里直接调用 `_find_symbol_events(tuple(buffer), config)` 并取最后一个匹配事件，跳过分组层和二次排序。

---

### 🟡 发现 4：snapshot 保留策略产生持续死元组

**影响**: ⭐⭐ 中高 — 表的死元组比例持续接近 autovacuum 阈值

**位置**: [compose.server.yaml:479-486](file:///Users/zhangshuai/PycharmProjects/crypto-momentum-lab/compose.server.yaml#L479-L486)

```yaml
--snapshot-retention-batch-size: 250            # 每批删除 250 行
--snapshot-retention-max-rows-per-table: 5000   # 单次最多删 5000 行
```

**问题**：`account_balance_snapshots` 有 40,597 个死元组（占 15.7%），但 autovacuum 默认 `scale_factor=0.2` 时阈值约 43,747 —— 死元组恰好低于触发线。小批次 DELETE（每次 250 行）产生碎片化的死元组分布。

**优化方案**：
- 该表设置表级 `autovacuum_vacuum_scale_factor = 0.1`（降低触发阈值）
- 验证清理耗时和 IO 影响后再推广

---

### 🟡 发现 5：`inspect.signature()` 在每次 checkpoint 时反复调用

**影响**: ⭐⭐ 中

**位置**: [daemon.py:3617-3636](file:///Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/live_rollout/daemon.py#L3617-L3636)

```python
def _checkpoint_for_persistence(strategy):
    checkpoint_method = strategy.checkpoint
    try:
        parameters = signature(checkpoint_method).parameters  # ← 每次都反射
    except (TypeError, ValueError):
        pass
    if parameters is not None and "include_market_state_buffers" in parameters:
        checkpoint = checkpoint_method(include_market_state_buffers=False)
```

**问题**：`inspect.signature()` 解析函数签名元数据开销不小，每 100 个 state 触发一次。`strategy` 实例在整个 daemon 生命周期内不变，签名结果是确定性的。

**优化方案**：daemon 初始化时检测一次，缓存为 `_supports_exclude_buffers: bool`。

---

### 🟡 发现 6：checkpoint 日志的冗余 JSON 序列化

**影响**: ⭐⭐ 中

**位置**: [checkpoint_writer.py:225-233](file:///Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/live_rollout/checkpoint_writer.py#L225-L233)

```python
def _payload_size_bytes(checkpoint):
    return len(
        json.dumps(checkpoint.payload, ..., default=str).encode("utf-8")
    )
```

**问题**：仅为日志中的 `payload_bytes` 字段，就对完整 checkpoint payload 做了一次 `json.dumps + encode`。这与实际数据库写入无关，是额外的 CPU 消耗。

**优化方案**：改用 `sys.getsizeof` 做粗略估计，或从 DB 写入结果获取实际大小，或直接移除。

---

### 🟡 发现 7：capture queue 回退路径触发 json.dumps

**影响**: ⭐⭐ 中（取决于 raw_payload_size_bytes 是否总被传递）

**位置**: [queue.py:346-355](file:///Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/market_data/capture/queue.py#L346-L355)

```python
def _encoded_payload_size(envelope):
    if envelope.raw_payload_size_bytes is not None:
        return envelope.raw_payload_size_bytes
    return len(json.dumps(envelope.raw_payload, ...).encode())  # ← 回退
```

**问题**：当 `raw_payload_size_bytes` 为 None 时，对每个 envelope 同步执行 `json.dumps`。

**优化方案**：确保 Binance WebSocket reader 始终传递从原始 bytes 获取的 `raw_payload_size_bytes`，消除回退路径。

---

### 🟢 发现 8：连接池静态分配

**影响**: ⭐ 低（当前可控）

**位置**: [session.py](file:///Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/persistence/postgres/session.py)

**现状**：每个 Python 进程定义 8 个独立引擎 pool。16 个容器 × 多个 pool = 47 idle 连接，但 `max_connections=100` 仍有余量。

**建议**：暂不行动。后续增加账号时考虑对低频 pool 使用 `NullPool`。

---

## 三、优化优先级总结

| 优先级 | 优化项 | 影响 | 工作量 |
|--------|--------|------|--------|
| **P0** | Quote hub 锁粒度 + subscriber 索引 | ⭐⭐⭐ | ~30行 |
| **P0** | N+1 INSERT → 批量 INSERT | ⭐⭐⭐ | ~10行 |
| **P0** | 策略评估跳过冗余分组/排序 | ⭐⭐⭐ | ~15行 |
| **P1** | `inspect.signature` 初始化缓存 | ⭐⭐ | ~5行 |
| **P1** | Checkpoint 日志移除冗余序列化 | ⭐⭐ | ~3行 |
| **P1** | Snapshot 表级 autovacuum 阈值 | ⭐⭐ | 1条 SQL |
| **P2** | 确保 raw_payload_size_bytes 传递 | ⭐⭐ | 排查 |
| **P2** | 连接池瘦身 | ⭐ | 后续 |

这些优化 **不需要改架构、不需要升级硬件**，只需针对性修改几十行代码，预期可以进一步降低 CPU PSI 和事件循环延迟。建议做 P0 项后观测 CPU PSI 变化再决定后续。
