# 实盘近两天状态流转 Review（2026-09-29）

## 结论

存在状态流转缺口，不能用“容器 healthy / 账户对账 ready”代表实盘链路完整。直接证据是：四个账户对账均 ready、mismatch_count 为 0，但各有一条 GRASSUSDT 退出决策持续 PENDING，当前运行流 epoch 与 durable Book head 的 epoch 不同。

本轮确定复现三类代码缺陷：自愈后调用不存在的重载接口、从无序历史集合选择当前 epoch、跨 epoch 自愈保留旧 sequence 水位。前一项与服务器异常完全一致；后两项的代码错误已经复现，但不能仅凭采样日志认定它们解释了每一次线上异常。

本次交付测试和 review；未修改业务代码、生产数据、审批或部署，未下单、平仓、重启服务。生产数据库查询使用 BEGIN READ ONLY 和 5 秒 statement_timeout。SSH 使用已有密钥认证，报告不包含凭据。

## 范围与证据口径

- 检查起点：北京时间 2026-09-29 20:22；主要现场取证至 20:30。
- 服务器、本地 Git 与当前策略容器启动元数据均指向 `2c4768cbcec0e7be108d9f1c5d4a9517cb619f0a`。
- 近两天窗口：约 2026-09-27 12:22 UTC 至检查时；下文日志时间统一为 UTC（北京时间加 8 小时）。
- 当前容器刚重建，不能只看 docker logs。补查 `/var/lib/crypto-momentum-lab/crash-logs/` 的归档。9/28、9/29 文件共 172 份、约 892 MB；同容器存在多次归档，按 service 和容器 ID 前 12 位选较大文件减少重复。这些文件数不是崩溃次数，行数也不是独立事故次数。
- 查阅实际交易路径：账户流绑定 → Book 恢复/自愈 → context 持仓分类 → durable decision exit → 派发确认。未对全部交易做逐笔财务对账，也未证明资金或成交事实丢失。

## 当前现场状态

12:25 UTC 查询，四个账户均 `status=ready`、`position_count=1`、`mismatch_count=0`。最新审批 commit 也已匹配 `2c4768c`；历史审批不匹配不能继续当作当前唯一原因。

| 账户 | PENDING 创建时间 UTC | durable LONG head epoch 前缀 | 12:29 日志的运行 epoch 前缀 | head sequence |
|---|---|---|---|---:|
| account-2 | 11:52:56 | 9ca0ccfa | f7a3cd66 | 461 |
| account-3 | 11:53:11 | 40ab174f | 337c6b28 | 440 |
| account-4 | 11:53:15 | 7e877782 | 5b41de7b | 441 |
| primary | 11:54:59 | 24e4f791 | f07b6eb1 | 463 |

四条 PENDING 的 `expected_projection_version` 均等于数据库对应 LONG head 的 `projection_version`（按 environment/account/symbol/side 关联查询）。这排除了“数据库中的 projection token 已变”作为这四条命令的直接解释；仍需核对内存恢复、stream_scope、readiness 和 allocation。12:29 四个策略都继续打印 `durable_decision_exit_deferred_until_book_ready`，距离创建已约 35–37 分钟。

LONG/SHORT 是不同 head。数据库中同 symbol 的两个 head 不能当作重复行。

## 主要 Findings

### INC-01 / P1：自愈提交完成后，内存重载接口断裂

位置：`domain/execution/execution_book.py:635` 的 `reload_position()`；`live_rollout/postgres_runtime.py:691` 的自愈调用链；`persistence/postgres/execution_unit_of_work.py:602`。

`auto_heal_unmanaged_position()` 提交数据库后，调用方执行 `book.reload_position()`。该方法调用 `load_execution_positions()`，实际 `AsyncPostgresExecutionUnitOfWork` 只实现 `load_positions()`。异常被 context 层捕获并记录，自愈事务却已经提交，因此数据库与内存没有形成闭环。调用方还在重载成功之前设置了 `healed_any=True`。

真实日志：`20260929T120029.703928124Z_live-strategy_c265ff522fb9f5e40dc6c53c.log`，10:14:20：

```text
auto_heal_unmanaged_position_failed ... 'AsyncPostgresExecutionUnitOfWork' object has no attribute 'load_execution_positions'
```

新增测试使用真实 UoW 类的 `create_autospec(..., spec_set=True)`，调用真实 `ExecutionBook.reload_position`，分别覆盖空结果与真实 `DurableExecutionPositionState` 非空结果，均复现 AttributeError。旧测试主要验证自愈 helper 返回 True、commit 和 add 次数，没有覆盖提交后的实际消费者。

修复要求不止改函数名：返回类型使用 `scope`，没有 reload 当前访问的 `s.key`；新测试的非空分支会继续约束这个接口。应复用完整恢复校验，验证 head、cut、sequence、去重身份和 reservation，成功发布内存后才宣告 healed，并测试后续 observe/read。

### INC-02 / P1：当前 stream 从历史无序集合中选择

位置：`domain/execution/execution_book.py:606` 的 `register_active_stream()` / `get_active_stream()`，以及 `live_rollout/decision_facts.py:188` 的流绑定。

注册采用 set.add，旧 epoch 保留；获取时返回首个匹配 account/environment 的元素。set 没有“最后注册”的语义。重连注册新 epoch 后，自愈或订单路径仍可能拿到旧 epoch。线上当前流与 head 不一致与这个问题相符，但尚不能排除其他恢复路径造成的偏差。

新增测试连续注册 16 个 epoch，并每次断言返回最新 epoch，同时验证其他账户隔离。固定 `PYTHONHASHSEED=0` 时稳定失败：预期 epoch-3，得到 epoch-1。

建议单独维护按 `(environment, account)` 定位的当前流引用，并显式处理 epoch 交接。不要直接删除所有历史流信息；历史合法性验证和“当前路由”需要不同语义。

### INC-03 / P1：自愈换 epoch 却继承旧 sequence

位置：`live_rollout/position_self_healing.py` 的 `prev_payload / last_seq / head_payload` 构造；`domain/execution/execution_book.py:2968` 的 sequence 单调检查。

自愈会更新 head.stream_epoch，却无条件保留 prev_payload.last_sequence。正常 epoch adoption 路径会清除旧水位，自愈路径没有保持同一约束。新流 sequence 从低值开始时，会被 `evidence.sequence <= previous_sequence` 拒绝，即使它是新 epoch 的新事实。

新增对照测试调用真实自愈 helper，使用真实 fill、cut、投影和 ORM head，仅替代存储 I/O：同 epoch 保留 461 的测试通过；旧 epoch → 新 epoch 应清空水位的测试失败，实际仍为 461。

线上 LONG head sequence 为 440–463，与此风险相符；本轮没有获得完整逐事件序列来证明这些水位已经丢弃了具体哪一笔成交。修复需以新流完成的恢复位置重建水位；同 epoch 重试仍须保留去重。不能全局取消 sequence 检查。

### INC-04 / P1：退出的安全延后缺少可验证的收敛路径

位置：`live_rollout/decision_facts.py:408` 的 `recover_pending_exits()`。

最新提交已经把 `requested account stream does not match the restored position` 从抛错变为 continue。这避免了崩溃，但没有完成恢复：当前四条退出仍长期 PENDING。函数只在 Book 的 scope、readiness、projection、allocation 全部匹配后派发，否则保留 pending；它不负责修复这些前置条件。

仓库已有 `mark_exit_superseded()`，本轮搜索没有发现调用方。它明确要求先对账确认旧命令未提交。因此不能通过强制 DISPATCHED、忽略 token、直接更换 epoch 或盲目重发来“解除卡住”。

新增测试覆盖：首次 epoch 不匹配不派发；Book 真正恢复到匹配状态后派发且记录 ack；epoch、projection、allocation、not-ready 四种不匹配连续重试均不得误发或误 ack；unknown/rejected 不推进状态；其他数据损坏不能被吞成 epoch 等待。

这些通过的用例证明了保护与条件满足后的进展，但不证明线上 epoch adoption 已能完成。还缺：对账旧订单结果 → 采用新流并完成恢复 → 对未提交且过期的旧命令给出终态 → 重新计算退出，整个过程须有重启回放测试。应该单独监测最老 pending 年龄和阻塞原因。

### INC-05 / P2：恢复、账户配置与审批状态互相放大重启

归档证据包括：`account_config_update` 触发 user-data pipeline recovery / reconnect，策略切换 `account_snapshot_recovering`、`risk_control_state_recovering`，以及 `approval_commit_mismatch,account_not_ready` 启动失败。策略近期启动仅订单恢复阶段就耗时约 88.5 秒，预热 30 个标的全部 deferred。

这些不是同一种错误：配置变化后对账是保护措施；审批 commit 不匹配应拒绝启动；warmup deferred 也不等价于数据永久丢失。需要部署与恢复过程明确显示当前阻塞阶段，避免把所有状态都呈现成普通 healthy 或反复启动。12:25 最新审批已匹配，不应绕过审批检查。

## 历史故障链（UTC）

| 时间 / 归档 | 证据 | 本轮判断 |
|---|---|---|
| 9/27 14:14，`20260927T142245.061023480Z_live-strategy-account-4_d28b2cf158dc6a360b9988b3.log` | `restore_active_commands_failed: execution command side is missing or invalid` | 近 48 小时窗口前段已有命令持久化/恢复契约不完整的证据 |
| 9/27 20:28，`20260927T203118.881281684Z_live-strategy-account-4_1a9fafcfbf29dd3fe145bced.log` | startup market buffer 的 `MarketStateHubReplayUnavailable` 导致 critical task/session 失败 | 启动依赖的行情回放不可用；不能仅靠重启策略修复 |
| 9/28 03:33，`20260928T034714.288373301Z_live-strategy-account-4_548e14762449e6adba301604.log` | `Exit order ... is missing required strategy_name` | 退出身份传递不完整的历史证据；不是网络错误 |
| 9/28 06:16，`20260928T062714.056723Z_live-strategy-account-2_16894b262243.log` | `live_closed_candle_exit_failed reason=order_identity_conflict` | 身份/预留恢复问题的历史证据，须与具体修复版本区分 |
| 9/29 10:08，`20260929T101058.700205777Z_live-strategy_5d7b9eadee083e6382fef566.log` | `auto_heal... duplicate key ... pk_execution_book_heads` | 后续提交已按真实主键查 head；当前 helper 的既有更新测试通过 |
| 9/29 10:14，同上 INC-01 归档 | `load_execution_positions` 不存在 | 当前仍可复现 |
| 9/29 10:58，同上 INC-01 归档 | `durable execution head is malformed` | 后续提交补齐 head schema；需要自愈→重启恢复集成验收，单独 commit 测试不够 |
| 9/29 12:05，`20260929T120726.807560036Z_live-strategy_fe917d8ae47c9cc2366ce2aa.log` | `requested account stream does not match the restored position` | 当前已转为延后，不能认定退出恢复已完成 |
| 9/29 12:29，当前四个策略容器 | `durable_decision_exit_deferred_until_book_ready` | 与四条长期 PENDING、head/current epoch 不一致共同构成当前问题证据 |

日志还存在 market-state gap recovery timeout 和 symbol reset；这说明行情也发生过断档或恢复超时，但不证明交易所从未发送数据，也不证明它是所有订单故障的根因。需要独立重放归档事件，比较 event_at、received_at、watermark 与持久化关闭时点。

## 测试交付与执行

新增：`tests/unit/live_rollout/test_incident_state_flow_20260929.py`，13 个参数化场景。

| 场景 | 数量 | 当前结果 |
|---|---:|---|
| 自愈后重载，空 / 非空真实恢复类型 | 2 | 已知缺陷，strict xfail |
| 连续 epoch 交接和账户隔离 | 1 | 已知缺陷，strict xfail |
| 同 epoch / 跨 epoch sequence 水位 | 2 | 1 pass / 1 strict xfail |
| 不匹配→恢复→派发确认 | 1 | pass |
| epoch / projection / allocation / readiness 保护 | 4 | pass |
| unknown / rejected 不提前 ack | 2 | pass |
| 非 epoch 错误不可吞掉 | 1 | pass |

为了交付 review 而不隐藏缺陷，4 个已复现失败用例使用 `xfail(strict=True, raises=...)`；XPASS 会使测试失败。修复时应移除对应标记。正常输出 `9 passed, 4 xfailed` 不代表问题已修好。

明确显示现存错误（已运行）：

```bash
rtk proxy env PYTHONPATH=src PYTHONHASHSEED=0 .venv/bin/python -m pytest -q --runxfail --tb=short tests/unit/live_rollout/test_incident_state_flow_20260929.py
# 4 failed, 9 passed in 0.58s
```

相关回归范围（已运行）：

```bash
rtk proxy env PYTHONPATH=src .venv/bin/python -m pytest -q tests/unit/live_rollout tests/unit/execution tests/unit/execution_account/orders
# 669 passed, 4 xfailed in 5.03s
```

新测试 Ruff 检查通过。测试使用真实领域函数和有接口约束的替身；不会访问交易所或数据库。自愈存储事务仍以 mock 隔离，因此不能替代 PostgreSQL 真实事务、冲突、恢复重放集成测试。本机 Docker 查询长时间无响应后已终止；本次没有在生产服务器开测试容器或执行写入测试。

## 修复验收顺序

1. 修复 INC-01，使用统一恢复契约，验证非空返回类型及后续 read/observe；不能只改方法名。
2. 明确当前 epoch 的唯一指向，并把 sequence、dedup、reservation 与 epoch adoption 一并验收；历史合法流与当前流分开保存。
3. 用隔离 PostgreSQL 做“真实 fill → 自愈 commit → 内存 reload → 进程重启 restore → 新 epoch 事件 → 退出派发/ack”的串联测试。包括同名 symbol 不同账户、不同环境、LONG/SHORT 隔离。
4. 证明四条 PENDING 有明确终态或可执行的恢复路径。对账不明时继续保护，但需报警、展示原因和年龄；不能静默无限延后。
5. 分别验证行情断档重放与部署审批交接。验收应检查闭环状态，不能仅看单测全绿或容器 healthy。

附加 review 风险：自愈中的 ExchangeOrderRow 归属回退只按 symbol 查询，fills 查询未包含 environment。它们还需要跨账户/跨环境数据库隔离测试；本轮没有把这项静态风险当成已证明的线上根因。
