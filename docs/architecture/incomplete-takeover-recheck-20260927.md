# 新模块接管复查（2026-09-27）

固定快照：`24c3384917ee1241fcd3e1c3fcc4bd14942fbe05`。本次为本地静态调用链复查，不是线上部署验收；未连接服务器或修改实现。

## 已补上的部分

- `f8265bd` 将私有 outbox 修改改为公开 `register_prepared_command`，并增加累计成交增量结算；不再沿用旧审查中“直接写私有 outbox”的描述。
- Trace 任务已有集合与 drain 方法，但 Live orchestrator 未发现调用/注册该 drain；政策/命令事务问题仍在。
- `132beef` 让灰度 reservation 查询转交 ExecutionBook，并把 PositionLedger primary 默认开关改为启用。
- `24c3384` 增加退出终态预留冲突处理及身份 epoch 推进。说明崩溃循环有针对性修复，不能仅凭代码确认服务器已恢复。

## 仍未完整接管

| 模块/协议 | 当前状态 | 未接管的具体职责 | 证据位置 |
| --- | --- | --- | --- |
| ExecutionBook 接受与结算 | 灰度查询/observe/outbox注册接入 | 旧 coordinator仍创建、保存、消费、释放预留；Live未走act接受请求，而是另行register_prepared_command | execution_account/orders/coordinator.py:429、:616、:688；domain/execution/execution_book.py:282、:448 |
| 耐久执行恢复 | 内存状态机+预留数据库 | requests/receipts/outbox/evidence/trade去重仍dict/set；没有统一接受事务与耐久回执恢复 | domain/execution/execution_book.py:222–228 |
| PositionLedger批次权威 | 默认primary启用 | 先运行旧rebuild，ledger未满足条件或异常时仍返回旧result.batches；旧算法可重新成为执行输入 | live_rollout/postgres_runtime.py:2423、:2500、:2585、:2603 |
| Live完整政策 | transition参与决策过滤 | 无candidate分支返回原decision，未执行exit_command；实际退出仍LiveExitManager/退出通道 | domain/decision/decision_engine.py:752–792；live_rollout/runtime_orchestrator.py:1143 |
| Paper完整政策/仿真 | DecisionEngine及SimulationExecutionAdapter有调用 | positions_by_id由旧mark_positions与旧fill resolver管理；未消费dec_res.exit_command，adapter入口旁路写journal，未统一完整生命周期 | strategy_runner/paper.py:304–316、:404、:435–449 |
| PolicyState和Trace提交 | 内存next_state+异步Trace | 未恢复耐久政策状态；state、命令、预留、Trace不共用事务；drain有实现但本Live入口未发现注册 | live_rollout/decision_facts.py:253、:291、:307；live_rollout/runtime_orchestrator.py:512、:1133 |
| 决策精确重现 | 增加结构/一致性检查 | audit仍不调用decide/transition重放，也不比较完整输出，末尾仍认证成功 | tools/reproduce_decision.py:136–159 |
| RuntimePlan配置权威 | 用于门禁身份 | Live compile不传实际overrides，compiler使用默认策略参数和固定风险参数；真实运行配置仍另行读取 | live_rollout/runtime_orchestrator.py:619；domain/runtime/runtime_plan.py:144–199 |
| CapabilityEvidence权威 | 最后提交前门禁已接入 | context按symbol统计未决订单，未排除当前命令；identity_verified和lease_active直接设True；证据未统一由账本/控制状态产生 | live_rollout/runtime_orchestrator.py:633–676 |
| DeploymentCoordinator | 领域协议及内存实现 | src/deploy中未发现实际部署调用；停止writer、授予epoch、durable journal尚未由此统一接管 | domain/runtime/deployment_coordinator.py:218、:241、:285；全仓调用搜索 |
| 收益CoverageReceipt | 新calculator已接Dashboard | builder仍按已有行评估覆盖并创建receipt，缺采集端完整扫描证据 | operator_dashboard/performance_builder.py:327、:336 |
| OperationalReadModel | 新模型已接Dashboard | cap_ok由旧状态推导；reconciliation_matched=非HALTED，未使用真实对账结果；统一模型包装旧推断 | operator_dashboard/overview_queries.py:264、:281 |
| Trace/数据集保留依赖 | RetentionAuthority清理入口接入，领域TraceService可登记依赖 | Live直连PostgresDecisionTraceRepository，未走领域TraceService的dependency注册；catalog存在不等于runner/trace已经登记依赖 | domain/market/decision_trace_service.py:106；live_rollout/runtime_orchestrator.py:509；persistence/postgres/decision_trace_repository.py:43 |

以上13行是职责/协议缺口，并不等于13个全新独立模块。ExecutionBook与耐久恢复、政策与状态分别拆开，以避免将“接上方法调用”误记为“恢复和事务也完成”。

PositionLedger旧算法若仅用于只读对比可以保留；当前问题是fallback结果仍可进入业务，不是文件存在本身。类似地，旧adapter可以留，但不应继续拥有另一套权威状态。

RetentionAuthority已在清理入口接入，不能笼统说完全未接管。本次新增明确缺口是Live Trace未登记保留依赖；未运行真实并发删除/恢复测试，因此不宣称已经发生误删。sizing/allocation本次未新增确认缺口，不重复列为已证实未接管。

建议下一步将每个接管项落实为四类证据：真实入口、唯一写入者、耐久恢复、旧业务旁路删除。首先收敛执行接受/结算和PositionLedger fallback，其次统一Live/Paper完整政策，再补配置/发布/证据链。
