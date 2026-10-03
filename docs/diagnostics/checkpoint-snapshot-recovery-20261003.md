# 检查点旧快照恢复误判：2026-10-03

## 生产证据

UTC 2026-10-03 00:13 检查服务器时，primary/account-2 累计重启分别为
342/146，镜像仍为 8fafa30f。两个策略在启动订单对账期间反复抛出
`cumulative order evidence was rejected: durable account journal reported a fact conflict`。
执行账户服务没有重启；行情曾因 stale 123 秒退出，随后恢复。主机两核，
当时 load average 5.50；不能把先前十分钟无重启验收推广到整个夜间。

使用 `PostgresAccountJournalStore.load_recovery_in_session` 对真实生产数据
只读重放，事务强制 READ ONLY，statement_timeout 5 秒，不请求交易所。
primary 的 CAP/MAGMA/PUMPBTC 和 account-2 的 CAP/FLUID/PUMPBTC
均重现旧快照迟到造成的恢复冲突，没有 divergent payload 冲突。

primary 检查点为 UTC 21:00:06.014，迟到快照为 21:00:04.181722；
account-2 检查点为 22:45:03.557，三份迟到快照为
22:45:02.092940、22:45:02.451671、22:45:03.008348。
后者快照在 22:45:27–29 才落库，其 source_revision 超过检查点 revision。
这些观察记录在队列中等待后持久化，未改变已确认的成交或平仓提交边界。

## 根因与修复

恢复层把所有检查点之前、revision 更新的非成交事实一概标为冲突。
领域账本在有效检查点恢复时已经明确跳过 `observed_at <= event_cut`
的账户快照；持久化层提前添加的冲突让这条领域规则无法生效。
恢复冲突又让启动对账失败，引发重复重放、迁移和连接初始化。

只从“迟到 reducer 冲突”分类中排除 snapshot，与领域投影规则保持一致。
旧快照仍被解码并保留，检查点校验和、身份和覆盖证明继续校验；
同身份不同载荷的快照仍冲突，迟到成交仍要求重建，迟到平仓提交边界仍拦截。
不吞异常，不修改账本数据，不释放预留，不手工平仓，不放宽风险参数。

在只读重放进程中应用同一个条件改动，八个有检查点的样本全部 READY，
其中原六个冲突样本恢复可比。前后数量、成本与批次由原检查点和事实决定，
未改写数据库；没有把无检查点的空流 CATCHING_UP 当作已恢复证据。

## 验证

`tests/unit/persistence/postgres/test_checkpoint_snapshot_recovery.py`
调用实际 Store 恢复与实际 PositionLedger，使用真实 codec、Row 和带完整
分页来源证明的检查点，仅替代数据库查询返回。原代码 2 failed / 3 passed；
修复后 5 passed。覆盖截止之前和恰好截止的排队旧快照、迟到成交、
迟到平仓边界和同身份不同载荷。旧快照数量故意与检查点不同，断言它不会
改写检查点仓位和 active_batches。

全量 unit/smoke：3252 passed、1 skipped、2 个既有 warning，43.45 秒。
跳过项为未配置测试数据库的 capture smoke。新测试 Ruff 完整检查通过；
源文件排除既有 E501 后通过，新增行长度符合限制，git diff --check 通过。

## 上线与验收

待填写滚动上线与无 profiler 的独立观察窗口结果。剩余行情延迟峰值和
历史空仓流迁移诊断仍需基于该窗口重新判断，不能用这次恢复修复替代性能验收。
