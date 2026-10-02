# 资源观察后逐项修复与上线（2026-10-02）

当前运行版本为 **2e3a65e1**，七项已定位修复已推送并部署。最终验证为 3,201 项单元/烟测、41 项真实 PostgreSQL 回归和 6 项故障注入通过。最终十分钟覆盖 21:00 收盘时刻，无关键任务崩溃、重启或 OOM，四账户恢复标记为零；行情仍有最高 592 ms 短时延迟，不能认定所有性能问题已消失。

基线见 [十分钟资源观察](resource-observation-20261002.md)。本次处理实际平仓任务崩溃、连接预算过大以及已测得的行情入口重复开销。

## 实现

1. `b89c35c9`：执行侧以 `OrderProjectionConflictError` 区分发单前仓位版本冲突，保留 Book 版本校验；平仓侧失效上下文并返回明确的待刷新结果。闭合 K 线通道保留原始事件，每轮最多重新评估三次，仍有冲突则等待账户事实通知；重新评估重新生成批次与数量，不为旧分配换 token。取消后的补单与未知订单恢复也处理该业务冲突；恢复补单读取当前目标批次，数量以目标批次和交易所剩余量中较小者为准，不继承新批次的退出权限。其他系统异常仍暴露。
2. `3e69a453`：移除 live runtime 的 `12+6` 执行池及 `4+2` 观测池硬编码，遵循专用数据库工厂与部署配置。两份 Compose 统一为执行池 `4+0`、观测池 `1+0`；四策略这两个平面的峰值预算由 96 降至 20。账户、行情、心跳、checkpoint、维护仍有独立连接池。未提高 PostgreSQL 连接上限，也未修改交易风控或退出规则。
3. `7d4f6f04`：行情磁盘健康时，最多复用一秒内的容量检查；进入 warning/halt 时不复用，显式安全与恢复检查始终刷新。WebSocket 将已有报文大小传入 RawEnvelope，队列入队与完成时不再为此重复 JSON 序列化；完整报文大小用于保守计量，未丢弃耐久行情。

4. `ce61d7d6`：旧单终态不代表补发任务完成；版本冲突、仓位未就绪、准入返回空及上下文暂不可用时保留恢复任务，按两秒首档退避重新检查，不消耗未发包的重试预算。测试同时验证退避期间不重发，条件恢复后任务完成。
5. `322c425f`：首次上线后的 19:30 K 线继续暴露账户 3/4 的真实结算阻塞：NIGHTUSDT 两单成交和预留消耗均为 2,258，数据库预留已 COMMITTED，启动却只加载 ACTIVE 行，导致结算证明缺少预留历史。恢复时按已恢复命令的预留 ID 加载缺失记录并核对命令与仓位身份；不全表加载历史，不删除阻塞标记。平仓侧将 ExecutionRecoveryPending 作为明确未就绪业务结果，保留执行保护并避免关键任务崩溃。

6. `23766550`：上线 322c425f 后实测 CPU 仍高。primary 的 30 秒非阻塞 GIL 采样获得 878 个样本（118 次读取错误），其中 178 个栈经过账户仓位上下文查询；行情健康时仍在重复加载 legacy 订单、成交及覆盖元数据。接入 Book 的提供器现在从 Hub 快照直接读取敞口，数据库回退只读取账户事实；批次与归属由 Book 提供。缓存异常标记采用 Book 分类，移除 legacy pending 造成的频繁重建；未放宽 TTL、事实版本校验或最终执行保护。无 Book 的旧路径保留兼容。

## 验证

- 修改前实际 Coordinator 排队准备 → 平仓处理调用链复现 `OrderPreSubmissionError` 逃逸，修改后返回待刷新结果，没有 prepare/POST 或预留副作用。
- 使用真实 Book 测试两个竞争窗口：检查前，以及 read 后 act 前推进仓位版本。原计划数量 1，被并发成交改为 0.5；通道重新生成当前计划，只发一次 0.5 的订单。
- 回归覆盖连续冲突的有界处理、原 K 线保留、系统异常继续传播、恢复不得平掉新追加批次。
- 磁盘健康下同一秒 100 次提交：修改前读取 100 次，修改后读取 1 次；到下一秒磁盘低于阈值时，消息拒绝进入队列，显式恢复检查立即刷新。
- WebSocket 读取路径：修改前 envelope 大小为空，修改后与读取报文大小一致，复用队列已有快速计量分支。
- 完整单测与烟测：3,198 passed，5 skipped，2 warnings（38.99 秒）。完整测试的跳过项是四个 loopback socket 场景与未设置数据库 URL 的 live smoke，不能计作已验证。
- 真实 PostgreSQL 订单准备、事务及 receipt recovery：最终扩展验证 40 passed（16.61 秒），包含真实 Book + PostgreSQL 已消耗预留重启恢复。在独立测试数据库和 192 MiB / 0.5 核受限临时容器中运行，未传入实盘交易所凭据；测试容器、测试数据库及角色已清理。本地测试 PostgreSQL 连接无响应，未把该失败计作通过。
- 新增测试模块 Ruff 通过，diff 空白检查通过；修改文件仍有历史 lint 遗留，未为本次修复扩大格式清理范围。

## 部署与生产验收

前三轮资源修复的目标运行提交：`23766550`，六个修复提交已推送 `origin/main`。最终全量回归为 3,198 passed / 5 skipped，真实 PostgreSQL 扩展回归为 40 passed。执行现有更新脚本，逐账户检查审批绑定、preflight、健康及结构化 readiness，并保持原审批限额。

前三轮上线已完成，账户 3/4 的结算 gate 已自动恢复为零。322c425f 的第一轮十分钟复测（19:51:42–20:01:42，600.41 秒）CPU 均值约 54%，行情 lag 最高 809 ms，未通过性能验收，因此继续补充第六项修复。12 容器本窗口均健康，无新增重启/OOM，内存余量最低 823.82 MiB，Swap 换出 26.32 MiB，空闲连接约 50。20:00 K 线有两次短暂 unmanaged 仓位保护，随后 readiness 恢复，无关键任务崩溃。23766550 已通过审批、preflight、四账户服务与四策略健康/readiness 验证，更新脚本耗时 574 秒。策略与 execution-account 镜像为 23766550；未再次修改的行情、面板与研究采集保持 7d4f6f04。最终十分钟资源复测结果见下。代码热路径改进不能直接推导整机 CPU 必然下降，也不能将健康探针等同于平仓验收。


第一轮复测分分钟指标见 [interim-minutes](resource-fix-20261002-interim-minutes.csv)，容器指标见 [interim-containers](resource-fix-20261002-interim-containers.csv)。

## 23766550 十分钟资源复测（收盘验收仍失败）

UTC 2026-10-02T12:19:15.165252+00:00 至 2026-10-02T12:29:15.387007+00:00，实际 600.222 秒，596 个主机样本。采样方法与基线一致。

| 指标 | 早期基线 | 第一轮复测（322c425f） | 最终（23766550） |
| --- | ---: | ---: | ---: |
| 主机 CPU 加权均值 | 27.97% | 54.40% | 34.35% |
| CPU P50 / P95 | 19.29% / 79.69% | 44.10% / 100% | 26.53% / 83.79% |
| CPU ≥90% 累计 / 最长连续 | 15.06 / 5.01 秒 | 163.86 / 27.12 秒 | 27.11 / 7.02 秒 |
| MemAvailable 最低 | 510.92 MiB | 823.82 MiB | 786.89 MiB |
| Swap 换出 | 54.77 MiB | 26.32 MiB | 0.00 MiB |
| Swap 换入 | 1.12 MiB | 6.16 MiB | 0.95 MiB |

最终内存余量 891.61→819.06 MiB，Swap 占用 375.85→375.35 MiB。主机新增 OOM 为 0；采样进程 CPU 用时 1.74 秒，采集错误 0 项。

| 服务 | CPU 均值（单核口径） | working set 开始→结束 | 峰值 |
| --- | ---: | ---: | ---: |
| market-data-1 | 22.39% | 189.83→197.69 MiB | 203.03 MiB |
| postgres-1 | 6.41% | 557.52→682.07 MiB | 682.07 MiB |
| live-strategy-1 | 5.33% | 228.72→228.73 MiB | 228.74 MiB |
| live-strategy-account-2-1 | 5.19% | 183.25→183.25 MiB | 183.50 MiB |
| live-strategy-account-4-1 | 4.95% | 188.69→188.69 MiB | 188.72 MiB |
| live-strategy-account-3-1 | 4.84% | 185.07→185.07 MiB | 185.09 MiB |
| dashboard-1 | 2.47% | 89.49→90.28 MiB | 99.09 MiB |
| research-collector-1 | 0.97% | 128.53→128.69 MiB | 128.69 MiB |
| execution-account-live-account-2-1 | 0.50% | 87.46→90.51 MiB | 90.95 MiB |
| execution-account-live-1 | 0.50% | 89.07→90.47 MiB | 90.84 MiB |
| execution-account-live-account-3-1 | 0.49% | 88.59→91.09 MiB | 91.33 MiB |
| execution-account-live-account-4-1 | 0.47% | 88.51→92.38 MiB | 92.38 MiB |

主机百分比使用两核总容量，容器百分比使用单核口径。市场活动和启动恢复负载不同，窗口间不能作为严格 A/B 因果实验。额外查询的剖析与局部回归能够证明已移除冗余路径；整机仍有尖峰，不能承诺消除全部开销或长期无故障。

该轮分分钟指标见 [final-minutes](resource-fix-20261002-final-minutes.csv)，容器指标见 [final-containers](resource-fix-20261002-final-containers.csv)。秒级原始采样临时保存于服务器 `/tmp/cml-resources-20261002T121915Z.json`。

## 20:30 收盘事件暴露的新问题

23766550 的资源窗口结束后，20:30:03 account-4 的 closed candle exit channel 再次发生 RuntimeError：`positive cumulative fill has no positive cumulative average price; execution facts require recovery`，容器于 20:30:57 重启。不能将 20:19–20:29 的健康状态扩大到下一根收盘 K 线，23766550 未通过业务验收。

币安 POST 和短期查询返回已成交 6,526、均价/累计成交额为零的暂不完整回报，状态机仍写入终态 FILLED；Coordinator 拒绝使用零成交价结算，异常终止策略。受影响 client ID 为 `cml_8bb22b8187ae76545b158dffdde75be0`，真实交易所 ID `1778240263`。只读数据库核对发现 6 条真实成交，数量 6,526，成交额 100.432943 USDT；重启恢复已将执行命令转为终态，四账户 gate 均为零，无手工改单或重发。

第七项修复：状态机在所有快照提交路径将缺价成交转为 UNKNOWN_PENDING_RECONCILIATION，保留数量、交易所 ID 和原回报状态；Coordinator 持久化 UNKNOWN，保留预留，不向 Book 提交伪造的零 quote。恢复侧遇到这一结果只继续查原订单，即使账户仓位仍滞后也不补发。价格补齐才正常结算。客户端删除委托价兜底，并只捕获已知的查询结果不确定异常。数据库写入失败和其他系统异常仍暴露。

REST 客户端 → StateMachine → Coordinator → 真实 Book 的回归已复现生产同一异常，修复后 MARKET/LIMIT 都通过，缺价阶段不释放预留，补齐后转终态，全程一个 POST。恢复阶段另有红绿测试证明不补发。原取消测试的正成交 fixture 补上实际成交价；一条故障注入测试等待事件队列处理完成后再断言恢复状态，保留原业务断言。

第七项本地验证：3,201 passed / 5 skipped / 2 warnings（41.93 秒），另有六项 fault injection 通过。无范围的 pytest 曾因本地 PostgreSQL 无响应中止，不能计作全量 e2e 通过；真实 PostgreSQL 测试另行在隔离测试库执行。

第七项提交 `2e3a65e1f411a1953282e451247eff32a5f8652e` 已推送。真实 PostgreSQL 扩展回归 **41 passed（21.12 秒）**；192 MiB / 0.5 核临时容器无实盘交易所凭据，独立测试库与角色已清理。现有部署脚本已成功完成，耗时 544 秒；四策略及四 execution-account 运行镜像为该提交，审批绑定、preflight、健康/readiness 通过。新的十分钟观察从北京时间 20:59:15 开始，覆盖 21:00 收盘周期，最终统计如下。

## 2e3a65e1 上线后的最终验收

北京时间 **20:59:15.274–21:09:15.614**，持续 **600.340 秒**，594 个主机样本，覆盖 21:00 收盘时刻。剖析在资源采样结束后进行，不计入该资源窗口。12 个容器所有健康采样均 healthy，无新增重启、身份变化、OOM、memory.events max 或 CPU throttling。

| 指标 | 早期基线 | 322c425f | 23766550 | 2e3a65e1 |
| --- | ---: | ---: | ---: | ---: |
| 主机 CPU 加权均值 | 27.97% | 54.40% | 34.35% | 36.37% |
| CPU ≥90% 累计 / 最长连续 | 15.06 / 5.01 秒 | 163.86 / 27.12 秒 | 27.11 / 7.02 秒 | 17.08 / 2.00 秒 |
| 最低可用内存 | 510.92 MiB | 823.82 MiB | 786.89 MiB | 779.95 MiB |
| Swap 换入 / 换出 | 1.12 / 54.77 MiB | 6.16 / 26.32 MiB | 0.95 / 0 MiB | 1.49 / 32.82 MiB |

CPU P50 / P95 为 29.53% / 83.76%，最高 100%；iowait 均值 0.64%。内存余量 882.72→831.65 MiB，Swap 占用 428.09→458.12 MiB。采集错误为零，采样进程用时 2.06 CPU 秒。内存 PSI some/full 为 0.047% / 0.033%。

| 服务 | CPU 平均（单核口径） | working set 开始→结束 |
| --- | ---: | ---: |
| market-data-1 | 26.60% | 193.48→214.21 MiB |
| postgres-1 | 6.31% | 533.00→632.02 MiB |
| live-strategy-1 | 5.14% | 228.30→228.32 MiB |
| live-strategy-account-2-1 | 4.67% | 178.84→178.86 MiB |
| live-strategy-account-4-1 | 4.50% | 187.05→187.07 MiB |
| live-strategy-account-3-1 | 4.45% | 186.48→186.50 MiB |
| dashboard-1 | 2.62% | 89.66→91.13 MiB |
| research-collector-1 | 1.00% | 102.55→97.43 MiB |
| execution-account-live-account-4-1 | 0.57% | 82.25→89.00 MiB |
| execution-account-live-account-3-1 | 0.55% | 82.25→88.40 MiB |
| execution-account-live-1 | 0.51% | 86.53→89.33 MiB |
| execution-account-live-account-2-1 | 0.48% | 86.64→89.13 MiB |

- 四策略 code_commit 均为 2e3a65e1，entry readiness 开启，末次行情年龄约 0.42–0.44 秒；没有 `live_runtime_critical_task_failed`、session_run_failed 或平仓任务崩溃。
- 四账户 command/external recovery gates 均为零，命令状态仅 rejected/terminal；空闲数据库连接 50，active 2（含诊断），没有锁等待和超过十秒的 idle-in-transaction。基线 idle 为 88。
- account-3/4 各有两条启动期 `account_snapshot_execution_book_conflicts`，最后在 21:00:05。原因是历史 flat 范围及新 stream epoch 等待完整 source-anchored scan；之后未增加，readiness 开启。未删除保护日志或放宽覆盖证明。
- 精确十分钟窗口内行情有四次 event_loop_lag warning，最高 592 ms。延续观察到 21:09:28 时另有一次 warning。不能宣称行情延迟已彻底消失，CPU 瞬时峰值仍存在。
- 21:00 后未复现平仓任务崩溃；本窗口没有观察到新下单确认，因此这一生产窗口验证的是任务与恢复状态，缺价/竞争等故障场景依赖上述真实调用链、PostgreSQL 回归和历史真实成交对账。没有为验收手工制造交易。

资源采样后，对行情服务追加 30 秒、99 Hz、nonblocking GIL 剖析，676 个有效样本，158 次读取错误。业务栈分布在采集发布、行情状态计算、WS 解析以及交易池刷新/持久化；没有足够证据将残余 592 ms 延迟归为某个单一新增阻塞点。未据此继续改写交易算法或删除耐久写入。样本只覆盖短时间，不能排除低频性能问题。

相较 322c425f 的高负载复测，CPU 均值降低约 33%，高负载连续时间缩短，可用内存与数据库余量明显好于早期基线。早期基线 CPU 均值仍低于最终窗口，市场活动、收盘与启动恢复负载不同，因此这些生产窗口不构成严格 A/B 实验，也不能据十分钟排除长期泄漏。已定位的七项修复均已上线；剩余短时延迟和换页作为明确的性能限制保留。

最终明细见 [deployed-minutes](resource-fix-20261002-deployed-minutes.csv)、[deployed-containers](resource-fix-20261002-deployed-containers.csv)。原始秒级文件临时保存在服务器 `/tmp/cml-resources-20261002T125915Z.json`，本地 `/tmp/cml-resource-fix-deployed-10m-raw.json`。本文与 CSV 归档属于文档提交，服务器运行代码保持 2e3a65e1。
