# 服务器十分钟资源观察（2026-10-02）

## 结论

CPU 没有持续满载；仍有可确认的平仓异常，以及需要收敛的内存、数据库连接开销。不能据本窗口健康状态认定所有故障已经解决。

观察主机：43.167.191.253。北京时间 **18:36:02.764–18:46:02.971**，实际持续 **600.207 秒**。应用镜像版本为 `34ba45ffea4493070d47c99a0a6afd5a35048dc8`。

本次只进行资源采样、日志和只读数据库检查，没有修改交易配置、重启服务或手动发单。

## 采样方法与资源结果

- 2 核主机，总内存 3,723.91 MiB。主机约每秒采样一次，共 597 条；容器 cgroup 约每 5 秒采样，健康、重启次数约每分钟核验。
- 主机 CPU 百分比以两核整体容量为分母，排除 idle 和 iowait；均值按采样间隔加权。峰值是约一秒区间的平均。
- 容器 CPU 以一个核心为 100%；与主机比较时需要除以 2。容器 working set 为 `memory.current - inactive_file`，并非进程 RSS。
- 内存余量采用 MemAvailable。Swap 换入、换出为窗口计数差，页大小 4,096 字节；历史累计 OOM 不计作本窗口故障。

| 指标 | 观察结果 |
| --- | ---: |
| 主机 CPU 平均 | 27.97% |
| CPU P50 / P95 | 19.29% / 79.69% |
| CPU 最高 | 100% |
| CPU ≥90% 累计 / 最长连续 | 15.06 秒 / 5.01 秒 |
| iowait 平均 / 最高 | 0.63% / 19.80% |
| 可用内存开始 / 结束 | 558.40 / 581.79 MiB |
| 可用内存最低 | 510.92 MiB（总内存约 13.7%） |
| Swap 占用开始 / 结束 | 370.25 / 423.38 MiB |
| Swap 累计换入 / 换出 | 1.12 / 54.77 MiB |
| 内存 PSI some / full 窗口比例 | 0.189% / 0.090% |
| 新增主机 OOM / 容器 OOM | 0 / 0 |
| 新增容器重启 / 容器身份变化 | 0 / 0 |

存在短时资源竞争和实际换出，但没有持续 CPU 打满、严重持续换入抖动或本窗口 OOM。可用内存结束时高于开始时，不能将 Swap 增长直接解释为内存泄漏；十分钟也不能排除长期泄漏。

| 服务 | CPU 平均（单核口径） | working set 开始→结束 | working set 峰值 |
| --- | ---: | ---: | ---: |
| market-data | 13.94% | 251.75→247.59 MiB | 253.59 MiB |
| postgres | 6.23% | 755.30→806.82 MiB | 824.74 MiB |
| primary strategy | 6.02% | 218.89→202.30 MiB | 267.96 MiB |
| account-2 strategy | 4.74% | 180.57→177.89 MiB | 180.57 MiB |
| account-4 strategy | 3.78% | 183.11→183.13 MiB | 183.37 MiB |
| account-3 strategy | 3.67% | 198.71→198.72 MiB | 198.74 MiB |
| dashboard | 2.58% | 94.76→94.27 MiB | 107.92 MiB |
| research-collector | 0.78% | 119.91→128.46 MiB | 128.46 MiB |
| 各 execution-account | 0.45%–0.52% | 结束 88.11–93.90 MiB | 最高 97.26 MiB |

Postgres `memory.current` 峰值 1,052.45 MiB，包括文件缓存；匿名内存 384.92→401.60 MiB。研究采集匿名内存 115.55→115.56 MiB，working set 小幅变化不能证明其对象持续堆积。所有容器本窗口 memory.events 的 max、oom、oom_kill 增量为零，CPU throttled 时间增量为零。

## 尚未解决的问题与建议顺序

### 1. 平仓计划版本过期会终止关键任务与策略进程

这是实际生产故障，发生在观察窗口**之前**，不能算作这十分钟新增重启：

| 北京时间 | 账户 | 事件 |
| --- | --- | --- |
| 18:30:07 | account-3 | `order_reservation_creation_failed_refusing_submission`：平仓计划的 projection token 与当前 Book 不一致 |
| 18:30:08 | account-3、account-4 | `live_runtime_critical_task_failed`，`OrderPreSubmissionError`，任务角色 `closed candle exit channel` |
| 18:30:18 | account-3、account-4 | `session_run_failed` |
| 18:30:42 | account-3、account-4 | `live_runtime_failed` |
| 18:30:58 / 18:30:59 | account-3 / account-4 | 容器重启；观察开始与结束时 RestartCount 都为 1 |

对应命令：

- account-3：`cml_61d02cdcbcb09a5ec581462adfbabce4`
- account-4：`cml_99f7104b864811dad82620e958b700f8`

执行侧拒绝旧仓位版本是正确的安全检查；问题在于该可预期的业务冲突被当作关键任务故障，导致整个平仓通道退出。校验位置为 `src/crypto_momentum_lab/execution_account/orders/coordinator.py:870`，闭合 K 线入口为 `src/crypto_momentum_lab/live_rollout/exit_processor.py:184`。

修复应保留版本校验，使用明确的业务冲突类型，读取最新 Book 后重新评估、分配平仓数量，并有界重试或明确延期处理。不能绕过版本检查、沿用旧分配量或仅吞异常后遗失退出请求。应增加在计划生成与最终准备之间推进 Book 版本的真实调用链回归测试。

日志能够证明“旧计划被拒绝→关键任务退出→重启”；现有证据不能确定具体哪个并发写入者推进了这两个 Book 版本，也不能据此断言相关订单之后始终未平仓。

### 2. 数据库连接余量和内存开销

中途与结束的只读检查均为 **88 个 idle 连接、1 个 active（本次诊断查询）**，`max_connections=100`。无锁等待、无超过十秒的 idle-in-transaction，当前没有数据库锁阻塞。这里的 idle 是空闲连接，不等同于持锁事务。

数据库设置：shared_buffers 256 MiB，work_mem 16 MiB，effective_cache_size 1 GiB；effective_cache_size 是规划器估计值，不代表实际预分配内存。

建议按进程与数据库平面核算所有连接池总预算，再收敛低吞吐服务的常驻池和 overflow，为交易和维护预留连接。88 个空闲连接是开销与容量风险的证据，尚不能证明是连接泄漏，也不能将 54.77 MiB 换出全部归因于数据库。直接提高 max_connections 会扩大资源预算，需要先核算。

### 3. 行情事件循环仍有短时延迟

观察窗口内有 **7 次** `market_data_event_loop_lag` warning，分别为 143、135、69、102、114、76、154 ms；最大 154 ms，未达到该告警设置中的 500 ms critical 阈值。

市场数据进程是应用容器中 CPU 平均最高者，但仍不足单核的 14%。十秒、49 Hz、非阻塞 GIL 采样只获得 31 个有效样本，并有 11 个读取错误，无法可靠归因某个具体热函数，亦不能解释全部主机峰值。后续应对齐闭合 K 线、数据库写入、日志和采集任务的时间，再覆盖完整周期分析；当前不能宣称所有额外开销已清除。

## 窗口结束时的业务与服务状态

- 12 个项目容器（11 个应用服务加 Postgres）在健康快照中均 healthy；没有新增重启或 OOM。
- 四策略 entry readiness 均开启，末次行情年龄约 0.42–0.45 秒。entry readiness 不是平仓可靠性证明。
- 四账户 durable head 的 command recovery gates 与 external recovery gates 均为零；execution_commands 仅出现 rejected、terminal 状态。
- 四策略和四 execution-account 的结构化日志中，本窗口未发现 warning/error/critical；market-data 有上述 7 次 warning，其他服务未发现这些等级事件。
- 该结论覆盖本窗口与已读取的重启历史，不代表所有潜在问题已排除。

分分钟指标见 [resource-observation-20261002-minutes.csv](resource-observation-20261002-minutes.csv)，容器指标见 [resource-observation-20261002-containers.csv](resource-observation-20261002-containers.csv)。原始秒级采样仍保存在服务器 `/tmp/cml-resources-20261002T103602Z.json`，本地 `/tmp/cml-observe-10m-raw.json`；这些临时文件不是长期归档。
