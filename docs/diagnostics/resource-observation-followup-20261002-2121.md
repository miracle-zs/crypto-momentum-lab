# 服务器十分钟复测：2026-10-02 21:21–21:31

完整采样结束，未发现持续 CPU 满载、OOM、容器重启或数据库锁阻塞；但 21:30 新开仓后的成交事实与仓位投影短暂不一致，让四账户的 MAGICUSDT 平仓通道进入保护降级。均已自动恢复，不能将其描述为不存在的异常。短时 CPU 尖峰和换页仍存在。

## 范围与方法

服务器 43.167.191.253；UTC 2026-10-02T13:21:00.806601+00:00 至 2026-10-02T13:31:01.458797+00:00，实际 600.652 秒，594 个主机样本。主机约一秒采样，容器 cgroup 约五秒采样，健康状态约一分钟采样。额外核对日志至 21:32，数据库末次核对约 21:33。四策略与四 execution-account 运行 2e3a65e1；行情、面板及研究采集保持 7d4f6f04。

CPU 主机口径为两核总容量，容器为单核百分比；working set 是 cgroup memory.current 减 inactive_file，并非 RSS。总 CPU 均值按采样间隔加权，分分钟 CSV 为样本算术平均。未重启、部署或手工下单。

采样进程用时 2.00 CPU 秒，采集错误 0。窗口内还运行了只读日志和数据库诊断；数据库 Python 进程在 primary 容器内运行，因此该容器的内存峰值及部分 CPU 包含诊断开销。未将这些峰值直接判为交易进程泄漏，也未扣除无法精确分离的诊断开销。

## 资源结果

| 指标 | 上轮 20:59–21:09 | 本轮 21:21–21:31 |
| --- | ---: | ---: |
| CPU 加权均值 | 36.37% | 36.03% |
| CPU P95 | 83.76% | 93.50% |
| CPU ≥90% 累计 | 17.08 秒 | 35.91 秒 |
| CPU ≥90% 最长连续 | 2.00 秒 | 8.82 秒 |
| 最低 MemAvailable | 779.95 MiB | 710.29 MiB |
| Swap 换入 | 1.49 MiB | 9.58 MiB |
| Swap 换出 | 32.82 MiB | 16.30 MiB |

CPU P50 27.76%，峰值 100%，iowait 平均 0.67%、最高 38.14%。可用内存 771.42→733.64 MiB，总内存 3723.91 MiB。Swap 占用 447.62→460.21 MiB；占用变化和换入换出不是同一指标。

12 容器全部健康采样通过，身份未变化、重启增量为零；主机 OOM 和各容器 memory.events oom/max 增量均为零，没有 CPU throttling。内存 PSI some/full 约 0.165% / 0.085%。磁盘 55%，剩余约 26 GiB；inode 使用 14%，窗口内内核 warning 日志为空。

最长高 CPU 段位于 21:30:52–21:31:00，与成交事实修复及只读数据库诊断重叠。时间重叠不能证明它全部由某一业务函数引起。CPU 均值与上轮接近，但 P95 与最长高负载时间变差，因此不能宣称性能全面提升。换页仍在发生，十分钟不足以排除长期内存问题。

| 服务 | CPU 均值（单核） | working set 开始→结束 | working set 峰值 |
| --- | ---: | ---: | ---: |
| market-data-1 | 24.20% | 221.63→212.74 MiB | 221.77 MiB |
| postgres-1 | 6.68% | 735.12→639.92 MiB | 747.38 MiB |
| live-strategy-1 | 5.02% | 228.44→247.20 MiB | 276.34 MiB |
| live-strategy-account-2-1 | 4.52% | 178.87→178.86 MiB | 180.90 MiB |
| live-strategy-account-3-1 | 4.41% | 186.52→186.52 MiB | 186.52 MiB |
| live-strategy-account-4-1 | 4.33% | 187.08→187.09 MiB | 188.18 MiB |
| dashboard-1 | 3.20% | 92.07→91.23 MiB | 104.41 MiB |
| research-collector-1 | 0.99% | 100.48→106.29 MiB | 106.50 MiB |
| execution-account-live-account-3-1 | 0.66% | 88.44→89.20 MiB | 90.88 MiB |
| execution-account-live-1 | 0.63% | 89.46→89.43 MiB | 91.03 MiB |
| execution-account-live-account-4-1 | 0.63% | 89.28→89.27 MiB | 91.77 MiB |
| execution-account-live-account-2-1 | 0.63% | 89.29→89.27 MiB | 91.23 MiB |

market-data 仍为主要业务 CPU 消耗来源。额外 21:26:36–21:27:21 的 45 秒进程采样中，行情主进程为单核 21.28%，dockerd 2.47%，YDService 2.15%，barad_agent 1.62%；该短窗口及进程口径不能与完整窗口的容器统计直接相加。

## 收盘处理与当前阻塞状态

21:30:46，四账户均创建 MAGICUSDT entry 命令。末次只读数据库核对显示四单均 terminal，填充数量各为 1671.4、成交额各为 99.999862 USDT，持久化 external order ID 存在，last_error 为空。这是现有系统自行发出的实盘开仓，本次诊断未制造交易。

21:30:53，四账户各记录一次 live_closed_candle_exit_degraded，原因为 unmanaged_live_positions:MAGICUSDT，并调度一秒后的 grace 重试。实际修复日志包括：

- primary：repair projection does not match actual account exposure；21:30:54 auto_healed_success，新增 4 个事实。
- account-2/4：complete account fill facts are missing；21:30:54 auto_healed_success，各新增 1 个事实。
- account-3：complete account fill facts are missing，随后 repair context advanced during fact load；21:30:55 tradeability_recovered。未观察到与另外三账户相同的 auto_healed_success 日志，不能杜撰该事件。

这些日志证明新成交后的事实装载及投影一致性窗口触发了实际保护；尚未以完整事件序列确定最初是哪一条通知先后顺序造成不一致，也未证明策略当时有应执行的平仓信号。因此这里的“平仓通道降级”不等于“本应平仓却永久漏单”。

| 账户 | 禁用开仓 | 恢复开仓 | 日志时间差 |
| --- | --- | --- | ---: |
| primary | 21:30:55 | 21:30:57 | 2 秒 |
| account-2 | 21:30:55 | 21:30:56 | 1 秒 |
| account-3 | 21:30:54 | 21:30:55 | 1 秒 |
| account-4 | 21:30:55 | 21:31:08 | 13 秒 |

末次四账户 readiness 均 entry_enabled=true，行情年龄约 0.42–0.44 秒；command/external recovery gates 均为零，命令状态仅 rejected/terminal。数据库约 55 个 idle、1 个 active（含诊断），无锁等待和超过十秒的 idle-in-transaction。21:30 附近曾采到一个短时 idle-in-transaction，随后消失，不能称作长事务死锁。没有关键任务崩溃或 session_run_failed。

## 仍需关注的事项

1. 新开仓后的短暂 unmanaged 保护会让退出通道降级，且 account-4 开仓暂停达 13 秒；已恢复，但这一并发事实同步窗口仍有待进一步复现和收敛。不得用删除保护或放宽归属证明来消除告警。
2. CPU 仍有峰值 100%，最长 ≥90% 达 8.82 秒，持续满载未见，但尾部负载不能忽略。
3. Swap 本轮换出 16.30 MiB、换入 9.58 MiB，内存余量最低 710.29 MiB；目前无 OOM，不能据此保证无长期泄漏。
4. 行情服务精确窗口内有一次 event_loop_lag 告警，150 ms；比上轮最高 592 ms 低，但生产活动不同，不能作为严格 A/B 改进证明。
5. 窗口结束附近 primary checkpoint 日志记录 total 589.872 ms、pool_acquire 420.88 ms；没有持续数据库锁阻塞，但存在瞬时等待。其发生于诊断重叠时段，不能直接判为连接池尺寸错误。

## 明细与原始数据

- [分分钟统计](resource-observation-followup-20261002-2121-minutes.csv)
- [容器统计](resource-observation-followup-20261002-2121-containers.csv)
- 服务器原始秒级记录：/tmp/cml-resources-20261002T132100Z.json
- 本地临时原始记录：/tmp/cml-resource-followup-20261002-raw.json
- 本地精确窗口日志审计：/tmp/cml-resource-followup-20261002-audit-final.log
- 本地收盘后扩展日志：/tmp/cml-resource-followup-20261002-primary-candle.log、/tmp/cml-resource-followup-20261002-accounts-candle.log
- 本地末次只读数据库记录：/tmp/cml-resource-followup-20261002-db-extended.log
