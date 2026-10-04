# 历史故障、修复与观测边界

整理日期：2026-10-04。以下均为日期和版本限定的历史证据，不代表服务器当前故障。本轮只整理文档，未连接服务器、平仓、部署或修改业务数据。原报告在 Git 历史；本目录 JSON/CSV 保留原始字段和采样，不覆盖事实。

## 交易与恢复故障

|时间/范围|已定位的问题与修复|证据/长期回归|
|---|---|---|
|2026-09-25 批次重建|两套持仓模型按不同事实和切面比较，外部平仓后残留旧生命周期；事实账本、准确批次分配替代猜测数量裁剪|runbooks 下两个 repro 脚本；批次/PositionLedger 回归|
|2026-09-29 修复发布|提交与重载窗口、旧 stream 选择及 epoch/sequence 接续导致重复恢复|repair publication race、epoch recovery、terminal settlement 回归|
|2026-10-01 事实与退出回执|零仓基线/覆盖证明生产接线缺失，Hub 丢原始身份载荷；仅以本地零仓取消待退出；修复明确来源和原订单回执核验|[验收 JSON](../architecture/business-anomaly-recovery-evidence-20261001.json)|
|2026-10-02 隐藏持有时限|RuntimeOrchestrator 注入 1200 秒 max_holding_period，与 K 线退出不同；已禁用该实盘时限，既有意图按原身份核对|实际 USUSDT 退出原因不是预期 K 线/宽限退出；不得把它描述为到期正常平仓|
|2026-10-02 订单身份与外部成交|成交与命令订单身份未关联、外部 SELL 引发跨仓位保护；绑定真实交易所身份并按 PositionKey 隔离恢复|execution identity、external recovery 和账户观察回归|
|2026-10-02 仓位版本竞争|排队期间数量/版本变化使退出通道崩溃；刷新重评保留原正式收盘事件，重新按当前批次分配|closed candle replay、exit projection retry 回归；[事实顺序 JSON](entry-fact-order-evidence-20261002.json)|
|2026-10-02 已消耗预留恢复|仅加载 ACTIVE 预留丢失终态结算证明；按准确命令预留身份恢复已消耗记录|真实 PostgreSQL 重启/终态结算回归|
|2026-10-02 FILLED 缺成交价|Binance 回报有数量但价格暂缺，零价事实导致策略重启；归类未知并保留预留，只查原订单，价格补齐再结算|真实客户端→状态机→Coordinator→Book 回归，禁止委托价兜底和补发|
|2026-10-02 宽限 LIMIT 缺 GTC|VELVET/FLUID 在客户端发包前因缺 time_in_force 被拒绝；计划生产者补 GTC|[拒绝证据](grace-limit-tif-evidence-20261002.json)、test_grace_limit_exchange_contract.py|
|2026-10-03 已有宽限预留丢失|订单行不含 batch allocations，视图漏活动预留导致下一收盘重复退出；按 client_order_id 关联 committed reservation|test_grace_reservation_visibility.py；[失败窗口](grace-reservation-failed-observation-20261003.json)、[修复窗口](grace-reservation-final-observation-20261003.json)|
|2026-10-03 启动装配时序|只读对账创建队列被当作发单开始，后续 configure_submission 失败；冻结点改为真实提交开始|coordinator 配置/启动恢复回归|
|2026-10-03 迟到旧快照|checkpoint 前快照晚落库被错误判冲突，primary/account-2 反复重启；旧快照按 event_cut 跳过但保留身份冲突校验|[只读重放与验收](checkpoint-snapshot-recovery-evidence-20261003.json)、test_checkpoint_snapshot_recovery.py|
|2026-10-03 缺证据与调度|缺覆盖被归为真实冲突，普通账户事件反复触发历史恢复；区分等待证据，按订单/退出/仓位任务预算调度|[02e6581f 验收](recovery-convergence-rollout-evidence-20261003.json)|
|2026-10-03 新版本装配回归|af7ba86a 本地失败注入发现装配异常清理及阶段分类缺口；不可用该版本的解耦成果证明所有生产问题修复|[版本对照审计](server-runtime-audit-20261003.json)|
|2026-10-03 RuntimePlan 属性错误|729170e9 的 API3/AR 七笔候选在 POST 前因 strategy_name 属性不存在被拒；0d386bcb 修复后自然退出获 ACK|[部署证据](live-deployment-performance-20261003.json)|
|2026-10-04 AR 操作员平仓|用户要求只平 AR；primary 已平未发单，账户 2–4 各平 21.1，随后均零仓无挂单|[操作记录](operator-ar-close-20261004.json)，不是宽限自动退出成功证据|

## 性能与失败窗口

行情 15 秒桶关闭时四策略集中计算，与主机 CPU 排队和事件循环 lag 同时出现。重复扫描已收窄；156 桶局部函数中位耗时 0.6633→0.08549 ms，仅是该函数基准，不是整机或发单提速倍数。详细采样见[扫描性能 JSON](impulse-scan-performance-20261002.json)。

|日期/版本与窗口|实际观察|限制|
|---|---|---|
|10-02 23766550 十分钟|CPU 均值 34.35%，随后 20:30 收盘缺成交价导致重启|资源窗口健康不代表下一收盘业务通过|
|10-03 GTC 首轮十分钟|CPU 均值 75.37%、P95 100%，三策略重启，无 OOM|重复预留/启动故障窗口；重启后容器 CPU 计数不可直接比较|
|10-03 8fafa30f 十分钟|修复后 CPU 均值 32.63%、12 容器无重启/OOM|行情 lag 仍约 2 秒，峰值验收未全通过|
|10-03 02e6581f 10:11–10:21 北京时间|CPU 均值 23.85%，无交易服务 warning/error，恢复状态收敛|无新订单；行情 lag 最大 514 ms；不是完整实盘生命周期验收|
|10-03 0d386bcb 14:27–14:31 UTC|七笔 API3/AR 出场获 ACK，账户 3/4 两笔 ZRO 成交结果 629.501/1136.603 ms|约四分钟，不能当等长十分钟 A/B；结果耗时不是纯 POST RTT|
|10-04 文件名的后续采样|记录实际观测时间 10-03 16:25 UTC 的 15/60 分钟指标|[后续 JSON](live-performance-followup-20261004.json)，按载荷时间读，非实时状态|

资源分分钟/容器 CSV 按前缀区分初始、interim、final、deployed 和 followup；保留失败窗口，不只摘成功数字。观测时间、市场负载、启动恢复与 profiler 不同，不能将差异全归因于某一改动。

## 仍需复核

- 宽限首次截止、撤单与剩余市价成交按实际订单逐笔核对；不能由健康、ACK 或某笔每日定时退出推断其他账户未延迟。
- 启动历史查询成本、数据库连接等待、检查点突发构建、行情尾部延迟与 swap 压力仍需同口径测量。
- 研究采集器曾报告 247 条历史 Parquet 状态载荷冲突；保留旧记录不是已解决来源问题。
- 延迟导致的资金损失需原预期截止、实际成交、数量、可执行价格和费用逐笔归因；本记录不提供未经核验的损失总额。

当前代码剩余工作只维护在[简化状态](../architecture/simplification.md)，不要把旧报告中的“当前”“已完成”直接套到今天。
