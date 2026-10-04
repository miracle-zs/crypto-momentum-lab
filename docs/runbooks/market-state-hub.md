# 行情与账户 Hub

核对日期：2026-10-04。Hub 是内部输入通道，PostgreSQL 保存恢复与审计事实。

```text
Binance 公共行情 → market-data → MarketState15s Hub → 各账户策略
                          └→ PostgreSQL / 研究采集器
Binance 私有 WS / REST → execution-account → AccountEventHub → 本账户交易进程
                                           └→ 持久化账户与成交事实
正式 15m 收盘 WS → 独立退出事件通道 → LiveExitProcessor
```

## 地址与模式

|服务|内部地址|用途|
|---|---|---|
|market-data|8766|15 秒状态|
|market-data|8768|实时报价/成交额|
|execution-account-live 及账户后缀服务|8767|对应账户事件|
|同上|8769|低频操作控制推送|

实时默认 `--market-state-source hub`。`postgres` 仅为显式诊断/恢复选项。策略正式收盘退出拥有独立 Binance K 线订阅，不能写成“所有策略都不连交易所行情”。

## 流连续性

Hub 维护 epoch、sequence 和有界缓冲。消费者使用独立 reader，避免策略/数据库工作反压 socket；缓冲溢出和序列缺口必须可观察。

行情缺口清除受影响标的的旧滚动指标并重新预热；实时有限回放不能冒充完整持久化历史。账户流先接收完整快照，再按 delta 更新有效快照；缺口或 epoch 改变需要重新取得 full snapshot。成交事实去重与恢复仍由持久化账本负责，不能仅凭内存快照归零抹去历史。

账户处理/持久化失败或接收溢出请求重连和事实补查。不得将未提交事件当作已持久化事实发布，也不得以全账户历史补采同步阻断其他健康标的退出。具体约束见[执行契约](../architecture/execution-contracts.md)。

## 操作推送

disable-new-entries、cancel-all-open-entries、request-flatten 先写持久化记录，再向 RiskControlHub 推送 command_id。可配置 CML_RISK_CONTROL_HUB_TOKEN；推送失败仍可读取持久化请求。消费者按账户序列确认连续性，再筛选会话；不能因另一会话事件制造假缺口。

该通道控制明确的操作员动作，告警和后台对账不通过它制造全局停止状态。看板只读。flatten 已接受不表示交易所仓位已经归零。

## 延迟测量

订单按 account + symbol + position_side 排序；reduce-only/撤单优先，后台恢复有独立预算。测量候选产生、准备提交、开始请求、响应、成交事实分别耗时；15 秒桶开始时间不是信号创建时间。上下文预取只有在事实版本仍有效时可使用，不能将缓存命中当作真实账户一致性证明。
