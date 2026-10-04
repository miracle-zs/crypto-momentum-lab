# ADR-0001：实盘读与交易凭证分离

状态：已在当前代码和 Compose 中落地；核对日期 2026-10-04。该状态不证明服务器实际密钥权限。

账户同步进程观察私有账户事件并补查事实，策略进程提交和撤销订单。两者按角色解析凭证，减少共享交易权限造成的影响范围；当前代码不提供通用 BINANCE_API_KEY 回退。

|角色|变量|使用者|
|---|---|---|
|read|BINANCE_READ_API_KEY / BINANCE_READ_API_SECRET|execution-account|
|trade|BINANCE_TRADE_API_KEY / BINANCE_TRADE_API_SECRET|live-strategy|

账户 2–4 使用对应 ACCOUNT_2/3/4 后缀。生产密钥权限与 IP 限制须在交易所确认；凭证名称本身不能证明只读。两个角色均不需要提现权限。

密钥只进入受保护的环境文件或密钥管理器，不进入 Git、诊断载荷或看板。轮换时更新对应服务映射并验证账户流或交易客户端，不通过恢复旧通用密钥分支回退。操作步骤见[实盘手册](../runbooks/small-capital-live-session.md)。
