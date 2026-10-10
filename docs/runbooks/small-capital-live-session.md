# 实盘启动、退出与四账户操作

核对日期：2026-10-10。当前运行策略仅 Orderflow。运行参数由[运行清单](../../deploy/live-runtime.yaml)、Compose 环境变量和 PostgreSQL 账户风险配置共同决定；本手册不能证明服务器当前实效值。租约、影子证据、Git/migration 比对及旧 preflight 不再是正常运行门槛。

## 账户与环境

账户观察与交易使用不同凭证：`BINANCE_READ_API_KEY/SECRET` 供 `execution-account` 同步账户，`BINANCE_TRADE_API_KEY/SECRET` 供 `live-strategy` 发单；账户 2–4 使用对应 `ACCOUNT_2/3/4` 后缀。代码不回退到通用 `BINANCE_API_KEY`。密钥存入权限受控的环境文件或密钥管理器，不放入 Git、日志或看板。交易所侧确认权限和 IP 限制；密钥名本身不能证明只读，两个角色都不需要提现权限。轮换时更新对应服务映射并验证账户流或交易客户端。

在服务器仓库目录建立权限 0600 的 `.env.server` 与 `.env.live`，字段参考根目录示例文件。所有账户使用合并 Compose，显式指定服务：

```bash
COMPOSE="docker compose --env-file .env.server --env-file .env.live -f compose.server.yaml -f compose.live.accounts.yaml --profile live"
$COMPOSE config --quiet
$COMPOSE up -d execution-account-live
$COMPOSE ps execution-account-live
```

先确认账户同步健康、当前仓位和订单身份；已有持仓不能当作新账户空仓重置。执行、行情、观测默认使用同一 PostgreSQL 的不同连接池；调整连接预算见[数据库手册](postgres-operations.md)。

## 风险配置与启动

已有账户沿用当前有效风险配置。首次配置或确需修改限额时使用 `prepare`，先查看该目标镜像的参数，显式填写所需账户和限额，不复制旧文档中的 unlimited 示例：

```bash
$COMPOSE run --rm --no-deps live-strategy prepare --help
$COMPOSE run --rm --no-deps live-strategy strategy-config-hash --account-label primary --runtime-manifest /app/deploy/live-runtime.yaml
$COMPOSE up -d live-strategy
$COMPOSE ps execution-account-live live-strategy
$COMPOSE logs --tail=200 live-strategy
```

`prepare` 仍可能写入历史租约字段；当前交易内核不依赖续租。旧 `approve` 行政接口及确认文本 `ENABLE SMALL LIVE TRADING` 仍存在，但不是当前运行必须经过的准入层。不要运行已删除的 preflight 或部署参数 `--refresh-approvals`。

Compose 的真实运行包含 `--i-understand-this-places-real-orders`；手工 run 同样需要该参数。检查风险配置和 runtime manifest 的完整展开结果，不能只比较策略哈希。源码/迁移字段可用于标识部署，不作为每次交易的证明链。

## 参数目标与实效核验

四个账户的统一参数目标记为 `2/1/1.25%/2.0/0.0x/slots=1`：脉冲桶、确认桶、最小收益、最小强度、5m/30m 成交额比和最大持仓槽位。这个简写不包含 `min_imbalance` 等完整策略字段；上线前仍须比较每个账户的完整展开清单。

本次仓库盘点发现，跟踪的 Compose、`deploy/live-runtime.yaml` 和环境示例中的回退值仍是最小收益 `0.75%`、最小强度 `3.0`、5m/30m 成交额比 `1.25x`；`slots` 则由 PostgreSQL 的账户风险配置提供。若环境未显式覆盖策略字段，仓库回退值与上述目标不一致。文档更新不会改变运行配置。

每次启动或更新前，逐一核验 primary、account-2、account-3、account-4 的实际环境和完整 runtime manifest，并读取各自的 `risk_config` 确认 `slots=1`。不要用 `max_concurrency_per_symbol` 代替最大持仓数，也不要只比较策略哈希。若部署文件或服务器配置未与目标一致，先按本次配置变更流程对齐，再启动实盘。

参数桶为 15 秒。四账户默认做多、正涨幅 Top30、LIMIT 开仓 TTL 900 秒、每次目标名义金额 100 USDT、5x 杠杆、无 EMA5/EMA10 入场过滤；实际值仍以完整运行清单为准。信号释放不是成交保证；限价未成交可能到期撤销。

退出跳过最新开仓所在 15m K 线，从下一完整 K 线开始按正式收盘事件评估。当前实现使用实时可执行报价，缺报价时使用蜡烛参考价比较直接退出阈值 0.001；否则挂批次均价上方 0.0088 的 GTC 宽限退出。宽限为 8 根，原截止时间到达后撤销限价并对剩余数量请求 reduce-only 市价退出，无需等待新行情。**不是下一根收盘就平仓，也不是等到每日 07:45 才平仓。**计时边界见[系统架构与交易契约](../architecture/overview.md)。

同批次追加成交按数量加权；提交退出后的新成交属于新批次。未知订单和真实预留通过原身份恢复，不能换身份重挂同一数量。旧批的 reduce-only 订单若仍作用于聚合仓位，必须先确认或撤销其责任，不能把预留量当成新批可用仓位；宽限到期退出也遵守这一点。每日定时风险窗口是独立配置，不代替宽限截止；查看本次展开配置是否启用。

## 四账户

账户 2 示例：

```bash
$COMPOSE up -d execution-account-live-account-2
$COMPOSE up -d live-strategy-account-2
$COMPOSE ps execution-account-live-account-2 live-strategy-account-2
```

账户 3、4 用对应服务名。避免不带服务名的 `$COMPOSE up -d` 意外启动全部实盘。每账户独立凭证、会话、风险配置和账户 Hub，共享行情；持仓标的在离开涨幅池后仍应受行情订阅保护。

## 停止新开仓与平仓

disable-new-entries 写入持久化操作状态，正常 reduce-only 退出继续；命令要求本会话实际策略/风险哈希：

```bash
$COMPOSE run --rm --no-deps live-strategy disable-new-entries --help
$COMPOSE run --rm --no-deps live-strategy request-flatten --help
$COMPOSE run --rm --no-deps live-strategy report --session-id "$CML_LIVE_SESSION_ID"
```

明确需要全账户平仓时，request-flatten 使用稳定 idempotency-key 和确认 `EMERGENCY FLATTEN LIVE ACCOUNT`。命令被接受不等于账户已平：核对交易所剩余持仓、挂单和本地成交。账户级 flatten 不是指定单个 symbol 的替代操作。

Disable the live-submit configuration immediately after the session：计划结束时先阻止新开仓，等待退出和未知订单收敛，再停止策略；只读账户同步保留到核对完成。不要在仍有仓位时停掉退出通道、释放真实预留或改账户模式。
