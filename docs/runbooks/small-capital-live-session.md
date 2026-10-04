# 实盘启动、退出与四账户操作

核对日期：2026-10-04。当前运行策略仅 Orderflow；[运行配置](../../deploy/live-runtime.yaml) 和 Compose 是参数来源。租约、影子证据、Git/migration 比对及旧 preflight 不再是正常运行门槛。

## 账户与环境

账户观察与交易分别使用 read/trade 角色凭证，见 [ADR-0001](../adr/0001-live-trading-credential-boundary.md)。primary 无后缀，账户 2–4 使用 ACCOUNT_2/3/4。密钥存入受保护环境文件或密钥管理器；不放入 Git、日志或看板。交易所侧确认 Hedge Mode、权限和 IP 限制；新标的按实际配置确认杠杆和保证金模式。

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

## 当前交易规则

四账户默认做多、正涨幅 Top30、LIMIT 开仓 TTL 900 秒、每次目标名义金额 100 USDT、5x 杠杆、无 EMA5/EMA10 入场过滤；环境覆盖后的值以实际 manifest 为准。

|账户|脉冲桶|确认桶|最小收益|不平衡|强度|5m/30m 成交额比|冷却桶|
|---|---:|---:|---:|---:|---:|---:|---:|
|primary / account-2|2|1|0.005|0.30|4.0|1.50|0|
|account-3 / account-4|3|1|0.015|0.30|1.5|0.00|0|

参数桶为 15 秒。信号释放不是成交保证；限价未成交可能到期撤销。

退出跳过最新开仓所在 15m K 线，从下一完整 K 线开始按正式收盘事件评估。当前实现使用实时可执行报价，缺报价时使用蜡烛参考价比较直接退出阈值 0.001；否则挂批次均价上方 0.0088 的 GTC 宽限退出。宽限为 8 根，原截止时间到达后撤销限价并对剩余数量请求 reduce-only 市价退出，无需等待新行情。**不是下一根收盘就平仓，也不是等到每日 07:45 才平仓。**计时边界的实现见[执行契约](../architecture/execution-contracts.md)。

同批次追加成交按数量加权；提交退出后的新成交属于新批次。未知订单和真实预留通过原身份恢复，不能换身份重挂同一数量。每日定时风险窗口是独立配置，不代替宽限截止；查看本次展开配置是否启用。

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
