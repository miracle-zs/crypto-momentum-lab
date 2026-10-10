# 服务器部署、更新与回退

核对日期：2026-10-10。步骤对应 [update_server.sh](../../deploy/ops/update_server.sh)、[基础 Compose](../../compose.server.yaml) 与[账户 overlay](../../compose.live.accounts.yaml)。这里是操作说明，本轮未执行部署。

## 首次安装

服务器安装 Docker Engine/Compose/Buildx，将仓库放到 `/opt/crypto-momentum-lab`。建立权限 0600 的 `.env.server`，按 `.env.server.example` 配置数据库密码、镜像/代码标识等字段；实盘角色凭证按[实盘手册](small-capital-live-session.md) 配置。不得提交实际环境文件。

```bash
docker compose --env-file .env.server -f compose.server.yaml config --quiet
docker compose --env-file .env.server -f compose.server.yaml up -d --build
```

基础栈包含 PostgreSQL、一次性 migrate/初始化、market-data、research-collector 和 dashboard。实盘需显式 live profile；无 Paper runner。不要用 `--remove-orphans`，单文件操作可能把账户 overlay 服务视为 orphan。

Nginx 使用 `deploy/nginx/crypto-momentum-lab.conf`，先 `nginx -t` 再 reload。看板默认匿名，应通过现有 TLS、VPN 或私有隧道访问。

## 常规更新

先将目标提交推送到远端，从工作站执行：

```bash
deploy/ops/update_server.sh <server-host> <commit-sha>
# 更新正在运行的实盘账户：
deploy/ops/update_server.sh <server-host> <commit-sha> --live
# 只更新看板：
deploy/ops/update_server.sh <server-host> <commit-sha> --dashboard-only
# 只恢复正在运行的只读账户服务：
deploy/ops/update_server.sh <server-host> <commit-sha> --live --execution-accounts-only
```

优先 SSH key/agent。密码连接时由安全的本地环境提供 CML_SSH_PASSWORD，结束后 unset，不把密码放入命令参数或文档。用户、路径分别由 CML_SERVER_USER、CML_REMOTE_DIR 覆盖。

脚本锁定同主机部署，要求干净的 main checkout，获取目标版本并按变化路径选择构建/重启范围。只有 schema 路径变化才运行 migration；这是实际建表升级，不是发单热路径的合规证明。文档/测试修改通常跳过应用构建；脚本自己的运维变化按其分类处理。

显式 `--live` 只更新已运行账户，停止的账户不会隐式启用。旧 Live 进程停下后再创建替代进程，防止同账户两个策略同时运行。重启前归档旧容器日志。失败重跑相同命令使用已有阶段信息；出现新目标时包含未完成的变更范围。

当前脚本不接受 `--refresh-approvals`，不运行已删除的 preflight，不要求续租。readiness 是诊断快照；部署仍核对容器健康、账户身份与实际镜像，不以内部预热计数代替健康检查。

## 超时与验证

|设置|默认秒数|
|---|---:|
|CML_DEPLOY_WAIT_TIMEOUT_SECONDS / CML_CONSUMER_WAIT_TIMEOUT_SECONDS / CML_LIVE_WAIT_TIMEOUT_SECONDS|300|
|CML_MARKET_DATA_WAIT_TIMEOUT_SECONDS|900|
|CML_LIVE_STOP_TIMEOUT_SECONDS|90|
|CML_DEPLOY_OPERATION_TIMEOUT_SECONDS|300|
|CML_DEPLOY_BUILD_TIMEOUT_SECONDS|900|

CML_LIVE_CONCURRENCY 默认 2。失败、重启、OOM 或超时应先查对应 phase/service 日志，不通过不断增大超时掩盖问题。看板默认还检查本机和反向代理 `/api/health`；仅在确实不用代理时设置 CML_DASHBOARD_REQUIRED=0。

```bash
docker compose --env-file .env.server -f compose.server.yaml ps
docker compose --env-file .env.server -f compose.server.yaml logs --tail=200 market-data research-collector dashboard
curl -fsS http://127.0.0.1:8765/api/health
curl -fsS http://127.0.0.1/momentum/api/health
```

实盘验证另核对账户隔离、实际订单状态、剩余持仓与后台未知单；至少覆盖正式收盘和已有宽限截止。无自然交易样本时报告“未覆盖新订单”，不要构造订单只为证明健康。持续观测需记录精确窗口、重启增量、CPU/内存和订单分段耗时。

## 回退

选择明确的已验证祖先提交，通过同一脚本部署并带上原来的 profile 参数。先检查 schema 和持久化格式是否兼容；代码回退不自动撤销数据库迁移，也不能把旧版本已删协议当作兼容版本。保留未决订单身份、真实成交与原宽限截止，禁止清库或重复 POST 来恢复服务。

## 主机空间维护

`cml-housekeeping.timer` 每日清理主机运维文件，不删除 PostgreSQL 数据或 `/var/lib/crypto-momentum-lab/table-archive`。默认保留 7 天崩溃日志、当前及最新两个应用镜像、近 7 天使用的构建缓存和 300 MB systemd journal；运行中容器所用镜像始终保留。

更新版本后安装并启用维护单元：

```bash
install -D -m 0755 deploy/ops/cml_housekeeping.sh /opt/crypto-momentum-lab/deploy/ops/cml_housekeeping.sh
install -D -m 0644 deploy/ops/cml-housekeeping.service /etc/systemd/system/cml-housekeeping.service
install -D -m 0644 deploy/ops/cml-housekeeping.timer /etc/systemd/system/cml-housekeeping.timer
systemctl daemon-reload
systemctl enable --now cml-housekeeping.timer
```

手动运行及查看下次计划时间：

```bash
systemctl start cml-housekeeping.service
systemctl list-timers cml-housekeeping.timer
```

保留期可通过 systemd service override 调整，例如 `CML_CRASH_LOG_RETENTION_DAYS=14`。应用镜像保留数量应至少为 3，除非明确接受无法快速回退。
