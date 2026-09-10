# 部署脚本审查逐项验收

复核日期：2026-09-10。对象：code-review-2026-03-19.md 新增“部署脚本审查”。已覆盖 D1–D10、4 条小问题、7 条正面评价。

依据为当前工作区，不代表线上实际配置。未连接服务器、未读取真实 .env.server、未启动或重启容器。初始 D1–D10 结论记录的是代码修复前的触发事实；`c62cd52` 已先行修复其中的回滚分支、D1/D2/D3/D5，后续二次验收状态见文末。仅执行本地配置解析、纯函数/隔离 Git 仓库复现及既有测试。

## D1–D10

| 项目 | 验收 | 核实结果与修正建议 |
|---|---|---|
| D1：无条件加载 live overlay/profile | 成立，已复现 | [compose 数组](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/deploy/ops/update_server.sh:463)始终加载 overlay。用临时假环境文件满足基础 manifest 的必填变量但不提供 account-2/3/4 凭证：base config 成功，base+overlay 无论是否启用 live profile 均因缺凭证失败。故真正关键是是否加载含必填变量的文件，不只是 profile。文档/测试类变更在 runtime_unchanged 分支可提前退出，不能说所有 paper 更新必失败。脚本重启命令仍明确列服务并受 live_update 判断，不等于会自动启动所有 live 账户。建议拆分 paper/base、primary live、附加账户配置，并按实际更新范围选文件；只用 --live 二分仍会让仅 primary 的 live 更新依赖其余三账户凭证。 |
| D2：strategy 路径拼错 | 成立，已复现 | [分类分支](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/deploy/ops/update_server.sh:375)写 strategy/*，实际为 strategies/*；strategy_runner/* 也未单列。提取原 case 执行，两类文件均得到 market/research/paper/dashboard/live=1。新镜像不收敛时会造成不必要的 market-data 重建。必须加限定：live 容器只有传 --live 才进入重启阶段，且 service_is_converged 可跳过；“也会强制重启 live”过宽。建议修两个路径，并用行为测试固定分类；不要机械断言所有 strategies 变更只影响 paper/live，仍需考虑 research 等实际 import 依赖。 |
| D3：ops-monitor 只盯 primary | 默认配置下成立，风险高 | [默认服务列表](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/deploy/ops/cml_ops_monitor.py:27)仅 postgres、market-data、primary execution/live；[systemd](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/deploy/ops/cml-ops-monitor.service:13)只有基础 compose 文件。[容器循环](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/deploy/ops/cml_ops_monitor.py:473)只扫描配置服务，[日志检查](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/deploy/ops/cml_ops_monitor.py:399)还硬编码 live-strategy。DB 查询只接收一组 run_id/account/lease，默认 primary，但 build_config 可从 CLI/env/.env 覆盖，所以不是永远硬编码 live-primary-v1。应同时支持多文件、按账户服务及日志检查、多组 DB liveness；只改服务列表或 overlay 不完整。未检查 /etc 下真实 override，不能断言线上一定失明。 |
| D4：primary 空凭证不 fail-closed | 描述与建议均需修正 | Compose 层可空、附加账户必填属实；但[凭证解析器](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/config/credentials.py:77)在构造客户端前拒绝缺失/空白值，READ/TRADE 均已本地复现。这是失败发生在容器应用启动期，而非配置解析期，不是允许无凭证交易。建议原样在 base 中改为 :? 有反效果：本地最小 Compose 复现证明未启用的 profile 内必填变量仍触发插值失败，会阻断 paper-only。应先隔离 live 配置或增加仅针对拟更新账户的预检；不建议按“立刻实盘安全 bug”排序。 |
| D5：持仓保护 labels 手工维护 | 成立，且还有一处遗漏 | [loader](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/market_data/main.py:677)只读取配置账户的持仓；example 复数列表注释掉，默认只查 primary。因此额外账户独有持仓 symbol 离开常规 universe 后，可能失去额外订阅保护；不是额外账户全部行情立即消失。另一个已复现问题：[parse_live_position_account_labels](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/apps/market_data/main.py:193)在复数列表非空时直接取代单数；配置 primary + plural=account-2 的结果只有 account-2，尽管注释声称 additive。应明确是全量替换还是合并，验证所有需保护账户。只从 running 服务发现也不充分：已停策略但仍有仓位的账户同样需要保护。 |
| D6：遥测持久化默认值不同 | 已验收 | primary 与 account-2/3/4 现在统一默认 `submit,cancel`，只持久化 exchange 请求/响应的写边界；order/fill 账本不受该 allow-list 影响。需要临时扩大排障范围时，将对应变量明确设为 `all`；CLI 解析仍兼容旧的空值全量语义，但 Compose 的空值/未设置值按新的 `submit,cancel` 默认处理。部署 manifest 与 CLI 解析测试覆盖默认值和 `all` 模式。 |
| D7：env 与 argv 重复 | 第一阶段已修复 | live strategy 的七维 profile 与 top-N 参数已从四个 service 的 argv 移除，环境变量成为 long-running daemon 和 `preflight`/`strategy-config-hash` 解析器的共同来源；账户、会话、hub、风险和退出参数仍显式保留在 argv。manifest 回归断言八个环境键存在且对应 CLI 选项不存在，避免再次出现双注入。 |
| D8：nginx 无应用层鉴权 | 默认发布模板的保护缺口成立，标题不准确 | [nginx snippet](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/deploy/nginx/crypto-momentum-lab.conf:5)无本地 auth/allowlist；但 [dashboard API](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/operator_dashboard/api.py:208)已有可选 Basic Auth，不能说应用没有鉴权。当前 compose 未注入 CML_DASHBOARD_USERNAME/PASSWORD，example 也未配置，故按模板单独部署不启用应用鉴权。上层 nginx/VPN/防火墙可能保护，未查真实服务器，不能断言公网裸露。若启用应用 auth，[api/health 同样受保护](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/src/crypto_momentum_lab/operator_dashboard/api.py:299)，现有 Docker healthcheck 和 update_server 的无凭证 curl 会失败，修复必须同步健康探测。nginx auth 可从上层继承，见下方官方来源。 |
| D9：gainer10-pair 无测试 | “完全未覆盖”不成立；共有断言漏一项成立 | [测试](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/tests/smoke/test_server_deployment_manifest.py:157)已经访问 paper-orderflow-gainer10-pair，检查 checkpoint phase=15、imbalance=0.40；还断言 account-16/17 在保护与 dashboard 列表。所以删服务或改这些参数会失败。不过前部 required-services 集合和三个 paper 的共同 healthcheck 循环确实遗漏它，gainer10 特有的 healthcheck 漂移可能漏检。建议仅修正为“共同配置断言覆盖不全”，将 paper 服务集合统一复用。 |
| D10：无备份/恢复编排 | 仓库缺项成立，线上结论未知 | 搜索 deploy/docker/compose/example/runbooks，以及 scripts 中 backup、pg_dump、pg_restore、WAL archive/工具等，未找到数据库备份与恢复演练编排。postgres-data 命名卷及 retention 不等于备份。不能据此断言没有宿主机/云平台备份；数据库平面配置也可指向外部 URL。应确认部署的恢复目标、外部备份及恢复演练证据，再决定新增作业。若确无备份，这应是可靠性缺口，不宜仅归为“结构债”。 |

### 配置解析复现

本机 Docker Compose v2.31.0-desktop.2；全部使用临时虚构变量，无 daemon 操作：

| 场景 | 结果 |
|---|---|
| base manifest，满足其必填配置，缺所有附加账户凭证 | config --quiet 返回 0 |
| base + live overlay，不启用 profile | 返回 15，附加账户凭证插值失败 |
| base + live overlay，启用 live profile | 返回 15，同类插值失败 |
| 最小独立 YAML：未启用 live profile，其服务 env 使用必填变量 | 返回 15，证明只关 profile 不足以避开 :? |

Compose 的 :? 要求变量已设置且非空，且按文件在合并前插值。[Docker 插值文档](https://docs.docker.com/reference/compose-file/interpolation/)。未启用 profile 的具体失败行为另有上述本地实测支持。

## 四条“部署侧小问题”

| 条目 | 验收 |
|---|---|
| volume-init-check：0=需要初始化、1=正常 | 返回码事实成立，但这是 predicate 命令的常见“条件为真返回 0”，调用方正确分支并将其他退出码视为错误，不是 bug。显式命名 needs_volume_init 已足以减少歧义，未必需要改协议。更实际的局限是检查只 find -maxdepth 1，而初始化 chown -R；深层单个属主错误可能不触发初始化。 |
| ops-monitor User=root，可 harden | root 属实，但已有 NoNewPrivileges=true、PrivateTmp=true，不能称完全未加固。ProtectSystem 可进一步限制宿主文件写入；需为 /var/lib/crypto-momentum-lab 状态文件明确可写路径。monitor 需要 Docker socket，简单换非 root 但保留 Docker 管理能力也不能视为消除高权限。属于可选运维加固。 |
| account-3/4 notional ratio=0，其他=1.50 | 事实成立，且测试显式固定。不止这一参数不同：窗口、return、intensity 也有账户分化，是有意策略规格。应在账户说明中呈现差异，不应以“配置漂移”擅自统一。 |
| start_period=15m 导致首次固定等很久 | 因果不成立。start_period 是失败宽限期，不是强制等待；期间成功即 healthy，并可放行依赖服务。start_interval=15s 还会更早探测。实际初始化慢可以导致等待，但不能从 15m 推导必等 15 分钟。[Docker HEALTHCHECK 文档](https://docs.docker.com/reference/dockerfile/#healthcheck)。 |

## 七条“做得好的地方”

| 原评价 | 验收与边界 |
|---|---|
| flock + state 分阶段恢复 | 成立。[锁](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/deploy/ops/update_server.sh:225)、原子 state 替换及 phase rank 存在，并保留 base diff；但不能等同任意阶段都已证明安全。阶段后的外部状态变化不自动失效已有记录。 |
| live 更新前 renew lease + preflight，失败不重启 | 正常首次更新路径成立：执行顺序为 refresh（可选）→ renew → preflight → restart；失败返回。边界有三点：只检查 active 且未收敛账户；migrate/dashboard 在 preflight 之前已经可能改变；[恢复到后续阶段](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/deploy/ops/update_server.sh:686)会直接令 live_preflight_complete=1 而跳过新校验，所以不是每次重启前都重新 renew/preflight。长间隔恢复时审批/租约可能已经变化，需单独评估。 |
| research 先停再换 market，保护 cursor | 正常阶段顺序成立：[research-stop](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/deploy/ops/update_server.sh:1000)在 market-data 前。它降低 epoch 切换竞态，但不单独证明任意失败/恢复都无数据缺口。 |
| healthcheck 全走本地标记，不打 DB | 只对 market-data、paper、execution、live 等标记探针成立。[docker/local-healthcheck](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/docker/local-healthcheck:1)确实只读文件时间。dashboard 仍调用 /api/health（会执行查询服务 health），postgres 是 pg_isready，research collector 是单独探针；不能概括为所有服务都无 DB 相关探测。 |
| Dockerfile 依赖层/源码层分离，commit 后置 | 成立。[Dockerfile](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/Dockerfile:12)先复制 pyproject 安装依赖，再 COPY src 并安装项目，ARG 在后。只是构建缓存优化，不代表依赖完全锁定或构建可复现。 |
| volume-init 先检查 ownership 再 chown | 对 update_server 增量更新路径成立，且检查通过会跳过；compose 独立运行 volume-init 服务仍是直接递归 chown。检查深度限制见上表。 |
| rollback reset --keep 且校验祖先 | 评价不成立，发现并复现实际故障。见下一节。 |

nginx 的 auth_basic 可以在 http/server 层生效并继承到 location，snippet 未写并不能证明上层没有认证。[nginx 官方文档](https://nginx.org/en/docs/http/ngx_http_auth_basic_module.html)。

## 新确认：祖先提交回滚分支不可达

[update_server.sh:282](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/deploy/ops/update_server.sh:282)的逻辑先执行 git merge --ff-only target，只有命令失败时才检查 target 是否祖先并 reset --keep。

但 target 是当前 HEAD 的祖先时，Git 返回成功并输出 Already up to date.，不会进入 reset 分支。随后 HEAD != target 的检查令脚本退出。

本地临时仓库两次空提交的复现：

- target=第一提交，HEAD=第二提交。
- merge --ff-only target 返回 0。
- HEAD 保持第二提交，未达到 target。
- 因而脚本不会执行其宣称的回滚操作。

应在 merge 前区分相等、快进、祖先回退、分叉四种情况；不是简单增加另一条 reset。即使修复 Git 回退，应用镜像/数据库迁移的回滚兼容性也要另行定义。此项优先级高于原文若干配置整齐度建议。

## 验证与处理顺序

- 既有测试：test_deployment_script.py、test_server_deployment_manifest.py、test_cml_ops_monitor.py，共 20 passed。
- 额外隔离复现：D1/Profile 插值、D2 两条目录分类、D4 空凭证启动校验、D5 复数覆盖单数、D7 四账户七维 env/argv 一致性、祖先 Git 回退。
- 测试通过不覆盖上述新增缺陷；部署脚本部分测试只检查字符串存在，因而可同时“有 reset --keep”且实际回滚失败。
- 优先：回滚分支、D1 配置选择、D2 分类、D3 多账户监测、D5 持仓保护范围。
- 条件性高优先：D8 若实际无外层鉴权、D10 若实际无可恢复备份。
- 不照做：D4 直接在 base 加 :?、D7 删除 env、D9 宣称零覆盖、将 start_period 当启动等待时长。

## 当前 HEAD 二次验收

在 `c62cd52` 及后续修复后重新核对：

| 项目 | 当前状态 | 证据与剩余边界 |
|---|---|---|
| 回滚分支 | 已修复 | `update_server.sh` 先区分相等、可快进、祖先回退和分叉；祖先目标实际进入 `git reset --keep`，隔离 Git 回归覆盖。 |
| D1 | 已修复 | paper-only 不加载 live overlay；`--live` 只在检测到 account-2/3/4 已有运行中的 Compose 服务时加载附加 overlay，因此 primary-only live 更新不再要求附加账户凭证。停止中的附加账户仍不会被隐式启动，若要恢复它们需显式选择对应服务。 |
| D2 | 已修复 | 分类匹配 `strategies/*` 和 `strategy_runner/*`，回归测试固定路径。 |
| D3 | 已修复 | systemd 只提供 base/overlay Compose 文件；monitor 直接从这些文件发现 live service、账户 label、session 和 lease owner，并用 Docker service label 查询容器，新增账户不再需要手工改 monitor 清单。`CML_MONITOR_SERVICES` 与 `CML_MONITOR_LIVE_ACCOUNTS` 仍可通过 ops-monitor.env 显式覆盖。 |
| D5 | 已修复 | market-data 每次刷新从 PostgreSQL 读取各账户最近一次 `ready` 对账；`position_count > 0` 的账户自动加入保护集合，因此停用但仍有仓位的账户无需手工列入 labels。singular/plural 配置仍作为启动提示并与自动发现结果合并；最近一次 ready 对账为零的账户会退出保护集合。 |
| D9 | 已修复 | server manifest 的 required services、healthcheck 循环和 gainer10 特有断言均包含 `paper-orderflow-gainer10-pair`。 |
| D4 | 已验收，不改 Compose | primary 的 Compose 环境变量仍可空以保持 paper-only 解析；应用在创建 Binance client 前对 READ/TRADE 凭证缺失或空白 fail-closed，已有 credential resolver 与 CLI 启动测试覆盖。 |
| D6 | 已完成 | 所有 live 账户默认只持久化 `submit,cancel` 写边界；CLI、Compose 和显式空值均采用该默认。只有明确传入 `all` 才启用全量 exchange telemetry，作为临时诊断开关；order/fill 账本不受该 allow-list 影响。 |
| D7 | 第一阶段已修复 | profile/top-N 已改为环境单源，preflight 与 daemon 使用同一解析路径；其余 argv 差异是账户运行参数，不属于该项重复。 |
| D8/D10 | 待外部证据 | 本地只能确认 dashboard Basic Auth 可选、仓库没有备份/恢复编排；仍需真实 nginx 上层鉴权和数据库恢复演练证据，不能用本地 Compose 结论替代。 |

当前部署回归：`tests/smoke/test_deployment_script.py`、`tests/smoke/test_server_deployment_manifest.py`、`tests/unit/ops/test_cml_ops_monitor.py` 共 29 passed。
