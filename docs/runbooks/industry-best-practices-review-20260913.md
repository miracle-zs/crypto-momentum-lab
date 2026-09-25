# 行业最佳实践 vs 当前系统：差距分析

- **目的**：说清楚三件事——行业里成熟实盘/在线系统通常怎么做；CML 现在做到哪一步；还剩哪些可改进空间。
- **对照对象**：本仓库实盘栈（Docker Compose + PostgreSQL + `cml-ops-monitor` + 本地 health marker）。
- **权威来源**：Kubernetes 探针语义、Google SRE Book、Prometheus 告警实践、PostgreSQL 18 文档、Docker 资源约束文档。
- **事实依据**：`docs/runbooks/server-ops-review-20260913.md` 中的 2026-09-12～13 现网证据。
- **不是**：改造排期表、升配决策书、交易策略评审。

---

## 0. 一句话结论

CML 在**交易语义层**（fail-closed、checkpoint、幂等单号、风险窗 flatten）已经达到不少行业常识；主要差距在**平台可靠性层**：业务新鲜度与基础设施心跳未分层、critical 告警无闭环、资源饱和余量不足、PG 缓冲仍是默认值。这些是「工程运维成熟度」问题，不是策略逻辑问题。

---

## 1. 健康检查与自愈

### 1.1 行业最佳实践长什么样

| 实践 | 含义 | 出处 |
|------|------|------|
| **Liveness / Readiness / Startup 分离** | 活着 ≠ 能接流量 ≠ 启动完成；失败后果不同（重启 / 摘流 / 等待） | Kubernetes probes |
| **Liveness 只打真正僵死** | 临时过载、依赖抖动不应立刻杀进程 | K8s 官方警告：不要用 liveness 代替 readiness |
| **业务新鲜度独立于基础设施心跳** | 「进程还在跑」≠「交易状态在正确落盘」 | 分布式系统通行做法；K8s 探针分层精神 |
| **自愈与探测匹配** | 探测失败后的动作（重启/摘流/告警）要有足够超时覆盖优雅退出 | 容器运维常识；Docker `stop_grace` |
| **Fail-closed 交易路径** | 状态不确定时停开仓，退出能力优先 | 实盘系统通行设计（本仓库已有） |

### 1.2 CML 现在长什么样

```text
                    ┌─ status marker（主循环心跳）
cml-local-healthcheck ─┤
  窗口 300s            └─ database marker
                              ▲
              ┌───────────────┼────────────────┐
              │               │                │
     策略 checkpoint    lease renewal      其他 DB 成功
     成功 heartbeat     on_lease_renewed
                        → database_ok()

Docker health ← healthcheck
     │
     ▼ 仅 unhealthy/dead
ops-monitor auto-restart（默认开）
     │
     └─ compose restart 15s 超时（常失败）
```

**已做到的**：
- 有本地 health marker，Compose healthcheck 不打 DB（轻量、正确方向）。
- checkpoint 成功会刷新 marker。
- lease 独立、fail-closed entry gate、scheduled flatten。
- ops 具备 unhealthy 自动重启骨架（cooldown / max attempts）。

**和行业差距**：
| 差距 | 现网证据 | 行业对照 |
|------|----------|----------|
| 业务新鲜度被基础设施心跳覆盖 | 09-13 策略 checkpoint 停 45min，health 仍 healthy，无 heartbeat_stale | 应有独立「策略状态新鲜度」liveness |
| 自动重启命令超时过短 | 15s < stop_grace 90s，多次 TimeoutExpired | 自动化必须匹配优雅退出 |
| Critical 只告警不动作 | `live_checkpoint_stale` 无对应 restart | 可自动化处置的不应长期只 page |
| 无独立 startup 语义 | start_period 有，但与 checkpoint 新鲜度混在 healthcheck 里 | 启动慢不应与运行期假活同一套判定 |

### 1.3 改进空间

1. **拆信号**：`database` marker 继续表示「DB 可达」；新增 `strategy_checkpoint` marker（或 ops 直接消费 `live_checkpoint_stale`）。
2. **拆动作**：基础设施抖动 → 重连/等待；策略状态停写超时 → 重启策略容器。
3. **修自动化本身**：restart 命令超时 ≥ 120s；失败要有独立 critical。
4. **启动期**：start 用更长窗口 + 不触发 live 重启；ready 后再套 300s 运行期窗口。

---

## 2. 告警与可观测性

### 2.1 行业最佳实践长什么样

| 实践 | 含义 | 出处 |
|------|------|------|
| **症状优先于 cause** | Page 打「用户/业务已受伤」；cause 留给 dashboard | Google SRE Book Ch.6 |
| **四大黄金信号** | Latency / Traffic / Errors / **Saturation** | SRE |
| **Page 要可行动** | 不能行动或动作固定 → 自动化或降为 ticket | SRE；Prometheus |
| **低噪声** | 高频 warning 刷屏会造成 alert fatigue，掩盖真 page | SRE Bigtable 案例 |
| **容量与饱和** | 接近满载需提前介入；tail latency 是饱和领先指标 | SRE；Prometheus capacity |
| **监控自身** | monitor 挂掉要有信号（metamonitoring） | Prometheus |
| **黑白盒结合** | 白盒看内部，黑盒看端到端是否真坏 | SRE |

### 2.2 CML 现在长什么样

| 维度 | 现状 |
|------|------|
| 通道 | journal + Server酱；可选 webhook / external heartbeat |
| 事件 | 结构化 JSON：ops_alert / resolved、checkpoint、memory、lag、业务 warning |
| 分级 | severity critical/warning + cooldown 900s |
| 金信号覆盖 | Lag 有；流量有观测；error 事件有；**饱和有告警但偏晚** |
| 噪声 | postgres memory growth 全天反复；veth 系统噪声不进 ops |
| 闭环 | heartbeat 路径尝试自动重启（常失败）；checkpoint 路径无 |
| 自监控 | external heartbeat 代码有；是否在用取决于部署配置 |

### 2.3 改进空间

1. **Page 级收敛**（需立即动作）：策略 checkpoint stale 持续、memory critical、heartbeat stale、restart 失败/耗尽。
2. **降噪**：memory growth 改为「持续 X 窗口且 fraction&gt;阈值」才升级；单次 blip 只留 dashboard。
3. **补饱和领先指标**：live RSS p95、PG checkpoint write p95、host available / swap used 曲线。
4. **补 metamonitoring**：确认 external heartbeat 在用；ops-monitor 自身 dead 有人知道。
5. **区分 what/why**：page 文案指向 runbook 与「该执行什么」，不只是事件名。

---

## 3. 资源与容器内存

### 3.1 行业最佳实践长什么样

| 实践 | 含义 | 出处 |
|------|------|------|
| **生产必须有 memory limit** | 防单容器吃光宿主机触发 OOME | Docker docs |
| **swap 是缓冲不是稳态** | 频繁 swap 严重伤延迟；交易路径尤其敏感 | Docker docs；延迟敏感系统常识 |
| **禁止 oom-kill-disable** | 禁 OOM kill 可能把整机拖死 | Docker 明确警告 |
| **limit 按峰值×余量** | 不是历史均值；关键服务常见 1.3–1.5× | 容量规划通行做法 |
| **宿主机 overcommit 有界** | 为 OS cache / 内核 / 突发留内存 | 运维常识 |
| **接近饱和即处理** | 100% 前就已劣化 | SRE Saturation |

### 3.2 CML 现在长什么样

| 组件 | limit | 事故/实测 | 问题 |
|------|-------|-----------|------|
| live-strategy ×4 | 512m + swap 768m | 峰值 99% | limit 贴峰值；注释仍写 &lt;200MiB |
| market-data | 640m | 95%，max events=8 | 无足够 headroom |
| postgres | 1g + swap | peak≈1g，回收 6.8 万 | 长期贴顶 |
| host | 3.6G | swap used ~1G | 磁盘当内存，尾延迟风险 |
| 全栈 mem_limit 和 | ≫ 物理内存 | 依赖错峰 | 脆弱 overcommit |

**已做到的**：有 limit、有 memswap 策略、有 trend 感知内存告警、未用 oom-kill-disable。

### 3.3 改进空间

1. 按 **7 天 p99 峰值** 重标 live/market-data limit（或升配后重算）。
2. 修正过时注释，避免后人按 200MiB 再砍。
3. 明确政策：交易路径 **稳态不进 swap**；swap 只覆盖秒级尖峰。
4. Host 层：available memory 与 swap used 进 page 阈值（例如 swap used &gt; 50% 持续告警）。
5. 评估非关键 paper 与实盘的资源优先级（cgroup 权重 / 停用策略）。

---

## 4. PostgreSQL 写路径

### 4.1 行业最佳实践长什么样（官方文档）

| 实践 | 含义 |
|------|------|
| **shared_buffers** | 专用机约 **RAM 25%**；通常不超 40%；默认 128MB 只是起点 |
| **更大 buffer ↔ 更大 max_wal_size** | 脏页多时要能摊开刷盘 |
| **checkpoint_completion_target = 0.9** | 摊平 I/O；**不要**为「单次更快」压低 |
| **wal_buffers** | WAL 输出高时加大，平滑 checkpoint 后延迟 |
| **work_mem** | 按操作×并发放大；勿无脑加大 |
| **effective_cache_size** | 给 planner 的 OS cache 估算，应贴近真实 |
| **禁止关 fsync / full_page_writes** | 数据安全红线 |
| **度量再调** | bgwriter / checkpointer / io_timing 前后对比 |

### 4.2 CML 现在长什么样

| 参数 | 当前 | 评价 |
|------|------|------|
| shared_buffers | **128MB 默认** | 对 3.6G / 1g cgroup 偏小 |
| wal_buffers / lz4 / bgwriter 400 | trial 已在 | 方向正确 |
| timeout / max_wal / completion_target | 15min / 2GB / 0.9 | 与官方一致 |
| work_mem / maintenance_work_mem | 4MB / 32MB | 小机可接受 |
| effective_cache_size | 4GB | 高于真实，误导 planner |
| 实测 | write 200–400s；buffers_backend 高 | 缓冲+云盘瓶颈仍在 |
| 观测 | track_io/wal、pg_stat_statements | 已开，利于迭代 |

**已做到的**：checkpoint 摊开参数正确、WAL trial、维护并行关掉、IO timing 可观测。

### 4.3 改进空间

1. `shared_buffers` 提到 **256MB**（容器 1g 下的合理步进），维护窗重启验证。
2. `effective_cache_size` → **1GB**。
3. `work_mem` 仅在有落盘证据时 4→8MB，一次一参。
4. 若 write 仍 &gt;200s：问题在盘/规格，不在 completion_target。
5. 长期：大表分区/保留裁剪（已有 runbook），减脏页产生量。

---

## 5. 实盘交易系统特有实践

### 5.1 行业通常还要求什么

| 实践 | CML 现状 | 差距 |
|------|----------|------|
| Fail-closed 不确定态 | entry gate / risk window 有 | 小 |
| 可恢复持久状态 | checkpoint 有 | 新鲜度监控不足 |
| 幂等下单 / client id | 有；legacy 修复路径 | 运维噪声需接受或清历史数据 |
| 风险窗 flatten / kill switch | scheduled flatten 有 | 小 |
| 站外 dead-man | external heartbeat 钩子有 | 需确认在用、有人响应 |
| 变更与交易隔离 | 部分（retention 有界） | 中等：监控重启、备份窗口 |
| 多可用区/主备 | 单机单 PG | **结构性**；小资金阶段可接受，需明确风险 |
| 混沌/故障注入演练 | 有测试文化线索 | 平台层假活场景覆盖不足 |

### 5.2 改进空间（交易特有）

1. **假活演练**：人为停写 checkpoint，验证多久被发现、能否自动恢复（当前设计下会失败）。
2. **变更窗口制度**：deploy / PG 参数 / limit 调整与开仓高峰错开。
3. **明确单机风险**：文档写清「主机或 PG 故障 = 全账户中断」及人工恢复步骤。
4. **订单审计链完整**：已有 fill/order 事件；补「重启前后仓位对账」例行检查。

---

## 6. 成熟度总表

| 领域 | 行业目标态 | CML 水平 | 差距等级 |
|------|------------|----------|----------|
| 交易语义（fail-closed、幂等、flatten） | 强 | 较强 | 低 |
| 持久状态 | 有 checkpoint + 恢复 | 有 | 低 |
| 健康分层 | liveness ≠ readiness ≠ 业务新鲜度 | 未分层 | **高** |
| 自愈闭环 | critical 可自动处置且可靠 | 半残（超时/漏检） | **高** |
| 告警质量 | 低噪声、可行动、有 page 分级 | 有框架，噪声与闭环不足 | 中 |
| 饱和与容量 | 提前余量、领先指标 | 贴顶才报 | **高** |
| 容器资源治理 | limit 合理、swap 政策清晰 | limit 贴峰值、host swap 重 | **高** |
| PG 写路径 | buffer 与盘匹配、可度量迭代 | 参数半对、buffer 偏小 | 中 |
| 架构冗余 | 多副本/多机（按资金规模） | 单机 | 中（阶段可接受） |
| 文档与复盘 | 事故有 postmortem | 本次已产出 review | 低（已开始） |

---

## 7. 改进空间清单（按领域，非排期）

### A. 探测与自愈（优先补课）

- [ ] 策略 checkpoint 新鲜度与 lease/DB 心跳解耦
- [ ] `live_checkpoint_stale` → 可靠自动重启（先修 15s 超时）
- [ ] 启动期 / 运行期探测窗口分离
- [ ] 假活故障注入演练

### B. 告警质量

- [ ] page / ticket 分级表
- [ ] memory growth 降噪
- [ ] 饱和领先指标（RSS p95、ckpt write p95、swap used）
- [ ] 确认 external dead-man 在用

### C. 资源

- [ ] 按 p99 重标 live/market-data limit 或升配
- [ ] 修正 compose 过时注释
- [ ] 明确 swap 政策（交易稳态不进 swap）
- [ ] host swap/available 进告警

### D. PostgreSQL

- [ ] shared_buffers 256MB 试验
- [ ] effective_cache_size 1GB
- [ ] 继续用 io_timing 做前后对比
- [ ] 大表保留/分区（已有方案，执行节奏）

### E. 架构与流程（阶段决策）

- [ ] 单机风险书面化
- [ ] 变更窗口制度
- [ ] 资金/规模到阈值后评估主备或独立行情机

---

## 8. 参考

| 来源 | 链接 |
|------|------|
| K8s liveness/readiness/startup | https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/ |
| Google SRE Ch.6 Monitoring | https://sre.google/sre-book/monitoring-distributed-systems/ |
| Prometheus alerting | https://prometheus.io/docs/practices/alerting/ |
| PG WAL / checkpoint | https://www.postgresql.org/docs/current/wal-configuration.html |
| PG shared_buffers 等 | https://www.postgresql.org/docs/current/runtime-config-resource.html |
| Docker memory/swap | https://docs.docker.com/config/containers/resource_constraints/ |
| 本库事故复盘 | `docs/runbooks/server-ops-review-20260913.md` |

---

*本文回答「行业什么样 / 我们什么样 / 还差什么」；不替代具体改造计划与维护窗方案。*
