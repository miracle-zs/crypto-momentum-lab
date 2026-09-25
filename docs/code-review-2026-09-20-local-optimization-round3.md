# local_optimization 修复后复查报告（2026-09-20）

## 结论

本轮修复关闭了上一轮的多项核心反例，但当前实现仍不能作为自动换参依据。测试和静态检查均通过，然而复利寻优、真实权益风险、实盘对账证据和样本外评价仍存在会改变结论的阻塞问题。

当前版本可以继续用于本地实验和问题定位；在下面的 P1 问题关闭前，不应根据日报结果自动替换服务器参数。

## 验证范围与结果

本次检查覆盖 `local_optimization` 的每日主链、六场景寻优、walk-forward、MTM、仓位策略、快照和实盘对账入口。没有修改实现代码。

| 检查 | 结果 | 说明 |
|---|---:|---|
| `pytest local_optimization/tests -q` | **96 passed** | 现有测试全部通过 |
| `ruff check local_optimization/ --output-format concise` | **通过** | 未发现 Ruff 错误 |
| 动态复利保证金反例 | **通过** | 复利峰值超过 280U 时，`m280_compounding` 返回无可行解 |
| 复利 MDD 15% 反例 | **通过** | 所有候选超过 15% 时返回 `None`，不再静默放宽 |
| 空事件/空候选 | **通过** | 六个场景均返回 `None`，不再抛出空集合异常 |
| 部分实盘数据 | **通过** | 缺少事件流时标记 `INSUFFICIENT_EVIDENCE` |
| 垃圾事件文件冒充完整对账 | **失败** | 文件存在且超过 50 字节时仍可能显示 PASS |
| 合法表头 + 非法值快照 | **失败** | 仍可能得到 `is_complete=True` |
| carry-in 期初浮盈 | **失败** | 期初权益包含浮盈时可能重复计入 |

## 已确认关闭的问题

- `m280_compounding` 已按动态复利后的峰值保证金筛选，不再在动态约束无解时退回固定仓位候选。
- 复利候选的 15% MDD 门槛已变成硬约束；无可行解会返回 `None`。
- 空事件和空候选会返回结构化的空场景结果。
- 日切复利不再把尚未平仓的未来收益提前用于下一天定仓；权益非正时新订单额度为零。
- MTM 引擎在订单事件和日切前会使用事件时刻的权益；非整桶的最后一笔交易不会被网格截断。
- MTM 曲线已使用共同起止时间和共同时间轴，收益率单位重复放大问题已修复。
- 快照目录为空或缺少必要文件时不会再被标记为完整；tracker 已接入快照完整性和对账状态。
- `RiskAdaptiveSizing(use_smoothing=False)` 和历史 CSV 复利实验分支的已知运行错误已修复。
- MTM 初始化日切重复触发的问题已有测试覆盖，当前单次重放只触发一次初始化日切。

## 仍未关闭的问题（P1）

### 1. 复利候选仍按已实现盈亏评分，没有用完整 MTM 权益

位置：[`generate_six_scenarios_dashboard.py:254`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/generate_six_scenarios_dashboard.py:254)、[`generate_six_scenarios_dashboard.py:425`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/generate_six_scenarios_dashboard.py:425)。

`compute_daily_compounding_scales` 只在平仓日把 `net_pnl_usdt` 加入权益，未纳入浮盈浮亏、跨日未平仓、无交易日和日内路径。真正的 15 秒 MTM 曲线是在候选选出后才重构，因此复利增长、UI 和 MDD 不能参与候选的真实排名与硬约束。

此外，`Candidate8D.mdd` 和 Calmar 仍由按退出事件累计的已实现收益计算；即使复利场景的动态保证金筛选已修复，普通收益和风险场景仍可能漏掉盘中回撤。

应先用统一执行账本为每个候选生成完整 MTM 权益，再计算 MDD、UI、CDaR、日内回撤和保证金峰值，最后进行可行性筛选与排名。

### 2. 参数寻优机会池不是完整参数空间

位置：[`run_walk_forward_analysis.py:179`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/run_walk_forward_analysis.py:179)。

当前只加载 `account_primary_events.csv` 和 `account_acc02_events.csv`。这些是既有配置已经触发并成交的事件，原策略没有触发的信号不会进入候选池。因此当前流程本质上是“在既有成交集合上筛参数”，无法验证完整七维/八维参数空间。

应从覆盖候选参数的原始行情、特征和状态重放信号、订单、成交和风险仲裁；在此之前，报告中应明确当前结果属于代理事件池分析。

### 3. Walk-forward 和日报 OOS 还不是严格的冻结前向评价

位置：[`run_walk_forward_analysis.py:296`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/run_walk_forward_analysis.py:296)、[`run_daily_local_optimization.py:529`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/run_daily_local_optimization.py:529)。

当前窗口筛选会丢弃期初已有仓位和期末仍未平仓的交易，跨边界仓位没有按 MTM 权益结算。日报的旧参数 OOS 主要通过两天累计收益相减得到，尚未固定旧参数、初始状态、数据修订版本和真正新增区间。

应保留 carry-in、期末未平仓和账户状态，在共同边界计算权益变化；日报必须保存每次冻结评价的窗口身份，不能只用累计收益差。

### 4. 实盘对账仍可能把不完整证据认证为 PASS

位置：[`generate_six_scenarios_dashboard.py:662`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/generate_six_scenarios_dashboard.py:662)、[`generate_six_scenarios_dashboard.py:922`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/generate_six_scenarios_dashboard.py:922)、[`run_daily_local_optimization.py:608`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/run_daily_local_optimization.py:608)。

事件流检查目前主要判断文件存在且大于 50 字节；L1/L2 匹配率、滑点和部分归因仍使用固定文案。临时目录中放入 12 条余额、1 笔回放交易和四个垃圾事件文件后，结果仍为 PASS。日报门禁只排除包含 `INSUFFICIENT` 的状态，`FAIL` 或 `DIVERGED` 仍可能被算作 `reconciliation_passed=True`。

应逐流解析 schema、时间、账户和事件数量，实际计算匹配率；任何关键流缺失、解析失败或审计失败都必须阻止 stable。

### 5. 快照完整性仍没有验证内容和时间水位

位置：[`snapshot.py:185`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/snapshot.py:185)。

当前只检查认可的列名和非空行，不验证数值、时间格式、时间是否覆盖目标 cutoff、账户覆盖、分区完整性或多文件合并。合法表头配合 `garbage` 数据仍可能被标记为 `is_complete=True`，并得到 `decision_replay_ready` 和 `execution_audit_ready`。

应解析并校验每行关键字段、UTC 时间范围、目标 cutoff、账户/配置身份和所有分区的水位；只要有关键流无法证明完整，就不能授予 ready 标签。

### 6. MTM carry-in 期初权益存在重复计入风险

位置：[`mtm_engine.py:296`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/mtm_engine.py:296)、[`mtm_engine.py:320`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/mtm_engine.py:320)。

carry-in 仓位会从原始入场价重新计算完整浮盈，并直接加到传入的 `initial_equity`。而实盘对账传入的是包含未实现盈亏的 `total_equity`，因此同一浮盈可能被加两次。一个期初权益 1010U、carry-in 浮盈 10U 的反例，首个权益点为 1019.95U。

应明确 `initial_equity` 是现金余额还是期初总权益；若是总权益，carry-in 只应加入从窗口起点开始的价格变化。

## 仍未关闭的问题（P2）

- [`run_two_stage_grid_optimization.py:219`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/run_two_stage_grid_optimization.py:219) 仍用 `MDD × 0.45` 近似 UI 参与正式筛选，需改为精确权益曲线指标。
- [`reconciliation.py:109`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/reconciliation.py:109) 在缺少账户或配置身份时使用默认值；两边都缺身份时仍可能错误匹配。缺失确定性身份时应降级为不可认证或 ambiguous。
- [`build_price_cache.py:59`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/build_price_cache.py:59) 只按币种覆盖判断缓存是否可复用，没有按快照截止时间、数据版本或内容哈希失效；旧行情可能被用于新交易日。
- [`generate_six_scenarios_dashboard.py:1308`](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/generate_six_scenarios_dashboard.py:1308) 在没有传入 governance 数据时仍展示固定的 85.7% 稳定性和 +18.52U OOS 文案，存在展示误导。

## 建议的关闭顺序

1. 先修复对账和快照门禁，确保缺失、失败和伪造数据不会进入 PASS 或 stable。
2. 将候选事件生成、仓位、订单、费用、资金和 MTM 权益统一到同一个逐参数执行账本。
3. 以完整 MTM 曲线先做硬约束，再计算 UI/CDaR/日内回撤并排名。
4. 从原始行情和特征重放完整候选机会池，取消只依赖既有账户成交集合。
5. 重建冻结旧参数的增量 OOS 窗口和 carry-in/期末未平仓边界。
6. 最后再处理缓存失效、近似 UI 和日报展示细节。

## 复查结论

当前版本的测试质量和若干基础防护已经明显改善，但上述 P1 问题仍足以改变最优参数、风险指标和实盘对账结论。因此本轮复查结果为：**部分修复，未达到可自动换参的验收标准**。

