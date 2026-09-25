# Local Optimization 模块复核报告

**复核日期：** 2026-09-22  
**复核对象：** `/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/`  
**复核目的：** 检查前几轮审查提出的机会池、Walk-forward、15 秒 MTM、复利约束、实盘对账和数据治理问题是否已经全部修复。

## 结论

当前不能认定“所有问题都已修复”。

机会池的正式入口、WFA 的状态传递和 8 维候选支持已经基本形成闭环；在这些前提下，代码可以继续做本地研究。可是，整个模块还不能作为“自动验证实盘参数并自动换参”的正式入口，主要阻断点是：实盘与回测的对账仍可能因仅按币种匹配而误判通过，参数协议仍同时存在 7 维旧 schema 和 8 维新 schema，六场景验证窗口与价格缓存覆盖也没有完全绑定到同一份 manifest。

因此本次验收结论为：

| 范围 | 结论 | 说明 |
|---|---|---|
| 参数无关机会池与 manifest | 基本通过 | 正式入口已拒绝静默回退到账户 CSV，并校验内容哈希、时间顺序和因果字段。 |
| Stateful WFA / Carry-In / Carry-Out | 基本通过 | 已支持 8D 自动识别、并发槽位、窗口状态传递和严格 MTM 二次验证。 |
| 六场景全流程 | 部分通过 | 复利候选会做真实 MTM，但前面仍有快速预筛，且窗口尚未强制使用 manifest 水印。 |
| 实盘逐笔因果对账 | 不通过 | 当前 dashboard 主要按 symbol 集合判断，不能证明是同一参数、同一信号和同一成交。 |
| 参数协议与长期收敛比较 | 不通过 | 新旧参数名称、单位、维度和 hash 入口不统一。 |
| 自动换参准入 | 不通过 | 在上述问题关闭前，不能仅依据日报中的 `PASS` 或 `PRODUCTION_ELIGIBLE` 执行换参。 |

## 已确认修复的部分

### 1. 正式机会池已经具备 fail-closed 入口

`run_walk_forward_analysis.py` 的正式路径默认要求 `manifest.json`，并默认禁止从账户事件 CSV 静默构造机会池。六场景 pipeline 还会检查 `pool_type=raw_parameter_independent`。机会池校验已经覆盖：

- `opportunity_id` 唯一性；
- `detected_at`、`entry_eligible_at` 的时间因果关系；
- 正价格和有限数值特征；
- symbol/row 数量和 watermark；
- 内容哈希和机会池类型。

这部分解决了“不同参数先过滤出不同事件集合”的核心污染问题。正式 WFA 缺少机会池或 manifest 时会拒绝运行；只有显式 debug 参数才允许账户 CSV fallback。

### 2. WFA 的 8D、并发槽位和连续状态已经接入

当前 WFA 会根据候选 CSV 是否有 `max_open_positions` 自动识别 8D；旧的 7D CSV 在 CLI 入口会展开为并发槽位 1～4。Stage 2 使用 `SimulationLedger` 做真实 15 秒 MTM，保存 `state_in/state_out`，支持 Carry-In/Carry-Out，并把已验证候选重新交给合规筛选。

正式 CLI 的 `--mtm-verify-depth` 默认值为 `0`，含义是“不按 top-N 截断已进入验证阶段的候选”；日报和六场景 selector 也采用相同默认值。价格缓存加载时已经校验 manifest 的内容哈希和 watermark，旧的无 metadata 缓存会在正式 WFA 中拒绝。

### 3. 复利候选已经开始用真实 MTM 约束

六场景的复利 selector 会对候选重建 15 秒权益曲线，并用真实 MTM 计算回撤、UI、峰值保证金和终值；真实峰值保证金超过上限、真实最大回撤超过阈值的候选会被丢弃。carry-in 的总权益基准也已修复，窗口起点能够保持传入的账户总权益。

### 4. 空数据和明显损坏数据的证据标签已有改善

快照检查现在会识别空文件、无效行和明显落后于 cutoff 的流，并在缺失时不给出完整能力标签。日报 tracker 的新记录也会保存 snapshot、reconciliation、OOS 和稳定性状态，治理流程比之前更容易 fail-closed。

## 仍未修复的问题

下面的问题会改变候选准入、日报状态或长期参数比较，属于正式换参前必须处理的事项。

### P1：实盘对账仍然可能误判通过

这是当前最严重的问题。

`generate_six_scenarios_dashboard.py` 在构造 dashboard 对账 payload 时，信号和成交的匹配率主要按 symbol 是否出现在实盘集合中计算。对应代码见：

- [generate_six_scenarios_dashboard.py:2019](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/generate_six_scenarios_dashboard.py:2019)
- [generate_six_scenarios_dashboard.py:2034](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/generate_six_scenarios_dashboard.py:2034)
- [generate_six_scenarios_dashboard.py:2042](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/generate_six_scenarios_dashboard.py:2042)
- [generate_six_scenarios_dashboard.py:2115](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/generate_six_scenarios_dashboard.py:2115)

当前 PASS 条件没有强制比较以下身份字段：

- 稳定的 `opportunity_id` / `signal_id` / `trade_id`；
- account id；
- 选中的候选参数 hash；
- 方向、开仓和成交时间；
- 成交数量；
- 成交价格及允许的滑点容差；
- 同一订单的部分成交和撤单关系。

`accounts_meta` 中的参数字符串仍是硬编码展示值，也没有绑定到当天选中的候选或服务器实际配置。因此“币种相同”可能被报告为“信号和成交匹配”，但并不能证明是同一参数产生的同一笔交易。

底层 [reconciliation.py:107](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/reconciliation.py:107) 的通用引擎比 dashboard 路径严格一些，会比较 symbol、方向、账户、配置和时间；但 fill key 仍不包含数量，缺少身份时还可能回退为默认 account/config。一个最小反例是：实盘成交数量为 999、回放数量为 1，symbol、side、时间和价格都相同，当前底层函数仍可能计为 matched。

日报入口只读取 status label：

- [run_daily_local_optimization.py:748](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/run_daily_local_optimization.py:748)

所以 dashboard 的误判会直接传递到日报的 `reconciliation_passed`，形成自动换参风险。

**必须改成：** 服务器信号、订单、fill、回放机会和回放交易共享稳定 ID；每一层都必须带 `account_id`、`parameter_hash`、方向、数量、时间和价格；字段缺失时返回 `INSUFFICIENT_DATA`，不能按 symbol 或默认身份猜测匹配。

### P1：参数协议仍存在 7D/8D 和名称、单位不一致

[protocol.py:73](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/protocol.py:73) 仍声明默认 orderflow protocol 是 7 维，并使用：

`impulse_window_bars`、`confirmation_window_bars`、`min_directional_return_bps`、`imbalance_threshold`、`min_notional_intensity`、`min_volume_ratio`、`symbol_cooldown_bars`。

六场景/WFA 实际使用的是另一套字段：

`impulse_window_buckets`、`confirmation_buckets`、`min_return_pct`、`min_imbalance`、`min_intensity`、`min_volume_ratio`、`cooldown_buckets`，再加 `max_open_positions`。

此外，`ParameterCandidate.from_dict` 目前直接对输入字典做 hash，没有把旧字段、新字段、单位、整数/浮点类型先规范化。这会导致语义相同但入口不同的候选得到不同 `parameter_id`，从而破坏“同一限制条件每天纵向比较直到收敛”。

旧的 [run_two_stage_grid_optimization.py:318](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/run_two_stage_grid_optimization.py:318) 仍按 7D 工作，并在 [run_two_stage_grid_optimization.py:236](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/run_two_stage_grid_optimization.py:236) 用 `MDD * 0.45` 近似 UI。若该脚本仍被视为正式入口，它会绕过真实 MTM；若只是历史诊断工具，应在 CLI、报告和文档中明确标记为 legacy/diagnostic，禁止其输出直接参与推荐。

**必须改成：** 建立唯一 canonical schema，统一字段名、单位、精度、默认值和维度；所有网格、WFA、六场景、日报、对账和 tracker 都只接受 canonical candidate，并用同一份 canonical JSON 生成 hash。

### P1：`verify_depth=0` 已取消 top-N 截断，但仍不是“全网格真实 MTM”

当前 0 的语义是：对已经进入 Stage 2/selector 的候选全部验证。这一点已经修复了默认 top-N 截断；但六场景在真实验证前仍执行 fast ledger 预筛：

- [generate_six_scenarios_dashboard.py:1252](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/generate_six_scenarios_dashboard.py:1252)
- [generate_six_scenarios_dashboard.py:1263](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/generate_six_scenarios_dashboard.py:1263)

候选必须先满足 fast `n_trades >= 15` 且 `oos_pnl > 0` 才能进入真实 MTM。WFA 也先根据 fast candidate compliance 决定 Stage 2 集合。因此现在不能声称“0 就是整个参数网格逐个做真实 MTM”。如果 fast 指标不是对真实 MTM 准入的保守下界，就可能提前排掉一个真实 MTM 合规候选。

此外，`select_pnl_max` 和 `select_balanced` 在真实验证结果全部为负收益时仍可能选择一个“最不差”的候选；复利 selector 主要检查真实 MDD 和 margin，也没有统一的真实正收益、最少交易数和“无可行解”门禁。

**必须改成：** 要么移除会改变候选集合的 fast 预筛，要么证明每个 fast gate 是真实 MTM 合规的保守必要条件；所有 selector 在真实验证后统一执行 `net_pnl > 0`、最小交易数、MDD、UI/CDaR、峰值保证金和 no-feasible 规则。

### P1/P2：六场景窗口还没有完全绑定机会池 manifest

六场景 `solve_six_scenarios` 会根据事件的最早 detected time 和最后 exit/detected time 自己推导 `w_start/w_end`：

- [generate_six_scenarios_dashboard.py:1195](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/generate_six_scenarios_dashboard.py:1195)

虽然 pipeline 已加载并校验 manifest，但没有把 `manifest.watermark_start` 和 `manifest.watermark_end` 强制传给 solver。这样可能遗漏最后一个事件之后仍影响 MTM 的价格、持仓或尾部回撤，也会让不同天的评价窗口不完全可比。

WFA 主入口已经使用 manifest watermark，这一项主要残留在六场景路径。

**必须改成：** 统一由 manifest 提供固定评估窗口，并明确预热区、评价区和尾部平仓区；事件推导时间只能用于诊断，不能覆盖 manifest 窗口。

### P1/P2：价格缓存缺少运行时全量 symbol 和 gap/staleness 门禁

[mtm_engine.py:532](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/mtm_engine.py:532) 已校验 cache metadata 的内容哈希和 watermark，但加载时没有强制检查：

- 机会池所有 symbol 都存在于价格缓存；
- 每个 symbol 覆盖整个 manifest 窗口；
- 15 秒序列没有超出允许阈值的 gap；
- 报价没有 stale 或倒序。

当某个 symbol 缺少价格序列时，MTM 取 entry price 作为 fallback，可能把真实浮动风险看成平坦净值。另一个问题是，dashboard 对账在没有传入价格时会在 [generate_six_scenarios_dashboard.py:1509](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/generate_six_scenarios_dashboard.py:1509) 直接加载不带 `expected_manifest` 的缓存；日报 Step 2 在正式优化之前调用这条路径，因此可能用 stale/legacy cache 生成对账 PASS。

**必须改成：** 对账和寻优共用同一份 manifest-bound cache；加载后检查 symbol 集合、起止 watermark、最大 gap 和时序单调性，任何失败都返回 `INSUFFICIENT_DATA`。

### P2：CDaR 和部分 UI 仍是近似值，却以正式指标名输出

日报 candidate conversion 在 [run_daily_local_optimization.py:118](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/run_daily_local_optimization.py:118) 以 `compounding_mdd * 0.8` 填充 `cdar_95`；fast ledger 也把 `mdd_usdt` 直接写成 `cdar_95`。真实 dashboard MTM 曲线能够计算真实 CDaR，但日报和候选对象的公共字段可能混入近似值。

**必须改成：** 未经完整 MTM 曲线计算的值命名为 `approx_*`，不得写入 `cdar_95`；正式准入只使用真实日内回撤序列计算的 UI、CDaR、intraday RMS 和 duration-weighted drawdown。

### P2：快照“完整”仍不代表覆盖完整

[snapshot.py:358](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/snapshot.py:358) 目前主要检查文件非空、行内容有效以及最新时间没有落后 cutoff 两天。它还没有强制检查：

- 最低覆盖时长和最低行数；
- 中间时间 gap；
- 重复 ID/重复时间戳；
- 每个账户、配置和 symbol 的覆盖；
- 严格的导出截止时刻。

因此只放入一行“看起来有效”的数据，也可能得到 `is_complete=True` 和 `execution_audit_ready`。这不足以证明可以做决策回放或逐笔审计。

**必须改成：** 为每条 stream 定义最小覆盖区间、最大 gap、去重规则和账户/配置覆盖矩阵；缺任何一项就降级为 `research_proxy` 或 `INSUFFICIENT_DATA`。

### P2：tracker 对旧记录的默认值过于乐观

[tracker.py:85](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/tracker.py:85) 和 [tracker.py:86](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/tracker.py:86) 对旧 JSON 缺少 `is_snapshot_complete` 或 `reconciliation_passed` 时默认 `True`。历史记录没有这些字段时，会被当成已验证数据。

**必须改成：** 缺字段默认 `False`，并做一次 schema migration；只有新 schema 明确写入并通过证据门禁时才能为 `True`。

## 其他工程风险

1. [generate_six_scenarios_dashboard.py:721](/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/local_optimization/generate_six_scenarios_dashboard.py:721) 对 `CAPUSDT` 和固定时间点做硬编码数据修复。应移到带版本、原因和原始哈希的输入修正清单，不能藏在通用转换器里。
2. WFA 允许 `step_days < oos_days`，但报告仍直接把各窗口 OOS PnL 相加；窗口重叠时这个累计值会重复计数。应强制 `step_days >= oos_days`，或把重叠累计明确标为诊断值。
3. 日常 CLI 的默认数据目录、网格目录和缓存文件是固定日期路径。日常流程应要求显式 manifest，或自动选择最新且校验通过的 manifest，避免无提示地使用旧数据。
4. 根目录 `.gitignore` 当前忽略整个 `local_optimization/`。这能避免研究数据误入 Git，但也会让代码、测试和本报告无法审查和复现。至少应把源码、测试、协议和报告纳入版本控制，把大体量数据和缓存单独忽略。

## 本次验证证据

在当前工作区执行：

```text
rtk proxy .venv/bin/python -m pytest local_optimization/tests/ -q
161 passed in 24.17s

rtk proxy .venv/bin/ruff check local_optimization/
All checks passed!
```

测试全绿只能说明现有测试覆盖的行为没有回归，不能证明上面的治理缺口已经关闭。当前测试还没有覆盖以下反例：错误账户/配置但同 symbol 的对账、数量不同却被匹配、缺 symbol 或有大 gap 的价格缓存、单行快照被判完整、7D/8D 语义相同参数 hash 不一致、fast 预筛排除真实 MTM 合规候选、六场景窗口超出事件推导范围，以及旧 tracker 记录缺字段时的默认值。

## 修复优先级与验收条件

### P1：换参前必须完成

1. 统一 canonical parameter schema、单位和 hash，并让所有入口只接受 canonical candidate。
2. 重写 dashboard 和底层 reconciliation 的匹配键，强制稳定 ID、账户、参数 hash、方向、时间、数量和价格；任何身份字段缺失直接 `INSUFFICIENT_DATA`。
3. 用 manifest 固定六场景与 WFA 的评估窗口，并对价格缓存做 symbol、watermark、gap、staleness 全量门禁。
4. 明确 fast 预筛的保守性；在没有证明之前，正式模式必须对完整候选集合执行真实 MTM。
5. 统一真实 MTM 后的 no-feasible 规则，禁止在所有候选不满足正收益或风险条件时“挑一个最不差”的候选。

### P2：完成 P1 后处理

1. 将近似 UI/CDaR 改为显式 `approx_*`，正式报告只呈现真实 MTM 指标。
2. 加强快照 coverage/gap/duplicate/account-config 检查，修正 tracker 旧记录默认值。
3. 移除硬编码数据修复，处理 WFA 重叠汇总和固定日期默认路径。
4. 调整 `.gitignore`，让源码、测试、协议和审查报告可追踪。

### 最低验收标准

只有同时满足以下条件，才可以把日报的 `reconciliation_passed` 和候选状态用于自动换参：

- 同一 manifest、同一 canonical parameter hash、同一账户和同一窗口；
- signal/order/fill/trade 逐笔稳定 ID 可追溯；
- 时间、方向、数量和价格在显式容差内一致；
- 所有必要价格序列完整且无超限 gap；
- 真实 15 秒 MTM 通过保证金、MDD、UI/CDaR、正收益和最少交易数门禁；
- 没有候选时明确输出 `NO_FEASIBLE_CANDIDATE`，不降级选择；
- 历史记录缺少证据字段时默认不通过。

## 最终判断

如果只问“机会池和 WFA 的核心修复是否已经落地”，答案是：**大部分已经落地，可以继续研究验证**。

如果问“local optimization 模块现在是否所有问题都修复、能否据此自动确认实盘参数并换参”，答案是：**还没有**。当前应把它视为经过加固的研究模块，继续保留人工审核；在 P1 的对账、canonical schema、manifest-bound window/cache 和真实 MTM 全候选门禁完成前，不应自动推广参数。

