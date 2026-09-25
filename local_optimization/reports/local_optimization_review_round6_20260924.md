# 本地寻优模块：新一轮架构、性能与正确性审查

日期：2026-09-24。审查对象为当前本地文件，未修改业务实现。

**跟进验证（Gemini 更新后，2026-09-24）：** 本文列出的 4 项缺陷均已在当前实现中修复；新增的 `tests/test_astra_review_round6_fixes.py` 针对性测试 5 项通过，`local_optimization/tests` 全量测试 229 项通过（22.38 秒）。复现脚本 `review_round6_20260924_repro.py` 现已对四类场景输出预期结果。以下缺陷描述记录的是修复前行为，供变更审阅和问题追踪。

此前修复的 stateful 起点浮盈重复计入问题仍然有效。本轮扩展到风控改写持仓、复利状态输出、15 秒网格之间的交易，以及字典输入的转换契约，发现以下 4 类可复现问题。

## 验证方式

- `rtk proxy .venv/bin/python -m pytest local_optimization/tests -q`：224 passed，22.79 秒。
- 新反例：`rtk proxy .venv/bin/python -m local_optimization.reports.review_round6_20260924_repro`。
- 性能采样：`rtk proxy .venv/bin/python -m local_optimization.reports.review_round2_20260924_repro --profile`。
- 反例使用合成数据，不访问生产数据、不重生成正式 dashboard。现有测试通过并不覆盖以下失败场景。

## 1. P1：网格之间的短交易漏计保证金，复利候选可越过上限

位置：`mtm_engine.py:469–483`，`evaluation_context.py:788–797`。

`_simulate_mtm_arrays` 将入场与离场时间分别向后映射到采样点。当交易在 10:00:01 入场、10:00:02 离场时，两者都落到 10:00:15，因此 `k_entry == k_exit`，保证金与持仓数量的累加分支完全跳过。

已复现：初始权益 1000，复利 f=0.5，实际名义本金 500、杠杆 5，真实占用保证金 100；设置上限 30 后，快评估与曲线评估仍返回 `peak_margin=0`、`is_feasible=True`。底层账本按缩放前的 100 本金检查，保证金仅为 20，因此也未拦截。

影响：候选可被错误判为满足保证金约束；图表的持仓峰值也会偏低。并非所有 15 秒以下交易都触发，触发条件是入场与离场映射到同一个网格点。

建议：保证金与并发峰值由精确交易事件时间线计算，采样网格用于 MTM 展示；或将交易事件点合并入采样时间线。复利后的仓位必须参与约束检查。测试应包含网格内短交易、退出/进入同刻、多个短交易重叠。

## 2. P1：风控已改写 carry-in 平仓，但统一评估仍使用原持仓

位置：`simulation_ledger.py:191–225`，`evaluation_context.py:722–725`。

账本对 carry-in 持仓应用 scheduled risk，生成更新后的平仓时间与价格。但返回的 `admitted_trades` 仅包含新交易。统一评估随后把原始 `context.state_in.active_positions` 拼回去，丢失账本的改写。

已复现：持仓入场价 100，原定 12:00 平仓；窗口 10:00–11:00；10:30 风控按 105 平仓，11:00 市价 120。快慢两条评估路径都返回权益 1019.93、退出时间 12:00；同一个结果中的 `state_out` 却为权益 1004.8565、无持仓。按费用模型，1004.8565 是该例正确的风控平仓权益。

影响：同一评估结果中的收益/曲线与续跑状态矛盾，风控退出后仍计入行情上涨收益。

建议：账本输出本窗口实际使用的完整交易时间线（包含已改写的 carry-in），下游只使用这一份记录，不从原始输入再次拼装。

## 3. P2：字典交易转换丢失 datetime 平仓时间，缺失 PnL 被写成零

位置：`evaluation_context.py:419–451`。

字典分支支持字符串、数值、pandas 时间戳，却没有普通 `datetime` 分支。传入 `exit_at=datetime(...)` 后，转换结果为 `exit_time=None`、`is_open=True`；同一时间改为 ISO 字符串则能正确平仓。

另一个同处的缺陷是 `pnl = float(r.get("net_pnl_usdt") or 0.0)`：只给入场价和退出价而未给已算好的 PnL 时，缺失值被变成显式零，覆盖 `TradeRecord.calculated_net_pnl` 的计算。100→110、默认 100 本金的例子返回 0，而按当前费用模型应为 9.853。

影响范围：直接调用该公开转换函数的字典输入，以及使用字典数据的回放/兼容路径。当前统一评估主路径使用 TradeRecord，因此并非每次评估都触发。

建议：共用一个时间解析函数；区分缺失值与合法零值。回归测试比较 datetime、ISO、epoch 的等价性，并比较“省略 PnL”与按价格计算的结果。

## 4. P2：复利评估输出的 state_out 仍是未缩放仓位

位置：`evaluation_context.py:708–758`、`evaluation_context.py:837`。

`state_out` 在原始本金的账本模拟中生成；随后 `to_trade_records` 按复利放大仓位并计算指标，但输出状态没有同步变化。

已复现：初始 1000、f=0.2，入场价 100，窗口末价格 120，持仓尚未退出。评估记录本金 200、期末权益 1039.86；输出状态中的本金仍为 100、权益为 1019.93。下一窗口以该状态续跑会丢失一半仓位。

影响范围：`ScenarioSpec(compounding=True)` 与跨窗口状态续跑的组合。当前 WFA 默认调用没有开启复利，不能把此项描述为所有 WFA 都出错。

建议：缩放后的实际交易和最终 MTM 应共同生成状态；若暂不支持复利续跑，应明确拒绝该组合，而非返回看似可用的状态。

## 架构改进优先级

### 优先：让交易事实、指标和状态只有一个来源

当前统一函数仍是多套计算逻辑的拼接：账本先决定交易和状态，转换函数再改变仓位，MTM 又计算一份权益，最后混合输出。上述第 2、4 项就是这个结构带来的偏差。

建议让一个内部模块拥有“约束下的实际交易时间线”，统一处理 carry-in、风控退出、仓位大小和新入场。随后基于同一时间线计算 MTM、保证金峰值和 state_out。外部继续保留 `evaluate_candidate(context, candidate, scenario)`，不需要给调用者增加更多协调参数。

必须守住的契约：`terminal_equity == state_out.total_equity_mtm`；结果中的未平仓实际交易与 `state_out.active_positions` 的金额、方向、退出安排一致；连续窗口与分段续跑在相同策略下终值一致。

### 其次：收紧输入语义与类型

转换函数标注字典列表，实际上也接受 TradeRecord；`raw_admitted` 标注字典列表，实际存入 TradeRecord。`EvaluationContext` 同时接收 raw prices/aligned grid、账本/资金参数、initial_equity/state_in，调用者需要掌握不少优先级规则。

建议在输入层完成类型和时间标准化，核心使用 `Sequence[TradeRecord]` 等准确类型；将已准备好的市场窗口（价格网格、排序后的机会）作为可复用的不可变输入；由状态或资金配置提供唯一初始权益来源。

## 性能改进

### 1. 优先减少非复利路径的重复工作

合成样本为 500 个机会、10 个交易对、2 天、连续 5 次候选验证。该次 profile 的累计时间约为：`simulate_window` 38 ms，数组 MTM 20 ms，`to_trade_records` 15 ms。该样本用于定位热点，不代表生产吞吐，也不是优化前后对照。

非复利路径仍排序并克隆不可变 TradeRecord，逐笔调用 `strftime`、时间戳与 PnL 属性。对已经标准化、排序且不需要变更的 TradeRecord，可复用记录，仅返回新的列表或不可变序列。账本计算完粗略回撤后统一评估又算完整回撤，适合把“生成实际交易”与“选择需要的统计输出”拆开，避免重复统计。

### 2. 复利计算从每天全量扫描改成事件推进

`compute_daily_compounding_scales` 在每天起点和终点均调用 `_calc_floating_at`，后者扫描全部交易并重复解析时间，复杂度包含 O(天数×交易数)。可预先缓存数值时间戳、按事件推进活动持仓集合，在日切时只重估当时仍持有的交易。需要保留跨日、午夜退出和 carry-in 的对照测试。

### 3. 在 WFA 同一窗口复用价格网格，控制 worker 内存

WFA 为 recommended、daily best、baseline 等多次构造 EvaluationContext，默认会重复对齐价格。可以按相同窗口准备一次 AlignedPriceGrid，供多个候选和轨道复用。当前 dashboard 的 worker/context 已有复用，应保留已有成果。

多进程下还需测量价格缓存与对齐数组的每进程驻留量。仅 float64 网格理论大小为 `交易对数×采样点数×8`：500 对、30 天、15 秒网格约 659 MiB/份，未含原始列表、机会与临时数组。这是容量估算，不是本机实测。若真实数据达到该量级，再评估只读 mmap/shared memory，避免先增加 worker 数导致内存压力。

## 建议执行顺序

先修正事件级保证金约束和风控 carry-in 记录，再统一复利状态输出、补齐字典转换契约。补充上述对照测试后，优先优化非复利转换和 WFA 网格复用，最后根据实际数据的 profile/RSS 决定事件扫描与共享内存改造规模。
