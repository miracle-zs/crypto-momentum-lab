# local_optimization 当前实现审查（2026-09-20）

## 结论与审查范围

**本轮修复关闭了若干上一轮反例，但当前实现仍不能据此确认最优参数、复利风险受控或实盘回测一致。** 每日主入口使用六场景流水线；它已经接上因果日切、动态峰值保证金、共同曲线窗口和同参数查找，但仍有约束放宽、机会池不完整、真实权益未进入复利选优、实盘证据不足等阻塞问题。它们会改变交易集合、参数排名、资金需求和前向验证结论，不只是显示或代码风格问题。

基线为[上一轮复审](code-review-2026-09-19-local-optimization-follow-up.md)的源码 SHA-256 和行为证据。需求依据为[本地寻优设计](superpowers/specs/2026-09-18-local-parameter-optimization-design.md)与[复利仓位研究](research/2026-09-19-compounding-position-sizing-and-objectives.md)。这些源码被 Git 忽略，不能用空 Git diff 判断没有变化；本次读取当前文件并核对真实入口。新增六场景、walk-forward、仓位模块也纳入审查。

审查方法：独立 Standards / Spec 双轴审查、现有完整模块测试、静态检查、内存和临时目录反例。未运行完整网格搜索，未连接服务器、拉取数据、改实现或覆盖既有业务报告。本报告不代表已覆盖所有分支。

### 本轮复查更新

以下结论覆盖正文中上一轮的状态描述：跨日未来收益提前放大新单、八曲线时间轴错位、收益率放大100倍、MTM事件时刻使用旧权益、非整桶末尾事件丢失、空实盘目录伪造PASS等反例已分别修复或降为部分修复；本轮仍以“硬约束不放宽、真实权益先于选优、缺证据不认证、同参冻结前向可重放”为关闭标准。正文的 R2–R6、M1–M6 记录当前状态，旧反例只作为修复轨迹保留。

### 验证结果

| 检查 | 实际结果 | 能说明什么 |
|---|---|---|
| `.venv/bin/python -m pytest local_optimization/tests/ -q` | **84 passed，16.37秒** | 当前测试通过，但仍未覆盖下述主流程反例 |
| `.venv/bin/python -m ruff check local_optimization/ --output-format concise` | **51 errors** | 新增脚本仍有未定义变量、未用导入、格式和行宽问题；其中 `t_min` 未定义会使真实 CSV 分支直接失败 |
| 主链资金约束反例 | 复利候选动态峰值保证金**308U**，仍被归入≤280U场景 | 动态计算已接通，但空集 fallback 仍破坏硬约束 |
| 主链回撤约束反例 | 所有候选 MDD=20% 时仍返回候选 | 超过15%时仍选择最小回撤，未返回无可行解 |
| 空/部分实盘对账反例 | 空数据为 INSUFFICIENT_DATA；仅11条余额+回放交易仍可PASS | 缺 signals/fills/orders 时证据仍不足但会被认证 |
| 八曲线与收益率反例 | 共同窗口和百分比单位已修复 | 该两项上一轮问题已关闭 |
| 真实数据复利 CLI | `run_compounding_experiment.py:202` 使用未定义 `t_min` | 真实历史输入分支仍不能运行 |

### 已确认的修复

- 旧 `dashboard.load_optimization_groups` 空目录返回空结果，不再生成固定356.39U冠军。
- 通用选优器拒绝已检查风险字段的NaN，不再原地污染跨场景复用的可行性；缺失理论邻居计入分母。
- tracker现在检查候选可行性、邻域稳定性、交易数和MDD门槛，但快照完整性和对账状态尚未传入准入条件。
- snapshot支持gzip并拒绝完全无关的表头；内容值、时间水位、账户覆盖仍未验证。
- FIFO配对已隔离账户；旧看板汇总笔数和records/summary结构已修，收益字段仍不匹配。
- MTM已支持共同起止、末尾非整桶事件、事件时刻预估权益；上一轮“未来收益复利”和曲线错位反例已关闭。
- 缺实盘数据现在返回 INSUFFICIENT_DATA；但只要余额行数超过门槛，缺少其他实盘事件流仍可能显示固定审计成功。

## Standards

本轴检查数据与展示的真实性、接口契约以及实现规则是否分叉。项目设计要求缺证据不能认证、实盘和模拟分轨、共同窗口比较；`CONTEXT.md`定义的持仓批次也不能仅凭简化FIFO宣称完全复现。重复业务实现属于工程判断，以下高优先级项均另有具体错误证据。

**独立结论：**旧看板和FIFO的若干问题已修，但新增六场景入口重新引入缺数据伪造实盘曲线和固定审计成功。账户汇总收益及身份校验仍不完整；按币种日期硬改成交事实不可追溯。三套对账/展示规则继续分叉，手册仍夸大验证程度。

### E1 [P1，部分修复] 部分实盘数据仍生成固定PASS与匹配率

位置：`local_optimization/generate_six_scenarios_dashboard.py:616–641,645–680`。

完全没有实盘余额或余额不足时，当前代码已返回 `INSUFFICIENT_DATA` 和六层 NA；但只要余额超过门槛且存在回放交易，即使没有实盘 signals、fills、orders，也会固定输出 L1 100%、L2 99.8%、滑点和人工平仓归因。这些仍不是逐事件计算结果。

**实测：**空临时data/live目录现在返回 `INSUFFICIENT_DATA`；但构造仅11条余额记录、1笔回放交易且不提供 signals/fills/orders，仍返回 PASS、99.8%匹配和固定滑点。每日总控仍可能把这种结果展示为因果对账。

应把每条所需实盘流作为独立证据门槛，缺任何关键流都只能是 `INSUFFICIENT_EVIDENCE`；匹配率、滑点和归因必须来自实际匹配记录。差异低于某金额不能证明六层因果审计通过。

### E2 [P1，新引入] 实盘与回放未建立共同窗口

位置：`generate_six_scenarios_dashboard.py:540–550,562–577`。

按入场是否落窗筛选交易，遗漏期初持仓；随后MTM不传共同start/end，可能延伸到实盘截止之后。merge_asof没有陈旧容忍，实盘last和回放last未必属于同一时刻，仍计算收益漂移。

应保留carry-in并按共同截止估值；缺价、资金流和配置段单列。否则所谓差异可能来自不同窗口，而非执行偏差。

### E3 [P1，仍有风险] 按币种和事故时刻硬改成交

位置：`generate_six_scenarios_dashboard.py:415–419`；`generate_batch_concurrency_curves.py:119–125`。

当前增加了“只有持仓跨过事故时刻才修订”的条件，已关闭“退出早于入场”的直接反例；但修订仍按币种、日期和时间硬编码，且发生在事件筛选之后的转换路径。

应按具体事件 ID、原始证据和修订版本在统一输入层修订，并让筛选、定仓、指标和曲线读取同一修订版本；不能在画图阶段用币种日期替换交易事实。

### E4 [P1，残留] “同参数”匹配仍未核验账户和配置身份

位置：`reconciliation.py:109–113,173–174,262–268`。

匹配主要依据symbol、方向、时间、价格，未强制账户/配置/运行区间一致。实测A/config-a与B/config-b记录可以匹配并返回 `is_audit_passed=True`。FIFO账户隔离修复不等于这个匹配入口已修复。

另外，`run_live_reconciliation.py:431,471`仍输出固定“零意外拒单”“风控与订单通路100%健全”。应按完整身份分区、验证同参初始状态，缺身份不可认证。

### E5 [P2，部分修复] 汇总收益字段契约仍不一致

位置：`dashboard.py:356–357,379–381`；`reconciliation.py:647–649`；`templates/dashboard_template.html:1372–1375`。

matcher输出 `total_live_net_pnl/total_replay_net_pnl`，汇总读取 `live_total_pnl/replay_total_pnl`，模板又读取matcher名称。实测单账户1笔、+10U，单账户summary正确，聚合笔数为1但收益为0。应统一结构并测试完整producer→aggregate→template链。

### E6 [P1/P2，残留] 部分期初持仓和旧入口说明尚未闭环

`reconciliation.py:416–422`：BUY1、SELL2只输出quantity1，却附上整笔SELL的20U收益，标记非carry-in；缺失的另一份数量未保留。旧目录与固定截点仍见 `dashboard.py:107–116,269–278,317`，D/F盲映射仍见465–466。`HANDOVER_MANUAL.md:104,164,184,393,407`仍有错误参数语义、未实现相位参数和“100%同构/完整压力验证”等声明。上述不是本轮新发现的全部问题，详见上一轮报告；不能据局部修复整体关闭。

### E7 [已关闭] 不同权益曲线按数组下标拼接

位置：`generate_six_scenarios_dashboard.py:870–875,915–959`。

当前先计算所有曲线的共同起止时间，再使用同一15秒采样轴；上一轮不同长度报错和同长度异起点错位的反例已通过。

仍需在正式报告中验证降采样极值和零持仓曲线，但共同窗口问题本身已修复。

### E8 [已关闭] 收益率显示放大100倍

位置：`generate_six_scenarios_dashboard.py:900`与`equity.py:358`。

当前直接使用 `metrics.net_return_pct`；本金1000、净收益10时metadata为1.0%，上一轮的100倍显示错误已关闭。

### E9 [P2，新引入] 每日价格缓存不会随新数据更新

位置：`build_price_cache.py:12–19`。

输入绑定9月19日，只要缓存存在即退出，没有快照、截止时间、标的覆盖或代码版本失效判断；六场景默认交易输入已为9月20日。结合MTM无限延用旧报价，新交易可能在旧价格上被当成完整估值。

应按输入内容与截止时间生成不可变缓存，并校验当日全部交易覆盖；缓存存在不等于当日可用。

## Spec

本轴单独检查需求是否落地，不与Standards合并计数或重排。直接依据设计中的原始数据回放、真实路径风险、冻结前向、有效新增日和无可行解规则。

**本轮复查结论（以 R1–R6 当前状态为准）：**未来收益提前进入日切的具体反例已关闭，但真实盯市权益、硬约束无解语义、机会池完整性、冻结前向窗口和快照准入仍未闭环。

**独立结论：**新版还不能作为可信复利寻优及换参依据。通用工具层修复了一批反例，但daily改走独立six-scenarios链。新链从两个既有配置的成交记录筛参数、按入场日提前计入未来退出收益，真正MTM发生在冠军确定之后；全部候选违约仍放宽门槛。daily OOS含换参收益，snapshot为日期构造的complete对象；walk-forward跨边界泄漏且重叠求和。应先统一逐参数执行、权益风险和冻结评价，再扩展搜索。

### R1 [P1，新引入] 两个基准的已成交集合不能代表全量参数空间

位置：`run_walk_forward_analysis.py:179–193,250–251,296–299`；`generate_six_scenarios_dashboard.py:300–310,724`。

只加载primary和acc02的已成交CSV，缺entry_at的机会被丢弃，w/c按来源强行标为(2,1)/(3,1)，候选又要求w/c精确相等。因此w=1/4、c=2/3等永远没有事件；降低原策略涨幅、强度、量比门槛，也补不回原策略拒绝的信号或限仓拒单。提高并发槽位也不能恢复已经被源配置丢掉的机会。

需求要求按候选参数生成自主信号与订单。当前只能称“在两个既有成交集合上做子集分析”，不能称全量8维寻优。应从覆盖候选空间的原始行情/特征和统一执行状态重新回放，或明确限定分析能力。

### R2 [P1，部分修复] 复利日切不再提前使用未来退出，但仍与真实MTM评分脱节

位置：`generate_six_scenarios_dashboard.py:186–220,391–404,741,841–875`。

当前已按退出日记录已实现盈亏，上一轮第5天退出收益提前用于第2天加仓的反例已关闭；但日切仍只使用已实现盈亏，没有浮动盈亏、开仓成本和完整无交易日，评分仍是事件日期汇总的 `log_growth - 2*UI`。冠军选定之后才重构15秒曲线，盘中风险没有参与选优。

**实测：**第1天入场、第5天退出盈利100；第2天另一笔零收益时，当前第2天额度保持100，说明未来退出没有提前进入余额；但增长分母仍只按有事件日期计算，和完整5日权益路径不同。研究文档第36、81、91–101行要求边界可得权益、完整日、高频路径硬约束与逐事件回放。

目标函数可以另立实验研究，但不能把这套未经声明且有未来信息的日汇总分数当成已落地的研究方案。应让统一账本先产生各候选真实权益和约束结果，再决定排名。

### R3 [P1，仍未关闭] 复利候选违反保证金/回撤门槛仍入选

位置：`generate_six_scenarios_dashboard.py:311–330,363–369`；`run_daily_local_optimization.py:79–90`。

动态复利峰值保证金已经计算并参与正常候选筛选；但当动态≤280U候选为空时，代码退回固定仓位候选。当所有复利候选MDD>15%时，代码返回最小回撤候选，而不是结构化无解。daily转换仍无条件把候选标为可行，且把固定仓位MDD和复利UI放进同一个标准评价。

**两个实测反例：**

- 第1天一单获利100，第2天14个不同币种同时开仓。固定仓位筛选峰值为280U，复利单笔110U后动态峰值为**308U**，仍被选为 `m280_compounding`。
- 15笔分15日，第一笔−200，之后14笔各+20；返回的复利推荐MDD为**20%**，超过自己的15%门槛。

空输入还会直接 `ValueError: max() iterable argument is empty`。研究验收第139行要求无可行解，不允许自动放宽。资金比例约束、绝对容量和动态拒单必须使用所评价机制的真实路径；无解应结构化返回。

另有两个边界问题：`compute_daily_compounding_scales` 用 `f * max(100, equity)`，权益降到0或负数时仍可能生成0.1倍的新单额度；没有破产停止或无解状态。`solve_six_scenarios` 在候选为空时仍直接对空集合取 `max`，抛出 `ValueError`，而不是返回可展示的无候选结果。

### R4 [P1，部分修复] 前向收益、有效日和换参资格仍未成立

位置：`run_daily_local_optimization.py:529–543,568–569,597–602,623–631`；`tracker.py:134–202`。

OOS现在会按昨日参数在今日 `all_candidates` 中查找，关闭了“直接用今日新推荐和昨日旧推荐相减”的主要反例；但它仍是候选全历史累计收益之差，没有固定窗口、盯市边界、数据修订版本和冻结状态证明。昨日参数若因今天候选集合只保留正收益/≥15笔而消失，仍不能形成可靠的负向前向结果。

主入口现在对非空 live 目录调用 `inspect_snapshot_dir`，空目录保存 `is_complete=False`；但快照内容验证仍很弱，且 `is_complete`、审计结果和数据覆盖没有传入 tracker 的 stable 准入。tracker虽已检查候选可行性、交易数、邻域稳定性和MDD，但不检查真实快照/对账证据。

**实测：**旧的“不可行候选也能stable”反例已被 tracker 门槛拦截；但用伪造的不同日期快照、缺少真实审计证据的记录仍可满足其余条件。设计第207、227–231行明确要求同参新增窗口、风险合规、真实新增数据与活动覆盖。

### R5 [P1，新引入] walk-forward训练读取未来退出，并重复累计重叠OOS

位置：`run_walk_forward_analysis.py:227–238,317–329,361–405,550–566`。

按检测/入场时刻归属IS或OOS后，直接求完整交易最终净收益，退出跨窗时未切点盯市。保证金借用全期CSV字段，缺失默认200；UI仍MDD×0.45。缺退出补15分钟假退出。默认OOS3天、步长2天，报告又将各折收益直接相加，重叠日重复计算。

**实测：**day1入场、day5退出+100，调用筛选day1→day2窗口，收益已经为100。设计第247、251行要求外层不重叠且不得使用未来退出收益。应连续保存跨边界仓位与状态，按权益边界计收益；重叠窗口可作诊断但不能相加为一条独立可交易业绩。

### R6 [P1/P2，残留] 旧正式入口和快照内容校验仍未完成

`run_two_stage_grid_optimization.py:219–248`仍用MDD×0.45做最终UI，没有精确复核；`snapshot.py:166–176`表头只需与认可字段集合有任意交集，任意非空行即计数，earliest/latest始终None（155–156），却可授予ready（197–205）。

**实测：**每个文件只写一个认可表头和 `not_a_valid_record`，仍complete及两个ready。gzip支持修复不能替代内容和水位验证。每日HTML固定同名、按日期伪造快照，也没有形成完整不可变run身份。旧两阶段新增并行仅加速邻域计算，不等于daily六场景真实回测已并行。

## 主审补充：底层仓位与回测反例

下列针对独立MTM/sizing/实验入口，与上述两个轴分开记录。目前daily复利并未调用这个仓位策略，所以修正此处也不能自动修正R2/R3。

### M1 [已关闭] 订单风控与日切使用上一个采样点的权益

位置：`mtm_engine.py:318–347,377–393`。

当前在日切和订单检查前先按事件时刻重算 MTM 权益；上一轮“旧仓浮亏仍按旧权益放行新单”的反例已关闭。

当前引擎仍始终传 `reserved_margin=0.0`；单元测试手填预占数值不等于回放中已经实现挂单生命周期。

### M2 [已关闭] 默认截止向下取整，丢掉最后一笔交易

位置：`mtm_engine.py:255–261`。

当前无显式 `end_time` 时使用网格上取整，00:00:01入场、00:00:10退出的交易可以进入最后采样点；共同窗口仍应由快照截止定义，不能由单个候选最后交易时刻决定。

### M3 [P2，仍未关闭] 复利实验CLI历史数据分支报错

位置：`run_compounding_experiment.py:200–214`；`mtm_engine.py:127–130`。

当前历史数据分支仍在计算 `calendar_days` 时使用未定义的 `t_min`（Ruff 报 F821），有平仓记录也会直接失败；全未平仓虽然已避免 `max(empty)`，但该路径仍需真实数据集成验证。

应统一接口、处理全未平仓与共同结束时刻；缺少用户指定数据不能静默切换成合成行情后产出貌似正常实验。默认合成实验只是演示，不能作为历史输入验收。

### M4 [已关闭] 关闭平滑的风险机制组合无法运行

位置：`sizing.py:385,410–411,455–457`。

`RiskAdaptiveSizing(use_smoothing=False).on_day_cut(1000, time)` 当前测试通过，上一轮对 `None` 格式化的TypeError已修复；仍需把各机制组合接入正式每日主链。

### M5 [后续验收缺口] 数据质量、资金流水与成交能力尚未形成统一闭环

`get_price_at`已修复回退入场价的问题，但 `max_stale_seconds`不再起作用；缺整个标的行情仍可使用入场价。当前复利回放没有挂单、部分成交、实际资金费/外部流水、交易精度与容量的完整状态驱动链。独立TWR和risk-check函数的单测不能证明这些机制进入daily链。应明确当前能力范围，不能用“100%连续真实估值”“完整压力验证”代替覆盖证据。

### M6 [P2，新发现] MTM 初始化重复触发日切

位置：`mtm_engine.py:282–285,336–339`。

初始化阶段对同一时刻调用了两次 `sizing_policy.on_day_cut`。普通比例策略的额度通常不变，但版本号会增加两次；风险自适应策略会重复记录初始日收益，影响波动率和状态历史。应只保留一个初始化入口，并用测试断言首次日切次数为1。

## 建议的修复顺序与关闭条件

1. **先让报告忠实反映缺失与失败。** 删除无实盘数据的回放冒充、固定审计率/归因；无候选或全部超限时返回无解；撤下自动PRODUCTION_ELIGIBLE结论，直到真实门槛接通。
2. **统一候选执行和账本。** 从足够完整的原始数据对各参数生成事件；固定仓位/复利共用策略执行与资金账本，仅仓位策略不同。不要维护six、WFA和独立sizing三套盈亏语义。
3. **让真实风险决定排名。** 每日边界可得权益定仓，事件时刻检查动态保证金及预占；候选精确MTM与硬约束先于最终推荐，违例不能用近似结果保留。
4. **接通冻结前向。** 同一旧参数、同一状态、真正新增的区间独立计分；数据延展和重新选参贡献分开；每个外层切点不能访问未来退出，OOS曲线不重复日期。
5. **恢复数据身份与共同窗口。** 快照内容、水位、修订版本和缓存身份可核验；固定所有横向曲线共同时间轴；每天保存不可变run产物，不用日期字符串伪造证据增长。
6. **再完善并行、UI与便捷脚本。** 先修收益单位和真实CLI，之后优化性能。当前邻域并行测试通过不说明主链回放可信。

下一轮应运行**真实生产入口的集成反例**，不是只测试辅助函数：缺数据→NA；空候选→无解；未来平仓→不影响早期定仓；280U→复利后仍满足同一资金合同；15%上限→20%候选被拒；旧参数新增亏损→OOS不得被新参数历史盈利覆盖；无效快照/风险失败→不可stable；不同起止曲线→按共同timestamp对齐；真实CSV有/无未平仓均可运行。

现有 `test_follow_up_review_fixes.py:test_r1_incremental_oos_forward_pnl`只断言100与90相减及数据类保存，不调用daily真正的OOS计算，因此它通过不能关闭R4。类似地，仓位测试手动传入reserved_margin，只证明一个算式，不证明回放中有预占状态。

## 文件指纹

以下为本轮审查时的SHA-256，便于后续按同一证据复核。源码行号随修改可能变化。

| 文件 | SHA-256 |
|---|---|
| `local_optimization/__init__.py` | `5da04cc5f2f3a3c9d20f585b413a3936ce80a8449e23c9a9a589233cd75d64f7` |
| `local_optimization/adaptive_grid.py` | `a76bfb9ebf9965d61c4785c8dd9df1abfb77c928ecffb8bc13c1c6faa79f50ad` |
| `local_optimization/build_opt_groups.py` | `7f95be3c092945d4f1efdd9d26cf033e2dc84d3c437cc8be0ddde4c026132b4c` |
| `local_optimization/build_price_cache.py` | `bf8aae2223e04cc0f46e04eda812bfebdb3f54a2c5b5238c8d3178876f240d57` |
| `local_optimization/compare_baseline_mtm_equity.py` | `080bdf985666b9133a98f38384cd9bf18cc39ced58fe8ddc35a0373cd2990f04` |
| `local_optimization/dashboard.py` | `933a4345df9b3aae4ed3b2e089fa23364df91ea2f478c0519cbdcefca1debfcc` |
| `local_optimization/equity.py` | `ece2f69dc8bd778bd461975308ab75ebc61b90b7643ff88c942f8594306cd09f` |
| `local_optimization/generate_batch_concurrency_curves.py` | `78e82497acdf5a9a6278a198cd3e805193b03b716d781949cfb911325b48c451` |
| `local_optimization/generate_six_scenarios_dashboard.py` | `d17f3482d8895b0b5474ab1f5e21fded21b924707782bcd48a3a1a76e818a898` |
| `local_optimization/mtm_engine.py` | `cbd664f2f6532da9bbffc4cd6b6046da3285a4e09d69d3bd4da87d2042f545cd` |
| `local_optimization/optimizer.py` | `c2704c10300bc903772be8443c0cebd345b6c9707d12affa711860fed3c89ded` |
| `local_optimization/protocol.py` | `90fa23aa88dd0211469d61f6e8d6d079ec1ebd68c921e4a8c5e5a6ba29f784dd` |
| `local_optimization/reconciliation.py` | `89b8392f30437af28f240e3c67f07abaaf78bd8bb2eff585b1f87b7ef3777839` |
| `local_optimization/reporter.py` | `c3376c6b8c7adf2a7bc85abd5d4824e57eb6d270997b54f1924342213c106615` |
| `local_optimization/run_compounding_experiment.py` | `64cc9eb730955de37a3a1132fc1e6971b016c10c87295e40c498cf1c91280eb5` |
| `local_optimization/run_daily_local_optimization.py` | `321a856e01d353370ef5800ee5ad9c44de9984eac3641648e762f16871da7f05` |
| `local_optimization/run_live_reconciliation.py` | `541f02b3aa655849c64773bd03f1a2fc7bd00c18df967689aeebd691f4864259` |
| `local_optimization/run_two_stage_grid_optimization.py` | `4334d66c5d79406f30ee1c1158ec1d2d14c6aa34e42bf30d51479a1f4223739e` |
| `local_optimization/run_walk_forward_analysis.py` | `2267a9bc661fff9bec96fc3dea13d543acbc727bb9585f032cb7a04d13c5ba12` |
| `local_optimization/sizing.py` | `5901c0331f0367ef5b3a25a53597ddcd05431b8f99814972d04cd833816bbfa3` |
| `local_optimization/snapshot.py` | `477a5f9d51ee9656eaf1eabfde3c28f56980dc1fc8a7c3ecee8d1063628ad0bb` |
| `local_optimization/tracker.py` | `3ebde923de35ba882f573b90f1c5745cf7ff82c8e19bb56c4f7624cbc26bb5b2` |
| `local_optimization/templates/concurrency_curves_template.html` | `98b81a8b7aae26cb7e48a228e332370acaf9eaa9c283de5c49f3bef39ecf4ed9` |
| `local_optimization/templates/dashboard_template.html` | `86cbb6848c30fed409b3c5024d02450a5ad1ffadf87579fe5ccfdcfeb94f62e1` |
| `local_optimization/templates/six_scenarios_template.html` | `d24a7addc511ad9023ef976d78f817ea7167f0b53ced2ce1b828260fe101e6c3` |

本轮双轴独立汇总：Standards 6组当前残留，最严重为缺少 signals/fills/orders 时仍可能生成审计成功；Spec 5组当前残留，最严重为机会池不完整、复利选优未使用完整账户盯市权益且硬约束仍会 fallback。两轴存在重叠，不相加为独立缺陷总数；本轮关闭项和新增 M6 已在正文标注。
