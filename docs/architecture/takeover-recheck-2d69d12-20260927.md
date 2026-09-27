# 接管复查：2d69d12（2026-09-27）

快照：`2d69d12`。复查 `24c3384` 之后的接线与删除，静态追踪 src/deploy，并运行本地隔离探针及相关单元测试。没有连接服务器、部署、访问交易所或修改实现。本报告取代上次清单中已过时的“未接线”描述，不证明线上正在运行此版本。

## 已确认补齐的接线/删除

- Live reduce-only 请求已调用 ExecutionBook.act，不再只是register_prepared_command补录。
- 旧 rebuild_position_batches 算法、PositionLedgerShadowComparator、primary/gray双轨开关已删除；不再列“旧批次算法fallback”。这只确认旧业务旁路删除，不代签PG故障恢复验收。
- Trace drain已注册到ownership_registry，不再列“drain无人调用”。
- Live已设置decision exit handler；Paper已执行dec_res.exit_command，不再列“退出命令完全无人消费”。
- RuntimePlan已传入部分真实overrides；CapabilityEvidence已排除当前clientOrderId并检查租约到期，不再列“无overrides/租约直接True”。
- 新增了execution_commands upsert与restore、latest Trace restore、保留依赖登记、对账head读取、audit decide调用。这些接口存在不等于全部语义完成。

## 仍未完整接管的12组职责

| 项目 | 现存缺口 | 可定位证据 |
| --- | --- | --- |
| 1. ExecutionBook完整接受/结算 | 入场非reduce_only在_ensure_reservation直接返回，未走act；消费/释放仍由OrderExecutionCoordinator直接操作domain coordinator和reservation repo，observe并不是唯一结算出口 | execution_account/orders/coordinator.py:413–419、:525–570、:575–667、:493 |
| 2. 耐久执行协议 | outbox写入与reservation分事务，保存失败被吞，状态变化create_task无监督；存储缺完整command/request/receipt，restore以默认LONG/MARKET/quantity=1重建，未恢复请求回执。命令加载不按账户scope隔离，去重只读最近2000记录 | domain/execution/execution_book.py:242–374、:704–720；persistence/postgres/order_repository.py:951–1033 |
| 3. Live唯一政策 | 新退出handler与旧LiveExitManager/独立退出channels同时存在；filter仍自行构造EffectivePolicy，不使用RuntimePlan.effective_policy，原signal strategy仍生成候选，未统一完整政策 | live_rollout/runtime_orchestrator.py:800–826、:1228；domain/decision/decision_engine.py:780、:836；live_rollout/exits.py:341、:558、:606 |
| 4. Paper唯一仿真/政策状态 | 新execute_exit已调用，但此前旧mark_positions仍先更新positions_by_id，旧fill resolver仍生成入场结果，新adapter另写journal；两个生命周期并行存在 | strategy_runner/paper.py:304–318、:409–449、:483–500 |
| 5. 政策状态与Trace事务 | 从最新Trace只恢复部分字段，grace/holding/signal/custom/sizing状态未保存恢复；内存next_state、异步Trace与命令接受仍分开。最新Trace加载还在持有session时调用load_decision_trace另开session，pool_size=1时可能超时 | live_rollout/decision_facts.py:266–298、:377–402；domain/decision/decision_engine.py:658–665；persistence/postgres/decision_trace_repository.py:265–283 |
| 6. RuntimePlan配置权威 | overrides已增加，但生产filter自行创建EffectivePolicy，effective_policy无实际消费入口；编译器未覆盖完整策略/执行/risk选项，身份与实际执行政策未完全收敛 | live_rollout/runtime_orchestrator.py:635–661；domain/runtime/runtime_plan.py:144–215；domain/decision/decision_engine.py:780、:836 |
| 7. SizingModel/SizingPlan | 模型/helper存在，但未发现生产给policy设置sizing_model或symbol_lot_rules；helper在model=None直接返回原candidate，旧定量/量化仍主导 | domain/decision/policy_transition.py:237–256；domain/decision/decision_engine.py:EffectivePolicy；全仓sizing_model=搜索 |
| 8. 权威能力证据 | 当前请求排除已修；account_identity仍按account_label+API key存在判断，concordance仅排除unmanaged，universe/approval借entry_enabled。不是独立、带来源的身份/覆盖/对账证据 | live_rollout/runtime_orchestrator.py:664–738 |
| 9. DeploymentCoordinator | src/deploy未发现实例化/调用；仍只有领域协议和内存journal/supervisor，真实writer移交与部署日志未由其接管 | domain/runtime/deployment_coordinator.py:216、:239、:281；全仓调用搜索 |
| 10. 保留依赖登记 | Live已调用登记，但runtime创建RetentionAuthority()，默认InMemoryRetentionRepository；独立PG清理进程看不到这些保护。Trace先保存、依赖后登记，也没有共同锁事务 | live_rollout/runtime_orchestrator.py:521；live_rollout/decision_facts.py:330–353；domain/operational/retention_authority.py:123–124 |
| 11. 收益覆盖认证 | calculator已接入，但builder继续从已有rows评估覆盖并创建CoverageReceipt，采集端未提供完整source扫描receipt | operator_dashboard/performance_builder.py:322–340 |
| 12. 运营健康权威证据 | 已读reconciliation head，但head缺失仍用非HALTED推成matched；capability仍由查询层cap_ok组合，fact_gaps仍按READY状态推断，不直接消费CapabilityDecision/真实覆盖证据 | operator_dashboard/overview_queries.py:279–319 |

以上是接管职责缺口，部分是同一模块中的不同协议，不应解读为12个相互独立的新类。

## 决策重现的附加缺口

审计CLI如今确实调用decide，应撤回“完全不重放”的旧描述。但它以默认PolicyState、空PositionView、固定cash=10000/universe=u1/risk_v1重建输入，而不是原冻结事实；仅比较是否有intent和拒绝原因，不比较完整intent、exit command、next state、input hash和frame digest；缺market_state时仍能返回VERIFIED_REPRODUCIBLE。因此仍未达到精确重现验收。

证据：tools/reproduce_decision.py:187–257；build_decision_trace未保存完整prior state/position/policy artifact。该项独立列出，因为它是新增实现存在但语义不足，不能再称“未调用新模块”。

## 验证与边界

相关测试：97 passed in 0.56s。范围为ExecutionBook、coordinator、LiveDecisionFactSource与decision；排除integration/live并去除数据库环境变量。本次没有运行真实PG并发或故障演练。

纯本地restore探针：一条EXIT执行记录恢复为quantity=1、side=long、order_type=market、reduce_only=False，receipts数量=0；原始保存details没有完整订单字段，默认重建会改变命令语义。此探针没有发送订单，不声称已经线上重发了默认命令。

PolicyState restore探针提供grace_until/holding_deadline/signal_memory，restore后均为空。默认RetentionAuthority仓库类型探针为InMemoryRetentionRepository。

优先处理：耐久完整执行命令和回执、唯一observe结算、完整政策state事务、持久保留依赖；其次收敛Live/Paper政策与定量，再完成发布、收益及运营证据。不要仅用新增方法名或提交标题认定完成。


## 前端自动刷新跳到顶部：具体修复方案

用户反馈：此前宣称修复后，仍出现前端跳到最上面。本项保持“未修复/待真实浏览器验收”，不能把663eeaf或相关单测通过作为关闭依据。此次检查了本地前端源码；尚未在用户实际浏览器/线上版本录制复现，因此以下区分确定的代码路径与需现场验证的触发因素。只写方案，未修改前端实现。

### 已确认的代码缺口

| 路径 | 当前问题 | 为什么之前保存scrollY仍不够 |
| --- | --- | --- |
| sections/strategy.js 的refreshStrategy | 重建上方账户cards后调用focusedTab?.focus()，没有preventScroll | 用户先选账户再向下阅读，焦点可能仍在上方tab；自动刷新重新focus可把tab滚入视口 |
| dashboard.js 的refreshSection catch | 超时/错误时把整个panel-body替换成短emptyBox | 长页面高度突然缩短，浏览器把scrollY截断到新的最大值；内容太短时可能接近0，scrollTo也不能恢复不存在的位置 |
| sections/account.js 的账户detail/metrics | 整块重建后还会异步填入详情或图表；初次/切换range使用短loading块 | 第二次替换和图表布局发生在第一次restore之后，scrollY保存不涵盖整个更新过程 |
| dashboard-dom.js 的replaceChildrenFromHtml | 仅保持minHeight到下一帧，下一帧释放后再恢复旧坐标 | 高度仍可变化；多块更新各自有旧scrollY与rAF回调，可能抢写全局滚动；用户期间主动滚动也可能被旧快照拉回 |
| dashboard-rendering.js 的sectionRenderKey | 排除部分心跳，但余额、盈亏、价格、缓存状态等变化仍可触发整块重建 | 金额频繁变动是正常刷新，不应让表格、tab、详情和图表节点全部销毁 |
| replaceElementFromHtml | 直接replaceWith，不使用view-state协议 | 策略cards等局部更新绕开已有scroll/focus保留逻辑 |

以上代码可明确定位；不能据静态阅读断言用户每次跳顶都由同一个触发因素造成。部署资源缓存、hash导航、实际滚动容器和异步图表高度需在真实浏览器确认。

### 修复后的行为契约

自动轮询只更新数据，不改变当前视图、账户/range选择、筛选条件、展开状态、键盘焦点和阅读位置。用户主动导航保留自己的导航语义；自动刷新不能触发导航或把焦点送回上方。数据错误保留最后成功内容，显示失效/过期状态，而不是把读到一半的页面清空。

阅读位置优先用“仍在阅读的业务节点 + 视口偏移”保持。例如当前在positions表某行，记录data-row-key与该行距离视口顶部的像素；上方增删行时保持该行同一位置。绝对scrollY只作找不到锚点的回退。页面实际内容变短、原节点删除时不能保证原坐标完全不变，应选相邻稳定节点并夹到合法范围，不能无条件跳0。

### 按文件执行的改动

1. **sections/strategy.js：修复被动focus滚动。** refreshStrategy恢复焦点使用`focusedTab.focus({ preventScroll: true })`；更优先保留原tab节点并只改数值，不必重新focus。把自动恢复焦点与Arrow/Home/End等用户键盘导航分开：用户主动导航可以合理滚入视图。审计所有自动刷新路径的focus/scrollIntoView/hash修改，不能全局禁止正常键盘导航。

2. **dashboard.js：轮询错误不删旧内容。** 区分首次加载与已有成功数据：首次无内容时可展示placeholder；已有内容时保留panel-body、表格、图表和高度，仅更新错误/STALE提示及最后成功时间。请求恢复后在原节点上更新。返回成功但render失败时也不清空旧视图。账户detail/metrics采用相同规则，同一账户/range后台刷新不得先插短loading块。

3. **dashboard-rendering.js +各section renderer：结构与数据更新分离。** structuralKey只包含视图类型、账户/列/业务行身份等结构，不能用整份响应JSON决定是否重建。常变的余额、盈亏、价格、时间、状态标签使用textContent/classList/attribute定点更新；表格按账户+标的+side/订单identity等稳定key更新行，图表复用实例setOption。data-only刷新保持DOM节点identity；动态状态仍必须即时更新，不能为减少重建把安全状态一起忽略。

4. **dashboard-dom.js：引入单一视图更新协调器。** 一次视图更新统一记录document.scrollingElement及真实overflow容器、稳定阅读锚点/offset、focus key、details/tab/filter/table-scroll状态。所有整块和局部替换通过该入口，包括replaceElementFromHtml、异步账户detail/metrics。批量提交后在布局稳定时只补偿一次锚点变化；同一视图使用递增render generation，过期rAF不得恢复旧快照。取消旧任务、校验root.isConnected；用户在快照后wheel/touch/导航键/拖动滚动条时取消旧位置恢复，不覆盖用户新位置。不能靠每个panel反复window.scrollTo互相抢写。

5. **布局/图表：保证真实内容加载阶段的稳定高度。** 首次加载占位区按该区最终布局预留高度；后台刷新保留图表surface与旧内容。账号/range明确切换时也保持详情shell稳定，待新数据到达再提交。临时minHeight不能仅到一帧就释放：由当前render generation控制，到此次受控结构变更与chart resize完成后释放，再做锚点补偿。不要永久固定整页高度，也不要无限ResizeObserver循环scrollTo。先使用浏览器原生滚动锚定；只有确定冲突的区域才按测试结果调整overflow-anchor。

6. **前端状态存储：选择与请求归属明确。** 选择账户、range、展开详情、筛选与每视图滚动状态放在明确的view state，renderer消费它；晚到请求按view/account/range/generation校验后才能写DOM，切换视图时丢弃旧响应与恢复任务。请求开始时的滚动位置不是提交时位置，快照应在实际DOM提交前获取。

7. **部署核对：确认用户实际加载的是修复版本。** index.html目前模块入口仍用日期query，dashboard.js子模块大多无内容版本。发布时应统一构建资源摘要/版本，入口HTML采用适合重新验证的缓存策略，避免入口与子模块混用旧资源。用浏览器Network核对实际dashboard-dom.js/strategy.js版本、etag、状态和served code generation；不能把要求用户反复清缓存作为最终修复，也不能未经核查就归咎于缓存。

### 必须补的真实浏览器回归验收

现有tests/frontend/dashboard-modules.test.mjs主要测render key、模型和HTML字符串；本次搜索未发现对captureViewState/restoreViewState的测试，也没有真实布局/焦点滚动验收。Node中伪造scrollY无法证明浏览器不会跳顶。

| 场景 | 操作 | 验收 |
| --- | --- | --- |
| 焦点遗留 | 点策略账户tab，让其保留焦点，再滚到下方详情；触发自动刷新 | 阅读锚点偏移变化≤2px，自动focus不把tab滚入视口 |
| 正常动态刷新 | 在账户positions/fills深处滚动，持续改变余额、价格、行数据，至少20轮 | 关键节点identity稳定；选择、筛选、展开与表格滚动不丢；不跳到顶 |
| API失败与恢复 | 在页面底部触发超时/500/离线，再返回成功 | 保留最后成功数据与页面高度，明确STALE；恢复时不跳顶 |
| 多请求竞态 | 交错完成overview、account、detail、metrics，包含晚到旧账户响应 | 旧响应不覆盖新视图，过期rAF不恢复旧坐标 |
| 刷新时用户滚动 | DOM更新到rAF恢复之间主动wheel/touch/键盘滚动 | 保留用户新位置，不被旧scrollY拉回 |
| 图表/布局变化 | 慢速图表初始化、窗口resize、上方业务行增删 | 使用稳定业务锚点，不因高度释放而跳顶 |
| 移动端/真实容器 | 窄视口、触摸滚动、details与table内滚动 | 实际scrollingElement及容器状态均保持，交互可达 |
| 版本验收 | 发布后使用普通已有浏览器会话，不依赖强制清缓存 | 所有模块为同一发布版本，以上场景仍通过 |

实施时使用真实浏览器E2E（例如现有测试环境可用的Playwright或浏览器自动化），记录scrollY、anchor rect、activeElement、scrollHeight和render generation；先把上述失败场景复现为测试，再改代码。桌面Chromium及用户实际使用的浏览器至少各验证一次。仅scrollY断言不足，应同时验证阅读锚点和没有被迫focus滚动。

验收完成后才能把本项标记“已修复”。当前结论：用户反馈有效，旧scrollTo补丁与render-key单测不足以关闭此问题。
