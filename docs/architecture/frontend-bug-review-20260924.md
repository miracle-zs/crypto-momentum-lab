# Operator Dashboard 前端 Bug 排查

日期：2026-09-24。范围：`src/crypto_momentum_lab/operator_dashboard/static/**`（约 5,300 行 JS + HTML/CSS），对照后端 `operator_dashboard/*.py` 字段口径。
方法：通读主循环 / DOM 补丁 / 图表引擎 / 8 个 section，核对 API 字段量纲，Node 复现，跑 `tests/frontend/dashboard-modules.test.mjs`（**36 passed**）。

**结论：语法与现有单测都是绿的，但仍有 1 个会把安全状态显示错的逻辑 bug，以及若干边界/一致性问题。最急的是全局就绪度在刷新失败后会把 BLOCKED 显示成 READY。**

> 2026-09-29 复核：P0 已在 `c8bc33a` 修复，且实测行为与本文“显示成 READY”的描述不同（实际降级为 `UNKNOWN` 并把读数清零），见下面「修复状态与实测更正」。

---

## 修复状态与实测更正（2026-09-29 复核）

**P0 已修复**（提交 `c8bc33a`）：失败分支改为 `markSectionError`，保留上次成功的 payload 并只标记 stale；关键分区刷新失败而读数本来正常时，就绪度显示 `STALE`（「尚未确认安全」），已确认的 BLOCKED / REVIEW 保持原判定并在 detail 追加「读数可能过期」。新增的 `latestSectionError` 记录失败原因与时间。

**对 P0 结论的一处更正**：上文“Node 复现”里的 `after error overwrite → READY` 不成立。用 JavaScriptCore 执行 `dashboard.js` 中真实的 `globalReadinessModel`（配同一份 `hasUncertainStatus` / `readinessStatusForSection` 逻辑）实测：错误占位对象会命中 `UNCERTAIN_STATUSES`，状态降级为 `UNKNOWN`（detail「3 个关键读数需要确认」），而不是 `READY`。真正的危害是**数字被清零**：`global-halts`、`global-ambiguous`、对账差异都显示 0 / —，“已知 2 个停机、5 处差异”被抹成“未知、0 个停机”。修复前的实测输出：

```text
BEFORE error: {"status":"BLOCKED","halts":"2","ambiguous":"1","reconciliation":"5 差异"}
AFTER  error: {"status":"UNKNOWN","halts":"0","ambiguous":"0","reconciliation":"UNKNOWN"}
```

**P1 / P2 状态（2026-09-29 抽查，仍未修）**：`dashboard.js` 非 LIVE 时的轮询范围与 `fetchTimeoutSignal` 定时器、`dashboard-ui.js` 的 `sideTag`、`dashboard-rendering.js` 的 account render key、`dashboard-chart-engine.js` 的 `notMerge:false`、`dashboard-formatters.js` 的 U+2212、`sections/performance.js` 的三处、`sections/account.js` 的选择器转义与手写 `×100`、`sections/collector.js` 的 `escNumber` 定义位置 —— 抽查后行为与本文档描述一致。

**本轮验证方式**：本机没有 node，`tests/frontend/dashboard-modules.test.mjs`（本文记录的 36 passed）未重跑；改用 JavaScriptCore（`osascript -l JavaScript`）抽取 `dashboard.js` 的真实函数体执行，并做语法解析检查。前端改动属于静态审读 + 上述探针，不等价于完整前端测试。

---

## P0 — 全局就绪度被刷新失败“洗白”

**位置**：`dashboard.js` 的 `updateGlobalState` / `refreshSection` catch 分支。

```js
// 成功时
updateGlobalState(id, data);                    // latestSectionData 存完整快照
// 失败时
updateGlobalState(id, { status: "UNKNOWN", error: true });  // 整份覆盖！
```

`globalReadinessModel()` 从 `latestSectionData` 读 halt / 未决订单 / 对账差异。错误占位对象会把真实数字冲成 0。

**Node 复现**：

```text
before error:        { status: 'BLOCKED', activeHalts: 2, mismatch: 5, ambiguous: 1 }
after error overwrite: { status: 'READY',  activeHalts: 0, mismatch: 0, ambiguous: 0 }
```

**后果**：网络抖动 / 单次 HTTP 5xx / 12s 超时后，顶部 SAFETY POSTURE 可能从「存在活跃停机 · 新入场已被阻断」变成「无实盘会话 · 只读安全 / READY」，掩盖真实停机与对账差异。

**建议**：错误时保留上次成功数据，只额外标记 `stale/error`；或 `updateGlobalState` 合并而非替换。就绪度在数据过期时应显示 `STALE`，不能归零。

---

## P1 — 非 LIVE 时 risk/account 停止轮询，旧读数一直挂在全局条

**位置**：`dashboard.js` `poll()`：

```js
const visibleSections = new Set(["overview", activeView]);
if (latestLiveMode === "LIVE") {
  visibleSections.add("risk");
  visibleSections.add("account");
}
```

LIVE 结束或 `live-rollout` 状态不再是 LIVE 后，risk/account 不再拉取，但 `latestSectionData` 里的旧 `mismatch_count` / `active_halts` 仍参与 `globalReadinessModel()`。

**复现**：`mismatch=9` 的旧 account 快照 + `latestLiveMode=UNKNOWN` → 一直显示 `REVIEW`，直到用户手动切到账户页。

**建议**：为参与就绪度的数据加 `observed_at` / TTL；过期则降级为 `UNKNOWN` 并提示“数据过期”，而不是沿用旧差异或假装无差异。LIVE 退出时也应显式刷新一次或清空安全读数。

---

## P1 — `sideTag` 对大小写/缺失方向一律显示「空」

**位置**：`dashboard-ui.js:12`

```js
export const sideTag = (side) => side === "long"
  ? '<span class="side-tag long">多</span>'
  : '<span class="side-tag short">空</span>';
```

**复现**：`LONG` / `Long` / `undefined` / `null` / `""` 全部渲染成「空」。

当前后端 `StrategySide` 是 `"long"/"short"`，主路径碰巧正确；但 live 信号、paper 持仓都走这里，一旦上游枚举变更或字段缺失，会把多头标成空头，直接影响运维判断。

**建议**：

```js
const s = String(side || "").toLowerCase();
if (s === "long") return '...多...';
if (s === "short") return '...空...';
return '<span class="side-tag unknown">—</span>';
```

---

## P2 — 其余问题清单

| # | 位置 | 问题 | 影响 |
| --- | --- | --- | --- |
| 1 | `performance.js:248-249` | `cpu_load_1m != null` 时直接格式化 `cpu_load_5m`，5m 为 null 会显示 `NaN` | 性能页脏数据 |
| 2 | `performance.js:252` | CPU 进度条写死 `load * 50`（按双核）；换核数后刻度失真 | 误导 |
| 3 | `performance.js:159` | 四账户相位卡写死 `primary/account-2/3/4` | 账户增减后 UI 错位 |
| 4 | `account.js:988` | `querySelector([data-live-account-label="${account.account_label}"]` 未转义；label 含 `"` `]` 会抛错 | 理论上可注入/崩溃 |
| 5 | `dashboard.js:346-352` | `AbortSignal.timeout` 不可用时，fallback 的 `setTimeout` 从不 `clearTimeout` | 定时器泄漏 |
| 6 | `dashboard-rendering.js:36-54` | account 的 render key 只保留 status/readiness 等，**丢掉 mismatch_count**；对账差异变化不触发重建，只靠 dynamic 刷新 | 若 dynamic 路径漏字段，差异角标不更新 |
| 7 | `dashboard-charts.js:429` | `refreshEcharts` 用 `notMerge:false`，系列数变少时可能残留旧 series | 图表偶发错线 |
| 8 | `dashboard-formatters.js:64` | 负金额用 Unicode `−`（U+2212），不是 ASCII `-` | 二次解析/对账拷贝会失败 |
| 9 | `account.js:122-125` | 绩效百分比手写 `*100`，未走统一 `percent()` | 与全局量纲口径分叉，后续易改错 |
| 10 | `dashboard.js:286-293` | 市场 tab 的 `data-market-wired` 标记在 patch 保留节点时有效；若节点被重建则依赖 `wireTableFilters`/`wireMarketViews` 重挂 — 目前有调用，但 search 输入值依赖 reconcile 保留 DOM property | 过滤词偶发丢失风险 |
| 11 | `index.html:7` | 注释里堆了大量 legacy asset marker | 无害，但说明测试/清单在靠字符串考古 |
| 12 | `collector.js:169` | `escNumber` 是函数声明会提升，能跑；但定义在使用之后，可读性差 | 维护风险 |

---

## 口径核对（目前正确的部分）

这些容易改错，本次核对**没有**发现量纲 bug：

| 字段 | 后端量纲 | 前端处理 | 结果 |
| --- | --- | --- | --- |
| `win_rate` | 0–1（胜/总） | `percent()` ×100 | 正确 |
| `return_pct`（paper 持仓） | 0–1（pnl/notional） | `signedPercent()` ×100 | 正确 |
| `upnl_pct` | 前端本地算 `upnl/notional` | `signedPercent()` | 正确 |
| `utc_day_return` | 0–1（price/open−1） | `returnBar` → `signedPercent` | 正确 |
| `impulse_return_pct` 等 features | 0–1 | `percent()` | 正确 |
| `equity_change_ratio` / `drawdown_ratio` / `margin_occupancy_ratio` | 0–1 | chart `percent` / `signed-percent` | 正确 |
| `twr` / `modified_dietz` | ratio | 手写 ×100 | 正确 |
| XSS | — | `esc()` 覆盖表格/标签主路径 | 基本安全 |

注意：`common_equity.py` 里的 `return_pct = delta/baseline*100` 已是百分数，但前端对比图用的是 `delta` 金额，**没有**再对这个字段做 ×100，所以当前未踩坑。若以后展示该字段，不能再乘 100。

---

## 测试缺口

现有 36 个前端单测覆盖渲染键稳定性、权益对比、滚动恢复，但**没有**覆盖：

1. 刷新失败后 `latestSectionData` 的保留/降级策略（P0）
2. 非 LIVE 时安全读数 TTL
3. `sideTag` 非法/大写 side
4. `performance` 字段部分为 null
5. `percent`/`signedPercent` 对 0–1 与 0–100 输入的契约测试

建议为 P0/P1 各补 1–2 个 node:test 用例，防止回归。

---

## 建议修复顺序

1. **P0** `refreshSection` 失败时不要覆盖成功快照；就绪度支持 STALE。
2. **P1** 安全读数 TTL / LIVE 退出清理。
3. **P1** `sideTag` 规范化 side。
4. **P2** performance 空值、label 选择器转义、AbortSignal 定时器清理。

---

## 补记：自动跳顶（2026-09-24 再修）

历史提交至少 5 次宣称 eliminate scroll jump（`663eeaf`…`0b4bd4f`，缓存串到 `zerojump-v2`），仍未根治。本次定位到三层根因并已改代码：

1. **双模块实例（关键）**  
   `dashboard.js` 用 `dashboard-dom.js?v=…`，`account.js`/`strategy.js` 用裸 `../dashboard-dom.js`。ESM 按 URL 区分模块，滚动状态（`userInteractionVersion`、scroll guard）各有一份，补丁互相看不见。现已去掉一等 import 的 `?v=`，只在 `index.html` 入口做缓存穿透。

2. **恢复逻辑只挡“掉到 0”**  
   `restoreViewState` 的 absolute defense 条件是 `targetY <= 20`，800→400 的半截上跳直接放行；且用户只要点过一次（pointerdown/click/keydown）就会 early-return 整段恢复。  
   现新增 `createScrollGuard()`，在 `updateChildrenFromHtml` / `poll()` 整批 DOM 写入前后锁住阅读位置；只有真实滚轮/触摸手势（350ms 内）才让位。

3. **`scroll-behavior: smooth`**  
   程序化回弹被做成动画，看起来就是“自动滚到顶”。已改为 `auto`。patch 路径的 `minHeight` 也改为 rAF 后再释放，避免同帧塌缩把 `scrollTop` 钳到 0。

回归：`tests/frontend/dashboard-modules.test.mjs` **39 passed**（含 3 个新的 scroll guard 用例）。  
入口缓存串：`dashboard.js?v=20260930-zerojump-v3`。

需要的话我可以直接按这个顺序改代码并补测试。
