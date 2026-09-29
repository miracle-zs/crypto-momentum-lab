# Operator Dashboard 前端解耦清单与接口草案

日期：2026-09-24。范围：`operator_dashboard/static/**`（不含 vendor）。  
性质：**设计草案，不改代码**。目标是减少上帝文件、收敛 DOM 字符串协议、隔离滚动/图 reconcile，避免再出现跳顶式回归。

---

## 0. 目标与非目标

**目标**

1. 改 A 功能不再必须读懂 B 模块的内部 DOM。
2. 滚动保持、DOM reconcile、图表挂载各自独立，可单测。
3. section 有可检查的契约，`dashboard.js` 只做调度。

**非目标**

- 不上 Vue/React/构建链。
- 不引入全局事件总线 / Redux 类状态库。
- 不重写 CSS，不改 API 形状。

---

## 1. 现状问题（回顾）

| 问题 | 位置 | 后果 |
| --- | --- | --- |
| 编排上帝对象 | `dashboard.js` ~760 行，11 个 Map/Set | 轮询/就绪度/路由/过滤耦在一起 |
| 账户上帝对象 | `account.js` ~1100 行 | 渲染+接线+fetch+选中态一锅炖 |
| DOM 字符串协议 | `querySelector('.live-account-card-status')` 等 | 改 class 静默坏功能 |
| 模块过多职责 | `dashboard-dom.js` | 滚动补丁牵动 reconcile |
| 模块级单例 | scroll 时钟、chart 实例、选中账户 | 双实例/import 图变化时分叉 |

---

## 2. 目标目录（建议）

```text
static/
  core/
    config.js            # 现 dashboard-config
    formatters.js
    status.js            # statusSlug/normalizedStatus/UNCERTAIN
    dom-reconcile.js     # fragment/reconcile/replace/patch（无滚动）
    scroll-keep.js       # capture/restore/createScrollGuard/isUserScrolling
    view-state.js        # disclosure/table scroll/focus capture
    chart-engine.js
    section-contract.js  # 类型注释 + createSection() 工厂
  ui/
    widgets.js           # pill/tile/emptyBox/blockTitle/dataTable/searchBox
    css-keys.js          # 全部 data-* / class 常量
  charts/
    equity.js
    comparison.js
    live-metrics.js
  sections/
    overview.js
    risk.js
    account/
      index.js           # renderLiveAccounts / wire / updateDynamic
      render-detail.js   # renderAccount 纯渲染
      render-fleet.js    # 卡片/汇总
      signals.js
      loaders.js         # fetch + request-id 竞态
    strategy.js
    universe.js
    collector.js
    reports.js
    performance.js
  app/
    shell.js             # 路由/工作区/顶栏
    poller.js            # 轮询引擎
    readiness.js         # 全局就绪度
    runtime-badge.js     # MODE / HEARTBEAT
  dashboard.js           # 仅装配 app/* + sections/*
```

不必一次到位；可按第 5 节分四步迁。

---

## 3. 接口草案

### 3.1 Section 契约

```js
/**
 * @typedef {Object} SectionModule
 * @property {string} id                    // 与 DOM section#id 一致
 * @property {string} endpoint              // data-endpoint，默认来源
 * @property {number} [pollMs]              // 覆盖 SECTION_POLL_MS
 * @property {boolean} [safety]             // 是否进入全局就绪度
 * @property {(data: any) => [string, string]} render
 *        // [sectionStatus, html] — 保持现有返回值，便于渐进迁移
 * @property {(root: HTMLElement, data: any) => void} [onMount]
 *        // 首次或结构变更后接线
 * @property {(root: HTMLElement, data: any) => void} [onPatch]
 *        // render key 未变时的动态刷新
 * @property {() => void} [onTeardown]      // 可选
 */
```

工厂（可选，便于统一默认值）：

```js
export function createSection(def) {
  return {
    pollMs: 15000,
    safety: false,
    onPatch: () => {},
    ...def,
  };
}
```

`dashboard.js` 注册表改为：

```js
const sections = [overview, risk, account, ...];
const byId = Object.fromEntries(sections.map((s) => [s.id, s]));
```

**验收**：新增 section 不改 `poll()` / `refreshSection()` 源码，只加一个模块。

### 3.2 CSS / DOM 常量表（`ui/css-keys.js`）

把散落字符串收拢，渲染与接线共用：

```js
export const K = {
  sectionState: "section-state",
  liveAccountCardStatus: "live-account-card-status",
  liveAccountCardKpis: "live-account-card-kpis",
  liveAccountDetail: "data-live-account-detail",
  accountEquityRange: "data-account-equity-range",
  wired: "data-wired",
  // ...
};

export const sel = {
  accountCard: (label) => `[data-live-account-label="${cssEscape(label)}"]`,
  // 所有 querySelector 入口
};
```

**规则**：`sections/**` 里禁止再出现裸 class 字符串选择器；`innerHTML` 模板拼 class 时也从 `K.*` 取。

**验收**：全局搜 `querySelector("` 不再出现业务 class 字面量。

### 3.3 滚动模块（`core/scroll-keep.js`）

只保留两个公开语义：

```js
/** 真正的“掉到顶部”才修复；禁止按绝对 pageY 往回拽 */
createScrollGuard() -> { pageX, pageY, restore({force}?) }

/** 仅真实滚轮/触摸手势 */
isUserScrolling(now?) -> boolean
```

**不变量（写进模块头注释 + 测试）**

1. `currentY < pageY` 且 `currentY > 20` 时 **不得** `scrollTo(pageY)`（防“往前跳”）。
2. 只有 `pageY > 20 && currentY <= 20` 才恢复。
3. 用户手势窗口内 `restore` 空操作（除非 `force`）。

`view-state.js` 的 `captureViewState` / `restoreViewState` 继续负责锚点；与 `scroll-keep` 不得互相强制覆盖。

### 3.4 Reconcile（`core/dom-reconcile.js`）

```js
patchChildren(root, html)   // 现 patchChildrenFromHtml
replaceChildren(root, html)
replaceElement(el, html)
```

内部固定流程：

```text
captureViewState → (minHeight 锁) → reconcile → restoreViewState
→ rAF: 解锁 minHeight → restoreViewState
→ 50ms: 再 restore（图表 MutationObserver 之后）
```

滚动插拔用可选 hook，**不把 scroll 逻辑写进 reconcile**：

```js
patchChildren(root, html, { onAfterLayout: [fn1, fn2] })
```

### 3.5 账户区拆分边界

| 模块 | 导出 | 不得依赖 |
| --- | --- | --- |
| `account/render-fleet.js` | `liveAccountSummary`, `liveAccountCard`, `renderLiveAccounts` | fetch、全局选中态 |
| `account/render-detail.js` | `renderAccount`（纯函数） | DOM 接线 |
| `account/signals.js` | 信号表/证据 | 轮询 |
| `account/loaders.js` | `loadDetail`, `loadMetrics`（含 request-id） | 渲染字符串 |
| `account/index.js` | `wireLiveAccounts`, `updateLiveAccountsDynamic` | — |

模块级 `selectedLiveAccount` / request 计数迁入 **单个** `createAccountSectionState()`，由 `index.js` 持有，避免多实例分叉。

### 3.6 就绪度与轮询

```js
// app/readiness.js
export function buildReadinessModel(snapshots, { liveMode, now }) -> ReadinessModel

// app/poller.js
export function createPoller({ sections, fetchSection, onAfterBatch })
```

`globalReadinessModel` 的业务规则（halt / mismatch / TTL / LIVE 缺数据）全部进 `readiness.js` 并单测；`dashboard.js` 不再内联 if 链。

---

## 4. 明确禁止的写法（Code Review 红线）

1. 在 `sections/**` 使用裸业务 class 选择器（必须走 `K` / `sel`）。
2. 在 `scroll-keep` 里根据 `currentY < pageY` 做绝对回拉。
3. `dashboard.js` 新增 `Map/Set` 模块级状态（先进 `app/*` 状态对象）。
4. section 内直接 `document.getElementById` 改顶栏（走 `runtime-badge` / `readiness` API）。
5. 为测试在 DOM 里留兼容字符串 marker（改测契约函数）。

---

## 5. 迁移顺序（每步可独立上线）

| 步 | 内容 | 风险 | 验收 |
| --- | --- | --- | --- |
| **A** | 抽出 `ui/css-keys.js`，account/strategy 选择器改常量 | 低 | 现有前端单测 + 手工点账户切换 |
| **B** | 拆 `dashboard-dom` → `scroll-keep` + `dom-reconcile` + `view-state`，补 3 条滚动不变量测试 | 中（滚动敏感） | 单测 39+；实盘页滚到中部轮询 2 分钟不跳 |
| **C** | `account.js` 拆 render/wire/loaders，状态收进 factory | 中 | 账户区间切换、详情刷新、四账户图 |
| **D** | `dashboard.js` 拆 shell/poller/readiness，section 注册契约化 | 中 | 就绪度用例、LIVE 开关、错误保留上次数据 |
| **E** | 清理 legacy import marker / 无读者兼容分支 | 低 | 全量前端测试 |

**建议先做 B**（直接对应滚动回归），再 A（防 class 改名事故），C/D 可并行排期。

---

## 6. 工作量粗估

| 步 | 大约 |
| --- | --- |
| A | 0.5–1 天 |
| B | 1–2 天（含回归） |
| C | 1–2 天 |
| D | 1–2 天 |
| E | 0.5 天 |

合计约 **4–8 人日**，可分 PR 落地；每步保持 `node --test tests/frontend` 全绿再进下一步。

---

## 7. 成功标准

1. `dashboard.js` ≤ 250 行且无业务 if 长链。  
2. `account/` 单文件 ≤ 400 行。  
3. 滚动不变量 3 条有自动化测试，且**禁止**绝对 pageY 回拉。  
4. 新增 section 零改 `poller.js`。  
5. 连续两轮后台轮询（含 LIVE）无跳顶/前跳（人工验收脚本见下）。

**人工验收脚本（滚动）**

```text
1. 打开「实盘账户」，滚到页面中部，记下某行文案与 scrollY
2. 等待 2 个 15s 轮询周期（含强制 STALE/DEGRADED 刷新）
3. 期望：scrollY 变化 ≤ 2px 或仅因内容高度变化做锚点补偿，正文仍在原处
4. 向上滚 200px 后停住 1s，再等下一次轮询
5. 期望：不被拉回原位（禁止前跳）
```

---

## 8. 不做的事（再次强调）

- 不迁移 React/Vue。
- 不把 HTML 模板改成 JSX/框架模板。
- 不统一“全局 store”。
- 本清单落地前，**不**再往 `dashboard-dom.js` 里堆滚动特例。
