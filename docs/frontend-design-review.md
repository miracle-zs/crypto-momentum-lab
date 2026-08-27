# CML 前端设计评估报告

> 评估日期：2026-08-16
> 评估范围：`src/crypto_momentum_lab/operator_dashboard/static/`

---

## 技术栈

- **纯原生** HTML + CSS + JavaScript，无任何框架依赖
- 单页面轮询（5秒一次），手动 DOM 操作
- SVG 手绘图表（sparkline、equity chart、strategy comparison）
- 代码规模：HTML ~165行、CSS ~1338行、JS ~1405行

---

## 优点（做得好的地方）

1. **设计系统完善** — CSS 变量体系完整（颜色、状态、系列色），`--s` 状态色自动绑定机制非常优雅
2. **暗色主题专业** — 渐变背景、毛玻璃 topbar、shimmer skeleton loading，视觉品质很高
3. **响应式设计扎实** — 4个断点（1280/1080/960/760/520px），移动端 sidebar 折叠为横向滚动标签
4. **无障碍基本到位** — `aria-label`、`aria-live`、`role="tablist"`、`prefers-reduced-motion`
5. **按需加载策略** — paper account 详情懒加载，equity 曲线后台批量加载
6. **状态管理清晰** — `latestSectionData` Map + `globalReadinessModel()` 聚合全局状态
7. **安全设计** — 只读界面、无凭据字段、XSS 转义 `esc()` 函数

---

## 问题和不足

| 类别 | 问题 | 严重程度 |
|------|------|----------|
| **可维护性** | 1400行单文件 JS，所有渲染逻辑混在一起 | 🔴 高 |
| **可维护性** | HTML 模板全部用字符串拼接，无组件抽象 | 🔴 高 |
| **性能** | 每次轮询全量 `innerHTML` 替换，触发大量重排重绘 | 🟡 中 |
| **体验** | 无 WebSocket/SSE，5秒轮询有延迟感 | 🟡 中 |
| **体验** | 手动管理 scroll state、disclosure state，代码复杂且脆弱 | 🟡 中 |
| **图表** | 纯手绘 SVG，无法实现缩放、tooltip、十字线等交互 | 🟡 中 |
| **测试** | 无前端测试，手工 DOM 拼接难以单测 | 🟡 中 |
| **国际化** | 中英文混写硬编码在 JS 模板里 | 🟢 低 |

---

## 评估结论

**不需要重写，但值得渐进优化。**

当前前端的**设计质量不错** — 暗色控制台风格统一、信息架构清晰、响应式完备。问题主要在**工程化**层面，而非设计层面。

- 设计层面评分：**8/10**
- 工程化评分：**5/10**

---

## 推荐优化路径（按优先级）

### 阶段 1：结构拆分（收益最高）

- 把 `dashboard.js` 拆成模块：`formatters.js`、`charts.js`、`sections/overview.js`、`sections/account.js` 等
- 用 ES modules 组织，保持原生无构建工具

### 阶段 2：渲染优化

- 用 `DocumentFragment` 或 keyed diff 替代全量 `innerHTML`
- 对高频更新区域（时钟、心跳状态）做局部更新

### 阶段 3：图表升级

- 引入轻量图表库（如 [uPlot](https://github.com/leeoniya/uPlot)，~35KB），支持 tooltip 和缩放
- 保持暗色主题一致性

### 阶段 4：通信升级（可选）

- 如果延迟敏感，后端加 SSE endpoint 替代轮询
- 前端用 `EventSource` 消费

---

## 不建议做的事

- ❌ 引入 React/Vue — 对这个规模的内部工具是过度工程
- ❌ 用 Tailwind/组件库 — 当前手写 CSS 品质很高，换掉可惜
- ❌ 全面重写 — 风险大、收益小
